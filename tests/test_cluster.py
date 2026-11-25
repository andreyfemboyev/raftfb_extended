from concurrent.futures import ThreadPoolExecutor
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from raftdb.client import Client
from raftdb.platform_support import WINDOWS
from raftdb.raft import SNAPSHOT_CHUNK, rpc
from raftdb.relational import Relational
from raftdb.storage import Storage


ROOT = Path(__file__).resolve().parents[1]


class Relay(BaseHTTPRequestHandler):
    def do_GET(self):
        connection = http.client.HTTPConnection(*self.server.destination, timeout=0.5)
        try:
            connection.request("GET", self.path)
            with connection.getresponse() as result:
                self.reply(result.status, json.load(result))
        except (OSError, http.client.HTTPException, ValueError):
            self.reply(503, {})
        finally:
            connection.close()

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if (payload.get("source"), self.server.member) in self.server.blocked:
            self.reply(503, {})
            return
        try:
            status, result = rpc(self.server.destination, self.path, payload, timeout=3)
        except (OSError, http.client.HTTPException, ValueError):
            status, result = 503, {}
        self.reply(status, result)

    def reply(self, status, result):
        try:
            self.send_response(status)
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())
        except (BrokenPipeError, ConnectionError):
            pass

    def log_message(self, *args):
        pass


class ClusterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.config = self.directory / "cluster.json"
        self.members = {}
        self.processes = {}
        self.blocked = set()
        self.relays = []
        self.addCleanup(self.stop_relays)
        self.addCleanup(self.stop_all)
        proxies = {}
        sockets = []
        try:
            for node in ("a", "b", "c"):
                for addresses in (self.members, proxies):
                    reservation = socket.socket()
                    sockets.append(reservation)
                    reservation.bind(("127.0.0.1", 0))
                    addresses[node] = reservation.getsockname()
        finally:
            for reservation in sockets:
                reservation.close()
        self.config.write_text(json.dumps({
            "cluster": "integration",
            "nodes": {node: f"{host}:{port}" for node, (host, port) in proxies.items()},
        }))
        # The peer config points to the relays; the CLI test config uses the
        # same peer addresses so that the fixed-membership fingerprint matches.
        for member, destination in self.members.items():
            relay = ThreadingHTTPServer(proxies[member], Relay)
            relay.member, relay.destination, relay.blocked = member, destination, self.blocked
            thread = threading.Thread(target=relay.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
            self.relays.append((relay, thread))
            thread.start()
        for node in self.members:
            self.start(node)

    def start(self, node):
        with (self.directory / f"{node}.log").open("ab") as log:
            self.processes[node] = subprocess.Popen(
                [sys.executable, "-m", "raftdb", "serve", "--node", node,
                 "--data", str(self.directory / node), "--config", str(self.config),
                 "--bind", "127.0.0.1", "--listen-port", str(self.members[node][1]), "--snapshot-interval", "12", "--segment-size", "65536"],
                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
            )

    def stop(self, node):
        process = self.processes.pop(node)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=3)

    def stop_all(self):
        for node in list(self.processes):
            self.stop(node)

    def stop_relays(self):
        for relay, thread in self.relays:
            relay.shutdown()
            thread.join()
            relay.server_close()

    def client(self):
        configured = Client.from_config(self.config)
        return Client(self.members, cluster=configured.cluster)

    def get_status(self, node):
        connection = http.client.HTTPConnection(*self.members[node], timeout=0.25)
        try:
            connection.request("GET", "/status")
            with connection.getresponse() as response:
                return json.load(response)
        except (OSError, http.client.HTTPException, ValueError):
            return {}
        finally:
            connection.close()

    def wait_for(self, description, check, timeout=8):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            result = check()
            if result:
                return result
            time.sleep(0.05)
        statuses = {node: self.get_status(node) for node in self.processes}
        logs = {node: (self.directory / f"{node}.log").read_text() for node in self.members}
        self.fail(f"Timed out waiting for {description}. Status: {statuses}. Logs: {logs}")

    def leader(self, *, among=None):
        def elected():
            states = [self.get_status(node) for node in (among or self.processes)]
            leaders = [state["node"] for state in states if state.get("role") == "leader"]
            return leaders[0] if len(leaders) == 1 else None

        return self.wait_for("one leader", elected)

    def caught_up(self, nodes, index):
        self.wait_for(
            f"{nodes} to apply index {index}",
            lambda: all(self.get_status(node).get("last_applied", 0) >= index for node in nodes),
        )

    def submit(self, node, request_id, sql, status=200):
        response_status, response = rpc(
            self.members[node], "/sql", {"request_id": request_id, "sql": sql}, timeout=3,
        )
        self.assertEqual(response_status, status, response)
        return response

    def cli(self, *args):
        result = subprocess.run(
            [sys.executable, "-m", "raftdb", *args, "--config", str(self.config)],
            cwd=ROOT, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        return json.loads(result.stdout)

    def test_leader_crash_concurrent_retries_and_full_restart(self):
        leader = self.leader()
        self.cli("sql", "CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)")
        followers = [node for node in self.members if node != leader]
        response = self.submit(followers[0], "follower-only", "SELECT * FROM events", status=503)
        self.assertEqual(response["code"], "not_leader")

        write = "INSERT INTO events VALUES (1, 'confirmed')"
        with ThreadPoolExecutor(max_workers=8) as workers:
            results = list(workers.map(lambda _: self.submit(leader, "write", write), range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.submit(leader, "write", "DELETE FROM events", status=409)

        self.stop(leader)
        successor = self.leader()
        self.assertNotEqual(successor, leader)
        self.assertEqual(self.submit(successor, "write", write), results[0])
        read = self.submit(successor, "read", "SELECT * FROM events")
        self.assertEqual(read["rows"], [[1, "confirmed"]])

        self.start(leader)
        self.caught_up(list(self.members), read["index"])
        self.stop_all()
        for node in self.members:
            self.start(node)
        recovered = self.leader()
        self.assertEqual(self.submit(recovered, "write", write), results[0])
        self.assertEqual(self.cli("sql", "SELECT COUNT(*) FROM events")["rows"], [[1]])

        def insert_distinct(number):
            return self.submit(recovered, f"distinct-{number}", f"INSERT INTO events VALUES ({number}, 'batch')")

        with ThreadPoolExecutor(max_workers=8) as workers:
            writes = list(workers.map(insert_distinct, range(2, 26)))
        self.assertEqual(len({result["index"] for result in writes}), len(writes))
        self.assertEqual(self.cli("sql", "SELECT COUNT(*) FROM events")["rows"], [[25]])

    def test_minority_cannot_confirm_and_divergent_suffix_is_repaired(self):
        leader = self.leader()
        created = self.submit(leader, "create", "CREATE TABLE test (id INTEGER PRIMARY KEY)")
        self.caught_up(list(self.members), created["index"])
        followers = [node for node in self.members if node != leader]
        # Prevent late proxy deliveries from the isolated leader when the
        # followers restart. A timed-out RPC can still arrive at its target.
        self.blocked.update((leader, node) for node in followers)
        for node in followers:
            self.stop(node)

        blocked = self.submit(leader, "lost-write", "INSERT INTO test VALUES (41)", status=503)
        self.assertEqual(blocked["code"], "commit_timeout")
        blocked = self.submit(leader, "lost-read", "SELECT * FROM test", status=503)
        self.assertEqual(blocked["code"], "commit_timeout")
        isolated = self.get_status(leader)
        self.assertEqual(isolated["commit_index"], created["index"])
        self.assertGreater(isolated["last_log_index"], isolated["commit_index"])

        self.stop(leader)
        for node in followers:
            self.start(node)
        successor = self.leader(among=followers)
        self.submit(successor, "new-write", "INSERT INTO test VALUES (42)")
        read = self.submit(successor, "new-read", "SELECT * FROM test")
        self.assertEqual(read["rows"], [[42]])

        self.blocked.clear()
        self.start(leader)
        self.caught_up(list(self.members), read["index"])
        self.stop(leader)
        storage = Storage(self.directory / leader)
        self.addCleanup(storage.close)
        log = storage.log.batch(storage.log.base + 1, storage.log.last_index - storage.log.base)
        requests = [entry.get("request_id") for entry in log]
        self.assertNotIn("lost-write", requests)
        self.assertNotIn("lost-read", requests)


    def test_live_minority_partition_rejects_fresh_reads_and_writes(self):
        leader = self.leader()
        created = self.submit(leader, "create", "CREATE TABLE test (id INTEGER PRIMARY KEY)")
        self.caught_up(list(self.members), created["index"])
        followers = [node for node in self.members if node != leader]
        self.blocked.update(
            edge
            for follower in followers
            for edge in ((leader, follower), (follower, leader))
        )
        successor = self.leader(among=followers)
        self.assertEqual(self.get_status(leader)["role"], "leader")
        confirmed = self.submit(successor, "majority", "INSERT INTO test VALUES (42)")
        with ThreadPoolExecutor(max_workers=2) as workers:
            futures = [
                workers.submit(self.submit, leader, "minority-read", "SELECT * FROM test", 503),
                workers.submit(self.submit, leader, "minority-write", "INSERT INTO test VALUES (41)", 503),
            ]
            self.assertTrue(all(future.result()["code"] == "commit_timeout" for future in futures))
        self.assertEqual(self.get_status(leader)["commit_index"], created["index"])
        self.blocked.clear()
        self.caught_up(list(self.members), confirmed["index"])
        read = self.submit(successor, "after-heal", "SELECT id FROM test ORDER BY id")
        self.assertEqual(read["rows"], [[42]])

    def test_multichunk_snapshot_repairs_lagging_replica_and_survives_restart(self):
        leader = self.leader()
        lagging = next(node for node in self.members if node != leader)
        self.stop(lagging)
        created = self.submit(leader, "create-big", "CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT)")
        self.assertGreater(created["index"], 0)
        values = [[number, str(number % 10) * 10_000] for number in range(20)]
        client = self.client()
        for number, payload in values:
            client.execute("INSERT INTO big VALUES (?, ?)", (number, payload), request_id=f"load-{number}")
        snapshot = self.cli("snapshot", "--node", leader)[leader]["snapshot"]
        self.assertGreater(snapshot["size"], SNAPSHOT_CHUNK)
        self.assertGreater(snapshot["index"], created["index"])
        inserted = self.submit(leader, "after-snapshot", "INSERT INTO big VALUES (20, 'suffix')")
        self.start(lagging)
        self.caught_up(list(self.members), inserted["index"])
        self.assertGreaterEqual(self.get_status(lagging)["snapshot_index"], snapshot["index"])
        self.assertEqual(self.cli("compact", "--node", lagging)[lagging]["ok"], True)
        for state in self.cli("verify").values():
            self.assertTrue(state["ok"])

        self.stop_all()
        for member in self.members:
            verified = self.cli("verify", "--data", str(self.directory / member))
            self.assertEqual(verified["replay_pending"], 0)
            db = Relational(self.directory / member / "relational", 65536, repair=False)
            try:
                rows = db.tables["big"].rows
                self.assertEqual([[key, rows[key]["data"]] for key in sorted(rows)], values + [[20, "suffix"]])
                self.assertEqual(db.result("after-snapshot")[1], inserted["index"])
            finally:
                db.store.close()

        for member in self.members:
            self.start(member)
        self.assertEqual(self.cli("sql", "SELECT COUNT(*) FROM big")["rows"], [[21]])

    def test_shell_formats_results_and_saves_history(self):
        client = self.client()
        client.execute("CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT)")
        client.execute("INSERT INTO demo VALUES (1, 'interactive')")
        history = self.directory / "history"
        command = [
            sys.executable, "-m", "raftdb", "shell", "--config", str(self.config),
            "--history", str(history),
        ]
        for statement in ("SELECT name FROM demo", "SELECT id FROM demo"):
            result = subprocess.run(
                command, input=f"{statement}\n.stats\n.quit\n", cwd=ROOT,
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("1 row(s)", result.stdout)
            self.assertIn("snapshot_bytes", result.stdout)
        # Read using the platform's readline so both GNU readline and libedit
        # history encodings are accepted.
        if WINDOWS:
            saved = history.read_text(encoding="utf-8").splitlines()
        else:
            saved = subprocess.check_output(
                [sys.executable, "-c", (
                    "import json, readline, sys; readline.read_history_file(sys.argv[1]); "
                    "print(json.dumps([readline.get_history_item(i) for i in "
                    "range(1, readline.get_current_history_length() + 1)]))"
                ), str(history)],
                text=True,
            )
            saved = json.loads(saved)
        self.assertEqual(saved, [
            "SELECT name FROM demo", ".stats", "SELECT id FROM demo", ".stats",
        ])

    def test_parameter_binding_csv_exports_and_batches_across_failover(self):
        client = self.client()
        leader = self.leader()
        client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT, active BOOLEAN)")
        hostile = "'); DROP TABLE items; SELECT '?\n"
        client.execute("INSERT INTO items VALUES (?, ?, ?)", [1, hostile, True])
        client.execute("INSERT INTO items VALUES (?, ?, ?)", [2, r"\N", None])
        client.execute("INSERT INTO items VALUES (?, ?, ?)", [3, None, False])
        pages = client.query_batches("SELECT * FROM items ORDER BY id", batch_size=1)
        first = next(pages)
        self.assertEqual(first["rows"], [[1, hostile, True]])
        self.stop(leader)
        self.leader()
        client.execute("UPDATE items SET note = 'changed' WHERE id = 2")
        second, third = list(pages)
        self.assertEqual(second["rows"], [[2, r"\N", None]])
        self.assertEqual(third["rows"], [[3, None, False]])
        self.assertEqual({first["index"], second["index"], third["index"]}, {first["index"]})

        client.execute("CREATE TABLE copy (id INTEGER PRIMARY KEY, note TEXT, active BOOLEAN)")
        data = io.StringIO()
        client.export_csv(data, "SELECT * FROM items ORDER BY id", batch_size=2)
        data.seek(0)
        types = {"id": "INTEGER", "note": "TEXT", "active": "BOOLEAN"}
        self.assertEqual(client.import_csv("copy", data, types, batch_size=2, request_id="import"), 3)
        data.seek(0)
        self.assertEqual(client.import_csv("copy", data, types, batch_size=2, request_id="import"), 3)
        result = io.StringIO()
        client.export_json(result, "SELECT * FROM copy ORDER BY id", batch_size=1)
        self.assertEqual(json.loads(result.getvalue())["rows"], [[1, hostile, True], [2, "changed", None], [3, None, False]])


if __name__ == "__main__":
    unittest.main()
