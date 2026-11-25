"""Reproducible local-process benchmarks for native and legacy RaftDB."""

from contextlib import contextmanager
import json
import math
from pathlib import Path
import platform
import random
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import perf_counter, sleep

from .platform_support import process_memory
from .client import Client, literal
from .raft import rpc


def positive_integer(raw):
    result = int(raw)
    if result <= 0:
        raise ValueError("Expected a positive integer")
    return result


def add_arguments(parser):
    parser.add_argument("--rows", type=positive_integer, default=384)
    parser.add_argument("--reads", type=positive_integer, default=128)
    parser.add_argument("--updates", type=positive_integer, default=128)
    parser.add_argument("--replication-samples", type=positive_integer, default=24)
    parser.add_argument("--payload-bytes", type=positive_integer, default=256)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--baseline", type=Path, help="Checkout of the original JSON-based RaftDB")
    parser.add_argument(
        "--dataset", type=Path, help="Write the seed's CREATE and INSERT SQL to this new file",
    )
    parser.add_argument(
        "--source", type=Path, default=Path(__file__).resolve().parents[1],
        help="Working directory for the native RaftDB server",
    )


def summarize(samples, elapsed=None):
    ordered = sorted(samples)
    result = {"operations": len(samples)}
    for p in (50, 95, 99):
        position = math.ceil(len(ordered) * p / 100) - 1
        result[f"p{p}_ms"] = round(1000 * ordered[position], 3)
    if elapsed is not None:
        result["operations_per_second"] = round(len(samples) / elapsed, 2)
    return result


def dataset(args):
    source = random.Random(args.seed)
    create = "CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
    payloads = [source.randbytes(args.payload_bytes).hex()[:args.payload_bytes] for _ in range(args.rows)]
    inserts = [f"INSERT INTO events VALUES ({key}, {literal(value)})" for key, value in enumerate(payloads)]
    updates = [
        f"UPDATE events SET payload = {literal('update-' + str(i))} WHERE id = {source.randrange(args.rows)}"
        for i in range(args.updates)
    ]
    reads = [
        f"SELECT payload FROM events WHERE id = {source.randrange(args.rows)}" for _ in range(args.reads)
    ]
    replication = [
        f"UPDATE events SET payload = {literal('replicate-' + str(i))} WHERE id = {i % args.rows}"
        for i in range(args.replication_samples)
    ]
    return create, inserts, updates, reads, replication


def wait_for(probe, description, *, interval=0.01):
    deadline = perf_counter() + 20
    while perf_counter() < deadline:
        value = probe()
        if value:
            return value
        sleep(interval)
    raise ValueError(f"Benchmark timed out while waiting for {description}")


class Cluster:
    def __init__(self, source, root):
        self.source = source
        self.root = root
        self.processes = {}
        self.config = root / "cluster.json"
        with socket.socket() as a, socket.socket() as b, socket.socket() as c:
            members = {}
            for name, reservation in zip(("a", "b", "c"), (a, b, c)):
                reservation.bind(("127.0.0.1", 0))
                members[name] = reservation.getsockname()
            self.config.write_text(json.dumps({
                "cluster": "benchmark",
                "nodes": {name: f"{host}:{port}" for name, (host, port) in members.items()},
            }))
        self.client = Client.from_config(self.config)

    def start(self):
        for node in self.client.members:
            with (self.root / f"{node}.log").open("ab") as log:
                self.processes[node] = subprocess.Popen(
                    [sys.executable, "-m", "raftdb", "serve", "--node", node,
                     "--data", str(self.root / node), "--config", str(self.config)],
                    cwd=self.source, stdout=log, stderr=subprocess.STDOUT,
                )

    def stop(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.kill()
        for process in self.processes.values():
            process.wait(timeout=5)
        self.processes.clear()

    def states(self):
        for node, process in self.processes.items():
            if process.poll() is not None:
                log = (self.root / f"{node}.log").read_text()[-3000:]
                raise ValueError(f"Benchmark node {node} exited ({process.returncode})\n{log}")
        return self.client.inspect()

    def leader(self):
        def elected():
            leaders = [node for node, state in self.states().items() if state.get("role") == "leader"]
            return leaders[0] if len(leaders) == 1 else None

        return wait_for(elected, "leader election")

    def caught_up(self, index):
        wait_for(
            lambda: all(state.get("last_applied", 0) >= index for state in self.states().values()),
            f"all replicas applying index {index}", interval=0.005,
        )

    def disk_bytes(self):
        return sum(
            path.stat().st_size
            for node in self.client.members
            for path in (self.root / node).rglob("*") if path.is_file()
        )

    def memory(self):
        samples = [process_memory(process.pid) for process in self.processes.values()]
        if not samples or any(sample is None for sample in samples):
            return None
        rss, peaks = zip(*samples)
        return {"cluster_rss_bytes": sum(rss), "max_node_peak_rss_bytes": max(peaks)}


@contextmanager
def disposable_cluster(source):
    with TemporaryDirectory(prefix="raftdb-bench-") as directory:
        cluster = Cluster(source, Path(directory))
        try:
            cluster.start()
            yield cluster
        finally:
            cluster.stop()


def measure(cluster, commands, args, *, native):
    create, inserts, updates, reads, replication = commands
    cluster.client.leader = cluster.leader()
    cluster.client.execute(create, request_id="create")

    def phase(name, statements):
        samples = []
        start = perf_counter()
        for number, sql in enumerate(statements):
            began = perf_counter()
            result = cluster.client.execute(sql, request_id=f"{name}-{number}")
            samples.append(perf_counter() - began)
            if result.get("affected") != 1 and len(result.get("rows", ())) != 1:
                raise ValueError(f"Unexpected {name} result in benchmark")
        return summarize(samples, perf_counter() - start)

    metrics = {
        "insert": phase("insert", inserts),
        "read": phase("read", reads),
        "update": phase("update", updates),
    }
    samples = []
    for number, sql in enumerate(replication):
        began = perf_counter()
        result = cluster.client.execute(sql, request_id=f"replicate-{number}")
        cluster.caught_up(result["index"])
        samples.append(perf_counter() - began)
    metrics["all_replica_apply"] = summarize(samples)

    metrics["disk_bytes_before_compaction"] = cluster.disk_bytes()
    metrics["memory_before_restart"] = cluster.memory()
    cluster.stop()
    began = perf_counter()
    cluster.start()
    leader = cluster.leader()
    cluster.client.leader = leader
    recovered = cluster.client.execute("SELECT COUNT(*) FROM events", request_id="recovered")
    metrics["restart_to_quorum_read_ms"] = round((perf_counter() - began) * 1000, 3)
    if recovered["rows"] != [[args.rows]]:
        raise ValueError("Unexpected row count after benchmark restart")
    cluster.caught_up(recovered["index"])
    metrics["recovery_verified_rows"] = args.rows
    metrics["compaction"] = None
    if native:
        before = cluster.disk_bytes()
        began = perf_counter()
        for node in cluster.client.members:
            status, result = rpc(
                cluster.client.members[node], "/admin/compact",
                {"cluster": cluster.client.cluster}, timeout=20,
            )
            if status != 200 or not result.get("ok"):
                raise ValueError(f"Benchmark compaction failed on {node}")
        metrics["compaction"] = {
            "all_nodes_ms": round((perf_counter() - began) * 1000, 3),
            "bytes_before": before, "bytes_after": cluster.disk_bytes(),
        }
    return metrics


def source_revision(source):
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=source, capture_output=True, text=True, check=False,
        )
    except FileNotFoundError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def run(args):
    if args.payload_bytes > 15_000:
        raise ValueError("Payload must be at most 15000 bytes")
    commands = dataset(args)
    if args.dataset is not None:
        with args.dataset.open("x", encoding="utf-8") as target:
            for sql in (commands[0], *commands[1]):
                target.write(sql + ";\n")
    result = {
        "python": platform.python_version(), "platform": platform.platform(),
        "nodes": 3, "clients": 1, "seed": args.seed, "payload_bytes": args.payload_bytes,
        "durability": "Each version's default fsync-before-ack settings",
        "native_revision": source_revision(args.source),
    }
    with disposable_cluster(args.source) as cluster:
        result["native"] = measure(cluster, commands, args, native=True)
    if args.baseline is not None:
        result["baseline_revision"] = source_revision(args.baseline)
        with disposable_cluster(args.baseline) as cluster:
            result["json_baseline"] = measure(cluster, commands, args, native=False)
    return result
