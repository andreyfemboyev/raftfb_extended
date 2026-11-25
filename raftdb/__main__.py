"""Run replicas and operate a RaftDB cluster."""

import argparse
import csv
import json
from pathlib import Path
import threading
import sys
import time

from .client import Client, QueryError
from .config import load_cluster
from .journal import DEFAULT_SEGMENT_SIZE
from .platform_support import ShellHistory, history_path
from .raft import Node, SNAPSHOT_INTERVAL
from .relational import Relational
from .server import Server
from .storage import Storage


def serve(args, members, cluster):
    if args.node not in members:
        raise ValueError(f"Unknown node {args.node}")
    host, configured_port = members[args.node]
    port = args.listen_port if args.listen_port is not None else configured_port
    storage = Storage(args.data, segment_size=args.segment_size)
    node = None
    try:
        node = Node(args.node, members, cluster, storage, snapshot_interval=args.snapshot_interval)
        with Server((args.bind or host, port), node) as server:
            server.launch_id = args.managed_stdin
            node.start()
            print(f"Node {args.node} listening on {args.bind or host}:{port}", flush=True)
            try:
                if args.managed_stdin:
                    # The only writer is the parent. Explicit close and parent
                    # death both deliver EOF without a PID or public stop endpoint.
                    stopped = threading.Event()

                    def watch_parent():
                        sys.stdin.buffer.read(1)
                        stopped.set()

                    threading.Thread(target=watch_parent, daemon=True).start()
                    server.timeout = 0.15
                    while not stopped.is_set():
                        server.handle_request()
                else:
                    server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                node.stop()
    finally:
        try:
            if node is not None:
                node.db.store.close()
        finally:
            storage.close()
    return 0


def verify_offline(directory):
    storage = Storage(directory, repair=False)
    try:
        if storage.log.identity is None:
            raise ValueError("Uninitialized Raft journal")
        relational = Relational(storage.directory / "relational", DEFAULT_SEGMENT_SIZE, repair=False)
        try:
            if relational.applied > storage.log.commit:
                raise ValueError("Relational checkpoint exceeds durable Raft commit")
            relational.verify()
            if storage.log.snapshot is not None:
                relational.validate_snapshot(storage.snapshot_path, storage.log.base)
            return {
                "ok": True, "verified_index": relational.applied, "commit_index": storage.log.commit,
                "snapshot_index": storage.log.base, "replay_pending": storage.log.commit - relational.applied,
            }
        finally:
            relational.store.close()
    finally:
        storage.close()


def print_json(result):
    print(json.dumps(result, ensure_ascii=True, indent=2))


def cell(value):
    if value is None:
        return "NULL"
    if type(value) is bool:
        return str(value).upper()
    text = str(value)
    text = "".join(char if char.isprintable() else repr(char)[1:-1] for char in text)
    return text if len(text) <= 60 else text[:59] + "…"


def print_table(columns, records):
    rows = [[cell(value) for value in row] for row in (columns, *records)]
    widths = [max(len(value) for value in values) for values in zip(*rows)]
    for number, row in enumerate(rows):
        print(" | ".join(value.ljust(width) for value, width in zip(row, widths)).rstrip())
        if number == 0:
            print("-+-".join("-" * width for width in widths))
    print(f"{len(records)} row(s)")


def shell(client, history):
    history = ShellHistory(history)
    try:
        history.load()
    except OSError as error:
        print(f"History unavailable ({error})", file=sys.stderr)
    print("RaftDBX SQL shell. Enter one statement per line. Use .status, .stats or .quit.")
    try:
        while True:
            try:
                sql = input("raftdb> ").strip()
                if not sql:
                    continue
                if sql in (".quit", ".exit"):
                    break
                history.add(sql)
                if sql in (".status", ".stats"):
                    print_json(client.inspect(sql[1:]))
                    continue
                started = time.perf_counter()
                result = client.execute(sql)
                if "rows" in result:
                    print_table(result["columns"], result["rows"])
                else:
                    print(f"{result['affected']} row(s) affected")
                print(f"{(time.perf_counter() - started) * 1000:.1f} ms")
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
            except QueryError as error:
                suffix = f" (request {error.request_id})" if error.request_id else ""
                print(f"{error}{suffix}", file=sys.stderr)
            except ValueError as error:
                print(error, file=sys.stderr)
    finally:
        try:
            history.save()
        except OSError as error:
            print(f"Cannot save SQL history ({error})", file=sys.stderr)
    return 0


def positive_timeout(raw):
    value = float(raw)
    if not 0 < value <= 3600:
        raise argparse.ArgumentTypeError("Timeout must be greater than zero and at most 3600 seconds")
    return value


def cli_parser():
    parser = argparse.ArgumentParser(prog="python -m raftdb")
    sub = parser.add_subparsers(dest="operation", required=True)
    server = sub.add_parser("serve", help="Start one replica")
    server.add_argument("--node", required=True)
    server.add_argument("--data", required=True)
    server.add_argument("--segment-size", type=int, default=DEFAULT_SEGMENT_SIZE, metavar="BYTES")
    server.add_argument("--snapshot-interval", type=int, default=SNAPSHOT_INTERVAL, metavar="ENTRIES")
    server.add_argument("--bind", help="Override the local bind host while retaining the configured port")
    server.add_argument("--listen-port", type=int, help="Override the port used for the local listener")
    server.add_argument("--config", default="cluster.json")
    server.add_argument("--managed-stdin", help=argparse.SUPPRESS)

    client = sub.add_parser("sql", help="Execute one statement from an argument or standard input")
    client.add_argument("sql", nargs="?")
    client.add_argument("--request-id", help="Reuse this ID when retrying the same operation")

    interactive = sub.add_parser("shell", help="Open the interactive SQL shell")
    interactive.add_argument("--history", default=history_path(), metavar="PATH")

    commands = [client, interactive]
    for name, description in {
        "status": "Show observed leader and replica progress",
        "stats": "Show local log, snapshot and relational storage usage",
        "compact": "Snapshot applied state and reclaim obsolete storage",
        "snapshot": "Publish a snapshot of each selected replica's applied state",
        "verify": "Check durable records, indexes and snapshots on selected replicas",
    }.items():
        command = sub.add_parser(name, help=description)
        target = command.add_mutually_exclusive_group() if name == "verify" else command
        target.add_argument("--node", help="Target one replica instead of all configured replicas")
        if name == "verify":
            target.add_argument("--data", help="Verify a stopped replica's data directory without repairs")
        commands.append(command)

    importer = sub.add_parser("import-csv", help="Insert CSV records into an existing table")
    importer.add_argument("table")
    importer.add_argument("path", type=Path)
    importer.add_argument("--types", required=True, metavar="COLUMN:TYPE,...")
    importer.add_argument("--batch-size", type=int, default=100)
    importer.add_argument("--request-id", help="Use the same prefix and CSV to resume an interrupted import")
    exporter = sub.add_parser("export", help="Export one committed SELECT result")
    exporter.add_argument("sql")
    exporter.add_argument("--format", choices=("csv", "json"), default="csv")
    exporter.add_argument("--batch-size", type=int, default=1000)
    commands.extend((importer, exporter))
    for command in commands:
        command.add_argument("--config", default="cluster.json")
        command.add_argument("--timeout", type=positive_timeout, default=6.0)

    from .benchmark import add_arguments

    add_arguments(sub.add_parser("benchmark", help="Measure a disposable three-node cluster"))
    return parser


def main():
    args = cli_parser().parse_args()
    try:
        operation = args.operation
        if operation == "benchmark":
            from .benchmark import run

            print_json(run(args))
            return 0
        if operation == "verify" and args.data is not None:
            print_json(verify_offline(args.data))
            return 0
        members, cluster = load_cluster(args.config)
        if operation == "serve":
            return serve(args, members, cluster)
        client = Client(members, cluster=cluster, timeout=args.timeout)
        if operation == "sql":
            sql = args.sql if args.sql is not None else sys.stdin.read()
            status, result = client.request(sql, request_id=args.request_id)
            print_json(result)
            return 0 if status == 200 else 2
        if operation == "shell":
            return shell(client, args.history)
        if operation == "import-csv":
            types = dict(part.split(":", 1) for part in args.types.split(","))
            with args.path.open(encoding="utf-8", newline="") as source:
                affected = client.import_csv(
                    args.table, source, {name: kind.upper() for name, kind in types.items()},
                    batch_size=args.batch_size, request_id=args.request_id,
                )
            print_json({"ok": True, "affected": affected})
            return 0
        if operation == "export":
            export = client.export_csv if args.format == "csv" else client.export_json
            export(sys.stdout, args.sql, batch_size=args.batch_size)
            return 0
        states = client.inspect(operation, node=args.node)
        print_json(states)
        errors = ["error" in state for state in states.values()]
        failed = all(errors) if operation == "status" else any(errors)
        return 2 if failed else 0
    except QueryError as error:
        print(f"raftdb: {error} (request {error.request_id})", file=sys.stderr)
        return 2
    except (OSError, ValueError, KeyError, TypeError, csv.Error) as error:
        print(f"raftdb: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
