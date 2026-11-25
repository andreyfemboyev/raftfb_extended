# RaftDB

[![CI](https://github.com/andreyfemboyev/raftfb_extended/actions/workflows/ci.yml/badge.svg)](https://github.com/andreyfemboyev/raftfb_extended/actions/workflows/ci.yml)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Version](https://img.shields.io/badge/version-0.2.0-orange)

**An experimental distributed SQL database with Raft consensus and native Bitcask-inspired storage.**

Built in Python with a dependency-free core. Developed with AI-assisted engineering using OpenAI Codex and ChatGPT.

> Experimental software for learning and research. Not recommended for production or untrusted networks.

[Architecture](ARCHITECTURE.md) · [Consistency](CONSISTENCY.md) · [Storage format](STORAGE_FORMAT.md) · [Benchmarks](benchmarks/README.md) · [Roadmap](ROADMAP.md)

## Highlights

- Custom SQL engine, replicated state machine and automatic leader election.
- Segmented binary journals, checksums, snapshots and crash recovery.
- Interactive SQL shell and Python client.
- CSV import, CSV/JSON export and optional pandas/NumPy integration.

## Quick start

```sh
git clone https://github.com/andreyfemboyev/raftfb_extended.git
cd raftfb_extended
python -m venv .venv
source .venv/bin/activate
python -m pip install .
```

## Performance

Reported single-client, three-node loopback benchmark (384 inserts, Python 3.12):

| Metric | v0.1 JSON | v0.2 native |
| --- | ---: | ---: |
| Inserts/s | 12.64 | 378.36 |
| Indexed reads/s | 12.44 | 376.51 |
| Median insert latency | 79.010 ms | 2.556 ms |
| Durable journal operations/s | 285.55 | 2,455.40 |

The end-to-end improvement includes replication scheduling changes, not only storage. These measurements are not general performance guarantees. See [methodology and raw results](benchmarks/README.md).

RaftDB is a fixed-membership relational database for Python 3.10+ on Linux/POSIX. It uses a custom SQL engine, Raft, and a native Bitcask-style relational store. The database and its CLI use the Python standard library.

## Run a cluster

Run each server in a separate terminal from the repository root. The supplied configuration uses loopback ports 8091–8093.

```sh
python -m raftdb serve --node n1 --data ./data/n1
python -m raftdb serve --node n2 --data ./data/n2
python -m raftdb serve --node n3 --data ./data/n3
```

In a fourth terminal, try the following commands.

```sh
python -m raftdb status
python -m raftdb sql "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN)"
python -m raftdb sql --request-id seed-tasks "INSERT INTO tasks VALUES (1, 'Write tests', TRUE), (2, 'Review results', FALSE)"
python -m raftdb sql "SELECT id, title FROM tasks WHERE done = FALSE ORDER BY id LIMIT 10"
python -m raftdb shell
```

The SQL client follows leader hints and retries. In the shell, use one statement per line and use `.status`, `.stats`, or `.quit`. The shell shows elapsed time and saves history under the XDG state directory. Override the file with `--history PATH`.

Use `--config PATH` on each command to select a different cluster. All nodes must share the same configuration, and each replica needs its own data directory. The [single-node example](examples/single.json) is suitable for local experiments. The [SQL examples](examples/tasks.sql) contain individual statements that can be submitted one at a time.

Each server accepts `--segment-size BYTES` (default 16 MiB) and `--snapshot-interval ENTRIES` (default 256 applied entries). `--bind` overrides the listening host while retaining the advertised port. Use a private, trusted network. The HTTP listener accepts unauthenticated replica, client, and maintenance requests.

## SQL

| Operation | Example |
| --- | --- |
| Create | `CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN)` |
| Drop | `DROP TABLE tasks` |
| Insert | `INSERT INTO tasks (id, title) VALUES (3, 'Add notes'), (4, 'It''s ready')` |
| Select | `SELECT id, title FROM tasks WHERE done IS NULL ORDER BY id DESC LIMIT 5 OFFSET 1` |
| Aggregate | `SELECT COUNT(*) FROM tasks WHERE id > 1` |
| Update | `UPDATE tasks SET done = TRUE WHERE id = 2` |
| Delete | `DELETE FROM tasks WHERE id = 3` |

Tables require one primary key. Column types are signed 64-bit `INTEGER`, `TEXT`, and `BOOLEAN`. Missing insert values default to `NULL` where allowed. Identifiers are unquoted and case-insensitive. Escape an apostrophe in SQL text by doubling it. The parser accepts one statement per request and an optional trailing semicolon.

`WHERE` supports comparisons to literals, `IS [NOT] NULL`, and `AND`. Use `ORDER BY` when row order matters. Nulls sort first in ascending order and last in descending order. `UPDATE` and `DELETE` apply to all rows when `WHERE` is omitted. A single statement is atomic, including constraint failures.

An aggregate query has one aggregate expression. `COUNT(*)` counts rows and `COUNT(column)` counts non-null values. `SUM` and `AVG` take integers and return `NULL` for an empty or all-null input. `AVG` returns a floating-point number. `MIN` and `MAX` use the column's ordering and ignore nulls. See the [roadmap](ROADMAP.md) for features outside this release.

## Client and data interchange

```python
from raftdb import Client

db = Client.from_config("cluster.json")
db.execute(
    "INSERT INTO tasks VALUES (?, ?, ?)",
    [3, "O'Brien's notes", False],
    request_id="tasks-import-3",
)
for page in db.query_batches("SELECT id, title FROM tasks ORDER BY id", batch_size=100):
    print(page["columns"], page["rows"])
```

Question marks bind scalar values. Use the same request ID and bound SQL to retry an operation with an uncertain outcome. Use a fresh ID for a fresh read. Each page of a batched result comes from the same committed SELECT. Server-side results are materialized and new results are capped at 16 MiB when encoded. Pagination bounds response batches. See [CONSISTENCY.md](CONSISTENCY.md) for the read and retry contracts.

```sh
python -m raftdb export --format csv "SELECT * FROM tasks ORDER BY id" > tasks.csv
python -m raftdb export --format json "SELECT * FROM tasks ORDER BY id" > tasks.json
python -m raftdb sql "CREATE TABLE imported_tasks (id INTEGER PRIMARY KEY, title TEXT, done BOOLEAN)"
python -m raftdb import-csv imported_tasks tasks.csv --types id:INTEGER,title:TEXT,done:BOOLEAN --request-id import-tasks
```

Import into an existing table with compatible columns and unused primary keys. CSV uses `\N` for SQL null and doubles a leading backslash on text values. Imports commit one batch at a time and split batches at the SQL byte limit. Retry with the same file, type map, batch size, and request prefix after a failed import. Python methods also accept file-like objects. Install `.[data-science]` to use the optional `to_pandas` and `to_numpy` methods.

## Operations

```sh
python -m raftdb stats
python -m raftdb snapshot --node n1
python -m raftdb compact --node n1
python -m raftdb verify
python -m raftdb verify --data ./data/n1  # n1 must be stopped
```

Online operations use the node's data lock and its Raft mutex. Snapshot and compact act per replica on applied committed state. They can temporarily delay that replica. Target replicas sequentially for large datasets. Offline verification takes the same exclusive directory lock and leaves the files untouched.

### Upgrade from the JSON format

Stop the entire v0.1 cluster and back up all data directories. Start every replica with v0.2 using its existing node ID, configuration, and directory. The first start imports the original term, vote, committed prefix, and uncommitted suffix. Imported statements replay with their original dialect, including recorded SQL errors. The original `raft.json` remains as the pre-upgrade image. Keep using the native files after upgrade. Rollback to v0.1 requires restoring the full pre-upgrade backup and loses any writes acknowledged after that backup.

## Verify and benchmark

```sh
python -W error::ResourceWarning -m unittest discover -s tests -v
python -m raftdb benchmark --baseline /path/to/original-raftdb
python benchmarks/storage.py /path/to/raftdb
```

See [benchmark methodology and results](benchmarks/README.md), [architecture](ARCHITECTURE.md), and the [storage format](STORAGE_FORMAT.md).

## Attribution and licensing

RaftDB retains its existing source history and authorship. The new storage design adapts the append-only segments, CRC checking, on-demand values, and atomic generation publication from femboydevs' BitcaskDB. This repository is distributed under the [MIT License](LICENSE). [RaftDB base](https://github.com/andreyfemboyev/raftdb/tree/7477321054526680df4652414c9b7a33abbc5bc3) · [BitcaskDB design reference](https://github.com/andreyfemboyev/bitcask-db/tree/e6beaa06130815811cd1048e8e89bfea5e726ad9)
