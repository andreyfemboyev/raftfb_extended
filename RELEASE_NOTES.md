# RaftDB v0.2.0 — Native Storage Engine

First public experimental milestone: distributed SQL, Raft consensus, segmented binary journals, persistent relational storage, snapshot transfer, compaction, migration from v0.1, interactive SQL shell and optional pandas/NumPy integration.

The reported small three-node loopback benchmark achieved 378.36 inserts/s versus 12.64 in v0.1. This includes replication scheduling improvements; see [methodology](benchmarks/README.md).

Requires Python 3.10+ on Linux/POSIX. Licensed under MIT. Experimental, unauthenticated transport: use only on trusted networks. Not for production.

See [README](README.md), [consistency guarantees](CONSISTENCY.md) and [roadmap](ROADMAP.md).
