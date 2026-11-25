# RaftDBX v0.3.0 development

Branch: `codex/raftdbx-studio-v0.3.0`. Base: `cca9a7f375bbbe18b3f388f649d80189712574fb`.

## Platform milestone

- Preserved the v0.2.0 Raft, SQL and frame formats. Added adapters for native Windows byte-range locks, write-through namespace publication, directory flush attempts, history, child processes and memory measurement.
- Windows power-loss durability is experimental. Win32 may reject directory flushes and reports that limitation with an explicit warning. Process-crash tests exercise recovery on both CI operating systems.
- Baseline Linux suite passed all 25 tests before edits.

## Continuation

Run `python -W error::ResourceWarning -m unittest discover -s tests -v`. Check the Windows matrix after pushing the branch. Implement Studio, exercise real local and remote connections, and add package builds and release validation before creating a version tag.
