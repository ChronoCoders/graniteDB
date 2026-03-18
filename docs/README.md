# GraniteDB Documentation

This directory specifies GraniteDB’s correctness contract, on-disk formats, crash-consistency rules, and the test program required to claim “production-grade”.

## Reading Order (Recommended)

1. `01-contract.md`
2. `02-architecture.md`
3. `03-formats-wal.md`
4. `04-formats-sstable.md`
5. `05-formats-manifest.md`
6. `06-iterators-and-snapshots.md`
7. `07-crash-consistency-and-testing.md`
8. `08-operations.md`
9. `09-verification-tools.md`
10. `10-io-fault-simulation.md`
11. `11-fuzzing.md`

## Non-Negotiables

- WAL uses fixed-size block framing and record fragmentation (FULL/FIRST/MIDDLE/LAST).
- Manifest is treated like WAL: per-record checksum, stop at first invalid record, truncate tail.
- Files become “live” only by a durable Manifest edit; incomplete files are never referenced.
- Directory fsync is required after creates/renames in Sync::Yes mode.
- “Production-grade” requires crash testing across all crash points, not feature completeness.
