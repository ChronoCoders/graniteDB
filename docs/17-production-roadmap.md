# 17. Production Roadmap

This document defines the staged plan to evolve GraniteDB from a correctness-driven prototype into a production-grade embedded LSM database.

## 17.1 What “Production-Grade” Means Here

GraniteDB is production-grade when it meets:

- explicit durability semantics for `SyncMode::{Yes,No}`
- crash safety across all persistence boundaries with automated coverage
- bounded recovery time (manifest checkpointing)
- predictable compaction behavior (multi-level invariants and backpressure)
- observability (metrics + tracing) sufficient for operating the system

## 17.2 Roadmap (Ordered)

### Phase A — Tighten Correctness & Fault Coverage

- Expand crash/IO fault matrix to systematically cover:
  - WAL, Manifest, SST, file rename, directory sync boundaries
  - actions: abort, torn abort, corrupt abort, partial, disk-full
- Validate restart state against the reference model in all cases.

### Phase B — Multi-Level LSM Metadata (Enabler)

- Generalize VersionSet to store `levels: Vec<Vec<FileMeta>>`.
- Manifest supports arbitrary `level_u32` in AddFile/DeleteFile.
- Read path iterates L0 then L1+ (per-level).

### Phase C — Multi-Level Compaction (L0..L3)

- Introduce L1+ invariant: per-level files are non-overlapping.
- Implement compaction selection and output placement for L0→L1 and Lk→L(k+1).
- Add per-level size targets and compaction triggers.

Status:

- Implemented metadata support (Phase B).
- Implemented synchronous multi-level compaction selection for L0→L1 and Lk→L(k+1) with configurable `max_levels`.

### Phase D — Background Work + Immutable MemTables

- Add immutable memtable queue.
- Move flush/compaction into a background worker with backpressure and stall rules.

### Phase E — Read Performance

- Bloom filters + filter blocks in SSTables.
- Block cache and iterator `seek`.

### Phase F — Operability

- Metrics/tracing, compaction stats, stall reporting.
- Manifest checkpointing / version snapshots.
- Repair/salvage tool.

### Phase G — Column Families (RocksDB Parity Enabler)

- Support multiple named column families with persistent IDs.
- Per-CF reads/writes/iterators.
- CF metadata stored in the manifest; recovery restores CF set.

### Phase H — Higher-Level Write Semantics

- `WriteBatch` with per-record CF ID.
- `WriteOptions` / `ReadOptions` style API surface (explicit per-call knobs).
- Range deletes (`DeleteRange`) and tombstone compaction correctness.
- Merge operator (application-defined value merge).

### Phase I — Advanced Compaction Control

- Compaction filters and TTL-style filtering.
- More compaction styles (universal/FIFO) and tuning controls.
- Trivial move, subcompactions, and better compaction scheduling/priority.

### Phase J — Table Format & Compression

- Pluggable compression (e.g., LZ4/Zstd) and/or per-level compression choices.
- More table options (partitioned index/filters, format versioning).
- Prefix extractor / prefix bloom optimizations.

### Phase K — Transactions & Concurrency

- Write groups (multiple writers) and improved concurrency semantics.
- Transactions (optimistic/pessimistic) and conflict checking.

### Phase L — Backup, Checkpoints, Ingestion

- Backup/checkpoint APIs and tooling.
- External SST ingestion (bulk load).

### Phase M — Operational Completeness

- Rich metrics, statistics, listeners, and compaction job reports.
- Rate limiting and IO prioritization.
