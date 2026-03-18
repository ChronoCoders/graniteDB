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
