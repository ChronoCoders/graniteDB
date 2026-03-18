# 1. Product & Contract

## 1.1 API Specification

### Core API

```rust
open(path: &str, options: Options) -> DB
close()

put(key: &[u8], value: &[u8])
get(key: &[u8]) -> Option<Vec<u8>>
delete(key: &[u8])

write_batch(batch: WriteBatch)

iter(range: Range) -> Iterator
```

### Snapshots

```rust
snapshot() -> Snapshot
get_with_snapshot(key: &[u8], snapshot: Snapshot) -> Option<Vec<u8>>
iter_with_snapshot(range: Range, snapshot: Snapshot) -> Iterator
```

## 1.2 Semantics

- Put overwrites previous values (by creating a newer version).
- Delete creates a tombstone.
- Reads return:
  - latest visible value, or
  - value visible at snapshot sequence.

## 1.3 Durability Modes

### Sync::Yes

- WAL is fsync’d before acknowledging the write.
- Parent directory is fsync’d after any file create/rename that must survive crash.

### Sync::No

- Writes are acknowledged after buffered write.
- Data loss is possible on crash, but recovered state must still be internally consistent.

## 1.4 Guarantees

- Atomic batch writes.
- No partial batch visible after crash.
- Crash-safe recovery correctness.
- Snapshot isolation (sequence-based visibility).

## 1.5 Non-Goals (Initial)

- No replication.
- No distributed consensus.
- No SQL layer.

## 1.6 Forbidden Early Features (Correctness Guardrails)

Do NOT implement early:

- async IO
- multi-writer
- advanced compaction heuristics
- prefix compression
- partitioned index / advanced table formats

Rationale: these hide correctness bugs and complicate crash invariants.

## 1.7 Definitions

- “Ack boundary”: the point at which a call returns success to the client.
- “Visible”: returned by get/iter for a given snapshot sequence.
- “Live SST”: an SST referenced by the durable Manifest state.
