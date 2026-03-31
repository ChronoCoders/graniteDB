# GraniteDB

An embedded key-value database written in Rust, built around crash consistency and correctness guarantees.

GraniteDB is an LSM-tree storage engine with a single-writer + concurrent-readers model. The design priority is correctness first: every persistence boundary is covered by automated crash testing before any new feature lands.

## Features

- **Two durability modes** — `SyncMode::Yes` (fsync before ack) and `SyncMode::No` (buffered, consistent-on-crash)
- **Snapshot isolation** — sequence-number-based MVCC; reads are always consistent
- **Atomic batch writes** — `WriteBatch` with per-record column family targeting
- **Column families** — multiple named namespaces with persistent IDs, backed by the Manifest
- **Transactions** — optimistic with read-set conflict detection
- **Range deletes** — `DeleteRange` with tombstone compaction correctness
- **Merge operator** — application-defined value merging (e.g. `AppendMergeOperator`)
- **Multiple compaction styles** — Leveled, Universal, FIFO; trivial move and subcompaction support
- **Compression** — LZ4 per SSTable block, pluggable per level
- **Bloom filters** — per-SST filter blocks for point-lookup acceleration
- **LRU block cache** — configurable capacity, shared across SSTables
- **Write backpressure** — L0 slowdown/stop triggers and `max_write_bytes_per_sec` rate limiting
- **Checkpoints** — consistent point-in-time snapshots of the live database
- **SST ingestion** — bulk-load external SST files
- **Repair tool** — salvage and recover a damaged database directory
- **Event listeners** — `DbEventListener` trait for flush, compaction, and stall events
- **Metrics** — per-operation counters and per-level size stats via `DbMetrics`

## Quick Start

```rust
use granitedb::{DB, Options, SyncMode};

let opts = Options {
    sync: SyncMode::Yes,
    ..Options::default()
};

let db = DB::open("/tmp/mydb", opts)?;

db.put(b"hello", b"world")?;
let val = db.get(b"hello")?; // Some(b"world")

db.delete(b"hello")?;
```

### Batch Writes

```rust
use granitedb::WriteBatch;

let mut batch = WriteBatch::new();
batch.put(b"k1", b"v1");
batch.put(b"k2", b"v2");
batch.delete(b"old");
db.write_batch(batch)?;
```

### Snapshots

```rust
let snap = db.snapshot();
db.put(b"key", b"new_value")?;

// Reads from the point in time the snapshot was taken
let old = db.get_with_snapshot(b"key", &snap)?;
```

### Transactions

```rust
let txn = db.begin_transaction();
txn.put(b"account:1", b"100")?;
txn.put(b"account:2", b"200")?;
txn.commit()?; // atomic, conflicts checked on commit
```

### Iteration

```rust
use std::ops::Bound;
use granitedb::Range;

let range = Range {
    start: Bound::Included(b"a".to_vec()),
    end: Bound::Excluded(b"z".to_vec()),
};

for (key, value) in db.iter(range)? {
    println!("{:?} => {:?}", key, value);
}
```

## Configuration

```rust
use granitedb::{Options, SyncMode, Compression, CompactionStyle};

let opts = Options {
    sync: SyncMode::Yes,
    memtable_max_bytes: 64 * 1024 * 1024,       // 64 MB memtable
    l0_slowdown_trigger: 8,
    l0_stop_trigger: 12,
    max_levels: 4,
    level1_target_bytes: 256 * 1024 * 1024,      // 256 MB L1
    level_multiplier: 10,
    compaction_style: CompactionStyle::Leveled,
    sstable_compression: Compression::Lz4,
    bloom_bits_per_key: 10,
    block_cache_capacity_bytes: 64 * 1024 * 1024,
    max_write_bytes_per_sec: 0,                  // 0 = unlimited
    ..Options::default()
};
```

## Development

```bash
# Build
cargo build

# Tests
cargo test
cargo test <test_name> -- --exact --nocapture

# Crash consistency tests
cargo test --test crash

# Concurrency tests (Loom)
cargo test --test loom --features loom

# Property-based tests
cargo test --test properties

# Lint and format
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings

# Benchmarks
cargo bench --bench db

# Fuzzing (requires cargo-fuzz + nightly)
cargo fuzz run fuzz_wal
cargo fuzz run fuzz_sstable
cargo fuzz run fuzz_manifest

# Miri (nightly)
cargo +nightly miri test

# Sanitizers (nightly)
RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -Zbuild-std
RUSTFLAGS="-Zsanitizer=thread" cargo +nightly test -Zbuild-std
```

## Architecture

GraniteDB follows a standard LSM-tree layout:

```
Write path:   client → WAL (fsync) → MemTable → immutable MemTable → L0 SST → L1+ SST
Read path:    MemTable → immutable MemTables → L0 SSTables → L1+ SSTables
Metadata:     Manifest (WAL-structured, per-record checksum, truncate-on-corruption)
```

A file is only "live" once a durable Manifest edit references it. Incomplete files written before a crash are never surfaced. Directory fsync is required after every file create or rename in `SyncMode::Yes`.

See [`docs/`](docs/) for format specifications, crash-consistency rules, and the full correctness contract.

## Testing Philosophy

Production-grade in GraniteDB means crash-safe across every persistence boundary — not feature-complete. The test suite includes:

- **Crash tests** — failpoint injection at WAL writes, flush boundaries, compaction boundaries, and manifest commits; post-crash recovery is verified against a reference model
- **IO fault simulation** — disk-full, partial write, torn abort scenarios
- **Fuzz targets** — WAL recovery, SSTable parsing, Manifest replay
- **Loom** — exhaustive concurrency interleavings
- **Miri** — undefined behavior detection (weekly CI)
- **Sanitizers** — ASan and TSan (weekly CI)

## License

MIT

---

© ChronoCoders — Altug Tatlisu
