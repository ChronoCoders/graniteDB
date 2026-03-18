# 11. Fuzz Testing

This phase uses `cargo-fuzz` to stress WAL, Manifest, and SST parsing/recovery code.

## 11.1 Targets

Targets live under `fuzz/fuzz_targets/`:

- `fuzz_wal`: WAL recovery and truncation logic
- `fuzz_manifest`: manifest replay and tail truncation logic
- `fuzz_sstable`: SSTable parsing and iteration logic

## 11.2 Setup

Install cargo-fuzz:

```bash
cargo install cargo-fuzz
```

## 11.3 Running

From the repository root:

```bash
cargo fuzz run fuzz_wal
```

## 11.4 Invariants

Fuzz targets must never:

- hang
- perform unbounded allocations without rejecting the input
- panic due to internal invariants being violated by malformed input (errors are OK)
