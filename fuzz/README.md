# GraniteDB Fuzzing

This directory is used with `cargo-fuzz` to stress parsers and recovery code.

## Setup

Install `cargo-fuzz`:

```bash
cargo install cargo-fuzz
```

## Targets

- `fuzz_wal`: WAL recovery and truncation logic
- `fuzz_sstable`: SSTable parsing and iteration
- `fuzz_manifest`: Manifest replay and truncation logic

Run a target:

```bash
cargo fuzz run fuzz_wal
```

Note: `cargo-fuzz` typically requires a nightly toolchain and a working clang toolchain on the host.
