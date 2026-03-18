# 9. Correctness & Verification Tooling

This project treats verification as a first-class requirement. These tools help find bugs, but correctness comes from: reference model + property tests + crash simulation.

## 9.1 Static Analysis (Compile-Time)

Clippy:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

## 9.2 Undefined Behavior Detection

Miri:

```bash
cargo +nightly miri test
```

## 9.3 Memory & Concurrency Sanitizers

ThreadSanitizer and AddressSanitizer are used as follow-up tools after correctness tests are stable.

## 9.4 Property Testing (Critical)

proptest:

- Generates random sequences of `put/delete/get/iter/restart`.
- Compares results against an in-memory `BTreeMap` model.

Run:

```bash
cargo test --test property
```

## 9.5 Fuzz Testing

cargo-fuzz:

- Targets WAL parsing, SST parsing, and manifest parsing.

## 9.6 Crash Testing (Most Important)

- Failpoints at persistence boundaries:
  - WAL fragment writes and WAL fsync
  - SST write/rename/dir fsync
  - Manifest append/fsync
- Restart DB and validate against a reference model.

Run:

```bash
cargo test --test crash -- --nocapture
```

## 9.7 Model Checking (Later)

loom is reserved for exploring concurrency interleavings after the single-writer design is proven correct.

## 9.8 Minimum Required Stack

- Clippy
- proptest
- failpoints + crash tests
- cargo-fuzz
- Miri (mandatory if `unsafe` is introduced)
