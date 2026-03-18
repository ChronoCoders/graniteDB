# 14. Model Checking (loom)

This phase uses `loom` to explore thread interleavings and detect concurrency bugs.

GraniteDB’s on-disk DB uses OS files, which loom does not model. Loom is used here with an in-memory DB variant to validate synchronization correctness.

## 14.1 Running

```bash
cargo test --test loom --features loom
```

## 14.2 Scope

- Detects data races and ordering bugs in concurrent access patterns.
- Does not test disk/IO correctness.
