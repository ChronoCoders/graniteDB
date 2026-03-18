# 12. Miri (Undefined Behavior Detection)

Miri is mandatory if `unsafe` is introduced. It can also be run proactively to catch UB in dependencies or future changes.

Run:

```bash
cargo +nightly miri test
```

Notes:

- Miri requires a nightly toolchain.
- Miri does not model disk/IO behavior; it checks for undefined behavior in Rust execution.
