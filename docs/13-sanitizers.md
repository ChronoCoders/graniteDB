# 13. Sanitizers (ASan / TSan)

Sanitizers are complementary tools for catching memory corruption and data races.

## 13.1 AddressSanitizer (ASan)

Example (nightly):

```bash
RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -Zbuild-std
```

## 13.2 ThreadSanitizer (TSan)

Example (nightly):

```bash
RUSTFLAGS="-Zsanitizer=thread" cargo +nightly test -Zbuild-std
```

Notes:

- Sanitizers typically require a working clang toolchain.
- Some platforms may require additional linker/runtime configuration.
