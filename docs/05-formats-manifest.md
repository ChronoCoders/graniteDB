# 5. Manifest + VersionSet (Authoritative Metadata)

Manifest defines the set of live SST files and their level placement. It is the source of truth for SST liveness.

## 5.1 Files

- `MANIFEST-000001`, `MANIFEST-000002`, ...
- `CURRENT` points to the active manifest filename.

## 5.2 Log Safety Requirements (Same as WAL)

Manifest is a checksummed append-only log.

Requirements:

- Each record is checksummed.
- Replay stops at the first invalid record.
- Corrupted tail is truncated.
- Partial records must never be interpreted as valid state.

Recommended: reuse the WAL block-framed record format exactly (32KiB blocks, FULL/FIRST/MIDDLE/LAST, CRC32C).

## 5.3 Manifest Record Payloads

Each logical manifest record payload begins with:

```text
| rec_type_u8 |
```

Record types (v1):

- `1 = AddFile`
- `2 = DeleteFile`

### AddFile Payload

```text
| rec_type_u8 = 1 |
| level_u32_le |
| file_id_u64_le |
| file_size_u64_le |
| smallest_key_len_u32_le | smallest_key_bytes |
| largest_key_len_u32_le  | largest_key_bytes |
```

- `smallest_key_bytes` and `largest_key_bytes` are internal keys.

### DeleteFile Payload

```text
| rec_type_u8 = 2 |
| level_u32_le |
| file_id_u64_le |
```

## 5.4 CURRENT File

`CURRENT` is a small text file containing exactly the active manifest filename plus newline, e.g.:

```text
MANIFEST-000042\n
```

Update protocol (Sync::Yes):

1. Write new CURRENT temp: `CURRENT.tmp`
2. fsync(CURRENT.tmp)
3. atomic rename to `CURRENT`
4. fsync(parent directory)

## 5.5 Startup Recovery Procedure

1. Read `CURRENT` → open active manifest.
2. Replay manifest log into VersionSet; stop at first invalid record; truncate tail.
3. Scan DB directory for WAL files newer than the latest checkpointed state; replay into MemTable(s).
4. Startup GC:
   - List all `.sst` files on disk.
   - Delete any SST not referenced by VersionSet.
   - Delete any `*.tmp` artifacts.

Correctness: Orphan SSTs must never affect visible state because they are not in Manifest.
