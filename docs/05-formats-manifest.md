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

Each logical manifest record payload is a single atomic VersionEdit containing one or more tagged fields.

```text
| tag_u8 | tag_payload |
| tag_u8 | tag_payload |
...
```

Tags (v1):

- `1 = AddFile`
- `2 = DeleteFile`
- `3 = SetLogNumber`
- `4 = SetLastSequence`

### AddFile Tag Payload

```text
| tag_u8 = 1 |
| level_u32_le |
| file_id_u64_le |
| file_size_u64_le |
| smallest_key_len_u32_le | smallest_key_bytes |
| largest_key_len_u32_le  | largest_key_bytes |
```

- `smallest_key_bytes` and `largest_key_bytes` are internal keys.

### DeleteFile Tag Payload

```text
| tag_u8 = 2 |
| level_u32_le |
| file_id_u64_le |
```

### SetLogNumber Tag Payload

Defines the active WAL file id for recovery.

```text
| tag_u8 = 3 |
| log_number_u64_le |
```

### SetLastSequence Tag Payload

Defines the minimum sequence baseline after restart (next_seq = last_sequence + 1).

```text
| tag_u8 = 4 |
| last_sequence_u64_le |
```

Atomicity requirement:

- Any flush or compaction that installs new SST files MUST include:
  - AddFile tags for outputs
  - DeleteFile tags for inputs (if applicable)
  - SetLastSequence reflecting the current `last_sequence`
  - SetLogNumber if WAL rotation is part of the operation

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
3. Open and replay the WAL file indicated by `log_number` into MemTable(s).
4. Startup GC:
   - List all `.sst` files on disk.
   - Delete any SST not referenced by VersionSet.
   - Delete any `.log` files with id less than `log_number`.
   - Delete any `*.tmp` artifacts.

Correctness: Orphan SSTs must never affect visible state because they are not in Manifest.
