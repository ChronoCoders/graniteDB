# 4. SSTable Format (v1, Minimal but Verifiable)

SSTables are immutable sorted files of internal keys. They are never modified in place.

## 4.1 Files

- Naming: `000001.sst`, `000002.sst`, ...
- SST becomes live only when referenced by the durable Manifest.

## 4.2 Internal Key (Required)

```text
internal_key = user_key || suffix
suffix = (MAX_U64 - seq) as u64_be || value_type_u8
```

- `u64_be` is big-endian to preserve lexicographic ordering.
- `value_type_u8`: `1 = Value`, `0 = Tombstone`

This ensures: for equal `user_key`, newer `seq` sorts first.

## 4.3 File Layout

```text
[data blocks]
[index block]
[filter block (optional, can be empty in v1)]
[footer]
```

All offsets are byte offsets from file start.

## 4.4 Data Blocks

A data block is a sequence of entries, sorted by `internal_key` ascending.

Entry encoding (v1, no prefix compression):

```text
| key_len_u32 (4B LE) | key_bytes |
| value_len_u32 (4B LE) | value_bytes |
```

- `key_bytes` is the internal key.
- `value_bytes` is the user value for Value records, or empty for Tombstones.

Block trailer:

```text
| block_crc32c_u32 (4B LE) | compression_type_u8 (1B) |
```

- `block_crc32c` covers the block entry bytes (not including trailer).
- `compression_type_u8`: `0 = None` (only this is allowed in v1).

## 4.5 Index Block

Index block contains one entry per data block:

```text
| count_u32 |
  repeated count times:
    | sep_key_len_u32 | sep_key_bytes |
    | block_offset_u64_le |
    | block_len_u32_le |
```

- `sep_key` is the last internal key in the corresponding data block (v1 simplicity).

Lookup:

- Binary search `sep_key` to find the first index entry with `sep_key >= target_internal_key`.

## 4.6 Filter Block (Optional)

v1 may omit bloom filters by writing an empty filter block and setting filter offsets to zero.

If implemented:

- Filter is keyed by user_key (not internal key).
- Filter format must include its own checksum.

## 4.7 Footer (Fixed Size)

Footer is fixed 40 bytes:

```text
| index_off_u64_le | index_len_u64_le |
| filter_off_u64_le | filter_len_u64_le |
| magic_u64_le |
```

- `magic_u64_le = 0x4752414E49544531` (ASCII "GRANITE1" packed)
- Footer is always at file end.

## 4.8 File Lifecycle (Crash-Safe)

To create an SST:

1. Write to temp name: `000123.sst.tmp`
2. fsync(temp file)
3. atomic rename to `000123.sst`
4. fsync(parent directory)
5. Append Manifest(AddFile) and fsync(manifest) in Sync::Yes
6. Only after manifest durability may old inputs be deleted (and deletions should be best-effort and restart-safe)

Rule: never reference temp files in the Manifest.
