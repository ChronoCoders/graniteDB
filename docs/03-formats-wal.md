# 3. WAL Format (Production-Safe)

This WAL format is mandatory. Without fixed-size block framing and fragmentation, corruption can cascade and destroy alignment for all subsequent records.

## 3.1 Files

- WAL files live in the DB directory.
- Naming: `000001.log`, `000002.log`, ... (monotonic file id).

## 3.2 Block Framing

- WAL is a sequence of fixed-size blocks: `BLOCK_SIZE = 32 * 1024` bytes.

Blocks are independently parseable. Record fragments never span blocks.

## 3.3 Record Header (Per Fragment)

Each fragment has a 7-byte header followed by payload bytes.

```text
| crc32c (4B LE) | len (2B LE) | type (1B) | payload (len bytes) |
```

- `crc32c`: CRC-32C (Castagnoli) of `type || payload` (exactly those bytes).
- `len`: number of payload bytes in this fragment.
- `type`: one of:
  - `1 = FULL`
  - `2 = FIRST`
  - `3 = MIDDLE`
  - `4 = LAST`

Constraints:
- `0 <= len <= (BLOCK_SIZE - header_bytes - block_offset_remaining)`.
- If remaining space in the block is less than header size (7 bytes), pad the remainder with zeros and start next record at next block.

## 3.4 Logical Records

A logical record (what the DB replays) is:

- a single `FULL` fragment, OR
- a sequence: `FIRST` + zero or more `MIDDLE` + `LAST`.

The payload bytes of fragments are concatenated to form the logical record payload.

## 3.5 Logical Payload (WriteBatch Record)

A logical WAL record payload encodes exactly one batch with an explicit completeness boundary:

```text
| batch_len_u32 (4B LE) | batch_bytes (batch_len_u32 bytes) |
```

The DB must only apply a batch if the full logical record is reconstructed and `batch_len_u32` is consistent with the logical record payload length.

`batch_bytes` encoding (v1, minimal):

```text
| count_u32 |
  repeated count times:
    | op_type_u8 | key_len_u32 | key_bytes | (value_len_u32 | value_bytes)? |
```

- `op_type_u8`: `1 = Put`, `2 = Delete`
- Put includes value; Delete does not.

Sequence numbers are not stored per entry in WAL payload; they are assigned at write time and derived during replay by re-applying batches in log order with a persisted `next_seq` state. If you prefer explicitness, store `start_seq_u64` in the WAL payload; doing so is allowed but must be specified consistently.

## 3.6 WAL Append Rules

- WAL is append-only.
- On `Sync::Yes`, the WAL file is fsync’d before acknowledging the write.
- Group commit is allowed: multiple pending batches can be appended, then a single fsync can acknowledge all of them.

## 3.7 Recovery

Algorithm:

1. Open WAL(s) in file-id order.
2. Scan blocks sequentially.
3. For each fragment:
   - Validate `len` bounds.
   - Validate `crc32c`.
   - If invalid: stop scanning at the first invalid fragment, truncate WAL to last known-good offset, and stop.
4. Reconstruct logical records:
   - Apply only FULL records or complete FIRST..LAST sequences.
   - Discard incomplete trailing sequences.
5. Replay batches in order into a fresh MemTable.

Outcome:

- Every acknowledged batch is either fully applied or fully absent after crash (per durability mode and ack boundary).
