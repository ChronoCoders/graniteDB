# 6. Iterators & Snapshots (Subtle and Critical)

## 6.1 Snapshot Model

- Global `seq` is a monotonic u64 assigned by the writer thread.
- A snapshot is a `read_seq: u64`.
- Visibility rule: an entry is visible to a snapshot if `entry.seq <= read_seq`.

## 6.2 Merging Iterator (Required)

A correct read path requires a merging iterator across sorted sources:

Sources (in precedence order):

- MemTable (newest writes)
- Immutable MemTables (newest to oldest, planned)
- L0 SSTs (newest-first; overlapping key ranges)
- L1+ SSTs (non-overlapping per level)

All sources yield entries ordered by internal key (user_key asc, seq desc via inverted suffix).

## 6.3 User-Key Deduplication Layer (Required)

On top of the merged internal-key stream, enforce:

- Snapshot filter: skip entries with `seq > read_seq`.
- For each `user_key`, return at most one visible result:
  - The first visible entry decides:
    - Value => yield (user_key, value)
    - Tombstone => yield nothing for that user_key
  - Then skip all remaining entries with that same `user_key` and continue.

This prevents duplicate user keys and ensures tombstones suppress older values.

## 6.4 get(key) Semantics

`get(key)` is a specialized case:

- Seek to `user_key` in each source iterator.
- Merge and apply the same snapshot+dedup logic.
- Return the first decided result (Value or Tombstone -> None).

## 6.5 iter(range) Semantics (Define Explicitly)

Range is over user keys.
Define:

- `Range { start: Bound, end: Bound }`
- Bounds are user_key bytes; start/end may be inclusive/exclusive/unbounded.

Iteration yields keys in ascending user_key order and must:

- not return duplicates
- respect snapshot visibility
- respect tombstones

Internal iteration is over internal keys; the dedup layer emits user keys.
