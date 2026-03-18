# 8. Operations

## 8.1 Deployment Notes

- SSD recommended.
- Understand your filesystem fsync semantics (and test them).
- Sync::Yes requires:
  - fsync(WAL) for acknowledged writes
  - fsync(SST) + rename + fsync(dir) for file installs
  - fsync(Manifest) for metadata installs

## 8.2 Startup GC (Leak Prevention)

On startup:

- List `*.sst` and `*.sst.tmp`
- Build live set from recovered Manifest
- Delete:
  - any `*.sst` not referenced by Manifest
  - any `*.tmp` artifacts
- Never delete referenced SSTs.

## 8.3 Disk Full / IO Errors

Rules:

- Any IO error during WAL append in Sync::Yes must fail the write (no ack).
- Any IO error during flush/compaction must not install partial output:
  - outputs remain unreferenced and are cleaned on restart.

Expose:

- explicit error results to caller
- metrics for disk-full, io-errors, stall time

## 8.4 Backups

File-level backup must produce a self-consistent set.

Safe options:

- Preferred: implement a “checkpoint” operation that:
  - forces MemTable flush
  - rotates WAL
  - ensures Manifest is durable
  - then copies only referenced SSTs + CURRENT + MANIFEST (+ any required WALs)
- If copying a live DB directory without checkpoint:
  - you must copy a consistent prefix of WAL and the active manifest plus all referenced SSTs; this is easy to get wrong and is not recommended in v1.

## 8.5 Roadmap Constraints

No async IO, no multi-writer, no advanced compaction heuristics until crash tests are comprehensive and stable.
