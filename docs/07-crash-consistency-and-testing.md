# 7. Crash Consistency & Testing (The Production Barrier)

“Production-grade” is earned by crash testing across every persistence boundary.

## 7.1 Crash Consistency Model

Authoritative state after restart:

- Live SSTables are exactly those referenced by the recovered Manifest state.
- WAL is replayed up to the first invalid record; tail is truncated.
- MemTables are rebuilt from WAL replay; they are not trusted across restarts.

Ordering:

1. CURRENT → Manifest replay (stop at first invalid record)
2. WAL replay (stop at first invalid record)
3. Rebuild in-memory state

## 7.2 Invariants (Must Hold After Any Crash)

- Atomic batch: no partial batch visible.
- Ack boundary respected:
  - Sync::Yes: acknowledged writes survive crash.
  - Sync::No: acknowledged writes may be lost, but state is consistent.
- Manifest authority: no SST outside Manifest affects reads.
- No resurrection: deletes are not undone by compaction/tombstone dropping.
- Iterators: no duplicate user keys, correct snapshot filtering.

## 7.3 Required Crash Points (Failpoint Matrix)

Inject crash (process kill) at least at:

WAL:

- after fragment header write
- after fragment payload write
- after block padding
- before/after fsync

Flush / SST:

- during temp SST write (mid-block)
- after SST fsync
- after rename
- after directory fsync
- before/after manifest append
- before/after manifest fsync

Compaction:

- same as flush plus input-file deletion timing

Manifest/CURRENT:

- during manifest record append
- after manifest fsync
- during CURRENT tmp write
- after CURRENT rename
- after directory fsync

Disk-full and IO-error injection:

- write returns short / error
- fsync returns error
- rename returns error

## 7.4 Crash Test Harness (Required)

- Generate random workloads:
  - put/get/delete/write_batch/iter
  - snapshots once implemented
- Maintain a reference model:
  - in-memory map with seq visibility and tombstones
- Run:
  - execute N operations with random failpoints that may kill the process
  - reopen DB and validate:
    - contents vs reference at the appropriate ack boundary
    - iterator properties (sorted, deduped, snapshot-consistent)
- Determinism:
  - log seed + operation stream so failures reproduce exactly.

## 7.5 Corruption Tests

- Flip bits in:
  - WAL tail
  - Manifest tail
  - SST data blocks and footer
- Verify:
  - detection (checksum failure)
  - safe truncation (WAL/Manifest)
  - safe refusal / error (SST checksum/footer mismatch) without silent corruption
