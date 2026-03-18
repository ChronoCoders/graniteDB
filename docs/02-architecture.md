# 2. Architecture

## 2.1 Components

- WAL (Write-Ahead Log)
- MemTable (mutable)
- Immutable MemTables (flush queue)
- SSTables (disk)
- VersionSet (in-memory metadata state)
- Manifest (persistent metadata log)
- Compaction engine (background)

## 2.2 Data Flow

### Write Path

```text
Client → (assign seq) → WAL append → (optional WAL fsync) → MemTable apply → ACK
```

### Flush

```text
Immutable MemTable → SST temp file → fsync(file) → rename → fsync(dir)
→ Manifest append(AddFile) → fsync(manifest) → install VersionSet
```

### Read Path

```text
MemTable → Immutable MemTables → L0 SSTs (overlapping) → L1+ SSTs (non-overlapping)
```

## 2.3 Threading Model (Initial)

- Single writer thread:
  - owns sequence assignment, WAL append ordering, MemTable mutation, VersionSet installs.
- One background thread:
  - flush and compaction work.
- Concurrent readers:
  - read MemTables and VersionSet under short-lived read guards.

No multi-writer until crash tests are exhaustive and stable.

## 2.4 Key Invariants (Global)

- WAL is the source of truth for acknowledged writes not yet persisted to SST + Manifest.
- Manifest is authoritative for the set of live SSTs.
- Files are written before they are referenced by Manifest.
- Recovery replays: CURRENT → Manifest → WAL; stops at first invalid log record.
