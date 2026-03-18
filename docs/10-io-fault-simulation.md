# 10. Disk / IO Fault Simulation

This phase validates that GraniteDB remains correct under IO failures by injecting deterministic IO errors at persistence boundaries.

## 10.1 Injected Failure Modes

GraniteDB supports injected IO errors via failpoints:

- `GRANITEDB_FAILPOINT`: failpoint name
- `GRANITEDB_FAILPOINT_ACTION=ioerr`: return an IO error at that point
- `GRANITEDB_FAILPOINT_ACTION=partial:<n>`: write the first `<n>` bytes and then return a write error
- `GRANITEDB_FAILPOINT_ACTION=torn_abort:<n>`: write the first `<n>` bytes and then abort the process
- `GRANITEDB_FAILPOINT_ACTION=corrupt_abort:<i>`: flip one byte at index `<i>` of the buffer, write, then abort
- `GRANITEDB_FAILPOINT_ACTION=diskfull:<n>`: write the first `<n>` bytes and then return a write error
- `GRANITEDB_FAILPOINT_OP=<n>`: optional op index selector (write op index)

Crash injection still uses:

- `GRANITEDB_FAILPOINT_ACTION=abort`

## 10.2 IO Failure Boundaries

WAL:

- `wal:write_header`
- `wal:write_payload`
- `wal:write_padding`
- `wal:sync`

Manifest:

- `manifest:write_header`
- `manifest:write_payload`
- `manifest:write_padding`
- `manifest:before_append`
- `manifest:after_append`
- `manifest:sync`

SSTable:

- `sst:write_data`
- `sst:write_trailer`
- `sst:write_index`
- `sst:write_footer`
- `sst:sync`

Flush:

- `flush:before_new_wal_create`
- `flush:before_sst_finish`
- `flush:before_sst_rename`
- `flush:before_dir_sync`
- `flush:before_manifest_edit`

Compaction:

- `compaction:before_sst_finish`
- `compaction:before_sst_rename`
- `compaction:before_dir_sync`
- `compaction:before_manifest_edit`

## 10.3 Correctness Requirements Under IO Errors

- The write that returns an error MUST NOT become visible after restart.
- Recovery must succeed using:
  - Manifest replay (stop at first invalid record)
  - WAL replay (stop at first invalid record)

## 10.4 Tests

Run the IO fault tests:

```bash
cargo test --test io_faults -- --nocapture
```
