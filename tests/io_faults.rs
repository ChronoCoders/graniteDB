#![deny(warnings)]

use std::process::Command;

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn flush_io_error_does_not_ack_or_persist_next_write() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    let status = Command::new(env!("CARGO_BIN_EXE_crash_worker"))
        .arg(&path)
        .arg("123")
        .arg("0")
        .arg("yes")
        .arg("ioerr_flush")
        .env("GRANITEDB_FAILPOINT", "flush:before_sst_finish")
        .env("GRANITEDB_FAILPOINT_ACTION", "ioerr")
        .status()
        .unwrap();
    assert!(status.success());

    let db = DB::open(
        &path,
        Options {
            sync: SyncMode::Yes,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 1000,
            l0_stop_trigger: 2000,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.get(b"a").unwrap(),
        Some(format!("flush:{}", 123u64).into_bytes())
    );
    assert_eq!(db.get(b"b").unwrap(), None);
    db.close().unwrap();
}

#[test]
fn compaction_io_error_does_not_ack_or_persist_next_write() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    let status = Command::new(env!("CARGO_BIN_EXE_crash_worker"))
        .arg(&path)
        .arg("7")
        .arg("0")
        .arg("yes")
        .arg("ioerr_compaction")
        .env("GRANITEDB_FAILPOINT", "compaction:before_sst_finish")
        .env("GRANITEDB_FAILPOINT_ACTION", "ioerr")
        .status()
        .unwrap();
    assert!(status.success());

    let db = DB::open(
        &path,
        Options {
            sync: SyncMode::Yes,
            memtable_max_bytes: 1024 * 1024 * 1024,
            l0_slowdown_trigger: 0,
            l0_stop_trigger: 0,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.get(b"a").unwrap(),
        Some(format!("c1:{}", 7u64).into_bytes())
    );
    assert_eq!(
        db.get(b"b").unwrap(),
        Some(format!("c2:{}", 7u64).into_bytes())
    );
    assert_eq!(db.get(b"c").unwrap(), None);
    db.close().unwrap();
}
