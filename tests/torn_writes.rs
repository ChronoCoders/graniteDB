#![deny(warnings)]

use granitedb::{DB, Options, SyncMode};
use std::ops::Bound;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn torn_wal_payload_does_not_corrupt() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("db");
    let case = base.join("case");

    let status = Command::new(env!("CARGO_BIN_EXE_crash_worker"))
        .arg(&case)
        .arg("1")
        .arg("10")
        .arg("yes")
        .arg("random")
        .env("GRANITEDB_FAILPOINT", "wal:write_payload")
        .env("GRANITEDB_FAILPOINT_OP", "0")
        .env("GRANITEDB_FAILPOINT_ACTION", "torn_abort:1")
        .status()
        .unwrap();
    assert!(!status.success());

    let db = DB::open(
        &case,
        Options {
            sync: SyncMode::Yes,
            ..Options::default()
        },
    )
    .unwrap();
    let _items: Vec<(Vec<u8>, Vec<u8>)> = db
        .iter(granitedb::Range {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        })
        .unwrap()
        .collect();
    db.close().unwrap();
}
