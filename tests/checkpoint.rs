#![deny(warnings)]

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn checkpoint_can_be_opened_as_db() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let checkpoint_dir = dir.path().join("checkpoint");

    let db = DB::open(
        &path,
        Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 1000,
            l0_stop_trigger: 2000,
            ..Options::default()
        },
    )
    .unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    db.create_checkpoint(&checkpoint_dir).unwrap();

    let db2 = DB::open(
        &checkpoint_dir,
        Options {
            sync: SyncMode::No,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(db2.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db2.get(b"b").unwrap(), Some(b"2".to_vec()));
    db2.close().unwrap();

    db.close().unwrap();
}
