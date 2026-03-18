#![deny(warnings)]

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn transaction_commit_writes_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let db = DB::open(
        &path,
        Options {
            sync: SyncMode::No,
            ..Options::default()
        },
    )
    .unwrap();

    let mut tx = db.begin_transaction().unwrap();
    tx.put(b"k", b"v");
    tx.commit().unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    db.close().unwrap();
}

#[test]
fn transaction_conflicts_on_concurrent_update() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let db = DB::open(
        &path,
        Options {
            sync: SyncMode::No,
            ..Options::default()
        },
    )
    .unwrap();

    db.put(b"k", b"v1").unwrap();

    let mut tx = db.begin_transaction().unwrap();
    assert_eq!(tx.get(b"k").unwrap(), Some(b"v1".to_vec()));

    db.put(b"k", b"v2").unwrap();
    tx.put(b"k", b"txn");

    let err = tx.commit().unwrap_err();
    assert!(format!("{err}").contains("transaction conflict"));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    db.close().unwrap();
}

#[test]
fn transaction_conflict_check_is_cf_aware() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let db = DB::open(
        &path,
        Options {
            sync: SyncMode::No,
            ..Options::default()
        },
    )
    .unwrap();

    let cf1 = db.create_column_family("cf1").unwrap();
    db.put_cf(&cf1, b"k", b"v1").unwrap();

    let mut tx = db.begin_transaction().unwrap();
    assert_eq!(tx.get_cf(&cf1, b"k").unwrap(), Some(b"v1".to_vec()));

    db.put(b"k", b"default").unwrap();
    tx.put_cf(&cf1, b"x", b"y");
    tx.commit().unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"default".to_vec()));
    assert_eq!(db.get_cf(&cf1, b"x").unwrap(), Some(b"y".to_vec()));
    db.close().unwrap();
}
