#![deny(warnings)]

use granitedb::{DB, Options, Range, SyncMode};
use tempfile::tempdir;

#[test]
fn column_families_persist_and_isolate_keys() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    {
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

        let cf1 = db.create_column_family("cf1").unwrap();
        db.put(b"k", b"default").unwrap();
        db.put_cf(&cf1, b"k", b"cf1").unwrap();

        db.close().unwrap();
    }

    let db2 = DB::open(
        &path,
        Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1024 * 1024,
            l0_slowdown_trigger: 1000,
            l0_stop_trigger: 2000,
            ..Options::default()
        },
    )
    .unwrap();

    let cf1 = db2.column_family("cf1").unwrap().unwrap();
    assert_eq!(db2.get(b"k").unwrap(), Some(b"default".to_vec()));
    assert_eq!(db2.get_cf(&cf1, b"k").unwrap(), Some(b"cf1".to_vec()));

    let cf1_items: Vec<_> = db2.iter_cf(&cf1, Range::all()).unwrap().collect();
    assert_eq!(cf1_items, vec![(b"k".to_vec(), b"cf1".to_vec())]);

    db2.close().unwrap();
}
