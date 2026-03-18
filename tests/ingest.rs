#![deny(warnings)]

use std::fs;

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn ingest_sst_files_imports_data() {
    let dir = tempdir().unwrap();
    let src_path = dir.path().join("src_db");
    let dst_path = dir.path().join("dst_db");

    {
        let db = DB::open(
            &src_path,
            Options {
                sync: SyncMode::No,
                memtable_max_bytes: 1,
                l0_slowdown_trigger: 1000,
                l0_stop_trigger: 2000,
                ..Options::default()
            },
        )
        .unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.close().unwrap();
    }

    let mut sst_files = Vec::new();
    for e in fs::read_dir(&src_path).unwrap() {
        let p = e.unwrap().path();
        if p.extension().and_then(|s| s.to_str()) == Some("sst") {
            sst_files.push(p);
        }
    }
    assert!(!sst_files.is_empty());

    let db2 = DB::open(
        &dst_path,
        Options {
            sync: SyncMode::No,
            ..Options::default()
        },
    )
    .unwrap();
    db2.ingest_sst_files(&sst_files, false).unwrap();

    assert_eq!(db2.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db2.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    db2.close().unwrap();
}
