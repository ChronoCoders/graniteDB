#![deny(warnings)]

use std::fs;
use std::process::Command;

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn repair_rebuilds_manifest_from_sstables() {
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
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.close().unwrap();
    }

    let entries: Vec<_> = fs::read_dir(&path).unwrap().map(|e| e.unwrap()).collect();
    for e in entries {
        let p = e.path();
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name == "CURRENT" || name.starts_with("MANIFEST-") {
            let _ = fs::remove_file(p);
        }
    }

    let status = Command::new(env!("CARGO_BIN_EXE_repair"))
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());

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
    assert_eq!(db2.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db2.get(b"b").unwrap(), Some(b"2".to_vec()));
    db2.close().unwrap();
}
