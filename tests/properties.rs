#![deny(warnings)]

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn get_property_stats_and_options() {
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

    db.put(b"a", b"1").unwrap();

    let stats = db.get_property("granitedb.stats").unwrap().unwrap();
    assert!(stats.contains("puts:"));
    let opts = db.get_property("granitedb.options").unwrap().unwrap();
    assert!(opts.contains("sync:"));

    db.close().unwrap();
}
