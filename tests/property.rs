#![deny(warnings)]

use std::collections::BTreeMap;

use granitedb::{DB, Options, Range, SyncMode};
use proptest::prelude::*;
use tempfile::tempdir;

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    IterAll,
    Restart,
}

fn op_strategy(include_restart: bool) -> BoxedStrategy<Op> {
    let key = prop::collection::vec(any::<u8>(), 0..8);
    let value = prop::collection::vec(any::<u8>(), 0..16);

    let base = prop_oneof![
        5 => (key.clone(), value).prop_map(|(k, v)| Op::Put(k, v)),
        2 => key.clone().prop_map(Op::Delete),
        3 => key.clone().prop_map(Op::Get),
        1 => Just(Op::IterAll),
    ];

    if include_restart {
        prop_oneof![8 => base, 1 => Just(Op::Restart)].boxed()
    } else {
        base.boxed()
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    #[test]
    fn db_matches_reference_model_no_flush(ops in prop::collection::vec(op_strategy(true), 1..200)) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");

        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1024 * 1024 * 1024,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
        };

        let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut db = DB::open(&path, opts.clone()).unwrap();

        for op in ops {
            match op {
                Op::Put(k, v) => {
                    db.put(&k, &v).unwrap();
                    model.insert(k, Some(v));
                }
                Op::Delete(k) => {
                    db.delete(&k).unwrap();
                    model.insert(k, None);
                }
                Op::Get(k) => {
                    let got = db.get(&k).unwrap();
                    let expected = model.get(&k).and_then(|v| v.clone());
                    prop_assert_eq!(got, expected);
                }
                Op::IterAll => {
                    let got: Vec<(Vec<u8>, Vec<u8>)> = db.iter(Range::all()).unwrap().collect();
                    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = model
                        .iter()
                        .filter_map(|(k, v)| v.as_ref().map(|vv| (k.clone(), vv.clone())))
                        .collect();
                    expected.sort_by(|a, b| a.0.cmp(&b.0));
                    prop_assert_eq!(got, expected);
                }
                Op::Restart => {
                    db.close().unwrap();
                    db = DB::open(&path, opts.clone()).unwrap();
                }
            }
        }
        db.close().unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, .. ProptestConfig::default() })]

    #[test]
    fn db_matches_reference_model_with_flush_and_compaction(ops in prop::collection::vec(op_strategy(true), 1..60)) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");

        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 64,
            l0_slowdown_trigger: 2,
            l0_stop_trigger: 4,
        };

        let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut db = DB::open(&path, opts.clone()).unwrap();

        for op in ops {
            match op {
                Op::Put(k, v) => {
                    db.put(&k, &v).unwrap();
                    model.insert(k, Some(v));
                }
                Op::Delete(k) => {
                    db.delete(&k).unwrap();
                    model.insert(k, None);
                }
                Op::Get(k) => {
                    let got = db.get(&k).unwrap();
                    let expected = model.get(&k).and_then(|v| v.clone());
                    prop_assert_eq!(got, expected);
                }
                Op::IterAll => {
                    let got: Vec<(Vec<u8>, Vec<u8>)> = db.iter(Range::all()).unwrap().collect();
                    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = model
                        .iter()
                        .filter_map(|(k, v)| v.as_ref().map(|vv| (k.clone(), vv.clone())))
                        .collect();
                    expected.sort_by(|a, b| a.0.cmp(&b.0));
                    prop_assert_eq!(got, expected);
                }
                Op::Restart => {
                    db.close().unwrap();
                    db = DB::open(&path, opts.clone()).unwrap();
                }
            }
        }
        db.close().unwrap();
    }
}
