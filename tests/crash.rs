#![deny(warnings)]

use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound;
use std::process::Command;

use granitedb::{DB, Options, Range, SyncMode};
use tempfile::tempdir;

#[test]
fn crash_in_wal_write_does_not_corrupt_and_preserves_prefix() {
    let dir = tempdir().unwrap();
    let base_path = dir.path().join("db");

    let seed = 123456789u64;
    let op_count = 50u64;
    for failpoint in [
        "wal:after_header",
        "wal:after_payload",
        "wal:before_sync",
        "wal:after_sync",
    ] {
        for crash_op in [0u64, 1, 2, 7, 13] {
            run_crash_case(
                &base_path,
                seed,
                op_count,
                crash_op,
                failpoint,
                "random",
                SyncMode::Yes,
            );
        }
    }

    for failpoint in [
        "flush:before_new_wal_create",
        "flush:after_new_wal_create",
        "flush:before_sst_finish",
        "flush:after_sst_finish",
        "flush:before_sst_rename",
        "flush:after_sst_rename",
        "flush:before_dir_sync",
        "flush:after_dir_sync",
        "flush:before_manifest_edit",
        "flush:after_manifest_edit",
        "manifest:before_append",
        "manifest:after_append",
        "manifest:before_sync",
        "manifest:after_sync",
    ] {
        run_crash_case(
            &base_path,
            seed,
            op_count,
            1,
            failpoint,
            "flush",
            SyncMode::Yes,
        );
    }

    for failpoint in [
        "compaction:before_sst_finish",
        "compaction:after_sst_finish",
        "compaction:before_sst_rename",
        "compaction:after_sst_rename",
        "compaction:before_dir_sync",
        "compaction:after_dir_sync",
        "compaction:before_manifest_edit",
        "compaction:after_manifest_edit",
        "manifest:before_append",
        "manifest:after_append",
        "manifest:before_sync",
        "manifest:after_sync",
    ] {
        run_crash_case(
            &base_path,
            seed,
            op_count,
            1,
            failpoint,
            "compaction",
            SyncMode::Yes,
        );
    }
}

fn run_crash_case(
    base_path: &std::path::Path,
    seed: u64,
    op_count: u64,
    crash_op: u64,
    failpoint: &str,
    scenario: &str,
    sync: SyncMode,
) {
    run_case(
        base_path,
        CrashCase {
            seed,
            op_count,
            crash_op,
            failpoint: failpoint.to_string(),
            scenario: scenario.to_string(),
            sync,
            action: "abort".to_string(),
        },
    );
}

struct CrashCase {
    seed: u64,
    op_count: u64,
    crash_op: u64,
    failpoint: String,
    scenario: String,
    sync: SyncMode,
    action: String,
}

fn run_case(base_path: &std::path::Path, case: CrashCase) {
    let case_path = base_path.join(case_dir_name(
        &case.failpoint,
        case.crash_op,
        &case.scenario,
    ));
    let status = Command::new(env!("CARGO_BIN_EXE_crash_worker"))
        .arg(&case_path)
        .arg(case.seed.to_string())
        .arg(case.op_count.to_string())
        .arg(match case.sync {
            SyncMode::Yes => "yes",
            SyncMode::No => "no",
        })
        .arg(&case.scenario)
        .env("GRANITEDB_FAILPOINT", &case.failpoint)
        .env("GRANITEDB_FAILPOINT_OP", case.crash_op.to_string())
        .env("GRANITEDB_FAILPOINT_ACTION", &case.action)
        .status()
        .expect("spawn crash worker");

    assert!(
        !status.success(),
        "expected crash for failpoint={} op={} scenario={} action={}",
        case.failpoint,
        case.crash_op,
        case.scenario,
        case.action
    );

    let acked_writes = read_acked_writes(&case_path);
    let mut expected = Vec::new();
    for i in 0..6u64 {
        expected.push(compute_expected_state(
            case.seed,
            case.op_count,
            acked_writes.saturating_add(i),
            &case.scenario,
        ));
    }
    let db = DB::open(
        &case_path,
        Options {
            sync: case.sync,
            ..Options::default()
        },
    )
    .unwrap();
    let ok_any = expected.iter().any(|m| db_matches_model(&db, m));
    assert!(ok_any);
    db.close().unwrap();
}

fn db_matches_model(db: &DB, model: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> bool {
    for (k, expected) in model {
        let got = match db.get(k) {
            Ok(v) => v,
            Err(_) => return false,
        };
        if got.as_ref() != expected.as_ref() {
            return false;
        }
    }

    let iter_items: Vec<(Vec<u8>, Vec<u8>)> = match db.iter(Range {
        start: Bound::Unbounded,
        end: Bound::Unbounded,
    }) {
        Ok(it) => it.collect(),
        Err(_) => return false,
    };

    let mut expected_items: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|vv| (k.clone(), vv.clone())))
        .collect();
    expected_items.sort_by(|a, b| a.0.cmp(&b.0));
    iter_items == expected_items
}

fn compute_expected_state(
    seed: u64,
    op_count: u64,
    writes_to_apply: u64,
    scenario: &str,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    match scenario {
        "padding" => {
            let mut state = BTreeMap::new();
            if writes_to_apply >= 1 {
                let mut value = vec![0u8; 32_738];
                value[..8].copy_from_slice(&seed.to_le_bytes());
                state.insert(b"k".to_vec(), Some(value));
            }
            if writes_to_apply >= 2 {
                state.insert(b"x".to_vec(), Some(b"y".to_vec()));
            }
            state
        }
        "flush" => {
            let mut state = BTreeMap::new();
            if writes_to_apply >= 1 {
                state.insert(b"a".to_vec(), Some(format!("flush:{seed}").into_bytes()));
            }
            if writes_to_apply >= 2 {
                state.insert(b"b".to_vec(), Some(format!("flush2:{seed}").into_bytes()));
            }
            state
        }
        "compaction" => {
            let mut state = BTreeMap::new();
            if writes_to_apply >= 1 {
                state.insert(b"a".to_vec(), Some(format!("c1:{seed}").into_bytes()));
            }
            if writes_to_apply >= 2 {
                state.insert(b"b".to_vec(), Some(format!("c2:{seed}").into_bytes()));
            }
            if writes_to_apply >= 3 {
                state.insert(b"c".to_vec(), Some(format!("c3:{seed}").into_bytes()));
            }
            state
        }
        _ => {
            let mut rng = XorShift64::new(seed);
            let mut state: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
            let mut write_idx = 0u64;
            for idx in 0..op_count {
                let op = gen_op(&mut rng, idx);
                let is_write = !matches!(op, Op::Get);
                if is_write && write_idx == writes_to_apply {
                    break;
                }
                match op {
                    Op::Put { key, value } => {
                        state.insert(key, Some(value));
                    }
                    Op::Delete { key } => {
                        state.insert(key, None);
                    }
                    Op::BatchPut2 { k1, v1, k2, v2 } => {
                        state.insert(k1, Some(v1));
                        state.insert(k2, Some(v2));
                    }
                    Op::Get => {}
                }
                if is_write {
                    write_idx = write_idx.saturating_add(1);
                }
            }
            state
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    BatchPut2 {
        k1: Vec<u8>,
        v1: Vec<u8>,
        k2: Vec<u8>,
        v2: Vec<u8>,
    },
    Get,
}

fn gen_op(rng: &mut XorShift64, idx: u64) -> Op {
    let choice = (rng.next_u64() % 10) as u8;
    let key_id = (rng.next_u64() % 8) as u8;
    match choice {
        0 => Op::Delete {
            key: format!("k{key_id}").into_bytes(),
        },
        1 => {
            let k1 = format!("k{}", (key_id % 8)).into_bytes();
            let k2 = format!("k{}", ((key_id + 1) % 8)).into_bytes();
            Op::BatchPut2 {
                k1,
                v1: value_bytes(idx, 1),
                k2,
                v2: value_bytes(idx, 2),
            }
        }
        2..=4 => Op::Get,
        _ => Op::Put {
            key: format!("k{key_id}").into_bytes(),
            value: value_bytes(idx, 0),
        },
    }
}

fn value_bytes(idx: u64, salt: u8) -> Vec<u8> {
    format!("v{idx}:{salt}").into_bytes()
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 { 0x4d595df4d0f33173 } else { seed };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

fn case_dir_name(failpoint: &str, crash_op: u64, scenario: &str) -> String {
    let mut s = String::new();
    s.push_str("case_");
    s.push_str(scenario);
    s.push('_');
    for ch in failpoint.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => s.push(ch),
            _ => s.push('_'),
        }
    }
    s.push('_');
    s.push_str(&crash_op.to_string());
    s
}

fn read_acked_writes(case_path: &std::path::Path) -> u64 {
    let path = case_path.join("acks.log");
    let contents = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    contents.lines().count() as u64
}
