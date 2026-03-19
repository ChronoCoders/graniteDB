#![deny(warnings)]

use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

use granitedb::{DB, Options, SyncMode};
use tempfile::tempdir;

#[test]
fn fault_matrix_covers_write_and_sync_boundaries() {
    let dir = tempdir().unwrap();
    let base_path = dir.path().join("db");

    let seed = 424242u64;
    let cases = [
        (
            "padding",
            1u64,
            &[
                "wal:write_padding",
                "wal:write_header",
                "wal:write_payload",
                "wal:sync",
            ][..],
        ),
        (
            "flush",
            1u64,
            &[
                "sst:write_data",
                "sst:write_trailer",
                "sst:write_index",
                "sst:write_filter",
                "sst:write_filter_trailer",
                "sst:write_footer",
                "sst:sync",
                "manifest:write_header",
                "manifest:write_payload",
                "manifest:sync",
                "flush:before_sst_rename",
                "flush:after_sst_rename",
                "flush:before_dir_sync",
                "flush:after_dir_sync",
                "flush:before_manifest_edit",
                "flush:after_manifest_edit",
            ][..],
        ),
        (
            "compaction",
            1u64,
            &[
                "sst:write_data",
                "sst:write_trailer",
                "sst:write_index",
                "sst:write_filter",
                "sst:write_filter_trailer",
                "sst:write_footer",
                "sst:sync",
                "manifest:write_header",
                "manifest:write_payload",
                "manifest:sync",
                "compaction:before_sst_rename",
                "compaction:after_sst_rename",
                "compaction:before_dir_sync",
                "compaction:after_dir_sync",
                "compaction:before_manifest_edit",
                "compaction:after_manifest_edit",
            ][..],
        ),
    ];

    for sync in [SyncMode::Yes] {
        for (scenario, crash_op, failpoints) in cases {
            for failpoint in failpoints {
                for action in actions_for_failpoint(failpoint) {
                    let case_path =
                        base_path.join(case_dir_name(scenario, failpoint, crash_op, action));
                    let status = Command::new(env!("CARGO_BIN_EXE_crash_worker"))
                        .arg(&case_path)
                        .arg(seed.to_string())
                        .arg("50")
                        .arg(match sync {
                            SyncMode::Yes => "yes",
                            SyncMode::No => "no",
                        })
                        .arg(scenario)
                        .env("GRANITEDB_FAILPOINT", failpoint)
                        .env("GRANITEDB_FAILPOINT_OP", crash_op.to_string())
                        .env("GRANITEDB_FAILPOINT_ACTION", action)
                        .status()
                        .expect("spawn crash worker");

                    assert!(
                        !status.success(),
                        "expected non-success for scenario={scenario} failpoint={failpoint} op={crash_op} action={action}",
                    );

                    let acked_writes = read_acked_writes(&case_path);
                    let expected = expected_states(scenario, seed, acked_writes);
                    let db = DB::open(
                        &case_path,
                        Options {
                            sync,
                            ..Options::default()
                        },
                    )
                    .unwrap();
                    assert!(expected.iter().any(|m| db_matches_model(scenario, &db, m)));
                    db.close().unwrap();
                }
            }
        }
    }
}

fn expected_states(
    scenario: &str,
    seed: u64,
    acked_writes: u64,
) -> Vec<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut out = Vec::new();
    for i in 0..6u64 {
        out.push(compute_expected_state(
            scenario,
            seed,
            acked_writes.saturating_add(i),
        ));
    }
    out
}

fn compute_expected_state(
    scenario: &str,
    seed: u64,
    writes_to_apply: u64,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut state: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    match scenario {
        "padding" => {
            if writes_to_apply >= 1 {
                let mut value = vec![0u8; 32_730];
                value[..8].copy_from_slice(&seed.to_le_bytes());
                state.insert(b"k".to_vec(), Some(value));
            }
            if writes_to_apply >= 2 {
                state.insert(b"x".to_vec(), Some(b"y".to_vec()));
            }
        }
        "flush" => {
            if writes_to_apply >= 1 {
                state.insert(b"a".to_vec(), Some(format!("flush:{seed}").into_bytes()));
            }
            if writes_to_apply >= 2 {
                state.insert(b"b".to_vec(), Some(format!("flush2:{seed}").into_bytes()));
            }
        }
        "compaction" => {
            if writes_to_apply >= 1 {
                state.insert(b"a".to_vec(), Some(format!("c1:{seed}").into_bytes()));
            }
            if writes_to_apply >= 2 {
                state.insert(b"b".to_vec(), Some(format!("c2:{seed}").into_bytes()));
            }
            if writes_to_apply >= 3 {
                state.insert(b"c".to_vec(), Some(format!("c3:{seed}").into_bytes()));
            }
        }
        _ => {}
    }
    state
}

fn db_matches_model(scenario: &str, db: &DB, model: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> bool {
    for &k in scenario_keys(scenario) {
        let expected = model.get(k).cloned().unwrap_or(None);
        if db.get(k).ok().flatten() != expected {
            return false;
        }
    }
    true
}

fn actions_for_failpoint(failpoint: &str) -> &'static [&'static str] {
    static WRITE_ACTIONS: &[&str] = &[
        "abort",
        "torn_abort:1",
        "corrupt_abort:1",
        "partial:1",
        "diskfull:1",
        "ioerr",
    ];
    static SYNC_ACTIONS: &[&str] = &["ioerr", "partial:1", "diskfull:1"];
    static IOERR_ACTIONS: &[&str] = &["abort", "ioerr"];

    if failpoint.contains(":write_") {
        return WRITE_ACTIONS;
    }
    if failpoint.ends_with(":sync") {
        return SYNC_ACTIONS;
    }
    if failpoint == "manifest:checkpoint_sync" {
        return SYNC_ACTIONS;
    }
    if failpoint.starts_with("flush:") || failpoint.starts_with("compaction:") {
        return IOERR_ACTIONS;
    }
    WRITE_ACTIONS
}

fn scenario_keys(scenario: &str) -> &'static [&'static [u8]] {
    static PADDING_KEYS: &[&[u8]] = &[b"k", b"x"];
    static FLUSH_KEYS: &[&[u8]] = &[b"a", b"b"];
    static COMPACTION_KEYS: &[&[u8]] = &[b"a", b"b", b"c"];
    match scenario {
        "padding" => PADDING_KEYS,
        "flush" => FLUSH_KEYS,
        "compaction" => COMPACTION_KEYS,
        _ => &[],
    }
}

fn case_dir_name(scenario: &str, failpoint: &str, crash_op: u64, action: &str) -> String {
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
    s.push('_');
    for ch in action.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => s.push(ch),
            _ => s.push('_'),
        }
    }
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
