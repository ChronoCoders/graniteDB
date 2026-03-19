#![deny(warnings)]

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use granitedb::{DB, Options, SyncMode, WriteBatch};

fn main() {
    let mut args = env::args().skip(1);
    let db_path: PathBuf = args.next().expect("db path").into();
    let seed: u64 = args.next().expect("seed").parse().expect("seed u64");
    let op_count: u64 = args
        .next()
        .expect("op_count")
        .parse()
        .expect("op_count u64");
    let sync = match args.next().as_deref() {
        Some("yes") => SyncMode::Yes,
        Some("no") => SyncMode::No,
        _ => SyncMode::Yes,
    };
    let scenario = args.next().unwrap_or_else(|| "random".into());

    let opts = match scenario.as_str() {
        "flush" => Options {
            sync,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 1000,
            l0_stop_trigger: 2000,
            ..Options::default()
        },
        "compaction" => Options {
            sync,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 1,
            l0_stop_trigger: 2,
            ..Options::default()
        },
        "ioerr_flush" => Options {
            sync,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 1000,
            l0_stop_trigger: 2000,
            ..Options::default()
        },
        "ioerr_compaction" => Options {
            sync,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 1,
            l0_stop_trigger: 2,
            ..Options::default()
        },
        _ => Options {
            sync,
            ..Options::default()
        },
    };

    let db = DB::open(&db_path, opts).expect("open db");

    let mut ack_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(db_path.join("acks.log"))
        .expect("open ack log");

    match scenario.as_str() {
        "padding" => run_padding_scenario(&db, seed, &mut ack_log),
        "flush" => run_flush_scenario(&db, seed, &mut ack_log),
        "compaction" => run_compaction_scenario(&db, seed, &mut ack_log),
        "ioerr_flush" => run_ioerr_flush_scenario(&db, seed, &mut ack_log),
        "ioerr_compaction" => run_ioerr_compaction_scenario(&db, seed, &mut ack_log),
        _ => run_random_scenario(&db, seed, op_count, &mut ack_log),
    };
    db.close().expect("close");
}

fn run_random_scenario(db: &DB, seed: u64, op_count: u64, ack_log: &mut std::fs::File) {
    let mut rng = XorShift64::new(seed);
    let mut write_idx = 0u64;
    for i in 0..op_count {
        let op = gen_op(&mut rng, i);
        match op {
            Op::Put { key, value } => {
                db.put(&key, &value).expect("put");
                log_acked_write(ack_log, write_idx);
                write_idx = write_idx.saturating_add(1);
            }
            Op::Delete { key } => {
                db.delete(&key).expect("delete");
                log_acked_write(ack_log, write_idx);
                write_idx = write_idx.saturating_add(1);
            }
            Op::BatchPut2 { k1, v1, k2, v2 } => {
                let batch = WriteBatch::default().put(k1, v1).put(k2, v2);
                db.write_batch(batch).expect("batch");
                log_acked_write(ack_log, write_idx);
                write_idx = write_idx.saturating_add(1);
            }
            Op::Get(key) => {
                let _ = db.get(&key).expect("get");
            }
        }
    }
}

fn run_padding_scenario(db: &DB, seed: u64, ack_log: &mut std::fs::File) {
    let mut value = vec![0u8; 32_730];
    value[..8].copy_from_slice(&seed.to_le_bytes());
    db.put(b"k", &value).expect("put large");
    log_acked_write(ack_log, 0);
    db.put(b"x", b"y").expect("put small");
    log_acked_write(ack_log, 1);
}

fn run_flush_scenario(db: &DB, seed: u64, ack_log: &mut std::fs::File) {
    let value = format!("flush:{seed}").into_bytes();
    db.put(b"a", &value).expect("put flush");
    log_acked_write(ack_log, 0);
    let value2 = format!("flush2:{seed}").into_bytes();
    db.put(b"b", &value2).expect("put flush 2");
    log_acked_write(ack_log, 1);
}

fn run_compaction_scenario(db: &DB, seed: u64, ack_log: &mut std::fs::File) {
    let v1 = format!("c1:{seed}").into_bytes();
    let v2 = format!("c2:{seed}").into_bytes();
    let v3 = format!("c3:{seed}").into_bytes();
    db.put(b"a", &v1).expect("put 1");
    log_acked_write(ack_log, 0);
    db.put(b"b", &v2).expect("put 2");
    log_acked_write(ack_log, 1);
    db.put(b"c", &v3).expect("put 3");
    log_acked_write(ack_log, 2);
}

fn run_ioerr_flush_scenario(db: &DB, seed: u64, ack_log: &mut std::fs::File) {
    let value = format!("flush:{seed}").into_bytes();
    db.put(b"a", &value).expect("put a");
    log_acked_write(ack_log, 0);
    let err = db.put(b"b", b"2").is_err();
    if !err {
        panic!("expected io error");
    }
}

fn run_ioerr_compaction_scenario(db: &DB, seed: u64, ack_log: &mut std::fs::File) {
    let v1 = format!("c1:{seed}").into_bytes();
    let v2 = format!("c2:{seed}").into_bytes();
    db.put(b"a", &v1).expect("put 1");
    log_acked_write(ack_log, 0);
    db.put(b"b", &v2).expect("put 2");
    log_acked_write(ack_log, 1);
    let err = db.put(b"c", b"3").is_err();
    if !err {
        panic!("expected io error");
    }
}

fn log_acked_write(ack_log: &mut std::fs::File, write_idx: u64) {
    writeln!(ack_log, "{write_idx}").expect("write ack log");
    ack_log.sync_data().expect("sync ack log");
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
    Get(Vec<u8>),
}

fn gen_op(rng: &mut XorShift64, idx: u64) -> Op {
    let choice = (rng.next_u64() % 10) as u8;
    let key_id = (rng.next_u64() % 8) as u8;
    let key = format!("k{key_id}").into_bytes();
    match choice {
        0 => Op::Delete { key },
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
        2..=4 => Op::Get(key),
        _ => Op::Put {
            key,
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
