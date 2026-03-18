use criterion::{Criterion, black_box, criterion_group, criterion_main};
use granitedb::{DB, Options, Range, SyncMode};
use tempfile::tempdir;

fn bench_put_get(c: &mut Criterion) {
    c.bench_function("put_get", |b| {
        b.iter(|| {
            let dir = tempdir().unwrap();
            let path = dir.path().join("db");
            let opts = Options {
                sync: SyncMode::No,
                memtable_max_bytes: 1024 * 1024,
                l0_slowdown_trigger: 8,
                l0_stop_trigger: 16,
            };
            let db = DB::open(&path, opts).unwrap();
            for i in 0..100u32 {
                let k = format!("k{i}").into_bytes();
                let v = format!("v{i}").into_bytes();
                db.put(&k, &v).unwrap();
            }
            for i in 0..100u32 {
                let k = format!("k{i}").into_bytes();
                let got = db.get(&k).unwrap();
                black_box(got);
            }
            let items: Vec<(Vec<u8>, Vec<u8>)> = db.iter(Range::all()).unwrap().collect();
            black_box(items);
            db.close().unwrap();
        })
    });
}

criterion_group!(benches, bench_put_get);
criterion_main!(benches);
