#![deny(warnings)]

#[cfg(feature = "loom")]
mod loom_tests {
    use granitedb::sync::Arc;

    #[test]
    fn loom_memdb_concurrent_put_get() {
        loom::model(|| {
            let db = Arc::new(granitedb::memdb::MemDb::new());
            let db2 = db.clone();

            let t1 = loom::thread::spawn(move || {
                db2.put(b"a".to_vec(), b"1".to_vec()).unwrap();
            });

            let db3 = db.clone();
            let t2 = loom::thread::spawn(move || {
                let _ = db3.get(b"a").unwrap();
            });

            t1.join().unwrap();
            t2.join().unwrap();

            let v = db.get(b"a").unwrap();
            assert!(v.is_none() || v == Some(b"1".to_vec()));
        });
    }
}
