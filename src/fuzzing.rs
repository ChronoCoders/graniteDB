use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::manifest::Manifest;
use crate::options::SyncMode;
use crate::sstable::TableReader;
use crate::wal::Wal;

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn fuzz_wal(bytes: &[u8]) {
    let dir = make_temp_dir("wal");
    let path = dir.join("000001.log");
    if write_file(&path, bytes).is_err() {
        return;
    }
    let _ = Wal::open(&path, SyncMode::No);
    let _ = fs::remove_dir_all(dir);
}

pub fn fuzz_sstable(bytes: &[u8]) {
    let dir = make_temp_dir("sst");
    let path = dir.join("000001.sst");
    if write_file(&path, bytes).is_err() {
        return;
    }
    if let Ok(r) = TableReader::open_with_cache(&path, None) {
        let _ = r.scanner();
    }
    let _ = fs::remove_dir_all(dir);
}

pub fn fuzz_manifest(bytes: &[u8]) {
    let dir = make_temp_dir("manifest");
    let manifest_name = "MANIFEST-000001";
    let manifest_path = dir.join(manifest_name);
    if write_file(&manifest_path, bytes).is_err() {
        return;
    }
    let current_path = dir.join("CURRENT");
    if write_file(&current_path, format!("{manifest_name}\n").as_bytes()).is_err() {
        return;
    }
    let _ = Manifest::open(&dir, SyncMode::No);
    let _ = fs::remove_dir_all(dir);
}

fn make_temp_dir(prefix: &str) -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push("granitedb_fuzz");
    let _ = fs::create_dir_all(&p);
    p.push(format!("{prefix}_{}_{}", std::process::id(), id));
    let _ = fs::create_dir_all(&p);
    p
}

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    f.write_all(bytes)?;
    Ok(())
}
