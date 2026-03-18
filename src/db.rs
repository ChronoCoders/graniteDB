use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{GraniteError, Result};
use crate::failpoint;
use crate::internal_key::{
    ValueType, cmp_internal_key_bytes_unchecked, encode_internal_key, parse_internal_key,
};
use crate::manifest::{Manifest, VersionSet};
use crate::memtable::{GetDecision, MemTable};
use crate::options::{Options, SyncMode};
use crate::sstable::{TableBuilder, TableReader};
use crate::util::sync_parent_dir;
use crate::wal::Wal;
use crate::write_batch::{WriteBatch, WriteOp};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub read_seq: u64,
}

#[derive(Clone, Debug)]
pub struct Range {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl Range {
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

pub struct DbIterator {
    items: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for DbIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next()
    }
}

struct WriterState {
    manifest: Manifest,
    version: VersionSet,
    next_file_id: u64,
    wal_id: u64,
    wal: Wal,
    next_seq: u64,
    next_op_index: u64,
    memtable: MemTable,
    options: Options,
}

pub struct DB {
    path: PathBuf,
    writer: Mutex<WriterState>,
}

impl DB {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        let (manifest, version) = Manifest::open(&path, options.sync)?;
        startup_gc(&path, &version)?;
        let next_file_id = version.max_file_id().saturating_add(1).max(1);

        let mut memtable = MemTable::default();
        let mut next_seq = version.last_sequence.saturating_add(1).max(1);

        let wal_id = version.log_number.max(1);
        let wal_path = path.join(wal_file_name(wal_id));
        let (wal, recovery) = Wal::open(&wal_path, options.sync)?;
        for rec in recovery.records {
            let batch = decode_wal_record_to_batch(&rec)?;
            apply_batch_to_memtable(&mut memtable, &batch, &mut next_seq)?;
        }

        Ok(Self {
            path,
            writer: Mutex::new(WriterState {
                manifest,
                version,
                next_file_id,
                wal_id,
                wal,
                next_seq,
                next_op_index: 0,
                memtable,
                options,
            }),
        })
    }

    pub fn close(&self) -> Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        w.wal.sync()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let batch = WriteBatch::default().put(key.to_vec(), value.to_vec());
        self.write_batch(batch)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let batch = WriteBatch::default().delete(key.to_vec());
        self.write_batch(batch)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;

        let op_index = w.next_op_index;
        w.next_op_index = w.next_op_index.saturating_add(1);
        let _op_guard = failpoint::set_current_op_index(op_index);

        let record = encode_batch_as_wal_record(&batch);
        w.wal.append_logical_record(&record)?;
        if w.options.sync == SyncMode::Yes {
            w.wal.sync()?;
        }
        let mut next_seq = w.next_seq;
        apply_batch_to_memtable(&mut w.memtable, &batch, &mut next_seq)?;
        w.next_seq = next_seq;
        if w.memtable.approx_bytes() >= w.options.memtable_max_bytes {
            flush_memtable_to_l0(&self.path, &mut w)?;
        }
        if w.version.level0.len() > w.options.l0_slowdown_trigger {
            compact_l0_to_l1(&self.path, &mut w)?;
        }
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        let w = self
            .writer
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        let read_seq = w.next_seq.saturating_sub(1);
        Ok(Snapshot { read_seq })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_with_snapshot(key, Snapshot { read_seq: u64::MAX })
    }

    pub fn get_with_snapshot(&self, key: &[u8], snapshot: Snapshot) -> Result<Option<Vec<u8>>> {
        let w = self
            .writer
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        match w.memtable.get(key, snapshot.read_seq)? {
            Some(GetDecision::Value(v)) => Ok(Some(v)),
            Some(GetDecision::Deleted) => Ok(None),
            None => {
                for meta in w.version.level0.iter().rev() {
                    let path = self.path.join(sst_file_name(meta.file_id));
                    let mut reader = TableReader::open(path)?;
                    let seek = encode_internal_key(key, snapshot.read_seq, ValueType::Tombstone);
                    let got =
                        reader.get_by_seek_key_prefix(&seek, key, snapshot.read_seq, |vt, v| {
                            match vt {
                                0 => Some(GetDecision::Deleted),
                                1 => Some(GetDecision::Value(v.to_vec())),
                                _ => None,
                            }
                        })?;
                    match got {
                        Some(GetDecision::Value(v)) => return Ok(Some(v)),
                        Some(GetDecision::Deleted) => return Ok(None),
                        None => {}
                    }
                }
                for meta in &w.version.level1 {
                    if !meta_may_contain_user_key(meta, key)? {
                        continue;
                    }
                    let path = self.path.join(sst_file_name(meta.file_id));
                    let mut reader = TableReader::open(path)?;
                    let seek = encode_internal_key(key, snapshot.read_seq, ValueType::Tombstone);
                    let got =
                        reader.get_by_seek_key_prefix(&seek, key, snapshot.read_seq, |vt, v| {
                            match vt {
                                0 => Some(GetDecision::Deleted),
                                1 => Some(GetDecision::Value(v.to_vec())),
                                _ => None,
                            }
                        })?;
                    match got {
                        Some(GetDecision::Value(v)) => return Ok(Some(v)),
                        Some(GetDecision::Deleted) => return Ok(None),
                        None => {}
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn iter(&self, range: Range) -> Result<DbIterator> {
        self.iter_with_snapshot(range, Snapshot { read_seq: u64::MAX })
    }

    pub fn iter_with_snapshot(&self, range: Range, snapshot: Snapshot) -> Result<DbIterator> {
        let w = self
            .writer
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;

        let out = collect_visible_items(&self.path, &w, &range, snapshot)?;

        Ok(DbIterator {
            items: out.into_iter(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn startup_gc(db_dir: &Path, version: &VersionSet) -> Result<()> {
    let entries = fs::read_dir(db_dir)?;
    for e in entries {
        let e = e?;
        let path = e.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sst") {
            continue;
        }
        let stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s,
            None => continue,
        };
        let file_id = match stem.parse::<u64>() {
            Ok(id) => id,
            Err(_) => continue,
        };
        if !version.contains_file_id(file_id) {
            let _ = fs::remove_file(path);
        }
    }

    let entries = fs::read_dir(db_dir)?;
    for e in entries {
        let e = e?;
        let path = e.path();
        if path.extension().and_then(|s| s.to_str()) != Some("log") {
            continue;
        }
        let stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s,
            None => continue,
        };
        let file_id = match stem.parse::<u64>() {
            Ok(id) => id,
            Err(_) => continue,
        };
        if file_id < version.log_number.max(1) {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

fn wal_file_name(file_id: u64) -> String {
    format!("{file_id:06}.log")
}

fn sst_file_name(file_id: u64) -> String {
    format!("{file_id:06}.sst")
}

fn sst_temp_file_name(file_id: u64) -> String {
    format!("{file_id:06}.sst.tmp")
}

fn flush_memtable_to_l0(db_dir: &Path, w: &mut WriterState) -> Result<()> {
    if w.memtable.approx_bytes() == 0 {
        return Ok(());
    }
    let file_id = w.next_file_id;
    w.next_file_id = w.next_file_id.saturating_add(1);

    let tmp_path = db_dir.join(sst_temp_file_name(file_id));
    let final_path = db_dir.join(sst_file_name(file_id));

    let new_wal_id = w.wal_id.saturating_add(1);
    let new_wal_path = db_dir.join(wal_file_name(new_wal_id));
    failpoint::hit("flush:before_new_wal_create");
    let (new_wal, _recovery) = Wal::open(&new_wal_path, w.options.sync)?;
    drop(new_wal);
    failpoint::hit("flush:after_new_wal_create");

    let mut builder = TableBuilder::create(&tmp_path)?;
    for (user_key, seq, value_type, value) in w.memtable.iter_user_entries() {
        let ik = encode_internal_key(user_key, seq, value_type);
        builder.add(&ik, value)?;
    }
    failpoint::hit("flush:before_sst_finish");
    let mut meta = builder.finish(w.options.sync == SyncMode::Yes)?;
    failpoint::hit("flush:after_sst_finish");
    meta.file_id = file_id;
    meta.level = 0;

    failpoint::hit("flush:before_sst_rename");
    fs::rename(&tmp_path, &final_path)?;
    failpoint::hit("flush:after_sst_rename");
    if w.options.sync == SyncMode::Yes {
        failpoint::hit("flush:before_dir_sync");
        sync_parent_dir(&final_path)?;
        failpoint::hit("flush:after_dir_sync");
    }

    let last_sequence = w.next_seq.saturating_sub(1);
    failpoint::hit("flush:before_manifest_edit");
    w.manifest
        .append_flush_edit(&meta, new_wal_id, last_sequence)?;
    failpoint::hit("flush:after_manifest_edit");
    w.version.level0.push(meta);
    w.version.log_number = new_wal_id;
    w.version.last_sequence = w.version.last_sequence.max(last_sequence);

    w.memtable = MemTable::default();

    let old_wal_id = w.wal_id;
    w.wal_id = new_wal_id;
    let wal_path = db_dir.join(wal_file_name(w.wal_id));
    let (wal, _recovery) = Wal::open(&wal_path, w.options.sync)?;
    w.wal = wal;
    let _ = fs::remove_file(db_dir.join(wal_file_name(old_wal_id)));
    Ok(())
}

fn meta_may_contain_user_key(meta: &crate::sstable::FileMeta, user_key: &[u8]) -> Result<bool> {
    let smallest = parse_internal_key(&meta.smallest_key)?.user_key.to_vec();
    let largest = parse_internal_key(&meta.largest_key)?.user_key.to_vec();
    Ok(user_key >= smallest.as_slice() && user_key <= largest.as_slice())
}

fn compact_l0_to_l1(db_dir: &Path, w: &mut WriterState) -> Result<()> {
    if w.version.level0.is_empty() {
        return Ok(());
    }

    let mut min_user: Option<Vec<u8>> = None;
    let mut max_user: Option<Vec<u8>> = None;
    for m in &w.version.level0 {
        let s = parse_internal_key(&m.smallest_key)?.user_key.to_vec();
        let l = parse_internal_key(&m.largest_key)?.user_key.to_vec();
        min_user = Some(match min_user {
            Some(cur) => cur.min(s),
            None => s,
        });
        max_user = Some(match max_user {
            Some(cur) => cur.max(l),
            None => l,
        });
    }
    let min_user = min_user.ok_or(GraniteError::Corrupt("missing min user key"))?;
    let max_user = max_user.ok_or(GraniteError::Corrupt("missing max user key"))?;

    let overlap_l1: Vec<crate::sstable::FileMeta> = w
        .version
        .level1
        .iter()
        .filter(|m| {
            let s = parse_internal_key(&m.smallest_key)
                .map(|p| p.user_key.to_vec())
                .ok();
            let l = parse_internal_key(&m.largest_key)
                .map(|p| p.user_key.to_vec())
                .ok();
            match (s, l) {
                (Some(s), Some(l)) => !(l < min_user || s > max_user),
                _ => false,
            }
        })
        .cloned()
        .collect();

    let file_id = w.next_file_id;
    w.next_file_id = w.next_file_id.saturating_add(1);
    let tmp_path = db_dir.join(sst_temp_file_name(file_id));
    let final_path = db_dir.join(sst_file_name(file_id));

    struct Source {
        scanner: crate::sstable::SstScanner,
        current: Option<(Vec<u8>, Vec<u8>)>,
    }

    let mut sources: Vec<Source> = Vec::new();
    for meta in w.version.level0.iter().chain(overlap_l1.iter()) {
        let path = db_dir.join(sst_file_name(meta.file_id));
        let reader = TableReader::open(path)?;
        let mut scanner = reader.scanner();
        let current = scanner.next_item()?;
        sources.push(Source { scanner, current });
    }

    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    #[derive(Clone, Eq, PartialEq)]
    struct HeapEntry {
        key: Vec<u8>,
        src: usize,
        value: Vec<u8>,
    }

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            match cmp_internal_key_bytes_unchecked(&self.key, &other.key) {
                std::cmp::Ordering::Equal => self.src.cmp(&other.src),
                other => other,
            }
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    type HeapItem = Reverse<HeapEntry>;
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for (i, s) in sources.iter().enumerate() {
        if let Some((k, v)) = &s.current {
            heap.push(Reverse(HeapEntry {
                key: k.clone(),
                src: i,
                value: v.clone(),
            }));
        }
    }

    let mut builder = TableBuilder::create(&tmp_path)?;
    while let Some(Reverse(e)) = heap.pop() {
        builder.add(&e.key, &e.value)?;
        let src = e.src;
        let next = sources[src].scanner.next_item()?;
        sources[src].current = next;
        if let Some((nk, nv)) = &sources[src].current {
            heap.push(Reverse(HeapEntry {
                key: nk.clone(),
                src,
                value: nv.clone(),
            }));
        }
    }
    failpoint::hit("compaction:before_sst_finish");
    let mut out_meta = builder.finish(w.options.sync == SyncMode::Yes)?;
    failpoint::hit("compaction:after_sst_finish");
    out_meta.file_id = file_id;
    out_meta.level = 1;
    drop(sources);

    failpoint::hit("compaction:before_sst_rename");
    fs::rename(&tmp_path, &final_path)?;
    failpoint::hit("compaction:after_sst_rename");
    if w.options.sync == SyncMode::Yes {
        failpoint::hit("compaction:before_dir_sync");
        sync_parent_dir(&final_path)?;
        failpoint::hit("compaction:after_dir_sync");
    }

    let l0_ids: Vec<u64> = w.version.level0.iter().map(|m| m.file_id).collect();
    let overlap_ids_vec: Vec<u64> = overlap_l1.iter().map(|m| m.file_id).collect();

    let mut deletes: Vec<(u32, u64)> = Vec::new();
    deletes.extend(l0_ids.iter().map(|id| (0u32, *id)));
    deletes.extend(overlap_ids_vec.iter().map(|id| (1u32, *id)));

    let last_sequence = w.next_seq.saturating_sub(1);
    failpoint::hit("compaction:before_manifest_edit");
    w.manifest
        .append_compaction_edit(&out_meta, &deletes, last_sequence)?;
    failpoint::hit("compaction:after_manifest_edit");
    w.version.last_sequence = w.version.last_sequence.max(last_sequence);

    let overlap_ids: std::collections::BTreeSet<u64> = overlap_ids_vec.iter().copied().collect();
    w.version.level0.clear();
    w.version
        .level1
        .retain(|m| !overlap_ids.contains(&m.file_id));
    w.version.level1.push(out_meta);

    for id in l0_ids.into_iter().chain(overlap_ids_vec.into_iter()) {
        let _ = fs::remove_file(db_dir.join(sst_file_name(id)));
    }
    Ok(())
}

fn collect_visible_items(
    db_dir: &Path,
    w: &WriterState,
    range: &Range,
    snapshot: Snapshot,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    struct Source {
        items: Vec<(Vec<u8>, Vec<u8>)>,
        idx: usize,
    }

    impl Source {
        fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
            if self.idx >= self.items.len() {
                return None;
            }
            let out = self.items[self.idx].clone();
            self.idx += 1;
            Some(out)
        }
    }

    let mut sources: Vec<Source> = Vec::new();

    let mem_items: Vec<(Vec<u8>, Vec<u8>)> = w.memtable.iter_internal().collect();
    sources.push(Source {
        items: mem_items,
        idx: 0,
    });

    for meta in &w.version.level0 {
        let path = db_dir.join(sst_file_name(meta.file_id));
        let reader = TableReader::open(path)?;
        let mut it = reader.scanner();
        let mut items = Vec::new();
        while let Some((k, v)) = it.next_item()? {
            items.push((k, v));
        }
        sources.push(Source { items, idx: 0 });
    }

    for meta in &w.version.level1 {
        let path = db_dir.join(sst_file_name(meta.file_id));
        let reader = TableReader::open(path)?;
        let mut it = reader.scanner();
        let mut items = Vec::new();
        while let Some((k, v)) = it.next_item()? {
            items.push((k, v));
        }
        sources.push(Source { items, idx: 0 });
    }

    #[derive(Clone, Eq, PartialEq)]
    struct HeapEntry {
        key: Vec<u8>,
        src: usize,
        value: Vec<u8>,
    }

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            match cmp_internal_key_bytes_unchecked(&self.key, &other.key) {
                std::cmp::Ordering::Equal => self.src.cmp(&other.src),
                other => other,
            }
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    type HeapItem = Reverse<HeapEntry>;
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for (i, s) in sources.iter_mut().enumerate() {
        if let Some((k, v)) = s.next() {
            heap.push(Reverse(HeapEntry {
                key: k,
                src: i,
                value: v,
            }));
        }
    }

    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut last_user_key: Option<Vec<u8>> = None;

    while let Some(Reverse(e)) = heap.pop() {
        let src = e.src;
        if let Some((nk, nv)) = sources[src].next() {
            heap.push(Reverse(HeapEntry {
                key: nk,
                src,
                value: nv,
            }));
        }

        let parsed = parse_internal_key(&e.key)?;
        if parsed.seq > snapshot.read_seq {
            continue;
        }
        if !user_key_in_range(parsed.user_key, range) {
            continue;
        }
        if last_user_key
            .as_ref()
            .is_some_and(|prev| prev.as_slice() == parsed.user_key)
        {
            continue;
        }
        last_user_key = Some(parsed.user_key.to_vec());

        match parsed.value_type {
            ValueType::Value => out.push((parsed.user_key.to_vec(), e.value)),
            ValueType::Tombstone => {}
        }
    }

    Ok(out)
}

fn user_key_in_range(user_key: &[u8], range: &Range) -> bool {
    match &range.start {
        Bound::Included(k) if user_key < k.as_slice() => return false,
        Bound::Excluded(k) if user_key <= k.as_slice() => return false,
        _ => {}
    }
    match &range.end {
        Bound::Included(k) if user_key > k.as_slice() => return false,
        Bound::Excluded(k) if user_key >= k.as_slice() => return false,
        _ => {}
    }
    true
}

fn encode_batch_as_wal_record(batch: &WriteBatch) -> Vec<u8> {
    let batch_bytes = batch.encode();
    let mut out = Vec::with_capacity(4 + batch_bytes.len());
    out.extend_from_slice(&(batch_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&batch_bytes);
    out
}

fn decode_wal_record_to_batch(record: &[u8]) -> Result<WriteBatch> {
    if record.len() < 4 {
        return Err(GraniteError::Corrupt("wal record too short"));
    }
    let len = u32::from_le_bytes(
        record[..4]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("u32"))?,
    ) as usize;
    if record.len() != 4 + len {
        return Err(GraniteError::Corrupt("wal record length mismatch"));
    }
    WriteBatch::decode(&record[4..])
}

fn apply_batch_to_memtable(
    memtable: &mut MemTable,
    batch: &WriteBatch,
    next_seq: &mut u64,
) -> Result<()> {
    for op in &batch.ops {
        let seq = *next_seq;
        *next_seq = next_seq.saturating_add(1);
        match op {
            WriteOp::Put { key, value } => {
                memtable.insert(key, seq, ValueType::Value, value.clone());
            }
            WriteOp::Delete { key } => {
                memtable.insert(key, seq, ValueType::Tombstone, Vec::new());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn db_recovery_from_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            ..Options::default()
        };

        {
            let db = DB::open(&path, opts.clone()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"a", b"2").unwrap();
            db.delete(b"b").unwrap();
            db.close().unwrap();
        }

        let db2 = DB::open(&path, opts).unwrap();
        assert_eq!(db2.get(b"a").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db2.get(b"b").unwrap(), None);
    }

    #[test]
    fn snapshots_filter_visibility() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            ..Options::default()
        };
        let db = DB::open(&path, opts).unwrap();

        db.put(b"k", b"v1").unwrap();
        let s = db.snapshot().unwrap();
        db.put(b"k", b"v2").unwrap();

        assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get_with_snapshot(b"k", s).unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn flush_creates_sst_and_reads_from_it() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            ..Options::default()
        };

        {
            let db = DB::open(&path, opts.clone()).unwrap();
            db.put(b"a", b"1").unwrap();
            assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
            db.close().unwrap();
        }

        let db2 = DB::open(&path, opts).unwrap();
        assert_eq!(db2.get(b"a").unwrap(), Some(b"1".to_vec()));
        let items: Vec<(Vec<u8>, Vec<u8>)> = db2.iter(Range::all()).unwrap().collect();
        assert_eq!(items, vec![(b"a".to_vec(), b"1".to_vec())]);
    }

    #[test]
    fn compaction_merges_l0_into_l1_and_deletes_inputs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 0,
            l0_stop_trigger: 0,
        };

        {
            let db = DB::open(&path, opts.clone()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
            db.close().unwrap();
        }

        let db2 = DB::open(&path, opts).unwrap();
        assert_eq!(db2.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db2.get(b"b").unwrap(), Some(b"2".to_vec()));
        db2.close().unwrap();

        let db3 = DB::open(
            &path,
            Options {
                sync: SyncMode::No,
                ..Options::default()
            },
        )
        .unwrap();
        db3.close().unwrap();

        let mut sst_count = 0usize;
        for e in fs::read_dir(&path).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            if p.extension().and_then(|s| s.to_str()) == Some("sst") {
                sst_count += 1;
            }
        }
        assert_eq!(sst_count, 2);
    }
}
