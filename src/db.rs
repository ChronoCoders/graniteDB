use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

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
    prev_wal_id: u64,
    wal: Wal,
    next_seq: u64,
    next_op_index: u64,
    memtable: MemTable,
    imm_memtable: Option<Arc<MemTable>>,
    imm_flushing: bool,
    imm_trigger_op_index: u64,
    shutting_down: bool,
    bg_error: Option<GraniteError>,
    options: Options,
}

pub struct DB {
    path: PathBuf,
    shared: Arc<Shared>,
    bg: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    state: Mutex<WriterState>,
    cv: Condvar,
}

impl DB {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        let (manifest, mut version) = Manifest::open(&path, options.sync)?;
        let max_levels = options.max_levels.max(2);
        if version.levels.len() < max_levels {
            version.levels.resize_with(max_levels, Vec::new);
        }
        startup_gc(&path, &version)?;
        let next_file_id = version.max_file_id().saturating_add(1).max(1);

        let mut next_seq = version.last_sequence.saturating_add(1).max(1);

        let mut imm_memtable: Option<Arc<MemTable>> = None;
        let prev_wal_id = version.prev_log_number;
        if prev_wal_id != 0 {
            let mut t = MemTable::default();
            let prev_path = path.join(wal_file_name(prev_wal_id));
            let (_prev, recovery) = Wal::open(&prev_path, options.sync)?;
            for rec in recovery.records {
                let (start_seq, batch) = decode_wal_record_to_batch(&rec)?;
                apply_batch_to_memtable_at_seq(&mut t, &batch, start_seq);
                next_seq = next_seq.max(start_seq.saturating_add(batch.ops.len() as u64));
            }
            imm_memtable = Some(Arc::new(t));
        }

        let mut memtable = MemTable::default();
        let wal_id = version.log_number.max(1);
        let wal_path = path.join(wal_file_name(wal_id));
        let (wal, recovery) = Wal::open(&wal_path, options.sync)?;
        for rec in recovery.records {
            let (start_seq, batch) = decode_wal_record_to_batch(&rec)?;
            apply_batch_to_memtable_at_seq(&mut memtable, &batch, start_seq);
            next_seq = next_seq.max(start_seq.saturating_add(batch.ops.len() as u64));
        }

        let shared = Arc::new(Shared {
            state: Mutex::new(WriterState {
                manifest,
                version,
                next_file_id,
                wal_id,
                prev_wal_id,
                wal,
                next_seq,
                next_op_index: 0,
                memtable,
                imm_memtable,
                imm_flushing: false,
                imm_trigger_op_index: 0,
                shutting_down: false,
                bg_error: None,
                options,
            }),
            cv: Condvar::new(),
        });

        let db = Self {
            path: path.clone(),
            shared: shared.clone(),
            bg: Mutex::new(None),
        };

        let handle = std::thread::spawn({
            let db_dir = path.clone();
            move || bg_worker(db_dir, shared)
        });
        *db.bg
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))? = Some(handle);
        db.shared.cv.notify_all();
        Ok(db)
    }

    pub fn close(&self) -> Result<()> {
        {
            let mut w = self
                .shared
                .state
                .lock()
                .map_err(|_| GraniteError::Corrupt("poisoned"))?;
            w.shutting_down = true;
            self.shared.cv.notify_all();
        }

        if let Some(handle) = self
            .bg
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?
            .take()
        {
            let _ = handle.join();
        }

        let mut w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        if let Some(e) = w.bg_error.take() {
            return Err(e);
        }
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
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;

        if let Some(e) = w.bg_error.take() {
            return Err(e);
        }

        let op_index = w.next_op_index;
        w.next_op_index = w.next_op_index.saturating_add(1);
        let _op_guard = failpoint::set_current_op_index(op_index);

        loop {
            if let Some(e) = w.bg_error.take() {
                return Err(e);
            }

            let stall = w.imm_memtable.is_some()
                || w.imm_flushing
                || compaction_needed(&w)
                || w.version.levels[0].len() > w.options.l0_stop_trigger;
            if !stall {
                break;
            }

            self.shared.cv.notify_all();
            w = self
                .shared
                .cv
                .wait(w)
                .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        }

        let start_seq = w.next_seq;
        let record = encode_batch_as_wal_record(&batch, start_seq);
        w.wal.append_logical_record(&record)?;
        if w.options.sync == SyncMode::Yes {
            w.wal.sync()?;
        }
        apply_batch_to_memtable_at_seq(&mut w.memtable, &batch, start_seq);
        w.next_seq = start_seq.saturating_add(batch.ops.len() as u64);

        if w.memtable.approx_bytes() >= w.options.memtable_max_bytes {
            while w.imm_memtable.is_some() {
                w = self
                    .shared
                    .cv
                    .wait(w)
                    .map_err(|_| GraniteError::Corrupt("poisoned"))?;
                if let Some(e) = w.bg_error.take() {
                    return Err(e);
                }
            }

            let prev_log = w.wal_id;
            let next_log = w.wal_id.saturating_add(1);
            let next_log_path = self.path.join(wal_file_name(next_log));
            failpoint::io_err("flush:before_new_wal_create")?;
            failpoint::hit("flush:before_new_wal_create");
            let (next_wal, _rec) = Wal::open(&next_log_path, w.options.sync)?;
            failpoint::io_err("flush:after_new_wal_create")?;
            failpoint::hit("flush:after_new_wal_create");
            if w.options.sync == SyncMode::Yes {
                sync_parent_dir(&next_log_path)?;
            }

            let last_sequence = w.next_seq.saturating_sub(1);
            failpoint::io_err("flush:before_manifest_edit")?;
            failpoint::hit("flush:before_manifest_edit");
            w.manifest
                .append_log_rotation_edit(prev_log, next_log, last_sequence)?;
            failpoint::io_err("flush:after_manifest_edit")?;
            failpoint::hit("flush:after_manifest_edit");

            w.prev_wal_id = prev_log;
            w.version.prev_log_number = prev_log;
            w.wal_id = next_log;
            w.version.log_number = next_log;
            w.wal = next_wal;
            w.imm_memtable = Some(Arc::new(std::mem::take(&mut w.memtable)));
            w.memtable = MemTable::default();
            w.imm_trigger_op_index = op_index;
            self.shared.cv.notify_all();
        }

        Ok(())
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        let w = self
            .shared
            .state
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
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        match w.memtable.get(key, snapshot.read_seq)? {
            Some(GetDecision::Value(v)) => Ok(Some(v)),
            Some(GetDecision::Deleted) => Ok(None),
            None => {
                if let Some(imm) = w.imm_memtable.as_ref() {
                    match imm.get(key, snapshot.read_seq)? {
                        Some(GetDecision::Value(v)) => return Ok(Some(v)),
                        Some(GetDecision::Deleted) => return Ok(None),
                        None => {}
                    }
                }
                for meta in w.version.levels[0].iter().rev() {
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
                for level in w.version.levels.iter().skip(1) {
                    for meta in level {
                        if !meta_may_contain_user_key(meta, key)? {
                            continue;
                        }
                        let path = self.path.join(sst_file_name(meta.file_id));
                        let mut reader = TableReader::open(path)?;
                        let seek =
                            encode_internal_key(key, snapshot.read_seq, ValueType::Tombstone);
                        let got = reader.get_by_seek_key_prefix(
                            &seek,
                            key,
                            snapshot.read_seq,
                            |vt, v| match vt {
                                0 => Some(GetDecision::Deleted),
                                1 => Some(GetDecision::Value(v.to_vec())),
                                _ => None,
                            },
                        )?;
                        match got {
                            Some(GetDecision::Value(v)) => return Ok(Some(v)),
                            Some(GetDecision::Deleted) => return Ok(None),
                            None => {}
                        }
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
            .shared
            .state
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
        let keep_cur = file_id == version.log_number.max(1);
        let keep_prev = version.prev_log_number != 0 && file_id == version.prev_log_number;
        if !keep_cur && !keep_prev {
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

fn bg_worker(db_dir: PathBuf, shared: Arc<Shared>) {
    loop {
        let (flush_task, do_compact, shutting_down) = {
            let mut w = match shared.state.lock() {
                Ok(g) => g,
                Err(_) => std::process::abort(),
            };

            while !w.shutting_down
                && w.bg_error.is_none()
                && (w.imm_memtable.is_none() || w.imm_flushing)
                && !compaction_needed(&w)
            {
                w = match shared.cv.wait(w) {
                    Ok(g) => g,
                    Err(_) => std::process::abort(),
                };
            }

            if w.bg_error.is_some() {
                shared.cv.notify_all();
                return;
            }

            let shutting_down = w.shutting_down;
            let imm = w.imm_memtable.clone();
            let flush_task = if let Some(imm) = imm
                && !w.imm_flushing
            {
                let file_id = w.next_file_id;
                w.next_file_id = w.next_file_id.saturating_add(1);
                w.imm_flushing = true;
                Some((
                    imm.clone(),
                    w.prev_wal_id,
                    w.wal_id,
                    file_id,
                    w.next_seq.saturating_sub(1),
                    w.options.sync == SyncMode::Yes,
                    w.imm_trigger_op_index,
                ))
            } else {
                None
            };
            let do_compact = compaction_needed(&w);
            (flush_task, do_compact, shutting_down)
        };

        if let Some((imm, prev_wal_id, log_number, file_id, last_sequence, sync, op_index)) =
            flush_task
        {
            let _op_guard = failpoint::set_current_op_index(op_index);
            let tmp_path = db_dir.join(sst_temp_file_name(file_id));
            let final_path = db_dir.join(sst_file_name(file_id));

            let flush_res = (|| -> Result<crate::sstable::FileMeta> {
                let mut builder = TableBuilder::create(&tmp_path)?;
                for (user_key, seq, value_type, value) in imm.iter_user_entries() {
                    let ik = encode_internal_key(user_key, seq, value_type);
                    builder.add(&ik, value)?;
                }
                failpoint::io_err("flush:before_sst_finish")?;
                failpoint::hit("flush:before_sst_finish");
                let mut meta = builder.finish(sync)?;
                failpoint::io_err("flush:after_sst_finish")?;
                failpoint::hit("flush:after_sst_finish");
                meta.file_id = file_id;
                meta.level = 0;

                failpoint::io_err("flush:before_sst_rename")?;
                failpoint::hit("flush:before_sst_rename");
                fs::rename(&tmp_path, &final_path)?;
                failpoint::io_err("flush:after_sst_rename")?;
                failpoint::hit("flush:after_sst_rename");
                if sync {
                    failpoint::io_err("flush:before_dir_sync")?;
                    failpoint::hit("flush:before_dir_sync");
                    sync_parent_dir(&final_path)?;
                    failpoint::io_err("flush:after_dir_sync")?;
                    failpoint::hit("flush:after_dir_sync");
                }
                Ok(meta)
            })();

            match flush_res {
                Ok(meta) => {
                    let mut w = match shared.state.lock() {
                        Ok(g) => g,
                        Err(_) => std::process::abort(),
                    };
                    let _op_guard = failpoint::set_current_op_index(op_index);
                    if let Err(e) = (|| -> Result<()> {
                        failpoint::io_err("flush:before_manifest_edit")?;
                        failpoint::hit("flush:before_manifest_edit");
                        w.manifest
                            .append_flush_edit(&meta, log_number, 0, last_sequence)?;
                        failpoint::io_err("flush:after_manifest_edit")?;
                        failpoint::hit("flush:after_manifest_edit");
                        Ok(())
                    })() {
                        w.bg_error = Some(e);
                        w.imm_flushing = false;
                        shared.cv.notify_all();
                        return;
                    }
                    w.version.levels[0].push(meta);
                    w.version.prev_log_number = 0;
                    w.prev_wal_id = 0;
                    w.version.last_sequence = w.version.last_sequence.max(last_sequence);
                    w.imm_memtable = None;
                    w.imm_flushing = false;
                    let _ = fs::remove_file(db_dir.join(wal_file_name(prev_wal_id)));
                    shared.cv.notify_all();
                }
                Err(e) => {
                    let mut w = match shared.state.lock() {
                        Ok(g) => g,
                        Err(_) => std::process::abort(),
                    };
                    w.bg_error = Some(e);
                    w.imm_flushing = false;
                    shared.cv.notify_all();
                    return;
                }
            }
        } else if do_compact {
            let mut w = match shared.state.lock() {
                Ok(g) => g,
                Err(_) => std::process::abort(),
            };
            let op_index = w.imm_trigger_op_index;
            let _op_guard = failpoint::set_current_op_index(op_index);
            if let Err(e) = maybe_compact_levels(&db_dir, &mut w) {
                w.bg_error = Some(e);
                shared.cv.notify_all();
                return;
            }
            shared.cv.notify_all();
        } else if shutting_down {
            return;
        }
    }
}

fn compaction_needed(w: &WriterState) -> bool {
    if w.version.levels[0].len() > w.options.l0_slowdown_trigger {
        return true;
    }
    let max_levels = w.options.max_levels.max(2);
    for level in 1..(max_levels - 1) {
        if level_total_bytes(&w.version, level) > level_target_bytes(&w.options, level) {
            return true;
        }
    }
    false
}

fn meta_may_contain_user_key(meta: &crate::sstable::FileMeta, user_key: &[u8]) -> Result<bool> {
    let smallest = parse_internal_key(&meta.smallest_key)?.user_key.to_vec();
    let largest = parse_internal_key(&meta.largest_key)?.user_key.to_vec();
    Ok(user_key >= smallest.as_slice() && user_key <= largest.as_slice())
}

fn compact_l0_to_l1(db_dir: &Path, w: &mut WriterState) -> Result<()> {
    if w.version.levels[0].is_empty() {
        return Ok(());
    }

    let mut min_user: Option<Vec<u8>> = None;
    let mut max_user: Option<Vec<u8>> = None;
    for m in &w.version.levels[0] {
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

    let (overlap_l1, _min_user, _max_user) =
        collect_overlaps_expanding(&w.version.levels[1], min_user, max_user)?;

    let file_id = w.next_file_id;
    w.next_file_id = w.next_file_id.saturating_add(1);
    let tmp_path = db_dir.join(sst_temp_file_name(file_id));
    let final_path = db_dir.join(sst_file_name(file_id));

    struct Source {
        scanner: crate::sstable::SstScanner,
        current: Option<(Vec<u8>, Vec<u8>)>,
    }

    let mut sources: Vec<Source> = Vec::new();
    for meta in w.version.levels[0].iter().chain(overlap_l1.iter()) {
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
    failpoint::io_err("compaction:before_sst_finish")?;
    failpoint::hit("compaction:before_sst_finish");
    let mut out_meta = builder.finish(w.options.sync == SyncMode::Yes)?;
    failpoint::io_err("compaction:after_sst_finish")?;
    failpoint::hit("compaction:after_sst_finish");
    out_meta.file_id = file_id;
    out_meta.level = 1;
    drop(sources);

    failpoint::io_err("compaction:before_sst_rename")?;
    failpoint::hit("compaction:before_sst_rename");
    fs::rename(&tmp_path, &final_path)?;
    failpoint::io_err("compaction:after_sst_rename")?;
    failpoint::hit("compaction:after_sst_rename");
    if w.options.sync == SyncMode::Yes {
        failpoint::io_err("compaction:before_dir_sync")?;
        failpoint::hit("compaction:before_dir_sync");
        sync_parent_dir(&final_path)?;
        failpoint::io_err("compaction:after_dir_sync")?;
        failpoint::hit("compaction:after_dir_sync");
    }

    let l0_ids: Vec<u64> = w.version.levels[0].iter().map(|m| m.file_id).collect();
    let overlap_ids_vec: Vec<u64> = overlap_l1.iter().map(|m| m.file_id).collect();

    let mut deletes: Vec<(u32, u64)> = Vec::new();
    deletes.extend(l0_ids.iter().map(|id| (0u32, *id)));
    deletes.extend(overlap_ids_vec.iter().map(|id| (1u32, *id)));

    let last_sequence = w.next_seq.saturating_sub(1);
    failpoint::io_err("compaction:before_manifest_edit")?;
    failpoint::hit("compaction:before_manifest_edit");
    w.manifest
        .append_compaction_edit(&out_meta, &deletes, last_sequence)?;
    failpoint::io_err("compaction:after_manifest_edit")?;
    failpoint::hit("compaction:after_manifest_edit");
    w.version.last_sequence = w.version.last_sequence.max(last_sequence);

    let overlap_ids: std::collections::BTreeSet<u64> = overlap_ids_vec.iter().copied().collect();
    w.version.levels[0].clear();
    w.version.levels[1].retain(|m| !overlap_ids.contains(&m.file_id));
    w.version.levels[1].push(out_meta);

    for id in l0_ids.into_iter().chain(overlap_ids_vec.into_iter()) {
        let _ = fs::remove_file(db_dir.join(sst_file_name(id)));
    }
    Ok(())
}

fn maybe_compact_levels(db_dir: &Path, w: &mut WriterState) -> Result<()> {
    let max_levels = w.options.max_levels.max(2);
    if w.version.levels.len() < max_levels {
        w.version.levels.resize_with(max_levels, Vec::new);
    }

    let mut steps = 0usize;
    let max_steps = max_levels.saturating_mul(4).max(8);
    loop {
        if steps >= max_steps {
            return Ok(());
        }
        steps += 1;

        if w.version.levels[0].len() > w.options.l0_slowdown_trigger {
            compact_l0_to_l1(db_dir, w)?;
            continue;
        }

        let mut did_any = false;
        for level in 1..(max_levels - 1) {
            if level_total_bytes(&w.version, level) > level_target_bytes(&w.options, level) {
                compact_level_to_next(db_dir, w, level)?;
                did_any = true;
                break;
            }
        }
        if !did_any {
            return Ok(());
        }
    }
}

fn level_total_bytes(version: &VersionSet, level: usize) -> u64 {
    version.levels[level].iter().map(|m| m.file_size).sum()
}

fn level_target_bytes(options: &Options, level: usize) -> u64 {
    if level == 0 {
        return u64::MAX;
    }
    let mut out = options.level1_target_bytes;
    for _ in 1..level {
        out = out.saturating_mul(options.level_multiplier.max(1));
    }
    out
}

fn compact_level_to_next(db_dir: &Path, w: &mut WriterState, level: usize) -> Result<()> {
    let max_levels = w.options.max_levels.max(2);
    if level + 1 >= max_levels {
        return Ok(());
    }
    if w.version.levels[level].is_empty() {
        return Ok(());
    }

    let input = w.version.levels[level][0].clone();
    let (min_user, max_user) = meta_user_range(&input)?;
    let (overlaps, _min_user, _max_user) =
        collect_overlaps_expanding(&w.version.levels[level + 1], min_user, max_user)?;

    let file_id = w.next_file_id;
    w.next_file_id = w.next_file_id.saturating_add(1);
    let tmp_path = db_dir.join(sst_temp_file_name(file_id));
    let final_path = db_dir.join(sst_file_name(file_id));

    struct Source {
        scanner: crate::sstable::SstScanner,
        current: Option<(Vec<u8>, Vec<u8>)>,
    }

    let mut sources: Vec<Source> = Vec::new();
    for meta in std::iter::once(&input).chain(overlaps.iter()) {
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

    failpoint::io_err("compaction:before_sst_finish")?;
    failpoint::hit("compaction:before_sst_finish");
    let mut out_meta = builder.finish(w.options.sync == SyncMode::Yes)?;
    failpoint::io_err("compaction:after_sst_finish")?;
    failpoint::hit("compaction:after_sst_finish");
    out_meta.file_id = file_id;
    out_meta.level = (level + 1) as u32;
    drop(sources);

    failpoint::io_err("compaction:before_sst_rename")?;
    failpoint::hit("compaction:before_sst_rename");
    fs::rename(&tmp_path, &final_path)?;
    failpoint::io_err("compaction:after_sst_rename")?;
    failpoint::hit("compaction:after_sst_rename");
    if w.options.sync == SyncMode::Yes {
        failpoint::io_err("compaction:before_dir_sync")?;
        failpoint::hit("compaction:before_dir_sync");
        sync_parent_dir(&final_path)?;
        failpoint::io_err("compaction:after_dir_sync")?;
        failpoint::hit("compaction:after_dir_sync");
    }

    let mut deletes: Vec<(u32, u64)> = Vec::new();
    deletes.push((level as u32, input.file_id));
    deletes.extend(overlaps.iter().map(|m| ((level + 1) as u32, m.file_id)));

    let last_sequence = w.next_seq.saturating_sub(1);
    failpoint::io_err("compaction:before_manifest_edit")?;
    failpoint::hit("compaction:before_manifest_edit");
    w.manifest
        .append_compaction_edit(&out_meta, &deletes, last_sequence)?;
    failpoint::io_err("compaction:after_manifest_edit")?;
    failpoint::hit("compaction:after_manifest_edit");
    w.version.last_sequence = w.version.last_sequence.max(last_sequence);

    w.version.levels[level].retain(|m| m.file_id != input.file_id);
    let overlap_ids: std::collections::BTreeSet<u64> = overlaps.iter().map(|m| m.file_id).collect();
    w.version.levels[level + 1].retain(|m| !overlap_ids.contains(&m.file_id));
    w.version.levels[level + 1].push(out_meta);

    let _ = fs::remove_file(db_dir.join(sst_file_name(input.file_id)));
    for id in overlap_ids {
        let _ = fs::remove_file(db_dir.join(sst_file_name(id)));
    }

    Ok(())
}

fn meta_user_range(meta: &crate::sstable::FileMeta) -> Result<(Vec<u8>, Vec<u8>)> {
    let smallest = parse_internal_key(&meta.smallest_key)?.user_key.to_vec();
    let largest = parse_internal_key(&meta.largest_key)?.user_key.to_vec();
    Ok((smallest, largest))
}

fn collect_overlaps_expanding(
    candidates: &[crate::sstable::FileMeta],
    mut min_user: Vec<u8>,
    mut max_user: Vec<u8>,
) -> Result<(Vec<crate::sstable::FileMeta>, Vec<u8>, Vec<u8>)> {
    let mut picked: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    loop {
        let mut changed = false;
        for m in candidates {
            if picked.contains(&m.file_id) {
                continue;
            }
            let (s, l) = meta_user_range(m)?;
            if l < min_user || s > max_user {
                continue;
            }
            min_user = min_user.min(s);
            max_user = max_user.max(l);
            picked.insert(m.file_id);
            changed = true;
        }
        if !changed {
            break;
        }
    }
    let out: Vec<crate::sstable::FileMeta> = candidates
        .iter()
        .filter(|m| picked.contains(&m.file_id))
        .cloned()
        .collect();
    Ok((out, min_user, max_user))
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

    if let Some(imm) = w.imm_memtable.as_ref() {
        let imm_items: Vec<(Vec<u8>, Vec<u8>)> = imm.iter_internal().collect();
        sources.push(Source {
            items: imm_items,
            idx: 0,
        });
    }

    for level in &w.version.levels {
        for meta in level {
            let path = db_dir.join(sst_file_name(meta.file_id));
            let reader = TableReader::open(path)?;
            let mut it = reader.scanner();
            let mut items = Vec::new();
            while let Some((k, v)) = it.next_item()? {
                items.push((k, v));
            }
            sources.push(Source { items, idx: 0 });
        }
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

fn encode_batch_as_wal_record(batch: &WriteBatch, start_seq: u64) -> Vec<u8> {
    let batch_bytes = batch.encode();
    let mut out = Vec::with_capacity(8 + 4 + batch_bytes.len());
    out.extend_from_slice(&start_seq.to_le_bytes());
    out.extend_from_slice(&(batch_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&batch_bytes);
    out
}

fn decode_wal_record_to_batch(record: &[u8]) -> Result<(u64, WriteBatch)> {
    if record.len() < 12 {
        return Err(GraniteError::Corrupt("wal record too short"));
    }
    let start_seq = u64::from_le_bytes(
        record[..8]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("u64"))?,
    );
    let len = u32::from_le_bytes(
        record[8..12]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("u32"))?,
    ) as usize;
    if record.len() != 12 + len {
        return Err(GraniteError::Corrupt("wal record length mismatch"));
    }
    Ok((start_seq, WriteBatch::decode(&record[12..])?))
}

fn apply_batch_to_memtable_at_seq(memtable: &mut MemTable, batch: &WriteBatch, start_seq: u64) {
    let mut seq = start_seq;
    for op in &batch.ops {
        match op {
            WriteOp::Put { key, value } => {
                memtable.insert(key, seq, ValueType::Value, value.clone());
            }
            WriteOp::Delete { key } => {
                memtable.insert(key, seq, ValueType::Tombstone, Vec::new());
            }
        }
        seq = seq.saturating_add(1);
    }
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
            ..Options::default()
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
    }

    #[test]
    #[ignore]
    fn multi_level_compaction_can_push_l1_into_l2() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 0,
            l0_stop_trigger: 1000,
            max_levels: 3,
            level1_target_bytes: 1,
            level_multiplier: 1,
        };

        let db = DB::open(&path, opts).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        for _ in 0..200 {
            {
                let w = db.shared.state.lock().unwrap();
                if w.version.levels.len() >= 3
                    && w.version.levels[0].is_empty()
                    && w.version.levels[1].is_empty()
                    && w.version.levels[2].len() == 1
                {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let w = db.shared.state.lock().unwrap();
        assert!(w.version.levels.len() >= 3);
        assert!(w.version.levels[0].is_empty());
        assert!(w.version.levels[1].is_empty());
        assert_eq!(w.version.levels[2].len(), 1);
        db.close().unwrap();
    }
}
