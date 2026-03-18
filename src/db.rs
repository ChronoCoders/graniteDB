use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{GraniteError, Result};
use crate::failpoint;
use crate::internal_key::{
    ValueType, cmp_internal_key_bytes_unchecked, encode_internal_key, parse_internal_key,
};
use crate::manifest::{Manifest, VersionSet};
use crate::memtable::MemTable;
use crate::options::{Options, SyncMode};
use crate::sstable::{BlockCache, TableBuilder, TableReader};
use crate::util::sync_parent_dir;
use crate::wal::Wal;
use crate::write_batch::{WriteBatch, WriteOp};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub read_seq: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadOptions {
    pub snapshot: Snapshot,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            snapshot: Snapshot { read_seq: u64::MAX },
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteOptions {
    pub sync: Option<SyncMode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnFamily {
    pub id: u32,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct TxKey {
    cf_id: u32,
    key: Vec<u8>,
}

pub struct Transaction<'a> {
    db: &'a DB,
    snapshot: Snapshot,
    reads: HashSet<TxKey>,
    batch: WriteBatch,
    write_options: WriteOptions,
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

#[derive(Clone, Debug, Default)]
pub struct DbMetrics {
    pub puts: u64,
    pub deletes: u64,
    pub write_batches: u64,
    pub stall_waits: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub trivial_moves: u64,
    pub background_errors: u64,
    pub checkpoints: u64,
}

#[derive(Clone, Debug)]
pub struct DbEvent {
    pub at_micros: u64,
    pub kind: DbEventKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbStallReason {
    ImmutableMemtable,
    CompactionNeeded,
    L0Stop,
}

#[derive(Clone, Debug)]
pub enum DbEventKind {
    Stall { reason: DbStallReason },
    FlushStart { file_id: u64 },
    FlushFinish { file_id: u64 },
    CompactionStart,
    CompactionFinish,
    CheckpointFinish { manifest_number: u64 },
    BackgroundError,
}

pub trait DbEventListener: Send + Sync {
    fn on_event(&self, event: &DbEvent);
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
    block_cache: Arc<BlockCache>,
    column_families: HashMap<String, u32>,
    next_cf_id: u32,
    metrics: DbMetrics,
    events: VecDeque<DbEvent>,
    event_listener: Option<Arc<dyn DbEventListener>>,
    options: Options,
}

pub struct DB {
    path: PathBuf,
    shared: Arc<Shared>,
    bg: Mutex<Option<JoinHandle<()>>>,
    rate_limiter: Mutex<RateLimiter>,
}

struct Shared {
    state: Mutex<WriterState>,
    cv: Condvar,
}

struct RateLimiter {
    bytes_per_sec: u64,
    available: u64,
    last: Instant,
}

impl RateLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            available: bytes_per_sec,
            last: Instant::now(),
        }
    }

    fn reserve_sleep(&mut self, bytes: u64) -> Option<Duration> {
        if self.bytes_per_sec == 0 {
            return None;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last);
        let added = (elapsed.as_secs_f64() * self.bytes_per_sec as f64) as u64;
        self.available = (self.available.saturating_add(added)).min(self.bytes_per_sec);
        self.last = now;
        if self.available >= bytes {
            self.available -= bytes;
            return None;
        }
        let missing = bytes - self.available;
        self.available = 0;
        let secs = missing as f64 / self.bytes_per_sec as f64;
        Some(Duration::from_secs_f64(secs))
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

impl WriterState {
    fn record_event(&mut self, kind: DbEventKind) {
        let cap = self.options.event_log_capacity;
        if cap == 0 {
            if let Some(listener) = &self.event_listener {
                listener.on_event(&DbEvent {
                    at_micros: now_micros(),
                    kind,
                });
            }
            return;
        }
        while self.events.len() >= cap {
            self.events.pop_front();
        }
        let e = DbEvent {
            at_micros: now_micros(),
            kind,
        };
        self.events.push_back(e.clone());
        if let Some(listener) = &self.event_listener {
            listener.on_event(&e);
        }
    }
}

impl DB {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        let max_write_bytes_per_sec = options.max_write_bytes_per_sec;
        let block_cache = Arc::new(BlockCache::new(options.block_cache_capacity_bytes));
        let (manifest, mut version) = Manifest::open(&path, options.sync)?;
        let max_levels = options.max_levels.max(2);
        if version.levels.len() < max_levels {
            version.levels.resize_with(max_levels, Vec::new);
        }

        let mut column_families: HashMap<String, u32> = HashMap::new();
        let mut max_cf_id: u32 = 0;
        for (id, name) in &version.column_families {
            column_families.insert(name.clone(), *id);
            max_cf_id = max_cf_id.max(*id);
        }
        if !column_families.contains_key("default") {
            column_families.insert("default".to_string(), 0);
            version.column_families.push((0u32, "default".to_string()));
        }
        let next_cf_id = max_cf_id.saturating_add(1);

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
                block_cache,
                column_families,
                next_cf_id,
                metrics: DbMetrics::default(),
                events: VecDeque::new(),
                event_listener: None,
                options,
            }),
            cv: Condvar::new(),
        });

        let db = Self {
            path: path.clone(),
            shared: shared.clone(),
            bg: Mutex::new(None),
            rate_limiter: Mutex::new(RateLimiter::new(max_write_bytes_per_sec)),
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

    fn rate_limit_bytes(&self, bytes: u64) -> Result<()> {
        let sleep = {
            let mut rl = self
                .rate_limiter
                .lock()
                .map_err(|_| GraniteError::Corrupt("poisoned"))?;
            rl.reserve_sleep(bytes)
        };
        if let Some(d) = sleep {
            std::thread::sleep(d);
        }
        Ok(())
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

    pub fn begin_transaction(&self) -> Result<Transaction<'_>> {
        Ok(Transaction {
            db: self,
            snapshot: self.snapshot()?,
            reads: HashSet::new(),
            batch: WriteBatch::default(),
            write_options: WriteOptions::default(),
        })
    }

    pub fn set_event_listener(&self, listener: Option<Arc<dyn DbEventListener>>) -> Result<()> {
        let mut w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        w.event_listener = listener;
        Ok(())
    }

    pub fn get_property(&self, name: &str) -> Result<Option<String>> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        match name {
            "granitedb.stats" => {
                let mut s = String::new();
                s.push_str(&format!("puts: {}\n", w.metrics.puts));
                s.push_str(&format!("deletes: {}\n", w.metrics.deletes));
                s.push_str(&format!("write_batches: {}\n", w.metrics.write_batches));
                s.push_str(&format!("stall_waits: {}\n", w.metrics.stall_waits));
                s.push_str(&format!("flushes: {}\n", w.metrics.flushes));
                s.push_str(&format!("compactions: {}\n", w.metrics.compactions));
                s.push_str(&format!("trivial_moves: {}\n", w.metrics.trivial_moves));
                s.push_str(&format!(
                    "background_errors: {}\n",
                    w.metrics.background_errors
                ));
                s.push_str(&format!("checkpoints: {}\n", w.metrics.checkpoints));
                for (i, level) in w.version.levels.iter().enumerate() {
                    let files = level.len();
                    let bytes: u64 = level.iter().map(|m| m.file_size).sum();
                    s.push_str(&format!("level_{i}_files: {files}\n"));
                    s.push_str(&format!("level_{i}_bytes: {bytes}\n"));
                }
                Ok(Some(s))
            }
            "granitedb.options" => Ok(Some(format!(
                "sync: {:?}\nmemtable_max_bytes: {}\nl0_slowdown_trigger: {}\nl0_stop_trigger: {}\nmax_levels: {}\nlevel1_target_bytes: {}\nlevel_multiplier: {}\nbloom_bits_per_key: {}\nblock_cache_capacity_bytes: {}\nmanifest_checkpoint_target_bytes: {}\ndrop_obsolete_versions_during_compaction: {}\nttl_filter_from_value_prefix_micros: {}\nsstable_compression: {:?}\nmax_write_bytes_per_sec: {}\n",
                w.options.sync,
                w.options.memtable_max_bytes,
                w.options.l0_slowdown_trigger,
                w.options.l0_stop_trigger,
                w.options.max_levels,
                w.options.level1_target_bytes,
                w.options.level_multiplier,
                w.options.bloom_bits_per_key,
                w.options.block_cache_capacity_bytes,
                w.options.manifest_checkpoint_target_bytes,
                w.options.drop_obsolete_versions_during_compaction,
                w.options.ttl_filter_from_value_prefix_micros,
                w.options.sstable_compression,
                w.options.max_write_bytes_per_sec
            ))),
            _ => Ok(None),
        }
    }

    pub fn create_checkpoint(&self, dest_dir: impl AsRef<Path>) -> Result<()> {
        let dest_dir = dest_dir.as_ref();
        fs::create_dir_all(dest_dir)?;
        if fs::read_dir(dest_dir)?.next().is_some() {
            return Err(GraniteError::InvalidArgument("checkpoint dir not empty"));
        }

        let mut w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        if let Some(e) = w.bg_error.take() {
            return Err(e);
        }

        w.manifest.sync_force()?;
        w.wal.sync_force()?;

        let current_src = self.path.join("CURRENT");
        let current_contents = fs::read_to_string(&current_src)
            .map_err(|_| GraniteError::Corrupt("missing CURRENT"))?;
        let manifest_name = current_contents.trim();
        if manifest_name.is_empty() {
            return Err(GraniteError::Corrupt("empty CURRENT"));
        }
        let manifest_src = self.path.join(manifest_name);

        let mut sst_ids: Vec<u64> = Vec::new();
        for level in &w.version.levels {
            for meta in level {
                sst_ids.push(meta.file_id);
            }
        }
        let wal_ids = [w.wal_id, w.prev_wal_id];

        fs::copy(&current_src, dest_dir.join("CURRENT"))?;
        fs::copy(&manifest_src, dest_dir.join(manifest_name))?;
        for file_id in sst_ids {
            let name = sst_file_name(file_id);
            hard_link_or_copy(&self.path.join(&name), &dest_dir.join(&name))?;
        }
        for file_id in wal_ids {
            if file_id == 0 {
                continue;
            }
            let name = wal_file_name(file_id);
            fs::copy(self.path.join(&name), dest_dir.join(&name))?;
        }
        Ok(())
    }

    pub fn ingest_sst_files(&self, sst_paths: &[PathBuf], move_files: bool) -> Result<Vec<u64>> {
        let mut w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        if let Some(e) = w.bg_error.take() {
            return Err(e);
        }

        let mut out_ids: Vec<u64> = Vec::new();
        for src_path in sst_paths {
            let file_id = w.next_file_id;
            w.next_file_id = w.next_file_id.saturating_add(1);
            let dst_path = self.path.join(sst_file_name(file_id));

            if move_files {
                fs::rename(src_path, &dst_path)?;
            } else {
                hard_link_or_copy(src_path, &dst_path)?;
            }

            let (smallest_key, largest_key, max_seq) =
                inspect_sstable(&dst_path, w.block_cache.clone())?;
            let file_size = fs::metadata(&dst_path)?.len();
            let meta = crate::sstable::FileMeta {
                level: 0,
                file_id,
                file_size,
                smallest_key,
                largest_key,
            };

            w.version.levels[0].push(meta.clone());
            w.version.last_sequence = w.version.last_sequence.max(max_seq);
            w.next_seq = w.next_seq.max(max_seq.saturating_add(1));

            let last_sequence = w.version.last_sequence;
            w.manifest.append_ingest_edit(&meta, last_sequence)?;
            out_ids.push(file_id);
        }

        self.shared.cv.notify_all();
        Ok(out_ids)
    }

    pub fn metrics(&self) -> Result<DbMetrics> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        Ok(w.metrics.clone())
    }

    pub fn recent_events(&self) -> Result<Vec<DbEvent>> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        Ok(w.events.iter().cloned().collect())
    }

    pub fn create_column_family(&self, name: &str) -> Result<ColumnFamily> {
        let mut w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;

        if let Some(e) = w.bg_error.take() {
            return Err(e);
        }

        if let Some(id) = w.column_families.get(name) {
            return Ok(ColumnFamily {
                id: *id,
                name: name.to_string(),
            });
        }

        let id = w.next_cf_id;
        w.next_cf_id = w.next_cf_id.saturating_add(1);
        w.manifest.append_create_column_family_edit(id, name)?;
        w.version.column_families.push((id, name.to_string()));
        w.column_families.insert(name.to_string(), id);
        Ok(ColumnFamily {
            id,
            name: name.to_string(),
        })
    }

    pub fn column_family(&self, name: &str) -> Result<Option<ColumnFamily>> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        Ok(w.column_families.get(name).copied().map(|id| ColumnFamily {
            id,
            name: name.to_string(),
        }))
    }

    pub fn list_column_families(&self) -> Result<Vec<ColumnFamily>> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        let mut out: Vec<ColumnFamily> = w
            .column_families
            .iter()
            .map(|(name, id)| ColumnFamily {
                id: *id,
                name: name.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id).then_with(|| a.name.cmp(&b.name)));
        Ok(out)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf_id(0, key, value)
    }

    pub fn put_cf(&self, cf: &ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf_id(cf.id, key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf_id(0, key)
    }

    pub fn delete_cf(&self, cf: &ColumnFamily, key: &[u8]) -> Result<()> {
        self.delete_cf_id(cf.id, key)
    }

    pub fn delete_range(&self, range: Range) -> Result<()> {
        self.delete_range_cf_id(0, range)
    }

    pub fn delete_range_cf(&self, cf: &ColumnFamily, range: Range) -> Result<()> {
        self.delete_range_cf_id(cf.id, range)
    }

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.merge_cf_id(0, key, value)
    }

    pub fn merge_cf(&self, cf: &ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.merge_cf_id(cf.id, key, value)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.write_batch_with_options(batch, WriteOptions::default())
    }

    pub fn write_batch_with_options(
        &self,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> Result<()> {
        self.write_batch_raw(batch, write_options)
    }

    fn put_cf_id(&self, cf_id: u32, key: &[u8], value: &[u8]) -> Result<()> {
        let batch = WriteBatch::default().put_cf(cf_id, key.to_vec(), value.to_vec());
        self.write_batch_in_cf(cf_id, batch)
    }

    fn delete_cf_id(&self, cf_id: u32, key: &[u8]) -> Result<()> {
        let batch = WriteBatch::default().delete_cf(cf_id, key.to_vec());
        self.write_batch_in_cf(cf_id, batch)
    }

    fn delete_range_cf_id(&self, cf_id: u32, range: Range) -> Result<()> {
        let start = match &range.start {
            Bound::Unbounded => return Err(GraniteError::InvalidArgument("unbounded range start")),
            Bound::Included(k) => k.clone(),
            Bound::Excluded(k) => successor_key(k.clone())
                .ok_or(GraniteError::InvalidArgument("range start overflow"))?,
        };
        let end = match &range.end {
            Bound::Unbounded => return Err(GraniteError::InvalidArgument("unbounded range end")),
            Bound::Included(k) => successor_key(k.clone())
                .ok_or(GraniteError::InvalidArgument("range end overflow"))?,
            Bound::Excluded(k) => k.clone(),
        };
        if start >= end {
            return Ok(());
        }
        let batch = WriteBatch::default().delete_range_cf(cf_id, start, end);
        self.write_batch_with_options(batch, WriteOptions::default())
    }

    fn merge_cf_id(&self, cf_id: u32, key: &[u8], value: &[u8]) -> Result<()> {
        let batch = WriteBatch::default().merge_cf(cf_id, key.to_vec(), value.to_vec());
        self.write_batch_with_options(batch, WriteOptions::default())
    }

    fn write_batch_in_cf(&self, cf_id: u32, batch: WriteBatch) -> Result<()> {
        let batch = set_batch_cf_id(cf_id, batch);
        self.write_batch_raw(batch, WriteOptions::default())
    }

    fn write_batch_raw(&self, batch: WriteBatch, write_options: WriteOptions) -> Result<()> {
        self.rate_limit_bytes(wal_record_len_for_batch(&batch))?;
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

            w.metrics.stall_waits = w.metrics.stall_waits.saturating_add(1);
            let reason = if w.imm_memtable.is_some() || w.imm_flushing {
                DbStallReason::ImmutableMemtable
            } else if w.version.levels[0].len() > w.options.l0_stop_trigger {
                DbStallReason::L0Stop
            } else {
                DbStallReason::CompactionNeeded
            };
            w.record_event(DbEventKind::Stall { reason });

            self.shared.cv.notify_all();
            w = self
                .shared
                .cv
                .wait(w)
                .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        }

        w.metrics.write_batches = w.metrics.write_batches.saturating_add(1);
        for op in &batch.ops {
            match op {
                WriteOp::Put { .. } => {
                    w.metrics.puts = w.metrics.puts.saturating_add(1);
                }
                WriteOp::Delete { .. } => {
                    w.metrics.deletes = w.metrics.deletes.saturating_add(1);
                }
                WriteOp::DeleteRange { .. } => {
                    w.metrics.deletes = w.metrics.deletes.saturating_add(1);
                }
                WriteOp::Merge { .. } => {
                    w.metrics.puts = w.metrics.puts.saturating_add(1);
                }
            }
        }

        let start_seq = w.next_seq;
        let record = encode_batch_as_wal_record(&batch, start_seq);
        w.wal.append_logical_record(&record)?;
        let sync = write_options.sync.unwrap_or(w.options.sync);
        if sync == SyncMode::Yes {
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
            let version_snapshot = w.version.clone();
            let target = w.options.manifest_checkpoint_target_bytes;
            if let Some(manifest_number) = w.manifest.maybe_checkpoint(&version_snapshot, target)? {
                w.metrics.checkpoints = w.metrics.checkpoints.saturating_add(1);
                w.record_event(DbEventKind::CheckpointFinish { manifest_number });
            }
            self.shared.cv.notify_all();
        }

        Ok(())
    }

    fn commit_transaction(
        &self,
        snapshot: Snapshot,
        mut reads: HashSet<TxKey>,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> Result<()> {
        self.rate_limit_bytes(wal_record_len_for_batch(&batch))?;
        for op in &batch.ops {
            match op {
                WriteOp::Put { cf_id, key, .. } => {
                    reads.insert(TxKey {
                        cf_id: *cf_id,
                        key: key.clone(),
                    });
                }
                WriteOp::Delete { cf_id, key } => {
                    reads.insert(TxKey {
                        cf_id: *cf_id,
                        key: key.clone(),
                    });
                }
                WriteOp::DeleteRange { cf_id, start, .. } => {
                    reads.insert(TxKey {
                        cf_id: *cf_id,
                        key: start.clone(),
                    });
                }
                WriteOp::Merge { cf_id, key, .. } => {
                    reads.insert(TxKey {
                        cf_id: *cf_id,
                        key: key.clone(),
                    });
                }
            }
        }

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

            w.metrics.stall_waits = w.metrics.stall_waits.saturating_add(1);
            let reason = if w.imm_memtable.is_some() || w.imm_flushing {
                DbStallReason::ImmutableMemtable
            } else if w.version.levels[0].len() > w.options.l0_stop_trigger {
                DbStallReason::L0Stop
            } else {
                DbStallReason::CompactionNeeded
            };
            w.record_event(DbEventKind::Stall { reason });

            self.shared.cv.notify_all();
            w = self
                .shared
                .cv
                .wait(w)
                .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        }

        for k in &reads {
            if let Some(latest) = self.latest_seq_for_cf_key(&w, k.cf_id, &k.key)?
                && latest > snapshot.read_seq
            {
                return Err(GraniteError::InvalidArgument("transaction conflict"));
            }
        }

        w.metrics.write_batches = w.metrics.write_batches.saturating_add(1);
        for op in &batch.ops {
            match op {
                WriteOp::Put { .. } => {
                    w.metrics.puts = w.metrics.puts.saturating_add(1);
                }
                WriteOp::Delete { .. } => {
                    w.metrics.deletes = w.metrics.deletes.saturating_add(1);
                }
                WriteOp::DeleteRange { .. } => {
                    w.metrics.deletes = w.metrics.deletes.saturating_add(1);
                }
                WriteOp::Merge { .. } => {
                    w.metrics.puts = w.metrics.puts.saturating_add(1);
                }
            }
        }

        let start_seq = w.next_seq;
        let record = encode_batch_as_wal_record(&batch, start_seq);
        w.wal.append_logical_record(&record)?;
        let sync = write_options.sync.unwrap_or(w.options.sync);
        if sync == SyncMode::Yes {
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
            let version_snapshot = w.version.clone();
            let target = w.options.manifest_checkpoint_target_bytes;
            if let Some(manifest_number) = w.manifest.maybe_checkpoint(&version_snapshot, target)? {
                w.metrics.checkpoints = w.metrics.checkpoints.saturating_add(1);
                w.record_event(DbEventKind::CheckpointFinish { manifest_number });
            }
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

    fn latest_seq_for_cf_key(
        &self,
        w: &WriterState,
        cf_id: u32,
        user_key: &[u8],
    ) -> Result<Option<u64>> {
        let key = encode_cf_user_key(cf_id, user_key);
        if let Some(seq) = w.memtable.latest_seq(&key) {
            return Ok(Some(seq));
        }
        if let Some(imm) = w.imm_memtable.as_ref()
            && let Some(seq) = imm.latest_seq(&key)
        {
            return Ok(Some(seq));
        }

        for meta in w.version.levels[0].iter().rev() {
            let path = self.path.join(sst_file_name(meta.file_id));
            let mut reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
            if let Some(seq) = reader.latest_seq(&key)? {
                return Ok(Some(seq));
            }
        }
        for level in w.version.levels.iter().skip(1) {
            for meta in level {
                if !meta_may_contain_user_key(meta, &key)? {
                    continue;
                }
                let path = self.path.join(sst_file_name(meta.file_id));
                let mut reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
                if let Some(seq) = reader.latest_seq(&key)? {
                    return Ok(Some(seq));
                }
            }
        }
        Ok(None)
    }

    fn max_range_tombstone_seq_for_internal_key(
        &self,
        w: &WriterState,
        internal_user_key: &[u8],
        read_seq: u64,
    ) -> Result<Option<u64>> {
        let mut max_seq: Option<u64> = None;
        if let Some(seq) = w
            .memtable
            .max_range_tombstone_seq_covering(internal_user_key, read_seq)
        {
            max_seq = Some(max_seq.unwrap_or(0).max(seq));
        }
        if let Some(imm) = w.imm_memtable.as_ref()
            && let Some(seq) = imm.max_range_tombstone_seq_covering(internal_user_key, read_seq)
        {
            max_seq = Some(max_seq.unwrap_or(0).max(seq));
        }

        for level in &w.version.levels {
            for meta in level {
                let smallest = parse_internal_key(&meta.smallest_key)?.user_key;
                if smallest > internal_user_key {
                    continue;
                }
                let path = self.path.join(sst_file_name(meta.file_id));
                let mut reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
                if let Some(seq) =
                    reader.max_range_tombstone_seq_covering(internal_user_key, read_seq)?
                {
                    max_seq = Some(max_seq.unwrap_or(0).max(seq));
                }
            }
        }
        Ok(max_seq)
    }

    fn get_cf_with_snapshot_id_locked(
        &self,
        w: &WriterState,
        cf_id: u32,
        key: &[u8],
        snapshot: Snapshot,
    ) -> Result<Option<Vec<u8>>> {
        let internal_key = encode_cf_user_key(cf_id, key);
        let range_seq =
            self.max_range_tombstone_seq_for_internal_key(w, &internal_key, snapshot.read_seq)?;

        let mut entries: Vec<(u64, ValueType, Vec<u8>)> = Vec::new();
        entries.extend(
            w.memtable
                .entries_for_user_key(&internal_key, snapshot.read_seq),
        );
        if let Some(imm) = w.imm_memtable.as_ref() {
            entries.extend(imm.entries_for_user_key(&internal_key, snapshot.read_seq));
        }
        for meta in w.version.levels[0].iter().rev() {
            let path = self.path.join(sst_file_name(meta.file_id));
            let mut reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
            entries.extend(reader.entries_for_user_key(&internal_key, snapshot.read_seq)?);
        }
        for level in w.version.levels.iter().skip(1) {
            for meta in level {
                if !meta_may_contain_user_key(meta, &internal_key)? {
                    continue;
                }
                let path = self.path.join(sst_file_name(meta.file_id));
                let mut reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
                entries.extend(reader.entries_for_user_key(&internal_key, snapshot.read_seq)?);
            }
        }
        entries.sort_by(|a, b| b.0.cmp(&a.0));

        let mut operands: Vec<Vec<u8>> = Vec::new();
        let mut base: Option<Option<Vec<u8>>> = None;

        for (seq, vt, v) in entries {
            if range_seq.is_some_and(|r| r >= seq) {
                base = Some(None);
                break;
            }
            match vt {
                ValueType::Value => {
                    base = Some(Some(v));
                    break;
                }
                ValueType::Tombstone => {
                    base = Some(None);
                    break;
                }
                ValueType::MergeOperand => operands.push(v),
                ValueType::RangeTombstone => {}
            }
        }

        let existing = match &base {
            Some(Some(v)) => Some(v.as_slice()),
            _ => None,
        };
        if operands.is_empty() {
            return Ok(existing.map(|v| v.to_vec()));
        }
        let operand_slices: Vec<&[u8]> = operands.iter().rev().map(|v| v.as_slice()).collect();
        w.options
            .merge_operator
            .full_merge(key, existing, &operand_slices)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf_with_snapshot_id(0, key, Snapshot { read_seq: u64::MAX })
    }

    pub fn get_with_options(
        &self,
        key: &[u8],
        read_options: ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.get_cf_with_snapshot_id(0, key, read_options.snapshot)
    }

    pub fn get_cf(&self, cf: &ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf_with_snapshot_id(cf.id, key, Snapshot { read_seq: u64::MAX })
    }

    pub fn get_cf_with_options(
        &self,
        cf: &ColumnFamily,
        key: &[u8],
        read_options: ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.get_cf_with_snapshot_id(cf.id, key, read_options.snapshot)
    }

    pub fn get_with_snapshot(&self, key: &[u8], snapshot: Snapshot) -> Result<Option<Vec<u8>>> {
        self.get_cf_with_snapshot_id(0, key, snapshot)
    }

    pub fn get_cf_with_snapshot(
        &self,
        cf: &ColumnFamily,
        key: &[u8],
        snapshot: Snapshot,
    ) -> Result<Option<Vec<u8>>> {
        self.get_cf_with_snapshot_id(cf.id, key, snapshot)
    }

    fn get_cf_with_snapshot_id(
        &self,
        cf_id: u32,
        key: &[u8],
        snapshot: Snapshot,
    ) -> Result<Option<Vec<u8>>> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        self.get_cf_with_snapshot_id_locked(&w, cf_id, key, snapshot)
    }

    pub fn iter(&self, range: Range) -> Result<DbIterator> {
        self.iter_cf_with_snapshot_id(0, range, Snapshot { read_seq: u64::MAX })
    }

    pub fn iter_with_snapshot(&self, range: Range, snapshot: Snapshot) -> Result<DbIterator> {
        self.iter_cf_with_snapshot_id(0, range, snapshot)
    }

    pub fn iter_cf(&self, cf: &ColumnFamily, range: Range) -> Result<DbIterator> {
        self.iter_cf_with_snapshot_id(cf.id, range, Snapshot { read_seq: u64::MAX })
    }

    pub fn iter_cf_with_snapshot(
        &self,
        cf: &ColumnFamily,
        range: Range,
        snapshot: Snapshot,
    ) -> Result<DbIterator> {
        self.iter_cf_with_snapshot_id(cf.id, range, snapshot)
    }

    fn iter_cf_with_snapshot_id(
        &self,
        cf_id: u32,
        range: Range,
        snapshot: Snapshot,
    ) -> Result<DbIterator> {
        let w = self
            .shared
            .state
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;

        let out = collect_visible_items(&self.path, &w, cf_id, &range, snapshot)?;

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

impl<'a> Transaction<'a> {
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot
    }

    pub fn set_write_options(&mut self, write_options: WriteOptions) {
        self.write_options = write_options;
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf_id(0, key)
    }

    pub fn get_cf(&mut self, cf: &ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf_id(cf.id, key)
    }

    fn get_cf_id(&mut self, cf_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.reads.insert(TxKey {
            cf_id,
            key: key.to_vec(),
        });
        self.db.get_cf_with_snapshot_id(cf_id, key, self.snapshot)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.put_cf_id(0, key, value);
    }

    pub fn put_cf(&mut self, cf: &ColumnFamily, key: &[u8], value: &[u8]) {
        self.put_cf_id(cf.id, key, value);
    }

    fn put_cf_id(&mut self, cf_id: u32, key: &[u8], value: &[u8]) {
        self.batch.ops.push(WriteOp::Put {
            cf_id,
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.delete_cf_id(0, key);
    }

    pub fn delete_cf(&mut self, cf: &ColumnFamily, key: &[u8]) {
        self.delete_cf_id(cf.id, key);
    }

    fn delete_cf_id(&mut self, cf_id: u32, key: &[u8]) {
        self.batch.ops.push(WriteOp::Delete {
            cf_id,
            key: key.to_vec(),
        });
    }

    pub fn commit(self) -> Result<()> {
        self.db
            .commit_transaction(self.snapshot, self.reads, self.batch, self.write_options)
    }
}

fn hard_link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    match fs::hard_link(src, dst) {
        Ok(_) => Ok(()),
        Err(_) => {
            fs::copy(src, dst)?;
            Ok(())
        }
    }
}

fn inspect_sstable(path: &Path, cache: Arc<BlockCache>) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    let reader = TableReader::open_with_cache(path, Some(cache))?;
    let mut scanner = reader.scanner();
    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Option<Vec<u8>> = None;
    let mut max_seq: u64 = 0;
    while let Some((k, _v)) = scanner.next_item()? {
        if smallest.is_none() {
            smallest = Some(k.clone());
        }
        largest = Some(k.clone());
        let parsed = parse_internal_key(&k)?;
        max_seq = max_seq.max(parsed.seq);
    }
    let smallest = smallest.ok_or(GraniteError::Corrupt("empty table"))?;
    let largest = largest.ok_or(GraniteError::Corrupt("empty table"))?;
    Ok((smallest, largest, max_seq))
}

fn wal_record_len_for_batch(batch: &WriteBatch) -> u64 {
    (8u64)
        .saturating_add(4)
        .saturating_add(encoded_write_batch_len(batch) as u64)
}

fn encoded_write_batch_len(batch: &WriteBatch) -> usize {
    let mut out = 4usize;
    for op in &batch.ops {
        match op {
            WriteOp::Put { key, value, .. } => {
                out = out
                    .saturating_add(1)
                    .saturating_add(4)
                    .saturating_add(4)
                    .saturating_add(key.len())
                    .saturating_add(4)
                    .saturating_add(value.len());
            }
            WriteOp::Delete { key, .. } => {
                out = out
                    .saturating_add(1)
                    .saturating_add(4)
                    .saturating_add(4)
                    .saturating_add(key.len());
            }
            WriteOp::DeleteRange { start, end, .. } => {
                out = out
                    .saturating_add(1)
                    .saturating_add(4)
                    .saturating_add(4)
                    .saturating_add(start.len())
                    .saturating_add(4)
                    .saturating_add(end.len());
            }
            WriteOp::Merge { key, value, .. } => {
                out = out
                    .saturating_add(1)
                    .saturating_add(4)
                    .saturating_add(4)
                    .saturating_add(key.len())
                    .saturating_add(4)
                    .saturating_add(value.len());
            }
        }
    }
    out
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
                w.record_event(DbEventKind::FlushStart { file_id });
                Some((
                    imm.clone(),
                    w.prev_wal_id,
                    w.wal_id,
                    file_id,
                    w.next_seq.saturating_sub(1),
                    w.options.sync == SyncMode::Yes,
                    w.options.bloom_bits_per_key,
                    w.options.sstable_compression,
                    w.imm_trigger_op_index,
                ))
            } else {
                None
            };
            let do_compact = compaction_needed(&w);
            (flush_task, do_compact, shutting_down)
        };

        if let Some((
            imm,
            prev_wal_id,
            log_number,
            file_id,
            last_sequence,
            sync,
            bloom_bits_per_key,
            compression,
            op_index,
        )) = flush_task
        {
            let _op_guard = failpoint::set_current_op_index(op_index);
            let tmp_path = db_dir.join(sst_temp_file_name(file_id));
            let final_path = db_dir.join(sst_file_name(file_id));

            let flush_res = (|| -> Result<crate::sstable::FileMeta> {
                let mut builder = TableBuilder::create(&tmp_path, bloom_bits_per_key, compression)?;
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
                        w.metrics.background_errors = w.metrics.background_errors.saturating_add(1);
                        w.record_event(DbEventKind::BackgroundError);
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
                    w.metrics.flushes = w.metrics.flushes.saturating_add(1);
                    w.record_event(DbEventKind::FlushFinish { file_id });
                    let version_snapshot = w.version.clone();
                    let target = w.options.manifest_checkpoint_target_bytes;
                    match w.manifest.maybe_checkpoint(&version_snapshot, target) {
                        Ok(Some(manifest_number)) => {
                            w.metrics.checkpoints = w.metrics.checkpoints.saturating_add(1);
                            w.record_event(DbEventKind::CheckpointFinish { manifest_number });
                        }
                        Ok(None) => {}
                        Err(e) => {
                            w.bg_error = Some(e);
                            w.metrics.background_errors =
                                w.metrics.background_errors.saturating_add(1);
                            w.record_event(DbEventKind::BackgroundError);
                            shared.cv.notify_all();
                            return;
                        }
                    }
                    let _ = fs::remove_file(db_dir.join(wal_file_name(prev_wal_id)));
                    shared.cv.notify_all();
                }
                Err(e) => {
                    let mut w = match shared.state.lock() {
                        Ok(g) => g,
                        Err(_) => std::process::abort(),
                    };
                    w.bg_error = Some(e);
                    w.metrics.background_errors = w.metrics.background_errors.saturating_add(1);
                    w.record_event(DbEventKind::BackgroundError);
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
            w.record_event(DbEventKind::CompactionStart);
            if let Err(e) = maybe_compact_levels(&db_dir, &mut w) {
                w.bg_error = Some(e);
                w.metrics.background_errors = w.metrics.background_errors.saturating_add(1);
                w.record_event(DbEventKind::BackgroundError);
                shared.cv.notify_all();
                return;
            }
            w.metrics.compactions = w.metrics.compactions.saturating_add(1);
            w.record_event(DbEventKind::CompactionFinish);
            let version_snapshot = w.version.clone();
            let target = w.options.manifest_checkpoint_target_bytes;
            match w.manifest.maybe_checkpoint(&version_snapshot, target) {
                Ok(Some(manifest_number)) => {
                    w.metrics.checkpoints = w.metrics.checkpoints.saturating_add(1);
                    w.record_event(DbEventKind::CheckpointFinish { manifest_number });
                }
                Ok(None) => {}
                Err(e) => {
                    w.bg_error = Some(e);
                    w.metrics.background_errors = w.metrics.background_errors.saturating_add(1);
                    w.record_event(DbEventKind::BackgroundError);
                    shared.cv.notify_all();
                    return;
                }
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

    struct Source {
        scanner: crate::sstable::SstScanner,
        current: Option<(Vec<u8>, Vec<u8>)>,
    }

    let mut sources: Vec<Source> = Vec::new();
    for meta in w.version.levels[0].iter().chain(overlap_l1.iter()) {
        let path = db_dir.join(sst_file_name(meta.file_id));
        let reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
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

    let split_limit_bytes = (level_target_bytes(&w.options, 1) / 2).max(1);
    let mut builder_file_id = w.next_file_id;
    w.next_file_id = w.next_file_id.saturating_add(1);
    let mut builder_tmp_path = db_dir.join(sst_temp_file_name(builder_file_id));
    let mut builder_final_path = db_dir.join(sst_file_name(builder_file_id));
    let mut builder = TableBuilder::create(
        &builder_tmp_path,
        w.options.bloom_bits_per_key,
        w.options.sstable_compression,
    )?;
    let mut builder_has_data = false;
    let mut builder_approx_bytes: u64 = 0;
    let mut outputs: Vec<crate::sstable::FileMeta> = Vec::new();
    let now_micros = now_micros();
    let smallest_snapshot = u64::MAX;
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut kept_snapshot_version_for_user_key = false;
    while let Some(Reverse(e)) = heap.pop() {
        let (user_key, seq, value_type) = {
            let parsed = parse_internal_key(&e.key)?;
            (parsed.user_key.to_vec(), parsed.seq, parsed.value_type)
        };
        let is_new_user_key = match current_user_key.as_ref() {
            Some(cur) => cur.as_slice() != user_key.as_slice(),
            None => true,
        };
        if is_new_user_key {
            if builder_has_data && builder_approx_bytes >= split_limit_bytes {
                failpoint::io_err("compaction:before_sst_finish")?;
                failpoint::hit("compaction:before_sst_finish");
                let mut meta = builder.finish(w.options.sync == SyncMode::Yes)?;
                failpoint::io_err("compaction:after_sst_finish")?;
                failpoint::hit("compaction:after_sst_finish");
                meta.file_id = builder_file_id;
                meta.level = 1;
                failpoint::io_err("compaction:before_sst_rename")?;
                failpoint::hit("compaction:before_sst_rename");
                fs::rename(&builder_tmp_path, &builder_final_path)?;
                failpoint::io_err("compaction:after_sst_rename")?;
                failpoint::hit("compaction:after_sst_rename");
                if w.options.sync == SyncMode::Yes {
                    failpoint::io_err("compaction:before_dir_sync")?;
                    failpoint::hit("compaction:before_dir_sync");
                    sync_parent_dir(&builder_final_path)?;
                    failpoint::io_err("compaction:after_dir_sync")?;
                    failpoint::hit("compaction:after_dir_sync");
                }
                outputs.push(meta);

                builder_file_id = w.next_file_id;
                w.next_file_id = w.next_file_id.saturating_add(1);
                builder_tmp_path = db_dir.join(sst_temp_file_name(builder_file_id));
                builder_final_path = db_dir.join(sst_file_name(builder_file_id));
                builder = TableBuilder::create(
                    &builder_tmp_path,
                    w.options.bloom_bits_per_key,
                    w.options.sstable_compression,
                )?;
                builder_has_data = false;
                builder_approx_bytes = 0;
            }
            current_user_key = Some(user_key.clone());
            kept_snapshot_version_for_user_key = false;
        }

        let mut out_key = e.key;
        let mut out_value = e.value;

        if w.options.ttl_filter_from_value_prefix_micros
            && value_type == ValueType::Value
            && out_value.len() >= 8
        {
            let expiry = u64::from_le_bytes(out_value[..8].try_into().unwrap_or([0u8; 8]));
            if expiry <= now_micros {
                out_key = encode_internal_key(&user_key, seq, ValueType::Tombstone);
                out_value.clear();
            }
        }

        if w.options.drop_obsolete_versions_during_compaction
            && kept_snapshot_version_for_user_key
            && value_type != ValueType::RangeTombstone
            && value_type != ValueType::MergeOperand
        {
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
            continue;
        }

        builder.add(&out_key, &out_value)?;
        builder_has_data = true;
        builder_approx_bytes = builder_approx_bytes.saturating_add(
            (out_key.len() as u64)
                .saturating_add(out_value.len() as u64)
                .saturating_add(16),
        );
        if w.options.drop_obsolete_versions_during_compaction
            && seq <= smallest_snapshot
            && matches!(value_type, ValueType::Value | ValueType::Tombstone)
        {
            kept_snapshot_version_for_user_key = true;
        }
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
    drop(sources);
    if builder_has_data {
        failpoint::io_err("compaction:before_sst_finish")?;
        failpoint::hit("compaction:before_sst_finish");
        let mut meta = builder.finish(w.options.sync == SyncMode::Yes)?;
        failpoint::io_err("compaction:after_sst_finish")?;
        failpoint::hit("compaction:after_sst_finish");
        meta.file_id = builder_file_id;
        meta.level = 1;
        failpoint::io_err("compaction:before_sst_rename")?;
        failpoint::hit("compaction:before_sst_rename");
        fs::rename(&builder_tmp_path, &builder_final_path)?;
        failpoint::io_err("compaction:after_sst_rename")?;
        failpoint::hit("compaction:after_sst_rename");
        if w.options.sync == SyncMode::Yes {
            failpoint::io_err("compaction:before_dir_sync")?;
            failpoint::hit("compaction:before_dir_sync");
            sync_parent_dir(&builder_final_path)?;
            failpoint::io_err("compaction:after_dir_sync")?;
            failpoint::hit("compaction:after_dir_sync");
        }
        outputs.push(meta);
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
        .append_compaction_edit_multi(&outputs, &deletes, last_sequence)?;
    failpoint::io_err("compaction:after_manifest_edit")?;
    failpoint::hit("compaction:after_manifest_edit");
    w.version.last_sequence = w.version.last_sequence.max(last_sequence);

    let overlap_ids: std::collections::BTreeSet<u64> = overlap_ids_vec.iter().copied().collect();
    w.version.levels[0].clear();
    w.version.levels[1].retain(|m| !overlap_ids.contains(&m.file_id));
    w.version.levels[1].extend(outputs);

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

        let mut best: Option<(usize, u64, u64)> = None;
        for level in 1..(max_levels - 1) {
            let bytes = level_total_bytes(&w.version, level);
            let target = level_target_bytes(&w.options, level).max(1);
            if bytes <= target {
                continue;
            }
            match best {
                None => best = Some((level, bytes, target)),
                Some((_best_level, best_bytes, best_target)) => {
                    if (bytes as u128).saturating_mul(best_target as u128)
                        > (best_bytes as u128).saturating_mul(target as u128)
                    {
                        best = Some((level, bytes, target));
                    }
                }
            }
        }
        if let Some((level, _bytes, _target)) = best {
            compact_level_to_next(db_dir, w, level)?;
            continue;
        } else {
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

    if overlaps.is_empty() {
        let mut moved = input.clone();
        moved.level = (level + 1) as u32;
        let deletes = vec![(level as u32, input.file_id)];

        let last_sequence = w.next_seq.saturating_sub(1);
        failpoint::io_err("compaction:before_manifest_edit")?;
        failpoint::hit("compaction:before_manifest_edit");
        w.manifest
            .append_compaction_edit(&moved, &deletes, last_sequence)?;
        failpoint::io_err("compaction:after_manifest_edit")?;
        failpoint::hit("compaction:after_manifest_edit");
        w.version.last_sequence = w.version.last_sequence.max(last_sequence);

        w.version.levels[level].retain(|m| m.file_id != input.file_id);
        w.version.levels[level + 1].push(moved);
        w.metrics.trivial_moves = w.metrics.trivial_moves.saturating_add(1);
        return Ok(());
    }

    struct Source {
        scanner: crate::sstable::SstScanner,
        current: Option<(Vec<u8>, Vec<u8>)>,
    }

    let mut sources: Vec<Source> = Vec::new();
    for meta in std::iter::once(&input).chain(overlaps.iter()) {
        let path = db_dir.join(sst_file_name(meta.file_id));
        let reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
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

    let split_limit_bytes = (level_target_bytes(&w.options, level + 1) / 2).max(1);
    let mut builder_file_id = w.next_file_id;
    w.next_file_id = w.next_file_id.saturating_add(1);
    let mut builder_tmp_path = db_dir.join(sst_temp_file_name(builder_file_id));
    let mut builder_final_path = db_dir.join(sst_file_name(builder_file_id));
    let mut builder = TableBuilder::create(
        &builder_tmp_path,
        w.options.bloom_bits_per_key,
        w.options.sstable_compression,
    )?;
    let mut builder_has_data = false;
    let mut builder_approx_bytes: u64 = 0;
    let mut outputs: Vec<crate::sstable::FileMeta> = Vec::new();
    let now_micros = now_micros();
    let smallest_snapshot = u64::MAX;
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut kept_snapshot_version_for_user_key = false;
    while let Some(Reverse(e)) = heap.pop() {
        let (user_key, seq, value_type) = {
            let parsed = parse_internal_key(&e.key)?;
            (parsed.user_key.to_vec(), parsed.seq, parsed.value_type)
        };
        let is_new_user_key = match current_user_key.as_ref() {
            Some(cur) => cur.as_slice() != user_key.as_slice(),
            None => true,
        };
        if is_new_user_key {
            if builder_has_data && builder_approx_bytes >= split_limit_bytes {
                failpoint::io_err("compaction:before_sst_finish")?;
                failpoint::hit("compaction:before_sst_finish");
                let mut meta = builder.finish(w.options.sync == SyncMode::Yes)?;
                failpoint::io_err("compaction:after_sst_finish")?;
                failpoint::hit("compaction:after_sst_finish");
                meta.file_id = builder_file_id;
                meta.level = (level + 1) as u32;

                failpoint::io_err("compaction:before_sst_rename")?;
                failpoint::hit("compaction:before_sst_rename");
                fs::rename(&builder_tmp_path, &builder_final_path)?;
                failpoint::io_err("compaction:after_sst_rename")?;
                failpoint::hit("compaction:after_sst_rename");
                if w.options.sync == SyncMode::Yes {
                    failpoint::io_err("compaction:before_dir_sync")?;
                    failpoint::hit("compaction:before_dir_sync");
                    sync_parent_dir(&builder_final_path)?;
                    failpoint::io_err("compaction:after_dir_sync")?;
                    failpoint::hit("compaction:after_dir_sync");
                }
                outputs.push(meta);

                builder_file_id = w.next_file_id;
                w.next_file_id = w.next_file_id.saturating_add(1);
                builder_tmp_path = db_dir.join(sst_temp_file_name(builder_file_id));
                builder_final_path = db_dir.join(sst_file_name(builder_file_id));
                builder = TableBuilder::create(
                    &builder_tmp_path,
                    w.options.bloom_bits_per_key,
                    w.options.sstable_compression,
                )?;
                builder_has_data = false;
                builder_approx_bytes = 0;
            }
            current_user_key = Some(user_key.clone());
            kept_snapshot_version_for_user_key = false;
        }

        let mut out_key = e.key;
        let mut out_value = e.value;

        if w.options.ttl_filter_from_value_prefix_micros
            && value_type == ValueType::Value
            && out_value.len() >= 8
        {
            let expiry = u64::from_le_bytes(out_value[..8].try_into().unwrap_or([0u8; 8]));
            if expiry <= now_micros {
                out_key = encode_internal_key(&user_key, seq, ValueType::Tombstone);
                out_value.clear();
            }
        }

        if w.options.drop_obsolete_versions_during_compaction
            && kept_snapshot_version_for_user_key
            && value_type != ValueType::RangeTombstone
            && value_type != ValueType::MergeOperand
        {
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
            continue;
        }

        builder.add(&out_key, &out_value)?;
        builder_has_data = true;
        builder_approx_bytes = builder_approx_bytes.saturating_add(
            (out_key.len() as u64)
                .saturating_add(out_value.len() as u64)
                .saturating_add(16),
        );
        if w.options.drop_obsolete_versions_during_compaction
            && seq <= smallest_snapshot
            && matches!(value_type, ValueType::Value | ValueType::Tombstone)
        {
            kept_snapshot_version_for_user_key = true;
        }
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

    drop(sources);
    if builder_has_data {
        failpoint::io_err("compaction:before_sst_finish")?;
        failpoint::hit("compaction:before_sst_finish");
        let mut meta = builder.finish(w.options.sync == SyncMode::Yes)?;
        failpoint::io_err("compaction:after_sst_finish")?;
        failpoint::hit("compaction:after_sst_finish");
        meta.file_id = builder_file_id;
        meta.level = (level + 1) as u32;

        failpoint::io_err("compaction:before_sst_rename")?;
        failpoint::hit("compaction:before_sst_rename");
        fs::rename(&builder_tmp_path, &builder_final_path)?;
        failpoint::io_err("compaction:after_sst_rename")?;
        failpoint::hit("compaction:after_sst_rename");
        if w.options.sync == SyncMode::Yes {
            failpoint::io_err("compaction:before_dir_sync")?;
            failpoint::hit("compaction:before_dir_sync");
            sync_parent_dir(&builder_final_path)?;
            failpoint::io_err("compaction:after_dir_sync")?;
            failpoint::hit("compaction:after_dir_sync");
        }
        outputs.push(meta);
    }

    let mut deletes: Vec<(u32, u64)> = Vec::new();
    deletes.push((level as u32, input.file_id));
    deletes.extend(overlaps.iter().map(|m| ((level + 1) as u32, m.file_id)));

    let last_sequence = w.next_seq.saturating_sub(1);
    failpoint::io_err("compaction:before_manifest_edit")?;
    failpoint::hit("compaction:before_manifest_edit");
    w.manifest
        .append_compaction_edit_multi(&outputs, &deletes, last_sequence)?;
    failpoint::io_err("compaction:after_manifest_edit")?;
    failpoint::hit("compaction:after_manifest_edit");
    w.version.last_sequence = w.version.last_sequence.max(last_sequence);

    w.version.levels[level].retain(|m| m.file_id != input.file_id);
    let overlap_ids: std::collections::BTreeSet<u64> = overlaps.iter().map(|m| m.file_id).collect();
    w.version.levels[level + 1].retain(|m| !overlap_ids.contains(&m.file_id));
    w.version.levels[level + 1].extend(outputs);

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
    cf_id: u32,
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

    let seek_prefix = encode_internal_key(&cf_id.to_be_bytes(), u64::MAX, ValueType::Tombstone);
    for level in &w.version.levels {
        for meta in level {
            let path = db_dir.join(sst_file_name(meta.file_id));
            let reader = TableReader::open_with_cache(path, Some(w.block_cache.clone()))?;
            let mut it = reader.scanner();
            it.seek(&seek_prefix)?;
            let mut items = Vec::new();
            while let Some((k, v)) = it.next_item()? {
                if internal_key_past_end_for_cf(&k, cf_id, range)? {
                    break;
                }
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
    let mut active_range_tombstones: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut current_in_range = false;
    let mut current_base: Option<Option<Vec<u8>>> = None;
    let mut current_operands: Vec<Vec<u8>> = Vec::new();

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
        let Ok((entry_cf_id, user_key)) = split_cf_user_key(parsed.user_key) else {
            continue;
        };
        if entry_cf_id != cf_id {
            if entry_cf_id > cf_id {
                break;
            }
            continue;
        }
        active_range_tombstones.retain(|(end, _)| user_key < end.as_slice());

        if parsed.value_type == ValueType::RangeTombstone {
            let Ok((end_cf_id, end_user_key)) = split_cf_user_key(&e.value) else {
                continue;
            };
            if end_cf_id != cf_id {
                continue;
            }
            if end_user_key > user_key {
                active_range_tombstones.push((end_user_key.to_vec(), parsed.seq));
            }
            continue;
        }

        let is_new_key = current_user_key
            .as_ref()
            .is_none_or(|k| k.as_slice() != user_key);
        if is_new_key {
            if let Some(k) = current_user_key.take()
                && current_in_range
            {
                if current_operands.is_empty() {
                    if let Some(Some(v)) = current_base.take() {
                        out.push((k, v));
                    }
                } else {
                    let existing = match &current_base {
                        Some(Some(v)) => Some(v.as_slice()),
                        _ => None,
                    };
                    let operand_slices: Vec<&[u8]> = current_operands
                        .iter()
                        .rev()
                        .map(|v| v.as_slice())
                        .collect();
                    if let Some(v) =
                        w.options
                            .merge_operator
                            .full_merge(&k, existing, &operand_slices)?
                    {
                        out.push((k, v));
                    }
                }
            }
            current_user_key = Some(user_key.to_vec());
            current_in_range = user_key_in_range(user_key, range);
            current_base = None;
            current_operands.clear();
        }

        if !current_in_range {
            continue;
        }

        let range_seq = active_range_tombstones
            .iter()
            .filter_map(|(end, seq)| (user_key < end.as_slice()).then_some(*seq))
            .max();

        if current_base.is_some() {
            continue;
        }

        if range_seq.is_some_and(|r| r >= parsed.seq) {
            current_base = Some(None);
            continue;
        }

        match parsed.value_type {
            ValueType::Value => current_base = Some(Some(e.value)),
            ValueType::Tombstone => current_base = Some(None),
            ValueType::MergeOperand => current_operands.push(e.value),
            ValueType::RangeTombstone => {}
        }
    }

    if let Some(k) = current_user_key.take()
        && current_in_range
    {
        if current_operands.is_empty() {
            if let Some(Some(v)) = current_base.take() {
                out.push((k, v));
            }
        } else {
            let existing = match &current_base {
                Some(Some(v)) => Some(v.as_slice()),
                _ => None,
            };
            let operand_slices: Vec<&[u8]> = current_operands
                .iter()
                .rev()
                .map(|v| v.as_slice())
                .collect();
            if let Some(v) = w
                .options
                .merge_operator
                .full_merge(&k, existing, &operand_slices)?
            {
                out.push((k, v));
            }
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

fn successor_key(mut key: Vec<u8>) -> Option<Vec<u8>> {
    for i in (0..key.len()).rev() {
        if key[i] != u8::MAX {
            key[i] = key[i].wrapping_add(1);
            key.truncate(i + 1);
            return Some(key);
        }
    }
    None
}

fn internal_key_past_end_for_cf(internal_key: &[u8], cf_id: u32, range: &Range) -> Result<bool> {
    let parsed = parse_internal_key(internal_key)?;
    let (entry_cf_id, user_key) = split_cf_user_key(parsed.user_key)?;
    if entry_cf_id > cf_id {
        return Ok(true);
    }
    if entry_cf_id < cf_id {
        return Ok(false);
    }
    match &range.end {
        Bound::Unbounded => Ok(false),
        Bound::Included(k) => Ok(user_key > k.as_slice()),
        Bound::Excluded(k) => Ok(user_key >= k.as_slice()),
    }
}

fn encode_cf_user_key(cf_id: u32, user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + user_key.len());
    out.extend_from_slice(&cf_id.to_be_bytes());
    out.extend_from_slice(user_key);
    out
}

fn split_cf_user_key(user_key: &[u8]) -> Result<(u32, &[u8])> {
    if user_key.len() < 4 {
        return Err(GraniteError::Corrupt("missing cf prefix"));
    }
    let cf_id = u32::from_be_bytes(
        user_key[..4]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("cf id bytes"))?,
    );
    Ok((cf_id, &user_key[4..]))
}

fn set_batch_cf_id(cf_id: u32, batch: WriteBatch) -> WriteBatch {
    let ops = batch
        .ops
        .into_iter()
        .map(|op| match op {
            WriteOp::Put {
                cf_id: _,
                key,
                value,
            } => WriteOp::Put { cf_id, key, value },
            WriteOp::Delete { cf_id: _, key } => WriteOp::Delete { cf_id, key },
            WriteOp::DeleteRange {
                cf_id: _,
                start,
                end,
            } => WriteOp::DeleteRange { cf_id, start, end },
            WriteOp::Merge {
                cf_id: _,
                key,
                value,
            } => WriteOp::Merge { cf_id, key, value },
        })
        .collect();
    WriteBatch { ops }
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
            WriteOp::Put { cf_id, key, value } => {
                let k = encode_cf_user_key(*cf_id, key);
                memtable.insert(&k, seq, ValueType::Value, value.clone());
            }
            WriteOp::Delete { cf_id, key } => {
                let k = encode_cf_user_key(*cf_id, key);
                memtable.insert(&k, seq, ValueType::Tombstone, Vec::new());
            }
            WriteOp::DeleteRange { cf_id, start, end } => {
                let start_k = encode_cf_user_key(*cf_id, start);
                let end_k = encode_cf_user_key(*cf_id, end);
                memtable.insert(&start_k, seq, ValueType::RangeTombstone, end_k);
            }
            WriteOp::Merge { cf_id, key, value } => {
                let k = encode_cf_user_key(*cf_id, key);
                memtable.insert(&k, seq, ValueType::MergeOperand, value.clone());
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
    fn compaction_can_split_into_multiple_output_files() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 0,
            l0_stop_trigger: 0,
            max_levels: 2,
            level1_target_bytes: 200,
            level_multiplier: 1,
            ..Options::default()
        };

        let db = DB::open(&path, opts).unwrap();
        for i in 0..20u8 {
            let k = [b'k', i];
            db.put(&k, &[b'x'; 80]).unwrap();
        }

        for _ in 0..500 {
            let w = db.shared.state.lock().unwrap();
            if w.version.levels[0].is_empty() && w.version.levels[1].len() >= 2 {
                break;
            }
            drop(w);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let w = db.shared.state.lock().unwrap();
        assert!(w.version.levels[0].is_empty());
        assert!(w.version.levels[1].len() >= 2);
        drop(w);

        assert_eq!(db.get(b"k\x00").unwrap(), Some(vec![b'x'; 80]));
        assert_eq!(db.get(b"k\x13").unwrap(), Some(vec![b'x'; 80]));
        db.close().unwrap();
    }

    #[test]
    fn merge_appends_value() {
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

        db.put(b"k", b"v1").unwrap();
        db.merge(b"k", b"v2").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v1v2".to_vec()));
        db.close().unwrap();
    }

    #[test]
    fn delete_range_deletes_keys_in_range() {
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
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();

        db.delete_range(Range {
            start: Bound::Included(b"b".to_vec()),
            end: Bound::Excluded(b"c".to_vec()),
        })
        .unwrap();

        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), None);
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
        db.close().unwrap();
    }

    #[test]
    fn delete_range_does_not_delete_newer_puts() {
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
        db.put(b"b", b"2").unwrap();
        db.delete_range(Range {
            start: Bound::Included(b"a".to_vec()),
            end: Bound::Excluded(b"z".to_vec()),
        })
        .unwrap();
        db.put(b"b", b"new").unwrap();

        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), Some(b"new".to_vec()));
        db.close().unwrap();
    }

    #[test]
    fn iter_respects_range_tombstones_starting_before_range_start() {
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
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.delete_range(Range {
            start: Bound::Included(b"a".to_vec()),
            end: Bound::Excluded(b"z".to_vec()),
        })
        .unwrap();

        let items: Vec<_> = db
            .iter(Range {
                start: Bound::Included(b"b".to_vec()),
                end: Bound::Excluded(b"d".to_vec()),
            })
            .unwrap()
            .collect();
        assert!(items.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn compaction_drops_obsolete_versions_when_enabled() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 0,
            l0_stop_trigger: 1000,
            drop_obsolete_versions_during_compaction: true,
            ..Options::default()
        };

        let db = DB::open(&path, opts).unwrap();
        db.put(b"k", b"v1").unwrap();
        db.put(b"k", b"v2").unwrap();
        db.close().unwrap();

        let db2 = DB::open(&path, Options::default()).unwrap();
        let w = db2.shared.state.lock().unwrap();
        let file_ids: Vec<u64> = w
            .version
            .levels
            .iter()
            .flat_map(|l| l.iter().map(|m| m.file_id))
            .collect();
        drop(w);

        let mut count = 0usize;
        for file_id in file_ids {
            let sst_path = path.join(sst_file_name(file_id));
            let reader = TableReader::open_with_cache(sst_path, None).unwrap();
            let mut it = reader.scanner();
            while let Some((k, _v)) = it.next_item().unwrap() {
                let parsed = parse_internal_key(&k).unwrap();
                if parsed.user_key == encode_cf_user_key(0, b"k").as_slice() {
                    count += 1;
                }
            }
        }
        assert_eq!(count, 1);
        db2.close().unwrap();
    }

    #[test]
    fn ttl_filter_turns_expired_values_into_tombstones_in_compaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = Options {
            sync: SyncMode::No,
            memtable_max_bytes: 1,
            l0_slowdown_trigger: 0,
            l0_stop_trigger: 1000,
            ttl_filter_from_value_prefix_micros: true,
            ..Options::default()
        };

        let db = DB::open(&path, opts).unwrap();
        let mut expired = Vec::new();
        expired.extend_from_slice(&0u64.to_le_bytes());
        expired.extend_from_slice(b"v");
        db.put(b"k", &expired).unwrap();
        db.put(b"x", b"y").unwrap();
        db.close().unwrap();

        let db2 = DB::open(&path, Options::default()).unwrap();
        assert_eq!(db2.get(b"k").unwrap(), None);
        let w = db2.shared.state.lock().unwrap();
        let file_ids: Vec<u64> = w
            .version
            .levels
            .iter()
            .flat_map(|l| l.iter().map(|m| m.file_id))
            .collect();
        drop(w);

        let mut saw_tombstone = false;
        for file_id in file_ids {
            let sst_path = path.join(sst_file_name(file_id));
            let reader = TableReader::open_with_cache(sst_path, None).unwrap();
            let mut it = reader.scanner();
            while let Some((k, v)) = it.next_item().unwrap() {
                let parsed = parse_internal_key(&k).unwrap();
                if parsed.user_key == encode_cf_user_key(0, b"k").as_slice() {
                    assert_eq!(parsed.value_type, ValueType::Tombstone);
                    assert!(v.is_empty());
                    saw_tombstone = true;
                }
            }
        }
        assert!(saw_tombstone);
        db2.close().unwrap();
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
            ..Options::default()
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

    #[test]
    fn trivial_move_compaction_moves_file_without_rewrite() {
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
            ..Options::default()
        };

        let db = DB::open(&path, opts).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        for _ in 0..500 {
            if db.metrics().unwrap().trivial_moves >= 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(db.metrics().unwrap().trivial_moves >= 1);

        let w = db.shared.state.lock().unwrap();
        assert!(w.version.levels.len() >= 3);
        assert!(w.version.levels[1].is_empty());
        assert_eq!(w.version.levels[2].len(), 1);
        drop(w);
        db.close().unwrap();
    }
}
