#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    Yes,
    No,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    None,
    Lz4,
}

#[derive(Clone)]
pub struct Options {
    pub sync: SyncMode,
    pub memtable_max_bytes: usize,
    pub l0_slowdown_trigger: usize,
    pub l0_stop_trigger: usize,
    pub max_levels: usize,
    pub level1_target_bytes: u64,
    pub level_multiplier: u64,
    pub bloom_bits_per_key: u8,
    pub sstable_compression: Compression,
    pub merge_operator: std::sync::Arc<dyn crate::merge::MergeOperator>,
    pub max_write_bytes_per_sec: u64,
    pub block_cache_capacity_bytes: usize,
    pub event_log_capacity: usize,
    pub manifest_checkpoint_target_bytes: u64,
    pub drop_obsolete_versions_during_compaction: bool,
    pub ttl_filter_from_value_prefix_micros: bool,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("sync", &self.sync)
            .field("memtable_max_bytes", &self.memtable_max_bytes)
            .field("l0_slowdown_trigger", &self.l0_slowdown_trigger)
            .field("l0_stop_trigger", &self.l0_stop_trigger)
            .field("max_levels", &self.max_levels)
            .field("level1_target_bytes", &self.level1_target_bytes)
            .field("level_multiplier", &self.level_multiplier)
            .field("bloom_bits_per_key", &self.bloom_bits_per_key)
            .field("sstable_compression", &self.sstable_compression)
            .field("max_write_bytes_per_sec", &self.max_write_bytes_per_sec)
            .field(
                "block_cache_capacity_bytes",
                &self.block_cache_capacity_bytes,
            )
            .field("event_log_capacity", &self.event_log_capacity)
            .field(
                "manifest_checkpoint_target_bytes",
                &self.manifest_checkpoint_target_bytes,
            )
            .field(
                "drop_obsolete_versions_during_compaction",
                &self.drop_obsolete_versions_during_compaction,
            )
            .field(
                "ttl_filter_from_value_prefix_micros",
                &self.ttl_filter_from_value_prefix_micros,
            )
            .finish()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sync: SyncMode::Yes,
            memtable_max_bytes: 64 * 1024 * 1024,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            max_levels: 4,
            level1_target_bytes: 4 * 1024 * 1024,
            level_multiplier: 10,
            bloom_bits_per_key: 10,
            sstable_compression: Compression::None,
            merge_operator: crate::merge::default_merge_operator(),
            max_write_bytes_per_sec: 0,
            block_cache_capacity_bytes: 64 * 1024 * 1024,
            event_log_capacity: 256,
            manifest_checkpoint_target_bytes: 4 * 1024 * 1024,
            drop_obsolete_versions_during_compaction: true,
            ttl_filter_from_value_prefix_micros: false,
        }
    }
}
