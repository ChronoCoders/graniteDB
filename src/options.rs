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

#[derive(Clone, Debug)]
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
    pub block_cache_capacity_bytes: usize,
    pub event_log_capacity: usize,
    pub manifest_checkpoint_target_bytes: u64,
    pub drop_obsolete_versions_during_compaction: bool,
    pub ttl_filter_from_value_prefix_micros: bool,
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
            block_cache_capacity_bytes: 64 * 1024 * 1024,
            event_log_capacity: 256,
            manifest_checkpoint_target_bytes: 4 * 1024 * 1024,
            drop_obsolete_versions_during_compaction: true,
            ttl_filter_from_value_prefix_micros: false,
        }
    }
}
