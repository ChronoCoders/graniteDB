#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    Yes,
    No,
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
        }
    }
}
