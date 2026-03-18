#![deny(warnings)]

mod db;
mod error;
mod failpoint;
#[cfg(feature = "fuzzing")]
#[path = "fuzzing.rs"]
mod fuzzing_support;
mod internal_key;
mod manifest;
#[cfg(feature = "loom")]
pub mod memdb;
mod memtable;
mod options;
mod sstable;
pub mod sync;
mod util;
mod wal;
mod write_batch;

pub use crate::db::{DB, DbIterator, Range, Snapshot};
pub use crate::error::{GraniteError, Result};
pub use crate::options::{Options, SyncMode};
pub use crate::write_batch::{WriteBatch, WriteOp};

#[cfg(feature = "fuzzing")]
pub mod fuzzing {
    pub use crate::fuzzing_support::{fuzz_manifest, fuzz_sstable, fuzz_wal};
}
