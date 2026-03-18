use crate::error::{GraniteError, Result};
use crate::memtable::{GetDecision, MemTable};
use crate::sync::Mutex;
use crate::write_batch::{WriteBatch, WriteOp};

pub struct MemDb {
    inner: Mutex<State>,
}

struct State {
    memtable: MemTable,
    next_seq: u64,
}

impl MemDb {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(State {
                memtable: MemTable::default(),
                next_seq: 1,
            }),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let batch = WriteBatch::default().put(key, value);
        self.write_batch(batch)
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        let batch = WriteBatch::default().delete(key);
        self.write_batch(batch)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let g = self
            .inner
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        match g.memtable.get(key, u64::MAX)? {
            Some(GetDecision::Value(v)) => Ok(Some(v)),
            Some(GetDecision::Deleted) => Ok(None),
            None => Ok(None),
        }
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| GraniteError::Corrupt("poisoned"))?;
        for op in &batch.ops {
            let seq = g.next_seq;
            g.next_seq = g.next_seq.saturating_add(1);
            match op {
                WriteOp::Put { key, value, .. } => {
                    g.memtable.insert(
                        key,
                        seq,
                        crate::internal_key::ValueType::Value,
                        value.clone(),
                    );
                }
                WriteOp::Delete { key, .. } => {
                    g.memtable.insert(
                        key,
                        seq,
                        crate::internal_key::ValueType::Tombstone,
                        Vec::new(),
                    );
                }
            }
        }
        Ok(())
    }
}

impl Default for MemDb {
    fn default() -> Self {
        Self::new()
    }
}
