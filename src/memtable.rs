use std::collections::BTreeMap;
use std::ops::Bound;

use crate::error::Result;
use crate::internal_key::{InternalKey, ValueType};

#[derive(Clone, Debug, Default)]
pub struct MemTable {
    map: BTreeMap<InternalKey, Vec<u8>>,
    approx_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetDecision {
    Value(Vec<u8>),
    Deleted,
}

impl MemTable {
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    pub fn insert(&mut self, user_key: &[u8], seq: u64, value_type: ValueType, value: Vec<u8>) {
        let key = InternalKey::new(user_key.to_vec(), seq, value_type);
        self.approx_bytes += user_key.len() + 9 + value.len();
        self.map.insert(key, value);
    }

    pub fn get(&self, user_key: &[u8], read_seq: u64) -> Result<Option<GetDecision>> {
        let start = InternalKey::new(user_key.to_vec(), u64::MAX, ValueType::Tombstone);
        let it = self.map.range((Bound::Included(start), Bound::Unbounded));
        for (k, v) in it {
            if k.user_key() != user_key {
                break;
            }
            if k.seq() > read_seq {
                continue;
            }
            return Ok(Some(match k.value_type() {
                ValueType::Value => GetDecision::Value(v.clone()),
                ValueType::Tombstone => GetDecision::Deleted,
            }));
        }
        Ok(None)
    }

    pub fn latest_seq(&self, user_key: &[u8]) -> Option<u64> {
        let start = InternalKey::new(user_key.to_vec(), u64::MAX, ValueType::Tombstone);
        let mut it = self.map.range((Bound::Included(start), Bound::Unbounded));
        if let Some((k, _)) = it.next()
            && k.user_key() == user_key
        {
            return Some(k.seq());
        }
        None
    }

    pub fn iter_internal(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        self.map.iter().map(|(k, v)| (k.to_bytes(), v.clone()))
    }

    pub fn iter_user_entries(&self) -> impl Iterator<Item = (&[u8], u64, ValueType, &[u8])> + '_ {
        self.map
            .iter()
            .map(|(k, v)| (k.user_key(), k.seq(), k.value_type(), v.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memtable_get_does_not_match_prefix_keys() {
        let mut mt = MemTable::default();
        mt.insert(b"a", 1, ValueType::Value, b"va".to_vec());
        mt.insert(b"ab", 2, ValueType::Value, b"vab".to_vec());
        assert_eq!(
            mt.get(b"a", u64::MAX).unwrap(),
            Some(GetDecision::Value(b"va".to_vec()))
        );
        assert_eq!(
            mt.get(b"ab", u64::MAX).unwrap(),
            Some(GetDecision::Value(b"vab".to_vec()))
        );
    }
}
