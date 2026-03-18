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

    pub fn get_with_seq(
        &self,
        user_key: &[u8],
        read_seq: u64,
    ) -> Result<Option<(u64, GetDecision)>> {
        let start = InternalKey::new(user_key.to_vec(), u64::MAX, ValueType::Tombstone);
        let it = self.map.range((Bound::Included(start), Bound::Unbounded));
        for (k, v) in it {
            if k.user_key() != user_key {
                break;
            }
            if k.seq() > read_seq {
                continue;
            }
            match k.value_type() {
                ValueType::Value => return Ok(Some((k.seq(), GetDecision::Value(v.clone())))),
                ValueType::Tombstone => return Ok(Some((k.seq(), GetDecision::Deleted))),
                ValueType::RangeTombstone => continue,
            }
        }
        Ok(None)
    }

    pub fn max_range_tombstone_seq_covering(&self, user_key: &[u8], read_seq: u64) -> Option<u64> {
        if user_key.len() < 4 {
            return None;
        }
        let prefix = &user_key[..4];
        let start = InternalKey::new(prefix.to_vec(), u64::MAX, ValueType::RangeTombstone);
        let end = InternalKey::new(user_key.to_vec(), 0, ValueType::RangeTombstone);
        let it = self
            .map
            .range((Bound::Included(start), Bound::Included(end)));
        let mut max_seq = None;
        for (k, v) in it {
            if k.seq() > read_seq {
                continue;
            }
            if k.value_type() != ValueType::RangeTombstone {
                continue;
            }
            if v.as_slice() <= user_key {
                continue;
            }
            max_seq = Some(max_seq.unwrap_or(0).max(k.seq()));
        }
        max_seq
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
            mt.get_with_seq(b"a", u64::MAX).unwrap().map(|(_s, d)| d),
            Some(GetDecision::Value(b"va".to_vec()))
        );
        assert_eq!(
            mt.get_with_seq(b"ab", u64::MAX).unwrap().map(|(_s, d)| d),
            Some(GetDecision::Value(b"vab".to_vec()))
        );
    }
}
