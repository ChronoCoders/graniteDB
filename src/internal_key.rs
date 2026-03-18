use std::cmp::Ordering;

use crate::error::{GraniteError, Result};

pub const INTERNAL_KEY_SUFFIX_LEN: usize = 8 + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ValueType {
    Tombstone = 0,
    Value = 1,
}

impl ValueType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Tombstone),
            1 => Some(Self::Value),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub seq: u64,
    pub value_type: ValueType,
}

pub fn encode_internal_key(user_key: &[u8], seq: u64, value_type: ValueType) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + INTERNAL_KEY_SUFFIX_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&(u64::MAX - seq).to_be_bytes());
    out.push(value_type as u8);
    out
}

pub fn parse_internal_key(internal_key: &[u8]) -> Result<ParsedInternalKey<'_>> {
    if internal_key.len() < INTERNAL_KEY_SUFFIX_LEN {
        return Err(GraniteError::Corrupt("internal key too short"));
    }
    let user_len = internal_key.len() - INTERNAL_KEY_SUFFIX_LEN;
    let user_key = &internal_key[..user_len];
    let inv_seq = u64::from_be_bytes(
        internal_key[user_len..user_len + 8]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("internal key seq bytes"))?,
    );
    let value_type_u8 = internal_key[user_len + 8];
    let value_type =
        ValueType::from_u8(value_type_u8).ok_or(GraniteError::Corrupt("bad value type"))?;
    Ok(ParsedInternalKey {
        user_key,
        seq: u64::MAX - inv_seq,
        value_type,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalKey {
    user_key: Vec<u8>,
    inv_seq: u64,
    value_type: ValueType,
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq: u64, value_type: ValueType) -> Self {
        Self {
            user_key,
            inv_seq: u64::MAX - seq,
            value_type,
        }
    }

    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    pub fn seq(&self) -> u64 {
        u64::MAX - self.inv_seq
    }

    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.user_key.len() + INTERNAL_KEY_SUFFIX_LEN);
        out.extend_from_slice(&self.user_key);
        out.extend_from_slice(&self.inv_seq.to_be_bytes());
        out.push(self.value_type as u8);
        out
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => match self.inv_seq.cmp(&other.inv_seq) {
                Ordering::Equal => (self.value_type as u8).cmp(&(other.value_type as u8)),
                other => other,
            },
            other => other,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn cmp_internal_key_bytes(a: &[u8], b: &[u8]) -> Result<Ordering> {
    if a.len() < INTERNAL_KEY_SUFFIX_LEN || b.len() < INTERNAL_KEY_SUFFIX_LEN {
        return Err(GraniteError::Corrupt("internal key too short"));
    }
    let a_user_len = a.len() - INTERNAL_KEY_SUFFIX_LEN;
    let b_user_len = b.len() - INTERNAL_KEY_SUFFIX_LEN;

    let a_user = &a[..a_user_len];
    let b_user = &b[..b_user_len];

    match a_user.cmp(b_user) {
        Ordering::Equal => {
            let a_suffix = &a[a_user_len..];
            let b_suffix = &b[b_user_len..];
            Ok(a_suffix.cmp(b_suffix))
        }
        other => Ok(other),
    }
}

pub fn cmp_internal_key_bytes_unchecked(a: &[u8], b: &[u8]) -> Ordering {
    let a_user_len = a.len() - INTERNAL_KEY_SUFFIX_LEN;
    let b_user_len = b.len() - INTERNAL_KEY_SUFFIX_LEN;

    let a_user = &a[..a_user_len];
    let b_user = &b[..b_user_len];

    match a_user.cmp(b_user) {
        Ordering::Equal => {
            let a_suffix = &a[a_user_len..];
            let b_suffix = &b[b_user_len..];
            a_suffix.cmp(b_suffix)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_key_sorts_newer_first_for_same_user_key() {
        let k1 = encode_internal_key(b"k", 5, ValueType::Value);
        let k2 = encode_internal_key(b"k", 7, ValueType::Value);
        assert!(k2 < k1);
    }

    #[test]
    fn internal_key_orders_by_user_key_first() {
        let a = encode_internal_key(b"a", 1, ValueType::Value);
        let b = encode_internal_key(b"b", 100, ValueType::Value);
        assert!(a < b);
    }

    #[test]
    fn internal_key_empty_user_key_orders_before_non_empty_in_comparator() {
        let empty = InternalKey::new(Vec::new(), 1, ValueType::Value);
        let non_empty = InternalKey::new(vec![0], 1, ValueType::Value);
        assert!(empty < non_empty);
    }

    #[test]
    fn parse_roundtrip() {
        let ik = encode_internal_key(b"hello", 42, ValueType::Tombstone);
        let parsed = parse_internal_key(&ik).unwrap();
        assert_eq!(parsed.user_key, b"hello");
        assert_eq!(parsed.seq, 42);
        assert_eq!(parsed.value_type, ValueType::Tombstone);
    }
}
