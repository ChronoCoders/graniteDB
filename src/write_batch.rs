use crate::error::{GraniteError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOp {
    Put {
        cf_id: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf_id: u32,
        key: Vec<u8>,
    },
    DeleteRange {
        cf_id: u32,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteBatch {
    pub ops: Vec<WriteOp>,
}

impl WriteBatch {
    pub fn put(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.ops.push(WriteOp::Put {
            cf_id: 0,
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn put_cf(
        mut self,
        cf_id: u32,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        self.ops.push(WriteOp::Put {
            cf_id,
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.ops.push(WriteOp::Delete {
            cf_id: 0,
            key: key.into(),
        });
        self
    }

    pub fn delete_cf(mut self, cf_id: u32, key: impl Into<Vec<u8>>) -> Self {
        self.ops.push(WriteOp::Delete {
            cf_id,
            key: key.into(),
        });
        self
    }

    pub fn delete_range(mut self, start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        self.ops.push(WriteOp::DeleteRange {
            cf_id: 0,
            start: start.into(),
            end: end.into(),
        });
        self
    }

    pub fn delete_range_cf(
        mut self,
        cf_id: u32,
        start: impl Into<Vec<u8>>,
        end: impl Into<Vec<u8>>,
    ) -> Self {
        self.ops.push(WriteOp::DeleteRange {
            cf_id,
            start: start.into(),
            end: end.into(),
        });
        self
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());
        for op in &self.ops {
            match op {
                WriteOp::Put { cf_id, key, value } => {
                    out.push(3u8);
                    out.extend_from_slice(&cf_id.to_le_bytes());
                    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    out.extend_from_slice(key);
                    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    out.extend_from_slice(value);
                }
                WriteOp::Delete { cf_id, key } => {
                    out.push(4u8);
                    out.extend_from_slice(&cf_id.to_le_bytes());
                    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    out.extend_from_slice(key);
                }
                WriteOp::DeleteRange { cf_id, start, end } => {
                    out.push(5u8);
                    out.extend_from_slice(&cf_id.to_le_bytes());
                    out.extend_from_slice(&(start.len() as u32).to_le_bytes());
                    out.extend_from_slice(start);
                    out.extend_from_slice(&(end.len() as u32).to_le_bytes());
                    out.extend_from_slice(end);
                }
            }
        }
        out
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        let count = read_u32(&mut bytes)? as usize;
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            let op_type = read_u8(&mut bytes)?;
            match op_type {
                1 => {
                    let key = read_bytes(&mut bytes)?;
                    let value = read_bytes(&mut bytes)?;
                    ops.push(WriteOp::Put {
                        cf_id: 0,
                        key,
                        value,
                    });
                }
                2 => {
                    let key = read_bytes(&mut bytes)?;
                    ops.push(WriteOp::Delete { cf_id: 0, key });
                }
                3 => {
                    let cf_id = read_u32(&mut bytes)?;
                    let key = read_bytes(&mut bytes)?;
                    let value = read_bytes(&mut bytes)?;
                    ops.push(WriteOp::Put { cf_id, key, value });
                }
                4 => {
                    let cf_id = read_u32(&mut bytes)?;
                    let key = read_bytes(&mut bytes)?;
                    ops.push(WriteOp::Delete { cf_id, key });
                }
                5 => {
                    let cf_id = read_u32(&mut bytes)?;
                    let start = read_bytes(&mut bytes)?;
                    let end = read_bytes(&mut bytes)?;
                    ops.push(WriteOp::DeleteRange { cf_id, start, end });
                }
                _ => return Err(GraniteError::Corrupt("unknown op type")),
            }
        }
        if !bytes.is_empty() {
            return Err(GraniteError::Corrupt("trailing bytes in write batch"));
        }
        Ok(Self { ops })
    }
}

fn read_u8(bytes: &mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(GraniteError::Corrupt("unexpected eof"));
    }
    let v = bytes[0];
    *bytes = &bytes[1..];
    Ok(v)
}

fn read_u32(bytes: &mut &[u8]) -> Result<u32> {
    if bytes.len() < 4 {
        return Err(GraniteError::Corrupt("unexpected eof"));
    }
    let v = u32::from_le_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("u32"))?,
    );
    *bytes = &bytes[4..];
    Ok(v)
}

fn read_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    let len = read_u32(bytes)? as usize;
    if bytes.len() < len {
        return Err(GraniteError::Corrupt("unexpected eof"));
    }
    let out = bytes[..len].to_vec();
    *bytes = &bytes[len..];
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_roundtrip() {
        let batch = WriteBatch::default()
            .put(b"a".to_vec(), b"1".to_vec())
            .delete(b"b".to_vec())
            .put_cf(7, b"k".to_vec(), b"v".to_vec())
            .delete_cf(7, b"k".to_vec())
            .delete_range(b"c".to_vec(), b"d".to_vec())
            .delete_range_cf(7, b"e".to_vec(), b"f".to_vec());
        let enc = batch.encode();
        let dec = WriteBatch::decode(&enc).unwrap();
        assert_eq!(batch, dec);
    }
}
