use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{GraniteError, Result};
use crate::internal_key::{
    INTERNAL_KEY_SUFFIX_LEN, ValueType, cmp_internal_key_bytes, cmp_internal_key_bytes_unchecked,
    parse_internal_key,
};

const DATA_BLOCK_TARGET_BYTES: usize = 4 * 1024;
const BLOCK_TRAILER_LEN: usize = 4 + 1;
const FOOTER_LEN: usize = 8 * 4 + 8;
const MAGIC_U64_LE: u64 = 0x4752_414E_4954_4531;

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub level: u32,
    pub file_id: u64,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

#[derive(Clone, Debug)]
struct IndexEntry {
    sep_key: Vec<u8>,
    block_offset: u64,
    block_len: u32,
}

pub struct TableBuilder {
    file: File,
    data_block_buf: Vec<u8>,
    data_block_last_key: Vec<u8>,
    index: Vec<IndexEntry>,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
}

impl TableBuilder {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            file,
            data_block_buf: Vec::new(),
            data_block_last_key: Vec::new(),
            index: Vec::new(),
            smallest_key: None,
            largest_key: None,
        })
    }

    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) -> Result<()> {
        if self.smallest_key.is_none() {
            self.smallest_key = Some(internal_key.to_vec());
        }
        self.largest_key = Some(internal_key.to_vec());

        let entry_len = 4 + internal_key.len() + 4 + value.len();
        if !self.data_block_buf.is_empty()
            && self.data_block_buf.len() + entry_len + BLOCK_TRAILER_LEN > DATA_BLOCK_TARGET_BYTES
        {
            self.flush_data_block()?;
        }

        self.data_block_buf
            .extend_from_slice(&(internal_key.len() as u32).to_le_bytes());
        self.data_block_buf.extend_from_slice(internal_key);
        self.data_block_buf
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.data_block_buf.extend_from_slice(value);
        self.data_block_last_key.clear();
        self.data_block_last_key.extend_from_slice(internal_key);
        Ok(())
    }

    pub fn finish(mut self, sync: bool) -> Result<FileMeta> {
        if !self.data_block_buf.is_empty() {
            self.flush_data_block()?;
        }

        let index_off = self.file.seek(SeekFrom::End(0))?;
        let index_bytes = encode_index_block(&self.index);
        self.file.write_all(&index_bytes)?;
        let index_len = index_bytes.len() as u64;

        let filter_off = 0u64;
        let filter_len = 0u64;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&index_off.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&filter_off.to_le_bytes());
        footer.extend_from_slice(&filter_len.to_le_bytes());
        footer.extend_from_slice(&MAGIC_U64_LE.to_le_bytes());
        self.file.write_all(&footer)?;

        if sync {
            self.file.sync_data()?;
        }
        let file_size = self.file.metadata()?.len();
        Ok(FileMeta {
            level: 0,
            file_id: 0,
            file_size,
            smallest_key: self
                .smallest_key
                .ok_or(GraniteError::Corrupt("empty table"))?,
            largest_key: self
                .largest_key
                .ok_or(GraniteError::Corrupt("empty table"))?,
        })
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let block_offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&self.data_block_buf)?;
        let crc = crc32c::crc32c(&self.data_block_buf);
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&[0u8])?;
        let block_len = (self.data_block_buf.len() + BLOCK_TRAILER_LEN) as u32;

        let sep_key = self.data_block_last_key.clone();
        if sep_key.is_empty() {
            return Err(GraniteError::Corrupt("empty data block"));
        }
        self.index.push(IndexEntry {
            sep_key,
            block_offset,
            block_len,
        });

        self.data_block_buf.clear();
        self.data_block_last_key.clear();
        Ok(())
    }
}

pub struct TableReader {
    file: File,
    index: Vec<IndexEntry>,
}

impl TableReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_LEN as u64 {
            return Err(GraniteError::Corrupt("sst too short for footer"));
        }
        file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer = vec![0u8; FOOTER_LEN];
        file.read_exact(&mut footer)?;

        let index_off = read_u64_le(&footer[0..8])?;
        let index_len = read_u64_le(&footer[8..16])?;
        let filter_off = read_u64_le(&footer[16..24])?;
        let filter_len = read_u64_le(&footer[24..32])?;
        let magic = read_u64_le(&footer[32..40])?;
        if magic != MAGIC_U64_LE {
            return Err(GraniteError::Corrupt("bad sst magic"));
        }
        if filter_off != 0 || filter_len != 0 {
            return Err(GraniteError::InvalidArgument("filters not supported in v1"));
        }
        if index_off + index_len + FOOTER_LEN as u64 != len {
            return Err(GraniteError::Corrupt("bad sst footer offsets"));
        }

        file.seek(SeekFrom::Start(index_off))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;
        let index = decode_index_block(&index_bytes)?;
        validate_index(&index)?;

        Ok(Self { file, index })
    }

    pub fn get_by_seek_key_prefix(
        &mut self,
        seek_internal_key: &[u8],
        user_key: &[u8],
        read_seq: u64,
        mut decide: impl FnMut(u8, &[u8]) -> Option<crate::memtable::GetDecision>,
    ) -> Result<Option<crate::memtable::GetDecision>> {
        let start_block = self.find_block(seek_internal_key);
        for block_idx in start_block..self.index.len() {
            let entries = self.read_block_entries(block_idx)?;
            for (k, v) in entries {
                let parsed = crate::internal_key::parse_internal_key(&k)?;
                if parsed.user_key < user_key {
                    continue;
                }
                if parsed.user_key > user_key {
                    return Ok(None);
                }
                if parsed.seq > read_seq {
                    continue;
                }
                let vt = parsed.value_type as u8;
                if let Some(d) = decide(vt, &v) {
                    return Ok(Some(d));
                }
            }
        }
        Ok(None)
    }

    pub fn scanner(self) -> SstScanner {
        SstScanner {
            reader: self,
            block_idx: 0,
            entry_idx: 0,
            entries: Vec::new(),
        }
    }

    fn find_block(&self, seek_internal_key: &[u8]) -> usize {
        match self
            .index
            .binary_search_by(|e| cmp_internal_key_bytes_unchecked(&e.sep_key, seek_internal_key))
        {
            Ok(i) => i,
            Err(i) => i,
        }
    }

    fn read_block_entries(&mut self, block_idx: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let entry = self
            .index
            .get(block_idx)
            .ok_or(GraniteError::Corrupt("block idx out of range"))?
            .clone();

        self.file.seek(SeekFrom::Start(entry.block_offset))?;
        let mut block = vec![0u8; entry.block_len as usize];
        self.file.read_exact(&mut block)?;

        if block.len() < BLOCK_TRAILER_LEN {
            return Err(GraniteError::Corrupt("block too short"));
        }
        let payload_len = block.len() - BLOCK_TRAILER_LEN;
        let payload = &block[..payload_len];
        let want = crc32c::crc32c(payload);
        let got = read_u32_le(&block[payload_len..payload_len + 4])?;
        if want != got {
            return Err(GraniteError::Corrupt("block checksum mismatch"));
        }
        let compression = block[payload_len + 4];
        if compression != 0 {
            return Err(GraniteError::InvalidArgument("compression not supported"));
        }

        decode_data_block_entries(payload)
    }
}

pub struct SstScanner {
    reader: TableReader,
    block_idx: usize,
    entry_idx: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl SstScanner {
    pub fn next_item(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if self.entry_idx < self.entries.len() {
                let item = self.entries[self.entry_idx].clone();
                self.entry_idx += 1;
                return Ok(Some(item));
            }
            if self.block_idx >= self.reader.index.len() {
                return Ok(None);
            }
            self.entries = self.reader.read_block_entries(self.block_idx)?;
            self.block_idx += 1;
            self.entry_idx = 0;
        }
    }
}

fn encode_index_block(index: &[IndexEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(index.len() as u32).to_le_bytes());
    for e in index {
        out.extend_from_slice(&(e.sep_key.len() as u32).to_le_bytes());
        out.extend_from_slice(&e.sep_key);
        out.extend_from_slice(&e.block_offset.to_le_bytes());
        out.extend_from_slice(&e.block_len.to_le_bytes());
    }
    out
}

fn decode_index_block(mut bytes: &[u8]) -> Result<Vec<IndexEntry>> {
    let count = read_u32(&mut bytes)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let sep_key = read_bytes(&mut bytes)?;
        let block_offset = read_u64(&mut bytes)?;
        let block_len = read_u32(&mut bytes)?;
        out.push(IndexEntry {
            sep_key,
            block_offset,
            block_len,
        });
    }
    if !bytes.is_empty() {
        return Err(GraniteError::Corrupt("trailing bytes in index block"));
    }
    Ok(out)
}

fn decode_data_block_entries(mut bytes: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut prev_key: Option<Vec<u8>> = None;
    while !bytes.is_empty() {
        let key = read_bytes(&mut bytes)?;
        let value = read_bytes(&mut bytes)?;
        if key.len() < INTERNAL_KEY_SUFFIX_LEN {
            return Err(GraniteError::Corrupt("internal key too short"));
        }
        let parsed = parse_internal_key(&key)?;
        match parsed.value_type {
            ValueType::Value | ValueType::Tombstone => {}
        }
        if let Some(prev) = &prev_key
            && cmp_internal_key_bytes(prev, &key)?.is_gt()
        {
            return Err(GraniteError::Corrupt("data block not sorted"));
        }
        prev_key = Some(key.clone());
        out.push((key, value));
    }
    Ok(out)
}

fn validate_index(index: &[IndexEntry]) -> Result<()> {
    let mut prev: Option<&[u8]> = None;
    for e in index {
        if e.sep_key.len() < INTERNAL_KEY_SUFFIX_LEN {
            return Err(GraniteError::Corrupt("index key too short"));
        }
        let _ = parse_internal_key(&e.sep_key)?;
        if let Some(prev) = prev
            && cmp_internal_key_bytes(prev, &e.sep_key)?.is_gt()
        {
            return Err(GraniteError::Corrupt("index not sorted"));
        }
        prev = Some(&e.sep_key);
    }
    Ok(())
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

fn read_u64(bytes: &mut &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(GraniteError::Corrupt("unexpected eof"));
    }
    let v = u64::from_le_bytes(
        bytes[..8]
            .try_into()
            .map_err(|_| GraniteError::Corrupt("u64"))?,
    );
    *bytes = &bytes[8..];
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

fn read_u32_le(bytes: &[u8]) -> Result<u32> {
    if bytes.len() != 4 {
        return Err(GraniteError::Corrupt("bad u32 length"));
    }
    Ok(u32::from_le_bytes(
        bytes.try_into().map_err(|_| GraniteError::Corrupt("u32"))?,
    ))
}

fn read_u64_le(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(GraniteError::Corrupt("bad u64 length"));
    }
    Ok(u64::from_le_bytes(
        bytes.try_into().map_err(|_| GraniteError::Corrupt("u64"))?,
    ))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::internal_key::{ValueType, encode_internal_key};
    use crate::memtable::GetDecision;

    use super::*;

    #[test]
    fn sstable_roundtrip_get() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.sst");

        let mut builder = TableBuilder::create(&path).unwrap();
        builder
            .add(&encode_internal_key(b"a", 2, ValueType::Tombstone), b"")
            .unwrap();
        builder
            .add(&encode_internal_key(b"a", 1, ValueType::Value), b"v1")
            .unwrap();
        builder
            .add(&encode_internal_key(b"b", 3, ValueType::Value), b"vb")
            .unwrap();
        let mut meta = builder.finish(false).unwrap();
        meta.file_id = 1;
        assert!(meta.file_size > 0);

        let mut reader = TableReader::open(&path).unwrap();
        let seek = encode_internal_key(b"a", u64::MAX, ValueType::Tombstone);
        let got = reader
            .get_by_seek_key_prefix(&seek, b"a", u64::MAX, |vt, v| match vt {
                0 => Some(GetDecision::Deleted),
                1 => Some(GetDecision::Value(v.to_vec())),
                _ => None,
            })
            .unwrap();
        assert_eq!(got, Some(GetDecision::Deleted));
    }
}
