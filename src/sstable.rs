use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::{GraniteError, Result};
use crate::failpoint;
use crate::internal_key::{
    INTERNAL_KEY_SUFFIX_LEN, ValueType, cmp_internal_key_bytes, cmp_internal_key_bytes_unchecked,
    parse_internal_key,
};

const DATA_BLOCK_TARGET_BYTES: usize = 4 * 1024;
const BLOCK_TRAILER_LEN: usize = 4 + 1;
const FOOTER_LEN: usize = 8 * 4 + 8;
const MAGIC_U64_LE: u64 = 0x4752_414E_4954_4531;

#[derive(Debug)]
pub struct BlockCache {
    inner: Mutex<BlockCacheInner>,
}

#[derive(Debug)]
struct BlockCacheInner {
    capacity_bytes: usize,
    used_bytes: usize,
    next_token: u64,
    entries: std::collections::HashMap<BlockKey, CacheEntry>,
    lru: std::collections::VecDeque<(BlockKey, u64)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct BlockKey {
    file: Arc<str>,
    offset: u64,
    len: u32,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    bytes: Arc<Vec<u8>>,
    size: usize,
    token: u64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(BlockCacheInner {
                capacity_bytes,
                used_bytes: 0,
                next_token: 1,
                entries: std::collections::HashMap::new(),
                lru: std::collections::VecDeque::new(),
            }),
        }
    }

    fn get(&self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        let mut inner = self.inner.lock().ok()?;
        let token = inner.next_token;
        inner.next_token = inner.next_token.saturating_add(1);
        let entry = inner.entries.get_mut(key)?;
        entry.token = token;
        let bytes = entry.bytes.clone();
        inner.lru.push_back((key.clone(), token));
        Some(bytes)
    }

    fn insert(&self, key: BlockKey, bytes: Arc<Vec<u8>>) {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let size = bytes.len();
        if size > inner.capacity_bytes {
            return;
        }
        let token = inner.next_token;
        inner.next_token = inner.next_token.saturating_add(1);
        inner
            .entries
            .insert(key.clone(), CacheEntry { bytes, size, token });
        inner.lru.push_back((key, token));
        inner.used_bytes = inner.used_bytes.saturating_add(size);
        inner.evict_as_needed();
    }
}

impl BlockCacheInner {
    fn evict_as_needed(&mut self) {
        while self.used_bytes > self.capacity_bytes {
            let Some((key, token)) = self.lru.pop_front() else {
                break;
            };
            let Some(entry) = self.entries.get(&key) else {
                continue;
            };
            if entry.token != token {
                continue;
            }
            let removed = self.entries.remove(&key);
            if let Some(r) = removed {
                self.used_bytes = self.used_bytes.saturating_sub(r.size);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct BloomFilter {
    bits: Vec<u8>,
    k: u8,
}

impl BloomFilter {
    fn from_hashes(hashes: &[u32], bits_per_key: u8) -> Option<Self> {
        if hashes.is_empty() || bits_per_key == 0 {
            return None;
        }
        let bits_per_key = bits_per_key.max(1) as usize;
        let mut bit_count = hashes.len().saturating_mul(bits_per_key);
        bit_count = bit_count.max(64);
        let byte_count = bit_count.div_ceil(8);
        bit_count = byte_count * 8;
        let mut bits = vec![0u8; byte_count];
        let k = ((bits_per_key as f64) * 0.69).ceil() as u8;
        let k = k.clamp(1, 30);
        for &h in hashes {
            let (mut h1, mut h2) = bloom_hash_pair_u32(h);
            for _ in 0..k {
                let bitpos = (h1 as usize) % bit_count;
                bits[bitpos / 8] |= 1u8 << (bitpos % 8);
                h1 = h1.wrapping_add(h2);
                h2 = h2.wrapping_mul(0x9E37_79B9);
            }
        }
        Some(Self { bits, k })
    }

    fn may_contain(&self, user_key: &[u8]) -> bool {
        if self.bits.is_empty() || self.k == 0 {
            return true;
        }
        let bit_count = self.bits.len() * 8;
        let h = bloom_hash32(user_key);
        let (mut h1, mut h2) = bloom_hash_pair_u32(h);
        for _ in 0..self.k {
            let bitpos = (h1 as usize) % bit_count;
            if (self.bits[bitpos / 8] & (1u8 << (bitpos % 8))) == 0 {
                return false;
            }
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(0x9E37_79B9);
        }
        true
    }
}

fn bloom_hash32(key: &[u8]) -> u32 {
    let mut h: u32 = 2166136261;
    for &b in key {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    h
}

fn bloom_hash_pair_u32(h: u32) -> (u32, u32) {
    let h1 = h;
    let h2 = h.rotate_left(17) ^ 0x85EB_CA6B;
    (h1, h2)
}

fn encode_filter_block(filter: &BloomFilter) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + filter.bits.len());
    out.push(filter.k);
    out.extend_from_slice(&(filter.bits.len() as u32).to_le_bytes());
    out.extend_from_slice(&filter.bits);
    out
}

fn decode_filter_block(mut bytes: &[u8]) -> Result<BloomFilter> {
    if bytes.len() < 1 + 4 {
        return Err(GraniteError::Corrupt("filter block too short"));
    }
    let k = bytes[0];
    bytes = &bytes[1..];
    let bit_len = read_u32(&mut bytes)? as usize;
    if bytes.len() != bit_len {
        return Err(GraniteError::Corrupt("filter block length mismatch"));
    }
    Ok(BloomFilter {
        bits: bytes.to_vec(),
        k,
    })
}

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
    bloom_hashes: Vec<u32>,
    bloom_bits_per_key: u8,
}

impl TableBuilder {
    pub fn create(path: impl AsRef<Path>, bloom_bits_per_key: u8) -> Result<Self> {
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
            bloom_hashes: Vec::new(),
            bloom_bits_per_key,
        })
    }

    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) -> Result<()> {
        let parsed = parse_internal_key(internal_key)?;
        self.bloom_hashes.push(bloom_hash32(parsed.user_key));
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
        failpoint::write_all("sst:write_index", &mut self.file, &index_bytes)?;
        let index_len = index_bytes.len() as u64;

        let filter = BloomFilter::from_hashes(&self.bloom_hashes, self.bloom_bits_per_key);
        let (filter_off, filter_len) = if let Some(filter) = filter {
            let filter_off = self.file.seek(SeekFrom::End(0))?;
            let payload = encode_filter_block(&filter);
            failpoint::write_all("sst:write_filter", &mut self.file, &payload)?;
            let crc = crc32c::crc32c(&payload);
            failpoint::write_all(
                "sst:write_filter_trailer",
                &mut self.file,
                &crc.to_le_bytes(),
            )?;
            failpoint::write_all("sst:write_filter_trailer", &mut self.file, &[0u8])?;
            (filter_off, (payload.len() + BLOCK_TRAILER_LEN) as u64)
        } else {
            (0u64, 0u64)
        };

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&index_off.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&filter_off.to_le_bytes());
        footer.extend_from_slice(&filter_len.to_le_bytes());
        footer.extend_from_slice(&MAGIC_U64_LE.to_le_bytes());
        failpoint::write_all("sst:write_footer", &mut self.file, &footer)?;

        if sync {
            failpoint::sync_data("sst:sync", &self.file)?;
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
        failpoint::write_all("sst:write_data", &mut self.file, &self.data_block_buf)?;
        let crc = crc32c::crc32c(&self.data_block_buf);
        failpoint::write_all("sst:write_trailer", &mut self.file, &crc.to_le_bytes())?;
        failpoint::write_all("sst:write_trailer", &mut self.file, &[0u8])?;
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
    filter: Option<BloomFilter>,
    cache: Option<Arc<BlockCache>>,
    cache_file: Arc<str>,
}

impl TableReader {
    pub fn open_with_cache(path: impl AsRef<Path>, cache: Option<Arc<BlockCache>>) -> Result<Self> {
        let path = path.as_ref();
        let cache_file: Arc<str> = path.to_string_lossy().into_owned().into();
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
        let data_end = len - FOOTER_LEN as u64;
        if index_off + index_len > data_end {
            return Err(GraniteError::Corrupt("bad sst index offsets"));
        }
        if filter_len == 0 {
            if filter_off != 0 {
                return Err(GraniteError::Corrupt("bad sst filter offsets"));
            }
            if index_off + index_len != data_end {
                return Err(GraniteError::Corrupt("bad sst footer offsets"));
            }
        } else {
            if filter_off != index_off + index_len {
                return Err(GraniteError::Corrupt("bad sst filter offsets"));
            }
            if filter_off + filter_len != data_end {
                return Err(GraniteError::Corrupt("bad sst footer offsets"));
            }
        }

        file.seek(SeekFrom::Start(index_off))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;
        let index = decode_index_block(&index_bytes)?;
        validate_index(&index)?;

        let filter = if filter_len == 0 {
            None
        } else {
            let block = read_block_bytes(&mut file, filter_off, filter_len as u32)?;
            let payload = decode_block_payload(&block)?;
            Some(decode_filter_block(payload)?)
        };

        Ok(Self {
            file,
            index,
            filter,
            cache,
            cache_file,
        })
    }

    pub fn get_by_seek_key_prefix(
        &mut self,
        seek_internal_key: &[u8],
        user_key: &[u8],
        read_seq: u64,
        mut decide: impl FnMut(u8, &[u8]) -> Option<crate::memtable::GetDecision>,
    ) -> Result<Option<crate::memtable::GetDecision>> {
        if let Some(filter) = &self.filter
            && !filter.may_contain(user_key)
        {
            return Ok(None);
        }
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

        let key = BlockKey {
            file: self.cache_file.clone(),
            offset: entry.block_offset,
            len: entry.block_len,
        };
        let block = if let Some(cache) = &self.cache {
            if let Some(b) = cache.get(&key) {
                b
            } else {
                let bytes = Arc::new(read_block_bytes(&mut self.file, key.offset, key.len)?);
                cache.insert(key, bytes.clone());
                bytes
            }
        } else {
            Arc::new(read_block_bytes(&mut self.file, key.offset, key.len)?)
        };
        let payload = decode_block_payload(block.as_slice())?;
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
    pub fn seek(&mut self, seek_internal_key: &[u8]) -> Result<()> {
        self.block_idx = self.reader.find_block(seek_internal_key);
        self.entries.clear();
        self.entry_idx = 0;
        if self.block_idx >= self.reader.index.len() {
            return Ok(());
        }
        self.entries = self.reader.read_block_entries(self.block_idx)?;
        self.entry_idx = match self
            .entries
            .binary_search_by(|(k, _)| cmp_internal_key_bytes_unchecked(k, seek_internal_key))
        {
            Ok(i) => i,
            Err(i) => i,
        };
        self.block_idx += 1;
        while self.entry_idx >= self.entries.len() && self.block_idx < self.reader.index.len() {
            self.entries = self.reader.read_block_entries(self.block_idx)?;
            self.block_idx += 1;
            self.entry_idx = 0;
        }
        Ok(())
    }

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

fn read_block_bytes(file: &mut File, offset: u64, len: u32) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut block = vec![0u8; len as usize];
    file.read_exact(&mut block)?;
    Ok(block)
}

fn decode_block_payload(block: &[u8]) -> Result<&[u8]> {
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
    Ok(payload)
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

        let mut builder = TableBuilder::create(&path, 10).unwrap();
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

        let mut reader = TableReader::open_with_cache(&path, None).unwrap();
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
