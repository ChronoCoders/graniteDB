use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{GraniteError, Result};
use crate::failpoint;
use crate::options::SyncMode;
use crate::sstable::FileMeta;
use crate::util::{sync_dir, sync_parent_dir};

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 2 + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Full),
            2 => Some(Self::First),
            3 => Some(Self::Middle),
            4 => Some(Self::Last),
            _ => None,
        }
    }
}

const TAG_ADD_FILE: u8 = 1;
const TAG_DELETE_FILE: u8 = 2;
const TAG_SET_LOG_NUMBER: u8 = 3;
const TAG_SET_LAST_SEQUENCE: u8 = 4;

#[derive(Clone, Debug)]
pub struct VersionSet {
    pub levels: Vec<Vec<FileMeta>>,
    pub log_number: u64,
    pub last_sequence: u64,
}

impl VersionSet {
    pub fn max_file_id(&self) -> u64 {
        self.levels
            .iter()
            .flatten()
            .map(|m| m.file_id)
            .max()
            .unwrap_or(0)
    }

    pub fn contains_file_id(&self, file_id: u64) -> bool {
        self.levels.iter().flatten().any(|m| m.file_id == file_id)
    }
}

impl Default for VersionSet {
    fn default() -> Self {
        Self {
            levels: vec![Vec::new(), Vec::new()],
            log_number: 1,
            last_sequence: 0,
        }
    }
}

pub struct Manifest {
    file: File,
    offset_in_block: usize,
    sync_mode: SyncMode,
}

impl Manifest {
    pub fn open(db_dir: impl AsRef<Path>, sync_mode: SyncMode) -> Result<(Self, VersionSet)> {
        let db_dir = db_dir.as_ref();
        let current_path = db_dir.join("CURRENT");
        let manifest_name = if current_path.exists() {
            read_current(&current_path)?
        } else {
            let name = "MANIFEST-000001".to_string();
            let manifest_path = db_dir.join(&name);
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&manifest_path)?;
            if sync_mode == SyncMode::Yes {
                sync_parent_dir(&manifest_path)?;
            }

            write_current(db_dir, &name, sync_mode)?;
            name
        };

        let path = db_dir.join(&manifest_name);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        let (records, truncated_to) = recover_and_truncate(&mut file)?;
        let version = apply_records(records)?;
        let offset_in_block = (truncated_to as usize) % BLOCK_SIZE;

        Ok((
            Self {
                file,
                offset_in_block,
                sync_mode,
            },
            version,
        ))
    }

    pub fn append_flush_edit(
        &mut self,
        meta: &FileMeta,
        new_log_number: u64,
        last_sequence: u64,
    ) -> Result<()> {
        let mut payload = Vec::new();
        encode_add_file(&mut payload, meta);
        encode_set_log_number(&mut payload, new_log_number);
        encode_set_last_sequence(&mut payload, last_sequence);
        failpoint::hit("manifest:before_append");
        self.append_logical_record(&payload)?;
        failpoint::hit("manifest:after_append");
        self.sync()
    }

    pub fn append_compaction_edit(
        &mut self,
        output: &FileMeta,
        deletes: &[(u32, u64)],
        last_sequence: u64,
    ) -> Result<()> {
        let mut payload = Vec::new();
        encode_add_file(&mut payload, output);
        for (level, file_id) in deletes {
            encode_delete_file(&mut payload, *level, *file_id);
        }
        encode_set_last_sequence(&mut payload, last_sequence);
        failpoint::hit("manifest:before_append");
        self.append_logical_record(&payload)?;
        failpoint::hit("manifest:after_append");
        self.sync()
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.sync_mode == SyncMode::Yes {
            failpoint::hit("manifest:before_sync");
            failpoint::sync_data("manifest:sync", &self.file)?;
            failpoint::hit("manifest:after_sync");
        }
        Ok(())
    }

    fn append_logical_record(&mut self, logical_payload: &[u8]) -> Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let mut remaining = logical_payload;
        let mut is_first = true;

        while !remaining.is_empty() {
            let block_remaining = BLOCK_SIZE - self.offset_in_block;
            if block_remaining < HEADER_SIZE {
                if block_remaining > 0 {
                    let padding = vec![0u8; block_remaining];
                    failpoint::write_all("manifest:write_padding", &mut self.file, &padding)?;
                }
                self.offset_in_block = 0;
                continue;
            }

            let available = block_remaining - HEADER_SIZE;
            let chunk_len = available.min(remaining.len());
            let chunk = &remaining[..chunk_len];
            let is_last = chunk_len == remaining.len();

            let record_type = match (is_first, is_last) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, false) => RecordType::Middle,
                (false, true) => RecordType::Last,
            };

            if chunk_len > u16::MAX as usize {
                return Err(GraniteError::InvalidArgument("manifest fragment too large"));
            }

            let mut crc_input = Vec::with_capacity(1 + chunk.len());
            crc_input.push(record_type as u8);
            crc_input.extend_from_slice(chunk);
            let crc = crc32c::crc32c(&crc_input);

            let mut header = [0u8; HEADER_SIZE];
            header[..4].copy_from_slice(&crc.to_le_bytes());
            header[4..6].copy_from_slice(&(chunk_len as u16).to_le_bytes());
            header[6] = record_type as u8;

            failpoint::write_all("manifest:write_header", &mut self.file, &header)?;
            failpoint::write_all("manifest:write_payload", &mut self.file, chunk)?;

            self.offset_in_block += HEADER_SIZE + chunk_len;
            remaining = &remaining[chunk_len..];
            is_first = false;
        }

        Ok(())
    }
}

fn apply_records(records: Vec<Vec<u8>>) -> Result<VersionSet> {
    let mut version = VersionSet::default();
    for rec in records {
        apply_one_record(&mut version, &rec)?;
    }
    Ok(version)
}

fn apply_one_record(version: &mut VersionSet, mut rec: &[u8]) -> Result<()> {
    while !rec.is_empty() {
        let tag = read_u8(&mut rec)?;
        match tag {
            TAG_ADD_FILE => {
                let level = read_u32(&mut rec)?;
                let file_id = read_u64(&mut rec)?;
                let file_size = read_u64(&mut rec)?;
                let smallest_key = read_bytes(&mut rec)?;
                let largest_key = read_bytes(&mut rec)?;
                let meta = FileMeta {
                    level,
                    file_id,
                    file_size,
                    smallest_key,
                    largest_key,
                };
                let idx = level as usize;
                if version.levels.len() <= idx {
                    version.levels.resize_with(idx + 1, Vec::new);
                }
                version.levels[idx].push(meta);
            }
            TAG_DELETE_FILE => {
                let level = read_u32(&mut rec)?;
                let file_id = read_u64(&mut rec)?;
                let idx = level as usize;
                if version.levels.len() <= idx {
                    version.levels.resize_with(idx + 1, Vec::new);
                }
                version.levels[idx].retain(|m| m.file_id != file_id);
            }
            TAG_SET_LOG_NUMBER => {
                let log_number = read_u64(&mut rec)?;
                version.log_number = log_number;
            }
            TAG_SET_LAST_SEQUENCE => {
                let last_sequence = read_u64(&mut rec)?;
                version.last_sequence = version.last_sequence.max(last_sequence);
            }
            _ => return Err(GraniteError::Corrupt("unknown manifest tag")),
        }
    }
    Ok(())
}

fn encode_add_file(out: &mut Vec<u8>, meta: &FileMeta) {
    out.push(TAG_ADD_FILE);
    out.extend_from_slice(&meta.level.to_le_bytes());
    out.extend_from_slice(&meta.file_id.to_le_bytes());
    out.extend_from_slice(&meta.file_size.to_le_bytes());
    out.extend_from_slice(&(meta.smallest_key.len() as u32).to_le_bytes());
    out.extend_from_slice(&meta.smallest_key);
    out.extend_from_slice(&(meta.largest_key.len() as u32).to_le_bytes());
    out.extend_from_slice(&meta.largest_key);
}

fn encode_delete_file(out: &mut Vec<u8>, level: u32, file_id: u64) {
    out.push(TAG_DELETE_FILE);
    out.extend_from_slice(&level.to_le_bytes());
    out.extend_from_slice(&file_id.to_le_bytes());
}

fn encode_set_log_number(out: &mut Vec<u8>, log_number: u64) {
    out.push(TAG_SET_LOG_NUMBER);
    out.extend_from_slice(&log_number.to_le_bytes());
}

fn encode_set_last_sequence(out: &mut Vec<u8>, last_sequence: u64) {
    out.push(TAG_SET_LAST_SEQUENCE);
    out.extend_from_slice(&last_sequence.to_le_bytes());
}

fn recover_and_truncate(file: &mut File) -> Result<(Vec<Vec<u8>>, u64)> {
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;

    let mut records = Vec::new();
    let mut global_off: u64 = 0;
    let mut last_good_off: u64 = 0;

    let mut pending: Vec<u8> = Vec::new();
    let mut pending_start_off: Option<u64> = None;

    let mut buf = vec![0u8; BLOCK_SIZE];
    while global_off < len {
        let to_read = (len - global_off).min(BLOCK_SIZE as u64) as usize;
        let block = &mut buf[..to_read];
        file.read_exact(block)?;

        let mut pos = 0usize;
        while pos + HEADER_SIZE <= to_read {
            let crc = u32::from_le_bytes(
                block[pos..pos + 4]
                    .try_into()
                    .map_err(|_| GraniteError::Corrupt("manifest header crc bytes"))?,
            );
            let frag_len = u16::from_le_bytes(
                block[pos + 4..pos + 6]
                    .try_into()
                    .map_err(|_| GraniteError::Corrupt("manifest header len bytes"))?,
            ) as usize;
            let typ = block[pos + 6];

            if crc == 0 && frag_len == 0 && typ == 0 {
                break;
            }

            let record_type = RecordType::from_u8(typ)
                .ok_or(GraniteError::Corrupt("manifest bad record type"))?;

            let end = pos + HEADER_SIZE + frag_len;
            if end > to_read {
                return finish_recovery(file, records, pending_start_off.unwrap_or(last_good_off));
            }

            let payload = &block[pos + HEADER_SIZE..end];
            let mut crc_input = Vec::with_capacity(1 + payload.len());
            crc_input.push(typ);
            crc_input.extend_from_slice(payload);
            let want = crc32c::crc32c(&crc_input);
            if want != crc {
                return finish_recovery(file, records, pending_start_off.unwrap_or(last_good_off));
            }

            match record_type {
                RecordType::Full => {
                    if pending_start_off.is_some() {
                        return finish_recovery(
                            file,
                            records,
                            pending_start_off.unwrap_or(last_good_off),
                        );
                    }
                    records.push(payload.to_vec());
                }
                RecordType::First => {
                    if pending_start_off.is_some() {
                        return finish_recovery(
                            file,
                            records,
                            pending_start_off.unwrap_or(last_good_off),
                        );
                    }
                    pending.clear();
                    pending.extend_from_slice(payload);
                    pending_start_off = Some(global_off + pos as u64);
                }
                RecordType::Middle => {
                    if pending_start_off.is_none() {
                        return finish_recovery(file, records, last_good_off);
                    }
                    pending.extend_from_slice(payload);
                }
                RecordType::Last => {
                    match pending_start_off.take() {
                        Some(_) => {}
                        None => return finish_recovery(file, records, last_good_off),
                    };
                    pending.extend_from_slice(payload);
                    records.push(std::mem::take(&mut pending));
                }
            }

            last_good_off = global_off + end as u64;
            pos = end;
        }

        global_off += to_read as u64;
    }

    if let Some(start) = pending_start_off {
        return finish_recovery(file, records, start);
    }

    finish_recovery(file, records, last_good_off)
}

fn finish_recovery(
    file: &mut File,
    records: Vec<Vec<u8>>,
    truncate_to: u64,
) -> Result<(Vec<Vec<u8>>, u64)> {
    file.set_len(truncate_to)?;
    file.seek(SeekFrom::End(0))?;
    Ok((records, truncate_to))
}

fn read_current(path: &Path) -> Result<String> {
    let contents = std::fs::read_to_string(path)?;
    let line = contents
        .lines()
        .next()
        .ok_or(GraniteError::Corrupt("empty CURRENT"))?;
    Ok(line.trim().to_string())
}

fn write_current(db_dir: &Path, manifest_name: &str, sync_mode: SyncMode) -> Result<()> {
    let tmp_path = db_dir.join("CURRENT.tmp");
    let final_path = db_dir.join("CURRENT");

    {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp_path)?;
        f.write_all(manifest_name.as_bytes())?;
        f.write_all(b"\n")?;
        if sync_mode == SyncMode::Yes {
            f.sync_data()?;
        }
    }

    std::fs::rename(&tmp_path, &final_path)?;
    if sync_mode == SyncMode::Yes {
        sync_dir(db_dir)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::io::Seek;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn manifest_roundtrip_and_truncate_tail() {
        let dir = tempdir().unwrap();
        let (mut m, v0) = Manifest::open(dir.path(), SyncMode::No).unwrap();
        assert!(v0.levels[0].is_empty());
        assert_eq!(v0.log_number, 1);
        assert_eq!(v0.last_sequence, 0);

        let meta = FileMeta {
            level: 0,
            file_id: 7,
            file_size: 123,
            smallest_key: b"a".to_vec(),
            largest_key: b"z".to_vec(),
        };
        m.append_flush_edit(&meta, 2, 9).unwrap();
        m.sync().unwrap();

        m.file.seek(SeekFrom::End(0)).unwrap();
        m.file.write_all(&[0xAA, 0xBB, 0xCC]).unwrap();
        drop(m);

        let (_m2, v2) = Manifest::open(dir.path(), SyncMode::No).unwrap();
        assert_eq!(v2.levels[0].len(), 1);
        assert_eq!(v2.levels[0][0].file_id, 7);
        assert_eq!(v2.log_number, 2);
        assert_eq!(v2.last_sequence, 9);
    }
}
