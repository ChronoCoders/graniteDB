use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{GraniteError, Result};
use crate::failpoint;
use crate::options::SyncMode;
use crate::util::sync_parent_dir;

pub const WAL_BLOCK_SIZE: usize = 32 * 1024;
const WAL_HEADER_SIZE: usize = 4 + 2 + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordType {
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

#[derive(Debug)]
pub struct Wal {
    file: File,
    offset_in_block: usize,
    sync_mode: SyncMode,
}

#[derive(Debug)]
pub struct WalRecovery {
    pub records: Vec<Vec<u8>>,
    pub truncated_to: u64,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>, sync_mode: SyncMode) -> Result<(Self, WalRecovery)> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        let recovery = recover_and_truncate(&mut file)?;
        if sync_mode == SyncMode::Yes && !existed {
            sync_parent_dir(&path)?;
        }

        let offset_in_block = (recovery.truncated_to as usize) % WAL_BLOCK_SIZE;
        Ok((
            Self {
                file,
                offset_in_block,
                sync_mode,
            },
            recovery,
        ))
    }

    pub fn append_logical_record(&mut self, logical_payload: &[u8]) -> Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let mut remaining = logical_payload;
        let mut is_first = true;

        while !remaining.is_empty() {
            let block_remaining = WAL_BLOCK_SIZE - self.offset_in_block;
            if block_remaining < WAL_HEADER_SIZE {
                if block_remaining > 0 {
                    let padding = vec![0u8; block_remaining];
                    failpoint::write_all("wal:write_padding", &mut self.file, &padding)?;
                    failpoint::hit("wal:after_padding");
                }
                self.offset_in_block = 0;
                continue;
            }

            let available = block_remaining - WAL_HEADER_SIZE;
            let chunk_len = available.min(remaining.len());
            let chunk = &remaining[..chunk_len];
            let is_last = chunk_len == remaining.len();

            let record_type = match (is_first, is_last) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, false) => RecordType::Middle,
                (false, true) => RecordType::Last,
            };

            let mut crc_input = Vec::with_capacity(1 + chunk.len());
            crc_input.push(record_type as u8);
            crc_input.extend_from_slice(chunk);
            let crc = crc32c::crc32c(&crc_input);

            if chunk_len > u16::MAX as usize {
                return Err(GraniteError::InvalidArgument("wal fragment too large"));
            }

            let mut header = [0u8; WAL_HEADER_SIZE];
            header[..4].copy_from_slice(&crc.to_le_bytes());
            header[4..6].copy_from_slice(&(chunk_len as u16).to_le_bytes());
            header[6] = record_type as u8;

            failpoint::write_all("wal:write_header", &mut self.file, &header)?;
            failpoint::hit("wal:after_header");
            failpoint::write_all("wal:write_payload", &mut self.file, chunk)?;
            failpoint::hit("wal:after_payload");

            self.offset_in_block += WAL_HEADER_SIZE + chunk_len;
            remaining = &remaining[chunk_len..];
            is_first = false;
        }

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.sync_mode == SyncMode::Yes {
            failpoint::hit("wal:before_sync");
            failpoint::sync_data("wal:sync", &self.file)?;
            failpoint::hit("wal:after_sync");
        }
        Ok(())
    }
}

fn recover_and_truncate(file: &mut File) -> Result<WalRecovery> {
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;

    let mut records = Vec::new();
    let mut global_off: u64 = 0;
    let mut last_good_off: u64 = 0;

    let mut pending: Vec<u8> = Vec::new();
    let mut pending_start_off: Option<u64> = None;

    let mut buf = vec![0u8; WAL_BLOCK_SIZE];
    while global_off < len {
        let to_read = (len - global_off).min(WAL_BLOCK_SIZE as u64) as usize;
        let block = &mut buf[..to_read];
        file.read_exact(block)?;

        let mut pos = 0usize;
        while pos + WAL_HEADER_SIZE <= to_read {
            let crc = u32::from_le_bytes(
                block[pos..pos + 4]
                    .try_into()
                    .map_err(|_| GraniteError::Corrupt("wal header crc bytes"))?,
            );
            let frag_len = u16::from_le_bytes(
                block[pos + 4..pos + 6]
                    .try_into()
                    .map_err(|_| GraniteError::Corrupt("wal header len bytes"))?,
            ) as usize;
            let typ = block[pos + 6];

            if crc == 0 && frag_len == 0 && typ == 0 {
                break;
            }

            let record_type =
                RecordType::from_u8(typ).ok_or(GraniteError::Corrupt("wal bad record type"))?;

            let end = pos + WAL_HEADER_SIZE + frag_len;
            if end > to_read {
                return finish_recovery(file, records, pending_start_off.unwrap_or(last_good_off));
            }

            let payload = &block[pos + WAL_HEADER_SIZE..end];
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
) -> Result<WalRecovery> {
    file.set_len(truncate_to)?;
    file.seek(SeekFrom::End(0))?;
    Ok(WalRecovery {
        records,
        truncated_to: truncate_to,
    })
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    use tempfile::tempdir;

    use super::*;

    fn wrap_batch_payload(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }

    #[test]
    fn wal_roundtrip_single_full_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.log");
        let (mut wal, recovery) = Wal::open(&path, SyncMode::No).unwrap();
        assert!(recovery.records.is_empty());

        let rec = wrap_batch_payload(b"abc");
        wal.append_logical_record(&rec).unwrap();
        wal.sync().unwrap();

        drop(wal);
        let (_wal2, recovery2) = Wal::open(&path, SyncMode::No).unwrap();
        assert_eq!(recovery2.records.len(), 1);
        assert_eq!(recovery2.records[0], rec);
    }

    #[test]
    fn wal_truncates_incomplete_trailing_logical_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.log");
        let (mut wal, _) = Wal::open(&path, SyncMode::No).unwrap();

        let big = vec![0xABu8; WAL_BLOCK_SIZE * 2];
        wal.append_logical_record(&big).unwrap();
        wal.sync().unwrap();
        drop(wal);

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let orig_len = f.metadata().unwrap().len();
        f.set_len(orig_len - 10).unwrap();
        f.seek(SeekFrom::End(0)).unwrap();
        f.write_all(&[0x11, 0x22, 0x33]).unwrap();
        drop(f);

        let (_wal2, recovery2) = Wal::open(&path, SyncMode::No).unwrap();
        assert!(recovery2.truncated_to < orig_len);
        assert!(recovery2.records.is_empty());
        let final_len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(final_len, recovery2.truncated_to);
    }
}
