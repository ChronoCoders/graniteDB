#![deny(warnings)]

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32c::crc32c;

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 4 + 2 + 1;
const INTERNAL_KEY_SUFFIX_LEN: usize = 8 + 1;

const TAG_ADD_FILE: u8 = 1;
const TAG_SET_LOG_NUMBER: u8 = 3;
const TAG_SET_LAST_SEQUENCE: u8 = 4;
const TAG_SET_PREV_LOG_NUMBER: u8 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let db_path: PathBuf = args.next().expect("db path").into();
    repair_dir(&db_path).expect("repair");
}

fn repair_dir(db_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(db_dir)?;

    let mut ssts: Vec<(u64, PathBuf)> = Vec::new();
    let mut logs: Vec<u64> = Vec::new();
    let mut max_manifest_number: u64 = 0;

    for e in fs::read_dir(db_dir)? {
        let path = e?.path();
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(rest) = file_name.strip_prefix("MANIFEST-")
            && let Ok(n) = rest.parse::<u64>()
        {
            max_manifest_number = max_manifest_number.max(n);
        }
        let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
            continue;
        };
        match ext {
            "sst" => {
                let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if let Ok(id) = stem.parse::<u64>() {
                    ssts.push((id, path));
                }
            }
            "log" => {
                let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if let Ok(id) = stem.parse::<u64>() {
                    logs.push(id);
                }
            }
            _ => {}
        }
    }

    ssts.sort_by_key(|(id, _)| *id);
    logs.sort();

    let log_number = logs.last().copied().unwrap_or(1);
    let prev_log_number = if logs.len() >= 2 {
        logs[logs.len() - 2]
    } else {
        0
    };

    let mut last_sequence: u64 = 0;
    let mut payload = Vec::new();
    for (file_id, path) in ssts {
        let meta = inspect_sst(file_id, &path)?;
        last_sequence = last_sequence.max(meta.max_seq);
        encode_add_file(&mut payload, &meta);
    }
    encode_set_log_number(&mut payload, log_number);
    encode_set_prev_log_number(&mut payload, prev_log_number);
    encode_set_last_sequence(&mut payload, last_sequence);

    let new_manifest_number = max_manifest_number.saturating_add(1).max(1);
    let new_manifest_name = format!("MANIFEST-{new_manifest_number:06}");
    let new_manifest_path = db_dir.join(&new_manifest_name);

    let mut manifest = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&new_manifest_path)?;

    let mut offset_in_block = 0usize;
    append_logical_record(&mut manifest, &mut offset_in_block, &payload)?;
    manifest.sync_data()?;

    write_current(db_dir, &new_manifest_name)?;
    Ok(())
}

struct SstMeta {
    level: u32,
    file_id: u64,
    file_size: u64,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    max_seq: u64,
}

fn inspect_sst(file_id: u64, path: &Path) -> std::io::Result<SstMeta> {
    let mut max_seq = 0u64;
    let mut smallest_key: Option<Vec<u8>> = None;
    let mut largest_key: Option<Vec<u8>> = None;

    if let Ok(reader) = granitedb::TableReader::open_with_cache(path, None) {
        let mut scanner = reader.scanner();
        while let Ok(Some((k, _v))) = scanner.next_item() {
            if smallest_key.is_none() {
                smallest_key = Some(k.clone());
            }
            largest_key = Some(k.clone());
            if let Some(seq) = parse_seq(&k) {
                max_seq = max_seq.max(seq);
            }
        }
    }

    let file_size = fs::metadata(path)?.len();
    let smallest_key =
        smallest_key.ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, "empty sst"))?;
    let largest_key =
        largest_key.ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, "empty sst"))?;
    Ok(SstMeta {
        level: 0,
        file_id,
        file_size,
        smallest_key,
        largest_key,
        max_seq,
    })
}

fn parse_seq(internal_key: &[u8]) -> Option<u64> {
    if internal_key.len() < INTERNAL_KEY_SUFFIX_LEN {
        return None;
    }
    let suffix = &internal_key[internal_key.len() - INTERNAL_KEY_SUFFIX_LEN..];
    let inv = u64::from_be_bytes(suffix[..8].try_into().ok()?);
    Some(u64::MAX - inv)
}

fn encode_add_file(out: &mut Vec<u8>, meta: &SstMeta) {
    out.push(TAG_ADD_FILE);
    out.extend_from_slice(&meta.level.to_le_bytes());
    out.extend_from_slice(&meta.file_id.to_le_bytes());
    out.extend_from_slice(&meta.file_size.to_le_bytes());
    out.extend_from_slice(&(meta.smallest_key.len() as u32).to_le_bytes());
    out.extend_from_slice(&meta.smallest_key);
    out.extend_from_slice(&(meta.largest_key.len() as u32).to_le_bytes());
    out.extend_from_slice(&meta.largest_key);
}

fn encode_set_log_number(out: &mut Vec<u8>, log_number: u64) {
    out.push(TAG_SET_LOG_NUMBER);
    out.extend_from_slice(&log_number.to_le_bytes());
}

fn encode_set_prev_log_number(out: &mut Vec<u8>, prev_log_number: u64) {
    out.push(TAG_SET_PREV_LOG_NUMBER);
    out.extend_from_slice(&prev_log_number.to_le_bytes());
}

fn encode_set_last_sequence(out: &mut Vec<u8>, last_sequence: u64) {
    out.push(TAG_SET_LAST_SEQUENCE);
    out.extend_from_slice(&last_sequence.to_le_bytes());
}

fn append_logical_record(
    file: &mut fs::File,
    offset_in_block: &mut usize,
    logical_payload: &[u8],
) -> std::io::Result<()> {
    file.seek(SeekFrom::End(0))?;
    let mut remaining = logical_payload;
    let mut is_first = true;

    while !remaining.is_empty() {
        let block_remaining = BLOCK_SIZE - *offset_in_block;
        if block_remaining < HEADER_SIZE {
            if block_remaining > 0 {
                let padding = vec![0u8; block_remaining];
                file.write_all(&padding)?;
            }
            *offset_in_block = 0;
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

        let mut crc_input = Vec::with_capacity(1 + chunk.len());
        crc_input.push(record_type as u8);
        crc_input.extend_from_slice(chunk);
        let crc = crc32c(&crc_input);

        let mut header = [0u8; HEADER_SIZE];
        header[..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&(chunk_len as u16).to_le_bytes());
        header[6] = record_type as u8;

        file.write_all(&header)?;
        file.write_all(chunk)?;

        *offset_in_block += HEADER_SIZE + chunk_len;
        remaining = &remaining[chunk_len..];
        is_first = false;
    }

    Ok(())
}

fn write_current(db_dir: &Path, manifest_name: &str) -> std::io::Result<()> {
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
        f.sync_data()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}
