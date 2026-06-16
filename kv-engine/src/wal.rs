use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::range_tombstone::RangeTombstone;

/// Result of recovering a WAL file, containing both point entries and range tombstones.
pub struct RecoveredWalBatch {
    /// Point key-value entries recovered from the WAL.
    pub points: Vec<(Bytes, Bytes)>,
    /// Point tombstones recovered from the WAL.
    pub point_tombstones: Vec<Bytes>,
    /// Range tombstones recovered from the WAL.
    pub range_tombstones: Vec<RangeTombstone>,
    /// Maximum commit_ts found across all batches.
    pub max_ts: u64,
}

/// Magic number for MVCC-format WAL files: "WAL2" in ASCII.
const WAL_MVCC_MAGIC: u32 = 0x5741_4C32;
/// MVCC WAL format version 2: untyped entries (key_len, key, val_len, val).
const WAL_FORMAT_VERSION_V2: u16 = 2;
/// MVCC WAL format version 3: typed entries with kind prefix.
const WAL_FORMAT_VERSION_V3: u16 = 3;
/// Current WAL format version written by new files.
const WAL_FORMAT_VERSION: u16 = WAL_FORMAT_VERSION_V3;
/// Size of the WAL file header: magic (4) + version (2) = 6 bytes.
const WAL_HEADER_SIZE: usize = 6;
/// Size of a batch header: commit_ts (8) + entry_count (4) + data_crc32 (4) = 16 bytes.
const BATCH_HEADER_SIZE: usize = 16;
/// Maximum WAL file size to prevent unbounded allocation during recovery (1 GB).
const MAX_WAL_FILE_SIZE: u64 = 1 << 30;

/// Entry kind tags for WAL v3 typed entries.
#[repr(u8)]
enum WalEntryKind {
    /// A point key-value put.
    Put = 0,
    /// A point tombstone (deletion marker).
    PointTombstone = 1,
    /// A range tombstone covering `[start, end)`.
    RangeTombstone = 2,
}

pub struct Wal {
    pub(crate) file: Arc<Mutex<BufWriter<File>>>,
    /// Whether this WAL uses the MVCC batch format (has file header).
    mvcc_format: bool,
}

// WAL files are garbage-collected by LsmStorageInner::force_flush_next_imm_memtable
// once the corresponding immutable memtable has been durably flushed to SST.
impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let f = File::create_new(path.as_ref()).context("failed to create WAL")?;
        let mut w = BufWriter::new(f);
        // Write MVCC WAL header (big-endian to match Buf::get_u32/get_u16).
        w.write_all(&WAL_MVCC_MAGIC.to_be_bytes())?;
        w.write_all(&WAL_FORMAT_VERSION.to_be_bytes())?;
        w.flush()?;

        Ok(Self {
            file: Arc::new(Mutex::new(w)),
            mvcc_format: true,
        })
    }

    /// Recover a WAL file, replaying entries into the skiplist.
    /// Returns the WAL handle and the maximum `commit_ts` found in any complete batch.
    pub fn recover(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<Bytes, Bytes>,
    ) -> Result<(Self, u64)> {
        let mut f = File::options()
            .read(true)
            .append(true)
            .open(path.as_ref())
            .context("failed to recover from WAL")?;
        let file_len = f.metadata()?.len();
        anyhow::ensure!(
            file_len <= MAX_WAL_FILE_SIZE,
            "WAL file too large: {} bytes (max {})",
            file_len,
            MAX_WAL_FILE_SIZE
        );
        let mut buf = Vec::with_capacity(file_len as usize);
        f.read_to_end(&mut buf)?;

        // Convert to Bytes once so that copy_to_bytes below yields zero-copy
        // slices sharing the same allocation, avoiding per-key/value heap allocs.
        let buf_bytes = Bytes::from(buf);
        let mut max_ts: u64 = 0;
        let mut data = buf_bytes.clone();

        // Detect MVCC format by checking the magic number AND version field.
        // Accept both v2 (untyped entries) and v3 (typed entries).
        let mvcc_format = if data.len() >= WAL_HEADER_SIZE {
            let magic = (&data[..4]).get_u32();
            let version = (&data[4..6]).get_u16();
            if magic == WAL_MVCC_MAGIC {
                anyhow::ensure!(
                    version == WAL_FORMAT_VERSION_V2 || version == WAL_FORMAT_VERSION_V3,
                    "unsupported WAL version: got {}, expected {} or {}",
                    version,
                    WAL_FORMAT_VERSION_V2,
                    WAL_FORMAT_VERSION_V3
                );
                true
            } else {
                false
            }
        } else {
            false
        };

        // Determine if this is v3 (typed entries) or v2 (untyped entries).
        let is_v3 = if data.len() >= WAL_HEADER_SIZE {
            (&data[4..6]).get_u16() == WAL_FORMAT_VERSION_V3
        } else {
            false
        };

        if mvcc_format {
            // Skip file header.
            data.advance(WAL_HEADER_SIZE);

            // Parse batch-framed records.
            let data_len = data.len();
            while data.remaining() >= BATCH_HEADER_SIZE {
                // Snapshot position before consuming the batch header so we can
                // back up if the batch turns out to be truncated / corrupt.
                // Bytes::clone() is a cheap refcount bump (no data copy).
                let before_batch = data.clone();

                let commit_ts = data.get_u64();
                let entry_count = data.get_u32() as usize;
                let data_crc32 = data.get_u32();

                // Compute the expected size of all entries.
                let entries_start = data.remaining();
                // Each entry is at least 4 bytes: key_len(u16) + value_len(u16).
                if entry_count > entries_start / 4 {
                    data = before_batch;
                    break;
                }
                let mut expected_size: usize = 0;
                let mut ok = true;
                // Peek ahead to check if we have enough data for all entries.
                for _ in 0..entry_count {
                    if is_v3 {
                        // v3: [kind:1][...]
                        if entries_start - expected_size < 1 {
                            ok = false;
                            break;
                        }
                        let kind = data[expected_size];
                        expected_size += 1;
                        match kind {
                            0 => {
                                // Put: [key_len:2][key][value_len:2][value]
                                if entries_start - expected_size < 4 {
                                    ok = false;
                                    break;
                                }
                                let pos = expected_size;
                                let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                                if entries_start - expected_size < 4 + key_size {
                                    ok = false;
                                    break;
                                }
                                let val_size = (&data[pos + 2 + key_size..pos + 4 + key_size])
                                    .get_u16()
                                    as usize;
                                expected_size += 4 + key_size + val_size;
                            }
                            1 => {
                                // PointTombstone: [key_len:2][key]
                                if entries_start - expected_size < 2 {
                                    ok = false;
                                    break;
                                }
                                let pos = expected_size;
                                let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                                expected_size += 2 + key_size;
                            }
                            2 => {
                                // RangeTombstone: [start_len:2][start][end_len:2][end]
                                if entries_start - expected_size < 4 {
                                    ok = false;
                                    break;
                                }
                                let pos = expected_size;
                                let start_size = (&data[pos..pos + 2]).get_u16() as usize;
                                if entries_start - expected_size < 4 + start_size {
                                    ok = false;
                                    break;
                                }
                                let end_size = (&data[pos + 2 + start_size..pos + 4 + start_size])
                                    .get_u16()
                                    as usize;
                                expected_size += 4 + start_size + end_size;
                            }
                            _ => {
                                ok = false;
                                break;
                            }
                        }
                    } else {
                        // v2: [key_len:2][key][value_len:2][value]
                        if entries_start - expected_size < 4 {
                            ok = false;
                            break;
                        }
                        let pos = expected_size;
                        let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                        if entries_start - expected_size < 4 + key_size {
                            ok = false;
                            break;
                        }
                        let val_size =
                            (&data[pos + 2 + key_size..pos + 4 + key_size]).get_u16() as usize;
                        expected_size += 4 + key_size + val_size;
                    }
                    if expected_size > entries_start {
                        ok = false;
                        break;
                    }
                }

                if !ok || expected_size > entries_start {
                    // Truncated batch — restore cursor to before the header so
                    // the truncation below cuts at the right offset.
                    data = before_batch;
                    break;
                }

                // Validate CRC32 over the entry data.
                let entry_data = &data[..expected_size];
                let computed_crc = crc32fast::hash(entry_data);
                if computed_crc != data_crc32 {
                    // CRC mismatch — restore cursor to before the header.
                    data = before_batch;
                    break;
                }

                // Replay entries into skiplist using zero-copy Bytes slices.
                let mut entry_buf = data.split_to(expected_size);
                for _ in 0..entry_count {
                    if is_v3 {
                        // v3: typed entries with kind prefix.
                        let kind = entry_buf.get_u8();
                        match kind {
                            0 => {
                                // Put: [kind:1][key_len:2][key][value_len:2][value]
                                let key_size = entry_buf.get_u16() as usize;
                                let key = entry_buf.split_to(key_size);
                                let value_size = entry_buf.get_u16() as usize;
                                let value = entry_buf.split_to(value_size);
                                skiplist.insert(key, value);
                            }
                            1 => {
                                // PointTombstone: [kind:1][key_len:2][key]
                                let key_size = entry_buf.get_u16() as usize;
                                let key = entry_buf.split_to(key_size);
                                // Store tombstone as key -> KvKind::Tombstone byte.
                                skiplist.insert(
                                    key,
                                    Bytes::from_static(&[crate::vlog::KvKind::Tombstone as u8]),
                                );
                            }
                            2 => {
                                // RangeTombstone: [kind:1][start_len:2][start][end_len:2][end]
                                let start_size = entry_buf.get_u16() as usize;
                                let start = entry_buf.split_to(start_size);
                                let end_size = entry_buf.get_u16() as usize;
                                let _end = entry_buf.split_to(end_size);
                                // Range tombstones are not replayed into the skiplist;
                                // they are recovered via recover_with_range_tombstones().
                                // Track max_ts from the batch header (already done above).
                                let _ = start;
                            }
                            _ => {
                                anyhow::bail!("unknown WAL v3 entry kind: {}", kind);
                            }
                        }
                    } else {
                        // v2: untyped entries [key_len:2][key][value_len:2][value].
                        let key_size = entry_buf.get_u16() as usize;
                        let key = entry_buf.split_to(key_size);
                        let value_size = entry_buf.get_u16() as usize;
                        let value = entry_buf.split_to(value_size);
                        skiplist.insert(key, value);
                    }
                }
                if commit_ts > max_ts {
                    max_ts = commit_ts;
                }
            }
            // Truncate file to the last valid byte — drop any trailing
            // partial/corrupt batch so subsequent appends don't leave garbage.
            // The valid data is already in the file (recovery doesn't rewrite
            // it); we only need to shorten the file to remove trailing junk.
            let valid_data = data_len - data.remaining();
            let valid_file = WAL_HEADER_SIZE + valid_data;
            if (valid_file as u64) < file_len {
                f.set_len(valid_file as u64)?;
            }
        } else {
            // Legacy format: flat [key_len: u16][key][value_len: u16][value] entries.
            let data_len = data.len();
            while data.has_remaining() {
                // Snapshot before reading entry fields so truncation cuts
                // at the right offset on parse failure.
                let before_entry = data.clone();
                // Guard against truncated entries.
                if data.remaining() < 4 {
                    data = before_entry;
                    break;
                }
                let key_size = data.get_u16() as usize;
                if data.remaining() < key_size + 2 {
                    data = before_entry;
                    break;
                }
                let key = data.split_to(key_size);

                if data.remaining() < 2 {
                    data = before_entry;
                    break;
                }
                let value_size = data.get_u16() as usize;
                if data.remaining() < value_size {
                    data = before_entry;
                    break;
                }
                let value = data.split_to(value_size);

                // Extract timestamp from encoded MVCC key (if present).
                if let Some(ts) = crate::key::extract_ts(&key)
                    && ts > max_ts
                {
                    max_ts = ts;
                }
                skiplist.insert(key, value);
            }
            // Truncate file to the last valid byte.
            let valid_len = data_len - data.remaining();
            if (valid_len as u64) < file_len {
                f.set_len(valid_len as u64)?;
            }
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(f))),
                mvcc_format,
            },
            max_ts,
        ))
    }

    /// Recover a WAL file with full range-tombstone support.
    ///
    /// Returns a [`RecoveredWalBatch`] containing point entries, point tombstones,
    /// range tombstones, and the maximum commit_ts. Point entries and tombstones
    /// are also replayed into the provided skiplist for backward compatibility.
    pub fn recover_with_range_tombstones(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<Bytes, Bytes>,
        range_tombstones: &crate::range_tombstone::RangeTombstoneSet,
    ) -> Result<(Self, RecoveredWalBatch)> {
        let mut f = File::options()
            .read(true)
            .append(true)
            .open(path.as_ref())
            .context("failed to recover from WAL")?;
        let file_len = f.metadata()?.len();
        anyhow::ensure!(
            file_len <= MAX_WAL_FILE_SIZE,
            "WAL file too large: {} bytes (max {})",
            file_len,
            MAX_WAL_FILE_SIZE
        );
        let mut buf = Vec::with_capacity(file_len as usize);
        f.read_to_end(&mut buf)?;

        let buf_bytes = Bytes::from(buf);
        let mut max_ts: u64 = 0;
        let mut data = buf_bytes.clone();
        let mut points = Vec::new();
        let mut point_tombstones = Vec::new();
        let mut range_ts = Vec::new();

        // Detect MVCC format.
        let mvcc_format = if data.len() >= WAL_HEADER_SIZE {
            let magic = (&data[..4]).get_u32();
            let version = (&data[4..6]).get_u16();
            if magic == WAL_MVCC_MAGIC {
                anyhow::ensure!(
                    version == WAL_FORMAT_VERSION_V2 || version == WAL_FORMAT_VERSION_V3,
                    "unsupported WAL version: got {}, expected {} or {}",
                    version,
                    WAL_FORMAT_VERSION_V2,
                    WAL_FORMAT_VERSION_V3
                );
                true
            } else {
                false
            }
        } else {
            false
        };

        let is_v3 = if data.len() >= WAL_HEADER_SIZE {
            (&data[4..6]).get_u16() == WAL_FORMAT_VERSION_V3
        } else {
            false
        };

        if mvcc_format {
            data.advance(WAL_HEADER_SIZE);
            let data_len = data.len();

            while data.remaining() >= BATCH_HEADER_SIZE {
                let before_batch = data.clone();
                let commit_ts = data.get_u64();
                let entry_count = data.get_u32() as usize;
                let data_crc32 = data.get_u32();

                let entries_start = data.remaining();
                if entry_count > entries_start / 4 {
                    data = before_batch;
                    break;
                }

                // For v3, we need to scan entries to compute expected size
                // because entry sizes vary by kind.
                let mut expected_size: usize = 0;
                let mut ok = true;
                for _ in 0..entry_count {
                    if is_v3 {
                        // v3: [kind:1][...]
                        if entries_start - expected_size < 1 {
                            ok = false;
                            break;
                        }
                        let kind = data[expected_size];
                        expected_size += 1;
                        match kind {
                            0 => {
                                // Put: [key_len:2][key][value_len:2][value]
                                if entries_start - expected_size < 4 {
                                    ok = false;
                                    break;
                                }
                                let pos = expected_size;
                                let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                                if entries_start - expected_size < 4 + key_size {
                                    ok = false;
                                    break;
                                }
                                let val_size = (&data[pos + 2 + key_size..pos + 4 + key_size])
                                    .get_u16()
                                    as usize;
                                expected_size += 4 + key_size + val_size;
                            }
                            1 => {
                                // PointTombstone: [key_len:2][key]
                                if entries_start - expected_size < 2 {
                                    ok = false;
                                    break;
                                }
                                let pos = expected_size;
                                let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                                expected_size += 2 + key_size;
                            }
                            2 => {
                                // RangeTombstone: [start_len:2][start][end_len:2][end]
                                if entries_start - expected_size < 4 {
                                    ok = false;
                                    break;
                                }
                                let pos = expected_size;
                                let start_size = (&data[pos..pos + 2]).get_u16() as usize;
                                if entries_start - expected_size < 4 + start_size {
                                    ok = false;
                                    break;
                                }
                                let end_size = (&data[pos + 2 + start_size..pos + 4 + start_size])
                                    .get_u16()
                                    as usize;
                                expected_size += 4 + start_size + end_size;
                            }
                            _ => {
                                ok = false;
                                break;
                            }
                        }
                    } else {
                        // v2: [key_len:2][key][value_len:2][value]
                        if entries_start - expected_size < 4 {
                            ok = false;
                            break;
                        }
                        let pos = expected_size;
                        let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                        if entries_start - expected_size < 4 + key_size {
                            ok = false;
                            break;
                        }
                        let val_size =
                            (&data[pos + 2 + key_size..pos + 4 + key_size]).get_u16() as usize;
                        expected_size += 4 + key_size + val_size;
                    }
                    if expected_size > entries_start {
                        ok = false;
                        break;
                    }
                }

                if !ok || expected_size > entries_start {
                    data = before_batch;
                    break;
                }

                let entry_data = &data[..expected_size];
                let computed_crc = crc32fast::hash(entry_data);
                if computed_crc != data_crc32 {
                    data = before_batch;
                    break;
                }

                let mut entry_buf = data.split_to(expected_size);
                for i in 0..entry_count {
                    if is_v3 {
                        let kind = entry_buf.get_u8();
                        match kind {
                            0 => {
                                // Put
                                let key_size = entry_buf.get_u16() as usize;
                                let key = entry_buf.split_to(key_size);
                                let value_size = entry_buf.get_u16() as usize;
                                let value = entry_buf.split_to(value_size);
                                points.push((key.clone(), value.clone()));
                                skiplist.insert(key, value);
                            }
                            1 => {
                                // PointTombstone
                                let key_size = entry_buf.get_u16() as usize;
                                let key = entry_buf.split_to(key_size);
                                point_tombstones.push(key.clone());
                                skiplist.insert(
                                    key,
                                    Bytes::from_static(&[crate::vlog::KvKind::Tombstone as u8]),
                                );
                            }
                            2 => {
                                // RangeTombstone
                                let start_size = entry_buf.get_u16() as usize;
                                let start = entry_buf.split_to(start_size);
                                let end_size = entry_buf.get_u16() as usize;
                                let end = entry_buf.split_to(end_size);
                                range_ts.push((
                                    start,
                                    end,
                                    commit_ts,
                                    u32::try_from(i).context("ordinal overflow")?,
                                ));
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        // v2: untyped entries
                        let key_size = entry_buf.get_u16() as usize;
                        let key = entry_buf.split_to(key_size);
                        let value_size = entry_buf.get_u16() as usize;
                        let value = entry_buf.split_to(value_size);
                        points.push((key.clone(), value.clone()));
                        skiplist.insert(key, value);
                    }
                }

                if commit_ts > max_ts {
                    max_ts = commit_ts;
                }
            }

            let valid_data = data_len - data.remaining();
            let valid_file = WAL_HEADER_SIZE + valid_data;
            if (valid_file as u64) < file_len {
                f.set_len(valid_file as u64)?;
            }
        } else {
            // Legacy format: flat entries.
            let data_len = data.len();
            while data.has_remaining() {
                let before_entry = data.clone();
                if data.remaining() < 4 {
                    data = before_entry;
                    break;
                }
                let key_size = data.get_u16() as usize;
                if data.remaining() < key_size + 2 {
                    data = before_entry;
                    break;
                }
                let key = data.split_to(key_size);
                if data.remaining() < 2 {
                    data = before_entry;
                    break;
                }
                let value_size = data.get_u16() as usize;
                if data.remaining() < value_size {
                    data = before_entry;
                    break;
                }
                let value = data.split_to(value_size);
                if let Some(ts) = crate::key::extract_ts(&key)
                    && ts > max_ts
                {
                    max_ts = ts;
                }
                points.push((key.clone(), value.clone()));
                skiplist.insert(key, value);
            }
            let valid_len = data_len - data.remaining();
            if (valid_len as u64) < file_len {
                f.set_len(valid_len as u64)?;
            }
        }

        // Insert recovered range tombstones into the set.
        for (start, end, ts, ordinal) in &range_ts {
            range_tombstones.add(
                RangeTombstone {
                    start: start.clone(),
                    end: end.clone(),
                    ts: *ts,
                },
                *ordinal,
            );
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(f))),
                mvcc_format,
            },
            RecoveredWalBatch {
                points,
                point_tombstones,
                range_tombstones: range_ts
                    .into_iter()
                    .map(|(start, end, ts, _)| RangeTombstone { start, end, ts })
                    .collect(),
                max_ts,
            },
        ))
    }

    /// Write a single key-value entry.
    /// In MVCC format, wraps the entry in a batch record (commit_ts=0 for
    /// non-timestamped writes). In legacy format, writes the flat format.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.mvcc_format {
            // Wrap in a single-entry batch with commit_ts=0.
            return self.put_batch(&[(key, value)], 0);
        }
        anyhow::ensure!(
            key.len() <= u16::MAX as usize,
            "WAL key too large: {} bytes (max {})",
            key.len(),
            u16::MAX
        );
        anyhow::ensure!(
            value.len() <= u16::MAX as usize,
            "WAL value too large: {} bytes (max {})",
            value.len(),
            u16::MAX
        );
        let mut file = self.file.lock();
        let mut buf = vec![];

        buf.put_u16(key.len() as u16);
        buf.put(key);

        buf.put_u16(value.len() as u16);
        buf.put(value);

        file.write_all(&buf).context("failed to write to WAL")
    }

    /// Write a batch of key-value entries as a single atomic WAL record.
    /// The batch includes a `commit_ts`, entry count, CRC32 checksum, and all entries.
    /// On crash during write, recovery will skip the incomplete batch.
    ///
    /// For legacy (non-MVCC) WALs recovered from pre-v2 files, entries are
    /// written in the flat legacy format so the file remains self-consistent.
    pub fn put_batch(&self, data: &[(&[u8], &[u8])], commit_ts: u64) -> Result<()> {
        for (key, value) in data {
            anyhow::ensure!(
                key.len() <= u16::MAX as usize,
                "WAL batch key too large: {} bytes (max {})",
                key.len(),
                u16::MAX
            );
            anyhow::ensure!(
                value.len() <= u16::MAX as usize,
                "WAL batch value too large: {} bytes (max {})",
                value.len(),
                u16::MAX
            );
        }
        let entry_count =
            u32::try_from(data.len()).context("batch entry count exceeds u32::MAX")?;
        let mut file = self.file.lock();

        if !self.mvcc_format {
            // Legacy format: write flat [key_len][key][val_len][val] entries
            // so the file remains consistent with legacy recovery.
            let mut buf = Vec::with_capacity(data.iter().map(|(k, v)| 4 + k.len() + v.len()).sum());
            for (key, value) in data {
                buf.put_u16(u16::try_from(key.len()).context("key length exceeds u16::MAX")?);
                buf.put(*key);
                buf.put_u16(u16::try_from(value.len()).context("value length exceeds u16::MAX")?);
                buf.put(*value);
            }
            return file.write_all(&buf).context("failed to write to WAL");
        }

        // v3: typed entries with Put kind prefix.
        // Each entry: [kind:1=0][key_len:2][key][value_len:2][value]
        let entries_size: usize = data.iter().map(|(k, v)| 5 + k.len() + v.len()).sum();
        let mut buf = Vec::with_capacity(BATCH_HEADER_SIZE + entries_size);
        buf.resize(BATCH_HEADER_SIZE, 0); // reserve header space

        for (key, value) in data {
            buf.put_u8(WalEntryKind::Put as u8);
            buf.put_u16(u16::try_from(key.len()).context("key length exceeds u16::MAX")?);
            buf.put(*key);
            buf.put_u16(u16::try_from(value.len()).context("value length exceeds u16::MAX")?);
            buf.put(*value);
        }

        let crc = crc32fast::hash(&buf[BATCH_HEADER_SIZE..]);

        // Overwrite the header at the beginning of the buffer.
        let mut header = &mut buf[0..BATCH_HEADER_SIZE];
        header.put_u64(commit_ts);
        header.put_u32(entry_count);
        header.put_u32(crc);

        file.write_all(&buf).context("failed to write WAL batch")
    }

    /// Write a batch of range tombstones as a single atomic WAL record.
    ///
    /// Each entry is encoded as: `[kind:1=2][start_len:2][start][end_len:2][end]`.
    /// The `commit_ts` is shared across all entries in the batch.
    pub fn put_range_tombstone_batch(
        &self,
        tombstones: &[(&[u8], &[u8])],
        commit_ts: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            self.mvcc_format,
            "range tombstone batches require MVCC WAL format"
        );
        for (start, end) in tombstones {
            anyhow::ensure!(
                start.len() <= u16::MAX as usize,
                "WAL range tombstone start too large: {} bytes (max {})",
                start.len(),
                u16::MAX
            );
            anyhow::ensure!(
                end.len() <= u16::MAX as usize,
                "WAL range tombstone end too large: {} bytes (max {})",
                end.len(),
                u16::MAX
            );
        }
        let entry_count =
            u32::try_from(tombstones.len()).context("batch entry count exceeds u32::MAX")?;
        let mut file = self.file.lock();

        // v3: typed entries with RangeTombstone kind prefix.
        // Each entry: [kind:1=2][start_len:2][start][end_len:2][end]
        let entries_size: usize = tombstones.iter().map(|(s, e)| 5 + s.len() + e.len()).sum();
        let mut buf = Vec::with_capacity(BATCH_HEADER_SIZE + entries_size);
        buf.resize(BATCH_HEADER_SIZE, 0); // reserve header space

        for (start, end) in tombstones {
            buf.put_u8(WalEntryKind::RangeTombstone as u8);
            buf.put_u16(u16::try_from(start.len()).context("start length exceeds u16::MAX")?);
            buf.put(*start);
            buf.put_u16(u16::try_from(end.len()).context("end length exceeds u16::MAX")?);
            buf.put(*end);
        }

        let crc = crc32fast::hash(&buf[BATCH_HEADER_SIZE..]);

        let mut header = &mut buf[0..BATCH_HEADER_SIZE];
        header.put_u64(commit_ts);
        header.put_u32(entry_count);
        header.put_u32(crc);

        file.write_all(&buf)
            .context("failed to write WAL range tombstone batch")
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;

        file.get_ref()
            .sync_all()
            .context("failed to sync WAL to disk")
    }
}
