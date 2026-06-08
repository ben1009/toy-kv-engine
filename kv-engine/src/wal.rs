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

/// Magic number for MVCC-format WAL files: "WAL2" in ASCII.
const WAL_MVCC_MAGIC: u32 = 0x5741_4C32;
/// MVCC WAL format version.
const WAL_FORMAT_VERSION: u16 = 2;
/// Size of the WAL file header: magic (4) + version (2) = 6 bytes.
const WAL_HEADER_SIZE: usize = 6;
/// Size of a batch header: commit_ts (8) + entry_count (4) + data_crc32 (4) = 16 bytes.
const BATCH_HEADER_SIZE: usize = 16;
/// Maximum WAL file size to prevent unbounded allocation during recovery (1 GB).
const MAX_WAL_FILE_SIZE: u64 = 1 << 30;

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

        let mut max_ts: u64 = 0;
        let mut data = &buf[..];

        // Detect MVCC format by checking the magic number AND version field.
        let mvcc_format = if data.len() >= WAL_HEADER_SIZE {
            let magic = (&data[..4]).get_u32();
            let version = (&data[4..6]).get_u16();
            if magic == WAL_MVCC_MAGIC {
                anyhow::ensure!(
                    version == WAL_FORMAT_VERSION,
                    "unsupported WAL version: got {}, expected {}",
                    version,
                    WAL_FORMAT_VERSION
                );
                true
            } else {
                false
            }
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
                let before_batch = data;

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
                    if entries_start - expected_size < 4 {
                        // Not enough data for key_len + value_len.
                        ok = false;
                        break;
                    }
                    let pos = expected_size;
                    let key_size = (&data[pos..pos + 2]).get_u16() as usize;
                    // Ensure key bytes + val_len u16 are within bounds before reading.
                    if entries_start - expected_size < 4 + key_size {
                        ok = false;
                        break;
                    }
                    let val_size =
                        (&data[pos + 2 + key_size..pos + 4 + key_size]).get_u16() as usize;
                    expected_size += 4 + key_size + val_size;
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

                // Replay entries into skiplist.
                let mut entry_buf = &data[..expected_size];
                for _ in 0..entry_count {
                    let key_size = entry_buf.get_u16() as usize;
                    let key = &entry_buf[..key_size];
                    entry_buf.advance(key_size);

                    let value_size = entry_buf.get_u16() as usize;
                    let value = &entry_buf[..value_size];
                    entry_buf.advance(value_size);

                    skiplist.insert(Bytes::from(key.to_owned()), Bytes::from(value.to_owned()));
                }

                data.advance(expected_size);
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
                let before_entry = data;
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
                let key = &data[..key_size];
                data.advance(key_size);

                if data.remaining() < 2 {
                    data = before_entry;
                    break;
                }
                let value_size = data.get_u16() as usize;
                if data.remaining() < value_size {
                    data = before_entry;
                    break;
                }
                let value = &data[..value_size];
                data.advance(value_size);

                // Extract timestamp from encoded MVCC key (if present).
                if let Some(ts) = crate::key::extract_ts(key)
                    && ts > max_ts
                {
                    max_ts = ts;
                }
                skiplist.insert(Bytes::from(key.to_owned()), Bytes::from(value.to_owned()));
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
                buf.put_u16(key.len() as u16);
                buf.put(*key);
                buf.put_u16(value.len() as u16);
                buf.put(*value);
            }
            return file.write_all(&buf).context("failed to write to WAL");
        }

        // Encode entries directly into the final buffer, leaving space for
        // the header.  This avoids a separate entries_buf allocation and copy.
        let entries_size: usize = data.iter().map(|(k, v)| 4 + k.len() + v.len()).sum();
        let mut buf = Vec::with_capacity(BATCH_HEADER_SIZE + entries_size);
        buf.resize(BATCH_HEADER_SIZE, 0); // reserve header space

        for (key, value) in data {
            buf.put_u16(key.len() as u16);
            buf.put(*key);
            buf.put_u16(value.len() as u16);
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

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;

        file.get_ref()
            .sync_all()
            .context("failed to sync WAL to disk")
    }
}
