use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use parking_lot::{Condvar, Mutex};

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
    /// Read during WAL recovery but never written — point tombstones are stored
    /// as `Put` entries with a tombstone value byte.
    #[allow(dead_code)]
    PointTombstone = 1,
    /// A range tombstone covering `[start, end)`.
    RangeTombstone = 2,
}

/// Pre-allocated buffer size for the lock-free pool (64 KB).
const BUFFER_POOL_BUF_SIZE: usize = 64 * 1024;
/// Number of buffers pre-allocated in the pool.
const BUFFER_POOL_CAPACITY: usize = 16;
/// Number of slots in the batch-result ring buffer. Must be larger than the
/// maximum number of in-flight batches so that a slot is never overwritten
/// before all followers from that batch have checked their result.
const BATCH_RESULT_RING_SIZE: usize = 256;

/// Outcome of a single group-commit batch.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BatchOutcome {
    Ok,
    Err,
}

/// Record of a completed batch, stored in the ring buffer so followers can
/// look up the result for their specific ticket.
#[derive(Clone, Copy)]
struct BatchResult {
    /// First ticket in this batch (inclusive).
    batch_start: u64,
    /// First ticket of the NEXT batch (exclusive). Equals `commit_waiters`
    /// at the time the leader was elected.
    batch_end: u64,
    outcome: BatchOutcome,
}

pub struct Wal {
    pub(crate) file: Arc<Mutex<BufWriter<File>>>,
    /// Whether this WAL uses the MVCC batch format (has file header).
    mvcc_format: bool,
    /// Whether this WAL uses v3 typed entries (kind prefix).
    /// Only meaningful when `mvcc_format` is true. When false, the WAL uses v2
    /// untyped entries. Preserved from recovery so appended records match the
    /// file header.
    is_v3: bool,
    /// Lock-free pool of pre-allocated buffers for MVCC `put_batch`.
    /// Threads pop a buffer, encode into it, then push to `ready_queue`.
    buf_pool: ArrayQueue<Vec<u8>>,
    /// Lock-free queue of filled buffers waiting for the leader to drain + fsync.
    /// M3: This queue is unbounded. Under sustained I/O stalls, buffers can
    /// accumulate. This is a known limitation — in practice the leader drains
    /// quickly, and the pool size (16 buffers × 64 KB) bounds steady-state
    /// memory usage. Monitor ready_queue depth if write latency spikes.
    ready_queue: crossbeam_queue::SegQueue<Vec<u8>>,
    /// Next ticket to hand out for group-commit participation.
    commit_waiters: AtomicU64,
    /// Highest ticket that has been durably committed (batch_end of the last
    /// completed batch, regardless of success/failure).
    committed_gen: AtomicU64,
    /// Whether a leader is currently draining and syncing a batch.
    leader_active: AtomicBool,
    /// Ring buffer of recent batch results, protected by the same mutex as
    /// `commit_cond`. Followers look up their ticket to find the result of
    /// the batch that contained it. This avoids the race condition where a
    /// single `last_failed_gen`/`last_succeeded_gen` pair can mask failures
    /// when later batches succeed.
    batch_results: Mutex<[BatchResult; BATCH_RESULT_RING_SIZE]>,
    /// Condvar the leader uses to wake followers after syncing.
    commit_cond: Condvar,
    /// Mutex paired with `commit_cond`.
    commit_mutex: Mutex<()>,
}

/// Abstraction over WAL entry recovery actions.
///
/// Implementors decide what to do with each recovered entry (e.g., replay into
/// a skiplist, collect into vectors, or both). This eliminates the massive code
/// duplication between [`Wal::recover`] and [`Wal::recover_with_range_tombstones`].
trait RecoveryHandler {
    /// Handle a recovered point key-value put.
    fn handle_put(&mut self, key: Bytes, value: Bytes) -> Result<()>;
    /// Handle a recovered point tombstone.
    fn handle_point_tombstone(&mut self, key: Bytes) -> Result<()>;
    /// Handle a recovered range tombstone (default: no-op).
    fn handle_range_tombstone(&mut self, _start: Bytes, _end: Bytes, _ts: u64) -> Result<()> {
        Ok(())
    }

    /// Called at the start of each WAL batch so range-tombstone ordinals
    /// restart from 0 (matching live-write behaviour).
    fn reset_range_ordinals(&mut self) {}
}

/// Recovery handler that replays entries into a skiplist.
struct SkiplistRecovery<'a> {
    skiplist: &'a SkipMap<Bytes, Bytes>,
}

impl RecoveryHandler for SkiplistRecovery<'_> {
    fn handle_put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.skiplist.insert(key, value);

        Ok(())
    }

    fn handle_point_tombstone(&mut self, key: Bytes) -> Result<()> {
        self.skiplist.insert(
            key,
            Bytes::from_static(&[crate::vlog::KvKind::Tombstone as u8]),
        );

        Ok(())
    }
}

/// Recovery handler that replays into a skiplist AND collects entries for range
/// tombstone support.
struct SkiplistRangeRecovery<'a> {
    skiplist: &'a SkipMap<Bytes, Bytes>,
    points: Vec<(Bytes, Bytes)>,
    point_tombstones: Vec<Bytes>,
    range_ts: Vec<(Bytes, Bytes, u64, u32)>,
    range_tombstone_idx: u32,
}

impl RecoveryHandler for SkiplistRangeRecovery<'_> {
    fn handle_put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if crate::vlog::KvKind::is_tombstone_value(&value) {
            self.point_tombstones.push(key.clone());
            self.skiplist.insert(key, value);
            return Ok(());
        }

        self.points.push((key.clone(), value.clone()));
        self.skiplist.insert(key, value);

        Ok(())
    }

    fn handle_point_tombstone(&mut self, key: Bytes) -> Result<()> {
        self.point_tombstones.push(key.clone());
        self.skiplist.insert(
            key,
            Bytes::from_static(&[crate::vlog::KvKind::Tombstone as u8]),
        );

        Ok(())
    }

    fn handle_range_tombstone(&mut self, start: Bytes, end: Bytes, ts: u64) -> Result<()> {
        self.range_ts
            .push((start, end, ts, self.range_tombstone_idx));
        self.range_tombstone_idx = self
            .range_tombstone_idx
            .checked_add(1)
            .context("ordinal overflow")?;

        Ok(())
    }

    fn reset_range_ordinals(&mut self) {
        self.range_tombstone_idx = 0;
    }
}

// WAL files are garbage-collected by LsmStorageInner::force_flush_next_imm_memtable
// once the corresponding immutable memtable has been durably flushed to SST.
impl Wal {
    /// Create a pre-filled lock-free buffer pool.
    fn new_buf_pool() -> ArrayQueue<Vec<u8>> {
        let pool = ArrayQueue::new(BUFFER_POOL_CAPACITY);
        for _ in 0..BUFFER_POOL_CAPACITY {
            // Ignore error if capacity is somehow exceeded (won't happen).
            let _ = pool.push(Vec::with_capacity(BUFFER_POOL_BUF_SIZE));
        }
        pool
    }

    const fn empty_batch_results() -> [BatchResult; BATCH_RESULT_RING_SIZE] {
        [BatchResult {
            batch_start: 0,
            batch_end: 0,
            outcome: BatchOutcome::Ok,
        }; BATCH_RESULT_RING_SIZE]
    }

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
            is_v3: true,
            buf_pool: Self::new_buf_pool(),
            ready_queue: crossbeam_queue::SegQueue::new(),
            commit_waiters: AtomicU64::new(0),
            committed_gen: AtomicU64::new(0),
            leader_active: AtomicBool::new(false),
            batch_results: Mutex::new(Self::empty_batch_results()),
            commit_cond: Condvar::new(),
            commit_mutex: Mutex::new(()),
        })
    }

    /// Parse an MVCC-format WAL file, delegating each recovered entry to the
    /// given handler. Returns the file (positioned for append) and `mvcc_format`.
    fn recover_mvcc<H: RecoveryHandler>(
        f: File,
        mut data: Bytes,
        is_v3: bool,
        file_len: u64,
        handler: &mut H,
    ) -> Result<(File, u64)> {
        let data_len = data.len();
        let mut max_ts: u64 = 0;

        while data.remaining() >= BATCH_HEADER_SIZE {
            let before_batch = data.clone();
            let commit_ts = data.get_u64();
            let entry_count = data.get_u32() as usize;
            let data_crc32 = data.get_u32();
            handler.reset_range_ordinals();

            let entries_start = data.remaining();
            let min_entry_size = if is_v3 { 3 } else { 4 };
            if entry_count > entries_start / min_entry_size {
                data = before_batch;
                break;
            }

            // Validate all entries fit and compute expected_size.
            let mut expected_size: usize = 0;
            let mut ok = true;
            for _ in 0..entry_count {
                if is_v3 {
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
                            let val_size =
                                (&data[pos + 2 + key_size..pos + 4 + key_size]).get_u16() as usize;
                            if entries_start - expected_size < 4 + key_size + val_size {
                                ok = false;
                                break;
                            }
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
                            if entries_start - expected_size < 2 + key_size {
                                ok = false;
                                break;
                            }
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
                                .get_u16() as usize;
                            if entries_start - expected_size < 4 + start_size + end_size {
                                ok = false;
                                break;
                            }
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
                    if entries_start - expected_size < 4 + key_size + val_size {
                        ok = false;
                        break;
                    }
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

            // Validate CRC32 over the entry data.
            let entry_data = &data[..expected_size];
            let computed_crc = crc32fast::hash(entry_data);
            if computed_crc != data_crc32 {
                data = before_batch;
                break;
            }

            // Replay entries into handler using zero-copy Bytes slices.
            let mut entry_buf = data.split_to(expected_size);
            for _ in 0..entry_count {
                if is_v3 {
                    let kind = entry_buf.get_u8();
                    match kind {
                        0 => {
                            // Put
                            let key_size = entry_buf.get_u16() as usize;
                            let key = entry_buf.split_to(key_size);
                            let value_size = entry_buf.get_u16() as usize;
                            let value = entry_buf.split_to(value_size);
                            handler.handle_put(key, value)?;
                        }
                        1 => {
                            // PointTombstone
                            let key_size = entry_buf.get_u16() as usize;
                            let key = entry_buf.split_to(key_size);
                            handler.handle_point_tombstone(key)?;
                        }
                        2 => {
                            // RangeTombstone
                            let start_size = entry_buf.get_u16() as usize;
                            let start = entry_buf.split_to(start_size);
                            let end_size = entry_buf.get_u16() as usize;
                            let end = entry_buf.split_to(end_size);
                            handler.handle_range_tombstone(start, end, commit_ts)?;
                        }
                        _ => {
                            anyhow::bail!("unknown WAL v3 entry kind: {}", kind);
                        }
                    }
                } else {
                    // v2: untyped entries
                    let key_size = entry_buf.get_u16() as usize;
                    let key = entry_buf.split_to(key_size);
                    let value_size = entry_buf.get_u16() as usize;
                    let value = entry_buf.split_to(value_size);
                    handler.handle_put(key, value)?;
                }
            }
            if commit_ts > max_ts {
                max_ts = commit_ts;
            }
        }

        // Truncate file to the last valid byte.
        let valid_data = data_len - data.remaining();
        let valid_file = WAL_HEADER_SIZE + valid_data;
        if (valid_file as u64) < file_len {
            f.set_len(valid_file as u64)?;
        }

        Ok((f, max_ts))
    }

    /// Parse a legacy-format WAL file, delegating each recovered entry to the
    /// given handler. Returns the file (positioned for append) and max_ts.
    fn recover_legacy_with<H: RecoveryHandler>(
        f: File,
        mut data: Bytes,
        file_len: u64,
        handler: &mut H,
    ) -> Result<(File, u64)> {
        let data_len = data.len();
        let mut max_ts: u64 = 0;

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
            handler.handle_put(key, value)?;
        }

        // Truncate file to the last valid byte.
        let valid_len = data_len - data.remaining();
        if (valid_len as u64) < file_len {
            f.set_len(valid_len as u64)?;
        }

        Ok((f, max_ts))
    }

    /// Open a WAL file, read its contents, and detect the format.
    /// Returns `(file, data, mvcc_format, is_v3, file_len)`.
    fn open_and_detect(path: impl AsRef<Path>) -> Result<(File, Bytes, bool, bool, u64)> {
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

        let data = Bytes::from(buf);

        // Detect MVCC format by checking magic number AND version field.
        let (mvcc_format, is_v3) = if data.len() >= WAL_HEADER_SIZE {
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
                (true, version == WAL_FORMAT_VERSION_V3)
            } else {
                (false, false)
            }
        } else {
            (false, false)
        };

        Ok((f, data, mvcc_format, is_v3, file_len))
    }

    /// Recover a WAL file, replaying entries into the skiplist.
    /// Returns the WAL handle and the maximum `commit_ts` found in any complete batch.
    pub fn recover(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<Bytes, Bytes>,
    ) -> Result<(Self, u64)> {
        let (f, mut data, mvcc_format, is_v3, file_len) = Self::open_and_detect(path)?;
        let mut handler = SkiplistRecovery { skiplist };

        let (f, max_ts) = if mvcc_format {
            data.advance(WAL_HEADER_SIZE);
            Self::recover_mvcc(f, data, is_v3, file_len, &mut handler)?
        } else {
            Self::recover_legacy_with(f, data, file_len, &mut handler)?
        };

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(f))),
                mvcc_format,
                is_v3,
                buf_pool: Self::new_buf_pool(),
                ready_queue: crossbeam_queue::SegQueue::new(),
                commit_waiters: AtomicU64::new(0),
                committed_gen: AtomicU64::new(0),
                leader_active: AtomicBool::new(false),
                batch_results: Mutex::new(Self::empty_batch_results()),
                commit_cond: Condvar::new(),
                commit_mutex: Mutex::new(()),
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
        let (f, mut data, mvcc_format, is_v3, file_len) = Self::open_and_detect(path)?;
        let mut handler = SkiplistRangeRecovery {
            skiplist,
            points: Vec::new(),
            point_tombstones: Vec::new(),
            range_ts: Vec::new(),
            range_tombstone_idx: 0,
        };

        let (f, max_ts) = if mvcc_format {
            data.advance(WAL_HEADER_SIZE);
            Self::recover_mvcc(f, data, is_v3, file_len, &mut handler)?
        } else {
            Self::recover_legacy_with(f, data, file_len, &mut handler)?
        };

        // Insert recovered range tombstones into the set and build the batch.
        let mut recovered_range_tombstones = Vec::with_capacity(handler.range_ts.len());
        for (start, end, ts, ordinal) in handler.range_ts {
            recovered_range_tombstones.push(RangeTombstone {
                start: start.clone(),
                end: end.clone(),
                ts,
            });
            range_tombstones.add(RangeTombstone { start, end, ts }, ordinal);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(f))),
                mvcc_format,
                is_v3,
                buf_pool: Self::new_buf_pool(),
                ready_queue: crossbeam_queue::SegQueue::new(),
                commit_waiters: AtomicU64::new(0),
                committed_gen: AtomicU64::new(0),
                leader_active: AtomicBool::new(false),
                batch_results: Mutex::new(Self::empty_batch_results()),
                commit_cond: Condvar::new(),
                commit_mutex: Mutex::new(()),
            },
            RecoveredWalBatch {
                points: handler.points,
                point_tombstones: handler.point_tombstones,
                range_tombstones: recovered_range_tombstones,
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

        // Legacy format: write directly to the file (no group commit).
        if !self.mvcc_format {
            let mut file = self.file.lock();
            // L8: Use checked_add for consistency with the MVCC path.
            let capacity = data
                .iter()
                .try_fold(0usize, |acc, (k, v)| acc.checked_add(4 + k.len() + v.len()))
                .context("legacy batch size overflow")?;
            let mut buf = Vec::with_capacity(capacity);
            for (key, value) in data {
                buf.put_u16(u16::try_from(key.len()).context("key length exceeds u16::MAX")?);
                buf.put(*key);
                buf.put_u16(u16::try_from(value.len()).context("value length exceeds u16::MAX")?);
                buf.put(*value);
            }
            return file.write_all(&buf).context("failed to write to WAL");
        }

        // MVCC format: encode into a lock-free pooled buffer, then push to
        // the ready queue.  The leader drains the queue + fsyncs in
        // `submit_and_commit`.  No mutex held during encoding.
        let per_entry_overhead = if self.is_v3 { 5 } else { 4 };
        let mut validated = Vec::with_capacity(data.len());
        let mut entries_size = 0usize;
        for (key, value) in data {
            let key_len = u16::try_from(key.len()).context("key length exceeds u16::MAX")?;
            let value_len = u16::try_from(value.len()).context("value length exceeds u16::MAX")?;
            entries_size = entries_size
                .checked_add(per_entry_overhead + key.len() + value.len())
                .context("batch size overflow")?;
            validated.push((key_len, value_len));
        }
        let total_size = BATCH_HEADER_SIZE + entries_size;

        // Pop a buffer from the pool (lock-free), or allocate a new one.
        let mut buf = self
            .buf_pool
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(total_size.max(BUFFER_POOL_BUF_SIZE)));
        buf.clear();
        buf.reserve(total_size);
        buf.resize(BATCH_HEADER_SIZE, 0);
        let mut pos = BATCH_HEADER_SIZE;

        for ((key, value), (kl, vl)) in data.iter().zip(validated.iter()) {
            if self.is_v3 {
                buf.push(WalEntryKind::Put as u8);
            }
            buf.extend_from_slice(&kl.to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&vl.to_be_bytes());
            buf.extend_from_slice(value);
            pos = buf.len();
        }

        let crc = crc32fast::hash(&buf[BATCH_HEADER_SIZE..pos]);
        buf[0..8].copy_from_slice(&commit_ts.to_be_bytes());
        buf[8..12].copy_from_slice(&entry_count.to_be_bytes());
        buf[12..16].copy_from_slice(&crc.to_be_bytes());

        // SegQueue::push is infallible (unbounded).
        self.ready_queue.push(buf);

        Ok(())
    }

    /// Write a batch of range tombstones as a single atomic WAL record.
    ///
    /// Each entry is encoded as: `[kind:1=2][start_len:2][start][end_len:2][end]`.
    /// The `commit_ts` is shared across all entries in the batch.
    ///
    /// The encoded buffer is pushed to the ready queue for group commit.
    /// The caller must call [`submit_and_commit`] (or [`sync`]) afterward to
    /// durably flush the data to disk.
    pub fn put_range_tombstone_batch(
        &self,
        tombstones: &[(&[u8], &[u8])],
        commit_ts: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            self.mvcc_format,
            "range tombstone batches require MVCC WAL format"
        );
        anyhow::ensure!(
            self.is_v3,
            "range tombstone batches require v3 WAL format (found v2)"
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

        // Push to the ready queue for group commit (M2).
        self.ready_queue.push(buf);

        Ok(())
    }

    /// Drain the ready queue, flush BufWriter, and fsync — all under a single
    /// file-lock acquisition.  The file lock is taken *before* draining so that
    /// concurrent `sync()` callers (e.g. external `engine.sync()` + leader in
    /// `submit_and_commit`) cannot steal each other's buffers.
    ///
    /// Returns the drained buffers so the caller can return them to the pool
    /// only after confirming the sync succeeded.
    fn sync_inner(&self) -> Result<Vec<Vec<u8>>> {
        let mut file = self.file.lock();

        // Drain into a local Vec first. Only return to the pool after fsync
        // succeeds, so that on I/O failure the buffers are not lost.
        let mut drained = Vec::new();
        while let Some(buf) = self.ready_queue.pop() {
            drained.push(buf);
        }
        for buf in &drained {
            file.write_all(buf)
                .context("failed to write pending WAL data")?;
        }
        file.flush()?;
        file.get_ref()
            .sync_all()
            .context("failed to sync WAL to disk")?;

        Ok(drained)
    }

    /// Drain, write, fsync, and return buffers to the pool.
    ///
    /// On success, all drained buffers are returned to the pool.
    /// On failure, the error is propagated and buffers that were drained but
    /// not successfully synced are dropped (the caller sees the error).
    pub(crate) fn sync(&self) -> Result<()> {
        let drained = self.sync_inner()?;
        // Return successfully-synced buffers to the pool.
        for buf in drained {
            if buf.capacity() <= BUFFER_POOL_BUF_SIZE * 2 {
                let _ = self.buf_pool.push(buf);
            }
        }
        Ok(())
    }

    /// Group-commit barrier: the first thread becomes the leader, drains the
    /// ready queue to the file, and fsyncs.  Subsequent threads wait on a
    /// condvar and return once the leader finishes.  This amortises the cost
    /// of `fsync` across concurrent writers.
    pub fn submit_and_commit(&self) -> Result<()> {
        let my_ticket = self.commit_waiters.fetch_add(1, Ordering::AcqRel);

        loop {
            let committed_gen = self.committed_gen.load(Ordering::Acquire);

            // Case 1: our ticket is already committed — look up the result.
            if my_ticket < committed_gen {
                let results = self.batch_results.lock();
                if let Some(entry) = Self::lookup_batch_result(&results, my_ticket) {
                    return match entry.outcome {
                        BatchOutcome::Ok => Ok(()),
                        BatchOutcome::Err => anyhow::bail!("group commit: leader sync failed"),
                    };
                }
                // Slot was overwritten (ring wrapped). This should not happen
                // if BATCH_RESULT_RING_SIZE is large enough. Treat as success
                // since the generation has advanced well past our ticket.
                return Ok(());
            }

            // Case 2: try to become the leader for this generation.
            if my_ticket == committed_gen
                && self
                    .leader_active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                let batch_start = committed_gen;
                // Drain the ready queue + write to BufWriter. Capture
                // batch_end AFTER the drain (M8) so that threads that pushed
                // buffers before the drain are included even if they haven't
                // registered a ticket yet. The SegQueue is FIFO and
                // put_batch() pushes to the queue before incrementing
                // commit_waiters, so all drained buffers are covered.
                let (result, drained) = match self.sync_inner() {
                    Ok(drained) => (Ok(()), drained),
                    Err(e) => (Err(e), Vec::new()),
                };
                let batch_end = self.commit_waiters.load(Ordering::Acquire);
                {
                    let mut results = self.batch_results.lock();
                    let idx = batch_start as usize % BATCH_RESULT_RING_SIZE;
                    results[idx] = BatchResult {
                        batch_start,
                        batch_end,
                        outcome: if result.is_ok() {
                            BatchOutcome::Ok
                        } else {
                            BatchOutcome::Err
                        },
                    };
                    // Return synced buffers to the pool.
                    for buf in drained {
                        if buf.capacity() <= BUFFER_POOL_BUF_SIZE * 2 {
                            let _ = self.buf_pool.push(buf);
                        }
                    }
                    // Update committed_gen and signal under commit_mutex to
                    // prevent lost wakeup: followers check committed_gen inside
                    // commit_mutex before calling wait(), so we must update it
                    // under the same mutex.
                    {
                        let _guard = self.commit_mutex.lock();
                        self.committed_gen.store(batch_end, Ordering::Release);
                        self.leader_active.store(false, Ordering::Release);
                        self.commit_cond.notify_all();
                    }
                }
                return result;
            }

            // Case 3: same generation but another thread became leader — wait.
            if my_ticket == committed_gen {
                let mut guard = self.commit_mutex.lock();
                while self.leader_active.load(Ordering::Acquire) {
                    self.commit_cond.wait(&mut guard);
                }
                continue;
            }

            // Case 4: our ticket is in a future generation — wait for that
            // generation to complete.
            let observed_gen = committed_gen;
            let mut guard = self.commit_mutex.lock();
            while self.committed_gen.load(Ordering::Acquire) == observed_gen {
                self.commit_cond.wait(&mut guard);
            }
        }
    }

    /// Look up the batch result for `ticket` in the ring buffer.
    /// Returns `Some(entry)` if the ticket falls within a recorded batch,
    /// `None` if the slot has been overwritten by a newer batch.
    fn lookup_batch_result(
        results: &[BatchResult; BATCH_RESULT_RING_SIZE],
        ticket: u64,
    ) -> Option<BatchResult> {
        let idx = ticket as usize % BATCH_RESULT_RING_SIZE;
        let entry = results[idx];
        if ticket >= entry.batch_start && ticket < entry.batch_end {
            Some(entry)
        } else {
            None
        }
    }
}
