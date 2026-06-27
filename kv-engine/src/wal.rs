use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    os::unix::{fs::OpenOptionsExt, io::AsRawFd},
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
/// MVCC WAL format version 4: 20-byte batch header with `data_len` for
/// O_DIRECT alignment-gap skipping.
const WAL_FORMAT_VERSION_V4: u16 = 4;
/// Size of the WAL file header: magic (4) + version (2) = 6 bytes.
const WAL_HEADER_SIZE: usize = 6;
/// Size of a batch header (v2/v3): commit_ts (8) + entry_count (4) + data_crc32 (4) = 16 bytes.
const BATCH_HEADER_SIZE: usize = 16;
/// v4 batch header size: commit_ts(8) + entry_count(4) + data_crc32(4) + data_len(4) = 20 bytes.
const V4_BATCH_HEADER_SIZE: usize = 20;
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

// ── io_uring + O_DIRECT constants ──────────────────────────────────────────

/// Fixed-file index for the WAL fd in the io_uring registered files table.
const WAL_FD_INDEX: u32 = 0;
/// Number of SQE slots in the io_uring ring.
const RING_SIZE: usize = 64;
/// Maximum write SQEs per chunk (leave 1 slot for the fsync DRAIN SQE).
const MAX_WRITES_PER_CHUNK: usize = RING_SIZE - 1;
/// Preallocation block size (1 MiB).
const PREALLOC_BLOCK: u64 = 1 << 20;
/// Ring buffer size for the completion barrier generation results.
/// Large enough to prevent wrap-around under realistic concurrency (256 threads
/// would need to complete between a waiter dropping submission_lock and acquiring
/// the completion mutex — practically impossible).
const COMPLETION_RING_SIZE: usize = 256;

// ── DirectBuf: page-aligned buffer for O_DIRECT I/O ───────────────────────

/// A page-aligned buffer for O_DIRECT I/O.
///
/// `Vec<u8>` cannot be used because its global allocator deallocates with
/// alignment 1, but O_DIRECT requires 4096-byte alignment. Dropping a
/// Vec backed by a 4096-aligned allocation is undefined behavior.
///
/// Buffers are pooled via [`ArrayQueue`] and recycled across writes. On error,
/// buffers are returned to the pool (not dropped) to prevent pool exhaustion.
/// If the pool is full, the buffer is dropped (freed) — this is safe because
/// `Drop` calls `libc::free` on the `posix_memalign` allocation.
struct DirectBuf {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

// SAFETY: DirectBuf owns its allocation exclusively. The raw pointer is not
// aliased. Send is required for transfer via ArrayQueue.
unsafe impl Send for DirectBuf {}

impl DirectBuf {
    fn new(size: usize) -> Self {
        // Cap must be 4KB-aligned to prevent io_uring out-of-bounds reads.
        // align_up() rounds write sizes to 4KB; if cap is smaller, io_uring
        // reads past the allocation.
        let cap = Self::align_up(size.max(4096));
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let ret = unsafe { libc::posix_memalign(&mut ptr, 4096, cap) };
        // posix_memalign failure is OOM — recovery is impossible at this point.
        assert_eq!(ret, 0, "posix_memalign failed (OOM)");
        // Zero-initialize: creating a &mut [u8] over uninitialized memory is UB.
        // Also prevents stale data leakage if the buffer is read before being
        // fully written.
        unsafe { std::ptr::write_bytes(ptr as *mut u8, 0, cap) };
        Self {
            ptr: ptr as *mut u8,
            len: 0,
            cap,
        }
    }

    /// Round `size` up to the next 4KB boundary (O_DIRECT alignment requirement).
    fn align_up(size: usize) -> usize {
        (size + 4095) & !4095
    }

    /// Returns a mutable slice over the full allocation (capacity, not length).
    /// Used for encoding batch data and zero-padding the alignment gap.
    /// Callers must not write beyond the intended region (header + entries +
    /// padding) — the cap is 4KB-aligned to prevent io_uring out-of-bounds reads.
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    fn len(&self) -> usize {
        self.len
    }

    fn cap(&self) -> usize {
        self.cap
    }

    fn set_len(&mut self, len: usize) {
        self.len = len;
    }

    fn clear(&mut self) {
        self.len = 0;
    }
}

impl Drop for DirectBuf {
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr as *mut libc::c_void) };
    }
}

// ── io_uring completion barrier ────────────────────────────────────────────

/// Shared completion state for the fate-sharing barrier.
///
/// Multiple threads call `submit_and_commit()` concurrently. Only one thread
/// drains `pending` and submits SQEs. The others wait for the CQE result
/// before returning — otherwise they may call `publish_raw_batch()` before
/// the data is durable.
struct CompletionState {
    mutex: parking_lot::Mutex<CompletionInner>,
    cond: Condvar,
}

/// Result slot for the completion barrier ring buffer.
type CompletionSlot = Option<(u64, Result<(), Arc<anyhow::Error>>)>;

struct CompletionInner {
    /// Generation counter — incremented on each commit cycle.
    generation: u64,
    /// Ring buffer of recent commit results, indexed by generation % COMPLETION_RING_SIZE.
    /// Each slot stores (generation, result) so a waiter can verify it is reading
    /// its own generation's result. Wrapped in Arc so each waiter can clone
    /// (anyhow::Error is !Clone).
    results: Vec<CompletionSlot>,
    /// True while a submitter is actively running I/O.
    in_flight: bool,
}

impl CompletionState {
    fn new() -> Self {
        let mut results = Vec::with_capacity(COMPLETION_RING_SIZE);
        results.resize_with(COMPLETION_RING_SIZE, || None);
        Self {
            mutex: parking_lot::Mutex::new(CompletionInner {
                generation: 0,
                results,
                in_flight: false,
            }),
            cond: Condvar::new(),
        }
    }
}

pub struct Wal {
    /// Buffered file handle for recovery reads and legacy `put()`.
    /// Not used for MVCC batch writes (those go through io_uring + O_DIRECT).
    pub(crate) buffered_file: Arc<Mutex<BufWriter<File>>>,
    /// Whether this WAL uses the MVCC batch format (has file header).
    mvcc_format: bool,
    /// Whether this WAL uses v3 typed entries (kind prefix).
    /// Only meaningful when `mvcc_format` is true. When false, the WAL uses v2
    /// untyped entries. Preserved from recovery so appended records match the
    /// file header.
    is_v3: bool,

    // ── io_uring + O_DIRECT fields ───────────────────────────────────────
    /// O_DIRECT file handle for io_uring writes (None for legacy buffered WALs).
    direct_file: Option<File>,
    /// io_uring ring for parallel WAL writes (None for legacy buffered WALs).
    ring: Option<Mutex<io_uring::IoUring>>,
    /// Lock-free pool of page-aligned buffers for O_DIRECT I/O.
    direct_buf_pool: ArrayQueue<DirectBuf>,
    /// Encoded DirectBuf buffers waiting for io_uring submission.
    pending: Mutex<Vec<DirectBuf>>,
    /// Atomic file offset allocator.
    alloc_offset: AtomicU64,
    /// Preallocated file size (tracked in memory).
    preallocated_size: AtomicU64,
    /// Shared completion state for the fate-sharing barrier.
    completion_state: CompletionState,
    /// Serializes the drain-or-wait decision in submit_and_commit.
    submission_lock: Mutex<()>,
    /// Set to true on I/O error to fail-fast future writes.
    /// Once poisoned, the WAL is unusable — callers must create a new WAL.
    poisoned: AtomicBool,
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
    // ── io_uring + O_DIRECT helpers ────────────────────────────────────────

    /// Create a pre-filled lock-free pool of page-aligned DirectBuf buffers.
    fn new_direct_buf_pool() -> ArrayQueue<DirectBuf> {
        let pool = ArrayQueue::new(BUFFER_POOL_CAPACITY);
        for _ in 0..BUFFER_POOL_CAPACITY {
            let _ = pool.push(DirectBuf::new(BUFFER_POOL_BUF_SIZE));
        }
        pool
    }

    /// Initialize io_uring + O_DIRECT for the given WAL file path.
    /// Returns (ring, direct_file, alloc_offset).
    fn try_init_io_uring(path: &Path) -> Result<(io_uring::IoUring, File, u64)> {
        let ring =
            io_uring::IoUring::new(RING_SIZE as u32).context("failed to create io_uring ring")?;

        let direct_file = File::options()
            .read(true)
            .write(true) // NOT append — pwrite controls position
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .context("failed to open O_DIRECT handle")?;

        let raw_fd = direct_file.as_raw_fd();
        ring.submitter()
            .register_files(&[raw_fd])
            .context("failed to register WAL fd with io_uring")?;

        let alloc_offset = direct_file.metadata()?.len();

        Ok((ring, direct_file, alloc_offset))
    }

    /// Construct a Wal from a recovered file, setting up io_uring for MVCC WALs.
    ///
    /// For legacy (non-MVCC) WALs, io_uring and O_DIRECT are skipped entirely —
    /// all writes go through the buffered file handle. This ensures recovery
    /// succeeds on systems/kernels where io_uring or O_DIRECT is unavailable.
    fn new_recovered(mvcc_format: bool, is_v3: bool, file_len: u64, path: &Path) -> Result<Self> {
        // Re-open a buffered handle for recovery reads and legacy put().
        let buf_file = File::options().read(true).append(true).open(path)?;

        if mvcc_format {
            let (ring, direct_file, alloc_offset) = Self::try_init_io_uring(path)?;
            Ok(Self {
                buffered_file: Arc::new(Mutex::new(BufWriter::new(buf_file))),
                mvcc_format,
                is_v3,
                direct_file: Some(direct_file),
                ring: Some(Mutex::new(ring)),
                direct_buf_pool: Self::new_direct_buf_pool(),
                pending: Mutex::new(Vec::new()),
                alloc_offset: AtomicU64::new(alloc_offset),
                preallocated_size: AtomicU64::new(file_len),
                completion_state: CompletionState::new(),
                submission_lock: Mutex::new(()),
                poisoned: AtomicBool::new(false),
            })
        } else {
            log::info!("WAL: recovered non-MVCC WAL, using buffered I/O only");
            Ok(Self {
                buffered_file: Arc::new(Mutex::new(BufWriter::new(buf_file))),
                mvcc_format,
                is_v3,
                direct_file: None,
                ring: None,
                direct_buf_pool: Self::new_direct_buf_pool(),
                pending: Mutex::new(Vec::new()),
                alloc_offset: AtomicU64::new(0),
                preallocated_size: AtomicU64::new(0),
                completion_state: CompletionState::new(),
                submission_lock: Mutex::new(()),
                poisoned: AtomicBool::new(false),
            })
        }
    }

    /// Preallocate WAL space in 1 MiB increments to stay on ext4's shared
    /// overwrite path (parallel DIO requires writes within preallocated extent).
    ///
    /// `needed` is the total aligned bytes about to be written (so the
    /// preallocation covers the full batch, not just the current offset).
    fn maybe_preallocate(&self, needed: u64) -> Result<()> {
        let offset = self.alloc_offset.load(Ordering::Acquire);
        let file_size = self.preallocated_size.load(Ordering::Acquire);
        let required_end = offset + needed;
        if required_end > file_size {
            // Round up to the next PREALLOC_BLOCK boundary.
            let new_size = required_end.div_ceil(PREALLOC_BLOCK) * PREALLOC_BLOCK;
            // SAFETY: only called for MVCC WALs which always have a direct_file.
            let direct_file = self.direct_file.as_ref().unwrap();
            let fd = direct_file.as_raw_fd();
            let alloc_len = new_size - file_size;
            let ret = unsafe { libc::fallocate(fd, 0, file_size as i64, alloc_len as i64) };
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EOPNOTSUPP)
                    || err.raw_os_error() == Some(libc::ENOSYS)
                {
                    log::warn!(
                        "fallocate not supported, falling back to ftruncate: {}",
                        err
                    );
                    direct_file.set_len(new_size)?;
                } else {
                    anyhow::bail!("fallocate failed: {}", err);
                }
            }
            self.preallocated_size.store(new_size, Ordering::Release);
        }
        Ok(())
    }

    /// Encode a point-entry batch into a DirectBuf with v4 header, zero-pad to
    /// 4KB alignment, and push to the `pending` queue for io_uring submission.
    fn encode_and_push_direct_buf(
        &self,
        data: &[(&[u8], &[u8])],
        validated: &[(u16, u16)],
        entries_size: usize,
        commit_ts: u64,
        entry_count: u32,
    ) -> Result<()> {
        // Fail fast after I/O error — don't allocate or enqueue into `pending`.
        if self.poisoned.load(Ordering::Acquire) {
            anyhow::bail!("WAL is poisoned due to a previous I/O error");
        }

        // Reject empty batches with commit_ts=0 — they produce an all-zero v4
        // header indistinguishable from preallocated (fallocate'd) space, which
        // recovery treats as end-of-log, silently truncating later records.
        anyhow::ensure!(
            entry_count > 0 || commit_ts != 0,
            "empty batch with commit_ts=0 would produce all-zero v4 header"
        );

        anyhow::ensure!(
            entries_size <= u32::MAX as usize,
            "batch entries_size exceeds u32::MAX"
        );

        let total_size = V4_BATCH_HEADER_SIZE + entries_size;
        let alloc_size = DirectBuf::align_up(total_size).max(BUFFER_POOL_BUF_SIZE);

        let mut buf = match self.direct_buf_pool.pop() {
            Some(b) if b.cap() >= alloc_size => b,
            Some(b) => {
                // Return undersized buffer to pool to avoid permanent capacity loss.
                let _ = self.direct_buf_pool.push(b);
                DirectBuf::new(alloc_size)
            }
            None => DirectBuf::new(alloc_size),
        };
        buf.clear();

        let slice = buf.as_mut_slice();
        // Reserve header space (filled later).
        let mut pos = V4_BATCH_HEADER_SIZE;

        for ((key, value), (kl, vl)) in data.iter().zip(validated.iter()) {
            if self.is_v3 {
                slice[pos] = WalEntryKind::Put as u8;
                pos += 1;
            }
            slice[pos..pos + 2].copy_from_slice(&kl.to_be_bytes());
            pos += 2;
            slice[pos..pos + key.len()].copy_from_slice(key);
            pos += key.len();
            slice[pos..pos + 2].copy_from_slice(&vl.to_be_bytes());
            pos += 2;
            slice[pos..pos + value.len()].copy_from_slice(value);
            pos += value.len();
        }

        let crc = crc32fast::hash(&slice[V4_BATCH_HEADER_SIZE..pos]);
        slice[0..8].copy_from_slice(&commit_ts.to_be_bytes());
        slice[8..12].copy_from_slice(&entry_count.to_be_bytes());
        slice[12..16].copy_from_slice(&crc.to_be_bytes());
        slice[16..20].copy_from_slice(&(entries_size as u32).to_be_bytes());

        // Zero-pad to 4KB alignment (O_DIRECT requirement).
        let aligned_len = DirectBuf::align_up(pos);
        slice[pos..aligned_len].fill(0);
        buf.set_len(aligned_len);

        self.pending.lock().push(buf);
        Ok(())
    }

    /// Encode a range-tombstone batch into a DirectBuf with v4 header, zero-pad
    /// to 4KB alignment, and push to the `pending` queue.
    fn encode_and_push_direct_buf_range(
        &self,
        tombstones: &[(&[u8], &[u8])],
        commit_ts: u64,
        entry_count: u32,
    ) -> Result<()> {
        // Fail fast after I/O error — don't allocate or enqueue into `pending`.
        if self.poisoned.load(Ordering::Acquire) {
            anyhow::bail!("WAL is poisoned due to a previous I/O error");
        }

        // Reject empty batches with commit_ts=0 — they produce an all-zero v4
        // header indistinguishable from preallocated (fallocate'd) space, which
        // recovery treats as end-of-log, silently truncating later records.
        anyhow::ensure!(
            !tombstones.is_empty() || commit_ts != 0,
            "empty range batch with commit_ts=0 would produce all-zero v4 header"
        );

        let entries_size: usize = tombstones.iter().map(|(s, e)| 5 + s.len() + e.len()).sum();
        anyhow::ensure!(
            entries_size <= u32::MAX as usize,
            "batch entries_size exceeds u32::MAX"
        );

        let total_size = V4_BATCH_HEADER_SIZE + entries_size;
        let alloc_size = DirectBuf::align_up(total_size).max(BUFFER_POOL_BUF_SIZE);

        let mut buf = match self.direct_buf_pool.pop() {
            Some(b) if b.cap() >= alloc_size => b,
            Some(b) => {
                // Return undersized buffer to pool to avoid permanent capacity loss.
                let _ = self.direct_buf_pool.push(b);
                DirectBuf::new(alloc_size)
            }
            None => DirectBuf::new(alloc_size),
        };
        buf.clear();

        let slice = buf.as_mut_slice();
        let mut pos = V4_BATCH_HEADER_SIZE;

        for (start, end) in tombstones {
            slice[pos] = WalEntryKind::RangeTombstone as u8;
            pos += 1;
            slice[pos..pos + 2].copy_from_slice(&(start.len() as u16).to_be_bytes());
            pos += 2;
            slice[pos..pos + start.len()].copy_from_slice(start);
            pos += start.len();
            slice[pos..pos + 2].copy_from_slice(&(end.len() as u16).to_be_bytes());
            pos += 2;
            slice[pos..pos + end.len()].copy_from_slice(end);
            pos += end.len();
        }

        let crc = crc32fast::hash(&slice[V4_BATCH_HEADER_SIZE..pos]);
        slice[0..8].copy_from_slice(&commit_ts.to_be_bytes());
        slice[8..12].copy_from_slice(&entry_count.to_be_bytes());
        slice[12..16].copy_from_slice(&crc.to_be_bytes());
        slice[16..20].copy_from_slice(&(entries_size as u32).to_be_bytes());

        let aligned_len = DirectBuf::align_up(pos);
        slice[pos..aligned_len].fill(0);
        buf.set_len(aligned_len);

        self.pending.lock().push(buf);
        Ok(())
    }

    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let f = File::create_new(path.as_ref()).context("failed to create WAL")?;
        let mut w = BufWriter::new(f);
        // Write MVCC WAL header (big-endian to match Buf::get_u32/get_u16).
        w.write_all(&WAL_MVCC_MAGIC.to_be_bytes())?;
        w.write_all(&WAL_FORMAT_VERSION_V4.to_be_bytes())?;
        // Pad header to 4096 bytes with zeros so alloc_offset starts at a 4KB boundary.
        // Use a stack buffer to avoid a heap allocation.
        let header_pad = [0u8; 4096 - WAL_HEADER_SIZE];
        w.write_all(&header_pad)?;
        w.flush()?;
        drop(w); // close buffered handle before opening O_DIRECT handle

        // Initialize io_uring + O_DIRECT AFTER header is flushed so alloc_offset is correct.
        let (ring, direct_file, alloc_offset) = Self::try_init_io_uring(path.as_ref())?;

        // Re-open buffered handle for recovery reads and legacy put().
        let buf_file = File::options()
            .read(true)
            .append(true)
            .open(path.as_ref())?;

        Ok(Self {
            buffered_file: Arc::new(Mutex::new(BufWriter::new(buf_file))),
            mvcc_format: true,
            is_v3: true,
            direct_file: Some(direct_file),
            ring: Some(Mutex::new(ring)),
            direct_buf_pool: Self::new_direct_buf_pool(),
            pending: Mutex::new(Vec::new()),
            alloc_offset: AtomicU64::new(alloc_offset),
            preallocated_size: AtomicU64::new(alloc_offset),
            completion_state: CompletionState::new(),
            submission_lock: Mutex::new(()),
            poisoned: AtomicBool::new(false),
        })
    }

    /// Parse an MVCC-format WAL file, delegating each recovered entry to the
    /// given handler. Returns the file (positioned for append) and max_ts.
    fn recover_mvcc<H: RecoveryHandler>(
        f: File,
        mut data: Bytes,
        is_v3: bool,
        wal_version: u16,
        file_len: u64,
        handler: &mut H,
    ) -> Result<(File, u64)> {
        let data_len = data.len();
        let mut max_ts: u64 = 0;
        let is_v4 = wal_version >= WAL_FORMAT_VERSION_V4;
        let batch_hdr_size = if is_v4 {
            V4_BATCH_HEADER_SIZE
        } else {
            BATCH_HEADER_SIZE
        };

        while data.remaining() >= batch_hdr_size {
            let before_batch = data.clone();
            let commit_ts = data.get_u64();
            let entry_count = data.get_u32() as usize;
            let data_crc32 = data.get_u32();
            // v4: read data_len for alignment-gap skipping.
            let data_len_field = if is_v4 { data.get_u32() as usize } else { 0 };
            handler.reset_range_ordinals();

            // v4: reject all-zero pages (preallocated but unwritten). Without
            // this check, fallocate'd zeros pass CRC(empty) and recovery treats
            // the page as a valid empty batch, advancing into unwritten space.
            if is_v4 && commit_ts == 0 && entry_count == 0 && data_crc32 == 0 && data_len_field == 0
            {
                data = before_batch;
                break;
            }

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

            // v4: validate data_len matches the actual parsed entry size.
            // The data_len field is not covered by CRC, so a bit-flip could
            // cause recovery to skip incorrectly. Catch the mismatch here.
            if is_v4 && data_len_field != expected_size {
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

            // v4: skip alignment gap — advance past zero-padding to next 4KB boundary.
            if is_v4 {
                let total_batch_size = batch_hdr_size + data_len_field;
                let aligned_size = DirectBuf::align_up(total_batch_size);
                let consumed = expected_size + batch_hdr_size;
                if aligned_size > consumed {
                    let skip = aligned_size - consumed;
                    if data.remaining() >= skip {
                        data.advance(skip);
                    } else {
                        data.advance(data.remaining());
                    }
                }
            }
        }

        // Truncate file to the last valid byte.
        let scan_start = if is_v4 {
            DirectBuf::align_up(WAL_HEADER_SIZE)
        } else {
            WAL_HEADER_SIZE
        };
        let valid_data = data_len - data.remaining();
        let valid_file = scan_start + valid_data;
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
    /// Returns `(file, data, mvcc_format, is_v3, wal_version, file_len)`.
    fn open_and_detect(path: impl AsRef<Path>) -> Result<(File, Bytes, bool, bool, u16, u64)> {
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
        let (mvcc_format, is_v3, wal_version) = if data.len() >= WAL_HEADER_SIZE {
            let magic = (&data[..4]).get_u32();
            let version = (&data[4..6]).get_u16();
            if magic == WAL_MVCC_MAGIC {
                anyhow::ensure!(
                    matches!(
                        version,
                        WAL_FORMAT_VERSION_V2 | WAL_FORMAT_VERSION_V3 | WAL_FORMAT_VERSION_V4
                    ),
                    "unsupported WAL version: got {}, expected {}, {}, or {}",
                    version,
                    WAL_FORMAT_VERSION_V2,
                    WAL_FORMAT_VERSION_V3,
                    WAL_FORMAT_VERSION_V4
                );
                (
                    true,
                    matches!(version, WAL_FORMAT_VERSION_V3 | WAL_FORMAT_VERSION_V4),
                    version,
                )
            } else {
                (false, false, 0)
            }
        } else {
            (false, false, 0)
        };

        Ok((f, data, mvcc_format, is_v3, wal_version, file_len))
    }

    /// Recover a WAL file, replaying entries into the skiplist.
    /// Returns the WAL handle and the maximum `commit_ts` found in any complete batch.
    pub fn recover(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<Bytes, Bytes>,
    ) -> Result<(Self, u64)> {
        let (f, mut data, mvcc_format, is_v3, wal_version, file_len) =
            Self::open_and_detect(&path)?;
        let mut handler = SkiplistRecovery { skiplist };

        let (f, max_ts) = if mvcc_format {
            let scan_start = if wal_version >= WAL_FORMAT_VERSION_V4 {
                DirectBuf::align_up(WAL_HEADER_SIZE)
            } else {
                WAL_HEADER_SIZE
            };
            // A truncated WAL (crash after header write) may have fewer bytes
            // than the padded header size. Treat as empty rather than panicking.
            // Extend the file to scan_start so O_DIRECT writes start at an
            // aligned offset (otherwise pwrite at unaligned EOF fails EINVAL).
            if data.len() < scan_start {
                data.advance(data.len());
                f.set_len(scan_start as u64)?;
                (f, 0u64)
            } else {
                data.advance(scan_start);
                Self::recover_mvcc(f, data, is_v3, wal_version, file_len, &mut handler)?
            }
        } else {
            Self::recover_legacy_with(f, data, file_len, &mut handler)?
        };

        let file_len_after = f.metadata()?.len();

        Ok((
            Self::new_recovered(mvcc_format, is_v3, file_len_after, path.as_ref())?,
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
        let (f, mut data, mvcc_format, is_v3, wal_version, file_len) =
            Self::open_and_detect(&path)?;
        let mut handler = SkiplistRangeRecovery {
            skiplist,
            points: Vec::new(),
            point_tombstones: Vec::new(),
            range_ts: Vec::new(),
            range_tombstone_idx: 0,
        };

        let (f, max_ts) = if mvcc_format {
            let scan_start = if wal_version >= WAL_FORMAT_VERSION_V4 {
                DirectBuf::align_up(WAL_HEADER_SIZE)
            } else {
                WAL_HEADER_SIZE
            };
            if data.len() < scan_start {
                data.advance(data.len());
                f.set_len(scan_start as u64)?;
                (f, 0u64)
            } else {
                data.advance(scan_start);
                Self::recover_mvcc(f, data, is_v3, wal_version, file_len, &mut handler)?
            }
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

        let file_len_after = f.metadata()?.len();

        Ok((
            Self::new_recovered(mvcc_format, is_v3, file_len_after, path.as_ref())?,
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
        let mut file = self.buffered_file.lock();
        let mut buf = Vec::with_capacity(key.len() + value.len() + 4);

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
            let mut file = self.buffered_file.lock();
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

        // MVCC format: encode into a DirectBuf with v4 header, push to pending.
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

        self.encode_and_push_direct_buf(data, &validated, entries_size, commit_ts, entry_count)
    }

    /// Write a batch of range tombstones as a single atomic WAL record.
    ///
    /// Each entry is encoded as: `[kind:1=2][start_len:2][start][end_len:2][end]`.
    /// The `commit_ts` is shared across all entries in the batch.
    ///
    /// The encoded buffer is pushed to the pending queue for io_uring submission.
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

        self.encode_and_push_direct_buf_range(tombstones, commit_ts, entry_count)
    }

    /// Durably sync the WAL to disk.
    ///
    /// For MVCC WALs: delegates to `submit_and_commit` (io_uring path).
    /// For legacy WALs: flushes and fsyncs the BufWriter directly.
    pub(crate) fn sync(&self) -> Result<()> {
        if !self.mvcc_format {
            // Legacy path: flush and fsync the BufWriter directly.
            // submit_and_commit() only handles the io_uring path.
            let mut file = self.buffered_file.lock();
            file.flush().context("failed to flush WAL")?;
            file.get_ref()
                .sync_all()
                .context("failed to sync WAL to disk")?;
            return Ok(());
        }
        self.submit_and_commit()
    }

    /// Close the WAL, draining any pending buffers and in-flight io_uring SQEs.
    pub fn close(&self) -> Result<()> {
        // Flush legacy buffered writes before draining io_uring.
        // submit_and_commit() only handles the io_uring path, so buffered
        // data from non-MVCC put() calls would be lost without this flush.
        if !self.mvcc_format {
            let mut file = self.buffered_file.lock();
            file.flush().context("failed to flush WAL on close")?;
            file.get_ref()
                .sync_all()
                .context("failed to sync WAL on close")?;
            return Ok(());
        }

        self.submit_and_commit()?;

        if let Some(ref ring) = self.ring {
            let mut ring = ring.lock();
            let cq = ring.completion();
            for cqe in cq {
                if cqe.result() < 0 {
                    log::error!("io_uring: stale CQE with error: {}", cqe.result());
                }
            }
        }

        Ok(())
    }

    // ── submit_and_commit: io_uring completion barrier ─────────────────────

    /// Group-commit barrier.
    ///
    /// Serializes drain-or-wait via `submission_lock`.
    /// The submitter drains `pending`, allocates offsets, submits SQEs, polls
    /// CQEs, and broadcasts the result via the completion barrier.
    pub fn submit_and_commit(&self) -> Result<()> {
        // Check poisoned flag — fail fast after I/O error.
        if self.poisoned.load(Ordering::Acquire) {
            anyhow::bail!("WAL is poisoned due to a previous I/O error");
        }

        // Serialize I/O submission: hold the lock while draining pending and
        // submitting. This eliminates the TOCTOU race between checking for
        // empty pending and actually draining it.
        let _submit_guard = self.submission_lock.lock();

        // Capture generation AFTER acquiring submission_lock. This ensures that
        // if our data was already drained and committed by a prior submitter,
        // we wait for the correct generation (my_generation - 1) instead of a
        // stale one, preventing a race where we return Ok for a failed commit.
        let my_generation = {
            let state = self.completion_state.mutex.lock();
            state.generation
        };

        let bufs = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };

        if bufs.is_empty() {
            // Nothing pending — our data was already drained and committed by
            // a prior submitter (or there was genuinely nothing to submit).
            // Release submission_lock before waiting — the leader needs it.
            drop(_submit_guard);
            if my_generation == 0 {
                return Ok(()); // Nothing was ever submitted.
            }
            // my_generation is the NEXT generation to commit. Our data was in
            // the generation before it (which already completed).
            return self.wait_for_completion(my_generation - 1);
        }

        // Mark in-flight and do the I/O.
        {
            let mut state = self.completion_state.mutex.lock();
            state.in_flight = true;
        }

        let result = self.submit_sqes_and_poll(bufs);

        // Poison the WAL on I/O error to prevent future writes.
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }

        // Signal all waiters (even on error).
        let shared_result = result
            .as_ref()
            .map(|_| ())
            .map_err(|e| Arc::new(anyhow::anyhow!("{e}")));
        {
            let mut state = self.completion_state.mutex.lock();
            let idx = (state.generation as usize) % COMPLETION_RING_SIZE;
            state.results[idx] = Some((state.generation, shared_result));
            state.in_flight = false;
            state.generation += 1;
            self.completion_state.cond.notify_all();
        }

        result
    }

    /// Wait for a commit at `min_generation` to complete and return its result.
    fn wait_for_completion(&self, min_generation: u64) -> Result<()> {
        let mut state = self.completion_state.mutex.lock();
        while state.generation <= min_generation
            || state.results[(min_generation as usize) % COMPLETION_RING_SIZE].is_none()
        {
            if !state.in_flight {
                if state.generation > min_generation {
                    break; // Our generation was committed by a prior leader.
                }
                if state.generation == min_generation {
                    break; // Nothing was ever submitted for this generation.
                }
            }
            self.completion_state.cond.wait(&mut state);
        }
        let idx = (min_generation as usize) % COMPLETION_RING_SIZE;
        match &state.results[idx] {
            Some((g, Ok(()))) if *g == min_generation => Ok(()),
            Some((g, Err(e))) if *g == min_generation => Err(anyhow::anyhow!("{e}")),
            Some((g, _)) => {
                // Ring buffer wrap-around: the slot was overwritten by a newer
                // generation's result. Since I/O errors poison the WAL and
                // prevent later generations from completing, a wraparound
                // implies our commit was durable. Treat as success.
                log::warn!(
                    "completion ring buffer wrap-around: expected gen {}, got {}; \
                     treating as success (later generations completed)",
                    min_generation,
                    g
                );
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Submit DirectBuf buffers as io_uring SQEs, poll CQEs for completion.
    ///
    /// Handles chunked submission when the batch exceeds ring capacity (64 SQEs).
    /// The entire drained set is one logical commit group.
    fn submit_sqes_and_poll(&self, bufs: Vec<DirectBuf>) -> Result<()> {
        // SAFETY: only called for MVCC WALs which always have a ring.
        let ring_ref = self.ring.as_ref().unwrap();

        // Compute total aligned size first, then preallocate to cover the full batch.
        let total_size: u64 = bufs
            .iter()
            .map(|b| DirectBuf::align_up(b.len()) as u64)
            .sum();
        self.maybe_preallocate(total_size)?;

        // Allocate file offsets atomically using aligned sizes.
        // NOTE: On write failure, the offset space is consumed but no valid data
        // was written. The resulting zero-filled holes are harmless on recovery
        // (commit_ts=0, entry_count=0 batches are skipped). This is acceptable
        // because write failures are rare and the WAL is poisoned after one.
        let base_offset = self.alloc_offset.fetch_add(total_size, Ordering::AcqRel);
        let mut offset = base_offset;
        let mut global_idx: u64 = 0;

        // Wrap in ManuallyDrop so we can leak on fatal error. If submit_and_wait
        // fails, the kernel may still be reading from the submitted buffers —
        // freeing them would be use-after-free. The WAL is poisoned after any
        // error, so the leak is bounded (one batch max).
        let bufs = std::mem::ManuallyDrop::new(bufs);

        // Collect chunk boundaries as index ranges so we can mutably access bufs
        // on error paths without borrow conflicts.
        let chunk_ranges: Vec<(usize, usize)> = {
            let len = bufs.len();
            (0..len)
                .step_by(MAX_WRITES_PER_CHUNK)
                .map(|start| (start, (start + MAX_WRITES_PER_CHUNK).min(len)))
                .collect()
        };

        for &(chunk_start, chunk_end) in &chunk_ranges {
            let chunk_len = chunk_end - chunk_start;

            // Submit write SQEs only (no per-chunk fsync).
            let mut ring = ring_ref.lock();
            for i in 0..chunk_len {
                let buf = &bufs[chunk_start + i];
                let aligned_len = DirectBuf::align_up(buf.len());
                let sqe = io_uring::opcode::Write::new(
                    io_uring::types::Fixed(WAL_FD_INDEX),
                    buf.as_ptr(),
                    aligned_len as u32,
                )
                .offset(offset)
                .build()
                .user_data(global_idx + i as u64);
                unsafe {
                    ring.submission().push(&sqe)?;
                }
                offset += aligned_len as u64;
            }

            // Submit write SQEs and wait for completion.
            if let Err(e) = ring.submit_and_wait(chunk_len) {
                let cq = ring.completion();
                for _cqe in cq {}
                anyhow::bail!("io_uring submit_and_wait failed: {}", e);
            }

            // Poll write CQEs.
            let mut write_err: Option<i32> = None;
            let chunk_start_idx = global_idx;
            let mut cq = ring.completion();
            for _ in 0..chunk_len {
                let cqe = cq
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("io_uring: missing write CQE"))?;
                let user_data = cqe.user_data();
                let local_idx = user_data.checked_sub(chunk_start_idx);
                match local_idx {
                    Some(idx) if idx < chunk_len as u64 => {
                        if cqe.result() < 0 {
                            if write_err.is_none() {
                                write_err = Some(cqe.result());
                            }
                        } else {
                            let written = cqe.result() as usize;
                            let expected =
                                DirectBuf::align_up(bufs[chunk_start + idx as usize].len());
                            if written < expected && write_err.is_none() {
                                write_err = Some(-libc::EIO);
                            }
                        }
                    }
                    _ => {
                        log::error!("io_uring: stale CQE with user_data={}", user_data);
                        drop(cq);
                        drop(ring);
                        anyhow::bail!("io_uring: stale CQE with user_data={}", user_data);
                    }
                }
            }
            drop(cq);
            drop(ring);

            if let Some(err) = write_err {
                anyhow::bail!("io_uring write error: {}", err);
            }

            global_idx += chunk_len as u64;
        }

        // Single fsync after all chunks are written. This ensures we never
        // durably persist a prefix of a commit group that ultimately fails.
        {
            let mut ring = ring_ref.lock();
            let fsync_sqe =
                io_uring::opcode::Fsync::new(io_uring::types::Fixed(WAL_FD_INDEX))
                    .flags(io_uring::types::FsyncFlags::DATASYNC)
                    .build()
                    .flags(io_uring::squeue::Flags::IO_DRAIN)
                    .user_data(u64::MAX);
            unsafe {
                ring.submission().push(&fsync_sqe)?;
            }
            if let Err(e) = ring.submit_and_wait(1) {
                let cq = ring.completion();
                for _cqe in cq {}
                anyhow::bail!("io_uring fsync submit_and_wait failed: {}", e);
            }
            let mut cq = ring.completion();
            let cqe = cq
                .next()
                .ok_or_else(|| anyhow::anyhow!("io_uring: missing fsync CQE"))?;
            if cqe.result() < 0 {
                anyhow::bail!("io_uring fsync error: {}", cqe.result());
            }
        }

        // Return buffers to pool on success. into_inner is safe here because
        // we only reach this path on success (no SQEs in flight).
        // Cap: don't return oversized buffers from bulk loads to the pool.
        let bufs = std::mem::ManuallyDrop::into_inner(bufs);
        for buf in bufs {
            if buf.cap() <= BUFFER_POOL_BUF_SIZE * 2 {
                let _ = self.direct_buf_pool.push(buf);
            }
        }

        Ok(())
    }
}
