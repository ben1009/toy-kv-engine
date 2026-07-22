use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    os::unix::{fs::OpenOptionsExt, io::AsRawFd},
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

#[cfg(feature = "bench")]
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_queue::ArrayQueue;
use crossbeam_skiplist::SkipMap;
use parking_lot::{Condvar, Mutex};

use crate::{key::KeySlice, range_tombstone::RangeTombstone};

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

/// Pre-allocated buffer size for the lock-free pool (256 KB).
const BUFFER_POOL_BUF_SIZE: usize = 256 * 1024;
/// Number of buffers pre-allocated in the pool.
const BUFFER_POOL_CAPACITY: usize = 64;

// ── io_uring + O_DIRECT constants ──────────────────────────────────────────

/// Fixed-file index for the WAL fd in the io_uring registered files table.
const WAL_FD_INDEX: u32 = 0;
/// Number of SQE slots in the io_uring ring.
const RING_SIZE: usize = 256;
/// Maximum write SQEs per chunk (all slots for writes; fsync done via fdatasync).
const MAX_WRITES_PER_CHUNK: usize = RING_SIZE;
/// Preallocation block size (1 MiB).
const PREALLOC_BLOCK: u64 = 1 << 20;
/// Briefly spin before a solo leader drains the WAL queue so peer writers can
/// join the same fdatasync.
const GROUP_COMMIT_SOLO_SPINS: usize = 4;
/// Only delay solo leaders for larger batches where one extra peer can amortize
/// a meaningful fdatasync cost.
const GROUP_COMMIT_MIN_SOLO_BYTES: usize = 512 * 1024;

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

    fn write_u8_at(&mut self, pos: usize, value: u8) {
        debug_assert!(pos < self.cap);
        unsafe { *self.ptr.add(pos) = value };
    }

    fn write_u16_be_at(&mut self, pos: usize, value: u16) {
        self.write_at(pos, &value.to_be_bytes());
    }

    fn write_u32_be_at(&mut self, pos: usize, value: u32) {
        self.write_at(pos, &value.to_be_bytes());
    }

    fn write_u64_be_at(&mut self, pos: usize, value: u64) {
        self.write_at(pos, &value.to_be_bytes());
    }

    fn write_at(&mut self, pos: usize, bytes: &[u8]) {
        debug_assert!(pos + bytes.len() <= self.cap);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(pos), bytes.len());
        }
    }

    fn zero_range(&mut self, start: usize, end: usize) {
        debug_assert!(start <= end);
        debug_assert!(end <= self.cap);
        unsafe {
            std::ptr::write_bytes(self.ptr.add(start), 0, end - start);
        }
    }

    fn initialized_slice(&self, start: usize, end: usize) -> &[u8] {
        debug_assert!(start <= end);
        debug_assert!(end <= self.cap);
        unsafe { std::slice::from_raw_parts(self.ptr.add(start), end - start) }
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

/// A pending buffer with its assigned ticket for the ticket-based group commit.
struct TicketedBuf {
    ticket: u64,
    buf: DirectBuf,
}

trait WalPointEntry {
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

impl WalPointEntry for (&[u8], &[u8]) {
    fn key(&self) -> &[u8] {
        self.0
    }

    fn value(&self) -> &[u8] {
        self.1
    }
}

impl WalPointEntry for (KeySlice<'_>, &[u8]) {
    fn key(&self) -> &[u8] {
        self.0.raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.1
    }
}

/// Shared completion state for the ticket-based group commit barrier.
///
/// Multiple threads call `submit_and_commit()` concurrently. Each thread
/// receives a monotonic ticket from `put_batch`. Only one thread (the leader)
/// drains `pending` and submits SQEs. Followers wait on the condvar until
/// `durable_ticket >= my_ticket`, then return immediately — no CAS loop.
struct CompletionState {
    mutex: parking_lot::Mutex<CompletionInner>,
    cond: Condvar,
    durable_ticket: AtomicU64,
}

struct CompletionInner {
    /// Error from the most recent leader I/O, if any.
    /// Propagated to all followers whose tickets are covered by the failed batch.
    last_error: Option<Arc<anyhow::Error>>,
}

#[derive(Debug, Default)]
struct WaitForTicketStats {
    condvar_waits: u64,
}

impl CompletionState {
    fn new() -> Self {
        Self {
            mutex: parking_lot::Mutex::new(CompletionInner { last_error: None }),
            cond: Condvar::new(),
            durable_ticket: AtomicU64::new(0),
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
    pending: Mutex<Vec<TicketedBuf>>,
    /// Monotonic ticket counter for the ticket-based group commit.
    /// Each `put_batch` call gets a unique ticket via `fetch_add(1)`.
    next_ticket: AtomicU64,
    /// Atomic file offset allocator.
    alloc_offset: AtomicU64,
    /// Preallocated file size (tracked in memory).
    preallocated_size: AtomicU64,
    /// Shared completion state for the fate-sharing barrier.
    completion_state: CompletionState,
    /// Atomic leader-election flag for submit_and_commit. Replaces a mutex so
    /// threads that are not the leader never block on a lock — they either
    /// become the leader (CAS wins) or wait on the completion condvar.
    submitting: AtomicBool,
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
            // Pad the file to a 4KB boundary if needed. Pre-v4 WALs may have
            // non-aligned file lengths; O_DIRECT requires page-aligned offsets.
            let aligned_len = DirectBuf::align_up(file_len as usize) as u64;
            if aligned_len > file_len {
                let buf_file_pad = File::options().write(true).open(path)?;
                buf_file_pad.set_len(aligned_len)?;
                buf_file_pad.sync_all()?;
                drop(buf_file_pad);
            }

            let (ring, direct_file, alloc_offset) = Self::try_init_io_uring(path)?;
            Ok(Self {
                buffered_file: Arc::new(Mutex::new(BufWriter::new(buf_file))),
                mvcc_format,
                is_v3,
                direct_file: Some(direct_file),
                ring: Some(Mutex::new(ring)),
                direct_buf_pool: Self::new_direct_buf_pool(),
                pending: Mutex::new(Vec::new()),
                next_ticket: AtomicU64::new(0),
                alloc_offset: AtomicU64::new(alloc_offset),
                preallocated_size: AtomicU64::new(alloc_offset),
                completion_state: CompletionState::new(),
                submitting: AtomicBool::new(false),
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
                next_ticket: AtomicU64::new(0),
                alloc_offset: AtomicU64::new(0),
                preallocated_size: AtomicU64::new(0),
                completion_state: CompletionState::new(),
                submitting: AtomicBool::new(false),
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
            let ret = unsafe {
                libc::fallocate(fd, 0, file_size as libc::off_t, alloc_len as libc::off_t)
            };
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
    fn encode_and_push_direct_buf<T: WalPointEntry>(
        &self,
        data: &[T],
        validated: &[(u16, u16)],
        entries_size: usize,
        commit_ts: u64,
        entry_count: u32,
        profile: Option<&crate::mem_table::WriteProfile>,
    ) -> Result<u64> {
        #[cfg(not(feature = "bench"))]
        let _ = profile;

        // Fail fast after I/O error — don't allocate or enqueue into `pending`.
        if self.poisoned.load(Ordering::Acquire) {
            anyhow::bail!("WAL is poisoned due to a previous I/O error");
        }

        #[cfg(feature = "bench")]
        let prepare_start = Instant::now();

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

        let total_size = entries_size
            .checked_add(V4_BATCH_HEADER_SIZE)
            .context("batch size overflow")?;
        anyhow::ensure!(
            total_size as u64 <= MAX_WAL_FILE_SIZE,
            "batch total_size exceeds maximum WAL file size"
        );
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
        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            profile.record_wal_prepare_ns(prepare_start.elapsed().as_nanos() as u64);
        }

        #[cfg(feature = "bench")]
        let encode_start = Instant::now();
        // Reserve header space (filled later).
        let mut pos = V4_BATCH_HEADER_SIZE;

        for (entry, (kl, vl)) in data.iter().zip(validated.iter()) {
            let key = entry.key();
            let value = entry.value();
            if self.is_v3 {
                buf.write_u8_at(pos, WalEntryKind::Put as u8);
                pos += 1;
            }
            buf.write_u16_be_at(pos, *kl);
            pos += 2;
            buf.write_at(pos, key);
            pos += key.len();
            buf.write_u16_be_at(pos, *vl);
            pos += 2;
            buf.write_at(pos, value);
            pos += value.len();
        }

        let crc = crc32fast::hash(buf.initialized_slice(V4_BATCH_HEADER_SIZE, pos));
        buf.write_u64_be_at(0, commit_ts);
        buf.write_u32_be_at(8, entry_count);
        buf.write_u32_be_at(12, crc);
        buf.write_u32_be_at(16, entries_size as u32);

        // Zero-pad to 4KB alignment (O_DIRECT requirement).
        let aligned_len = DirectBuf::align_up(pos);
        buf.zero_range(pos, aligned_len);
        buf.set_len(aligned_len);
        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            profile.record_wal_encode_ns(encode_start.elapsed().as_nanos() as u64);
        }

        #[cfg(feature = "bench")]
        let enqueue_start = Instant::now();
        let mut pending = self.pending.lock();
        let ticket = self.next_ticket.fetch_add(1, Ordering::Release);
        pending.push(TicketedBuf { ticket, buf });
        drop(pending);
        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            profile.record_wal_enqueue_ns(enqueue_start.elapsed().as_nanos() as u64);
        }

        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("wal.after_batch_encode");
        }

        Ok(ticket)
    }

    /// Encode a range-tombstone batch into a DirectBuf with v4 header, zero-pad
    /// to 4KB alignment, and push to the `pending` queue.
    fn encode_and_push_direct_buf_range(
        &self,
        tombstones: &[(&[u8], &[u8])],
        commit_ts: u64,
        entry_count: u32,
    ) -> Result<u64> {
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

        let total_size = entries_size
            .checked_add(V4_BATCH_HEADER_SIZE)
            .context("batch size overflow")?;
        anyhow::ensure!(
            total_size as u64 <= MAX_WAL_FILE_SIZE,
            "batch total_size exceeds maximum WAL file size"
        );
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

        let mut pos = V4_BATCH_HEADER_SIZE;

        for (start, end) in tombstones {
            buf.write_u8_at(pos, WalEntryKind::RangeTombstone as u8);
            pos += 1;
            buf.write_u16_be_at(pos, start.len() as u16);
            pos += 2;
            buf.write_at(pos, start);
            pos += start.len();
            buf.write_u16_be_at(pos, end.len() as u16);
            pos += 2;
            buf.write_at(pos, end);
            pos += end.len();
        }

        let crc = crc32fast::hash(buf.initialized_slice(V4_BATCH_HEADER_SIZE, pos));
        buf.write_u64_be_at(0, commit_ts);
        buf.write_u32_be_at(8, entry_count);
        buf.write_u32_be_at(12, crc);
        buf.write_u32_be_at(16, entries_size as u32);

        let aligned_len = DirectBuf::align_up(pos);
        buf.zero_range(pos, aligned_len);
        buf.set_len(aligned_len);

        let mut pending = self.pending.lock();
        let ticket = self.next_ticket.fetch_add(1, Ordering::Release);
        pending.push(TicketedBuf { ticket, buf });
        drop(pending);

        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("wal.after_batch_encode");
        }

        Ok(ticket)
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
        w.get_ref().sync_all()?;
        drop(w); // close buffered handle before opening O_DIRECT handle

        // Initialize io_uring + O_DIRECT AFTER header is flushed so alloc_offset is correct.
        // If this fails (e.g. old kernel, filesystem rejects O_DIRECT), clean up
        // the WAL file so retries don't fail on create_new.
        let (ring, direct_file, alloc_offset) = Self::try_init_io_uring(path.as_ref())
            .inspect_err(|_| {
                let _ = std::fs::remove_file(path.as_ref());
            })?;

        // Re-open buffered handle for recovery reads and legacy put().
        let buf_file = File::options()
            .read(true)
            .append(true)
            .open(path.as_ref())
            .inspect_err(|_| {
                let _ = std::fs::remove_file(path.as_ref());
            })?;

        Ok(Self {
            buffered_file: Arc::new(Mutex::new(BufWriter::new(buf_file))),
            mvcc_format: true,
            is_v3: true,
            direct_file: Some(direct_file),
            ring: Some(Mutex::new(ring)),
            direct_buf_pool: Self::new_direct_buf_pool(),
            pending: Mutex::new(Vec::new()),
            next_ticket: AtomicU64::new(0),
            alloc_offset: AtomicU64::new(alloc_offset),
            preallocated_size: AtomicU64::new(alloc_offset),
            completion_state: CompletionState::new(),
            submitting: AtomicBool::new(false),
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
        let batch_hdr_size = Self::wal_batch_header_size(is_v4);

        while data.remaining() >= batch_hdr_size {
            let before_batch = data.clone();
            let commit_ts = data.get_u64();
            let entry_count = data.get_u32() as usize;
            let data_crc32 = data.get_u32();
            // v4: read data_len for alignment-gap skipping.
            let data_len_field = if is_v4 { data.get_u32() as usize } else { 0 };
            handler.reset_range_ordinals();

            if Self::is_zero_v4_batch(is_v4, commit_ts, entry_count, data_crc32, data_len_field) {
                data = before_batch;
                break;
            }

            let entries_start = data.remaining();
            let Some(expected_size) =
                Self::validate_mvcc_batch_entries(&data, entry_count, entries_start, is_v3)
            else {
                data = before_batch;
                break;
            };

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

            Self::replay_mvcc_batch_entries(
                &mut data,
                expected_size,
                entry_count,
                is_v3,
                commit_ts,
                handler,
            )?;
            if commit_ts > max_ts {
                max_ts = commit_ts;
            }

            Self::skip_v4_batch_alignment_gap(
                &mut data,
                is_v4,
                batch_hdr_size,
                data_len_field,
                expected_size,
            );
        }

        Self::truncate_recovered_wal_file(&f, is_v4, data_len, data.remaining(), file_len)?;

        Ok((f, max_ts))
    }

    fn wal_batch_header_size(is_v4: bool) -> usize {
        if is_v4 {
            V4_BATCH_HEADER_SIZE
        } else {
            BATCH_HEADER_SIZE
        }
    }

    fn is_zero_v4_batch(
        is_v4: bool,
        commit_ts: u64,
        entry_count: usize,
        data_crc32: u32,
        data_len_field: usize,
    ) -> bool {
        // Reject all-zero pages (preallocated but unwritten). Without this
        // check, fallocate'd zeros pass CRC(empty) and recovery treats the
        // page as a valid empty batch, advancing into unwritten space.
        is_v4 && commit_ts == 0 && entry_count == 0 && data_crc32 == 0 && data_len_field == 0
    }

    fn validate_mvcc_batch_entries(
        data: &Bytes,
        entry_count: usize,
        entries_start: usize,
        is_v3: bool,
    ) -> Option<usize> {
        let min_entry_size = if is_v3 { 3 } else { 4 };
        if entry_count > entries_start / min_entry_size {
            return None;
        }

        let mut expected_size = 0usize;
        for _ in 0..entry_count {
            expected_size = if is_v3 {
                Self::validate_v3_entry_size(data, entries_start, expected_size)?
            } else {
                Self::validate_v2_entry_size(data, entries_start, expected_size)?
            };

            if expected_size > entries_start {
                return None;
            }
        }

        Some(expected_size)
    }

    fn validate_v3_entry_size(
        data: &Bytes,
        entries_start: usize,
        expected_size: usize,
    ) -> Option<usize> {
        if entries_start.checked_sub(expected_size)? < 1 {
            return None;
        }

        let kind = data[expected_size];
        let after_kind = expected_size + 1;
        match kind {
            0 => Self::validate_put_entry_size(data, entries_start, after_kind),
            1 => Self::validate_point_tombstone_entry_size(data, entries_start, after_kind),
            2 => Self::validate_range_tombstone_entry_size(data, entries_start, after_kind),
            _ => None,
        }
    }

    fn validate_v2_entry_size(
        data: &Bytes,
        entries_start: usize,
        expected_size: usize,
    ) -> Option<usize> {
        Self::validate_put_entry_size(data, entries_start, expected_size)
    }

    fn validate_put_entry_size(data: &Bytes, entries_start: usize, pos: usize) -> Option<usize> {
        if entries_start.checked_sub(pos)? < 4 {
            return None;
        }
        let key_size = (&data[pos..pos + 2]).get_u16() as usize;
        if entries_start.checked_sub(pos)? < 4 + key_size {
            return None;
        }
        let val_size = (&data[pos + 2 + key_size..pos + 4 + key_size]).get_u16() as usize;
        if entries_start.checked_sub(pos)? < 4 + key_size + val_size {
            return None;
        }

        Some(pos + 4 + key_size + val_size)
    }

    fn validate_point_tombstone_entry_size(
        data: &Bytes,
        entries_start: usize,
        pos: usize,
    ) -> Option<usize> {
        if entries_start.checked_sub(pos)? < 2 {
            return None;
        }
        let key_size = (&data[pos..pos + 2]).get_u16() as usize;
        if entries_start.checked_sub(pos)? < 2 + key_size {
            return None;
        }

        Some(pos + 2 + key_size)
    }

    fn validate_range_tombstone_entry_size(
        data: &Bytes,
        entries_start: usize,
        pos: usize,
    ) -> Option<usize> {
        if entries_start.checked_sub(pos)? < 4 {
            return None;
        }
        let start_size = (&data[pos..pos + 2]).get_u16() as usize;
        if entries_start.checked_sub(pos)? < 4 + start_size {
            return None;
        }
        let end_size = (&data[pos + 2 + start_size..pos + 4 + start_size]).get_u16() as usize;
        if entries_start.checked_sub(pos)? < 4 + start_size + end_size {
            return None;
        }

        Some(pos + 4 + start_size + end_size)
    }

    fn replay_mvcc_batch_entries<H: RecoveryHandler>(
        data: &mut Bytes,
        expected_size: usize,
        entry_count: usize,
        is_v3: bool,
        commit_ts: u64,
        handler: &mut H,
    ) -> Result<()> {
        let mut entry_buf = data.split_to(expected_size);
        for _ in 0..entry_count {
            if is_v3 {
                Self::replay_v3_entry(&mut entry_buf, commit_ts, handler)?;
            } else {
                Self::replay_v2_entry(&mut entry_buf, handler)?;
            }
        }

        Ok(())
    }

    fn replay_v3_entry<H: RecoveryHandler>(
        entry_buf: &mut Bytes,
        commit_ts: u64,
        handler: &mut H,
    ) -> Result<()> {
        let kind = entry_buf.get_u8();
        match kind {
            0 => Self::replay_put_entry(entry_buf, handler),
            1 => Self::replay_point_tombstone_entry(entry_buf, handler),
            2 => Self::replay_range_tombstone_entry(entry_buf, commit_ts, handler),
            _ => anyhow::bail!("unknown WAL v3 entry kind: {}", kind),
        }
    }

    fn replay_v2_entry<H: RecoveryHandler>(entry_buf: &mut Bytes, handler: &mut H) -> Result<()> {
        Self::replay_put_entry(entry_buf, handler)
    }

    fn replay_put_entry<H: RecoveryHandler>(entry_buf: &mut Bytes, handler: &mut H) -> Result<()> {
        let key_size = entry_buf.get_u16() as usize;
        let key = entry_buf.split_to(key_size);
        let value_size = entry_buf.get_u16() as usize;
        let value = entry_buf.split_to(value_size);
        handler.handle_put(key, value)
    }

    fn replay_point_tombstone_entry<H: RecoveryHandler>(
        entry_buf: &mut Bytes,
        handler: &mut H,
    ) -> Result<()> {
        let key_size = entry_buf.get_u16() as usize;
        let key = entry_buf.split_to(key_size);
        handler.handle_point_tombstone(key)
    }

    fn replay_range_tombstone_entry<H: RecoveryHandler>(
        entry_buf: &mut Bytes,
        commit_ts: u64,
        handler: &mut H,
    ) -> Result<()> {
        let start_size = entry_buf.get_u16() as usize;
        let start = entry_buf.split_to(start_size);
        let end_size = entry_buf.get_u16() as usize;
        let end = entry_buf.split_to(end_size);
        handler.handle_range_tombstone(start, end, commit_ts)
    }

    fn skip_v4_batch_alignment_gap(
        data: &mut Bytes,
        is_v4: bool,
        batch_hdr_size: usize,
        data_len_field: usize,
        expected_size: usize,
    ) {
        if !is_v4 {
            return;
        }

        let total_batch_size = batch_hdr_size + data_len_field;
        let aligned_size = DirectBuf::align_up(total_batch_size);
        let consumed = expected_size + batch_hdr_size;
        if aligned_size > consumed {
            let skip = aligned_size - consumed;
            data.advance(data.remaining().min(skip));
        }
    }

    fn truncate_recovered_wal_file(
        f: &File,
        is_v4: bool,
        data_len: usize,
        remaining_data: usize,
        file_len: u64,
    ) -> Result<()> {
        let scan_start = if is_v4 {
            DirectBuf::align_up(WAL_HEADER_SIZE)
        } else {
            WAL_HEADER_SIZE
        };
        let valid_data = data_len - remaining_data;
        let valid_file = scan_start + valid_data;
        if (valid_file as u64) < file_len {
            f.set_len(valid_file as u64)?;
            f.sync_all()?;
        }

        Ok(())
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
            // Reject key_size=0 — a v4 WAL with corrupted magic falls back to
            // legacy recovery, but its 4KB zero-padded header produces millions
            // of key_size=0 entries, causing OOM. Empty keys are invalid in
            // legacy WALs.
            if key_size == 0 {
                data = before_entry;
                break;
            }
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
                f.sync_all()?;
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
                f.sync_all()?;
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
            self.put_batch(&[(key, value)], 0)?;
            return Ok(());
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
    pub fn put_batch(&self, data: &[(&[u8], &[u8])], commit_ts: u64) -> Result<u64> {
        self.put_batch_entries(data, commit_ts)
    }

    #[cfg(not(feature = "bench"))]
    pub(crate) fn put_key_batch(
        &self,
        data: &[(KeySlice<'_>, &[u8])],
        commit_ts: u64,
    ) -> Result<u64> {
        self.put_batch_entries_inner(data, commit_ts, None)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn put_key_batch_profiled(
        &self,
        data: &[(KeySlice<'_>, &[u8])],
        commit_ts: u64,
        profile: &crate::mem_table::WriteProfile,
    ) -> Result<u64> {
        self.put_batch_entries_inner(data, commit_ts, Some(profile))
    }

    fn put_batch_entries<T: WalPointEntry>(&self, data: &[T], commit_ts: u64) -> Result<u64> {
        self.put_batch_entries_inner(data, commit_ts, None)
    }

    fn put_batch_entries_inner<T: WalPointEntry>(
        &self,
        data: &[T],
        commit_ts: u64,
        profile: Option<&crate::mem_table::WriteProfile>,
    ) -> Result<u64> {
        for entry in data {
            let key = entry.key();
            let value = entry.value();
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
                .try_fold(0usize, |acc, entry| {
                    acc.checked_add(4 + entry.key().len() + entry.value().len())
                })
                .context("legacy batch size overflow")?;
            let mut buf = Vec::with_capacity(capacity);
            for entry in data {
                let key = entry.key();
                let value = entry.value();
                buf.put_u16(u16::try_from(key.len()).context("key length exceeds u16::MAX")?);
                buf.put(key);
                buf.put_u16(u16::try_from(value.len()).context("value length exceeds u16::MAX")?);
                buf.put(value);
            }
            file.write_all(&buf).context("failed to write to WAL")?;
            return Ok(0);
        }

        // MVCC format: encode into a DirectBuf with v4 header, push to pending.
        #[cfg(feature = "bench")]
        let validate_start = Instant::now();
        let per_entry_overhead = if self.is_v3 { 5 } else { 4 };
        let mut validated = Vec::with_capacity(data.len());
        let mut entries_size = 0usize;
        for entry in data {
            let key = entry.key();
            let value = entry.value();
            let key_len = u16::try_from(key.len()).context("key length exceeds u16::MAX")?;
            let value_len = u16::try_from(value.len()).context("value length exceeds u16::MAX")?;
            entries_size = entries_size
                .checked_add(per_entry_overhead + key.len() + value.len())
                .context("batch size overflow")?;
            validated.push((key_len, value_len));
        }
        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            profile.record_wal_validate_ns(validate_start.elapsed().as_nanos() as u64);
        }

        self.encode_and_push_direct_buf(
            data,
            &validated,
            entries_size,
            commit_ts,
            entry_count,
            profile,
        )
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
    ) -> Result<u64> {
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
    pub fn sync(&self) -> Result<()> {
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
        let ticket = self.next_ticket.load(Ordering::Acquire);
        if ticket == 0 {
            return Ok(());
        }
        self.submit_and_commit(ticket - 1)
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

        let ticket = self.next_ticket.load(Ordering::Acquire);
        if ticket > 0 {
            self.submit_and_commit(ticket - 1)?;
        }

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

    /// Ticket-based group-commit barrier.
    ///
    /// Each `put_batch` assigns a monotonic ticket. One thread (the leader)
    /// drains all pending buffers and does I/O. Followers wait on the condvar
    /// until `durable_ticket > ticket`. If a follower wakes and its ticket
    /// is still not durable (late arrival after leader drained), it re-enters
    /// the CAS loop to become the leader itself.
    pub fn submit_and_commit(&self, ticket: u64) -> Result<()> {
        self.submit_and_commit_inner(ticket, None)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn submit_and_commit_profiled(
        &self,
        ticket: u64,
        profile: &crate::mem_table::WriteProfile,
    ) -> Result<()> {
        self.submit_and_commit_inner(ticket, Some(profile))
    }

    fn submit_and_commit_inner(
        &self,
        ticket: u64,
        profile: Option<&crate::mem_table::WriteProfile>,
    ) -> Result<()> {
        if !self.mvcc_format {
            return self.flush_legacy_wal();
        }

        // Fast-path: ticket already durable — avoid unnecessary atomic loads
        // and the CAS loop entirely.
        if self.completion_state.durable_ticket.load(Ordering::Acquire) > ticket {
            return Ok(());
        }

        let next_ticket = self.next_ticket.load(Ordering::Acquire);
        if next_ticket == 0 {
            return Ok(());
        }
        anyhow::ensure!(
            ticket < next_ticket,
            "submit_and_commit called with unassigned ticket {ticket} (next_ticket={next_ticket})"
        );

        loop {
            if self.completion_state.durable_ticket.load(Ordering::Acquire) > ticket {
                return Ok(());
            }

            // Check poisoned flag — fail fast after I/O error.
            if self.poisoned.load(Ordering::Acquire) {
                anyhow::bail!("WAL is poisoned due to a previous I/O error");
            }

            // Try to become the leader via CAS.
            let is_leader = self
                .submitting
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();

            if is_leader {
                self.submit_as_leader(ticket, profile)?;
            } else {
                // Follower path: wait until the leader commits our ticket.
                #[cfg(feature = "bench")]
                let wait_start = Instant::now();
                let wait_stats = self.wait_for_ticket_durability(ticket)?;
                #[cfg(feature = "bench")]
                if let Some(profile) = profile {
                    profile.record_wal_follower_wait_ns(wait_start.elapsed().as_nanos() as u64);
                    let retried_leadership =
                        self.completion_state.durable_ticket.load(Ordering::Acquire) <= ticket;
                    profile.record_wal_follower_wait(wait_stats.condvar_waits, retried_leadership);
                }
                #[cfg(not(feature = "bench"))]
                let _ = wait_stats;
                // Our ticket is durable — return success. Don't check
                // last_error: a subsequent leader's failure should not
                // affect data that was already committed.
            }

            // Re-check: our ticket may have arrived after the leader drained.
            // If durable_ticket still doesn't cover us, loop and try to become
            // the leader ourselves.
            if self.completion_state.durable_ticket.load(Ordering::Acquire) > ticket {
                return Ok(());
            }
        }
    }

    fn flush_legacy_wal(&self) -> Result<()> {
        let mut file = self.buffered_file.lock();
        file.flush().context("failed to flush WAL")?;
        file.get_ref()
            .sync_all()
            .context("failed to sync WAL to disk")?;

        Ok(())
    }

    fn submit_as_leader(
        &self,
        ticket: u64,
        profile: Option<&crate::mem_table::WriteProfile>,
    ) -> Result<()> {
        self.wait_for_group_commit_peers(ticket);
        let ticketed_bufs = self.drain_pending_ticketed_bufs();
        if ticketed_bufs.is_empty() {
            return self.fail_empty_leader_drain(ticket);
        }

        let max_ticket = ticketed_bufs.last().unwrap().ticket;
        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            let buffers = ticketed_bufs.len() as u64;
            let bytes = ticketed_bufs
                .iter()
                .map(|buf| DirectBuf::align_up(buf.buf.len()) as u64)
                .sum();
            profile.record_wal_commit_group(buffers, bytes);
        }
        let result = self.submit_sqes_and_poll(ticketed_bufs, profile);

        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("wal.after_fsync_before_publish");
        }

        self.publish_submit_result(max_ticket, &result);
        result
    }

    fn wait_for_group_commit_peers(&self, ticket: u64) {
        {
            let pending = self.pending.lock();
            if pending.len() != 1 {
                return;
            }
            let buf = pending.last().unwrap();
            if buf.ticket != ticket || buf.buf.len() < GROUP_COMMIT_MIN_SOLO_BYTES {
                return;
            }
        }

        for _ in 0..GROUP_COMMIT_SOLO_SPINS {
            if self.completion_state.durable_ticket.load(Ordering::Acquire) > ticket
                || self.next_ticket.load(Ordering::Acquire) > ticket + 1
            {
                break;
            }
            std::hint::spin_loop();
        }
    }

    fn drain_pending_ticketed_bufs(&self) -> Vec<TicketedBuf> {
        let mut pending = self.pending.lock();

        std::mem::take(&mut *pending)
    }

    fn fail_empty_leader_drain(&self, ticket: u64) -> Result<()> {
        // A leader with no pending buffers while our ticket is still not
        // durable indicates the caller supplied an uncovered ticket or state
        // became inconsistent. Surface it instead of spinning on an empty
        // drain loop.
        let err_msg =
            format!("invariant violation: no pending WAL buffers for undurable ticket {ticket}");
        let mut state = self.completion_state.mutex.lock();
        state.last_error = Some(Arc::new(anyhow::anyhow!("{err_msg}")));
        self.submitting.store(false, Ordering::Release);
        self.completion_state.cond.notify_all();

        Err(anyhow::anyhow!("{err_msg}"))
    }

    fn publish_submit_result(&self, max_ticket: u64, result: &Result<()>) {
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }

        let mut state = self.completion_state.mutex.lock();
        if let Err(e) = result {
            state.last_error = Some(Arc::new(anyhow::anyhow!("{e}")));
        } else {
            state.last_error = None;
            self.completion_state
                .durable_ticket
                .fetch_max(max_ticket + 1, Ordering::Release);
        }
        self.submitting.store(false, Ordering::Release);
        self.completion_state.cond.notify_all();
    }

    fn wait_for_ticket_durability(&self, ticket: u64) -> Result<WaitForTicketStats> {
        let mut state = self.completion_state.mutex.lock();
        let mut stats = WaitForTicketStats::default();
        while self.completion_state.durable_ticket.load(Ordering::Acquire) <= ticket {
            if let Some(ref e) = state.last_error {
                return Err(anyhow::anyhow!("{e}"));
            }
            if !self.submitting.load(Ordering::Acquire) {
                break;
            }
            stats.condvar_waits += 1;
            self.completion_state.cond.wait(&mut state);
        }

        Ok(stats)
    }

    /// Submit DirectBuf buffers as io_uring SQEs, poll CQEs for completion.
    ///
    /// Handles chunked submission when the batch exceeds ring capacity (64 SQEs).
    /// The entire drained set is one logical commit group.
    fn submit_sqes_and_poll(
        &self,
        bufs: Vec<TicketedBuf>,
        profile: Option<&crate::mem_table::WriteProfile>,
    ) -> Result<()> {
        #[cfg(not(feature = "bench"))]
        let _ = profile;

        // SAFETY: only called for MVCC WALs which always have a ring.
        let ring_ref = self.ring.as_ref().unwrap();

        // Compute total aligned size first, then preallocate to cover the full batch.
        let total_size: u64 = bufs
            .iter()
            .map(|b| DirectBuf::align_up(b.buf.len()) as u64)
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

        #[cfg(feature = "bench")]
        let submit_start = Instant::now();

        for &(chunk_start, chunk_end) in &chunk_ranges {
            let chunk_len = chunk_end - chunk_start;

            // Submit write SQEs, wait for completion, and poll CQEs under a
            // single lock hold. This prevents close() from draining CQEs in
            // the gap between submit and poll.
            let mut write_err: Option<i32> = None;
            let chunk_start_idx = global_idx;
            {
                let mut ring = ring_ref.lock();
                for i in 0..chunk_len {
                    let buf = &bufs[chunk_start + i];
                    let aligned_len = DirectBuf::align_up(buf.buf.len());
                    let sqe = io_uring::opcode::Write::new(
                        io_uring::types::Fixed(WAL_FD_INDEX),
                        buf.buf.as_ptr(),
                        aligned_len as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(global_idx + i as u64);
                    // SAFETY: the SQE is fully initialized by the builder above.
                    unsafe {
                        ring.submission().push(&sqe)?;
                    }
                    offset += aligned_len as u64;
                }

                // Submit all write SQEs in one syscall and wait for completions.
                // Retry on EINTR to prevent spurious failures from signals
                // (profilers, thread suspension, etc.), matching fdatasync below.

                #[cfg(feature = "chaos-testing")]
                {
                    crate::chaos::failpoint::fail_point!("wal.after_submit_before_wait");
                }

                loop {
                    match ring.submit_and_wait(chunk_len) {
                        Ok(_) => break,
                        Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            let cq = ring.completion();
                            for _cqe in cq {}
                            anyhow::bail!("io_uring submit_and_wait failed: {}", e);
                        }
                    }
                }

                // Poll CQEs — lock is still held, so close() cannot interfere.
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
                                    DirectBuf::align_up(bufs[chunk_start + idx as usize].buf.len());
                                if written < expected && write_err.is_none() {
                                    write_err = Some(-libc::EIO);
                                }
                            }
                        }
                        _ => {
                            log::error!("io_uring: stale CQE with user_data={}", user_data);
                            anyhow::bail!("io_uring: stale CQE with user_data={}", user_data);
                        }
                    }
                }
                // ring and cq drop here
            }

            if let Some(err) = write_err {
                // All CQEs reaped — kernel is done with buffers, safe to drop.
                drop(std::mem::ManuallyDrop::into_inner(bufs));
                anyhow::bail!("io_uring write error: {}", err);
            }

            global_idx += chunk_len as u64;
        }

        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            profile.record_wal_submit_ns(submit_start.elapsed().as_nanos() as u64);
        }

        // Single fdatasync after all chunks are written. Uses fdatasync(2)
        // directly instead of IORING_OP_FSYNC — lower per-call overhead since
        // it avoids the io_uring SQE/CQE round-trip for the fsync operation.
        // All write SQEs have been submitted and completed at this point.
        // Retry on EINTR — signal handlers/profilers can interrupt the syscall.
        {
            let direct_file = self.direct_file.as_ref().unwrap();
            let fd = direct_file.as_raw_fd();
            let mut fdatasync_err = None;
            #[cfg(feature = "bench")]
            let fdatasync_start = Instant::now();
            loop {
                let ret = unsafe { libc::fdatasync(fd) };
                if ret == 0 {
                    break;
                }
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                fdatasync_err = Some(err);
                break;
            }
            #[cfg(feature = "bench")]
            if let Some(profile) = profile {
                profile.record_wal_fdatasync_ns(fdatasync_start.elapsed().as_nanos() as u64);
            }
            if let Some(err) = fdatasync_err {
                // All CQEs reaped — kernel is done with buffers, safe to drop.
                drop(std::mem::ManuallyDrop::into_inner(bufs));
                anyhow::bail!("fdatasync failed: {}", err);
            }
        }

        // Return buffers to pool on success. into_inner is safe here because
        // we only reach this path on success (no SQEs in flight).
        // Cap: don't return oversized buffers from bulk loads to the pool.
        let bufs = std::mem::ManuallyDrop::into_inner(bufs);
        for ticketed_buf in bufs {
            let buf = ticketed_buf.buf;
            if buf.cap() <= BUFFER_POOL_BUF_SIZE * 2 {
                let _ = self.direct_buf_pool.push(buf);
            }
        }

        Ok(())
    }
}
