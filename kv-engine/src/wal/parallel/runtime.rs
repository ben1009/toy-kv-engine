//! Runtime admission, ordered packing, and durability coordination for parallel WALs.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, MetadataExt},
    },
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    pitr::{CanonicalWalEntryStream, LIVE_WAL_V5_LIMITS, WalBatch, WalV5Limits},
    wal::v7::{
        admission::{WalV7BatchBufferPin, WalV7OwnedBatchReservation, WalV7ReservationOwner},
        allocator::{
            WalV7DataAllocation, WalV7DataCapacityStatus, WalV7FrontierCursor,
            WalV7FrontierReservation, WalV7PhysicalAllocator,
        },
        codec::{
            PreparedWalV7LogicalBatch, WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7Frontier,
            WalV7Header, data_fragment_count, encode_data_frames_into,
            encode_frontier_successor_into, frame_record_digest, prepare_logical_batch,
        },
        install::{WalV7RuntimeMetadata, WalV7RuntimeSeed},
        leader::WalV7LeaderCapacityError,
    },
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Condvar, Mutex, MutexGuard};
use sha2::{Digest, Sha256};

use super::{
    BUFFER_POOL_BUF_SIZE, BUFFER_POOL_CAPACITY, DirectBuf, MAX_WAL_FILE_SIZE, PREALLOC_BLOCK,
    parallel_worker::{
        GroupWriteResult, IoWorker, IoWorkerClient, WalSyncProgress, WorkerBuffer, WriteBuffer,
        WriteGroup, WriteGroupBuffers,
    },
};

const NORMAL_ACTIVE_BUFFER_BUDGET: u64 = 64 * 1024 * 1024;
const MAX_BUFFER_CAPACITY: u64 = 240 * 1024 * 1024;
const WAL_HEADER_END: u64 = 4096;
const PACKER_GROUP_MAX_TICKETS: usize = 8;
const EXTENT_INITIALIZATION_CHUNK: usize = 128 * 1024;
// Large writes amortize extent conversion themselves. Zero-filling their
// allocated space adds a second write of the same bytes on ext4.
const ALLOCATION_ONLY_MIN_BATCH_BYTES: usize = 64 * 1024;
// Coalesce already-admitted tickets with one fixed deadline. Refresh the
// batching cutoff only when the written prefix catches it; a refresh cannot
// extend the deadline. Skip the wait when the prior sync was cheap.
const SYNC_COALESCE_WAIT: Duration = Duration::from_micros(400);
const SYNC_COALESCE_MIN_SYNC: Duration = Duration::from_micros(100);

#[derive(Debug)]
pub(crate) struct WalFull;

impl std::fmt::Display for WalFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WAL full; rotate and retry")
    }
}

impl std::error::Error for WalFull {}

/// A v7 batch whose canonical entry stream and scratch reservation were
/// prepared before the caller enters the MVCC write sequencer.
pub(crate) struct WalV7PreparedBatch<'a> {
    prepared: PreparedWalV7LogicalBatch<'a>,
    owner: Arc<WalV7ReservationOwner>,
    preparation: Option<crate::wal::v7::admission::WalV7OwnedPreparationReservation>,
    limits: WalV5Limits,
}

/// Coordinates and resource owner from which a parallel runtime begins admission.
pub(crate) struct ParallelWalRuntimeStart {
    append_offset: u64,
    next_ticket: u64,
    max_file_size: u64,
    v7_allocator: Option<WalV7PhysicalAllocator>,
    v7_metadata: Option<WalV7RuntimeMetadata>,
    v7_header_digest: Option<[u8; 32]>,
    expected_v7_inode: Option<(u64, u64)>,
    v7_reservation_owner: Option<Arc<WalV7ReservationOwner>>,
    min_append_offset: u64,
    admission_open: bool,
    initialize_extents: bool,
    prewarm_buffers: bool,
}

impl ParallelWalRuntimeStart {
    pub(crate) fn v4(append_offset: u64) -> Result<Self> {
        let start = Self {
            append_offset,
            next_ticket: 0,
            max_file_size: MAX_WAL_FILE_SIZE,
            v7_allocator: None,
            v7_metadata: None,
            v7_header_digest: None,
            expected_v7_inode: None,
            v7_reservation_owner: None,
            min_append_offset: WAL_HEADER_END,
            admission_open: true,
            initialize_extents: true,
            prewarm_buffers: true,
        };
        start.validate()?;
        Ok(start)
    }

    pub(crate) fn v7(
        seed: WalV7RuntimeSeed,
        max_file_size: u64,
        reservation_owner: Option<Arc<WalV7ReservationOwner>>,
    ) -> Result<Self> {
        let expected_v7_inode = Some((seed.installed_dev, seed.installed_ino));
        if let Some(owner) = &reservation_owner {
            owner.validate_image(seed.installed_dev, seed.installed_ino)?;
        }
        let v7_allocator = WalV7PhysicalAllocator::new(
            seed.next_ticket,
            seed.append_offset,
            max_file_size,
            seed.frontier_cursor,
        )?;
        let admission_open = reservation_owner.is_some();
        let start = Self {
            append_offset: seed.append_offset,
            next_ticket: seed.next_ticket,
            max_file_size,
            v7_allocator: Some(v7_allocator),
            v7_metadata: Some(seed.metadata),
            v7_header_digest: Some(seed.header_digest),
            expected_v7_inode,
            v7_reservation_owner: reservation_owner,
            min_append_offset: seed.data_start,
            admission_open,
            initialize_extents: false,
            prewarm_buffers: false,
        };
        start.validate()?;
        Ok(start)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.max_file_size >= self.min_append_offset && self.max_file_size <= MAX_WAL_FILE_SIZE,
            "invalid WAL runtime physical size limit {}",
            self.max_file_size
        );
        ensure!(
            self.min_append_offset >= WAL_HEADER_END
                && self.min_append_offset.is_multiple_of(4096)
                && (self.min_append_offset..=self.max_file_size).contains(&self.append_offset)
                && self.append_offset.is_multiple_of(4096),
            "invalid initial WAL append offset {}",
            self.append_offset
        );
        match (
            &self.v7_allocator,
            &self.v7_metadata,
            self.v7_header_digest,
            &self.v7_reservation_owner,
        ) {
            (Some(allocator), Some(metadata), Some(_), Some(_)) => {
                metadata
                    .validate_frontier(allocator.last_frontier().frontier, self.append_offset)?;
            }
            (Some(allocator), Some(metadata), Some(_), None) if !self.admission_open => {
                metadata
                    .validate_frontier(allocator.last_frontier().frontier, self.append_offset)?;
            }
            (None, None, None, None) => {}
            _ => bail!("parallel WAL startup has incomplete v7 recovery metadata"),
        }
        Ok(())
    }

    fn validate_v7_file_handles(
        &self,
        worker_file: &File,
        sync_file: &File,
        preallocator: &File,
    ) -> Result<()> {
        let Some(expected) = self.expected_v7_inode else {
            return Ok(());
        };

        for (role, file) in [
            ("worker", worker_file),
            ("sync", sync_file),
            ("preallocator", preallocator),
        ] {
            let metadata = file
                .metadata()
                .with_context(|| format!("inspect v7 WAL {role} handle identity"))?;
            ensure!(
                (metadata.dev(), metadata.ino()) == expected,
                "v7 WAL {role} handle does not refer to the installed Active inode"
            );
            ensure!(
                metadata.len() >= self.append_offset,
                "v7 WAL {role} handle is shorter than the installed append cursor"
            );
            ensure!(
                metadata.len() <= self.max_file_size,
                "v7 WAL {role} handle exceeds the configured maximum physical size"
            );
        }

        let expected_header_digest = self
            .v7_header_digest
            .context("v7 WAL runtime startup has no recovered header digest")?;
        validate_v7_header_digest(worker_file, expected_header_digest)?;

        Ok(())
    }
}

fn read_v7_header_digest(file: &File) -> Result<[u8; 32]> {
    let mut header_buffer = DirectBuf::new(WAL_V7_HEADER_LEN);
    let header_bytes = header_buffer.as_mut_slice();
    let read = loop {
        match file.read_at(header_bytes, 0) {
            Ok(read) if read == WAL_V7_HEADER_LEN => break read,
            Ok(read) => {
                bail!("short v7 WAL immutable header read: {read} of {WAL_V7_HEADER_LEN} bytes");
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error).context("read v7 WAL immutable header"),
        }
    };
    header_buffer.set_len(read);

    let header_bytes = header_buffer.initialized_slice(0, read);
    let header = WalV7Header::decode(header_bytes).context("decode v7 WAL immutable header")?;
    ensure!(
        header.encode()?.as_slice() == header_bytes,
        "v7 WAL immutable header is not canonically encoded"
    );

    Ok(Sha256::digest(header_bytes).into())
}

fn validate_v7_header_digest(file: &File, expected_digest: [u8; 32]) -> Result<()> {
    let actual_digest = read_v7_header_digest(file)?;
    ensure!(
        actual_digest == expected_digest,
        "v7 WAL immutable header digest does not match the recovered runtime seed"
    );
    Ok(())
}

#[derive(Debug)]
struct BufferBudget {
    state: Mutex<BufferBudgetState>,
    available: Condvar,
    async_available: tokio::sync::Notify,
}

#[derive(Debug)]
struct BufferBudgetState {
    active_bytes: u64,
    oversized_active: bool,
    oversized_waiters: usize,
    closed: bool,
}

impl BufferBudget {
    fn new() -> Self {
        Self {
            state: Mutex::new(BufferBudgetState {
                active_bytes: 0,
                oversized_active: false,
                oversized_waiters: 0,
                closed: false,
            }),
            available: Condvar::new(),
            async_available: tokio::sync::Notify::new(),
        }
    }

    fn reserve(&self, bytes: u64) -> Result<()> {
        ensure!(
            bytes <= MAX_BUFFER_CAPACITY,
            "WAL batch exceeds the direct-buffer capacity limit"
        );
        let mut state = self.state.lock();
        let oversized = bytes > NORMAL_ACTIVE_BUFFER_BUDGET;
        if oversized {
            state.oversized_waiters = state
                .oversized_waiters
                .checked_add(1)
                .ok_or_else(|| anyhow!("oversized WAL buffer waiter count overflow"))?;
        }
        loop {
            if state.closed {
                if oversized {
                    state.oversized_waiters -= 1;
                }
                bail!("parallel WAL is closed");
            }
            let fits = if oversized {
                state.active_bytes == 0 && !state.oversized_active
            } else {
                !state.oversized_active
                    && state.oversized_waiters == 0
                    && state
                        .active_bytes
                        .checked_add(bytes)
                        .is_some_and(|active| active <= NORMAL_ACTIVE_BUFFER_BUDGET)
            };
            if fits {
                if oversized {
                    state.oversized_waiters -= 1;
                }
                state.active_bytes = state
                    .active_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| anyhow!("WAL buffer budget overflow"))?;
                state.oversized_active = oversized;
                return Ok(());
            }
            self.available.wait(&mut state);
        }
    }

    async fn reserve_async(&self, bytes: u64) -> Result<()> {
        ensure!(
            bytes <= MAX_BUFFER_CAPACITY,
            "WAL batch exceeds the direct-buffer capacity limit"
        );
        let oversized = bytes > NORMAL_ACTIVE_BUFFER_BUDGET;
        let mut waiter = OversizedBudgetWaiter {
            budget: self,
            registered: false,
        };
        if oversized {
            let mut state = self.state.lock();
            state.oversized_waiters = state
                .oversized_waiters
                .checked_add(1)
                .ok_or_else(|| anyhow!("oversized WAL buffer waiter count overflow"))?;
            waiter.registered = true;
        }
        let notification = self.async_available.notified();
        tokio::pin!(notification);
        loop {
            notification.as_mut().enable();
            {
                let mut state = self.state.lock();
                ensure!(!state.closed, "parallel WAL is closed");
                let fits = if oversized {
                    state.active_bytes == 0 && !state.oversized_active
                } else {
                    !state.oversized_active
                        && state.oversized_waiters == 0
                        && state
                            .active_bytes
                            .checked_add(bytes)
                            .is_some_and(|active| active <= NORMAL_ACTIVE_BUFFER_BUDGET)
                };
                if fits {
                    if oversized {
                        state.oversized_waiters -= 1;
                        waiter.registered = false;
                    }
                    state.active_bytes = state
                        .active_bytes
                        .checked_add(bytes)
                        .ok_or_else(|| anyhow!("WAL buffer budget overflow"))?;
                    state.oversized_active = oversized;
                    return Ok(());
                }
            }
            notification.as_mut().await;
            notification.set(self.async_available.notified());
        }
    }

    fn release(&self, bytes: u64) {
        let mut state = self.state.lock();
        state.active_bytes = state
            .active_bytes
            .checked_sub(bytes)
            .expect("active WAL buffers own their reserved budget");
        if bytes > NORMAL_ACTIVE_BUFFER_BUDGET {
            debug_assert!(state.oversized_active);
            state.oversized_active = false;
        }
        self.available.notify_all();
        self.async_available.notify_waiters();
    }

    fn close(&self) {
        self.state.lock().closed = true;
        self.available.notify_all();
        self.async_available.notify_waiters();
    }
}

/// Cancellation before allocation must not leave ordinary producers behind
/// an oversized waiter that no longer exists.
struct OversizedBudgetWaiter<'a> {
    budget: &'a BufferBudget,
    registered: bool,
}

impl Drop for OversizedBudgetWaiter<'_> {
    fn drop(&mut self) {
        if self.registered {
            self.budget.state.lock().oversized_waiters -= 1;
            self.budget.available.notify_all();
            self.budget.async_available.notify_waiters();
        }
    }
}

/// A direct buffer whose active memory is accounted until its write CQE.
pub(crate) struct ParallelBuffer {
    buffer: Option<DirectBuf>,
    budget: Arc<BufferBudget>,
    active_bytes: Option<u64>,
    poolable: bool,
    // Field order frees the DATA allocation before retiring its ledger pin.
    _v7_reservation_pin: Option<WalV7BatchBufferPin>,
}

impl ParallelBuffer {
    fn pooled(buffer: DirectBuf, budget: Arc<BufferBudget>) -> Self {
        Self {
            buffer: Some(buffer),
            budget,
            active_bytes: None,
            poolable: true,
            _v7_reservation_pin: None,
        }
    }

    fn activate(buffer: DirectBuf, budget: Arc<BufferBudget>, active_bytes: u64) -> Self {
        Self {
            buffer: Some(buffer),
            budget,
            active_bytes: Some(active_bytes),
            poolable: true,
            _v7_reservation_pin: None,
        }
    }

    fn activate_unpooled(
        buffer: DirectBuf,
        budget: Arc<BufferBudget>,
        active_bytes: u64,
        reservation_pin: WalV7BatchBufferPin,
    ) -> Self {
        Self {
            buffer: Some(buffer),
            budget,
            active_bytes: Some(active_bytes),
            poolable: false,
            _v7_reservation_pin: Some(reservation_pin),
        }
    }

    pub(crate) fn direct_mut(&mut self) -> &mut DirectBuf {
        self.buffer
            .as_mut()
            .expect("live parallel buffer owns DirectBuf")
    }

    fn release_budget(&mut self) {
        if let Some(bytes) = self.active_bytes.take() {
            self.budget.release(bytes);
        }
    }
}

impl WorkerBuffer for ParallelBuffer {
    fn as_ptr(&self) -> *const u8 {
        self.buffer
            .as_ref()
            .expect("live parallel buffer owns DirectBuf")
            .as_ptr()
    }

    fn len(&self) -> usize {
        self.buffer
            .as_ref()
            .expect("live parallel buffer owns DirectBuf")
            .len()
    }

    fn cap(&self) -> usize {
        self.buffer
            .as_ref()
            .expect("live parallel buffer owns DirectBuf")
            .cap()
    }

    fn retire(mut self, pool: &ArrayQueue<Self>) {
        self.release_budget();
        if self.poolable && self.cap() == BUFFER_POOL_BUF_SIZE {
            let _ = pool.push(self);
        }
    }
}

impl Drop for ParallelBuffer {
    fn drop(&mut self) {
        self.release_budget();
    }
}

struct AdmittedBatch {
    ticket: u64,
    file_offset: u64,
    aligned_len: usize,
    v7_allocation: Option<WalV7DataAllocation>,
    buffer: ParallelBuffer,
}

struct PackedGroup {
    first_ticket: u64,
    next_ticket: u64,
    allocation_start: u64,
    reserved_end: u64,
    frontier_offsets: Vec<u64>,
    v7_layout: bool,
    writes: WriteGroupBuffers<ParallelBuffer>,
    initialize_extents: bool,
}

struct AdmissionState {
    open: bool,
    close_cutoff: Option<u64>,
    poison: Option<(u64, String)>,
    #[cfg(feature = "bench")]
    initial_ticket: u64,
    next_ticket: u64,
    admitted_end: u64,
    v7_allocator: Option<WalV7PhysicalAllocator>,
    v7_header_digest: Option<[u8; 32]>,
    v7_reservation_owner: Option<Arc<WalV7ReservationOwner>>,
    v7_metadata: Option<WalV7RuntimeMetadata>,
    v7_prefixes: BTreeMap<u64, WalV7PrefixMetadata>,
    v7_reservations: BTreeMap<u64, WalV7OwnedBatchReservation>,
    queue: VecDeque<AdmittedBatch>,
}

#[derive(Clone, Copy)]
struct WalV7PrefixMetadata {
    last_commit_ts: u64,
    prefix_digest: [u8; 32],
}

struct WalV7FrontierCycle {
    reservation: WalV7FrontierReservation,
    cursor: WalV7FrontierCursor,
}

#[derive(Default)]
struct DurabilityState {
    written_frontier: u64,
    durable_frontier: u64,
    poison_ticket: Option<u64>,
    poison_error: Option<String>,
    completed: BTreeMap<u64, GroupWriteResult>,
}

struct DurabilityShared {
    state: Mutex<DurabilityState>,
    changed: Condvar,
    async_changed: tokio::sync::Notify,
}

impl DurabilityShared {
    fn notify_change(&self) {
        self.changed.notify_all();
        self.async_changed.notify_waiters();
    }
}

struct RuntimeInner {
    admission: Mutex<AdmissionState>,
    packer_state: Mutex<PackerState>,
    packer_failures: Mutex<Option<Sender<GroupWriteResult>>>,
    packer_wake: Sender<bool>,
    #[cfg(test)]
    file_size_limit: std::sync::atomic::AtomicU64,
    buffer_budget: Arc<BufferBudget>,
    buffer_pool: Arc<ArrayQueue<ParallelBuffer>>,
    worker: IoWorkerClient<ParallelBuffer>,
    sync_progress: Arc<WalSyncProgress>,
    preallocator: Arc<File>,
    preallocation_lock: Mutex<()>,
    max_file_size: u64,
    durability: Arc<DurabilityShared>,
}

struct RuntimeThreads {
    packer: Option<JoinHandle<Result<()>>>,
    worker: Option<IoWorker<ParallelBuffer>>,
    coordinator: Option<JoinHandle<Result<()>>>,
    closed: bool,
    close_error: Option<String>,
}

struct PackerState {
    next_ticket: u64,
    reserved_end: u64,
    preallocated_end: u64,
    initializer: Option<ExtentInitializer>,
}

fn is_ext_filesystem(file: &File) -> bool {
    // ext2/3/4 share this magic. Keep other filesystems, particularly tmpfs,
    // on allocation only; initialization there has no measured benefit.
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: file is live, and stat points to writable storage of the required size.
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return false;
    }

    // SAFETY: a successful fstatfs initialized stat.
    unsafe { stat.assume_init() }.f_type == libc::EXT4_SUPER_MAGIC
}

struct ExtentPreparation {
    end: u64,
    zero_fill: bool,
}

/// Owns preparation beyond the packer's ready prefix. There is at most
/// one outstanding request/result, so stopping admission and dropping the
/// request sender lets close join without draining the completion channel.
/// The worker retains its file and aligned buffer until synchronous writes
/// finish, including on initialization failure or runtime construction failure.
struct ExtentInitializer {
    requests: Option<Sender<ExtentPreparation>>,
    completions: Receiver<std::result::Result<u64, String>>,
    join: Option<JoinHandle<Result<()>>>,
    ready_end: u64,
}

impl ExtentInitializer {
    fn spawn(file: Arc<File>, start: u64) -> Result<Self> {
        ensure!(
            (WAL_HEADER_END..=MAX_WAL_FILE_SIZE).contains(&start) && start.is_multiple_of(4096),
            "invalid WAL extent initialization offset"
        );
        let (requests, incoming) = crossbeam_channel::bounded::<ExtentPreparation>(1);
        let (finished, completions) = crossbeam_channel::bounded(1);
        let join = thread::Builder::new()
            .name("wal-extent-initializer".to_owned())
            .spawn(move || {
                use std::os::unix::fs::FileExt;

                let mut offset = start;
                let mut zeros = DirectBuf::new(EXTENT_INITIALIZATION_CHUNK);
                zeros.zero_range(0, EXTENT_INITIALIZATION_CHUNK);
                while let Ok(ExtentPreparation { end, zero_fill }) = incoming.recv() {
                    let result = (|| -> Result<()> {
                        preallocate(&file, end)?;
                        while zero_fill && offset < end {
                            let len =
                                (end - offset).min(EXTENT_INITIALIZATION_CHUNK as u64) as usize;
                            file.write_all_at(zeros.initialized_slice(0, len), offset)
                                .context("failed to initialize WAL extent")?;
                            offset += len as u64;
                        }
                        // Skipped initialization is still a prepared prefix.
                        // Later small writes must never zero-fill it again:
                        // the packer may already have submitted WAL data there.
                        offset = end;

                        Ok(())
                    })();
                    let _ =
                        finished.send(result.as_ref().map(|()| end).map_err(|e| format!("{e:#}")));
                    if result.is_err() {
                        // A failed speculative suffix must not fail close for
                        // the prepared prefix. If a group needs that suffix,
                        // prepare consumes this error and poisons its tickets.
                        break;
                    }
                }

                Ok(())
            })
            .context("failed to spawn WAL extent initializer")?;
        let initializer = Self {
            requests: Some(requests),
            completions,
            join: Some(join),
            ready_end: start,
        };
        if start < MAX_WAL_FILE_SIZE {
            initializer.request(
                round_up(start + 1, PREALLOC_BLOCK).context("extent overflow")?,
                true,
            )?;
        }

        Ok(initializer)
    }

    fn request(&self, end: u64, zero_fill: bool) -> Result<()> {
        self.requests
            .as_ref()
            .context("initializer closed")?
            .send(ExtentPreparation { end, zero_fill })
            .context("extent initializer stopped")
    }

    fn prepare(&mut self, end: u64, zero_fill: bool) -> Result<()> {
        ensure!(
            end <= MAX_WAL_FILE_SIZE && end.is_multiple_of(4096),
            "invalid WAL extent initialization target"
        );
        while self.ready_end < end {
            self.ready_end = self
                .completions
                .recv()
                .context("extent initializer completion disconnected")?
                .map_err(anyhow::Error::msg)?;
            if self.ready_end < MAX_WAL_FILE_SIZE {
                // Start the next extent while the packer submits this one.
                // For a large batch, first catch up to its required end.
                self.request(
                    end.max(self.ready_end + PREALLOC_BLOCK)
                        .min(MAX_WAL_FILE_SIZE),
                    zero_fill,
                )?;
            }
        }

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.requests.take();
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| anyhow!("extent initializer panicked"))??;
        }

        Ok(())
    }
}

impl Drop for ExtentInitializer {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            log::error!("failed to stop WAL extent initializer: {error:#}");
        }
    }
}

/// Parallel WAL runtime with admission-driven packing, an I/O worker, a
/// single-owner durability coordinator, and ext-family extent lookahead.
pub(crate) struct ParallelWalRuntime {
    inner: Arc<RuntimeInner>,
    threads: Mutex<RuntimeThreads>,
}

impl ParallelWalRuntime {
    pub(crate) fn spawn(
        worker_file: Arc<File>,
        sync_file: Arc<File>,
        preallocator: Arc<File>,
        start: ParallelWalRuntimeStart,
    ) -> Result<Self> {
        start.validate()?;
        start.validate_v7_file_handles(&worker_file, &sync_file, &preallocator)?;
        let initial_file_end = start.append_offset;

        ensure!(
            (start.min_append_offset..=start.max_file_size).contains(&initial_file_end),
            "invalid initial WAL append offset {initial_file_end}"
        );

        let buffer_budget = Arc::new(BufferBudget::new());
        let buffer_pool = Arc::new(ArrayQueue::new(BUFFER_POOL_CAPACITY));
        if start.prewarm_buffers {
            for _ in 0..BUFFER_POOL_CAPACITY {
                let _ = buffer_pool.push(ParallelBuffer::pooled(
                    DirectBuf::new(BUFFER_POOL_BUF_SIZE),
                    Arc::clone(&buffer_budget),
                ));
            }
        }

        let initializer = if start.initialize_extents && is_ext_filesystem(&preallocator) {
            Some(ExtentInitializer::spawn(
                Arc::clone(&preallocator),
                initial_file_end,
            )?)
        } else {
            None
        };
        let mut worker = IoWorker::spawn(worker_file, Arc::clone(&buffer_pool))?;
        let worker_client = worker.client();
        let sync_progress = worker.sync_progress();
        sync_progress.initialize_frontiers(start.next_ticket);
        let worker_completions = worker.take_completions();
        let packer_failures_tx = worker.failure_sender();
        let durability = Arc::new(DurabilityShared {
            state: Mutex::new(DurabilityState {
                written_frontier: start.next_ticket,
                durable_frontier: start.next_ticket,
                ..DurabilityState::default()
            }),
            changed: Condvar::new(),
            async_changed: tokio::sync::Notify::new(),
        });
        let (packer_wake, packer_requests) = crossbeam_channel::bounded(1);
        let inner = Arc::new(RuntimeInner {
            admission: Mutex::new(AdmissionState {
                open: start.admission_open,
                close_cutoff: None,
                poison: None,
                #[cfg(feature = "bench")]
                initial_ticket: start.next_ticket,
                next_ticket: start.next_ticket,
                admitted_end: initial_file_end,
                v7_allocator: start.v7_allocator,
                v7_header_digest: start.v7_header_digest,
                v7_reservation_owner: start.v7_reservation_owner,
                v7_metadata: start.v7_metadata,
                v7_prefixes: BTreeMap::new(),
                v7_reservations: BTreeMap::new(),
                queue: VecDeque::new(),
            }),
            packer_state: Mutex::new(PackerState {
                next_ticket: start.next_ticket,
                reserved_end: initial_file_end,
                preallocated_end: initial_file_end,
                initializer,
            }),
            packer_failures: Mutex::new(Some(packer_failures_tx)),
            packer_wake,
            #[cfg(test)]
            file_size_limit: std::sync::atomic::AtomicU64::new(MAX_WAL_FILE_SIZE),
            buffer_budget,
            buffer_pool,
            worker: worker_client,
            sync_progress,
            preallocator,
            preallocation_lock: Mutex::new(()),
            max_file_size: start.max_file_size,
            durability: Arc::clone(&durability),
        });

        let coordinator_inner = Arc::clone(&inner);
        let coordinator = match thread::Builder::new()
            .name("wal-sync-coordinator".to_owned())
            .spawn(move || {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_sync_coordinator(
                        sync_file,
                        worker_completions,
                        Arc::clone(&coordinator_inner),
                    )
                })) {
                    Ok(result) => result,
                    Err(_) => {
                        poison_unacknowledged(&coordinator_inner, "WAL sync coordinator panicked");
                        Err(anyhow!("WAL sync coordinator panicked"))
                    }
                }
            }) {
            Ok(join) => join,
            Err(error) => {
                let _ = worker.close();
                return Err(error).context("failed to spawn WAL sync coordinator");
            }
        };

        let packer_inner = Arc::clone(&inner);
        let packer = match thread::Builder::new()
            .name("wal-ordered-packer".to_owned())
            .spawn(move || {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while let Ok(true) = packer_requests.recv() {
                        pack_admitted_groups(&packer_inner, packer_inner.packer_state.lock())?;
                    }
                    Ok(())
                })) {
                    Ok(result) => result,
                    Err(_) => {
                        poison_unacknowledged(&packer_inner, "WAL ordered packer panicked");
                        Err(anyhow!("WAL ordered packer panicked"))
                    }
                }
            }) {
            Ok(join) => join,
            Err(error) => {
                inner.packer_failures.lock().take();
                let _ = worker.close();
                let _ = coordinator.join();
                return Err(error).context("failed to spawn WAL ordered packer");
            }
        };

        Ok(Self {
            inner,
            threads: Mutex::new(RuntimeThreads {
                packer: Some(packer),
                worker: Some(worker),
                coordinator: Some(coordinator),
                closed: false,
                close_error: None,
            }),
        })
    }

    /// Return the last successfully synced v7 FRONTIER after a caller has
    /// stopped admission and drained its fixed ticket cutoff.
    pub(crate) fn v7_frontier_cursor(&self) -> Result<WalV7FrontierCursor> {
        let admission = self.inner.admission.lock();
        let allocator = admission
            .v7_allocator
            .as_ref()
            .context("parallel WAL runtime is not v7")?;
        let cursor = allocator.last_frontier();
        let frontier_end = cursor
            .frame_offset
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .context("v7 FRONTIER end overflows")?;
        admission
            .v7_metadata
            .as_ref()
            .context("v7 Parallel runtime has no hash metadata")?
            .validate_frontier(cursor.frontier, frontier_end)?;

        Ok(cursor)
    }

    pub(crate) fn allocate_buffer(&self, capacity: usize) -> Result<ParallelBuffer> {
        let capacity = DirectBuf::align_up(capacity.max(BUFFER_POOL_BUF_SIZE));
        let capacity_u64 = u64::try_from(capacity).context("WAL buffer capacity exceeds u64")?;
        self.inner.buffer_budget.reserve(capacity_u64)?;

        Ok(self.activate_buffer(capacity, capacity_u64))
    }

    pub(crate) async fn allocate_buffer_async(&self, capacity: usize) -> Result<ParallelBuffer> {
        let capacity = DirectBuf::align_up(capacity.max(BUFFER_POOL_BUF_SIZE));
        let capacity_u64 = u64::try_from(capacity).context("WAL buffer capacity exceeds u64")?;
        self.inner.buffer_budget.reserve_async(capacity_u64).await?;

        Ok(self.activate_buffer(capacity, capacity_u64))
    }

    fn allocate_v7_buffer(
        &self,
        capacity: usize,
        reservation: &WalV7OwnedBatchReservation,
    ) -> Result<ParallelBuffer> {
        ensure!(
            capacity > 0 && capacity.is_multiple_of(WAL_V7_FRAME_LEN),
            "v7 DATA buffer capacity is not a positive frame multiple"
        );
        let capacity_u64 =
            u64::try_from(capacity).context("v7 DATA buffer capacity exceeds u64")?;
        self.inner.buffer_budget.reserve(capacity_u64)?;

        Ok(ParallelBuffer::activate_unpooled(
            DirectBuf::new(capacity),
            Arc::clone(&self.inner.buffer_budget),
            capacity_u64,
            reservation.pin_buffer(),
        ))
    }

    /// Validate a v7 batch and hold its temporary preparation charge. Callers
    /// may retain this value while assigning ordered MVCC metadata.
    pub(crate) fn prepare_v7_batch<'a>(
        &self,
        batch: &'a WalBatch,
        limits: WalV5Limits,
    ) -> Result<WalV7PreparedBatch<'a>> {
        ensure!(!batch.entries.is_empty(), "v7 WAL batch is empty");
        let limits = WalV5Limits {
            max_input_entry_count: limits
                .max_input_entry_count
                .min(LIVE_WAL_V5_LIMITS.max_input_entry_count),
            max_batch_data_bytes: limits
                .max_batch_data_bytes
                .min(LIVE_WAL_V5_LIMITS.max_batch_data_bytes),
            max_entry_count: limits
                .max_entry_count
                .min(LIVE_WAL_V5_LIMITS.max_entry_count),
            max_key_bytes: limits.max_key_bytes.min(LIVE_WAL_V5_LIMITS.max_key_bytes),
            max_value_bytes: limits
                .max_value_bytes
                .min(LIVE_WAL_V5_LIMITS.max_value_bytes),
        };
        let owner = self
            .inner
            .admission
            .lock()
            .v7_reservation_owner
            .clone()
            .context("v7 Parallel runtime has no reservation owner")?;
        {
            let admission = self.inner.admission.lock();
            ensure!(admission.open, "v7 Parallel WAL admission is closed");
            ensure!(admission.v7_allocator.is_some(), "WAL runtime is not v7");
        }

        let preparation_bytes = CanonicalWalEntryStream::preparation_buffer_bytes(batch, limits)?;
        let preparation = owner.reserve_preparation_buffer(preparation_bytes)?;
        let prepared = prepare_logical_batch(batch, limits)?;

        Ok(WalV7PreparedBatch {
            prepared,
            owner,
            preparation,
            limits,
        })
    }

    /// Prepare and admit one v7 ticket into the ordered parallel DATA packer.
    /// The caller publishes the returned ticket only after `wait_durable`.
    pub(crate) fn put_v7_batch(&self, batch: &WalBatch, limits: WalV5Limits) -> Result<(u64, u64)> {
        let commit_ts = batch.commit_ts;
        let recorded_at = batch.recorded_at;
        let prepared = self.prepare_v7_batch(batch, limits)?;
        self.put_v7_prepared_batch(prepared, commit_ts, recorded_at)
    }

    /// Admit a previously prepared batch with ordered MVCC metadata.
    pub(crate) fn put_v7_prepared_batch(
        &self,
        prepared_batch: WalV7PreparedBatch<'_>,
        commit_ts: u64,
        sampled_recorded_at: crate::pitr::RecordedAt,
    ) -> Result<(u64, u64)> {
        let WalV7PreparedBatch {
            prepared,
            owner,
            preparation,
            limits,
        } = prepared_batch;

        let encoded_batch_bytes =
            u64::try_from(prepared.encoded_len()).context("v7 logical batch length exceeds u64")?;
        let fragment_count = data_fragment_count(encoded_batch_bytes)
            .context("v7 logical batch has no valid DATA fragment count")?;
        let frame_count =
            usize::try_from(fragment_count).context("v7 DATA fragment count exceeds usize")?;
        let frame_bytes = frame_count
            .checked_mul(WAL_V7_FRAME_LEN)
            .context("v7 DATA frame buffer length overflows")?;

        {
            let admission = self.inner.admission.lock();
            ensure!(admission.open, "v7 Parallel WAL admission is closed");
            validate_v7_data_capacity(&admission, fragment_count)?;
        }

        // Charge persistent frames, marker headroom, seal index, and all batch
        // buffers before allocating a direct buffer or assigning coordinates.
        let mut reservation = match owner.reserve_batch(encoded_batch_bytes) {
            Ok(reservation) => reservation,
            Err(resource_error) => {
                // Concurrent producers can consume the remaining segment space
                // after preflight. Preserve rotation as the actionable result
                // when that happened while resource accounting was in flight.
                let admission = self.inner.admission.lock();
                ensure!(admission.open, "v7 Parallel WAL admission is closed");
                validate_v7_data_capacity(&admission, fragment_count)?;
                return Err(resource_error).context("reserve v7 Parallel batch resources");
            }
        };
        let mut buffer = self.allocate_v7_buffer(frame_bytes, &reservation)?;
        let mut post_admission_failure = None;
        let (ticket, first_data_offset) = {
            let mut admission = self.inner.admission.lock();
            ensure!(admission.open, "v7 Parallel WAL admission is closed");
            validate_v7_data_capacity(&admission, fragment_count)?;
            let allocator = admission
                .v7_allocator
                .as_ref()
                .context("v7 Parallel runtime has no physical allocator")?;

            let ticket = allocator.next_ticket();
            ensure!(
                ticket == admission.next_ticket,
                "v7 Parallel admission ticket cursor disagrees with its allocator"
            );
            let previous = allocator.last_frontier();
            let allocation_tail = allocator.allocation_tail();
            let physical_hashed_end = admission
                .v7_metadata
                .as_ref()
                .map(|metadata| metadata.physical_hashed_end)
                .context("v7 Parallel runtime has no hash metadata")?;
            ensure!(
                physical_hashed_end == allocation_tail,
                "v7 Parallel physical hash cursor disagrees with DATA allocator"
            );
            let last_commit_ts = admission
                .v7_prefixes
                .last_key_value()
                .map_or(previous.frontier.last_commit_ts, |(_, metadata)| {
                    metadata.last_commit_ts
                });
            ensure!(
                commit_ts > last_commit_ts,
                "v7 Parallel batch commit timestamp does not advance"
            );
            let recorded_at = admission
                .v7_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_recorded_at)
                .map_or(sampled_recorded_at, |previous| {
                    previous.max(sampled_recorded_at)
                });
            let logical_batch = prepared.encode_with_metadata(ticket, commit_ts, recorded_at)?;
            drop(preparation);
            let next_ticket = ticket
                .checked_add(1)
                .context("v7 Parallel ticket counter overflows")?;
            let header_digest = admission
                .v7_header_digest
                .context("v7 Parallel runtime has no immutable-header digest")?;
            ensure!(
                !admission.v7_reservations.contains_key(&ticket),
                "v7 Parallel admission reused a reservation ticket"
            );
            // All expected fallible work is complete before reserve_data. If
            // frame materialization fails after coordinate assignment, poison
            // the runtime and leave the persistent charge with its image owner.
            let allocation = admission
                .v7_allocator
                .as_mut()
                .context("v7 Parallel runtime has no physical allocator")?
                .reserve_data(fragment_count)?;
            let first_data_offset = allocation.frame_range().start;
            let data_end = allocation.frame_range().end;
            reservation.mark_admitted();
            let previous_reservation = admission.v7_reservations.insert(ticket, reservation);
            debug_assert!(previous_reservation.is_none());
            admission.next_ticket = next_ticket;
            admission.admitted_end = data_end;

            let encoded_len = encode_data_frames_into(
                header_digest,
                first_data_offset,
                ticket,
                &logical_batch,
                limits,
                buffer.direct_mut().as_mut_slice(),
            );
            match encoded_len {
                Ok(encoded_len) if encoded_len == frame_bytes => {
                    buffer.direct_mut().set_len(encoded_len);
                    let metadata = admission
                        .v7_metadata
                        .as_mut()
                        .expect("v7 runtime metadata is present for a v7 allocator");
                    metadata.logical_hasher.update(&logical_batch);
                    let prefix_digest: [u8; 32] = metadata.logical_hasher.clone().finalize().into();
                    metadata
                        .physical_hasher
                        .update(buffer.direct_mut().initialized_slice(0, encoded_len));
                    metadata.physical_hashed_end = data_end;
                    metadata.last_recorded_at = Some(recorded_at);
                    admission.v7_prefixes.insert(
                        next_ticket,
                        WalV7PrefixMetadata {
                            last_commit_ts: commit_ts,
                            prefix_digest,
                        },
                    );
                    admission.queue.push_back(AdmittedBatch {
                        ticket,
                        file_offset: first_data_offset,
                        aligned_len: encoded_len,
                        v7_allocation: Some(allocation),
                        buffer,
                    });
                    #[cfg(feature = "chaos-testing")]
                    crate::chaos::failpoint::note_parallel_wal_admission();
                }
                Ok(_) => {
                    post_admission_failure = Some((
                        ticket,
                        anyhow!("v7 DATA materializer returned an unexpected frame length"),
                    ));
                }
                Err(error) => post_admission_failure = Some((ticket, error)),
            }
            if let Some((failed_ticket, error)) = &post_admission_failure {
                admission.open = false;
                if admission
                    .poison
                    .as_ref()
                    .is_none_or(|(current, _)| failed_ticket < current)
                {
                    admission.poison = Some((*failed_ticket, format!("{error:#}")));
                }
            }
            (ticket, first_data_offset)
        };

        if let Some((ticket, error)) = post_admission_failure {
            report_packer_failure(&self.inner, ticket, &error);
            return Err(error).context("materialize admitted v7 Parallel DATA frames");
        }

        let _ = self.inner.packer_wake.try_send(true);
        Ok((ticket, first_data_offset))
    }

    fn activate_buffer(&self, capacity: usize, capacity_u64: u64) -> ParallelBuffer {
        let direct_buffer = match self.inner.buffer_pool.pop() {
            Some(mut pooled) if pooled.cap() >= capacity => {
                pooled.release_budget();
                pooled
                    .buffer
                    .take()
                    .expect("pooled parallel buffer owns its DirectBuf")
            }
            Some(pooled) => {
                let _ = self.inner.buffer_pool.push(pooled);
                DirectBuf::new(capacity)
            }
            None => DirectBuf::new(capacity),
        };

        ParallelBuffer::activate(
            direct_buffer,
            Arc::clone(&self.inner.buffer_budget),
            capacity_u64,
        )
    }

    pub(crate) fn admit(&self, buffer: ParallelBuffer, aligned_len: usize) -> Result<u64> {
        if aligned_len == 0
            || !aligned_len.is_multiple_of(4096)
            || aligned_len > buffer.cap()
            || buffer.len() != aligned_len
        {
            self.recycle(buffer);
            bail!("invalid prepared parallel WAL buffer length");
        }
        let aligned_len_u64 = match u64::try_from(aligned_len) {
            Ok(aligned_len) => aligned_len,
            Err(error) => {
                self.recycle(buffer);
                return Err(anyhow!(error).context("WAL write length exceeds u64"));
            }
        };

        let mut state = self.inner.admission.lock();
        #[cfg(test)]
        let file_size_limit = self
            .inner
            .file_size_limit
            .load(std::sync::atomic::Ordering::Acquire)
            .min(self.inner.max_file_size);
        #[cfg(not(test))]
        let file_size_limit = self.inner.max_file_size;

        if let Some((ticket, error)) = &state.poison {
            let error = anyhow!("parallel WAL is poisoned at ticket {ticket}: {error}");
            drop(state);
            self.recycle(buffer);
            return Err(error);
        }
        if !state.open {
            drop(state);
            self.recycle(buffer);
            bail!("parallel WAL is closed");
        }
        if state.v7_allocator.is_some() {
            drop(state);
            self.recycle(buffer);
            bail!("v7 WAL admission requires a reserved DATA allocation");
        }

        let Some(file_end) = state.admitted_end.checked_add(aligned_len_u64) else {
            drop(state);
            self.recycle(buffer);
            bail!("WAL file offset overflow");
        };
        let Some(preallocation_end) = round_up(file_end, PREALLOC_BLOCK) else {
            drop(state);
            self.recycle(buffer);
            bail!("WAL preallocation offset overflow");
        };
        if file_end > file_size_limit || preallocation_end > file_size_limit {
            let empty_file_end = WAL_HEADER_END.checked_add(aligned_len_u64);
            let empty_end = empty_file_end.and_then(|end| round_up(end, PREALLOC_BLOCK));
            drop(state);
            self.recycle(buffer);
            if empty_end.is_none_or(|end| end > file_size_limit) {
                return Err(anyhow!("WAL batch exceeds the maximum empty-file capacity"));
            }
            return Err(anyhow::Error::new(WalFull));
        }

        let ticket = state.next_ticket;
        let Some(next_ticket) = state.next_ticket.checked_add(1) else {
            drop(state);
            self.recycle(buffer);
            bail!("WAL ticket counter overflow");
        };
        state.next_ticket = next_ticket;
        let file_offset = state.admitted_end;
        state.admitted_end = file_end;
        state.queue.push_back(AdmittedBatch {
            ticket,
            file_offset,
            aligned_len,
            v7_allocation: None,
            buffer,
        });
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::note_parallel_wal_admission();
        debug_assert!(WAL_HEADER_END <= state.admitted_end);
        debug_assert!(state.admitted_end <= self.inner.max_file_size);
        debug_assert_eq!(state.next_ticket, ticket + 1);

        drop(state);
        // One queued wake is sufficient: the dedicated packer drains admission.
        // Disk allocation and extent initialization never run on a producer.
        let _ = self.inner.packer_wake.try_send(true);

        Ok(ticket)
    }

    #[cfg(test)]
    pub(crate) fn set_file_size_limit(&self, limit: u64) -> Result<()> {
        ensure!(
            (PREALLOC_BLOCK..=MAX_WAL_FILE_SIZE).contains(&limit)
                && limit.is_multiple_of(PREALLOC_BLOCK),
            "test WAL size limit must be a preallocation-aligned value within the production cap"
        );
        let admission = self.inner.admission.lock();
        ensure!(
            round_up(admission.admitted_end, PREALLOC_BLOCK).is_some_and(|end| end <= limit),
            "test WAL size limit cannot exclude admitted bytes or their preallocation extent"
        );
        self.inner
            .file_size_limit
            .store(limit, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn assigned_ticket_count(&self) -> u64 {
        self.inner.admission.lock().next_ticket
    }

    pub(crate) fn logical_length(&self) -> u64 {
        self.inner.admission.lock().admitted_end
    }

    pub(crate) fn batch_count(&self) -> u64 {
        self.inner.admission.lock().next_ticket
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        if self.inner.durability.state.lock().poison_ticket.is_some() {
            return true;
        }
        self.inner.admission.lock().poison.is_some()
    }

    pub(crate) fn set_write_profile(&self, profile: Arc<crate::mem_table::WriteProfile>) {
        self.inner.sync_progress.set_profile(profile);
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_wal_sync_diagnostics_enabled(
        &self,
        profile: &crate::mem_table::WriteProfile,
        enabled: bool,
    ) -> Result<()> {
        let admission = self.inner.admission.lock();
        validate_sync_diagnostics_transition(
            enabled,
            profile.wal_sync_diagnostics_enabled(),
            admission.next_ticket != admission.initial_ticket,
        )?;
        profile.set_wal_sync_diagnostics_enabled(enabled);
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        let cutoff = {
            let admission = self.inner.admission.lock();
            admission.next_ticket
        };
        if cutoff == 0 {
            return Ok(());
        }
        self.wait_durable(cutoff - 1)
    }

    pub(crate) async fn wait_durable_async(&self, ticket: u64) -> Result<()> {
        let assigned = self.inner.admission.lock().next_ticket;
        ensure!(ticket < assigned, "unassigned parallel WAL ticket {ticket}");
        let notification = self.inner.durability.async_changed.notified();
        tokio::pin!(notification);
        loop {
            // Register before reading the predicate; completion may race with
            // this check and the subsequent await without losing the wakeup.
            notification.as_mut().enable();
            {
                let state = self.inner.durability.state.lock();
                if state.durable_frontier > ticket {
                    return Ok(());
                }
                if state.poison_ticket.is_some_and(|poison| ticket >= poison) {
                    let message = state
                        .poison_error
                        .as_deref()
                        .unwrap_or("WAL durability failed");
                    bail!("WAL ticket {ticket} failed at poison boundary: {message}");
                }
            }
            notification.as_mut().await;
            notification.set(self.inner.durability.async_changed.notified());
        }
    }

    pub(crate) fn wait_durable(&self, ticket: u64) -> Result<()> {
        let mut state = self.inner.durability.state.lock();
        let assigned = self.inner.admission.lock().next_ticket;
        ensure!(
            ticket < assigned,
            "submit_and_commit called with unassigned parallel WAL ticket {ticket} (next_ticket={assigned})"
        );
        loop {
            if state.durable_frontier > ticket {
                return Ok(());
            }
            if state.poison_ticket.is_some_and(|poison| ticket >= poison) {
                let message = state
                    .poison_error
                    .as_deref()
                    .unwrap_or("WAL durability failed");
                bail!("WAL ticket {ticket} failed at poison boundary: {message}");
            }
            self.inner.durability.changed.wait(&mut state);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.threads.lock().closed
    }

    #[cfg(test)]
    pub(crate) fn admission_is_open(&self) -> bool {
        self.inner.admission.lock().open
    }

    pub(crate) fn close(&self) -> Result<()> {
        let mut threads = self.threads.lock();
        if threads.closed {
            return match &threads.close_error {
                Some(error) => Err(anyhow!("parallel WAL close previously failed: {error}")),
                None => Ok(()),
            };
        }

        {
            let mut admission = self.inner.admission.lock();
            admission.open = false;
            admission.close_cutoff = Some(admission.next_ticket);
        }
        self.inner.buffer_budget.close();

        let mut close_error = None;
        let _ = self.inner.packer_wake.send(false);
        if let Some(packer) = threads.packer.take() {
            match packer.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    close_error = Some(format!("packer failed: {error:#}"));
                }
                Err(_) => {
                    close_error = Some("WAL ordered packer panicked".to_owned());
                }
            }
        }
        let packer = self.inner.packer_state.lock();
        if let Err(error) = pack_admitted_groups(&self.inner, packer) {
            close_error = Some(format!("packer failed: {error:#}"));
        }
        self.inner.packer_failures.lock().take();

        // No packer can request more ranges after admission closes and drains.
        // Join initialization before finishing the WAL worker/coordinator drain.
        if let Some(mut initializer) = self.inner.packer_state.lock().initializer.take()
            && let Err(error) = initializer.close()
        {
            close_error.get_or_insert_with(|| format!("extent initializer failed: {error:#}"));
        }

        if let Some(worker) = threads.worker.take()
            && let Err(error) = worker.close()
        {
            close_error.get_or_insert_with(|| format!("I/O worker failed: {error:#}"));
        }

        if let Some(coordinator) = threads.coordinator.take() {
            match coordinator.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    close_error
                        .get_or_insert_with(|| format!("sync coordinator failed: {error:#}"));
                }
                Err(_) => {
                    close_error.get_or_insert_with(|| "WAL sync coordinator panicked".to_owned());
                }
            }
        }

        // Stop owning failed/discarded batch operations after worker and
        // coordinator drain. A DATA buffer retained for ambiguous submission
        // keeps its own reservation pin, so only freed buffers lose their
        // charges. Persistent charges stay with the image until cleanup.
        let remaining_reservations = {
            let mut admission = self.inner.admission.lock();
            admission.queue.clear();
            std::mem::take(&mut admission.v7_reservations)
        };
        for reservation in remaining_reservations.into_values() {
            if let Err(error) = reservation.finish_after_io() {
                close_error.get_or_insert_with(|| {
                    format!("failed to retire v7 WAL buffer reservation: {error}")
                });
            }
        }

        if close_error.is_none()
            && let Some(error) = self.inner.durability.state.lock().poison_error.clone()
        {
            close_error = Some(format!("parallel WAL is poisoned: {error}"));
        }

        #[cfg(feature = "chaos-testing")]
        if Self::close_failure_is_injected() {
            close_error.get_or_insert_with(|| "injected parallel WAL close failure".to_owned());
        }

        threads.closed = true;
        threads.close_error.clone_from(&close_error);
        match close_error {
            Some(error) => Err(anyhow!(error)),
            None => Ok(()),
        }
    }

    #[cfg(feature = "chaos-testing")]
    fn close_failure_is_injected() -> bool {
        crate::chaos::failpoint::fail_point!("parallel_wal.close_failure", |_| true);
        false
    }

    fn recycle(&self, buffer: ParallelBuffer) {
        buffer.retire(&self.inner.buffer_pool);
    }
}

impl Drop for ParallelWalRuntime {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            log::error!("failed to close parallel WAL runtime: {error:#}");
        }
    }
}

fn validate_v7_data_capacity(admission: &AdmissionState, fragment_count: u32) -> Result<()> {
    let allocator = admission
        .v7_allocator
        .as_ref()
        .context("v7 Parallel runtime has no physical allocator")?;
    match allocator.data_capacity_status(fragment_count)? {
        WalV7DataCapacityStatus::Fits => Ok(()),
        WalV7DataCapacityStatus::RotationRequired => Err(anyhow::Error::new(
            WalV7LeaderCapacityError::RotationRequired,
        )),
        WalV7DataCapacityStatus::BatchTooLarge => {
            Err(anyhow::Error::new(WalV7LeaderCapacityError::BatchTooLarge))
        }
    }
}

fn pack_admitted_groups(
    inner: &RuntimeInner,
    mut packer: MutexGuard<'_, PackerState>,
) -> Result<()> {
    loop {
        let packed_result = {
            let mut admission = inner.admission.lock();
            if admission.queue.is_empty() {
                if admission.poison.is_none()
                    && let Some(cutoff) = admission.close_cutoff
                {
                    ensure!(
                        packer.next_ticket == cutoff,
                        "parallel WAL packer stopped at ticket {} before close cutoff {cutoff}",
                        packer.next_ticket
                    );
                }
                // The dedicated packer drains all admitted work before
                // returning to its coalesced wake channel.
                drop(packer);
                return Ok(());
            }
            take_admitted_group(
                &mut admission,
                packer.next_ticket,
                packer.reserved_end,
                PACKER_GROUP_MAX_TICKETS,
            )
        };
        let packed = match packed_result {
            Ok(Some(packed)) => packed,
            Ok(None) => continue,
            Err(error) => {
                report_packer_failure(inner, packer.next_ticket, &error);
                return Err(error);
            }
        };
        let first_ticket = packed.first_ticket;
        let expected_ticket = packed.next_ticket;
        let allocation_start = packed.allocation_start;
        packer.reserved_end = packed.reserved_end;
        let writes = packed.writes;
        let frontier_offsets = packed.frontier_offsets;
        let v7_layout = packed.v7_layout;

        let Some(target_preallocated_end) = round_up(packer.reserved_end, PREALLOC_BLOCK) else {
            let error = anyhow!("parallel WAL preallocation offset overflow");
            report_packer_failure(inner, first_ticket, &error);
            return Err(error);
        };
        let target_preallocated_end = target_preallocated_end.min(inner.max_file_size);
        if target_preallocated_end < packer.reserved_end {
            let error = anyhow!("WAL write exceeds the configured physical size limit");
            report_packer_failure(inner, first_ticket, &error);
            return Err(error);
        }
        if target_preallocated_end > packer.preallocated_end {
            #[cfg(feature = "bench")]
            let preallocation_start = Instant::now();
            let allocation = match packer.initializer.as_mut() {
                Some(initializer) => {
                    initializer.prepare(target_preallocated_end, packed.initialize_extents)
                }
                None => preallocate_runtime(inner, target_preallocated_end),
            };
            if let Err(error) = allocation {
                report_packer_failure(inner, first_ticket, &error);
                return Err(error);
            }
            #[cfg(feature = "bench")]
            // On ext filesystems this measures readiness wait, not the
            // initializer's background I/O time or its additional write bytes.
            inner
                .sync_progress
                .record_preallocation_ns(preallocation_start.elapsed().as_nanos() as u64);
            packer.preallocated_end = target_preallocated_end;
        }
        debug_assert!(WAL_HEADER_END <= packer.reserved_end);
        #[cfg(debug_assertions)]
        {
            let admitted_end = inner.admission.lock().admitted_end;
            debug_assert!(packer.reserved_end <= admitted_end);
            debug_assert!(admitted_end <= inner.max_file_size);
        }
        debug_assert!(packer.preallocated_end >= packer.reserved_end);
        debug_assert!(packer.preallocated_end <= inner.max_file_size);
        if packer.reserved_end > packer.preallocated_end {
            let error = anyhow!("WAL write extends beyond preallocation");
            report_packer_failure(inner, first_ticket, &error);
            return Err(error);
        }

        let group = match if v7_layout {
            WriteGroup::new_with_frontiers(
                packer.next_ticket..expected_ticket,
                writes,
                allocation_start..packer.reserved_end,
                &frontier_offsets,
            )
        } else {
            WriteGroup::new(packer.next_ticket..expected_ticket, writes)
        } {
            Ok(group) => group,
            Err(error) => {
                let error = anyhow!("invalid packed WAL group: {error:?}");
                report_packer_failure(inner, first_ticket, &error);
                return Err(error);
            }
        };
        if let Err(error) = inner.worker.submit_group(group) {
            report_packer_failure(inner, first_ticket, &error);
            return Err(error).context("failed to submit packed WAL group");
        }
        packer.next_ticket = expected_ticket;
    }
}

fn take_admitted_group(
    admission: &mut AdmissionState,
    next_ticket: u64,
    reserved_end: u64,
    max_tickets: usize,
) -> Result<Option<PackedGroup>> {
    debug_assert!(max_tickets > 0);
    let Some(first) = admission.queue.front() else {
        return Ok(None);
    };
    if admission
        .poison
        .as_ref()
        .is_some_and(|(poison_ticket, _)| first.ticket >= *poison_ticket)
    {
        admission.queue.clear();
        return Ok(None);
    }

    let first_ticket = first.ticket;
    let v7_layout = admission.v7_allocator.is_some();
    let group_capacity = admission.queue.len().min(max_tickets);
    let mut writes = WriteGroupBuffers::with_capacity(group_capacity);
    let mut data_allocations = v7_layout.then(|| Vec::with_capacity(group_capacity));
    let mut initialize_extents = false;
    let mut expected_ticket = next_ticket;
    let mut packed_end = reserved_end;
    while writes.len() < max_tickets {
        let Some(next) = admission.queue.front() else {
            break;
        };
        if admission
            .poison
            .as_ref()
            .is_some_and(|(poison_ticket, _)| next.ticket >= *poison_ticket)
        {
            admission.queue.clear();
            break;
        }
        let batch = admission
            .queue
            .pop_front()
            .expect("front WAL batch remains queued");
        let AdmittedBatch {
            ticket,
            file_offset,
            aligned_len,
            v7_allocation,
            buffer,
        } = batch;
        // Keep zero-fill for groups containing small writes. Only extent
        // preparation changes; every batch still uses the parallel pipeline.
        initialize_extents |= aligned_len < ALLOCATION_ONLY_MIN_BATCH_BYTES;
        ensure!(
            ticket == expected_ticket,
            "parallel WAL admission queue is not ticket contiguous"
        );
        match (v7_layout, v7_allocation) {
            (true, Some(allocation)) => {
                ensure!(
                    allocation.ticket() == ticket
                        && allocation.frame_range().start == file_offset
                        && allocation.frame_range().end.checked_sub(file_offset)
                            == Some(aligned_len as u64)
                        && file_offset >= packed_end,
                    "parallel v7 WAL batch disagrees with its physical allocation"
                );
                packed_end = allocation.frame_range().end;
                data_allocations
                    .as_mut()
                    .expect("v7 layout has allocation metadata")
                    .push(allocation);
            }
            (false, None) => {
                ensure!(
                    file_offset == packed_end,
                    "parallel WAL admission queue is not offset contiguous"
                );
                packed_end = file_offset
                    .checked_add(aligned_len as u64)
                    .ok_or_else(|| anyhow!("parallel WAL offset overflow"))?;
            }
            (true, None) => bail!("v7 WAL admission batch has no physical allocation"),
            (false, Some(_)) => bail!("legacy WAL admission batch has a v7 physical allocation"),
        }
        expected_ticket = expected_ticket
            .checked_add(1)
            .ok_or_else(|| anyhow!("parallel WAL ticket counter overflow"))?;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::wal_crash_point("parallel_wal.offset_reserved");
        writes.push(WriteBuffer::new(buffer, file_offset, aligned_len));
    }

    let frontier_offsets = if v7_layout {
        let allocator = admission
            .v7_allocator
            .as_ref()
            .expect("v7 layout has a physical allocator");
        // Validate allocator ownership, ticket order, and every DATA gap.
        allocator.validate_data_group(
            data_allocations
                .as_deref()
                .expect("v7 layout has allocation metadata"),
        )?;
        allocator.frontier_offsets_in(reserved_end..packed_end)?
    } else {
        Vec::new()
    };

    Ok(Some(PackedGroup {
        first_ticket,
        next_ticket: expected_ticket,
        allocation_start: reserved_end,
        reserved_end: packed_end,
        frontier_offsets,
        v7_layout,
        writes,
        initialize_extents,
    }))
}

fn report_packer_failure(inner: &RuntimeInner, ticket: u64, error: &anyhow::Error) {
    let message = format!("{error:#}");
    {
        let mut admission = inner.admission.lock();
        admission.open = false;
        if admission
            .poison
            .as_ref()
            .is_none_or(|(current, _)| ticket < *current)
        {
            admission.poison = Some((ticket, message.clone()));
        }
        let poison_ticket = admission
            .poison
            .as_ref()
            .map_or(ticket, |(poison_ticket, _)| *poison_ticket);
        admission.queue.retain(|batch| batch.ticket < poison_ticket);
    }
    let _ = inner.packer_wake.try_send(true);
    inner.buffer_budget.close();
    if let Some(failures) = inner.packer_failures.lock().as_ref() {
        let _ = failures.send(GroupWriteResult {
            group_id: u64::MAX,
            tickets: ticket..ticket.saturating_add(1),
            write_count: 0,
            write_bytes: 0,
            error: Some(message),
        });
    }
}

/// A task panic has an unknown boundary. Preserve every acknowledged ticket,
/// stop admission and wake both kinds of waiter before the task terminates.
fn poison_unacknowledged(inner: &RuntimeInner, message: &str) {
    let mut state = inner.durability.state.lock();
    let boundary = state.durable_frontier;
    set_poison(&mut state, boundary, message.to_owned());
    let mut admission = inner.admission.lock();
    admission.open = false;
    admission.poison = Some((boundary, message.to_owned()));
    admission.queue.clear();
    inner.buffer_budget.close();
    inner.durability.notify_change();
}

fn run_sync_coordinator(
    sync_file: Arc<File>,
    completions: Receiver<GroupWriteResult>,
    inner: Arc<RuntimeInner>,
) -> Result<()> {
    let mut last_sync_latency = None;

    while let Ok(result) = completions.recv() {
        process_group_result(&inner, result);
        #[cfg(all(test, feature = "chaos-testing"))]
        crate::chaos::failpoint::before_parallel_wal_result_drain();
        drain_ready_results(&inner, &completions);
        if last_sync_latency.is_some_and(|latency| latency >= SYNC_COALESCE_MIN_SYNC) {
            coalesce_admitted_prefix(&inner, &completions);
            // Coalescing may return at its captured admission cutoff while
            // later groups have already completed. Include the results that
            // are queued now before capturing the next durability target.
            // Bound this drain to its initial queue length so new completions
            // cannot extend the coalescing window indefinitely.
            drain_results_at_capture(&inner, &completions);
        }
        if let Some(latency) = synchronize_written_prefix(&sync_file, &inner)? {
            last_sync_latency = Some(latency);
        }
    }

    // Both producers have terminated. Freeze the final written prefix and
    // perform the shutdown sync before allowing the file handles to drop.
    terminalize_unresolved_prefix(&inner);
    synchronize_written_prefix(&sync_file, &inner)?;

    Ok(())
}

fn coalesce_admitted_prefix(inner: &RuntimeInner, completions: &Receiver<GroupWriteResult>) {
    {
        let state = inner.durability.state.lock();
        if state.poison_ticket.is_some() || state.written_frontier <= state.durable_frontier {
            return;
        }
    }

    let deadline = Instant::now() + SYNC_COALESCE_WAIT;
    let mut cutoff = inner.admission.lock().next_ticket;
    loop {
        let state = inner.durability.state.lock();
        if state.poison_ticket.is_some() {
            return;
        }
        let written = state.written_frontier;
        drop(state);
        if written >= cutoff {
            cutoff = inner.admission.lock().next_ticket;
            if written >= cutoff {
                return;
            }
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        match completions.recv_timeout(remaining) {
            Ok(result) => process_group_result(inner, result),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn drain_results_at_capture(inner: &RuntimeInner, completions: &Receiver<GroupWriteResult>) {
    for _ in 0..completions.len() {
        let Ok(result) = completions.try_recv() else {
            break;
        };
        process_group_result(inner, result);
    }
}

fn drain_ready_results(inner: &RuntimeInner, completions: &Receiver<GroupWriteResult>) {
    while let Ok(result) = completions.try_recv() {
        process_group_result(inner, result);
    }
}

fn process_group_result(inner: &RuntimeInner, result: GroupWriteResult) {
    #[cfg(feature = "chaos-testing")]
    let result_end = result.tickets.end;
    let failed = result.error.is_some();
    let mut state = inner.durability.state.lock();
    #[cfg(feature = "chaos-testing")]
    let out_of_order_completion =
        result.error.is_none() && result.tickets.start > state.written_frontier;
    if let Some(error) = &result.error {
        set_poison(&mut state, result.tickets.start, error.clone());
        let durability_poison_ticket = state.poison_ticket.unwrap_or(result.tickets.start);
        let mut admission = inner.admission.lock();
        admission.open = false;
        if admission
            .poison
            .as_ref()
            .is_none_or(|(ticket, _)| result.tickets.start < *ticket)
        {
            admission.poison = Some((result.tickets.start, error.clone()));
        }
        let poison_ticket = admission
            .poison
            .as_ref()
            .map_or(durability_poison_ticket, |(poison_ticket, _)| {
                durability_poison_ticket.min(*poison_ticket)
            });
        admission.queue.retain(|batch| batch.ticket < poison_ticket);
        inner.buffer_budget.close();
    } else {
        let previous = state.completed.insert(result.tickets.start, result);
        debug_assert!(previous.is_none(), "WAL group completion is unique");
    }

    loop {
        let frontier = state.written_frontier;
        let Some(completed) = state.completed.remove(&frontier) else {
            break;
        };
        if state
            .poison_ticket
            .is_some_and(|poison| completed.tickets.end > poison)
        {
            break;
        }
        state.written_frontier = completed.tickets.end;
    }
    debug_assert!(state.durable_frontier <= state.written_frontier);
    if let Some(poison) = state.poison_ticket {
        debug_assert!(state.durable_frontier <= poison);
    }
    debug_assert!(state.written_frontier <= inner.admission.lock().next_ticket);
    debug_assert!(state.durable_frontier <= state.written_frontier);
    #[cfg(feature = "chaos-testing")]
    let later_group_remains_outside_prefix = state.written_frontier < result_end;
    // A successful write CQE only advances the written frontier. Durability
    // waiters can finish after fdatasync; poison must wake them immediately.
    if failed {
        inner.durability.notify_change();
    }
    drop(state);

    #[cfg(feature = "chaos-testing")]
    if out_of_order_completion && later_group_remains_outside_prefix {
        crate::chaos::failpoint::wal_crash_point("parallel_wal.later_group_completed_first");
    }
}

fn terminalize_unresolved_prefix(inner: &RuntimeInner) {
    let mut state = inner.durability.state.lock();
    let assigned = inner.admission.lock().next_ticket;
    if state.poison_ticket.is_none() && state.durable_frontier < assigned {
        let error = "WAL pipeline stopped before assigned tickets became durable".to_owned();
        let poison = state.durable_frontier;
        set_poison(&mut state, poison, error.clone());
        let mut admission = inner.admission.lock();
        admission.open = false;
        admission.queue.clear();
        if admission
            .poison
            .as_ref()
            .is_none_or(|(ticket, _)| poison < *ticket)
        {
            admission.poison = Some((poison, error));
        }
        inner.buffer_budget.close();
    }
    inner.durability.notify_change();
}

fn set_poison(state: &mut DurabilityState, ticket: u64, error: String) {
    if state.poison_ticket.is_none_or(|current| ticket < current) {
        state.poison_ticket = Some(ticket);
        state.poison_error = Some(error);
    }
}

fn poison_v7_runtime(
    inner: &RuntimeInner,
    reservation: Option<WalV7FrontierReservation>,
    durable_ticket: u64,
    message: impl Into<String>,
) {
    let mut message = message.into();
    let mut state = inner.durability.state.lock();
    set_poison(&mut state, durable_ticket, message.clone());
    let mut admission = inner.admission.lock();
    if let Some(reservation) = reservation
        && let Some(allocator) = admission.v7_allocator.as_mut()
        && let Err(error) = allocator.poison_frontier(reservation)
    {
        message.push_str(&format!(
            "; also failed to poison frontier allocator: {error:#}"
        ));
    }
    admission.open = false;
    admission.queue.clear();
    if admission
        .poison
        .as_ref()
        .is_none_or(|(ticket, _)| durable_ticket < *ticket)
    {
        admission.poison = Some((durable_ticket, message.clone()));
    }
    inner.buffer_budget.close();
    inner.durability.notify_change();
}

fn append_v7_frontier(
    sync_file: &File,
    inner: &RuntimeInner,
    target: u64,
    durable_ticket: u64,
    poison_ticket: Option<u64>,
) -> Result<Option<WalV7FrontierCycle>> {
    let mut frontier_reservation = None;
    let result = (|| -> Result<Option<WalV7FrontierCycle>> {
        let mut admission = inner.admission.lock();
        let allocator = admission
            .v7_allocator
            .as_ref()
            .context("v7 Parallel runtime has no physical allocator")?;
        if allocator.eligible_durable_end_for(target)?.is_none() {
            if let Some(poison) = poison_ticket {
                ensure!(
                    allocator.eligible_durable_end_for(poison)?.is_some(),
                    "v7 DATA failure prevents an eligible FRONTIER below poison"
                );
            }
            return Ok(None);
        }
        let mut frame = DirectBuf::new(WAL_V7_FRAME_LEN);
        let previous = allocator.last_frontier();
        let allocation_tail = allocator.allocation_tail();
        let previous_frontier_end = previous
            .frame_offset
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .context("previous v7 FRONTIER end overflows")?;
        let physical_hashed_end = admission
            .v7_metadata
            .as_ref()
            .map(|metadata| metadata.physical_hashed_end)
            .context("v7 Parallel runtime has no hash metadata")?;
        ensure!(
            physical_hashed_end == allocation_tail,
            "v7 Parallel physical hash cursor disagrees with FRONTIER allocation tail"
        );
        let prefix = admission
            .v7_prefixes
            .get(&target)
            .copied()
            .context("v7 Parallel runtime has no digest snapshot for FRONTIER target")?;
        let header_digest = admission
            .v7_header_digest
            .context("v7 Parallel runtime has no immutable-header digest")?;
        let reservation = admission
            .v7_allocator
            .as_mut()
            .context("v7 Parallel runtime has no physical allocator")?
            .reserve_frontier(target)?;
        frontier_reservation = Some(reservation.clone());

        let frontier = WalV7Frontier {
            generation: previous
                .frontier
                .generation
                .checked_add(1)
                .context("v7 Parallel FRONTIER generation overflows")?,
            ticket_end: target,
            durable_end: reservation.durable_end(),
            last_commit_ts: prefix.last_commit_ts,
            prefix_digest: prefix.prefix_digest,
            previous_frontier_offset: previous.frame_offset,
            previous_frontier_digest: previous.frame_digest,
        };
        encode_frontier_successor_into(
            frame.as_mut_slice(),
            frontier,
            previous.frontier,
            previous.frame_offset,
            &previous.frame_digest,
            reservation.frame_offset(),
            header_digest,
        )
        .context("encode v7 Parallel FRONTIER")?;
        frame.set_len(WAL_V7_FRAME_LEN);
        let frontier_bytes = frame.initialized_slice(0, WAL_V7_FRAME_LEN);
        let frontier_digest = frame_record_digest(frontier_bytes)?;
        ensure!(
            previous_frontier_end <= reservation.durable_end()
                && reservation.durable_end() <= reservation.frame_offset(),
            "v7 Parallel FRONTIER has invalid physical prefix bounds"
        );
        let frame_end = reservation
            .frame_offset()
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .context("v7 Parallel FRONTIER end overflows")?;
        let preallocation_end = round_up(frame_end, PREALLOC_BLOCK)
            .context("v7 Parallel FRONTIER preallocation end overflows")?
            .min(inner.max_file_size);
        ensure!(
            preallocation_end >= frame_end,
            "v7 Parallel FRONTIER exceeds the configured segment cap"
        );
        let metadata = admission
            .v7_metadata
            .as_mut()
            .context("v7 Parallel runtime lost its hash metadata")?;
        metadata.physical_hasher.update(frontier_bytes);
        metadata.physical_hashed_end = frame_end;
        admission.admitted_end = frame_end;
        // Record the marker in the shared hash cursor before producers can
        // append after it. Any failure in the I/O below poisons this runtime.
        drop(admission);

        preallocate_runtime(inner, preallocation_end)
            .context("preallocate v7 Parallel FRONTIER frame")?;
        write_v7_frontier_once(sync_file, frontier_bytes, reservation.frame_offset())
            .context("write v7 Parallel FRONTIER")?;

        Ok(Some(WalV7FrontierCycle {
            reservation,
            cursor: WalV7FrontierCursor {
                frontier,
                frame_offset: frontier_reservation
                    .as_ref()
                    .expect("the encoded FRONTIER has a reservation")
                    .frame_offset(),
                frame_digest: frontier_digest,
            },
        }))
    })();

    if let Err(error) = &result {
        poison_v7_runtime(
            inner,
            frontier_reservation,
            durable_ticket,
            format!("v7 Parallel FRONTIER write failed: {error:#}"),
        );
    }
    result
}

fn write_v7_frontier_once(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    loop {
        match write_v7_frontier_at(file, bytes, offset) {
            Ok(written) if written == bytes.len() => return Ok(()),
            Ok(written) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "short v7 FRONTIER write: wrote {written} of {} bytes",
                        bytes.len()
                    ),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn write_v7_frontier_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::fail_point!("parallel_wal.v7_frontier_short_write", |_| {
        // The runtime file is O_DIRECT, which can reject a byte-sized write
        // before the short-write path runs. Reopen the same inode through procfs
        // without O_DIRECT so the test leaves a real partial frame behind.
        #[cfg(target_os = "linux")]
        {
            let buffered_path = format!("/proc/self/fd/{}", file.as_raw_fd());
            let buffered_file = File::options().write(true).open(buffered_path)?;
            buffered_file.write_at(&bytes[..1], offset)
        }

        #[cfg(not(target_os = "linux"))]
        {
            Ok(1)
        }
    });

    file.write_at(bytes, offset)
}

fn complete_v7_frontier_cycle(inner: &RuntimeInner, cycle: WalV7FrontierCycle) -> Result<()> {
    let ticket_end = cycle.reservation.ticket_end();
    let retained_marker_ticket = ticket_end
        .checked_sub(1)
        .context("v7 FRONTIER target has no ticket to retain its marker credit")?;
    let covered_reservations = {
        let mut admission = inner.admission.lock();
        let allocator = admission
            .v7_allocator
            .as_mut()
            .context("v7 Parallel runtime has no physical allocator")?;
        allocator.complete_frontier_sync(cycle.reservation.clone(), cycle.cursor)?;

        let mut covered = std::mem::take(&mut admission.v7_reservations);
        let later = covered.split_off(&ticket_end);
        ensure!(
            covered.contains_key(&retained_marker_ticket),
            "v7 FRONTIER target has no owned batch reservation"
        );
        for (ticket, reservation) in &mut covered {
            if *ticket != retained_marker_ticket {
                reservation.release_unused_frontier_credit()?;
            }
        }
        admission.v7_reservations = later;
        admission
            .v7_prefixes
            .retain(|prefix_end, _| *prefix_end > ticket_end);
        covered
    };

    for reservation in covered_reservations.into_values() {
        reservation
            .finish_after_io()
            .context("retire v7 Parallel transient buffer reservation")?;
    }
    Ok(())
}

#[cfg(feature = "bench")]
fn validate_sync_diagnostics_transition(
    enabled: bool,
    currently_enabled: bool,
    has_admitted_ticket: bool,
) -> Result<()> {
    ensure!(
        !enabled || currently_enabled || !has_admitted_ticket,
        "WAL sync diagnostics must be enabled before the first WAL ticket"
    );
    Ok(())
}

fn synchronize_written_prefix(sync_file: &File, inner: &RuntimeInner) -> Result<Option<Duration>> {
    let (target, durable, poison_ticket) = {
        let state = inner.durability.state.lock();
        let target = state
            .poison_ticket
            .map_or(state.written_frontier, |poison| {
                state.written_frontier.min(poison)
            });
        #[cfg(all(test, feature = "chaos-testing"))]
        if state.poison_ticket.is_some() && target > state.durable_frontier {
            crate::chaos::failpoint::note_parallel_wal_sync_with_poison();
        }
        (target, state.durable_frontier, state.poison_ticket)
    };
    if target <= durable {
        return Ok(None);
    }

    let frontier_cycle = if inner.admission.lock().v7_allocator.is_some() {
        let cycle = append_v7_frontier(sync_file, inner, target, durable, poison_ticket)?;
        if cycle.is_none() {
            // Retaining the preceding marker includes later DATA. Completion
            // of those already admitted tickets will make a larger target
            // eligible; no marker slot or sync is consumed while waiting.
            return Ok(None);
        }
        cycle
    } else {
        None
    };

    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::wal_crash_point("parallel_wal.before_fdatasync");
    #[cfg(all(test, feature = "chaos-testing"))]
    crate::chaos::failpoint::before_parallel_wal_fdatasync_call();
    let _sync_start = inner.sync_progress.begin_sync(target, durable);
    inner.sync_progress.start_sync_activity();
    let started_at = Instant::now();
    #[cfg(feature = "bench")]
    let syscall_started_at = Some(started_at);
    #[cfg(not(feature = "bench"))]
    let syscall_started_at = None;
    let (sync_error, syscall_finished_at) = loop {
        let error = fdatasync_file(sync_file).err();
        let call_finished_at = wal_sync_timestamp();
        if let Some(error) = error {
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break (Some(error), call_finished_at);
        }
        break (None, call_finished_at);
    };
    let sync_latency = started_at.elapsed();
    let (_sync_end, finished_at) = inner
        .sync_progress
        .finish_sync(syscall_started_at, syscall_finished_at);
    #[cfg(feature = "bench")]
    let duration_ns = finished_at
        .expect("benchmark WAL sync completion has a timestamp")
        .duration_since(started_at)
        .as_nanos() as u64;
    #[cfg(not(feature = "bench"))]
    let _ = finished_at;
    #[cfg(feature = "bench")]
    if let Some(profile) = inner.sync_progress.profile() {
        profile.record_wal_sync(_sync_end.groups_covered);
        profile.record_wal_fdatasync_ns(duration_ns);
        if profile.wal_sync_diagnostics_enabled() {
            profile.record_wal_sync_observation(crate::mem_table::WalSyncObservation {
                captured_target: target,
                written_frontier_start: _sync_end.written_frontier_start,
                written_frontier_end: _sync_end.written_frontier_end,
                write_sqes_submitted: _sync_end.write_sqes_submitted,
                write_cqes_completed: _sync_end.write_cqes_completed,
                groups_completed: _sync_end.groups_completed,
                groups_covered: _sync_end.groups_covered,
                duration_ns,
                succeeded: sync_error.is_none(),
            });
        }
    }
    let Some(error) = sync_error else {
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::wal_crash_point("parallel_wal.after_fdatasync");

        if let Some(cycle) = frontier_cycle
            && let Err(error) = complete_v7_frontier_cycle(inner, cycle)
        {
            poison_v7_runtime(
                inner,
                None,
                durable,
                format!("v7 Parallel post-sync frontier publication failed: {error:#}"),
            );
            return Err(error).context("complete v7 Parallel FRONTIER sync cycle");
        }

        let mut state = inner.durability.state.lock();
        let acknowledged = state
            .poison_ticket
            .map_or(target, |poison| target.min(poison));
        state.durable_frontier = state.durable_frontier.max(acknowledged);
        debug_assert!(state.durable_frontier <= state.written_frontier);
        if let Some(poison) = state.poison_ticket {
            debug_assert!(state.durable_frontier <= poison);
        }
        debug_assert!(state.written_frontier <= inner.admission.lock().next_ticket);
        inner.durability.notify_change();
        drop(state);
        inner.sync_progress.mark_durable(acknowledged);

        return Ok(Some(sync_latency));
    };

    let message = format!("fdatasync failed: {error}");
    if let Some(cycle) = frontier_cycle {
        poison_v7_runtime(inner, Some(cycle.reservation), durable, message);
        return Ok(Some(sync_latency));
    }
    let mut state = inner.durability.state.lock();
    let poison = state.durable_frontier;
    set_poison(&mut state, poison, message.clone());
    let mut admission = inner.admission.lock();
    admission.open = false;
    if admission
        .poison
        .as_ref()
        .is_none_or(|(ticket, _)| durable < *ticket)
    {
        admission.poison = Some((durable, message));
    }
    inner.buffer_budget.close();
    // Publish the admission poison before waking writers that are waiting on
    // this failed sync. Otherwise a waiter can return `Err` and race a new
    // batch into the still-open admission queue.
    inner.durability.notify_change();

    Ok(Some(sync_latency))
}

fn fdatasync_file(sync_file: &File) -> io::Result<()> {
    #[cfg(all(test, feature = "chaos-testing"))]
    crate::chaos::failpoint::fail_point!("parallel_wal.fdatasync_failure", |_| Err(
        io::Error::other("injected parallel WAL fdatasync failure")
    ));

    let result = unsafe { libc::fdatasync(sync_file.as_raw_fd()) };
    if result == 0 {
        return Ok(());
    }

    Err(io::Error::last_os_error())
}

#[inline]
fn wal_sync_timestamp() -> Option<std::time::Instant> {
    #[cfg(feature = "bench")]
    {
        Some(Instant::now())
    }
    #[cfg(not(feature = "bench"))]
    {
        None
    }
}

/// Preallocate space and extend `i_size` so later direct writes target an
/// already-sized range and avoid extending the WAL themselves.
/// Only allocate the new suffix; revisiting the existing extents adds work
/// as the WAL grows without reserving any additional space.
fn preallocate_runtime(inner: &RuntimeInner, end: u64) -> Result<()> {
    let _guard = inner.preallocation_lock.lock();
    preallocate(&inner.preallocator, end)
}

fn preallocate(file: &File, end: u64) -> Result<()> {
    ensure!(
        end <= MAX_WAL_FILE_SIZE,
        "WAL preallocation exceeds file cap"
    );
    let len = file.metadata()?.len();
    if end <= len {
        return Ok(());
    }

    let result = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            0,
            i64::try_from(len).context("WAL preallocation offset exceeds i64")?,
            i64::try_from(end - len).context("WAL preallocation length exceeds i64")?,
        )
    };
    if result == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)
    ) {
        file.set_len(end)
            .context("ftruncate fallback failed during WAL preallocation")?;
        return Ok(());
    }
    Err(error).context("fallocate failed during WAL preallocation")
}

fn round_up(value: u64, alignment: u64) -> Option<u64> {
    let remainder = value % alignment;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(alignment - remainder)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, os::unix::fs::FileExt, path::Path, sync::Arc};

    use crate::{
        pitr::{LIVE_WAL_V5_LIMITS, RecordedAt, WalBatch, WalEntry, manifest::ActiveBoundary},
        wal::v7::admission::{
            WalV7ReservationOwner, WalV7ResourceCharge, WalV7ResourceLedger, WalV7ResourceLimits,
        },
    };

    struct V7RuntimeTestWal {
        wal: Wal,
        owner: Arc<WalV7ReservationOwner>,
        anchor: ActiveBoundary,
    }

    fn v7_test_resource_limits() -> WalV7ResourceLimits {
        WalV7ResourceLimits {
            max_logical_wal_bytes: u64::MAX,
            max_source_spool_bytes: u64::MAX,
            max_recovery_workspace_bytes: u64::MAX,
            max_buffer_memory_bytes: u64::MAX,
            max_seal_index_bytes: u64::MAX,
        }
    }

    fn live_v7_test_wal(
        path: &Path,
        max_file_size: u64,
        limits: WalV7ResourceLimits,
    ) -> anyhow::Result<Option<V7RuntimeTestWal>> {
        use crate::{
            pitr::{ArchiveEpochId, ChainAnchor, SegmentId, TimelineId},
            tests::harness::is_io_uring_unavailable_error,
            wal::v7::{
                codec::{WAL_V7_HEADER_LEN, WalV7Header, encode_generation_zero_frontier},
                install::recover_and_install_active,
                recovery::WalV7RecoveryAuthority,
            },
        };

        let archive_epoch_id = ArchiveEpochId([0x22; 16]);
        let header = WalV7Header {
            timeline_id: TimelineId([0x11; 16]),
            archive_epoch_id,
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [0x44; 16],
        };
        let header_digest = header.digest()?;
        let mut empty_image = header.encode()?.to_vec();
        empty_image.extend_from_slice(&encode_generation_zero_frontier(header_digest)?);
        let anchor = ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: header.segment_id.0,
            incarnation: header.incarnation,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
        };
        std::fs::write(path, empty_image)?;
        let recovered = recover_and_install_active(
            path,
            WalV7RecoveryAuthority::Active {
                predecessor: header.predecessor,
                durable_anchors: vec![anchor],
            },
        )?;
        let ledger = WalV7ResourceLedger::new(limits)?;
        let owner = Arc::new(WalV7ReservationOwner::new(ledger, &recovered.installed)?);
        let wal = match Wal::from_v7_parallel(
            path,
            recovered.installed.runtime_seed()?,
            max_file_size,
            Some(Arc::clone(&owner)),
        ) {
            Ok(wal) => wal,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };

        Ok(Some(V7RuntimeTestWal { wal, owner, anchor }))
    }

    #[cfg(feature = "chaos-testing")]
    fn count_valid_v7_frontiers(path: &Path) -> anyhow::Result<usize> {
        use crate::wal::v7::codec::{
            WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7FrameKind, WalV7Header,
            decode_frame_structural,
        };

        let image = std::fs::read(path)?;
        let header = WalV7Header::decode(&image[..WAL_V7_HEADER_LEN])?;
        let header_digest = header.digest()?;
        let mut count = 0;
        for (index, frame) in image[WAL_V7_HEADER_LEN..]
            .as_chunks::<WAL_V7_FRAME_LEN>()
            .0
            .iter()
            .enumerate()
        {
            let offset = WAL_V7_HEADER_LEN as u64 + (index * WAL_V7_FRAME_LEN) as u64;
            if decode_frame_structural(frame, offset, &header_digest)
                .is_ok_and(|decoded| decoded.kind == WalV7FrameKind::Frontier)
            {
                count += 1;
            }
        }
        Ok(count)
    }

    fn v7_test_batch(commit_ts: u64, value_len: usize) -> WalBatch {
        WalBatch {
            commit_ts,
            recorded_at: RecordedAt { secs: 50, nanos: 0 },
            entries: vec![WalEntry::Put {
                key: format!("key-{commit_ts}").into_bytes(),
                value: vec![0x5A; value_len],
            }],
        }
    }

    fn complete_test_v7_data(
        inner: &super::RuntimeInner,
        batch: AdmittedBatch,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let AdmittedBatch {
            ticket,
            file_offset,
            aligned_len,
            buffer,
            ..
        } = batch;
        if error.is_none() {
            inner.preallocator.write_all_at(
                buffer
                    .buffer
                    .as_ref()
                    .expect("test owns the encoded DATA buffer")
                    .initialized_slice(0, aligned_len),
                file_offset,
            )?;
        }
        super::WorkerBuffer::retire(buffer, &inner.buffer_pool);
        super::process_group_result(
            inner,
            super::GroupWriteResult {
                group_id: ticket,
                tickets: ticket..ticket + 1,
                write_count: 1,
                write_bytes: aligned_len as u64,
                error,
            },
        );

        Ok(())
    }

    fn check_v7_frontier_recapture(failed_ticket: Option<u64>) -> anyhow::Result<()> {
        use crate::wal::v7::{
            install::recover_and_install_active, recovery::WalV7RecoveryAuthority,
        };

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("recapture.wal");
        let Some(V7RuntimeTestWal { wal, owner, anchor }) =
            live_v7_test_wal(&path, super::MAX_WAL_FILE_SIZE, v7_test_resource_limits())?
        else {
            return Ok(());
        };
        let runtime = wal.parallel_runtime.as_ref().expect("v7 Parallel runtime");
        // Hold the automatic packer so completion order and each intermediate
        // target are deterministic. Feed real encoded writes and their CQEs to
        // the coordinator state machine instead of relying on device timing.
        let mut packer = runtime.inner.packer_state.lock();
        let mut batches = (1..=3).map(|ts| v7_test_batch(ts, 10)).collect::<Vec<_>>();
        for (ticket, batch) in batches.iter().enumerate() {
            assert_eq!(
                wal.put_v7_batch_with_index(batch, LIVE_WAL_V5_LIMITS)?.0,
                ticket as u64
            );
        }
        let (mut pending, data_end) = {
            let mut admission = runtime.inner.admission.lock();
            (std::mem::take(&mut admission.queue), admission.admitted_end)
        };
        complete_test_v7_data(&runtime.inner, pending.pop_front().unwrap(), None)?;
        assert!(
            super::synchronize_written_prefix(&runtime.inner.preallocator, &runtime.inner)?
                .is_some()
        );
        runtime.wait_durable(0)?;
        let first_frontier = runtime
            .inner
            .admission
            .lock()
            .v7_allocator
            .as_ref()
            .unwrap()
            .last_frontier();
        assert_eq!(first_frontier.frontier.ticket_end, 1);
        assert_eq!(first_frontier.frame_offset, data_end);

        if failed_ticket == Some(3) {
            // A failed post-marker ticket does not prevent the pre-marker
            // tickets from completing an eligible frontier at T=3.
            batches.push(v7_test_batch(4, 10));
            assert_eq!(
                wal.put_v7_batch_with_index(batches.last().unwrap(), LIVE_WAL_V5_LIMITS)?
                    .0,
                3
            );
            let failed = runtime.inner.admission.lock().queue.pop_front().unwrap();
            complete_test_v7_data(
                &runtime.inner,
                failed,
                Some("injected later DATA failure".to_owned()),
            )?;
        }

        complete_test_v7_data(&runtime.inner, pending.pop_front().unwrap(), None)?;
        let tail_before_wait = runtime.logical_length();
        assert!(
            super::synchronize_written_prefix(&runtime.inner.preallocator, &runtime.inner)?
                .is_none()
        );
        assert_eq!(runtime.is_poisoned(), failed_ticket == Some(3));
        assert_eq!(
            runtime.inner.durability.state.lock().poison_ticket,
            failed_ticket.filter(|ticket| *ticket == 3)
        );
        assert_eq!(runtime.logical_length(), tail_before_wait);
        assert_eq!(runtime.inner.durability.state.lock().durable_frontier, 1);

        complete_test_v7_data(
            &runtime.inner,
            pending.pop_front().unwrap(),
            (failed_ticket == Some(2)).then(|| "injected third DATA failure".to_owned()),
        )?;
        let sync = super::synchronize_written_prefix(&runtime.inner.preallocator, &runtime.inner);
        let expected_ticket_end = if failed_ticket == Some(2) {
            assert!(sync.is_err());
            assert!(runtime.is_poisoned());
            assert!(runtime.wait_durable(1).is_err());
            assert!(runtime.wait_durable(2).is_err());
            runtime.wait_durable(0)?;
            1
        } else {
            assert!(sync?.is_some());
            assert_eq!(runtime.is_poisoned(), failed_ticket.is_some());
            runtime.wait_durable(2)?;
            if failed_ticket == Some(3) {
                assert!(runtime.wait_durable(3).is_err());
            }
            let frontier = runtime
                .inner
                .admission
                .lock()
                .v7_allocator
                .as_ref()
                .unwrap()
                .last_frontier();
            assert_eq!(frontier.frontier.ticket_end, 3);
            assert_eq!(
                frontier.frontier.durable_end,
                first_frontier.frame_offset + 4096
            );
            3
        };
        packer.next_ticket = wal.next_ticket();
        packer.reserved_end = runtime.logical_length();
        packer.preallocated_end = std::fs::metadata(&path)?.len();
        drop(packer);
        let closed = wal.close();
        if failed_ticket.is_some() {
            assert!(closed.is_err());
        } else {
            closed?;
        }
        assert_eq!(owner.snapshot().buffer_memory_bytes, 0);
        assert!(runtime.inner.admission.lock().v7_reservations.is_empty());
        let retained_charge = owner.snapshot();
        assert!(retained_charge.logical_wal_bytes > 0);
        assert_eq!(wal.close().is_err(), failed_ticket.is_some());
        assert_eq!(owner.snapshot(), retained_charge);
        drop(wal);

        let recovered = recover_and_install_active(
            &path,
            WalV7RecoveryAuthority::Active {
                predecessor: crate::pitr::ChainAnchor::Genesis {
                    archive_epoch_id: crate::pitr::ArchiveEpochId(anchor.archive_epoch_id),
                },
                durable_anchors: vec![anchor],
            },
        )?;
        assert_eq!(recovered.installed.next_ticket, expected_ticket_end);
        assert_eq!(
            recovered.installed.batches,
            batches[..expected_ticket_end as usize]
        );
        owner.retire_after_durable_cleanup()?;
        assert_eq!(owner.snapshot(), WalV7ResourceCharge::default());

        Ok(())
    }

    #[test]
    fn v7_frontier_recaptures_after_retained_marker_covers_pending_data() -> anyhow::Result<()> {
        check_v7_frontier_recapture(None)
    }

    #[test]
    fn v7_frontier_poison_terminates_waiters_when_recapture_is_impossible() -> anyhow::Result<()> {
        check_v7_frontier_recapture(Some(2))
    }

    #[test]
    fn v7_frontier_recaptures_healthy_prefix_below_a_later_poison() -> anyhow::Result<()> {
        check_v7_frontier_recapture(Some(3))
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_v7_parallel_close_retires_buffers_after_sync_failure() -> anyhow::Result<()> {
        use crate::chaos::failpoint::{self, FailScenario};

        let _scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sync-failure.wal");
        let Some(V7RuntimeTestWal { wal, owner, .. }) =
            live_v7_test_wal(&path, super::MAX_WAL_FILE_SIZE, v7_test_resource_limits())?
        else {
            return Ok(());
        };
        let (durable_ticket, _) =
            wal.put_v7_batch_with_index(&v7_test_batch(1, 10), LIVE_WAL_V5_LIMITS)?;
        wal.submit_and_commit(durable_ticket)?;
        assert_eq!(owner.snapshot().buffer_memory_bytes, 0);

        failpoint::cfg("parallel_wal.fdatasync_failure", "return")
            .expect("inject v7 Parallel sync failure");
        let (failed_ticket, _) =
            wal.put_v7_batch_with_index(&v7_test_batch(2, 10), LIVE_WAL_V5_LIMITS)?;
        let failed = wal.submit_and_commit(failed_ticket);
        failpoint::cfg("parallel_wal.fdatasync_failure", "off").expect("disable sync failure");
        assert!(failed.is_err());
        let before_close = owner.snapshot();
        assert!(before_close.buffer_memory_bytes > 0);
        wal.submit_and_commit(durable_ticket)?;

        assert!(wal.close().is_err());
        let after_close = owner.snapshot();
        assert_eq!(after_close.buffer_memory_bytes, 0);
        assert_eq!(
            after_close.logical_wal_bytes,
            before_close.logical_wal_bytes
        );
        assert_eq!(
            after_close.source_spool_bytes,
            before_close.source_spool_bytes
        );
        assert_eq!(after_close.seal_index_bytes, before_close.seal_index_bytes);
        assert!(after_close.logical_wal_bytes > 0);
        assert!(
            wal.parallel_runtime
                .as_ref()
                .unwrap()
                .inner
                .admission
                .lock()
                .v7_reservations
                .is_empty()
        );
        assert!(wal.close().is_err());
        assert_eq!(owner.snapshot(), after_close);

        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_v7_parallel_failed_frontier_cycle_never_appends_again() -> anyhow::Result<()> {
        use crate::chaos::failpoint::{self, FailScenario};

        let _scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("failed-frontier-cycle.wal");
        let Some(V7RuntimeTestWal { wal, .. }) =
            live_v7_test_wal(&path, super::MAX_WAL_FILE_SIZE, v7_test_resource_limits())?
        else {
            return Ok(());
        };

        let (durable_ticket, _) =
            wal.put_v7_batch_with_index(&v7_test_batch(1, 10), LIVE_WAL_V5_LIMITS)?;
        wal.submit_and_commit(durable_ticket)?;
        let before_failure = count_valid_v7_frontiers(&path)?;
        assert_eq!(before_failure, 2, "genesis plus the first durable marker");

        failpoint::cfg("parallel_wal.fdatasync_failure", "return")
            .expect("inject FRONTIER sync failure");
        let (failed_ticket, _) =
            wal.put_v7_batch_with_index(&v7_test_batch(2, 10), LIVE_WAL_V5_LIMITS)?;
        let failed = wal.submit_and_commit(failed_ticket);
        failpoint::cfg("parallel_wal.fdatasync_failure", "off")
            .expect("disable FRONTIER sync failure");
        assert!(failed.is_err());

        let after_failure = count_valid_v7_frontiers(&path)?;
        assert_eq!(
            after_failure,
            before_failure + 1,
            "the failed sync may leave its complete marker in the file"
        );
        assert!(
            wal.put_v7_batch_with_index(&v7_test_batch(3, 10), LIVE_WAL_V5_LIMITS)
                .is_err(),
            "a poisoned frontier cycle must close admission"
        );
        assert_eq!(
            count_valid_v7_frontiers(&path)?,
            after_failure,
            "rejected writes must not append another FRONTIER"
        );

        assert!(wal.submit_and_commit(durable_ticket).is_ok());
        assert!(wal.close().is_err());
        assert_eq!(
            count_valid_v7_frontiers(&path)?,
            after_failure,
            "retry and close after an uncertain marker sync must not append another FRONTIER"
        );

        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_v7_parallel_short_frontier_write_poisons_cycle() -> anyhow::Result<()> {
        use crate::chaos::failpoint::{self, FailScenario};
        use crate::{
            pitr::{ArchiveEpochId, ChainAnchor},
            wal::v7::{install::recover_and_install_active, recovery::WalV7RecoveryAuthority},
        };

        let _scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("short-frontier-write.wal");
        let Some(V7RuntimeTestWal { wal, anchor, .. }) =
            live_v7_test_wal(&path, super::MAX_WAL_FILE_SIZE, v7_test_resource_limits())?
        else {
            return Ok(());
        };

        let before_failure = count_valid_v7_frontiers(&path)?;
        assert_eq!(before_failure, 1, "genesis is the only initial marker");

        failpoint::cfg("parallel_wal.v7_frontier_short_write", "return")
            .expect("inject a positive short FRONTIER write");
        let (ticket, _) = wal.put_v7_batch_with_index(&v7_test_batch(1, 10), LIVE_WAL_V5_LIMITS)?;
        let failed = wal.submit_and_commit(ticket);
        failpoint::cfg("parallel_wal.v7_frontier_short_write", "off")
            .expect("disable short FRONTIER write");
        let error = failed.expect_err("a short FRONTIER write must fail the durability cycle");
        assert!(
            format!("{error:#}").contains("short v7 FRONTIER write"),
            "unexpected failure: {error:#}"
        );

        assert_eq!(
            count_valid_v7_frontiers(&path)?,
            before_failure,
            "the injected short write does not leave a complete FRONTIER"
        );
        assert!(
            wal.put_v7_batch_with_index(&v7_test_batch(2, 10), LIVE_WAL_V5_LIMITS)
                .is_err(),
            "a short FRONTIER write must poison further admission"
        );
        assert_eq!(count_valid_v7_frontiers(&path)?, before_failure);
        assert!(wal.close().is_err());
        assert_eq!(
            count_valid_v7_frontiers(&path)?,
            before_failure,
            "close after a short write must not append another FRONTIER"
        );
        drop(wal);

        let recovered = recover_and_install_active(
            &path,
            WalV7RecoveryAuthority::Active {
                predecessor: ChainAnchor::Genesis {
                    archive_epoch_id: ArchiveEpochId(anchor.archive_epoch_id),
                },
                durable_anchors: vec![anchor],
            },
        )?;
        assert_eq!(recovered.installed.active_boundary, anchor);
        assert_eq!(recovered.installed.next_ticket, 0);
        assert!(
            recovered.installed.batches.is_empty(),
            "recovery must discard the DATA covered only by the partial FRONTIER"
        );

        Ok(())
    }

    #[test]
    fn v7_unresolved_buffer_keeps_its_charge_after_close_and_handle_drop() -> anyhow::Result<()> {
        use crate::wal::v7::admission::WalV7ReservationError;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("unresolved-buffer.wal");
        let batch = v7_test_batch(1, 10);
        let encoded_len = super::prepare_logical_batch(&batch, LIVE_WAL_V5_LIMITS)?.encoded_len();
        let charge = WalV7ResourceCharge::for_batch(encoded_len as u64)?;
        let preparation_bytes =
            super::CanonicalWalEntryStream::preparation_buffer_bytes(&batch, LIVE_WAL_V5_LIMITS)?;
        let mut limits = v7_test_resource_limits();
        limits.max_buffer_memory_bytes = charge.buffer_memory_bytes + preparation_bytes;
        let Some(V7RuntimeTestWal { wal, owner, .. }) =
            live_v7_test_wal(&path, super::MAX_WAL_FILE_SIZE, limits)?
        else {
            return Ok(());
        };
        let runtime = wal.parallel_runtime.as_ref().unwrap();
        let mut packer = runtime.inner.packer_state.lock();
        let (ticket, _) = wal.put_v7_batch_with_index(&batch, LIVE_WAL_V5_LIMITS)?;
        let AdmittedBatch {
            buffer: unresolved_buffer,
            file_offset,
            aligned_len,
            ..
        } = runtime.inner.admission.lock().queue.pop_front().unwrap();
        let data_end = file_offset + aligned_len as u64;
        super::preallocate_runtime(&runtime.inner, data_end)?;
        // Retain the allocation just as WorkerCore::drop does for an ambiguous
        // submission. No SQE is actually submitted here, allowing the test to
        // release it safely after checking accounting across close/retirement.
        super::process_group_result(
            &runtime.inner,
            super::GroupWriteResult {
                group_id: 0,
                tickets: ticket..ticket + 1,
                write_count: 1,
                write_bytes: aligned_len as u64,
                error: Some("ambiguous DATA submission".to_owned()),
            },
        );
        packer.next_ticket = ticket + 1;
        packer.reserved_end = data_end;
        packer.preallocated_end = data_end;
        drop(packer);

        let before_close = owner.snapshot();
        assert!(wal.close().is_err());
        assert!(runtime.inner.admission.lock().v7_reservations.is_empty());
        assert_eq!(owner.snapshot(), before_close);
        drop(wal);
        assert_eq!(owner.snapshot(), before_close);
        assert_eq!(
            owner.reserve_batch(encoded_len as u64).err(),
            Some(WalV7ReservationError::BufferMemoryLimit)
        );
        assert_eq!(owner.snapshot(), before_close);

        std::fs::remove_file(&path)?;
        std::fs::File::open(directory.path())?.sync_all()?;
        owner.retire_after_durable_cleanup()?;
        assert_eq!(owner.snapshot(), before_close);
        drop(unresolved_buffer);
        assert_eq!(owner.snapshot(), WalV7ResourceCharge::default());

        Ok(())
    }

    #[test]
    fn v7_admission_uses_exact_buffers_and_never_retires_them_into_the_v4_pool()
    -> anyhow::Result<()> {
        use crate::wal::v7::codec::WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY;

        let directory = tempfile::tempdir()?;
        let large_value_len = 64 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY - 128;
        for (value_len, expected_capacity) in
            [(10, 4096), (large_value_len, super::BUFFER_POOL_BUF_SIZE)]
        {
            let path = directory.path().join(format!("buffer-{value_len}.wal"));
            let mut batch = v7_test_batch(1, value_len.min(LIVE_WAL_V5_LIMITS.max_value_bytes));
            if value_len > LIVE_WAL_V5_LIMITS.max_value_bytes {
                batch.entries = (0..4)
                    .map(|index| WalEntry::Put {
                        key: format!("key-{index}").into_bytes(),
                        value: vec![0x5A; value_len / 4],
                    })
                    .collect();
            }
            let encoded_len =
                super::prepare_logical_batch(&batch, LIVE_WAL_V5_LIMITS)?.encoded_len();
            let charge = WalV7ResourceCharge::for_batch(encoded_len as u64)?;
            let preparation_bytes = super::CanonicalWalEntryStream::preparation_buffer_bytes(
                &batch,
                LIVE_WAL_V5_LIMITS,
            )?;
            let mut limits = v7_test_resource_limits();
            limits.max_buffer_memory_bytes = charge.buffer_memory_bytes + preparation_bytes;
            let Some(V7RuntimeTestWal { wal, owner, .. }) =
                live_v7_test_wal(&path, super::MAX_WAL_FILE_SIZE, limits)?
            else {
                return Ok(());
            };
            let runtime = wal.parallel_runtime.as_ref().unwrap();
            let packer = runtime.inner.packer_state.lock();
            let (ticket, _) = wal.put_v7_batch_with_index(&batch, LIVE_WAL_V5_LIMITS)?;
            let admission = runtime.inner.admission.lock();
            let queued = admission.queue.front().unwrap();
            assert_eq!(super::WorkerBuffer::cap(&queued.buffer), expected_capacity);
            assert_eq!(queued.aligned_len, expected_capacity);
            assert!(!queued.buffer.poolable);
            assert_eq!(
                runtime.inner.buffer_budget.state.lock().active_bytes,
                expected_capacity as u64
            );
            assert_eq!(
                owner.snapshot().buffer_memory_bytes,
                charge.buffer_memory_bytes
            );
            drop(admission);
            drop(packer);
            wal.submit_and_commit(ticket)?;
            wal.close()?;
            assert!(runtime.inner.buffer_pool.is_empty());
            assert_eq!(owner.snapshot().buffer_memory_bytes, 0);
        }

        Ok(())
    }

    #[test]
    fn recovered_v7_runtime_keeps_its_metadata_and_cursors_paused() {
        use std::{
            fs::File,
            os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
            sync::Arc,
        };

        use sha2::{Digest, Sha256};

        use super::{ParallelWalRuntime, ParallelWalRuntimeStart};
        use crate::{
            pitr::{ArchiveEpochId, ChainAnchor, RecordedAt, SegmentId, TimelineId},
            tests::harness::is_io_uring_unavailable_error,
            wal::v7::{
                allocator::WalV7FrontierCursor,
                codec::{WalV7Frontier, WalV7Header},
                install::{WalV7RuntimeMetadata, WalV7RuntimeSeed},
            },
        };

        const DATA_START: u64 = 2 * 4096;
        const APPEND_OFFSET: u64 = 9 * 4096;
        const FRONTIER_OFFSET: u64 = 8 * 4096;
        const NEXT_TICKET: u64 = 6;
        let mut logical_hasher = Sha256::new();
        logical_hasher.update(b"accepted logical prefix");
        let recovered_logical_hasher = logical_hasher.clone();
        let prefix_digest = logical_hasher.clone().finalize().into();
        let mut physical_hasher = Sha256::new();
        physical_hasher.update(b"normalized physical image");
        let physical_digest: [u8; 32] = physical_hasher.clone().finalize().into();
        let last_recorded_at = Some(RecordedAt { secs: 60, nanos: 0 });
        let frontier_cursor = WalV7FrontierCursor {
            frontier: WalV7Frontier {
                generation: 3,
                ticket_end: NEXT_TICKET,
                durable_end: FRONTIER_OFFSET,
                last_commit_ts: 60,
                prefix_digest,
                previous_frontier_offset: 6 * 4096,
                previous_frontier_digest: [0xB2; 32],
            },
            frame_offset: FRONTIER_OFFSET,
            frame_digest: [0xC3; 32],
        };

        let directory = tempfile::tempdir().expect("create WAL directory");
        let path = directory.path().join("recovered-v7.wal");
        let archive_epoch_id = ArchiveEpochId([0x22; 16]);
        let header = WalV7Header {
            timeline_id: TimelineId([0x11; 16]),
            archive_epoch_id,
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [0x44; 16],
        };
        let header_bytes = header.encode().expect("encode v7 WAL header");
        let header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
        let file = File::create(&path).expect("create recovered WAL image");
        file.write_all_at(&header_bytes, 0)
            .expect("write recovered WAL header");
        file.set_len(APPEND_OFFSET)
            .expect("size recovered WAL image");
        drop(file);

        let direct_file = File::options()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .expect("open recovered WAL with direct I/O");
        let direct_metadata = direct_file.metadata().expect("inspect recovered WAL inode");
        let direct_file = Arc::new(direct_file);
        let make_seed = || WalV7RuntimeSeed {
            data_start: DATA_START,
            append_offset: APPEND_OFFSET,
            next_ticket: NEXT_TICKET,
            header_digest,
            installed_dev: direct_metadata.dev(),
            installed_ino: direct_metadata.ino(),
            frontier_cursor,
            metadata: WalV7RuntimeMetadata {
                logical_hasher: logical_hasher.clone(),
                physical_hasher: physical_hasher.clone(),
                physical_hashed_end: APPEND_OFFSET,
                last_recorded_at,
            },
        };
        let make_start =
            || ParallelWalRuntimeStart::v7(make_seed(), crate::wal::MAX_WAL_FILE_SIZE, None);
        let configured_limit = APPEND_OFFSET + 4096;
        let limited_start = ParallelWalRuntimeStart::v7(make_seed(), configured_limit, None)
            .expect("accept configured v7 segment limit");
        assert_eq!(limited_start.max_file_size, configured_limit);
        assert_eq!(
            limited_start
                .v7_allocator
                .as_ref()
                .expect("limited start owns its v7 allocator")
                .data_capacity_status(1)
                .expect("classify limited v7 segment capacity"),
            crate::wal::v7::allocator::WalV7DataCapacityStatus::RotationRequired
        );
        assert!(ParallelWalRuntimeStart::v7(make_seed(), APPEND_OFFSET - 4096, None).is_err());

        direct_file
            .set_len(configured_limit + 4096)
            .expect("extend recovered WAL beyond configured segment limit");
        let configured_limit_error = limited_start
            .validate_v7_file_handles(&direct_file, &direct_file, &direct_file)
            .expect_err("reject a WAL image larger than its configured segment limit");
        assert!(
            format!("{configured_limit_error:#}")
                .contains("exceeds the configured maximum physical size")
        );
        direct_file
            .set_len(APPEND_OFFSET)
            .expect("restore recovered WAL length after configured-cap check");
        let mut start = limited_start;
        start
            .v7_metadata
            .as_mut()
            .expect("v7 startup metadata")
            .physical_hashed_end -= 4096;
        assert!(start.validate().is_err());
        start
            .v7_metadata
            .as_mut()
            .expect("v7 startup metadata")
            .physical_hashed_end = APPEND_OFFSET;
        start
            .v7_metadata
            .as_mut()
            .expect("v7 startup metadata")
            .logical_hasher
            .update(b"unaccepted suffix");
        assert!(start.validate().is_err());
        start
            .v7_metadata
            .as_mut()
            .expect("v7 startup metadata")
            .logical_hasher = recovered_logical_hasher;
        start
            .v7_metadata
            .as_mut()
            .expect("v7 startup metadata")
            .last_recorded_at = None;
        assert!(start.validate().is_err());
        start
            .v7_metadata
            .as_mut()
            .expect("v7 startup metadata")
            .last_recorded_at = last_recorded_at;

        let wrong_sync_path = directory.path().join("different-inode.wal");
        let wrong_sync_file =
            File::create(&wrong_sync_path).expect("create mismatched sync handle inode");
        wrong_sync_file
            .set_len(APPEND_OFFSET)
            .expect("size mismatched sync handle inode");
        let wrong_sync_file = Arc::new(wrong_sync_file);

        let assert_start_rejected = |expected: &str| {
            let error = match ParallelWalRuntime::spawn(
                Arc::clone(&direct_file),
                Arc::clone(&direct_file),
                Arc::clone(&direct_file),
                make_start().expect("validate recovered runtime cursor for length check"),
            ) {
                Ok(runtime) => {
                    runtime.close().expect("close unexpectedly started runtime");
                    panic!("v7 runtime accepted a WAL file with an invalid physical length");
                }
                Err(error) => error,
            };
            assert!(
                format!("{error:#}").contains(expected),
                "unexpected invalid-length error: {error:#}"
            );
        };

        direct_file
            .set_len(APPEND_OFFSET - 4096)
            .expect("truncate recovered WAL below append cursor");
        assert_start_rejected("v7 WAL worker handle is shorter than the installed append cursor");
        assert_eq!(
            direct_file
                .metadata()
                .expect("inspect truncated recovered WAL")
                .len(),
            APPEND_OFFSET - 4096
        );
        direct_file
            .set_len(APPEND_OFFSET)
            .expect("restore recovered WAL length after short-file check");

        direct_file
            .set_len(crate::wal::MAX_WAL_FILE_SIZE + 4096)
            .expect("extend recovered WAL beyond the segment cap");
        assert_start_rejected("v7 WAL worker handle exceeds the configured maximum physical size");
        assert_eq!(
            direct_file
                .metadata()
                .expect("inspect oversized recovered WAL")
                .len(),
            crate::wal::MAX_WAL_FILE_SIZE + 4096
        );
        direct_file
            .set_len(APPEND_OFFSET)
            .expect("restore recovered WAL length after cap check");

        let alternate_header = WalV7Header {
            incarnation: [0x55; 16],
            ..header
        }
        .encode()
        .expect("encode alternate valid v7 WAL header");
        let buffered_file = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open recovered WAL header for mismatch check");
        buffered_file
            .write_all_at(&alternate_header, 0)
            .expect("replace header with another valid v7 header");
        let error = match ParallelWalRuntime::spawn(
            Arc::clone(&direct_file),
            Arc::clone(&direct_file),
            Arc::clone(&direct_file),
            make_start().expect("validate recovered runtime cursor for header check"),
        ) {
            Ok(runtime) => {
                runtime.close().expect("close unexpectedly started runtime");
                panic!("v7 runtime accepted a different immutable WAL header");
            }
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("v7 WAL immutable header digest does not match"),
            "unexpected mismatched-header error: {error:#}"
        );
        buffered_file
            .write_all_at(&header_bytes, 0)
            .expect("restore recovered WAL header after mismatch check");

        let error = match ParallelWalRuntime::spawn(
            Arc::clone(&direct_file),
            Arc::clone(&wrong_sync_file),
            Arc::clone(&direct_file),
            make_start().expect("validate recovered runtime cursor for mismatch check"),
        ) {
            Ok(runtime) => {
                runtime.close().expect("close unexpectedly started runtime");
                panic!("v7 runtime accepted a sync handle for a different inode");
            }
            Err(error) => error,
        };
        assert!(
            format!("{error:#}")
                .contains("v7 WAL sync handle does not refer to the installed Active inode"),
            "unexpected mismatched-inode error: {error:#}"
        );
        assert_eq!(
            direct_file
                .metadata()
                .expect("inspect installed WAL after failed startup")
                .len(),
            APPEND_OFFSET
        );
        assert_eq!(
            wrong_sync_file
                .metadata()
                .expect("inspect mismatched inode after failed startup")
                .len(),
            APPEND_OFFSET
        );

        let runtime = match ParallelWalRuntime::spawn(
            Arc::clone(&direct_file),
            Arc::clone(&direct_file),
            direct_file,
            start,
        ) {
            Ok(runtime) => runtime,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return;
            }
            Err(error) => panic!("failed to start recovered v7 runtime: {error:#}"),
        };

        assert_eq!(runtime.inner.max_file_size, configured_limit);
        {
            let admission = runtime.inner.admission.lock();
            assert!(!admission.open);
            assert_eq!(admission.next_ticket, NEXT_TICKET);
            assert_eq!(admission.admitted_end, APPEND_OFFSET);
            assert_eq!(
                admission
                    .v7_allocator
                    .as_ref()
                    .expect("v7 runtime owns its physical allocator")
                    .last_frontier(),
                frontier_cursor
            );
            assert_eq!(
                admission
                    .v7_allocator
                    .as_ref()
                    .expect("attached v7 allocator retains the configured cap")
                    .data_capacity_status(1)
                    .expect("classify attached v7 segment capacity"),
                crate::wal::v7::allocator::WalV7DataCapacityStatus::RotationRequired
            );
        }
        {
            let packer = runtime.inner.packer_state.lock();
            assert_eq!(packer.next_ticket, NEXT_TICKET);
            assert_eq!(packer.reserved_end, APPEND_OFFSET);
            assert_eq!(packer.preallocated_end, APPEND_OFFSET);
            assert!(packer.initializer.is_none());
        }
        {
            let durability = runtime.inner.durability.state.lock();
            assert_eq!(durability.written_frontier, NEXT_TICKET);
            assert_eq!(durability.durable_frontier, NEXT_TICKET);
        }
        {
            let admission = runtime.inner.admission.lock();
            let metadata = admission
                .v7_metadata
                .as_ref()
                .expect("runtime retains v7 metadata");
            let logical_digest: [u8; 32] = metadata.logical_hasher.clone().finalize().into();
            let retained_physical_digest: [u8; 32] =
                metadata.physical_hasher.clone().finalize().into();
            assert_eq!(logical_digest, prefix_digest);
            assert_eq!(retained_physical_digest, physical_digest);
            assert_eq!(metadata.physical_hashed_end, APPEND_OFFSET);
            assert_eq!(metadata.last_recorded_at, last_recorded_at);
        }
        assert!(runtime.inner.buffer_pool.is_empty());
        assert_eq!(
            std::fs::metadata(&path)
                .expect("inspect recovered WAL image")
                .len(),
            APPEND_OFFSET
        );
        #[cfg(feature = "bench")]
        {
            let profile = crate::mem_table::WriteProfile::default();
            runtime
                .set_wal_sync_diagnostics_enabled(&profile, true)
                .expect("enable diagnostics before any post-recovery ticket is admitted");
            assert!(profile.wal_sync_diagnostics_enabled());
        }
        assert!(runtime.wait_durable(NEXT_TICKET - 1).is_ok());
        assert!(runtime.wait_durable(NEXT_TICKET).is_err());
        runtime.close().expect("close paused recovered runtime");
    }

    #[test]
    fn v7_packer_preallocation_respects_small_segment_limits() -> anyhow::Result<()> {
        use crate::wal::v7::{
            codec::WAL_V7_FRAME_LEN, install::recover_and_install_active,
            leader::WalV7LeaderCapacityError, recovery::WalV7RecoveryAuthority,
        };

        let directory = tempfile::tempdir()?;
        for extra_bytes in [0, 1] {
            let path = directory
                .path()
                .join(format!("small-cap-{extra_bytes}.wal"));
            let configured_limit = 4 * WAL_V7_FRAME_LEN as u64 + extra_bytes;
            assert!(configured_limit < super::PREALLOC_BLOCK);
            let mut limits = v7_test_resource_limits();
            limits.max_logical_wal_bytes = 2 * WAL_V7_FRAME_LEN as u64;
            limits.max_source_spool_bytes = 2 * WAL_V7_FRAME_LEN as u64;
            limits.max_recovery_workspace_bytes = 0;
            let Some(V7RuntimeTestWal { wal, owner, anchor }) =
                live_v7_test_wal(&path, configured_limit, limits)?
            else {
                return Ok(());
            };
            let runtime = wal.parallel_runtime.as_ref().expect("v7 Parallel runtime");
            assert_eq!(runtime.inner.max_file_size, configured_limit);
            let batch = v7_test_batch(1, 10);
            let (ticket, first_data_offset) =
                wal.put_v7_batch_with_index(&batch, LIVE_WAL_V5_LIMITS)?;
            assert_eq!(
                (ticket, first_data_offset),
                (0, 2 * WAL_V7_FRAME_LEN as u64)
            );
            wal.submit_and_commit(ticket)?;
            assert_eq!(
                owner.snapshot().logical_wal_bytes,
                2 * WAL_V7_FRAME_LEN as u64
            );
            assert_eq!(std::fs::metadata(&path)?.len(), configured_limit);
            // A full persistent ledger must not mask the segment's actionable
            // rotation result or consume another ticket/offset.
            let error = wal
                .put_v7_batch_with_index(&v7_test_batch(2, 10), LIVE_WAL_V5_LIMITS)
                .expect_err("the next batch requires rotation");
            assert_eq!(
                error.downcast_ref::<WalV7LeaderCapacityError>(),
                Some(&WalV7LeaderCapacityError::RotationRequired)
            );
            assert_eq!(wal.next_ticket(), 1);
            assert!(!wal.v7_requires_recovery());
            wal.close()?;
            drop(wal);
            assert_eq!(std::fs::metadata(&path)?.len(), configured_limit);
            let recovered = recover_and_install_active(
                &path,
                WalV7RecoveryAuthority::Active {
                    predecessor: crate::pitr::ChainAnchor::Genesis {
                        archive_epoch_id: crate::pitr::ArchiveEpochId(anchor.archive_epoch_id),
                    },
                    durable_anchors: vec![anchor],
                },
            )?;
            assert_eq!(recovered.installed.batches, vec![batch]);
            assert_eq!(recovered.installed.next_ticket, 1);
        }

        Ok(())
    }

    #[test]
    fn admitted_ticket_keeps_its_outcome_when_later_group_packing_fails() {
        use std::{sync::Arc, thread, time::Duration};

        use crossbeam_skiplist::SkipMap;

        use super::{ExtentInitializer, PREALLOC_BLOCK, WAL_HEADER_END};
        use crate::{tests::harness::is_io_uring_unavailable_error, wal::WalIoMode};

        let directory = tempfile::tempdir().expect("create WAL directory");
        let path = directory.path().join("packing-failure.wal");
        let wal = match Wal::create_with_io_mode(&path, WalIoMode::Parallel) {
            Ok(wal) => Arc::new(wal),
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return;
            }
            Err(error) => panic!("failed to create parallel WAL: {error:#}"),
        };
        let runtime = wal.parallel_runtime.as_ref().expect("parallel runtime");
        let (prepared_tx, prepared_rx) = crossbeam_channel::bounded(1);
        let (requests_tx, _requests_rx) = crossbeam_channel::unbounded();
        {
            let mut packer = runtime.inner.packer_state.lock();
            if let Some(mut initializer) = packer.initializer.take() {
                initializer.close().expect("join extent initializer");
            }
            super::preallocate(&runtime.inner.preallocator, PREALLOC_BLOCK)
                .expect("prepare the test extent");
            // Pause packing the first group after taking its ticket, without
            // blocking later writers from admitting their ready buffers.
            packer.initializer = Some(ExtentInitializer {
                requests: Some(requests_tx),
                completions: prepared_rx,
                join: None,
                ready_end: WAL_HEADER_END,
            });
        }
        let first_wal = Arc::clone(&wal);
        let first_writer = thread::spawn(move || {
            first_wal.put_batch(&[(b"prefix".as_slice(), b"value".as_slice())], 1)
        });
        let started = std::time::Instant::now();
        loop {
            let admission = runtime.inner.admission.lock();
            if admission.next_ticket == 1 && admission.queue.is_empty() {
                break;
            }
            drop(admission);
            assert!(started.elapsed() < Duration::from_secs(5));
            thread::yield_now();
        }
        let later_tickets = (2..=9)
            .map(|commit_ts| {
                wal.put_batch(&[(b"later".as_slice(), b"value".as_slice())], commit_ts)
                    .expect("admit later ticket while the first group is packing")
            })
            .collect::<Vec<_>>();
        // Force the next group's packing to fail after the first group has
        // been submitted. Its poison boundary must not change ticket zero.
        runtime
            .inner
            .admission
            .lock()
            .queue
            .front_mut()
            .expect("later batch remains queued")
            .file_offset += 4096;
        prepared_tx.send(Ok(PREALLOC_BLOCK)).expect("resume packer");
        let first_ticket = first_writer
            .join()
            .expect("first writer joins")
            .expect("packing failure cannot retract an admitted ticket");
        assert_eq!(first_ticket, 0);
        wal.submit_and_commit(first_ticket)
            .expect("prefix before the packing failure becomes durable");
        for ticket in later_tickets {
            assert!(wal.submit_and_commit(ticket).is_err());
        }
        assert!(wal.close().is_err());
        drop(wal);

        let skiplist = Arc::new(SkipMap::new());
        let (recovered, max_ts) = Wal::recover(&path, &skiplist).expect("recover durable prefix");
        assert_eq!(max_ts, 1);
        assert_eq!(skiplist.len(), 1);
        assert_eq!(
            skiplist.get(b"prefix".as_slice()).unwrap().value().as_ref(),
            b"value"
        );
        assert!(skiplist.get(b"later".as_slice()).is_none());
        recovered.close().expect("close recovered WAL");
    }

    #[test]
    fn extent_lookahead_stops_at_file_cap_and_rejects_larger_targets() {
        use std::{os::unix::fs::FileExt, sync::Arc};

        let file = Arc::new(tempfile::tempfile().unwrap());
        let start = super::MAX_WAL_FILE_SIZE - 4096;
        file.set_len(start).unwrap();
        file.write_all_at(b"batch", start - 5).unwrap();
        let mut initializer = super::ExtentInitializer::spawn(Arc::clone(&file), start).unwrap();
        assert!(
            initializer
                .prepare(super::MAX_WAL_FILE_SIZE + 4096, true)
                .is_err()
        );
        initializer.prepare(super::MAX_WAL_FILE_SIZE, true).unwrap();
        initializer.close().unwrap();
        assert_eq!(file.metadata().unwrap().len(), super::MAX_WAL_FILE_SIZE);
        let mut batch = [0; 5];
        file.read_exact_at(&mut batch, start - 5).unwrap();
        assert_eq!(&batch, b"batch");
    }

    #[test]
    fn extent_lookahead_preserves_previously_written_batches() {
        use std::{os::unix::fs::FileExt, sync::Arc};

        let file = Arc::new(tempfile::tempfile().unwrap());
        file.set_len(super::WAL_HEADER_END).unwrap();
        file.write_all_at(b"header", 0).unwrap();
        let mut initializer =
            super::ExtentInitializer::spawn(Arc::clone(&file), super::WAL_HEADER_END).unwrap();
        initializer.prepare(super::PREALLOC_BLOCK, true).unwrap();
        file.write_all_at(b"batch", super::PREALLOC_BLOCK - 5)
            .unwrap();
        initializer
            .prepare(3 * super::PREALLOC_BLOCK, true)
            .unwrap();
        initializer.close().unwrap();
        let mut bytes = [0; 6];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"header");
        file.read_exact_at(&mut bytes[..5], super::PREALLOC_BLOCK - 5)
            .unwrap();
        assert_eq!(&bytes[..5], b"batch");
        file.read_exact_at(&mut bytes, 3 * super::PREALLOC_BLOCK - 6)
            .unwrap();
        assert_eq!(bytes, [0; 6]);
    }

    #[test]
    fn extent_preparation_never_zero_fills_a_skipped_prefix_after_small_writes_resume() {
        use std::{os::unix::fs::FileExt, sync::Arc};

        let file = Arc::new(tempfile::tempfile().unwrap());
        let block = super::PREALLOC_BLOCK;
        file.set_len(5 * block).unwrap();
        // Markers prove allocation-only requests don't write zeros, rather
        // than merely reading as zeros from an unwritten extent.
        file.write_all_at(b"large", block + 4096).unwrap();
        file.write_all_at(b"mixed", 2 * block + 4096).unwrap();
        file.write_all_at(b"stale", 4 * block - 5).unwrap();
        let mut initializer =
            super::ExtentInitializer::spawn(Arc::clone(&file), super::WAL_HEADER_END).unwrap();

        initializer.prepare(block, false).unwrap();
        initializer.prepare(2 * block, false).unwrap();
        initializer.prepare(3 * block, true).unwrap();
        initializer.prepare(4 * block, true).unwrap();
        initializer.close().unwrap();

        let mut bytes = [0; 5];
        file.read_exact_at(&mut bytes, block + 4096).unwrap();
        assert_eq!(&bytes, b"large");
        file.read_exact_at(&mut bytes, 2 * block + 4096).unwrap();
        assert_eq!(&bytes, b"mixed");
        file.read_exact_at(&mut bytes, 4 * block - 5).unwrap();
        assert_eq!(bytes, [0; 5]);
    }

    #[test]
    fn unused_extent_lookahead_failure_does_not_fail_close() {
        use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(super::PREALLOC_BLOCK).unwrap();
        file.as_file().write_all_at(b"batch", 4096).unwrap();
        let readonly = Arc::new(File::open(file.path()).unwrap());
        let mut initializer =
            super::ExtentInitializer::spawn(Arc::clone(&readonly), super::PREALLOC_BLOCK).unwrap();

        // The prepared prefix is available. Only the unused next extent fails
        // because its descriptor cannot allocate or write additional bytes.
        initializer.prepare(super::PREALLOC_BLOCK, true).unwrap();
        initializer
            .close()
            .expect("unused lookahead cannot fail close");

        let mut batch = [0; 5];
        readonly.read_exact_at(&mut batch, 4096).unwrap();
        assert_eq!(&batch, b"batch");
    }

    #[test]
    fn unused_extent_lookahead_failure_preserves_durable_wal_close_and_recovery() {
        use std::{fs::File, sync::Arc};

        use crossbeam_skiplist::SkipMap;

        use crate::{tests::harness::is_io_uring_unavailable_error, wal::WalIoMode};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unused-lookahead.wal");
        let wal = match Wal::create_with_io_mode(&path, WalIoMode::Parallel) {
            Ok(wal) => wal,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return;
            }
            Err(error) => panic!("failed to create parallel WAL: {error:#}"),
        };
        let runtime = wal.parallel_runtime.as_ref().unwrap();
        {
            let mut packer = runtime.inner.packer_state.lock();
            if let Some(mut initializer) = packer.initializer.take() {
                initializer.close().unwrap();
            }
            super::preallocate(&runtime.inner.preallocator, super::PREALLOC_BLOCK).unwrap();
            packer.preallocated_end = super::PREALLOC_BLOCK;
            packer.initializer = Some(
                super::ExtentInitializer::spawn(
                    Arc::new(File::open(&path).unwrap()),
                    super::PREALLOC_BLOCK,
                )
                .unwrap(),
            );
        }
        let ticket = wal
            .put_batch(&[(b"prefix".as_slice(), b"durable".as_slice())], 1)
            .unwrap();
        wal.submit_and_commit(ticket).unwrap();
        wal.close()
            .expect("unused lookahead cannot fail durable WAL close");
        drop(wal);

        let skiplist = Arc::new(SkipMap::new());
        let (recovered, max_ts) = Wal::recover(&path, &skiplist).unwrap();
        assert_eq!(max_ts, 1);
        assert_eq!(skiplist.len(), 1);
        assert_eq!(
            skiplist.get(b"prefix".as_slice()).unwrap().value().as_ref(),
            b"durable"
        );
        recovered.close().unwrap();
    }

    #[test]
    fn extent_lookahead_failure_is_reported_when_suffix_is_required() {
        use std::{fs::File, sync::Arc};

        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(super::PREALLOC_BLOCK).unwrap();
        let readonly = Arc::new(File::open(file.path()).unwrap());
        let mut initializer =
            super::ExtentInitializer::spawn(readonly, super::PREALLOC_BLOCK).unwrap();

        initializer.prepare(super::PREALLOC_BLOCK, true).unwrap();
        let error = initializer
            .prepare(2 * super::PREALLOC_BLOCK, true)
            .expect_err("a required suffix must report its allocation failure");
        assert!(format!("{error:#}").contains("fallocate failed during WAL preallocation"));
        initializer.close().unwrap();
    }

    #[test]
    fn extent_initializer_failure_is_reported_and_joined() {
        use std::{fs::File, sync::Arc};

        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(super::WAL_HEADER_END).unwrap();
        let readonly = Arc::new(File::open(file.path()).unwrap());
        let mut initializer =
            super::ExtentInitializer::spawn(readonly, super::WAL_HEADER_END).unwrap();
        assert!(initializer.prepare(super::PREALLOC_BLOCK, true).is_err());
        initializer
            .close()
            .expect("reported failure still joins safely");
    }

    #[test]
    fn extent_initializer_can_close_with_an_unconsumed_completion() {
        use std::sync::Arc;

        let file = Arc::new(tempfile::tempfile().unwrap());
        file.set_len(super::WAL_HEADER_END).unwrap();
        let mut initializer = super::ExtentInitializer::spawn(file, super::WAL_HEADER_END).unwrap();
        initializer.close().unwrap();
    }

    #[cfg(feature = "bench")]
    use super::validate_sync_diagnostics_transition;
    use super::{
        AdmissionState, AdmittedBatch, BufferBudget, MAX_BUFFER_CAPACITY, MAX_WAL_FILE_SIZE,
        NORMAL_ACTIVE_BUFFER_BUDGET, PREALLOC_BLOCK, ParallelBuffer, WAL_HEADER_END, WalFull,
        WalV7DataAllocation, preallocate, round_up, take_admitted_group,
    };
    use crate::wal::{DirectBuf, Wal};

    fn admitted_batch(
        budget: &std::sync::Arc<BufferBudget>,
        ticket: u64,
        file_offset: u64,
    ) -> AdmittedBatch {
        admitted_batch_of_size(budget, ticket, file_offset, 4096)
    }

    fn admitted_batch_of_size(
        budget: &std::sync::Arc<BufferBudget>,
        ticket: u64,
        file_offset: u64,
        aligned_len: usize,
    ) -> AdmittedBatch {
        budget
            .reserve(aligned_len as u64)
            .expect("reserve test buffer");
        let mut direct = DirectBuf::new(aligned_len);
        direct.set_len(aligned_len);

        AdmittedBatch {
            ticket,
            file_offset,
            aligned_len,
            v7_allocation: None,
            buffer: ParallelBuffer::activate(
                direct,
                std::sync::Arc::clone(budget),
                aligned_len as u64,
            ),
        }
    }

    fn admitted_v7_batch(
        budget: &std::sync::Arc<BufferBudget>,
        allocation: WalV7DataAllocation,
    ) -> AdmittedBatch {
        let ticket = allocation.ticket();
        let range = allocation.frame_range().clone();
        let aligned_len = usize::try_from(range.end - range.start).expect("v7 batch fits usize");
        budget
            .reserve(aligned_len as u64)
            .expect("reserve test v7 buffer");
        let mut direct = DirectBuf::new(aligned_len);
        direct.set_len(aligned_len);

        AdmittedBatch {
            ticket,
            file_offset: range.start,
            aligned_len,
            v7_allocation: Some(allocation),
            buffer: ParallelBuffer::activate(
                direct,
                std::sync::Arc::clone(budget),
                aligned_len as u64,
            ),
        }
    }

    #[test]
    fn packer_preserves_zero_fill_for_mixed_groups_and_skips_large_only_groups() {
        let budget = std::sync::Arc::new(BufferBudget::new());
        let large = super::ALLOCATION_ONLY_MIN_BATCH_BYTES;
        let mut admission = AdmissionState {
            open: true,
            close_cutoff: None,
            poison: None,
            #[cfg(feature = "bench")]
            initial_ticket: 0,
            next_ticket: 4,
            admitted_end: WAL_HEADER_END + 3 * large as u64 + 4096,
            v7_allocator: None,
            v7_header_digest: None,
            v7_reservation_owner: None,
            v7_metadata: None,
            v7_prefixes: BTreeMap::new(),
            v7_reservations: BTreeMap::new(),
            queue: std::collections::VecDeque::from([
                admitted_batch_of_size(&budget, 0, WAL_HEADER_END, large),
                admitted_batch_of_size(&budget, 1, WAL_HEADER_END + large as u64, large),
                admitted_batch(&budget, 2, WAL_HEADER_END + 2 * large as u64),
                admitted_batch_of_size(&budget, 3, WAL_HEADER_END + 2 * large as u64 + 4096, large),
            ]),
        };

        let large_group = take_admitted_group(&mut admission, 0, WAL_HEADER_END, 2)
            .unwrap()
            .unwrap();
        assert!(!large_group.initialize_extents);
        let mixed_group = take_admitted_group(
            &mut admission,
            large_group.next_ticket,
            large_group.reserved_end,
            2,
        )
        .unwrap()
        .unwrap();
        assert!(mixed_group.initialize_extents);
        assert_eq!(mixed_group.reserved_end, admission.admitted_end);
        assert!(admission.queue.is_empty());
        drop((large_group, mixed_group));
        assert_eq!(budget.state.lock().active_bytes, 0);
    }

    #[test]
    fn packer_coalesces_contiguous_tickets_but_stops_before_poison() {
        let budget = std::sync::Arc::new(BufferBudget::new());
        let mut admission = AdmissionState {
            open: false,
            close_cutoff: Some(4),
            poison: Some((3, "write failure".to_owned())),
            #[cfg(feature = "bench")]
            initial_ticket: 0,
            next_ticket: 4,
            admitted_end: WAL_HEADER_END + 4 * 4096,
            v7_allocator: None,
            v7_header_digest: None,
            v7_reservation_owner: None,
            v7_metadata: None,
            v7_prefixes: BTreeMap::new(),
            v7_reservations: BTreeMap::new(),
            queue: std::collections::VecDeque::from([
                admitted_batch(&budget, 0, WAL_HEADER_END),
                admitted_batch(&budget, 1, WAL_HEADER_END + 4096),
                admitted_batch(&budget, 2, WAL_HEADER_END + 2 * 4096),
                admitted_batch(&budget, 3, WAL_HEADER_END + 3 * 4096),
            ]),
        };

        let group = take_admitted_group(&mut admission, 0, WAL_HEADER_END, 8)
            .expect("contiguous tickets form a valid I/O group")
            .expect("tickets below the poison boundary remain packable");

        assert_eq!(group.first_ticket, 0);
        assert_eq!(group.next_ticket, 3);
        assert_eq!(group.reserved_end, WAL_HEADER_END + 3 * 4096);
        assert_eq!(group.writes.len(), 3);
        assert!(admission.queue.is_empty());
        drop(group);
        assert_eq!(budget.state.lock().active_bytes, 0);
    }

    #[test]
    fn v7_packer_carries_frontier_gap_into_the_following_worker_group() {
        use crate::wal::v7::{
            allocator::{WalV7FrontierCursor, WalV7PhysicalAllocator},
            codec::WalV7Frontier,
        };

        const DATA_START: u64 = 8 * 1024;
        const FRAME: u64 = 4 * 1024;

        let genesis_cursor = WalV7FrontierCursor {
            frontier: WalV7Frontier {
                generation: 0,
                ticket_end: 0,
                durable_end: 4096,
                last_commit_ts: 0,
                prefix_digest: [0x11; 32],
                previous_frontier_offset: 0,
                previous_frontier_digest: [0; 32],
            },
            frame_offset: 4096,
            frame_digest: [0x22; 32],
        };
        let mut allocator =
            WalV7PhysicalAllocator::new(0, DATA_START, MAX_WAL_FILE_SIZE, genesis_cursor)
                .expect("create v7 allocator");
        let first = allocator
            .reserve_data(1)
            .expect("reserve first DATA ticket");
        let frontier = allocator
            .reserve_frontier(1)
            .expect("reserve marker after first ticket");
        let second = allocator
            .reserve_data(2)
            .expect("reserve second DATA ticket");
        assert_eq!(frontier.frame_offset(), DATA_START + FRAME);

        let budget = std::sync::Arc::new(BufferBudget::new());
        let mut admission = AdmissionState {
            open: false,
            close_cutoff: Some(2),
            poison: None,
            #[cfg(feature = "bench")]
            initial_ticket: 0,
            next_ticket: 2,
            admitted_end: allocator.allocation_tail(),
            v7_allocator: Some(allocator),
            v7_header_digest: None,
            v7_reservation_owner: None,
            v7_metadata: None,
            v7_prefixes: BTreeMap::new(),
            v7_reservations: BTreeMap::new(),
            queue: std::collections::VecDeque::from([
                admitted_v7_batch(&budget, first),
                admitted_v7_batch(&budget, second),
            ]),
        };

        let first_group = take_admitted_group(&mut admission, 0, DATA_START, 1)
            .expect("pack first v7 ticket")
            .expect("first ticket is queued");
        assert_eq!(first_group.allocation_start, DATA_START);
        assert_eq!(first_group.reserved_end, DATA_START + FRAME);
        assert!(first_group.frontier_offsets.is_empty());
        super::WriteGroup::new_with_frontiers(
            first_group.first_ticket..first_group.next_ticket,
            first_group.writes,
            first_group.allocation_start..first_group.reserved_end,
            &first_group.frontier_offsets,
        )
        .expect("first group ends immediately before the reserved marker");

        let second_group = take_admitted_group(
            &mut admission,
            first_group.next_ticket,
            first_group.reserved_end,
            1,
        )
        .expect("pack second v7 ticket across marker")
        .expect("second ticket is queued");
        assert_eq!(second_group.allocation_start, DATA_START + FRAME);
        assert_eq!(second_group.reserved_end, DATA_START + 4 * FRAME);
        assert_eq!(second_group.frontier_offsets, [DATA_START + FRAME]);
        super::WriteGroup::new_with_frontiers(
            second_group.first_ticket..second_group.next_ticket,
            second_group.writes,
            second_group.allocation_start..second_group.reserved_end,
            &second_group.frontier_offsets,
        )
        .expect("second group accounts for the leading marker frame");

        assert!(admission.queue.is_empty());
        assert_eq!(budget.state.lock().active_bytes, 0);
    }

    #[test]
    fn buffer_budget_allows_one_exclusive_oversized_batch() {
        let budget = BufferBudget::new();
        let oversized = NORMAL_ACTIVE_BUFFER_BUDGET + 4096;

        budget.reserve(oversized).expect("reserve oversized batch");
        {
            let state = budget.state.lock();
            assert_eq!(state.active_bytes, oversized);
            assert!(state.oversized_active);
        }

        budget.release(oversized);
        let state = budget.state.lock();
        assert_eq!(state.active_bytes, 0);
        assert!(!state.oversized_active);
        drop(state);

        budget
            .reserve(NORMAL_ACTIVE_BUFFER_BUDGET)
            .expect("normal budget available after oversized CQE");
        budget.release(NORMAL_ACTIVE_BUFFER_BUDGET);
    }

    #[test]
    fn oversized_waiter_blocks_new_normal_reservations_until_it_runs() {
        use std::{
            sync::Arc,
            thread,
            time::{Duration, Instant},
        };

        use crossbeam_channel::bounded;

        let budget = Arc::new(BufferBudget::new());
        let initial_bytes = NORMAL_ACTIVE_BUFFER_BUDGET - 4096;
        let oversized_bytes = NORMAL_ACTIVE_BUFFER_BUDGET + 4096;
        budget
            .reserve(initial_bytes)
            .expect("reserve normal buffers");

        let (oversized_tx, oversized_rx) = bounded(1);
        let oversized_budget = Arc::clone(&budget);
        let oversized_waiter = thread::spawn(move || {
            let result = oversized_budget.reserve(oversized_bytes);
            oversized_tx
                .send(result.is_ok())
                .expect("report large reservation");
        });

        let started = Instant::now();
        loop {
            if budget.state.lock().oversized_waiters == 1 {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(5));
            thread::yield_now();
        }

        let (normal_tx, normal_rx) = bounded(1);
        let normal_budget = Arc::clone(&budget);
        let normal_waiter = thread::spawn(move || {
            let result = normal_budget.reserve(4096);
            normal_tx
                .send(result.is_ok())
                .expect("report normal reservation");
        });

        assert!(normal_rx.recv_timeout(Duration::from_millis(25)).is_err());
        budget.release(initial_bytes);
        assert!(
            oversized_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("oversized reservation completes")
        );
        assert!(normal_rx.try_recv().is_err());

        budget.release(oversized_bytes);
        assert!(
            normal_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("normal reservation resumes")
        );
        budget.release(4096);
        oversized_waiter.join().expect("oversized waiter joins");
        normal_waiter.join().expect("normal waiter joins");
    }

    #[test]
    fn buffer_budget_rejects_hard_cap_before_reserving_memory() {
        let budget = BufferBudget::new();
        assert!(budget.reserve(MAX_BUFFER_CAPACITY + 4096).is_err());
        assert_eq!(budget.state.lock().active_bytes, 0);
    }

    #[test]
    fn wal_full_remains_downcastable_through_anyhow_context() {
        let error = anyhow::Error::new(WalFull).context("WAL admission failed");
        assert!(Wal::is_retryable_full_error(&error));
        assert!(!Wal::is_retryable_full_error(&anyhow::anyhow!(
            "permanent failure"
        )));
    }

    #[test]
    fn rounds_wal_extent_to_preallocation_boundary() {
        assert_eq!(round_up(4096, 1 << 20), Some(1 << 20));
        assert_eq!(round_up(1 << 20, 1 << 20), Some(1 << 20));
        assert_eq!(round_up(u64::MAX, 4096), None);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn sync_diagnostics_can_only_be_enabled_before_wal_admission() {
        assert!(validate_sync_diagnostics_transition(false, false, true).is_ok());
        assert!(validate_sync_diagnostics_transition(true, false, false).is_ok());
        assert!(validate_sync_diagnostics_transition(true, true, true).is_ok());
        assert!(validate_sync_diagnostics_transition(true, false, true).is_err());
    }

    #[test]
    fn preallocate_extends_the_file_size_for_parallel_direct_writes() {
        use std::fs::OpenOptions;
        use std::os::unix::fs::FileExt;

        let directory = tempfile::tempdir().expect("create temp directory");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(directory.path().join("wal"))
            .expect("create WAL file");
        file.set_len(WAL_HEADER_END)
            .expect("write WAL header extent");
        file.write_all_at(b"header", 0).expect("write header");

        preallocate(&file, PREALLOC_BLOCK).expect("preallocate WAL extent");

        assert_eq!(
            file.metadata().expect("read WAL metadata").len(),
            PREALLOC_BLOCK
        );

        file.write_all_at(b"batch", PREALLOC_BLOCK - 5)
            .expect("write existing batch");
        preallocate(&file, 2 * PREALLOC_BLOCK).expect("extend WAL again");
        preallocate(&file, PREALLOC_BLOCK).expect("ignore smaller extent");
        assert_eq!(file.metadata().unwrap().len(), 2 * PREALLOC_BLOCK);
        let mut header = [0; 6];
        file.read_exact_at(&mut header, 0).unwrap();
        assert_eq!(&header, b"header");
        let mut batch = [0; 5];
        file.read_exact_at(&mut batch, PREALLOC_BLOCK - 5).unwrap();
        assert_eq!(&batch, b"batch");
    }
}

#[cfg(test)]
mod native_async_tests {
    use super::*;
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_wal_waits_for_durability_and_wakes_at_the_poison_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let wal = match crate::wal::Wal::create_with_io_mode(
            directory.path().join("probe.wal"),
            crate::wal::WalIoMode::Parallel,
        ) {
            Ok(wal) => wal,
            Err(error) if crate::tests::harness::is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return;
            }
            Err(error) => panic!("failed to create parallel WAL: {error:#}"),
        };
        let runtime = wal.parallel_runtime.as_ref().unwrap();
        // Drive only the frontier model in a live runtime with no submitted I/O.
        runtime.inner.admission.lock().next_ticket = 2;
        let first = runtime.wait_durable_async(0);
        let second = runtime.wait_durable_async(1);
        tokio::pin!(first, second);
        poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            assert!(second.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        runtime.inner.durability.state.lock().written_frontier = 2;
        runtime.inner.durability.notify_change();
        poll_fn(|cx| {
            assert!(
                first.as_mut().poll(cx).is_pending(),
                "written is not durable"
            );
            assert!(second.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        {
            let mut state = runtime.inner.durability.state.lock();
            set_poison(&mut state, 1, "injected later-group failure".to_owned());
        }
        runtime.inner.durability.notify_change();
        assert!(second.await.is_err());
        poll_fn(|cx| {
            assert!(
                first.as_mut().poll(cx).is_pending(),
                "earlier written prefix can still sync"
            );
            Poll::Ready(())
        })
        .await;
        runtime.inner.durability.state.lock().durable_frontier = 1;
        runtime.inner.durability.notify_change();
        first.await.unwrap();
        assert!(runtime.wait_durable_async(2).await.is_err());
        assert!(wal.close().is_err());
    }
}

#[cfg(test)]
mod async_budget_tests {
    use super::*;
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_cancelled_oversized_budget_waiter_does_not_starve_ordinary_writes() {
        let budget = BufferBudget::new();
        budget.reserve(4096).unwrap();
        let mut oversized = Box::pin(budget.reserve_async(NORMAL_ACTIVE_BUFFER_BUDGET + 4096));
        let mut ordinary = Box::pin(budget.reserve_async(4096));
        poll_fn(|cx| {
            assert!(oversized.as_mut().poll(cx).is_pending());
            assert!(ordinary.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(oversized);
        ordinary.await.unwrap();
        assert_eq!(budget.state.lock().active_bytes, 8192);
        assert_eq!(budget.state.lock().oversized_waiters, 0);
        budget.release(8192);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_budget_close_wakes_ordinary_and_oversized_waiters() {
        let budget = BufferBudget::new();
        budget.reserve(NORMAL_ACTIVE_BUFFER_BUDGET).unwrap();
        let mut ordinary = Box::pin(budget.reserve_async(4096));
        let mut oversized = Box::pin(budget.reserve_async(NORMAL_ACTIVE_BUFFER_BUDGET + 4096));
        poll_fn(|cx| {
            assert!(ordinary.as_mut().poll(cx).is_pending());
            assert!(oversized.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        budget.close();
        assert!(ordinary.await.is_err());
        assert!(oversized.await.is_err());
        assert_eq!(budget.state.lock().oversized_waiters, 0);
        budget.release(NORMAL_ACTIVE_BUFFER_BUDGET);
    }
}
