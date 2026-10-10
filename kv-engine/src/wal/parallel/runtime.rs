//! Runtime admission, ordered packing, and durability coordination for parallel WALs.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io,
    os::fd::AsRawFd,
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::wal::v7::install::WalV7RuntimeSeed;
use anyhow::{Context, Result, anyhow, bail, ensure};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Condvar, Mutex, MutexGuard};

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

/// Coordinates from which a parallel runtime begins admission.
///
/// A v7 runtime is deliberately constructed paused: until later runtime slices
/// install its marker writer and live resource reservations, it must not admit
/// v4-shaped writes or allocate speculative extents.
pub(crate) struct ParallelWalRuntimeStart {
    append_offset: u64,
    next_ticket: u64,
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
            min_append_offset: WAL_HEADER_END,
            admission_open: true,
            initialize_extents: true,
            prewarm_buffers: true,
        };
        start.validate()?;
        Ok(start)
    }

    #[allow(dead_code)] // Production v7 open wiring follows in a later runtime slice.
    pub(crate) fn v7(seed: WalV7RuntimeSeed) -> Result<Self> {
        let start = Self {
            append_offset: seed.append_offset,
            next_ticket: seed.next_ticket,
            min_append_offset: seed.data_start,
            admission_open: false,
            initialize_extents: false,
            prewarm_buffers: false,
        };
        start.validate()?;
        Ok(start)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.min_append_offset >= WAL_HEADER_END
                && self.min_append_offset.is_multiple_of(4096)
                && (self.min_append_offset..=MAX_WAL_FILE_SIZE).contains(&self.append_offset)
                && self.append_offset.is_multiple_of(4096),
            "invalid initial WAL append offset {}",
            self.append_offset
        );
        Ok(())
    }
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
}

impl ParallelBuffer {
    fn pooled(buffer: DirectBuf, budget: Arc<BufferBudget>) -> Self {
        Self {
            buffer: Some(buffer),
            budget,
            active_bytes: None,
        }
    }

    fn activate(buffer: DirectBuf, budget: Arc<BufferBudget>, active_bytes: u64) -> Self {
        Self {
            buffer: Some(buffer),
            budget,
            active_bytes: Some(active_bytes),
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
        if self.cap() == BUFFER_POOL_BUF_SIZE {
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
    buffer: ParallelBuffer,
}

struct PackedGroup {
    first_ticket: u64,
    next_ticket: u64,
    reserved_end: u64,
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
    queue: VecDeque<AdmittedBatch>,
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
        let initial_file_end = start.append_offset;

        ensure!(
            (start.min_append_offset..=MAX_WAL_FILE_SIZE).contains(&initial_file_end),
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
            .load(std::sync::atomic::Ordering::Acquire);
        #[cfg(not(test))]
        let file_size_limit = MAX_WAL_FILE_SIZE;

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
        if preallocation_end > file_size_limit {
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
            buffer,
        });
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::note_parallel_wal_admission();
        debug_assert!(WAL_HEADER_END <= state.admitted_end);
        debug_assert!(state.admitted_end <= MAX_WAL_FILE_SIZE);
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

        if close_error.is_none()
            && let Some(error) = self.inner.durability.state.lock().poison_error.clone()
        {
            close_error = Some(format!("parallel WAL is poisoned: {error}"));
        }

        threads.closed = true;
        threads.close_error.clone_from(&close_error);
        match close_error {
            Some(error) => Err(anyhow!(error)),
            None => Ok(()),
        }
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
        packer.reserved_end = packed.reserved_end;
        let writes = packed.writes;

        let Some(target_preallocated_end) = round_up(packer.reserved_end, PREALLOC_BLOCK) else {
            let error = anyhow!("parallel WAL preallocation offset overflow");
            report_packer_failure(inner, first_ticket, &error);
            return Err(error);
        };
        if target_preallocated_end > packer.preallocated_end {
            #[cfg(feature = "bench")]
            let preallocation_start = Instant::now();
            let allocation = match packer.initializer.as_mut() {
                Some(initializer) => {
                    initializer.prepare(target_preallocated_end, packed.initialize_extents)
                }
                None => preallocate(&inner.preallocator, target_preallocated_end),
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
            debug_assert!(admitted_end <= MAX_WAL_FILE_SIZE);
        }
        debug_assert!(packer.preallocated_end >= packer.reserved_end);
        debug_assert!(packer.preallocated_end <= MAX_WAL_FILE_SIZE);
        if packer.reserved_end > packer.preallocated_end {
            let error = anyhow!("WAL write extends beyond preallocation");
            report_packer_failure(inner, first_ticket, &error);
            return Err(error);
        }

        let group = match WriteGroup::new(packer.next_ticket..expected_ticket, writes) {
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
    let group_capacity = admission.queue.len().min(max_tickets);
    let mut writes = WriteGroupBuffers::with_capacity(group_capacity);
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
        // Keep zero-fill for groups containing small writes. Only extent
        // preparation changes; every batch still uses the parallel pipeline.
        initialize_extents |= batch.aligned_len < ALLOCATION_ONLY_MIN_BATCH_BYTES;
        ensure!(
            batch.ticket == expected_ticket && batch.file_offset == packed_end,
            "parallel WAL admission queue is not ticket/offset contiguous"
        );
        packed_end = batch
            .file_offset
            .checked_add(batch.aligned_len as u64)
            .ok_or_else(|| anyhow!("parallel WAL offset overflow"))?;
        expected_ticket = expected_ticket
            .checked_add(1)
            .ok_or_else(|| anyhow!("parallel WAL ticket counter overflow"))?;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::parallel_wal_crash_point("parallel_wal.offset_reserved");
        writes.push(WriteBuffer::new(
            batch.buffer,
            batch.file_offset,
            batch.aligned_len,
        ));
    }
    Ok(Some(PackedGroup {
        first_ticket,
        next_ticket: expected_ticket,
        reserved_end: packed_end,
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
        admission.queue.clear();
    }
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
        let mut admission = inner.admission.lock();
        admission.open = false;
        if admission
            .poison
            .as_ref()
            .is_none_or(|(ticket, _)| result.tickets.start < *ticket)
        {
            admission.poison = Some((result.tickets.start, error.clone()));
        }
        admission.queue.clear();
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
        crate::chaos::failpoint::parallel_wal_crash_point(
            "parallel_wal.later_group_completed_first",
        );
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
    let (target, durable) = {
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
        (target, state.durable_frontier)
    };
    if target <= durable {
        return Ok(None);
    }

    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::parallel_wal_crash_point("parallel_wal.before_fdatasync");
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
        crate::chaos::failpoint::parallel_wal_crash_point("parallel_wal.after_fdatasync");

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
    #[test]
    fn recovered_v7_runtime_keeps_its_ticket_and_append_cursors_paused() {
        use std::{fs::File, os::unix::fs::OpenOptionsExt, sync::Arc};

        use super::{ParallelWalRuntime, ParallelWalRuntimeStart};
        use crate::{
            tests::harness::is_io_uring_unavailable_error, wal::v7::install::WalV7RuntimeSeed,
        };

        const DATA_START: u64 = 2 * 4096;
        const APPEND_OFFSET: u64 = 4 * 4096;
        const NEXT_TICKET: u64 = 7;

        let directory = tempfile::tempdir().expect("create WAL directory");
        let path = directory.path().join("recovered-v7.wal");
        let file = File::create(&path).expect("create recovered WAL image");
        file.set_len(APPEND_OFFSET)
            .expect("size recovered WAL image");
        drop(file);

        let direct_file = File::options()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .expect("open recovered WAL with direct I/O");
        let direct_file = Arc::new(direct_file);
        let start = ParallelWalRuntimeStart::v7(WalV7RuntimeSeed {
            data_start: DATA_START,
            append_offset: APPEND_OFFSET,
            next_ticket: NEXT_TICKET,
        })
        .expect("validate recovered runtime cursor");
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

        {
            let admission = runtime.inner.admission.lock();
            assert!(!admission.open);
            assert_eq!(admission.next_ticket, NEXT_TICKET);
            assert_eq!(admission.admitted_end, APPEND_OFFSET);
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
        AdmissionState, AdmittedBatch, BufferBudget, MAX_BUFFER_CAPACITY,
        NORMAL_ACTIVE_BUFFER_BUDGET, PREALLOC_BLOCK, ParallelBuffer, WAL_HEADER_END, WalFull,
        preallocate, round_up, take_admitted_group,
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
