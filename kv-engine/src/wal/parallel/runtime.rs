//! Runtime admission, ordered packing, and durability coordination for v4 WALs.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io,
    os::fd::AsRawFd,
    sync::Arc,
    thread::{self, JoinHandle},
};

#[cfg(feature = "bench")]
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail, ensure};
use crossbeam_channel::{Receiver, Sender, TryRecvError, unbounded};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Condvar, Mutex};

use super::{
    BUFFER_POOL_BUF_SIZE, BUFFER_POOL_CAPACITY, DirectBuf, MAX_WAL_FILE_SIZE, PREALLOC_BLOCK,
    parallel_worker::{
        GroupWriteResult, IoWorker, IoWorkerClient, WalSyncProgress, WorkerBuffer, WriteBuffer,
        WriteGroup,
    },
};

const NORMAL_ACTIVE_BUFFER_BUDGET: u64 = 64 * 1024 * 1024;
const MAX_BUFFER_CAPACITY: u64 = 240 * 1024 * 1024;
const WAL_HEADER_END: u64 = 4096;

#[derive(Debug)]
pub(crate) struct WalFull;

impl std::fmt::Display for WalFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WAL full; rotate and retry")
    }
}

impl std::error::Error for WalFull {}

#[derive(Debug)]
struct BufferBudget {
    state: Mutex<BufferBudgetState>,
    available: Condvar,
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
    }

    fn close(&self) {
        self.state.lock().closed = true;
        self.available.notify_all();
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

struct AdmissionState {
    open: bool,
    close_cutoff: Option<u64>,
    poison: Option<(u64, String)>,
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
}

struct RuntimeInner {
    admission: Mutex<AdmissionState>,
    admission_changed: Condvar,
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

/// Dedicated packer, I/O worker, and single-owner durability coordinator.
pub(crate) struct ParallelWalRuntime {
    inner: Arc<RuntimeInner>,
    threads: Mutex<RuntimeThreads>,
}

impl ParallelWalRuntime {
    pub(crate) fn spawn(
        worker_file: Arc<File>,
        sync_file: Arc<File>,
        preallocator: Arc<File>,
        initial_file_end: u64,
    ) -> Result<Self> {
        ensure!(
            (WAL_HEADER_END..=MAX_WAL_FILE_SIZE).contains(&initial_file_end),
            "invalid initial WAL append offset {initial_file_end}"
        );

        let buffer_budget = Arc::new(BufferBudget::new());
        let buffer_pool = Arc::new(ArrayQueue::new(BUFFER_POOL_CAPACITY));
        for _ in 0..BUFFER_POOL_CAPACITY {
            let _ = buffer_pool.push(ParallelBuffer::pooled(
                DirectBuf::new(BUFFER_POOL_BUF_SIZE),
                Arc::clone(&buffer_budget),
            ));
        }

        let mut worker = IoWorker::spawn(worker_file, Arc::clone(&buffer_pool))?;
        let worker_client = worker.client();
        let sync_progress = worker.sync_progress();
        let worker_completions = worker.take_completions();
        let (packer_failures_tx, packer_failures_rx) = unbounded();
        let durability = Arc::new(DurabilityShared {
            state: Mutex::new(DurabilityState::default()),
            changed: Condvar::new(),
        });
        let inner = Arc::new(RuntimeInner {
            admission: Mutex::new(AdmissionState {
                open: true,
                close_cutoff: None,
                poison: None,
                next_ticket: 0,
                admitted_end: initial_file_end,
                queue: VecDeque::new(),
            }),
            admission_changed: Condvar::new(),
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
                run_sync_coordinator(
                    sync_file,
                    worker_completions,
                    packer_failures_rx,
                    coordinator_inner,
                )
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
            .spawn(move || run_packer(packer_inner, packer_failures_tx, initial_file_end))
        {
            Ok(join) => join,
            Err(error) => {
                inner.buffer_budget.close();
                {
                    let mut admission = inner.admission.lock();
                    admission.open = false;
                    inner.admission_changed.notify_all();
                }
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

        Ok(ParallelBuffer::activate(
            direct_buffer,
            Arc::clone(&self.inner.buffer_budget),
            capacity_u64,
        ))
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
        self.inner.admission_changed.notify_one();

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
            admission.next_ticket,
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
            self.inner.admission_changed.notify_all();
        }
        self.inner.buffer_budget.close();

        let mut close_error = None;
        if let Some(packer) = threads.packer.take() {
            match packer.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => close_error = Some(format!("packer failed: {error:#}")),
                Err(_) => close_error = Some("WAL packer panicked".to_owned()),
            }
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

fn run_packer(
    inner: Arc<RuntimeInner>,
    failures: Sender<GroupWriteResult>,
    initial_file_end: u64,
) -> Result<()> {
    let mut next_ticket = 0_u64;
    let mut reserved_end = initial_file_end;
    let mut preallocated_end = initial_file_end;

    loop {
        let batch = {
            let mut admission = inner.admission.lock();
            while admission.queue.is_empty() && admission.open {
                inner.admission_changed.wait(&mut admission);
            }
            if admission.queue.is_empty() && !admission.open {
                if admission.poison.is_none()
                    && let Some(cutoff) = admission.close_cutoff
                {
                    ensure!(
                        next_ticket == cutoff,
                        "parallel WAL packer stopped at ticket {next_ticket} before close cutoff {cutoff}"
                    );
                }
                break;
            }
            let batch = admission
                .queue
                .pop_front()
                .expect("nonempty WAL admission queue has a first batch");
            if admission
                .poison
                .as_ref()
                .is_some_and(|(poison, _)| batch.ticket >= *poison)
            {
                admission.queue.clear();
                inner.admission_changed.notify_all();
                None
            } else {
                Some(batch)
            }
        };
        let Some(batch) = batch else {
            break;
        };

        if batch.ticket != next_ticket || batch.file_offset != reserved_end {
            let error = anyhow!("parallel WAL admission queue is not ticket/offset contiguous");
            report_packer_failure(&inner, &failures, batch.ticket, &error);
            return Err(error);
        }
        let file_end = batch
            .file_offset
            .checked_add(batch.aligned_len as u64)
            .ok_or_else(|| anyhow!("parallel WAL offset overflow"))?;
        reserved_end = file_end;
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::parallel_wal_crash_point("parallel_wal.offset_reserved");

        let target_preallocated_end = round_up(file_end, PREALLOC_BLOCK)
            .ok_or_else(|| anyhow!("parallel WAL preallocation offset overflow"))?;
        if target_preallocated_end > preallocated_end {
            #[cfg(feature = "bench")]
            let preallocation_start = Instant::now();
            if let Err(error) = preallocate(&inner.preallocator, target_preallocated_end) {
                report_packer_failure(&inner, &failures, batch.ticket, &error);
                return Err(error);
            }
            #[cfg(feature = "bench")]
            inner
                .sync_progress
                .record_preallocation_ns(preallocation_start.elapsed().as_nanos() as u64);
            preallocated_end = target_preallocated_end;
        }
        let admitted_end = inner.admission.lock().admitted_end;
        debug_assert!(WAL_HEADER_END <= reserved_end);
        debug_assert!(reserved_end <= admitted_end);
        debug_assert!(admitted_end <= MAX_WAL_FILE_SIZE);
        debug_assert!(preallocated_end >= reserved_end);
        debug_assert!(preallocated_end <= MAX_WAL_FILE_SIZE);
        ensure!(
            file_end <= preallocated_end,
            "WAL write extends beyond preallocation"
        );

        let write = WriteBuffer::new(batch.buffer, batch.file_offset, batch.aligned_len);
        let group = match WriteGroup::new(batch.ticket..batch.ticket + 1, vec![write]) {
            Ok(group) => group,
            Err(error) => {
                let error = anyhow!("invalid packed WAL group: {error:?}");
                report_packer_failure(&inner, &failures, batch.ticket, &error);
                return Err(error);
            }
        };
        if let Err(error) = inner.worker.submit_group(group) {
            report_packer_failure(&inner, &failures, batch.ticket, &error);
            return Err(error).context("failed to submit packed WAL group");
        }
        next_ticket = next_ticket
            .checked_add(1)
            .ok_or_else(|| anyhow!("parallel WAL ticket counter overflow"))?;
    }

    Ok(())
}

fn report_packer_failure(
    inner: &RuntimeInner,
    failures: &Sender<GroupWriteResult>,
    ticket: u64,
    error: &anyhow::Error,
) {
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
        inner.admission_changed.notify_all();
    }
    inner.buffer_budget.close();
    let _ = failures.send(GroupWriteResult {
        group_id: u64::MAX,
        tickets: ticket..ticket.saturating_add(1),
        write_count: 0,
        write_bytes: 0,
        error: Some(message),
    });
}

fn run_sync_coordinator(
    sync_file: Arc<File>,
    worker_completions: Receiver<GroupWriteResult>,
    packer_failures: Receiver<GroupWriteResult>,
    inner: Arc<RuntimeInner>,
) -> Result<()> {
    let disconnected = crossbeam_channel::never();
    let mut worker_completions = worker_completions;
    let mut packer_failures = packer_failures;
    let mut worker_open = true;
    let mut packer_open = true;

    while worker_open || packer_open {
        crossbeam_channel::select! {
            recv(worker_completions) -> result => {
                match result {
                    Ok(result) => process_group_result(&inner, result),
                    Err(_) => {
                        worker_open = false;
                        worker_completions = disconnected.clone();
                    }
                }
            }
            recv(packer_failures) -> result => {
                match result {
                    Ok(result) => process_group_result(&inner, result),
                    Err(_) => {
                        packer_open = false;
                        packer_failures = disconnected.clone();
                    }
                }
            }
        }
        #[cfg(all(test, feature = "chaos-testing"))]
        crate::chaos::failpoint::before_parallel_wal_result_drain();
        drain_ready_results(
            &inner,
            &mut worker_completions,
            &mut packer_failures,
            &mut worker_open,
            &mut packer_open,
            &disconnected,
        );
        synchronize_written_prefix(&sync_file, &inner)?;
    }

    // Both producers have terminated. Freeze the final written prefix and
    // perform the shutdown sync before allowing the file handles to drop.
    terminalize_unresolved_prefix(&inner);
    synchronize_written_prefix(&sync_file, &inner)?;

    Ok(())
}

fn drain_ready_results(
    inner: &RuntimeInner,
    worker_completions: &mut Receiver<GroupWriteResult>,
    packer_failures: &mut Receiver<GroupWriteResult>,
    worker_open: &mut bool,
    packer_open: &mut bool,
    disconnected: &Receiver<GroupWriteResult>,
) {
    while *worker_open {
        match worker_completions.try_recv() {
            Ok(result) => process_group_result(inner, result),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                *worker_open = false;
                *worker_completions = disconnected.clone();
            }
        }
    }

    while *packer_open {
        match packer_failures.try_recv() {
            Ok(result) => process_group_result(inner, result),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                *packer_open = false;
                *packer_failures = disconnected.clone();
            }
        }
    }
}

fn process_group_result(inner: &RuntimeInner, result: GroupWriteResult) {
    #[cfg(feature = "chaos-testing")]
    let result_end = result.tickets.end;
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
        inner.admission_changed.notify_all();
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
    let assigned = inner.admission.lock().next_ticket;
    debug_assert!(state.written_frontier <= assigned);
    debug_assert!(state.durable_frontier <= state.written_frontier);
    #[cfg(feature = "chaos-testing")]
    let later_group_remains_outside_prefix = state.written_frontier < result_end;
    inner.durability.changed.notify_all();
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
        inner.admission_changed.notify_all();
        inner.buffer_budget.close();
    }
    inner.durability.changed.notify_all();
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
    next_ticket: u64,
) -> Result<()> {
    ensure!(
        !enabled || currently_enabled || next_ticket == 0,
        "WAL sync diagnostics must be enabled before the first WAL ticket"
    );
    Ok(())
}

fn synchronize_written_prefix(sync_file: &File, inner: &RuntimeInner) -> Result<()> {
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
        return Ok(());
    }

    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::parallel_wal_crash_point("parallel_wal.before_fdatasync");
    #[cfg(all(test, feature = "chaos-testing"))]
    crate::chaos::failpoint::before_parallel_wal_fdatasync_call();
    let _sync_start = inner.sync_progress.begin_sync(target, durable);
    inner.sync_progress.start_sync_activity();
    #[cfg(feature = "bench")]
    let mut syscall_started_at = None;
    #[cfg(not(feature = "bench"))]
    let syscall_started_at = None;
    let (sync_error, syscall_finished_at) = loop {
        #[cfg(feature = "bench")]
        let call_started_at = Instant::now();
        let error = fdatasync_file(sync_file).err();
        let call_finished_at = wal_sync_timestamp();
        #[cfg(feature = "bench")]
        if syscall_started_at.is_none() {
            syscall_started_at = Some(call_started_at);
        }
        if let Some(error) = error {
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break (Some(error), call_finished_at);
        }
        break (None, call_finished_at);
    };
    let (_sync_end, finished_at) = inner
        .sync_progress
        .finish_sync(syscall_started_at, syscall_finished_at);
    #[cfg(feature = "bench")]
    let duration_ns = finished_at
        .expect("benchmark WAL sync completion has a timestamp")
        .duration_since(syscall_started_at.expect("fdatasync call records its start timestamp"))
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
        let assigned = inner.admission.lock().next_ticket;
        debug_assert!(state.written_frontier <= assigned);
        inner.durability.changed.notify_all();
        drop(state);
        inner.sync_progress.mark_durable(acknowledged);

        return Ok(());
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
    inner.admission_changed.notify_all();
    inner.buffer_budget.close();
    // Publish the admission poison before waking writers that are waiting on
    // this failed sync. Otherwise a waiter can return `Err` and race a new
    // batch into the still-open admission queue.
    inner.durability.changed.notify_all();

    Ok(())
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
            0,
            i64::try_from(end).context("WAL preallocation exceeds i64")?,
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
    #[cfg(feature = "bench")]
    use super::validate_sync_diagnostics_transition;
    use super::{
        BufferBudget, MAX_BUFFER_CAPACITY, NORMAL_ACTIVE_BUFFER_BUDGET, PREALLOC_BLOCK,
        WAL_HEADER_END, WalFull, preallocate, round_up,
    };
    use crate::wal::Wal;

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
        assert!(validate_sync_diagnostics_transition(false, false, 10).is_ok());
        assert!(validate_sync_diagnostics_transition(true, false, 0).is_ok());
        assert!(validate_sync_diagnostics_transition(true, true, 10).is_ok());
        assert!(validate_sync_diagnostics_transition(true, false, 10).is_err());
    }

    #[test]
    fn preallocate_extends_the_file_size_for_parallel_direct_writes() {
        use std::fs::OpenOptions;

        let directory = tempfile::tempdir().expect("create temp directory");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(directory.path().join("wal"))
            .expect("create WAL file");
        file.set_len(WAL_HEADER_END)
            .expect("write WAL header extent");

        preallocate(&file, PREALLOC_BLOCK).expect("preallocate WAL extent");

        assert_eq!(
            file.metadata().expect("read WAL metadata").len(),
            PREALLOC_BLOCK
        );
    }
}
