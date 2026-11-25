//! Bounded admission and readiness coordination for the V8 Parallel writer.
//!
//! Admitted tickets stay visible to group formation independently of digest,
//! frame and DATA completion. Preparation, durability progress and durable
//! publication use separate notifications for the submitter, single durability
//! driver and followers.

use std::{
    collections::VecDeque,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(feature = "bench")]
use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use crossbeam_channel::{Receiver, Sender};
use parking_lot::{Condvar, Mutex};

use crate::{pitr::RecordedAt, wal::MAX_WAL_FILE_SIZE};

#[cfg(feature = "bench")]
use crate::wal::WalV8ProgressDiagnostics;

use super::{
    WAL_V8_HEADER_LEN,
    codec::{self, WAL_V8_FRAME_LEN, WalV8Header},
    digest::WalV8Digest,
    leader::WalV8LeaderCapacityError,
    prepare::{
        WalV8AdmissionGuard, WalV8AdmissionPlan, WalV8AdmissionPublisher, WalV8BatchAssignment,
        WalV8DigestReady,
    },
};

const DATA_START: u64 = (WAL_V8_HEADER_LEN + WAL_V8_FRAME_LEN) as u64;
const FRAME_LEN_U64: u64 = WAL_V8_FRAME_LEN as u64;
/// Bound both admission and the allocation-free cursor retirement snapshot.
pub(crate) const MAX_PARALLEL_INFLIGHT_TICKETS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV8ProgressTokenWait {
    Signaled,
    TimedOut,
    Disconnected,
}

/// Consume a pending wake before scanning the ring and completion queue.
/// Publications racing after this receive leave a token for the next park.
pub(crate) fn consume_progress_token_before_scan(receiver: &Receiver<()>) {
    let _ = receiver.try_recv();
}

pub(crate) fn wait_for_progress_token(
    receiver: &Receiver<()>,
    timeout: Duration,
) -> WalV8ProgressTokenWait {
    match receiver.recv_timeout(timeout) {
        Ok(()) => WalV8ProgressTokenWait::Signaled,
        Err(crossbeam_channel::RecvTimeoutError::Timeout) => WalV8ProgressTokenWait::TimedOut,
        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
            WalV8ProgressTokenWait::Disconnected
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV8ProgressMode {
    Eager,
    Cursor,
}

/// Selects whether direct DATA completion also posts the capacity-one
/// progress token. `Select` keeps the token for control progress and waits on
/// the completion queue directly.
#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum WalV8DirectCompletionWakeMode {
    #[default]
    Token,
    Select,
}

impl WalV8ProgressMode {
    pub(crate) fn uses_ring(self) -> bool {
        self == Self::Cursor
    }
}

/// Configuration for a v8 Parallel admission window resumed from recovery.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalV8ParallelConfig {
    /// First ticket not included in the recovered durable prefix.
    pub(crate) next_ticket: u64,
    /// First physical DATA offset available to a new ticket.
    pub(crate) next_data_offset: u64,
    /// Timestamp of the last ticket in the recovered durable prefix.
    pub(crate) last_commit_ts: u64,
    /// Recorded-at value of the last recovered ticket, if one exists.
    pub(crate) last_recorded_at: Option<RecordedAt>,
    /// Verified logical prefix digest at `next_ticket` from recovery.
    pub(crate) durable_prefix_digest: WalV8Digest,
    /// Maximum number of admitted tickets whose durability is unresolved.
    pub(crate) max_inflight_tickets: usize,
    /// Maximum logical and frame buffer bytes reserved by unresolved tickets.
    pub(crate) max_inflight_bytes: u64,
    /// Configured limit for this Active WAL image.
    pub(crate) max_file_size: u64,
    /// Readiness backend, selected before Parallel attachment.
    pub(crate) progress_mode: WalV8ProgressMode,
}

#[derive(Clone)]
pub(crate) struct WalV8ParallelCoordinator {
    inner: Arc<CoordinatorInner>,
}

/// Narrow notification and terminal-failure handle lent to the I/O worker in
/// the Cursor direct-completion experiment. It carries no writer or worker
/// ownership and is used only after publishing to the bounded completion queue
/// or after worker cleanup has released its permits and admission waiters.
#[derive(Clone)]
pub(crate) struct WalV8WorkerCompletionHandle {
    coordinator: WalV8ParallelCoordinator,
    completion_token_wake_enabled: bool,
}

impl WalV8WorkerCompletionHandle {
    pub(crate) fn notify_completion_queued(&self) -> Result<()> {
        ensure!(
            self.coordinator.progress_ring_enabled(),
            "direct completion notification requires Cursor progress mode"
        );
        if self.completion_token_wake_enabled {
            try_signal_progress_token(&self.coordinator.inner)
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "bench")]
    pub(crate) fn capture_data_completion_time(&self) -> Option<Instant> {
        self.coordinator.capture_data_completion_time()
    }

    #[cfg(not(feature = "bench"))]
    pub(crate) fn capture_data_completion_time(&self) -> Option<Instant> {
        None
    }

    /// This is called only after the worker has fenced admission and dropped
    /// all worker-owned permits, or from its unwind guard after stack cleanup.
    pub(crate) fn worker_failed_after_cleanup(&self) {
        self.coordinator.poison();
    }
}

struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
    changed: Condvar,
    /// Preparation and admission changes cannot make a durability waiter
    /// useful. Subscribe frontier formation to DATA/prefix progress instead.
    durability_changed: Condvar,
    /// Followers need acknowledged durability or an available driver, not
    /// another producer's preparation or DATA-completion notification.
    durable_published: Condvar,
    /// Published before releasing admission or returning a producer job. A
    /// DATA submitter can identify a lone admitted head without taking the
    /// coordinator lock under its submission lock.
    admitted_ticket_end: AtomicU64,
    /// Avoid timestamping every DATA completion unless detailed sync
    /// diagnostics are enabled for this WAL.
    #[cfg(feature = "bench")]
    data_completion_timestamps_enabled: std::sync::atomic::AtomicBool,
    /// Collect cursor progress counters without enabling detailed snapshots.
    #[cfg(feature = "bench")]
    progress_counters_enabled: AtomicBool,
    /// Fast opt-in gate for ordering HOL instrumentation.
    #[cfg(feature = "bench")]
    hol_diagnostics_enabled: std::sync::atomic::AtomicBool,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
    segment_incarnation: [u8; 16],
    physical_header_digest: [u8; 32],
    initial_prefix_digest: WalV8Digest,
    max_inflight_tickets: usize,
    max_inflight_bytes: u64,
    max_file_size: u64,
    progress_mode: WalV8ProgressMode,
    progress_ring: Option<TicketProgressRing>,
    progress_token_sender: Option<Sender<()>>,
    progress_token_receiver: Mutex<Option<Receiver<()>>>,
    #[cfg(feature = "bench")]
    progress_token_attempts: AtomicU64,
    #[cfg(feature = "bench")]
    progress_token_accepts: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TicketCellIdentity {
    segment_incarnation: [u8; 16],
    ticket: u64,
}

struct TicketProgressRing {
    segment_incarnation: [u8; 16],
    cells: Box<[TicketProgressCell]>,
    /// Set only by the Coordinator's terminal poison transition.
    poisoned: AtomicBool,
}

struct TicketProgressCell {
    payload: Mutex<TicketProgressPayload>,
}

impl Default for TicketProgressCell {
    fn default() -> Self {
        Self {
            payload: Mutex::new(TicketProgressPayload::default()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TicketProgressPayload {
    identity: Option<TicketCellIdentity>,
    logical_batch_bytes: Option<u64>,
    digest_ready: Option<WalV8DigestReady>,
    frames_ready: bool,
}

/// Successful completion of one physical DATA worker group. The containing
/// queue is private to a single Parallel writer instance.
#[derive(Clone, Debug)]
pub(crate) struct WalV8CompletedGroupEvent {
    pub(crate) group_id: u64,
    pub(crate) tickets: Range<u64>,
    pub(crate) write_count: u64,
    pub(crate) write_bytes: u64,
    #[cfg(feature = "bench")]
    pub(crate) completion_observed_at: Option<Instant>,
}

impl TicketProgressRing {
    fn new(capacity: usize, segment_incarnation: [u8; 16]) -> Result<Self> {
        ensure!(capacity > 0, "v8 progress ring capacity must be nonempty");
        ensure!(
            capacity <= MAX_PARALLEL_INFLIGHT_TICKETS,
            "v8 progress ring capacity exceeds the bounded retirement scratch capacity"
        );
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(capacity)
            .context("reserve v8 progress ring cells")?;
        cells.resize_with(capacity, TicketProgressCell::default);

        Ok(Self {
            segment_incarnation,
            cells: cells.into_boxed_slice(),
            poisoned: AtomicBool::new(false),
        })
    }

    fn cell(&self, ticket: u64) -> Result<&TicketProgressCell> {
        let capacity =
            u64::try_from(self.cells.len()).context("v8 progress ring capacity exceeds u64")?;
        let index =
            usize::try_from(ticket % capacity).context("v8 progress ring index exceeds usize")?;
        self.cells
            .get(index)
            .context("v8 progress ring index is outside its capacity")
    }

    fn ensure_free(&self, ticket: u64) -> Result<()> {
        let payload = self.cell(ticket)?.payload.lock();
        ensure!(
            *payload == TicketProgressPayload::default(),
            "v8 progress ring slot is still occupied"
        );
        Ok(())
    }

    fn initialize(
        &self,
        assignment: WalV8BatchAssignment,
        logical_batch_bytes: usize,
    ) -> Result<()> {
        ensure!(
            assignment.segment_incarnation == self.segment_incarnation,
            "v8 progress ring assignment belongs to another segment"
        );
        let logical_batch_bytes =
            u64::try_from(logical_batch_bytes).context("v8 logical batch size exceeds u64")?;
        let mut payload = self.cell(assignment.segment_ticket)?.payload.lock();
        ensure!(
            *payload == TicketProgressPayload::default(),
            "v8 progress ring slot is still occupied"
        );
        payload.identity = Some(TicketCellIdentity {
            segment_incarnation: assignment.segment_incarnation,
            ticket: assignment.segment_ticket,
        });
        payload.logical_batch_bytes = Some(logical_batch_bytes);
        Ok(())
    }

    fn publish_digest(&self, digest: WalV8DigestReady) -> Result<bool> {
        let mut payload = self.cell(digest.segment_ticket)?.payload.lock();
        ensure!(
            !self.poisoned.load(Ordering::Acquire),
            "v8 Parallel coordinator is poisoned"
        );
        ensure!(
            digest.segment_incarnation == self.segment_incarnation
                && payload.identity
                    == Some(TicketCellIdentity {
                        segment_incarnation: digest.segment_incarnation,
                        ticket: digest.segment_ticket,
                    }),
            "v8 progress ring digest names a stale or foreign ticket"
        );
        ensure!(
            payload.logical_batch_bytes == Some(digest.logical_batch_bytes),
            "v8 progress ring digest length differs from its admitted ticket"
        );
        match payload.digest_ready {
            Some(existing) if existing == digest => Ok(false),
            Some(_) => anyhow::bail!("conflicting v8 progress ring digest"),
            None => {
                payload.digest_ready = Some(digest);
                Ok(true)
            }
        }
    }

    fn mark_frames_ready(&self, segment_incarnation: [u8; 16], ticket: u64) -> Result<bool> {
        let mut payload = self.cell(ticket)?.payload.lock();
        ensure!(
            !self.poisoned.load(Ordering::Acquire),
            "v8 Parallel coordinator is poisoned"
        );
        ensure!(
            segment_incarnation == self.segment_incarnation
                && payload.identity
                    == Some(TicketCellIdentity {
                        segment_incarnation,
                        ticket,
                    }),
            "v8 progress ring frame-ready event names a stale or foreign ticket"
        );
        if payload.frames_ready {
            return Ok(false);
        }
        payload.frames_ready = true;
        Ok(true)
    }

    fn validate_data_completion(&self, segment_incarnation: [u8; 16], ticket: u64) -> Result<()> {
        let payload = self.cell(ticket)?.payload.lock();
        ensure!(
            segment_incarnation == self.segment_incarnation
                && payload.identity
                    == Some(TicketCellIdentity {
                        segment_incarnation,
                        ticket,
                    })
                && payload.frames_ready,
            "v8 DATA completion names a stale or frame-unready ticket"
        );
        Ok(())
    }

    fn read(&self, segment_incarnation: [u8; 16], ticket: u64) -> Result<TicketProgressPayload> {
        let payload = *self.cell(ticket)?.payload.lock();
        ensure!(
            segment_incarnation == self.segment_incarnation
                && payload.identity
                    == Some(TicketCellIdentity {
                        segment_incarnation,
                        ticket,
                    }),
            "v8 progress ring read names a stale or foreign ticket"
        );
        Ok(payload)
    }

    fn retire_prefix(&self, ticket_start: u64, ticket_end: u64) -> Result<()> {
        let ticket_count = ticket_end
            .checked_sub(ticket_start)
            .context("v8 progress ring retirement range moves backwards")?;
        ensure!(
            ticket_count > 0,
            "v8 progress ring retirement range is empty"
        );
        let ticket_count = usize::try_from(ticket_count)
            .context("v8 progress ring retirement range exceeds usize")?;
        ensure!(
            ticket_count <= self.cells.len(),
            "v8 progress ring retirement exceeds its capacity"
        );

        // The Coordinator lock excludes admission and reuse. Once a cell
        // validates here, producers can only repeat the accepted digest or
        // frame event. Neither operation can change the payload. Keep a
        // bounded stack snapshot so the successful path validates and clears
        // each cell while holding its mutex once. If a later cell is invalid,
        // restore the already-cleared prefix before returning the error,
        // preserving all-or-nothing retirement.
        let mut retired_payloads =
            [TicketProgressPayload::default(); MAX_PARALLEL_INFLIGHT_TICKETS];
        let capacity = u64::try_from(self.cells.len())?;
        let start = usize::try_from(ticket_start % capacity)?;
        let (before, after) = self.cells.split_at(start);
        let cells = || after.iter().chain(before).take(ticket_count);
        for (offset, (ticket, cell)) in (ticket_start..ticket_end).zip(cells()).enumerate() {
            let mut payload = cell.payload.lock();
            if let Err(error) = self.validate_retiring_cell(ticket, &payload) {
                drop(payload);
                for (restore_cell, previous) in
                    cells().take(offset).zip(retired_payloads[..offset].iter())
                {
                    *restore_cell.payload.lock() = *previous;
                }
                return Err(error);
            }
            retired_payloads[offset] = *payload;
            *payload = TicketProgressPayload::default();
        }
        Ok(())
    }

    fn validate_retiring_cell(&self, ticket: u64, payload: &TicketProgressPayload) -> Result<()> {
        ensure!(
            payload.identity
                == Some(TicketCellIdentity {
                    segment_incarnation: self.segment_incarnation,
                    ticket,
                })
                && payload.logical_batch_bytes.is_some()
                && payload.frames_ready
                && payload.digest_ready.is_some_and(|digest| {
                    digest.segment_incarnation == self.segment_incarnation
                        && digest.segment_ticket == ticket
                        && Some(digest.logical_batch_bytes) == payload.logical_batch_bytes
                }),
            "v8 progress ring durable prefix contains an incomplete ticket"
        );
        Ok(())
    }

    #[cfg(test)]
    fn snapshot(&self, ticket: u64) -> Result<TicketProgressPayload> {
        self.read(self.segment_incarnation, ticket)
    }
}

#[derive(Clone, Copy)]
struct WalV8ReadyCursor {
    ticket_end: u64,
    #[cfg(feature = "bench")]
    data_bytes: u64,
}

struct CoordinatorState {
    durable_ticket_end: u64,
    durable_prefix_digest: WalV8Digest,
    folded_ticket_end: u64,
    folded_prefix_digest: WalV8Digest,
    /// Exclusive end of the prefix whose frames, DATA and ordered digest
    /// were validated by the driver. Only Coordinator-owned state moves it.
    ready_cursor: Option<WalV8ReadyCursor>,
    /// Cursor mode lends this buffer to the elected driver until handoff.
    completion_batch: Option<Vec<WalV8CompletedGroupEvent>>,
    #[cfg(feature = "bench")]
    progress_diagnostics: WalV8ProgressDiagnostics,
    #[cfg(feature = "bench")]
    last_scanned_ready_ticket_end: u64,
    next_ticket: u64,
    next_data_offset: u64,
    last_commit_ts: u64,
    last_recorded_at: Option<RecordedAt>,
    reserved_bytes: u64,
    reserved_preparation_bytes: u64,
    tickets: VecDeque<TicketState>,
    pending_frontier: Option<WalV8FrontierReservation>,
    last_frontier_end: u64,
    /// Tickets whose DATA was allocated before the previous FRONTIER marker.
    /// The next marker must cover this whole physical window so its durable
    /// byte boundary cannot split a later DATA ticket.
    last_frontier_ticket_end: u64,
    change_generation: u64,
    durability_generation: u64,
    durability_driver_active: bool,
    closed: bool,
    poisoned: bool,
    #[cfg(test)]
    ticket_admission_waiters: usize,
    #[cfg(test)]
    frontier_coalesce_waiters: usize,
    #[cfg(test)]
    durability_waiters: usize,
    #[cfg(test)]
    durable_followers: usize,
    #[cfg(feature = "bench")]
    ordered_link_hash_cpu_ns: u64,
    #[cfg(feature = "bench")]
    completion_timeline_frontier_ready_end: u64,
    #[cfg(feature = "bench")]
    completion_timeline_samples: BTreeMap<u64, PendingCompletionTimeline>,
    #[cfg(feature = "bench")]
    durability_driver_waiters: u64,
    #[cfg(feature = "bench")]
    last_durability_notify_at: Option<Instant>,
    #[cfg(feature = "bench")]
    last_durability_notify_waiters: u64,
    #[cfg(feature = "bench")]
    durable_publish_sample_timestamps: BTreeMap<u64, Instant>,
}

#[cfg(feature = "bench")]
struct WalV8FollowerWaitSample {
    sample_id: Option<u64>,
    requested_ticket_end: u64,
    durable_ticket_end_on_resume: u64,
    coordinator_lock_wait_ns: u64,
    coordinator_lock_wait_cpu_ns: u64,
    wait_started: Option<Instant>,
    wait_cpu_started: Option<u64>,
    condvar_wait_count: u64,
    notify_to_resume_ns: Option<u64>,
    followers_woken_by_last_notify: u64,
}

#[cfg(feature = "bench")]
fn make_follower_wait_observation(
    sample: WalV8FollowerWaitSample,
) -> Option<crate::mem_table::WalV8FollowerWaitObservation> {
    let sample_id = sample.sample_id?;
    let wait_started = sample.wait_started?;
    let wait_cpu_started = sample.wait_cpu_started?;

    Some(crate::mem_table::WalV8FollowerWaitObservation {
        sample_id,
        requested_ticket_end: sample.requested_ticket_end,
        durable_ticket_end_on_resume: sample.durable_ticket_end_on_resume,
        coordinator_lock_wait_ns: sample.coordinator_lock_wait_ns,
        coordinator_lock_wait_cpu_ns: sample.coordinator_lock_wait_cpu_ns,
        total_wait_wall_ns: wait_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        total_wait_cpu_ns: super::digest::thread_cpu_time_ns().saturating_sub(wait_cpu_started),
        condvar_wait_count: sample.condvar_wait_count,
        notify_to_resume_ns: sample.notify_to_resume_ns,
        followers_woken_by_last_notify: sample.followers_woken_by_last_notify,
    })
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WalV8DurabilityPublishMetrics {
    pub(crate) coordinator_lock_wait_ns: u64,
    pub(crate) coordinator_lock_wait_cpu_ns: u64,
    pub(crate) publish_wall_ns: u64,
    pub(crate) publish_cpu_ns: u64,
    pub(crate) notify_all_wall_ns: u64,
    pub(crate) followers_waiting: u64,
}

struct TicketState {
    assignment: WalV8BatchAssignment,
    plan: WalV8AdmissionPlan,
    reserved_bytes: u64,
    frames_ready: bool,
    digest_ready: Option<WalV8DigestReady>,
    prefix_digest_after: Option<WalV8Digest>,
    data_complete: bool,
    #[cfg(feature = "bench")]
    data_write_started_at: Option<Instant>,
    #[cfg(feature = "bench")]
    data_completed_at: Option<Instant>,
    #[cfg(feature = "bench")]
    digest_hol_started_at: Option<Instant>,
    #[cfg(feature = "bench")]
    data_completion_hol_started_at: Option<Instant>,
}

#[derive(Default)]
struct WalV8DataGroupCompletionMetrics {
    changed: bool,
    #[cfg(feature = "bench")]
    ticket_lookup_wall_ns: Option<u64>,
    #[cfg(feature = "bench")]
    ticket_lookup_cpu_ns: Option<u64>,
    #[cfg(feature = "bench")]
    ticket_lookup_entries_scanned: Option<u64>,
    #[cfg(feature = "bench")]
    ticket_validation_wall_ns: Option<u64>,
    #[cfg(feature = "bench")]
    ticket_validation_cpu_ns: Option<u64>,
    #[cfg(feature = "bench")]
    mark_wall_ns: Option<u64>,
    #[cfg(feature = "bench")]
    mark_cpu_ns: Option<u64>,
    #[cfg(feature = "bench")]
    prefix_end_before: Option<u64>,
}

/// Validate and apply one worker completion while the caller holds the
/// Coordinator state lock. The complete range is checked before any ticket is
/// marked so FRONTIER formation cannot observe a partial worker group.
#[inline]
fn apply_data_group_completion_locked(
    inner: &CoordinatorInner,
    state: &mut CoordinatorState,
    tickets: &Range<u64>,
    #[cfg(feature = "bench")] timeline_sampled: bool,
    #[cfg(feature = "bench")] data_completed_at: Option<Instant>,
) -> Result<WalV8DataGroupCompletionMetrics> {
    ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
    let ticket_count = tickets
        .end
        .checked_sub(tickets.start)
        .context("v8 DATA completion group has an invalid ticket range")?;
    let ticket_count = usize::try_from(ticket_count)
        .context("v8 DATA completion group exceeds addressable tickets")?;
    ensure!(ticket_count > 0, "v8 DATA completion group is empty");

    #[cfg(feature = "bench")]
    let ticket_lookup_started_at = timeline_sampled.then(Instant::now);
    #[cfg(feature = "bench")]
    let ticket_lookup_started_cpu =
        timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
    let start_index = if state.ready_cursor.is_some() {
        tickets
            .start
            .checked_sub(state.durable_ticket_end)
            .and_then(|offset| {
                usize::try_from(offset)
                    .ok()
                    .filter(|&index| index < state.tickets.len())
            })
    } else {
        state
            .tickets
            .iter()
            .position(|entry| entry.assignment.segment_ticket == tickets.start)
    }
    .context("v8 DATA-completion group names an unknown ticket")?;
    let mut metrics = WalV8DataGroupCompletionMetrics::default();
    #[cfg(feature = "bench")]
    if timeline_sampled {
        metrics.ticket_lookup_wall_ns = Some(
            completion_timeline_duration_ns(ticket_lookup_started_at, Some(Instant::now()))
                .unwrap_or_default(),
        );
        metrics.ticket_lookup_cpu_ns = Some(
            crate::wal::v8::digest::thread_cpu_time_ns()
                .saturating_sub(ticket_lookup_started_cpu.unwrap_or_default()),
        );
        metrics.ticket_lookup_entries_scanned = Some(start_index.saturating_add(1) as u64);
    }

    #[cfg(feature = "bench")]
    let ticket_validation_started_at = timeline_sampled.then(Instant::now);
    #[cfg(feature = "bench")]
    let ticket_validation_started_cpu =
        timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
    let end_index = start_index
        .checked_add(ticket_count)
        .context("v8 DATA-completion group index overflows")?;
    ensure!(
        end_index <= state.tickets.len(),
        "v8 DATA-completion group extends past admitted tickets"
    );

    let mut all_complete = true;
    for (offset, entry) in state
        .tickets
        .iter()
        .skip(start_index)
        .take(ticket_count)
        .enumerate()
    {
        let expected_ticket = tickets
            .start
            .checked_add(u64::try_from(offset)?)
            .context("v8 DATA-completion ticket overflows")?;
        ensure!(
            entry.assignment.segment_ticket == expected_ticket
                && ticket_frames_ready(inner, entry)?,
            "v8 DATA-completion group does not match ready admitted tickets"
        );
        all_complete &= entry.data_complete;
    }
    ensure!(
        all_complete
            || !state
                .tickets
                .iter()
                .skip(start_index)
                .take(ticket_count)
                .any(|entry| entry.data_complete),
        "v8 DATA-completion group partially duplicates prior completion"
    );
    #[cfg(feature = "bench")]
    if timeline_sampled {
        metrics.ticket_validation_wall_ns = Some(
            completion_timeline_duration_ns(ticket_validation_started_at, Some(Instant::now()))
                .unwrap_or_default(),
        );
        metrics.ticket_validation_cpu_ns = Some(
            crate::wal::v8::digest::thread_cpu_time_ns()
                .saturating_sub(ticket_validation_started_cpu.unwrap_or_default()),
        );
    }

    if all_complete {
        return Ok(metrics);
    }

    #[cfg(feature = "bench")]
    if timeline_sampled {
        metrics.prefix_end_before = Some(data_complete_prefix_end(state));
    }
    #[cfg(feature = "bench")]
    let mark_started_at = timeline_sampled.then(Instant::now);
    #[cfg(feature = "bench")]
    let mark_started_cpu = timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
    for entry in state
        .tickets
        .iter_mut()
        .skip(start_index)
        .take(ticket_count)
    {
        entry.data_complete = true;
        #[cfg(feature = "bench")]
        {
            entry.data_completed_at = data_completed_at;
        }
    }
    metrics.changed = true;
    #[cfg(feature = "bench")]
    {
        if timeline_sampled {
            metrics.mark_wall_ns = Some(
                completion_timeline_duration_ns(mark_started_at, Some(Instant::now()))
                    .unwrap_or_default(),
            );
            metrics.mark_cpu_ns = Some(
                crate::wal::v8::digest::thread_cpu_time_ns()
                    .saturating_sub(mark_started_cpu.unwrap_or_default()),
            );
        }
    }

    Ok(metrics)
}

#[cfg(feature = "bench")]
struct PendingCompletionTimeline {
    group: crate::wal::parallel_worker::WalWorkerGroupCompletionTimeline,
    reaper_recv_to_data_complete_publish_ns: Option<u64>,
    data_complete_lock_wait_wall_ns: Option<u64>,
    data_complete_lock_wait_cpu_ns: Option<u64>,
    data_complete_lock_hold_wall_ns: Option<u64>,
    data_complete_lock_hold_cpu_ns: Option<u64>,
    data_complete_prefix_end_before: Option<u64>,
    data_complete_prefix_end_after: Option<u64>,
    hol_blocker_ticket: Option<u64>,
    hol_blocker_frames_ready: Option<bool>,
    hol_blocker_digest_folded: Option<bool>,
    hol_blocker_data_write_elapsed_ns: Option<u64>,
    prefix_ready_at: Option<Instant>,
}

/// Immutable view of one ticket in the admission window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8TicketProgress {
    pub(crate) assignment: WalV8BatchAssignment,
    pub(crate) logical_batch_bytes: usize,
    pub(crate) frame_buffer_bytes: usize,
    pub(crate) frames_ready: bool,
    pub(crate) digest_ready: Option<WalV8DigestReady>,
    /// Ordered prefix digest after this ticket, once all preceding tickets
    /// have published their producer digests.
    pub(crate) prefix_digest_after: Option<WalV8Digest>,
    pub(crate) data_complete: bool,
    #[cfg(feature = "bench")]
    pub(crate) data_write_elapsed_ns: Option<u64>,
    #[cfg(feature = "bench")]
    pub(crate) data_completed_at: Option<Instant>,
}

impl WalV8TicketProgress {
    fn is_frontier_ready(&self) -> bool {
        self.frames_ready
            && self.digest_ready.is_some()
            && self.prefix_digest_after.is_some()
            && self.data_complete
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WalV8FrontierTail {
    pub(crate) first_data_offset: u64,
    pub(crate) commit_ts: u64,
    pub(crate) frame_buffer_bytes: usize,
}

pub(crate) enum WalV8ReadyPrefix {
    Tickets(Vec<WalV8TicketProgress>),
    Cursor {
        ticket_count: usize,
        tail: Option<WalV8FrontierTail>,
        #[cfg(feature = "bench")]
        data_bytes: u64,
    },
}

impl WalV8ReadyPrefix {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Tickets(tickets) => tickets.len(),
            Self::Cursor { ticket_count, .. } => *ticket_count,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn tail(&self) -> Option<WalV8FrontierTail> {
        match self {
            Self::Tickets(tickets) => tickets.last().map(|ticket| WalV8FrontierTail {
                first_data_offset: ticket.assignment.first_data_offset,
                commit_ts: ticket.assignment.commit_ts,
                frame_buffer_bytes: ticket.frame_buffer_bytes,
            }),
            Self::Cursor { tail, .. } => *tail,
        }
    }

    #[cfg(feature = "bench")]
    pub(crate) fn data_bytes(&self) -> u64 {
        match self {
            Self::Tickets(tickets) => tickets.iter().fold(0_u64, |total, ticket| {
                total.saturating_add(ticket.frame_buffer_bytes as u64)
            }),
            Self::Cursor { data_bytes, .. } => *data_bytes,
        }
    }
}

/// View of the durability decision point. `next_not_ready` is the first
/// admitted ticket beyond the ready prefix. Ring snapshots collect blocker
/// and readiness details only when a diagnostic timestamp is requested.
pub(crate) struct WalV8FrontierReadySnapshot {
    pub(crate) ready_prefix: WalV8ReadyPrefix,
    pub(crate) admitted_ticket_count: usize,
    pub(crate) closed: bool,
    #[cfg(feature = "bench")]
    pub(crate) frames_ready_prefix_count: usize,
    #[cfg(feature = "bench")]
    pub(crate) data_complete_prefix_count: usize,
    #[cfg(feature = "bench")]
    pub(crate) reserved_preparation_bytes: u64,
    #[cfg(feature = "bench")]
    pub(crate) next_not_ready: Option<WalV8TicketProgress>,
    #[cfg(feature = "bench")]
    pub(crate) data_hol_blocker: Option<WalV8TicketProgress>,
    #[cfg(feature = "bench")]
    pub(crate) snapshot_monotonic_ns: Option<u64>,
}

/// Optional timing breakdown for one Cursor completion drain and progress
/// advance. The driver supplies this only for low-frequency bench samples.
#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WalV8ProgressAdvanceTiming {
    pub(crate) coordinator_lock_wait_wall_ns: u64,
    pub(crate) coordinator_lock_wait_cpu_ns: u64,
    pub(crate) coordinator_lock_hold_wall_ns: u64,
    pub(crate) coordinator_lock_hold_cpu_ns: u64,
    pub(crate) completion_apply_wall_ns: u64,
    pub(crate) completion_apply_cpu_ns: u64,
    pub(crate) digest_fold_wall_ns: u64,
    pub(crate) digest_fold_cpu_ns: u64,
    pub(crate) cursor_advance_wall_ns: u64,
    pub(crate) cursor_advance_cpu_ns: u64,
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WalV8CoalescingObservation {
    pub(crate) waited: bool,
    pub(crate) state_lock_wait_wall_ns: u64,
    pub(crate) state_lock_wait_cpu_ns: u64,
    pub(crate) condvar_wait_wall_ns: u64,
    pub(crate) condvar_wait_count: u64,
    pub(crate) condvar_reacquire_count: u64,
    /// Non-timeout returns, including spurious condition-variable wakeups.
    pub(crate) condvar_non_timeout_returns: u64,
    pub(crate) condvar_prefix_growth_returns: u64,
    pub(crate) condvar_generation_changed_returns: u64,
    pub(crate) condvar_timeout_returns: u64,
    pub(crate) loop_check_count: u64,
    pub(crate) condvar_wait_cpu_ns: u64,
}

/// DATA group membership comes from admission, including tickets whose
/// producers have not encoded or framed their bytes yet.
pub(crate) struct WalV8DataGroupWindow {
    pub(crate) ticket_end: u64,
    pub(crate) closed: bool,
}

/// The reservation for appending a FRONTIER after the current physical tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8FrontierReservation {
    pub(crate) ticket_end: u64,
    pub(crate) frame_offset: u64,
    /// Exclusive ticket boundary whose DATA offsets precede this marker.
    pub(crate) preceding_ticket_end: u64,
    /// RFC 026 logical prefix digest for exactly `ticket_end`.
    pub(crate) prefix_digest: WalV8Digest,
}

/// Wake result returned by a bounded admission/readiness wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8CoordinatorWake {
    pub(crate) change_generation: u64,
    pub(crate) closed: bool,
    pub(crate) poisoned: bool,
}

/// Opaque proof that the coordinator checked a durable ticket prefix.
pub(crate) struct WalV8DurablePrefixCheck {
    coordinator: Arc<CoordinatorInner>,
    ticket_end: u64,
}

impl WalV8DurablePrefixCheck {
    fn covers(&self, coordinator: &WalV8ParallelCoordinator, ticket: u64) -> bool {
        Arc::ptr_eq(&self.coordinator, &coordinator.inner) && ticket < self.ticket_end
    }
}

impl WalV8ParallelCoordinator {
    pub(crate) fn worker_completion_handle(&self) -> WalV8WorkerCompletionHandle {
        WalV8WorkerCompletionHandle {
            coordinator: self.clone(),
            completion_token_wake_enabled: true,
        }
    }

    #[cfg(feature = "bench")]
    pub(crate) fn worker_completion_handle_with_wake_mode(
        &self,
        mode: WalV8DirectCompletionWakeMode,
    ) -> WalV8WorkerCompletionHandle {
        WalV8WorkerCompletionHandle {
            coordinator: self.clone(),
            completion_token_wake_enabled: mode == WalV8DirectCompletionWakeMode::Token,
        }
    }

    pub(crate) fn progress_ring_enabled(&self) -> bool {
        self.inner.progress_ring.is_some()
    }

    /// Build a fresh coordinator with the requested readiness backend before
    /// the writer is attached to the parallel pipeline. Reconfiguration is
    /// only valid while the recovered writer has no admitted work.
    pub(crate) fn with_progress_mode(
        &self,
        header: WalV8Header,
        mode: WalV8ProgressMode,
    ) -> Result<Self> {
        if self.inner.progress_mode == mode {
            return Ok(self.clone());
        }

        let state = self.inner.state.lock();
        ensure!(
            state.tickets.is_empty()
                && state.next_ticket == state.durable_ticket_end
                && state.reserved_bytes == 0
                && state.reserved_preparation_bytes == 0
                && state.pending_frontier.is_none()
                && !state.durability_driver_active
                && !state.closed
                && !state.poisoned,
            "v8 progress mode must be selected before Parallel admission"
        );
        ensure!(
            state.next_data_offset == state.last_frontier_end,
            "v8 progress mode requires a clean recovered frontier"
        );

        Self::new(
            header,
            WalV8ParallelConfig {
                next_ticket: state.next_ticket,
                next_data_offset: state.next_data_offset,
                last_commit_ts: state.last_commit_ts,
                last_recorded_at: state.last_recorded_at,
                durable_prefix_digest: state.durable_prefix_digest,
                max_inflight_tickets: self.inner.max_inflight_tickets,
                max_inflight_bytes: self.inner.max_inflight_bytes,
                max_file_size: self.inner.max_file_size,
                progress_mode: mode,
            },
        )
    }

    pub(crate) fn completion_queue_capacity(&self) -> usize {
        self.inner.max_inflight_tickets
    }

    pub(crate) fn progress_cursor_enabled(&self) -> bool {
        self.inner.progress_mode == WalV8ProgressMode::Cursor
    }

    pub(crate) fn take_progress_token_receiver(&self) -> Result<Option<Receiver<()>>> {
        let Some(receiver) = self.inner.progress_token_receiver.lock().take() else {
            if self.progress_ring_enabled() {
                anyhow::bail!("v8 progress token receiver was already taken");
            }
            return Ok(None);
        };
        Ok(Some(receiver))
    }

    /// Nonblocking persistent wake token for F0 readiness. A full capacity-one
    /// channel already represents pending progress.
    pub(crate) fn signal_progress_token(&self) {
        signal_progress_token(&self.inner);
    }

    #[cfg(feature = "bench")]
    pub(super) fn progress_diagnostics(&self) -> Option<WalV8ProgressDiagnostics> {
        let enabled = self
            .inner
            .data_completion_timestamps_enabled
            .load(Ordering::Acquire)
            || self.inner.progress_counters_enabled.load(Ordering::Acquire);
        if !self.progress_ring_enabled() || !enabled {
            return None;
        }
        Some(WalV8ProgressDiagnostics {
            token_accepts: self.inner.progress_token_accepts.load(Ordering::Relaxed),
            token_attempts: self.inner.progress_token_attempts.load(Ordering::Relaxed),
            ..self.inner.state.lock().progress_diagnostics
        })
    }

    fn publish_progress_digest(&self, digest: WalV8DigestReady, before_data: bool) -> Result<bool> {
        let Some(progress_ring) = &self.inner.progress_ring else {
            anyhow::bail!("v8 per-ticket progress ring is disabled");
        };
        let changed = match progress_ring.publish_digest(digest) {
            Ok(changed) => changed,
            Err(error) => {
                self.poison();
                return Err(error).context("publish v8 progress ring digest");
            }
        };
        // FrameJob can only submit this ticket after publishing its digest.
        // Its completion will signal the owner before the ticket can become
        // ready. Independent digest events may arrive after DATA completion.
        if changed && !(before_data && self.progress_cursor_enabled()) {
            self.signal_progress_token();
        }
        Ok(changed)
    }

    pub(crate) fn publish_progress_frames_ready(&self, ticket: u64) -> Result<bool> {
        let Some(progress_ring) = &self.inner.progress_ring else {
            anyhow::bail!("v8 per-ticket progress ring is disabled");
        };
        // Buffer insertion wakes the submitter. The owner cannot use this
        // frame until the Reaper enqueues its DATA completion.
        match progress_ring.mark_frames_ready(self.inner.segment_incarnation, ticket) {
            Ok(changed) => Ok(changed),
            Err(error) => {
                self.poison();
                Err(error).context("publish v8 progress ring frame readiness")
            }
        }
    }

    pub(crate) fn owns_durability_driver(&self, driver: &WalV8DurabilityDriver) -> bool {
        // The guard keeps ownership until it is dropped.
        Arc::ptr_eq(&self.inner, &driver.coordinator.inner)
    }

    /// Wake the elected durability driver after a completion has been placed
    /// in its private queue. The queue itself is the source of truth.
    pub(crate) fn notify_completion_queued(&self) {
        if self.progress_ring_enabled() {
            self.signal_progress_token();
            return;
        }
        let mut state = self.inner.state.lock();
        if !state.poisoned {
            signal_durability_progress(&self.inner, &mut state);
        }
    }

    /// Create a bounded ticket coordinator positioned at a recovered Active
    /// image. Source-spool capacity is managed by the caller; preparation
    /// indexes are charged against this coordinator's shared byte window.
    pub(crate) fn new(header: WalV8Header, config: WalV8ParallelConfig) -> Result<Self> {
        ensure!(
            config.max_inflight_tickets > 0,
            "v8 Parallel ticket window must be nonempty"
        );
        ensure!(
            config.max_inflight_bytes > 0,
            "v8 Parallel byte window must be nonempty"
        );
        ensure!(
            (DATA_START..=MAX_WAL_FILE_SIZE).contains(&config.max_file_size),
            "invalid v8 Parallel segment size"
        );
        ensure!(
            config.next_data_offset >= DATA_START
                && config.next_data_offset.is_multiple_of(FRAME_LEN_U64)
                && config.next_data_offset <= config.max_file_size,
            "invalid v8 Parallel next DATA offset"
        );
        if config.next_ticket == 0 {
            ensure!(
                config.last_commit_ts == 0 && config.last_recorded_at.is_none(),
                "v8 Parallel empty recovery has nonempty timestamp state"
            );
        } else {
            ensure!(
                config.last_commit_ts > 0 && config.last_recorded_at.is_some(),
                "v8 Parallel recovered ticket is missing timestamp state"
            );
        }
        if let Some(recorded_at) = config.last_recorded_at {
            ensure!(
                recorded_at.nanos < 1_000_000_000,
                "v8 Parallel recovered recorded-at timestamp is invalid"
            );
        }

        let physical_header_digest = header.physical_digest()?;
        let initial_prefix_digest = header.initial_prefix_digest()?;
        if config.next_ticket == 0 {
            ensure!(
                config.durable_prefix_digest == initial_prefix_digest,
                "v8 empty recovery prefix digest differs from P_0"
            );
        }
        let mut tickets = VecDeque::new();
        tickets
            .try_reserve_exact(config.max_inflight_tickets)
            .context("reserve v8 Parallel ticket slots")?;
        let progress_ring = config
            .progress_mode
            .uses_ring()
            .then(|| TicketProgressRing::new(config.max_inflight_tickets, header.incarnation))
            .transpose()?;
        let ready_cursor =
            (config.progress_mode == WalV8ProgressMode::Cursor).then_some(WalV8ReadyCursor {
                ticket_end: config.next_ticket,
                #[cfg(feature = "bench")]
                data_bytes: 0,
            });
        let completion_batch = if ready_cursor.is_some() {
            let mut batch = Vec::new();
            batch
                .try_reserve_exact(config.max_inflight_tickets)
                .context("reserve v8 completion event drain buffer")?;
            Some(batch)
        } else {
            None
        };
        let (progress_token_sender, progress_token_receiver) = if progress_ring.is_some() {
            let (sender, receiver) = crossbeam_channel::bounded(1);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };

        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState {
                    durable_ticket_end: config.next_ticket,
                    durable_prefix_digest: config.durable_prefix_digest,
                    folded_ticket_end: config.next_ticket,
                    folded_prefix_digest: config.durable_prefix_digest,
                    ready_cursor,
                    completion_batch,
                    #[cfg(feature = "bench")]
                    progress_diagnostics: WalV8ProgressDiagnostics::default(),
                    #[cfg(feature = "bench")]
                    last_scanned_ready_ticket_end: config.next_ticket,
                    next_ticket: config.next_ticket,
                    next_data_offset: config.next_data_offset,
                    last_commit_ts: config.last_commit_ts,
                    last_recorded_at: config.last_recorded_at,
                    reserved_bytes: 0,
                    reserved_preparation_bytes: 0,
                    tickets,
                    pending_frontier: None,
                    last_frontier_end: config.next_data_offset,
                    last_frontier_ticket_end: config.next_ticket,
                    change_generation: 0,
                    durability_generation: 0,
                    durability_driver_active: false,
                    closed: false,
                    poisoned: false,
                    #[cfg(test)]
                    ticket_admission_waiters: 0,
                    #[cfg(test)]
                    frontier_coalesce_waiters: 0,
                    #[cfg(test)]
                    durability_waiters: 0,
                    #[cfg(test)]
                    durable_followers: 0,
                    #[cfg(feature = "bench")]
                    ordered_link_hash_cpu_ns: 0,
                    #[cfg(feature = "bench")]
                    completion_timeline_frontier_ready_end: config.next_ticket,
                    #[cfg(feature = "bench")]
                    completion_timeline_samples: BTreeMap::new(),
                    #[cfg(feature = "bench")]
                    durability_driver_waiters: 0,
                    #[cfg(feature = "bench")]
                    last_durability_notify_at: None,
                    #[cfg(feature = "bench")]
                    last_durability_notify_waiters: 0,
                    #[cfg(feature = "bench")]
                    durable_publish_sample_timestamps: BTreeMap::new(),
                }),
                changed: Condvar::new(),
                durability_changed: Condvar::new(),
                durable_published: Condvar::new(),
                admitted_ticket_end: AtomicU64::new(config.next_ticket),
                #[cfg(feature = "bench")]
                data_completion_timestamps_enabled: std::sync::atomic::AtomicBool::new(false),
                #[cfg(feature = "bench")]
                progress_counters_enabled: AtomicBool::new(false),
                #[cfg(feature = "bench")]
                hol_diagnostics_enabled: std::sync::atomic::AtomicBool::new(false),
                #[cfg(feature = "bench")]
                profile: arc_swap::ArcSwapOption::empty(),
                segment_incarnation: header.incarnation,
                physical_header_digest,
                initial_prefix_digest,
                max_inflight_tickets: config.max_inflight_tickets,
                max_inflight_bytes: config.max_inflight_bytes,
                max_file_size: config.max_file_size,
                progress_mode: config.progress_mode,
                progress_ring,
                progress_token_sender,
                progress_token_receiver: Mutex::new(progress_token_receiver),
                #[cfg(feature = "bench")]
                progress_token_attempts: AtomicU64::new(0),
                #[cfg(feature = "bench")]
                progress_token_accepts: AtomicU64::new(0),
            }),
        })
    }

    /// Return all admitted tickets, including producers that have not finished
    /// encoding. This is the group-visibility source for the Parallel packer.
    pub(crate) fn admitted_window(&self) -> Vec<WalV8TicketProgress> {
        let mut state = self.inner.state.lock();
        match state
            .tickets
            .iter()
            .map(|ticket| ticket_progress(&self.inner, ticket))
            .collect::<Result<Vec<_>>>()
        {
            Ok(progress) => progress,
            Err(_) => {
                poison_locked(&self.inner, &mut state);
                Vec::new()
            }
        }
    }

    pub(crate) fn data_group_window(
        &self,
        first_ticket: u64,
        max_tickets: usize,
    ) -> Result<WalV8DataGroupWindow> {
        ensure!(max_tickets > 0, "v8 DATA group window must be nonempty");
        let state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        ensure!(
            first_ticket >= state.durable_ticket_end && first_ticket <= state.next_ticket,
            "v8 DATA group starts outside the admitted window"
        );
        let count =
            usize::try_from((state.next_ticket - first_ticket).min(u64::try_from(max_tickets)?))?;
        let ticket_count = count;
        ensure!(ticket_count > 0, "v8 DATA group has no admitted tickets");

        Ok(WalV8DataGroupWindow {
            ticket_end: first_ticket + u64::try_from(ticket_count)?,
            closed: state.closed,
        })
    }

    pub(crate) fn admitted_ticket_end(&self) -> u64 {
        self.inner.admitted_ticket_end.load(Ordering::Acquire)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_data_completion_timestamps_enabled(&self, enabled: bool) {
        self.inner
            .data_completion_timestamps_enabled
            .store(enabled, Ordering::Release);
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_progress_counters_enabled(&self, enabled: bool) -> Result<()> {
        ensure!(
            !enabled || self.progress_cursor_enabled(),
            "lightweight v8 progress counters require Cursor mode"
        );
        let state = self.inner.state.lock();
        ensure!(
            state.next_ticket == state.durable_ticket_end && state.tickets.is_empty(),
            "lightweight v8 progress counters must be configured before admission"
        );
        self.inner
            .progress_counters_enabled
            .store(enabled, Ordering::Release);
        Ok(())
    }

    #[cfg(feature = "bench")]
    pub(crate) fn capture_data_completion_time(&self) -> Option<Instant> {
        self.inner
            .data_completion_timestamps_enabled
            .load(Ordering::Acquire)
            .then(Instant::now)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_hol_diagnostics_profile(&self, profile: Arc<crate::mem_table::WriteProfile>) {
        let enabled = profile.wal_hol_diagnostics_enabled();
        self.inner.profile.store(Some(profile));
        self.inner
            .hol_diagnostics_enabled
            .store(enabled, Ordering::Release);
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_hol_diagnostics_enabled(&self, enabled: bool) {
        self.inner
            .hol_diagnostics_enabled
            .store(enabled, Ordering::Release);
    }

    /// Return the contiguous head prefix whose DATA frames are ready to submit.
    pub(crate) fn frame_ready_prefix(&self) -> Vec<WalV8TicketProgress> {
        let mut state = self.inner.state.lock();
        if state.poisoned {
            return Vec::new();
        }
        let mut prefix = Vec::new();
        for ticket in &state.tickets {
            match ticket_progress(&self.inner, ticket) {
                Ok(progress) if progress.frames_ready => prefix.push(progress),
                Ok(_) => break,
                Err(_) => {
                    poison_locked(&self.inner, &mut state);
                    return Vec::new();
                }
            }
        }
        prefix
    }

    /// Return the contiguous head prefix whose frames, ordered digest fold,
    /// and DATA completion are ready for an embedded FRONTIER.
    pub(crate) fn frontier_ready_prefix(&self) -> Vec<WalV8TicketProgress> {
        let mut state = self.inner.state.lock();
        if state.poisoned {
            return Vec::new();
        }
        let mut prefix = Vec::new();
        for ticket in &state.tickets {
            match ticket_progress(&self.inner, ticket) {
                Ok(progress) if progress.is_frontier_ready() => prefix.push(progress),
                Ok(_) => break,
                Err(_) => {
                    poison_locked(&self.inner, &mut state);
                    return Vec::new();
                }
            }
        }
        if !ready_prefix_reaches_frontier_floor(&state, prefix.len()) {
            prefix.clear();
        }
        prefix
    }

    /// Give already-admitted successors one bounded opportunity to join a
    /// ready durability prefix. A lone producer or fully ready window never
    /// waits, and close/poison terminates the optional wait immediately.
    pub(crate) fn coalesce_frontier_ready(&self, budget: Duration) -> bool {
        #[cfg(feature = "bench")]
        {
            self.coalesce_frontier_ready_observed(budget, false).waited
        }
        #[cfg(not(feature = "bench"))]
        {
            let mut state = self.inner.state.lock();
            let deadline = Instant::now() + budget;
            let mut waited = false;
            loop {
                if state.closed
                    || state.poisoned
                    || !state
                        .tickets
                        .front()
                        .is_some_and(TicketState::is_frontier_ready)
                    || !state
                        .tickets
                        .iter()
                        .any(|ticket| !ticket.is_frontier_ready())
                {
                    break;
                }
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                waited = true;
                #[cfg(test)]
                {
                    state.frontier_coalesce_waiters += 1;
                }
                self.inner
                    .durability_changed
                    .wait_for(&mut state, remaining);
                #[cfg(test)]
                {
                    state.frontier_coalesce_waiters -= 1;
                }
            }

            waited
        }
    }

    #[cfg(feature = "bench")]
    pub(crate) fn coalesce_frontier_ready_sampled(
        &self,
        budget: Duration,
    ) -> WalV8CoalescingObservation {
        self.coalesce_frontier_ready_observed(budget, true)
    }

    #[cfg(feature = "bench")]
    fn coalesce_frontier_ready_observed(
        &self,
        budget: Duration,
        capture_metrics: bool,
    ) -> WalV8CoalescingObservation {
        let lock_wait_started = capture_metrics.then(Instant::now);
        let lock_wait_cpu_started = capture_metrics.then(super::digest::thread_cpu_time_ns);
        let mut observation = WalV8CoalescingObservation::default();
        let mut state = self.inner.state.lock();
        {
            observation.state_lock_wait_wall_ns = lock_wait_started
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or_default();
            observation.state_lock_wait_cpu_ns = lock_wait_cpu_started
                .map(|started| super::digest::thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default();
        }
        let deadline = Instant::now() + budget;
        loop {
            if capture_metrics {
                observation.loop_check_count = observation.loop_check_count.saturating_add(1);
            }
            if state.closed
                || state.poisoned
                || !state
                    .tickets
                    .front()
                    .is_some_and(TicketState::is_frontier_ready)
                || !state
                    .tickets
                    .iter()
                    .any(|ticket| !ticket.is_frontier_ready())
            {
                break;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            observation.waited = true;
            if capture_metrics {
                observation.condvar_wait_count = observation.condvar_wait_count.saturating_add(1);
            }
            #[cfg(test)]
            {
                state.frontier_coalesce_waiters += 1;
            }
            let prior_generation = state.durability_generation;
            let prior_ready = capture_metrics.then(|| {
                state
                    .tickets
                    .iter()
                    .take_while(|ticket| ticket.is_frontier_ready())
                    .count()
            });
            let condvar_cpu_started = capture_metrics.then(super::digest::thread_cpu_time_ns);
            let condvar_wall_started = capture_metrics.then(Instant::now);
            let wait_result = self
                .inner
                .durability_changed
                .wait_for(&mut state, remaining);
            if capture_metrics {
                if let Some(started) = condvar_cpu_started {
                    observation.condvar_wait_cpu_ns =
                        observation.condvar_wait_cpu_ns.saturating_add(
                            super::digest::thread_cpu_time_ns().saturating_sub(started),
                        );
                }
                if let Some(started) = condvar_wall_started {
                    observation.condvar_wait_wall_ns = observation
                        .condvar_wait_wall_ns
                        .saturating_add(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                }
                if wait_result.timed_out() {
                    observation.condvar_timeout_returns =
                        observation.condvar_timeout_returns.saturating_add(1);
                }
                if state.durability_generation != prior_generation {
                    observation.condvar_generation_changed_returns = observation
                        .condvar_generation_changed_returns
                        .saturating_add(1);
                }
                if prior_ready.is_some_and(|before| {
                    state
                        .tickets
                        .iter()
                        .take_while(|ticket| ticket.is_frontier_ready())
                        .count()
                        > before
                }) {
                    observation.condvar_prefix_growth_returns =
                        observation.condvar_prefix_growth_returns.saturating_add(1);
                }
                observation.condvar_reacquire_count =
                    observation.condvar_reacquire_count.saturating_add(1);
                if !wait_result.timed_out() {
                    observation.condvar_non_timeout_returns =
                        observation.condvar_non_timeout_returns.saturating_add(1);
                }
            }
            #[cfg(test)]
            {
                state.frontier_coalesce_waiters -= 1;
            }
        }

        observation
    }

    /// Snapshot the frontier-ready prefix and the first admitted ticket that
    /// would extend it, under one lock acquisition. Benchmark diagnostics use
    /// the blocked ticket's readiness to identify why a durability group
    /// stopped at this boundary.
    pub(crate) fn frontier_ready_snapshot(&self) -> WalV8FrontierReadySnapshot {
        self.frontier_ready_snapshot_inner(false)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn frontier_ready_snapshot_with_monotonic_ns(&self) -> WalV8FrontierReadySnapshot {
        self.frontier_ready_snapshot_inner(true)
    }

    fn frontier_ready_snapshot_inner(
        &self,
        capture_monotonic_ns: bool,
    ) -> WalV8FrontierReadySnapshot {
        #[cfg(not(feature = "bench"))]
        let _ = capture_monotonic_ns;

        let mut state = self.inner.state.lock();
        match frontier_ready_snapshot_locked(&self.inner, &state, capture_monotonic_ns) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                poison_locked(&self.inner, &mut state);
                empty_frontier_ready_snapshot()
            }
        }
    }

    /// Apply queued DATA completions and fold producer digests before taking a
    /// single owner-consistent readiness snapshot. Only the active durability
    /// driver may consume the completion queue or advance the ordered digest.
    pub(crate) fn advance_and_snapshot(
        &self,
        driver: &WalV8DurabilityDriver,
        events: &[WalV8CompletedGroupEvent],
        capture_monotonic_ns: bool,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_inner(
            driver,
            events,
            capture_monotonic_ns,
            #[cfg(feature = "bench")]
            None,
        )
    }

    #[cfg(feature = "bench")]
    pub(crate) fn advance_and_snapshot_with_timing(
        &self,
        driver: &WalV8DurabilityDriver,
        events: &[WalV8CompletedGroupEvent],
        capture_monotonic_ns: bool,
        timing: &mut WalV8ProgressAdvanceTiming,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_inner(driver, events, capture_monotonic_ns, Some(timing))
    }

    fn advance_and_snapshot_inner(
        &self,
        driver: &WalV8DurabilityDriver,
        events: &[WalV8CompletedGroupEvent],
        capture_monotonic_ns: bool,
        #[cfg(feature = "bench")] mut timing: Option<&mut WalV8ProgressAdvanceTiming>,
    ) -> Result<WalV8FrontierReadySnapshot> {
        ensure!(
            self.inner.progress_ring.is_some(),
            "v8 progress-ring advance requested while disabled"
        );
        ensure!(
            self.owns_durability_driver(driver),
            "v8 progress-ring advance belongs to another durability driver"
        );
        #[cfg(not(feature = "bench"))]
        let _ = capture_monotonic_ns;

        #[cfg(feature = "bench")]
        let coordinator_lock_wait_wall_started = timing.as_ref().map(|_| Instant::now());
        #[cfg(feature = "bench")]
        let coordinator_lock_wait_cpu_started = timing
            .as_ref()
            .map(|_| crate::wal::v8::digest::thread_cpu_time_ns());
        let mut state = self.inner.state.lock();
        #[cfg(feature = "bench")]
        let coordinator_lock_hold_wall_started = timing.as_ref().map(|_| Instant::now());
        #[cfg(feature = "bench")]
        let coordinator_lock_hold_cpu_started = timing
            .as_ref()
            .map(|_| crate::wal::v8::digest::thread_cpu_time_ns());
        #[cfg(feature = "bench")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.coordinator_lock_wait_wall_ns =
                timing.coordinator_lock_wait_wall_ns.saturating_add(
                    coordinator_lock_wait_wall_started
                        .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                        .unwrap_or_default(),
                );
            timing.coordinator_lock_wait_cpu_ns =
                timing.coordinator_lock_wait_cpu_ns.saturating_add(
                    coordinator_lock_wait_cpu_started
                        .map(|started| {
                            crate::wal::v8::digest::thread_cpu_time_ns().saturating_sub(started)
                        })
                        .unwrap_or_default(),
                );
        }
        ensure!(
            state.durability_driver_active,
            "v8 progress-ring advance has no active durability driver"
        );
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");

        #[cfg(feature = "bench")]
        let completion_apply_wall_started = timing.as_ref().map(|_| Instant::now());
        #[cfg(feature = "bench")]
        let completion_apply_cpu_started = timing
            .as_ref()
            .map(|_| crate::wal::v8::digest::thread_cpu_time_ns());
        for event in events {
            let applied = apply_data_group_completion_locked(
                &self.inner,
                &mut state,
                &event.tickets,
                #[cfg(feature = "bench")]
                false,
                #[cfg(feature = "bench")]
                event.completion_observed_at,
            );
            if let Err(error) = applied {
                poison_locked(&self.inner, &mut state);
                return Err(error).with_context(|| {
                    format!(
                        "apply v8 DATA completion group {} ({} tickets, {} writes, {} bytes)",
                        event.group_id,
                        event.tickets.end.saturating_sub(event.tickets.start),
                        event.write_count,
                        event.write_bytes
                    )
                });
            }
        }
        #[cfg(feature = "bench")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.completion_apply_wall_ns = timing.completion_apply_wall_ns.saturating_add(
                completion_apply_wall_started
                    .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                    .unwrap_or_default(),
            );
            timing.completion_apply_cpu_ns = timing.completion_apply_cpu_ns.saturating_add(
                completion_apply_cpu_started
                    .map(|started| {
                        crate::wal::v8::digest::thread_cpu_time_ns().saturating_sub(started)
                    })
                    .unwrap_or_default(),
            );
        }

        #[cfg(feature = "bench")]
        let fold_cpu_before = state.ordered_link_hash_cpu_ns;
        #[cfg(feature = "bench")]
        let digest_fold_wall_started = timing.as_ref().map(|_| Instant::now());
        #[cfg(feature = "bench")]
        let digest_fold_cpu_started = timing
            .as_ref()
            .map(|_| crate::wal::v8::digest::thread_cpu_time_ns());
        if let Err(error) = fold_ready_digests(&self.inner, &mut state, None) {
            poison_locked(&self.inner, &mut state);
            return Err(error).context("fold v8 progress-ring digests");
        }
        #[cfg(feature = "bench")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.digest_fold_wall_ns = timing.digest_fold_wall_ns.saturating_add(
                digest_fold_wall_started
                    .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                    .unwrap_or_default(),
            );
            timing.digest_fold_cpu_ns = timing.digest_fold_cpu_ns.saturating_add(
                digest_fold_cpu_started
                    .map(|started| {
                        crate::wal::v8::digest::thread_cpu_time_ns().saturating_sub(started)
                    })
                    .unwrap_or_default(),
            );
        }

        #[cfg(feature = "bench")]
        let cursor_advance_wall_started = timing.as_ref().map(|_| Instant::now());
        #[cfg(feature = "bench")]
        let cursor_advance_cpu_started = timing
            .as_ref()
            .map(|_| crate::wal::v8::digest::thread_cpu_time_ns());
        if let Err(error) = advance_ready_cursor(&mut state) {
            poison_locked(&self.inner, &mut state);
            return Err(error).context("advance v8 ready prefix cursor");
        }
        #[cfg(feature = "bench")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.cursor_advance_wall_ns = timing.cursor_advance_wall_ns.saturating_add(
                cursor_advance_wall_started
                    .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                    .unwrap_or_default(),
            );
            timing.cursor_advance_cpu_ns = timing.cursor_advance_cpu_ns.saturating_add(
                cursor_advance_cpu_started
                    .map(|started| {
                        crate::wal::v8::digest::thread_cpu_time_ns().saturating_sub(started)
                    })
                    .unwrap_or_default(),
            );
        }
        #[cfg(feature = "bench")]
        {
            let fold_cpu_ns = state
                .ordered_link_hash_cpu_ns
                .saturating_sub(fold_cpu_before);
            if fold_cpu_ns != 0
                && let Some(profile) = self.inner.profile.load().as_ref()
            {
                profile.record_wal_v8_ordered_link_hash_cpu_ns(fold_cpu_ns);
            }
        }

        // Applying progress does not free admission capacity. The owner
        // already has the progress token, and durable publication wakes
        // admission and ACK waiters.
        let snapshot = if let Some(cursor) = state.ready_cursor
            && !capture_monotonic_ns
        {
            cursor_frontier_snapshot_locked(&state, cursor)
        } else {
            frontier_ready_snapshot_locked(&self.inner, &state, capture_monotonic_ns)
        };
        match snapshot {
            Ok(snapshot) => {
                #[cfg(feature = "bench")]
                if capture_monotonic_ns
                    || self.inner.progress_counters_enabled.load(Ordering::Acquire)
                {
                    let ready_end = state.durable_ticket_end + snapshot.ready_prefix.len() as u64;
                    let advanced = ready_end > state.last_scanned_ready_ticket_end;
                    state.last_scanned_ready_ticket_end = ready_end;
                    let stats = &mut state.progress_diagnostics;
                    stats.owner_scans += 1;
                    stats.scans_without_ready_progress += u64::from(!advanced);
                    stats.completion_events_drained += events.len() as u64;
                }
                drop(state);
                #[cfg(feature = "bench")]
                if let Some(timing) = timing.as_mut() {
                    timing.coordinator_lock_hold_wall_ns =
                        timing.coordinator_lock_hold_wall_ns.saturating_add(
                            coordinator_lock_hold_wall_started
                                .map(|started| {
                                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
                                })
                                .unwrap_or_default(),
                        );
                    timing.coordinator_lock_hold_cpu_ns =
                        timing.coordinator_lock_hold_cpu_ns.saturating_add(
                            coordinator_lock_hold_cpu_started
                                .map(|started| {
                                    crate::wal::v8::digest::thread_cpu_time_ns()
                                        .saturating_sub(started)
                                })
                                .unwrap_or_default(),
                        );
                }
                Ok(snapshot)
            }
            Err(error) => {
                poison_locked(&self.inner, &mut state);
                Err(error).context("snapshot v8 progress-ring readiness")
            }
        }
    }

    /// Publish that one ticket's physical DATA frames have been materialized.
    pub(crate) fn mark_frames_ready(&self, ticket: u64) -> Result<()> {
        self.mark_frames_ready_inner(ticket, false).map(|_| ())
    }

    #[cfg(feature = "bench")]
    pub(crate) fn mark_frames_ready_with_timeline_sample(
        &self,
        ticket: u64,
        sample_timeline: bool,
    ) -> Result<Option<Instant>> {
        self.mark_frames_ready_inner(ticket, sample_timeline)
    }

    fn mark_frames_ready_inner(
        &self,
        ticket: u64,
        sample_timeline: bool,
    ) -> Result<Option<Instant>> {
        if self.inner.progress_ring.is_some() {
            let changed = self.publish_progress_frames_ready(ticket)?;
            #[cfg(feature = "bench")]
            let frame_ready_at = (changed && sample_timeline).then(Instant::now);
            #[cfg(not(feature = "bench"))]
            let frame_ready_at = {
                let _ = sample_timeline;
                let _ = changed;
                None
            };
            return Ok(frame_ready_at);
        }

        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        let Some(index) = state
            .tickets
            .iter()
            .position(|entry| entry.assignment.segment_ticket == ticket)
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 frame-ready event names an unknown ticket")
        };
        if state.tickets[index].frames_ready {
            return Ok(None);
        }
        state.tickets[index].frames_ready = true;
        #[cfg(feature = "bench")]
        let frame_ready_at = sample_timeline.then(Instant::now);
        #[cfg(not(feature = "bench"))]
        let frame_ready_at = {
            let _ = sample_timeline;
            None
        };
        #[cfg(feature = "bench")]
        refresh_data_completion_order_hol(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        advance_completion_timeline_prefix(&mut state);
        signal_change(&self.inner, &mut state);

        Ok(frame_ready_at)
    }

    /// Record when a producer begins publishing DATA to the ordered worker
    /// submitter for opt-in diagnostics. This is not a physical-write or
    /// readiness timestamp.
    #[cfg(feature = "bench")]
    pub(crate) fn mark_data_write_started(&self, ticket: u64) -> Result<()> {
        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        let Some(index) = state
            .tickets
            .iter()
            .position(|entry| entry.assignment.segment_ticket == ticket)
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-write start event names an unknown ticket")
        };
        let frames_ready = match ticket_frames_ready(&self.inner, &state.tickets[index]) {
            Ok(frames_ready) => frames_ready,
            Err(error) => {
                poison_locked(&self.inner, &mut state);
                return Err(error).context("validate v8 DATA-write frame readiness");
            }
        };
        if !frames_ready
            || state.tickets[index].data_complete
            || state.tickets[index].data_write_started_at.is_some()
        {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-write start event is invalid for its ticket")
        }
        state.tickets[index].data_write_started_at = Some(Instant::now());

        Ok(())
    }

    /// Publish one producer's fixed-size digest independently of frame
    /// materialization and DATA I/O completion.
    pub(crate) fn mark_digest_ready(&self, digest: WalV8DigestReady) -> Result<()> {
        self.mark_digest_ready_inner(digest, false).map(|_| ())
    }

    /// Publish a digest before any DATA for this ticket can reach the worker.
    /// Returns the ordered-link CPU time used in eager mode.
    pub(super) fn mark_digest_ready_before_data(&self, digest: WalV8DigestReady) -> Result<u64> {
        self.mark_digest_ready_inner(digest, true)
    }

    fn mark_digest_ready_inner(&self, digest: WalV8DigestReady, before_data: bool) -> Result<u64> {
        if self.inner.progress_ring.is_some() {
            self.publish_progress_digest(digest, before_data)?;
            return Ok(0);
        }

        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        let Some(index) = state
            .tickets
            .iter()
            .position(|entry| entry.assignment.segment_ticket == digest.segment_ticket)
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 digest-ready event names an unknown ticket")
        };
        let expected_batch_bytes =
            match u64::try_from(state.tickets[index].plan.logical_batch_bytes) {
                Ok(bytes) => bytes,
                Err(error) => {
                    poison_locked(&self.inner, &mut state);
                    return Err(error.into());
                }
            };
        if digest.segment_incarnation != self.inner.segment_incarnation
            || digest.logical_batch_bytes != expected_batch_bytes
        {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 digest-ready event does not match its admitted ticket");
        }
        match state.tickets[index].digest_ready {
            Some(existing) if existing == digest => return Ok(0),
            Some(_) => {
                poison_locked(&self.inner, &mut state);
                anyhow::bail!("conflicting v8 digest-ready event")
            }
            None => {
                state.tickets[index].digest_ready = Some(digest);
                #[cfg(feature = "bench")]
                if self.inner.hol_diagnostics_enabled.load(Ordering::Acquire)
                    && digest.segment_ticket > state.folded_ticket_end
                {
                    state.tickets[index].digest_hol_started_at = Some(Instant::now());
                }
            }
        }
        #[cfg(feature = "bench")]
        let cpu_before = state.ordered_link_hash_cpu_ns;
        let folded_before = state.folded_ticket_end;
        let hol_profile: Option<Arc<crate::mem_table::WriteProfile>> = {
            #[cfg(feature = "bench")]
            {
                self.inner
                    .hol_diagnostics_enabled
                    .load(Ordering::Acquire)
                    .then(|| self.inner.profile.load_full())
                    .flatten()
            }
            #[cfg(not(feature = "bench"))]
            {
                None
            }
        };
        if let Err(error) = fold_ready_digests(&self.inner, &mut state, hol_profile.as_deref()) {
            poison_locked(&self.inner, &mut state);
            return Err(error);
        }
        #[cfg(feature = "bench")]
        refresh_data_completion_order_hol(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        advance_completion_timeline_prefix(&mut state);
        #[cfg(feature = "bench")]
        let ordered_link_hash_cpu_ns = state.ordered_link_hash_cpu_ns.saturating_sub(cpu_before);
        #[cfg(not(feature = "bench"))]
        let ordered_link_hash_cpu_ns = 0;
        signal_change(&self.inner, &mut state);
        // DATA may complete before the producer digest. Only a newly folded
        // digest inside the contiguous ready prefix can unblock durability;
        // hashing in front of unfinished DATA is preparation progress only.
        if state.folded_ticket_end > folded_before
            && state
                .tickets
                .iter()
                .take_while(|ticket| ticket.is_frontier_ready())
                .any(|ticket| ticket.assignment.segment_ticket >= folded_before)
        {
            signal_durability_progress(&self.inner, &mut state);
        }

        Ok(ordered_link_hash_cpu_ns)
    }

    /// Publish that a submitted DATA write has completed. Frame readiness must
    /// already have been published, while digest readiness can arrive later.
    pub(crate) fn mark_data_complete(&self, ticket: u64) -> Result<()> {
        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        let Some(index) = state
            .tickets
            .iter()
            .position(|entry| entry.assignment.segment_ticket == ticket)
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-complete event names an unknown ticket")
        };
        let frames_ready = if let Some(progress_ring) = &self.inner.progress_ring {
            match progress_ring.validate_data_completion(self.inner.segment_incarnation, ticket) {
                Ok(()) => true,
                Err(error) => {
                    poison_locked(&self.inner, &mut state);
                    return Err(error).context("validate v8 DATA completion readiness");
                }
            }
        } else {
            state.tickets[index].frames_ready
        };
        if !frames_ready {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA completed before its frames were published ready");
        }
        if state.tickets[index].data_complete {
            return Ok(());
        }
        state.tickets[index].data_complete = true;
        #[cfg(feature = "bench")]
        if self
            .inner
            .data_completion_timestamps_enabled
            .load(Ordering::Acquire)
        {
            state.tickets[index].data_completed_at = Some(Instant::now());
        }
        #[cfg(feature = "bench")]
        refresh_data_completion_order_hol(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        advance_completion_timeline_prefix(&mut state);
        signal_change(&self.inner, &mut state);
        signal_durability_progress(&self.inner, &mut state);

        Ok(())
    }

    /// Publish a worker group's DATA completion atomically so frontier
    /// formation cannot observe a partial completion from one group.
    pub(crate) fn mark_data_group_complete(&self, tickets: Range<u64>) -> Result<()> {
        #[cfg(feature = "bench")]
        {
            self.mark_data_group_complete_inner(tickets, self.capture_data_completion_time(), None)
        }
        #[cfg(not(feature = "bench"))]
        {
            self.mark_data_group_complete_inner(tickets)
        }
    }

    #[cfg(feature = "bench")]
    pub(crate) fn mark_data_group_complete_with_timeline(
        &self,
        tickets: Range<u64>,
        completion_observed_at: Option<Instant>,
        timeline: Option<crate::wal::parallel_worker::WalWorkerGroupCompletionTimeline>,
    ) -> Result<()> {
        self.mark_data_group_complete_inner(tickets, completion_observed_at, timeline)
    }

    #[inline(always)]
    fn mark_data_group_complete_inner(
        &self,
        tickets: Range<u64>,
        #[cfg(feature = "bench")] completion_observed_at: Option<Instant>,
        #[cfg(feature = "bench")] timeline: Option<
            crate::wal::parallel_worker::WalWorkerGroupCompletionTimeline,
        >,
    ) -> Result<()> {
        #[cfg(feature = "bench")]
        let timeline_sampled = timeline.is_some();
        #[cfg(feature = "bench")]
        let mut handoff_timeline = timeline.clone();
        #[cfg(feature = "bench")]
        let lock_wait_started = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_wait_cpu_started =
            timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
        let mut state = self.inner.state.lock();

        #[cfg(feature = "bench")]
        let lock_acquired_at = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_acquired_cpu = timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
        #[cfg(feature = "bench")]
        let mut v8_prefix_diagnostic_wall_ns = None;
        #[cfg(feature = "bench")]
        let mut v8_prefix_diagnostic_cpu_ns = None;
        #[cfg(feature = "bench")]
        let mut v8_notification_wall_ns = None;
        #[cfg(feature = "bench")]
        let mut v8_notification_cpu_ns = None;

        #[cfg(feature = "bench")]
        if let Some(group) = timeline {
            ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
            ensure!(
                group.ticket_start < group.ticket_end
                    && group.ticket_start >= state.durable_ticket_end
                    && group.ticket_end <= state.next_ticket,
                "v8 completion timeline names an invalid ticket range"
            );
            ensure!(
                group.ticket_start == tickets.start && group.ticket_end == tickets.end,
                "v8 completion timeline ticket range differs from DATA completion"
            );
            ensure!(
                group.request_start <= group.request_id && group.request_id < group.request_end,
                "v8 completion timeline request is outside its group"
            );
            ensure!(
                group.ticket_end > state.completion_timeline_frontier_ready_end,
                "v8 completion timeline was recorded after its prefix became ready"
            );
            ensure!(
                state.completion_timeline_samples.len() < self.inner.max_inflight_tickets,
                "v8 completion timeline exceeded the bounded ticket window"
            );
            ensure!(
                !state
                    .completion_timeline_samples
                    .contains_key(&group.ticket_end),
                "v8 completion timeline duplicates a ticket range"
            );
            let ticket_end = group.ticket_end;
            state.completion_timeline_samples.insert(
                ticket_end,
                PendingCompletionTimeline {
                    group,
                    reaper_recv_to_data_complete_publish_ns: None,
                    data_complete_lock_wait_wall_ns: None,
                    data_complete_lock_wait_cpu_ns: None,
                    data_complete_lock_hold_wall_ns: None,
                    data_complete_lock_hold_cpu_ns: None,
                    data_complete_prefix_end_before: None,
                    data_complete_prefix_end_after: None,
                    hol_blocker_ticket: None,
                    hol_blocker_frames_ready: None,
                    hol_blocker_digest_folded: None,
                    hol_blocker_data_write_elapsed_ns: None,
                    prefix_ready_at: None,
                },
            );
            advance_completion_timeline_prefix(&mut state);
        }

        let applied = match apply_data_group_completion_locked(
            &self.inner,
            &mut state,
            &tickets,
            #[cfg(feature = "bench")]
            timeline_sampled,
            #[cfg(feature = "bench")]
            completion_observed_at,
        ) {
            Ok(applied) => applied,
            Err(error) => {
                poison_locked(&self.inner, &mut state);
                return Err(error);
            }
        };
        if !applied.changed {
            return Ok(());
        }

        #[cfg(feature = "bench")]
        let prefix_diagnostic_started_at = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let prefix_diagnostic_started_cpu =
            timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
        #[cfg(feature = "bench")]
        let data_complete_published_at = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let data_complete_published_monotonic_ns =
            timeline_sampled.then(crate::mem_table::benchmark_monotonic_ns);
        #[cfg(feature = "bench")]
        let data_complete_prefix_end_after =
            timeline_sampled.then(|| data_complete_prefix_end(&state));
        #[cfg(feature = "bench")]
        let blocker_observed_at = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let hol_blocker = timeline_sampled.then(|| {
            state
                .tickets
                .iter()
                .find(|entry| !entry.data_complete)
                .map(|entry| {
                    (
                        entry.assignment.segment_ticket,
                        entry.frames_ready,
                        entry.prefix_digest_after.is_some(),
                        entry.data_write_started_at.map(|started_at| {
                            blocker_observed_at
                                .expect("sampled blocker timestamp exists")
                                .saturating_duration_since(started_at)
                                .as_nanos()
                                .min(u64::MAX as u128) as u64
                        }),
                    )
                })
        });

        #[cfg(feature = "bench")]
        refresh_data_completion_order_hol(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        advance_completion_timeline_prefix(&mut state);
        #[cfg(feature = "bench")]
        if timeline_sampled {
            v8_prefix_diagnostic_wall_ns = Some(
                completion_timeline_duration_ns(prefix_diagnostic_started_at, Some(Instant::now()))
                    .unwrap_or_default(),
            );
            v8_prefix_diagnostic_cpu_ns = Some(
                crate::wal::v8::digest::thread_cpu_time_ns()
                    .saturating_sub(prefix_diagnostic_started_cpu.unwrap_or_default()),
            );
        }
        #[cfg(feature = "bench")]
        let notification_started_at = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let notification_started_cpu =
            timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);
        signal_change(&self.inner, &mut state);
        signal_durability_progress(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        if timeline_sampled {
            v8_notification_wall_ns = Some(
                completion_timeline_duration_ns(notification_started_at, Some(Instant::now()))
                    .unwrap_or_default(),
            );
            v8_notification_cpu_ns = Some(
                crate::wal::v8::digest::thread_cpu_time_ns()
                    .saturating_sub(notification_started_cpu.unwrap_or_default()),
            );
        }

        #[cfg(feature = "bench")]
        if timeline_sampled {
            let lock_hold_snapshot_at = Instant::now();
            let lock_hold_snapshot_cpu = crate::wal::v8::digest::thread_cpu_time_ns();
            let lock_wait_wall_ns =
                completion_timeline_duration_ns(lock_wait_started, lock_acquired_at)
                    .unwrap_or_default();
            let lock_wait_cpu_ns = lock_acquired_cpu
                .zip(lock_wait_cpu_started)
                .map(|(end, start)| end.saturating_sub(start))
                .unwrap_or_default();
            let ticket_end = tickets.end;
            let pending = state
                .completion_timeline_samples
                .get_mut(&ticket_end)
                .context("sampled DATA completion timeline disappeared")?;
            let reaper_recv_at = pending.group.reaper_recv_at;
            pending.data_complete_lock_wait_wall_ns = Some(lock_wait_wall_ns);
            pending.data_complete_lock_wait_cpu_ns = Some(lock_wait_cpu_ns);
            pending.data_complete_lock_hold_wall_ns = Some(
                completion_timeline_duration_ns(lock_acquired_at, Some(lock_hold_snapshot_at))
                    .unwrap_or_default(),
            );
            pending.data_complete_lock_hold_cpu_ns =
                Some(lock_hold_snapshot_cpu.saturating_sub(lock_acquired_cpu.unwrap_or_default()));
            pending.data_complete_prefix_end_before = applied.prefix_end_before;
            pending.data_complete_prefix_end_after = data_complete_prefix_end_after;
            if let Some(published_at) = data_complete_published_at {
                pending.reaper_recv_to_data_complete_publish_ns = Some(
                    completion_timeline_duration_ns(reaper_recv_at, Some(published_at))
                        .unwrap_or_default(),
                );
            }
            if let Some((ticket, frames_ready, digest_folded, write_elapsed_ns)) =
                hol_blocker.flatten()
            {
                pending.hol_blocker_ticket = Some(ticket);
                pending.hol_blocker_frames_ready = Some(frames_ready);
                pending.hol_blocker_digest_folded = Some(digest_folded);
                pending.hol_blocker_data_write_elapsed_ns = write_elapsed_ns;
            }
        }

        #[cfg(feature = "bench")]
        let lock_hold_finished_at = timeline_sampled.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_hold_finished_cpu =
            timeline_sampled.then(crate::wal::v8::digest::thread_cpu_time_ns);

        #[cfg(feature = "bench")]
        if let Some(timeline) = handoff_timeline.as_mut() {
            timeline.data_complete_published_at = data_complete_published_at;
            timeline.data_complete_published_monotonic_ns = data_complete_published_monotonic_ns;
            timeline.data_complete_prefix_end_before = applied.prefix_end_before;
            timeline.data_complete_prefix_end_after = data_complete_prefix_end_after;
            timeline.hol_blocker_ticket = hol_blocker.flatten().map(|(ticket, _, _, _)| ticket);
        }

        drop(state);
        #[cfg(feature = "bench")]
        if let (Some(timeline), true) = (handoff_timeline.as_mut(), timeline_sampled) {
            timeline.publication_phases = Some(crate::mem_table::WalCompletionPublicationPhases {
                v8_coordinator_state_lock_wait_wall_ns: Some(
                    completion_timeline_duration_ns(lock_wait_started, lock_acquired_at)
                        .unwrap_or_default(),
                ),
                v8_coordinator_state_lock_wait_cpu_ns: Some(
                    lock_acquired_cpu
                        .unwrap_or_default()
                        .saturating_sub(lock_wait_cpu_started.unwrap_or_default()),
                ),
                v8_ticket_lookup_wall_ns: applied.ticket_lookup_wall_ns,
                v8_ticket_lookup_cpu_ns: applied.ticket_lookup_cpu_ns,
                v8_ticket_lookup_entries_scanned: applied.ticket_lookup_entries_scanned,
                v8_ticket_validation_wall_ns: applied.ticket_validation_wall_ns,
                v8_ticket_validation_cpu_ns: applied.ticket_validation_cpu_ns,
                v8_data_complete_mark_wall_ns: applied.mark_wall_ns,
                v8_data_complete_mark_cpu_ns: applied.mark_cpu_ns,
                v8_prefix_diagnostic_wall_ns,
                v8_prefix_diagnostic_cpu_ns,
                v8_notification_wall_ns,
                v8_notification_cpu_ns,
                v8_notification_calls: Some(2),
                v8_coordinator_state_lock_hold_wall_ns: Some(
                    completion_timeline_duration_ns(lock_acquired_at, lock_hold_finished_at)
                        .unwrap_or_default(),
                ),
                v8_coordinator_state_lock_hold_cpu_ns: Some(
                    lock_hold_finished_cpu
                        .unwrap_or_default()
                        .saturating_sub(lock_acquired_cpu.unwrap_or_default()),
                ),
                ..Default::default()
            });
        }

        #[cfg(feature = "bench")]
        if let (Some(profile), Some(timeline)) = (self.inner.profile.load_full(), handoff_timeline)
            && let Some(observation) =
                crate::wal::parallel_worker::WalSyncProgress::handoff_observation(&timeline)
        {
            profile.record_wal_completion_handoff_timeline(observation);
        }

        Ok(())
    }

    /// Reserve a physical tail slot for the next FRONTIER. Admission after this
    /// call allocates DATA after the marker, even if the selected prefix's
    /// earlier DATA is still awaiting a sync.
    pub(crate) fn reserve_frontier(&self, ticket_end: u64) -> Result<WalV8FrontierReservation> {
        self.reserve_frontier_with(ticket_end, |_| Ok(()))
    }

    /// Reserve a frontier and publish its physical offset to the DATA
    /// submitter before another admission can allocate beyond that marker.
    pub(crate) fn reserve_frontier_with<F>(
        &self,
        ticket_end: u64,
        publish: F,
    ) -> Result<WalV8FrontierReservation>
    where
        F: FnOnce(WalV8FrontierReservation) -> Result<()>,
    {
        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        ensure!(
            state.pending_frontier.is_none(),
            "v8 FRONTIER cycle is already in progress"
        );
        ensure!(
            ticket_end > state.durable_ticket_end,
            "v8 FRONTIER does not advance the durable ticket boundary"
        );
        ensure!(
            ticket_end >= state.last_frontier_ticket_end,
            "v8 FRONTIER does not cover DATA allocated before its predecessor"
        );
        let ticket_count = usize::try_from(ticket_end - state.durable_ticket_end)
            .context("v8 FRONTIER ticket count exceeds usize")?;
        let ready = if let Some(cursor) = state.ready_cursor {
            let within_cursor = ticket_end <= cursor.ticket_end;
            if within_cursor
                && !state
                    .tickets
                    .get(ticket_count - 1)
                    .is_some_and(|entry| entry.assignment.segment_ticket == ticket_end - 1)
            {
                poison_locked(&self.inner, &mut state);
                anyhow::bail!("v8 FRONTIER ticket does not match the validated prefix");
            }
            within_cursor
        } else {
            let mut ready_count = 0;
            for entry in &state.tickets {
                match ticket_is_frontier_ready(&self.inner, entry) {
                    Ok(true) => ready_count += 1,
                    Ok(false) => break,
                    Err(error) => {
                        poison_locked(&self.inner, &mut state);
                        return Err(error).context("validate v8 FRONTIER readiness");
                    }
                }
            }
            ticket_count <= ready_count
        };
        ensure!(
            ready,
            "v8 FRONTIER extends past the contiguous completed ticket prefix"
        );
        let Some(prefix_digest) = state
            .tickets
            .get(ticket_count - 1)
            .and_then(|ticket| ticket.prefix_digest_after)
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 FRONTIER prefix digest was not folded in ticket order");
        };
        let frame_offset = state.next_data_offset;
        let frontier_end = frame_offset
            .checked_add(FRAME_LEN_U64)
            .context("v8 FRONTIER allocation end overflows")?;
        ensure!(
            frontier_end <= self.inner.max_file_size,
            "v8 FRONTIER exceeds the configured segment limit"
        );
        let reservation = WalV8FrontierReservation {
            ticket_end,
            frame_offset,
            preceding_ticket_end: state.next_ticket,
            prefix_digest,
        };
        state.next_data_offset = frontier_end;
        state.pending_frontier = Some(reservation);
        if let Err(error) = publish(reservation) {
            poison_locked(&self.inner, &mut state);
            return Err(error).context("publish v8 FRONTIER offset to DATA submitter");
        }
        signal_change(&self.inner, &mut state);

        Ok(reservation)
    }

    /// Publish a FRONTIER only after its write and the covering WAL sync have
    /// succeeded. The caller must disarm the covered producer guards next.
    pub(crate) fn publish_durable_frontier(
        &self,
        reservation: WalV8FrontierReservation,
    ) -> Result<()> {
        self.publish_durable_frontier_with_metrics(reservation, false)
            .map(|_| ())
    }

    #[cfg(feature = "bench")]
    pub(crate) fn publish_durable_frontier_sampled(
        &self,
        reservation: WalV8FrontierReservation,
    ) -> Result<WalV8DurabilityPublishMetrics> {
        self.publish_durable_frontier_with_metrics(reservation, true)
    }

    fn publish_durable_frontier_with_metrics(
        &self,
        reservation: WalV8FrontierReservation,
        sample: bool,
    ) -> Result<WalV8DurabilityPublishMetrics> {
        #[cfg(feature = "bench")]
        let publish_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let publish_cpu_started = sample.then(super::digest::thread_cpu_time_ns);
        #[cfg(feature = "bench")]
        let lock_wait_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_wait_cpu_started = sample.then(super::digest::thread_cpu_time_ns);
        let mut state = self.inner.state.lock();
        #[cfg(feature = "bench")]
        let coordinator_lock_wait_ns = lock_wait_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let coordinator_lock_wait_cpu_ns = lock_wait_cpu_started
            .map(|started| super::digest::thread_cpu_time_ns().saturating_sub(started))
            .unwrap_or_default();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        if state.pending_frontier != Some(reservation) {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 durable FRONTIER differs from its active reservation");
        }
        let Some(ticket_span) = reservation.ticket_end.checked_sub(state.durable_ticket_end) else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 durable FRONTIER moves the ticket boundary backwards");
        };
        let ticket_count = match usize::try_from(ticket_span) {
            Ok(count) => count,
            Err(error) => {
                poison_locked(&self.inner, &mut state);
                return Err(error.into());
            }
        };
        let Some(frontier_end) = reservation.frame_offset.checked_add(FRAME_LEN_U64) else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 durable FRONTIER end overflows");
        };
        let valid_ticket_prefix = (|| -> Result<bool> {
            if ticket_count == 0 || ticket_count > state.tickets.len() {
                return Ok(false);
            }
            if let Some(cursor) = state.ready_cursor {
                // Reservation identity matched above. The cursor recorded
                // this exact ticket range under the same Coordinator lock.
                return Ok(reservation.ticket_end <= cursor.ticket_end
                    && state.tickets[ticket_count - 1].assignment.segment_ticket
                        == reservation.ticket_end - 1);
            }
            for (index, entry) in state.tickets.iter().take(ticket_count).enumerate() {
                let Some(expected_ticket) = u64::try_from(index)
                    .ok()
                    .and_then(|index| state.durable_ticket_end.checked_add(index))
                else {
                    return Ok(false);
                };
                if entry.assignment.segment_ticket != expected_ticket
                    || !ticket_is_frontier_ready(&self.inner, entry)?
                {
                    return Ok(false);
                }
            }
            Ok(true)
        })();
        let valid_ticket_prefix = match valid_ticket_prefix {
            Ok(valid) => valid,
            Err(error) => {
                poison_locked(&self.inner, &mut state);
                return Err(error).context("validate v8 durable FRONTIER ticket readiness");
            }
        };
        let Some(prefix_reserved_bytes) = state
            .tickets
            .iter()
            .take(ticket_count)
            .try_fold(0_u64, |total, entry| {
                total.checked_add(entry.reserved_bytes)
            })
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 durable ticket reservation sum overflows");
        };
        if !valid_ticket_prefix
            || state.tickets[ticket_count - 1].prefix_digest_after
                != Some(reservation.prefix_digest)
            || reservation.preceding_ticket_end < reservation.ticket_end
            || reservation.preceding_ticket_end > state.next_ticket
            || frontier_end < state.last_frontier_end
            || frontier_end > state.next_data_offset
            || frontier_end > self.inner.max_file_size
            || prefix_reserved_bytes > state.reserved_bytes
        {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 durable FRONTIER covers an invalid ticket prefix");
        }
        #[cfg(feature = "bench")]
        let remaining_ready_data_bytes = if let Some(cursor) = state.ready_cursor {
            let remaining = if reservation.ticket_end == cursor.ticket_end {
                0
            } else {
                // Physical Parallel advances normally retire the entire ready
                // prefix. Preserve accounting if a caller reserves a subset.
                let covered_data_bytes = state
                    .tickets
                    .iter()
                    .take(ticket_count)
                    .map(|ticket| ticket.plan.frame_buffer_bytes as u64)
                    .sum();
                let Some(remaining) = cursor.data_bytes.checked_sub(covered_data_bytes) else {
                    poison_locked(&self.inner, &mut state);
                    anyhow::bail!("v8 ready prefix DATA byte count underflows");
                };
                remaining
            };
            Some(remaining)
        } else {
            None
        };
        if let Some(progress_ring) = &self.inner.progress_ring
            && let Err(error) =
                progress_ring.retire_prefix(state.durable_ticket_end, reservation.ticket_end)
        {
            poison_locked(&self.inner, &mut state);
            return Err(error).context("retire v8 progress ring durable prefix");
        }
        for _ in 0..ticket_count {
            state.tickets.pop_front();
        }
        #[cfg(feature = "bench")]
        if let (Some(cursor), Some(data_bytes)) =
            (&mut state.ready_cursor, remaining_ready_data_bytes)
        {
            cursor.data_bytes = data_bytes;
        }
        state.reserved_bytes -= prefix_reserved_bytes;
        #[cfg(feature = "bench")]
        let durable_ticket_start = state.durable_ticket_end;
        state.durable_ticket_end = reservation.ticket_end;
        state.durable_prefix_digest = reservation.prefix_digest;
        state.last_frontier_end = frontier_end;
        state.last_frontier_ticket_end = reservation.preceding_ticket_end;
        state.pending_frontier = None;
        #[cfg(feature = "bench")]
        let durable_published_at = Instant::now();
        #[cfg(feature = "bench")]
        if let Some(profile) = self.inner.profile.load_full() {
            profile.record_commit_order_frontier_published(
                durable_ticket_start,
                reservation.ticket_end,
            );
        }
        #[cfg(feature = "bench")]
        if let Some(profile) = self.inner.profile.load_full() {
            let every = profile.wal_v8_durability_sample_every();
            if every > 0 {
                let first_sample_end = state
                    .durable_ticket_end
                    .checked_sub(ticket_span)
                    .and_then(|previous_end| previous_end.checked_div(every))
                    .and_then(|sample| sample.checked_add(1))
                    .and_then(|sample| sample.checked_mul(every));
                if let Some(mut sample_end) = first_sample_end {
                    while sample_end <= reservation.ticket_end {
                        state
                            .durable_publish_sample_timestamps
                            .insert(sample_end, durable_published_at);
                        sample_end = match sample_end.checked_add(every) {
                            Some(next) => next,
                            None => break,
                        };
                    }
                }
                while state.durable_publish_sample_timestamps.len() > 4096 {
                    let Some(oldest) = state
                        .durable_publish_sample_timestamps
                        .first_key_value()
                        .map(|(ticket_end, _)| *ticket_end)
                    else {
                        break;
                    };
                    state.durable_publish_sample_timestamps.remove(&oldest);
                }
            }
        }
        #[cfg(feature = "bench")]
        let covered_timeline_samples = {
            let covered_ends = state
                .completion_timeline_samples
                .range(..=reservation.ticket_end)
                .map(|(ticket_end, _)| *ticket_end)
                .collect::<Vec<_>>();
            covered_ends
                .into_iter()
                .filter_map(|ticket_end| state.completion_timeline_samples.remove(&ticket_end))
                .collect::<Vec<_>>()
        };
        signal_change(&self.inner, &mut state);
        signal_durability_progress(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        if state.durability_driver_waiters > 0 {
            state.last_durability_notify_at = Some(Instant::now());
            state.last_durability_notify_waiters = state.durability_driver_waiters;
        }
        #[cfg(feature = "bench")]
        let followers_waiting = state.durability_driver_waiters;
        #[cfg(feature = "bench")]
        let notify_started = sample.then(Instant::now);
        self.inner.durable_published.notify_all();
        #[cfg(feature = "bench")]
        let notify_all_wall_ns = notify_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        drop(state);

        #[cfg(feature = "bench")]
        if let Some(profile) = self.inner.profile.load_full() {
            for sample in covered_timeline_samples {
                let Some(prefix_ready_at) = sample.prefix_ready_at else {
                    continue;
                };
                let group = sample.group;
                let Some(submit_to_cqe_observed_ns) =
                    completion_timeline_duration_ns(group.sqe_submitted_at, group.cqe_observed_at)
                else {
                    continue;
                };
                let Some(cqe_observed_to_group_finished_ns) =
                    completion_timeline_duration_ns(group.cqe_observed_at, group.group_finished_at)
                else {
                    continue;
                };
                let Some(group_finished_to_result_send_start_ns) = completion_timeline_duration_ns(
                    group.group_finished_at,
                    group.result_send_started_at,
                ) else {
                    continue;
                };
                let Some(result_send_to_reaper_ns) = completion_timeline_duration_ns(
                    group.result_send_started_at,
                    group.reaper_recv_at,
                ) else {
                    continue;
                };
                let Some(reaper_to_prefix_ready_ns) =
                    completion_timeline_duration_ns(group.reaper_recv_at, Some(prefix_ready_at))
                else {
                    continue;
                };
                let Some(prefix_ready_to_durable_ns) = completion_timeline_duration_ns(
                    Some(prefix_ready_at),
                    Some(durable_published_at),
                ) else {
                    continue;
                };
                let observation = crate::mem_table::WalV8CompletionTimelineObservation {
                    group_id: group.group_id,
                    ticket_start: group.ticket_start,
                    ticket_end: group.ticket_end,
                    worker_tid: group.worker_tid,
                    request_id: group.request_id,
                    request_start: group.request_start,
                    request_end: group.request_end,
                    submit_to_cqe_observed_ns,
                    cqe_observed_monotonic_ns: group.cqe_observed_monotonic_ns,
                    group_finished_monotonic_ns: group.group_finished_monotonic_ns,
                    cqe_observed_to_group_finished_ns,
                    group_finished_to_result_send_start_ns,
                    result_send_start_to_reaper_recv_ns: result_send_to_reaper_ns,
                    reaper_recv_to_data_complete_publish_ns: sample
                        .reaper_recv_to_data_complete_publish_ns
                        .unwrap_or_default(),
                    data_complete_lock_wait_wall_ns: sample
                        .data_complete_lock_wait_wall_ns
                        .unwrap_or_default(),
                    data_complete_lock_wait_cpu_ns: sample
                        .data_complete_lock_wait_cpu_ns
                        .unwrap_or_default(),
                    data_complete_lock_hold_wall_ns: sample
                        .data_complete_lock_hold_wall_ns
                        .unwrap_or_default(),
                    data_complete_lock_hold_cpu_ns: sample
                        .data_complete_lock_hold_cpu_ns
                        .unwrap_or_default(),
                    data_complete_prefix_end_before: sample
                        .data_complete_prefix_end_before
                        .unwrap_or_default(),
                    data_complete_prefix_end_after: sample
                        .data_complete_prefix_end_after
                        .unwrap_or_default(),
                    hol_blocker_ticket: sample.hol_blocker_ticket,
                    hol_blocker_frames_ready: sample.hol_blocker_frames_ready,
                    hol_blocker_digest_folded: sample.hol_blocker_digest_folded,
                    hol_blocker_data_write_elapsed_ns: sample.hol_blocker_data_write_elapsed_ns,
                    reaper_recv_to_prefix_ready_ns: reaper_to_prefix_ready_ns,
                    prefix_ready_to_durable_ns,
                };
                profile.record_wal_v8_completion_timeline(observation);
            }
        }

        #[cfg(feature = "bench")]
        {
            let metrics = WalV8DurabilityPublishMetrics {
                coordinator_lock_wait_ns,
                coordinator_lock_wait_cpu_ns,
                publish_wall_ns: publish_started
                    .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                    .unwrap_or_default(),
                publish_cpu_ns: publish_cpu_started
                    .map(|started| super::digest::thread_cpu_time_ns().saturating_sub(started))
                    .unwrap_or_default(),
                notify_all_wall_ns,
                followers_waiting,
            };

            Ok(metrics)
        }
        #[cfg(not(feature = "bench"))]
        {
            let _ = sample;
            Ok(WalV8DurabilityPublishMetrics::default())
        }
    }

    /// Stop new admissions and wake packer/durability waiters.
    pub(crate) fn close(&self) {
        let mut state = self.inner.state.lock();

        if !state.closed {
            state.closed = true;
            signal_change(&self.inner, &mut state);
            signal_durability_progress(&self.inner, &mut state);
            #[cfg(feature = "bench")]
            record_durable_notification_for_waiters(&mut state);
            self.inner.durable_published.notify_all();
        }
        drop(state);
        self.signal_progress_token();
    }

    /// Poison the admission window after an uncertain write, marker, or sync.
    pub(crate) fn poison(&self) {
        let mut state = self.inner.state.lock();
        poison_locked(&self.inner, &mut state);
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.inner.state.lock().poisoned
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.state.lock().closed
    }

    #[cfg(feature = "bench")]
    pub(crate) fn ordered_link_hash_cpu_ns(&self) -> u64 {
        self.inner.state.lock().ordered_link_hash_cpu_ns
    }

    pub(crate) fn durable_ticket_end(&self) -> u64 {
        self.inner.state.lock().durable_ticket_end
    }

    #[cfg(feature = "bench")]
    pub(crate) fn take_durable_publish_sample_timestamp(&self, ticket: u64) -> Option<Instant> {
        let ticket_end = ticket.checked_add(1)?;

        self.inner
            .state
            .lock()
            .durable_publish_sample_timestamps
            .remove(&ticket_end)
    }

    /// Return the published durable ticket/image ends and admission end from
    /// one state snapshot. Unfinished DATA and reserved FRONTIERs do not
    /// advance the first two fields.
    pub(crate) fn published_cursor(&self) -> (u64, u64, u64) {
        let state = self.inner.state.lock();

        (
            state.durable_ticket_end,
            state.last_frontier_end,
            state.next_ticket,
        )
    }

    /// Return the verified digest at the current durable ticket boundary.
    pub(crate) fn durable_prefix_digest(&self) -> WalV8Digest {
        self.inner.state.lock().durable_prefix_digest
    }

    /// Return the folded digest at a ticket end still represented in the live
    /// admission window, or the current durable digest. Older durable prefixes
    /// are intentionally not retained here.
    pub(crate) fn prefix_digest_at(&self, ticket_end: u64) -> Option<WalV8Digest> {
        let state = self.inner.state.lock();
        if state.poisoned || ticket_end < state.durable_ticket_end {
            return None;
        }
        if ticket_end == state.durable_ticket_end {
            return Some(state.durable_prefix_digest);
        }
        if ticket_end > state.folded_ticket_end {
            return None;
        }
        let previous_ticket = ticket_end.checked_sub(1)?;
        let index = usize::try_from(previous_ticket.checked_sub(state.durable_ticket_end)?).ok()?;

        state.tickets.get(index)?.prefix_digest_after
    }

    pub(crate) fn next_ticket(&self) -> u64 {
        self.inner.state.lock().next_ticket
    }

    /// Run a diagnostics-state update while excluding ticket admission. This
    /// keeps the benchmark-only enablement check atomic with publication of a
    /// new ticket.
    #[cfg(feature = "bench")]
    pub(crate) fn with_next_ticket<T>(&self, update: impl FnOnce(u64) -> T) -> T {
        let state = self.inner.state.lock();

        update(state.next_ticket)
    }

    pub(crate) fn last_frontier_end(&self) -> u64 {
        self.inner.state.lock().last_frontier_end
    }

    /// Keep all admitted DATA and reserved FRONTIER ranges stable while
    /// reclaiming a speculative physical suffix. The callback must not enter
    /// the coordinator or acquire the submission lock.
    pub(crate) fn with_allocation_tail<T>(
        &self,
        update: impl FnOnce(u64) -> Result<T>,
    ) -> Result<T> {
        let state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        let result = update(state.next_data_offset);
        drop(state);

        result
    }

    pub(crate) fn change_generation(&self) -> u64 {
        self.inner.state.lock().change_generation
    }

    /// Notification generation for DATA/prefix readiness and durable
    /// publication. This is independent of the on-disk FRONTIER generation.
    pub(crate) fn durability_generation(&self) -> u64 {
        self.inner.state.lock().durability_generation
    }

    /// Elect one caller to form FRONTIERs. Other callers wait for published
    /// durability or driver handoff and can return at their own ticket end
    /// without acquiring the driver's physical FRONTIER mutex.
    pub(crate) fn acquire_durability_driver(
        &self,
        ticket_end: u64,
    ) -> Result<Option<WalV8DurabilityDriver>> {
        #[cfg(feature = "bench")]
        let profile = self.inner.profile.load();
        #[cfg(feature = "bench")]
        let diagnostics_enabled = profile
            .as_ref()
            .is_some_and(|profile| profile.wal_v8_durability_sample_every() > 0);
        #[cfg(feature = "bench")]
        let lock_wait_started = diagnostics_enabled.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_wait_cpu_started = diagnostics_enabled.then(super::digest::thread_cpu_time_ns);
        let mut state = self.inner.state.lock();
        #[cfg(feature = "bench")]
        let coordinator_lock_wait_ns = lock_wait_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let coordinator_lock_wait_cpu_ns = lock_wait_cpu_started
            .map(|started| super::digest::thread_cpu_time_ns().saturating_sub(started))
            .unwrap_or_default();
        ensure!(
            ticket_end <= state.next_ticket,
            "v8 durability driver names an unassigned ticket"
        );

        #[cfg(feature = "bench")]
        let mut follower_sampling_decided = false;
        #[cfg(feature = "bench")]
        let mut follower_sample_id = None;
        #[cfg(feature = "bench")]
        let mut follower_wait_started = None;
        #[cfg(feature = "bench")]
        let mut follower_wait_cpu_started = None;
        #[cfg(feature = "bench")]
        let mut follower_condvar_wait_count = 0_u64;
        #[cfg(feature = "bench")]
        let mut notify_to_resume_ns = None;
        #[cfg(feature = "bench")]
        let mut followers_woken_by_last_notify = 0_u64;

        loop {
            ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
            if state.durable_ticket_end >= ticket_end {
                #[cfg(feature = "bench")]
                let observation = make_follower_wait_observation(WalV8FollowerWaitSample {
                    sample_id: follower_sample_id,
                    requested_ticket_end: ticket_end,
                    durable_ticket_end_on_resume: state.durable_ticket_end,
                    coordinator_lock_wait_ns,
                    coordinator_lock_wait_cpu_ns,
                    wait_started: follower_wait_started,
                    wait_cpu_started: follower_wait_cpu_started,
                    condvar_wait_count: follower_condvar_wait_count,
                    notify_to_resume_ns,
                    followers_woken_by_last_notify,
                });
                drop(state);
                #[cfg(feature = "bench")]
                if let (Some(profile), Some(observation)) = (profile.as_ref(), observation) {
                    profile.record_wal_v8_follower_wait(observation);
                }

                return Ok(None);
            }
            if !state.durability_driver_active {
                let completion_batch = state.completion_batch.take();
                if self.progress_cursor_enabled() && completion_batch.is_none() {
                    poison_locked(&self.inner, &mut state);
                    anyhow::bail!("v8 durability driver completion buffer is missing");
                }
                state.durability_driver_active = true;
                let driver = WalV8DurabilityDriver {
                    coordinator: self.clone(),
                    completed: false,
                    completion_batch,
                };
                #[cfg(feature = "bench")]
                let observation = make_follower_wait_observation(WalV8FollowerWaitSample {
                    sample_id: follower_sample_id,
                    requested_ticket_end: ticket_end,
                    durable_ticket_end_on_resume: state.durable_ticket_end,
                    coordinator_lock_wait_ns,
                    coordinator_lock_wait_cpu_ns,
                    wait_started: follower_wait_started,
                    wait_cpu_started: follower_wait_cpu_started,
                    condvar_wait_count: follower_condvar_wait_count,
                    notify_to_resume_ns,
                    followers_woken_by_last_notify,
                });
                drop(state);
                #[cfg(feature = "bench")]
                if let (Some(profile), Some(observation)) = (profile.as_ref(), observation) {
                    profile.record_wal_v8_follower_wait(observation);
                }

                return Ok(Some(driver));
            }
            #[cfg(feature = "bench")]
            if diagnostics_enabled {
                state.durability_driver_waiters += 1;
                if !follower_sampling_decided {
                    follower_sampling_decided = true;
                    follower_sample_id = profile
                        .as_ref()
                        .and_then(|profile| profile.next_wal_v8_follower_sample());
                    if follower_sample_id.is_some() {
                        follower_wait_started = Some(Instant::now());
                        follower_wait_cpu_started = Some(super::digest::thread_cpu_time_ns());
                    }
                }
            }
            #[cfg(test)]
            {
                state.durable_followers += 1;
            }
            // Close wakes followers as well, but admitted tickets may still
            // drain through the active driver. Poison or driver failure is
            // terminal; a normal handoff permits the next caller to drive.
            #[cfg(feature = "bench")]
            let observed_durability_generation = state.durability_generation;
            self.inner.durable_published.wait(&mut state);
            #[cfg(feature = "bench")]
            if diagnostics_enabled {
                state.durability_driver_waiters -= 1;
                if follower_sample_id.is_some() {
                    follower_condvar_wait_count = follower_condvar_wait_count.saturating_add(1);
                    if state.durability_generation != observed_durability_generation
                        || !state.durability_driver_active
                    {
                        notify_to_resume_ns = state
                            .last_durability_notify_at
                            .map(|at| at.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                        followers_woken_by_last_notify = state.last_durability_notify_waiters;
                    }
                }
            }
            #[cfg(test)]
            {
                state.durable_followers -= 1;
            }
        }
    }

    /// Wait for durability progress without subscribing to producer
    /// preparation notifications. The locked predicate prevents lost wakes;
    /// one deadline bounds the wait even after a spurious notification.
    pub(crate) fn wait_for_durability_change(
        &self,
        observed_generation: u64,
        timeout: Duration,
    ) -> WalV8CoordinatorWake {
        let deadline = Instant::now() + timeout;
        let mut state = self.inner.state.lock();
        while state.durability_generation == observed_generation && !state.closed && !state.poisoned
        {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            #[cfg(test)]
            {
                state.durability_waiters += 1;
            }
            self.inner
                .durability_changed
                .wait_for(&mut state, remaining);
            #[cfg(test)]
            {
                state.durability_waiters -= 1;
            }
        }

        WalV8CoordinatorWake {
            change_generation: state.durability_generation,
            closed: state.closed,
            poisoned: state.poisoned,
        }
    }

    #[cfg(test)]
    fn ticket_admission_waiter_count(&self) -> usize {
        self.inner.state.lock().ticket_admission_waiters
    }

    /// Wait until admission, readiness, close, or poison changes, with a
    /// deadline so callers can periodically recheck group policy.
    pub(crate) fn wait_for_change(
        &self,
        observed_generation: u64,
        timeout: Duration,
    ) -> WalV8CoordinatorWake {
        let mut state = self.inner.state.lock();
        if state.change_generation == observed_generation && !state.closed && !state.poisoned {
            self.inner.changed.wait_for(&mut state, timeout);
        }

        WalV8CoordinatorWake {
            change_generation: state.change_generation,
            closed: state.closed,
            poisoned: state.poisoned,
        }
    }

    /// Charge preparation-index memory before canonicalization allocates it.
    /// The charge shares the admission byte window so concurrent producers
    /// cannot each allocate an unbounded pre-admission index.
    pub(crate) fn reserve_preparation(&self, bytes: u64) -> Result<WalV8PreparationReservation> {
        ensure!(
            bytes <= self.inner.max_inflight_bytes,
            "v8 preparation index exceeds the configured byte window"
        );
        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        ensure!(!state.closed, "v8 Parallel coordinator is closed");
        let new_preparation_bytes = state
            .reserved_preparation_bytes
            .checked_add(bytes)
            .context("v8 preparation reservation accounting overflows")?;
        let total_reserved_bytes = state
            .reserved_bytes
            .checked_add(new_preparation_bytes)
            .context("v8 combined admission reservation accounting overflows")?;
        ensure!(
            total_reserved_bytes <= self.inner.max_inflight_bytes,
            "v8 preparation reservation window is full"
        );
        state.reserved_preparation_bytes = new_preparation_bytes;

        Ok(WalV8PreparationReservation {
            coordinator: self.clone(),
            bytes,
        })
    }
}

/// Driver ownership is independent of the physical FRONTIER mutex. On an
/// error or unwind, poison before allowing another caller to take over.
pub(crate) struct WalV8DurabilityDriver {
    coordinator: WalV8ParallelCoordinator,
    completed: bool,
    pub(super) completion_batch: Option<Vec<WalV8CompletedGroupEvent>>,
}

impl WalV8DurabilityDriver {
    pub(crate) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for WalV8DurabilityDriver {
    fn drop(&mut self) {
        let mut state = self.coordinator.inner.state.lock();
        if !self.completed {
            poison_locked(&self.coordinator.inner, &mut state);
        }
        if let Some(mut batch) = self.completion_batch.take() {
            batch.clear();
            state.completion_batch = Some(batch);
        }
        state.durability_driver_active = false;
        #[cfg(feature = "bench")]
        record_durable_notification_for_waiters(&mut state);
        self.coordinator.inner.durable_published.notify_all();
    }
}

impl WalV8AdmissionPublisher for WalV8ParallelCoordinator {
    type Guard = WalV8ParallelAdmissionGuard;

    fn admit_and_publish(
        &self,
        plan: WalV8AdmissionPlan,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)> {
        self.admit_and_publish_with(plan, |_| Ok(()))
    }

    fn admit_and_publish_with<F>(
        &self,
        plan: WalV8AdmissionPlan,
        register: F,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)>
    where
        F: FnOnce(WalV8BatchAssignment) -> Result<()>,
    {
        validate_plan(plan)?;
        let data_bytes = u64::try_from(plan.frame_buffer_bytes)?;
        let reserved_bytes = u64::try_from(plan.logical_batch_bytes)?
            .checked_add(data_bytes)
            .context("v8 admitted buffer reservation overflows")?;
        ensure!(
            reserved_bytes <= self.inner.max_inflight_bytes,
            "v8 Parallel batch exceeds the configured byte window"
        );

        let mut state = self.inner.state.lock();
        while state.tickets.len() >= self.inner.max_inflight_tickets {
            ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
            ensure!(!state.closed, "v8 Parallel coordinator is closed");
            // Ticket capacity is a backpressure boundary, not a failed write.
            // The durability worker frees slots and signals this condition
            // after publishing a successful FRONTIER.
            #[cfg(test)]
            {
                state.ticket_admission_waiters += 1;
            }
            self.inner.changed.wait(&mut state);
            #[cfg(test)]
            {
                state.ticket_admission_waiters -= 1;
            }
        }
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        ensure!(!state.closed, "v8 Parallel coordinator is closed");
        let new_reserved_bytes = state
            .reserved_bytes
            .checked_add(reserved_bytes)
            .context("v8 Parallel reservation accounting overflows")?;
        let total_reserved_bytes = new_reserved_bytes
            .checked_add(state.reserved_preparation_bytes)
            .context("v8 combined admission reservation accounting overflows")?;
        ensure!(
            total_reserved_bytes <= self.inner.max_inflight_bytes,
            "v8 Parallel buffer reservation is full"
        );
        ensure!(
            plan.commit_ts > state.last_commit_ts,
            "v8 Parallel commit timestamp does not advance"
        );
        let recorded_at = state
            .last_recorded_at
            .map_or(plan.recorded_at, |previous| previous.max(plan.recorded_at));
        let ticket = state.next_ticket;
        let next_ticket = ticket
            .checked_add(1)
            .context("v8 Parallel ticket cursor overflows")?;
        let first_data_offset = state.next_data_offset;
        let data_end = first_data_offset
            .checked_add(data_bytes)
            .context("v8 Parallel DATA allocation end overflows")?;
        // Each admitted ticket retains space for its own FRONTIER until it is
        // durable: earlier tickets may finish before later DATA is ready. A
        // pending marker occupies one of those reserved slots; unused group
        // reservations are released only when that marker becomes durable.
        let reserved_frontier_bytes = next_ticket
            .checked_sub(state.durable_ticket_end)
            .and_then(|tickets| tickets.checked_sub(u64::from(state.pending_frontier.is_some())))
            .and_then(|tickets| tickets.checked_mul(FRAME_LEN_U64))
            .context("v8 Parallel FRONTIER reservation overflows")?;
        if !data_end
            .checked_add(reserved_frontier_bytes)
            .is_some_and(|end| end <= self.inner.max_file_size)
        {
            let fresh_end = u64::try_from(WAL_V8_HEADER_LEN)
                .ok()
                .and_then(|header| header.checked_add(FRAME_LEN_U64))
                .and_then(|data_start| data_start.checked_add(data_bytes))
                .and_then(|data_end| data_end.checked_add(FRAME_LEN_U64));
            let error = if fresh_end.is_some_and(|end| end <= self.inner.max_file_size) {
                WalV8LeaderCapacityError::RotationRequired
            } else {
                WalV8LeaderCapacityError::BatchTooLarge
            };
            return Err(anyhow::Error::new(error));
        }
        let assignment = WalV8BatchAssignment {
            segment_ticket: ticket,
            commit_ts: plan.commit_ts,
            recorded_at,
            first_data_offset,
            segment_incarnation: self.inner.segment_incarnation,
            physical_header_digest: self.inner.physical_header_digest,
            initial_prefix_digest: self.inner.initial_prefix_digest,
        };

        if !self.progress_cursor_enabled()
            && let Some(progress_ring) = &self.inner.progress_ring
            && let Err(error) = progress_ring.ensure_free(ticket)
        {
            poison_locked(&self.inner, &mut state);
            return Err(error).context("check v8 progress ring admission slot");
        }

        // Register physical-range ownership before publishing the assigned
        // ticket. Extent preparation can otherwise race ahead and initialize
        // a range whose DATA strategy has not yet been made visible. The
        // Coordinator lock and admission bound exclude slot reuse here, so
        // cursor mode only checks the cell in initialize after registration.
        // A failed registration leaves the cell untouched.
        register(assignment)?;

        if let Some(progress_ring) = &self.inner.progress_ring
            && let Err(error) = progress_ring.initialize(assignment, plan.logical_batch_bytes)
        {
            poison_locked(&self.inner, &mut state);
            return Err(error).context("initialize v8 progress ring admission slot");
        }

        state.tickets.push_back(TicketState {
            assignment,
            plan,
            reserved_bytes,
            frames_ready: false,
            digest_ready: None,
            prefix_digest_after: None,
            data_complete: false,
            #[cfg(feature = "bench")]
            data_write_started_at: None,
            #[cfg(feature = "bench")]
            data_completed_at: None,
            #[cfg(feature = "bench")]
            digest_hol_started_at: None,
            #[cfg(feature = "bench")]
            data_completion_hol_started_at: None,
        });
        state.next_ticket = next_ticket;
        state.next_data_offset = data_end;
        state.last_commit_ts = plan.commit_ts;
        state.last_recorded_at = Some(recorded_at);
        state.reserved_bytes = new_reserved_bytes;
        self.inner
            .admitted_ticket_end
            .store(next_ticket, Ordering::Release);
        #[cfg(feature = "bench")]
        if let Some(profile) = self.inner.profile.load_full() {
            profile.record_commit_order_ticket_admitted(plan.commit_ts, ticket);
        }
        signal_change(&self.inner, &mut state);

        Ok((
            assignment,
            WalV8ParallelAdmissionGuard {
                coordinator: self.clone(),
                ticket,
                armed: true,
            },
        ))
    }
}

/// Guard that poisons the complete Active writer if an admitted producer is
/// dropped before its ticket is included in a successful durable FRONTIER.
pub(crate) struct WalV8ParallelAdmissionGuard {
    coordinator: WalV8ParallelCoordinator,
    ticket: u64,
    armed: bool,
}

/// RAII charge for canonicalization indexes allocated before ticket admission.
/// It releases shared-window capacity when preparation encoding consumes the
/// canonical stream or when preflight fails.
pub(crate) struct WalV8PreparationReservation {
    coordinator: WalV8ParallelCoordinator,
    bytes: u64,
}

impl Drop for WalV8PreparationReservation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let mut state = self.coordinator.inner.state.lock();
        let Some(remaining) = state.reserved_preparation_bytes.checked_sub(self.bytes) else {
            poison_locked(&self.coordinator.inner, &mut state);
            return;
        };
        state.reserved_preparation_bytes = remaining;
    }
}

impl WalV8AdmissionGuard for WalV8ParallelAdmissionGuard {
    fn segment_ticket(&self) -> u64 {
        self.ticket
    }

    fn poison(&mut self) {
        if self.armed {
            self.coordinator.poison_ticket_if_pending(self.ticket);
            self.armed = false;
        }
    }

    fn disarm_after_durable(&mut self) {
        if self.armed {
            self.coordinator.poison_ticket_if_pending(self.ticket);
            self.armed = false;
        }
    }

    fn disarm_after_durable_prefix_check(&mut self, prefix_check: &WalV8DurablePrefixCheck) {
        if self.armed && !prefix_check.covers(&self.coordinator, self.ticket) {
            self.coordinator.poison_ticket_if_pending(self.ticket);
        }

        self.armed = false;
    }
}

impl Drop for WalV8ParallelAdmissionGuard {
    fn drop(&mut self) {
        self.poison();
    }
}

impl WalV8ParallelCoordinator {
    fn poison_ticket_if_pending(&self, ticket: u64) {
        let mut state = self.inner.state.lock();

        if ticket >= state.durable_ticket_end {
            poison_locked(&self.inner, &mut state);
        }
    }

    /// Preserve the admission guards' poison fallback with one coordinator
    /// lock when the caller is retiring a contiguous, already-collected range.
    pub(crate) fn poison_ticket_prefix_if_pending(
        &self,
        ticket_end: u64,
    ) -> WalV8DurablePrefixCheck {
        let mut state = self.inner.state.lock();

        if ticket_end > state.durable_ticket_end {
            poison_locked(&self.inner, &mut state);
        }

        WalV8DurablePrefixCheck {
            coordinator: Arc::clone(&self.inner),
            ticket_end,
        }
    }
}

impl TicketState {
    fn progress(&self) -> WalV8TicketProgress {
        WalV8TicketProgress {
            assignment: self.assignment,
            logical_batch_bytes: self.plan.logical_batch_bytes,
            frame_buffer_bytes: self.plan.frame_buffer_bytes,
            frames_ready: self.frames_ready,
            digest_ready: self.digest_ready,
            prefix_digest_after: self.prefix_digest_after,
            data_complete: self.data_complete,
            #[cfg(feature = "bench")]
            data_write_elapsed_ns: self
                .data_write_started_at
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64),
            #[cfg(feature = "bench")]
            data_completed_at: self.data_completed_at,
        }
    }

    fn is_frontier_ready(&self) -> bool {
        self.frames_ready
            && self.digest_ready.is_some()
            && self.prefix_digest_after.is_some()
            && self.data_complete
    }
}

fn ticket_progress(inner: &CoordinatorInner, ticket: &TicketState) -> Result<WalV8TicketProgress> {
    let mut progress = ticket.progress();
    if let Some(ring) = &inner.progress_ring {
        let cell = ring.read(inner.segment_incarnation, ticket.assignment.segment_ticket)?;
        ensure!(
            cell.logical_batch_bytes == Some(u64::try_from(ticket.plan.logical_batch_bytes)?),
            "v8 progress ring payload differs from its admitted ticket"
        );
        progress.frames_ready = cell.frames_ready;
        progress.digest_ready = cell.digest_ready;
    }
    Ok(progress)
}

fn ticket_frames_ready(inner: &CoordinatorInner, ticket: &TicketState) -> Result<bool> {
    if let Some(ring) = &inner.progress_ring {
        return Ok(ring
            .read(inner.segment_incarnation, ticket.assignment.segment_ticket)?
            .frames_ready);
    }
    Ok(ticket.frames_ready)
}

fn ticket_is_frontier_ready(inner: &CoordinatorInner, ticket: &TicketState) -> Result<bool> {
    Ok(ticket_progress(inner, ticket)?.is_frontier_ready())
}

fn empty_frontier_ready_snapshot() -> WalV8FrontierReadySnapshot {
    WalV8FrontierReadySnapshot {
        ready_prefix: WalV8ReadyPrefix::Tickets(Vec::new()),
        admitted_ticket_count: 0,
        closed: false,
        #[cfg(feature = "bench")]
        frames_ready_prefix_count: 0,
        #[cfg(feature = "bench")]
        data_complete_prefix_count: 0,
        #[cfg(feature = "bench")]
        reserved_preparation_bytes: 0,
        #[cfg(feature = "bench")]
        next_not_ready: None,
        #[cfg(feature = "bench")]
        data_hol_blocker: None,
        #[cfg(feature = "bench")]
        snapshot_monotonic_ns: None,
    }
}

fn ready_prefix_reaches_frontier_floor(state: &CoordinatorState, ticket_count: usize) -> bool {
    state
        .durable_ticket_end
        .saturating_add(u64::try_from(ticket_count).unwrap_or(u64::MAX))
        >= state.last_frontier_ticket_end
}

fn empty_ready_prefix_like(prefix: &WalV8ReadyPrefix) -> WalV8ReadyPrefix {
    match prefix {
        WalV8ReadyPrefix::Tickets(_) => WalV8ReadyPrefix::Tickets(Vec::new()),
        WalV8ReadyPrefix::Cursor { .. } => WalV8ReadyPrefix::Cursor {
            ticket_count: 0,
            tail: None,
            #[cfg(feature = "bench")]
            data_bytes: 0,
        },
    }
}

fn enforce_frontier_ticket_floor(
    state: &CoordinatorState,
    snapshot: &mut WalV8FrontierReadySnapshot,
) {
    if !ready_prefix_reaches_frontier_floor(state, snapshot.ready_prefix.len()) {
        snapshot.ready_prefix = empty_ready_prefix_like(&snapshot.ready_prefix);
    }
}

fn frontier_ready_snapshot_locked(
    inner: &CoordinatorInner,
    state: &CoordinatorState,
    capture_monotonic_ns: bool,
) -> Result<WalV8FrontierReadySnapshot> {
    #[cfg(not(feature = "bench"))]
    let _ = capture_monotonic_ns;
    let mut snapshot = empty_frontier_ready_snapshot();
    snapshot.closed = state.closed;
    if state.poisoned {
        return Ok(snapshot);
    }
    snapshot.admitted_ticket_count = state.tickets.len();
    if inner.progress_ring.is_none() {
        snapshot.ready_prefix = WalV8ReadyPrefix::Tickets(
            state
                .tickets
                .iter()
                .take_while(|ticket| ticket.is_frontier_ready())
                .map(TicketState::progress)
                .collect(),
        );
        #[cfg(feature = "bench")]
        {
            snapshot.frames_ready_prefix_count = state
                .tickets
                .iter()
                .take_while(|ticket| ticket.frames_ready)
                .count();
            snapshot.data_complete_prefix_count = state
                .tickets
                .iter()
                .take_while(|ticket| ticket.data_complete)
                .count();
            snapshot.next_not_ready = state
                .tickets
                .get(snapshot.ready_prefix.len())
                .map(TicketState::progress);
            snapshot.data_hol_blocker = state
                .tickets
                .get(snapshot.data_complete_prefix_count)
                .map(TicketState::progress);
        }
    } else {
        let mut ready_prefix = Vec::new();
        for (index, ticket) in state.tickets.iter().enumerate() {
            // Producers can publish readiness while the Coordinator lock is
            // held. Reuse this view for both the counts and blocker fields.
            let progress = ticket_progress(inner, ticket)?;
            if ready_prefix.len() == index {
                if progress.is_frontier_ready() {
                    ready_prefix.push(progress);
                } else {
                    #[cfg(not(feature = "bench"))]
                    break;
                    #[cfg(feature = "bench")]
                    if capture_monotonic_ns {
                        snapshot.next_not_ready = Some(progress);
                    } else {
                        break;
                    }
                }
            }
            #[cfg(feature = "bench")]
            if capture_monotonic_ns {
                if snapshot.frames_ready_prefix_count == index && progress.frames_ready {
                    snapshot.frames_ready_prefix_count += 1;
                }
                if snapshot.data_complete_prefix_count == index {
                    if progress.data_complete {
                        snapshot.data_complete_prefix_count += 1;
                    } else {
                        snapshot.data_hol_blocker = Some(progress);
                    }
                }
            }
            #[cfg(feature = "bench")]
            if snapshot.frames_ready_prefix_count <= index
                && snapshot.data_hol_blocker.is_some()
                && snapshot.next_not_ready.is_some()
            {
                break;
            }
        }
        snapshot.ready_prefix = WalV8ReadyPrefix::Tickets(ready_prefix);
    }
    #[cfg(feature = "bench")]
    {
        snapshot.reserved_preparation_bytes = state.reserved_preparation_bytes;
        snapshot.snapshot_monotonic_ns =
            capture_monotonic_ns.then(crate::mem_table::benchmark_monotonic_ns);
    }

    enforce_frontier_ticket_floor(state, &mut snapshot);

    Ok(snapshot)
}

fn cursor_frontier_snapshot_locked(
    state: &CoordinatorState,
    cursor: WalV8ReadyCursor,
) -> Result<WalV8FrontierReadySnapshot> {
    let ticket_count = ready_cursor_index(state, cursor)?;
    let tail = if ticket_count == 0 {
        None
    } else {
        let entry = &state.tickets[ticket_count - 1];
        Some(WalV8FrontierTail {
            first_data_offset: entry.assignment.first_data_offset,
            commit_ts: entry.assignment.commit_ts,
            frame_buffer_bytes: entry.plan.frame_buffer_bytes,
        })
    };
    let mut snapshot = empty_frontier_ready_snapshot();
    snapshot.ready_prefix = WalV8ReadyPrefix::Cursor {
        ticket_count,
        tail,
        #[cfg(feature = "bench")]
        data_bytes: cursor.data_bytes,
    };
    snapshot.admitted_ticket_count = state.tickets.len();
    snapshot.closed = state.closed;
    enforce_frontier_ticket_floor(state, &mut snapshot);
    Ok(snapshot)
}

fn ready_cursor_index(state: &CoordinatorState, cursor: WalV8ReadyCursor) -> Result<usize> {
    let index = usize::try_from(
        cursor
            .ticket_end
            .checked_sub(state.durable_ticket_end)
            .context("v8 ready cursor precedes the durable boundary")?,
    )?;
    ensure!(
        index <= state.tickets.len() && cursor.ticket_end <= state.folded_ticket_end,
        "v8 ready cursor exceeds the admitted or folded boundary"
    );
    Ok(index)
}

fn advance_ready_cursor(state: &mut CoordinatorState) -> Result<()> {
    let Some(mut cursor) = state.ready_cursor else {
        return Ok(());
    };
    let index = ready_cursor_index(state, cursor)?;
    for ticket in state.tickets.range(index..) {
        // DATA completion validates frame readiness for every member of a
        // group before setting data_complete. Neither that flag nor the
        // accepted and folded producer digest can regress on a live ticket.
        if !ticket.data_complete || cursor.ticket_end == state.folded_ticket_end {
            break;
        }
        ensure!(
            ticket.assignment.segment_ticket == cursor.ticket_end
                && ticket.prefix_digest_after.is_some(),
            "v8 ready cursor encountered an invalid ticket"
        );
        cursor.ticket_end = cursor
            .ticket_end
            .checked_add(1)
            .context("v8 ready cursor overflows")?;
        #[cfg(feature = "bench")]
        {
            cursor.data_bytes = cursor
                .data_bytes
                .checked_add(u64::try_from(ticket.plan.frame_buffer_bytes)?)
                .context("v8 ready prefix DATA byte count overflows")?;
        }
    }
    state.ready_cursor = Some(cursor);
    Ok(())
}

/// Fold producer digests in strict ticket order, even when later producers
/// finish hashing first. The fold is bounded by the admitted ticket window.
fn fold_ready_digests(
    inner: &CoordinatorInner,
    state: &mut CoordinatorState,
    hol_profile: Option<&crate::mem_table::WriteProfile>,
) -> Result<()> {
    #[cfg(not(feature = "bench"))]
    let _ = hol_profile;

    loop {
        let index = usize::try_from(
            state
                .folded_ticket_end
                .checked_sub(state.durable_ticket_end)
                .context("v8 folded ticket end precedes the durable boundary")?,
        )
        .context("v8 folded ticket index exceeds usize")?;
        let Some(ticket) = state.tickets.get(index) else {
            break;
        };
        let batch_digest = if let Some(ring) = &inner.progress_ring {
            ring.read(inner.segment_incarnation, ticket.assignment.segment_ticket)?
                .digest_ready
        } else {
            ticket.digest_ready
        };
        let Some(batch_digest) = batch_digest else {
            break;
        };
        ensure!(
            ticket.assignment.segment_ticket == state.folded_ticket_end,
            "v8 ordered digest fold encountered a noncontiguous ticket"
        );
        let logical_batch_bytes = u64::try_from(ticket.plan.logical_batch_bytes)
            .context("v8 folded logical batch length exceeds u64")?;
        #[cfg(feature = "bench")]
        let digest_hol_started_at = ticket.digest_hol_started_at;
        #[cfg(feature = "bench")]
        let hash_started = super::digest::should_sample_hash_cpu(state.folded_ticket_end)
            .then(super::digest::thread_cpu_time_ns);
        let prefix_digest = super::digest::advance_prefix(
            &state.folded_prefix_digest,
            state.folded_ticket_end,
            logical_batch_bytes,
            &batch_digest.batch_digest,
        );
        #[cfg(feature = "bench")]
        if let Some(hash_started) = hash_started {
            state.ordered_link_hash_cpu_ns = state
                .ordered_link_hash_cpu_ns
                .saturating_add(super::digest::thread_cpu_time_ns().saturating_sub(hash_started));
        }
        let next_ticket = state
            .folded_ticket_end
            .checked_add(1)
            .context("v8 folded ticket cursor overflows")?;
        state.tickets[index].prefix_digest_after = Some(prefix_digest);
        #[cfg(feature = "bench")]
        {
            state.tickets[index].digest_hol_started_at = None;
            if let (Some(profile), Some(started_at)) = (hol_profile, digest_hol_started_at) {
                profile.record_wal_v8_digest_order_hol(elapsed_ns(started_at, Instant::now()));
            }
        }
        state.folded_ticket_end = next_ticket;
        state.folded_prefix_digest = prefix_digest;
    }

    Ok(())
}

#[cfg(feature = "bench")]
fn refresh_data_completion_order_hol(inner: &CoordinatorInner, state: &mut CoordinatorState) {
    if !inner.hol_diagnostics_enabled.load(Ordering::Acquire) {
        return;
    }
    let Some(profile) = inner.profile.load_full() else {
        return;
    };

    let now = Instant::now();
    let mut prefix_open = true;
    for ticket in &mut state.tickets {
        let ready = ticket.data_complete;
        if prefix_open && ready {
            if let Some(started_at) = ticket.data_completion_hol_started_at.take() {
                profile.record_wal_v8_data_completion_hol(elapsed_ns(started_at, now));
            }
        } else if ready {
            ticket.data_completion_hol_started_at.get_or_insert(now);
        } else {
            prefix_open = false;
        }
    }
}

#[cfg(feature = "bench")]
fn advance_completion_timeline_prefix(state: &mut CoordinatorState) {
    state.completion_timeline_frontier_ready_end = state
        .completion_timeline_frontier_ready_end
        .max(state.durable_ticket_end);
    let previous_end = state.completion_timeline_frontier_ready_end;
    while let Some(index) = state
        .completion_timeline_frontier_ready_end
        .checked_sub(state.durable_ticket_end)
        .and_then(|offset| usize::try_from(offset).ok())
    {
        let Some(ticket) = state.tickets.get(index) else {
            break;
        };
        if ticket.assignment.segment_ticket != state.completion_timeline_frontier_ready_end
            || !ticket.is_frontier_ready()
        {
            break;
        }
        state.completion_timeline_frontier_ready_end += 1;
    }

    if state.completion_timeline_frontier_ready_end > previous_end {
        let now = Instant::now();
        let ready_end = state.completion_timeline_frontier_ready_end;
        for sample in state
            .completion_timeline_samples
            .range_mut((
                std::ops::Bound::Excluded(previous_end),
                std::ops::Bound::Included(ready_end),
            ))
            .map(|(_, sample)| sample)
        {
            sample.prefix_ready_at = Some(now);
        }
    }
}

#[cfg(feature = "bench")]
fn data_complete_prefix_end(state: &CoordinatorState) -> u64 {
    let completed = state
        .tickets
        .iter()
        .take_while(|ticket| ticket.data_complete)
        .count();
    state
        .durable_ticket_end
        .saturating_add(u64::try_from(completed).unwrap_or(u64::MAX))
}

#[cfg(feature = "bench")]
fn completion_timeline_duration_ns(start: Option<Instant>, end: Option<Instant>) -> Option<u64> {
    Some(
        end?.checked_duration_since(start?)?
            .as_nanos()
            .min(u64::MAX as u128) as u64,
    )
}

#[cfg(feature = "bench")]
fn elapsed_ns(started_at: Instant, ended_at: Instant) -> u64 {
    ended_at
        .saturating_duration_since(started_at)
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn validate_plan(plan: WalV8AdmissionPlan) -> Result<()> {
    ensure!(plan.commit_ts > 0, "v8 admission commit timestamp is zero");
    ensure!(
        plan.recorded_at.nanos < 1_000_000_000,
        "v8 admission recorded-at timestamp is invalid"
    );
    let logical_batch_bytes = u64::try_from(plan.logical_batch_bytes)?;
    let expected_fragment_count = codec::data_fragment_count(logical_batch_bytes)
        .context("v8 admission logical batch length is outside wire bounds")?;
    ensure!(
        expected_fragment_count == plan.fragment_count,
        "v8 admission fragment count differs from its logical batch size"
    );
    let expected_frame_bytes = usize::try_from(expected_fragment_count)?
        .checked_mul(WAL_V8_FRAME_LEN)
        .context("v8 admission DATA frame length overflows")?;
    ensure!(
        expected_frame_bytes == plan.frame_buffer_bytes,
        "v8 admission frame buffer length differs from its fragment count"
    );

    Ok(())
}

fn signal_change(inner: &CoordinatorInner, state: &mut CoordinatorState) {
    state.change_generation = state.change_generation.wrapping_add(1);
    inner.changed.notify_all();
}

fn signal_durability_progress(inner: &CoordinatorInner, state: &mut CoordinatorState) {
    state.durability_generation = state.durability_generation.wrapping_add(1);
    inner.durability_changed.notify_all();
}

#[cfg(feature = "bench")]
fn record_durable_notification_for_waiters(state: &mut CoordinatorState) {
    if state.durability_driver_waiters > 0 {
        state.last_durability_notify_at = Some(Instant::now());
        state.last_durability_notify_waiters = state.durability_driver_waiters;
    }
}

fn signal_progress_token(inner: &CoordinatorInner) {
    let _ = try_signal_progress_token(inner);
}

fn try_signal_progress_token(inner: &CoordinatorInner) -> Result<()> {
    let Some(sender) = &inner.progress_token_sender else {
        anyhow::bail!("v8 progress token sender is unavailable");
    };
    let result = sender.try_send(());
    #[cfg(feature = "bench")]
    if inner
        .data_completion_timestamps_enabled
        .load(Ordering::Relaxed)
        || inner.progress_counters_enabled.load(Ordering::Relaxed)
    {
        inner
            .progress_token_attempts
            .fetch_add(1, Ordering::Relaxed);
        if result.is_ok() {
            inner.progress_token_accepts.fetch_add(1, Ordering::Relaxed);
        }
    }
    match result {
        Ok(()) | Err(crossbeam_channel::TrySendError::Full(())) => Ok(()),
        Err(crossbeam_channel::TrySendError::Disconnected(())) => {
            anyhow::bail!("v8 progress token receiver disconnected")
        }
    }
}

fn poison_locked(inner: &CoordinatorInner, state: &mut CoordinatorState) {
    if !state.poisoned {
        state.poisoned = true;
        if let Some(ring) = &inner.progress_ring {
            ring.poisoned.store(true, Ordering::Release);
        }
        signal_progress_token(inner);
        signal_change(inner, state);
        signal_durability_progress(inner, state);
        #[cfg(feature = "bench")]
        record_durable_notification_for_waiters(state);
        inner.durable_published.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pitr::{ArchiveEpochId, ChainAnchor, SegmentId, TimelineId, WalBatch, WalEntry},
        wal::v8::prepare::{PreparedWalV8Batch, WalV8WriteClass},
    };

    fn header() -> WalV8Header {
        let archive_epoch_id = ArchiveEpochId([0x22; 16]);

        WalV8Header {
            timeline_id: TimelineId([0x11; 16]),
            archive_epoch_id,
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [0x44; 16],
        }
    }

    fn coordinator_with_byte_limit(
        max_inflight_tickets: usize,
        max_inflight_bytes: u64,
    ) -> Result<WalV8ParallelCoordinator> {
        coordinator_with_limits(max_inflight_tickets, max_inflight_bytes, MAX_WAL_FILE_SIZE)
    }

    fn coordinator_with_limits(
        max_inflight_tickets: usize,
        max_inflight_bytes: u64,
        max_file_size: u64,
    ) -> Result<WalV8ParallelCoordinator> {
        let wal_header = header();

        WalV8ParallelCoordinator::new(
            wal_header,
            WalV8ParallelConfig {
                next_ticket: 0,
                next_data_offset: DATA_START,
                last_commit_ts: 0,
                last_recorded_at: None,
                durable_prefix_digest: wal_header.initial_prefix_digest()?,
                max_inflight_tickets,
                max_inflight_bytes,
                max_file_size,
                progress_mode: WalV8ProgressMode::Eager,
            },
        )
    }

    fn coordinator_with_progress_ring(
        capacity: usize,
        next_ticket: u64,
    ) -> Result<WalV8ParallelCoordinator> {
        coordinator_with_progress_mode(capacity, next_ticket, WalV8ProgressMode::Cursor)
    }

    fn coordinator_with_progress_mode(
        capacity: usize,
        next_ticket: u64,
        progress_mode: WalV8ProgressMode,
    ) -> Result<WalV8ParallelCoordinator> {
        let wal_header = header();
        let durable_prefix_digest = if next_ticket == 0 {
            wal_header.initial_prefix_digest()?
        } else {
            [0xa7; 32]
        };
        let next_data_offset = DATA_START
            .checked_add(next_ticket.saturating_sub(1).saturating_mul(FRAME_LEN_U64))
            .context("test progress ring DATA offset overflows")?;

        WalV8ParallelCoordinator::new(
            wal_header,
            WalV8ParallelConfig {
                next_ticket,
                next_data_offset,
                last_commit_ts: next_ticket.saturating_mul(10),
                last_recorded_at: (next_ticket > 0).then_some(RecordedAt {
                    secs: i64::try_from(next_ticket.saturating_mul(10)).unwrap_or(i64::MAX),
                    nanos: 17,
                }),
                durable_prefix_digest,
                max_inflight_tickets: capacity,
                max_inflight_bytes: 64 * 1024 * 1024,
                max_file_size: MAX_WAL_FILE_SIZE,
                progress_mode,
            },
        )
    }

    fn coordinator(max_inflight_tickets: usize) -> Result<WalV8ParallelCoordinator> {
        coordinator_with_byte_limit(max_inflight_tickets, 64 * 1024 * 1024)
    }

    #[test]
    fn poison_ticket_prefix_if_pending_checks_the_prefix_once() -> Result<()> {
        let coordinator = coordinator(4)?;
        coordinator.inner.state.lock().durable_ticket_end = 3;

        let covered = coordinator.poison_ticket_prefix_if_pending(3);
        assert!(covered.covers(&coordinator, 2));
        assert!(!covered.covers(&coordinator, 3));
        assert!(!coordinator.is_poisoned());

        coordinator.poison_ticket_prefix_if_pending(4);
        assert!(coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn disarm_after_durable_prefix_check_keeps_covered_guard_unarmed() -> Result<()> {
        let coordinator = coordinator(4)?;
        let batch = batch(1);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;
        coordinator.inner.state.lock().durable_ticket_end = 1;

        let prefix_check = coordinator.poison_ticket_prefix_if_pending(1);
        encoded.disarm_after_durable_prefix_check(&prefix_check);
        drop(encoded);

        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn disarm_after_durable_prefix_check_keeps_uncovered_guard_fail_safe() -> Result<()> {
        let coordinator = coordinator(4)?;
        let batch = batch(1);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;
        let prefix_check = coordinator.poison_ticket_prefix_if_pending(0);

        encoded.disarm_after_durable_prefix_check(&prefix_check);
        drop(encoded);

        assert!(coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn durable_prefix_check_cannot_disarm_another_coordinators_guard() -> Result<()> {
        let writer_coordinator = coordinator(4)?;
        let other_coordinator = coordinator(4)?;
        let batch = batch(1);
        let mut encoded = admit(&batch, &writer_coordinator)?.encode()?;
        other_coordinator.inner.state.lock().durable_ticket_end = 1;
        let foreign_check = other_coordinator.poison_ticket_prefix_if_pending(1);

        encoded.disarm_after_durable_prefix_check(&foreign_check);
        drop(encoded);

        assert!(writer_coordinator.is_poisoned());
        assert!(!other_coordinator.is_poisoned());

        Ok(())
    }

    fn batch(commit_ts: u64) -> WalBatch {
        WalBatch {
            commit_ts,
            recorded_at: RecordedAt {
                secs: i64::try_from(commit_ts).unwrap_or(i64::MAX),
                nanos: 17,
            },
            entries: vec![WalEntry::Put {
                key: b"key".to_vec(),
                value: vec![0x5a; 32],
            }],
        }
    }

    fn admit<'a>(
        batch: &'a WalBatch,
        coordinator: &WalV8ParallelCoordinator,
    ) -> Result<super::super::prepare::AdmittedWalV8Batch<'a, WalV8ParallelAdmissionGuard, ()>>
    {
        PreparedWalV8Batch::<()>::preflight(batch, crate::pitr::LIVE_WAL_V5_LIMITS, |_| Ok(()))?
            .admit(coordinator)
    }

    fn admission_plan(batch: &WalBatch) -> Result<WalV8AdmissionPlan> {
        let prepared = PreparedWalV8Batch::<()>::preflight(
            batch,
            crate::pitr::LIVE_WAL_V5_LIMITS,
            |_| Ok(()),
        )?;

        Ok(WalV8AdmissionPlan {
            commit_ts: batch.commit_ts,
            recorded_at: batch.recorded_at,
            write_class: WalV8WriteClass::Batch,
            logical_batch_bytes: prepared.logical_batch_bytes(),
            #[cfg(feature = "bench")]
            operation_count: u32::try_from(batch.entries.len())?,
            fragment_count: prepared.fragment_count(),
            frame_buffer_bytes: prepared.frame_buffer_bytes(),
        })
    }

    fn mark_producer_ready(
        encoded: &mut super::super::prepare::EncodedWalV8Batch<WalV8ParallelAdmissionGuard>,
        coordinator: &WalV8ParallelCoordinator,
        mark_data_complete: bool,
    ) -> Result<()> {
        let assignment = encoded.assignment();
        coordinator.mark_digest_ready(encoded.digest_ready()?)?;
        let mut frames = vec![0; encoded.frame_buffer_bytes()];
        let written = encoded.encode_data_frames_into(&mut frames)?;
        ensure!(
            written == frames.len(),
            "v8 test frame buffer length changed"
        );
        coordinator.mark_frames_ready(assignment.segment_ticket)?;
        if mark_data_complete {
            coordinator.mark_data_complete(assignment.segment_ticket)?;
        }

        Ok(())
    }

    #[test]
    fn progress_cursor_preserves_registration_completion_and_poison_guards() -> Result<()> {
        let mode = WalV8ProgressMode::Cursor;
        check_progress_registration_failure(mode)?;
        check_progress_late_digest_wake(mode)?;
        check_progress_single_slot_reuse(mode)?;
        check_progress_group_atomicity(mode, false)?;
        check_progress_group_atomicity(mode, true)?;
        check_progress_stale_digest(mode)
    }

    #[test]
    fn progress_cursor_post_registration_slot_conflict_is_fail_stop() -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, WalV8ProgressMode::Cursor)?;
        let ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .context("missing ring")?;
        let input = batch(10);
        let plan = admission_plan(&input)?;
        let mut registered = false;
        let result = coordinator.admit_and_publish_with(plan, |assignment| {
            let mut payload = ring
                .cell(assignment.segment_ticket)?
                .payload
                .try_lock()
                .context("registration must run without holding the cell lock")?;
            payload.identity = Some(TicketCellIdentity {
                segment_incarnation: assignment.segment_incarnation,
                ticket: assignment.segment_ticket,
            });
            registered = true;
            Ok(())
        });
        assert!(registered);
        assert!(result.is_err());
        assert!(coordinator.is_poisoned());
        assert_eq!(coordinator.admitted_ticket_end(), 0);
        assert!(coordinator.inner.state.lock().tickets.is_empty());

        Ok(())
    }

    #[test]
    fn progress_cursor_retirement_failure_keeps_reserved_prefix_unpublished() -> Result<()> {
        let coordinator = coordinator_with_progress_mode(2, 0, WalV8ProgressMode::Cursor)?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let mut second = admit(&second_batch, &coordinator)?.encode()?;
        mark_producer_ready(&mut first, &coordinator, true)?;
        mark_producer_ready(&mut second, &coordinator, true)?;
        let driver = coordinator
            .acquire_durability_driver(2)?
            .context("missing driver")?;
        coordinator.advance_and_snapshot(&driver, &[], false)?;
        let frontier = coordinator.reserve_frontier(2)?;

        // Simulate corrupted bookkeeping at the publication boundary, after
        // the reservation was validated and physical I/O would have run.
        let ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .context("missing ring")?;
        let first_before = ring.snapshot(0)?;
        ring.cell(1)?.payload.lock().frames_ready = false;
        assert!(coordinator.publish_durable_frontier(frontier).is_err());
        assert!(coordinator.is_poisoned());
        assert_eq!(coordinator.durable_ticket_end(), 0);
        assert_eq!(ring.snapshot(0)?, first_before);
        assert_eq!(coordinator.inner.state.lock().tickets.len(), 2);

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn lightweight_cursor_progress_counters_keep_the_cursor_snapshot_path() -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, WalV8ProgressMode::Cursor)?;
        coordinator.set_progress_counters_enabled(true)?;
        assert!(
            !coordinator
                .inner
                .data_completion_timestamps_enabled
                .load(Ordering::Acquire)
        );

        let input = batch(10);
        let mut encoded = admit(&input, &coordinator)?.encode()?;
        coordinator.mark_digest_ready_before_data(encoded.digest_ready()?)?;
        coordinator.mark_frames_ready(encoded.assignment().segment_ticket)?;
        let mut driver = coordinator
            .acquire_durability_driver(1)?
            .context("missing durability driver")?;
        let event = WalV8CompletedGroupEvent {
            group_id: 1,
            tickets: 0..1,
            write_count: 1,
            write_bytes: FRAME_LEN_U64,
            completion_observed_at: None,
        };

        let snapshot = coordinator.advance_and_snapshot(&driver, &[event], false)?;
        assert!(matches!(
            snapshot.ready_prefix,
            WalV8ReadyPrefix::Cursor { .. }
        ));
        let stats = coordinator
            .progress_diagnostics()
            .context("lightweight counters were not exposed")?;
        assert_eq!(stats.owner_scans, 1);
        assert_eq!(stats.scans_without_ready_progress, 0);
        assert_eq!(stats.completion_events_drained, 1);

        let unchanged = coordinator.advance_and_snapshot(&driver, &[], false)?;
        assert!(matches!(
            unchanged.ready_prefix,
            WalV8ReadyPrefix::Cursor { .. }
        ));
        let stats = coordinator
            .progress_diagnostics()
            .context("lightweight counters disappeared")?;
        assert_eq!(stats.owner_scans, 2);
        assert_eq!(stats.scans_without_ready_progress, 1);

        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        encoded.disarm_after_durable();
        driver.complete();

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn lightweight_cursor_progress_counters_count_tokens_without_timestamps() -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, WalV8ProgressMode::Cursor)?;
        coordinator.set_progress_counters_enabled(true)?;
        assert!(
            !coordinator
                .inner
                .data_completion_timestamps_enabled
                .load(Ordering::Acquire)
        );

        let tokens = coordinator
            .take_progress_token_receiver()?
            .context("missing progress token")?;
        coordinator.signal_progress_token();
        coordinator.signal_progress_token();

        assert_eq!(tokens.try_recv(), Ok(()));
        assert!(
            tokens.is_empty(),
            "the second token should coalesce into the first"
        );
        let stats = coordinator
            .progress_diagnostics()
            .context("lightweight counters were not exposed")?;
        assert_eq!(stats.token_attempts, 2);
        assert_eq!(stats.token_accepts, 1);

        Ok(())
    }

    #[test]
    fn progress_cursor_advances_through_a_gap_and_survives_partial_retirement_and_handoff()
    -> Result<()> {
        let coordinator = coordinator_with_progress_mode(2, 1, WalV8ProgressMode::Cursor)?;
        let tokens = coordinator
            .take_progress_token_receiver()?
            .context("missing progress token")?;
        let first_batch = batch(20);
        let second_batch = batch(30);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let mut second = admit(&second_batch, &coordinator)?.encode()?;
        #[cfg(feature = "bench")]
        coordinator.set_data_completion_timestamps_enabled(true);
        for encoded in [&mut first, &mut second] {
            coordinator.mark_digest_ready_before_data(encoded.digest_ready()?)?;
            coordinator.mark_frames_ready(encoded.assignment().segment_ticket)?;
        }
        assert!(tokens.is_empty(), "DATA submission will supply the wake");

        let mut driver = coordinator
            .acquire_durability_driver(2)?
            .context("missing driver")?;
        let storage = driver
            .completion_batch
            .as_ref()
            .context("missing drain buffer")?
            .as_ptr();
        let event = |ticket| WalV8CompletedGroupEvent {
            group_id: ticket,
            tickets: ticket..ticket + 1,
            write_count: 1,
            write_bytes: FRAME_LEN_U64,
            #[cfg(feature = "bench")]
            completion_observed_at: Some(Instant::now()),
        };
        coordinator.notify_completion_queued();
        coordinator.notify_completion_queued();
        assert_eq!(tokens.try_recv(), Ok(()));
        assert!(
            tokens.is_empty(),
            "the second token coalesced into the first"
        );
        let gap = coordinator.advance_and_snapshot(&driver, &[event(2)], false)?;
        assert!(gap.ready_prefix.is_empty());
        assert!(coordinator.reserve_frontier(3).is_err());

        // Applying the same fully completed live range again is idempotent.
        let filled = coordinator.advance_and_snapshot(&driver, &[event(2), event(1)], false)?;
        assert!(matches!(
            filled.ready_prefix,
            WalV8ReadyPrefix::Cursor { .. }
        ));
        assert_eq!(filled.ready_prefix.len(), 2);
        assert_eq!(
            filled
                .ready_prefix
                .tail()
                .context("missing ready tail")?
                .commit_ts,
            30
        );
        #[cfg(feature = "bench")]
        {
            assert_eq!(filled.ready_prefix.data_bytes(), 2 * FRAME_LEN_U64);
            let detailed = coordinator.advance_and_snapshot(&driver, &[], true)?;
            assert_eq!(detailed.ready_prefix.len(), filled.ready_prefix.len());
            assert_eq!(detailed.data_complete_prefix_count, 2);
            coordinator.advance_and_snapshot(&driver, &[], true)?;
            let stats = coordinator
                .progress_diagnostics()
                .context("missing progress diagnostics")?;
            assert_eq!(stats.token_attempts, 2);
            assert_eq!(stats.token_accepts, 1);
            assert_eq!(stats.owner_scans, 2);
            assert_eq!(stats.scans_without_ready_progress, 1);
            assert_eq!(stats.completion_events_drained, 0);
        }

        let first_frontier = coordinator.reserve_frontier(2)?;
        assert_eq!(
            coordinator.prefix_digest_at(2),
            Some(first_frontier.prefix_digest)
        );
        coordinator.publish_durable_frontier(first_frontier)?;
        first.disarm_after_durable();
        driver.complete();
        drop(driver);

        let third_batch = batch(40);
        let mut third = admit(&third_batch, &coordinator)?.encode()?;
        assert_eq!(third.assignment().segment_ticket, 3);
        coordinator.mark_digest_ready_before_data(third.digest_ready()?)?;
        coordinator.mark_frames_ready(3)?;
        let mut successor = coordinator
            .acquire_durability_driver(4)?
            .context("missing successor")?;
        assert_eq!(
            successor
                .completion_batch
                .as_ref()
                .context("missing drain buffer")?
                .as_ptr(),
            storage,
        );
        let carried = coordinator.advance_and_snapshot(&successor, &[], false)?;
        assert_eq!(carried.ready_prefix.len(), 1);
        #[cfg(feature = "bench")]
        assert_eq!(carried.ready_prefix.data_bytes(), FRAME_LEN_U64);
        let wrapped = coordinator.advance_and_snapshot(&successor, &[event(3)], false)?;
        assert_eq!(wrapped.ready_prefix.len(), 2);
        let frontier = coordinator.reserve_frontier(4)?;
        assert_eq!(
            coordinator.prefix_digest_at(4),
            Some(frontier.prefix_digest)
        );
        coordinator.publish_durable_frontier(frontier)?;
        second.disarm_after_durable();
        third.disarm_after_durable();
        successor.complete();
        assert_eq!(coordinator.durable_ticket_end(), 4);
        assert_eq!(coordinator.inner.state.lock().reserved_bytes, 0);
        assert!(!coordinator.is_poisoned());
        assert!(
            coordinator
                .advance_and_snapshot(&successor, &[event(3)], false)
                .is_err(),
            "a completion for a retired ticket must be rejected",
        );
        assert!(coordinator.is_poisoned());
        assert_eq!(coordinator.durable_ticket_end(), 4);

        Ok(())
    }

    #[test]
    fn progress_ring_registration_failure_leaves_slot_free_for_same_ticket_retry() -> Result<()> {
        check_progress_registration_failure(WalV8ProgressMode::Cursor)
    }

    fn check_progress_registration_failure(mode: WalV8ProgressMode) -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, mode)?;
        let batch = batch(10);
        let plan = admission_plan(&batch)?;

        let failed = coordinator.admit_and_publish_with(plan, |_| {
            anyhow::bail!("simulated extent registration failure")
        });
        assert!(failed.is_err());
        assert!(!coordinator.is_poisoned());
        assert_eq!(coordinator.admitted_ticket_end(), 0);
        assert_eq!(coordinator.inner.state.lock().next_ticket, 0);
        coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .ensure_free(0)?;

        let (assignment, _guard) = coordinator.admit_and_publish_with(plan, |_| Ok(()))?;
        assert_eq!(assignment.segment_ticket, 0);
        assert_eq!(coordinator.admitted_ticket_end(), 1);
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(0)?
                .identity,
            Some(TicketCellIdentity {
                segment_incarnation: header().incarnation,
                ticket: 0,
            })
        );

        Ok(())
    }

    #[test]
    fn progress_ring_reuses_single_slot_only_after_durable_publication() -> Result<()> {
        check_progress_single_slot_reuse(WalV8ProgressMode::Cursor)
    }

    fn check_progress_single_slot_reuse(mode: WalV8ProgressMode) -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, mode)?;
        let first_batch = batch(10);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        mark_producer_ready(&mut first, &coordinator, true)?;
        assert!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .ensure_free(1)
                .is_err()
        );

        let mut driver = coordinator
            .acquire_durability_driver(1)?
            .context("test driver was not elected")?;
        coordinator.advance_and_snapshot(&driver, &[], false)?;
        let first_frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(first_frontier)?;
        driver.complete();
        first.disarm_after_durable();
        coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .ensure_free(0)?;

        let second_batch = batch(20);
        let second = admit(&second_batch, &coordinator)?;
        assert_eq!(second.assignment().segment_ticket, 1);
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(1)?
                .identity,
            Some(TicketCellIdentity {
                segment_incarnation: header().incarnation,
                ticket: 1,
            })
        );

        Ok(())
    }

    #[test]
    fn progress_ring_reuses_capacity_two_slots_by_absolute_ticket() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 0)?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let mut second = admit(&second_batch, &coordinator)?.encode()?;
        mark_producer_ready(&mut first, &coordinator, true)?;
        mark_producer_ready(&mut second, &coordinator, true)?;

        let mut driver = coordinator
            .acquire_durability_driver(2)?
            .context("test driver was not elected")?;
        coordinator.advance_and_snapshot(&driver, &[], false)?;
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        driver.complete();
        first.disarm_after_durable();
        second.disarm_after_durable();

        let third_batch = batch(30);
        let fourth_batch = batch(40);
        let third = admit(&third_batch, &coordinator)?;
        let fourth = admit(&fourth_batch, &coordinator)?;
        assert_eq!(third.assignment().segment_ticket, 2);
        assert_eq!(fourth.assignment().segment_ticket, 3);

        let ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        assert_eq!(ring.snapshot(2)?.identity.unwrap().ticket, 2);
        assert_eq!(ring.snapshot(3)?.identity.unwrap().ticket, 3);

        Ok(())
    }

    #[test]
    fn progress_ring_maps_nonzero_recovered_ticket_to_its_absolute_slot() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 5)?;
        let recovered_batch = batch(60);
        let recovered = admit(&recovered_batch, &coordinator)?;
        assert_eq!(recovered.assignment().segment_ticket, 5);

        let next_batch = batch(70);
        let next = admit(&next_batch, &coordinator)?;
        assert_eq!(next.assignment().segment_ticket, 6);

        let ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        assert_eq!(ring.snapshot(5)?.identity.unwrap().ticket, 5);
        assert_eq!(ring.snapshot(6)?.identity.unwrap().ticket, 6);

        Ok(())
    }

    #[test]
    fn progress_ring_readiness_publishes_without_the_coordinator_lock() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let input = batch(10);
        let encoded = admit(&input, &coordinator)?.encode()?;
        let digest = encoded.digest_ready()?;
        let tokens = coordinator
            .take_progress_token_receiver()?
            .context("missing progress token receiver")?;
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let state = coordinator.inner.state.lock();
            scope.spawn(|| {
                let result = (|| {
                    assert!(coordinator.publish_progress_frames_ready(0)?);
                    assert!(
                        tokens.is_empty(),
                        "frame readiness alone must not wake the owner"
                    );
                    assert!(coordinator.publish_progress_digest(digest, false)?);
                    assert_eq!(tokens.try_recv(), Ok(()));
                    Ok(())
                })();
                let _ = result_tx.send(result);
            });
            let result = result_rx.recv_timeout(Duration::from_secs(2));
            drop(state);

            result.context("ring publication waited for the Coordinator mutex")?
        })
    }

    #[test]
    fn progress_ring_readiness_rejects_events_after_poison_without_mutating_ticket() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let input = batch(10);
        let encoded = admit(&input, &coordinator)?.encode()?;
        let digest = encoded.digest_ready()?;
        let ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        let before = ring.snapshot(0)?;

        coordinator.poison();
        assert!(coordinator.publish_progress_digest(digest, false).is_err());
        assert!(coordinator.publish_progress_frames_ready(0).is_err());
        assert_eq!(ring.snapshot(0)?, before);

        Ok(())
    }

    #[test]
    fn progress_ring_readiness_can_drain_admitted_ticket_after_close() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let input = batch(10);
        let encoded = admit(&input, &coordinator)?.encode()?;
        let digest = encoded.digest_ready()?;

        coordinator.close();
        assert!(coordinator.publish_progress_digest(digest, false)?);
        assert!(coordinator.publish_progress_frames_ready(0)?);
        let payload = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .snapshot(0)?;
        assert_eq!(payload.digest_ready, Some(digest));
        assert!(payload.frames_ready);

        Ok(())
    }

    #[test]
    fn progress_ring_late_digest_wakes_after_consumed_data_token_and_scan() -> Result<()> {
        check_progress_late_digest_wake(WalV8ProgressMode::Cursor)
    }

    fn check_progress_late_digest_wake(mode: WalV8ProgressMode) -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, mode)?;
        let receiver = coordinator
            .take_progress_token_receiver()?
            .context("missing progress token")?;
        let input = batch(10);
        let mut encoded = admit(&input, &coordinator)?.encode()?;
        let digest = encoded.digest_ready()?;
        coordinator.publish_progress_frames_ready(0)?;

        let mut driver = coordinator
            .acquire_durability_driver(1)?
            .context("missing durability driver")?;

        // Observe the missing digest before the DATA completion arrives.
        consume_progress_token_before_scan(&receiver);
        assert!(
            coordinator
                .advance_and_snapshot(&driver, &[], false)?
                .ready_prefix
                .is_empty()
        );
        assert_eq!(coordinator.inner.state.lock().folded_ticket_end, 0);

        // A DATA completion is queued and its only token is consumed. The
        // owner applies the completion but still cannot advance without the
        // digest. No later DATA event will arrive to wake it.
        let (completion_tx, completion_rx) = crossbeam_channel::bounded(1);
        completion_tx.send(WalV8CompletedGroupEvent {
            group_id: 1,
            tickets: 0..1,
            write_count: 1,
            write_bytes: u64::try_from(WAL_V8_FRAME_LEN)?,
            #[cfg(feature = "bench")]
            completion_observed_at: None,
        })?;
        coordinator.notify_completion_queued();
        consume_progress_token_before_scan(&receiver);
        let event = [completion_rx.try_recv()?];
        assert!(
            coordinator
                .advance_and_snapshot(&driver, &event, false)?
                .ready_prefix
                .is_empty()
        );
        assert!(coordinator.inner.state.lock().tickets[0].data_complete);

        // Publish after the scan and before the driver parks. The persistent
        // token must carry this wake into the subsequent receive.
        coordinator.publish_progress_digest(digest, false)?;
        assert_eq!(
            wait_for_progress_token(&receiver, Duration::from_secs(1)),
            WalV8ProgressTokenWait::Signaled
        );
        consume_progress_token_before_scan(&receiver);
        assert_eq!(
            coordinator
                .advance_and_snapshot(&driver, &[], false)?
                .ready_prefix
                .len(),
            1
        );
        assert_eq!(coordinator.inner.state.lock().folded_ticket_end, 1);

        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        driver.complete();
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn progress_ring_handoff_scans_completion_queued_after_previous_scan() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 0)?;
        let first_input = batch(10);
        let second_input = batch(20);
        let mut first = admit(&first_input, &coordinator)?.encode()?;
        let mut second = admit(&second_input, &coordinator)?.encode()?;
        for encoded in [&first, &second] {
            coordinator.mark_digest_ready(encoded.digest_ready()?)?;
            coordinator.publish_progress_frames_ready(encoded.assignment().segment_ticket)?;
        }

        let tokens = coordinator
            .take_progress_token_receiver()?
            .context("missing progress token")?;
        let (completion_tx, completion_rx) = crossbeam_channel::bounded(2);
        let first_event = WalV8CompletedGroupEvent {
            group_id: 1,
            tickets: 0..1,
            write_count: 1,
            write_bytes: u64::try_from(WAL_V8_FRAME_LEN)?,
            #[cfg(feature = "bench")]
            completion_observed_at: None,
        };
        completion_tx.send(first_event)?;
        coordinator.notify_completion_queued();

        let mut first_driver = coordinator
            .acquire_durability_driver(1)?
            .context("missing first durability driver")?;
        consume_progress_token_before_scan(&tokens);
        let first_event = [completion_rx.try_recv()?];
        assert_eq!(
            coordinator
                .advance_and_snapshot(&first_driver, &first_event, false)?
                .ready_prefix
                .len(),
            1
        );
        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        first.disarm_after_durable();
        first_driver.complete();

        // This completion arrives after the previous owner's final scan but
        // before its handoff. The queued event and pending token must be seen
        // by the next owner without another publication.
        completion_tx.send(WalV8CompletedGroupEvent {
            group_id: 2,
            tickets: 1..2,
            write_count: 1,
            write_bytes: u64::try_from(WAL_V8_FRAME_LEN)?,
            #[cfg(feature = "bench")]
            completion_observed_at: None,
        })?;
        coordinator.notify_completion_queued();
        drop(first_driver);

        let mut second_driver = coordinator
            .acquire_durability_driver(2)?
            .context("missing handed-off durability driver")?;
        consume_progress_token_before_scan(&tokens);
        let second_event = [completion_rx.try_recv()?];
        assert_eq!(
            coordinator
                .advance_and_snapshot(&second_driver, &second_event, false)?
                .ready_prefix
                .len(),
            1
        );
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        second.disarm_after_durable();
        second_driver.complete();

        Ok(())
    }

    #[test]
    fn progress_ring_close_and_poison_leave_persistent_wake_tokens() -> Result<()> {
        for poison in [false, true] {
            let coordinator = coordinator_with_progress_ring(1, 0)?;
            let receiver = coordinator
                .take_progress_token_receiver()?
                .context("missing progress token")?;
            let waiting_coordinator = coordinator.clone();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let waiter = std::thread::spawn(move || {
                // The parent may signal before this receive parks. The
                // capacity-one token must preserve that wake until consumed.
                ready_tx.send(()).expect("parent remains connected");
                wait_for_progress_token(&receiver, Duration::from_secs(5))
            });
            ready_rx.recv_timeout(Duration::from_secs(1))?;

            if poison {
                waiting_coordinator.poison();
            } else {
                waiting_coordinator.close();
            }

            assert_eq!(
                waiter.join().expect("progress-token waiter panicked"),
                WalV8ProgressTokenWait::Signaled
            );
        }

        Ok(())
    }

    #[test]
    fn progress_ring_concurrent_publishers_coalesce_to_one_persistent_token() -> Result<()> {
        const PUBLISHERS: usize = 8;
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let receiver = coordinator
            .take_progress_token_receiver()?
            .context("missing progress token")?;
        let start = Arc::new(std::sync::Barrier::new(PUBLISHERS + 1));

        std::thread::scope(|scope| {
            for _ in 0..PUBLISHERS {
                let coordinator = coordinator.clone();
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..32 {
                        coordinator.signal_progress_token();
                    }
                });
            }
            start.wait();
        });

        assert_eq!(
            wait_for_progress_token(&receiver, Duration::from_secs(1)),
            WalV8ProgressTokenWait::Signaled
        );
        assert_eq!(
            wait_for_progress_token(&receiver, Duration::from_millis(10)),
            WalV8ProgressTokenWait::TimedOut
        );

        Ok(())
    }

    #[test]
    fn progress_ring_rejects_bad_digest_length_without_mutating_cell() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 0)?;
        let point_batch = batch(10);
        let encoded = admit(&point_batch, &coordinator)?.encode()?;
        let mut digest = encoded.digest_ready()?;
        digest.logical_batch_bytes = digest.logical_batch_bytes.saturating_add(1);
        let before = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .snapshot(0)?;

        let progress_ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        assert!(progress_ring.publish_digest(digest).is_err());
        assert_eq!(progress_ring.snapshot(0)?, before);
        assert!(coordinator.mark_digest_ready(digest).is_err());
        assert!(coordinator.is_poisoned());
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(0)?,
            before
        );
        Ok(())
    }

    #[test]
    fn progress_ring_rejects_foreign_incarnation_without_mutating_cell() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 0)?;
        let point_batch = batch(10);
        let encoded = admit(&point_batch, &coordinator)?.encode()?;
        let mut digest = encoded.digest_ready()?;
        digest.segment_incarnation[0] ^= 0xff;
        let before = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .snapshot(0)?;

        let progress_ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        assert!(progress_ring.publish_digest(digest).is_err());
        assert_eq!(progress_ring.snapshot(0)?, before);
        assert!(coordinator.mark_digest_ready(digest).is_err());
        assert!(coordinator.is_poisoned());
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(0)?,
            before
        );
        Ok(())
    }

    #[test]
    fn progress_ring_accepts_identical_digest_retry_after_fold() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let point_batch = batch(10);
        let encoded = admit(&point_batch, &coordinator)?.encode()?;
        let digest = encoded.digest_ready()?;

        coordinator.mark_digest_ready(digest)?;
        let mut driver = coordinator
            .acquire_durability_driver(1)?
            .context("test driver was not elected")?;
        coordinator.advance_and_snapshot(&driver, &[], false)?;
        let folded_prefix = coordinator.inner.state.lock().folded_ticket_end;
        assert_eq!(folded_prefix, 1);
        coordinator.mark_digest_ready(digest)?;
        assert_eq!(
            coordinator.inner.state.lock().folded_ticket_end,
            folded_prefix
        );
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(0)?
                .digest_ready,
            Some(digest)
        );
        driver.complete();

        Ok(())
    }

    #[test]
    fn progress_ring_conflicting_digest_after_fold_preserves_first_value() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let point_batch = batch(10);
        let encoded = admit(&point_batch, &coordinator)?.encode()?;
        let digest = encoded.digest_ready()?;
        coordinator.mark_digest_ready(digest)?;
        let before = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .snapshot(0)?;
        let mut conflicting = digest;
        conflicting.batch_digest[0] ^= 0xff;

        assert!(coordinator.mark_digest_ready(conflicting).is_err());
        assert!(coordinator.is_poisoned());
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(0)?,
            before
        );

        Ok(())
    }

    #[test]
    fn progress_ring_stale_digest_after_slot_reuse_does_not_change_new_ticket() -> Result<()> {
        check_progress_stale_digest(WalV8ProgressMode::Cursor)
    }

    fn check_progress_stale_digest(mode: WalV8ProgressMode) -> Result<()> {
        let coordinator = coordinator_with_progress_mode(1, 0, mode)?;
        let first_batch = batch(10);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let stale_digest = first.digest_ready()?;
        mark_producer_ready(&mut first, &coordinator, true)?;
        let mut driver = coordinator
            .acquire_durability_driver(1)?
            .context("test driver was not elected")?;
        coordinator.advance_and_snapshot(&driver, &[], false)?;
        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        driver.complete();
        first.disarm_after_durable();

        let second_batch = batch(20);
        let second = admit(&second_batch, &coordinator)?;
        let before = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring")
            .snapshot(1)?;
        let progress_ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        assert!(
            progress_ring
                .mark_frames_ready(
                    stale_digest.segment_incarnation,
                    stale_digest.segment_ticket
                )
                .is_err()
        );
        assert_eq!(progress_ring.snapshot(1)?, before);
        assert!(coordinator.mark_digest_ready(stale_digest).is_err());
        assert!(coordinator.is_poisoned());
        assert_eq!(
            coordinator
                .inner
                .progress_ring
                .as_ref()
                .expect("test enables progress ring")
                .snapshot(1)?,
            before
        );
        drop(second);

        Ok(())
    }

    #[test]
    fn progress_ring_retires_prefix_atomically_after_validating_every_cell() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 0)?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let second = admit(&second_batch, &coordinator)?;
        let first_digest = first.digest_ready()?;
        coordinator.mark_digest_ready(first_digest)?;
        let mut first_frames = vec![0; first.frame_buffer_bytes()];
        first.encode_data_frames_into(&mut first_frames)?;
        coordinator.mark_frames_ready(0)?;

        let ring = coordinator
            .inner
            .progress_ring
            .as_ref()
            .expect("test enables progress ring");
        let first_before = ring.snapshot(0)?;
        let second_before = ring.snapshot(1)?;
        assert!(ring.retire_prefix(0, 2).is_err());
        assert_eq!(ring.snapshot(0)?, first_before);
        assert_eq!(ring.snapshot(1)?, second_before);
        drop(second);

        Ok(())
    }

    #[test]
    fn progress_ring_completion_batch_applies_multi_ticket_group_for_active_driver() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(2, 0)?;
        #[cfg(feature = "bench")]
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        #[cfg(feature = "bench")]
        coordinator.set_hol_diagnostics_profile(Arc::clone(&profile));
        let first_batch = batch(10);
        let second_batch = batch(20);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let mut second = admit(&second_batch, &coordinator)?.encode()?;
        let first_assignment = first.assignment();

        coordinator.mark_digest_ready(first.digest_ready()?)?;
        coordinator.mark_digest_ready(second.digest_ready()?)?;
        coordinator.mark_frames_ready(first_assignment.segment_ticket)?;
        coordinator.mark_frames_ready(second.assignment().segment_ticket)?;

        let mut driver = coordinator
            .acquire_durability_driver(2)?
            .context("test driver was not elected")?;
        let change_generation = coordinator.change_generation();
        let durability_generation = coordinator.durability_generation();
        #[cfg(feature = "bench")]
        let completion_observed_at = {
            assert!(coordinator.capture_data_completion_time().is_none());
            coordinator.set_data_completion_timestamps_enabled(true);
            coordinator.capture_data_completion_time()
        };
        let events = [WalV8CompletedGroupEvent {
            group_id: 7,
            tickets: 0..2,
            write_count: 2,
            write_bytes: u64::try_from(2 * WAL_V8_FRAME_LEN)?,
            #[cfg(feature = "bench")]
            completion_observed_at,
        }];
        let snapshot =
            coordinator.advance_and_snapshot(&driver, &events, cfg!(feature = "bench"))?;
        assert_eq!(snapshot.ready_prefix.len(), 2);
        assert_eq!(coordinator.change_generation(), change_generation);
        assert_eq!(coordinator.durability_generation(), durability_generation);
        #[cfg(feature = "bench")]
        {
            assert!(completion_observed_at.is_some());
            let WalV8ReadyPrefix::Tickets(tickets) = snapshot.ready_prefix else {
                anyhow::bail!("detailed readiness snapshot did not include its tickets");
            };
            assert!(
                tickets
                    .iter()
                    .all(|ticket| ticket.data_completed_at == completion_observed_at)
            );
            assert_eq!(snapshot.frames_ready_prefix_count, 2);
            assert_eq!(snapshot.data_complete_prefix_count, 2);
            let fold_cpu_ns = coordinator.inner.state.lock().ordered_link_hash_cpu_ns;
            assert_eq!(
                profile.snapshot().wal_v8_ordered_link_hash_cpu_ns,
                fold_cpu_ns
            );
            coordinator.advance_and_snapshot(&driver, &[], false)?;
            let measured = profile.snapshot();
            assert_eq!(measured.wal_v8_ordered_link_hash_cpu_ns, fold_cpu_ns);
            assert_eq!(measured.wal_v8_cpu_timing_sampled_tickets, 0);
        }

        let reservation = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(reservation)?;
        driver.complete();
        first.disarm_after_durable();
        second.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn progress_ring_completion_batch_validates_entire_group_before_mutation() -> Result<()> {
        check_progress_group_atomicity(WalV8ProgressMode::Cursor, false)
    }

    fn check_progress_group_atomicity(
        mode: WalV8ProgressMode,
        partial_duplicate: bool,
    ) -> Result<()> {
        let coordinator = coordinator_with_progress_mode(2, 0, mode)?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let first = admit(&first_batch, &coordinator)?.encode()?;
        let second = admit(&second_batch, &coordinator)?.encode()?;
        let first_assignment = first.assignment();
        coordinator.mark_digest_ready(first.digest_ready()?)?;
        coordinator.mark_digest_ready(second.digest_ready()?)?;
        coordinator.mark_frames_ready(first_assignment.segment_ticket)?;

        #[cfg(feature = "bench")]
        {
            coordinator.mark_data_write_started(first_assignment.segment_ticket)?;
            let snapshot = coordinator.frontier_ready_snapshot_with_monotonic_ns();
            assert_eq!(snapshot.frames_ready_prefix_count, 1);
            assert_eq!(snapshot.data_complete_prefix_count, 0);
            let blocked = snapshot
                .next_not_ready
                .context("missing readiness blocker")?;
            assert_eq!(Some(blocked), snapshot.data_hol_blocker);
            assert!(blocked.frames_ready);
            assert!(blocked.data_write_elapsed_ns.is_some());
        }

        let driver = coordinator
            .acquire_durability_driver(2)?
            .context("test driver was not elected")?;
        if partial_duplicate {
            coordinator.mark_frames_ready(second.assignment().segment_ticket)?;
            coordinator.advance_and_snapshot(
                &driver,
                &[WalV8CompletedGroupEvent {
                    group_id: 8,
                    tickets: 0..1,
                    write_count: 1,
                    write_bytes: FRAME_LEN_U64,
                    #[cfg(feature = "bench")]
                    completion_observed_at: None,
                }],
                false,
            )?;
        }
        let events = [WalV8CompletedGroupEvent {
            group_id: 9,
            tickets: 0..2,
            write_count: 2,
            write_bytes: u64::try_from(2 * WAL_V8_FRAME_LEN)?,
            #[cfg(feature = "bench")]
            completion_observed_at: None,
        }];
        assert!(
            coordinator
                .advance_and_snapshot(&driver, &events, false)
                .is_err()
        );
        let state = coordinator.inner.state.lock();
        assert_eq!(state.tickets[0].data_complete, partial_duplicate);
        assert!(!state.tickets[1].data_complete);
        drop(state);
        assert!(coordinator.is_poisoned());
        drop(driver);
        drop(first);
        drop(second);

        Ok(())
    }

    #[test]
    fn progress_ring_completion_batch_rejects_a_foreign_driver() -> Result<()> {
        let coordinator = coordinator_with_progress_ring(1, 0)?;
        let foreign = coordinator_with_progress_ring(1, 0)?;
        let target_batch = batch(10);
        let encoded = admit(&target_batch, &coordinator)?.encode()?;
        let assignment = encoded.assignment();
        coordinator.mark_digest_ready(encoded.digest_ready()?)?;
        coordinator.mark_frames_ready(assignment.segment_ticket)?;

        let foreign_batch = batch(10);
        let foreign_encoded = admit(&foreign_batch, &foreign)?.encode()?;

        let target_driver = coordinator
            .acquire_durability_driver(1)?
            .context("target test driver was not elected")?;
        let foreign_driver = foreign
            .acquire_durability_driver(1)?
            .context("foreign test driver was not elected")?;
        let events = [WalV8CompletedGroupEvent {
            group_id: 1,
            tickets: 0..1,
            write_count: 1,
            write_bytes: WAL_V8_FRAME_LEN as u64,
            #[cfg(feature = "bench")]
            completion_observed_at: None,
        }];
        assert!(
            coordinator
                .advance_and_snapshot(&foreign_driver, &events, false)
                .is_err()
        );
        assert!(!coordinator.inner.state.lock().tickets[0].data_complete);
        assert!(!coordinator.is_poisoned());

        drop(foreign_driver);
        drop(foreign_encoded);
        drop(target_driver);
        drop(encoded);
        assert!(coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn admission_is_visible_before_encoding_and_the_open_window_grows() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let first_admitted = admit(&first, &coordinator)?;
        assert_eq!(coordinator.admitted_window().len(), 1);
        assert!(!coordinator.admitted_window()[0].frames_ready);
        assert_eq!(coordinator.admitted_window()[0].digest_ready, None);

        let second_admitted = admit(&second, &coordinator)?;
        let window = coordinator.admitted_window();
        assert_eq!(
            window
                .iter()
                .map(|ticket| ticket.assignment.segment_ticket)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(window.iter().all(|ticket| !ticket.frames_ready));
        drop(first_admitted);
        drop(second_admitted);
        assert!(coordinator.is_poisoned());

        Ok(())
    }

    fn wait_for_frontier_coalescer(coordinator: &WalV8ParallelCoordinator) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);
        while coordinator.inner.state.lock().frontier_coalesce_waiters == 0 {
            ensure!(
                Instant::now() < deadline,
                "frontier coalescer did not start waiting"
            );
            std::thread::yield_now();
        }

        Ok(())
    }

    #[test]
    fn frontier_coalescing_skips_lone_producers_and_bounds_unfinished_successors() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        assert!(!coordinator.coalesce_frontier_ready(Duration::from_secs(1)));
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        assert!(!coordinator.coalesce_frontier_ready(Duration::from_secs(1)));

        let second = batch(20);
        let _unfinished = admit(&second, &coordinator)?;
        assert!(coordinator.coalesce_frontier_ready(Duration::from_millis(1)));
        assert_eq!(coordinator.frontier_ready_prefix().len(), 1);
        assert_eq!(coordinator.durable_ticket_end(), 0);

        Ok(())
    }

    #[test]
    fn frontier_coalescing_joins_completed_successors_without_publishing_durability() -> Result<()>
    {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        mark_producer_ready(&mut second_encoded, &coordinator, false)?;

        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = coordinator.clone();
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiting.coalesce_frontier_ready(Duration::from_secs(2)));
        });
        wait_for_frontier_coalescer(&coordinator)?;
        coordinator.mark_data_complete(1)?;
        assert!(rx.recv_timeout(Duration::from_secs(1))?);
        waiter.join().expect("frontier coalescer panicked");
        assert_eq!(coordinator.frontier_ready_prefix().len(), 2);
        assert_eq!(coordinator.durable_ticket_end(), 0);
        assert!(!coordinator.coalesce_frontier_ready(Duration::from_secs(1)));

        Ok(())
    }

    #[test]
    fn close_and_poison_wake_frontier_coalescing() -> Result<()> {
        for poison in [false, true] {
            let coordinator = coordinator(8)?;
            let first = batch(10);
            let second = batch(20);
            let mut first_encoded = admit(&first, &coordinator)?.encode()?;
            let _unfinished = admit(&second, &coordinator)?;
            mark_producer_ready(&mut first_encoded, &coordinator, true)?;
            let (tx, rx) = std::sync::mpsc::channel();
            let waiting = coordinator.clone();
            let waiter = std::thread::spawn(move || {
                let _ = tx.send(waiting.coalesce_frontier_ready(Duration::from_secs(2)));
            });
            wait_for_frontier_coalescer(&coordinator)?;
            if poison {
                coordinator.poison();
            } else {
                coordinator.close();
            }
            assert!(rx.recv_timeout(Duration::from_secs(1))?);
            waiter.join().expect("frontier coalescer panicked");
            assert_eq!(coordinator.durable_ticket_end(), 0);
        }

        Ok(())
    }

    #[test]
    fn later_ticket_readiness_waits_for_a_contiguous_head_prefix() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let first_admitted = admit(&first, &coordinator)?;
        let second_admitted = admit(&second, &coordinator)?;
        let mut first_encoded = first_admitted.encode()?;
        let mut second_encoded = second_admitted.encode()?;
        let first_digest = first_encoded.digest_ready()?;
        let second_digest = second_encoded.digest_ready()?;
        coordinator.mark_digest_ready(second_digest)?;

        assert!(coordinator.frame_ready_prefix().is_empty());
        assert!(coordinator.frontier_ready_prefix().is_empty());
        assert_eq!(coordinator.prefix_digest_at(2), None);

        coordinator.mark_digest_ready(first_digest)?;
        let prefix_one = super::super::digest::advance_prefix(
            &header().initial_prefix_digest()?,
            0,
            first_digest.logical_batch_bytes,
            &first_digest.batch_digest,
        );
        let prefix_two = super::super::digest::advance_prefix(
            &prefix_one,
            1,
            second_digest.logical_batch_bytes,
            &second_digest.batch_digest,
        );
        assert_eq!(coordinator.prefix_digest_at(1), Some(prefix_one));
        assert_eq!(coordinator.prefix_digest_at(2), Some(prefix_two));

        mark_producer_ready(&mut second_encoded, &coordinator, true)?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        assert_eq!(
            coordinator
                .frontier_ready_prefix()
                .iter()
                .map(|ticket| ticket.assignment.segment_ticket)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let reservation = coordinator.reserve_frontier(2)?;
        assert_eq!(reservation.frame_offset, DATA_START + 2 * FRAME_LEN_U64);
        assert_eq!(reservation.prefix_digest, prefix_two);
        coordinator.publish_durable_frontier(reservation)?;
        first_encoded.disarm_after_durable();
        second_encoded.disarm_after_durable();
        assert_eq!(coordinator.durable_ticket_end(), 2);
        assert_eq!(coordinator.durable_prefix_digest(), prefix_two);
        assert_eq!(coordinator.prefix_digest_at(2), Some(prefix_two));
        assert_eq!(coordinator.prefix_digest_at(1), None);
        assert_eq!(
            coordinator.last_frontier_end(),
            reservation.frame_offset + FRAME_LEN_U64
        );
        assert!(coordinator.admitted_window().is_empty());

        Ok(())
    }

    #[test]
    fn digest_frames_and_data_completion_are_independent_events() -> Result<()> {
        let coordinator = coordinator(8)?;
        let batch = batch(10);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;
        let assignment = encoded.assignment();

        coordinator.mark_digest_ready(encoded.digest_ready()?)?;
        assert!(coordinator.frame_ready_prefix().is_empty());
        assert!(coordinator.frontier_ready_prefix().is_empty());

        let mut frames = vec![0; encoded.frame_buffer_bytes()];
        encoded.encode_data_frames_into(&mut frames)?;
        coordinator.mark_frames_ready(assignment.segment_ticket)?;
        assert_eq!(coordinator.frame_ready_prefix().len(), 1);
        assert!(coordinator.frontier_ready_prefix().is_empty());

        coordinator.mark_data_complete(assignment.segment_ticket)?;
        assert_eq!(coordinator.frontier_ready_prefix().len(), 1);
        let reservation = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(reservation)?;
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    #[cfg(feature = "bench")]
    fn frontier_snapshot_reports_the_first_blocking_ticket_stage() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let mut first = admit(&first_batch, &coordinator)?.encode()?;
        let mut second = admit(&second_batch, &coordinator)?.encode()?;
        let preparation = coordinator.reserve_preparation(640)?;

        mark_producer_ready(&mut first, &coordinator, true)?;
        coordinator.mark_digest_ready(second.digest_ready()?)?;

        let snapshot = coordinator.frontier_ready_snapshot();
        assert_eq!(snapshot.admitted_ticket_count, 2);
        assert_eq!(snapshot.frames_ready_prefix_count, 1);
        assert_eq!(snapshot.data_complete_prefix_count, 1);
        assert_eq!(snapshot.reserved_preparation_bytes, 640);
        assert_eq!(snapshot.ready_prefix.len(), 1);
        let blocked = snapshot
            .next_not_ready
            .expect("second admitted ticket blocks the prefix");
        assert!(!blocked.frames_ready);
        assert!(blocked.prefix_digest_after.is_some());
        assert!(!blocked.data_complete);
        assert_eq!(blocked.data_write_elapsed_ns, None);

        let mut second_frames = vec![0; second.frame_buffer_bytes()];
        second.encode_data_frames_into(&mut second_frames)?;
        coordinator.mark_frames_ready(1)?;
        let snapshot = coordinator.frontier_ready_snapshot();
        assert_eq!(snapshot.frames_ready_prefix_count, 2);
        assert_eq!(snapshot.data_complete_prefix_count, 1);
        let blocked = snapshot
            .next_not_ready
            .expect("second ticket still awaits DATA completion");
        assert!(blocked.frames_ready);
        assert!(blocked.prefix_digest_after.is_some());
        assert!(!blocked.data_complete);
        assert_eq!(blocked.data_write_elapsed_ns, None);

        coordinator.mark_data_write_started(1)?;
        let snapshot = coordinator.frontier_ready_snapshot();
        let blocked = snapshot
            .next_not_ready
            .expect("second ticket still awaits DATA completion");
        assert!(blocked.data_write_elapsed_ns.is_some());

        coordinator.mark_data_complete(1)?;
        let snapshot = coordinator.frontier_ready_snapshot();
        assert_eq!(snapshot.ready_prefix.len(), 2);
        assert_eq!(snapshot.frames_ready_prefix_count, 2);
        assert_eq!(snapshot.data_complete_prefix_count, 2);
        assert!(snapshot.next_not_ready.is_none());

        let reservation = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(reservation)?;
        first.disarm_after_durable();
        second.disarm_after_durable();
        drop(preparation);

        Ok(())
    }

    #[test]
    fn next_frontier_waits_for_tickets_before_previous_marker() -> Result<()> {
        for mode in [WalV8ProgressMode::Eager, WalV8ProgressMode::Cursor] {
            let coordinator = coordinator_with_progress_mode(3, 0, mode)?;
            let mut first = admit(&batch(10), &coordinator)?.encode()?;
            let mut second = admit(&batch(20), &coordinator)?.encode()?;
            let mut third = admit(&batch(30), &coordinator)?.encode()?;
            let mut driver = if mode == WalV8ProgressMode::Cursor {
                Some(
                    coordinator
                        .acquire_durability_driver(3)?
                        .context("missing cursor durability driver")?,
                )
            } else {
                None
            };

            let mark_frames_ready = |encoded: &mut super::super::prepare::EncodedWalV8Batch<
                WalV8ParallelAdmissionGuard,
            >,
                                     coordinator: &WalV8ParallelCoordinator|
             -> Result<()> {
                let assignment = encoded.assignment();
                coordinator.mark_digest_ready(encoded.digest_ready()?)?;
                let mut frames = vec![0; encoded.frame_buffer_bytes()];
                encoded.encode_data_frames_into(&mut frames)?;
                coordinator.mark_frames_ready(assignment.segment_ticket)
            };
            let complete_cursor_group = |driver: &WalV8DurabilityDriver,
                                         ticket: u64|
             -> Result<WalV8FrontierReadySnapshot> {
                coordinator.advance_and_snapshot(
                    driver,
                    &[WalV8CompletedGroupEvent {
                        group_id: ticket + 1,
                        tickets: ticket..ticket + 1,
                        write_count: 1,
                        write_bytes: FRAME_LEN_U64,
                        #[cfg(feature = "bench")]
                        completion_observed_at: None,
                    }],
                    false,
                )
            };

            mark_frames_ready(&mut first, &coordinator)?;
            let first_snapshot = if let Some(driver) = &mut driver {
                complete_cursor_group(driver, 0)?
            } else {
                coordinator.mark_data_complete(0)?;
                coordinator.frontier_ready_snapshot()
            };
            assert_eq!(first_snapshot.ready_prefix.len(), 1);

            let first_frontier = coordinator.reserve_frontier(1)?;
            assert_eq!(first_frontier.preceding_ticket_end, 3);
            coordinator.publish_durable_frontier(first_frontier)?;
            first.disarm_after_durable();

            mark_frames_ready(&mut second, &coordinator)?;
            let partial_snapshot = if let Some(driver) = &mut driver {
                complete_cursor_group(driver, 1)?
            } else {
                coordinator.mark_data_complete(1)?;
                coordinator.frontier_ready_snapshot()
            };
            assert!(
                partial_snapshot.ready_prefix.is_empty(),
                "frontier must not advance past only part of the pre-marker DATA window"
            );
            assert!(coordinator.reserve_frontier(2).is_err());
            assert!(!coordinator.is_poisoned());

            mark_frames_ready(&mut third, &coordinator)?;
            let complete_snapshot = if let Some(driver) = &mut driver {
                complete_cursor_group(driver, 2)?
            } else {
                coordinator.mark_data_complete(2)?;
                coordinator.frontier_ready_snapshot()
            };
            assert_eq!(complete_snapshot.ready_prefix.len(), 2);

            let second_frontier = coordinator.reserve_frontier(3)?;
            coordinator.publish_durable_frontier(second_frontier)?;
            second.disarm_after_durable();
            third.disarm_after_durable();
            if let Some(mut driver) = driver {
                driver.complete();
            }
        }

        Ok(())
    }

    #[test]
    fn ordered_fold_resumes_from_recovered_durable_prefix() -> Result<()> {
        let wal_header = header();
        let recovered_prefix = [0xa7; 32];
        let coordinator = WalV8ParallelCoordinator::new(
            wal_header,
            WalV8ParallelConfig {
                next_ticket: 5,
                next_data_offset: DATA_START + 4 * FRAME_LEN_U64,
                last_commit_ts: 50,
                last_recorded_at: Some(RecordedAt { secs: 50, nanos: 9 }),
                durable_prefix_digest: recovered_prefix,
                max_inflight_tickets: 8,
                max_inflight_bytes: 64 * 1024 * 1024,
                max_file_size: MAX_WAL_FILE_SIZE,
                progress_mode: WalV8ProgressMode::Eager,
            },
        )?;
        let batch = batch(60);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;
        let digest_ready = encoded.digest_ready()?;
        assert_eq!(digest_ready.segment_ticket, 5);

        coordinator.mark_digest_ready(digest_ready)?;
        let expected_prefix = super::super::digest::advance_prefix(
            &recovered_prefix,
            5,
            digest_ready.logical_batch_bytes,
            &digest_ready.batch_digest,
        );
        assert_eq!(coordinator.durable_prefix_digest(), recovered_prefix);
        assert_eq!(coordinator.prefix_digest_at(5), Some(recovered_prefix));
        assert_eq!(coordinator.prefix_digest_at(6), Some(expected_prefix));

        mark_producer_ready(&mut encoded, &coordinator, true)?;
        let reservation = coordinator.reserve_frontier(6)?;
        assert_eq!(reservation.prefix_digest, expected_prefix);
        coordinator.publish_durable_frontier(reservation)?;
        encoded.disarm_after_durable();
        assert_eq!(coordinator.durable_prefix_digest(), expected_prefix);

        Ok(())
    }

    #[test]
    fn dropped_admission_poison_stops_the_ticket_window() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first_batch = batch(10);
        drop(admit(&first_batch, &coordinator)?);
        assert!(coordinator.is_poisoned());
        assert!(coordinator.admitted_window().iter().all(|ticket| {
            !ticket.frames_ready && ticket.digest_ready.is_none() && !ticket.data_complete
        }));
        assert!(admit(&batch(20), &coordinator).is_err());

        Ok(())
    }

    #[test]
    fn invalid_digest_identity_poisons_without_advancing_frontier() -> Result<()> {
        let coordinator = coordinator(8)?;
        let batch = batch(10);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;
        let mut invalid_digest = encoded.digest_ready()?;
        invalid_digest.segment_incarnation[0] ^= 1;

        assert!(coordinator.mark_digest_ready(invalid_digest).is_err());
        assert!(coordinator.is_poisoned());
        assert!(coordinator.frontier_ready_prefix().is_empty());
        assert!(coordinator.reserve_frontier(1).is_err());
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn data_completion_before_frame_readiness_poisons() -> Result<()> {
        let coordinator = coordinator(8)?;
        let batch = batch(10);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;

        assert!(coordinator.mark_data_complete(0).is_err());
        assert!(coordinator.is_poisoned());
        assert!(coordinator.frontier_ready_prefix().is_empty());
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn frontier_reservation_stays_after_speculative_data() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let third_batch = batch(30);
        let fourth_batch = batch(40);
        let first_admitted = admit(&first_batch, &coordinator)?;
        let second_admitted = admit(&second_batch, &coordinator)?;
        let third_admitted = admit(&third_batch, &coordinator)?;
        let mut first_encoded = first_admitted.encode()?;
        let mut second_encoded = second_admitted.encode()?;
        let mut third_encoded = third_admitted.encode()?;
        let first_digest = first_encoded.digest_ready()?;
        let second_digest = second_encoded.digest_ready()?;
        let third_digest = third_encoded.digest_ready()?;
        coordinator.mark_digest_ready(third_digest)?;
        coordinator.mark_digest_ready(second_digest)?;
        assert_eq!(coordinator.prefix_digest_at(3), None);
        coordinator.mark_digest_ready(first_digest)?;
        let prefix_one = super::super::digest::advance_prefix(
            &header().initial_prefix_digest()?,
            0,
            first_digest.logical_batch_bytes,
            &first_digest.batch_digest,
        );
        let prefix_two = super::super::digest::advance_prefix(
            &prefix_one,
            1,
            second_digest.logical_batch_bytes,
            &second_digest.batch_digest,
        );
        let prefix_three = super::super::digest::advance_prefix(
            &prefix_two,
            2,
            third_digest.logical_batch_bytes,
            &third_digest.batch_digest,
        );
        assert_eq!(coordinator.prefix_digest_at(1), Some(prefix_one));
        assert_eq!(coordinator.prefix_digest_at(2), Some(prefix_two));
        assert_eq!(coordinator.prefix_digest_at(3), Some(prefix_three));

        let mut first_frames = vec![0; first_encoded.frame_buffer_bytes()];
        first_encoded.encode_data_frames_into(&mut first_frames)?;
        coordinator.mark_frames_ready(0)?;
        coordinator.mark_data_complete(0)?;

        let first_frontier = coordinator.reserve_frontier(1)?;
        assert_eq!(first_frontier.frame_offset, DATA_START + 3 * FRAME_LEN_U64);
        assert_eq!(first_frontier.prefix_digest, prefix_one);
        let fourth_admitted = admit(&fourth_batch, &coordinator)?;
        let mut fourth_encoded = fourth_admitted.encode()?;
        assert_eq!(
            fourth_encoded.assignment().first_data_offset,
            first_frontier.frame_offset + FRAME_LEN_U64
        );

        coordinator.publish_durable_frontier(first_frontier)?;
        first_encoded.disarm_after_durable();
        assert_eq!(coordinator.durable_prefix_digest(), prefix_one);
        assert_eq!(coordinator.prefix_digest_at(1), Some(prefix_one));
        assert_eq!(coordinator.prefix_digest_at(2), Some(prefix_two));
        assert_eq!(coordinator.prefix_digest_at(3), Some(prefix_three));

        let fourth_digest = fourth_encoded.digest_ready()?;
        coordinator.mark_digest_ready(fourth_digest)?;
        let prefix_four = super::super::digest::advance_prefix(
            &prefix_three,
            3,
            fourth_digest.logical_batch_bytes,
            &fourth_digest.batch_digest,
        );
        assert_eq!(coordinator.prefix_digest_at(4), Some(prefix_four));

        mark_producer_ready(&mut second_encoded, &coordinator, true)?;
        mark_producer_ready(&mut third_encoded, &coordinator, true)?;
        mark_producer_ready(&mut fourth_encoded, &coordinator, true)?;
        assert_eq!(coordinator.frontier_ready_prefix().len(), 3);
        let second_frontier = coordinator.reserve_frontier(4)?;
        assert_eq!(second_frontier.prefix_digest, prefix_four);
        assert_eq!(
            second_frontier.frame_offset,
            fourth_encoded.assignment().first_data_offset + FRAME_LEN_U64
        );
        coordinator.publish_durable_frontier(second_frontier)?;
        second_encoded.disarm_after_durable();
        third_encoded.disarm_after_durable();
        fourth_encoded.disarm_after_durable();
        assert!(!coordinator.is_poisoned());
        assert_eq!(coordinator.durable_ticket_end(), 4);
        assert_eq!(coordinator.durable_prefix_digest(), prefix_four);

        Ok(())
    }

    #[test]
    fn frontier_space_covers_every_admitted_ticket() -> Result<()> {
        let coordinator =
            coordinator_with_limits(8, 64 * 1024 * 1024, DATA_START + 4 * FRAME_LEN_U64)?;
        let first = batch(10);
        let second = batch(20);
        let third = batch(30);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;

        // Both tickets must remain able to commit separately, even if the
        // second DATA write is not complete when the first FRONTIER is issued.
        assert!(admit(&third, &coordinator).is_err());
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        let first_frontier = coordinator.reserve_frontier(1)?;
        assert!(admit(&third, &coordinator).is_err());
        coordinator.publish_durable_frontier(first_frontier)?;
        first_encoded.disarm_after_durable();

        mark_producer_ready(&mut second_encoded, &coordinator, true)?;
        let second_frontier = coordinator.reserve_frontier(2)?;
        assert_eq!(
            second_frontier.frame_offset + FRAME_LEN_U64,
            DATA_START + 4 * FRAME_LEN_U64
        );
        coordinator.publish_durable_frontier(second_frontier)?;
        second_encoded.disarm_after_durable();
        assert_eq!(coordinator.durable_ticket_end(), 2);
        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn group_frontier_releases_unused_marker_space_after_durability() -> Result<()> {
        let coordinator =
            coordinator_with_limits(8, 64 * 1024 * 1024, DATA_START + 5 * FRAME_LEN_U64)?;
        let first = batch(10);
        let second = batch(20);
        let third = batch(30);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        assert!(admit(&third, &coordinator).is_err());

        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        mark_producer_ready(&mut second_encoded, &coordinator, true)?;
        let group_frontier = coordinator.reserve_frontier(2)?;
        // The unused marker reservation is retained until the pending group
        // has actually become durable.
        assert!(admit(&third, &coordinator).is_err());
        coordinator.publish_durable_frontier(group_frontier)?;
        first_encoded.disarm_after_durable();
        second_encoded.disarm_after_durable();

        let mut third_encoded = admit(&third, &coordinator)?.encode()?;
        mark_producer_ready(&mut third_encoded, &coordinator, true)?;
        let final_frontier = coordinator.reserve_frontier(3)?;
        coordinator.publish_durable_frontier(final_frontier)?;
        third_encoded.disarm_after_durable();
        assert_eq!(coordinator.durable_ticket_end(), 3);
        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn bounded_admission_waits_until_a_ticket_is_durable() -> Result<()> {
        use std::sync::mpsc;

        let coordinator = coordinator(1)?;
        let first = batch(10);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;

        let waiter_coordinator = coordinator.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (ticket_tx, ticket_rx) = mpsc::channel();
        let (durable_tx, durable_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || -> Result<()> {
            let second = batch(20);
            started_tx
                .send(())
                .context("notify test that the waiter is starting")?;
            let mut encoded = admit(&second, &waiter_coordinator)?.encode()?;
            let ticket = encoded.assignment().segment_ticket;
            mark_producer_ready(&mut encoded, &waiter_coordinator, true)?;
            ticket_tx
                .send(ticket)
                .context("publish second test ticket")?;
            durable_rx
                .recv()
                .context("wait for second test ticket durability")?;
            encoded.disarm_after_durable();

            Ok(())
        });

        started_rx
            .recv()
            .context("wait for admission waiter to start")?;
        let observed_waiter = std::time::Instant::now() + Duration::from_secs(1);
        while coordinator.ticket_admission_waiter_count() == 0
            && std::time::Instant::now() < observed_waiter
        {
            std::thread::yield_now();
        }
        assert_eq!(coordinator.ticket_admission_waiter_count(), 1);
        assert!(matches!(
            ticket_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let first_frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(first_frontier)?;
        first_encoded.disarm_after_durable();

        let second_ticket = ticket_rx
            .recv_timeout(Duration::from_secs(1))
            .context("wait for admission after durable capacity is released")?;
        assert_eq!(second_ticket, 1);
        let second_frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(second_frontier)?;
        durable_tx
            .send(())
            .context("release second test ticket guard")?;
        waiter
            .join()
            .map_err(|_| anyhow::anyhow!("admission waiter panicked"))??;
        assert_eq!(coordinator.durable_ticket_end(), 2);
        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn closing_wakes_an_admission_waiter() -> Result<()> {
        use std::sync::mpsc;

        let coordinator = coordinator(1)?;
        let first = batch(10);
        let _first_admitted = admit(&first, &coordinator)?;

        let waiter_coordinator = coordinator.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || -> Result<()> {
            let second = batch(20);
            started_tx
                .send(())
                .context("notify test that the waiter is starting")?;
            let result = admit(&second, &waiter_coordinator).map(|_| ());
            result_tx
                .send(result.map_err(|error| error.to_string()))
                .context("publish closed admission result")?;

            Ok(())
        });

        started_rx
            .recv()
            .context("wait for admission waiter to start")?;
        let observed_waiter = std::time::Instant::now() + Duration::from_secs(1);
        while coordinator.ticket_admission_waiter_count() == 0
            && std::time::Instant::now() < observed_waiter
        {
            std::thread::yield_now();
        }
        assert_eq!(coordinator.ticket_admission_waiter_count(), 1);

        coordinator.close();
        let error = result_rx
            .recv_timeout(Duration::from_secs(1))
            .context("wait for closed admission to wake its waiter")?
            .expect_err("closed ticket window unexpectedly admitted a ticket");
        assert!(error.contains("coordinator is closed"));
        waiter
            .join()
            .map_err(|_| anyhow::anyhow!("admission waiter panicked"))??;

        Ok(())
    }

    #[test]
    fn preparation_reservation_counts_against_admission_window() -> Result<()> {
        let mut batch = batch(10);
        batch.entries.push(WalEntry::Put {
            key: b"other".to_vec(),
            value: vec![0x3c; 24],
        });
        let preparation_bytes = PreparedWalV8Batch::<()>::preparation_buffer_bytes(
            &batch,
            crate::pitr::LIVE_WAL_V5_LIMITS,
        )?;
        assert!(preparation_bytes > 0);
        let prepared =
            PreparedWalV8Batch::<()>::preflight(&batch, crate::pitr::LIVE_WAL_V5_LIMITS, |_| {
                Ok(())
            })?;
        let ticket_bytes = u64::try_from(prepared.logical_batch_bytes())?
            .checked_add(u64::try_from(prepared.frame_buffer_bytes())?)
            .context("test ticket reservation overflows")?;
        drop(prepared);

        let byte_limit = ticket_bytes
            .checked_add(preparation_bytes)
            .and_then(|total| total.checked_sub(1))
            .context("test byte limit underflows")?;
        let coordinator = coordinator_with_byte_limit(8, byte_limit)?;
        let preparation = coordinator.reserve_preparation(preparation_bytes)?;

        assert!(admit(&batch, &coordinator).is_err());
        assert_eq!(coordinator.next_ticket(), 0);
        assert!(coordinator.admitted_window().is_empty());
        assert!(!coordinator.is_poisoned());

        drop(preparation);
        let admitted = admit(&batch, &coordinator)?;
        assert_eq!(coordinator.next_ticket(), 1);
        drop(admitted);
        assert!(coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn batch_larger_than_byte_window_is_rejected_as_permanent() -> Result<()> {
        let coordinator = coordinator_with_byte_limit(8, 1)?;
        let batch = batch(10);
        let error = match admit(&batch, &coordinator) {
            Ok(_) => anyhow::bail!("batch unexpectedly fit in the byte window"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("exceeds the configured byte window")
        );
        assert_eq!(coordinator.next_ticket(), 0);
        assert!(coordinator.admitted_window().is_empty());
        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn duplicate_readiness_is_idempotent_and_conflicts_poison() -> Result<()> {
        let coordinator = coordinator(8)?;
        let batch = batch(10);
        let mut encoded = admit(&batch, &coordinator)?.encode()?;
        let assignment = encoded.assignment();
        let digest = encoded.digest_ready()?;
        coordinator.mark_digest_ready(digest)?;
        coordinator.mark_digest_ready(digest)?;
        coordinator.mark_frames_ready(assignment.segment_ticket)?;
        coordinator.mark_frames_ready(assignment.segment_ticket)?;

        let mut conflicting = digest;
        conflicting.batch_digest[0] ^= 0x80;
        assert!(coordinator.mark_digest_ready(conflicting).is_err());
        assert!(coordinator.is_poisoned());
        assert!(coordinator.frontier_ready_prefix().is_empty());
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn closing_wakes_waiters_and_rejects_new_admissions() -> Result<()> {
        let coordinator = coordinator(8)?;
        let observed = coordinator.change_generation();
        let waiter = coordinator.clone();
        let thread =
            std::thread::spawn(move || waiter.wait_for_change(observed, Duration::from_secs(5)));
        coordinator.close();
        let wake = thread.join().expect("coordinator waiter panicked");
        assert!(wake.closed);
        assert!(!wake.poisoned);
        let batch = batch(10);
        assert!(admit(&batch, &coordinator).is_err());
        assert_eq!(coordinator.next_ticket(), 0);

        Ok(())
    }

    fn wait_for_durability_subscriber(coordinator: &WalV8ParallelCoordinator) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);
        while coordinator.inner.state.lock().durability_waiters == 0 {
            ensure!(
                Instant::now() < deadline,
                "durability subscriber did not start waiting"
            );
            std::thread::yield_now();
        }

        Ok(())
    }

    #[test]
    fn preparation_updates_do_not_release_durability_waiters() -> Result<()> {
        let coordinator = coordinator(8)?;
        let generation = coordinator.durability_generation();
        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiting.wait_for_durability_change(generation, Duration::from_secs(5)));
        });
        wait_for_durability_subscriber(&coordinator)?;

        let first = batch(10);
        let mut encoded = admit(&first, &coordinator)?.encode()?;
        mark_producer_ready(&mut encoded, &coordinator, false)?;
        assert!(coordinator.change_generation() > 0);
        assert_eq!(coordinator.durability_generation(), generation);
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        coordinator.mark_data_complete(0)?;
        let wake = rx.recv_timeout(Duration::from_secs(1))?;
        assert_ne!(wake.change_generation, generation);
        assert!(!wake.closed && !wake.poisoned);
        assert_eq!(coordinator.durable_ticket_end(), 0);
        waiter.join().expect("durability subscriber panicked");
        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn late_digest_fold_wakes_durability_waiters_after_data_completes() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        coordinator.mark_frames_ready(0)?;
        coordinator.mark_frames_ready(1)?;
        coordinator.mark_data_group_complete(0..2)?;
        assert!(coordinator.frontier_ready_prefix().is_empty());

        let generation = coordinator.durability_generation();
        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiting.wait_for_durability_change(generation, Duration::from_secs(5)));
        });
        wait_for_durability_subscriber(&coordinator)?;
        coordinator.mark_digest_ready(second_encoded.digest_ready()?)?;
        assert_eq!(coordinator.durability_generation(), generation);
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        coordinator.mark_digest_ready(first_encoded.digest_ready()?)?;
        let wake = rx.recv_timeout(Duration::from_secs(1))?;
        assert_ne!(wake.change_generation, generation);
        assert_eq!(coordinator.frontier_ready_prefix().len(), 2);
        assert_eq!(coordinator.durable_ticket_end(), 0);
        waiter.join().expect("late-digest subscriber panicked");
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        first_encoded.disarm_after_durable();
        second_encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn out_of_order_data_completion_keeps_durable_frontier_contiguous() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, false)?;
        mark_producer_ready(&mut second_encoded, &coordinator, false)?;

        coordinator.mark_data_group_complete(1..2)?;
        assert!(coordinator.frontier_ready_prefix().is_empty());
        assert_eq!(coordinator.durable_ticket_end(), 0);

        coordinator.mark_data_group_complete(0..1)?;
        assert_eq!(
            coordinator
                .frontier_ready_prefix()
                .iter()
                .map(|ticket| ticket.assignment.segment_ticket)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        first_encoded.disarm_after_durable();
        second_encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn frontier_reservation_does_not_replace_durable_publication_notification() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let mut encoded = admit(&first, &coordinator)?.encode()?;
        mark_producer_ready(&mut encoded, &coordinator, true)?;
        let generation = coordinator.durability_generation();
        let frontier = coordinator.reserve_frontier(1)?;
        assert_eq!(coordinator.durability_generation(), generation);
        assert_eq!(coordinator.durable_ticket_end(), 0);

        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiting.wait_for_durability_change(generation, Duration::from_secs(5)));
        });
        wait_for_durability_subscriber(&coordinator)?;
        coordinator.publish_durable_frontier(frontier)?;
        let wake = rx.recv_timeout(Duration::from_secs(1))?;
        assert_ne!(wake.change_generation, generation);
        assert_eq!(coordinator.durable_ticket_end(), 1);
        waiter
            .join()
            .expect("durable-publication subscriber panicked");
        encoded.disarm_after_durable();

        Ok(())
    }

    #[test]
    fn close_and_poison_wake_durability_subscribers() -> Result<()> {
        for poison in [false, true] {
            let coordinator = coordinator(8)?;
            let generation = coordinator.durability_generation();
            let waiting = coordinator.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let waiter = std::thread::spawn(move || {
                let _ =
                    tx.send(waiting.wait_for_durability_change(generation, Duration::from_secs(5)));
            });
            wait_for_durability_subscriber(&coordinator)?;
            if poison {
                coordinator.poison();
            } else {
                coordinator.close();
            }
            let wake = rx.recv_timeout(Duration::from_secs(1))?;
            assert_ne!(wake.change_generation, generation);
            assert_eq!(wake.poisoned, poison);
            assert_eq!(wake.closed, !poison);
            waiter.join().expect("lifecycle subscriber panicked");
        }

        Ok(())
    }

    #[test]
    fn late_digest_wakes_frontier_coalescing_without_a_data_completion_event() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        coordinator.mark_frames_ready(1)?;
        coordinator.mark_data_complete(1)?;

        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiting.coalesce_frontier_ready(Duration::from_secs(5)));
        });
        wait_for_frontier_coalescer(&coordinator)?;
        coordinator.mark_digest_ready(second_encoded.digest_ready()?)?;
        assert!(rx.recv_timeout(Duration::from_secs(1))?);
        assert_eq!(coordinator.frontier_ready_prefix().len(), 2);
        waiter.join().expect("late-digest coalescer panicked");
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        first_encoded.disarm_after_durable();
        second_encoded.disarm_after_durable();

        Ok(())
    }

    fn wait_for_durable_follower(coordinator: &WalV8ParallelCoordinator) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);
        while coordinator.inner.state.lock().durable_followers == 0 {
            ensure!(
                Instant::now() < deadline,
                "durable follower did not start waiting"
            );
            std::thread::yield_now();
        }

        Ok(())
    }

    #[test]
    fn durable_follower_returns_while_driver_still_owns_a_later_ticket() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        mark_producer_ready(&mut second_encoded, &coordinator, false)?;
        let mut driver = coordinator
            .acquire_durability_driver(2)?
            .context("missing driver")?;

        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let follower = std::thread::spawn(move || {
            let result = waiting
                .acquire_durability_driver(1)
                .map(|driver| driver.is_none());
            let _ = tx.send(result);
        });
        wait_for_durable_follower(&coordinator)?;
        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        first_encoded.disarm_after_durable();
        assert!(rx.recv_timeout(Duration::from_secs(1))??);
        assert!(coordinator.inner.state.lock().durability_driver_active);
        assert_eq!(coordinator.durable_ticket_end(), 1);
        follower.join().expect("durable follower panicked");

        coordinator.mark_data_complete(1)?;
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        second_encoded.disarm_after_durable();
        driver.complete();
        drop(driver);
        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn driver_handoff_releases_a_follower_whose_ticket_is_not_yet_durable() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        mark_producer_ready(&mut second_encoded, &coordinator, false)?;
        let mut first_driver = coordinator
            .acquire_durability_driver(1)?
            .context("missing first driver")?;

        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let follower = std::thread::spawn(move || {
            let _ = tx.send(waiting.acquire_durability_driver(2));
        });
        wait_for_durable_follower(&coordinator)?;
        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        first_encoded.disarm_after_durable();
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        first_driver.complete();
        drop(first_driver);
        let mut second_driver = rx
            .recv_timeout(Duration::from_secs(1))??
            .context("missing successor driver")?;
        follower.join().expect("driver-handoff follower panicked");

        coordinator.mark_data_complete(1)?;
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        second_encoded.disarm_after_durable();
        second_driver.complete();
        drop(second_driver);
        assert_eq!(coordinator.durable_ticket_end(), 2);
        assert!(!coordinator.is_poisoned());

        Ok(())
    }

    #[test]
    fn unfinished_driver_poisons_before_waking_a_successor() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let _unfinished = admit(&first, &coordinator)?;
        let driver = coordinator
            .acquire_durability_driver(1)?
            .context("missing driver")?;
        let waiting = coordinator.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let follower = std::thread::spawn(move || {
            let result = waiting
                .acquire_durability_driver(1)
                .map(|driver| driver.is_some())
                .map_err(|error| error.to_string());
            let _ = tx.send(result);
        });
        wait_for_durable_follower(&coordinator)?;
        drop(driver);
        let result = rx.recv_timeout(Duration::from_secs(1))?;
        assert!(
            result
                .expect_err("follower took over a failed runtime")
                .contains("poisoned")
        );
        follower.join().expect("poisoned follower panicked");
        assert!(coordinator.is_poisoned());
        assert_eq!(coordinator.durable_ticket_end(), 0);

        Ok(())
    }

    #[test]
    fn closing_before_driver_election_still_allows_admitted_ready_work_to_drain() -> Result<()> {
        let coordinator = coordinator(8)?;
        let first = batch(10);
        let mut encoded = admit(&first, &coordinator)?.encode()?;
        mark_producer_ready(&mut encoded, &coordinator, true)?;
        coordinator.close();
        let mut driver = coordinator
            .acquire_durability_driver(1)?
            .context("missing close driver")?;
        let frontier = coordinator.reserve_frontier(1)?;
        coordinator.publish_durable_frontier(frontier)?;
        encoded.disarm_after_durable();
        driver.complete();
        drop(driver);
        assert_eq!(coordinator.durable_ticket_end(), 1);
        assert!(!coordinator.is_poisoned());
        assert!(coordinator.acquire_durability_driver(1)?.is_none());

        Ok(())
    }

    #[test]
    fn published_cursor_excludes_unfinished_data_and_reserved_frontiers() -> Result<()> {
        let coordinator = coordinator(8)?;
        assert_eq!(coordinator.published_cursor(), (0, DATA_START, 0));
        let first = batch(10);
        let second = batch(20);
        let mut first_encoded = admit(&first, &coordinator)?.encode()?;
        let mut second_encoded = admit(&second, &coordinator)?.encode()?;
        mark_producer_ready(&mut first_encoded, &coordinator, true)?;
        mark_producer_ready(&mut second_encoded, &coordinator, false)?;
        assert_eq!(coordinator.published_cursor(), (0, DATA_START, 2));
        let frontier = coordinator.reserve_frontier(1)?;
        assert_eq!(coordinator.published_cursor(), (0, DATA_START, 2));
        coordinator.publish_durable_frontier(frontier)?;
        first_encoded.disarm_after_durable();
        assert_eq!(
            coordinator.published_cursor(),
            (1, frontier.frame_offset + FRAME_LEN_U64, 2)
        );

        coordinator.mark_data_complete(1)?;
        let frontier = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(frontier)?;
        second_encoded.disarm_after_durable();
        assert_eq!(
            coordinator.published_cursor(),
            (2, frontier.frame_offset + FRAME_LEN_U64, 2)
        );

        Ok(())
    }
}
