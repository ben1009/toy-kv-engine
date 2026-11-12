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
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(feature = "bench")]
use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use parking_lot::{Condvar, Mutex};

use crate::{pitr::RecordedAt, wal::MAX_WAL_FILE_SIZE};

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
}

#[derive(Clone)]
pub(crate) struct WalV8ParallelCoordinator {
    inner: Arc<CoordinatorInner>,
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
}

struct CoordinatorState {
    durable_ticket_end: u64,
    durable_prefix_digest: WalV8Digest,
    folded_ticket_end: u64,
    folded_prefix_digest: WalV8Digest,
    next_ticket: u64,
    next_data_offset: u64,
    last_commit_ts: u64,
    last_recorded_at: Option<RecordedAt>,
    reserved_bytes: u64,
    reserved_preparation_bytes: u64,
    tickets: VecDeque<TicketState>,
    pending_frontier: Option<WalV8FrontierReservation>,
    last_frontier_end: u64,
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

#[cfg(feature = "bench")]
struct PendingCompletionTimeline {
    group: crate::wal::parallel_worker::WalV8GroupCompletionTimeline,
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

/// Atomic view of the durability decision point. `next_not_ready` is the
/// first admitted ticket beyond the prefix that can be covered by a FRONTIER.
pub(crate) struct WalV8FrontierReadySnapshot {
    pub(crate) ready_prefix: Vec<WalV8TicketProgress>,
    pub(crate) admitted_ticket_count: usize,
    #[cfg(feature = "bench")]
    pub(crate) reserved_preparation_bytes: u64,
    #[cfg(feature = "bench")]
    pub(crate) next_not_ready: Option<WalV8TicketProgress>,
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

impl WalV8ParallelCoordinator {
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

        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState {
                    durable_ticket_end: config.next_ticket,
                    durable_prefix_digest: config.durable_prefix_digest,
                    folded_ticket_end: config.next_ticket,
                    folded_prefix_digest: config.durable_prefix_digest,
                    next_ticket: config.next_ticket,
                    next_data_offset: config.next_data_offset,
                    last_commit_ts: config.last_commit_ts,
                    last_recorded_at: config.last_recorded_at,
                    reserved_bytes: 0,
                    reserved_preparation_bytes: 0,
                    tickets,
                    pending_frontier: None,
                    last_frontier_end: config.next_data_offset,
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
                }),
                changed: Condvar::new(),
                durability_changed: Condvar::new(),
                durable_published: Condvar::new(),
                admitted_ticket_end: AtomicU64::new(config.next_ticket),
                #[cfg(feature = "bench")]
                data_completion_timestamps_enabled: std::sync::atomic::AtomicBool::new(false),
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
            }),
        })
    }

    /// Return all admitted tickets, including producers that have not finished
    /// encoding. This is the group-visibility source for the Parallel packer.
    pub(crate) fn admitted_window(&self) -> Vec<WalV8TicketProgress> {
        self.inner
            .state
            .lock()
            .tickets
            .iter()
            .map(TicketState::progress)
            .collect()
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
        let count = (state.next_ticket - first_ticket).min(u64::try_from(max_tickets)?);

        Ok(WalV8DataGroupWindow {
            ticket_end: first_ticket + count,
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
        let state = self.inner.state.lock();
        if state.poisoned {
            return Vec::new();
        }

        state
            .tickets
            .iter()
            .take_while(|ticket| ticket.frames_ready)
            .map(TicketState::progress)
            .collect()
    }

    /// Return the contiguous head prefix whose frames, ordered digest fold,
    /// and DATA completion are ready for an embedded FRONTIER.
    pub(crate) fn frontier_ready_prefix(&self) -> Vec<WalV8TicketProgress> {
        let state = self.inner.state.lock();
        if state.poisoned {
            return Vec::new();
        }

        state
            .tickets
            .iter()
            .take_while(|ticket| ticket.is_frontier_ready())
            .map(TicketState::progress)
            .collect()
    }

    /// Give already-admitted successors one bounded opportunity to join a
    /// ready durability prefix. A lone producer or fully ready window never
    /// waits, and close/poison terminates the optional wait immediately.
    pub(crate) fn coalesce_frontier_ready(&self, budget: Duration) -> bool {
        let mut state = self.inner.state.lock();
        let deadline = Instant::now() + budget;
        let mut waited = false;
        while !state.closed
            && !state.poisoned
            && state
                .tickets
                .front()
                .is_some_and(TicketState::is_frontier_ready)
            && state
                .tickets
                .iter()
                .any(|ticket| !ticket.is_frontier_ready())
        {
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

    /// Snapshot the frontier-ready prefix and the first admitted ticket that
    /// would extend it, under one lock acquisition. Benchmark diagnostics use
    /// the blocked ticket's readiness to identify why a durability group
    /// stopped at this boundary.
    pub(crate) fn frontier_ready_snapshot(&self) -> WalV8FrontierReadySnapshot {
        let state = self.inner.state.lock();
        if state.poisoned {
            return WalV8FrontierReadySnapshot {
                ready_prefix: Vec::new(),
                admitted_ticket_count: 0,
                #[cfg(feature = "bench")]
                reserved_preparation_bytes: 0,
                #[cfg(feature = "bench")]
                next_not_ready: None,
            };
        }
        let mut ready_prefix = Vec::new();
        for ticket in &state.tickets {
            if !ticket.is_frontier_ready() {
                break;
            }
            ready_prefix.push(ticket.progress());
        }
        #[cfg(feature = "bench")]
        let next_not_ready = state
            .tickets
            .get(ready_prefix.len())
            .map(TicketState::progress);

        WalV8FrontierReadySnapshot {
            ready_prefix,
            admitted_ticket_count: state.tickets.len(),
            #[cfg(feature = "bench")]
            reserved_preparation_bytes: state.reserved_preparation_bytes,
            #[cfg(feature = "bench")]
            next_not_ready,
        }
    }

    /// Publish that one ticket's physical DATA frames have been materialized.
    pub(crate) fn mark_frames_ready(&self, ticket: u64) -> Result<()> {
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
            return Ok(());
        }
        state.tickets[index].frames_ready = true;
        #[cfg(feature = "bench")]
        refresh_data_completion_order_hol(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        advance_completion_timeline_prefix(&mut state);
        signal_change(&self.inner, &mut state);

        Ok(())
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
        if !state.tickets[index].frames_ready
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
        self.mark_digest_ready_with_cpu(digest).map(|_| ())
    }

    /// Publish a producer digest and return the ordered-link CPU time spent
    /// folding digests that became contiguous because of this event.
    pub(crate) fn mark_digest_ready_with_cpu(&self, digest: WalV8DigestReady) -> Result<u64> {
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
        if let Err(error) = fold_ready_digests(&mut state, hol_profile.as_deref()) {
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
        if !state.tickets[index].frames_ready {
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
        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        let Some(ticket_count) = tickets.end.checked_sub(tickets.start) else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA completion group has an invalid ticket range")
        };
        let Ok(ticket_count) = usize::try_from(ticket_count) else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA completion group exceeds addressable tickets")
        };
        if ticket_count == 0 {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA completion group is empty")
        }
        let Some(start_index) = state
            .tickets
            .iter()
            .position(|entry| entry.assignment.segment_ticket == tickets.start)
        else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-completion group names an unknown ticket")
        };
        let Some(end_index) = start_index.checked_add(ticket_count) else {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-completion group index overflows")
        };
        if end_index > state.tickets.len() {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-completion group extends past admitted tickets")
        }
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
            if entry.assignment.segment_ticket != expected_ticket || !entry.frames_ready {
                poison_locked(&self.inner, &mut state);
                anyhow::bail!("v8 DATA-completion group does not match ready admitted tickets")
            }
            all_complete &= entry.data_complete;
        }
        if all_complete {
            return Ok(());
        }
        if state
            .tickets
            .iter()
            .skip(start_index)
            .take(ticket_count)
            .any(|entry| entry.data_complete)
        {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 DATA-completion group partially duplicates prior completion")
        }
        #[cfg(feature = "bench")]
        let data_completed_at = self
            .inner
            .data_completion_timestamps_enabled
            .load(Ordering::Acquire)
            .then(Instant::now);
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
        #[cfg(feature = "bench")]
        refresh_data_completion_order_hol(&self.inner, &mut state);
        #[cfg(feature = "bench")]
        advance_completion_timeline_prefix(&mut state);
        signal_change(&self.inner, &mut state);
        signal_durability_progress(&self.inner, &mut state);

        Ok(())
    }

    #[cfg(feature = "bench")]
    pub(crate) fn record_data_completion_timeline(
        &self,
        group: crate::wal::parallel_worker::WalV8GroupCompletionTimeline,
    ) -> Result<()> {
        let mut state = self.inner.state.lock();
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        ensure!(
            group.ticket_start < group.ticket_end
                && group.ticket_start >= state.durable_ticket_end
                && group.ticket_end <= state.next_ticket,
            "v8 completion timeline names an invalid ticket range"
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
        state.completion_timeline_samples.insert(
            group.ticket_end,
            PendingCompletionTimeline {
                group,
                prefix_ready_at: None,
            },
        );
        advance_completion_timeline_prefix(&mut state);

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
        let ready = state
            .tickets
            .iter()
            .take_while(|entry| entry.is_frontier_ready())
            .count();
        let ticket_count = usize::try_from(ticket_end - state.durable_ticket_end)
            .context("v8 FRONTIER ticket count exceeds usize")?;
        ensure!(
            ticket_count <= ready,
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
        let mut state = self.inner.state.lock();
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
        let valid_ticket_prefix = ticket_count > 0
            && ticket_count <= state.tickets.len()
            && state
                .tickets
                .iter()
                .take(ticket_count)
                .enumerate()
                .all(|(index, entry)| {
                    u64::try_from(index)
                        .ok()
                        .and_then(|index| state.durable_ticket_end.checked_add(index))
                        .is_some_and(|ticket| {
                            entry.assignment.segment_ticket == ticket && entry.is_frontier_ready()
                        })
                });
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
            || frontier_end < state.last_frontier_end
            || frontier_end > state.next_data_offset
            || frontier_end > self.inner.max_file_size
            || prefix_reserved_bytes > state.reserved_bytes
        {
            poison_locked(&self.inner, &mut state);
            anyhow::bail!("v8 durable FRONTIER covers an invalid ticket prefix");
        }
        for _ in 0..ticket_count {
            state.tickets.pop_front();
        }
        state.reserved_bytes -= prefix_reserved_bytes;
        state.durable_ticket_end = reservation.ticket_end;
        state.durable_prefix_digest = reservation.prefix_digest;
        state.last_frontier_end = frontier_end;
        state.pending_frontier = None;
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
        self.inner.durable_published.notify_all();
        drop(state);

        #[cfg(feature = "bench")]
        let durable_published_at = Instant::now();

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
                    reaper_recv_to_prefix_ready_ns: reaper_to_prefix_ready_ns,
                    prefix_ready_to_durable_ns,
                };
                profile.record_wal_v8_completion_timeline(observation);
            }
        }

        Ok(())
    }

    /// Stop new admissions and wake packer/durability waiters.
    pub(crate) fn close(&self) {
        let mut state = self.inner.state.lock();

        if !state.closed {
            state.closed = true;
            signal_change(&self.inner, &mut state);
            signal_durability_progress(&self.inner, &mut state);
            self.inner.durable_published.notify_all();
        }
    }

    /// Poison the admission window after an uncertain write, marker, or sync.
    pub(crate) fn poison(&self) {
        let mut state = self.inner.state.lock();
        poison_locked(&self.inner, &mut state);
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.inner.state.lock().poisoned
    }

    #[cfg(feature = "bench")]
    pub(crate) fn ordered_link_hash_cpu_ns(&self) -> u64 {
        self.inner.state.lock().ordered_link_hash_cpu_ns
    }

    pub(crate) fn durable_ticket_end(&self) -> u64 {
        self.inner.state.lock().durable_ticket_end
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
        let mut state = self.inner.state.lock();
        ensure!(
            ticket_end <= state.next_ticket,
            "v8 durability driver names an unassigned ticket"
        );

        loop {
            ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
            if state.durable_ticket_end >= ticket_end {
                return Ok(None);
            }
            if !state.durability_driver_active {
                state.durability_driver_active = true;
                return Ok(Some(WalV8DurabilityDriver {
                    coordinator: self.clone(),
                    completed: false,
                }));
            }
            #[cfg(test)]
            {
                state.durable_followers += 1;
            }
            // Close wakes followers as well, but admitted tickets may still
            // drain through the active driver. Poison or driver failure is
            // terminal; a normal handoff permits the next caller to drive.
            self.inner.durable_published.wait(&mut state);
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
        state.durability_driver_active = false;
        self.coordinator.inner.durable_published.notify_all();
    }
}

impl WalV8AdmissionPublisher for WalV8ParallelCoordinator {
    type Guard = WalV8ParallelAdmissionGuard;

    fn admit_and_publish(
        &self,
        plan: WalV8AdmissionPlan,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)> {
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

/// Fold producer digests in strict ticket order, even when later producers
/// finish hashing first. The fold is bounded by the admitted ticket window.
fn fold_ready_digests(
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
        let Some(batch_digest) = ticket.digest_ready else {
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

fn poison_locked(inner: &CoordinatorInner, state: &mut CoordinatorState) {
    if !state.poisoned {
        state.poisoned = true;
        signal_change(inner, state);
        signal_durability_progress(inner, state);
        inner.durable_published.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pitr::{ArchiveEpochId, ChainAnchor, SegmentId, TimelineId, WalBatch, WalEntry},
        wal::v8::prepare::PreparedWalV8Batch,
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
            },
        )
    }

    fn coordinator(max_inflight_tickets: usize) -> Result<WalV8ParallelCoordinator> {
        coordinator_with_byte_limit(max_inflight_tickets, 64 * 1024 * 1024)
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
        assert!(snapshot.next_not_ready.is_none());

        let reservation = coordinator.reserve_frontier(2)?;
        coordinator.publish_durable_frontier(reservation)?;
        first.disarm_after_durable();
        second.disarm_after_durable();
        drop(preparation);

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
