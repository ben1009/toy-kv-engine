//! Bounded admission and readiness coordination for the proposed v8 Parallel writer.
//!
//! This coordinator is intentionally not connected to the WAL registry. It
//! keeps ticket visibility independent from digest, frame, and DATA completion
//! so the live writer can form groups from admitted work without waiting for
//! producer preparation.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use parking_lot::{Condvar, Mutex};

use crate::{pitr::RecordedAt, wal::MAX_WAL_FILE_SIZE};

use super::{
    WAL_V8_HEADER_LEN,
    codec::{self, WAL_V8_FRAME_LEN, WalV8Header},
    digest::WalV8Digest,
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
    closed: bool,
    poisoned: bool,
    #[cfg(feature = "bench")]
    ordered_link_hash_cpu_ns: u64,
}

struct TicketState {
    assignment: WalV8BatchAssignment,
    plan: WalV8AdmissionPlan,
    reserved_bytes: u64,
    frames_ready: bool,
    digest_ready: Option<WalV8DigestReady>,
    prefix_digest_after: Option<WalV8Digest>,
    data_complete: bool,
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
                    closed: false,
                    poisoned: false,
                    #[cfg(feature = "bench")]
                    ordered_link_hash_cpu_ns: 0,
                }),
                changed: Condvar::new(),
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
        signal_change(&self.inner, &mut state);
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
            None => state.tickets[index].digest_ready = Some(digest),
        }
        #[cfg(feature = "bench")]
        let cpu_before = state.ordered_link_hash_cpu_ns;
        if let Err(error) = fold_ready_digests(&mut state) {
            poison_locked(&self.inner, &mut state);
            return Err(error);
        }
        #[cfg(feature = "bench")]
        let ordered_link_hash_cpu_ns = state.ordered_link_hash_cpu_ns.saturating_sub(cpu_before);
        #[cfg(not(feature = "bench"))]
        let ordered_link_hash_cpu_ns = 0;
        signal_change(&self.inner, &mut state);
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
        signal_change(&self.inner, &mut state);
        Ok(())
    }

    /// Reserve a physical tail slot for the next FRONTIER. Admission after this
    /// call allocates DATA after the marker, even if the selected prefix's
    /// earlier DATA is still awaiting a sync.
    pub(crate) fn reserve_frontier(&self, ticket_end: u64) -> Result<WalV8FrontierReservation> {
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
        signal_change(&self.inner, &mut state);
        Ok(())
    }

    /// Stop new admissions and wake packer/durability waiters.
    pub(crate) fn close(&self) {
        let mut state = self.inner.state.lock();
        if !state.closed {
            state.closed = true;
            signal_change(&self.inner, &mut state);
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

    pub(crate) fn last_frontier_end(&self) -> u64 {
        self.inner.state.lock().last_frontier_end
    }

    pub(crate) fn change_generation(&self) -> u64 {
        self.inner.state.lock().change_generation
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
        ensure!(!state.poisoned, "v8 Parallel coordinator is poisoned");
        ensure!(!state.closed, "v8 Parallel coordinator is closed");
        ensure!(
            state.tickets.len() < self.inner.max_inflight_tickets,
            "v8 Parallel ticket window is full"
        );
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
        ensure!(
            data_end
                .checked_add(reserved_frontier_bytes)
                .is_some_and(|end| end <= self.inner.max_file_size),
            "v8 Parallel WAL segment rotation required"
        );
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
        });
        state.next_ticket = next_ticket;
        state.next_data_offset = data_end;
        state.last_commit_ts = plan.commit_ts;
        state.last_recorded_at = Some(recorded_at);
        state.reserved_bytes = new_reserved_bytes;
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
fn fold_ready_digests(state: &mut CoordinatorState) -> Result<()> {
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
        state.folded_ticket_end = next_ticket;
        state.folded_prefix_digest = prefix_digest;
    }
    Ok(())
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

fn poison_locked(inner: &CoordinatorInner, state: &mut CoordinatorState) {
    if !state.poisoned {
        state.poisoned = true;
        signal_change(inner, state);
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
    fn bounded_admission_does_not_consume_a_ticket_when_full() -> Result<()> {
        let coordinator = coordinator(1)?;
        let first = batch(10);
        let second = batch(20);
        let admitted = admit(&first, &coordinator)?;
        assert!(admit(&second, &coordinator).is_err());
        assert_eq!(coordinator.next_ticket(), 1);
        assert_eq!(coordinator.admitted_window().len(), 1);
        drop(admitted);
        assert!(coordinator.is_poisoned());
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
}
