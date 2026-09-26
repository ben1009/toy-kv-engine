//! Dormant dedicated io_uring writer for the v4 parallel WAL candidate.
//!
//! Dedicated io_uring writer for the v4 parallel WAL candidate.

use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io,
    marker::PhantomData,
    ops::Range,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    sync::Arc,
    thread::{self, JoinHandle},
};

#[cfg(feature = "bench")]
use std::collections::BTreeMap;
#[cfg(feature = "bench")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "bench")]
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail, ensure};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Condvar, Mutex};

use super::{BUFFER_POOL_BUF_SIZE, DirectBuf, MAX_WAL_FILE_SIZE, RING_SIZE, WAL_FD_INDEX};

/// Benchmark-only observations shared by the WAL worker and sync coordinator.
/// Production builds keep this as a zero-sized no-op.
#[derive(Default)]
pub(crate) struct WalSyncProgress {
    #[cfg(feature = "bench")]
    state: Mutex<WalSyncProgressState>,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
    #[cfg(feature = "bench")]
    sync_active: AtomicBool,
    #[cfg(feature = "bench")]
    io_event_gate: Mutex<()>,
}

/// Pins a diagnostic-window tag across one io_uring operation and its accounting.
pub(crate) struct WalIoEvent<'a> {
    progress: &'a WalSyncProgress,
    #[cfg(feature = "bench")]
    _gate: Option<parking_lot::MutexGuard<'a, ()>>,
    #[cfg(feature = "bench")]
    during_sync: bool,
    #[cfg(feature = "bench")]
    diagnostics_enabled: bool,
    #[cfg(feature = "bench")]
    event_at: Option<Instant>,
    #[cfg(not(feature = "bench"))]
    _progress: PhantomData<&'a WalSyncProgress>,
}

impl WalIoEvent<'_> {
    fn during_sync(&self) -> bool {
        #[cfg(feature = "bench")]
        {
            self.during_sync
        }
        #[cfg(not(feature = "bench"))]
        {
            false
        }
    }

    fn set_event_at(&mut self, event_at: Option<std::time::Instant>) {
        #[cfg(feature = "bench")]
        {
            self.event_at = event_at;
        }
        #[cfg(not(feature = "bench"))]
        let _ = event_at;
    }

    fn event_timestamp(&self) -> Option<std::time::Instant> {
        #[cfg(feature = "bench")]
        {
            self.diagnostics_enabled.then(Instant::now)
        }
        #[cfg(not(feature = "bench"))]
        {
            None
        }
    }

    fn event_at(&self) -> Option<std::time::Instant> {
        #[cfg(feature = "bench")]
        {
            self.event_at
        }
        #[cfg(not(feature = "bench"))]
        {
            None
        }
    }

    fn record_write_submission(
        &self,
        count: u64,
        inflight_groups: u64,
        outstanding_write_sqes: u64,
    ) {
        self.progress.record_write_submission(
            count,
            inflight_groups,
            outstanding_write_sqes,
            self.during_sync(),
            self.event_at(),
        );
    }

    fn record_write_completions(&self, count: u64) {
        self.progress
            .record_write_completions(count, self.during_sync(), self.event_at());
    }

    fn record_group_completion(&self, result: &GroupWriteResult) {
        self.progress
            .record_group_completion(result, self.during_sync(), self.event_at());
    }
}

#[cfg(feature = "bench")]
#[derive(Default)]
struct WalSyncProgressState {
    sync_write_sqes_submitted: u64,
    sync_write_cqes_completed: u64,
    sync_groups_completed: u64,
    sync_written_frontier_start: u64,
    sync_written_frontier_end: u64,
    sync_groups_covered: u64,
    sync_io_events: Vec<WalSyncIoEvent>,
    written_frontier: u64,
    durable_frontier: u64,
    completed_groups: BTreeMap<u64, (u64, u64, u64, Instant)>,
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy)]
struct WalSyncIoEvent {
    at: Instant,
    kind: WalSyncIoEventKind,
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy)]
enum WalSyncIoEventKind {
    WriteSubmission(u64),
    WriteCompletion(u64),
    GroupCompletion,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SyncProgressSnapshot {
    pub(crate) written_frontier_start: u64,
    pub(crate) written_frontier_end: u64,
    pub(crate) write_sqes_submitted: u64,
    pub(crate) write_cqes_completed: u64,
    pub(crate) groups_completed: u64,
    pub(crate) groups_covered: u64,
}

impl WalSyncProgress {
    #[cfg(feature = "bench")]
    pub(crate) fn set_profile(&self, profile: std::sync::Arc<crate::mem_table::WriteProfile>) {
        self.profile.store(Some(profile));
    }

    #[cfg(not(feature = "bench"))]
    pub(crate) fn set_profile(&self, _profile: std::sync::Arc<crate::mem_table::WriteProfile>) {}

    #[cfg(feature = "bench")]
    pub(crate) fn profile(&self) -> Option<std::sync::Arc<crate::mem_table::WriteProfile>> {
        self.profile.load_full()
    }

    pub(crate) fn record_worker_eventfd_notification(&self) {
        #[cfg(feature = "bench")]
        if let Some(profile) = self.profile() {
            profile.record_wal_worker_eventfd_notification();
        }
    }

    /// Capture the active-window tag before an I/O operation.
    ///
    /// The caller must timestamp the operation itself and pass that timestamp
    /// when recording metrics. The tag alone is insufficient because the
    /// worker may be descheduled between this call and the operation.
    pub(crate) fn begin_io_event(&self) -> WalIoEvent<'_> {
        #[cfg(feature = "bench")]
        {
            let diagnostics_enabled = self
                .profile()
                .is_some_and(|profile| profile.wal_sync_diagnostics_enabled());
            if !diagnostics_enabled {
                return WalIoEvent {
                    progress: self,
                    _gate: None,
                    during_sync: false,
                    diagnostics_enabled: false,
                    event_at: None,
                };
            }
            let gate = self.io_event_gate.lock();
            let during_sync = self.sync_active.load(Ordering::Acquire);
            WalIoEvent {
                progress: self,
                _gate: Some(gate),
                during_sync,
                diagnostics_enabled: true,
                event_at: None,
            }
        }
        #[cfg(not(feature = "bench"))]
        {
            WalIoEvent {
                progress: self,
                _progress: PhantomData,
            }
        }
    }

    pub(crate) fn record_write_submission(
        &self,
        count: u64,
        inflight_groups: u64,
        outstanding_write_sqes: u64,
        during_sync: bool,
        event_at: Option<std::time::Instant>,
    ) {
        #[cfg(feature = "bench")]
        {
            let profile = self.profile();
            if let Some(profile) = &profile {
                profile.record_wal_inflight_groups(inflight_groups);
                profile.record_wal_outstanding_write_sqes(outstanding_write_sqes);
            }
            if profile.is_some_and(|profile| profile.wal_sync_diagnostics_enabled()) {
                let mut state = self.state.lock();
                if during_sync && let Some(at) = event_at {
                    state.sync_io_events.push(WalSyncIoEvent {
                        at,
                        kind: WalSyncIoEventKind::WriteSubmission(count),
                    });
                }
            }
        }
        #[cfg(not(feature = "bench"))]
        let _ = (
            count,
            inflight_groups,
            outstanding_write_sqes,
            during_sync,
            event_at,
        );
    }

    pub(crate) fn record_write_completions(
        &self,
        count: u64,
        during_sync: bool,
        event_at: Option<std::time::Instant>,
    ) {
        #[cfg(feature = "bench")]
        {
            if let Some(profile) = self.profile() {
                profile.record_wal_cqe_count(count);
                if profile.wal_sync_diagnostics_enabled() {
                    let mut state = self.state.lock();
                    if during_sync && let Some(at) = event_at {
                        state.sync_io_events.push(WalSyncIoEvent {
                            at,
                            kind: WalSyncIoEventKind::WriteCompletion(count),
                        });
                    }
                }
            }
        }
        #[cfg(not(feature = "bench"))]
        let _ = (count, during_sync, event_at);
    }

    pub(crate) fn record_group_completion(
        &self,
        result: &GroupWriteResult,
        during_sync: bool,
        event_at: Option<std::time::Instant>,
    ) {
        #[cfg(feature = "bench")]
        if result.error.is_none()
            && let Some(profile) = self.profile()
        {
            profile.record_wal_commit_group(result.write_count, result.write_bytes);
            if profile.wal_sync_diagnostics_enabled() {
                let mut state = self.state.lock();
                let previous = state.completed_groups.insert(
                    result.tickets.start,
                    (
                        result.tickets.end,
                        result.write_count,
                        result.write_bytes,
                        event_at.unwrap_or_else(Instant::now),
                    ),
                );
                debug_assert!(previous.is_none(), "WAL group completion is unique");
                while let Some((end, _, _, _)) = state.completed_groups.get(&state.written_frontier)
                {
                    state.written_frontier = *end;
                }
                if during_sync && let Some(at) = event_at {
                    state.sync_io_events.push(WalSyncIoEvent {
                        at,
                        kind: WalSyncIoEventKind::GroupCompletion,
                    });
                }
            }
        }
        #[cfg(not(feature = "bench"))]
        let _ = (result, during_sync, event_at);
    }

    pub(crate) fn record_preallocation_ns(&self, nanos: u64) {
        #[cfg(feature = "bench")]
        if let Some(profile) = self.profile() {
            profile.record_wal_preallocation_ns(nanos);
        }
        #[cfg(not(feature = "bench"))]
        let _ = nanos;
    }

    pub(crate) fn begin_sync(&self, target: u64, durable_frontier: u64) -> SyncProgressSnapshot {
        #[cfg(feature = "bench")]
        {
            let Some(profile) = self.profile().filter(|p| p.wal_sync_diagnostics_enabled()) else {
                let _ = (target, durable_frontier);
                return SyncProgressSnapshot::default();
            };
            drop(profile);
            let mut state = self.state.lock();
            debug_assert!(
                !self.sync_active.load(Ordering::Acquire),
                "only one WAL sync may be active"
            );
            debug_assert_eq!(state.durable_frontier, durable_frontier);
            state.sync_write_sqes_submitted = 0;
            state.sync_write_cqes_completed = 0;
            state.sync_groups_completed = 0;
            let groups_covered = state
                .completed_groups
                .range(durable_frontier..target)
                .count() as u64;
            state.sync_written_frontier_start = state.written_frontier;
            state.sync_written_frontier_end = state.written_frontier;
            state.sync_groups_covered = groups_covered;
            SyncProgressSnapshot {
                written_frontier_start: state.written_frontier,
                written_frontier_end: state.written_frontier,
                groups_covered,
                ..SyncProgressSnapshot::default()
            }
        }
        #[cfg(not(feature = "bench"))]
        {
            let _ = (target, durable_frontier);
            SyncProgressSnapshot::default()
        }
    }

    /// Mark the start of the `fdatasync` observation window after its snapshot
    /// has been prepared. The event gate ensures earlier worker bookkeeping is
    /// assigned to the preceding window before this one starts.
    #[cfg(feature = "bench")]
    pub(crate) fn start_sync_activity(&self) {
        let diagnostics_enabled = self
            .profile()
            .is_some_and(|profile| profile.wal_sync_diagnostics_enabled());
        if diagnostics_enabled {
            let _event_gate = self.io_event_gate.lock();
            let mut state = self.state.lock();
            state.sync_written_frontier_start = state.written_frontier;
            state.sync_written_frontier_end = state.written_frontier;
            state.sync_io_events.clear();
            let was_active = self.sync_active.swap(true, Ordering::AcqRel);
            debug_assert!(!was_active, "only one WAL sync may be active");
        }
    }

    #[cfg(not(feature = "bench"))]
    pub(crate) fn start_sync_activity(&self) {}

    pub(crate) fn finish_sync(
        &self,
        started_at: Option<std::time::Instant>,
        finished_at: Option<std::time::Instant>,
    ) -> (SyncProgressSnapshot, Option<std::time::Instant>) {
        #[cfg(feature = "bench")]
        {
            // The caller samples both timestamps around the fdatasync call.
            // Event timestamps exclude work in the setup/teardown gaps even if
            // an event retained a stale active-window tag.
            let was_active = self.sync_active.swap(false, Ordering::AcqRel);
            let finished_at = finished_at.unwrap_or_else(Instant::now);
            if !was_active {
                return (SyncProgressSnapshot::default(), Some(finished_at));
            }
            let _event_gate = self.io_event_gate.lock();
            let mut state = self.state.lock();
            let started_at =
                started_at.expect("active benchmark WAL sync records its start timestamp");
            let mut written_frontier_start = state.sync_written_frontier_start;
            loop {
                let next_group = state
                    .completed_groups
                    .get(&written_frontier_start)
                    .map(|(end, _, _, completed_at)| (*end, *completed_at));
                let Some((end, completed_at)) = next_group else {
                    break;
                };
                if completed_at >= started_at {
                    break;
                }
                written_frontier_start = end;
            }
            state.sync_written_frontier_start = written_frontier_start;
            let mut write_sqes_submitted = 0_u64;
            let mut write_cqes_completed = 0_u64;
            let mut groups_completed = 0_u64;
            for event in &state.sync_io_events {
                if event.at < started_at || event.at >= finished_at {
                    continue;
                }
                match event.kind {
                    WalSyncIoEventKind::WriteSubmission(count) => {
                        write_sqes_submitted = write_sqes_submitted.saturating_add(count);
                    }
                    WalSyncIoEventKind::WriteCompletion(count) => {
                        write_cqes_completed = write_cqes_completed.saturating_add(count);
                    }
                    WalSyncIoEventKind::GroupCompletion => {
                        groups_completed = groups_completed.saturating_add(1);
                    }
                }
            }
            state.sync_write_sqes_submitted = write_sqes_submitted;
            state.sync_write_cqes_completed = write_cqes_completed;
            state.sync_groups_completed = groups_completed;
            state.sync_io_events.clear();
            state.sync_written_frontier_end = written_frontier_start;
            loop {
                let next_group = state
                    .completed_groups
                    .get(&state.sync_written_frontier_end)
                    .map(|(end, _, _, completed_at)| (*end, *completed_at));
                let Some((end, completed_at)) = next_group else {
                    break;
                };
                if completed_at >= finished_at {
                    break;
                }
                state.sync_written_frontier_end = end;
            }
            (
                SyncProgressSnapshot {
                    written_frontier_start,
                    written_frontier_end: state.sync_written_frontier_end,
                    write_sqes_submitted: state.sync_write_sqes_submitted,
                    write_cqes_completed: state.sync_write_cqes_completed,
                    groups_completed: state.sync_groups_completed,
                    groups_covered: state.sync_groups_covered,
                },
                Some(finished_at),
            )
        }
        #[cfg(not(feature = "bench"))]
        {
            let _ = (started_at, finished_at);
            (SyncProgressSnapshot::default(), None)
        }
    }

    pub(crate) fn mark_durable(&self, target: u64) {
        #[cfg(feature = "bench")]
        {
            let Some(profile) = self.profile().filter(|p| p.wal_sync_diagnostics_enabled()) else {
                let _ = target;
                return;
            };
            drop(profile);
            let mut state = self.state.lock();
            debug_assert!(!self.sync_active.load(Ordering::Acquire));
            state.durable_frontier = state.durable_frontier.max(target);
            state.completed_groups = state.completed_groups.split_off(&target);
        }
        #[cfg(not(feature = "bench"))]
        let _ = target;
    }
}

const MAX_INFLIGHT_GROUPS: usize = 8;
const MAX_OUTSTANDING_SQES: usize = RING_SIZE;
const DIRECT_IO_ALIGNMENT: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct GroupId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct RequestId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BufferId(u64);

/// An owned buffer and its already-reserved location in the WAL file.
pub(crate) struct WriteBuffer<B = DirectBuf> {
    buffer: B,
    file_offset: u64,
    write_len: usize,
}

impl<B> WriteBuffer<B> {
    pub(crate) fn new(buffer: B, file_offset: u64, write_len: usize) -> Self {
        Self {
            buffer,
            file_offset,
            write_len,
        }
    }
}

/// A ticket-ordered group whose file range has already been reserved and
/// preallocated by the packer.
pub(crate) struct WriteGroup<B = DirectBuf> {
    tickets: Range<u64>,
    writes: Vec<WriteBuffer<B>>,
}

impl<B> WriteGroup<B> {
    pub(crate) fn new(
        tickets: Range<u64>,
        writes: Vec<WriteBuffer<B>>,
    ) -> Result<Self, WorkerError> {
        if tickets.start >= tickets.end {
            return Err(WorkerError::InvalidTicketRange);
        }
        if writes.is_empty() {
            return Err(WorkerError::EmptyGroup);
        }

        let mut expected_offset = writes[0].file_offset;
        for write in &writes {
            if write.write_len == 0
                || write.write_len > i32::MAX as usize
                || !write.write_len.is_multiple_of(DIRECT_IO_ALIGNMENT)
                || !write.file_offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
                || write.file_offset != expected_offset
            {
                return Err(WorkerError::InvalidWriteRange);
            }
            expected_offset = expected_offset
                .checked_add(write.write_len as u64)
                .ok_or(WorkerError::InvalidWriteRange)?;
            if expected_offset > MAX_WAL_FILE_SIZE {
                return Err(WorkerError::InvalidWriteRange);
            }
        }

        Ok(Self { tickets, writes })
    }
}

pub(crate) trait WorkerBuffer: Send + 'static + Sized {
    fn as_ptr(&self) -> *const u8;
    fn len(&self) -> usize;
    fn cap(&self) -> usize;
    fn retire(self, pool: &ArrayQueue<Self>);
}

impl WorkerBuffer for DirectBuf {
    fn as_ptr(&self) -> *const u8 {
        DirectBuf::as_ptr(self)
    }

    fn len(&self) -> usize {
        DirectBuf::len(self)
    }

    fn cap(&self) -> usize {
        DirectBuf::cap(self)
    }

    fn retire(self, pool: &ArrayQueue<Self>) {
        if self.cap() == BUFFER_POOL_BUF_SIZE {
            let _ = pool.push(self);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteState {
    Ready,
    Staged,
    Submitted,
    Ambiguous,
    Completed,
    Failed,
    Cancelled,
}

impl WriteState {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    fn may_be_kernel_owned(self) -> bool {
        matches!(self, Self::Staged | Self::Submitted | Self::Ambiguous)
    }
}

struct OwnedWrite<B> {
    group_id: GroupId,
    buffer_id: BufferId,
    buffer: Option<B>,
    file_offset: u64,
    write_len: usize,
    state: WriteState,
}

struct GroupState {
    tickets: Range<u64>,
    file_range: Range<u64>,
    request_ids: Vec<RequestId>,
    next_ready_index: usize,
    remaining_writes: usize,
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerError {
    InvalidTicketRange,
    EmptyGroup,
    InvalidWriteRange,
    TicketHole,
    DuplicateGroup,
    TooManyGroups,
    Poisoned,
    CounterOverflow,
    UnknownRequest,
    RequestNotStaged,
    RequestNotOutstanding,
    TooManySqes,
}

#[derive(Clone, Copy, Debug)]
struct WriteSubmission {
    request_id: RequestId,
    group_id: GroupId,
    buffer_id: BufferId,
    buffer_ptr: *const u8,
    file_offset: u64,
    write_len: usize,
}

enum WorkerEvent<B> {
    BufferRetired(B),
    GroupFinished(GroupWriteResult),
}

/// Completion of all writes in one group. A successful result means every
/// write CQE was full length; it does not imply WAL durability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupWriteResult {
    pub(crate) group_id: u64,
    pub(crate) tickets: Range<u64>,
    pub(crate) write_count: u64,
    pub(crate) write_bytes: u64,
    pub(crate) error: Option<String>,
}

/// Single-owner submission and completion state used by the dedicated writer.
///
/// The ring thread is the only code that mutates this state. Each request maps
/// one SQE identity to one owned buffer, so a buffer cannot leave the worker
/// before its terminal CQE.
struct WorkerCore<B: WorkerBuffer> {
    groups: HashMap<GroupId, GroupState>,
    group_order: VecDeque<GroupId>,
    writes: HashMap<RequestId, OwnedWrite<B>>,
    next_ticket: Option<u64>,
    next_file_offset: Option<u64>,
    next_group_id: u64,
    next_request_id: u64,
    next_buffer_id: u64,
    stage_cursor: usize,
    staged_sqes: usize,
    outstanding_sqes: usize,
    poison_ticket: Option<u64>,
}

impl<B: WorkerBuffer> WorkerCore<B> {
    fn new() -> Self {
        Self {
            groups: HashMap::new(),
            group_order: VecDeque::new(),
            writes: HashMap::new(),
            next_ticket: None,
            next_file_offset: None,
            next_group_id: 0,
            next_request_id: 0,
            next_buffer_id: 0,
            stage_cursor: 0,
            staged_sqes: 0,
            outstanding_sqes: 0,
            poison_ticket: None,
        }
    }

    fn enqueue(&mut self, group: WriteGroup<B>) -> Result<GroupId, WorkerError> {
        if self
            .poison_ticket
            .is_some_and(|poison| group.tickets.start >= poison)
        {
            return Err(WorkerError::Poisoned);
        }
        if let Some(next_ticket) = self.next_ticket
            && group.tickets.start != next_ticket
        {
            return Err(WorkerError::TicketHole);
        }
        if self.groups.len() >= MAX_INFLIGHT_GROUPS {
            return Err(WorkerError::TooManyGroups);
        }

        let file_start = group.writes[0].file_offset;
        if self
            .next_file_offset
            .is_some_and(|next_file_offset| file_start != next_file_offset)
        {
            return Err(WorkerError::InvalidWriteRange);
        }
        let last_write = group.writes.last().expect("validated group has writes");
        let file_end = last_write
            .file_offset
            .checked_add(last_write.write_len as u64)
            .ok_or(WorkerError::InvalidWriteRange)?;

        let group_id = GroupId(self.next_group_id);
        let next_group_id = self
            .next_group_id
            .checked_add(1)
            .ok_or(WorkerError::CounterOverflow)?;

        let request_count =
            u64::try_from(group.writes.len()).map_err(|_| WorkerError::CounterOverflow)?;
        let next_request_id = self
            .next_request_id
            .checked_add(request_count)
            .ok_or(WorkerError::CounterOverflow)?;
        let next_buffer_id = self
            .next_buffer_id
            .checked_add(request_count)
            .ok_or(WorkerError::CounterOverflow)?;
        let mut request_ids = Vec::with_capacity(group.writes.len());
        for write in group.writes {
            let request_id = RequestId(self.next_request_id);
            self.next_request_id += 1;
            let buffer_id = BufferId(self.next_buffer_id);
            self.next_buffer_id += 1;
            request_ids.push(request_id);
            self.writes.insert(
                request_id,
                OwnedWrite {
                    group_id,
                    buffer_id,
                    buffer: Some(write.buffer),
                    file_offset: write.file_offset,
                    write_len: write.write_len,
                    state: WriteState::Ready,
                },
            );
        }

        self.next_group_id = next_group_id;
        self.next_request_id = next_request_id;
        self.next_buffer_id = next_buffer_id;
        self.next_ticket = Some(group.tickets.end);
        self.next_file_offset = Some(file_end);
        self.group_order.push_back(group_id);
        self.groups.insert(
            group_id,
            GroupState {
                tickets: group.tickets,
                file_range: file_start..file_end,
                remaining_writes: request_ids.len(),
                request_ids,
                next_ready_index: 0,
                error: None,
            },
        );
        self.assert_invariants();

        Ok(group_id)
    }

    /// Stage SQEs across groups in round-robin order without waiting for any
    /// group's completions.
    fn stage_writes(&mut self, available: usize) -> Result<Vec<WriteSubmission>, WorkerError> {
        let free_slots = MAX_OUTSTANDING_SQES
            .checked_sub(self.staged_sqes + self.outstanding_sqes)
            .ok_or(WorkerError::TooManySqes)?;
        let limit = available.min(free_slots);
        let mut submissions = Vec::with_capacity(limit);

        while submissions.len() < limit && !self.group_order.is_empty() {
            let group_count = self.group_order.len();
            let start = self.stage_cursor % group_count;
            let mut selected = None;

            for step in 0..group_count {
                let index = (start + step) % group_count;
                let group_id = self.group_order[index];
                let group = self.groups.get(&group_id).expect("ordered group exists");
                if self
                    .poison_ticket
                    .is_some_and(|poison| group.tickets.start >= poison)
                {
                    continue;
                }
                if let Some((request_offset, request_id)) = group
                    .request_ids
                    .iter()
                    .copied()
                    .enumerate()
                    .skip(group.next_ready_index)
                    .find(|(_, request_id)| {
                        self.writes
                            .get(request_id)
                            .is_some_and(|write| write.state == WriteState::Ready)
                    })
                {
                    selected = Some((index, group_id, request_id, request_offset + 1));
                    break;
                }
            }

            let Some((index, group_id, request_id, next_ready_index)) = selected else {
                break;
            };
            self.groups
                .get_mut(&group_id)
                .expect("selected group exists")
                .next_ready_index = next_ready_index;
            let write = self
                .writes
                .get_mut(&request_id)
                .expect("selected write exists");
            let buffer = write.buffer.as_ref().expect("ready write owns buffer");
            let submission = WriteSubmission {
                request_id,
                group_id,
                buffer_id: write.buffer_id,
                buffer_ptr: buffer.as_ptr(),
                file_offset: write.file_offset,
                write_len: write.write_len,
            };
            write.state = WriteState::Staged;
            self.staged_sqes += 1;
            self.stage_cursor = (index + 1) % group_count;
            submissions.push(submission);
        }

        self.assert_invariants();

        Ok(submissions)
    }

    fn mark_submitted(&mut self, request_id: RequestId) -> Result<(), WorkerError> {
        let write = self
            .writes
            .get_mut(&request_id)
            .ok_or(WorkerError::UnknownRequest)?;
        if write.state != WriteState::Staged {
            return Err(WorkerError::RequestNotStaged);
        }

        write.state = WriteState::Submitted;
        self.staged_sqes -= 1;
        self.outstanding_sqes += 1;
        self.assert_invariants();

        Ok(())
    }

    /// Conservatively mark SQEs as potentially kernel-owned after an
    /// ambiguous submit result. Their buffers stay owned until CQEs arrive or
    /// the worker's unresolved state is intentionally leaked on drop.
    fn mark_ambiguous(
        &mut self,
        request_ids: &[RequestId],
    ) -> Result<Vec<WorkerEvent<B>>, WorkerError> {
        let mut failed_starts = Vec::with_capacity(request_ids.len());
        for request_id in request_ids {
            let write = self
                .writes
                .get_mut(request_id)
                .ok_or(WorkerError::UnknownRequest)?;
            if write.state != WriteState::Staged {
                return Err(WorkerError::RequestNotStaged);
            }
            write.state = WriteState::Ambiguous;
            self.staged_sqes -= 1;
            self.outstanding_sqes += 1;
            let group = self
                .groups
                .get(&write.group_id)
                .expect("write's group exists");
            failed_starts.push(group.tickets.start);
        }
        if let Some(first_ambiguous_ticket) = failed_starts.into_iter().min() {
            self.poison_at(first_ambiguous_ticket);
        }
        let mut events = self.cancel_poisoned_ready_writes();
        events.extend(self.finish_completed_groups());
        self.assert_invariants();

        Ok(events)
    }

    fn complete_write(
        &mut self,
        request_id: RequestId,
        cqe_result: i32,
    ) -> Result<Vec<WorkerEvent<B>>, WorkerError> {
        let (group_id, write_len, state) = self
            .writes
            .get(&request_id)
            .map(|write| (write.group_id, write.write_len, write.state))
            .ok_or(WorkerError::UnknownRequest)?;
        if !matches!(state, WriteState::Submitted | WriteState::Ambiguous) {
            return Err(WorkerError::RequestNotOutstanding);
        }

        self.outstanding_sqes -= 1;
        let is_full_write = cqe_result >= 0 && cqe_result as usize == write_len;
        let retired_buffer = {
            let write = self
                .writes
                .get_mut(&request_id)
                .expect("validated write exists");
            write.state = if is_full_write {
                WriteState::Completed
            } else {
                WriteState::Failed
            };
            write.buffer.take().expect("outstanding write owns buffer")
        };

        if !is_full_write {
            let reason = if cqe_result < 0 {
                format!("io_uring write failed with result {cqe_result}")
            } else {
                format!("short io_uring write: expected {write_len}, got {cqe_result}")
            };
            self.groups
                .get_mut(&group_id)
                .expect("write's group exists")
                .error = Some(reason);
            let failed_ticket = self
                .groups
                .get(&group_id)
                .expect("write's group exists")
                .tickets
                .start;
            self.poison_at(failed_ticket);
        }

        let mut events = vec![WorkerEvent::BufferRetired(retired_buffer)];
        let group = self
            .groups
            .get_mut(&group_id)
            .expect("write's group exists");
        group.remaining_writes = group
            .remaining_writes
            .checked_sub(1)
            .expect("outstanding write contributes to group count");
        events.extend(self.cancel_poisoned_ready_writes());
        events.extend(self.finish_completed_groups());
        self.assert_invariants();

        Ok(events)
    }

    fn cancel_poisoned_ready_writes(&mut self) -> Vec<WorkerEvent<B>> {
        let Some(poison_ticket) = self.poison_ticket else {
            return Vec::new();
        };

        let poisoned_groups = self
            .group_order
            .iter()
            .copied()
            .filter(|group_id| {
                self.groups
                    .get(group_id)
                    .is_some_and(|group| group.tickets.start >= poison_ticket)
            })
            .collect::<Vec<_>>();
        let mut events = Vec::new();

        for group_id in poisoned_groups {
            let request_ids = self
                .groups
                .get(&group_id)
                .expect("ordered group exists")
                .request_ids
                .clone();
            let group = self
                .groups
                .get_mut(&group_id)
                .expect("ordered group exists");
            group
                .error
                .get_or_insert_with(|| format!("WAL poisoned at ticket {poison_ticket}"));
            let mut cancelled_writes = 0;

            for request_id in request_ids {
                let write = self
                    .writes
                    .get_mut(&request_id)
                    .expect("group write exists");
                if write.state == WriteState::Ready {
                    write.state = WriteState::Cancelled;
                    events.push(WorkerEvent::BufferRetired(
                        write.buffer.take().expect("ready write owns buffer"),
                    ));
                    cancelled_writes += 1;
                }
            }
            group.remaining_writes = group
                .remaining_writes
                .checked_sub(cancelled_writes)
                .expect("cancelled writes contribute to group count");
        }

        events
    }

    fn finish_completed_groups(&mut self) -> Vec<WorkerEvent<B>> {
        let completed = self
            .group_order
            .iter()
            .copied()
            .filter(|group_id| {
                self.groups
                    .get(group_id)
                    .is_some_and(|group| group.remaining_writes == 0)
            })
            .collect::<Vec<_>>();
        let mut events = Vec::with_capacity(completed.len());

        for group_id in completed {
            let group = self
                .groups
                .remove(&group_id)
                .expect("completed group exists");
            let write_count = group.request_ids.len() as u64;
            let write_bytes = group.file_range.end - group.file_range.start;
            self.group_order.retain(|queued_id| *queued_id != group_id);
            for request_id in group.request_ids {
                let write = self.writes.remove(&request_id);
                debug_assert!(write.is_some_and(|write| write.state.is_terminal()));
            }
            let poison_blocks_group = self
                .poison_ticket
                .is_some_and(|poison| group.tickets.start >= poison);
            let error = if poison_blocks_group {
                group.error.or_else(|| {
                    self.poison_ticket
                        .map(|ticket| format!("WAL poisoned at ticket {ticket}"))
                })
            } else {
                group.error
            };
            events.push(WorkerEvent::GroupFinished(GroupWriteResult {
                group_id: group_id.0,
                tickets: group.tickets,
                write_count,
                write_bytes,
                error,
            }));
        }

        self.stage_cursor = if self.group_order.is_empty() {
            0
        } else {
            self.stage_cursor % self.group_order.len()
        };
        events
    }

    fn close(&mut self) -> Result<Vec<WorkerEvent<B>>, WorkerError> {
        if self
            .writes
            .values()
            .any(|write| write.state.may_be_kernel_owned())
        {
            return Err(WorkerError::RequestNotOutstanding);
        }

        let mut cancelled_by_group = HashMap::<GroupId, usize>::new();
        for write in self.writes.values_mut() {
            if write.state == WriteState::Ready {
                write.state = WriteState::Cancelled;
                write.buffer.take();
                *cancelled_by_group.entry(write.group_id).or_default() += 1;
            }
        }
        for (group_id, cancelled_writes) in cancelled_by_group {
            let group = self
                .groups
                .get_mut(&group_id)
                .expect("cancelled write's group exists");
            group.remaining_writes = group
                .remaining_writes
                .checked_sub(cancelled_writes)
                .expect("cancelled writes contribute to group count");
        }
        let events = self.finish_completed_groups();
        self.assert_invariants();

        Ok(events)
    }

    fn has_ready_writes(&self) -> bool {
        self.writes
            .values()
            .any(|write| write.state == WriteState::Ready)
    }

    fn group_count(&self) -> usize {
        self.groups.len()
    }

    #[cfg(feature = "chaos-testing")]
    fn lowest_group_ticket(&self) -> Option<u64> {
        self.groups.values().map(|group| group.tickets.start).min()
    }

    #[cfg(feature = "chaos-testing")]
    fn request_ticket_start(&self, request_id: RequestId) -> Option<u64> {
        let write = self.writes.get(&request_id)?;
        self.groups
            .get(&write.group_id)
            .map(|group| group.tickets.start)
    }

    fn outstanding_sqe_count(&self) -> usize {
        self.outstanding_sqes
    }

    fn inflight_group_count(&self) -> usize {
        self.groups
            .values()
            .filter(|group| {
                group.request_ids.iter().any(|request_id| {
                    self.writes.get(request_id).is_some_and(|write| {
                        matches!(write.state, WriteState::Submitted | WriteState::Ambiguous)
                    })
                })
            })
            .count()
    }

    fn is_idle(&self) -> bool {
        self.groups.is_empty() && self.staged_sqes == 0 && self.outstanding_sqes == 0
    }

    fn failed_group_results(&self, reason: &str) -> Vec<GroupWriteResult> {
        self.group_order
            .iter()
            .filter_map(|group_id| {
                self.groups.get(group_id).map(|group| GroupWriteResult {
                    group_id: group_id.0,
                    tickets: group.tickets.clone(),
                    write_count: group.request_ids.len() as u64,
                    write_bytes: group.file_range.end - group.file_range.start,
                    error: Some(reason.to_owned()),
                })
            })
            .collect()
    }

    fn poison_at(&mut self, ticket: u64) {
        self.poison_ticket = Some(
            self.poison_ticket
                .map_or(ticket, |current| current.min(ticket)),
        );
    }

    fn assert_invariants(&self) {
        debug_assert!(self.groups.len() <= MAX_INFLIGHT_GROUPS);
        debug_assert_eq!(self.group_order.len(), self.groups.len());
        debug_assert!(self.staged_sqes + self.outstanding_sqes <= MAX_OUTSTANDING_SQES);
        debug_assert!(self.stage_cursor < self.group_order.len().max(1));
        for pair in self.group_order.iter().collect::<Vec<_>>().windows(2) {
            let first = self.groups.get(pair[0]).expect("ordered group exists");
            let second = self.groups.get(pair[1]).expect("ordered group exists");
            debug_assert!(first.tickets.end <= second.tickets.start);
            debug_assert!(first.file_range.end <= second.file_range.start);
        }
    }
}

impl<B: WorkerBuffer> Drop for WorkerCore<B> {
    fn drop(&mut self) {
        for write in self.writes.values_mut() {
            if write.state.may_be_kernel_owned()
                && let Some(buffer) = write.buffer.take()
            {
                // SAFETY: the submission result may be ambiguous, so the
                // kernel may still be reading this allocation. Leaking it is
                // required to prevent DirectBuf::drop from freeing it early.
                std::mem::forget(buffer);
            }
        }
    }
}

struct SlotState {
    active_groups: usize,
    next_group_id: u64,
    closed: bool,
}

struct GroupSlots {
    state: Mutex<SlotState>,
    available: Condvar,
}

struct GroupPermit {
    slots: Arc<GroupSlots>,
}

impl Drop for GroupPermit {
    fn drop(&mut self) {
        let mut state = self.slots.state.lock();
        state.active_groups -= 1;
        self.slots.available.notify_one();
    }
}

enum WorkerCommand<B = DirectBuf> {
    Group {
        id: GroupId,
        group: WriteGroup<B>,
        permit: GroupPermit,
    },
    Shutdown(Sender<Result<(), String>>),
}

/// Wakes the worker when a command arrives while it is waiting for a CQE.
/// Only the worker reads the eventfd; submitters share its write side.
struct WorkerWake {
    fd: OwnedFd,
}

impl WorkerWake {
    fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: eventfd returned a fresh descriptor, now owned by this value.
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    fn signal(&self) -> io::Result<()> {
        let value = 1_u64;
        loop {
            // SAFETY: eventfd requires an eight-byte write; `value` remains
            // valid for the duration of this call.
            let written = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    (&value as *const u64).cast(),
                    std::mem::size_of::<u64>(),
                )
            };
            if written == std::mem::size_of::<u64>() as isize {
                return Ok(());
            }
            if written >= 0 {
                return Err(io::Error::other("short WAL worker wakeup write"));
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }

    fn drain(&self) -> io::Result<()> {
        let mut value = 0_u64;
        loop {
            // SAFETY: eventfd requires an eight-byte read into live storage.
            let read = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    (&mut value as *mut u64).cast(),
                    std::mem::size_of::<u64>(),
                )
            };
            if read == std::mem::size_of::<u64>() as isize {
                return Ok(());
            }
            if read >= 0 {
                return Err(io::Error::other("short WAL worker wakeup read"));
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }
}

/// A handle to one dedicated WAL write thread. The thread creates and owns its
/// registered ring and retains the WAL file until it has reaped all CQEs.
pub(crate) struct IoWorker<B: WorkerBuffer = DirectBuf> {
    commands: Sender<WorkerCommand<B>>,
    slots: Arc<GroupSlots>,
    wake: Arc<WorkerWake>,
    sync_progress: Arc<WalSyncProgress>,
    completions: Option<Receiver<GroupWriteResult>>,
    join: Option<JoinHandle<Result<()>>>,
    shutdown_sent: bool,
}

/// Cloneable submission handle for the worker. Completion ownership remains
/// with the WAL durability coordinator.
pub(crate) struct IoWorkerClient<B: WorkerBuffer> {
    commands: Sender<WorkerCommand<B>>,
    slots: Arc<GroupSlots>,
    wake: Arc<WorkerWake>,
    sync_progress: Arc<WalSyncProgress>,
    marker: PhantomData<fn() -> B>,
}

impl<B: WorkerBuffer> Clone for IoWorkerClient<B> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            slots: Arc::clone(&self.slots),
            wake: Arc::clone(&self.wake),
            sync_progress: Arc::clone(&self.sync_progress),
            marker: PhantomData,
        }
    }
}

impl<B: WorkerBuffer> IoWorker<B> {
    pub(crate) fn spawn(wal_file: Arc<File>, buffer_pool: Arc<ArrayQueue<B>>) -> Result<Self> {
        let (command_tx, command_rx) = unbounded();
        let (completion_tx, completion_rx) = unbounded();
        let (startup_tx, startup_rx) = bounded::<Result<()>>(1);
        let slots = Arc::new(GroupSlots {
            state: Mutex::new(SlotState {
                active_groups: 0,
                next_group_id: 0,
                closed: false,
            }),
            available: Condvar::new(),
        });
        let worker_slots = Arc::clone(&slots);
        let sync_progress = Arc::new(WalSyncProgress::default());
        let worker_sync_progress = Arc::clone(&sync_progress);
        let worker_file = Arc::clone(&wal_file);
        let wake = Arc::new(WorkerWake::new().context("failed to create WAL worker wakeup")?);
        let worker_wake = Arc::clone(&wake);
        let join = thread::Builder::new()
            .name("wal-io-worker".to_owned())
            .spawn(move || {
                let mut ring = match io_uring::IoUring::new(RING_SIZE as u32) {
                    Ok(ring) => ring,
                    Err(error) => {
                        let _ = startup_tx
                            .send(Err(anyhow!(error).context("failed to create WAL io_uring")));
                        return Err(anyhow!("WAL io_uring initialization failed"));
                    }
                };
                if let Err(error) = ring.submitter().register_files(&[worker_file.as_raw_fd()]) {
                    let _ = startup_tx.send(Err(
                        anyhow!(error).context("failed to register WAL file with io_uring")
                    ));
                    return Err(anyhow!("WAL io_uring file registration failed"));
                }
                startup_tx
                    .send(Ok(()))
                    .map_err(|_| anyhow!("WAL worker startup receiver was dropped"))?;

                run_worker(
                    &mut ring,
                    buffer_pool,
                    command_rx,
                    completion_tx,
                    worker_slots,
                    &worker_wake,
                    worker_sync_progress,
                )
            })
            .context("failed to spawn WAL I/O worker")?;

        match startup_rx
            .recv()
            .context("WAL worker exited during startup")?
        {
            Ok(()) => Ok(Self {
                commands: command_tx,
                slots,
                wake,
                sync_progress,
                completions: Some(completion_rx),
                join: Some(join),
                shutdown_sent: false,
            }),
            Err(error) => {
                let _ = join.join();
                Err(error).context("failed to start WAL I/O worker")
            }
        }
    }

    /// Queue a preallocated group. Backpressure counts queued and in-flight
    /// groups together, not just groups already submitted to the ring.
    pub(crate) fn submit_group(&self, group: WriteGroup<B>) -> Result<u64> {
        self.client().submit_group(group)
    }

    pub(crate) fn client(&self) -> IoWorkerClient<B> {
        IoWorkerClient {
            commands: self.commands.clone(),
            slots: Arc::clone(&self.slots),
            wake: Arc::clone(&self.wake),
            sync_progress: Arc::clone(&self.sync_progress),
            marker: PhantomData,
        }
    }

    pub(crate) fn sync_progress(&self) -> Arc<WalSyncProgress> {
        Arc::clone(&self.sync_progress)
    }

    pub(crate) fn take_completions(&mut self) -> Receiver<GroupWriteResult> {
        self.completions
            .take()
            .expect("WAL worker completions may be transferred only once")
    }

    pub(crate) fn completions(&self) -> &Receiver<GroupWriteResult> {
        self.completions
            .as_ref()
            .expect("WAL worker completions were transferred")
    }

    pub(crate) fn close(mut self) -> Result<()> {
        self.shutdown_and_join()
    }

    fn shutdown_and_join(&mut self) -> Result<()> {
        let mut ack_receiver = None;
        let mut wake_result = Ok(());
        if !self.shutdown_sent {
            let (ack_tx, ack_rx) = bounded(1);
            let mut state = self.slots.state.lock();
            state.closed = true;
            self.slots.available.notify_all();
            let send_result = self.commands.send(WorkerCommand::Shutdown(ack_tx));
            drop(state);
            self.shutdown_sent = true;
            if send_result.is_ok() {
                ack_receiver = Some(ack_rx);
                wake_result = self
                    .wake
                    .signal()
                    .context("failed to wake WAL I/O worker for shutdown");
            }
        }

        let ack_result = if let Some(receiver) = ack_receiver {
            match receiver.recv() {
                Ok(Ok(())) => Ok(()),
                Ok(Err(message)) => Err(anyhow!(message)),
                Err(error) => Err(anyhow!(
                    "WAL worker exited before acknowledging shutdown: {error}"
                )),
            }
        } else {
            Ok(())
        };
        let join_result = match self.join.take() {
            Some(join) => join
                .join()
                .map_err(|_| anyhow!("WAL I/O worker panicked"))?,
            None => Ok(()),
        };

        wake_result.and(ack_result).and(join_result)
    }
}

impl<B: WorkerBuffer> IoWorkerClient<B> {
    /// Queue a preallocated group. Backpressure counts queued and in-flight
    /// groups together, not just groups already submitted to the ring.
    pub(crate) fn submit_group(&self, group: WriteGroup<B>) -> Result<u64> {
        let tickets = group.tickets.clone();
        for write in &group.writes {
            ensure!(
                write.write_len == write.buffer.len() && write.write_len <= write.buffer.cap(),
                "WAL write length exceeds its owned DirectBuf"
            );
        }
        let mut state = self.slots.state.lock();
        while state.active_groups >= MAX_INFLIGHT_GROUPS && !state.closed {
            self.slots.available.wait(&mut state);
        }
        ensure!(!state.closed, "WAL I/O worker is closed");
        if let Some(id) = state.next_group_id.checked_add(1) {
            let group_id = GroupId(state.next_group_id);
            state.next_group_id = id;
            state.active_groups += 1;
            let permit = GroupPermit {
                slots: Arc::clone(&self.slots),
            };
            let send_result = self.commands.send(WorkerCommand::Group {
                id: group_id,
                group,
                permit,
            });
            drop(state);
            match send_result {
                Ok(()) => {
                    // The group is already owned by the worker. A wakeup
                    // failure cannot be reported as failed admission; the
                    // bounded poll timeout still drains the queue.
                    match self.wake.signal() {
                        Ok(()) => self.sync_progress.record_worker_eventfd_notification(),
                        Err(error) => log::error!("failed to wake WAL I/O worker: {error}"),
                    }
                    Ok(group_id.0)
                }
                Err(error) => {
                    drop(error.0);
                    Err(anyhow!("WAL I/O worker has stopped"))
                }
            }
        } else {
            Err(anyhow!("WAL I/O group identifier overflow"))
        }
        .with_context(|| format!("failed to enqueue WAL group for tickets {tickets:?}"))
    }
}

impl<B: WorkerBuffer> Drop for IoWorker<B> {
    fn drop(&mut self) {
        if self.join.is_some()
            && let Err(error) = self.shutdown_and_join()
        {
            log::error!("failed to shut down WAL I/O worker: {error:#}");
        }
    }
}

fn run_worker<B: WorkerBuffer>(
    ring: &mut io_uring::IoUring,
    buffer_pool: Arc<ArrayQueue<B>>,
    commands: Receiver<WorkerCommand<B>>,
    completions: Sender<GroupWriteResult>,
    slots: Arc<GroupSlots>,
    wake: &WorkerWake,
    sync_progress: Arc<WalSyncProgress>,
) -> Result<()> {
    let mut core = WorkerCore::<B>::new();
    let mut group_permits = HashMap::<GroupId, GroupPermit>::new();
    let mut ring_staged = VecDeque::<RequestId>::new();
    let mut shutdown_reply = None;
    let mut stopping = false;

    let result = run_worker_loop(
        ring,
        &buffer_pool,
        &commands,
        &completions,
        &slots,
        wake,
        &sync_progress,
        &mut core,
        &mut group_permits,
        &mut ring_staged,
        &mut shutdown_reply,
        &mut stopping,
    );
    if let Err(error) = &result {
        close_group_admission(&slots);
        if !ring_staged.is_empty() {
            let staged = ring_staged.iter().copied().collect::<Vec<_>>();
            if let Ok(events) = core.mark_ambiguous(&staged) {
                let io_event = sync_progress.begin_io_event();
                process_worker_events(
                    events,
                    &buffer_pool,
                    &mut group_permits,
                    &completions,
                    &io_event,
                );
            }
        }
        let reason = format!("WAL I/O worker stopped: {error:#}");
        fail_shutdown_reply(&mut shutdown_reply, &reason);
        for failed in core.failed_group_results(&reason) {
            let _ = completions.send(failed);
        }
        for command in commands.try_iter() {
            match command {
                WorkerCommand::Group { id, group, permit } => {
                    let tickets = group.tickets.clone();
                    drop(group);
                    drop(permit);
                    let _ = completions.send(GroupWriteResult {
                        group_id: id.0,
                        tickets,
                        write_count: 0,
                        write_bytes: 0,
                        error: Some(reason.clone()),
                    });
                }
                WorkerCommand::Shutdown(reply) => {
                    let _ = reply.send(Err(reason.clone()));
                }
            }
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
fn run_worker_loop<B: WorkerBuffer>(
    ring: &mut io_uring::IoUring,
    buffer_pool: &ArrayQueue<B>,
    commands: &Receiver<WorkerCommand<B>>,
    completions: &Sender<GroupWriteResult>,
    slots: &GroupSlots,
    wake: &WorkerWake,
    sync_progress: &WalSyncProgress,
    core: &mut WorkerCore<B>,
    group_permits: &mut HashMap<GroupId, GroupPermit>,
    ring_staged: &mut VecDeque<RequestId>,
    shutdown_reply: &mut Option<Sender<Result<(), String>>>,
    stopping: &mut bool,
) -> Result<()> {
    #[cfg(feature = "chaos-testing")]
    let mut deferred_lowest_group_completions = Vec::new();

    loop {
        if !*stopping {
            while core.group_count() < MAX_INFLIGHT_GROUPS {
                match commands.try_recv() {
                    Ok(WorkerCommand::Group { id, group, permit }) => {
                        enqueue_group_command(id, group, permit, core, group_permits, completions)?;
                    }
                    Ok(WorkerCommand::Shutdown(reply)) => {
                        *shutdown_reply = Some(reply);
                        *stopping = true;
                        break;
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        *stopping = true;
                        break;
                    }
                }
            }
        }

        if ring_staged.is_empty() {
            let submission_queue = ring.submission();
            let available = submission_queue.capacity() - submission_queue.len();
            drop(submission_queue);
            let submissions = core
                .stage_writes(available)
                .map_err(|error| anyhow!("invalid WAL worker scheduling state: {error:?}"))?;
            for submission in submissions {
                let sqe = io_uring::opcode::Write::new(
                    io_uring::types::Fixed(WAL_FD_INDEX),
                    submission.buffer_ptr,
                    submission.write_len as u32,
                )
                .offset(submission.file_offset)
                .build()
                .user_data(submission.request_id.0);
                // SAFETY: `core` owns the DirectBuf and retains it until its CQE is
                // consumed. The ring is private to this worker thread and has no
                // SQPOLL mode, so a pushed SQE cannot outlive this ownership path.
                unsafe {
                    ring.submission()
                        .push(&sqe)
                        .map_err(|_| anyhow!("WAL io_uring submission queue unexpectedly full"))?;
                }
                ring_staged.push_back(submission.request_id);
            }
        }

        if !ring_staged.is_empty() {
            flush_staged_writes(ring_staged, core, sync_progress, || ring.submit())?;
        }

        let completion_count = {
            let mut completion_event = sync_progress.begin_io_event();
            #[cfg(feature = "chaos-testing")]
            let mut completed = drain_completions(ring);
            #[cfg(not(feature = "chaos-testing"))]
            let completed = drain_completions(ring);
            let completion_at = completion_event.event_timestamp();
            completion_event.set_event_at(completion_at);
            completion_event.record_write_completions(completed.len() as u64);
            #[cfg(feature = "chaos-testing")]
            if crate::chaos::failpoint::defer_lowest_parallel_wal_group_completion()
                && let Some(lowest_ticket) = core.lowest_group_ticket()
            {
                let mut ready = Vec::with_capacity(completed.len());
                for completion in completed.drain(..) {
                    let request_id = RequestId(completion.0);
                    if core.request_ticket_start(request_id) == Some(lowest_ticket) {
                        deferred_lowest_group_completions.push(completion);
                    } else {
                        ready.push(completion);
                    }
                }
                completed = ready;
            }
            let completion_count = completed.len();
            process_completions(
                completed,
                core,
                buffer_pool,
                group_permits,
                completions,
                slots,
                &completion_event,
            )?;
            completion_count
        };

        if core.is_idle() && ring_staged.is_empty() {
            if *stopping {
                if let Some(reply) = shutdown_reply.take() {
                    let _ = reply.send(Ok(()));
                }
                return Ok(());
            }
            match commands.recv() {
                Ok(WorkerCommand::Group { id, group, permit }) => {
                    enqueue_group_command(id, group, permit, core, group_permits, completions)?;
                }
                Ok(WorkerCommand::Shutdown(reply)) => {
                    *shutdown_reply = Some(reply);
                    *stopping = true;
                }
                Err(_) => *stopping = true,
            }
            wake.drain().context("failed to drain WAL worker wakeup")?;
            continue;
        }

        if completion_count == 0 && core.outstanding_sqe_count() > 0 && ring_staged.is_empty() {
            // Wake on either a later group or a CQE. Waiting only for a CQE
            // would serialize a group admitted after this wait begins.
            wait_for_worker_progress(ring.as_raw_fd(), wake)
                .context("failed while waiting for WAL I/O progress")?;
        }
    }
}

fn enqueue_group_command<B: WorkerBuffer>(
    id: GroupId,
    group: WriteGroup<B>,
    permit: GroupPermit,
    core: &mut WorkerCore<B>,
    group_permits: &mut HashMap<GroupId, GroupPermit>,
    completions: &Sender<GroupWriteResult>,
) -> Result<()> {
    let tickets = group.tickets.clone();
    match core.enqueue(group) {
        Ok(assigned_id) if assigned_id == id => {
            group_permits.insert(id, permit);
        }
        Ok(_) => bail!("WAL worker assigned an unexpected group identifier"),
        Err(WorkerError::Poisoned) => {
            let poison_ticket = core.poison_ticket.unwrap_or(tickets.start);
            drop(permit);
            let _ = completions.send(GroupWriteResult {
                group_id: id.0,
                tickets,
                write_count: 0,
                write_bytes: 0,
                error: Some(format!("WAL poisoned at ticket {poison_ticket}")),
            });
        }
        Err(error) => {
            drop(permit);
            let _ = completions.send(GroupWriteResult {
                group_id: id.0,
                tickets,
                write_count: 0,
                write_bytes: 0,
                error: Some(format!("WAL worker rejected group: {error:?}")),
            });
            bail!("WAL worker rejected group: {error:?}");
        }
    }

    Ok(())
}

fn close_group_admission(slots: &GroupSlots) {
    let mut state = slots.state.lock();
    if !state.closed {
        state.closed = true;
        slots.available.notify_all();
    }
}

fn flush_staged_writes<B: WorkerBuffer>(
    staged: &mut VecDeque<RequestId>,
    core: &mut WorkerCore<B>,
    sync_progress: &WalSyncProgress,
    mut submit: impl FnMut() -> io::Result<usize>,
) -> Result<()> {
    while !staged.is_empty() {
        let mut io_event = sync_progress.begin_io_event();
        let (submitted, submitted_at) =
            submit_staged_writes(staged, core, &mut submit, || io_event.event_timestamp())?;
        io_event.set_event_at(submitted_at);
        io_event.record_write_submission(
            submitted as u64,
            core.inflight_group_count() as u64,
            core.outstanding_sqe_count() as u64,
        );
        ensure!(
            submitted > 0,
            "io_uring made no progress with staged WAL writes"
        );
    }

    Ok(())
}

fn submit_staged_writes<B: WorkerBuffer>(
    staged: &mut VecDeque<RequestId>,
    core: &mut WorkerCore<B>,
    mut submit: impl FnMut() -> io::Result<usize>,
    mut timestamp: impl FnMut() -> Option<std::time::Instant>,
) -> Result<(usize, Option<std::time::Instant>)> {
    let mut submitted_at = None;
    let submitted = retry_interrupted(|| {
        let result = submit();
        if result.is_ok() {
            submitted_at = timestamp();
        }
        result
    })
    .context("failed to submit WAL writes to io_uring")?;
    mark_submitted_prefix(submitted, staged, core)?;
    Ok((submitted, submitted_at))
}

fn wait_for_worker_progress(ring_fd: RawFd, wake: &WorkerWake) -> io::Result<bool> {
    let mut fds = [
        libc::pollfd {
            fd: ring_fd,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    loop {
        // A timeout is only a fallback if eventfd signaling fails after a
        // command was queued; normal progress is driven by readiness.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, 1000) };
        if ready >= 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    if fds
        .iter()
        .any(|fd| fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0)
    {
        return Err(io::Error::other("WAL worker progress fd failed"));
    }
    let command_ready = fds[1].revents & libc::POLLIN != 0;
    if command_ready {
        wake.drain()?;
    }
    Ok(command_ready)
}

fn mark_submitted_prefix<B: WorkerBuffer>(
    submitted: usize,
    staged: &mut VecDeque<RequestId>,
    core: &mut WorkerCore<B>,
) -> Result<()> {
    ensure!(
        submitted <= staged.len(),
        "io_uring submitted more WAL writes than the worker staged"
    );
    for _ in 0..submitted {
        let request_id = staged
            .pop_front()
            .expect("validated submitted prefix remains staged");
        core.mark_submitted(request_id)
            .map_err(|error| anyhow!("invalid WAL worker submission state: {error:?}"))?;
    }

    Ok(())
}

fn drain_completions(ring: &mut io_uring::IoUring) -> Vec<(u64, i32)> {
    ring.completion()
        .map(|cqe| (cqe.user_data(), cqe.result()))
        .collect()
}

fn process_completions<B: WorkerBuffer>(
    completed: Vec<(u64, i32)>,
    core: &mut WorkerCore<B>,
    buffer_pool: &ArrayQueue<B>,
    group_permits: &mut HashMap<GroupId, GroupPermit>,
    completion_tx: &Sender<GroupWriteResult>,
    slots: &GroupSlots,
    io_event: &WalIoEvent<'_>,
) -> Result<()> {
    for (user_data, cqe_result) in completed {
        let request_id = RequestId(user_data);
        let events = core
            .complete_write(request_id, cqe_result)
            .map_err(|error| anyhow!("invalid or stale WAL io_uring completion: {error:?}"))?;
        if core.poison_ticket.is_some() {
            close_group_admission(slots);
        }
        process_worker_events(events, buffer_pool, group_permits, completion_tx, io_event);
    }

    Ok(())
}

fn process_worker_events<B: WorkerBuffer>(
    events: Vec<WorkerEvent<B>>,
    buffer_pool: &ArrayQueue<B>,
    group_permits: &mut HashMap<GroupId, GroupPermit>,
    completion_tx: &Sender<GroupWriteResult>,
    io_event: &WalIoEvent<'_>,
) {
    for event in events {
        match event {
            WorkerEvent::BufferRetired(buffer) => {
                buffer.retire(buffer_pool);
            }
            WorkerEvent::GroupFinished(result) => {
                #[cfg(all(test, feature = "chaos-testing"))]
                let (result, injected_failure) = inject_parallel_wal_group_failure(result);

                io_event.record_group_completion(&result);
                group_permits.remove(&GroupId(result.group_id));
                #[cfg(all(test, feature = "chaos-testing"))]
                if completion_tx.send(result).is_ok() && injected_failure {
                    crate::chaos::failpoint::note_parallel_wal_injected_group_failure();
                }
                #[cfg(not(all(test, feature = "chaos-testing")))]
                let _ = completion_tx.send(result);
            }
        }
    }
}

#[cfg(all(test, feature = "chaos-testing"))]
fn inject_parallel_wal_group_failure(mut result: GroupWriteResult) -> (GroupWriteResult, bool) {
    let injected_failure =
        match crate::chaos::failpoint::parallel_wal_group_completion_error(result.tickets.start) {
            Some(error) => {
                result.error = Some(error);
                true
            }
            None => false,
        };

    (result, injected_failure)
}

fn fail_shutdown_reply(reply: &mut Option<Sender<Result<(), String>>>, reason: &str) {
    if let Some(reply) = reply.take() {
        let _ = reply.send(Err(reason.to_owned()));
    }
}

fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::{
        fs::File,
        os::{
            fd::AsRawFd,
            unix::{fs::OpenOptionsExt, net::UnixStream},
        },
        thread,
        time::Duration,
    };

    #[cfg(feature = "bench")]
    use super::SyncProgressSnapshot;
    use super::{
        DirectBuf, GroupId, GroupPermit, GroupSlots, GroupWriteResult, IoWorker, RequestId,
        SlotState, WalSyncProgress, WorkerCommand, WorkerCore, WorkerError, WorkerEvent,
        WorkerWake, WriteBuffer, WriteGroup, close_group_admission, enqueue_group_command,
        fail_shutdown_reply, flush_staged_writes, retry_interrupted, wait_for_worker_progress,
    };
    use crossbeam_queue::ArrayQueue;
    use parking_lot::{Condvar, Mutex};

    struct DropProbe {
        bytes: Box<[u8]>,
        drops: Arc<AtomicUsize>,
    }

    impl DropProbe {
        fn new(len: usize, drops: Arc<AtomicUsize>) -> Self {
            Self {
                bytes: vec![0; len].into_boxed_slice(),
                drops,
            }
        }
    }

    impl super::WorkerBuffer for DropProbe {
        fn as_ptr(&self) -> *const u8 {
            self.bytes.as_ptr()
        }

        fn len(&self) -> usize {
            self.bytes.len()
        }

        fn cap(&self) -> usize {
            self.bytes.len()
        }

        fn retire(self, _pool: &ArrayQueue<Self>) {}
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn make_group(
        tickets: std::ops::Range<u64>,
        file_offset: u64,
        write_count: usize,
        len: usize,
        drops: &Arc<AtomicUsize>,
    ) -> WriteGroup<DropProbe> {
        make_group_with_capacity(tickets, file_offset, write_count, len, len, drops)
    }

    fn make_group_with_capacity(
        tickets: std::ops::Range<u64>,
        file_offset: u64,
        write_count: usize,
        write_len: usize,
        allocation_len: usize,
        drops: &Arc<AtomicUsize>,
    ) -> WriteGroup<DropProbe> {
        let writes = (0..write_count)
            .map(|index| {
                WriteBuffer::new(
                    DropProbe::new(allocation_len, Arc::clone(drops)),
                    file_offset + (index * write_len) as u64,
                    write_len,
                )
            })
            .collect();
        WriteGroup::new(tickets, writes).expect("valid group")
    }

    fn submitted_ids(core: &mut WorkerCore<DropProbe>, available: usize) -> Vec<RequestId> {
        let submissions = core.stage_writes(available).expect("stage writes");
        let request_ids = submissions
            .iter()
            .map(|submission| {
                assert!(!submission.buffer_ptr.is_null());
                let _identity = (submission.group_id, submission.buffer_id);
                submission.request_id
            })
            .collect::<Vec<_>>();
        for request_id in &request_ids {
            core.mark_submitted(*request_id)
                .expect("submit staged write");
        }

        request_ids
    }

    fn take_group_result(events: Vec<WorkerEvent<DropProbe>>) -> GroupWriteResult {
        events
            .into_iter()
            .find_map(|event| match event {
                WorkerEvent::GroupFinished(result) => Some(result),
                WorkerEvent::BufferRetired(buffer) => {
                    drop(buffer);
                    None
                }
            })
            .expect("group completion event")
    }

    fn io_uring_unavailable(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .and_then(|error| error.raw_os_error())
                .is_some_and(|code| matches!(code, libc::EPERM | libc::ENOMEM | libc::ENOSYS))
        })
    }

    #[test]
    fn io_worker_writes_groups_through_its_owned_ring() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("wal");
        File::create(&path)
            .expect("create buffered WAL file")
            .set_len(8192)
            .expect("size WAL file");
        let file = File::options()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .expect("open direct WAL handle");
        let buffer_pool = Arc::new(super::ArrayQueue::new(4));
        let worker = match IoWorker::spawn(Arc::new(file), buffer_pool) {
            Ok(worker) => worker,
            Err(error) if io_uring_unavailable(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return;
            }
            Err(error) => panic!("failed to start WAL I/O worker: {error:#}"),
        };

        let mut first_buffer = DirectBuf::new(4096);
        first_buffer.as_mut_slice().fill(0xa1);
        first_buffer.set_len(4096);
        let mut second_buffer = DirectBuf::new(4096);
        second_buffer.as_mut_slice().fill(0xb2);
        second_buffer.set_len(4096);
        worker
            .submit_group(
                WriteGroup::new(0..1, vec![WriteBuffer::new(first_buffer, 0, 4096)])
                    .expect("build first group"),
            )
            .expect("submit first group");
        worker
            .submit_group(
                WriteGroup::new(1..2, vec![WriteBuffer::new(second_buffer, 4096, 4096)])
                    .expect("build second group"),
            )
            .expect("submit second group");

        let first_result = worker
            .completions()
            .recv_timeout(Duration::from_secs(5))
            .expect("first group completion");
        let second_result = worker
            .completions()
            .recv_timeout(Duration::from_secs(5))
            .expect("second group completion");
        assert_eq!(first_result.error, None);
        assert_eq!(second_result.error, None);
        worker.close().expect("close worker");

        let bytes = std::fs::read(path).expect("read WAL contents");
        assert!(bytes[..4096].iter().all(|byte| *byte == 0xa1));
        assert!(bytes[4096..].iter().all(|byte| *byte == 0xb2));
    }

    #[test]
    fn worker_stages_two_groups_before_waiting_for_either_group() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group(0..1, 4096, 2, 4096, &drops))
            .expect("enqueue first group");
        core.enqueue(make_group(1..2, 12_288, 2, 4096, &drops))
            .expect("enqueue second group");

        let requests = submitted_ids(&mut core, 256);

        assert_eq!(requests.len(), 4);
        assert_eq!(core.inflight_group_count(), 2);
        assert_eq!(core.outstanding_sqe_count(), 4);

        for request_id in requests.into_iter().rev() {
            let events = core
                .complete_write(request_id, 4096)
                .expect("complete full-length write");
            for event in events {
                if let WorkerEvent::BufferRetired(buffer) = event {
                    drop(buffer);
                }
            }
        }
        assert!(core.is_idle());
        assert_eq!(drops.load(Ordering::Acquire), 4);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn sync_progress_records_frontiers_and_write_activity_during_sync() {
        let progress = WalSyncProgress::default();
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        profile.set_wal_sync_diagnostics_enabled(true);
        progress.set_profile(Arc::clone(&profile));

        // The later group finishes first. It must not move the contiguous
        // written frontier past the unresolved first group.
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 1,
                tickets: 1..2,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            false,
            Some(std::time::Instant::now()),
        );
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 0,
                tickets: 0..1,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            false,
            Some(std::time::Instant::now()),
        );

        let sync_start = progress.begin_sync(2, 0);
        // Snapshot preparation happens before the syscall window and must not
        // inflate its activity counts.
        progress.record_write_submission(1, 1, 1, false, None);
        progress.start_sync_activity();
        let syscall_started_at = std::time::Instant::now();
        let event_at = std::time::Instant::now();
        progress.record_write_submission(1, 1, 1, true, Some(event_at));
        progress.record_write_completions(1, true, Some(event_at));
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 2,
                tickets: 2..3,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            true,
            Some(event_at),
        );
        let (sync_end, finished_at) =
            progress.finish_sync(Some(syscall_started_at), Some(std::time::Instant::now()));

        assert!(finished_at.is_some());
        assert_eq!(sync_start.written_frontier_start, 2);
        assert_eq!(sync_start.groups_covered, 2);
        assert_eq!(sync_end.written_frontier_end, 3);
        assert_eq!(sync_end.write_sqes_submitted, 1);
        assert_eq!(sync_end.write_cqes_completed, 1);
        assert_eq!(sync_end.groups_completed, 1);

        // Events observed after the sync window closes must not be attributed
        // to that fdatasync, even if they arrive before the snapshot is read.
        progress.record_write_submission(1, 1, 1, false, None);
        progress.record_write_completions(1, false, None);
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 3,
                tickets: 3..4,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            false,
            Some(std::time::Instant::now()),
        );
        let next_sync = progress.begin_sync(4, 0);
        progress.start_sync_activity();
        let syscall_started_at = std::time::Instant::now();
        let (after_sync, _) =
            progress.finish_sync(Some(syscall_started_at), Some(std::time::Instant::now()));
        assert_eq!(next_sync.groups_covered, 4);
        assert_eq!(after_sync.write_sqes_submitted, 0);
        assert_eq!(after_sync.write_cqes_completed, 0);
        assert_eq!(after_sync.groups_completed, 0);

        let profile = profile.snapshot();
        assert_eq!(profile.wal_inflight_groups_max, 1);
        assert_eq!(profile.wal_outstanding_write_sqes_max, 1);
        assert_eq!(profile.wal_cqe_count, 2);
        assert_eq!(profile.wal_commit_groups, 4);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn sync_progress_keeps_aggregate_metrics_without_detailed_diagnostics() {
        let progress = WalSyncProgress::default();
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        progress.set_profile(Arc::clone(&profile));
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 0,
                tickets: 0..1,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            false,
            Some(std::time::Instant::now()),
        );

        let sync_start = progress.begin_sync(1, 0);
        progress.start_sync_activity();
        let syscall_started_at = std::time::Instant::now();
        progress.record_write_submission(1, 1, 1, false, None);
        progress.record_write_completions(1, false, None);
        let (sync_end, finished_at) =
            progress.finish_sync(Some(syscall_started_at), Some(std::time::Instant::now()));

        assert_eq!(sync_start, SyncProgressSnapshot::default());
        assert_eq!(sync_end, SyncProgressSnapshot::default());
        assert!(finished_at.is_some());

        let profile = profile.snapshot();
        assert_eq!(profile.wal_commit_groups, 1);
        assert_eq!(profile.wal_inflight_groups_max, 1);
        assert_eq!(profile.wal_outstanding_write_sqes_max, 1);
        assert_eq!(profile.wal_cqe_count, 1);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn sync_finish_waits_for_tagged_io_event_accounting() {
        let progress = Arc::new(WalSyncProgress::default());
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        profile.set_wal_sync_diagnostics_enabled(true);
        progress.set_profile(profile);

        progress.begin_sync(1, 0);
        progress.start_sync_activity();
        let syscall_started_at = std::time::Instant::now();
        let io_event = progress.begin_io_event();
        assert!(io_event.during_sync());
        let operation_at = std::time::Instant::now();

        let finish_progress = Arc::clone(&progress);
        let finish = thread::spawn(move || {
            finish_progress.finish_sync(Some(syscall_started_at), Some(std::time::Instant::now()))
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while progress.sync_active.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "sync finish did not close the event window"
            );
            thread::yield_now();
        }

        // The operation happened during sync but metrics were delayed. Finish
        // waits for the tag and uses the operation timestamp, not accounting time.
        progress.record_write_submission(1, 1, 1, io_event.during_sync(), Some(operation_at));
        progress.record_write_completions(1, io_event.during_sync(), Some(operation_at));
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 0,
                tickets: 0..1,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            io_event.during_sync(),
            Some(operation_at),
        );
        drop(io_event);

        let (snapshot, _) = finish.join().expect("sync finish thread");
        assert_eq!(snapshot.write_sqes_submitted, 1);
        assert_eq!(snapshot.write_cqes_completed, 1);
        assert_eq!(snapshot.groups_completed, 1);
        assert_eq!(snapshot.written_frontier_end, 1);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn sync_finish_excludes_io_that_runs_after_the_sync_window() {
        let progress = Arc::new(WalSyncProgress::default());
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        profile.set_wal_sync_diagnostics_enabled(true);
        progress.set_profile(profile);

        progress.begin_sync(1, 0);
        progress.start_sync_activity();
        let syscall_started_at = std::time::Instant::now();
        let io_event = progress.begin_io_event();
        assert!(io_event.during_sync());

        let finished_at = std::time::Instant::now();
        let finish_progress = Arc::clone(&progress);
        let finish = thread::spawn(move || {
            finish_progress.finish_sync(Some(syscall_started_at), Some(finished_at))
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while progress.sync_active.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "sync finish did not close the event window"
            );
            thread::yield_now();
        }

        // The old tag is still true, but the actual operation timestamp is
        // after fdatasync returned and must not be counted in its observation.
        let operation_at = std::time::Instant::now();
        progress.record_write_submission(1, 1, 1, io_event.during_sync(), Some(operation_at));
        progress.record_write_completions(1, io_event.during_sync(), Some(operation_at));
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 0,
                tickets: 0..1,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            io_event.during_sync(),
            Some(operation_at),
        );
        drop(io_event);

        let (snapshot, _) = finish.join().expect("sync finish thread");
        assert_eq!(snapshot.write_sqes_submitted, 0);
        assert_eq!(snapshot.write_cqes_completed, 0);
        assert_eq!(snapshot.groups_completed, 0);
        assert_eq!(snapshot.written_frontier_end, 0);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn sync_finish_excludes_io_between_window_setup_and_syscall_start() {
        let progress = WalSyncProgress::default();
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        profile.set_wal_sync_diagnostics_enabled(true);
        progress.set_profile(profile);

        progress.begin_sync(1, 0);
        progress.start_sync_activity();
        let io_event = progress.begin_io_event();
        assert!(io_event.during_sync());
        let operation_at = std::time::Instant::now();
        progress.record_write_submission(1, 1, 1, io_event.during_sync(), Some(operation_at));
        progress.record_write_completions(1, io_event.during_sync(), Some(operation_at));
        progress.record_group_completion(
            &GroupWriteResult {
                group_id: 0,
                tickets: 0..1,
                write_count: 1,
                write_bytes: 4096,
                error: None,
            },
            io_event.during_sync(),
            Some(operation_at),
        );
        drop(io_event);

        let syscall_started_at = std::time::Instant::now();
        let (snapshot, _) =
            progress.finish_sync(Some(syscall_started_at), Some(std::time::Instant::now()));

        assert_eq!(snapshot.written_frontier_start, 1);
        assert_eq!(snapshot.written_frontier_end, 1);
        assert_eq!(snapshot.write_sqes_submitted, 0);
        assert_eq!(snapshot.write_cqes_completed, 0);
        assert_eq!(snapshot.groups_completed, 0);
    }

    #[test]
    fn worker_bounds_eight_groups_and_256_outstanding_sqes() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        let bytes_per_group = 33 * 4096;
        for group_index in 0..8 {
            core.enqueue(make_group(
                group_index..group_index + 1,
                4096 + group_index * bytes_per_group,
                33,
                4096,
                &drops,
            ))
            .expect("enqueue group within in-flight limit");
        }
        assert!(matches!(
            core.enqueue(make_group(
                8..9,
                4096 + 8 * bytes_per_group,
                1,
                4096,
                &drops
            )),
            Err(WorkerError::TooManyGroups)
        ));

        let first_wave = submitted_ids(&mut core, 512);
        assert_eq!(first_wave.len(), 256);
        assert_eq!(core.group_count(), 8);
        assert_eq!(core.inflight_group_count(), 8);
        assert_eq!(core.outstanding_sqe_count(), 256);
        assert!(core.stage_writes(1).expect("ring at capacity").is_empty());

        for request_id in first_wave {
            for event in core
                .complete_write(request_id, 4096)
                .expect("complete first wave")
            {
                if let WorkerEvent::BufferRetired(buffer) = event {
                    drop(buffer);
                }
            }
        }

        let second_wave = submitted_ids(&mut core, 256);
        assert_eq!(second_wave.len(), 8);
        assert_eq!(core.inflight_group_count(), 8);
        assert_eq!(core.outstanding_sqe_count(), 8);
        for request_id in second_wave {
            for event in core
                .complete_write(request_id, 4096)
                .expect("complete second wave")
            {
                if let WorkerEvent::BufferRetired(buffer) = event {
                    drop(buffer);
                }
            }
        }

        assert!(core.is_idle());
        assert_eq!(drops.load(Ordering::Acquire), 8 * 33 + 1);
    }

    #[test]
    fn oversized_buffer_retires_before_the_rest_of_its_group() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group_with_capacity(
            0..1,
            4096,
            2,
            240 * 1024 * 1024,
            4096,
            &drops,
        ))
        .expect("enqueue oversized group");
        let requests = submitted_ids(&mut core, 256);

        let events = core
            .complete_write(requests[0], 240 * 1024 * 1024)
            .expect("complete first write");
        let mut retired = 0;
        let mut group_finished = false;
        for event in events {
            match event {
                WorkerEvent::BufferRetired(buffer) => {
                    retired += 1;
                    drop(buffer);
                }
                WorkerEvent::GroupFinished(_) => group_finished = true,
            }
        }

        assert_eq!(retired, 1);
        assert!(!group_finished);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(core.outstanding_sqe_count(), 1);

        let events = core
            .complete_write(requests[1], 240 * 1024 * 1024)
            .expect("complete second write");
        let result = take_group_result(events);
        assert_eq!(result.error, None);
        assert_eq!(drops.load(Ordering::Acquire), 2);
    }

    #[test]
    fn ambiguous_submission_close_error_and_drop_do_not_free_kernel_owned_buffer() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group_with_capacity(
            0..1,
            4096,
            1,
            240 * 1024 * 1024,
            4096,
            &drops,
        ))
        .expect("enqueue oversized group");
        let staged = core.stage_writes(1).expect("stage oversized write");
        let request_id = staged[0].request_id;
        let events = core.mark_ambiguous(&[request_id]).expect("mark ambiguous");
        assert!(events.is_empty());

        assert!(matches!(
            core.close(),
            Err(WorkerError::RequestNotOutstanding)
        ));
        drop(core);

        assert_eq!(drops.load(Ordering::Acquire), 0);
    }

    #[test]
    fn partial_submission_keeps_unsubmitted_sqe_staged_without_resubmitting_it() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group(0..1, 4096, 2, 4096, &drops))
            .expect("enqueue group");
        let staged = core.stage_writes(2).expect("stage two writes");

        core.mark_submitted(staged[0].request_id)
            .expect("submit partial prefix");
        assert_eq!(core.outstanding_sqe_count(), 1);
        assert_eq!(core.staged_sqes, 1);
        assert!(core.stage_writes(2).expect("stage remaining").is_empty());

        core.mark_submitted(staged[1].request_id)
            .expect("submit remaining SQE");
        assert_eq!(core.outstanding_sqe_count(), 2);
    }

    #[test]
    fn short_or_negative_cqe_poisons_group_and_cancels_later_unsubmitted_groups() {
        for cqe_result in [-libc::EIO, 2048] {
            let drops = Arc::new(AtomicUsize::new(0));
            let mut core = WorkerCore::new();
            core.enqueue(make_group(0..1, 4096, 1, 4096, &drops))
                .expect("enqueue first group");
            core.enqueue(make_group(1..2, 8192, 1, 4096, &drops))
                .expect("enqueue second group");
            let requests = submitted_ids(&mut core, 1);

            let events = core
                .complete_write(requests[0], cqe_result)
                .expect("complete failed write");
            let result = take_group_result(events);

            assert_eq!(result.tickets, 0..1);
            assert!(result.error.is_some());
            assert_eq!(core.poison_ticket, Some(0));
            assert_eq!(core.group_count(), 0);
            assert_eq!(drops.load(Ordering::Acquire), 2);
        }
    }

    #[test]
    fn poisoned_queued_group_does_not_abort_an_earlier_inflight_group() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group(0..1, 4096, 1, 4096, &drops))
            .expect("enqueue prefix group");
        core.enqueue(make_group(1..2, 8192, 1, 4096, &drops))
            .expect("enqueue failing group");
        let requests = submitted_ids(&mut core, 2);

        let failed_group = take_group_result(
            core.complete_write(requests[1], -libc::EIO)
                .expect("fail second group"),
        );
        assert_eq!(failed_group.tickets, 1..2);
        assert!(failed_group.error.is_some());
        assert_eq!(core.poison_ticket, Some(1));
        assert_eq!(core.group_count(), 1);

        let slots = Arc::new(GroupSlots {
            state: Mutex::new(SlotState {
                active_groups: 2,
                next_group_id: 4,
                closed: false,
            }),
            available: Condvar::new(),
        });
        close_group_admission(&slots);
        let permit = GroupPermit {
            slots: Arc::clone(&slots),
        };
        let (completion_tx, completion_rx) = crossbeam_channel::unbounded();
        let mut group_permits = std::collections::HashMap::new();
        enqueue_group_command(
            GroupId(2),
            make_group(2..3, 12_288, 1, 4096, &drops),
            GroupPermit {
                slots: Arc::clone(&slots),
            },
            &mut core,
            &mut group_permits,
            &completion_tx,
        )
        .expect("reject poisoned group without stopping the worker");
        enqueue_group_command(
            GroupId(3),
            make_group(3..4, 16_384, 1, 4096, &drops),
            permit,
            &mut core,
            &mut group_permits,
            &completion_tx,
        )
        .expect("reject later poisoned group without a ticket-hole failure");

        let first_rejected_group = completion_rx
            .recv()
            .expect("first post-poison group receives a failure result");
        let second_rejected_group = completion_rx
            .recv()
            .expect("later post-poison group receives a failure result");
        assert_eq!(first_rejected_group.tickets, 2..3);
        assert!(first_rejected_group.error.is_some());
        assert_eq!(second_rejected_group.tickets, 3..4);
        assert!(second_rejected_group.error.is_some());
        assert!(slots.state.lock().closed);
        assert_eq!(slots.state.lock().active_groups, 0);
        assert_eq!(core.group_count(), 1);
        assert_eq!(core.inflight_group_count(), 1);

        let prefix_group = take_group_result(
            core.complete_write(requests[0], 4096)
                .expect("complete pre-poison group"),
        );
        assert_eq!(prefix_group.tickets, 0..1);
        assert_eq!(prefix_group.error, None);
        assert!(core.is_idle());
        assert_eq!(drops.load(Ordering::Acquire), 4);
    }

    #[test]
    fn partial_submission_is_fully_accounted_before_processing_failure_cqe() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group(0..1, 4096, 1, 4096, &drops))
            .expect("enqueue prefix group");
        core.enqueue(make_group(1..2, 8192, 1, 4096, &drops))
            .expect("enqueue failing group");
        core.enqueue(make_group(2..3, 12_288, 1, 4096, &drops))
            .expect("enqueue later group");
        let submissions = core.stage_writes(3).expect("stage writes across groups");
        let requests = submissions
            .iter()
            .map(|submission| submission.request_id)
            .collect::<Vec<_>>();
        let mut ring_staged = requests.iter().copied().collect();
        let mut submission_counts = [2, 1].into_iter();
        let mut submit_attempts = 0;

        let sync_progress = WalSyncProgress::default();
        flush_staged_writes(&mut ring_staged, &mut core, &sync_progress, || {
            submit_attempts += 1;
            Ok(submission_counts.next().expect("expected partial submit"))
        })
        .expect("flush partial submissions before polling CQEs");

        assert_eq!(submit_attempts, 2);
        assert!(ring_staged.is_empty());
        assert_eq!(core.outstanding_sqe_count(), 3);
        assert!(
            requests
                .iter()
                .all(|request_id| core.writes[request_id].state == super::WriteState::Submitted)
        );

        let failed_group = take_group_result(
            core.complete_write(requests[1], -libc::EIO)
                .expect("process failure CQE after staged suffix was submitted"),
        );
        assert_eq!(failed_group.tickets, 1..2);
        assert!(failed_group.error.is_some());
        assert_eq!(core.poison_ticket, Some(1));

        let prefix_group = take_group_result(
            core.complete_write(requests[0], 4096)
                .expect("complete earlier group"),
        );
        assert_eq!(prefix_group.error, None);
        let later_group = take_group_result(
            core.complete_write(requests[2], 4096)
                .expect("complete submitted later group"),
        );
        assert!(later_group.error.is_some());
        assert!(core.is_idle());
        assert_eq!(drops.load(Ordering::Acquire), 3);
    }

    #[test]
    fn stale_completion_is_rejected_without_releasing_any_owned_buffer() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group(0..1, 4096, 1, 4096, &drops))
            .expect("enqueue group");
        let requests = submitted_ids(&mut core, 1);

        assert!(matches!(
            core.complete_write(RequestId(requests[0].0 + 100), 4096),
            Err(WorkerError::UnknownRequest)
        ));
        assert_eq!(drops.load(Ordering::Acquire), 0);
    }

    #[test]
    fn retry_interrupted_repeats_submit_until_it_stops_returning_eintr() {
        let mut attempts = 0;

        let result = retry_interrupted(|| {
            attempts += 1;
            if attempts < 3 {
                Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
            } else {
                Ok(7)
            }
        });

        assert_eq!(result.expect("submit retry succeeds"), 7);
        assert_eq!(attempts, 3);
    }

    #[test]
    fn later_group_wakes_worker_before_earlier_write_completes() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut core = WorkerCore::new();
        core.enqueue(make_group(0..1, 4096, 1, 4096, &drops))
            .expect("enqueue group");
        let first = submitted_ids(&mut core, 1);
        assert_eq!(core.outstanding_sqe_count(), 1);

        // The fake CQ descriptor is never written: only the later group may
        // wake the worker, while the first write remains outstanding.
        let (fake_cq, _peer) = UnixStream::pair().expect("fake CQ socket");
        let wake = Arc::new(WorkerWake::new().expect("eventfd"));
        let (group_tx, group_rx) = crossbeam_channel::unbounded();
        let producer_wake = Arc::clone(&wake);
        let producer_drops = Arc::clone(&drops);
        let producer = thread::spawn(move || {
            group_tx
                .send(make_group(1..2, 8192, 1, 4096, &producer_drops))
                .expect("enqueue later group");
            producer_wake.signal().expect("signal worker");
        });

        assert!(wait_for_worker_progress(fake_cq.as_raw_fd(), &wake).expect("worker wakes"));
        producer.join().expect("producer joins");
        core.enqueue(group_rx.try_recv().expect("later group queued"))
            .expect("worker accepts later group");
        let second = submitted_ids(&mut core, 1);
        assert_eq!(core.inflight_group_count(), 2);
        assert_eq!(core.outstanding_sqe_count(), 2);

        drop(
            core.complete_write(second[0], 4096)
                .expect("later write CQE"),
        );
        drop(
            core.complete_write(first[0], 4096)
                .expect("earlier write CQE"),
        );
        assert!(core.is_idle());
        assert_eq!(drops.load(Ordering::Acquire), 2);
    }

    #[test]
    fn worker_failure_reports_error_to_consumed_shutdown_request() {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let mut reply = Some(reply_tx);

        fail_shutdown_reply(&mut reply, "injected worker failure");

        assert!(reply.is_none());
        assert_eq!(
            reply_rx.recv().expect("shutdown reply remains connected"),
            Err("injected worker failure".to_owned())
        );
    }

    #[test]
    fn shutdown_joins_worker_even_when_acknowledgement_sender_disconnects() {
        let (command_tx, command_rx) = crossbeam_channel::unbounded();
        let join = thread::spawn(move || -> anyhow::Result<()> {
            if let Ok(WorkerCommand::Shutdown(reply)) = command_rx.recv() {
                drop(reply);
            }
            Ok(())
        });
        let slots = Arc::new(GroupSlots {
            state: Mutex::new(SlotState {
                active_groups: 0,
                next_group_id: 0,
                closed: false,
            }),
            available: Condvar::new(),
        });
        let (_completion_tx, completions) = crossbeam_channel::unbounded();
        let mut worker: IoWorker<DirectBuf> = IoWorker {
            commands: command_tx,
            slots,
            wake: Arc::new(WorkerWake::new().expect("eventfd")),
            sync_progress: Arc::new(WalSyncProgress::default()),
            completions: Some(completions),
            join: Some(join),
            shutdown_sent: false,
        };

        assert!(worker.shutdown_and_join().is_err());
        assert!(worker.join.is_none());
    }
}
