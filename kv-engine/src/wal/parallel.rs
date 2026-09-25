//! Test-only models for the dedicated WAL pipeline.
//!
//! `PipelineState` is a deterministic model with no I/O or synchronization
//! primitives. The admission model uses locks and an atomic counter to exercise
//! producer/packer concurrency without connecting to the production WAL.

mod admission;

use std::ops::Range;

use super::{MAX_WAL_FILE_SIZE, PREALLOC_BLOCK};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GroupId(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SyncId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteStatus {
    NotSubmitted,
    Submitted,
    Completed,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteCompletion {
    FullLength,
    Short,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncResult {
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TicketResult {
    Pending,
    Durable,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateError {
    Poisoned,
    InvalidTicketRange,
    NonContiguousTickets,
    InvalidFileRange,
    NonContiguousFileRange,
    EmptyGroup,
    UnknownGroup,
    UnknownWrite,
    WriteAlreadySubmitted,
    WriteNotSubmitted,
    WriteAlreadyCompleted,
    SyncAlreadyInFlight,
    SyncNotInFlight,
    WrongSyncId,
    UnadmittedTicket,
    PreallocationPending,
    InvalidPreallocationRange,
    CounterOverflow,
}

struct IoGroup {
    tickets: Range<u64>,
    file_bytes: Range<u64>,
    writes: Vec<WriteStatus>,
}

impl IoGroup {
    fn is_written(&self) -> bool {
        self.writes
            .iter()
            .all(|status| *status == WriteStatus::Completed)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SyncAttempt {
    id: SyncId,
    target: u64,
}

/// Deterministic model of ticket, group, write, sync, and poison frontiers.
///
/// Ticket ranges and file ranges are half-open. `written_frontier` and
/// `durable_ticket` are exclusive ends, so a value of `n` covers tickets
/// `0..n`. All groups are assigned in ticket and file-offset order.
struct PipelineState {
    groups: Vec<IoGroup>,
    admitted_ticket_end: u64,
    assigned_ticket_end: u64,
    reserved_file_end: u64,
    preallocated_file_end: u64,
    written_frontier: u64,
    durable_ticket: u64,
    poison_ticket: Option<u64>,
    sync_in_flight: Option<SyncAttempt>,
    next_sync_id: u64,
}

impl PipelineState {
    fn new(header_end: u64) -> Self {
        Self {
            groups: Vec::new(),
            admitted_ticket_end: 0,
            assigned_ticket_end: 0,
            reserved_file_end: header_end,
            preallocated_file_end: header_end,
            written_frontier: 0,
            durable_ticket: 0,
            poison_ticket: None,
            sync_in_flight: None,
            next_sync_id: 0,
        }
    }

    fn admit_ticket(&mut self, ticket: u64) -> Result<(), StateError> {
        if self.poison_ticket.is_some() {
            return Err(StateError::Poisoned);
        }
        if ticket != self.admitted_ticket_end {
            return Err(StateError::NonContiguousTickets);
        }
        self.admitted_ticket_end = self
            .admitted_ticket_end
            .checked_add(1)
            .ok_or(StateError::CounterOverflow)?;
        self.assert_invariants();

        Ok(())
    }

    fn assign_group(
        &mut self,
        tickets: Range<u64>,
        file_bytes: Range<u64>,
        write_count: usize,
    ) -> Result<GroupId, StateError> {
        if self.poison_ticket.is_some() {
            return Err(StateError::Poisoned);
        }
        if tickets.start >= tickets.end {
            return Err(StateError::InvalidTicketRange);
        }
        if tickets.start != self.assigned_ticket_end {
            return Err(StateError::NonContiguousTickets);
        }
        if file_bytes.start >= file_bytes.end {
            return Err(StateError::InvalidFileRange);
        }
        if file_bytes.start != self.reserved_file_end {
            return Err(StateError::NonContiguousFileRange);
        }
        if tickets.end > self.admitted_ticket_end {
            return Err(StateError::UnadmittedTicket);
        }
        if write_count == 0 {
            return Err(StateError::EmptyGroup);
        }

        let id = GroupId(self.groups.len());
        let ticket_end = tickets.end;
        let file_end = file_bytes.end;
        self.groups.push(IoGroup {
            tickets,
            file_bytes,
            writes: vec![WriteStatus::NotSubmitted; write_count],
        });
        self.assigned_ticket_end = ticket_end;
        self.reserved_file_end = file_end;
        self.assert_invariants();

        Ok(id)
    }

    /// Record a successful, contiguous preallocation operation.
    fn complete_preallocation(&mut self, range: Range<u64>) -> Result<(), StateError> {
        let required_end = self
            .reserved_file_end
            .checked_add(PREALLOC_BLOCK - 1)
            .map(|end| end / PREALLOC_BLOCK * PREALLOC_BLOCK)
            .ok_or(StateError::InvalidPreallocationRange)?;
        if range.start != self.preallocated_file_end
            || range.end < range.start
            || range.end > required_end
            || range.end > MAX_WAL_FILE_SIZE
            || !range.end.is_multiple_of(PREALLOC_BLOCK)
        {
            return Err(StateError::InvalidPreallocationRange);
        }
        self.preallocated_file_end = range.end;

        Ok(())
    }

    fn submit_write(&mut self, group_id: GroupId, write_index: usize) -> Result<(), StateError> {
        let group = self
            .groups
            .get_mut(group_id.0)
            .ok_or(StateError::UnknownGroup)?;
        if self
            .poison_ticket
            .is_some_and(|poison| group.tickets.start >= poison)
        {
            return Err(StateError::Poisoned);
        }
        if group.file_bytes.end > self.preallocated_file_end {
            return Err(StateError::PreallocationPending);
        }

        let status = group
            .writes
            .get_mut(write_index)
            .ok_or(StateError::UnknownWrite)?;
        match status {
            WriteStatus::NotSubmitted => *status = WriteStatus::Submitted,
            WriteStatus::Submitted | WriteStatus::Completed | WriteStatus::Failed => {
                return Err(StateError::WriteAlreadySubmitted);
            }
        }

        Ok(())
    }

    fn complete_write(
        &mut self,
        group_id: GroupId,
        write_index: usize,
        completion: WriteCompletion,
    ) -> Result<(), StateError> {
        let failed_group_start = {
            let group = self
                .groups
                .get_mut(group_id.0)
                .ok_or(StateError::UnknownGroup)?;
            let status = group
                .writes
                .get_mut(write_index)
                .ok_or(StateError::UnknownWrite)?;

            if *status == WriteStatus::NotSubmitted {
                return Err(StateError::WriteNotSubmitted);
            }
            if *status != WriteStatus::Submitted {
                return Err(StateError::WriteAlreadyCompleted);
            }

            match completion {
                WriteCompletion::FullLength => {
                    *status = WriteStatus::Completed;
                    None
                }
                WriteCompletion::Short | WriteCompletion::Failed => {
                    *status = WriteStatus::Failed;
                    Some(group.tickets.start)
                }
            }
        };

        if let Some(failed_ticket) = failed_group_start {
            self.poison_at(failed_ticket);
        }
        self.advance_written_frontier();
        self.assert_invariants();

        Ok(())
    }

    /// Fail an assigned group before any write is submitted, for example when
    /// preallocation fails. Earlier written groups may still become durable.
    fn fail_unsubmitted_group(&mut self, group_id: GroupId) -> Result<(), StateError> {
        let group = self
            .groups
            .get(group_id.0)
            .ok_or(StateError::UnknownGroup)?;
        if group
            .writes
            .iter()
            .any(|status| *status != WriteStatus::NotSubmitted)
        {
            return Err(StateError::WriteAlreadySubmitted);
        }

        let failed_ticket = group.tickets.start;
        self.poison_at(failed_ticket);
        self.assert_invariants();

        Ok(())
    }

    fn begin_sync(&mut self) -> Result<Option<SyncId>, StateError> {
        if self.sync_in_flight.is_some() {
            return Err(StateError::SyncAlreadyInFlight);
        }

        let poison_limit = self.poison_ticket.unwrap_or(u64::MAX);
        let target = self.written_frontier.min(poison_limit);
        if target <= self.durable_ticket {
            return Ok(None);
        }

        let next_sync_id = self
            .next_sync_id
            .checked_add(1)
            .ok_or(StateError::CounterOverflow)?;
        let id = SyncId(self.next_sync_id);
        self.next_sync_id = next_sync_id;
        self.sync_in_flight = Some(SyncAttempt { id, target });

        Ok(Some(id))
    }

    fn sync_target(&self, sync_id: SyncId) -> Result<u64, StateError> {
        let attempt = self.sync_in_flight.ok_or(StateError::SyncNotInFlight)?;
        if attempt.id != sync_id {
            return Err(StateError::WrongSyncId);
        }

        Ok(attempt.target)
    }

    fn finish_sync(&mut self, sync_id: SyncId, result: SyncResult) -> Result<(), StateError> {
        let attempt = self.sync_in_flight.ok_or(StateError::SyncNotInFlight)?;
        if attempt.id != sync_id {
            return Err(StateError::WrongSyncId);
        }
        self.sync_in_flight = None;

        match result {
            SyncResult::Succeeded => {
                let poison_limit = self.poison_ticket.unwrap_or(u64::MAX);
                self.durable_ticket = self.durable_ticket.max(attempt.target.min(poison_limit));
            }
            SyncResult::Failed => self.poison_at(self.durable_ticket),
        }
        self.assert_invariants();

        Ok(())
    }

    fn ticket_result(&self, ticket: u64) -> Result<TicketResult, StateError> {
        if ticket >= self.admitted_ticket_end {
            return Err(StateError::UnadmittedTicket);
        }
        if ticket < self.durable_ticket {
            return Ok(TicketResult::Durable);
        }
        if self.poison_ticket.is_some_and(|poison| ticket >= poison) {
            return Ok(TicketResult::Failed);
        }

        Ok(TicketResult::Pending)
    }

    fn poison_at(&mut self, ticket: u64) {
        self.poison_ticket = Some(
            self.poison_ticket
                .map_or(ticket, |current| current.min(ticket)),
        );
    }

    fn advance_written_frontier(&mut self) {
        for group in &self.groups {
            if group.tickets.end <= self.written_frontier {
                continue;
            }
            if group.tickets.start != self.written_frontier || !group.is_written() {
                break;
            }
            self.written_frontier = group.tickets.end;
        }
    }

    fn assert_invariants(&self) {
        debug_assert!(self.assigned_ticket_end <= self.admitted_ticket_end);
        debug_assert!(self.durable_ticket <= self.written_frontier);
        debug_assert!(self.written_frontier <= self.assigned_ticket_end);
        debug_assert!(
            self.poison_ticket
                .is_none_or(|poison| self.durable_ticket <= poison)
        );
        debug_assert!(
            self.groups
                .last()
                .is_none_or(|group| group.tickets.end == self.assigned_ticket_end
                    && group.file_bytes.end == self.reserved_file_end)
        );

        for pair in self.groups.windows(2) {
            debug_assert_eq!(pair[0].tickets.end, pair[1].tickets.start);
            debug_assert_eq!(pair[0].file_bytes.end, pair[1].file_bytes.start);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GroupId, MAX_WAL_FILE_SIZE, PREALLOC_BLOCK, PipelineState, StateError, SyncResult,
        TicketResult, WriteCompletion,
    };

    const WAL_HEADER_END: u64 = 4096;

    fn assign_group(
        state: &mut PipelineState,
        ticket_count: u64,
        file_bytes: u64,
        write_count: usize,
    ) -> GroupId {
        let ticket_start = state.assigned_ticket_end;
        let ticket_end = ticket_start + ticket_count;
        let file_start = state.reserved_file_end;
        while state.admitted_ticket_end < ticket_end {
            state
                .admit_ticket(state.admitted_ticket_end)
                .expect("admit next ticket");
        }
        let group = state
            .assign_group(
                ticket_start..ticket_end,
                file_start..file_start + file_bytes,
                write_count,
            )
            .expect("assign contiguous group");
        let required_end = (file_start + file_bytes).div_ceil(PREALLOC_BLOCK) * PREALLOC_BLOCK;
        state
            .complete_preallocation(state.preallocated_file_end..required_end)
            .expect("preallocate assigned group");

        group
    }

    fn submit_all(state: &mut PipelineState, group_id: GroupId, write_count: usize) {
        for write_index in 0..write_count {
            state
                .submit_write(group_id, write_index)
                .expect("submit write");
        }
    }

    fn complete_all(state: &mut PipelineState, group_id: GroupId, write_count: usize) {
        for write_index in 0..write_count {
            state
                .complete_write(group_id, write_index, WriteCompletion::FullLength)
                .expect("complete full-length write");
        }
    }

    #[test]
    fn out_of_order_group_completions_advance_only_contiguous_written_prefix() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let first = assign_group(&mut state, 1, 4096, 2);
        let second = assign_group(&mut state, 2, 8192, 1);
        submit_all(&mut state, first, 2);
        submit_all(&mut state, second, 1);

        complete_all(&mut state, second, 1);
        assert_eq!(state.written_frontier, 0);

        complete_all(&mut state, first, 2);
        assert_eq!(state.written_frontier, 3);
        assert_eq!(state.ticket_result(2), Ok(TicketResult::Pending));
    }

    #[test]
    fn failed_group_sets_earliest_poison_ticket_when_failures_arrive_in_reverse_order() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let first = assign_group(&mut state, 1, 4096, 1);
        let second = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, first, 1);
        submit_all(&mut state, second, 1);

        state
            .complete_write(second, 0, WriteCompletion::Failed)
            .expect("fail later group first");
        assert_eq!(state.poison_ticket, Some(1));
        state
            .complete_write(first, 0, WriteCompletion::Failed)
            .expect("then fail earlier group");

        assert_eq!(state.poison_ticket, Some(0));
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Failed));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Failed));
        assert_eq!(state.admit_ticket(2), Err(StateError::Poisoned));
        assert_eq!(state.admitted_ticket_end, 2);
        assert_eq!(
            state.assign_group(2..3, 12_288..16_384, 1),
            Err(StateError::Poisoned)
        );
    }

    #[test]
    fn sync_success_acknowledges_only_its_captured_written_prefix() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let first = assign_group(&mut state, 1, 4096, 1);
        let second = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, first, 1);
        submit_all(&mut state, second, 1);
        complete_all(&mut state, first, 1);

        let first_sync = state
            .begin_sync()
            .expect("start sync")
            .expect("written prefix");
        assert_eq!(state.sync_target(first_sync), Ok(1));
        complete_all(&mut state, second, 1);
        assert_eq!(state.written_frontier, 2);
        state
            .finish_sync(first_sync, SyncResult::Succeeded)
            .expect("finish first sync");
        assert_eq!(state.durable_ticket, 1);
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Durable));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Pending));

        let second_sync = state
            .begin_sync()
            .expect("start next sync")
            .expect("new prefix");
        assert_eq!(state.sync_target(second_sync), Ok(2));
        state
            .finish_sync(second_sync, SyncResult::Succeeded)
            .expect("finish second sync");
        assert_eq!(state.durable_ticket, 2);
    }

    #[test]
    fn short_write_poison_blocks_ticket_and_does_not_advance_written_frontier() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let group = assign_group(&mut state, 1, 4096, 2);
        submit_all(&mut state, group, 2);
        state
            .complete_write(group, 0, WriteCompletion::FullLength)
            .expect("complete first request");
        state
            .complete_write(group, 1, WriteCompletion::Short)
            .expect("short write fails its group");

        assert_eq!(state.poison_ticket, Some(0));
        assert_eq!(state.written_frontier, 0);
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Failed));
        assert_eq!(state.begin_sync(), Ok(None));
    }

    #[test]
    fn durable_ticket_stays_successful_after_a_later_group_failure() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let earlier = assign_group(&mut state, 1, 4096, 1);
        let later = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, earlier, 1);
        submit_all(&mut state, later, 1);
        complete_all(&mut state, earlier, 1);
        let sync = state
            .begin_sync()
            .expect("start sync")
            .expect("written prefix");
        state
            .finish_sync(sync, SyncResult::Succeeded)
            .expect("make earlier ticket durable");

        state
            .complete_write(later, 0, WriteCompletion::Failed)
            .expect("fail later group");

        // WAL durability is independent from the later MVCC publication stage.
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Durable));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Failed));
        assert_eq!(state.durable_ticket, 1);
    }

    #[test]
    fn written_prefix_before_poison_can_sync_after_failure() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let earlier = assign_group(&mut state, 2, 8192, 1);
        let failed = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, earlier, 1);
        submit_all(&mut state, failed, 1);
        complete_all(&mut state, earlier, 1);
        state
            .complete_write(failed, 0, WriteCompletion::Failed)
            .expect("fail later group");

        let sync = state
            .begin_sync()
            .expect("start prefix sync")
            .expect("prefix before poison");
        assert_eq!(state.sync_target(sync), Ok(2));
        state
            .finish_sync(sync, SyncResult::Succeeded)
            .expect("sync prefix before poison");

        assert_eq!(state.durable_ticket, 2);
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Durable));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Durable));
        assert_eq!(state.ticket_result(2), Ok(TicketResult::Failed));
    }

    #[test]
    fn failure_during_sync_does_not_change_the_captured_prefix_result() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let earlier = assign_group(&mut state, 1, 4096, 1);
        let later = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, earlier, 1);
        submit_all(&mut state, later, 1);
        complete_all(&mut state, earlier, 1);
        let sync = state
            .begin_sync()
            .expect("start prefix sync")
            .expect("written prefix");
        state
            .complete_write(later, 0, WriteCompletion::Failed)
            .expect("later failure while sync is active");
        state
            .finish_sync(sync, SyncResult::Succeeded)
            .expect("finish captured prefix sync");

        assert_eq!(state.durable_ticket, 1);
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Durable));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Failed));
        assert_eq!(state.admit_ticket(2), Err(StateError::Poisoned));
        assert_eq!(state.admitted_ticket_end, 2);
    }

    #[test]
    fn failed_sync_poisons_only_the_unacknowledged_suffix() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        let earlier = assign_group(&mut state, 1, 4096, 1);
        let later = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, earlier, 1);
        submit_all(&mut state, later, 1);
        complete_all(&mut state, earlier, 1);
        let first_sync = state
            .begin_sync()
            .expect("start sync")
            .expect("written prefix");
        state
            .finish_sync(first_sync, SyncResult::Succeeded)
            .expect("durable first ticket");
        complete_all(&mut state, later, 1);
        let second_sync = state.begin_sync().expect("start sync").expect("new prefix");
        state
            .finish_sync(second_sync, SyncResult::Failed)
            .expect("sync failure poisons remaining suffix");

        assert_eq!(state.poison_ticket, Some(1));
        assert_eq!(state.durable_ticket, 1);
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Durable));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Failed));
        assert_eq!(state.admit_ticket(2), Err(StateError::Poisoned));
        assert_eq!(state.admitted_ticket_end, 2);
    }

    #[test]
    fn group_assignment_rejects_ticket_or_file_range_holes_without_consuming_ranges() {
        let mut state = PipelineState::new(WAL_HEADER_END);

        assert_eq!(
            state.assign_group(1..2, 4096..8192, 1),
            Err(StateError::NonContiguousTickets)
        );
        assert_eq!(
            state.assign_group(0..1, 8192..12_288, 1),
            Err(StateError::NonContiguousFileRange)
        );
        assert_eq!(state.assigned_ticket_end, 0);
        assert_eq!(state.reserved_file_end, WAL_HEADER_END);

        let group = assign_group(&mut state, 1, 4096, 1);
        assert_eq!(group, GroupId(0));
        assert_eq!(state.assigned_ticket_end, 1);
        assert_eq!(state.reserved_file_end, 8192);
    }

    #[test]
    fn write_submission_waits_for_contiguous_preallocation() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        state.admit_ticket(0).expect("admit ticket");
        let group = state
            .assign_group(0..1, 4096..8192, 1)
            .expect("assign group");
        assert_eq!(
            state.submit_write(group, 0),
            Err(StateError::PreallocationPending)
        );
        assert_eq!(
            state.complete_preallocation(8192..12_288),
            Err(StateError::InvalidPreallocationRange)
        );
        assert_eq!(
            state.complete_preallocation(4096..PREALLOC_BLOCK * 2),
            Err(StateError::InvalidPreallocationRange)
        );
        assert_eq!(
            state.complete_preallocation(4096..MAX_WAL_FILE_SIZE + PREALLOC_BLOCK),
            Err(StateError::InvalidPreallocationRange)
        );
        state
            .complete_preallocation(4096..PREALLOC_BLOCK)
            .expect("preallocate contiguous range");
        assert_eq!(state.submit_write(group, 0), Ok(()));
    }

    #[test]
    fn sync_is_single_flight_and_empty_frontier_is_a_noop() {
        let mut state = PipelineState::new(WAL_HEADER_END);
        assert_eq!(state.begin_sync(), Ok(None));

        let group = assign_group(&mut state, 1, 4096, 1);
        submit_all(&mut state, group, 1);
        complete_all(&mut state, group, 1);
        let sync = state
            .begin_sync()
            .expect("start sync")
            .expect("written prefix");
        assert_eq!(state.begin_sync(), Err(StateError::SyncAlreadyInFlight));
        state
            .finish_sync(sync, SyncResult::Succeeded)
            .expect("finish sync");
        assert_eq!(state.begin_sync(), Ok(None));
    }
}
