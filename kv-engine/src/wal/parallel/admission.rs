//! Test-only model for prepared-buffer admission, ordered packing, and file caps.

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::{Condvar, Mutex};

use super::super::{BUFFER_POOL_BUF_SIZE, BUFFER_POOL_CAPACITY, MAX_WAL_FILE_SIZE, PREALLOC_BLOCK};

const WAL_HEADER_END: u64 = 4096;
const NORMAL_ACTIVE_BUFFER_BUDGET: u64 = 64 * 1024 * 1024;
const MAX_BUFFER_CAPACITY: u64 = 240 * 1024 * 1024;
const HARD_DIRECT_BUFFER_CAP: u64 = 256 * 1024 * 1024;
const QUEUED_BATCH_LIMIT: usize = 256;

const FIXED_POOL_BYTES: u64 = BUFFER_POOL_BUF_SIZE as u64 * BUFFER_POOL_CAPACITY as u64;
const MAX_EXTRA_BUFFER_BYTES: u64 = HARD_DIRECT_BUFFER_CAP - FIXED_POOL_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BatchLayout {
    /// Exact file bytes reserved for this encoded v4 batch, including padding.
    aligned_len: u64,
    /// DirectBuf capacity reserved before encoding and ticket admission.
    buffer_capacity: u64,
}

impl BatchLayout {
    fn from_encoded_len(encoded_len: u64) -> Result<Self, AdmissionError> {
        if encoded_len == 0 {
            return Err(AdmissionError::EmptyBatch);
        }

        let aligned_len = align_up(encoded_len, 4096).ok_or(AdmissionError::SizeOverflow)?;
        let buffer_capacity = aligned_len.max(BUFFER_POOL_BUF_SIZE as u64);
        if buffer_capacity > MAX_BUFFER_CAPACITY {
            return Err(AdmissionError::BufferLimitExceeded);
        }

        Ok(Self {
            aligned_len,
            buffer_capacity,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BufferId(u64);

#[derive(Debug, PartialEq, Eq)]
struct PreparedBatch {
    buffer_id: BufferId,
    layout: BatchLayout,
}

#[derive(Debug, PartialEq, Eq)]
enum ReserveBuffer {
    Ready(PreparedBatch),
    Wait,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdmissionError {
    EmptyBatch,
    BufferLimitExceeded,
    RetryWalFull,
    BatchTooLargeForWal,
    Closed,
    Poisoned,
    SizeOverflow,
    CounterOverflow,
    UnknownBuffer,
    InvalidGroupLimit,
    InvalidPreallocationEnd,
    NonContiguousQueue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BufferKind {
    Pool,
    Extra,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BufferAllocation {
    capacity: u64,
    kind: BufferKind,
}

struct BufferBudget {
    active_bytes: u64,
    extra_resident_bytes: u64,
    pool_available: usize,
    oversized_buffer: Option<BufferId>,
    allocations: HashMap<BufferId, BufferAllocation>,
    normal_budget: u64,
    max_extra_bytes: u64,
}

impl BufferBudget {
    fn new(normal_budget: u64, max_extra_bytes: u64) -> Self {
        Self {
            active_bytes: 0,
            extra_resident_bytes: 0,
            pool_available: BUFFER_POOL_CAPACITY,
            oversized_buffer: None,
            allocations: HashMap::new(),
            normal_budget,
            max_extra_bytes,
        }
    }

    fn reserve(&mut self, id: BufferId, capacity: u64) -> bool {
        let oversized = capacity > self.normal_budget;
        if oversized {
            if self.active_bytes != 0 || self.oversized_buffer.is_some() {
                return false;
            }
        } else if self.oversized_buffer.is_some()
            || self
                .active_bytes
                .checked_add(capacity)
                .is_none_or(|active| active > self.normal_budget)
        {
            return false;
        }

        let kind = if capacity == BUFFER_POOL_BUF_SIZE as u64 && self.pool_available > 0 {
            BufferKind::Pool
        } else {
            if self
                .extra_resident_bytes
                .checked_add(capacity)
                .is_none_or(|resident| resident > self.max_extra_bytes)
            {
                return false;
            }
            BufferKind::Extra
        };

        self.active_bytes += capacity;
        match kind {
            BufferKind::Pool => self.pool_available -= 1,
            BufferKind::Extra => self.extra_resident_bytes += capacity,
        }
        if oversized {
            self.oversized_buffer = Some(id);
        }
        let previous = self
            .allocations
            .insert(id, BufferAllocation { capacity, kind });
        debug_assert!(previous.is_none());
        self.assert_invariants();

        true
    }

    fn release(&mut self, id: BufferId) -> Result<(), AdmissionError> {
        let allocation = self
            .allocations
            .remove(&id)
            .ok_or(AdmissionError::UnknownBuffer)?;
        self.active_bytes -= allocation.capacity;
        match allocation.kind {
            BufferKind::Pool => self.pool_available += 1,
            BufferKind::Extra => self.extra_resident_bytes -= allocation.capacity,
        }
        if allocation.capacity > self.normal_budget {
            debug_assert_eq!(self.oversized_buffer, Some(id));
            self.oversized_buffer = None;
        }
        self.assert_invariants();

        Ok(())
    }

    fn assert_invariants(&self) {
        debug_assert!(self.active_bytes <= self.normal_budget || self.oversized_buffer.is_some());
        debug_assert!(self.extra_resident_bytes <= self.max_extra_bytes);
        debug_assert!(FIXED_POOL_BYTES + self.extra_resident_bytes <= HARD_DIRECT_BUFFER_CAP);
        debug_assert!(self.pool_available <= BUFFER_POOL_CAPACITY);
        debug_assert_eq!(
            self.active_bytes,
            self.allocations
                .values()
                .map(|allocation| allocation.capacity)
                .sum::<u64>()
        );
        debug_assert_eq!(
            self.oversized_buffer.is_some(),
            self.allocations
                .values()
                .any(|allocation| allocation.capacity > self.normal_budget)
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueuedBatch {
    ticket: u64,
    buffer_id: BufferId,
    layout: BatchLayout,
}

struct AdmissionState {
    open: bool,
    poisoned: bool,
    next_ticket: u64,
    admitted_end: u64,
    queue: VecDeque<QueuedBatch>,
    queue_limit: usize,
}

struct PackerState {
    reserved_end: u64,
    preallocated_end: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct PackedGroup {
    tickets: Range<u64>,
    file_bytes: Range<u64>,
    preallocation_end: u64,
    batches: Vec<QueuedBatch>,
}

/// Concurrent model with separate locks for admission, buffer accounting, and
/// worker-side packing/preallocation state.
struct AdmissionController {
    buffers: Mutex<BufferBudget>,
    admission: Mutex<AdmissionState>,
    queue_available: Condvar,
    packer_work_available: Condvar,
    packer: Mutex<PackerState>,
    next_buffer_id: AtomicU64,
    queue_waiters: AtomicU64,
}

impl AdmissionController {
    fn new() -> Self {
        Self::with_limits(WAL_HEADER_END, QUEUED_BATCH_LIMIT)
    }

    fn with_limits(file_end: u64, queue_limit: usize) -> Self {
        assert!(file_end >= WAL_HEADER_END);
        assert!(file_end <= MAX_WAL_FILE_SIZE);
        assert!(queue_limit > 0);

        Self {
            buffers: Mutex::new(BufferBudget::new(
                NORMAL_ACTIVE_BUFFER_BUDGET,
                MAX_EXTRA_BUFFER_BYTES,
            )),
            admission: Mutex::new(AdmissionState {
                open: true,
                poisoned: false,
                next_ticket: 0,
                admitted_end: file_end,
                queue: VecDeque::new(),
                queue_limit,
            }),
            queue_available: Condvar::new(),
            packer_work_available: Condvar::new(),
            packer: Mutex::new(PackerState {
                reserved_end: file_end,
                preallocated_end: file_end,
            }),
            next_buffer_id: AtomicU64::new(0),
            queue_waiters: AtomicU64::new(0),
        }
    }

    /// Reserve buffer capacity before allocation/encoding; this never assigns a ticket.
    fn reserve_buffer(&self, encoded_len: u64) -> Result<ReserveBuffer, AdmissionError> {
        let layout = BatchLayout::from_encoded_len(encoded_len)?;
        let id = self
            .next_buffer_id
            .try_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                next.checked_add(1)
            })
            .map(BufferId)
            .map_err(|_| AdmissionError::CounterOverflow)?;

        if !self.buffers.lock().reserve(id, layout.buffer_capacity) {
            return Ok(ReserveBuffer::Wait);
        }

        Ok(ReserveBuffer::Ready(PreparedBatch {
            buffer_id: id,
            layout,
        }))
    }

    /// Commit ticket assignment and queue insertion at one linearization point.
    fn admit(&self, prepared: PreparedBatch) -> Result<u64, AdmissionError> {
        let buffer_id = prepared.buffer_id;
        let result = self.admit_locked(&prepared);
        if result.is_err() {
            self.buffers.lock().release(buffer_id)?;
        } else {
            self.packer_work_available.notify_one();
        }

        result
    }

    fn admit_locked(&self, prepared: &PreparedBatch) -> Result<u64, AdmissionError> {
        let mut state = self.admission.lock();
        loop {
            if state.poisoned {
                return Err(AdmissionError::Poisoned);
            }
            if !state.open {
                return Err(AdmissionError::Closed);
            }

            let file_end = state
                .admitted_end
                .checked_add(prepared.layout.aligned_len)
                .ok_or(AdmissionError::SizeOverflow)?;
            let empty_file_end = WAL_HEADER_END
                .checked_add(prepared.layout.aligned_len)
                .ok_or(AdmissionError::SizeOverflow)?;
            let required_preallocation =
                round_up(file_end, PREALLOC_BLOCK).ok_or(AdmissionError::SizeOverflow)?;
            let empty_preallocation =
                round_up(empty_file_end, PREALLOC_BLOCK).ok_or(AdmissionError::SizeOverflow)?;

            if empty_file_end > MAX_WAL_FILE_SIZE || empty_preallocation > MAX_WAL_FILE_SIZE {
                return Err(AdmissionError::BatchTooLargeForWal);
            }
            if file_end > MAX_WAL_FILE_SIZE || required_preallocation > MAX_WAL_FILE_SIZE {
                return Err(AdmissionError::RetryWalFull);
            }
            if state.queue.len() >= state.queue_limit {
                self.packer_work_available.notify_one();
                self.queue_waiters.fetch_add(1, Ordering::AcqRel);
                self.queue_available.wait(&mut state);
                self.queue_waiters.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            let next_ticket = state
                .next_ticket
                .checked_add(1)
                .ok_or(AdmissionError::CounterOverflow)?;
            let ticket = state.next_ticket;
            state.next_ticket = next_ticket;
            state.admitted_end = file_end;
            state.queue.push_back(QueuedBatch {
                ticket,
                buffer_id: prepared.buffer_id,
                layout: prepared.layout,
            });
            Self::assert_admission_invariants(&state);

            return Ok(ticket);
        }
    }

    /// Return a prepared buffer when encoding fails or a caller abandons preparation.
    fn cancel_prepared(&self, prepared: PreparedBatch) -> Result<(), AdmissionError> {
        self.buffers.lock().release(prepared.buffer_id)
    }

    /// Model a full-length write CQE: kernel ownership ends and buffer capacity retires.
    fn retire_buffer(&self, buffer_id: BufferId) -> Result<(), AdmissionError> {
        self.buffers.lock().release(buffer_id)
    }

    fn capture_sync_cutoff(&self) -> u64 {
        self.admission.lock().next_ticket
    }

    fn close_and_capture_cutoff(&self) -> u64 {
        let mut state = self.admission.lock();
        state.open = false;
        self.queue_available.notify_all();
        state.next_ticket
    }

    /// Stop admission and retire queued buffers; packed groups remain worker-owned.
    fn poison_and_drain_queued(&self) -> Result<Range<u64>, AdmissionError> {
        let (tickets, queued) = {
            let mut state = self.admission.lock();
            state.poisoned = true;
            let first = state
                .queue
                .front()
                .map_or(state.next_ticket, |batch| batch.ticket);
            let tickets = first..state.next_ticket;
            let queued = state.queue.drain(..).collect::<Vec<_>>();
            self.queue_available.notify_all();
            (tickets, queued)
        };
        let mut buffers = self.buffers.lock();
        for batch in queued {
            buffers.release(batch.buffer_id)?;
        }
        Ok(tickets)
    }

    fn pending_batches(&self) -> usize {
        self.admission.lock().queue.len()
    }

    fn ticket_count(&self) -> u64 {
        self.admission.lock().next_ticket
    }

    fn file_frontiers(&self) -> (u64, u64, u64) {
        let admission = self.admission.lock();
        let packer = self.packer.lock();
        (
            admission.admitted_end,
            packer.reserved_end,
            packer.preallocated_end,
        )
    }

    fn buffer_snapshot(&self) -> (u64, u64, usize) {
        let buffers = self.buffers.lock();
        (
            buffers.active_bytes,
            buffers.extra_resident_bytes,
            buffers.allocations.len(),
        )
    }

    /// Transfer a contiguous queue prefix to the worker and reserve its offsets.
    /// The worker preallocates the returned range after both locks are released.
    fn pack_next_group(&self, max_batches: usize) -> Result<Option<PackedGroup>, AdmissionError> {
        if max_batches == 0 {
            return Err(AdmissionError::InvalidGroupLimit);
        }

        let mut admission = self.admission.lock();
        if admission.poisoned {
            return Err(AdmissionError::Poisoned);
        }
        let mut packer = self.packer.lock();
        let batches = admission
            .queue
            .iter()
            .take(max_batches)
            .copied()
            .collect::<Vec<_>>();
        let Some(first) = batches.first() else {
            return Ok(None);
        };

        for pair in batches.windows(2) {
            if pair[0].ticket.checked_add(1) != Some(pair[1].ticket) {
                return Err(AdmissionError::NonContiguousQueue);
            }
        }
        let group_len = batches.iter().try_fold(0_u64, |total, batch| {
            total.checked_add(batch.layout.aligned_len)
        });
        let group_len = group_len.ok_or(AdmissionError::SizeOverflow)?;
        let file_end = packer
            .reserved_end
            .checked_add(group_len)
            .ok_or(AdmissionError::SizeOverflow)?;
        let preallocation_end =
            round_up(file_end, PREALLOC_BLOCK).ok_or(AdmissionError::SizeOverflow)?;
        if file_end > MAX_WAL_FILE_SIZE || preallocation_end > MAX_WAL_FILE_SIZE {
            return Err(AdmissionError::RetryWalFull);
        }
        let last = batches.last().expect("first batch implies a last batch");
        let tickets = first.ticket..last.ticket + 1;
        let file_bytes = packer.reserved_end..file_end;

        for _ in &batches {
            admission
                .queue
                .pop_front()
                .expect("validated group remains queued");
        }
        packer.reserved_end = file_end;
        Self::assert_invariants(&admission, &packer);
        self.queue_available.notify_all();

        Ok(Some(PackedGroup {
            tickets,
            file_bytes,
            preallocation_end,
            batches,
        }))
    }

    /// Advance the worker-side preallocation watermark after fallocate/ftruncate succeeds.
    fn mark_preallocated(&self, completed: Range<u64>) -> Result<(), AdmissionError> {
        let mut packer = self.packer.lock();
        let required_end =
            round_up(packer.reserved_end, PREALLOC_BLOCK).ok_or(AdmissionError::SizeOverflow)?;
        if completed.end > MAX_WAL_FILE_SIZE
            || completed.end > required_end
            || !completed.end.is_multiple_of(PREALLOC_BLOCK)
            || completed.start != packer.preallocated_end
            || completed.end < completed.start
        {
            return Err(AdmissionError::InvalidPreallocationEnd);
        }
        packer.preallocated_end = completed.end;

        Ok(())
    }

    /// Release buffers from a group that failed before any write submission.
    fn discard_unsubmitted_group(&self, group: PackedGroup) -> Result<(), AdmissionError> {
        let mut buffers = self.buffers.lock();
        for batch in group.batches {
            buffers.release(batch.buffer_id)?;
        }

        Ok(())
    }

    fn assert_admission_invariants(state: &AdmissionState) {
        debug_assert!(WAL_HEADER_END <= state.admitted_end);
        debug_assert!(state.admitted_end <= MAX_WAL_FILE_SIZE);
        debug_assert!(state.queue.len() <= state.queue_limit);
        debug_assert!(
            state
                .queue
                .iter()
                .all(|batch| batch.ticket < state.next_ticket)
        );
        for pair in state.queue.as_slices().0.windows(2) {
            debug_assert_eq!(pair[0].ticket + 1, pair[1].ticket);
        }
        // A VecDeque can wrap; check the boundary between its two slices too.
        let (first, second) = state.queue.as_slices();
        if let (Some(last_first), Some(first_second)) = (first.last(), second.first()) {
            debug_assert_eq!(last_first.ticket + 1, first_second.ticket);
        }
        for pair in second.windows(2) {
            debug_assert_eq!(pair[0].ticket + 1, pair[1].ticket);
        }
    }

    fn assert_invariants(admission: &AdmissionState, packer: &PackerState) {
        Self::assert_admission_invariants(admission);
        debug_assert!(WAL_HEADER_END <= packer.reserved_end);
        debug_assert!(packer.reserved_end <= admission.admitted_end);
        debug_assert!(admission.admitted_end <= MAX_WAL_FILE_SIZE);
        debug_assert!(WAL_HEADER_END <= packer.preallocated_end);
        debug_assert!(packer.preallocated_end <= MAX_WAL_FILE_SIZE);
        if !admission.poisoned {
            debug_assert_eq!(
                packer.reserved_end
                    + admission
                        .queue
                        .iter()
                        .map(|batch| batch.layout.aligned_len)
                        .sum::<u64>(),
                admission.admitted_end
            );
        }
    }
}

fn align_up(value: u64, alignment: u64) -> Option<u64> {
    value
        .checked_add(alignment - 1)
        .map(|value| value / alignment * alignment)
}

fn round_up(value: u64, block: u64) -> Option<u64> {
    align_up(value, block)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };

    use super::super::{PipelineState, SyncResult, TicketResult, WriteCompletion};
    use super::{
        AdmissionController, AdmissionError, BatchLayout, FIXED_POOL_BYTES, HARD_DIRECT_BUFFER_CAP,
        MAX_BUFFER_CAPACITY, NORMAL_ACTIVE_BUFFER_BUDGET, PREALLOC_BLOCK, PackedGroup,
        ReserveBuffer, WAL_HEADER_END,
    };
    use crate::wal::{BUFFER_POOL_BUF_SIZE, MAX_WAL_FILE_SIZE};

    fn reserve(controller: &AdmissionController, encoded_len: u64) -> super::PreparedBatch {
        match controller
            .reserve_buffer(encoded_len)
            .expect("reserve buffer")
        {
            ReserveBuffer::Ready(prepared) => prepared,
            ReserveBuffer::Wait => panic!("buffer budget should admit this reservation"),
        }
    }

    fn pack(controller: &AdmissionController, max_batches: usize) -> PackedGroup {
        let group = controller
            .pack_next_group(max_batches)
            .expect("pack I/O group")
            .expect("queued batches");
        let preallocated_end = controller.file_frontiers().2;
        controller
            .mark_preallocated(preallocated_end..group.preallocation_end)
            .expect("complete preallocation");

        group
    }

    fn retire_group(controller: &AdmissionController, group: PackedGroup) {
        for batch in group.batches {
            controller
                .retire_buffer(batch.buffer_id)
                .expect("retire completed buffer");
        }
    }

    #[test]
    fn layout_keeps_aligned_file_length_separate_from_buffer_capacity() {
        let small = BatchLayout::from_encoded_len(20).expect("small v4 batch");
        assert_eq!(small.aligned_len, 4096);
        assert_eq!(small.buffer_capacity, BUFFER_POOL_BUF_SIZE as u64);

        let above_pool = BatchLayout::from_encoded_len(BUFFER_POOL_BUF_SIZE as u64 + 1)
            .expect("batch above pool capacity");
        assert_eq!(above_pool.aligned_len, BUFFER_POOL_BUF_SIZE as u64 + 4096);
        assert_eq!(above_pool.buffer_capacity, above_pool.aligned_len);
    }

    #[test]
    fn buffer_limit_is_checked_before_reservation_or_ticket_assignment() {
        let controller = AdmissionController::new();
        assert_eq!(
            controller.reserve_buffer(MAX_BUFFER_CAPACITY + 1),
            Err(AdmissionError::BufferLimitExceeded)
        );
        assert_eq!(controller.ticket_count(), 0);
        assert_eq!(controller.pending_batches(), 0);
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
    }

    #[test]
    fn normal_budget_blocks_at_64_mib_and_oversized_buffers_are_exclusive_until_cqe() {
        let controller = AdmissionController::new();
        let first = reserve(&controller, NORMAL_ACTIVE_BUFFER_BUDGET);
        assert_eq!(controller.buffer_snapshot().0, NORMAL_ACTIVE_BUFFER_BUDGET);
        assert_eq!(
            controller.reserve_buffer(1),
            Ok(ReserveBuffer::Wait),
            "normal reservations may not exceed the 64 MiB active budget"
        );

        let first_ticket = controller.admit(first).expect("admit normal buffer");
        assert_eq!(first_ticket, 0);
        let first_group = pack(&controller, 1);
        assert_eq!(first_group.tickets, 0..1);
        retire_group(&controller, first_group);

        let oversized = reserve(&controller, NORMAL_ACTIVE_BUFFER_BUDGET + 1);
        let oversized_capacity = oversized.layout.buffer_capacity;
        assert_eq!(oversized_capacity, NORMAL_ACTIVE_BUFFER_BUDGET + 4096);
        let oversized_ticket = controller
            .admit(oversized)
            .expect("admit exclusive oversized buffer");
        assert_eq!(oversized_ticket, 1);
        assert_eq!(
            controller.reserve_buffer(1),
            Ok(ReserveBuffer::Wait),
            "ordinary reservations wait until the oversized buffer's CQE"
        );

        let oversized_group = pack(&controller, 1);
        assert_eq!(controller.buffer_snapshot().0, oversized_capacity);
        retire_group(&controller, oversized_group);
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
        assert!(controller.reserve_buffer(1).is_ok());
    }

    #[test]
    fn maximum_oversized_buffer_fits_the_hard_per_wal_direct_buffer_cap() {
        let controller = AdmissionController::new();
        let prepared = reserve(&controller, MAX_BUFFER_CAPACITY);
        assert_eq!(prepared.layout.buffer_capacity, MAX_BUFFER_CAPACITY);
        assert_eq!(
            FIXED_POOL_BYTES + controller.buffer_snapshot().1,
            HARD_DIRECT_BUFFER_CAP
        );
        controller
            .cancel_prepared(prepared)
            .expect("release prepared oversized buffer");
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
        assert_eq!(
            controller.reserve_buffer(MAX_BUFFER_CAPACITY + 1),
            Err(AdmissionError::BufferLimitExceeded)
        );
    }

    #[test]
    fn failed_preparation_and_rejected_admission_leave_no_ticket_or_offset_hole() {
        let controller = AdmissionController::new();
        let prepared = reserve(&controller, 64);
        controller
            .cancel_prepared(prepared)
            .expect("cancel failed encode before ticket assignment");
        assert_eq!(controller.ticket_count(), 0);

        let closed = reserve(&controller, 64);
        assert_eq!(controller.close_and_capture_cutoff(), 0);
        assert_eq!(controller.admit(closed), Err(AdmissionError::Closed));
        assert_eq!(controller.ticket_count(), 0);
        assert_eq!(controller.file_frontiers(), (4096, 4096, 4096));
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
    }

    #[test]
    fn queue_pressure_waits_without_consuming_a_ticket_then_admits_after_packing() {
        let controller = Arc::new(AdmissionController::with_limits(4096, 2));
        let first = reserve(&controller, 64);
        let second = reserve(&controller, 64);
        let waiting = reserve(&controller, 64);
        assert_eq!(controller.admit(first), Ok(0));
        assert_eq!(controller.admit(second), Ok(1));
        let waiting_controller = Arc::clone(&controller);
        let writer = thread::spawn(move || waiting_controller.admit(waiting));
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while controller
            .queue_waiters
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            assert!(std::time::Instant::now() < deadline, "writer did not wait");
            thread::yield_now();
        }
        assert_eq!(controller.ticket_count(), 2);
        assert_eq!(controller.pending_batches(), 2);
        assert_eq!(controller.buffer_snapshot().2, 3);

        let group = pack(&controller, 2);
        assert_eq!(group.tickets, 0..2);
        assert_eq!(writer.join().expect("join waiting writer"), Ok(2));
        assert_eq!(controller.ticket_count(), 3);
        retire_group(&controller, group);
    }

    #[test]
    fn poisoned_queue_wakes_waiter_and_releases_queued_and_prepared_buffers() {
        let controller = Arc::new(AdmissionController::with_limits(4096, 1));
        let first = reserve(&controller, 64);
        let waiting = reserve(&controller, 64);
        assert_eq!(controller.admit(first), Ok(0));
        let waiting_controller = Arc::clone(&controller);
        let writer = thread::spawn(move || waiting_controller.admit(waiting));
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while controller
            .queue_waiters
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            assert!(std::time::Instant::now() < deadline, "writer did not wait");
            thread::yield_now();
        }

        assert_eq!(controller.poison_and_drain_queued(), Ok(0..1));
        assert_eq!(
            writer.join().expect("join waiting writer"),
            Err(AdmissionError::Poisoned)
        );
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
        assert_eq!(controller.ticket_count(), 1);
    }

    #[test]
    fn preallocation_cannot_skip_a_range_or_exceed_packed_extent() {
        let controller = AdmissionController::new();
        let prepared = reserve(&controller, 64);
        assert_eq!(controller.admit(prepared), Ok(0));
        let group = controller
            .pack_next_group(1)
            .expect("pack group")
            .expect("queued batch");
        assert_eq!(
            controller.mark_preallocated(PREALLOC_BLOCK..PREALLOC_BLOCK * 2),
            Err(AdmissionError::InvalidPreallocationEnd)
        );
        assert_eq!(
            controller.mark_preallocated(WAL_HEADER_END..PREALLOC_BLOCK * 2),
            Err(AdmissionError::InvalidPreallocationEnd)
        );
        controller
            .mark_preallocated(WAL_HEADER_END..group.preallocation_end)
            .expect("preallocate packed extent");
        retire_group(&controller, group);
    }

    #[test]
    fn packer_uses_admission_lengths_and_reserves_contiguous_ticket_ordered_ranges() {
        let controller = AdmissionController::new();
        let lengths = [4096, BUFFER_POOL_BUF_SIZE as u64 + 1, 8192];
        for length in &lengths[..2] {
            let prepared = reserve(&controller, *length);
            controller.admit(prepared).expect("admit batch");
        }

        let expected = lengths.map(|length| {
            BatchLayout::from_encoded_len(length)
                .expect("layout")
                .aligned_len
        });
        let first = controller
            .pack_next_group(2)
            .expect("pack first group")
            .expect("queued batches");
        assert_eq!(first.tickets, 0..2);
        assert_eq!(first.file_bytes, 4096..4096 + expected[0] + expected[1]);
        assert_eq!(
            first
                .batches
                .iter()
                .map(|batch| batch.layout.aligned_len)
                .collect::<Vec<_>>(),
            expected[..2]
        );
        assert_eq!(controller.pending_batches(), 0);

        // New producer admission is independent from the worker's preallocation state.
        let later = reserve(&controller, lengths[2]);
        assert_eq!(controller.admit(later), Ok(2));
        controller
            .mark_preallocated(WAL_HEADER_END..first.preallocation_end)
            .expect("complete worker-side preallocation");
        assert_eq!(first.file_bytes.end, 4096 + expected[0] + expected[1]);

        let second = pack(&controller, 1);
        assert_eq!(second.tickets, 2..3);
        assert_eq!(second.file_bytes.start, first.file_bytes.end);
        assert_eq!(second.file_bytes.end - second.file_bytes.start, expected[2]);
        retire_group(&controller, first);
        retire_group(&controller, second);
        let (admitted_end, reserved_end, preallocated_end) = controller.file_frontiers();
        assert_eq!(admitted_end, reserved_end);
        assert!(reserved_end <= preallocated_end);
        assert!(preallocated_end <= MAX_WAL_FILE_SIZE);
    }

    #[test]
    fn preallocation_failure_after_packing_fails_the_assigned_suffix() {
        let controller = AdmissionController::new();
        let mut state = PipelineState::new(WAL_HEADER_END);
        let first = reserve(&controller, PREALLOC_BLOCK - WAL_HEADER_END);
        let second = reserve(&controller, 4096);
        let third = reserve(&controller, 4096);
        assert_eq!(controller.admit(first), Ok(0));
        assert_eq!(controller.admit(second), Ok(1));
        assert_eq!(controller.admit(third), Ok(2));
        for ticket in 0..3 {
            state.admit_ticket(ticket).expect("admit state ticket");
        }

        let first_group = controller
            .pack_next_group(1)
            .expect("pack first group")
            .expect("first batch queued");
        assert_eq!(first_group.file_bytes.end, PREALLOC_BLOCK);
        let first_id = state
            .assign_group(
                first_group.tickets.clone(),
                first_group.file_bytes.clone(),
                first_group.batches.len(),
            )
            .expect("assign first group");
        controller
            .mark_preallocated(WAL_HEADER_END..first_group.preallocation_end)
            .expect("preallocate first extent");
        state
            .complete_preallocation(WAL_HEADER_END..first_group.preallocation_end)
            .expect("record first preallocation");
        state.submit_write(first_id, 0).expect("submit first write");
        state
            .complete_write(first_id, 0, WriteCompletion::FullLength)
            .expect("complete first write");
        retire_group(&controller, first_group);

        let failed_group = controller
            .pack_next_group(1)
            .expect("pack second group")
            .expect("second batch queued");
        let failed_id = state
            .assign_group(
                failed_group.tickets.clone(),
                failed_group.file_bytes.clone(),
                failed_group.batches.len(),
            )
            .expect("assign second group");
        let (admitted_end, reserved_end, preallocated_end) = controller.file_frontiers();
        assert_eq!(controller.pending_batches(), 1);
        assert!(admitted_end > reserved_end);
        assert_eq!(preallocated_end, PREALLOC_BLOCK);
        assert!(failed_group.file_bytes.end > preallocated_end);

        // The second extent fails before any SQE is submitted. Its group and
        // ticket already exist, so the state model can fail its waiter.
        state
            .fail_unsubmitted_group(failed_id)
            .expect("fail unsubmitted group");
        assert_eq!(controller.poison_and_drain_queued(), Ok(2..3));
        assert_eq!(state.ticket_result(1), Ok(TicketResult::Failed));
        assert_eq!(state.ticket_result(2), Ok(TicketResult::Failed));
        let sync = state
            .begin_sync()
            .expect("begin final prefix sync")
            .expect("written prefix");
        state
            .finish_sync(sync, SyncResult::Succeeded)
            .expect("finish final prefix sync");
        assert_eq!(state.ticket_result(0), Ok(TicketResult::Durable));

        controller
            .discard_unsubmitted_group(failed_group)
            .expect("release buffer never submitted to kernel");
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
        let later = reserve(&controller, 4096);
        assert_eq!(controller.admit(later), Err(AdmissionError::Poisoned));
    }

    #[test]
    fn blocked_worker_preallocation_state_does_not_block_producer_admission() {
        let controller = Arc::new(AdmissionController::new());
        let prepared = reserve(&controller, 64);
        // Holding this worker-side lock models a slow fallocate/ftruncate call.
        let preallocation_guard = controller.packer.lock();
        let (sender, receiver) = mpsc::channel();
        let writer_controller = Arc::clone(&controller);
        let writer = thread::spawn(move || {
            sender
                .send(writer_controller.admit(prepared))
                .expect("send admission result");
        });

        let result = receiver.recv_timeout(Duration::from_secs(1));
        drop(preallocation_guard);
        writer.join().expect("join admission writer");
        assert_eq!(
            result.expect("admission must not wait for preallocation"),
            Ok(0)
        );
    }

    #[test]
    fn wal_full_retries_without_ticket_but_last_aligned_block_fits_preallocation_cap() {
        let controller = AdmissionController::with_limits(MAX_WAL_FILE_SIZE - 4096, 8);
        let too_large_for_remaining_space = reserve(&controller, 4097);
        assert_eq!(
            controller.admit(too_large_for_remaining_space),
            Err(AdmissionError::RetryWalFull)
        );
        assert_eq!(controller.ticket_count(), 0);
        assert_eq!(
            controller.file_frontiers(),
            (
                MAX_WAL_FILE_SIZE - 4096,
                MAX_WAL_FILE_SIZE - 4096,
                MAX_WAL_FILE_SIZE - 4096
            )
        );

        let last_block = reserve(&controller, 4096);
        assert_eq!(controller.admit(last_block), Ok(0));
        let group = pack(&controller, 1);
        assert_eq!(group.file_bytes.end, MAX_WAL_FILE_SIZE);
        assert_eq!(group.preallocation_end, MAX_WAL_FILE_SIZE);
        assert!(group.preallocation_end.is_multiple_of(PREALLOC_BLOCK));
        retire_group(&controller, group);
    }

    #[test]
    fn sync_cutoff_and_ticket_admission_share_one_linearization_point() {
        for _ in 0..16 {
            let controller = Arc::new(AdmissionController::new());
            let prepared = reserve(&controller, 64);
            let barrier = Arc::new(Barrier::new(3));
            let writer_controller = Arc::clone(&controller);
            let writer_barrier = Arc::clone(&barrier);
            let writer = thread::spawn(move || {
                writer_barrier.wait();
                writer_controller.admit(prepared)
            });
            let sync_controller = Arc::clone(&controller);
            let sync_barrier = Arc::clone(&barrier);
            let sync = thread::spawn(move || {
                sync_barrier.wait();
                sync_controller.capture_sync_cutoff()
            });

            barrier.wait();
            let admitted = writer.join().expect("join admission writer");
            let cutoff = sync.join().expect("join sync cutoff capture");
            match admitted {
                Ok(0) => assert!(cutoff <= 1),
                Err(AdmissionError::Closed) => unreachable!("sync capture does not close WAL"),
                other => panic!("unexpected admission result: {other:?}"),
            }
            assert!(cutoff <= controller.ticket_count());
        }
    }

    #[test]
    fn sync_cutoff_captures_the_exact_ticket_boundary_before_and_after_admission() {
        let controller = AdmissionController::new();
        assert_eq!(controller.capture_sync_cutoff(), 0);

        let prepared = reserve(&controller, 64);
        assert_eq!(controller.admit(prepared), Ok(0));
        assert_eq!(controller.capture_sync_cutoff(), 1);
    }

    #[test]
    fn close_racing_a_prepared_batch_places_it_entirely_before_or_after_cutoff() {
        for _ in 0..16 {
            let controller = Arc::new(AdmissionController::new());
            let prepared = reserve(&controller, 64);
            let barrier = Arc::new(Barrier::new(3));
            let writer_controller = Arc::clone(&controller);
            let writer_barrier = Arc::clone(&barrier);
            let writer = thread::spawn(move || {
                writer_barrier.wait();
                writer_controller.admit(prepared)
            });
            let closer_controller = Arc::clone(&controller);
            let closer_barrier = Arc::clone(&barrier);
            let closer = thread::spawn(move || {
                closer_barrier.wait();
                closer_controller.close_and_capture_cutoff()
            });

            barrier.wait();
            let admitted = writer.join().expect("join admission writer");
            let cutoff = closer.join().expect("join close");
            match admitted {
                Ok(ticket) => assert_eq!(cutoff, ticket + 1),
                Err(AdmissionError::Closed) => assert_eq!(cutoff, 0),
                other => panic!("unexpected admission result: {other:?}"),
            }
            assert_eq!(controller.ticket_count(), cutoff);
            assert_eq!(controller.buffer_snapshot().2, usize::from(cutoff == 1));
        }
    }

    #[test]
    fn concurrent_file_cap_checks_do_not_over_admit_or_consume_a_failed_ticket() {
        let controller = Arc::new(AdmissionController::with_limits(
            MAX_WAL_FILE_SIZE - 4096,
            8,
        ));
        let first = reserve(&controller, 4096);
        let second = reserve(&controller, 4096);
        let barrier = Arc::new(Barrier::new(3));
        let first_controller = Arc::clone(&controller);
        let first_barrier = Arc::clone(&barrier);
        let first_writer = thread::spawn(move || {
            first_barrier.wait();
            first_controller.admit(first)
        });
        let second_controller = Arc::clone(&controller);
        let second_barrier = Arc::clone(&barrier);
        let second_writer = thread::spawn(move || {
            second_barrier.wait();
            second_controller.admit(second)
        });

        barrier.wait();
        let outcomes = [
            first_writer.join().expect("join first writer"),
            second_writer.join().expect("join second writer"),
        ];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| **result == Err(AdmissionError::RetryWalFull))
                .count(),
            1
        );
        assert_eq!(controller.ticket_count(), 1);
        assert_eq!(controller.file_frontiers().0, MAX_WAL_FILE_SIZE);
        assert_eq!(controller.buffer_snapshot().2, 1);
    }

    #[test]
    fn poison_stops_admission_and_releases_prepared_capacity_without_a_ticket() {
        let controller = AdmissionController::new();
        let prepared = reserve(&controller, 64);
        assert_eq!(controller.poison_and_drain_queued(), Ok(0..0));

        assert_eq!(controller.admit(prepared), Err(AdmissionError::Poisoned));
        assert_eq!(controller.ticket_count(), 0);
        assert_eq!(controller.file_frontiers(), (4096, 4096, 4096));
        assert_eq!(controller.buffer_snapshot(), (0, 0, 0));
    }
}
