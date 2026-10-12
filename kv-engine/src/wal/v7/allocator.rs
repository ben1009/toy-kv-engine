//! Serialized physical reservations for v7 DATA and FRONTIER frames.
//!
//! The runtime admission owner must serialize calls to this allocator with
//! both DATA and FRONTIER admission. DATA allocations consume a sequential
//! ticket and a contiguous range of frames. A FRONTIER consumes one frame at
//! the current allocation tail without changing ticket numbering. The runtime
//! can therefore split a ticket group at a known control-frame gap instead of
//! issuing a DATA write that could overwrite a FRONTIER.
//! Each admitted ticket also holds capacity for one worst-case marker. Placing
//! a marker consumes one credit; successful sync releases the unused credits
//! of its captured prefix while retaining those of later admissions.

use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};

use super::codec::{WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN};

const FRAME_LEN_U64: u64 = WAL_V7_FRAME_LEN as u64;
const FIRST_DATA_OFFSET: u64 = (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64;

/// Coordinates for one ticket's contiguous DATA frame allocation.
#[derive(Debug)]
#[must_use = "a v7 DATA allocation owns its assigned ticket and physical frame range"]
pub(crate) struct WalV7DataAllocation {
    ticket: u64,
    frame_range: Range<u64>,
    owner: Arc<WalV7AllocatorIdentity>,
}

/// Identity of one reserved FRONTIER slot.
#[derive(Clone, Debug)]
#[must_use = "a v7 FRONTIER reservation must be completed or poison its runtime"]
pub(crate) struct WalV7FrontierReservation {
    frame_offset: u64,
    ticket_end: u64,
    frame_end: u64,
    owner: Arc<WalV7AllocatorIdentity>,
}

/// Unique identity shared only by tokens issued by one allocator instance.
#[derive(Debug)]
struct WalV7AllocatorIdentity;

impl WalV7DataAllocation {
    pub(crate) fn ticket(&self) -> u64 {
        self.ticket
    }

    pub(crate) fn frame_range(&self) -> &Range<u64> {
        &self.frame_range
    }
}

impl WalV7FrontierReservation {
    pub(crate) fn frame_offset(&self) -> u64 {
        self.frame_offset
    }

    pub(crate) fn ticket_end(&self) -> u64 {
        self.ticket_end
    }
}

/// One owner serializes all v7 physical allocations for a WAL incarnation.
///
/// This type does not perform I/O or validate frontier contents. Its caller
/// must reserve resources before `reserve_data`, write and sync the selected
/// marker before `complete_frontier_sync`, and poison the WAL after any
/// uncertain marker write or sync.
#[derive(Debug)]
pub(crate) struct WalV7PhysicalAllocator {
    next_ticket: u64,
    allocation_tail: u64,
    max_file_size: u64,
    identity: Arc<WalV7AllocatorIdentity>,
    frontier_offsets: Vec<u64>,
    last_frontier_ticket_end: u64,
    in_flight_frontier: Option<WalV7FrontierReservation>,
    poisoned: bool,
}

impl WalV7PhysicalAllocator {
    /// Resume allocation after an installed Active image.
    pub(crate) fn new(next_ticket: u64, allocation_tail: u64, max_file_size: u64) -> Result<Self> {
        ensure!(
            allocation_tail >= FIRST_DATA_OFFSET && allocation_tail.is_multiple_of(FRAME_LEN_U64),
            "invalid v7 allocation tail {allocation_tail}"
        );
        ensure!(
            max_file_size >= allocation_tail && max_file_size <= crate::wal::MAX_WAL_FILE_SIZE,
            "invalid v7 allocation limit {max_file_size}"
        );
        ensure!(
            next_ticket <= (allocation_tail - FIRST_DATA_OFFSET) / FRAME_LEN_U64,
            "v7 ticket cursor exceeds the physical DATA capacity"
        );

        Ok(Self {
            next_ticket,
            allocation_tail,
            max_file_size,
            identity: Arc::new(WalV7AllocatorIdentity),
            frontier_offsets: Vec::new(),
            last_frontier_ticket_end: next_ticket,
            in_flight_frontier: None,
            poisoned: false,
        })
    }

    /// Atomically assign the next ticket and reserve its contiguous DATA
    /// frames plus one worst-case FRONTIER's capacity. Call only after the
    /// batch's resource reservations succeed.
    pub(crate) fn reserve_data(&mut self, frame_count: u32) -> Result<WalV7DataAllocation> {
        self.ensure_healthy()?;
        ensure!(frame_count > 0, "v7 DATA allocation has no frames");

        let next_ticket = self
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("v7 ticket counter overflows"))?;
        let byte_len = u64::from(frame_count)
            .checked_mul(FRAME_LEN_U64)
            .ok_or_else(|| anyhow::anyhow!("v7 DATA allocation length overflows"))?;
        let end = self
            .allocation_tail
            .checked_add(byte_len)
            .ok_or_else(|| anyhow::anyhow!("v7 DATA allocation end overflows"))?;
        let marker_frames = self
            .reserved_marker_frames()?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("v7 marker reservation count overflows"))?;
        let reserved_marker_bytes = marker_frames
            .checked_mul(FRAME_LEN_U64)
            .ok_or_else(|| anyhow::anyhow!("v7 marker reservation capacity overflows"))?;
        let reserved_end = end
            .checked_add(reserved_marker_bytes)
            .ok_or_else(|| anyhow::anyhow!("v7 DATA and marker reservation end overflows"))?;
        ensure!(
            reserved_end <= self.max_file_size,
            "v7 DATA and marker reservations exceed the segment limit"
        );

        let allocation = WalV7DataAllocation {
            ticket: self.next_ticket,
            frame_range: self.allocation_tail..end,
            owner: Arc::clone(&self.identity),
        };
        self.next_ticket = next_ticket;
        self.allocation_tail = end;
        Ok(allocation)
    }

    /// Reserve one FRONTIER frame at the current tail without assigning a
    /// DATA ticket. `ticket_end` is the captured target: it must advance the
    /// last synced frontier and cannot exceed the assigned ticket cursor.
    /// Later DATA allocations start after this immutable slot. Its frame
    /// consumes one of the admitted tickets' reserved marker credits.
    pub(crate) fn reserve_frontier(&mut self, ticket_end: u64) -> Result<WalV7FrontierReservation> {
        self.ensure_healthy()?;
        ensure!(
            self.in_flight_frontier.is_none(),
            "a v7 FRONTIER write/sync cycle is already in flight"
        );
        ensure!(
            ticket_end > self.last_frontier_ticket_end && ticket_end <= self.next_ticket,
            "v7 FRONTIER ticket target does not advance within admitted tickets"
        );
        let frame_end = self
            .allocation_tail
            .checked_add(FRAME_LEN_U64)
            .ok_or_else(|| anyhow::anyhow!("v7 FRONTIER allocation end overflows"))?;
        ensure!(
            frame_end <= self.max_file_size,
            "v7 FRONTIER allocation exceeds the segment limit"
        );
        self.frontier_offsets
            .try_reserve(1)
            .context("reserve v7 FRONTIER allocation metadata")?;

        let reservation = WalV7FrontierReservation {
            frame_offset: self.allocation_tail,
            ticket_end,
            frame_end,
            owner: Arc::clone(&self.identity),
        };
        self.frontier_offsets.push(reservation.frame_offset);
        self.allocation_tail = frame_end;
        self.in_flight_frontier = Some(reservation.clone());
        Ok(reservation)
    }

    /// Complete the exclusive marker cycle after its WAL sync succeeds,
    /// releasing unused marker credits only for its captured ticket prefix.
    pub(crate) fn complete_frontier_sync(
        &mut self,
        reservation: WalV7FrontierReservation,
    ) -> Result<()> {
        self.validate_in_flight_frontier(&reservation)?;
        self.last_frontier_ticket_end = reservation.ticket_end;
        self.in_flight_frontier = None;
        Ok(())
    }

    /// Poison allocation after an uncertain marker write or sync.
    pub(crate) fn poison_frontier(&mut self, reservation: WalV7FrontierReservation) -> Result<()> {
        self.validate_in_flight_frontier(&reservation)?;
        self.in_flight_frontier = None;
        self.poisoned = true;
        Ok(())
    }

    /// Return DATA write ranges for a ticket-contiguous group, splitting at
    /// every FRONTIER slot reserved between its tickets.
    pub(crate) fn data_write_ranges(
        &self,
        allocations: &[WalV7DataAllocation],
    ) -> Result<Vec<Range<u64>>> {
        let mut ranges = Vec::new();
        ranges
            .try_reserve_exact(allocations.len())
            .context("reserve v7 DATA group range metadata")?;
        self.visit_data_write_ranges(allocations, |range| ranges.push(range))?;
        Ok(ranges)
    }

    /// Validate a ticket group without allocating the coalesced DATA ranges.
    pub(crate) fn validate_data_group(&self, allocations: &[WalV7DataAllocation]) -> Result<()> {
        self.visit_data_write_ranges(allocations, |_| {})
    }

    fn visit_data_write_ranges(
        &self,
        allocations: &[WalV7DataAllocation],
        mut visit: impl FnMut(Range<u64>),
    ) -> Result<()> {
        self.ensure_healthy()?;
        ensure!(!allocations.is_empty(), "v7 DATA write group is empty");

        let mut previous: Option<&WalV7DataAllocation> = None;
        let mut current_range: Option<Range<u64>> = None;
        for allocation in allocations {
            self.validate_data_allocation(allocation)?;
            let Some(prior) = previous else {
                current_range = Some(allocation.frame_range.clone());
                previous = Some(allocation);
                continue;
            };

            let expected_ticket = prior
                .ticket
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("v7 ticket range overflows"))?;
            ensure!(
                allocation.ticket == expected_ticket,
                "v7 DATA write group has a ticket gap"
            );
            ensure!(
                prior.frame_range.end <= allocation.frame_range.start,
                "v7 DATA allocations overlap or are out of physical order"
            );

            if prior.frame_range.end == allocation.frame_range.start {
                current_range
                    .as_mut()
                    .expect("the first allocation starts a range")
                    .end = allocation.frame_range.end;
            } else {
                let first_frontier = self
                    .frontier_offsets
                    .partition_point(|offset| *offset < prior.frame_range.end);
                let after_last_frontier = self
                    .frontier_offsets
                    .partition_point(|offset| *offset < allocation.frame_range.start);
                let mut control_offset = prior.frame_range.end;
                for &frontier_offset in &self.frontier_offsets[first_frontier..after_last_frontier]
                {
                    ensure!(
                        frontier_offset == control_offset,
                        "v7 DATA allocations are separated by an unreserved control gap"
                    );
                    control_offset = control_offset
                        .checked_add(FRAME_LEN_U64)
                        .ok_or_else(|| anyhow::anyhow!("v7 control-frame end overflows"))?;
                }
                ensure!(
                    control_offset == allocation.frame_range.start,
                    "v7 DATA allocations are separated by an unreserved control gap"
                );
                visit(
                    current_range
                        .take()
                        .expect("the prior allocation has a DATA range"),
                );
                current_range = Some(allocation.frame_range.clone());
            }
            previous = Some(allocation);
        }

        visit(current_range.expect("nonempty allocation group has a DATA range"));
        Ok(())
    }

    /// Return every reserved FRONTIER frame intersecting a worker allocation
    /// span. The worker validates that DATA writes and these control frames
    /// tile the full span without holes or overlaps.
    pub(crate) fn frontier_offsets_in(&self, physical_range: Range<u64>) -> Result<Vec<u64>> {
        self.ensure_healthy()?;
        ensure!(
            physical_range.start.is_multiple_of(FRAME_LEN_U64)
                && physical_range.end.is_multiple_of(FRAME_LEN_U64)
                && physical_range.start <= physical_range.end
                && physical_range.end <= self.allocation_tail,
            "invalid v7 physical allocation span"
        );

        let first = self
            .frontier_offsets
            .partition_point(|offset| *offset < physical_range.start);
        let after_last = self
            .frontier_offsets
            .partition_point(|offset| *offset < physical_range.end);
        let offsets = &self.frontier_offsets[first..after_last];
        let mut result = Vec::new();
        result
            .try_reserve_exact(offsets.len())
            .context("reserve v7 worker FRONTIER metadata")?;
        result.extend_from_slice(offsets);
        Ok(result)
    }

    pub(crate) fn next_ticket(&self) -> u64 {
        self.next_ticket
    }

    pub(crate) fn allocation_tail(&self) -> u64 {
        self.allocation_tail
    }

    /// Each ticket beyond the last synced frontier retains one marker credit,
    /// except for the credit already consumed by the in-flight physical marker.
    fn reserved_marker_frames(&self) -> Result<u64> {
        self.next_ticket
            .checked_sub(self.last_frontier_ticket_end)
            .and_then(|pending| pending.checked_sub(u64::from(self.in_flight_frontier.is_some())))
            .ok_or_else(|| anyhow::anyhow!("invalid v7 marker reservation accounting"))
    }

    fn validate_in_flight_frontier(&self, reservation: &WalV7FrontierReservation) -> Result<()> {
        let Some(in_flight) = self.in_flight_frontier.as_ref() else {
            bail!("v7 FRONTIER reservation is stale or belongs to another cycle")
        };
        ensure!(
            Arc::ptr_eq(&reservation.owner, &self.identity)
                && Arc::ptr_eq(&reservation.owner, &in_flight.owner)
                && reservation.frame_offset == in_flight.frame_offset
                && reservation.ticket_end == in_flight.ticket_end
                && reservation.frame_end == in_flight.frame_end,
            "v7 FRONTIER reservation is stale or belongs to another cycle"
        );
        Ok(())
    }

    fn validate_data_allocation(&self, allocation: &WalV7DataAllocation) -> Result<()> {
        ensure!(
            Arc::ptr_eq(&allocation.owner, &self.identity),
            "v7 DATA allocation belongs to another allocator"
        );
        ensure!(
            allocation.ticket < self.next_ticket,
            "v7 DATA allocation has an unassigned ticket"
        );
        ensure!(
            allocation.frame_range.start >= FIRST_DATA_OFFSET
                && allocation.frame_range.start.is_multiple_of(FRAME_LEN_U64)
                && allocation.frame_range.end.is_multiple_of(FRAME_LEN_U64)
                && allocation.frame_range.start < allocation.frame_range.end
                && allocation.frame_range.end <= self.allocation_tail,
            "invalid v7 DATA frame range"
        );
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            bail!("v7 WAL physical allocator is poisoned")
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: u64 = FIRST_DATA_OFFSET;
    const FRAME: u64 = FRAME_LEN_U64;

    fn allocator(limit: u64) -> WalV7PhysicalAllocator {
        WalV7PhysicalAllocator::new(0, START, limit).expect("create v7 physical allocator")
    }

    #[test]
    fn group_ranges_split_at_a_frontier_and_keep_ticket_order() {
        let mut allocator = allocator(START + 8 * FRAME);
        let first = allocator.reserve_data(2).unwrap();
        let frontier = allocator.reserve_frontier(1).unwrap();
        let second = allocator.reserve_data(1).unwrap();

        assert_eq!(first.ticket(), 0);
        assert_eq!(first.frame_range(), &(START..START + 2 * FRAME));
        assert_eq!(frontier.frame_offset(), START + 2 * FRAME);
        assert_eq!(second.ticket(), 1);
        assert_eq!(
            second.frame_range(),
            &(START + 3 * FRAME..START + 4 * FRAME)
        );
        assert_eq!(
            allocator.data_write_ranges(&[first, second]).unwrap(),
            [
                START..START + 2 * FRAME,
                START + 3 * FRAME..START + 4 * FRAME
            ]
        );
    }

    #[test]
    fn group_ranges_split_at_multiple_consecutive_frontiers() {
        let mut allocator = allocator(START + 6 * FRAME);
        let first = allocator.reserve_data(1).unwrap();
        let second = allocator.reserve_data(1).unwrap();

        let first_frontier = allocator.reserve_frontier(1).unwrap();
        allocator.complete_frontier_sync(first_frontier).unwrap();
        let second_frontier = allocator.reserve_frontier(2).unwrap();
        allocator.complete_frontier_sync(second_frontier).unwrap();

        let third = allocator.reserve_data(1).unwrap();

        assert_eq!(first.ticket(), 0);
        assert_eq!(second.ticket(), 1);
        assert_eq!(third.ticket(), 2);
        assert_eq!(
            allocator.data_write_ranges(&[second, third]).unwrap(),
            [
                START + FRAME..START + 2 * FRAME,
                START + 4 * FRAME..START + 5 * FRAME,
            ]
        );
    }

    #[test]
    fn adjacent_ticket_data_ranges_coalesce_without_crossing_a_control_frame() {
        let mut allocator = allocator(START + 8 * FRAME);
        let first = allocator.reserve_data(1).unwrap();
        let second = allocator.reserve_data(2).unwrap();

        let ranges = allocator.data_write_ranges(&[first, second]).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], START..START + 3 * FRAME);
    }

    #[test]
    fn later_data_can_reserve_after_an_in_flight_frontier_but_a_second_frontier_cannot() {
        let mut allocator = allocator(START + 8 * FRAME);
        let first = allocator.reserve_data(1).unwrap();
        let frontier = allocator.reserve_frontier(1).unwrap();
        let second = allocator.reserve_data(1).unwrap();

        assert!(allocator.reserve_frontier(2).is_err());
        allocator.complete_frontier_sync(frontier).unwrap();
        assert!(allocator.reserve_frontier(1).is_err());
        let next_frontier = allocator.reserve_frontier(2).unwrap();

        assert_eq!(second.ticket(), first.ticket() + 1);
        assert_eq!(next_frontier.frame_offset(), second.frame_range().end);
        assert_eq!(
            allocator.allocation_tail(),
            next_frontier.frame_offset() + FRAME
        );
    }

    #[test]
    fn rejected_allocation_does_not_consume_ticket_or_offset() {
        let mut allocator = allocator(START + 2 * FRAME);
        assert!(allocator.reserve_data(3).is_err());
        assert_eq!(allocator.next_ticket(), 0);
        assert_eq!(allocator.allocation_tail(), START);

        assert!(allocator.reserve_data(0).is_err());
        assert_eq!(allocator.next_ticket(), 0);
        assert_eq!(allocator.allocation_tail(), START);
    }

    #[test]
    fn marker_uses_the_same_segment_limit_as_data() {
        let mut allocator = allocator(START + 2 * FRAME);
        let data = allocator.reserve_data(1).unwrap();
        let frontier = allocator.reserve_frontier(1).unwrap();
        assert_eq!(frontier.frame_offset(), data.frame_range().end);
        assert_eq!(allocator.allocation_tail(), START + 2 * FRAME);

        allocator.complete_frontier_sync(frontier).unwrap();
        assert!(allocator.reserve_frontier(2).is_err());
        assert!(allocator.reserve_data(1).is_err());
        assert_eq!(allocator.next_ticket(), 1);
        assert_eq!(allocator.allocation_tail(), START + 2 * FRAME);
    }

    #[test]
    fn data_admission_preserves_capacity_for_every_pending_ticket_marker() {
        let mut single = allocator(START + 2 * FRAME);
        assert!(single.reserve_data(2).is_err());
        assert_eq!(single.next_ticket(), 0);
        assert_eq!(single.allocation_tail(), START);
        let _data = single.reserve_data(1).unwrap();
        let terminal = single.reserve_frontier(1).unwrap();
        single.complete_frontier_sync(terminal).unwrap();
        assert_eq!(single.allocation_tail(), START + 2 * FRAME);

        let mut pending = allocator(START + 4 * FRAME);
        let _first = pending.reserve_data(1).unwrap();
        let _second = pending.reserve_data(1).unwrap();
        assert!(pending.reserve_data(1).is_err());
        assert_eq!(pending.next_ticket(), 2);
        assert_eq!(pending.allocation_tail(), START + 2 * FRAME);
        let first_frontier = pending.reserve_frontier(1).unwrap();
        pending.complete_frontier_sync(first_frontier).unwrap();
        assert!(pending.reserve_data(1).is_err());
        let terminal = pending.reserve_frontier(2).unwrap();
        pending.complete_frontier_sync(terminal).unwrap();
        assert_eq!(pending.allocation_tail(), START + 4 * FRAME);
    }

    #[test]
    fn coalesced_marker_releases_only_synced_captured_prefix_capacity() {
        let mut allocator = allocator(START + 8 * FRAME);
        for _ in 0..3 {
            let _data = allocator.reserve_data(1).unwrap();
        }
        let coalesced = allocator.reserve_frontier(3).unwrap();
        let _later = allocator.reserve_data(1).unwrap();
        assert!(allocator.reserve_data(1).is_err());
        assert_eq!(allocator.next_ticket(), 4);
        assert_eq!(allocator.allocation_tail(), START + 5 * FRAME);

        allocator.complete_frontier_sync(coalesced).unwrap();
        let _later = allocator.reserve_data(1).unwrap();
        assert!(allocator.reserve_data(1).is_err());
        assert_eq!(allocator.next_ticket(), 5);
        assert_eq!(allocator.allocation_tail(), START + 6 * FRAME);

        let next = allocator.reserve_frontier(4).unwrap();
        allocator.complete_frontier_sync(next).unwrap();
        let terminal = allocator.reserve_frontier(5).unwrap();
        allocator.complete_frontier_sync(terminal).unwrap();
        assert_eq!(allocator.allocation_tail(), START + 8 * FRAME);
    }

    #[test]
    fn failed_frontier_cycle_poisons_all_further_allocations() {
        let mut allocator = allocator(START + 8 * FRAME);
        let _data = allocator.reserve_data(1).unwrap();
        let frontier = allocator.reserve_frontier(1).unwrap();
        allocator.poison_frontier(frontier).unwrap();

        assert!(allocator.reserve_data(1).is_err());
        assert!(allocator.reserve_frontier(2).is_err());
    }

    #[test]
    fn group_rejects_noncontiguous_tickets() {
        let mut allocator = allocator(START + 8 * FRAME);
        let first = allocator.reserve_data(1).unwrap();
        let _second = allocator.reserve_data(1).unwrap();
        let third = allocator.reserve_data(1).unwrap();
        assert!(allocator.data_write_ranges(&[first, third]).is_err());
    }

    #[test]
    fn allocations_are_bound_to_their_allocator() {
        let mut source = allocator(START + 8 * FRAME);
        let _first = source.reserve_data(1).unwrap();
        let _frontier = source.reserve_frontier(1).unwrap();
        let foreign = source.reserve_data(1).unwrap();

        let mut destination = allocator(START + 8 * FRAME);
        let _local = destination.reserve_data(3).unwrap();
        assert!(destination.data_write_ranges(&[foreign]).is_err());
    }

    #[test]
    fn malformed_start_and_foreign_frontier_reservation_are_rejected() {
        assert!(WalV7PhysicalAllocator::new(0, START + 1, START + 4 * FRAME).is_err());
        assert!(WalV7PhysicalAllocator::new(4, START + 3 * FRAME, START + 4 * FRAME).is_err());
        assert!(
            WalV7PhysicalAllocator::new(0, START, crate::wal::MAX_WAL_FILE_SIZE + FRAME).is_err()
        );

        let mut first = allocator(START + 8 * FRAME);
        let mut second = allocator(START + 8 * FRAME);
        let _data = first.reserve_data(1).unwrap();
        let _other_data = second.reserve_data(1).unwrap();
        let reservation = first.reserve_frontier(1).unwrap();
        let matching_reservation = second.reserve_frontier(1).unwrap();
        assert_eq!(
            reservation.frame_offset(),
            matching_reservation.frame_offset()
        );
        assert_eq!(reservation.ticket_end(), matching_reservation.ticket_end());
        assert!(second.complete_frontier_sync(reservation).is_err());
        second.complete_frontier_sync(matching_reservation).unwrap();
    }
}
