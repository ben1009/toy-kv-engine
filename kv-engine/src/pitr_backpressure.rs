//! Dormant PITR source-spool accounting and seal-boundary coordination.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};

use crate::mvcc::LsmMvccInner;
use crate::pitr_base::PublishedBaseReceipt;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SpoolReservation {
    id: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) bytes: u64,
    pub(crate) kind: ReservationKind,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReservationKind {
    Batch,
    Segment,
    Maintenance,
}
#[derive(Debug)]
struct SpoolState {
    next_id: u64,
    used_bytes: u64,
    logical_used_bytes: u64,
    maintenance_reserved: bool,
    admission_open: bool,
    reservations: BTreeMap<u64, SpoolReservation>,
}

#[derive(Debug)]
pub(crate) struct PitrSpoolAccountant {
    logical_limit: u64,
    limit: u64,
    maintenance_bytes: u64,
    state: Mutex<SpoolState>,
}
impl PitrSpoolAccountant {
    pub(crate) fn new(logical_limit: u64, limit: u64, maintenance_bytes: u64) -> Result<Self> {
        ensure!(
            logical_limit > 0 && limit > 0 && maintenance_bytes > 0 && maintenance_bytes <= limit,
            "PITR accounting limits are invalid"
        );
        Ok(Self {
            logical_limit,
            limit,
            maintenance_bytes,
            state: Mutex::new(SpoolState {
                next_id: 1,
                used_bytes: 0,
                logical_used_bytes: 0,
                maintenance_reserved: false,
                admission_open: true,
                reservations: BTreeMap::new(),
            }),
        })
    }

    pub(crate) fn reserve(&self, bytes: u64, kind: ReservationKind) -> Result<SpoolReservation> {
        ensure!(
            kind != ReservationKind::Batch,
            "batch reservations must include logical WAL bytes"
        );
        self.reserve_with_logical(0, bytes, kind)
    }

    pub(crate) fn reserve_batch(
        &self,
        logical_bytes: u64,
        physical_bytes: u64,
    ) -> Result<SpoolReservation> {
        ensure!(logical_bytes > 0, "batch logical WAL bytes must be nonzero");
        self.reserve_with_logical(logical_bytes, physical_bytes, ReservationKind::Batch)
    }

    fn reserve_with_logical(
        &self,
        logical_bytes: u64,
        bytes: u64,
        kind: ReservationKind,
    ) -> Result<SpoolReservation> {
        ensure!(bytes > 0, "PITR reservation must be nonzero");
        let mut state = self.state.lock();
        ensure!(
            kind != ReservationKind::Batch || state.admission_open,
            "PITR write admission is stopped"
        );
        ensure!(
            logical_bytes <= self.logical_limit.saturating_sub(state.logical_used_bytes),
            "PITR logical WAL limit exceeded"
        );
        if kind == ReservationKind::Maintenance {
            ensure!(
                !state.maintenance_reserved,
                "PITR maintenance reserve is already held"
            );
            ensure!(
                bytes == self.maintenance_bytes,
                "PITR maintenance reservation has wrong size"
            );
        }
        let next = state
            .used_bytes
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("PITR spool accounting overflow"))?;
        let user_limit = if state.maintenance_reserved {
            self.limit
        } else {
            self.limit.saturating_sub(self.maintenance_bytes)
        };
        ensure!(
            next <= user_limit || kind == ReservationKind::Maintenance,
            "PITR maintenance headroom is not reserved"
        );
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PITR reservation ID exhausted"))?;
        let reservation = SpoolReservation {
            id,
            logical_bytes,
            bytes,
            kind,
        };
        state.used_bytes = next;
        state.logical_used_bytes = state
            .logical_used_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| anyhow::anyhow!("PITR logical accounting overflow"))?;
        state.maintenance_reserved |= kind == ReservationKind::Maintenance;
        state.reservations.insert(
            id,
            SpoolReservation {
                id,
                logical_bytes,
                bytes,
                kind,
            },
        );
        Ok(reservation)
    }

    pub(crate) fn release(&self, reservation: SpoolReservation) -> Result<()> {
        let mut state = self.state.lock();
        let issued = state
            .reservations
            .get(&reservation.id)
            .ok_or_else(|| anyhow::anyhow!("unknown or already released PITR reservation"))?;
        ensure!(issued == &reservation, "PITR reservation identity mismatch");
        state.reservations.remove(&reservation.id);
        ensure!(
            state.used_bytes >= reservation.bytes,
            "PITR spool accounting underflow"
        );
        if reservation.kind == ReservationKind::Maintenance {
            state.maintenance_reserved = false;
        }
        state.used_bytes -= reservation.bytes;
        state.logical_used_bytes = state
            .logical_used_bytes
            .checked_sub(reservation.logical_bytes)
            .ok_or_else(|| anyhow::anyhow!("PITR logical accounting underflow"))?;
        Ok(())
    }

    fn stop_admission(&self) {
        self.state.lock().admission_open = false;
    }

    fn stop_admission_if_no_batches(&self) -> bool {
        let mut state = self.state.lock();
        state.admission_open = false;
        if state
            .reservations
            .values()
            .any(|reservation| reservation.kind == ReservationKind::Batch)
        {
            state.admission_open = true;
            return false;
        }
        true
    }

    fn resume_admission(&self) {
        self.state.lock().admission_open = true;
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.state.lock().used_bytes
    }

    pub(crate) fn remaining_bytes(&self) -> u64 {
        let state = self.state.lock();
        let user_limit = if state.maintenance_reserved {
            self.limit
        } else {
            self.limit.saturating_sub(self.maintenance_bytes)
        };
        user_limit.saturating_sub(state.used_bytes)
    }

    #[cfg(test)]
    fn admission_is_open(&self) -> bool {
        self.state.lock().admission_open
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum SealRequest {
    Timer,
    Size,
    Backup,
    Barrier,
    Shutdown,
}
impl SealRequest {
    fn priority(self) -> u8 {
        match self {
            Self::Timer => 0,
            Self::Size => 1,
            Self::Backup => 2,
            Self::Barrier => 3,
            Self::Shutdown => 4,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SealBoundaryState {
    AdmissionOpen,
    AdmissionStopped,
    Sealed,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CapturedBaseBoundary {
    timeline_id: [u8; 16],
    archive_epoch_id: [u8; 16],
    segment_id: u64,
    commit_high_water: Option<u64>,
    generation: u64,
}

impl CapturedBaseBoundary {
    pub(crate) fn timeline_id(&self) -> [u8; 16] {
        self.timeline_id
    }

    pub(crate) fn archive_epoch_id(&self) -> [u8; 16] {
        self.archive_epoch_id
    }

    pub(crate) fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) fn commit_high_water(&self) -> Option<u64> {
        self.commit_high_water
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    #[cfg(test)]
    pub(crate) fn for_test(segment_id: u64, commit_high_water: Option<u64>) -> Self {
        Self {
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            segment_id,
            commit_high_water,
            generation: 1,
        }
    }
}
#[derive(Debug)]
pub(crate) struct SealBoundaryCoordinator {
    accounting: Arc<PitrSpoolAccountant>,
    state: SealBoundaryState,
    pending: Option<SealRequest>,
    active_request: Option<SealRequest>,
    boundary: Option<u64>,
    last_completed_boundary: Option<u64>,
    barrier_generation: u64,
    base_boundary_issued: bool,
    base_commit_high_water: Option<Option<u64>>,
    base_sequencer_id: Option<u64>,
    base_identity: Option<([u8; 16], [u8; 16], u64, u64)>,
}
impl SealBoundaryCoordinator {
    pub(crate) fn new(accounting: Arc<PitrSpoolAccountant>) -> Self {
        Self {
            accounting,
            state: SealBoundaryState::AdmissionOpen,
            pending: None,
            active_request: None,
            boundary: None,
            last_completed_boundary: None,
            barrier_generation: 0,
            base_boundary_issued: false,
            base_commit_high_water: None,
            base_sequencer_id: None,
            base_identity: None,
        }
    }

    pub(crate) fn request(&mut self, request: SealRequest) -> bool {
        let highest_existing_priority = self
            .pending
            .iter()
            .chain(self.active_request.iter())
            .map(|request| request.priority())
            .max();
        if highest_existing_priority.is_some_and(|priority| request.priority() <= priority) {
            return false;
        }
        self.pending = Some(request);
        true
    }

    pub(crate) fn take_request(&mut self) -> Option<SealRequest> {
        if self.active_request.is_none() {
            self.active_request = self.pending.take();
        }
        self.active_request
    }

    pub(crate) fn complete_request(&mut self) {
        self.active_request = None;
    }

    pub(crate) fn stop_admission(&mut self, boundary: u64) -> Result<()> {
        ensure!(
            self.last_completed_boundary
                .is_none_or(|last_completed| boundary > last_completed),
            "seal boundary regressed"
        );
        ensure!(
            self.state == SealBoundaryState::AdmissionOpen,
            "seal boundary admission is not open"
        );
        self.accounting.stop_admission();
        self.boundary = Some(boundary);
        self.state = SealBoundaryState::AdmissionStopped;
        Ok(())
    }

    pub(crate) fn publish_sealed_boundary(&mut self, boundary: u64) -> Result<()> {
        ensure!(
            self.state == SealBoundaryState::AdmissionStopped && self.boundary == Some(boundary),
            "seal boundary is not stopped at requested boundary"
        );
        self.state = SealBoundaryState::Sealed;
        Ok(())
    }

    pub(crate) fn stop_admission_for_base(
        &mut self,
        boundary: u64,
        sequencer: &LsmMvccInner,
    ) -> Result<()> {
        ensure!(
            self.last_completed_boundary
                .is_none_or(|last_completed| boundary > last_completed),
            "seal boundary regressed"
        );
        ensure!(
            self.state == SealBoundaryState::AdmissionOpen,
            "seal boundary admission is not open"
        );
        ensure!(
            self.accounting.stop_admission_if_no_batches(),
            "PITR base barrier has pre-admitted batches"
        );
        let commit_high_water = match sequencer.stop_commit_admission_and_capture() {
            Ok(high_water) => high_water,
            Err(error) => {
                self.accounting.resume_admission();
                return Err(error);
            }
        };
        self.boundary = Some(boundary);
        self.base_commit_high_water = Some(commit_high_water);
        self.base_sequencer_id = Some(sequencer.instance_id());
        self.state = SealBoundaryState::AdmissionStopped;
        Ok(())
    }

    pub(crate) fn issue_base_boundary(
        &mut self,
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
        segment_id: u64,
    ) -> Result<CapturedBaseBoundary> {
        ensure!(
            self.state == SealBoundaryState::AdmissionStopped && self.boundary == Some(segment_id),
            "PITR base boundary is not stopped at the requested segment"
        );
        ensure!(
            !self.base_boundary_issued,
            "PITR base boundary was already issued"
        );
        let commit_high_water = self
            .base_commit_high_water
            .ok_or_else(|| anyhow::anyhow!("PITR base publication frontier was not captured"))?;
        ensure!(
            timeline_id != [0; 16],
            "PITR base boundary timeline is empty"
        );
        ensure!(
            archive_epoch_id != [0; 16],
            "PITR base boundary epoch is empty"
        );
        ensure!(
            commit_high_water.is_none_or(|commit_ts| commit_ts != 0),
            "PITR base boundary commit high-water is invalid"
        );
        self.barrier_generation = self
            .barrier_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PITR base boundary generation exhausted"))?;
        self.base_boundary_issued = true;
        self.base_identity = Some((
            timeline_id,
            archive_epoch_id,
            segment_id,
            self.barrier_generation,
        ));
        Ok(CapturedBaseBoundary {
            timeline_id,
            archive_epoch_id,
            segment_id,
            commit_high_water,
            generation: self.barrier_generation,
        })
    }

    pub(crate) fn release_admission(&mut self) -> Result<()> {
        ensure!(
            self.base_commit_high_water.is_none(),
            "PITR base boundary must release commit admission"
        );
        self.release_admission_inner()
    }

    pub(crate) fn release_base_admission(
        &mut self,
        sequencer: &LsmMvccInner,
        receipt: PublishedBaseReceipt,
    ) -> Result<()> {
        ensure!(
            self.base_commit_high_water.is_some(),
            "PITR base boundary did not stop commit admission"
        );
        ensure!(
            self.state == SealBoundaryState::Sealed,
            "cannot release before sealing"
        );
        ensure!(
            self.base_sequencer_id == Some(sequencer.instance_id()),
            "PITR base release uses a different commit sequencer"
        );
        ensure!(
            self.base_identity
                == Some((
                    receipt.timeline_id(),
                    receipt.archive_epoch_id(),
                    receipt.segment_id(),
                    receipt.generation(),
                )),
            "PITR base release receipt does not match the issued boundary"
        );
        sequencer.resume_commit_admission();
        self.release_admission_inner()
    }

    pub(crate) fn abort_base_admission(&mut self, sequencer: &LsmMvccInner) -> Result<()> {
        ensure!(
            self.state == SealBoundaryState::AdmissionStopped,
            "PITR base abort requires stopped admission"
        );
        ensure!(
            self.base_commit_high_water.is_some()
                && self.base_sequencer_id == Some(sequencer.instance_id()),
            "PITR base abort uses a different or inactive sequencer"
        );
        sequencer.resume_commit_admission();
        self.accounting.resume_admission();
        self.state = SealBoundaryState::AdmissionOpen;
        self.boundary = None;
        self.base_boundary_issued = false;
        self.base_commit_high_water = None;
        self.base_sequencer_id = None;
        self.base_identity = None;
        Ok(())
    }

    fn release_admission_inner(&mut self) -> Result<()> {
        ensure!(
            self.state == SealBoundaryState::Sealed,
            "cannot release before sealing"
        );
        self.accounting.resume_admission();
        self.active_request = None;
        self.state = SealBoundaryState::AdmissionOpen;
        self.last_completed_boundary = self.boundary.take();
        self.base_boundary_issued = false;
        self.base_commit_high_water = None;
        self.base_sequencer_id = None;
        self.base_identity = None;
        Ok(())
    }

    pub(crate) fn state(&self) -> SealBoundaryState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::LsmMvccInner;
    #[test]
    fn spool_accounting_enforces_identity_limits_and_release() {
        let accounting = PitrSpoolAccountant::new(100, 100, 20).unwrap();
        assert_eq!(accounting.remaining_bytes(), 80);
        assert!(accounting.reserve(1, ReservationKind::Batch).is_err());
        assert!(accounting.reserve_batch(0, 1).is_err());
        let batch = accounting.reserve_batch(60, 60).unwrap();
        assert!(accounting.reserve(21, ReservationKind::Segment).is_err());
        assert!(
            accounting
                .release(SpoolReservation {
                    id: 999,
                    logical_bytes: 0,
                    bytes: 60,
                    kind: ReservationKind::Batch
                })
                .is_err()
        );
        accounting.release(batch).unwrap();
        let maintenance = accounting
            .reserve(20, ReservationKind::Maintenance)
            .unwrap();
        accounting.release(maintenance).unwrap();
        assert_eq!(accounting.used_bytes(), 0);
    }
    #[test]
    fn seal_boundary_stops_accounting_and_preserves_priority() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let mut c = SealBoundaryCoordinator::new(Arc::clone(&accounting));
        assert!(c.request(SealRequest::Timer));
        assert!(c.request(SealRequest::Barrier));
        assert_eq!(c.take_request(), Some(SealRequest::Barrier));
        assert!(!c.request(SealRequest::Timer));
        assert!(!c.request(SealRequest::Barrier));
        assert!(c.request(SealRequest::Shutdown));
        c.complete_request();
        assert_eq!(c.take_request(), Some(SealRequest::Shutdown));
        c.stop_admission(0).unwrap();
        assert!(accounting.reserve_batch(1, 1).is_err());
        c.publish_sealed_boundary(0).unwrap();
        c.release_admission().unwrap();
        assert!(accounting.reserve_batch(1, 1).is_ok());
        assert!(c.request(SealRequest::Barrier));
        assert!(c.stop_admission(0).is_err());
        c.stop_admission(7).unwrap();
    }

    #[test]
    fn admission_owner_issues_one_bound_base_token_per_generation() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let mut coordinator = SealBoundaryCoordinator::new(accounting);
        let sequencer = LsmMvccInner::new(7);
        coordinator.stop_admission_for_base(9, &sequencer).unwrap();
        let token = coordinator
            .issue_base_boundary([2; 16], [3; 16], 9)
            .unwrap();
        assert_eq!(token.segment_id(), 9);
        assert_eq!(token.commit_high_water(), Some(7));
        assert!(
            coordinator
                .issue_base_boundary([2; 16], [3; 16], 9)
                .is_err()
        );
        coordinator.publish_sealed_boundary(9).unwrap();
        let wrong_sequencer = LsmMvccInner::new(7);
        let wrong_release = PublishedBaseReceipt::for_test([2; 16], [3; 16], 9, token.generation());
        assert!(
            coordinator
                .release_base_admission(&wrong_sequencer, wrong_release)
                .is_err()
        );
        assert!(!sequencer.commit_admission_is_open());
        let release = PublishedBaseReceipt::for_test([2; 16], [3; 16], 9, token.generation());
        coordinator
            .release_base_admission(&sequencer, release)
            .unwrap();
        assert!(sequencer.commit_admission_is_open());
        let next_commit = sequencer.reserve_commit_ts().unwrap();
        sequencer.publish_commit_ts(next_commit).unwrap();
        coordinator.stop_admission_for_base(10, &sequencer).unwrap();
        let next = coordinator
            .issue_base_boundary([2; 16], [3; 16], 10)
            .unwrap();
        assert!(next.generation() > token.generation());
    }

    #[test]
    fn base_token_captures_frontier_after_admission_stops() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let mut coordinator = SealBoundaryCoordinator::new(accounting);
        let sequencer = LsmMvccInner::new(7);
        assert!(sequencer.update_commit_ts(8));
        coordinator.stop_admission_for_base(9, &sequencer).unwrap();
        let token = coordinator
            .issue_base_boundary([2; 16], [3; 16], 9)
            .unwrap();
        assert_eq!(token.commit_high_water(), Some(8));
    }

    #[test]
    fn base_barrier_rejects_writer_paused_before_commit_reservation() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let batch = accounting.reserve_batch(1, 1).unwrap();
        let mut coordinator = SealBoundaryCoordinator::new(Arc::clone(&accounting));
        let sequencer = LsmMvccInner::new(7);
        assert!(coordinator.stop_admission_for_base(9, &sequencer).is_err());
        let retry = accounting.reserve_batch(1, 1).unwrap();
        accounting.release(retry).unwrap();
        accounting.release(batch).unwrap();
        coordinator.stop_admission_for_base(9, &sequencer).unwrap();
        assert!(sequencer.reserve_commit_ts().is_err());
        let token = coordinator
            .issue_base_boundary([2; 16], [3; 16], 9)
            .unwrap();
        assert_eq!(token.commit_high_water(), Some(7));
    }

    #[test]
    fn poisoned_base_barrier_rolls_back_admission_gates() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let mut coordinator = SealBoundaryCoordinator::new(Arc::clone(&accounting));
        let sequencer = LsmMvccInner::new(7);
        sequencer.poison_commit_ts(8);
        assert!(coordinator.stop_admission_for_base(9, &sequencer).is_err());
        assert_eq!(coordinator.state(), SealBoundaryState::AdmissionOpen);
        assert!(sequencer.commit_admission_is_open());
        assert!(accounting.reserve_batch(1, 1).is_ok());
    }

    #[test]
    fn aborted_base_barrier_can_retry_same_segment() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let sequencer = LsmMvccInner::new(7);
        let mut coordinator = SealBoundaryCoordinator::new(accounting);

        coordinator.stop_admission_for_base(9, &sequencer).unwrap();
        coordinator.abort_base_admission(&sequencer).unwrap();
        coordinator.stop_admission_for_base(9, &sequencer).unwrap();
    }

    #[test]
    fn poison_during_reserved_commit_drain_aborts_barrier() {
        let accounting = Arc::new(PitrSpoolAccountant::new(100, 100, 20).unwrap());
        let sequencer = Arc::new(LsmMvccInner::new(7));
        let commit_ts = sequencer.reserve_commit_ts().unwrap();
        let thread_accounting = Arc::clone(&accounting);
        let thread_sequencer = Arc::clone(&sequencer);
        let waiter = std::thread::spawn(move || {
            let mut coordinator = SealBoundaryCoordinator::new(thread_accounting);
            let result = coordinator.stop_admission_for_base(9, &thread_sequencer);
            (coordinator, result)
        });
        while accounting.admission_is_open() || sequencer.commit_admission_is_open() {
            std::thread::yield_now();
        }
        sequencer.poison_commit_ts(commit_ts);
        let (coordinator, result) = waiter.join().unwrap();
        assert!(result.is_err());
        assert_eq!(coordinator.state(), SealBoundaryState::AdmissionOpen);
        assert!(accounting.admission_is_open());
        assert!(sequencer.commit_admission_is_open());
    }
}
