//! Dormant PITR source-spool accounting and seal-boundary coordination.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};

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
#[derive(Debug)]
pub(crate) struct SealBoundaryCoordinator {
    accounting: Arc<PitrSpoolAccountant>,
    state: SealBoundaryState,
    pending: Option<SealRequest>,
    active_request: Option<SealRequest>,
    boundary: Option<u64>,
    last_completed_boundary: Option<u64>,
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

    pub(crate) fn release_admission(&mut self) -> Result<()> {
        ensure!(
            self.state == SealBoundaryState::Sealed,
            "cannot release before sealing"
        );
        self.accounting.resume_admission();
        self.active_request = None;
        self.state = SealBoundaryState::AdmissionOpen;
        self.last_completed_boundary = self.boundary.take();
        Ok(())
    }

    pub(crate) fn state(&self) -> SealBoundaryState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
