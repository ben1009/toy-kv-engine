//! Owned v7 resource reservations for admission and maintenance work.
//!
//! The ledger is the accounting primitive for later WAL/runtime integration.
//! Callers reserve all required resources before assigning a ticket or offset,
//! then retain the token until the corresponding work is canceled or durably
//! released. `logical_wal_bytes` includes the worst-case embedded FRONTIER
//! reservation for admitted DATA. Recovery workspace is tracked separately
//! and also counts against the total source-spool limit. Dropping a live token
//! releases its charges; callers can also explicitly release it after retiring
//! the reserved work. Buffer memory can retire separately after terminal I/O
//! and hash consumption, while the token continues to own persistent resources.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use std::sync::atomic::AtomicU64;

use parking_lot::{Condvar, Mutex};

use super::codec::{WAL_V7_FRAME_LEN, data_fragment_count};

/// Resource amounts charged atomically by one reservation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalV7ResourceCharge {
    pub(crate) logical_wal_bytes: u64,
    pub(crate) source_spool_bytes: u64,
    pub(crate) recovery_workspace_bytes: u64,
    pub(crate) buffer_memory_bytes: u64,
    pub(crate) seal_index_bytes: u64,
}

impl WalV7ResourceCharge {
    /// Calculate the resources for one logical batch, including all DATA
    /// frames, one worst-case FRONTIER, logical-batch and DATA/FRONTIER write
    /// buffers, and a seal-index entry. `encoded_batch_bytes` includes the
    /// logical header and entry stream.
    ///
    /// # Errors
    /// Returns an error for lengths outside the wire envelope or arithmetic
    /// overflow. This calculation does not reserve resources or WAL coordinates.
    pub(crate) fn for_batch(encoded_batch_bytes: u64) -> Result<Self, WalV7ReservationError> {
        let fragment_count = u64::from(
            data_fragment_count(encoded_batch_bytes)
                .ok_or(WalV7ReservationError::InvalidBatchLength)?,
        );
        let data_frame_bytes = fragment_count
            .checked_mul(WAL_V7_FRAME_LEN as u64)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        let logical_wal_bytes = data_frame_bytes
            .checked_add(WAL_V7_FRAME_LEN as u64)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        let buffer_memory_bytes = encoded_batch_bytes
            .checked_add(data_frame_bytes)
            .and_then(|bytes| bytes.checked_add(WAL_V7_FRAME_LEN as u64))
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;

        Ok(Self {
            logical_wal_bytes,
            source_spool_bytes: logical_wal_bytes,
            recovery_workspace_bytes: 0,
            buffer_memory_bytes,
            seal_index_bytes: std::mem::size_of::<u64>() as u64,
        })
    }

    fn is_empty(self) -> bool {
        self == Self::default()
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            logical_wal_bytes: self
                .logical_wal_bytes
                .checked_add(other.logical_wal_bytes)?,
            source_spool_bytes: self
                .source_spool_bytes
                .checked_add(other.source_spool_bytes)?,
            recovery_workspace_bytes: self
                .recovery_workspace_bytes
                .checked_add(other.recovery_workspace_bytes)?,
            buffer_memory_bytes: self
                .buffer_memory_bytes
                .checked_add(other.buffer_memory_bytes)?,
            seal_index_bytes: self.seal_index_bytes.checked_add(other.seal_index_bytes)?,
        })
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            logical_wal_bytes: self
                .logical_wal_bytes
                .checked_sub(other.logical_wal_bytes)?,
            source_spool_bytes: self
                .source_spool_bytes
                .checked_sub(other.source_spool_bytes)?,
            recovery_workspace_bytes: self
                .recovery_workspace_bytes
                .checked_sub(other.recovery_workspace_bytes)?,
            buffer_memory_bytes: self
                .buffer_memory_bytes
                .checked_sub(other.buffer_memory_bytes)?,
            seal_index_bytes: self.seal_index_bytes.checked_sub(other.seal_index_bytes)?,
        })
    }
}

/// Independent caps for logical WAL growth and its supporting resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV7ResourceLimits {
    pub(crate) max_logical_wal_bytes: u64,
    /// Includes both source spool bytes and recovery workspace bytes.
    pub(crate) max_source_spool_bytes: u64,
    pub(crate) max_recovery_workspace_bytes: u64,
    /// Total memory cap, including any protected maintenance reserve.
    pub(crate) max_buffer_memory_bytes: u64,
    pub(crate) max_seal_index_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV7ReservationError {
    InvalidLimits,
    InvalidBatchLength,
    EmptyReservation,
    ArithmeticOverflow,
    LogicalWalLimit,
    SourceSpoolLimit { generation: u64 },
    RecoveryWorkspaceLimit,
    BufferMemoryLimit,
    SealIndexLimit,
    ReservationIdExhausted,
    ForeignReservation,
    UnknownReservation,
    ReservationMismatch,
    ReservationOwnerRetired,
    ActiveOperationsOutstanding,
    AccountingUnderflow,
}

impl fmt::Display for WalV7ReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "invalid v7 resource limits",
            Self::InvalidBatchLength => "invalid v7 logical batch length",
            Self::EmptyReservation => "v7 resource reservation is empty",
            Self::ArithmeticOverflow => "v7 resource accounting overflow",
            Self::LogicalWalLimit => "v7 logical WAL limit exceeded",
            Self::SourceSpoolLimit { .. } => "v7 source spool limit exceeded",
            Self::RecoveryWorkspaceLimit => "v7 recovery workspace limit exceeded",
            Self::BufferMemoryLimit => "v7 buffer memory limit exceeded",
            Self::SealIndexLimit => "v7 seal index limit exceeded",
            Self::ReservationIdExhausted => "v7 reservation ID exhausted",
            Self::ForeignReservation => "v7 reservation belongs to another resource ledger",
            Self::UnknownReservation => "unknown or already released v7 reservation",
            Self::ReservationMismatch => "v7 reservation identity mismatch",
            Self::ReservationOwnerRetired => "v7 reservation owner has been retired",
            Self::ActiveOperationsOutstanding => {
                "v7 reservation owner still has active batch operations"
            }
            Self::AccountingUnderflow => "v7 resource accounting underflow",
        })
    }
}

impl Error for WalV7ReservationError {}

#[must_use = "a v7 reservation must stay owned until its work is canceled or retired"]
#[derive(Debug)]
pub(crate) struct WalV7Reservation {
    id: u64,
    charge: WalV7ResourceCharge,
    ledger: Arc<WalV7ResourceLedgerInner>,
    active: bool,
}

impl PartialEq for WalV7Reservation {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.charge == other.charge
            && Arc::ptr_eq(&self.ledger, &other.ledger)
            && self.active == other.active
    }
}

impl Eq for WalV7Reservation {}

#[derive(Debug)]
struct LedgerState {
    next_id: u64,
    charged: WalV7ResourceCharge,
    reservations: BTreeMap<u64, WalV7ResourceCharge>,
    source_spool_generation: u64,
    source_spool_work_generation: u64,
}

impl LedgerState {
    fn source_spool_progress_changed(&self, generation: u64, work_generation: Option<u64>) -> bool {
        self.source_spool_generation != generation
            || work_generation
                .is_some_and(|generation| self.source_spool_work_generation != generation)
    }
}

#[derive(Debug)]
struct WalV7ResourceLedgerInner {
    limits: WalV7ResourceLimits,
    /// Buffer headroom ordinary admissions cannot consume.
    maintenance_buffer_memory_reserve: u64,
    #[cfg(test)]
    test_max_seal_index_bytes: AtomicU64,
    state: Mutex<LedgerState>,
    source_spool_changed: Condvar,
    async_source_spool_changed: tokio::sync::Notify,
    #[cfg(test)]
    source_spool_waiters: AtomicU64,
}

/// Thread-safe ledger for owned, multi-resource reservations.
#[derive(Clone, Debug)]
pub(crate) struct WalV7ResourceLedger {
    inner: Arc<WalV7ResourceLedgerInner>,
}

/// Keeps reconstructed charges for PITR source files on the same ledger used
/// by the Active v7 writer. Segment charges stay owned until durable source
/// cleanup; startup staging and unattributed recovery images have separate
/// lifetimes because they do not necessarily correspond to a manifest segment.
#[derive(Debug)]
pub(crate) struct WalV7RetainedResources {
    ledger: WalV7ResourceLedger,
    state: Mutex<WalV7RetainedResourceState>,
}

#[derive(Debug, Default)]
struct WalV7RetainedResourceState {
    by_segment: BTreeMap<u64, Vec<WalV7Reservation>>,
    seal_sidecars_charged: BTreeSet<u64>,
    pending_successors: BTreeMap<u64, WalV7SuccessorReservation>,
    obligation_logical: BTreeMap<u64, WalV7Reservation>,
    startup_staging: Vec<WalV7Reservation>,
    unattributed: Vec<WalV7Reservation>,
}

/// Keeps persistent reservations alive for one installed WAL image,
/// independently of the writer handle that admitted them.
///
/// Recovery and segment lifecycle code retain a clone of this owner until the
/// corresponding source image is durably reclaimed. The writer can therefore
/// be replaced after an uncertain I/O outcome without releasing the charges
/// for bytes that may still be present in the active file. Fresh-inode recovery
/// creates a new owner from the replacement image, retiring the old owner only
/// after the old image is durably removed.
#[derive(Debug)]
pub(crate) struct WalV7ReservationOwner {
    ledger: WalV7ResourceLedger,
    retained_resources: Option<Arc<WalV7RetainedResources>>,
    installed_dev: u64,
    installed_ino: u64,
    state: Mutex<WalV7ReservationOwnerState>,
}

#[derive(Debug)]
struct WalV7ReservationOwnerState {
    retired: bool,
    active_batch_operations: u64,
    active_source_spool_preparations: u64,
    reservations: BTreeMap<u64, WalV7Reservation>,
}

/// A batch reservation already held by its image owner. Declare this guard
/// before allocating the batch and frame buffers so they drop before its
/// buffer charge retires, including when an assigned append unwinds.
pub(crate) struct WalV7OwnedBatchReservation {
    operation: Arc<WalV7BatchOperation>,
    frontier_credit_held: bool,
}

struct WalV7BatchOperation {
    owner: Arc<WalV7ReservationOwner>,
    id: u64,
    admitted: AtomicBool,
    finished: bool,
}

/// Keeps a batch's buffer charge alive while its DATA allocation is owned by
/// the worker, including when ambiguous submission requires leaking it.
pub(crate) struct WalV7BatchBufferPin {
    _operation: Arc<WalV7BatchOperation>,
}

/// Temporary resources held while preparing v7 PITR data. A returned source
/// spool charge can outlive its buffer charge until the sidecar is reclaimed.
/// This guard owns its charges independently of source-image retirement.
#[must_use = "retain the reservation until its PITR seal sidecar is reclaimed"]
pub(crate) struct WalV7OwnedPreparationReservation {
    owner: Arc<WalV7ReservationOwner>,
    reservation: Option<WalV7Reservation>,
    holds_source_spool: bool,
}

/// Capacity reserved before a durable Sealing intent for a successor's
/// canonical image and its atomic-install staging image.
#[must_use = "retain both successor reservations through durable Sealing and installation"]
#[derive(Debug)]
pub(crate) struct WalV7SuccessorReservation {
    persistent: WalV7Reservation,
    staging: WalV7Reservation,
}

impl WalV7ReservationOwner {
    /// Charge an installed Active image exactly once when creating its owner.
    /// Reopening handles on this inode reuses the owner; fresh-inode recovery
    /// must create a replacement owner rather than add another prefix charge.
    pub(crate) fn new(
        ledger: WalV7ResourceLedger,
        installed: &super::install::WalV7InstalledRecovery,
    ) -> anyhow::Result<Self> {
        Self::new_inner(ledger, None, installed, false)
    }

    /// Create an image owner on the reconstructed PITR ledger so live Active
    /// admission shares capacity with recovered source and pin reservations.
    pub(crate) fn new_with_retained_resources(
        retained_resources: Arc<WalV7RetainedResources>,
        installed: &super::install::WalV7InstalledRecovery,
    ) -> anyhow::Result<Self> {
        Self::new_inner(
            retained_resources.ledger(),
            Some(retained_resources),
            installed,
            false,
        )
    }

    /// Create the frozen Active image owner for a durable Sealing obligation.
    /// Its logical bytes are already charged by the retained obligation, so
    /// only physical source and in-memory index usage are charged here.
    pub(crate) fn new_sealing_with_retained_resources(
        retained_resources: Arc<WalV7RetainedResources>,
        installed: &super::install::WalV7InstalledRecovery,
    ) -> anyhow::Result<Self> {
        Self::new_inner(
            retained_resources.ledger(),
            Some(retained_resources),
            installed,
            true,
        )
    }

    fn new_inner(
        ledger: WalV7ResourceLedger,
        retained_resources: Option<Arc<WalV7RetainedResources>>,
        installed: &super::install::WalV7InstalledRecovery,
        sealing: bool,
    ) -> anyhow::Result<Self> {
        let seed = installed.runtime_seed()?;
        let mut reservations = BTreeMap::new();
        let mut recovered_charge = installed.recovered_resource_charge()?;
        if sealing {
            recovered_charge.logical_wal_bytes = 0;
        }
        if recovered_charge != WalV7ResourceCharge::default() {
            let reservation = ledger.reserve(recovered_charge)?;
            reservations.insert(reservation.id, reservation);
        }
        Ok(Self {
            ledger,
            retained_resources,
            installed_dev: seed.installed_dev,
            installed_ino: seed.installed_ino,
            state: Mutex::new(WalV7ReservationOwnerState {
                retired: false,
                active_batch_operations: 0,
                active_source_spool_preparations: 0,
                reservations,
            }),
        })
    }

    pub(crate) fn retained_resources(&self) -> Option<Arc<WalV7RetainedResources>> {
        self.retained_resources.as_ref().map(Arc::clone)
    }

    /// Whether source-bearing preparations or batch I/O must settle before
    /// pressure recovery can reassess the Active boundary. Independent scratch
    /// buffers do not prevent that recovery.
    pub(crate) fn has_in_flight_source_spool_work(&self) -> bool {
        let state = self.state.lock();
        state.active_source_spool_preparations != 0 || state.active_batch_operations != 0
    }

    pub(crate) fn source_spool_generation_changed(&self, generation: u64) -> bool {
        self.ledger.source_spool_generation() != generation
    }

    /// Capture after the rejected attempt unwinds, before checking progress,
    /// so only later work completions prompt another pressure retry.
    pub(crate) fn source_spool_work_generation(&self) -> u64 {
        self.ledger.inner.state.lock().source_spool_work_generation
    }

    pub(crate) fn release_startup_staging_resources(&self) {
        if let Some(resources) = &self.retained_resources {
            resources.release_startup_staging();
        }
    }

    pub(crate) fn validate_image(&self, dev: u64, ino: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            (self.installed_dev, self.installed_ino) == (dev, ino),
            "v7 resource owner belongs to a different installed WAL image"
        );
        Ok(())
    }

    /// Reserve one batch before materializing its logical bytes or frame
    /// buffers, and before assigning its ticket and physical offsets.
    pub(crate) fn reserve_batch(
        self: &Arc<Self>,
        encoded_batch_bytes: u64,
    ) -> Result<WalV7OwnedBatchReservation, WalV7ReservationError> {
        let mut state = self.state.lock();
        if state.retired {
            return Err(WalV7ReservationError::ReservationOwnerRetired);
        }
        let active_batch_operations = state
            .active_batch_operations
            .checked_add(1)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        let reservation = self.ledger.reserve_batch(encoded_batch_bytes)?;
        let id = reservation.id;
        state.reservations.insert(id, reservation);
        state.active_batch_operations = active_batch_operations;
        drop(state);
        Ok(WalV7OwnedBatchReservation {
            operation: Arc::new(WalV7BatchOperation {
                owner: Arc::clone(self),
                id,
                admitted: AtomicBool::new(false),
                finished: false,
            }),
            frontier_credit_held: true,
        })
    }

    /// Reserve temporary canonicalization memory before the point-operation
    /// index is allocated. The caller drops this token as soon as preparation
    /// has consumed the index.
    pub(crate) fn reserve_preparation_buffer(
        self: &Arc<Self>,
        buffer_memory_bytes: u64,
    ) -> Result<Option<WalV7OwnedPreparationReservation>, WalV7ReservationError> {
        self.reserve_preparation_resources(WalV7ResourceCharge {
            buffer_memory_bytes,
            ..WalV7ResourceCharge::default()
        })
    }

    /// Reserve temporary memory and/or persistent source bytes before
    /// preparing and installing a PITR seal sidecar.
    pub(crate) fn reserve_preparation_resources(
        self: &Arc<Self>,
        charge: WalV7ResourceCharge,
    ) -> Result<Option<WalV7OwnedPreparationReservation>, WalV7ReservationError> {
        self.reserve_preparation_resources_with_priority(charge, false)
    }

    /// Reserve bounded maintenance memory without consuming capacity protected
    /// from ordinary batch and range preparation.
    pub(crate) fn reserve_maintenance_preparation_resources(
        self: &Arc<Self>,
        charge: WalV7ResourceCharge,
    ) -> Result<Option<WalV7OwnedPreparationReservation>, WalV7ReservationError> {
        self.reserve_preparation_resources_with_priority(charge, true)
    }

    fn reserve_preparation_resources_with_priority(
        self: &Arc<Self>,
        charge: WalV7ResourceCharge,
        maintenance: bool,
    ) -> Result<Option<WalV7OwnedPreparationReservation>, WalV7ReservationError> {
        if charge.is_empty() {
            return Ok(None);
        }
        let mut state = self.state.lock();
        if state.retired {
            return Err(WalV7ReservationError::ReservationOwnerRetired);
        }
        let holds_source_spool =
            charge.source_spool_bytes != 0 || charge.recovery_workspace_bytes != 0;
        let active_source_spool_preparations = state
            .active_source_spool_preparations
            .checked_add(u64::from(holds_source_spool))
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        let reservation = if maintenance {
            self.ledger.reserve_maintenance(charge)?
        } else {
            self.ledger.reserve(charge)?
        };
        state.active_source_spool_preparations = active_source_spool_preparations;
        drop(state);
        Ok(Some(WalV7OwnedPreparationReservation {
            owner: Arc::clone(self),
            reservation: Some(reservation),
            holds_source_spool,
        }))
    }

    /// Stop admission and release this image's persistent charges after its WAL
    /// and seal have been durably removed. Existing operation guards retain
    /// their charges until their work and buffer use finish.
    pub(crate) fn retire_after_durable_cleanup(&self) -> Result<(), WalV7ReservationError> {
        let mut state = self.state.lock();
        state.retired = true;
        release_retired_reservations(&mut state);
        Ok(())
    }

    /// Release logical WAL quota after the segment is durably archived while
    /// keeping its source-spool and seal-index charges until source cleanup.
    /// Preparers may still own scratch buffers on this image while queued to
    /// write its successor; those independent tokens retain their own charges.
    pub(crate) fn release_logical_after_archive(
        &self,
        segment_id: u64,
    ) -> Result<(), WalV7ReservationError> {
        let mut state = self.state.lock();
        if state.active_batch_operations != 0 {
            return Err(WalV7ReservationError::ActiveOperationsOutstanding);
        }
        for reservation in state.reservations.values_mut() {
            self.ledger.release_logical_wal_bytes(reservation)?;
        }
        drop(state);

        if let Some(resources) = &self.retained_resources {
            resources.release_obligation_logical(segment_id);
        }
        Ok(())
    }

    fn finish_batch_operation(&self, id: u64, admitted: bool) -> Result<(), WalV7ReservationError> {
        let mut state = self.state.lock();
        let result = if admitted {
            match state.reservations.get_mut(&id) {
                Some(reservation) => self.ledger.release_buffer_memory(reservation),
                None => Err(WalV7ReservationError::UnknownReservation),
            }
        } else {
            match state.reservations.remove(&id) {
                Some(reservation) => {
                    // Drop under the owner lock so retirement cannot pass the
                    // active operation before its pre-admission charge is released.
                    drop(reservation);
                    Ok(())
                }
                None => Err(WalV7ReservationError::UnknownReservation),
            }
        };
        let operation_result = state
            .active_batch_operations
            .checked_sub(1)
            .map(|active_batch_operations| state.active_batch_operations = active_batch_operations)
            .ok_or(WalV7ReservationError::AccountingUnderflow);
        release_retired_reservations(&mut state);
        drop(state);
        // A solo admitted batch retains source bytes when only its buffers
        // retire. Wake pressure waiters so they can now reclaim Active.
        self.ledger.notify_source_spool_work_finished();
        result.and(operation_result)
    }

    fn release_frontier_credit(&self, id: u64) -> Result<(), WalV7ReservationError> {
        let mut state = self.state.lock();
        let reservation = state
            .reservations
            .get_mut(&id)
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        self.ledger.release_frontier_credit(reservation)
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> WalV7ResourceCharge {
        self.ledger.snapshot()
    }

    /// Wait until source-spool capacity or in-flight work changes, or
    /// `should_wait` becomes false.
    /// The predicate is polled while blocked so callers can cancel a wait even
    /// when no reservation is released.
    pub(crate) fn wait_for_source_spool_change_while(
        &self,
        generation: u64,
        work_generation: u64,
        should_wait: impl Fn() -> bool,
    ) -> bool {
        self.ledger.wait_for_source_spool_change_while(
            generation,
            Some(work_generation),
            should_wait,
        )
    }

    /// Wait for one bounded interval for a source-spool release.
    pub(crate) fn wait_for_source_spool_change_for(
        &self,
        generation: u64,
        timeout: Duration,
    ) -> bool {
        self.ledger
            .wait_for_source_spool_change_for(generation, timeout)
    }

    /// Async counterpart to [`Self::wait_for_source_spool_change_while`].
    pub(crate) async fn wait_for_source_spool_change_async_while<F>(
        &self,
        generation: u64,
        work_generation: u64,
        closing: &tokio::sync::Notify,
        should_wait: F,
    ) -> bool
    where
        F: Fn() -> bool,
    {
        self.ledger
            .wait_for_source_spool_change_async_while(
                generation,
                Some(work_generation),
                closing,
                should_wait,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) fn source_spool_waiter_count(&self) -> u64 {
        self.ledger.source_spool_waiter_count()
    }
}

impl WalV7OwnedBatchReservation {
    /// Mark the reservation persistent immediately after coordinates advance.
    pub(crate) fn mark_admitted(&mut self) {
        self.operation.admitted.store(true, Ordering::Release);
    }

    pub(crate) fn pin_buffer(&self) -> WalV7BatchBufferPin {
        WalV7BatchBufferPin {
            _operation: Arc::clone(&self.operation),
        }
    }

    /// Retire this ticket's unused marker credit after a coalesced frontier
    /// has durably consumed another ticket's reserved marker frame.
    pub(crate) fn release_unused_frontier_credit(&mut self) -> Result<(), WalV7ReservationError> {
        if !self.operation.admitted.load(Ordering::Acquire) {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        if !self.frontier_credit_held {
            return Ok(());
        }
        self.operation
            .owner
            .release_frontier_credit(self.operation.id)?;
        self.frontier_credit_held = false;
        Ok(())
    }

    /// Retire the coordinator's operation ownership after I/O and hashing.
    /// Any remaining buffer pin defers transient retirement until that DATA
    /// allocation is released. Persistent charges remain in the image owner.
    pub(crate) fn finish_after_io(self) -> Result<(), WalV7ReservationError> {
        if !self.operation.admitted.load(Ordering::Acquire) {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        match Arc::try_unwrap(self.operation) {
            Ok(operation) => operation.finish(),
            Err(operation) => {
                drop(operation);
                Ok(())
            }
        }
    }
}

impl WalV7BatchOperation {
    fn finish(mut self) -> Result<(), WalV7ReservationError> {
        let result = self
            .owner
            .finish_batch_operation(self.id, self.admitted.load(Ordering::Acquire));
        self.finished = true;
        result
    }
}

impl Drop for WalV7BatchOperation {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Only the last coordinator/buffer owner retires this operation.
        // A leaked DATA allocation retains its pin and therefore its charge.
        let _ = self
            .owner
            .finish_batch_operation(self.id, self.admitted.load(Ordering::Acquire));
    }
}

impl Drop for WalV7OwnedPreparationReservation {
    fn drop(&mut self) {
        // Release the temporary ledger token before allowing image retirement.
        drop(self.reservation.take());
        if !self.holds_source_spool {
            return;
        }
        let mut state = self.owner.state.lock();
        if let Some(active_preparations) = state.active_source_spool_preparations.checked_sub(1) {
            state.active_source_spool_preparations = active_preparations;
        } else {
            debug_assert!(false, "v7 preparation-operation accounting underflow");
        }
        release_retired_reservations(&mut state);
        drop(state);
        self.owner.ledger.notify_source_spool_work_finished();
    }
}

impl WalV7OwnedPreparationReservation {
    /// Retire only the temporary-memory part of this preparation reservation.
    /// Persistent source-spool bytes stay charged until the guard is dropped.
    pub(crate) fn release_buffer_memory(&mut self) -> Result<(), WalV7ReservationError> {
        let Some(reservation) = self.reservation.as_mut() else {
            return Ok(());
        };
        self.owner.ledger.release_buffer_memory(reservation)
    }
}

fn release_retired_reservations(state: &mut WalV7ReservationOwnerState) {
    if state.retired && state.active_batch_operations == 0 {
        // Batch I/O may still use the image's buffers and reservations.
        // Preparers own separate tokens; keeping those tokens alive must not
        // retain a durably reclaimed image's spool or seal-index quota.
        let reservations = std::mem::take(&mut state.reservations);
        // Release under the owner lock so another idempotent retirement cannot
        // return before the ledger charges have actually been dropped.
        drop(reservations);
    }
}

impl WalV7ResourceLedger {
    pub(crate) fn new(limits: WalV7ResourceLimits) -> Result<Self, WalV7ReservationError> {
        Self::new_with_maintenance_buffer_reserve(limits, 0)
    }

    /// Construct a ledger with buffer capacity protected for bounded
    /// maintenance work such as sealing an Active WAL during rotation.
    pub(crate) fn new_with_maintenance_buffer_reserve(
        limits: WalV7ResourceLimits,
        maintenance_buffer_memory_reserve: u64,
    ) -> Result<Self, WalV7ReservationError> {
        if limits.max_logical_wal_bytes == 0
            || limits.max_source_spool_bytes == 0
            || limits.max_recovery_workspace_bytes > limits.max_source_spool_bytes
            || maintenance_buffer_memory_reserve > limits.max_buffer_memory_bytes
        {
            return Err(WalV7ReservationError::InvalidLimits);
        }

        Ok(Self {
            inner: Arc::new(WalV7ResourceLedgerInner {
                limits,
                maintenance_buffer_memory_reserve,
                #[cfg(test)]
                test_max_seal_index_bytes: AtomicU64::new(u64::MAX),
                state: Mutex::new(LedgerState {
                    next_id: 1,
                    charged: WalV7ResourceCharge::default(),
                    reservations: BTreeMap::new(),
                    source_spool_generation: 0,
                    source_spool_work_generation: 0,
                }),
                source_spool_changed: Condvar::new(),
                async_source_spool_changed: tokio::sync::Notify::new(),
                #[cfg(test)]
                source_spool_waiters: AtomicU64::new(0),
            }),
        })
    }

    /// Atomically charge every resource before a caller assigns WAL coordinates.
    pub(crate) fn reserve(
        &self,
        charge: WalV7ResourceCharge,
    ) -> Result<WalV7Reservation, WalV7ReservationError> {
        self.reserve_with_priority(charge, false)
    }

    pub(crate) fn reserve_maintenance(
        &self,
        charge: WalV7ResourceCharge,
    ) -> Result<WalV7Reservation, WalV7ReservationError> {
        self.reserve_with_priority(charge, true)
    }

    fn reserve_with_priority(
        &self,
        charge: WalV7ResourceCharge,
        maintenance: bool,
    ) -> Result<WalV7Reservation, WalV7ReservationError> {
        if charge.is_empty() {
            return Err(WalV7ReservationError::EmptyReservation);
        }

        let mut state = self.inner.state.lock();
        let next = state
            .charged
            .checked_add(charge)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        if next.logical_wal_bytes > self.inner.limits.max_logical_wal_bytes {
            return Err(WalV7ReservationError::LogicalWalLimit);
        }
        if next
            .source_spool_bytes
            .checked_add(next.recovery_workspace_bytes)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?
            > self.inner.limits.max_source_spool_bytes
        {
            return Err(WalV7ReservationError::SourceSpoolLimit {
                generation: state.source_spool_generation,
            });
        }
        if next.recovery_workspace_bytes > self.inner.limits.max_recovery_workspace_bytes {
            return Err(WalV7ReservationError::RecoveryWorkspaceLimit);
        }
        let buffer_limit = if maintenance {
            self.inner.limits.max_buffer_memory_bytes
        } else {
            self.inner.limits.max_buffer_memory_bytes - self.inner.maintenance_buffer_memory_reserve
        };
        if next.buffer_memory_bytes > buffer_limit {
            return Err(WalV7ReservationError::BufferMemoryLimit);
        }
        let max_seal_index_bytes = self.inner.limits.max_seal_index_bytes;
        #[cfg(test)]
        let max_seal_index_bytes =
            max_seal_index_bytes.min(self.inner.test_max_seal_index_bytes.load(Ordering::Acquire));
        if next.seal_index_bytes > max_seal_index_bytes {
            return Err(WalV7ReservationError::SealIndexLimit);
        }
        let next_id = state
            .next_id
            .checked_add(1)
            .ok_or(WalV7ReservationError::ReservationIdExhausted)?;
        let id = state.next_id;

        state.charged = next;
        state.next_id = next_id;
        let previous = state.reservations.insert(id, charge);
        debug_assert!(previous.is_none());

        Ok(WalV7Reservation {
            id,
            charge,
            ledger: Arc::clone(&self.inner),
            active: true,
        })
    }

    /// Reserve the worst-case resources for one framed DATA batch before its
    /// caller assigns a ticket or file offset.
    ///
    /// The reservation includes all fixed-size DATA frames, one FRONTIER
    /// frame, logical-batch and DATA/FRONTIER write-buffer memory, and one
    /// first-DATA seal-index entry. FRONTIER reservations are per admitted batch even when a later
    /// group commit coalesces several batches into one marker.
    /// `encoded_batch_bytes` includes the logical batch header and entry stream.
    /// The caller must validate batch contents and reserve any additional
    /// maintenance resources before assigning WAL coordinates.
    ///
    /// # Errors
    /// Returns an error for lengths outside the wire envelope, accounting
    /// overflow, or a resource limit exceeded. Rejection changes no charges.
    pub(crate) fn reserve_batch(
        &self,
        encoded_batch_bytes: u64,
    ) -> Result<WalV7Reservation, WalV7ReservationError> {
        self.reserve(WalV7ResourceCharge::for_batch(encoded_batch_bytes)?)
    }

    /// Atomically retire a reservation's buffer charge after its buffers are
    /// safe to release. Persistent resource charges remain owned by the token.
    /// Repeating this operation on a live token with no buffers is harmless.
    ///
    /// # Errors
    /// Rejects foreign, retired, or mismatched tokens without changing charges.
    pub(crate) fn release_buffer_memory(
        &self,
        reservation: &mut WalV7Reservation,
    ) -> Result<(), WalV7ReservationError> {
        if !Arc::ptr_eq(&reservation.ledger, &self.inner) {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        let mut state = self.inner.state.lock();
        let issued_charge = state
            .reservations
            .get(&reservation.id)
            .copied()
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        if issued_charge != reservation.charge {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        if issued_charge.buffer_memory_bytes == 0 {
            return Ok(());
        }
        let retired = WalV7ResourceCharge {
            buffer_memory_bytes: issued_charge.buffer_memory_bytes,
            ..WalV7ResourceCharge::default()
        };
        let retained = WalV7ResourceCharge {
            buffer_memory_bytes: 0,
            ..issued_charge
        };
        let next = state
            .charged
            .checked_sub(retired)
            .ok_or(WalV7ReservationError::AccountingUnderflow)?;

        if retained.is_empty() {
            state.reservations.remove(&reservation.id);
            reservation.active = false;
        } else {
            state.reservations.insert(reservation.id, retained);
        }
        state.charged = next;
        reservation.charge = retained;
        Ok(())
    }

    /// Retire a reservation's logical WAL quota after durable archive
    /// publication. Physical source-spool and seal-index charges remain owned
    /// by the token until durable local cleanup.
    pub(crate) fn release_logical_wal_bytes(
        &self,
        reservation: &mut WalV7Reservation,
    ) -> Result<(), WalV7ReservationError> {
        if !Arc::ptr_eq(&reservation.ledger, &self.inner) {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        let mut state = self.inner.state.lock();
        let issued_charge = state
            .reservations
            .get(&reservation.id)
            .copied()
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        if issued_charge != reservation.charge {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        if issued_charge.logical_wal_bytes == 0 {
            return Ok(());
        }
        let retired = WalV7ResourceCharge {
            logical_wal_bytes: issued_charge.logical_wal_bytes,
            ..WalV7ResourceCharge::default()
        };
        let retained = WalV7ResourceCharge {
            logical_wal_bytes: 0,
            ..issued_charge
        };
        let next = state
            .charged
            .checked_sub(retired)
            .ok_or(WalV7ReservationError::AccountingUnderflow)?;

        if retained.is_empty() {
            state.reservations.remove(&reservation.id);
            reservation.active = false;
        } else {
            state.reservations.insert(reservation.id, retained);
        }
        state.charged = next;
        reservation.charge = retained;
        Ok(())
    }

    fn release_frontier_credit(
        &self,
        reservation: &mut WalV7Reservation,
    ) -> Result<(), WalV7ReservationError> {
        if !Arc::ptr_eq(&reservation.ledger, &self.inner) {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        let credit = WAL_V7_FRAME_LEN as u64;
        let mut state = self.inner.state.lock();
        let issued_charge = state
            .reservations
            .get(&reservation.id)
            .copied()
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        if issued_charge != reservation.charge {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        let retired = WalV7ResourceCharge {
            logical_wal_bytes: credit,
            source_spool_bytes: credit,
            ..WalV7ResourceCharge::default()
        };
        let retained = issued_charge
            .checked_sub(retired)
            .ok_or(WalV7ReservationError::AccountingUnderflow)?;
        let next = state
            .charged
            .checked_sub(retired)
            .ok_or(WalV7ReservationError::AccountingUnderflow)?;
        state.reservations.insert(reservation.id, retained);
        state.charged = next;
        reservation.charge = retained;
        state.source_spool_generation = state.source_spool_generation.wrapping_add(1);
        drop(state);
        self.inner.source_spool_changed.notify_all();
        self.inner.async_source_spool_changed.notify_waiters();
        Ok(())
    }

    /// Release a reservation only after its owner has canceled or retired it.
    pub(crate) fn release(
        &self,
        reservation: &mut WalV7Reservation,
    ) -> Result<(), WalV7ReservationError> {
        if !Arc::ptr_eq(&reservation.ledger, &self.inner) {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        release_reservation(&self.inner, reservation.id, reservation.charge)?;
        reservation.active = false;
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> WalV7ResourceCharge {
        self.inner.state.lock().charged
    }

    fn source_spool_generation(&self) -> u64 {
        self.inner.state.lock().source_spool_generation
    }

    fn notify_source_spool_work_finished(&self) {
        let mut state = self.inner.state.lock();
        state.source_spool_work_generation = state.source_spool_work_generation.wrapping_add(1);
        drop(state);
        self.inner.source_spool_changed.notify_all();
        self.inner.async_source_spool_changed.notify_waiters();
    }

    /// Wait for a release that may make a failed source-spool reservation fit.
    /// The generation comes from the failed reservation check, preventing a
    /// release between rejection and waiting from being lost.
    pub(crate) fn wait_for_source_spool_change(&self, generation: u64) {
        self.wait_for_source_spool_change_while(generation, None, || true);
    }

    pub(crate) fn wait_for_source_spool_change_while(
        &self,
        generation: u64,
        work_generation: Option<u64>,
        should_wait: impl Fn() -> bool,
    ) -> bool {
        #[cfg(test)]
        let _waiter = SourceSpoolWaiter::new(&self.inner.source_spool_waiters);
        let mut state = self.inner.state.lock();
        while !state.source_spool_progress_changed(generation, work_generation) && should_wait() {
            self.inner
                .source_spool_changed
                .wait_for(&mut state, Duration::from_millis(50));
        }
        state.source_spool_progress_changed(generation, work_generation)
    }

    pub(crate) fn wait_for_source_spool_change_for(
        &self,
        generation: u64,
        timeout: Duration,
    ) -> bool {
        #[cfg(test)]
        let _waiter = SourceSpoolWaiter::new(&self.inner.source_spool_waiters);
        let mut state = self.inner.state.lock();
        let started = std::time::Instant::now();
        while state.source_spool_generation == generation {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            self.inner
                .source_spool_changed
                .wait_for(&mut state, remaining);
        }
        state.source_spool_generation != generation
    }

    /// Async counterpart to [`Self::wait_for_source_spool_change`].
    pub(crate) async fn wait_for_source_spool_change_async(&self, generation: u64) {
        let closing = tokio::sync::Notify::new();
        self.wait_for_source_spool_change_async_while(generation, None, &closing, || true)
            .await;
    }

    pub(crate) async fn wait_for_source_spool_change_async_while<F>(
        &self,
        generation: u64,
        work_generation: Option<u64>,
        closing: &tokio::sync::Notify,
        should_wait: F,
    ) -> bool
    where
        F: Fn() -> bool,
    {
        #[cfg(test)]
        let _waiter = SourceSpoolWaiter::new(&self.inner.source_spool_waiters);
        let notification = self.inner.async_source_spool_changed.notified();
        tokio::pin!(notification);
        let closing_notification = closing.notified();
        tokio::pin!(closing_notification);
        loop {
            notification.as_mut().enable();
            closing_notification.as_mut().enable();
            if self
                .inner
                .state
                .lock()
                .source_spool_progress_changed(generation, work_generation)
            {
                return true;
            }
            if !should_wait() {
                return false;
            }
            tokio::select! {
                () = notification.as_mut() => {
                    notification.set(self.inner.async_source_spool_changed.notified());
                }
                () = closing_notification.as_mut() => {
                    return self.inner.state.lock().source_spool_progress_changed(generation, work_generation);
                }
            }
        }
    }

    #[cfg(test)]
    fn source_spool_waiter_count(&self) -> u64 {
        self.inner.source_spool_waiters.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn set_max_seal_index_bytes_for_test(&self, max_seal_index_bytes: u64) {
        self.inner
            .test_max_seal_index_bytes
            .store(max_seal_index_bytes, Ordering::Release);
    }
}

impl WalV7RetainedResources {
    pub(crate) fn new(ledger: WalV7ResourceLedger) -> Arc<Self> {
        Arc::new(Self {
            ledger,
            state: Mutex::new(WalV7RetainedResourceState::default()),
        })
    }

    pub(crate) fn ledger(&self) -> WalV7ResourceLedger {
        self.ledger.clone()
    }

    pub(crate) fn reserve_segment(
        &self,
        segment_id: u64,
        charge: WalV7ResourceCharge,
    ) -> Result<(), WalV7ReservationError> {
        if charge.is_empty() {
            return Ok(());
        }
        let reservation = self.ledger.reserve(charge)?;
        self.state
            .lock()
            .by_segment
            .entry(segment_id)
            .or_default()
            .push(reservation);
        Ok(())
    }

    /// Transfer a seal's persistent spool charge to the source segment and
    /// retire its temporary memory charge once the prepared seal is dropped.
    pub(crate) fn retain_preparation_reservation(
        &self,
        segment_id: u64,
        mut reservation: WalV7OwnedPreparationReservation,
    ) -> Result<(), WalV7ReservationError> {
        if !Arc::ptr_eq(&reservation.owner.ledger.inner, &self.ledger.inner) {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        let token = reservation
            .reservation
            .as_ref()
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        if !Arc::ptr_eq(&token.ledger, &self.ledger.inner) {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        reservation.release_buffer_memory()?;
        let token = reservation
            .reservation
            .take()
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        let mut state = self.state.lock();
        state.seal_sidecars_charged.insert(segment_id);
        if token.charge.is_empty() {
            drop(token);
        } else {
            state.by_segment.entry(segment_id).or_default().push(token);
        }
        drop(state);
        // The preparation operation is complete, while its ledger token now
        // remains owned by the source segment through durable reclamation.
        drop(reservation);

        Ok(())
    }

    /// Mark a recovered seal sidecar as included in its segment's reconstructed
    /// source-spool reservation.
    pub(crate) fn mark_seal_sidecar_charged(&self, segment_id: u64) {
        self.state.lock().seal_sidecars_charged.insert(segment_id);
    }

    /// Retain the source-spool allocation of an already installed seal
    /// sidecar. The marker and reservation are installed atomically so a
    /// preparation failure cannot leave an on-disk sidecar uncharged.
    pub(crate) fn retain_existing_seal_sidecar_charge(
        &self,
        segment_id: u64,
        source_spool_bytes: u64,
    ) -> Result<(), WalV7ReservationError> {
        let mut state = self.state.lock();
        if state.seal_sidecars_charged.contains(&segment_id) {
            return Ok(());
        }

        let reservation = self.ledger.reserve(WalV7ResourceCharge {
            source_spool_bytes,
            ..WalV7ResourceCharge::default()
        })?;
        state
            .by_segment
            .entry(segment_id)
            .or_default()
            .push(reservation);
        state.seal_sidecars_charged.insert(segment_id);
        Ok(())
    }

    /// Whether the current resource ledger already owns the canonical seal
    /// sidecar's source-spool charge for this segment.
    pub(crate) fn has_seal_sidecar_charge(&self, segment_id: u64) -> bool {
        self.state
            .lock()
            .seal_sidecars_charged
            .contains(&segment_id)
    }

    /// Reserve both the successor's empty installed image and its atomic
    /// install staging extent before the predecessor's Sealing intent is
    /// durable. The returned token releases both charges if preparation is
    /// abandoned before that intent.
    pub(crate) fn reserve_successor(
        &self,
        segment_id: u64,
        source_spool_bytes: u64,
    ) -> Result<WalV7SuccessorReservation, WalV7ReservationError> {
        if source_spool_bytes == 0 {
            return Err(WalV7ReservationError::EmptyReservation);
        }
        {
            let state = self.state.lock();
            if state.pending_successors.contains_key(&segment_id)
                || state.by_segment.contains_key(&segment_id)
            {
                return Err(WalV7ReservationError::ReservationMismatch);
            }
        }
        let charge = WalV7ResourceCharge {
            source_spool_bytes,
            ..WalV7ResourceCharge::default()
        };
        let persistent = self.ledger.reserve(charge)?;
        let staging = self.ledger.reserve(charge)?;
        Ok(WalV7SuccessorReservation {
            persistent,
            staging,
        })
    }

    /// Retain a pre-Sealing reservation after the manifest has recorded the
    /// durable Sealing obligation. Pending successor charges survive prepared
    /// object drop and remain charged until installation or source cleanup.
    pub(crate) fn retain_pending_successor(
        &self,
        segment_id: u64,
        reservation: WalV7SuccessorReservation,
    ) -> Result<(), WalV7ReservationError> {
        if !Arc::ptr_eq(&reservation.persistent.ledger, &self.ledger.inner)
            || !Arc::ptr_eq(&reservation.staging.ledger, &self.ledger.inner)
        {
            return Err(WalV7ReservationError::ForeignReservation);
        }
        let mut state = self.state.lock();
        if state.pending_successors.contains_key(&segment_id)
            || state.by_segment.contains_key(&segment_id)
        {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        state.pending_successors.insert(segment_id, reservation);
        Ok(())
    }

    pub(crate) fn has_pending_successor(&self, segment_id: u64) -> bool {
        self.state
            .lock()
            .pending_successors
            .contains_key(&segment_id)
    }

    pub(crate) fn has_segment_reservation(&self, segment_id: u64) -> bool {
        self.state
            .lock()
            .by_segment
            .get(&segment_id)
            .is_some_and(|reservations| !reservations.is_empty())
    }

    /// Promote a successfully installed successor's permanent image charge
    /// to its segment lifetime and release its now-clean staging charge.
    pub(crate) fn activate_pending_successor(
        &self,
        segment_id: u64,
    ) -> Result<(), WalV7ReservationError> {
        let mut state = self.state.lock();
        if state.by_segment.contains_key(&segment_id) {
            return if state.pending_successors.contains_key(&segment_id) {
                Err(WalV7ReservationError::ReservationMismatch)
            } else {
                Ok(())
            };
        }
        let reservation = state
            .pending_successors
            .remove(&segment_id)
            .ok_or(WalV7ReservationError::UnknownReservation)?;
        let WalV7SuccessorReservation {
            persistent,
            staging,
        } = reservation;
        state
            .by_segment
            .entry(segment_id)
            .or_default()
            .push(persistent);
        drop(state);
        drop(staging);
        Ok(())
    }

    pub(crate) fn reserve_startup_staging(
        &self,
        charge: WalV7ResourceCharge,
    ) -> Result<(), WalV7ReservationError> {
        if charge.is_empty() {
            return Ok(());
        }
        let reservation = self.ledger.reserve(charge)?;
        self.state.lock().startup_staging.push(reservation);
        Ok(())
    }

    pub(crate) fn reserve_obligation_logical(
        &self,
        segment_id: u64,
        logical_wal_bytes: u64,
    ) -> Result<(), WalV7ReservationError> {
        if logical_wal_bytes == 0 {
            return Ok(());
        }
        let reservation = self.ledger.reserve(WalV7ResourceCharge {
            logical_wal_bytes,
            ..WalV7ResourceCharge::default()
        })?;
        let previous = self
            .state
            .lock()
            .obligation_logical
            .insert(segment_id, reservation);
        debug_assert!(previous.is_none());
        Ok(())
    }

    pub(crate) fn release_obligation_logical(&self, segment_id: u64) {
        drop(self.state.lock().obligation_logical.remove(&segment_id));
    }

    pub(crate) fn reserve_unattributed(
        &self,
        charge: WalV7ResourceCharge,
    ) -> Result<(), WalV7ReservationError> {
        if charge.is_empty() {
            return Ok(());
        }
        let reservation = self.ledger.reserve(charge)?;
        self.state.lock().unattributed.push(reservation);
        Ok(())
    }

    /// Release a segment's reconstructed charges only after its source files
    /// and directory entry have been durably removed.
    pub(crate) fn release_segment(&self, segment_id: u64) {
        let mut state = self.state.lock();
        drop(state.by_segment.remove(&segment_id));
        state.seal_sidecars_charged.remove(&segment_id);
        drop(state.pending_successors.remove(&segment_id));
        drop(state.obligation_logical.remove(&segment_id));
    }

    /// Startup install staging files are removed and the directory synced
    /// before this is called. Their temporary reservations can then retire.
    pub(crate) fn release_startup_staging(&self) {
        drop(std::mem::take(&mut self.state.lock().startup_staging));
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> WalV7ResourceCharge {
        self.ledger.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn set_max_seal_index_bytes_for_test(&self, max_seal_index_bytes: u64) {
        self.ledger
            .set_max_seal_index_bytes_for_test(max_seal_index_bytes);
    }
}

impl Drop for WalV7Reservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if release_reservation(&self.ledger, self.id, self.charge).is_ok() {
            self.active = false;
        }
    }
}

fn release_reservation(
    ledger: &WalV7ResourceLedgerInner,
    id: u64,
    charge: WalV7ResourceCharge,
) -> Result<(), WalV7ReservationError> {
    let mut state = ledger.state.lock();
    let issued_charge = state
        .reservations
        .get(&id)
        .copied()
        .ok_or(WalV7ReservationError::UnknownReservation)?;
    if issued_charge != charge {
        return Err(WalV7ReservationError::ReservationMismatch);
    }
    let next = state
        .charged
        .checked_sub(charge)
        .ok_or(WalV7ReservationError::AccountingUnderflow)?;

    state.reservations.remove(&id);
    state.charged = next;
    let source_spool_released =
        charge.source_spool_bytes > 0 || charge.recovery_workspace_bytes > 0;
    if source_spool_released {
        state.source_spool_generation = state.source_spool_generation.wrapping_add(1);
    }
    drop(state);
    if source_spool_released {
        ledger.source_spool_changed.notify_all();
        ledger.async_source_spool_changed.notify_waiters();
    }
    Ok(())
}

#[cfg(test)]
struct SourceSpoolWaiter<'a> {
    waiters: &'a AtomicU64,
}

#[cfg(test)]
impl<'a> SourceSpoolWaiter<'a> {
    fn new(waiters: &'a AtomicU64) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self { waiters }
    }
}

#[cfg(test)]
impl Drop for SourceSpoolWaiter<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    fn limits() -> WalV7ResourceLimits {
        WalV7ResourceLimits {
            max_logical_wal_bytes: 100,
            max_source_spool_bytes: 100,
            max_recovery_workspace_bytes: 80,
            max_buffer_memory_bytes: 40,
            max_seal_index_bytes: 32,
        }
    }

    #[test]
    fn source_spool_wait_wakes_after_a_quota_release() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let held = ledger.reserve(charge(0, 100, 0, 0, 0)).unwrap();
        let generation = match ledger.reserve(charge(0, 1, 0, 0, 0)) {
            Err(WalV7ReservationError::SourceSpoolLimit { generation }) => generation,
            other => panic!("expected source-spool rejection, got {other:?}"),
        };

        let waiting_ledger = ledger.clone();
        let waiter = std::thread::spawn(move || {
            waiting_ledger.wait_for_source_spool_change(generation);
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while ledger.source_spool_waiter_count() == 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        let registered = ledger.source_spool_waiter_count() > 0;

        drop(held);
        waiter.join().unwrap();
        assert!(registered, "the synchronous waiter was not registered");
        let retry = ledger.reserve(charge(0, 1, 0, 0, 0)).unwrap();
        assert_eq!(ledger.snapshot().source_spool_bytes, 1);
        drop(retry);
    }

    #[tokio::test]
    async fn async_source_spool_wait_wakes_after_a_quota_release() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let held = ledger.reserve(charge(0, 100, 0, 0, 0)).unwrap();
        let generation = match ledger.reserve(charge(0, 1, 0, 0, 0)) {
            Err(WalV7ReservationError::SourceSpoolLimit { generation }) => generation,
            other => panic!("expected source-spool rejection, got {other:?}"),
        };

        let waiting_ledger = ledger.clone();
        let waiter = tokio::spawn(async move {
            waiting_ledger
                .wait_for_source_spool_change_async(generation)
                .await;
        });
        let registered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while ledger.source_spool_waiter_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();

        drop(held);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("async quota waiter did not wake")
            .unwrap();
        assert!(registered, "the asynchronous waiter was not registered");
        let retry = ledger.reserve(charge(0, 1, 0, 0, 0)).unwrap();
        assert_eq!(ledger.snapshot().source_spool_bytes, 1);
        drop(retry);
    }

    #[tokio::test]
    async fn source_spool_wait_observes_batch_completion_before_waiting() {
        let ledger = WalV7ResourceLedger::new(batch_limits()).unwrap();
        let owner = Arc::new(WalV7ReservationOwner {
            ledger: ledger.clone(),
            retained_resources: None,
            installed_dev: 0,
            installed_ino: 0,
            state: Mutex::new(WalV7ReservationOwnerState {
                retired: false,
                active_batch_operations: 0,
                active_source_spool_preparations: 0,
                reservations: BTreeMap::new(),
            }),
        });
        let mut batch = owner.reserve_batch(128).unwrap();
        batch.mark_admitted();
        let generation = ledger.source_spool_generation();
        let work_generation = owner.source_spool_work_generation();

        // Complete before either wait registers its notification. The source
        // bytes remain charged until pressure recovery reclaims the image.
        batch.finish_after_io().unwrap();
        assert_eq!(ledger.snapshot().source_spool_bytes, 8192);
        assert!(!owner.source_spool_generation_changed(generation));
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        assert!(
            owner.wait_for_source_spool_change_while(generation, work_generation, || {
                std::time::Instant::now() < deadline
            })
        );

        let closing = tokio::sync::Notify::new();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                owner.wait_for_source_spool_change_async_while(
                    generation,
                    work_generation,
                    &closing,
                    || true
                ),
            )
            .await
            .expect("batch completion before registration was lost")
        );
    }

    fn charge(
        logical: u64,
        source: u64,
        workspace: u64,
        buffers: u64,
        index: u64,
    ) -> WalV7ResourceCharge {
        WalV7ResourceCharge {
            logical_wal_bytes: logical,
            source_spool_bytes: source,
            recovery_workspace_bytes: workspace,
            buffer_memory_bytes: buffers,
            seal_index_bytes: index,
        }
    }

    fn batch_limits() -> WalV7ResourceLimits {
        WalV7ResourceLimits {
            max_logical_wal_bytes: 16384,
            max_source_spool_bytes: 20480,
            max_recovery_workspace_bytes: 4096,
            max_buffer_memory_bytes: 32768,
            max_seal_index_bytes: 16,
        }
    }

    #[test]
    fn ordinary_reservations_preserve_maintenance_buffer_headroom() {
        let limits = WalV7ResourceLimits {
            max_buffer_memory_bytes: 100,
            ..limits()
        };
        let ledger = WalV7ResourceLedger::new_with_maintenance_buffer_reserve(limits, 40).unwrap();

        let ordinary = ledger.reserve(charge(0, 0, 0, 60, 0)).unwrap();
        assert_eq!(
            ledger.reserve(charge(0, 0, 0, 1, 0)).err(),
            Some(WalV7ReservationError::BufferMemoryLimit)
        );

        let maintenance = ledger.reserve_maintenance(charge(0, 0, 0, 40, 0)).unwrap();
        assert_eq!(ledger.snapshot().buffer_memory_bytes, 100);
        assert_eq!(
            ledger.reserve_maintenance(charge(0, 0, 0, 1, 0)).err(),
            Some(WalV7ReservationError::BufferMemoryLimit)
        );

        drop(maintenance);
        drop(ordinary);
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn retained_recovery_resources_release_by_durable_lifetime() {
        let ledger = WalV7ResourceLedger::new(WalV7ResourceLimits {
            max_logical_wal_bytes: 100,
            max_source_spool_bytes: 100,
            max_recovery_workspace_bytes: 80,
            max_buffer_memory_bytes: 40,
            max_seal_index_bytes: 32,
        })
        .unwrap();
        let retained = WalV7RetainedResources::new(ledger.clone());
        retained.reserve_segment(7, charge(0, 15, 0, 0, 0)).unwrap();
        retained.reserve_obligation_logical(7, 20).unwrap();
        retained
            .reserve_startup_staging(charge(0, 10, 0, 0, 0))
            .unwrap();
        retained
            .reserve_unattributed(charge(0, 5, 0, 0, 0))
            .unwrap();

        assert_eq!(retained.snapshot(), charge(20, 30, 0, 0, 0));
        retained.release_startup_staging();
        retained.release_obligation_logical(7);
        assert_eq!(retained.snapshot(), charge(0, 20, 0, 0, 0));

        retained.release_segment(7);
        assert_eq!(retained.snapshot(), charge(0, 5, 0, 0, 0));
    }

    #[test]
    fn archived_image_releases_logical_quota_but_retains_source_spool() {
        let ledger = WalV7ResourceLedger::new(WalV7ResourceLimits {
            max_logical_wal_bytes: 8192,
            max_source_spool_bytes: 16384,
            max_recovery_workspace_bytes: 4096,
            max_buffer_memory_bytes: 16384,
            max_seal_index_bytes: 32,
        })
        .unwrap();
        let retained = WalV7RetainedResources::new(ledger.clone());
        retained.reserve_obligation_logical(7, 4096).unwrap();
        let reservation = ledger.reserve(charge(4096, 4096, 0, 0, 8)).unwrap();
        let owner = Arc::new(WalV7ReservationOwner {
            ledger: ledger.clone(),
            retained_resources: Some(Arc::clone(&retained)),
            installed_dev: 0,
            installed_ino: 0,
            state: Mutex::new(WalV7ReservationOwnerState {
                retired: false,
                active_batch_operations: 0,
                active_source_spool_preparations: 0,
                reservations: BTreeMap::from([(reservation.id, reservation)]),
            }),
        });

        assert_eq!(ledger.snapshot(), charge(8192, 4096, 0, 0, 8));
        let preparation = owner.reserve_preparation_buffer(128).unwrap().unwrap();
        owner.release_logical_after_archive(7).unwrap();
        assert_eq!(ledger.snapshot(), charge(0, 4096, 0, 128, 8));

        let batch = owner.reserve_batch(49).unwrap();
        assert_eq!(
            owner.release_logical_after_archive(7),
            Err(WalV7ReservationError::ActiveOperationsOutstanding)
        );
        drop(batch);

        let successor = ledger.reserve(charge(8192, 8192, 0, 0, 8)).unwrap();
        assert_eq!(ledger.snapshot(), charge(8192, 12288, 0, 128, 16));
        drop(successor);
        owner.retire_after_durable_cleanup().unwrap();
        assert_eq!(ledger.snapshot(), charge(0, 0, 0, 128, 0));
        // Independent queued scratch must not consume a reclaimed image's
        // index budget and reject a batch in its writable successor.
        let successor = ledger.reserve(charge(8192, 8192, 0, 0, 32)).unwrap();
        assert_eq!(ledger.snapshot(), charge(8192, 8192, 0, 128, 32));
        drop(successor);
        drop(preparation);
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn batch_reservation_charges_fragment_rounding_and_one_frontier() {
        let ledger = WalV7ResourceLedger::new(batch_limits()).unwrap();
        for (batch_bytes, data_bytes, wal_bytes) in [
            (49, 4096, 8192),
            (4008, 4096, 8192),
            (4009, 8192, 12288),
            (8016, 8192, 12288),
            (8017, 12288, 16384),
        ] {
            let reservation = ledger.reserve_batch(batch_bytes).unwrap();
            assert_eq!(
                ledger.snapshot(),
                charge(
                    wal_bytes,
                    wal_bytes,
                    0,
                    batch_bytes + data_bytes + WAL_V7_FRAME_LEN as u64,
                    8,
                )
            );
            drop(reservation);
            assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
        }
    }

    #[test]
    fn buffer_memory_retirement_reuses_capacity_and_preserves_persistent_charges() {
        let ledger = WalV7ResourceLedger::new(WalV7ResourceLimits {
            max_buffer_memory_bytes: 49 + 4096 + WAL_V7_FRAME_LEN as u64,
            ..batch_limits()
        })
        .unwrap();
        let mut first = ledger.reserve_batch(49).unwrap();
        let batch_buffer_bytes = 49 + 4096 + WAL_V7_FRAME_LEN as u64;
        assert_eq!(
            ledger.snapshot(),
            charge(8192, 8192, 0, batch_buffer_bytes, 8)
        );

        ledger.release_buffer_memory(&mut first).unwrap();
        assert_eq!(ledger.snapshot(), charge(8192, 8192, 0, 0, 8));
        ledger.release_buffer_memory(&mut first).unwrap();
        assert_eq!(ledger.snapshot(), charge(8192, 8192, 0, 0, 8));

        let mut second = ledger.reserve_batch(49).unwrap();
        assert_eq!(second.id, 2);
        assert_eq!(
            ledger.snapshot(),
            charge(16384, 16384, 0, batch_buffer_bytes, 16)
        );
        ledger.release_buffer_memory(&mut second).unwrap();
        assert_eq!(ledger.snapshot(), charge(16384, 16384, 0, 0, 16));
        drop(first);
        assert_eq!(ledger.snapshot(), charge(8192, 8192, 0, 0, 8));
        drop(second);
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn buffer_only_reservation_retires_without_a_second_drop_charge() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let mut reservation = ledger.reserve(charge(0, 0, 0, 8, 0)).unwrap();
        ledger.release_buffer_memory(&mut reservation).unwrap();
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
        assert_eq!(
            ledger.release_buffer_memory(&mut reservation),
            Err(WalV7ReservationError::UnknownReservation)
        );
        drop(reservation);
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn invalid_batch_lengths_consume_no_resources_or_reservation_ids() {
        let ledger = WalV7ResourceLedger::new(batch_limits()).unwrap();
        for batch_bytes in [0, 48, 4_294_967_344, u64::MAX] {
            assert_eq!(
                ledger.reserve_batch(batch_bytes),
                Err(WalV7ReservationError::InvalidBatchLength)
            );
            assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
        }
        let reservation = ledger.reserve_batch(49).unwrap();
        assert_eq!(reservation.id, 1);
    }

    #[test]
    fn rejected_batch_reservation_preserves_usage_and_reserved_workspace() {
        let exact_limits = WalV7ResourceLimits {
            max_buffer_memory_bytes: 2 * (49 + 4096 + WAL_V7_FRAME_LEN as u64),
            ..batch_limits()
        };
        for (limits, expected_error) in [
            (
                WalV7ResourceLimits {
                    max_logical_wal_bytes: 16383,
                    ..exact_limits
                },
                WalV7ReservationError::LogicalWalLimit,
            ),
            (
                WalV7ResourceLimits {
                    max_source_spool_bytes: 20479,
                    ..exact_limits
                },
                WalV7ReservationError::SourceSpoolLimit { generation: 0 },
            ),
            (
                WalV7ResourceLimits {
                    max_buffer_memory_bytes: 2 * (49 + 4096 + WAL_V7_FRAME_LEN as u64) - 1,
                    ..exact_limits
                },
                WalV7ReservationError::BufferMemoryLimit,
            ),
            (
                WalV7ResourceLimits {
                    max_seal_index_bytes: 15,
                    ..exact_limits
                },
                WalV7ReservationError::SealIndexLimit,
            ),
        ] {
            let ledger = WalV7ResourceLedger::new(limits).unwrap();
            let existing = ledger.reserve_batch(49).unwrap();
            let workspace = ledger.reserve(charge(0, 0, 4096, 0, 0)).unwrap();
            let batch_buffer_bytes = 49 + 4096 + WAL_V7_FRAME_LEN as u64;
            assert_eq!(
                ledger.snapshot(),
                charge(8192, 8192, 4096, batch_buffer_bytes, 8)
            );

            assert_eq!(ledger.reserve_batch(49), Err(expected_error));
            assert_eq!(
                ledger.snapshot(),
                charge(8192, 8192, 4096, batch_buffer_bytes, 8)
            );
            assert_eq!(ledger.inner.state.lock().next_id, 3);

            drop(existing);
            assert_eq!(ledger.snapshot(), charge(0, 0, 4096, 0, 0));
            drop(workspace);
            assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
        }
    }

    #[test]
    fn reservation_charges_all_resources_and_releases_as_one_owner() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let reservation = ledger.reserve(charge(20, 24, 16, 8, 4)).unwrap();
        assert_eq!(ledger.snapshot(), charge(20, 24, 16, 8, 4));

        let mut reservation = reservation;
        ledger.release(&mut reservation).unwrap();
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn rejected_reservation_does_not_partially_charge_other_resources() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let existing = ledger.reserve(charge(80, 24, 16, 8, 4)).unwrap();
        let before = ledger.snapshot();

        assert_eq!(
            ledger.reserve(charge(21, 1, 0, 1, 1)),
            Err(WalV7ReservationError::LogicalWalLimit)
        );
        assert_eq!(ledger.snapshot(), before);

        let mut existing = existing;
        ledger.release(&mut existing).unwrap();
    }

    #[test]
    fn recovery_workspace_uses_spool_capacity_and_its_own_limit() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        assert_eq!(
            ledger.reserve(charge(0, 40, 61, 0, 0)),
            Err(WalV7ReservationError::SourceSpoolLimit { generation: 0 })
        );
        assert_eq!(
            ledger.reserve(charge(0, 0, 81, 0, 0)),
            Err(WalV7ReservationError::RecoveryWorkspaceLimit)
        );
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn independent_buffer_and_index_limits_are_enforced_atomically() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        assert_eq!(
            ledger.reserve(charge(0, 0, 0, 41, 1)),
            Err(WalV7ReservationError::BufferMemoryLimit)
        );
        assert_eq!(
            ledger.reserve(charge(0, 0, 0, 1, 33)),
            Err(WalV7ReservationError::SealIndexLimit)
        );
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn invalid_limits_are_rejected_before_ledger_creation() {
        let invalid_limits = WalV7ResourceLimits {
            max_recovery_workspace_bytes: 101,
            ..limits()
        };
        assert_eq!(
            WalV7ResourceLedger::new(invalid_limits).err(),
            Some(WalV7ReservationError::InvalidLimits)
        );
    }

    #[test]
    fn release_rejects_forged_and_reused_tokens() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let mut reservation = ledger.reserve(charge(10, 12, 0, 4, 4)).unwrap();
        let mut forged = WalV7Reservation {
            id: reservation.id,
            charge: charge(9, 12, 0, 4, 4),
            ledger: Arc::clone(&reservation.ledger),
            active: true,
        };
        assert_eq!(
            ledger.release_buffer_memory(&mut forged),
            Err(WalV7ReservationError::ReservationMismatch)
        );
        assert_eq!(ledger.snapshot(), charge(10, 12, 0, 4, 4));
        assert_eq!(
            ledger.release(&mut forged),
            Err(WalV7ReservationError::ReservationMismatch)
        );

        ledger.release(&mut reservation).unwrap();
        let mut reused = WalV7Reservation {
            id: 1,
            charge: charge(10, 12, 0, 4, 4),
            ledger: Arc::clone(&ledger.inner),
            active: true,
        };
        assert_eq!(
            ledger.release_buffer_memory(&mut reused),
            Err(WalV7ReservationError::UnknownReservation)
        );
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
        assert_eq!(
            ledger.release(&mut reused),
            Err(WalV7ReservationError::UnknownReservation)
        );
    }

    #[test]
    fn dropping_a_reservation_releases_its_charge() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        drop(ledger.reserve(charge(10, 12, 4, 8, 4)).unwrap());

        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn reservation_keeps_ledger_state_alive_until_it_is_dropped() {
        let ledger = WalV7ResourceLedger::new(limits()).unwrap();
        let ledger_state = Arc::downgrade(&ledger.inner);
        let reservation = ledger.reserve(charge(10, 12, 4, 8, 4)).unwrap();
        drop(ledger);

        let state = ledger_state.upgrade().unwrap();
        assert_eq!(state.state.lock().charged, charge(10, 12, 4, 8, 4));
        drop(state);
        drop(reservation);
        assert!(ledger_state.upgrade().is_none());
    }

    #[test]
    fn reservation_cannot_be_released_through_another_ledger() {
        let first = WalV7ResourceLedger::new(limits()).unwrap();
        let second = WalV7ResourceLedger::new(limits()).unwrap();
        let mut first_reservation = first.reserve(charge(10, 12, 4, 8, 4)).unwrap();
        let mut second_reservation = second.reserve(charge(10, 12, 4, 8, 4)).unwrap();

        assert_eq!(
            second.release_buffer_memory(&mut first_reservation),
            Err(WalV7ReservationError::ForeignReservation)
        );
        assert_eq!(
            second.release(&mut first_reservation),
            Err(WalV7ReservationError::ForeignReservation)
        );
        assert_eq!(first.snapshot(), charge(10, 12, 4, 8, 4));
        assert_eq!(second.snapshot(), charge(10, 12, 4, 8, 4));

        first.release(&mut first_reservation).unwrap();
        second.release(&mut second_reservation).unwrap();
    }

    #[test]
    fn concurrent_reservations_cannot_exceed_any_shared_limit() {
        const WRITERS: usize = 16;
        let concurrent_limits = WalV7ResourceLimits {
            max_logical_wal_bytes: 100,
            max_source_spool_bytes: 100,
            max_recovery_workspace_bytes: 0,
            max_buffer_memory_bytes: 100,
            max_seal_index_bytes: 100,
        };
        let ledger = Arc::new(WalV7ResourceLedger::new(concurrent_limits).unwrap());
        let start = Arc::new(Barrier::new(WRITERS));
        let workers = (0..WRITERS)
            .map(|_| {
                let ledger = Arc::clone(&ledger);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    ledger.reserve(charge(10, 10, 0, 10, 10))
                })
            })
            .collect::<Vec<_>>();

        let mut admitted = Vec::new();
        for worker in workers {
            match worker.join().expect("reservation worker panicked") {
                Ok(reservation) => admitted.push(reservation),
                Err(error) => assert!(
                    matches!(
                        error,
                        WalV7ReservationError::LogicalWalLimit
                            | WalV7ReservationError::SourceSpoolLimit { .. }
                            | WalV7ReservationError::BufferMemoryLimit
                            | WalV7ReservationError::SealIndexLimit
                    ),
                    "unexpected reservation rejection: {error}"
                ),
            }
        }
        assert_eq!(admitted.len(), 10);
        assert_eq!(ledger.snapshot(), charge(100, 100, 0, 100, 100));
        for mut reservation in admitted {
            ledger.release(&mut reservation).unwrap();
        }
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
    }
}
