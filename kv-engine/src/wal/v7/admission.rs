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

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use parking_lot::Mutex;

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
    SourceSpoolLimit,
    RecoveryWorkspaceLimit,
    BufferMemoryLimit,
    SealIndexLimit,
    ReservationIdExhausted,
    ForeignReservation,
    UnknownReservation,
    ReservationMismatch,
    ReservationOwnerRetired,
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
            Self::SourceSpoolLimit => "v7 source spool limit exceeded",
            Self::RecoveryWorkspaceLimit => "v7 recovery workspace limit exceeded",
            Self::BufferMemoryLimit => "v7 buffer memory limit exceeded",
            Self::SealIndexLimit => "v7 seal index limit exceeded",
            Self::ReservationIdExhausted => "v7 reservation ID exhausted",
            Self::ForeignReservation => "v7 reservation belongs to another resource ledger",
            Self::UnknownReservation => "unknown or already released v7 reservation",
            Self::ReservationMismatch => "v7 reservation identity mismatch",
            Self::ReservationOwnerRetired => "v7 reservation owner has been retired",
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
}

#[derive(Debug)]
struct WalV7ResourceLedgerInner {
    limits: WalV7ResourceLimits,
    state: Mutex<LedgerState>,
}

/// Thread-safe ledger for owned, multi-resource reservations.
#[derive(Clone, Debug)]
pub(crate) struct WalV7ResourceLedger {
    inner: Arc<WalV7ResourceLedgerInner>,
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
    installed_dev: u64,
    installed_ino: u64,
    state: Mutex<WalV7ReservationOwnerState>,
}

#[derive(Debug)]
struct WalV7ReservationOwnerState {
    retired: bool,
    active_operations: u64,
    reservations: BTreeMap<u64, WalV7Reservation>,
}

/// A batch reservation already held by its image owner. Declare this guard
/// before allocating the batch and frame buffers so they drop before its
/// buffer charge retires, including when an assigned append unwinds.
pub(crate) struct WalV7OwnedBatchReservation {
    owner: Arc<WalV7ReservationOwner>,
    id: u64,
    admitted: bool,
    finished: bool,
}

/// Temporary preparation memory held while a caller builds a logical batch.
/// Retiring the source image waits for this guard to drop.
#[must_use = "the preparation reservation must live through batch preparation"]
pub(crate) struct WalV7OwnedPreparationReservation {
    owner: Arc<WalV7ReservationOwner>,
    reservation: Option<WalV7Reservation>,
}

impl WalV7ReservationOwner {
    /// Charge an installed Active image exactly once when creating its owner.
    /// Reopening handles on this inode reuses the owner; fresh-inode recovery
    /// must create a replacement owner rather than add another prefix charge.
    pub(crate) fn new(
        ledger: WalV7ResourceLedger,
        installed: &super::install::WalV7InstalledRecovery,
    ) -> anyhow::Result<Self> {
        let seed = installed.runtime_seed()?;
        let mut reservations = BTreeMap::new();
        if let Some(reservation) = installed.reserve_recovered_resources(&ledger)? {
            reservations.insert(reservation.id, reservation);
        }
        Ok(Self {
            ledger,
            installed_dev: seed.installed_dev,
            installed_ino: seed.installed_ino,
            state: Mutex::new(WalV7ReservationOwnerState {
                retired: false,
                active_operations: 0,
                reservations,
            }),
        })
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
        let active_operations = state
            .active_operations
            .checked_add(1)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        let reservation = self.ledger.reserve_batch(encoded_batch_bytes)?;
        let id = reservation.id;
        state.reservations.insert(id, reservation);
        state.active_operations = active_operations;
        drop(state);
        Ok(WalV7OwnedBatchReservation {
            owner: Arc::clone(self),
            id,
            admitted: false,
            finished: false,
        })
    }

    /// Reserve temporary canonicalization memory before the point-operation
    /// index is allocated. The caller drops this token as soon as preparation
    /// has consumed the index.
    pub(crate) fn reserve_preparation_buffer(
        self: &Arc<Self>,
        buffer_memory_bytes: u64,
    ) -> Result<Option<WalV7OwnedPreparationReservation>, WalV7ReservationError> {
        if buffer_memory_bytes == 0 {
            return Ok(None);
        }
        let mut state = self.state.lock();
        if state.retired {
            return Err(WalV7ReservationError::ReservationOwnerRetired);
        }
        let active_operations = state
            .active_operations
            .checked_add(1)
            .ok_or(WalV7ReservationError::ArithmeticOverflow)?;
        let reservation = self.ledger.reserve(WalV7ResourceCharge {
            buffer_memory_bytes,
            ..WalV7ResourceCharge::default()
        })?;
        state.active_operations = active_operations;
        drop(state);
        Ok(Some(WalV7OwnedPreparationReservation {
            owner: Arc::clone(self),
            reservation: Some(reservation),
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
            .active_operations
            .checked_sub(1)
            .map(|active_operations| state.active_operations = active_operations)
            .ok_or(WalV7ReservationError::AccountingUnderflow);
        release_retired_reservations(&mut state);
        result.and(operation_result)
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> WalV7ResourceCharge {
        self.ledger.snapshot()
    }
}

impl WalV7OwnedBatchReservation {
    /// Mark the reservation persistent immediately after coordinates advance.
    pub(crate) fn mark_admitted(&mut self) {
        self.admitted = true;
    }

    /// Retire buffers after synchronous I/O and hashing stop using them.
    /// Persistent charges remain in the image owner on success or failure.
    pub(crate) fn finish_after_io(mut self) -> Result<(), WalV7ReservationError> {
        if !self.admitted {
            return Err(WalV7ReservationError::ReservationMismatch);
        }
        let result = self.owner.finish_batch_operation(self.id, true);
        self.finished = true;
        result
    }
}

impl Drop for WalV7OwnedBatchReservation {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // The buffers declared after this guard have already dropped. Keep
        // persistent charges for admitted work even if accounting rejects
        // transient-buffer retirement.
        let _ = self.owner.finish_batch_operation(self.id, self.admitted);
    }
}

impl Drop for WalV7OwnedPreparationReservation {
    fn drop(&mut self) {
        // Release the temporary ledger token before allowing image retirement.
        drop(self.reservation.take());
        let mut state = self.owner.state.lock();
        if let Some(active_operations) = state.active_operations.checked_sub(1) {
            state.active_operations = active_operations;
        } else {
            debug_assert!(false, "v7 preparation-operation accounting underflow");
        }
        release_retired_reservations(&mut state);
    }
}

fn release_retired_reservations(state: &mut WalV7ReservationOwnerState) {
    if state.retired && state.active_operations == 0 {
        let reservations = std::mem::take(&mut state.reservations);
        // Release under the owner lock so another idempotent retirement cannot
        // return before the ledger charges have actually been dropped.
        drop(reservations);
    }
}

impl WalV7ResourceLedger {
    pub(crate) fn new(limits: WalV7ResourceLimits) -> Result<Self, WalV7ReservationError> {
        if limits.max_logical_wal_bytes == 0
            || limits.max_source_spool_bytes == 0
            || limits.max_recovery_workspace_bytes > limits.max_source_spool_bytes
        {
            return Err(WalV7ReservationError::InvalidLimits);
        }

        Ok(Self {
            inner: Arc::new(WalV7ResourceLedgerInner {
                limits,
                state: Mutex::new(LedgerState {
                    next_id: 1,
                    charged: WalV7ResourceCharge::default(),
                    reservations: BTreeMap::new(),
                }),
            }),
        })
    }

    /// Atomically charge every resource before a caller assigns WAL coordinates.
    pub(crate) fn reserve(
        &self,
        charge: WalV7ResourceCharge,
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
            return Err(WalV7ReservationError::SourceSpoolLimit);
        }
        if next.recovery_workspace_bytes > self.inner.limits.max_recovery_workspace_bytes {
            return Err(WalV7ReservationError::RecoveryWorkspaceLimit);
        }
        if next.buffer_memory_bytes > self.inner.limits.max_buffer_memory_bytes {
            return Err(WalV7ReservationError::BufferMemoryLimit);
        }
        if next.seal_index_bytes > self.inner.limits.max_seal_index_bytes {
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
    Ok(())
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
                WalV7ReservationError::SourceSpoolLimit,
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
            Err(WalV7ReservationError::SourceSpoolLimit)
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
                            | WalV7ReservationError::SourceSpoolLimit
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
