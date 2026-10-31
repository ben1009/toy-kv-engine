//! Image-bound v8 admission and fresh-inode recovery reservations.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};

use crate::wal::v7::admission::{
    WalV7OwnedBatchReservation, WalV7OwnedPreparationReservation, WalV7Reservation,
    WalV7ReservationOwner, WalV7ResourceCharge, WalV7RetainedResources,
};

use super::{WAL_V8_HEADER_LEN, codec::WAL_V8_FRAME_LEN, install::WalV8InstalledRecovery};

const FIXED_IMAGE_BYTES: u64 = (WAL_V8_HEADER_LEN + WAL_V8_FRAME_LEN) as u64;

/// Holds the installed image and its recovery workspace on the shared PITR
/// resource ledger. The owner must survive until the image is retired.
#[derive(Debug)]
pub(crate) struct WalV8ReservationOwner {
    image_owner: Arc<WalV7ReservationOwner>,
    _recovery_workspace: WalV7Reservation,
    recovery_workspace_bytes: u64,
    source_allocation_unit: u64,
}

impl WalV8ReservationOwner {
    /// The caller must first drop the temporary recovery reservation and retain
    /// the reconstructed ledger, which already owns the fixed 8 KiB image
    /// charge. `installed_allocated_bytes` must describe the installed inode.
    pub(crate) fn new_with_retained_resources(
        retained_resources: Arc<WalV7RetainedResources>,
        installed: &WalV8InstalledRecovery,
        installed_allocated_bytes: u64,
        recovery_workspace_bytes: u64,
        source_allocation_unit: u64,
    ) -> Result<Self> {
        ensure!(
            source_allocation_unit > 0,
            "v8 source allocation unit is zero"
        );
        ensure!(
            installed.image_len >= FIXED_IMAGE_BYTES,
            "installed v8 image is shorter than its fixed prefix"
        );
        ensure!(
            recovery_workspace_bytes >= round_up(installed.image_len, source_allocation_unit)?,
            "v8 recovery workspace cannot hold the installed image"
        );
        let workspace = retained_resources
            .ledger()
            .reserve_maintenance(WalV7ResourceCharge {
                recovery_workspace_bytes,
                ..WalV7ResourceCharge::default()
            })?;
        let recovered_charge = WalV7ResourceCharge {
            logical_wal_bytes: installed.image_len - FIXED_IMAGE_BYTES,
            source_spool_bytes: installed_allocated_bytes.saturating_sub(FIXED_IMAGE_BYTES),
            ..WalV7ResourceCharge::default()
        };
        let image_owner = Arc::new(WalV7ReservationOwner::new_with_retained_image_charge(
            retained_resources,
            installed.installed_dev,
            installed.installed_ino,
            recovered_charge,
        )?);

        Ok(Self {
            image_owner,
            _recovery_workspace: workspace,
            recovery_workspace_bytes,
            source_allocation_unit,
        })
    }

    /// Give the segment lifecycle the same image owner and retained ledger used
    /// by the writer, so source cleanup releases the matching charges.
    pub(crate) fn image_owner(&self) -> Arc<WalV7ReservationOwner> {
        Arc::clone(&self.image_owner)
    }

    pub(crate) fn validate_image_and_workspace(
        &self,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
    ) -> Result<()> {
        self.image_owner
            .validate_image(installed.installed_dev, installed.installed_ino)?;
        ensure!(
            self.recovery_workspace_bytes >= round_up(max_file_size, self.source_allocation_unit)?,
            "v8 recovery workspace cannot hold the configured maximum image"
        );
        Ok(())
    }

    pub(crate) fn reserve_preparation_buffer(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<Option<WalV7OwnedPreparationReservation>> {
        self.image_owner
            .reserve_preparation_buffer(bytes)
            .map_err(Into::into)
    }

    /// Reserve physical source space, logical WAL quota, and buffers before
    /// assigning a ticket. One worst-case FRONTIER is charged per batch.
    pub(crate) fn reserve_batch(
        self: &Arc<Self>,
        logical_batch_bytes: u64,
        frame_buffer_bytes: u64,
    ) -> Result<WalV7OwnedBatchReservation> {
        let mut charge = WalV7ResourceCharge::for_batch(logical_batch_bytes)?;
        let expected_wal_bytes = frame_buffer_bytes
            .checked_add(WAL_V8_FRAME_LEN as u64)
            .context("v8 DATA and FRONTIER reservation overflows")?;
        ensure!(
            charge.logical_wal_bytes == expected_wal_bytes,
            "v8 DATA frame reservation differs from the encoded batch length"
        );
        let marker_source_bytes = round_up(WAL_V8_FRAME_LEN as u64, self.source_allocation_unit)?;
        charge.source_spool_bytes = round_up(frame_buffer_bytes, self.source_allocation_unit)?
            .checked_add(marker_source_bytes)
            .context("v8 source spool reservation overflows")?;
        self.image_owner
            .reserve_batch_charge(charge, marker_source_bytes)
            .map_err(Into::into)
    }
}

fn round_up(bytes: u64, unit: u64) -> Result<u64> {
    ensure!(unit > 0, "v8 source allocation unit is zero");
    let blocks = bytes
        .checked_add(unit - 1)
        .context("v8 source allocation rounding overflows")?
        / unit;
    blocks
        .checked_mul(unit)
        .context("v8 source allocation reservation overflows")
}
