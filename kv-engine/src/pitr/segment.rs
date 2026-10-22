//! PITR segment lifecycle and source-pin management.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc};

use crate::wal::v7::admission::{WalV7ReservationOwner, WalV7RetainedResources};

#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::ffi::OsStrExt,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(target_os = "linux")]
static SUCCESSOR_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentState {
    Active,
    Sealing,
    Sealed,
    Archived,
    Reclaimable,
    Reclaiming,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentMetadata {
    pub(crate) segment_id: u64,
    pub(crate) state: SegmentState,
    pub(crate) logical_length: u64,
    pub(crate) source_spool_bytes: u64,
    pub(crate) source_pins: u32,
    pub(crate) archive_pin: bool,
    pub(crate) successor_segment_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CleanupReceipt {
    pub(crate) wal_unlinked: bool,
    pub(crate) seal_unlinked: bool,
    pub(crate) directory_synced: bool,
}

impl CleanupReceipt {
    pub(crate) const fn durable() -> Self {
        Self {
            wal_unlinked: true,
            seal_unlinked: true,
            directory_synced: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RotationReason {
    Size,
    Timer,
    Backup,
    Barrier,
    Shutdown,
}

#[derive(Clone, Debug)]
pub(crate) struct PitrSegmentManager {
    segments: BTreeMap<u64, SegmentMetadata>,
    v7_reservation_owners: BTreeMap<u64, Arc<WalV7ReservationOwner>>,
    v7_retained_resources: Option<Arc<WalV7RetainedResources>>,
    active_segment_id: u64,
    next_segment_id: u64,
    pending_successor: Option<SegmentMetadata>,
    source_spool_limit: u64,
    source_spool_reserved: u64,
    pending_rotation: Option<RotationReason>,
    pending_obligation_reconciliation: Option<BTreeMap<u64, crate::pitr::manifest::PitrObligation>>,
}

/// Prepared releases from both the current pass and deferred manifest work.
/// Apply them only after the corresponding manifest projection is durable.
/// The previous projection is retained if application is only partially successful.
#[derive(Debug, Default)]
pub(crate) struct PitrObligationReconciliation {
    logical_releases: Vec<u64>,
    reclaimed_sources: Vec<u64>,
    retry_obligations: BTreeMap<u64, crate::pitr::manifest::PitrObligation>,
}

impl PitrSegmentManager {
    pub(crate) fn new(active_segment_id: u64, source_spool_limit: u64) -> Result<Self> {
        ensure!(
            source_spool_limit >= 4096,
            "PITR source spool limit is too small"
        );
        let active = SegmentMetadata {
            segment_id: active_segment_id,
            state: SegmentState::Active,
            logical_length: 4096,
            source_spool_bytes: 4096,
            source_pins: 0,
            archive_pin: false,
            successor_segment_id: None,
        };
        let mut segments = BTreeMap::new();
        segments.insert(active_segment_id, active);

        Ok(Self {
            segments,
            v7_reservation_owners: BTreeMap::new(),
            v7_retained_resources: None,
            active_segment_id,
            next_segment_id: active_segment_id
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("segment ID exhausted"))?,
            pending_successor: None,
            source_spool_limit,
            source_spool_reserved: 4096,
            pending_rotation: None,
            pending_obligation_reconciliation: None,
        })
    }

    /// Rebuild the segment and source-spool reservation estimate from durable
    /// lifecycle state. The active WAL length comes from the recovered current
    /// or immutable memtable, falling back to validated active-WAL file recovery
    /// when no matching memtable exists. Other retained WALs are represented by
    /// manifest obligations. A `Sealing` obligation may still name the active
    /// segment while its successor is pending.
    pub(crate) fn from_recovered_state(
        active_segment_id: u64,
        next_segment_id: u64,
        active_logical_length: u64,
        source_spool_limit: u64,
        obligations: &BTreeMap<u64, crate::pitr::manifest::PitrObligation>,
    ) -> Result<Self> {
        ensure!(
            active_logical_length >= 4096 && active_logical_length.is_multiple_of(4096),
            "invalid recovered active WAL length"
        );
        let first_successor = active_segment_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("segment ID exhausted"))?;
        let mut manager = Self::new(active_segment_id, source_spool_limit)?;
        manager.next_segment_id = next_segment_id.max(first_successor);
        {
            let active = manager
                .segments
                .get_mut(&active_segment_id)
                .expect("new segment manager contains its active segment");
            active.logical_length = active_logical_length;
            active.source_spool_bytes = active_logical_length;
        }

        for (&segment_id, obligation) in obligations {
            ensure!(
                obligation.logical_length >= 4096 && obligation.logical_length.is_multiple_of(4096),
                "invalid recovered PITR obligation length"
            );
            ensure!(
                obligation.successor_segment_id > segment_id,
                "recovered PITR successor ID is not monotonic"
            );
            manager.next_segment_id = manager
                .next_segment_id
                .max(obligation.successor_segment_id.saturating_add(1));

            if segment_id == active_segment_id {
                ensure!(
                    obligation.state == crate::pitr::manifest::ObligationState::Sealing,
                    "only a Sealing obligation may name the active segment"
                );
                ensure!(
                    manager.pending_successor.is_none(),
                    "multiple recovered PITR successors are pending"
                );
                let active = manager
                    .segments
                    .get_mut(&active_segment_id)
                    .expect("active segment invariant");
                active.state = SegmentState::Sealing;
                active.logical_length = obligation.logical_length;
                active.source_spool_bytes = active_logical_length.max(obligation.logical_length);
                active.successor_segment_id = Some(obligation.successor_segment_id);
                manager.pending_successor = Some(SegmentMetadata {
                    segment_id: obligation.successor_segment_id,
                    state: SegmentState::Sealing,
                    logical_length: 4096,
                    source_spool_bytes: 4096,
                    source_pins: 0,
                    archive_pin: false,
                    successor_segment_id: None,
                });
                continue;
            }

            ensure!(
                segment_id < active_segment_id,
                "recovered PITR obligation is newer than the active segment"
            );
            let (state, archive_pin) = match obligation.state {
                crate::pitr::manifest::ObligationState::Sealing => {
                    anyhow::bail!("non-active PITR obligation is still Sealing")
                }
                crate::pitr::manifest::ObligationState::Sealed => (SegmentState::Sealed, true),
                crate::pitr::manifest::ObligationState::Archived => (SegmentState::Archived, true),
                crate::pitr::manifest::ObligationState::Reclaimable => {
                    (SegmentState::Reclaimable, false)
                }
                crate::pitr::manifest::ObligationState::Abandoned => {
                    (SegmentState::Abandoned, false)
                }
            };
            ensure!(
                manager
                    .segments
                    .insert(
                        segment_id,
                        SegmentMetadata {
                            segment_id,
                            state,
                            logical_length: obligation.logical_length,
                            source_spool_bytes: obligation.logical_length,
                            source_pins: 0,
                            archive_pin,
                            successor_segment_id: Some(obligation.successor_segment_id),
                        },
                    )
                    .is_none(),
                "duplicate recovered PITR segment"
            );
        }

        manager.recompute_reserved()?;
        Ok(manager)
    }

    pub(crate) fn request_rotation(&mut self, reason: RotationReason) -> bool {
        if let Some(existing) = self.pending_rotation
            && reason.priority() <= existing.priority()
        {
            return false;
        }
        self.pending_rotation = Some(reason);

        true
    }

    pub(crate) fn take_rotation_request(&mut self) -> Option<RotationReason> {
        self.pending_rotation.take()
    }

    pub(crate) fn begin_sealing(
        &mut self,
        logical_length: u64,
        successor_spool_bytes: u64,
    ) -> Result<u64> {
        ensure!(
            self.pending_successor.is_none(),
            "PITR rotation already has a pending successor"
        );
        ensure!(
            logical_length >= 4096 && logical_length.is_multiple_of(4096),
            "invalid sealed logical length"
        );
        let active = self
            .segments
            .get_mut(&self.active_segment_id)
            .expect("active segment invariant");
        ensure!(
            active.state == SegmentState::Active,
            "PITR active segment is not active"
        );
        let successor_id = self.next_segment_id;
        ensure!(
            successor_spool_bytes >= 4096,
            "successor reservation is below minimum WAL header"
        );
        let physical_bytes = active.source_spool_bytes.max(logical_length);
        let active_growth = physical_bytes.saturating_sub(active.source_spool_bytes);
        let reserved = self
            .source_spool_reserved
            .checked_add(active_growth)
            .and_then(|bytes| bytes.checked_add(successor_spool_bytes))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;
        ensure!(
            reserved <= self.source_spool_limit,
            "PITR source spool limit exceeded"
        );
        active.state = SegmentState::Sealing;
        active.logical_length = logical_length;
        active.source_spool_bytes = physical_bytes;
        active.successor_segment_id = Some(successor_id);
        self.pending_successor = Some(SegmentMetadata {
            segment_id: successor_id,
            state: SegmentState::Sealing,
            logical_length: 4096,
            source_spool_bytes: successor_spool_bytes,
            source_pins: 0,
            archive_pin: false,
            successor_segment_id: None,
        });
        self.source_spool_reserved = reserved;

        Ok(successor_id)
    }

    /// Record a segment that a lifecycle barrier sealed and archived.
    ///
    /// The barrier owns its own durable transitions; this only keeps the
    /// in-memory bookkeeping - and therefore reported status - in step with
    /// them. It deliberately does not re-run the admission-time spool
    /// arithmetic, which belongs to the write path's reservations.
    /// Track an obligation inherited from a previous run.
    ///
    /// A reopen starts the manager at the active segment and knows nothing
    /// about the sealed segments the manifest still lists, so reconciliation
    /// has to be able to pick them up before it can finish their lifecycle.
    pub(crate) fn adopt_obligation(
        &mut self,
        segment_id: u64,
        logical_length: u64,
        successor_segment_id: u64,
    ) -> Result<()> {
        ensure!(
            logical_length >= 4096 && logical_length.is_multiple_of(4096),
            "invalid sealed logical length"
        );
        ensure!(
            successor_segment_id > segment_id,
            "sealed successor segment ID is not monotonic"
        );
        self.segments.entry(segment_id).or_insert(SegmentMetadata {
            segment_id,
            state: SegmentState::Sealed,
            logical_length,
            source_spool_bytes: logical_length,
            source_pins: 0,
            archive_pin: true,
            successor_segment_id: Some(successor_segment_id),
        });
        self.next_segment_id = self
            .next_segment_id
            .max(successor_segment_id.saturating_add(1));
        self.recompute_reserved()?;

        Ok(())
    }

    pub(crate) fn record_sealed(
        &mut self,
        segment_id: u64,
        logical_length: u64,
        successor_segment_id: u64,
    ) -> Result<()> {
        ensure!(
            logical_length >= 4096 && logical_length.is_multiple_of(4096),
            "invalid sealed logical length"
        );
        ensure!(
            successor_segment_id > segment_id,
            "sealed successor segment ID is not monotonic"
        );
        match self.segments.get_mut(&segment_id) {
            Some(segment) => {
                segment.state = SegmentState::Sealed;
                segment.archive_pin = true;
                segment.logical_length = logical_length;
                segment.source_spool_bytes = segment.source_spool_bytes.max(logical_length);
                segment.successor_segment_id = Some(successor_segment_id);
            }
            None => {
                self.segments.insert(
                    segment_id,
                    SegmentMetadata {
                        segment_id,
                        state: SegmentState::Sealed,
                        logical_length,
                        source_spool_bytes: logical_length,
                        source_pins: 0,
                        archive_pin: true,
                        successor_segment_id: Some(successor_segment_id),
                    },
                );
            }
        }
        // The sealed segment stops being the active one the moment its
        // successor WAL exists, which is what lets it be reclaimed later.
        self.active_segment_id = successor_segment_id;
        let successor = if self
            .pending_successor
            .is_some_and(|successor| successor.segment_id == successor_segment_id)
        {
            let successor = self.pending_successor.take().expect("matching successor");
            SegmentMetadata {
                state: SegmentState::Active,
                ..successor
            }
        } else {
            SegmentMetadata {
                segment_id: successor_segment_id,
                state: SegmentState::Active,
                logical_length: 4096,
                source_spool_bytes: 4096,
                source_pins: 0,
                archive_pin: false,
                successor_segment_id: None,
            }
        };
        self.segments
            .entry(successor_segment_id)
            .or_insert(successor);
        self.next_segment_id = self
            .next_segment_id
            .max(successor_segment_id.saturating_add(1));
        self.recompute_reserved()?;

        Ok(())
    }

    /// Prepare bookkeeping for reconciled manifest obligations without losing
    /// live pins or reservations. The caller applies this to a clone and only
    /// publishes it and applies the returned releases after the corresponding
    /// manifest records are durable.
    pub(crate) fn reconcile_obligations(
        &mut self,
        previous: &BTreeMap<u64, crate::pitr::manifest::PitrObligation>,
        current: &BTreeMap<u64, crate::pitr::manifest::PitrObligation>,
    ) -> Result<PitrObligationReconciliation> {
        use crate::pitr::manifest::ObligationState;

        let mut reconciliation = PitrObligationReconciliation::default();
        let mut previous_obligations = self
            .pending_obligation_reconciliation
            .take()
            .unwrap_or_default();
        previous_obligations.extend(previous.iter().map(|(&id, &obligation)| (id, obligation)));
        for (&segment_id, previous_obligation) in &previous_obligations {
            if let Some(obligation) = current.get(&segment_id) {
                if segment_id == self.active_segment_id
                    && obligation.state != ObligationState::Sealing
                {
                    self.record_sealed(
                        segment_id,
                        obligation.logical_length,
                        obligation.successor_segment_id,
                    )?;
                }
                self.adopt_obligation(
                    segment_id,
                    obligation.logical_length,
                    obligation.successor_segment_id,
                )?;
                let segment = self
                    .segments
                    .get_mut(&segment_id)
                    .expect("adopted obligation exists");
                segment.state = match obligation.state {
                    ObligationState::Sealing => SegmentState::Sealing,
                    ObligationState::Sealed => SegmentState::Sealed,
                    ObligationState::Archived => SegmentState::Archived,
                    ObligationState::Reclaimable => SegmentState::Reclaimable,
                    ObligationState::Abandoned => SegmentState::Abandoned,
                };
                segment.archive_pin = matches!(
                    obligation.state,
                    ObligationState::Sealed | ObligationState::Archived
                );
                segment.logical_length = obligation.logical_length;
                segment.source_spool_bytes =
                    segment.source_spool_bytes.max(obligation.logical_length);
                segment.successor_segment_id = Some(obligation.successor_segment_id);
                if !matches!(
                    obligation.state,
                    ObligationState::Sealing | ObligationState::Sealed
                ) {
                    reconciliation.logical_releases.push(segment_id);
                }
            } else {
                ensure!(
                    previous_obligation.state == ObligationState::Reclaimable,
                    "reconciled PITR obligation was not reclaimable"
                );
                if segment_id == self.active_segment_id {
                    self.record_sealed(
                        segment_id,
                        previous_obligation.logical_length,
                        previous_obligation.successor_segment_id,
                    )?;
                }
                if let Some(segment) = self.segments.get(&segment_id) {
                    ensure!(
                        segment.source_pins == 0 && segment_id != self.active_segment_id,
                        "reconciled PITR segment still has source pins or is active"
                    );
                    self.segments.remove(&segment_id);
                }
                reconciliation.logical_releases.push(segment_id);
                reconciliation.reclaimed_sources.push(segment_id);
            }
        }

        self.recompute_reserved()?;
        reconciliation.retry_obligations = previous_obligations;

        Ok(reconciliation)
    }

    /// Retire charges after the reconciled manifest projection is durable.
    /// A removed Reclaimable obligation also certifies prior durable source
    /// cleanup, including when this pass only syncs a deferred append.
    pub(crate) fn apply_v7_obligation_reconciliation(
        &mut self,
        reconciliation: PitrObligationReconciliation,
    ) -> Result<()> {
        let PitrObligationReconciliation {
            logical_releases,
            reclaimed_sources,
            retry_obligations,
        } = reconciliation;
        let mut first_error = None;
        if let Some(resources) = &self.v7_retained_resources {
            for segment_id in logical_releases {
                if let Some(owner) = self.v7_reservation_owners.get(&segment_id)
                    && let Err(error) = owner.release_logical_after_archive(segment_id)
                {
                    first_error.get_or_insert_with(|| anyhow::Error::new(error));
                }
                resources.release_obligation_logical(segment_id);
            }
        } else {
            for segment_id in logical_releases {
                if let Some(owner) = self.v7_reservation_owners.get(&segment_id)
                    && let Err(error) = owner.release_logical_after_archive(segment_id)
                {
                    first_error.get_or_insert_with(|| anyhow::Error::new(error));
                }
            }
        }
        for segment_id in reclaimed_sources {
            if let Err(error) = self.retire_v7_reservation_owner_after_cleanup(segment_id) {
                first_error.get_or_insert(error);
            }
        }

        if let Some(error) = first_error {
            self.defer_obligation_reconciliation(retry_obligations);
            return Err(error);
        }

        Ok(())
    }

    /// Keep the previous obligations when their transition was appended but its
    /// sync failed. The manifest projection may already have advanced, so a
    /// retry needs these obligations even if no new lifecycle records remain.
    pub(crate) fn defer_obligation_reconciliation(
        &mut self,
        previous: BTreeMap<u64, crate::pitr::manifest::PitrObligation>,
    ) {
        self.pending_obligation_reconciliation
            .get_or_insert_with(BTreeMap::new)
            .extend(previous);
    }

    pub(crate) fn has_pending_obligation_reconciliation(&self) -> bool {
        self.pending_obligation_reconciliation.is_some()
    }

    /// Drop a segment whose source WAL and sidecar have been unlinked.
    pub(crate) fn record_reclaimed(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get(&segment_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaimable,
            "PITR segment was not reclaimable"
        );
        self.segments.remove(&segment_id);
        self.recompute_reserved()?;

        Ok(())
    }

    /// Keep the installed v7 image's reservations alive for as long as its
    /// source segment can still be needed by recovery or archive work.
    pub(crate) fn retain_v7_reservation_owner(
        &mut self,
        segment_id: u64,
        owner: Arc<WalV7ReservationOwner>,
    ) -> Result<()> {
        ensure!(
            self.segments.contains_key(&segment_id),
            "cannot retain a v7 reservation owner for an unknown PITR segment"
        );
        if let Some(existing) = self.v7_reservation_owners.get(&segment_id) {
            ensure!(
                Arc::ptr_eq(existing, &owner),
                "PITR segment already has a different v7 reservation owner"
            );
            return Ok(());
        }
        if let Some(resources) = owner.retained_resources() {
            if let Some(existing) = &self.v7_retained_resources {
                ensure!(
                    Arc::ptr_eq(existing, &resources),
                    "PITR v7 segments use different reconstructed resource ledgers"
                );
            } else {
                self.v7_retained_resources = Some(resources);
            }
        }
        self.v7_reservation_owners.insert(segment_id, owner);
        Ok(())
    }

    /// Release an image owner's persistent charges only after its source WAL
    /// and seal have been durably removed. The owner map intentionally
    /// outlives segment metadata removed by manifest reconciliation.
    pub(crate) fn retire_v7_reservation_owner_after_cleanup(
        &mut self,
        segment_id: u64,
    ) -> Result<()> {
        if let Some(owner) = self.v7_reservation_owners.get(&segment_id) {
            owner.retire_after_durable_cleanup()?;
            self.v7_reservation_owners.remove(&segment_id);
        }
        if let Some(resources) = &self.v7_retained_resources {
            resources.release_segment(segment_id);
        }
        Ok(())
    }

    /// Recompute the reservation estimate from the tracked segment charges.
    fn recompute_reserved(&mut self) -> Result<()> {
        let segments = self.segments.values().try_fold(0_u64, |total, segment| {
            let bytes = total
                .checked_add(segment.source_spool_bytes)
                .and_then(|bytes| {
                    if segment.state == SegmentState::Active {
                        Some(bytes)
                    } else {
                        bytes.checked_add(4096)
                    }
                })
                .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;
            Ok::<_, anyhow::Error>(bytes)
        })?;
        self.source_spool_reserved = segments
            .checked_add(
                self.pending_successor
                    .map_or(0, |successor| successor.source_spool_bytes),
            )
            .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;

        Ok(())
    }

    pub(crate) fn mark_sealed(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Sealing,
            "PITR segment sealed out of order"
        );
        segment.state = SegmentState::Sealed;
        segment.archive_pin = true;

        Ok(())
    }

    pub(crate) fn install_successor(&mut self) -> Result<u64> {
        let successor = self
            .pending_successor
            .ok_or_else(|| anyhow::anyhow!("PITR successor is not pending"))?;
        ensure!(
            self.segments
                .get(&self.active_segment_id)
                .is_some_and(|segment| segment.state == SegmentState::Sealed),
            "cannot install successor before sealing old segment"
        );
        let next_segment_id = successor
            .segment_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("segment ID exhausted"))?;
        self.pending_successor = None;
        self.segments.insert(
            successor.segment_id,
            SegmentMetadata {
                state: SegmentState::Active,
                ..successor
            },
        );
        self.active_segment_id = successor.segment_id;
        self.next_segment_id = next_segment_id;

        Ok(self.active_segment_id)
    }

    pub(crate) fn install_successor_after_wal(
        &mut self,
        install_wal: impl FnOnce(u64) -> Result<()>,
    ) -> Result<u64> {
        let successor_id = self
            .pending_successor
            .ok_or_else(|| anyhow::anyhow!("PITR successor is not pending"))?
            .segment_id;
        install_wal(successor_id)?;

        self.install_successor()
    }

    pub(crate) fn mark_archived(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Sealed,
            "PITR archive is out of order"
        );
        segment.state = SegmentState::Archived;

        Ok(())
    }

    pub(crate) fn mark_reclaimable(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Archived,
            "PITR reclaimable state is out of order"
        );
        ensure!(
            segment.source_pins == 0,
            "PITR segment has transient source pins"
        );
        ensure!(
            segment.archive_pin,
            "PITR segment is missing lifecycle archive pin"
        );
        ensure!(
            segment_id != self.active_segment_id,
            "cannot reclaim the active PITR segment"
        );
        ensure!(
            self.pending_successor.is_none(),
            "cannot reclaim while successor installation is pending"
        );
        segment.state = SegmentState::Reclaimable;

        Ok(())
    }

    pub(crate) fn release_archive_pin(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaimable,
            "archive pin released before durable reclaimable state"
        );
        ensure!(segment.archive_pin, "PITR archive pin is not held");
        segment.archive_pin = false;

        Ok(())
    }

    pub(crate) fn pin(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        segment.source_pins = segment
            .source_pins
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PITR source pin count overflow"))?;

        Ok(())
    }

    pub(crate) fn unpin(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(segment.source_pins > 0, "PITR source pin underflow");
        segment.source_pins -= 1;

        Ok(())
    }

    pub(crate) fn reclaim(&mut self, segment_id: u64) -> Result<SegmentMetadata> {
        let segment = self
            .segments
            .get(&segment_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaimable
                && segment.source_pins == 0
                && !segment.archive_pin,
            "PITR segment is not reclaimable"
        );
        let segment = self
            .segments
            .get_mut(&segment_id)
            .expect("segment was present");
        segment.state = SegmentState::Reclaiming;

        Ok(*segment)
    }

    pub(crate) fn complete_reclaim(
        &mut self,
        segment_id: u64,
        cleanup: CleanupReceipt,
    ) -> Result<SegmentMetadata> {
        let segment = self
            .segments
            .get(&segment_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaiming && segment.source_pins == 0,
            "PITR cleanup is not ready"
        );
        ensure!(
            segment_id != self.active_segment_id,
            "cannot complete reclaim of the active PITR segment"
        );
        ensure!(
            self.pending_successor.is_none(),
            "cannot complete reclaim while successor installation is pending"
        );
        ensure!(
            cleanup.wal_unlinked && cleanup.seal_unlinked && cleanup.directory_synced,
            "PITR cleanup is not durable"
        );
        let released = segment
            .source_spool_bytes
            .checked_add(4096)
            .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;
        let new_reserved = self
            .source_spool_reserved
            .checked_sub(released)
            .ok_or_else(|| anyhow::anyhow!("source spool accounting underflow"))?;
        self.retire_v7_reservation_owner_after_cleanup(segment_id)?;
        let removed = self
            .segments
            .remove(&segment_id)
            .expect("segment was present");
        self.source_spool_reserved = new_reserved;

        Ok(removed)
    }

    pub(crate) fn segment(&self, segment_id: u64) -> Option<SegmentMetadata> {
        self.segments.get(&segment_id).copied()
    }

    pub(crate) fn segment_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.segments.keys().copied()
    }

    pub(crate) fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    pub(crate) fn active_logical_length(&self) -> u64 {
        self.segments
            .get(&self.active_segment_id)
            .map_or(0, |segment| segment.logical_length)
    }

    pub(crate) fn source_spool_reserved(&self) -> u64 {
        self.source_spool_reserved
    }

    pub(crate) fn source_spool_charges_by_segment(&self) -> Result<BTreeMap<u64, u64>> {
        let mut charges = self.segments.iter().try_fold(
            BTreeMap::new(),
            |mut charges, (&segment_id, segment)| {
                let seal_reservation = if segment.state == SegmentState::Active {
                    0
                } else {
                    4096
                };
                let charge = segment
                    .source_spool_bytes
                    .checked_add(seal_reservation)
                    .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;
                charges.insert(segment_id, charge);
                Ok::<_, anyhow::Error>(charges)
            },
        )?;
        if let Some(successor) = self.pending_successor {
            ensure!(
                charges
                    .insert(successor.segment_id, successor.source_spool_bytes)
                    .is_none(),
                "pending PITR successor duplicates a segment charge"
            );
        }

        Ok(charges)
    }

    pub(crate) fn sealed_unarchived_bytes(&self) -> u64 {
        self.segments
            .values()
            .filter(|segment| matches!(segment.state, SegmentState::Sealing | SegmentState::Sealed))
            .map(|segment| segment.logical_length)
            .sum()
    }

    pub(crate) fn pending_successor_id(&self) -> Result<u64> {
        self.pending_successor
            .map(|segment| segment.segment_id)
            .ok_or_else(|| anyhow::anyhow!("PITR successor is not pending"))
    }
}

pub(crate) fn install_v5_wal_header(
    path: impl AsRef<std::path::Path>,
    header: crate::pitr::WalV5Header,
) -> Result<()> {
    let bytes = crate::pitr::encode_v5_file_header(header)?;

    install_pitr_file_no_replace(path, &bytes)
}

#[cfg(target_os = "linux")]
#[derive(Default)]
pub(crate) struct PitrFileInstallFaults {
    pub(crate) after_header_write_before_remainder: Option<&'static str>,
    pub(crate) after_file_write_before_sync: Option<&'static str>,
    pub(crate) after_file_sync_before_rename: Option<&'static str>,
    pub(crate) after_rename_before_directory_sync: Option<&'static str>,
    pub(crate) inject_empty_active_image_failures: bool,
}

#[cfg(target_os = "linux")]
pub(crate) fn install_pitr_file_no_replace(
    path: impl AsRef<std::path::Path>,
    bytes: &[u8],
) -> Result<()> {
    install_pitr_file_no_replace_with_faults(path, bytes, PitrFileInstallFaults::default())
}

#[cfg(target_os = "linux")]
pub(crate) fn install_pitr_file_no_replace_with_faults(
    path: impl AsRef<std::path::Path>,
    bytes: &[u8],
    faults: PitrFileInstallFaults,
) -> Result<()> {
    #[cfg(not(feature = "chaos-testing"))]
    let _ = (
        faults.after_header_write_before_remainder,
        faults.after_file_write_before_sync,
        faults.after_file_sync_before_rename,
        faults.after_rename_before_directory_sync,
        faults.inject_empty_active_image_failures,
    );
    let path = path.as_ref();
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("PITR WAL has no file name"))?;
    ensure!(
        file_name.to_str().is_some_and(|name| !name.is_empty()),
        "PITR WAL file name is not valid UTF-8"
    );
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("PITR successor WAL has no parent directory"))?;
    let temp_name = format!(
        ".{}.tmp-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        SUCCESSOR_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let temp_path = parent.join(&temp_name);
    let result = (|| -> Result<()> {
        let parent_file = std::fs::File::open(parent)?;
        match std::fs::File::open(path) {
            Ok(existing_file) => {
                return verify_existing_pitr_file(existing_file, &parent_file, bytes);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        #[cfg(feature = "chaos-testing")]
        if let Some(crash_point) = faults.after_header_write_before_remainder {
            ensure!(
                bytes.len() > 4096,
                "PITR image is too short for a header crash point"
            );
            file.write_all(&bytes[..4096])?;
            crate::chaos::failpoint::wal_crash_point(crash_point);
            file.write_all(&bytes[4096..])?;
        } else {
            file.write_all(bytes)?;
        }
        #[cfg(not(feature = "chaos-testing"))]
        file.write_all(bytes)?;
        #[cfg(feature = "chaos-testing")]
        if let Some(crash_point) = faults.after_file_write_before_sync {
            crate::chaos::failpoint::wal_crash_point(crash_point);
        }
        #[cfg(feature = "chaos-testing")]
        if faults.inject_empty_active_image_failures {
            crate::chaos::failpoint::fail_point!("wal_v7.active_image.before_file_sync", |_| Err(
                anyhow::anyhow!("injected empty Active v7 image file sync failure")
            ));
        }
        file.sync_all()?;
        drop(file);
        #[cfg(feature = "chaos-testing")]
        if let Some(crash_point) = faults.after_file_sync_before_rename {
            crate::chaos::failpoint::wal_crash_point(crash_point);
        }
        #[cfg(feature = "chaos-testing")]
        if faults.inject_empty_active_image_failures {
            crate::chaos::failpoint::fail_point!("wal_v7.active_image.before_rename", |_| Err(
                anyhow::anyhow!("injected empty Active v7 image rename failure")
            ));
        }
        let from = CString::new(temp_name.as_bytes())?;
        let to = CString::new(file_name.as_bytes())?;
        let rename = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                parent_file.as_raw_fd(),
                from.as_ptr(),
                parent_file.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rename != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                std::fs::remove_file(&temp_path)?;
                let existing_file = std::fs::File::open(path)?;
                return verify_existing_pitr_file(existing_file, &parent_file, bytes);
            }
            return Err(error.into());
        }
        #[cfg(feature = "chaos-testing")]
        if let Some(crash_point) = faults.after_rename_before_directory_sync {
            crate::chaos::failpoint::wal_crash_point(crash_point);
        }
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("pitr.install_before_directory_sync", |_| Err(
            anyhow::anyhow!("injected PITR install directory sync failure")
        ));
        #[cfg(feature = "chaos-testing")]
        if faults.inject_empty_active_image_failures {
            crate::chaos::failpoint::fail_point!(
                "wal_v7.active_image.before_directory_sync",
                |_| Err(anyhow::anyhow!(
                    "injected empty Active v7 image directory sync failure"
                ))
            );
        }
        std::fs::File::open(parent)?.sync_all()?;

        Ok(())
    })();
    let _ = std::fs::remove_file(&temp_path);

    result
}

#[cfg(target_os = "linux")]
fn verify_existing_pitr_file(
    mut existing_file: std::fs::File,
    parent_file: &std::fs::File,
    bytes: &[u8],
) -> Result<()> {
    ensure!(
        existing_file.metadata()?.len() == bytes.len() as u64,
        "existing PITR file length mismatch"
    );
    let mut existing = vec![0; bytes.len()];
    existing_file.read_exact(&mut existing)?;
    ensure!(existing == bytes, "existing PITR file identity mismatch");
    existing_file.sync_all()?;
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::fail_point!("pitr.install_before_directory_sync", |_| Err(
        anyhow::anyhow!("injected PITR install directory sync failure")
    ));
    parent_file.sync_all()?;

    Ok(())
}

/// Reserve source-spool bytes using the allocation unit of the destination
/// filesystem, matching the `st_blocks * 512` accounting used during recovery.
#[cfg(target_os = "linux")]
pub(crate) fn filesystem_allocation_reservation(
    directory: impl AsRef<std::path::Path>,
    logical_bytes: u64,
) -> Result<u64> {
    let directory = CString::new(directory.as_ref().as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    // SAFETY: `directory` is a valid NUL-terminated path and `stats` points to
    // writable storage for the duration of the syscall.
    let result = unsafe { libc::statvfs(directory.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(anyhow::Error::new(std::io::Error::last_os_error())
            .context("inspect PITR source filesystem allocation unit"));
    }
    // SAFETY: statvfs initialized `stats` when it returned success.
    let stats = unsafe { stats.assume_init() };
    let allocation_unit = if stats.f_frsize == 0 {
        stats.f_bsize
    } else {
        stats.f_frsize
    };
    // statvfs uses c_ulong, which is narrower on 32-bit Linux.
    #[cfg(target_pointer_width = "32")]
    let allocation_unit = u64::from(allocation_unit);
    ensure!(
        allocation_unit > 0,
        "PITR filesystem allocation unit is zero"
    );

    let blocks = logical_bytes
        .checked_add(allocation_unit - 1)
        .ok_or_else(|| anyhow::anyhow!("PITR source allocation reservation overflows"))?
        / allocation_unit;
    blocks
        .checked_mul(allocation_unit)
        .ok_or_else(|| anyhow::anyhow!("PITR source allocation reservation overflows"))
}

/// Remove staging files left behind by an interrupted PITR file installation.
///
/// Installs stage through `.<object>.tmp-<pid>-<sequence>` and rename the file
/// into place, so a crash between the two steps can strand the staging file.
/// Only regular files matching exactly that shape are removed; anything else in
/// the directory is left alone.
#[cfg(target_os = "linux")]
pub(crate) fn cleanup_pitr_temp_files(dir: impl AsRef<std::path::Path>) -> Result<u64> {
    let dir = dir.as_ref();
    let mut removed = 0u64;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_pitr_install_temp_name(name) {
            continue;
        }
        // Only regular files are ours to remove. A directory matching the temp
        // shape makes this fail with EISDIR, and every caller propagates that:
        // the engine would refuse to open until someone deleted it by hand.
        if !entry.file_type()?.is_file() {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => removed = removed.saturating_add(1),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if removed > 0 {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(removed)
}

#[cfg(target_os = "linux")]
fn is_pitr_install_temp_name(name: &str) -> bool {
    pitr_install_temp_object_name(name).is_some()
}

pub(crate) fn pitr_install_temp_object_name(name: &str) -> Option<&str> {
    let rest = name.strip_prefix('.')?;
    let (object, suffix) = rest.split_once(".tmp-")?;
    if !object.ends_with(".wal") && !object.ends_with(".seal") {
        return None;
    }
    let mut fields = suffix.split('-');

    matches!(
        (fields.next(), fields.next(), fields.next()),
        (Some(pid), Some(sequence), None)
            if !pid.is_empty()
                && pid.bytes().all(|byte| byte.is_ascii_digit())
                && !sequence.is_empty()
                && sequence.bytes().all(|byte| byte.is_ascii_digit())
    )
    .then_some(object)
}

impl RotationReason {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn v7_obligation_reconciliation_continues_releases_and_retries_failures() {
        use crate::{
            pitr::manifest::{ActiveBoundary, ObligationState, PitrObligation, PitrWalFormat},
            pitr::{ArchiveEpochId, ChainAnchor, SegmentId, TimelineId},
            wal::v7::{
                admission::{
                    WalV7ReservationOwner, WalV7ResourceCharge, WalV7ResourceLedger,
                    WalV7ResourceLimits, WalV7RetainedResources,
                },
                codec::{WAL_V7_HEADER_LEN, WalV7Header, encode_generation_zero_frontier},
                install::recover_and_install_active,
                recovery::WalV7RecoveryAuthority,
            },
        };

        let directory = tempfile::tempdir().unwrap();
        let archive_epoch_id = ArchiveEpochId([2; 16]);
        let header = WalV7Header {
            timeline_id: TimelineId([1; 16]),
            archive_epoch_id,
            segment_id: SegmentId(1),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [3; 16],
        };
        let wal_path = directory.path().join("pitr-00000000000000000001.wal");
        let mut image = header.encode().unwrap().to_vec();
        image
            .extend_from_slice(&encode_generation_zero_frontier(header.digest().unwrap()).unwrap());
        std::fs::write(&wal_path, image).unwrap();
        let active_boundary = ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: header.segment_id.0,
            incarnation: header.incarnation,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header.digest().unwrap(),
        };
        let installed = recover_and_install_active(
            &wal_path,
            WalV7RecoveryAuthority::Active {
                predecessor: header.predecessor,
                durable_anchors: vec![active_boundary],
            },
        )
        .unwrap()
        .installed;

        let ledger = WalV7ResourceLedger::new(WalV7ResourceLimits {
            max_logical_wal_bytes: 64 * 1024,
            max_source_spool_bytes: 64 * 1024,
            max_recovery_workspace_bytes: 64 * 1024,
            max_buffer_memory_bytes: 64 * 1024,
            max_seal_index_bytes: 64 * 1024,
        })
        .unwrap();
        let retained = WalV7RetainedResources::new(ledger);
        retained.reserve_obligation_logical(1, 4096).unwrap();
        retained.reserve_obligation_logical(2, 4096).unwrap();
        retained
            .reserve_segment(
                2,
                WalV7ResourceCharge {
                    source_spool_bytes: 4096,
                    ..WalV7ResourceCharge::default()
                },
            )
            .unwrap();
        let owner = Arc::new(
            WalV7ReservationOwner::new_with_retained_resources(Arc::clone(&retained), &installed)
                .unwrap(),
        );
        let batch = owner.reserve_batch(128).unwrap();

        let archived = PitrObligation {
            state: ObligationState::Archived,
            successor_segment_id: 2,
            logical_length: 4096,
            wal_format: PitrWalFormat::V7,
            immutable_boundary: None,
            migration_successor_boundary: None,
        };
        let reclaimable = PitrObligation {
            state: ObligationState::Reclaimable,
            successor_segment_id: 3,
            logical_length: 4096,
            wal_format: PitrWalFormat::V7,
            immutable_boundary: None,
            migration_successor_boundary: None,
        };
        let previous = BTreeMap::from([(1, archived), (2, reclaimable)]);
        let current = BTreeMap::from([(1, archived)]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(3, 4, 4096, 64 * 1024, &previous).unwrap();
        manager
            .retain_v7_reservation_owner(1, Arc::clone(&owner))
            .unwrap();

        let releases = manager.reconcile_obligations(&previous, &current).unwrap();
        let error = manager
            .apply_v7_obligation_reconciliation(releases)
            .unwrap_err();

        assert!(error.to_string().contains("active batch operations"));
        assert!(manager.has_pending_obligation_reconciliation());
        assert!(manager.segment(2).is_none());
        assert_eq!(
            retained.snapshot(),
            WalV7ResourceCharge::for_batch(128).unwrap(),
            "later logical and reclaimed-source releases must run after the first error"
        );

        drop(batch);
        let retry = manager.reconcile_obligations(&current, &current).unwrap();
        manager.apply_v7_obligation_reconciliation(retry).unwrap();

        assert!(!manager.has_pending_obligation_reconciliation());
        assert_eq!(retained.snapshot(), WalV7ResourceCharge::default());
    }

    #[test]
    fn rotation_keeps_old_segment_authoritative_until_install() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        assert_eq!(manager.active_segment_id(), 1);
        assert_eq!(manager.begin_sealing(8192, 4096).unwrap(), 2);
        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealing);
        manager.mark_sealed(1).unwrap();
        assert!(manager.segment(1).unwrap().archive_pin);
        assert_eq!(manager.install_successor().unwrap(), 2);
        assert_eq!(manager.segment(2).unwrap().state, SegmentState::Active);
        assert_eq!(manager.active_segment_id(), 2);
    }

    #[test]
    fn pins_block_reclamation_until_released() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        manager.begin_sealing(4096, 4096).unwrap();
        manager.mark_sealed(1).unwrap();
        assert_eq!(manager.pending_successor_id().unwrap(), 2);
        manager.install_successor().unwrap();
        manager.mark_archived(1).unwrap();
        assert!(manager.segment(1).unwrap().archive_pin);
        manager.pin(1).unwrap();
        assert!(manager.mark_reclaimable(1).is_err());
        manager.unpin(1).unwrap();
        manager.mark_reclaimable(1).unwrap();
        assert!(manager.reclaim(1).is_err());
        manager.release_archive_pin(1).unwrap();
        assert_eq!(manager.reclaim(1).unwrap().state, SegmentState::Reclaiming);
        assert!(
            manager
                .complete_reclaim(
                    1,
                    CleanupReceipt {
                        wal_unlinked: true,
                        seal_unlinked: true,
                        directory_synced: false
                    }
                )
                .is_err()
        );
        assert_eq!(
            manager
                .complete_reclaim(1, CleanupReceipt::durable())
                .unwrap()
                .state,
            SegmentState::Reclaiming
        );
    }

    #[test]
    fn rotation_requests_coalesce() {
        let mut manager = PitrSegmentManager::new(1, 16 * 1024).unwrap();
        assert!(manager.request_rotation(RotationReason::Timer));
        assert!(manager.request_rotation(RotationReason::Backup));
        assert_eq!(
            manager.take_rotation_request(),
            Some(RotationReason::Backup)
        );
        assert!(manager.request_rotation(RotationReason::Backup));
    }

    #[test]
    fn lifecycle_rejects_premature_or_unknown_transitions() {
        let mut manager = PitrSegmentManager::new(1, 16 * 1024).unwrap();
        assert!(manager.install_successor().is_err());
        assert!(manager.mark_sealed(99).is_err());
        assert!(manager.begin_sealing(4096, 0).is_err());
        manager.begin_sealing(4096, 4096).unwrap();
        assert!(manager.install_successor().is_err());
        manager.mark_sealed(1).unwrap();
        manager.install_successor().unwrap();
        assert!(manager.mark_archived(99).is_err());
    }

    #[test]
    fn successor_install_waits_for_durable_wal_and_is_retryable() {
        let mut manager = PitrSegmentManager::new(1, 16 * 1024).unwrap();
        manager.begin_sealing(4096, 4096).unwrap();
        manager.mark_sealed(1).unwrap();
        assert!(
            manager
                .install_successor_after_wal(|_| anyhow::bail!("wal install failed"))
                .is_err()
        );
        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealed);
        assert_eq!(
            manager
                .install_successor_after_wal(|id| {
                    assert_eq!(id, 2);

                    Ok(())
                })
                .unwrap(),
            2
        );
    }

    #[test]
    fn segment_id_exhaustion_is_reported() {
        assert!(PitrSegmentManager::new(u64::MAX, 16 * 1024).is_err());
    }

    #[test]
    fn recovered_obligations_restore_the_source_spool_budget() {
        let obligations = std::collections::BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Sealed,
                successor_segment_id: 2,
                logical_length: 8192,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(2, 3, 4096, 16 * 1024, &obligations).unwrap();

        assert_eq!(manager.active_segment_id(), 2);
        assert_eq!(manager.next_segment_id, 3);
        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealed);
        assert!(manager.segment(1).unwrap().archive_pin);
        assert_eq!(manager.sealed_unarchived_bytes(), 8192);
        assert_eq!(manager.source_spool_reserved(), 16 * 1024);

        manager.mark_archived(1).unwrap();
        manager.mark_reclaimable(1).unwrap();
        manager.release_archive_pin(1).unwrap();
        manager.reclaim(1).unwrap();
        manager
            .complete_reclaim(1, CleanupReceipt::durable())
            .unwrap();
        assert_eq!(manager.source_spool_reserved(), 4096);
    }

    #[test]
    fn recovered_sealing_obligation_retains_its_pending_successor() {
        let obligations = std::collections::BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Sealing,
                successor_segment_id: 2,
                logical_length: 8192,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(1, 3, 8192, 16 * 1024, &obligations).unwrap();

        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealing);
        assert_eq!(manager.pending_successor_id().unwrap(), 2);
        assert_eq!(manager.source_spool_reserved(), 16 * 1024);
        manager.mark_sealed(1).unwrap();
        assert_eq!(manager.install_successor().unwrap(), 2);
        assert_eq!(manager.active_segment_id(), 2);
        assert_eq!(manager.source_spool_reserved(), 16 * 1024);
    }

    #[test]
    fn reconciled_reclamation_releases_recovered_spool_charge() {
        let obligations = BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Reclaimable,
                successor_segment_id: 2,
                logical_length: 8192,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(2, 3, 4096, 16 * 1024, &obligations).unwrap();
        assert_eq!(manager.source_spool_reserved(), 16 * 1024);

        manager
            .reconcile_obligations(&obligations, &BTreeMap::new())
            .unwrap();

        assert!(manager.segment(1).is_none());
        assert_eq!(manager.active_segment_id(), 2);
        assert_eq!(manager.source_spool_reserved(), 4096);
    }

    #[test]
    fn reconciled_sealing_consumes_pending_successor_and_preserves_pins() {
        let obligations = BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Sealing,
                successor_segment_id: 2,
                logical_length: 8192,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(1, 3, 12 * 1024, 32 * 1024, &obligations)
                .unwrap();
        manager.pin(1).unwrap();
        assert_eq!(manager.source_spool_reserved(), 20 * 1024);
        let mut reconciled = obligations.clone();
        reconciled.get_mut(&1).unwrap().state = crate::pitr::manifest::ObligationState::Sealed;

        manager
            .reconcile_obligations(&obligations, &reconciled)
            .unwrap();

        assert_eq!(manager.active_segment_id(), 2);
        assert!(manager.pending_successor_id().is_err());
        let sealed = manager.segment(1).unwrap();
        assert_eq!(sealed.state, SegmentState::Sealed);
        assert!(sealed.archive_pin);
        assert_eq!(sealed.source_pins, 1);
        assert_eq!(sealed.source_spool_bytes, 12 * 1024);
        assert_eq!(manager.segment(2).unwrap().state, SegmentState::Active);
        assert_eq!(manager.source_spool_reserved(), 20 * 1024);
    }

    #[test]
    fn reconciled_sealing_preserves_the_pending_successor_reservation() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        manager.begin_sealing(8192, 8192).unwrap();
        let obligations = BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Sealing,
                successor_segment_id: 2,
                logical_length: 8192,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut reconciled = obligations.clone();
        reconciled.get_mut(&1).unwrap().state = crate::pitr::manifest::ObligationState::Sealed;

        manager
            .reconcile_obligations(&obligations, &reconciled)
            .unwrap();

        assert!(manager.pending_successor_id().is_err());
        assert_eq!(manager.segment(2).unwrap().source_spool_bytes, 8192);
        assert_eq!(manager.source_spool_reserved(), 20 * 1024);
    }

    #[test]
    fn deferred_sealing_reconciliation_uses_the_advanced_manifest_projection() {
        let obligations = BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Sealing,
                successor_segment_id: 2,
                logical_length: 8192,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(1, 3, 8192, 16 * 1024, &obligations).unwrap();
        manager.pin(1).unwrap();
        manager.defer_obligation_reconciliation(obligations.clone());
        let mut published = obligations;
        published.get_mut(&1).unwrap().state = crate::pitr::manifest::ObligationState::Sealed;

        manager
            .reconcile_obligations(&published, &published)
            .unwrap();

        assert_eq!(manager.active_segment_id(), 2);
        assert!(manager.pending_successor_id().is_err());
        assert!(!manager.has_pending_obligation_reconciliation());
        assert_eq!(manager.segment(1).unwrap().source_pins, 1);
        assert_eq!(manager.source_spool_reserved(), 16 * 1024);
    }

    #[test]
    fn recovered_over_limit_estimate_attaches_but_blocks_new_growth() {
        let obligations = std::collections::BTreeMap::from([(
            1,
            crate::pitr::manifest::PitrObligation {
                state: crate::pitr::manifest::ObligationState::Sealed,
                successor_segment_id: 2,
                logical_length: 4096,
                wal_format: crate::pitr::manifest::PitrWalFormat::LegacyV5V6,
                immutable_boundary: None,
                migration_successor_boundary: None,
            },
        )]);
        let mut manager =
            PitrSegmentManager::from_recovered_state(2, 3, 4096, 8192, &obligations).unwrap();

        assert_eq!(manager.source_spool_reserved(), 12 * 1024);
        assert!(manager.begin_sealing(4096, 4096).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn successor_wal_install_is_no_replace_and_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment-1.wal");
        let header = crate::pitr::WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(1),
            predecessor: crate::pitr::ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };
        install_v5_wal_header(&path, header).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
        assert!(install_v5_wal_header(&path, header).is_ok());
        let mismatched = crate::pitr::WalV5Header {
            segment_id: crate::pitr::SegmentId(2),
            ..header
        };
        assert!(install_v5_wal_header(&path, mismatched).is_err());
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(crate::pitr::decode_v5_file_header(&bytes).unwrap(), header);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wal_header_install_rejects_existing_payload_or_partial_header() {
        let dir = tempfile::tempdir().unwrap();
        let header = crate::pitr::WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(1),
            predecessor: crate::pitr::ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };

        let payload_path = dir.path().join("payload.wal");
        install_v5_wal_header(&payload_path, header).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&payload_path)
            .unwrap()
            .write_all(&[1])
            .unwrap();
        assert!(install_v5_wal_header(&payload_path, header).is_err());

        let partial_path = dir.path().join("partial.wal");
        std::fs::write(&partial_path, b"WAL2").unwrap();
        assert!(install_v5_wal_header(&partial_path, header).is_err());
    }
}
