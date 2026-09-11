//! Dormant PITR segment lifecycle and source-pin harness.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use std::collections::BTreeMap;

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

#[derive(Debug)]
pub(crate) struct PitrSegmentManager {
    segments: BTreeMap<u64, SegmentMetadata>,
    active_segment_id: u64,
    next_segment_id: u64,
    pending_successor: Option<SegmentMetadata>,
    source_spool_limit: u64,
    source_spool_reserved: u64,
    pending_rotation: Option<RotationReason>,
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
            active_segment_id,
            next_segment_id: active_segment_id
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("segment ID exhausted"))?,
            pending_successor: None,
            source_spool_limit,
            source_spool_reserved: 4096,
            pending_rotation: None,
        })
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

    #[test]
    fn rotation_keeps_old_segment_authoritative_until_install() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        assert_eq!(manager.begin_sealing(8192, 4096).unwrap(), 2);
        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealing);
        manager.mark_sealed(1).unwrap();
        assert!(manager.segment(1).unwrap().archive_pin);
        assert_eq!(manager.install_successor().unwrap(), 2);
        assert_eq!(manager.segment(2).unwrap().state, SegmentState::Active);
    }

    #[test]
    fn pins_block_reclamation_until_released() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        manager.begin_sealing(4096, 4096).unwrap();
        manager.mark_sealed(1).unwrap();
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
    fn segment_id_exhaustion_is_reported() {
        assert!(PitrSegmentManager::new(u64::MAX, 16 * 1024).is_err());
    }
}
