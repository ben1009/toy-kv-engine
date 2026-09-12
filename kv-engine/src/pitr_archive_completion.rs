//! Dormant PITR archive-to-source-manifest completion bridge.
#![allow(dead_code)]

use std::collections::BTreeSet;

use anyhow::{Result, ensure};

use crate::pitr_segment::{PitrSegmentManager, SegmentState};

#[derive(Debug)]
pub(crate) struct PitrArchiveCompletion {
    segments: PitrSegmentManager,
    published: BTreeSet<u64>,
    manifest_archived: BTreeSet<u64>,
}

impl PitrArchiveCompletion {
    pub(crate) fn new(segments: PitrSegmentManager) -> Self {
        Self {
            segments,
            published: BTreeSet::new(),
            manifest_archived: BTreeSet::new(),
        }
    }

    pub(crate) fn recover(
        segments: PitrSegmentManager,
        catalog_committed: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        let catalog_committed = catalog_committed.into_iter().collect::<BTreeSet<_>>();
        let current_segments = segments.segment_ids().collect::<BTreeSet<_>>();
        let published = catalog_committed
            .intersection(&current_segments)
            .copied()
            .collect::<BTreeSet<_>>();
        let archive_completed = segments
            .segment_ids()
            .filter(|segment_id| {
                segments.segment(*segment_id).is_some_and(|segment| {
                    matches!(
                        segment.state,
                        SegmentState::Archived
                            | SegmentState::Reclaimable
                            | SegmentState::Reclaiming
                    )
                })
            })
            .collect::<BTreeSet<_>>();
        ensure!(
            archive_completed.is_subset(&published),
            "source manifest Archived state lacks catalog commit"
        );
        let manifest_archived = segments
            .segment_ids()
            .filter(|segment_id| {
                segments.segment(*segment_id).is_some_and(|segment| {
                    segment.state == SegmentState::Archived
                        || (segment.state == SegmentState::Reclaimable && segment.archive_pin)
                })
            })
            .collect::<BTreeSet<_>>();
        Ok(Self {
            segments,
            published,
            manifest_archived,
        })
    }

    pub(crate) fn archive_committed(&mut self, segment_id: u64) -> Result<()> {
        ensure!(
            self.segments.segment(segment_id).is_some(),
            "unknown PITR segment"
        );
        ensure!(
            self.segment_state(segment_id)? == SegmentState::Sealed,
            "archive commit requires a sealed segment"
        );
        ensure!(
            self.published.insert(segment_id),
            "archive completion already recorded"
        );
        Ok(())
    }

    pub(crate) fn publish_source_manifest_archived(&mut self, segment_id: u64) -> Result<()> {
        ensure!(
            self.published.contains(&segment_id),
            "archive commit is not durable"
        );
        ensure!(
            !self.manifest_archived.contains(&segment_id),
            "source manifest archive state already published"
        );
        self.segments.mark_archived(segment_id)?;
        self.manifest_archived.insert(segment_id);
        Ok(())
    }

    pub(crate) fn release_archive_pin(&mut self, segment_id: u64) -> Result<()> {
        ensure!(
            self.manifest_archived.contains(&segment_id),
            "source manifest archive state is not durable"
        );
        match self.segment_state(segment_id)? {
            SegmentState::Archived => self.segments.mark_reclaimable(segment_id)?,
            SegmentState::Reclaimable => {}
            _ => anyhow::bail!("PITR archive pin release is out of order"),
        }
        self.segments.release_archive_pin(segment_id)?;
        self.published.remove(&segment_id);
        self.manifest_archived.remove(&segment_id);
        Ok(())
    }

    pub(crate) fn segment_state(&self, segment_id: u64) -> Result<SegmentState> {
        self.segments
            .segment(segment_id)
            .map(|segment| segment.state)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed_completion() -> PitrArchiveCompletion {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        PitrArchiveCompletion::new(segments)
    }

    #[test]
    fn archive_pin_release_requires_durable_source_manifest_state() {
        let mut completion = sealed_completion();
        assert!(completion.release_archive_pin(1).is_err());
        completion.archive_committed(1).unwrap();
        assert!(completion.release_archive_pin(1).is_err());
        completion.publish_source_manifest_archived(1).unwrap();
        assert!(completion.release_archive_pin(1).is_err());
        assert_eq!(completion.segment_state(1).unwrap(), SegmentState::Archived);
    }

    #[test]
    fn completion_is_one_shot_and_preserves_ordering() {
        let mut completion = sealed_completion();
        completion.archive_committed(1).unwrap();
        assert!(completion.archive_committed(1).is_err());
        assert!(completion.publish_source_manifest_archived(1).is_ok());
        assert!(completion.publish_source_manifest_archived(1).is_err());
    }

    #[test]
    fn terminal_pin_release_prunes_completion_markers() {
        let mut completion = sealed_completion();
        completion.segments.install_successor().unwrap();
        completion.archive_committed(1).unwrap();
        completion.publish_source_manifest_archived(1).unwrap();
        completion.release_archive_pin(1).unwrap();
        assert!(completion.archive_committed(1).is_err());
        assert!(completion.release_archive_pin(1).is_err());
    }

    #[test]
    fn recovery_resumes_after_catalog_commit() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        let mut completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        completion.publish_source_manifest_archived(1).unwrap();
        assert_eq!(completion.segment_state(1).unwrap(), SegmentState::Archived);
    }

    #[test]
    fn recovery_releases_pin_after_manifest_archived() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        let mut completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        completion.release_archive_pin(1).unwrap();
        assert_eq!(
            completion.segment_state(1).unwrap(),
            SegmentState::Reclaimable
        );
    }

    #[test]
    fn recovery_ignores_historical_catalog_segments() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        let completion = PitrArchiveCompletion::recover(segments, [0, 1]).unwrap();
        assert_eq!(completion.segment_state(1).unwrap(), SegmentState::Sealed);
    }

    #[test]
    fn recovery_does_not_retain_marker_after_archive_pin_release() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        segments.mark_reclaimable(1).unwrap();
        segments.release_archive_pin(1).unwrap();
        segments.reclaim(1).unwrap();

        let mut completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        let error = completion.release_archive_pin(1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source manifest archive state is not durable")
        );
    }

    #[test]
    fn recovery_releases_pin_from_durable_reclaimable_state() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        segments.mark_reclaimable(1).unwrap();

        let mut completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        completion.release_archive_pin(1).unwrap();
        let segment = completion.segments.segment(1).unwrap();
        assert_eq!(segment.state, SegmentState::Reclaimable);
        assert!(!segment.archive_pin);
    }

    #[test]
    fn recovery_does_not_restore_marker_after_pin_was_released() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        segments.mark_reclaimable(1).unwrap();
        segments.release_archive_pin(1).unwrap();

        let mut completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        let error = completion.release_archive_pin(1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source manifest archive state is not durable")
        );
    }

    #[test]
    fn pin_release_rejects_unexpected_reclaiming_state() {
        let mut completion = sealed_completion();
        completion.segments.install_successor().unwrap();
        completion.archive_committed(1).unwrap();
        completion.publish_source_manifest_archived(1).unwrap();
        completion.segments.mark_reclaimable(1).unwrap();
        completion.segments.release_archive_pin(1).unwrap();
        completion.segments.reclaim(1).unwrap();

        let error = completion.release_archive_pin(1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("PITR archive pin release is out of order")
        );
    }
}
