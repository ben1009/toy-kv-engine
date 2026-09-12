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
        let requires_catalog = segments
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
            requires_catalog.is_subset(&catalog_committed),
            "source archive state lacks catalog commit"
        );
        let current_segments = segments
            .segment_ids()
            .filter(|segment_id| {
                segments.segment(*segment_id).is_some_and(|segment| {
                    !(segment.state == SegmentState::Reclaimable && !segment.archive_pin)
                })
            })
            .collect::<BTreeSet<_>>();
        let published = catalog_committed
            .intersection(&current_segments)
            .copied()
            .collect::<BTreeSet<_>>();
        let manifest_archived = segments
            .segment_ids()
            .filter(|segment_id| {
                segments.segment(*segment_id).is_some_and(|segment| {
                    segment.state == SegmentState::Archived
                        || (segment.state == SegmentState::Reclaimable && segment.archive_pin)
                })
            })
            .collect::<BTreeSet<_>>();
        ensure!(
            manifest_archived.is_subset(&published),
            "source manifest Archived state lacks catalog commit"
        );
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
            _ => anyhow::bail!("archive pin release requires archived or reclaimable state"),
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
    fn recovery_releases_pin_after_reclaimable_transition() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        segments.mark_reclaimable(1).unwrap();
        let mut completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        completion.release_archive_pin(1).unwrap();
        assert_eq!(
            completion.segment_state(1).unwrap(),
            SegmentState::Reclaimable
        );
    }

    #[test]
    fn recovery_treats_reclaimable_without_pin_as_complete() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        segments.mark_reclaimable(1).unwrap();
        segments.release_archive_pin(1).unwrap();
        let completion = PitrArchiveCompletion::recover(segments, [1]).unwrap();
        assert!(!completion.published.contains(&1));
        assert!(!completion.manifest_archived.contains(&1));
        assert_eq!(
            completion.segment_state(1).unwrap(),
            SegmentState::Reclaimable
        );
    }

    #[test]
    fn terminal_source_state_still_requires_catalog_evidence() {
        let mut segments = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        segments.begin_sealing(8192, 4096).unwrap();
        segments.mark_sealed(1).unwrap();
        segments.install_successor().unwrap();
        segments.mark_archived(1).unwrap();
        segments.mark_reclaimable(1).unwrap();
        segments.release_archive_pin(1).unwrap();
        assert!(PitrArchiveCompletion::recover(segments, []).is_err());
    }
}
