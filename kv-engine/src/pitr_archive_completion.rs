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
        self.segments.mark_reclaimable(segment_id)?;
        self.segments.release_archive_pin(segment_id)
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
}
