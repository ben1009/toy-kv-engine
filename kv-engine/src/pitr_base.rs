//! Dormant PITR-aware base-capture boundary and metadata contract.
#![allow(dead_code)]

use anyhow::{Result, ensure};

use crate::pitr_manifest::{PersistedChainAnchor, PersistedRecordedAt};

pub(crate) const PITR_BASE_WAL_REPLAY_VERSION: u16 = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrBaseMetadata {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) included_commit_ts: Option<u64>,
    pub(crate) boundary_segment_id: u64,
    pub(crate) boundary_anchor: PersistedChainAnchor,
    pub(crate) base_recorded_at: PersistedRecordedAt,
    pub(crate) wal_replay_version: u16,
    pub(crate) compatibility_digest: [u8; 32],
}

impl PitrBaseMetadata {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.repository_id != [0; 16],
            "PITR base repository identity is empty"
        );
        ensure!(
            self.timeline_id != [0; 16],
            "PITR base timeline identity is empty"
        );
        ensure!(
            self.archive_epoch_id != [0; 16],
            "PITR base archive epoch identity is empty"
        );
        ensure!(
            self.included_commit_ts
                .is_none_or(|commit_ts| commit_ts != 0),
            "PITR base commit high-water is invalid"
        );
        ensure!(
            self.wal_replay_version == PITR_BASE_WAL_REPLAY_VERSION,
            "unsupported PITR base WAL replay version"
        );
        ensure!(
            self.base_recorded_at.nanos < 1_000_000_000,
            "PITR base recorded time is invalid"
        );
        match self.boundary_anchor {
            PersistedChainAnchor::Genesis { archive_epoch_id } => {
                ensure!(
                    archive_epoch_id == self.archive_epoch_id,
                    "PITR base genesis anchor has the wrong epoch"
                );
            }
            PersistedChainAnchor::Segment { segment_id, .. } => {
                ensure!(
                    segment_id == self.boundary_segment_id,
                    "PITR base boundary anchor has the wrong segment"
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PitrBaseCaptureState {
    AdmissionOpen,
    AdmissionStopped,
    Captured,
    Published,
}

#[derive(Debug)]
pub(crate) struct PitrBaseCaptureCoordinator {
    state: PitrBaseCaptureState,
    boundary_segment_id: Option<u64>,
    metadata: Option<PitrBaseMetadata>,
}

impl Default for PitrBaseCaptureCoordinator {
    fn default() -> Self {
        Self {
            state: PitrBaseCaptureState::AdmissionOpen,
            boundary_segment_id: None,
            metadata: None,
        }
    }
}

impl PitrBaseCaptureCoordinator {
    pub(crate) fn stop_admission(&mut self, boundary_segment_id: u64) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::AdmissionOpen,
            "PITR base admission is not open"
        );
        self.boundary_segment_id = Some(boundary_segment_id);
        self.state = PitrBaseCaptureState::AdmissionStopped;
        Ok(())
    }

    pub(crate) fn capture(&mut self, metadata: PitrBaseMetadata) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::AdmissionStopped,
            "PITR base capture is not stopped at a boundary"
        );
        ensure!(
            self.boundary_segment_id == Some(metadata.boundary_segment_id),
            "PITR base metadata does not match the stopped boundary"
        );
        metadata.validate()?;
        self.metadata = Some(metadata);
        self.state = PitrBaseCaptureState::Captured;
        Ok(())
    }

    pub(crate) fn publish(&mut self) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::Captured,
            "PITR base cannot publish before capture"
        );
        self.state = PitrBaseCaptureState::Published;
        Ok(())
    }

    pub(crate) fn release_admission(&mut self) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::Published,
            "PITR base admission cannot resume before publication"
        );
        self.state = PitrBaseCaptureState::AdmissionOpen;
        self.boundary_segment_id = None;
        self.metadata = None;
        Ok(())
    }

    pub(crate) fn state(&self) -> PitrBaseCaptureState {
        self.state
    }

    pub(crate) fn metadata(&self) -> Option<&PitrBaseMetadata> {
        self.metadata.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> PitrBaseMetadata {
        PitrBaseMetadata {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            included_commit_ts: Some(7),
            boundary_segment_id: 9,
            boundary_anchor: PersistedChainAnchor::Segment {
                segment_id: 9,
                wal_digest: [4; 32],
                seal_digest: [5; 32],
            },
            base_recorded_at: PersistedRecordedAt {
                secs: 10,
                nanos: 11,
            },
            wal_replay_version: PITR_BASE_WAL_REPLAY_VERSION,
            compatibility_digest: [6; 32],
        }
    }

    #[test]
    fn base_capture_requires_durable_ordering() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        assert!(coordinator.publish().is_err());
        assert!(coordinator.capture(metadata()).is_err());
        coordinator.stop_admission(9).unwrap();
        coordinator.capture(metadata()).unwrap();
        assert_eq!(coordinator.state(), PitrBaseCaptureState::Captured);
        assert!(coordinator.release_admission().is_err());
        coordinator.publish().unwrap();
        coordinator.release_admission().unwrap();
        assert_eq!(coordinator.state(), PitrBaseCaptureState::AdmissionOpen);
        assert!(coordinator.metadata().is_none());
    }

    #[test]
    fn base_capture_rejects_boundary_and_identity_mismatches() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.stop_admission(9).unwrap();
        let mut wrong_boundary = metadata();
        wrong_boundary.boundary_segment_id = 10;
        assert!(coordinator.capture(wrong_boundary).is_err());
        let mut wrong_anchor = metadata();
        wrong_anchor.boundary_anchor = PersistedChainAnchor::Genesis {
            archive_epoch_id: [8; 16],
        };
        assert!(coordinator.capture(wrong_anchor).is_err());
        assert_eq!(coordinator.state(), PitrBaseCaptureState::AdmissionStopped);
    }

    #[test]
    fn base_metadata_rejects_invalid_replay_version_and_time() {
        let mut invalid = metadata();
        invalid.wal_replay_version = 4;
        assert!(invalid.validate().is_err());
        invalid = metadata();
        invalid.base_recorded_at.nanos = 1_000_000_000;
        assert!(invalid.validate().is_err());
    }
}
