//! Dormant PITR-aware base-capture boundary and metadata contract.
#![allow(dead_code)]

use anyhow::{Result, ensure};

use crate::pitr_manifest::{
    PersistedChainAnchor, PersistedRecordedAt, PitrManifestRecord, PitrMode, PitrState,
    replay_pitr_records,
};

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
    manifest_state: Option<PitrState>,
    metadata: Option<PitrBaseMetadata>,
}

impl Default for PitrBaseCaptureCoordinator {
    fn default() -> Self {
        Self {
            state: PitrBaseCaptureState::AdmissionOpen,
            boundary_segment_id: None,
            manifest_state: None,
            metadata: None,
        }
    }
}

impl PitrBaseCaptureCoordinator {
    pub(crate) fn bind_manifest_state(&mut self, manifest_state: PitrState) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::AdmissionOpen,
            "PITR base manifest state cannot change during capture"
        );
        ensure!(
            manifest_state.mode == PitrMode::Enabled,
            "PITR base requires an enabled manifest state"
        );
        self.manifest_state = Some(replay_pitr_records([PitrManifestRecord::Snapshot(
            Box::new(manifest_state),
        )])?);
        Ok(())
    }

    pub(crate) fn stop_admission(&mut self, boundary_segment_id: u64) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::AdmissionOpen,
            "PITR base admission is not open"
        );
        let manifest_state = self
            .manifest_state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PITR base manifest state is not bound"))?;
        ensure!(
            manifest_state.active_segment_id == Some(boundary_segment_id),
            "PITR base boundary is not the active manifest segment"
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
        if let Some(manifest_state) = &self.manifest_state {
            ensure!(
                manifest_state.repository_id == Some(metadata.repository_id)
                    && manifest_state.timeline_id == Some(metadata.timeline_id)
                    && manifest_state.archive_epoch_id == Some(metadata.archive_epoch_id),
                "PITR base metadata identity does not match the manifest"
            );
            ensure!(
                manifest_state.predecessor_anchor == Some(metadata.boundary_anchor),
                "PITR base anchor does not match the manifest predecessor"
            );
            match (
                metadata.included_commit_ts,
                manifest_state.last_commit_anchor,
            ) {
                (None, None) => {}
                (Some(included), Some(anchor)) => ensure!(
                    included <= anchor.commit_ts,
                    "PITR base commit high-water exceeds the manifest"
                ),
                _ => ensure!(
                    false,
                    "PITR base commit high-water does not match the manifest"
                ),
            }
            if let Some(last_recorded_at) = manifest_state.last_recorded_at {
                ensure!(
                    metadata.base_recorded_at >= last_recorded_at,
                    "PITR base recorded time regresses the manifest"
                );
            }
        }
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
        self.manifest_state = None;
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
    use crate::pitr_manifest::{PersistedCommitAnchor, PitrManifestRecord, replay_pitr_records};

    fn metadata() -> PitrBaseMetadata {
        PitrBaseMetadata {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            included_commit_ts: None,
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

    fn manifest_state() -> PitrState {
        let mut state = replay_pitr_records([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: crate::pitr_manifest::PersistedPitrConfig {
                    archive_interval_ms: 1000,
                    max_segment_bytes: 4096,
                    max_unarchived_bytes: 8192,
                    max_source_spool_bytes: 16384,
                },
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 9,
            },
        ])
        .unwrap();
        state.predecessor_anchor = Some(metadata().boundary_anchor);
        let recorded_at = PersistedRecordedAt {
            secs: 10,
            nanos: 11,
        };
        state.last_recorded_at = Some(recorded_at);
        state.last_commit_anchor = Some(PersistedCommitAnchor {
            segment_id: 9,
            commit_ts: 7,
            recorded_at,
            entry_digest: [6; 32],
        });
        state
    }

    #[test]
    fn base_capture_requires_durable_ordering() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        assert!(coordinator.publish().is_err());
        assert!(coordinator.capture(metadata()).is_err());
        coordinator.bind_manifest_state(manifest_state()).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut captured = metadata();
        captured.included_commit_ts = Some(7);
        coordinator.capture(captured).unwrap();
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
        coordinator.bind_manifest_state(manifest_state()).unwrap();
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

    #[test]
    fn bound_capture_rejects_manifest_identity_or_anchor_mismatch() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(manifest_state()).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut wrong_identity = metadata();
        wrong_identity.archive_epoch_id = [8; 16];
        assert!(coordinator.capture(wrong_identity).is_err());
        let mut wrong_anchor = metadata();
        wrong_anchor.boundary_anchor = PersistedChainAnchor::Segment {
            segment_id: 9,
            wal_digest: [7; 32],
            seal_digest: [7; 32],
        };
        assert!(coordinator.capture(wrong_anchor).is_err());
        assert_eq!(coordinator.state(), PitrBaseCaptureState::AdmissionStopped);
    }

    #[test]
    fn bound_capture_rejects_commit_high_water_mismatch() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(manifest_state()).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut excessive = metadata();
        excessive.included_commit_ts = Some(8);
        assert!(coordinator.capture(excessive).is_err());

        let mut missing = metadata();
        missing.included_commit_ts = None;
        assert!(coordinator.capture(missing).is_err());
    }

    #[test]
    fn bound_capture_rejects_recorded_time_regression() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(manifest_state()).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut regressed = metadata();
        regressed.included_commit_ts = Some(7);
        regressed.base_recorded_at = PersistedRecordedAt {
            secs: 10,
            nanos: 10,
        };
        assert!(coordinator.capture(regressed).is_err());
    }
}
