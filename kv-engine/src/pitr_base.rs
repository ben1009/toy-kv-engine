//! Dormant PITR-aware base-capture boundary and metadata contract.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::cmp::max;

use crate::pitr_manifest::{
    PersistedChainAnchor, PersistedRecordedAt, PitrManifestRecord, PitrMode, PitrState,
    replay_pitr_records,
};

pub(crate) const PITR_BASE_WAL_REPLAY_VERSION: u16 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PitrBaseTimeAnchor {
    Indexed {
        segment_id: u64,
        commit_ts: u64,
        recorded_at: PersistedRecordedAt,
        entry_digest: [u8; 32],
    },
    Observed {
        commit_ts: Option<u64>,
        observed_at: PersistedRecordedAt,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PitrBaseMetadata {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) included_commit_ts: Option<u64>,
    pub(crate) boundary_segment_id: u64,
    pub(crate) boundary_anchor: PersistedChainAnchor,
    pub(crate) base_recorded_at: PersistedRecordedAt,
    pub(crate) time_anchor: PitrBaseTimeAnchor,
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
            self.compatibility_digest != [0; 32],
            "PITR base compatibility digest is empty"
        );
        ensure!(
            self.base_recorded_at.nanos < 1_000_000_000,
            "PITR base recorded time is invalid"
        );
        match self.time_anchor {
            PitrBaseTimeAnchor::Indexed {
                segment_id,
                commit_ts,
                recorded_at,
                ..
            } => ensure!(
                segment_id < self.boundary_segment_id
                    && commit_ts != 0
                    && recorded_at == self.base_recorded_at,
                "PITR indexed time anchor is invalid"
            ),
            PitrBaseTimeAnchor::Observed {
                commit_ts,
                observed_at,
            } => {
                ensure!(
                    commit_ts.is_none_or(|commit_ts| commit_ts != 0)
                        && observed_at.nanos < 1_000_000_000,
                    "PITR observed time anchor is invalid"
                );
                ensure!(
                    observed_at <= self.base_recorded_at,
                    "PITR clamped base time precedes observed time"
                );
            }
        }
        ensure!(
            match (self.included_commit_ts, self.time_anchor) {
                (included, PitrBaseTimeAnchor::Indexed { commit_ts, .. })
                | (
                    included,
                    PitrBaseTimeAnchor::Observed {
                        commit_ts: Some(commit_ts),
                        ..
                    },
                ) => {
                    included == Some(commit_ts)
                }
                (
                    None,
                    PitrBaseTimeAnchor::Observed {
                        commit_ts: None, ..
                    },
                ) => true,
                _ => false,
            },
            "PITR base commit high-water disagrees with its time anchor"
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
                    segment_id < self.boundary_segment_id,
                    "PITR base predecessor anchor is not before the boundary"
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CapturedBaseBoundary {
    segment_id: u64,
    commit_high_water: Option<u64>,
}

impl CapturedBaseBoundary {
    fn new(segment_id: u64, commit_high_water: Option<u64>) -> Self {
        Self {
            segment_id,
            commit_high_water,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PitrBaseCaptureCoordinator {
    state: PitrBaseCaptureState,
    boundary_segment_id: Option<u64>,
    captured_commit_high_water: Option<Option<u64>>,
    manifest_state: Option<PitrState>,
    compatibility_digest: Option<[u8; 32]>,
    observed_clamp_persisted: bool,
    metadata: Option<PitrBaseMetadata>,
}

impl Default for PitrBaseCaptureCoordinator {
    fn default() -> Self {
        Self {
            state: PitrBaseCaptureState::AdmissionOpen,
            boundary_segment_id: None,
            captured_commit_high_water: None,
            manifest_state: None,
            compatibility_digest: None,
            observed_clamp_persisted: false,
            metadata: None,
        }
    }
}

impl PitrBaseCaptureCoordinator {
    #[cfg(test)]
    pub(crate) fn bind_manifest_state(&mut self, manifest_state: PitrState) -> Result<()> {
        self.bind_manifest_state_with_compatibility(manifest_state, [6; 32])
    }

    pub(crate) fn bind_manifest_state_with_compatibility(
        &mut self,
        manifest_state: PitrState,
        compatibility_digest: [u8; 32],
    ) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::AdmissionOpen,
            "PITR base manifest state cannot change during capture"
        );
        ensure!(
            compatibility_digest != [0; 32],
            "PITR base compatibility digest is empty"
        );
        let validated_state =
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(manifest_state))])?;
        ensure!(
            validated_state.mode == PitrMode::Enabled,
            "PITR base requires an enabled manifest state"
        );
        self.manifest_state = Some(validated_state);
        self.compatibility_digest = Some(compatibility_digest);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn stop_admission(&mut self, boundary_segment_id: u64) -> Result<()> {
        let captured_commit_high_water = self
            .manifest_state
            .as_ref()
            .and_then(|state| state.last_commit_anchor.map(|anchor| anchor.commit_ts));
        self.stop_admission_at(CapturedBaseBoundary::new(
            boundary_segment_id,
            captured_commit_high_water,
        ))
    }

    pub(crate) fn stop_admission_at(&mut self, boundary: CapturedBaseBoundary) -> Result<()> {
        let boundary_segment_id = boundary.segment_id;
        let captured_commit_high_water = boundary.commit_high_water;
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
        ensure!(
            captured_commit_high_water.is_none_or(|commit_ts| commit_ts != 0),
            "PITR captured commit high-water is invalid"
        );
        self.boundary_segment_id = Some(boundary_segment_id);
        self.captured_commit_high_water = Some(captured_commit_high_water);
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
        ensure!(
            self.captured_commit_high_water == Some(metadata.included_commit_ts),
            "PITR base commit high-water does not match the capture barrier"
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
            match metadata.time_anchor {
                PitrBaseTimeAnchor::Indexed { .. } => {
                    ensure!(
                        manifest_state
                            .last_commit_anchor
                            .is_some_and(
                                |anchor| metadata.included_commit_ts == Some(anchor.commit_ts)
                            ),
                        "PITR indexed base commit high-water does not match the manifest"
                    )
                }
                PitrBaseTimeAnchor::Observed { commit_ts, .. } => ensure!(
                    metadata.included_commit_ts == commit_ts
                        && match (commit_ts, manifest_state.last_commit_anchor) {
                            (None, None) => true,
                            (Some(_), None) => true,
                            (Some(observed), Some(anchor)) => observed > anchor.commit_ts,
                            (None, Some(_)) => false,
                        },
                    "PITR observed base commit high-water does not match the manifest"
                ),
            }
            let expected = self
                .compatibility_digest
                .ok_or_else(|| anyhow::anyhow!("PITR base compatibility is not bound"))?;
            ensure!(
                metadata.compatibility_digest == expected,
                "PITR base compatibility does not match the manifest"
            );
            if let PitrBaseTimeAnchor::Indexed {
                segment_id,
                commit_ts,
                recorded_at,
                entry_digest,
            } = metadata.time_anchor
            {
                let anchor = manifest_state
                    .last_commit_anchor
                    .ok_or_else(|| anyhow::anyhow!("indexed PITR base has no manifest anchor"))?;
                ensure!(
                    (segment_id, commit_ts, recorded_at, entry_digest)
                        == (
                            anchor.segment_id,
                            anchor.commit_ts,
                            anchor.recorded_at,
                            anchor.entry_digest,
                        ),
                    "PITR indexed time anchor does not match the manifest"
                );
            }
            if let PitrBaseTimeAnchor::Observed { observed_at, .. } = metadata.time_anchor {
                ensure!(
                    metadata.base_recorded_at
                        == max(
                            manifest_state.last_recorded_at.unwrap_or(observed_at),
                            observed_at
                        ),
                    "PITR observed base time is not the canonical clamp"
                );
            }
        }
        metadata.validate()?;
        self.metadata = Some(metadata);
        self.observed_clamp_persisted = false;
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
        if matches!(
            self.metadata.as_ref().map(|metadata| metadata.time_anchor),
            Some(PitrBaseTimeAnchor::Observed { .. })
        ) {
            ensure!(
                self.observed_clamp_persisted,
                "PITR observed base clock clamp is not durable"
            );
        }
        self.state = PitrBaseCaptureState::AdmissionOpen;
        self.boundary_segment_id = None;
        self.captured_commit_high_water = None;
        self.manifest_state = None;
        self.compatibility_digest = None;
        self.metadata = None;
        self.observed_clamp_persisted = false;
        Ok(())
    }

    pub(crate) fn confirm_observed_clamp_persisted(
        &mut self,
        persisted_manifest_state: PitrState,
    ) -> Result<()> {
        ensure!(
            self.state == PitrBaseCaptureState::Published,
            "PITR base clamp confirmation requires publication"
        );
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PITR base metadata is missing"))?;
        let PitrBaseTimeAnchor::Observed { observed_at, .. } = metadata.time_anchor else {
            return Err(anyhow::anyhow!(
                "PITR indexed base does not need a clock clamp"
            ));
        };
        let persisted = replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(
            persisted_manifest_state,
        ))])?;
        let current = self
            .manifest_state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PITR base manifest state is missing"))?;
        ensure!(
            persisted.repository_id == current.repository_id
                && persisted.timeline_id == current.timeline_id
                && persisted.archive_epoch_id == current.archive_epoch_id,
            "PITR persisted clamp changes the manifest identity"
        );
        ensure!(
            persisted.last_recorded_at
                == Some(max(
                    current.last_recorded_at.unwrap_or(observed_at),
                    observed_at
                )),
            "PITR persisted clamp is not the exact monotonic update"
        );
        let mut expected = current.clone();
        expected.last_recorded_at = persisted.last_recorded_at;
        ensure!(
            persisted == expected,
            "PITR persisted clamp changes unrelated manifest state"
        );
        self.manifest_state = Some(persisted);
        self.observed_clamp_persisted = true;
        Ok(())
    }

    pub(crate) fn state(&self) -> PitrBaseCaptureState {
        self.state
    }

    pub(crate) fn metadata(&self) -> Option<&PitrBaseMetadata> {
        self.metadata.as_ref()
    }

    pub(crate) fn manifest_state(&self) -> Option<&PitrState> {
        self.manifest_state.as_ref()
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
                segment_id: 8,
                wal_digest: [4; 32],
                seal_digest: [5; 32],
            },
            base_recorded_at: PersistedRecordedAt {
                secs: 10,
                nanos: 11,
            },
            time_anchor: PitrBaseTimeAnchor::Indexed {
                segment_id: 8,
                commit_ts: 7,
                recorded_at: PersistedRecordedAt {
                    secs: 10,
                    nanos: 11,
                },
                entry_digest: [6; 32],
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
            segment_id: 8,
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
        assert!(coordinator.compatibility_digest.is_none());
        assert!(coordinator.captured_commit_high_water.is_none());
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
        invalid = metadata();
        invalid.compatibility_digest = [0; 32];
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
        let mut compatibility_bound = PitrBaseCaptureCoordinator::default();
        compatibility_bound
            .bind_manifest_state_with_compatibility(manifest_state(), [9; 32])
            .unwrap();
        compatibility_bound.stop_admission(9).unwrap();
        assert!(compatibility_bound.capture(metadata()).is_err());
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

    #[test]
    fn observed_and_indexed_time_anchors_round_trip_and_bind() {
        let mut observed = metadata();
        observed.included_commit_ts = None;
        observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: None,
            observed_at: observed.base_recorded_at,
        };
        let encoded = serde_json::to_vec(&observed).unwrap();
        let decoded: PitrBaseMetadata = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, observed);

        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(manifest_state()).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut wrong_kind = metadata();
        wrong_kind.included_commit_ts = Some(7);
        wrong_kind.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: Some(7),
            observed_at: wrong_kind.base_recorded_at,
        };
        assert!(coordinator.capture(wrong_kind).is_err());
        let mut greater_observed = metadata();
        greater_observed.included_commit_ts = Some(8);
        greater_observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: Some(8),
            observed_at: greater_observed.base_recorded_at,
        };
        assert!(coordinator.capture(greater_observed).is_err());

        let mut lower_observed = metadata();
        lower_observed.included_commit_ts = Some(6);
        lower_observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: Some(6),
            observed_at: lower_observed.base_recorded_at,
        };
        assert!(coordinator.capture(lower_observed).is_err());
    }

    #[test]
    fn failed_compatibility_binding_is_atomic() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        assert!(
            coordinator
                .bind_manifest_state_with_compatibility(manifest_state(), [0; 32])
                .is_err()
        );
        assert_eq!(coordinator.state(), PitrBaseCaptureState::AdmissionOpen);
        assert!(coordinator.stop_admission(9).is_err());
    }

    #[test]
    fn observed_base_persists_clock_clamp_before_release() {
        let mut state = manifest_state();
        state.last_recorded_at = None;
        state.last_commit_anchor = None;
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(state).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut observed = metadata();
        observed.included_commit_ts = None;
        observed.base_recorded_at = PersistedRecordedAt { secs: 20, nanos: 0 };
        observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: None,
            observed_at: observed.base_recorded_at,
        };
        coordinator.capture(observed).unwrap();
        coordinator.publish().unwrap();
        assert!(coordinator.release_admission().is_err());
        let mut persisted = coordinator.manifest_state().unwrap().clone();
        persisted.last_recorded_at = Some(PersistedRecordedAt { secs: 20, nanos: 0 });
        coordinator
            .confirm_observed_clamp_persisted(persisted)
            .unwrap();
        assert_eq!(
            coordinator.manifest_state().unwrap().last_recorded_at,
            Some(PersistedRecordedAt { secs: 20, nanos: 0 })
        );
        coordinator.release_admission().unwrap();
    }

    #[test]
    fn observed_clamp_rejects_unrelated_manifest_changes() {
        let mut state = manifest_state();
        state.last_recorded_at = None;
        state.last_commit_anchor = None;
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(state).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut observed = metadata();
        observed.included_commit_ts = None;
        observed.base_recorded_at = PersistedRecordedAt { secs: 20, nanos: 0 };
        observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: None,
            observed_at: observed.base_recorded_at,
        };
        coordinator.capture(observed).unwrap();
        coordinator.publish().unwrap();
        let mut unrelated = coordinator.manifest_state().unwrap().clone();
        unrelated.last_recorded_at = Some(PersistedRecordedAt { secs: 20, nanos: 0 });
        unrelated.next_segment_id += 1;
        assert!(
            coordinator
                .confirm_observed_clamp_persisted(unrelated)
                .is_err()
        );
    }

    #[test]
    fn observed_clock_rollback_is_clamped_at_persisted_high_water() {
        let mut state = manifest_state();
        state.last_commit_anchor = None;
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(state).unwrap();
        coordinator
            .stop_admission_at(CapturedBaseBoundary::new(9, Some(6)))
            .unwrap();
        let mut observed = metadata();
        observed.included_commit_ts = Some(6);
        observed.base_recorded_at = PersistedRecordedAt {
            secs: 10,
            nanos: 11,
        };
        observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: Some(6),
            observed_at: PersistedRecordedAt { secs: 9, nanos: 0 },
        };
        coordinator.capture(observed).unwrap();
        coordinator.publish().unwrap();
        let persisted = coordinator.manifest_state().unwrap().clone();
        coordinator
            .confirm_observed_clamp_persisted(persisted)
            .unwrap();
        coordinator.release_admission().unwrap();
    }

    #[test]
    fn observed_capture_rejects_over_clamped_time() {
        let mut state = manifest_state();
        state.last_commit_anchor = None;
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(state).unwrap();
        coordinator
            .stop_admission_at(CapturedBaseBoundary::new(9, Some(6)))
            .unwrap();
        let mut observed = metadata();
        observed.included_commit_ts = Some(6);
        observed.base_recorded_at = PersistedRecordedAt { secs: 20, nanos: 0 };
        observed.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: Some(6),
            observed_at: PersistedRecordedAt { secs: 9, nanos: 0 },
        };
        assert!(coordinator.capture(observed).is_err());
    }

    #[test]
    fn indexed_base_survives_a_later_global_clock_clamp() {
        let mut state = manifest_state();
        state.last_recorded_at = Some(PersistedRecordedAt { secs: 20, nanos: 0 });
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(state).unwrap();
        coordinator.stop_admission(9).unwrap();
        let mut indexed = metadata();
        indexed.included_commit_ts = Some(7);
        coordinator.capture(indexed).unwrap();
    }

    #[test]
    fn observed_base_accepts_only_unindexed_commit_above_manifest_anchor() {
        let mut coordinator = PitrBaseCaptureCoordinator::default();
        coordinator.bind_manifest_state(manifest_state()).unwrap();
        coordinator
            .stop_admission_at(CapturedBaseBoundary::new(9, Some(8)))
            .unwrap();
        let mut newer = metadata();
        newer.included_commit_ts = Some(8);
        newer.base_recorded_at = PersistedRecordedAt { secs: 11, nanos: 0 };
        newer.time_anchor = PitrBaseTimeAnchor::Observed {
            commit_ts: Some(8),
            observed_at: newer.base_recorded_at,
        };
        coordinator.capture(newer).unwrap();

        for commit_ts in [6, 7] {
            let mut coordinator = PitrBaseCaptureCoordinator::default();
            coordinator.bind_manifest_state(manifest_state()).unwrap();
            coordinator
                .stop_admission_at(CapturedBaseBoundary::new(9, Some(commit_ts)))
                .unwrap();
            let mut invalid = metadata();
            invalid.included_commit_ts = Some(commit_ts);
            invalid.time_anchor = PitrBaseTimeAnchor::Observed {
                commit_ts: Some(commit_ts),
                observed_at: invalid.base_recorded_at,
            };
            assert!(coordinator.capture(invalid).is_err());
        }
    }
}
