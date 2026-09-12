//! Dormant PITR enable-transition harness over manifest v7 state.
#![allow(dead_code)]

use anyhow::{Result, ensure};

use crate::pitr_manifest::{
    PersistedPitrConfig, PitrManifestRecord, PitrMode, PitrState, replay_pitr_records,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrEnableRequest {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) config: PersistedPitrConfig,
}

#[derive(Debug, Default)]
pub(crate) struct PitrEnableCoordinator {
    state: PitrState,
    records: Vec<PitrManifestRecord>,
}

impl PitrEnableCoordinator {
    pub(crate) fn recover(records: Vec<PitrManifestRecord>) -> Result<Self> {
        let state = replay_pitr_records(records.clone())?;
        Ok(Self { state, records })
    }

    pub(crate) fn begin_enable(&mut self, request: PitrEnableRequest) -> Result<()> {
        ensure!(
            self.state.mode == PitrMode::Disabled,
            "PITR enable overlaps existing state"
        );
        ensure!(
            request.repository_id != [0; 16],
            "PITR repository identity is empty"
        );
        ensure!(
            request.timeline_id != [0; 16],
            "PITR timeline identity is empty"
        );
        ensure!(
            request.archive_epoch_id != [0; 16],
            "PITR archive epoch identity is empty"
        );
        request.config.validate_for_enable()?;
        let record = PitrManifestRecord::EnableIntent {
            repository_id: request.repository_id,
            timeline_id: request.timeline_id,
            archive_epoch_id: request.archive_epoch_id,
            config: request.config,
        };
        let mut records = self.records.clone();
        records.push(record);
        self.state = replay_pitr_records(records.clone())?;
        self.records = records;
        Ok(())
    }

    pub(crate) fn complete_enable(&mut self, active_segment_id: u64) -> Result<()> {
        ensure!(
            self.state.mode == PitrMode::Enabling,
            "PITR enable completion has no intent"
        );
        let record = PitrManifestRecord::EnableComplete { active_segment_id };
        let mut records = self.records.clone();
        records.push(record.clone());
        self.state = replay_pitr_records(records.clone())?;
        self.records = records;
        Ok(())
    }

    pub(crate) fn state(&self) -> &PitrState {
        &self.state
    }

    pub(crate) fn records(&self) -> &[PitrManifestRecord] {
        &self.records
    }
}

trait EnableConfigValidation {
    fn validate_for_enable(&self) -> Result<()>;
}

impl EnableConfigValidation for PersistedPitrConfig {
    fn validate_for_enable(&self) -> Result<()> {
        ensure!(
            self.archive_interval_ms > 0,
            "PITR archive interval must be nonzero"
        );
        ensure!(
            self.max_segment_bytes > 0,
            "PITR segment limit must be nonzero"
        );
        ensure!(
            self.max_unarchived_bytes >= self.max_segment_bytes,
            "PITR unarchived limit is below segment limit"
        );
        ensure!(
            self.max_source_spool_bytes >= self.max_unarchived_bytes,
            "PITR source spool limit is below unarchived limit"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pitr_manifest::{CoverageBreakReason, PersistedChainAnchor, PersistedRecoveryGap};

    fn request() -> PitrEnableRequest {
        PitrEnableRequest {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            config: PersistedPitrConfig {
                archive_interval_ms: 1000,
                max_segment_bytes: 8192,
                max_unarchived_bytes: 16384,
                max_source_spool_bytes: 32768,
            },
        }
    }

    #[test]
    fn enable_intent_and_completion_recover_deterministically() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        assert_eq!(coordinator.state().mode, PitrMode::Enabling);
        coordinator.complete_enable(7).unwrap();
        assert_eq!(coordinator.state().mode, PitrMode::Enabled);
        let recovered = PitrEnableCoordinator::recover(coordinator.records().to_vec()).unwrap();
        assert_eq!(recovered.state(), coordinator.state());
    }

    #[test]
    fn enable_rejects_invalid_or_overlapping_requests() {
        let mut coordinator = PitrEnableCoordinator::default();
        let mut invalid = request();
        invalid.config.max_unarchived_bytes = 4096;
        assert!(coordinator.begin_enable(invalid).is_err());
        coordinator.begin_enable(request()).unwrap();
        assert!(coordinator.begin_enable(request()).is_err());
        coordinator.complete_enable(8).unwrap();
        assert!(coordinator.complete_enable(7).is_err());
    }

    #[test]
    fn reenable_preserves_timeline_and_requires_fresh_epoch() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        coordinator.complete_enable(7).unwrap();
        let mut records = coordinator.records().to_vec();
        records.push(PitrManifestRecord::DisableClean);

        let mut coordinator = PitrEnableCoordinator::recover(records).unwrap();
        let mut changed_timeline = request();
        changed_timeline.timeline_id = [9; 16];
        changed_timeline.archive_epoch_id = [4; 16];
        assert!(coordinator.begin_enable(changed_timeline).is_err());
        assert!(coordinator.begin_enable(request()).is_err());
        let mut fresh = request();
        fresh.archive_epoch_id = [4; 16];
        coordinator.begin_enable(fresh).unwrap();
    }

    #[test]
    fn forced_gap_reenable_requires_fresh_epoch() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        coordinator.complete_enable(7).unwrap();
        let mut records = coordinator.records().to_vec();
        records.extend([
            PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                after: PersistedChainAnchor::Genesis {
                    archive_epoch_id: [3; 16],
                },
                last_archived_commit_ts: None,
                first_uncovered_commit_ts: None,
                reason: CoverageBreakReason::ForcedDisable,
            }),
            PitrManifestRecord::ReconciliationComplete,
        ]);

        let mut coordinator = PitrEnableCoordinator::recover(records).unwrap();
        assert!(coordinator.begin_enable(request()).is_err());
        let mut fresh = request();
        fresh.archive_epoch_id = [4; 16];
        coordinator.begin_enable(fresh).unwrap();
    }

    #[test]
    fn compacted_snapshot_preserves_lifecycle_identities_after_disable() {
        let enabled = replay_pitr_records([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: request().config,
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 7,
            },
        ])
        .unwrap();
        let mut coordinator = PitrEnableCoordinator::recover(vec![
            PitrManifestRecord::Snapshot(Box::new(enabled)),
            PitrManifestRecord::DisableClean,
        ])
        .unwrap();

        let mut changed_timeline = request();
        changed_timeline.timeline_id = [9; 16];
        changed_timeline.archive_epoch_id = [4; 16];
        assert!(coordinator.begin_enable(changed_timeline).is_err());
        assert!(coordinator.begin_enable(request()).is_err());
        let mut fresh = request();
        fresh.archive_epoch_id = [4; 16];
        coordinator.begin_enable(fresh).unwrap();
    }

    #[test]
    fn disabled_snapshot_preserves_lifecycle_identities() {
        let disabled = replay_pitr_records([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: request().config,
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 7,
            },
            PitrManifestRecord::DisableClean,
        ])
        .unwrap();
        assert_eq!(disabled.database_timeline_id, Some([2; 16]));
        assert_eq!(disabled.archive_epoch_id, Some([3; 16]));
        let mut coordinator =
            PitrEnableCoordinator::recover(vec![PitrManifestRecord::Snapshot(Box::new(disabled))])
                .unwrap();

        assert!(coordinator.begin_enable(request()).is_err());
        let mut changed_timeline = request();
        changed_timeline.timeline_id = [9; 16];
        changed_timeline.archive_epoch_id = [4; 16];
        assert!(coordinator.begin_enable(changed_timeline).is_err());
    }

    #[test]
    fn compacted_snapshot_retains_last_epoch_without_lifetime_cap() {
        let mut records = vec![
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: request().config,
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 7,
            },
            PitrManifestRecord::DisableClean,
        ];
        let mut second = request();
        second.archive_epoch_id = [4; 16];
        records.extend([
            PitrManifestRecord::EnableIntent {
                repository_id: second.repository_id,
                timeline_id: second.timeline_id,
                archive_epoch_id: second.archive_epoch_id,
                config: second.config,
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 9,
            },
        ]);
        let second_epoch = replay_pitr_records(records).unwrap();
        assert_eq!(second_epoch.archive_epoch_id, Some([4; 16]));
        let mut coordinator = PitrEnableCoordinator::recover(vec![
            PitrManifestRecord::Snapshot(Box::new(second_epoch)),
            PitrManifestRecord::DisableClean,
        ])
        .unwrap();

        let mut reused_second = request();
        reused_second.archive_epoch_id = [4; 16];
        assert!(coordinator.begin_enable(reused_second).is_err());
        let mut fresh = request();
        fresh.archive_epoch_id = [5; 16];
        coordinator.begin_enable(fresh).unwrap();
    }

    #[test]
    fn compacted_snapshot_preserves_identities_after_forced_gap() {
        let enabled = replay_pitr_records([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: request().config,
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 7,
            },
        ])
        .unwrap();
        let mut coordinator = PitrEnableCoordinator::recover(vec![
            PitrManifestRecord::Snapshot(Box::new(enabled)),
            PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                after: PersistedChainAnchor::Genesis {
                    archive_epoch_id: [3; 16],
                },
                last_archived_commit_ts: None,
                first_uncovered_commit_ts: None,
                reason: CoverageBreakReason::ForcedDisable,
            }),
            PitrManifestRecord::ReconciliationComplete,
        ])
        .unwrap();

        assert!(coordinator.begin_enable(request()).is_err());
        let mut fresh = request();
        fresh.archive_epoch_id = [4; 16];
        coordinator.begin_enable(fresh).unwrap();
    }

    #[test]
    fn recovery_rejects_snapshot_with_changed_timeline() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        coordinator.complete_enable(7).unwrap();
        let mut changed = coordinator.state().clone();
        changed.database_timeline_id = Some([9; 16]);
        changed.timeline_id = Some([9; 16]);
        let mut records = coordinator.records().to_vec();
        records.push(PitrManifestRecord::Snapshot(Box::new(changed)));
        assert!(PitrEnableCoordinator::recover(records).is_err());
    }

    #[test]
    fn recovery_rejects_appended_snapshot_that_replaces_epoch() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        coordinator.complete_enable(7).unwrap();
        let mut changed = coordinator.state().clone();
        changed.archive_epoch_id = Some([4; 16]);
        changed.epoch_genesis_anchor = Some(PersistedChainAnchor::Genesis {
            archive_epoch_id: [4; 16],
        });
        changed.predecessor_anchor = changed.epoch_genesis_anchor;
        let mut records = coordinator.records().to_vec();
        records.push(PitrManifestRecord::Snapshot(Box::new(changed)));
        assert!(PitrEnableCoordinator::recover(records).is_err());
    }

    #[test]
    fn recovery_rejects_zero_identity_records_and_snapshots() {
        let mut zero_intent = request();
        zero_intent.repository_id = [0; 16];
        assert!(
            PitrEnableCoordinator::recover(vec![PitrManifestRecord::EnableIntent {
                repository_id: zero_intent.repository_id,
                timeline_id: zero_intent.timeline_id,
                archive_epoch_id: zero_intent.archive_epoch_id,
                config: zero_intent.config,
            }])
            .is_err()
        );

        let valid_state = replay_pitr_records([PitrManifestRecord::EnableIntent {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            config: request().config,
        }])
        .unwrap();
        let mut invalid_state = valid_state;
        invalid_state.timeline_id = Some([0; 16]);
        assert!(
            PitrEnableCoordinator::recover(vec![PitrManifestRecord::Snapshot(Box::new(
                invalid_state
            ))])
            .is_err()
        );
    }
}
