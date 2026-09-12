//! Dormant PITR enable-transition harness over manifest v7 state.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use rand::{RngCore, rngs::OsRng};

use crate::pitr_manifest::{
    PersistedPitrConfig, PitrManifestRecord, PitrMode, PitrState, replay_pitr_records,
};

const MAX_IDENTITY_GENERATION_ATTEMPTS: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrEnableRequest {
    pub(crate) repository_id: [u8; 16],
    pub(crate) config: PersistedPitrConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BeginEnableOutcome {
    Started {
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
    },
    Resumed {
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
    },
    AlreadyEnabled {
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
    },
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

    pub(crate) fn begin_enable(
        &mut self,
        request: PitrEnableRequest,
    ) -> Result<BeginEnableOutcome> {
        self.begin_enable_with_rng(request, |identity| {
            OsRng
                .try_fill_bytes(identity)
                .map_err(|error| anyhow::anyhow!("PITR identity entropy unavailable: {error}"))
        })
    }

    fn begin_enable_with_rng(
        &mut self,
        request: PitrEnableRequest,
        mut fill_identity: impl FnMut(&mut [u8; 16]) -> Result<()>,
    ) -> Result<BeginEnableOutcome> {
        ensure!(
            request.repository_id != [0; 16],
            "PITR repository identity is empty"
        );
        request.config.validate_for_enable()?;
        if matches!(self.state.mode, PitrMode::Enabling | PitrMode::Enabled) {
            ensure!(
                self.state.repository_id == Some(request.repository_id)
                    && self.state.config.as_ref() == Some(&request.config),
                "PITR enable retry does not match persisted intent"
            );
            let identity = (
                self.state.timeline_id.unwrap(),
                self.state.archive_epoch_id.unwrap(),
            );
            return Ok(if self.state.mode == PitrMode::Enabling {
                BeginEnableOutcome::Resumed {
                    timeline_id: identity.0,
                    archive_epoch_id: identity.1,
                }
            } else {
                BeginEnableOutcome::AlreadyEnabled {
                    timeline_id: identity.0,
                    archive_epoch_id: identity.1,
                }
            });
        }
        ensure!(
            self.state.mode == PitrMode::Disabled,
            "PITR enable overlaps existing state"
        );
        let timeline_id = match self.state.database_timeline_id {
            Some(timeline_id) => timeline_id,
            None => next_identity(&mut fill_identity, None)?,
        };
        let archive_epoch_id = next_identity(&mut fill_identity, self.state.archive_epoch_id)?;
        self.begin_enable_with_identities(request, timeline_id, archive_epoch_id)?;
        Ok(BeginEnableOutcome::Started {
            timeline_id,
            archive_epoch_id,
        })
    }

    fn begin_enable_with_identities(
        &mut self,
        request: PitrEnableRequest,
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
    ) -> Result<()> {
        ensure!(
            self.state.mode == PitrMode::Disabled,
            "PITR enable overlaps existing state"
        );
        ensure!(
            request.repository_id != [0; 16],
            "PITR repository identity is empty"
        );
        ensure!(timeline_id != [0; 16], "PITR timeline identity is empty");
        ensure!(
            archive_epoch_id != [0; 16],
            "PITR archive epoch identity is empty"
        );
        request.config.validate_for_enable()?;
        let record = PitrManifestRecord::EnableIntent {
            repository_id: request.repository_id,
            timeline_id,
            archive_epoch_id,
            config: request.config,
        };
        let mut records = self.records.clone();
        records.push(record);
        self.state = replay_pitr_records(records.clone())?;
        self.records = records;
        Ok(())
    }

    #[cfg(test)]
    fn begin_enable_for_test(
        &mut self,
        request: PitrEnableRequest,
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
    ) -> Result<BeginEnableOutcome> {
        self.begin_enable_with_identities(request, timeline_id, archive_epoch_id)?;
        Ok(BeginEnableOutcome::Started {
            timeline_id,
            archive_epoch_id,
        })
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

fn next_identity(
    fill_identity: &mut impl FnMut(&mut [u8; 16]) -> Result<()>,
    reject: Option<[u8; 16]>,
) -> Result<[u8; 16]> {
    for _ in 0..MAX_IDENTITY_GENERATION_ATTEMPTS {
        let mut identity = [0; 16];
        fill_identity(&mut identity)?;
        if identity != [0; 16] && Some(identity) != reject {
            return Ok(identity);
        }
    }
    anyhow::bail!("PITR identity generation exhausted redraw attempts")
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
            config: PersistedPitrConfig {
                archive_interval_ms: 1000,
                max_segment_bytes: 8192,
                max_unarchived_bytes: 16384,
                max_source_spool_bytes: 32768,
            },
        }
    }

    fn begin(
        coordinator: &mut PitrEnableCoordinator,
        archive_epoch_id: [u8; 16],
    ) -> Result<BeginEnableOutcome> {
        coordinator.begin_enable_for_test(request(), [2; 16], archive_epoch_id)
    }

    #[test]
    fn enable_intent_and_completion_recover_deterministically() {
        let mut coordinator = PitrEnableCoordinator::default();
        begin(&mut coordinator, [3; 16]).unwrap();
        assert_eq!(coordinator.state().mode, PitrMode::Enabling);
        coordinator.complete_enable(7).unwrap();
        assert_eq!(coordinator.state().mode, PitrMode::Enabled);
        let recovered = PitrEnableCoordinator::recover(coordinator.records().to_vec()).unwrap();
        assert_eq!(recovered.state(), coordinator.state());
    }

    #[test]
    fn enable_generates_identities_and_recovery_reuses_the_intent() {
        let mut coordinator = PitrEnableCoordinator::default();
        coordinator.begin_enable(request()).unwrap();
        let enabling = coordinator.state().clone();
        assert!(
            enabling
                .database_timeline_id
                .is_some_and(|id| id != [0; 16])
        );
        assert!(enabling.archive_epoch_id.is_some_and(|id| id != [0; 16]));

        let records = coordinator.records().to_vec();
        let mut recovered = PitrEnableCoordinator::recover(records.clone()).unwrap();
        assert_eq!(recovered.state(), &enabling);
        assert_eq!(
            recovered
                .begin_enable_with_rng(request(), |_| panic!("resume requested entropy"))
                .unwrap(),
            BeginEnableOutcome::Resumed {
                timeline_id: enabling.timeline_id.unwrap(),
                archive_epoch_id: enabling.archive_epoch_id.unwrap(),
            }
        );
        let mut mismatched = request();
        mismatched.config.archive_interval_ms += 1;
        assert!(
            recovered
                .begin_enable_with_rng(mismatched, |_| panic!("mismatch requested entropy"))
                .is_err()
        );
        assert_eq!(recovered.records(), records);
        recovered.complete_enable(7).unwrap();
        assert_eq!(
            recovered.state().archive_epoch_id,
            enabling.archive_epoch_id
        );
    }

    #[test]
    fn identity_generation_propagates_entropy_failure() {
        let mut coordinator = PitrEnableCoordinator::default();
        assert!(
            coordinator
                .begin_enable_with_rng(request(), |_| anyhow::bail!("entropy failure"))
                .is_err()
        );
        assert_eq!(coordinator.state(), &PitrState::default());
        assert!(coordinator.records().is_empty());
    }

    #[test]
    fn identity_generation_bounds_zero_redraws() {
        let mut coordinator = PitrEnableCoordinator::default();
        assert!(
            coordinator
                .begin_enable_with_rng(request(), |output| {
                    *output = [0; 16];
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(coordinator.state(), &PitrState::default());
        assert!(coordinator.records().is_empty());
    }

    #[test]
    fn identity_generation_bounds_last_epoch_collisions() {
        let mut coordinator = PitrEnableCoordinator::default();
        begin(&mut coordinator, [3; 16]).unwrap();
        coordinator.complete_enable(7).unwrap();
        let mut records = coordinator.records().to_vec();
        records.push(PitrManifestRecord::DisableClean);
        let mut coordinator = PitrEnableCoordinator::recover(records.clone()).unwrap();
        let state = coordinator.state().clone();
        assert!(
            coordinator
                .begin_enable_with_rng(request(), |output| {
                    *output = [3; 16];
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(coordinator.state(), &state);
        assert_eq!(coordinator.records(), records);
    }

    #[test]
    fn completed_enable_retry_reports_existing_identity() {
        let mut coordinator = PitrEnableCoordinator::default();
        begin(&mut coordinator, [3; 16]).unwrap();
        coordinator.complete_enable(7).unwrap();
        let records = coordinator.records().to_vec();
        let mut recovered = PitrEnableCoordinator::recover(records.clone()).unwrap();
        assert_eq!(
            recovered
                .begin_enable_with_rng(request(), |_| panic!("enabled retry requested entropy"))
                .unwrap(),
            BeginEnableOutcome::AlreadyEnabled {
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
            }
        );
        let mut mismatched = request();
        mismatched.repository_id = [9; 16];
        assert!(
            recovered
                .begin_enable_with_rng(mismatched, |_| panic!("mismatch requested entropy"))
                .is_err()
        );
        assert_eq!(recovered.records(), records);
    }

    #[test]
    fn identity_generation_redraws_zero_and_last_epoch() {
        let mut coordinator = PitrEnableCoordinator::default();
        let mut identities = [[0; 16], [2; 16], [0; 16], [3; 16]].into_iter();
        let outcome = coordinator
            .begin_enable_with_rng(request(), |output| {
                *output = identities.next().unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(
            outcome,
            BeginEnableOutcome::Started {
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
            }
        );
        coordinator.complete_enable(7).unwrap();
        let mut records = coordinator.records().to_vec();
        records.push(PitrManifestRecord::DisableClean);
        let mut coordinator = PitrEnableCoordinator::recover(records).unwrap();
        let mut identities = [[3; 16], [4; 16]].into_iter();
        let outcome = coordinator
            .begin_enable_with_rng(request(), |output| {
                *output = identities.next().unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(
            outcome,
            BeginEnableOutcome::Started {
                timeline_id: [2; 16],
                archive_epoch_id: [4; 16],
            }
        );
    }

    #[test]
    fn recovery_only_timeline_can_enable_without_source_identity() {
        let recovered_state = PitrState {
            database_timeline_id: Some([8; 16]),
            ..PitrState::default()
        };
        let mut coordinator = PitrEnableCoordinator::recover(vec![PitrManifestRecord::Snapshot(
            Box::new(recovered_state),
        )])
        .unwrap();
        coordinator
            .begin_enable_for_test(request(), [8; 16], [4; 16])
            .unwrap();
        assert_eq!(coordinator.state().timeline_id, Some([8; 16]));
    }

    #[test]
    fn enable_rejects_invalid_or_overlapping_requests() {
        let mut coordinator = PitrEnableCoordinator::default();
        let mut invalid = request();
        invalid.config.max_unarchived_bytes = 4096;
        assert!(coordinator.begin_enable(invalid).is_err());
        begin(&mut coordinator, [3; 16]).unwrap();
        assert!(begin(&mut coordinator, [4; 16]).is_err());
        coordinator.complete_enable(8).unwrap();
        assert!(coordinator.complete_enable(7).is_err());
    }

    #[test]
    fn reenable_preserves_timeline_and_requires_fresh_epoch() {
        let mut coordinator = PitrEnableCoordinator::default();
        begin(&mut coordinator, [3; 16]).unwrap();
        coordinator.complete_enable(7).unwrap();
        let mut records = coordinator.records().to_vec();
        records.push(PitrManifestRecord::DisableClean);

        let mut coordinator = PitrEnableCoordinator::recover(records).unwrap();
        assert!(
            coordinator
                .begin_enable_for_test(request(), [9; 16], [4; 16])
                .is_err()
        );
        assert!(begin(&mut coordinator, [3; 16]).is_err());
        begin(&mut coordinator, [4; 16]).unwrap();
    }

    #[test]
    fn forced_gap_reenable_requires_fresh_epoch() {
        let mut coordinator = PitrEnableCoordinator::default();
        begin(&mut coordinator, [3; 16]).unwrap();
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
        assert!(begin(&mut coordinator, [3; 16]).is_err());
        begin(&mut coordinator, [4; 16]).unwrap();
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

        assert!(
            coordinator
                .begin_enable_for_test(request(), [9; 16], [4; 16])
                .is_err()
        );
        assert!(begin(&mut coordinator, [3; 16]).is_err());
        begin(&mut coordinator, [4; 16]).unwrap();
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

        assert!(begin(&mut coordinator, [3; 16]).is_err());
        assert!(
            coordinator
                .begin_enable_for_test(request(), [9; 16], [4; 16])
                .is_err()
        );
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
        records.extend([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [4; 16],
                config: request().config,
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

        assert!(begin(&mut coordinator, [4; 16]).is_err());
        begin(&mut coordinator, [5; 16]).unwrap();
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

        assert!(begin(&mut coordinator, [3; 16]).is_err());
        begin(&mut coordinator, [4; 16]).unwrap();
    }

    #[test]
    fn recovery_rejects_snapshot_with_changed_timeline() {
        let mut coordinator = PitrEnableCoordinator::default();
        begin(&mut coordinator, [3; 16]).unwrap();
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
        begin(&mut coordinator, [3; 16]).unwrap();
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
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
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
