//! Dormant persisted PITR manifest state and replay reducer.
//!
//! This module models v7 crash boundaries without participating in the live
//! v6 manifest. Later slices will embed these records in `ManifestRecord`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub(crate) const PITR_MANIFEST_FORMAT_VERSION: u32 = 7;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedPitrConfig {
    pub(crate) archive_interval_ms: u64,
    pub(crate) max_segment_bytes: u64,
    pub(crate) max_unarchived_bytes: u64,
    pub(crate) max_source_spool_bytes: u64,
}

impl PersistedPitrConfig {
    fn validate(&self) -> Result<()> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PersistedChainAnchor {
    Genesis {
        archive_epoch_id: [u8; 16],
    },
    Segment {
        segment_id: u64,
        wal_digest: [u8; 32],
        seal_digest: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub(crate) struct PersistedRecordedAt {
    pub(crate) secs: i64,
    pub(crate) nanos: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedCommitAnchor {
    pub(crate) segment_id: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at: PersistedRecordedAt,
    pub(crate) entry_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum CoverageBreakReason {
    RepositoryUnavailable,
    ArchiveFailure,
    ForcedDisable,
    PublicationUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedRecoveryGap {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) after: PersistedChainAnchor,
    pub(crate) last_archived_commit_ts: Option<u64>,
    pub(crate) first_uncovered_commit_ts: Option<u64>,
    pub(crate) reason: CoverageBreakReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PitrMode {
    Disabled,
    Enabling,
    Enabled,
    PublicationUncertain,
    ReconciliationRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum ObligationState {
    Sealing,
    Sealed,
    Archived,
    Reclaimable,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PitrObligation {
    pub(crate) state: ObligationState,
    pub(crate) successor_segment_id: u64,
    pub(crate) logical_length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PitrState {
    pub(crate) mode: PitrMode,
    pub(crate) repository_id: Option<[u8; 16]>,
    pub(crate) timeline_id: Option<[u8; 16]>,
    pub(crate) archive_epoch_id: Option<[u8; 16]>,
    pub(crate) config: Option<PersistedPitrConfig>,
    pub(crate) active_segment_id: Option<u64>,
    pub(crate) next_segment_id: u64,
    pub(crate) epoch_genesis_anchor: Option<PersistedChainAnchor>,
    pub(crate) predecessor_anchor: Option<PersistedChainAnchor>,
    pub(crate) last_recorded_at: Option<PersistedRecordedAt>,
    pub(crate) last_commit_anchor: Option<PersistedCommitAnchor>,
    pub(crate) obligations: BTreeMap<u64, PitrObligation>,
    pub(crate) uncertain_segment_id: Option<u64>,
    pub(crate) recovery_gap: Option<PersistedRecoveryGap>,
}

impl Default for PitrState {
    fn default() -> Self {
        Self {
            mode: PitrMode::Disabled,
            repository_id: None,
            timeline_id: None,
            archive_epoch_id: None,
            config: None,
            active_segment_id: None,
            next_segment_id: 0,
            epoch_genesis_anchor: None,
            predecessor_anchor: None,
            last_recorded_at: None,
            last_commit_anchor: None,
            obligations: BTreeMap::new(),
            uncertain_segment_id: None,
            recovery_gap: None,
        }
    }
}

impl PitrState {
    fn validate(&self) -> Result<()> {
        if self.mode == PitrMode::Disabled {
            ensure!(
                self == &Self::default(),
                "disabled PITR state is not canonical"
            );
            return Ok(());
        }
        ensure!(
            self.repository_id.is_some_and(|id| id != [0; 16]),
            "PITR state is missing repository identity"
        );
        ensure!(
            self.timeline_id.is_some_and(|id| id != [0; 16]),
            "PITR state is missing timeline identity"
        );
        ensure!(
            self.archive_epoch_id.is_some_and(|id| id != [0; 16]),
            "PITR state is missing archive epoch identity"
        );
        self.config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PITR state is missing config"))?
            .validate()?;
        ensure!(
            matches!(self.epoch_genesis_anchor, Some(PersistedChainAnchor::Genesis { archive_epoch_id }) if Some(archive_epoch_id) == self.archive_epoch_id),
            "PITR epoch genesis anchor is invalid"
        );

        if self.mode == PitrMode::Enabling {
            ensure!(
                self.active_segment_id.is_none() && self.obligations.is_empty(),
                "enabling PITR state has live segments"
            );
            ensure!(
                self.predecessor_anchor == self.epoch_genesis_anchor,
                "enabling PITR predecessor is not Genesis"
            );
            ensure!(
                self.next_segment_id == 0,
                "enabling PITR state has segment high-water"
            );
            ensure!(
                self.last_recorded_at.is_none() && self.last_commit_anchor.is_none(),
                "enabling PITR state has commit high-water"
            );
            ensure!(
                self.uncertain_segment_id.is_none() && self.recovery_gap.is_none(),
                "enabling PITR state has terminal metadata"
            );
        } else {
            let active = self
                .active_segment_id
                .ok_or_else(|| anyhow::anyhow!("active PITR epoch is missing active segment"))?;
            ensure!(
                active < self.next_segment_id,
                "active segment exceeds segment high-water"
            );
            ensure!(
                self.predecessor_anchor.is_some(),
                "active PITR epoch is missing predecessor anchor"
            );
        }
        ensure!(
            self.mode == PitrMode::PublicationUncertain || self.uncertain_segment_id.is_none(),
            "non-uncertain PITR state retains uncertainty"
        );
        if self.mode == PitrMode::PublicationUncertain {
            let segment = self
                .uncertain_segment_id
                .ok_or_else(|| anyhow::anyhow!("publication uncertainty is missing segment"))?;
            ensure!(
                self.obligations
                    .get(&segment)
                    .is_some_and(|value| value.state == ObligationState::Sealed),
                "uncertain segment is not sealed"
            );
        }
        ensure!(
            self.mode == PitrMode::ReconciliationRequired || self.recovery_gap.is_none(),
            "non-reconciliation PITR state retains gap"
        );
        if self.mode == PitrMode::ReconciliationRequired {
            let gap = self
                .recovery_gap
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("reconciliation state is missing gap"))?;
            ensure!(
                Some(gap.repository_id) == self.repository_id
                    && Some(gap.timeline_id) == self.timeline_id
                    && Some(gap.archive_epoch_id) == self.archive_epoch_id,
                "recovery gap identity mismatch"
            );
            ensure!(
                Some(gap.after) == self.predecessor_anchor,
                "recovery gap chain anchor mismatch"
            );
            if let (Some(archived), Some(uncovered)) =
                (gap.last_archived_commit_ts, gap.first_uncovered_commit_ts)
            {
                ensure!(
                    uncovered > archived,
                    "recovery gap commit range is inverted"
                );
            }
            if let (Some(archived), Some(anchor)) =
                (gap.last_archived_commit_ts, self.last_commit_anchor)
            {
                ensure!(
                    archived <= anchor.commit_ts,
                    "recovery gap exceeds commit high-water"
                );
            }
            ensure!(
                self.obligations.values().all(|obligation| matches!(
                    obligation.state,
                    ObligationState::Archived
                        | ObligationState::Reclaimable
                        | ObligationState::Abandoned
                )),
                "reconciliation state retains an unclassified archive obligation"
            );
        }
        let mut successors = HashSet::new();
        for (&segment, obligation) in &self.obligations {
            ensure!(
                segment != obligation.successor_segment_id,
                "obligation successor reuses segment ID"
            );
            ensure!(
                obligation.successor_segment_id < self.next_segment_id,
                "obligation successor exceeds segment high-water"
            );
            ensure!(
                obligation.successor_segment_id > segment,
                "obligation successor is not monotonic"
            );
            ensure!(
                successors.insert(obligation.successor_segment_id),
                "duplicate obligation successor"
            );
            ensure!(
                obligation.logical_length >= 4096 && obligation.logical_length.is_multiple_of(4096),
                "obligation logical length is invalid"
            );
        }
        if let Some(time) = self.last_recorded_at {
            ensure!(
                time.nanos < 1_000_000_000,
                "recorded-time high-water is invalid"
            );
        }
        if let Some(anchor) = self.last_commit_anchor {
            ensure!(
                anchor.commit_ts != 0 && anchor.recorded_at.nanos < 1_000_000_000,
                "commit anchor is invalid"
            );
            ensure!(
                self.last_recorded_at
                    .is_some_and(|time| anchor.recorded_at <= time),
                "commit anchor exceeds recorded-time high-water"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PitrManifestRecord {
    EnableIntent {
        repository_id: [u8; 16],
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
        config: PersistedPitrConfig,
    },
    EnableComplete {
        active_segment_id: u64,
    },
    SealStarted {
        segment_id: u64,
        successor_segment_id: u64,
        logical_length: u64,
    },
    SegmentSealed {
        segment_id: u64,
        segment_anchor: PersistedChainAnchor,
        last_recorded_at: Option<PersistedRecordedAt>,
        last_commit_anchor: Option<PersistedCommitAnchor>,
    },
    SegmentArchived {
        segment_id: u64,
    },
    ArchivePublicationUnknown {
        segment_id: u64,
    },
    ArchivePublicationResolved {
        segment_id: u64,
        durable: bool,
    },
    SegmentReclaimable {
        segment_id: u64,
    },
    SegmentReclaimed {
        segment_id: u64,
    },
    SegmentAbandoned {
        segment_id: u64,
    },
    DisableClean,
    CoverageGap(PersistedRecoveryGap),
    ReconciliationComplete,
    Snapshot(Box<PitrState>),
}

pub(crate) fn replay_pitr_records(
    records: impl IntoIterator<Item = PitrManifestRecord>,
) -> Result<PitrState> {
    let mut state = PitrState::default();
    for record in records {
        match record {
            PitrManifestRecord::EnableIntent {
                repository_id,
                timeline_id,
                archive_epoch_id,
                config,
            } => {
                ensure!(
                    state == PitrState::default(),
                    "enable intent overlaps existing PITR state"
                );
                config.validate()?;
                let genesis = PersistedChainAnchor::Genesis { archive_epoch_id };
                state = PitrState {
                    mode: PitrMode::Enabling,
                    repository_id: Some(repository_id),
                    timeline_id: Some(timeline_id),
                    archive_epoch_id: Some(archive_epoch_id),
                    config: Some(config),
                    epoch_genesis_anchor: Some(genesis),
                    predecessor_anchor: Some(genesis),
                    ..PitrState::default()
                };
            }
            PitrManifestRecord::EnableComplete { active_segment_id } => {
                ensure!(
                    state.mode == PitrMode::Enabling,
                    "enable completion has no intent"
                );
                state.next_segment_id = active_segment_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("active segment ID exhausted"))?;
                state.active_segment_id = Some(active_segment_id);
                state.mode = PitrMode::Enabled;
            }
            PitrManifestRecord::SealStarted {
                segment_id,
                successor_segment_id,
                logical_length,
            } => {
                ensure!(
                    state.mode == PitrMode::Enabled,
                    "seal started while PITR is not enabled"
                );
                ensure!(
                    state.active_segment_id == Some(segment_id),
                    "seal does not name active segment"
                );
                ensure!(
                    successor_segment_id >= state.next_segment_id,
                    "successor segment ID is not monotonic"
                );
                ensure!(
                    !state.obligations.contains_key(&segment_id),
                    "duplicate sealing obligation"
                );
                ensure!(
                    logical_length >= 4096 && logical_length.is_multiple_of(4096),
                    "sealing logical length is invalid"
                );
                state.next_segment_id = successor_segment_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("successor segment ID exhausted"))?;
                state.obligations.insert(
                    segment_id,
                    PitrObligation {
                        state: ObligationState::Sealing,
                        successor_segment_id,
                        logical_length,
                    },
                );
            }
            PitrManifestRecord::SegmentSealed {
                segment_id,
                segment_anchor,
                last_recorded_at,
                last_commit_anchor,
            } => {
                ensure!(
                    matches!(
                        state.mode,
                        PitrMode::Enabled | PitrMode::ReconciliationRequired
                    ),
                    "segment sealed while PITR is inactive"
                );
                let obligation = *state
                    .obligations
                    .get(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("sealed segment has no obligation"))?;
                ensure!(
                    obligation.state == ObligationState::Sealing,
                    "segment sealed out of order"
                );
                ensure!(
                    matches!(segment_anchor, PersistedChainAnchor::Segment { segment_id: id, .. } if id == segment_id),
                    "sealed anchor does not bind segment"
                );
                let successor = obligation.successor_segment_id;
                state.active_segment_id = Some(successor);
                state.predecessor_anchor = Some(segment_anchor);
                apply_high_water(&mut state, segment_id, last_recorded_at, last_commit_anchor)?;
                state.obligations.get_mut(&segment_id).unwrap().state = ObligationState::Sealed;
            }
            PitrManifestRecord::SegmentArchived { segment_id } => transition_obligation(
                &mut state,
                segment_id,
                ObligationState::Sealed,
                ObligationState::Archived,
            )?,
            PitrManifestRecord::ArchivePublicationUnknown { segment_id } => {
                ensure!(
                    state.mode == PitrMode::Enabled,
                    "publication uncertainty while PITR is inactive"
                );
                ensure!(
                    state
                        .obligations
                        .get(&segment_id)
                        .is_some_and(|value| value.state == ObligationState::Sealed),
                    "uncertain segment is not sealed"
                );
                state.uncertain_segment_id = Some(segment_id);
                state.mode = PitrMode::PublicationUncertain;
            }
            PitrManifestRecord::ArchivePublicationResolved {
                segment_id,
                durable,
            } => {
                ensure!(
                    state.mode == PitrMode::PublicationUncertain
                        && state.uncertain_segment_id == Some(segment_id),
                    "publication resolution does not match uncertainty"
                );
                if durable {
                    let obligation = state
                        .obligations
                        .get_mut(&segment_id)
                        .ok_or_else(|| anyhow::anyhow!("resolved segment has no obligation"))?;
                    ensure!(
                        obligation.state == ObligationState::Sealed,
                        "resolved segment is not sealed"
                    );
                    obligation.state = ObligationState::Archived;
                }
                state.uncertain_segment_id = None;
                state.mode = PitrMode::Enabled;
            }
            PitrManifestRecord::SegmentReclaimable { segment_id } => transition_obligation(
                &mut state,
                segment_id,
                ObligationState::Archived,
                ObligationState::Reclaimable,
            )?,
            PitrManifestRecord::SegmentReclaimed { segment_id } => {
                let obligation = state
                    .obligations
                    .remove(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("reclaimed segment has no obligation"))?;
                ensure!(
                    obligation.state == ObligationState::Reclaimable,
                    "segment reclaimed before reclaimable"
                );
            }
            PitrManifestRecord::SegmentAbandoned { segment_id } => {
                ensure!(
                    state.mode == PitrMode::ReconciliationRequired,
                    "segment abandoned outside forced-gap reconciliation"
                );
                let obligation = state
                    .obligations
                    .remove(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("abandoned segment has no obligation"))?;
                ensure!(
                    obligation.state == ObligationState::Abandoned,
                    "segment was not marked abandoned by coverage gap"
                );
            }
            PitrManifestRecord::DisableClean => {
                ensure!(
                    state.mode == PitrMode::Enabled && state.obligations.is_empty(),
                    "clean disable has outstanding work"
                );
                state = PitrState::default();
            }
            PitrManifestRecord::CoverageGap(gap) => {
                ensure!(
                    matches!(
                        state.mode,
                        PitrMode::Enabled | PitrMode::PublicationUncertain
                    ),
                    "coverage gap while PITR is inactive"
                );
                ensure!(
                    Some(gap.repository_id) == state.repository_id
                        && Some(gap.timeline_id) == state.timeline_id
                        && Some(gap.archive_epoch_id) == state.archive_epoch_id
                        && Some(gap.after) == state.predecessor_anchor,
                    "coverage gap does not bind current epoch"
                );
                state.recovery_gap = Some(gap);
                state.uncertain_segment_id = None;
                for obligation in state.obligations.values_mut() {
                    if matches!(
                        obligation.state,
                        ObligationState::Sealing | ObligationState::Sealed
                    ) {
                        obligation.state = ObligationState::Abandoned;
                    }
                }
                state.mode = PitrMode::ReconciliationRequired;
            }
            PitrManifestRecord::ReconciliationComplete => {
                ensure!(
                    state.mode == PitrMode::ReconciliationRequired && state.obligations.is_empty(),
                    "reconciliation has outstanding work"
                );
                state = PitrState::default();
            }
            PitrManifestRecord::Snapshot(snapshot) => {
                snapshot.validate()?;
                state = *snapshot;
            }
        }
        state.validate()?;
    }
    Ok(state)
}

fn transition_obligation(
    state: &mut PitrState,
    segment_id: u64,
    from: ObligationState,
    to: ObligationState,
) -> Result<()> {
    ensure!(
        matches!(
            state.mode,
            PitrMode::Enabled | PitrMode::ReconciliationRequired
        ),
        "obligation transition while PITR is inactive"
    );
    let obligation = state
        .obligations
        .get_mut(&segment_id)
        .ok_or_else(|| anyhow::anyhow!("segment has no obligation"))?;
    ensure!(
        obligation.state == from,
        "segment obligation transitioned out of order"
    );
    obligation.state = to;
    Ok(())
}

fn apply_high_water(
    state: &mut PitrState,
    segment_id: u64,
    recorded_at: Option<PersistedRecordedAt>,
    anchor: Option<PersistedCommitAnchor>,
) -> Result<()> {
    if let Some(time) = recorded_at {
        ensure!(
            time.nanos < 1_000_000_000,
            "recorded-time high-water is invalid"
        );
        ensure!(
            state.last_recorded_at.is_none_or(|old| time >= old),
            "recorded-time high-water regressed"
        );
        state.last_recorded_at = Some(time);
    }
    if let Some(new_anchor) = anchor {
        ensure!(
            new_anchor.segment_id == segment_id,
            "commit anchor does not bind sealed segment"
        );
        ensure!(
            recorded_at == Some(new_anchor.recorded_at),
            "commit anchor time does not match segment high-water"
        );
        if let Some(old) = state.last_commit_anchor {
            ensure!(
                new_anchor.commit_ts > old.commit_ts,
                "commit anchor did not advance"
            );
            ensure!(
                new_anchor.recorded_at >= old.recorded_at,
                "commit anchor time regressed"
            );
        }
        state.last_commit_anchor = Some(new_anchor);
    } else {
        ensure!(
            recorded_at.is_none(),
            "empty segment cannot advance recorded-time high-water"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PersistedPitrConfig {
        PersistedPitrConfig {
            archive_interval_ms: 1000,
            max_segment_bytes: 4096,
            max_unarchived_bytes: 8192,
            max_source_spool_bytes: 16384,
        }
    }
    fn enable() -> Vec<PitrManifestRecord> {
        vec![
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: config(),
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 1,
            },
        ]
    }
    fn anchor(segment_id: u64) -> PersistedChainAnchor {
        PersistedChainAnchor::Segment {
            segment_id,
            wal_digest: [4; 32],
            seal_digest: [5; 32],
        }
    }

    #[test]
    fn sealing_intent_does_not_install_successor_until_sealed() {
        let mut records = enable();
        records.push(PitrManifestRecord::SealStarted {
            segment_id: 1,
            successor_segment_id: 2,
            logical_length: 4096,
        });
        let sealing = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(sealing.active_segment_id, Some(1));
        records.push(PitrManifestRecord::SegmentSealed {
            segment_id: 1,
            segment_anchor: anchor(1),
            last_recorded_at: None,
            last_commit_anchor: None,
        });
        let sealed = replay_pitr_records(records).unwrap();
        assert_eq!(sealed.active_segment_id, Some(2));
        assert_eq!(sealed.predecessor_anchor, Some(anchor(1)));
    }

    #[test]
    fn publication_uncertainty_is_not_a_coverage_gap() {
        let mut records = enable();
        records.extend([
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: 4096,
            },
            PitrManifestRecord::SegmentSealed {
                segment_id: 1,
                segment_anchor: anchor(1),
                last_recorded_at: None,
                last_commit_anchor: None,
            },
            PitrManifestRecord::ArchivePublicationUnknown { segment_id: 1 },
        ]);
        let uncertain = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(uncertain.mode, PitrMode::PublicationUncertain);
        assert!(uncertain.recovery_gap.is_none());
        let mut forced_gap = records.clone();
        forced_gap.push(PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            after: anchor(1),
            last_archived_commit_ts: None,
            first_uncovered_commit_ts: None,
            reason: CoverageBreakReason::PublicationUnknown,
        }));
        let gap_state = replay_pitr_records(forced_gap).unwrap();
        assert_eq!(gap_state.mode, PitrMode::ReconciliationRequired);
        assert!(gap_state.uncertain_segment_id.is_none());
        assert_eq!(gap_state.obligations[&1].state, ObligationState::Abandoned);
        records.push(PitrManifestRecord::ArchivePublicationResolved {
            segment_id: 1,
            durable: true,
        });
        assert_eq!(
            replay_pitr_records(records).unwrap().mode,
            PitrMode::Enabled
        );
    }

    #[test]
    fn forced_gap_requires_chain_identity_and_new_epoch_after_reconciliation() {
        let mut records = enable();
        records.push(PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            after: PersistedChainAnchor::Genesis {
                archive_epoch_id: [3; 16],
            },
            last_archived_commit_ts: None,
            first_uncovered_commit_ts: None,
            reason: CoverageBreakReason::ForcedDisable,
        }));
        records.push(PitrManifestRecord::ReconciliationComplete);
        assert_eq!(replay_pitr_records(records).unwrap(), PitrState::default());
    }

    #[test]
    fn snapshot_and_transition_validation_fail_closed() {
        let enabled = replay_pitr_records(enable()).unwrap();
        let bytes = serde_json::to_vec(&enabled).unwrap();
        let decoded: PitrState = serde_json::from_slice(&bytes).unwrap();
        decoded.validate().unwrap();
        assert!(
            replay_pitr_records([PitrManifestRecord::SegmentArchived { segment_id: 1 }]).is_err()
        );
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(PitrState {
                mode: PitrMode::Enabled,
                ..PitrState::default()
            }))])
            .is_err()
        );
    }
}
