//! Dormant persisted PITR manifest state and replay reducer.
//!
//! This module does not participate in the live v6 manifest format. It is the
//! validated substrate used by the later v7 enablement and segment lifecycle
//! slices.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, bail, ensure};
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
    pub(crate) fn validate(&self) -> Result<()> {
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
pub(crate) struct PersistedPitrAnchor {
    pub(crate) segment_id: u64,
    pub(crate) commit_ts: u64,
    pub(crate) recorded_at_secs: i64,
    pub(crate) recorded_at_nanos: u32,
    pub(crate) entry_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PitrMode {
    Disabled,
    Enabling,
    Enabled,
    ReconciliationRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PitrObligationState {
    Sealing,
    Sealed,
    Archived,
    Reclaimable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PitrObligation {
    pub(crate) state: PitrObligationState,
    pub(crate) successor_segment_id: u64,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedRecoveryGap {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) predecessor: PersistedChainAnchor,
    pub(crate) last_archived_commit_ts: Option<u64>,
    pub(crate) first_uncovered_commit_ts: Option<u64>,
    pub(crate) reason: CoverageBreakReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum CoverageBreakReason {
    RepositoryUnavailable,
    ArchiveFailure,
    ForcedDisable,
    PublicationUnknown,
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
    pub(crate) last_commit_anchor: Option<PersistedPitrAnchor>,
    pub(crate) obligations: BTreeMap<u64, PitrObligation>,
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
            recovery_gap: None,
        }
    }
}

impl PitrState {
    pub(crate) fn validate(&self) -> Result<()> {
        match self.mode {
            PitrMode::Disabled => ensure!(
                self == &Self::default(),
                "disabled PITR state is not canonical"
            ),
            PitrMode::Enabling | PitrMode::Enabled | PitrMode::ReconciliationRequired => {
                ensure!(
                    self.repository_id.is_some(),
                    "PITR state is missing repository identity"
                );
                ensure!(
                    self.timeline_id.is_some(),
                    "PITR state is missing timeline identity"
                );
                ensure!(
                    self.archive_epoch_id.is_some(),
                    "PITR state is missing archive epoch identity"
                );
                self.config
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("PITR state is missing persisted config"))?
                    .validate()?;
            }
        }
        if self.mode == PitrMode::ReconciliationRequired {
            ensure!(
                self.recovery_gap.is_some(),
                "PITR reconciliation state is missing recovery gap"
            );
        }
        if self.mode == PitrMode::Enabling {
            ensure!(
                self.active_segment_id.is_none(),
                "enabling PITR state already has an active segment"
            );
            ensure!(
                self.obligations.is_empty(),
                "enabling PITR state has segment obligations"
            );
        }
        if self.mode == PitrMode::Enabled {
            ensure!(
                self.active_segment_id.is_some(),
                "enabled PITR state is missing active segment"
            );
            ensure!(
                self.epoch_genesis_anchor.is_some(),
                "enabled PITR state is missing epoch genesis anchor"
            );
            ensure!(
                self.predecessor_anchor.is_some(),
                "enabled PITR state is missing predecessor anchor"
            );
            ensure!(
                self.recovery_gap.is_none(),
                "enabled PITR state retains a recovery gap"
            );
        }
        if self.mode == PitrMode::ReconciliationRequired {
            ensure!(
                self.active_segment_id.is_some(),
                "reconciliation state is missing active segment"
            );
            ensure!(
                self.epoch_genesis_anchor.is_some(),
                "reconciliation state is missing epoch genesis anchor"
            );
            ensure!(
                self.predecessor_anchor.is_some(),
                "reconciliation state is missing predecessor anchor"
            );
        }
        if let Some(active) = self.active_segment_id {
            ensure!(
                !self.obligations.contains_key(&active),
                "active PITR segment has a sealed obligation"
            );
            ensure!(
                active < self.next_segment_id,
                "active PITR segment exceeds segment high-water"
            );
        }
        let mut successors = HashSet::new();
        for (&segment_id, obligation) in &self.obligations {
            ensure!(
                segment_id != obligation.successor_segment_id,
                "PITR obligation successor reuses segment ID"
            );
            ensure!(
                obligation.successor_segment_id < self.next_segment_id,
                "PITR obligation successor exceeds segment high-water"
            );
            ensure!(
                successors.insert(obligation.successor_segment_id),
                "duplicate PITR successor segment ID"
            );
        }
        if let Some(anchor) = self.last_commit_anchor {
            ensure!(
                anchor.commit_ts != 0,
                "PITR anchor commit timestamp must be nonzero"
            );
            ensure!(
                anchor.recorded_at_nanos < 1_000_000_000,
                "PITR anchor time is invalid"
            );
        }
        if let Some(recorded_at) = self.last_recorded_at {
            ensure!(
                recorded_at.nanos < 1_000_000_000,
                "PITR recorded-time high-water is invalid"
            );
        }
        if let Some(PersistedChainAnchor::Genesis { archive_epoch_id }) = self.epoch_genesis_anchor
        {
            ensure!(
                Some(archive_epoch_id) == self.archive_epoch_id,
                "PITR genesis anchor epoch mismatch"
            );
        }
        if let Some(gap) = &self.recovery_gap {
            ensure!(
                Some(gap.repository_id) == self.repository_id,
                "PITR recovery gap repository mismatch"
            );
            ensure!(
                Some(gap.timeline_id) == self.timeline_id,
                "PITR recovery gap timeline mismatch"
            );
            ensure!(
                Some(gap.archive_epoch_id) == self.archive_epoch_id,
                "PITR recovery gap epoch mismatch"
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
        predecessor_anchor: PersistedChainAnchor,
        last_recorded_at: Option<PersistedRecordedAt>,
        last_commit_anchor: Option<PersistedPitrAnchor>,
    },
    SegmentSealed {
        segment_id: u64,
    },
    SegmentArchived {
        segment_id: u64,
    },
    SegmentReclaimable {
        segment_id: u64,
    },
    SegmentReclaimed {
        segment_id: u64,
    },
    DisableClean,
    ReconciliationComplete,
    CoverageGap(PersistedRecoveryGap),
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
                    matches!(state.mode, PitrMode::Disabled),
                    "PITR enable intent overlaps an active epoch"
                );
                config.validate()?;
                state.repository_id = Some(repository_id);
                state.timeline_id = Some(timeline_id);
                state.archive_epoch_id = Some(archive_epoch_id);
                state.config = Some(config);
                state.mode = PitrMode::Enabling;
                state.epoch_genesis_anchor =
                    Some(PersistedChainAnchor::Genesis { archive_epoch_id });
                state.predecessor_anchor = state.epoch_genesis_anchor;
            }
            PitrManifestRecord::EnableComplete { active_segment_id } => {
                ensure!(
                    state.mode == PitrMode::Enabling,
                    "PITR enable completion has no pending intent"
                );
                ensure!(
                    state.repository_id.is_some(),
                    "PITR enable completion has no intent"
                );
                ensure!(
                    state.active_segment_id.is_none(),
                    "PITR enable completion duplicates active segment"
                );
                state.active_segment_id = Some(active_segment_id);
                state.next_segment_id = active_segment_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("PITR active segment ID exhausted"))?;
                state.mode = PitrMode::Enabled;
            }
            PitrManifestRecord::SealStarted {
                segment_id,
                successor_segment_id,
                predecessor_anchor,
                last_recorded_at,
                last_commit_anchor,
            } => {
                ensure!(
                    state.mode == PitrMode::Enabled,
                    "PITR seal started while inactive"
                );
                ensure!(
                    state.active_segment_id == Some(segment_id),
                    "PITR seal does not name the active segment"
                );
                ensure!(
                    segment_id != successor_segment_id,
                    "PITR successor reuses active segment ID"
                );
                ensure!(
                    successor_segment_id >= state.next_segment_id,
                    "PITR segment ID is not monotonic"
                );
                match predecessor_anchor {
                    PersistedChainAnchor::Genesis { .. } => {
                        bail!("PITR seal cannot reset predecessor chain to Genesis")
                    }
                    PersistedChainAnchor::Segment {
                        segment_id: anchor_segment_id,
                        ..
                    } => {
                        ensure!(
                            anchor_segment_id == segment_id,
                            "PITR seal predecessor does not name sealed segment"
                        );
                    }
                }
                state.active_segment_id = Some(successor_segment_id);
                state.next_segment_id = successor_segment_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("PITR successor segment ID exhausted"))?;
                state.predecessor_anchor = Some(predecessor_anchor);
                if let Some(new_time) = last_recorded_at {
                    if let Some(old_time) = state.last_recorded_at {
                        ensure!(
                            new_time >= old_time,
                            "PITR recorded-time high-water regressed"
                        );
                    }
                    state.last_recorded_at = Some(new_time);
                }
                if let Some(new_anchor) = last_commit_anchor {
                    if let Some(old_anchor) = state.last_commit_anchor {
                        ensure!(
                            new_anchor.commit_ts >= old_anchor.commit_ts,
                            "PITR commit high-water regressed"
                        );
                        ensure!(
                            (new_anchor.recorded_at_secs, new_anchor.recorded_at_nanos)
                                >= (old_anchor.recorded_at_secs, old_anchor.recorded_at_nanos),
                            "PITR commit recorded-time high-water regressed"
                        );
                    }
                    state.last_commit_anchor = Some(new_anchor);
                    if let Some(recorded_at) = state.last_recorded_at {
                        ensure!(
                            (new_anchor.recorded_at_secs, new_anchor.recorded_at_nanos)
                                <= (recorded_at.secs, recorded_at.nanos),
                            "PITR commit anchor exceeds recorded-time high-water"
                        );
                    }
                }
                state.obligations.insert(
                    segment_id,
                    PitrObligation {
                        state: PitrObligationState::Sealing,
                        successor_segment_id,
                    },
                );
            }
            PitrManifestRecord::SegmentSealed { segment_id } => {
                ensure!(
                    matches!(
                        state.mode,
                        PitrMode::Enabled | PitrMode::ReconciliationRequired
                    ),
                    "PITR segment sealed while inactive"
                );
                let obligation = state
                    .obligations
                    .get_mut(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("PITR sealed segment has no obligation"))?;
                ensure!(
                    obligation.state == PitrObligationState::Sealing,
                    "PITR segment sealed out of order"
                );
                obligation.state = PitrObligationState::Sealed;
            }
            PitrManifestRecord::SegmentArchived { segment_id } => {
                ensure!(
                    matches!(
                        state.mode,
                        PitrMode::Enabled | PitrMode::ReconciliationRequired
                    ),
                    "PITR segment archived while inactive"
                );
                let obligation = state
                    .obligations
                    .get_mut(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("PITR archived segment has no obligation"))?;
                ensure!(
                    obligation.state == PitrObligationState::Sealed,
                    "PITR segment archived out of order"
                );
                obligation.state = PitrObligationState::Archived;
            }
            PitrManifestRecord::SegmentReclaimable { segment_id } => {
                ensure!(
                    matches!(
                        state.mode,
                        PitrMode::Enabled | PitrMode::ReconciliationRequired
                    ),
                    "PITR segment reclamation started while inactive"
                );
                let obligation = state
                    .obligations
                    .get_mut(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("PITR reclaimed segment has no obligation"))?;
                ensure!(
                    obligation.state == PitrObligationState::Archived,
                    "PITR segment cannot become reclaimable before archive durability"
                );
                obligation.state = PitrObligationState::Reclaimable;
            }
            PitrManifestRecord::DisableClean => {
                ensure!(
                    state.mode == PitrMode::Enabled && state.obligations.is_empty(),
                    "PITR clean disable has outstanding obligations"
                );
                state = PitrState::default();
                state.mode = PitrMode::Disabled;
            }
            PitrManifestRecord::ReconciliationComplete => {
                ensure!(
                    state.mode == PitrMode::ReconciliationRequired,
                    "PITR reconciliation is not active"
                );
                ensure!(
                    state.obligations.is_empty(),
                    "PITR reconciliation has outstanding obligations"
                );
                state = PitrState::default();
            }
            PitrManifestRecord::CoverageGap(gap) => {
                ensure!(
                    state.mode == PitrMode::Enabled,
                    "PITR coverage gap recorded while inactive"
                );
                ensure!(
                    Some(gap.repository_id) == state.repository_id,
                    "PITR recovery gap repository mismatch"
                );
                ensure!(
                    Some(gap.timeline_id) == state.timeline_id,
                    "PITR recovery gap timeline mismatch"
                );
                ensure!(
                    Some(gap.archive_epoch_id) == state.archive_epoch_id,
                    "PITR recovery gap epoch mismatch"
                );
                state.recovery_gap = Some(gap);
                state.mode = PitrMode::ReconciliationRequired;
            }
            PitrManifestRecord::SegmentReclaimed { segment_id } => {
                ensure!(
                    matches!(
                        state.mode,
                        PitrMode::Enabled | PitrMode::ReconciliationRequired
                    ),
                    "PITR segment reclamation completed while inactive"
                );
                let obligation = state
                    .obligations
                    .remove(&segment_id)
                    .ok_or_else(|| anyhow::anyhow!("PITR reclaimed segment has no obligation"))?;
                ensure!(
                    obligation.state == PitrObligationState::Reclaimable,
                    "PITR segment removed before reclaimable state"
                );
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

    #[test]
    fn replay_preserves_seal_archive_reclaim_order() {
        let records = vec![
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: config(),
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 2,
            },
            PitrManifestRecord::SealStarted {
                segment_id: 2,
                successor_segment_id: 3,
                predecessor_anchor: PersistedChainAnchor::Segment {
                    segment_id: 2,
                    wal_digest: [4; 32],
                    seal_digest: [5; 32],
                },
                last_recorded_at: Some(PersistedRecordedAt { secs: 10, nanos: 0 }),
                last_commit_anchor: None,
            },
            PitrManifestRecord::SegmentSealed { segment_id: 2 },
            PitrManifestRecord::SegmentArchived { segment_id: 2 },
            PitrManifestRecord::SegmentReclaimable { segment_id: 2 },
            PitrManifestRecord::SegmentReclaimed { segment_id: 2 },
        ];
        let state = replay_pitr_records(records).unwrap();
        assert_eq!(state.mode, PitrMode::Enabled);
        assert_eq!(state.active_segment_id, Some(3));
        assert!(state.obligations.is_empty());
    }

    #[test]
    fn replay_rejects_out_of_order_transitions() {
        assert!(
            replay_pitr_records([PitrManifestRecord::SegmentArchived { segment_id: 1 }]).is_err()
        );
        assert!(
            replay_pitr_records([
                PitrManifestRecord::EnableIntent {
                    repository_id: [1; 16],
                    timeline_id: [2; 16],
                    archive_epoch_id: [3; 16],
                    config: config()
                },
                PitrManifestRecord::EnableComplete {
                    active_segment_id: 1
                },
                PitrManifestRecord::SegmentArchived { segment_id: 1 },
            ])
            .is_err()
        );
    }

    #[test]
    fn coverage_gap_requires_matching_structured_evidence() {
        let records = [
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: config(),
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 1,
            },
            PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                predecessor: PersistedChainAnchor::Genesis {
                    archive_epoch_id: [3; 16],
                },
                last_archived_commit_ts: Some(1),
                first_uncovered_commit_ts: Some(2),
                reason: CoverageBreakReason::ForcedDisable,
            }),
        ];
        let state = replay_pitr_records(records).unwrap();
        assert_eq!(state.mode, PitrMode::ReconciliationRequired);
        assert!(state.recovery_gap.is_some());
        let cleared = replay_pitr_records([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: config(),
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 1,
            },
            PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                predecessor: PersistedChainAnchor::Genesis {
                    archive_epoch_id: [3; 16],
                },
                last_archived_commit_ts: None,
                first_uncovered_commit_ts: None,
                reason: CoverageBreakReason::ForcedDisable,
            }),
            PitrManifestRecord::ReconciliationComplete,
        ])
        .unwrap();
        assert_eq!(cleared, PitrState::default());
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(PitrState {
                mode: PitrMode::ReconciliationRequired,
                ..PitrState::default()
            }))])
            .is_err()
        );
    }

    #[test]
    fn snapshot_round_trip_is_validated() {
        let state = replay_pitr_records([
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: config(),
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 1,
            },
        ])
        .unwrap();
        let bytes = serde_json::to_vec(&state).unwrap();
        let decoded: PitrState = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, state);
        decoded.validate().unwrap();
    }
}
