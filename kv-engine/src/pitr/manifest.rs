//! Persisted PITR manifest state and replay reducer.
//!
//! This module models v7 crash boundaries without participating in the live
//! v6 manifest. Later slices will embed these records in `ManifestRecord`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const PITR_MANIFEST_FORMAT_VERSION: u32 = 7;
pub(crate) const MAX_PITR_SNAPSHOT_BYTES: usize = 1 << 20;
pub(crate) const MAX_PITR_RECORD_STREAM_BYTES: usize = 4 << 20;
const PITR_SNAPSHOT_MAGIC: &[u8; 5] = b"PITR7";
const PITR_SNAPSHOT_HEADER_LEN: usize = 5 + 4 + 4 + 32;
const PITR_RECORD_MAGIC: &[u8; 5] = b"PITRr";
const PITR_RECORD_HEADER_LEN: usize = 5 + 4 + 4 + 32;

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

/// Logical recovery boundary for an Active v7 WAL.
///
/// This deliberately excludes the physical FRONTIER offset, generation, and
/// frame digest because Active recovery may relocate and rechain its frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ActiveBoundary {
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) segment_id: u64,
    pub(crate) incarnation: [u8; 16],
    pub(crate) ticket_end: u64,
    pub(crate) durable_end: u64,
    pub(crate) last_commit_ts: u64,
    pub(crate) prefix_digest: [u8; 32],
}

impl ActiveBoundary {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.timeline_id != [0; 16],
            "Active WAL timeline identity is empty"
        );
        ensure!(
            self.archive_epoch_id != [0; 16],
            "Active WAL archive epoch identity is empty"
        );
        ensure!(
            self.incarnation != [0; 16],
            "Active WAL incarnation is empty"
        );
        ensure!(
            self.durable_end.is_multiple_of(4096),
            "Active WAL durable boundary is not frame aligned"
        );
        match self.ticket_end {
            0 => ensure!(
                self.durable_end == 4096 && self.last_commit_ts == 0,
                "empty Active WAL boundary is inconsistent"
            ),
            _ => ensure!(
                self.durable_end >= 3 * 4096 && self.last_commit_ts != 0,
                "nonempty Active WAL boundary is inconsistent"
            ),
        }

        Ok(())
    }
}

/// Physical recovery boundary for a Sealing or Sealed v7 WAL image.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ImmutableBoundary {
    pub(crate) active: ActiveBoundary,
    pub(crate) sealed_end: u64,
    pub(crate) terminal_frontier_digest: [u8; 32],
    pub(crate) wal_digest: [u8; 32],
}

impl ImmutableBoundary {
    pub(crate) fn validate(&self) -> Result<()> {
        self.active.validate()?;
        let min_sealed_end = self
            .active
            .durable_end
            .checked_add(4096)
            .ok_or_else(|| anyhow::anyhow!("immutable WAL boundary overflows"))?;
        ensure!(
            self.sealed_end.is_multiple_of(4096) && self.sealed_end >= min_sealed_end,
            "immutable WAL boundary is inconsistent with its Active anchor"
        );
        if self.active.ticket_end == 0 {
            ensure!(
                self.sealed_end == min_sealed_end,
                "empty immutable WAL must end immediately after the generation-zero FRONTIER"
            );
        }

        Ok(())
    }
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
    pub(crate) database_timeline_id: Option<[u8; 16]>,
    pub(crate) repository_id: Option<[u8; 16]>,
    pub(crate) timeline_id: Option<[u8; 16]>,
    pub(crate) archive_epoch_id: Option<[u8; 16]>,
    pub(crate) config: Option<PersistedPitrConfig>,
    pub(crate) active_segment_id: Option<u64>,
    /// Logical durable floor for an Active v7 WAL. This excludes physical
    /// FRONTIER identity so recovery may relocate and rechain the marker.
    #[serde(default)]
    pub(crate) active_boundary: Option<ActiveBoundary>,
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
            database_timeline_id: None,
            repository_id: None,
            timeline_id: None,
            archive_epoch_id: None,
            config: None,
            active_segment_id: None,
            active_boundary: None,
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
    pub(crate) fn validate_for_status(&self) -> Result<()> {
        self.validate()
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.database_timeline_id.is_none_or(|id| id != [0; 16]),
            "database timeline identity is empty"
        );
        if self.mode == PitrMode::Disabled {
            let is_legacy = self.database_timeline_id.is_none();
            ensure!(
                self.active_segment_id.is_none()
                    && self.active_boundary.is_none()
                    && self.obligations.is_empty()
                    && self.uncertain_segment_id.is_none(),
                "disabled PITR state is not canonical"
            );
            if is_legacy {
                ensure!(
                    self == &Self::default(),
                    "legacy disabled PITR state is not empty"
                );
                return Ok(());
            }
            if self.repository_id.is_none() {
                ensure!(
                    self.timeline_id.is_none()
                        && self.archive_epoch_id.is_none()
                        && self.config.is_none()
                        && self.active_boundary.is_none()
                        && self.next_segment_id == 0
                        && self.epoch_genesis_anchor.is_none()
                        && self.predecessor_anchor.is_none()
                        && self.last_recorded_at.is_none()
                        && self.last_commit_anchor.is_none()
                        && self.recovery_gap.is_none(),
                    "recovery-only disabled PITR state retains source lifecycle"
                );
                return Ok(());
            }
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
            self.timeline_id == self.database_timeline_id,
            "PITR epoch changes the database timeline"
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
                self.active_segment_id.is_none()
                    && self.active_boundary.is_none()
                    && self.obligations.is_empty(),
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
        } else if self.mode == PitrMode::Disabled {
            ensure!(
                self.predecessor_anchor.is_some() && self.next_segment_id > 0,
                "disabled PITR state is missing its last segment boundary"
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
            if let Some(PersistedChainAnchor::Segment { segment_id, .. }) = self.predecessor_anchor
            {
                ensure!(
                    segment_id < active,
                    "active PITR predecessor is not before the active segment"
                );
            }
        }
        if let Some(boundary) = self.active_boundary {
            boundary.validate()?;
            ensure!(
                self.mode != PitrMode::Disabled && self.mode != PitrMode::Enabling,
                "inactive PITR state retains an Active WAL boundary"
            );
            ensure!(
                Some(boundary.timeline_id) == self.timeline_id
                    && Some(boundary.archive_epoch_id) == self.archive_epoch_id
                    && Some(boundary.segment_id) == self.active_segment_id,
                "Active WAL boundary does not match the PITR frontier"
            );
            ensure!(
                boundary.ticket_end == 0
                    || self
                        .last_commit_anchor
                        .is_none_or(|anchor| boundary.last_commit_ts > anchor.commit_ts),
                "Active WAL commit boundary does not advance the sealed high-water"
            );
            if let Some(obligation) = self.obligations.get(&boundary.segment_id) {
                ensure!(
                    obligation.state == ObligationState::Sealing
                        || (self.mode == PitrMode::ReconciliationRequired
                            && obligation.state == ObligationState::Abandoned),
                    "Active WAL boundary has an incompatible segment obligation"
                );
                if obligation.state == ObligationState::Sealing {
                    validate_active_sealing_length(boundary, obligation.logical_length)?;
                }
            }
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
            matches!(
                self.mode,
                PitrMode::ReconciliationRequired | PitrMode::Disabled
            ) || self.recovery_gap.is_none(),
            "non-reconciliation PITR state retains gap"
        );
        if self.mode == PitrMode::ReconciliationRequired || self.recovery_gap.is_some() {
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
            if let Some(boundary) = self.active_boundary
                && boundary.ticket_end != 0
            {
                ensure!(
                    gap.first_uncovered_commit_ts
                        .is_some_and(|first| first != 0 && first <= boundary.last_commit_ts),
                    "recovery gap omits known uncovered Active WAL commits"
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
    /// Durably advance the logical recovery floor for the current Active v7
    /// WAL. The first boundary for a segment must be its empty header prefix.
    ActiveBoundaryAdvanced {
        boundary: ActiveBoundary,
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
    RecordedAtAdvanced {
        recorded_at: PersistedRecordedAt,
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
    let mut saw_record = false;

    for record in records {
        match record {
            PitrManifestRecord::EnableIntent {
                repository_id,
                timeline_id,
                archive_epoch_id,
                config,
            } => {
                ensure!(
                    state.mode == PitrMode::Disabled,
                    "enable intent overlaps existing PITR state"
                );
                ensure!(
                    repository_id != [0; 16],
                    "PITR repository identity is empty"
                );
                ensure!(timeline_id != [0; 16], "PITR timeline identity is empty");
                ensure!(
                    archive_epoch_id != [0; 16],
                    "PITR archive epoch identity is empty"
                );
                ensure!(
                    state
                        .database_timeline_id
                        .is_none_or(|known| known == timeline_id),
                    "PITR enable changes the database timeline"
                );
                ensure!(
                    state.archive_epoch_id != Some(archive_epoch_id),
                    "PITR enable reuses the last archive epoch"
                );
                config.validate()?;
                let genesis = PersistedChainAnchor::Genesis { archive_epoch_id };
                let database_timeline_id = Some(timeline_id);
                state = PitrState {
                    mode: PitrMode::Enabling,
                    database_timeline_id,
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
                if state.mode == PitrMode::Enabled {
                    ensure!(
                        state.active_segment_id == Some(active_segment_id),
                        "enable completion conflicts with enabled PITR state"
                    );
                    // A persistence callback may have committed the completion
                    // record before returning an error. Replaying the exact same
                    // completion is therefore an idempotent retry.
                    continue;
                }
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
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary } => {
                ensure!(
                    state.mode == PitrMode::Enabled,
                    "Active WAL boundary advanced while PITR is not enabled"
                );
                boundary.validate()?;
                ensure!(
                    Some(boundary.timeline_id) == state.timeline_id
                        && Some(boundary.archive_epoch_id) == state.archive_epoch_id
                        && Some(boundary.segment_id) == state.active_segment_id,
                    "Active WAL boundary does not match the PITR frontier"
                );
                ensure!(
                    state
                        .obligations
                        .get(&boundary.segment_id)
                        .is_none_or(|obligation| obligation.state != ObligationState::Sealing),
                    "Active WAL boundary advanced after sealing started"
                );
                if let Some(previous) = state.active_boundary {
                    validate_active_boundary_advance(previous, boundary)?;
                } else {
                    ensure!(
                        boundary.ticket_end == 0
                            && boundary.durable_end == 4096
                            && boundary.last_commit_ts == 0,
                        "first Active WAL boundary is not the empty prefix"
                    );
                }
                state.active_boundary = Some(boundary);
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
                if let Some(boundary) = state.active_boundary {
                    validate_active_sealing_length(boundary, logical_length)?;
                }
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
                if let Some(boundary) = state.active_boundary {
                    ensure!(
                        boundary.segment_id == segment_id,
                        "sealed segment does not contain the Active WAL boundary"
                    );
                    ensure!(
                        boundary.ticket_end == 0
                            || last_commit_anchor
                                .is_some_and(|anchor| anchor.commit_ts >= boundary.last_commit_ts),
                        "sealed segment is below its Active WAL commit boundary"
                    );
                }
                let successor = obligation.successor_segment_id;
                state.active_segment_id = Some(successor);
                state.active_boundary = None;
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
            PitrManifestRecord::RecordedAtAdvanced { recorded_at } => {
                ensure!(
                    state.mode == PitrMode::Enabled,
                    "recorded-time advance while PITR is inactive"
                );
                ensure!(
                    recorded_at.nanos < 1_000_000_000
                        && state
                            .last_recorded_at
                            .is_none_or(|previous| recorded_at >= previous),
                    "PITR recorded-time advance regresses or is invalid"
                );
                state.last_recorded_at = Some(recorded_at);
            }
            PitrManifestRecord::DisableClean => {
                ensure!(
                    state.mode == PitrMode::Enabled && state.obligations.is_empty(),
                    "clean disable has outstanding work"
                );
                ensure!(
                    state
                        .active_boundary
                        .is_none_or(|boundary| boundary.ticket_end == 0),
                    "clean disable has an unsealed Active WAL commit boundary"
                );
                state = disabled_lifecycle_state(&state);
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
                state = disabled_lifecycle_state(&state);
            }
            PitrManifestRecord::Snapshot(snapshot) => {
                ensure!(!saw_record, "PITR snapshot is not the first record");
                snapshot.validate()?;
                state = *snapshot;
            }
        }
        state.validate()?;
        saw_record = true;
    }
    Ok(state)
}

pub(crate) fn encode_pitr_snapshot(state: &PitrState) -> Result<Vec<u8>> {
    state.validate()?;
    let payload = serde_json::to_vec(state)?;
    ensure!(
        payload.len() <= MAX_PITR_SNAPSHOT_BYTES,
        "PITR snapshot exceeds the configured size limit"
    );
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("PITR snapshot payload exceeds wire length"))?;
    let mut digest = Sha256::new();
    digest.update(b"TOYKV-PITR-SNAPSHOT-V1");
    digest.update(payload_len.to_be_bytes());
    digest.update(&payload);
    let digest: [u8; 32] = digest.finalize().into();
    let mut encoded = Vec::with_capacity(PITR_SNAPSHOT_HEADER_LEN + payload.len());
    encoded.extend_from_slice(PITR_SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&PITR_MANIFEST_FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&digest);
    encoded.extend_from_slice(&payload);
    ensure!(
        encoded.len() <= MAX_PITR_SNAPSHOT_BYTES,
        "PITR snapshot exceeds the configured size limit"
    );

    Ok(encoded)
}

pub(crate) fn decode_pitr_snapshot(bytes: &[u8]) -> Result<PitrState> {
    ensure!(
        bytes.len() <= MAX_PITR_SNAPSHOT_BYTES,
        "PITR snapshot exceeds the configured size limit"
    );
    ensure!(
        bytes.len() >= PITR_SNAPSHOT_HEADER_LEN,
        "truncated PITR snapshot envelope"
    );
    ensure!(
        &bytes[..PITR_SNAPSHOT_MAGIC.len()] == PITR_SNAPSHOT_MAGIC,
        "invalid PITR snapshot magic"
    );
    ensure!(
        u32::from_be_bytes(bytes[5..9].try_into().unwrap()) == PITR_MANIFEST_FORMAT_VERSION,
        "unsupported PITR snapshot version"
    );
    let payload_len = usize::try_from(u32::from_be_bytes(bytes[9..13].try_into().unwrap()))
        .map_err(|_| anyhow::anyhow!("PITR snapshot payload length is invalid"))?;
    ensure!(
        payload_len == bytes.len() - PITR_SNAPSHOT_HEADER_LEN,
        "PITR snapshot payload length does not match envelope"
    );
    let payload = &bytes[PITR_SNAPSHOT_HEADER_LEN..];
    let mut digest = Sha256::new();
    digest.update(b"TOYKV-PITR-SNAPSHOT-V1");
    digest.update(u32::try_from(payload_len).unwrap().to_be_bytes());
    digest.update(payload);
    ensure!(
        digest.finalize().as_slice() == &bytes[13..PITR_SNAPSHOT_HEADER_LEN],
        "PITR snapshot digest mismatch"
    );
    let state: PitrState = serde_json::from_slice(payload)?;
    state.validate()?;

    Ok(state)
}

pub(crate) fn encode_pitr_record(record: &PitrManifestRecord) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(record)?;
    ensure!(
        payload.len() <= MAX_PITR_SNAPSHOT_BYTES,
        "PITR manifest record exceeds the configured size limit"
    );
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("PITR manifest record exceeds wire length"))?;
    let mut digest = Sha256::new();
    digest.update(b"TOYKV-PITR-RECORD-V1");
    digest.update(payload_len.to_be_bytes());
    digest.update(&payload);
    let digest: [u8; 32] = digest.finalize().into();
    let mut encoded = Vec::with_capacity(PITR_RECORD_HEADER_LEN + payload.len());
    encoded.extend_from_slice(PITR_RECORD_MAGIC);
    encoded.extend_from_slice(&PITR_MANIFEST_FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&digest);
    encoded.extend_from_slice(&payload);

    Ok(encoded)
}

pub(crate) fn decode_pitr_record(bytes: &[u8]) -> Result<PitrManifestRecord> {
    ensure!(
        bytes.len() <= MAX_PITR_SNAPSHOT_BYTES,
        "PITR manifest record exceeds the configured size limit"
    );
    ensure!(
        bytes.len() >= PITR_RECORD_HEADER_LEN,
        "truncated PITR manifest record envelope"
    );
    ensure!(
        &bytes[..PITR_RECORD_MAGIC.len()] == PITR_RECORD_MAGIC,
        "invalid PITR manifest record magic"
    );
    ensure!(
        u32::from_be_bytes(bytes[5..9].try_into().unwrap()) == PITR_MANIFEST_FORMAT_VERSION,
        "unsupported PITR manifest record version"
    );
    let payload_len = usize::try_from(u32::from_be_bytes(bytes[9..13].try_into().unwrap()))
        .map_err(|_| anyhow::anyhow!("PITR manifest record payload length is invalid"))?;
    ensure!(
        payload_len == bytes.len() - PITR_RECORD_HEADER_LEN,
        "PITR manifest record payload length does not match envelope"
    );
    let payload = &bytes[PITR_RECORD_HEADER_LEN..];
    let mut digest = Sha256::new();
    digest.update(b"TOYKV-PITR-RECORD-V1");
    digest.update(u32::try_from(payload_len).unwrap().to_be_bytes());
    digest.update(payload);
    ensure!(
        digest.finalize().as_slice() == &bytes[13..PITR_RECORD_HEADER_LEN],
        "PITR manifest record digest mismatch"
    );

    Ok(serde_json::from_slice(payload)?)
}

pub(crate) fn encode_pitr_record_stream(records: &[PitrManifestRecord]) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();

    for record in records {
        let frame = encode_pitr_record(record)?;
        let frame_len = u32::try_from(frame.len())
            .map_err(|_| anyhow::anyhow!("PITR manifest record frame is too large"))?;
        encoded.extend_from_slice(&frame_len.to_be_bytes());
        encoded.extend_from_slice(&frame);
        ensure!(
            encoded.len() <= MAX_PITR_RECORD_STREAM_BYTES,
            "PITR manifest record stream exceeds the configured size limit"
        );
    }
    Ok(encoded)
}

pub(crate) fn decode_pitr_record_stream(bytes: &[u8]) -> Result<Vec<PitrManifestRecord>> {
    ensure!(
        bytes.len() <= MAX_PITR_RECORD_STREAM_BYTES,
        "PITR manifest record stream exceeds the configured size limit"
    );
    let mut offset = 0;
    let mut records = Vec::new();

    while offset < bytes.len() {
        ensure!(
            bytes.len() - offset >= 4,
            "truncated PITR manifest record frame length"
        );
        let frame_len = usize::try_from(u32::from_be_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ))
        .map_err(|_| anyhow::anyhow!("PITR manifest record frame length is invalid"))?;
        offset += 4;
        ensure!(
            frame_len > 0 && frame_len <= bytes.len() - offset,
            "invalid PITR manifest record frame length"
        );
        records.push(decode_pitr_record(&bytes[offset..offset + frame_len])?);
        offset += frame_len;
    }
    Ok(records)
}

pub(crate) fn replay_pitr_record_stream(bytes: &[u8]) -> Result<PitrState> {
    replay_pitr_records(decode_pitr_record_stream(bytes)?)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PitrManifestLog {
    records: Vec<PitrManifestRecord>,
    encoded: Vec<u8>,
    state: PitrState,
}

impl PitrManifestLog {
    pub(crate) fn recover(encoded: &[u8]) -> Result<Self> {
        let records = decode_pitr_record_stream(encoded)?;
        let state = replay_pitr_records(records.clone())?;

        Ok(Self {
            records,
            encoded: encoded.to_vec(),
            state,
        })
    }

    pub(crate) fn append(&mut self, record: PitrManifestRecord) -> Result<()> {
        let mut records = self.records.clone();
        records.push(record.clone());
        let state = replay_pitr_records(records.clone())?;
        let frame = encode_pitr_record_stream(std::slice::from_ref(&record))?;
        ensure!(
            self.encoded.len() + frame.len() <= MAX_PITR_RECORD_STREAM_BYTES,
            "PITR manifest record stream exceeds the configured size limit"
        );
        self.records = records;
        self.encoded.extend_from_slice(&frame);
        self.state = state;

        Ok(())
    }

    pub(crate) fn records(&self) -> &[PitrManifestRecord] {
        &self.records
    }

    pub(crate) fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    pub(crate) fn state(&self) -> &PitrState {
        &self.state
    }
}

fn disabled_lifecycle_state(state: &PitrState) -> PitrState {
    let mut disabled = state.clone();
    disabled.mode = PitrMode::Disabled;
    disabled.active_segment_id = None;
    disabled.active_boundary = None;
    disabled.obligations.clear();
    disabled.uncertain_segment_id = None;

    disabled
}

fn validate_active_boundary_advance(previous: ActiveBoundary, next: ActiveBoundary) -> Result<()> {
    ensure!(
        previous.timeline_id == next.timeline_id
            && previous.archive_epoch_id == next.archive_epoch_id
            && previous.segment_id == next.segment_id
            && previous.incarnation == next.incarnation,
        "Active WAL boundary changes segment identity"
    );
    ensure!(
        next.ticket_end >= previous.ticket_end
            && next.durable_end >= previous.durable_end
            && next.last_commit_ts >= previous.last_commit_ts,
        "Active WAL boundary regresses"
    );
    if next.ticket_end == previous.ticket_end {
        ensure!(
            next == previous,
            "Active WAL boundary changes without advancing its ticket"
        );
    } else {
        ensure!(
            next.durable_end > previous.durable_end
                && next.last_commit_ts > previous.last_commit_ts,
            "Active WAL boundary ticket advance lacks a larger durable prefix"
        );
    }
    Ok(())
}

fn validate_active_sealing_length(boundary: ActiveBoundary, logical_length: u64) -> Result<()> {
    let minimum_length = boundary
        .durable_end
        .checked_add(4096)
        .ok_or_else(|| anyhow::anyhow!("Active WAL boundary and terminal frontier overflow"))?;
    ensure!(
        logical_length >= minimum_length,
        "sealing length omits the Active WAL boundary or terminal frontier"
    );

    Ok(())
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

    fn active_boundary() -> ActiveBoundary {
        ActiveBoundary {
            timeline_id: [1; 16],
            archive_epoch_id: [2; 16],
            segment_id: 3,
            incarnation: [4; 16],
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 5,
            prefix_digest: [6; 32],
        }
    }

    fn empty_active_boundary_for_enabled_segment() -> ActiveBoundary {
        ActiveBoundary {
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            segment_id: 1,
            incarnation: [4; 16],
            ticket_end: 0,
            durable_end: 4096,
            last_commit_ts: 0,
            prefix_digest: [6; 32],
        }
    }

    #[test]
    fn v7_boundaries_validate_and_round_trip_without_active_physical_identity() {
        let active = active_boundary();
        active.validate().unwrap();
        let active_bytes = serde_json::to_vec(&active).unwrap();
        let active_json: serde_json::Value = serde_json::from_slice(&active_bytes).unwrap();
        assert!(active_json.get("frontier_offset").is_none());
        assert!(active_json.get("generation").is_none());
        assert!(active_json.get("frontier_digest").is_none());
        assert_eq!(
            serde_json::from_slice::<ActiveBoundary>(&active_bytes).unwrap(),
            active
        );

        let immutable = ImmutableBoundary {
            active,
            sealed_end: 4 * 4096,
            terminal_frontier_digest: [7; 32],
            wal_digest: [8; 32],
        };
        immutable.validate().unwrap();
        let immutable_bytes = serde_json::to_vec(&immutable).unwrap();
        assert_eq!(
            serde_json::from_slice::<ImmutableBoundary>(&immutable_bytes).unwrap(),
            immutable
        );
    }

    #[test]
    fn v7_boundaries_reject_inconsistent_empty_and_sealed_ranges() {
        let mut empty = active_boundary();
        empty.ticket_end = 0;
        empty.durable_end = 8192;
        empty.last_commit_ts = 0;
        assert!(empty.validate().is_err());

        let immutable = ImmutableBoundary {
            active: active_boundary(),
            sealed_end: 3 * 4096,
            terminal_frontier_digest: [7; 32],
            wal_digest: [8; 32],
        };
        assert!(immutable.validate().is_err());
    }

    #[test]
    fn v7_boundaries_empty_immutable_ends_after_generation_zero() {
        let mut active = active_boundary();
        active.ticket_end = 0;
        active.durable_end = 4096;
        active.last_commit_ts = 0;
        let empty = ImmutableBoundary {
            active,
            sealed_end: 8192,
            terminal_frontier_digest: [7; 32],
            wal_digest: [8; 32],
        };
        empty.validate().unwrap();
        let bytes = serde_json::to_vec(&empty).unwrap();
        assert_eq!(
            serde_json::from_slice::<ImmutableBoundary>(&bytes).unwrap(),
            empty
        );

        for sealed_end in [12288, 16384] {
            let oversized = ImmutableBoundary {
                sealed_end,
                ..empty
            };
            assert!(oversized.validate().is_err());
        }
    }

    #[test]
    fn active_boundaries_are_durably_replayed_as_logical_floors() {
        let first = empty_active_boundary_for_enabled_segment();
        let advanced = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..first
        };
        let records = [
            enable()[0].clone(),
            enable()[1].clone(),
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: first },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: advanced },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: advanced },
        ];
        let state = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(state.active_boundary, Some(advanced));

        let encoded = encode_pitr_record_stream(&records).unwrap();
        assert_eq!(replay_pitr_record_stream(&encoded).unwrap(), state);
    }

    #[test]
    fn active_boundary_updates_reject_missing_initial_floor_and_regressions() {
        let first = empty_active_boundary_for_enabled_segment();
        let nonempty = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..first
        };
        let prefix = [enable()[0].clone(), enable()[1].clone()];

        let mut records = prefix.to_vec();
        records.push(PitrManifestRecord::ActiveBoundaryAdvanced { boundary: nonempty });
        assert!(replay_pitr_records(records).is_err());

        for invalid in [
            ActiveBoundary {
                incarnation: [10; 16],
                ticket_end: 2,
                durable_end: 4 * 4096,
                last_commit_ts: 8,
                prefix_digest: [10; 32],
                ..nonempty
            },
            ActiveBoundary {
                ticket_end: 0,
                durable_end: 4096,
                last_commit_ts: 0,
                prefix_digest: first.prefix_digest,
                ..nonempty
            },
            ActiveBoundary {
                prefix_digest: [10; 32],
                ..nonempty
            },
            ActiveBoundary {
                durable_end: 4 * 4096,
                ..nonempty
            },
            ActiveBoundary {
                ticket_end: 2,
                durable_end: nonempty.durable_end,
                last_commit_ts: 8,
                prefix_digest: [10; 32],
                ..nonempty
            },
        ] {
            let mut records = prefix.to_vec();
            records.extend([
                PitrManifestRecord::ActiveBoundaryAdvanced { boundary: first },
                PitrManifestRecord::ActiveBoundaryAdvanced { boundary: nonempty },
                PitrManifestRecord::ActiveBoundaryAdvanced { boundary: invalid },
            ]);
            assert!(replay_pitr_records(records).is_err());
        }
    }

    #[test]
    fn old_pitr_snapshots_default_to_no_active_v7_boundary() {
        let state = replay_pitr_records(enable()).unwrap();
        let mut serialized = serde_json::to_value(&state).unwrap();
        serialized
            .as_object_mut()
            .unwrap()
            .remove("active_boundary");

        let decoded: PitrState = serde_json::from_value(serialized).unwrap();
        assert_eq!(decoded, state);
        assert_eq!(decoded.active_boundary, None);
    }

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
        let active_boundary = empty_active_boundary_for_enabled_segment();
        records.push(PitrManifestRecord::ActiveBoundaryAdvanced {
            boundary: active_boundary,
        });
        records.push(PitrManifestRecord::SealStarted {
            segment_id: 1,
            successor_segment_id: 2,
            logical_length: 8192,
        });
        let sealing = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(sealing.active_segment_id, Some(1));
        assert_eq!(sealing.active_boundary, Some(active_boundary));

        let terminal_boundary = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..active_boundary
        };
        let mut invalid_records = records.clone();
        invalid_records.push(PitrManifestRecord::ActiveBoundaryAdvanced {
            boundary: terminal_boundary,
        });
        assert!(replay_pitr_records(invalid_records).is_err());

        records.push(PitrManifestRecord::SegmentSealed {
            segment_id: 1,
            segment_anchor: anchor(1),
            last_recorded_at: None,
            last_commit_anchor: None,
        });
        let sealed = replay_pitr_records(records).unwrap();
        assert_eq!(sealed.active_segment_id, Some(2));
        assert_eq!(sealed.active_boundary, None);
        assert_eq!(sealed.predecessor_anchor, Some(anchor(1)));
    }

    #[test]
    fn sealing_handoff_preserves_the_active_boundary() {
        let first = empty_active_boundary_for_enabled_segment();
        let durable = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..first
        };
        let mut records = enable();
        records.extend([
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: first },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: durable },
        ]);

        for logical_length in [4096, durable.durable_end] {
            let mut invalid_records = records.clone();
            invalid_records.push(PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length,
            });
            assert!(replay_pitr_records(invalid_records).is_err());
        }

        records.push(PitrManifestRecord::SealStarted {
            segment_id: 1,
            successor_segment_id: 2,
            logical_length: 4 * 4096,
        });
        let sealing = replay_pitr_records(records.clone()).unwrap();
        let mut invalid_snapshot = sealing.clone();
        invalid_snapshot
            .obligations
            .get_mut(&1)
            .unwrap()
            .logical_length = durable.durable_end;
        assert!(encode_pitr_snapshot(&invalid_snapshot).is_err());
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(invalid_snapshot))])
                .is_err()
        );

        let mut overflow_snapshot = sealing;
        overflow_snapshot
            .active_boundary
            .as_mut()
            .unwrap()
            .durable_end = u64::MAX - 4095;
        assert!(encode_pitr_snapshot(&overflow_snapshot).is_err());

        let commit_anchor = PersistedCommitAnchor {
            segment_id: 1,
            commit_ts: durable.last_commit_ts,
            recorded_at: PersistedRecordedAt { secs: 1, nanos: 0 },
            entry_digest: [10; 32],
        };
        for last_commit_anchor in [
            None,
            Some(PersistedCommitAnchor {
                commit_ts: durable.last_commit_ts - 1,
                ..commit_anchor
            }),
        ] {
            let mut invalid_records = records.clone();
            invalid_records.push(PitrManifestRecord::SegmentSealed {
                segment_id: 1,
                segment_anchor: anchor(1),
                last_recorded_at: last_commit_anchor.map(|anchor| anchor.recorded_at),
                last_commit_anchor,
            });
            assert!(replay_pitr_records(invalid_records).is_err());
        }

        for commit_ts in [durable.last_commit_ts, durable.last_commit_ts + 1] {
            let mut sealed_records = records.clone();
            let last_commit_anchor = PersistedCommitAnchor {
                commit_ts,
                ..commit_anchor
            };
            sealed_records.push(PitrManifestRecord::SegmentSealed {
                segment_id: 1,
                segment_anchor: anchor(1),
                last_recorded_at: Some(last_commit_anchor.recorded_at),
                last_commit_anchor: Some(last_commit_anchor),
            });
            let sealed = replay_pitr_records(sealed_records).unwrap();
            assert_eq!(sealed.active_boundary, None);
            assert_eq!(sealed.last_commit_anchor, Some(last_commit_anchor));
            assert_eq!(sealed.active_segment_id, Some(2));
        }
    }

    #[test]
    fn active_boundaries_follow_the_sealed_commit_high_water() {
        let mut records = enable();
        let sealed_commit = PersistedCommitAnchor {
            segment_id: 1,
            commit_ts: 10,
            recorded_at: PersistedRecordedAt { secs: 1, nanos: 0 },
            entry_digest: [10; 32],
        };
        records.extend([
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: 8192,
            },
            PitrManifestRecord::SegmentSealed {
                segment_id: 1,
                segment_anchor: anchor(1),
                last_recorded_at: Some(sealed_commit.recorded_at),
                last_commit_anchor: Some(sealed_commit),
            },
        ]);
        let empty = ActiveBoundary {
            segment_id: 2,
            incarnation: [11; 16],
            ..empty_active_boundary_for_enabled_segment()
        };
        records.push(PitrManifestRecord::ActiveBoundaryAdvanced { boundary: empty });
        let empty_state = replay_pitr_records(records.clone()).unwrap();

        for last_commit_ts in [9, 10, 11] {
            let boundary = ActiveBoundary {
                ticket_end: 1,
                durable_end: 3 * 4096,
                last_commit_ts,
                prefix_digest: [12; 32],
                ..empty
            };
            let mut candidate_records = records.clone();
            candidate_records.push(PitrManifestRecord::ActiveBoundaryAdvanced { boundary });
            let mut candidate_snapshot = empty_state.clone();
            candidate_snapshot.active_boundary = Some(boundary);
            if last_commit_ts <= sealed_commit.commit_ts {
                assert!(replay_pitr_records(candidate_records).is_err());
                assert!(encode_pitr_snapshot(&candidate_snapshot).is_err());
                assert!(
                    replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(
                        candidate_snapshot
                    ))])
                    .is_err()
                );
            } else {
                assert_eq!(
                    replay_pitr_records(candidate_records).unwrap(),
                    candidate_snapshot
                );
                assert_eq!(
                    decode_pitr_snapshot(&encode_pitr_snapshot(&candidate_snapshot).unwrap())
                        .unwrap(),
                    candidate_snapshot
                );
            }
        }
    }

    #[test]
    fn clean_disable_preserves_an_unsealed_active_boundary() {
        let first = empty_active_boundary_for_enabled_segment();
        let durable = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..first
        };
        let mut records = enable();
        records.extend([
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: first },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: durable },
        ]);
        let mut clean_records = records.clone();
        clean_records.push(PitrManifestRecord::DisableClean);
        assert!(replay_pitr_records(clean_records).is_err());

        for boundary in [None, Some(first)] {
            let mut empty_records = enable();
            if let Some(boundary) = boundary {
                empty_records.push(PitrManifestRecord::ActiveBoundaryAdvanced { boundary });
            }
            empty_records.push(PitrManifestRecord::DisableClean);
            let disabled = replay_pitr_records(empty_records).unwrap();
            assert_eq!(disabled.mode, PitrMode::Disabled);
            assert_eq!(disabled.active_boundary, None);
        }

        records.extend([
            PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                after: PersistedChainAnchor::Genesis {
                    archive_epoch_id: [3; 16],
                },
                last_archived_commit_ts: None,
                first_uncovered_commit_ts: Some(durable.last_commit_ts),
                reason: CoverageBreakReason::ForcedDisable,
            }),
            PitrManifestRecord::ReconciliationComplete,
        ]);
        let disabled = replay_pitr_records(records).unwrap();
        assert_eq!(disabled.mode, PitrMode::Disabled);
        assert_eq!(disabled.active_boundary, None);
        assert_eq!(
            disabled.recovery_gap.unwrap().first_uncovered_commit_ts,
            Some(durable.last_commit_ts)
        );
    }

    #[test]
    fn reconciliation_snapshots_and_gaps_preserve_the_active_floor() {
        let first = empty_active_boundary_for_enabled_segment();
        let durable = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..first
        };
        let mut log = PitrManifestLog::default();
        for record in enable().into_iter().chain([
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: first },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: durable },
        ]) {
            log.append(record).unwrap();
        }
        let active = log.state().clone();
        let encoded_before = log.encoded().to_vec();
        let snapshot_before = encode_pitr_snapshot(&active).unwrap();
        let valid_gap = PersistedRecoveryGap {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            after: PersistedChainAnchor::Genesis {
                archive_epoch_id: [3; 16],
            },
            last_archived_commit_ts: None,
            first_uncovered_commit_ts: Some(durable.last_commit_ts),
            reason: CoverageBreakReason::ForcedDisable,
        };
        let invalid_gaps = [
            None,
            Some(PersistedRecoveryGap {
                first_uncovered_commit_ts: None,
                ..valid_gap.clone()
            }),
            Some(PersistedRecoveryGap {
                first_uncovered_commit_ts: Some(0),
                ..valid_gap.clone()
            }),
            Some(PersistedRecoveryGap {
                first_uncovered_commit_ts: Some(durable.last_commit_ts + 1),
                ..valid_gap.clone()
            }),
        ];
        for recovery_gap in invalid_gaps {
            let mut snapshot = active.clone();
            snapshot.mode = PitrMode::ReconciliationRequired;
            snapshot.recovery_gap = recovery_gap.clone();
            assert!(encode_pitr_snapshot(&snapshot).is_err());
            assert!(
                replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(snapshot.clone()))])
                    .is_err()
            );

            // Recompute the envelope digest so decoding must reject the state
            // itself rather than a checksum mismatch.
            let payload = serde_json::to_vec(&snapshot).unwrap();
            let payload_len = u32::try_from(payload.len()).unwrap().to_be_bytes();
            let mut digest = Sha256::new();
            digest.update(b"TOYKV-PITR-SNAPSHOT-V1");
            digest.update(payload_len);
            digest.update(&payload);
            let mut encoded_snapshot = snapshot_before[..PITR_SNAPSHOT_HEADER_LEN].to_vec();
            encoded_snapshot[9..13].copy_from_slice(&payload_len);
            encoded_snapshot[13..PITR_SNAPSHOT_HEADER_LEN]
                .copy_from_slice(digest.finalize().as_slice());
            encoded_snapshot.extend_from_slice(&payload);
            assert!(decode_pitr_snapshot(&encoded_snapshot).is_err());

            if let Some(gap) = recovery_gap {
                assert!(log.append(PitrManifestRecord::CoverageGap(gap)).is_err());
                assert_eq!(log.state(), &active);
                assert_eq!(log.encoded(), encoded_before.as_slice());
            }
        }

        log.append(PitrManifestRecord::CoverageGap(valid_gap.clone()))
            .unwrap();
        let snapshot = encode_pitr_snapshot(log.state()).unwrap();
        assert_eq!(&decode_pitr_snapshot(&snapshot).unwrap(), log.state());
        log.append(PitrManifestRecord::ReconciliationComplete)
            .unwrap();
        assert_eq!(log.state().mode, PitrMode::Disabled);
        assert_eq!(log.state().active_boundary, None);
        assert_eq!(log.state().recovery_gap, Some(valid_gap));
    }

    #[test]
    fn active_wal_cannot_be_reclaimed_through_a_snapshot_obligation() {
        let first = empty_active_boundary_for_enabled_segment();
        let durable = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: 7,
            prefix_digest: [9; 32],
            ..first
        };
        let mut records = enable();
        records.extend([
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: first },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: durable },
        ]);
        let active = replay_pitr_records(records.clone()).unwrap();
        for state in [
            ObligationState::Sealed,
            ObligationState::Archived,
            ObligationState::Reclaimable,
        ] {
            let mut invalid = active.clone();
            invalid.next_segment_id = 3;
            invalid.obligations.insert(
                durable.segment_id,
                PitrObligation {
                    state,
                    successor_segment_id: 2,
                    logical_length: durable.durable_end + 4096,
                },
            );
            assert!(encode_pitr_snapshot(&invalid).is_err());
            let mut reclaim_records = vec![PitrManifestRecord::Snapshot(Box::new(invalid))];
            if state == ObligationState::Sealed {
                reclaim_records.push(PitrManifestRecord::SegmentArchived { segment_id: 1 });
            }
            if state != ObligationState::Reclaimable {
                reclaim_records.push(PitrManifestRecord::SegmentReclaimable { segment_id: 1 });
            }
            reclaim_records.push(PitrManifestRecord::SegmentReclaimed { segment_id: 1 });
            let encoded = encode_pitr_record_stream(&reclaim_records).unwrap();
            assert!(PitrManifestLog::recover(&encoded).is_err());
        }

        records.extend([
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: durable.durable_end + 4096,
            },
            PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                after: PersistedChainAnchor::Genesis {
                    archive_epoch_id: [3; 16],
                },
                last_archived_commit_ts: None,
                first_uncovered_commit_ts: Some(durable.last_commit_ts),
                reason: CoverageBreakReason::ForcedDisable,
            }),
        ]);
        let abandoned = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(abandoned.obligations[&1].state, ObligationState::Abandoned);
        assert_eq!(
            decode_pitr_snapshot(&encode_pitr_snapshot(&abandoned).unwrap()).unwrap(),
            abandoned
        );
        records.extend([
            PitrManifestRecord::SegmentAbandoned { segment_id: 1 },
            PitrManifestRecord::ReconciliationComplete,
        ]);
        let disabled = replay_pitr_records(records).unwrap();
        assert_eq!(disabled.mode, PitrMode::Disabled);
        assert_eq!(disabled.active_boundary, None);
    }

    #[test]
    fn duplicate_enable_completion_is_idempotent() {
        let mut records = enable();
        records.push(PitrManifestRecord::EnableComplete {
            active_segment_id: 1,
        });
        let state = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(state.mode, PitrMode::Enabled);
        assert_eq!(state.active_segment_id, Some(1));

        records.push(PitrManifestRecord::EnableComplete {
            active_segment_id: 2,
        });
        assert!(replay_pitr_records(records).is_err());
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
        let disabled = replay_pitr_records(records).unwrap();
        assert_eq!(disabled.mode, PitrMode::Disabled);
        assert_eq!(disabled.database_timeline_id, Some([2; 16]));
        assert_eq!(disabled.archive_epoch_id, Some([3; 16]));
    }

    #[test]
    fn snapshot_and_transition_validation_fail_closed() {
        let enabled = replay_pitr_records(enable()).unwrap();
        let bytes = encode_pitr_snapshot(&enabled).unwrap();
        let decoded = decode_pitr_snapshot(&bytes).unwrap();
        assert_eq!(decoded, enabled);
        let mut corrupt = bytes.clone();
        corrupt[13] ^= 1;
        assert!(decode_pitr_snapshot(&corrupt).is_err());
        assert!(decode_pitr_snapshot(&vec![b' '; MAX_PITR_SNAPSHOT_BYTES + 1]).is_err());
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

    #[test]
    fn manifest_record_envelope_round_trips_and_rejects_corruption() {
        let record = PitrManifestRecord::EnableComplete {
            active_segment_id: 9,
        };
        let encoded = encode_pitr_record(&record).unwrap();
        assert_eq!(decode_pitr_record(&encoded).unwrap(), record);
        let mut corrupt = encoded;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(decode_pitr_record(&corrupt).is_err());
    }

    #[test]
    fn manifest_record_stream_replays_in_order_and_rejects_truncation() {
        let records = vec![
            PitrManifestRecord::EnableIntent {
                repository_id: [1; 16],
                timeline_id: [2; 16],
                archive_epoch_id: [3; 16],
                config: PersistedPitrConfig {
                    archive_interval_ms: 1000,
                    max_segment_bytes: 4096,
                    max_unarchived_bytes: 8192,
                    max_source_spool_bytes: 16384,
                },
            },
            PitrManifestRecord::EnableComplete {
                active_segment_id: 0,
            },
        ];
        let encoded = encode_pitr_record_stream(&records).unwrap();
        assert_eq!(decode_pitr_record_stream(&encoded).unwrap(), records);
        assert_eq!(
            replay_pitr_record_stream(&encoded).unwrap().mode,
            PitrMode::Enabled
        );
        assert!(decode_pitr_record_stream(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn manifest_log_append_is_transactional_and_recoverable() {
        let mut log = PitrManifestLog::default();
        let intent = PitrManifestRecord::EnableIntent {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            config: PersistedPitrConfig {
                archive_interval_ms: 1000,
                max_segment_bytes: 4096,
                max_unarchived_bytes: 8192,
                max_source_spool_bytes: 16384,
            },
        };
        log.append(intent.clone()).unwrap();
        let before = log.encoded().to_vec();
        assert!(
            log.append(PitrManifestRecord::SegmentArchived { segment_id: 9 })
                .is_err()
        );
        assert_eq!(log.encoded(), before);
        log.append(PitrManifestRecord::EnableComplete {
            active_segment_id: 0,
        })
        .unwrap();
        let recovered = PitrManifestLog::recover(log.encoded()).unwrap();
        assert_eq!(recovered.records(), log.records());
        assert_eq!(recovered.state(), log.state());
    }

    #[test]
    fn repeated_reenable_has_no_lifetime_epoch_cap() {
        let mut records = Vec::new();
        for epoch in 1..=128 {
            let mut archive_epoch_id = [0; 16];
            archive_epoch_id[..8].copy_from_slice(&(epoch as u64).to_le_bytes());
            records.extend([
                PitrManifestRecord::EnableIntent {
                    repository_id: [1; 16],
                    timeline_id: [2; 16],
                    archive_epoch_id,
                    config: config(),
                },
                PitrManifestRecord::EnableComplete {
                    active_segment_id: epoch as u64,
                },
                PitrManifestRecord::DisableClean,
            ]);
        }
        let disabled = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(disabled.repository_id, Some([1; 16]));
        assert_eq!(disabled.timeline_id, Some([2; 16]));
        let mut last_epoch = [0; 16];
        last_epoch[..8].copy_from_slice(&128_u64.to_le_bytes());
        assert_eq!(disabled.archive_epoch_id, Some(last_epoch));
        assert!(disabled.config.is_some());
        assert!(disabled.predecessor_anchor.is_some());
    }

    #[test]
    fn disabled_snapshot_validates_retained_terminal_state() {
        let mut clean_records = enable();
        clean_records.push(PitrManifestRecord::DisableClean);
        let mut invalid_config = replay_pitr_records(clean_records).unwrap();
        invalid_config.config.as_mut().unwrap().archive_interval_ms = 0;
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(invalid_config))]).is_err()
        );

        let mut gap_records = enable();
        gap_records.extend([
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
        let mut invalid_gap = replay_pitr_records(gap_records).unwrap();
        invalid_gap.recovery_gap.as_mut().unwrap().timeline_id = [9; 16];
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(invalid_gap))]).is_err()
        );
    }
}
