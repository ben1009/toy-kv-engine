//! Persisted PITR manifest state and replay reducer.
//!
//! This module models v7 crash boundaries without participating in the live
//! v6 manifest. Later slices will embed these records in `ManifestRecord`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const LEGACY_PITR_MANIFEST_FORMAT_VERSION: u32 = 7;
// Version 8 makes the obligation WAL-format marker mandatory in persisted state.
pub(crate) const PITR_MANIFEST_FORMAT_VERSION: u32 = 8;
pub(crate) const MAX_PITR_SNAPSHOT_BYTES: usize = 1 << 20;
pub(crate) const MAX_PITR_RECORD_STREAM_BYTES: usize = 4 << 20;
const PITR_SNAPSHOT_MAGIC: &[u8; 5] = b"PITR7";
const PITR_SNAPSHOT_HEADER_LEN: usize = 5 + 4 + 4 + 32;
const PITR_RECORD_MAGIC: &[u8; 5] = b"PITRr";
const PITR_RECORD_HEADER_LEN: usize = 5 + 4 + 4 + 32;
const PRE_V8_OBLIGATION_UPGRADE_GUIDANCE: &str = concat!(
    "reopen with a v7-compatible build and drain seal, archive, and reclaim work, ",
    "publish a compacted canonical snapshot, then retry the upgrade"
);

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PitrWalFormat {
    #[default]
    Unspecified,
    LegacyV5V6,
    V7,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PitrObligation {
    pub(crate) state: ObligationState,
    pub(crate) successor_segment_id: u64,
    pub(crate) logical_length: u64,
    /// Missing in legacy snapshots written before v7 obligations existed.
    /// It remains unspecified until the envelope version selects legacy
    /// snapshot migration; current snapshots must persist an explicit format.
    #[serde(default)]
    pub(crate) wal_format: PitrWalFormat,
    /// Frozen v7 image authority captured before successor installation.
    /// Legacy v5/v6 obligations do not have this boundary.
    #[serde(default)]
    pub(crate) immutable_boundary: Option<ImmutableBoundary>,
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
                let immutable = obligation.immutable_boundary.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Active v7 sealing obligation is missing its immutable boundary"
                    )
                })?;
                immutable.validate()?;
                ensure!(
                    immutable.active == boundary
                        && immutable.sealed_end == obligation.logical_length,
                    "Active v7 sealing obligation disagrees with its durable boundary"
                );
                validate_active_sealing_length(boundary, obligation.logical_length)?;
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
        let mut previous_immutable_commit_ts = None;

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
            ensure!(
                obligation.wal_format != PitrWalFormat::Unspecified,
                "PITR obligation is missing its WAL format marker"
            );
            ensure!(
                (obligation.wal_format == PitrWalFormat::V7)
                    == obligation.immutable_boundary.is_some(),
                "PITR obligation format and immutable boundary disagree"
            );
            if let Some(immutable) = obligation.immutable_boundary {
                immutable.validate()?;
                ensure!(
                    Some(immutable.active.timeline_id) == self.timeline_id
                        && Some(immutable.active.archive_epoch_id) == self.archive_epoch_id,
                    "immutable boundary does not match its PITR timeline or archive epoch"
                );
                ensure!(
                    immutable.active.segment_id == segment
                        && immutable.sealed_end == obligation.logical_length,
                    "immutable boundary does not match its PITR obligation"
                );
                if let Some(PersistedChainAnchor::Segment {
                    segment_id: anchor_segment_id,
                    wal_digest,
                    ..
                }) = self.predecessor_anchor
                    && anchor_segment_id == segment
                {
                    ensure!(
                        immutable.wal_digest == wal_digest,
                        "immutable WAL digest does not match the predecessor anchor"
                    );
                }
                let is_unsealed = obligation.state == ObligationState::Sealing
                    || (obligation.state == ObligationState::Abandoned
                        && self.active_segment_id == Some(segment));
                if is_unsealed {
                    ensure!(
                        self.active_boundary == Some(immutable.active),
                        "unsealed obligation does not match the Active v7 boundary"
                    );
                    validate_active_sealing_length(immutable.active, immutable.sealed_end)?;
                } else {
                    if immutable.active.ticket_end != 0 {
                        // Snapshots must preserve the same strict timestamp
                        // advancement that SegmentSealed event replay enforces.
                        ensure!(
                            previous_immutable_commit_ts
                                .is_none_or(|previous| immutable.active.last_commit_ts > previous),
                            "immutable WAL commit boundaries are not strictly increasing"
                        );
                        previous_immutable_commit_ts = Some(immutable.active.last_commit_ts);
                        ensure!(
                            self.last_commit_anchor.is_some_and(|anchor| {
                                anchor.segment_id >= segment
                                    && if anchor.segment_id == segment {
                                        anchor.commit_ts >= immutable.active.last_commit_ts
                                    } else {
                                        anchor.commit_ts > immutable.active.last_commit_ts
                                    }
                            }),
                            "immutable boundary exceeds the sealed commit high-water"
                        );
                    }
                    ensure!(
                        self.last_commit_anchor.is_none_or(|anchor| {
                            anchor.segment_id != segment
                                || (immutable.active.ticket_end != 0
                                    && anchor.commit_ts == immutable.active.last_commit_ts)
                        }),
                        "sealed commit anchor does not match its immutable WAL boundary"
                    );
                }
            }
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
        /// Present for v7 and absent for legacy v5/v6 seals.
        #[serde(default)]
        immutable_boundary: Option<ImmutableBoundary>,
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
                immutable_boundary,
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
                match (state.active_boundary, immutable_boundary) {
                    (Some(active), Some(immutable)) => {
                        immutable.validate()?;
                        ensure!(
                            immutable.active == active
                                && immutable.active.segment_id == segment_id
                                && immutable.sealed_end == logical_length,
                            "v7 sealing intent disagrees with the Active manifest boundary"
                        );
                        validate_active_sealing_length(active, immutable.sealed_end)?;
                    }
                    (Some(_), None) => {
                        anyhow::bail!(
                            "pre-v8 Active v7 sealing intent is missing its immutable boundary; {PRE_V8_OBLIGATION_UPGRADE_GUIDANCE}"
                        )
                    }
                    (None, Some(_)) => anyhow::bail!(
                        "legacy PITR sealing intent cannot carry a v7 immutable boundary"
                    ),
                    (None, None) => {}
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
                        wal_format: if immutable_boundary.is_some() {
                            PitrWalFormat::V7
                        } else {
                            PitrWalFormat::LegacyV5V6
                        },
                        immutable_boundary,
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
                if let Some(immutable) = obligation.immutable_boundary {
                    ensure!(
                        matches!(
                            segment_anchor,
                            PersistedChainAnchor::Segment { wal_digest, .. }
                                if wal_digest == immutable.wal_digest
                        ),
                        "sealed chain anchor does not match the immutable WAL digest"
                    );
                    if immutable.active.ticket_end == 0 {
                        ensure!(
                            last_commit_anchor.is_none() && last_recorded_at.is_none(),
                            "empty immutable WAL cannot carry sealed commit metadata"
                        );
                    } else {
                        ensure!(
                            last_commit_anchor.is_some_and(|anchor| {
                                anchor.segment_id == segment_id
                                    && anchor.commit_ts == immutable.active.last_commit_ts
                            }),
                            "sealed commit anchor does not match its immutable WAL boundary"
                        );
                    }
                }
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
    let digest = pitr_snapshot_digest(PITR_MANIFEST_FORMAT_VERSION, payload_len, &payload);
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
    let format_version = u32::from_be_bytes(bytes[5..9].try_into().unwrap());
    ensure!(
        matches!(
            format_version,
            LEGACY_PITR_MANIFEST_FORMAT_VERSION | PITR_MANIFEST_FORMAT_VERSION
        ),
        "unsupported PITR snapshot version"
    );
    let payload_len_u32 = u32::from_be_bytes(bytes[9..13].try_into().unwrap());
    let payload_len = usize::try_from(payload_len_u32)
        .map_err(|_| anyhow::anyhow!("PITR snapshot payload length is invalid"))?;
    ensure!(
        payload_len == bytes.len() - PITR_SNAPSHOT_HEADER_LEN,
        "PITR snapshot payload length does not match envelope"
    );
    let payload = &bytes[PITR_SNAPSHOT_HEADER_LEN..];
    ensure!(
        pitr_snapshot_digest(format_version, payload_len_u32, payload).as_slice()
            == &bytes[13..PITR_SNAPSHOT_HEADER_LEN],
        "PITR snapshot digest mismatch"
    );
    let mut state: PitrState = serde_json::from_slice(payload)?;
    if format_version == LEGACY_PITR_MANIFEST_FORMAT_VERSION {
        migrate_legacy_snapshot_obligations(&mut state)?;
    }
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
    let digest = pitr_record_digest(PITR_MANIFEST_FORMAT_VERSION, payload_len, &payload);
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
    let format_version = u32::from_be_bytes(bytes[5..9].try_into().unwrap());
    ensure!(
        matches!(
            format_version,
            LEGACY_PITR_MANIFEST_FORMAT_VERSION | PITR_MANIFEST_FORMAT_VERSION
        ),
        "unsupported PITR manifest record version"
    );
    let payload_len_u32 = u32::from_be_bytes(bytes[9..13].try_into().unwrap());
    let payload_len = usize::try_from(payload_len_u32)
        .map_err(|_| anyhow::anyhow!("PITR manifest record payload length is invalid"))?;
    ensure!(
        payload_len == bytes.len() - PITR_RECORD_HEADER_LEN,
        "PITR manifest record payload length does not match envelope"
    );
    let payload = &bytes[PITR_RECORD_HEADER_LEN..];
    ensure!(
        pitr_record_digest(format_version, payload_len_u32, payload).as_slice()
            == &bytes[13..PITR_RECORD_HEADER_LEN],
        "PITR manifest record digest mismatch"
    );

    let mut record: PitrManifestRecord = serde_json::from_slice(payload)?;
    if let PitrManifestRecord::Snapshot(state) = &mut record {
        if format_version == LEGACY_PITR_MANIFEST_FORMAT_VERSION {
            migrate_legacy_snapshot_obligations(state)?;
        }
        state.validate()?;
    }

    Ok(record)
}

/// Migrate a snapshot selected by a pre-v8 persisted manifest version.
/// Current snapshots must validate without this compatibility inference.
pub(crate) fn migrate_legacy_snapshot_obligations(state: &mut PitrState) -> Result<()> {
    for (&segment_id, obligation) in &mut state.obligations {
        if obligation.wal_format == PitrWalFormat::Unspecified {
            match (
                obligation.immutable_boundary,
                state.active_boundary,
                obligation.state,
            ) {
                (Some(_), _, _) => obligation.wal_format = PitrWalFormat::V7,
                (None, None, ObligationState::Sealing) => {
                    obligation.wal_format = PitrWalFormat::LegacyV5V6;
                }
                (None, _, _) => anyhow::bail!(
                    "pre-v8 PITR snapshot has an ambiguous {:?} obligation for segment {segment_id}; {PRE_V8_OBLIGATION_UPGRADE_GUIDANCE}",
                    obligation.state,
                ),
            }
        }
    }
    state.validate()
}

fn pitr_snapshot_digest(format_version: u32, payload_len: u32, payload: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    if format_version == LEGACY_PITR_MANIFEST_FORMAT_VERSION {
        digest.update(b"TOYKV-PITR-SNAPSHOT-V1");
    } else {
        digest.update(b"TOYKV-PITR-SNAPSHOT-V2");
        digest.update(format_version.to_be_bytes());
    }
    digest.update(payload_len.to_be_bytes());
    digest.update(payload);
    digest.finalize().into()
}

fn pitr_record_digest(format_version: u32, payload_len: u32, payload: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    if format_version == LEGACY_PITR_MANIFEST_FORMAT_VERSION {
        digest.update(b"TOYKV-PITR-RECORD-V1");
    } else {
        digest.update(b"TOYKV-PITR-RECORD-V2");
        digest.update(format_version.to_be_bytes());
    }
    digest.update(payload_len.to_be_bytes());
    digest.update(payload);
    digest.finalize().into()
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

    fn snapshot_fixture(format_version: u32, payload: &[u8]) -> Vec<u8> {
        let payload_len = u32::try_from(payload.len()).unwrap();
        let digest = pitr_snapshot_digest(format_version, payload_len, payload);
        let mut encoded = Vec::with_capacity(PITR_SNAPSHOT_HEADER_LEN + payload.len());
        encoded.extend_from_slice(PITR_SNAPSHOT_MAGIC);
        encoded.extend_from_slice(&format_version.to_be_bytes());
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(&digest);
        encoded.extend_from_slice(payload);
        encoded
    }

    fn record_fixture(format_version: u32, payload: &[u8]) -> Vec<u8> {
        let payload_len = u32::try_from(payload.len()).unwrap();
        let digest = pitr_record_digest(format_version, payload_len, payload);
        let mut encoded = Vec::with_capacity(PITR_RECORD_HEADER_LEN + payload.len());
        encoded.extend_from_slice(PITR_RECORD_MAGIC);
        encoded.extend_from_slice(&format_version.to_be_bytes());
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(&digest);
        encoded.extend_from_slice(payload);
        encoded
    }

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

        let mut legacy_records = enable();
        legacy_records.push(PitrManifestRecord::SealStarted {
            segment_id: 1,
            successor_segment_id: 2,
            logical_length: 4096,
            immutable_boundary: None,
        });
        let legacy_state = replay_pitr_records(legacy_records).unwrap();
        let mut legacy_snapshot = serde_json::to_value(&legacy_state).unwrap();
        legacy_snapshot["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("immutable_boundary");
        legacy_snapshot["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("wal_format");
        let old_snapshot_payload = serde_json::to_vec(&legacy_snapshot).unwrap();
        assert_eq!(
            decode_pitr_snapshot(&snapshot_fixture(
                LEGACY_PITR_MANIFEST_FORMAT_VERSION,
                &old_snapshot_payload
            ))
            .unwrap(),
            legacy_state
        );
        let old_snapshot_record = serde_json::to_vec(&serde_json::json!({
            "Snapshot": legacy_snapshot
        }))
        .unwrap();
        assert_eq!(
            decode_pitr_record(&record_fixture(
                LEGACY_PITR_MANIFEST_FORMAT_VERSION,
                &old_snapshot_record
            ))
            .unwrap(),
            PitrManifestRecord::Snapshot(Box::new(legacy_state))
        );

        let mut old_sealed_records = enable();
        old_sealed_records.extend([
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: 4096,
                immutable_boundary: None,
            },
            PitrManifestRecord::SegmentSealed {
                segment_id: 1,
                segment_anchor: anchor(1),
                last_recorded_at: None,
                last_commit_anchor: None,
            },
        ]);
        let old_sealed_state = replay_pitr_records(old_sealed_records).unwrap();
        let mut ambiguous_snapshot = serde_json::to_value(&old_sealed_state).unwrap();
        ambiguous_snapshot["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("immutable_boundary");
        ambiguous_snapshot["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("wal_format");
        let ambiguous_payload = serde_json::to_vec(&ambiguous_snapshot).unwrap();
        let migration_error = decode_pitr_snapshot(&snapshot_fixture(
            LEGACY_PITR_MANIFEST_FORMAT_VERSION,
            &ambiguous_payload,
        ))
        .unwrap_err();
        assert!(
            migration_error
                .to_string()
                .contains("ambiguous Sealed obligation")
        );

        let old_seal_record = serde_json::json!({
            "SealStarted": {
                "segment_id": 1,
                "successor_segment_id": 2,
                "logical_length": 4096
            }
        });
        assert_eq!(
            serde_json::from_value::<PitrManifestRecord>(old_seal_record).unwrap(),
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: 4096,
                immutable_boundary: None,
            }
        );
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

    fn immutable_boundary(active: ActiveBoundary, sealed_end: u64) -> ImmutableBoundary {
        ImmutableBoundary {
            active,
            sealed_end,
            terminal_frontier_digest: [7; 32],
            wal_digest: [4; 32],
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
            immutable_boundary: Some(immutable_boundary(active_boundary, 8192)),
        });
        let sealing = replay_pitr_records(records.clone()).unwrap();
        assert_eq!(sealing.active_segment_id, Some(1));
        assert_eq!(sealing.active_boundary, Some(active_boundary));
        assert_eq!(
            decode_pitr_snapshot(&encode_pitr_snapshot(&sealing).unwrap()).unwrap(),
            sealing
        );
        let mut old_incomplete_seal = serde_json::to_value(&sealing).unwrap();
        old_incomplete_seal["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("immutable_boundary");
        old_incomplete_seal["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("wal_format");
        let old_incomplete_payload = serde_json::to_vec(&old_incomplete_seal).unwrap();
        assert!(
            decode_pitr_snapshot(&snapshot_fixture(
                LEGACY_PITR_MANIFEST_FORMAT_VERSION,
                &old_incomplete_payload
            ))
            .is_err()
        );

        let mut missing_boundary_snapshot = sealing.clone();
        missing_boundary_snapshot
            .obligations
            .get_mut(&1)
            .unwrap()
            .immutable_boundary = None;
        assert!(encode_pitr_snapshot(&missing_boundary_snapshot).is_err());
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(
                missing_boundary_snapshot
            ))])
            .is_err()
        );

        let mut missing_active_snapshot = sealing.clone();
        missing_active_snapshot.active_boundary = None;
        assert!(encode_pitr_snapshot(&missing_active_snapshot).is_err());
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(
                missing_active_snapshot
            ))])
            .is_err()
        );
        assert_eq!(
            sealing.obligations[&1].immutable_boundary,
            Some(immutable_boundary(active_boundary, 8192))
        );
        assert_eq!(sealing.obligations[&1].wal_format, PitrWalFormat::V7);

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
        assert_eq!(
            sealed.obligations[&1].immutable_boundary,
            Some(immutable_boundary(active_boundary, 8192))
        );
        assert_eq!(
            decode_pitr_snapshot(&encode_pitr_snapshot(&sealed).unwrap()).unwrap(),
            sealed
        );
        let mut omitted_format_fields = serde_json::to_value(&sealed).unwrap();
        omitted_format_fields["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("wal_format");
        omitted_format_fields["obligations"]["1"]
            .as_object_mut()
            .unwrap()
            .remove("immutable_boundary");
        let omitted_snapshot_payload = serde_json::to_vec(&omitted_format_fields).unwrap();
        assert!(
            decode_pitr_snapshot(&snapshot_fixture(
                PITR_MANIFEST_FORMAT_VERSION,
                &omitted_snapshot_payload
            ))
            .is_err()
        );
        let omitted_record_payload = serde_json::to_vec(&serde_json::json!({
            "Snapshot": omitted_format_fields
        }))
        .unwrap();
        assert!(
            decode_pitr_record(&record_fixture(
                PITR_MANIFEST_FORMAT_VERSION,
                &omitted_record_payload
            ))
            .is_err()
        );
        let mut missing_immutable_boundary = sealed.clone();
        missing_immutable_boundary
            .obligations
            .get_mut(&1)
            .unwrap()
            .immutable_boundary = None;
        assert!(encode_pitr_snapshot(&missing_immutable_boundary).is_err());
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(
                missing_immutable_boundary
            ))])
            .is_err()
        );
    }

    fn terminal_v7_immutable_states(nonempty: bool) -> Vec<PitrState> {
        let active = empty_active_boundary_for_enabled_segment();
        let durable = if nonempty {
            ActiveBoundary {
                ticket_end: 1,
                durable_end: 3 * 4096,
                last_commit_ts: 7,
                prefix_digest: [9; 32],
                ..active
            }
        } else {
            active
        };
        let commit = nonempty.then_some(PersistedCommitAnchor {
            segment_id: 1,
            commit_ts: 7,
            recorded_at: PersistedRecordedAt { secs: 1, nanos: 0 },
            entry_digest: [8; 32],
        });
        let logical_length = durable.durable_end + 4096;
        let mut records = enable();
        records.push(PitrManifestRecord::ActiveBoundaryAdvanced { boundary: active });
        if nonempty {
            records.push(PitrManifestRecord::ActiveBoundaryAdvanced { boundary: durable });
        }
        records.extend([
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length,
                immutable_boundary: Some(immutable_boundary(durable, logical_length)),
            },
            PitrManifestRecord::SegmentSealed {
                segment_id: 1,
                segment_anchor: anchor(1),
                last_recorded_at: commit.map(|anchor| anchor.recorded_at),
                last_commit_anchor: commit,
            },
        ]);
        let mut states = vec![replay_pitr_records(records.clone()).unwrap()];
        records.push(PitrManifestRecord::SegmentArchived { segment_id: 1 });
        states.push(replay_pitr_records(records.clone()).unwrap());
        records.push(PitrManifestRecord::SegmentReclaimable { segment_id: 1 });
        states.push(replay_pitr_records(records).unwrap());
        states.push(
            replay_pitr_records([
                PitrManifestRecord::Snapshot(Box::new(states[0].clone())),
                PitrManifestRecord::CoverageGap(PersistedRecoveryGap {
                    repository_id: [1; 16],
                    timeline_id: [2; 16],
                    archive_epoch_id: [3; 16],
                    after: anchor(1),
                    last_archived_commit_ts: None,
                    first_uncovered_commit_ts: None,
                    reason: CoverageBreakReason::ForcedDisable,
                }),
            ])
            .unwrap(),
        );

        states
    }

    #[test]
    fn immutable_obligations_reject_foreign_timeline_and_archive_epoch() {
        for state in [false, true]
            .into_iter()
            .flat_map(terminal_v7_immutable_states)
        {
            assert!(state.active_boundary.is_none());
            assert_eq!(
                decode_pitr_snapshot(&encode_pitr_snapshot(&state).unwrap()).unwrap(),
                state
            );
            for field in ["timeline_id", "archive_epoch_id"] {
                let mut corrupt = serde_json::to_value(&state).unwrap();
                corrupt["obligations"]["1"]["immutable_boundary"]["active"][field] =
                    serde_json::to_value([9_u8; 16]).unwrap();
                let payload = serde_json::to_vec(&corrupt).unwrap();
                let error =
                    decode_pitr_snapshot(&snapshot_fixture(PITR_MANIFEST_FORMAT_VERSION, &payload))
                        .unwrap_err();
                assert!(error.to_string().contains("timeline or archive epoch"));
                let record_payload =
                    serde_json::to_vec(&serde_json::json!({"Snapshot": corrupt.clone()})).unwrap();
                assert!(
                    decode_pitr_record(&record_fixture(
                        PITR_MANIFEST_FORMAT_VERSION,
                        &record_payload
                    ))
                    .is_err()
                );
                let corrupt: PitrState = serde_json::from_value(corrupt).unwrap();
                assert!(encode_pitr_snapshot(&corrupt).is_err());
                assert!(
                    replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(corrupt))]).is_err()
                );
            }
        }
    }

    #[test]
    fn terminal_immutable_obligations_preserve_the_sealed_commit_high_water() {
        for state in terminal_v7_immutable_states(true) {
            let mut beyond_high_water = state.clone();
            beyond_high_water
                .obligations
                .get_mut(&1)
                .unwrap()
                .immutable_boundary
                .as_mut()
                .unwrap()
                .active
                .last_commit_ts += 1;
            let mut missing_high_water = state.clone();
            missing_high_water.last_commit_anchor = None;
            missing_high_water.last_recorded_at = None;
            let mut unproven_commit = state.clone();
            unproven_commit
                .last_commit_anchor
                .as_mut()
                .unwrap()
                .commit_ts += 1;
            let mut preceding_segment = state;
            preceding_segment
                .last_commit_anchor
                .as_mut()
                .unwrap()
                .segment_id = 0;
            for (corrupt, error_text) in [
                (beyond_high_water, "sealed commit high-water"),
                (missing_high_water, "sealed commit high-water"),
                (preceding_segment, "sealed commit high-water"),
                (unproven_commit, "immutable WAL boundary"),
            ] {
                let payload = serde_json::to_vec(&corrupt).unwrap();
                let error =
                    decode_pitr_snapshot(&snapshot_fixture(PITR_MANIFEST_FORMAT_VERSION, &payload))
                        .unwrap_err();
                assert!(error.to_string().contains(error_text));
                let record_payload =
                    serde_json::to_vec(&PitrManifestRecord::Snapshot(Box::new(corrupt.clone())))
                        .unwrap();
                assert!(
                    decode_pitr_record(&record_fixture(
                        PITR_MANIFEST_FORMAT_VERSION,
                        &record_payload
                    ))
                    .is_err()
                );
                assert!(encode_pitr_snapshot(&corrupt).is_err());
                assert!(
                    replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(corrupt))]).is_err()
                );
            }
        }
    }

    #[test]
    fn empty_immutable_segments_cannot_own_commit_metadata() {
        let commit = PersistedCommitAnchor {
            segment_id: 1,
            commit_ts: 1,
            recorded_at: PersistedRecordedAt { secs: 1, nanos: 0 },
            entry_digest: [8; 32],
        };
        for mut state in terminal_v7_immutable_states(false) {
            state.last_commit_anchor = Some(commit);
            state.last_recorded_at = Some(commit.recorded_at);
            let payload = serde_json::to_vec(&state).unwrap();
            let error =
                decode_pitr_snapshot(&snapshot_fixture(PITR_MANIFEST_FORMAT_VERSION, &payload))
                    .unwrap_err();
            assert!(error.to_string().contains("immutable WAL boundary"));
            let record_payload =
                serde_json::to_vec(&PitrManifestRecord::Snapshot(Box::new(state.clone()))).unwrap();
            assert!(
                decode_pitr_record(&record_fixture(
                    PITR_MANIFEST_FORMAT_VERSION,
                    &record_payload
                ))
                .is_err()
            );
            assert!(encode_pitr_snapshot(&state).is_err());
            assert!(replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(state))]).is_err());
        }

        let active = empty_active_boundary_for_enabled_segment();
        let mut records = enable();
        records.extend([
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: active },
            PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: 8192,
                immutable_boundary: Some(immutable_boundary(active, 8192)),
            },
        ]);
        for last_commit_anchor in [None, Some(commit)] {
            let error = replay_pitr_records(records.iter().cloned().chain([
                PitrManifestRecord::SegmentSealed {
                    segment_id: 1,
                    segment_anchor: anchor(1),
                    last_recorded_at: Some(commit.recorded_at),
                    last_commit_anchor,
                },
            ]))
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("empty immutable WAL cannot carry sealed commit metadata")
            );
        }
    }

    fn seal_next_v7_segment(previous: PitrState, commit_ts: u64) -> PitrState {
        let segment_id = previous.active_segment_id.unwrap();
        let initial = ActiveBoundary {
            segment_id,
            incarnation: [5; 16],
            ..empty_active_boundary_for_enabled_segment()
        };
        let durable = ActiveBoundary {
            ticket_end: 1,
            durable_end: 3 * 4096,
            last_commit_ts: commit_ts,
            prefix_digest: [10; 32],
            ..initial
        };
        let commit = PersistedCommitAnchor {
            segment_id,
            commit_ts,
            recorded_at: PersistedRecordedAt {
                secs: segment_id as i64,
                nanos: 0,
            },
            entry_digest: [9; 32],
        };
        replay_pitr_records([
            PitrManifestRecord::Snapshot(Box::new(previous)),
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: initial },
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: durable },
            PitrManifestRecord::SealStarted {
                segment_id,
                successor_segment_id: segment_id + 1,
                logical_length: 4 * 4096,
                immutable_boundary: Some(immutable_boundary(durable, 4 * 4096)),
            },
            PitrManifestRecord::SegmentSealed {
                segment_id,
                segment_anchor: anchor(segment_id),
                last_recorded_at: Some(commit.recorded_at),
                last_commit_anchor: Some(commit),
            },
        ])
        .unwrap()
    }

    #[test]
    fn later_segments_can_advance_the_global_commit_anchor() {
        for nonempty in [false, true] {
            let previous = terminal_v7_immutable_states(nonempty).remove(0);
            let state = seal_next_v7_segment(previous, 8);
            assert_eq!(state.last_commit_anchor.unwrap().commit_ts, 8);
            assert_eq!(
                decode_pitr_snapshot(&encode_pitr_snapshot(&state).unwrap()).unwrap(),
                state
            );
        }
    }

    #[test]
    fn retained_immutable_commit_timestamps_strictly_increase_between_segments() {
        let first = terminal_v7_immutable_states(true).remove(0);
        let second = seal_next_v7_segment(first, 8);
        let third = seal_next_v7_segment(second.clone(), 10);
        assert_eq!(
            decode_pitr_snapshot(&encode_pitr_snapshot(&third).unwrap()).unwrap(),
            third
        );

        // The earlier boundary cannot equal the later global anchor, nor can
        // retained boundaries repeat or regress beneath a still higher anchor.
        for (mut corrupt, first_commit_ts) in [(second, 8), (third.clone(), 8), (third, 9)] {
            corrupt
                .obligations
                .get_mut(&1)
                .unwrap()
                .immutable_boundary
                .as_mut()
                .unwrap()
                .active
                .last_commit_ts = first_commit_ts;
            let payload = serde_json::to_vec(&corrupt).unwrap();
            let record_payload =
                serde_json::to_vec(&PitrManifestRecord::Snapshot(Box::new(corrupt.clone())))
                    .unwrap();
            for version in [
                LEGACY_PITR_MANIFEST_FORMAT_VERSION,
                PITR_MANIFEST_FORMAT_VERSION,
            ] {
                assert!(decode_pitr_snapshot(&snapshot_fixture(version, &payload)).is_err());
                assert!(decode_pitr_record(&record_fixture(version, &record_payload)).is_err());
            }
            assert!(encode_pitr_snapshot(&corrupt).is_err());
            assert!(
                replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(corrupt))]).is_err()
            );
        }
    }

    #[test]
    fn v7_sealing_intent_requires_a_matching_immutable_boundary() {
        let active = empty_active_boundary_for_enabled_segment();
        let prefix = [
            enable()[0].clone(),
            enable()[1].clone(),
            PitrManifestRecord::ActiveBoundaryAdvanced { boundary: active },
        ];

        let invalid_boundaries = [
            None,
            Some(immutable_boundary(
                ActiveBoundary {
                    incarnation: [9; 16],
                    ..active
                },
                8192,
            )),
            Some(immutable_boundary(active, 12288)),
        ];
        for immutable_boundary in invalid_boundaries {
            let records = prefix
                .iter()
                .cloned()
                .chain([PitrManifestRecord::SealStarted {
                    segment_id: 1,
                    successor_segment_id: 2,
                    logical_length: 8192,
                    immutable_boundary,
                }]);
            assert!(replay_pitr_records(records).is_err());
        }

        let missing_boundary_records =
            prefix
                .iter()
                .cloned()
                .chain([PitrManifestRecord::SealStarted {
                    segment_id: 1,
                    successor_segment_id: 2,
                    logical_length: 8192,
                    immutable_boundary: None,
                }]);
        let error = replay_pitr_records(missing_boundary_records).unwrap_err();
        assert!(error.to_string().contains("v7-compatible build"));
        assert!(
            error
                .to_string()
                .contains("drain seal, archive, and reclaim work")
        );

        let legacy_event_records =
            prefix
                .iter()
                .cloned()
                .chain([PitrManifestRecord::SealStarted {
                    segment_id: 1,
                    successor_segment_id: 2,
                    logical_length: 8192,
                    immutable_boundary: None,
                }]);
        let mut legacy_event_stream = Vec::new();
        for record in legacy_event_records {
            let mut payload = serde_json::to_value(record).unwrap();
            if let Some(fields) = payload
                .get_mut("SealStarted")
                .and_then(serde_json::Value::as_object_mut)
            {
                fields.remove("immutable_boundary");
            }
            let payload = serde_json::to_vec(&payload).unwrap();
            let frame = record_fixture(LEGACY_PITR_MANIFEST_FORMAT_VERSION, &payload);
            legacy_event_stream
                .extend_from_slice(&u32::try_from(frame.len()).unwrap().to_be_bytes());
            legacy_event_stream.extend_from_slice(&frame);
        }
        let error = replay_pitr_record_stream(&legacy_event_stream).unwrap_err();
        assert!(error.to_string().contains("v7-compatible build"));
        assert!(
            error
                .to_string()
                .contains("drain seal, archive, and reclaim work")
        );

        let mismatched_length = prefix
            .iter()
            .cloned()
            .chain([PitrManifestRecord::SealStarted {
                segment_id: 1,
                successor_segment_id: 2,
                logical_length: 12288,
                immutable_boundary: Some(immutable_boundary(active, 8192)),
            }]);
        assert!(replay_pitr_records(mismatched_length).is_err());
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
                immutable_boundary: Some(immutable_boundary(durable, logical_length)),
            });
            assert!(replay_pitr_records(invalid_records).is_err());
        }

        records.push(PitrManifestRecord::SealStarted {
            segment_id: 1,
            successor_segment_id: 2,
            logical_length: 4 * 4096,
            immutable_boundary: Some(immutable_boundary(durable, 4 * 4096)),
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

        let mut invalid_anchor_records = records.clone();
        invalid_anchor_records.push(PitrManifestRecord::SegmentSealed {
            segment_id: 1,
            segment_anchor: PersistedChainAnchor::Segment {
                segment_id: 1,
                wal_digest: [8; 32],
                seal_digest: [5; 32],
            },
            last_recorded_at: Some(commit_anchor.recorded_at),
            last_commit_anchor: Some(commit_anchor),
        });
        assert!(replay_pitr_records(invalid_anchor_records).is_err());

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
            if commit_ts != durable.last_commit_ts {
                assert!(replay_pitr_records(sealed_records).is_err());
                continue;
            }
            let sealed = replay_pitr_records(sealed_records).unwrap();
            assert_eq!(sealed.active_boundary, None);
            assert_eq!(sealed.last_commit_anchor, Some(last_commit_anchor));
            assert_eq!(sealed.active_segment_id, Some(2));
            assert_eq!(
                sealed.obligations[&1].immutable_boundary,
                Some(immutable_boundary(durable, 4 * 4096))
            );
            assert_eq!(
                decode_pitr_snapshot(&encode_pitr_snapshot(&sealed).unwrap()).unwrap(),
                sealed
            );
            let mut mismatched_wal_digest = sealed.clone();
            mismatched_wal_digest
                .obligations
                .get_mut(&1)
                .unwrap()
                .immutable_boundary
                .as_mut()
                .unwrap()
                .wal_digest = [8; 32];
            assert!(encode_pitr_snapshot(&mismatched_wal_digest).is_err());
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
                immutable_boundary: None,
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
            let payload_len = u32::try_from(payload.len()).unwrap();
            let version = u32::from_be_bytes(snapshot_before[5..9].try_into().unwrap());
            let digest = pitr_snapshot_digest(version, payload_len, &payload);
            let payload_len = payload_len.to_be_bytes();
            let mut encoded_snapshot = snapshot_before[..PITR_SNAPSHOT_HEADER_LEN].to_vec();
            encoded_snapshot[9..13].copy_from_slice(&payload_len);
            encoded_snapshot[13..PITR_SNAPSHOT_HEADER_LEN].copy_from_slice(&digest);
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
                    wal_format: PitrWalFormat::LegacyV5V6,
                    immutable_boundary: None,
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
                immutable_boundary: Some(immutable_boundary(durable, durable.durable_end + 4096)),
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
        let mut missing_abandoned_floor = abandoned.clone();
        missing_abandoned_floor.active_boundary = None;
        assert!(encode_pitr_snapshot(&missing_abandoned_floor).is_err());
        assert!(
            replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(
                missing_abandoned_floor
            ))])
            .is_err()
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
                immutable_boundary: None,
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
