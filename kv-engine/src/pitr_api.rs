//! Public PITR contracts and validation.
//!
//! The state-machine implementations remain crate-private until the synchronous
//! lifecycle is wired into the engine. Keeping the public contracts here makes
//! their validation independently testable without exposing a partially live API.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::RangeInclusive,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use anyhow::{Result, ensure};

pub const MAX_STATUS_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const MAX_VERIFY_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const MAX_VERIFY_SAMPLED_TARGETS: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

#[derive(Clone, Debug)]
pub struct PitrOptions {
    pub repository: PathBuf,
    pub config: PersistedPitrConfig,
    pub runtime: PitrRuntimeOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedPitrConfig {
    pub archive_interval: Duration,
    pub max_segment_bytes: u64,
    pub max_unarchived_bytes: u64,
    pub max_source_spool_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PitrRuntimeOptions {
    pub archive_io_bytes_per_second: Option<NonZeroU64>,
    pub archive_burst_bytes: NonZeroU64,
    pub archive_io_priority: ArchiveIoPriority,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveIoPriority {
    Background,
    Normal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryTarget {
    Latest,
    CommitTs(u64),
    AtOrBeforeSystemTime(SystemTime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryPoint {
    pub commit_ts: Option<u64>,
    pub observed_at: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitTimeHighWater {
    pub archive_epoch_id: [u8; 16],
    pub segment_id: u64,
    pub commit_ts: u64,
    pub recorded_at: SystemTime,
    pub entry_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoverySelector {
    pub timeline_id: [u8; 16],
    pub archive_epoch_id: Option<[u8; 16]>,
    pub base_backup_id: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryInterval {
    pub repository_id: [u8; 16],
    pub timeline_id: [u8; 16],
    pub archive_epoch_id: [u8; 16],
    pub base_backup_id: u64,
    pub boundary: RecoveryChainAnchor,
    pub commit_bounds: Option<RangeInclusive<u64>>,
    pub recorded_time_bounds: Option<RangeInclusive<SystemTime>>,
    pub base_time_anchor: BaseTimeAnchor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryChainAnchor {
    Genesis { archive_epoch_id: [u8; 16] },
    Segment(SegmentAnchor),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentAnchor {
    pub segment_id: u64,
    pub wal_digest: [u8; 32],
    pub seal_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BaseTimeAnchor {
    Indexed {
        segment_id: u64,
        commit_ts: u64,
        recorded_at: SystemTime,
        entry_digest: [u8; 32],
    },
    ObservedBoundary {
        commit_ts: Option<u64>,
        observed_at: SystemTime,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PitrArchiveState {
    NeverEnabled,
    Disabled,
    Active,
    ReconciliationRequired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PitrStatus {
    pub state: PitrArchiveState,
    pub archive_epoch_id: Option<[u8; 16]>,
    pub latest_durable_commit_ts: Option<u64>,
    pub latest_archived_commit_ts: Option<u64>,
    pub recoverable_intervals: Vec<RecoveryInterval>,
    pub active_wal_bytes: u64,
    pub sealed_unarchived_wal_bytes: u64,
    pub source_spool_bytes: u64,
    pub archive_lag_commits: u64,
    pub archive_lag_bytes: u64,
    pub oldest_unarchived_recorded_at: Option<SystemTime>,
    pub archive_lag_duration: Option<Duration>,
    pub scheduler_delay: Duration,
    pub repository_staging_bytes: u64,
    pub repository_orphan_bytes: u64,
    pub next_cursor: Option<PitrStatusCursor>,
    pub last_archive_error: Option<PitrArchiveErrorSummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PitrStatusCursor {
    pub catalog_digest: [u8; 32],
    pub catalog_high_water: u64,
    pub interval_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PitrStatusOptions {
    pub cursor: Option<PitrStatusCursor>,
    pub page_size: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifyPitrDepth {
    Shallow,
    Deep { sampled_targets: NonZeroUsize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifyPitrOptions {
    pub depth: VerifyPitrDepth,
    pub selector: Option<RecoverySelector>,
    pub cursor: Option<VerifyPitrCursor>,
    pub page_size: NonZeroUsize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyPitrReport {
    pub verified_intervals: Vec<RecoveryInterval>,
    pub next_cursor: Option<VerifyPitrCursor>,
    pub first_failure: Option<SegmentFailureLocator>,
    pub last_verified_commit_ts: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifyPitrCursor {
    pub catalog_digest: [u8; 32],
    pub catalog_high_water: u64,
    pub query_digest: [u8; 32],
    pub interval_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreToInfo {
    pub requested_target: RecoveryTarget,
    pub resolved_commit_ts: Option<u64>,
    pub last_applied_commit_ts: Option<u64>,
    pub selected_interval: RecoveryInterval,
    pub replayed_segments: u64,
    pub replayed_batches: u64,
    pub replayed_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryIntervalPage {
    pub items: Vec<RecoveryInterval>,
    pub next_cursor: Option<PitrStatusCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryGap {
    pub repository_id: [u8; 16],
    pub timeline_id: [u8; 16],
    pub archive_epoch_id: [u8; 16],
    pub after: RecoveryChainAnchor,
    pub last_archived_commit_ts: Option<u64>,
    pub first_uncovered_commit_ts: Option<u64>,
    pub reason: CoverageBreakReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageBreakReason {
    SourceLost,
    RepositoryUnavailable,
    CorruptSegment,
    CleanupFailure,
    OperatorRequested,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PitrPurgeInfo {
    pub retained_interval_count: u64,
    pub planned_reclaim_segments: u64,
    pub planned_reclaim_bytes: u64,
    pub deleted_segments: Option<u64>,
    pub deleted_bytes: Option<u64>,
    pub oldest_recoverable_commit_ts: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PitrRetentionPolicy {
    pub minimum_window: Duration,
    pub retain_timelines: NonZeroUsize,
    pub retain_base_backups: NonZeroUsize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PitrArchiveErrorSummary {
    pub operation: PitrOperation,
    pub path: PathBuf,
    pub kind: PitrArchiveErrorKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PitrOperation {
    Archive,
    Verify,
    Reconcile,
    Reclaim,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PitrArchiveErrorKind {
    Io,
    Corruption,
    IdentityMismatch,
    Capacity,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentFailureLocator {
    pub expected_segment_id: Option<u64>,
    pub decoded_anchor: Option<SegmentAnchor>,
    pub catalog_sequence: Option<u64>,
    pub kind: SegmentFailureKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentFailureKind {
    Missing,
    Corrupt,
    WrongIdentity,
    Gap,
    Overlap,
    Unsupported,
}

#[derive(Debug)]
pub enum EnablePitrOutcome {
    Enabled {
        repository_id: [u8; 16],
        archive_epoch_id: [u8; 16],
    },
    RepositoryPublishedButNotDurable {
        repository: PathBuf,
        error: std::io::Error,
    },
    SourceManifestPublishedButNotDurable {
        archive_epoch_id: [u8; 16],
        error: std::io::Error,
    },
    PublicationUnknown {
        repository: PathBuf,
        repository_id: Option<[u8; 16]>,
        archive_epoch_id: Option<[u8; 16]>,
        request_id: [u8; 16],
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

#[derive(Debug)]
pub enum PitrResumeOutcome {
    Resumed,
    ReconciliationRequired(PitrArchiveError),
}

#[derive(Debug)]
pub enum RecoveryPointOutcome {
    Durable(RecoveryPoint),
    CommitPublishedButNotDurable {
        point: RecoveryPoint,
        error: std::io::Error,
    },
    PublicationUnknown {
        point: RecoveryPoint,
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

#[derive(Debug)]
pub enum DisablePitrOutcome {
    Disabled {
        final_point: Option<RecoveryPoint>,
    },
    GapRecorded(RecoveryGap),
    FinalArchivePublishedButNotDurable {
        point: RecoveryPoint,
        error: std::io::Error,
    },
    FinalArchivePublicationUnknown {
        point: RecoveryPoint,
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
    SourceManifestPublishedButNotDurable {
        gap: Option<RecoveryGap>,
        error: std::io::Error,
    },
    PublicationUnknown {
        gap: Option<RecoveryGap>,
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

#[derive(Debug)]
pub enum PitrCloseOutcome {
    ClosedDurably {
        final_point: Option<RecoveryPoint>,
    },
    ArchiveNotDurable {
        point: Option<RecoveryPoint>,
        error: anyhow::Error,
    },
    PublicationUnknown {
        point: Option<RecoveryPoint>,
        error: anyhow::Error,
    },
}

#[derive(Debug)]
pub enum RestoreToOutcome {
    Restored(RestoreToInfo),
    NoRecoverablePoint,
    PublishedButNotDurable {
        info: RestoreToInfo,
        error: std::io::Error,
    },
    PublicationUnknown {
        info: RestoreToInfo,
        rename_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

#[derive(Debug)]
pub enum PitrPurgeOutcome {
    Purged(PitrPurgeInfo),
    CatalogsDurableCleanupIncomplete {
        info: PitrPurgeInfo,
        error: std::io::Error,
    },
    CatalogsPublishedButNotDurable {
        info: PitrPurgeInfo,
        error: std::io::Error,
    },
    PublicationUnknown {
        info: PitrPurgeInfo,
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

#[derive(Debug)]
pub struct PitrArchiveError {
    pub operation: PitrOperation,
    pub path: PathBuf,
    pub kind: PitrArchiveErrorKind,
    pub source: anyhow::Error,
}

impl PitrOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.repository.as_os_str().is_empty(),
            "PITR repository is empty"
        );
        self.config.validate()?;
        self.runtime.validate()
    }
}

impl PersistedPitrConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.archive_interval.is_zero(),
            "PITR archive interval is zero"
        );
        ensure!(self.max_segment_bytes > 0, "PITR segment limit is zero");
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

impl PitrRuntimeOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.archive_burst_bytes.get() > 0,
            "PITR archive burst is zero"
        );
        Ok(())
    }
}

impl RecoveryTarget {
    pub fn validate(self) -> Result<()> {
        ensure!(
            !matches!(self, Self::CommitTs(0)),
            "PITR recovery commit timestamp is zero"
        );
        Ok(())
    }
}

impl RecoverySelector {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.timeline_id != [0; 16],
            "PITR timeline identity is empty"
        );
        ensure!(
            self.archive_epoch_id.is_none_or(|id| id != [0; 16]),
            "PITR archive epoch identity is empty"
        );
        Ok(())
    }
}

impl PitrStatusOptions {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.page_size <= MAX_STATUS_PAGE_SIZE,
            "PITR status page size exceeds the configured maximum"
        );
        Ok(())
    }
}

impl VerifyPitrOptions {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.page_size <= MAX_VERIFY_PAGE_SIZE,
            "PITR verification page size exceeds the configured maximum"
        );
        if let Some(selector) = self.selector {
            selector.validate()?;
        }
        if let VerifyPitrDepth::Deep { sampled_targets } = self.depth {
            ensure!(
                sampled_targets <= MAX_VERIFY_SAMPLED_TARGETS,
                "PITR verification sample count exceeds the configured maximum"
            );
        }
        Ok(())
    }
}

impl PitrRetentionPolicy {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.retain_timelines.get() > 0,
            "PITR retained timeline count is zero"
        );
        ensure!(
            self.retain_base_backups.get() > 0,
            "PITR retained base-backup count is zero"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> PitrRuntimeOptions {
        PitrRuntimeOptions {
            archive_io_bytes_per_second: None,
            archive_burst_bytes: NonZeroU64::new(1).unwrap(),
            archive_io_priority: ArchiveIoPriority::Background,
        }
    }

    fn config() -> PersistedPitrConfig {
        PersistedPitrConfig {
            archive_interval: Duration::from_secs(1),
            max_segment_bytes: 1,
            max_unarchived_bytes: 1,
            max_source_spool_bytes: 1,
        }
    }

    #[test]
    fn validates_pitr_options_and_rejects_invalid_limits() {
        let options = PitrOptions {
            repository: PathBuf::from("repo"),
            config: config(),
            runtime: runtime(),
        };
        options.validate().unwrap();

        let mut invalid = options.clone();
        invalid.config.max_unarchived_bytes = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn validates_targets_selectors_and_page_bounds() {
        assert!(RecoveryTarget::Latest.validate().is_ok());
        assert!(RecoveryTarget::CommitTs(0).validate().is_err());
        assert!(
            RecoverySelector {
                timeline_id: [0; 16],
                archive_epoch_id: None,
                base_backup_id: None
            }
            .validate()
            .is_err()
        );
        assert!(
            PitrStatusOptions {
                cursor: None,
                page_size: MAX_STATUS_PAGE_SIZE
            }
            .validate()
            .is_ok()
        );
        assert!(
            VerifyPitrOptions {
                depth: VerifyPitrDepth::Deep {
                    sampled_targets: MAX_VERIFY_SAMPLED_TARGETS
                },
                selector: None,
                cursor: None,
                page_size: MAX_VERIFY_PAGE_SIZE,
            }
            .validate()
            .is_ok()
        );
    }
}
