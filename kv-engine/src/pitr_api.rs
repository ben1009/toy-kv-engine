//! Public PITR contracts and validation.
//!
//! The state-machine implementations remain crate-private until the synchronous
//! lifecycle is wired into the engine. Keeping the public contracts here makes
//! their validation independently testable without exposing a partially live API.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::RangeInclusive,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

pub const MAX_STATUS_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const MAX_VERIFY_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const MAX_VERIFY_SAMPLED_TARGETS: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const DEFAULT_PITR_ARCHIVE_BURST_BYTES: NonZeroU64 = NonZeroU64::new(1024 * 1024).unwrap();

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

impl Default for PitrRuntimeOptions {
    fn default() -> Self {
        Self {
            archive_io_bytes_per_second: None,
            archive_burst_bytes: DEFAULT_PITR_ARCHIVE_BURST_BYTES,
            archive_io_priority: ArchiveIoPriority::Background,
        }
    }
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

#[derive(Debug)]
pub(crate) struct PitrRuntimeController {
    limiter: Arc<crate::pitr_limiter::PitrArchiveLimiter>,
    priority: parking_lot::Mutex<ArchiveIoPriority>,
}

impl PitrRuntimeController {
    #[allow(dead_code)]
    pub(crate) fn new(options: &PitrRuntimeOptions, now: std::time::Instant) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            limiter: Arc::new(crate::pitr_limiter::PitrArchiveLimiter::new(
                options.limiter_options(),
                now,
            )),
            priority: parking_lot::Mutex::new(options.archive_io_priority),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn update(
        &self,
        options: &PitrRuntimeOptions,
        now: std::time::Instant,
    ) -> Result<()> {
        options.validate()?;
        self.limiter.update(options.limiter_options(), now)?;
        *self.priority.lock() = options.archive_io_priority;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn limiter(&self) -> Arc<crate::pitr_limiter::PitrArchiveLimiter> {
        Arc::clone(&self.limiter)
    }

    #[allow(dead_code)]
    pub(crate) fn priority(&self) -> ArchiveIoPriority {
        *self.priority.lock()
    }
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

    #[allow(dead_code)]
    pub(crate) fn persisted_config(&self) -> Result<crate::pitr_manifest::PersistedPitrConfig> {
        self.config.to_persisted()
    }

    #[allow(dead_code)]
    pub(crate) fn limiter_options(&self) -> crate::pitr_limiter::ArchiveLimiterOptions {
        self.runtime.limiter_options()
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

    #[allow(dead_code)]
    pub(crate) fn to_persisted(&self) -> Result<crate::pitr_manifest::PersistedPitrConfig> {
        self.validate()?;
        let archive_interval_ms = u64::try_from(self.archive_interval.as_millis())
            .map_err(|_| anyhow::anyhow!("PITR archive interval exceeds supported range"))?;
        ensure!(
            archive_interval_ms > 0,
            "PITR archive interval is below one millisecond"
        );
        Ok(crate::pitr_manifest::PersistedPitrConfig {
            archive_interval_ms,
            max_segment_bytes: self.max_segment_bytes,
            max_unarchived_bytes: self.max_unarchived_bytes,
            max_source_spool_bytes: self.max_source_spool_bytes,
        })
    }
}

impl PitrRuntimeOptions {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn limiter_options(&self) -> crate::pitr_limiter::ArchiveLimiterOptions {
        crate::pitr_limiter::ArchiveLimiterOptions {
            bytes_per_second: self.archive_io_bytes_per_second,
            burst_bytes: self.archive_burst_bytes,
        }
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

    #[allow(dead_code)]
    pub(crate) fn to_restore_target(self) -> Result<crate::pitr_restore::PitrRestoreTarget> {
        self.validate()?;
        Ok(match self {
            Self::Latest => anyhow::bail!("Latest target requires archived interval selection"),
            Self::CommitTs(commit_ts) => {
                crate::pitr_restore::PitrRestoreTarget::CommitTs(commit_ts)
            }
            Self::AtOrBeforeSystemTime(_) => {
                anyhow::bail!("wall-clock target requires archived time-index selection")
            }
        })
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

    #[allow(dead_code)]
    pub(crate) fn validate_for_restore(self) -> Result<Self> {
        self.validate()?;
        ensure!(
            self.base_backup_id.is_none_or(|id| id != 0),
            "PITR base backup identity is zero"
        );
        Ok(self)
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
        Ok(())
    }
}

impl PitrStatus {
    #[allow(dead_code)]
    pub(crate) fn from_manifest_state(state: &crate::pitr_manifest::PitrState) -> Self {
        let archive_state = match state.mode {
            crate::pitr_manifest::PitrMode::Disabled => {
                if state.database_timeline_id.is_some() {
                    PitrArchiveState::Disabled
                } else {
                    PitrArchiveState::NeverEnabled
                }
            }
            crate::pitr_manifest::PitrMode::Enabling | crate::pitr_manifest::PitrMode::Enabled => {
                PitrArchiveState::Active
            }
            crate::pitr_manifest::PitrMode::PublicationUncertain
            | crate::pitr_manifest::PitrMode::ReconciliationRequired => {
                PitrArchiveState::ReconciliationRequired
            }
        };
        Self {
            state: archive_state,
            archive_epoch_id: state.archive_epoch_id,
            latest_durable_commit_ts: state.last_commit_anchor.map(|anchor| anchor.commit_ts),
            latest_archived_commit_ts: None,
            recoverable_intervals: Vec::new(),
            active_wal_bytes: 0,
            sealed_unarchived_wal_bytes: 0,
            source_spool_bytes: 0,
            archive_lag_commits: 0,
            archive_lag_bytes: 0,
            oldest_unarchived_recorded_at: None,
            archive_lag_duration: None,
            scheduler_delay: Duration::ZERO,
            repository_staging_bytes: 0,
            repository_orphan_bytes: 0,
            next_cursor: None,
            last_archive_error: None,
        }
    }
}

impl RecoveryChainAnchor {
    #[allow(dead_code)]
    pub(crate) fn to_persisted(self) -> crate::pitr_manifest::PersistedChainAnchor {
        match self {
            Self::Genesis { archive_epoch_id } => {
                crate::pitr_manifest::PersistedChainAnchor::Genesis { archive_epoch_id }
            }
            Self::Segment(anchor) => crate::pitr_manifest::PersistedChainAnchor::Segment {
                segment_id: anchor.segment_id,
                wal_digest: anchor.wal_digest,
                seal_digest: anchor.seal_digest,
            },
        }
    }
}

impl BaseTimeAnchor {
    #[allow(dead_code)]
    pub(crate) fn to_persisted(self) -> crate::pitr_base::PitrBaseTimeAnchor {
        match self {
            Self::Indexed {
                segment_id,
                commit_ts,
                recorded_at,
                entry_digest,
            } => crate::pitr_base::PitrBaseTimeAnchor::Indexed {
                segment_id,
                commit_ts,
                recorded_at: public_recorded_at(recorded_at),
                entry_digest,
            },
            Self::ObservedBoundary {
                commit_ts,
                observed_at,
            } => crate::pitr_base::PitrBaseTimeAnchor::Observed {
                commit_ts,
                observed_at: public_recorded_at(observed_at),
            },
        }
    }
}

#[allow(dead_code)]
fn public_recorded_at(time: SystemTime) -> crate::pitr_manifest::PersistedRecordedAt {
    let recorded_at =
        crate::pitr::RecordedAt::from_system_time(time).expect("validated public PITR time anchor");
    crate::pitr_manifest::PersistedRecordedAt {
        secs: recorded_at.secs,
        nanos: recorded_at.nanos,
    }
}

#[allow(dead_code)]
pub(crate) fn page_recovery_intervals(
    intervals: &[RecoveryInterval],
    options: PitrStatusOptions,
    catalog_digest: [u8; 32],
    catalog_high_water: u64,
) -> Result<RecoveryIntervalPage> {
    options.validate()?;
    let start = match options.cursor {
        None => 0,
        Some(cursor) => {
            ensure!(
                cursor.catalog_digest == catalog_digest
                    && cursor.catalog_high_water == catalog_high_water,
                "PITR status cursor does not match the current catalog"
            );
            usize::try_from(cursor.interval_index)
                .map_err(|_| anyhow::anyhow!("PITR status cursor index is too large"))?
        }
    };
    ensure!(
        start <= intervals.len(),
        "PITR status cursor is past the interval list"
    );
    let end = start
        .saturating_add(options.page_size.get())
        .min(intervals.len());
    let next_cursor = (end < intervals.len()).then_some(PitrStatusCursor {
        catalog_digest,
        catalog_high_water,
        interval_index: u64::try_from(end)
            .map_err(|_| anyhow::anyhow!("PITR status interval list is too large"))?,
    });
    Ok(RecoveryIntervalPage {
        items: intervals[start..end].to_vec(),
        next_cursor,
    })
}

#[allow(dead_code)]
pub(crate) fn verification_query_digest(options: VerifyPitrOptions) -> Result<[u8; 32]> {
    options.validate()?;
    let mut digest = Sha256::new();
    digest.update(b"TOYKV-PITR-VERIFY-V1");
    match options.depth {
        VerifyPitrDepth::Shallow => digest.update([0]),
        VerifyPitrDepth::Deep { sampled_targets } => {
            digest.update([1]);
            digest.update(
                u64::try_from(sampled_targets.get())
                    .map_err(|_| anyhow::anyhow!("PITR verification sample count is too large"))?
                    .to_be_bytes(),
            );
        }
    }
    match options.selector {
        None => digest.update([0]),
        Some(selector) => {
            digest.update([1]);
            digest.update(selector.timeline_id);
            digest.update(selector.archive_epoch_id.unwrap_or([0; 16]));
            digest.update([u8::from(selector.base_backup_id.is_some())]);
            if let Some(base_backup_id) = selector.base_backup_id {
                digest.update(base_backup_id.to_be_bytes());
            }
        }
    }
    Ok(digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> PitrRuntimeOptions {
        PitrRuntimeOptions::default()
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
    fn converts_public_options_to_durable_and_limiter_contracts() {
        let options = PitrOptions {
            repository: PathBuf::from("repo"),
            config: config(),
            runtime: runtime(),
        };
        assert_eq!(
            options.persisted_config().unwrap().archive_interval_ms,
            1000
        );
        assert_eq!(
            options.limiter_options().burst_bytes,
            DEFAULT_PITR_ARCHIVE_BURST_BYTES
        );

        let mut submillisecond = options;
        submillisecond.config.archive_interval = Duration::from_nanos(1);
        assert!(submillisecond.persisted_config().is_err());
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

    #[test]
    fn pages_intervals_with_catalog_bound_cursor() {
        let interval = RecoveryInterval {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            base_backup_id: 1,
            boundary: RecoveryChainAnchor::Genesis {
                archive_epoch_id: [3; 16],
            },
            commit_bounds: None,
            recorded_time_bounds: None,
            base_time_anchor: BaseTimeAnchor::ObservedBoundary {
                commit_ts: None,
                observed_at: SystemTime::UNIX_EPOCH,
            },
        };
        let intervals = vec![
            interval.clone(),
            RecoveryInterval {
                base_backup_id: 2,
                ..interval
            },
        ];
        let first = page_recovery_intervals(
            &intervals,
            PitrStatusOptions {
                cursor: None,
                page_size: NonZeroUsize::new(1).unwrap(),
            },
            [4; 32],
            7,
        )
        .unwrap();
        assert_eq!(first.items[0].base_backup_id, 1);
        let second = page_recovery_intervals(
            &intervals,
            PitrStatusOptions {
                cursor: first.next_cursor,
                page_size: NonZeroUsize::new(1).unwrap(),
            },
            [4; 32],
            7,
        )
        .unwrap();
        assert_eq!(second.items[0].base_backup_id, 2);
        assert!(second.next_cursor.is_none());
        assert!(
            page_recovery_intervals(
                &intervals,
                PitrStatusOptions {
                    cursor: Some(PitrStatusCursor {
                        catalog_digest: [4; 32],
                        catalog_high_water: 8,
                        interval_index: 1,
                    }),
                    page_size: NonZeroUsize::new(1).unwrap(),
                },
                [4; 32],
                7,
            )
            .is_err()
        );
    }
    #[test]
    fn verification_cursor_digest_binds_query_shape() {
        let shallow = VerifyPitrOptions {
            depth: VerifyPitrDepth::Shallow,
            selector: None,
            cursor: None,
            page_size: NonZeroUsize::new(1).unwrap(),
        };
        let deep = VerifyPitrOptions {
            depth: VerifyPitrDepth::Deep {
                sampled_targets: NonZeroUsize::new(1).unwrap(),
            },
            ..shallow
        };
        assert_ne!(
            verification_query_digest(shallow).unwrap(),
            verification_query_digest(deep).unwrap()
        );
    }
    #[test]
    fn converts_restore_coordinates_to_internal_contracts() {
        assert_eq!(
            RecoveryTarget::CommitTs(7).to_restore_target().unwrap(),
            crate::pitr_restore::PitrRestoreTarget::CommitTs(7)
        );
        assert!(RecoveryTarget::Latest.to_restore_target().is_err());
        assert_eq!(
            RecoveryChainAnchor::Genesis {
                archive_epoch_id: [3; 16]
            }
            .to_persisted(),
            crate::pitr_manifest::PersistedChainAnchor::Genesis {
                archive_epoch_id: [3; 16]
            }
        );
        let anchor = BaseTimeAnchor::ObservedBoundary {
            commit_ts: Some(7),
            observed_at: SystemTime::UNIX_EPOCH,
        }
        .to_persisted();
        assert!(matches!(
            anchor,
            crate::pitr_base::PitrBaseTimeAnchor::Observed { .. }
        ));
    }

    #[test]
    fn runtime_controller_applies_online_limiter_updates() {
        let now = std::time::Instant::now();
        let unlimited = runtime();
        let controller = PitrRuntimeController::new(&unlimited, now).unwrap();
        let limited = PitrRuntimeOptions {
            archive_io_bytes_per_second: NonZeroU64::new(10),
            archive_burst_bytes: NonZeroU64::new(20).unwrap(),
            archive_io_priority: ArchiveIoPriority::Normal,
        };
        controller.update(&limited, now).unwrap();
        assert_eq!(controller.priority(), ArchiveIoPriority::Normal);
        assert_eq!(controller.limiter().tokens(now), 20);
        controller
            .limiter()
            .try_grant(NonZeroU64::new(5).unwrap(), now)
            .unwrap();
        let reduced = PitrRuntimeOptions {
            archive_io_bytes_per_second: NonZeroU64::new(10),
            archive_burst_bytes: NonZeroU64::new(8).unwrap(),
            archive_io_priority: ArchiveIoPriority::Background,
        };
        controller.update(&reduced, now).unwrap();
        assert_eq!(controller.priority(), ArchiveIoPriority::Background);
        assert_eq!(controller.limiter().tokens(now), 8);
    }

    #[test]
    fn status_projection_preserves_manifest_mode_epoch_and_frontier() {
        let mut state = crate::pitr_manifest::PitrState {
            mode: crate::pitr_manifest::PitrMode::Enabled,
            archive_epoch_id: Some([3; 16]),
            last_commit_anchor: Some(crate::pitr_manifest::PersistedCommitAnchor {
                segment_id: 7,
                commit_ts: 11,
                recorded_at: crate::pitr_manifest::PersistedRecordedAt { secs: 1, nanos: 0 },
                entry_digest: [4; 32],
            }),
            ..Default::default()
        };
        let status = PitrStatus::from_manifest_state(&state);
        assert_eq!(status.state, PitrArchiveState::Active);
        assert_eq!(status.archive_epoch_id, Some([3; 16]));
        assert_eq!(status.latest_durable_commit_ts, Some(11));

        state.mode = crate::pitr_manifest::PitrMode::ReconciliationRequired;
        assert_eq!(
            PitrStatus::from_manifest_state(&state).state,
            PitrArchiveState::ReconciliationRequired
        );
    }
}
