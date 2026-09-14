//! Dormant exact-target PITR restore planning over the validated catalog.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use rand::{RngCore, rngs::OsRng};

use crate::{
    pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId, WalBatch},
    pitr_base::PitrBaseMetadata,
    pitr_catalog::{PitrCatalogRecord, SegmentMetadata, encode_catalog},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PitrRestoreTarget {
    Base,
    CommitTs(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrRestorePlan {
    pub(crate) repository_id: [u8; 16],
    pub(crate) timeline_id: [u8; 16],
    pub(crate) archive_epoch_id: [u8; 16],
    pub(crate) target: PitrRestoreTarget,
    pub(crate) segments: Vec<SegmentId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactRestoreState {
    Planned,
    Staging,
    Applying,
    ReadyToPublish,
    Published,
}

#[derive(Debug)]
pub(crate) struct ExactRestoreExecutor {
    plan: PitrRestorePlan,
    state: ExactRestoreState,
    destination_timeline_id: Option<[u8; 16]>,
    last_commit_ts: Option<u64>,
    applied_batches: u64,
}

impl ExactRestoreExecutor {
    pub(crate) fn new(plan: PitrRestorePlan) -> Self {
        Self {
            plan,
            state: ExactRestoreState::Planned,
            destination_timeline_id: None,
            last_commit_ts: None,
            applied_batches: 0,
        }
    }

    pub(crate) fn begin_staging(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Planned,
            "PITR restore staging has already started"
        );
        self.state = ExactRestoreState::Staging;
        Ok(())
    }

    pub(crate) fn begin_apply(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Staging,
            "PITR restore cannot apply before staging"
        );
        ensure!(
            self.destination_timeline_id.is_some(),
            "PITR restore destination timeline is not assigned"
        );
        self.state = ExactRestoreState::Applying;
        Ok(())
    }

    pub(crate) fn assign_new_timeline(&mut self) -> Result<[u8; 16]> {
        self.assign_new_timeline_with_rng(|identity| {
            OsRng.try_fill_bytes(identity).map_err(|error| {
                anyhow::anyhow!("PITR restore timeline entropy unavailable: {error}")
            })
        })
    }

    fn assign_new_timeline_with_rng(
        &mut self,
        mut fill_identity: impl FnMut(&mut [u8; 16]) -> Result<()>,
    ) -> Result<[u8; 16]> {
        ensure!(
            self.state == ExactRestoreState::Staging,
            "PITR restore timeline must be assigned during staging"
        );
        ensure!(
            self.destination_timeline_id.is_none(),
            "PITR restore destination timeline is already assigned"
        );
        for _ in 0..32 {
            let mut identity = [0; 16];
            fill_identity(&mut identity)?;
            if identity != [0; 16] && identity != self.plan.timeline_id {
                self.destination_timeline_id = Some(identity);
                return Ok(identity);
            }
        }
        anyhow::bail!("PITR restore timeline generation exhausted redraw attempts")
    }

    pub(crate) fn apply_batch(&mut self, batch: &WalBatch) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Applying,
            "PITR restore batch is outside the apply phase"
        );
        ensure!(batch.commit_ts != 0, "PITR restore batch timestamp is zero");
        ensure!(!batch.entries.is_empty(), "PITR restore batch is empty");
        ensure!(
            batch.recorded_at.nanos < 1_000_000_000,
            "PITR restore batch recorded time is invalid"
        );
        ensure!(
            self.last_commit_ts
                .is_none_or(|last| batch.commit_ts > last),
            "PITR restore batches are not strictly ordered"
        );
        if let PitrRestoreTarget::CommitTs(target) = self.plan.target {
            ensure!(
                batch.commit_ts <= target,
                "PITR restore batch exceeds the requested target"
            );
        }
        self.last_commit_ts = Some(batch.commit_ts);
        self.applied_batches = self
            .applied_batches
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PITR restore batch count exhausted"))?;
        Ok(())
    }

    pub(crate) fn finish_apply(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Applying,
            "PITR restore apply is not active"
        );
        self.state = ExactRestoreState::ReadyToPublish;
        Ok(())
    }

    pub(crate) fn publish(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::ReadyToPublish,
            "PITR restore cannot publish before apply completes"
        );
        self.state = ExactRestoreState::Published;
        Ok(())
    }

    pub(crate) fn abort(&mut self) -> Result<()> {
        ensure!(
            self.state != ExactRestoreState::Published,
            "published PITR restore cannot be aborted"
        );
        self.state = ExactRestoreState::Planned;
        self.destination_timeline_id = None;
        self.last_commit_ts = None;
        self.applied_batches = 0;
        Ok(())
    }

    pub(crate) fn state(&self) -> ExactRestoreState {
        self.state
    }

    pub(crate) fn applied_batches(&self) -> u64 {
        self.applied_batches
    }

    pub(crate) fn destination_timeline_id(&self) -> Option<[u8; 16]> {
        self.destination_timeline_id
    }
}

pub(crate) fn plan_exact_restore(
    base: &PitrBaseMetadata,
    segments: Vec<SegmentMetadata>,
    target: PitrRestoreTarget,
) -> Result<PitrRestorePlan> {
    base.validate()?;
    if let PitrRestoreTarget::CommitTs(commit_ts) = target {
        ensure!(commit_ts != 0, "PITR restore target timestamp is zero");
        ensure!(
            base.included_commit_ts
                .is_none_or(|included| commit_ts >= included),
            "PITR restore target precedes the base boundary"
        );
    }

    let expected_timeline = TimelineId(base.timeline_id);
    let expected_epoch = ArchiveEpochId(base.archive_epoch_id);
    let mut records = Vec::with_capacity(segments.len());
    for segment in segments {
        ensure!(
            segment.key.repository_id == base.repository_id
                && segment.key.timeline_id == expected_timeline
                && segment.key.archive_epoch_id == expected_epoch,
            "PITR restore segment identity does not match the base"
        );
        records.push(PitrCatalogRecord::CommitSegment { metadata: segment });
    }
    let encoded = encode_catalog(&records)?;
    let replay = crate::pitr_catalog::replay_catalog(&encoded)?;
    let mut retained = replay
        .records
        .into_iter()
        .filter_map(|record| match record {
            PitrCatalogRecord::CommitSegment { metadata } => Some(metadata),
            _ => None,
        })
        .collect::<Vec<_>>();
    retained.sort_by_key(|segment| segment.key.segment_id);

    if let Some(first) = retained.first() {
        ensure!(
            first.predecessor == persisted_to_chain_anchor(base.boundary_anchor),
            "PITR restore chain does not start at the base boundary"
        );
    }

    let selected = match target {
        PitrRestoreTarget::Base => Vec::new(),
        PitrRestoreTarget::CommitTs(target) => retained
            .iter()
            .filter(|segment| segment.first_commit_ts.is_some_and(|first| first <= target))
            .map(|segment| segment.key.segment_id)
            .collect(),
    };
    if let PitrRestoreTarget::CommitTs(target) = target {
        ensure!(
            base.included_commit_ts == Some(target)
                || selected.iter().any(|segment_id| {
                    retained.iter().any(|segment| {
                        segment.key.segment_id == *segment_id
                            && segment.first_commit_ts.is_some_and(|first| first <= target)
                            && segment.last_commit_ts.is_some_and(|last| last >= target)
                    })
                }),
            "PITR restore target is not covered by the archived chain"
        );
    }

    Ok(PitrRestorePlan {
        repository_id: base.repository_id,
        timeline_id: base.timeline_id,
        archive_epoch_id: base.archive_epoch_id,
        target,
        segments: selected,
    })
}

fn persisted_to_chain_anchor(anchor: crate::pitr_manifest::PersistedChainAnchor) -> ChainAnchor {
    match anchor {
        crate::pitr_manifest::PersistedChainAnchor::Genesis { archive_epoch_id } => {
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId(archive_epoch_id),
            }
        }
        crate::pitr_manifest::PersistedChainAnchor::Segment {
            segment_id,
            wal_digest,
            seal_digest,
        } => ChainAnchor::Segment(SegmentAnchor {
            segment_id: SegmentId(segment_id),
            wal_digest,
            seal_digest,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pitr_base::{PITR_BASE_WAL_REPLAY_VERSION, PitrBaseTimeAnchor};
    use crate::pitr_catalog::SegmentKey;
    use crate::pitr_manifest::{PersistedChainAnchor, PersistedRecordedAt};

    fn base() -> PitrBaseMetadata {
        PitrBaseMetadata {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            included_commit_ts: Some(7),
            boundary_segment_id: 1,
            boundary_anchor: PersistedChainAnchor::Genesis {
                archive_epoch_id: [3; 16],
            },
            base_recorded_at: PersistedRecordedAt { secs: 1, nanos: 0 },
            time_anchor: PitrBaseTimeAnchor::Indexed {
                segment_id: 0,
                commit_ts: 7,
                recorded_at: PersistedRecordedAt { secs: 1, nanos: 0 },
                entry_digest: [6; 32],
            },
            wal_replay_version: PITR_BASE_WAL_REPLAY_VERSION,
            compatibility_digest: [7; 32],
        }
    }

    fn segment(id: u64, first: u64, last: u64, predecessor: ChainAnchor) -> SegmentMetadata {
        SegmentMetadata {
            key: SegmentKey {
                repository_id: [1; 16],
                timeline_id: TimelineId([2; 16]),
                archive_epoch_id: ArchiveEpochId([3; 16]),
                segment_id: SegmentId(id),
            },
            wal_format_version: 5,
            seal_format_version: 1,
            anchor: SegmentAnchor {
                segment_id: SegmentId(id),
                wal_digest: [id as u8; 32],
                seal_digest: [id as u8 + 1; 32],
            },
            predecessor,
            first_commit_ts: Some(first),
            last_commit_ts: Some(last),
            batch_count: 1,
            logical_bytes: 4096,
            wal_bytes: 4096,
            wal_digest: [id as u8; 32],
            seal_digest: [id as u8 + 1; 32],
            source_identity: [9; 32],
        }
    }

    #[test]
    fn exact_target_plans_only_the_covering_segment() {
        let segments = vec![segment(
            1,
            8,
            10,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        )];
        let plan = plan_exact_restore(&base(), segments, PitrRestoreTarget::CommitTs(9)).unwrap();
        assert_eq!(plan.segments, vec![SegmentId(1)]);
    }

    #[test]
    fn restore_rejects_mismatched_identity_and_uncovered_target() {
        let mut mismatched = segment(
            1,
            8,
            10,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        );
        mismatched.key.timeline_id = TimelineId([8; 16]);
        assert!(
            plan_exact_restore(&base(), vec![mismatched], PitrRestoreTarget::CommitTs(9)).is_err()
        );
        assert!(plan_exact_restore(&base(), vec![], PitrRestoreTarget::CommitTs(9)).is_err());
    }

    #[test]
    fn base_target_requires_no_archived_segments() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        assert!(plan.segments.is_empty());
    }

    #[test]
    fn exact_restore_executor_enforces_order_and_target() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        assert!(executor.publish().is_err());
        executor.begin_staging().unwrap();
        assert!(executor.assign_new_timeline().is_ok());
        executor.begin_apply().unwrap();
        let batch = WalBatch {
            commit_ts: 8,
            recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
            entries: vec![crate::pitr::WalEntry::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        };
        executor.apply_batch(&batch).unwrap();
        assert_eq!(executor.applied_batches(), 1);
        assert!(executor.apply_batch(&batch).is_err());
        executor.finish_apply().unwrap();
        executor.publish().unwrap();
        assert_eq!(executor.state(), ExactRestoreState::Published);
        assert!(executor.abort().is_err());
    }

    #[test]
    fn exact_restore_executor_rejects_invalid_batch_atomically() {
        let plan = plan_exact_restore(
            &base(),
            vec![segment(
                1,
                8,
                10,
                ChainAnchor::Genesis {
                    archive_epoch_id: ArchiveEpochId([3; 16]),
                },
            )],
            PitrRestoreTarget::CommitTs(8),
        )
        .unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        executor.begin_staging().unwrap();
        assert!(
            executor
                .assign_new_timeline_with_rng(|output| {
                    *output = [0; 16];
                    Ok(())
                })
                .is_err()
        );
        assert!(executor.destination_timeline_id().is_none());
        executor
            .assign_new_timeline_with_rng(|output| {
                *output = [4; 16];
                Ok(())
            })
            .unwrap();
        executor.begin_apply().unwrap();
        let too_late = WalBatch {
            commit_ts: 9,
            recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
            entries: vec![crate::pitr::WalEntry::PointDelete { key: b"k".to_vec() }],
        };
        assert!(executor.apply_batch(&too_late).is_err());
        assert_eq!(executor.applied_batches(), 0);
        executor.abort().unwrap();
        assert_eq!(executor.state(), ExactRestoreState::Planned);
    }
}
