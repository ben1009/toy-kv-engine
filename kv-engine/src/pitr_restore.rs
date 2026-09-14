//! Dormant exact-target PITR restore planning over the validated catalog.
#![allow(dead_code)]

use anyhow::{Result, ensure};

use crate::{
    pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId},
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
}
