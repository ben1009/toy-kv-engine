//! Dormant exact-target PITR restore planning over the validated catalog.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::{
    pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId, WalBatch},
    pitr_archive::{ArchiveObjectKind, archive_object_name},
    pitr_base::PitrBaseMetadata,
    pitr_catalog::{PitrCatalogRecord, SegmentMetadata, encode_catalog},
    pitr_manifest::{PitrManifestRecord, PitrState, replay_pitr_records},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PitrRecoveryInfo {
    pub(crate) source_repository_id: [u8; 16],
    pub(crate) source_timeline_id: [u8; 16],
    pub(crate) source_archive_epoch_id: [u8; 16],
    pub(crate) destination_timeline_id: [u8; 16],
    pub(crate) target: PitrRestoreTarget,
    pub(crate) last_commit_ts: Option<u64>,
    pub(crate) applied_batches: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RestorePublicationState {
    Prepared,
    RecoveryInfoWritten,
    Published,
}

#[derive(Debug)]
pub(crate) struct PitrRestorePublication {
    target_name: String,
    state: RestorePublicationState,
    recovery_info: Option<PitrRecoveryInfo>,
}

impl PitrRestorePublication {
    pub(crate) fn prepare(target_name: impl Into<String>) -> Result<Self> {
        let target_name = target_name.into();
        ensure!(!target_name.is_empty(), "PITR restore target is empty");
        ensure!(
            !target_name.contains('/')
                && !target_name.contains('\\')
                && target_name != "."
                && target_name != "..",
            "PITR restore target must be a single path component"
        );
        Ok(Self {
            target_name,
            state: RestorePublicationState::Prepared,
            recovery_info: None,
        })
    }

    pub(crate) fn write_recovery_info(&mut self, info: PitrRecoveryInfo) -> Result<()> {
        ensure!(
            self.state == RestorePublicationState::Prepared,
            "PITR recovery info is already written or published"
        );
        ensure!(
            info.destination_timeline_id != [0; 16],
            "PITR recovery timeline is empty"
        );
        self.recovery_info = Some(info);
        self.state = RestorePublicationState::RecoveryInfoWritten;
        Ok(())
    }

    pub(crate) fn publish(&mut self) -> Result<()> {
        ensure!(
            self.state == RestorePublicationState::RecoveryInfoWritten,
            "PITR restore cannot publish before recovery info"
        );
        self.state = RestorePublicationState::Published;
        Ok(())
    }

    pub(crate) fn publish_with(
        &mut self,
        publish_recovery_info: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<()> {
        ensure!(
            self.state == RestorePublicationState::RecoveryInfoWritten,
            "PITR restore cannot publish before recovery info"
        );
        let encoded = self.encoded_recovery_info()?;
        publish_recovery_info(&encoded)?;
        self.state = RestorePublicationState::Published;
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn publish_staging(
        &mut self,
        staging: &std::path::Path,
        target: &std::path::Path,
    ) -> Result<()> {
        ensure!(
            self.state == RestorePublicationState::RecoveryInfoWritten,
            "PITR restore staging is not ready to publish"
        );
        let info = self.encoded_recovery_info()?;
        let info_tmp = staging.join(".RECOVERY_INFO.tmp");
        let info_path = staging.join("RECOVERY_INFO");
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&info_tmp)?;
            file.write_all(&info)?;
            file.sync_all()?;
        }
        std::fs::rename(&info_tmp, &info_path)?;
        crate::checkpoint::publish_pitr_restore_staging(staging, target)?;
        self.state = RestorePublicationState::Published;
        Ok(())
    }

    pub(crate) fn state(&self) -> RestorePublicationState {
        self.state
    }

    pub(crate) fn target_name(&self) -> &str {
        &self.target_name
    }

    pub(crate) fn recovery_info(&self) -> Option<&PitrRecoveryInfo> {
        self.recovery_info.as_ref()
    }

    pub(crate) fn encoded_recovery_info(&self) -> Result<Vec<u8>> {
        let info = self
            .recovery_info
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PITR recovery info has not been written"))?;
        Ok(serde_json::to_vec(info)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrRestoreSourceObject {
    pub(crate) segment_id: SegmentId,
    pub(crate) kind: ArchiveObjectKind,
    pub(crate) name: String,
    pub(crate) digest: [u8; 32],
    pub(crate) bytes: u64,
}

pub(crate) fn required_source_objects(
    base: &PitrBaseMetadata,
    segments: &[SegmentMetadata],
    plan: &PitrRestorePlan,
) -> Result<Vec<PitrRestoreSourceObject>> {
    ensure!(
        plan.repository_id == base.repository_id
            && plan.timeline_id == base.timeline_id
            && plan.archive_epoch_id == base.archive_epoch_id,
        "PITR restore source plan identity mismatch"
    );
    let mut objects = Vec::new();
    for segment_id in &plan.segments {
        let segment = segments
            .iter()
            .find(|segment| segment.key.segment_id == *segment_id)
            .ok_or_else(|| anyhow::anyhow!("PITR restore segment metadata is missing"))?;
        ensure!(
            segment.key.repository_id == base.repository_id
                && segment.key.timeline_id.0 == base.timeline_id
                && segment.key.archive_epoch_id.0 == base.archive_epoch_id,
            "PITR restore source segment identity mismatch"
        );
        objects.push(PitrRestoreSourceObject {
            segment_id: *segment_id,
            kind: ArchiveObjectKind::Wal,
            name: archive_object_name(
                segment.key.timeline_id,
                segment.key.archive_epoch_id,
                *segment_id,
                ArchiveObjectKind::Wal,
                segment.wal_digest,
            ),
            digest: segment.wal_digest,
            bytes: segment.wal_bytes,
        });
        objects.push(PitrRestoreSourceObject {
            segment_id: *segment_id,
            kind: ArchiveObjectKind::Seal,
            name: archive_object_name(
                segment.key.timeline_id,
                segment.key.archive_epoch_id,
                *segment_id,
                ArchiveObjectKind::Seal,
                segment.seal_digest,
            ),
            digest: segment.seal_digest,
            bytes: segment.logical_bytes,
        });
    }
    Ok(objects)
}

pub(crate) fn verify_source_object(object: &PitrRestoreSourceObject, bytes: &[u8]) -> Result<()> {
    ensure!(
        object.bytes == bytes.len() as u64,
        "PITR restore source object length mismatch"
    );
    ensure!(
        Sha256::digest(bytes).as_slice() == object.digest,
        "PITR restore source object digest mismatch"
    );
    Ok(())
}

pub(crate) fn load_verified_source_objects(
    objects: &[PitrRestoreSourceObject],
    mut reader: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<Vec<(PitrRestoreSourceObject, Vec<u8>)>> {
    let mut loaded = Vec::with_capacity(objects.len());
    for object in objects {
        let bytes = reader(&object.name)?;
        verify_source_object(object, &bytes)?;
        loaded.push((object.clone(), bytes));
    }
    Ok(loaded)
}

#[cfg(target_os = "linux")]
pub(crate) fn load_verified_archive_objects(
    stager: &crate::pitr_archive::ArchiveObjectStager,
    objects: &[PitrRestoreSourceObject],
) -> Result<Vec<(PitrRestoreSourceObject, Vec<u8>)>> {
    load_verified_source_objects(objects, |name| stager.read(name))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactRestoreState {
    Planned,
    Staging,
    Applying,
    ReadyToPersist,
    FrontierPersisted,
    RecoveryWalClean,
    Published,
    Closed,
}

#[derive(Debug)]
pub(crate) struct ExactRestoreExecutor {
    plan: PitrRestorePlan,
    state: ExactRestoreState,
    destination_timeline_id: Option<[u8; 16]>,
    last_commit_ts: Option<u64>,
    applied_batches: u64,
    model: BTreeMap<Vec<u8>, Vec<u8>>,
    sources_verified: bool,
    base_materialized: bool,
}

impl ExactRestoreExecutor {
    pub(crate) fn new(plan: PitrRestorePlan) -> Self {
        Self {
            plan,
            state: ExactRestoreState::Planned,
            destination_timeline_id: None,
            last_commit_ts: None,
            applied_batches: 0,
            model: BTreeMap::new(),
            sources_verified: false,
            base_materialized: false,
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
        ensure!(
            self.base_materialized,
            "PITR restore base has not been materialized"
        );
        ensure!(
            self.sources_verified,
            "PITR restore source objects are not verified"
        );
        self.state = ExactRestoreState::Applying;
        Ok(())
    }

    pub(crate) fn verify_source_objects(
        &mut self,
        objects: &[(PitrRestoreSourceObject, Vec<u8>)],
    ) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Staging,
            "PITR restore source verification is outside staging"
        );
        ensure!(
            objects.len() == self.plan.segments.len() * 2,
            "PITR restore source object set is incomplete"
        );
        let mut seen = BTreeMap::<SegmentId, (bool, bool)>::new();
        for (object, bytes) in objects {
            ensure!(
                self.plan.segments.contains(&object.segment_id),
                "PITR restore source object is not required by the plan"
            );
            verify_source_object(object, bytes)?;
            let entry = seen.entry(object.segment_id).or_insert((false, false));
            let slot = match object.kind {
                ArchiveObjectKind::Wal => &mut entry.0,
                ArchiveObjectKind::Seal => &mut entry.1,
            };
            ensure!(!*slot, "PITR restore source object is duplicated");
            *slot = true;
        }
        ensure!(
            seen.len() == self.plan.segments.len()
                && seen.values().all(|(wal, seal)| *wal && *seal),
            "PITR restore source object set is missing WAL or seal data"
        );
        self.sources_verified = true;
        Ok(())
    }

    pub(crate) fn materialize_base(
        &mut self,
        materialize: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Staging,
            "PITR base materialization is outside staging"
        );
        ensure!(
            self.destination_timeline_id.is_some(),
            "PITR restore destination timeline is not assigned"
        );
        materialize()?;
        self.base_materialized = true;
        Ok(())
    }

    #[cfg(test)]
    fn mark_sources_verified_for_test(&mut self) {
        self.sources_verified = true;
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
        let mut next_state = self.model.clone();
        for entry in &batch.entries {
            match entry {
                crate::pitr::WalEntry::Put { key, value } => {
                    next_state.insert(key.clone(), value.clone());
                }
                crate::pitr::WalEntry::PointDelete { key } => {
                    next_state.remove(key);
                }
                crate::pitr::WalEntry::RangeDelete { start, end } => {
                    let keys = next_state
                        .range(start.clone()..end.clone())
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    for key in keys {
                        next_state.remove(&key);
                    }
                }
            }
        }
        self.model = next_state;
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
        self.state = ExactRestoreState::ReadyToPersist;
        Ok(())
    }

    pub(crate) fn persist_frontier(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::ReadyToPersist,
            "PITR restore frontier cannot persist before apply completes"
        );
        self.state = ExactRestoreState::FrontierPersisted;
        Ok(())
    }

    pub(crate) fn remove_recovery_wal(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::FrontierPersisted,
            "PITR restore WAL cleanup cannot run before frontier persistence"
        );
        self.state = ExactRestoreState::RecoveryWalClean;
        Ok(())
    }

    pub(crate) fn publish(&mut self) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::RecoveryWalClean,
            "PITR restore cannot publish before frontier and WAL cleanup"
        );
        self.state = ExactRestoreState::Published;
        Ok(())
    }

    pub(crate) fn abort(&mut self) -> Result<()> {
        ensure!(
            !matches!(
                self.state,
                ExactRestoreState::Published | ExactRestoreState::Closed
            ),
            "published or closed PITR restore cannot be aborted"
        );
        self.state = ExactRestoreState::Planned;
        self.destination_timeline_id = None;
        self.last_commit_ts = None;
        self.applied_batches = 0;
        self.model.clear();
        self.sources_verified = false;
        self.base_materialized = false;
        Ok(())
    }

    pub(crate) fn state(&self) -> ExactRestoreState {
        self.state
    }

    pub(crate) fn applied_batches(&self) -> u64 {
        self.applied_batches
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.model.get(key).map(Vec::as_slice)
    }

    pub(crate) fn destination_timeline_id(&self) -> Option<[u8; 16]> {
        self.destination_timeline_id
    }

    pub(crate) fn recovery_info(&self) -> Result<PitrRecoveryInfo> {
        ensure!(
            matches!(
                self.state,
                ExactRestoreState::Published | ExactRestoreState::Closed
            ),
            "PITR recovery info requires a published restore"
        );
        Ok(PitrRecoveryInfo {
            source_repository_id: self.plan.repository_id,
            source_timeline_id: self.plan.timeline_id,
            source_archive_epoch_id: self.plan.archive_epoch_id,
            destination_timeline_id: self
                .destination_timeline_id
                .ok_or_else(|| anyhow::anyhow!("PITR restore destination timeline is missing"))?,
            target: self.plan.target,
            last_commit_ts: self.last_commit_ts,
            applied_batches: self.applied_batches,
        })
    }

    pub(crate) fn close(&mut self) -> Result<PitrRecoveryInfo> {
        ensure!(
            self.state == ExactRestoreState::Published,
            "PITR restore close requires a published restore"
        );
        let info = self.recovery_info()?;
        self.state = ExactRestoreState::Closed;
        Ok(info)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn publish_staging(
        &mut self,
        publication: &mut PitrRestorePublication,
        staging: &std::path::Path,
        target: &std::path::Path,
    ) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Published,
            "PITR restore executor is not published"
        );
        let info = self.recovery_info()?;
        if publication.state() == RestorePublicationState::Prepared {
            publication.write_recovery_info(info)?;
        } else if publication.state() == RestorePublicationState::RecoveryInfoWritten {
            ensure!(
                publication.recovery_info() == Some(&info),
                "PITR publication recovery info does not match the executor"
            );
        }
        publication.publish_staging(staging, target)?;
        self.state = ExactRestoreState::Closed;
        Ok(())
    }

    pub(crate) fn sanitized_restore_state(&self) -> Result<PitrState> {
        let timeline_id = self
            .destination_timeline_id
            .ok_or_else(|| anyhow::anyhow!("PITR restore destination timeline is not assigned"))?;
        ensure!(
            self.state != ExactRestoreState::Planned,
            "PITR restore state cannot be sanitized before staging"
        );
        replay_pitr_records([PitrManifestRecord::Snapshot(Box::new(PitrState {
            database_timeline_id: Some(timeline_id),
            ..PitrState::default()
        }))])
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
    fn restore_source_objects_are_identity_bound_and_ordered() {
        let segment = segment(
            1,
            8,
            10,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        );
        let segments = vec![segment.clone()];
        let plan =
            plan_exact_restore(&base(), segments.clone(), PitrRestoreTarget::CommitTs(9)).unwrap();
        let objects = required_source_objects(&base(), &segments, &plan).unwrap();
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].kind, ArchiveObjectKind::Wal);
        assert_eq!(objects[1].kind, ArchiveObjectKind::Seal);
        assert_eq!(objects[0].digest, segment.wal_digest);
        assert_eq!(objects[1].digest, segment.seal_digest);
        assert!(objects[0].name.ends_with(".wal"));
        assert!(objects[1].name.ends_with(".seal"));
        let wal = b"wal-bytes";
        assert!(verify_source_object(&objects[0], wal).is_err());
        let mut matching = objects[0].clone();
        matching.bytes = wal.len() as u64;
        matching.digest = Sha256::digest(wal).into();
        verify_source_object(&matching, wal).unwrap();
        assert!(verify_source_object(&matching, b"tampered").is_err());
        let loaded = load_verified_source_objects(&[matching], |name| {
            ensure!(name.ends_with(".wal"), "unexpected object request");
            Ok(wal.to_vec())
        })
        .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1, wal);
    }

    #[test]
    fn exact_restore_executor_enforces_order_and_target() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        assert!(executor.publish().is_err());
        executor.begin_staging().unwrap();
        assert!(executor.assign_new_timeline().is_ok());
        executor.materialize_base(|| Ok(())).unwrap();
        executor.verify_source_objects(&[]).unwrap();
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
        assert_eq!(executor.get(b"k"), Some(b"v".as_slice()));
        assert!(executor.apply_batch(&batch).is_err());
        executor.finish_apply().unwrap();
        executor.persist_frontier().unwrap();
        executor.remove_recovery_wal().unwrap();
        executor.publish().unwrap();
        assert_eq!(executor.state(), ExactRestoreState::Published);
        let info = executor.recovery_info().unwrap();
        assert_eq!(info.source_timeline_id, [2; 16]);
        assert_eq!(info.applied_batches, 1);
        let closed = executor.close().unwrap();
        assert_eq!(closed, info);
        assert_eq!(executor.state(), ExactRestoreState::Closed);
        assert!(executor.close().is_err());
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
        executor.mark_sources_verified_for_test();
        executor.materialize_base(|| Ok(())).unwrap();
        executor.begin_apply().unwrap();
        let too_late = WalBatch {
            commit_ts: 9,
            recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
            entries: vec![crate::pitr::WalEntry::PointDelete { key: b"k".to_vec() }],
        };
        assert!(executor.apply_batch(&too_late).is_err());
        assert_eq!(executor.applied_batches(), 0);
        assert!(executor.get(b"k").is_none());
        executor.abort().unwrap();
        assert_eq!(executor.state(), ExactRestoreState::Planned);
        assert!(executor.recovery_info().is_err());
    }

    #[test]
    fn restore_sanitizes_inherited_pitr_state_for_new_timeline() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        executor.begin_staging().unwrap();
        let destination = executor.assign_new_timeline().unwrap();
        let state = executor.sanitized_restore_state().unwrap();
        assert_eq!(state.mode, crate::pitr_manifest::PitrMode::Disabled);
        assert_eq!(state.database_timeline_id, Some(destination));
        assert!(state.repository_id.is_none());
        assert!(state.archive_epoch_id.is_none());
    }

    #[test]
    fn exact_restore_executor_applies_point_and_range_deletes() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        executor.begin_staging().unwrap();
        executor.assign_new_timeline().unwrap();
        executor.materialize_base(|| Ok(())).unwrap();
        executor.verify_source_objects(&[]).unwrap();
        executor.begin_apply().unwrap();
        executor
            .apply_batch(&WalBatch {
                commit_ts: 8,
                recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
                entries: vec![
                    crate::pitr::WalEntry::Put {
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    crate::pitr::WalEntry::Put {
                        key: b"b".to_vec(),
                        value: b"2".to_vec(),
                    },
                ],
            })
            .unwrap();
        executor
            .apply_batch(&WalBatch {
                commit_ts: 9,
                recorded_at: crate::pitr::RecordedAt { secs: 3, nanos: 0 },
                entries: vec![crate::pitr::WalEntry::RangeDelete {
                    start: b"a".to_vec(),
                    end: b"c".to_vec(),
                }],
            })
            .unwrap();
        assert!(executor.get(b"a").is_none());
        assert!(executor.get(b"b").is_none());
    }

    #[test]
    fn restore_publication_requires_recovery_info_and_safe_target() {
        assert!(PitrRestorePublication::prepare("../escape").is_err());
        let mut publication = PitrRestorePublication::prepare("restored-db").unwrap();
        assert_eq!(publication.target_name(), "restored-db");
        assert!(publication.publish().is_err());
        let info = PitrRecoveryInfo {
            source_repository_id: [1; 16],
            source_timeline_id: [2; 16],
            source_archive_epoch_id: [3; 16],
            destination_timeline_id: [4; 16],
            target: PitrRestoreTarget::Base,
            last_commit_ts: None,
            applied_batches: 0,
        };
        publication.write_recovery_info(info.clone()).unwrap();
        assert_eq!(publication.recovery_info(), Some(&info));
        let encoded = publication.encoded_recovery_info().unwrap();
        assert_eq!(
            serde_json::from_slice::<PitrRecoveryInfo>(&encoded).unwrap(),
            info
        );
        publication.publish().unwrap();
        assert_eq!(publication.state(), RestorePublicationState::Published);
        assert!(publication.write_recovery_info(info).is_err());
    }

    #[test]
    fn restore_publication_retries_after_publisher_failure() {
        let mut publication = PitrRestorePublication::prepare("restored-db").unwrap();
        publication
            .write_recovery_info(PitrRecoveryInfo {
                source_repository_id: [1; 16],
                source_timeline_id: [2; 16],
                source_archive_epoch_id: [3; 16],
                destination_timeline_id: [4; 16],
                target: PitrRestoreTarget::Base,
                last_commit_ts: None,
                applied_batches: 0,
            })
            .unwrap();
        assert!(
            publication
                .publish_with(|_| anyhow::bail!("publisher failure"))
                .is_err()
        );
        assert_eq!(
            publication.state(),
            RestorePublicationState::RecoveryInfoWritten
        );
        publication
            .publish_with(|bytes| {
                ensure!(!bytes.is_empty(), "empty recovery info");
                Ok(())
            })
            .unwrap();
        assert_eq!(publication.state(), RestorePublicationState::Published);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn restore_staging_publishes_recovery_info_without_replacement() {
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("staging");
        let target = root.path().join("restored");
        std::fs::create_dir(&staging).unwrap();
        let info = PitrRecoveryInfo {
            source_repository_id: [1; 16],
            source_timeline_id: [2; 16],
            source_archive_epoch_id: [3; 16],
            destination_timeline_id: [4; 16],
            target: PitrRestoreTarget::Base,
            last_commit_ts: None,
            applied_batches: 0,
        };
        let mut publication = PitrRestorePublication::prepare("restored").unwrap();
        publication.write_recovery_info(info).unwrap();
        publication.publish_staging(&staging, &target).unwrap();
        assert!(target.join("RECOVERY_INFO").is_file());
        assert!(
            publication
                .publish_staging(&staging, &root.path().join("other"))
                .is_err()
        );
    }

    #[test]
    fn restore_base_materialization_failure_is_retryable() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        executor.begin_staging().unwrap();
        executor.assign_new_timeline().unwrap();
        assert!(
            executor
                .materialize_base(|| anyhow::bail!("copy failed"))
                .is_err()
        );
        assert!(executor.begin_apply().is_err());
        executor.materialize_base(|| Ok(())).unwrap();
        executor.mark_sources_verified_for_test();
        executor.begin_apply().unwrap();
    }
}
