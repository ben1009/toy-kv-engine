//! Exact-target PITR restore planning over the validated catalog.
#![allow(dead_code)]

use anyhow::{Result, bail, ensure};
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
    pub(crate) base_included_commit_ts: Option<u64>,
    pub(crate) segments: Vec<SegmentId>,
    pub(crate) segment_metadata: Vec<SegmentMetadata>,
    pub(crate) proof_segments: Vec<SegmentId>,
    pub(crate) proof_metadata: Vec<SegmentMetadata>,
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

    #[cfg(target_os = "linux")]
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
        std::fs::File::open(staging)?.sync_all()?;
        match crate::checkpoint::publish_pitr_restore_staging(staging, target) {
            Ok(()) => {
                self.state = RestorePublicationState::Published;
                Ok(())
            }
            Err(error)
                if target.is_dir()
                    && !staging.exists()
                    && target_recovery_info_matches(target, &info) =>
            {
                self.state = RestorePublicationState::Published;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn reconcile_published_staging(
        &mut self,
        staging: &std::path::Path,
        target: &std::path::Path,
    ) -> Result<()> {
        ensure!(
            self.state == RestorePublicationState::RecoveryInfoWritten,
            "PITR restore reconciliation is not pending"
        );
        ensure!(
            target.is_dir()
                && !staging.exists()
                && target_recovery_info_matches(target, &self.encoded_recovery_info()?),
            "PITR restore publication cannot be reconciled"
        );
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

#[cfg(target_os = "linux")]
fn target_recovery_info_matches(target: &std::path::Path, expected: &[u8]) -> bool {
    std::fs::read(target.join("RECOVERY_INFO")).is_ok_and(|actual| actual == expected)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PitrRestoreSourceObject {
    pub(crate) segment_id: SegmentId,
    pub(crate) kind: ArchiveObjectKind,
    pub(crate) name: String,
    pub(crate) digest: [u8; 32],
    pub(crate) bytes: Option<u64>,
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
    let required_segment_ids = plan
        .segments
        .iter()
        .chain(plan.proof_segments.iter())
        .copied()
        .collect::<Vec<_>>();
    for segment_id in &required_segment_ids {
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
            bytes: Some(segment.wal_bytes),
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
            bytes: None,
        });
    }
    Ok(objects)
}

pub(crate) fn verify_source_object(object: &PitrRestoreSourceObject, bytes: &[u8]) -> Result<()> {
    if let Some(expected_bytes) = object.bytes {
        ensure!(
            expected_bytes == bytes.len() as u64,
            "PITR restore source object length mismatch"
        );
    }
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

pub(crate) fn decode_restore_wal_batches(
    wal: &[u8],
    limits: crate::pitr::WalV5Limits,
) -> Result<Vec<WalBatch>> {
    let header = crate::pitr::decode_v5_file_header(wal)?;
    let mut offset = crate::pitr::WAL_V5_HEADER_LEN;
    let mut batches = Vec::new();
    while offset < wal.len() {
        let decoded = crate::pitr::decode_v5_batch(wal, offset, limits)?;
        offset = decoded.logical_end;
        batches.push(decoded.batch);
    }
    ensure!(
        header.segment_id.0 != u64::MAX,
        "PITR restore WAL segment identity is exhausted"
    );
    Ok(batches)
}

pub(crate) fn decode_restore_wal_batches_for_segment(
    wal: &[u8],
    expected: &SegmentMetadata,
    limits: crate::pitr::WalV5Limits,
) -> Result<Vec<WalBatch>> {
    let header = crate::pitr::decode_v5_file_header(wal)?;
    ensure!(
        header.timeline_id.0 == expected.key.timeline_id.0
            && header.archive_epoch_id.0 == expected.key.archive_epoch_id.0
            && header.segment_id.0 == expected.key.segment_id.0,
        "PITR restore WAL header identity does not match catalog metadata"
    );
    ensure!(
        header.predecessor == expected.predecessor,
        "PITR restore WAL predecessor does not match catalog metadata"
    );
    decode_restore_wal_batches(wal, limits)
}

fn validate_segment_batch_range(metadata: &SegmentMetadata, batches: &[WalBatch]) -> Result<()> {
    ensure!(
        batches.len() as u64 == metadata.batch_count
            && batches.first().map(|batch| batch.commit_ts) == metadata.first_commit_ts
            && batches.last().map(|batch| batch.commit_ts) == metadata.last_commit_ts,
        "PITR restore WAL commit range does not match catalog metadata"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn load_verified_archive_objects(
    stager: &crate::pitr_archive::ArchiveObjectStager,
    objects: &[PitrRestoreSourceObject],
) -> Result<Vec<(PitrRestoreSourceObject, Vec<u8>)>> {
    load_verified_source_objects(objects, |name| {
        let expected = objects
            .iter()
            .find(|object| object.name == name)
            .and_then(|object| object.bytes);
        stager.read(name, expected)
    })
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
    expected_sources: Option<Vec<PitrRestoreSourceObject>>,
    state: ExactRestoreState,
    destination_timeline_id: Option<[u8; 16]>,
    last_commit_ts: Option<u64>,
    applied_batches: u64,
    model: BTreeMap<Vec<u8>, Vec<u8>>,
    sources_verified: bool,
    proofs_verified: bool,
    base_materialized: bool,
}

impl ExactRestoreExecutor {
    pub(crate) fn new(plan: PitrRestorePlan) -> Self {
        Self::new_with_sources(plan, None)
    }

    pub(crate) fn new_with_sources(
        plan: PitrRestorePlan,
        expected_sources: Option<Vec<PitrRestoreSourceObject>>,
    ) -> Self {
        let last_commit_ts = plan.base_included_commit_ts;
        let proofs_verified = plan.proof_segments.is_empty();
        Self {
            plan,
            expected_sources,
            state: ExactRestoreState::Planned,
            destination_timeline_id: None,
            last_commit_ts,
            applied_batches: 0,
            model: BTreeMap::new(),
            sources_verified: false,
            proofs_verified,
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
        ensure!(
            self.proofs_verified,
            "PITR restore gap proof WALs are not verified"
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
            objects.len() == (self.plan.segments.len() + self.plan.proof_segments.len()) * 2,
            "PITR restore source object set is incomplete"
        );
        if let Some(expected) = &self.expected_sources {
            ensure!(
                expected.len() == objects.len(),
                "PITR restore source set length mismatch"
            );
            for expected_object in expected {
                ensure!(
                    objects.iter().any(|(object, _)| object == expected_object),
                    "PITR restore source descriptor is not canonical"
                );
            }
        } else {
            ensure!(
                self.plan.segments.is_empty()
                    && self.plan.proof_segments.is_empty()
                    && objects.is_empty(),
                "PITR restore source descriptors are not bound to a catalog plan"
            );
        }
        let mut seen = BTreeMap::<SegmentId, (bool, bool)>::new();
        for (object, bytes) in objects {
            ensure!(
                self.plan.segments.contains(&object.segment_id)
                    || self.plan.proof_segments.contains(&object.segment_id),
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
            seen.len() == self.plan.segments.len() + self.plan.proof_segments.len()
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

    pub(crate) fn verify_proof_wals(
        &mut self,
        objects: &[(PitrRestoreSourceObject, Vec<u8>)],
        limits: crate::pitr::WalV5Limits,
    ) -> Result<()> {
        ensure!(
            self.state == ExactRestoreState::Staging,
            "PITR restore proof verification is outside staging"
        );
        ensure!(
            self.sources_verified,
            "PITR restore sources must be verified before proofs"
        );
        self.validate_proof_wals(objects, limits)?;
        self.proofs_verified = true;
        Ok(())
    }

    fn validate_proof_wals(
        &self,
        objects: &[(PitrRestoreSourceObject, Vec<u8>)],
        limits: crate::pitr::WalV5Limits,
    ) -> Result<()> {
        let expected = self
            .expected_sources
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PITR restore proof sources are not canonical"))?;
        for metadata in &self.plan.proof_metadata {
            let expected_wal = expected
                .iter()
                .find(|object| {
                    object.segment_id == metadata.key.segment_id
                        && object.kind == ArchiveObjectKind::Wal
                })
                .ok_or_else(|| anyhow::anyhow!("PITR restore proof descriptor is missing"))?;
            let (_, wal) = objects
                .iter()
                .find(|(object, _)| object == expected_wal)
                .ok_or_else(|| anyhow::anyhow!("PITR restore proof WAL is missing"))?;
            let batches = decode_restore_wal_batches_for_segment(wal, metadata, limits)?;
            ensure!(
                batches.len() as u64 == metadata.batch_count
                    && batches.first().map(|batch| batch.commit_ts) == metadata.first_commit_ts
                    && batches.last().map(|batch| batch.commit_ts) == metadata.last_commit_ts,
                "PITR restore proof WAL commit range mismatch"
            );
            if let PitrRestoreTarget::CommitTs(target) = self.plan.target
                && let Some(first) = metadata.first_commit_ts
            {
                ensure!(
                    first > target,
                    "PITR restore proof does not start after the target"
                );
            }
        }
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
        self.validate_batch(batch)?;
        let next_count = self
            .applied_batches
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PITR restore batch count exhausted"))?;
        let mut next_state = self.model.clone();
        apply_batch_entries(&mut next_state, batch);
        self.model = next_state;
        self.last_commit_ts = Some(batch.commit_ts);
        self.applied_batches = next_count;
        Ok(())
    }

    fn validate_batch(&self, batch: &WalBatch) -> Result<()> {
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
        let PitrRestoreTarget::CommitTs(target) = self.plan.target else {
            bail!("PITR base restore cannot apply archived WAL batches");
        };
        ensure!(
            batch.commit_ts <= target,
            "PITR restore batch exceeds the requested target"
        );
        Ok(())
    }

    pub(crate) fn apply_wal_v5(
        &mut self,
        wal: &[u8],
        limits: crate::pitr::WalV5Limits,
    ) -> Result<()> {
        let batches = decode_restore_wal_batches(wal, limits)?;
        self.apply_batches_through_target(batches)
    }

    pub(crate) fn apply_wal_segment(
        &mut self,
        metadata: &SegmentMetadata,
        wal: &[u8],
        limits: crate::pitr::WalV5Limits,
    ) -> Result<()> {
        ensure!(
            self.plan.segments.contains(&metadata.key.segment_id),
            "PITR restore proof segment cannot be replayed"
        );
        self.validate_plan_segment(metadata)?;
        let batches = decode_restore_wal_batches_for_segment(wal, metadata, limits)?;
        validate_segment_batch_range(metadata, &batches)?;
        self.apply_batches_through_target(batches)
    }

    fn apply_batches_through_target(&mut self, batches: Vec<WalBatch>) -> Result<()> {
        let model = self.model.clone();
        let last_commit_ts = self.last_commit_ts;
        let applied_batches = self.applied_batches;
        let result = (|| {
            for batch in batches {
                if let PitrRestoreTarget::CommitTs(target) = self.plan.target
                    && batch.commit_ts > target
                {
                    break;
                }
                self.validate_batch(&batch)?;
                let next_count = self
                    .applied_batches
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("PITR restore batch count exhausted"))?;
                apply_batch_entries(&mut self.model, &batch);
                self.last_commit_ts = Some(batch.commit_ts);
                self.applied_batches = next_count;
            }
            Ok(())
        })();
        if result.is_err() {
            self.model = model;
            self.last_commit_ts = last_commit_ts;
            self.applied_batches = applied_batches;
        }
        result
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
        self.last_commit_ts = self.plan.base_included_commit_ts;
        self.applied_batches = 0;
        self.model.clear();
        self.sources_verified = false;
        self.proofs_verified = self.plan.proof_segments.is_empty();
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

    fn run_exact_restore(
        &mut self,
        materialize: impl FnOnce() -> Result<()>,
        source_objects: &[(PitrRestoreSourceObject, Vec<u8>)],
        batches: impl IntoIterator<Item = Result<WalBatch>>,
    ) -> Result<PitrRecoveryInfo> {
        let result = (|| {
            self.begin_staging()?;
            self.assign_new_timeline()?;
            self.materialize_base(materialize)?;
            self.verify_source_objects(source_objects)?;
            self.begin_apply()?;
            let batches = batches.into_iter().collect::<Result<Vec<_>>>()?;
            self.apply_batches_through_target(batches)?;
            self.finish_apply()?;
            self.persist_frontier()?;
            self.remove_recovery_wal()?;
            self.publish()?;
            self.recovery_info()
        })();
        if result.is_err() {
            let _ = self.abort();
        }
        result
    }

    pub(crate) fn run_exact_restore_with_wal(
        &mut self,
        materialize: impl FnOnce() -> Result<()>,
        source_objects: &[(PitrRestoreSourceObject, Vec<u8>)],
        metadata: &SegmentMetadata,
        limits: crate::pitr::WalV5Limits,
    ) -> Result<PitrRecoveryInfo> {
        ensure!(
            self.plan.segments.as_slice() == [metadata.key.segment_id],
            "single-WAL restore does not match the selected segment"
        );
        self.validate_plan_segment(metadata)?;
        self.run_exact_restore_with_segments(materialize, source_objects, limits)
    }

    pub(crate) fn run_exact_restore_with_segments(
        &mut self,
        materialize: impl FnOnce() -> Result<()>,
        source_objects: &[(PitrRestoreSourceObject, Vec<u8>)],
        limits: crate::pitr::WalV5Limits,
    ) -> Result<PitrRecoveryInfo> {
        let mut batches = Vec::new();
        for metadata in &self.plan.segment_metadata {
            let wal = source_objects
                .iter()
                .find(|(object, _)| {
                    object.segment_id == metadata.key.segment_id
                        && object.kind == ArchiveObjectKind::Wal
                })
                .map(|(_, bytes)| bytes.as_slice())
                .ok_or_else(|| anyhow::anyhow!("PITR restore canonical WAL object is missing"))?;
            let segment_batches = decode_restore_wal_batches_for_segment(wal, metadata, limits)?;
            validate_segment_batch_range(metadata, &segment_batches)?;
            batches.extend(segment_batches);
        }
        self.validate_proof_wals(source_objects, limits)?;
        self.proofs_verified = true;
        self.run_exact_restore(materialize, source_objects, batches.into_iter().map(Ok))
    }

    fn validate_plan_segment(&self, metadata: &SegmentMetadata) -> Result<()> {
        let canonical = self
            .plan
            .segment_metadata
            .iter()
            .chain(self.plan.proof_metadata.iter())
            .find(|candidate| candidate.key.segment_id == metadata.key.segment_id);
        ensure!(
            canonical == Some(metadata),
            "PITR restore segment metadata does not match the plan identity"
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
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

fn apply_batch_entries(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, batch: &WalBatch) {
    for entry in &batch.entries {
        match entry {
            crate::pitr::WalEntry::Put { key, value } => {
                model.insert(key.clone(), value.clone());
            }
            crate::pitr::WalEntry::PointDelete { key } => {
                model.remove(key);
            }
            crate::pitr::WalEntry::RangeDelete { start, end } => {
                let keys = model
                    .range(start.clone()..end.clone())
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                for key in keys {
                    model.remove(&key);
                }
            }
        }
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
    let segment_metadata = retained
        .iter()
        .filter(|segment| selected.contains(&segment.key.segment_id))
        .cloned()
        .collect::<Vec<_>>();
    let mut proof_segments = Vec::new();
    let mut proof_metadata = Vec::new();
    if let PitrRestoreTarget::CommitTs(target) = target {
        let target_is_represented = base.included_commit_ts == Some(target)
            || retained.iter().any(|segment| {
                segment.first_commit_ts.is_some_and(|first| first <= target)
                    && segment.last_commit_ts.is_some_and(|last| last >= target)
            });
        let gap_successor = retained
            .iter()
            .find(|segment| segment.first_commit_ts.is_some_and(|first| first > target));
        ensure!(
            target_is_represented || gap_successor.is_some(),
            "PITR restore target is not covered by the archived chain"
        );
        if !target_is_represented && let Some(successor) = gap_successor {
            proof_segments.push(successor.key.segment_id);
            proof_metadata.push(successor.clone());
        }
    }
    if let Some(last_required_index) = retained.iter().rposition(|segment| {
        selected.contains(&segment.key.segment_id)
            || proof_segments.contains(&segment.key.segment_id)
    }) {
        for segment in retained.iter().take(last_required_index + 1) {
            if segment.batch_count == 0
                && !selected.contains(&segment.key.segment_id)
                && !proof_segments.contains(&segment.key.segment_id)
            {
                proof_segments.push(segment.key.segment_id);
                proof_metadata.push(segment.clone());
            }
        }
    }
    proof_metadata.sort_by_key(|segment| segment.key.segment_id);
    proof_segments.sort();

    Ok(PitrRestorePlan {
        repository_id: base.repository_id,
        timeline_id: base.timeline_id,
        archive_epoch_id: base.archive_epoch_id,
        target,
        base_included_commit_ts: base.included_commit_ts,
        segments: selected,
        segment_metadata,
        proof_segments,
        proof_metadata,
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

    fn empty_segment(id: u64, predecessor: ChainAnchor) -> SegmentMetadata {
        let mut segment = segment(id, 1, 1, predecessor);
        segment.first_commit_ts = None;
        segment.last_commit_ts = None;
        segment.batch_count = 0;
        segment
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
    fn planner_accepts_only_successor_proven_timestamp_gaps() {
        let first = segment(
            1,
            10,
            12,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        );
        let plan = plan_exact_restore(&base(), vec![first.clone()], PitrRestoreTarget::CommitTs(9))
            .unwrap();
        assert!(plan.segments.is_empty());
        assert_eq!(plan.proof_segments, vec![SegmentId(1)]);

        let second = segment(2, 20, 22, ChainAnchor::Segment(first.anchor));
        let plan = plan_exact_restore(
            &base(),
            vec![first, second],
            PitrRestoreTarget::CommitTs(15),
        )
        .unwrap();
        assert_eq!(plan.segments, vec![SegmentId(1)]);
        assert_eq!(plan.proof_segments, vec![SegmentId(2)]);
        assert!(plan_exact_restore(&base(), vec![], PitrRestoreTarget::CommitTs(99)).is_err());

        let empty = empty_segment(
            1,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        );
        let successor = segment(2, 10, 12, ChainAnchor::Segment(empty.anchor));
        let plan = plan_exact_restore(
            &base(),
            vec![empty, successor],
            PitrRestoreTarget::CommitTs(9),
        )
        .unwrap();
        assert_eq!(plan.proof_segments, vec![SegmentId(1), SegmentId(2)]);
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
        matching.bytes = Some(wal.len() as u64);
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
    fn restore_decodes_complete_v5_wal_batches() {
        let limits = crate::pitr::WalV5Limits {
            max_input_entry_count: 8,
            max_batch_data_bytes: 1024,
            max_entry_count: 8,
            max_key_bytes: 64,
            max_value_bytes: 64,
        };
        let header = crate::pitr::encode_v5_file_header(crate::pitr::WalV5Header {
            timeline_id: TimelineId([2; 16]),
            archive_epoch_id: ArchiveEpochId([3; 16]),
            segment_id: SegmentId(1),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        })
        .unwrap();
        let batch = crate::pitr::encode_v5_batch(
            &WalBatch {
                commit_ts: 8,
                recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
                entries: vec![crate::pitr::WalEntry::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }],
            },
            limits,
        )
        .unwrap();
        let mut wal = header.to_vec();
        wal.extend_from_slice(&batch);
        let decoded = decode_restore_wal_batches(&wal, limits).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].commit_ts, 8);
    }

    #[test]
    fn exact_restore_executor_applies_decoded_wal_v5() {
        let limits = crate::pitr::WalV5Limits {
            max_input_entry_count: 8,
            max_batch_data_bytes: 1024,
            max_entry_count: 8,
            max_key_bytes: 64,
            max_value_bytes: 64,
        };
        let header = crate::pitr::encode_v5_file_header(crate::pitr::WalV5Header {
            timeline_id: TimelineId([2; 16]),
            archive_epoch_id: ArchiveEpochId([3; 16]),
            segment_id: SegmentId(1),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        })
        .unwrap();
        let batch = crate::pitr::encode_v5_batch(
            &WalBatch {
                commit_ts: 8,
                recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
                entries: vec![crate::pitr::WalEntry::Put {
                    key: b"decoded".to_vec(),
                    value: b"yes".to_vec(),
                }],
            },
            limits,
        )
        .unwrap();
        let mut wal = header.to_vec();
        wal.extend_from_slice(&batch);
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
        executor.assign_new_timeline().unwrap();
        executor.materialize_base(|| Ok(())).unwrap();
        executor.mark_sources_verified_for_test();
        executor.begin_apply().unwrap();
        executor.apply_wal_v5(&wal, limits).unwrap();
        assert_eq!(executor.get(b"decoded"), Some(b"yes".as_slice()));
    }

    #[test]
    fn exact_restore_applies_only_validated_prefix_through_target() {
        let limits = crate::pitr::WalV5Limits {
            max_input_entry_count: 8,
            max_batch_data_bytes: 1024,
            max_entry_count: 8,
            max_key_bytes: 64,
            max_value_bytes: 64,
        };
        let mut wal = crate::pitr::encode_v5_file_header(crate::pitr::WalV5Header {
            timeline_id: TimelineId([2; 16]),
            archive_epoch_id: ArchiveEpochId([3; 16]),
            segment_id: SegmentId(1),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        })
        .unwrap()
        .to_vec();
        for commit_ts in 8..=10 {
            wal.extend_from_slice(
                &crate::pitr::encode_v5_batch(
                    &WalBatch {
                        commit_ts,
                        recorded_at: crate::pitr::RecordedAt {
                            secs: commit_ts as i64,
                            nanos: 0,
                        },
                        entries: vec![crate::pitr::WalEntry::Put {
                            key: vec![commit_ts as u8],
                            value: vec![commit_ts as u8],
                        }],
                    },
                    limits,
                )
                .unwrap(),
            );
        }
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
            PitrRestoreTarget::CommitTs(9),
        )
        .unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        executor.begin_staging().unwrap();
        executor.assign_new_timeline().unwrap();
        executor.materialize_base(|| Ok(())).unwrap();
        executor.mark_sources_verified_for_test();
        executor.begin_apply().unwrap();
        executor.apply_wal_v5(&wal, limits).unwrap();
        assert_eq!(executor.get(&[8]), Some([8].as_slice()));
        assert_eq!(executor.get(&[9]), Some([9].as_slice()));
        assert!(executor.get(&[10]).is_none());
    }

    #[test]
    fn exact_restore_executor_enforces_order_and_target() {
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
        assert!(executor.publish().is_err());
        executor.begin_staging().unwrap();
        assert!(executor.assign_new_timeline().is_ok());
        executor.materialize_base(|| Ok(())).unwrap();
        executor.mark_sources_verified_for_test();
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
            PitrRestoreTarget::CommitTs(9),
        )
        .unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        executor.begin_staging().unwrap();
        executor.assign_new_timeline().unwrap();
        executor.materialize_base(|| Ok(())).unwrap();
        executor.mark_sources_verified_for_test();
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
    fn exact_restore_orchestration_aborts_on_materialization_failure() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        assert!(
            executor
                .run_exact_restore(
                    || anyhow::bail!("base copy failed"),
                    &[],
                    std::iter::empty(),
                )
                .is_err()
        );
        assert_eq!(executor.state(), ExactRestoreState::Planned);
        assert!(executor.destination_timeline_id().is_none());
    }

    #[test]
    fn exact_restore_orchestration_returns_recovery_info() {
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        let info = executor
            .run_exact_restore(|| Ok(()), &[], std::iter::empty())
            .unwrap();
        assert_eq!(info.target, PitrRestoreTarget::Base);
        assert_eq!(info.last_commit_ts, Some(7));
        assert_eq!(executor.state(), ExactRestoreState::Published);
    }

    #[test]
    fn exact_restore_orchestration_replays_wal_v5_end_to_end() {
        let limits = crate::pitr::WalV5Limits {
            max_input_entry_count: 8,
            max_batch_data_bytes: 1024,
            max_entry_count: 8,
            max_key_bytes: 64,
            max_value_bytes: 64,
        };
        let header = crate::pitr::encode_v5_file_header(crate::pitr::WalV5Header {
            timeline_id: TimelineId([2; 16]),
            archive_epoch_id: ArchiveEpochId([3; 16]),
            segment_id: SegmentId(1),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        })
        .unwrap();
        let batch = crate::pitr::encode_v5_batch(
            &WalBatch {
                commit_ts: 8,
                recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
                entries: vec![crate::pitr::WalEntry::Put {
                    key: b"end-to-end".to_vec(),
                    value: b"ok".to_vec(),
                }],
            },
            limits,
        )
        .unwrap();
        let mut wal = header.to_vec();
        wal.extend_from_slice(&batch);
        let plan = plan_exact_restore(&base(), Vec::new(), PitrRestoreTarget::Base).unwrap();
        let mut executor = ExactRestoreExecutor::new(plan);
        let metadata = segment(
            1,
            8,
            10,
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
        );
        assert!(
            executor
                .run_exact_restore_with_wal(|| Ok(()), &[], &metadata, limits)
                .is_err()
        );
    }

    #[test]
    fn proof_wal_must_match_catalog_commit_range() {
        let limits = crate::pitr::WalV5Limits {
            max_input_entry_count: 8,
            max_batch_data_bytes: 1024,
            max_entry_count: 8,
            max_key_bytes: 64,
            max_value_bytes: 64,
        };
        let predecessor = ChainAnchor::Genesis {
            archive_epoch_id: ArchiveEpochId([3; 16]),
        };
        let mut metadata = segment(1, 20, 22, predecessor);
        let mut wal = crate::pitr::encode_v5_file_header(crate::pitr::WalV5Header {
            timeline_id: TimelineId([2; 16]),
            archive_epoch_id: ArchiveEpochId([3; 16]),
            segment_id: SegmentId(1),
            predecessor,
        })
        .unwrap()
        .to_vec();
        wal.extend_from_slice(
            &crate::pitr::encode_v5_batch(
                &WalBatch {
                    commit_ts: 12,
                    recorded_at: crate::pitr::RecordedAt { secs: 2, nanos: 0 },
                    entries: vec![crate::pitr::WalEntry::Put {
                        key: b"unexpected".to_vec(),
                        value: b"commit".to_vec(),
                    }],
                },
                limits,
            )
            .unwrap(),
        );
        metadata.wal_digest = Sha256::digest(&wal).into();
        metadata.anchor.wal_digest = metadata.wal_digest;
        let seal = b"seal".to_vec();
        metadata.seal_digest = Sha256::digest(&seal).into();
        metadata.anchor.seal_digest = metadata.seal_digest;
        metadata.wal_bytes = wal.len() as u64;
        let plan = plan_exact_restore(
            &base(),
            vec![metadata.clone()],
            PitrRestoreTarget::CommitTs(15),
        )
        .unwrap();
        let expected = required_source_objects(&base(), &[metadata], &plan).unwrap();
        let loaded = expected
            .iter()
            .cloned()
            .map(|object| {
                let bytes = match object.kind {
                    ArchiveObjectKind::Wal => wal.clone(),
                    ArchiveObjectKind::Seal => seal.clone(),
                };
                (object, bytes)
            })
            .collect::<Vec<_>>();
        let mut executor = ExactRestoreExecutor::new_with_sources(plan, Some(expected));
        executor.begin_staging().unwrap();
        executor.verify_source_objects(&loaded).unwrap();
        assert!(executor.verify_proof_wals(&loaded, limits).is_err());
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

    #[cfg(target_os = "linux")]
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
