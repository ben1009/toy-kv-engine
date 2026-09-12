//! Dormant PITR archive-object publication harness.
//!
//! This module validates the object/catalog transaction boundary without
//! touching the repository filesystem or enabling live PITR archival.
#![allow(dead_code)]

use sha2::{Digest, Sha256};

use crate::pitr::{ArchiveEpochId, SegmentId, TimelineId};
use crate::pitr_catalog::{PitrCatalogRecord, SegmentMetadata, encode_catalog, replay_catalog};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchiveObjectKind {
    Wal,
    Seal,
}

pub(crate) fn archive_object_name(
    timeline_id: TimelineId,
    archive_epoch_id: ArchiveEpochId,
    segment_id: SegmentId,
    kind: ArchiveObjectKind,
    digest: [u8; 32],
) -> String {
    let timeline = hex(timeline_id.0);
    let epoch = hex(archive_epoch_id.0);
    let digest = hex(digest);
    let suffix = match kind {
        ArchiveObjectKind::Wal => "wal",
        ArchiveObjectKind::Seal => "seal",
    };
    format!("{timeline}-{epoch}-{:016x}-{digest}.{suffix}", segment_id.0)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedArchiveObjects {
    pub(crate) wal_name: String,
    pub(crate) seal_name: String,
    pub(crate) wal_bytes: u64,
    pub(crate) seal_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivePublicationOutcome {
    Committed { sequence: u64 },
    AlreadyCommitted { sequence: u64 },
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PitrArchiveCatalog {
    bytes: Vec<u8>,
}

impl PitrArchiveCatalog {
    pub(crate) fn open(bytes: Vec<u8>) -> anyhow::Result<Self> {
        replay_catalog(&bytes)?;
        Ok(Self { bytes })
    }

    pub(crate) fn prepare_objects(
        &self,
        metadata: &SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
    ) -> anyhow::Result<PreparedArchiveObjects> {
        anyhow::ensure!(
            metadata.wal_bytes == wal.len() as u64,
            "archived WAL length mismatch"
        );
        anyhow::ensure!(
            Sha256::digest(wal).as_slice() == metadata.wal_digest,
            "archived WAL digest mismatch"
        );
        anyhow::ensure!(
            Sha256::digest(seal).as_slice() == metadata.seal_digest,
            "archived seal digest mismatch"
        );
        Ok(PreparedArchiveObjects {
            wal_name: archive_object_name(
                metadata.key.timeline_id,
                metadata.key.archive_epoch_id,
                metadata.key.segment_id,
                ArchiveObjectKind::Wal,
                metadata.wal_digest,
            ),
            seal_name: archive_object_name(
                metadata.key.timeline_id,
                metadata.key.archive_epoch_id,
                metadata.key.segment_id,
                ArchiveObjectKind::Seal,
                metadata.seal_digest,
            ),
            wal_bytes: wal.len() as u64,
            seal_bytes: seal.len() as u64,
        })
    }

    pub(crate) fn commit_segment(
        &mut self,
        metadata: SegmentMetadata,
    ) -> anyhow::Result<ArchivePublicationOutcome> {
        let replay = replay_catalog(&self.bytes)?;
        for (index, record) in replay.records.iter().enumerate() {
            if let PitrCatalogRecord::CommitSegment { metadata: existing } = record
                && existing.key == metadata.key
            {
                anyhow::ensure!(
                    existing == &metadata,
                    "archived segment metadata conflicts with catalog"
                );
                return Ok(ArchivePublicationOutcome::AlreadyCommitted {
                    sequence: index as u64 + 1,
                });
            }
        }
        let mut records = replay.records;
        records.push(PitrCatalogRecord::CommitSegment { metadata });
        self.bytes = encode_catalog(&records)?;
        Ok(ArchivePublicationOutcome::Committed {
            sequence: records.len() as u64,
        })
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn hex<const N: usize>(bytes: [u8; N]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId};

    fn metadata() -> SegmentMetadata {
        let wal_digest = Sha256::digest(b"wal").into();
        let seal_digest = Sha256::digest(b"seal").into();
        SegmentMetadata {
            key: crate::pitr_catalog::SegmentKey {
                repository_id: [1; 16],
                timeline_id: TimelineId([2; 16]),
                archive_epoch_id: ArchiveEpochId([3; 16]),
                segment_id: SegmentId(1),
            },
            wal_format_version: 5,
            seal_format_version: 1,
            anchor: SegmentAnchor {
                segment_id: SegmentId(1),
                wal_digest,
                seal_digest,
            },
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
            first_commit_ts: Some(1),
            last_commit_ts: Some(1),
            batch_count: 1,
            logical_bytes: 3,
            wal_bytes: 3,
            wal_digest,
            seal_digest,
            source_identity: [4; 32],
        }
    }

    #[test]
    fn prepares_identity_bound_objects_and_commits_once() {
        let mut catalog = PitrArchiveCatalog::default();
        let metadata = metadata();
        let prepared = catalog.prepare_objects(&metadata, b"wal", b"seal").unwrap();
        assert!(prepared.wal_name.ends_with(".wal"));
        assert!(prepared.seal_name.ends_with(".seal"));
        assert!(matches!(
            catalog.commit_segment(metadata.clone()).unwrap(),
            ArchivePublicationOutcome::Committed { sequence: 1 }
        ));
        assert!(matches!(
            catalog.commit_segment(metadata).unwrap(),
            ArchivePublicationOutcome::AlreadyCommitted { sequence: 1 }
        ));
    }

    #[test]
    fn rejects_object_identity_mismatch() {
        let catalog = PitrArchiveCatalog::default();
        assert!(
            catalog
                .prepare_objects(&metadata(), b"wrong", b"seal")
                .is_err()
        );
    }
}
