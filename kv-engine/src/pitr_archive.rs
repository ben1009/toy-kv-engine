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
    wal_name: String,
    seal_name: String,
    wal_bytes: u64,
    seal_bytes: u64,
    segment_key: crate::pitr_catalog::SegmentKey,
    wal_digest: [u8; 32],
    seal_digest: [u8; 32],
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
        anyhow::ensure!(!seal.is_empty(), "archived seal must be nonempty");
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
            segment_key: metadata.key,
            wal_digest: metadata.wal_digest,
            seal_digest: metadata.seal_digest,
        })
    }

    pub(crate) fn commit_segment(
        &mut self,
        metadata: SegmentMetadata,
        prepared: &PreparedArchiveObjects,
    ) -> anyhow::Result<ArchivePublicationOutcome> {
        let expected_wal_name = archive_object_name(
            metadata.key.timeline_id,
            metadata.key.archive_epoch_id,
            metadata.key.segment_id,
            ArchiveObjectKind::Wal,
            metadata.wal_digest,
        );
        let expected_seal_name = archive_object_name(
            metadata.key.timeline_id,
            metadata.key.archive_epoch_id,
            metadata.key.segment_id,
            ArchiveObjectKind::Seal,
            metadata.seal_digest,
        );
        anyhow::ensure!(
            prepared.wal_name == expected_wal_name,
            "prepared WAL identity does not match segment"
        );
        anyhow::ensure!(
            prepared.seal_name == expected_seal_name,
            "prepared seal identity does not match segment"
        );
        anyhow::ensure!(
            prepared.wal_bytes == metadata.wal_bytes,
            "prepared WAL length does not match segment"
        );
        anyhow::ensure!(prepared.seal_bytes > 0, "prepared seal must be nonempty");
        anyhow::ensure!(
            prepared.segment_key == metadata.key,
            "prepared segment key does not match segment"
        );
        anyhow::ensure!(
            prepared.wal_digest == metadata.wal_digest,
            "prepared WAL digest does not match segment"
        );
        anyhow::ensure!(
            prepared.seal_digest == metadata.seal_digest,
            "prepared seal digest does not match segment"
        );
        let replay = replay_catalog(&self.bytes)?;
        anyhow::ensure!(
            replay.retained_offset == self.bytes.len(),
            "catalog has an unreconciled incomplete terminal frame"
        );
        for (index, record) in replay.records.iter().enumerate() {
            if let PitrCatalogRecord::CommitSegment { metadata: existing } = record
                && existing.key == metadata.key
            {
                anyhow::ensure!(
                    existing == &metadata,
                    "archived segment metadata conflicts with catalog"
                );
                let first_sequence = replay_first_sequence(&replay.records)?;
                return Ok(ArchivePublicationOutcome::AlreadyCommitted {
                    sequence: first_sequence
                        .checked_add(index as u64)
                        .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted"))?,
                });
            }
        }
        let mut records = replay.records;
        records.push(PitrCatalogRecord::CommitSegment { metadata });
        self.bytes = encode_catalog(&records)?;
        let first_sequence = replay_first_sequence(&records)?;
        Ok(ArchivePublicationOutcome::Committed {
            sequence: first_sequence
                .checked_add(records.len() as u64 - 1)
                .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted"))?,
        })
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn replay_first_sequence(records: &[PitrCatalogRecord]) -> anyhow::Result<u64> {
    match records.first() {
        Some(PitrCatalogRecord::RetentionSnapshot(snapshot)) => snapshot
            .replaced_prefix_high_water
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("catalog sequence exhausted")),
        _ => Ok(1),
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
            catalog.commit_segment(metadata.clone(), &prepared).unwrap(),
            ArchivePublicationOutcome::Committed { sequence: 1 }
        ));
        assert!(matches!(
            catalog.commit_segment(metadata, &prepared).unwrap(),
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

    #[test]
    fn duplicate_commit_reports_wire_sequence_after_snapshot_replacement() {
        let snapshot =
            PitrCatalogRecord::RetentionSnapshot(crate::pitr_catalog::RetentionSnapshot {
                repository_id: [1; 16],
                replaced_prefix_high_water: 10,
                replaced_prefix_digest: [5; 32],
                chain_starts: Vec::new(),
                segments: Vec::new(),
                breaks: Vec::new(),
                retention_cutoff: None,
                oldest_advertised_commit_ts: None,
                backup_catalog_high_water: 0,
                backup_catalog_digest: [0; 32],
            });
        let bytes = crate::pitr_catalog::encode_catalog(&[snapshot]).unwrap();
        let mut catalog = PitrArchiveCatalog::open(bytes).unwrap();
        let metadata = metadata();
        let prepared = catalog.prepare_objects(&metadata, b"wal", b"seal").unwrap();
        assert!(matches!(
            catalog.commit_segment(metadata.clone(), &prepared).unwrap(),
            ArchivePublicationOutcome::Committed { sequence: 12 }
        ));
        assert!(matches!(
            catalog.commit_segment(metadata, &prepared).unwrap(),
            ArchivePublicationOutcome::AlreadyCommitted { sequence: 12 }
        ));
    }

    #[test]
    fn commit_refuses_to_discard_an_incomplete_catalog_tail() {
        let first = metadata();
        let mut second = first.clone();
        second.key.segment_id = SegmentId(2);
        second.anchor.segment_id = SegmentId(2);
        second.predecessor = ChainAnchor::Segment(first.anchor);
        second.first_commit_ts = Some(2);
        second.last_commit_ts = Some(2);
        let complete = crate::pitr_catalog::encode_catalog(&[
            PitrCatalogRecord::CommitSegment { metadata: first },
            PitrCatalogRecord::CommitSegment {
                metadata: second.clone(),
            },
        ])
        .unwrap();
        let torn = complete[..complete.len() - 2].to_vec();
        let mut catalog = PitrArchiveCatalog::open(torn.clone()).unwrap();
        let prepared = catalog.prepare_objects(&second, b"wal", b"seal").unwrap();
        assert!(catalog.commit_segment(second, &prepared).is_err());
        assert_eq!(catalog.bytes(), torn);
    }
}
