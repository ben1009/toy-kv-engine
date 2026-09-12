//! Dormant PITR archive-object publication harness.
//!
//! This module validates the object/catalog transaction boundary without
//! touching the repository filesystem or enabling live PITR archival.
#![allow(dead_code)]

use sha2::{Digest, Sha256};

#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    fs::File,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    os::unix::ffi::OsStrExt,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::pitr::{ArchiveEpochId, SegmentId, TimelineId};
use crate::pitr_catalog::{PitrCatalogRecord, SegmentMetadata, encode_catalog, replay_catalog};

#[cfg(target_os = "linux")]
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct ArchiveObjectStager {
    wal_dir: File,
}

#[cfg(target_os = "linux")]
impl ArchiveObjectStager {
    pub(crate) fn new(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let root_fd = open_dir(&root)?;
        let wal_name = CString::new("wal")?;
        let created = unsafe { libc::mkdirat(root_fd.as_raw_fd(), wal_name.as_ptr(), 0o700) } == 0;
        if !created {
            let error = std::io::Error::last_os_error();
            anyhow::ensure!(error.kind() == std::io::ErrorKind::AlreadyExists, error);
        }
        let wal_fd = open_dir_at(&root_fd, "wal")?;
        if created {
            sync_fd(&root_fd)?;
        }
        drop(root_fd);
        Ok(Self { wal_dir: wal_fd })
    }

    pub(crate) fn publish(
        &self,
        prepared: &PreparedArchiveObjects,
        wal: &[u8],
        seal: &[u8],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            prepared.wal_bytes == wal.len() as u64,
            "prepared WAL length mismatch"
        );
        anyhow::ensure!(
            prepared.seal_bytes == seal.len() as u64,
            "prepared seal length mismatch"
        );
        anyhow::ensure!(
            Sha256::digest(wal).as_slice() == prepared.wal_digest,
            "prepared WAL digest mismatch"
        );
        anyhow::ensure!(
            Sha256::digest(seal).as_slice() == prepared.seal_digest,
            "prepared seal digest mismatch"
        );
        publish_one(&self.wal_dir, &prepared.wal_name, wal)?;
        publish_one(&self.wal_dir, &prepared.seal_name, seal)?;
        sync_fd(&self.wal_dir)?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn publish_one(directory: &File, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let final_name = CString::new(name)?;
    if let Ok(existing) = open_existing(directory, &final_name) {
        let mut existing_bytes = Vec::new();
        (&existing)
            .take((bytes.len() as u64).saturating_add(1))
            .read_to_end(&mut existing_bytes)?;
        anyhow::ensure!(
            existing_bytes == bytes,
            "existing archive object identity mismatch"
        );
        return Ok(());
    }
    let (temp_name, mut temp) = create_temp(directory, name)?;
    let mut temp_consumed = false;
    let result = (|| -> anyhow::Result<()> {
        temp.write_all(bytes)?;
        temp.sync_all()?;
        let rename = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                directory.as_raw_fd(),
                temp_name.as_ptr(),
                directory.as_raw_fd(),
                final_name.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rename != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(error.into());
            }
            let existing = open_existing(directory, &final_name)?;
            let mut existing_bytes = Vec::new();
            (&existing)
                .take((bytes.len() as u64).saturating_add(1))
                .read_to_end(&mut existing_bytes)?;
            anyhow::ensure!(
                existing_bytes == bytes,
                "concurrent archive object identity mismatch"
            );
        } else {
            temp_consumed = true;
        }
        Ok(())
    })();
    drop(temp);
    if !temp_consumed {
        let unlink = unsafe { libc::unlinkat(directory.as_raw_fd(), temp_name.as_ptr(), 0) };
        if unlink != 0 && result.is_ok() {
            return Err(std::io::Error::last_os_error().into());
        }
        sync_fd(directory)?;
    }
    result
}

#[cfg(target_os = "linux")]
fn create_temp(directory: &File, name: &str) -> anyhow::Result<(CString, File)> {
    for _ in 0..64 {
        let sequence = STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_name = CString::new(format!(".{name}.tmp-{}-{sequence}", std::process::id()))?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                temp_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if fd >= 0 {
            return Ok((temp_name, unsafe { File::from_raw_fd(fd) }));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
    }
    anyhow::bail!("failed to allocate unique archive staging name")
}

#[cfg(target_os = "linux")]
fn open_existing(directory: &File, name: &CString) -> anyhow::Result<File> {
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    anyhow::ensure!(fd >= 0, std::io::Error::last_os_error());
    let file = unsafe { File::from_raw_fd(fd) };
    let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
    anyhow::ensure!(
        unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } == 0,
        std::io::Error::last_os_error()
    );
    anyhow::ensure!(
        stat.st_mode & libc::S_IFMT == libc::S_IFREG,
        "archive object is not a regular file"
    );
    Ok(file)
}

#[cfg(target_os = "linux")]
fn open_dir(path: &Path) -> anyhow::Result<File> {
    let fd = unsafe {
        libc::open(
            CString::new(path.as_os_str().as_bytes())?.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    anyhow::ensure!(fd >= 0, std::io::Error::last_os_error());
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn open_dir_at(parent: &File, name: &str) -> anyhow::Result<File> {
    let name = CString::new(name)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    anyhow::ensure!(fd >= 0, std::io::Error::last_os_error());
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn sync_fd(file: &File) -> anyhow::Result<()> {
    anyhow::ensure!(
        unsafe { libc::fsync(file.as_raw_fd()) } == 0,
        std::io::Error::last_os_error()
    );
    Ok(())
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

    #[cfg(target_os = "linux")]
    #[test]
    fn publishes_and_reuses_no_replace_objects() {
        let root = std::env::temp_dir().join(format!("toy-kv-pitr-stage-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let stager = ArchiveObjectStager::new(&root).unwrap();
        let metadata = metadata();
        let catalog = PitrArchiveCatalog::default();
        let prepared = catalog.prepare_objects(&metadata, b"wal", b"seal").unwrap();
        stager.publish(&prepared, b"wal", b"seal").unwrap();
        stager.publish(&prepared, b"wal", b"seal").unwrap();
        assert!(root.join("wal").join(&prepared.wal_name).is_file());
        assert!(root.join("wal").join(&prepared.seal_name).is_file());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejects_existing_fifo_without_blocking() {
        let root = std::env::temp_dir().join(format!("toy-kv-pitr-fifo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let stager = ArchiveObjectStager::new(&root).unwrap();
        let metadata = metadata();
        let catalog = PitrArchiveCatalog::default();
        let prepared = catalog.prepare_objects(&metadata, b"wal", b"seal").unwrap();
        let fifo = root.join("wal").join(&prepared.wal_name);
        let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(stager.publish(&prepared, b"wal", b"seal").is_err());
        std::fs::remove_dir_all(root).unwrap();
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
