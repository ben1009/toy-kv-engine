//! Dormant PITR archive transaction orchestration.
#![allow(dead_code)]

#[cfg(target_os = "linux")]
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use anyhow::{Result, ensure};

#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};

#[cfg(target_os = "linux")]
use crate::{
    pitr_archive::{ArchiveObjectStager, ArchivePublicationOutcome, PitrArchiveCatalog},
    pitr_catalog::SegmentMetadata,
    pitr_limiter::{ArchiveStreamId, PitrArchiveLimiter, StreamGrantOutcome},
};

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchiveTransactionOutcome {
    Committed { sequence: u64 },
    AlreadyCommitted { sequence: u64 },
    RateLimited { wait: Duration },
    Busy,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct PitrArchiver {
    stager: ArchiveObjectStager,
    catalog: PitrArchiveCatalog,
    limiter: Arc<PitrArchiveLimiter>,
    priority: Arc<parking_lot::Mutex<crate::pitr_api::ArchiveIoPriority>>,
}

#[cfg(target_os = "linux")]
impl PitrArchiver {
    #[allow(dead_code)]
    pub(crate) fn new_with_runtime_options(
        root: impl AsRef<std::path::Path>,
        options: &crate::pitr_api::PitrRuntimeOptions,
        now: Instant,
    ) -> Result<Self> {
        options.validate()?;
        Self::new_with_limiter_and_priority(
            root,
            Arc::new(PitrArchiveLimiter::new(options.limiter_options(), now)),
            Arc::new(parking_lot::Mutex::new(options.archive_io_priority)),
        )
    }

    pub(crate) fn new(
        root: impl AsRef<std::path::Path>,
        options: crate::pitr_limiter::ArchiveLimiterOptions,
        now: Instant,
    ) -> Result<Self> {
        Self::new_with_limiter(root, Arc::new(PitrArchiveLimiter::new(options, now)))
    }

    pub(crate) fn new_with_limiter(
        root: impl AsRef<std::path::Path>,
        limiter: Arc<PitrArchiveLimiter>,
    ) -> Result<Self> {
        Self::new_with_limiter_and_priority(
            root,
            limiter,
            Arc::new(parking_lot::Mutex::new(
                crate::pitr_api::ArchiveIoPriority::Background,
            )),
        )
    }

    pub(crate) fn new_with_limiter_and_priority(
        root: impl AsRef<std::path::Path>,
        limiter: Arc<PitrArchiveLimiter>,
        priority: Arc<parking_lot::Mutex<crate::pitr_api::ArchiveIoPriority>>,
    ) -> Result<Self> {
        Ok(Self {
            stager: ArchiveObjectStager::new(root)?,
            catalog: PitrArchiveCatalog::default(),
            limiter,
            priority,
        })
    }

    pub(crate) fn archive_segment(
        &mut self,
        metadata: SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
        now: Instant,
    ) -> Result<ArchiveTransactionOutcome> {
        self.archive_segment_inner(metadata, wal, seal, now, 2)
    }

    fn archive_segment_inner(
        &mut self,
        metadata: SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
        now: Instant,
        io_multiplier: u64,
    ) -> Result<ArchiveTransactionOutcome> {
        let prepared = self.catalog.prepare_objects(&metadata, wal, seal)?;
        let aggregate = u64::try_from(wal.len())
            .and_then(|wal_bytes| {
                u64::try_from(seal.len()).map(|seal_bytes| wal_bytes.checked_add(seal_bytes))
            })
            .ok()
            .flatten()
            .and_then(|bytes| bytes.checked_mul(io_multiplier))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| anyhow::anyhow!("archive I/O charge overflow or is zero"))?;
        let id = archive_stream_id(&metadata);
        match self.limiter.try_grant_stream(id, aggregate, now)? {
            StreamGrantOutcome::Granted => {}
            StreamGrantOutcome::Wait(wait) => {
                return Ok(ArchiveTransactionOutcome::RateLimited { wait });
            }
            StreamGrantOutcome::Busy => return Ok(ArchiveTransactionOutcome::Busy),
        }
        if *self.priority.lock() == crate::pitr_api::ArchiveIoPriority::Background {
            std::thread::yield_now();
        }
        self.stager.publish(&prepared, wal, seal)?;
        Ok(match self.catalog.commit_segment(metadata, &prepared)? {
            ArchivePublicationOutcome::Committed { sequence } => {
                ArchiveTransactionOutcome::Committed { sequence }
            }
            ArchivePublicationOutcome::AlreadyCommitted { sequence } => {
                ArchiveTransactionOutcome::AlreadyCommitted { sequence }
            }
        })
    }

    pub(crate) fn archive_segment_from_paths(
        &mut self,
        metadata: SegmentMetadata,
        wal_path: impl AsRef<std::path::Path>,
        seal_path: impl AsRef<std::path::Path>,
        now: Instant,
    ) -> Result<ArchiveTransactionOutcome> {
        let wal_path = wal_path.as_ref();
        let seal_path = seal_path.as_ref();
        let wal_bytes = std::fs::metadata(wal_path)?.len();
        ensure!(
            wal_bytes == metadata.wal_bytes,
            "PITR WAL length does not match segment metadata"
        );
        let seal_bytes = std::fs::metadata(seal_path)?.len();
        let source_bytes = metadata
            .wal_bytes
            .checked_add(seal_bytes)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| anyhow::anyhow!("archive source size overflow or is zero"))?;
        match self
            .limiter
            .try_grant_stream(archive_stream_id(&metadata), source_bytes, now)?
        {
            StreamGrantOutcome::Granted => {}
            StreamGrantOutcome::Wait(wait) => {
                return Ok(ArchiveTransactionOutcome::RateLimited { wait });
            }
            StreamGrantOutcome::Busy => return Ok(ArchiveTransactionOutcome::Busy),
        }
        let wal = read_bounded_source(wal_path, wal_bytes)?;
        let seal = read_bounded_source(seal_path, seal_bytes)?;
        ensure!(
            wal.len() as u64 == metadata.wal_bytes,
            "PITR WAL length does not match segment metadata"
        );
        ensure!(
            Sha256::digest(&wal).as_slice() == metadata.wal_digest,
            "PITR WAL digest does not match segment metadata"
        );
        ensure!(
            Sha256::digest(&seal).as_slice() == metadata.seal_digest,
            "PITR seal digest does not match segment metadata"
        );
        self.archive_segment_inner(metadata, &wal, &seal, now, 1)
    }

    pub(crate) fn catalog_bytes(&self) -> &[u8] {
        self.catalog.bytes()
    }
}

#[cfg(target_os = "linux")]
fn archive_stream_id(metadata: &SegmentMetadata) -> ArchiveStreamId {
    let mut digest = Sha256::new();
    digest.update(metadata.key.repository_id);
    digest.update(metadata.key.timeline_id.0);
    digest.update(metadata.key.archive_epoch_id.0);
    digest.update(metadata.key.segment_id.0.to_be_bytes());
    digest.update(metadata.wal_digest);
    digest.update(metadata.seal_digest);
    ArchiveStreamId(digest.finalize().into())
}

#[cfg(target_os = "linux")]
fn read_bounded_source(path: &std::path::Path, length: u64) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let capacity =
        usize::try_from(length).map_err(|_| anyhow::anyhow!("PITR source object is too large"))?;
    let mut bytes = vec![0_u8; capacity];
    std::io::Read::read_exact(&mut file, &mut bytes)?;
    Ok(bytes)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::{
        pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId},
        pitr_catalog::{SegmentKey, SegmentMetadata},
        pitr_limiter::ArchiveLimiterOptions,
    };
    use sha2::{Digest, Sha256};

    fn metadata() -> SegmentMetadata {
        let wal_digest = Sha256::digest(b"wal").into();
        let seal_digest = Sha256::digest(b"seal").into();
        SegmentMetadata {
            key: SegmentKey {
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
    fn throttling_happens_before_publication_and_retry_commits() {
        let root =
            std::env::temp_dir().join(format!("toy-kv-pitr-archiver-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let start = Instant::now();
        let mut archiver = PitrArchiver::new(
            &root,
            ArchiveLimiterOptions {
                bytes_per_second: NonZeroU64::new(10),
                burst_bytes: NonZeroU64::new(20).unwrap(),
            },
            start,
        )
        .unwrap();
        let first = metadata();
        assert!(matches!(
            archiver
                .archive_segment(first.clone(), b"wal", b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::Committed { sequence: 1 }
        ));
        assert!(matches!(
            archiver
                .archive_segment(first.clone(), b"wal", b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::RateLimited { .. }
        ));
        assert!(matches!(
            archiver
                .archive_segment(first, b"wal", b"seal", start + Duration::from_secs(2))
                .unwrap(),
            ArchiveTransactionOutcome::AlreadyCommitted { sequence: 1 }
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archiver_reads_and_verifies_sealed_source_paths() {
        let root = std::env::temp_dir().join(format!("toy-kv-pitr-paths-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let wal_path = root.join("source.wal");
        let seal_path = root.join("source.seal");
        std::fs::write(&wal_path, b"wal").unwrap();
        std::fs::write(&seal_path, b"seal").unwrap();
        let mut archiver = PitrArchiver::new(
            &root,
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
        assert!(matches!(
            archiver
                .archive_segment_from_paths(metadata(), &wal_path, &seal_path, Instant::now())
                .unwrap(),
            ArchiveTransactionOutcome::Committed { sequence: 1 }
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
}
