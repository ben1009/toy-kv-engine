//! Dormant PITR archive transaction orchestration.
#![allow(dead_code)]

#[cfg(target_os = "linux")]
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use anyhow::Result;

#[cfg(target_os = "linux")]
use crate::{
    pitr_archive::{ArchiveObjectStager, ArchivePublicationOutcome, PitrArchiveCatalog},
    pitr_catalog::SegmentMetadata,
    pitr_limiter::PitrArchiveLimiter,
};

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchiveTransactionOutcome {
    Committed { sequence: u64 },
    AlreadyCommitted { sequence: u64 },
    RateLimited { wait: Duration },
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct PitrArchiver {
    stager: ArchiveObjectStager,
    catalog: PitrArchiveCatalog,
    limiter: Arc<PitrArchiveLimiter>,
}

#[cfg(target_os = "linux")]
impl PitrArchiver {
    pub(crate) fn new(
        root: impl AsRef<std::path::Path>,
        options: crate::pitr_limiter::ArchiveLimiterOptions,
        now: Instant,
    ) -> Result<Self> {
        Ok(Self {
            stager: ArchiveObjectStager::new(root)?,
            catalog: PitrArchiveCatalog::default(),
            limiter: Arc::new(PitrArchiveLimiter::new(options, now)),
        })
    }

    pub(crate) fn archive_segment(
        &mut self,
        metadata: SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
        now: Instant,
    ) -> Result<ArchiveTransactionOutcome> {
        let prepared = self.catalog.prepare_objects(&metadata, wal, seal)?;
        let aggregate = u64::try_from(wal.len())
            .and_then(|wal_bytes| {
                u64::try_from(seal.len()).map(|seal_bytes| wal_bytes.checked_add(seal_bytes))
            })
            .ok()
            .flatten()
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| anyhow::anyhow!("archive I/O charge overflow or is zero"))?;
        let wait = self.limiter.try_grant(aggregate, now)?;
        if !wait.is_zero() {
            return Ok(ArchiveTransactionOutcome::RateLimited { wait });
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

    pub(crate) fn catalog_bytes(&self) -> &[u8] {
        self.catalog.bytes()
    }
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
}
