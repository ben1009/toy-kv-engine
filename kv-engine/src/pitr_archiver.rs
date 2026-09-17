//! Dormant PITR archive transaction orchestration.
#![allow(dead_code)]

#[cfg(target_os = "linux")]
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(test)]
static CATALOG_PUBLICATION_TEST_MODE: std::sync::Mutex<Option<(std::path::PathBuf, u8)>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn set_catalog_publication_test_mode(repository: &std::path::Path, mode: u8) {
    *CATALOG_PUBLICATION_TEST_MODE.lock().unwrap() =
        Some((repository.join("PITR_CATALOG_LOG"), mode));
}

#[cfg(target_os = "linux")]
use anyhow::{Result, ensure};

#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};

#[cfg(target_os = "linux")]
use crate::{
    pitr_archive::{ArchiveObjectStager, ArchivePublicationOutcome, PitrArchiveCatalog},
    pitr_catalog::SegmentMetadata,
    pitr_limiter::PitrArchiveLimiter,
};

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) enum ArchiveTransactionOutcome {
    Committed {
        sequence: u64,
    },
    AlreadyCommitted {
        sequence: u64,
    },
    PublishedButNotDurable {
        sequence: u64,
        error: anyhow::Error,
    },
    PublicationUnknown {
        sequence: u64,
        fsync_error: anyhow::Error,
        revalidation_error: anyhow::Error,
    },
    RateLimited {
        wait: Duration,
    },
    Busy,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct ArchiveThrottleWait(Duration);

#[cfg(target_os = "linux")]
impl std::fmt::Display for ArchiveThrottleWait {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "archive chunk requires a limiter wait")
    }
}

#[cfg(target_os = "linux")]
impl std::error::Error for ArchiveThrottleWait {}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct PitrArchiver {
    stager: ArchiveObjectStager,
    catalog: PitrArchiveCatalog,
    catalog_path: std::path::PathBuf,
    limiter: Arc<PitrArchiveLimiter>,
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
        Self::new(root, options.limiter_options(), now)
    }

    pub(crate) fn new(
        root: impl AsRef<std::path::Path>,
        options: crate::pitr_limiter::ArchiveLimiterOptions,
        now: Instant,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let catalog_path = root.join("PITR_CATALOG_LOG");
        let catalog = match std::fs::read(&catalog_path) {
            Ok(bytes) => PitrArchiveCatalog::open(bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                PitrArchiveCatalog::default()
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            stager: ArchiveObjectStager::new(&root)?,
            catalog,
            catalog_path,
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
        self.archive_segment_inner(metadata, wal, seal, now, 2)
    }

    fn archive_segment_inner(
        &mut self,
        metadata: SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
        now: Instant,
        io_operations_per_chunk: usize,
    ) -> Result<ArchiveTransactionOutcome> {
        let prepared = self.catalog.prepare_objects(&metadata, wal, seal)?;
        let chunk_bytes = usize::try_from(
            self.limiter
                .burst_bytes()
                .checked_div(io_operations_per_chunk as u64)
                .unwrap_or(1)
                .max(1),
        )
        .unwrap_or(usize::MAX);
        let wal_chunk_count = (wal.len().saturating_add(chunk_bytes - 1) / chunk_bytes) as u64;
        let mut completed_chunks = 0_u64;
        let mut chunk_now = now;
        let publish_result =
            self.stager
                .publish_chunked(&prepared, wal, seal, chunk_bytes, |bytes| {
                    let charge = bytes
                        .checked_mul(io_operations_per_chunk as u64)
                        .and_then(NonZeroU64::new)
                        .ok_or_else(|| anyhow::anyhow!("archive chunk is empty"))?;
                    loop {
                        match self.limiter.try_grant(charge, chunk_now)? {
                            Duration::ZERO => break,
                            wait if completed_chunks == 0
                                || completed_chunks == wal_chunk_count =>
                            {
                                return Err(anyhow::Error::new(ArchiveThrottleWait(wait)));
                            }
                            wait => {
                                std::thread::sleep(wait);
                                chunk_now = Instant::now();
                            }
                        }
                    }
                    completed_chunks = completed_chunks.saturating_add(1);
                    Ok(())
                });
        if let Err(error) = publish_result {
            return match error.downcast::<ArchiveThrottleWait>() {
                Ok(wait) => Ok(ArchiveTransactionOutcome::RateLimited { wait: wait.0 }),
                Err(error) => Err(error),
            };
        }
        let previous_catalog = self.catalog.clone();
        let expected = metadata.clone();
        let publication = self.catalog.commit_segment(metadata, &prepared)?;
        if matches!(publication, ArchivePublicationOutcome::Committed { .. })
            && let Err(error) = self.persist_catalog()
        {
            let sequence = match publication {
                ArchivePublicationOutcome::Committed { sequence } => sequence,
                ArchivePublicationOutcome::AlreadyCommitted { .. } => unreachable!(),
            };
            return match self.revalidate_segment(&expected) {
                Ok(true) => {
                    Ok(ArchiveTransactionOutcome::PublishedButNotDurable { sequence, error })
                }
                Ok(false) => {
                    self.catalog = previous_catalog;
                    Err(error)
                }
                Err(revalidation_error) => Ok(ArchiveTransactionOutcome::PublicationUnknown {
                    sequence,
                    fsync_error: error,
                    revalidation_error,
                }),
            };
        }
        Ok(match publication {
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
        let chunk_bytes = usize::try_from(self.limiter.burst_bytes()).unwrap_or(usize::MAX);
        let wal = match read_source_object(
            wal_path.as_ref(),
            metadata.wal_bytes,
            metadata.wal_digest,
            chunk_bytes,
            &self.limiter,
            now,
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                return match error.downcast::<ArchiveThrottleWait>() {
                    Ok(wait) => Ok(ArchiveTransactionOutcome::RateLimited { wait: wait.0 }),
                    Err(error) => Err(error),
                };
            }
        };
        let seal = match read_source_object(
            seal_path.as_ref(),
            0,
            metadata.seal_digest,
            chunk_bytes,
            &self.limiter,
            now,
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                return match error.downcast::<ArchiveThrottleWait>() {
                    Ok(wait) => Ok(ArchiveTransactionOutcome::RateLimited { wait: wait.0 }),
                    Err(error) => Err(error),
                };
            }
        };
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

    pub(crate) fn committed_segment_ids(&self) -> Result<std::collections::BTreeSet<u64>> {
        let replay = crate::pitr_catalog::replay_catalog(self.catalog.bytes())?;
        let mut ids = std::collections::BTreeSet::new();
        for record in replay.records {
            match record {
                crate::pitr_catalog::PitrCatalogRecord::CommitSegment { metadata } => {
                    ids.insert(metadata.key.segment_id.0);
                }
                crate::pitr_catalog::PitrCatalogRecord::RetentionSnapshot(snapshot) => {
                    ids.extend(
                        snapshot
                            .segments
                            .into_iter()
                            .map(|metadata| metadata.key.segment_id.0),
                    );
                }
                crate::pitr_catalog::PitrCatalogRecord::CoverageBreak(_) => {}
            }
        }
        Ok(ids)
    }

    fn persist_catalog(&self) -> Result<()> {
        let temp_path = self.catalog_path.with_extension("tmp");
        let result = (|| -> Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            std::io::Write::write_all(&mut file, self.catalog.bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp_path, &self.catalog_path)?;
            #[cfg(test)]
            let test_mode = {
                let mut configured = CATALOG_PUBLICATION_TEST_MODE.lock().unwrap();
                if configured
                    .as_ref()
                    .is_some_and(|(path, _)| path == &self.catalog_path)
                {
                    configured.take().map_or(0, |(_, mode)| mode)
                } else {
                    0
                }
            };
            #[cfg(test)]
            match test_mode {
                1 => {
                    return Err(
                        std::io::Error::other("injected catalog directory fsync failure").into(),
                    );
                }
                2 => {
                    std::fs::remove_file(&self.catalog_path)?;
                    return Err(
                        std::io::Error::other("injected catalog publication ambiguity").into(),
                    );
                }
                _ => {}
            }
            std::fs::File::open(
                self.catalog_path
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("PITR catalog has no parent directory"))?,
            )?
            .sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
        }
        result
    }

    fn revalidate_segment(&self, expected: &SegmentMetadata) -> Result<bool> {
        let bytes = std::fs::read(&self.catalog_path)?;
        let replay = crate::pitr_catalog::replay_catalog(&bytes)?;
        Ok(replay.records.iter().any(|record| match record {
            crate::pitr_catalog::PitrCatalogRecord::CommitSegment { metadata } => {
                metadata == expected
            }
            crate::pitr_catalog::PitrCatalogRecord::RetentionSnapshot(snapshot) => snapshot
                .segments
                .iter()
                .any(|metadata| metadata == expected),
            crate::pitr_catalog::PitrCatalogRecord::CoverageBreak(_) => false,
        }))
    }
}

#[cfg(target_os = "linux")]
fn read_source_object(
    path: &std::path::Path,
    expected_bytes: u64,
    expected_digest: [u8; 32],
    chunk_bytes: usize,
    limiter: &PitrArchiveLimiter,
    now: Instant,
) -> Result<Vec<u8>> {
    anyhow::ensure!(chunk_bytes > 0, "archive chunk size is zero");
    let mut file = std::fs::File::open(path)?;
    let actual_bytes = file.metadata()?.len();
    if expected_bytes != 0 {
        anyhow::ensure!(
            actual_bytes == expected_bytes,
            "PITR source object length does not match segment metadata"
        );
    }
    let capacity = usize::try_from(actual_bytes)
        .map_err(|_| anyhow::anyhow!("source archive object is too large"))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut remaining = actual_bytes;
    let mut completed_chunks = 0_u64;
    let mut chunk_now = now;
    let mut chunk = vec![0_u8; chunk_bytes];
    while remaining > 0 {
        let amount = remaining.min(chunk_bytes as u64);
        let amount = NonZeroU64::new(amount)
            .ok_or_else(|| anyhow::anyhow!("source archive chunk is empty"))?;
        loop {
            match limiter.try_grant(amount, chunk_now)? {
                Duration::ZERO => break,
                wait if completed_chunks == 0 => {
                    return Err(anyhow::Error::new(ArchiveThrottleWait(wait)));
                }
                wait => {
                    std::thread::sleep(wait);
                    chunk_now = Instant::now();
                }
            }
        }
        std::io::Read::read_exact(&mut file, &mut chunk[..amount.get() as usize])?;
        bytes.extend_from_slice(&chunk[..amount.get() as usize]);
        remaining -= amount.get();
        completed_chunks = completed_chunks.saturating_add(1);
    }
    if expected_bytes != 0 {
        anyhow::ensure!(
            bytes.len() as u64 == expected_bytes,
            "PITR source object length does not match segment metadata"
        );
    }
    anyhow::ensure!(
        Sha256::digest(&bytes).as_slice() == expected_digest,
        "PITR source object digest does not match segment metadata"
    );
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
    fn catalog_publication_failure_revalidates_typed_outcomes() {
        for (mode, expect_unknown) in [(1, false), (2, true)] {
            let root = tempfile::tempdir().unwrap();
            std::fs::create_dir(root.path().join("wal")).unwrap();
            let mut archiver = PitrArchiver::new(
                root.path(),
                ArchiveLimiterOptions {
                    bytes_per_second: None,
                    burst_bytes: NonZeroU64::new(4096).unwrap(),
                },
                Instant::now(),
            )
            .unwrap();
            set_catalog_publication_test_mode(root.path(), mode);
            let outcome = archiver
                .archive_segment(metadata(), b"wal", b"seal", Instant::now())
                .unwrap();
            assert_eq!(
                matches!(
                    &outcome,
                    ArchiveTransactionOutcome::PublicationUnknown { .. }
                ),
                expect_unknown
            );
            assert_eq!(
                matches!(
                    &outcome,
                    ArchiveTransactionOutcome::PublishedButNotDurable { .. }
                ),
                !expect_unknown
            );
        }
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

    #[test]
    fn archiver_reloads_durable_catalog_on_reopen() {
        let root = std::env::temp_dir().join(format!("toy-kv-pitr-reopen-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let start = Instant::now();
        let first = metadata();
        let mut archiver = PitrArchiver::new(
            &root,
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            start,
        )
        .unwrap();
        assert!(matches!(
            archiver
                .archive_segment(first.clone(), b"wal", b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::Committed { sequence: 1 }
        ));
        drop(archiver);

        let mut reopened = PitrArchiver::new(
            &root,
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            start,
        )
        .unwrap();
        assert!(matches!(
            reopened
                .archive_segment(first, b"wal", b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::AlreadyCommitted { sequence: 1 }
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
}
