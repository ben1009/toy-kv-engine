//! PITR archive transaction orchestration.
#![allow(dead_code)]

#[cfg(target_os = "linux")]
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(test)]
static CATALOG_PUBLICATION_TEST_MODE: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u8>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Arms the injected catalog-publication outcome for one repository.
///
/// Keyed by the repository's catalog path rather than held in a single slot:
/// the unit tests that use this hook live in the same test binary and run in
/// parallel, so a shared slot would let whichever test set it last shadow the
/// others and silently drop the injection the caller asked for.
#[cfg(test)]
pub(crate) fn set_catalog_publication_test_mode(repository: &std::path::Path, mode: u8) {
    CATALOG_PUBLICATION_TEST_MODE
        .lock()
        .unwrap()
        .insert(repository.join("PITR_CATALOG_LOG"), mode);
}

#[cfg(target_os = "linux")]
use anyhow::{Context, Result};

#[cfg(target_os = "linux")]
use crate::{
    pitr::archive::{ArchiveObjectStager, ArchivePublicationOutcome, PitrArchiveCatalog},
    pitr::catalog::SegmentMetadata,
    pitr::limiter::PitrArchiveLimiter,
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
    priority: Arc<parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
}

#[cfg(target_os = "linux")]
impl PitrArchiver {
    pub(crate) fn staging_bytes(&self) -> u64 {
        self.stager.staging_bytes()
    }

    #[allow(dead_code)]
    pub(crate) fn new_with_runtime_options(
        root: impl AsRef<std::path::Path>,
        options: &crate::pitr::api::PitrRuntimeOptions,
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
        options: crate::pitr::limiter::ArchiveLimiterOptions,
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
                crate::pitr::api::ArchiveIoPriority::Background,
            )),
        )
    }

    pub(crate) fn new_with_limiter_and_priority(
        root: impl AsRef<std::path::Path>,
        limiter: Arc<PitrArchiveLimiter>,
        priority: Arc<parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
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
        self.archive_segment_inner(metadata, wal, seal, now, 2, None)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn archive_segment_cancellable(
        &mut self,
        metadata: SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
        now: Instant,
        cancellation: &std::sync::atomic::AtomicBool,
    ) -> Result<ArchiveTransactionOutcome> {
        self.archive_segment_inner(metadata, wal, seal, now, 2, Some(cancellation))
    }

    fn archive_segment_inner(
        &mut self,
        metadata: SegmentMetadata,
        wal: &[u8],
        seal: &[u8],
        now: Instant,
        io_operations_per_chunk: usize,
        cancellation: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<ArchiveTransactionOutcome> {
        let _repository_lock = self.stager.lock_exclusive()?;
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
        if *self.priority.lock() == crate::pitr::api::ArchiveIoPriority::Background {
            std::thread::yield_now();
        }
        let publish_result = self.stager.publish_chunked(
            &prepared,
            wal,
            seal,
            chunk_bytes,
            Some(&self.priority),
            |bytes| {
                check_archive_cancellation(cancellation)?;
                let charge = bytes
                    .checked_mul(io_operations_per_chunk as u64)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| anyhow::anyhow!("archive chunk is empty"))?;
                loop {
                    match self.limiter.try_grant(charge, chunk_now)? {
                        Duration::ZERO => break,
                        wait if completed_chunks == 0 || completed_chunks == wal_chunk_count => {
                            return Err(anyhow::Error::new(ArchiveThrottleWait(wait)));
                        }
                        wait => {
                            wait_for_archive_tokens(wait, cancellation)?;
                            chunk_now = Instant::now();
                        }
                    }
                }
                completed_chunks = completed_chunks.saturating_add(1);

                Ok(())
            },
        );
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
        self.archive_segment_from_paths_cancellable(metadata, wal_path, seal_path, now, None)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn archive_segment_from_paths_cancellable(
        &mut self,
        metadata: SegmentMetadata,
        wal_path: impl AsRef<std::path::Path>,
        seal_path: impl AsRef<std::path::Path>,
        now: Instant,
        cancellation: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<ArchiveTransactionOutcome> {
        check_archive_cancellation(cancellation)?;
        let chunk_bytes = usize::try_from(self.limiter.burst_bytes()).unwrap_or(usize::MAX);
        let wal = match read_source_object(
            wal_path.as_ref(),
            metadata.wal_bytes,
            metadata.wal_digest,
            crate::pitr::archive::SourceObjectDigest::Wal(metadata.wal_digest_rule()?),
            chunk_bytes,
            &self.limiter,
            &self.priority,
            now,
            cancellation,
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
            crate::pitr::archive::SourceObjectDigest::WholeObject,
            chunk_bytes,
            &self.limiter,
            &self.priority,
            now,
            cancellation,
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                return match error.downcast::<ArchiveThrottleWait>() {
                    Ok(wait) => Ok(ArchiveTransactionOutcome::RateLimited { wait: wait.0 }),
                    Err(error) => Err(error),
                };
            }
        };

        self.archive_segment_inner(metadata, &wal, &seal, now, 1, cancellation)
    }

    pub(crate) fn catalog_bytes(&self) -> &[u8] {
        self.catalog.bytes()
    }

    pub(crate) fn committed_segment_ids(&self) -> Result<std::collections::BTreeSet<u64>> {
        let replay = crate::pitr::catalog::replay_catalog(self.catalog.bytes())?;
        let mut ids = std::collections::BTreeSet::new();

        for record in replay.records {
            match record {
                crate::pitr::catalog::PitrCatalogRecord::CommitSegment { metadata } => {
                    ids.insert(metadata.key.segment_id.0);
                }
                crate::pitr::catalog::PitrCatalogRecord::RetentionSnapshot(snapshot) => {
                    ids.extend(
                        snapshot
                            .segments
                            .into_iter()
                            .map(|metadata| metadata.key.segment_id.0),
                    );
                }
                crate::pitr::catalog::PitrCatalogRecord::CoverageBreak(_) => {}
            }
        }
        Ok(ids)
    }

    fn persist_catalog(&self) -> Result<()> {
        let temp_path = self.catalog_path.with_extension("tmp");
        // A crash between creating and renaming the temporary leaves it behind,
        // and `create_new` would then fail on every later attempt.
        remove_stale_temp(&temp_path)?;
        let result = (|| -> Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            std::io::Write::write_all(&mut file, self.catalog.bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp_path, &self.catalog_path)?;
            #[cfg(test)]
            if std::env::var_os("PITR_PROCESS_KILL_AFTER_CATALOG_RENAME").is_some() {
                // SAFETY: this is an isolated child-process crash test. It
                // intentionally terminates immediately after publication and
                // before the parent-directory sync boundary.
                unsafe { libc::_exit(137) }
            }
            #[cfg(test)]
            let test_mode = {
                let mut configured = CATALOG_PUBLICATION_TEST_MODE.lock().unwrap();
                configured.remove(&self.catalog_path).unwrap_or(0)
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
            #[cfg(test)]
            if std::env::var_os("PITR_PROCESS_KILL_AFTER_CATALOG_DIR_SYNC").is_some() {
                // SAFETY: this is an isolated child-process crash test after
                // the catalog directory durability boundary.
                unsafe { libc::_exit(137) }
            }
            Ok(())
        })();

        if result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
        }
        result
    }

    fn revalidate_segment(&self, expected: &SegmentMetadata) -> Result<bool> {
        let bytes = std::fs::read(&self.catalog_path)?;
        let replay = crate::pitr::catalog::replay_catalog(&bytes)?;

        Ok(replay.records.iter().any(|record| match record {
            crate::pitr::catalog::PitrCatalogRecord::CommitSegment { metadata } => {
                metadata == expected
            }
            crate::pitr::catalog::PitrCatalogRecord::RetentionSnapshot(snapshot) => snapshot
                .segments
                .iter()
                .any(|metadata| metadata == expected),
            crate::pitr::catalog::PitrCatalogRecord::CoverageBreak(_) => false,
        }))
    }
}

/// Clear a staging file a crashed run may have left behind, so the next
/// `create_new` does not fail on it.
fn remove_stale_temp(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to remove stale PITR catalog temporary {}",
                path.display()
            )
        }),
    }
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn read_source_object(
    path: &std::path::Path,
    expected_bytes: u64,
    expected_digest: [u8; 32],
    digest: crate::pitr::archive::SourceObjectDigest,
    chunk_bytes: usize,
    limiter: &PitrArchiveLimiter,
    priority: &parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>,
    now: Instant,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
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
    // The buffer holds one chunk, and no chunk can be larger than what is left of
    // the file - so sizing it from the limiter burst would reserve a burst of
    // memory for every source, including a segment holding a few bytes. The grant
    // below still asks for the burst-sized chunk, which is what paces the I/O.
    let read_bytes = chunk_bytes.min(capacity);
    let mut chunk = vec![0_u8; read_bytes];
    while remaining > 0 {
        check_archive_cancellation(cancellation)?;
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
                    wait_for_archive_tokens(wait, cancellation)?;
                    chunk_now = Instant::now();
                }
            }
        }
        if *priority.lock() == crate::pitr::api::ArchiveIoPriority::Background {
            std::thread::yield_now();
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
        digest.digest(&bytes)?.as_slice() == expected_digest,
        "PITR source object digest does not match segment metadata"
    );

    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn check_archive_cancellation(cancellation: Option<&std::sync::atomic::AtomicBool>) -> Result<()> {
    if cancellation.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire)) {
        anyhow::bail!("PITR archive cancelled between I/O chunks");
    }
    Ok(())
}

/// Sleeps out a rate-limiter wait, giving up early if the archive is cancelled.
///
/// A configured `archive_io_bytes_per_second` can hand back a wait measured in
/// seconds, and the limiter loops that call this sleep it off the archiver's
/// thread. Sleeping it in one call would hold a cancellation - a close, or a
/// dropped async task - for the whole wait, so the sleep is sliced and cancellation
/// is rechecked between slices. The total slept is unchanged, so the pacing the
/// limiter asked for is not weakened.
#[cfg(target_os = "linux")]
fn wait_for_archive_tokens(
    mut wait: Duration,
    cancellation: Option<&std::sync::atomic::AtomicBool>,
) -> Result<()> {
    const SLICE: Duration = Duration::from_millis(50);

    while !wait.is_zero() {
        check_archive_cancellation(cancellation)?;
        let slice = wait.min(SLICE);
        std::thread::sleep(slice);
        wait -= slice;
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::{
        pitr::catalog::{SegmentKey, SegmentMetadata},
        pitr::limiter::ArchiveLimiterOptions,
        pitr::{ArchiveEpochId, ChainAnchor, SegmentAnchor, SegmentId, TimelineId},
    };
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;

    /// The segment's identity, shared by its WAL header and its metadata.
    fn header() -> crate::pitr::WalV5Header {
        crate::tests::harness::pitr_segment_header(
            TimelineId([2; 16]),
            ArchiveEpochId([3; 16]),
            SegmentId(1),
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
            crate::pitr::WAL_V5_VERSION,
        )
    }

    /// A real v5 WAL, not a stand-in: the digest rules are defined over the
    /// encoding, so a payload that is not a WAL cannot be verified under one.
    fn wal_bytes() -> Vec<u8> {
        crate::tests::harness::pitr_segment_wal_bytes(header())
    }

    fn metadata() -> SegmentMetadata {
        let wal_digest =
            crate::tests::harness::pitr_wal_digest(&wal_bytes(), crate::pitr::WAL_V5_VERSION);
        let seal_digest = Sha256::digest(b"seal").into();

        SegmentMetadata {
            key: SegmentKey {
                repository_id: [1; 16],
                timeline_id: TimelineId([2; 16]),
                archive_epoch_id: ArchiveEpochId([3; 16]),
                segment_id: SegmentId(1),
            },
            wal_format_version: crate::pitr::WAL_V5_VERSION,
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
            logical_bytes: wal_bytes().len() as u64,
            wal_bytes: wal_bytes().len() as u64,
            wal_digest,
            seal_digest,
            source_identity: [4; 32],
        }
    }

    /// A repository holding a segment written before the logical-batch digest and
    /// one written after it, archived, chained and verified in one go.
    ///
    /// This is the boundary the rule change is most likely to break: the successor's
    /// predecessor anchor is a *stored* digest copied from the older segment, so it
    /// is a v5-rule value sitting in a v6 segment's header, and object names are
    /// built from those stored digests. Nothing may re-derive either under the
    /// running binary's rule.
    #[test]
    fn mixed_version_repository_archives_verifies_and_chains() {
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

        // The frozen legacy segment, straight from the fixtures.
        let legacy_wal = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/tests/fixtures/pitr-v5-segment.wal"
        ))
        .unwrap();
        let legacy_seal = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/tests/fixtures/pitr-v5-segment.seal"
        ))
        .unwrap();
        let legacy_header = crate::pitr::decode_v5_file_header(&legacy_wal).unwrap();
        assert_eq!(
            legacy_header.wal_format_version,
            crate::pitr::WAL_V5_VERSION_LEGACY
        );
        let legacy_wal_digest: [u8; 32] = Sha256::digest(&legacy_wal).into();
        let legacy_seal_digest: [u8; 32] = Sha256::digest(&legacy_seal).into();
        let legacy = SegmentMetadata {
            key: SegmentKey {
                repository_id: [1; 16],
                timeline_id: legacy_header.timeline_id,
                archive_epoch_id: legacy_header.archive_epoch_id,
                segment_id: legacy_header.segment_id,
            },
            wal_format_version: legacy_header.wal_format_version,
            seal_format_version: 1,
            anchor: SegmentAnchor {
                segment_id: legacy_header.segment_id,
                wal_digest: legacy_wal_digest,
                seal_digest: legacy_seal_digest,
            },
            predecessor: legacy_header.predecessor,
            first_commit_ts: Some(1),
            last_commit_ts: Some(2),
            batch_count: 2,
            logical_bytes: legacy_wal.len() as u64,
            wal_bytes: legacy_wal.len() as u64,
            wal_digest: legacy_wal_digest,
            seal_digest: legacy_seal_digest,
            source_identity: [4; 32],
        };

        // Its successor, written under the new rule, pointing back at the legacy
        // segment's stored anchor.
        let successor_id = crate::pitr::SegmentId(legacy_header.segment_id.0 + 1);
        let successor_header = crate::tests::harness::pitr_segment_header(
            legacy_header.timeline_id,
            legacy_header.archive_epoch_id,
            successor_id,
            crate::pitr::ChainAnchor::Segment(crate::pitr::SegmentAnchor {
                segment_id: legacy_header.segment_id,
                wal_digest: legacy_wal_digest,
                seal_digest: legacy_seal_digest,
            }),
            crate::pitr::WAL_V5_VERSION,
        );
        // After the legacy segment's range, which ends at 2.
        let successor_wal = crate::tests::harness::pitr_segment_wal_bytes_at(successor_header, 3);
        let (successor_seal_parsed, successor_seal) =
            crate::pitr::seal::build_v5_seal(&successor_wal).unwrap();
        let successor = SegmentMetadata {
            key: SegmentKey {
                repository_id: [1; 16],
                timeline_id: successor_header.timeline_id,
                archive_epoch_id: successor_header.archive_epoch_id,
                segment_id: successor_id,
            },
            wal_format_version: successor_header.wal_format_version,
            seal_format_version: 1,
            anchor: SegmentAnchor {
                segment_id: successor_id,
                wal_digest: successor_seal_parsed.wal_digest,
                seal_digest: Sha256::digest(&successor_seal).into(),
            },
            predecessor: successor_header.predecessor,
            first_commit_ts: successor_seal_parsed.first_commit_ts(),
            last_commit_ts: successor_seal_parsed.last_commit_ts(),
            batch_count: successor_seal_parsed.entries.len() as u64,
            logical_bytes: successor_seal_parsed.logical_length,
            wal_bytes: successor_seal_parsed.logical_length,
            wal_digest: successor_seal_parsed.wal_digest,
            seal_digest: Sha256::digest(&successor_seal).into(),
            source_identity: [5; 32],
        };

        for (metadata, wal, seal) in [
            (legacy.clone(), &legacy_wal, &legacy_seal),
            (successor.clone(), &successor_wal, &successor_seal),
        ] {
            assert!(matches!(
                archiver
                    .archive_segment(metadata, wal, seal, Instant::now())
                    .unwrap(),
                ArchiveTransactionOutcome::Committed { .. }
            ));
        }

        // The catalog keeps both, in order, with the successor still anchored to the
        // legacy digest - the value that only the legacy rule reproduces.
        let replay = crate::pitr::catalog::replay_catalog(archiver.catalog_bytes()).unwrap();
        let committed = replay
            .records
            .iter()
            .filter_map(|record| match record {
                crate::pitr::catalog::PitrCatalogRecord::CommitSegment { metadata } => {
                    Some(metadata.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(committed.len(), 2);
        assert_eq!(
            committed[0].wal_format_version,
            crate::pitr::WAL_V5_VERSION_LEGACY
        );
        assert_eq!(committed[1].wal_format_version, crate::pitr::WAL_V5_VERSION);
        assert_eq!(
            committed[1].predecessor,
            crate::pitr::ChainAnchor::Segment(committed[0].anchor)
        );

        // Restore-side verification agrees with both rules, from the stored digests
        // alone - and both objects are named after the digests it checks.
        let wal_dir = root.path().join("wal");
        for (metadata, wal) in [
            (&committed[0], &legacy_wal),
            (&committed[1], &successor_wal),
        ] {
            let name = crate::pitr::archive::archive_object_name(
                metadata.key.timeline_id,
                metadata.key.archive_epoch_id,
                metadata.key.segment_id,
                crate::pitr::archive::ArchiveObjectKind::Wal,
                metadata.wal_digest,
            );
            let stored = std::fs::read(wal_dir.join(&name)).unwrap();
            assert_eq!(&stored, wal);
            let object = crate::pitr::restore::PitrRestoreSourceObject {
                segment_id: metadata.key.segment_id,
                kind: crate::pitr::archive::ArchiveObjectKind::Wal,
                name,
                digest: metadata.wal_digest,
                bytes: Some(metadata.wal_bytes),
                wal_digest_rule: metadata.wal_digest_rule().unwrap(),
            };
            crate::pitr::restore::verify_source_object(&object, &stored).unwrap();
            // A flipped padding byte is refused here under either rule - the parser
            // rejects a nonzero alignment gap before any digest is compared - so this
            // asserts the object is not accepted, not that the legacy rule caught it.
            // That the legacy digest covers the padding is pinned by the frozen
            // fixture's `wal_digest == SHA256(file)` in `pitr_seal.rs`.
            let mut tampered = stored.clone();
            let last = tampered.len() - 1;
            tampered[last] ^= 1;
            assert!(crate::pitr::restore::verify_source_object(&object, &tampered).is_err());
        }
        // The new rule does not cover its padding, so the parser is what refuses a
        // corrupted gap there: pin that the narrowing is deliberate.
        let name = crate::pitr::archive::archive_object_name(
            committed[1].key.timeline_id,
            committed[1].key.archive_epoch_id,
            committed[1].key.segment_id,
            crate::pitr::archive::ArchiveObjectKind::Wal,
            committed[1].wal_digest,
        );
        let mut tampered = successor_wal.clone();
        let gap = crate::pitr::WAL_V5_HEADER_LEN + crate::pitr::WAL_V5_BATCH_HEADER_LEN + 64;
        tampered[gap] ^= 1;
        let object = crate::pitr::restore::PitrRestoreSourceObject {
            segment_id: committed[1].key.segment_id,
            kind: crate::pitr::archive::ArchiveObjectKind::Wal,
            name,
            digest: committed[1].wal_digest,
            bytes: Some(committed[1].wal_bytes),
            wal_digest_rule: committed[1].wal_digest_rule().unwrap(),
        };
        assert!(crate::pitr::restore::verify_source_object(&object, &tampered).is_err());
    }

    #[test]
    fn throttling_happens_before_publication_and_retry_commits() {
        let root =
            std::env::temp_dir().join(format!("toy-kv-pitr-archiver-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let start = Instant::now();
        // Sized against the fixture rather than a handful of stand-in bytes: the
        // segment is real, so one burst has to cover the whole WAL's chunk - charged
        // once per I/O operation per chunk - with room left for the seal's, and one
        // chunk has to hold the whole WAL, or the archiver legitimately sleeps out a
        // refill for every one of hundreds of chunks.
        let burst = wal_bytes().len() as u64 * 3;
        let mut archiver = PitrArchiver::new(
            &root,
            ArchiveLimiterOptions {
                bytes_per_second: NonZeroU64::new(burst * 2),
                burst_bytes: NonZeroU64::new(burst).unwrap(),
            },
            start,
        )
        .unwrap();
        let first = metadata();
        assert!(matches!(
            archiver
                .archive_segment(first.clone(), &wal_bytes(), b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::Committed { sequence: 1 }
        ));
        assert!(matches!(
            archiver
                .archive_segment(first.clone(), &wal_bytes(), b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::RateLimited { .. }
        ));
        assert!(matches!(
            archiver
                .archive_segment(first, &wal_bytes(), b"seal", start + Duration::from_secs(2))
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
                .archive_segment(metadata(), &wal_bytes(), b"seal", Instant::now())
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
        std::fs::write(&wal_path, wal_bytes()).unwrap();
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
    fn archive_stream_cancellation_is_checked_before_next_chunk() {
        let cancelled = std::sync::atomic::AtomicBool::new(true);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("wal");
        std::fs::write(&path, b"wal").unwrap();
        let limiter = PitrArchiveLimiter::new(
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(2).unwrap(),
            },
            Instant::now(),
        );
        let priority = parking_lot::Mutex::new(crate::pitr::api::ArchiveIoPriority::Background);
        let error = read_source_object(
            &path,
            3,
            Sha256::digest(b"wal").into(),
            crate::pitr::archive::SourceObjectDigest::WholeObject,
            2,
            &limiter,
            &priority,
            Instant::now(),
            Some(&cancelled),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
    }

    /// A rate-limit wait must not hold a cancellation for the whole wait.
    ///
    /// A configured `archive_io_bytes_per_second` can ask for a wait of seconds,
    /// and the limiter loops sleep it off the archiver's thread. Slicing the sleep
    /// is what lets a close or a dropped async task get through; without it this
    /// returns only once the ten seconds are up.
    #[test]
    fn a_rate_limit_wait_ends_early_when_the_archive_is_cancelled() {
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        let started = Instant::now();
        let error = std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(30));
                cancelled.store(true, std::sync::atomic::Ordering::Release);
            });
            wait_for_archive_tokens(Duration::from_secs(10), Some(&cancelled)).unwrap_err()
        });
        assert!(error.to_string().contains("cancelled"));
        // Generous against a loaded machine, and still far below the wait asked for.
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the wait ran to completion instead of observing the cancellation: {:?}",
            started.elapsed()
        );
    }

    /// The sliced wait still sleeps out the whole interval the limiter asked for.
    #[test]
    fn a_rate_limit_wait_without_cancellation_sleeps_the_full_interval() {
        let started = Instant::now();
        wait_for_archive_tokens(Duration::from_millis(120), None).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(120));
    }

    #[test]
    fn missing_source_wal_fails_closed_without_catalog_advertisement() {
        let root = tempfile::tempdir().unwrap();
        let seal_path = root.path().join("segment.seal");
        std::fs::write(&seal_path, b"seal").unwrap();
        let missing_wal = root.path().join("segment.wal");
        let repository = root.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        let mut archiver = PitrArchiver::new(
            repository,
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
        let error = archiver
            .archive_segment_from_paths(metadata(), missing_wal, seal_path, Instant::now())
            .unwrap_err();
        assert!(
            error.to_string().contains("No such file") || error.to_string().contains("not found")
        );
        assert!(archiver.committed_segment_ids().unwrap().is_empty());
    }

    #[test]
    fn repository_path_loss_fails_catalog_publication_closed() {
        let parent = tempfile::tempdir().unwrap();
        let repository = parent.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        let mut archiver = PitrArchiver::new(
            &repository,
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
        let moved = parent.path().join("repository-moved");
        std::fs::rename(&repository, &moved).unwrap();
        let outcome = archiver
            .archive_segment(metadata(), &wal_bytes(), b"seal", Instant::now())
            .unwrap();
        assert!(matches!(
            outcome,
            ArchiveTransactionOutcome::PublicationUnknown { .. }
        ));
    }

    #[test]
    fn process_kill_after_catalog_rename_reopens_committed_segment() {
        let root = tempfile::tempdir().unwrap();
        if std::env::var_os("PITR_PROCESS_KILL_CHILD_ROOT").is_some() {
            let child_root = std::env::var_os("PITR_PROCESS_KILL_CHILD_ROOT").unwrap();
            let mut archiver = PitrArchiver::new(
                PathBuf::from(child_root),
                ArchiveLimiterOptions {
                    bytes_per_second: None,
                    burst_bytes: NonZeroU64::new(1024).unwrap(),
                },
                Instant::now(),
            )
            .unwrap();
            let _ = archiver.archive_segment(metadata(), &wal_bytes(), b"seal", Instant::now());
            unreachable!("child must exit at the catalog rename boundary");
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "pitr::archiver::tests::process_kill_after_catalog_rename_reopens_committed_segment",
            )
            .arg("--nocapture")
            .env("PITR_PROCESS_KILL_CHILD_ROOT", root.path())
            .env("PITR_PROCESS_KILL_AFTER_CATALOG_RENAME", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(137));
        let archiver = PitrArchiver::new(
            root.path(),
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
        assert_eq!(
            archiver
                .committed_segment_ids()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            [1]
        );
    }

    #[test]
    fn process_kill_after_object_rename_does_not_advertise_segment() {
        let root = tempfile::tempdir().unwrap();
        if std::env::var_os("PITR_PROCESS_OBJECT_CHILD_ROOT").is_some() {
            let child_root = std::env::var_os("PITR_PROCESS_OBJECT_CHILD_ROOT").unwrap();
            let mut archiver = PitrArchiver::new(
                PathBuf::from(child_root),
                ArchiveLimiterOptions {
                    bytes_per_second: None,
                    burst_bytes: NonZeroU64::new(1024).unwrap(),
                },
                Instant::now(),
            )
            .unwrap();
            let _ = archiver.archive_segment(metadata(), &wal_bytes(), b"seal", Instant::now());
            unreachable!("child must exit at the object rename boundary");
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "pitr::archiver::tests::process_kill_after_object_rename_does_not_advertise_segment",
            )
            .arg("--nocapture")
            .env("PITR_PROCESS_OBJECT_CHILD_ROOT", root.path())
            .env("PITR_PROCESS_KILL_AFTER_OBJECT_RENAME", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(137));
        let archiver = PitrArchiver::new(
            root.path(),
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
        assert!(archiver.committed_segment_ids().unwrap().is_empty());
    }

    #[test]
    fn process_kill_after_catalog_dir_sync_reopens_committed_segment() {
        let root = tempfile::tempdir().unwrap();
        if std::env::var_os("PITR_PROCESS_CATALOG_SYNC_CHILD_ROOT").is_some() {
            let child_root = std::env::var_os("PITR_PROCESS_CATALOG_SYNC_CHILD_ROOT").unwrap();
            let mut archiver = PitrArchiver::new(
                PathBuf::from(child_root),
                ArchiveLimiterOptions {
                    bytes_per_second: None,
                    burst_bytes: NonZeroU64::new(1024).unwrap(),
                },
                Instant::now(),
            )
            .unwrap();
            let _ = archiver.archive_segment(metadata(), &wal_bytes(), b"seal", Instant::now());
            unreachable!("child must exit after catalog directory sync");
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "pitr::archiver::tests::process_kill_after_catalog_dir_sync_reopens_committed_segment",
            )
            .arg("--nocapture")
            .env("PITR_PROCESS_CATALOG_SYNC_CHILD_ROOT", root.path())
            .env("PITR_PROCESS_KILL_AFTER_CATALOG_DIR_SYNC", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(137));
        let archiver = PitrArchiver::new(
            root.path(),
            ArchiveLimiterOptions {
                bytes_per_second: None,
                burst_bytes: NonZeroU64::new(1024).unwrap(),
            },
            Instant::now(),
        )
        .unwrap();
        assert_eq!(
            archiver
                .committed_segment_ids()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            [1]
        );
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
                .archive_segment(first.clone(), &wal_bytes(), b"seal", start)
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
                .archive_segment(first, &wal_bytes(), b"seal", start)
                .unwrap(),
            ArchiveTransactionOutcome::AlreadyCommitted { sequence: 1 }
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
}
