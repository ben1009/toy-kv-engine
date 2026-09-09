//! Incremental-backup repository catalog primitives (RFC 022).
//!
//! The catalog is intentionally append-only. A frame is length-delimited and
//! checksummed, so recovery can discard only a torn final write while treating
//! complete semantic corruption as an error.

#![allow(dead_code)] // Wired to repository publication in the next RFC 022 slice.

use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(target_os = "linux")]
use std::{
    ffi::{CStr, CString},
    future::Future,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    pin::Pin,
    sync::{Arc, atomic::AtomicBool},
    task::{Context as TaskContext, Poll},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use crc32fast::Hasher;
use parking_lot::{Mutex, ReentrantMutex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_CATALOG_FRAME_BYTES: usize = 1024 * 1024;
const CATALOG_FRAME_HEADER_BYTES: usize = 12;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_RECORDS: usize = 1_000_000;
const CATALOG_FORMAT_VERSION: u8 = 1;
const MAX_GENERATION_METADATA_BYTES: usize = 1024 * 1024;
const MAX_REPOSITORY_OBJECT_BYTES: u64 = 128 * 1024 * 1024;
static OBJECT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(all(test, target_os = "linux", feature = "chaos-testing"))]
static BACKUP_FAILPOINT_TEST_LOCK: ReentrantMutex<()> = ReentrantMutex::new(());

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RepositoryObjectKind {
    Sst,
    Vlog,
}

pub(crate) fn derived_object_name(
    kind: RepositoryObjectKind,
    file_id: u64,
    checksum: [u8; 32],
) -> String {
    let prefix = match kind {
        RepositoryObjectKind::Sst => "sst",
        RepositoryObjectKind::Vlog => "vlog",
    };
    let digest = checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}-{file_id}-{digest}")
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationEnvelope {
    version: u8,
    id: u64,
    created_at_secs: u64,
    parent_id: Option<u64>,
    #[serde(default, skip_serializing_if = "is_zero")]
    new_object_bytes: u64,
    snapshot_len: u64,
    snapshot_checksum: [u8; 32],
    #[serde(default)]
    objects: Option<Vec<GenerationObject>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compatibility: Option<RestoreCompatibility>,
    body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreCompatibility {
    manifest_format_version: u32,
    value_separation_enabled: bool,
    vlog_format_version: Option<u16>,
    ttl_records_present: bool,
    serializable_at_capture: bool,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn is_zero_digest(value: &[u8; 32]) -> bool {
    *value == [0; 32]
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupInfo {
    pub id: u64,
    pub created_at_secs: u64,
    pub parent_id: Option<u64>,
    pub logical_bytes: u64,
    pub file_count: u64,
    pub new_object_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct BackupOptions {
    pub repository: PathBuf,
    pub use_hard_links: bool,
}

#[derive(Debug)]
pub enum BackupOutcome {
    Committed(BackupInfo),
    CancelledBeforeCommit,
    CommittedAfterCancellation(BackupInfo),
    RepositoryPublishedButNotDurable {
        repository: PathBuf,
        error: std::io::Error,
    },
    CommitPublishedButNotDurable {
        info: BackupInfo,
        error: std::io::Error,
    },
    CommitPublicationUnknown {
        info: BackupInfo,
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

/// RFC 022 typed result for synchronous backup creation.
#[derive(Debug)]
pub enum CreateBackupOutcome {
    Committed(BackupInfo),
    RepositoryPublishedButNotDurable {
        repository: PathBuf,
        error: std::io::Error,
    },
    CommitPublishedButNotDurable {
        info: BackupInfo,
        error: std::io::Error,
    },
    CommitPublicationUnknown {
        info: BackupInfo,
        fsync_error: std::io::Error,
        revalidation_error: anyhow::Error,
    },
}

#[cfg(target_os = "linux")]
/// Eagerly dispatched backup operation that can be awaited or cancelled.
#[derive(Debug)]
pub struct BackupTask {
    handle: Option<tokio::task::JoinHandle<Result<BackupOutcome>>>,
    ready: Option<Result<BackupOutcome>>,
    control: Arc<BackupTaskControl>,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
/// Thread-safe cancellation request handle for an eagerly dispatched backup.
pub struct BackupCancellationHandle {
    control: Arc<BackupTaskControl>,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct BackupTaskControl {
    cancelled: AtomicBool,
    commit_decided: Mutex<bool>,
    #[cfg(test)]
    barrier_token: u64,
}

#[cfg(test)]
mod commit_decision_test_hook {
    use parking_lot::{Condvar, Mutex};
    use std::sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    };

    #[allow(clippy::type_complexity)]
    static STATE: OnceLock<(Mutex<(Option<(u64, u64)>, bool, bool)>, Condvar)> = OnceLock::new();
    static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);
    #[allow(clippy::type_complexity)]
    static POST_STATE: OnceLock<(Mutex<(Option<(u64, u64)>, bool, bool)>, Condvar)> =
        OnceLock::new();

    pub fn next_token() -> u64 {
        NEXT_TOKEN.fetch_add(1, Ordering::Relaxed)
    }

    pub fn arm(token: u64, id: u64) {
        let (lock, _) = STATE.get_or_init(|| (Mutex::new((None, false, false)), Condvar::new()));
        let mut state = lock.lock();
        assert!(state.0.is_none(), "commit decision barrier already armed");
        *state = (Some((token, id)), false, false);
    }

    pub fn wait_until_entered(token: u64, id: u64) {
        let (lock, condvar) = STATE.get().unwrap();
        let mut state = lock.lock();
        while state.0 != Some((token, id)) || !state.1 {
            condvar.wait(&mut state);
        }
    }

    pub fn release(token: u64, id: u64) {
        let (lock, condvar) = STATE.get().unwrap();
        let mut state = lock.lock();
        assert_eq!(state.0, Some((token, id)));
        state.2 = true;
        condvar.notify_all();
    }

    pub fn wait_if_armed(token: u64, id: u64) {
        let Some((lock, condvar)) = STATE.get() else {
            return;
        };
        let mut state = lock.lock();
        if state.0 == Some((token, id)) && !state.1 && !state.2 {
            state.1 = true;
            condvar.notify_all();
            while !state.2 {
                condvar.wait(&mut state);
            }
            *state = (None, false, false);
        }
    }

    pub fn arm_after(token: u64, id: u64) {
        let (lock, _) =
            POST_STATE.get_or_init(|| (Mutex::new((None, false, false)), Condvar::new()));
        let mut state = lock.lock();
        assert!(state.0.is_none());
        *state = (Some((token, id)), false, false);
    }

    pub fn wait_after(token: u64, id: u64) {
        let (lock, condvar) = POST_STATE.get().unwrap();
        let mut state = lock.lock();
        while state.0 != Some((token, id)) || !state.1 {
            condvar.wait(&mut state);
        }
    }

    pub fn release_after(token: u64, id: u64) {
        let (lock, condvar) = POST_STATE.get().unwrap();
        let mut state = lock.lock();
        assert_eq!(state.0, Some((token, id)));
        state.2 = true;
        condvar.notify_all();
    }

    pub fn wait_if_armed_after(token: u64, id: u64) {
        let Some((lock, condvar)) = POST_STATE.get() else {
            return;
        };
        let mut state = lock.lock();
        if state.0 == Some((token, id)) && !state.1 && !state.2 {
            state.1 = true;
            condvar.notify_all();
            while !state.2 {
                condvar.wait(&mut state);
            }
            *state = (None, false, false);
        }
    }
}

#[cfg(test)]
mod restore_unlock_test_hook {
    use parking_lot::{Condvar, Mutex, MutexGuard};
    use std::sync::OnceLock;

    static STATE: OnceLock<(Mutex<(bool, bool)>, Condvar)> = OnceLock::new();
    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static EXPECTED_TARGET: OnceLock<Mutex<Option<String>>> = OnceLock::new();

    pub fn lock() -> MutexGuard<'static, ()> {
        TEST_LOCK.get_or_init(|| Mutex::new(())).lock()
    }

    pub fn arm(target_name: &str) {
        *EXPECTED_TARGET.get_or_init(|| Mutex::new(None)).lock() = Some(target_name.to_owned());
        *STATE
            .get_or_init(|| (Mutex::new((false, false)), Condvar::new()))
            .0
            .lock() = (false, false);
    }

    pub fn pause(target_name: &str) {
        let expected = EXPECTED_TARGET.get_or_init(|| Mutex::new(None)).lock();
        if expected.as_deref() != Some(target_name) {
            return;
        }
        let (lock, condvar) = STATE.get_or_init(|| (Mutex::new((false, false)), Condvar::new()));
        let mut state = lock.lock();
        state.0 = true;
        condvar.notify_all();
        while !state.1 {
            condvar.wait(&mut state);
        }
    }

    pub fn wait() {
        let (lock, condvar) = STATE.get_or_init(|| (Mutex::new((false, false)), Condvar::new()));
        let mut state = lock.lock();
        while !state.0 {
            condvar.wait(&mut state);
        }
    }

    pub fn release() {
        let (lock, condvar) = STATE.get_or_init(|| (Mutex::new((false, false)), Condvar::new()));
        lock.lock().1 = true;
        condvar.notify_all();
    }
}

#[cfg(target_os = "linux")]
impl BackupTask {
    fn ready(error: anyhow::Error) -> Self {
        Self {
            handle: None,
            ready: Some(Err(error)),
            control: Arc::new(BackupTaskControl {
                cancelled: AtomicBool::new(false),
                commit_decided: Mutex::new(true),
                #[cfg(test)]
                barrier_token: commit_decision_test_hook::next_token(),
            }),
        }
    }

    pub fn cancellation_handle(&self) -> BackupCancellationHandle {
        BackupCancellationHandle {
            control: Arc::clone(&self.control),
        }
    }

    /// Requests cancellation. The worker may still complete a commit if it
    /// has already passed the commit decision point.
    pub fn cancel(&self) {
        self.cancellation_handle().cancel();
    }
}

#[cfg(target_os = "linux")]
impl BackupCancellationHandle {
    /// Requests cancellation and records it even if the commit decision has
    /// already been sealed, allowing the task to report the commit race.
    pub fn cancel(&self) {
        let _decision = self.control.commit_decided.lock();
        self.control.cancelled.store(true, Ordering::Release);
    }
}

#[cfg(target_os = "linux")]
impl Future for BackupTask {
    type Output = Result<BackupOutcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.ready.take() {
            return Poll::Ready(result);
        }
        match Pin::new(
            self.handle
                .as_mut()
                .expect("backup task has no result source"),
        )
        .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(error)) => Poll::Ready(Err(anyhow!("backup task failed: {error}"))),
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for BackupTask {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Debug)]
pub(crate) struct RepositoryPublicationError {
    pub id: u64,
    pub source: anyhow::Error,
}

impl std::fmt::Display for RepositoryPublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "repository publication failed for {}: {}",
            self.id, self.source
        )
    }
}

impl std::error::Error for RepositoryPublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
struct RepositoryBootstrapPublicationError {
    source: anyhow::Error,
}

impl std::fmt::Display for RepositoryBootstrapPublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "backup repository bootstrap publication failed: {}",
            self.source
        )
    }
}

impl std::error::Error for RepositoryBootstrapPublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitFailureKind {
    BeforeCommitRecord,
    CommitPublishedButNotDurable,
    CommitDurabilityUnknown,
}

#[derive(Debug)]
pub(crate) struct CommitPublicationError {
    pub id: u64,
    pub info: Option<BackupInfo>,
    pub kind: CommitFailureKind,
    pub source: anyhow::Error,
    pub revalidation_error: Option<anyhow::Error>,
}

impl std::fmt::Display for CommitPublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "commit publication {:?}: {}", self.kind, self.source)
    }
}

impl std::error::Error for CommitPublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Result of publishing a restored database directory.
#[derive(Debug)]
pub enum RestoreOutcome {
    Restored,
    PublishedButNotDurable {
        target: PathBuf,
        error: std::io::Error,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenerationObject {
    kind: RepositoryObjectKind,
    source_path: String,
    object_name: String,
    file_id: u64,
    file_size: u64,
    file_checksum: [u8; 32],
}

#[cfg(target_os = "linux")]
pub struct BackupRepository {
    root: OwnedFd,
    _lock: RepositoryLock,
    replay: CatalogReplay,
    usable: Arc<AtomicBool>,
    stale_after_restore: AtomicBool,
    operation_lock: ReentrantMutex<()>,
    pending_prepare: bool,
    pending_prepare_digest: Option<[u8; 32]>,
    pending_generation_checksum: Option<[u8; 32]>,
    pending_parent_id: Option<Option<u64>>,
}

#[cfg(target_os = "linux")]
impl BackupRepository {
    fn ensure_usable(&self) -> Result<()> {
        ensure!(
            self.usable.load(Ordering::Acquire),
            "backup repository is invalidated; reopen it before retrying"
        );
        Ok(())
    }

    fn ensure_mutation_allowed(&self) -> Result<()> {
        ensure!(
            !self.stale_after_restore.load(Ordering::Acquire),
            "backup repository handle is stale after restore; reopen it before mutation"
        );
        self.ensure_usable()
    }

    fn discard_pending_generation(&mut self, id: u64) -> Result<()> {
        self.ensure_mutation_allowed()?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        remove_generation_directory(&generations, id)?;
        fsync_fd(&generations)?;
        let catalog_fd = openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_WRONLY, 0)?;
        let catalog = File::from(catalog_fd);
        catalog.set_len(self.replay.retained_offset)?;
        catalog.sync_all()?;
        fsync_fd(&self.root)?;
        self.replay.last_sequence = self.replay.last_sequence.saturating_sub(1);
        self.pending_prepare = false;
        self.pending_prepare_digest = None;
        self.pending_generation_checksum = None;
        self.pending_parent_id = None;
        Ok(())
    }

    fn revalidate_commit_visibility(&self, id: u64) -> Result<Option<bool>> {
        let catalog_fd = openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_RDONLY, 0)?;
        let mut catalog = File::from(catalog_fd);
        let frames = read_catalog_records(&mut catalog)?;
        let replay = replay_catalog(&frames)?;
        if replay.committed_ids.contains(&id) {
            Ok(Some(true))
        } else if replay.abandoned_generation_id == Some(id) || replay.high_water_id < id {
            Ok(Some(false))
        } else {
            Ok(None)
        }
    }

    /// Reloads catalog state for read-only operations after a restore briefly
    /// releases the repository lock.
    fn load_replay(&self) -> Result<CatalogReplay> {
        self.ensure_usable()?;
        let catalog_fd = openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_RDONLY, 0)?;
        let mut catalog = File::from(catalog_fd);
        replay_catalog(&read_catalog_records(&mut catalog)?)
    }

    fn classify_commit_failure(
        &self,
        id: u64,
        source: anyhow::Error,
    ) -> (CommitFailureKind, anyhow::Error, Option<anyhow::Error>) {
        match self.revalidate_commit_visibility(id) {
            Ok(Some(true)) => (
                CommitFailureKind::CommitPublishedButNotDurable,
                source,
                None,
            ),
            Ok(Some(false)) => (CommitFailureKind::BeforeCommitRecord, source, None),
            Ok(None) => (
                CommitFailureKind::CommitDurabilityUnknown,
                source,
                Some(anyhow!("catalog revalidation was inconclusive")),
            ),
            Err(revalidation) => (
                CommitFailureKind::CommitDurabilityUnknown,
                source,
                Some(revalidation),
            ),
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = open_directory_no_follow(path.as_ref())?;
        Self::open_root(root)
    }

    fn open_at(parent: &OwnedFd, name: &str) -> Result<Self> {
        let root = openat_no_follow(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        Self::open_root(root)
    }

    fn open_root(root: OwnedFd) -> Result<Self> {
        let lock = RepositoryLock::acquire(&root, true)?;
        let files = openat_no_follow(&root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let generations =
            openat_no_follow(&root, "generations", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        cleanup_stale_catalog_temps(&root)?;
        recover_catalog_successor(&root)?;
        fsync_fd(&files)?;
        fsync_fd(&generations)?;
        let catalog_fd = openat_no_follow(&root, "BACKUP_MANIFEST", libc::O_RDWR, 0)?;
        ensure_regular_file(catalog_fd.as_raw_fd())?;
        let mut catalog = File::from(catalog_fd);
        let frames = read_catalog_records(&mut catalog)?;
        let replay = replay_catalog(&frames)?;
        let require_snapshot_metadata = matches!(
            frames.frames.first().map(|frame| &frame.record),
            Some(CatalogRecord::Snapshot {
                base_catalog_digest,
                ..
            }) if *base_catalog_digest != [0; 32]
        );
        if let Some(id) = replay.abandoned_generation_id {
            remove_generation_orphan(&generations, id)?;
            fsync_fd(&generations)?;
        }
        remove_uncommitted_generation_orphans(&generations, &replay.committed_ids)?;
        validate_replay_generations(&root, &replay, require_snapshot_metadata)?;
        if frames.torn_tail || replay.retained_offset < frames.last_complete_offset {
            catalog.set_len(replay.retained_offset)?;
            catalog.sync_all()?;
            fsync_fd(&root)?;
        }
        Ok(Self {
            root,
            _lock: lock,
            replay,
            usable: Arc::new(AtomicBool::new(true)),
            stale_after_restore: AtomicBool::new(false),
            operation_lock: ReentrantMutex::new(()),
            pending_prepare: false,
            pending_prepare_digest: None,
            pending_generation_checksum: None,
            pending_parent_id: None,
        })
    }

    pub(crate) fn high_water_id(&self) -> u64 {
        self.replay.high_water_id
    }

    /// Returns committed generation identifiers in ascending order.
    pub fn list_ids(&self) -> Result<Vec<u64>> {
        let _operation_guard = self.operation_lock.lock();
        let replay = self.load_replay()?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        for committed in &replay.committed_generations {
            let generation = openat_no_follow(
                &generations,
                &committed.id.to_string(),
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
            let generation_bytes = read_generation_metadata(&generation, "GENERATION")?;
            let checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
            ensure!(
                checksum == committed.generation_checksum,
                "backup generation checksum mismatch"
            );
            let snapshot_bytes = read_generation_metadata(&generation, "MANIFEST_SNAPSHOT")?;
            let envelope: GenerationEnvelope =
                serde_json::from_slice(&generation_bytes).context("invalid generation envelope")?;
            ensure!(
                matches!(envelope.version, 1..=4),
                "unsupported generation envelope version"
            );
            ensure!(
                envelope.id == committed.id,
                "generation envelope id mismatch"
            );
            ensure!(
                envelope.parent_id == committed.parent_id,
                "generation envelope parent mismatch"
            );
            validate_generation_objects(&envelope)?;
            if envelope.version >= 2 {
                ensure!(
                    generation_bytes == serde_json::to_vec(&envelope)?,
                    "generation envelope is not canonically encoded"
                );
                validate_generation_object_metadata_on_disk(&self.root, &envelope)?;
            }
            ensure!(
                envelope.snapshot_len == snapshot_bytes.len() as u64,
                "generation snapshot length mismatch"
            );
            let snapshot_checksum: [u8; 32] = Sha256::digest(&snapshot_bytes).into();
            ensure!(
                envelope.snapshot_checksum == snapshot_checksum,
                "generation snapshot checksum mismatch"
            );
        }
        Ok(replay.committed_ids)
    }

    pub fn list_info(&self) -> Result<Vec<BackupInfo>> {
        let _operation_guard = self.operation_lock.lock();
        let replay = self.load_replay()?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let mut result = Vec::with_capacity(replay.committed_generations.len());
        for committed in &replay.committed_generations {
            let generation = openat_no_follow(
                &generations,
                &committed.id.to_string(),
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
            let bytes = read_generation_metadata(&generation, "GENERATION")?;
            let checksum: [u8; 32] = Sha256::digest(&bytes).into();
            ensure!(
                checksum == committed.generation_checksum,
                "backup generation checksum mismatch"
            );
            let envelope: GenerationEnvelope = serde_json::from_slice(&bytes)?;
            ensure!(
                envelope.id == committed.id,
                "generation envelope id mismatch"
            );
            ensure!(
                envelope.parent_id == committed.parent_id,
                "generation envelope parent mismatch"
            );
            ensure!(
                matches!(envelope.version, 1..=4),
                "unsupported generation envelope version"
            );
            validate_generation_objects(&envelope)?;
            if envelope.version >= 2 {
                ensure!(
                    bytes == serde_json::to_vec(&envelope)?,
                    "generation envelope is not canonically encoded"
                );
            }
            let snapshot = read_generation_metadata(&generation, "MANIFEST_SNAPSHOT")?;
            ensure!(
                envelope.snapshot_len == snapshot.len() as u64,
                "generation snapshot length mismatch"
            );
            let snapshot_checksum: [u8; 32] = Sha256::digest(&snapshot).into();
            ensure!(
                envelope.snapshot_checksum == snapshot_checksum,
                "generation snapshot checksum mismatch"
            );
            validate_generation_object_metadata_on_disk(&self.root, &envelope)?;
            let objects = envelope.objects.as_ref().map_or(&[][..], Vec::as_slice);
            let logical_bytes = objects.iter().try_fold(0_u64, |total, object| {
                total
                    .checked_add(object.file_size)
                    .ok_or_else(|| anyhow!("backup logical byte count overflow"))
            })?;
            result.push(BackupInfo {
                id: envelope.id,
                created_at_secs: envelope.created_at_secs,
                parent_id: envelope.parent_id,
                logical_bytes,
                file_count: objects.len() as u64,
                new_object_bytes: envelope.new_object_bytes,
            });
        }
        Ok(result)
    }

    /// Returns metadata for every committed generation in ascending ID order.
    pub fn list(&self) -> Result<Vec<BackupInfo>> {
        self.list_info()
    }

    /// Returns metadata for one committed generation.
    pub fn info(&self, id: u64) -> Result<BackupInfo> {
        self.list_info()?
            .into_iter()
            .find(|info| info.id == id)
            .ok_or_else(|| anyhow!("backup generation {id} is not committed"))
    }

    /// Returns metadata for the newest committed generation, if any.
    pub fn latest_info(&self) -> Result<Option<BackupInfo>> {
        Ok(self.list_info()?.into_iter().max_by_key(|info| info.id))
    }

    /// Returns the newest committed generation identifier, if any.
    /// Returns the newest committed generation, preserving catalog I/O and
    /// replay-validation errors for callers that need to distinguish failure
    /// from an empty repository.
    pub fn latest_id_result(&self) -> Result<Option<u64>> {
        let _operation_guard = self.operation_lock.lock();
        Ok(self.load_replay()?.committed_ids.last().copied())
    }

    /// Best-effort legacy accessor. Prefer [`Self::latest_id_result`] when
    /// catalog errors must remain distinguishable from an empty repository.
    pub fn latest_id(&self) -> Option<u64> {
        self.latest_id_result().ok().flatten()
    }

    /// Returns the newest `retain` committed generation IDs in ascending order.
    pub fn retained_ids(&self, retain: usize) -> Result<Vec<u64>> {
        let _operation_guard = self.operation_lock.lock();
        ensure!(retain > 0, "retention count must be greater than zero");
        let replay = self.load_replay()?;
        let keep_from = replay.committed_ids.len().saturating_sub(retain);
        Ok(replay.committed_ids[keep_from..].to_vec())
    }

    /// Returns sorted repository object names referenced by retained generations.
    pub fn retained_object_names(&self, retain: usize) -> Result<Vec<String>> {
        let _operation_guard = self.operation_lock.lock();
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let mut names = HashSet::new();
        for id in self.retained_ids(retain)? {
            self.verify(id)?;
            let generation = openat_no_follow(
                &generations,
                &id.to_string(),
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
            let bytes = read_generation_metadata(&generation, "GENERATION")?;
            let envelope: GenerationEnvelope = serde_json::from_slice(&bytes)?;
            validate_generation_objects(&envelope)?;
            let objects = envelope
                .objects
                .ok_or_else(|| anyhow!("retention requires a generation object map"))?;
            names.extend(objects.into_iter().map(|object| object.object_name));
        }
        let mut names = names.into_iter().collect::<Vec<_>>();
        names.sort_unstable();
        Ok(names)
    }

    /// Returns sorted immutable objects currently unreferenced by retained generations.
    pub fn unreferenced_object_names(&self, retain: usize) -> Result<Vec<String>> {
        let _operation_guard = self.operation_lock.lock();
        let retained = self
            .retained_object_names(retain)?
            .into_iter()
            .collect::<HashSet<_>>();
        let files = openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let path = PathBuf::from(format!("/proc/self/fd/{}", files.as_raw_fd()));
        let mut result = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry
                .file_name()
                .to_str()
                .ok_or_else(|| anyhow!("repository object name is not UTF-8"))?
                .to_owned();
            if name.starts_with('.') && name.contains(".tmp-") {
                continue;
            }
            ensure_repository_object_name(&name)?;
            let candidate = openat_no_follow(&files, &name, libc::O_RDONLY, 0)?;
            ensure_regular_file(candidate.as_raw_fd())?;
            if !retained.contains(&name) {
                result.push(name);
            }
        }
        result.sort_unstable();
        Ok(result)
    }

    /// Computes a retention plan without modifying the repository.
    pub fn plan_purge(&self, retain: usize) -> Result<(Vec<u64>, Vec<String>)> {
        let _operation_guard = self.operation_lock.lock();
        Ok((
            self.retained_ids(retain)?,
            self.unreferenced_object_names(retain)?,
        ))
    }

    /// Opens every immutable object before restore staging can release the
    /// repository lock. Open descriptors pin the source inodes across purge.
    fn pin_generation_objects(&self, envelope: &GenerationEnvelope) -> Result<Vec<File>> {
        let files = openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        envelope
            .objects
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|object| {
                let file = File::from(openat_no_follow(
                    &files,
                    &object.object_name,
                    libc::O_RDONLY,
                    0,
                )?);
                ensure_regular_file(file.as_raw_fd())?;
                ensure!(
                    file.metadata()?.len() == object.file_size,
                    "repository object size mismatch"
                );
                Ok(file)
            })
            .collect()
    }

    /// Materializes every object referenced by a validated generation.
    fn materialize_generation_objects(
        &self,
        envelope: &GenerationEnvelope,
        target_dir: &OwnedFd,
        pinned_objects: &mut [File],
    ) -> Result<()> {
        validate_generation_objects(envelope)?;
        let vlog_dir = if envelope.objects.as_ref().is_some_and(|objects| {
            objects
                .iter()
                .any(|object| object.kind == RepositoryObjectKind::Vlog)
        }) {
            Some(mkdirat_exclusive(target_dir, "vlog", 0o700)?)
        } else {
            None
        };
        for (object, pinned) in envelope
            .objects
            .as_deref()
            .unwrap_or_default()
            .iter()
            .zip(pinned_objects)
        {
            let destination = match object.kind {
                RepositoryObjectKind::Sst => target_dir,
                RepositoryObjectKind::Vlog => vlog_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("vLog restore directory was not created"))?,
            };
            let (size, checksum) = copy_immutable_object_from_file(
                pinned.try_clone()?,
                destination,
                &object.source_path,
                object.file_size,
                object.file_checksum,
            )?;
            ensure!(
                size == object.file_size && checksum == object.file_checksum,
                "restored object identity mismatch"
            );
        }
        Ok(())
    }

    /// Writes a validated captured manifest into a restore staging directory.
    fn write_restore_manifest(target_dir: &OwnedFd, snapshot: &[u8]) -> Result<()> {
        let _: crate::manifest::ManifestRecord =
            serde_json::from_slice(snapshot).context("invalid restore manifest snapshot")?;
        for (name, bytes) in [("MANIFEST_SNAPSHOT", snapshot), ("MANIFEST", &[][..])] {
            let mut file = File::from(openat_no_follow(
                target_dir,
                name,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )?);
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        fsync_fd(target_dir)
    }

    /// Atomically publishes a completed restore staging directory.
    fn publish_restore_staging(
        parent: &OwnedFd,
        staging: &str,
        target: &str,
    ) -> Result<Option<std::io::Error>> {
        ensure!(
            !staging.is_empty()
                && !target.is_empty()
                && staging != "."
                && staging != ".."
                && target != "."
                && target != ".."
                && !staging.contains('/')
                && !target.contains('/'),
            "restore publish names must be basenames"
        );
        let from = CString::new(staging)?;
        let to = CString::new(target)?;
        // SAFETY: parent is trusted and both names are generated/validated basenames.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                parent.as_raw_fd(),
                from.as_ptr(),
                parent.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        match fsync_fd(parent) {
            Ok(()) => Ok(None),
            Err(error) => match error.downcast::<std::io::Error>() {
                Ok(error) => Ok(Some(error)),
                Err(error) => Err(error),
            },
        }
    }

    /// Validates that a restore destination is an absent directory entry in a
    /// trusted parent, without following symlinks.
    pub fn validate_restore_target(target: impl AsRef<Path>) -> Result<()> {
        let target = target.as_ref();
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("restore target must have a UTF-8 basename"))?;
        let parent_fd = open_directory_no_follow(parent)?;
        ensure_restore_target_absent(&parent_fd, name)
    }

    /// Restores one committed generation into an absent target directory.
    pub fn restore(
        &self,
        id: u64,
        target: impl AsRef<Path>,
        options: crate::lsm_storage::LsmStorageOptions,
    ) -> Result<RestoreOutcome> {
        let _operation_guard = self.operation_lock.lock();
        self.ensure_mutation_allowed()?;
        let replay = self.load_replay()?;
        ensure!(
            replay.committed_ids.contains(&id),
            "backup generation {id} is not committed"
        );
        let target = target.as_ref();
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        let target_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("restore target must have a UTF-8 basename"))?;
        let parent_fd = open_directory_no_follow(parent)?;
        ensure_restore_target_absent(&parent_fd, target_name)?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let generation_dir = openat_no_follow(
            &generations,
            &id.to_string(),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let generation_bytes = read_generation_metadata(&generation_dir, "GENERATION")?;
        let committed = replay
            .committed_generations
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| anyhow!("backup generation {id} is not committed"))?;
        let generation_checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        ensure!(
            generation_checksum == committed.generation_checksum,
            "backup generation checksum mismatch"
        );
        let envelope: GenerationEnvelope = serde_json::from_slice(&generation_bytes)?;
        validate_restore_options(&envelope, &options)?;
        ensure!(envelope.id == id, "generation envelope id mismatch");
        ensure!(
            envelope.parent_id == committed.parent_id,
            "generation envelope parent mismatch"
        );
        ensure!(
            matches!(envelope.version, 1..=4),
            "unsupported generation envelope version"
        );
        if envelope.version >= 2 {
            ensure!(
                generation_bytes == serde_json::to_vec(&envelope)?,
                "generation envelope is not canonically encoded"
            );
        }
        validate_generation_objects(&envelope)?;
        ensure!(
            envelope.objects.is_some(),
            "restore requires a generation object map"
        );
        validate_generation_object_metadata_on_disk(&self.root, &envelope)?;
        let snapshot = read_generation_metadata(&generation_dir, "MANIFEST_SNAPSHOT")?;
        ensure!(
            envelope.snapshot_len == snapshot.len() as u64,
            "generation snapshot length mismatch"
        );
        let snapshot_checksum: [u8; 32] = Sha256::digest(&snapshot).into();
        ensure!(
            envelope.snapshot_checksum == snapshot_checksum,
            "generation snapshot checksum mismatch"
        );
        validate_restore_snapshot_objects(&envelope, &snapshot)?;
        let (staging_name, staging_fd) = Self::create_restore_staging(&parent_fd, target_name)?;
        let mut cleanup = RestoreStagingCleanup {
            parent: &parent_fd,
            name: staging_name.clone(),
        };
        let mut pinned_objects = self.pin_generation_objects(&envelope)?;
        self.stale_after_restore.store(true, Ordering::Release);
        self._lock.unlock()?;
        let relock_guard = RepositoryRelockGuard::new(&self._lock, &self.usable);
        #[cfg(test)]
        restore_unlock_test_hook::pause(target_name);
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("backup.restore.after_unlock");
        let result = (|| {
            self.materialize_generation_objects(&envelope, &staging_fd, &mut pinned_objects)?;
            Self::write_restore_manifest(&staging_fd, &snapshot)?;
            let durability_error =
                Self::publish_restore_staging(&parent_fd, &staging_name, target_name)?;
            cleanup.disarm();
            Ok(match durability_error {
                Some(error) => RestoreOutcome::PublishedButNotDurable {
                    target: target.to_path_buf(),
                    error,
                },
                None => RestoreOutcome::Restored,
            })
        })();
        let relock = relock_guard.reacquire();
        match relock {
            Ok(()) => result,
            Err(error) => {
                Err(error.context("failed to reacquire backup repository lock after restore"))
            }
        }
    }

    /// Creates a unique sibling staging directory for a restore operation.
    fn create_restore_staging(parent: &OwnedFd, target_name: &str) -> Result<(String, OwnedFd)> {
        ensure!(
            !target_name.is_empty(),
            "restore target name must not be empty"
        );
        for _ in 0..32 {
            let sequence = OBJECT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let name = format!(".{target_name}.restore-{}-{sequence}", std::process::id());
            match mkdirat_exclusive(parent, &name, 0o700) {
                Ok(fd) => return Ok((name, fd)),
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) => {
                }
                Err(error) => return Err(error),
            }
        }
        bail!("failed to allocate unique restore staging directory")
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_object(
        &self,
        source_dir: &OwnedFd,
        source_name: &str,
        kind: RepositoryObjectKind,
        file_id: u64,
        file_size: u64,
        file_checksum: [u8; 32],
        use_hard_links: bool,
    ) -> Result<bool> {
        self.ensure_mutation_allowed()?;
        let _operation_guard = self.operation_lock.lock();
        self.ensure_mutation_allowed()?;
        let files = openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let object_name = derived_object_name(kind, file_id, file_checksum);
        copy_or_reuse_object(
            source_dir,
            source_name,
            &files,
            &object_name,
            file_size,
            file_checksum,
            use_hard_links,
        )
    }

    pub(crate) fn publish_capture_objects(
        &self,
        storage: &crate::lsm_storage::LsmStorageInner,
        capture: &crate::checkpoint::CheckpointCapture<'_>,
        use_hard_links: bool,
        cancelled: Option<&AtomicBool>,
    ) -> Result<(Vec<GenerationObject>, u64, u64, Vec<String>)> {
        self.ensure_mutation_allowed()?;
        let _operation_guard = self.operation_lock.lock();
        self.ensure_mutation_allowed()?;
        ensure!(
            capture.immutable_file_metadata.len() == capture.sst_ids.len() + capture.vlog_ids.len(),
            "capture immutable metadata is incomplete"
        );
        let mut identities = HashSet::new();
        for identity in &capture.immutable_file_metadata {
            ensure!(
                identities.insert(identity.identity()),
                "capture immutable metadata contains duplicates"
            );
            ensure!(
                match identity.kind {
                    crate::manifest::ImmutableFileKind::Sst => {
                        identity.file_id <= usize::MAX as u64
                            && capture.sst_ids.contains(&(identity.file_id as usize))
                    }
                    crate::manifest::ImmutableFileKind::Vlog => {
                        identity.file_id <= u32::MAX as u64
                            && capture.vlog_ids.contains(&(identity.file_id as u32))
                    }
                },
                "capture immutable metadata does not match pinned file IDs"
            );
        }
        let source_root = open_directory_no_follow(storage.db_path())?;
        let vlog_root = storage
            .vlog
            .as_ref()
            .map(|vlog| open_directory_no_follow(&vlog.path))
            .transpose()?;
        let mut reused = 0_u64;
        let mut published = 0_u64;
        let mut objects = Vec::with_capacity(capture.immutable_file_metadata.len());
        let mut new_objects = Vec::new();
        for identity in &capture.immutable_file_metadata {
            if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
                if let Err(cleanup_error) = self.remove_objects(&new_objects) {
                    return Err(anyhow!("backup cancelled before object publication")
                        .context(format!("object cleanup failed: {cleanup_error}")));
                }
                bail!("backup cancelled before object publication");
            }
            let (directory, name) = match identity.kind {
                crate::manifest::ImmutableFileKind::Sst => {
                    (&source_root, format!("{:05}.sst", identity.file_id))
                }
                crate::manifest::ImmutableFileKind::Vlog => (
                    vlog_root
                        .as_ref()
                        .ok_or_else(|| anyhow!("vLog identity without a vLog source"))?,
                    format!("{}.vlog", identity.file_id),
                ),
            };
            let kind = match identity.kind {
                crate::manifest::ImmutableFileKind::Sst => RepositoryObjectKind::Sst,
                crate::manifest::ImmutableFileKind::Vlog => RepositoryObjectKind::Vlog,
            };
            let object_name = derived_object_name(kind, identity.file_id, identity.file_checksum);
            let reused_object = match self.publish_object(
                directory,
                &name,
                kind,
                identity.file_id,
                identity.file_size,
                identity.file_checksum,
                use_hard_links,
            ) {
                Ok(reused) => reused,
                Err(error) => {
                    if let Err(cleanup_error) = self.remove_objects(&new_objects) {
                        return Err(error.context(format!(
                            "failed to reclaim attempt-owned objects after publication error: {cleanup_error}"
                        )));
                    }
                    return Err(error);
                }
            };
            if reused_object {
                let Some(total) = reused.checked_add(identity.file_size) else {
                    if let Err(cleanup_error) = self.remove_objects(&new_objects) {
                        return Err(anyhow!("reused byte count overflow")
                            .context(format!("object cleanup failed: {cleanup_error}")));
                    }
                    bail!("reused byte count overflow");
                };
                reused = total;
            } else {
                new_objects.push(object_name.clone());
                let Some(total) = published.checked_add(identity.file_size) else {
                    if let Err(cleanup_error) = self.remove_objects(&new_objects) {
                        return Err(anyhow!("published byte count overflow")
                            .context(format!("object cleanup failed: {cleanup_error}")));
                    }
                    bail!("published byte count overflow");
                };
                published = total;
            }
            objects.push(GenerationObject {
                kind,
                source_path: name,
                object_name,
                file_id: identity.file_id,
                file_size: identity.file_size,
                file_checksum: identity.file_checksum,
            });
        }
        if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
            if let Err(cleanup_error) = self.remove_objects(&new_objects) {
                return Err(anyhow!("backup cancelled after object publication")
                    .context(format!("object cleanup failed: {cleanup_error}")));
            }
            bail!("backup cancelled after object publication");
        }
        objects.sort_by(|left, right| left.object_name.cmp(&right.object_name));
        Ok((objects, published, reused, new_objects))
    }

    fn remove_objects(&self, names: &[String]) -> Result<()> {
        self.ensure_mutation_allowed()?;
        let _operation_guard = self.operation_lock.lock();
        self.ensure_mutation_allowed()?;
        if names.is_empty() {
            return Ok(());
        }
        let files = openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let mut first_error = None;
        for name in names {
            if let Err(error) = ensure_repository_object_name(name)
                .and_then(|_| validate_object_before_reclaim(&files, name))
            {
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                {
                    continue;
                }
                first_error.get_or_insert(error);
                continue;
            }
            let name = CString::new(name.as_str())?;
            let result = unsafe { libc::unlinkat(files.as_raw_fd(), name.as_ptr(), 0) };
            if result != 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound
            {
                first_error.get_or_insert(anyhow!("failed to remove cancelled backup object"));
            }
        }
        if let Err(error) = fsync_fd(&files) {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    pub fn verify(&self, id: u64) -> Result<()> {
        let _operation_guard = self.operation_lock.lock();
        let replay = self.load_replay()?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let committed = replay
            .committed_generations
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| anyhow!("backup generation {id} is not committed"))?;
        let generation = openat_no_follow(
            &generations,
            &id.to_string(),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let generation_bytes = read_generation_metadata(&generation, "GENERATION")?;
        let checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        ensure!(
            checksum == committed.generation_checksum,
            "backup generation checksum mismatch"
        );
        let snapshot_bytes = read_generation_metadata(&generation, "MANIFEST_SNAPSHOT")?;
        let envelope: GenerationEnvelope = serde_json::from_slice(&generation_bytes)?;
        ensure!(
            envelope.id == id
                && envelope.parent_id == committed.parent_id
                && matches!(envelope.version, 1..=4),
            "backup generation envelope identity mismatch"
        );
        if envelope.version >= 2 {
            ensure!(
                generation_bytes == serde_json::to_vec(&envelope)?,
                "backup generation envelope is not canonically encoded"
            );
        }
        validate_generation_objects(&envelope)?;
        validate_generation_objects_on_disk(&self.root, &envelope)?;
        ensure!(
            envelope.snapshot_len == snapshot_bytes.len() as u64,
            "backup generation snapshot length mismatch"
        );
        let snapshot_checksum: [u8; 32] = Sha256::digest(&snapshot_bytes).into();
        ensure!(
            envelope.snapshot_checksum == snapshot_checksum,
            "backup generation snapshot checksum mismatch"
        );
        if envelope.version >= 2 {
            validate_restore_snapshot_objects(&envelope, &snapshot_bytes)?;
        }
        Ok(())
    }

    /// Verifies every committed generation and all referenced immutable objects.
    pub fn verify_all(&self) -> Result<()> {
        let _operation_guard = self.operation_lock.lock();
        let replay = self.load_replay()?;
        for id in &replay.committed_ids {
            self.verify(*id)
                .with_context(|| format!("backup generation {id} failed verification"))?;
        }
        Ok(())
    }

    /// Reserves the next backup ID durably while the repository's exclusive
    /// lock is held. Abandoned reservations are intentionally never reused.
    pub(crate) fn allocate_backup_id(&mut self) -> Result<u64> {
        self.ensure_mutation_allowed()?;
        self.replay = self.load_replay()?;
        ensure!(
            !self.pending_prepare,
            "backup repository has an uncommitted generation"
        );
        let id = self
            .replay
            .high_water_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("backup catalog id space is exhausted"))?;
        let sequence = self
            .replay
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?;
        let catalog_fd = match openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_WRONLY, 0) {
            Ok(fd) => fd,
            Err(error) => {
                self.usable.store(false, Ordering::Release);
                return Err(error);
            }
        };
        let mut catalog = File::from(catalog_fd);
        if let Err(error) = catalog.seek(SeekFrom::End(0)) {
            self.usable.store(false, Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(
            &mut catalog,
            &CatalogRecord::HighWater {
                sequence,
                allocated_id: id,
            },
        ) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable.store(false, Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        self.replay.high_water_id = id;
        self.replay.last_sequence = sequence;
        Ok(id)
    }

    pub(crate) fn prepare_generation(
        &mut self,
        id: u64,
        parent_id: Option<u64>,
        generation_checksum: [u8; 32],
    ) -> Result<[u8; 32]> {
        self.ensure_mutation_allowed()?;
        ensure!(
            !self.pending_prepare,
            "backup repository already has a pending Prepare"
        );
        ensure!(
            id == self.replay.high_water_id,
            "generation id is not the current reservation"
        );
        ensure!(
            parent_id == self.replay.committed_ids.last().copied(),
            "generation parent does not match the latest committed generation"
        );
        let record = CatalogRecord::Prepare {
            sequence: self
                .replay
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?,
            id,
            parent_id,
            generation_checksum,
        };
        let payload = encode_catalog_payload(&record)?;
        let mut catalog = File::from(openat_no_follow(
            &self.root,
            "BACKUP_MANIFEST",
            libc::O_WRONLY,
            0,
        )?);
        if let Err(error) = catalog.seek(SeekFrom::End(0)) {
            self.usable.store(false, Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(&mut catalog, &record) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable.store(false, Ordering::Release);
            return Err(error.into());
        }
        self.replay.last_sequence = record_sequence(&record);
        self.pending_prepare = true;
        let digest = prepare_payload_digest(&payload);
        self.pending_prepare_digest = Some(digest);
        self.pending_generation_checksum = Some(generation_checksum);
        self.pending_parent_id = Some(parent_id);
        Ok(digest)
    }

    pub(crate) fn commit_generation(
        &mut self,
        id: u64,
        prepare_digest: [u8; 32],
        info: Option<BackupInfo>,
    ) -> Result<()> {
        self.ensure_mutation_allowed()?;
        ensure!(
            self.pending_prepare,
            "backup repository has no pending generation"
        );
        ensure!(
            self.pending_prepare_digest == Some(prepare_digest),
            "commit digest does not match pending Prepare"
        );
        let generation_checksum = self
            .pending_generation_checksum
            .ok_or_else(|| anyhow!("pending Prepare is missing generation checksum"))?;
        let parent_id = self
            .pending_parent_id
            .ok_or_else(|| anyhow!("pending Prepare is missing parent ID"))?;
        ensure!(
            id == self.replay.high_water_id,
            "commit id is not the pending generation"
        );
        let record = CatalogRecord::Commit {
            sequence: self
                .replay
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?,
            id,
            prepare_sequence: self.replay.last_sequence,
            prepare_digest,
        };
        let mut catalog = File::from(openat_no_follow(
            &self.root,
            "BACKUP_MANIFEST",
            libc::O_WRONLY,
            0,
        )?);
        if let Err(error) = catalog.seek(SeekFrom::End(0)) {
            self.usable.store(false, Ordering::Release);
            return Err(anyhow::Error::new(CommitPublicationError {
                id,
                info: info.clone(),
                kind: CommitFailureKind::BeforeCommitRecord,
                source: error.into(),
                revalidation_error: None,
            }));
        }
        if let Err(error) = append_catalog_record(&mut catalog, &record) {
            self.usable.store(false, Ordering::Release);
            let (kind, source, revalidation_error) = match self.revalidate_commit_visibility(id) {
                Ok(Some(true)) => (CommitFailureKind::CommitPublishedButNotDurable, error, None),
                Ok(Some(false)) => (CommitFailureKind::BeforeCommitRecord, error, None),
                Ok(None) => (
                    CommitFailureKind::CommitDurabilityUnknown,
                    error,
                    Some(anyhow!("catalog revalidation was inconclusive")),
                ),
                Err(revalidation) => (
                    CommitFailureKind::CommitDurabilityUnknown,
                    error,
                    Some(revalidation),
                ),
            };
            return Err(anyhow::Error::new(CommitPublicationError {
                id,
                info: info.clone(),
                kind,
                source,
                revalidation_error,
            }));
        }
        if let Err(error) = catalog.sync_all() {
            self.usable.store(false, Ordering::Release);
            let (kind, source, revalidation_error) = match self.revalidate_commit_visibility(id) {
                Ok(Some(true)) => (
                    CommitFailureKind::CommitPublishedButNotDurable,
                    error.into(),
                    None,
                ),
                Ok(Some(false)) => (CommitFailureKind::BeforeCommitRecord, error.into(), None),
                Ok(None) => (
                    CommitFailureKind::CommitDurabilityUnknown,
                    error.into(),
                    Some(anyhow!("catalog revalidation was inconclusive")),
                ),
                Err(revalidation) => (
                    CommitFailureKind::CommitDurabilityUnknown,
                    error.into(),
                    Some(revalidation),
                ),
            };
            return Err(anyhow::Error::new(CommitPublicationError {
                id,
                info: info.clone(),
                kind,
                source,
                revalidation_error,
            }));
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable.store(false, Ordering::Release);
            let (kind, source, revalidation_error) = match self.revalidate_commit_visibility(id) {
                Ok(Some(true)) => (CommitFailureKind::CommitPublishedButNotDurable, error, None),
                Ok(Some(false)) => (CommitFailureKind::BeforeCommitRecord, error, None),
                Ok(None) => (
                    CommitFailureKind::CommitDurabilityUnknown,
                    error,
                    Some(anyhow!("catalog revalidation was inconclusive")),
                ),
                Err(revalidation) => (
                    CommitFailureKind::CommitDurabilityUnknown,
                    error,
                    Some(revalidation),
                ),
            };
            return Err(anyhow::Error::new(CommitPublicationError {
                id,
                info,
                kind,
                source,
                revalidation_error,
            }));
        }
        self.replay.last_sequence = record_sequence(&record);
        self.replay.committed_ids.push(id);
        self.replay.committed_generations.push(CommittedGeneration {
            id,
            parent_id,
            generation_checksum,
            snapshot_metadata: None,
        });
        self.pending_prepare = false;
        self.pending_prepare_digest = None;
        self.pending_generation_checksum = None;
        self.pending_parent_id = None;
        Ok(())
    }

    pub(crate) fn publish_retention(&mut self, retained_ids: &[u64]) -> Result<()> {
        self.ensure_mutation_allowed()?;
        ensure!(!retained_ids.is_empty(), "retention set must not be empty");
        ensure!(
            !self.pending_prepare,
            "backup repository has an uncommitted generation"
        );
        ensure!(
            retained_ids.windows(2).all(|ids| ids[0] < ids[1]) && {
                let committed = self
                    .replay
                    .committed_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>();
                retained_ids.iter().all(|id| committed.contains(id))
            },
            "retention set is invalid"
        );
        let record = CatalogRecord::Retention {
            sequence: self
                .replay
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?,
            retained_ids: retained_ids.to_vec(),
        };
        let catalog_fd = openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_WRONLY, 0)?;
        let mut catalog = File::from(catalog_fd);
        if let Err(error) = catalog.seek(SeekFrom::End(0)) {
            self.usable.store(false, Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(&mut catalog, &record) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable.store(false, Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        self.replay.last_sequence = record_sequence(&record);
        let retained_set = retained_ids.iter().copied().collect::<HashSet<_>>();
        self.replay
            .committed_ids
            .retain(|id| retained_set.contains(id));
        self.replay
            .committed_generations
            .retain(|generation| retained_set.contains(&generation.id));
        Ok(())
    }

    pub(crate) fn compact_catalog(&mut self) -> Result<()> {
        self.ensure_mutation_allowed()?;
        self.replay = self.load_replay()?;
        ensure!(
            !self.pending_prepare,
            "backup repository has an uncommitted generation"
        );
        let catalog_fd = openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_RDONLY, 0)?;
        let mut catalog = File::from(catalog_fd);
        let mut catalog_bytes = Vec::new();
        catalog.read_to_end(&mut catalog_bytes)?;
        let frames = read_catalog_records(catalog_bytes.as_slice())?;
        let base_catalog_digest: [u8; 32] =
            Sha256::digest(&catalog_bytes[..frames.last_complete_offset as usize]).into();
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let mut committed_generations = Vec::with_capacity(self.replay.committed_generations.len());
        for generation in &self.replay.committed_generations {
            committed_generations.push(catalog_generation_snapshot(&generations, generation)?);
        }
        let snapshot = CatalogRecord::Snapshot {
            sequence: self
                .replay
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?,
            base_catalog_digest,
            high_water_id: self.replay.high_water_id,
            committed_generations,
        };
        let temp_name = "BACKUP_MANIFEST.purge.tmp".to_owned();
        let temp_fd = openat_no_follow(
            &self.root,
            &temp_name,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        let mut cleanup = TempObjectCleanup {
            directory: &self.root,
            name: temp_name.clone(),
        };
        let mut temp = File::from(temp_fd);
        append_catalog_record(&mut temp, &snapshot)?;
        temp.sync_all()?;
        temp.seek(SeekFrom::Start(0))?;
        let successor_frames = read_catalog_records(&mut temp)?;
        ensure!(
            successor_frames.frames.len() == 1 && !successor_frames.torn_tail,
            "backup catalog successor must contain one complete snapshot"
        );
        let successor_replay = replay_catalog(&successor_frames)?;
        validate_replay_generations(&self.root, &successor_replay, true)?;
        cleanup.disarm();
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("backup.compact.after_temp_sync");
        }
        let from = CString::new(temp_name.as_str())?;
        let to = CString::new("BACKUP_MANIFEST")?;
        // SAFETY: root is trusted and both names are fixed/generated basenames.
        let result = unsafe {
            libc::renameat(
                self.root.as_raw_fd(),
                from.as_ptr(),
                self.root.as_raw_fd(),
                to.as_ptr(),
            )
        };
        if result != 0 {
            self.usable.store(false, Ordering::Release);
            return Err(std::io::Error::last_os_error().into());
        }
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("backup.compact.after_manifest_replace");
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        self.replay.last_sequence = record_sequence(&snapshot);
        self.replay.retained_offset = 0;
        Ok(())
    }

    /// Compacts the append-only backup catalog into a single snapshot record.
    pub fn compact(&mut self) -> Result<()> {
        let usable = Arc::clone(&self.usable);
        let mut unwind_guard = UnwindInvalidationGuard::new(&usable);
        let result = self.compact_catalog();
        unwind_guard.disarm();
        result
    }

    pub fn purge(&self, retain: usize) -> Result<()> {
        let _operation_guard = self.operation_lock.lock();
        self.ensure_mutation_allowed()?;
        let mut unwind_guard = UnwindInvalidationGuard::new(&self.usable);
        let mut working = BackupRepository {
            root: self.root.try_clone()?,
            _lock: self._lock.duplicate()?,
            replay: self.load_replay()?,
            usable: Arc::clone(&self.usable),
            stale_after_restore: AtomicBool::new(false),
            operation_lock: ReentrantMutex::new(()),
            pending_prepare: false,
            pending_prepare_digest: None,
            pending_generation_checksum: None,
            pending_parent_id: None,
        };
        let result = working.purge_inner(retain);
        unwind_guard.disarm();
        result
    }

    fn purge_inner(&mut self, retain: usize) -> Result<()> {
        self.ensure_mutation_allowed()?;
        let retained = self.retained_ids(retain)?;
        let unreferenced = self.unreferenced_object_names(retain)?;
        let replay = self.load_replay()?;
        let removed_generations = replay
            .committed_ids
            .iter()
            .copied()
            .filter(|id| !retained.contains(id))
            .collect::<Vec<_>>();
        if !removed_generations.is_empty() {
            self.publish_retention(&retained)?;
            self.compact_catalog()?;
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::fail_point!("backup.purge.after_snapshot");
        }
        let generations = match openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        ) {
            Ok(fd) => fd,
            Err(error) => {
                self.usable.store(false, Ordering::Release);
                return Err(error);
            }
        };
        for id in removed_generations {
            if let Err(error) = remove_generation_directory(&generations, id) {
                self.usable.store(false, Ordering::Release);
                return Err(error);
            }
        }
        if let Err(error) = fsync_fd(&generations) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("backup.purge.after_generation_reclaim");
        let files =
            match openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0) {
                Ok(fd) => fd,
                Err(error) => {
                    self.usable.store(false, Ordering::Release);
                    return Err(error);
                }
            };
        for name in unreferenced {
            if let Err(error) = validate_object_before_reclaim(&files, &name) {
                self.usable.store(false, Ordering::Release);
                return Err(error);
            }
            let name = CString::new(name)?;
            // SAFETY: files is trusted and names came from validated entries.
            let result = unsafe { libc::unlinkat(files.as_raw_fd(), name.as_ptr(), 0) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::NotFound {
                    self.usable.store(false, Ordering::Release);
                    return Err(error.into());
                }
            }
        }
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("backup.purge.after_object_reclaim");
        if let Err(error) = fsync_fd(&files) {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        #[cfg(feature = "chaos-testing")]
        crate::chaos::failpoint::fail_point!("backup.purge.after_object_fsync");
        let root_sync_result = (|| {
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::fail_point!("backup.purge.before_root_fsync", |_| {
                Err(anyhow!("injected backup purge root fsync failure"))
            });
            fsync_fd(&self.root)
        })();
        if let Err(error) = root_sync_result {
            self.usable.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn stage_generation(
        &self,
        id: u64,
        parent_id: Option<u64>,
        generation: &[u8],
        snapshot: &[u8],
        objects: &[GenerationObject],
        new_object_bytes: u64,
        compatibility: Option<RestoreCompatibility>,
    ) -> Result<(String, Vec<u8>)> {
        self.ensure_mutation_allowed()?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let name = id.to_string();
        let attempt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let staging = format!(".{name}.staging-{}-{attempt}", std::process::id());
        let staging_fd = mkdirat_exclusive(&generations, &staging, 0o700)?;
        let mut cleanup = StagingCleanup {
            root: &self.root,
            name: staging.clone(),
        };
        let snapshot_checksum: [u8; 32] = Sha256::digest(snapshot).into();
        let generation = serde_json::to_vec(&GenerationEnvelope {
            version: if compatibility.is_some() { 4 } else { 3 },
            id,
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            parent_id,
            new_object_bytes,
            snapshot_len: snapshot.len() as u64,
            snapshot_checksum,
            objects: Some(objects.to_vec()),
            compatibility,
            body: generation.to_vec(),
        })?;
        ensure!(
            generation.len() <= MAX_GENERATION_METADATA_BYTES,
            "generation metadata exceeds limit"
        );
        let envelope: GenerationEnvelope = serde_json::from_slice(&generation)?;
        validate_generation_objects(&envelope)?;
        for (file_name, bytes) in [
            ("GENERATION", generation.as_slice()),
            ("MANIFEST_SNAPSHOT", snapshot),
        ] {
            let mut file = File::from(openat_no_follow(
                &staging_fd,
                file_name,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )?);
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        fsync_fd(&staging_fd)?;
        cleanup.disarm();
        Ok((staging, generation))
    }

    fn publish_staged_generation(&mut self, id: u64, staging: &str) -> Result<()> {
        self.ensure_mutation_allowed()?;
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let name = id.to_string();
        let from = CString::new(staging)?;
        let to = CString::new(name)?;
        // SAFETY: descriptors and names are valid; RENAME_NOREPLACE prevents overwrite.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                generations.as_raw_fd(),
                from.as_ptr(),
                generations.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        ensure!(
            result == 0,
            "failed to publish backup generation: {}",
            std::io::Error::last_os_error()
        );
        if let Err(error) = fsync_fd(&generations).and_then(|_| fsync_fd(&self.root)) {
            self.usable.store(false, Ordering::Release);
            return Err(anyhow::Error::new(RepositoryPublicationError {
                id,
                source: error,
            }));
        }
        Ok(())
    }

    /// Publishes one metadata-only generation in the required durable order.
    pub(crate) fn create_generation(&mut self, generation: &[u8], snapshot: &[u8]) -> Result<u64> {
        self.create_generation_with_objects(
            generation,
            snapshot,
            &[],
            0,
            None,
            &[],
            None,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(unused_variables)]
    fn create_generation_with_objects(
        &mut self,
        generation: &[u8],
        snapshot: &[u8],
        objects: &[GenerationObject],
        new_object_bytes: u64,
        compatibility: Option<RestoreCompatibility>,
        new_objects: &[String],
        cancelled: Option<&AtomicBool>,
        decision: Option<&Mutex<bool>>,
        decision_token: Option<u64>,
    ) -> Result<u64> {
        self.ensure_mutation_allowed()?;
        let id = self.allocate_backup_id()?;
        let parent_id = self.replay.committed_ids.last().copied();
        let (staging, generation_bytes) = self.stage_generation(
            id,
            parent_id,
            generation,
            snapshot,
            objects,
            new_object_bytes,
            compatibility,
        )?;
        let generation_checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        let envelope: GenerationEnvelope = match serde_json::from_slice(&generation_bytes) {
            Ok(envelope) => envelope,
            Err(error) => {
                let objects_result = self.remove_objects(new_objects);
                let mut primary = anyhow!(error);
                if let Err(cleanup_error) = cleanup_staging_generation(&self.root, &staging) {
                    primary = primary.context(format!("staging cleanup failed: {cleanup_error}"));
                }
                if let Err(cleanup_error) = objects_result {
                    primary = primary.context(format!("object cleanup failed: {cleanup_error}"));
                }
                return Err(primary);
            }
        };
        let logical_bytes = match objects.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.file_size)
                .ok_or_else(|| anyhow!("backup logical byte count overflow"))
        }) {
            Ok(bytes) => bytes,
            Err(error) => {
                let objects_result = self.remove_objects(new_objects);
                let mut primary = error;
                if let Err(cleanup_error) = cleanup_staging_generation(&self.root, &staging) {
                    primary = primary.context(format!("staging cleanup failed: {cleanup_error}"));
                }
                if let Err(cleanup_error) = objects_result {
                    primary = primary.context(format!("object cleanup failed: {cleanup_error}"));
                }
                return Err(primary);
            }
        };
        let info = BackupInfo {
            id,
            created_at_secs: envelope.created_at_secs,
            parent_id,
            logical_bytes,
            file_count: objects.len() as u64,
            new_object_bytes,
        };
        if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
            let staging_result = cleanup_staging_generation(&self.root, &staging);
            let objects_result = self.remove_objects(new_objects);
            let mut primary = anyhow!("backup cancelled before Prepare");
            if let Err(cleanup_error) = staging_result {
                primary = primary.context(format!("staging cleanup failed: {cleanup_error}"));
            }
            if let Err(cleanup_error) = objects_result {
                primary = primary.context(format!("object cleanup failed: {cleanup_error}"));
            }
            return Err(primary);
        }
        let prepare_digest = match self.prepare_generation(id, parent_id, generation_checksum) {
            Ok(digest) => digest,
            Err(error) => {
                let staging_result = cleanup_staging_generation(&self.root, &staging);
                let objects_result = self.remove_objects(new_objects);
                let mut primary = error;
                if let Err(cleanup_error) = staging_result {
                    primary = primary.context(format!("staging cleanup failed: {cleanup_error}"));
                }
                if let Err(cleanup_error) = objects_result {
                    primary = primary.context(format!("object cleanup failed: {cleanup_error}"));
                }
                return Err(primary);
            }
        };
        if let Err(error) = self.publish_staged_generation(id, &staging) {
            if error.downcast_ref::<RepositoryPublicationError>().is_some() {
                let rollback_result = self.discard_pending_generation(id);
                let objects_result = self.remove_objects(new_objects);
                let mut primary = error;
                if let Err(cleanup_error) = rollback_result {
                    primary = primary.context(format!(
                        "failed to clean renamed generation after publication failure: {cleanup_error}"
                    ));
                }
                if let Err(cleanup_error) = objects_result {
                    primary = primary.context(format!(
                        "failed to reclaim attempt-owned objects after publication failure: {cleanup_error}"
                    ));
                }
                return Err(primary);
            } else {
                let staging_result = cleanup_staging_generation(&self.root, &staging);
                let rollback_result = self.discard_pending_generation(id);
                let objects_result = self.remove_objects(new_objects);
                let mut primary = error;
                if let Err(cleanup_error) = staging_result {
                    primary = primary.context(format!("staging cleanup failed: {cleanup_error}"));
                }
                if let Err(cleanup_error) = rollback_result {
                    primary = primary.context(format!("pending rollback failed: {cleanup_error}"));
                }
                if let Err(cleanup_error) = objects_result {
                    primary = primary.context(format!("object cleanup failed: {cleanup_error}"));
                }
                return Err(primary);
            }
        }
        if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
            let rollback_result = self.discard_pending_generation(id);
            let objects_result = self.remove_objects(new_objects);
            let mut primary = anyhow!("backup cancelled before Commit");
            if let Err(cleanup_error) = rollback_result {
                primary = primary.context(format!("pending rollback failed: {cleanup_error}"));
            }
            if let Err(cleanup_error) = objects_result {
                primary = primary.context(format!("object cleanup failed: {cleanup_error}"));
            }
            return Err(primary);
        }
        #[cfg(test)]
        commit_decision_test_hook::wait_if_armed(decision_token.unwrap_or_default(), id);
        if let Some(decision) = decision {
            let mut decided = decision.lock();
            if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
                drop(decided);
                let rollback_result = self.discard_pending_generation(id);
                let objects_result = self.remove_objects(new_objects);
                let mut primary = anyhow!("backup cancelled before Commit");
                if let Err(error) = rollback_result {
                    primary = primary.context(format!("pending rollback failed: {error}"));
                }
                if let Err(error) = objects_result {
                    primary = primary.context(format!("object cleanup failed: {error}"));
                }
                return Err(primary);
            }
            *decided = true;
        }
        #[cfg(test)]
        commit_decision_test_hook::wait_if_armed_after(decision_token.unwrap_or_default(), id);
        self.commit_generation(id, prepare_digest, Some(info))?;
        Ok(id)
    }
}

#[cfg(target_os = "linux")]
fn cleanup_stale_catalog_temps(root: &OwnedFd) -> Result<()> {
    let path = PathBuf::from(format!("/proc/self/fd/{}", root.as_raw_fd()));
    let mut removed = false;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(suffix) = name.strip_prefix(".BACKUP_MANIFEST.compact-") else {
            continue;
        };
        let Some((pid, sequence)) = suffix.split_once('-') else {
            continue;
        };
        let (Ok(pid), Ok(sequence)) = (pid.parse::<u64>(), sequence.parse::<u64>()) else {
            continue;
        };
        if pid == 0 || name != format!(".BACKUP_MANIFEST.compact-{pid}-{sequence}") {
            continue;
        }
        let Ok(file) = openat_no_follow(root, name, libc::O_RDONLY, 0) else {
            continue;
        };
        if ensure_regular_file(file.as_raw_fd()).is_err() {
            continue;
        }
        let name = CString::new(name)?;
        // SAFETY: root is trusted and the name was validated as a generated temp basename.
        let result = unsafe { libc::unlinkat(root.as_raw_fd(), name.as_ptr(), 0) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error.into());
            }
        } else {
            removed = true;
        }
    }
    if removed {
        fsync_fd(root)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn recover_catalog_successor(root: &OwnedFd) -> Result<()> {
    let successor = match openat_no_follow(root, "BACKUP_MANIFEST.purge.tmp", libc::O_RDONLY, 0) {
        Ok(file) => file,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    ensure_regular_file(successor.as_raw_fd())?;
    let mut successor = File::from(successor);
    let successor_frames = read_catalog_records(&mut successor)?;
    if successor_frames.frames.is_empty() || successor_frames.torn_tail {
        let name = CString::new("BACKUP_MANIFEST.purge.tmp")?;
        // SAFETY: root is trusted and the name is a fixed basename.
        let result = unsafe { libc::unlinkat(root.as_raw_fd(), name.as_ptr(), 0) };
        ensure!(
            result == 0 || std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound,
            "failed to discard incomplete backup purge successor: {}",
            std::io::Error::last_os_error()
        );
        return fsync_fd(root);
    }
    ensure!(
        successor_frames.frames.len() == 1 && !successor_frames.torn_tail,
        "backup purge successor must contain one complete snapshot"
    );
    let CatalogRecord::Snapshot {
        sequence,
        base_catalog_digest,
        ..
    } = &successor_frames.frames[0].record
    else {
        bail!("backup purge successor is not a catalog snapshot");
    };
    let primary = openat_no_follow(root, "BACKUP_MANIFEST", libc::O_RDONLY, 0)?;
    let mut primary = File::from(primary);
    let mut primary_bytes = Vec::new();
    primary.read_to_end(&mut primary_bytes)?;
    let primary_frames = read_catalog_records(primary_bytes.as_slice())?;
    let primary_replay = replay_catalog(&primary_frames)?;
    let successor_replay = replay_catalog(&successor_frames)?;
    let primary_digest: [u8; 32] =
        Sha256::digest(&primary_bytes[..primary_frames.last_complete_offset as usize]).into();
    ensure!(
        *base_catalog_digest == primary_digest,
        "backup purge successor base catalog digest mismatch"
    );
    ensure!(
        *sequence
            == primary_frames
                .frames
                .last()
                .map_or(0, |frame| record_sequence(&frame.record))
                .checked_add(1)
                .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?,
        "backup purge successor sequence is invalid"
    );
    ensure!(
        successor_replay.high_water_id == primary_replay.high_water_id
            && successor_replay.committed_ids == primary_replay.committed_ids,
        "backup purge successor retained generation set mismatch"
    );
    validate_replay_generations(root, &successor_replay, true)?;
    let from = CString::new("BACKUP_MANIFEST.purge.tmp")?;
    let to = CString::new("BACKUP_MANIFEST")?;
    // SAFETY: root is trusted and both names are fixed basenames.
    let result = unsafe {
        libc::renameat(
            root.as_raw_fd(),
            from.as_ptr(),
            root.as_raw_fd(),
            to.as_ptr(),
        )
    };
    ensure!(
        result == 0,
        "failed to install backup purge successor: {}",
        std::io::Error::last_os_error()
    );
    fsync_fd(root)
}

#[cfg(target_os = "linux")]
fn validate_replay_generations(
    root: &OwnedFd,
    replay: &CatalogReplay,
    require_snapshot_metadata: bool,
) -> Result<()> {
    let generations = openat_no_follow(root, "generations", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    for committed in &replay.committed_generations {
        let generation = openat_no_follow(
            &generations,
            &committed.id.to_string(),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let generation_bytes = read_generation_metadata(&generation, "GENERATION")?;
        let checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        ensure!(
            checksum == committed.generation_checksum,
            "backup generation checksum mismatch"
        );
        let envelope: GenerationEnvelope =
            serde_json::from_slice(&generation_bytes).context("invalid generation envelope")?;
        ensure!(
            matches!(envelope.version, 1..=4),
            "unsupported generation envelope version"
        );
        ensure!(
            envelope.id == committed.id,
            "generation envelope id mismatch"
        );
        ensure!(
            envelope.parent_id == committed.parent_id,
            "generation envelope parent mismatch"
        );
        validate_generation_objects(&envelope)?;
        if envelope.version >= 2 {
            ensure!(
                generation_bytes == serde_json::to_vec(&envelope)?,
                "generation envelope is not canonically encoded"
            );
            validate_generation_object_metadata_on_disk(root, &envelope)?;
        }
        let snapshot_bytes = read_generation_metadata(&generation, "MANIFEST_SNAPSHOT")?;
        ensure!(
            envelope.snapshot_len == snapshot_bytes.len() as u64,
            "generation snapshot length mismatch"
        );
        let snapshot_checksum: [u8; 32] = Sha256::digest(&snapshot_bytes).into();
        ensure!(
            envelope.snapshot_checksum == snapshot_checksum,
            "generation snapshot checksum mismatch"
        );
        if envelope.version >= 2 {
            validate_restore_snapshot_objects(&envelope, &snapshot_bytes)?;
        }
        if let Some(metadata) = &committed.snapshot_metadata {
            let fields_present = metadata.manifest_snapshot_len.is_some()
                && metadata.manifest_snapshot_checksum.is_some()
                && metadata.created_at_secs.is_some()
                && metadata.logical_bytes.is_some()
                && metadata.new_object_bytes.is_some()
                && metadata.file_count.is_some();
            let fields_absent = metadata.manifest_snapshot_len.is_none()
                && metadata.manifest_snapshot_checksum.is_none()
                && metadata.created_at_secs.is_none()
                && metadata.logical_bytes.is_none()
                && metadata.new_object_bytes.is_none()
                && metadata.file_count.is_none();
            ensure!(
                fields_present || fields_absent,
                "partial catalog snapshot metadata"
            );
            ensure!(
                !require_snapshot_metadata || fields_present,
                "backup purge successor is missing generation metadata"
            );
            if fields_present {
                let objects = envelope.objects.as_deref().unwrap_or_default();
                let logical_bytes = objects.iter().try_fold(0_u64, |total, object| {
                    total
                        .checked_add(object.file_size)
                        .ok_or_else(|| anyhow!("backup logical byte count overflow"))
                })?;
                ensure!(
                    metadata.manifest_snapshot_len == Some(snapshot_bytes.len() as u64)
                        && metadata.manifest_snapshot_checksum == Some(snapshot_checksum)
                        && metadata.created_at_secs == Some(envelope.created_at_secs)
                        && metadata.logical_bytes == Some(logical_bytes)
                        && metadata.new_object_bytes == Some(envelope.new_object_bytes)
                        && metadata.file_count == Some(objects.len() as u64),
                    "catalog snapshot generation metadata mismatch"
                );
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_object_before_reclaim(files: &OwnedFd, name: &str) -> Result<()> {
    let file = File::from(openat_no_follow(files, name, libc::O_RDONLY, 0)?);
    ensure_regular_file(file.as_raw_fd())?;
    let mut file = file;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let checksum: [u8; 32] = hasher.finalize().into();
    let expected = name.rsplit('-').next().unwrap_or_default();
    let actual = checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    ensure!(
        actual == expected,
        "repository object changed before reclaim"
    );
    Ok(())
}

fn ensure_repository_object_name(name: &str) -> Result<()> {
    let mut parts = name.split('-');
    let prefix = parts.next().unwrap_or_default();
    let id = parts.next().unwrap_or_default();
    let digest = parts.next().unwrap_or_default();
    ensure!(
        matches!(prefix, "sst" | "vlog")
            && id.parse::<u64>().is_ok()
            && (id == "0" || !id.starts_with('0'))
            && digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            && parts.next().is_none(),
        "unexpected repository object name"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
struct RestoreStagingCleanup<'a> {
    parent: &'a OwnedFd,
    name: String,
}

#[cfg(target_os = "linux")]
fn ensure_restore_target_absent(parent: &OwnedFd, name: &str) -> Result<()> {
    match openat_no_follow(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0) {
        Ok(_) => bail!("restore target already exists"),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
impl RestoreStagingCleanup<'_> {
    fn disarm(&mut self) {
        self.name.clear();
    }
}

#[cfg(target_os = "linux")]
impl Drop for RestoreStagingCleanup<'_> {
    fn drop(&mut self) {
        if self.name.is_empty() {
            return;
        }
        if let Ok(name) = CString::new(self.name.as_str()) {
            if let Ok(staging) = openat_no_follow(
                self.parent,
                self.name.as_str(),
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            ) {
                remove_restore_staging_contents(&staging);
            }
            // SAFETY: parent is trusted and name is generated by this module.
            unsafe {
                libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR);
            }
            let _ = fsync_fd(self.parent);
        }
    }
}

#[cfg(target_os = "linux")]
fn remove_restore_staging_contents(directory: &OwnedFd) {
    // SAFETY: directory is valid; the duplicate is consumed by fdopendir.
    let duplicate = unsafe { libc::dup(directory.as_raw_fd()) };
    if duplicate < 0 {
        return;
    }
    // SAFETY: duplicate is uniquely owned and valid; closed by closedir.
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        // SAFETY: fdopendir did not take ownership on failure.
        unsafe { libc::close(duplicate) };
        return;
    }
    loop {
        // SAFETY: stream remains valid until closedir.
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            break;
        }
        // SAFETY: d_name is NUL-terminated for this directory entry.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        let bytes = name.to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: directory and name are valid; stat is writable storage.
        let result = unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result != 0 {
            continue;
        }
        // SAFETY: fstatat initialized stat on success.
        let stat = unsafe { stat.assume_init() };
        if (stat.st_mode & libc::S_IFMT) == libc::S_IFDIR {
            // SAFETY: directory and name are valid; no-follow prevents traversal.
            let child = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if child >= 0 {
                // SAFETY: child is uniquely owned after successful openat.
                let child = unsafe { OwnedFd::from_raw_fd(child) };
                remove_restore_staging_contents(&child);
            }
            // SAFETY: directory and name are valid; removal does not follow symlinks.
            unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
        } else {
            // SAFETY: directory and name are valid; removal does not follow symlinks.
            unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) };
        }
    }
    // SAFETY: fdopendir owns stream and its descriptor.
    unsafe { libc::closedir(stream) };
}

#[cfg(target_os = "linux")]
fn backup_outcome_from_error(repository: PathBuf, error: anyhow::Error) -> Result<BackupOutcome> {
    let error = match error.downcast::<RepositoryPublicationError>() {
        Ok(publication) => {
            return Ok(BackupOutcome::RepositoryPublishedButNotDurable {
                repository,
                error: into_io_error(publication.source),
            });
        }
        Err(error) => error,
    };
    let error = match error.downcast::<RepositoryBootstrapPublicationError>() {
        Ok(publication) => {
            return Ok(BackupOutcome::RepositoryPublishedButNotDurable {
                repository,
                error: into_io_error(publication.source),
            });
        }
        Err(error) => error,
    };
    match error.downcast::<CommitPublicationError>() {
        Ok(publication) if publication.kind == CommitFailureKind::CommitDurabilityUnknown => {
            let info = publication
                .info
                .ok_or_else(|| anyhow!("commit publication metadata is unavailable"))?;
            Ok(BackupOutcome::CommitPublicationUnknown {
                info,
                fsync_error: into_io_error(publication.source),
                revalidation_error: publication.revalidation_error.ok_or_else(|| {
                    anyhow!("commit publication was unknown without a revalidation error")
                })?,
            })
        }
        Ok(publication) if publication.kind == CommitFailureKind::CommitPublishedButNotDurable => {
            let info = publication
                .info
                .ok_or_else(|| anyhow!("commit publication metadata is unavailable"))?;
            Ok(BackupOutcome::CommitPublishedButNotDurable {
                info,
                error: into_io_error(publication.source),
            })
        }
        Ok(publication) => Err(publication.source),
        Err(error) => Err(error),
    }
}

/// Preserves a concrete I/O error for the RFC outcome contract. Internal
/// publication errors may add context, but the public durability outcome must
/// retain the original error kind and OS error whenever the source was I/O.
fn into_io_error(error: anyhow::Error) -> std::io::Error {
    match error.downcast::<std::io::Error>() {
        Ok(error) => error,
        Err(error) => std::io::Error::other(error),
    }
}

fn sync_outcome(outcome: BackupOutcome) -> Result<CreateBackupOutcome> {
    match outcome {
        BackupOutcome::Committed(info) => Ok(CreateBackupOutcome::Committed(info)),
        BackupOutcome::RepositoryPublishedButNotDurable { repository, error } => {
            Ok(CreateBackupOutcome::RepositoryPublishedButNotDurable { repository, error })
        }
        BackupOutcome::CommitPublishedButNotDurable { info, error } => {
            Ok(CreateBackupOutcome::CommitPublishedButNotDurable { info, error })
        }
        BackupOutcome::CommitPublicationUnknown {
            info,
            fsync_error,
            revalidation_error,
        } => Ok(CreateBackupOutcome::CommitPublicationUnknown {
            info,
            fsync_error,
            revalidation_error,
        }),
        BackupOutcome::CancelledBeforeCommit | BackupOutcome::CommittedAfterCancellation(_) => {
            Err(anyhow!("cancellation is not valid for synchronous backup"))
        }
    }
}

#[cfg(target_os = "linux")]
impl crate::lsm_storage::KvEngine {
    #[deprecated(note = "use create_backup")]
    pub fn create_backup_info(&self, options: BackupOptions) -> Result<BackupInfo> {
        let _lifecycle_guard = self.inner.lifecycle.admit_write()?;
        self.inner.create_backup_inner(options)
    }

    /// Eagerly dispatches a backup task onto the engine's blocking executor.
    ///
    /// The caller must be inside a Tokio runtime. Dropping or cancelling the
    /// returned task requests cancellation; a worker that has already passed
    /// the commit decision may still publish a generation.
    pub fn create_backup_task(&self, options: BackupOptions) -> Result<BackupTask> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|error| anyhow!("backup task requires a Tokio runtime: {error}"))?;
        let guard = self.inner.lifecycle.admit_write()?;
        let inner = self.inner.clone();
        let repository = options.repository.clone();
        let control = Arc::new(BackupTaskControl {
            cancelled: AtomicBool::new(false),
            commit_decided: Mutex::new(false),
            #[cfg(test)]
            barrier_token: commit_decision_test_hook::next_token(),
        });
        let task_control = Arc::clone(&control);
        let handle = runtime.spawn(async move {
            if task_control.cancelled.load(Ordering::Acquire) {
                drop(guard);
                return Ok(BackupOutcome::CancelledBeforeCommit);
            }
            let worker_inner = inner.clone();
            let worker_control = Arc::clone(&task_control);
            let result = inner
                .blocking
                .run_result_cancelable(&task_control.cancelled, move || {
                    let _guard = guard;
                    worker_inner.create_backup_inner_with_cancellation(
                        options,
                        Some(&worker_control.cancelled),
                        Some(&worker_control.commit_decided),
                        #[cfg(test)]
                        Some(worker_control.barrier_token),
                        #[cfg(not(test))]
                        None,
                    )
                })
                .await;
            match result {
                Ok(None) => Ok(BackupOutcome::CancelledBeforeCommit),
                Ok(Some(info)) if task_control.cancelled.load(Ordering::Acquire) => {
                    Ok(BackupOutcome::CommittedAfterCancellation(info))
                }
                Ok(Some(info)) => Ok(BackupOutcome::Committed(info)),
                Err(error)
                    if error.chain().count() == 1
                        && error.to_string().starts_with("backup cancelled") =>
                {
                    Ok(BackupOutcome::CancelledBeforeCommit)
                }
                Err(error) => backup_outcome_from_error(repository, error),
            }
        });
        Ok(BackupTask {
            handle: Some(handle),
            ready: None,
            control,
        })
    }

    pub fn create_backup(&self, options: BackupOptions) -> Result<CreateBackupOutcome> {
        sync_outcome(self.create_backup_with_outcome(options)?)
    }

    pub fn create_backup_with_outcome(&self, options: BackupOptions) -> Result<BackupOutcome> {
        let _guard = self.inner.lifecycle.admit_write()?;
        let repository = options.repository.clone();
        match self.inner.create_backup_inner(options) {
            Ok(info) => Ok(BackupOutcome::Committed(info)),
            Err(error) => backup_outcome_from_error(repository, error),
        }
    }

    /// RFC 022-named typed synchronous backup entry point.
    pub fn create_backup_outcome(&self, options: BackupOptions) -> Result<CreateBackupOutcome> {
        self.create_backup(options)
    }

    pub fn create_backup_async(&self, options: BackupOptions) -> BackupTask {
        match self.create_backup_task(options) {
            Ok(task) => task,
            Err(error) => BackupTask::ready(error),
        }
    }

    #[deprecated(note = "use create_backup_async")]
    pub async fn create_backup_async_info(&self, options: BackupOptions) -> Result<BackupInfo> {
        let lifecycle_guard = self.inner.lifecycle.admit_write()?;
        let inner = self.inner.clone();
        self.inner
            .blocking
            .run_result(move || {
                let _lifecycle_guard = lifecycle_guard;
                inner.create_backup_inner(options)
            })
            .await
    }

    pub async fn create_backup_async_with_outcome(
        &self,
        options: BackupOptions,
    ) -> Result<BackupOutcome> {
        let guard = self.inner.lifecycle.admit_write()?;
        let inner = self.inner.clone();
        let repository = options.repository.clone();
        self.inner
            .blocking
            .run_result(move || {
                let _guard = guard;
                match inner.create_backup_inner(options) {
                    Ok(info) => Ok(BackupOutcome::Committed(info)),
                    Err(error) => backup_outcome_from_error(repository, error),
                }
            })
            .await
    }

    /// RFC 022-named typed asynchronous backup entry point.
    pub async fn create_backup_async_outcome(
        &self,
        options: BackupOptions,
    ) -> Result<CreateBackupOutcome> {
        self.create_backup_async_with_outcome(options)
            .await
            .and_then(sync_outcome)
    }
}

impl crate::lsm_storage::LsmStorageInner {
    fn create_backup_inner(&self, options: BackupOptions) -> Result<BackupInfo> {
        self.create_backup_inner_with_cancellation(options, None, None, None)
    }

    fn create_backup_inner_with_cancellation(
        &self,
        options: BackupOptions,
        cancelled: Option<&AtomicBool>,
        decision: Option<&Mutex<bool>>,
        decision_token: Option<u64>,
    ) -> Result<BackupInfo> {
        self.ensure_manifest_v6()?;
        let capture = self.prepare_backup_capture()?;
        let BackupOptions {
            repository: repository_path,
            use_hard_links,
        } = options;
        let parent = repository_path.parent().unwrap_or_else(|| Path::new("."));
        let name = repository_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("backup repository path must have a UTF-8 basename"))?;
        let parent_fd = open_directory_no_follow(parent)?;
        let mut repository = match BackupRepository::open_at(&parent_fd, name) {
            Ok(repository) => repository,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                match bootstrap_repository(&parent_fd, name) {
                    Ok(()) => BackupRepository::open_at(&parent_fd, name)?,
                    Err(error)
                        if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                            error.kind() == std::io::ErrorKind::AlreadyExists
                        }) =>
                    {
                        BackupRepository::open_at(&parent_fd, name)?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        let (objects, new_object_bytes, _, new_objects) =
            repository.publish_capture_objects(self, &capture, use_hard_links, cancelled)?;
        let snapshot = serde_json::to_vec(&capture.snapshot_record)?;
        let value_separation_enabled = self
            .options
            .value_separation
            .as_ref()
            .is_some_and(|options| options.enabled);
        let compatibility = RestoreCompatibility {
            manifest_format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
            value_separation_enabled,
            vlog_format_version: value_separation_enabled
                .then_some(crate::vlog::VLOG_FORMAT_VERSION),
            ttl_records_present: capture.has_ttl_entries,
            serializable_at_capture: self.options.serializable,
        };
        let id = repository.create_generation_with_objects(
            &snapshot,
            &snapshot,
            &objects,
            new_object_bytes,
            Some(compatibility),
            &new_objects,
            cancelled,
            decision,
            decision_token,
        )?;
        repository
            .list_info()?
            .into_iter()
            .find(|info| info.id == id)
            .ok_or_else(|| anyhow!("committed backup generation is missing from catalog"))
    }
}

#[cfg(target_os = "linux")]
fn cleanup_staging_generation(root: &OwnedFd, staging: &str) -> Result<()> {
    let generations = openat_no_follow(root, "generations", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    let generation =
        openat_no_follow(&generations, staging, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    let mut first_error = None;
    for name in ["GENERATION", "MANIFEST_SNAPSHOT"] {
        let name = CString::new(name).unwrap();
        // SAFETY: generation is a trusted descriptor and name is fixed.
        let result = unsafe { libc::unlinkat(generation.as_raw_fd(), name.as_ptr(), 0) };
        if result != 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound {
            first_error.get_or_insert(anyhow!("failed to remove staged metadata"));
        }
    }
    let name = CString::new(staging).unwrap();
    // SAFETY: generations is trusted and staging is a generated basename.
    let result =
        unsafe { libc::unlinkat(generations.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
    if result != 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound {
        first_error.get_or_insert(anyhow!("failed to remove staging directory"));
    }
    if let Err(error) = fsync_fd(&generations) {
        first_error.get_or_insert(error);
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(target_os = "linux")]
fn remove_generation_directory(generations: &OwnedFd, id: u64) -> Result<()> {
    let generation = match openat_no_follow(
        generations,
        &id.to_string(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    ) {
        Ok(fd) => fd,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    for name in ["GENERATION", "MANIFEST_SNAPSHOT"] {
        let name = CString::new(name)?;
        // SAFETY: generation is trusted and names are fixed metadata files.
        let result = unsafe { libc::unlinkat(generation.as_raw_fd(), name.as_ptr(), 0) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }
    }
    let name = CString::new(id.to_string())?;
    // SAFETY: generations is trusted and the ID-derived name is a basename.
    let result =
        unsafe { libc::unlinkat(generations.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error.into());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn remove_uncommitted_generation_orphans(
    generations: &OwnedFd,
    committed_ids: &[u64],
) -> Result<()> {
    let path = PathBuf::from(format!("/proc/self/fd/{}", generations.as_raw_fd()));
    let mut removed = false;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(id) = name.parse::<u64>() else {
            continue;
        };
        if !committed_ids.contains(&id) {
            remove_generation_directory(generations, id)?;
            removed = true;
        }
    }
    if removed {
        fsync_fd(generations)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct StagingCleanup<'a> {
    root: &'a OwnedFd,
    name: String,
}

#[cfg(target_os = "linux")]
impl StagingCleanup<'_> {
    fn disarm(&mut self) {
        self.name.clear();
    }
}

#[cfg(target_os = "linux")]
impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        if !self.name.is_empty()
            && let Err(error) = cleanup_staging_generation(self.root, &self.name)
        {
            log::warn!("failed to clean staged backup generation during drop: {error}");
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn ensure_regular_file(fd: std::os::fd::RawFd) -> Result<()> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fd is valid and stat points to writable storage of the expected type.
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    ensure!(
        result == 0,
        "failed to stat backup metadata: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: fstat initialized stat on success.
    let stat = unsafe { stat.assume_init() };
    ensure!(
        (stat.st_mode & libc::S_IFMT) == libc::S_IFREG,
        "backup metadata must be a regular file"
    );
    Ok(())
}

fn validate_generation_objects(envelope: &GenerationEnvelope) -> Result<()> {
    if envelope.version < 4 {
        ensure!(
            envelope.compatibility.is_none(),
            "legacy generation envelope must not contain restore compatibility"
        );
    }
    if envelope.version >= 4 {
        let compatibility = envelope
            .compatibility
            .as_ref()
            .ok_or_else(|| anyhow!("v4 generation envelope is missing restore compatibility"))?;
        ensure!(
            (3..=crate::manifest::MANIFEST_FORMAT_VERSION)
                .contains(&compatibility.manifest_format_version),
            "unsupported backup manifest format version"
        );
        ensure!(
            compatibility.vlog_format_version
                == compatibility
                    .value_separation_enabled
                    .then_some(crate::vlog::VLOG_FORMAT_VERSION),
            "invalid backup vLog compatibility metadata"
        );
    }
    if envelope.version < 2 {
        ensure!(
            envelope.objects.is_none(),
            "v1 generation envelope must not contain an object map"
        );
        return Ok(());
    }
    let objects = envelope
        .objects
        .as_ref()
        .ok_or_else(|| anyhow!("v2 generation envelope is missing object map"))?;
    if envelope.version >= 4
        && objects
            .iter()
            .any(|object| object.kind == RepositoryObjectKind::Vlog)
    {
        ensure!(
            envelope
                .compatibility
                .as_ref()
                .is_some_and(|compatibility| compatibility.value_separation_enabled),
            "generation contains vLog objects but compatibility disables value separation"
        );
    }
    let mut names = HashSet::new();
    let mut identities = HashSet::new();
    let mut previous_name: Option<&str> = None;
    for object in objects {
        ensure!(
            !object.source_path.is_empty() && !object.source_path.starts_with('/'),
            "invalid generation source path"
        );
        ensure!(
            !object
                .source_path
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."),
            "invalid generation source path"
        );
        ensure!(
            object.object_name
                == derived_object_name(object.kind, object.file_id, object.file_checksum),
            "generation object name is not derived from identity"
        );
        ensure!(
            previous_name.is_none_or(|previous| previous < object.object_name.as_str()),
            "generation objects are not in canonical order"
        );
        ensure!(
            names.insert(object.object_name.clone()),
            "duplicate generation object name"
        );
        ensure!(
            identities.insert((object.kind, object.file_id)),
            "duplicate generation object identity"
        );
        ensure!(
            object.file_size <= MAX_REPOSITORY_OBJECT_BYTES,
            "generation object exceeds size limit"
        );
        previous_name = Some(&object.object_name);
    }
    Ok(())
}

fn validate_restore_options(
    envelope: &GenerationEnvelope,
    options: &crate::lsm_storage::LsmStorageOptions,
) -> Result<()> {
    let value_separation_enabled = options
        .value_separation
        .as_ref()
        .is_some_and(|options| options.enabled);
    let Some(compatibility) = &envelope.compatibility else {
        let has_vlog_objects = envelope
            .objects
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|object| object.kind == RepositoryObjectKind::Vlog);
        ensure!(
            !has_vlog_objects || value_separation_enabled,
            "legacy restore with vLog objects requires value separation"
        );
        return Ok(());
    };
    ensure!(
        (3..=crate::manifest::MANIFEST_FORMAT_VERSION)
            .contains(&compatibility.manifest_format_version),
        "restore manifest format is incompatible"
    );
    ensure!(
        compatibility.value_separation_enabled == value_separation_enabled,
        "restore value-separation setting is incompatible"
    );
    ensure!(
        compatibility.vlog_format_version
            == value_separation_enabled.then_some(crate::vlog::VLOG_FORMAT_VERSION),
        "restore vLog format is incompatible"
    );
    Ok(())
}

fn validate_restore_snapshot_objects(envelope: &GenerationEnvelope, snapshot: &[u8]) -> Result<()> {
    let record: crate::manifest::ManifestRecord =
        serde_json::from_slice(snapshot).context("invalid restore manifest snapshot")?;
    let crate::manifest::ManifestRecord::Snapshot {
        immutable_file_metadata,
        format_version,
        ..
    } = record
    else {
        bail!("restore manifest must be a snapshot record");
    };
    ensure!(
        (3..=crate::manifest::MANIFEST_FORMAT_VERSION).contains(&format_version),
        "restore manifest snapshot format is unsupported"
    );
    if let Some(compatibility) = &envelope.compatibility {
        ensure!(
            compatibility.manifest_format_version == format_version,
            "generation compatibility does not match manifest snapshot format"
        );
    }
    let objects = envelope
        .objects
        .as_ref()
        .ok_or_else(|| anyhow!("restore requires a generation object map"))?;
    let expected: HashSet<_> = objects
        .iter()
        .map(|object| {
            (
                match object.kind {
                    RepositoryObjectKind::Sst => crate::manifest::ImmutableFileKind::Sst,
                    RepositoryObjectKind::Vlog => crate::manifest::ImmutableFileKind::Vlog,
                },
                object.file_id,
                object.file_size,
                object.file_checksum,
            )
        })
        .collect();
    let actual: HashSet<_> = immutable_file_metadata
        .iter()
        .map(|metadata| {
            (
                metadata.kind,
                metadata.file_id,
                metadata.file_size,
                metadata.file_checksum,
            )
        })
        .collect();
    ensure!(
        expected.len() == objects.len(),
        "restore generation object map contains duplicate identities"
    );
    ensure!(
        actual.len() == immutable_file_metadata.len(),
        "restore manifest contains duplicate immutable object identities"
    );
    ensure!(
        expected.len() == actual.len() && expected == actual,
        "restore manifest object identities do not match generation"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_generation_objects_on_disk(
    root: &OwnedFd,
    envelope: &GenerationEnvelope,
) -> Result<()> {
    let Some(objects) = envelope.objects.as_ref() else {
        return Ok(());
    };
    let files = openat_no_follow(root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    for object in objects {
        let file = File::from(openat_no_follow(
            &files,
            &object.object_name,
            libc::O_RDONLY,
            0,
        )?);
        ensure_regular_file(file.as_raw_fd())?;
        ensure!(
            file.metadata()?.len() == object.file_size,
            "repository object size mismatch"
        );
        let mut file = file;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let checksum: [u8; 32] = hasher.finalize().into();
        ensure!(
            checksum == object.file_checksum,
            "repository object checksum mismatch"
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_generation_object_metadata_on_disk(
    root: &OwnedFd,
    envelope: &GenerationEnvelope,
) -> Result<()> {
    let Some(objects) = envelope.objects.as_ref() else {
        return Ok(());
    };
    let files = openat_no_follow(root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    for object in objects {
        let file = File::from(openat_no_follow(
            &files,
            &object.object_name,
            libc::O_RDONLY,
            0,
        )?);
        ensure_regular_file(file.as_raw_fd())?;
        ensure!(
            file.metadata()?.len() == object.file_size,
            "repository object size mismatch"
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_generation_metadata(generation: &OwnedFd, name: &str) -> Result<Vec<u8>> {
    let fd = openat_no_follow(generation, name, libc::O_RDONLY, 0)?;
    ensure_regular_file(fd.as_raw_fd())?;
    let mut bytes = Vec::new();
    File::from(fd)
        .take((MAX_GENERATION_METADATA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_GENERATION_METADATA_BYTES,
        "backup generation metadata {name} exceeds limit"
    );
    Ok(bytes)
}

fn record_sequence(record: &CatalogRecord) -> u64 {
    match record {
        CatalogRecord::HighWater { sequence, .. }
        | CatalogRecord::Prepare { sequence, .. }
        | CatalogRecord::Commit { sequence, .. }
        | CatalogRecord::Retention { sequence, .. }
        | CatalogRecord::Snapshot { sequence, .. } => *sequence,
    }
}

#[cfg(target_os = "linux")]
fn catalog_generation_snapshot(
    generations: &OwnedFd,
    committed: &CommittedGeneration,
) -> Result<CatalogGenerationSnapshot> {
    let generation = openat_no_follow(
        generations,
        &committed.id.to_string(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )?;
    let generation_bytes = read_generation_metadata(&generation, "GENERATION")?;
    let envelope: GenerationEnvelope = serde_json::from_slice(&generation_bytes)?;
    let snapshot = read_generation_metadata(&generation, "MANIFEST_SNAPSHOT")?;
    let objects = envelope.objects.as_deref().unwrap_or_default();
    let logical_bytes = objects.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.file_size)
            .ok_or_else(|| anyhow!("backup logical byte count overflow"))
    })?;
    Ok(CatalogGenerationSnapshot {
        id: committed.id,
        parent_id: committed.parent_id,
        generation_checksum: committed.generation_checksum,
        manifest_snapshot_len: Some(snapshot.len() as u64),
        manifest_snapshot_checksum: Some(Sha256::digest(&snapshot).into()),
        created_at_secs: Some(envelope.created_at_secs),
        logical_bytes: Some(logical_bytes),
        new_object_bytes: Some(envelope.new_object_bytes),
        file_count: Some(objects.len() as u64),
    })
}
pub(crate) struct CatalogFrames {
    pub(crate) frames: Vec<CatalogFrame>,
    pub(crate) last_complete_offset: u64,
    pub(crate) torn_tail: bool,
}

pub(crate) struct CatalogFrame {
    pub(crate) record: CatalogRecord,
    /// Exact validated bytes from the catalog, used for Commit/Prepare binding.
    pub(crate) payload: Vec<u8>,
    pub(crate) start_offset: u64,
}

pub(crate) struct CatalogReplay {
    pub(crate) committed_ids: Vec<u64>,
    pub(crate) committed_generations: Vec<CommittedGeneration>,
    pub(crate) high_water_id: u64,
    pub(crate) retained_offset: u64,
    pub(crate) last_sequence: u64,
    pub(crate) abandoned_generation_id: Option<u64>,
}

/// Build the backup-specific captured file view without extending the
/// checkpoint lock's critical section with hashing I/O.
impl crate::lsm_storage::LsmStorageInner {
    pub(crate) fn prepare_backup_capture(
        &self,
    ) -> Result<crate::checkpoint::CheckpointCapture<'_>> {
        let mut capture = self.capture_checkpoint_state()?;
        let metadata = self.hash_immutable_file_metadata(&capture.sst_ids, &capture.vlog_ids)?;
        if let crate::manifest::ManifestRecord::Snapshot {
            immutable_file_metadata,
            ..
        } = &mut capture.snapshot_record
        {
            *immutable_file_metadata = metadata.clone();
        }
        capture.immutable_file_metadata = metadata;
        Ok(capture)
    }
}

pub(crate) struct CommittedGeneration {
    pub(crate) id: u64,
    pub(crate) parent_id: Option<u64>,
    pub(crate) generation_checksum: [u8; 32],
    pub(crate) snapshot_metadata: Option<CatalogGenerationSnapshot>,
}

#[cfg(target_os = "linux")]
pub(crate) struct RepositoryLock {
    _fd: OwnedFd,
}

#[cfg(target_os = "linux")]
impl RepositoryLock {
    fn duplicate(&self) -> Result<Self> {
        Ok(Self {
            _fd: self._fd.try_clone()?,
        })
    }

    pub(crate) fn acquire(parent: &OwnedFd, exclusive: bool) -> Result<Self> {
        let fd = openat_no_follow(parent, "LOCK", libc::O_RDWR, 0)?;
        ensure_regular_file(fd.as_raw_fd())?;
        let operation = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        };
        let result = loop {
            // SAFETY: fd is a valid open descriptor and flock does not retain
            // any borrowed pointers.
            let result = unsafe { libc::flock(fd.as_raw_fd(), operation) };
            if result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break result;
            }
        };
        ensure!(result == 0, "failed to acquire backup repository lock");
        Ok(Self { _fd: fd })
    }

    fn unlock(&self) -> Result<()> {
        // SAFETY: the descriptor remains open and owned by this lock.
        let result = unsafe { libc::flock(self._fd.as_raw_fd(), libc::LOCK_UN) };
        ensure!(result == 0, "failed to release backup repository lock");
        Ok(())
    }

    fn reacquire(&self) -> Result<()> {
        let result = loop {
            // SAFETY: the descriptor remains valid for the lifetime of self.
            let result = unsafe { libc::flock(self._fd.as_raw_fd(), libc::LOCK_EX) };
            if result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break result;
            }
        };
        ensure!(result == 0, "failed to reacquire backup repository lock");
        Ok(())
    }
}

#[cfg(target_os = "linux")]
struct RepositoryRelockGuard<'a> {
    lock: &'a RepositoryLock,
    usable: &'a AtomicBool,
    armed: bool,
}

#[cfg(target_os = "linux")]
struct UnwindInvalidationGuard<'a> {
    usable: &'a AtomicBool,
    armed: bool,
}

#[cfg(target_os = "linux")]
impl<'a> UnwindInvalidationGuard<'a> {
    fn new(usable: &'a AtomicBool) -> Self {
        Self {
            usable,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(target_os = "linux")]
impl Drop for UnwindInvalidationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.usable.store(false, Ordering::Release);
        }
    }
}

#[cfg(target_os = "linux")]
impl<'a> RepositoryRelockGuard<'a> {
    fn new(lock: &'a RepositoryLock, usable: &'a AtomicBool) -> Self {
        Self {
            lock,
            usable,
            armed: true,
        }
    }

    fn reacquire(mut self) -> Result<()> {
        let result = self.lock.reacquire();
        self.armed = false;
        if result.is_err() {
            self.usable.store(false, Ordering::Release);
        }
        result
    }
}

#[cfg(target_os = "linux")]
impl Drop for RepositoryRelockGuard<'_> {
    fn drop(&mut self) {
        if self.armed && self.lock.reacquire().is_err() {
            self.usable.store(false, Ordering::Release);
        }
    }
}

/// Open a repository directory without permitting a symlink at the final
/// component. Callers keep the descriptor and use `openat_no_follow` for all
/// children, so a later path replacement cannot redirect the operation.
#[cfg(target_os = "linux")]
pub(crate) fn open_directory_no_follow(path: &std::path::Path) -> Result<OwnedFd> {
    let start = if path.is_absolute() { "/" } else { "." };
    let start = CString::new(start).unwrap();
    // SAFETY: start is a static NUL-terminated path and the successful fd is
    // immediately transferred to OwnedFd.
    let fd = unsafe {
        libc::open(
            start.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    ensure!(
        fd >= 0,
        "failed to open trusted repository path root: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: `fd` is valid after the successful `open` above and ownership is
    // transferred exactly once into `OwnedFd`.
    let mut current = unsafe { OwnedFd::from_raw_fd(fd) };
    for component in path.components() {
        let std::path::Component::Normal(component) = component else {
            ensure!(
                matches!(
                    component,
                    std::path::Component::RootDir | std::path::Component::CurDir
                ),
                "repository path must not contain .. components"
            );
            continue;
        };
        let name = CString::new(component.as_bytes())?;
        // SAFETY: current is a live directory descriptor and name is a raw
        // Unix component encoded as a NUL-terminated C string.
        let fd = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to open no-follow repository path component");
        }
        // SAFETY: `fd` is valid after the successful openat above.
        current = unsafe { OwnedFd::from_raw_fd(fd) };
    }
    Ok(current)
}

#[cfg(target_os = "linux")]
pub(crate) fn openat_no_follow(
    parent: &OwnedFd,
    name: &str,
    flags: i32,
    mode: u32,
) -> Result<OwnedFd> {
    ensure!(
        !name.is_empty() && name != "." && name != ".." && !name.contains('/'),
        "repository component must be a single basename"
    );
    let name =
        CString::new(name).map_err(|_| anyhow!("repository component contains an interior NUL"))?;
    // SAFETY: parent is a live directory descriptor and name is NUL-terminated.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to open repository component without following symlinks");
    }
    // SAFETY: fd is valid because openat returned non-negative.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
pub(crate) fn mkdirat_no_follow(parent: &OwnedFd, name: &str, mode: u32) -> Result<OwnedFd> {
    ensure!(
        !name.is_empty() && name != "." && name != ".." && !name.contains('/'),
        "repository component must be a single basename"
    );
    let name =
        CString::new(name).map_err(|_| anyhow!("repository component contains an interior NUL"))?;
    // SAFETY: parent is a live directory descriptor and name is NUL-terminated.
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), mode) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        ensure!(
            error.kind() == std::io::ErrorKind::AlreadyExists,
            "failed to create repository directory: {error}"
        );
    }
    openat_no_follow(
        parent,
        name.to_str().unwrap(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )
}

#[cfg(target_os = "linux")]
pub(crate) fn fsync_fd(fd: &OwnedFd) -> Result<()> {
    // SAFETY: fd is a valid descriptor owned by the caller.
    let result = loop {
        let result = unsafe { libc::fsync(fd.as_raw_fd()) };
        if result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break result;
        }
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn copy_immutable_object(
    source_dir: &OwnedFd,
    source_name: &str,
    target_dir: &OwnedFd,
    target_name: &str,
    expected_size: u64,
    expected_checksum: [u8; 32],
) -> Result<(u64, [u8; 32])> {
    let source = File::from(openat_no_follow(
        source_dir,
        source_name,
        libc::O_RDONLY,
        0,
    )?);
    copy_immutable_object_from_file(
        source,
        target_dir,
        target_name,
        expected_size,
        expected_checksum,
    )
}

#[cfg(target_os = "linux")]
fn copy_immutable_object_from_file(
    source: File,
    target_dir: &OwnedFd,
    target_name: &str,
    expected_size: u64,
    expected_checksum: [u8; 32],
) -> Result<(u64, [u8; 32])> {
    ensure!(
        !target_name.is_empty()
            && target_name != "."
            && target_name != ".."
            && !target_name.contains('/'),
        "repository object name must be a single basename"
    );
    ensure_regular_file(source.as_raw_fd())?;
    let source_size = source.metadata()?.len();
    let temp_name = format!(
        ".{target_name}.tmp-{}-{}",
        std::process::id(),
        OBJECT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut target = File::from(openat_no_follow(
        target_dir,
        &temp_name,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )?);
    let mut cleanup = TempObjectCleanup {
        directory: target_dir,
        name: temp_name.clone(),
    };
    let mut source = source;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let next_bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("immutable object size overflow"))?;
        ensure!(
            next_bytes <= MAX_REPOSITORY_OBJECT_BYTES,
            "repository object exceeds size limit"
        );
        target.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        bytes = next_bytes;
    }
    target.sync_all()?;
    ensure!(
        bytes == source_size,
        "immutable source changed while copying"
    );
    ensure!(
        bytes == expected_size,
        "copied object size does not match captured identity"
    );
    let copied_checksum: [u8; 32] = hasher.clone().finalize().into();
    ensure!(
        copied_checksum == expected_checksum,
        "immutable source checksum mismatch"
    );
    let from = CString::new(temp_name)?;
    let to = CString::new(target_name)?;
    // SAFETY: both descriptors are trusted directories and names are single
    // components; no-replace prevents overwriting a prior immutable object.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            target_dir.as_raw_fd(),
            from.as_ptr(),
            target_dir.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    ensure!(
        result == 0,
        "failed to publish repository object: {}",
        std::io::Error::last_os_error()
    );
    if let Err(error) = fsync_fd(target_dir) {
        let final_name = CString::new(target_name)?;
        // SAFETY: target_dir is trusted, target_name is validated, and this
        // entry was created by the immediately preceding no-replace rename.
        unsafe {
            libc::unlinkat(target_dir.as_raw_fd(), final_name.as_ptr(), 0);
        }
        let _ = fsync_fd(target_dir);
        return Err(error);
    }
    cleanup.disarm();
    Ok((bytes, hasher.finalize().into()))
}

#[cfg(target_os = "linux")]
pub(crate) fn reuse_matching_object(
    target_dir: &OwnedFd,
    target_name: &str,
    expected_size: u64,
    expected_checksum: [u8; 32],
) -> Result<bool> {
    ensure!(
        expected_size <= MAX_REPOSITORY_OBJECT_BYTES,
        "repository object exceeds size limit"
    );
    let file = File::from(openat_no_follow(
        target_dir,
        target_name,
        libc::O_RDONLY,
        0,
    )?);
    ensure_regular_file(file.as_raw_fd())?;
    ensure!(
        file.metadata()?.len() == expected_size,
        "repository object size mismatch"
    );
    let mut file = file;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    ensure!(
        hasher.finalize().as_slice() == expected_checksum,
        "repository object checksum mismatch"
    );
    fsync_fd(target_dir)?;
    Ok(true)
}

#[cfg(target_os = "linux")]
pub(crate) fn copy_or_reuse_object(
    source_dir: &OwnedFd,
    source_name: &str,
    target_dir: &OwnedFd,
    target_name: &str,
    expected_size: u64,
    expected_checksum: [u8; 32],
    use_hard_links: bool,
) -> Result<bool> {
    match reuse_matching_object(target_dir, target_name, expected_size, expected_checksum) {
        Ok(reused) => Ok(reused),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            if use_hard_links {
                match hard_link_immutable_object(
                    source_dir,
                    source_name,
                    target_dir,
                    target_name,
                    expected_size,
                    expected_checksum,
                ) {
                    Ok(()) => return Ok(false),
                    Err(error)
                        if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                            matches!(
                                error.raw_os_error(),
                                Some(
                                    libc::EXDEV
                                        | libc::EPERM
                                        | libc::EACCES
                                        | libc::EINVAL
                                        | libc::ENOTSUP,
                                )
                            )
                        }) => {}
                    Err(error) => return Err(error),
                }
            }
            let (size, checksum) = copy_immutable_object(
                source_dir,
                source_name,
                target_dir,
                target_name,
                expected_size,
                expected_checksum,
            )?;
            ensure!(
                size == expected_size && checksum == expected_checksum,
                "copied repository object identity mismatch"
            );
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn hard_link_immutable_object(
    source_dir: &OwnedFd,
    source_name: &str,
    target_dir: &OwnedFd,
    target_name: &str,
    expected_size: u64,
    expected_checksum: [u8; 32],
) -> Result<()> {
    let source_path = CString::new(source_name)?;
    // SAFETY: source_dir is trusted and source_name is a validated basename.
    let link_fd = unsafe {
        libc::openat(
            source_dir.as_raw_fd(),
            source_path.as_ptr(),
            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if link_fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: link_fd was returned by openat and is uniquely owned here.
    let link_fd = unsafe { OwnedFd::from_raw_fd(link_fd) };
    ensure_regular_file(link_fd.as_raw_fd())?;
    let proc_path = format!("/proc/self/fd/{}", link_fd.as_raw_fd());
    let source = File::open(proc_path)?;
    ensure!(
        source.metadata()?.len() == expected_size,
        "immutable source size mismatch"
    );
    let mut source_for_hash = source;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source_for_hash.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let checksum: [u8; 32] = hasher.finalize().into();
    ensure!(
        checksum == expected_checksum,
        "immutable source checksum mismatch"
    );
    ensure!(
        expected_size <= MAX_REPOSITORY_OBJECT_BYTES,
        "repository object exceeds size limit"
    );
    let temp_name = format!(
        ".{target_name}.tmp-{}-{}",
        std::process::id(),
        OBJECT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let temp_c = CString::new(temp_name.as_str())?;
    let empty_source = CString::new("")?;
    // SAFETY: both descriptors are trusted directories and names are validated basenames.
    let result = unsafe {
        libc::linkat(
            link_fd.as_raw_fd(),
            empty_source.as_ptr(),
            target_dir.as_raw_fd(),
            temp_c.as_ptr(),
            libc::AT_EMPTY_PATH,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut cleanup = TempObjectCleanup {
        directory: target_dir,
        name: temp_name.clone(),
    };
    let linked = File::from(openat_no_follow(target_dir, &temp_name, libc::O_RDONLY, 0)?);
    ensure_regular_file(linked.as_raw_fd())?;
    ensure!(
        linked.metadata()?.len() == expected_size,
        "hard-linked object size mismatch"
    );
    let from = CString::new(temp_name)?;
    let to = CString::new(target_name)?;
    // SAFETY: trusted directory descriptors and validated names; no-replace avoids overwrite.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            target_dir.as_raw_fd(),
            from.as_ptr(),
            target_dir.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    ensure!(
        result == 0,
        "failed to publish hard-linked object: {}",
        std::io::Error::last_os_error()
    );
    if let Err(error) = fsync_fd(target_dir) {
        let final_name = CString::new(target_name)?;
        // SAFETY: target_dir is trusted and target_name is validated; this
        // entry was created by the immediately preceding no-replace rename.
        unsafe {
            libc::unlinkat(target_dir.as_raw_fd(), final_name.as_ptr(), 0);
        }
        let _ = fsync_fd(target_dir);
        return Err(error);
    }
    cleanup.disarm();
    Ok(())
}

#[cfg(target_os = "linux")]
struct TempObjectCleanup<'a> {
    directory: &'a OwnedFd,
    name: String,
}

#[cfg(target_os = "linux")]
impl TempObjectCleanup<'_> {
    fn disarm(&mut self) {
        self.name.clear();
    }
}

#[cfg(target_os = "linux")]
impl Drop for TempObjectCleanup<'_> {
    fn drop(&mut self) {
        if self.name.is_empty() {
            return;
        }
        if let Ok(name) = CString::new(self.name.as_str()) {
            // SAFETY: directory is trusted and name is the exact generated
            // temporary basename.
            unsafe {
                libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0);
            }
            let _ = fsync_fd(self.directory);
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn bootstrap_repository(parent: &OwnedFd, name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name != "." && name != ".." && !name.contains('/'),
        "repository name must be a basename"
    );
    let init_name = format!(".{name}.incremental-backup.init.lock");
    let init_fd = openat_no_follow(parent, &init_name, libc::O_RDWR | libc::O_CREAT, 0o600)?;
    ensure_regular_file(init_fd.as_raw_fd())?;
    let lock_result = loop {
        // SAFETY: init_fd is a valid regular-file descriptor.
        let result = unsafe { libc::flock(init_fd.as_raw_fd(), libc::LOCK_EX) };
        if result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break result;
        }
    };
    ensure!(
        lock_result == 0,
        "failed to acquire backup initialization lock: {}",
        std::io::Error::last_os_error()
    );
    fsync_fd(&init_fd)?;
    fsync_fd(parent)?;
    let attempt = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let staging = format!(
        ".{name}.incremental-backup-{}-{attempt}.staging",
        std::process::id()
    );
    let staging_fd = mkdirat_exclusive(parent, &staging, 0o700)?;
    let mut cleanup = BootstrapStagingCleanup {
        parent,
        name: staging.clone(),
    };
    let files_fd = mkdirat_no_follow(&staging_fd, "files", 0o700)?;
    let generations_fd = mkdirat_no_follow(&staging_fd, "generations", 0o700)?;
    fsync_fd(&files_fd)?;
    fsync_fd(&generations_fd)?;
    let lock = openat_no_follow(
        &staging_fd,
        "LOCK",
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )?;
    fsync_fd(&lock)?;
    let catalog = openat_no_follow(
        &staging_fd,
        "BACKUP_MANIFEST",
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )?;
    fsync_fd(&catalog)?;
    fsync_fd(&staging_fd)?;
    let source = CString::new(staging.as_str())?;
    let target = CString::new(name)?;
    // SAFETY: both descriptors are valid directories and both names are
    // validated single path components.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            source.as_ptr(),
            parent.as_raw_fd(),
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    cleanup.disarm();
    fsync_fd(parent)
        .map_err(|source| anyhow::Error::new(RepositoryBootstrapPublicationError { source }))
}

#[cfg(target_os = "linux")]
struct BootstrapStagingCleanup<'a> {
    parent: &'a OwnedFd,
    name: String,
}

#[cfg(target_os = "linux")]
impl BootstrapStagingCleanup<'_> {
    fn disarm(&mut self) {
        self.name.clear();
    }
}

#[cfg(target_os = "linux")]
impl Drop for BootstrapStagingCleanup<'_> {
    fn drop(&mut self) {
        if self.name.is_empty() {
            return;
        }
        let Ok(staging) = openat_no_follow(
            self.parent,
            &self.name,
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        ) else {
            return;
        };
        for name in ["LOCK", "BACKUP_MANIFEST"] {
            let name = CString::new(name).unwrap();
            unsafe {
                libc::unlinkat(staging.as_raw_fd(), name.as_ptr(), 0);
            }
        }
        for name in ["files", "generations"] {
            let name = CString::new(name).unwrap();
            unsafe {
                libc::unlinkat(staging.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR);
            }
        }
        let name = CString::new(self.name.as_str()).unwrap();
        unsafe {
            libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR);
        }
    }
}

#[cfg(target_os = "linux")]
fn mkdirat_exclusive(parent: &OwnedFd, name: &str, mode: u32) -> Result<OwnedFd> {
    let name = CString::new(name)?;
    // SAFETY: parent is a valid directory descriptor and name is NUL-terminated.
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), mode) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    openat_no_follow(
        parent,
        name.to_str()?,
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    version: u8,
    record: CatalogRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CatalogRecord {
    HighWater {
        sequence: u64,
        allocated_id: u64,
    },
    Prepare {
        sequence: u64,
        id: u64,
        parent_id: Option<u64>,
        generation_checksum: [u8; 32],
    },
    Commit {
        sequence: u64,
        id: u64,
        prepare_sequence: u64,
        prepare_digest: [u8; 32],
    },
    Retention {
        sequence: u64,
        retained_ids: Vec<u64>,
    },
    Snapshot {
        sequence: u64,
        #[serde(default, skip_serializing_if = "is_zero_digest")]
        base_catalog_digest: [u8; 32],
        high_water_id: u64,
        committed_generations: Vec<CatalogGenerationSnapshot>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogGenerationSnapshot {
    pub(crate) id: u64,
    pub(crate) parent_id: Option<u64>,
    pub(crate) generation_checksum: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) manifest_snapshot_len: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) manifest_snapshot_checksum: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) created_at_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) logical_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) new_object_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) file_count: Option<u64>,
}

pub(crate) fn append_catalog_record(file: &mut impl Write, record: &CatalogRecord) -> Result<()> {
    let payload = encode_catalog_payload(record)?;
    ensure!(
        payload.len() <= MAX_CATALOG_FRAME_BYTES,
        "backup catalog record exceeds frame limit"
    );
    let length =
        u32::try_from(payload.len()).map_err(|_| anyhow!("backup catalog record too large"))?;
    let checksum = crc32(&payload);
    let mut header = [0_u8; CATALOG_FRAME_HEADER_BYTES];
    header[..4].copy_from_slice(&length.to_le_bytes());
    header[4..8].copy_from_slice(&checksum.to_le_bytes());
    let header_checksum = crc32(&header[..8]);
    header[8..].copy_from_slice(&header_checksum.to_le_bytes());
    file.write_all(&header)?;
    file.write_all(&payload)?;
    Ok(())
}

fn encode_catalog_payload(record: &CatalogRecord) -> Result<Vec<u8>> {
    serde_json::to_vec(&WireRecord {
        version: CATALOG_FORMAT_VERSION,
        record: record.clone(),
    })
    .context("failed to encode backup catalog record")
}

pub(crate) fn prepare_payload_digest(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

/// Reads bounded, valid complete frames. A torn tail retains the exact byte
/// offset to which a lock-holding recovery path may truncate.
pub(crate) fn read_catalog_records(mut file: impl Read) -> Result<CatalogFrames> {
    let mut bytes = Vec::new();
    file.by_ref()
        .take((MAX_CATALOG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_CATALOG_BYTES,
        "backup catalog exceeds size limit"
    );
    let mut frames = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if bytes.len() - offset < CATALOG_FRAME_HEADER_BYTES {
            return Ok(CatalogFrames {
                frames,
                last_complete_offset: offset as u64,
                torn_tail: true,
            });
        }
        let header = &bytes[offset..offset + CATALOG_FRAME_HEADER_BYTES];
        ensure!(
            crc32(&header[..8]) == u32::from_le_bytes(header[8..].try_into().unwrap()),
            "backup catalog frame header checksum mismatch"
        );
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        ensure!(
            length <= MAX_CATALOG_FRAME_BYTES,
            "backup catalog frame exceeds limit"
        );
        ensure!(
            frames.len() < MAX_CATALOG_RECORDS,
            "backup catalog has too many records"
        );
        let expected_checksum = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let end = offset
            .checked_add(CATALOG_FRAME_HEADER_BYTES + length)
            .ok_or_else(|| anyhow!("backup catalog frame overflow"))?;
        if end > bytes.len() {
            return Ok(CatalogFrames {
                frames,
                last_complete_offset: offset as u64,
                torn_tail: true,
            });
        }
        let payload = &bytes[offset + CATALOG_FRAME_HEADER_BYTES..end];
        ensure!(
            crc32(payload) == expected_checksum,
            "backup catalog frame checksum mismatch"
        );
        let wire: WireRecord =
            serde_json::from_slice(payload).context("invalid backup catalog record")?;
        ensure!(
            wire.version == CATALOG_FORMAT_VERSION,
            "unsupported backup catalog format version"
        );
        ensure!(
            payload == encode_catalog_payload(&wire.record)?,
            "backup catalog record is not canonically encoded"
        );
        frames.push(CatalogFrame {
            record: wire.record,
            payload: payload.to_vec(),
            start_offset: offset as u64,
        });
        offset = end;
    }
    Ok(CatalogFrames {
        frames,
        last_complete_offset: offset as u64,
        torn_tail: false,
    })
}

pub(crate) fn replay_catalog(frames: &CatalogFrames) -> Result<CatalogReplay> {
    let mut high_water_id = 0_u64;
    let mut committed_ids = Vec::new();
    let mut committed_generations = Vec::new();
    let mut seen_ids = HashSet::new();
    let mut pending: Option<(&CatalogFrame, Option<&CatalogFrame>)> = None;
    let mut previous_sequence = 0_u64;

    for (index, frame) in frames.frames.iter().enumerate() {
        let sequence = match &frame.record {
            CatalogRecord::HighWater { sequence, .. }
            | CatalogRecord::Prepare { sequence, .. }
            | CatalogRecord::Commit { sequence, .. }
            | CatalogRecord::Retention { sequence, .. }
            | CatalogRecord::Snapshot { sequence, .. } => sequence,
        };
        let expected_sequence = if index == 0 {
            match &frame.record {
                CatalogRecord::Snapshot { .. } => *sequence,
                _ => 1,
            }
        } else {
            previous_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("backup catalog sequence space is exhausted"))?
        };
        ensure!(
            *sequence == expected_sequence,
            "backup catalog sequence is not strictly monotonic"
        );
        previous_sequence = *sequence;
        match &frame.record {
            CatalogRecord::HighWater { allocated_id, .. } => {
                ensure!(
                    !matches!(pending, Some((_, Some(_)))),
                    "backup catalog transaction is incomplete"
                );
                let next_id = high_water_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("backup catalog id space is exhausted"))?;
                ensure!(
                    *allocated_id == next_id,
                    "backup catalog high-water allocation is invalid"
                );
                high_water_id = *allocated_id;
                pending = Some((frame, None));
            }
            CatalogRecord::Prepare { id, parent_id, .. } => {
                let Some((high_water, None)) = pending else {
                    bail!("backup Prepare is not adjacent to HighWater")
                };
                let CatalogRecord::HighWater { allocated_id, .. } = high_water.record else {
                    unreachable!()
                };
                ensure!(
                    *id == allocated_id,
                    "backup Prepare id does not match HighWater"
                );
                ensure!(
                    *parent_id == committed_ids.last().copied(),
                    "backup Prepare parent is invalid"
                );
                pending = Some((high_water, Some(frame)));
            }
            CatalogRecord::Commit {
                id,
                prepare_sequence,
                prepare_digest,
                ..
            } => {
                let Some((_, Some(prepare))) = pending else {
                    bail!("backup Commit has no adjacent Prepare")
                };
                let CatalogRecord::Prepare {
                    sequence,
                    id: prepare_id,
                    parent_id,
                    generation_checksum,
                    ..
                } = prepare.record
                else {
                    unreachable!()
                };
                ensure!(
                    *id == prepare_id && *prepare_sequence == sequence,
                    "backup Commit does not bind Prepare"
                );
                ensure!(
                    *prepare_digest == prepare_payload_digest(&prepare.payload),
                    "backup Commit digest mismatch"
                );
                ensure!(
                    seen_ids.insert(*id),
                    "backup catalog reuses a generation id"
                );
                committed_ids.push(*id);
                committed_generations.push(CommittedGeneration {
                    id: *id,
                    parent_id,
                    generation_checksum,
                    snapshot_metadata: None,
                });
                pending = None;
            }
            CatalogRecord::Retention { retained_ids, .. } => {
                ensure!(
                    !retained_ids.is_empty(),
                    "backup retention set must not be empty"
                );
                let retained_set = retained_ids.iter().copied().collect::<HashSet<_>>();
                let committed_set = committed_ids.iter().copied().collect::<HashSet<_>>();
                let mut previous = None;
                for retained_id in retained_ids {
                    ensure!(
                        previous.is_none_or(|previous| previous < *retained_id),
                        "backup retention IDs are not strictly ordered"
                    );
                    ensure!(
                        committed_set.contains(retained_id),
                        "backup retention references an uncommitted generation"
                    );
                    previous = Some(*retained_id);
                }
                ensure!(
                    pending.is_none(),
                    "backup retention interrupts a transaction"
                );
                committed_ids.retain(|id| retained_set.contains(id));
                committed_generations.retain(|generation| retained_set.contains(&generation.id));
            }
            CatalogRecord::Snapshot {
                high_water_id: snapshot_high_water,
                committed_generations: snapshot_generations,
                ..
            } => {
                ensure!(
                    index == 0 && *sequence > 0,
                    "backup catalog snapshot must be a nonzero replay base"
                );
                ensure!(
                    pending.is_none(),
                    "backup snapshot interrupts a transaction"
                );
                ensure!(
                    *snapshot_high_water >= high_water_id,
                    "backup snapshot high-water regresses"
                );
                let mut previous_id = None;
                for generation in snapshot_generations {
                    ensure!(
                        generation.id > 0,
                        "backup snapshot contains an invalid generation ID"
                    );
                    ensure!(
                        generation.id <= *snapshot_high_water,
                        "backup snapshot generation exceeds high-water"
                    );
                    ensure!(
                        previous_id.is_none_or(|previous| previous < generation.id),
                        "backup snapshot generations are not strictly ordered"
                    );
                    ensure!(
                        generation
                            .parent_id
                            .is_none_or(|parent| parent < generation.id),
                        "backup snapshot parent chain is invalid"
                    );
                    previous_id = Some(generation.id);
                }
                high_water_id = *snapshot_high_water;
                committed_ids = snapshot_generations
                    .iter()
                    .map(|generation| generation.id)
                    .collect();
                committed_generations = snapshot_generations
                    .iter()
                    .map(|generation| CommittedGeneration {
                        id: generation.id,
                        parent_id: generation.parent_id,
                        generation_checksum: generation.generation_checksum,
                        snapshot_metadata: Some(generation.clone()),
                    })
                    .collect();
            }
        }
    }
    let abandoned_generation_id = pending
        .as_ref()
        .and_then(|(_, prepare)| prepare.as_ref())
        .and_then(|frame| match &frame.record {
            CatalogRecord::Prepare { id, .. } => Some(id),
            _ => None,
        })
        .copied();
    let (retained_offset, retained_sequence) = match pending {
        Some((high_water, Some(_))) => {
            let CatalogRecord::HighWater { sequence, .. } = high_water.record else {
                unreachable!()
            };
            (high_water.start_offset + frame_len(high_water)?, sequence)
        }
        _ => (frames.last_complete_offset, previous_sequence),
    };
    Ok(CatalogReplay {
        committed_ids,
        committed_generations,
        high_water_id,
        retained_offset,
        last_sequence: retained_sequence,
        abandoned_generation_id,
    })
}

#[cfg(target_os = "linux")]
fn remove_generation_orphan(generations: &OwnedFd, id: u64) -> Result<()> {
    let generation = match openat_no_follow(
        generations,
        &id.to_string(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    ) {
        Ok(fd) => fd,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    for name in ["GENERATION", "MANIFEST_SNAPSHOT"] {
        let name = CString::new(name).unwrap();
        // SAFETY: descriptor and basename are validated, and unlinkat removes
        // only the named regular child.
        let result = unsafe { libc::unlinkat(generation.as_raw_fd(), name.as_ptr(), 0) };
        ensure!(
            result == 0 || std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound,
            "failed to remove orphan metadata"
        );
    }
    let name = CString::new(id.to_string()).unwrap();
    // SAFETY: generations is a trusted directory descriptor and name is a basename.
    let result =
        unsafe { libc::unlinkat(generations.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
    ensure!(
        result == 0 || std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound,
        "failed to remove orphan generation"
    );
    Ok(())
}

fn frame_len(frame: &CatalogFrame) -> Result<u64> {
    Ok(u64::try_from(
        CATALOG_FRAME_HEADER_BYTES + frame.payload.len(),
    )?)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed(outcome: CreateBackupOutcome) -> BackupInfo {
        let CreateBackupOutcome::Committed(info) = outcome else {
            panic!("expected committed backup outcome");
        };
        info
    }

    fn committed_async(outcome: BackupOutcome) -> BackupInfo {
        let BackupOutcome::Committed(info) = outcome else {
            panic!("expected committed backup outcome");
        };
        info
    }

    #[cfg(target_os = "linux")]
    static COMMIT_DECISION_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    #[cfg(feature = "chaos-testing")]
    #[test]
    fn catalog_round_trip_and_torn_tail() {
        let first = CatalogRecord::HighWater {
            sequence: 1,
            allocated_id: 1,
        };
        let second = CatalogRecord::Prepare {
            sequence: 2,
            id: 1,
            parent_id: None,
            generation_checksum: [7; 32],
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &first).unwrap();
        append_catalog_record(&mut bytes, &second).unwrap();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        assert_eq!(
            frames
                .frames
                .iter()
                .map(|frame| &frame.record)
                .collect::<Vec<_>>(),
            vec![&first, &second]
        );
        assert!(!frames.torn_tail);
        assert_eq!(frames.last_complete_offset as usize, bytes.len());

        bytes.pop();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        assert_eq!(
            frames
                .frames
                .iter()
                .map(|frame| &frame.record)
                .collect::<Vec<_>>(),
            vec![&first]
        );
        assert!(frames.torn_tail);
    }

    #[test]
    fn catalog_rejects_checksum_mismatch() {
        let mut bytes = Vec::new();
        append_catalog_record(
            &mut bytes,
            &CatalogRecord::HighWater {
                sequence: 1,
                allocated_id: 1,
            },
        )
        .unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(read_catalog_records(bytes.as_slice()).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn legacy_catalog_snapshot_opens_restores_and_recompacts() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let generation_checksum = repository.replay.committed_generations[0].generation_checksum;
        drop(repository);
        let checksum_json = serde_json::to_string(&generation_checksum).unwrap();
        let payload = format!(
            r#"{{"version":1,"record":{{"type":"snapshot","sequence":4,"high_water_id":1,"committed_generations":[{{"id":1,"parent_id":null,"generation_checksum":{checksum_json}}}]}}}}"#
        )
        .into_bytes();
        assert!(
            !payload
                .windows(19)
                .any(|window| window == b"base_catalog_digest")
        );
        assert!(
            !payload
                .windows(21)
                .any(|window| window == b"manifest_snapshot_len")
        );
        let mut frame = vec![0_u8; CATALOG_FRAME_HEADER_BYTES];
        frame[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        frame[4..8].copy_from_slice(&crc32(&payload).to_le_bytes());
        let header_checksum = crc32(&frame[..8]);
        frame[8..].copy_from_slice(&header_checksum.to_le_bytes());
        frame.extend_from_slice(&payload);
        std::fs::write(dir.path().join("repository/BACKUP_MANIFEST"), frame).unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![1]);
        assert!(matches!(
            repository
                .restore(
                    1,
                    dir.path().join("restored-legacy-catalog"),
                    crate::lsm_storage::LsmStorageOptions::default_for_test(),
                )
                .unwrap(),
            RestoreOutcome::Restored
        ));
        drop(repository);

        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository.compact().unwrap();
        drop(repository);
        assert_eq!(
            BackupRepository::open(dir.path().join("repository"))
                .unwrap()
                .list_ids()
                .unwrap(),
            vec![1]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn modern_catalog_snapshot_rejects_missing_generation_metadata() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository.compact().unwrap();
        drop(repository);
        let catalog_path = dir.path().join("repository/BACKUP_MANIFEST");
        let bytes = std::fs::read(&catalog_path).unwrap();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        let mut record = frames.frames.into_iter().next().unwrap().record;
        let CatalogRecord::Snapshot {
            base_catalog_digest,
            committed_generations,
            ..
        } = &mut record
        else {
            panic!("compacted catalog did not contain a snapshot");
        };
        assert_ne!(*base_catalog_digest, [0; 32]);
        let generation = &mut committed_generations[0];
        generation.manifest_snapshot_len = None;
        generation.manifest_snapshot_checksum = None;
        generation.created_at_secs = None;
        generation.logical_bytes = None;
        generation.new_object_bytes = None;
        generation.file_count = None;
        let mut catalog = std::fs::File::create(catalog_path).unwrap();
        append_catalog_record(&mut catalog, &record).unwrap();
        catalog.sync_all().unwrap();

        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn compact_rejects_corrupt_retained_snapshot_before_replacing_primary() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let catalog_path = dir.path().join("repository/BACKUP_MANIFEST");
        let primary_before = std::fs::read(&catalog_path).unwrap();
        let snapshot_path = dir
            .path()
            .join("repository/generations/1/MANIFEST_SNAPSHOT");
        let snapshot_before = std::fs::read(&snapshot_path).unwrap();
        let mut corrupt_snapshot = snapshot_before.clone();
        corrupt_snapshot[0] ^= 1;
        std::fs::write(&snapshot_path, corrupt_snapshot).unwrap();

        assert!(repository.compact().is_err());
        assert_eq!(std::fs::read(&catalog_path).unwrap(), primary_before);
        assert!(
            !dir.path()
                .join("repository/BACKUP_MANIFEST.purge.tmp")
                .exists()
        );
        std::fs::write(snapshot_path, snapshot_before).unwrap();
        drop(repository);

        assert_eq!(
            BackupRepository::open(dir.path().join("repository"))
                .unwrap()
                .list_ids()
                .unwrap(),
            vec![1]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn legacy_v1_generation_opens_and_recompacts() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let generation_path = dir.path().join("repository/generations/1/GENERATION");
        let snapshot_path = dir
            .path()
            .join("repository/generations/1/MANIFEST_SNAPSHOT");
        let snapshot = std::fs::read(snapshot_path).unwrap();
        let snapshot_checksum: [u8; 32] = Sha256::digest(&snapshot).into();
        let checksum_json = serde_json::to_string(&snapshot_checksum).unwrap();
        let generation = format!(
            r#"{{"version":1,"id":1,"created_at_secs":7,"parent_id":null,"snapshot_len":{},"snapshot_checksum":{checksum_json},"body":[]}}"#,
            snapshot.len()
        )
        .into_bytes();
        std::fs::write(generation_path, &generation).unwrap();
        let generation_checksum: [u8; 32] = Sha256::digest(&generation).into();
        let record = CatalogRecord::Snapshot {
            sequence: 4,
            base_catalog_digest: [0; 32],
            high_water_id: 1,
            committed_generations: vec![CatalogGenerationSnapshot {
                id: 1,
                parent_id: None,
                generation_checksum,
                manifest_snapshot_len: None,
                manifest_snapshot_checksum: None,
                created_at_secs: None,
                logical_bytes: None,
                new_object_bytes: None,
                file_count: None,
            }],
        };
        let mut catalog =
            std::fs::File::create(dir.path().join("repository/BACKUP_MANIFEST")).unwrap();
        append_catalog_record(&mut catalog, &record).unwrap();
        catalog.sync_all().unwrap();

        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![1]);
        assert!(
            repository
                .restore(
                    1,
                    dir.path().join("legacy-v1-restore"),
                    crate::lsm_storage::LsmStorageOptions::default_for_test(),
                )
                .is_err()
        );
        repository.compact().unwrap();
        drop(repository);
        assert_eq!(
            BackupRepository::open(dir.path().join("repository"))
                .unwrap()
                .list_ids()
                .unwrap(),
            vec![1]
        );
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn compact_catalog_temp_sync_failpoint_recovers() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        failpoint::cfg("backup.compact.after_temp_sync", "panic").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.compact().unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.compact.after_temp_sync", "off").unwrap();
        let error = repository.compact().unwrap_err();
        assert!(error.to_string().contains("invalidated"));
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert!(reopened.list().unwrap().is_empty());
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn compact_catalog_after_replace_failpoint_reopens() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        failpoint::cfg("backup.compact.after_manifest_replace", "panic").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.compact().unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.compact.after_manifest_replace", "off").unwrap();
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert!(reopened.list().unwrap().is_empty());
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn compact_catalog_corrupt_successor_fails_reopen() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.compact.after_temp_sync", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.compact().unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.compact.after_temp_sync", "off").unwrap();
        let error = repository.purge(1).unwrap_err();
        assert!(error.to_string().contains("invalidated"));
        drop(repository);
        std::fs::write(
            dir.path().join("repository/BACKUP_MANIFEST.purge.tmp"),
            b"corrupt-successor",
        )
        .unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
        scenario.teardown();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn purge_successor_discards_empty_or_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let repository_path = dir.path().join("repository");
        let successor_path = repository_path.join("BACKUP_MANIFEST.purge.tmp");
        std::fs::File::create(&successor_path).unwrap();
        assert!(BackupRepository::open(&repository_path).is_ok());
        assert!(!successor_path.exists());

        std::fs::write(&successor_path, [0_u8]).unwrap();
        assert!(BackupRepository::open(&repository_path).is_ok());
        assert!(!successor_path.exists());
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_successor_with_invalid_or_missing_metadata_preserves_primary() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.compact.after_temp_sync", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.compact.after_temp_sync", "off").unwrap();
        let error = repository.purge(1).unwrap_err();
        assert!(error.to_string().contains("invalidated"));
        drop(repository);

        let primary_path = dir.path().join("repository/BACKUP_MANIFEST");
        let primary_before = std::fs::read(&primary_path).unwrap();
        let successor_path = dir.path().join("repository/BACKUP_MANIFEST.purge.tmp");
        let successor_bytes = std::fs::read(&successor_path).unwrap();
        let frames = read_catalog_records(successor_bytes.as_slice()).unwrap();
        let mut record = frames.frames.into_iter().next().unwrap().record;
        let CatalogRecord::Snapshot {
            committed_generations,
            ..
        } = &mut record
        else {
            panic!("purge successor is not a snapshot");
        };
        committed_generations[0].manifest_snapshot_checksum = Some([9; 32]);
        let mut successor = std::fs::File::create(&successor_path).unwrap();
        append_catalog_record(&mut successor, &record).unwrap();
        successor.sync_all().unwrap();

        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
        assert_eq!(std::fs::read(primary_path).unwrap(), primary_before);
        assert!(dir.path().join("repository/generations/2").exists());

        let CatalogRecord::Snapshot {
            committed_generations,
            ..
        } = &mut record
        else {
            unreachable!();
        };
        let generation = &mut committed_generations[0];
        generation.manifest_snapshot_len = None;
        generation.manifest_snapshot_checksum = None;
        generation.created_at_secs = None;
        generation.logical_bytes = None;
        generation.new_object_bytes = None;
        generation.file_count = None;
        let mut successor = std::fs::File::create(&successor_path).unwrap();
        append_catalog_record(&mut successor, &record).unwrap();
        successor.sync_all().unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
        assert_eq!(
            std::fs::read(dir.path().join("repository/BACKUP_MANIFEST")).unwrap(),
            primary_before
        );
        assert!(dir.path().join("repository/generations/2").exists());
        scenario.teardown();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn purge_successor_rejects_wrong_base_digest() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        drop(repository);
        let successor_path = dir.path().join("repository/BACKUP_MANIFEST.purge.tmp");
        let mut successor = std::fs::File::create(successor_path).unwrap();
        append_catalog_record(
            &mut successor,
            &CatalogRecord::Snapshot {
                sequence: 1,
                base_catalog_digest: [9; 32],
                high_water_id: 0,
                committed_generations: Vec::new(),
            },
        )
        .unwrap();
        successor.sync_all().unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn purge_successor_rejects_noncontiguous_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        drop(repository);
        let primary = std::fs::read(dir.path().join("repository/BACKUP_MANIFEST")).unwrap();
        let frames = read_catalog_records(primary.as_slice()).unwrap();
        let base_catalog_digest: [u8; 32] = Sha256::digest(&primary).into();
        let mut successor =
            std::fs::File::create(dir.path().join("repository/BACKUP_MANIFEST.purge.tmp")).unwrap();
        append_catalog_record(
            &mut successor,
            &CatalogRecord::Snapshot {
                sequence: frames
                    .frames
                    .last()
                    .map_or(0, |frame| record_sequence(&frame.record))
                    + 2,
                base_catalog_digest,
                high_water_id: 0,
                committed_generations: Vec::new(),
            },
        )
        .unwrap();
        successor.sync_all().unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn purge_then_create_on_same_handle_refreshes_replay() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let snapshot = serde_json::to_vec(&crate::manifest::ManifestRecord::Snapshot {
            l0_sstables: Vec::new(),
            levels: Vec::new(),
            range_only_ssts: Vec::new(),
            next_sst_id: 0,
            vlog_references: Vec::new(),
            imm_memtable_ids: Vec::new(),
            active_compaction_filters: Vec::new(),
            next_compaction_filter_id: 0,
            format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
            immutable_file_metadata: Vec::new(),
        })
        .unwrap();
        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(
            repository.create_generation(&snapshot, &snapshot).unwrap(),
            1
        );
        repository.purge(1).unwrap();
        assert_eq!(
            repository.create_generation(&snapshot, &snapshot).unwrap(),
            2
        );
        assert_eq!(repository.list_ids().unwrap(), vec![1, 2]);
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_catalog_compaction_failpoints_preserve_recoverable_generations() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();

        let create_repository = |root: &Path| {
            let engine = crate::lsm_storage::KvEngine::open(
                root.join("db"),
                crate::lsm_storage::LsmStorageOptions::default_for_test(),
            )
            .unwrap();
            engine.put(b"key", b"one").unwrap();
            engine
                .create_backup(BackupOptions {
                    repository: root.join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            engine.put(b"key", b"two").unwrap();
            engine
                .create_backup(BackupOptions {
                    repository: root.join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            engine.close().unwrap();
        };

        let before_replace = tempfile::tempdir().unwrap();
        create_repository(before_replace.path());
        let repository = BackupRepository::open(before_replace.path().join("repository")).unwrap();
        failpoint::cfg("backup.compact.after_temp_sync", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.compact.after_temp_sync", "off").unwrap();
        let error = repository.purge(1).unwrap_err();
        assert!(error.to_string().contains("invalidated"));
        drop(repository);
        assert_eq!(
            BackupRepository::open(before_replace.path().join("repository"))
                .unwrap()
                .list_ids()
                .unwrap(),
            vec![2]
        );
        assert!(
            !before_replace
                .path()
                .join("repository/generations/1")
                .exists()
        );

        let after_replace = tempfile::tempdir().unwrap();
        create_repository(after_replace.path());
        let repository = BackupRepository::open(after_replace.path().join("repository")).unwrap();
        failpoint::cfg("backup.compact.after_manifest_replace", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.compact.after_manifest_replace", "off").unwrap();
        let error = repository.purge(1).unwrap_err();
        assert!(error.to_string().contains("invalidated"));
        drop(repository);
        assert_eq!(
            BackupRepository::open(after_replace.path().join("repository"))
                .unwrap()
                .list_ids()
                .unwrap(),
            vec![2]
        );
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_snapshot_failpoint_reopens_with_retained_generation() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.purge.after_snapshot", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap();
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.purge.after_snapshot", "off").unwrap();
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.list_ids().unwrap(), vec![2]);
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_generation_reclaim_failpoint_reopens_with_retained_generation() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.purge.after_generation_reclaim", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap()
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.purge.after_generation_reclaim", "off").unwrap();
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.list_ids().unwrap(), vec![2]);
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_object_reclaim_failpoint_reopens_with_retained_generation() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.purge.after_object_reclaim", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap()
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.purge.after_object_reclaim", "off").unwrap();
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.list_ids().unwrap(), vec![2]);
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_object_fsync_failpoint_reopens_with_retained_generation() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions::default_for_test();
        let engine = crate::lsm_storage::KvEngine::open(dir.path().join("db"), options).unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.purge.after_object_fsync", "panic").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            repository.purge(1).unwrap()
        }));
        assert!(result.is_err());
        failpoint::cfg("backup.purge.after_object_fsync", "off").unwrap();
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.list_ids().unwrap(), vec![2]);
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn purge_io_failure_invalidates_same_handle() {
        use crate::chaos::failpoint::{self, FailScenario};
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        failpoint::cfg("backup.purge.before_root_fsync", "return").unwrap();
        let error = repository.purge(1).unwrap_err();
        assert!(error.to_string().contains("injected backup purge"));
        failpoint::cfg("backup.purge.before_root_fsync", "off").unwrap();
        let error = repository.purge(1).unwrap_err();
        assert!(error.to_string().contains("invalidated"));
        drop(repository);

        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.list_ids().unwrap(), vec![2]);
        scenario.teardown();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn purge_snapshot_reopen_preserves_next_backup_id() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions::default_for_test();
        let engine =
            crate::lsm_storage::KvEngine::open(dir.path().join("db"), options.clone()).unwrap();
        engine.put(b"key", b"one").unwrap();
        let backup = |engine: &crate::lsm_storage::KvEngine| {
            engine.create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
        };
        backup(&engine).unwrap();
        engine.put(b"key", b"two").unwrap();
        backup(&engine).unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository.purge(1).unwrap();
        drop(repository);
        assert_eq!(
            BackupRepository::open(dir.path().join("repository"))
                .unwrap()
                .list_ids()
                .unwrap(),
            vec![2]
        );

        let reopened = crate::lsm_storage::KvEngine::open(dir.path().join("db"), options).unwrap();
        let third = committed(
            reopened
                .create_backup(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap(),
        );
        assert_eq!(third.id, 3);
        reopened.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn purge_is_idempotent_when_retaining_all_generations() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"one").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"two").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let manifest_len_before = std::fs::metadata(dir.path().join("repository/BACKUP_MANIFEST"))
            .unwrap()
            .len();
        repository.purge(10).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![1, 2]);
        assert_eq!(
            std::fs::metadata(dir.path().join("repository/BACKUP_MANIFEST"))
                .unwrap()
                .len(),
            manifest_len_before
        );
        repository.purge(10).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![1, 2]);
        repository.purge(1).unwrap();
        repository.purge(1).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![2]);
    }

    #[test]
    fn catalog_rejects_corrupt_header_and_noncanonical_payload() {
        let record = CatalogRecord::HighWater {
            sequence: 1,
            allocated_id: 1,
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &record).unwrap();
        bytes[0] ^= 1;
        assert!(read_catalog_records(bytes.as_slice()).is_err());

        let noncanonical =
            br#"{"version":1, "record":{"type":"high_water","sequence":1,"allocated_id":1}}"#;
        let mut framed = Vec::new();
        let mut header = [0_u8; CATALOG_FRAME_HEADER_BYTES];
        header[..4].copy_from_slice(&(noncanonical.len() as u32).to_le_bytes());
        header[4..8].copy_from_slice(&crc32(noncanonical).to_le_bytes());
        let header_checksum = crc32(&header[..8]);
        header[8..].copy_from_slice(&header_checksum.to_le_bytes());
        framed.extend_from_slice(&header);
        framed.extend_from_slice(noncanonical);
        assert!(read_catalog_records(framed.as_slice()).is_err());
    }

    #[test]
    fn prepare_digest_uses_exact_persisted_payload() {
        let prepare = CatalogRecord::Prepare {
            sequence: 2,
            id: 1,
            parent_id: None,
            generation_checksum: [9; 32],
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &prepare).unwrap();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        let frame = &frames.frames[0];
        assert_eq!(frame.record, prepare);
        let expected: [u8; 32] = Sha256::digest(&frame.payload).into();
        assert_eq!(prepare_payload_digest(&frame.payload), expected);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn no_follow_open_rejects_symlink_components() {
        assert!(open_directory_no_follow(Path::new(".")).is_ok());
        let dir = tempfile::tempdir().unwrap();
        assert!(BackupRepository::validate_restore_target(dir.path().join("new")).is_ok());
        std::fs::create_dir(dir.path().join("existing")).unwrap();
        assert!(BackupRepository::validate_restore_target(dir.path().join("existing")).is_err());
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::os::unix::fs::symlink(&real, dir.path().join("target-link")).unwrap();
        assert!(BackupRepository::validate_restore_target(dir.path().join("target-link")).is_err());
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(open_directory_no_follow(&link).is_err());

        let parent = open_directory_no_follow(&real).unwrap();
        std::fs::write(real.join("file"), b"ok").unwrap();
        assert!(openat_no_follow(&parent, "file", libc::O_RDONLY, 0).is_ok());
        let (staging_name, _staging_fd) =
            BackupRepository::create_restore_staging(&parent, "restore-target").unwrap();
        assert!(real.join(&staging_name).is_dir());
        let cleanup = RestoreStagingCleanup {
            parent: &parent,
            name: staging_name.clone(),
        };
        drop(cleanup);
        assert!(!real.join(staging_name).exists());
        let collision_sequence = OBJECT_TEMP_SEQUENCE.load(Ordering::Relaxed);
        let collision_name = format!(
            ".restore-target.restore-{}-{collision_sequence}",
            std::process::id()
        );
        std::fs::create_dir(real.join(&collision_name)).unwrap();
        let (retry_name, _retry_fd) =
            BackupRepository::create_restore_staging(&parent, "restore-target").unwrap();
        assert_ne!(retry_name, collision_name);
        std::fs::remove_dir(real.join(collision_name)).unwrap();
        std::fs::remove_dir(real.join(retry_name)).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bootstrap_publishes_fsynced_repository_layout() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        std::fs::write(
            dir.path().join(format!(
                "repository/.BACKUP_MANIFEST.compact-{}-0",
                std::process::id()
            )),
            b"stale",
        )
        .unwrap();
        let lookalike = ".BACKUP_MANIFEST.compact-0001-0002";
        std::fs::write(dir.path().join("repository").join(lookalike), b"unrelated").unwrap();
        let canonical_dir = format!(".BACKUP_MANIFEST.compact-{}-1", std::process::id());
        std::fs::create_dir(dir.path().join("repository").join(&canonical_dir)).unwrap();
        let canonical_link = format!(".BACKUP_MANIFEST.compact-{}-2", std::process::id());
        std::os::unix::fs::symlink(
            "BACKUP_MANIFEST",
            dir.path().join("repository").join(&canonical_link),
        )
        .unwrap();
        use std::os::unix::ffi::OsStringExt;
        let unrelated = std::ffi::OsString::from_vec(vec![0xff, b'-', b'x']);
        std::fs::write(dir.path().join("repository").join(&unrelated), b"unrelated").unwrap();
        let repository =
            openat_no_follow(&parent, "repository", libc::O_RDONLY | libc::O_DIRECTORY, 0).unwrap();
        assert!(
            openat_no_follow(&repository, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0).is_ok()
        );
        assert!(
            openat_no_follow(
                &repository,
                "generations",
                libc::O_RDONLY | libc::O_DIRECTORY,
                0
            )
            .is_ok()
        );
        assert!(openat_no_follow(&repository, "BACKUP_MANIFEST", libc::O_RDONLY, 0).is_ok());
        let mut opened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert!(
            !dir.path()
                .join(format!(
                    "repository/.BACKUP_MANIFEST.compact-{}-0",
                    std::process::id()
                ))
                .exists()
        );
        assert!(dir.path().join("repository").join(unrelated).exists());
        assert!(dir.path().join("repository").join(lookalike).exists());
        assert!(dir.path().join("repository").join(canonical_dir).is_dir());
        assert!(dir.path().join("repository").join(canonical_link).exists());
        assert!(opened.latest_info().unwrap().is_none());
        assert_eq!(opened.latest_id(), None);
        opened.compact().unwrap();
        assert!(opened.list().unwrap().is_empty());
        std::fs::write(dir.path().join("source-object"), b"stable-object").unwrap();
        let object_checksum: [u8; 32] = Sha256::digest(b"stable-object").into();
        assert!(
            !opened
                .publish_object(
                    &parent,
                    "source-object",
                    RepositoryObjectKind::Sst,
                    9,
                    13,
                    object_checksum,
                    false
                )
                .unwrap()
        );
        assert!(
            opened
                .publish_object(
                    &parent,
                    "source-object",
                    RepositoryObjectKind::Sst,
                    9,
                    13,
                    object_checksum,
                    false
                )
                .unwrap()
        );
        let id = opened.allocate_backup_id().unwrap();
        let snapshot = serde_json::to_vec(&crate::manifest::ManifestRecord::Snapshot {
            l0_sstables: Vec::new(),
            levels: Vec::new(),
            range_only_ssts: Vec::new(),
            next_sst_id: 0,
            vlog_references: Vec::new(),
            imm_memtable_ids: Vec::new(),
            active_compaction_filters: Vec::new(),
            next_compaction_filter_id: 0,
            format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
            immutable_file_metadata: Vec::new(),
        })
        .unwrap();
        let (staging, generation_bytes) = opened
            .stage_generation(id, None, br#"{"id":1}"#, &snapshot, &[], 0, None)
            .unwrap();
        let generation_checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        let digest = opened
            .prepare_generation(id, None, generation_checksum)
            .unwrap();
        opened.publish_staged_generation(id, &staging).unwrap();
        opened.commit_generation(id, digest, None).unwrap();
        drop(opened);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.high_water_id(), 1);
        assert_eq!(reopened.list_ids().unwrap(), vec![1]);
        let infos = reopened.list_info().unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].id, 1);
        assert_eq!(reopened.list().unwrap(), infos.clone());
        assert_eq!(reopened.info(1).unwrap().id, 1);
        assert_eq!(reopened.latest_info().unwrap().unwrap().id, 1);
        assert_eq!(reopened.latest_id_result().unwrap(), Some(1));
        assert_eq!(reopened.latest_id(), Some(1));
        assert!(reopened.retained_ids(0).is_err());
        assert_eq!(reopened.retained_ids(1).unwrap(), vec![1]);
        assert_eq!(reopened.retained_ids(10).unwrap(), vec![1]);
        assert!(reopened.retained_object_names(1).unwrap().is_empty());
        let orphan_name = derived_object_name(RepositoryObjectKind::Sst, 9, object_checksum);
        assert_eq!(
            reopened.unreferenced_object_names(1).unwrap(),
            vec![orphan_name.clone()]
        );
        assert_eq!(
            reopened.plan_purge(1).unwrap(),
            (vec![1], vec![orphan_name])
        );
        assert_eq!(infos[0].parent_id, None);
        assert_eq!(infos[0].file_count, 0);
        reopened.verify(1).unwrap();
        reopened.verify_all().unwrap();
        assert!(reopened.verify(2).is_err());
        drop(reopened);
        let mut compacted = BackupRepository::open(dir.path().join("repository")).unwrap();
        compacted.compact_catalog().unwrap();
        drop(compacted);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.list_ids().unwrap(), vec![1]);
        std::fs::OpenOptions::new()
            .write(true)
            .open(
                dir.path()
                    .join("repository/generations/1/MANIFEST_SNAPSHOT"),
            )
            .unwrap()
            .set_len(0)
            .unwrap();
        assert!(reopened.verify_all().is_err());
        drop(reopened);
        let published = dir.path().join("repository").join("generations").join("1");
        assert_eq!(
            std::fs::read(published.join("GENERATION")).unwrap(),
            generation_bytes
        );
        let reopened = BackupRepository::open(dir.path().join("repository"));
        assert!(reopened.is_err());
        std::fs::write(published.join("GENERATION"), br#"{"id":2}"#).unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reopen_removes_uncommitted_generation_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        std::fs::create_dir(dir.path().join("repository/generations/99")).unwrap();
        std::fs::write(
            dir.path().join("repository/generations/99/GENERATION"),
            b"orphan",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("repository/generations/99/MANIFEST_SNAPSHOT"),
            b"orphan",
        )
        .unwrap();
        let _opened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert!(!dir.path().join("repository/generations/99").exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_object_publication_removes_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("source"), b"bytes").unwrap();
        std::fs::write(dir.path().join("object"), b"existing").unwrap();
        let source_dir = open_directory_no_follow(dir.path()).unwrap();
        let target_dir = open_directory_no_follow(dir.path()).unwrap();
        let checksum: [u8; 32] = Sha256::digest(b"bytes").into();
        assert!(
            copy_immutable_object(&source_dir, "source", &target_dir, "object", 5, checksum)
                .is_err()
        );
        assert!(!std::fs::read_dir(dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".object.tmp-")
        }));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn matching_repository_object_can_be_reused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("source"), b"stable-object").unwrap();
        let directory = open_directory_no_follow(dir.path()).unwrap();
        let checksum: [u8; 32] = Sha256::digest(b"stable-object").into();
        assert!(
            !copy_or_reuse_object(
                &directory, "source", &directory, "object", 13, checksum, true
            )
            .unwrap()
        );
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(dir.path().join("source")).unwrap().ino(),
            std::fs::metadata(dir.path().join("object")).unwrap().ino()
        );
        assert!(
            copy_or_reuse_object(
                &directory, "source", &directory, "object", 13, checksum, false
            )
            .unwrap()
        );
        assert!(reuse_matching_object(&directory, "object", 12, checksum).is_err());
        assert!(reuse_matching_object(&directory, "object", 13, [0; 32]).is_err());
    }

    #[test]
    fn derived_object_names_are_stable_and_typed() {
        let name = derived_object_name(RepositoryObjectKind::Sst, 7, [0xab; 32]);
        assert_eq!(name, format!("sst-7-{}", "ab".repeat(32)));
        assert!(derived_object_name(RepositoryObjectKind::Vlog, 7, [0; 32]).starts_with("vlog-7-"));
    }

    #[test]
    fn gc_compaction_v2_round_trips_metadata() {
        let metadata = crate::manifest::ImmutableFileMetadata {
            kind: crate::manifest::ImmutableFileKind::Vlog,
            file_id: 7,
            file_size: 11,
            file_checksum: [0xcd; 32],
        };
        let record = crate::manifest::ManifestRecord::GcCompactionV2(3, 7, 2, metadata.clone());
        let encoded = serde_json::to_vec(&record).unwrap();
        let decoded: crate::manifest::ManifestRecord = serde_json::from_slice(&encoded).unwrap();
        let crate::manifest::ManifestRecord::GcCompactionV2(_, _, _, decoded_metadata) = decoded
        else {
            panic!("unexpected manifest record variant");
        };
        assert!(decoded_metadata.matches(&metadata));
    }

    #[test]
    fn vlog_retire_record_round_trips() {
        let record = crate::manifest::ManifestRecord::VlogRetire(42);
        let encoded = serde_json::to_vec(&record).unwrap();
        let decoded: crate::manifest::ManifestRecord = serde_json::from_slice(&encoded).unwrap();
        assert!(matches!(
            decoded,
            crate::manifest::ManifestRecord::VlogRetire(42)
        ));
    }

    #[test]
    fn generation_object_validation_rejects_unsafe_identity() {
        let checksum = [1; 32];
        let valid = GenerationEnvelope {
            version: 2,
            id: 1,
            created_at_secs: 1,
            parent_id: None,
            new_object_bytes: 0,
            snapshot_len: 0,
            snapshot_checksum: [0; 32],
            objects: Some(vec![GenerationObject {
                kind: RepositoryObjectKind::Sst,
                source_path: "00001.sst".into(),
                object_name: derived_object_name(RepositoryObjectKind::Sst, 1, checksum),
                file_id: 1,
                file_size: 1,
                file_checksum: checksum,
            }]),
            compatibility: None,
            body: Vec::new(),
        };
        assert!(validate_generation_objects(&valid).is_ok());
        let mut invalid = valid;
        invalid.objects.as_mut().unwrap()[0].source_path = "../escape".into();
        assert!(validate_generation_objects(&invalid).is_err());

        invalid.version = 4;
        invalid.compatibility = Some(RestoreCompatibility {
            manifest_format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
            value_separation_enabled: false,
            vlog_format_version: None,
            ttl_records_present: false,
            serializable_at_capture: false,
        });
        let object = &mut invalid.objects.as_mut().unwrap()[0];
        object.kind = RepositoryObjectKind::Vlog;
        object.source_path = "vlog/1.vlog".into();
        object.object_name = derived_object_name(RepositoryObjectKind::Vlog, 1, checksum);
        assert!(validate_generation_objects(&invalid).is_err());

        invalid.version = 3;
        invalid.compatibility = None;
        assert!(validate_generation_objects(&invalid).is_ok());
        let disabled = crate::lsm_storage::LsmStorageOptions::default_for_test();
        assert!(validate_restore_options(&invalid, &disabled).is_err());
        let mut enabled = disabled;
        enabled.value_separation = Some(crate::vlog::ValueSeparationOptions {
            enabled: true,
            ..Default::default()
        });
        assert!(validate_restore_options(&invalid, &enabled).is_ok());
    }

    #[test]
    fn legacy_v2_envelope_without_accounting_field_remains_canonical() {
        let legacy = br#"{"version":2,"id":7,"created_at_secs":9,"parent_id":null,"snapshot_len":0,"snapshot_checksum":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"objects":null,"body":[]}"#;
        let envelope: GenerationEnvelope = serde_json::from_slice(legacy).unwrap();
        assert_eq!(envelope.new_object_bytes, 0);
        assert_eq!(serde_json::to_vec(&envelope).unwrap(), legacy);
    }

    #[test]
    fn restore_snapshot_validation_rejects_unsupported_and_mismatched_formats() {
        let snapshot = |format_version| {
            serde_json::to_vec(&crate::manifest::ManifestRecord::Snapshot {
                l0_sstables: Vec::new(),
                levels: Vec::new(),
                range_only_ssts: Vec::new(),
                next_sst_id: 0,
                vlog_references: Vec::new(),
                imm_memtable_ids: Vec::new(),
                active_compaction_filters: Vec::new(),
                next_compaction_filter_id: 0,
                format_version,
                immutable_file_metadata: Vec::new(),
            })
            .unwrap()
        };
        let mut envelope = GenerationEnvelope {
            version: 3,
            id: 1,
            created_at_secs: 1,
            parent_id: None,
            new_object_bytes: 0,
            snapshot_len: 0,
            snapshot_checksum: [0; 32],
            objects: Some(Vec::new()),
            compatibility: None,
            body: Vec::new(),
        };
        assert!(validate_restore_snapshot_objects(&envelope, &snapshot(2)).is_err());

        envelope.version = 4;
        envelope.compatibility = Some(RestoreCompatibility {
            manifest_format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
            value_separation_enabled: false,
            vlog_format_version: None,
            ttl_records_present: false,
            serializable_at_capture: false,
        });
        assert!(validate_restore_snapshot_objects(&envelope, &snapshot(5)).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn generation_object_validation_checks_repository_bytes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("files")).unwrap();
        let checksum: [u8; 32] = Sha256::digest(b"object").into();
        let name = derived_object_name(RepositoryObjectKind::Sst, 3, checksum);
        std::fs::write(dir.path().join("files").join(&name), b"object").unwrap();
        let root = open_directory_no_follow(dir.path()).unwrap();
        let envelope = GenerationEnvelope {
            version: 2,
            id: 1,
            created_at_secs: 1,
            parent_id: None,
            new_object_bytes: 0,
            snapshot_len: 0,
            snapshot_checksum: [0; 32],
            objects: Some(vec![GenerationObject {
                kind: RepositoryObjectKind::Sst,
                source_path: "00003.sst".into(),
                object_name: name,
                file_id: 3,
                file_size: 6,
                file_checksum: checksum,
            }]),
            compatibility: None,
            body: Vec::new(),
        };
        validate_generation_objects_on_disk(&root, &envelope).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_publishes_captured_generation() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        let info = committed(
            engine
                .create_backup(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap(),
        );
        assert_eq!(info.id, 1);
        assert_eq!(info.parent_id, None);
        assert_eq!(info.file_count, 1);
        assert!(info.logical_bytes > 0);
        let snapshot_bytes = std::fs::read(
            dir.path()
                .join("repository/generations/1/MANIFEST_SNAPSHOT"),
        )
        .unwrap();
        let snapshot: crate::manifest::ManifestRecord =
            serde_json::from_slice(&snapshot_bytes).unwrap();
        let crate::manifest::ManifestRecord::Snapshot {
            immutable_file_metadata,
            ..
        } = snapshot
        else {
            panic!("backup snapshot is not a manifest snapshot");
        };
        assert_eq!(immutable_file_metadata.len(), info.file_count as usize);
        let second = committed(
            engine
                .create_backup(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap(),
        );
        assert_eq!(second.id, 2);
        assert_eq!(second.parent_id, Some(1));
        assert_eq!(second.new_object_bytes, 0);
        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository.purge(1).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![2]);
        let catalog = std::fs::read(dir.path().join("repository/BACKUP_MANIFEST")).unwrap();
        let frames = read_catalog_records(catalog.as_slice()).unwrap();
        let CatalogRecord::Snapshot {
            base_catalog_digest,
            committed_generations,
            ..
        } = &frames.frames[0].record
        else {
            panic!("purge did not install a catalog snapshot");
        };
        assert_ne!(*base_catalog_digest, [0; 32]);
        assert_eq!(committed_generations.len(), 1);
        assert!(committed_generations[0].manifest_snapshot_len.unwrap() > 0);
        assert!(committed_generations[0].file_count.unwrap() > 0);
        repository.compact().unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![2]);
        assert!(!dir.path().join("repository/generations/1").exists());
        repository.purge(1).unwrap();
        assert_eq!(repository.list_ids().unwrap(), vec![2]);
        drop(repository);
        engine.close().unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let outcome = repository
            .restore(
                2,
                dir.path().join("restored"),
                crate::lsm_storage::LsmStorageOptions::default_for_test(),
            )
            .unwrap();
        assert!(matches!(outcome, RestoreOutcome::Restored));
        let restored = crate::lsm_storage::KvEngine::open(
            dir.path().join("restored"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        assert_eq!(
            restored.get(b"key").unwrap(),
            Some(bytes::Bytes::from_static(b"value"))
        );
        restored.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backup_capture_pins_ttl_compatibility_state() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        let capture = engine.inner.prepare_backup_capture().unwrap();
        assert!(!capture.has_ttl_entries);
        engine
            .put_with_ttl(b"later", b"ttl", std::time::Duration::from_secs(60))
            .unwrap();
        assert!(!capture.has_ttl_entries);
        drop(capture);
        engine.close().unwrap();
    }

    #[cfg(all(target_os = "linux", feature = "chaos-testing"))]
    #[test]
    fn restore_releases_repository_lock_while_materializing() {
        use crate::chaos::failpoint::FailScenario;
        use std::{sync::mpsc, time::Duration};

        let _test_lock = restore_unlock_test_hook::lock();
        let _failpoint_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.put(b"key", b"new-value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let target = dir.path().join("restored-lock-handoff");
        let restored_path = target.clone();
        restore_unlock_test_hook::arm("restored-lock-handoff");
        let restore = std::thread::spawn(move || {
            repository.restore(
                1,
                target,
                crate::lsm_storage::LsmStorageOptions::default_for_test(),
            )
        });
        restore_unlock_test_hook::wait();
        let (opened_tx, opened_rx) = mpsc::channel();
        let repository_path = dir.path().join("repository");
        std::thread::spawn(move || {
            let opened = BackupRepository::open(repository_path)
                .and_then(|repository| repository.purge(1))
                .is_ok();
            opened_tx.send(opened).unwrap();
        });
        assert!(opened_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        restore_unlock_test_hook::release();
        assert!(matches!(
            restore.join().unwrap().unwrap(),
            RestoreOutcome::Restored
        ));
        let restored = crate::lsm_storage::KvEngine::open(
            restored_path,
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        assert_eq!(
            restored.get(b"key").unwrap(),
            Some(bytes::Bytes::from_static(b"value"))
        );
        restored.close().unwrap();
        scenario.teardown();
    }

    #[cfg(all(target_os = "linux", feature = "chaos-testing"))]
    #[test]
    fn restore_invalidates_same_handle_before_unlock() {
        use crate::chaos::failpoint::FailScenario;
        use std::{sync::mpsc, time::Duration};

        let _test_lock = restore_unlock_test_hook::lock();
        let _failpoint_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        std::fs::write(dir.path().join("source"), b"source").unwrap();

        let repository =
            std::sync::Arc::new(BackupRepository::open(dir.path().join("repository")).unwrap());
        let restore_repository = std::sync::Arc::clone(&repository);
        let restore_target = dir.path().join("restored-same-handle-race");
        restore_unlock_test_hook::arm("restored-same-handle-race");
        let restore = std::thread::spawn(move || {
            restore_repository.restore(
                1,
                restore_target,
                crate::lsm_storage::LsmStorageOptions::default_for_test(),
            )
        });
        restore_unlock_test_hook::wait();
        let (result_tx, result_rx) = mpsc::channel();
        let mutation_repository = std::sync::Arc::clone(&repository);
        let source_dir = dir.path().to_path_buf();
        std::thread::spawn(move || {
            let source = open_directory_no_follow(&source_dir).unwrap();
            let checksum: [u8; 32] = Sha256::digest(b"source").into();
            result_tx
                .send(mutation_repository.publish_object(
                    &source,
                    "source",
                    RepositoryObjectKind::Sst,
                    99,
                    6,
                    checksum,
                    false,
                ))
                .unwrap();
        });
        let error = result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("stale after restore"));
        restore_unlock_test_hook::release();
        assert!(matches!(
            restore.join().unwrap().unwrap(),
            RestoreOutcome::Restored
        ));
        scenario.teardown();
    }

    #[cfg(all(target_os = "linux", feature = "chaos-testing"))]
    #[test]
    fn restore_serializes_same_handle_purge_through_relock() {
        use crate::chaos::failpoint::FailScenario;
        use std::{sync::mpsc, time::Duration};

        let _test_lock = restore_unlock_test_hook::lock();
        let _failpoint_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository =
            std::sync::Arc::new(BackupRepository::open(dir.path().join("repository")).unwrap());
        let restore_repository = std::sync::Arc::clone(&repository);
        let restore_target = dir.path().join("restore-serializes-purge");
        restore_unlock_test_hook::arm("restore-serializes-purge");
        let restore = std::thread::spawn(move || {
            restore_repository.restore(
                1,
                restore_target,
                crate::lsm_storage::LsmStorageOptions::default_for_test(),
            )
        });
        restore_unlock_test_hook::wait();

        let (purge_tx, purge_rx) = mpsc::channel();
        let purge_repository = std::sync::Arc::clone(&repository);
        let purge = std::thread::spawn(move || {
            purge_tx.send(purge_repository.purge(1)).unwrap();
        });
        let (second_restore_tx, second_restore_rx) = mpsc::channel();
        let second_restore_repository = std::sync::Arc::clone(&repository);
        let second_restore_target = dir.path().join("second-concurrent-restore");
        let second_restore = std::thread::spawn(move || {
            second_restore_tx
                .send(second_restore_repository.restore(
                    1,
                    second_restore_target,
                    crate::lsm_storage::LsmStorageOptions::default_for_test(),
                ))
                .unwrap();
        });
        let (list_tx, list_rx) = mpsc::channel();
        let list_repository = std::sync::Arc::clone(&repository);
        let list = std::thread::spawn(move || {
            list_tx.send(list_repository.list_ids()).unwrap();
        });
        let (verify_tx, verify_rx) = mpsc::channel();
        let verify_repository = std::sync::Arc::clone(&repository);
        let verify = std::thread::spawn(move || {
            verify_tx.send(verify_repository.verify(1)).unwrap();
        });
        assert!(matches!(
            purge_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            second_restore_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            list_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            verify_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        restore_unlock_test_hook::release();
        assert!(matches!(
            restore.join().unwrap().unwrap(),
            RestoreOutcome::Restored
        ));
        let error = purge_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("stale after restore"));
        let error = second_restore_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("stale after restore"));
        assert_eq!(
            list_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            vec![1]
        );
        verify_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        purge.join().unwrap();
        second_restore.join().unwrap();
        list.join().unwrap();
        verify.join().unwrap();
        scenario.teardown();
    }

    #[cfg(all(target_os = "linux", feature = "chaos-testing"))]
    #[test]
    fn restore_panic_after_unlock_reacquires_repository_lock() {
        use crate::chaos::failpoint::{self, FailScenario};
        use std::{sync::mpsc, time::Duration};

        let _restore_lock = restore_unlock_test_hook::lock();
        let _failpoint_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let scenario = FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository =
            std::sync::Arc::new(BackupRepository::open(dir.path().join("repository")).unwrap());
        let restore_repository = std::sync::Arc::clone(&repository);
        let restore_target = dir.path().join("restore-panic-relock");
        restore_unlock_test_hook::arm("restore-panic-relock");
        failpoint::cfg("backup.restore.after_unlock", "panic").unwrap();
        let restore = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                restore_repository.restore(
                    1,
                    restore_target,
                    crate::lsm_storage::LsmStorageOptions::default_for_test(),
                )
            }))
        });
        restore_unlock_test_hook::wait();
        restore_unlock_test_hook::release();
        assert!(restore.join().unwrap().is_err());
        failpoint::cfg("backup.restore.after_unlock", "off").unwrap();

        assert_eq!(repository.list_ids().unwrap(), vec![1]);
        let (opened_tx, opened_rx) = mpsc::channel();
        let repository_path = dir.path().join("repository");
        let opener = std::thread::spawn(move || {
            let opened = BackupRepository::open(repository_path).is_ok();
            opened_tx.send(opened).unwrap();
        });
        assert!(matches!(
            opened_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(repository);
        assert!(opened_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        opener.join().unwrap();
        scenario.teardown();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_invalidates_repository_handle_for_mutation() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository
            .restore(
                1,
                dir.path().join("restored"),
                crate::lsm_storage::LsmStorageOptions::default_for_test(),
            )
            .unwrap();
        assert!(repository.purge(1).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_with_outcome_reports_commit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = engine
            .create_backup_with_outcome(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        assert!(matches!(outcome, BackupOutcome::Committed(_)));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_rfc_named_outcome_reports_commit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = engine
            .create_backup_outcome(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        assert!(matches!(outcome, CreateBackupOutcome::Committed(_)));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bootstrap_publication_error_reports_repository_without_generation() {
        let repository = PathBuf::from("repository");
        let outcome = backup_outcome_from_error(
            repository.clone(),
            anyhow::Error::new(RepositoryBootstrapPublicationError {
                source: std::io::Error::from_raw_os_error(libc::EIO).into(),
            }),
        )
        .unwrap();
        let BackupOutcome::RepositoryPublishedButNotDurable {
            repository: reported,
            error,
        } = outcome
        else {
            panic!("expected repository durability outcome");
        };
        assert_eq!(reported, repository);
        assert_eq!(
            error.kind(),
            std::io::Error::from_raw_os_error(libc::EIO).kind()
        );
        assert_eq!(error.raw_os_error(), Some(libc::EIO));

        let outcome = sync_outcome(BackupOutcome::RepositoryPublishedButNotDurable {
            repository: repository.clone(),
            error: std::io::Error::from_raw_os_error(libc::ENOSPC),
        })
        .unwrap();
        let CreateBackupOutcome::RepositoryPublishedButNotDurable {
            repository: reported,
            error,
        } = outcome
        else {
            panic!("expected synchronous repository durability outcome");
        };
        assert_eq!(reported, repository);
        assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
        assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn commit_publication_outcomes_preserve_backup_info() {
        let info = BackupInfo {
            id: 7,
            created_at_secs: 11,
            parent_id: Some(6),
            logical_bytes: 13,
            file_count: 2,
            new_object_bytes: 5,
        };
        let outcome = backup_outcome_from_error(
            PathBuf::from("repository"),
            anyhow::Error::new(CommitPublicationError {
                id: info.id,
                info: Some(info.clone()),
                kind: CommitFailureKind::CommitPublishedButNotDurable,
                source: std::io::Error::from_raw_os_error(libc::EIO).into(),
                revalidation_error: None,
            }),
        )
        .unwrap();
        let BackupOutcome::CommitPublishedButNotDurable {
            info: reported,
            error,
        } = outcome
        else {
            panic!("expected asynchronous commit durability outcome");
        };
        assert_eq!(reported, info);
        assert_eq!(
            error.kind(),
            std::io::Error::from_raw_os_error(libc::EIO).kind()
        );
        assert_eq!(error.raw_os_error(), Some(libc::EIO));

        let outcome = sync_outcome(BackupOutcome::CommitPublishedButNotDurable {
            info: info.clone(),
            error: std::io::Error::from_raw_os_error(libc::ENOSPC),
        })
        .unwrap();
        let CreateBackupOutcome::CommitPublishedButNotDurable {
            info: reported,
            error,
        } = outcome
        else {
            panic!("expected synchronous commit durability outcome");
        };
        assert_eq!(reported, info);
        assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
        assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));

        let outcome = backup_outcome_from_error(
            PathBuf::from("repository"),
            anyhow::Error::new(CommitPublicationError {
                id: info.id,
                info: Some(info.clone()),
                kind: CommitFailureKind::CommitDurabilityUnknown,
                source: anyhow!("catalog fsync failed"),
                revalidation_error: Some(anyhow!("catalog replay was inconclusive")),
            }),
        )
        .unwrap();
        assert!(matches!(
            outcome,
            BackupOutcome::CommitPublicationUnknown {
                info: reported,
                ..
            } if reported == info
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn attempt_object_cleanup_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        std::fs::write(dir.path().join("source-object"), b"stable-object").unwrap();
        let checksum: [u8; 32] = Sha256::digest(b"stable-object").into();
        let created = repository
            .publish_object(
                &parent,
                "source-object",
                RepositoryObjectKind::Sst,
                1,
                13,
                checksum,
                false,
            )
            .unwrap();
        assert!(!created);
        let name = derived_object_name(RepositoryObjectKind::Sst, 1, checksum);
        repository
            .remove_objects(std::slice::from_ref(&name))
            .unwrap();
        repository
            .remove_objects(std::slice::from_ref(&name))
            .unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn staging_cleanup_reports_missing_generation() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let error = cleanup_staging_generation(&repository.root, "missing-staging").unwrap_err();
        assert!(error.to_string().contains("failed to open"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_wal_backup_reopens_with_compatible_options() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions {
            enable_wal: true,
            ..crate::lsm_storage::LsmStorageOptions::default_for_test()
        };
        let engine =
            crate::lsm_storage::KvEngine::open(dir.path().join("db"), options.clone()).unwrap();
        engine.put(b"wal-key", b"wal-value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository
            .restore(1, dir.path().join("restored"), options.clone())
            .unwrap();
        let restored =
            crate::lsm_storage::KvEngine::open(dir.path().join("restored"), options).unwrap();
        assert_eq!(
            restored.get(b"wal-key").unwrap(),
            Some(bytes::Bytes::from_static(b"wal-value"))
        );
        restored.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_vlog_backup_reopens_with_compatible_options() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions {
            value_separation: Some(crate::vlog::ValueSeparationOptions {
                enabled: true,
                min_value_size: 16,
                ..crate::vlog::ValueSeparationOptions::default()
            }),
            ..crate::lsm_storage::LsmStorageOptions::default_for_test()
        };
        let engine =
            crate::lsm_storage::KvEngine::open(dir.path().join("db"), options.clone()).unwrap();
        let value = b"value-large-enough-for-vlog";
        engine.put(b"vlog-key", value).unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository
            .restore(1, dir.path().join("restored"), options.clone())
            .unwrap();
        let restored =
            crate::lsm_storage::KvEngine::open(dir.path().join("restored"), options).unwrap();
        assert_eq!(
            restored.get(b"vlog-key").unwrap(),
            Some(bytes::Bytes::from_static(value))
        );
        restored.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_rejects_incompatible_value_separation_before_publication() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions::default_for_test();
        let engine = crate::lsm_storage::KvEngine::open(dir.path().join("db"), options).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let mut incompatible = crate::lsm_storage::LsmStorageOptions::default_for_test();
        incompatible.value_separation = Some(crate::vlog::ValueSeparationOptions {
            enabled: true,
            ..Default::default()
        });
        let target = dir.path().join("restored");
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert!(repository.restore(1, &target, incompatible).is_err());
        assert!(!target.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_range_tombstone_backup_reopens_with_tombstone() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions::default_for_test();
        let engine =
            crate::lsm_storage::KvEngine::open(dir.path().join("db"), options.clone()).unwrap();
        engine.put(b"k1", b"v1").unwrap();
        engine.put(b"k2", b"v2").unwrap();
        engine.put(b"k3", b"v3").unwrap();
        engine.delete_range(b"k2", b"k3").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository
            .restore(1, dir.path().join("restored"), options.clone())
            .unwrap();
        let restored =
            crate::lsm_storage::KvEngine::open(dir.path().join("restored"), options).unwrap();
        assert_eq!(
            restored.get(b"k1").unwrap(),
            Some(bytes::Bytes::from_static(b"v1"))
        );
        assert_eq!(restored.get(b"k2").unwrap(), None);
        assert_eq!(
            restored.get(b"k3").unwrap(),
            Some(bytes::Bytes::from_static(b"v3"))
        );
        restored.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_ttl_backup_reopens_with_live_ttl_entry() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions::default_for_test();
        let engine =
            crate::lsm_storage::KvEngine::open(dir.path().join("db"), options.clone()).unwrap();
        engine
            .put_with_ttl(b"ttl-key", b"ttl-value", std::time::Duration::from_secs(60))
            .unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository
            .restore(1, dir.path().join("restored"), options.clone())
            .unwrap();
        let restored =
            crate::lsm_storage::KvEngine::open(dir.path().join("restored"), options).unwrap();
        assert_eq!(
            restored.get(b"ttl-key").unwrap(),
            Some(bytes::Bytes::from_static(b"ttl-value"))
        );
        restored.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restore_serializable_backup_reopens_with_compatible_options() {
        #[cfg(feature = "chaos-testing")]
        let _test_lock = BACKUP_FAILPOINT_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let options = crate::lsm_storage::LsmStorageOptions {
            serializable: true,
            ..crate::lsm_storage::LsmStorageOptions::default_for_test()
        };
        let engine =
            crate::lsm_storage::KvEngine::open(dir.path().join("db"), options.clone()).unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"txn-key", b"txn-value").unwrap();
        txn.commit().unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();

        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository
            .restore(1, dir.path().join("restored"), options.clone())
            .unwrap();
        let restored =
            crate::lsm_storage::KvEngine::open(dir.path().join("restored"), options).unwrap();
        assert_eq!(
            restored.get(b"txn-key").unwrap(),
            Some(bytes::Bytes::from_static(b"txn-value"))
        );
        restored.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_async_with_outcome_reports_commit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(engine.create_backup_async_with_outcome(BackupOptions {
            repository: dir.path().join("repository"),
            use_hard_links: false,
        }))
        .unwrap();
        assert!(matches!(outcome, BackupOutcome::Committed(_)));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_async_rfc_named_outcome_reports_commit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(engine.create_backup_async_outcome(BackupOptions {
            repository: dir.path().join("repository"),
            use_hard_links: false,
        }))
        .unwrap();
        assert!(matches!(outcome, CreateBackupOutcome::Committed(_)));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_task_eagerly_dispatches() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(async {
            let task = engine
                .create_backup_task(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            task.await
        })
        .unwrap();
        assert!(matches!(outcome, BackupOutcome::Committed(_)));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_task_cancel_is_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(async {
            let task = engine
                .create_backup_task(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            task.cancel();
            task.await
        })
        .unwrap();
        assert!(matches!(
            outcome,
            BackupOutcome::CancelledBeforeCommit | BackupOutcome::CommittedAfterCancellation(_)
        ));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backup_task_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<BackupTask>();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backup_task_without_runtime_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let error = engine
            .create_backup_task(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap_err();
        assert!(error.to_string().contains("requires a Tokio runtime"));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backup_cancellation_handle_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BackupCancellationHandle>();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn backup_cancellation_handle_can_cancel_from_another_thread() {
        let _test_lock = COMMIT_DECISION_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(async {
            let task = engine
                .create_backup_task(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            let token = task.control.barrier_token;
            commit_decision_test_hook::arm(token, 1);
            let cancellation = task.cancellation_handle();
            let join = tokio::spawn(task);
            let entered = tokio::task::spawn_blocking(move || {
                commit_decision_test_hook::wait_until_entered(token, 1)
            });
            entered.await.unwrap();
            std::thread::spawn(move || cancellation.cancel())
                .join()
                .unwrap();
            commit_decision_test_hook::release(token, 1);
            join.await.unwrap()
        })
        .unwrap();
        assert!(matches!(outcome, BackupOutcome::CancelledBeforeCommit));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancellation_before_commit_decision_is_deterministic() {
        let _test_lock = COMMIT_DECISION_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(async {
            let task = engine
                .create_backup_task(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            let token = task.control.barrier_token;
            commit_decision_test_hook::arm(token, 1);
            let cancellation = task.cancellation_handle();
            let join = tokio::spawn(task);
            let entered = tokio::task::spawn_blocking(move || {
                commit_decision_test_hook::wait_until_entered(token, 1)
            });
            entered.await.unwrap();
            cancellation.cancel();
            commit_decision_test_hook::release(token, 1);
            join.await.unwrap()
        })
        .unwrap();
        assert!(matches!(outcome, BackupOutcome::CancelledBeforeCommit));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancellation_after_commit_decision_is_deterministic() {
        let _test_lock = COMMIT_DECISION_TEST_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let outcome = crate::block_on(async {
            let task = engine
                .create_backup_task(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                })
                .unwrap();
            let token = task.control.barrier_token;
            commit_decision_test_hook::arm_after(token, 1);
            let cancellation = task.cancellation_handle();
            let join = tokio::spawn(task);
            let entered = tokio::task::spawn_blocking(move || {
                commit_decision_test_hook::wait_after(token, 1)
            });
            entered.await.unwrap();
            cancellation.cancel();
            commit_decision_test_hook::release_after(token, 1);
            join.await.unwrap()
        })
        .unwrap();
        assert!(matches!(
            outcome,
            BackupOutcome::CommittedAfterCancellation(_)
        ));
        engine.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_async_publishes_generation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"async-key", b"async-value").unwrap();
        let info = committed_async(
            crate::block_on(async {
                let task = engine.create_backup_async(BackupOptions {
                    repository: dir.path().join("repository"),
                    use_hard_links: false,
                });
                task.await
            })
            .unwrap(),
        );
        assert_eq!(info.id, 1);
        engine.close().unwrap();
    }

    #[test]
    fn replay_allows_next_high_water_after_abandoned_reservation() {
        let first = CatalogRecord::HighWater {
            sequence: 1,
            allocated_id: 1,
        };
        let second = CatalogRecord::HighWater {
            sequence: 2,
            allocated_id: 2,
        };
        let first_payload = encode_catalog_payload(&first).unwrap();
        let second_payload = encode_catalog_payload(&second).unwrap();
        let frames = CatalogFrames {
            frames: vec![
                CatalogFrame {
                    record: first,
                    payload: first_payload,
                    start_offset: 0,
                },
                CatalogFrame {
                    record: second,
                    payload: second_payload,
                    start_offset: 1,
                },
            ],
            last_complete_offset: 2,
            torn_tail: false,
        };
        let replay = replay_catalog(&frames).unwrap();
        assert_eq!(replay.high_water_id, 2);
    }

    #[test]
    fn replay_snapshot_preserves_high_water_and_generations() {
        let record = CatalogRecord::Snapshot {
            sequence: 1,
            base_catalog_digest: [0; 32],
            high_water_id: 10,
            committed_generations: vec![CatalogGenerationSnapshot {
                id: 5,
                parent_id: None,
                generation_checksum: [7; 32],
                manifest_snapshot_len: None,
                manifest_snapshot_checksum: None,
                created_at_secs: None,
                logical_bytes: None,
                new_object_bytes: None,
                file_count: None,
            }],
        };
        let payload = encode_catalog_payload(&record).unwrap();
        let frames = CatalogFrames {
            frames: vec![CatalogFrame {
                record,
                payload,
                start_offset: 0,
            }],
            last_complete_offset: 1,
            torn_tail: false,
        };
        let replay = replay_catalog(&frames).unwrap();
        assert_eq!(replay.high_water_id, 10);
        assert_eq!(replay.committed_ids, vec![5]);
        assert_eq!(replay.committed_generations[0].generation_checksum, [7; 32]);
    }

    #[test]
    fn replay_rejects_snapshot_after_catalog_records() {
        let high_water = CatalogRecord::HighWater {
            sequence: 1,
            allocated_id: 1,
        };
        let prepare = CatalogRecord::Prepare {
            sequence: 2,
            id: 1,
            parent_id: None,
            generation_checksum: [7; 32],
        };
        let commit = CatalogRecord::Commit {
            sequence: 3,
            id: 1,
            prepare_sequence: 2,
            prepare_digest: prepare_payload_digest(&encode_catalog_payload(&prepare).unwrap()),
        };
        let snapshot = CatalogRecord::Snapshot {
            sequence: 4,
            base_catalog_digest: [0; 32],
            high_water_id: 1,
            committed_generations: vec![CatalogGenerationSnapshot {
                id: 1,
                parent_id: None,
                generation_checksum: [7; 32],
                manifest_snapshot_len: None,
                manifest_snapshot_checksum: None,
                created_at_secs: None,
                logical_bytes: None,
                new_object_bytes: None,
                file_count: None,
            }],
        };
        let mut bytes = Vec::new();
        for record in [&high_water, &prepare, &commit, &snapshot] {
            append_catalog_record(&mut bytes, record).unwrap();
        }
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        assert!(replay_catalog(&frames).is_err());
    }

    #[test]
    fn replay_rejects_second_or_zero_sequence_snapshot() {
        let snapshot = |sequence| CatalogRecord::Snapshot {
            sequence,
            base_catalog_digest: [0; 32],
            high_water_id: 0,
            committed_generations: Vec::new(),
        };
        let mut bytes = Vec::new();
        append_catalog_record(&mut bytes, &snapshot(7)).unwrap();
        append_catalog_record(&mut bytes, &snapshot(8)).unwrap();
        let frames = read_catalog_records(bytes.as_slice()).unwrap();
        assert!(replay_catalog(&frames).is_err());

        let record = snapshot(0);
        let frames = CatalogFrames {
            frames: vec![CatalogFrame {
                payload: encode_catalog_payload(&record).unwrap(),
                record,
                start_offset: 0,
            }],
            last_complete_offset: 1,
            torn_tail: false,
        };
        assert!(replay_catalog(&frames).is_err());
    }

    #[test]
    fn replay_rejects_snapshot_with_low_high_water() {
        let first = CatalogRecord::Snapshot {
            sequence: 1,
            base_catalog_digest: [0; 32],
            high_water_id: 5,
            committed_generations: vec![CatalogGenerationSnapshot {
                id: 5,
                parent_id: None,
                generation_checksum: [7; 32],
                manifest_snapshot_len: None,
                manifest_snapshot_checksum: None,
                created_at_secs: None,
                logical_bytes: None,
                new_object_bytes: None,
                file_count: None,
            }],
        };
        let second = CatalogRecord::Snapshot {
            sequence: 2,
            base_catalog_digest: [0; 32],
            high_water_id: 4,
            committed_generations: Vec::new(),
        };
        let first_payload = encode_catalog_payload(&first).unwrap();
        let second_payload = encode_catalog_payload(&second).unwrap();
        let frames = CatalogFrames {
            frames: vec![
                CatalogFrame {
                    record: first,
                    payload: first_payload,
                    start_offset: 0,
                },
                CatalogFrame {
                    record: second,
                    payload: second_payload,
                    start_offset: 1,
                },
            ],
            last_complete_offset: 2,
            torn_tail: false,
        };
        assert!(replay_catalog(&frames).is_err());
    }

    #[test]
    fn replay_rejects_snapshot_with_duplicate_generations() {
        let generation = CatalogGenerationSnapshot {
            id: 5,
            parent_id: None,
            generation_checksum: [7; 32],
            manifest_snapshot_len: None,
            manifest_snapshot_checksum: None,
            created_at_secs: None,
            logical_bytes: None,
            new_object_bytes: None,
            file_count: None,
        };
        let record = CatalogRecord::Snapshot {
            sequence: 1,
            base_catalog_digest: [0; 32],
            high_water_id: 5,
            committed_generations: vec![generation.clone(), generation],
        };
        let payload = encode_catalog_payload(&record).unwrap();
        let frames = CatalogFrames {
            frames: vec![CatalogFrame {
                record,
                payload,
                start_offset: 0,
            }],
            last_complete_offset: 1,
            torn_tail: false,
        };
        assert!(replay_catalog(&frames).is_err());
    }

    #[test]
    fn replay_rejects_snapshot_with_invalid_parent_chain() {
        let record = CatalogRecord::Snapshot {
            sequence: 1,
            base_catalog_digest: [0; 32],
            high_water_id: 5,
            committed_generations: vec![CatalogGenerationSnapshot {
                id: 5,
                parent_id: Some(5),
                generation_checksum: [7; 32],
                manifest_snapshot_len: None,
                manifest_snapshot_checksum: None,
                created_at_secs: None,
                logical_bytes: None,
                new_object_bytes: None,
                file_count: None,
            }],
        };
        let payload = encode_catalog_payload(&record).unwrap();
        let frames = CatalogFrames {
            frames: vec![CatalogFrame {
                record,
                payload,
                start_offset: 0,
            }],
            last_complete_offset: 1,
            torn_tail: false,
        };
        assert!(replay_catalog(&frames).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reopen_rejects_missing_retained_generation_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        std::fs::remove_file(
            dir.path()
                .join("repository/generations/1/MANIFEST_SNAPSHOT"),
        )
        .unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reopen_rejects_missing_retained_object() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        let object = std::fs::read_dir(dir.path().join("repository/files"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::remove_file(object).unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reopen_rejects_corrupt_retained_object() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        engine.close().unwrap();
        let object = std::fs::read_dir(dir.path().join("repository/files"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(object, b"corrupt").unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }
}
