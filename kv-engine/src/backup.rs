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
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_CATALOG_FRAME_BYTES: usize = 1024 * 1024;
const CATALOG_FRAME_HEADER_BYTES: usize = 12;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_RECORDS: usize = 1_000_000;
const CATALOG_FORMAT_VERSION: u8 = 1;
/// Manifest identity format required by RFC 022 backup repositories.
pub const BACKUP_MANIFEST_FORMAT_VERSION: u32 = 6;
const MAX_GENERATION_METADATA_BYTES: usize = 1024 * 1024;
const MAX_REPOSITORY_OBJECT_BYTES: u64 = 128 * 1024 * 1024;
static OBJECT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    body: Vec<u8>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
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
    usable: bool,
    pending_prepare: bool,
    pending_prepare_digest: Option<[u8; 32]>,
    pending_generation_checksum: Option<[u8; 32]>,
    pending_parent_id: Option<Option<u64>>,
}

#[cfg(target_os = "linux")]
impl BackupRepository {
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
        fsync_fd(&files)?;
        fsync_fd(&generations)?;
        let catalog_fd = openat_no_follow(&root, "BACKUP_MANIFEST", libc::O_RDWR, 0)?;
        ensure_regular_file(catalog_fd.as_raw_fd())?;
        let mut catalog = File::from(catalog_fd);
        let frames = read_catalog_records(&mut catalog)?;
        let replay = replay_catalog(&frames)?;
        if let Some(id) = replay.abandoned_generation_id {
            remove_generation_orphan(&generations, id)?;
            fsync_fd(&generations)?;
        }
        remove_uncommitted_generation_orphans(&generations, &replay.committed_ids)?;
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
                matches!(envelope.version, 1..=3),
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
                validate_generation_object_metadata_on_disk(&root, &envelope)?;
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
        if frames.torn_tail || replay.retained_offset < frames.last_complete_offset {
            catalog.set_len(replay.retained_offset)?;
            catalog.sync_all()?;
            fsync_fd(&root)?;
        }
        Ok(Self {
            root,
            _lock: lock,
            replay,
            usable: true,
            pending_prepare: false,
            pending_prepare_digest: None,
            pending_generation_checksum: None,
            pending_parent_id: None,
        })
    }

    pub(crate) fn high_water_id(&self) -> u64 {
        self.replay.high_water_id
    }

    pub fn list(&self) -> Result<Vec<u64>> {
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        for committed in &self.replay.committed_generations {
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
                matches!(envelope.version, 1..=3),
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
        Ok(self.replay.committed_ids.clone())
    }

    pub fn list_info(&self) -> Result<Vec<BackupInfo>> {
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let mut result = Vec::with_capacity(self.replay.committed_generations.len());
        for committed in &self.replay.committed_generations {
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
                matches!(envelope.version, 1..=3),
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
    pub fn latest_id(&self) -> Option<u64> {
        self.replay.committed_ids.last().copied()
    }

    /// Returns the newest `retain` committed generation IDs in ascending order.
    pub fn retained_ids(&self, retain: usize) -> Result<Vec<u64>> {
        ensure!(retain > 0, "retention count must be greater than zero");
        let keep_from = self.replay.committed_ids.len().saturating_sub(retain);
        Ok(self.replay.committed_ids[keep_from..].to_vec())
    }

    /// Returns sorted repository object names referenced by retained generations.
    pub fn retained_object_names(&self, retain: usize) -> Result<Vec<String>> {
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
        Ok((
            self.retained_ids(retain)?,
            self.unreferenced_object_names(retain)?,
        ))
    }

    /// Copies one validated repository object into a restore staging directory.
    fn materialize_object(
        &self,
        object: &GenerationObject,
        target_dir: &OwnedFd,
        target_name: &str,
    ) -> Result<()> {
        ensure!(
            !target_name.is_empty() && !target_name.contains('/'),
            "restore object target must be a basename"
        );
        let files = openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let (size, checksum) = copy_immutable_object(
            &files,
            &object.object_name,
            target_dir,
            target_name,
            object.file_size,
            object.file_checksum,
        )?;
        ensure!(
            size == object.file_size && checksum == object.file_checksum,
            "restored object identity mismatch"
        );
        Ok(())
    }

    /// Materializes every object referenced by a validated generation.
    fn materialize_generation_objects(
        &self,
        envelope: &GenerationEnvelope,
        target_dir: &OwnedFd,
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
        for object in envelope.objects.as_deref().unwrap_or_default() {
            let destination = match object.kind {
                RepositoryObjectKind::Sst => target_dir,
                RepositoryObjectKind::Vlog => vlog_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("vLog restore directory was not created"))?,
            };
            self.materialize_object(object, destination, &object.source_path)?;
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
    pub fn restore(&self, id: u64, target: impl AsRef<Path>) -> Result<RestoreOutcome> {
        ensure!(
            self.replay.committed_ids.contains(&id),
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
        let committed = self
            .replay
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
        ensure!(envelope.id == id, "generation envelope id mismatch");
        ensure!(
            envelope.parent_id == committed.parent_id,
            "generation envelope parent mismatch"
        );
        ensure!(
            matches!(envelope.version, 1..=3),
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
        self.materialize_generation_objects(&envelope, &staging_fd)?;
        Self::write_restore_manifest(&staging_fd, &snapshot)?;
        let durability_error =
            Self::publish_restore_staging(&parent_fd, &staging_name, target_name)?;
        cleanup.disarm();
        match durability_error {
            Some(error) => Ok(RestoreOutcome::PublishedButNotDurable {
                target: target.to_path_buf(),
                error,
            }),
            None => Ok(RestoreOutcome::Restored),
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
    ) -> Result<(Vec<GenerationObject>, u64, u64)> {
        ensure!(
            capture.immutable_file_metadata.len() == capture.sst_ids.len() + capture.vlog_ids.len(),
            "capture immutable metadata is incomplete"
        );
        let mut identities = HashSet::new();
        for identity in &capture.immutable_file_metadata {
            ensure!(
                identities.insert((identity.kind, identity.file_id)),
                "capture immutable metadata contains duplicates"
            );
            ensure!(
                match identity.kind {
                    crate::manifest::ImmutableFileKind::Sst =>
                        capture.sst_ids.contains(&(identity.file_id as usize)),
                    crate::manifest::ImmutableFileKind::Vlog =>
                        capture.vlog_ids.contains(&(identity.file_id as u32)),
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
        for identity in &capture.immutable_file_metadata {
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
            if self.publish_object(
                directory,
                &name,
                kind,
                identity.file_id,
                identity.file_size,
                identity.file_checksum,
                use_hard_links,
            )? {
                reused = reused
                    .checked_add(identity.file_size)
                    .ok_or_else(|| anyhow!("reused byte count overflow"))?;
            } else {
                published = published
                    .checked_add(identity.file_size)
                    .ok_or_else(|| anyhow!("published byte count overflow"))?;
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
        objects.sort_by(|left, right| left.object_name.cmp(&right.object_name));
        Ok((objects, published, reused))
    }

    pub fn verify(&self, id: u64) -> Result<()> {
        let generations = openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        let committed = self
            .replay
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
                && matches!(envelope.version, 1..=3),
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
        Ok(())
    }

    /// Verifies every committed generation and all referenced immutable objects.
    pub fn verify_all(&self) -> Result<()> {
        for id in &self.replay.committed_ids {
            self.verify(*id)
                .with_context(|| format!("backup generation {id} failed verification"))?;
        }
        Ok(())
    }

    /// Reserves the next backup ID durably while the repository's exclusive
    /// lock is held. Abandoned reservations are intentionally never reused.
    pub(crate) fn allocate_backup_id(&mut self) -> Result<u64> {
        ensure!(
            self.usable,
            "backup repository is invalidated; reopen it before retrying"
        );
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
                self.usable = false;
                return Err(error);
            }
        };
        let mut catalog = File::from(catalog_fd);
        if let Err(error) = catalog.seek(SeekFrom::End(0)) {
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(
            &mut catalog,
            &CatalogRecord::HighWater {
                sequence,
                allocated_id: id,
            },
        ) {
            self.usable = false;
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable = false;
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
        ensure!(
            self.usable,
            "backup repository is invalidated; reopen it before retrying"
        );
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
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(&mut catalog, &record) {
            self.usable = false;
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable = false;
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

    pub(crate) fn commit_generation(&mut self, id: u64, prepare_digest: [u8; 32]) -> Result<()> {
        ensure!(
            self.usable && self.pending_prepare,
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
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(&mut catalog, &record) {
            self.usable = false;
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable = false;
            return Err(error);
        }
        self.replay.last_sequence = record_sequence(&record);
        self.replay.committed_ids.push(id);
        self.replay.committed_generations.push(CommittedGeneration {
            id,
            parent_id,
            generation_checksum,
        });
        self.pending_prepare = false;
        self.pending_prepare_digest = None;
        self.pending_generation_checksum = None;
        self.pending_parent_id = None;
        Ok(())
    }

    pub(crate) fn publish_retention(&mut self, retained_ids: &[u64]) -> Result<()> {
        ensure!(
            self.usable,
            "backup repository is invalidated; reopen it before retrying"
        );
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
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = append_catalog_record(&mut catalog, &record) {
            self.usable = false;
            return Err(error);
        }
        if let Err(error) = catalog.sync_all() {
            self.usable = false;
            return Err(error.into());
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable = false;
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
        ensure!(
            self.usable,
            "backup repository is invalidated; reopen it before retrying"
        );
        ensure!(
            !self.pending_prepare,
            "backup repository has an uncommitted generation"
        );
        let snapshot = CatalogRecord::Snapshot {
            sequence: 1,
            high_water_id: self.replay.high_water_id,
            committed_generations: self
                .replay
                .committed_generations
                .iter()
                .map(|generation| CatalogGenerationSnapshot {
                    id: generation.id,
                    parent_id: generation.parent_id,
                    generation_checksum: generation.generation_checksum,
                })
                .collect(),
        };
        let (temp_name, temp_fd) = (0..32)
            .find_map(|_| {
                let name = format!(
                    ".BACKUP_MANIFEST.compact-{}-{}",
                    std::process::id(),
                    OBJECT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                );
                match openat_no_follow(
                    &self.root,
                    &name,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                    0o600,
                ) {
                    Ok(fd) => Some(Ok((name, fd))),
                    Err(error)
                        if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                            error.kind() == std::io::ErrorKind::AlreadyExists
                        }) =>
                    {
                        None
                    }
                    Err(error) => Some(Err(error)),
                }
            })
            .transpose()?
            .ok_or_else(|| anyhow!("failed to allocate unique catalog compaction temp file"))?;
        let mut cleanup = TempObjectCleanup {
            directory: &self.root,
            name: temp_name.clone(),
        };
        let mut temp = File::from(temp_fd);
        append_catalog_record(&mut temp, &snapshot)?;
        temp.sync_all()?;
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
            self.usable = false;
            return Err(std::io::Error::last_os_error().into());
        }
        cleanup.disarm();
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("backup.compact.after_manifest_replace");
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable = false;
            return Err(error);
        }
        self.replay.last_sequence = 1;
        self.replay.retained_offset = 0;
        Ok(())
    }

    /// Compacts the append-only backup catalog into a single snapshot record.
    pub fn compact(&mut self) -> Result<()> {
        self.compact_catalog()
    }

    pub fn purge(&mut self, retain: usize) -> Result<()> {
        ensure!(
            self.usable,
            "backup repository is invalidated; reopen it before retrying"
        );
        let retained = self.retained_ids(retain)?;
        let unreferenced = self.unreferenced_object_names(retain)?;
        let removed_generations = self
            .replay
            .committed_ids
            .iter()
            .copied()
            .filter(|id| !retained.contains(id))
            .collect::<Vec<_>>();
        if !removed_generations.is_empty() {
            self.publish_retention(&retained)?;
        }
        let generations = match openat_no_follow(
            &self.root,
            "generations",
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        ) {
            Ok(fd) => fd,
            Err(error) => {
                self.usable = false;
                return Err(error);
            }
        };
        for id in removed_generations {
            if let Err(error) = remove_generation_directory(&generations, id) {
                self.usable = false;
                return Err(error);
            }
        }
        if let Err(error) = fsync_fd(&generations) {
            self.usable = false;
            return Err(error);
        }
        let files =
            match openat_no_follow(&self.root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0) {
                Ok(fd) => fd,
                Err(error) => {
                    self.usable = false;
                    return Err(error);
                }
            };
        for name in unreferenced {
            if let Err(error) = validate_object_before_reclaim(&files, &name) {
                self.usable = false;
                return Err(error);
            }
            let name = CString::new(name)?;
            // SAFETY: files is trusted and names came from validated entries.
            let result = unsafe { libc::unlinkat(files.as_raw_fd(), name.as_ptr(), 0) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::NotFound {
                    self.usable = false;
                    return Err(error.into());
                }
            }
        }
        if let Err(error) = fsync_fd(&files) {
            self.usable = false;
            return Err(error);
        }
        if let Err(error) = fsync_fd(&self.root) {
            self.usable = false;
            return Err(error);
        }
        Ok(())
    }

    fn stage_generation(
        &self,
        id: u64,
        parent_id: Option<u64>,
        generation: &[u8],
        snapshot: &[u8],
        objects: &[GenerationObject],
        new_object_bytes: u64,
    ) -> Result<(String, Vec<u8>)> {
        ensure!(
            self.usable,
            "backup repository is invalidated; reopen it before retrying"
        );
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
            version: 3,
            id,
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            parent_id,
            new_object_bytes,
            snapshot_len: snapshot.len() as u64,
            snapshot_checksum,
            objects: Some(objects.to_vec()),
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
            self.usable = false;
            return Err(error);
        }
        Ok(())
    }

    /// Publishes one metadata-only generation in the required durable order.
    pub(crate) fn create_generation(&mut self, generation: &[u8], snapshot: &[u8]) -> Result<u64> {
        self.create_generation_with_objects(generation, snapshot, &[], 0)
    }

    fn create_generation_with_objects(
        &mut self,
        generation: &[u8],
        snapshot: &[u8],
        objects: &[GenerationObject],
        new_object_bytes: u64,
    ) -> Result<u64> {
        let id = self.allocate_backup_id()?;
        let parent_id = self.replay.committed_ids.last().copied();
        let (staging, generation_bytes) = self.stage_generation(
            id,
            parent_id,
            generation,
            snapshot,
            objects,
            new_object_bytes,
        )?;
        let generation_checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        let prepare_digest = match self.prepare_generation(id, parent_id, generation_checksum) {
            Ok(digest) => digest,
            Err(error) => {
                cleanup_staging_generation(&self.root, &staging);
                return Err(error);
            }
        };
        if let Err(error) = self.publish_staged_generation(id, &staging) {
            cleanup_staging_generation(&self.root, &staging);
            return Err(error);
        }
        self.commit_generation(id, prepare_digest)?;
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
impl crate::lsm_storage::KvEngine {
    pub fn create_backup(&self, options: BackupOptions) -> Result<BackupInfo> {
        let _lifecycle_guard = self.inner.lifecycle.admit_write()?;
        self.inner.create_backup_inner(options)
    }

    pub async fn create_backup_async(&self, options: BackupOptions) -> Result<BackupInfo> {
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
}

impl crate::lsm_storage::LsmStorageInner {
    fn create_backup_inner(&self, options: BackupOptions) -> Result<BackupInfo> {
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
        let (objects, new_object_bytes, _) =
            repository.publish_capture_objects(self, &capture, use_hard_links)?;
        let snapshot = serde_json::to_vec(&capture.snapshot_record)?;
        let id = repository.create_generation_with_objects(
            &snapshot,
            &snapshot,
            &objects,
            new_object_bytes,
        )?;
        repository
            .list_info()?
            .into_iter()
            .find(|info| info.id == id)
            .ok_or_else(|| anyhow!("committed backup generation is missing from catalog"))
    }
}

#[cfg(target_os = "linux")]
fn cleanup_staging_generation(root: &OwnedFd, staging: &str) {
    let Ok(generations) =
        openat_no_follow(root, "generations", libc::O_RDONLY | libc::O_DIRECTORY, 0)
    else {
        return;
    };
    let Ok(generation) =
        openat_no_follow(&generations, staging, libc::O_RDONLY | libc::O_DIRECTORY, 0)
    else {
        return;
    };
    for name in ["GENERATION", "MANIFEST_SNAPSHOT"] {
        let name = CString::new(name).unwrap();
        // SAFETY: generation is a trusted descriptor and name is fixed.
        unsafe {
            libc::unlinkat(generation.as_raw_fd(), name.as_ptr(), 0);
        }
    }
    let name = CString::new(staging).unwrap();
    // SAFETY: generations is trusted and staging is a generated basename.
    unsafe {
        libc::unlinkat(generations.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR);
    }
    let _ = fsync_fd(&generations);
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
        if !self.name.is_empty() {
            cleanup_staging_generation(self.root, &self.name);
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

fn validate_restore_snapshot_objects(envelope: &GenerationEnvelope, snapshot: &[u8]) -> Result<()> {
    let record: crate::manifest::ManifestRecord =
        serde_json::from_slice(snapshot).context("invalid restore manifest snapshot")?;
    let crate::manifest::ManifestRecord::Snapshot {
        immutable_file_metadata,
        ..
    } = record
    else {
        bail!("restore manifest must be a snapshot record");
    };
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
}

#[cfg(target_os = "linux")]
pub(crate) struct RepositoryLock {
    _fd: OwnedFd,
}

#[cfg(target_os = "linux")]
impl RepositoryLock {
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
    ensure!(
        !target_name.is_empty()
            && target_name != "."
            && target_name != ".."
            && !target_name.contains('/'),
        "repository object name must be a single basename"
    );
    let source = File::from(openat_no_follow(
        source_dir,
        source_name,
        libc::O_RDONLY,
        0,
    )?);
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
        high_water_id: u64,
        committed_generations: Vec<CatalogGenerationSnapshot>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogGenerationSnapshot {
    pub(crate) id: u64,
    pub(crate) parent_id: Option<u64>,
    pub(crate) generation_checksum: [u8; 32],
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

    for (index, frame) in frames.frames.iter().enumerate() {
        let expected_sequence = u64::try_from(index + 1)?;
        let sequence = match &frame.record {
            CatalogRecord::HighWater { sequence, .. }
            | CatalogRecord::Prepare { sequence, .. }
            | CatalogRecord::Commit { sequence, .. }
            | CatalogRecord::Retention { sequence, .. }
            | CatalogRecord::Snapshot { sequence, .. } => sequence,
        };
        ensure!(
            *sequence == expected_sequence,
            "backup catalog sequence is not strictly monotonic"
        );
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
        _ => (
            frames.last_complete_offset,
            u64::try_from(frames.frames.len())?,
        ),
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

    #[test]
    fn catalog_round_trip_and_torn_tail() {
        assert_eq!(BACKUP_MANIFEST_FORMAT_VERSION, 6);
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

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn compact_catalog_temp_sync_failpoint_recovers() {
        use crate::chaos::failpoint::{self, FailScenario};
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
        drop(repository);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert!(reopened.list().unwrap().is_empty());
        scenario.teardown();
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn compact_catalog_after_replace_failpoint_reopens() {
        use crate::chaos::failpoint::{self, FailScenario};
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
        let (staging, generation_bytes) = opened
            .stage_generation(id, None, br#"{"id":1}"#, br#"snapshot"#, &[], 0)
            .unwrap();
        let generation_checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        let digest = opened
            .prepare_generation(id, None, generation_checksum)
            .unwrap();
        opened.publish_staged_generation(id, &staging).unwrap();
        opened.commit_generation(id, digest).unwrap();
        drop(opened);
        let reopened = BackupRepository::open(dir.path().join("repository")).unwrap();
        assert_eq!(reopened.high_water_id(), 1);
        assert_eq!(reopened.list().unwrap(), vec![1]);
        let infos = reopened.list_info().unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].id, 1);
        assert_eq!(reopened.info(1).unwrap().id, 1);
        assert_eq!(reopened.latest_info().unwrap().unwrap().id, 1);
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
        assert_eq!(reopened.list().unwrap(), vec![1]);
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
            body: Vec::new(),
        };
        assert!(validate_generation_objects(&valid).is_ok());
        let mut invalid = valid;
        invalid.objects.as_mut().unwrap()[0].source_path = "../escape".into();
        assert!(validate_generation_objects(&invalid).is_err());
    }

    #[test]
    fn legacy_v2_envelope_without_accounting_field_remains_canonical() {
        let legacy = br#"{"version":2,"id":7,"created_at_secs":9,"parent_id":null,"snapshot_len":0,"snapshot_checksum":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"objects":null,"body":[]}"#;
        let envelope: GenerationEnvelope = serde_json::from_slice(legacy).unwrap();
        assert_eq!(envelope.new_object_bytes, 0);
        assert_eq!(serde_json::to_vec(&envelope).unwrap(), legacy);
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
            body: Vec::new(),
        };
        validate_generation_objects_on_disk(&root, &envelope).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn engine_create_backup_publishes_captured_generation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"key", b"value").unwrap();
        let info = engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
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
        let second = engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        assert_eq!(second.id, 2);
        assert_eq!(second.parent_id, Some(1));
        assert_eq!(second.new_object_bytes, 0);
        let mut repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        repository.purge(1).unwrap();
        assert_eq!(repository.list().unwrap(), vec![2]);
        repository.compact().unwrap();
        assert_eq!(repository.list().unwrap(), vec![2]);
        assert!(!dir.path().join("repository/generations/1").exists());
        repository.purge(1).unwrap();
        assert_eq!(repository.list().unwrap(), vec![2]);
        drop(repository);
        engine.close().unwrap();
        let repository = BackupRepository::open(dir.path().join("repository")).unwrap();
        let outcome = repository.restore(2, dir.path().join("restored")).unwrap();
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
    fn engine_create_backup_async_publishes_generation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path().join("db"),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"async-key", b"async-value").unwrap();
        let info = crate::block_on(engine.create_backup_async(BackupOptions {
            repository: dir.path().join("repository"),
            use_hard_links: false,
        }))
        .unwrap();
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
}
