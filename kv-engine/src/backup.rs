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
    ffi::CString,
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
    #[serde(default)]
    new_object_bytes: u64,
    snapshot_len: u64,
    snapshot_checksum: [u8; 32],
    #[serde(default)]
    objects: Option<Vec<GenerationObject>>,
    body: Vec<u8>,
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
        let lock = RepositoryLock::acquire(&root, true)?;
        let files = openat_no_follow(&root, "files", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let generations =
            openat_no_follow(&root, "generations", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
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
                validate_generation_objects(&envelope)?;
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
                validate_generation_objects(&envelope)?;
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
            validate_generation_objects(&envelope)?;
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
        let catalog_fd = openat_no_follow(&self.root, "BACKUP_MANIFEST", libc::O_WRONLY, 0)?;
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
        let mut repository = match BackupRepository::open(&repository_path) {
            Ok(repository) => repository,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                match bootstrap_repository(&parent_fd, name) {
                    Ok(()) => BackupRepository::open(&repository_path)?,
                    Err(error)
                        if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                            error.kind() == std::io::ErrorKind::AlreadyExists
                        }) =>
                    {
                        BackupRepository::open(&repository_path)?
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
        | CatalogRecord::Commit { sequence, .. } => *sequence,
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
                matches!(component, std::path::Component::RootDir),
                "repository path must not contain . or .. components"
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
    ensure!(
        result == 0,
        "failed to fsync repository descriptor: {}",
        std::io::Error::last_os_error()
    );
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
    ensure!(
        result == 0,
        "failed to publish backup repository: {}",
        std::io::Error::last_os_error()
    );
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
    ensure!(
        result == 0,
        "failed to create unique repository staging directory: {}",
        std::io::Error::last_os_error()
    );
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
            | CatalogRecord::Commit { sequence, .. } => sequence,
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
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(open_directory_no_follow(&link).is_err());

        let parent = open_directory_no_follow(&real).unwrap();
        std::fs::write(real.join("file"), b"ok").unwrap();
        assert!(openat_no_follow(&parent, "file", libc::O_RDONLY, 0).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bootstrap_publishes_fsynced_repository_layout() {
        let dir = tempfile::tempdir().unwrap();
        let parent = open_directory_no_follow(dir.path()).unwrap();
        bootstrap_repository(&parent, "repository").unwrap();
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
        assert_eq!(infos[0].parent_id, None);
        assert_eq!(infos[0].file_count, 0);
        reopened.verify(1).unwrap();
        reopened.verify_all().unwrap();
        assert!(reopened.verify(2).is_err());
        drop(reopened);
        let published = dir.path().join("repository").join("generations").join("1");
        assert_eq!(
            std::fs::read(published.join("GENERATION")).unwrap(),
            generation_bytes
        );
        std::fs::write(published.join("GENERATION"), br#"{"id":2}"#).unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
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
        let second = engine
            .create_backup(BackupOptions {
                repository: dir.path().join("repository"),
                use_hard_links: false,
            })
            .unwrap();
        assert_eq!(second.id, 2);
        assert_eq!(second.parent_id, Some(1));
        assert_eq!(second.new_object_bytes, 0);
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
