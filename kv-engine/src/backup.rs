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
    path::Path,
};

#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationEnvelope {
    version: u8,
    id: u64,
    created_at_secs: u64,
    parent_id: Option<u64>,
    snapshot_len: u64,
    snapshot_checksum: [u8; 32],
    body: Vec<u8>,
}

#[cfg(target_os = "linux")]
pub(crate) struct BackupRepository {
    root: OwnedFd,
    _lock: RepositoryLock,
    replay: CatalogReplay,
    usable: bool,
    pending_prepare: bool,
    pending_prepare_digest: Option<[u8; 32]>,
    pending_generation_checksum: Option<[u8; 32]>,
}

#[cfg(target_os = "linux")]
impl BackupRepository {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
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
                envelope.version == 1,
                "unsupported generation envelope version"
            );
            ensure!(
                envelope.id == committed.id,
                "generation envelope id mismatch"
            );
            ensure!(
                generation_bytes == serde_json::to_vec(&envelope)?,
                "generation envelope is not canonically encoded"
            );
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
        })
    }

    pub(crate) fn high_water_id(&self) -> u64 {
        self.replay.high_water_id
    }

    pub(crate) fn list(&self) -> Result<Vec<u64>> {
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
                envelope.version == 1,
                "unsupported generation envelope version"
            );
            ensure!(
                envelope.id == committed.id,
                "generation envelope id mismatch"
            );
            ensure!(
                generation_bytes == serde_json::to_vec(&envelope)?,
                "generation envelope is not canonically encoded"
            );
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
            generation_checksum,
        });
        self.pending_prepare = false;
        self.pending_prepare_digest = None;
        self.pending_generation_checksum = None;
        Ok(())
    }

    fn stage_generation(
        &self,
        id: u64,
        parent_id: Option<u64>,
        generation: &[u8],
        snapshot: &[u8],
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
        let snapshot_checksum: [u8; 32] = Sha256::digest(snapshot).into();
        let generation = serde_json::to_vec(&GenerationEnvelope {
            version: 1,
            id,
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            parent_id,
            snapshot_len: snapshot.len() as u64,
            snapshot_checksum,
            body: generation.to_vec(),
        })?;
        ensure!(
            generation.len() <= MAX_GENERATION_METADATA_BYTES,
            "generation metadata exceeds limit"
        );
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
        Ok((staging, generation))
    }

    fn publish_staged_generation(&self, id: u64, staging: &str) -> Result<()> {
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
        fsync_fd(&generations).and_then(|_| fsync_fd(&self.root))
    }

    /// Publishes one metadata-only generation in the required durable order.
    pub(crate) fn create_generation(&mut self, generation: &[u8], snapshot: &[u8]) -> Result<u64> {
        let id = self.allocate_backup_id()?;
        let parent_id = self.replay.committed_ids.last().copied();
        let (staging, generation_bytes) =
            self.stage_generation(id, parent_id, generation, snapshot)?;
        let generation_checksum: [u8; 32] = Sha256::digest(&generation_bytes).into();
        let prepare_digest = self.prepare_generation(id, parent_id, generation_checksum)?;
        self.publish_staged_generation(id, &staging)?;
        self.commit_generation(id, prepare_digest)?;
        Ok(id)
    }
}

#[cfg(target_os = "linux")]
fn ensure_regular_file(fd: std::os::fd::RawFd) -> Result<()> {
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
}

pub(crate) struct CommittedGeneration {
    pub(crate) id: u64,
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
        let name = component
            .to_str()
            .ok_or_else(|| anyhow!("repository path is not UTF-8"))?;
        current = openat_no_follow(&current, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
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
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode,
        )
    };
    ensure!(
        fd >= 0,
        "failed to open repository component without following symlinks: {}",
        std::io::Error::last_os_error()
    );
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
    fsync_fd(parent)
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
                    generation_checksum,
                });
                pending = None;
            }
        }
    }
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
    })
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
        let id = opened.allocate_backup_id().unwrap();
        let (staging, generation_bytes) = opened
            .stage_generation(id, None, br#"{"id":1}"#, br#"snapshot"#)
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
        drop(reopened);
        let published = dir.path().join("repository").join("generations").join("1");
        assert_eq!(
            std::fs::read(published.join("GENERATION")).unwrap(),
            generation_bytes
        );
        std::fs::write(published.join("GENERATION"), br#"{"id":2}"#).unwrap();
        assert!(BackupRepository::open(dir.path().join("repository")).is_err());
    }

    #[test]
    fn replay_allows_a_recovered_abandoned_high_water() {
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
        assert_eq!(replay.retained_offset, 2);
    }
}
