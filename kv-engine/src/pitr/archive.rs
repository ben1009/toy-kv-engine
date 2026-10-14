//! PITR archive-object publication and durable catalog support.
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
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::pitr::catalog::{PitrCatalogRecord, SegmentMetadata, encode_catalog, replay_catalog};
use crate::pitr::{ArchiveEpochId, SegmentId, TimelineId};

#[cfg(target_os = "linux")]
static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
    segment_metadata: SegmentMetadata,
    /// The rule the segment's `wal_digest` follows. Carried from the metadata the
    /// objects were prepared from so publishing re-checks the WAL under the same
    /// rule the catalog recorded.
    wal_digest_policy: SourceObjectDigest,
}

/// How a source object's bytes are digested. v5/v6 WALs use their recorded
/// coverage rule, v7 WALs use the frozen image's whole-object digest, and seal
/// sidecars are always hashed whole.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceObjectDigest {
    WholeObject,
    Wal(crate::pitr::WalDigestRule),
}

impl SourceObjectDigest {
    /// Select the digest policy recorded by a segment's WAL format.
    pub(crate) fn for_wal_format(version: u16) -> anyhow::Result<Self> {
        if version == crate::wal::format::WAL_FORMAT_VERSION_V7 {
            Ok(Self::WholeObject)
        } else {
            Ok(Self::Wal(crate::pitr::wal_digest_rule(version)?))
        }
    }

    /// Digest `bytes` the way this object's name was derived.
    pub(crate) fn digest(self, bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
        match self {
            Self::WholeObject => Ok(sha2::Sha256::digest(bytes).into()),
            Self::Wal(rule) => crate::pitr::seal::wal_digest(bytes, rule),
        }
    }
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
    root: File,
    wal_dir: File,
    wal_path: PathBuf,
}

#[cfg(target_os = "linux")]
impl Drop for ArchiveObjectStager {
    fn drop(&mut self) {
        self.sweep_abandoned_staging();
    }
}

#[cfg(target_os = "linux")]
const REPOSITORY_LOCK_FILE: &str = "LOCK";

#[cfg(target_os = "linux")]
pub(crate) struct ArchiveLockGuard {
    lock: File,
}

#[cfg(target_os = "linux")]
impl Drop for ArchiveLockGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.lock.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(target_os = "linux")]
impl ArchiveObjectStager {
    /// Open and exclusively lock the repository `LOCK` file.
    ///
    /// Every acquisition opens a fresh descriptor on purpose. `flock` locks belong to the open file
    /// description, so duplicating an already opened descriptor (`try_clone`, `dup`, `fork`) shares
    /// a single lock instead of serializing the callers: two threads would both "acquire" it, and
    /// either thread's `LOCK_UN` would release it while the other still assumed it held the lock.
    fn acquire_repository_lock(root: &File) -> anyhow::Result<File> {
        let name = CString::new(REPOSITORY_LOCK_FILE)?;
        let fd = unsafe {
            libc::openat(
                root.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        anyhow::ensure!(fd >= 0, std::io::Error::last_os_error());
        let lock = unsafe { File::from_raw_fd(fd) };
        crate::backup::ensure_regular_file(lock.as_raw_fd())?;
        let result = loop {
            // SAFETY: `lock` owns a valid descriptor and `flock` does not retain any pointers.
            let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
            if result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break result;
            }
        };
        anyhow::ensure!(
            result == 0,
            "failed to acquire PITR archive repository lock"
        );

        Ok(lock)
    }

    /// Takes the repository lock only if it is free, reporting a busy repository
    /// as `None` instead of waiting for it.
    ///
    /// `flock` conflicts on distinct descriptors, and the same LOCK file is held
    /// for the whole lifetime of a `BackupRepository` as well as for the duration
    /// of a peer's publication - both of which can run for minutes. A caller that
    /// would otherwise block on that must use this.
    fn try_acquire_repository_lock(root: &File) -> anyhow::Result<Option<File>> {
        let name = CString::new(REPOSITORY_LOCK_FILE)?;
        let fd = unsafe {
            libc::openat(
                root.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        anyhow::ensure!(fd >= 0, std::io::Error::last_os_error());
        let lock = unsafe { File::from_raw_fd(fd) };
        crate::backup::ensure_regular_file(lock.as_raw_fd())?;
        let result = loop {
            // SAFETY: `lock` owns a valid descriptor and `flock` does not retain any pointers.
            let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break result;
            }
        };
        if result == 0 {
            return Ok(Some(lock));
        }
        let error = std::io::Error::last_os_error();

        if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
            return Ok(None);
        }

        Err(error.into())
    }

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
        let wal_path = root.join("wal");
        // Sweep only while nothing else can be publishing. Another holder of this
        // LOCK - a backup, or a peer's enable - can hold it for minutes, and this
        // call must not wait on that: attaching PITR is not allowed to block
        // behind somebody else's publication. Skipping costs only disk, because
        // the same names are swept on the next construction.
        let mut removed_staging = false;

        if let Some(_lock) = Self::try_acquire_repository_lock(&root_fd)? {
            for entry in std::fs::read_dir(&wal_path)? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if is_archive_temp_name(name) {
                    // Only regular files are ours to remove. A directory matching
                    // the staging shape makes `unlinkat` fail with EISDIR, and this
                    // loop reports that as an error: PITR could not be attached
                    // until someone removed the entry by hand.
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let name = CString::new(name)?;
                    let result = unsafe { libc::unlinkat(wal_fd.as_raw_fd(), name.as_ptr(), 0) };
                    if result != 0
                        && std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound
                    {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    removed_staging = true;
                }
            }
        }
        if removed_staging {
            sync_fd(&wal_fd)?;
        }
        if created {
            sync_fd(&root_fd)?;
        }
        Ok(Self {
            root: root_fd,
            wal_dir: wal_fd,
            wal_path,
        })
    }

    pub(crate) fn staging_bytes(&self) -> u64 {
        Self::staging_bytes_at(&self.wal_path)
    }

    pub(crate) fn staging_bytes_at(wal_path: &Path) -> u64 {
        std::fs::read_dir(wal_path)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_str().is_some_and(is_archive_temp_name))
            .filter_map(|entry| entry.metadata().ok().map(|metadata| metadata.len()))
            .sum()
    }

    pub(crate) fn lock_exclusive(&self) -> anyhow::Result<ArchiveLockGuard> {
        Ok(ArchiveLockGuard {
            lock: Self::acquire_repository_lock(&self.root)?,
        })
    }

    /// Takes the repository lock only if it is free. Lets a caller that must not
    /// block tell an idle repository from one with a publication in flight.
    pub(crate) fn try_lock_exclusive(&self) -> anyhow::Result<Option<ArchiveLockGuard>> {
        Ok(Self::try_acquire_repository_lock(&self.root)?.map(|lock| ArchiveLockGuard { lock }))
    }

    /// Removes staging files this repository left behind, if nothing can be
    /// publishing right now.
    ///
    /// Staging names carry no owner - a concurrent stager for the same repository
    /// writes names of the same shape - so sweeping without the repository lock
    /// could unlink a live transaction's temp file. Skipping instead costs only
    /// disk: `new` sweeps the same names on the next construction, and engine open
    /// sweeps them too.
    fn sweep_abandoned_staging(&self) {
        let Ok(Some(_lock)) = self.try_lock_exclusive() else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(&self.wal_path) else {
            return;
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !is_archive_temp_name(name) {
                continue;
            }
            let Ok(name) = CString::new(name) else {
                continue;
            };
            // SAFETY: `wal_dir` owns a valid descriptor for the directory the entry
            // came from, and `unlinkat` does not retain the pointer.
            unsafe { libc::unlinkat(self.wal_dir.as_raw_fd(), name.as_ptr(), 0) };
        }
    }

    pub(crate) fn publish(
        &self,
        prepared: &PreparedArchiveObjects,
        wal: &[u8],
        seal: &[u8],
    ) -> anyhow::Result<()> {
        self.publish_with_priority(prepared, wal, seal, None)
    }

    pub(crate) fn publish_with_priority(
        &self,
        prepared: &PreparedArchiveObjects,
        wal: &[u8],
        seal: &[u8],
        priority: Option<&parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
    ) -> anyhow::Result<()> {
        let _lock = self.lock_exclusive()?;

        self.publish_with_priority_unlocked(prepared, wal, seal, priority)
    }

    pub(crate) fn publish_with_priority_unlocked(
        &self,
        prepared: &PreparedArchiveObjects,
        wal: &[u8],
        seal: &[u8],
        priority: Option<&parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
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
            prepared.wal_digest_policy.digest(wal)?.as_slice()
                == prepared.segment_metadata.wal_digest,
            "prepared WAL digest mismatch"
        );
        anyhow::ensure!(
            Sha256::digest(seal).as_slice() == prepared.segment_metadata.seal_digest,
            "prepared seal digest mismatch"
        );
        let mut before_chunk = |_: u64| {
            if priority.is_some_and(|priority| {
                *priority.lock() == crate::pitr::api::ArchiveIoPriority::Background
            }) {
                std::thread::yield_now();
            }
            Ok(())
        };
        publish_one_chunked(
            &self.wal_dir,
            &prepared.wal_name,
            wal,
            64 * 1024,
            priority,
            &mut before_chunk,
        )?;
        publish_one_chunked(
            &self.wal_dir,
            &prepared.seal_name,
            seal,
            64 * 1024,
            priority,
            &mut before_chunk,
        )?;
        sync_fd(&self.wal_dir)?;

        Ok(())
    }

    /// Publishes prepared archive objects in throttled chunks.
    ///
    /// The caller is expected to hold the repository lock so chunk scheduling and
    /// catalog publication stay serialized with other archive operations.
    pub(crate) fn publish_chunked(
        &self,
        prepared: &PreparedArchiveObjects,
        wal: &[u8],
        seal: &[u8],
        chunk_bytes: usize,
        priority: Option<&parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
        mut before_chunk: impl FnMut(u64) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(chunk_bytes > 0, "archive chunk size is zero");
        anyhow::ensure!(
            prepared.wal_bytes == wal.len() as u64 && prepared.seal_bytes == seal.len() as u64,
            "prepared archive object length mismatch"
        );
        anyhow::ensure!(
            prepared.wal_digest_policy.digest(wal)?.as_slice()
                == prepared.segment_metadata.wal_digest
                && SourceObjectDigest::WholeObject.digest(seal)?.as_slice()
                    == prepared.segment_metadata.seal_digest,
            "prepared archive object digest mismatch"
        );
        publish_one_chunked(
            &self.wal_dir,
            &prepared.wal_name,
            wal,
            chunk_bytes,
            priority,
            &mut before_chunk,
        )?;
        publish_one_chunked(
            &self.wal_dir,
            &prepared.seal_name,
            seal,
            chunk_bytes,
            priority,
            &mut before_chunk,
        )?;
        sync_fd(&self.wal_dir)?;

        Ok(())
    }

    pub(crate) fn read(&self, name: &str, expected_bytes: Option<u64>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(
            !name.is_empty()
                && !name.contains('/')
                && !name.contains('\\')
                && name != "."
                && name != "..",
            "archive object name is not a single component"
        );
        let name = CString::new(name)?;
        let file = open_existing(&self.wal_dir, &name)?;
        let mut bytes = Vec::new();
        let limit = expected_bytes
            .map(|bytes| bytes.saturating_add(1))
            .unwrap_or(u64::MAX);
        (&file).take(limit).read_to_end(&mut bytes)?;

        if let Some(expected_bytes) = expected_bytes {
            anyhow::ensure!(
                bytes.len() as u64 == expected_bytes,
                "archive object length exceeds expected size"
            );
        }
        Ok(bytes)
    }
}

#[cfg(target_os = "linux")]
fn publish_one_chunked(
    directory: &File,
    name: &str,
    bytes: &[u8],
    chunk_bytes: usize,
    priority: Option<&parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
    before_chunk: &mut impl FnMut(u64) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let final_name = CString::new(name)?;
    if let Ok(existing) = open_existing(directory, &final_name) {
        let existing_bytes =
            read_bounded_file(&existing, (bytes.len() as u64).saturating_add(1), priority)?;
        anyhow::ensure!(
            existing_bytes == bytes,
            "existing archive object identity mismatch"
        );
        for chunk in bytes.chunks(chunk_bytes) {
            before_chunk(chunk.len() as u64)?;
        }
        return Ok(());
    }
    let (temp_name, mut temp) = create_temp(directory, name)?;
    let mut temp_consumed = false;
    let result = (|| -> anyhow::Result<()> {
        for chunk in bytes.chunks(chunk_bytes) {
            before_chunk(chunk.len() as u64)?;
            temp.write_all(chunk)?;
        }
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
            let existing_bytes =
                read_bounded_file(&existing, (bytes.len() as u64).saturating_add(1), priority)?;
            anyhow::ensure!(
                existing_bytes == bytes,
                "concurrent archive object identity mismatch"
            );
        } else {
            temp_consumed = true;
            #[cfg(test)]
            if std::env::var_os("PITR_PROCESS_KILL_AFTER_OBJECT_RENAME").is_some() {
                // SAFETY: this is an isolated child-process crash test at the
                // object-publication boundary.
                unsafe { libc::_exit(137) }
            }
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
fn read_bounded_file(
    file: &File,
    limit: u64,
    priority: Option<&parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut reader = file.take(limit);

    loop {
        if priority.is_some_and(|priority| {
            *priority.lock() == crate::pitr::api::ArchiveIoPriority::Background
        }) {
            std::thread::yield_now();
        }
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn write_chunked(
    file: &mut File,
    bytes: &[u8],
    priority: Option<&parking_lot::Mutex<crate::pitr::api::ArchiveIoPriority>>,
) -> anyhow::Result<()> {
    for chunk in bytes.chunks(64 * 1024) {
        if priority.is_some_and(|priority| {
            *priority.lock() == crate::pitr::api::ArchiveIoPriority::Background
        }) {
            std::thread::yield_now();
        }
        file.write_all(chunk)?;
    }
    Ok(())
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
fn is_archive_temp_name(name: &str) -> bool {
    let Some(name) = name.strip_prefix('.') else {
        return false;
    };
    let Some((object, suffix)) = name.split_once(".tmp-") else {
        return false;
    };
    let Some((stem, extension)) = object.rsplit_once('.') else {
        return false;
    };
    let fields = stem.split('-').collect::<Vec<_>>();
    if fields.len() != 4
        || extension != "wal" && extension != "seal"
        || fields[0].len() != 32
        || fields[1].len() != 32
        || fields[2].len() != 16
        || fields[3].len() != 64
        || fields.iter().any(|field| {
            !field
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
    {
        return false;
    }
    let mut suffix = suffix.split('-');

    (object.ends_with(".wal") || object.ends_with(".seal"))
        && suffix
            .next()
            .is_some_and(|pid| !pid.is_empty() && pid.bytes().all(|byte| byte.is_ascii_digit()))
        && suffix.next().is_some_and(|sequence| {
            !sequence.is_empty() && sequence.bytes().all(|byte| byte.is_ascii_digit())
        })
        && suffix.next().is_none()
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
        metadata.validate()?;
        anyhow::ensure!(!seal.is_empty(), "archived seal must be nonempty");
        anyhow::ensure!(
            metadata.wal_bytes == wal.len() as u64,
            "archived WAL length mismatch"
        );
        let wal_digest_policy = SourceObjectDigest::for_wal_format(metadata.wal_format_version)?;
        anyhow::ensure!(
            wal_digest_policy.digest(wal)?.as_slice() == metadata.wal_digest,
            "archived WAL digest mismatch"
        );
        anyhow::ensure!(
            Sha256::digest(seal).as_slice() == metadata.seal_digest,
            "archived seal digest mismatch"
        );
        if metadata.wal_format_version == crate::wal::format::WAL_FORMAT_VERSION_V7 {
            verify_v7_archive_objects(metadata, wal, seal)?;
        }

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
            segment_metadata: metadata.clone(),
            wal_digest_policy,
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
            prepared.segment_metadata == metadata,
            "prepared segment metadata does not match segment"
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

pub(crate) fn verify_v7_archive_objects(
    metadata: &SegmentMetadata,
    wal: &[u8],
    seal_bytes: &[u8],
) -> anyhow::Result<()> {
    let seal = decode_v7_archive_seal(metadata, seal_bytes)?;
    seal.verify_wal(wal)?;

    Ok(())
}

/// Decode and bind a v7 seal sidecar to its catalog descriptor.
///
/// Repository metadata consumers can use this when they need the seal index
/// without loading the whole WAL image. Archive preparation additionally calls
/// `verify_wal` before publishing or accepting the frozen object pair.
pub(crate) fn decode_v7_archive_seal(
    metadata: &SegmentMetadata,
    seal_bytes: &[u8],
) -> anyhow::Result<crate::pitr::seal::V7Seal> {
    metadata.validate()?;
    anyhow::ensure!(
        metadata.wal_format_version == crate::wal::format::WAL_FORMAT_VERSION_V7,
        "segment is not a v7 WAL"
    );
    anyhow::ensure!(
        Sha256::digest(seal_bytes).as_slice() == metadata.seal_digest,
        "v7 archive seal digest mismatch"
    );
    let boundary = metadata
        .immutable_boundary
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("v7 archive descriptor has no immutable boundary"))?;
    let seal = crate::pitr::seal::V7Seal::decode(seal_bytes, metadata.wal_bytes)?;
    anyhow::ensure!(
        seal.boundary == *boundary,
        "v7 archive seal boundary mismatch"
    );
    anyhow::ensure!(
        seal.header.timeline_id == metadata.key.timeline_id
            && seal.header.archive_epoch_id == metadata.key.archive_epoch_id
            && seal.header.segment_id == metadata.key.segment_id
            && seal.header.predecessor == metadata.predecessor,
        "v7 archive seal identity mismatch"
    );
    anyhow::ensure!(
        seal.entries.len() as u64 == metadata.batch_count
            && seal.entries.first().map(|entry| entry.commit_ts) == metadata.first_commit_ts
            && seal.entries.last().map(|entry| entry.commit_ts) == metadata.last_commit_ts,
        "v7 archive seal commit range mismatch"
    );

    Ok(seal)
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
            key: crate::pitr::catalog::SegmentKey {
                repository_id: [1; 16],
                timeline_id: TimelineId([2; 16]),
                archive_epoch_id: ArchiveEpochId([3; 16]),
                segment_id: SegmentId(1),
            },
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            seal_format_version: 1,
            immutable_boundary: None,
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

    fn v7_archive_objects() -> (SegmentMetadata, Vec<u8>, Vec<u8>) {
        v7_archive_objects_with_batches(
            SegmentId(4),
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
            &[],
        )
    }

    fn v7_archive_objects_with_batches(
        segment_id: SegmentId,
        predecessor: ChainAnchor,
        batches: &[crate::pitr::WalBatch],
    ) -> (SegmentMetadata, Vec<u8>, Vec<u8>) {
        use crate::wal::v7::codec::{
            WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7Frontier, WalV7Header, data_fragment_count,
            encode_data_frames_into, encode_frontier_successor, encode_generation_zero_frontier,
            encode_logical_batch, frame_record_digest,
        };

        let key = crate::pitr::catalog::SegmentKey {
            repository_id: [1; 16],
            timeline_id: TimelineId([2; 16]),
            archive_epoch_id: ArchiveEpochId([3; 16]),
            segment_id,
        };
        let header = WalV7Header {
            timeline_id: key.timeline_id,
            archive_epoch_id: key.archive_epoch_id,
            segment_id: key.segment_id,
            predecessor,
            incarnation: [5; 16],
        };
        let header_digest = header.digest().unwrap();
        let header_bytes = header.encode().unwrap();
        let generation_zero = encode_generation_zero_frontier(header_digest).unwrap();
        let mut wal = header_bytes.to_vec();
        wal.extend_from_slice(&generation_zero);
        let mut previous = WalV7Frontier {
            generation: 0,
            ticket_end: 0,
            durable_end: WAL_V7_HEADER_LEN as u64,
            last_commit_ts: 0,
            prefix_digest: header_digest,
            previous_frontier_offset: 0,
            previous_frontier_digest: [0; 32],
        };
        let mut previous_offset = WAL_V7_HEADER_LEN as u64;
        let mut previous_digest = frame_record_digest(&generation_zero).unwrap();
        let mut prefix = Sha256::new();
        prefix.update(header_bytes);
        let mut logical_bytes = 0;
        for (ticket, batch) in batches.iter().enumerate() {
            let ticket = ticket as u64;
            let logical =
                encode_logical_batch(ticket, batch, crate::pitr::LIVE_WAL_V5_LIMITS).unwrap();
            logical_bytes += logical.len() as u64;
            let frame_bytes =
                data_fragment_count(logical.len() as u64).unwrap() as usize * WAL_V7_FRAME_LEN;
            let first_data_offset = wal.len() as u64;
            let mut frames = vec![0; frame_bytes];
            assert_eq!(
                encode_data_frames_into(
                    header_digest,
                    first_data_offset,
                    ticket,
                    &logical,
                    crate::pitr::LIVE_WAL_V5_LIMITS,
                    &mut frames,
                )
                .unwrap(),
                frame_bytes
            );
            wal.extend_from_slice(&frames);
            prefix.update(logical);
            let offset = wal.len() as u64;
            let frontier = WalV7Frontier {
                generation: ticket + 1,
                ticket_end: ticket + 1,
                durable_end: offset,
                last_commit_ts: batch.commit_ts,
                prefix_digest: prefix.clone().finalize().into(),
                previous_frontier_offset: previous_offset,
                previous_frontier_digest: previous_digest,
            };
            let frame = encode_frontier_successor(
                frontier,
                previous,
                previous_offset,
                &previous_digest,
                offset,
                header_digest,
            )
            .unwrap();
            wal.extend_from_slice(&frame);
            previous = frontier;
            previous_offset = offset;
            previous_digest = frame_record_digest(&frame).unwrap();
        }
        let active = crate::pitr::manifest::ActiveBoundary {
            timeline_id: key.timeline_id.0,
            archive_epoch_id: key.archive_epoch_id.0,
            segment_id: key.segment_id.0,
            incarnation: header.incarnation,
            ticket_end: batches.len() as u64,
            durable_end: previous_offset,
            last_commit_ts: batches.last().map_or(0, |batch| batch.commit_ts),
            prefix_digest: prefix.finalize().into(),
        };
        let (seal, seal_bytes) =
            crate::pitr::seal::build_v7_seal(&wal, active, predecessor).unwrap();
        let wal_digest = seal.boundary.wal_digest;
        let seal_digest = Sha256::digest(&seal_bytes).into();
        let metadata = SegmentMetadata {
            key,
            wal_format_version: crate::wal::format::WAL_FORMAT_VERSION_V7,
            seal_format_version: 2,
            immutable_boundary: Some(Box::new(seal.boundary)),
            anchor: SegmentAnchor {
                segment_id: key.segment_id,
                wal_digest,
                seal_digest,
            },
            predecessor,
            first_commit_ts: batches.first().map(|batch| batch.commit_ts),
            last_commit_ts: batches.last().map(|batch| batch.commit_ts),
            batch_count: batches.len() as u64,
            logical_bytes,
            wal_bytes: wal.len() as u64,
            wal_digest,
            seal_digest,
            source_identity: [6; 32],
        };

        (metadata, wal, seal_bytes)
    }

    /// Construction must not wait on a repository lock somebody else holds. A
    /// live `BackupRepository` keeps that LOCK for its whole lifetime and a peer
    /// publication holds it for the length of a transfer, so blocking here would
    /// stall `enable_pitr`/`resume_pitr` behind either of them with no timeout.
    #[cfg(target_os = "linux")]
    #[test]
    fn stager_construction_skips_the_sweep_under_a_held_repository_lock() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join(REPOSITORY_LOCK_FILE);
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        // SAFETY: `held` owns a valid descriptor for the LOCK file.
        assert_eq!(
            unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );

        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let staged = wal_dir.join(format!(
            ".{}-{}-{}-{}.wal.tmp-1-2",
            "0".repeat(32),
            "1".repeat(32),
            "2".repeat(16),
            "3".repeat(64)
        ));
        assert!(is_archive_temp_name(
            staged.file_name().unwrap().to_str().unwrap()
        ));
        std::fs::write(&staged, b"staged").unwrap();

        // Returns rather than blocking, and leaves the entry for a later sweep.
        let stager = ArchiveObjectStager::new(dir.path()).unwrap();
        assert!(
            staged.exists(),
            "the sweep must be skipped while another holder owns the repository lock"
        );
        drop(stager);
        assert!(
            staged.exists(),
            "dropping the stager must not sweep under a held lock either"
        );
    }

    #[test]
    fn prepares_identity_bound_objects_and_commits_once() {
        let mut catalog = PitrArchiveCatalog::default();
        let metadata = metadata();
        let prepared = catalog
            .prepare_objects(&metadata, &wal_bytes(), b"seal")
            .unwrap();
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
    fn prepares_v7_objects_after_verifying_the_seal_against_the_frozen_image() {
        let (metadata, wal, seal) = v7_archive_objects();
        let prepared = PitrArchiveCatalog::default()
            .prepare_objects(&metadata, &wal, &seal)
            .unwrap();

        assert_eq!(prepared.wal_digest_policy, SourceObjectDigest::WholeObject);
        assert_eq!(
            SourceObjectDigest::for_wal_format(metadata.wal_format_version)
                .unwrap()
                .digest(&wal)
                .unwrap(),
            metadata.wal_digest
        );
    }

    #[test]
    fn rejects_a_v7_seal_that_disagrees_with_the_catalog_boundary() {
        let (mut metadata, wal, seal_bytes) = v7_archive_objects();
        let mut seal = crate::pitr::seal::V7Seal::decode(&seal_bytes, metadata.wal_bytes).unwrap();
        seal.boundary.terminal_frontier_digest[0] ^= 1;
        let seal_bytes = seal.encode().unwrap();
        metadata.seal_digest = Sha256::digest(&seal_bytes).into();
        metadata.anchor.seal_digest = metadata.seal_digest;

        assert!(
            PitrArchiveCatalog::default()
                .prepare_objects(&metadata, &wal, &seal_bytes)
                .is_err()
        );
    }

    #[test]
    fn commit_rejects_v7_metadata_changed_after_object_verification() {
        let (mut metadata, wal, seal) = v7_archive_objects();
        let prepared = PitrArchiveCatalog::default()
            .prepare_objects(&metadata, &wal, &seal)
            .unwrap();
        metadata
            .immutable_boundary
            .as_mut()
            .unwrap()
            .terminal_frontier_digest[0] ^= 1;

        assert!(
            PitrArchiveCatalog::default()
                .commit_segment(metadata, &prepared)
                .is_err()
        );
    }

    #[test]
    fn rejects_a_corrupt_v7_image_even_when_its_new_digest_is_cataloged() {
        let (mut metadata, mut wal, seal_bytes) = v7_archive_objects();
        wal[0] ^= 1;
        let mut seal = crate::pitr::seal::V7Seal::decode(&seal_bytes, metadata.wal_bytes).unwrap();
        seal.boundary.wal_digest = Sha256::digest(&wal).into();
        let seal_bytes = seal.encode().unwrap();
        metadata.wal_digest = seal.boundary.wal_digest;
        metadata.anchor.wal_digest = metadata.wal_digest;
        metadata.immutable_boundary = Some(Box::new(seal.boundary));
        metadata.seal_digest = Sha256::digest(&seal_bytes).into();
        metadata.anchor.seal_digest = metadata.seal_digest;

        assert!(
            PitrArchiveCatalog::default()
                .prepare_objects(&metadata, &wal, &seal_bytes)
                .is_err()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn repository_verification_checks_v7_objects_at_shallow_and_deep_depths() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("objects")).unwrap();
        std::fs::create_dir(root.path().join("backups")).unwrap();
        std::fs::write(root.path().join("BACKUP_CATALOG_LOG"), []).unwrap();
        let (metadata, wal, seal) = v7_archive_objects();
        let mut catalog = PitrArchiveCatalog::default();
        let prepared = catalog.prepare_objects(&metadata, &wal, &seal).unwrap();
        let stager = ArchiveObjectStager::new(root.path()).unwrap();
        stager.publish(&prepared, &wal, &seal).unwrap();
        catalog.commit_segment(metadata, &prepared).unwrap();
        std::fs::write(root.path().join("PITR_CATALOG_LOG"), catalog.bytes()).unwrap();
        drop(stager);
        let repository = crate::backup::BackupRepository::open(root.path()).unwrap();
        let options = crate::pitr::api::VerifyPitrOptions {
            depth: crate::pitr::api::VerifyPitrDepth::Shallow,
            selector: None,
            cursor: None,
            page_size: crate::pitr::api::MAX_VERIFY_PAGE_SIZE,
        };

        let report = repository.verify_pitr(options).unwrap();
        assert!(report.first_failure.is_none());
        assert!(report.verified_intervals.is_empty());

        let deep_options = crate::pitr::api::VerifyPitrOptions {
            depth: crate::pitr::api::VerifyPitrDepth::Deep {
                sampled_targets: std::num::NonZeroUsize::new(1).unwrap(),
            },
            ..options
        };
        let report = repository.verify_pitr(deep_options).unwrap();
        assert!(report.first_failure.is_none());
        assert!(report.verified_intervals.is_empty());

        let mut corrupt_wal = wal;
        corrupt_wal[0] ^= 1;
        std::fs::write(
            root.path().join("wal").join(&prepared.wal_name),
            corrupt_wal,
        )
        .unwrap();
        let report = repository.verify_pitr(options).unwrap();
        assert_eq!(
            report.first_failure.unwrap().kind,
            crate::pitr::api::SegmentFailureKind::Corrupt
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn deep_repository_verification_reports_rehashed_v7_structural_corruption() {
        assert_deep_v7_corruption_reports(false);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn deep_repository_verification_reports_v7_corruption_with_a_base() {
        assert_deep_v7_corruption_reports(true);
    }

    #[cfg(target_os = "linux")]
    fn assert_deep_v7_corruption_reports(with_base: bool) {
        use crate::wal::v7::codec::{WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WAL_V7_FRAME_LEN};

        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("objects")).unwrap();
        std::fs::create_dir(root.path().join("backups")).unwrap();
        std::fs::write(root.path().join("BACKUP_CATALOG_LOG"), []).unwrap();
        let first_batch = crate::pitr::WalBatch {
            commit_ts: 10,
            recorded_at: crate::pitr::RecordedAt { secs: 10, nanos: 0 },
            entries: vec![crate::pitr::WalEntry::Put {
                key: b"first".to_vec(),
                value: b"value".to_vec(),
            }],
        };
        let (mut first, first_wal, first_seal) = v7_archive_objects_with_batches(
            SegmentId(4),
            ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([3; 16]),
            },
            &[first_batch],
        );
        let second_batch = crate::pitr::WalBatch {
            commit_ts: 20,
            recorded_at: crate::pitr::RecordedAt { secs: 20, nanos: 0 },
            entries: vec![crate::pitr::WalEntry::Put {
                key: b"second".to_vec(),
                value: vec![0x5a; 2 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY],
            }],
        };
        let (mut second, wal, seal_bytes) = v7_archive_objects_with_batches(
            SegmentId(5),
            ChainAnchor::Segment(first.anchor),
            &[second_batch],
        );
        let stager = ArchiveObjectStager::new(root.path()).unwrap();
        let mut catalog = PitrArchiveCatalog::default();
        for (metadata, wal, seal) in [
            (&first, &first_wal, &first_seal),
            (&second, &wal, &seal_bytes),
        ] {
            let prepared = catalog.prepare_objects(metadata, wal, seal).unwrap();
            stager.publish(&prepared, wal, seal).unwrap();
            catalog.commit_segment(metadata.clone(), &prepared).unwrap();
        }
        std::fs::write(root.path().join("PITR_CATALOG_LOG"), catalog.bytes()).unwrap();
        drop(stager);
        let mut repository = crate::backup::BackupRepository::open(root.path()).unwrap();
        if with_base {
            let repository_id = repository.ensure_pitr_repository_identity().unwrap();
            first.key.repository_id = repository_id;
            second.key.repository_id = repository_id;
            let catalog = encode_catalog(&[
                PitrCatalogRecord::CommitSegment {
                    metadata: first.clone(),
                },
                PitrCatalogRecord::CommitSegment {
                    metadata: second.clone(),
                },
            ])
            .unwrap();
            std::fs::write(root.path().join("PITR_CATALOG_LOG"), catalog).unwrap();
            let base_recorded_at = crate::pitr::manifest::PersistedRecordedAt { secs: 1, nanos: 0 };
            let base = crate::pitr::base::PitrBaseMetadata {
                repository_id,
                timeline_id: first.key.timeline_id.0,
                archive_epoch_id: first.key.archive_epoch_id.0,
                included_commit_ts: None,
                boundary_segment_id: first.key.segment_id.0,
                boundary_anchor: crate::pitr::manifest::PersistedChainAnchor::Genesis {
                    archive_epoch_id: first.key.archive_epoch_id.0,
                },
                base_recorded_at,
                time_anchor: crate::pitr::base::PitrBaseTimeAnchor::Observed {
                    commit_ts: None,
                    observed_at: base_recorded_at,
                },
                wal_replay_version: crate::pitr::base::PITR_BASE_WAL_REPLAY_VERSION,
                compatibility_digest: [1; 32],
            };
            let compatibility = serde_json::from_value(serde_json::json!({
                "manifest_format_version": crate::manifest::MANIFEST_FORMAT_VERSION,
                "value_separation_enabled": false,
                "vlog_format_version": null,
                "ttl_records_present": false,
                "serializable_at_capture": false,
            }))
            .unwrap();
            repository
                .create_backup_with_pitr_base(b"backup", b"snapshot", base, compatibility)
                .unwrap();
        }
        let deep_options = crate::pitr::api::VerifyPitrOptions {
            depth: crate::pitr::api::VerifyPitrDepth::Deep {
                sampled_targets: std::num::NonZeroUsize::new(1).unwrap(),
            },
            selector: None,
            cursor: None,
            page_size: crate::pitr::api::MAX_VERIFY_PAGE_SIZE,
        };
        let report = repository.verify_pitr(deep_options).unwrap();
        assert!(report.first_failure.is_none());
        assert_eq!(report.last_verified_commit_ts, Some(20));
        if with_base {
            assert_eq!(report.verified_intervals.len(), 1);
            assert_eq!(report.verified_intervals[0].commit_bounds, Some(10..=20));
        }
        let shallow_options = crate::pitr::api::VerifyPitrOptions {
            depth: crate::pitr::api::VerifyPitrDepth::Shallow,
            ..deep_options
        };

        for (kind, offset) in [
            (ArchiveObjectKind::Wal, 2 * WAL_V7_FRAME_LEN + 100),
            (ArchiveObjectKind::Wal, wal.len() - WAL_V7_FRAME_LEN + 100),
            (ArchiveObjectKind::Seal, 0),
        ] {
            let mut corrupt_wal = wal.clone();
            if kind == ArchiveObjectKind::Wal {
                corrupt_wal[offset] ^= 1;
            }
            let mut seal =
                crate::pitr::seal::V7Seal::decode(&seal_bytes, second.wal_bytes).unwrap();
            seal.boundary.wal_digest = Sha256::digest(&corrupt_wal).into();
            let mut corrupt_seal = seal.encode().unwrap();
            if kind == ArchiveObjectKind::Seal {
                corrupt_seal[offset] ^= 1;
            }
            // Keep every outer object/catalog digest consistent so Shallow
            // accepts the objects and Deep must validate their structure.
            let mut metadata = second.clone();
            metadata.wal_digest = seal.boundary.wal_digest;
            metadata.anchor.wal_digest = metadata.wal_digest;
            metadata.immutable_boundary = Some(Box::new(seal.boundary));
            metadata.seal_digest = Sha256::digest(&corrupt_seal).into();
            metadata.anchor.seal_digest = metadata.seal_digest;
            for (object_kind, bytes, digest) in [
                (ArchiveObjectKind::Wal, &corrupt_wal, metadata.wal_digest),
                (ArchiveObjectKind::Seal, &corrupt_seal, metadata.seal_digest),
            ] {
                let name = archive_object_name(
                    metadata.key.timeline_id,
                    metadata.key.archive_epoch_id,
                    metadata.key.segment_id,
                    object_kind,
                    digest,
                );
                std::fs::write(root.path().join("wal").join(name), bytes).unwrap();
            }
            let catalog = encode_catalog(&[
                PitrCatalogRecord::CommitSegment {
                    metadata: first.clone(),
                },
                PitrCatalogRecord::CommitSegment { metadata },
            ])
            .unwrap();
            std::fs::write(root.path().join("PITR_CATALOG_LOG"), catalog).unwrap();

            if !with_base || kind != ArchiveObjectKind::Seal {
                let shallow = repository.verify_pitr(shallow_options).unwrap();
                assert!(shallow.first_failure.is_none());
                assert_eq!(shallow.last_verified_commit_ts, Some(20));
            }
            let deep = repository.verify_pitr(deep_options).unwrap();
            let failure = deep.first_failure.unwrap();
            assert_eq!(failure.kind, crate::pitr::api::SegmentFailureKind::Corrupt);
            assert_eq!(failure.expected_segment_id, Some(5));
            assert_eq!(deep.last_verified_commit_ts, Some(10));
            assert!(deep.next_cursor.is_none());
            if with_base {
                assert_eq!(deep.verified_intervals.len(), 1);
                assert_eq!(deep.verified_intervals[0].commit_bounds, Some(10..=10));
            }

            // Page one must not derive intervals using the corrupt later seal.
            let paged_options = crate::pitr::api::VerifyPitrOptions {
                page_size: std::num::NonZeroUsize::new(1).unwrap(),
                ..deep_options
            };
            let page = repository.verify_pitr(paged_options).unwrap();
            assert!(page.first_failure.is_none());
            assert_eq!(page.last_verified_commit_ts, Some(10));
            if with_base {
                assert_eq!(page.verified_intervals.len(), 1);
                assert_eq!(page.verified_intervals[0].commit_bounds, Some(10..=10));
            }
            let cursor = page.next_cursor.unwrap();
            let page = repository
                .verify_pitr(crate::pitr::api::VerifyPitrOptions {
                    cursor: Some(cursor),
                    ..paged_options
                })
                .unwrap();
            let failure = page.first_failure.unwrap();
            assert_eq!(failure.kind, crate::pitr::api::SegmentFailureKind::Corrupt);
            assert_eq!(failure.expected_segment_id, Some(5));
            // This watermark describes successful checks within this page.
            assert_eq!(page.last_verified_commit_ts, None);
            assert!(page.next_cursor.is_none());
            if with_base {
                assert_eq!(page.verified_intervals.len(), 1);
                assert_eq!(page.verified_intervals[0].commit_bounds, Some(10..=10));
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reclaims_stale_archive_staging_files_on_open() {
        let root = std::env::temp_dir().join(format!("toy-kv-pitr-stale-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let stager = ArchiveObjectStager::new(&root).unwrap();
        drop(stager);
        let stale = root
            .join("wal")
            .join(".aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-0000000000000001-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.wal.tmp-123-1");
        std::fs::write(&stale, b"stale").unwrap();
        let stager = ArchiveObjectStager::new(&root).unwrap();
        // Asserted while the stager is still alive: the drop-time sweep reclaims
        // these too, so checking after the drop would pass even without the
        // open-time sweep this test is named for.
        assert!(!stale.exists());
        drop(stager);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn repository_lock_serializes_threads_that_share_one_stager() {
        let root = std::env::temp_dir().join(format!("toy-kv-pitr-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let stager = std::sync::Arc::new(ArchiveObjectStager::new(&root).unwrap());
        let concurrent = std::sync::Arc::new(AtomicU64::new(0));
        let peak = std::sync::Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..3 {
                let stager = std::sync::Arc::clone(&stager);
                let concurrent = std::sync::Arc::clone(&concurrent);
                let peak = std::sync::Arc::clone(&peak);
                scope.spawn(move || {
                    let _guard = stager.lock_exclusive().unwrap();
                    let holders = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(holders, Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    concurrent.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "the repository lock must serialize concurrent archive transactions"
        );
        std::fs::remove_dir_all(root).unwrap();
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
        let prepared = catalog
            .prepare_objects(&metadata, &wal_bytes(), b"seal")
            .unwrap();
        stager.publish(&prepared, &wal_bytes(), b"seal").unwrap();
        stager.publish(&prepared, &wal_bytes(), b"seal").unwrap();
        assert_eq!(
            stager
                .read(&prepared.wal_name, Some(wal_bytes().len() as u64))
                .unwrap(),
            wal_bytes()
        );
        assert_eq!(stager.read(&prepared.seal_name, None).unwrap(), b"seal");
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
        let prepared = catalog
            .prepare_objects(&metadata, &wal_bytes(), b"seal")
            .unwrap();
        let fifo = root.join("wal").join(&prepared.wal_name);
        let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(stager.publish(&prepared, &wal_bytes(), b"seal").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn duplicate_commit_reports_wire_sequence_after_snapshot_replacement() {
        let snapshot =
            PitrCatalogRecord::RetentionSnapshot(crate::pitr::catalog::RetentionSnapshot {
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
        let bytes = crate::pitr::catalog::encode_catalog(&[snapshot]).unwrap();
        let mut catalog = PitrArchiveCatalog::open(bytes).unwrap();
        let metadata = metadata();
        let prepared = catalog
            .prepare_objects(&metadata, &wal_bytes(), b"seal")
            .unwrap();
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
        let complete = crate::pitr::catalog::encode_catalog(&[
            PitrCatalogRecord::CommitSegment { metadata: first },
            PitrCatalogRecord::CommitSegment {
                metadata: second.clone(),
            },
        ])
        .unwrap();
        let torn = complete[..complete.len() - 2].to_vec();
        let mut catalog = PitrArchiveCatalog::open(torn.clone()).unwrap();
        let prepared = catalog
            .prepare_objects(&second, &wal_bytes(), b"seal")
            .unwrap();
        assert!(catalog.commit_segment(second, &prepared).is_err());
        assert_eq!(catalog.bytes(), torn);
    }
}
