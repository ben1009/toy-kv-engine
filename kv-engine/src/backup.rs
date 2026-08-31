//! Incremental-backup repository catalog primitives (RFC 022).
//!
//! The catalog is intentionally append-only. A frame is length-delimited and
//! checksummed, so recovery can discard only a torn final write while treating
//! complete semantic corruption as an error.

#![allow(dead_code)] // Wired to repository publication in the next RFC 022 slice.

use std::{
    collections::HashSet,
    io::{Read, Write},
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
    pub(crate) high_water_id: u64,
    pub(crate) retained_offset: u64,
}

#[cfg(target_os = "linux")]
pub(crate) struct RepositoryLock {
    _fd: OwnedFd,
}

#[cfg(target_os = "linux")]
impl RepositoryLock {
    pub(crate) fn acquire(parent: &OwnedFd, exclusive: bool) -> Result<Self> {
        let fd = openat_no_follow(parent, "LOCK", libc::O_RDWR | libc::O_CREAT, 0o600)?;
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
                pending = None;
            }
        }
    }
    let retained_offset = match pending {
        Some((high_water, Some(_))) => high_water.start_offset + frame_len(high_water)?,
        _ => frames.last_complete_offset,
    };
    Ok(CatalogReplay {
        committed_ids,
        high_water_id,
        retained_offset,
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

        let noncanonical = br#"{\"version\":1, \"record\":{\"type\":\"high_water\",\"sequence\":1,\"allocated_id\":1}}"#;
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
