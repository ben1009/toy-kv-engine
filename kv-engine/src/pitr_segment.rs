//! PITR segment lifecycle and source-pin management.
#![allow(dead_code)]

use anyhow::{Result, ensure};
use std::collections::BTreeMap;

#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::ffi::OsStrExt,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(target_os = "linux")]
static SUCCESSOR_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentState {
    Active,
    Sealing,
    Sealed,
    Archived,
    Reclaimable,
    Reclaiming,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentMetadata {
    pub(crate) segment_id: u64,
    pub(crate) state: SegmentState,
    pub(crate) logical_length: u64,
    pub(crate) source_spool_bytes: u64,
    pub(crate) source_pins: u32,
    pub(crate) archive_pin: bool,
    pub(crate) successor_segment_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CleanupReceipt {
    pub(crate) wal_unlinked: bool,
    pub(crate) seal_unlinked: bool,
    pub(crate) directory_synced: bool,
}

impl CleanupReceipt {
    pub(crate) const fn durable() -> Self {
        Self {
            wal_unlinked: true,
            seal_unlinked: true,
            directory_synced: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RotationReason {
    Size,
    Timer,
    Backup,
    Barrier,
    Shutdown,
}

#[derive(Debug)]
pub(crate) struct PitrSegmentManager {
    segments: BTreeMap<u64, SegmentMetadata>,
    active_segment_id: u64,
    next_segment_id: u64,
    pending_successor: Option<SegmentMetadata>,
    source_spool_limit: u64,
    source_spool_reserved: u64,
    pending_rotation: Option<RotationReason>,
}

impl PitrSegmentManager {
    pub(crate) fn new(active_segment_id: u64, source_spool_limit: u64) -> Result<Self> {
        ensure!(
            source_spool_limit >= 4096,
            "PITR source spool limit is too small"
        );
        let active = SegmentMetadata {
            segment_id: active_segment_id,
            state: SegmentState::Active,
            logical_length: 4096,
            source_spool_bytes: 4096,
            source_pins: 0,
            archive_pin: false,
            successor_segment_id: None,
        };
        let mut segments = BTreeMap::new();
        segments.insert(active_segment_id, active);
        Ok(Self {
            segments,
            active_segment_id,
            next_segment_id: active_segment_id
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("segment ID exhausted"))?,
            pending_successor: None,
            source_spool_limit,
            source_spool_reserved: 4096,
            pending_rotation: None,
        })
    }

    pub(crate) fn request_rotation(&mut self, reason: RotationReason) -> bool {
        if let Some(existing) = self.pending_rotation
            && reason.priority() <= existing.priority()
        {
            return false;
        }
        self.pending_rotation = Some(reason);
        true
    }

    pub(crate) fn take_rotation_request(&mut self) -> Option<RotationReason> {
        self.pending_rotation.take()
    }

    pub(crate) fn begin_sealing(
        &mut self,
        logical_length: u64,
        successor_spool_bytes: u64,
    ) -> Result<u64> {
        ensure!(
            self.pending_successor.is_none(),
            "PITR rotation already has a pending successor"
        );
        ensure!(
            logical_length >= 4096 && logical_length.is_multiple_of(4096),
            "invalid sealed logical length"
        );
        let active = self
            .segments
            .get_mut(&self.active_segment_id)
            .expect("active segment invariant");
        ensure!(
            active.state == SegmentState::Active,
            "PITR active segment is not active"
        );
        let successor_id = self.next_segment_id;
        ensure!(
            successor_spool_bytes >= 4096,
            "successor reservation is below minimum WAL header"
        );
        let physical_bytes = active.source_spool_bytes.max(logical_length);
        let active_growth = physical_bytes.saturating_sub(active.source_spool_bytes);
        let reserved = self
            .source_spool_reserved
            .checked_add(active_growth)
            .and_then(|bytes| bytes.checked_add(successor_spool_bytes))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;
        ensure!(
            reserved <= self.source_spool_limit,
            "PITR source spool limit exceeded"
        );
        active.state = SegmentState::Sealing;
        active.logical_length = logical_length;
        active.source_spool_bytes = physical_bytes;
        active.successor_segment_id = Some(successor_id);
        self.pending_successor = Some(SegmentMetadata {
            segment_id: successor_id,
            state: SegmentState::Sealing,
            logical_length: 4096,
            source_spool_bytes: successor_spool_bytes,
            source_pins: 0,
            archive_pin: false,
            successor_segment_id: None,
        });
        self.source_spool_reserved = reserved;
        Ok(successor_id)
    }

    /// Record a segment that a lifecycle barrier sealed and archived.
    ///
    /// The barrier owns its own durable transitions; this only keeps the
    /// in-memory bookkeeping - and therefore reported status - in step with
    /// them. It deliberately does not re-run the admission-time spool
    /// arithmetic, which belongs to the write path's reservations.
    /// Track an obligation inherited from a previous run.
    ///
    /// A reopen starts the manager at the active segment and knows nothing
    /// about the sealed segments the manifest still lists, so reconciliation
    /// has to be able to pick them up before it can finish their lifecycle.
    pub(crate) fn adopt_obligation(
        &mut self,
        segment_id: u64,
        logical_length: u64,
        successor_segment_id: u64,
    ) -> Result<()> {
        ensure!(
            logical_length >= 4096 && logical_length.is_multiple_of(4096),
            "invalid sealed logical length"
        );
        ensure!(
            successor_segment_id > segment_id,
            "sealed successor segment ID is not monotonic"
        );
        self.segments.entry(segment_id).or_insert(SegmentMetadata {
            segment_id,
            state: SegmentState::Sealed,
            logical_length,
            source_spool_bytes: logical_length,
            source_pins: 0,
            archive_pin: true,
            successor_segment_id: Some(successor_segment_id),
        });
        self.next_segment_id = self
            .next_segment_id
            .max(successor_segment_id.saturating_add(1));
        self.recompute_reserved();

        Ok(())
    }

    pub(crate) fn record_sealed(
        &mut self,
        segment_id: u64,
        logical_length: u64,
        successor_segment_id: u64,
    ) -> Result<()> {
        ensure!(
            logical_length >= 4096 && logical_length.is_multiple_of(4096),
            "invalid sealed logical length"
        );
        ensure!(
            successor_segment_id > segment_id,
            "sealed successor segment ID is not monotonic"
        );
        match self.segments.get_mut(&segment_id) {
            Some(segment) => {
                segment.state = SegmentState::Sealed;
                segment.archive_pin = true;
                segment.logical_length = logical_length;
                segment.source_spool_bytes = logical_length;
                segment.successor_segment_id = Some(successor_segment_id);
            }
            None => {
                self.segments.insert(
                    segment_id,
                    SegmentMetadata {
                        segment_id,
                        state: SegmentState::Sealed,
                        logical_length,
                        source_spool_bytes: logical_length,
                        source_pins: 0,
                        archive_pin: true,
                        successor_segment_id: Some(successor_segment_id),
                    },
                );
            }
        }
        // The sealed segment stops being the active one the moment its
        // successor WAL exists, which is what lets it be reclaimed later.
        self.active_segment_id = successor_segment_id;
        self.segments
            .entry(successor_segment_id)
            .or_insert(SegmentMetadata {
                segment_id: successor_segment_id,
                state: SegmentState::Active,
                logical_length: 4096,
                source_spool_bytes: 4096,
                source_pins: 0,
                archive_pin: false,
                successor_segment_id: None,
            });
        self.next_segment_id = self
            .next_segment_id
            .max(successor_segment_id.saturating_add(1));
        self.recompute_reserved();

        Ok(())
    }

    /// Drop a segment whose source WAL and sidecar have been unlinked.
    pub(crate) fn record_reclaimed(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get(&segment_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaimable,
            "PITR segment was not reclaimable"
        );
        self.segments.remove(&segment_id);
        self.recompute_reserved();

        Ok(())
    }

    /// Reserved bytes are exactly what the tracked segments hold.
    fn recompute_reserved(&mut self) {
        self.source_spool_reserved = self.segments.values().fold(0_u64, |total, segment| {
            total.saturating_add(segment.source_spool_bytes)
        });
    }

    pub(crate) fn mark_sealed(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Sealing,
            "PITR segment sealed out of order"
        );
        segment.state = SegmentState::Sealed;
        segment.archive_pin = true;
        Ok(())
    }

    pub(crate) fn install_successor(&mut self) -> Result<u64> {
        let successor = self
            .pending_successor
            .ok_or_else(|| anyhow::anyhow!("PITR successor is not pending"))?;
        ensure!(
            self.segments
                .get(&self.active_segment_id)
                .is_some_and(|segment| segment.state == SegmentState::Sealed),
            "cannot install successor before sealing old segment"
        );
        let next_segment_id = successor
            .segment_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("segment ID exhausted"))?;
        self.pending_successor = None;
        self.segments.insert(
            successor.segment_id,
            SegmentMetadata {
                state: SegmentState::Active,
                ..successor
            },
        );
        self.active_segment_id = successor.segment_id;
        self.next_segment_id = next_segment_id;
        Ok(self.active_segment_id)
    }

    pub(crate) fn install_successor_after_wal(
        &mut self,
        install_wal: impl FnOnce(u64) -> Result<()>,
    ) -> Result<u64> {
        let successor_id = self
            .pending_successor
            .ok_or_else(|| anyhow::anyhow!("PITR successor is not pending"))?
            .segment_id;
        install_wal(successor_id)?;
        self.install_successor()
    }

    pub(crate) fn mark_archived(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Sealed,
            "PITR archive is out of order"
        );
        segment.state = SegmentState::Archived;
        Ok(())
    }

    pub(crate) fn mark_reclaimable(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Archived,
            "PITR reclaimable state is out of order"
        );
        ensure!(
            segment.source_pins == 0,
            "PITR segment has transient source pins"
        );
        ensure!(
            segment.archive_pin,
            "PITR segment is missing lifecycle archive pin"
        );
        ensure!(
            segment_id != self.active_segment_id,
            "cannot reclaim the active PITR segment"
        );
        ensure!(
            self.pending_successor.is_none(),
            "cannot reclaim while successor installation is pending"
        );
        segment.state = SegmentState::Reclaimable;
        Ok(())
    }

    pub(crate) fn release_archive_pin(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaimable,
            "archive pin released before durable reclaimable state"
        );
        ensure!(segment.archive_pin, "PITR archive pin is not held");
        segment.archive_pin = false;
        Ok(())
    }

    pub(crate) fn pin(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        segment.source_pins = segment
            .source_pins
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PITR source pin count overflow"))?;
        Ok(())
    }

    pub(crate) fn unpin(&mut self, segment_id: u64) -> Result<()> {
        let segment = self
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(segment.source_pins > 0, "PITR source pin underflow");
        segment.source_pins -= 1;
        Ok(())
    }

    pub(crate) fn reclaim(&mut self, segment_id: u64) -> Result<SegmentMetadata> {
        let segment = self
            .segments
            .get(&segment_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaimable
                && segment.source_pins == 0
                && !segment.archive_pin,
            "PITR segment is not reclaimable"
        );
        let segment = self
            .segments
            .get_mut(&segment_id)
            .expect("segment was present");
        segment.state = SegmentState::Reclaiming;
        Ok(*segment)
    }

    pub(crate) fn complete_reclaim(
        &mut self,
        segment_id: u64,
        cleanup: CleanupReceipt,
    ) -> Result<SegmentMetadata> {
        let segment = self
            .segments
            .get(&segment_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown PITR segment"))?;
        ensure!(
            segment.state == SegmentState::Reclaiming && segment.source_pins == 0,
            "PITR cleanup is not ready"
        );
        ensure!(
            segment_id != self.active_segment_id,
            "cannot complete reclaim of the active PITR segment"
        );
        ensure!(
            self.pending_successor.is_none(),
            "cannot complete reclaim while successor installation is pending"
        );
        ensure!(
            cleanup.wal_unlinked && cleanup.seal_unlinked && cleanup.directory_synced,
            "PITR cleanup is not durable"
        );
        let released = segment
            .source_spool_bytes
            .checked_add(4096)
            .ok_or_else(|| anyhow::anyhow!("source spool accounting overflow"))?;
        let new_reserved = self
            .source_spool_reserved
            .checked_sub(released)
            .ok_or_else(|| anyhow::anyhow!("source spool accounting underflow"))?;
        let removed = self
            .segments
            .remove(&segment_id)
            .expect("segment was present");
        self.source_spool_reserved = new_reserved;
        Ok(removed)
    }

    pub(crate) fn segment(&self, segment_id: u64) -> Option<SegmentMetadata> {
        self.segments.get(&segment_id).copied()
    }

    pub(crate) fn segment_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.segments.keys().copied()
    }

    pub(crate) fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    pub(crate) fn active_logical_length(&self) -> u64 {
        self.segments
            .get(&self.active_segment_id)
            .map_or(0, |segment| segment.logical_length)
    }

    pub(crate) fn source_spool_reserved(&self) -> u64 {
        self.source_spool_reserved
    }

    pub(crate) fn sealed_unarchived_bytes(&self) -> u64 {
        self.segments
            .values()
            .filter(|segment| matches!(segment.state, SegmentState::Sealing | SegmentState::Sealed))
            .map(|segment| segment.logical_length)
            .sum()
    }

    pub(crate) fn pending_successor_id(&self) -> Result<u64> {
        self.pending_successor
            .map(|segment| segment.segment_id)
            .ok_or_else(|| anyhow::anyhow!("PITR successor is not pending"))
    }
}

pub(crate) fn install_v5_wal_header(
    path: impl AsRef<std::path::Path>,
    header: crate::pitr::WalV5Header,
) -> Result<()> {
    let path = path.as_ref();
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("PITR WAL has no file name"))?;
    ensure!(
        file_name.to_str().is_some_and(|name| !name.is_empty()),
        "PITR WAL file name is not valid UTF-8"
    );
    let bytes = crate::pitr::encode_v5_file_header(header)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("PITR successor WAL has no parent directory"))?;
    let temp_name = format!(
        ".{}.tmp-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        SUCCESSOR_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let temp_path = parent.join(&temp_name);
    let result = (|| -> Result<()> {
        let parent_file = std::fs::File::open(parent)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        let from = CString::new(temp_name.as_bytes())?;
        let to = CString::new(file_name.as_bytes())?;
        let rename = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                parent_file.as_raw_fd(),
                from.as_ptr(),
                parent_file.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rename != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                let mut existing_file = std::fs::File::open(path)?;
                ensure!(
                    existing_file.metadata()?.len() == bytes.len() as u64,
                    "existing PITR WAL is not header-only"
                );
                let mut existing = vec![0; bytes.len()];
                existing_file.read_exact(&mut existing)?;
                ensure!(
                    existing == bytes,
                    "existing PITR WAL is not an exact header-only identity match"
                );
                parent_file.sync_all()?;
                return Ok(());
            }
            return Err(error.into());
        }
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&temp_path);
    result
}

/// Remove staging files left behind by an interrupted PITR file installation.
///
/// Installs stage through `.<object>.tmp-<pid>-<sequence>` and rename the file
/// into place, so a crash between the two steps can strand the staging file.
/// Only regular files matching exactly that shape are removed; anything else in
/// the directory is left alone.
#[cfg(target_os = "linux")]
pub(crate) fn cleanup_pitr_temp_files(dir: impl AsRef<std::path::Path>) -> Result<u64> {
    let mut removed = 0u64;
    for entry in std::fs::read_dir(dir.as_ref())? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_pitr_install_temp_name(name) {
            continue;
        }
        // Only regular files are ours to remove. A directory matching the temp
        // shape makes this fail with EISDIR, and every caller propagates that:
        // the engine would refuse to open until someone deleted it by hand.
        if !entry.file_type()?.is_file() {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => removed = removed.saturating_add(1),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(removed)
}

#[cfg(target_os = "linux")]
fn is_pitr_install_temp_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix('.') else {
        return false;
    };
    let Some((object, suffix)) = rest.split_once(".tmp-") else {
        return false;
    };
    if !object.ends_with(".wal") && !object.ends_with(".seal") {
        return false;
    }
    let mut fields = suffix.split('-');
    matches!(
        (fields.next(), fields.next(), fields.next()),
        (Some(pid), Some(sequence), None)
            if !pid.is_empty()
                && pid.bytes().all(|byte| byte.is_ascii_digit())
                && !sequence.is_empty()
                && sequence.bytes().all(|byte| byte.is_ascii_digit())
    )
}

impl RotationReason {
    fn priority(self) -> u8 {
        match self {
            Self::Timer => 0,
            Self::Size => 1,
            Self::Backup => 2,
            Self::Barrier => 3,
            Self::Shutdown => 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_keeps_old_segment_authoritative_until_install() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        assert_eq!(manager.active_segment_id(), 1);
        assert_eq!(manager.begin_sealing(8192, 4096).unwrap(), 2);
        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealing);
        manager.mark_sealed(1).unwrap();
        assert!(manager.segment(1).unwrap().archive_pin);
        assert_eq!(manager.install_successor().unwrap(), 2);
        assert_eq!(manager.segment(2).unwrap().state, SegmentState::Active);
        assert_eq!(manager.active_segment_id(), 2);
    }

    #[test]
    fn pins_block_reclamation_until_released() {
        let mut manager = PitrSegmentManager::new(1, 32 * 1024).unwrap();
        manager.begin_sealing(4096, 4096).unwrap();
        manager.mark_sealed(1).unwrap();
        assert_eq!(manager.pending_successor_id().unwrap(), 2);
        manager.install_successor().unwrap();
        manager.mark_archived(1).unwrap();
        assert!(manager.segment(1).unwrap().archive_pin);
        manager.pin(1).unwrap();
        assert!(manager.mark_reclaimable(1).is_err());
        manager.unpin(1).unwrap();
        manager.mark_reclaimable(1).unwrap();
        assert!(manager.reclaim(1).is_err());
        manager.release_archive_pin(1).unwrap();
        assert_eq!(manager.reclaim(1).unwrap().state, SegmentState::Reclaiming);
        assert!(
            manager
                .complete_reclaim(
                    1,
                    CleanupReceipt {
                        wal_unlinked: true,
                        seal_unlinked: true,
                        directory_synced: false
                    }
                )
                .is_err()
        );
        assert_eq!(
            manager
                .complete_reclaim(1, CleanupReceipt::durable())
                .unwrap()
                .state,
            SegmentState::Reclaiming
        );
    }

    #[test]
    fn rotation_requests_coalesce() {
        let mut manager = PitrSegmentManager::new(1, 16 * 1024).unwrap();
        assert!(manager.request_rotation(RotationReason::Timer));
        assert!(manager.request_rotation(RotationReason::Backup));
        assert_eq!(
            manager.take_rotation_request(),
            Some(RotationReason::Backup)
        );
        assert!(manager.request_rotation(RotationReason::Backup));
    }

    #[test]
    fn lifecycle_rejects_premature_or_unknown_transitions() {
        let mut manager = PitrSegmentManager::new(1, 16 * 1024).unwrap();
        assert!(manager.install_successor().is_err());
        assert!(manager.mark_sealed(99).is_err());
        assert!(manager.begin_sealing(4096, 0).is_err());
        manager.begin_sealing(4096, 4096).unwrap();
        assert!(manager.install_successor().is_err());
        manager.mark_sealed(1).unwrap();
        manager.install_successor().unwrap();
        assert!(manager.mark_archived(99).is_err());
    }

    #[test]
    fn successor_install_waits_for_durable_wal_and_is_retryable() {
        let mut manager = PitrSegmentManager::new(1, 16 * 1024).unwrap();
        manager.begin_sealing(4096, 4096).unwrap();
        manager.mark_sealed(1).unwrap();
        assert!(
            manager
                .install_successor_after_wal(|_| anyhow::bail!("wal install failed"))
                .is_err()
        );
        assert_eq!(manager.segment(1).unwrap().state, SegmentState::Sealed);
        assert_eq!(
            manager
                .install_successor_after_wal(|id| {
                    assert_eq!(id, 2);
                    Ok(())
                })
                .unwrap(),
            2
        );
    }

    #[test]
    fn segment_id_exhaustion_is_reported() {
        assert!(PitrSegmentManager::new(u64::MAX, 16 * 1024).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn successor_wal_install_is_no_replace_and_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment-1.wal");
        let header = crate::pitr::WalV5Header {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(1),
            predecessor: crate::pitr::ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };
        install_v5_wal_header(&path, header).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
        assert!(install_v5_wal_header(&path, header).is_ok());
        let mismatched = crate::pitr::WalV5Header {
            segment_id: crate::pitr::SegmentId(2),
            ..header
        };
        assert!(install_v5_wal_header(&path, mismatched).is_err());
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(crate::pitr::decode_v5_file_header(&bytes).unwrap(), header);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wal_header_install_rejects_existing_payload_or_partial_header() {
        let dir = tempfile::tempdir().unwrap();
        let header = crate::pitr::WalV5Header {
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(1),
            predecessor: crate::pitr::ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };

        let payload_path = dir.path().join("payload.wal");
        install_v5_wal_header(&payload_path, header).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&payload_path)
            .unwrap()
            .write_all(&[1])
            .unwrap();
        assert!(install_v5_wal_header(&payload_path, header).is_err());

        let partial_path = dir.path().join("partial.wal");
        std::fs::write(&partial_path, b"WAL2").unwrap();
        assert!(install_v5_wal_header(&partial_path, header).is_err());
    }
}
