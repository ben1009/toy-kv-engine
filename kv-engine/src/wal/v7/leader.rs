//! Single-owner v7 WAL commits using the embedded FRONTIER protocol.
//!
//! This is the serial Leader leg used to establish the on-disk protocol before
//! wiring the same marker cycle into the parallel runtime. The caller supplies
//! an installed Active recovery seed and exclusive ownership of both file
//! handles. A successful call writes one ticket's DATA frames, writes its
//! canonical FRONTIER, calls `fdatasync` once, and only then returns success.

use std::{
    fs::File,
    io,
    os::unix::{
        fs::{FileExt, MetadataExt},
        io::AsRawFd,
    },
};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{LIVE_WAL_V5_LIMITS, RecordedAt, WalBatch, WalV5Limits},
    wal::{DirectBuf, MAX_WAL_FILE_SIZE},
};

use super::{
    allocator::{WalV7FrontierCursor, WalV7PhysicalAllocator},
    codec::{
        WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7FrameKind, WalV7Frontier, WalV7Header,
        data_fragment_count, decode_frame_structural, encode_data_frames_into,
        encode_frontier_successor, encode_generation_zero_frontier,
        encode_logical_batch_with_recorded_at, frame_record_digest,
    },
    install::{WalV7RuntimeMetadata, WalV7RuntimeSeed},
};

/// The result of one durably committed v7 Leader batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV7LeaderCommit {
    /// Segment ticket assigned to this batch.
    pub(crate) ticket: u64,
    /// First physical DATA frame offset, for seal-index maintenance.
    pub(crate) first_data_offset: u64,
    /// Frontier cursor established by the successful WAL sync.
    pub(crate) frontier: WalV7FrontierCursor,
}

/// A serial v7 WAL writer resumed from a durably installed Active image.
///
/// The caller must serialize access to this writer and retain exclusive
/// ownership of the active image. The write handle must support 4 KiB aligned
/// positional writes without `O_APPEND`; the sync handle must refer to the same inode. The
/// allocator reserves DATA and one worst-case FRONTIER before the first write.
pub(crate) struct WalV7LeaderWriter {
    write_file: File,
    sync_file: File,
    header_digest: [u8; 32],
    data_start: u64,
    append_offset: u64,
    installed_dev: u64,
    installed_ino: u64,
    allocator: WalV7PhysicalAllocator,
    metadata: WalV7RuntimeMetadata,
    poisoned: bool,
}

impl WalV7LeaderWriter {
    /// Resume a v7 writer from installed recovery metadata.
    ///
    /// `max_file_size` is the segment's complete physical cap, including
    /// embedded DATA and FRONTIER frames. The writer validates the immutable
    /// header on `write_file` and derives the frame-binding digest from those bytes.
    /// The caller must provide the current seed: no DATA admission may have
    /// followed its capture on this inode. After an uncertain write or sync,
    /// exclusive recovery must install a fresh image before another writer starts.
    pub(crate) fn new(
        write_file: File,
        sync_file: File,
        seed: WalV7RuntimeSeed,
        max_file_size: u64,
    ) -> Result<Self> {
        validate_positional_write_handle(&write_file)
            .context("validate v7 WAL positional write handle")?;
        let write_metadata = write_file
            .metadata()
            .context("inspect v7 WAL write handle")?;
        let sync_metadata = sync_file.metadata().context("inspect v7 WAL sync handle")?;
        ensure!(
            write_metadata.dev() == sync_metadata.dev()
                && write_metadata.ino() == sync_metadata.ino(),
            "v7 WAL write and sync handles refer to different files"
        );
        ensure!(
            write_metadata.dev() == seed.installed_dev
                && write_metadata.ino() == seed.installed_ino,
            "v7 WAL handles do not refer to the installed recovery inode"
        );
        ensure!(
            write_metadata.len() >= seed.append_offset,
            "v7 WAL is shorter than its installed append cursor"
        );
        ensure!(
            write_metadata.len() <= max_file_size,
            "v7 WAL physical length exceeds the segment limit"
        );
        let header_digest = read_header_digest(&write_file)?;
        ensure!(
            seed.data_start >= (WAL_V7_HEADER_LEN + WAL_V7_FRAME_LEN) as u64
                && seed.data_start <= MAX_WAL_FILE_SIZE
                && (seed.data_start..=MAX_WAL_FILE_SIZE).contains(&seed.append_offset)
                && seed.append_offset.is_multiple_of(WAL_V7_FRAME_LEN as u64),
            "invalid v7 WAL runtime seed append offset"
        );
        seed.metadata
            .validate_frontier(seed.frontier_cursor.frontier, seed.append_offset)?;

        let allocator = WalV7PhysicalAllocator::new(
            seed.next_ticket,
            seed.append_offset,
            max_file_size,
            seed.frontier_cursor,
        )?;
        validate_seed_frontier(&write_file, header_digest, seed.frontier_cursor)?;

        Ok(Self {
            write_file,
            sync_file,
            header_digest,
            data_start: seed.data_start,
            append_offset: seed.append_offset,
            installed_dev: seed.installed_dev,
            installed_ino: seed.installed_ino,
            allocator,
            metadata: seed.metadata,
            poisoned: false,
        })
    }

    /// Write and durably publish one ticketed batch using one WAL sync.
    ///
    /// Recorded time is clamped to the last accepted v7 batch. Any failure
    /// after a DATA allocation poisons this writer; exclusive Active recovery
    /// must reconcile the uncertain suffix before another writer can append.
    /// Caller-supplied limits can tighten the live recovery bounds, but cannot
    /// permit batches that the live v7 decoder would reject.
    ///
    /// # Errors
    /// Returns an error if the batch is invalid, the segment lacks capacity,
    /// any DATA or FRONTIER write is short or fails, the sync fails, or the
    /// post-sync frontier cursor cannot be validated.
    pub(crate) fn append_batch(
        &mut self,
        batch: &WalBatch,
        limits: WalV5Limits,
    ) -> Result<WalV7LeaderCommit> {
        ensure!(!self.poisoned, "v7 Leader WAL is poisoned");
        let limits = WalV5Limits {
            max_input_entry_count: limits
                .max_input_entry_count
                .min(LIVE_WAL_V5_LIMITS.max_input_entry_count),
            max_batch_data_bytes: limits
                .max_batch_data_bytes
                .min(LIVE_WAL_V5_LIMITS.max_batch_data_bytes),
            max_entry_count: limits
                .max_entry_count
                .min(LIVE_WAL_V5_LIMITS.max_entry_count),
            max_key_bytes: limits.max_key_bytes.min(LIVE_WAL_V5_LIMITS.max_key_bytes),
            max_value_bytes: limits
                .max_value_bytes
                .min(LIVE_WAL_V5_LIMITS.max_value_bytes),
        };

        let ticket = self.allocator.next_ticket();
        let previous_cursor = self.allocator.last_frontier();
        ensure!(
            batch.commit_ts > previous_cursor.frontier.last_commit_ts,
            "v7 Leader batch commit timestamp does not advance"
        );
        let recorded_at = self
            .metadata
            .last_recorded_at
            .map_or(batch.recorded_at, |previous| {
                previous.max(batch.recorded_at)
            });
        let logical_batch =
            encode_logical_batch_with_recorded_at(ticket, batch, recorded_at, limits)?;
        let fragment_count = data_fragment_count(u64::try_from(logical_batch.len())?)
            .context("v7 logical batch has no valid DATA fragment count")?;
        self.allocator.check_data_capacity(fragment_count)?;
        let data_buffer_len = usize::try_from(fragment_count)?
            .checked_mul(WAL_V7_FRAME_LEN)
            .context("v7 DATA buffer length overflows")?;
        let mut data_buffer = DirectBuf::new(data_buffer_len);

        // A capacity rejection here is still pre-admission: reserve_data does
        // not advance either cursor unless the full DATA-plus-marker budget
        // fits the segment.
        let allocation = self.allocator.reserve_data(fragment_count)?;
        let result = self.append_assigned_batch(
            allocation,
            &logical_batch,
            batch.commit_ts,
            recorded_at,
            limits,
            &mut data_buffer,
        );
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Return the cursor and hash state after the last successful commit.
    /// This snapshot remains current only until the next DATA admission. A clean
    /// ownership handoff must stop appending after capture; a poisoned writer
    /// requires exclusive recovery, including when an older snapshot is available.
    pub(crate) fn runtime_seed(&self) -> Result<WalV7RuntimeSeed> {
        ensure!(!self.poisoned, "cannot seed a poisoned v7 Leader WAL");
        self.metadata
            .validate_frontier(self.allocator.last_frontier().frontier, self.append_offset)?;

        Ok(WalV7RuntimeSeed {
            data_start: self.data_start,
            append_offset: self.append_offset,
            next_ticket: self.allocator.next_ticket(),
            installed_dev: self.installed_dev,
            installed_ino: self.installed_ino,
            frontier_cursor: self.allocator.last_frontier(),
            metadata: WalV7RuntimeMetadata {
                logical_hasher: self.metadata.logical_hasher.clone(),
                physical_hasher: self.metadata.physical_hasher.clone(),
                physical_hashed_end: self.metadata.physical_hashed_end,
                last_recorded_at: self.metadata.last_recorded_at,
            },
        })
    }

    fn append_assigned_batch(
        &mut self,
        allocation: super::allocator::WalV7DataAllocation,
        logical_batch: &[u8],
        commit_ts: u64,
        recorded_at: RecordedAt,
        limits: WalV5Limits,
        data_buffer: &mut DirectBuf,
    ) -> Result<WalV7LeaderCommit> {
        let ticket = allocation.ticket();
        let previous_cursor = self.allocator.last_frontier();
        let mut frontier_reservation = None;
        let result = (|| {
            ensure!(
                ticket.checked_add(1) == Some(self.allocator.next_ticket())
                    && allocation.frame_range().start == self.append_offset,
                "v7 Leader DATA allocation disagrees with its append cursor"
            );
            let encoded_len = encode_data_frames_into(
                self.header_digest,
                allocation.frame_range().start,
                ticket,
                logical_batch,
                limits,
                data_buffer.as_mut_slice(),
            )?;
            ensure!(
                encoded_len == data_buffer.cap(),
                "v7 Leader materializer returned an unexpected DATA length"
            );
            data_buffer.set_len(encoded_len);

            let ticket_end = ticket
                .checked_add(1)
                .context("v7 Leader ticket end overflows")?;
            let reservation = self.allocator.reserve_frontier(ticket_end)?;
            frontier_reservation = Some(reservation.clone());
            let mut logical_hasher = self.metadata.logical_hasher.clone();
            logical_hasher.update(logical_batch);
            let prefix_digest: [u8; 32] = logical_hasher.clone().finalize().into();
            let frontier = WalV7Frontier {
                generation: previous_cursor
                    .frontier
                    .generation
                    .checked_add(1)
                    .context("v7 Leader frontier generation overflows")?,
                ticket_end,
                durable_end: reservation.durable_end(),
                last_commit_ts: commit_ts,
                prefix_digest,
                previous_frontier_offset: previous_cursor.frame_offset,
                previous_frontier_digest: previous_cursor.frame_digest,
            };
            let frontier_frame = encode_frontier_successor(
                frontier,
                previous_cursor.frontier,
                previous_cursor.frame_offset,
                &previous_cursor.frame_digest,
                reservation.frame_offset(),
                self.header_digest,
            )?;

            let mut frontier_buffer = DirectBuf::new(WAL_V7_FRAME_LEN);
            frontier_buffer.as_mut_slice()[..WAL_V7_FRAME_LEN].copy_from_slice(&frontier_frame);
            frontier_buffer.set_len(WAL_V7_FRAME_LEN);

            let mut physical_hasher = self.metadata.physical_hasher.clone();
            physical_hasher.update(data_buffer.initialized_slice(0, encoded_len));
            physical_hasher.update(frontier_buffer.initialized_slice(0, WAL_V7_FRAME_LEN));
            let append_offset = reservation
                .frame_offset()
                .checked_add(WAL_V7_FRAME_LEN as u64)
                .context("v7 Leader frontier end overflows")?;
            ensure!(
                self.metadata.physical_hashed_end == allocation.frame_range().start,
                "v7 Leader physical hash cursor does not match its DATA allocation"
            );

            write_aligned_once(
                &self.write_file,
                data_buffer.initialized_slice(0, encoded_len),
                allocation.frame_range().start,
            )
            .context("write v7 Leader DATA frames")?;
            write_aligned_once(
                &self.write_file,
                frontier_buffer.initialized_slice(0, WAL_V7_FRAME_LEN),
                reservation.frame_offset(),
            )
            .context("write v7 Leader FRONTIER")?;
            fdatasync_once(&self.sync_file).context("fdatasync v7 Leader WAL")?;

            let frontier_cursor = WalV7FrontierCursor {
                frontier,
                frame_offset: reservation.frame_offset(),
                frame_digest: frame_record_digest(&frontier_frame)?,
            };
            self.allocator.complete_frontier_sync(
                frontier_reservation
                    .take()
                    .expect("v7 Leader marker reservation remains owned through sync"),
                frontier_cursor,
            )?;
            self.metadata.logical_hasher = logical_hasher;
            self.metadata.physical_hasher = physical_hasher;
            self.metadata.physical_hashed_end = append_offset;
            self.metadata.last_recorded_at = Some(recorded_at);
            self.append_offset = append_offset;

            Ok(WalV7LeaderCommit {
                ticket,
                first_data_offset: allocation.frame_range().start,
                frontier: frontier_cursor,
            })
        })();

        if result.is_err()
            && let Some(reservation) = frontier_reservation.take()
        {
            self.allocator
                .poison_frontier(reservation)
                .context("poison v7 Leader after an uncertain FRONTIER cycle")?;
        }

        result
    }
}

fn validate_positional_write_handle(file: &File) -> io::Result<()> {
    let flags = loop {
        // SAFETY: the descriptor is live, and F_GETFL takes no extra argument.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if flags >= 0 {
            break flags;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    if flags & libc::O_APPEND != 0 {
        // Linux pwrite ignores its offset when the descriptor uses O_APPEND.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "v7 WAL positional write handle uses O_APPEND",
        ));
    }

    Ok(())
}

fn write_aligned_once(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    if bytes.is_empty()
        || !bytes.len().is_multiple_of(WAL_V7_FRAME_LEN)
        || !offset.is_multiple_of(WAL_V7_FRAME_LEN as u64)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "v7 positional write is not frame aligned",
        ));
    }

    loop {
        match file.write_at(bytes, offset) {
            Ok(written) if written == bytes.len() => return Ok(()),
            Ok(written) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "short v7 positional write: {written} of {} bytes",
                        bytes.len()
                    ),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn read_header_digest(file: &File) -> Result<[u8; 32]> {
    let mut header_buffer = DirectBuf::new(WAL_V7_HEADER_LEN);
    let header_bytes = header_buffer.as_mut_slice();
    let read = loop {
        match file.read_at(header_bytes, 0) {
            Ok(read) if read == WAL_V7_HEADER_LEN => break read,
            Ok(read) => {
                anyhow::bail!("short v7 WAL header read: {read} of {WAL_V7_HEADER_LEN} bytes");
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error).context("read v7 WAL immutable header"),
        }
    };
    header_buffer.set_len(read);

    let header_bytes = header_buffer.initialized_slice(0, read);
    let header = WalV7Header::decode(header_bytes).context("decode v7 WAL immutable header")?;
    ensure!(
        header.encode()?.as_slice() == header_bytes,
        "v7 WAL immutable header is not canonical"
    );
    Ok(Sha256::digest(header_bytes).into())
}

fn validate_seed_frontier(
    file: &File,
    header_digest: [u8; 32],
    expected: WalV7FrontierCursor,
) -> Result<()> {
    let mut frame_buffer = DirectBuf::new(WAL_V7_FRAME_LEN);
    let frame_bytes = frame_buffer.as_mut_slice();
    let read = loop {
        match file.read_at(frame_bytes, expected.frame_offset) {
            Ok(read) if read == WAL_V7_FRAME_LEN => break read,
            Ok(read) => {
                anyhow::bail!(
                    "short v7 installed FRONTIER read at {}: {read} of {WAL_V7_FRAME_LEN} bytes",
                    expected.frame_offset
                );
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error).context("read installed v7 FRONTIER frame"),
        }
    };
    frame_buffer.set_len(read);

    let frame_bytes = frame_buffer.initialized_slice(0, read);
    let decoded = decode_frame_structural(frame_bytes, expected.frame_offset, &header_digest)
        .context("validate installed v7 FRONTIER frame")?;
    ensure!(
        decoded.kind == WalV7FrameKind::Frontier,
        "installed v7 recovery cursor points to a DATA frame"
    );
    let frontier = WalV7Frontier::decode_body(&decoded.body)?;
    ensure!(
        frontier == expected.frontier && frame_record_digest(frame_bytes)? == expected.frame_digest,
        "installed v7 FRONTIER frame disagrees with its recovery seed"
    );
    if frontier.generation == 0 {
        ensure!(
            frame_bytes == encode_generation_zero_frontier(header_digest)?,
            "installed v7 generation-zero FRONTIER is not canonical"
        );
    }

    Ok(())
}

fn fdatasync_once(file: &File) -> io::Result<()> {
    loop {
        // SAFETY: the file descriptor is live and owned by `file`.
        let result = unsafe { libc::fdatasync(file.as_raw_fd()) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::OpenOptions, os::unix::fs::OpenOptionsExt, path::Path};

    use super::*;
    use crate::{
        pitr::{
            ArchiveEpochId, ChainAnchor, LIVE_WAL_V5_LIMITS, SegmentId, TimelineId, WalEntry,
            manifest::ActiveBoundary,
        },
        wal::v7::{
            codec::WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY,
            install::{WalV7InstalledRecovery, recover_and_install_active},
            recovery::WalV7RecoveryAuthority,
        },
    };

    fn test_header() -> WalV7Header {
        WalV7Header {
            timeline_id: TimelineId([1; 16]),
            archive_epoch_id: ArchiveEpochId([2; 16]),
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis {
                archive_epoch_id: ArchiveEpochId([2; 16]),
            },
            incarnation: [3; 16],
        }
    }

    fn recover(path: &Path, anchor: ActiveBoundary) -> Result<WalV7InstalledRecovery> {
        Ok(recover_and_install_active(
            path,
            WalV7RecoveryAuthority::Active {
                predecessor: test_header().predecessor,
                durable_anchors: vec![anchor],
            },
        )?
        .installed)
    }

    fn install_empty(path: &Path) -> Result<WalV7InstalledRecovery> {
        let header = test_header();
        let mut image = header.encode()?.to_vec();
        image.extend_from_slice(&encode_generation_zero_frontier(header.digest()?)?);
        fs::write(path, image)?;
        recover(
            path,
            ActiveBoundary {
                timeline_id: header.timeline_id.0,
                archive_epoch_id: header.archive_epoch_id.0,
                segment_id: header.segment_id.0,
                incarnation: header.incarnation,
                ticket_end: 0,
                durable_end: WAL_V7_HEADER_LEN as u64,
                last_commit_ts: 0,
                prefix_digest: header.digest()?,
            },
        )
    }

    fn write_handle(path: &Path) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?)
    }

    fn open_writer(
        path: &Path,
        installed: &WalV7InstalledRecovery,
        max_file_size: u64,
    ) -> Result<WalV7LeaderWriter> {
        WalV7LeaderWriter::new(
            write_handle(path)?,
            File::open(path)?,
            installed.runtime_seed()?,
            max_file_size,
        )
    }

    fn batch(commit_ts: u64, secs: i64, value_len: usize) -> WalBatch {
        WalBatch {
            commit_ts,
            recorded_at: RecordedAt { secs, nanos: 5 },
            entries: vec![WalEntry::Put {
                key: commit_ts.to_be_bytes().to_vec(),
                value: vec![0x5a; value_len],
            }],
        }
    }

    #[test]
    fn leader_commits_fragmented_batches_and_resumes_after_recovery() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let anchor = installed.active_boundary;
        let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let first = batch(10, 100, 2 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY);
        let mut second = batch(20, 90, 10);

        assert_eq!(writer.append_batch(&first, LIVE_WAL_V5_LIMITS)?.ticket, 0);
        let committed = writer.append_batch(&second, LIVE_WAL_V5_LIMITS)?;
        assert_eq!(committed.ticket, 1);
        assert_eq!(committed.frontier.frontier.ticket_end, 2);
        let seed = writer.runtime_seed()?;
        let expected_physical_digest: [u8; 32] = Sha256::digest(fs::read(&path)?).into();
        let actual_physical_digest: [u8; 32] = seed.metadata.physical_hasher.finalize().into();
        assert_eq!(actual_physical_digest, expected_physical_digest);
        drop(writer);

        let installed = recover(&path, anchor)?;
        second.recorded_at = first.recorded_at;
        assert_eq!(installed.batches, [first, second]);
        assert_eq!(installed.next_ticket, 2);
        let recovered_anchor = installed.active_boundary;
        let mut resumed = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let third = batch(30, 101, 4);
        assert_eq!(resumed.append_batch(&third, LIVE_WAL_V5_LIMITS)?.ticket, 2);
        drop(resumed);

        let recovered = recover(&path, recovered_anchor)?;
        assert_eq!(recovered.next_ticket, 3);
        assert_eq!(recovered.batches.last(), Some(&third));

        Ok(())
    }

    #[test]
    fn leader_uses_seed_cursor_with_preallocated_tail() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let anchor = installed.active_boundary;
        let preallocated_len = 12 * WAL_V7_FRAME_LEN as u64;
        write_handle(&path)?.set_len(preallocated_len)?;
        let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let batch = batch(10, 100, 4);

        let committed = writer.append_batch(&batch, LIVE_WAL_V5_LIMITS)?;
        assert_eq!(committed.first_data_offset, installed.append_offset);
        assert_eq!(fs::metadata(&path)?.len(), preallocated_len);
        let committed_end = writer.runtime_seed()?.append_offset;
        drop(writer);

        let recovered = recover(&path, anchor)?;
        assert_eq!(recovered.batches, [batch]);
        assert_eq!(recovered.image_len, committed_end);
        assert_eq!(fs::metadata(&path)?.len(), committed_end);

        Ok(())
    }

    #[test]
    fn leader_rejects_capacity_before_assigning_a_ticket() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let original_image = fs::read(&path)?;
        let mut writer = open_writer(&path, &installed, 4 * WAL_V7_FRAME_LEN as u64)?;

        assert!(
            writer
                .append_batch(
                    &batch(10, 100, 2 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY),
                    LIVE_WAL_V5_LIMITS,
                )
                .is_err()
        );
        assert_eq!(fs::read(&path)?, original_image);
        assert_eq!(writer.runtime_seed()?.next_ticket, 0);
        assert_eq!(
            writer.runtime_seed()?.append_offset,
            installed.append_offset
        );
        assert_eq!(
            writer
                .append_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS)?
                .ticket,
            0
        );

        Ok(())
    }

    #[test]
    fn leader_sync_failure_poison_prevents_further_writes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        // An O_PATH descriptor has the same inode metadata but cannot sync.
        let sync_handle = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH)
            .open(&path)?;
        let mut writer = WalV7LeaderWriter::new(
            write_handle(&path)?,
            sync_handle,
            installed.runtime_seed()?,
            MAX_WAL_FILE_SIZE,
        )?;
        let previous = writer.allocator.last_frontier();

        let error = writer
            .append_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS)
            .expect_err("fdatasync must reject an O_PATH descriptor");
        assert!(format!("{error:#}").contains("fdatasync"));
        assert_eq!(writer.allocator.last_frontier(), previous);
        assert!(writer.runtime_seed().is_err());
        let uncertain_image = fs::read(&path)?;
        assert!(
            writer
                .append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS)
                .is_err()
        );
        assert_eq!(fs::read(&path)?, uncertain_image);

        Ok(())
    }

    #[test]
    fn leader_rejects_descriptors_from_a_replaced_recovery_inode() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let initial = install_empty(&path)?;
        let stale_write = write_handle(&path)?;
        let stale_sync = File::open(&path)?;
        let original_image = fs::read(&path)?;
        let installed = recover(&path, initial.active_boundary)?;
        assert_eq!(fs::read(&path)?, original_image);

        let result = WalV7LeaderWriter::new(
            stale_write,
            stale_sync,
            installed.runtime_seed()?,
            MAX_WAL_FILE_SIZE,
        );
        let error = result.err().expect("stale recovery descriptors must fail");
        assert!(error.to_string().contains("installed recovery inode"));
        assert_eq!(fs::read(&path)?, original_image);

        Ok(())
    }

    #[test]
    fn leader_rejects_append_handle_with_preallocated_tail() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        write_handle(&path)?.set_len(12 * WAL_V7_FRAME_LEN as u64)?;
        let original_image = fs::read(&path)?;
        let append_handle = OpenOptions::new()
            .read(true)
            .append(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;

        let result = WalV7LeaderWriter::new(
            append_handle,
            File::open(&path)?,
            installed.runtime_seed()?,
            MAX_WAL_FILE_SIZE,
        );
        assert!(result.is_err(), "O_APPEND ignores positional write offsets");
        assert_eq!(fs::read(&path)?, original_image);

        Ok(())
    }

    #[test]
    fn leader_rejects_physical_tail_beyond_segment_limit() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let frame_len = WAL_V7_FRAME_LEN as u64;

        for (physical_len, limit) in [
            (5 * frame_len, 4 * frame_len),
            (MAX_WAL_FILE_SIZE + frame_len, MAX_WAL_FILE_SIZE),
        ] {
            write_handle(&path)?.set_len(physical_len)?;
            assert!(
                open_writer(&path, &installed, limit).is_err(),
                "preallocated EOF {physical_len} exceeds segment limit {limit}"
            );
            assert_eq!(fs::metadata(&path)?.len(), physical_len);
        }

        Ok(())
    }

    #[test]
    fn leader_rejects_batches_outside_live_recovery_limits() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let original_image = fs::read(&path)?;
        let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let oversized_value_len = LIVE_WAL_V5_LIMITS.max_value_bytes + 1;
        let wider_limits = WalV5Limits {
            max_value_bytes: oversized_value_len,
            ..LIVE_WAL_V5_LIMITS
        };

        assert!(
            writer
                .append_batch(&batch(10, 100, oversized_value_len), wider_limits)
                .is_err(),
            "a durably committed batch must be readable under live recovery limits"
        );
        assert_eq!(writer.runtime_seed()?.next_ticket, 0);
        assert_eq!(fs::read(&path)?, original_image);

        let stricter_limits = WalV5Limits {
            max_value_bytes: 3,
            ..LIVE_WAL_V5_LIMITS
        };
        assert!(
            writer
                .append_batch(&batch(10, 100, 4), stricter_limits)
                .is_err()
        );
        assert_eq!(fs::read(&path)?, original_image);

        let batch = batch(10, 100, 4);
        assert_eq!(writer.append_batch(&batch, wider_limits)?.ticket, 0);
        drop(writer);
        assert_eq!(recover(&path, installed.active_boundary)?.batches, [batch]);

        Ok(())
    }
}
