//! Single-owner v7 WAL commits using the embedded FRONTIER protocol.
//!
//! This is the serial Leader leg used to establish the on-disk protocol before
//! wiring the same marker cycle into the parallel runtime. The caller supplies
//! an installed Active recovery seed and exclusive ownership of both file
//! handles. A successful call writes one ticket's DATA frames, writes its
//! canonical FRONTIER, calls `fdatasync` once, and only then returns success.

use std::{
    error::Error,
    fmt,
    fs::File,
    io,
    os::unix::{
        fs::{FileExt, MetadataExt},
        io::AsRawFd,
    },
    sync::Arc,
};

#[cfg(feature = "bench")]
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{CanonicalWalEntryStream, LIVE_WAL_V5_LIMITS, RecordedAt, WalBatch, WalV5Limits},
    wal::{DirectBuf, MAX_WAL_FILE_SIZE},
};

use super::{
    admission::WalV7ReservationOwner,
    allocator::{WalV7DataCapacityStatus, WalV7FrontierCursor, WalV7PhysicalAllocator},
    codec::{
        WAL_V7_FRAME_LEN, WAL_V7_HEADER_LEN, WalV7FrameKind, WalV7Frontier, WalV7Header,
        data_fragment_count, decode_frame_structural, encode_data_frames_into,
        encode_frontier_successor_into, encode_generation_zero_frontier, frame_record_digest,
        prepare_logical_batch,
    },
    install::{WalV7RuntimeMetadata, WalV7RuntimeSeed},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV7LeaderCapacityError {
    RotationRequired,
    BatchTooLarge,
}

impl fmt::Display for WalV7LeaderCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RotationRequired => "v7 WAL segment rotation required before retrying batch",
            Self::BatchTooLarge => "v7 WAL batch cannot fit a fresh segment",
        })
    }
}

impl Error for WalV7LeaderCapacityError {}

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
    reservation_owner: Arc<WalV7ReservationOwner>,
    #[cfg(feature = "bench")]
    profile: Option<Arc<crate::mem_table::WriteProfile>>,
    poisoned: bool,
}

struct WalV7LeaderBuffers {
    data: DirectBuf,
    frontier: DirectBuf,
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
    /// The reservation owner must cover this installed inode; constructing it
    /// charges the recovered prefix before the writer can open.
    pub(crate) fn new(
        write_file: File,
        sync_file: File,
        seed: WalV7RuntimeSeed,
        max_file_size: u64,
        reservation_owner: Arc<WalV7ReservationOwner>,
    ) -> Result<Self> {
        reservation_owner.validate_image(seed.installed_dev, seed.installed_ino)?;
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
            header_digest == seed.header_digest,
            "v7 WAL immutable header digest does not match the recovered runtime seed"
        );
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
            reservation_owner,
            #[cfg(feature = "bench")]
            profile: None,
            poisoned: false,
        })
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_write_profile(&mut self, profile: Arc<crate::mem_table::WriteProfile>) {
        self.profile = Some(profile);
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
    /// Returns an error if the batch is invalid, resource admission fails,
    /// capacity requires rotation or the batch cannot fit a fresh segment,
    /// any DATA or FRONTIER write is short or fails, the sync fails, or the
    /// post-sync frontier cursor cannot be validated. Capacity errors are
    /// available as [`WalV7LeaderCapacityError`] through the anyhow chain.
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
        let preparation_buffer_bytes =
            CanonicalWalEntryStream::preparation_buffer_bytes(batch, limits)?;
        let preparation_reservation = self
            .reservation_owner
            .reserve_preparation_buffer(preparation_buffer_bytes)?;
        let prepared_batch = prepare_logical_batch(batch, limits)?;
        let encoded_batch_bytes = u64::try_from(prepared_batch.encoded_len())?;
        let fragment_count = data_fragment_count(encoded_batch_bytes)
            .context("v7 logical batch has no valid DATA fragment count")?;
        match self.allocator.data_capacity_status(fragment_count)? {
            WalV7DataCapacityStatus::Fits => {}
            WalV7DataCapacityStatus::RotationRequired => {
                return Err(anyhow::Error::new(
                    WalV7LeaderCapacityError::RotationRequired,
                ));
            }
            WalV7DataCapacityStatus::BatchTooLarge => {
                return Err(anyhow::Error::new(WalV7LeaderCapacityError::BatchTooLarge));
            }
        }
        let data_buffer_len = usize::try_from(fragment_count)?
            .checked_mul(WAL_V7_FRAME_LEN)
            .context("v7 DATA buffer length overflows")?;

        // Charge every admitted DATA/control/index byte before advancing the
        // physical allocator or allocating the potentially large frame buffer.
        // A capacity rejection below drops this token without consuming a WAL
        // ticket or offset.
        let mut reservation = self.reservation_owner.reserve_batch(encoded_batch_bytes)?;
        let logical_batch = prepared_batch.encode_with_recorded_at(ticket, recorded_at)?;
        drop(preparation_reservation);

        // A capacity rejection here is still pre-admission: reserve_data does
        // not advance either cursor unless the full DATA-plus-marker budget
        // fits the segment.
        // Allocate both charged direct buffers before assigning a ticket or
        // physical range, so allocation failure cannot consume coordinates.
        let mut buffers = WalV7LeaderBuffers {
            data: DirectBuf::new(data_buffer_len),
            frontier: DirectBuf::new(WAL_V7_FRAME_LEN),
        };
        let allocation = self.allocator.reserve_data(fragment_count)?;
        reservation.mark_admitted();
        // From this point a panic or error must force exclusive recovery: the
        // physical allocator has consumed the ticket and range.
        self.poisoned = true;
        let result = self.append_assigned_batch(
            allocation,
            &logical_batch,
            batch.commit_ts,
            recorded_at,
            limits,
            &mut buffers,
        );
        drop(buffers);
        drop(logical_batch);
        let retain_result = reservation.finish_after_io();
        match (result, retain_result) {
            (Ok(commit), Ok(())) => {
                self.poisoned = false;
                Ok(commit)
            }
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => {
                Err(anyhow::Error::new(error).context("retire v7 WAL transient buffer reservation"))
            }
            (Err(error), Err(retain_error)) => Err(error.context(format!(
                "also failed to retire v7 transient buffer reservation: {retain_error:#}"
            ))),
        }
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
            header_digest: self.header_digest,
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

    pub(crate) fn try_clone_sync_file(&self) -> Result<File> {
        self.sync_file
            .try_clone()
            .context("clone v7 WAL sync file handle")
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn append_assigned_batch(
        &mut self,
        allocation: super::allocator::WalV7DataAllocation,
        logical_batch: &[u8],
        commit_ts: u64,
        recorded_at: RecordedAt,
        limits: WalV5Limits,
        buffers: &mut WalV7LeaderBuffers,
    ) -> Result<WalV7LeaderCommit> {
        let WalV7LeaderBuffers {
            data: data_buffer,
            frontier: frontier_buffer,
        } = buffers;
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
            encode_frontier_successor_into(
                frontier_buffer.as_mut_slice(),
                frontier,
                previous_cursor.frontier,
                previous_cursor.frame_offset,
                &previous_cursor.frame_digest,
                reservation.frame_offset(),
                self.header_digest,
            )?;
            frontier_buffer.set_len(WAL_V7_FRAME_LEN);
            let frontier_bytes = frontier_buffer.initialized_slice(0, WAL_V7_FRAME_LEN);
            let frontier_digest = frame_record_digest(frontier_bytes)?;

            let mut physical_hasher = self.metadata.physical_hasher.clone();
            physical_hasher.update(data_buffer.initialized_slice(0, encoded_len));
            physical_hasher.update(frontier_bytes);
            let append_offset = reservation
                .frame_offset()
                .checked_add(WAL_V7_FRAME_LEN as u64)
                .context("v7 Leader frontier end overflows")?;
            ensure!(
                self.metadata.physical_hashed_end == allocation.frame_range().start,
                "v7 Leader physical hash cursor does not match its DATA allocation"
            );

            write_data_once(
                &self.write_file,
                data_buffer.initialized_slice(0, encoded_len),
                allocation.frame_range().start,
            )
            .context("write v7 Leader DATA frames")?;
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::fail_point!("wal.v7.leader.after_data_write");
            #[cfg(feature = "bench")]
            let frontier_write_started = self.profile.as_ref().map(|_| Instant::now());
            write_frontier_once(
                &self.write_file,
                frontier_buffer.initialized_slice(0, WAL_V7_FRAME_LEN),
                reservation.frame_offset(),
            )
            .context("write v7 Leader FRONTIER")?;
            #[cfg(feature = "bench")]
            if let (Some(profile), Some(started)) = (&self.profile, frontier_write_started) {
                profile.record_wal_commit_group(1, encoded_len as u64);
                profile.record_wal_inflight_groups(1);
                profile.record_wal_frontier_write(
                    WAL_V7_FRAME_LEN as u64,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            #[cfg(feature = "bench")]
            let sync_started = self.profile.as_ref().map(|_| Instant::now());
            let sync_result = fdatasync_once(&self.sync_file);
            #[cfg(feature = "bench")]
            if let (Some(profile), Some(started)) = (&self.profile, sync_started) {
                let duration_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                profile.record_wal_sync(1);
                profile.record_wal_fdatasync_ns(duration_ns);
                if profile.wal_sync_diagnostics_enabled() {
                    profile.record_wal_sync_observation(crate::mem_table::WalSyncObservation {
                        captured_target: ticket_end,
                        written_frontier_start: previous_cursor.frontier.ticket_end,
                        written_frontier_end: ticket_end,
                        groups_covered: 1,
                        duration_ns,
                        succeeded: sync_result.is_ok(),
                        ..crate::mem_table::WalSyncObservation::default()
                    });
                }
            }
            sync_result.context("fdatasync v7 Leader WAL")?;
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::wal_crash_point(
                "wal_v7.leader.after_fdatasync_before_publish",
            );

            let frontier_cursor = WalV7FrontierCursor {
                frontier,
                frame_offset: reservation.frame_offset(),
                frame_digest: frontier_digest,
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

fn write_data_once(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    write_aligned_once_with(file, bytes, offset, write_data_at)
}

fn write_frontier_once(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    write_aligned_once_with(file, bytes, offset, write_frontier_at)
}

fn write_aligned_once_with(
    file: &File,
    bytes: &[u8],
    offset: u64,
    mut write_at: impl FnMut(&File, &[u8], u64) -> io::Result<usize>,
) -> io::Result<()> {
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
        match write_at(file, bytes, offset) {
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

fn write_data_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::fail_point!("wal.v7.leader.data_short_write", |_| {
        write_one_byte_prefix(file, bytes, offset)
    });

    file.write_at(bytes, offset)
}

fn write_frontier_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::fail_point!("wal.v7.leader.frontier_short_write", |_| {
        write_one_byte_prefix(file, bytes, offset)
    });

    file.write_at(bytes, offset)
}

#[cfg(feature = "chaos-testing")]
fn write_one_byte_prefix(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    // The Leader write handle uses O_DIRECT, which may reject a one-byte write
    // before exercising the short-write path. Reopen the same inode through
    // procfs without O_DIRECT so the injected fault leaves a real partial frame.
    #[cfg(target_os = "linux")]
    {
        let buffered_path = format!("/proc/self/fd/{}", file.as_raw_fd());
        let buffered_file = File::options().write(true).open(buffered_path)?;
        buffered_file.write_at(&bytes[..1], offset)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, bytes, offset);
        Ok(1)
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
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::fail_point!("wal.v7.leader.fdatasync", |_| Err(io::Error::other(
        "injected v7 WAL sync failure"
    )));
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
    use std::{
        fs,
        fs::OpenOptions,
        os::unix::fs::OpenOptionsExt,
        path::Path,
        sync::{Arc, atomic::Ordering},
    };

    use super::*;
    use crate::{
        pitr::{
            ArchiveEpochId, ChainAnchor, LIVE_WAL_V5_LIMITS, SegmentId, TimelineId, WalEntry,
            manifest::ActiveBoundary,
        },
        wal::{
            Wal,
            v7::{
                admission::{
                    WalV7ReservationError, WalV7ReservationOwner, WalV7ResourceCharge,
                    WalV7ResourceLedger, WalV7ResourceLimits,
                },
                codec::{WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY, WAL_V7_FRAME_LEN},
                install::{WalV7InstalledRecovery, recover_and_install_active},
                recovery::WalV7RecoveryAuthority,
            },
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

    fn resource_limits() -> WalV7ResourceLimits {
        WalV7ResourceLimits {
            max_logical_wal_bytes: u64::MAX,
            max_source_spool_bytes: u64::MAX,
            max_recovery_workspace_bytes: u64::MAX,
            max_buffer_memory_bytes: u64::MAX,
            max_seal_index_bytes: u64::MAX,
        }
    }

    fn resource_owner(installed: &WalV7InstalledRecovery) -> Result<Arc<WalV7ReservationOwner>> {
        let ledger = WalV7ResourceLedger::new(resource_limits())?;
        Ok(Arc::new(WalV7ReservationOwner::new(ledger, installed)?))
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
            resource_owner(installed)?,
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
    fn wal_adapter_dispatches_v7_batches_as_already_durable() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let initial = install_empty(&path)?;
        let initial_anchor = initial.active_boundary;
        let first = batch(10, 100, 4);
        let mut first_writer = open_writer(&path, &initial, MAX_WAL_FILE_SIZE)?;
        first_writer.append_batch(&first, LIVE_WAL_V5_LIMITS)?;
        drop(first_writer);

        let recovered = recover(&path, initial_anchor)?;
        assert_eq!(recovered.next_ticket, 1);
        let recovered_anchor = recovered.active_boundary;
        let writer = open_writer(&path, &recovered, MAX_WAL_FILE_SIZE)?;
        let wal = Wal::from_v7_leader(writer)?;
        assert_eq!(wal.next_ticket.load(Ordering::Acquire), 1);
        assert_eq!(
            wal.completion_state.durable_ticket.load(Ordering::Acquire),
            1
        );

        let mut second = batch(20, 90, 8);
        assert_eq!(wal.put_v5_batch(&second, LIVE_WAL_V5_LIMITS, None)?, 1);
        assert_eq!(wal.next_ticket.load(Ordering::Acquire), 2);
        assert_eq!(
            wal.completion_state.durable_ticket.load(Ordering::Acquire),
            2
        );
        let committed_end = wal.alloc_offset.load(Ordering::Acquire);

        // The v7 append already wrote and synced its FRONTIER. Ticket barriers
        // take the durable fast path; sync/close drain the serial writer, with
        // none of these barriers touching an absent ring.
        wal.submit_and_commit(1)?;
        wal.sync()?;
        wal.close()?;
        drop(wal);

        second.recorded_at = first.recorded_at;
        let installed = recover(&path, recovered_anchor)?;
        assert_eq!(installed.batches, [first, second]);
        assert_eq!(installed.next_ticket, 2);
        assert_eq!(installed.append_offset, committed_end);
        Ok(())
    }

    #[test]
    fn wal_adapter_keeps_v7_cursor_unchanged_on_pre_admission_rejection() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let original_image = fs::read(&path)?;
        let writer = open_writer(&path, &installed, 3 * WAL_V7_FRAME_LEN as u64)?;
        let wal = Wal::from_v7_leader(writer)?;

        let error = wal
            .put_v5_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS, None)
            .expect_err("batch plus FRONTIER cannot fit this segment");
        assert_eq!(
            error.downcast_ref::<WalV7LeaderCapacityError>(),
            Some(&WalV7LeaderCapacityError::BatchTooLarge)
        );
        assert_eq!(wal.next_ticket.load(Ordering::Acquire), 0);
        assert_eq!(
            wal.completion_state.durable_ticket.load(Ordering::Acquire),
            0
        );
        assert_eq!(
            wal.alloc_offset.load(Ordering::Acquire),
            installed.append_offset
        );
        wal.sync()?;
        wal.close()?;
        assert_eq!(fs::read(&path)?, original_image);
        Ok(())
    }

    #[test]
    fn wal_adapter_preserves_durable_ticket_after_later_sync_failure() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let wal = Wal::from_v7_leader(writer)?;
        let durable_ticket = wal.put_v5_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS, None)?;
        wal.v7_leader
            .as_ref()
            .context("adapter has a v7 writer")?
            .lock()
            .sync_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH)
            .open(&path)?;

        let error = wal
            .put_v5_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS, None)
            .expect_err("later fdatasync must reject an O_PATH descriptor");
        assert!(format!("{error:#}").contains("fdatasync"));
        wal.submit_and_commit(durable_ticket)?;
        assert!(wal.submit_and_commit(durable_ticket + 1).is_err());
        assert!(wal.sync().is_err());
        assert!(wal.close().is_err());
        let uncertain_image = fs::read(&path)?;
        assert!(
            wal.put_v5_batch(&batch(30, 102, 4), LIVE_WAL_V5_LIMITS, None)
                .is_err()
        );
        assert_eq!(fs::read(&path)?, uncertain_image);
        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_wal_adapter_barriers_reject_poison_after_append_unwind() -> Result<()> {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        use crate::chaos::failpoint::{self, FailScenario};

        for barrier in ["sync", "close", "ticket"] {
            let scenario = FailScenario::setup();
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("active.wal");
            let installed = install_empty(&path)?;
            let writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
            let wal = Wal::from_v7_leader(writer)?;
            failpoint::cfg("wal.v7.leader.after_data_write", "panic")
                .expect("arm v7 adapter unwind failpoint");
            let result = catch_unwind(AssertUnwindSafe(|| {
                wal.put_v5_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS, None)
            }));
            scenario.teardown();

            assert!(result.is_err());
            assert_eq!(wal.next_ticket.load(Ordering::Acquire), 0);
            assert_eq!(
                wal.completion_state.durable_ticket.load(Ordering::Acquire),
                0
            );
            let barrier_result = match barrier {
                "sync" => wal.sync(),
                "close" => wal.close(),
                "ticket" => wal.submit_and_commit(0),
                _ => unreachable!(),
            };
            assert!(barrier_result.is_err(), "{barrier} observes writer poison");
            let uncertain_image = fs::read(&path)?;
            assert!(
                wal.put_v5_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS, None)
                    .is_err()
            );
            assert_eq!(fs::read(&path)?, uncertain_image);
        }
        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_wal_adapter_sync_and_close_wait_for_admitted_append() -> Result<()> {
        use std::{thread, time::Duration};

        use crossbeam_channel::{RecvTimeoutError, bounded};

        use crate::chaos::failpoint::FailScenario;

        for close in [false, true] {
            for fail_sync in [false, true] {
                let scenario = FailScenario::setup();
                let directory = tempfile::tempdir()?;
                let path = directory.path().join("active.wal");
                let installed = install_empty(&path)?;
                let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
                if fail_sync {
                    writer.sync_file = OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_PATH)
                        .open(&path)?;
                }
                let wal = Arc::new(Wal::from_v7_leader(writer)?);
                let (admitted_tx, admitted_rx) = bounded(1);
                let (resume_tx, resume_rx) = bounded(1);
                fail::cfg_callback("wal.v7.leader.after_data_write", move || {
                    admitted_tx.send(()).expect("report admitted v7 append");
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("resume admitted v7 append");
                })
                .expect("pause v7 append after DATA write");
                let append_wal = Arc::clone(&wal);
                let append = thread::spawn(move || {
                    append_wal.put_v5_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS, None)
                });
                admitted_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("append reaches its admitted DATA write");

                let (started_tx, started_rx) = bounded(1);
                let (finished_tx, finished_rx) = bounded(1);
                let barrier_wal = Arc::clone(&wal);
                let barrier = thread::spawn(move || {
                    started_tx.send(()).expect("report barrier start");
                    let result = if close {
                        barrier_wal.close()
                    } else {
                        barrier_wal.sync()
                    };
                    finished_tx.send(result).expect("report barrier result");
                });
                started_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("barrier thread starts");
                let early_result = finished_rx.recv_timeout(Duration::from_millis(100));
                let returned_early = early_result.is_ok();
                resume_tx.send(()).expect("release admitted append");
                let append_result = append.join().expect("append thread joins");
                let barrier_result = match early_result {
                    Ok(result) => result,
                    Err(RecvTimeoutError::Timeout) => finished_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("barrier completes after append"),
                    Err(RecvTimeoutError::Disconnected) => panic!("barrier result disconnected"),
                };
                barrier.join().expect("barrier thread joins");
                scenario.teardown();

                assert!(!returned_early, "close={close}, fail_sync={fail_sync}");
                assert_eq!(append_result.is_err(), fail_sync);
                assert_eq!(barrier_result.is_err(), fail_sync);
                assert_eq!(
                    wal.next_ticket.load(Ordering::Acquire),
                    u64::from(!fail_sync)
                );
            }
        }
        Ok(())
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

    #[cfg(feature = "bench")]
    #[test]
    fn wal_adapter_profiles_v7_leader_frontier_and_sync() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let wal = Wal::from_v7_leader(writer)?;
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        profile.set_wal_sync_diagnostics_enabled(true);
        wal.set_write_profile(Arc::clone(&profile));

        let first = batch(10, 100, 4);
        assert_eq!(wal.put_v5_batch(&first, LIVE_WAL_V5_LIMITS, None)?, 0);
        wal.submit_and_commit(0)?;

        let snapshot = profile.snapshot();
        assert_eq!(snapshot.wal_commit_groups, 1);
        assert_eq!(snapshot.wal_commit_solo_groups, 1);
        assert_eq!(snapshot.wal_commit_buffers, 1);
        assert_eq!(snapshot.wal_commit_bytes, WAL_V7_FRAME_LEN as u64);
        assert_eq!(snapshot.wal_frontier_writes, 1);
        assert_eq!(snapshot.wal_frontier_bytes, WAL_V7_FRAME_LEN as u64);
        assert_eq!(snapshot.wal_sync_count, 1);
        assert_eq!(snapshot.wal_sync_groups_total, 1);
        let observations = profile.wal_sync_observations();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].captured_target, 1);
        assert_eq!(observations[0].written_frontier_start, 0);
        assert_eq!(observations[0].written_frontier_end, 1);
        assert_eq!(observations[0].groups_covered, 1);
        assert!(observations[0].succeeded);

        wal.close()?;
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

        let error = writer
            .append_batch(
                &batch(10, 100, 2 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY),
                LIVE_WAL_V5_LIMITS,
            )
            .expect_err("batch cannot fit a fresh segment");
        assert_eq!(
            error.downcast_ref::<WalV7LeaderCapacityError>(),
            Some(&WalV7LeaderCapacityError::BatchTooLarge)
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
    fn leader_reports_rotation_before_assigning_coordinates() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let mut writer = open_writer(&path, &installed, 4 * WAL_V7_FRAME_LEN as u64)?;
        writer.append_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS)?;
        let image_before_retry = fs::read(&path)?;
        let seed_before_retry = writer.runtime_seed()?;
        let resources_before_retry = writer.reservation_owner.snapshot();

        let error = writer
            .append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS)
            .expect_err("the next batch requires a fresh segment");
        assert_eq!(
            error.downcast_ref::<WalV7LeaderCapacityError>(),
            Some(&WalV7LeaderCapacityError::RotationRequired)
        );
        assert_eq!(fs::read(&path)?, image_before_retry);
        assert_eq!(
            writer.runtime_seed()?.next_ticket,
            seed_before_retry.next_ticket
        );
        assert_eq!(
            writer.runtime_seed()?.append_offset,
            seed_before_retry.append_offset
        );
        assert_eq!(writer.reservation_owner.snapshot(), resources_before_retry);

        Ok(())
    }

    #[test]
    fn leader_reports_rotation_when_only_the_frontier_does_not_fit() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let segment_limit = 5 * WAL_V7_FRAME_LEN as u64;
        let mut writer = open_writer(&path, &installed, segment_limit)?;
        writer.append_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS)?;

        let seed_before_retry = writer.runtime_seed()?;
        let data_end = seed_before_retry.append_offset + WAL_V7_FRAME_LEN as u64;
        let marker_end = data_end + WAL_V7_FRAME_LEN as u64;
        assert!(data_end <= segment_limit);
        assert!(marker_end > segment_limit);
        let image_before_retry = fs::read(&path)?;
        let resources_before_retry = writer.reservation_owner.snapshot();

        let error = writer
            .append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS)
            .expect_err("DATA fits but its required FRONTIER does not");
        assert_eq!(
            error.downcast_ref::<WalV7LeaderCapacityError>(),
            Some(&WalV7LeaderCapacityError::RotationRequired)
        );
        assert_eq!(fs::read(&path)?, image_before_retry);
        assert_eq!(
            writer.runtime_seed()?.next_ticket,
            seed_before_retry.next_ticket
        );
        assert_eq!(
            writer.runtime_seed()?.append_offset,
            seed_before_retry.append_offset
        );
        assert_eq!(writer.reservation_owner.snapshot(), resources_before_retry);

        Ok(())
    }

    #[test]
    fn leader_resource_rejection_preserves_image_and_admission_state() -> Result<()> {
        let request = batch(10, 100, 2 * WAL_V7_DATA_FRAGMENT_PAYLOAD_CAPACITY);
        let required = WalV7ResourceCharge::for_batch(u64::try_from(
            prepare_logical_batch(&request, LIVE_WAL_V5_LIMITS)?.encoded_len(),
        )?)?;
        for (limits, expected_error) in [
            (
                WalV7ResourceLimits {
                    max_logical_wal_bytes: required.logical_wal_bytes - 1,
                    ..resource_limits()
                },
                WalV7ReservationError::LogicalWalLimit,
            ),
            (
                WalV7ResourceLimits {
                    max_source_spool_bytes: required.source_spool_bytes - 1,
                    max_recovery_workspace_bytes: 0,
                    ..resource_limits()
                },
                WalV7ReservationError::SourceSpoolLimit { generation: 0 },
            ),
            (
                WalV7ResourceLimits {
                    max_buffer_memory_bytes: required.buffer_memory_bytes - 1,
                    ..resource_limits()
                },
                WalV7ReservationError::BufferMemoryLimit,
            ),
            (
                WalV7ResourceLimits {
                    max_seal_index_bytes: 0,
                    ..resource_limits()
                },
                WalV7ReservationError::SealIndexLimit,
            ),
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("active.wal");
            let installed = install_empty(&path)?;
            let original_image = fs::read(&path)?;
            let ledger = WalV7ResourceLedger::new(limits)?;
            let owner = Arc::new(WalV7ReservationOwner::new(ledger, &installed)?);
            let mut writer = WalV7LeaderWriter::new(
                write_handle(&path)?,
                File::open(&path)?,
                installed.runtime_seed()?,
                MAX_WAL_FILE_SIZE,
                Arc::clone(&owner),
            )?;

            let error = writer
                .append_batch(&request, LIVE_WAL_V5_LIMITS)
                .expect_err("the requested resource exceeds its cap");
            assert_eq!(
                error.downcast_ref::<WalV7ReservationError>(),
                Some(&expected_error)
            );
            assert_eq!(owner.snapshot(), WalV7ResourceCharge::default());
            assert_eq!(fs::read(&path)?, original_image);
            assert_eq!(writer.runtime_seed()?.next_ticket, 0);
            assert_eq!(
                writer.runtime_seed()?.append_offset,
                installed.append_offset
            );
            if expected_error != WalV7ReservationError::SealIndexLimit {
                assert_eq!(
                    writer
                        .append_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS)?
                        .ticket,
                    0
                );
                assert_eq!(owner.snapshot().buffer_memory_bytes, 0);
            }
        }
        Ok(())
    }

    #[test]
    fn leader_owner_survives_handle_replacement_and_recovery_rebinds_exact_charge() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let request = batch(10, 100, 4);
        let mut expected = WalV7ResourceCharge::for_batch(u64::try_from(
            prepare_logical_batch(&request, LIVE_WAL_V5_LIMITS)?.encoded_len(),
        )?)?;
        let ledger = WalV7ResourceLedger::new(WalV7ResourceLimits {
            max_logical_wal_bytes: expected.logical_wal_bytes,
            max_source_spool_bytes: expected.source_spool_bytes,
            max_recovery_workspace_bytes: 0,
            max_buffer_memory_bytes: expected.buffer_memory_bytes,
            max_seal_index_bytes: expected.seal_index_bytes,
        })?;
        let owner = Arc::new(WalV7ReservationOwner::new(ledger.clone(), &installed)?);
        let mut writer = WalV7LeaderWriter::new(
            write_handle(&path)?,
            File::open(&path)?,
            installed.runtime_seed()?,
            MAX_WAL_FILE_SIZE,
            Arc::clone(&owner),
        )?;
        writer.append_batch(&request, LIVE_WAL_V5_LIMITS)?;
        expected.buffer_memory_bytes = 0;
        let seed = writer.runtime_seed()?;
        drop(writer);
        assert_eq!(ledger.snapshot(), expected);
        let mut reopened = WalV7LeaderWriter::new(
            write_handle(&path)?,
            File::open(&path)?,
            seed,
            MAX_WAL_FILE_SIZE,
            Arc::clone(&owner),
        )?;
        let error = reopened
            .append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS)
            .expect_err("retained prefix fills the WAL cap");
        assert_eq!(
            error.downcast_ref::<WalV7ReservationError>(),
            Some(&WalV7ReservationError::LogicalWalLimit)
        );
        assert_eq!(ledger.snapshot(), expected);
        drop(reopened);

        let recovered = recover(&path, installed.active_boundary)?;
        assert!(
            WalV7LeaderWriter::new(
                write_handle(&path)?,
                File::open(&path)?,
                recovered.runtime_seed()?,
                MAX_WAL_FILE_SIZE,
                Arc::clone(&owner),
            )
            .is_err()
        );
        drop(owner);
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
        let replacement = Arc::new(WalV7ReservationOwner::new(ledger.clone(), &recovered)?);
        assert_eq!(ledger.snapshot(), expected);
        let writer = WalV7LeaderWriter::new(
            write_handle(&path)?,
            File::open(&path)?,
            recovered.runtime_seed()?,
            MAX_WAL_FILE_SIZE,
            replacement,
        )?;
        assert_eq!(writer.runtime_seed()?.next_ticket, 1);
        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_leader_unwind_retains_persistent_charge_after_data_write() -> Result<()> {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        use crate::chaos::failpoint::{self, FailScenario};

        let scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let ledger = WalV7ResourceLedger::new(resource_limits())?;
        let owner = Arc::new(WalV7ReservationOwner::new(ledger.clone(), &installed)?);
        let mut writer = WalV7LeaderWriter::new(
            write_handle(&path)?,
            File::open(&path)?,
            installed.runtime_seed()?,
            MAX_WAL_FILE_SIZE,
            Arc::clone(&owner),
        )?;
        let request = batch(10, 100, 4);
        let mut expected = WalV7ResourceCharge::for_batch(u64::try_from(
            prepare_logical_batch(&request, LIVE_WAL_V5_LIMITS)?.encoded_len(),
        )?)?;
        failpoint::cfg("wal.v7.leader.after_data_write", "panic")
            .expect("arm v7 Leader unwind failpoint");
        let result = catch_unwind(AssertUnwindSafe(|| {
            writer.append_batch(&request, LIVE_WAL_V5_LIMITS)
        }));
        scenario.teardown();
        assert!(result.is_err());
        expected.buffer_memory_bytes = 0;
        assert_eq!(ledger.snapshot(), expected);
        assert!(fs::metadata(&path)?.len() > installed.image_len);
        assert!(writer.runtime_seed().is_err());
        let uncertain_image = fs::read(&path)?;
        assert!(
            writer
                .append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS)
                .is_err()
        );
        assert_eq!(fs::read(&path)?, uncertain_image);
        drop(writer);
        assert_eq!(ledger.snapshot(), expected);
        drop(owner);
        assert_eq!(ledger.snapshot(), WalV7ResourceCharge::default());
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
            resource_owner(&installed)?,
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

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_leader_frontier_sync_failure_poison_prevents_further_writes() -> Result<()> {
        use crate::chaos::failpoint::{self, FailScenario};

        let scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let previous = writer.allocator.last_frontier();

        failpoint::cfg("wal.v7.leader.fdatasync", "return")
            .expect("arm v7 Leader FRONTIER sync failure");
        let error = writer
            .append_batch(&batch(10, 100, 4), LIVE_WAL_V5_LIMITS)
            .expect_err("injected fdatasync failure must reject the commit");
        failpoint::cfg("wal.v7.leader.fdatasync", "off")
            .expect("disable v7 Leader FRONTIER sync failure");
        scenario.teardown();

        assert!(format!("{error:#}").contains("injected v7 WAL sync failure"));
        assert_eq!(writer.allocator.last_frontier(), previous);
        assert!(
            writer.runtime_seed().is_err(),
            "uncertain writer is poisoned"
        );
        let uncertain_image = fs::read(&path)?;
        assert!(
            uncertain_image.len() > installed.image_len as usize,
            "DATA and FRONTIER bytes precede the injected sync error"
        );
        assert!(
            writer
                .append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS)
                .is_err()
        );
        assert_eq!(
            fs::read(&path)?,
            uncertain_image,
            "a poisoned writer must not append another FRONTIER"
        );

        Ok(())
    }

    #[cfg(all(feature = "chaos-testing", target_os = "linux"))]
    #[test]
    fn failpoint_leader_short_frontier_write_poisons_and_recovers_prior_anchor() -> Result<()> {
        use crate::chaos::failpoint::{self, FailScenario};

        let _scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("short-frontier-write.wal");
        let installed = install_empty(&path)?;
        let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let baseline = batch(10, 100, 4);
        writer.append_batch(&baseline, LIVE_WAL_V5_LIMITS)?;
        let previous = writer.allocator.last_frontier();
        let before_failure = fs::read(&path)?;
        let data_offset = before_failure.len();
        let frontier_offset = data_offset + WAL_V7_FRAME_LEN;

        failpoint::cfg("wal.v7.leader.frontier_short_write", "return")
            .expect("inject a positive short FRONTIER write");
        let result = writer.append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS);
        failpoint::cfg("wal.v7.leader.frontier_short_write", "off")
            .expect("disable short FRONTIER write");
        let error = result.expect_err("a short FRONTIER write must fail the durability cycle");

        assert!(
            format!("{error:#}").contains("short v7 positional write"),
            "unexpected failure: {error:#}"
        );
        assert_eq!(writer.allocator.last_frontier(), previous);
        assert!(
            writer.runtime_seed().is_err(),
            "uncertain writer is poisoned"
        );
        let uncertain_image = fs::read(&path)?;
        assert_eq!(
            uncertain_image.len(),
            frontier_offset + 1,
            "one complete DATA frame and exactly one FRONTIER byte were appended"
        );
        assert_eq!(&uncertain_image[..data_offset], before_failure.as_slice());
        assert_eq!(
            decode_frame_structural(
                &uncertain_image[data_offset..frontier_offset],
                u64::try_from(data_offset)?,
                &writer.header_digest,
            )?
            .kind,
            WalV7FrameKind::Data
        );
        assert_eq!(
            &uncertain_image[frontier_offset..],
            b"T",
            "the partial FRONTIER contains the first byte of its frame magic"
        );
        assert!(
            writer
                .append_batch(&batch(30, 102, 4), LIVE_WAL_V5_LIMITS)
                .is_err(),
            "a poisoned writer must reject later appends"
        );
        assert_eq!(fs::read(&path)?, uncertain_image);
        drop(writer);

        let recovered = recover(&path, installed.active_boundary)?;
        assert_eq!(recovered.next_ticket, previous.frontier.ticket_end);
        assert_eq!(
            recovered.active_boundary.ticket_end,
            previous.frontier.ticket_end
        );
        assert_eq!(
            recovered.active_boundary.durable_end,
            previous.frontier.durable_end
        );
        assert_eq!(
            recovered.active_boundary.last_commit_ts,
            previous.frontier.last_commit_ts
        );
        assert_eq!(
            recovered.active_boundary.prefix_digest,
            previous.frontier.prefix_digest
        );
        assert_eq!(recovered.batches, [baseline]);
        Ok(())
    }

    #[cfg(all(feature = "chaos-testing", target_os = "linux"))]
    #[test]
    fn failpoint_leader_short_data_write_poisons_and_recovers_prior_anchor() -> Result<()> {
        use crate::chaos::failpoint::{self, FailScenario};

        let _scenario = FailScenario::setup();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("short-data-write.wal");
        let installed = install_empty(&path)?;
        let mut writer = open_writer(&path, &installed, MAX_WAL_FILE_SIZE)?;
        let baseline = batch(10, 100, 4);
        writer.append_batch(&baseline, LIVE_WAL_V5_LIMITS)?;
        let previous = writer.allocator.last_frontier();
        let before_failure = fs::read(&path)?;
        let data_offset = before_failure.len();

        failpoint::cfg("wal.v7.leader.data_short_write", "return")
            .expect("inject a positive short DATA write");
        let result = writer.append_batch(&batch(20, 101, 4), LIVE_WAL_V5_LIMITS);
        failpoint::cfg("wal.v7.leader.data_short_write", "off").expect("disable short DATA write");
        let error = result.expect_err("a short DATA write must fail the durability cycle");

        assert!(
            format!("{error:#}").contains("short v7 positional write"),
            "unexpected failure: {error:#}"
        );
        assert_eq!(writer.allocator.last_frontier(), previous);
        assert!(
            writer.runtime_seed().is_err(),
            "uncertain writer is poisoned"
        );
        let uncertain_image = fs::read(&path)?;
        assert_eq!(
            uncertain_image.len(),
            data_offset + 1,
            "exactly one DATA byte was appended without another FRONTIER"
        );
        assert_eq!(&uncertain_image[..data_offset], before_failure.as_slice());
        assert_eq!(
            &uncertain_image[data_offset..],
            b"T",
            "the partial DATA frame contains the first byte of its frame magic"
        );
        assert!(
            writer
                .append_batch(&batch(30, 102, 4), LIVE_WAL_V5_LIMITS)
                .is_err(),
            "a poisoned writer must reject later appends"
        );
        assert_eq!(fs::read(&path)?, uncertain_image);
        drop(writer);

        let recovered = recover(&path, installed.active_boundary)?;
        assert_eq!(recovered.next_ticket, previous.frontier.ticket_end);
        assert_eq!(
            recovered.active_boundary.ticket_end,
            previous.frontier.ticket_end
        );
        assert_eq!(
            recovered.active_boundary.durable_end,
            previous.frontier.durable_end
        );
        assert_eq!(
            recovered.active_boundary.last_commit_ts,
            previous.frontier.last_commit_ts
        );
        assert_eq!(
            recovered.active_boundary.prefix_digest,
            previous.frontier.prefix_digest
        );
        assert_eq!(recovered.batches, [baseline]);
        Ok(())
    }

    #[test]
    fn leader_rejects_seed_header_digest_mismatch_without_mutating_image() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.wal");
        let installed = install_empty(&path)?;
        let original_image = fs::read(&path)?;
        let owner = resource_owner(&installed)?;
        let original_charge = owner.snapshot();
        let mut seed = installed.runtime_seed()?;
        seed.header_digest[0] ^= 1;

        let result = WalV7LeaderWriter::new(
            write_handle(&path)?,
            File::open(&path)?,
            seed,
            MAX_WAL_FILE_SIZE,
            Arc::clone(&owner),
        );
        let error = result
            .err()
            .expect("a mismatched seed header digest must fail");
        assert!(
            error
                .to_string()
                .contains("immutable header digest does not match the recovered runtime seed")
        );
        assert_eq!(fs::read(&path)?, original_image);
        assert_eq!(owner.snapshot(), original_charge);

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
            resource_owner(&installed)?,
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
            resource_owner(&installed)?,
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
