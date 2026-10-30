//! Single-owner v8 commits for exercising the RFC 026 durability protocol.
//!
//! This path is deliberately not selected by the WAL registry. V8 production
//! selection stays disabled until persistence, compatibility and qualification
//! slices are complete. The writer nevertheless follows the same admission,
//! DATA/FRONTIER and single-sync ordering required by the eventual Parallel
//! runtime.

use std::{
    error::Error,
    fmt,
    fs::File,
    io,
    os::unix::{
        fs::{FileExt, MetadataExt},
        io::AsRawFd,
    },
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{LIVE_WAL_V5_LIMITS, RecordedAt, WalBatch, WalV5Limits},
    wal::{DirectBuf, MAX_WAL_FILE_SIZE},
};

use super::{
    WAL_V8_HEADER_LEN,
    codec::{
        WAL_V8_FRAME_LEN, WalV8Frontier, WalV8Header, decode_frame_structural,
        encode_frontier_successor, frame_record_digest,
    },
    digest,
    install::WalV8InstalledRecovery,
    prepare::{
        EncodedWalV8Batch, PreparedWalV8Batch, WalV8AdmissionGuard, WalV8AdmissionPlan,
        WalV8AdmissionPublisher, WalV8BatchAssignment, WalV8DigestReady,
    },
};

const FRAME_LEN_U64: u64 = WAL_V8_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = WAL_V8_HEADER_LEN as u64;
const MAX_GROUP_TICKETS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV8LeaderCapacityError {
    RotationRequired,
    BatchTooLarge,
}

impl fmt::Display for WalV8LeaderCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RotationRequired => "v8 WAL segment rotation required before retrying batch",
            Self::BatchTooLarge => "v8 WAL batch cannot fit a fresh segment",
        })
    }
}

impl Error for WalV8LeaderCapacityError {}

/// One ticket whose embedded FRONTIER was synced before this result returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8LeaderCommit {
    pub(crate) ticket: u64,
    pub(crate) first_data_offset: u64,
    pub(crate) frontier: WalV8Frontier,
    pub(crate) frontier_offset: u64,
    pub(crate) frontier_digest: [u8; 32],
}

/// Tickets that became durable under one embedded FRONTIER and one WAL sync.
pub(crate) struct WalV8LeaderCommitGroup {
    pub(crate) commits: Vec<WalV8LeaderCommit>,
    pub(crate) frontier: WalV8Frontier,
}

/// Serial protocol exerciser resumed only from a normalized Active image.
///
/// A caller must exclusively own the WAL and serialize access. A failure after
/// admission poisons the writer; recovery must install a new inode before the
/// writer can be used again. DATA and FRONTIER are written to the same inode
/// and covered by exactly one `fdatasync` before the commit is returned.
pub(crate) struct WalV8LeaderWriter {
    write_file: File,
    sync_file: File,
    header: WalV8Header,
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    prefix_digest: digest::WalV8Digest,
    frontier: WalV8Frontier,
    frontier_offset: u64,
    frontier_digest: [u8; 32],
    append_offset: u64,
    next_ticket: u64,
    last_recorded_at: Option<RecordedAt>,
    max_file_size: u64,
    installed_dev: u64,
    installed_ino: u64,
    poisoned: Arc<AtomicBool>,
}

struct LeaderAdmissionGuard {
    ticket: u64,
    poisoned: Arc<AtomicBool>,
    armed: bool,
}

impl WalV8AdmissionGuard for LeaderAdmissionGuard {
    fn segment_ticket(&self) -> u64 {
        self.ticket
    }

    fn poison(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
            self.armed = false;
        }
    }

    fn disarm_after_durable(&mut self) {
        self.armed = false;
    }
}

impl Drop for LeaderAdmissionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

struct FixedAdmissionPublisher {
    assignment: WalV8BatchAssignment,
    expected_plan: WalV8AdmissionPlan,
    poisoned: Arc<AtomicBool>,
}

struct ProducerCompletion {
    assignment: WalV8BatchAssignment,
    digest_ready: WalV8DigestReady,
    encoded: EncodedWalV8Batch<LeaderAdmissionGuard>,
}

impl WalV8AdmissionPublisher for FixedAdmissionPublisher {
    type Guard = LeaderAdmissionGuard;

    fn admit_and_publish(
        &self,
        plan: WalV8AdmissionPlan,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)> {
        ensure!(
            plan == self.expected_plan,
            "v8 admission plan differs from its preflighted reservation"
        );
        let guard = LeaderAdmissionGuard {
            ticket: self.assignment.segment_ticket,
            poisoned: Arc::clone(&self.poisoned),
            armed: true,
        };
        Ok((self.assignment, guard))
    }
}

impl WalV8LeaderWriter {
    /// Resume a v8 Leader writer from a durably installed Active recovery.
    pub(crate) fn new(
        write_file: File,
        sync_file: File,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
    ) -> Result<Self> {
        ensure!(
            max_file_size >= installed.image_len && max_file_size <= MAX_WAL_FILE_SIZE,
            "invalid v8 Leader segment size"
        );
        validate_positional_write_handle(&write_file)
            .context("validate v8 WAL positional write handle")?;
        let write_metadata = write_file
            .metadata()
            .context("inspect v8 WAL write handle")?;
        let sync_metadata = sync_file.metadata().context("inspect v8 WAL sync handle")?;
        ensure!(
            write_metadata.is_file()
                && sync_metadata.is_file()
                && write_metadata.dev() == sync_metadata.dev()
                && write_metadata.ino() == sync_metadata.ino(),
            "v8 WAL write and sync handles refer to different files"
        );
        ensure!(
            write_metadata.dev() == installed.installed_dev
                && write_metadata.ino() == installed.installed_ino,
            "v8 WAL handles do not refer to the installed recovery inode"
        );
        ensure!(
            write_metadata.len() == installed.image_len
                && installed.image_len.is_multiple_of(FRAME_LEN_U64),
            "v8 WAL length differs from the installed recovery image"
        );

        let header_bytes = read_exact_at::<WAL_V8_HEADER_LEN>(&write_file, 0)
            .context("read v8 WAL header before writer startup")?;
        let header = WalV8Header::decode(&header_bytes)?;
        ensure!(
            header.encode()? == header_bytes && header == installed.selection.header,
            "v8 WAL header differs from the installed recovery identity"
        );
        let physical_header_digest: [u8; 32] = Sha256::digest(header_bytes).into();
        ensure!(
            physical_header_digest == installed.header_digest,
            "v8 WAL physical header digest differs from recovery"
        );
        let initial_prefix_digest = header.initial_prefix_digest()?;
        let frontier_bytes =
            read_exact_at::<WAL_V8_FRAME_LEN>(&write_file, installed.frontier_offset)
                .context("read installed v8 FRONTIER before writer startup")?;
        let frontier_frame = decode_frame_structural(
            &frontier_bytes,
            installed.frontier_offset,
            &physical_header_digest,
            &initial_prefix_digest,
        )?;
        ensure!(
            frontier_frame.kind == super::codec::WalV8FrameKind::Frontier
                && WalV8Frontier::decode_body(&frontier_frame.body)? == installed.frontier
                && frame_record_digest(&frontier_bytes)? == installed.frontier_digest,
            "installed v8 FRONTIER differs from its recovery metadata"
        );
        ensure!(
            installed.frontier.ticket_end == installed.active_boundary.ticket_end
                && installed.frontier.durable_end == installed.active_boundary.durable_end
                && installed.frontier.last_commit_ts == installed.active_boundary.last_commit_ts
                && installed.frontier.prefix_digest == installed.active_boundary.prefix_digest
                && installed.frontier_offset + FRAME_LEN_U64 == installed.image_len
                && installed.next_ticket == installed.active_boundary.ticket_end,
            "installed v8 FRONTIER does not match its logical recovery boundary"
        );

        Ok(Self {
            write_file,
            sync_file,
            header,
            physical_header_digest,
            initial_prefix_digest,
            prefix_digest: installed.active_boundary.prefix_digest,
            frontier: installed.frontier,
            frontier_offset: installed.frontier_offset,
            frontier_digest: installed.frontier_digest,
            append_offset: installed.image_len,
            next_ticket: installed.next_ticket,
            last_recorded_at: installed.last_recorded_at,
            max_file_size,
            installed_dev: installed.installed_dev,
            installed_ino: installed.installed_ino,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Encode, write, and durably publish one ticket using a single WAL sync.
    pub(crate) fn append_batch(
        &mut self,
        batch: &WalBatch,
        limits: WalV5Limits,
    ) -> Result<WalV8LeaderCommit> {
        let mut group = self.append_group(std::slice::from_ref(batch), limits)?;
        group
            .commits
            .pop()
            .context("v8 one-ticket group returned no commit")
    }

    /// Admit and prepare a bounded ticket group in parallel, then publish one
    /// embedded FRONTIER with one WAL sync for the whole group.
    pub(crate) fn append_group(
        &mut self,
        batches: &[WalBatch],
        limits: WalV5Limits,
    ) -> Result<WalV8LeaderCommitGroup> {
        ensure!(!self.is_poisoned(), "v8 Leader WAL is poisoned");
        ensure!(
            !batches.is_empty() && batches.len() <= MAX_GROUP_TICKETS,
            "v8 Leader group size is outside its bounded ticket window"
        );
        let limits = bounded_limits(limits);
        let mut prepared_batches = Vec::new();
        prepared_batches
            .try_reserve_exact(batches.len())
            .context("reserve v8 preflighted group")?;
        let mut data_buffers = Vec::new();
        data_buffers
            .try_reserve_exact(batches.len())
            .context("reserve v8 producer frame buffers")?;
        let mut write_handles = Vec::new();
        write_handles
            .try_reserve_exact(batches.len())
            .context("reserve v8 producer write handles")?;
        let mut assignments = Vec::new();
        assignments
            .try_reserve_exact(batches.len())
            .context("reserve v8 group assignments")?;
        let mut admitted_batches = Vec::new();
        admitted_batches
            .try_reserve_exact(batches.len())
            .context("reserve v8 admitted group")?;
        let mut producer_completions = Vec::new();
        producer_completions
            .try_reserve_exact(batches.len())
            .context("reserve v8 producer completions")?;
        let mut commits = Vec::new();
        commits
            .try_reserve_exact(batches.len())
            .context("reserve v8 durable commit results")?;

        for batch in batches {
            let prepared = PreparedWalV8Batch::<()>::preflight(batch, limits, |_| Ok(()))?;
            data_buffers.push(DirectBuf::new(prepared.frame_buffer_bytes()));
            write_handles.push(
                self.write_file
                    .try_clone()
                    .context("clone v8 WAL data-write handle before admission")?,
            );
            prepared_batches.push(prepared);
        }
        let mut frontier_buffer = DirectBuf::new(WAL_V8_FRAME_LEN);
        let ticket_start = self.next_ticket;
        let mut ticket = ticket_start;
        let mut data_offset = self.append_offset;
        let mut previous_commit_ts = self.frontier.last_commit_ts;
        let mut previous_recorded_at = self.last_recorded_at;
        for (batch, prepared) in batches.iter().zip(&prepared_batches) {
            ensure!(
                batch.commit_ts > previous_commit_ts,
                "v8 Leader group commit timestamps do not advance"
            );
            let recorded_at = previous_recorded_at.map_or(batch.recorded_at, |previous| {
                previous.max(batch.recorded_at)
            });
            let assignment = WalV8BatchAssignment {
                segment_ticket: ticket,
                commit_ts: batch.commit_ts,
                recorded_at,
                first_data_offset: data_offset,
                segment_incarnation: self.header.incarnation,
                physical_header_digest: self.physical_header_digest,
                initial_prefix_digest: self.initial_prefix_digest,
            };
            let plan = WalV8AdmissionPlan {
                logical_batch_bytes: prepared.logical_batch_bytes(),
                fragment_count: prepared.fragment_count(),
                frame_buffer_bytes: prepared.frame_buffer_bytes(),
            };
            assignments.push((assignment, plan));
            ticket = ticket
                .checked_add(1)
                .context("v8 ticket cursor overflows")?;
            data_offset = data_offset
                .checked_add(u64::try_from(prepared.frame_buffer_bytes())?)
                .context("v8 group DATA allocation end overflows")?;
            previous_commit_ts = batch.commit_ts;
            previous_recorded_at = Some(recorded_at);
        }
        let ticket_end = ticket;
        let frontier_offset = data_offset;
        let frontier_end = frontier_offset
            .checked_add(FRAME_LEN_U64)
            .context("v8 group FRONTIER allocation end overflows")?;
        if frontier_end > self.max_file_size {
            let fresh_end = HEADER_LEN_U64
                .checked_add(FRAME_LEN_U64)
                .and_then(|start| {
                    assignments.iter().try_fold(start, |offset, (_, plan)| {
                        offset.checked_add(u64::try_from(plan.frame_buffer_bytes).ok()?)
                    })
                })
                .and_then(|end| end.checked_add(FRAME_LEN_U64))
                .context("v8 fresh-segment capacity calculation overflows")?;
            let error = if fresh_end <= self.max_file_size {
                WalV8LeaderCapacityError::RotationRequired
            } else {
                WalV8LeaderCapacityError::BatchTooLarge
            };
            return Err(anyhow::Error::new(error));
        }
        let next_generation = self
            .frontier
            .generation
            .checked_add(1)
            .context("v8 FRONTIER generation overflows")?;
        let previous_frontier_end = self
            .frontier_offset
            .checked_add(FRAME_LEN_U64)
            .context("v8 previous FRONTIER end overflows")?;
        ensure!(
            frontier_offset >= previous_frontier_end,
            "v8 DATA group overlaps its previous FRONTIER"
        );

        // All reservations and output buffers exist before coordinates are
        // consumed. Publishing the full group here lets every admitted ticket
        // become visible before any producer encodes payload bytes.
        self.next_ticket = ticket_end;
        self.append_offset = frontier_end;
        self.poisoned.store(true, Ordering::Release);
        for (prepared, (assignment, plan)) in prepared_batches
            .into_iter()
            .zip(assignments.iter().copied())
        {
            let publisher = FixedAdmissionPublisher {
                assignment,
                expected_plan: plan,
                poisoned: Arc::clone(&self.poisoned),
            };
            admitted_batches.push(prepared.admit(&publisher)?);
        }

        let producer_result = thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            handles
                .try_reserve_exact(admitted_batches.len())
                .context("reserve scoped v8 producer handles")?;
            for ((admitted, (assignment, _)), (mut data_buffer, write_file)) in admitted_batches
                .into_iter()
                .zip(assignments.iter().copied())
                .zip(data_buffers.into_iter().zip(write_handles))
            {
                handles.push(scope.spawn(move || -> Result<ProducerCompletion> {
                    let mut encoded = admitted.encode()?;
                    let digest_ready = encoded.digest_ready()?;
                    let data_len = encoded.encode_data_frames_into(data_buffer.as_mut_slice())?;
                    ensure!(
                        data_len == data_buffer.cap(),
                        "v8 DATA materializer returned an unexpected frame length"
                    );
                    data_buffer.set_len(data_len);
                    write_aligned_once(
                        &write_file,
                        data_buffer.initialized_slice(0, data_len),
                        assignment.first_data_offset,
                    )
                    .context("write v8 Parallel DATA frames")?;
                    Ok(ProducerCompletion {
                        assignment,
                        digest_ready,
                        encoded,
                    })
                }));
            }

            let mut first_error = None;
            for handle in handles {
                match handle.join() {
                    Ok(Ok(completion)) => producer_completions.push(completion),
                    Ok(Err(error)) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(anyhow::anyhow!("v8 producer thread panicked"));
                        }
                    }
                }
            }
            if let Some(error) = first_error {
                Err(error)
            } else {
                Ok(())
            }
        });
        producer_result?;

        let mut next_prefix = self.prefix_digest;
        for ((completion, (expected_assignment, _)), expected_ticket) in producer_completions
            .iter()
            .zip(assignments.iter())
            .zip(ticket_start..ticket_end)
        {
            ensure!(
                completion.assignment == *expected_assignment
                    && completion.digest_ready.segment_ticket == expected_ticket
                    && completion.digest_ready.segment_incarnation == self.header.incarnation,
                "v8 producer completion does not match its admitted ticket"
            );
            next_prefix = digest::advance_prefix(
                &next_prefix,
                expected_ticket,
                completion.digest_ready.logical_batch_bytes,
                &completion.digest_ready.batch_digest,
            );
        }

        let frontier = WalV8Frontier {
            generation: next_generation,
            ticket_end,
            durable_end: frontier_offset.max(previous_frontier_end),
            last_commit_ts: previous_commit_ts,
            prefix_digest: next_prefix,
            previous_frontier_offset: self.frontier_offset,
            previous_frontier_digest: self.frontier_digest,
        };
        let frontier_frame = encode_frontier_successor(
            frontier,
            self.frontier,
            self.frontier_offset,
            &self.frontier_digest,
            frontier_offset,
            self.physical_header_digest,
            self.initial_prefix_digest,
        )?;
        frontier_buffer.as_mut_slice()[..WAL_V8_FRAME_LEN].copy_from_slice(&frontier_frame);
        frontier_buffer.set_len(WAL_V8_FRAME_LEN);
        let frontier_digest = frame_record_digest(&frontier_frame)?;

        write_aligned_once(
            &self.write_file,
            frontier_buffer.initialized_slice(0, WAL_V8_FRAME_LEN),
            frontier_offset,
        )
        .context("write v8 Leader FRONTIER")?;
        fdatasync_once(&self.sync_file).context("fdatasync v8 Leader WAL")?;

        // Everything below is infallible publication after the successful
        // sync. Every ticket guard remains armed until this boundary.
        self.frontier = frontier;
        self.frontier_offset = frontier_offset;
        self.frontier_digest = frontier_digest;
        self.prefix_digest = next_prefix;
        self.last_recorded_at = previous_recorded_at;
        for completion in &mut producer_completions {
            completion.encoded.disarm_after_durable();
        }
        self.poisoned.store(false, Ordering::Release);

        for (assignment, _) in assignments {
            commits.push(WalV8LeaderCommit {
                ticket: assignment.segment_ticket,
                first_data_offset: assignment.first_data_offset,
                frontier,
                frontier_offset,
                frontier_digest,
            });
        }
        Ok(WalV8LeaderCommitGroup { commits, frontier })
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }
}

fn bounded_limits(limits: WalV5Limits) -> WalV5Limits {
    WalV5Limits {
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
    }
}

fn validate_positional_write_handle(file: &File) -> io::Result<()> {
    let flags = loop {
        // SAFETY: the descriptor is live and F_GETFL does not require a third argument.
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
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "v8 WAL positional write handle uses O_APPEND",
        ));
    }
    Ok(())
}

fn write_aligned_once(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    if bytes.is_empty()
        || !bytes.len().is_multiple_of(WAL_V8_FRAME_LEN)
        || !offset.is_multiple_of(FRAME_LEN_U64)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "v8 positional write is not frame aligned",
        ));
    }
    loop {
        match file.write_at(bytes, offset) {
            Ok(written) if written == bytes.len() => return Ok(()),
            Ok(written) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "short v8 positional write: {written} of {} bytes",
                        bytes.len()
                    ),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn fdatasync_once(file: &File) -> io::Result<()> {
    loop {
        // SAFETY: fdatasync only borrows the live descriptor for the call.
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

fn read_exact_at<const N: usize>(file: &File, offset: u64) -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    let mut read = 0;
    while read < N {
        let read_offset = offset
            .checked_add(u64::try_from(read).map_err(io::Error::other)?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "v8 read offset overflows")
            })?;
        match file.read_at(&mut bytes[read..], read_offset) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::{ffi::CString, fs::OpenOptions, os::fd::FromRawFd, path::Path};

    use anyhow::Result;

    use super::*;
    use crate::pitr::{
        ArchiveEpochId, ChainAnchor, SegmentId, TimelineId, WalEntry, manifest::ActiveBoundary,
    };

    fn header() -> WalV8Header {
        let archive_epoch_id = ArchiveEpochId([0x22; 16]);
        WalV8Header {
            timeline_id: TimelineId([0x11; 16]),
            archive_epoch_id,
            segment_id: SegmentId(7),
            predecessor: ChainAnchor::Genesis { archive_epoch_id },
            incarnation: [0x44; 16],
        }
    }

    fn create_empty_active(path: &Path, header: WalV8Header) -> Result<ActiveBoundary> {
        super::super::install::create_empty_active_image(path, header)
    }

    fn install_empty(path: &Path, header: WalV8Header) -> Result<WalV8InstalledRecovery> {
        let boundary = create_empty_active(path, header)?;
        super::super::install::recover_and_install_active(
            path,
            &[boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )
    }

    fn batch(commit_ts: u64) -> WalBatch {
        WalBatch {
            commit_ts,
            recorded_at: RecordedAt {
                secs: i64::try_from(commit_ts).unwrap_or(i64::MAX),
                nanos: 17,
            },
            entries: vec![WalEntry::Put {
                key: b"k".to_vec(),
                value: b"value".to_vec(),
            }],
        }
    }

    fn open_writer(path: &Path, installed: &WalV8InstalledRecovery) -> Result<WalV8LeaderWriter> {
        WalV8LeaderWriter::new(
            OpenOptions::new().read(true).write(true).open(path)?,
            OpenOptions::new().read(true).write(true).open(path)?,
            installed,
            MAX_WAL_FILE_SIZE,
        )
    }

    #[test]
    fn empty_active_image_creation_is_canonical_and_idempotent() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();

        let boundary = super::super::install::create_empty_active_image(&path, header)?;
        assert_eq!(
            boundary.wal_format,
            crate::pitr::manifest::PitrWalFormat::V8
        );
        assert_eq!(boundary.ticket_end, 0);
        assert_eq!(boundary.durable_end, HEADER_LEN_U64);
        assert_eq!(boundary.prefix_digest, header.initial_prefix_digest()?);

        let image = std::fs::read(&path)?;
        assert_eq!(image.len(), WAL_V8_HEADER_LEN + WAL_V8_FRAME_LEN);
        let decoded_header = WalV8Header::decode(&image[..WAL_V8_HEADER_LEN])?;
        assert_eq!(decoded_header, header);
        let physical_header_digest = header.physical_digest()?;
        let initial_prefix_digest = header.initial_prefix_digest()?;
        let frame = &image[WAL_V8_HEADER_LEN..];
        let decoded = decode_frame_structural(
            frame,
            HEADER_LEN_U64,
            &physical_header_digest,
            &initial_prefix_digest,
        )?;
        assert_eq!(decoded.kind, super::super::codec::WalV8FrameKind::Frontier);
        WalV8Frontier::decode_body(&decoded.body)?
            .validate_generation_zero(&initial_prefix_digest, HEADER_LEN_U64)?;

        assert_eq!(
            super::super::install::create_empty_active_image(&path, header)?,
            boundary
        );
        assert_eq!(std::fs::read(&path)?, image);
        Ok(())
    }

    fn boundary_for(header: WalV8Header, frontier: WalV8Frontier) -> ActiveBoundary {
        ActiveBoundary {
            timeline_id: header.timeline_id.0,
            archive_epoch_id: header.archive_epoch_id.0,
            segment_id: header.segment_id.0,
            incarnation: header.incarnation,
            wal_format: crate::pitr::manifest::PitrWalFormat::V8,
            ticket_end: frontier.ticket_end,
            durable_end: frontier.durable_end,
            last_commit_ts: frontier.last_commit_ts,
            prefix_digest: frontier.prefix_digest,
        }
    }

    #[test]
    fn leader_syncs_embedded_frontier_before_ack_and_reopens() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let mut writer = open_writer(&path, &installed)?;

        let commit = writer.append_batch(&batch(10), LIVE_WAL_V5_LIMITS)?;
        assert_eq!(commit.ticket, 0);
        assert_eq!(commit.frontier.ticket_end, 1);
        assert!(!writer.is_poisoned());
        assert_eq!(
            std::fs::metadata(&path)?.len(),
            commit.frontier_offset + FRAME_LEN_U64
        );

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary_for(header, commit.frontier)],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 1);
        assert_eq!(recovered.selection.batches.len(), 1);
        assert_eq!(recovered.selection.batches[0], batch(10));
        Ok(())
    }

    #[test]
    fn parallel_group_admits_before_preparing_and_uses_one_frontier() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let mut writer = open_writer(&path, &installed)?;
        let batches = vec![batch(10), batch(20), batch(30)];

        let group = writer.append_group(&batches, LIVE_WAL_V5_LIMITS)?;
        assert_eq!(group.commits.len(), 3);
        assert_eq!(group.commits[0].ticket, 0);
        assert_eq!(group.commits[1].ticket, 1);
        assert_eq!(group.commits[2].ticket, 2);
        assert_eq!(group.frontier.ticket_end, 3);
        assert!(
            group
                .commits
                .iter()
                .all(|commit| commit.frontier == group.frontier)
        );
        assert_eq!(group.frontier.generation, 1);
        assert!(!writer.is_poisoned());

        let next_batch = batch(40);
        let next_group =
            writer.append_group(std::slice::from_ref(&next_batch), LIVE_WAL_V5_LIMITS)?;
        assert_eq!(next_group.frontier.ticket_end, 4);
        assert_eq!(next_group.frontier.generation, 2);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[
                boundary_for(header, group.frontier),
                boundary_for(header, next_group.frontier),
            ],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(
            recovered.selection.batches,
            batches.into_iter().chain([next_batch]).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn pre_admission_capacity_failure_does_not_consume_a_ticket() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let mut writer = WalV8LeaderWriter::new(
            OpenOptions::new().read(true).write(true).open(&path)?,
            OpenOptions::new().read(true).write(true).open(&path)?,
            &installed,
            installed.image_len,
        )?;

        let error = writer
            .append_batch(&batch(10), LIVE_WAL_V5_LIMITS)
            .expect_err("fresh segment cannot fit DATA and its FRONTIER");
        assert!(error.downcast_ref::<WalV8LeaderCapacityError>().is_some());
        assert!(!writer.is_poisoned());
        assert_eq!(std::fs::metadata(&path)?.len(), installed.image_len);
        Ok(())
    }

    #[test]
    fn sync_failure_poisons_admitted_ticket_and_writer() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let write_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let path_c = CString::new(path.as_os_str().as_encoded_bytes())?;
        // SAFETY: path_c is NUL-terminated; O_PATH returns a descriptor owned below.
        let sync_fd = unsafe { libc::open(path_c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        ensure!(
            sync_fd >= 0,
            "open O_PATH sync handle: {}",
            io::Error::last_os_error()
        );
        // SAFETY: sync_fd was returned by open and ownership transfers to File.
        let sync_file = unsafe { File::from_raw_fd(sync_fd) };
        let mut writer =
            WalV8LeaderWriter::new(write_file, sync_file, &installed, MAX_WAL_FILE_SIZE)?;

        let error = writer
            .append_batch(&batch(10), LIVE_WAL_V5_LIMITS)
            .expect_err("fdatasync failure must reject the commit");
        assert!(format!("{error:#}").contains("fdatasync"));
        assert!(writer.is_poisoned());
        assert!(writer.append_batch(&batch(20), LIVE_WAL_V5_LIMITS).is_err());
        Ok(())
    }
}
