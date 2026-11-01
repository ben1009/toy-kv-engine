//! Single-owner v8 commits for exercising the RFC 026 durability protocol.
//!
//! This path is deliberately not selected by the WAL registry. V8 production
//! selection stays disabled until persistence, compatibility and qualification
//! slices are complete. The writer nevertheless follows the same admission,
//! DATA/FRONTIER and single-sync ordering required by the eventual Parallel
//! runtime.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    error::Error,
    fmt,
    fs::File,
    io,
    os::unix::{
        fs::{FileExt, MetadataExt},
        io::AsRawFd,
    },
    sync::Arc,
    thread,
    time::Duration,
};

#[cfg(feature = "bench")]
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::{
    pitr::{LIVE_WAL_V5_LIMITS, WalBatch, WalV5Limits},
    wal::{
        DirectBuf, MAX_WAL_FILE_SIZE,
        v7::admission::{WalV7OwnedBatchReservation, WalV7OwnedPreparationReservation},
    },
};

use super::{
    WAL_V8_HEADER_LEN,
    codec::{
        WAL_V8_FRAME_LEN, WalV8Frontier, WalV8Header, decode_frame_structural,
        encode_frontier_successor, frame_record_digest,
    },
    digest,
    install::WalV8InstalledRecovery,
    parallel::{
        WalV8ParallelAdmissionGuard, WalV8ParallelConfig, WalV8ParallelCoordinator,
        WalV8PreparationReservation,
    },
    prepare::{
        AdmittedWalV8Batch, EncodedWalV8Batch, PreparedWalV8Batch, WalV8AdmissionPlan,
        WalV8AdmissionPublisher, WalV8BatchAssignment, WalV8DigestReady,
    },
    resource::WalV8ReservationOwner,
};

const FRAME_LEN_U64: u64 = WAL_V8_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = WAL_V8_HEADER_LEN as u64;
const MAX_GROUP_TICKETS: usize = 8;
// Keep enough admitted producers in flight to form larger shared FRONTIERs
// under contention; explicit Leader groups retain their separate smaller cap.
const MAX_PARALLEL_INFLIGHT_TICKETS: usize = 32;

pub(crate) type WalV8PreparedBatch<'a> = PreparedWalV8Batch<'a, WalV8PreflightReservation>;

pub(crate) struct WalV8PreflightReservation {
    _coordinator: WalV8PreparationReservation,
    _source: Option<WalV7OwnedPreparationReservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV8LeaderCapacityError {
    RotationRequired,
    BatchTooLarge,
    GroupExceedsAdmissionWindow,
}

impl fmt::Display for WalV8LeaderCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RotationRequired => "v8 WAL segment rotation required before retrying batch",
            Self::BatchTooLarge => "v8 WAL batch or group cannot fit a fresh segment",
            Self::GroupExceedsAdmissionWindow => {
                "v8 WAL group exceeds the bounded admission memory window"
            }
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
    #[cfg(feature = "bench")]
    pub(crate) digest_metrics: digest::WalV8DigestMetrics,
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
    coordinator: WalV8ParallelCoordinator,
    reservation_owner: Option<Arc<WalV8ReservationOwner>>,
    frontier: WalV8Frontier,
    frontier_offset: u64,
    frontier_digest: [u8; 32],
    max_file_size: u64,
    installed_dev: u64,
    installed_ino: u64,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
}

/// Shared v8 writer path for concurrent producers. Admission and DATA writes
/// can overlap across callers; this mutex protects only the one-at-a-time
/// FRONTIER write and sync cycle.
pub(crate) struct WalV8ParallelWriter {
    write_file: File,
    sync_file: File,
    header: WalV8Header,
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    coordinator: WalV8ParallelCoordinator,
    reservation_owner: Option<Arc<WalV8ReservationOwner>>,
    frontier: Mutex<WalV8ParallelFrontier>,
    frontier_resources: Mutex<BTreeMap<u64, WalV7OwnedBatchReservation>>,
    producer_guards: Arc<Mutex<BTreeMap<u64, EncodedWalV8Batch<WalV8ParallelAdmissionGuard>>>>,
    max_file_size: u64,
    installed_dev: u64,
    installed_ino: u64,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
}

struct WalV8ParallelFrontier {
    frontier: WalV8Frontier,
    offset: u64,
    digest: [u8; 32],
}

/// Poison the runtime if a reserved FRONTIER cycle exits before its new
/// durable boundary has been fully published, including during unwinding.
struct WalV8FrontierCycleGuard {
    coordinator: WalV8ParallelCoordinator,
    armed: bool,
}

impl WalV8FrontierCycleGuard {
    fn new(coordinator: &WalV8ParallelCoordinator) -> Self {
        Self {
            coordinator: coordinator.clone(),
            armed: true,
        }
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for WalV8FrontierCycleGuard {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator.poison();
        }
    }
}

struct ProducerCompletion {
    assignment: WalV8BatchAssignment,
    digest_ready: WalV8DigestReady,
    encoded: EncodedWalV8Batch<WalV8ParallelAdmissionGuard>,
}

/// Admitted v8 DATA work that can be finished after releasing the MVCC
/// sequencer. Its ticket remains guarded until a successful frontier sync.
#[must_use = "finish the admitted v8 producer job or drop it to poison the WAL"]
pub(crate) struct WalV8FrameJob<'a> {
    admitted:
        Option<AdmittedWalV8Batch<'a, WalV8ParallelAdmissionGuard, WalV8PreflightReservation>>,
    assignment: WalV8BatchAssignment,
    coordinator: WalV8ParallelCoordinator,
    producer_guards: Arc<Mutex<BTreeMap<u64, EncodedWalV8Batch<WalV8ParallelAdmissionGuard>>>>,
    write_file: File,
    data_buffer: DirectBuf,
    #[cfg(feature = "bench")]
    profile: Option<Arc<crate::mem_table::WriteProfile>>,
}

impl WalV8FrameJob<'_> {
    pub(crate) fn assignment(&self) -> WalV8BatchAssignment {
        self.assignment
    }

    pub(crate) fn frame_buffer_bytes(&self) -> usize {
        self.data_buffer.cap()
    }

    pub(crate) fn finish(mut self) -> Result<digest::WalV8DigestMetrics> {
        let admitted = self
            .admitted
            .take()
            .context("v8 producer job lost its admitted batch")?;
        let mut encoded = match admitted.encode() {
            Ok(encoded) => encoded,
            Err(error) => {
                self.coordinator.poison();
                return Err(error);
            }
        };
        let digest_ready = encoded.digest_ready()?;
        #[cfg(feature = "bench")]
        let ordered_link_hash_cpu_ns = self.coordinator.mark_digest_ready_with_cpu(digest_ready)?;
        #[cfg(not(feature = "bench"))]
        self.coordinator.mark_digest_ready(digest_ready)?;
        #[cfg(feature = "bench")]
        let metrics = {
            let mut metrics = encoded.digest_metrics();
            metrics.ordered_link_hash_cpu_ns = ordered_link_hash_cpu_ns;
            metrics
        };

        let data_len = encoded.encode_data_frames_into(self.data_buffer.as_mut_slice())?;
        ensure!(
            data_len == self.data_buffer.cap(),
            "v8 DATA materializer returned an unexpected frame length"
        );
        self.data_buffer.set_len(data_len);
        self.coordinator
            .mark_frames_ready(self.assignment.segment_ticket)?;
        #[cfg(feature = "bench")]
        let write_started = self.profile.as_ref().map(|_| Instant::now());
        let write_result = write_aligned_once(
            &self.write_file,
            self.data_buffer.initialized_slice(0, data_len),
            self.assignment.first_data_offset,
        );
        #[cfg(feature = "bench")]
        if let (Some(profile), Some(started)) = (&self.profile, write_started) {
            profile.record_wal_v8_data_write_call_ns(
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        write_result.context("write v8 Parallel DATA frames")?;
        // The admission reservation accounts for this buffer until DATA
        // completion is published. Drop it before notifying the coordinator,
        // which may retire that reservation as soon as the frontier syncs.
        drop(self.data_buffer);

        let ticket = self.assignment.segment_ticket;
        let previous = self.producer_guards.lock().insert(ticket, encoded);
        if previous.is_some() {
            self.coordinator.poison();
            anyhow::bail!("v8 Parallel ticket already owns a producer guard");
        }
        if let Err(error) = self.coordinator.mark_data_complete(ticket) {
            self.coordinator.poison();
            return Err(error);
        }

        #[cfg(feature = "bench")]
        return Ok(metrics);
        #[cfg(not(feature = "bench"))]
        {
            Ok(digest::WalV8DigestMetrics)
        }
    }
}

/// Mark the persistent resource charge at the exact admission boundary, even
/// if a later assignment validation fails or the caller unwinds.
struct WalV8LeaderAdmissionPublisher<'a> {
    coordinator: &'a WalV8ParallelCoordinator,
    reservation: RefCell<&'a mut Option<WalV7OwnedBatchReservation>>,
}

impl WalV8AdmissionPublisher for WalV8LeaderAdmissionPublisher<'_> {
    type Guard = WalV8ParallelAdmissionGuard;

    fn admit_and_publish(
        &self,
        plan: WalV8AdmissionPlan,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)> {
        let admitted = self.coordinator.admit_and_publish(plan)?;
        if let Some(reservation) = self.reservation.borrow_mut().as_mut() {
            reservation.mark_admitted();
        }
        Ok(admitted)
    }
}

impl WalV8LeaderWriter {
    /// Resume a writer with image-bound source and recovery-workspace charges.
    pub(crate) fn new_with_reservation_owner(
        write_file: File,
        sync_file: File,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
        reservation_owner: Arc<WalV8ReservationOwner>,
    ) -> Result<Self> {
        reservation_owner.validate_image_and_workspace(installed, max_file_size)?;
        Self::new_inner(
            write_file,
            sync_file,
            installed,
            max_file_size,
            Some(reservation_owner),
        )
    }

    /// Resume a v8 Leader writer from a durably installed Active recovery.
    #[cfg(test)]
    pub(crate) fn new(
        write_file: File,
        sync_file: File,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
    ) -> Result<Self> {
        Self::new_inner(write_file, sync_file, installed, max_file_size, None)
    }

    fn new_inner(
        write_file: File,
        sync_file: File,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
        reservation_owner: Option<Arc<WalV8ReservationOwner>>,
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
        let coordinator = WalV8ParallelCoordinator::new(
            header,
            WalV8ParallelConfig {
                next_ticket: installed.next_ticket,
                next_data_offset: installed.image_len,
                last_commit_ts: installed.active_boundary.last_commit_ts,
                last_recorded_at: installed.last_recorded_at,
                durable_prefix_digest: installed.active_boundary.prefix_digest,
                max_inflight_tickets: MAX_PARALLEL_INFLIGHT_TICKETS,
                max_inflight_bytes: max_file_size,
                max_file_size,
            },
        )?;
        ensure!(
            coordinator.durable_ticket_end() == installed.active_boundary.ticket_end
                && coordinator.last_frontier_end() == installed.image_len
                && coordinator.durable_prefix_digest() == installed.active_boundary.prefix_digest,
            "v8 admission coordinator differs from the installed recovery boundary"
        );

        Ok(Self {
            write_file,
            sync_file,
            header,
            physical_header_digest,
            initial_prefix_digest,
            coordinator,
            reservation_owner,
            frontier: installed.frontier,
            frontier_offset: installed.frontier_offset,
            frontier_digest: installed.frontier_digest,
            max_file_size,
            installed_dev: installed.installed_dev,
            installed_ino: installed.installed_ino,
            #[cfg(feature = "bench")]
            profile: arc_swap::ArcSwapOption::empty(),
        })
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_write_profile(&self, profile: Arc<crate::mem_table::WriteProfile>) {
        self.profile.store(Some(profile));
    }

    /// Transfer the validated image and coordinator to the concurrent
    /// producer path. Only the frontier write and WAL sync remain serialized.
    pub(crate) fn into_parallel(self) -> WalV8ParallelWriter {
        WalV8ParallelWriter {
            write_file: self.write_file,
            sync_file: self.sync_file,
            header: self.header,
            physical_header_digest: self.physical_header_digest,
            initial_prefix_digest: self.initial_prefix_digest,
            coordinator: self.coordinator,
            reservation_owner: self.reservation_owner,
            frontier: Mutex::new(WalV8ParallelFrontier {
                frontier: self.frontier,
                offset: self.frontier_offset,
                digest: self.frontier_digest,
            }),
            frontier_resources: Mutex::new(BTreeMap::new()),
            producer_guards: Arc::new(Mutex::new(BTreeMap::new())),
            max_file_size: self.max_file_size,
            installed_dev: self.installed_dev,
            installed_ino: self.installed_ino,
            #[cfg(feature = "bench")]
            profile: self.profile,
        }
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
        let mut prepared_batches = Vec::new();
        prepared_batches
            .try_reserve_exact(batches.len())
            .context("reserve v8 preflighted group")?;
        let mut metadata = Vec::new();
        metadata
            .try_reserve_exact(batches.len())
            .context("reserve v8 group metadata")?;
        for batch in batches {
            prepared_batches.push(self.preflight_batch(batch, limits)?);
            metadata.push((batch.commit_ts, batch.recorded_at));
        }
        self.append_prepared_group(prepared_batches, &metadata)
    }

    /// Preflight canonical operation sizes and reserve preparation memory
    /// before the caller enters the MVCC commit sequencer.
    pub(crate) fn preflight_batch<'a>(
        &self,
        batch: &'a WalBatch,
        limits: WalV5Limits,
    ) -> Result<WalV8PreparedBatch<'a>> {
        ensure!(!self.is_poisoned(), "v8 Leader WAL is poisoned");
        let limits = bounded_limits(limits);
        WalV8PreparedBatch::preflight(batch, limits, |bytes| {
            ensure!(
                bytes <= self.max_file_size,
                WalV8LeaderCapacityError::GroupExceedsAdmissionWindow
            );
            let source = self
                .reservation_owner
                .as_ref()
                .map(|owner| owner.reserve_preparation_buffer(bytes))
                .transpose()?
                .flatten();
            let coordinator = self.coordinator.reserve_preparation(bytes)?;
            Ok(WalV8PreflightReservation {
                _coordinator: coordinator,
                _source: source,
            })
        })
    }

    /// Admit a preflighted ticket with sequencer-owned MVCC metadata.
    pub(crate) fn append_prepared_batch(
        &mut self,
        prepared: WalV8PreparedBatch<'_>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
    ) -> Result<(WalV8LeaderCommit, digest::WalV8DigestMetrics)> {
        let mut group = self.append_prepared_group(vec![prepared], &[(commit_ts, recorded_at)])?;
        let commit = group
            .commits
            .pop()
            .context("v8 one-ticket group returned no commit")?;
        #[cfg(feature = "bench")]
        let digest_metrics = group.digest_metrics;
        #[cfg(not(feature = "bench"))]
        let digest_metrics = digest::WalV8DigestMetrics;
        Ok((commit, digest_metrics))
    }

    fn append_prepared_group<'a>(
        &mut self,
        mut prepared_batches: Vec<WalV8PreparedBatch<'a>>,
        metadata: &[(u64, crate::pitr::RecordedAt)],
    ) -> Result<WalV8LeaderCommitGroup> {
        ensure!(!self.is_poisoned(), "v8 Leader WAL is poisoned");
        #[cfg(feature = "bench")]
        let ordered_link_hash_cpu_ns_before = self.coordinator.ordered_link_hash_cpu_ns();
        #[cfg(feature = "bench")]
        let profile = self.profile.load_full();
        ensure!(
            !prepared_batches.is_empty()
                && prepared_batches.len() <= MAX_GROUP_TICKETS
                && prepared_batches.len() == metadata.len(),
            "v8 preflighted group size is outside its bounded ticket window"
        );
        let mut batch_resources = Vec::new();
        batch_resources
            .try_reserve_exact(prepared_batches.len())
            .context("reserve v8 batch resource guards")?;
        let mut data_buffers = Vec::new();
        data_buffers
            .try_reserve_exact(prepared_batches.len())
            .context("reserve v8 producer frame buffers")?;
        let mut write_handles = Vec::new();
        write_handles
            .try_reserve_exact(prepared_batches.len())
            .context("reserve v8 producer write handles")?;
        let mut admitted_batches = Vec::new();
        admitted_batches
            .try_reserve_exact(prepared_batches.len())
            .context("reserve v8 admitted group")?;
        let mut producer_completions = Vec::new();
        producer_completions
            .try_reserve_exact(prepared_batches.len())
            .context("reserve v8 producer completions")?;
        let mut commits = Vec::new();
        commits
            .try_reserve_exact(prepared_batches.len())
            .context("reserve v8 durable commit results")?;

        let preparation_index_bytes =
            prepared_batches.iter().try_fold(0_u64, |total, prepared| {
                total
                    .checked_add(prepared.preparation_index_bytes())
                    .context("v8 group preparation index reservation overflows")
            })?;
        ensure!(
            preparation_index_bytes <= self.max_file_size,
            WalV8LeaderCapacityError::GroupExceedsAdmissionWindow
        );
        let mut previous_commit_ts = self.frontier.last_commit_ts;
        let mut frontier_offset = self.coordinator.last_frontier_end();
        for ((commit_ts, _), prepared) in metadata.iter().zip(&prepared_batches) {
            ensure!(
                *commit_ts > previous_commit_ts,
                "v8 Leader group commit timestamps do not advance"
            );
            frontier_offset = frontier_offset
                .checked_add(u64::try_from(prepared.frame_buffer_bytes())?)
                .context("v8 group DATA allocation end overflows")?;
            previous_commit_ts = *commit_ts;
        }
        let group_marker_bytes = u64::try_from(prepared_batches.len())?
            .checked_mul(FRAME_LEN_U64)
            .context("v8 group FRONTIER reservation overflows")?;
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

        let group_buffer_bytes =
            prepared_batches
                .iter()
                .try_fold(preparation_index_bytes, |total, prepared| {
                    let logical_bytes = u64::try_from(prepared.logical_batch_bytes())
                        .context("v8 logical batch reservation exceeds u64")?;
                    let frame_bytes = u64::try_from(prepared.frame_buffer_bytes())
                        .context("v8 frame buffer reservation exceeds u64")?;
                    total
                        .checked_add(logical_bytes)
                        .and_then(|total| total.checked_add(frame_bytes))
                        .context("v8 group buffer reservation overflows")
                })?;
        ensure!(
            group_buffer_bytes <= self.max_file_size,
            WalV8LeaderCapacityError::GroupExceedsAdmissionWindow
        );

        // Admission reserves one worst-case marker per ticket, even though
        // this Leader group normally publishes a single shared FRONTIER.
        let reserved_end = frontier_offset
            .checked_add(group_marker_bytes)
            .context("v8 group FRONTIER allocation end overflows")?;
        if reserved_end > self.max_file_size {
            let fresh_end = HEADER_LEN_U64
                .checked_add(FRAME_LEN_U64)
                .and_then(|start| {
                    prepared_batches.iter().try_fold(start, |offset, prepared| {
                        offset.checked_add(u64::try_from(prepared.frame_buffer_bytes()).ok()?)
                    })
                })
                .and_then(|end| end.checked_add(group_marker_bytes))
                .context("v8 fresh-segment capacity calculation overflows")?;
            let error = if fresh_end <= self.max_file_size {
                WalV8LeaderCapacityError::RotationRequired
            } else {
                WalV8LeaderCapacityError::BatchTooLarge
            };
            return Err(anyhow::Error::new(error));
        }

        // Reserve every DATA range, worst-case marker, and buffer before the
        // first ticket is assigned or a frame buffer is allocated.
        for prepared in &prepared_batches {
            let reservation = self
                .reservation_owner
                .as_ref()
                .map(|owner| {
                    owner.reserve_batch(
                        u64::try_from(prepared.logical_batch_bytes())?,
                        u64::try_from(prepared.frame_buffer_bytes())?,
                    )
                })
                .transpose()?;
            batch_resources.push(reservation);
        }

        // Delay large aligned allocations and handle clones until metadata,
        // timestamp, physical-capacity, and aggregate-memory preflight pass.
        for prepared in &prepared_batches {
            data_buffers.push(DirectBuf::new(prepared.frame_buffer_bytes()));
            write_handles.push(
                self.write_file
                    .try_clone()
                    .context("clone v8 WAL data-write handle before admission")?,
            );
        }
        let mut frontier_buffer = DirectBuf::new(WAL_V8_FRAME_LEN);

        // Resource buffers and write handles exist before admission. Publish
        // every ticket through the actual coordinator before a producer can
        // encode or hash payload bytes.
        let ticket_start = self.coordinator.next_ticket();
        ensure!(
            self.coordinator.admitted_window().is_empty(),
            "serial v8 protocol writer has unresolved tickets"
        );
        for ((prepared, reservation), (commit_ts, recorded_at)) in prepared_batches
            .drain(..)
            .zip(&mut batch_resources)
            .zip(metadata.iter().copied())
        {
            let publisher = WalV8LeaderAdmissionPublisher {
                coordinator: &self.coordinator,
                reservation: RefCell::new(reservation),
            };
            admitted_batches.push(prepared.admit_with_metadata(
                commit_ts,
                recorded_at,
                &publisher,
            )?);
        }

        let producer_result = thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            handles
                .try_reserve_exact(admitted_batches.len())
                .context("reserve scoped v8 producer handles")?;
            for (admitted, (mut data_buffer, write_file)) in admitted_batches
                .into_iter()
                .zip(data_buffers.into_iter().zip(write_handles))
            {
                let coordinator = self.coordinator.clone();
                #[cfg(feature = "bench")]
                let data_write_profile = profile.clone();
                handles.push(scope.spawn(move || -> Result<ProducerCompletion> {
                    let mut encoded = admitted.encode()?;
                    let assignment = encoded.assignment();
                    let digest_ready = encoded.digest_ready()?;
                    coordinator.mark_digest_ready(digest_ready)?;
                    let data_len = encoded.encode_data_frames_into(data_buffer.as_mut_slice())?;
                    ensure!(
                        data_len == data_buffer.cap(),
                        "v8 DATA materializer returned an unexpected frame length"
                    );
                    data_buffer.set_len(data_len);
                    coordinator.mark_frames_ready(assignment.segment_ticket)?;
                    #[cfg(feature = "bench")]
                    let write_started = data_write_profile.as_ref().map(|_| Instant::now());
                    let write_result = write_aligned_once(
                        &write_file,
                        data_buffer.initialized_slice(0, data_len),
                        assignment.first_data_offset,
                    );
                    #[cfg(feature = "bench")]
                    if let (Some(profile), Some(started)) = (&data_write_profile, write_started) {
                        profile.record_wal_v8_data_write_call_ns(
                            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        );
                    }
                    write_result.context("write v8 Parallel DATA frames")?;
                    coordinator.mark_data_complete(assignment.segment_ticket)?;
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

        let ticket_end = ticket_start
            .checked_add(u64::try_from(producer_completions.len())?)
            .context("v8 group ticket end overflows")?;
        let ready_prefix = self.coordinator.frontier_ready_prefix();
        ensure!(
            ready_prefix.len() >= producer_completions.len(),
            "v8 coordinator did not observe the complete DATA/digest-ready group"
        );
        for (progress, expected_ticket) in ready_prefix
            .iter()
            .take(producer_completions.len())
            .zip(ticket_start..ticket_end)
        {
            ensure!(
                progress.assignment.segment_ticket == expected_ticket,
                "v8 coordinator frontier prefix is not ticket-contiguous"
            );
        }
        for (completion, expected_ticket) in
            producer_completions.iter().zip(ticket_start..ticket_end)
        {
            ensure!(
                completion.assignment.segment_ticket == expected_ticket
                    && completion.digest_ready.segment_ticket == expected_ticket
                    && completion.digest_ready.segment_incarnation == self.header.incarnation,
                "v8 producer completion does not match its admitted ticket"
            );
        }
        let reservation = self.coordinator.reserve_frontier(ticket_end)?;
        ensure!(
            reservation.frame_offset == frontier_offset,
            "v8 coordinator FRONTIER offset differs from preflight reservation"
        );

        let frontier = WalV8Frontier {
            generation: next_generation,
            ticket_end,
            durable_end: frontier_offset.max(previous_frontier_end),
            last_commit_ts: previous_commit_ts,
            prefix_digest: reservation.prefix_digest,
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

        #[cfg(feature = "bench")]
        let frontier_write_started = profile.as_ref().map(|_| Instant::now());
        write_aligned_once(
            &self.write_file,
            frontier_buffer.initialized_slice(0, WAL_V8_FRAME_LEN),
            frontier_offset,
        )
        .context("write v8 Leader FRONTIER")?;
        #[cfg(feature = "bench")]
        if let (Some(profile), Some(started)) = (&profile, frontier_write_started) {
            let data_bytes = producer_completions
                .iter()
                .fold(0_u64, |total, completion| {
                    total.saturating_add(completion.encoded.frame_buffer_bytes() as u64)
                });
            profile.record_wal_commit_group(producer_completions.len() as u64, data_bytes);
            profile.record_wal_frontier_write(
                WAL_V8_FRAME_LEN as u64,
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        #[cfg(feature = "bench")]
        let sync_started = profile.as_ref().map(|_| Instant::now());
        let sync_result = fdatasync_once(&self.sync_file);
        #[cfg(feature = "bench")]
        if let (Some(profile), Some(started)) = (&profile, sync_started) {
            let duration_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            profile.record_wal_sync(producer_completions.len() as u64);
            profile.record_wal_fdatasync_ns(duration_ns);
            if profile.wal_sync_diagnostics_enabled() {
                profile.record_wal_sync_observation(crate::mem_table::WalSyncObservation {
                    captured_target: ticket_end,
                    written_frontier_start: self.frontier.ticket_end,
                    written_frontier_end: ticket_end,
                    groups_completed: producer_completions.len() as u64,
                    groups_covered: producer_completions.len() as u64,
                    duration_ns,
                    succeeded: sync_result.is_ok(),
                    ..crate::mem_table::WalSyncObservation::default()
                });
            }
        }
        sync_result.context("fdatasync v8 Leader WAL")?;
        // The group used one marker. Retire each additional marker credit only
        // after that marker and all DATA have reached the durable boundary.
        for reservation in batch_resources.iter_mut().skip(1).flatten() {
            reservation
                .release_unused_frontier_credit()
                .context("release coalesced v8 FRONTIER reservation")?;
        }
        self.coordinator.publish_durable_frontier(reservation)?;

        // Everything below is infallible publication after the successful
        // sync and coordinator publication. Every ticket guard remains armed
        // until this boundary.
        self.frontier = frontier;
        self.frontier_offset = frontier_offset;
        self.frontier_digest = frontier_digest;
        for completion in &mut producer_completions {
            completion.encoded.disarm_after_durable();
        }

        #[cfg(feature = "bench")]
        let mut digest_metrics = digest::WalV8DigestMetrics::default();
        #[cfg(feature = "bench")]
        for completion in &producer_completions {
            digest_metrics.add_assign(completion.encoded.digest_metrics());
        }
        #[cfg(feature = "bench")]
        {
            digest_metrics.ordered_link_hash_cpu_ns = self
                .coordinator
                .ordered_link_hash_cpu_ns()
                .saturating_sub(ordered_link_hash_cpu_ns_before);
            if let Some(profile) = &profile {
                profile.record_wal_v8_digest_metrics(digest_metrics);
            }
        }

        for completion in producer_completions {
            commits.push(WalV8LeaderCommit {
                ticket: completion.assignment.segment_ticket,
                first_data_offset: completion.assignment.first_data_offset,
                frontier,
                frontier_offset,
                frontier_digest,
            });
        }
        Ok(WalV8LeaderCommitGroup {
            commits,
            frontier,
            #[cfg(feature = "bench")]
            digest_metrics,
        })
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.coordinator.is_poisoned()
    }

    pub(crate) fn try_clone_sync_file(&self) -> Result<File> {
        self.sync_file
            .try_clone()
            .context("clone v8 WAL sync handle")
    }

    pub(crate) fn image_len(&self) -> Result<u64> {
        self.frontier_offset
            .checked_add(FRAME_LEN_U64)
            .context("v8 Active WAL image length overflows")
    }

    pub(crate) fn active_boundaries(
        &self,
    ) -> Result<(
        crate::pitr::manifest::ActiveBoundary,
        crate::pitr::manifest::ActiveBoundary,
    )> {
        let empty = crate::pitr::manifest::ActiveBoundary {
            timeline_id: self.header.timeline_id.0,
            archive_epoch_id: self.header.archive_epoch_id.0,
            segment_id: self.header.segment_id.0,
            incarnation: self.header.incarnation,
            wal_format: crate::pitr::manifest::PitrWalFormat::V8,
            ticket_end: 0,
            durable_end: HEADER_LEN_U64,
            last_commit_ts: 0,
            prefix_digest: self.initial_prefix_digest,
        };
        let active = crate::pitr::manifest::ActiveBoundary {
            ticket_end: self.frontier.ticket_end,
            durable_end: self.frontier.durable_end,
            last_commit_ts: self.frontier.last_commit_ts,
            prefix_digest: self.frontier.prefix_digest,
            ..empty
        };
        empty.validate()?;
        active.validate()?;
        Ok((empty, active))
    }
}

impl WalV8ParallelWriter {
    #[cfg(feature = "bench")]
    pub(crate) fn set_write_profile(&self, profile: Arc<crate::mem_table::WriteProfile>) {
        self.profile.store(Some(profile));
    }

    /// Reserve bounded preparation state before canonicalizing a batch.
    pub(crate) fn preflight_batch<'a>(
        &self,
        batch: &'a WalBatch,
        limits: WalV5Limits,
    ) -> Result<WalV8PreparedBatch<'a>> {
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        let limits = bounded_limits(limits);
        WalV8PreparedBatch::preflight(batch, limits, |bytes| {
            ensure!(
                bytes <= self.max_file_size,
                WalV8LeaderCapacityError::GroupExceedsAdmissionWindow
            );
            let source = self
                .reservation_owner
                .as_ref()
                .map(|owner| owner.reserve_preparation_buffer(bytes))
                .transpose()?
                .flatten();
            let coordinator = self.coordinator.reserve_preparation(bytes)?;
            Ok(WalV8PreflightReservation {
                _coordinator: coordinator,
                _source: source,
            })
        })
    }

    /// Admit, encode and write one ticket without serializing it behind other
    /// producers. The caller returns only after its ticket is covered by a
    /// successful embedded FRONTIER and WAL sync.
    pub(crate) fn append_prepared_batch(
        &self,
        prepared: WalV8PreparedBatch<'_>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
    ) -> Result<(WalV8LeaderCommit, digest::WalV8DigestMetrics)> {
        self.append_prepared_batch_inner(prepared, commit_ts, recorded_at, || {})
    }

    fn append_prepared_batch_inner<F>(
        &self,
        prepared: WalV8PreparedBatch<'_>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
        after_admission: F,
    ) -> Result<(WalV8LeaderCommit, digest::WalV8DigestMetrics)>
    where
        F: FnOnce(),
    {
        let job =
            self.admit_prepared_batch_inner(prepared, commit_ts, recorded_at, after_admission)?;
        let assignment = job.assignment();
        let metrics = job.finish()?;
        #[cfg(feature = "bench")]
        if let Some(profile) = self.profile.load_full() {
            profile.record_wal_v8_digest_metrics(metrics);
        }
        if let Err(error) = self.wait_durable(assignment.segment_ticket) {
            self.coordinator.poison();
            return Err(error);
        }

        let frontier = self.frontier.lock();
        let commit = WalV8LeaderCommit {
            ticket: assignment.segment_ticket,
            first_data_offset: assignment.first_data_offset,
            frontier: frontier.frontier,
            frontier_offset: frontier.offset,
            frontier_digest: frontier.digest,
        };
        Ok((commit, metrics))
    }

    /// Admit one batch and return its producer job without doing payload-sized
    /// encoding or I/O. The caller can release the MVCC sequencer before
    /// finishing this job, allowing later tickets to be admitted concurrently.
    pub(crate) fn admit_prepared_batch<'a>(
        &self,
        prepared: WalV8PreparedBatch<'a>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
    ) -> Result<WalV8FrameJob<'a>> {
        self.admit_prepared_batch_inner(prepared, commit_ts, recorded_at, || {})
    }

    fn admit_prepared_batch_inner<'a, F>(
        &self,
        prepared: WalV8PreparedBatch<'a>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
        after_admission: F,
    ) -> Result<WalV8FrameJob<'a>>
    where
        F: FnOnce(),
    {
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        let logical_bytes = u64::try_from(prepared.logical_batch_bytes())?;
        let frame_bytes = u64::try_from(prepared.frame_buffer_bytes())?;
        let mut reservation = self
            .reservation_owner
            .as_ref()
            .map(|owner| owner.reserve_batch(logical_bytes, frame_bytes))
            .transpose()?;

        // Allocate buffers and handles before publishing the ticket. Once
        // admission succeeds, every resource required to finish is already
        // owned by this producer.
        let data_buffer = DirectBuf::new(prepared.frame_buffer_bytes());
        let write_file = self
            .write_file
            .try_clone()
            .context("clone v8 Parallel DATA write handle before admission")?;
        let publisher = WalV8LeaderAdmissionPublisher {
            coordinator: &self.coordinator,
            reservation: RefCell::new(&mut reservation),
        };
        let admitted = prepared.admit_with_metadata(commit_ts, recorded_at, &publisher)?;
        let assignment = admitted.assignment();
        if let Some(reservation) = reservation.take() {
            let previous = self
                .frontier_resources
                .lock()
                .insert(assignment.segment_ticket, reservation);
            if previous.is_some() {
                self.coordinator.poison();
                anyhow::bail!("v8 Parallel ticket already owns a resource reservation");
            }
        }
        after_admission();
        Ok(WalV8FrameJob {
            admitted: Some(admitted),
            assignment,
            coordinator: self.coordinator.clone(),
            producer_guards: Arc::clone(&self.producer_guards),
            write_file,
            data_buffer,
            #[cfg(feature = "bench")]
            profile: self.profile.load_full(),
        })
    }

    fn advance_frontier_through(&self, ticket: u64) -> Result<()> {
        let ticket_end = ticket
            .checked_add(1)
            .context("v8 Parallel durable ticket end overflows")?;
        loop {
            if self.coordinator.is_poisoned() {
                anyhow::bail!("v8 Parallel WAL is poisoned");
            }
            if self.coordinator.durable_ticket_end() >= ticket_end {
                return Ok(());
            }

            let observed_generation = self.coordinator.change_generation();
            let mut previous = self.frontier.lock();
            if self.coordinator.durable_ticket_end() >= ticket_end {
                return Ok(());
            }
            let ready = self.coordinator.frontier_ready_prefix();
            if ready.is_empty() {
                drop(previous);
                let wake = self
                    .coordinator
                    .wait_for_change(observed_generation, Duration::from_secs(1));
                if wake.poisoned {
                    anyhow::bail!("v8 Parallel WAL was poisoned while waiting for durability");
                }
                if wake.closed && self.coordinator.durable_ticket_end() < ticket_end {
                    anyhow::bail!("v8 Parallel WAL closed before the ticket became durable");
                }
                continue;
            }

            let first_ticket = self.coordinator.durable_ticket_end();
            let frontier_ticket_end = first_ticket
                .checked_add(u64::try_from(ready.len())?)
                .context("v8 Parallel frontier ticket end overflows")?;
            let last = ready
                .last()
                .context("v8 Parallel ready prefix unexpectedly empty")?;
            let last_data_end = last
                .assignment
                .first_data_offset
                .checked_add(u64::try_from(last.frame_buffer_bytes)?)
                .context("v8 Parallel durable DATA end overflows")?;
            let previous_frontier_end = previous
                .offset
                .checked_add(FRAME_LEN_U64)
                .context("v8 previous FRONTIER end overflows")?;
            let reservation = self.coordinator.reserve_frontier(frontier_ticket_end)?;
            let mut cycle_guard = WalV8FrontierCycleGuard::new(&self.coordinator);
            let durable_end = last_data_end.max(previous_frontier_end);
            ensure!(
                previous_frontier_end <= durable_end
                    && durable_end <= reservation.frame_offset
                    && reservation.frame_offset + FRAME_LEN_U64 <= self.max_file_size,
                "v8 Parallel FRONTIER physical boundaries are invalid"
            );
            let frontier = WalV8Frontier {
                generation: previous
                    .frontier
                    .generation
                    .checked_add(1)
                    .context("v8 FRONTIER generation overflows")?,
                ticket_end: frontier_ticket_end,
                durable_end,
                last_commit_ts: last.assignment.commit_ts,
                prefix_digest: reservation.prefix_digest,
                previous_frontier_offset: previous.offset,
                previous_frontier_digest: previous.digest,
            };
            let frontier_frame = encode_frontier_successor(
                frontier,
                previous.frontier,
                previous.offset,
                &previous.digest,
                reservation.frame_offset,
                self.physical_header_digest,
                self.initial_prefix_digest,
            )?;
            let frontier_digest = frame_record_digest(&frontier_frame)?;
            let mut frontier_buffer = DirectBuf::new(WAL_V8_FRAME_LEN);
            frontier_buffer.as_mut_slice()[..WAL_V8_FRAME_LEN].copy_from_slice(&frontier_frame);
            frontier_buffer.set_len(WAL_V8_FRAME_LEN);
            #[cfg(feature = "bench")]
            let profile = self.profile.load_full();
            #[cfg(feature = "bench")]
            let frontier_write_started = profile.as_ref().map(|_| Instant::now());
            write_aligned_once(
                &self.write_file,
                frontier_buffer.initialized_slice(0, WAL_V8_FRAME_LEN),
                reservation.frame_offset,
            )
            .context("write v8 Parallel FRONTIER")?;
            #[cfg(feature = "bench")]
            if let (Some(profile), Some(started)) = (&profile, frontier_write_started) {
                let data_bytes = ready.iter().fold(0_u64, |total, progress| {
                    total.saturating_add(progress.frame_buffer_bytes as u64)
                });
                let groups = ready.len() as u64;
                profile.record_wal_commit_group(groups, data_bytes);
                profile.record_wal_frontier_write(
                    WAL_V8_FRAME_LEN as u64,
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
            }
            #[cfg(feature = "bench")]
            let sync_started = profile.as_ref().map(|_| Instant::now());
            let sync_result = fdatasync_once(&self.sync_file);
            #[cfg(feature = "bench")]
            if let (Some(profile), Some(started)) = (&profile, sync_started) {
                let duration_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                let groups = ready.len() as u64;
                profile.record_wal_sync(groups);
                profile.record_wal_fdatasync_ns(duration_ns);
                if profile.wal_sync_diagnostics_enabled() {
                    profile.record_wal_sync_observation(crate::mem_table::WalSyncObservation {
                        captured_target: frontier_ticket_end,
                        written_frontier_start: first_ticket,
                        written_frontier_end: frontier_ticket_end,
                        groups_completed: groups,
                        groups_covered: groups,
                        duration_ns,
                        succeeded: sync_result.is_ok(),
                        ..crate::mem_table::WalSyncObservation::default()
                    });
                }
            }
            sync_result.context("fdatasync v8 Parallel WAL")?;

            self.retire_frontier_resources(first_ticket, frontier_ticket_end)?;
            self.coordinator.publish_durable_frontier(reservation)?;
            self.retire_frontier_guards(first_ticket, frontier_ticket_end)?;
            previous.frontier = frontier;
            previous.offset = reservation.frame_offset;
            previous.digest = frontier_digest;
            cycle_guard.complete();
        }
    }

    fn retire_frontier_resources(&self, first_ticket: u64, ticket_end: u64) -> Result<()> {
        if self.reservation_owner.is_none() {
            return Ok(());
        }
        let mut resources = self.frontier_resources.lock();
        let tickets: Vec<u64> = resources
            .range(first_ticket..ticket_end)
            .map(|(&t, _)| t)
            .collect();
        let expected = usize::try_from(ticket_end - first_ticket)?;
        ensure!(
            tickets.len() == expected,
            "v8 Parallel durable prefix is missing resource reservations"
        );
        for (index, ticket) in tickets.into_iter().enumerate() {
            let mut reservation = resources
                .remove(&ticket)
                .context("v8 Parallel resource reservation disappeared")?;
            if index > 0 {
                reservation
                    .release_unused_frontier_credit()
                    .context("release coalesced v8 FRONTIER reservation")?;
            }
            reservation
                .finish_after_io()
                .context("retire durable v8 batch resources")?;
        }
        Ok(())
    }

    fn retire_frontier_guards(&self, first_ticket: u64, ticket_end: u64) -> Result<()> {
        let mut guards = self.producer_guards.lock();
        let tickets: Vec<u64> = guards
            .range(first_ticket..ticket_end)
            .map(|(&ticket, _)| ticket)
            .collect();
        let expected = usize::try_from(ticket_end - first_ticket)?;
        ensure!(
            tickets.len() == expected,
            "v8 Parallel durable prefix is missing producer guards"
        );
        for ticket in tickets {
            let mut encoded = guards
                .remove(&ticket)
                .context("v8 Parallel producer guard disappeared")?;
            encoded.disarm_after_durable();
        }
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        let ticket_end = self.coordinator.next_ticket();
        if ticket_end == 0 {
            return Ok(());
        }
        if let Err(error) = self.advance_frontier_through(ticket_end - 1) {
            self.coordinator.poison();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn wait_durable(&self, ticket: u64) -> Result<()> {
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        ensure!(
            ticket < self.coordinator.next_ticket(),
            "v8 Parallel durability wait names an unassigned ticket"
        );
        if self.coordinator.durable_ticket_end() > ticket {
            return Ok(());
        }
        if let Err(error) = self.advance_frontier_through(ticket) {
            self.coordinator.poison();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn next_ticket(&self) -> u64 {
        self.coordinator.next_ticket()
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.coordinator.is_poisoned()
    }

    pub(crate) fn try_clone_sync_file(&self) -> Result<File> {
        self.sync_file
            .try_clone()
            .context("clone v8 Parallel WAL sync handle")
    }

    pub(crate) fn image_len(&self) -> Result<u64> {
        self.frontier
            .lock()
            .offset
            .checked_add(FRAME_LEN_U64)
            .context("v8 Active WAL image length overflows")
    }

    pub(crate) fn logical_tail(&self) -> u64 {
        self.coordinator.last_frontier_end()
    }

    pub(crate) fn active_boundaries(
        &self,
    ) -> Result<(
        crate::pitr::manifest::ActiveBoundary,
        crate::pitr::manifest::ActiveBoundary,
    )> {
        let frontier = self.frontier.lock();
        let empty = crate::pitr::manifest::ActiveBoundary {
            timeline_id: self.header.timeline_id.0,
            archive_epoch_id: self.header.archive_epoch_id.0,
            segment_id: self.header.segment_id.0,
            incarnation: self.header.incarnation,
            wal_format: crate::pitr::manifest::PitrWalFormat::V8,
            ticket_end: 0,
            durable_end: HEADER_LEN_U64,
            last_commit_ts: 0,
            prefix_digest: self.initial_prefix_digest,
        };
        let active = crate::pitr::manifest::ActiveBoundary {
            ticket_end: frontier.frontier.ticket_end,
            durable_end: frontier.frontier.durable_end,
            last_commit_ts: frontier.frontier.last_commit_ts,
            prefix_digest: frontier.frontier.prefix_digest,
            ..empty
        };
        empty.validate()?;
        active.validate()?;
        Ok((empty, active))
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
    if flags & libc::O_ACCMODE == libc::O_RDONLY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "v8 WAL positional write handle is not writable",
        ));
    }
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
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::fail_point!("wal.v8.leader.fdatasync", |_| Err(io::Error::other(
        "injected v8 WAL sync failure"
    )));
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
    use std::{
        ffi::CString,
        fs::OpenOptions,
        os::fd::FromRawFd,
        path::Path,
        sync::{Arc, mpsc},
        thread,
        time::{Duration, Instant},
    };

    use anyhow::Result;

    use super::*;
    use crate::pitr::{
        ArchiveEpochId, ChainAnchor, RecordedAt, SegmentId, TimelineId, WalEntry,
        manifest::ActiveBoundary,
    };
    use crate::wal::v7::admission::{
        WalV7ResourceCharge, WalV7ResourceLedger, WalV7ResourceLimits, WalV7RetainedResources,
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

    fn replayed_batches(
        path: &Path,
        installed: &super::super::install::WalV8InstalledRecovery,
    ) -> Result<Vec<WalBatch>> {
        let mut batches = Vec::new();
        super::super::install::replay_installed_active_batches(
            path,
            installed,
            LIVE_WAL_V5_LIMITS,
            |batch| {
                batches.push(batch.clone());
                Ok(())
            },
        )?;
        Ok(batches)
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

    #[cfg(feature = "bench")]
    fn assert_one_two_ticket_profile(profile: &crate::mem_table::WriteProfile) {
        let snapshot = profile.snapshot();
        assert_eq!(snapshot.wal_commit_groups, 1);
        assert_eq!(snapshot.wal_commit_solo_groups, 0);
        assert_eq!(snapshot.wal_commit_buffers, 2);
        assert_eq!(snapshot.wal_commit_bytes, 2 * FRAME_LEN_U64);
        assert_eq!(snapshot.wal_sync_count, 1);
        assert_eq!(snapshot.wal_sync_groups_total, 2);
        assert_eq!(snapshot.wal_frontier_writes, 1);
        assert_eq!(snapshot.wal_frontier_bytes, FRAME_LEN_U64);
        assert!(snapshot.wal_fdatasync_ns > 0);
    }

    #[cfg(feature = "bench")]
    #[test]
    fn benchmark_profile_counts_v8_leader_and_parallel_durability_groups() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let leader_path = directory.path().join("leader-v8.wal");
        let leader_installed = install_empty(&leader_path, header())?;
        let leader_profile = Arc::new(crate::mem_table::WriteProfile::default());
        let mut leader = open_writer(&leader_path, &leader_installed)?;
        leader.set_write_profile(Arc::clone(&leader_profile));
        leader.append_group(&[batch(10), batch(20)], LIVE_WAL_V5_LIMITS)?;
        assert_one_two_ticket_profile(&leader_profile);
        assert!(leader_profile.snapshot().wal_v8_payload_hash_bytes > 0);

        let parallel_path = directory.path().join("parallel-v8.wal");
        let parallel_installed = install_empty(&parallel_path, header())?;
        let parallel_profile = Arc::new(crate::mem_table::WriteProfile::default());
        let parallel = open_writer(&parallel_path, &parallel_installed)?.into_parallel();
        parallel.set_write_profile(Arc::clone(&parallel_profile));
        let first_batch = batch(10);
        let second_batch = batch(20);
        let first = parallel.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?;
        let second = parallel.preflight_batch(&second_batch, LIVE_WAL_V5_LIMITS)?;
        let first_job = parallel.admit_prepared_batch(first, 10, first_batch.recorded_at)?;
        let second_job = parallel.admit_prepared_batch(second, 20, second_batch.recorded_at)?;
        first_job.finish()?;
        second_job.finish()?;
        parallel.wait_durable(1)?;
        assert_one_two_ticket_profile(&parallel_profile);

        Ok(())
    }

    fn open_accounted_writer(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
        max_source_spool_bytes: u64,
        allocation_unit: u64,
        sync_file: File,
    ) -> Result<(WalV8LeaderWriter, Arc<WalV8ReservationOwner>)> {
        let ledger = WalV7ResourceLedger::new(WalV7ResourceLimits {
            max_logical_wal_bytes: max_file_size,
            max_source_spool_bytes,
            max_recovery_workspace_bytes: max_file_size,
            max_buffer_memory_bytes: max_file_size * 2,
            max_seal_index_bytes: max_file_size,
        })?;
        let retained = WalV7RetainedResources::new(ledger);
        retained.reserve_segment(
            installed.selection.header.segment_id.0,
            WalV7ResourceCharge {
                source_spool_bytes: HEADER_LEN_U64 + FRAME_LEN_U64,
                ..WalV7ResourceCharge::default()
            },
        )?;
        let owner = Arc::new(WalV8ReservationOwner::new_with_retained_resources(
            retained,
            installed,
            installed.image_len,
            max_file_size,
            allocation_unit,
        )?);
        let writer = WalV8LeaderWriter::new_with_reservation_owner(
            OpenOptions::new().read(true).write(true).open(path)?,
            sync_file,
            installed,
            max_file_size,
            Arc::clone(&owner),
        )?;
        Ok((writer, owner))
    }

    #[test]
    fn accounted_group_retains_one_marker_and_recovery_workspace() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (mut writer, owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT,
            8192,
            sync_file,
        )?;

        writer.append_group(&[batch(10), batch(20)], LIVE_WAL_V5_LIMITS)?;
        let charge = owner.image_owner().snapshot();
        assert_eq!(charge.logical_wal_bytes, 3 * FRAME_LEN_U64);
        assert_eq!(charge.source_spool_bytes, FIXED_IMAGE_TEST_BYTES + 3 * 8192);
        assert_eq!(charge.recovery_workspace_bytes, SEGMENT_LIMIT);
        assert_eq!(charge.buffer_memory_bytes, 0);
        assert_eq!(
            charge.seal_index_bytes,
            2 * std::mem::size_of::<u64>() as u64
        );
        assert_eq!(
            writer.image_len()?,
            FIXED_IMAGE_TEST_BYTES + 3 * FRAME_LEN_U64
        );
        Ok(())
    }

    const FIXED_IMAGE_TEST_BYTES: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;

    #[test]
    fn spool_pressure_rejects_before_ticket_and_preserves_workspace() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (mut writer, owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            SEGMENT_LIMIT + FIXED_IMAGE_TEST_BYTES + 2 * FRAME_LEN_U64 - 1,
            FRAME_LEN_U64,
            sync_file,
        )?;

        assert!(writer.append_batch(&batch(10), LIVE_WAL_V5_LIMITS).is_err());
        assert_eq!(writer.coordinator.next_ticket(), 0);
        assert!(!writer.is_poisoned());
        let charge = owner.image_owner().snapshot();
        assert_eq!(charge.logical_wal_bytes, 0);
        assert_eq!(charge.source_spool_bytes, FIXED_IMAGE_TEST_BYTES);
        assert_eq!(charge.recovery_workspace_bytes, SEGMENT_LIMIT);
        assert_eq!(charge.buffer_memory_bytes, 0);
        Ok(())
    }

    #[test]
    fn uncertain_sync_keeps_admitted_source_and_workspace_charged() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let installed = install_empty(&path, header())?;
        let path_c = CString::new(path.as_os_str().as_encoded_bytes())?;
        // SAFETY: path_c is NUL-terminated; ownership of the returned fd is transferred below.
        let sync_fd = unsafe { libc::open(path_c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        ensure!(
            sync_fd >= 0,
            "open O_PATH sync handle: {}",
            io::Error::last_os_error()
        );
        // SAFETY: sync_fd is a newly opened descriptor with no other owner.
        let sync_file = unsafe { File::from_raw_fd(sync_fd) };
        let (mut writer, owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT,
            FRAME_LEN_U64,
            sync_file,
        )?;

        assert!(writer.append_batch(&batch(10), LIVE_WAL_V5_LIMITS).is_err());
        assert!(writer.is_poisoned());
        let charge = owner.image_owner().snapshot();
        assert_eq!(charge.logical_wal_bytes, 2 * FRAME_LEN_U64);
        assert_eq!(
            charge.source_spool_bytes,
            FIXED_IMAGE_TEST_BYTES + 2 * FRAME_LEN_U64
        );
        assert_eq!(charge.recovery_workspace_bytes, SEGMENT_LIMIT);
        assert_eq!(charge.buffer_memory_bytes, 0);
        Ok(())
    }

    #[test]
    fn recovered_image_recharges_its_existing_source_and_logical_bytes() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (mut writer, owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT,
            FRAME_LEN_U64,
            sync_file,
        )?;
        writer.append_group(&[batch(10), batch(20)], LIVE_WAL_V5_LIMITS)?;
        let (_, boundary) = writer.active_boundaries()?;
        drop(writer);
        drop(owner);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            SEGMENT_LIMIT,
        )?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (mut writer, owner) = open_accounted_writer(
            &path,
            &recovered,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let charge = owner.image_owner().snapshot();
        assert_eq!(
            charge.logical_wal_bytes,
            recovered.image_len - FIXED_IMAGE_TEST_BYTES
        );
        assert_eq!(charge.source_spool_bytes, recovered.image_len);
        assert_eq!(charge.recovery_workspace_bytes, SEGMENT_LIMIT);
        assert_eq!(
            writer.append_batch(&batch(30), LIVE_WAL_V5_LIMITS)?.ticket,
            2
        );
        Ok(())
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
        assert!(recovered.selection.batches.is_empty());
        assert_eq!(recovered.replay_tickets.len(), 1);
        assert_eq!(replayed_batches(&path, &recovered)?, [batch(10)]);
        Ok(())
    }

    #[test]
    fn preflighted_batch_uses_sequencer_metadata_at_admission() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let mut writer = open_writer(&path, &installed)?;
        let input = batch(10);
        let prepared = writer.preflight_batch(&input, LIVE_WAL_V5_LIMITS)?;
        let recorded_at = RecordedAt {
            secs: 55,
            nanos: 123,
        };

        let (commit, _) = writer.append_prepared_batch(prepared, 20, recorded_at)?;
        assert_eq!(commit.ticket, 0);
        assert_eq!(commit.frontier.last_commit_ts, 20);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary_for(header, commit.frontier)],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        let batches = replayed_batches(&path, &recovered)?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].commit_ts, 20);
        assert_eq!(batches[0].recorded_at, recorded_at);
        assert_eq!(batches[0].entries, input.entries);
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
        #[cfg(feature = "bench")]
        {
            assert!(group.digest_metrics.payload_hash_bytes > 0);
            assert_eq!(group.digest_metrics.post_encode_hash_reread_bytes, 0);
        }
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
            replayed_batches(&path, &recovered)?,
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
    fn group_capacity_failure_preserves_writer_before_any_admission() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let installed = install_empty(&path, header())?;
        // Two DATA frames and one shared marker would fit, but the two
        // worst-case per-ticket marker reservations do not.
        let max_file_size = FIXED_IMAGE_TEST_BYTES + 3 * FRAME_LEN_U64;
        let mut writer = WalV8LeaderWriter::new(
            OpenOptions::new().read(true).write(true).open(&path)?,
            OpenOptions::new().read(true).write(true).open(&path)?,
            &installed,
            max_file_size,
        )?;
        let first = batch(10);
        let second = batch(20);
        let error = writer
            .append_group(&[first.clone(), second.clone()], LIVE_WAL_V5_LIMITS)
            .err()
            .context("group should exceed fresh-segment reservation capacity")?;
        assert_eq!(
            error.downcast_ref::<WalV8LeaderCapacityError>(),
            Some(&WalV8LeaderCapacityError::BatchTooLarge)
        );
        assert_eq!(writer.coordinator.next_ticket(), 0);
        assert_eq!(std::fs::metadata(&path)?.len(), installed.image_len);
        assert!(!writer.is_poisoned());

        // The rejected group left the writer usable. A single batch fits, and
        // the next one reports rotation rather than poisoning on admission.
        assert_eq!(writer.append_batch(&first, LIVE_WAL_V5_LIMITS)?.ticket, 0);
        let error = writer
            .append_batch(&second, LIVE_WAL_V5_LIMITS)
            .expect_err("next batch should require a fresh segment");
        assert_eq!(
            error.downcast_ref::<WalV8LeaderCapacityError>(),
            Some(&WalV8LeaderCapacityError::RotationRequired)
        );
        assert_eq!(writer.coordinator.next_ticket(), 1);
        assert!(!writer.is_poisoned());
        Ok(())
    }

    #[test]
    fn read_only_write_handle_is_rejected_before_mutation() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let before = std::fs::read(&path)?;
        let read_only_file = OpenOptions::new().read(true).open(&path)?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;

        let error =
            WalV8LeaderWriter::new(read_only_file, sync_file, &installed, MAX_WAL_FILE_SIZE)
                .err()
                .context("read-only positional write handle should be rejected")?;
        assert!(format!("{error:#}").contains("not writable"));
        assert_eq!(std::fs::read(&path)?, before);

        Ok(())
    }

    #[test]
    fn aggregate_admission_window_failure_does_not_consume_tickets() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 32 * 1024;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let mut writer = WalV8LeaderWriter::new(
            OpenOptions::new().read(true).write(true).open(&path)?,
            OpenOptions::new().read(true).write(true).open(&path)?,
            &installed,
            SEGMENT_LIMIT,
        )?;
        let batches = (1..=5)
            .map(|ticket| WalBatch {
                commit_ts: ticket * 10,
                recorded_at: RecordedAt {
                    secs: i64::try_from(ticket).unwrap_or(i64::MAX),
                    nanos: 17,
                },
                entries: vec![WalEntry::Put {
                    key: vec![u8::try_from(ticket).unwrap_or(u8::MAX)],
                    value: vec![0x5a; 3800],
                }],
            })
            .collect::<Vec<_>>();

        let error = writer
            .append_group(&batches, LIVE_WAL_V5_LIMITS)
            .err()
            .context("aggregate memory reservation exceeds the admission window")?;
        assert!(matches!(
            error.downcast_ref::<WalV8LeaderCapacityError>(),
            Some(WalV8LeaderCapacityError::GroupExceedsAdmissionWindow)
        ));
        assert_eq!(writer.coordinator.next_ticket(), 0);
        assert!(!writer.is_poisoned());
        assert_eq!(std::fs::metadata(&path)?.len(), installed.image_len);
        Ok(())
    }

    #[test]
    fn preparation_indexes_count_toward_the_admission_window() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 32 * 1024;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let mut writer = WalV8LeaderWriter::new(
            OpenOptions::new().read(true).write(true).open(&path)?,
            OpenOptions::new().read(true).write(true).open(&path)?,
            &installed,
            SEGMENT_LIMIT,
        )?;
        let batches = (1..=5)
            .map(|ticket| WalBatch {
                commit_ts: ticket * 10,
                recorded_at: RecordedAt {
                    secs: i64::try_from(ticket).unwrap_or(i64::MAX),
                    nanos: 17,
                },
                entries: (0..100)
                    .map(|key| WalEntry::Put {
                        key: vec![u8::try_from(key).unwrap_or(u8::MAX)],
                        value: vec![0x5a],
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        let error = writer
            .append_group(&batches, LIVE_WAL_V5_LIMITS)
            .err()
            .context("preparation indexes plus output buffers exceed the admission window")?;
        assert!(matches!(
            error.downcast_ref::<WalV8LeaderCapacityError>(),
            Some(WalV8LeaderCapacityError::GroupExceedsAdmissionWindow)
        ));
        assert_eq!(writer.coordinator.next_ticket(), 0);
        assert!(!writer.is_poisoned());
        assert_eq!(std::fs::metadata(&path)?.len(), installed.image_len);
        let retried = writer.append_group(&[batch(10)], LIVE_WAL_V5_LIMITS)?;
        assert_eq!(retried.commits.len(), 1);
        assert_eq!(retried.commits[0].ticket, 0);
        assert!(!writer.is_poisoned());
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
        drop(writer);

        let original_inode = std::fs::metadata(&path)?.ino();
        let recovered = super::super::install::recover_and_install_active(
            &path,
            std::slice::from_ref(&installed.active_boundary),
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 1);
        assert_eq!(replayed_batches(&path, &recovered)?, [batch(10)]);
        assert_ne!(std::fs::metadata(&path)?.ino(), original_inode);
        Ok(())
    }

    #[test]
    fn explicit_sync_failure_poisons_parallel_writer() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let write_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let path_c = CString::new(path.as_os_str().as_encoded_bytes())?;
        // SAFETY: O_PATH returns a descriptor owned below.
        let sync_fd = unsafe { libc::open(path_c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        ensure!(
            sync_fd >= 0,
            "open O_PATH sync handle: {}",
            io::Error::last_os_error()
        );
        // SAFETY: sync_fd was returned by open and ownership transfers to File.
        let sync_file = unsafe { File::from_raw_fd(sync_fd) };
        let writer = WalV8LeaderWriter::new(write_file, sync_file, &installed, MAX_WAL_FILE_SIZE)?
            .into_parallel();
        let first_batch = batch(10);
        let prepared = writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?;
        let mut data_buffer = DirectBuf::new(prepared.frame_buffer_bytes());
        let mut reservation = None;
        let publisher = WalV8LeaderAdmissionPublisher {
            coordinator: &writer.coordinator,
            reservation: RefCell::new(&mut reservation),
        };
        let admitted = prepared.admit_with_metadata(10, first_batch.recorded_at, &publisher)?;
        let mut encoded = admitted.encode()?;
        let assignment = encoded.assignment();
        let digest_ready = encoded.digest_ready()?;
        writer.coordinator.mark_digest_ready(digest_ready)?;
        let data_len = encoded.encode_data_frames_into(data_buffer.as_mut_slice())?;
        ensure!(
            data_len == data_buffer.cap(),
            "v8 DATA materializer returned an unexpected frame length"
        );
        data_buffer.set_len(data_len);
        writer
            .coordinator
            .mark_frames_ready(assignment.segment_ticket)?;
        write_aligned_once(
            &writer.write_file,
            data_buffer.initialized_slice(0, data_len),
            assignment.first_data_offset,
        )?;
        writer
            .coordinator
            .mark_data_complete(assignment.segment_ticket)?;
        drop(data_buffer);

        let error = writer
            .sync()
            .expect_err("explicit fdatasync failure must reject the durability advance");
        assert!(format!("{error:#}").contains("fdatasync"));
        assert!(writer.is_poisoned());
        assert!(
            writer
                .preflight_batch(&batch(20), LIVE_WAL_V5_LIMITS)
                .is_err()
        );
        drop(encoded);
        Ok(())
    }

    #[test]
    fn concurrent_producers_share_one_frontier_after_admitted_head_finishes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = Arc::new(open_writer(&path, &installed)?.into_parallel());
        let first_batch = batch(10);
        let second_batch = batch(20);
        let first_prepared = writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?;
        let second_prepared = writer.preflight_batch(&second_batch, LIVE_WAL_V5_LIMITS)?;
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let (first_commit, second_commit) = thread::scope(|scope| -> Result<_> {
            let first_writer = Arc::clone(&writer);
            let first_handle = scope.spawn(move || {
                first_writer.append_prepared_batch_inner(
                    first_prepared,
                    10,
                    first_batch.recorded_at,
                    || {
                        admitted_tx
                            .send(())
                            .expect("admission receiver remains open");
                        release_rx.recv().expect("test releases the first producer");
                    },
                )
            });
            admitted_rx.recv_timeout(Duration::from_secs(2))?;

            let second_writer = Arc::clone(&writer);
            let second_handle = scope.spawn(move || {
                second_writer.append_prepared_batch(second_prepared, 20, second_batch.recorded_at)
            });

            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let admitted = writer.coordinator.admitted_window();
                if admitted.len() == 2 && admitted[1].data_complete {
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "later v8 producer did not complete while the head was preparing"
                );
                thread::yield_now();
            }
            release_tx
                .send(())
                .expect("first producer is still waiting");

            let first_commit = first_handle
                .join()
                .map_err(|_| anyhow::anyhow!("first v8 producer panicked"))??;
            let second_commit = second_handle
                .join()
                .map_err(|_| anyhow::anyhow!("second v8 producer panicked"))??;
            Ok((first_commit, second_commit))
        })?;
        assert_eq!(first_commit.0.ticket, 0);
        assert_eq!(second_commit.0.ticket, 1);
        assert_eq!(writer.active_boundaries()?.1.ticket_end, 2);
        assert_eq!(
            writer.image_len()?,
            installed.image_len + 3 * FRAME_LEN_U64,
            "two DATA frames and one shared FRONTIER are durable"
        );

        drop(writer);
        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[installed.active_boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 2);
        assert_eq!(replayed_batches(&path, &recovered)?, [batch(10), batch(20)]);
        Ok(())
    }
}
