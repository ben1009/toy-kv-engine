//! V8 WAL writer with Leader and Parallel submission under RFC 026.
//!
//! Parallel is the default; Leader remains an explicit comparison path. Both
//! modes use admission-before-encoding, DATA/FRONTIER frames and one successful
//! WAL sync before durable publication. Parallel submits DATA through the shared
//! I/O worker and folds only fixed-size digest links in ticket order.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsStr,
    fmt,
    fs::File,
    io,
    ops::{Deref, DerefMut},
    os::unix::{
        fs::{FileExt, MetadataExt},
        io::AsRawFd,
    },
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(feature = "bench")]
use std::sync::atomic::{AtomicU64, AtomicUsize};

#[cfg(test)]
use std::os::unix::fs::OpenOptionsExt;

use crate::wal::mutex_diagnostics::{self, MutexLockKind};
use anyhow::{Context, Result, ensure};
use crossbeam_channel::{Receiver, Sender};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Condvar, Mutex};
use sha2::{Digest, Sha256};

use crate::{
    pitr::{LIVE_WAL_V5_LIMITS, WalBatch, WalV5Limits},
    wal::{
        DirectBuf, DirectBufAllocation, MAX_WAL_FILE_SIZE,
        parallel_worker::{
            GroupWriteResult, IoWorker, IoWorkerClient, WorkerBuffer, WriteBuffer, WriteGroup,
            WriteGroupBuffers,
        },
        v7::admission::{
            WalV7OwnedBatchReservation, WalV7OwnedPreparationReservation, WalV7ReservationError,
        },
    },
};

#[cfg(feature = "bench")]
use crate::wal::{
    WalExtentPreparationDiagnostics, WalExtentPreparationMode, WalIoDiagnostics,
    WalIoHandleMetadata, WalV8SlidingExtentPreparationDiagnostics,
};

#[cfg(feature = "bench")]
use crate::wal::parallel_worker::DirectGroupWriteCompletion;

#[cfg(feature = "bench")]
use super::parallel::WalV8ProgressAdvanceTiming;
#[cfg(feature = "bench")]
use super::parallel::{WalV8ReadyPrefix, WalV8TicketProgress};

use super::{
    WAL_V8_HEADER_LEN,
    codec::{
        WAL_V8_FRAME_LEN, WalV8Frontier, WalV8Header, decode_frame_structural,
        encode_frontier_successor, frame_record_digest,
    },
    digest,
    install::WalV8InstalledRecovery,
    parallel::{
        MAX_PARALLEL_INFLIGHT_TICKETS, WalV8CompletedGroupEvent, WalV8DurabilityDriver,
        WalV8FrontierReadySnapshot, WalV8ParallelAdmissionGuard, WalV8ParallelConfig,
        WalV8ParallelCoordinator, WalV8PreparationReservation, WalV8ProgressMode,
        WalV8ProgressTokenWait, consume_progress_token_before_scan, wait_for_progress_token,
    },
    prepare::{
        AdmittedWalV8Batch, EncodedWalV8Batch, PreparedWalV8Batch, WalV8AdmissionPlan,
        WalV8AdmissionPublisher, WalV8BatchAssignment, WalV8DigestReady, WalV8WriteClass,
    },
    resource::WalV8ReservationOwner,
};

#[cfg(feature = "bench")]
use super::parallel::WalV8DirectCompletionWakeMode;

const FRAME_LEN_U64: u64 = WAL_V8_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = WAL_V8_HEADER_LEN as u64;
const MAX_GROUP_TICKETS: usize = 8;
#[cfg(feature = "bench")]
const MAX_POINT_BUFFER_PREALLOCATIONS: usize = 16;
const DATA_GROUP_JOIN_BUDGET: Duration = Duration::from_micros(10);
const WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES: u64 = 1 << 20;
const WAL_V8_EXTENT_LOOKAHEAD_ENV: &str = "TOYKV_WAL_V8_EXTENT_LOOKAHEAD";

#[cfg(feature = "bench")]
fn data_completion_to_frontier_start_ns(
    tickets: &[WalV8TicketProgress],
    frontier_write_started: Option<Instant>,
) -> Option<u64> {
    let frontier_write_started = frontier_write_started?;
    let mut latest_completion = None;
    for ticket in tickets {
        let completed_at = ticket.data_completed_at?;
        latest_completion = Some(
            latest_completion.map_or(completed_at, |latest: Instant| latest.max(completed_at)),
        );
    }

    let wait = frontier_write_started.checked_duration_since(latest_completion?)?;

    Some(wait.as_nanos().min(u64::MAX as u128) as u64)
}

pub(crate) type WalV8PreparedBatch<'a> = PreparedWalV8Batch<'a, WalV8PreflightReservation>;

/// Point-owned preparation state created before entering the MVCC sequencer.
/// The optional DATA buffer is uncharged and bounded; serialized admission
/// still performs the resource reservation in ticket order.
#[cfg(feature = "bench")]
pub(crate) struct WalV8PointAdmissionScratch<'a> {
    prepared: WalV8PreparedBatch<'a>,
    preallocated_buffer: Option<WalV8PointBufferPreallocation>,
}

/// A bounded speculative DATA buffer allocation. It has no quota reservation;
/// the normal serialized admission path charges quota before consuming it.
#[cfg(feature = "bench")]
pub(crate) struct WalV8PointBufferPreallocation {
    data_buffer: DirectBufAllocation,
    _permit: WalV8PointBufferPreallocationPermit,
}

#[cfg(feature = "bench")]
struct WalV8PointBufferPreallocationPermit {
    in_flight: Arc<AtomicUsize>,
}

#[cfg(feature = "bench")]
impl Drop for WalV8PointBufferPreallocationPermit {
    fn drop(&mut self) {
        let previous = self.in_flight.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "point buffer preallocation permit underflow");
    }
}

#[cfg(feature = "bench")]
impl WalV8PointBufferPreallocationPermit {
    fn try_acquire(in_flight: &Arc<AtomicUsize>) -> Option<Self> {
        in_flight
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |count| {
                (count < MAX_POINT_BUFFER_PREALLOCATIONS).then_some(count + 1)
            })
            .ok()?;

        Some(Self {
            in_flight: Arc::clone(in_flight),
        })
    }
}

#[cfg(feature = "bench")]
impl<'a> WalV8PointAdmissionScratch<'a> {
    pub(crate) fn new(prepared: WalV8PreparedBatch<'a>) -> Self {
        Self {
            prepared,
            preallocated_buffer: None,
        }
    }

    pub(crate) fn into_prepared(self) -> WalV8PreparedBatch<'a> {
        self.prepared
    }

    fn set_preallocated_buffer(&mut self, preallocated_buffer: WalV8PointBufferPreallocation) {
        debug_assert!(self.preallocated_buffer.is_none());
        self.preallocated_buffer = Some(preallocated_buffer);
    }
}

struct WalV8AdmissionResources {
    data_buffer: DirectBufAllocation,
    reservation: Option<WalV7OwnedBatchReservation>,
}

#[cfg(feature = "bench")]
impl WalV8PointBufferPreallocation {
    fn into_data_buffer(self) -> DirectBufAllocation {
        let Self {
            data_buffer,
            _permit,
        } = self;
        drop(_permit);
        data_buffer
    }
}

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
    io_diagnostics: WalIoDiagnostics,
    #[cfg(feature = "bench")]
    initial_ticket: u64,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
}

/// Shared v8 writer path for concurrent producers. Admission and DATA writes
/// can overlap across callers; this mutex protects only the one-at-a-time
/// FRONTIER write and sync cycle.
pub(crate) struct WalV8ParallelWriter {
    frontier_file: File,
    sync_file: File,
    header: WalV8Header,
    physical_header_digest: [u8; 32],
    initial_prefix_digest: digest::WalV8Digest,
    coordinator: WalV8ParallelCoordinator,
    io: Arc<WalV8ParallelIo>,
    reservation_owner: Option<Arc<WalV8ReservationOwner>>,
    frontier: Mutex<WalV8ParallelFrontier>,
    frontier_resources: Mutex<BTreeMap<u64, WalV8FrontierResource>>,
    producer_guards: Arc<Mutex<BTreeMap<u64, EncodedWalV8Batch<WalV8ParallelAdmissionGuard>>>>,
    max_file_size: u64,
    installed_dev: u64,
    installed_ino: u64,
    #[cfg(feature = "bench")]
    initial_ticket: u64,
    #[cfg(feature = "bench")]
    io_diagnostics: WalIoDiagnostics,
    /// Benchmark-only budget for eligible coalescing calls, independent of
    /// detailed sync-observation capture.
    #[cfg(feature = "bench")]
    coalesce_budget_override_us: Option<u64>,
    /// Benchmark-only experiment that batches credit release for Point-only
    /// durable prefixes. Batch and mixed prefixes retain the existing path.
    #[cfg(feature = "bench")]
    batch_point_frontier_credits: bool,
    #[cfg(feature = "bench")]
    point_buffer_preallocation_enabled: bool,
    #[cfg(all(test, feature = "bench"))]
    point_buffer_preallocation_test_override: AtomicBool,
    #[cfg(feature = "bench")]
    point_buffer_preallocation_attempts: AtomicU64,
    #[cfg(feature = "bench")]
    point_buffer_preallocation_hits: AtomicU64,
    #[cfg(feature = "bench")]
    point_buffer_preallocation_fallbacks: AtomicU64,
    #[cfg(feature = "bench")]
    point_buffer_preallocations_in_flight: Arc<AtomicUsize>,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
}

struct WalV8FrontierResource {
    reservation: WalV7OwnedBatchReservation,
    #[cfg(feature = "bench")]
    write_class: WalV8WriteClass,
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Default)]
struct WalV8CursorCoalescingTiming {
    progress_wait_wall_ns: u64,
    progress_wait_cpu_ns: u64,
    completion_drain_wall_ns: u64,
    completion_drain_cpu_ns: u64,
    iterations: u64,
    ready_tickets_gained: u64,
    advance: WalV8ProgressAdvanceTiming,
}

#[cfg(feature = "bench")]
struct WalV8DurabilityPhaseSampleGuard {
    profile: Arc<crate::mem_table::WriteProfile>,
    recorded: bool,
}

#[cfg(feature = "bench")]
impl WalV8DurabilityPhaseSampleGuard {
    fn new(profile: Arc<crate::mem_table::WriteProfile>) -> Self {
        Self {
            profile,
            recorded: false,
        }
    }

    fn record(mut self, observation: crate::mem_table::WalV8DurabilityPhaseObservation) {
        self.profile.record_wal_v8_durability_phase(observation);
        self.recorded = true;
    }
}

#[cfg(feature = "bench")]
impl Drop for WalV8DurabilityPhaseSampleGuard {
    fn drop(&mut self) {
        if !self.recorded {
            self.profile.drop_wal_v8_durability_phase_sample();
        }
    }
}

#[cfg(feature = "bench")]
fn coalescing_active_duration_ns(
    total_ns: u64,
    detailed_wait_ns: Option<u64>,
    coalescing_attempted: bool,
) -> Option<u64> {
    match detailed_wait_ns {
        Some(wait_ns) => Some(total_ns.saturating_sub(wait_ns)),
        None if !coalescing_attempted => Some(0),
        None => None,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct WalV8RetirementObservation {
    #[cfg(feature = "bench")]
    wall_ns: u64,
    #[cfg(feature = "bench")]
    cpu_ns: u64,
    #[cfg(feature = "bench")]
    lock_wait_wall_ns: u64,
    #[cfg(feature = "bench")]
    lock_wait_cpu_ns: u64,
    #[cfg(feature = "bench")]
    lock_hold_wall_ns: u64,
    #[cfg(feature = "bench")]
    lock_hold_cpu_ns: u64,
    #[cfg(feature = "bench")]
    tickets: u64,
    #[cfg(feature = "bench")]
    collect_cpu_ns: u64,
    #[cfg(feature = "bench")]
    remove_cpu_ns: u64,
    #[cfg(feature = "bench")]
    release_cpu_ns: u64,
    #[cfg(feature = "bench")]
    finish_cpu_ns: u64,
    #[cfg(feature = "bench")]
    batch_point_frontier_credits: bool,
}

#[cfg(feature = "bench")]
struct WalV8SlidingExtentPreparation {
    data_file: File,
    start_offset: u64,
    max_file_size: u64,
    window_bytes: u64,
    mode: WalExtentPreparationMode,
    state: Arc<Mutex<WalV8SlidingExtentPreparationState>>,
    prepared: Arc<Condvar>,
    request_sender: Option<mpsc::Sender<u64>>,
    worker: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "bench")]
struct WalV8SlidingExtentPreparationState {
    prepared_end: u64,
    allocated_end: u64,
    requested_end: u64,
    fallocate_calls: u64,
    allocated_bytes: u64,
    initialization_chunks: u64,
    initialized_bytes: u64,
    elapsed_ns: u64,
    fallocate_elapsed_ns: u64,
    initialization_elapsed_ns: u64,
    wait_calls: u64,
    wait_ns: u64,
    failure: Option<String>,
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalV8FrontierWriteMode {
    Buffered,
    Direct,
}

#[cfg(feature = "bench")]
impl WalV8FrontierWriteMode {
    fn from_environment() -> Result<Self> {
        Self::from_value(std::env::var_os("TOYKV_WAL_V8_FRONTIER_MODE").as_deref())
    }

    fn from_value(value: Option<&OsStr>) -> Result<Self> {
        match value.and_then(OsStr::to_str) {
            None | Some("buffered") => Ok(Self::Buffered),
            Some("direct") => Ok(Self::Direct),
            Some(value) => anyhow::bail!(
                "invalid TOYKV_WAL_V8_FRONTIER_MODE {value:?}; expected `buffered` or `direct`"
            ),
        }
    }
}

#[cfg(feature = "bench")]
const WAL_V8_EXTENT_PREPARATION_ENV: &str = "TOYKV_BENCH_V8_EXTENT_PREP";
#[cfg(feature = "bench")]
const WAL_V8_EXTENT_PREPARATION_BYTES_ENV: &str = "TOYKV_BENCH_V8_EXTENT_PREP_BYTES";
#[cfg(feature = "bench")]
const WAL_V8_POINT_FRONTIER_CREDIT_BATCH_ENV: &str = "TOYKV_BENCH_V8_POINT_FRONTIER_CREDIT_BATCH";
#[cfg(feature = "bench")]
const WAL_V8_POINT_PROGRESS_RING_ENV: &str = "TOYKV_BENCH_V8_POINT_PROGRESS_RING";
#[cfg(feature = "bench")]
const WAL_V8_POINT_PROGRESS_COUNTERS_ENV: &str = "TOYKV_BENCH_V8_PROGRESS_COUNTERS";
#[cfg(feature = "bench")]
const WAL_V8_CURSOR_DIRECT_COMPLETION_ENV: &str = "TOYKV_BENCH_V8_CURSOR_DIRECT_COMPLETION";
#[cfg(feature = "bench")]
const WAL_V8_DIRECT_COMPLETION_WAKE_ENV: &str = "TOYKV_BENCH_V8_DIRECT_COMPLETION_WAKE";
#[cfg(feature = "bench")]
const WAL_V8_CURSOR_SINGLETON_SUBMISSION_ENV: &str = "TOYKV_BENCH_V8_CURSOR_SINGLETON_SUBMISSION";
#[cfg(not(feature = "bench"))]
const WAL_V8_PROGRESS_MODE_ENV: &str = "TOYKV_WAL_V8_PROGRESS_MODE";
const WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES: usize = 128 * 1024;

#[cfg(not(feature = "bench"))]
fn production_progress_mode_from_value(value: Option<&OsStr>) -> Result<WalV8ProgressMode> {
    match value {
        None => Ok(WalV8ProgressMode::Eager),
        Some(value) => match value.to_str() {
            Some("eager") => Ok(WalV8ProgressMode::Eager),
            Some("cursor") => Ok(WalV8ProgressMode::Cursor),
            Some(value) => anyhow::bail!(
                "invalid {WAL_V8_PROGRESS_MODE_ENV} {value:?}; expected `eager` or `cursor`"
            ),
            None => anyhow::bail!(
                "{WAL_V8_PROGRESS_MODE_ENV} is not valid UTF-8; expected `eager` or `cursor`"
            ),
        },
    }
}

#[cfg(not(feature = "bench"))]
fn production_progress_mode_from_environment() -> Result<WalV8ProgressMode> {
    production_progress_mode_from_value(std::env::var_os(WAL_V8_PROGRESS_MODE_ENV).as_deref())
}

#[cfg(feature = "bench")]
fn progress_mode_from_value(value: Option<&OsStr>) -> Result<WalV8ProgressMode> {
    let Some(value) = value else {
        return Ok(WalV8ProgressMode::Eager);
    };
    match value.to_str() {
        Some("0") => Ok(WalV8ProgressMode::Eager),
        Some("2") => Ok(WalV8ProgressMode::Cursor),
        Some(value) => {
            anyhow::bail!("invalid {WAL_V8_POINT_PROGRESS_RING_ENV} {value:?}; expected `0` or `2`")
        }
        None => anyhow::bail!(
            "{WAL_V8_POINT_PROGRESS_RING_ENV} is not valid UTF-8; expected `0` or `2`"
        ),
    }
}

#[cfg(feature = "bench")]
fn progress_mode_from_environment() -> Result<WalV8ProgressMode> {
    progress_mode_from_value(std::env::var_os(WAL_V8_POINT_PROGRESS_RING_ENV).as_deref())
}

#[cfg(feature = "bench")]
fn progress_counters_enabled_from_value(
    value: Option<&OsStr>,
    progress_mode: WalV8ProgressMode,
) -> Result<bool> {
    let enabled = match value {
        None => false,
        Some(value) => match value.to_str() {
            Some("0") => false,
            Some("1") => true,
            Some(value) => anyhow::bail!(
                "invalid {WAL_V8_POINT_PROGRESS_COUNTERS_ENV} {value:?}; expected `0` or `1`"
            ),
            None => anyhow::bail!(
                "{WAL_V8_POINT_PROGRESS_COUNTERS_ENV} is not valid UTF-8; expected `0` or `1`"
            ),
        },
    };
    ensure!(
        !enabled || progress_mode == WalV8ProgressMode::Cursor,
        "{WAL_V8_POINT_PROGRESS_COUNTERS_ENV}=1 requires Cursor progress mode"
    );
    Ok(enabled)
}

#[cfg(feature = "bench")]
fn progress_counters_enabled_from_environment(progress_mode: WalV8ProgressMode) -> Result<bool> {
    progress_counters_enabled_from_value(
        std::env::var_os(WAL_V8_POINT_PROGRESS_COUNTERS_ENV).as_deref(),
        progress_mode,
    )
}

#[cfg(feature = "bench")]
fn cursor_direct_completion_enabled_from_value(
    value: Option<&OsStr>,
    progress_mode: WalV8ProgressMode,
) -> Result<bool> {
    let enabled = match value {
        None => false,
        Some(value) if value.to_str() == Some("0") => false,
        Some(value) if value.to_str() == Some("1") => true,
        Some(value) => anyhow::bail!(
            "invalid {WAL_V8_CURSOR_DIRECT_COMPLETION_ENV} {value:?}; expected `0` or `1`"
        ),
    };
    ensure!(
        !enabled || progress_mode == WalV8ProgressMode::Cursor,
        "{WAL_V8_CURSOR_DIRECT_COMPLETION_ENV}=1 requires Cursor progress mode"
    );
    Ok(enabled)
}

#[cfg(feature = "bench")]
fn cursor_direct_completion_enabled_from_environment(
    progress_mode: WalV8ProgressMode,
) -> Result<bool> {
    cursor_direct_completion_enabled_from_value(
        std::env::var_os(WAL_V8_CURSOR_DIRECT_COMPLETION_ENV).as_deref(),
        progress_mode,
    )
}

#[cfg(feature = "bench")]
fn direct_completion_wake_mode_from_value(
    value: Option<&OsStr>,
    progress_mode: WalV8ProgressMode,
    direct_completion_enabled: bool,
) -> Result<WalV8DirectCompletionWakeMode> {
    let mode = match value {
        None => WalV8DirectCompletionWakeMode::Token,
        Some(value) if value.to_str() == Some("token") => WalV8DirectCompletionWakeMode::Token,
        Some(value) if value.to_str() == Some("select") => WalV8DirectCompletionWakeMode::Select,
        Some(value) => anyhow::bail!(
            "invalid {WAL_V8_DIRECT_COMPLETION_WAKE_ENV} {value:?}; expected `token` or `select`"
        ),
    };
    ensure!(
        mode != WalV8DirectCompletionWakeMode::Select
            || (progress_mode == WalV8ProgressMode::Cursor && direct_completion_enabled),
        "{WAL_V8_DIRECT_COMPLETION_WAKE_ENV}=select requires Cursor progress mode and direct completion"
    );
    Ok(mode)
}

#[cfg(feature = "bench")]
fn direct_completion_wake_mode_from_environment(
    progress_mode: WalV8ProgressMode,
    direct_completion_enabled: bool,
) -> Result<WalV8DirectCompletionWakeMode> {
    direct_completion_wake_mode_from_value(
        std::env::var_os(WAL_V8_DIRECT_COMPLETION_WAKE_ENV).as_deref(),
        progress_mode,
        direct_completion_enabled,
    )
}

#[cfg(feature = "bench")]
fn cursor_singleton_submission_enabled_from_value(
    value: Option<&OsStr>,
    progress_mode: WalV8ProgressMode,
) -> Result<bool> {
    let enabled = match value {
        None => false,
        Some(value) if value.to_str() == Some("0") => false,
        Some(value) if value.to_str() == Some("1") => true,
        Some(value) => anyhow::bail!(
            "invalid {WAL_V8_CURSOR_SINGLETON_SUBMISSION_ENV} {value:?}; expected `0` or `1`"
        ),
    };
    ensure!(
        !enabled || progress_mode == WalV8ProgressMode::Cursor,
        "{WAL_V8_CURSOR_SINGLETON_SUBMISSION_ENV}=1 requires Cursor progress mode"
    );
    Ok(enabled)
}

#[cfg(feature = "bench")]
fn cursor_singleton_submission_enabled_from_environment(
    progress_mode: WalV8ProgressMode,
) -> Result<bool> {
    cursor_singleton_submission_enabled_from_value(
        std::env::var_os(WAL_V8_CURSOR_SINGLETON_SUBMISSION_ENV).as_deref(),
        progress_mode,
    )
}

#[cfg(feature = "bench")]
pub(crate) fn progress_ring_enabled_from_environment() -> Result<bool> {
    Ok(progress_mode_from_environment()?.uses_ring())
}

#[cfg(feature = "bench")]
impl WalExtentPreparationMode {
    fn from_environment() -> Result<Option<(Self, u64)>> {
        let mode = std::env::var_os(WAL_V8_EXTENT_PREPARATION_ENV);
        let bytes = std::env::var_os(WAL_V8_EXTENT_PREPARATION_BYTES_ENV);
        Self::from_values(mode.as_deref(), bytes.as_deref())
    }

    fn from_values(mode: Option<&OsStr>, bytes: Option<&OsStr>) -> Result<Option<(Self, u64)>> {
        let Some(mode) = mode else {
            ensure!(
                bytes.is_none(),
                "{WAL_V8_EXTENT_PREPARATION_BYTES_ENV} requires {WAL_V8_EXTENT_PREPARATION_ENV}"
            );
            return Ok(None);
        };
        let mode = match mode.to_str() {
            Some("fallocate") => Self::Fallocate,
            Some("initialize") => Self::Initialize,
            Some("sliding-fallocate") => Self::SlidingFallocate,
            Some("sliding-initialize") => Self::SlidingInitialize,
            Some("sliding-lookahead") => Self::SlidingLookahead,
            Some("sliding-point-lookahead") => Self::SlidingPointLookahead,
            Some(value) => anyhow::bail!(
                "invalid {WAL_V8_EXTENT_PREPARATION_ENV} {value:?}; expected `fallocate`, `initialize`, `sliding-fallocate`, `sliding-initialize`, `sliding-lookahead`, or `sliding-point-lookahead`"
            ),
            None => anyhow::bail!("{WAL_V8_EXTENT_PREPARATION_ENV} is not valid UTF-8"),
        };
        let bytes = bytes.context(format!("{WAL_V8_EXTENT_PREPARATION_BYTES_ENV} is required"))?;
        let bytes = bytes
            .to_str()
            .context("V8 extent preparation byte count is not valid UTF-8")?
            .parse::<u64>()
            .context("V8 extent preparation byte count must be an unsigned integer")?;
        ensure!(
            bytes > 0 && bytes.is_multiple_of(WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64),
            "V8 extent preparation bytes must be a positive multiple of {}",
            WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES
        );

        Ok(Some((mode, bytes)))
    }

    const fn is_sliding(self) -> bool {
        matches!(
            self,
            Self::SlidingFallocate
                | Self::SlidingInitialize
                | Self::SlidingLookahead
                | Self::SlidingPointLookahead
        )
    }
}

fn wal_v8_extent_lookahead_from_value(value: Option<&OsStr>) -> Result<bool> {
    match value.and_then(OsStr::to_str) {
        None | Some("0" | "false") => Ok(false),
        Some("1" | "true") => Ok(true),
        Some(value) => anyhow::bail!(
            "invalid {WAL_V8_EXTENT_LOOKAHEAD_ENV} {value:?}; expected `0`, `1`, `false`, or `true`"
        ),
    }
}

fn wal_v8_extent_lookahead_from_environment() -> Result<bool> {
    wal_v8_extent_lookahead_from_value(std::env::var_os(WAL_V8_EXTENT_LOOKAHEAD_ENV).as_deref())
}

#[cfg(feature = "bench")]
fn prepare_v8_extent_suffix(
    data_file: &File,
    sync_file: &File,
    start_offset: u64,
    max_file_size: u64,
    mode: WalExtentPreparationMode,
    bytes: u64,
) -> Result<WalExtentPreparationDiagnostics> {
    ensure!(
        !mode.is_sliding(),
        "sliding V8 extent preparation cannot use one-shot suffix preparation"
    );
    ensure!(
        WalIoHandleMetadata::from_file(data_file)?.o_direct == Some(true),
        "V8 extent preparation requires an O_DIRECT DATA worker"
    );
    let before = data_file
        .metadata()
        .context("inspect V8 WAL before extent preparation")?;
    let sync_metadata = sync_file
        .metadata()
        .context("inspect V8 WAL sync handle before extent preparation")?;
    ensure!(
        before.is_file()
            && sync_metadata.is_file()
            && before.dev() == sync_metadata.dev()
            && before.ino() == sync_metadata.ino()
            && before.len() == start_offset,
        "V8 extent preparation requires the untouched installed Active WAL inode"
    );
    let end_offset = start_offset
        .checked_add(bytes)
        .context("V8 extent preparation range overflows")?;
    ensure!(
        end_offset <= max_file_size,
        "V8 extent preparation exceeds the configured WAL segment size"
    );

    let started_at = Instant::now();
    let allocated_bytes_before = before.blocks().saturating_mul(512);
    let offset = libc::off_t::try_from(start_offset)
        .context("V8 extent preparation offset exceeds off_t")?;
    let len = libc::off_t::try_from(bytes).context("V8 extent preparation length exceeds off_t")?;
    // SAFETY: both values are checked, and the owned descriptor remains live.
    let result = unsafe { libc::fallocate(data_file.as_raw_fd(), 0, offset, len) };
    ensure!(
        result == 0,
        "V8 benchmark fallocate failed: {}",
        io::Error::last_os_error()
    );

    let initialized_bytes = if mode == WalExtentPreparationMode::Initialize {
        let chunk_bytes = WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES;
        let mut zeros = DirectBuf::new(chunk_bytes);
        zeros.zero_range(0, chunk_bytes);
        let mut current_offset = start_offset;
        while current_offset < end_offset {
            data_file
                .write_all_at(zeros.initialized_slice(0, chunk_bytes), current_offset)
                .with_context(|| format!("initialize V8 WAL extent at offset {current_offset}"))?;
            current_offset += chunk_bytes as u64;
        }
        bytes
    } else {
        0
    };
    sync_file
        .sync_data()
        .context("sync V8 WAL extent preparation before benchmark")?;

    let after = data_file
        .metadata()
        .context("inspect V8 WAL after extent preparation")?;
    ensure!(
        after.dev() == before.dev() && after.ino() == before.ino() && after.len() == end_offset,
        "V8 extent preparation did not preserve the WAL inode and expected range"
    );
    let allocated_bytes = after
        .blocks()
        .saturating_mul(512)
        .saturating_sub(allocated_bytes_before);
    ensure!(
        allocated_bytes >= bytes,
        "V8 fallocate reported fewer allocated bytes than the requested suffix"
    );

    Ok(WalExtentPreparationDiagnostics {
        mode,
        start_offset,
        end_offset,
        allocated_bytes,
        initialized_bytes,
        elapsed_ns: u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX),
    })
}

#[cfg(feature = "bench")]
impl WalV8SlidingExtentPreparation {
    fn new(
        data_file: File,
        sync_file: &File,
        start_offset: u64,
        max_file_size: u64,
        window_bytes: u64,
        mode: WalExtentPreparationMode,
    ) -> Result<Self> {
        ensure!(
            mode.is_sliding(),
            "sliding V8 extent preparation requires a sliding mode"
        );
        ensure!(
            WalIoHandleMetadata::from_file(&data_file)?.o_direct == Some(true),
            "sliding V8 extent preparation requires an O_DIRECT DATA worker"
        );
        let data_metadata = data_file
            .metadata()
            .context("inspect V8 WAL before sliding extent preparation")?;
        let sync_metadata = sync_file
            .metadata()
            .context("inspect V8 WAL sync handle before sliding extent preparation")?;
        ensure!(
            data_metadata.is_file()
                && sync_metadata.is_file()
                && data_metadata.dev() == sync_metadata.dev()
                && data_metadata.ino() == sync_metadata.ino()
                && data_metadata.len() == start_offset,
            "sliding V8 extent preparation requires the untouched installed Active WAL inode"
        );
        ensure!(
            window_bytes > 0
                && window_bytes.is_multiple_of(WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64),
            "sliding V8 extent preparation window is not aligned"
        );
        ensure!(
            start_offset <= max_file_size,
            "sliding V8 extent preparation starts beyond the WAL segment limit"
        );

        let state = Arc::new(Mutex::new(WalV8SlidingExtentPreparationState {
            prepared_end: start_offset,
            allocated_end: start_offset,
            requested_end: start_offset,
            fallocate_calls: 0,
            allocated_bytes: 0,
            initialization_chunks: 0,
            initialized_bytes: 0,
            elapsed_ns: 0,
            fallocate_elapsed_ns: 0,
            initialization_elapsed_ns: 0,
            wait_calls: 0,
            wait_ns: 0,
            failure: None,
        }));
        let prepared = Arc::new(Condvar::new());
        let (request_sender, worker) = if mode == WalExtentPreparationMode::SlidingInitialize {
            let worker_file = data_file
                .try_clone()
                .context("clone V8 WAL handle for background extent initialization")?;
            let worker_state = Arc::clone(&state);
            let worker_prepared = Arc::clone(&prepared);
            let run_state = Arc::clone(&worker_state);
            let run_prepared = Arc::clone(&worker_prepared);
            let (request_sender, request_receiver) = mpsc::channel();
            let worker = thread::Builder::new()
                .name("toy-kv-v8-extent-preparer".to_owned())
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_v8_sliding_extent_initializer(
                            worker_file,
                            request_receiver,
                            run_state,
                            run_prepared,
                            start_offset,
                            max_file_size,
                            window_bytes,
                        )
                    }));
                    let failure = match result {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(format!("{error:#}")),
                        Err(payload) => Some(format!(
                            "background V8 extent initializer panicked: {}",
                            panic_payload_message(payload.as_ref())
                        )),
                    };
                    if let Some(failure) = failure {
                        let mut state = worker_state.lock();
                        state.failure = Some(failure);
                        worker_prepared.notify_all();
                    }
                })
                .context("failed to spawn V8 background extent initializer")?;
            (Some(request_sender), Some(worker))
        } else {
            (None, None)
        };

        Ok(Self {
            data_file,
            start_offset,
            max_file_size,
            window_bytes,
            mode,
            state,
            prepared,
            request_sender,
            worker,
        })
    }

    fn ensure_prepared_through(&self, required_end: u64) -> Result<()> {
        ensure!(
            required_end <= self.max_file_size,
            "sliding V8 extent preparation exceeds the WAL segment limit"
        );
        if self.mode == WalExtentPreparationMode::SlidingInitialize {
            return self.wait_for_background_preparation(required_end);
        }

        let mut state = self.state.lock();
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 sliding extent preparation failed: {failure}");
        }
        if required_end <= state.prepared_end {
            return Ok(());
        }

        let target_end = self.target_end(required_end)?;
        ensure!(
            target_end >= required_end && target_end > state.prepared_end,
            "sliding V8 extent preparation did not cover the requested write"
        );

        let prepared_start = state.prepared_end;
        let offset = libc::off_t::try_from(prepared_start)
            .context("sliding V8 extent preparation offset exceeds off_t")?;
        let len = libc::off_t::try_from(target_end - prepared_start)
            .context("sliding V8 extent preparation length exceeds off_t")?;
        let started_at = Instant::now();
        // SAFETY: the checked range is within the configured WAL file cap and
        // the owned descriptor remains live for the syscall.
        let result = unsafe { libc::fallocate(self.data_file.as_raw_fd(), 0, offset, len) };
        ensure!(
            result == 0,
            "V8 benchmark sliding fallocate failed: {}",
            io::Error::last_os_error()
        );
        let elapsed_ns = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        state.prepared_end = target_end;
        state.allocated_end = target_end;
        state.requested_end = target_end;
        state.fallocate_calls = state.fallocate_calls.saturating_add(1);
        state.allocated_bytes = state
            .allocated_bytes
            .saturating_add(target_end - prepared_start);
        state.elapsed_ns = state.elapsed_ns.saturating_add(elapsed_ns);
        state.fallocate_elapsed_ns = state.fallocate_elapsed_ns.saturating_add(elapsed_ns);

        Ok(())
    }

    fn target_end(&self, required_end: u64) -> Result<u64> {
        sliding_extent_target_end(
            self.start_offset,
            self.max_file_size,
            self.window_bytes,
            required_end,
        )
    }

    fn queue_background_preparation(&self, required_end: u64) -> Result<()> {
        ensure!(
            required_end <= self.max_file_size,
            "sliding V8 extent preparation exceeds the WAL segment limit"
        );
        let mut state = self.state.lock();
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 sliding extent preparation failed: {failure}");
        }
        if required_end <= state.prepared_end {
            return Ok(());
        }
        if required_end > state.requested_end {
            let sender = self
                .request_sender
                .as_ref()
                .context("V8 background extent initializer is unavailable")?;
            sender
                .send(required_end)
                .context("V8 background extent initializer stopped unexpectedly")?;
            state.requested_end = required_end;
        }

        Ok(())
    }

    fn wait_for_background_preparation(&self, required_end: u64) -> Result<()> {
        self.queue_background_preparation(required_end)?;
        let mut state = self.state.lock();
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 sliding extent preparation failed: {failure}");
        }
        if required_end <= state.prepared_end {
            return Ok(());
        }

        let wait_started = Instant::now();
        state.wait_calls = state.wait_calls.saturating_add(1);
        while required_end > state.prepared_end && state.failure.is_none() {
            self.prepared.wait(&mut state);
        }
        state.wait_ns = state
            .wait_ns
            .saturating_add(u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 sliding extent preparation failed: {failure}");
        }
        ensure!(
            required_end <= state.prepared_end,
            "V8 background extent initializer stopped before preparing the requested range"
        );

        Ok(())
    }

    fn diagnostics(&self) -> WalV8SlidingExtentPreparationDiagnostics {
        let state = self.state.lock();

        WalV8SlidingExtentPreparationDiagnostics {
            mode: self.mode,
            start_offset: self.start_offset,
            max_file_size: self.max_file_size,
            window_bytes: self.window_bytes,
            initialization_chunk_bytes: WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64,
            prepared_end: state.prepared_end,
            allocated_end: state.allocated_end,
            fallocate_calls: state.fallocate_calls,
            allocated_bytes: state.allocated_bytes,
            initialization_chunks: state.initialization_chunks,
            initialized_bytes: state.initialized_bytes,
            elapsed_ns: state.elapsed_ns,
            fallocate_elapsed_ns: state.fallocate_elapsed_ns,
            initialization_elapsed_ns: state.initialization_elapsed_ns,
            wait_calls: state.wait_calls,
            wait_ns: state.wait_ns,
            lookahead_check_calls: 0,
            extent_policy_entries_scanned: 0,
            extent_policy_map_max_entries: 0,
            lookahead_worker_cpu_ns: 0,
            extent_policy_storage: "vec",
            extent_policy_scan_diagnostics_enabled: false,
        }
    }
}

fn sliding_extent_target_end(
    start_offset: u64,
    max_file_size: u64,
    window_bytes: u64,
    required_end: u64,
) -> Result<u64> {
    let relative_end = required_end
        .checked_sub(start_offset)
        .context("sliding V8 extent preparation precedes its initial cursor")?;
    let windows = relative_end
        .checked_add(window_bytes - 1)
        .context("sliding V8 extent preparation window count overflows")?
        / window_bytes;

    Ok(start_offset
        .checked_add(
            windows
                .checked_mul(window_bytes)
                .context("sliding V8 extent preparation window length overflows")?,
        )
        .context("sliding V8 extent preparation end overflows")?
        .min(max_file_size))
}

fn extent_window_start(start_offset: u64, window_bytes: u64, offset: u64) -> Result<u64> {
    let relative = offset
        .checked_sub(start_offset)
        .context("V8 extent offset precedes its initial cursor")?;
    start_offset
        .checked_add((relative / window_bytes) * window_bytes)
        .context("V8 extent window start overflows")
}

struct WalV8ExtentSlack {
    start: u64,
    end: u64,
    reservation_id: u64,
    charged_bytes: u64,
}

struct WalV8ExtentPreallocationState {
    prepared_end: u64,
    allocated_end: u64,
    requested_end: u64,
    background_request_pending: bool,
    extent_policies: WalV8ExtentPolicies,
    slack: Vec<WalV8ExtentSlack>,
    window_preallocation_disabled: bool,
    failure: Option<String>,
    #[cfg(feature = "bench")]
    fallocate_calls: u64,
    #[cfg(feature = "bench")]
    allocated_bytes: u64,
    #[cfg(feature = "bench")]
    initialization_chunks: u64,
    #[cfg(feature = "bench")]
    initialized_bytes: u64,
    #[cfg(feature = "bench")]
    initialization_elapsed_ns: u64,
    #[cfg(feature = "bench")]
    fallocate_elapsed_ns: u64,
    #[cfg(feature = "bench")]
    elapsed_ns: u64,
    #[cfg(feature = "bench")]
    wait_calls: u64,
    #[cfg(feature = "bench")]
    wait_ns: u64,
    #[cfg(feature = "bench")]
    lookahead_check_calls: u64,
    #[cfg(feature = "bench")]
    extent_policy_scan_diagnostics_enabled: bool,
    #[cfg(feature = "bench")]
    extent_policy_entries_scanned: u64,
    #[cfg(feature = "bench")]
    extent_policy_map_max_entries: u64,
    #[cfg(feature = "bench")]
    lookahead_worker_cpu_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalV8ExtentPolicyStorageKind {
    BTreeMap,
    Vec,
}

enum WalV8ExtentPolicyStorage {
    BTreeMap(BTreeMap<u64, WalV8ExtentPolicy>),
    Vec(Vec<Option<WalV8ExtentPolicy>>),
}

/// Tracks the first write class assigned to each fixed WAL extent window.
///
/// The Vec representation keeps an `Option` per window so an unregistered
/// window remains distinct from a window owned by a FallocateOnly write.
struct WalV8ExtentPolicies {
    start_offset: u64,
    window_bytes: u64,
    storage: WalV8ExtentPolicyStorage,
    entries: usize,
}

impl WalV8ExtentPolicies {
    fn new(start_offset: u64, window_bytes: u64, kind: WalV8ExtentPolicyStorageKind) -> Self {
        let storage = match kind {
            WalV8ExtentPolicyStorageKind::BTreeMap => {
                WalV8ExtentPolicyStorage::BTreeMap(BTreeMap::new())
            }
            WalV8ExtentPolicyStorageKind::Vec => WalV8ExtentPolicyStorage::Vec(Vec::new()),
        };
        Self {
            start_offset,
            window_bytes,
            storage,
            entries: 0,
        }
    }

    fn index(&self, extent_start: u64) -> Result<usize> {
        let relative = extent_start
            .checked_sub(self.start_offset)
            .context("V8 extent policy precedes its initial cursor")?;
        ensure!(
            relative.is_multiple_of(self.window_bytes),
            "V8 extent policy offset is not window-aligned"
        );
        usize::try_from(relative / self.window_bytes)
            .context("V8 extent policy index exceeds addressable memory")
    }

    fn insert_if_absent(
        &mut self,
        extent_start: u64,
        policy: WalV8ExtentPolicy,
    ) -> Result<WalV8ExtentPolicy> {
        let index = self.index(extent_start)?;
        match &mut self.storage {
            WalV8ExtentPolicyStorage::BTreeMap(policies) => {
                let selected = *policies.entry(extent_start).or_insert(policy);
                self.entries = policies.len();
                Ok(selected)
            }
            WalV8ExtentPolicyStorage::Vec(policies) => {
                let required_len = index
                    .checked_add(1)
                    .context("V8 extent policy vector length overflows")?;
                if policies.len() < required_len {
                    policies.resize(required_len, None);
                }
                let slot = &mut policies[index];
                if let Some(existing) = slot {
                    Ok(*existing)
                } else {
                    *slot = Some(policy);
                    self.entries = self.entries.saturating_add(1);
                    Ok(policy)
                }
            }
        }
    }

    fn get(&self, extent_start: u64) -> Option<WalV8ExtentPolicy> {
        let index = self.index(extent_start).ok()?;
        match &self.storage {
            WalV8ExtentPolicyStorage::BTreeMap(policies) => policies.get(&extent_start).copied(),
            WalV8ExtentPolicyStorage::Vec(policies) => policies.get(index).copied().flatten(),
        }
    }

    fn truncate_before(&mut self, end: u64) {
        if end < self.start_offset {
            return;
        }
        match &mut self.storage {
            WalV8ExtentPolicyStorage::BTreeMap(policies) => {
                policies.retain(|extent_start, _| *extent_start < end);
                self.entries = policies.len();
            }
            WalV8ExtentPolicyStorage::Vec(policies) => {
                let relative_end = end.saturating_sub(self.start_offset);
                let retained_windows = relative_end / self.window_bytes
                    + u64::from(!relative_end.is_multiple_of(self.window_bytes));
                let retained_windows = usize::try_from(retained_windows).unwrap_or(usize::MAX);
                let removed = policies
                    .get(retained_windows..)
                    .unwrap_or_default()
                    .iter()
                    .filter(|policy| policy.is_some())
                    .count();
                policies.truncate(retained_windows);
                self.entries = self.entries.saturating_sub(removed);
            }
        }
    }

    fn has_unprepared_lookahead(
        &self,
        end: u64,
        prepared_end: u64,
        count_scanned: bool,
    ) -> (bool, u64) {
        match &self.storage {
            WalV8ExtentPolicyStorage::BTreeMap(policies) => {
                if count_scanned {
                    let mut scanned = 0_u64;
                    for (&extent_start, &policy) in policies {
                        scanned = scanned.saturating_add(1);
                        if policy == WalV8ExtentPolicy::Lookahead
                            && extent_start < end
                            && extent_start.saturating_add(self.window_bytes) > prepared_end
                        {
                            return (true, scanned);
                        }
                    }
                    (false, scanned)
                } else {
                    (
                        policies.iter().any(|(&extent_start, &policy)| {
                            policy == WalV8ExtentPolicy::Lookahead
                                && extent_start < end
                                && extent_start.saturating_add(self.window_bytes) > prepared_end
                        }),
                        0,
                    )
                }
            }
            WalV8ExtentPolicyStorage::Vec(policies) => {
                let lower = prepared_end.saturating_sub(self.window_bytes.saturating_sub(1));
                if lower >= end || end <= self.start_offset {
                    return (false, 0);
                }
                let relative_lower = lower.saturating_sub(self.start_offset);
                let first_index = relative_lower / self.window_bytes
                    + u64::from(!relative_lower.is_multiple_of(self.window_bytes));
                let relative_end = end.saturating_sub(self.start_offset);
                let end_index = relative_end / self.window_bytes
                    + u64::from(!relative_end.is_multiple_of(self.window_bytes));
                let Ok(first_index) = usize::try_from(first_index) else {
                    return (false, 0);
                };
                let Ok(end_index) = usize::try_from(end_index) else {
                    return (false, 0);
                };
                let start_index = first_index.min(policies.len());
                let end_index = end_index.min(policies.len());
                if start_index >= end_index {
                    return (false, 0);
                }

                let Some(candidates) = policies.get(start_index..end_index) else {
                    return (false, 0);
                };
                let Some(first_extent_start) = u64::try_from(start_index)
                    .ok()
                    .and_then(|index| index.checked_mul(self.window_bytes))
                    .and_then(|relative| self.start_offset.checked_add(relative))
                else {
                    return (false, 0);
                };

                let mut scanned = 0_u64;
                for (relative_index, policy) in candidates.iter().enumerate() {
                    if count_scanned {
                        scanned = scanned.saturating_add(1);
                    }
                    let Some(policy) = policy else {
                        continue;
                    };
                    let Some(extent_start) = u64::try_from(relative_index)
                        .ok()
                        .and_then(|index| index.checked_mul(self.window_bytes))
                        .and_then(|relative| first_extent_start.checked_add(relative))
                    else {
                        continue;
                    };
                    if *policy == WalV8ExtentPolicy::Lookahead
                        && extent_start < end
                        && extent_start.saturating_add(self.window_bytes) > prepared_end
                    {
                        return (true, scanned);
                    }
                }
                (false, scanned)
            }
        }
    }

    fn kind(&self) -> WalV8ExtentPolicyStorageKind {
        match &self.storage {
            WalV8ExtentPolicyStorage::BTreeMap(_) => WalV8ExtentPolicyStorageKind::BTreeMap,
            WalV8ExtentPolicyStorage::Vec(_) => WalV8ExtentPolicyStorageKind::Vec,
        }
    }

    fn len(&self) -> usize {
        self.entries
    }
}

#[cfg(feature = "bench")]
fn extent_policy_storage_kind_from_env() -> Result<WalV8ExtentPolicyStorageKind> {
    match std::env::var("TOYKV_BENCH_V8_EXTENT_POLICY_STORAGE") {
        Ok(value) if value == "btree" => Ok(WalV8ExtentPolicyStorageKind::BTreeMap),
        Ok(value) if value == "vec" => Ok(WalV8ExtentPolicyStorageKind::Vec),
        Ok(value) => anyhow::bail!(
            "TOYKV_BENCH_V8_EXTENT_POLICY_STORAGE must be `btree` or `vec`, got `{value}`"
        ),
        Err(std::env::VarError::NotPresent) => Ok(WalV8ExtentPolicyStorageKind::Vec),
        Err(error) => Err(error).context("read V8 extent policy storage setting"),
    }
}

#[cfg(not(feature = "bench"))]
fn extent_policy_storage_kind_from_env() -> Result<WalV8ExtentPolicyStorageKind> {
    Ok(WalV8ExtentPolicyStorageKind::Vec)
}

#[cfg(feature = "bench")]
fn extent_policy_scan_diagnostics_enabled_from_env() -> Result<bool> {
    match std::env::var("TOYKV_BENCH_V8_EXTENT_POLICY_SCAN_DIAGNOSTICS") {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" => Ok(false),
        Ok(value) => anyhow::bail!(
            "TOYKV_BENCH_V8_EXTENT_POLICY_SCAN_DIAGNOSTICS must be `0` or `1`, got `{value}`"
        ),
        Err(std::env::VarError::NotPresent) => Ok(true),
        Err(error) => Err(error).context("read V8 extent policy scan diagnostics setting"),
    }
}

#[cfg(not(feature = "bench"))]
fn extent_policy_scan_diagnostics_enabled_from_env() -> Result<bool> {
    Ok(false)
}

#[cfg(feature = "bench")]
fn record_extent_policy_map_len(state: &mut WalV8ExtentPreallocationState) {
    if !state.extent_policy_scan_diagnostics_enabled {
        return;
    }
    let entries = u64::try_from(state.extent_policies.len()).unwrap_or(u64::MAX);
    state.extent_policy_map_max_entries = state.extent_policy_map_max_entries.max(entries);
}

#[cfg(feature = "bench")]
fn record_lookahead_worker_cpu(
    state: &mut WalV8ExtentPreallocationState,
    previous_cpu_ns: &mut u64,
) {
    let current_cpu_ns = digest::thread_cpu_time_ns();
    state.lookahead_worker_cpu_ns = state
        .lookahead_worker_cpu_ns
        .saturating_add(current_cpu_ns.saturating_sub(*previous_cpu_ns));
    *previous_cpu_ns = current_cpu_ns;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalV8ExtentPolicy {
    Lookahead,
    FallocateOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalV8ExtentPreparationStrategy {
    FallocateOnly,
    Lookahead,
    #[cfg(feature = "bench")]
    PointOnlyLookahead,
}

impl WalV8ExtentPreparationStrategy {
    const fn uses_lookahead(self) -> bool {
        !matches!(self, Self::FallocateOnly)
    }

    const fn is_point_only(self) -> bool {
        #[cfg(feature = "bench")]
        {
            matches!(self, Self::PointOnlyLookahead)
        }
        #[cfg(not(feature = "bench"))]
        {
            false
        }
    }
}

/// Sliding ext4 extent allocation for V8. Each window's unused suffix is
/// charged to the image owner, then its charge transfers to the normal DATA or
/// FRONTIER reservation as that range is assigned.
struct WalV8ExtentPreallocator {
    data_file: File,
    start_offset: u64,
    max_file_size: u64,
    window_bytes: u64,
    source_allocation_unit: u64,
    reservation_owner: Option<Arc<WalV8ReservationOwner>>,
    strategy: WalV8ExtentPreparationStrategy,
    state: Arc<Mutex<WalV8ExtentPreallocationState>>,
    prepared: Arc<Condvar>,
    preparation_lock: Arc<Mutex<()>>,
    request_sender: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl WalV8ExtentPreallocator {
    fn new(
        data_file: File,
        sync_file: &File,
        start_offset: u64,
        max_file_size: u64,
        window_bytes: u64,
        reservation_owner: Option<Arc<WalV8ReservationOwner>>,
    ) -> Result<Self> {
        Self::new_with_strategy(
            data_file,
            sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            reservation_owner,
            WalV8ExtentPreparationStrategy::FallocateOnly,
        )
    }

    fn new_with_lookahead(
        data_file: File,
        sync_file: &File,
        start_offset: u64,
        max_file_size: u64,
        window_bytes: u64,
        reservation_owner: Option<Arc<WalV8ReservationOwner>>,
        lookahead: bool,
    ) -> Result<Self> {
        Self::new_with_strategy(
            data_file,
            sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            reservation_owner,
            if lookahead {
                WalV8ExtentPreparationStrategy::Lookahead
            } else {
                WalV8ExtentPreparationStrategy::FallocateOnly
            },
        )
    }

    #[cfg(feature = "bench")]
    fn new_with_point_only_lookahead(
        data_file: File,
        sync_file: &File,
        start_offset: u64,
        max_file_size: u64,
        window_bytes: u64,
        reservation_owner: Arc<WalV8ReservationOwner>,
    ) -> Result<Self> {
        Self::new_with_strategy(
            data_file,
            sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            Some(reservation_owner),
            #[cfg(feature = "bench")]
            WalV8ExtentPreparationStrategy::PointOnlyLookahead,
        )
    }

    fn new_with_strategy(
        data_file: File,
        sync_file: &File,
        start_offset: u64,
        max_file_size: u64,
        window_bytes: u64,
        reservation_owner: Option<Arc<WalV8ReservationOwner>>,
        strategy: WalV8ExtentPreparationStrategy,
    ) -> Result<Self> {
        let lookahead = strategy.uses_lookahead();
        let point_only_lookahead = strategy.is_point_only();
        let extent_policy_storage_kind = extent_policy_storage_kind_from_env()?;
        #[cfg(feature = "bench")]
        let extent_policy_scan_diagnostics_enabled =
            extent_policy_scan_diagnostics_enabled_from_env()?;
        #[cfg(not(feature = "bench"))]
        let _extent_policy_scan_diagnostics_enabled =
            extent_policy_scan_diagnostics_enabled_from_env()?;
        ensure!(
            is_o_direct_file(&data_file)?,
            "V8 sliding extent preparation requires an O_DIRECT DATA worker"
        );
        let data_metadata = data_file
            .metadata()
            .context("inspect V8 WAL before sliding extent preparation")?;
        let sync_metadata = sync_file
            .metadata()
            .context("inspect V8 WAL sync handle before sliding extent preparation")?;
        ensure!(
            data_metadata.is_file()
                && sync_metadata.is_file()
                && data_metadata.dev() == sync_metadata.dev()
                && data_metadata.ino() == sync_metadata.ino()
                && data_metadata.len() == start_offset,
            "V8 sliding extent preparation requires the untouched installed Active WAL inode"
        );
        ensure!(
            window_bytes > 0 && window_bytes.is_multiple_of(FRAME_LEN_U64),
            "V8 sliding extent window must be a positive multiple of the frame size"
        );
        ensure!(
            start_offset <= max_file_size,
            "V8 sliding extent preparation starts beyond the WAL segment limit"
        );
        if let Some(owner) = &reservation_owner {
            ensure!(
                max_file_size <= MAX_WAL_FILE_SIZE,
                "V8 sliding extent preparation exceeds the maximum WAL file size"
            );
            ensure!(
                owner.source_allocation_unit() > 0,
                "V8 source allocation unit is zero"
            );
        }

        ensure!(
            !lookahead || reservation_owner.is_some(),
            "V8 extent lookahead requires a quota reservation owner"
        );

        let state = Arc::new(Mutex::new(WalV8ExtentPreallocationState {
            prepared_end: start_offset,
            allocated_end: start_offset,
            requested_end: start_offset,
            background_request_pending: false,
            extent_policies: WalV8ExtentPolicies::new(
                start_offset,
                window_bytes,
                extent_policy_storage_kind,
            ),
            slack: Vec::new(),
            window_preallocation_disabled: false,
            failure: None,
            #[cfg(feature = "bench")]
            fallocate_calls: 0,
            #[cfg(feature = "bench")]
            allocated_bytes: 0,
            #[cfg(feature = "bench")]
            initialization_chunks: 0,
            #[cfg(feature = "bench")]
            initialized_bytes: 0,
            #[cfg(feature = "bench")]
            initialization_elapsed_ns: 0,
            #[cfg(feature = "bench")]
            fallocate_elapsed_ns: 0,
            #[cfg(feature = "bench")]
            elapsed_ns: 0,
            #[cfg(feature = "bench")]
            wait_calls: 0,
            #[cfg(feature = "bench")]
            wait_ns: 0,
            #[cfg(feature = "bench")]
            lookahead_check_calls: 0,
            #[cfg(feature = "bench")]
            extent_policy_scan_diagnostics_enabled,
            #[cfg(feature = "bench")]
            extent_policy_entries_scanned: 0,
            #[cfg(feature = "bench")]
            extent_policy_map_max_entries: 0,
            #[cfg(feature = "bench")]
            lookahead_worker_cpu_ns: 0,
        }));
        let prepared = Arc::new(Condvar::new());
        let preparation_lock = Arc::new(Mutex::new(()));
        let (request_sender, worker) = if lookahead {
            let worker_file = data_file
                .try_clone()
                .context("clone V8 WAL handle for extent lookahead")?;
            let worker_state = Arc::clone(&state);
            let worker_prepared = Arc::clone(&prepared);
            let worker_preparation_lock = Arc::clone(&preparation_lock);
            let worker_owner = Arc::clone(
                reservation_owner
                    .as_ref()
                    .context("V8 extent lookahead lost its quota owner")?,
            );
            let (request_sender, request_receiver) = mpsc::channel();
            let worker = thread::Builder::new()
                .name("toy-kv-v8-extent-lookahead".to_owned())
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        WalV8ExtentLookaheadWorker {
                            data_file: worker_file,
                            requests: request_receiver,
                            state: worker_state.clone(),
                            prepared: worker_prepared.clone(),
                            preparation_lock: worker_preparation_lock,
                            owner: worker_owner,
                            start_offset,
                            max_file_size,
                            window_bytes,
                            point_only_lookahead,
                        }
                        .run()
                    }));
                    let failure = match result {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(format!("{error:#}")),
                        Err(_) => Some("background V8 extent lookahead panicked".to_owned()),
                    };
                    if let Some(failure) = failure {
                        let (_extent_state_timer, mut state) =
                            mutex_diagnostics::lock(&worker_state, MutexLockKind::ExtentState);
                        state.failure = Some(failure);
                        worker_prepared.notify_all();
                    }
                })
                .context("failed to spawn V8 extent lookahead worker")?;
            (Some(request_sender), Some(worker))
        } else {
            (None, None)
        };

        Ok(Self {
            data_file,
            start_offset,
            max_file_size,
            window_bytes,
            source_allocation_unit: reservation_owner
                .as_ref()
                .map_or(FRAME_LEN_U64, |owner| owner.source_allocation_unit()),
            reservation_owner,
            strategy,
            state,
            prepared,
            preparation_lock,
            request_sender,
            worker,
        })
    }

    fn register_data_range(
        &self,
        start: u64,
        end: u64,
        write_class: WalV8WriteClass,
        frame_bytes: u64,
    ) -> Result<()> {
        if !self.strategy.is_point_only() {
            return Ok(());
        }
        ensure!(
            start < end
                && start >= self.start_offset
                && end <= self.max_file_size
                && start.is_multiple_of(FRAME_LEN_U64)
                && end.is_multiple_of(FRAME_LEN_U64),
            "invalid V8 DATA range for extent policy registration"
        );

        let policy = if write_class == WalV8WriteClass::Point && frame_bytes == FRAME_LEN_U64 {
            WalV8ExtentPolicy::Lookahead
        } else {
            WalV8ExtentPolicy::FallocateOnly
        };
        let (_extent_state_timer, mut state) =
            mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 extent lookahead failed: {failure}");
        }
        let prepared_end = state.prepared_end;
        let mut extent_start = self.extent_window_start(start)?;
        while extent_start < end {
            // The first admitted DATA range chooses the policy for this
            // physical extent. Single-frame Point API writes can use lookahead;
            // Batch API writes and multi-frame writes keep the existing
            // fallocate-only path. Later ranges inherit the first owner's policy.
            // If preparation already passed it, keep the conservative
            // fallocate-only policy instead of claiming bytes were initialized.
            state.extent_policies.insert_if_absent(
                extent_start,
                if extent_start < prepared_end {
                    WalV8ExtentPolicy::FallocateOnly
                } else {
                    policy
                },
            )?;
            extent_start = extent_start
                .checked_add(self.window_bytes)
                .context("V8 extent policy cursor overflows")?;
        }

        #[cfg(feature = "bench")]
        record_extent_policy_map_len(&mut state);

        Ok(())
    }

    fn extent_window_start(&self, offset: u64) -> Result<u64> {
        let relative = offset
            .checked_sub(self.start_offset)
            .context("V8 extent offset precedes its initial cursor")?;
        self.start_offset
            .checked_add((relative / self.window_bytes) * self.window_bytes)
            .context("V8 extent window start overflows")
    }

    fn has_unprepared_lookahead_extent(
        &self,
        state: &mut WalV8ExtentPreallocationState,
        end: u64,
    ) -> bool {
        let prepared_end = state.prepared_end;
        #[cfg(feature = "bench")]
        let diagnostics_enabled = state.extent_policy_scan_diagnostics_enabled;
        #[cfg(not(feature = "bench"))]
        let diagnostics_enabled = false;
        #[cfg(feature = "bench")]
        let (has_unprepared, entries_scanned) =
            state
                .extent_policies
                .has_unprepared_lookahead(end, prepared_end, diagnostics_enabled);
        #[cfg(not(feature = "bench"))]
        let (has_unprepared, _) =
            state
                .extent_policies
                .has_unprepared_lookahead(end, prepared_end, diagnostics_enabled);
        #[cfg(feature = "bench")]
        if diagnostics_enabled {
            state.lookahead_check_calls = state.lookahead_check_calls.saturating_add(1);
            state.extent_policy_entries_scanned = state
                .extent_policy_entries_scanned
                .saturating_add(entries_scanned);
            state.extent_policy_map_max_entries = state
                .extent_policy_map_max_entries
                .max(u64::try_from(state.extent_policies.len()).unwrap_or(u64::MAX));
        }

        has_unprepared
    }

    fn claim_fallocate_only_windows(
        &self,
        state: &mut WalV8ExtentPreallocationState,
        start: u64,
        end: u64,
    ) -> Result<()> {
        if !self.strategy.is_point_only() || start >= end {
            return Ok(());
        }

        let mut extent_start = self.extent_window_start(start)?;
        while extent_start < end {
            state
                .extent_policies
                .insert_if_absent(extent_start, WalV8ExtentPolicy::FallocateOnly)?;
            extent_start = extent_start
                .checked_add(self.window_bytes)
                .context("V8 extent policy cursor overflows")?;
        }

        #[cfg(feature = "bench")]
        record_extent_policy_map_len(state);

        Ok(())
    }

    fn ensure_prepared_through(&self, required_end: u64) -> Result<()> {
        if self.strategy.uses_lookahead()
            && (!self.strategy.is_point_only() || {
                let (_extent_state_timer, mut state) =
                    mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
                self.has_unprepared_lookahead_extent(&mut state, required_end)
            })
        {
            return self.wait_for_prepared_through(required_end);
        }

        self.ensure_prepared_through_with(required_end, |start, end| {
            fallocate_wal_range(&self.data_file, start, end)
        })
    }

    fn ensure_prepared_through_with(
        &self,
        required_end: u64,
        mut fallocate: impl FnMut(u64, u64) -> Result<()>,
    ) -> Result<()> {
        ensure!(
            required_end <= self.max_file_size,
            "V8 sliding extent preparation exceeds the WAL segment limit"
        );
        let (_preparation_timer, _preparation) = self
            .strategy
            .uses_lookahead()
            .then(|| {
                mutex_diagnostics::lock(&self.preparation_lock, MutexLockKind::ExtentPreparation)
            })
            .unzip();
        let (_extent_state_timer, mut state) =
            mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 extent preparation failed: {failure}");
        }
        if required_end <= state.prepared_end {
            return Ok(());
        }

        let mut target_end = if state.window_preallocation_disabled {
            required_end
        } else {
            sliding_extent_target_end(
                self.start_offset,
                self.max_file_size,
                self.window_bytes,
                required_end,
            )?
        };
        if self.reservation_owner.is_none() {
            // The window suffix has no quota owner in test-only writers, so
            // keep those paths exact rather than allocating uncharged space.
            target_end = required_end;
        }
        if self.strategy.is_point_only()
            && self.has_unprepared_lookahead_extent(&mut state, target_end)
        {
            drop(state);
            drop(_extent_state_timer);
            drop(_preparation);
            drop(_preparation_timer);
            return self.wait_for_prepared_through(target_end);
        }
        let mut slack_reservation = None;
        if target_end > required_end
            && let Some(owner) = &self.reservation_owner
        {
            let slack_bytes = target_end - required_end;
            let charged_bytes = round_up_extent_charge(slack_bytes, self.source_allocation_unit)?;
            match owner.reserve_extent_slack(charged_bytes) {
                Ok(reservation_id) => slack_reservation = Some((reservation_id, charged_bytes)),
                Err(WalV7ReservationError::SourceSpoolLimit { .. }) => {
                    // Ticket reservations already cover the exact DATA and
                    // worst-case FRONTIER prefix. If quota cannot cover the
                    // speculative suffix, allocate only that required prefix.
                    target_end = required_end;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let prepared_start = state.allocated_end;
        if let Some((reservation_id, charged_bytes)) = slack_reservation.take() {
            // Keep the charge reachable even if fallocate fails after
            // partially allocating the speculative suffix.
            state.slack.push(WalV8ExtentSlack {
                start: required_end,
                end: target_end,
                reservation_id,
                charged_bytes,
            });
        }
        #[cfg(feature = "bench")]
        let started_at = Instant::now();
        #[cfg(feature = "bench")]
        let mut fallocate_calls = 0_u64;
        let prepared_end = fallocate_sliding_extent_with_fallback(
            prepared_start,
            required_end,
            target_end,
            &mut state.window_preallocation_disabled,
            |start, end| {
                #[cfg(feature = "bench")]
                {
                    fallocate_calls = fallocate_calls.saturating_add(1);
                }
                fallocate(start, end)
            },
        );
        #[cfg(feature = "bench")]
        {
            state.fallocate_calls = state.fallocate_calls.saturating_add(fallocate_calls);
            let elapsed_ns = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            state.fallocate_elapsed_ns = state.fallocate_elapsed_ns.saturating_add(elapsed_ns);
            state.elapsed_ns = state.elapsed_ns.saturating_add(elapsed_ns);
        }
        let prepared_end = prepared_end?;
        target_end = prepared_end;
        let prepared_start = state.prepared_end;
        self.claim_fallocate_only_windows(&mut state, prepared_start, target_end)?;
        #[cfg(feature = "bench")]
        let allocated_bytes = target_end - prepared_start;

        state.allocated_end = target_end;
        state.prepared_end = target_end;
        #[cfg(feature = "bench")]
        {
            state.allocated_bytes = state.allocated_bytes.saturating_add(allocated_bytes);
        }

        ensure!(
            state.prepared_end >= required_end,
            "V8 sliding extent preparation did not cover the requested write"
        );

        Ok(())
    }

    fn queue_background_preparation(&self, required_end: u64) -> Result<()> {
        ensure!(
            self.strategy.uses_lookahead(),
            "V8 background extent preparation is not enabled"
        );
        ensure!(
            required_end <= self.max_file_size,
            "V8 sliding extent preparation exceeds the WAL segment limit"
        );
        let (_extent_state_timer, mut state) =
            mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 extent lookahead failed: {failure}");
        }
        if self.strategy.is_point_only()
            && !self.has_unprepared_lookahead_extent(&mut state, required_end)
        {
            return Ok(());
        }
        if required_end <= state.prepared_end || required_end <= state.requested_end {
            return Ok(());
        }

        if !state.background_request_pending {
            let sender = self
                .request_sender
                .as_ref()
                .context("V8 extent lookahead worker is unavailable")?;
            sender
                .send(())
                .context("V8 extent lookahead worker stopped unexpectedly")?;
            state.background_request_pending = true;
        }
        state.requested_end = required_end;

        Ok(())
    }

    fn wait_for_prepared_through(&self, required_end: u64) -> Result<()> {
        self.queue_background_preparation(required_end)?;
        let (mut extent_state_timer, mut state) =
            mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
        while required_end > state.prepared_end && state.failure.is_none() {
            #[cfg(feature = "bench")]
            {
                state.wait_calls = state.wait_calls.saturating_add(1);
            }
            #[cfg(feature = "bench")]
            let wait_started = Instant::now();
            let paused_at = extent_state_timer.pause_for_condvar();
            self.prepared.wait(&mut state);
            extent_state_timer.resume_after_condvar(paused_at);
            #[cfg(feature = "bench")]
            {
                state.wait_ns = state.wait_ns.saturating_add(
                    u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
            }
        }
        if let Some(failure) = &state.failure {
            anyhow::bail!("V8 extent lookahead failed: {failure}");
        }
        ensure!(
            required_end <= state.prepared_end,
            "V8 extent lookahead stopped before preparing the requested range"
        );

        Ok(())
    }

    /// Transfer quota coverage for a unique DATA or FRONTIER write range from
    /// window slack to the write's existing batch reservation.
    fn consume_source_credit(&self, start: u64, end: u64) -> Result<()> {
        ensure!(start <= end, "V8 extent source-credit range is reversed");
        if start == end {
            return Ok(());
        }

        let Some(owner) = &self.reservation_owner else {
            return Ok(());
        };
        let (_extent_state_timer, mut state) =
            mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
        let first = state.slack.partition_point(|window| window.end <= start);
        for window in &mut state.slack[first..] {
            if window.start >= end {
                break;
            }
            let overlap_start = start.max(window.start);
            let overlap_end = end.min(window.end);
            if overlap_start >= overlap_end || window.charged_bytes == 0 {
                continue;
            }
            let overlap_bytes = overlap_end - overlap_start;
            let released_bytes =
                round_up_extent_charge(overlap_bytes, self.source_allocation_unit)?
                    .min(window.charged_bytes);
            owner.release_extent_slack(window.reservation_id, released_bytes)?;
            window.charged_bytes -= released_bytes;
        }

        Ok(())
    }

    /// Discard unused physical space before retrying quota admission. The
    /// caller must exclude new coordinate assignments until this returns and
    /// include every admitted DATA and reserved FRONTIER in `allocated_tail`.
    fn discard_speculative_tail(&self, allocated_tail: u64) -> Result<()> {
        ensure!(
            (self.start_offset..=self.max_file_size).contains(&allocated_tail)
                && allocated_tail.is_multiple_of(FRAME_LEN_U64),
            "invalid v8 extent reclamation tail"
        );
        let Some(owner) = &self.reservation_owner else {
            return Ok(());
        };
        let (_preparation_timer, _preparation) = self
            .strategy
            .uses_lookahead()
            .then(|| {
                mutex_diagnostics::lock(&self.preparation_lock, MutexLockKind::ExtentPreparation)
            })
            .unzip();
        let (_extent_state_timer, mut state) =
            mutex_diagnostics::lock(&self.state, MutexLockKind::ExtentState);
        state.window_preallocation_disabled = true;
        if state.slack.iter().all(|window| window.charged_bytes == 0) {
            return Ok(());
        }

        // Keep this conservative even if truncation or sync fails after
        // changing the inode. All assigned ranges retain batch quota; only
        // the speculative suffix may be removed, and its charge stays held
        // until that removal is durable.
        state.prepared_end = state.prepared_end.min(allocated_tail);
        state.allocated_end = state.allocated_end.min(allocated_tail);
        state.requested_end = state.requested_end.min(allocated_tail);
        state.extent_policies.truncate_before(allocated_tail);
        self.data_file
            .set_len(allocated_tail)
            .context("truncate speculative v8 WAL extent tail")?;
        self.data_file
            .sync_data()
            .context("sync reclaimed v8 WAL extent tail")?;
        for window in &mut state.slack {
            owner.release_extent_slack(window.reservation_id, window.charged_bytes)?;
            window.charged_bytes = 0;
        }
        state.slack.clear();

        Ok(())
    }

    #[cfg(feature = "bench")]
    fn diagnostics(&self) -> WalV8SlidingExtentPreparationDiagnostics {
        let state = self.state.lock();

        WalV8SlidingExtentPreparationDiagnostics {
            mode: if self.strategy.is_point_only() {
                WalExtentPreparationMode::SlidingPointLookahead
            } else if self.strategy.uses_lookahead() {
                WalExtentPreparationMode::SlidingLookahead
            } else {
                WalExtentPreparationMode::SlidingFallocate
            },
            start_offset: self.start_offset,
            max_file_size: self.max_file_size,
            window_bytes: self.window_bytes,
            initialization_chunk_bytes: WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64,
            prepared_end: state.prepared_end,
            allocated_end: state.allocated_end,
            fallocate_calls: state.fallocate_calls,
            allocated_bytes: state.allocated_bytes,
            initialization_chunks: state.initialization_chunks,
            initialized_bytes: state.initialized_bytes,
            elapsed_ns: state.elapsed_ns,
            fallocate_elapsed_ns: state.fallocate_elapsed_ns,
            initialization_elapsed_ns: state.initialization_elapsed_ns,
            wait_calls: state.wait_calls,
            wait_ns: state.wait_ns,
            lookahead_check_calls: state.lookahead_check_calls,
            extent_policy_entries_scanned: state.extent_policy_entries_scanned,
            extent_policy_map_max_entries: state.extent_policy_map_max_entries,
            lookahead_worker_cpu_ns: state.lookahead_worker_cpu_ns,
            extent_policy_storage: match state.extent_policies.kind() {
                WalV8ExtentPolicyStorageKind::BTreeMap => "btree",
                WalV8ExtentPolicyStorageKind::Vec => "vec",
            },
            extent_policy_scan_diagnostics_enabled: state.extent_policy_scan_diagnostics_enabled,
        }
    }
}

impl Drop for WalV8ExtentPreallocator {
    fn drop(&mut self) {
        self.request_sender.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            log::error!("V8 extent lookahead worker panicked");
        }
    }
}

struct WalV8ExtentLookaheadWorker {
    data_file: File,
    requests: mpsc::Receiver<()>,
    state: Arc<Mutex<WalV8ExtentPreallocationState>>,
    prepared: Arc<Condvar>,
    preparation_lock: Arc<Mutex<()>>,
    owner: Arc<WalV8ReservationOwner>,
    start_offset: u64,
    max_file_size: u64,
    window_bytes: u64,
    point_only_lookahead: bool,
}

impl WalV8ExtentLookaheadWorker {
    fn run(self) -> Result<()> {
        let Self {
            data_file,
            requests,
            state,
            prepared,
            preparation_lock,
            owner,
            start_offset,
            max_file_size,
            window_bytes,
            point_only_lookahead,
        } = self;
        let chunk_bytes = WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64;
        #[cfg(feature = "bench")]
        let mut worker_cpu_checkpoint = digest::thread_cpu_time_ns();
        let mut zeros = DirectBuf::new(chunk_bytes as usize);
        while requests.recv().is_ok() {
            let (_preparation_timer, _preparation) =
                mutex_diagnostics::lock(&preparation_lock, MutexLockKind::ExtentPreparation);
            loop {
                let (_extent_state_timer, mut state_guard) =
                    mutex_diagnostics::lock(&state, MutexLockKind::ExtentState);
                let required_end = state_guard.requested_end;
                if required_end <= state_guard.prepared_end {
                    state_guard.background_request_pending = false;
                    #[cfg(feature = "bench")]
                    record_lookahead_worker_cpu(&mut state_guard, &mut worker_cpu_checkpoint);
                    prepared.notify_all();
                    break;
                }
                ensure!(
                    required_end <= max_file_size,
                    "V8 extent lookahead request exceeds the WAL segment limit"
                );

                let mut target_end = if state_guard.window_preallocation_disabled {
                    required_end
                } else {
                    sliding_extent_target_end(
                        start_offset,
                        max_file_size,
                        window_bytes,
                        required_end,
                    )?
                };
                let mut slack_reservation = None;
                if target_end > required_end {
                    let slack_bytes = target_end - required_end;
                    let charged_bytes =
                        round_up_extent_charge(slack_bytes, owner.source_allocation_unit())?;
                    match owner.reserve_extent_slack(charged_bytes) {
                        Ok(reservation_id) => {
                            slack_reservation = Some((reservation_id, charged_bytes));
                        }
                        Err(WalV7ReservationError::SourceSpoolLimit { .. }) => {
                            target_end = required_end;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }

                if let Some((reservation_id, charged_bytes)) = slack_reservation {
                    state_guard.slack.push(WalV8ExtentSlack {
                        start: required_end,
                        end: target_end,
                        reservation_id,
                        charged_bytes,
                    });
                }
                let allocated_start = state_guard.allocated_end;
                let mut preallocation_disabled = state_guard.window_preallocation_disabled;
                drop(state_guard);
                drop(_extent_state_timer);

                let mut fallocate_calls = 0_u64;
                let allocation_started = Instant::now();
                let allocated_end = if target_end > allocated_start {
                    fallocate_sliding_extent_with_fallback(
                        allocated_start,
                        required_end,
                        target_end,
                        &mut preallocation_disabled,
                        |start, end| {
                            fallocate_calls = fallocate_calls.saturating_add(1);
                            fallocate_wal_range(&data_file, start, end)
                        },
                    )?
                } else {
                    allocated_start
                };
                ensure!(
                    allocated_end >= required_end,
                    "V8 extent lookahead allocation missed the requested prefix"
                );
                let allocation_elapsed_ns =
                    u64::try_from(allocation_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

                let (_extent_state_timer, mut state_guard) =
                    mutex_diagnostics::lock(&state, MutexLockKind::ExtentState);
                ensure!(
                    state_guard.allocated_end == allocated_start,
                    "V8 extent lookahead lost its serialized allocation prefix"
                );
                state_guard.window_preallocation_disabled = preallocation_disabled;
                state_guard.allocated_end = allocated_end;
                #[cfg(feature = "bench")]
                {
                    state_guard.fallocate_calls =
                        state_guard.fallocate_calls.saturating_add(fallocate_calls);
                    state_guard.allocated_bytes = state_guard
                        .allocated_bytes
                        .saturating_add(allocated_end - allocated_start);
                    state_guard.fallocate_elapsed_ns = state_guard
                        .fallocate_elapsed_ns
                        .saturating_add(allocation_elapsed_ns);
                    state_guard.elapsed_ns =
                        state_guard.elapsed_ns.saturating_add(allocation_elapsed_ns);
                }
                #[cfg(not(feature = "bench"))]
                let _ = (fallocate_calls, allocation_elapsed_ns);
                drop(state_guard);

                loop {
                    let (_extent_state_timer, mut state_guard) =
                        mutex_diagnostics::lock(&state, MutexLockKind::ExtentState);
                    let initialized_start = state_guard.prepared_end;
                    if initialized_start >= allocated_end {
                        break;
                    }
                    let extent_start =
                        extent_window_start(start_offset, window_bytes, initialized_start)?;
                    let extent_end = extent_start
                        .checked_add(window_bytes)
                        .context("V8 extent lookahead window end overflows")?
                        .min(allocated_end);
                    let extent_policy = if point_only_lookahead {
                        let policy = state_guard
                            .extent_policies
                            .insert_if_absent(extent_start, WalV8ExtentPolicy::FallocateOnly)?;
                        #[cfg(feature = "bench")]
                        record_extent_policy_map_len(&mut state_guard);
                        policy
                    } else {
                        WalV8ExtentPolicy::Lookahead
                    };
                    if extent_policy == WalV8ExtentPolicy::FallocateOnly {
                        state_guard.prepared_end = extent_end;
                        prepared.notify_all();
                        continue;
                    }
                    let initialized_end = initialized_start
                        .checked_add(chunk_bytes)
                        .context("V8 extent lookahead chunk end overflows")?
                        .min(extent_end);
                    let chunk_len = usize::try_from(initialized_end - initialized_start)?;
                    ensure!(
                        initialized_start.is_multiple_of(FRAME_LEN_U64)
                            && chunk_len.is_multiple_of(FRAME_LEN_U64 as usize),
                        "V8 extent lookahead lost direct-I/O alignment"
                    );
                    drop(state_guard);
                    drop(_extent_state_timer);

                    let initialization_started = Instant::now();
                    initialize_v8_extent_chunk(&state, initialized_start, initialized_end, || {
                        zeros.zero_range(0, chunk_len);
                        data_file
                            .write_all_at(zeros.initialized_slice(0, chunk_len), initialized_start)
                            .with_context(|| {
                                format!("initialize V8 WAL extent at offset {initialized_start}")
                            })
                    })?;

                    #[allow(unused_mut)]
                    let (extent_state_timer, mut state_guard) =
                        mutex_diagnostics::lock(&state, MutexLockKind::ExtentState);
                    ensure!(
                        state_guard.prepared_end == initialized_end,
                        "V8 extent lookahead failed to publish its initialized prefix"
                    );
                    ensure!(
                        state_guard.allocated_end >= initialized_end,
                        "V8 extent lookahead initialized beyond allocated space"
                    );
                    #[cfg(feature = "bench")]
                    {
                        let initialization_elapsed_ns =
                            u64::try_from(initialization_started.elapsed().as_nanos())
                                .unwrap_or(u64::MAX);
                        state_guard.initialization_chunks =
                            state_guard.initialization_chunks.saturating_add(1);
                        state_guard.initialized_bytes = state_guard
                            .initialized_bytes
                            .saturating_add(initialized_end - initialized_start);
                        state_guard.initialization_elapsed_ns = state_guard
                            .initialization_elapsed_ns
                            .saturating_add(initialization_elapsed_ns);
                        state_guard.elapsed_ns = state_guard
                            .elapsed_ns
                            .saturating_add(initialization_elapsed_ns);
                        record_lookahead_worker_cpu(&mut state_guard, &mut worker_cpu_checkpoint);
                    }
                    #[cfg(not(feature = "bench"))]
                    let _ = initialization_started;
                    drop(state_guard);
                    drop(extent_state_timer);
                    prepared.notify_all();
                }
            }
        }

        Ok(())
    }
}

fn initialize_v8_extent_chunk(
    state: &Mutex<WalV8ExtentPreallocationState>,
    initialized_start: u64,
    initialized_end: u64,
    write_chunk: impl FnOnce() -> Result<()>,
) -> Result<()> {
    ensure!(
        initialized_end > initialized_start,
        "V8 extent initialization chunk is empty"
    );
    write_chunk()?;

    let (_extent_state_timer, mut state) =
        mutex_diagnostics::lock(state, MutexLockKind::ExtentState);
    ensure!(
        state.prepared_end == initialized_start,
        "V8 extent lookahead lost its serialized initialization prefix"
    );
    ensure!(
        state.allocated_end >= initialized_end,
        "V8 extent lookahead initialized beyond allocated space"
    );
    state.prepared_end = initialized_end;

    Ok(())
}

fn round_up_extent_charge(bytes: u64, unit: u64) -> Result<u64> {
    ensure!(unit > 0, "V8 source allocation unit is zero");
    bytes
        .checked_add(unit - 1)
        .context("V8 extent source charge overflows")?
        .checked_div(unit)
        .context("V8 source allocation unit is zero")?
        .checked_mul(unit)
        .context("V8 extent source charge overflows")
}

fn fallocate_wal_range(file: &File, start: u64, end: u64) -> Result<()> {
    ensure!(end > start, "V8 fallocate range is empty");
    let offset = libc::off_t::try_from(start).context("V8 fallocate offset exceeds off_t")?;
    let len = libc::off_t::try_from(end - start).context("V8 fallocate length exceeds off_t")?;
    // SAFETY: both values are checked, and the owned descriptor remains live.
    let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, offset, len) };
    if result != 0 {
        return Err(io::Error::last_os_error()).context("V8 fallocate syscall failed");
    }

    Ok(())
}

fn fallocate_sliding_extent_with_fallback(
    prepared_start: u64,
    required_end: u64,
    target_end: u64,
    window_preallocation_disabled: &mut bool,
    mut fallocate: impl FnMut(u64, u64) -> Result<()>,
) -> Result<u64> {
    ensure!(
        prepared_start < required_end,
        "V8 fallocate prefix is empty"
    );
    ensure!(
        target_end >= required_end,
        "V8 fallocate window misses the required prefix"
    );

    let window_end = if *window_preallocation_disabled {
        required_end
    } else {
        target_end
    };
    match fallocate(prepared_start, window_end) {
        Ok(()) => Ok(window_end),
        Err(window_error)
            if window_end > required_end && is_fallocate_capacity_error(&window_error) =>
        {
            *window_preallocation_disabled = true;
            fallocate(prepared_start, required_end).with_context(|| {
                format!(
                    "fallocate required V8 WAL prefix after speculative window failure: {window_error:#}"
                )
            })?;
            Ok(required_end)
        }
        Err(error) => Err(error).context("fallocate sliding V8 WAL extent"),
    }
}

fn is_fallocate_capacity_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<io::Error>().is_some_and(|error| {
            matches!(
                error.raw_os_error(),
                Some(libc::ENOSPC) | Some(libc::EDQUOT) | Some(libc::EFBIG)
            )
        })
    })
}

fn is_o_direct_file(file: &File) -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the descriptor is valid for the lifetime of `file`, and
        // F_GETFL does not dereference its third argument.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        ensure!(
            flags >= 0,
            "failed to query V8 WAL handle flags with F_GETFL: {}",
            io::Error::last_os_error()
        );

        Ok(flags & libc::O_DIRECT != 0)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        Ok(false)
    }
}

fn is_ext_family_filesystem(file: &File) -> bool {
    #[cfg(target_os = "linux")]
    {
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: file is live, and stat points to writable storage of the required size.
        if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return false;
        }

        // SAFETY: a successful fstatfs initialized stat.
        unsafe { stat.assume_init() }.f_type == libc::EXT4_SUPER_MAGIC
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        false
    }
}

#[cfg(feature = "bench")]
impl Drop for WalV8SlidingExtentPreparation {
    fn drop(&mut self) {
        self.request_sender.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            log::error!("V8 background extent initializer panicked");
        }
    }
}

#[cfg(feature = "bench")]
fn run_v8_sliding_extent_initializer(
    data_file: File,
    requests: mpsc::Receiver<u64>,
    state: Arc<Mutex<WalV8SlidingExtentPreparationState>>,
    prepared: Arc<Condvar>,
    start_offset: u64,
    max_file_size: u64,
    window_bytes: u64,
) -> Result<()> {
    let chunk_bytes = WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64;
    let mut zeros = DirectBuf::new(WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES);
    while let Ok(requested_end) = requests.recv() {
        ensure!(
            requested_end <= max_file_size,
            "V8 background extent request exceeds the WAL segment limit"
        );
        let allocation_target_end =
            sliding_extent_target_end(start_offset, max_file_size, window_bytes, requested_end)?;
        let relative_end = requested_end
            .checked_sub(start_offset)
            .context("V8 background extent request precedes its initial cursor")?;
        let initialization_chunks = relative_end
            .checked_add(chunk_bytes - 1)
            .context("V8 background extent chunk count overflows")?
            / chunk_bytes;
        let initialization_target_end = start_offset
            .checked_add(
                initialization_chunks
                    .checked_mul(chunk_bytes)
                    .context("V8 background extent initialization length overflows")?,
            )
            .context("V8 background extent initialization end overflows")?
            .min(allocation_target_end);

        loop {
            let (prepared_start, allocated_end, chunk_end) = {
                let state = state.lock();
                if requested_end <= state.prepared_end {
                    break;
                }
                let chunk_end = state
                    .prepared_end
                    .checked_add(chunk_bytes)
                    .context("V8 background extent chunk end overflows")?
                    .min(initialization_target_end);
                (state.prepared_end, state.allocated_end, chunk_end)
            };

            if chunk_end > allocated_end {
                let next_allocated_end = allocated_end
                    .checked_add(window_bytes)
                    .context("V8 background extent window end overflows")?
                    .min(allocation_target_end)
                    .min(max_file_size);
                ensure!(
                    next_allocated_end > allocated_end,
                    "V8 background extent allocation did not advance"
                );
                let offset = libc::off_t::try_from(allocated_end)
                    .context("V8 background extent offset exceeds off_t")?;
                let len = libc::off_t::try_from(next_allocated_end - allocated_end)
                    .context("V8 background extent length exceeds off_t")?;
                let fallocate_started = Instant::now();
                // SAFETY: the checked range is within the configured WAL file
                // cap and the owned descriptor remains live for the syscall.
                let result = unsafe { libc::fallocate(data_file.as_raw_fd(), 0, offset, len) };
                ensure!(
                    result == 0,
                    "V8 benchmark sliding fallocate failed: {}",
                    io::Error::last_os_error()
                );
                let fallocate_elapsed_ns =
                    u64::try_from(fallocate_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                let mut state = state.lock();
                ensure!(
                    state.allocated_end == allocated_end,
                    "V8 background extent allocation lost its serialized prefix"
                );
                state.allocated_end = next_allocated_end;
                state.fallocate_calls = state.fallocate_calls.saturating_add(1);
                state.allocated_bytes = state
                    .allocated_bytes
                    .saturating_add(next_allocated_end - allocated_end);
                state.fallocate_elapsed_ns = state
                    .fallocate_elapsed_ns
                    .saturating_add(fallocate_elapsed_ns);
                state.elapsed_ns = state.elapsed_ns.saturating_add(fallocate_elapsed_ns);
            }

            let initialization_started = Instant::now();
            let chunk_len = usize::try_from(chunk_end - prepared_start)?;
            ensure!(
                prepared_start.is_multiple_of(4096) && chunk_len.is_multiple_of(4096),
                "V8 background extent initialization lost direct-I/O alignment"
            );
            zeros.zero_range(0, chunk_len);
            data_file
                .write_all_at(zeros.initialized_slice(0, chunk_len), prepared_start)
                .with_context(|| format!("initialize V8 WAL extent at offset {prepared_start}"))?;
            let initialization_elapsed_ns =
                u64::try_from(initialization_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

            let mut state = state.lock();
            ensure!(
                state.prepared_end == prepared_start,
                "V8 background extent preparation lost its serialized prefix"
            );
            ensure!(
                state.allocated_end >= chunk_end,
                "V8 background extent initialization exceeded its allocated prefix"
            );
            state.prepared_end = chunk_end;
            state.initialization_chunks = state.initialization_chunks.saturating_add(1);
            state.initialized_bytes = state
                .initialized_bytes
                .saturating_add(chunk_end - prepared_start);
            state.initialization_elapsed_ns = state
                .initialization_elapsed_ns
                .saturating_add(initialization_elapsed_ns);
            state.elapsed_ns = state.elapsed_ns.saturating_add(initialization_elapsed_ns);
            prepared.notify_all();
        }
    }

    Ok(())
}

#[cfg(feature = "bench")]
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[cfg(feature = "bench")]
fn coalesce_budget_override_from_environment() -> Result<Option<u64>> {
    let Some(value) = std::env::var_os("TOYKV_WAL_V8_COALESCE_BUDGET_US") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .context("TOYKV_WAL_V8_COALESCE_BUDGET_US is not valid UTF-8")?;
    let budget_us = value.parse::<u64>().with_context(|| {
        format!("invalid TOYKV_WAL_V8_COALESCE_BUDGET_US {value:?}; expected `100` or `400`")
    })?;
    ensure!(
        matches!(budget_us, 100 | 400),
        "invalid TOYKV_WAL_V8_COALESCE_BUDGET_US {value:?}; expected `100` or `400`"
    );

    Ok(Some(budget_us))
}

#[cfg(feature = "bench")]
fn point_frontier_credit_batch_from_environment() -> Result<bool> {
    let Some(value) = std::env::var_os(WAL_V8_POINT_FRONTIER_CREDIT_BATCH_ENV) else {
        return Ok(false);
    };
    let value = value
        .to_str()
        .with_context(|| format!("{WAL_V8_POINT_FRONTIER_CREDIT_BATCH_ENV} is not valid UTF-8"))?;
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => anyhow::bail!(
            "invalid {WAL_V8_POINT_FRONTIER_CREDIT_BATCH_ENV} {value:?}; expected `0` or `1`"
        ),
    }
}

#[cfg(feature = "bench")]
fn point_buffer_preallocation_from_environment() -> Result<bool> {
    let Some(value) = std::env::var_os("TOYKV_BENCH_V8_POINT_BUFFER_PREALLOC") else {
        return Ok(false);
    };
    let value = value
        .to_str()
        .context("TOYKV_BENCH_V8_POINT_BUFFER_PREALLOC is not valid UTF-8")?;
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => anyhow::bail!(
            "invalid TOYKV_BENCH_V8_POINT_BUFFER_PREALLOC {value:?}; expected `0` or `1`"
        ),
    }
}

struct WalV8ParallelFrontier {
    frontier: WalV8Frontier,
    offset: u64,
    digest: [u8; 32],
    last_sync_latency: Option<Duration>,
}

struct PendingV8DataWrite {
    file_offset: u64,
    buffer: WalV8DataBuffer,
    #[cfg(feature = "bench")]
    frame_ready_at: Option<Instant>,
}

/// V8 accounts DATA frame buffers per admitted ticket, so completed buffers
/// must be dropped rather than retained in the generic worker's reusable pool.
struct WalV8DataBuffer(DirectBuf);

impl Deref for WalV8DataBuffer {
    type Target = DirectBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WalV8DataBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl WorkerBuffer for WalV8DataBuffer {
    fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn cap(&self) -> usize {
        self.0.cap()
    }

    fn retire(self, _pool: &ArrayQueue<Self>) {}
}

struct WalV8SubmissionState {
    next_ticket: u64,
    next_file_offset: Option<u64>,
    pending: BTreeMap<u64, PendingV8DataWrite>,
    frontier_offsets: BTreeSet<u64>,
    submit_any_ready_ticket: bool,
    packing: bool,
    #[cfg(test)]
    group_wait_cutoff: Option<u64>,
    #[cfg(test)]
    /// Explicit selected-ticket and write-buffer `Vec` constructions in DATA submission.
    /// Does not count the required FRONTIER offset list or test-only snapshots.
    temporary_ticket_and_write_vec_constructions: usize,
    #[cfg(test)]
    submitted_group_snapshots: Vec<WalV8SubmittedGroupSnapshot>,
    #[cfg(feature = "bench")]
    data_submit_hol: Option<WalV8DataSubmitHol>,
}

enum WalV8ProgressWake {
    Control,
    TimedOut,
    #[cfg(feature = "bench")]
    Completion(DirectGroupWriteCompletion),
}

impl WalV8ProgressWake {
    fn is_signaled(&self) -> bool {
        match self {
            Self::Control => true,
            Self::TimedOut => false,
            #[cfg(feature = "bench")]
            Self::Completion(_) => true,
        }
    }

    fn wait_status(&self) -> WalV8ProgressTokenWait {
        if self.is_signaled() {
            WalV8ProgressTokenWait::Signaled
        } else {
            WalV8ProgressTokenWait::TimedOut
        }
    }
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalV8ProgressChannelClosed {
    Control,
    Completion,
}

/// Prefer a queued control token before looking at the DATA queue. If DATA is
/// selected first while a control token races in, the token remains available
/// for the pre-scan drain and subsequent wait.
#[cfg(feature = "bench")]
fn direct_completion_event(
    completion: DirectGroupWriteCompletion,
) -> Result<WalV8CompletedGroupEvent> {
    if let Some(error) = completion.result.error.as_deref() {
        anyhow::bail!("Cursor direct completion queue received failed DATA result: {error}");
    }
    Ok(WalV8CompletedGroupEvent {
        group_id: completion.result.group_id,
        tickets: completion.result.tickets,
        write_count: completion.result.write_count,
        write_bytes: completion.result.write_bytes,
        completion_observed_at: completion.completion_observed_at,
    })
}

#[cfg(feature = "bench")]
fn drain_direct_completion_queue(
    receiver: &Receiver<DirectGroupWriteCompletion>,
    batch: &mut Vec<WalV8CompletedGroupEvent>,
    limit: usize,
    shutdown_started: bool,
) -> Result<()> {
    for _ in 0..limit {
        match receiver.try_recv() {
            Ok(completion) => batch.push(direct_completion_event(completion)?),
            Err(crossbeam_channel::TryRecvError::Empty) => break,
            Err(crossbeam_channel::TryRecvError::Disconnected) if shutdown_started => break,
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                anyhow::bail!("v8 direct DATA completion queue disconnected before teardown");
            }
        }
    }
    Ok(())
}

#[cfg(feature = "bench")]
fn wait_for_direct_completion_or_control(
    tokens: &Receiver<()>,
    completions: &Receiver<DirectGroupWriteCompletion>,
    timeout: Duration,
) -> std::result::Result<WalV8ProgressWake, WalV8ProgressChannelClosed> {
    wait_for_direct_completion_or_control_with_hook(tokens, completions, timeout, || {})
}

#[cfg(feature = "bench")]
fn wait_for_direct_completion_or_control_with_hook(
    tokens: &Receiver<()>,
    completions: &Receiver<DirectGroupWriteCompletion>,
    timeout: Duration,
    before_select: impl FnOnce(),
) -> std::result::Result<WalV8ProgressWake, WalV8ProgressChannelClosed> {
    let control_disconnected = match tokens.try_recv() {
        Ok(()) => return Ok(WalV8ProgressWake::Control),
        Err(crossbeam_channel::TryRecvError::Empty) => false,
        Err(crossbeam_channel::TryRecvError::Disconnected) => true,
    };

    match completions.try_recv() {
        Ok(completion) => return Ok(WalV8ProgressWake::Completion(completion)),
        Err(crossbeam_channel::TryRecvError::Disconnected) => {
            return Err(WalV8ProgressChannelClosed::Completion);
        }
        Err(crossbeam_channel::TryRecvError::Empty) => {}
    }
    if control_disconnected {
        return Err(WalV8ProgressChannelClosed::Control);
    }

    let mut selection = crossbeam_channel::Select::new();
    let control_operation = selection.recv(tokens);
    let completion_operation = selection.recv(completions);
    before_select();
    let selected = match selection.select_timeout(timeout) {
        Ok(selected) => selected,
        Err(_) => return Ok(WalV8ProgressWake::TimedOut),
    };
    if selected.index() == control_operation {
        selected
            .recv(tokens)
            .map(|()| WalV8ProgressWake::Control)
            .map_err(|_| WalV8ProgressChannelClosed::Control)
    } else {
        debug_assert_eq!(selected.index(), completion_operation);
        selected
            .recv(completions)
            .map(WalV8ProgressWake::Completion)
            .map_err(|_| WalV8ProgressChannelClosed::Completion)
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct WalV8SubmittedGroupSnapshot {
    tickets: std::ops::Range<u64>,
    allocation_range: std::ops::Range<u64>,
    data_ranges: Vec<std::ops::Range<u64>>,
    frontier_offsets: Vec<u64>,
}

#[cfg(feature = "bench")]
struct WalV8DataSubmitHol {
    started_at: Instant,
    last_updated_at: Instant,
    blocked_successors: u64,
    successor_ticket_wait_ns: u64,
    max_successors: u64,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum WalV8ProgressWaitTestEvent {
    BeforeWait,
    AfterWait(WalV8ProgressTokenWait),
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum WalV8CompletionReapTestEvent {
    BeforeReap,
    AfterReap,
}

#[cfg(test)]
type WalV8ProgressWaitHook = Arc<dyn Fn(WalV8ProgressWaitTestEvent) + Send + Sync>;

#[cfg(test)]
type WalV8CompletionReapHook = Arc<dyn Fn(WalV8CompletionReapTestEvent) + Send + Sync>;

#[cfg(test)]
type WalV8SharedCompletionReapHook = Arc<Mutex<Option<WalV8CompletionReapHook>>>;

fn benchmark_submits_any_ready_ticket() -> Result<bool> {
    #[cfg(feature = "bench")]
    {
        const SUBMIT_ORDER_ENV: &str = "TOYKV_BENCH_V8_DATA_SUBMIT_ORDER";
        let Some(value) = std::env::var_os(SUBMIT_ORDER_ENV) else {
            return Ok(false);
        };
        let value = value
            .into_string()
            .map_err(|_| anyhow::anyhow!("{SUBMIT_ORDER_ENV} is not valid UTF-8"))?;
        match value.as_str() {
            "ordered" => Ok(false),
            "ready" => Ok(true),
            _ => anyhow::bail!("{SUBMIT_ORDER_ENV} must be 'ordered' or 'ready'"),
        }
    }

    #[cfg(not(feature = "bench"))]
    {
        Ok(false)
    }
}

/// DATA submission over the repository's bounded io_uring worker. Production
/// submits only contiguous ticket ranges. A benchmark-only mode can submit
/// each ready ticket independently at its preassigned file offset.
struct WalV8ParallelIo {
    client: IoWorkerClient<WalV8DataBuffer>,
    worker: Mutex<Option<IoWorker<WalV8DataBuffer>>>,
    submission: Mutex<WalV8SubmissionState>,
    frames_ready: Condvar,
    #[cfg(test)]
    group_join_micros: std::sync::atomic::AtomicU64,
    coordinator: WalV8ParallelCoordinator,
    progress: Arc<crate::wal::parallel_worker::WalSyncProgress>,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
    #[cfg(feature = "bench")]
    hol_diagnostics_enabled: std::sync::atomic::AtomicBool,
    #[cfg(feature = "bench")]
    singleton_submission_enabled: bool,
    extent_preallocator: Option<Arc<WalV8ExtentPreallocator>>,
    #[cfg(feature = "bench")]
    extent_preparation: Option<Arc<WalV8SlidingExtentPreparation>>,
    completion_events: Option<Receiver<WalV8CompletedGroupEvent>>,
    #[cfg(feature = "bench")]
    direct_completions: Option<Receiver<DirectGroupWriteCompletion>>,
    #[cfg(feature = "bench")]
    select_completion_wake: bool,
    #[cfg(test)]
    progress_wait_hook: Mutex<Option<WalV8ProgressWaitHook>>,
    #[cfg(test)]
    completion_reap_hook: WalV8SharedCompletionReapHook,
    progress_tokens: Option<Receiver<()>>,
    shutdown_started: Arc<AtomicBool>,
    completion_thread: Mutex<Option<thread::JoinHandle<()>>>,
}

struct WalV8ParallelIoConfig {
    #[cfg(feature = "bench")]
    extent_preparation: Option<Arc<WalV8SlidingExtentPreparation>>,
    direct_completion_enabled: bool,
    #[cfg(feature = "bench")]
    direct_completion_wake_mode: WalV8DirectCompletionWakeMode,
    singleton_submission_enabled: bool,
    #[cfg(all(test, feature = "bench"))]
    worker_group_limit: Option<usize>,
}

impl WalV8ParallelIo {
    fn spawn(
        worker_file: File,
        coordinator: WalV8ParallelCoordinator,
        first_ticket: u64,
        extent_preallocator: Option<Arc<WalV8ExtentPreallocator>>,
        config: WalV8ParallelIoConfig,
    ) -> Result<Arc<Self>> {
        #[cfg(feature = "bench")]
        let extent_preparation = config.extent_preparation;
        let direct_completion_enabled = config.direct_completion_enabled;
        #[cfg(feature = "bench")]
        let direct_completion_wake_mode = config.direct_completion_wake_mode;
        #[cfg(feature = "bench")]
        let select_completion_wake =
            direct_completion_wake_mode == WalV8DirectCompletionWakeMode::Select;
        let singleton_submission_enabled = config.singleton_submission_enabled;
        #[cfg(all(test, feature = "bench"))]
        let worker_group_limit = config.worker_group_limit;

        ensure!(
            !direct_completion_enabled || coordinator.progress_cursor_enabled(),
            "Cursor direct completion requires Cursor progress mode"
        );
        ensure!(
            !singleton_submission_enabled || coordinator.progress_cursor_enabled(),
            "Cursor singleton submission requires Cursor progress mode"
        );
        #[cfg(feature = "bench")]
        ensure!(
            !select_completion_wake
                || (direct_completion_enabled && coordinator.progress_cursor_enabled()),
            "select completion wake requires Cursor direct completion"
        );
        #[cfg(not(feature = "bench"))]
        ensure!(
            !direct_completion_enabled && !singleton_submission_enabled,
            "Cursor direct completion and singleton submission are available only with the bench feature"
        );

        let (completion_event_sender, completion_events) =
            if coordinator.progress_ring_enabled() && !direct_completion_enabled {
                let capacity = coordinator.completion_queue_capacity();
                ensure!(capacity > 0, "v8 completion event queue capacity is empty");
                let (sender, receiver) = crossbeam_channel::bounded(capacity);
                (Some(sender), Some(receiver))
            } else {
                (None, None)
            };
        let progress_tokens = coordinator.take_progress_token_receiver()?;
        let shutdown_started = Arc::new(AtomicBool::new(false));
        let submit_any_ready_ticket = benchmark_submits_any_ready_ticket()?;
        let worker_file = Arc::new(worker_file);
        let buffer_pool = Arc::new(ArrayQueue::<WalV8DataBuffer>::new(1));
        #[cfg(feature = "bench")]
        let direct_completion_handle =
            coordinator.worker_completion_handle_with_wake_mode(direct_completion_wake_mode);
        #[cfg(feature = "bench")]
        let (mut worker, direct_completions) = if direct_completion_enabled {
            let capacity = coordinator.completion_queue_capacity();
            ensure!(capacity > 0, "v8 direct completion queue capacity is empty");
            let (sender, receiver) = crossbeam_channel::bounded(capacity);
            #[cfg(test)]
            let worker = if let Some(group_limit) = worker_group_limit {
                IoWorker::spawn_cursor_direct_completions_with_group_limit(
                    worker_file,
                    buffer_pool,
                    None,
                    submit_any_ready_ticket,
                    sender,
                    direct_completion_handle,
                    group_limit,
                )?
            } else {
                IoWorker::spawn_cursor_direct_completions(
                    worker_file,
                    buffer_pool,
                    None,
                    submit_any_ready_ticket,
                    sender,
                    direct_completion_handle,
                )?
            };
            #[cfg(not(test))]
            let worker = IoWorker::spawn_cursor_direct_completions(
                worker_file,
                buffer_pool,
                None,
                submit_any_ready_ticket,
                sender,
                direct_completion_handle,
            )?;
            (worker, Some(receiver))
        } else {
            let worker = if submit_any_ready_ticket {
                IoWorker::spawn_out_of_order(worker_file, buffer_pool, None)?
            } else {
                IoWorker::spawn(worker_file, buffer_pool, None)?
            };
            (worker, None)
        };
        #[cfg(not(feature = "bench"))]
        let mut worker = if submit_any_ready_ticket {
            IoWorker::spawn_out_of_order(worker_file, buffer_pool, None)?
        } else {
            IoWorker::spawn(worker_file, buffer_pool, None)?
        };
        let client = worker.client();
        let progress = worker.sync_progress();
        progress.initialize_frontiers(first_ticket);
        // V8 reports one commit group per embedded FRONTIER/sync cycle. The
        // worker still contributes SQE/CQE and eventfd metrics, but its DATA
        // submission groups are not V8 durability groups.
        progress.suppress_commit_group_metrics();
        let completions = if direct_completion_enabled {
            None
        } else {
            Some(worker.take_completions())
        };
        #[cfg(feature = "bench")]
        let completion_progress = Arc::clone(&progress);
        let completion_coordinator = coordinator.clone();
        let completion_shutdown_started = Arc::clone(&shutdown_started);
        let completion_event_sender_for_thread = completion_event_sender;
        #[cfg(test)]
        let completion_reap_hook: WalV8SharedCompletionReapHook = Arc::new(Mutex::new(None));
        #[cfg(test)]
        let completion_reap_hook_for_thread = Arc::clone(&completion_reap_hook);
        let completion_thread = if let Some(completions) = completions {
            Some(
                thread::Builder::new()
                    .name("wal-v8-completion-reaper".to_owned())
                    .spawn(move || {
                        let _exit_guard = WalV8CompletionReaperExitGuard {
                            coordinator: completion_coordinator.clone(),
                            shutdown_started: completion_shutdown_started,
                        };
                        #[cfg(feature = "bench")]
                        let mut completion_timeline_enabled = None;
                        while let Ok(result) = completions.recv() {
                            #[cfg(feature = "bench")]
                            let completion_observed_at =
                                completion_coordinator.capture_data_completion_time();
                            #[cfg(feature = "bench")]
                            let timeline_enabled =
                                *completion_timeline_enabled.get_or_insert_with(|| {
                                    completion_progress.completion_timeline_sample_every() > 0
                                });
                            #[cfg(feature = "bench")]
                            let reaper_recv_at = timeline_enabled
                                .then(|| completion_observed_at.unwrap_or_else(Instant::now));
                            #[cfg(feature = "bench")]
                            let mut cpu_sample = {
                                let profile = completion_progress.profile();
                                let work_items =
                                    result.tickets.end.saturating_sub(result.tickets.start);
                                profile.as_ref().and_then(|profile| {
                                    profile.begin_wal_cpu_thread_sample(
                                        crate::mem_table::WalCpuThreadRole::V8CompletionReaper,
                                        work_items,
                                    )
                                })
                            };
                            #[cfg(feature = "bench")]
                            let timeline = reaper_recv_at.and_then(|received_at| {
                                completion_progress
                                    .take_completion_timeline(result.group_id, received_at)
                            });
                            #[cfg(test)]
                            if let Some(hook) = completion_reap_hook_for_thread.lock().clone() {
                                hook(WalV8CompletionReapTestEvent::BeforeReap);
                            }
                            let reap_result = {
                                #[cfg(feature = "bench")]
                                {
                                    reap_v8_group_result(
                                        &completion_coordinator,
                                        result,
                                        completion_event_sender_for_thread.as_ref(),
                                        completion_observed_at,
                                        timeline,
                                    )
                                }
                                #[cfg(not(feature = "bench"))]
                                {
                                    reap_v8_group_result(
                                        &completion_coordinator,
                                        result,
                                        completion_event_sender_for_thread.as_ref(),
                                    )
                                }
                            };
                            #[cfg(test)]
                            if reap_result.is_ok()
                                && let Some(hook) = completion_reap_hook_for_thread.lock().clone()
                            {
                                hook(WalV8CompletionReapTestEvent::AfterReap);
                            }
                            #[cfg(feature = "bench")]
                            if let Some(sample) = cpu_sample.as_mut()
                                && reap_result.is_ok()
                            {
                                sample.mark_completed();
                            }
                            if let Err(error) = reap_result {
                                log::error!("v8 WAL completion reaper failed: {error:#}");
                                completion_coordinator.poison();
                            }
                        }
                    })
                    .context("failed to spawn v8 WAL completion reaper")?,
            )
        } else {
            None
        };

        Ok(Arc::new(Self {
            client,
            worker: Mutex::new(Some(worker)),
            submission: Mutex::new(WalV8SubmissionState {
                next_ticket: first_ticket,
                next_file_offset: None,
                pending: BTreeMap::new(),
                frontier_offsets: BTreeSet::new(),
                submit_any_ready_ticket,
                packing: false,
                #[cfg(test)]
                group_wait_cutoff: None,
                #[cfg(test)]
                temporary_ticket_and_write_vec_constructions: 0,
                #[cfg(test)]
                submitted_group_snapshots: Vec::new(),
                #[cfg(feature = "bench")]
                data_submit_hol: None,
            }),
            frames_ready: Condvar::new(),
            #[cfg(test)]
            group_join_micros: std::sync::atomic::AtomicU64::new(
                DATA_GROUP_JOIN_BUDGET.as_micros() as u64,
            ),
            coordinator,
            progress,
            #[cfg(feature = "bench")]
            profile: arc_swap::ArcSwapOption::empty(),
            #[cfg(feature = "bench")]
            hol_diagnostics_enabled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "bench")]
            singleton_submission_enabled,
            extent_preallocator,
            #[cfg(feature = "bench")]
            extent_preparation,
            completion_events,
            #[cfg(feature = "bench")]
            direct_completions,
            #[cfg(feature = "bench")]
            select_completion_wake,
            #[cfg(test)]
            progress_wait_hook: Mutex::new(None),
            #[cfg(test)]
            completion_reap_hook,
            progress_tokens,
            shutdown_started,
            completion_thread: Mutex::new(completion_thread),
        }))
    }

    fn advance_and_snapshot(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_after_wake(
            driver,
            capture_monotonic_ns,
            WalV8ProgressWake::Control,
        )
    }

    fn advance_and_snapshot_after_wake(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
        wake: WalV8ProgressWake,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_inner(
            driver,
            capture_monotonic_ns,
            wake,
            #[cfg(feature = "bench")]
            None,
        )
    }

    #[cfg(feature = "bench")]
    fn advance_and_snapshot_with_timing(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
        timing: &mut WalV8CursorCoalescingTiming,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_after_wake_with_timing(
            driver,
            capture_monotonic_ns,
            WalV8ProgressWake::Control,
            timing,
        )
    }

    #[cfg(feature = "bench")]
    fn advance_and_snapshot_after_wake_with_timing(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
        wake: WalV8ProgressWake,
        timing: &mut WalV8CursorCoalescingTiming,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_inner(driver, capture_monotonic_ns, wake, Some(timing))
    }

    fn advance_and_snapshot_inner(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
        wake: WalV8ProgressWake,
        #[cfg(feature = "bench")] mut timing: Option<&mut WalV8CursorCoalescingTiming>,
    ) -> Result<WalV8FrontierReadySnapshot> {
        #[cfg(not(feature = "bench"))]
        let _ = wake;
        ensure!(
            self.coordinator.owns_durability_driver(driver),
            "v8 progress scan is not owned by the active durability driver"
        );
        #[cfg(feature = "bench")]
        let has_direct_completion_queue = self.direct_completions.is_some();
        #[cfg(not(feature = "bench"))]
        let has_direct_completion_queue = false;
        if self.completion_events.is_none() && !has_direct_completion_queue {
            return Ok(if capture_monotonic_ns {
                #[cfg(feature = "bench")]
                {
                    self.coordinator.frontier_ready_snapshot_with_monotonic_ns()
                }
                #[cfg(not(feature = "bench"))]
                {
                    self.coordinator.frontier_ready_snapshot()
                }
            } else {
                self.coordinator.frontier_ready_snapshot()
            });
        }

        // The capacity-one token is persistent: consuming it before scanning
        // means a publisher racing after the scan leaves another token for the
        // subsequent park.
        if let Some(tokens) = &self.progress_tokens {
            consume_progress_token_before_scan(tokens);
        }

        let mut batch = driver
            .completion_batch
            .take()
            .context("cursor durability driver completion buffer is missing")?;
        #[cfg(feature = "bench")]
        let completion_drain_wall_started = timing.as_ref().map(|_| Instant::now());
        #[cfg(feature = "bench")]
        let completion_drain_cpu_started = timing.as_ref().map(|_| digest::thread_cpu_time_ns());
        let drain_result = (|| -> Result<()> {
            #[cfg(feature = "bench")]
            if let WalV8ProgressWake::Completion(completion) = wake {
                self.push_direct_completion_event(completion, &mut batch)?;
            }

            let capacity = self.coordinator.completion_queue_capacity();
            ensure!(
                batch.len() <= capacity,
                "cursor completion seed exceeds the reusable batch capacity"
            );
            let remaining_capacity = capacity - batch.len();
            if let Some(receiver) = &self.completion_events {
                self.drain_completion_events(receiver, &mut batch, remaining_capacity)
            } else {
                #[cfg(feature = "bench")]
                {
                    let Some(receiver) = self.direct_completions.as_ref() else {
                        anyhow::bail!("Cursor direct completion queue is missing")
                    };
                    self.drain_direct_completion_events(receiver, &mut batch, remaining_capacity)
                }
                #[cfg(not(feature = "bench"))]
                {
                    anyhow::bail!("Cursor direct completion is unavailable without bench feature")
                }
            }
        })();
        #[cfg(feature = "bench")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.completion_drain_wall_ns = timing.completion_drain_wall_ns.saturating_add(
                completion_drain_wall_started
                    .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                    .unwrap_or_default(),
            );
            timing.completion_drain_cpu_ns = timing.completion_drain_cpu_ns.saturating_add(
                completion_drain_cpu_started
                    .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                    .unwrap_or_default(),
            );
        }
        let snapshot = match drain_result {
            Ok(()) => {
                #[cfg(feature = "bench")]
                {
                    if let Some(timing) = timing.as_mut() {
                        self.coordinator.advance_and_snapshot_with_timing(
                            driver,
                            &batch,
                            capture_monotonic_ns,
                            &mut timing.advance,
                        )
                    } else {
                        self.coordinator
                            .advance_and_snapshot(driver, &batch, capture_monotonic_ns)
                    }
                }
                #[cfg(not(feature = "bench"))]
                {
                    self.coordinator
                        .advance_and_snapshot(driver, &batch, capture_monotonic_ns)
                }
            }
            Err(error) => Err(error),
        };
        batch.clear();
        driver.completion_batch = Some(batch);
        snapshot
    }

    fn drain_completion_events(
        &self,
        receiver: &Receiver<WalV8CompletedGroupEvent>,
        batch: &mut Vec<WalV8CompletedGroupEvent>,
        limit: usize,
    ) -> Result<()> {
        for _ in 0..limit {
            match receiver.try_recv() {
                Ok(event) => batch.push(event),
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    if !self.shutdown_started.load(Ordering::Acquire) {
                        self.coordinator.poison();
                        anyhow::bail!("v8 DATA completion queue disconnected before teardown");
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "bench")]
    fn drain_direct_completion_events(
        &self,
        receiver: &Receiver<DirectGroupWriteCompletion>,
        batch: &mut Vec<WalV8CompletedGroupEvent>,
        limit: usize,
    ) -> Result<()> {
        let result = drain_direct_completion_queue(
            receiver,
            batch,
            limit,
            self.shutdown_started.load(Ordering::Acquire),
        );
        if result.is_err() {
            self.coordinator.poison();
        }
        result
    }

    #[cfg(feature = "bench")]
    fn push_direct_completion_event(
        &self,
        completion: DirectGroupWriteCompletion,
        batch: &mut Vec<WalV8CompletedGroupEvent>,
    ) -> Result<()> {
        match direct_completion_event(completion) {
            Ok(event) => {
                batch.push(event);
                Ok(())
            }
            Err(error) => {
                self.coordinator.poison();
                Err(error)
            }
        }
    }

    fn coalesce_progress_ring(
        &self,
        driver: &mut WalV8DurabilityDriver,
        snapshot: &mut WalV8FrontierReadySnapshot,
        budget: Duration,
        capture_monotonic_ns: bool,
        #[cfg(feature = "bench")] mut timing: Option<&mut WalV8CursorCoalescingTiming>,
    ) -> Result<bool> {
        #[cfg(not(feature = "bench"))]
        let _ = (driver, capture_monotonic_ns);

        let deadline = Instant::now() + budget;
        let mut waited = false;
        while !snapshot.closed
            && let Some(remaining) = deadline.checked_duration_since(Instant::now())
        {
            waited = true;
            #[cfg(feature = "bench")]
            if let Some(timing) = timing.as_deref_mut() {
                timing.iterations = timing.iterations.saturating_add(1);
            }
            #[cfg(feature = "bench")]
            let wait_wall_started = timing.as_ref().map(|_| Instant::now());
            #[cfg(feature = "bench")]
            let wait_cpu_started = timing.as_ref().map(|_| digest::thread_cpu_time_ns());
            let wake = self.wait_for_progress(remaining)?;
            let signaled = wake.is_signaled();
            #[cfg(not(feature = "bench"))]
            let _ = wake;
            #[cfg(feature = "bench")]
            if let Some(timing) = timing.as_deref_mut() {
                timing.progress_wait_wall_ns = timing.progress_wait_wall_ns.saturating_add(
                    wait_wall_started
                        .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                        .unwrap_or_default(),
                );
                timing.progress_wait_cpu_ns = timing.progress_wait_cpu_ns.saturating_add(
                    wait_cpu_started
                        .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                        .unwrap_or_default(),
                );
            }
            #[cfg(feature = "bench")]
            let ready_before = snapshot.ready_prefix.len();
            #[cfg(feature = "bench")]
            if let Some(timing) = timing.as_deref_mut() {
                *snapshot = self.advance_and_snapshot_after_wake_with_timing(
                    driver,
                    capture_monotonic_ns,
                    wake,
                    timing,
                )?;
            } else {
                *snapshot =
                    self.advance_and_snapshot_after_wake(driver, capture_monotonic_ns, wake)?;
            }
            #[cfg(feature = "bench")]
            if let Some(timing) = timing.as_deref_mut() {
                timing.ready_tickets_gained =
                    timing.ready_tickets_gained.saturating_add(
                        snapshot.ready_prefix.len().saturating_sub(ready_before) as u64,
                    );
            }
            if !signaled
                || snapshot.ready_prefix.is_empty()
                || snapshot.ready_prefix.len() >= snapshot.admitted_ticket_count
            {
                break;
            }
        }
        Ok(waited)
    }

    fn wait_for_progress(&self, timeout: Duration) -> Result<WalV8ProgressWake> {
        let Some(tokens) = &self.progress_tokens else {
            anyhow::bail!("v8 progress wait requested while the ring is disabled");
        };
        #[cfg(test)]
        {
            let hook = self.progress_wait_hook.lock().clone();
            if let Some(hook) = hook {
                hook(WalV8ProgressWaitTestEvent::BeforeWait);
            }
        }
        #[cfg(feature = "bench")]
        let result = if self.select_completion_wake {
            let Some(completions) = self.direct_completions.as_ref() else {
                self.coordinator.poison();
                anyhow::bail!("select completion wake requires a direct completion queue");
            };
            match wait_for_direct_completion_or_control(tokens, completions, timeout) {
                Ok(wake) => Ok(wake),
                Err(_closed) if self.shutdown_started.load(Ordering::Acquire) => {
                    Ok(WalV8ProgressWake::TimedOut)
                }
                Err(WalV8ProgressChannelClosed::Control) => {
                    self.coordinator.poison();
                    anyhow::bail!("v8 progress token channel disconnected before teardown");
                }
                Err(WalV8ProgressChannelClosed::Completion) => {
                    self.coordinator.poison();
                    anyhow::bail!("v8 direct DATA completion queue disconnected before teardown");
                }
            }
        } else {
            match wait_for_progress_token(tokens, timeout) {
                WalV8ProgressTokenWait::Signaled => Ok(WalV8ProgressWake::Control),
                WalV8ProgressTokenWait::TimedOut => Ok(WalV8ProgressWake::TimedOut),
                WalV8ProgressTokenWait::Disconnected
                    if self.shutdown_started.load(Ordering::Acquire) =>
                {
                    Ok(WalV8ProgressWake::TimedOut)
                }
                WalV8ProgressTokenWait::Disconnected => {
                    self.coordinator.poison();
                    anyhow::bail!("v8 progress token channel disconnected before teardown");
                }
            }
        };
        #[cfg(not(feature = "bench"))]
        let result = match wait_for_progress_token(tokens, timeout) {
            WalV8ProgressTokenWait::Signaled => Ok(WalV8ProgressWake::Control),
            WalV8ProgressTokenWait::TimedOut => Ok(WalV8ProgressWake::TimedOut),
            WalV8ProgressTokenWait::Disconnected
                if self.shutdown_started.load(Ordering::Acquire) =>
            {
                Ok(WalV8ProgressWake::TimedOut)
            }
            WalV8ProgressTokenWait::Disconnected => {
                self.coordinator.poison();
                anyhow::bail!("v8 progress token channel disconnected before teardown");
            }
        };
        #[cfg(test)]
        {
            let hook = self.progress_wait_hook.lock().clone();
            if let Some(hook) = hook {
                #[cfg(feature = "bench")]
                let status = match &result {
                    Ok(wake) => wake.wait_status(),
                    Err(_) => WalV8ProgressTokenWait::Disconnected,
                };
                #[cfg(not(feature = "bench"))]
                let status = match &result {
                    Ok(wake) => wake.wait_status(),
                    Err(_) => WalV8ProgressTokenWait::Disconnected,
                };
                hook(WalV8ProgressWaitTestEvent::AfterWait(status));
            }
        }
        result
    }

    #[cfg(test)]
    fn set_progress_wait_hook(&self, hook: WalV8ProgressWaitHook) {
        *self.progress_wait_hook.lock() = Some(hook);
    }

    #[cfg(test)]
    fn set_completion_reap_hook(&self, hook: WalV8CompletionReapHook) {
        *self.completion_reap_hook.lock() = Some(hook);
    }

    fn ensure_extent_prepared_through(&self, required_end: u64) -> Result<()> {
        if let Some(preallocator) = &self.extent_preallocator {
            preallocator.ensure_prepared_through(required_end)?;
        }

        #[cfg(feature = "bench")]
        if let Some(preparation) = &self.extent_preparation {
            preparation.ensure_prepared_through(required_end)?;
        }

        Ok(())
    }

    fn consume_extent_source_credit(&self, start: u64, end: u64) -> Result<()> {
        if let Some(preallocator) = &self.extent_preallocator {
            preallocator.consume_source_credit(start, end)?;
        }

        Ok(())
    }

    fn request_extent_preparation_through(&self, required_end: u64) -> Result<()> {
        if let Some(preallocator) = &self.extent_preallocator
            && preallocator.strategy.uses_lookahead()
        {
            return preallocator.queue_background_preparation(required_end);
        }

        #[cfg(feature = "bench")]
        if let Some(preparation) = &self.extent_preparation
            && preparation.mode == WalExtentPreparationMode::SlidingInitialize
        {
            return preparation.queue_background_preparation(required_end);
        }

        #[cfg(not(feature = "bench"))]
        let _ = required_end;

        Ok(())
    }

    #[cfg(feature = "bench")]
    fn extent_preparation_diagnostics(&self) -> Option<WalV8SlidingExtentPreparationDiagnostics> {
        self.extent_preallocator
            .as_ref()
            .map(|preallocator| preallocator.diagnostics())
            .or_else(|| {
                self.extent_preparation
                    .as_ref()
                    .map(|preparation| preparation.diagnostics())
            })
    }

    #[cfg(feature = "bench")]
    fn set_profile(&self, profile: Arc<crate::mem_table::WriteProfile>) {
        let enabled = profile.wal_hol_diagnostics_enabled();
        self.progress.set_profile(Arc::clone(&profile));
        self.profile.store(Some(profile));
        self.hol_diagnostics_enabled
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    #[cfg(feature = "bench")]
    fn set_hol_diagnostics_enabled(&self, enabled: bool) {
        self.hol_diagnostics_enabled
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    fn register_frontier_offset(&self, offset: u64) -> Result<()> {
        ensure!(
            offset.is_multiple_of(FRAME_LEN_U64),
            "v8 FRONTIER worker offset is not frame aligned"
        );
        let mut submission = self.submission.lock();
        ensure!(
            submission
                .next_file_offset
                .is_none_or(|next_offset| offset >= next_offset),
            "v8 FRONTIER worker offset precedes submitted DATA"
        );
        if !submission.submit_any_ready_ticket {
            ensure!(
                submission.frontier_offsets.insert(offset),
                "v8 FRONTIER worker offset was registered twice"
            );
        }
        // A producer may already have queued the next DATA ticket while it
        // was waiting for this physical gap to be assigned. Revisit the
        // queue now that the gap is known; otherwise its DATA can remain
        // pending forever when no later producer publishes another ticket.
        self.frames_ready.notify_all();
        // This callback holds the coordinator's admission lock. An elected
        // packer will observe the gap after waking; otherwise drain ready
        // work directly without acquiring the coordinator in reverse order.
        if submission.submit_any_ready_ticket {
            self.drain_ready_tickets_locked(&mut submission)
                .context("submit v8 DATA after registering a FRONTIER gap")?;
        } else if !submission.packing {
            self.drain_ready_locked(&mut submission)
                .context("submit v8 DATA after registering a FRONTIER gap")?;
        }

        Ok(())
    }

    fn default_group_join_budget(&self) -> Duration {
        #[cfg(test)]
        {
            Duration::from_micros(
                self.group_join_micros
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
        }
        #[cfg(not(test))]
        {
            DATA_GROUP_JOIN_BUDGET
        }
    }

    fn publish_data(
        &self,
        ticket: u64,
        file_offset: u64,
        buffer: WalV8DataBuffer,
        frame_ready_at: Option<Instant>,
    ) -> Result<()> {
        let allocation_end = file_offset
            .checked_add(u64::try_from(buffer.len())?)
            .context("v8 worker DATA allocation end overflows")?;
        self.request_extent_preparation_through(allocation_end)?;
        #[cfg(not(feature = "bench"))]
        let _ = frame_ready_at;

        let mut submission = self.submission.lock();
        ensure!(
            ticket >= submission.next_ticket,
            "v8 DATA ticket was already submitted"
        );
        ensure!(
            submission
                .pending
                .insert(
                    ticket,
                    PendingV8DataWrite {
                        file_offset,
                        buffer,
                        #[cfg(feature = "bench")]
                        frame_ready_at,
                    }
                )
                .is_none(),
            "v8 DATA ticket was queued twice"
        );
        self.frames_ready.notify_all();
        #[cfg(feature = "bench")]
        if !submission.submit_any_ready_ticket {
            self.update_data_submit_hol(&mut submission);
        }
        if submission.submit_any_ready_ticket {
            return self.drain_ready_tickets_locked(&mut submission);
        }
        if submission.packing || !submission.pending.contains_key(&submission.next_ticket) {
            return Ok(());
        }
        // Admission publishes this atomic before any producer can encode.
        // Single-frame writes submit immediately; larger writes may be
        // coalesced with already-admitted successors.
        let head = &submission.pending[&submission.next_ticket];
        let small_head = head.buffer.len() == WAL_V8_FRAME_LEN;
        if small_head
            || self.coordinator.admitted_ticket_end() == submission.next_ticket.saturating_add(1)
        {
            return self.drain_ready_locked(&mut submission);
        }
        let join_budget = self.default_group_join_budget();
        let first_ticket = submission.next_ticket;
        submission.packing = true;
        drop(submission);

        self.pack_admitted_group(first_ticket, join_budget)
    }

    #[cfg(feature = "bench")]
    fn update_data_submit_hol(&self, state: &mut WalV8SubmissionState) {
        if !self
            .hol_diagnostics_enabled
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        let Some(profile) = self.profile.load_full() else {
            return;
        };

        let now = Instant::now();
        let blocked_successors = if state.pending.contains_key(&state.next_ticket) {
            0
        } else {
            state.pending.len() as u64
        };
        let Some(mut hol) = state.data_submit_hol.take() else {
            if blocked_successors > 0 {
                state.data_submit_hol = Some(WalV8DataSubmitHol {
                    started_at: now,
                    last_updated_at: now,
                    blocked_successors,
                    successor_ticket_wait_ns: 0,
                    max_successors: blocked_successors,
                });
            }

            return;
        };

        let elapsed_ns = now
            .saturating_duration_since(hol.last_updated_at)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        hol.successor_ticket_wait_ns = hol
            .successor_ticket_wait_ns
            .saturating_add(elapsed_ns.saturating_mul(hol.blocked_successors));
        if blocked_successors == 0 {
            let wait_ns = now
                .saturating_duration_since(hol.started_at)
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            profile.record_wal_v8_data_submit_hol(
                wait_ns,
                hol.successor_ticket_wait_ns,
                hol.max_successors,
            );

            return;
        }

        hol.last_updated_at = now;
        hol.blocked_successors = blocked_successors;
        hol.max_successors = hol.max_successors.max(blocked_successors);
        state.data_submit_hol = Some(hol);
    }

    /// Submit every currently ready ticket separately, preserving each
    /// admission-assigned offset while allowing later tickets to pass a
    /// producer whose DATA buffer is not ready yet.
    fn drain_ready_tickets_locked(&self, state: &mut WalV8SubmissionState) -> Result<()> {
        let admitted_end = self.coordinator.admitted_ticket_end();
        while let Some((&ticket, _)) = state.pending.first_key_value() {
            ensure!(
                ticket < admitted_end,
                "v8 pending DATA ticket has not been admitted"
            );
            let pending = state
                .pending
                .remove(&ticket)
                .context("v8 ready DATA ticket disappeared")?;
            let write_len = pending.buffer.len();
            let allocation_end = pending
                .file_offset
                .checked_add(u64::try_from(write_len)?)
                .context("v8 out-of-order DATA allocation end overflows")?;
            let ticket_end = ticket
                .checked_add(1)
                .context("v8 out-of-order DATA ticket overflows")?;
            self.ensure_extent_prepared_through(allocation_end)?;
            self.consume_extent_source_credit(pending.file_offset, allocation_end)?;
            let write = WriteBuffer::new(pending.buffer, pending.file_offset, write_len);
            let group = WriteGroup::new(ticket..ticket_end, vec![write])
                .map_err(|error| anyhow::anyhow!("build v8 out-of-order DATA group: {error:?}"))?;
            self.client
                .submit_group(group)
                .context("submit out-of-order v8 DATA ticket")?;
            state.next_file_offset = Some(
                state
                    .next_file_offset
                    .map_or(allocation_end, |next_offset| {
                        next_offset.max(allocation_end)
                    }),
            );
        }

        Ok(())
    }

    fn pack_admitted_group(&self, first_ticket: u64, budget: Duration) -> Result<()> {
        let mut guard = WalV8DataPackingGuard {
            io: self,
            armed: true,
        };
        let mut deadline = None;

        loop {
            // Never hold submission while entering the coordinator: FRONTIER
            // registration uses coordinator -> submission lock order.
            let window = self
                .coordinator
                .data_group_window(first_ticket, MAX_GROUP_TICKETS)?;
            ensure!(
                window.ticket_end > first_ticket,
                "v8 DATA packer has no admitted head"
            );
            let mut submission = self.submission.lock();
            ensure!(
                submission.packing && submission.next_ticket == first_ticket,
                "v8 DATA packer lost its submission cursor"
            );
            let mut ready_end = first_ticket;
            while ready_end < window.ticket_end && submission.pending.contains_key(&ready_end) {
                ready_end += 1;
            }
            if ready_end > first_ticket && ready_end < window.ticket_end && !window.closed {
                let deadline = deadline.get_or_insert_with(|| Instant::now() + budget);
                if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                    #[cfg(test)]
                    {
                        submission.group_wait_cutoff = Some(window.ticket_end);
                    }
                    self.frames_ready.wait_for(&mut submission, remaining);
                    #[cfg(test)]
                    {
                        submission.group_wait_cutoff = None;
                    }
                    drop(submission);
                    // Membership may grow while waiting for frames. Reinspect
                    // admission even after timeout, using the same deadline.
                    continue;
                }
            }
            self.submit_group_locked(&mut submission, window.ticket_end)?;
            // Bound this producer's packer tenure: drain only work currently
            // ready under this lock, then atomically release ownership so a
            // subsequent publisher can open the next admitted window.
            self.drain_ready_locked(&mut submission)?;
            submission.packing = false;
            guard.armed = false;
            return Ok(());
        }
    }

    fn drain_ready_locked(&self, state: &mut WalV8SubmissionState) -> Result<()> {
        let admitted_end = self.coordinator.admitted_ticket_end();

        loop {
            let limit = state
                .next_ticket
                .saturating_add(MAX_GROUP_TICKETS as u64)
                .min(admitted_end);
            if limit == state.next_ticket {
                return Ok(());
            }
            if self.submit_group_locked(state, limit)? == 0 {
                return Ok(());
            }
        }
    }

    /// Submit one currently contiguous ready prefix. A physical gap is
    /// accepted only when it is tiled by FRONTIER offsets published under the
    /// coordinator's admission lock.
    fn submit_group_locked(
        &self,
        state: &mut WalV8SubmissionState,
        ticket_end: u64,
    ) -> Result<usize> {
        if let Some(first) = state.pending.get(&state.next_ticket) {
            let allocation_start = state.next_file_offset.unwrap_or(first.file_offset);
            let mut cursor = allocation_start;
            let mut next_ticket = state.next_ticket;
            #[cfg(feature = "bench")]
            let singleton_submission_experiment = self.singleton_submission_enabled;
            #[cfg(not(feature = "bench"))]
            let singleton_submission_experiment = false;
            let mut selected = if singleton_submission_experiment {
                None
            } else {
                #[cfg(test)]
                {
                    state.temporary_ticket_and_write_vec_constructions += 1;
                }
                Some(Vec::with_capacity(MAX_GROUP_TICKETS))
            };
            let mut selected_count = 0usize;
            let mut frontier_offsets = Vec::new();
            #[cfg(test)]
            let mut data_ranges = Vec::with_capacity(MAX_GROUP_TICKETS);

            'collect: while next_ticket < ticket_end {
                let Some(pending) = state.pending.get(&next_ticket) else {
                    break;
                };
                while cursor < pending.file_offset {
                    if !state.frontier_offsets.contains(&cursor) {
                        break 'collect;
                    }
                    frontier_offsets.push(cursor);
                    cursor = cursor
                        .checked_add(FRAME_LEN_U64)
                        .context("v8 worker FRONTIER offset overflows")?;
                }
                ensure!(
                    cursor == pending.file_offset,
                    "v8 worker DATA offsets are not contiguous"
                );
                ensure!(
                    pending.buffer.len() > 0
                        && pending.buffer.len().is_multiple_of(WAL_V8_FRAME_LEN)
                        && pending.file_offset.is_multiple_of(FRAME_LEN_U64),
                    "v8 worker DATA buffer is not frame aligned"
                );
                #[cfg(test)]
                let write_start = cursor;
                cursor = cursor
                    .checked_add(u64::try_from(pending.buffer.len())?)
                    .context("v8 worker DATA end overflows")?;
                #[cfg(test)]
                data_ranges.push(write_start..cursor);
                if let Some(selected) = &mut selected {
                    selected.push(next_ticket);
                }
                selected_count = selected_count
                    .checked_add(1)
                    .context("v8 worker selected ticket count overflows")?;
                next_ticket = next_ticket
                    .checked_add(1)
                    .context("v8 worker ticket overflows")?;
            }
            if selected_count == 0 {
                return Ok(0);
            }

            self.ensure_extent_prepared_through(cursor)?;
            // Keep the experiment limited to singleton groups. Multi-ticket
            // groups continue through the existing Vec-backed representation
            // so this switch measures only the common one-ticket case.
            if selected.is_none() && selected_count > 1 {
                #[cfg(test)]
                {
                    state.temporary_ticket_and_write_vec_constructions += 1;
                }
                selected = Some((state.next_ticket..next_ticket).collect());
            }
            let writes: WriteGroupBuffers<WalV8DataBuffer> = if let Some(selected) = selected {
                #[cfg(test)]
                {
                    state.temporary_ticket_and_write_vec_constructions += 1;
                }
                let mut writes = Vec::with_capacity(selected.len());
                for ticket in selected {
                    writes.push(self.take_pending_write(state, ticket)?);
                }
                writes.into()
            } else {
                debug_assert_eq!(selected_count, 1);
                let mut writes = WriteGroupBuffers::with_capacity(1);
                writes.push(self.take_pending_write(state, state.next_ticket)?);
                writes
            };

            let group = WriteGroup::new_with_frontiers(
                state.next_ticket..next_ticket,
                writes,
                allocation_start..cursor,
                &frontier_offsets,
            )
            .map_err(|error| anyhow::anyhow!("build v8 DATA worker group: {error:?}"))?;
            #[cfg(test)]
            state
                .submitted_group_snapshots
                .push(WalV8SubmittedGroupSnapshot {
                    tickets: state.next_ticket..next_ticket,
                    allocation_range: allocation_start..cursor,
                    data_ranges,
                    frontier_offsets: frontier_offsets.clone(),
                });
            self.client
                .submit_group(group)
                .context("submit v8 DATA worker group")?;

            for offset in frontier_offsets {
                let removed = state.frontier_offsets.remove(&offset);
                ensure!(removed, "v8 worker FRONTIER offset disappeared");
            }
            state.next_ticket = next_ticket;
            state.next_file_offset = Some(cursor);
            #[cfg(feature = "bench")]
            if !state.submit_any_ready_ticket {
                self.update_data_submit_hol(state);
            }

            return Ok(selected_count);
        }

        Ok(0)
    }

    fn take_pending_write(
        &self,
        state: &mut WalV8SubmissionState,
        ticket: u64,
    ) -> Result<WriteBuffer<WalV8DataBuffer>> {
        let pending = state
            .pending
            .remove(&ticket)
            .context("v8 worker pending DATA disappeared")?;
        let write_len = pending.buffer.len();
        let write_end = pending
            .file_offset
            .checked_add(u64::try_from(write_len)?)
            .context("v8 DATA source-credit range overflows")?;
        // The driver transfers credit for FRONTIER gaps separately.
        self.consume_extent_source_credit(pending.file_offset, write_end)?;
        let write = WriteBuffer::new(pending.buffer, pending.file_offset, write_len);
        #[cfg(feature = "bench")]
        let write = match pending.frame_ready_at {
            Some(frame_ready_at) => write.with_ticket_timeline(ticket, frame_ready_at),
            None => write,
        };

        Ok(write)
    }
}

/// Failure or unwinding during packing must poison admitted work and release
/// the ownership flag. Buffer ownership stays with pending or worker writes.
struct WalV8DataPackingGuard<'a> {
    io: &'a WalV8ParallelIo,
    armed: bool,
}

impl Drop for WalV8DataPackingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.io.coordinator.poison();
            self.io.submission.lock().packing = false;
            self.io.frames_ready.notify_all();
        }
    }
}

fn reap_v8_group_result(
    coordinator: &WalV8ParallelCoordinator,
    result: GroupWriteResult,
    completion_event_sender: Option<&Sender<WalV8CompletedGroupEvent>>,
    #[cfg(feature = "bench")] completion_observed_at: Option<Instant>,
    #[cfg(feature = "bench")] timeline: Option<
        crate::wal::parallel_worker::WalWorkerGroupCompletionTimeline,
    >,
) -> Result<()> {
    if let Some(error) = result.error {
        coordinator.poison();
        anyhow::bail!("v8 DATA worker group {} failed: {error}", result.group_id);
    }
    #[cfg(feature = "chaos-testing")]
    crate::chaos::failpoint::wal_crash_point("wal_v8.parallel.after_data_write");

    if let Some(sender) = completion_event_sender {
        #[cfg(feature = "bench")]
        ensure!(
            timeline.is_none(),
            "v8 progress-ring completion timeline is not supported"
        );
        let event = WalV8CompletedGroupEvent {
            group_id: result.group_id,
            tickets: result.tickets,
            write_count: result.write_count,
            write_bytes: result.write_bytes,
            #[cfg(feature = "bench")]
            completion_observed_at,
        };
        sender.try_send(event).map_err(|error| {
            coordinator.poison();
            anyhow::anyhow!("v8 DATA completion event queue overflow or disconnect: {error}")
        })?;
        coordinator.notify_completion_queued();
        return Ok(());
    }

    #[cfg(feature = "bench")]
    let completion = coordinator.mark_data_group_complete_with_timeline(
        result.tickets,
        completion_observed_at,
        timeline,
    );
    #[cfg(not(feature = "bench"))]
    let completion = coordinator.mark_data_group_complete(result.tickets);

    completion.context("publish v8 DATA worker group completion")
}

struct WalV8CompletionReaperExitGuard {
    coordinator: WalV8ParallelCoordinator,
    shutdown_started: Arc<AtomicBool>,
}

impl Drop for WalV8CompletionReaperExitGuard {
    fn drop(&mut self) {
        if !self.shutdown_started.load(Ordering::Acquire) {
            self.coordinator.poison();
        }
    }
}

impl Drop for WalV8ParallelIo {
    fn drop(&mut self) {
        self.shutdown_started.store(true, Ordering::Release);
        if let Some(worker) = self.worker.get_mut().take()
            && let Err(error) = worker.close()
        {
            log::error!("failed to close v8 WAL I/O worker: {error:#}");
        }

        if let Some(completion_thread) = self.completion_thread.get_mut().take()
            && completion_thread.join().is_err()
        {
            log::error!("v8 WAL completion reaper panicked");
        }
    }
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
    io: Arc<WalV8ParallelIo>,
    data_buffer: Option<DirectBufAllocation>,
    #[cfg(feature = "bench")]
    profile: Option<Arc<crate::mem_table::WriteProfile>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WalV8ProducerCpuPhases {
    pub(crate) logical_encode_wall_ns: u64,
    pub(crate) logical_encode_cpu_ns: u64,
    pub(crate) digest_ready_wall_ns: u64,
    pub(crate) digest_ready_cpu_ns: u64,
    pub(crate) frame_encode_wall_ns: u64,
    pub(crate) frame_encode_cpu_ns: u64,
    pub(crate) frames_ready_submit_wall_ns: u64,
    pub(crate) frames_ready_submit_cpu_ns: u64,
}

impl WalV8FrameJob<'_> {
    pub(crate) fn assignment(&self) -> WalV8BatchAssignment {
        self.assignment
    }

    pub(crate) fn frame_buffer_bytes(&self) -> usize {
        self.data_buffer.as_ref().map_or(0, |buffer| buffer.cap())
    }

    pub(crate) fn finish(self) -> Result<digest::WalV8DigestMetrics> {
        self.finish_with_point_direct_frame(Self::point_direct_frame_enabled())
    }

    #[cfg(feature = "bench")]
    pub(crate) fn finish_with_cpu_attribution(
        self,
    ) -> Result<(digest::WalV8DigestMetrics, WalV8ProducerCpuPhases)> {
        let mut phases = WalV8ProducerCpuPhases::default();
        let metrics = self.finish_with_point_direct_frame_and_phases(
            Self::point_direct_frame_enabled(),
            Some(&mut phases),
        )?;
        Ok((metrics, phases))
    }

    fn finish_with_point_direct_frame(
        self,
        direct_point_enabled: bool,
    ) -> Result<digest::WalV8DigestMetrics> {
        self.finish_with_point_direct_frame_and_phases(direct_point_enabled, None)
    }

    fn point_direct_frame_enabled() -> bool {
        #[cfg(feature = "bench")]
        {
            static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
            *ENABLED.get_or_init(|| {
                std::env::var("TOYKV_BENCH_V8_POINT_DIRECT_FRAME").as_deref() == Ok("1")
            })
        }
        #[cfg(not(feature = "bench"))]
        {
            false
        }
    }

    fn finish_with_point_direct_frame_and_phases(
        mut self,
        direct_point_enabled: bool,
        #[cfg(feature = "bench")] mut phases: Option<&mut WalV8ProducerCpuPhases>,
        #[cfg(not(feature = "bench"))] _phases: Option<&mut ()>,
    ) -> Result<digest::WalV8DigestMetrics> {
        let admitted = self
            .admitted
            .take()
            .context("v8 producer job lost its admitted batch")?;
        // The selector is injectable in unit tests. Production only enables it
        // through the bench-only OnceLock above.
        let direct_point = direct_point_enabled && admitted.can_encode_point_single_frame();
        let mut early_buffer = if direct_point {
            Some(WalV8DataBuffer(
                self.data_buffer
                    .take()
                    .context("v8 producer job lost its DATA buffer")?
                    .initialize(),
            ))
        } else {
            None
        };
        #[cfg(feature = "bench")]
        let encode_started = phases
            .as_ref()
            .map(|_| (Instant::now(), digest::thread_cpu_time_ns()));
        let encode_result = if let Some(buffer) = early_buffer.as_mut() {
            admitted.encode_point_single_frame_logical_into(buffer.as_mut_slice())
        } else {
            admitted.encode()
        };
        #[cfg(feature = "bench")]
        if let (Some(phases), Some((wall_started, cpu_started))) =
            (phases.as_deref_mut(), encode_started)
        {
            phases.logical_encode_wall_ns =
                wall_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            phases.logical_encode_cpu_ns = digest::thread_cpu_time_ns().saturating_sub(cpu_started);
        }
        let mut encoded = match encode_result {
            Ok(encoded) => encoded,
            Err(error) => {
                self.coordinator.poison();
                return Err(error);
            }
        };
        #[cfg(feature = "bench")]
        let digest_started = phases
            .as_ref()
            .map(|_| (Instant::now(), digest::thread_cpu_time_ns()));
        let digest_ready = encoded.digest_ready()?;
        #[cfg(feature = "bench")]
        let ordered_link_hash_cpu_ns = self
            .coordinator
            .mark_digest_ready_before_data(digest_ready)?;
        #[cfg(not(feature = "bench"))]
        self.coordinator
            .mark_digest_ready_before_data(digest_ready)?;
        #[cfg(feature = "bench")]
        if let (Some(phases), Some((wall_started, cpu_started))) =
            (phases.as_deref_mut(), digest_started)
        {
            phases.digest_ready_wall_ns =
                wall_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            phases.digest_ready_cpu_ns = digest::thread_cpu_time_ns().saturating_sub(cpu_started);
        }
        #[cfg(feature = "bench")]
        let metrics = {
            let mut metrics = encoded.digest_metrics();
            metrics.ordered_link_hash_cpu_ns = ordered_link_hash_cpu_ns;
            metrics
        };

        let mut data_buffer = if let Some(buffer) = early_buffer {
            buffer
        } else {
            WalV8DataBuffer(
                self.data_buffer
                    .take()
                    .context("v8 producer job lost its DATA buffer")?
                    .initialize(),
            )
        };
        #[cfg(feature = "bench")]
        let frame_encode_started = phases
            .as_ref()
            .map(|_| (Instant::now(), digest::thread_cpu_time_ns()));
        if direct_point {
            if let Err(error) = encoded.finish_point_frame_in_place(data_buffer.as_mut_slice()) {
                self.coordinator.poison();
                return Err(error).context("finish v8 direct Point DATA frame");
            }
            #[cfg(feature = "bench")]
            if let Some(profile) = &self.profile {
                profile.record_v8_point_direct_frame_encoded();
            }
        }
        let data_len = if direct_point {
            encoded.frame_buffer_bytes()
        } else {
            encoded.encode_data_frames_into(data_buffer.as_mut_slice())?
        };
        ensure!(
            data_len == data_buffer.cap(),
            "v8 DATA materializer returned an unexpected frame length"
        );
        data_buffer.set_len(data_len);
        #[cfg(feature = "bench")]
        if let (Some(phases), Some((wall_started, cpu_started))) =
            (phases.as_deref_mut(), frame_encode_started)
        {
            phases.frame_encode_wall_ns =
                wall_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            phases.frame_encode_cpu_ns = digest::thread_cpu_time_ns().saturating_sub(cpu_started);
        }
        #[cfg(feature = "bench")]
        let frames_ready_started = phases
            .as_ref()
            .map(|_| (Instant::now(), digest::thread_cpu_time_ns()));
        #[cfg(feature = "bench")]
        let frame_ready_at = {
            let sample_every = self.profile.as_ref().map_or(0, |profile| {
                profile.wal_v8_completion_timeline_sample_every()
            });
            self.coordinator.mark_frames_ready_with_timeline_sample(
                self.assignment.segment_ticket,
                sample_every > 0 && self.assignment.segment_ticket.is_multiple_of(sample_every),
            )?
        };
        #[cfg(not(feature = "bench"))]
        self.coordinator
            .mark_frames_ready(self.assignment.segment_ticket)?;
        #[cfg(not(feature = "bench"))]
        let frame_ready_at = None;
        #[cfg(feature = "bench")]
        if self
            .profile
            .as_ref()
            .is_some_and(|profile| profile.wal_sync_diagnostics_enabled())
        {
            self.coordinator
                .mark_data_write_started(self.assignment.segment_ticket)?;
        }
        let ticket = self.assignment.segment_ticket;
        let previous = self.producer_guards.lock().insert(ticket, encoded);
        if previous.is_some() {
            self.coordinator.poison();
            anyhow::bail!("v8 Parallel ticket already owns a producer guard");
        }
        let write_result = self.io.publish_data(
            ticket,
            self.assignment.first_data_offset,
            data_buffer,
            frame_ready_at,
        );
        if let Err(error) = write_result {
            self.coordinator.poison();
            return Err(error).context("submit v8 Parallel DATA frames");
        }

        #[cfg(feature = "bench")]
        if let (Some(phases), Some((wall_started, cpu_started))) = (phases, frames_ready_started) {
            phases.frames_ready_submit_wall_ns =
                wall_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            phases.frames_ready_submit_cpu_ns =
                digest::thread_cpu_time_ns().saturating_sub(cpu_started);
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
    extent_preallocator: Option<&'a WalV8ExtentPreallocator>,
}

impl WalV8AdmissionPublisher for WalV8LeaderAdmissionPublisher<'_> {
    type Guard = WalV8ParallelAdmissionGuard;

    fn admit_and_publish(
        &self,
        plan: WalV8AdmissionPlan,
    ) -> Result<(WalV8BatchAssignment, Self::Guard)> {
        #[cfg(feature = "bench")]
        let admitted = self
            .coordinator
            .admit_and_publish_with(plan, |assignment| {
                if let Some(preallocator) = self
                    .extent_preallocator
                    .filter(|preallocator| preallocator.strategy.is_point_only())
                {
                    let data_end = assignment
                        .first_data_offset
                        .checked_add(u64::try_from(plan.frame_buffer_bytes)?)
                        .context("V8 admitted DATA range end overflows")?;
                    preallocator.register_data_range(
                        assignment.first_data_offset,
                        data_end,
                        plan.write_class,
                        u64::try_from(plan.frame_buffer_bytes)?,
                    )?;
                }

                Ok(())
            })?;
        #[cfg(not(feature = "bench"))]
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
                progress_mode: WalV8ProgressMode::Eager,
            },
        )?;
        ensure!(
            coordinator.durable_ticket_end() == installed.active_boundary.ticket_end
                && coordinator.last_frontier_end() == installed.image_len
                && coordinator.durable_prefix_digest() == installed.active_boundary.prefix_digest,
            "v8 admission coordinator differs from the installed recovery boundary"
        );

        #[cfg(feature = "bench")]
        let io_diagnostics = WalIoDiagnostics {
            data: WalIoHandleMetadata::from_file(&write_file)?,
            frontier: WalIoHandleMetadata::from_file(&write_file)?,
            sync: WalIoHandleMetadata::from_file(&sync_file)?,
            extent_preparation: None,
            sliding_extent_preparation: None,
            progress_ring_enabled: false,
            progress_cursor_enabled: false,
            cursor_direct_completion_enabled: false,
            cursor_direct_completion_select_wake_enabled: false,
            cursor_singleton_submission_enabled: false,
            admission_cursor_publish_fast_path_enabled: false,
            point_buffer_preallocation_enabled: false,
            independent_ack_waiter_enabled: false,
            dedicated_durability_driver_enabled: false,
            dedicated_durability_diagnostics_enabled: false,
            dedicated_durability_requests: 0,
            dedicated_durability_wake_signals: 0,
            dedicated_durability_coalesced_wakes: 0,
            dedicated_durability_advance_attempts: 0,
            caller_durability_driver_election_attempts: 0,
            point_buffer_preallocation_attempts: 0,
            point_buffer_preallocation_hits: 0,
            point_buffer_preallocation_fallbacks: 0,
            point_resource_retirement_mode: 0,
            point_resource_retirement_detached_prefixes: 0,
            point_resource_retirement_detached_tickets: 0,
            point_resource_retirement_batched_prefixes: 0,
            point_buffer_preallocations_in_flight: 0,
            point_resource_preclaim_enabled: false,
            point_resource_preclaim_sum_cache_enabled: false,
            point_resource_preclaim_after_buffer_enabled: false,
            point_resource_preclaim_attempts: 0,
            point_resource_preclaim_granted: 0,
            point_resource_preclaim_promoted: 0,
            point_resource_preclaim_invalidated: 0,
            point_resource_preclaim_owner_trylock_busy: 0,
            point_resource_preclaim_owner_retired: 0,
            point_resource_preclaim_ledger_trylock_busy: 0,
            point_resource_preclaim_max_preclaims: 0,
            point_resource_preclaim_max_live: 0,
            point_resource_preclaim_logical_quota_rejected: 0,
            point_resource_preclaim_source_spool_quota_rejected: 0,
            point_resource_preclaim_buffer_quota_rejected: 0,
            point_resource_preclaim_seal_index_quota_rejected: 0,
            point_resource_preclaim_recovery_workspace_rejected: 0,
            point_resource_preclaim_arithmetic_rejected: 0,
            point_resource_preclaim_invalid_charge_rejected: 0,
            point_resource_preclaim_fallbacks: 0,
            point_resource_preclaim_revoked: 0,
            point_resource_preclaim_abandoned: 0,
            point_resource_preclaim_cpu_sample_every: 0,
            point_resource_preclaim_cpu_ns: 0,
            point_resource_preclaim_cpu_samples: 0,
            point_resource_preclaim_promotion_cpu_ns: 0,
            point_resource_preclaim_promotion_cpu_samples: 0,
            progress_diagnostics: None,
        };

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
            io_diagnostics,
            #[cfg(feature = "bench")]
            initial_ticket: installed.next_ticket,
            #[cfg(feature = "bench")]
            profile: arc_swap::ArcSwapOption::empty(),
        })
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_write_profile(&self, profile: Arc<crate::mem_table::WriteProfile>) {
        if self.coordinator.next_ticket() == self.initial_ticket {
            self.coordinator
                .set_data_completion_timestamps_enabled(profile.wal_sync_diagnostics_enabled());
        }
        self.profile.store(Some(profile));
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_wal_sync_diagnostics_enabled(
        &self,
        profile: &crate::mem_table::WriteProfile,
        enabled: bool,
    ) -> Result<()> {
        self.coordinator.with_next_ticket(|next_ticket| {
            ensure!(
                !enabled
                    || profile.wal_sync_diagnostics_enabled()
                    || next_ticket == self.initial_ticket,
                "WAL sync diagnostics must be enabled before the first WAL ticket"
            );
            profile.set_wal_sync_diagnostics_enabled(enabled);
            self.coordinator
                .set_data_completion_timestamps_enabled(enabled);

            Ok(())
        })
    }

    #[cfg(feature = "bench")]
    pub(crate) fn io_diagnostics(&self) -> WalIoDiagnostics {
        self.io_diagnostics
    }

    /// Transfer the validated image and coordinator to the concurrent
    /// producer path, using the same file handle for worker DATA submissions.
    pub(crate) fn into_parallel(self) -> Result<WalV8ParallelWriter> {
        let worker_file = self
            .write_file
            .try_clone()
            .context("clone v8 WAL handle for the I/O worker")?;

        self.into_parallel_with_worker_file(worker_file)
    }

    /// Transfer the validated image and coordinator to the concurrent
    /// producer path with a caller-opened worker handle (normally O_DIRECT).
    pub(crate) fn into_parallel_with_worker_file(
        self,
        worker_file: File,
    ) -> Result<WalV8ParallelWriter> {
        #[cfg(feature = "bench")]
        let progress_mode = progress_mode_from_environment()?;
        #[cfg(not(feature = "bench"))]
        let progress_mode = production_progress_mode_from_environment()?;

        self.into_parallel_with_worker_file_and_progress_mode(worker_file, progress_mode)
    }

    fn into_parallel_with_worker_file_and_progress_mode(
        self,
        worker_file: File,
        progress_mode: WalV8ProgressMode,
    ) -> Result<WalV8ParallelWriter> {
        #[cfg(feature = "bench")]
        let direct_completion_enabled =
            cursor_direct_completion_enabled_from_environment(progress_mode)?;
        #[cfg(feature = "bench")]
        let direct_completion_wake_mode =
            direct_completion_wake_mode_from_environment(progress_mode, direct_completion_enabled)?;
        #[cfg(feature = "bench")]
        let singleton_submission_enabled =
            cursor_singleton_submission_enabled_from_environment(progress_mode)?;
        #[cfg(not(feature = "bench"))]
        let direct_completion_enabled = false;
        #[cfg(not(feature = "bench"))]
        let singleton_submission_enabled = false;

        self.into_parallel_with_worker_file_progress_and_completion(
            worker_file,
            progress_mode,
            direct_completion_enabled,
            #[cfg(feature = "bench")]
            direct_completion_wake_mode,
            singleton_submission_enabled,
            #[cfg(all(test, feature = "bench"))]
            None,
        )
    }

    fn into_parallel_with_worker_file_progress_and_completion(
        mut self,
        worker_file: File,
        progress_mode: WalV8ProgressMode,
        direct_completion_enabled: bool,
        #[cfg(feature = "bench")] direct_completion_wake_mode: WalV8DirectCompletionWakeMode,
        singleton_submission_enabled: bool,
        #[cfg(all(test, feature = "bench"))] worker_group_limit: Option<usize>,
    ) -> Result<WalV8ParallelWriter> {
        validate_positional_write_handle(&worker_file)
            .context("validate v8 WAL I/O worker handle")?;
        let worker_metadata = worker_file
            .metadata()
            .context("inspect v8 WAL I/O worker handle")?;
        ensure!(
            worker_metadata.dev() == self.installed_dev
                && worker_metadata.ino() == self.installed_ino
                && worker_metadata.len() >= self.coordinator.last_frontier_end(),
            "v8 WAL I/O worker handle does not refer to the installed Active inode"
        );
        self.coordinator = self
            .coordinator
            .with_progress_mode(self.header, progress_mode)?;
        #[cfg(feature = "bench")]
        self.coordinator.set_progress_counters_enabled(
            progress_counters_enabled_from_environment(progress_mode)?,
        )?;
        #[cfg(feature = "bench")]
        let profile = self.profile.load_full();
        #[cfg(feature = "bench")]
        if let Some(profile) = &profile {
            if progress_mode.uses_ring() {
                profile.validate_v8_progress_ring_compatibility()?;
            }
            self.coordinator
                .set_hol_diagnostics_profile(Arc::clone(profile));
            self.coordinator
                .set_data_completion_timestamps_enabled(profile.wal_sync_diagnostics_enabled());
        }
        #[cfg(feature = "bench")]
        let mut io_diagnostics = self.io_diagnostics;
        #[cfg(feature = "bench")]
        let coalesce_budget_override_us = coalesce_budget_override_from_environment()?;
        #[cfg(feature = "bench")]
        let batch_point_frontier_credits = point_frontier_credit_batch_from_environment()?;
        #[cfg(feature = "bench")]
        let point_buffer_preallocation_enabled = point_buffer_preallocation_from_environment()?;
        #[cfg(feature = "bench")]
        {
            io_diagnostics.data = WalIoHandleMetadata::from_file(&worker_file)?;
            io_diagnostics.progress_ring_enabled = progress_mode.uses_ring();
            io_diagnostics.progress_cursor_enabled = self.coordinator.progress_cursor_enabled();
            io_diagnostics.cursor_direct_completion_enabled = direct_completion_enabled;
            io_diagnostics.cursor_direct_completion_select_wake_enabled =
                direct_completion_wake_mode == WalV8DirectCompletionWakeMode::Select;
            io_diagnostics.cursor_singleton_submission_enabled = singleton_submission_enabled;
            io_diagnostics.point_buffer_preallocation_enabled = point_buffer_preallocation_enabled;
        }
        #[cfg(feature = "bench")]
        let (extent_preallocator, extent_preparation) =
            match WalExtentPreparationMode::from_environment()? {
                Some((mode, window_bytes)) if mode.is_sliding() => {
                    let start_offset = self.coordinator.last_frontier_end();
                    let owned_preallocator = match mode {
                        WalExtentPreparationMode::SlidingLookahead => {
                            let owner = self.reservation_owner.as_ref().context(
                                "sliding lookahead benchmark requires PITR quota accounting",
                            )?;
                            Some(Arc::new(WalV8ExtentPreallocator::new_with_lookahead(
                                worker_file
                                    .try_clone()
                                    .context("clone v8 WAL handle for sliding extent lookahead")?,
                                &self.sync_file,
                                start_offset,
                                self.max_file_size,
                                window_bytes,
                                Some(Arc::clone(owner)),
                                true,
                            )?))
                        }
                        WalExtentPreparationMode::SlidingPointLookahead => {
                            let owner = self.reservation_owner.as_ref().context(
                                "point-only sliding lookahead benchmark requires PITR quota accounting",
                            )?;
                            Some(Arc::new(
                                WalV8ExtentPreallocator::new_with_point_only_lookahead(
                                    worker_file.try_clone().context(
                                        "clone v8 WAL handle for point-only extent lookahead",
                                    )?,
                                    &self.sync_file,
                                    start_offset,
                                    self.max_file_size,
                                    window_bytes,
                                    Arc::clone(owner),
                                )?,
                            ))
                        }
                        WalExtentPreparationMode::SlidingFallocate => {
                            if let Some(owner) = &self.reservation_owner {
                                Some(Arc::new(WalV8ExtentPreallocator::new(
                                    worker_file.try_clone().context(
                                        "clone v8 WAL handle for sliding extent preparation",
                                    )?,
                                    &self.sync_file,
                                    start_offset,
                                    self.max_file_size,
                                    window_bytes,
                                    Some(Arc::clone(owner)),
                                )?))
                            } else {
                                None
                            }
                        }
                        WalExtentPreparationMode::SlidingInitialize => None,
                        WalExtentPreparationMode::Fallocate
                        | WalExtentPreparationMode::Initialize => {
                            anyhow::bail!("invalid sliding V8 extent preparation mode")
                        }
                    };
                    if let Some(preallocator) = owned_preallocator {
                        io_diagnostics.sliding_extent_preparation =
                            Some(preallocator.diagnostics());

                        (Some(preallocator), None)
                    } else {
                        // Unit tests and non-PITR benchmark constructors have
                        // no image quota owner. Preserve their explicit
                        // benchmark preparation behavior without enabling it
                        // in the production path.
                        let preparation = Arc::new(WalV8SlidingExtentPreparation::new(
                            worker_file
                                .try_clone()
                                .context("clone v8 WAL handle for sliding extent preparation")?,
                            &self.sync_file,
                            start_offset,
                            self.max_file_size,
                            window_bytes,
                            mode,
                        )?);
                        io_diagnostics.sliding_extent_preparation = Some(preparation.diagnostics());

                        (None, Some(preparation))
                    }
                }
                Some((mode, bytes)) => {
                    io_diagnostics.extent_preparation = Some(prepare_v8_extent_suffix(
                        &worker_file,
                        &self.sync_file,
                        self.coordinator.last_frontier_end(),
                        self.max_file_size,
                        mode,
                        bytes,
                    )?);

                    (None, None)
                }
                None => (None, None),
            };
        #[cfg(feature = "bench")]
        let frontier_file = match WalV8FrontierWriteMode::from_environment()? {
            WalV8FrontierWriteMode::Buffered => self.write_file,
            WalV8FrontierWriteMode::Direct => {
                ensure!(
                    io_diagnostics.data.o_direct == Some(true),
                    "direct FRONTIER experiment requires an O_DIRECT DATA worker"
                );
                worker_file
                    .try_clone()
                    .context("clone direct V8 WAL DATA handle for FRONTIER writes")?
            }
        };
        #[cfg(feature = "bench")]
        {
            io_diagnostics.frontier = WalIoHandleMetadata::from_file(&frontier_file)?;
            ensure!(
                io_diagnostics.frontier.device == io_diagnostics.data.device
                    && io_diagnostics.frontier.inode == io_diagnostics.data.inode,
                "V8 WAL FRONTIER handle does not refer to the DATA inode"
            );
        }
        #[cfg(not(feature = "bench"))]
        let frontier_file = self.write_file;
        #[cfg(not(feature = "bench"))]
        let extent_preallocator = {
            let lookahead = wal_v8_extent_lookahead_from_environment()?;
            let eligible = self.reservation_owner.is_some()
                && is_ext_family_filesystem(&worker_file)
                && is_o_direct_file(&worker_file)?;
            ensure!(
                !lookahead || eligible,
                "{WAL_V8_EXTENT_LOOKAHEAD_ENV} requires an ext4 WAL, an O_DIRECT DATA worker, and a quota owner"
            );
            if let Some(owner) = &self.reservation_owner
                && eligible
            {
                Some(Arc::new(WalV8ExtentPreallocator::new_with_lookahead(
                    worker_file
                        .try_clone()
                        .context("clone v8 WAL handle for production extent preparation")?,
                    &self.sync_file,
                    self.coordinator.last_frontier_end(),
                    self.max_file_size,
                    WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES,
                    Some(Arc::clone(owner)),
                    lookahead,
                )?))
            } else {
                None
            }
        };
        let initial_ticket = self.coordinator.next_ticket();
        let io = WalV8ParallelIo::spawn(
            worker_file,
            self.coordinator.clone(),
            initial_ticket,
            extent_preallocator,
            WalV8ParallelIoConfig {
                #[cfg(feature = "bench")]
                extent_preparation,
                direct_completion_enabled,
                #[cfg(feature = "bench")]
                direct_completion_wake_mode,
                singleton_submission_enabled,
                #[cfg(all(test, feature = "bench"))]
                worker_group_limit,
            },
        )?;
        #[cfg(feature = "bench")]
        if let Some(profile) = profile {
            io.set_profile(profile);
        }

        Ok(WalV8ParallelWriter {
            frontier_file,
            sync_file: self.sync_file,
            header: self.header,
            physical_header_digest: self.physical_header_digest,
            initial_prefix_digest: self.initial_prefix_digest,
            coordinator: self.coordinator,
            io,
            reservation_owner: self.reservation_owner,
            frontier: Mutex::new(WalV8ParallelFrontier {
                frontier: self.frontier,
                offset: self.frontier_offset,
                digest: self.frontier_digest,
                last_sync_latency: None,
            }),
            frontier_resources: Mutex::new(BTreeMap::new()),
            producer_guards: Arc::new(Mutex::new(BTreeMap::new())),
            max_file_size: self.max_file_size,
            installed_dev: self.installed_dev,
            installed_ino: self.installed_ino,
            #[cfg(feature = "bench")]
            initial_ticket,
            #[cfg(feature = "bench")]
            io_diagnostics,
            #[cfg(feature = "bench")]
            coalesce_budget_override_us,
            #[cfg(feature = "bench")]
            batch_point_frontier_credits,
            #[cfg(feature = "bench")]
            point_buffer_preallocation_enabled,
            #[cfg(all(test, feature = "bench"))]
            point_buffer_preallocation_test_override: AtomicBool::new(false),
            #[cfg(feature = "bench")]
            point_buffer_preallocation_attempts: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            point_buffer_preallocation_hits: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            point_buffer_preallocation_fallbacks: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            point_buffer_preallocations_in_flight: Arc::new(AtomicUsize::new(0)),
            #[cfg(feature = "bench")]
            profile: self.profile,
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
        self.preflight_batch_with_write_class(batch, limits, WalV8WriteClass::Batch)
    }

    pub(crate) fn preflight_batch_with_write_class<'a>(
        &self,
        batch: &'a WalBatch,
        limits: WalV5Limits,
        write_class: WalV8WriteClass,
    ) -> Result<WalV8PreparedBatch<'a>> {
        ensure!(!self.is_poisoned(), "v8 Leader WAL is poisoned");
        let limits = bounded_limits(limits);

        WalV8PreparedBatch::preflight_with_write_class(batch, limits, write_class, |bytes| {
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
                extent_preallocator: None,
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
        let frontier_write_ns =
            if let (Some(profile), Some(started)) = (&profile, frontier_write_started) {
                let data_bytes = producer_completions
                    .iter()
                    .fold(0_u64, |total, completion| {
                        total.saturating_add(completion.encoded.frame_buffer_bytes() as u64)
                    });
                let frontier_write_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                profile.record_wal_commit_group(producer_completions.len() as u64, data_bytes);
                profile.record_wal_frontier_write(WAL_V8_FRAME_LEN as u64, frontier_write_ns);
                Some(frontier_write_ns)
            } else {
                None
            };
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
                    decision_ticket_start: Some(self.frontier.ticket_end),
                    written_frontier_start: self.frontier.ticket_end,
                    written_frontier_end: ticket_end,
                    groups_completed: producer_completions.len() as u64,
                    groups_covered: producer_completions.len() as u64,
                    tickets_covered: Some(producer_completions.len() as u64),
                    frontier_write_ns: frontier_write_ns.unwrap_or(0),
                    data_completion_to_frontier_start_ns: data_completion_to_frontier_start_ns(
                        &ready_prefix[..producer_completions.len()],
                        frontier_write_started,
                    ),
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
    pub(crate) fn progress_ring_enabled(&self) -> bool {
        self.coordinator.progress_ring_enabled()
    }

    #[cfg(feature = "bench")]
    pub(crate) fn io_diagnostics(&self) -> WalIoDiagnostics {
        let mut diagnostics = self.io_diagnostics;
        diagnostics.sliding_extent_preparation = self.io.extent_preparation_diagnostics();
        diagnostics.progress_ring_enabled = self.coordinator.progress_ring_enabled();
        diagnostics.progress_cursor_enabled = self.coordinator.progress_cursor_enabled();
        diagnostics.progress_diagnostics = self.coordinator.progress_diagnostics();
        diagnostics.point_buffer_preallocation_attempts = self
            .point_buffer_preallocation_attempts
            .load(Ordering::Relaxed);
        diagnostics.point_buffer_preallocation_hits =
            self.point_buffer_preallocation_hits.load(Ordering::Relaxed);
        diagnostics.point_buffer_preallocation_fallbacks = self
            .point_buffer_preallocation_fallbacks
            .load(Ordering::Relaxed);
        diagnostics.point_buffer_preallocations_in_flight = u64::try_from(
            self.point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
        )
        .expect("outstanding preallocation permits fit in u64");
        #[cfg(test)]
        {
            diagnostics.point_buffer_preallocation_enabled |= self
                .point_buffer_preallocation_test_override
                .load(Ordering::Relaxed);
        }

        diagnostics
    }

    #[cfg(all(test, feature = "bench"))]
    pub(crate) fn set_point_buffer_preallocation_for_test(&self, enabled: bool) {
        self.point_buffer_preallocation_test_override
            .store(enabled, Ordering::Relaxed);
    }

    #[cfg(feature = "bench")]
    fn should_preallocate_point_buffer(&self) -> bool {
        #[cfg(test)]
        {
            self.point_buffer_preallocation_enabled
                || self
                    .point_buffer_preallocation_test_override
                    .load(Ordering::Relaxed)
        }

        #[cfg(not(test))]
        {
            self.point_buffer_preallocation_enabled
        }
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_write_profile(
        &self,
        profile: Arc<crate::mem_table::WriteProfile>,
    ) -> Result<()> {
        if self.progress_ring_enabled() {
            profile.validate_v8_progress_ring_compatibility()?;
        }

        self.io.set_profile(Arc::clone(&profile));
        self.coordinator
            .set_hol_diagnostics_profile(Arc::clone(&profile));
        if self.coordinator.next_ticket() == self.initial_ticket {
            self.coordinator
                .set_data_completion_timestamps_enabled(profile.wal_sync_diagnostics_enabled());
        }
        self.profile.store(Some(profile));

        Ok(())
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_wal_sync_diagnostics_enabled(
        &self,
        profile: &crate::mem_table::WriteProfile,
        enabled: bool,
    ) -> Result<()> {
        self.coordinator.with_next_ticket(|next_ticket| {
            ensure!(
                !enabled
                    || profile.wal_sync_diagnostics_enabled()
                    || next_ticket == self.initial_ticket,
                "WAL sync diagnostics must be enabled before the first WAL ticket"
            );
            profile.set_wal_sync_diagnostics_enabled(enabled);
            self.coordinator
                .set_data_completion_timestamps_enabled(enabled);

            Ok(())
        })
    }

    #[cfg(feature = "bench")]
    pub(crate) fn set_wal_hol_diagnostics_enabled(
        &self,
        profile: &crate::mem_table::WriteProfile,
        enabled: bool,
    ) -> Result<()> {
        ensure!(
            !enabled || !self.coordinator.progress_ring_enabled(),
            "V8 HOL diagnostics are not supported with the progress ring"
        );
        self.coordinator.with_next_ticket(|next_ticket| {
            ensure!(
                !enabled
                    || profile.wal_hol_diagnostics_enabled()
                    || next_ticket == self.initial_ticket,
                "WAL HOL diagnostics must be enabled before the first WAL ticket"
            );
            profile.set_wal_hol_diagnostics_enabled(enabled);
            self.coordinator.set_hol_diagnostics_enabled(enabled);
            self.io.set_hol_diagnostics_enabled(enabled);

            Ok(())
        })
    }

    /// Reserve bounded preparation state before canonicalizing a batch.
    pub(crate) fn preflight_batch<'a>(
        &self,
        batch: &'a WalBatch,
        limits: WalV5Limits,
    ) -> Result<WalV8PreparedBatch<'a>> {
        self.preflight_batch_with_write_class(batch, limits, WalV8WriteClass::Batch)
    }

    pub(crate) fn preflight_batch_with_write_class<'a>(
        &self,
        batch: &'a WalBatch,
        limits: WalV5Limits,
        write_class: WalV8WriteClass,
    ) -> Result<WalV8PreparedBatch<'a>> {
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        let limits = bounded_limits(limits);

        WalV8PreparedBatch::preflight_with_write_class(batch, limits, write_class, |bytes| {
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

    /// Prepare optional Point scratch before the sequencer. The return value
    /// indicates whether the DATA buffer was preallocated.
    #[cfg(feature = "bench")]
    pub(crate) fn try_preallocate_point_buffer(
        &self,
        scratch: &mut WalV8PointAdmissionScratch<'_>,
    ) -> bool {
        self.try_preallocate_point_buffer_only(scratch)
    }

    #[cfg(feature = "bench")]
    fn try_preallocate_point_buffer_only(
        &self,
        scratch: &mut WalV8PointAdmissionScratch<'_>,
    ) -> bool {
        if !self.should_preallocate_point_buffer() {
            return false;
        }
        self.point_buffer_preallocation_attempts
            .fetch_add(1, Ordering::Relaxed);

        if scratch.prepared.frame_buffer_bytes() != WAL_V8_FRAME_LEN {
            self.point_buffer_preallocation_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let Some(permit) = WalV8PointBufferPreallocationPermit::try_acquire(
            &self.point_buffer_preallocations_in_flight,
        ) else {
            self.point_buffer_preallocation_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return false;
        };

        // This allocation is bounded to one frame and never charges shared
        // quota. An abandoned attempt drops both the buffer and its permit.
        let data_buffer = DirectBufAllocation::new(scratch.prepared.frame_buffer_bytes());
        scratch.set_preallocated_buffer(WalV8PointBufferPreallocation {
            data_buffer,
            _permit: permit,
        });
        self.point_buffer_preallocation_hits
            .fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Admit a Point scratch object using ordered quota reservation and then
    /// consume its optional speculative DATA buffer.
    #[cfg(feature = "bench")]
    pub(crate) fn admit_prepared_point_batch<'a>(
        &self,
        scratch: WalV8PointAdmissionScratch<'a>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
    ) -> Result<WalV8FrameJob<'a>> {
        let WalV8PointAdmissionScratch {
            prepared,
            preallocated_buffer,
        } = scratch;
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        let logical_bytes = u64::try_from(prepared.logical_batch_bytes())?;
        let frame_bytes = u64::try_from(prepared.frame_buffer_bytes())?;
        #[cfg(feature = "bench")]
        let resources = self.reserve_and_allocate_batch_resources(
            logical_bytes,
            frame_bytes,
            prepared.frame_buffer_bytes(),
            preallocated_buffer,
        )?;
        #[cfg(not(feature = "bench"))]
        let resources = self.reserve_and_allocate_batch_resources(
            logical_bytes,
            frame_bytes,
            prepared.frame_buffer_bytes(),
        )?;

        self.admit_prepared_batch_with_resources(prepared, commit_ts, recorded_at, resources, || {})
    }

    fn reserve_batch_resources(
        &self,
        logical_bytes: u64,
        frame_bytes: u64,
    ) -> Result<Option<WalV7OwnedBatchReservation>> {
        let Some(owner) = &self.reservation_owner else {
            return Ok(None);
        };
        let reservation = owner.reserve_batch(logical_bytes, frame_bytes);
        if let Err(error) = &reservation
            && matches!(
                error.downcast_ref::<WalV7ReservationError>(),
                Some(WalV7ReservationError::SourceSpoolLimit { .. })
            )
            && let Some(preallocator) = &self.io.extent_preallocator
        {
            self.coordinator
                .with_allocation_tail(|tail| preallocator.discard_speculative_tail(tail))?;

            return owner.reserve_batch(logical_bytes, frame_bytes).map(Some);
        }

        reservation.map(Some)
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
        #[cfg(feature = "bench")]
        let resources = self.reserve_and_allocate_batch_resources(
            logical_bytes,
            frame_bytes,
            prepared.frame_buffer_bytes(),
            None,
        )?;
        #[cfg(not(feature = "bench"))]
        let resources = self.reserve_and_allocate_batch_resources(
            logical_bytes,
            frame_bytes,
            prepared.frame_buffer_bytes(),
        )?;
        self.admit_prepared_batch_with_resources(
            prepared,
            commit_ts,
            recorded_at,
            resources,
            after_admission,
        )
    }

    fn reserve_and_allocate_batch_resources(
        &self,
        logical_bytes: u64,
        frame_bytes: u64,
        buffer_bytes: usize,
        #[cfg(feature = "bench")] preallocated_buffer: Option<WalV8PointBufferPreallocation>,
    ) -> Result<WalV8AdmissionResources> {
        let reservation = self.reserve_batch_resources(logical_bytes, frame_bytes)?;
        #[cfg(feature = "bench")]
        let data_buffer = preallocated_buffer
            .map(WalV8PointBufferPreallocation::into_data_buffer)
            .unwrap_or_else(|| DirectBufAllocation::new(buffer_bytes));
        #[cfg(not(feature = "bench"))]
        let data_buffer = DirectBufAllocation::new(buffer_bytes);
        // Own the allocation before publishing the ticket. Initialization is
        // payload-sized work, so defer it to finish after the MVCC sequencer
        // has been released.
        Ok(WalV8AdmissionResources {
            data_buffer,
            reservation,
        })
    }

    fn admit_prepared_batch_with_resources<'a, F>(
        &self,
        prepared: WalV8PreparedBatch<'a>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
        mut resources: WalV8AdmissionResources,
        after_admission: F,
    ) -> Result<WalV8FrameJob<'a>>
    where
        F: FnOnce(),
    {
        let publisher = WalV8LeaderAdmissionPublisher {
            coordinator: &self.coordinator,
            reservation: RefCell::new(&mut resources.reservation),
            extent_preallocator: self.io.extent_preallocator.as_deref(),
        };
        let admitted = prepared.admit_with_metadata(commit_ts, recorded_at, &publisher)?;
        let assignment = admitted.assignment();
        #[cfg(feature = "bench")]
        let write_class = admitted.write_class();
        if let Some(reservation) = resources.reservation.take() {
            let previous = self.frontier_resources.lock().insert(
                assignment.segment_ticket,
                WalV8FrontierResource {
                    reservation,
                    #[cfg(feature = "bench")]
                    write_class,
                },
            );
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
            io: Arc::clone(&self.io),
            data_buffer: Some(resources.data_buffer),
            #[cfg(feature = "bench")]
            profile: self.profile.load_full(),
        })
    }

    fn advance_frontier_through(&self, ticket: u64) -> Result<()> {
        let ticket_end = ticket
            .checked_add(1)
            .context("v8 Parallel durable ticket end overflows")?;
        let Some(mut driver) = self.coordinator.acquire_durability_driver(ticket_end)? else {
            return Ok(());
        };
        let result = self.drive_frontier_through(ticket_end, &mut driver);
        if result.is_ok() {
            driver.complete();
        }

        result
    }

    fn drive_frontier_through(
        &self,
        ticket_end: u64,
        driver: &mut WalV8DurabilityDriver,
    ) -> Result<()> {
        let progress_ring_enabled = self.coordinator.progress_ring_enabled();
        let mut pending_progress_wake = None;
        loop {
            if self.coordinator.is_poisoned() {
                anyhow::bail!("v8 Parallel WAL is poisoned");
            }
            if self.coordinator.durable_ticket_end() >= ticket_end {
                return Ok(());
            }

            #[cfg(feature = "bench")]
            let profile = self.profile.load_full();
            #[cfg(feature = "bench")]
            let driver_attempt_sequence = profile
                .as_ref()
                .and_then(|profile| profile.record_wal_v8_durability_driver_attempt());
            #[cfg(feature = "bench")]
            let phase_sample_candidate = driver_attempt_sequence.and_then(|_| {
                profile
                    .as_ref()
                    .and_then(|profile| profile.next_wal_v8_durability_advance_sample_candidate())
            });
            #[cfg(feature = "bench")]
            let frontier_lock_wait_started = phase_sample_candidate.map(|_| Instant::now());
            #[cfg(feature = "bench")]
            let driver_wall_started = phase_sample_candidate.map(|_| Instant::now());
            #[cfg(feature = "bench")]
            let driver_cpu_started = phase_sample_candidate.map(|_| digest::thread_cpu_time_ns());
            let eager_observed_generation =
                (!progress_ring_enabled).then(|| self.coordinator.durability_generation());
            let mut previous = self.frontier.lock();
            #[cfg(feature = "bench")]
            let frontier_lock_wait_ns = frontier_lock_wait_started
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or_default();
            if self.coordinator.durable_ticket_end() >= ticket_end {
                return Ok(());
            }

            #[cfg(feature = "bench")]
            let diagnostics_enabled = profile
                .as_ref()
                .is_some_and(|profile| profile.wal_sync_diagnostics_enabled());
            #[cfg(feature = "bench")]
            let mut coalescing_observation = None;
            #[cfg(feature = "bench")]
            let mut coalescing_detail = None;
            #[cfg(feature = "bench")]
            let mut coalescing_attempted = false;
            #[cfg(feature = "bench")]
            let mut coalescing_wall_ns = 0_u64;
            #[cfg(feature = "bench")]
            let mut coalescing_cpu_ns = 0_u64;
            #[cfg(feature = "bench")]
            let mut coalescing_budget_us = None;
            let scan_wake = pending_progress_wake
                .take()
                .unwrap_or(WalV8ProgressWake::Control);
            #[cfg(feature = "bench")]
            let mut snapshot = if progress_ring_enabled {
                self.io
                    .advance_and_snapshot_after_wake(driver, diagnostics_enabled, scan_wake)?
            } else if diagnostics_enabled {
                self.coordinator.frontier_ready_snapshot_with_monotonic_ns()
            } else {
                self.coordinator.frontier_ready_snapshot()
            };
            #[cfg(not(feature = "bench"))]
            let mut snapshot = if progress_ring_enabled {
                self.io
                    .advance_and_snapshot_after_wake(driver, false, scan_wake)?
            } else {
                self.coordinator.frontier_ready_snapshot()
            };
            #[cfg(feature = "bench")]
            let has_ready_prefix = !snapshot.ready_prefix.is_empty();
            #[cfg(feature = "bench")]
            if has_ready_prefix && driver_attempt_sequence.is_some() {
                profile
                    .as_ref()
                    .expect("sampled durability driver has a write profile")
                    .record_wal_v8_durability_advance_attempt();
            }
            #[cfg(feature = "bench")]
            let phase_sample_id = has_ready_prefix.then_some(phase_sample_candidate).flatten();
            #[cfg(feature = "bench")]
            let mut cursor_coalescing_timing = (progress_ring_enabled && phase_sample_id.is_some())
                .then(WalV8CursorCoalescingTiming::default);
            #[cfg(feature = "bench")]
            let phase_sample_guard = phase_sample_id
                .and_then(|_| profile.as_ref().cloned())
                .map(WalV8DurabilityPhaseSampleGuard::new);
            if !snapshot.ready_prefix.is_empty()
                && snapshot.ready_prefix.len() < snapshot.admitted_ticket_count
                && let Some(latency) = previous.last_sync_latency
            {
                #[cfg(feature = "bench")]
                let budget_us = self.coalesce_budget_override_us.unwrap_or_else(|| {
                    if latency >= Duration::from_micros(100) {
                        400
                    } else {
                        100
                    }
                });
                #[cfg(not(feature = "bench"))]
                let budget_us = if latency >= Duration::from_micros(100) {
                    400
                } else {
                    100
                };
                let budget = Duration::from_micros(budget_us);
                #[cfg(feature = "bench")]
                let ready_tickets_before_coalescing = snapshot.ready_prefix.len() as u64;
                #[cfg(feature = "bench")]
                let admitted_tickets_before_coalescing = snapshot.admitted_ticket_count as u64;
                #[cfg(feature = "bench")]
                {
                    coalescing_attempted = true;
                    coalescing_budget_us = Some(budget_us);
                }
                #[cfg(feature = "bench")]
                let started = profile.as_ref().map(|_| Instant::now());
                #[cfg(feature = "bench")]
                let phase_started = phase_sample_id.map(|_| Instant::now());
                #[cfg(feature = "bench")]
                let phase_cpu_started = phase_sample_id.map(|_| digest::thread_cpu_time_ns());
                #[cfg(feature = "bench")]
                let waited = if progress_ring_enabled {
                    self.io.coalesce_progress_ring(
                        driver,
                        &mut snapshot,
                        budget,
                        diagnostics_enabled,
                        cursor_coalescing_timing.as_mut(),
                    )?
                } else if phase_sample_id.is_some() {
                    let observation = self.coordinator.coalesce_frontier_ready_sampled(budget);
                    let waited = observation.waited;
                    coalescing_detail = Some(observation);
                    waited
                } else {
                    self.coordinator.coalesce_frontier_ready(budget)
                };
                #[cfg(not(feature = "bench"))]
                let waited = if progress_ring_enabled {
                    self.io
                        .coalesce_progress_ring(driver, &mut snapshot, budget, false)?
                } else {
                    self.coordinator.coalesce_frontier_ready(budget)
                };
                #[cfg(feature = "bench")]
                {
                    coalescing_wall_ns = phase_started
                        .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                        .unwrap_or_default();
                    coalescing_cpu_ns = phase_cpu_started
                        .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                        .unwrap_or_default();
                }
                #[cfg(feature = "bench")]
                let coalescing_wait_ns = started
                    .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                #[cfg(feature = "bench")]
                if waited && let Some(profile) = &profile {
                    profile.record_wal_sync_coalesce_wait(coalescing_wait_ns.unwrap_or_default());
                }
                #[cfg(not(feature = "bench"))]
                let _ = waited;
                #[cfg(feature = "bench")]
                if !progress_ring_enabled {
                    snapshot = if diagnostics_enabled {
                        self.coordinator.frontier_ready_snapshot_with_monotonic_ns()
                    } else {
                        self.coordinator.frontier_ready_snapshot()
                    };
                }
                #[cfg(not(feature = "bench"))]
                if !progress_ring_enabled {
                    snapshot = self.coordinator.frontier_ready_snapshot();
                }
                #[cfg(feature = "bench")]
                if diagnostics_enabled {
                    let ready_tickets_after_coalescing = snapshot.ready_prefix.len() as u64;
                    coalescing_observation = Some((
                        budget_us,
                        waited,
                        coalescing_wait_ns.unwrap_or_default(),
                        ready_tickets_before_coalescing,
                        ready_tickets_after_coalescing,
                        ready_tickets_after_coalescing
                            .saturating_sub(ready_tickets_before_coalescing),
                        admitted_tickets_before_coalescing,
                        snapshot.admitted_ticket_count as u64,
                    ));
                }
            }
            #[cfg(feature = "bench")]
            let frontier_observation = diagnostics_enabled.then(|| {
                (
                    snapshot.admitted_ticket_count as u64,
                    snapshot.reserved_preparation_bytes,
                    snapshot.frames_ready_prefix_count as u64,
                    snapshot.data_complete_prefix_count as u64,
                    snapshot.ready_prefix.len() as u64,
                    snapshot.next_not_ready,
                    snapshot
                        .snapshot_monotonic_ns
                        .expect("diagnostic frontier snapshots capture their lock timestamp"),
                    snapshot.data_hol_blocker,
                )
            });
            let ready = snapshot.ready_prefix;

            if ready.is_empty() {
                #[cfg(feature = "bench")]
                if driver_attempt_sequence.is_some() {
                    profile
                        .as_ref()
                        .expect("sampled durability driver has a write profile")
                        .record_wal_v8_durability_empty_driver_attempt();
                }
                drop(previous);
                if let Some(observed_generation) = eager_observed_generation {
                    let wake = self
                        .coordinator
                        .wait_for_durability_change(observed_generation, Duration::from_secs(1));
                    if wake.poisoned {
                        anyhow::bail!("v8 Parallel WAL was poisoned while waiting for durability");
                    }
                    if wake.closed && self.coordinator.durable_ticket_end() < ticket_end {
                        anyhow::bail!("v8 Parallel WAL closed before the ticket became durable");
                    }
                } else {
                    if snapshot.closed {
                        anyhow::bail!("v8 Parallel WAL closed before the ticket became durable");
                    }
                    pending_progress_wake =
                        Some(self.io.wait_for_progress(Duration::from_secs(1))?);
                    if self.coordinator.is_poisoned() {
                        anyhow::bail!("v8 Parallel WAL was poisoned while waiting for durability");
                    }
                }
                continue;
            }

            let first_ticket = self.coordinator.durable_ticket_end();
            let frontier_ticket_end = first_ticket
                .checked_add(u64::try_from(ready.len())?)
                .context("v8 Parallel frontier ticket end overflows")?;
            let last = ready
                .tail()
                .context("v8 Parallel ready prefix unexpectedly empty")?;
            let last_data_end = last
                .first_data_offset
                .checked_add(u64::try_from(last.frame_buffer_bytes)?)
                .context("v8 Parallel durable DATA end overflows")?;
            let previous_frontier_end = previous
                .offset
                .checked_add(FRAME_LEN_U64)
                .context("v8 previous FRONTIER end overflows")?;
            #[cfg(feature = "bench")]
            let preparation_started = phase_sample_id.map(|_| Instant::now());
            #[cfg(feature = "bench")]
            let preparation_cpu_started = phase_sample_id.map(|_| digest::thread_cpu_time_ns());
            let reservation = self
                .coordinator
                .reserve_frontier_with(frontier_ticket_end, |reservation| {
                    self.io.register_frontier_offset(reservation.frame_offset)
                })?;
            let mut cycle_guard = WalV8FrontierCycleGuard::new(&self.coordinator);
            let durable_end = last_data_end.max(previous_frontier_end);
            ensure!(
                previous_frontier_end <= durable_end
                    && durable_end <= reservation.frame_offset
                    && reservation.frame_offset + FRAME_LEN_U64 <= self.max_file_size,
                "v8 Parallel FRONTIER physical boundaries are invalid"
            );
            let frontier_end = reservation
                .frame_offset
                .checked_add(FRAME_LEN_U64)
                .context("v8 Parallel FRONTIER extent end overflows")?;
            self.io.ensure_extent_prepared_through(frontier_end)?;
            self.io
                .consume_extent_source_credit(reservation.frame_offset, frontier_end)?;
            let frontier = WalV8Frontier {
                generation: previous
                    .frontier
                    .generation
                    .checked_add(1)
                    .context("v8 FRONTIER generation overflows")?,
                ticket_end: frontier_ticket_end,
                durable_end,
                last_commit_ts: last.commit_ts,
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
            let preparation_wall_ns = preparation_started
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or_default();
            #[cfg(feature = "bench")]
            let preparation_cpu_ns = preparation_cpu_started
                .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default();
            #[cfg(feature = "bench")]
            let frontier_write_started = profile.as_ref().map(|_| Instant::now());
            #[cfg(feature = "bench")]
            let phase_frontier_write_started = phase_sample_id.map(|_| Instant::now());
            #[cfg(feature = "bench")]
            let phase_frontier_write_cpu_started =
                phase_sample_id.map(|_| digest::thread_cpu_time_ns());
            write_aligned_once(
                &self.frontier_file,
                frontier_buffer.initialized_slice(0, WAL_V8_FRAME_LEN),
                reservation.frame_offset,
            )
            .context("write v8 Parallel FRONTIER")?;
            #[cfg(feature = "bench")]
            let phase_frontier_write_wall_ns = phase_frontier_write_started
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or_default();
            #[cfg(feature = "bench")]
            let phase_frontier_write_cpu_ns = phase_frontier_write_cpu_started
                .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default();
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::wal_crash_point("wal_v8.parallel.after_frontier_write");
            #[cfg(feature = "bench")]
            let frontier_write_ns = if let (Some(profile), Some(started)) =
                (&profile, frontier_write_started)
            {
                let data_bytes = ready.data_bytes();
                let groups = ready.len() as u64;
                let frontier_write_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                profile.record_wal_commit_group(groups, data_bytes);
                profile.record_wal_frontier_write(WAL_V8_FRAME_LEN as u64, frontier_write_ns);
                Some(frontier_write_ns)
            } else {
                None
            };
            let sync_started = Instant::now();
            #[cfg(feature = "bench")]
            let phase_sync_cpu_started = phase_sample_id.map(|_| digest::thread_cpu_time_ns());
            let sync_result = fdatasync_once(&self.sync_file);
            previous.last_sync_latency = Some(sync_started.elapsed());
            #[cfg(feature = "bench")]
            let phase_fdatasync_wall_ns =
                sync_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            #[cfg(feature = "bench")]
            let phase_fdatasync_cpu_ns = phase_sync_cpu_started
                .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default();
            #[cfg(feature = "bench")]
            if let Some(profile) = &profile {
                let duration_ns = sync_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                let groups = ready.len() as u64;
                profile.record_wal_sync(groups);
                profile.record_wal_fdatasync_ns(duration_ns);
                if diagnostics_enabled {
                    let (
                        admitted_ticket_count,
                        v8_preparation_reserved_bytes,
                        frames_ready_prefix_tickets,
                        data_complete_prefix_tickets,
                        durability_ready_prefix_tickets,
                        next_ticket,
                        sync_decision_monotonic_ns,
                        data_hol_blocker,
                    ) = frontier_observation
                        .expect("detailed v8 sync diagnostics capture a frontier snapshot");
                    profile.record_wal_sync_observation(crate::mem_table::WalSyncObservation {
                        captured_target: frontier_ticket_end,
                        decision_ticket_start: Some(first_ticket),
                        sync_decision_monotonic_ns: Some(sync_decision_monotonic_ns),
                        written_frontier_start: first_ticket,
                        written_frontier_end: frontier_ticket_end,
                        admitted_ticket_count,
                        frames_ready_prefix_tickets: Some(frames_ready_prefix_tickets),
                        data_complete_prefix_tickets: Some(data_complete_prefix_tickets),
                        data_hol_blocker_ticket: data_hol_blocker
                            .map(|ticket| ticket.assignment.segment_ticket),
                        data_hol_blocker_frames_ready: data_hol_blocker
                            .map(|ticket| ticket.frames_ready),
                        data_hol_blocker_digest_folded: data_hol_blocker
                            .map(|ticket| ticket.prefix_digest_after.is_some()),
                        data_hol_blocker_data_write_elapsed_ns: data_hol_blocker
                            .and_then(|ticket| ticket.data_write_elapsed_ns),
                        durability_ready_prefix_tickets: Some(durability_ready_prefix_tickets),
                        v8_preparation_reserved_bytes,
                        next_ticket_present: next_ticket.is_some(),
                        next_ticket_frames_ready: next_ticket
                            .is_some_and(|ticket| ticket.frames_ready),
                        next_ticket_digest_folded: next_ticket
                            .is_some_and(|ticket| ticket.prefix_digest_after.is_some()),
                        next_ticket_data_complete: next_ticket
                            .is_some_and(|ticket| ticket.data_complete),
                        next_ticket_data_write_elapsed_ns: next_ticket
                            .and_then(|ticket| ticket.data_write_elapsed_ns),
                        groups_completed: groups,
                        groups_covered: groups,
                        tickets_covered: Some(frontier_ticket_end - first_ticket),
                        frontier_write_ns: frontier_write_ns.unwrap_or(0),
                        data_completion_to_frontier_start_ns: match &ready {
                            WalV8ReadyPrefix::Tickets(tickets) => {
                                data_completion_to_frontier_start_ns(
                                    tickets,
                                    frontier_write_started,
                                )
                            }
                            WalV8ReadyPrefix::Cursor { .. } => None,
                        },
                        coalescing_budget_us: coalescing_observation.map(|value| value.0),
                        coalescing_waited: coalescing_observation.map(|value| value.1),
                        coalescing_wait_ns: coalescing_observation.map(|value| value.2),
                        ready_tickets_before_coalescing: coalescing_observation
                            .map(|value| value.3),
                        ready_tickets_after_coalescing: coalescing_observation.map(|value| value.4),
                        ready_tickets_gained_during_coalescing: coalescing_observation
                            .map(|value| value.5),
                        admitted_tickets_before_coalescing: coalescing_observation
                            .map(|value| value.6),
                        admitted_tickets_after_coalescing: coalescing_observation
                            .map(|value| value.7),
                        duration_ns,
                        succeeded: sync_result.is_ok(),
                        ..crate::mem_table::WalSyncObservation::default()
                    });
                }
            }
            sync_result.context("fdatasync v8 Parallel WAL")?;
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::wal_crash_point("wal_v8.parallel.after_fdatasync");

            #[cfg(feature = "bench")]
            let resource_retire_before_publish = self.retire_frontier_resources(
                first_ticket,
                frontier_ticket_end,
                phase_sample_id.is_some(),
            )?;
            #[cfg(not(feature = "bench"))]
            let _ = self.retire_frontier_resources(first_ticket, frontier_ticket_end, false)?;
            #[cfg(feature = "bench")]
            let publish_metrics = if phase_sample_id.is_some() {
                Some(
                    self.coordinator
                        .publish_durable_frontier_sampled(reservation)?,
                )
            } else {
                self.coordinator.publish_durable_frontier(reservation)?;
                None
            };
            #[cfg(not(feature = "bench"))]
            self.coordinator.publish_durable_frontier(reservation)?;
            #[cfg(feature = "bench")]
            let advance_sequence = if driver_attempt_sequence.is_some() {
                Some(
                    profile
                        .as_ref()
                        .expect("sampled durability driver has a write profile")
                        .complete_wal_v8_durability_advance(),
                )
            } else {
                None
            };
            // The shared worker tracks completed groups for its diagnostics.
            // Retire the physical DATA groups covered by this V8 durability
            // boundary so long-running diagnostic runs keep that map bounded.
            #[cfg(feature = "bench")]
            let sample_guard_retirement = phase_sample_id.is_some();
            self.io.progress.mark_durable(frontier_ticket_end);
            #[cfg(feature = "chaos-testing")]
            crate::chaos::failpoint::wal_crash_point("wal_v8.parallel.after_durable_publish");
            #[cfg(not(feature = "bench"))]
            let _ = self.retire_frontier_guards(first_ticket, frontier_ticket_end, false)?;
            #[cfg(feature = "bench")]
            let resource_retire_after_publish = self.retire_frontier_guards(
                first_ticket,
                frontier_ticket_end,
                sample_guard_retirement,
            )?;
            previous.frontier = frontier;
            previous.offset = reservation.frame_offset;
            previous.digest = frontier_digest;
            cycle_guard.complete();
            #[cfg(feature = "bench")]
            if let (
                Some(sample_id),
                Some(driver_attempt_sequence),
                Some(wall_started),
                Some(cpu_started),
            ) = (
                phase_sample_id,
                driver_attempt_sequence,
                driver_wall_started,
                driver_cpu_started,
            ) {
                let advance_sequence =
                    advance_sequence.expect("sampled durability advance has a completed sequence");
                debug_assert_eq!(sample_id, advance_sequence);
                let publish_metrics =
                    publish_metrics.expect("sampled durability advance records publish metrics");
                phase_sample_guard
                    .expect("sampled durability advance has a sample guard")
                    .record(crate::mem_table::WalV8DurabilityPhaseObservation {
                        sample_id,
                        driver_attempt_sequence,
                        advance_sequence,
                        ticket_start: first_ticket,
                        ticket_end: frontier_ticket_end,
                        tickets_covered: frontier_ticket_end.saturating_sub(first_ticket),
                        driver_wall_ns: wall_started.elapsed().as_nanos().min(u64::MAX as u128)
                            as u64,
                        driver_cpu_ns: digest::thread_cpu_time_ns().saturating_sub(cpu_started),
                        frontier_lock_wait_ns,
                        coalescing_wall_ns,
                        coalescing_cpu_ns,
                        coalescing_active_wall_ns: coalescing_active_duration_ns(
                            coalescing_wall_ns,
                            coalescing_detail
                                .map(|detail| {
                                    detail
                                        .state_lock_wait_wall_ns
                                        .saturating_add(detail.condvar_wait_wall_ns)
                                })
                                .or_else(|| {
                                    cursor_coalescing_timing
                                        .map(|timing| timing.progress_wait_wall_ns)
                                }),
                            coalescing_attempted,
                        ),
                        coalescing_active_cpu_ns: coalescing_active_duration_ns(
                            coalescing_cpu_ns,
                            coalescing_detail
                                .map(|detail| {
                                    detail
                                        .state_lock_wait_cpu_ns
                                        .saturating_add(detail.condvar_wait_cpu_ns)
                                })
                                .or_else(|| {
                                    cursor_coalescing_timing
                                        .map(|timing| timing.progress_wait_cpu_ns)
                                }),
                            coalescing_attempted,
                        ),
                        coalescing_wait_breakdown_available: !coalescing_attempted
                            || coalescing_detail.is_some(),
                        coalescing_budget_us,
                        coalescing_progress_wait_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.progress_wait_wall_ns),
                        coalescing_progress_wait_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.progress_wait_cpu_ns),
                        coalescing_completion_drain_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.completion_drain_wall_ns),
                        coalescing_completion_drain_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.completion_drain_cpu_ns),
                        coalescing_coordinator_lock_wait_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.coordinator_lock_wait_wall_ns),
                        coalescing_coordinator_lock_wait_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.coordinator_lock_wait_cpu_ns),
                        coalescing_coordinator_lock_hold_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.coordinator_lock_hold_wall_ns),
                        coalescing_coordinator_lock_hold_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.coordinator_lock_hold_cpu_ns),
                        coalescing_completion_apply_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.completion_apply_wall_ns),
                        coalescing_completion_apply_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.completion_apply_cpu_ns),
                        coalescing_digest_fold_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.digest_fold_wall_ns),
                        coalescing_digest_fold_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.digest_fold_cpu_ns),
                        coalescing_cursor_advance_wall_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.cursor_advance_wall_ns),
                        coalescing_cursor_advance_cpu_ns: cursor_coalescing_timing
                            .map(|timing| timing.advance.cursor_advance_cpu_ns),
                        coalescing_iterations: cursor_coalescing_timing
                            .map(|timing| timing.iterations),
                        coalescing_ready_tickets_gained: cursor_coalescing_timing
                            .map(|timing| timing.ready_tickets_gained),
                        coalescing_state_lock_wait_wall_ns: coalescing_detail
                            .map_or(0, |detail| detail.state_lock_wait_wall_ns),
                        coalescing_state_lock_wait_cpu_ns: coalescing_detail
                            .map_or(0, |detail| detail.state_lock_wait_cpu_ns),
                        coalescing_condvar_wait_wall_ns: coalescing_detail
                            .map_or(0, |detail| detail.condvar_wait_wall_ns),
                        coalescing_condvar_wait_cpu_ns: coalescing_detail
                            .map_or(0, |detail| detail.condvar_wait_cpu_ns),
                        coalescing_condvar_wait_count: coalescing_detail
                            .map_or(0, |detail| detail.condvar_wait_count),
                        coalescing_condvar_reacquire_count: coalescing_detail
                            .map_or(0, |detail| detail.condvar_reacquire_count),
                        coalescing_condvar_non_timeout_returns: coalescing_detail
                            .map_or(0, |detail| detail.condvar_non_timeout_returns),
                        coalescing_condvar_prefix_growth_returns: coalescing_detail
                            .map_or(0, |detail| detail.condvar_prefix_growth_returns),
                        coalescing_condvar_generation_changed_returns: coalescing_detail
                            .map_or(0, |detail| detail.condvar_generation_changed_returns),
                        coalescing_condvar_timeout_returns: coalescing_detail
                            .map_or(0, |detail| detail.condvar_timeout_returns),
                        coalescing_loop_check_count: coalescing_detail
                            .map_or(0, |detail| detail.loop_check_count),
                        preparation_wall_ns,
                        preparation_cpu_ns,
                        frontier_write_wall_ns: phase_frontier_write_wall_ns,
                        frontier_write_cpu_ns: phase_frontier_write_cpu_ns,
                        fdatasync_wall_ns: phase_fdatasync_wall_ns,
                        fdatasync_cpu_ns: phase_fdatasync_cpu_ns,
                        resource_retire_before_publish_wall_ns: resource_retire_before_publish
                            .wall_ns,
                        resource_retire_before_publish_cpu_ns: resource_retire_before_publish
                            .cpu_ns,
                        resource_retire_before_publish_lock_wait_wall_ns:
                            resource_retire_before_publish.lock_wait_wall_ns,
                        resource_retire_before_publish_lock_wait_cpu_ns:
                            resource_retire_before_publish.lock_wait_cpu_ns,
                        resource_retire_before_publish_lock_hold_wall_ns:
                            resource_retire_before_publish.lock_hold_wall_ns,
                        resource_retire_before_publish_lock_hold_cpu_ns:
                            resource_retire_before_publish.lock_hold_cpu_ns,
                        resource_retire_before_publish_tickets: resource_retire_before_publish
                            .tickets,
                        resource_retire_before_publish_collect_cpu_ns:
                            resource_retire_before_publish.collect_cpu_ns,
                        resource_retire_before_publish_remove_cpu_ns:
                            resource_retire_before_publish.remove_cpu_ns,
                        resource_retire_before_publish_release_cpu_ns:
                            resource_retire_before_publish.release_cpu_ns,
                        resource_retire_before_publish_finish_cpu_ns:
                            resource_retire_before_publish.finish_cpu_ns,
                        resource_retire_before_publish_batched_point_credits:
                            resource_retire_before_publish.batch_point_frontier_credits,
                        resource_retire_before_publish_point_prefix_detached: false,
                        resource_retire_after_publish_wall_ns: resource_retire_after_publish
                            .wall_ns,
                        resource_retire_after_publish_cpu_ns: resource_retire_after_publish.cpu_ns,
                        resource_retire_after_publish_lock_wait_wall_ns:
                            resource_retire_after_publish.lock_wait_wall_ns,
                        resource_retire_after_publish_lock_wait_cpu_ns:
                            resource_retire_after_publish.lock_wait_cpu_ns,
                        resource_retire_after_publish_lock_hold_wall_ns:
                            resource_retire_after_publish.lock_hold_wall_ns,
                        resource_retire_after_publish_lock_hold_cpu_ns:
                            resource_retire_after_publish.lock_hold_cpu_ns,
                        resource_retire_after_publish_tickets: resource_retire_after_publish
                            .tickets,
                        resource_retire_after_publish_collect_cpu_ns: resource_retire_after_publish
                            .collect_cpu_ns,
                        resource_retire_after_publish_remove_cpu_ns: resource_retire_after_publish
                            .remove_cpu_ns,
                        resource_retire_after_publish_finish_cpu_ns: resource_retire_after_publish
                            .finish_cpu_ns,
                        coordinator_publish_lock_wait_ns: publish_metrics.coordinator_lock_wait_ns,
                        coordinator_publish_lock_wait_cpu_ns: publish_metrics
                            .coordinator_lock_wait_cpu_ns,
                        coordinator_publish_wall_ns: publish_metrics.publish_wall_ns,
                        coordinator_publish_cpu_ns: publish_metrics.publish_cpu_ns,
                        notify_all_wall_ns: publish_metrics.notify_all_wall_ns,
                        followers_waiting_at_publish: publish_metrics.followers_waiting,
                    });
            }
        }
    }

    fn retire_frontier_resources(
        &self,
        first_ticket: u64,
        ticket_end: u64,
        sample: bool,
    ) -> Result<WalV8RetirementObservation> {
        #[cfg(not(feature = "bench"))]
        let _ = sample;
        #[cfg(feature = "bench")]
        let started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let cpu_started = sample.then(digest::thread_cpu_time_ns);
        if self.reservation_owner.is_none() {
            return Ok(WalV8RetirementObservation::default());
        }
        let expected = usize::try_from(ticket_end - first_ticket)?;
        #[cfg(feature = "bench")]
        let lock_wait_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_wait_cpu_started = sample.then(digest::thread_cpu_time_ns);
        let mut resources = self.frontier_resources.lock();
        #[cfg(feature = "bench")]
        let lock_wait_wall_ns = lock_wait_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let lock_wait_cpu_ns = lock_wait_cpu_started
            .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let lock_hold_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_hold_cpu_started = sample.then(digest::thread_cpu_time_ns);
        #[cfg(feature = "bench")]
        let collect_started = sample.then(digest::thread_cpu_time_ns);
        #[cfg(feature = "bench")]
        let mut point_only_prefix = true;
        let mut tickets = Vec::with_capacity(expected);
        for (&ticket, _resource) in resources.range(first_ticket..ticket_end) {
            #[cfg(feature = "bench")]
            {
                point_only_prefix &= _resource.write_class == WalV8WriteClass::Point;
            }
            tickets.push(ticket);
        }
        #[cfg(feature = "bench")]
        let collect_cpu_ns = collect_started
            .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let (mut remove_cpu_ns, mut release_cpu_ns, mut finish_cpu_ns) = (0u64, 0u64, 0u64);
        ensure!(
            tickets.len() == expected,
            "v8 Parallel durable prefix is missing resource reservations"
        );

        #[cfg(feature = "bench")]
        let batch_point_frontier_credits =
            self.batch_point_frontier_credits && point_only_prefix && expected > 1;
        #[cfg(not(feature = "bench"))]
        let batch_point_frontier_credits = false;

        if batch_point_frontier_credits {
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            let mut reservations: Vec<&mut WalV7OwnedBatchReservation> = resources
                .range_mut(first_ticket..ticket_end)
                .skip(1)
                .map(|(_, resource)| &mut resource.reservation)
                .collect();
            WalV7OwnedBatchReservation::release_unused_frontier_credits_batch(&mut reservations)
                .context("release batched Point v8 FRONTIER credits")?;
            #[cfg(feature = "bench")]
            {
                release_cpu_ns = release_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
        }

        for (index, ticket) in tickets.into_iter().enumerate() {
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            let resource = resources
                .remove(&ticket)
                .context("v8 Parallel resource reservation disappeared")?;
            #[cfg(feature = "bench")]
            {
                remove_cpu_ns = remove_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
            let mut reservation = resource.reservation;
            if index > 0 && !batch_point_frontier_credits {
                #[cfg(feature = "bench")]
                let step_started = sample.then(digest::thread_cpu_time_ns);
                if let Err(error) = reservation.release_unused_frontier_credit() {
                    return Err(error).context("release coalesced v8 FRONTIER reservation");
                }
                #[cfg(feature = "bench")]
                {
                    release_cpu_ns = release_cpu_ns.saturating_add(
                        step_started
                            .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                            .unwrap_or_default(),
                    );
                }
            }
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            if let Err(error) = reservation.finish_after_io() {
                return Err(error).context("retire durable v8 batch resources");
            }
            #[cfg(feature = "bench")]
            {
                finish_cpu_ns = finish_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
        }

        #[cfg(feature = "bench")]
        let lock_hold_wall_ns = lock_hold_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let lock_hold_cpu_ns = lock_hold_cpu_started
            .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
            .unwrap_or_default();
        drop(resources);

        #[allow(unused_mut)]
        let mut observation = WalV8RetirementObservation::default();
        #[cfg(feature = "bench")]
        {
            observation.collect_cpu_ns = collect_cpu_ns;
            observation.remove_cpu_ns = remove_cpu_ns;
            observation.release_cpu_ns = release_cpu_ns;
            observation.finish_cpu_ns = finish_cpu_ns;
            observation.batch_point_frontier_credits = batch_point_frontier_credits;
            observation.tickets = u64::try_from(expected).unwrap_or(u64::MAX);
            observation.wall_ns = started
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or_default();
            observation.cpu_ns = cpu_started
                .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default();
            observation.lock_wait_wall_ns = lock_wait_wall_ns;
            observation.lock_wait_cpu_ns = lock_wait_cpu_ns;
            observation.lock_hold_wall_ns = lock_hold_wall_ns;
            observation.lock_hold_cpu_ns = lock_hold_cpu_ns;
        }

        Ok(observation)
    }

    fn retire_frontier_guards(
        &self,
        first_ticket: u64,
        ticket_end: u64,
        sample: bool,
    ) -> Result<WalV8RetirementObservation> {
        #[cfg(not(feature = "bench"))]
        let _ = sample;
        #[cfg(feature = "bench")]
        let started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let cpu_started = sample.then(digest::thread_cpu_time_ns);
        // The prefix was just published by this durability driver. Preserve
        // the guards' fail-safe behavior with one coordinator lock instead of
        // reacquiring it once for every covered ticket.
        let prefix_check = self.coordinator.poison_ticket_prefix_if_pending(ticket_end);
        #[cfg(feature = "bench")]
        let lock_wait_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_wait_cpu_started = sample.then(digest::thread_cpu_time_ns);
        let mut guards = self.producer_guards.lock();
        #[cfg(feature = "bench")]
        let lock_wait_wall_ns = lock_wait_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let lock_wait_cpu_ns = lock_wait_cpu_started
            .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let lock_hold_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_hold_cpu_started = sample.then(digest::thread_cpu_time_ns);
        #[cfg(feature = "bench")]
        let collect_started = sample.then(digest::thread_cpu_time_ns);
        let tickets: Vec<u64> = guards
            .range(first_ticket..ticket_end)
            .map(|(&ticket, _)| ticket)
            .collect();
        #[cfg(feature = "bench")]
        let collect_cpu_ns = collect_started
            .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let (mut remove_cpu_ns, mut finish_cpu_ns) = (0u64, 0u64);
        let expected = usize::try_from(ticket_end - first_ticket)?;
        ensure!(
            tickets.len() == expected,
            "v8 Parallel durable prefix is missing producer guards"
        );
        for ticket in tickets {
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            let mut encoded = guards
                .remove(&ticket)
                .context("v8 Parallel producer guard disappeared")?;
            ensure!(
                encoded.assignment().segment_ticket == ticket
                    && (first_ticket..ticket_end).contains(&ticket),
                "v8 Parallel producer guard ticket does not match its durable prefix"
            );
            #[cfg(feature = "bench")]
            {
                remove_cpu_ns = remove_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            encoded.disarm_after_durable_prefix_check(&prefix_check);
            #[cfg(feature = "bench")]
            {
                finish_cpu_ns = finish_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
        }

        #[cfg(feature = "bench")]
        let lock_hold_wall_ns = lock_hold_started
            .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        #[cfg(feature = "bench")]
        let lock_hold_cpu_ns = lock_hold_cpu_started
            .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
            .unwrap_or_default();
        drop(guards);

        #[allow(unused_mut)]
        let mut observation = WalV8RetirementObservation::default();
        #[cfg(feature = "bench")]
        {
            observation.collect_cpu_ns = collect_cpu_ns;
            observation.remove_cpu_ns = remove_cpu_ns;
            observation.finish_cpu_ns = finish_cpu_ns;
            observation.tickets = u64::try_from(expected).unwrap_or(u64::MAX);
            observation.wall_ns = started
                .map(|started| started.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                .unwrap_or_default();
            observation.cpu_ns = cpu_started
                .map(|started| digest::thread_cpu_time_ns().saturating_sub(started))
                .unwrap_or_default();
            observation.lock_wait_wall_ns = lock_wait_wall_ns;
            observation.lock_wait_cpu_ns = lock_wait_cpu_ns;
            observation.lock_hold_wall_ns = lock_hold_wall_ns;
            observation.lock_hold_cpu_ns = lock_hold_cpu_ns;
        }

        Ok(observation)
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

    #[cfg(feature = "bench")]
    pub(crate) fn take_durable_publish_sample_timestamp(&self, ticket: u64) -> Option<Instant> {
        self.coordinator
            .take_durable_publish_sample_timestamp(ticket)
    }

    pub(crate) fn next_ticket(&self) -> u64 {
        self.coordinator.next_ticket()
    }

    /// Completing callers consume published state without waiting for the
    /// physical FRONTIER mutex, which may already cover a later sync cycle.
    pub(crate) fn published_cursor(&self) -> (u64, u64, u64) {
        self.coordinator.published_cursor()
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
    #[cfg(all(test, feature = "chaos-testing"))]
    crate::chaos::failpoint::before_parallel_wal_fdatasync_call();

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

    #[cfg(feature = "chaos-testing")]
    use std::{
        os::unix::process::ExitStatusExt,
        process::{Command, Stdio},
    };

    use anyhow::Result;

    use super::*;
    use crate::pitr::{
        ArchiveEpochId, ChainAnchor, RecordedAt, SegmentId, TimelineId, WalEntry,
        manifest::ActiveBoundary,
    };
    use crate::tests::harness::is_io_uring_unavailable_error;
    use crate::wal::v7::admission::{
        WalV7ResourceCharge, WalV7ResourceLedger, WalV7ResourceLimits, WalV7RetainedResources,
    };

    #[cfg(feature = "bench")]
    #[test]
    fn coalescing_active_duration_is_unknown_without_a_wait_breakdown() {
        assert_eq!(coalescing_active_duration_ns(120, None, true), None);
        assert_eq!(coalescing_active_duration_ns(0, None, false), Some(0));
        assert_eq!(coalescing_active_duration_ns(120, Some(35), true), Some(85));
    }

    #[cfg(feature = "bench")]
    #[test]
    fn point_buffer_preallocation_permits_bound_and_release_speculation() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let permits = (0..MAX_POINT_BUFFER_PREALLOCATIONS)
            .map(|_| {
                WalV8PointBufferPreallocationPermit::try_acquire(&in_flight)
                    .expect("permit should be available below the configured bound")
            })
            .collect::<Vec<_>>();

        assert_eq!(
            in_flight.load(Ordering::Relaxed),
            MAX_POINT_BUFFER_PREALLOCATIONS
        );
        assert!(WalV8PointBufferPreallocationPermit::try_acquire(&in_flight).is_none());

        let mut permits = permits;
        let preallocated = WalV8PointBufferPreallocation {
            data_buffer: DirectBufAllocation::new(WAL_V8_FRAME_LEN),
            _permit: permits.pop().expect("one permit is available"),
        };
        assert_eq!(
            in_flight.load(Ordering::Relaxed),
            MAX_POINT_BUFFER_PREALLOCATIONS
        );

        let data_buffer = preallocated.into_data_buffer();
        assert_eq!(data_buffer.cap(), WAL_V8_FRAME_LEN);
        assert_eq!(
            in_flight.load(Ordering::Relaxed),
            MAX_POINT_BUFFER_PREALLOCATIONS - 1
        );
        drop(data_buffer);
        drop(permits);
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);

        // Dropping an abandoned pre-admission allocation releases its permit.
        let abandoned = WalV8PointBufferPreallocation {
            data_buffer: DirectBufAllocation::new(WAL_V8_FRAME_LEN),
            _permit: WalV8PointBufferPreallocationPermit::try_acquire(&in_flight)
                .expect("all permits were released"),
        };
        drop(abandoned);
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn extent_policy_vec_matches_btree_for_mixed_out_of_order_ranges() -> Result<()> {
        const START: u64 = 4096;
        const WINDOW: u64 = 16 * 1024;
        const WINDOW_COUNT: u64 = 128;

        let mut btree =
            WalV8ExtentPolicies::new(START, WINDOW, WalV8ExtentPolicyStorageKind::BTreeMap);
        let mut vector = WalV8ExtentPolicies::new(START, WINDOW, WalV8ExtentPolicyStorageKind::Vec);

        // Admission order is deliberately different from physical order. Repeated
        // registrations model Point/Batch writes sharing an extent; the first
        // policy must remain authoritative in both representations.
        let registration_order = [17, 3, 88, 0, 16, 42, 1, 127, 5, 42, 17, 64, 2, 90, 4];
        for (position, window) in registration_order.into_iter().enumerate() {
            let extent_start = START + window * WINDOW;
            let policy = if (position + window as usize).is_multiple_of(2) {
                WalV8ExtentPolicy::Lookahead
            } else {
                WalV8ExtentPolicy::FallocateOnly
            };
            let prepared_end = START + (u64::try_from(position % 6)? * WINDOW) + WINDOW / 2;
            let first_owner_policy = if extent_start < prepared_end {
                WalV8ExtentPolicy::FallocateOnly
            } else {
                policy
            };
            assert_eq!(
                btree.insert_if_absent(extent_start, first_owner_policy)?,
                vector.insert_if_absent(extent_start, first_owner_policy)?
            );
        }
        assert_eq!(btree.len(), vector.len());

        // Exercise exact window boundaries, partial windows, gaps between
        // out-of-order admissions, and endpoints immediately around a window.
        let mut endpoints = vec![START, START + 1, START + WINDOW_COUNT * WINDOW];
        for index in 0..=WINDOW_COUNT {
            let boundary = START + index * WINDOW;
            endpoints.extend([
                boundary.saturating_sub(1),
                boundary,
                boundary.saturating_add(1),
            ]);
        }
        let prepared_ends = endpoints.clone();
        for prepared_end in prepared_ends {
            for end in &endpoints {
                assert_eq!(
                    btree.has_unprepared_lookahead(*end, prepared_end, false).0,
                    vector.has_unprepared_lookahead(*end, prepared_end, false).0,
                    "prepared_end={prepared_end}, end={end}"
                );
            }
        }

        // Deterministic randomized admission and query states add coverage for
        // sparse Vec slots and first-owner-wins collisions without a new crate.
        let mut random_btree =
            WalV8ExtentPolicies::new(START, WINDOW, WalV8ExtentPolicyStorageKind::BTreeMap);
        let mut random_vector =
            WalV8ExtentPolicies::new(START, WINDOW, WalV8ExtentPolicyStorageKind::Vec);
        let mut seed = 0x7d31_9a5b_u64;
        let mut next = || {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            seed
        };
        for _ in 0..512 {
            let window = next() % WINDOW_COUNT;
            let extent_start = START + window * WINDOW;
            let prepared_end = START + (next() % (WINDOW_COUNT + 1)) * WINDOW;
            let policy = if next() & 1 == 0 {
                WalV8ExtentPolicy::Lookahead
            } else {
                WalV8ExtentPolicy::FallocateOnly
            };
            let first_owner_policy = if extent_start < prepared_end {
                WalV8ExtentPolicy::FallocateOnly
            } else {
                policy
            };
            assert_eq!(
                random_btree.insert_if_absent(extent_start, first_owner_policy)?,
                random_vector.insert_if_absent(extent_start, first_owner_policy)?
            );
        }
        for _ in 0..10_000 {
            let prepared_end = START + (next() % (WINDOW_COUNT + 2)) * WINDOW;
            let end = START + (next() % (WINDOW_COUNT + 2)) * WINDOW;
            assert_eq!(
                random_btree
                    .has_unprepared_lookahead(end, prepared_end, false)
                    .0,
                random_vector
                    .has_unprepared_lookahead(end, prepared_end, false)
                    .0,
                "random prepared_end={prepared_end}, end={end}"
            );
        }

        let truncate_end = START + 65 * WINDOW + 1;
        let btree_len = btree.len();
        let vector_len = vector.len();
        btree.truncate_before(START - 1);
        vector.truncate_before(START - 1);
        assert_eq!(btree.len(), btree_len);
        assert_eq!(vector.len(), vector_len);
        btree.truncate_before(truncate_end);
        vector.truncate_before(truncate_end);
        assert_eq!(btree.len(), vector.len());
        for index in 0..WINDOW_COUNT {
            let extent_start = START + index * WINDOW;
            assert_eq!(btree.get(extent_start), vector.get(extent_start));
        }

        Ok(())
    }

    #[test]
    fn sliding_extent_falls_back_to_exact_prefix_after_capacity_error() -> Result<()> {
        let mut preallocation_disabled = false;
        let mut calls = Vec::new();
        let prepared_end = fallocate_sliding_extent_with_fallback(
            0,
            4096,
            1024 * 1024,
            &mut preallocation_disabled,
            |start, end| {
                calls.push((start, end));
                if end == 1024 * 1024 {
                    Err(io::Error::from_raw_os_error(libc::ENOSPC).into())
                } else {
                    Ok(())
                }
            },
        )?;

        assert_eq!(prepared_end, 4096);
        assert!(preallocation_disabled);
        assert_eq!(calls, [(0, 1024 * 1024), (0, 4096)]);

        calls.clear();
        let prepared_end = fallocate_sliding_extent_with_fallback(
            4096,
            8192,
            2 * 1024 * 1024,
            &mut preallocation_disabled,
            |start, end| {
                calls.push((start, end));
                Ok(())
            },
        )?;

        assert_eq!(prepared_end, 8192);
        assert_eq!(calls, [(4096, 8192)]);

        Ok(())
    }

    #[test]
    fn sliding_extent_does_not_retry_non_capacity_errors() {
        let mut preallocation_disabled = false;
        let mut calls = Vec::new();
        let result = fallocate_sliding_extent_with_fallback(
            0,
            4096,
            1024 * 1024,
            &mut preallocation_disabled,
            |start, end| {
                calls.push((start, end));
                Err(io::Error::from_raw_os_error(libc::EIO).into())
            },
        );

        assert!(result.is_err());
        assert!(!preallocation_disabled);
        assert_eq!(calls, [(0, 1024 * 1024)]);
    }

    #[test]
    fn sliding_extent_fallback_retains_and_transfers_quota_credit() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, false)?;
        let slack_bytes = fixture.window_end - fixture.required_end;

        let mut calls = Vec::new();
        fixture
            .preallocator
            .ensure_prepared_through_with(fixture.required_end, |start, end| {
                calls.push((start, end));
                if end == fixture.window_end {
                    Err(io::Error::from_raw_os_error(libc::ENOSPC).into())
                } else {
                    Ok(())
                }
            })?;

        assert_eq!(
            calls,
            [
                (fixture.start_offset, fixture.window_end),
                (fixture.start_offset, fixture.required_end)
            ]
        );
        assert_eq!(
            fixture.preallocator.state.lock().prepared_end,
            fixture.required_end
        );
        assert_eq!(
            fixture.owner.image_owner().snapshot().source_spool_bytes,
            FIXED_IMAGE_TEST_BYTES + slack_bytes
        );
        assert!(matches!(
            fixture.owner.reserve_extent_slack(FRAME_LEN_U64),
            Err(WalV7ReservationError::SourceSpoolLimit { .. })
        ));

        fixture
            .preallocator
            .ensure_prepared_through_with(fixture.window_end, |start, end| {
                calls.push((start, end));
                Ok(())
            })?;
        assert_eq!(
            calls.last(),
            Some(&(fixture.required_end, fixture.window_end))
        );

        fixture
            .preallocator
            .consume_source_credit(fixture.required_end, fixture.window_end)?;
        assert_eq!(
            fixture.owner.image_owner().snapshot().source_spool_bytes,
            FIXED_IMAGE_TEST_BYTES
        );
        let probe = fixture.owner.reserve_extent_slack(FRAME_LEN_U64)?;
        fixture.owner.release_extent_slack(probe, FRAME_LEN_U64)?;

        Ok(())
    }

    #[test]
    fn sliding_extent_fallback_keeps_quota_when_exact_prefix_fails() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, false)?;
        let slack_bytes = fixture.window_end - fixture.required_end;

        let mut calls = Vec::new();
        let error = fixture
            .preallocator
            .ensure_prepared_through_with(fixture.required_end, |start, end| {
                calls.push((start, end));
                Err(io::Error::from_raw_os_error(libc::ENOSPC).into())
            })
            .expect_err("exact-prefix allocation should fail");

        assert!(error.to_string().contains("required V8 WAL prefix"));
        assert_eq!(
            calls,
            [
                (fixture.start_offset, fixture.window_end),
                (fixture.start_offset, fixture.required_end)
            ]
        );
        let state = fixture.preallocator.state.lock();
        assert_eq!(state.prepared_end, fixture.start_offset);
        assert!(state.window_preallocation_disabled);
        drop(state);
        assert_eq!(
            fixture.owner.image_owner().snapshot().source_spool_bytes,
            FIXED_IMAGE_TEST_BYTES + slack_bytes
        );

        Ok(())
    }

    #[test]
    fn sliding_extent_reclamation_failure_retains_quota() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let mut fixture =
            accounted_extent_preallocator_fixture(&path, 64 * 1024, FRAME_LEN_U64 * 4, false)?;
        fixture
            .preallocator
            .ensure_prepared_through_with(fixture.required_end, |_, _| Ok(()))?;
        let source_bytes = fixture.owner.image_owner().snapshot().source_spool_bytes;
        fixture.preallocator.data_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;

        fixture
            .preallocator
            .discard_speculative_tail(fixture.required_end)
            .expect_err("read-only descriptor must reject tail truncation");
        let state = fixture.preallocator.state.lock();
        assert_eq!(state.prepared_end, fixture.required_end);
        assert!(state.window_preallocation_disabled);
        assert_eq!(
            fixture.owner.image_owner().snapshot().source_spool_bytes,
            source_bytes
        );

        Ok(())
    }

    #[test]
    fn extent_lookahead_initializes_window_before_releasing_waiters() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lookahead-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, true)?;
        fixture
            .preallocator
            .ensure_prepared_through(fixture.required_end)?;

        let state = fixture.preallocator.state.lock();
        assert_eq!(state.allocated_end, fixture.window_end);
        assert_eq!(state.prepared_end, fixture.window_end);
        drop(state);
        assert_eq!(std::fs::metadata(&path)?.len(), fixture.window_end);
        assert_eq!(
            fixture.owner.image_owner().snapshot().source_spool_bytes,
            FIXED_IMAGE_TEST_BYTES + fixture.window_end - fixture.required_end
        );

        fixture
            .preallocator
            .consume_source_credit(fixture.required_end, fixture.window_end)?;
        assert_eq!(
            fixture.owner.image_owner().snapshot().source_spool_bytes,
            FIXED_IMAGE_TEST_BYTES
        );
        let prepared_bytes = std::fs::read(&path)?;
        assert!(
            prepared_bytes[fixture.start_offset as usize..fixture.window_end as usize]
                .iter()
                .all(|byte| *byte == 0)
        );

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn point_only_lookahead_keeps_batches_on_fallocate_path() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 1024 * 1024;
        const WINDOW_BYTES: u64 = WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64;

        let point_directory = tempfile::tempdir()?;
        let point_path = point_directory.path().join("point-only-lookahead-v8.wal");
        let point_fixture =
            point_only_extent_preallocator_fixture(&point_path, SEGMENT_LIMIT, WINDOW_BYTES)?;
        point_fixture.preallocator.register_data_range(
            point_fixture.start_offset,
            point_fixture.required_end,
            WalV8WriteClass::Point,
            FRAME_LEN_U64,
        )?;
        point_fixture.preallocator.register_data_range(
            point_fixture.required_end,
            point_fixture.required_end + FRAME_LEN_U64 * 3,
            WalV8WriteClass::Batch,
            FRAME_LEN_U64 * 3,
        )?;
        point_fixture
            .preallocator
            .ensure_prepared_through(point_fixture.required_end)?;

        let point_state = point_fixture.preallocator.state.lock();
        assert_eq!(point_state.prepared_end, point_fixture.window_end);
        assert_eq!(point_state.initialized_bytes, WINDOW_BYTES);
        assert_eq!(
            point_state.extent_policies.get(point_fixture.start_offset),
            Some(WalV8ExtentPolicy::Lookahead)
        );
        drop(point_state);

        let batch_directory = tempfile::tempdir()?;
        let batch_path = batch_directory.path().join("batch-fallocate-only-v8.wal");
        let batch_fixture =
            point_only_extent_preallocator_fixture(&batch_path, SEGMENT_LIMIT, WINDOW_BYTES)?;
        // Even a one-entry WriteBatch remains a Batch API request and keeps
        // the existing fallocate-only path.
        let batch_frame_bytes = FRAME_LEN_U64;
        let batch_end = batch_fixture.start_offset + batch_frame_bytes;
        batch_fixture.preallocator.register_data_range(
            batch_fixture.start_offset,
            batch_end,
            WalV8WriteClass::Batch,
            batch_frame_bytes,
        )?;
        batch_fixture.preallocator.register_data_range(
            batch_end,
            batch_end + FRAME_LEN_U64,
            WalV8WriteClass::Point,
            FRAME_LEN_U64,
        )?;
        batch_fixture
            .preallocator
            .ensure_prepared_through(batch_end)?;

        let batch_state = batch_fixture.preallocator.state.lock();
        assert_eq!(batch_state.prepared_end, batch_fixture.window_end);
        assert_eq!(batch_state.initialized_bytes, 0);
        assert_eq!(
            batch_state.extent_policies.get(batch_fixture.start_offset),
            Some(WalV8ExtentPolicy::FallocateOnly)
        );

        let large_directory = tempfile::tempdir()?;
        let large_path = large_directory
            .path()
            .join("large-point-fallocate-only-v8.wal");
        let large_fixture =
            point_only_extent_preallocator_fixture(&large_path, SEGMENT_LIMIT, WINDOW_BYTES)?;
        large_fixture.preallocator.register_data_range(
            large_fixture.start_offset,
            large_fixture.start_offset + FRAME_LEN_U64 * 3,
            WalV8WriteClass::Point,
            FRAME_LEN_U64 * 3,
        )?;
        assert_eq!(
            large_fixture
                .preallocator
                .state
                .lock()
                .extent_policies
                .get(large_fixture.start_offset),
            Some(WalV8ExtentPolicy::FallocateOnly)
        );

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn extent_lookahead_records_condvar_waits() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lookahead-wait-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, true)?;
        let required_end = fixture.required_end;
        let preallocator = Arc::new(fixture.preallocator);
        let preparation = preallocator.preparation_lock.lock();
        let waiting_preallocator = Arc::clone(&preallocator);
        let waiter =
            thread::spawn(move || waiting_preallocator.ensure_prepared_through(required_end));

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut observed_wait = false;
        while Instant::now() < deadline {
            if preallocator.state.lock().wait_calls > 0 {
                observed_wait = true;
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        drop(preparation);
        waiter
            .join()
            .expect("lookahead preparation waiter panicked")?;

        assert!(observed_wait, "lookahead wait counter was not recorded");
        let diagnostics = preallocator.diagnostics();
        assert_eq!(diagnostics.wait_calls, 1);
        assert!(diagnostics.wait_ns > 0);

        Ok(())
    }

    #[test]
    fn extent_lookahead_falls_back_to_quota_covered_prefix() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lookahead-quota-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, true)?;
        let slack_bytes = fixture.window_end - fixture.required_end;
        let occupying_reservation = fixture.owner.reserve_extent_slack(slack_bytes)?;

        fixture
            .preallocator
            .ensure_prepared_through(fixture.required_end)?;

        let state = fixture.preallocator.state.lock();
        assert_eq!(state.allocated_end, fixture.required_end);
        assert_eq!(state.prepared_end, fixture.required_end);
        assert!(state.slack.is_empty());
        drop(state);
        assert_eq!(std::fs::metadata(&path)?.len(), fixture.required_end);
        fixture
            .owner
            .release_extent_slack(occupying_reservation, slack_bytes)?;

        Ok(())
    }

    #[test]
    fn extent_lookahead_failure_wakes_preparation_waiters() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lookahead-failure-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, false)?;
        let AccountedExtentPreallocatorFixture {
            owner,
            preallocator,
            start_offset,
            required_end,
            ..
        } = fixture;
        drop(preallocator);
        let data_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let preallocator = WalV8ExtentPreallocator::new_with_lookahead(
            data_file,
            &sync_file,
            start_offset,
            SEGMENT_LIMIT,
            WINDOW_BYTES,
            Some(owner),
            true,
        )?;

        assert!(preallocator.ensure_prepared_through(required_end).is_err());
        assert!(preallocator.state.lock().failure.is_some());

        Ok(())
    }

    #[test]
    fn extent_lookahead_does_not_publish_a_partial_chunk() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lookahead-partial-v8.wal");
        let fixture =
            accounted_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES, false)?;
        let initialized_start = fixture.start_offset;
        let initialized_end = initialized_start + 2 * FRAME_LEN_U64;
        fixture.preallocator.state.lock().allocated_end = initialized_end;

        let mut partial = DirectBuf::new(WAL_V8_FRAME_LEN);
        partial.zero_range(0, WAL_V8_FRAME_LEN);
        let error = initialize_v8_extent_chunk(
            &fixture.preallocator.state,
            initialized_start,
            initialized_end,
            || {
                fixture
                    .preallocator
                    .data_file
                    .write_all_at(
                        partial.initialized_slice(0, WAL_V8_FRAME_LEN),
                        initialized_start,
                    )
                    .context("write first frame of injected partial initialization")?;
                anyhow::bail!("injected partial extent initialization failure")
            },
        )
        .expect_err("partial initialization must not publish its chunk");

        assert!(error.to_string().contains("injected partial"));
        assert_eq!(
            fixture.preallocator.state.lock().prepared_end,
            initialized_start
        );
        assert_eq!(
            std::fs::metadata(&path)?.len(),
            initialized_start + FRAME_LEN_U64
        );

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn frontier_write_mode_parses_only_supported_values() {
        assert_eq!(
            WalV8FrontierWriteMode::from_value(None).unwrap(),
            WalV8FrontierWriteMode::Buffered
        );
        assert_eq!(
            WalV8FrontierWriteMode::from_value(Some(OsStr::new("buffered"))).unwrap(),
            WalV8FrontierWriteMode::Buffered
        );
        assert_eq!(
            WalV8FrontierWriteMode::from_value(Some(OsStr::new("direct"))).unwrap(),
            WalV8FrontierWriteMode::Direct
        );
        assert!(WalV8FrontierWriteMode::from_value(Some(OsStr::new("invalid"))).is_err());
    }

    #[cfg(feature = "bench")]
    #[test]
    fn v8_extent_preparation_parses_modes_and_aligned_lengths() {
        let chunk_bytes = WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64;
        assert_eq!(
            WalExtentPreparationMode::from_values(None, None).unwrap(),
            None
        );
        assert_eq!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("fallocate")),
                Some(OsStr::new("262144")),
            )
            .unwrap(),
            Some((WalExtentPreparationMode::Fallocate, chunk_bytes * 2))
        );
        assert_eq!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("initialize")),
                Some(OsStr::new("131072")),
            )
            .unwrap(),
            Some((WalExtentPreparationMode::Initialize, chunk_bytes))
        );
        assert_eq!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("sliding-fallocate")),
                Some(OsStr::new("1048576")),
            )
            .unwrap(),
            Some((WalExtentPreparationMode::SlidingFallocate, 1024 * 1024,))
        );
        assert_eq!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("sliding-initialize")),
                Some(OsStr::new("1048576")),
            )
            .unwrap(),
            Some((WalExtentPreparationMode::SlidingInitialize, 1024 * 1024,))
        );
        assert_eq!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("sliding-lookahead")),
                Some(OsStr::new("1048576")),
            )
            .unwrap(),
            Some((WalExtentPreparationMode::SlidingLookahead, 1024 * 1024,))
        );
        assert_eq!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("sliding-point-lookahead")),
                Some(OsStr::new("1048576")),
            )
            .unwrap(),
            Some((WalExtentPreparationMode::SlidingPointLookahead, 1024 * 1024,))
        );
        assert!(WalExtentPreparationMode::from_values(None, Some(OsStr::new("131072"))).is_err());
        assert!(WalExtentPreparationMode::from_values(Some(OsStr::new("none")), None).is_err());
        assert!(
            WalExtentPreparationMode::from_values(Some(OsStr::new("fallocate")), None).is_err()
        );
        assert!(
            WalExtentPreparationMode::from_values(
                Some(OsStr::new("initialize")),
                Some(OsStr::new("4096"))
            )
            .is_err()
        );
    }

    #[test]
    fn production_extent_lookahead_is_explicitly_opt_in() {
        assert!(!wal_v8_extent_lookahead_from_value(None).unwrap());
        assert!(!wal_v8_extent_lookahead_from_value(Some(OsStr::new("0"))).unwrap());
        assert!(!wal_v8_extent_lookahead_from_value(Some(OsStr::new("false"))).unwrap());
        assert!(wal_v8_extent_lookahead_from_value(Some(OsStr::new("1"))).unwrap());
        assert!(wal_v8_extent_lookahead_from_value(Some(OsStr::new("true"))).unwrap());
        assert!(wal_v8_extent_lookahead_from_value(Some(OsStr::new("yes"))).is_err());
    }

    #[cfg(feature = "bench")]
    #[test]
    fn progress_ring_experiment_switch_is_explicitly_opt_in() {
        assert_eq!(
            progress_mode_from_value(None).unwrap(),
            WalV8ProgressMode::Eager
        );
        assert_eq!(
            progress_mode_from_value(Some(OsStr::new("0"))).unwrap(),
            WalV8ProgressMode::Eager
        );
        assert_eq!(
            progress_mode_from_value(Some(OsStr::new("2"))).unwrap(),
            WalV8ProgressMode::Cursor
        );
        assert!(progress_mode_from_value(Some(OsStr::new("1"))).is_err());
        assert!(progress_mode_from_value(Some(OsStr::new("true"))).is_err());
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_direct_completion_switch_requires_cursor_mode() {
        assert!(
            !cursor_direct_completion_enabled_from_value(None, WalV8ProgressMode::Cursor).unwrap()
        );
        assert!(
            !cursor_direct_completion_enabled_from_value(
                Some(OsStr::new("0")),
                WalV8ProgressMode::Cursor
            )
            .unwrap()
        );
        assert!(
            cursor_direct_completion_enabled_from_value(
                Some(OsStr::new("1")),
                WalV8ProgressMode::Cursor
            )
            .unwrap()
        );
        assert!(
            cursor_direct_completion_enabled_from_value(
                Some(OsStr::new("1")),
                WalV8ProgressMode::Eager
            )
            .is_err()
        );
        assert!(
            cursor_direct_completion_enabled_from_value(
                Some(OsStr::new("2")),
                WalV8ProgressMode::Cursor
            )
            .is_err()
        );
    }

    #[cfg(feature = "bench")]
    #[test]
    fn direct_completion_wake_mode_is_opt_in_and_requires_cursor_direct_completion() {
        assert_eq!(
            direct_completion_wake_mode_from_value(None, WalV8ProgressMode::Cursor, true,).unwrap(),
            WalV8DirectCompletionWakeMode::Token
        );
        assert_eq!(
            direct_completion_wake_mode_from_value(
                Some(OsStr::new("token")),
                WalV8ProgressMode::Cursor,
                true,
            )
            .unwrap(),
            WalV8DirectCompletionWakeMode::Token
        );
        assert_eq!(
            direct_completion_wake_mode_from_value(
                Some(OsStr::new("select")),
                WalV8ProgressMode::Cursor,
                true,
            )
            .unwrap(),
            WalV8DirectCompletionWakeMode::Select
        );
        assert!(
            direct_completion_wake_mode_from_value(
                Some(OsStr::new("select")),
                WalV8ProgressMode::Eager,
                true,
            )
            .is_err()
        );
        assert!(
            direct_completion_wake_mode_from_value(
                Some(OsStr::new("select")),
                WalV8ProgressMode::Cursor,
                false,
            )
            .is_err()
        );
        assert!(
            direct_completion_wake_mode_from_value(
                Some(OsStr::new("invalid")),
                WalV8ProgressMode::Cursor,
                true,
            )
            .is_err()
        );
    }

    #[cfg(all(feature = "bench", unix))]
    #[test]
    fn direct_completion_wake_mode_rejects_non_utf8_values() {
        use std::os::unix::ffi::OsStrExt;

        let invalid = OsStr::from_bytes(b"select\xff");
        assert!(
            direct_completion_wake_mode_from_value(
                Some(invalid),
                WalV8ProgressMode::Cursor,
                true,
            )
            .is_err()
        );
    }

    #[cfg(feature = "bench")]
    #[test]
    fn direct_completion_select_checks_control_first_and_delivers_each_event_once() -> Result<()> {
        let (control_tx, control_rx) = crossbeam_channel::bounded(1);
        let (completion_tx, completion_rx) = crossbeam_channel::bounded(1);
        let completion = DirectGroupWriteCompletion {
            result: GroupWriteResult {
                group_id: 7,
                tickets: 3..4,
                write_count: 1,
                write_bytes: FRAME_LEN_U64,
                error: None,
            },
            completion_observed_at: Some(Instant::now()),
        };
        completion_tx.send(completion.clone())?;
        control_tx.send(())?;

        let control = wait_for_direct_completion_or_control(
            &control_rx,
            &completion_rx,
            Duration::from_secs(1),
        )
        .map_err(|closed| anyhow::anyhow!("unexpected closed channel: {closed:?}"))?;
        assert!(matches!(control, WalV8ProgressWake::Control));
        assert_eq!(
            completion_rx.len(),
            1,
            "control wake consumed the DATA event"
        );

        let data = wait_for_direct_completion_or_control(
            &control_rx,
            &completion_rx,
            Duration::from_secs(1),
        )
        .map_err(|closed| anyhow::anyhow!("unexpected closed channel: {closed:?}"))?;
        match data {
            WalV8ProgressWake::Completion(observed) => assert_eq!(observed, completion),
            WalV8ProgressWake::Control | WalV8ProgressWake::TimedOut => {
                anyhow::bail!("queued DATA completion was not selected")
            }
        }
        assert!(
            completion_rx.is_empty(),
            "DATA completion was delivered more than once"
        );

        let timeout =
            wait_for_direct_completion_or_control(&control_rx, &completion_rx, Duration::ZERO)
                .map_err(|closed| anyhow::anyhow!("unexpected closed channel: {closed:?}"))?;
        assert!(matches!(timeout, WalV8ProgressWake::TimedOut));

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_singleton_submission_switch_requires_cursor_mode() {
        assert!(
            !cursor_singleton_submission_enabled_from_value(None, WalV8ProgressMode::Cursor)
                .unwrap()
        );
        assert!(
            !cursor_singleton_submission_enabled_from_value(
                Some(OsStr::new("0")),
                WalV8ProgressMode::Cursor
            )
            .unwrap()
        );
        assert!(
            cursor_singleton_submission_enabled_from_value(
                Some(OsStr::new("1")),
                WalV8ProgressMode::Cursor
            )
            .unwrap()
        );
        assert!(
            cursor_singleton_submission_enabled_from_value(
                Some(OsStr::new("1")),
                WalV8ProgressMode::Eager
            )
            .is_err()
        );
        assert!(
            cursor_singleton_submission_enabled_from_value(
                Some(OsStr::new("2")),
                WalV8ProgressMode::Cursor
            )
            .is_err()
        );
    }

    #[cfg(feature = "bench")]
    #[test]
    fn lightweight_progress_counters_are_opt_in_for_cursor_mode() {
        assert!(!progress_counters_enabled_from_value(None, WalV8ProgressMode::Cursor).unwrap());
        assert!(
            !progress_counters_enabled_from_value(Some(OsStr::new("0")), WalV8ProgressMode::Cursor)
                .unwrap()
        );
        assert!(
            progress_counters_enabled_from_value(Some(OsStr::new("1")), WalV8ProgressMode::Cursor)
                .unwrap()
        );
        assert!(
            progress_counters_enabled_from_value(Some(OsStr::new("1")), WalV8ProgressMode::Eager)
                .is_err()
        );
        assert!(
            progress_counters_enabled_from_value(
                Some(OsStr::new("true")),
                WalV8ProgressMode::Cursor
            )
            .is_err()
        );
    }

    #[cfg(all(feature = "bench", unix))]
    #[test]
    fn progress_ring_experiment_switch_rejects_non_utf8_values() {
        use std::os::unix::ffi::OsStrExt;

        let non_utf8 = OsStr::from_bytes(b"\xff");
        assert!(progress_mode_from_value(Some(non_utf8)).is_err());
    }

    #[cfg(not(feature = "bench"))]
    #[test]
    fn production_progress_mode_is_eager_by_default_and_cursor_is_opt_in() {
        assert_eq!(
            production_progress_mode_from_value(None).unwrap(),
            WalV8ProgressMode::Eager
        );
        assert_eq!(
            production_progress_mode_from_value(Some(OsStr::new("eager"))).unwrap(),
            WalV8ProgressMode::Eager
        );
        assert_eq!(
            production_progress_mode_from_value(Some(OsStr::new("cursor"))).unwrap(),
            WalV8ProgressMode::Cursor
        );
        assert!(production_progress_mode_from_value(Some(OsStr::new("1"))).is_err());
        assert!(production_progress_mode_from_value(Some(OsStr::new("ring"))).is_err());
    }

    #[cfg(all(not(feature = "bench"), unix))]
    #[test]
    fn production_progress_mode_rejects_non_utf8_values() {
        use std::os::unix::ffi::OsStrExt;

        let non_utf8 = OsStr::from_bytes(b"\xff");
        assert!(production_progress_mode_from_value(Some(non_utf8)).is_err());
    }

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

    #[cfg(feature = "bench")]
    #[test]
    fn background_sliding_initializer_prepares_multiple_windows_for_concurrent_waiters()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sliding-initialize.wal");
        let installed = install_empty(&path, header())?;
        let start_offset = installed.image_len;
        let window_bytes = (WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64) * 2;
        let max_file_size = start_offset + window_bytes * 4;
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let preparation = Arc::new(WalV8SlidingExtentPreparation::new(
            data_file,
            &sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            WalExtentPreparationMode::SlidingInitialize,
        )?);

        let required_ends = [
            start_offset + 4096,
            start_offset + window_bytes + 4096,
            start_offset + window_bytes * 3 - 4096,
        ];
        let waiters: Vec<_> = required_ends
            .into_iter()
            .map(|required_end| {
                let preparation = Arc::clone(&preparation);
                thread::spawn(move || preparation.ensure_prepared_through(required_end))
            })
            .collect();
        for waiter in waiters {
            waiter.join().expect("extent preparation waiter panicked")?;
        }

        let diagnostics = preparation.diagnostics();
        assert_eq!(diagnostics.prepared_end, start_offset + window_bytes * 3);
        assert_eq!(diagnostics.fallocate_calls, 3);
        assert_eq!(diagnostics.initialized_bytes, window_bytes * 3);
        assert_eq!(
            diagnostics.initialization_chunks,
            window_bytes * 3 / WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64
        );
        drop(preparation);

        let bytes = std::fs::read(&path)?;
        assert!(bytes[start_offset as usize..].iter().all(|byte| *byte == 0));

        Ok(())
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

    #[cfg(feature = "bench")]
    fn multi_window_batch(commit_ts: u64) -> WalBatch {
        WalBatch {
            entries: (0_u64..12)
                .map(|index| WalEntry::Put {
                    key: index.to_be_bytes().to_vec(),
                    value: vec![0x5a; 48 * 1024],
                })
                .collect(),
            ..batch(commit_ts)
        }
    }

    fn multi_frame_batch(commit_ts: u64) -> WalBatch {
        WalBatch {
            entries: vec![WalEntry::Put {
                key: b"k".to_vec(),
                value: vec![0x5a; WAL_V8_FRAME_LEN],
            }],
            ..batch(commit_ts)
        }
    }

    #[test]
    fn v8_data_buffers_are_not_retained_by_the_generic_worker_pool() {
        let pool = ArrayQueue::new(1);
        let buffer = WalV8DataBuffer(DirectBuf::new(crate::wal::BUFFER_POOL_BUF_SIZE));

        buffer.retire(&pool);

        assert!(pool.is_empty());
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
        let parallel_leader = open_writer(&parallel_path, &parallel_installed)?;
        let worker_file = parallel_leader.write_file.try_clone()?;
        let parallel = parallel_leader.into_parallel_with_worker_file_and_progress_mode(
            worker_file,
            WalV8ProgressMode::Eager,
        )?;
        parallel.set_write_profile(Arc::clone(&parallel_profile))?;
        let first_batch = batch(10);
        let second_batch = batch(20);
        let first = parallel.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?;
        let second = parallel.preflight_batch(&second_batch, LIVE_WAL_V5_LIMITS)?;
        let first_job = parallel.admit_prepared_batch(first, 10, first_batch.recorded_at)?;
        let second_job = parallel.admit_prepared_batch(second, 20, second_batch.recorded_at)?;
        first_job.finish()?;
        second_job.finish()?;
        let deadline = Instant::now() + Duration::from_secs(2);
        while parallel.coordinator.frontier_ready_prefix().len() < 2 {
            ensure!(
                Instant::now() < deadline,
                "both v8 tickets did not become frontier-ready"
            );
            thread::yield_now();
        }
        parallel.wait_durable(1)?;
        assert_one_two_ticket_profile(&parallel_profile);

        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
    #[test]
    fn failpoint_parallel_writer_crashes_at_durability_boundaries() -> Result<()> {
        const CHILD_WAL_ENV: &str = "TOY_KV_V8_PARALLEL_CRASH_CHILD_WAL";
        const CHILD_ACK_ENV: &str = "TOY_KV_V8_PARALLEL_CRASH_CHILD_ACK";
        const CHILD_FRONTIER_MODE_ENV: &str = "TOY_KV_V8_PARALLEL_CRASH_FRONTIER_MODE";
        const FRONTIER_MODE_ENV: &str = "TOYKV_WAL_V8_FRONTIER_MODE";
        const EXTENT_PREPARATION_ENV: &str = "TOYKV_BENCH_V8_EXTENT_PREP";
        const EXTENT_PREPARATION_BYTES_ENV: &str = "TOYKV_BENCH_V8_EXTENT_PREP_BYTES";
        const TEST_NAME: &str =
            "wal::v8::leader::tests::failpoint_parallel_writer_crashes_at_durability_boundaries";
        const CRASH_POINTS: [(&str, u64, u64); 4] = [
            ("wal_v8.parallel.after_data_write", 0, 0),
            ("wal_v8.parallel.after_frontier_write", 0, 1),
            ("wal_v8.parallel.after_fdatasync", 1, 1),
            ("wal_v8.parallel.after_durable_publish", 1, 1),
        ];

        if let (Some(wal_path), Some(ack_path)) = (
            std::env::var_os(CHILD_WAL_ENV),
            std::env::var_os(CHILD_ACK_ENV),
        ) {
            let wal_path = Path::new(&wal_path);
            let scratch_path = wal_path.with_extension("empty-fixture.wal");
            let boundary = create_empty_active(&scratch_path, header())?;
            std::fs::remove_file(&scratch_path)?;
            let installed = super::super::install::recover_and_install_active(
                wal_path,
                &[boundary],
                header().predecessor,
                LIVE_WAL_V5_LIMITS,
                MAX_WAL_FILE_SIZE,
            )?;
            #[cfg(feature = "bench")]
            let writer = if std::env::var(CHILD_FRONTIER_MODE_ENV).as_deref() == Ok("direct")
                || std::env::var_os(EXTENT_PREPARATION_ENV).is_some()
            {
                let worker_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(wal_path)?;
                if std::env::var_os(EXTENT_PREPARATION_ENV)
                    .as_deref()
                    .is_some_and(|mode| {
                        mode == OsStr::new("sliding-lookahead")
                            || mode == OsStr::new("sliding-point-lookahead")
                    })
                {
                    let sync_file = OpenOptions::new().read(true).write(true).open(wal_path)?;
                    let (leader, _owner) = open_accounted_writer(
                        wal_path,
                        &installed,
                        MAX_WAL_FILE_SIZE,
                        2 * MAX_WAL_FILE_SIZE,
                        FRAME_LEN_U64,
                        sync_file,
                    )?;
                    leader.into_parallel_with_worker_file(worker_file)?
                } else {
                    open_writer(wal_path, &installed)?
                        .into_parallel_with_worker_file(worker_file)?
                }
            } else {
                open_writer(wal_path, &installed)?.into_parallel()?
            };
            #[cfg(feature = "bench")]
            {
                let extent_mode = std::env::var_os(EXTENT_PREPARATION_ENV);
                let expected_len = match extent_mode.as_deref().and_then(OsStr::to_str) {
                    Some("fallocate" | "initialize") => {
                        installed.image_len + WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64
                    }
                    None
                    | Some(
                        "sliding-fallocate"
                        | "sliding-initialize"
                        | "sliding-lookahead"
                        | "sliding-point-lookahead",
                    ) => installed.image_len,
                    Some(other) => anyhow::bail!("unexpected V8 extent preparation mode {other:?}"),
                };
                ensure!(
                    std::fs::metadata(wal_path)?.len() == expected_len,
                    "V8 crash fixture has an unexpected pre-write physical length"
                );
                ensure!(
                    std::env::var_os(EXTENT_PREPARATION_BYTES_ENV).is_some()
                        == std::env::var_os(EXTENT_PREPARATION_ENV).is_some(),
                    "V8 crash fixture extent mode and byte count must be configured together"
                );
            }
            #[cfg(not(feature = "bench"))]
            let writer = open_writer(wal_path, &installed)?.into_parallel()?;
            #[cfg(feature = "bench")]
            {
                let requested = std::env::var(WAL_V8_POINT_PROGRESS_RING_ENV)?;
                assert_eq!(
                    writer.progress_ring_enabled(),
                    matches!(requested.as_str(), "1" | "2"),
                    "child writer must use the requested progress ring mode"
                );
                assert_eq!(
                    writer.coordinator.progress_cursor_enabled(),
                    requested == "2",
                    "child writer must use the requested progress cursor mode"
                );
            }
            #[cfg(feature = "bench")]
            let value = if std::env::var_os(EXTENT_PREPARATION_ENV).as_deref()
                == Some(OsStr::new("sliding-initialize"))
            {
                multi_window_batch(10)
            } else {
                batch(10)
            };
            #[cfg(not(feature = "bench"))]
            let value = batch(10);
            writer.append_prepared_batch(
                writer.preflight_batch(&value, LIVE_WAL_V5_LIMITS)?,
                10,
                value.recorded_at,
            )?;
            std::fs::write(ack_path, b"acknowledged")?;
            anyhow::bail!("configured V8 writer crash point did not stop the child")
        }

        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        #[cfg(feature = "bench")]
        let frontier_modes = ["buffered", "direct"];
        #[cfg(not(feature = "bench"))]
        let frontier_modes = ["buffered"];
        #[cfg(feature = "bench")]
        let extent_preparation_modes = [
            "none",
            "fallocate",
            "initialize",
            "sliding-fallocate",
            "sliding-initialize",
            "sliding-lookahead",
            "sliding-point-lookahead",
        ];
        #[cfg(not(feature = "bench"))]
        let extent_preparation_modes = ["none"];

        for frontier_mode in frontier_modes {
            for extent_preparation_mode in extent_preparation_modes {
                let mut cases = CRASH_POINTS
                    .map(|(point, min, max)| ("0", point, min, max))
                    .to_vec();
                if cfg!(feature = "bench")
                    && matches!(
                        (frontier_mode, extent_preparation_mode),
                        ("buffered", "none") | ("direct", "sliding-point-lookahead")
                    )
                {
                    cases.extend(CRASH_POINTS.map(|(point, min, max)| ("2", point, min, max)));
                }
                for (
                    progress_mode,
                    crash_point,
                    expected_min_ticket_end,
                    expected_max_ticket_end,
                ) in cases
                {
                    let directory = tempfile::tempdir_in(&parent)?;
                    let wal_path = directory.path().join("active-v8.wal");
                    let ack_path = directory.path().join("acknowledged");
                    let marker_path = directory.path().join("crash.marker");
                    let anchor = create_empty_active(&wal_path, header())?;

                    let mut child = Command::new(std::env::current_exe()?);
                    child
                        .arg("--exact")
                        .arg(TEST_NAME)
                        .arg("--nocapture")
                        .env(CHILD_WAL_ENV, &wal_path)
                        .env(CHILD_ACK_ENV, &ack_path)
                        .env(CHILD_FRONTIER_MODE_ENV, frontier_mode)
                        .env(FRONTIER_MODE_ENV, frontier_mode)
                        .env_remove(EXTENT_PREPARATION_ENV)
                        .env_remove(EXTENT_PREPARATION_BYTES_ENV)
                        .env(
                            crate::chaos::failpoint::PARALLEL_WAL_CRASH_POINT_ENV,
                            crash_point,
                        )
                        .env(
                            crate::chaos::failpoint::PARALLEL_WAL_CRASH_OCCURRENCE_ENV,
                            "1",
                        )
                        .env(
                            crate::chaos::failpoint::PARALLEL_WAL_CRASH_MARKER_ENV,
                            &marker_path,
                        )
                        .stdout(Stdio::null())
                        .stderr(Stdio::inherit());
                    #[cfg(feature = "bench")]
                    child.env(WAL_V8_POINT_PROGRESS_RING_ENV, progress_mode);
                    #[cfg(feature = "bench")]
                    if extent_preparation_mode != "none" {
                        child
                            .env(EXTENT_PREPARATION_ENV, extent_preparation_mode)
                            .env(
                                EXTENT_PREPARATION_BYTES_ENV,
                                WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES.to_string(),
                            );
                    }
                    let mut child = child.spawn()?;
                    let deadline = Instant::now() + Duration::from_secs(30);
                    loop {
                        if marker_path.is_file() {
                            break;
                        }
                        if let Some(status) = child.try_wait()? {
                            anyhow::bail!(
                                "V8 writer child exited before reaching {crash_point} in {frontier_mode}/{extent_preparation_mode} mode (progress mode={progress_mode}): {status}"
                            );
                        }
                        if Instant::now() >= deadline {
                            let _ = child.kill();
                            let _ = child.wait();
                            anyhow::bail!(
                                "V8 writer child did not reach {crash_point} in {frontier_mode}/{extent_preparation_mode} mode (progress mode={progress_mode})"
                            );
                        }
                        thread::sleep(Duration::from_millis(10));
                    }

                    child.kill()?;
                    let status = child.wait()?;
                    ensure!(
                        status.signal() == Some(libc::SIGKILL),
                        "V8 writer child was not killed at {crash_point} in {frontier_mode}/{extent_preparation_mode} mode: {status}"
                    );
                    assert_eq!(std::fs::read_to_string(&marker_path)?.trim(), crash_point);
                    assert!(!ack_path.exists(), "crashed V8 write was acknowledged");

                    #[cfg(feature = "bench")]
                    if extent_preparation_mode == "sliding-fallocate" {
                        ensure!(
                            std::fs::metadata(&wal_path)?.len()
                                == HEADER_LEN_U64
                                    + FRAME_LEN_U64
                                    + WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64,
                            "sliding V8 extent preparation did not cover the first DATA write"
                        );
                    }
                    #[cfg(feature = "bench")]
                    if extent_preparation_mode == "sliding-initialize" {
                        let physical_len = std::fs::metadata(&wal_path)?.len();
                        let preparation_start = HEADER_LEN_U64 + FRAME_LEN_U64;
                        ensure!(
                            physical_len
                                >= preparation_start
                                    + (WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64) * 5
                                && (physical_len - preparation_start)
                                    .is_multiple_of(WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64),
                            "sliding V8 initialization did not prepare the multi-window DATA range"
                        );
                    }
                    #[cfg(feature = "bench")]
                    if matches!(
                        extent_preparation_mode,
                        "sliding-lookahead" | "sliding-point-lookahead"
                    ) {
                        ensure!(
                            std::fs::metadata(&wal_path)?.len()
                                == HEADER_LEN_U64
                                    + FRAME_LEN_U64
                                    + WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64,
                            "sliding V8 lookahead did not initialize its full first window"
                        );
                    }

                    let recovered = super::super::install::recover_and_install_active(
                        &wal_path,
                        &[anchor],
                        header().predecessor,
                        LIVE_WAL_V5_LIMITS,
                        MAX_WAL_FILE_SIZE,
                    )?;
                    assert_eq!(
                        std::fs::metadata(&wal_path)?.len(),
                        recovered.image_len,
                        "recovery retained a preallocated suffix after {crash_point} in {frontier_mode}/{extent_preparation_mode} mode"
                    );
                    assert!(
                        (expected_min_ticket_end..=expected_max_ticket_end)
                            .contains(&recovered.active_boundary.ticket_end),
                        "recovered ticket end {} is outside the permitted range for {crash_point}",
                        recovered.active_boundary.ticket_end,
                    );
                    let batches = replayed_batches(&wal_path, &recovered)?;
                    assert_eq!(
                        batches.len() as u64,
                        recovered.active_boundary.ticket_end,
                        "replay count differs from recovered boundary at {crash_point} in {frontier_mode} mode"
                    );
                    if recovered.active_boundary.ticket_end == 1 {
                        #[cfg(feature = "bench")]
                        let expected_batch = if extent_preparation_mode == "sliding-initialize" {
                            multi_window_batch(10)
                        } else {
                            batch(10)
                        };
                        #[cfg(not(feature = "bench"))]
                        let expected_batch = batch(10);
                        assert_eq!(batches, [expected_batch]);
                    } else {
                        assert!(batches.is_empty());
                    }
                }
            }
        }

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn parallel_sync_diagnostics_transition_uses_the_recovered_ticket_boundary() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("diagnostics-v8.wal");
        let mut leader = open_writer(&path, &install_empty(&path, header())?)?;
        leader.append_batch(&batch(10), LIVE_WAL_V5_LIMITS)?;
        let (_, boundary) = leader.active_boundaries()?;
        drop(leader);

        let installed = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            header().predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(installed.next_ticket, 1);

        let writer = open_writer(&path, &installed)?.into_parallel()?;
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        writer.set_write_profile(Arc::clone(&profile))?;
        writer.set_wal_sync_diagnostics_enabled(&profile, true)?;

        let next = batch(20);
        writer.append_prepared_batch(
            writer.preflight_batch(&next, LIVE_WAL_V5_LIMITS)?,
            20,
            next.recorded_at,
        )?;
        let observations = profile.wal_sync_observations();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].tickets_covered, Some(1));
        assert!(observations[0].frontier_write_ns > 0);
        assert!(observations[0].duration_ns > 0);
        assert!(
            observations[0]
                .data_completion_to_frontier_start_ns
                .is_some()
        );
        writer.set_wal_sync_diagnostics_enabled(&profile, false)?;
        let error = writer
            .set_wal_sync_diagnostics_enabled(&profile, true)
            .expect_err("diagnostics re-enable after a new ticket must be rejected");
        assert!(error.to_string().contains("before the first WAL ticket"));
        assert!(!profile.wal_sync_diagnostics_enabled());

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn progress_ring_rejects_incompatible_profile_inherited_from_leader() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory
            .path()
            .join("progress-ring-inherited-profile-v8.wal");
        let installed = install_empty(&path, header())?;
        let leader = open_writer(&path, &installed)?;
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        profile.set_wal_hol_diagnostics_enabled(true);
        leader.set_write_profile(Arc::clone(&profile));

        let worker_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;
        let error = match leader.into_parallel_with_worker_file_and_progress_mode(
            worker_file,
            WalV8ProgressMode::Cursor,
        ) {
            Ok(writer) => {
                drop(writer);
                anyhow::bail!("parallel conversion accepted an incompatible inherited profile")
            }
            Err(error) => error,
        };
        assert!(error.to_string().contains("V8 HOL diagnostics"));

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn progress_ring_rejects_hol_and_timeline_but_samples_durability_phases() -> Result<()> {
        let directory = tempfile::tempdir()?;
        for direct_completion in [false, true] {
            let mode_name = if direct_completion { "direct" } else { "relay" };
            let path = directory
                .path()
                .join(format!("progress-ring-profile-{mode_name}-v8.wal"));
            let installed = install_empty(&path, header())?;
            let writer = if direct_completion {
                open_parallel_with_direct_completion(
                    &path,
                    &installed,
                    WalV8ProgressMode::Cursor,
                    None,
                )
            } else {
                open_parallel_with_progress_mode(&path, &installed, WalV8ProgressMode::Cursor)
            };
            let writer = match writer {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping {mode_name} case (io_uring unavailable): {error:#}");
                    if direct_completion {
                        continue;
                    }
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            if !direct_completion {
                let hol_profile = Arc::new(crate::mem_table::WriteProfile::default());
                hol_profile.set_wal_hol_diagnostics_enabled(true);
                let error = writer
                    .set_write_profile(Arc::clone(&hol_profile))
                    .expect_err("Cursor writer must reject preconfigured HOL diagnostics");
                assert!(error.to_string().contains("V8 HOL diagnostics"));

                let timeline_profile = Arc::new(crate::mem_table::WriteProfile::default());
                timeline_profile.set_wal_v8_completion_timeline_sample_every(1);
                let error = writer
                    .set_write_profile(Arc::clone(&timeline_profile))
                    .expect_err("Cursor writer must reject a preconfigured completion timeline");
                assert!(
                    error
                        .to_string()
                        .contains("completion timeline diagnostics")
                );
            }

            let durability_profile = Arc::new(crate::mem_table::WriteProfile::default());
            writer.set_write_profile(Arc::clone(&durability_profile))?;
            let unsampled = batch(1);
            let prepared = writer.preflight_batch(&unsampled, LIVE_WAL_V5_LIMITS)?;
            writer.append_prepared_batch(prepared, unsampled.commit_ts, unsampled.recorded_at)?;
            writer.sync()?;
            assert!(
                durability_profile
                    .wal_v8_durability_phase_observations()
                    .is_empty()
            );

            durability_profile.set_wal_v8_durability_sample_every(1);
            for writer_index in 0..4 {
                let mut sampled = batch(writer_index + 2);
                sampled.entries[0] = WalEntry::Put {
                    key: format!("sampled-{writer_index}").into_bytes(),
                    value: b"value".to_vec(),
                };
                let prepared = writer.preflight_batch(&sampled, LIVE_WAL_V5_LIMITS)?;
                writer.append_prepared_batch(prepared, sampled.commit_ts, sampled.recorded_at)?;
            }
            writer.sync()?;

            let observations = durability_profile.wal_v8_durability_phase_observations();
            assert!(
                !observations.is_empty(),
                "Cursor {mode_name} must emit durability phase samples"
            );
            assert!(observations.iter().all(|observation| {
                observation.coalescing_iterations.is_some()
                    && observation.coalescing_progress_wait_wall_ns.is_some()
                    && observation.coalescing_progress_wait_cpu_ns.is_some()
                    && observation.coalescing_completion_drain_wall_ns.is_some()
                    && observation.coalescing_completion_apply_wall_ns.is_some()
                    && observation.coalescing_digest_fold_cpu_ns.is_some()
                    && observation.coalescing_cursor_advance_cpu_ns.is_some()
                    && observation.coalescing_ready_tickets_gained.is_some()
            }));
        }

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn leader_sync_diagnostics_capture_frontier_io_and_completion_wait() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("diagnostics-leader-v8.wal");
        let mut writer = open_writer(&path, &install_empty(&path, header())?)?;
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        writer.set_write_profile(Arc::clone(&profile));
        writer.set_wal_sync_diagnostics_enabled(&profile, true)?;

        writer.append_batch(&batch(10), LIVE_WAL_V5_LIMITS)?;

        let observations = profile.wal_sync_observations();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].tickets_covered, Some(1));
        assert!(observations[0].frontier_write_ns > 0);
        assert!(observations[0].duration_ns > 0);
        assert!(
            observations[0]
                .data_completion_to_frontier_start_ns
                .is_some()
        );
        writer.set_wal_sync_diagnostics_enabled(&profile, false)?;
        let error = writer
            .set_wal_sync_diagnostics_enabled(&profile, true)
            .expect_err("diagnostics re-enable after a new ticket must be rejected");
        assert!(error.to_string().contains("before the first WAL ticket"));
        assert!(!profile.wal_sync_diagnostics_enabled());

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

    #[cfg(feature = "bench")]
    fn open_accounted_parallel_writer(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
        max_source_spool_bytes: u64,
        allocation_unit: u64,
    ) -> Result<(WalV8ParallelWriter, Arc<WalV8ReservationOwner>)> {
        let sync_file = OpenOptions::new().read(true).write(true).open(path)?;
        let (leader, owner) = open_accounted_writer(
            path,
            installed,
            max_file_size,
            max_source_spool_bytes,
            allocation_unit,
            sync_file,
        )?;
        let worker_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        let mut writer = leader.into_parallel_with_worker_file_and_progress_mode(
            worker_file,
            WalV8ProgressMode::Eager,
        )?;
        writer.point_buffer_preallocation_enabled = true;

        Ok((writer, owner))
    }

    #[cfg(feature = "bench")]
    #[test]
    fn point_buffer_preallocation_does_not_reserve_quota_before_admission() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const INITIAL_SOURCE_CHARGE: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;
        const SOURCE_SPOOL_LIMIT: u64 = SEGMENT_LIMIT + INITIAL_SOURCE_CHARGE;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("point-prealloc-quota-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, owner) = match open_accounted_parallel_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            SOURCE_SPOOL_LIMIT,
            4096,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let point = batch(10);
        let prepared = writer.preflight_batch_with_write_class(
            &point,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut scratch = WalV8PointAdmissionScratch::new(prepared);

        let quota_before = owner.image_owner().snapshot();
        assert!(writer.try_preallocate_point_buffer(&mut scratch));
        assert_eq!(owner.image_owner().snapshot(), quota_before);
        assert_eq!(
            writer
                .point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
            1
        );

        let error = writer
            .admit_prepared_point_batch(scratch, point.commit_ts, point.recorded_at)
            .err()
            .expect("the exhausted source-spool quota must reject serialized admission");
        assert!(format!("{error:#}").contains("source spool limit"));
        assert_eq!(writer.coordinator.next_ticket(), 0);
        assert_eq!(owner.image_owner().snapshot(), quota_before);
        assert_eq!(
            writer
                .point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
            0,
            "failed admission drops its speculative buffer and permit"
        );
        let diagnostics = writer.io_diagnostics();
        assert_eq!(diagnostics.point_buffer_preallocation_attempts, 1);
        assert_eq!(diagnostics.point_buffer_preallocation_hits, 1);
        assert_eq!(diagnostics.point_buffer_preallocation_fallbacks, 0);

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn point_buffer_preallocation_does_not_consume_quota_before_admission() -> Result<()> {
        point_buffer_preallocation_preserves_ordered_quota_admission()
    }

    #[cfg(feature = "bench")]
    fn point_buffer_preallocation_preserves_ordered_quota_admission() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const INITIAL_SOURCE_CHARGE: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;
        const ONE_BATCH_SOURCE_CHARGE: u64 = 2 * WAL_V8_FRAME_LEN as u64;
        const SOURCE_SPOOL_LIMIT: u64 =
            SEGMENT_LIMIT + INITIAL_SOURCE_CHARGE + ONE_BATCH_SOURCE_CHARGE;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("point-prealloc-order-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, owner) = match open_accounted_parallel_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            SOURCE_SPOOL_LIMIT,
            4096,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let earlier = batch(10);
        let later = batch(20);
        let earlier_scratch =
            WalV8PointAdmissionScratch::new(writer.preflight_batch_with_write_class(
                &earlier,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?);
        let mut later_scratch =
            WalV8PointAdmissionScratch::new(writer.preflight_batch_with_write_class(
                &later,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?);

        let quota_before_admission = owner.image_owner().snapshot();
        assert!(writer.try_preallocate_point_buffer(&mut later_scratch));
        assert_eq!(owner.image_owner().snapshot(), quota_before_admission);

        let earlier_job = writer.admit_prepared_point_batch(
            earlier_scratch,
            earlier.commit_ts,
            earlier.recorded_at,
        )?;
        let earlier_ticket = earlier_job.assignment().segment_ticket;
        earlier_job.finish()?;
        writer.wait_durable(earlier_ticket)?;

        let quota_after_earlier_admission = owner.image_owner().snapshot();
        assert_eq!(
            quota_after_earlier_admission.source_spool_bytes,
            quota_before_admission.source_spool_bytes + ONE_BATCH_SOURCE_CHARGE
        );
        let error = writer
            .admit_prepared_point_batch(later_scratch, later.commit_ts, later.recorded_at)
            .err()
            .expect("the later admission exceeds the remaining source-spool quota");
        assert!(format!("{error:#}").contains("source spool limit"));
        assert_eq!(writer.coordinator.next_ticket(), 1);
        assert_eq!(
            owner.image_owner().snapshot(),
            quota_after_earlier_admission
        );
        assert_eq!(
            writer
                .point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
            0,
            "failed later admission releases its speculative buffer permit"
        );

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn point_buffer_preallocation_exhaustion_falls_back_to_normal_admission() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 256 * 1024;
        const INITIAL_SOURCE_CHARGE: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;
        const BATCH_COUNT: usize = MAX_POINT_BUFFER_PREALLOCATIONS + 1;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("point-prealloc-bound-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, _owner) = match open_accounted_parallel_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT,
            4096,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let batches = (1..=BATCH_COUNT)
            .map(|ticket| batch(ticket as u64))
            .collect::<Vec<_>>();
        let mut scratches = Vec::with_capacity(BATCH_COUNT);

        // Keep the first 16 buffers live so the next Point falls back to the
        // ordinary in-sequencer allocation path.
        for (index, point) in batches.iter().enumerate() {
            let prepared = writer.preflight_batch_with_write_class(
                point,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?;
            let mut scratch = WalV8PointAdmissionScratch::new(prepared);
            let preallocated = writer.try_preallocate_point_buffer(&mut scratch);
            assert_eq!(
                preallocated,
                index < MAX_POINT_BUFFER_PREALLOCATIONS,
                "the bounded permit pool should fall back after its configured limit"
            );
            scratches.push(scratch);
        }

        assert_eq!(
            writer
                .point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
            MAX_POINT_BUFFER_PREALLOCATIONS
        );
        let fallback = scratches.pop().expect("the last scratch used fallback");
        let fallback_point = batches.last().expect("one fallback batch exists");
        let job = writer.admit_prepared_point_batch(
            fallback,
            fallback_point.commit_ts,
            fallback_point.recorded_at,
        )?;
        let ticket = job.assignment().segment_ticket;
        job.finish()?;
        writer.wait_durable(ticket)?;
        assert_eq!(
            writer
                .point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
            MAX_POINT_BUFFER_PREALLOCATIONS,
            "the ordinary allocation fallback must not consume another permit"
        );

        drop(scratches);
        assert_eq!(
            writer
                .point_buffer_preallocations_in_flight
                .load(Ordering::Relaxed),
            0,
            "abandoned pre-admission buffers release all permits"
        );
        let diagnostics = writer.io_diagnostics();
        assert_eq!(
            diagnostics.point_buffer_preallocation_attempts,
            BATCH_COUNT as u64
        );
        assert_eq!(
            diagnostics.point_buffer_preallocation_hits,
            MAX_POINT_BUFFER_PREALLOCATIONS as u64
        );
        assert_eq!(diagnostics.point_buffer_preallocation_fallbacks, 1);

        Ok(())
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

    fn open_extent_accounted_parallel_writer(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
        max_source_spool_bytes: u64,
        window_bytes: u64,
    ) -> Result<(
        WalV8ParallelWriter,
        Arc<WalV8ReservationOwner>,
        Arc<WalV8ExtentPreallocator>,
    )> {
        open_extent_accounted_parallel_writer_with_lookahead(
            path,
            installed,
            max_file_size,
            max_source_spool_bytes,
            window_bytes,
            false,
        )
    }

    fn open_extent_accounted_parallel_writer_with_lookahead(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        max_file_size: u64,
        max_source_spool_bytes: u64,
        window_bytes: u64,
        lookahead: bool,
    ) -> Result<(
        WalV8ParallelWriter,
        Arc<WalV8ReservationOwner>,
        Arc<WalV8ExtentPreallocator>,
    )> {
        let sync_file = OpenOptions::new().read(true).write(true).open(path)?;
        let (leader, owner) = open_accounted_writer(
            path,
            installed,
            max_file_size,
            max_source_spool_bytes,
            FRAME_LEN_U64,
            sync_file.try_clone()?,
        )?;
        let worker_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        let preallocator = Arc::new(WalV8ExtentPreallocator::new_with_lookahead(
            worker_file.try_clone()?,
            &sync_file,
            installed.image_len,
            max_file_size,
            window_bytes,
            Some(Arc::clone(&owner)),
            lookahead,
        )?);
        let mut writer = leader.into_parallel_with_worker_file(worker_file)?;
        Arc::get_mut(&mut writer.io)
            .context("new V8 writer unexpectedly shares its I/O state")?
            .extent_preallocator = Some(Arc::clone(&preallocator));

        Ok((writer, owner, preallocator))
    }

    struct AccountedExtentPreallocatorFixture {
        owner: Arc<WalV8ReservationOwner>,
        preallocator: WalV8ExtentPreallocator,
        start_offset: u64,
        required_end: u64,
        window_end: u64,
    }

    fn accounted_extent_preallocator_fixture(
        path: &Path,
        max_file_size: u64,
        window_bytes: u64,
        lookahead: bool,
    ) -> Result<AccountedExtentPreallocatorFixture> {
        let installed = install_empty(path, header())?;
        let start_offset = installed.image_len;
        let required_end = start_offset + FRAME_LEN_U64;
        let window_end = start_offset + window_bytes;
        let slack_bytes = window_end - required_end;
        let max_source_spool_bytes = max_file_size + FIXED_IMAGE_TEST_BYTES + slack_bytes;
        let sync_file = OpenOptions::new().read(true).write(true).open(path)?;
        let (writer, owner) = open_accounted_writer(
            path,
            &installed,
            max_file_size,
            max_source_spool_bytes,
            FRAME_LEN_U64,
            sync_file,
        )?;
        drop(writer);

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        let sync_file = OpenOptions::new().read(true).write(true).open(path)?;
        let preallocator = WalV8ExtentPreallocator::new_with_lookahead(
            data_file,
            &sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            Some(Arc::clone(&owner)),
            lookahead,
        )?;

        Ok(AccountedExtentPreallocatorFixture {
            owner,
            preallocator,
            start_offset,
            required_end,
            window_end,
        })
    }

    #[cfg(feature = "bench")]
    fn point_only_extent_preallocator_fixture(
        path: &Path,
        max_file_size: u64,
        window_bytes: u64,
    ) -> Result<AccountedExtentPreallocatorFixture> {
        let fixture =
            accounted_extent_preallocator_fixture(path, max_file_size, window_bytes, false)?;
        let AccountedExtentPreallocatorFixture {
            owner,
            preallocator,
            start_offset,
            required_end,
            window_end,
        } = fixture;
        drop(preallocator);

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        let sync_file = OpenOptions::new().read(true).write(true).open(path)?;
        let preallocator = WalV8ExtentPreallocator::new_with_point_only_lookahead(
            data_file,
            &sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            Arc::clone(&owner),
        )?;

        Ok(AccountedExtentPreallocatorFixture {
            owner,
            preallocator,
            start_offset,
            required_end,
            window_end,
        })
    }

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
    fn direct_point_frame_syncs_and_recovers_through_parallel_runtime() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-direct-point-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = open_writer(&path, &installed)?.into_parallel()?;
        let input = batch(10);
        let prepared = writer.preflight_batch_with_write_class(
            &input,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let job = writer.admit_prepared_batch(prepared, input.commit_ts, input.recorded_at)?;
        let ticket = job.assignment().segment_ticket;

        job.finish_with_point_direct_frame(true)?;
        writer.wait_durable(ticket)?;
        let durable_frontier = writer.active_boundaries()?.1;
        assert_eq!(durable_frontier.ticket_end, 1);
        assert!(!writer.is_poisoned());

        drop(writer);
        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[installed.active_boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 1);
        assert_eq!(replayed_batches(&path, &recovered)?, [input]);

        Ok(())
    }

    #[test]
    fn direct_point_frame_digest_is_ready_before_head_data_submission() -> Result<()> {
        struct ReleaseFirstProducer(Option<mpsc::Sender<()>>);

        impl ReleaseFirstProducer {
            fn send(&mut self) {
                if let Some(release_tx) = self.0.take() {
                    let _ = release_tx.send(());
                }
            }
        }

        impl Drop for ReleaseFirstProducer {
            fn drop(&mut self) {
                self.send();
            }
        }

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-direct-point-ooo-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = Arc::new(open_writer(&path, &installed)?.into_parallel()?);
        let first_batch = batch(10);
        let first_prepared = writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?;
        let second_batch = batch(20);
        let second_prepared = writer.preflight_batch_with_write_class(
            &second_batch,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        thread::scope(|scope| -> Result<()> {
            let mut release_first_producer = ReleaseFirstProducer(Some(release_tx));
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
                        release_rx
                            .recv_timeout(Duration::from_secs(5))
                            .expect("test releases the first producer before timeout");
                    },
                )
            });
            admitted_rx.recv_timeout(Duration::from_secs(2))?;

            let second_job =
                writer.admit_prepared_batch(second_prepared, 20, second_batch.recorded_at)?;
            let second_ticket = second_job.assignment().segment_ticket;
            assert_eq!(second_ticket, 1);
            let second_writer = Arc::clone(&writer);
            let second_handle = scope.spawn(move || -> Result<()> {
                second_job.finish_with_point_direct_frame(true)?;
                second_writer.wait_durable(second_ticket)
            });

            // Ticket 1 can publish its digest and frame while ticket 0's
            // producer is paused, but the submission worker cannot complete
            // ticket 1's DATA write until the missing head frame is submitted.
            // Keep this wait result separate so the head producer is always
            // released before propagating a timeout or assertion failure.
            let readiness_result = (|| -> Result<()> {
                let deadline = Instant::now() + Duration::from_secs(2);
                loop {
                    let admitted = writer.coordinator.admitted_window();
                    if admitted.len() == 2
                        && admitted[1].frames_ready
                        && admitted[1].digest_ready.is_some()
                    {
                        ensure!(
                            !admitted[0].frames_ready && !admitted[0].data_complete,
                            "head DATA producer unexpectedly advanced while paused"
                        );
                        ensure!(
                            !admitted[1].data_complete,
                            "later DATA completed before the missing head was submitted"
                        );
                        ensure!(
                            writer.coordinator.durable_ticket_end() == 0,
                            "durable frontier advanced across an incomplete head ticket"
                        );
                        return Ok(());
                    }
                    ensure!(
                        Instant::now() < deadline,
                        "later direct-frame ticket did not publish its digest and frame"
                    );
                    thread::yield_now();
                }
            })();

            release_first_producer.send();
            let first_result = first_handle
                .join()
                .map_err(|_| anyhow::anyhow!("first v8 producer panicked"))?;
            let second_result = second_handle
                .join()
                .map_err(|_| anyhow::anyhow!("direct-frame producer panicked"))?;
            readiness_result?;
            first_result?;
            second_result?;

            Ok(())
        })?;

        assert_eq!(writer.active_boundaries()?.1.ticket_end, 2);
        assert!(!writer.is_poisoned());
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
            .into_parallel()?;
        let first_batch = batch(10);
        let prepared = writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?;
        let mut data_buffer = DirectBuf::new(prepared.frame_buffer_bytes());
        let mut reservation = None;
        let publisher = WalV8LeaderAdmissionPublisher {
            coordinator: &writer.coordinator,
            reservation: RefCell::new(&mut reservation),
            extent_preallocator: writer.io.extent_preallocator.as_deref(),
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
            &writer.frontier_file,
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

    fn wait_for_data_group_cutoff(writer: &WalV8ParallelWriter, cutoff: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(2);
        while writer.io.submission.lock().group_wait_cutoff != Some(cutoff) {
            ensure!(
                Instant::now() < deadline,
                "v8 DATA group did not observe cutoff {cutoff}"
            );
            thread::yield_now();
        }

        Ok(())
    }

    #[test]
    fn data_groups_observe_admission_before_frames_and_grow_while_waiting() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("admitted-groups.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = open_writer(&path, &installed)?.into_parallel()?;
        writer
            .io
            .group_join_micros
            .store(2_000_000, std::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "bench")]
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        #[cfg(feature = "bench")]
        writer.set_write_profile(Arc::clone(&profile))?;
        let first = multi_frame_batch(10);
        let second = batch(20);
        let third = batch(30);
        let first_job = writer.admit_prepared_batch(
            writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
            10,
            first.recorded_at,
        )?;
        let second_job = writer.admit_prepared_batch(
            writer.preflight_batch(&second, LIVE_WAL_V5_LIMITS)?,
            20,
            second.recorded_at,
        )?;
        thread::scope(|scope| -> Result<()> {
            let first_handle = scope.spawn(move || first_job.finish());
            wait_for_data_group_cutoff(&writer, 2)?;
            assert_eq!(writer.io.submission.lock().next_ticket, 0);
            assert!(!writer.coordinator.admitted_window()[1].frames_ready);
            #[cfg(feature = "bench")]
            assert_eq!(profile.snapshot().wal_worker_eventfd_notifications, 0);

            let third_job = writer.admit_prepared_batch(
                writer.preflight_batch(&third, LIVE_WAL_V5_LIMITS)?,
                30,
                third.recorded_at,
            )?;
            second_job.finish()?;
            wait_for_data_group_cutoff(&writer, 3)?;
            assert_eq!(writer.io.submission.lock().next_ticket, 0);
            third_job.finish()?;
            first_handle.join().expect("first DATA producer panicked")?;

            Ok(())
        })?;
        writer.wait_durable(2)?;
        assert_eq!(writer.io.submission.lock().next_ticket, 3);
        #[cfg(feature = "bench")]
        {
            assert_eq!(profile.snapshot().wal_worker_eventfd_notifications, 1);
            assert_eq!(profile.snapshot().wal_sync_count, 1);
        }
        drop(writer);
        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[installed.active_boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, [first, second, third]);

        Ok(())
    }

    #[test]
    fn single_frame_head_skips_the_optional_join_for_unfinished_successors() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("small-data-group.wal");
        let installed = install_empty(&path, header())?;
        let writer = open_writer(&path, &installed)?.into_parallel()?;
        writer
            .io
            .group_join_micros
            .store(2_000_000, std::sync::atomic::Ordering::Relaxed);
        let first = batch(10);
        let second = batch(20);
        let first_job = writer.admit_prepared_batch(
            writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
            10,
            first.recorded_at,
        )?;
        let second_job = writer.admit_prepared_batch(
            writer.preflight_batch(&second, LIVE_WAL_V5_LIMITS)?,
            20,
            second.recorded_at,
        )?;
        thread::scope(|scope| -> Result<()> {
            let (tx, rx) = mpsc::channel();
            let first_handle = scope.spawn(move || {
                let _ = tx.send(first_job.finish());
            });
            rx.recv_timeout(Duration::from_secs(1))??;
            first_handle
                .join()
                .expect("single-frame DATA producer panicked");

            Ok(())
        })?;
        assert_eq!(writer.io.submission.lock().next_ticket, 1);
        assert!(!writer.io.submission.lock().packing);
        writer.wait_durable(0)?;
        second_job.finish()?;
        writer.wait_durable(1)?;

        Ok(())
    }

    #[test]
    fn unfinished_data_successor_does_not_block_a_ready_head_indefinitely() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("slow-data-successor.wal");
        let installed = install_empty(&path, header())?;
        let writer = open_writer(&path, &installed)?.into_parallel()?;
        let first = multi_frame_batch(10);
        let second = batch(20);
        let first_job = writer.admit_prepared_batch(
            writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
            10,
            first.recorded_at,
        )?;
        let second_job = writer.admit_prepared_batch(
            writer.preflight_batch(&second, LIVE_WAL_V5_LIMITS)?,
            20,
            second.recorded_at,
        )?;
        first_job.finish()?;
        writer.wait_durable(0)?;
        assert_eq!(writer.coordinator.durable_ticket_end(), 1);
        assert_eq!(writer.io.submission.lock().next_ticket, 1);
        assert!(!writer.io.submission.lock().packing);
        second_job.finish()?;
        writer.wait_durable(1)?;
        assert_eq!(writer.coordinator.durable_ticket_end(), 2);

        Ok(())
    }

    #[test]
    fn data_group_wait_is_bounded_when_closed_or_poisoned() -> Result<()> {
        for poison in [false, true] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("stopped-data-group.wal");
            let installed = install_empty(&path, header())?;
            let writer = open_writer(&path, &installed)?.into_parallel()?;
            writer
                .io
                .group_join_micros
                .store(100_000, std::sync::atomic::Ordering::Relaxed);
            let first = multi_frame_batch(10);
            let second = batch(20);
            let first_job = writer.admit_prepared_batch(
                writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
                10,
                first.recorded_at,
            )?;
            let _unfinished = writer.admit_prepared_batch(
                writer.preflight_batch(&second, LIVE_WAL_V5_LIMITS)?,
                20,
                second.recorded_at,
            )?;
            thread::scope(|scope| -> Result<()> {
                let (tx, rx) = mpsc::channel();
                let first_handle = scope.spawn(move || {
                    let _ = tx.send(first_job.finish());
                });
                wait_for_data_group_cutoff(&writer, 2)?;
                if poison {
                    writer.coordinator.poison();
                } else {
                    writer.coordinator.close();
                }
                let result = rx.recv_timeout(Duration::from_secs(1))?;
                assert_eq!(result.is_err(), poison);
                first_handle.join().expect("stopped DATA packer panicked");

                Ok(())
            })?;
            assert!(!writer.io.submission.lock().packing);
            assert_eq!(writer.coordinator.durable_ticket_end(), 0);
            assert_eq!(writer.io.submission.lock().next_ticket, u64::from(!poison));
        }

        Ok(())
    }

    #[test]
    fn concurrent_producers_share_one_frontier_after_admitted_head_finishes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = Arc::new(open_writer(&path, &installed)?.into_parallel()?);
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
                let second_data_queued = writer.io.submission.lock().pending.contains_key(&1);
                if admitted.len() == 2
                    && admitted[1].frames_ready
                    && admitted[1].digest_ready.is_some()
                    && second_data_queued
                {
                    assert!(!admitted[1].data_complete);
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "later v8 producer did not finish preparation while the head was blocked"
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

    #[test]
    fn published_cursor_is_readable_while_the_physical_frontier_mutex_is_held() -> Result<()> {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("cursor-v8.wal");
        let installed = install_empty(&path, header())?;
        let writer = Arc::new(open_writer(&path, &installed)?.into_parallel()?);
        assert_eq!(writer.published_cursor(), (0, installed.image_len, 0));
        let first = batch(10);
        writer.append_prepared_batch(
            writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
            10,
            first.recorded_at,
        )?;
        let expected = (1, writer.image_len()?, 1);
        let frontier = writer.frontier.lock();
        let reading = Arc::clone(&writer);
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = thread::spawn(move || {
            let _ = tx.send(reading.published_cursor());
        });
        let snapshot = rx.recv_timeout(Duration::from_secs(1));
        // Release before joining so a regression fails by timeout rather
        // than leaving this test deadlocked on its own assertion path.
        drop(frontier);
        reader.join().expect("published cursor reader panicked");
        assert_eq!(snapshot?, expected);

        Ok(())
    }

    #[test]
    fn parallel_frontier_gaps_keep_unused_extent_space_charged() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 2 * 1024 * 1024;
        const WINDOW_BYTES: u64 = 256 * 1024;
        const DATA_AND_FRONTIER_BYTES_PER_COMMIT: u64 = 2 * FRAME_LEN_U64;
        const COMMITS_TO_CROSS_TWO_WINDOWS: u64 =
            (2 * WINDOW_BYTES) / DATA_AND_FRONTIER_BYTES_PER_COMMIT + 1;

        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("accounted-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, owner, preallocator) = open_extent_accounted_parallel_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT,
            WINDOW_BYTES,
        )?;

        let mut previous_frontier_end = installed.image_len;
        // Each single-entry commit advances one DATA frame and one FRONTIER frame.
        for commit_ts in 10..(10 + COMMITS_TO_CROSS_TWO_WINDOWS) {
            let batch = batch(commit_ts);
            let (commit, _) = writer.append_prepared_batch(
                writer.preflight_batch(&batch, LIVE_WAL_V5_LIMITS)?,
                commit_ts,
                batch.recorded_at,
            )?;
            assert_eq!(commit.first_data_offset, previous_frontier_end);
            previous_frontier_end = commit.frontier_offset + FRAME_LEN_U64;

            let prepared_end = preallocator.state.lock().prepared_end;
            assert_eq!(std::fs::metadata(&path)?.len(), prepared_end);
            assert_eq!(
                owner.image_owner().snapshot().source_spool_bytes,
                prepared_end,
                "DATA submission must leave the unused extent suffix charged after commit {commit_ts}"
            );
        }
        assert!(previous_frontier_end > installed.image_len + 2 * WINDOW_BYTES);

        Ok(())
    }

    #[test]
    fn parallel_lookahead_replays_across_extent_windows() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 1024 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 8;

        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("lookahead-recovery-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, owner, preallocator) =
            match open_extent_accounted_parallel_writer_with_lookahead(
                &path,
                &installed,
                SEGMENT_LIMIT,
                2 * SEGMENT_LIMIT,
                WINDOW_BYTES,
                true,
            ) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };

        let batches: Vec<_> = (10..80).map(batch).collect();
        for batch in &batches {
            writer.append_prepared_batch(
                writer.preflight_batch(batch, LIVE_WAL_V5_LIMITS)?,
                batch.commit_ts,
                batch.recorded_at,
            )?;

            let state = preallocator.state.lock();
            let allocated_end = state.allocated_end;
            assert!(state.prepared_end >= writer.image_len()?);
            assert!(state.prepared_end <= allocated_end);
            drop(state);
            assert_eq!(std::fs::metadata(&path)?.len(), allocated_end);
            assert_eq!(
                owner.image_owner().snapshot().source_spool_bytes,
                allocated_end,
                "lookahead suffix must remain charged after commit {}",
                batch.commit_ts
            );
        }

        let (_, boundary) = writer.active_boundaries()?;
        assert!(preallocator.state.lock().allocated_end > installed.image_len + 2 * WINDOW_BYTES);
        drop(writer);
        drop(preallocator);
        drop(owner);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            header().predecessor,
            LIVE_WAL_V5_LIMITS,
            SEGMENT_LIMIT,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, batches);
        assert_eq!(std::fs::metadata(&path)?.len(), recovered.image_len);

        Ok(())
    }

    #[test]
    fn parallel_extent_slack_does_not_block_batch_admission() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;

        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("pressured-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, owner, preallocator) = open_extent_accounted_parallel_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT + FRAME_LEN_U64,
            WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES,
        )?;
        let batches = [batch(10), batch(20)];
        for batch in &batches {
            writer.append_prepared_batch(
                writer.preflight_batch(batch, LIVE_WAL_V5_LIMITS)?,
                batch.commit_ts,
                batch.recorded_at,
            )?;
        }

        assert!(preallocator.state.lock().window_preallocation_disabled);
        assert_eq!(std::fs::metadata(&path)?.len(), writer.image_len()?);
        assert_eq!(
            owner.image_owner().snapshot().source_spool_bytes,
            writer.image_len()?
        );
        let (_, boundary) = writer.active_boundaries()?;
        drop(writer);
        drop(preallocator);
        drop(owner);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            header().predecessor,
            LIVE_WAL_V5_LIMITS,
            SEGMENT_LIMIT,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, batches);

        Ok(())
    }

    #[test]
    fn parallel_extent_pressure_preserves_admitted_data() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;

        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("pending-v8.wal");
        let installed = install_empty(&path, header())?;
        let (writer, owner, preallocator) = open_extent_accounted_parallel_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            2 * SEGMENT_LIMIT + 2 * FRAME_LEN_U64,
            WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES,
        )?;
        let batches = [batch(10), batch(20), batch(30)];
        writer.append_prepared_batch(
            writer.preflight_batch(&batches[0], LIVE_WAL_V5_LIMITS)?,
            batches[0].commit_ts,
            batches[0].recorded_at,
        )?;
        let second = writer.admit_prepared_batch(
            writer.preflight_batch(&batches[1], LIVE_WAL_V5_LIMITS)?,
            batches[1].commit_ts,
            batches[1].recorded_at,
        )?;
        let second_data_end = second.assignment().first_data_offset + FRAME_LEN_U64;
        second.finish()?;

        let third = writer.admit_prepared_batch(
            writer.preflight_batch(&batches[2], LIVE_WAL_V5_LIMITS)?,
            batches[2].commit_ts,
            batches[2].recorded_at,
        )?;
        assert!(preallocator.state.lock().window_preallocation_disabled);
        assert_eq!(std::fs::metadata(&path)?.len(), second_data_end);
        let third_ticket = third.assignment().segment_ticket;
        third.finish()?;
        writer.wait_durable(third_ticket)?;
        let (_, boundary) = writer.active_boundaries()?;
        drop(writer);
        drop(preallocator);
        drop(owner);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            header().predecessor,
            LIVE_WAL_V5_LIMITS,
            SEGMENT_LIMIT,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, batches);

        Ok(())
    }

    #[test]
    fn parallel_worker_submits_data_across_an_embedded_frontier_gap() -> Result<()> {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("active-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let worker_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;
        let writer = open_writer(&path, &installed)?.into_parallel_with_worker_file(worker_file)?;

        let first_batch = batch(10);
        let first = writer
            .append_prepared_batch(
                writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?,
                10,
                first_batch.recorded_at,
            )?
            .0;
        let second_batch = batch(20);
        let second = writer
            .append_prepared_batch(
                writer.preflight_batch(&second_batch, LIVE_WAL_V5_LIMITS)?,
                20,
                second_batch.recorded_at,
            )?
            .0;

        assert_eq!(
            second.first_data_offset,
            first.frontier_offset + FRAME_LEN_U64,
            "the second ticket follows the first embedded FRONTIER"
        );
        assert_eq!(second.frontier.ticket_end, 2);
        assert_eq!(second.frontier.generation, 2);
        assert_eq!(writer.image_len()?, second.frontier_offset + FRAME_LEN_U64);
        let (_, boundary) = writer.active_boundaries()?;
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 2);
        assert_eq!(
            replayed_batches(&path, &recovered)?,
            [first_batch, second_batch]
        );

        Ok(())
    }

    fn open_parallel_with_progress_mode(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        mode: WalV8ProgressMode,
    ) -> Result<WalV8ParallelWriter> {
        let worker_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        open_writer(path, installed)?
            .into_parallel_with_worker_file_and_progress_mode(worker_file, mode)
    }

    #[cfg(feature = "bench")]
    fn open_parallel_with_cursor_submission_experiment(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        mode: WalV8ProgressMode,
        singleton_submission_enabled: bool,
        worker_group_limit: Option<usize>,
    ) -> Result<WalV8ParallelWriter> {
        let worker_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        open_writer(path, installed)?.into_parallel_with_worker_file_progress_and_completion(
            worker_file,
            mode,
            false,
            #[cfg(feature = "bench")]
            WalV8DirectCompletionWakeMode::Token,
            singleton_submission_enabled,
            worker_group_limit,
        )
    }

    #[cfg(feature = "bench")]
    fn open_parallel_with_direct_completion(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        mode: WalV8ProgressMode,
        worker_group_limit: Option<usize>,
    ) -> Result<WalV8ParallelWriter> {
        open_parallel_with_direct_completion_wake_mode(
            path,
            installed,
            mode,
            WalV8DirectCompletionWakeMode::Token,
            worker_group_limit,
        )
    }

    #[cfg(feature = "bench")]
    fn open_parallel_with_direct_completion_wake_mode(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        mode: WalV8ProgressMode,
        wake_mode: WalV8DirectCompletionWakeMode,
        worker_group_limit: Option<usize>,
    ) -> Result<WalV8ParallelWriter> {
        let worker_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        open_writer(path, installed)?.into_parallel_with_worker_file_progress_and_completion(
            worker_file,
            mode,
            true,
            wake_mode,
            false,
            worker_group_limit,
        )
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_direct_completion_writes_reopens_and_recovers_without_reaper() -> Result<()> {
        check_cursor_direct_completion_writes_reopens_and_recovers(
            WalV8DirectCompletionWakeMode::Token,
        )
    }

    #[cfg(feature = "bench")]
    #[test]
    fn direct_completion_select_wakes_a_waiting_driver_for_data_and_control() -> Result<()> {
        let (_control_tx, control_rx) = crossbeam_channel::bounded(1);
        let (completion_tx, completion_rx) = crossbeam_channel::bounded(1);
        let (registered_tx, registered_rx) = mpsc::channel();
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (wake_tx, wake_rx) = mpsc::channel();
        let driver = thread::spawn(move || {
            let wake = wait_for_direct_completion_or_control_with_hook(
                &control_rx,
                &completion_rx,
                Duration::from_secs(2),
                || {
                    registered_tx.send(()).expect("test receiver remains connected");
                    let _ = release_rx.recv();
                },
            );
            let _ = wake_tx.send(wake);
        });
        registered_rx.recv_timeout(Duration::from_secs(1))?;
        let completion = DirectGroupWriteCompletion {
            result: GroupWriteResult {
                group_id: 8,
                tickets: 4..5,
                write_count: 1,
                write_bytes: FRAME_LEN_U64,
                error: None,
            },
            completion_observed_at: Some(Instant::now()),
        };
        completion_tx.send(completion.clone())?;
        release_tx.send(())?;
        let wake = wake_rx
            .recv_timeout(Duration::from_secs(1))
            .context("completion waiter did not wake")?
            .map_err(|closed| anyhow::anyhow!("completion wait disconnected: {closed:?}"))?;
        assert!(matches!(wake, WalV8ProgressWake::Completion(event) if event == completion));
        driver.join().expect("completion wait thread panicked");

        let (control_coordinator, control_rx) = cursor_completion_coordinator()?;
        let (_completion_tx, completion_rx) = crossbeam_channel::bounded(1);
        let (registered_tx, registered_rx) = mpsc::channel();
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (wake_tx, wake_rx) = mpsc::channel();
        let driver = thread::spawn(move || {
            let wake = wait_for_direct_completion_or_control_with_hook(
                &control_rx,
                &completion_rx,
                Duration::from_secs(2),
                || {
                    registered_tx.send(()).expect("test receiver remains connected");
                    let _ = release_rx.recv();
                },
            );
            let _ = wake_tx.send(wake);
        });
        registered_rx.recv_timeout(Duration::from_secs(1))?;
        control_coordinator.signal_progress_token();
        release_tx.send(())?;
        let wake = wake_rx
            .recv_timeout(Duration::from_secs(1))
            .context("control waiter did not wake")?
            .map_err(|closed| anyhow::anyhow!("control wait disconnected: {closed:?}"))?;
        assert!(matches!(wake, WalV8ProgressWake::Control));
        driver.join().expect("control wait thread panicked");

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn direct_completion_select_seed_and_batch_drain_preserve_capacity_and_order() -> Result<()> {
        let (completion_tx, completion_rx) = crossbeam_channel::bounded(3);
        let observed_at = Instant::now();
        for ticket in 1..=3 {
            completion_tx.send(DirectGroupWriteCompletion {
                result: GroupWriteResult {
                    group_id: ticket,
                    tickets: ticket..ticket + 1,
                    write_count: 1,
                    write_bytes: FRAME_LEN_U64,
                    error: None,
                },
                completion_observed_at: Some(observed_at),
            })?;
        }

        let first = completion_rx.try_recv()?;
        let mut batch = vec![direct_completion_event(first)?];
        let capacity = 3;
        let remaining_capacity = capacity - batch.len();
        drain_direct_completion_queue(
            &completion_rx,
            &mut batch,
            remaining_capacity,
            false,
        )?;
        assert_eq!(batch.len(), capacity);
        assert!(completion_rx.is_empty());
        assert_eq!(
            batch.iter().map(|event| event.group_id).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            batch.iter().map(|event| event.tickets.clone()).collect::<Vec<_>>(),
            [1..2, 2..3, 3..4]
        );
        assert!(
            batch.iter().all(|event| event.completion_observed_at == Some(observed_at))
        );

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn direct_completion_select_poison_wakes_a_waiting_driver() -> Result<()> {
        let (coordinator, tokens) = cursor_completion_coordinator()?;
        let (_completion_tx, completions) = crossbeam_channel::bounded(1);
        let (registered_tx, registered_rx) = mpsc::channel();
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (wake_tx, wake_rx) = mpsc::channel();
        let driver = thread::spawn(move || {
            let wake = wait_for_direct_completion_or_control_with_hook(
                &tokens,
                &completions,
                Duration::from_secs(2),
                || {
                    registered_tx.send(()).expect("test receiver remains connected");
                    let _ = release_rx.recv();
                },
            );
            let _ = wake_tx.send(wake);
        });
        registered_rx.recv_timeout(Duration::from_secs(1))?;
        coordinator.poison();
        release_tx.send(())?;
        let wake = wake_rx
            .recv_timeout(Duration::from_secs(1))
            .context("poison did not wake select waiter")?
            .map_err(|closed| anyhow::anyhow!("poison wait disconnected: {closed:?}"))?;
        assert!(matches!(wake, WalV8ProgressWake::Control));
        assert!(coordinator.is_poisoned());
        driver.join().expect("poison wait thread panicked");

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn direct_completion_select_close_wakes_a_waiting_driver() -> Result<()> {
        let (coordinator, tokens) = cursor_completion_coordinator()?;
        let (_completion_tx, completions) = crossbeam_channel::bounded(1);
        let (registered_tx, registered_rx) = mpsc::channel();
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (wake_tx, wake_rx) = mpsc::channel();
        let driver = thread::spawn(move || {
            let wake = wait_for_direct_completion_or_control_with_hook(
                &tokens,
                &completions,
                Duration::from_secs(2),
                || {
                    registered_tx.send(()).expect("test receiver remains connected");
                    let _ = release_rx.recv();
                },
            );
            let _ = wake_tx.send(wake);
        });
        registered_rx.recv_timeout(Duration::from_secs(1))?;
        coordinator.close();
        release_tx.send(())?;
        let wake = wake_rx
            .recv_timeout(Duration::from_secs(1))
            .context("close did not wake select waiter")?
            .map_err(|closed| anyhow::anyhow!("close wait disconnected: {closed:?}"))?;
        assert!(matches!(wake, WalV8ProgressWake::Control));
        assert!(coordinator.is_closed());
        driver.join().expect("close wait thread panicked");

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_direct_completion_select_wake_writes_reopens_and_recovers() -> Result<()> {
        check_cursor_direct_completion_writes_reopens_and_recovers(
            WalV8DirectCompletionWakeMode::Select,
        )
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_direct_completion_select_wakes_driver_after_empty_scan() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-select-scan-to-park-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion_wake_mode(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
            WalV8DirectCompletionWakeMode::Select,
            None,
        ) {
            Ok(writer) => Arc::new(writer),
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let input = batch(10);
        let job = writer.admit_prepared_batch(
            writer.preflight_batch(&input, LIVE_WAL_V5_LIMITS)?,
            input.commit_ts,
            input.recorded_at,
        )?;
        let ticket = job.assignment().segment_ticket;
        let (wait_entered_tx, wait_entered_rx) = crossbeam_channel::bounded(1);
        let (release_wait_tx, release_wait_rx) = crossbeam_channel::bounded(1);
        let first_wait = Arc::new(AtomicBool::new(false));
        writer.io.set_progress_wait_hook(Arc::new({
            let first_wait = Arc::clone(&first_wait);
            move |event| {
                if matches!(event, WalV8ProgressWaitTestEvent::BeforeWait)
                    && !first_wait.swap(true, Ordering::AcqRel)
                {
                    let _ = wait_entered_tx.send(());
                    let _ = release_wait_rx.recv_timeout(Duration::from_secs(5));
                }
            }
        }));

        let (driver_result_tx, driver_result_rx) = mpsc::channel();
        let (finish_result_tx, finish_result_rx) = mpsc::channel();
        thread::scope(|scope| -> Result<()> {
            let driver_writer = Arc::clone(&writer);
            let driver = scope.spawn(move || {
                let _ = driver_result_tx.send(driver_writer.advance_frontier_through(ticket));
            });
            wait_entered_rx
                .recv_timeout(Duration::from_secs(5))
                .context("durability driver did not reach its empty-prefix wait")?;

            let finish = scope.spawn(move || {
                let _ = finish_result_tx.send(job.finish().map(|_| ()));
            });
            let completion_deadline = Instant::now() + Duration::from_secs(5);
            while writer
                .io
                .direct_completions
                .as_ref()
                .is_none_or(|completions| completions.is_empty())
                && Instant::now() < completion_deadline
            {
                thread::yield_now();
            }
            ensure!(
                writer
                    .io
                    .direct_completions
                    .as_ref()
                    .is_some_and(|completions| !completions.is_empty()),
                "worker did not enqueue the DATA completion while the driver was paused"
            );
            ensure!(
                writer
                    .io
                    .progress_tokens
                    .as_ref()
                    .is_some_and(Receiver::is_empty),
                "select-mode DATA completion emitted a progress token"
            );

            release_wait_tx.send(())?;
            driver_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("select-mode driver did not receive its queued completion")??;
            finish_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("Point waiter did not receive its durable ACK")??;
            driver
                .join()
                .map_err(|_| anyhow::anyhow!("select-mode durability driver panicked"))?;
            finish
                .join()
                .map_err(|_| anyhow::anyhow!("select-mode Point finisher panicked"))?;
            Ok(())
        })?;

        assert_eq!(writer.active_boundaries()?.1.ticket_end, 1);
        drop(writer);
        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[installed.active_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 1);
        assert_eq!(replayed_batches(&path, &recovered)?, [input]);

        Ok(())
    }

    #[cfg(feature = "bench")]
    fn check_cursor_direct_completion_writes_reopens_and_recovers(
        wake_mode: WalV8DirectCompletionWakeMode,
    ) -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-direct-completion-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion_wake_mode(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
            wake_mode,
            None,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        assert!(writer.io.completion_thread.lock().is_none());
        assert!(writer.io.completion_events.is_none());
        assert!(writer.io.direct_completions.is_some());
        assert!(writer.io_diagnostics().cursor_direct_completion_enabled);
        assert_eq!(
            writer
                .io_diagnostics()
                .cursor_direct_completion_select_wake_enabled,
            wake_mode == WalV8DirectCompletionWakeMode::Select
        );

        let first_batch = batch(10);
        writer.append_prepared_batch(
            writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?,
            first_batch.commit_ts,
            first_batch.recorded_at,
        )?;
        let second_batch = batch(20);
        writer.append_prepared_batch(
            writer.preflight_batch(&second_batch, LIVE_WAL_V5_LIMITS)?,
            second_batch.commit_ts,
            second_batch.recorded_at,
        )?;
        let (_, first_boundary) = writer.active_boundaries()?;
        assert_eq!(first_boundary.ticket_end, 2);
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[first_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(
            replayed_batches(&path, &recovered)?,
            [first_batch.clone(), second_batch.clone()]
        );

        let reopened = open_parallel_with_direct_completion_wake_mode(
            &path,
            &recovered,
            WalV8ProgressMode::Cursor,
            wake_mode,
            None,
        )?;
        let third_batch = batch(30);
        reopened.append_prepared_batch(
            reopened.preflight_batch(&third_batch, LIVE_WAL_V5_LIMITS)?,
            third_batch.commit_ts,
            third_batch.recorded_at,
        )?;
        let (_, final_boundary) = reopened.active_boundaries()?;
        assert_eq!(final_boundary.ticket_end, 3);
        drop(reopened);

        let final_recovered = super::super::install::recover_and_install_active(
            &path,
            &[final_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(
            replayed_batches(&path, &final_recovered)?,
            [first_batch, second_batch, third_batch]
        );

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_singleton_submission_preserves_groups_ranges_and_recovery() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let baseline_path = directory.path().join("cursor-singleton-baseline-v8.wal");
        let candidate_path = directory.path().join("cursor-singleton-candidate-v8.wal");
        let wal_header = header();
        let baseline_installed = install_empty(&baseline_path, wal_header)?;
        let candidate_installed = install_empty(&candidate_path, wal_header)?;

        let mut snapshots = Vec::new();
        let mut ticket_write_vec_constructions = Vec::new();
        let mut boundaries = Vec::new();
        let input_batches = [batch(10), multi_frame_batch(20), batch(30)];

        for (path, installed, singleton_submission_enabled) in [
            (&baseline_path, &baseline_installed, false),
            (&candidate_path, &candidate_installed, true),
        ] {
            let writer = match open_parallel_with_cursor_submission_experiment(
                path,
                installed,
                WalV8ProgressMode::Cursor,
                singleton_submission_enabled,
                None,
            ) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            assert_eq!(
                writer.io_diagnostics().cursor_singleton_submission_enabled,
                singleton_submission_enabled
            );

            for input in &input_batches {
                writer.append_prepared_batch(
                    writer.preflight_batch(input, LIVE_WAL_V5_LIMITS)?,
                    input.commit_ts,
                    input.recorded_at,
                )?;
            }
            let (_, boundary) = writer.active_boundaries()?;
            boundaries.push(boundary);
            let submission = writer.io.submission.lock();
            snapshots.push(submission.submitted_group_snapshots.clone());
            ticket_write_vec_constructions
                .push(submission.temporary_ticket_and_write_vec_constructions);
        }

        assert_eq!(snapshots[0], snapshots[1]);
        assert!(
            snapshots[0]
                .iter()
                .any(|group| !group.frontier_offsets.is_empty())
        );
        assert!(
            snapshots[0]
                .iter()
                .all(|group| group.tickets.end - group.tickets.start == 1)
        );
        assert!(snapshots[0].iter().any(|group| {
            group
                .data_ranges
                .iter()
                .any(|range| range.end - range.start > WAL_V8_FRAME_LEN as u64)
        }));
        assert!(ticket_write_vec_constructions[0] > 0);
        assert_eq!(ticket_write_vec_constructions[1], 0);
        assert_eq!(boundaries[0].ticket_end, boundaries[1].ticket_end);

        let mut recovered_batches = Vec::new();
        for (path, boundary) in [
            (&baseline_path, boundaries[0]),
            (&candidate_path, boundaries[1]),
        ] {
            let recovered = super::super::install::recover_and_install_active(
                path,
                &[boundary],
                wal_header.predecessor,
                LIVE_WAL_V5_LIMITS,
                MAX_WAL_FILE_SIZE,
            )?;
            recovered_batches.push(replayed_batches(path, &recovered)?);
        }
        assert_eq!(recovered_batches[0], input_batches);
        assert_eq!(recovered_batches[0], recovered_batches[1]);

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn cursor_singleton_submission_preserves_multi_ticket_group_fallback() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let baseline_path = directory.path().join("cursor-multi-ticket-baseline-v8.wal");
        let candidate_path = directory
            .path()
            .join("cursor-multi-ticket-candidate-v8.wal");
        let wal_header = header();
        let baseline_installed = install_empty(&baseline_path, wal_header)?;
        let candidate_installed = install_empty(&candidate_path, wal_header)?;
        let input_batches = [multi_frame_batch(10), batch(20), batch(30)];

        let mut snapshots = Vec::new();
        let mut vector_constructions = Vec::new();
        let mut boundaries = Vec::new();
        for (path, installed, singleton_submission_enabled) in [
            (&baseline_path, &baseline_installed, false),
            (&candidate_path, &candidate_installed, true),
        ] {
            let writer = match open_parallel_with_cursor_submission_experiment(
                path,
                installed,
                WalV8ProgressMode::Cursor,
                singleton_submission_enabled,
                None,
            ) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            writer
                .io
                .group_join_micros
                .store(2_000_000, std::sync::atomic::Ordering::Relaxed);

            let first = &input_batches[0];
            let second = &input_batches[1];
            let first_job = writer.admit_prepared_batch(
                writer.preflight_batch(first, LIVE_WAL_V5_LIMITS)?,
                first.commit_ts,
                first.recorded_at,
            )?;
            let second_job = writer.admit_prepared_batch(
                writer.preflight_batch(second, LIVE_WAL_V5_LIMITS)?,
                second.commit_ts,
                second.recorded_at,
            )?;

            thread::scope(|scope| -> Result<()> {
                let first_handle = scope.spawn(move || first_job.finish());
                wait_for_data_group_cutoff(&writer, 2)?;

                let third = &input_batches[2];
                let third_job = writer.admit_prepared_batch(
                    writer.preflight_batch(third, LIVE_WAL_V5_LIMITS)?,
                    third.commit_ts,
                    third.recorded_at,
                )?;
                second_job.finish()?;
                wait_for_data_group_cutoff(&writer, 3)?;
                third_job.finish()?;
                first_handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("first v8 DATA producer panicked"))??;

                Ok(())
            })?;

            writer.wait_durable(2)?;
            let (_, boundary) = writer.active_boundaries()?;
            boundaries.push(boundary);
            let submission = writer.io.submission.lock();
            snapshots.push(submission.submitted_group_snapshots.clone());
            vector_constructions.push(submission.temporary_ticket_and_write_vec_constructions);
        }

        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[0].len(), 1);
        assert_eq!(snapshots[0][0].tickets, 0..3);
        assert_eq!(snapshots[0][0].data_ranges.len(), input_batches.len());
        assert_eq!(vector_constructions[0], vector_constructions[1]);
        assert!(vector_constructions[1] > 0);
        assert_eq!(boundaries[0].ticket_end, boundaries[1].ticket_end);

        let mut recovered_batches = Vec::new();
        for (path, boundary) in [
            (&baseline_path, boundaries[0]),
            (&candidate_path, boundaries[1]),
        ] {
            let recovered = super::super::install::recover_and_install_active(
                path,
                &[boundary],
                wal_header.predecessor,
                LIVE_WAL_V5_LIMITS,
                MAX_WAL_FILE_SIZE,
            )?;
            recovered_batches.push(replayed_batches(path, &recovered)?);
        }
        assert_eq!(recovered_batches[0], input_batches);
        assert_eq!(recovered_batches[0], recovered_batches[1]);

        Ok(())
    }

    #[cfg(all(feature = "bench", feature = "chaos-testing"))]
    #[test]
    fn failpoint_cursor_direct_worker_failure_unblocks_sync_and_slot_waiters() -> Result<()> {
        use std::sync::mpsc;

        use crate::chaos::failpoint::{
            FailScenario, arm_parallel_wal_completion_drain_gate, arm_parallel_wal_sync_gate,
            parallel_wal_injected_negative_cqe_count, reset_parallel_wal_failure_test_counters,
        };

        const GROUP_LIMIT: usize = 8;
        const TIMEOUT: Duration = Duration::from_secs(5);

        let _scenario = FailScenario::setup();
        reset_parallel_wal_failure_test_counters();

        let directory = tempfile::tempdir()?;
        let path = directory
            .path()
            .join("cursor-direct-completion-failure-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
            Some(GROUP_LIMIT),
        ) {
            Ok(writer) => Arc::new(writer),
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let first_batch = batch(10);
        let first_job = writer.admit_prepared_batch(
            writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?,
            first_batch.commit_ts,
            first_batch.recorded_at,
        )?;
        assert_eq!(first_job.assignment().segment_ticket, 0);

        let later_batches: Vec<_> = (11..=19).map(batch).collect();
        let mut later_jobs = later_batches
            .iter()
            .map(|input| {
                writer.admit_prepared_batch(
                    writer.preflight_batch(input, LIVE_WAL_V5_LIMITS)?,
                    input.commit_ts,
                    input.recorded_at,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(later_jobs.len(), GROUP_LIMIT + 1);
        let blocked_producer_job = later_jobs.pop().expect("ticket 9 job exists");

        let sync_gate = arm_parallel_wal_sync_gate();

        thread::scope(|scope| -> Result<()> {
            let (first_tx, first_rx) = mpsc::channel();
            let first_writer = Arc::clone(&writer);
            let first_handle = scope.spawn(move || {
                let finish = first_job
                    .finish()
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}"));
                let durable = first_writer
                    .wait_durable(0)
                    .map_err(|error| format!("{error:#}"));
                let _ = first_tx.send((finish, durable));
            });

            ensure!(
                sync_gate.wait_until_entered(TIMEOUT),
                "first durability driver did not reach fdatasync"
            );

            // Let ticket 0 reach its sync before pausing CQE draining. The
            // worker must be free to publish its completion first; otherwise
            // the durability driver cannot enter the frontier sync at all.
            let completion_gate = arm_parallel_wal_completion_drain_gate();

            for (index, job) in later_jobs.into_iter().enumerate() {
                assert_eq!(job.assignment().segment_ticket, index as u64 + 1);
                job.finish()?;
                if index == 0 {
                    ensure!(
                        completion_gate.wait_until_entered(TIMEOUT),
                        "worker did not pause with a DATA SQE awaiting CQE processing"
                    );
                }
            }

            let active_groups = writer
                .io
                .worker
                .lock()
                .as_ref()
                .context("direct completion worker was already removed")?
                .active_group_count_for_test();
            assert_eq!(
                active_groups, GROUP_LIMIT,
                "the test must fill every Worker GroupSlot before blocking the next producer"
            );

            let (waiter_started_tx, waiter_started_rx) = mpsc::channel();
            let (waiter_result_tx, waiter_result_rx) = mpsc::channel();
            for ticket in 1..=GROUP_LIMIT as u64 {
                let waiter_writer = Arc::clone(&writer);
                let started = waiter_started_tx.clone();
                let results = waiter_result_tx.clone();
                scope.spawn(move || {
                    let _ = started.send(ticket);
                    let result = waiter_writer
                        .wait_durable(ticket)
                        .map_err(|error| format!("{error:#}"));
                    let _ = results.send((ticket, result));
                });
            }
            drop(waiter_started_tx);
            drop(waiter_result_tx);
            for _ in 1..=GROUP_LIMIT {
                waiter_started_rx
                    .recv_timeout(TIMEOUT)
                    .context("durability waiter did not start")?;
            }

            let (producer_started_tx, producer_started_rx) = mpsc::channel();
            let (producer_result_tx, producer_result_rx) = mpsc::channel();
            let producer_handle = scope.spawn(move || {
                let _ = producer_started_tx.send(());
                let result = blocked_producer_job
                    .finish()
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}"));
                let _ = producer_result_tx.send(result);
            });
            producer_started_rx
                .recv_timeout(TIMEOUT)
                .context("blocked producer did not start")?;
            assert!(
                matches!(
                    producer_result_rx.recv_timeout(Duration::from_millis(50)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ),
                "producer should wait while all worker group slots are occupied"
            );

            fail::cfg("parallel_wal.negative_cqe", "return").map_err(anyhow::Error::msg)?;
            completion_gate.release();

            let poison_deadline = Instant::now() + TIMEOUT;
            while !writer.is_poisoned() && Instant::now() < poison_deadline {
                thread::sleep(Duration::from_millis(1));
            }
            ensure!(
                writer.is_poisoned(),
                "direct worker failure did not poison the V8 coordinator"
            );
            assert_eq!(parallel_wal_injected_negative_cqe_count(), 1);

            // Unblock the driver only after the worker has failed, so the
            // test exercises failure while it is parked at the FRONTIER sync
            // call, before that ticket can be acknowledged.
            sync_gate.release();
            let (first_finish, first_durable) = first_rx
                .recv_timeout(TIMEOUT)
                .context("first producer or durability driver did not unblock")?;
            first_handle
                .join()
                .map_err(|_| anyhow::anyhow!("first producer panicked"))?;
            assert!(
                first_finish.is_ok(),
                "first DATA job failed: {first_finish:?}"
            );
            assert!(
                first_durable.is_err(),
                "ticket 0 must not be acknowledged after the coordinator is poisoned"
            );

            let mut waiter_results = Vec::with_capacity(GROUP_LIMIT);
            for _ in 1..=GROUP_LIMIT {
                waiter_results.push(
                    waiter_result_rx
                        .recv_timeout(TIMEOUT)
                        .context("durability waiter remained blocked after worker failure")?,
                );
            }
            for (ticket, result) in waiter_results {
                assert!(
                    result.is_err(),
                    "ticket {ticket} received an invalid durability ACK"
                );
            }

            let blocked_producer = producer_result_rx
                .recv_timeout(TIMEOUT)
                .context("GroupSlots waiter remained blocked after worker failure")?;
            assert!(
                blocked_producer.is_err(),
                "blocked producer was admitted after worker failure"
            );
            producer_handle
                .join()
                .map_err(|_| anyhow::anyhow!("blocked producer panicked"))?;

            assert!(writer.is_poisoned());
            assert_eq!(
                writer.coordinator.durable_ticket_end(),
                0,
                "a ticket was acknowledged after the worker failure poisoned the coordinator"
            );

            let worker = writer
                .io
                .worker
                .lock()
                .take()
                .context("direct completion worker was already removed")?;
            // Joining a failed or stopped worker must complete after it has
            // closed GroupSlots and released permits for queued commands.
            let _ = worker.close();
            assert_eq!(
                writer.coordinator.durable_ticket_end(),
                0,
                "a FRONTIER prefix must not advance after the worker failure poisoned the coordinator"
            );

            Ok(())
        })?;

        let durable_boundary = writer.active_boundaries()?.1;
        assert_eq!(durable_boundary.ticket_end, 0);
        drop(writer);
        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[durable_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        let replayed = replayed_batches(&path, &recovered)?;
        assert!(
            replayed.len() <= 1,
            "recovery accepted DATA beyond the FRONTIER sync that was in progress"
        );
        if let Some(replayed_batch) = replayed.first() {
            // The failure is injected before the fdatasync syscall. Once the
            // gate is released, ticket 0's sync may reach disk even though
            // poison prevents the caller from receiving an ACK.
            assert_eq!(replayed_batch, &first_batch);
        }

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn progress_ring_sync_diagnostics_survive_warmup_measurement_reset() -> Result<()> {
        let mode = WalV8ProgressMode::Cursor;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("progress-warmup-v8.wal");
        let installed = install_empty(&path, header())?;
        let writer = match open_parallel_with_progress_mode(&path, &installed, mode) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let profile = Arc::new(crate::mem_table::WriteProfile::default());
        writer.coordinator.set_progress_counters_enabled(true)?;
        writer.set_write_profile(Arc::clone(&profile))?;
        writer.set_wal_sync_diagnostics_enabled(&profile, true)?;

        let append = |commit_ts| -> Result<()> {
            let input = batch(commit_ts);
            let prepared = writer.preflight_batch(&input, LIVE_WAL_V5_LIMITS)?;
            writer.append_prepared_batch(prepared, input.commit_ts, input.recorded_at)?;
            Ok(())
        };

        append(10)?;
        assert_eq!(profile.wal_sync_observations().len(), 1);
        profile.reset_measurements();
        assert!(profile.wal_sync_diagnostics_enabled());
        assert!(profile.wal_sync_observations().is_empty());
        let start = writer
            .io_diagnostics()
            .progress_diagnostics
            .context("warm writer lost progress diagnostics")?;

        append(20)?;
        // The wakeup can let the driver finish before the sender records its
        // diagnostic counter, so wait for that trailing update before checking.
        let token_deadline = Instant::now() + Duration::from_secs(5);
        let mut end = writer
            .io_diagnostics()
            .progress_diagnostics
            .context("timed writer lost progress diagnostics")?;
        while end.token_attempts <= start.token_attempts && Instant::now() < token_deadline {
            std::thread::yield_now();
            end = writer
                .io_diagnostics()
                .progress_diagnostics
                .context("timed writer lost progress diagnostics")?;
        }
        assert!(
            end.token_attempts > start.token_attempts,
            "{mode:?}: progress token attempts did not advance ({} -> {})",
            start.token_attempts,
            end.token_attempts
        );
        assert!(
            end.owner_scans > start.owner_scans,
            "{mode:?}: owner scans did not advance ({} -> {})",
            start.owner_scans,
            end.owner_scans
        );
        assert_eq!(
            end.completion_events_drained - start.completion_events_drained,
            1,
            "{mode:?}"
        );
        let observed = profile.wal_sync_observations();
        assert_eq!(observed.len(), 1, "{mode:?}");
        assert_eq!(observed[0].written_frontier_start, 1, "{mode:?}");
        assert_eq!(observed[0].written_frontier_end, 2, "{mode:?}");

        Ok(())
    }

    #[test]
    fn progress_cursor_driver_scan_to_park_observes_its_only_completion_wake() -> Result<()> {
        check_progress_scan_to_park(WalV8ProgressMode::Cursor)
    }

    fn check_progress_scan_to_park(mode: WalV8ProgressMode) -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("progress-ring-scan-to-park-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = match open_parallel_with_progress_mode(&path, &installed, mode) {
            Ok(writer) => Arc::new(writer),
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let (first_wait_entered_tx, first_wait_entered_rx) = crossbeam_channel::bounded(1);
        let (release_first_wait_tx, release_first_wait_rx) = crossbeam_channel::bounded(1);
        let (wait_result_tx, wait_result_rx) = crossbeam_channel::bounded(4);
        let first_wait = Arc::new(AtomicBool::new(false));
        writer.io.set_progress_wait_hook(Arc::new({
            let first_wait = Arc::clone(&first_wait);
            move |event| match event {
                WalV8ProgressWaitTestEvent::BeforeWait
                    if !first_wait.swap(true, Ordering::AcqRel) =>
                {
                    let _ = first_wait_entered_tx.send(());
                    let _ = release_first_wait_rx.recv_timeout(Duration::from_secs(5));
                }
                WalV8ProgressWaitTestEvent::AfterWait(result) => {
                    let _ = wait_result_tx.send(result);
                }
                WalV8ProgressWaitTestEvent::BeforeWait => {}
            }
        }));

        let (completion_received_tx, completion_received_rx) = crossbeam_channel::bounded(1);
        let (completion_published_tx, completion_published_rx) = crossbeam_channel::bounded(1);
        let (release_completion_before_tx, release_completion_before_rx) =
            crossbeam_channel::bounded(1);
        let (release_completion_after_tx, release_completion_after_rx) =
            crossbeam_channel::bounded(1);
        writer
            .io
            .set_completion_reap_hook(Arc::new(move |event| match event {
                WalV8CompletionReapTestEvent::BeforeReap => {
                    let _ = completion_received_tx.send(());
                    let _ = release_completion_before_rx.recv_timeout(Duration::from_secs(5));
                }
                WalV8CompletionReapTestEvent::AfterReap => {
                    let _ = completion_published_tx.send(());
                    let _ = release_completion_after_rx.recv_timeout(Duration::from_secs(5));
                }
            }));

        struct ProgressRingRaceCleanup {
            coordinator: WalV8ParallelCoordinator,
            releases: Vec<Option<crossbeam_channel::Sender<()>>>,
            armed: bool,
        }
        impl ProgressRingRaceCleanup {
            fn release(&mut self, index: usize) {
                if let Some(sender) = self.releases[index].take() {
                    let _ = sender.send(());
                }
            }
        }
        impl Drop for ProgressRingRaceCleanup {
            fn drop(&mut self) {
                for sender in &mut self.releases {
                    if let Some(sender) = sender.take() {
                        let _ = sender.send(());
                    }
                }
                if self.armed {
                    self.coordinator.poison();
                }
            }
        }

        let input = batch(10);
        let prepared = writer.preflight_batch(&input, LIVE_WAL_V5_LIMITS)?;
        let job = writer.admit_prepared_batch(prepared, input.commit_ts, input.recorded_at)?;
        let ticket = job.assignment().segment_ticket;
        let (driver_result_tx, driver_result_rx) = mpsc::channel();

        thread::scope(|scope| -> Result<()> {
            let mut cleanup = ProgressRingRaceCleanup {
                coordinator: writer.coordinator.clone(),
                releases: vec![
                    Some(release_first_wait_tx),
                    None,
                    Some(release_completion_before_tx),
                    Some(release_completion_after_tx),
                ],
                armed: true,
            };
            let driver_writer = Arc::clone(&writer);
            let driver = scope.spawn(move || {
                let _ = driver_result_tx.send(driver_writer.advance_frontier_through(ticket));
            });

            first_wait_entered_rx
                .recv_timeout(Duration::from_secs(5))
                .context("durability driver did not reach its first post-scan wait")?;

            // Finish DATA after the first scan. Pause the Reaper before
            // enqueueing completion so it cannot race ahead of the driver.
            job.finish()?;
            completion_received_rx
                .recv_timeout(Duration::from_secs(5))
                .context("completion reaper did not receive the DATA result")?;
            // Cursor does not emit the eager digest wake. The driver must
            // remain parked until DATA completion is published.
            assert!(
                writer
                    .io
                    .progress_tokens
                    .as_ref()
                    .context("missing progress token receiver")?
                    .is_empty(),
                "the cursor must rely on the completion wake after a before-DATA digest"
            );
            cleanup.release(2);
            completion_published_rx
                .recv_timeout(Duration::from_secs(5))
                .context("completion reaper did not queue and signal the DATA result")?;
            cleanup.release(0);
            cleanup.release(3);

            let completion_wait_result = wait_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("driver's completion wait did not return")?;
            assert_eq!(
                completion_wait_result,
                WalV8ProgressTokenWait::Signaled,
                "late DATA-completion publication must wake the driver without timeout polling"
            );

            let driver_result = driver_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("durability driver did not observe the late completion")?;
            driver
                .join()
                .map_err(|_| anyhow::anyhow!("durability driver panicked"))?;
            driver_result?;
            cleanup.armed = false;

            assert_eq!(writer.active_boundaries()?.1.ticket_end, ticket + 1);
            Ok(())
        })?;

        drop(writer);
        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[installed.active_boundary],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(recovered.active_boundary.ticket_end, 1);
        assert_eq!(replayed_batches(&path, &recovered)?, [input]);

        Ok(())
    }

    #[test]
    fn progress_ring_clean_close_drains_ready_tickets_without_waiting_for_unready_ones()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("progress-ring-clean-close.wal");
        let installed = install_empty(&path, header())?;
        let writer =
            match open_parallel_with_progress_mode(&path, &installed, WalV8ProgressMode::Cursor) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };

        let (reaped_tx, reaped_rx) = mpsc::sync_channel(1);
        writer.io.set_completion_reap_hook(Arc::new(move |event| {
            if matches!(event, WalV8CompletionReapTestEvent::AfterReap) {
                let _ = reaped_tx.try_send(());
            }
        }));

        // Establish a sync latency so the next partially ready prefix would
        // normally be eligible for coalescing.
        let first = batch(10);
        writer.append_prepared_batch(
            writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
            first.commit_ts,
            first.recorded_at,
        )?;
        reaped_rx
            .recv_timeout(Duration::from_secs(5))
            .context("first DATA group was not reaped")?;

        let ready = batch(20);
        let ready_job = writer.admit_prepared_batch(
            writer.preflight_batch(&ready, LIVE_WAL_V5_LIMITS)?,
            ready.commit_ts,
            ready.recorded_at,
        )?;
        let ready_ticket = ready_job.assignment().segment_ticket;
        let unready = batch(30);
        let unready_job = writer.admit_prepared_batch(
            writer.preflight_batch(&unready, LIVE_WAL_V5_LIMITS)?,
            unready.commit_ts,
            unready.recorded_at,
        )?;
        ready_job.finish()?;
        reaped_rx
            .recv_timeout(Duration::from_secs(5))
            .context("ready DATA group was not reaped")?;

        writer.io.set_progress_wait_hook(Arc::new(|event| {
            assert!(
                !matches!(event, WalV8ProgressWaitTestEvent::BeforeWait),
                "closed progress cursor attempted to park"
            );
        }));
        writer.coordinator.close();
        writer.wait_durable(ready_ticket)?;
        assert_eq!(writer.active_boundaries()?.1.ticket_end, ready_ticket + 1);
        let error = writer
            .wait_durable(unready_job.assignment().segment_ticket)
            .expect_err("unready ticket must fail after close");
        assert!(format!("{error:#}").contains("WAL closed before the ticket became durable"));

        Ok(())
    }

    #[test]
    fn v8_progress_cursor_recovers_mixed_point_and_batch_after_out_of_order_frame_readiness()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-v8.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = Arc::new(
            match open_parallel_with_progress_mode(&path, &installed, WalV8ProgressMode::Cursor) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            },
        );
        let point = batch(10);
        let mut multi_put = batch(20);
        multi_put.entries.push(crate::pitr::WalEntry::Put {
            key: b"multi-frame".to_vec(),
            value: vec![0x37; WAL_V8_FRAME_LEN * 2],
        });
        let point_job = writer.admit_prepared_batch(
            writer.preflight_batch_with_write_class(
                &point,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?,
            point.commit_ts,
            point.recorded_at,
        )?;
        let batch_job = writer.admit_prepared_batch(
            writer.preflight_batch(&multi_put, LIVE_WAL_V5_LIMITS)?,
            multi_put.commit_ts,
            multi_put.recorded_at,
        )?;
        let last_ticket = batch_job.assignment().segment_ticket;
        let (driver_result_tx, driver_result_rx) = mpsc::channel();
        let (driver_waiting_tx, driver_waiting_rx) = crossbeam_channel::bounded(2);
        writer.io.set_progress_wait_hook(Arc::new(move |event| {
            if matches!(event, WalV8ProgressWaitTestEvent::BeforeWait) {
                let _ = driver_waiting_tx.try_send(());
            }
        }));
        thread::scope(|scope| -> Result<()> {
            struct DriverCleanup {
                coordinator: WalV8ParallelCoordinator,
                armed: bool,
            }
            impl Drop for DriverCleanup {
                fn drop(&mut self) {
                    if self.armed {
                        self.coordinator.poison();
                    }
                }
            }

            let mut cleanup = DriverCleanup {
                coordinator: writer.coordinator.clone(),
                armed: true,
            };
            let driver_writer = Arc::clone(&writer);
            let driver = scope.spawn(move || {
                let _ = driver_result_tx.send(driver_writer.advance_frontier_through(last_ticket));
            });

            driver_waiting_rx
                .recv_timeout(Duration::from_secs(5))
                .context("durability driver did not wait for the first Point ticket")?;
            batch_job.finish()?;
            assert_eq!(writer.coordinator.durable_ticket_end(), 0);
            assert!(
                matches!(
                    driver_result_rx.recv_timeout(Duration::from_millis(20)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ),
                "durability driver advanced through the later Batch while the earlier Point was not frame-ready"
            );

            point_job.finish()?;
            driver_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("durability driver did not advance after the Point became ready")??;
            driver
                .join()
                .map_err(|_| anyhow::anyhow!("durability driver panicked"))?;
            cleanup.armed = false;
            Ok(())
        })?;
        let (_, frontier) = writer.active_boundaries()?;
        assert_eq!(frontier.ticket_end, 2);
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[frontier],
            header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, [point, multi_put]);
        let reopened =
            open_parallel_with_progress_mode(&path, &recovered, WalV8ProgressMode::Cursor)?;
        assert!(reopened.coordinator.progress_cursor_enabled());
        let next = batch(30);
        reopened.append_prepared_batch(
            reopened.preflight_batch(&next, LIVE_WAL_V5_LIMITS)?,
            next.commit_ts,
            next.recorded_at,
        )?;
        assert_eq!(reopened.active_boundaries()?.1.ticket_end, 3);

        Ok(())
    }

    #[test]
    fn v8_progress_ring_parallel_write_reopen_and_recovery() -> Result<()> {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join("progress-ring-active-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer =
            match open_parallel_with_progress_mode(&path, &installed, WalV8ProgressMode::Eager) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
        assert!(!writer.progress_ring_enabled());

        let first_batch = batch(10);
        writer.append_prepared_batch(
            writer.preflight_batch(&first_batch, LIVE_WAL_V5_LIMITS)?,
            first_batch.commit_ts,
            first_batch.recorded_at,
        )?;

        let (_, first_boundary) = writer.active_boundaries()?;
        assert_eq!(first_boundary.ticket_end, 1);
        drop(writer);

        let ring_recovered = super::super::install::recover_and_install_active(
            &path,
            &[first_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(ring_recovered.next_ticket, 1);
        assert_eq!(
            replayed_batches(&path, &ring_recovered)?,
            std::slice::from_ref(&first_batch)
        );

        // Reopen the nonempty Active image in ring mode. Its new ring starts
        // at the recovered absolute ticket and digest boundary.
        let ring_writer =
            open_parallel_with_progress_mode(&path, &ring_recovered, WalV8ProgressMode::Cursor)?;
        assert!(ring_writer.progress_ring_enabled());
        assert_eq!(
            ring_writer.coordinator.durable_ticket_end(),
            ring_recovered.active_boundary.ticket_end
        );
        assert_eq!(
            ring_writer.coordinator.durable_prefix_digest(),
            ring_recovered.active_boundary.prefix_digest
        );
        let second_batch = batch(20);
        ring_writer.append_prepared_batch(
            ring_writer.preflight_batch(&second_batch, LIVE_WAL_V5_LIMITS)?,
            second_batch.commit_ts,
            second_batch.recorded_at,
        )?;
        let (_, second_boundary) = ring_writer.active_boundaries()?;
        assert_eq!(second_boundary.ticket_end, 2);
        drop(ring_writer);

        // A subsequent same-segment reopen can select eager publication
        // again while preserving the recovered durable prefix.
        let eager_recovered = super::super::install::recover_and_install_active(
            &path,
            &[second_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(eager_recovered.next_ticket, 2);
        assert_eq!(
            replayed_batches(&path, &eager_recovered)?,
            [first_batch.clone(), second_batch.clone()]
        );
        let eager_writer =
            open_parallel_with_progress_mode(&path, &eager_recovered, WalV8ProgressMode::Eager)?;
        assert!(!eager_writer.progress_ring_enabled());
        let third_batch = batch(30);
        eager_writer.append_prepared_batch(
            eager_writer.preflight_batch(&third_batch, LIVE_WAL_V5_LIMITS)?,
            third_batch.commit_ts,
            third_batch.recorded_at,
        )?;
        let (_, third_boundary) = eager_writer.active_boundaries()?;
        assert_eq!(third_boundary.ticket_end, 3);
        drop(eager_writer);

        let final_recovered = super::super::install::recover_and_install_active(
            &path,
            &[third_boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(
            replayed_batches(&path, &final_recovered)?,
            [first_batch, second_batch, third_batch]
        );

        // A separately constructed fresh segment gets a new pipeline when
        // that mode is selected. This checks writer construction, not rotation.
        let fresh_segment_header = WalV8Header {
            segment_id: crate::pitr::SegmentId(wal_header.segment_id.0 + 1),
            predecessor: crate::pitr::ChainAnchor::Segment(crate::pitr::SegmentAnchor {
                segment_id: wal_header.segment_id,
                wal_digest: [0x77; 32],
                seal_digest: [0x88; 32],
            }),
            incarnation: [0x55; 16],
            ..wal_header
        };
        let fresh_segment_path = directory.path().join("progress-ring-fresh-segment-v8.wal");
        let fresh_segment_installed = install_empty(&fresh_segment_path, fresh_segment_header)?;
        let fresh_segment_writer = open_parallel_with_progress_mode(
            &fresh_segment_path,
            &fresh_segment_installed,
            WalV8ProgressMode::Cursor,
        )?;
        assert!(fresh_segment_writer.progress_ring_enabled());

        Ok(())
    }
}
