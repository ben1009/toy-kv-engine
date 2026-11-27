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
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(any(test, feature = "bench"))]
use std::sync::atomic::AtomicU64;

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
        WalV8AdmissionPublisher, WalV8BatchAssignment, WalV8DigestReady, WalV8DurabilityGuard,
        WalV8WriteClass,
    },
    resource::WalV8ReservationOwner,
};

const FRAME_LEN_U64: u64 = WAL_V8_FRAME_LEN as u64;
const HEADER_LEN_U64: u64 = WAL_V8_HEADER_LEN as u64;
const MAX_GROUP_TICKETS: usize = 8;
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
pub(crate) struct WalV8PointAdmissionScratch<'a> {
    prepared: WalV8PreparedBatch<'a>,
    preallocated_buffer: Option<WalV8PointBufferPreallocation>,
}

/// A bounded speculative DATA buffer allocation. It has no quota reservation;
/// the normal serialized admission path charges quota before consuming it.
pub(crate) struct WalV8PointBufferPreallocation {
    data_buffer: DirectBufAllocation,
    _permit: WalV8PointBufferPreallocationPermit,
}

struct WalV8PointBufferPreallocationPermit {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for WalV8PointBufferPreallocationPermit {
    fn drop(&mut self) {
        let previous = self.in_flight.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "point buffer preallocation permit underflow");
    }
}

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
    ticket_resources: Arc<Mutex<BTreeMap<u64, WalV8TicketResources>>>,
    max_file_size: u64,
    installed_dev: u64,
    installed_ino: u64,
    #[cfg(feature = "bench")]
    initial_ticket: u64,
    #[cfg(feature = "bench")]
    io_diagnostics: WalIoDiagnostics,
    point_buffer_preallocation_enabled: bool,
    effective_config: WalV8EffectiveConfig,
    #[cfg(test)]
    point_buffer_preallocation_test_override: AtomicBool,
    #[cfg(feature = "bench")]
    point_buffer_preallocation_attempts: AtomicU64,
    #[cfg(any(test, feature = "bench"))]
    point_buffer_preallocation_hits: AtomicU64,
    #[cfg(feature = "bench")]
    point_buffer_preallocation_fallbacks: AtomicU64,
    point_buffer_preallocations_in_flight: Arc<AtomicUsize>,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
}

struct WalV8TicketResources {
    reservation: Option<WalV7OwnedBatchReservation>,
    guard: Option<WalV8DurabilityGuard<WalV8ParallelAdmissionGuard>>,
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

#[cfg(test)]
type WalV8LookaheadWorkerHook = Arc<Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>;

struct WalV8ExtentWorkerHandle<T> {
    request_sender: Mutex<Option<mpsc::Sender<T>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    /// Serializes `quiesce` callers so concurrent callers both observe worker exit.
    join_lock: Mutex<()>,
}

impl<T: Send + Sync + 'static> WalV8ExtentWorkerHandle<T> {
    fn new(
        request_sender: Option<mpsc::Sender<T>>,
        worker: Option<thread::JoinHandle<()>>,
    ) -> Self {
        Self {
            request_sender: Mutex::new(request_sender),
            worker: Mutex::new(worker),
            join_lock: Mutex::new(()),
        }
    }

    fn send(&self, msg: T, unavailable: &str, stopped: &str) -> Result<()> {
        let guard = self.request_sender.lock();
        let sender = guard.as_ref().with_context(|| unavailable.to_owned())?;
        sender.send(msg).with_context(|| stopped.to_owned())?;
        Ok(())
    }

    /// Close the request channel and join the background worker.
    /// Serialized by `join_lock` so concurrent callers never return before
    /// the worker thread has exited.
    fn quiesce(&self, panic_msg: &str) {
        let _join_guard = self.join_lock.lock();
        let sender = self.request_sender.lock().take();
        drop(sender);
        let worker = self.worker.lock().take();
        if let Some(worker) = worker
            && worker.join().is_err()
        {
            log::error!("{panic_msg}");
        }
    }
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
    worker_handle: WalV8ExtentWorkerHandle<u64>,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum WalV8FrontierWriteMode {
    #[default]
    Buffered,
    Direct,
}

impl WalV8FrontierWriteMode {
    fn from_environment() -> Result<Self> {
        Self::from_value(std::env::var_os("TOYKV_WAL_V8_FRONTIER_MODE").as_deref())
    }

    fn from_value(value: Option<&OsStr>) -> Result<Self> {
        match value {
            None => Ok(Self::Buffered),
            Some(value) => match value.to_str() {
                Some("buffered") => Ok(Self::Buffered),
                Some("direct") => Ok(Self::Direct),
                Some(value) => anyhow::bail!(
                    "invalid TOYKV_WAL_V8_FRONTIER_MODE {value:?}; expected `buffered` or `direct`"
                ),
                None => anyhow::bail!(
                    "TOYKV_WAL_V8_FRONTIER_MODE is not valid UTF-8; expected `buffered` or `direct`"
                ),
            },
        }
    }
}

#[cfg(feature = "bench")]
const WAL_V8_EXTENT_PREPARATION_ENV: &str = "TOYKV_BENCH_V8_EXTENT_PREP";
#[cfg(feature = "bench")]
const WAL_V8_EXTENT_PREPARATION_BYTES_ENV: &str = "TOYKV_BENCH_V8_EXTENT_PREP_BYTES";
#[cfg(feature = "bench")]
#[cfg(feature = "bench")]
const WAL_V8_POINT_PROGRESS_RING_ENV: &str = "TOYKV_BENCH_V8_POINT_PROGRESS_RING";
#[cfg(feature = "bench")]
const WAL_V8_POINT_PROGRESS_COUNTERS_ENV: &str = "TOYKV_BENCH_V8_PROGRESS_COUNTERS";
#[cfg(feature = "bench")]
const WAL_V8_CURSOR_DIRECT_COMPLETION_ENV: &str = "TOYKV_BENCH_V8_CURSOR_DIRECT_COMPLETION";
#[cfg(feature = "bench")]
const WAL_V8_COALESCE_READY_TARGET_ENV: &str = "TOYKV_BENCH_V8_COALESCE_READY_TARGET";
#[cfg(not(feature = "bench"))]
const WAL_V8_PROGRESS_MODE_ENV: &str = "TOYKV_WAL_V8_PROGRESS_MODE";
const WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES: usize = 128 * 1024;

#[cfg(test)]
#[derive(Clone, Copy)]
struct WalV8TestModeOverride {
    progress_mode: WalV8ProgressMode,
    direct_completion_enabled: bool,
}

#[cfg(test)]
thread_local! {
    static WAL_V8_TEST_MODE_OVERRIDE: std::cell::Cell<Option<WalV8TestModeOverride>> = const {
        std::cell::Cell::new(None)
    };
    static WAL_V8_TEST_PROFILE_OVERRIDE: std::cell::Cell<Option<WalV8Profile>> = const {
        std::cell::Cell::new(None)
    };
}

/// Run an engine-level test with the Candidate V8 profile without mutating
/// process-wide environment variables observed by parallel tests.
#[cfg(test)]
pub(crate) fn with_candidate_profile_for_test<T>(run: impl FnOnce() -> T) -> T {
    with_v8_profile_for_test(WalV8Profile::Candidate, run)
}

/// Run an engine-level test with the Legacy V8 profile without mutating
/// process-wide environment variables observed by parallel tests.
#[cfg(test)]
pub(crate) fn with_legacy_profile_for_test<T>(run: impl FnOnce() -> T) -> T {
    with_v8_profile_for_test(WalV8Profile::Legacy, run)
}

#[cfg(test)]
fn with_v8_profile_for_test<T>(profile: WalV8Profile, run: impl FnOnce() -> T) -> T {
    struct Restore(Option<WalV8Profile>);

    impl Drop for Restore {
        fn drop(&mut self) {
            WAL_V8_TEST_PROFILE_OVERRIDE.with(|current| current.set(self.0));
        }
    }

    let previous = WAL_V8_TEST_PROFILE_OVERRIDE.with(|current| current.replace(Some(profile)));
    let _restore = Restore(previous);
    run()
}

/// Run an engine-level test with Cursor direct completion without mutating
/// process-wide environment variables observed by parallel tests.
#[cfg(test)]
pub(crate) fn with_cursor_direct_completion_for_test<T>(run: impl FnOnce() -> T) -> T {
    struct Restore(Option<WalV8TestModeOverride>);

    impl Drop for Restore {
        fn drop(&mut self) {
            WAL_V8_TEST_MODE_OVERRIDE.with(|current| current.set(self.0));
        }
    }

    let previous = WAL_V8_TEST_MODE_OVERRIDE.with(|current| {
        current.replace(Some(WalV8TestModeOverride {
            progress_mode: WalV8ProgressMode::Cursor,
            direct_completion_enabled: true,
        }))
    });
    let _restore = Restore(previous);
    run()
}

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
    #[cfg(test)]
    if let Some(override_mode) = WAL_V8_TEST_MODE_OVERRIDE.with(std::cell::Cell::get) {
        return Ok(override_mode.progress_mode);
    }
    production_progress_mode_from_value(std::env::var_os(WAL_V8_PROGRESS_MODE_ENV).as_deref())
}

#[cfg(not(feature = "bench"))]
fn production_direct_completion_enabled_from_environment(
    progress_mode: WalV8ProgressMode,
) -> Result<bool> {
    #[cfg(test)]
    if let Some(override_mode) = WAL_V8_TEST_MODE_OVERRIDE.with(std::cell::Cell::get) {
        ensure!(progress_mode == override_mode.progress_mode);
        return Ok(override_mode.direct_completion_enabled);
    }
    let _ = progress_mode;
    Ok(false)
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
    #[cfg(test)]
    if let Some(override_mode) = WAL_V8_TEST_MODE_OVERRIDE.with(std::cell::Cell::get) {
        return Ok(override_mode.progress_mode);
    }
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
    #[cfg(test)]
    if let Some(override_mode) = WAL_V8_TEST_MODE_OVERRIDE.with(std::cell::Cell::get) {
        ensure!(progress_mode == override_mode.progress_mode);
        return Ok(override_mode.direct_completion_enabled);
    }
    cursor_direct_completion_enabled_from_value(
        std::env::var_os(WAL_V8_CURSOR_DIRECT_COMPLETION_ENV).as_deref(),
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
    match value {
        None => Ok(false),
        Some(value) => match value.to_str() {
            Some("0" | "false") => Ok(false),
            Some("1" | "true") => Ok(true),
            Some(value) => anyhow::bail!(
                "invalid {WAL_V8_EXTENT_LOOKAHEAD_ENV} {value:?}; expected `0`, `1`, `false`, or `true`"
            ),
            None => anyhow::bail!(
                "{WAL_V8_EXTENT_LOOKAHEAD_ENV} is not valid UTF-8; expected `0`, `1`, `false`, or `true`"
            ),
        },
    }
}

fn wal_v8_extent_lookahead_from_environment() -> Result<bool> {
    wal_v8_extent_lookahead_from_value(std::env::var_os(WAL_V8_EXTENT_LOOKAHEAD_ENV).as_deref())
}

/// V8 WAL configuration profile.
/// Production defaults to [`WalV8Profile::Candidate`] (`Cursor`, Direct
/// completion, Direct FRONTIER with Buffered fallback, Point-only 1 MiB
/// lookahead when eligible, Stage A on, Dynamic + Point-only Target8);
/// `TOYKV_WAL_V8_PROFILE=legacy` remains available as a full fallback.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum WalV8Profile {
    Legacy,
    #[default]
    Candidate,
}

impl WalV8Profile {
    fn from_value(value: Option<&OsStr>) -> Result<Self> {
        match value {
            None => Ok(Self::Candidate),
            Some(value) => match value.to_str() {
                Some("legacy") => Ok(Self::Legacy),
                Some("candidate") => Ok(Self::Candidate),
                Some(value) => anyhow::bail!(
                    "invalid TOYKV_WAL_V8_PROFILE {value:?}; expected `legacy` or `candidate`"
                ),
                None => anyhow::bail!(
                    "TOYKV_WAL_V8_PROFILE is not valid UTF-8; expected `legacy` or `candidate`"
                ),
            },
        }
    }

    fn from_environment() -> Result<Self> {
        Self::from_value(std::env::var_os("TOYKV_WAL_V8_PROFILE").as_deref())
    }
}

/// Coalescing early-exit target policy for Cursor durability cycles.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum WalV8CoalesceTargetPolicy {
    #[default]
    Disabled,
    #[cfg(feature = "bench")]
    Always(usize),
    PointOnly(usize),
}

impl WalV8CoalesceTargetPolicy {
    fn target_for_snapshot(self, snapshot: &WalV8FrontierReadySnapshot) -> Option<usize> {
        match self {
            Self::Disabled => None,
            #[cfg(feature = "bench")]
            Self::Always(target) => Some(target),
            Self::PointOnly(target) => (!snapshot.has_outstanding_batch_tickets).then_some(target),
        }
    }

    fn target_count(self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            #[cfg(feature = "bench")]
            Self::Always(target) => Some(target),
            Self::PointOnly(target) => Some(target),
        }
    }

    #[cfg(feature = "bench")]
    fn policy_name(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Always(_) => "always",
            Self::PointOnly(_) => "point_only",
        }
    }

    fn from_value(
        value: Option<&OsStr>,
        progress_mode: WalV8ProgressMode,
        direct_completion_enabled: bool,
    ) -> Result<Option<Self>> {
        let Some(value) = value else {
            return Ok(None);
        };
        let value = value
            .to_str()
            .context("TOYKV_WAL_V8_COALESCING is not valid UTF-8")?;
        match value {
            "dynamic" => Ok(Some(Self::Disabled)),
            "point-target8" => {
                ensure!(
                    progress_mode == WalV8ProgressMode::Cursor && direct_completion_enabled,
                    "TOYKV_WAL_V8_COALESCING=point-target8 requires Cursor progress mode and direct completion"
                );
                Ok(Some(Self::PointOnly(8)))
            }
            _ => anyhow::bail!(
                "invalid TOYKV_WAL_V8_COALESCING {value:?}; expected `dynamic` or `point-target8`"
            ),
        }
    }

    fn from_environment(
        progress_mode: WalV8ProgressMode,
        direct_completion_enabled: bool,
    ) -> Result<Option<Self>> {
        Self::from_value(
            std::env::var_os("TOYKV_WAL_V8_COALESCING").as_deref(),
            progress_mode,
            direct_completion_enabled,
        )
    }
}

#[cfg(feature = "bench")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalV8BenchExtentOverride {
    Disabled,
    Mode(WalExtentPreparationMode, u64),
}

#[cfg(feature = "bench")]
fn bench_extent_override_from_environment() -> Result<Option<WalV8BenchExtentOverride>> {
    if let Some(value) = std::env::var_os(WAL_V8_EXTENT_PREPARATION_ENV)
        && matches!(value.to_str(), Some("none" | "disabled"))
    {
        return Ok(Some(WalV8BenchExtentOverride::Disabled));
    }
    Ok(WalExtentPreparationMode::from_environment()?
        .map(|(m, b)| WalV8BenchExtentOverride::Mode(m, b)))
}

/// Effective extent preparation policy resolved for one V8 WAL instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalV8ExtentPreparationPolicy {
    Disabled,
    FallocateOnly,
    Lookahead,
    PointOnlyLookahead,
    #[cfg(feature = "bench")]
    SlidingInitialize,
    #[cfg(feature = "bench")]
    OneShotFallocate,
    #[cfg(feature = "bench")]
    OneShotInitialize,
}

/// Process-wide V8 WAL configuration requested at Engine open and inherited
/// across automatic and PITR WAL rotations without on-disk serialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8RequestedConfig {
    pub(crate) profile: WalV8Profile,
    pub(crate) progress_mode: WalV8ProgressMode,
    pub(crate) direct_completion_enabled: bool,
    pub(crate) frontier_mode: WalV8FrontierWriteMode,
    pub(crate) extent_lookahead: bool,
    pub(crate) point_buffer_preallocation_enabled: bool,
    pub(crate) coalesce_policy: WalV8CoalesceTargetPolicy,
    #[cfg(feature = "bench")]
    progress_counters_enabled: bool,
    #[cfg(feature = "bench")]
    extent_preparation: Option<WalV8BenchExtentOverride>,
    #[cfg(feature = "bench")]
    extent_policy_storage: WalV8ExtentPolicyStorageKind,
    #[cfg(feature = "bench")]
    extent_policy_scan_diagnostics_enabled: bool,
    #[cfg(feature = "bench")]
    data_submit_ready_order: bool,
    #[cfg(feature = "bench")]
    point_direct_frame_enabled: bool,
}

impl Default for WalV8RequestedConfig {
    fn default() -> Self {
        Self::candidate()
    }
}

impl WalV8RequestedConfig {
    /// Construct the legacy V8 profile (`Eager`, Relay completion, Buffered
    /// FRONTIER, 1 MiB fallocate-only extent preparation, Stage A off,
    /// Dynamic coalescing), retained as the `TOYKV_WAL_V8_PROFILE=legacy` fallback.
    pub(crate) fn legacy() -> Self {
        Self {
            profile: WalV8Profile::Legacy,
            progress_mode: WalV8ProgressMode::default(),
            direct_completion_enabled: false,
            frontier_mode: WalV8FrontierWriteMode::default(),
            extent_lookahead: false,
            point_buffer_preallocation_enabled: false,
            coalesce_policy: WalV8CoalesceTargetPolicy::Disabled,
            #[cfg(feature = "bench")]
            progress_counters_enabled: false,
            #[cfg(feature = "bench")]
            extent_preparation: None,
            #[cfg(feature = "bench")]
            extent_policy_storage: WalV8ExtentPolicyStorageKind::Vec,
            #[cfg(feature = "bench")]
            extent_policy_scan_diagnostics_enabled: true,
            #[cfg(feature = "bench")]
            data_submit_ready_order: false,
            #[cfg(feature = "bench")]
            point_direct_frame_enabled: false,
        }
    }

    /// Construct the promoted V8 Candidate Target8 profile (`Cursor`, Direct
    /// completion, Direct FRONTIER with Buffered fallback, Point-only 1 MiB
    /// lookahead when eligible, Stage A on, Dynamic + Point-only Target8).
    #[allow(clippy::needless_update)]
    pub(crate) fn candidate() -> Self {
        Self {
            profile: WalV8Profile::Candidate,
            progress_mode: WalV8ProgressMode::Cursor,
            direct_completion_enabled: true,
            frontier_mode: WalV8FrontierWriteMode::Direct,
            extent_lookahead: false,
            point_buffer_preallocation_enabled: true,
            coalesce_policy: WalV8CoalesceTargetPolicy::PointOnly(8),
            ..Self::legacy()
        }
    }

    /// Parse V8 WAL configuration from environment variables and test overrides.
    pub(crate) fn from_environment() -> Result<Self> {
        #[cfg(test)]
        let test_profile = WAL_V8_TEST_PROFILE_OVERRIDE.with(std::cell::Cell::get);
        #[cfg(not(test))]
        let test_profile: Option<WalV8Profile> = None;
        let profile = match test_profile {
            Some(profile) => profile,
            None => WalV8Profile::from_environment()?,
        };
        let base = match profile {
            WalV8Profile::Legacy => Self::legacy(),
            WalV8Profile::Candidate => Self::candidate(),
        };

        let test_override_active = {
            #[cfg(test)]
            {
                WAL_V8_TEST_MODE_OVERRIDE
                    .with(std::cell::Cell::get)
                    .is_some()
            }
            #[cfg(not(test))]
            {
                false
            }
        };

        #[cfg(feature = "bench")]
        let progress_mode =
            if std::env::var_os(WAL_V8_POINT_PROGRESS_RING_ENV).is_some() || test_override_active {
                progress_mode_from_environment()?
            } else {
                base.progress_mode
            };
        #[cfg(not(feature = "bench"))]
        let progress_mode =
            if std::env::var_os(WAL_V8_PROGRESS_MODE_ENV).is_some() || test_override_active {
                production_progress_mode_from_environment()?
            } else {
                base.progress_mode
            };

        #[cfg(feature = "bench")]
        let direct_completion_enabled = if std::env::var_os(WAL_V8_CURSOR_DIRECT_COMPLETION_ENV)
            .is_some()
            || test_override_active
        {
            cursor_direct_completion_enabled_from_environment(progress_mode)?
        } else {
            base.direct_completion_enabled && progress_mode == WalV8ProgressMode::Cursor
        };
        #[cfg(not(feature = "bench"))]
        let direct_completion_enabled = if test_override_active {
            production_direct_completion_enabled_from_environment(progress_mode)?
        } else {
            base.direct_completion_enabled && progress_mode == WalV8ProgressMode::Cursor
        };

        let frontier_mode = if std::env::var_os("TOYKV_WAL_V8_FRONTIER_MODE").is_some() {
            WalV8FrontierWriteMode::from_environment()?
        } else {
            base.frontier_mode
        };
        let extent_lookahead = wal_v8_extent_lookahead_from_environment()?;

        #[cfg(feature = "bench")]
        let point_buffer_preallocation_enabled =
            if std::env::var_os("TOYKV_BENCH_V8_POINT_BUFFER_PREALLOC").is_some() {
                point_buffer_preallocation_from_environment()?
            } else {
                base.point_buffer_preallocation_enabled
            };
        #[cfg(not(feature = "bench"))]
        let point_buffer_preallocation_enabled = base.point_buffer_preallocation_enabled;

        let explicit_coalescing =
            WalV8CoalesceTargetPolicy::from_environment(progress_mode, direct_completion_enabled)?;
        #[cfg(feature = "bench")]
        let coalesce_policy = if let Some(target) =
            coalesce_ready_target_from_environment(progress_mode, direct_completion_enabled)?
        {
            ensure!(
                explicit_coalescing.is_none(),
                "TOYKV_WAL_V8_COALESCING and {WAL_V8_COALESCE_READY_TARGET_ENV} cannot both be set"
            );
            WalV8CoalesceTargetPolicy::Always(target)
        } else if let Some(policy) = explicit_coalescing {
            policy
        } else if progress_mode == WalV8ProgressMode::Cursor && direct_completion_enabled {
            base.coalesce_policy
        } else {
            WalV8CoalesceTargetPolicy::Disabled
        };
        #[cfg(not(feature = "bench"))]
        let coalesce_policy = if let Some(policy) = explicit_coalescing {
            policy
        } else if progress_mode == WalV8ProgressMode::Cursor && direct_completion_enabled {
            base.coalesce_policy
        } else {
            WalV8CoalesceTargetPolicy::Disabled
        };

        #[cfg(feature = "bench")]
        let progress_counters_enabled = progress_counters_enabled_from_environment(progress_mode)?;
        #[cfg(feature = "bench")]
        let extent_preparation = bench_extent_override_from_environment()?;
        #[cfg(feature = "bench")]
        let extent_policy_storage = extent_policy_storage_kind_from_env()?;
        #[cfg(feature = "bench")]
        let extent_policy_scan_diagnostics_enabled =
            extent_policy_scan_diagnostics_enabled_from_env()?;
        #[cfg(feature = "bench")]
        let data_submit_ready_order = benchmark_submits_any_ready_ticket()?;
        #[cfg(feature = "bench")]
        let point_direct_frame_enabled = point_direct_frame_from_environment()?;

        Ok(Self {
            profile,
            progress_mode,
            direct_completion_enabled,
            frontier_mode,
            extent_lookahead,
            point_buffer_preallocation_enabled,
            coalesce_policy,
            #[cfg(feature = "bench")]
            progress_counters_enabled,
            #[cfg(feature = "bench")]
            extent_preparation,
            #[cfg(feature = "bench")]
            extent_policy_storage,
            #[cfg(feature = "bench")]
            extent_policy_scan_diagnostics_enabled,
            #[cfg(feature = "bench")]
            data_submit_ready_order,
            #[cfg(feature = "bench")]
            point_direct_frame_enabled,
        })
    }

    pub(crate) fn with_progress_and_completion(
        mut self,
        progress_mode: WalV8ProgressMode,
        direct_completion_enabled: bool,
    ) -> Self {
        self.progress_mode = progress_mode;
        self.direct_completion_enabled = direct_completion_enabled;
        if !(progress_mode == WalV8ProgressMode::Cursor && direct_completion_enabled) {
            self.coalesce_policy = WalV8CoalesceTargetPolicy::Disabled;
        }
        self
    }
}

/// Per-WAL effective V8 configuration resolved from [`WalV8RequestedConfig`]
/// and the actual worker file, filesystem, and quota owner of one WAL segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalV8EffectiveConfig {
    pub(crate) profile: WalV8Profile,
    pub(crate) progress_mode: WalV8ProgressMode,
    pub(crate) direct_completion_enabled: bool,
    pub(crate) frontier_mode: WalV8FrontierWriteMode,
    pub(crate) extent_policy: WalV8ExtentPreparationPolicy,
    pub(crate) extent_window_bytes: u64,
    pub(crate) point_buffer_preallocation_enabled: bool,
    pub(crate) coalesce_policy: WalV8CoalesceTargetPolicy,
    pub(crate) o_direct_worker: bool,
    pub(crate) ext_family_filesystem: bool,
    pub(crate) has_reservation_owner: bool,
    #[cfg(feature = "bench")]
    pub(crate) progress_counters_enabled: bool,
    #[cfg(feature = "bench")]
    extent_policy_storage: WalV8ExtentPolicyStorageKind,
    #[cfg(feature = "bench")]
    extent_policy_scan_diagnostics_enabled: bool,
    #[cfg(feature = "bench")]
    data_submit_ready_order: bool,
    #[cfg(feature = "bench")]
    point_direct_frame_enabled: bool,
}

impl WalV8EffectiveConfig {
    /// Resolve effective settings for one V8 WAL instance before spawning its worker.
    pub(crate) fn resolve(
        requested: &WalV8RequestedConfig,
        worker_file: &File,
        has_reservation_owner: bool,
    ) -> Result<Self> {
        Self::resolve_with_test_override(
            requested,
            worker_file,
            has_reservation_owner,
            #[cfg(test)]
            None,
        )
    }

    fn resolve_with_test_override(
        requested: &WalV8RequestedConfig,
        worker_file: &File,
        has_reservation_owner: bool,
        #[cfg(test)] extent_strategy_override: Option<(WalV8ExtentPreparationStrategy, u64)>,
    ) -> Result<Self> {
        ensure!(
            !requested.direct_completion_enabled
                || requested.progress_mode == WalV8ProgressMode::Cursor,
            "direct completion requires Cursor progress mode"
        );
        let o_direct_worker = is_o_direct_file(worker_file)?;
        let ext_family_filesystem = is_ext_family_filesystem(worker_file);
        let frontier_mode = match requested.frontier_mode {
            WalV8FrontierWriteMode::Buffered => WalV8FrontierWriteMode::Buffered,
            WalV8FrontierWriteMode::Direct => {
                if o_direct_worker {
                    WalV8FrontierWriteMode::Direct
                } else {
                    WalV8FrontierWriteMode::Buffered
                }
            }
        };

        let (extent_policy, extent_window_bytes) = {
            #[cfg(test)]
            let override_strategy = extent_strategy_override;
            #[cfg(not(test))]
            let override_strategy: Option<(WalV8ExtentPreparationStrategy, u64)> = None;

            let resolve_production_extent = || -> Result<(WalV8ExtentPreparationPolicy, u64)> {
                let eligible = has_reservation_owner && ext_family_filesystem && o_direct_worker;
                ensure!(
                    !requested.extent_lookahead || eligible,
                    "{WAL_V8_EXTENT_LOOKAHEAD_ENV} requires an ext4 WAL, an O_DIRECT DATA worker, and a quota owner"
                );
                if eligible {
                    let policy = if requested.extent_lookahead {
                        WalV8ExtentPreparationPolicy::Lookahead
                    } else if requested.profile == WalV8Profile::Candidate {
                        WalV8ExtentPreparationPolicy::PointOnlyLookahead
                    } else {
                        WalV8ExtentPreparationPolicy::FallocateOnly
                    };
                    Ok((policy, WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES))
                } else {
                    Ok((WalV8ExtentPreparationPolicy::Disabled, 0))
                }
            };

            if let Some((strategy, window_bytes)) = override_strategy {
                ensure!(
                    has_reservation_owner,
                    "point-only extent strategy test requires PITR quota accounting"
                );
                let policy = match strategy {
                    WalV8ExtentPreparationStrategy::FallocateOnly => {
                        WalV8ExtentPreparationPolicy::FallocateOnly
                    }
                    WalV8ExtentPreparationStrategy::Lookahead => {
                        WalV8ExtentPreparationPolicy::Lookahead
                    }
                    WalV8ExtentPreparationStrategy::PointOnlyLookahead => {
                        WalV8ExtentPreparationPolicy::PointOnlyLookahead
                    }
                };
                (policy, window_bytes)
            } else {
                #[cfg(feature = "bench")]
                match requested.extent_preparation {
                    Some(WalV8BenchExtentOverride::Disabled) => {
                        (WalV8ExtentPreparationPolicy::Disabled, 0)
                    }
                    Some(WalV8BenchExtentOverride::Mode(mode, bytes)) => {
                        let policy = match mode {
                            WalExtentPreparationMode::SlidingLookahead => {
                                ensure!(
                                    has_reservation_owner,
                                    "sliding lookahead benchmark requires PITR quota accounting"
                                );
                                WalV8ExtentPreparationPolicy::Lookahead
                            }
                            WalExtentPreparationMode::SlidingPointLookahead => {
                                ensure!(
                                    has_reservation_owner,
                                    "point-only sliding lookahead benchmark requires PITR quota accounting"
                                );
                                WalV8ExtentPreparationPolicy::PointOnlyLookahead
                            }
                            WalExtentPreparationMode::SlidingFallocate => {
                                WalV8ExtentPreparationPolicy::FallocateOnly
                            }
                            WalExtentPreparationMode::SlidingInitialize => {
                                WalV8ExtentPreparationPolicy::SlidingInitialize
                            }
                            WalExtentPreparationMode::Fallocate => {
                                WalV8ExtentPreparationPolicy::OneShotFallocate
                            }
                            WalExtentPreparationMode::Initialize => {
                                WalV8ExtentPreparationPolicy::OneShotInitialize
                            }
                        };
                        (policy, bytes)
                    }
                    None => {
                        if requested.profile == WalV8Profile::Candidate
                            || requested.extent_lookahead
                        {
                            resolve_production_extent()?
                        } else {
                            // Preserve historical bench default when no extent
                            // override or candidate profile is selected.
                            (WalV8ExtentPreparationPolicy::Disabled, 0)
                        }
                    }
                }
                #[cfg(not(feature = "bench"))]
                resolve_production_extent()?
            }
        };

        Ok(Self {
            profile: requested.profile,
            progress_mode: requested.progress_mode,
            direct_completion_enabled: requested.direct_completion_enabled,
            frontier_mode,
            extent_policy,
            extent_window_bytes,
            point_buffer_preallocation_enabled: requested.point_buffer_preallocation_enabled,
            coalesce_policy: requested.coalesce_policy,
            o_direct_worker,
            ext_family_filesystem,
            has_reservation_owner,
            #[cfg(feature = "bench")]
            progress_counters_enabled: requested.progress_counters_enabled,
            #[cfg(feature = "bench")]
            extent_policy_storage: requested.extent_policy_storage,
            #[cfg(feature = "bench")]
            extent_policy_scan_diagnostics_enabled: requested
                .extent_policy_scan_diagnostics_enabled,
            #[cfg(feature = "bench")]
            data_submit_ready_order: requested.data_submit_ready_order,
            #[cfg(feature = "bench")]
            point_direct_frame_enabled: requested.point_direct_frame_enabled,
        })
    }
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
            worker_handle: WalV8ExtentWorkerHandle::new(request_sender, worker),
        })
    }

    fn quiesce(&self) {
        self.worker_handle
            .quiesce("V8 background extent initializer panicked");
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
            self.worker_handle.send(
                required_end,
                "V8 background extent initializer is unavailable",
                "V8 background extent initializer stopped unexpectedly",
            )?;
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
    #[cfg(any(test, feature = "bench"))]
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
    PointOnlyLookahead,
}

impl WalV8ExtentPreparationStrategy {
    const fn uses_lookahead(self) -> bool {
        !matches!(self, Self::FallocateOnly)
    }

    const fn is_point_only(self) -> bool {
        matches!(self, Self::PointOnlyLookahead)
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
    worker_handle: WalV8ExtentWorkerHandle<()>,
    #[cfg(test)]
    worker_hook_for_test: WalV8LookaheadWorkerHook,
    #[cfg(test)]
    quiesce_hook_for_test: WalV8LookaheadWorkerHook,
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
        Self::new_with_strategy_and_policy_config(
            data_file,
            sync_file,
            start_offset,
            max_file_size,
            window_bytes,
            reservation_owner,
            strategy,
            WalV8ExtentPolicyStorageKind::Vec,
            cfg!(feature = "bench"),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_strategy_and_policy_config(
        data_file: File,
        sync_file: &File,
        start_offset: u64,
        max_file_size: u64,
        window_bytes: u64,
        reservation_owner: Option<Arc<WalV8ReservationOwner>>,
        strategy: WalV8ExtentPreparationStrategy,
        extent_policy_storage_kind: WalV8ExtentPolicyStorageKind,
        extent_policy_scan_diagnostics_enabled: bool,
    ) -> Result<Self> {
        let lookahead = strategy.uses_lookahead();
        let point_only_lookahead = strategy.is_point_only();
        #[cfg(not(feature = "bench"))]
        let _extent_policy_scan_diagnostics_enabled = extent_policy_scan_diagnostics_enabled;
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
            #[cfg(any(test, feature = "bench"))]
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
        #[cfg(test)]
        let worker_hook_for_test: WalV8LookaheadWorkerHook = Arc::new(Mutex::new(None));
        let (request_sender, worker) = if lookahead {
            let worker_file = data_file
                .try_clone()
                .context("clone V8 WAL handle for extent lookahead")?;
            let worker_state = Arc::clone(&state);
            let worker_prepared = Arc::clone(&prepared);
            let worker_preparation_lock = Arc::clone(&preparation_lock);
            #[cfg(test)]
            let worker_hook = Arc::clone(&worker_hook_for_test);
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
                            #[cfg(test)]
                            worker_hook,
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
            worker_handle: WalV8ExtentWorkerHandle::new(request_sender, worker),
            #[cfg(test)]
            worker_hook_for_test,
            #[cfg(test)]
            quiesce_hook_for_test: Arc::new(Mutex::new(None)),
        })
    }

    #[cfg(test)]
    fn set_worker_hook_for_test(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self.worker_hook_for_test.lock() = hook;
    }

    #[cfg(test)]
    fn set_quiesce_hook_for_test(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self.quiesce_hook_for_test.lock() = hook;
    }

    fn quiesce(&self) {
        #[cfg(test)]
        if let Some(hook) = self.quiesce_hook_for_test.lock().take() {
            hook();
        }
        self.worker_handle
            .quiesce("V8 extent lookahead worker panicked");
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
            self.worker_handle.send(
                (),
                "V8 extent lookahead worker is unavailable",
                "V8 extent lookahead worker stopped unexpectedly",
            )?;
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
        self.quiesce();
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
    #[cfg(test)]
    worker_hook: WalV8LookaheadWorkerHook,
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
            #[cfg(test)]
            worker_hook,
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

                    #[cfg(test)]
                    if initialized_start >= required_end
                        && let Some(hook) = worker_hook.lock().take()
                    {
                        hook();
                    }

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
                    #[cfg(any(test, feature = "bench"))]
                    {
                        state_guard.initialized_bytes = state_guard
                            .initialized_bytes
                            .saturating_add(initialized_end - initialized_start);
                    }
                    #[cfg(feature = "bench")]
                    {
                        let initialization_elapsed_ns =
                            u64::try_from(initialization_started.elapsed().as_nanos())
                                .unwrap_or(u64::MAX);
                        state_guard.initialization_chunks =
                            state_guard.initialization_chunks.saturating_add(1);
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
        self.quiesce();
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
fn coalesce_ready_target_from_value(
    value: Option<&OsStr>,
    progress_mode: WalV8ProgressMode,
    direct_completion_enabled: bool,
) -> Result<Option<usize>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .with_context(|| format!("{WAL_V8_COALESCE_READY_TARGET_ENV} is not valid UTF-8"))?;
    let target = value.parse::<usize>().with_context(|| {
        format!("invalid {WAL_V8_COALESCE_READY_TARGET_ENV} {value:?}; expected `4` or `8`")
    })?;
    ensure!(
        matches!(target, 4 | 8),
        "invalid {WAL_V8_COALESCE_READY_TARGET_ENV} {value:?}; expected `4` or `8`"
    );
    ensure!(
        progress_mode == WalV8ProgressMode::Cursor && direct_completion_enabled,
        "{WAL_V8_COALESCE_READY_TARGET_ENV} requires Cursor progress mode and direct completion"
    );
    Ok(Some(target))
}

#[cfg(feature = "bench")]
fn coalesce_ready_target_from_environment(
    progress_mode: WalV8ProgressMode,
    direct_completion_enabled: bool,
) -> Result<Option<usize>> {
    coalesce_ready_target_from_value(
        std::env::var_os(WAL_V8_COALESCE_READY_TARGET_ENV).as_deref(),
        progress_mode,
        direct_completion_enabled,
    )
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

#[cfg(feature = "bench")]
fn point_direct_frame_from_value(value: Option<&OsStr>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let value = value
        .to_str()
        .context("TOYKV_BENCH_V8_POINT_DIRECT_FRAME is not valid UTF-8")?;
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => anyhow::bail!(
            "invalid TOYKV_BENCH_V8_POINT_DIRECT_FRAME {value:?}; expected `0` or `1`"
        ),
    }
}

#[cfg(feature = "bench")]
fn point_direct_frame_from_environment() -> Result<bool> {
    point_direct_frame_from_value(std::env::var_os("TOYKV_BENCH_V8_POINT_DIRECT_FRAME").as_deref())
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
    submitted_group_snapshots: Vec<WalV8SubmittedGroupSnapshot>,
    #[cfg(feature = "bench")]
    data_submit_hol: Option<WalV8DataSubmitHol>,
}

enum WalV8ProgressWake {
    Control,
    TimedOut,
}

impl WalV8ProgressWake {
    fn is_signaled(&self) -> bool {
        match self {
            Self::Control => true,
            Self::TimedOut => false,
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

/// Prefer a queued control token before looking at the DATA queue. If DATA is
/// selected first while a control token races in, the token remains available
/// for the pre-scan drain and subsequent wait.
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
        #[cfg(feature = "bench")]
        completion_observed_at: completion.completion_observed_at,
    })
}

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

#[cfg(feature = "bench")]
fn benchmark_submits_any_ready_ticket() -> Result<bool> {
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
    point_direct_frame_enabled: bool,
    #[cfg(feature = "bench")]
    profile: arc_swap::ArcSwapOption<crate::mem_table::WriteProfile>,
    #[cfg(feature = "bench")]
    hol_diagnostics_enabled: std::sync::atomic::AtomicBool,
    extent_preallocator: Option<Arc<WalV8ExtentPreallocator>>,
    #[cfg(feature = "bench")]
    extent_preparation: Option<Arc<WalV8SlidingExtentPreparation>>,
    completion_events: Option<Receiver<WalV8CompletedGroupEvent>>,
    direct_completions: Option<Receiver<DirectGroupWriteCompletion>>,
    coalesce_ready_target: WalV8CoalesceTargetPolicy,
    #[cfg(feature = "bench")]
    coalesce_ready_target_hits: AtomicU64,
    #[cfg(feature = "bench")]
    coalesce_target_eligible_cycles: AtomicU64,
    #[cfg(feature = "bench")]
    coalesce_target_mixed_fallback_cycles: AtomicU64,
    #[cfg(feature = "bench")]
    coalesce_target_initial_hits: AtomicU64,
    #[cfg(feature = "bench")]
    coalesce_target_after_wake_hits: AtomicU64,
    #[cfg(test)]
    progress_wait_hook: Mutex<Option<WalV8ProgressWaitHook>>,
    #[cfg(test)]
    completion_reap_hook: WalV8SharedCompletionReapHook,
    progress_tokens: Option<Receiver<()>>,
    shutdown_started: Arc<AtomicBool>,
    completion_thread: Mutex<Option<thread::JoinHandle<()>>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WalV8CoalesceOutcome {
    waited: bool,
    #[cfg(feature = "bench")]
    target_hit: bool,
    #[cfg(feature = "bench")]
    eligible_cycle: bool,
    #[cfg(feature = "bench")]
    mixed_fallback_cycle: bool,
}

struct WalV8ParallelIoConfig {
    #[cfg(feature = "bench")]
    extent_preparation: Option<Arc<WalV8SlidingExtentPreparation>>,
    direct_completion_enabled: bool,
    coalesce_ready_target: WalV8CoalesceTargetPolicy,
    #[cfg(feature = "bench")]
    data_submit_ready_order: bool,
    #[cfg(feature = "bench")]
    point_direct_frame_enabled: bool,
    #[cfg(test)]
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
        let coalesce_ready_target = config.coalesce_ready_target;
        #[cfg(test)]
        let worker_group_limit = config.worker_group_limit;

        ensure!(
            !direct_completion_enabled || coordinator.progress_cursor_enabled(),
            "Cursor direct completion requires Cursor progress mode"
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
        #[cfg(feature = "bench")]
        let submit_any_ready_ticket = config.data_submit_ready_order;
        #[cfg(not(feature = "bench"))]
        let submit_any_ready_ticket = false;
        let worker_file = Arc::new(worker_file);
        let buffer_pool = Arc::new(ArrayQueue::<WalV8DataBuffer>::new(1));
        let direct_completion_handle = coordinator.worker_completion_handle();
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
            point_direct_frame_enabled: config.point_direct_frame_enabled,
            #[cfg(feature = "bench")]
            profile: arc_swap::ArcSwapOption::empty(),
            #[cfg(feature = "bench")]
            hol_diagnostics_enabled: std::sync::atomic::AtomicBool::new(false),
            extent_preallocator,
            #[cfg(feature = "bench")]
            extent_preparation,
            completion_events,
            direct_completions,
            coalesce_ready_target,
            #[cfg(feature = "bench")]
            coalesce_ready_target_hits: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            coalesce_target_eligible_cycles: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            coalesce_target_mixed_fallback_cycles: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            coalesce_target_initial_hits: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            coalesce_target_after_wake_hits: AtomicU64::new(0),
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
        self.advance_and_snapshot_after_wake(driver, capture_monotonic_ns)
    }

    fn advance_and_snapshot_after_wake(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_inner(
            driver,
            capture_monotonic_ns,
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
        self.advance_and_snapshot_after_wake_with_timing(driver, capture_monotonic_ns, timing)
    }

    #[cfg(feature = "bench")]
    fn advance_and_snapshot_after_wake_with_timing(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
        timing: &mut WalV8CursorCoalescingTiming,
    ) -> Result<WalV8FrontierReadySnapshot> {
        self.advance_and_snapshot_inner(driver, capture_monotonic_ns, Some(timing))
    }

    fn advance_and_snapshot_inner(
        &self,
        driver: &mut WalV8DurabilityDriver,
        capture_monotonic_ns: bool,
        #[cfg(feature = "bench")] mut timing: Option<&mut WalV8CursorCoalescingTiming>,
    ) -> Result<WalV8FrontierReadySnapshot> {
        ensure!(
            self.coordinator.owns_durability_driver(driver),
            "v8 progress scan is not owned by the active durability driver"
        );
        let has_direct_completion_queue = self.direct_completions.is_some();
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
            let capacity = self.coordinator.completion_queue_capacity();
            ensure!(
                batch.len() <= capacity,
                "cursor completion seed exceeds the reusable batch capacity"
            );
            let remaining_capacity = capacity - batch.len();
            if let Some(receiver) = &self.completion_events {
                self.drain_completion_events(receiver, &mut batch, remaining_capacity)
            } else {
                let Some(receiver) = self.direct_completions.as_ref() else {
                    anyhow::bail!("Cursor direct completion queue is missing")
                };
                self.drain_direct_completion_events(receiver, &mut batch, remaining_capacity)
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

    fn coalesce_ready_target_reached(ready_prefix_len: usize, target: Option<usize>) -> bool {
        target.is_some_and(|target| ready_prefix_len >= target)
    }

    fn coalesce_ready_prefix_with(
        snapshot: &mut WalV8FrontierReadySnapshot,
        budget: Duration,
        coalesce_policy: WalV8CoalesceTargetPolicy,
        mut wait_and_advance: impl FnMut(Duration, &mut WalV8FrontierReadySnapshot) -> Result<bool>,
    ) -> Result<WalV8CoalesceOutcome> {
        let deadline = Instant::now() + budget;
        #[cfg(feature = "bench")]
        let mut saw_mixed_fallback =
            matches!(coalesce_policy, WalV8CoalesceTargetPolicy::PointOnly(_))
                && snapshot.has_outstanding_batch_tickets;
        let mut outcome = WalV8CoalesceOutcome {
            waited: false,
            #[cfg(feature = "bench")]
            target_hit: !snapshot.closed
                && snapshot.ready_prefix.len() < snapshot.admitted_ticket_count
                && Self::coalesce_ready_target_reached(
                    snapshot.ready_prefix.len(),
                    coalesce_policy.target_for_snapshot(snapshot),
                ),
            #[cfg(feature = "bench")]
            eligible_cycle: false,
            #[cfg(feature = "bench")]
            mixed_fallback_cycle: false,
        };

        while !snapshot.closed
            && !Self::coalesce_ready_target_reached(
                snapshot.ready_prefix.len(),
                coalesce_policy.target_for_snapshot(snapshot),
            )
            && let Some(remaining) = deadline.checked_duration_since(Instant::now())
        {
            outcome.waited = true;
            let signaled = wait_and_advance(remaining, snapshot)?;
            let current_target = coalesce_policy.target_for_snapshot(snapshot);
            #[cfg(feature = "bench")]
            {
                if matches!(coalesce_policy, WalV8CoalesceTargetPolicy::PointOnly(_))
                    && snapshot.has_outstanding_batch_tickets
                {
                    saw_mixed_fallback = true;
                }
                if signaled
                    && !snapshot.closed
                    && snapshot.ready_prefix.len() < snapshot.admitted_ticket_count
                    && Self::coalesce_ready_target_reached(
                        snapshot.ready_prefix.len(),
                        current_target,
                    )
                {
                    outcome.target_hit = true;
                }
            }
            if !signaled
                || snapshot.ready_prefix.is_empty()
                || snapshot.ready_prefix.len() >= snapshot.admitted_ticket_count
                || Self::coalesce_ready_target_reached(snapshot.ready_prefix.len(), current_target)
            {
                break;
            }
        }

        #[cfg(feature = "bench")]
        if !snapshot.closed {
            match coalesce_policy {
                WalV8CoalesceTargetPolicy::Disabled => {}
                WalV8CoalesceTargetPolicy::Always(_) => {
                    outcome.eligible_cycle = true;
                }
                WalV8CoalesceTargetPolicy::PointOnly(_) => {
                    if saw_mixed_fallback {
                        outcome.mixed_fallback_cycle = true;
                    } else {
                        outcome.eligible_cycle = true;
                    }
                }
            }
        }

        Ok(outcome)
    }

    fn coalesce_progress_ring(
        &self,
        driver: &mut WalV8DurabilityDriver,
        snapshot: &mut WalV8FrontierReadySnapshot,
        budget: Duration,
        capture_monotonic_ns: bool,
        #[cfg(feature = "bench")] mut timing: Option<&mut WalV8CursorCoalescingTiming>,
    ) -> Result<bool> {
        let outcome = Self::coalesce_ready_prefix_with(
            snapshot,
            budget,
            self.coalesce_ready_target,
            |remaining, snapshot| {
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
                #[cfg(feature = "bench")]
                if let Some(timing) = timing.as_deref_mut() {
                    timing.progress_wait_wall_ns = timing.progress_wait_wall_ns.saturating_add(
                        wait_wall_started
                            .map(|started| {
                                started.elapsed().as_nanos().min(u64::MAX as u128) as u64
                            })
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
                        timing,
                    )?;
                } else {
                    *snapshot =
                        self.advance_and_snapshot_after_wake(driver, capture_monotonic_ns)?;
                }
                #[cfg(not(feature = "bench"))]
                {
                    *snapshot =
                        self.advance_and_snapshot_after_wake(driver, capture_monotonic_ns)?;
                }
                #[cfg(feature = "bench")]
                if let Some(timing) = timing.as_deref_mut() {
                    timing.ready_tickets_gained = timing.ready_tickets_gained.saturating_add(
                        snapshot.ready_prefix.len().saturating_sub(ready_before) as u64,
                    );
                }
                Ok(signaled)
            },
        )?;
        #[cfg(feature = "bench")]
        {
            if outcome.eligible_cycle {
                self.coalesce_target_eligible_cycles
                    .fetch_add(1, Ordering::Relaxed);
            }
            if outcome.mixed_fallback_cycle {
                self.coalesce_target_mixed_fallback_cycles
                    .fetch_add(1, Ordering::Relaxed);
            }
            if outcome.target_hit {
                self.coalesce_ready_target_hits
                    .fetch_add(1, Ordering::Relaxed);
                if outcome.waited {
                    self.coalesce_target_after_wake_hits
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.coalesce_target_initial_hits
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(outcome.waited)
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
        // This callback holds the coordinator's admission lock. An elected
        // packer will observe the gap after waking; otherwise drain ready
        // work directly without acquiring the coordinator in reverse order.
        if submission.submit_any_ready_ticket {
            self.drain_ready_tickets_locked(&mut submission)
                .context("submit v8 DATA after registering a FRONTIER gap")?;
        } else {
            self.frames_ready.notify_all();
            if !submission.packing {
                self.drain_ready_locked(&mut submission)
                    .context("submit v8 DATA after registering a FRONTIER gap")?;
            }
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
        if submission.submit_any_ready_ticket {
            ensure!(
                !submission.pending.contains_key(&ticket),
                "v8 DATA ticket was queued twice"
            );
            if submission.pending.is_empty() {
                let admitted_end = self.coordinator.admitted_ticket_end();
                ensure!(
                    ticket < admitted_end,
                    "v8 pending DATA ticket has not been admitted"
                );
                return self.submit_ready_ticket_locked(
                    &mut submission,
                    ticket,
                    file_offset,
                    buffer,
                );
            }
            submission.pending.insert(
                ticket,
                PendingV8DataWrite {
                    file_offset,
                    buffer,
                    #[cfg(feature = "bench")]
                    frame_ready_at,
                },
            );
            return self.drain_ready_tickets_locked(&mut submission);
        }
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
        self.update_data_submit_hol(&mut submission);
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

    fn submit_ready_ticket_locked(
        &self,
        state: &mut WalV8SubmissionState,
        ticket: u64,
        file_offset: u64,
        buffer: WalV8DataBuffer,
    ) -> Result<()> {
        let write_len = buffer.len();
        let allocation_end = file_offset
            .checked_add(u64::try_from(write_len)?)
            .context("v8 out-of-order DATA allocation end overflows")?;
        let ticket_end = ticket
            .checked_add(1)
            .context("v8 out-of-order DATA ticket overflows")?;
        self.ensure_extent_prepared_through(allocation_end)?;
        self.consume_extent_source_credit(file_offset, allocation_end)?;
        let write = WriteBuffer::new(buffer, file_offset, write_len);
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
        Ok(())
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
            self.submit_ready_ticket_locked(state, ticket, pending.file_offset, pending.buffer)?;
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
            let mut selected = Vec::with_capacity(MAX_GROUP_TICKETS);
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
                selected.push(next_ticket);
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
            let mut writes = Vec::with_capacity(selected.len());
            for ticket in selected {
                writes.push(self.take_pending_write(state, ticket)?);
            }
            let writes: WriteGroupBuffers<WalV8DataBuffer> = writes.into();

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
    ticket_resources: Arc<Mutex<BTreeMap<u64, WalV8TicketResources>>>,
    io: Arc<WalV8ParallelIo>,
    data_buffer: Option<DirectBufAllocation>,
    reservation: Option<WalV7OwnedBatchReservation>,
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
        #[cfg(feature = "bench")]
        let enabled = self.io.point_direct_frame_enabled;
        #[cfg(not(feature = "bench"))]
        let enabled = false;
        self.finish_with_point_direct_frame(enabled)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn finish_with_cpu_attribution(
        self,
    ) -> Result<(digest::WalV8DigestMetrics, WalV8ProducerCpuPhases)> {
        let mut phases = WalV8ProducerCpuPhases::default();
        let enabled = self.io.point_direct_frame_enabled;
        let metrics = self.finish_with_point_direct_frame_and_phases(enabled, Some(&mut phases))?;
        Ok((metrics, phases))
    }

    fn finish_with_point_direct_frame(
        self,
        direct_point_enabled: bool,
    ) -> Result<digest::WalV8DigestMetrics> {
        self.finish_with_point_direct_frame_and_phases(direct_point_enabled, None)
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
        let merge_digest_and_frames_ready = self.coordinator.progress_ring_enabled()
            && self.coordinator.progress_cursor_enabled()
            && admitted.can_encode_point_single_frame();
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
        let digest_ready = encoded.digest_ready()?;
        #[cfg(feature = "bench")]
        let ordered_link_hash_cpu_ns = if merge_digest_and_frames_ready {
            0
        } else {
            let digest_started = phases
                .as_ref()
                .map(|_| (Instant::now(), digest::thread_cpu_time_ns()));
            let cpu_ns = self
                .coordinator
                .mark_digest_ready_before_data(digest_ready)?;
            if let (Some(phases), Some((wall_started, cpu_started))) =
                (phases.as_deref_mut(), digest_started)
            {
                phases.digest_ready_wall_ns =
                    wall_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                phases.digest_ready_cpu_ns =
                    digest::thread_cpu_time_ns().saturating_sub(cpu_started);
            }
            cpu_ns
        };
        #[cfg(not(feature = "bench"))]
        if !merge_digest_and_frames_ready {
            self.coordinator
                .mark_digest_ready_before_data(digest_ready)?;
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
            let sample_timeline =
                sample_every > 0 && self.assignment.segment_ticket.is_multiple_of(sample_every);
            if merge_digest_and_frames_ready {
                let frame_ready_at = self
                    .coordinator
                    .publish_digest_and_frames_ready_before_data(digest_ready, sample_timeline)?;
                if let Some(profile) = &self.profile {
                    profile.record_wal_v8_merged_digest_frames_ready();
                }
                frame_ready_at
            } else {
                self.coordinator.mark_frames_ready_with_timeline_sample(
                    self.assignment.segment_ticket,
                    sample_timeline,
                )?
            }
        };
        #[cfg(not(feature = "bench"))]
        let frame_ready_at = if merge_digest_and_frames_ready {
            self.coordinator
                .publish_digest_and_frames_ready_before_data(digest_ready, false)?
        } else {
            self.coordinator
                .mark_frames_ready(self.assignment.segment_ticket)?;
            None
        };
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
        let guard = encoded.into_durability_guard();
        let reservation = self.reservation.take();
        {
            let mut ticket_resources = self.ticket_resources.lock();
            match ticket_resources.entry(ticket) {
                std::collections::btree_map::Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    if entry.guard.is_some() {
                        self.coordinator.poison();
                        anyhow::bail!("v8 Parallel ticket already owns a producer guard");
                    }
                    if reservation.is_some() {
                        if entry.reservation.is_some() {
                            self.coordinator.poison();
                            anyhow::bail!("v8 Parallel ticket already owns a resource reservation");
                        }
                        entry.reservation = reservation;
                    }
                    entry.guard = Some(guard);
                }
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    vacant.insert(WalV8TicketResources {
                        reservation,
                        guard: Some(guard),
                    });
                }
            }
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
            coalesce_ready_target: None,
            coalesce_ready_target_hits: 0,
            coalesce_target_policy: "disabled",
            coalesce_target_eligible_cycles: 0,
            coalesce_target_mixed_fallback_cycles: 0,
            coalesce_target_initial_hits: 0,
            coalesce_target_after_wake_hits: 0,
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
        let requested = WalV8RequestedConfig::from_environment()?;
        self.into_parallel_with_worker_file_and_config(worker_file, requested)
    }

    /// Transfer the validated image and coordinator to the concurrent
    /// producer path using a pre-parsed requested configuration.
    pub(crate) fn into_parallel_with_worker_file_and_config(
        self,
        worker_file: File,
        requested: WalV8RequestedConfig,
    ) -> Result<WalV8ParallelWriter> {
        self.into_parallel_with_worker_file_config_and_overrides(
            worker_file,
            requested,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
    }

    fn into_parallel_with_worker_file_and_progress_mode(
        self,
        worker_file: File,
        progress_mode: WalV8ProgressMode,
    ) -> Result<WalV8ParallelWriter> {
        #[cfg(feature = "bench")]
        let direct_completion_enabled =
            cursor_direct_completion_enabled_from_environment(progress_mode)?;
        #[cfg(not(feature = "bench"))]
        let direct_completion_enabled =
            production_direct_completion_enabled_from_environment(progress_mode)?;
        let requested = WalV8RequestedConfig::from_environment()?
            .with_progress_and_completion(progress_mode, direct_completion_enabled);

        self.into_parallel_with_worker_file_config_and_overrides(
            worker_file,
            requested,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
    }

    fn into_parallel_with_worker_file_progress_and_completion(
        self,
        worker_file: File,
        progress_mode: WalV8ProgressMode,
        direct_completion_enabled: bool,
        frontier_mode: WalV8FrontierWriteMode,
        #[cfg(test)] worker_group_limit: Option<usize>,
        #[cfg(test)] extent_strategy_override: Option<(WalV8ExtentPreparationStrategy, u64)>,
    ) -> Result<WalV8ParallelWriter> {
        let mut requested = WalV8RequestedConfig::from_environment()?
            .with_progress_and_completion(progress_mode, direct_completion_enabled);
        requested.frontier_mode = frontier_mode;

        self.into_parallel_with_worker_file_config_and_overrides(
            worker_file,
            requested,
            #[cfg(test)]
            worker_group_limit,
            #[cfg(test)]
            extent_strategy_override,
        )
    }

    fn into_parallel_with_worker_file_config_and_overrides(
        mut self,
        worker_file: File,
        requested: WalV8RequestedConfig,
        #[cfg(test)] worker_group_limit: Option<usize>,
        #[cfg(test)] extent_strategy_override: Option<(WalV8ExtentPreparationStrategy, u64)>,
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
        let effective_config = WalV8EffectiveConfig::resolve_with_test_override(
            &requested,
            &worker_file,
            self.reservation_owner.is_some(),
            #[cfg(test)]
            extent_strategy_override,
        )?;
        self.coordinator = self
            .coordinator
            .with_progress_mode(self.header, effective_config.progress_mode)?;
        #[cfg(feature = "bench")]
        self.coordinator
            .set_progress_counters_enabled(effective_config.progress_counters_enabled)?;
        #[cfg(feature = "bench")]
        let profile = self.profile.load_full();
        #[cfg(feature = "bench")]
        if let Some(profile) = &profile {
            if effective_config.progress_mode.uses_ring() {
                profile.validate_v8_progress_ring_compatibility()?;
            }
            self.coordinator
                .set_hol_diagnostics_profile(Arc::clone(profile));
            self.coordinator
                .set_data_completion_timestamps_enabled(profile.wal_sync_diagnostics_enabled());
        }
        #[cfg(feature = "bench")]
        let mut io_diagnostics = self.io_diagnostics;
        let coalesce_ready_target = effective_config.coalesce_policy;
        let point_buffer_preallocation_enabled =
            effective_config.point_buffer_preallocation_enabled;
        #[cfg(feature = "bench")]
        {
            io_diagnostics.data = WalIoHandleMetadata::from_file(&worker_file)?;
            io_diagnostics.progress_ring_enabled = effective_config.progress_mode.uses_ring();
            io_diagnostics.progress_cursor_enabled = self.coordinator.progress_cursor_enabled();
            io_diagnostics.cursor_direct_completion_enabled =
                effective_config.direct_completion_enabled;
            io_diagnostics.cursor_direct_completion_select_wake_enabled = false;
            io_diagnostics.cursor_singleton_submission_enabled = false;
            io_diagnostics.point_buffer_preallocation_enabled = point_buffer_preallocation_enabled;
        }
        #[cfg(feature = "bench")]
        type ExtentPrep = Option<Arc<WalV8SlidingExtentPreparation>>;
        #[cfg(not(feature = "bench"))]
        type ExtentPrep = Option<()>;
        let (extent_preallocator, _extent_preparation): (
            Option<Arc<WalV8ExtentPreallocator>>,
            ExtentPrep,
        ) = {
            let start_offset = self.coordinator.last_frontier_end();
            #[cfg(feature = "bench")]
            let extent_policy_storage = effective_config.extent_policy_storage;
            #[cfg(not(feature = "bench"))]
            let extent_policy_storage = WalV8ExtentPolicyStorageKind::Vec;
            #[cfg(feature = "bench")]
            let extent_policy_scan_diagnostics_enabled =
                effective_config.extent_policy_scan_diagnostics_enabled;
            #[cfg(not(feature = "bench"))]
            let extent_policy_scan_diagnostics_enabled = false;

            let preallocate_with_strategy = |strategy: WalV8ExtentPreparationStrategy,
                                             owner: &Arc<WalV8ReservationOwner>|
             -> Result<Arc<WalV8ExtentPreallocator>> {
                Ok(Arc::new(
                    WalV8ExtentPreallocator::new_with_strategy_and_policy_config(
                        worker_file
                            .try_clone()
                            .context("clone v8 WAL handle for extent preparation")?,
                        &self.sync_file,
                        start_offset,
                        self.max_file_size,
                        effective_config.extent_window_bytes,
                        Some(Arc::clone(owner)),
                        strategy,
                        extent_policy_storage,
                        extent_policy_scan_diagnostics_enabled,
                    )?,
                ))
            };

            match effective_config.extent_policy {
                WalV8ExtentPreparationPolicy::Disabled => (None, None),
                WalV8ExtentPreparationPolicy::FallocateOnly
                | WalV8ExtentPreparationPolicy::Lookahead
                | WalV8ExtentPreparationPolicy::PointOnlyLookahead => {
                    let strategy = match effective_config.extent_policy {
                        WalV8ExtentPreparationPolicy::FallocateOnly => {
                            WalV8ExtentPreparationStrategy::FallocateOnly
                        }
                        WalV8ExtentPreparationPolicy::Lookahead => {
                            WalV8ExtentPreparationStrategy::Lookahead
                        }
                        WalV8ExtentPreparationPolicy::PointOnlyLookahead => {
                            WalV8ExtentPreparationStrategy::PointOnlyLookahead
                        }
                        _ => unreachable!(),
                    };
                    if let Some(owner) = &self.reservation_owner {
                        let preallocator = preallocate_with_strategy(strategy, owner)?;
                        #[cfg(feature = "bench")]
                        {
                            io_diagnostics.sliding_extent_preparation =
                                Some(preallocator.diagnostics());
                        }
                        (Some(preallocator), None)
                    } else {
                        #[cfg(feature = "bench")]
                        {
                            let preparation = Arc::new(WalV8SlidingExtentPreparation::new(
                                worker_file.try_clone().context(
                                    "clone v8 WAL handle for sliding extent preparation",
                                )?,
                                &self.sync_file,
                                start_offset,
                                self.max_file_size,
                                effective_config.extent_window_bytes,
                                WalExtentPreparationMode::SlidingFallocate,
                            )?);
                            io_diagnostics.sliding_extent_preparation =
                                Some(preparation.diagnostics());
                            (None, Some(preparation))
                        }
                        #[cfg(not(feature = "bench"))]
                        (None, None)
                    }
                }
                #[cfg(feature = "bench")]
                WalV8ExtentPreparationPolicy::SlidingInitialize => {
                    let preparation = Arc::new(WalV8SlidingExtentPreparation::new(
                        worker_file
                            .try_clone()
                            .context("clone v8 WAL handle for sliding extent preparation")?,
                        &self.sync_file,
                        start_offset,
                        self.max_file_size,
                        effective_config.extent_window_bytes,
                        WalExtentPreparationMode::SlidingInitialize,
                    )?);
                    io_diagnostics.sliding_extent_preparation = Some(preparation.diagnostics());
                    (None, Some(preparation))
                }
                #[cfg(feature = "bench")]
                WalV8ExtentPreparationPolicy::OneShotFallocate => {
                    io_diagnostics.extent_preparation = Some(prepare_v8_extent_suffix(
                        &worker_file,
                        &self.sync_file,
                        start_offset,
                        self.max_file_size,
                        WalExtentPreparationMode::Fallocate,
                        effective_config.extent_window_bytes,
                    )?);
                    (None, None)
                }
                #[cfg(feature = "bench")]
                WalV8ExtentPreparationPolicy::OneShotInitialize => {
                    io_diagnostics.extent_preparation = Some(prepare_v8_extent_suffix(
                        &worker_file,
                        &self.sync_file,
                        start_offset,
                        self.max_file_size,
                        WalExtentPreparationMode::Initialize,
                        effective_config.extent_window_bytes,
                    )?);
                    (None, None)
                }
            }
        };
        let frontier_file = match effective_config.frontier_mode {
            WalV8FrontierWriteMode::Buffered => self.write_file,
            WalV8FrontierWriteMode::Direct => worker_file
                .try_clone()
                .context("clone direct V8 WAL DATA handle for FRONTIER writes")?,
        };
        let frontier_metadata = frontier_file
            .metadata()
            .context("inspect v8 WAL FRONTIER handle")?;
        ensure!(
            frontier_metadata.dev() == self.installed_dev
                && frontier_metadata.ino() == self.installed_ino,
            "V8 WAL FRONTIER handle does not refer to the installed Active inode"
        );
        #[cfg(feature = "bench")]
        {
            io_diagnostics.frontier = WalIoHandleMetadata::from_file(&frontier_file)?;
        }
        let initial_ticket = self.coordinator.next_ticket();
        let io = WalV8ParallelIo::spawn(
            worker_file,
            self.coordinator.clone(),
            initial_ticket,
            extent_preallocator,
            WalV8ParallelIoConfig {
                #[cfg(feature = "bench")]
                extent_preparation: _extent_preparation,
                direct_completion_enabled: effective_config.direct_completion_enabled,
                coalesce_ready_target,
                #[cfg(feature = "bench")]
                data_submit_ready_order: effective_config.data_submit_ready_order,
                #[cfg(feature = "bench")]
                point_direct_frame_enabled: effective_config.point_direct_frame_enabled,
                #[cfg(test)]
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
            ticket_resources: Arc::new(Mutex::new(BTreeMap::new())),
            max_file_size: self.max_file_size,
            installed_dev: self.installed_dev,
            installed_ino: self.installed_ino,
            #[cfg(feature = "bench")]
            initial_ticket,
            #[cfg(feature = "bench")]
            io_diagnostics,
            point_buffer_preallocation_enabled,
            effective_config,
            #[cfg(test)]
            point_buffer_preallocation_test_override: AtomicBool::new(false),
            #[cfg(feature = "bench")]
            point_buffer_preallocation_attempts: AtomicU64::new(0),
            #[cfg(any(test, feature = "bench"))]
            point_buffer_preallocation_hits: AtomicU64::new(0),
            #[cfg(feature = "bench")]
            point_buffer_preallocation_fallbacks: AtomicU64::new(0),
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

    #[cfg(test)]
    pub(crate) fn uses_cursor_direct_completion(&self) -> bool {
        self.coordinator.progress_cursor_enabled()
            && self.io.direct_completions.is_some()
            && self.io.completion_events.is_none()
            && self.io.completion_thread.lock().is_none()
    }

    #[cfg(test)]
    pub(crate) fn uses_direct_frontier(&self) -> Result<bool> {
        is_o_direct_file(&self.frontier_file)
    }

    #[cfg(test)]
    pub(crate) fn effective_config(&self) -> WalV8EffectiveConfig {
        self.effective_config
    }

    #[cfg(feature = "bench")]
    pub(crate) fn io_diagnostics(&self) -> WalIoDiagnostics {
        let mut diagnostics = self.io_diagnostics;
        diagnostics.sliding_extent_preparation = self.io.extent_preparation_diagnostics();
        diagnostics.progress_ring_enabled = self.coordinator.progress_ring_enabled();
        diagnostics.progress_cursor_enabled = self.coordinator.progress_cursor_enabled();
        diagnostics.coalesce_ready_target = self.io.coalesce_ready_target.target_count();
        diagnostics.coalesce_ready_target_hits =
            self.io.coalesce_ready_target_hits.load(Ordering::Relaxed);
        diagnostics.coalesce_target_policy = self.io.coalesce_ready_target.policy_name();
        diagnostics.coalesce_target_eligible_cycles = self
            .io
            .coalesce_target_eligible_cycles
            .load(Ordering::Relaxed);
        diagnostics.coalesce_target_mixed_fallback_cycles = self
            .io
            .coalesce_target_mixed_fallback_cycles
            .load(Ordering::Relaxed);
        diagnostics.coalesce_target_initial_hits =
            self.io.coalesce_target_initial_hits.load(Ordering::Relaxed);
        diagnostics.coalesce_target_after_wake_hits = self
            .io
            .coalesce_target_after_wake_hits
            .load(Ordering::Relaxed);
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

    #[cfg(test)]
    pub(crate) fn set_point_buffer_preallocation_for_test(&self, enabled: bool) {
        self.point_buffer_preallocation_test_override
            .store(enabled, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn point_buffer_preallocation_enabled_for_test(&self) -> bool {
        self.should_preallocate_point_buffer()
    }

    #[cfg(test)]
    pub(crate) fn point_buffer_preallocations_in_flight_for_test(&self) -> usize {
        self.point_buffer_preallocations_in_flight
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn point_buffer_preallocation_hits_for_test(&self) -> u64 {
        self.point_buffer_preallocation_hits.load(Ordering::Relaxed)
    }

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
    pub(crate) fn try_preallocate_point_buffer(
        &self,
        scratch: &mut WalV8PointAdmissionScratch<'_>,
    ) -> bool {
        self.try_preallocate_point_buffer_only(scratch)
    }

    fn try_preallocate_point_buffer_only(
        &self,
        scratch: &mut WalV8PointAdmissionScratch<'_>,
    ) -> bool {
        if !self.should_preallocate_point_buffer() {
            return false;
        }
        #[cfg(feature = "bench")]
        self.point_buffer_preallocation_attempts
            .fetch_add(1, Ordering::Relaxed);

        if scratch.prepared.write_class() != WalV8WriteClass::Point
            || scratch.prepared.frame_buffer_bytes() != WAL_V8_FRAME_LEN
        {
            #[cfg(feature = "bench")]
            self.point_buffer_preallocation_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let Some(permit) = WalV8PointBufferPreallocationPermit::try_acquire(
            &self.point_buffer_preallocations_in_flight,
        ) else {
            #[cfg(feature = "bench")]
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
        #[cfg(any(test, feature = "bench"))]
        self.point_buffer_preallocation_hits
            .fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Admit a Point scratch object using ordered quota reservation and then
    /// consume its optional speculative DATA buffer.
    pub(crate) fn admit_prepared_point_batch<'a>(
        &self,
        scratch: WalV8PointAdmissionScratch<'a>,
        commit_ts: u64,
        recorded_at: crate::pitr::RecordedAt,
    ) -> Result<WalV8FrameJob<'a>> {
        ensure!(!self.is_poisoned(), "v8 Parallel WAL is poisoned");
        let WalV8PointAdmissionScratch {
            prepared,
            preallocated_buffer,
        } = scratch;
        let logical_bytes = u64::try_from(prepared.logical_batch_bytes())?;
        let frame_bytes = u64::try_from(prepared.frame_buffer_bytes())?;
        let resources = self.reserve_and_allocate_batch_resources(
            logical_bytes,
            frame_bytes,
            prepared.frame_buffer_bytes(),
            preallocated_buffer,
        )?;

        self.admit_prepared_batch_with_resources(
            prepared,
            commit_ts,
            recorded_at,
            resources,
            true,
            || {},
        )
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
        let resources = self.reserve_and_allocate_batch_resources(
            logical_bytes,
            frame_bytes,
            prepared.frame_buffer_bytes(),
            None,
        )?;
        self.admit_prepared_batch_with_resources(
            prepared,
            commit_ts,
            recorded_at,
            resources,
            false,
            after_admission,
        )
    }

    fn reserve_and_allocate_batch_resources(
        &self,
        logical_bytes: u64,
        frame_bytes: u64,
        buffer_bytes: usize,
        preallocated_buffer: Option<WalV8PointBufferPreallocation>,
    ) -> Result<WalV8AdmissionResources> {
        let reservation = self.reserve_batch_resources(logical_bytes, frame_bytes)?;
        let data_buffer = preallocated_buffer
            .map(WalV8PointBufferPreallocation::into_data_buffer)
            .unwrap_or_else(|| DirectBufAllocation::new(buffer_bytes));
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
        defer_frontier_resource_registration: bool,
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
        let reservation = if defer_frontier_resource_registration {
            resources.reservation.take()
        } else {
            if let Some(reservation) = resources.reservation.take() {
                let mut ticket_resources = self.ticket_resources.lock();
                match ticket_resources.entry(assignment.segment_ticket) {
                    std::collections::btree_map::Entry::Occupied(mut occupied) => {
                        let entry = occupied.get_mut();
                        if entry.reservation.is_some() {
                            self.coordinator.poison();
                            anyhow::bail!("v8 Parallel ticket already owns a resource reservation");
                        }
                        entry.reservation = Some(reservation);
                    }
                    std::collections::btree_map::Entry::Vacant(vacant) => {
                        vacant.insert(WalV8TicketResources {
                            reservation: Some(reservation),
                            guard: None,
                        });
                    }
                }
            }
            None
        };
        after_admission();

        Ok(WalV8FrameJob {
            admitted: Some(admitted),
            assignment,
            coordinator: self.coordinator.clone(),
            ticket_resources: Arc::clone(&self.ticket_resources),
            io: Arc::clone(&self.io),
            data_buffer: Some(resources.data_buffer),
            reservation,
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
        loop {
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
            #[cfg(feature = "bench")]
            let mut snapshot = if progress_ring_enabled {
                self.io
                    .advance_and_snapshot_after_wake(driver, diagnostics_enabled)?
            } else if diagnostics_enabled {
                self.coordinator.frontier_ready_snapshot_with_monotonic_ns()
            } else {
                self.coordinator.frontier_ready_snapshot()
            };
            #[cfg(not(feature = "bench"))]
            let mut snapshot = if progress_ring_enabled {
                self.io.advance_and_snapshot_after_wake(driver, false)?
            } else {
                self.coordinator.frontier_ready_snapshot()
            };
            if snapshot.poisoned {
                anyhow::bail!("v8 Parallel WAL is poisoned");
            }
            if snapshot.durable_ticket_end >= ticket_end {
                return Ok(());
            }
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
                if snapshot.poisoned {
                    anyhow::bail!("v8 Parallel WAL is poisoned");
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
                    self.io.wait_for_progress(Duration::from_secs(1))?;
                    if self.coordinator.is_poisoned() {
                        anyhow::bail!("v8 Parallel WAL was poisoned while waiting for durability");
                    }
                }
                continue;
            }

            let first_ticket = snapshot.durable_ticket_end;
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
            if frontier_ticket_end >= ticket_end {
                ensure!(
                    !self.coordinator.is_poisoned(),
                    "v8 Parallel WAL is poisoned"
                );
                return Ok(());
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn drain_ticket_prefix<V, P, F>(
        map: &mut BTreeMap<u64, V>,
        first_ticket: u64,
        ticket_end: u64,
        missing_msg: &'static str,
        disappeared_msg: &'static str,
        #[cfg(feature = "bench")] sample: bool,
        #[cfg(not(feature = "bench"))] _sample: bool,
        mut validate_entry: P,
        mut on_entry: F,
    ) -> Result<(usize, u64, u64, u64)>
    where
        P: FnMut(&V) -> Result<()>,
        F: FnMut(usize, u64, V) -> Result<()>,
    {
        let expected_u64 = ticket_end
            .checked_sub(first_ticket)
            .context("v8 ticket retirement range moves backwards")?;
        let expected = usize::try_from(expected_u64)?;
        if expected == 0 {
            return Ok((0, 0, 0, 0));
        }
        #[cfg(feature = "bench")]
        let collect_started = sample.then(digest::thread_cpu_time_ns);
        let first_key = map.first_key_value().map(|(&k, _)| k);
        let mut present = 0usize;
        for (_, value) in map.range(first_ticket..ticket_end) {
            validate_entry(value)?;
            present += 1;
        }
        #[cfg(feature = "bench")]
        let collect_cpu_ns = collect_started
            .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
            .unwrap_or_default();
        #[cfg(not(feature = "bench"))]
        let collect_cpu_ns = 0u64;
        ensure!(present == expected, "{missing_msg}");

        #[cfg(feature = "bench")]
        let (mut remove_cpu_ns, mut finish_cpu_ns) = (0u64, 0u64);
        #[cfg(not(feature = "bench"))]
        let (remove_cpu_ns, finish_cpu_ns) = (0u64, 0u64);

        let drain_from_front = first_key == Some(first_ticket);
        for index in 0..expected {
            let expected_ticket = first_ticket + u64::try_from(index)?;
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            let (ticket, value) = if drain_from_front {
                map.pop_first().context(disappeared_msg)?
            } else {
                let value = map.remove(&expected_ticket).context(disappeared_msg)?;
                (expected_ticket, value)
            };
            ensure!(ticket == expected_ticket, "{disappeared_msg}");
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
            on_entry(index, ticket, value)?;
            #[cfg(feature = "bench")]
            {
                finish_cpu_ns = finish_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
        }

        Ok((expected, collect_cpu_ns, remove_cpu_ns, finish_cpu_ns))
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
        let expected_u64 = ticket_end
            .checked_sub(first_ticket)
            .context("v8 ticket retirement range moves backwards")?;
        let expected = usize::try_from(expected_u64)?;
        if expected == 0 {
            return Ok(WalV8RetirementObservation::default());
        }
        #[cfg(feature = "bench")]
        let lock_wait_started = sample.then(Instant::now);
        #[cfg(feature = "bench")]
        let lock_wait_cpu_started = sample.then(digest::thread_cpu_time_ns);
        let mut resources = self.ticket_resources.lock();
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
        let mut present = 0usize;
        for (_, entry) in resources.range(first_ticket..ticket_end) {
            ensure!(
                entry.reservation.is_some() && entry.guard.is_some(),
                "incomplete v8 ticket resources in durable prefix"
            );
            present += 1;
        }
        #[cfg(feature = "bench")]
        let collect_cpu_ns = collect_started
            .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
            .unwrap_or_default();
        ensure!(
            present == expected,
            "v8 Parallel durable prefix is missing resource reservations"
        );
        #[cfg(feature = "bench")]
        let mut finish_cpu_ns = 0u64;
        #[cfg(feature = "bench")]
        let (mut fast_path_count, mut pinned_fallback_count) = (0u64, 0u64);
        for (index, (_ticket, entry)) in resources.range_mut(first_ticket..ticket_end).enumerate() {
            let reservation = entry
                .reservation
                .take()
                .context("v8 Parallel resource reservation disappeared")?;
            #[cfg(feature = "bench")]
            let step_started = sample.then(digest::thread_cpu_time_ns);
            let outcome = reservation
                .retire_durable(index > 0)
                .context("retire durable v8 batch resources")?;
            #[cfg(feature = "bench")]
            {
                match outcome {
                    crate::wal::v7::admission::WalV7DurableRetirementPath::ConsolidatedFastPath => {
                        fast_path_count += 1;
                    }
                    crate::wal::v7::admission::WalV7DurableRetirementPath::PinnedFallback => {
                        pinned_fallback_count += 1;
                    }
                }
                finish_cpu_ns = finish_cpu_ns.saturating_add(
                    step_started
                        .map(|t| digest::thread_cpu_time_ns().saturating_sub(t))
                        .unwrap_or_default(),
                );
            }
            #[cfg(not(feature = "bench"))]
            let _ = outcome;
        }
        #[cfg(not(feature = "bench"))]
        let _ = expected;
        #[cfg(feature = "bench")]
        let remove_cpu_ns = 0u64;
        #[cfg(feature = "bench")]
        if (fast_path_count > 0 || pinned_fallback_count > 0)
            && let Some(profile) = self.profile.load_full()
        {
            profile.record_wal_v8_retire_durable_counts(fast_path_count, pinned_fallback_count);
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
            observation.release_cpu_ns = 0;
            observation.finish_cpu_ns = finish_cpu_ns;
            observation.batch_point_frontier_credits = false;
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
        let mut guards = self.ticket_resources.lock();
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
        let (expected, collect_cpu_ns, remove_cpu_ns, finish_cpu_ns) = Self::drain_ticket_prefix(
            &mut guards,
            first_ticket,
            ticket_end,
            "v8 Parallel durable prefix is missing producer guards",
            "v8 Parallel producer guard disappeared",
            sample,
            |entry| {
                ensure!(
                    entry.reservation.is_none() && entry.guard.is_some(),
                    "v8 Parallel ticket resources are not ready for guard retirement"
                );
                Ok(())
            },
            |_index, ticket, entry| {
                let mut guard = entry
                    .guard
                    .context("v8 Parallel producer guard disappeared")?;
                ensure!(
                    guard.segment_ticket() == ticket
                        && (first_ticket..ticket_end).contains(&ticket),
                    "v8 Parallel producer guard ticket does not match its durable prefix"
                );
                guard.disarm_after_durable_prefix_check(&prefix_check);
                Ok(())
            },
        )?;
        #[cfg(not(feature = "bench"))]
        let _ = (expected, collect_cpu_ns, remove_cpu_ns, finish_cpu_ns);

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
        self.advance_frontier_through(ticket_end - 1)
    }

    pub(crate) fn wait_durable(&self, ticket: u64) -> Result<()> {
        self.advance_frontier_through(ticket)
    }

    #[cfg(feature = "bench")]
    pub(crate) fn take_durable_publish_sample_timestamp(&self, ticket: u64) -> Option<Instant> {
        self.coordinator
            .take_durable_publish_sample_timestamp(ticket)
    }

    pub(crate) fn next_ticket(&self) -> u64 {
        self.coordinator.next_ticket()
    }

    pub(crate) fn admitted_ticket_end(&self) -> u64 {
        self.coordinator.admitted_ticket_end()
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

    pub(crate) fn quiesce_extent_preparation(&self) {
        if let Some(preallocator) = &self.io.extent_preallocator {
            preallocator.quiesce();
        }
        #[cfg(feature = "bench")]
        if let Some(preparation) = &self.io.extent_preparation {
            preparation.quiesce();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_lookahead_worker_hook_for_test(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        if let Some(preallocator) = &self.io.extent_preallocator {
            preallocator.set_worker_hook_for_test(hook);
        }
    }

    #[cfg(test)]
    pub(crate) fn set_lookahead_quiesce_hook_for_test(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        if let Some(preallocator) = &self.io.extent_preallocator {
            preallocator.set_quiesce_hook_for_test(hook);
        }
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

    #[test]
    fn point_only_lookahead_respects_extent_window_boundaries_for_mixed_writes() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 1024 * 1024;
        const WINDOW_BYTES: u64 = WAL_V8_EXTENT_PREPARATION_CHUNK_BYTES as u64;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("point-only-mixed-boundary-v8.wal");
        let fixture = point_only_extent_preallocator_fixture(&path, SEGMENT_LIMIT, WINDOW_BYTES)?;
        let window0_start = fixture.start_offset;
        let window1_start = window0_start + WINDOW_BYTES;

        // Window 0 receives a single-frame Point write -> Lookahead (initialized).
        fixture.preallocator.register_data_range(
            window0_start,
            window0_start + FRAME_LEN_U64,
            WalV8WriteClass::Point,
            FRAME_LEN_U64,
        )?;
        // Window 1 receives a Batch write -> FallocateOnly (not initialized).
        fixture.preallocator.register_data_range(
            window1_start,
            window1_start + FRAME_LEN_U64,
            WalV8WriteClass::Batch,
            FRAME_LEN_U64,
        )?;
        fixture
            .preallocator
            .ensure_prepared_through(window1_start + FRAME_LEN_U64)?;

        let state = fixture.preallocator.state.lock();
        assert_eq!(state.prepared_end, window1_start + WINDOW_BYTES);
        assert_eq!(state.initialized_bytes, WINDOW_BYTES);
        assert_eq!(
            state.extent_policies.get(window0_start),
            Some(WalV8ExtentPolicy::Lookahead)
        );
        assert_eq!(
            state.extent_policies.get(window1_start),
            Some(WalV8ExtentPolicy::FallocateOnly)
        );
        Ok(())
    }

    #[test]
    fn point_only_lookahead_writer_admission_classifies_policies_and_recovers() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 1024 * 1024;
        const WINDOW_BYTES: u64 = FRAME_LEN_U64 * 4;

        let open_point_only_writer = |path: &Path| -> Result<
            Option<(WalV8ParallelWriter, Arc<WalV8ReservationOwner>, WalV8Header)>,
        > {
            let wal_header = header();
            let installed = install_empty(path, wal_header)?;
            let worker_file = match OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
            {
                Ok(file) => file,
                Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
                    eprintln!("skipping test (O_DIRECT unsupported on test fs): {error}");
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            };
            let sync_file = OpenOptions::new().read(true).write(true).open(path)?;
            let (leader, owner) = open_accounted_writer(
                path,
                &installed,
                SEGMENT_LIMIT,
                2 * SEGMENT_LIMIT,
                FRAME_LEN_U64,
                sync_file,
            )?;
            let writer = match leader.into_parallel_with_worker_file_progress_and_completion(
                worker_file,
                WalV8ProgressMode::Cursor,
                true,
                WalV8FrontierWriteMode::Direct,
                None,
                Some((
                    WalV8ExtentPreparationStrategy::PointOnlyLookahead,
                    WINDOW_BYTES,
                )),
            ) {
                Ok(writer) => writer,
                Err(error) if is_io_uring_unavailable_error(&error) => {
                    eprintln!("skipping test (io_uring unavailable): {error:#}");
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            Ok(Some((writer, owner, wal_header)))
        };

        // 1. Single-frame Point admission -> Lookahead + initialized_bytes > 0. Then Batch in the
        //    same window -> preserves Point first-owner (Lookahead). Then Batch in the next window
        //    -> FallocateOnly (not initialized).
        let dir1 = tempfile::tempdir()?;
        let path1 = dir1.path().join("writer-point-then-batch-v8.wal");
        let Some((writer1, owner1, header1)) = open_point_only_writer(&path1)? else {
            return Ok(());
        };
        let point1 = batch(10);
        writer1.append_prepared_batch(
            writer1.preflight_batch_with_write_class(
                &point1,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?,
            point1.commit_ts,
            point1.recorded_at,
        )?;
        let window0_start = HEADER_LEN_U64 + FRAME_LEN_U64;
        let window1_start = window0_start + WINDOW_BYTES;
        {
            let preallocator = writer1
                .io
                .extent_preallocator
                .as_ref()
                .expect("point-only preallocator installed");
            let state = preallocator.state.lock();
            assert_eq!(
                state.extent_policies.get(window0_start),
                Some(WalV8ExtentPolicy::Lookahead)
            );
            assert_eq!(state.initialized_bytes, WINDOW_BYTES);
        }

        // Batch in the same window keeps Lookahead first-owner.
        let batch_same_win = batch(20);
        writer1.append_prepared_batch(
            writer1.preflight_batch_with_write_class(
                &batch_same_win,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Batch,
            )?,
            batch_same_win.commit_ts,
            batch_same_win.recorded_at,
        )?;
        assert_eq!(
            writer1
                .io
                .extent_preallocator
                .as_ref()
                .unwrap()
                .state
                .lock()
                .extent_policies
                .get(window0_start),
            Some(WalV8ExtentPolicy::Lookahead)
        );

        // Advance into window 1 with a Batch write -> FallocateOnly, initialized_bytes stays
        // WINDOW_BYTES.
        let batch_next_win = multi_frame_batch(30);
        writer1.append_prepared_batch(
            writer1.preflight_batch_with_write_class(
                &batch_next_win,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Batch,
            )?,
            batch_next_win.commit_ts,
            batch_next_win.recorded_at,
        )?;
        {
            let preallocator = writer1.io.extent_preallocator.as_ref().unwrap();
            let state = preallocator.state.lock();
            assert_eq!(
                state.extent_policies.get(window1_start),
                Some(WalV8ExtentPolicy::FallocateOnly)
            );
            assert_eq!(state.initialized_bytes, WINDOW_BYTES);
        }
        let (_, boundary1) = writer1.active_boundaries()?;
        drop(writer1);
        let recovered1 = super::super::install::recover_and_install_active(
            &path1,
            &[boundary1],
            header1.predecessor,
            LIVE_WAL_V5_LIMITS,
            SEGMENT_LIMIT,
        )?;
        assert_eq!(
            replayed_batches(&path1, &recovered1)?,
            [point1, batch_same_win, batch_next_win]
        );
        let charge = owner1.image_owner().snapshot();
        assert_eq!(
            charge.logical_wal_bytes,
            recovered1.image_len - FIXED_IMAGE_TEST_BYTES
        );
        assert!(charge.source_spool_bytes >= recovered1.image_len);
        assert_eq!(charge.buffer_memory_bytes, 0);

        // 2. Single-frame Batch admission -> FallocateOnly, then Point in same window -> stays
        //    FallocateOnly.
        let dir2 = tempfile::tempdir()?;
        let path2 = dir2.path().join("writer-batch-then-point-v8.wal");
        let Some((writer2, _, _)) = open_point_only_writer(&path2)? else {
            return Ok(());
        };
        let batch_first = batch(40);
        writer2.append_prepared_batch(
            writer2.preflight_batch_with_write_class(
                &batch_first,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Batch,
            )?,
            batch_first.commit_ts,
            batch_first.recorded_at,
        )?;
        let point_second = batch(50);
        writer2.append_prepared_batch(
            writer2.preflight_batch_with_write_class(
                &point_second,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?,
            point_second.commit_ts,
            point_second.recorded_at,
        )?;
        {
            let preallocator = writer2.io.extent_preallocator.as_ref().unwrap();
            let state = preallocator.state.lock();
            assert_eq!(
                state.extent_policies.get(window0_start),
                Some(WalV8ExtentPolicy::FallocateOnly)
            );
            assert_eq!(state.initialized_bytes, 0);
        }
        drop(writer2);

        // 3. Multi-frame Point admission -> FallocateOnly + initialized_bytes == 0.
        let dir3 = tempfile::tempdir()?;
        let path3 = dir3.path().join("writer-multiframe-point-v8.wal");
        let Some((writer3, _, _)) = open_point_only_writer(&path3)? else {
            return Ok(());
        };
        let large_point = multi_frame_batch(60);
        writer3.append_prepared_batch(
            writer3.preflight_batch_with_write_class(
                &large_point,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?,
            large_point.commit_ts,
            large_point.recorded_at,
        )?;
        {
            let preallocator = writer3.io.extent_preallocator.as_ref().unwrap();
            let state = preallocator.state.lock();
            assert_eq!(
                state.extent_policies.get(window0_start),
                Some(WalV8ExtentPolicy::FallocateOnly)
            );
            assert_eq!(state.initialized_bytes, 0);
        }
        drop(writer3);

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
        let non_utf8 = std::os::unix::ffi::OsStrExt::from_bytes(b"\xff");
        assert!(WalV8FrontierWriteMode::from_value(Some(non_utf8)).is_err());
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
    fn coalesce_ready_target_accepts_only_point_experiment_values() {
        assert_eq!(
            coalesce_ready_target_from_value(None, WalV8ProgressMode::Cursor, true).unwrap(),
            None
        );
        for value in ["4", "8"] {
            assert!(
                coalesce_ready_target_from_value(
                    Some(OsStr::new(value)),
                    WalV8ProgressMode::Cursor,
                    true
                )
                .unwrap()
                .is_some()
            );
        }
        for value in ["0", "1", "2", "3", "5", "6", "12", "100", "400", "invalid"] {
            assert!(
                coalesce_ready_target_from_value(
                    Some(OsStr::new(value)),
                    WalV8ProgressMode::Cursor,
                    true
                )
                .is_err()
            );
        }
        assert!(
            coalesce_ready_target_from_value(Some(OsStr::new("8")), WalV8ProgressMode::Eager, true)
                .is_err()
        );
        assert!(
            coalesce_ready_target_from_value(
                Some(OsStr::new("4")),
                WalV8ProgressMode::Cursor,
                false
            )
            .is_err()
        );
    }

    #[cfg(all(feature = "bench", unix))]
    #[test]
    fn coalesce_ready_target_rejects_non_utf8_values() {
        use std::os::unix::ffi::OsStrExt;

        let invalid = OsStr::from_bytes(b"8\\xff");
        assert!(
            coalesce_ready_target_from_value(Some(invalid), WalV8ProgressMode::Cursor, true)
                .is_err()
        );
    }

    #[test]
    fn ready_target_one_uses_current_prefix_without_waiting_for_successors() {
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(0, Some(1)));
        assert!(WalV8ParallelIo::coalesce_ready_target_reached(1, Some(1)));
        assert!(WalV8ParallelIo::coalesce_ready_target_reached(2, Some(1)));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(0, Some(2)));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(1, Some(2)));
        assert!(WalV8ParallelIo::coalesce_ready_target_reached(2, Some(2)));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(0, Some(8)));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(1, Some(8)));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(1, Some(12)));
        assert!(WalV8ParallelIo::coalesce_ready_target_reached(8, Some(8)));
        assert!(WalV8ParallelIo::coalesce_ready_target_reached(12, Some(12)));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(
            11,
            Some(12)
        ));
        assert!(!WalV8ParallelIo::coalesce_ready_target_reached(16, None));
    }

    #[cfg(feature = "bench")]
    fn coalescing_test_snapshot(ready: usize, admitted: usize) -> WalV8FrontierReadySnapshot {
        WalV8FrontierReadySnapshot {
            ready_prefix: WalV8ReadyPrefix::Cursor {
                ticket_count: ready,
                tail: None,
                data_bytes: 0,
            },
            durable_ticket_end: 0,
            poisoned: false,
            admitted_ticket_count: admitted,
            has_outstanding_batch_tickets: false,
            closed: false,
            frames_ready_prefix_count: ready,
            data_complete_prefix_count: ready,
            reserved_preparation_bytes: 0,
            next_not_ready: None,
            data_hol_blocker: None,
            snapshot_monotonic_ns: None,
        }
    }

    #[cfg(feature = "bench")]
    fn set_coalescing_test_ready(snapshot: &mut WalV8FrontierReadySnapshot, ready: usize) {
        snapshot.ready_prefix = WalV8ReadyPrefix::Cursor {
            ticket_count: ready,
            tail: None,
            data_bytes: 0,
        };
        snapshot.frames_ready_prefix_count = ready;
        snapshot.data_complete_prefix_count = ready;
    }

    #[cfg(feature = "bench")]
    #[test]
    fn coalescing_target_skips_wait_and_keeps_the_full_ready_prefix() -> Result<()> {
        let mut at_target = coalescing_test_snapshot(8, 16);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut at_target,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::Always(8),
            |_, _| anyhow::bail!("coalescing should not wait after reaching the target"),
        )?;

        assert!(!outcome.waited);
        assert!(outcome.target_hit);
        assert_eq!(at_target.ready_prefix.len(), 8);

        let mut beyond_target = coalescing_test_snapshot(10, 16);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut beyond_target,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::Always(8),
            |_, _| anyhow::bail!("coalescing should preserve an already-ready prefix"),
        )?;

        assert!(!outcome.waited);
        assert!(outcome.target_hit);
        assert_eq!(beyond_target.ready_prefix.len(), 10);

        let mut complete_prefix = coalescing_test_snapshot(16, 16);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut complete_prefix,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::Always(8),
            |_, _| anyhow::bail!("coalescing should not wait when the full prefix is ready"),
        )?;

        assert!(!outcome.waited);
        assert!(!outcome.target_hit);
        assert_eq!(complete_prefix.ready_prefix.len(), 16);
        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn coalescing_target_stops_after_wait_without_truncating_ready_prefix() -> Result<()> {
        let mut snapshot = coalescing_test_snapshot(4, 16);
        let mut calls = 0;
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut snapshot,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::Always(8),
            |_, snapshot| {
                calls += 1;
                set_coalescing_test_ready(snapshot, 10);
                Ok(true)
            },
        )?;

        assert_eq!(calls, 1);
        assert!(outcome.waited);
        assert!(outcome.target_hit);
        assert_eq!(snapshot.ready_prefix.len(), 10);
        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn coalescing_below_target_keeps_the_original_deadline() -> Result<()> {
        let mut snapshot = coalescing_test_snapshot(4, 16);
        let budget = Duration::from_millis(100);
        let mut calls = 0;
        let mut second_remaining = None;
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut snapshot,
            budget,
            WalV8CoalesceTargetPolicy::Always(8),
            |remaining, snapshot| {
                calls += 1;
                if calls == 1 {
                    thread::sleep(Duration::from_millis(20));
                    set_coalescing_test_ready(snapshot, 6);
                    Ok(true)
                } else {
                    second_remaining = Some(remaining);
                    set_coalescing_test_ready(snapshot, 7);
                    Ok(false)
                }
            },
        )?;

        assert_eq!(calls, 2);
        assert!(
            second_remaining
                .is_some_and(|remaining| remaining < budget - Duration::from_millis(10))
        );
        assert!(outcome.waited);
        assert!(!outcome.target_hit);
        assert_eq!(snapshot.ready_prefix.len(), 7);
        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn coalescing_without_target_preserves_deadline_behavior() -> Result<()> {
        let mut snapshot = coalescing_test_snapshot(4, 16);
        let mut calls = 0;
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut snapshot,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::Disabled,
            |_, snapshot| {
                calls += 1;
                set_coalescing_test_ready(snapshot, 5);
                Ok(false)
            },
        )?;

        assert_eq!(calls, 1);
        assert!(outcome.waited);
        assert!(!outcome.target_hit);
        assert_eq!(snapshot.ready_prefix.len(), 5);
        Ok(())
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
            let requested_direct_frontier =
                std::env::var(CHILD_FRONTIER_MODE_ENV).as_deref() == Ok("direct");
            let requested_candidate = std::env::var_os("TOYKV_WAL_V8_PROFILE").as_deref()
                == Some(OsStr::new("candidate"));
            #[cfg(feature = "bench")]
            let writer = if requested_direct_frontier
                || requested_candidate
                || std::env::var_os(EXTENT_PREPARATION_ENV).is_some()
            {
                let worker_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(wal_path)?;
                if requested_candidate
                    || std::env::var_os(EXTENT_PREPARATION_ENV)
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
            let writer = if requested_candidate {
                let worker_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(wal_path)?;
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
            } else if requested_direct_frontier {
                let worker_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(wal_path)?;
                open_writer(wal_path, &installed)?.into_parallel_with_worker_file(worker_file)?
            } else {
                open_writer(wal_path, &installed)?.into_parallel()?
            };
            assert_eq!(
                writer.uses_direct_frontier()?,
                requested_direct_frontier,
                "child writer must use the requested FRONTIER mode"
            );
            #[cfg(feature = "bench")]
            if !requested_candidate {
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
            if requested_candidate {
                let effective = writer.effective_config();
                assert_eq!(effective.profile, WalV8Profile::Candidate);
                assert!(writer.uses_cursor_direct_completion());
                assert!(writer.point_buffer_preallocation_enabled_for_test());
                assert_eq!(
                    effective.coalesce_policy,
                    WalV8CoalesceTargetPolicy::PointOnly(8)
                );
            }
            let value = if requested_candidate {
                batch(10)
            } else {
                #[cfg(feature = "bench")]
                {
                    if std::env::var_os(EXTENT_PREPARATION_ENV).as_deref()
                        == Some(OsStr::new("sliding-initialize"))
                    {
                        multi_window_batch(10)
                    } else {
                        batch(10)
                    }
                }
                #[cfg(not(feature = "bench"))]
                {
                    batch(10)
                }
            };
            if requested_candidate {
                let effective = writer.effective_config();
                if effective.ext_family_filesystem && effective.o_direct_worker {
                    assert_eq!(
                        effective.extent_policy,
                        WalV8ExtentPreparationPolicy::PointOnlyLookahead
                    );
                }
                let prepared = writer.preflight_batch_with_write_class(
                    &value,
                    LIVE_WAL_V5_LIMITS,
                    WalV8WriteClass::Point,
                )?;
                let mut scratch = WalV8PointAdmissionScratch::new(prepared);
                assert!(writer.try_preallocate_point_buffer(&mut scratch));
                let job = writer.admit_prepared_point_batch(scratch, 10, value.recorded_at)?;
                let ticket = job.assignment().segment_ticket;
                let _ = job.finish()?;
                writer.wait_durable(ticket)?;
                if effective.ext_family_filesystem && effective.o_direct_worker {
                    assert!(
                        std::fs::metadata(wal_path)?.len()
                            >= installed.image_len + WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES,
                        "PointOnlyLookahead must extend the physical WAL image on eligible ext4/O_DIRECT filesystems"
                    );
                }
            } else {
                writer.append_prepared_batch(
                    writer.preflight_batch(&value, LIVE_WAL_V5_LIMITS)?,
                    10,
                    value.recorded_at,
                )?;
            }
            std::fs::write(ack_path, b"acknowledged")?;
            anyhow::bail!("configured V8 writer crash point did not stop the child")
        }

        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/wal-v8-tests");
        std::fs::create_dir_all(&parent)?;
        let frontier_modes = ["buffered", "direct"];
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
                    .map(|(point, min, max)| ("0", "legacy", point, min, max))
                    .to_vec();
                if cfg!(feature = "bench")
                    && matches!(
                        (frontier_mode, extent_preparation_mode),
                        ("buffered", "none") | ("direct", "sliding-point-lookahead")
                    )
                {
                    cases.extend(
                        CRASH_POINTS.map(|(point, min, max)| ("2", "legacy", point, min, max)),
                    );
                }
                if frontier_mode == "direct" && extent_preparation_mode == "none" {
                    cases.extend(
                        CRASH_POINTS.map(|(point, min, max)| ("2", "candidate", point, min, max)),
                    );
                }
                for (
                    progress_mode,
                    profile,
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
                    if frontier_mode == "direct"
                        && let Err(error) = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .custom_flags(libc::O_DIRECT)
                            .open(&wal_path)
                    {
                        if error.raw_os_error() == Some(libc::EINVAL) {
                            eprintln!(
                                "skipping {crash_point} in direct mode (O_DIRECT unsupported on test fs): {error}"
                            );
                            continue;
                        }
                        return Err(error.into());
                    }

                    let mut child = Command::new(std::env::current_exe()?);
                    child
                        .arg("--exact")
                        .arg(TEST_NAME)
                        .arg("--nocapture")
                        .env(CHILD_WAL_ENV, &wal_path)
                        .env(CHILD_ACK_ENV, &ack_path)
                        .env(CHILD_FRONTIER_MODE_ENV, frontier_mode)
                        .env(FRONTIER_MODE_ENV, frontier_mode)
                        .env("TOYKV_WAL_V8_PROFILE", profile)
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
        #[cfg(feature = "bench")]
        {
            let diagnostics = writer.io_diagnostics();
            assert_eq!(diagnostics.point_buffer_preallocation_attempts, 1);
            assert_eq!(diagnostics.point_buffer_preallocation_hits, 1);
            assert_eq!(diagnostics.point_buffer_preallocation_fallbacks, 0);
        }

        Ok(())
    }

    #[test]
    fn point_buffer_preallocation_does_not_consume_quota_before_admission() -> Result<()> {
        point_buffer_preallocation_preserves_ordered_quota_admission()
    }

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
        #[cfg(feature = "bench")]
        {
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
        }

        Ok(())
    }

    #[test]
    fn point_buffer_preallocation_rejects_non_point_and_multiframe_writes() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 128 * 1024;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("point-prealloc-eligibility-v8.wal");
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

        let single_point = batch(10);
        let single_batch = batch(20);
        let multi_point = multi_frame_batch(30);
        let multi_batch = multi_frame_batch(40);

        let mut single_point_scratch =
            WalV8PointAdmissionScratch::new(writer.preflight_batch_with_write_class(
                &single_point,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?);
        assert!(writer.try_preallocate_point_buffer(&mut single_point_scratch));
        assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 1);
        drop(single_point_scratch);
        assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 0);

        for (input, class) in [
            (&single_batch, WalV8WriteClass::Batch),
            (&multi_point, WalV8WriteClass::Point),
            (&multi_batch, WalV8WriteClass::Batch),
        ] {
            let mut scratch = WalV8PointAdmissionScratch::new(
                writer.preflight_batch_with_write_class(input, LIVE_WAL_V5_LIMITS, class)?,
            );
            assert!(!writer.try_preallocate_point_buffer(&mut scratch));
            assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 0);
        }

        assert_eq!(writer.point_buffer_preallocation_hits_for_test(), 1);
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

    fn open_parallel_with_direct_completion(
        path: &Path,
        installed: &WalV8InstalledRecovery,
        mode: WalV8ProgressMode,
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
            WalV8FrontierWriteMode::Buffered,
            worker_group_limit,
            None,
        )
    }

    fn cursor_completion_coordinator() -> Result<(WalV8ParallelCoordinator, Receiver<()>)> {
        let wal_header = header();
        let durable_prefix_digest = wal_header.initial_prefix_digest()?;
        let coordinator = WalV8ParallelCoordinator::new(
            wal_header,
            WalV8ParallelConfig {
                next_ticket: 0,
                next_data_offset: HEADER_LEN_U64 + FRAME_LEN_U64,
                last_commit_ts: 0,
                last_recorded_at: None,
                durable_prefix_digest,
                max_inflight_tickets: 1,
                max_inflight_bytes: FRAME_LEN_U64,
                max_file_size: MAX_WAL_FILE_SIZE,
                progress_mode: WalV8ProgressMode::Cursor,
            },
        )?;
        let control_tokens = coordinator
            .take_progress_token_receiver()?
            .context("Cursor coordinator has no control-token receiver")?;
        Ok((coordinator, control_tokens))
    }

    #[test]
    fn cursor_direct_completion_writes_reopens_and_recovers_without_reaper() -> Result<()> {
        check_cursor_direct_completion_writes_reopens_and_recovers()
    }

    #[test]
    fn cursor_direct_completion_token_wakes_driver_after_empty_scan() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-token-scan-to-park-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
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
            while Instant::now() < completion_deadline {
                let completion_queued = writer
                    .io
                    .direct_completions
                    .as_ref()
                    .is_some_and(|completions| !completions.is_empty());
                let progress_signaled = writer
                    .io
                    .progress_tokens
                    .as_ref()
                    .is_some_and(|tokens| !tokens.is_empty());
                if completion_queued && progress_signaled {
                    break;
                }
                thread::yield_now();
            }
            ensure!(
                writer
                    .io
                    .direct_completions
                    .as_ref()
                    .is_some_and(|completions| !completions.is_empty()),
                "worker did not enqueue DATA completion while the driver was paused"
            );
            ensure!(
                writer
                    .io
                    .progress_tokens
                    .as_ref()
                    .is_some_and(|tokens| !tokens.is_empty()),
                "direct DATA completion did not signal the progress token"
            );

            release_wait_tx.send(())?;
            driver_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("token-mode driver did not scan the queued completion")??;
            finish_result_rx
                .recv_timeout(Duration::from_secs(5))
                .context("Point waiter did not receive its durable ACK")??;
            driver
                .join()
                .map_err(|_| anyhow::anyhow!("token-mode durability driver panicked"))?;
            finish
                .join()
                .map_err(|_| anyhow::anyhow!("token-mode Point finisher panicked"))?;
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

    fn check_cursor_direct_completion_writes_reopens_and_recovers() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-direct-completion-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
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
        #[cfg(feature = "bench")]
        {
            assert!(writer.io_diagnostics().cursor_direct_completion_enabled);
            assert!(
                !writer
                    .io_diagnostics()
                    .cursor_direct_completion_select_wake_enabled
            );
        }

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

        let reopened = open_parallel_with_direct_completion(
            &path,
            &recovered,
            WalV8ProgressMode::Cursor,
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

    #[test]
    fn cursor_submission_preserves_groups_ranges_and_recovery() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-submission-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
            None,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let input_batches = [batch(10), multi_frame_batch(20), batch(30)];
        for input in &input_batches {
            writer.append_prepared_batch(
                writer.preflight_batch(input, LIVE_WAL_V5_LIMITS)?,
                input.commit_ts,
                input.recorded_at,
            )?;
        }
        let (_, boundary) = writer.active_boundaries()?;
        let snapshots = writer
            .io
            .submission
            .lock()
            .submitted_group_snapshots
            .clone();
        assert!(
            snapshots
                .iter()
                .any(|group| !group.frontier_offsets.is_empty())
        );
        assert!(
            snapshots
                .iter()
                .all(|group| group.tickets.end - group.tickets.start == 1)
        );
        assert!(snapshots.iter().any(|group| {
            group
                .data_ranges
                .iter()
                .any(|range| range.end - range.start > WAL_V8_FRAME_LEN as u64)
        }));
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, input_batches);
        Ok(())
    }

    #[test]
    fn cursor_submission_preserves_multi_ticket_groups_and_recovery() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cursor-multi-ticket-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
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
        let input_batches = [multi_frame_batch(10), batch(20), batch(30)];
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
        let snapshots = writer
            .io
            .submission
            .lock()
            .submitted_group_snapshots
            .clone();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].tickets, 0..3);
        assert_eq!(snapshots[0].data_ranges.len(), input_batches.len());
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, input_batches);
        Ok(())
    }

    #[cfg(feature = "chaos-testing")]
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

    fn run_coalescing_snapshot_refresh_case(
        filename: &str,
        budget: Duration,
        drop_token_before_wait: bool,
        expected_wait: WalV8ProgressTokenWait,
    ) -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join(filename);
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let writer = match open_parallel_with_direct_completion(
            &path,
            &installed,
            WalV8ProgressMode::Cursor,
            None,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let first = batch(10);
        let second = batch(20);
        let mut first_job = writer.admit_prepared_batch(
            writer.preflight_batch(&first, LIVE_WAL_V5_LIMITS)?,
            first.commit_ts,
            first.recorded_at,
        )?;
        let mut second_job = writer.admit_prepared_batch(
            writer.preflight_batch(&second, LIVE_WAL_V5_LIMITS)?,
            second.commit_ts,
            second.recorded_at,
        )?;
        let encoded_first = first_job
            .admitted
            .take()
            .expect("admitted batch exists")
            .encode()?;
        let first_digest = encoded_first.digest_ready()?;
        writer.coordinator.mark_digest_ready(first_digest)?;
        let encoded_second = second_job
            .admitted
            .take()
            .expect("admitted batch exists")
            .encode()?;
        let second_digest = encoded_second.digest_ready()?;
        writer.coordinator.mark_digest_ready(second_digest)?;
        writer.coordinator.mark_frames_ready(0)?;
        writer.coordinator.mark_frames_ready(1)?;
        writer.coordinator.mark_data_group_complete(0..1)?;

        let mut driver = writer
            .coordinator
            .acquire_durability_driver(1)?
            .expect("test acquires durability driver");
        let mut snapshot = writer.io.advance_and_snapshot(&mut driver, false)?;
        assert_eq!(snapshot.ready_prefix.len(), 1);
        assert_eq!(snapshot.admitted_ticket_count, 2);

        let wait_outcomes = Arc::new(Mutex::new(Vec::new()));
        let injected = Arc::new(AtomicBool::new(false));
        writer.io.set_progress_wait_hook(Arc::new({
            let coordinator = writer.coordinator.clone();
            let progress_tokens = writer.io.progress_tokens.clone();
            let wait_outcomes = Arc::clone(&wait_outcomes);
            let injected = Arc::clone(&injected);
            move |event| match event {
                WalV8ProgressWaitTestEvent::BeforeWait => {
                    if !injected.swap(true, Ordering::AcqRel) {
                        coordinator
                            .mark_data_group_complete(1..2)
                            .expect("mark ticket 1 complete for coalescing refresh test");
                        if drop_token_before_wait {
                            if let Some(tokens) = progress_tokens.as_ref() {
                                while tokens.try_recv().is_ok() {}
                            }
                        } else {
                            coordinator
                                .worker_completion_handle()
                                .notify_completion_queued()
                                .expect("signal progress token for coalescing refresh test");
                        }
                    }
                }
                WalV8ProgressWaitTestEvent::AfterWait(status) => {
                    wait_outcomes.lock().push(status);
                }
            }
        }));

        let waited = writer.io.coalesce_progress_ring(
            &mut driver,
            &mut snapshot,
            budget,
            false,
            #[cfg(feature = "bench")]
            None,
        )?;
        assert!(waited);
        assert_eq!(wait_outcomes.lock().as_slice(), &[expected_wait]);
        assert_eq!(snapshot.ready_prefix.len(), 2);

        drop(driver);
        drop(first_job);
        drop(second_job);
        Ok(())
    }

    #[test]
    fn coalescing_progress_ring_refreshes_snapshot_after_token_wake() -> Result<()> {
        run_coalescing_snapshot_refresh_case(
            "coalesce-token-wake-v8.wal",
            Duration::from_secs(5),
            false,
            WalV8ProgressTokenWait::Signaled,
        )
    }

    #[test]
    fn coalescing_progress_ring_refreshes_snapshot_on_timeout_final_scan() -> Result<()> {
        run_coalescing_snapshot_refresh_case(
            "coalesce-timeout-final-scan-v8.wal",
            Duration::from_millis(5),
            true,
            WalV8ProgressTokenWait::TimedOut,
        )
    }

    #[test]
    fn direct_frontier_uses_o_direct_data_descriptor_and_recovers() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("direct-frontier-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let worker_file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
                eprintln!("skipping test (O_DIRECT unsupported on test fs): {error}");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let writer = match open_writer(&path, &installed)?
            .into_parallel_with_worker_file_progress_and_completion(
                worker_file,
                WalV8ProgressMode::Cursor,
                true,
                WalV8FrontierWriteMode::Direct,
                None,
                None,
            ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        assert!(writer.uses_direct_frontier()?);

        let inputs = [batch(10), batch(20)];
        for input in &inputs {
            writer.append_prepared_batch(
                writer.preflight_batch(input, LIVE_WAL_V5_LIMITS)?,
                input.commit_ts,
                input.recorded_at,
            )?;
        }
        let (_, boundary) = writer.active_boundaries()?;
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, inputs);
        Ok(())
    }

    #[test]
    fn direct_frontier_falls_back_to_buffered_when_data_worker_is_buffered() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("direct-frontier-fallback-v8.wal");
        let wal_header = header();
        let installed = install_empty(&path, wal_header)?;
        let worker_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let writer = match open_writer(&path, &installed)?
            .into_parallel_with_worker_file_progress_and_completion(
                worker_file,
                WalV8ProgressMode::Eager,
                false,
                WalV8FrontierWriteMode::Direct,
                None,
                None,
            ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        assert!(!writer.uses_direct_frontier()?);

        let input = batch(30);
        writer.append_prepared_batch(
            writer.preflight_batch(&input, LIVE_WAL_V5_LIMITS)?,
            input.commit_ts,
            input.recorded_at,
        )?;
        let (_, boundary) = writer.active_boundaries()?;
        drop(writer);

        let recovered = super::super::install::recover_and_install_active(
            &path,
            &[boundary],
            wal_header.predecessor,
            LIVE_WAL_V5_LIMITS,
            MAX_WAL_FILE_SIZE,
        )?;
        assert_eq!(replayed_batches(&path, &recovered)?, [input]);
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

    #[test]
    fn wal_v8_extent_lookahead_from_value_rejects_non_utf8() {
        use std::os::unix::ffi::OsStrExt;

        assert!(!wal_v8_extent_lookahead_from_value(None).unwrap());
        assert!(!wal_v8_extent_lookahead_from_value(Some(OsStr::new("0"))).unwrap());
        assert!(!wal_v8_extent_lookahead_from_value(Some(OsStr::new("false"))).unwrap());
        assert!(wal_v8_extent_lookahead_from_value(Some(OsStr::new("1"))).unwrap());
        assert!(wal_v8_extent_lookahead_from_value(Some(OsStr::new("true"))).unwrap());
        assert!(wal_v8_extent_lookahead_from_value(Some(OsStr::new("yes"))).is_err());

        let non_utf8 = OsStr::from_bytes(b"\xff\xfe");
        let err = wal_v8_extent_lookahead_from_value(Some(non_utf8))
            .expect_err("non-UTF-8 extent lookahead env value must fail");
        assert!(err.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn wal_v8_requested_config_defaults_to_candidate_and_preserves_legacy() {
        let defaults = WalV8RequestedConfig::default();
        assert_eq!(defaults, WalV8RequestedConfig::candidate());
        assert_eq!(defaults.profile, WalV8Profile::Candidate);
        assert_eq!(defaults.progress_mode, WalV8ProgressMode::Cursor);
        assert!(defaults.direct_completion_enabled);
        assert_eq!(defaults.frontier_mode, WalV8FrontierWriteMode::Direct);
        assert!(!defaults.extent_lookahead);
        assert!(defaults.point_buffer_preallocation_enabled);
        assert_eq!(
            defaults.coalesce_policy,
            WalV8CoalesceTargetPolicy::PointOnly(8)
        );

        let legacy = WalV8RequestedConfig::legacy();
        assert_eq!(legacy.profile, WalV8Profile::Legacy);
        assert_eq!(legacy.progress_mode, WalV8ProgressMode::Eager);
        assert!(!legacy.direct_completion_enabled);
        assert_eq!(legacy.frontier_mode, WalV8FrontierWriteMode::Buffered);
        assert!(!legacy.extent_lookahead);
        assert!(!legacy.point_buffer_preallocation_enabled);
        assert_eq!(legacy.coalesce_policy, WalV8CoalesceTargetPolicy::Disabled);
    }

    #[test]
    fn wal_v8_effective_config_resolves_frontier_fallback_and_owner() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("effective-config.wal");
        let buffered_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let requested = WalV8RequestedConfig {
            progress_mode: WalV8ProgressMode::Cursor,
            direct_completion_enabled: true,
            frontier_mode: WalV8FrontierWriteMode::Direct,
            extent_lookahead: false,
            ..WalV8RequestedConfig::legacy()
        };
        let buffered_effective = WalV8EffectiveConfig::resolve(&requested, &buffered_file, false)?;
        assert_eq!(buffered_effective.progress_mode, WalV8ProgressMode::Cursor);
        assert!(buffered_effective.direct_completion_enabled);
        assert!(!buffered_effective.o_direct_worker);
        assert_eq!(
            buffered_effective.frontier_mode,
            WalV8FrontierWriteMode::Buffered
        );
        assert_eq!(
            buffered_effective.extent_policy,
            WalV8ExtentPreparationPolicy::Disabled
        );
        assert_eq!(buffered_effective.extent_window_bytes, 0);
        assert!(!buffered_effective.has_reservation_owner);

        if let Ok(direct_file) = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
        {
            let direct_effective = WalV8EffectiveConfig::resolve(&requested, &direct_file, true)?;
            assert!(direct_effective.o_direct_worker);
            assert_eq!(
                direct_effective.frontier_mode,
                WalV8FrontierWriteMode::Direct
            );
            assert!(direct_effective.has_reservation_owner);
            let expected_policy =
                if cfg!(not(feature = "bench")) && direct_effective.ext_family_filesystem {
                    WalV8ExtentPreparationPolicy::FallocateOnly
                } else {
                    WalV8ExtentPreparationPolicy::Disabled
                };
            assert_eq!(direct_effective.extent_policy, expected_policy);
        }

        let lookahead_requested = WalV8RequestedConfig {
            extent_lookahead: true,
            ..WalV8RequestedConfig::default()
        };
        assert!(
            WalV8EffectiveConfig::resolve(&lookahead_requested, &buffered_file, false).is_err()
        );

        Ok(())
    }

    #[test]
    fn point_only_target8_stops_only_when_no_batch_tickets_are_outstanding() -> Result<()> {
        use crate::wal::v8::parallel::WalV8ReadyPrefix;

        let make_prefix = |ticket_count: usize| WalV8ReadyPrefix::Cursor {
            ticket_count,
            tail: None,
            #[cfg(feature = "bench")]
            data_bytes: 0,
        };
        let make_snapshot = |ready: usize, has_batch: bool| WalV8FrontierReadySnapshot {
            ready_prefix: make_prefix(ready),
            durable_ticket_end: 0,
            poisoned: false,
            admitted_ticket_count: 16,
            has_outstanding_batch_tickets: has_batch,
            closed: false,
            #[cfg(feature = "bench")]
            frames_ready_prefix_count: ready,
            #[cfg(feature = "bench")]
            data_complete_prefix_count: ready,
            #[cfg(feature = "bench")]
            reserved_preparation_bytes: 0,
            #[cfg(feature = "bench")]
            next_not_ready: None,
            #[cfg(feature = "bench")]
            data_hol_blocker: None,
            #[cfg(feature = "bench")]
            snapshot_monotonic_ns: None,
        };

        let mut point_only_at_8 = make_snapshot(8, false);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut point_only_at_8,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::PointOnly(8),
            |_, _| anyhow::bail!("PointOnly(8) should stop without waiting at 8 ready Points"),
        )?;
        assert!(!outcome.waited);

        // Scenario B: 8 Point tickets are ready, but an unready Batch ticket is
        // outstanding in the admitted window -> must keep Dynamic coalescing.
        let mut mixed_at_8 = make_snapshot(8, true);
        let mut wait_calls = 0;
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut mixed_at_8,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::PointOnly(8),
            |_, snapshot| {
                wait_calls += 1;
                snapshot.ready_prefix = make_prefix(9);
                Ok(false)
            },
        )?;
        assert!(outcome.waited);
        assert_eq!(wait_calls, 1);
        assert_eq!(mixed_at_8.ready_prefix.len(), 9);

        // Transition mid-wait: starts Point-only below target (4 ready), then a
        // Batch ticket is admitted before reaching 8 -> continues Dynamic wait.
        let mut transition = make_snapshot(4, false);
        let mut transition_calls = 0;
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut transition,
            Duration::from_millis(100),
            WalV8CoalesceTargetPolicy::PointOnly(8),
            |_, snapshot| {
                transition_calls += 1;
                if transition_calls == 1 {
                    snapshot.has_outstanding_batch_tickets = true;
                    snapshot.ready_prefix = make_prefix(8);
                    Ok(true)
                } else {
                    snapshot.ready_prefix = make_prefix(10);
                    Ok(false)
                }
            },
        )?;
        assert!(outcome.waited);
        assert_eq!(transition_calls, 2);
        assert_eq!(transition.ready_prefix.len(), 10);

        Ok(())
    }

    #[test]
    fn coordinator_tracks_outstanding_batch_tickets_across_admission_and_retirement() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("outstanding-batch.wal");
        let header = header();
        let installed = install_empty(&path, header)?;
        let writer = open_writer(&path, &installed)?.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        )?;

        // 1. Admit a Point ticket -> no outstanding Batch tickets.
        let point_batch = batch(10);
        let point_prepared = writer.preflight_batch_with_write_class(
            &point_batch,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut point_scratch = WalV8PointAdmissionScratch::new(point_prepared);
        assert!(writer.try_preallocate_point_buffer(&mut point_scratch));
        let point_job = writer.admit_prepared_point_batch(
            point_scratch,
            point_batch.commit_ts,
            point_batch.recorded_at,
        )?;
        assert!(
            !writer
                .coordinator
                .frontier_ready_snapshot()
                .has_outstanding_batch_tickets
        );

        // 2. Admit an unready Batch ticket -> outstanding Batch flag becomes true.
        let batch_write = batch(20);
        let batch_prepared = writer.preflight_batch_with_write_class(
            &batch_write,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Batch,
        )?;
        let batch_job = writer.admit_prepared_batch(
            batch_prepared,
            batch_write.commit_ts,
            batch_write.recorded_at,
        )?;
        assert!(
            writer
                .coordinator
                .frontier_ready_snapshot()
                .has_outstanding_batch_tickets
        );

        // 3. Complete and sync both tickets -> outstanding Batch flag clears.
        let _ = point_job.finish()?;
        let _ = batch_job.finish()?;
        writer.sync()?;
        assert!(
            !writer
                .coordinator
                .frontier_ready_snapshot()
                .has_outstanding_batch_tickets
        );

        Ok(())
    }

    #[test]
    fn candidate_profile_resolves_all_components_and_preserves_legacy_defaults() -> Result<()> {
        use std::os::unix::ffi::OsStrExt;

        assert_eq!(WalV8Profile::from_value(None)?, WalV8Profile::Candidate);
        assert_eq!(
            WalV8Profile::from_value(Some(OsStr::new("legacy")))?,
            WalV8Profile::Legacy
        );
        assert_eq!(
            WalV8Profile::from_value(Some(OsStr::new("candidate")))?,
            WalV8Profile::Candidate
        );
        assert!(WalV8Profile::from_value(Some(OsStr::new("unknown"))).is_err());
        assert!(WalV8Profile::from_value(Some(OsStr::from_bytes(b"\xff"))).is_err());

        let legacy = WalV8RequestedConfig::legacy();
        assert_eq!(legacy.profile, WalV8Profile::Legacy);
        assert_eq!(legacy.progress_mode, WalV8ProgressMode::Eager);
        assert!(!legacy.direct_completion_enabled);
        assert_eq!(legacy.frontier_mode, WalV8FrontierWriteMode::Buffered);
        assert!(!legacy.point_buffer_preallocation_enabled);
        assert_eq!(legacy.coalesce_policy, WalV8CoalesceTargetPolicy::Disabled);

        let candidate = WalV8RequestedConfig::candidate();
        assert_eq!(candidate.profile, WalV8Profile::Candidate);
        assert_eq!(candidate.progress_mode, WalV8ProgressMode::Cursor);
        assert!(candidate.direct_completion_enabled);
        assert_eq!(candidate.frontier_mode, WalV8FrontierWriteMode::Direct);
        assert!(candidate.point_buffer_preallocation_enabled);
        assert_eq!(
            candidate.coalesce_policy,
            WalV8CoalesceTargetPolicy::PointOnly(8)
        );

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("candidate-profile.wal");
        let buffered_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let buffered_effective = WalV8EffectiveConfig::resolve(&candidate, &buffered_file, true)?;
        assert_eq!(
            buffered_effective.frontier_mode,
            WalV8FrontierWriteMode::Buffered
        );
        assert_eq!(
            buffered_effective.extent_policy,
            WalV8ExtentPreparationPolicy::Disabled
        );
        assert_eq!(
            buffered_effective.coalesce_policy,
            WalV8CoalesceTargetPolicy::PointOnly(8)
        );
        assert!(buffered_effective.point_buffer_preallocation_enabled);

        if let Ok(direct_file) = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
        {
            let direct_effective = WalV8EffectiveConfig::resolve(&candidate, &direct_file, true)?;
            assert_eq!(
                direct_effective.frontier_mode,
                WalV8FrontierWriteMode::Direct
            );
            if direct_effective.ext_family_filesystem {
                assert_eq!(
                    direct_effective.extent_policy,
                    WalV8ExtentPreparationPolicy::PointOnlyLookahead
                );
                assert_eq!(
                    direct_effective.extent_window_bytes,
                    WAL_V8_PRODUCTION_EXTENT_WINDOW_BYTES
                );
            }
        }

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn point_direct_frame_from_value_rejects_invalid_values() {
        use std::os::unix::ffi::OsStrExt;

        assert!(!point_direct_frame_from_value(None).unwrap());
        assert!(!point_direct_frame_from_value(Some(OsStr::new("0"))).unwrap());
        assert!(point_direct_frame_from_value(Some(OsStr::new("1"))).unwrap());
        assert!(point_direct_frame_from_value(Some(OsStr::new("true"))).is_err());
        assert!(point_direct_frame_from_value(Some(OsStr::new("2"))).is_err());
        assert!(point_direct_frame_from_value(Some(OsStr::from_bytes(b"\xff"))).is_err());
    }

    #[test]
    fn coalescing_override_parses_dynamic_and_point_target8() -> Result<()> {
        use std::os::unix::ffi::OsStrExt;

        assert_eq!(
            WalV8CoalesceTargetPolicy::from_value(None, WalV8ProgressMode::Eager, false)?,
            None
        );
        assert_eq!(
            WalV8CoalesceTargetPolicy::from_value(
                Some(OsStr::new("dynamic")),
                WalV8ProgressMode::Eager,
                false,
            )?,
            Some(WalV8CoalesceTargetPolicy::Disabled)
        );
        assert_eq!(
            WalV8CoalesceTargetPolicy::from_value(
                Some(OsStr::new("dynamic")),
                WalV8ProgressMode::Cursor,
                true,
            )?,
            Some(WalV8CoalesceTargetPolicy::Disabled)
        );
        assert_eq!(
            WalV8CoalesceTargetPolicy::from_value(
                Some(OsStr::new("point-target8")),
                WalV8ProgressMode::Cursor,
                true,
            )?,
            Some(WalV8CoalesceTargetPolicy::PointOnly(8))
        );
        assert!(
            WalV8CoalesceTargetPolicy::from_value(
                Some(OsStr::new("point-target8")),
                WalV8ProgressMode::Eager,
                false,
            )
            .is_err()
        );
        assert!(
            WalV8CoalesceTargetPolicy::from_value(
                Some(OsStr::new("always-8")),
                WalV8ProgressMode::Cursor,
                true,
            )
            .is_err()
        );
        assert!(
            WalV8CoalesceTargetPolicy::from_value(
                Some(OsStr::from_bytes(b"\xff")),
                WalV8ProgressMode::Cursor,
                true,
            )
            .is_err()
        );

        Ok(())
    }

    #[test]
    fn v8_candidate_mixed_target8_falls_back_to_dynamic_on_real_admission() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut next_ts = 10u64;

        let open_warmed_writer =
            |name: &str, ts: &mut u64| -> Result<Option<WalV8ParallelWriter>> {
                let path = dir.path().join(name);
                let installed = install_empty(&path, header())?;
                let writer = match open_writer(&path, &installed)?
                    .into_parallel_with_worker_file_and_config(
                        OpenOptions::new().read(true).write(true).open(&path)?,
                        WalV8RequestedConfig::candidate(),
                    ) {
                    Ok(writer) => writer,
                    Err(error) if is_io_uring_unavailable_error(&error) => {
                        eprintln!("skipping test (io_uring unavailable): {error:#}");
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                };
                let warmup = batch(*ts);
                *ts += 10;
                let warmup_prepared = writer.preflight_batch_with_write_class(
                    &warmup,
                    LIVE_WAL_V5_LIMITS,
                    WalV8WriteClass::Point,
                )?;
                let mut warmup_scratch = WalV8PointAdmissionScratch::new(warmup_prepared);
                assert!(writer.try_preallocate_point_buffer(&mut warmup_scratch));
                let warmup_job = writer.admit_prepared_point_batch(
                    warmup_scratch,
                    warmup.commit_ts,
                    warmup.recorded_at,
                )?;
                let _ = warmup_job.finish()?;
                writer.sync()?;
                Ok(Some(writer))
            };

        let wait_until_ready = |writer: &WalV8ParallelWriter,
                                driver: &mut WalV8DurabilityDriver,
                                expected_ready: usize,
                                expected_admitted: usize|
         -> Result<WalV8FrontierReadySnapshot> {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let snapshot = writer.io.advance_and_snapshot(driver, false)?;
                if snapshot.ready_prefix.len() == expected_ready
                    && snapshot.admitted_ticket_count == expected_admitted
                {
                    return Ok(snapshot);
                }
                ensure!(
                    Instant::now() < deadline,
                    "timed out waiting for ready_prefix={expected_ready}, admitted={expected_admitted} (got ready={}, admitted={})",
                    snapshot.ready_prefix.len(),
                    snapshot.admitted_ticket_count
                );
                thread::sleep(Duration::from_millis(2));
            }
        };

        // Case 1: 8 ready Points + 1 pending Point -> Target8 stops without waiting.
        let Some(writer1) = open_warmed_writer("case1.wal", &mut next_ts)? else {
            return Ok(());
        };
        let mut case1_batches = Vec::new();
        for _ in 0..9 {
            case1_batches.push(batch(next_ts));
            next_ts += 10;
        }
        let mut case1_jobs = Vec::new();
        for b in &case1_batches {
            let prepared = writer1.preflight_batch_with_write_class(
                b,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?;
            let mut scratch = WalV8PointAdmissionScratch::new(prepared);
            assert!(writer1.try_preallocate_point_buffer(&mut scratch));
            case1_jobs.push(writer1.admit_prepared_point_batch(
                scratch,
                b.commit_ts,
                b.recorded_at,
            )?);
        }
        let case1_pending = case1_jobs.pop().unwrap();
        for job in case1_jobs {
            let _ = job.finish()?;
        }
        let mut driver1 = writer1
            .coordinator
            .acquire_durability_driver(writer1.next_ticket())?
            .context("acquire driver for Case 1")?;
        let mut snapshot1 = wait_until_ready(&writer1, &mut driver1, 8, 9)?;
        assert!(!snapshot1.has_outstanding_batch_tickets);
        let waited1 = writer1.io.coalesce_progress_ring(
            &mut driver1,
            &mut snapshot1,
            Duration::from_millis(50),
            false,
            #[cfg(feature = "bench")]
            None,
        )?;
        assert!(
            !waited1,
            "Point-only window at 8 ready Points must stop without coalescing wait"
        );
        drop(driver1);
        drop(case1_pending);
        drop(writer1);

        // Case 2: 8 ready Points + 1 pending Batch -> Target8 suppressed, Dynamic waits.
        let writer2 = open_warmed_writer("case2.wal", &mut next_ts)?.unwrap();
        let mut case2_point_batches = Vec::new();
        for _ in 0..8 {
            case2_point_batches.push(batch(next_ts));
            next_ts += 10;
        }
        let mut case2_jobs = Vec::new();
        for b in &case2_point_batches {
            let prepared = writer2.preflight_batch_with_write_class(
                b,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?;
            let mut scratch = WalV8PointAdmissionScratch::new(prepared);
            assert!(writer2.try_preallocate_point_buffer(&mut scratch));
            case2_jobs.push(writer2.admit_prepared_point_batch(
                scratch,
                b.commit_ts,
                b.recorded_at,
            )?);
        }
        let case2_batch = batch(next_ts);
        next_ts += 10;
        let case2_pending_batch = writer2.admit_prepared_batch(
            writer2.preflight_batch_with_write_class(
                &case2_batch,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Batch,
            )?,
            case2_batch.commit_ts,
            case2_batch.recorded_at,
        )?;
        for job in case2_jobs {
            let _ = job.finish()?;
        }
        let mut driver2 = writer2
            .coordinator
            .acquire_durability_driver(writer2.next_ticket())?
            .context("acquire driver for Case 2")?;
        let mut snapshot2 = wait_until_ready(&writer2, &mut driver2, 8, 9)?;
        assert_eq!(snapshot2.ready_prefix.len(), 8);
        assert_eq!(snapshot2.admitted_ticket_count, 9);
        assert!(snapshot2.has_outstanding_batch_tickets);

        let parked = Arc::new(AtomicBool::new(false));
        writer2.io.set_progress_wait_hook(Arc::new({
            let parked = Arc::clone(&parked);
            move |event| {
                if matches!(event, WalV8ProgressWaitTestEvent::BeforeWait) {
                    parked.store(true, Ordering::Release);
                }
            }
        }));
        let waited2 = writer2.io.coalesce_progress_ring(
            &mut driver2,
            &mut snapshot2,
            Duration::from_millis(30),
            false,
            #[cfg(feature = "bench")]
            None,
        )?;
        assert!(waited2, "mixed window must enter Dynamic coalescing wait");
        assert!(parked.load(Ordering::Acquire));
        drop(driver2);
        drop(case2_pending_batch);
        drop(writer2);

        // Case 3: 10 ready Points + 1 pending Point -> preserves full 10-ticket prefix.
        let writer3 = open_warmed_writer("case3.wal", &mut next_ts)?.unwrap();
        let mut case3_batches = Vec::new();
        for _ in 0..11 {
            case3_batches.push(batch(next_ts));
            next_ts += 10;
        }
        let mut case3_jobs = Vec::new();
        for b in &case3_batches {
            let prepared = writer3.preflight_batch_with_write_class(
                b,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?;
            let mut scratch = WalV8PointAdmissionScratch::new(prepared);
            assert!(writer3.try_preallocate_point_buffer(&mut scratch));
            case3_jobs.push(writer3.admit_prepared_point_batch(
                scratch,
                b.commit_ts,
                b.recorded_at,
            )?);
        }
        let case3_pending = case3_jobs.pop().unwrap();
        for job in case3_jobs {
            let _ = job.finish()?;
        }
        let mut driver3 = writer3
            .coordinator
            .acquire_durability_driver(writer3.next_ticket())?
            .context("acquire driver for Case 3")?;
        let mut snapshot3 = wait_until_ready(&writer3, &mut driver3, 10, 11)?;
        assert!(!snapshot3.has_outstanding_batch_tickets);
        let waited3 = writer3.io.coalesce_progress_ring(
            &mut driver3,
            &mut snapshot3,
            Duration::from_millis(50),
            false,
            #[cfg(feature = "bench")]
            None,
        )?;
        assert!(!waited3);
        assert_eq!(snapshot3.ready_prefix.len(), 10);
        // Drive FRONTIER through ticket 11 (1 warmup + 10 ready Points) and
        // verify all 10 ready Points are committed without truncation at 8.
        writer3.drive_frontier_through(11, &mut driver3)?;
        assert_eq!(writer3.active_boundaries()?.1.ticket_end, 11);
        drop(driver3);
        drop(case3_pending);
        drop(writer3);

        // Case 4: Starts Point-only below target (4 ready Points), then a real
        // Batch ticket is admitted during the wait while 4 more Points finish
        // (reaching 8 ready Points) -> Target8 is cancelled and falls back to Dynamic.
        let writer4 = Arc::new(open_warmed_writer("case4.wal", &mut next_ts)?.unwrap());
        let mut case4_initial_batches = Vec::new();
        for _ in 0..5 {
            case4_initial_batches.push(batch(next_ts));
            next_ts += 10;
        }
        let mut case4_jobs = Vec::new();
        for b in &case4_initial_batches {
            let prepared = writer4.preflight_batch_with_write_class(
                b,
                LIVE_WAL_V5_LIMITS,
                WalV8WriteClass::Point,
            )?;
            let mut scratch = WalV8PointAdmissionScratch::new(prepared);
            assert!(writer4.try_preallocate_point_buffer(&mut scratch));
            case4_jobs.push(writer4.admit_prepared_point_batch(
                scratch,
                b.commit_ts,
                b.recorded_at,
            )?);
        }
        let case4_fifth = case4_jobs.pop().unwrap();
        for job in case4_jobs {
            let _ = job.finish()?;
        }
        let mut driver4 = writer4
            .coordinator
            .acquire_durability_driver(writer4.next_ticket())?
            .context("acquire driver for Case 4")?;
        let mut snapshot4 = wait_until_ready(&writer4, &mut driver4, 4, 5)?;
        assert!(!snapshot4.has_outstanding_batch_tickets);

        let wait_calls = Arc::new(AtomicU64::new(0));
        let injected = Arc::new(AtomicBool::new(false));
        let (trigger_tx, trigger_rx) = crossbeam_channel::bounded::<()>(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);
        let hook_batches: Vec<_> = (0..3).map(|i| batch(next_ts + i * 10)).collect();
        next_ts += 30;
        let hook_batch_write = batch(next_ts);
        writer4.io.set_progress_wait_hook(Arc::new({
            let wait_calls = Arc::clone(&wait_calls);
            let injected = Arc::clone(&injected);
            move |event| {
                if matches!(event, WalV8ProgressWaitTestEvent::BeforeWait) {
                    wait_calls.fetch_add(1, Ordering::AcqRel);
                    if !injected.swap(true, Ordering::AcqRel) {
                        let _ = trigger_tx.send(());
                        let _ = done_rx.recv_timeout(Duration::from_secs(5));
                    }
                }
            }
        }));
        let waited4 = thread::scope(|scope| -> Result<bool> {
            let injector = scope.spawn(|| -> Result<()> {
                if trigger_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
                    let _ = case4_fifth.finish()?;
                    for b in &hook_batches {
                        let prepared = writer4.preflight_batch_with_write_class(
                            b,
                            LIVE_WAL_V5_LIMITS,
                            WalV8WriteClass::Point,
                        )?;
                        let mut scratch = WalV8PointAdmissionScratch::new(prepared);
                        assert!(writer4.try_preallocate_point_buffer(&mut scratch));
                        let job = writer4.admit_prepared_point_batch(
                            scratch,
                            b.commit_ts,
                            b.recorded_at,
                        )?;
                        let _ = job.finish()?;
                    }
                    let prepared_batch = writer4.preflight_batch_with_write_class(
                        &hook_batch_write,
                        LIVE_WAL_V5_LIMITS,
                        WalV8WriteClass::Batch,
                    )?;
                    let batch_job = writer4.admit_prepared_batch(
                        prepared_batch,
                        hook_batch_write.commit_ts,
                        hook_batch_write.recorded_at,
                    )?;
                    let _ = done_tx.send(());
                    thread::sleep(Duration::from_millis(60));
                    let _ = batch_job.finish()?;
                }
                Ok(())
            });
            let waited = writer4.io.coalesce_progress_ring(
                &mut driver4,
                &mut snapshot4,
                Duration::from_millis(40),
                false,
                #[cfg(feature = "bench")]
                None,
            )?;
            injector.join().expect("case4 injector panicked")?;
            Ok(waited)
        })?;
        assert!(waited4);
        assert!(snapshot4.has_outstanding_batch_tickets);
        assert!(
            wait_calls.load(Ordering::Acquire) >= 2,
            "reaching 8 ready Points after Batch admission must continue Dynamic waiting"
        );
        drop(driver4);

        Ok(())
    }

    #[cfg(feature = "bench")]
    #[test]
    fn target8_diagnostics_distinguish_eligible_mixed_and_hit_phases() -> Result<()> {
        use crate::wal::v8::parallel::WalV8ReadyPrefix;

        assert_eq!(
            WalV8CoalesceTargetPolicy::Disabled.policy_name(),
            "disabled"
        );
        assert_eq!(WalV8CoalesceTargetPolicy::Always(8).policy_name(), "always");
        assert_eq!(
            WalV8CoalesceTargetPolicy::PointOnly(8).policy_name(),
            "point_only"
        );

        let make_snapshot = |ready: usize, has_batch: bool| WalV8FrontierReadySnapshot {
            ready_prefix: WalV8ReadyPrefix::Cursor {
                ticket_count: ready,
                tail: None,
                data_bytes: 0,
            },
            durable_ticket_end: 0,
            poisoned: false,
            admitted_ticket_count: 16,
            has_outstanding_batch_tickets: has_batch,
            closed: false,
            frames_ready_prefix_count: ready,
            data_complete_prefix_count: ready,
            reserved_preparation_bytes: 0,
            next_not_ready: None,
            data_hol_blocker: None,
            snapshot_monotonic_ns: None,
        };

        // Initial hit on Point-only cycle.
        let mut initial_hit = make_snapshot(8, false);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut initial_hit,
            Duration::from_millis(50),
            WalV8CoalesceTargetPolicy::PointOnly(8),
            |_, _| unreachable!(),
        )?;
        assert!(outcome.eligible_cycle);
        assert!(!outcome.mixed_fallback_cycle);
        assert!(outcome.target_hit);
        assert!(!outcome.waited);

        // After-wake hit on Point-only cycle.
        let mut after_wake_hit = make_snapshot(4, false);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut after_wake_hit,
            Duration::from_millis(50),
            WalV8CoalesceTargetPolicy::PointOnly(8),
            |_, snapshot| {
                snapshot.ready_prefix = WalV8ReadyPrefix::Cursor {
                    ticket_count: 8,
                    tail: None,
                    data_bytes: 0,
                };
                Ok(true)
            },
        )?;
        assert!(outcome.eligible_cycle);
        assert!(!outcome.mixed_fallback_cycle);
        assert!(outcome.target_hit);
        assert!(outcome.waited);

        // Mixed fallback cycle (unready Batch in window).
        let mut mixed = make_snapshot(8, true);
        let outcome = WalV8ParallelIo::coalesce_ready_prefix_with(
            &mut mixed,
            Duration::from_millis(50),
            WalV8CoalesceTargetPolicy::PointOnly(8),
            |_, snapshot| {
                snapshot.ready_prefix = WalV8ReadyPrefix::Cursor {
                    ticket_count: 9,
                    tail: None,
                    data_bytes: 0,
                };
                Ok(false)
            },
        )?;
        assert!(!outcome.eligible_cycle);
        assert!(outcome.mixed_fallback_cycle);
        assert!(!outcome.target_hit);
        assert!(outcome.waited);

        Ok(())
    }

    #[test]
    fn v8_candidate_stage_a_source_spool_exhaustion_and_recovery() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const INITIAL_SOURCE_CHARGE: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;
        const ONE_BATCH_SOURCE_CHARGE: u64 = 2 * WAL_V8_FRAME_LEN as u64;
        const SOURCE_SPOOL_LIMIT: u64 =
            SEGMENT_LIMIT + INITIAL_SOURCE_CHARGE + ONE_BATCH_SOURCE_CHARGE;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("candidate-quota.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            SOURCE_SPOOL_LIMIT,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => {
                eprintln!("skipping test (io_uring unavailable): {error:#}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let pt_batch = batch(10);
        let prepared = writer.preflight_batch_with_write_class(
            &pt_batch,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let quota_initial = owner.image_owner().snapshot();

        // 1. Hold one exact-sized batch reservation to exhaust source-spool quota.
        let held_reservation = owner.reserve_batch(
            u64::try_from(prepared.logical_batch_bytes())?,
            u64::try_from(prepared.frame_buffer_bytes())?,
        )?;
        let quota_while_held = owner.image_owner().snapshot();
        assert_eq!(
            quota_while_held.source_spool_bytes,
            quota_initial.source_spool_bytes + ONE_BATCH_SOURCE_CHARGE
        );

        // 2. Single-frame Point write hits Stage A without charging quota, fails
        // at admission with SourceSpoolLimit, releases its Stage A permit, and
        // leaves quota and writer unpoisoned.
        let mut scratch = WalV8PointAdmissionScratch::new(prepared);
        assert!(writer.try_preallocate_point_buffer(&mut scratch));
        assert!(writer.point_buffer_preallocation_hits_for_test() >= 1);
        assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 1);
        assert_eq!(owner.image_owner().snapshot(), quota_while_held);

        let err = match writer.admit_prepared_point_batch(
            scratch,
            pt_batch.commit_ts,
            pt_batch.recorded_at,
        ) {
            Ok(_) => anyhow::bail!("expected SourceSpoolLimit when quota is exhausted"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err.downcast_ref::<crate::wal::v7::admission::WalV7ReservationError>(),
                Some(crate::wal::v7::admission::WalV7ReservationError::SourceSpoolLimit { .. })
            ),
            "expected SourceSpoolLimit, got {err:#}"
        );
        assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 0);
        assert_eq!(owner.image_owner().snapshot(), quota_while_held);
        assert!(!writer.is_poisoned());

        // 3. Release held reservation -> quota restores and Point write succeeds.
        drop(held_reservation);
        assert_eq!(owner.image_owner().snapshot(), quota_initial);
        let prepared_ok = writer.preflight_batch_with_write_class(
            &pt_batch,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut scratch_ok = WalV8PointAdmissionScratch::new(prepared_ok);
        assert!(writer.try_preallocate_point_buffer(&mut scratch_ok));
        let job = writer.admit_prepared_point_batch(
            scratch_ok,
            pt_batch.commit_ts,
            pt_batch.recorded_at,
        )?;
        let _ = job.finish()?;
        writer.sync()?;
        assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 0);

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wait_durable_unassigned_ticket_does_not_poison_and_poisoned_fails() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wait-durable-edge.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, _owner) = open_accounted_writer(
            &path,
            &installed,
            64 * 1024,
            1024 * 1024,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        };

        // Unassigned ticket fails without poisoning the coordinator.
        let unassigned_err = writer
            .wait_durable(0)
            .expect_err("unassigned ticket wait must fail");
        assert!(unassigned_err.to_string().contains("unassigned ticket"));
        assert!(!writer.is_poisoned());

        // Overflowing ticket also fails without poisoning.
        assert!(writer.wait_durable(u64::MAX).is_err());
        assert!(!writer.is_poisoned());

        // Admit and finish one ticket; wait_durable succeeds twice (driver + already-durable).
        let b = batch(1);
        let prepared = writer.preflight_batch_with_write_class(
            &b,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let ticket = writer
            .append_prepared_batch(prepared, b.commit_ts, b.recorded_at)?
            .0
            .ticket;
        writer.wait_durable(ticket)?;
        writer.wait_durable(ticket)?;
        assert!(!writer.is_poisoned());

        // Poisoned coordinator rejects wait_durable even for an already-durable or unassigned
        // ticket.
        writer.coordinator.poison();
        assert!(writer.is_poisoned());
        let poisoned_durable = writer
            .wait_durable(ticket)
            .expect_err("poisoned coordinator must reject wait_durable");
        assert!(poisoned_durable.to_string().contains("poisoned"));
        let poisoned_unassigned = writer
            .wait_durable(ticket + 10)
            .expect_err("poisoned coordinator must reject unassigned wait_durable");
        assert!(poisoned_unassigned.to_string().contains("poisoned"));

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn point_admit_defers_frontier_resource_registration_until_finish() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const INITIAL_SOURCE_CHARGE: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;
        const TWO_BATCH_SOURCE_CHARGE: u64 = 4 * WAL_V8_FRAME_LEN as u64;
        const SOURCE_SPOOL_LIMIT: u64 =
            SEGMENT_LIMIT + INITIAL_SOURCE_CHARGE + TWO_BATCH_SOURCE_CHARGE;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("defer-frontier-point.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, _owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            SOURCE_SPOOL_LIMIT,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        };

        // Hold an earlier unfinished job j1 so the durability driver cannot retire j2 before we
        // observe j2's frontier_resources entry after j2.finish().
        let b1 = batch(10);
        let p1 = writer.preflight_batch_with_write_class(
            &b1,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s1 = WalV8PointAdmissionScratch::new(p1);
        assert!(writer.try_preallocate_point_buffer(&mut s1));
        let j1 = writer.admit_prepared_point_batch(s1, b1.commit_ts, b1.recorded_at)?;
        let t1 = j1.assignment().segment_ticket;
        assert!(!writer.ticket_resources.lock().contains_key(&t1));

        let b2 = batch(20);
        let p2 = writer.preflight_batch_with_write_class(
            &b2,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s2 = WalV8PointAdmissionScratch::new(p2);
        assert!(writer.try_preallocate_point_buffer(&mut s2));
        let j2 = writer.admit_prepared_point_batch(s2, b2.commit_ts, b2.recorded_at)?;
        let t2 = j2.assignment().segment_ticket;
        assert!(!writer.ticket_resources.lock().contains_key(&t2));

        let _ = j2.finish()?;
        assert!(writer.ticket_resources.lock().contains_key(&t2));

        let _ = j1.finish()?;
        writer.sync()?;
        assert!(!writer.ticket_resources.lock().contains_key(&t1));
        assert!(!writer.ticket_resources.lock().contains_key(&t2));
        assert!(writer.ticket_resources.lock().is_empty());
        assert_eq!(writer.point_buffer_preallocations_in_flight_for_test(), 0);

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn point_unfinished_job_drop_poisons_and_releases_buffer_quota() -> Result<()> {
        const SEGMENT_LIMIT: u64 = 64 * 1024;
        const INITIAL_SOURCE_CHARGE: u64 = HEADER_LEN_U64 + FRAME_LEN_U64;
        const ONE_BATCH_SOURCE_CHARGE: u64 = 2 * WAL_V8_FRAME_LEN as u64;
        const SOURCE_SPOOL_LIMIT: u64 =
            SEGMENT_LIMIT + INITIAL_SOURCE_CHARGE + ONE_BATCH_SOURCE_CHARGE;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("point-unfinished-drop.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, owner) = open_accounted_writer(
            &path,
            &installed,
            SEGMENT_LIMIT,
            SOURCE_SPOOL_LIMIT,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        let initial_quota = owner.image_owner().snapshot();

        // Admit then drop FrameJob without finish(): coordinator is poisoned and buffer quota
        // released.
        let pt_batch = batch(10);
        let prepared = writer.preflight_batch_with_write_class(
            &pt_batch,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut scratch = WalV8PointAdmissionScratch::new(prepared);
        assert!(writer.try_preallocate_point_buffer(&mut scratch));
        let job =
            writer.admit_prepared_point_batch(scratch, pt_batch.commit_ts, pt_batch.recorded_at)?;
        assert!(
            owner.image_owner().snapshot().source_spool_bytes > initial_quota.source_spool_bytes
        );
        assert!(
            owner.image_owner().snapshot().buffer_memory_bytes > initial_quota.buffer_memory_bytes
        );
        drop(job);
        assert!(writer.is_poisoned());
        assert!(writer.ticket_resources.lock().is_empty());
        assert_eq!(
            owner.image_owner().snapshot().buffer_memory_bytes,
            initial_quota.buffer_memory_bytes
        );

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn point_out_of_order_finish_and_mixed_batch_retire_all_resources() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("point-ooo-mixed.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, owner) = open_accounted_writer(
            &path,
            &installed,
            256 * 1024,
            1024 * 1024,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        let initial_quota = owner.image_owner().snapshot();

        // Admit two Point jobs and one Batch job, then finish them out of order.
        let b1 = batch(10);
        let p1 = writer.preflight_batch_with_write_class(
            &b1,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s1 = WalV8PointAdmissionScratch::new(p1);
        assert!(writer.try_preallocate_point_buffer(&mut s1));
        let j1 = writer.admit_prepared_point_batch(s1, b1.commit_ts, b1.recorded_at)?;

        let b2 = batch(20);
        let p2 = writer.preflight_batch_with_write_class(
            &b2,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s2 = WalV8PointAdmissionScratch::new(p2);
        assert!(writer.try_preallocate_point_buffer(&mut s2));
        let j2 = writer.admit_prepared_point_batch(s2, b2.commit_ts, b2.recorded_at)?;

        let b3 = batch(30);
        let p3 = writer.preflight_batch_with_write_class(
            &b3,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Batch,
        )?;
        let j3 = writer.admit_prepared_batch(p3, b3.commit_ts, b3.recorded_at)?;

        // Out-of-order finish: Batch j3, then Point j2, then Point j1.
        let _ = j3.finish()?;
        let _ = j2.finish()?;
        let _ = j1.finish()?;
        writer.sync()?;
        assert!(writer.ticket_resources.lock().is_empty());
        let after_sync = owner.image_owner().snapshot();
        assert_eq!(
            after_sync.buffer_memory_bytes,
            initial_quota.buffer_memory_bytes
        );
        assert_eq!(
            after_sync.seal_index_bytes,
            initial_quota.seal_index_bytes + 24
        );

        Ok(())
    }

    #[test]
    fn drain_ticket_prefix_handles_exact_partial_non_front_missing_and_fail_stop() -> Result<()> {
        let make_map =
            |keys: &[u64]| -> BTreeMap<u64, u64> { keys.iter().map(|&k| (k, k * 10)).collect() };

        // 1. Exact prefix [1, 4) from [1, 2, 3].
        let mut m1 = make_map(&[1, 2, 3]);
        let mut seen = Vec::new();
        let (count, _, _, _) = WalV8ParallelWriter::drain_ticket_prefix(
            &mut m1,
            1,
            4,
            "missing",
            "disappeared",
            false,
            |_| Ok(()),
            |idx, ticket, val| {
                seen.push((idx, ticket, val));
                Ok(())
            },
        )?;
        assert_eq!(count, 3);
        assert_eq!(seen, vec![(0, 1, 10), (1, 2, 20), (2, 3, 30)]);
        assert!(m1.is_empty());

        // 2. Partial prefix [1, 3) from [1, 2, 3, 4] retains 3, 4.
        let mut m2 = make_map(&[1, 2, 3, 4]);
        let (count, _, _, _) = WalV8ParallelWriter::drain_ticket_prefix(
            &mut m2,
            1,
            3,
            "missing",
            "disappeared",
            false,
            |_| Ok(()),
            |_, _, _| Ok(()),
        )?;
        assert_eq!(count, 2);
        assert_eq!(m2.keys().copied().collect::<Vec<_>>(), vec![3, 4]);

        // 3. Non-front range [1, 3) from [0, 1, 2, 3] retains 0, 3.
        let mut m3 = make_map(&[0, 1, 2, 3]);
        let mut seen3 = Vec::new();
        let (count, _, _, _) = WalV8ParallelWriter::drain_ticket_prefix(
            &mut m3,
            1,
            3,
            "missing",
            "disappeared",
            false,
            |_| Ok(()),
            |idx, ticket, val| {
                seen3.push((idx, ticket, val));
                Ok(())
            },
        )?;
        assert_eq!(count, 2);
        assert_eq!(seen3, vec![(0, 1, 10), (1, 2, 20)]);
        assert_eq!(m3.keys().copied().collect::<Vec<_>>(), vec![0, 3]);

        // 4. Missing middle ticket [1, 4) from [1, 3, 4] errors without mutating map.
        let mut m4 = make_map(&[1, 3, 4]);
        assert!(
            WalV8ParallelWriter::drain_ticket_prefix(
                &mut m4,
                1,
                4,
                "missing",
                "disappeared",
                false,
                |_| Ok(()),
                |_, _, _| Ok(()),
            )
            .is_err()
        );
        assert_eq!(m4.keys().copied().collect::<Vec<_>>(), vec![1, 3, 4]);

        // 5. Missing start ticket [1, 4) from [2, 3, 4] errors without mutating map.
        let mut m5 = make_map(&[2, 3, 4]);
        assert!(
            WalV8ParallelWriter::drain_ticket_prefix(
                &mut m5,
                1,
                4,
                "missing",
                "disappeared",
                false,
                |_| Ok(()),
                |_, _, _| Ok(()),
            )
            .is_err()
        );
        assert_eq!(m5.keys().copied().collect::<Vec<_>>(), vec![2, 3, 4]);

        // 6. Empty range [1, 1) on empty map is a no-op.
        let mut m6 = BTreeMap::<u64, u64>::new();
        let (count, _, _, _) = WalV8ParallelWriter::drain_ticket_prefix(
            &mut m6,
            1,
            1,
            "missing",
            "disappeared",
            false,
            |_| Ok(()),
            |_, _, _| Ok(()),
        )?;
        assert_eq!(count, 0);

        // 7. Reverse range returns Err without panicking or mutating map.
        let mut m7 = make_map(&[1, 2]);
        assert!(
            WalV8ParallelWriter::drain_ticket_prefix(
                &mut m7,
                4,
                2,
                "missing",
                "disappeared",
                false,
                |_| Ok(()),
                |_, _, _| Ok(()),
            )
            .is_err()
        );
        assert_eq!(m7.keys().copied().collect::<Vec<_>>(), vec![1, 2]);

        // 8. Callback error preserves fail-stop semantics.
        let mut m8 = make_map(&[1, 2, 3]);
        let mut visited = Vec::new();
        let err = WalV8ParallelWriter::drain_ticket_prefix(
            &mut m8,
            1,
            4,
            "missing",
            "disappeared",
            false,
            |_| Ok(()),
            |_, ticket, _| {
                visited.push(ticket);
                anyhow::ensure!(ticket != 2, "fail on 2");
                Ok(())
            },
        );
        assert!(err.is_err());
        assert_eq!(visited, vec![1, 2]);
        assert_eq!(m8.keys().copied().collect::<Vec<_>>(), vec![3]);

        // 9. Pre-drain validator failure rejects before removing any entry.
        let mut m9 = make_map(&[1, 2, 3]);
        assert!(
            WalV8ParallelWriter::drain_ticket_prefix(
                &mut m9,
                1,
                4,
                "missing",
                "disappeared",
                false,
                |val| {
                    anyhow::ensure!(*val != 20, "invalid entry 20");
                    Ok(())
                },
                |_, _, _| unreachable!("must not invoke on_entry after validation failure"),
            )
            .is_err()
        );
        assert_eq!(m9.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ticket_resources_retirement_rejects_incomplete_or_unretired_entries_atomically() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("incomplete-ticket-resources.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, _owner) = open_accounted_writer(
            &path,
            &installed,
            256 * 1024,
            1024 * 1024,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            WalV8RequestedConfig::candidate(),
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        };

        let b1 = batch(10);
        let p1 = writer.preflight_batch_with_write_class(
            &b1,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s1 = WalV8PointAdmissionScratch::new(p1);
        assert!(writer.try_preallocate_point_buffer(&mut s1));
        let j1 = writer.admit_prepared_point_batch(s1, b1.commit_ts, b1.recorded_at)?;
        let t1 = j1.assignment().segment_ticket;

        let b2 = batch(20);
        let p2 = writer.preflight_batch_with_write_class(
            &b2,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s2 = WalV8PointAdmissionScratch::new(p2);
        assert!(writer.try_preallocate_point_buffer(&mut s2));
        let j2 = writer.admit_prepared_point_batch(s2, b2.commit_ts, b2.recorded_at)?;
        let t2 = j2.assignment().segment_ticket;

        let _ = j1.finish()?;
        let _ = j2.finish()?;

        // Missing guard on t2: retire_frontier_resources must fail without retiring t1.
        let saved_guard_t2 = writer
            .ticket_resources
            .lock()
            .get_mut(&t2)
            .expect("t2 entry")
            .guard
            .take();
        assert!(writer.retire_frontier_resources(t1, t2 + 1, false).is_err());
        assert!(
            writer
                .ticket_resources
                .lock()
                .get(&t1)
                .expect("t1 entry")
                .reservation
                .is_some(),
            "t1 reservation must remain unretired when t2 guard is missing"
        );
        writer
            .ticket_resources
            .lock()
            .get_mut(&t2)
            .expect("t2 entry")
            .guard = saved_guard_t2;

        // Guard retirement before resource retirement: must fail without removing entries.
        assert!(writer.retire_frontier_guards(t1, t2 + 1, false).is_err());
        assert!(writer.ticket_resources.lock().contains_key(&t1));
        assert!(writer.ticket_resources.lock().contains_key(&t2));

        // Normal full sync on a fresh writer retires resources then guards; duplicate resource
        // retirement fails.
        let path2 = dir.path().join("complete-ticket-resources.wal");
        let installed2 = install_empty(&path2, header())?;
        let sync_file2 = OpenOptions::new().read(true).write(true).open(&path2)?;
        let (leader2, _owner2) = open_accounted_writer(
            &path2,
            &installed2,
            256 * 1024,
            1024 * 1024,
            FRAME_LEN_U64,
            sync_file2,
        )?;
        let writer2 = leader2.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path2)?,
            WalV8RequestedConfig::candidate(),
        )?;
        let b3 = batch(30);
        let p3 = writer2.preflight_batch_with_write_class(
            &b3,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s3 = WalV8PointAdmissionScratch::new(p3);
        assert!(writer2.try_preallocate_point_buffer(&mut s3));
        let j3 = writer2.admit_prepared_point_batch(s3, b3.commit_ts, b3.recorded_at)?;
        let t3 = j3.assignment().segment_ticket;
        let _ = j3.finish()?;
        writer2.sync()?;
        assert!(writer2.ticket_resources.lock().is_empty());
        assert!(
            writer2
                .retire_frontier_resources(t3, t3 + 1, false)
                .is_err()
        );

        Ok(())
    }

    #[cfg(all(target_os = "linux", feature = "bench"))]
    #[test]
    fn publish_data_single_ticket_fast_path_validates_and_bypasses_pending_map() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("single-ticket-publish.wal");
        let installed = install_empty(&path, header())?;
        let sync_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let (leader, _owner) = open_accounted_writer(
            &path,
            &installed,
            256 * 1024,
            1024 * 1024,
            FRAME_LEN_U64,
            sync_file,
        )?;
        let config = WalV8RequestedConfig {
            data_submit_ready_order: true,
            ..WalV8RequestedConfig::candidate()
        };
        let writer = match leader.into_parallel_with_worker_file_and_config(
            OpenOptions::new().read(true).write(true).open(&path)?,
            config,
        ) {
            Ok(writer) => writer,
            Err(error) if is_io_uring_unavailable_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        };

        // 1. Unadmitted ticket is rejected without inserting into pending map.
        let unadmitted_ticket = writer.coordinator.admitted_ticket_end() + 5;
        let mut data_unadmitted = WalV8DataBuffer(DirectBuf::new(WAL_V8_FRAME_LEN));
        data_unadmitted.0.as_mut_slice().fill(0);
        data_unadmitted.set_len(WAL_V8_FRAME_LEN);
        assert!(
            writer
                .io
                .publish_data(
                    unadmitted_ticket,
                    HEADER_LEN_U64 + FRAME_LEN_U64,
                    data_unadmitted,
                    None,
                )
                .is_err()
        );
        assert!(writer.io.submission.lock().pending.is_empty());

        // 2. Normal single-ticket Point finishes cleanly and leaves pending map empty.
        let b1 = batch(10);
        let p1 = writer.preflight_batch_with_write_class(
            &b1,
            LIVE_WAL_V5_LIMITS,
            WalV8WriteClass::Point,
        )?;
        let mut s1 = WalV8PointAdmissionScratch::new(p1);
        assert!(writer.try_preallocate_point_buffer(&mut s1));
        let j1 = writer.admit_prepared_point_batch(s1, b1.commit_ts, b1.recorded_at)?;
        let offset1 = j1.assignment().first_data_offset;
        let _ = j1.finish()?;
        {
            let submission = writer.io.submission.lock();
            assert!(submission.pending.is_empty());
            assert_eq!(submission.next_file_offset, Some(offset1 + FRAME_LEN_U64));
        }

        // 3. Stale ticket (< next_ticket) is rejected.
        writer.io.submission.lock().next_ticket = 99;
        let mut data_stale = WalV8DataBuffer(DirectBuf::new(WAL_V8_FRAME_LEN));
        data_stale.0.as_mut_slice().fill(0);
        data_stale.set_len(WAL_V8_FRAME_LEN);
        assert!(
            writer
                .io
                .publish_data(1, offset1 + FRAME_LEN_U64, data_stale, None)
                .is_err()
        );
        writer.io.submission.lock().next_ticket = 0;

        writer.sync()?;
        Ok(())
    }
}
