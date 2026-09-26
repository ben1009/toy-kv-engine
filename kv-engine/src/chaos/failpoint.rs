//! Deterministic failpoint injection for chaos testing (RFC 013 Phase 3).

//! Powered by the `fail` crate (tikv/fail-rs v0.5). All injection sites use
//! the `fail_point!` macro so that when failpoints are disabled the macro
//! expands to a no-op — zero runtime cost in production builds.
//!
//! ## Usage at injection sites
//!
//! ```ignore
//! #[cfg(feature = "chaos-testing")]
//! {
//!     crate::chaos::failpoint::fail_point!("wal.after_batch_encode");
//! }
//! ```
//!
//! ## Usage in tests
//!
//! ```ignore
//! use kv_engine::chaos::failpoint::{cfg, FailScenario};
//! let scenario = FailScenario::setup();
//! cfg("wal.after_batch_encode", "panic").unwrap();
//! // ... run code that triggers the failpoint ...
//! scenario.teardown();
//! ```

use std::{
    fs::OpenOptions,
    io::Write,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

/// The core failpoint macro. Re-exported from the `fail` crate so injection
/// sites within this crate can use it via `crate::chaos::failpoint::fail_point!`.
pub(crate) use fail::fail_point;

/// Re-export failpoint configuration primitives for tests.
pub use fail::{FailScenario, cfg};

/// Environment variable selecting the parallel WAL crash boundary for a child process.
#[doc(hidden)]
pub const PARALLEL_WAL_CRASH_POINT_ENV: &str = "TOY_KV_PARALLEL_WAL_CRASH_POINT";
/// Environment variable selecting which occurrence of a crash boundary to pause at.
#[doc(hidden)]
pub const PARALLEL_WAL_CRASH_OCCURRENCE_ENV: &str = "TOY_KV_PARALLEL_WAL_CRASH_OCCURRENCE";
/// Environment variable containing the marker file path written before a child pauses.
#[doc(hidden)]
pub const PARALLEL_WAL_CRASH_MARKER_ENV: &str = "TOY_KV_PARALLEL_WAL_CRASH_MARKER";

static PARALLEL_WAL_CRASH_POINT_HITS: AtomicUsize = AtomicUsize::new(0);
static DEFER_LOWEST_PARALLEL_WAL_GROUP_COMPLETION: AtomicBool = AtomicBool::new(false);
static PARALLEL_WAL_ADMISSIONS: AtomicUsize = AtomicUsize::new(0);
#[cfg(all(test, feature = "chaos-testing"))]
static PARALLEL_WAL_INJECTED_GROUP_FAILURES: AtomicUsize = AtomicUsize::new(0);
#[cfg(all(test, feature = "chaos-testing"))]
static PARALLEL_WAL_SYNCS_WITH_POISON: AtomicUsize = AtomicUsize::new(0);
#[cfg(all(test, feature = "chaos-testing"))]
#[derive(Default)]
struct ParallelWalTestGateState {
    armed: bool,
    entered: bool,
    released: bool,
}
#[cfg(all(test, feature = "chaos-testing"))]
#[derive(Default)]
struct ParallelWalTestGate {
    state: std::sync::Mutex<ParallelWalTestGateState>,
    changed: std::sync::Condvar,
}
#[cfg(all(test, feature = "chaos-testing"))]
static PARALLEL_WAL_FDATASYNC_GATE: std::sync::OnceLock<ParallelWalTestGate> =
    std::sync::OnceLock::new();
#[cfg(all(test, feature = "chaos-testing"))]
static PARALLEL_WAL_RESULT_DRAIN_GATE: std::sync::OnceLock<ParallelWalTestGate> =
    std::sync::OnceLock::new();

/// Pause a child at a configured parallel WAL boundary until the parent kills it.
pub(crate) fn parallel_wal_crash_point(point: &str) {
    if !matches!(
        std::env::var(PARALLEL_WAL_CRASH_POINT_ENV),
        Ok(configured) if configured == point
    ) {
        return;
    }

    let occurrence = std::env::var(PARALLEL_WAL_CRASH_OCCURRENCE_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let hit = PARALLEL_WAL_CRASH_POINT_HITS.fetch_add(1, Ordering::AcqRel) + 1;
    if hit != occurrence {
        return;
    }

    let marker_path = std::env::var_os(PARALLEL_WAL_CRASH_MARKER_ENV)
        .expect("parallel WAL crash test must provide a marker path");
    let marker_path = std::path::PathBuf::from(marker_path);
    let temporary_marker_path = marker_path.with_extension("tmp");
    let mut marker = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary_marker_path)
        .expect("create parallel WAL crash marker");
    writeln!(marker, "{point}").expect("write parallel WAL crash marker");
    marker.sync_all().expect("sync parallel WAL crash marker");
    drop(marker);
    std::fs::rename(temporary_marker_path, marker_path).expect("publish parallel WAL crash marker");

    loop {
        std::thread::park_timeout(Duration::from_secs(60));
    }
}

/// Make the chaos-testing WAL worker defer CQEs for its lowest-ticket group.
#[doc(hidden)]
pub fn enable_deferred_lowest_parallel_wal_group_completion() {
    DEFER_LOWEST_PARALLEL_WAL_GROUP_COMPLETION.store(true, Ordering::Release);
}

/// Reset the chaos child admission counter used to order parallel WAL test writes.
#[doc(hidden)]
pub fn reset_parallel_wal_admission_count() {
    PARALLEL_WAL_ADMISSIONS.store(0, Ordering::Release);
}

/// Return the number of parallel WAL batches admitted since the last reset.
#[doc(hidden)]
pub fn parallel_wal_admission_count() -> usize {
    PARALLEL_WAL_ADMISSIONS.load(Ordering::Acquire)
}

pub(crate) fn note_parallel_wal_admission() {
    PARALLEL_WAL_ADMISSIONS.fetch_add(1, Ordering::AcqRel);
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn reset_parallel_wal_failure_test_counters() {
    PARALLEL_WAL_INJECTED_GROUP_FAILURES.store(0, Ordering::Release);
    PARALLEL_WAL_SYNCS_WITH_POISON.store(0, Ordering::Release);
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn parallel_wal_injected_group_failure_count() -> usize {
    PARALLEL_WAL_INJECTED_GROUP_FAILURES.load(Ordering::Acquire)
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn parallel_wal_sync_with_poison_count() -> usize {
    PARALLEL_WAL_SYNCS_WITH_POISON.load(Ordering::Acquire)
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn note_parallel_wal_sync_with_poison() {
    PARALLEL_WAL_SYNCS_WITH_POISON.fetch_add(1, Ordering::AcqRel);
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) struct ParallelWalTestGateGuard(&'static ParallelWalTestGate);

#[cfg(all(test, feature = "chaos-testing"))]
impl ParallelWalTestGateGuard {
    pub(crate) fn wait_until_entered(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self
            .0
            .state
            .lock()
            .expect("parallel WAL test gate mutex poisoned");
        while !state.entered {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            let (next_state, result) = self
                .0
                .changed
                .wait_timeout(state, remaining)
                .expect("parallel WAL test gate mutex poisoned");
            state = next_state;
            if result.timed_out() && !state.entered {
                return false;
            }
        }

        true
    }

    pub(crate) fn release(&self) {
        let mut state = self
            .0
            .state
            .lock()
            .expect("parallel WAL test gate mutex poisoned");
        state.armed = false;
        state.released = true;
        self.0.changed.notify_all();
    }
}

#[cfg(all(test, feature = "chaos-testing"))]
impl Drop for ParallelWalTestGateGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(all(test, feature = "chaos-testing"))]
impl ParallelWalTestGate {
    fn arm(&'static self) -> ParallelWalTestGateGuard {
        let mut state = self
            .state
            .lock()
            .expect("parallel WAL test gate mutex poisoned");
        *state = ParallelWalTestGateState {
            armed: true,
            ..ParallelWalTestGateState::default()
        };

        ParallelWalTestGateGuard(self)
    }

    fn enter(&self) {
        let mut state = self
            .state
            .lock()
            .expect("parallel WAL test gate mutex poisoned");
        if !state.armed {
            return;
        }

        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .expect("parallel WAL test gate mutex poisoned");
        }
        state.armed = false;
    }
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn arm_parallel_wal_sync_gate() -> ParallelWalTestGateGuard {
    PARALLEL_WAL_FDATASYNC_GATE
        .get_or_init(ParallelWalTestGate::default)
        .arm()
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn arm_parallel_wal_result_drain_gate() -> ParallelWalTestGateGuard {
    PARALLEL_WAL_RESULT_DRAIN_GATE
        .get_or_init(ParallelWalTestGate::default)
        .arm()
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn before_parallel_wal_fdatasync_call() {
    PARALLEL_WAL_FDATASYNC_GATE
        .get_or_init(ParallelWalTestGate::default)
        .enter();
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn before_parallel_wal_result_drain() {
    PARALLEL_WAL_RESULT_DRAIN_GATE
        .get_or_init(ParallelWalTestGate::default)
        .enter();
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn parallel_wal_group_completion_error(ticket_start: u64) -> Option<String> {
    fail_point!(
        "parallel_wal.group_completion_failure",
        ticket_start > 0,
        |_| Some("injected parallel WAL group write failure".to_owned())
    );
    None
}

#[cfg(all(test, feature = "chaos-testing"))]
pub(crate) fn note_parallel_wal_injected_group_failure() {
    PARALLEL_WAL_INJECTED_GROUP_FAILURES.fetch_add(1, Ordering::AcqRel);
}

pub(crate) fn defer_lowest_parallel_wal_group_completion() -> bool {
    DEFER_LOWEST_PARALLEL_WAL_GROUP_COMPLETION.load(Ordering::Acquire)
}
