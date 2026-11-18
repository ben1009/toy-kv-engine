//! Opt-in sampled timing for selected hot mutexes in benchmark builds.
//!
//! Exact acquisition counters and sampled histograms are kept per thread to
//! avoid making the instrumentation itself a shared cache-line bottleneck.

#[cfg(feature = "bench")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::{Mutex, MutexGuard};
use serde::Serialize;

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum MutexLockKind {
    ResourceLedger,
    ReservationOwner,
    ExtentState,
    ExtentPreparation,
    MvccPublication,
}

#[cfg(feature = "bench")]
const LOCK_KIND_COUNT: usize = 5;
#[cfg(feature = "bench")]
const HISTOGRAM_BUCKETS: usize = 64;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[doc(hidden)]
pub struct MutexLockStatsSnapshot {
    pub name: &'static str,
    pub acquisition_count: u64,
    pub sampled_acquisition_count: u64,
    pub sampled_contended_count: u64,
    pub estimated_contended_count: u64,
    pub sampled_wait_ns_total: u64,
    pub estimated_wait_ns_total: u64,
    pub wait_p50_ns: Option<u64>,
    pub wait_p95_ns: Option<u64>,
    pub wait_p99_ns: Option<u64>,
    pub sampled_hold_ns_total: u64,
    pub estimated_hold_ns_total: u64,
    pub hold_p50_ns: Option<u64>,
    pub hold_p95_ns: Option<u64>,
    pub hold_p99_ns: Option<u64>,
    pub condvar_reacquisitions: u64,
    pub sampled_condvar_wait_ns_total: u64,
    pub condvar_wait_p50_ns: Option<u64>,
    pub condvar_wait_p95_ns: Option<u64>,
    pub condvar_wait_p99_ns: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[doc(hidden)]
pub struct MutexLockDiagnosticsSnapshot {
    pub sample_every: u64,
    pub histogram: &'static str,
    pub acquisition_scope: &'static str,
    pub locks: Vec<MutexLockStatsSnapshot>,
}

#[cfg(feature = "bench")]
struct LockCounters {
    acquisition_count: AtomicU64,
    sampled_acquisition_count: AtomicU64,
    sampled_contended_count: AtomicU64,
    sampled_wait_ns_total: AtomicU64,
    sampled_hold_ns_total: AtomicU64,
    condvar_reacquisitions: AtomicU64,
    sampled_condvar_wait_ns_total: AtomicU64,
    wait_histogram: [AtomicU64; HISTOGRAM_BUCKETS],
    hold_histogram: [AtomicU64; HISTOGRAM_BUCKETS],
    condvar_wait_histogram: [AtomicU64; HISTOGRAM_BUCKETS],
}

#[cfg(feature = "bench")]
impl LockCounters {
    fn new() -> Self {
        Self {
            acquisition_count: AtomicU64::new(0),
            sampled_acquisition_count: AtomicU64::new(0),
            sampled_contended_count: AtomicU64::new(0),
            sampled_wait_ns_total: AtomicU64::new(0),
            sampled_hold_ns_total: AtomicU64::new(0),
            condvar_reacquisitions: AtomicU64::new(0),
            sampled_condvar_wait_ns_total: AtomicU64::new(0),
            wait_histogram: std::array::from_fn(|_| AtomicU64::new(0)),
            hold_histogram: std::array::from_fn(|_| AtomicU64::new(0)),
            condvar_wait_histogram: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn reset(&self) {
        self.acquisition_count.store(0, Ordering::Relaxed);
        self.sampled_acquisition_count.store(0, Ordering::Relaxed);
        self.sampled_contended_count.store(0, Ordering::Relaxed);
        self.sampled_wait_ns_total.store(0, Ordering::Relaxed);
        self.sampled_hold_ns_total.store(0, Ordering::Relaxed);
        self.condvar_reacquisitions.store(0, Ordering::Relaxed);
        self.sampled_condvar_wait_ns_total
            .store(0, Ordering::Relaxed);
        for bucket in self
            .wait_histogram
            .iter()
            .chain(&self.hold_histogram)
            .chain(&self.condvar_wait_histogram)
        {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(feature = "bench")]
struct ThreadCounters {
    locks: [LockCounters; LOCK_KIND_COUNT],
}

#[cfg(feature = "bench")]
impl ThreadCounters {
    fn new() -> Self {
        Self {
            locks: std::array::from_fn(|_| LockCounters::new()),
        }
    }

    fn counter(&self, kind: MutexLockKind) -> &LockCounters {
        &self.locks[kind as usize]
    }
}

#[cfg(feature = "bench")]
struct Registry {
    sample_every: AtomicU64,
    threads: Mutex<Vec<std::sync::Arc<ThreadCounters>>>,
}

#[cfg(feature = "bench")]
impl Registry {
    fn new() -> Self {
        Self {
            sample_every: AtomicU64::new(0),
            threads: Mutex::new(Vec::new()),
        }
    }
}

#[cfg(feature = "bench")]
static REGISTRY: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();

#[cfg(feature = "bench")]
thread_local! {
    static THREAD_COUNTERS: std::cell::RefCell<Option<std::sync::Arc<ThreadCounters>>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(feature = "bench")]
fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

#[cfg(feature = "bench")]
fn lock_name(kind: MutexLockKind) -> &'static str {
    match kind {
        MutexLockKind::ResourceLedger => "resource_ledger",
        MutexLockKind::ReservationOwner => "reservation_owner",
        MutexLockKind::ExtentState => "v8_extent_state",
        MutexLockKind::ExtentPreparation => "v8_extent_preparation",
        MutexLockKind::MvccPublication => "mvcc_publication",
    }
}

#[cfg(feature = "bench")]
fn histogram_bucket(ns: u64) -> usize {
    if ns == 0 {
        0
    } else {
        (u64::BITS - ns.leading_zeros()) as usize - 1
    }
}

#[cfg(feature = "bench")]
fn record_histogram(histogram: &[AtomicU64; HISTOGRAM_BUCKETS], ns: u64) {
    histogram[histogram_bucket(ns)].fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "bench")]
fn histogram_quantile(
    histogram: &[u64; HISTOGRAM_BUCKETS],
    numerator: u64,
    denominator: u64,
) -> Option<u64> {
    let total = histogram.iter().copied().sum::<u64>();
    if total == 0 {
        return None;
    }
    let rank = total.saturating_mul(numerator).div_ceil(denominator).max(1);
    let mut cumulative = 0u64;
    for (index, count) in histogram.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= rank {
            return Some(if index == 63 {
                u64::MAX
            } else {
                1u64 << (index + 1)
            });
        }
    }
    Some(u64::MAX)
}

#[cfg(feature = "bench")]
fn snapshot_lock(
    kind: MutexLockKind,
    threads: &[std::sync::Arc<ThreadCounters>],
) -> MutexLockStatsSnapshot {
    let mut acquisition_count = 0u64;
    let mut sampled_acquisition_count = 0u64;
    let mut sampled_contended_count = 0u64;
    let mut sampled_wait_ns_total = 0u64;
    let mut sampled_hold_ns_total = 0u64;
    let mut condvar_reacquisitions = 0u64;
    let mut sampled_condvar_wait_ns_total = 0u64;
    let mut wait_histogram = [0u64; HISTOGRAM_BUCKETS];
    let mut hold_histogram = [0u64; HISTOGRAM_BUCKETS];
    let mut condvar_wait_histogram = [0u64; HISTOGRAM_BUCKETS];

    for thread in threads {
        let counters = thread.counter(kind);
        acquisition_count =
            acquisition_count.saturating_add(counters.acquisition_count.load(Ordering::Relaxed));
        sampled_acquisition_count = sampled_acquisition_count
            .saturating_add(counters.sampled_acquisition_count.load(Ordering::Relaxed));
        sampled_contended_count = sampled_contended_count
            .saturating_add(counters.sampled_contended_count.load(Ordering::Relaxed));
        sampled_wait_ns_total = sampled_wait_ns_total
            .saturating_add(counters.sampled_wait_ns_total.load(Ordering::Relaxed));
        sampled_hold_ns_total = sampled_hold_ns_total
            .saturating_add(counters.sampled_hold_ns_total.load(Ordering::Relaxed));
        condvar_reacquisitions = condvar_reacquisitions
            .saturating_add(counters.condvar_reacquisitions.load(Ordering::Relaxed));
        sampled_condvar_wait_ns_total = sampled_condvar_wait_ns_total.saturating_add(
            counters
                .sampled_condvar_wait_ns_total
                .load(Ordering::Relaxed),
        );
        for index in 0..HISTOGRAM_BUCKETS {
            wait_histogram[index] = wait_histogram[index]
                .saturating_add(counters.wait_histogram[index].load(Ordering::Relaxed));
            hold_histogram[index] = hold_histogram[index]
                .saturating_add(counters.hold_histogram[index].load(Ordering::Relaxed));
            condvar_wait_histogram[index] = condvar_wait_histogram[index]
                .saturating_add(counters.condvar_wait_histogram[index].load(Ordering::Relaxed));
        }
    }

    let scale = |sampled: u64| {
        if sampled_acquisition_count == 0 {
            0
        } else {
            ((sampled as u128 * acquisition_count as u128) / sampled_acquisition_count as u128)
                .min(u64::MAX as u128) as u64
        }
    };

    MutexLockStatsSnapshot {
        name: lock_name(kind),
        acquisition_count,
        sampled_acquisition_count,
        sampled_contended_count,
        estimated_contended_count: scale(sampled_contended_count),
        sampled_wait_ns_total,
        estimated_wait_ns_total: scale(sampled_wait_ns_total),
        wait_p50_ns: histogram_quantile(&wait_histogram, 50, 100),
        wait_p95_ns: histogram_quantile(&wait_histogram, 95, 100),
        wait_p99_ns: histogram_quantile(&wait_histogram, 99, 100),
        sampled_hold_ns_total,
        estimated_hold_ns_total: scale(sampled_hold_ns_total),
        hold_p50_ns: histogram_quantile(&hold_histogram, 50, 100),
        hold_p95_ns: histogram_quantile(&hold_histogram, 95, 100),
        hold_p99_ns: histogram_quantile(&hold_histogram, 99, 100),
        condvar_reacquisitions,
        sampled_condvar_wait_ns_total,
        condvar_wait_p50_ns: histogram_quantile(&condvar_wait_histogram, 50, 100),
        condvar_wait_p95_ns: histogram_quantile(&condvar_wait_histogram, 95, 100),
        condvar_wait_p99_ns: histogram_quantile(&condvar_wait_histogram, 99, 100),
    }
}

#[cfg(feature = "bench")]
pub fn configure(sample_every: u64) {
    let registry = registry();
    registry.sample_every.store(0, Ordering::Release);
    for thread in registry.threads.lock().iter() {
        for counters in &thread.locks {
            counters.reset();
        }
    }
    registry.sample_every.store(sample_every, Ordering::Release);
}

#[cfg(not(feature = "bench"))]
pub fn configure(_sample_every: u64) {}

pub fn snapshot() -> Option<MutexLockDiagnosticsSnapshot> {
    #[cfg(feature = "bench")]
    {
        let registry = registry();
        let sample_every = registry.sample_every.load(Ordering::Acquire);
        if sample_every == 0 {
            return None;
        }
        let threads = registry.threads.lock();
        Some(MutexLockDiagnosticsSnapshot {
            sample_every,
            histogram: "log2_upper_bound_nanoseconds",
            acquisition_scope: "instrumented synchronous WAL resource/extent and MVCC publication call sites",
            locks: [
                MutexLockKind::ResourceLedger,
                MutexLockKind::ReservationOwner,
                MutexLockKind::ExtentState,
                MutexLockKind::ExtentPreparation,
                MutexLockKind::MvccPublication,
            ]
            .into_iter()
            .map(|kind| snapshot_lock(kind, &threads))
            .collect(),
        })
    }
    #[cfg(not(feature = "bench"))]
    {
        None
    }
}

pub(crate) struct MutexLockTimer {
    #[cfg(feature = "bench")]
    counters: Option<std::sync::Arc<ThreadCounters>>,
    #[cfg(feature = "bench")]
    kind: Option<MutexLockKind>,
    #[cfg(feature = "bench")]
    hold_started: Option<Instant>,
    #[cfg(feature = "bench")]
    held_ns: u64,
}

impl MutexLockTimer {
    fn inactive() -> Self {
        Self {
            #[cfg(feature = "bench")]
            counters: None,
            #[cfg(feature = "bench")]
            kind: None,
            #[cfg(feature = "bench")]
            hold_started: None,
            #[cfg(feature = "bench")]
            held_ns: 0,
        }
    }

    /// Call immediately before a Condvar wait releases this mutex.
    #[inline]
    pub(crate) fn pause_for_condvar(&mut self) -> Option<Instant> {
        #[cfg(feature = "bench")]
        if self.kind.is_some() {
            self.finish_hold_segment();
            return Some(Instant::now());
        }
        None
    }

    /// Call immediately after a Condvar wait has reacquired this mutex.
    #[inline]
    pub(crate) fn resume_after_condvar(&mut self, started: Option<Instant>) {
        #[cfg(feature = "bench")]
        if let (Some(kind), Some(started), Some(counters)) =
            (self.kind, started, self.counters.as_ref())
        {
            let wait_ns = elapsed_ns(started);
            let counters = counters.counter(kind);
            counters
                .condvar_reacquisitions
                .fetch_add(1, Ordering::Relaxed);
            counters
                .sampled_condvar_wait_ns_total
                .fetch_add(wait_ns, Ordering::Relaxed);
            record_histogram(&counters.condvar_wait_histogram, wait_ns);
            self.hold_started = Some(Instant::now());
        }
        #[cfg(not(feature = "bench"))]
        let _ = started;
    }

    #[cfg(feature = "bench")]
    fn finish_hold_segment(&mut self) {
        if let Some(started) = self.hold_started.take() {
            self.held_ns = self.held_ns.saturating_add(elapsed_ns(started));
        }
    }
}

impl Drop for MutexLockTimer {
    fn drop(&mut self) {
        #[cfg(feature = "bench")]
        {
            let (Some(kind), Some(thread_counters)) = (self.kind, self.counters.clone()) else {
                return;
            };
            self.finish_hold_segment();
            let counters = thread_counters.counter(kind);
            counters
                .sampled_hold_ns_total
                .fetch_add(self.held_ns, Ordering::Relaxed);
            record_histogram(&counters.hold_histogram, self.held_ns);
        }
    }
}

#[inline]
pub(crate) fn lock<'a, T>(
    mutex: &'a Mutex<T>,
    kind: MutexLockKind,
) -> (MutexLockTimer, MutexGuard<'a, T>) {
    #[cfg(feature = "bench")]
    {
        let registry = registry();
        let sample_every = registry.sample_every.load(Ordering::Relaxed);
        if sample_every == 0 {
            return (MutexLockTimer::inactive(), mutex.lock());
        }
        let sampled_counters = THREAD_COUNTERS.with(|thread_counters| {
            let mut thread_counters = thread_counters.borrow_mut();
            if thread_counters.is_none() {
                let counters = std::sync::Arc::new(ThreadCounters::new());
                registry
                    .threads
                    .lock()
                    .push(std::sync::Arc::clone(&counters));
                *thread_counters = Some(counters);
            }
            let counters = thread_counters
                .as_ref()
                .expect("thread counters initialized");
            let lock_counters = counters.counter(kind);
            let sequence = lock_counters
                .acquisition_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            sequence
                .is_multiple_of(sample_every)
                .then(|| std::sync::Arc::clone(counters))
        });
        let Some(thread_counters) = sampled_counters else {
            return (MutexLockTimer::inactive(), mutex.lock());
        };

        let counters = thread_counters.counter(kind);
        let started = Instant::now();
        let (guard, contended) = match mutex.try_lock() {
            Some(guard) => (guard, false),
            None => (mutex.lock(), true),
        };
        let wait_ns = elapsed_ns(started);
        counters
            .sampled_acquisition_count
            .fetch_add(1, Ordering::Relaxed);
        if contended {
            counters
                .sampled_contended_count
                .fetch_add(1, Ordering::Relaxed);
        }
        counters
            .sampled_wait_ns_total
            .fetch_add(wait_ns, Ordering::Relaxed);
        record_histogram(&counters.wait_histogram, wait_ns);
        (
            MutexLockTimer {
                counters: Some(thread_counters),
                kind: Some(kind),
                hold_started: Some(Instant::now()),
                held_ns: 0,
            },
            guard,
        )
    }
    #[cfg(not(feature = "bench"))]
    {
        let _ = kind;
        (MutexLockTimer::inactive(), mutex.lock())
    }
}

#[cfg(feature = "bench")]
fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(all(test, feature = "bench"))]
mod tests {
    use super::{HISTOGRAM_BUCKETS, histogram_bucket, histogram_quantile};

    #[test]
    fn histogram_quantiles_use_bucket_upper_bounds() {
        let mut histogram = [0; HISTOGRAM_BUCKETS];
        for value in [1, 2, 3, 100] {
            histogram[histogram_bucket(value)] += 1;
        }

        assert_eq!(histogram_quantile(&histogram, 50, 100), Some(4));
        assert_eq!(histogram_quantile(&histogram, 95, 100), Some(128));
        assert_eq!(histogram_quantile(&histogram, 99, 100), Some(128));
    }
}
