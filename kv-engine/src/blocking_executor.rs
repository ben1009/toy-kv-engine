//! Bounded blocking executor for running synchronous engine work inside async tasks.
//!
//! Wraps [`tokio::task::spawn_blocking`] with a simple atomic counter to cap
//! concurrent blocking operations, preventing unbounded thread pool expansion
//! under load.
//!
//! This is the engine-owned blocking boundary required by RFC 014 section 8.4.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Default maximum number of concurrent blocking operations.
pub(crate) const DEFAULT_MAX_BLOCKING_THREADS: usize = 512;

/// A bounded blocking executor that runs synchronous closures via
/// [`tokio::task::spawn_blocking`], with concurrency limited by an atomic counter.
///
/// This ensures the engine owns the blocking boundary — callers never need to
/// wrap engine calls in their own `spawn_blocking`.
#[derive(Clone)]
pub(crate) struct BlockingExecutor {
    max_blocking: usize,
    active: Arc<AtomicUsize>,
}

/// RAII guard that decrements the active count on drop.
struct DecrementOnDrop(Arc<AtomicUsize>);

impl Drop for DecrementOnDrop {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

impl BlockingExecutor {
    /// Create a new executor with the given maximum concurrent blocking tasks.
    pub fn new(max_blocking: usize) -> Self {
        Self {
            max_blocking,
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Default executor with [`DEFAULT_MAX_BLOCKING_THREADS`] concurrent slots.
    pub fn with_default() -> Self {
        Self::new(DEFAULT_MAX_BLOCKING_THREADS)
    }

    /// Run a synchronous closure on the blocking thread pool.
    ///
    /// Waits (cooperatively via `yield_now`) until fewer than `max_blocking`
    /// tasks are active, then spawns the closure via `spawn_blocking`.
    ///
    /// # Panics
    ///
    /// Panics if the blocking task panics.
    pub async fn run<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.acquire_slot().await;
        let active = Arc::clone(&self.active);

        tokio::task::spawn_blocking(move || {
            let _guard = DecrementOnDrop(active);
            f()
        })
        .await
        .expect("blocking task panicked")
    }

    /// Run a fallible synchronous closure on the blocking thread pool.
    ///
    /// Like [`run`], but propagates the closure's `Result`.
    pub async fn run_result<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.acquire_slot().await;
        let active = Arc::clone(&self.active);

        tokio::task::spawn_blocking(move || {
            let _guard = DecrementOnDrop(active);
            f()
        })
        .await
        .expect("blocking task panicked")
    }

    /// Run lifecycle maintenance on the blocking pool. Ordinary work leaves
    /// one slot available whenever the executor has more than one slot. A
    /// maintenance operation may release resources awaited by ordinary work.
    pub async fn run_result_with_reserved_capacity<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.acquire_slot_below(self.max_blocking).await;
        let active = Arc::clone(&self.active);

        tokio::task::spawn_blocking(move || {
            let _guard = DecrementOnDrop(active);
            f()
        })
        .await
        .expect("blocking task panicked")
    }

    pub async fn run_result_cancelable<F, T, E>(
        &self,
        cancelled: &AtomicBool,
        f: F,
    ) -> Result<Option<T>, E>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        if !self.acquire_slot_cancelable(cancelled).await {
            return Ok(None);
        }
        if cancelled.load(Ordering::Acquire) {
            self.active.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }
        let active = Arc::clone(&self.active);

        tokio::task::spawn_blocking(move || {
            let _guard = DecrementOnDrop(active);
            f()
        })
        .await
        .expect("blocking task panicked")
        .map(Some)
    }

    /// Wait until we are under the concurrency limit, then increment.
    async fn acquire_slot(&self) {
        // Keep one slot available for lifecycle maintenance when possible.
        // A source-spool waiter can occupy a blocking thread until that same
        // maintenance reclaims its WAL and releases quota.
        let limit = if self.max_blocking > 1 {
            self.max_blocking - 1
        } else {
            self.max_blocking
        };
        self.acquire_slot_below(limit).await;
    }

    async fn acquire_slot_below(&self, limit: usize) {
        loop {
            let current = self.active.load(Ordering::Acquire);
            if current < limit
                && self
                    .active
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    async fn acquire_slot_cancelable(&self, cancelled: &AtomicBool) -> bool {
        let limit = if self.max_blocking > 1 {
            self.max_blocking - 1
        } else {
            self.max_blocking
        };
        loop {
            if cancelled.load(Ordering::Acquire) {
                return false;
            }
            let current = self.active.load(Ordering::Acquire);
            if current < limit
                && self
                    .active
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                return true;
            }
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn hold_all_slots_for_test(&self) -> HeldBlockingSlots {
        self.active.fetch_add(self.max_blocking, Ordering::AcqRel);

        HeldBlockingSlots {
            active: Arc::clone(&self.active),
            slots: self.max_blocking,
        }
    }

    /// Check available slots without blocking.
    #[allow(dead_code)]
    pub fn available_permits(&self) -> usize {
        self.max_blocking
            .saturating_sub(self.active.load(Ordering::Acquire))
    }
}

#[cfg(test)]
pub(crate) struct HeldBlockingSlots {
    active: Arc<AtomicUsize>,
    slots: usize,
}

#[cfg(test)]
impl Drop for HeldBlockingSlots {
    fn drop(&mut self) {
        self.active.fetch_sub(self.slots, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::BlockingExecutor;

    #[tokio::test(flavor = "current_thread")]
    async fn reserved_capacity_runs_maintenance_when_regular_slots_are_full() {
        let executor = BlockingExecutor::new(2);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();

        let regular_executor = executor.clone();
        let regular = tokio::spawn(async move {
            regular_executor
                .run(move || {
                    started_tx.send(()).unwrap();
                    release_rx.blocking_recv().unwrap();
                })
                .await;
        });
        started_rx.await.unwrap();

        let queued_executor = executor.clone();
        let queued = tokio::spawn(async move { queued_executor.run(|| ()).await });
        tokio::task::yield_now().await;
        assert!(
            !queued.is_finished(),
            "ordinary work must leave maintenance capacity free"
        );

        tokio::time::timeout(
            Duration::from_secs(1),
            executor.run_result_with_reserved_capacity(|| Ok::<_, anyhow::Error>(())),
        )
        .await
        .expect("maintenance must acquire reserved capacity")
        .unwrap();
        assert!(
            !queued.is_finished(),
            "ordinary work must not consume the reserved slot"
        );

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), regular)
            .await
            .expect("regular task must finish after release")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), queued)
            .await
            .expect("queued regular task must acquire its slot after release")
            .unwrap();
        assert_eq!(executor.available_permits(), 2);
    }
}
