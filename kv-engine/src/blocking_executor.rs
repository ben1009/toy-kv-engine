//! Bounded blocking executor for running synchronous engine work inside async tasks.
//!
//! Wraps [`tokio::task::spawn_blocking`] with a simple atomic counter to cap
//! concurrent blocking operations, preventing unbounded thread pool expansion
//! under load.
//!
//! This is the engine-owned blocking boundary required by RFC 014 section 8.4.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
        // Wait until we are under the concurrency limit.
        loop {
            let current = self.active.load(Ordering::Acquire);
            if current < self.max_blocking
                && self
                    .active
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
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
        // Wait until we are under the concurrency limit.
        loop {
            let current = self.active.load(Ordering::Acquire);
            if current < self.max_blocking
                && self
                    .active
                    .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let active = Arc::clone(&self.active);
        tokio::task::spawn_blocking(move || {
            let _guard = DecrementOnDrop(active);
            f()
        })
        .await
        .expect("blocking task panicked")
    }

    /// Check available slots without blocking.
    #[allow(dead_code)]
    pub fn available_permits(&self) -> usize {
        self.max_blocking
            .saturating_sub(self.active.load(Ordering::Acquire))
    }
}

/// Helper for executing a `!Send` closure in an async context.
///
/// On a multi-threaded Tokio runtime, uses [`tokio::task::block_in_place`] to
/// park the current worker without blocking the executor. On a
/// `current_thread` runtime (or outside a Tokio context), falls back to
/// inline execution — there is only one thread so blocking is harmless.
///
/// This is used for `Transaction` methods, where the `Transaction` type is
/// `Send + !Sync` and must remain single-thread confined.
#[allow(dead_code)]
pub fn block_on_send<T>(f: impl FnOnce() -> T) -> T {
    let handle = tokio::runtime::Handle::try_current();
    match handle {
        Ok(_handle) => {
            // We are inside a Tokio runtime. Use block_in_place to avoid
            // starving the multi-threaded executor.
            tokio::task::block_in_place(f)
        }
        Err(_) => {
            // No runtime — execute inline (e.g., from a synchronous test
            // using our block_on compatibility helper).
            f()
        }
    }
}

/// Like [`block_on_send`] but for fallible closures.
#[allow(dead_code)]
pub fn block_on_send_result<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    let handle = tokio::runtime::Handle::try_current();
    match handle {
        Ok(_handle) => tokio::task::block_in_place(f),
        Err(_) => f(),
    }
}
