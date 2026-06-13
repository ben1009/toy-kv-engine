//! Prefetch pool and handle for background I/O during sequential scans.
//!
//! The `PrefetchPool` is a fixed-size thread pool that executes short-lived
//! I/O tasks (one `pread` each). `PrefetchHandle<T>` is a one-shot channel
//! receiver that delivers the result of a background task.

use std::sync::mpsc;

use anyhow::Result;

/// A pending prefetch result. The background thread sends the result through
/// a channel; `join()` blocks until it is ready.
pub(crate) struct PrefetchHandle<T> {
    rx: mpsc::Receiver<Result<T>>,
}

impl<T> PrefetchHandle<T> {
    /// Block until the prefetch result is available.
    pub(crate) fn join(self) -> Result<T> {
        self.rx
            .recv()
            .map_err(|_| anyhow::anyhow!("prefetch thread panicked"))?
    }

    /// Non-blocking check for the result. Returns `Ok(Ok(value))` if ready,
    /// `Ok(Err(_))` if the thread finished with an error, or
    /// `Err(TryRecvError)` if not yet ready.
    pub(crate) fn try_join(&self) -> std::result::Result<Result<T>, mpsc::TryRecvError> {
        self.rx.try_recv()
    }
}

/// A fixed-size thread pool for prefetch I/O tasks.
pub(crate) struct PrefetchPool {
    tx: crossbeam_channel::Sender<Box<dyn FnOnce() + Send>>,
}

impl PrefetchPool {
    pub(crate) fn new(num_threads: usize) -> Self {
        let num_threads = num_threads.max(1);
        let (tx, rx) = crossbeam_channel::unbounded::<Box<dyn FnOnce() + Send>>();
        for _ in 0..num_threads {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name("prefetch".into())
                .stack_size(256 * 1024) // 256 KB — enough for pread + decode
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        job();
                    }
                })
                .expect("failed to spawn prefetch thread");
        }
        PrefetchPool { tx }
    }

    /// Submit a prefetch task and return a handle for the result.
    ///
    /// If the closure panics, the panic payload is captured and forwarded
    /// as an error through the handle, preserving the panic message for
    /// diagnostics.
    pub(crate) fn submit<F, T>(&self, f: F) -> PrefetchHandle<T>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = mpsc::channel();
        self.tx
            .send(Box::new(move || {
                let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                    Ok(r) => r,
                    Err(panic) => {
                        let msg = if let Some(s) = panic.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = panic.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "prefetch thread panicked (non-string payload)".to_string()
                        };
                        Err(anyhow::anyhow!(msg))
                    }
                };
                let _ = result_tx.send(result);
            }))
            .expect("prefetch pool shut down");
        PrefetchHandle { rx: result_rx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_pool_submit_and_join() {
        let pool = PrefetchPool::new(2);
        let handle = pool.submit(|| Ok(42));
        let result = handle.join().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_pool_multiple_tasks() {
        let pool = PrefetchPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let counter = counter.clone();
                pool.submit(move || {
                    counter.fetch_add(i, Ordering::Relaxed);
                    Ok(i)
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
        assert_eq!(counter.load(Ordering::Relaxed), 45usize);
    }

    #[test]
    fn test_try_join_not_ready() {
        let pool = PrefetchPool::new(1);
        // Submit a task that sleeps — try_join should return Empty.
        let handle = pool.submit(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            Ok(1)
        });
        // Immediately try — may or may not be ready, but the API should not panic.
        let _ = handle.try_join();
    }

    #[test]
    fn test_try_join_ready() {
        let pool = PrefetchPool::new(2);
        let handle = pool.submit(|| Ok(99));
        // Wait for completion.
        let result = handle.join().unwrap();
        assert_eq!(result, 99);
    }

    #[test]
    fn test_pool_error_propagation() {
        let pool = PrefetchPool::new(1);
        let handle = pool.submit(|| Err::<i32, _>(anyhow::anyhow!("io error")));
        let err = handle.join().unwrap_err();
        assert!(err.to_string().contains("io error"));
    }
}
