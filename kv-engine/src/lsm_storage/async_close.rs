//! One shutdown owner independent of the caller's Tokio blocking pool.
//!
//! A close waiter must not hold a blocking slot while draining admitted work:
//! those writes may need the same pool for memtable freeze or WAL rotation.
//! The shutdown thread owns the engine until close settles; all callers await
//! one saved result cooperatively, including after another waiter cancels.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use anyhow::{Result, anyhow};
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::LifecycleHandle;

#[derive(Default)]
pub(super) struct AsyncClose {
    attempt: Mutex<Option<Arc<CloseAttempt>>>,
}

#[derive(Default)]
pub(super) struct CloseAttempt {
    result: Mutex<Option<Result<(), Arc<anyhow::Error>>>>,
    completed: Notify,
}

/// Classification for a failure returned by [`super::KvEngine::close_async`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AsyncCloseErrorKind {
    /// A filesystem operation failed with this I/O error kind.
    Io(std::io::ErrorKind),
    /// A PITR manifest record may have been published but was not confirmed durable.
    PitrManifestPublishedButNotDurable,
    /// Whether a PITR manifest transition was published could not be determined.
    PitrManifestPublicationUnknown,
    /// The close failed with another error type.
    Other,
}

/// Shared async-close failure with a typed classification and its underlying close error.
#[derive(Debug)]
pub struct AsyncCloseError {
    kind: AsyncCloseErrorKind,
    source: Arc<anyhow::Error>,
}

impl AsyncCloseError {
    fn new(source: Arc<anyhow::Error>) -> Self {
        let kind = Self::classify(&source);

        Self { kind, source }
    }

    fn classify(source: &anyhow::Error) -> AsyncCloseErrorKind {
        if let Some(cached) = source.downcast_ref::<CachedCloseError>() {
            return cached.kind;
        }

        match source.downcast_ref::<super::PitrManifestPublicationError>() {
            Some(super::PitrManifestPublicationError::PublishedButNotDurable(_)) => {
                AsyncCloseErrorKind::PitrManifestPublishedButNotDurable
            }
            Some(super::PitrManifestPublicationError::Unknown { .. }) => {
                AsyncCloseErrorKind::PitrManifestPublicationUnknown
            }
            None => source
                .downcast_ref::<std::io::Error>()
                .map_or(AsyncCloseErrorKind::Other, |error| {
                    AsyncCloseErrorKind::Io(error.kind())
                }),
        }
    }

    /// Returns the stable classification for this close failure.
    pub fn kind(&self) -> AsyncCloseErrorKind {
        self.kind
    }

    /// Returns the underlying close error for detailed inspection.
    ///
    /// An async shutdown owner retains the original error chain. When a
    /// synchronous caller settles close first, its cache instead retains the
    /// typed classification and the formatted chain so that the first caller
    /// can receive its original error.
    pub fn source_error(&self) -> &anyhow::Error {
        &self.source
    }
}

impl Display for AsyncCloseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "engine close failed: {}", self.source)
    }
}

impl StdError for AsyncCloseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref().as_ref())
    }
}

#[derive(Clone, Debug)]
pub(super) struct CachedCloseError {
    kind: AsyncCloseErrorKind,
    detail: String,
}

impl CachedCloseError {
    pub(super) fn new(error: &anyhow::Error) -> Self {
        Self {
            kind: AsyncCloseError::classify(error),
            detail: format!("{error:#}"),
        }
    }
}

impl Display for CachedCloseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl StdError for CachedCloseError {}

impl AsyncClose {
    pub(super) fn start<F>(&self, lifecycle: &LifecycleHandle, close: F) -> Arc<CloseAttempt>
    where
        F: Fn() -> Result<()> + Send + 'static,
    {
        let mut current = self.attempt.lock();
        if let Some(attempt) = current.as_ref() {
            let failed = attempt.result.lock().as_ref().is_some_and(Result::is_err);
            // A rejected PITR precondition or thread-spawn failure can leave
            // admission open. Permit a later retry after that error; a real
            // shutdown attempt instead retains its terminal outcome.
            if !failed || lifecycle.ensure_open().is_err() {
                return Arc::clone(attempt);
            }
        }
        let attempt = Arc::new(CloseAttempt::default());
        *current = Some(Arc::clone(&attempt));
        let completion = Arc::clone(&attempt);
        let owner = std::thread::Builder::new()
            .name("kv-engine-close".into())
            .spawn(move || {
                // Borrow the closure so its shutdown owner remains alive until
                // the attempt's terminal result has been saved.
                let result = match catch_unwind(AssertUnwindSafe(&close)) {
                    Ok(result) => result,
                    Err(_) => Err(anyhow!("engine shutdown owner panicked")),
                };
                completion.finish(result);
            });
        if let Err(error) = owner {
            attempt.finish(Err(anyhow!(error).context("start engine shutdown owner")));
        }

        attempt
    }

    pub(super) fn owns_shutdown(&self) -> bool {
        self.attempt
            .lock()
            .as_ref()
            .is_some_and(|attempt| attempt.result.lock().is_none())
    }
}

impl CloseAttempt {
    fn finish(&self, result: Result<()>) {
        *self.result.lock() = Some(result.map_err(Arc::new));
        self.completed.notify_waiters();
    }

    pub(super) async fn wait(&self) -> Result<()> {
        let notified = self.completed.notified();
        tokio::pin!(notified);

        loop {
            // Register before checking the saved outcome so a completion
            // between the predicate check and await cannot lose its wakeup.
            notified.as_mut().enable();
            let result = self.result.lock().clone();
            if let Some(result) = result {
                return result.map_err(|error| anyhow::Error::new(AsyncCloseError::new(error)));
            }
            notified.as_mut().await;
            notified.set(self.completed.notified());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm_storage::{KvEngine, LsmStorageOptions};

    #[tokio::test(flavor = "current_thread")]
    async fn async_close_preserves_error_kind_after_synchronous_close() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database");
        let moved_path = directory.path().join("moved-database");
        let engine = KvEngine::open(&path, LsmStorageOptions::default_for_test()).unwrap();
        engine.put(b"key", b"value").unwrap();
        std::fs::rename(&path, &moved_path).unwrap();

        let synchronous_error = engine.close().expect_err("flush cannot use the old path");
        assert_eq!(
            synchronous_error
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        let error = engine
            .close_async()
            .await
            .expect_err("close failure is terminal");
        let close_error = error.downcast_ref::<AsyncCloseError>().unwrap();
        assert_eq!(
            close_error.kind(),
            AsyncCloseErrorKind::Io(std::io::ErrorKind::NotFound)
        );
        assert!(format!("{error:#}").contains(&synchronous_error.to_string()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_close_supports_moved_engine() {
        let directory = tempfile::tempdir().unwrap();
        let options = LsmStorageOptions::default_for_test();
        let engine = Arc::try_unwrap(KvEngine::open(directory.path(), options.clone()).unwrap())
            .ok()
            .expect("engine has one public owner");
        engine.put(b"key", b"value").unwrap();

        engine.close_async().await.unwrap();

        let reopened = KvEngine::open(directory.path(), options).unwrap();
        assert_eq!(reopened.get(b"key").unwrap().unwrap().as_ref(), b"value");
        reopened.close().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_close_of_moved_engine_finishes_after_caller_drop() {
        let directory = tempfile::tempdir().unwrap();
        let options = LsmStorageOptions::default_for_test();
        let engine = Arc::try_unwrap(KvEngine::open(directory.path(), options.clone()).unwrap())
            .ok()
            .expect("engine has one public owner");
        // A new Arc does not repair the constructor-time weak reference.
        let engine = Arc::new(engine);
        engine.put(b"key", b"value").unwrap();
        let lifecycle = engine.inner.lifecycle.clone();
        let guard = lifecycle.admit_write().unwrap();
        let mut close = Box::pin(engine.close_async());
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(close.as_mut(), context).is_pending());

            std::task::Poll::Ready(())
        })
        .await;
        drop(close);
        drop(engine);
        assert!(
            !lifecycle.is_closed(),
            "owned shutdown still has work to drain"
        );
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !lifecycle.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned shutdown finishes after its caller is dropped");

        let reopened = KvEngine::open(directory.path(), options).unwrap();
        assert_eq!(reopened.get(b"key").unwrap().unwrap().as_ref(), b"value");
        reopened.close().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_close_retries_a_rejected_pitr_precondition() {
        let directory = tempfile::tempdir().unwrap();
        let engine =
            KvEngine::open(directory.path(), LsmStorageOptions::default_for_test()).unwrap();
        engine.pitr_manifest_state.lock().mode = crate::pitr::manifest::PitrMode::Enabling;
        assert!(engine.close_async().await.is_err());
        assert!(engine.inner.lifecycle.ensure_open().is_ok());
        engine.pitr_manifest_state.lock().mode = crate::pitr::manifest::PitrMode::Disabled;
        engine.close_async().await.unwrap();
        assert!(engine.inner.lifecycle.is_closed());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_close_preserves_typed_errors_for_each_waiter() {
        let attempt = CloseAttempt::default();
        attempt.finish(Err(anyhow::Error::new(std::io::Error::other(
            "typed close failure",
        ))));

        for _ in 0..2 {
            let error = attempt.wait().await.expect_err("close should fail");
            let close_error = error
                .downcast_ref::<AsyncCloseError>()
                .expect("async close should expose its typed failure classification");
            assert_eq!(
                close_error.kind(),
                AsyncCloseErrorKind::Io(std::io::ErrorKind::Other)
            );
            assert!(
                close_error
                    .source_error()
                    .downcast_ref::<std::io::Error>()
                    .is_some()
            );
            assert!(format!("{error:#}").contains("typed close failure"));
        }
    }
}
