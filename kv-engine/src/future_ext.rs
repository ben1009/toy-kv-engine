use std::{fmt::Debug, future::Future};

use tokio::runtime::Builder;

/// Compatibility helpers for call sites that still need to synchronously wait
/// on async engine methods during the phase-1 migration.
///
/// The engine surface itself is async; this trait only exists so legacy tests
/// and binaries can keep compiling while we convert their call sites.
pub trait FutureResultExt<T, E>: Future<Output = std::result::Result<T, E>> + Sized {
    fn into_result(self) -> std::result::Result<T, E> {
        build_runtime().block_on(self)
    }

    fn unwrap(self) -> T
    where
        E: Debug,
    {
        self.into_result().unwrap()
    }

    fn expect(self, msg: &str) -> T
    where
        E: Debug,
    {
        self.into_result().expect(msg)
    }

    fn unwrap_err(self) -> E
    where
        T: Debug,
    {
        self.into_result().unwrap_err()
    }

    fn ok(self) -> Option<T> {
        self.into_result().ok()
    }

    fn is_ok(self) -> bool
    where
        E: Debug,
    {
        self.into_result().is_ok()
    }

    fn is_err(self) -> bool
    where
        E: Debug,
    {
        self.into_result().is_err()
    }
}

impl<F, T, E> FutureResultExt<T, E> for F where F: Future<Output = std::result::Result<T, E>> + Sized
{}

fn build_runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build compatibility runtime")
}
