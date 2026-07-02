//! Compatibility helpers for synchronous callers (tests, benchmarks, CLI tools)
//! during the async migration.  Prefer the async API in new code.
//!
//! Uses a thread-local Tokio `current_thread` runtime so callers do not pay
//! the cost of building a new runtime on every invocation.

use std::cell::RefCell;

thread_local! {
    static RUNTIME: RefCell<Option<tokio::runtime::Runtime>> = const { RefCell::new(None) };
}

fn with_runtime<T>(f: impl FnOnce(&tokio::runtime::Runtime) -> T) -> T {
    RUNTIME.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            *opt = Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build thread-local tokio runtime"),
            );
        }
        f(opt.as_ref().unwrap())
    })
}

/// Run an async future to completion on a thread-local `current_thread` runtime.
pub fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    with_runtime(|rt| rt.block_on(future))
}

/// Run a fallible async future to completion.
pub fn block_on_result<T, E>(
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E> {
    block_on(future)
}
