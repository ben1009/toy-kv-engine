pub mod block;
pub(crate) mod blocking_executor;
pub(crate) mod cache;
pub mod compact;
pub mod debug;
pub mod future_ext;
pub mod iterators;
pub mod key;
pub mod lsm_iterator;
pub mod lsm_storage;
pub mod manifest;
pub mod mem_table;
pub mod mvcc;
pub mod range_tombstone;
pub(crate) mod scan_trace;
pub mod table;
pub mod vlog;
pub mod wal;

#[cfg(feature = "chaos-testing")]
pub mod chaos;

pub use future_ext::block_on;

/// Initialize structured logging via logforth.
///
/// Reads `RUST_LOG` for level filtering (defaults to `info`).
/// Outputs structured JSON to stderr. Safe to call multiple times;
/// only the first call takes effect.
pub fn init_logging() {
    use logforth::append;
    use logforth::filter;
    use logforth::layout;

    let _ = logforth::starter_log::builder()
        .dispatch(|d| {
            d.filter(filter::rustlog::RustLogFilterBuilder::from_default_env_or("info").build())
                .append(append::Stderr::default().with_layout(layout::JsonLayout::default()))
        })
        .try_apply();
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod logforth_tests {
    #[test]
    fn init_logging_is_safe_to_call_twice() {
        crate::init_logging();
        crate::init_logging(); // second call should be a no-op via try_apply
        log::error!("test error from logforth");
        log::warn!("test warning from logforth");
        log::info!("test info from logforth");
        // If we get here without panicking, try_apply works correctly
        assert!(log::max_level() >= log::LevelFilter::Error);
    }
}
