pub mod block;
pub(crate) mod cache;
pub mod compact;
pub mod debug;
pub mod iterators;
pub mod key;
pub mod lsm_iterator;
pub mod lsm_storage;
pub mod manifest;
pub mod mem_table;
pub mod mvcc;
pub mod range_tombstone;
pub mod table;
pub mod vlog;
pub mod wal;

#[cfg(test)]
mod tests;

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
            d.filter(filter::rustlog::RustLogFilterBuilder::from_default_env().build())
                .append(append::Stderr::default().with_layout(layout::JsonLayout::default()))
        })
        .try_apply();
}
