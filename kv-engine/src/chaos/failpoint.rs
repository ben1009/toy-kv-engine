//! Deterministic failpoint injection for chaos testing (RFC 013 Phase 3).
//!
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

/// The core failpoint macro. Re-exported from the `fail` crate so injection
/// sites within this crate can use it via `crate::chaos::failpoint::fail_point!`.
pub(crate) use fail::fail_point;

/// Re-export failpoint configuration primitives for tests.
pub use fail::{FailScenario, cfg};
