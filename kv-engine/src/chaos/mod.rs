//! Chaos-testing harness for kv-engine (RFC 013).
//!
//! This module provides the building blocks for process-level crash/chaos tests:
//! a control log for external operation tracking, an oracle for post-crash state
//! reconciliation, and deterministic scenario generators.
//!
//! All code here is gated behind `#[cfg(feature = "chaos-testing")]` and is never
//! compiled into production builds.

pub mod control_log;
pub mod oracle;
pub mod scenarios;

/// File name for the test control log written by the child process.
pub const CONTROL_LOG_FILENAME: &str = "chaos_control.log";
