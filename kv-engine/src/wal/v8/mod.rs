//! Staged primitives for the proposed RFC 026 WAL format.
//!
//! The format is registered in the format catalog, but remains unavailable for
//! reading or writing until the wire codec and recovery implementation are complete.
#![allow(dead_code)]

pub(crate) mod codec;
pub(crate) mod digest;

pub(crate) const WAL_V8_HEADER_LEN: usize = 4096;
