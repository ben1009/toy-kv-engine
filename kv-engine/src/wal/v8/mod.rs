//! RFC 026 WAL with composable BLAKE3 prefix digests.
//!
//! V8 is the default for newly enabled PITR. Dedicated readers and writers
//! preserve the embedded-frontier recovery and compatibility contracts.
#![allow(dead_code)]

pub(crate) mod codec;
pub(crate) mod digest;
pub(crate) mod install;
pub(crate) mod leader;
pub(crate) mod parallel;
pub(crate) mod prepare;
pub(crate) mod recovery;
pub(crate) mod resource;

pub(crate) const WAL_V8_HEADER_LEN: usize = 4096;
