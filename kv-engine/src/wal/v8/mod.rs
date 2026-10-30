//! Staged primitives for the proposed RFC 026 WAL format.
//!
//! The format is registered in the format catalog, but remains unavailable to
//! production readers and writers until persistence, compatibility, and
//! qualification gates are complete.
#![allow(dead_code)]

pub(crate) mod codec;
pub(crate) mod digest;
pub(crate) mod install;
pub(crate) mod leader;
pub(crate) mod prepare;
pub(crate) mod recovery;

pub(crate) const WAL_V8_HEADER_LEN: usize = 4096;
