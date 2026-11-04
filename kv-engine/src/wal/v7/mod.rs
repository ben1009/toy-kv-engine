//! PITR WAL V7 codecs, recovery, reservations and Leader compatibility writer.
//!
//! V7 remains supported for existing segments and explicit compatibility
//! selection. New PITR epochs default to the V8 format defined by RFC 026.
#![allow(dead_code)]

pub(crate) mod admission;
pub(crate) mod allocator;
pub(crate) mod codec;
pub(crate) mod install;
pub(crate) mod leader;
pub(crate) mod recovery;

#[cfg(test)]
mod model;
