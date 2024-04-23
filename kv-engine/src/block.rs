#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::mem;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
use nom::AsBytes;

pub const SIZE_OF_U16: usize = mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut ret = vec![];
        ret.put(self.data.as_bytes());
        for o in &self.offsets {
            ret.put_u16(*o);
        }
        ret.put_u16(self.offsets.len() as u16);

        Bytes::from(ret)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZE_OF_U16..]).get_u16() as usize;
        let offset = data.len() - SIZE_OF_U16 - num_of_elements * SIZE_OF_U16;

        let datas = data[0..offset].to_vec();
        let offsets = data[offset..data.len() - SIZE_OF_U16]
            .chunks(SIZE_OF_U16)
            .map(|mut x| x.get_u16())
            .collect();

        Self {
            data: datas,
            offsets,
        }
    }
}
