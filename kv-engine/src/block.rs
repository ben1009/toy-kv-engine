mod builder;
mod iterator;

use std::mem;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const SIZE_OF_U16: usize = mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout defined in the block layout.
    /// Consumes self to reuse the `data` buffer — avoids an intermediate allocation.
    pub fn encode(mut self) -> Bytes {
        for o in &self.offsets {
            self.data.put_u16(*o);
        }
        self.data.put_u16(self.offsets.len() as u16);
        Bytes::from(self.data)
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
