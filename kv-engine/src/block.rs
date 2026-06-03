mod builder;
mod iterator;

use std::mem;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub const SIZE_OF_U16: usize = mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    pub(crate) data: Bytes,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout defined in the block layout.
    /// Consumes self to reuse the `data` buffer — avoids an intermediate allocation.
    pub fn encode(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(
            self.data.len() + (self.offsets.len() + 1) * SIZE_OF_U16,
        );
        buf.put(self.data);
        for o in &self.offsets {
            buf.put_u16(*o);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZE_OF_U16..]).get_u16() as usize;
        let offset = data.len() - SIZE_OF_U16 - num_of_elements * SIZE_OF_U16;

        let datas = Bytes::copy_from_slice(&data[0..offset]);
        let offsets = data[offset..data.len() - SIZE_OF_U16]
            .chunks(SIZE_OF_U16)
            .map(|mut x| x.get_u16())
            .collect();

        Self {
            data: datas,
            offsets,
        }
    }

    /// Returns a zero-copy slice of the block data.
    /// The returned `Bytes` shares the underlying buffer via reference counting.
    #[inline]
    pub fn data_slice(&self, range: std::ops::Range<usize>) -> Bytes {
        self.data.slice(range)
    }
}
