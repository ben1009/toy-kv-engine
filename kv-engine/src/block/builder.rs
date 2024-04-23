use bytes::BufMut;

use super::{Block, SIZE_OF_U16};
use crate::key::{KeySlice, KeyVec};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn current_size(&self) -> usize {
        SIZE_OF_U16 /*num_of_elements*/ + self.data.len() /* key value pairs*/ + self.offsets.len() *SIZE_OF_U16
        // offsets
    }

    /// overlap_len returns the number of bytes that overlap with `first_key` in the block.
    fn overlap_len(&self, key: &[u8]) -> usize {
        let mut ret = 0;
        while ret < self.first_key.len()
            && ret < key.len()
            && self.first_key.raw_ref()[ret] == key[ret]
        {
            ret += 1;
        }

        ret
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // if first data, skip check
        if !self.is_empty()
            && self.current_size() + key.len() + value.len() + 3 * SIZE_OF_U16 >= self.block_size
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let overlap = self.overlap_len(key.raw_ref());
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len() - overlap) as u16);
        self.data.put(&key.raw_ref()[overlap..]);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
