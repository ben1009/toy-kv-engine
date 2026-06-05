use bytes::{BufMut, Bytes};

use super::{Block, SIZE_OF_U16};
use crate::key::{KeySlice, shared_bytes_from_slice};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// Offset in `self.data` where the first key's suffix begins.
    first_key_offset: usize,
    /// Length of the first key in bytes.
    first_key_len: u16,
    /// Whether the first key of this block has been recorded.
    /// Distinguishes a genuine empty first key from the initial state.
    has_first_key: bool,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            // Pre-allocate to avoid repeated Vec doubling during flush.
            // Typical entry is ~32 bytes, so block_size/32 is a good offset estimate.
            offsets: Vec::with_capacity(block_size / 32),
            data: Vec::with_capacity(block_size),
            block_size,
            first_key_offset: 0,
            first_key_len: 0,
            has_first_key: false,
        }
    }

    fn current_size(&self) -> usize {
        SIZE_OF_U16 /*num_of_elements*/ + self.data.len() /* key value pairs*/ + self.offsets.len() *SIZE_OF_U16
        // offsets
    }

    /// overlap_len returns the number of bytes that overlap with `first_key` in the block.
    fn overlap_len(&self, key: &[u8]) -> usize {
        let first_key =
            &self.data[self.first_key_offset..self.first_key_offset + self.first_key_len as usize];
        // 128 bytes = two cache lines; batch-compares before falling back to byte-by-byte.
        let chunk_size = 128;
        let offset = std::iter::zip(
            first_key.chunks_exact(chunk_size),
            key.chunks_exact(chunk_size),
        )
        .take_while(|(a, b)| a == b)
        .count()
            * chunk_size;

        offset
            + std::iter::zip(&first_key[offset..], &key[offset..])
                .take_while(|(a, b)| a == b)
                .count()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // The first entry always fits; skip the size check.
        if !self.is_empty()
            && self.current_size() + key.len() + value.len() + 3 * SIZE_OF_U16 >= self.block_size
        {
            return false;
        }

        let entry_start = self.data.len();
        self.offsets.push(self.data.len() as u16);

        let overlap = if self.is_empty() {
            0
        } else {
            self.overlap_len(key.raw_ref())
        };
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len() - overlap) as u16);
        self.data.put(&key.raw_ref()[overlap..]);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if !self.has_first_key {
            // First entry always has overlap == 0, so the suffix is the full key.
            self.has_first_key = true;
            self.first_key_offset = entry_start + 2 * SIZE_OF_U16;
            self.first_key_len = key.len() as u16;
        }

        true
    }

    /// Return the full key bytes of the `idx`-th entry.
    /// Used at block boundaries to produce `BlockMeta` first/last keys without
    /// maintaining a live clone of the latest key.
    pub(crate) fn key_at(&self, idx: usize) -> Bytes {
        assert!(idx < self.num_entries(), "key_at index out of bounds");

        let off = self.offsets[idx] as usize;
        let overlap = u16::from_be_bytes([self.data[off], self.data[off + 1]]) as usize;
        let suffix_len = u16::from_be_bytes([
            self.data[off + SIZE_OF_U16],
            self.data[off + SIZE_OF_U16 + 1],
        ]) as usize;
        let suffix_start = off + 2 * SIZE_OF_U16;
        let suffix = &self.data[suffix_start..suffix_start + suffix_len];

        if overlap == 0 {
            shared_bytes_from_slice(suffix)
        } else {
            let first_key = &self.data
                [self.first_key_offset..self.first_key_offset + self.first_key_len as usize];
            let mut out = Vec::with_capacity(overlap + suffix_len);
            out.extend_from_slice(&first_key[..overlap]);
            out.extend_from_slice(suffix);
            Bytes::from(out)
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Number of entries added to this block so far.
    pub(crate) fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: Bytes::from(self.data),
            offsets: self.offsets,
        }
    }
}
