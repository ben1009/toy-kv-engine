use std::sync::Arc;

use bytes::Buf;

use super::{Block, SIZE_OF_U16};
use crate::key::{KeySlice, KeyVec};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = Self::get_first_key(block.clone());

        Self {
            block,
            key: first_key.clone(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    fn get_first_key(block: Arc<Block>) -> KeyVec {
        let mut data = &block.data[0..];
        data.get_u16(); // skip the first key overlap_len
        let key_len = data.get_u16() as usize;

        KeyVec::from_vec(data[..key_len].to_vec())
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut ret = BlockIterator::new(block.clone());
        ret.seek_to_first();

        ret
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut ret = Self::new(block.clone());
        ret.seek_to_key(key);
        ret
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns the value as `Bytes` — zero-copy slice into the cached block.
    /// The returned `Bytes` shares the block's underlying buffer via reference
    /// counting. No heap allocation occurs.
    pub fn value_bytes(&self) -> bytes::Bytes {
        self.block
            .data_slice(self.value_range.0..self.value_range.1)
    }

    /// Returns the raw value bytes including any kind prefix.
    pub fn raw_value(&self) -> &[u8] {
        self.value()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// idx is the index of block.offsets
    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        // if invalid, set is_valid state by reset values
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[self.idx] as usize;
        let mut data = &self.block.data[offset..];
        let overlap_len = data.get_u16() as usize;
        let ret_key_len = data.get_u16() as usize;
        let ret_key = &data[..ret_key_len];
        self.key.clear();
        self.key.append(&self.first_key.raw_ref()[..overlap_len]);
        self.key.append(ret_key);
        data.advance(ret_key_len);
        let value_len = data.get_u16() as usize;
        self.value_range = (
            offset + SIZE_OF_U16 * 2 + ret_key_len + SIZE_OF_U16,
            offset + SIZE_OF_U16 * 2 + ret_key_len + SIZE_OF_U16 + value_len,
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Compare the entry at `idx` against `target` without reconstructing the
    /// full key.  Reads only the overlap length and suffix from the block data,
    /// then compares prefix (from `first_key`) followed by suffix — avoiding the
    /// `key.clear()` + 2× `key.append()` + `Key::from_slice` overhead per probe.
    fn cmp_entry_at(&self, idx: usize, target: &[u8]) -> std::cmp::Ordering {
        let offset = self.block.offsets[idx] as usize;
        let remaining = &self.block.data[offset..];
        assert!(
            remaining.len() >= SIZE_OF_U16 * 2,
            "block entry at offset {offset} too short: need {} bytes, got {}",
            SIZE_OF_U16 * 2,
            remaining.len()
        );
        let mut data = remaining;
        let overlap_len = data.get_u16() as usize;
        let suffix_len = data.get_u16() as usize;
        assert!(
            data.len() >= suffix_len,
            "block entry suffix overflows: need {suffix_len} bytes, got {}",
            data.len()
        );
        let suffix = &data[..suffix_len];

        // Compare the prefix portion (shared with first_key).
        // If the target is shorter than the overlap, compare only the
        // overlapping prefix bytes — the entry's full key is longer, so it
        // is greater.
        let prefix_cmp_len = overlap_len
            .min(target.len())
            .min(self.first_key.raw_ref().len());
        match self.first_key.raw_ref()[..prefix_cmp_len].cmp(&target[..prefix_cmp_len]) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        // If the target is shorter than the overlap length, the entry's
        // prefix already exceeds the target.
        if target.len() < overlap_len {
            return std::cmp::Ordering::Greater;
        }
        // Compare the suffix portion
        suffix.cmp(&target[overlap_len..])
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let raw = key.raw_ref();
        let n = self.block.offsets.len();
        let mut lo = 0;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.cmp_entry_at(mid, raw) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    lo = mid;
                    break;
                }
            }
        }

        self.seek_to_idx(lo);
    }
}
