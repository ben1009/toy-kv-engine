// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let filter = &buf[..buf.len() - 1];
        let k = buf[buf.len() - 1];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.filter);
        buf.put_u8(self.k);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(key_hashes: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (key_hashes.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // h is every hash value in hashed keys
        for h in key_hashes {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            // set k bits
            for i in 0..k {
                let idx = (h as usize) % nbits;
                filter.set_bit(idx, true);
                h = h.wrapping_add(delta);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain the key, h is the hash of the key
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);

            for i in 0..self.k {
                let idx = (h as usize) % nbits;
                if !self.filter.get_bit(idx) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }

            true
        }
    }
}
