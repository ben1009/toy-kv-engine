use std::hash::{BuildHasher, Hasher};

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Deterministic ahash state for bloom filter hashing.
static AHASH_STATE: std::sync::LazyLock<ahash::RandomState> =
    std::sync::LazyLock::new(|| ahash::RandomState::with_seed(0));

/// Fast 32-bit hash for bloom filter keys. Uses ahash (AES-NI accelerated)
/// Uses AES-NI acceleration for ~2-3x better throughput on modern CPUs.
pub fn hash_key(key: &[u8]) -> u32 {
    let mut hasher = AHASH_STATE.build_hasher();
    hasher.write(key);

    hasher.finish() as u32
}

/// Implements a bloom filter
#[derive(Debug)]
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
        let size = -(entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(key_hashes: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        let nbits = (key_hashes.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // h is every hash value in hashed keys
        for h in key_hashes {
            let mut h = *h;
            let delta = h.rotate_left(15);
            // set k bits
            for _i in 0..k {
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
            let delta = h.rotate_left(15);

            for _i in 0..self.k {
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

/// A bloom filter that supports incremental insertion (one hash at a time).
/// Used for memtable negative lookups — avoids skiplist epoch pin overhead
/// on cache misses.
///
/// # Thread Safety
///
/// Uses `Vec<AtomicU8>` for the filter bits:
/// - `push_hash` sets bits via `fetch_or(Ordering::Release)` — safe from the writer thread.
/// - `may_contain_hash` reads bits via `load(Ordering::Acquire)` — safe from reader threads.
///
/// Acquire/Release ordering ensures that if a reader sees any bloom bit set by `push_hash`,
/// all prior stores (including other bits from the same call and the skiplist insert that
/// follows) are visible. The bloom filter is always updated BEFORE the skiplist insert
/// (see `put_raw_batch`), so the only possible incorrect state is a false positive (bloom
/// says "contained" for a key not yet in the skiplist), which just causes an unnecessary
/// skiplist probe — harmless.
pub struct IncrementalBloom {
    filter: Vec<std::sync::atomic::AtomicU8>,
    k: u32,
    nbits: usize,
}

impl IncrementalBloom {
    /// Create a new incremental bloom filter sized for `expected_entries` with
    /// a target false positive rate.
    pub fn new(expected_entries: usize, false_positive_rate: f64) -> Self {
        let expected_entries = expected_entries.max(1); // Avoid division by zero
        let bits_per_key = Bloom::bloom_bits_per_key(expected_entries, false_positive_rate);
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        let nbits = (expected_entries * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8;
        let filter = (0..nbytes)
            .map(|_| std::sync::atomic::AtomicU8::new(0))
            .collect();
        Self { filter, k, nbits }
    }

    /// Add a key hash to the bloom filter.
    /// Uses `fetch_or(Release)` — safe to call concurrently with `may_contain_hash`.
    pub fn push_hash(&self, mut h: u32) {
        let delta = h.rotate_left(15);
        for _ in 0..self.k {
            let idx = (h as usize) % self.nbits;
            let pos = idx / 8;
            let offset = idx % 8;
            self.filter[pos].fetch_or(1 << offset, std::sync::atomic::Ordering::Release);
            h = h.wrapping_add(delta);
        }
    }

    /// Check if the bloom filter may contain this hash.
    /// Uses `load(Acquire)` — safe to call concurrently with `push_hash`.
    /// Acquire/Release ensures that if a reader sees any bit set by `push_hash`,
    /// all prior stores (including other bits from the same `push_hash` call)
    /// are visible. This eliminates false negatives on ARM/POWER.
    pub fn may_contain_hash(&self, mut h: u32) -> bool {
        let delta = h.rotate_left(15);
        for _ in 0..self.k {
            let idx = (h as usize) % self.nbits;
            let pos = idx / 8;
            let offset = idx % 8;
            let byte = self.filter[pos].load(std::sync::atomic::Ordering::Acquire);
            if byte & (1 << offset) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}
