//! Cache wrappers around TinyUFO.
//!
//! Provides [`BlockCache`] (with reverse-index for SST block invalidation)
//! and [`ValueCache`] (with byte-weight clamping and per-file key tracking).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use tinyufo::TinyUfo;

use crate::block::Block;

/// Weight divisor for the value cache.  A value of N bytes gets weight
/// `max(1, N / VALUE_WEIGHT_DIVISOR)`, keeping totals within u16 range
/// for a 64 MiB budget (64 MiB / 64 = 1 048 576 <= 65 535 * 16).
const VALUE_WEIGHT_DIVISOR: usize = 64;

// ---------------------------------------------------------------------------
// BlockCache
// ---------------------------------------------------------------------------

/// Block cache backed by TinyUFO with a reverse index for bulk invalidation
/// by SST id.
///
/// **Limitations:**
/// - TinyUFO does not expose eviction callbacks, so when blocks are evicted by the cache their keys
///   remain in the reverse index until the owning SST is compacted away.  In practice this is a
///   slow, bounded leak because the number of live SSTs (and their blocks) is finite.
/// - Concurrent misses on the same key are not coalesced (unlike moka's `get_with`).  Each thread
///   performs its own disk read.  This is safe because all closures are idempotent, but wastes I/O
///   under contention.
pub struct BlockCache {
    inner: TinyUfo<(usize, usize), Arc<Block>>,
    /// Reverse index: sst_id → keys cached for that SST.
    /// `HashSet` prevents duplicate entries on re-insert after eviction.
    sst_blocks: Mutex<HashMap<usize, HashSet<(usize, usize)>>>,
    /// Fast entry count — incremented on insert, decremented on invalidation.
    count: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: TinyUfo::new(cap, cap),
            sst_blocks: Mutex::new(HashMap::new()),
            count: AtomicU64::new(0),
        }
    }

    /// Cache-through block read.  On miss, calls `f` to read from disk,
    /// inserts the result, and registers the key in the reverse index.
    pub fn get_or_insert<F>(&self, sst_id: usize, block_idx: usize, f: F) -> Arc<Block>
    where
        F: FnOnce() -> Arc<Block>,
    {
        let key = (sst_id, block_idx);
        if let Some(v) = self.inner.get(&key) {
            return v;
        }
        let v = f();
        // Use `force_put` to bypass TinyLFU admission — guarantees the block
        // is cached even under cache pressure.  Hit-ratio cost is ~0.5pp.
        self.inner.force_put(key, v.clone(), 1);
        self.sst_blocks
            .lock()
            .entry(sst_id)
            .or_default()
            .insert(key);
        self.count.fetch_add(1, Ordering::Relaxed);
        v
    }

    /// Like [`get_or_insert`], but the closure returns `Result`.
    /// On error the cache is left unchanged and the error is propagated.
    pub fn try_get_with<F, E>(&self, sst_id: usize, block_idx: usize, f: F) -> Result<Arc<Block>, E>
    where
        F: FnOnce() -> Result<Arc<Block>, E>,
    {
        let key = (sst_id, block_idx);
        if let Some(v) = self.inner.get(&key) {
            return Ok(v);
        }
        let v = f()?;
        self.inner.force_put(key, v.clone(), 1);
        self.sst_blocks
            .lock()
            .entry(sst_id)
            .or_default()
            .insert(key);
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(v)
    }

    /// Remove all cached blocks belonging to the given SST ids.
    /// Called during compaction after SST files are deleted.
    ///
    /// Collects keys under the lock, then releases it before calling
    /// `inner.remove()` to avoid blocking concurrent cache misses.
    pub fn invalidate_ssts(&self, removed_ids: &HashSet<usize>) {
        let keys: Vec<_> = {
            let mut index = self.sst_blocks.lock();
            removed_ids
                .iter()
                .flat_map(|&id| index.remove(&id).into_iter().flatten())
                .collect()
        };
        let removed = keys.len() as u64;
        for key in keys {
            self.inner.remove(&key);
        }
        self.count.fetch_sub(removed, Ordering::Relaxed);
    }

    /// Approximate number of cached entries.  O(1), lock-free.
    pub fn entry_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// ValueCache
// ---------------------------------------------------------------------------

/// Value cache backed by TinyUFO with byte-weight clamping and per-file
/// key tracking for bulk invalidation.
///
/// **Limitations:** Same as [`BlockCache`] — evicted entries leave stale keys
/// in the reverse index until the owning vLog file is deleted.  Values larger
/// than ~4 MiB (u16::MAX * VALUE_WEIGHT_DIVISOR) are not cached to avoid
/// saturating the weight and overshooting the byte budget.
pub struct ValueCache {
    inner: TinyUfo<(u32, u64), Bytes>,
    /// Reverse index: file_id → cache keys for that file.
    /// `HashSet` prevents duplicate entries on re-insert after eviction.
    file_keys: Mutex<HashMap<u32, HashSet<(u32, u64)>>>,
}

impl ValueCache {
    /// Create a new value cache with the given byte budget.
    ///
    /// The budget is converted to TinyUFO weight units by dividing by
    /// [`VALUE_WEIGHT_DIVISOR`].
    pub fn new(byte_budget: u64) -> Self {
        let weight_budget = (byte_budget / VALUE_WEIGHT_DIVISOR as u64).max(1) as usize;
        // Estimate number of entries: assume average value is 4 KiB.
        let estimated_items = (byte_budget / 4096).max(16) as usize;
        Self {
            inner: TinyUfo::new(weight_budget, estimated_items),
            file_keys: Mutex::new(HashMap::new()),
        }
    }

    /// Compute the TinyUFO weight for a value using ceiling division.
    /// Returns `None` if the value is too large to represent accurately
    /// (would saturate to `u16::MAX`), meaning it should not be cached
    /// to avoid budget overshoot.
    fn value_weight(value: &Bytes) -> Option<u16> {
        let raw = value.len().div_ceil(VALUE_WEIGHT_DIVISOR);
        if raw > u16::MAX as usize {
            return None;
        }
        Some(raw.max(1) as u16)
    }

    /// Look up a cached value.
    pub fn get(&self, key: &(u32, u64)) -> Option<Bytes> {
        self.inner.get(key)
    }

    /// Insert a value into the cache, registering it in the per-file index.
    /// Values too large to represent accurately in u16 weight are skipped
    /// to avoid overshooting the byte budget.
    pub fn insert(&self, key: (u32, u64), value: Bytes) {
        let Some(w) = Self::value_weight(&value) else {
            return;
        };
        self.inner.force_put(key, value, w);
        self.file_keys.lock().entry(key.0).or_default().insert(key);
    }

    /// Remove all cached values belonging to `file_id`.
    ///
    /// Collects keys under the lock, then releases it before calling
    /// `inner.remove()` to avoid blocking concurrent cache misses.
    pub fn invalidate_file(&self, file_id: u32) {
        let keys: Vec<_> = {
            let mut index = self.file_keys.lock();
            index.remove(&file_id).into_iter().flatten().collect()
        };
        for key in keys {
            self.inner.remove(&key);
        }
    }
}
