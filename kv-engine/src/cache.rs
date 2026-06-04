//! Cache wrappers around TinyUFO.
//!
//! Provides [`BlockCache`] (with reverse-index for SST block invalidation)
//! and [`ValueCache`] (with byte-weight clamping and per-file key tracking).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tinyufo::TinyUfo;

use crate::block::Block;

/// Weight divisor for the value cache.  A value of N bytes gets weight
/// `max(1, N / VALUE_WEIGHT_DIVISOR)`, keeping totals within u16 range
/// for a 64 MiB budget (64 MiB / 64 = 1 048 576 < 65 536 * 16).
const VALUE_WEIGHT_DIVISOR: usize = 64;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Cache-through lookup: return the cached value for `key`, or compute it
/// with `f`, insert it with the given `weight`, and return it.
///
/// This is **not** atomic under concurrent misses — the closure may run
/// more than once.  All existing call sites perform idempotent disk reads,
/// so duplicate work is safe.
pub fn try_get_with<K, V, E, F>(cache: &TinyUfo<K, V>, key: K, weight: u16, f: F) -> Result<V, E>
where
    K: std::hash::Hash + Clone + Eq,
    V: Clone + Send + Sync + 'static,
    F: FnOnce() -> Result<V, E>,
{
    if let Some(v) = cache.get(&key) {
        return Ok(v);
    }
    let v = f()?;
    cache.put(key, v.clone(), weight);
    Ok(v)
}

// ---------------------------------------------------------------------------
// BlockCache
// ---------------------------------------------------------------------------

/// Block cache backed by TinyUFO with a reverse index for bulk invalidation
/// by SST id.
///
/// **Limitation:** TinyUFO does not expose eviction callbacks, so when blocks
/// are evicted by the cache their keys remain in the reverse index until the
/// owning SST is compacted away.  In practice this is a slow, bounded leak
/// because the number of live SSTs (and their blocks) is finite.
pub struct BlockCache {
    inner: TinyUfo<(usize, usize), Arc<Block>>,
    /// Reverse index: sst_id → keys cached for that SST.
    /// `HashSet` prevents duplicate entries on re-insert after eviction.
    sst_blocks: Mutex<HashMap<usize, HashSet<(usize, usize)>>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: TinyUfo::new(cap, cap),
            sst_blocks: Mutex::new(HashMap::new()),
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
        self.inner.put(key, v.clone(), 1);
        self.sst_blocks
            .lock()
            .entry(sst_id)
            .or_default()
            .insert(key);
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
        self.inner.put(key, v.clone(), 1);
        self.sst_blocks
            .lock()
            .entry(sst_id)
            .or_default()
            .insert(key);
        Ok(v)
    }

    /// Remove all cached blocks belonging to the given SST ids.
    /// Called during compaction after SST files are deleted.
    pub fn invalidate_ssts(&self, removed_ids: &HashSet<usize>) {
        let mut index = self.sst_blocks.lock();
        for &sst_id in removed_ids {
            if let Some(keys) = index.remove(&sst_id) {
                for key in keys {
                    self.inner.remove(&key);
                }
            }
        }
    }

    /// Approximate number of cached entries (tracked via the reverse index).
    /// May overcount if blocks were evicted from the cache but their SST
    /// has not yet been compacted away.
    pub fn entry_count(&self) -> u64 {
        self.sst_blocks
            .lock()
            .values()
            .map(|v| v.len() as u64)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// ValueCache
// ---------------------------------------------------------------------------

/// Value cache backed by TinyUFO with byte-weight clamping and per-file
/// key tracking for bulk invalidation.
///
/// **Limitation:** Same as [`BlockCache`] — evicted entries leave stale keys
/// in the reverse index until the owning vLog file is deleted.
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

    fn value_weight(value: &Bytes) -> u16 {
        ((value.len() / VALUE_WEIGHT_DIVISOR).max(1)).min(u16::MAX as usize) as u16
    }

    /// Look up a cached value.
    pub fn get(&self, key: &(u32, u64)) -> Option<Bytes> {
        self.inner.get(key)
    }

    /// Insert a value into the cache, registering it in the per-file index.
    pub fn insert(&self, key: (u32, u64), value: Bytes) {
        let w = Self::value_weight(&value);
        self.inner.put(key, value, w);
        self.file_keys.lock().entry(key.0).or_default().insert(key);
    }

    /// Remove all cached values belonging to `file_id`.
    pub fn invalidate_file(&self, file_id: u32) {
        let mut index = self.file_keys.lock();
        if let Some(keys) = index.remove(&file_id) {
            for key in keys {
                self.inner.remove(&key);
            }
        }
    }
}
