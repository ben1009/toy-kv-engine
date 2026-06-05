//! Cache wrappers around TinyUFO.
//!
//! Provides [`BlockCache`] (with reverse-index for SST block invalidation)
//! and [`ValueCache`] (with byte-weight clamping and per-file key tracking).

use std::collections::HashSet;

use ahash::{AHashMap, AHashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use tinyufo::TinyUfo;

use crate::block::Block;

/// Cache key for block cache entries: (sst_id, block_idx).
type BlockKey = (usize, usize);
/// Per-key lock map for single-flight miss coalescing.
type WaiterMap = Mutex<AHashMap<BlockKey, Arc<Mutex<()>>>>;

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
/// - If `invalidate_ssts` runs while a `get_or_insert` closure is in flight for the same SST, the
///   loading thread will re-insert the block and its reverse-index entry after invalidation
///   completes.  This is a bounded leak (one entry per concurrent in-flight load) and the orphaned
///   block will be evicted by TinyUFO under memory pressure.
pub struct BlockCache {
    inner: TinyUfo<BlockKey, Arc<Block>>,
    /// Reverse index: sst_id → keys cached for that SST.
    /// `AHashSet` prevents duplicate entries on re-insert after eviction.
    sst_blocks: Mutex<AHashMap<usize, AHashSet<BlockKey>>>,
    /// Per-key locks to coalesce concurrent misses (single-flight).
    /// Prevents multiple threads from loading the same block from disk.
    waiters: WaiterMap,
    /// Fast entry count — incremented on insert, decremented on invalidation.
    count: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: TinyUfo::new(cap, cap),
            sst_blocks: Mutex::new(AHashMap::new()),
            waiters: Mutex::new(AHashMap::new()),
            count: AtomicU64::new(0),
        }
    }

    /// Cache-through block read.  On miss, calls `f` to read from disk,
    /// inserts the result, and registers the key in the reverse index.
    ///
    /// Concurrent misses on the same key are coalesced: only the first
    /// thread runs `f`; others wait and read from cache.
    pub fn get_or_insert<F>(&self, sst_id: usize, block_idx: usize, f: F) -> Arc<Block>
    where
        F: FnOnce() -> Arc<Block>,
    {
        let key = (sst_id, block_idx);
        if let Some(v) = self.inner.get(&key) {
            return v;
        }
        // Single-flight: acquire a per-key lock so only one thread loads.
        let waiter = {
            let mut w = self.waiters.lock();
            w.entry(key).or_default().clone()
        };
        let _guard = waiter.lock();
        // Double-check after acquiring the per-key lock.
        if let Some(v) = self.inner.get(&key) {
            // Only remove if we own this exact waiter (not a newer one).
            let mut w = self.waiters.lock();
            if w.get(&key).is_some_and(|cur| Arc::ptr_eq(cur, &waiter)) {
                w.remove(&key);
            }
            return v;
        }
        let v = f();
        self.inner.force_put(key, v.clone(), 1);
        self.sst_blocks
            .lock()
            .entry(sst_id)
            .or_default()
            .insert(key);
        self.count.fetch_add(1, Ordering::Relaxed);
        {
            let mut w = self.waiters.lock();
            if w.get(&key).is_some_and(|cur| Arc::ptr_eq(cur, &waiter)) {
                w.remove(&key);
            }
        }
        v
    }

    /// Like [`get_or_insert`], but the closure returns `Result`.
    /// On error the cache is left unchanged and the error is propagated.
    ///
    /// Concurrent misses on the same key are coalesced: only the first
    /// thread runs `f`; others wait and read from cache.  On error, the
    /// waiter entry is retained so subsequent callers serialize their
    /// retries.  If the error is permanent, the entry leaks until a
    /// successful insert or SST invalidation occurs.
    pub fn try_get_with<F, E>(&self, sst_id: usize, block_idx: usize, f: F) -> Result<Arc<Block>, E>
    where
        F: FnOnce() -> Result<Arc<Block>, E>,
    {
        let key = (sst_id, block_idx);
        if let Some(v) = self.inner.get(&key) {
            return Ok(v);
        }
        let waiter = {
            let mut w = self.waiters.lock();
            w.entry(key).or_default().clone()
        };
        let _guard = waiter.lock();
        // Double-check after acquiring the per-key lock.
        if let Some(v) = self.inner.get(&key) {
            let mut w = self.waiters.lock();
            if w.get(&key).is_some_and(|cur| Arc::ptr_eq(cur, &waiter)) {
                w.remove(&key);
            }
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
        {
            let mut w = self.waiters.lock();
            if w.get(&key).is_some_and(|cur| Arc::ptr_eq(cur, &waiter)) {
                w.remove(&key);
            }
        }
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
        for key in &keys {
            self.inner.remove(key);
        }
        // Also clean up any in-flight waiter entries for removed keys.
        {
            let mut w = self.waiters.lock();
            for key in &keys {
                w.remove(key);
            }
        }
        self.count.fetch_sub(removed, Ordering::Relaxed);
    }

    /// Approximate number of cached entries.  O(1), lock-free.
    ///
    /// **Drift:** This counter is incremented on insert but only decremented on
    /// [`invalidate_ssts`], not on TinyUFO eviction.  With cache backfill enabled
    /// (inserting many blocks per flush/compaction), the drift can grow faster
    /// because `force_put` evictions are invisible.  Treat the return value as an
    /// upper bound, not an exact count.
    pub fn entry_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Insert a batch of blocks for a newly created SST into the cache.
    ///
    /// Used by flush and compaction threads to warm the cache with newly
    /// produced blocks.  Bypasses single-flight miss coalescing (there are
    /// no concurrent misses for brand-new blocks) and updates the reverse
    /// index in one batch.
    pub fn backfill(&self, sst_id: usize, blocks: Vec<Arc<Block>>) {
        if blocks.is_empty() {
            return;
        }
        let num_blocks = blocks.len();
        // Insert into cache outside the sst_blocks lock to avoid blocking
        // concurrent readers during force_put (which may trigger eviction).
        let mut keys = Vec::with_capacity(num_blocks);
        for (idx, block) in blocks.into_iter().enumerate() {
            let key = (sst_id, idx);
            self.inner.force_put(key, block, 1);
            keys.push(key);
        }
        self.count.fetch_add(num_blocks as u64, Ordering::Relaxed);
        let mut index = self.sst_blocks.lock();
        index
            .entry(sst_id)
            .or_insert_with(|| AHashSet::with_capacity(num_blocks))
            .extend(keys);
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
    file_keys: Mutex<AHashMap<u32, AHashSet<(u32, u64)>>>,
    /// Weight budget (in TinyUFO weight units).  Values whose scaled
    /// weight exceeds this are skipped to avoid exceeding the byte budget.
    weight_budget: u16,
}

impl ValueCache {
    /// Create a new value cache with the given byte budget.
    ///
    /// The budget is converted to TinyUFO weight units by dividing by
    /// [`VALUE_WEIGHT_DIVISOR`].
    pub fn new(byte_budget: u64) -> Self {
        let weight_budget: usize = (byte_budget / VALUE_WEIGHT_DIVISOR as u64)
            .max(1)
            .try_into()
            .unwrap_or(usize::MAX);
        // Estimate number of entries: assume average value is 4 KiB.
        let estimated_items = (byte_budget / 4096).max(16) as usize;
        Self {
            inner: TinyUfo::new(weight_budget, estimated_items),
            file_keys: Mutex::new(AHashMap::new()),
            weight_budget: weight_budget.min(u16::MAX as usize) as u16,
        }
    }

    /// Compute the TinyUFO weight for a value using ceiling division.
    /// Returns `None` if the value is too large to represent accurately
    /// (would saturate to `u16::MAX`) or exceeds the cache's weight
    /// budget, to avoid budget overshoot.
    fn value_weight(&self, value: &Bytes) -> Option<u16> {
        let raw = value.len().div_ceil(VALUE_WEIGHT_DIVISOR);
        if raw > u16::MAX as usize || raw > self.weight_budget as usize {
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
        let Some(w) = self.value_weight(&value) else {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn make_block(data: &[u8]) -> Arc<Block> {
        Arc::new(Block {
            data: Bytes::copy_from_slice(data),
            offsets: vec![],
        })
    }

    // -----------------------------------------------------------------------
    // BlockCache
    // -----------------------------------------------------------------------

    #[test]
    fn block_cache_new_clamps_capacity() {
        let cache = BlockCache::new(0);
        // capacity is clamped to at least 1
        let b = cache.get_or_insert(1, 0, || make_block(b"x"));
        assert_eq!(&b.data[..], b"x");
    }

    #[test]
    fn block_cache_get_or_insert_hit_and_miss() {
        let cache = BlockCache::new(64);
        let calls = std::sync::atomic::AtomicUsize::new(0);

        // miss — closure runs
        let b1 = cache.get_or_insert(1, 0, || {
            calls.fetch_add(1, Ordering::Relaxed);
            make_block(b"hello")
        });
        assert_eq!(&b1.data[..], b"hello");
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        // hit — closure does NOT run
        let b2 = cache.get_or_insert(1, 0, || {
            calls.fetch_add(1, Ordering::Relaxed);
            make_block(b"WRONG")
        });
        assert_eq!(&b2.data[..], b"hello");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn block_cache_different_keys_independent() {
        let cache = BlockCache::new(64);
        let a = cache.get_or_insert(1, 0, || make_block(b"a"));
        let b = cache.get_or_insert(1, 1, || make_block(b"b"));
        let c = cache.get_or_insert(2, 0, || make_block(b"c"));
        assert_eq!(&a.data[..], b"a");
        assert_eq!(&b.data[..], b"b");
        assert_eq!(&c.data[..], b"c");
        assert_eq!(cache.entry_count(), 3);
    }

    #[test]
    fn block_cache_try_get_with_hit() {
        let cache = BlockCache::new(64);
        let r1: Result<Arc<Block>, String> = cache.try_get_with(1, 0, || Ok(make_block(b"ok")));
        assert!(r1.is_ok());
        assert_eq!(&r1.unwrap().data[..], b"ok");

        // second call is a hit
        let r2: Result<Arc<Block>, String> = cache.try_get_with(1, 0, || Ok(make_block(b"WRONG")));
        assert_eq!(&r2.unwrap().data[..], b"ok");
    }

    #[test]
    fn block_cache_try_get_with_error_propagates() {
        let cache = BlockCache::new(64);
        let r: Result<Arc<Block>, &str> = cache.try_get_with(1, 0, || Err("disk broken"));
        match r {
            Err(e) => assert_eq!(e, "disk broken"),
            Ok(_) => panic!("expected error"),
        }

        // cache is empty — entry_count stays 0
        assert_eq!(cache.entry_count(), 0);

        // can still insert after error
        let r2: Result<Arc<Block>, &str> = cache.try_get_with(1, 0, || Ok(make_block(b"ok")));
        assert_eq!(&r2.unwrap().data[..], b"ok");
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn block_cache_invalidate_ssts() {
        let cache = BlockCache::new(64);
        cache.get_or_insert(1, 0, || make_block(b"a"));
        cache.get_or_insert(1, 1, || make_block(b"b"));
        cache.get_or_insert(2, 0, || make_block(b"c"));
        assert_eq!(cache.entry_count(), 3);

        let mut remove = HashSet::new();
        remove.insert(1);
        cache.invalidate_ssts(&remove);

        // SST 1 blocks gone, SST 2 block remains
        assert!(cache.inner.get(&(1, 0)).is_none());
        assert!(cache.inner.get(&(1, 1)).is_none());
        assert!(cache.inner.get(&(2, 0)).is_some());
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn block_cache_invalidate_unknown_id_noop() {
        let cache = BlockCache::new(64);
        cache.get_or_insert(1, 0, || make_block(b"a"));
        let mut remove = HashSet::new();
        remove.insert(999);
        cache.invalidate_ssts(&remove);
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn block_cache_single_flight_coalesces_misses() {
        let cache = Arc::new(BlockCache::new(64));
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                let calls = calls.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    cache.get_or_insert(1, 0, || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        // Simulate disk latency
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        make_block(b"data")
                    })
                })
            })
            .collect();

        for h in handles {
            let b = h.join().unwrap();
            assert_eq!(&b.data[..], b"data");
        }
        // Closure should have run exactly once (single-flight).
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn block_cache_try_get_with_concurrent_error() {
        let cache = Arc::new(BlockCache::new(64));
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                let calls = calls.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    cache.try_get_with::<_, &str>(1, 0, || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        Err("disk broken")
                    })
                })
            })
            .collect();

        for h in handles {
            let r = h.join().unwrap();
            assert!(r.is_err());
        }
        // Error path: each blocked thread retries after the previous one fails,
        // so the closure runs once per thread (serialized by per-key lock).
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        // Cache should remain empty after all errors.
        assert_eq!(cache.entry_count(), 0);

        // Subsequent successful call still works — waiter entry is reused.
        let r: Result<Arc<Block>, &str> = cache.try_get_with(1, 0, || Ok(make_block(b"ok")));
        assert_eq!(&r.unwrap().data[..], b"ok");
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn block_cache_try_get_with_single_flight_success() {
        let cache = Arc::new(BlockCache::new(64));
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                let calls = calls.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    cache.try_get_with::<_, &str>(1, 0, || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        Ok(make_block(b"data"))
                    })
                })
            })
            .collect();

        for h in handles {
            let r = h.join().unwrap();
            assert_eq!(&r.unwrap().data[..], b"data");
        }
        // Closure should have run exactly once (single-flight on success).
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn block_cache_backfill_inserts_blocks() {
        let cache = BlockCache::new(64);
        let blocks: Vec<Arc<Block>> = (0..3).map(|i| make_block(&[i as u8; 4])).collect();

        cache.backfill(1, blocks);

        // All 3 blocks should be cached.
        assert_eq!(cache.entry_count(), 3);
        for i in 0..3 {
            let b = cache.inner.get(&(1, i));
            assert!(b.is_some(), "block {i} should be in cache");
            assert_eq!(&b.unwrap().data[..], &[i as u8; 4]);
        }

        // Reverse index should have all 3 keys.
        let index = cache.sst_blocks.lock();
        assert_eq!(index.get(&1).unwrap().len(), 3);
    }

    #[test]
    fn block_cache_backfill_empty_is_noop() {
        let cache = BlockCache::new(64);
        cache.backfill(1, vec![]);
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn block_cache_entry_count_drifts_after_eviction() {
        // Capacity = 4 entries.
        let cache = BlockCache::new(4);
        // Insert 10 unique blocks — TinyUFO will evict some.
        for i in 0..10 {
            cache.get_or_insert(1, i, || make_block(&[i as u8]));
        }
        // entry_count overestimates because TinyUFO evictions are invisible.
        // Each insert incremented count, but evictions didn't decrement it.
        assert_eq!(cache.entry_count(), 10);
        // Verify that TinyUFO actually evicted some early entries.
        // (The exact evictions depend on TinyUFO's S3-FIFO algorithm.)
        let evicted = (0..10)
            .filter(|&i| cache.inner.get(&(1, i)).is_none())
            .count();
        assert!(
            evicted > 0,
            "expected some evictions in a capacity-4 cache with 10 inserts"
        );
    }

    // -----------------------------------------------------------------------
    // ValueCache
    // -----------------------------------------------------------------------

    #[test]
    fn value_cache_insert_and_get() {
        let cache = ValueCache::new(1024 * 1024); // 1 MiB
        cache.insert((1, 0), Bytes::from_static(b"hello"));
        assert_eq!(cache.get(&(1, 0)), Some(Bytes::from_static(b"hello")));
        assert_eq!(cache.get(&(1, 1)), None);
    }

    #[test]
    fn value_cache_weight_clamping_small_value() {
        let cache = ValueCache::new(1024 * 1024);
        // 10 bytes → weight = ceil(10/64) = 1, fits easily
        cache.insert((1, 0), Bytes::from_static(b"0123456789"));
        assert_eq!(cache.get(&(1, 0)), Some(Bytes::from_static(b"0123456789")));
    }

    #[test]
    fn value_cache_weight_exceeds_budget_skipped() {
        // budget = 1024 bytes → weight_budget = 1024/64 = 16
        let cache = ValueCache::new(1024);
        // value = 2048 bytes → weight = ceil(2048/64) = 32 > 16 → skipped
        let big = Bytes::from(vec![0u8; 2048]);
        cache.insert((1, 0), big.clone());
        assert_eq!(cache.get(&(1, 0)), None);
    }

    #[test]
    fn value_cache_weight_exceeds_u16_skipped() {
        // 5 MiB budget → weight_budget capped at u16::MAX (65535)
        let cache = ValueCache::new(5 * 1024 * 1024);
        // 5 MiB value → raw weight = ceil(5MiB / 64) = 81920 > u16::MAX
        let huge = Bytes::from(vec![0u8; 5 * 1024 * 1024]);
        cache.insert((1, 0), huge);
        assert_eq!(cache.get(&(1, 0)), None);
    }

    #[test]
    fn value_cache_invalidate_file() {
        let cache = ValueCache::new(1024 * 1024);
        cache.insert((1, 0), Bytes::from_static(b"a"));
        cache.insert((1, 1), Bytes::from_static(b"b"));
        cache.insert((2, 0), Bytes::from_static(b"c"));

        cache.invalidate_file(1);
        assert_eq!(cache.get(&(1, 0)), None);
        assert_eq!(cache.get(&(1, 1)), None);
        assert_eq!(cache.get(&(2, 0)), Some(Bytes::from_static(b"c")));
    }

    #[test]
    fn value_cache_invalidate_unknown_file_noop() {
        let cache = ValueCache::new(1024 * 1024);
        cache.insert((1, 0), Bytes::from_static(b"a"));
        cache.invalidate_file(999);
        assert_eq!(cache.get(&(1, 0)), Some(Bytes::from_static(b"a")));
    }

    #[test]
    fn value_cache_duplicate_key_updates_reverse_index() {
        let cache = ValueCache::new(1024 * 1024);
        cache.insert((1, 0), Bytes::from_static(b"old"));
        cache.insert((1, 0), Bytes::from_static(b"new"));
        assert_eq!(cache.get(&(1, 0)), Some(Bytes::from_static(b"new")));

        // reverse index still has exactly one entry for (1,0)
        let index = cache.file_keys.lock();
        assert_eq!(index.get(&1).unwrap().len(), 1);
    }

    #[test]
    fn value_cache_concurrent_insert_and_invalidate() {
        let cache = Arc::new(ValueCache::new(1024 * 1024));
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let handles: Vec<_> = (0..4)
            .map(|t| {
                let cache = cache.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    // Each thread inserts into a different file_id.
                    let file_id = t as u32;
                    for i in 0..50u64 {
                        cache.insert((file_id, i), Bytes::from(format!("t{t}-v{i}")));
                    }
                    // Invalidate own file.
                    cache.invalidate_file(file_id);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        // All files were invalidated — cache should be empty.
        for t in 0..4u32 {
            for i in 0..50u64 {
                assert_eq!(cache.get(&(t, i)), None);
            }
        }
    }
}
