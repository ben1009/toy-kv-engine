#[cfg(feature = "bench")]
use std::time::Instant;
use std::{
    ops::Bound,
    path::Path,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize},
    },
};

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::{
    iterators::StorageIterator,
    key::{Key, KeySlice, shared_bytes_from_slice},
    range_tombstone::{
        RangeTombstoneFragment, RangeTombstoneIterator, RangeTombstoneKey, RangeTombstoneSet,
        fragment_range,
    },
    table::{SsTableBuilder, bloom::IncrementalBloom},
    vlog::{KvKind, ValueLog, ValuePointer},
    wal::Wal,
};

/// Expected number of entries per memtable for bloom filter sizing.
/// At 1KB values and 1MB SST target, ~1000 entries. We size generously.
const BLOOM_EXPECTED_ENTRIES: usize = 4096;
const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

/// Cumulative wall-clock timing for write-path phases.
/// All durations are in nanoseconds, stored as atomics for lock-free updates.
///
/// Profiling counters are only updated when the `bench` feature is enabled.
/// Without the feature, the struct is still present (for API compatibility)
/// but the hot path has zero profiling overhead.
#[derive(Debug, Default)]
pub struct WriteProfile {
    /// Time in `Wal::put_batch` (encode + BufWriter write).
    pub wal_write_ns: AtomicU64,
    /// Time in `Wal::sync` (BufWriter flush + `fsync`).
    pub wal_sync_ns: AtomicU64,
    /// Time inserting into the SkipMap + bloom filter.
    pub memtable_insert_ns: AtomicU64,
    /// Number of write operations profiled.
    pub op_count: AtomicU64,
}

impl WriteProfile {
    pub fn snapshot(&self) -> WriteProfileSnapshot {
        let o = std::sync::atomic::Ordering::Relaxed;
        WriteProfileSnapshot {
            wal_write_ns: self.wal_write_ns.load(o),
            wal_sync_ns: self.wal_sync_ns.load(o),
            memtable_insert_ns: self.memtable_insert_ns.load(o),
            op_count: self.op_count.load(o),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WriteProfileSnapshot {
    pub wal_write_ns: u64,
    pub wal_sync_ns: u64,
    pub memtable_insert_ns: u64,
    pub op_count: u64,
}

impl WriteProfileSnapshot {
    pub fn wal_write_ms(&self) -> f64 {
        self.wal_write_ns as f64 / 1_000_000.0
    }

    pub fn wal_sync_ms(&self) -> f64 {
        self.wal_sync_ns as f64 / 1_000_000.0
    }

    pub fn memtable_insert_ms(&self) -> f64 {
        self.memtable_insert_ns as f64 / 1_000_000.0
    }

    pub fn total_ms(&self) -> f64 {
        self.wal_write_ms() + self.wal_sync_ms() + self.memtable_insert_ms()
    }

    pub fn wal_sync_pct(&self) -> f64 {
        let total = self.total_ms();
        if total == 0.0 {
            0.0
        } else {
            self.wal_sync_ms() / total * 100.0
        }
    }
}

/// Immutable range-tombstone view for frozen memtables.
///
/// After a memtable is frozen, its range-tombstone set becomes read-only.
/// This wrapper adds a lazily-built fragment cache (`OnceLock`) so that all
/// readers of the same immutable memtable share the same fragmented view,
/// avoiding the stale-install race that affects the active memtable.
pub struct ImmutableRangeTombstoneSet {
    /// Raw tombstone entries shared with the frozen memtable.
    raw: Arc<SkipMap<RangeTombstoneKey, Bytes>>,
    /// Lazily-built non-overlapping fragment view.
    fragments: OnceLock<Arc<[RangeTombstoneFragment]>>,
}

impl ImmutableRangeTombstoneSet {
    /// Wrap a `RangeTombstoneSet`'s raw skipmap into an immutable view.
    pub fn from_range_tombstone_set(set: &RangeTombstoneSet) -> Self {
        Self {
            raw: Arc::clone(set.raw()),
            fragments: OnceLock::new(),
        }
    }

    /// Return a reference to the fragmented view, building it lazily on first
    /// access. Returns `&Arc` to avoid cloning on the hot point-lookup path.
    pub fn fragments(&self) -> &Arc<[RangeTombstoneFragment]> {
        self.fragments.get_or_init(|| {
            let frags = fragment_range(&self.raw);
            Arc::from(frags)
        })
    }

    /// Find the newest covering tombstone timestamp for `user_key` at `read_ts`.
    ///
    /// Delegates to [`crate::range_tombstone::find_newest_covering_ts`] to
    /// avoid duplicating the binary-search logic.
    pub fn newest_covering_ts(&self, user_key: &[u8], read_ts: u64) -> Option<u64> {
        let frags = self.fragments();
        crate::range_tombstone::find_newest_covering_ts(frags, user_key, read_ts)
    }

    /// Build a `RangeTombstoneIterator` for scan-path use.
    pub fn iter(&self) -> RangeTombstoneIterator {
        RangeTombstoneIterator::new(self.fragments().clone())
    }
}

/// A basic mem-table based on crossbeam-skiplist.
///
/// A basic memtable implementation. It will be incrementally extended in other modules.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
    /// One-past the highest ticket assigned by WAL writes in this memtable.
    /// Zero means "no WAL writes yet", which avoids ambiguity with ticket 0.
    last_ticket: AtomicU64,
    /// When true, values in the map are kind-prefixed: `[KvKind:1][payload]`.
    vlog_enabled: bool,
    /// Incremental bloom filter for negative lookups. Avoids skiplist epoch pin
    /// overhead when the key is not in this memtable.
    bloom: IncrementalBloom,
    /// Range tombstones stored in this memtable.
    range_tombstones: RangeTombstoneSet,
    /// Immutable range-tombstone view with cached fragments, set after freeze.
    /// Uses `OnceLock` for interior mutability so it can be initialized through
    /// `&self` (the memtable may already be inside an `Arc` when frozen).
    immutable_range_tombstones: OnceLock<ImmutableRangeTombstoneSet>,
    /// Write-path profiling counters. Uses `ArcSwap` so the profile can be
    /// replaced after construction (e.g., to share the engine-level profile).
    write_profile: arc_swap::ArcSwap<WriteProfile>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table. `id` is the sst_id when flushing this memtable to SST.
    /// When `vlog_enabled` is true, values are stored with a [`KvKind`] prefix byte.
    pub fn create(id: usize, vlog_enabled: bool) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
            last_ticket: AtomicU64::new(0),
            vlog_enabled,
            bloom: IncrementalBloom::new(BLOOM_EXPECTED_ENTRIES, BLOOM_FALSE_POSITIVE_RATE),
            range_tombstones: RangeTombstoneSet::new(),
            immutable_range_tombstones: OnceLock::new(),
            write_profile: arc_swap::ArcSwap::new(Arc::new(WriteProfile::default())),
        }
    }

    /// Create a new mem-table with vLog (kind-prefixed values).
    pub fn create_vlog(id: usize) -> Self {
        Self::create(id, true)
    }

    /// Create a new mem-table with WAL.
    pub fn create_with_wal(id: usize, vlog_enabled: bool, path: impl AsRef<Path>) -> Result<Self> {
        let mut ret = Self::create(id, vlog_enabled);
        ret.wal = Some(Wal::create(path)?);

        Ok(ret)
    }

    /// Create a new mem-table with WAL and vLog (kind-prefixed values).
    pub fn create_with_wal_vlog(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_wal(id, true, path)
    }

    /// Create a memtable from WAL. Returns the memtable and the max commit_ts found.
    ///
    /// Uses [`Wal::recover`] which replays point entries into the skiplist but
    /// silently discards range tombstones. For full recovery including range
    /// tombstones, use [`recover_from_wal_with_range_tombstones`].
    pub fn recover_from_wal(
        id: usize,
        vlog_enabled: bool,
        path: impl AsRef<Path>,
    ) -> Result<(Self, u64)> {
        let mut ret = Self::create(id, vlog_enabled);
        let (wal, max_ts) = Wal::recover(path, &ret.map)?;
        ret.wal = Some(wal);
        ret.rebuild_bloom();

        Ok((ret, max_ts))
    }

    /// Create a memtable from WAL with vLog (kind-prefixed values). Returns the memtable and max
    /// commit_ts.
    pub fn recover_from_wal_vlog(id: usize, path: impl AsRef<Path>) -> Result<(Self, u64)> {
        Self::recover_from_wal(id, true, path)
    }

    /// Create a memtable from WAL with full range-tombstone recovery.
    ///
    /// Unlike [`recover_from_wal`], this method uses
    /// [`Wal::recover_with_range_tombstones`] to also populate the memtable's
    /// [`RangeTombstoneSet`] from WAL v3 range-tombstone entries.
    pub fn recover_from_wal_with_range_tombstones(
        id: usize,
        vlog_enabled: bool,
        path: impl AsRef<Path>,
    ) -> Result<(Self, u64)> {
        let mut ret = Self::create(id, vlog_enabled);
        let (wal, batch) =
            Wal::recover_with_range_tombstones(path, &ret.map, &ret.range_tombstones)?;
        ret.wal = Some(wal);
        ret.rebuild_bloom();

        Ok((ret, batch.max_ts))
    }

    /// Rebuild the bloom filter from existing skiplist entries.
    /// Used after WAL recovery where entries are inserted directly into the skiplist.
    fn rebuild_bloom(&self) {
        let mut buf = Vec::new();
        for entry in self.map.iter() {
            let key = entry.key();
            let hash_src: &[u8] = if crate::key::TS_ENABLED {
                buf.clear();
                crate::key::KeySlice::from_slice(key).decode_user_key_into(&mut buf);
                &buf
            } else {
                key
            };
            self.bloom
                .push_hash(super::table::bloom::hash_key(hash_src));
        }
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Whether this memtable uses kind-prefixed values.
    pub fn vlog_enabled(&self) -> bool {
        self.vlog_enabled
    }

    /// Get a value by key.
    /// When vlog_enabled, strips the 1-byte KvKind prefix from the stored value.
    /// Uses the bloom filter to skip skiplist lookup on negative lookups.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let h = super::table::bloom::hash_key(key);

        self.get_with_hash(key, h)
    }

    /// Get a value by key using a precomputed bloom hash.
    /// Avoids recomputing the hash when checking multiple memtables.
    #[must_use]
    pub fn get_with_hash(&self, key: &[u8], hash: u32) -> Option<Bytes> {
        if self.is_empty() || !self.bloom.may_contain_hash(hash) {
            return None;
        }
        self.map.get(key).map(|x| {
            let val = x.value();
            if !val.is_empty() && val[0] != crate::vlog::KvKind::Tombstone as u8 {
                val.slice(1..)
            } else {
                val.clone()
            }
        })
    }

    /// Get the raw value (with kind prefix if vlog_enabled) by key.
    /// Uses the bloom filter to skip skiplist lookup on negative lookups.
    pub fn get_raw(&self, key: &[u8]) -> Option<Bytes> {
        let h = super::table::bloom::hash_key(key);

        self.get_raw_with_hash(key, h)
    }

    /// Get the raw value by key using a precomputed bloom hash.
    /// Avoids recomputing the hash when checking multiple memtables.
    pub fn get_raw_with_hash(&self, key: &[u8], hash: u32) -> Option<Bytes> {
        if self.is_empty() || !self.bloom.may_contain_hash(hash) {
            return None;
        }
        self.map.get(key).map(|x| x.value().clone())
    }

    /// Exact lookup by full encoded internal key (user key + timestamp).
    /// Used by GC to check if a specific version's pointer still matches.
    pub fn get_raw_exact(&self, encoded_internal_key: &[u8]) -> Option<Bytes> {
        // Check bloom filter using the decoded user key. If the bloom says
        // "not contained" we can skip the skiplist lookup entirely.
        if crate::key::TS_ENABLED {
            thread_local! {
                static BUF: std::cell::RefCell<Vec<u8>> =
                    const { std::cell::RefCell::new(Vec::new()) };
            }
            let may_contain = BUF.with(|buf| {
                let mut b = buf.borrow_mut();
                b.clear();
                crate::key::KeySlice::from_slice(encoded_internal_key).decode_user_key_into(&mut b);
                let h = super::table::bloom::hash_key(&b);
                self.bloom.may_contain_hash(h)
            });
            if !may_contain {
                return None;
            }
        } else if !self
            .bloom
            .may_contain_hash(super::table::bloom::hash_key(encoded_internal_key))
        {
            return None;
        }

        self.map
            .get(encoded_internal_key)
            .map(|x| x.value().clone())
    }

    /// Exact lookup by encoded internal key with a pre-computed bloom hash.
    ///
    /// Avoids the thread-local buffer overhead of [`get_raw_exact`] by
    /// accepting a pre-computed `hash_key(user_key)` from the caller. This
    /// eliminates the per-call `thread_local!` + `RefCell` + `decode_user_key`
    /// overhead that dominates the `get_raw_exact` profile.
    pub fn get_raw_exact_with_hash(
        &self,
        encoded_internal_key: &[u8],
        bloom_hash: u32,
    ) -> Option<Bytes> {
        if !self.bloom.may_contain_hash(bloom_hash) {
            return None;
        }
        self.map
            .get(encoded_internal_key)
            .map(|x| x.value().clone())
    }

    /// Get the newest version of a user key visible at `read_ts`.
    ///
    /// The memtable stores encoded internal keys. This method seeks to
    /// `encode(user_key, u64::MAX)` (the smallest encoded form for the user key)
    /// and iterates through versions in newest-first order, returning the first
    /// version whose timestamp is <= `read_ts`.
    pub fn get_versioned(&self, user_key: &[u8], read_ts: u64) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let bloom_hash = super::table::bloom::hash_key(user_key);
        if !self.bloom.may_contain_hash(bloom_hash) {
            return None;
        }
        let seek_key = Bytes::from(crate::key::encode_internal_key(user_key, u64::MAX));
        let seek_prefix = crate::key::encoded_user_key_prefix(&seek_key)
            .expect("seek_key is newly encoded and guaranteed to be well-formed");
        let mut range = self.map.range::<Bytes, _>(seek_key.clone()..);
        for entry in range.by_ref() {
            let found_key = entry.key();
            // Check if the decoded user key matches
            if let Some(found_user_key) = crate::key::encoded_user_key_prefix(found_key) {
                if found_user_key != seek_prefix {
                    // Different user key — no more versions to check.
                    break;
                }
            } else {
                break;
            }
            // Check timestamp visibility: skip versions newer than read_ts.
            // extract_ts returns None for keys < 17 bytes (non-MVCC or corrupt).
            // debug_assert catches corruption in test builds; release falls back to ts=0.
            let ts_opt = crate::key::extract_ts(found_key);
            debug_assert!(
                !crate::key::TS_ENABLED || ts_opt.is_some(),
                "corrupt MVCC key missing timestamp: {} bytes",
                found_key.len()
            );
            let ts = ts_opt.unwrap_or(0);
            if ts > read_ts {
                continue;
            }
            let val = entry.value();
            if !val.is_empty() && val[0] != crate::vlog::KvKind::Tombstone as u8 {
                return Some(val.slice(1..));
            } else {
                return Some(val.clone());
            }
        }
        None
    }

    /// Get the raw value for the newest version of a user key visible at `read_ts`.
    pub fn get_versioned_raw(&self, user_key: &[u8], read_ts: u64) -> Option<Bytes> {
        self.get_versioned_raw_with_key(user_key, read_ts)
            .map(|(val, _key)| val)
    }

    /// Like `get_versioned_raw`, but also returns the full encoded internal key
    /// of the matching entry. Used for vLog key verification.
    pub fn get_versioned_raw_with_key(
        &self,
        user_key: &[u8],
        read_ts: u64,
    ) -> Option<(Bytes, Vec<u8>)> {
        let bloom_hash = super::table::bloom::hash_key(user_key);
        self.get_versioned_raw_with_key_and_hash(user_key, read_ts, bloom_hash)
    }

    /// Like `get_versioned_raw_with_key`, but accepts a precomputed bloom hash
    /// to avoid redundant hashing when the caller already computed it.
    pub fn get_versioned_raw_with_key_and_hash(
        &self,
        user_key: &[u8],
        read_ts: u64,
        bloom_hash: u32,
    ) -> Option<(Bytes, Vec<u8>)> {
        thread_local! {
            static SEEK_BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        SEEK_BUF.with(|buf| {
            self.get_versioned_with_buf(user_key, read_ts, bloom_hash, &mut buf.borrow_mut())
        })
    }

    /// Version-aware lookup that reuses an encode buffer to avoid per-key
    /// `Bytes` allocation. Used by the small-batch `batch_get` fast path.
    pub(crate) fn get_versioned_with_buf(
        &self,
        user_key: &[u8],
        read_ts: u64,
        bloom_hash: u32,
        seek_buf: &mut Vec<u8>,
    ) -> Option<(Bytes, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }
        if !self.bloom.may_contain_hash(bloom_hash) {
            return None;
        }
        seek_buf.clear();
        crate::key::encode_internal_key_to_buf(seek_buf, user_key, u64::MAX);
        let seek_prefix = crate::key::encoded_user_key_prefix(seek_buf)
            .expect("seek_key is newly encoded and guaranteed to be well-formed");
        let mut range = self.map.range::<[u8], _>((
            std::ops::Bound::Included(seek_buf.as_slice()),
            std::ops::Bound::Unbounded,
        ));
        for entry in range.by_ref() {
            let found_key = entry.key();
            if let Some(found_user_key) = crate::key::encoded_user_key_prefix(found_key) {
                if found_user_key != seek_prefix {
                    break;
                }
            } else {
                break;
            }
            let ts_opt = crate::key::extract_ts(found_key);
            debug_assert!(
                !crate::key::TS_ENABLED || ts_opt.is_some(),
                "corrupt MVCC key missing timestamp: {} bytes",
                found_key.len()
            );
            let ts = ts_opt.unwrap_or(0);
            if ts > read_ts {
                continue;
            }
            return Some((entry.value().clone(), found_key.to_vec()));
        }
        None
    }

    /// Batch version-aware lookup for sorted keys.
    ///
    /// `sorted_keys` must be sorted by raw user key bytes. Each entry is
    /// `(original_index, user_key)`. `bloom_hashes` is indexed by original_index.
    ///
    /// Returns a `Vec` of `(original_index, raw_value, found_key)` for keys
    /// found in this memtable. Keys not found (bloom negative or not present)
    /// are omitted.
    pub(crate) fn batch_get_versioned(
        &self,
        sorted_keys: &[(usize, &[u8])],
        read_ts: u64,
        bloom_hashes: &[u32],
    ) -> Vec<(usize, Bytes, Bytes)> {
        if self.is_empty() || sorted_keys.is_empty() {
            return Vec::new();
        }
        let mut results = Vec::with_capacity(sorted_keys.len());
        let mut seek_buf: Vec<u8> = Vec::new();
        for &(orig_idx, user_key) in sorted_keys {
            if let Some((value, found_key)) = self.get_versioned_with_buf(
                user_key,
                read_ts,
                bloom_hashes[orig_idx],
                &mut seek_buf,
            ) {
                results.push((orig_idx, value, Bytes::from(found_key)));
            }
        }
        results
    }

    /// Collect vLog file IDs referenced by ValuePointer entries in this memtable.
    /// Used during startup to prevent orphan cleanup from deleting vLog files
    /// that are still needed by unflushed memtable entries.
    pub fn collect_vlog_file_ids(&self) -> std::collections::HashSet<u32> {
        let mut ids = std::collections::HashSet::new();
        if !self.vlog_enabled {
            return ids;
        }
        for entry in self.map.iter() {
            let val = entry.value();
            if val.len() > 1
                && val[0] == KvKind::ValuePointer as u8
                && let Some(ptr) = ValuePointer::try_decode(&val[1..])
            {
                ids.insert(ptr.file_id);
            }
            if val.len() > 9
                && val[0] == KvKind::TtlValuePointer as u8
                && let Some(ptr) = ValuePointer::try_decode(&val[9..])
            {
                ids.insert(ptr.file_id);
            }
        }
        ids
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// Always prepends `KvKind::Inline` so values are self-describing
    /// regardless of vlog mode.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut prefixed = Vec::with_capacity(1 + value.len());
        prefixed.push(crate::vlog::KvKind::Inline as u8);
        prefixed.extend_from_slice(value);

        self.put_raw(key, &prefixed)
    }

    /// Put a key-value pair into the mem-table without syncing the WAL.
    ///
    /// Caller must call [`commit_wal`] after releasing write locks.
    /// Write a tombstone (deletion marker) for the given key.
    ///
    /// When vlog_enabled, stores `[KvKind::Tombstone]` as a single-byte value.
    /// When vlog disabled, stores an empty value (legacy behavior).
    pub fn put_tombstone(&self, key: &[u8]) -> Result<()> {
        self.put_raw(key, &[crate::vlog::KvKind::Tombstone as u8])
    }

    /// Put a raw key-value pair into the mem-table without kind prefixing.
    /// Used by compare_and_set_with_kind to write pre-prefixed values.
    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_raw_batch(&[(KeySlice::from_slice(key), value)])
    }

    /// Put a batch of raw (pre-prefixed) key-value pairs without WAL.
    /// Used for idempotent GC rewrites that don't need WAL replay on recovery.
    pub fn put_raw_batch_no_wal(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.publish_raw_batch(data)
    }

    /// Put a batch of raw (pre-prefixed) key-value pairs and sync the WAL.
    /// The skiplist insert happens only after the WAL sync succeeds.
    pub fn put_raw_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.write_wal_batch(data)?;
        self.commit_wal()?;
        self.publish_raw_batch(data)
    }

    /// Write a batch to the WAL buffer only (no skiplist insert, no sync).
    ///
    /// The caller must subsequently call:
    /// 1. [`commit_wal`] to durably sync the WAL.
    /// 2. [`publish_raw_batch`] to insert into the skiplist + bloom filter.
    ///
    /// This split ensures data is not visible to readers until the WAL sync
    /// succeeds, preventing ghost entries on fsync failure.
    pub fn write_wal_batch_only(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.write_wal_batch(data)
    }

    fn write_wal_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let commit_ts = data
            .first()
            .and_then(|(k, _)| crate::key::extract_ts(k.raw_ref()))
            .unwrap_or(0);
        // All entries in a batch must share the same commit_ts (L2).
        debug_assert!(
            data.iter()
                .all(|(k, _)| { crate::key::extract_ts(k.raw_ref()).unwrap_or(0) == commit_ts }),
            "write_wal_batch: entries have mismatched commit_ts (first={commit_ts})",
        );
        let entries: Vec<(&[u8], &[u8])> = data.iter().map(|(k, v)| (k.raw_ref(), *v)).collect();
        #[cfg(feature = "bench")]
        let t = Instant::now();
        let ticket = wal.put_batch(&entries, commit_ts)?;
        self.last_ticket
            .fetch_max(ticket + 1, std::sync::atomic::Ordering::Release);
        #[cfg(feature = "bench")]
        self.write_profile.load().wal_write_ns.fetch_add(
            t.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    pub(crate) fn publish_raw_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        struct AbortOnPanic;
        impl Drop for AbortOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    log::error!(
                        "panic during memtable publication — aborting to prevent WAL/memtable divergence"
                    );
                    std::process::abort();
                }
            }
        }
        let _abort_guard = AbortOnPanic;

        #[cfg(feature = "bench")]
        let t = Instant::now();
        let mut buf = Vec::new();
        for (key, value) in data {
            buf.clear();
            key.decode_user_key_into(&mut buf);
            self.bloom.push_hash(super::table::bloom::hash_key(&buf));
            self.map.insert(
                shared_bytes_from_slice(key.raw_ref()),
                shared_bytes_from_slice(value),
            );

            // approximate_size is already approximate — Relaxed ordering
            // is sufficient and avoids unnecessary fence overhead (L3).
            // Use raw_ref().len() for key data size (size_of_val on KeySlice
            // returns the struct size, not the data length).
            self.approximate_size.fetch_add(
                key.raw_ref().len() + value.len(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        #[cfg(feature = "bench")]
        {
            let wp = self.write_profile.load();
            wp.memtable_insert_ns.fetch_add(
                t.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            wp.op_count
                .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    /// Sync all pending WAL writes via group commit.
    ///
    /// Call this AFTER releasing any write locks (e.g., the MVCC write lock)
    /// so that concurrent writers can batch their fsyncs together.
    pub fn commit_wal(&self) -> Result<()> {
        if let Some(wal) = &self.wal {
            let watermark = self.last_ticket.load(std::sync::atomic::Ordering::Acquire);
            if watermark == 0 {
                return Ok(());
            }
            #[cfg(feature = "bench")]
            let t = Instant::now();
            wal.submit_and_commit(watermark - 1)?;
            #[cfg(feature = "bench")]
            self.write_profile.load().wal_sync_ns.fetch_add(
                t.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        Ok(())
    }

    /// Implement this in MVCC.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let prefixed: Vec<(KeySlice, Vec<u8>)> = data
            .iter()
            .map(|(k, v)| {
                let mut p = Vec::with_capacity(1 + v.len());
                p.push(crate::vlog::KvKind::Inline as u8);
                p.extend_from_slice(v);
                (*k, p)
            })
            .collect();
        let refs: Vec<(KeySlice, &[u8])> =
            prefixed.iter().map(|(k, v)| (*k, v.as_slice())).collect();

        self.put_raw_batch(&refs)
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }

        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        self.scan_with_vlog(lower, upper, None)
    }

    /// Get an iterator over a range of keys, with optional vLog for ValuePointer dereferencing.
    pub fn scan_with_vlog(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        vlog: Option<Arc<ValueLog>>,
    ) -> MemTableIterator {
        let vlog_enabled = self.vlog_enabled;
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
            vlog_enabled,
            vlog,
            resolved: Bytes::new(),
        }
        .build();
        // next() always returns Ok(()) — positions iterator at first element or marks invalid.
        iter.next()
            .expect("next() is infallible for MemTableIterator");

        iter
    }

    /// Flush the mem-table to SSTable. Implement in scan and flush.
    /// When vlog_enabled, checks the KvKind prefix: ValuePointer and Tombstone
    /// entries are passed through via `add_raw()` to preserve the marker;
    /// Inline entries have their prefix stripped and go through `add()` for
    /// value separation.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for e in self.map.iter() {
            let key_bytes = Key::from_bytes(e.key().clone());
            let key = key_bytes.as_key_slice();
            let val = e.value();
            if !val.is_empty()
                && (val[0] == crate::vlog::KvKind::ValuePointer as u8
                    || val[0] == crate::vlog::KvKind::Tombstone as u8)
            {
                // ValuePointer or Tombstone — pass through as-is
                builder.add_raw(key, val)?;
            } else if !val.is_empty()
                && (val[0] == crate::vlog::KvKind::TtlInline as u8
                    || val[0] == crate::vlog::KvKind::TtlValuePointer as u8)
            {
                // TTL entry — preserve TTL metadata through flush.
                // TtlValuePointer: pass through as-is (already has vLog pointer).
                // TtlInline: if value separation is active and the user value
                // exceeds min_value_size, convert to TtlValuePointer; otherwise
                // pass through as-is.
                if val[0] == crate::vlog::KvKind::TtlValuePointer as u8 {
                    builder.add_raw(key, val)?;
                } else {
                    // TtlInline: [0x03][expire_at:8][user_value]
                    builder.add_with_ttl(key, val)?;
                }
            } else {
                // Inline entry — strip KvKind prefix, let add() handle value separation
                let raw = if !val.is_empty() {
                    &val[1..]
                } else {
                    val.as_ref()
                };
                builder.add(key, raw)?;
            }
        }

        // Write range tombstones into the SST v4 range-tombstone block.
        if !self.range_tombstones.is_empty() {
            let frags = crate::range_tombstone::fragment_range(self.range_tombstones.raw());
            builder.add_range_tombstones(frags);
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
            + self.range_tombstones.approximate_size()
    }

    /// Access the underlying skiplist map. Intended for tests and benchmarks only.
    #[doc(hidden)]
    #[cfg(any(test, feature = "bench"))]
    pub fn raw_map(&self) -> &Arc<SkipMap<Bytes, Bytes>> {
        &self.map
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && self.range_tombstones.is_empty()
    }

    pub fn write_profile(&self) -> arc_swap::Guard<Arc<WriteProfile>> {
        self.write_profile.load()
    }

    /// Replace the write profile with a shared instance (e.g., from LsmStorageInner).
    /// This allows profiling to accumulate across memtable freezes.
    pub fn set_write_profile(&self, profile: Arc<WriteProfile>) {
        self.write_profile.store(profile);
    }

    /// Write a range tombstone into the memtable.
    ///
    /// Convenience wrapper around [`put_range_tombstone_batch`] for a single entry.
    pub fn put_range_tombstone(
        &self,
        start: &[u8],
        end: &[u8],
        ts: u64,
        ordinal: u32,
    ) -> Result<()> {
        self.put_range_tombstone_batch(&[(start, end)], ts, ordinal)
    }

    /// Write a batch of range tombstones into the memtable.
    ///
    /// All entries share a single WAL write and sync, avoiding per-entry
    /// write amplification. The `base_ordinal` is the ordinal for the first
    /// entry; subsequent entries get `base_ordinal + i`.
    ///
    /// When WAL is enabled, the batch is durably written to the WAL before
    /// being published, satisfying the RFC Section 6.5 durability requirement.
    pub fn put_range_tombstone_batch(
        &self,
        tombstones: &[(&[u8], &[u8])],
        ts: u64,
        base_ordinal: u32,
    ) -> Result<()> {
        if tombstones.is_empty() {
            return Ok(());
        }
        // Pre-validate ordinal overflow before WAL write so that the
        // in-memory publication loop below is infallible.
        let _ = base_ordinal
            .checked_add(u32::try_from(tombstones.len()).context("ordinal overflow")?)
            .context("ordinal overflow")?;

        // Single WAL write + sync for the entire batch.
        if let Some(wal) = &self.wal {
            let ticket = wal.put_range_tombstone_batch(tombstones, ts)?;
            self.last_ticket
                .fetch_max(ticket + 1, std::sync::atomic::Ordering::Release);
            wal.submit_and_commit(ticket)?;
        }

        self.publish_range_tombstones(tombstones, ts, base_ordinal)
    }

    /// Write a batch of range tombstones into the memtable without syncing
    /// the WAL. The caller must call [`commit_wal`] after releasing write
    /// locks so that concurrent writers can batch their fsyncs together.
    pub fn put_range_tombstone_batch_no_sync(
        &self,
        tombstones: &[(&[u8], &[u8])],
        ts: u64,
        base_ordinal: u32,
    ) -> Result<()> {
        if tombstones.is_empty() {
            return Ok(());
        }
        let _ = base_ordinal
            .checked_add(u32::try_from(tombstones.len()).context("ordinal overflow")?)
            .context("ordinal overflow")?;

        if let Some(wal) = &self.wal {
            let ticket = wal.put_range_tombstone_batch(tombstones, ts)?;
            self.last_ticket
                .fetch_max(ticket + 1, std::sync::atomic::Ordering::Release);
            // No sync — caller commits the WAL after releasing locks.
        }

        self.publish_range_tombstones(tombstones, ts, base_ordinal)
    }

    /// Write a batch of range tombstones to the WAL buffer only (no skiplist
    /// insert, no sync). The caller must subsequently call:
    /// 1. [`commit_wal`] to durably sync the WAL.
    /// 2. [`publish_range_tombstones`] to insert into the in-memory set.
    ///
    /// This ensures tombstones are not visible to readers until the WAL sync
    /// succeeds, preventing ghost entries on fsync failure.
    pub fn put_range_tombstone_batch_wal_only(
        &self,
        tombstones: &[(&[u8], &[u8])],
        ts: u64,
        base_ordinal: u32,
    ) -> Result<()> {
        if tombstones.is_empty() {
            return Ok(());
        }
        let _ = base_ordinal
            .checked_add(u32::try_from(tombstones.len()).context("ordinal overflow")?)
            .context("ordinal overflow")?;

        if let Some(wal) = &self.wal {
            let ticket = wal.put_range_tombstone_batch(tombstones, ts)?;
            self.last_ticket
                .fetch_max(ticket + 1, std::sync::atomic::Ordering::Release);
        }

        Ok(())
    }

    /// Insert range tombstones into the in-memory set.
    ///
    /// Abort on panic during in-memory publication. If a panic (e.g. OOM)
    /// occurs after the WAL has durably recorded the tombstone but before
    /// the memtable has published it, unwinding would leave the engine in
    /// an inconsistent state. Aborting prevents that.
    pub(crate) fn publish_range_tombstones(
        &self,
        tombstones: &[(&[u8], &[u8])],
        ts: u64,
        base_ordinal: u32,
    ) -> Result<()> {
        struct AbortOnPanic;
        impl Drop for AbortOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    log::error!(
                        "panic during range tombstone publication — aborting to prevent WAL/memtable divergence"
                    );
                    std::process::abort();
                }
            }
        }
        let _abort_guard = AbortOnPanic;

        use crate::range_tombstone::RangeTombstone;
        for (i, (start, end)) in tombstones.iter().enumerate() {
            let ordinal = base_ordinal
                .checked_add(u32::try_from(i).expect("pre-validated"))
                .expect("pre-validated");
            self.range_tombstones.add(
                RangeTombstone {
                    start: Bytes::copy_from_slice(start),
                    end: Bytes::copy_from_slice(end),
                    ts,
                },
                ordinal,
            );
        }

        Ok(())
    }

    /// Access the range tombstone set for this memtable.
    pub fn range_tombstones(&self) -> &RangeTombstoneSet {
        &self.range_tombstones
    }

    /// Freeze the range tombstones into an immutable view with cached fragments.
    ///
    /// Called once when the memtable transitions from active to immutable.
    /// Uses `OnceLock` for interior mutability so it can be called through `&self`
    /// (the memtable may already be inside an `Arc` when frozen). Safe to call
    /// multiple times; only the first call has any effect.
    pub fn freeze_range_tombstones(&self) {
        if !self.range_tombstones.is_empty() {
            self.immutable_range_tombstones.get_or_init(|| {
                ImmutableRangeTombstoneSet::from_range_tombstone_set(&self.range_tombstones)
            });
        }
    }

    /// Access the immutable range-tombstone view (available after freeze).
    pub fn imm_range_tombstones(&self) -> Option<&ImmutableRangeTombstoneSet> {
        self.immutable_range_tombstones.get()
    }

    /// Convert a `Bound` into the key to pass to `overlaps()` as a start bound.
    /// `Included(x)` → `x`, `Excluded(x)` → `x ++ \0` (successor), `Unbounded` → `&[]`.
    fn bound_to_start(bound: Bound<&[u8]>) -> Option<Vec<u8>> {
        match bound {
            Bound::Included(x) => Some(x.to_vec()),
            Bound::Excluded(x) => {
                let mut v = x.to_vec();
                v.push(0);
                Some(v)
            }
            Bound::Unbounded => None,
        }
    }

    /// Convert a `Bound` into the key to pass to `overlaps()` as an end bound.
    /// `Included(x)` → `x ++ \0` (successor), `Excluded(x)` → `x`, `Unbounded` → `None`.
    fn bound_to_end(bound: Bound<&[u8]>) -> Option<Vec<u8>> {
        match bound {
            Bound::Included(x) => {
                let mut v = x.to_vec();
                v.push(0);
                Some(v)
            }
            Bound::Excluded(x) => Some(x.to_vec()),
            Bound::Unbounded => None,
        }
    }

    #[must_use]
    pub fn range_overlap(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        // Check point entries first.
        let point_overlap = if !self.map.is_empty() {
            let front = self.map.front().expect("map is not empty");
            let lo_raw = front.key();
            let back = self.map.back().expect("map is not empty");
            let hi_raw = back.key();
            // Decode user keys from internal keys (user_key + ts) when MVCC is
            // enabled, so comparisons with raw user key bounds are correct.
            let (lo, hi): (&[u8], &[u8]);
            let lo_decoded;
            let hi_decoded;
            if crate::key::TS_ENABLED {
                lo_decoded = crate::key::decode_user_key_cow(lo_raw);
                hi_decoded = crate::key::decode_user_key_cow(hi_raw);
                match (&lo_decoded, &hi_decoded) {
                    (Some(l), Some(h)) => {
                        lo = l.as_ref();
                        hi = h.as_ref();
                    }
                    _ => return false,
                }
            } else {
                lo = lo_raw;
                hi = hi_raw;
            }
            // Two ranges overlap iff query_start <= memtable_end && memtable_start <= query_end.
            let l_le_hi = match lower {
                Bound::Included(x) => x <= hi,
                Bound::Excluded(x) => x < hi,
                Bound::Unbounded => true,
            };
            let lo_le_u = match upper {
                Bound::Included(y) => lo <= y,
                Bound::Excluded(y) => lo < y,
                Bound::Unbounded => true,
            };
            l_le_hi && lo_le_u
        } else {
            false
        };

        if point_overlap {
            return true;
        }

        // Also check range tombstones.
        // `overlaps(l, end)` treats the range as [l, end) (half-open).
        // Use `bound_to_start`/`bound_to_end` to normalize Included/Excluded
        // bounds (adding `\0` successor for Included), then delegate.
        let start = Self::bound_to_start(lower);
        let end = Self::bound_to_end(upper);
        match (start, end) {
            (Some(ref l), Some(ref e)) => self.range_tombstones.overlaps(l, e, u64::MAX),
            (Some(ref l), None) => self
                .range_tombstones
                .raw()
                .iter()
                .any(|entry| l.as_slice() < entry.value().as_ref()),
            (None, Some(ref e)) => self.range_tombstones.overlaps(&[], e, u64::MAX),
            (None, None) => !self.range_tombstones.is_empty(),
        }
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to
/// iterator chapter for more information.
///
/// This is part of iterator.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
    /// Whether values are kind-prefixed (need to strip prefix in value()).
    vlog_enabled: bool,
    /// Optional vLog for dereferencing ValuePointer entries during scans.
    vlog: Option<Arc<ValueLog>>,
    /// Cached resolved value (stripped of kind prefix, ValuePointers dereferenced).
    resolved: Bytes,
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_resolved().as_ref()
    }

    fn key(&self) -> KeySlice<'_> {
        Key::from_slice(self.borrow_item().0.as_ref())
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let n = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
        });

        self.with_mut(|m| {
            *m.item = n;
            *m.resolved = Self::resolve_item_value(m.vlog, m.item);
        });

        Ok(())
    }

    fn raw_value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }
}

impl MemTableIterator {
    /// Resolve the value for the current item: dereference ValuePointers via vLog,
    /// strip kind prefix for Inline entries.
    fn resolve_item_value(vlog: &Option<Arc<ValueLog>>, item: &(Bytes, Bytes)) -> Bytes {
        let val = &item.1;
        if val.is_empty() {
            return val.clone();
        }

        // Tombstone — return as-is so callers can detect via is_tombstone_value
        if val[0] == KvKind::Tombstone as u8 {
            return val.clone();
        }
        // TTL Inline — strip 9-byte prefix (kind + expire_at_secs)
        if val[0] == KvKind::TtlInline as u8 {
            return if val.len() >= 9 {
                val.slice(9..)
            } else {
                Bytes::new()
            };
        }
        // TTL ValuePointer — dereference through vLog (pointer at offset 9)
        if val[0] == KvKind::TtlValuePointer as u8 {
            if val.len() < 25 {
                return Bytes::new();
            }
            let Some(vlog) = vlog else {
                return Bytes::new();
            };
            let Some(ptr) = crate::vlog::ValuePointer::try_decode(&val[9..]) else {
                return Bytes::new();
            };
            return match vlog.read(&ptr, &item.0) {
                std::result::Result::Ok(bytes) => bytes,
                std::result::Result::Err(_) => Bytes::new(),
            };
        }
        // Not a ValuePointer — strip kind prefix (Inline or unknown)
        if val[0] != KvKind::ValuePointer as u8 {
            return val.slice(1..);
        }

        // ValuePointer — dereference through vLog
        let Some(vlog) = vlog else {
            return val.slice(1..);
        };
        let Some(ptr) = crate::vlog::ValuePointer::try_decode(&val[1..]) else {
            return Bytes::new();
        };
        // With MVCC, vLog entries are keyed by the full encoded internal key
        // (user key + ts). Pass it directly for verification.
        match vlog.read(&ptr, &item.0) {
            std::result::Result::Ok(bytes) => bytes,
            std::result::Result::Err(_) => Bytes::new(),
        }
    }
}
