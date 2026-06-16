use std::{
    ops::Bound,
    path::Path,
    sync::{Arc, atomic::AtomicUsize},
};

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::{
    iterators::StorageIterator,
    key::{Key, KeySlice, shared_bytes_from_slice},
    range_tombstone::RangeTombstoneSet,
    table::{SsTableBuilder, bloom::IncrementalBloom},
    vlog::{KvKind, ValueLog, ValuePointer},
    wal::Wal,
};

/// Expected number of entries per memtable for bloom filter sizing.
/// At 1KB values and 1MB SST target, ~1000 entries. We size generously.
const BLOOM_EXPECTED_ENTRIES: usize = 4096;
const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

/// A basic mem-table based on crossbeam-skiplist.
///
/// A basic memtable implementation. It will be incrementally extended in other modules.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
    /// When true, values in the map are kind-prefixed: `[KvKind:1][payload]`.
    vlog_enabled: bool,
    /// Incremental bloom filter for negative lookups. Avoids skiplist epoch pin
    /// overhead when the key is not in this memtable.
    bloom: IncrementalBloom,
    /// Range tombstones stored in this memtable.
    range_tombstones: RangeTombstoneSet,
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
    /// Create a new mem-table. id is the sst_id when flush this memtable to sst
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
            vlog_enabled: false,
            bloom: IncrementalBloom::new(BLOOM_EXPECTED_ENTRIES, BLOOM_FALSE_POSITIVE_RATE),
            range_tombstones: RangeTombstoneSet::new(),
        }
    }

    /// Create a new mem-table with vLog (kind-prefixed values).
    pub fn create_vlog(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
            vlog_enabled: true,
            bloom: IncrementalBloom::new(BLOOM_EXPECTED_ENTRIES, BLOOM_FALSE_POSITIVE_RATE),
            range_tombstones: RangeTombstoneSet::new(),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut ret = Self::create(id);
        let wal = Wal::create(path)?;
        ret.wal = Some(wal);

        Ok(ret)
    }

    /// Create a new mem-table with WAL and vLog (kind-prefixed values).
    pub fn create_with_wal_vlog(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut ret = Self::create_vlog(id);
        let wal = Wal::create(path)?;
        ret.wal = Some(wal);

        Ok(ret)
    }

    /// Create a memtable from WAL. Returns the memtable and the max commit_ts found.
    ///
    /// Uses [`Wal::recover`] which replays point entries into the skiplist but
    /// silently discards range tombstones. For full recovery including range
    /// tombstones, use [`recover_from_wal_with_range_tombstones`].
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<(Self, u64)> {
        let mut ret = Self::create(id);
        let (wal, max_ts) = Wal::recover(path, &ret.map)?;
        ret.wal = Some(wal);
        // Populate bloom filter from recovered entries
        ret.rebuild_bloom();

        Ok((ret, max_ts))
    }

    /// Create a memtable from WAL with vLog (kind-prefixed values). Returns the memtable and max
    /// commit_ts.
    pub fn recover_from_wal_vlog(id: usize, path: impl AsRef<Path>) -> Result<(Self, u64)> {
        let mut ret = Self::create_vlog(id);
        let (wal, max_ts) = Wal::recover(path, &ret.map)?;
        ret.wal = Some(wal);
        // Populate bloom filter from recovered entries
        ret.rebuild_bloom();

        Ok((ret, max_ts))
    }

    /// Create a memtable from WAL with full range-tombstone recovery.
    ///
    /// Unlike [`recover_from_wal`], this method uses
    /// [`Wal::recover_with_range_tombstones`] to also populate the memtable's
    /// [`RangeTombstoneSet`] from WAL v3 range-tombstone entries.
    pub fn recover_from_wal_with_range_tombstones(
        id: usize,
        path: impl AsRef<Path>,
    ) -> Result<(Self, u64)> {
        let mut ret = Self::create(id);
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
        } else {
            let h = super::table::bloom::hash_key(encoded_internal_key);
            if !self.bloom.may_contain_hash(h) {
                return None;
            }
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
            let ts = crate::key::extract_ts(found_key).unwrap_or(0);
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
            if let Some(found_user_key) = crate::key::encoded_user_key_prefix(found_key) {
                if found_user_key != seek_prefix {
                    break;
                }
            } else {
                break;
            }
            let ts = crate::key::extract_ts(found_key).unwrap_or(0);
            if ts > read_ts {
                continue;
            }
            return Some((entry.value().clone(), found_key.to_vec()));
        }
        None
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
        self.put_raw_batch_inner(data, false)
    }

    /// Put a batch of raw (pre-prefixed) key-value pairs.
    pub fn put_raw_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        self.put_raw_batch_inner(data, true)
    }

    fn put_raw_batch_inner(&self, data: &[(KeySlice, &[u8])], write_wal: bool) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if write_wal && let Some(wal) = &self.wal {
            // Extract commit_ts from the first key. All entries in a batch
            // share the same commit_ts since they are committed atomically.
            let commit_ts = data
                .first()
                .and_then(|(k, _)| crate::key::extract_ts(k.raw_ref()))
                .unwrap_or(0);
            let entries: Vec<(&[u8], &[u8])> =
                data.iter().map(|(k, v)| (k.raw_ref(), *v)).collect();
            wal.put_batch(&entries, commit_ts)?;
            wal.sync()?;
        }

        let mut buf = Vec::new();
        for (key, value) in data {
            // Update bloom filter BEFORE skiplist insert. This ensures that a
            // concurrent reader never sees a false negative (bloom says "not
            // contained" for a key that IS in the skiplist). The worst case is
            // a false positive (bloom says "contained" but key not yet inserted),
            // which just causes an unnecessary skiplist probe — harmless.
            // Hash the decoded user key (not the encoded internal key) so that
            // bloom filter lookups using raw user keys still match.
            buf.clear();
            key.decode_user_key_into(&mut buf);
            self.bloom.push_hash(super::table::bloom::hash_key(&buf));
            self.map.insert(
                shared_bytes_from_slice(key.raw_ref()),
                shared_bytes_from_slice(value),
            );

            self.approximate_size.fetch_add(
                std::mem::size_of_val(key) + std::mem::size_of_val(*value),
                std::sync::atomic::Ordering::SeqCst,
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
        iter.next().unwrap();

        iter
    }

    /// Flush the mem-table to SSTable. Implement in scan and flush.
    /// When vlog_enabled, checks the KvKind prefix: ValuePointer and Tombstone
    /// entries are passed through via `add_raw()` to preserve the marker;
    /// Inline entries have their prefix stripped and go through `add()` for
    /// value separation.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        // Phase 1/2 guard: refuse to flush a memtable containing range
        // tombstones until SST v4 write support exists (Phase 3). Without
        // this guard, a flush would produce an SST with no range-tombstone
        // data, and the subsequent WAL deletion would permanently lose them.
        anyhow::ensure!(
            self.range_tombstones.is_empty(),
            "cannot flush memtable containing range tombstones: \
             SST v4 write support not yet implemented (Phase 3)"
        );

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

    /// Only use this function when closing the database.
    /// Returns `false` when either point entries or range tombstones are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && self.range_tombstones.is_empty()
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
            wal.put_range_tombstone_batch(tombstones, ts)?;
            wal.sync()?;
        }

        // Abort on panic during in-memory publication. If a panic (e.g. OOM)
        // occurs after the WAL has durably recorded the tombstone but before
        // the memtable has published it, unwinding would leave the engine in
        // an inconsistent state. Aborting prevents that.
        struct AbortOnPanic;
        impl Drop for AbortOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
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

    #[must_use]
    pub fn range_overlap(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        // Check point entries first.
        let point_overlap = if !self.map.is_empty() {
            let front = self.map.front().expect("map is not empty");
            let lo = front.key();
            let back = self.map.back().expect("map is not empty");
            let hi = back.key();
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
        // For `Bound::Included(u)`, we need to include tombstones starting
        // at `u` itself, so append a `\0` byte to create `u`'s
        // lexicographical successor.
        match (lower, upper) {
            (Bound::Included(l), Bound::Included(u)) => {
                let mut u_succ = u.to_vec();
                u_succ.push(0);
                self.range_tombstones.overlaps(l, &u_succ, u64::MAX)
            }
            (Bound::Included(l), Bound::Excluded(u)) => {
                self.range_tombstones.overlaps(l, u, u64::MAX)
            }
            (Bound::Excluded(l), Bound::Included(u)) => {
                let mut l_succ = l.to_vec();
                l_succ.push(0);
                let mut u_succ = u.to_vec();
                u_succ.push(0);
                self.range_tombstones.overlaps(&l_succ, &u_succ, u64::MAX)
            }
            (Bound::Excluded(l), Bound::Excluded(u)) => {
                let mut l_succ = l.to_vec();
                l_succ.push(0);
                self.range_tombstones.overlaps(&l_succ, u, u64::MAX)
            }
            // Unbounded lower: treat as starting from the empty key.
            (Bound::Unbounded, Bound::Included(u)) => {
                let mut u_succ = u.to_vec();
                u_succ.push(0);
                self.range_tombstones.overlaps(&[], &u_succ, u64::MAX)
            }
            (Bound::Unbounded, Bound::Excluded(u)) => {
                self.range_tombstones.overlaps(&[], u, u64::MAX)
            }
            // Unbounded upper: check if any tombstone's end > lower bound.
            (Bound::Included(l), Bound::Unbounded) => self
                .range_tombstones
                .raw()
                .iter()
                .any(|entry| l < entry.value().as_ref()),
            (Bound::Excluded(l), Bound::Unbounded) => {
                let mut l_succ = l.to_vec();
                l_succ.push(0);
                self.range_tombstones
                    .raw()
                    .iter()
                    .any(|entry| l_succ.as_slice() < entry.value().as_ref())
            }
            // Both unbounded: any tombstone overlaps.
            (Bound::Unbounded, Bound::Unbounded) => !self.range_tombstones.is_empty(),
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
            *m.resolved = Self::resolve_item_value(m.vlog, m.vlog_enabled, m.item);
        });

        Ok(())
    }
}

impl MemTableIterator {
    /// Resolve the value for the current item: dereference ValuePointers via vLog,
    /// strip kind prefix for Inline entries.
    fn resolve_item_value(
        vlog: &Option<Arc<ValueLog>>,
        _vlog_enabled: &bool,
        item: &(Bytes, Bytes),
    ) -> Bytes {
        let val = &item.1;
        if val.is_empty() {
            return val.clone();
        }

        // Tombstone — return as-is so callers can detect via is_tombstone_value
        if val[0] == KvKind::Tombstone as u8 {
            return val.clone();
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
