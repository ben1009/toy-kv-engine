pub mod txn;
mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use bytes::Bytes;

use crossbeam_skiplist::SkipMap;
use parking_lot::{Mutex, RwLock};

use self::{txn::Transaction, watermark::Watermark};
use crate::{
    key::{KeySlice, encode_internal_key},
    lsm_storage::LsmStorageInner,
    mem_table::MemTable,
};

pub(crate) struct CommittedTxnData {
    pub(crate) write_set: HashSet<Bytes>,
    // Stored for watermark/GC purposes; currently only write_set is read during
    // conflict detection. Suppress dead_code until GC consumes these fields.
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) reader_lock: RwLock<()>,
    pub(crate) current_ts: AtomicU64,
    pub(crate) watermark: Watermark,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            reader_lock: RwLock::new(()),
            current_ts: AtomicU64::new(initial_ts),
            watermark: Watermark::new(),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[allow(dead_code)]
    pub fn latest_commit_ts(&self) -> u64 {
        self.current_ts.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    pub fn update_commit_ts(&self, ts: u64) {
        self.current_ts.store(ts, Ordering::Release);
    }

    /// All ts (strictly) below this ts can be garbage collected.
    /// Acquires the write lock on `reader_lock` to ensure no reader is
    /// mid-registration (between reading `current_ts` and calling `add_reader`),
    /// which could cause premature GC of versions still in use.
    pub fn watermark(&self) -> u64 {
        let _guard = self.reader_lock.write();
        self.watermark
            .watermark()
            .unwrap_or(self.current_ts.load(Ordering::Acquire))
    }

    /// Physical deletion by compaction filters is only safe when no active
    /// readers remain. Readers reload the current LSM state on each access, so
    /// even readers with read_ts > cutoff_ts could observe disappearing keys if
    /// deletion were published while they are active. This check is
    /// intentionally stronger than `watermark > cutoff_ts` — it requires zero
    /// active readers regardless of their read timestamp.
    /// Acquires the write lock to ensure no reader is mid-registration.
    pub(crate) fn can_publish_filter_deletion(&self) -> bool {
        let _guard = self.reader_lock.write();
        self.watermark.watermark().is_none()
    }

    /// Allocate a commit timestamp under the write lock and write to the memtable.
    /// Returns the commit timestamp used.
    ///
    /// **Note:** This publishes to the skiplist before WAL sync. Prefer
    /// [`write_wal_only`] for the durable-write path.
    #[allow(dead_code)]
    pub fn write(
        &self,
        user_key: &[u8],
        value: &[u8],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        anyhow::ensure!(
            !(value.len() == 1 && value[0] == crate::vlog::KvKind::Tombstone as u8),
            "value must not be the tombstone marker byte (0x02)"
        );
        let _write_guard = self.write_lock.lock();
        // The caller commits the WAL after releasing storage locks.
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        memtable.put_no_sync(&encoded_key, value)?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok(commit_ts)
    }

    /// Write to WAL only (no skiplist insert). Returns `(commit_ts, encoded_key,
    /// prefixed_value)`. The caller must subsequently call
    /// [`MemTable::commit_wal`] then [`MemTable::publish_raw_batch`] to make
    /// the data visible to readers.
    ///
    /// This ensures data is not visible until the WAL sync succeeds.
    pub fn write_wal_only(
        &self,
        user_key: &[u8],
        value: &[u8],
        memtable: &MemTable,
    ) -> Result<(u64, Vec<u8>, Vec<u8>), anyhow::Error> {
        anyhow::ensure!(
            !(value.len() == 1 && value[0] == crate::vlog::KvKind::Tombstone as u8),
            "value must not be the tombstone marker byte (0x02)"
        );
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        let mut prefixed = Vec::with_capacity(1 + value.len());
        prefixed.push(crate::vlog::KvKind::Inline as u8);
        prefixed.extend_from_slice(value);
        memtable.write_wal_batch_only(&[(
            crate::key::KeySlice::from_slice(&encoded_key),
            prefixed.as_slice(),
        )])?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok((commit_ts, encoded_key, prefixed))
    }

    /// Write a tombstone (deletion marker) for the given user key.
    /// Returns the commit timestamp used.
    ///
    /// **Note:** This publishes to the skiplist before WAL sync. Prefer
    /// [`write_tombstone_wal_only`] for the durable-write path.
    #[allow(dead_code)]
    pub fn write_tombstone(
        &self,
        user_key: &[u8],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        memtable.put_tombstone_no_sync(&encoded_key)?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok(commit_ts)
    }

    /// Write a tombstone to WAL only. Returns `(commit_ts, encoded_key, tombstone_value)`.
    /// The caller must subsequently call [`MemTable::commit_wal`] then
    /// [`MemTable::publish_raw_batch`].
    pub fn write_tombstone_wal_only(
        &self,
        user_key: &[u8],
        memtable: &MemTable,
    ) -> Result<(u64, Vec<u8>, Vec<u8>), anyhow::Error> {
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        let tombstone_val = vec![crate::vlog::KvKind::Tombstone as u8];
        memtable.write_wal_batch_only(&[(
            crate::key::KeySlice::from_slice(&encoded_key),
            tombstone_val.as_slice(),
        )])?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok((commit_ts, encoded_key, tombstone_val))
    }

    /// Write a range tombstone covering `[start, end)`.
    /// Returns the commit timestamp used.
    pub fn write_range_tombstone(
        &self,
        start: &[u8],
        end: &[u8],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        memtable.put_range_tombstone(start, end, commit_ts, 0)?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok(commit_ts)
    }

    /// Write a batch of range tombstones atomically under a single commit timestamp.
    ///
    /// All entries share one `commit_ts` and receive sequential ordinals.
    /// Returns the commit timestamp used, or 0 if entries is empty.
    #[allow(dead_code)] // Kept for API completeness; prefer write_range_batch_no_sync.
    pub fn write_range_batch(
        &self,
        entries: &[(&[u8], &[u8])],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        if entries.is_empty() {
            return Ok(0);
        }
        anyhow::ensure!(
            entries.len() <= u32::MAX as usize,
            "range tombstone batch too large: {} entries",
            entries.len()
        );
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        // Single WAL write + sync for the entire batch, preserving ordinals.
        memtable.put_range_tombstone_batch(entries, commit_ts, 0)?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok(commit_ts)
    }

    /// Write a batch of range tombstones to WAL buffer only (no skiplist
    /// insert, no sync). Returns `(commit_ts, base_ordinal)`.
    ///
    /// The caller must subsequently call [`MemTable::commit_wal`] then
    /// [`MemTable::publish_range_tombstones`] to make tombstones visible.
    pub fn write_range_batch_wal_only(
        &self,
        entries: &[(&[u8], &[u8])],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        if entries.is_empty() {
            return Ok(0);
        }
        anyhow::ensure!(
            entries.len() <= u32::MAX as usize,
            "range tombstone batch too large: {} entries",
            entries.len()
        );
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        memtable.put_range_tombstone_batch_wal_only(entries, commit_ts, 0)?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok(commit_ts)
    }

    /// Write a batch of operations atomically under a single commit timestamp.
    /// Each entry is `(user_key, value, is_tombstone)`.
    /// Returns the commit timestamp used, or 0 if entries is empty.
    pub fn write_batch(
        &self,
        entries: &[(&[u8], &[u8], bool)],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        if entries.is_empty() {
            return Ok(0);
        }
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.current_ts.load(Ordering::Acquire) + 1;
        let encoded: Vec<(Vec<u8>, &[u8], bool)> = entries
            .iter()
            .map(|(key, value, is_tombstone)| {
                (encode_internal_key(key, commit_ts), *value, *is_tombstone)
            })
            .collect();
        // Always prefix non-tombstone values with KvKind::Inline so values
        // are self-describing regardless of vlog mode.
        let prefixed: Vec<Vec<u8>> = encoded
            .iter()
            .map(|(_, value, is_tombstone)| {
                if *is_tombstone {
                    vec![crate::vlog::KvKind::Tombstone as u8]
                } else {
                    let mut p = Vec::with_capacity(1 + value.len());
                    p.push(crate::vlog::KvKind::Inline as u8);
                    p.extend_from_slice(value);
                    p
                }
            })
            .collect();
        let raw: Vec<(KeySlice, &[u8])> = encoded
            .iter()
            .enumerate()
            .map(|(i, (key, _, _))| {
                let k = KeySlice::from_slice(key);
                (k, prefixed[i].as_slice())
            })
            .collect();
        memtable.put_raw_batch_no_sync(&raw)?;
        self.current_ts.store(commit_ts, Ordering::Release);

        Ok(commit_ts)
    }

    /// Get a read timestamp (the latest committed ts).
    /// This does NOT add a reader to the watermark — use `new_read_guard()` instead.
    #[allow(dead_code)]
    pub(crate) fn read_ts(&self) -> u64 {
        self.current_ts.load(Ordering::Acquire)
    }

    pub fn new_txn(
        self: &Arc<Self>,
        inner: Arc<LsmStorageInner>,
        serializable: bool,
    ) -> Arc<Transaction> {
        let read_guard = self.new_read_guard();
        let read_ts = read_guard.read_ts();
        let occ_sets = if serializable {
            (
                Some(Mutex::new(HashSet::new())),
                Some(Mutex::new(HashSet::new())),
            )
        } else {
            (None, None)
        };
        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(Transaction {
            read_ts,
            read_guard: Mutex::new(Some(read_guard)),
            inner,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            read_set: occ_sets.0,
            write_set: occ_sets.1,
            _not_sync: std::marker::PhantomData,
        })
    }

    /// Create a `ReadGuard` that atomically reads the latest commit timestamp
    /// and registers it in the watermark. The guard unregisters on drop.
    pub(crate) fn new_read_guard(self: &Arc<Self>) -> ReadGuard {
        ReadGuard::new_latest(Arc::clone(self))
    }

    /// Record a committed write set in `committed_txns` for serializable OCC.
    /// Prunes entries below watermark to prevent unbounded memory growth.
    pub(crate) fn record_committed_txn(
        &self,
        commit_ts: u64,
        write_set: std::collections::HashSet<bytes::Bytes>,
        read_ts: u64,
    ) {
        let watermark = self.watermark();
        let mut committed = self.committed_txns.lock();
        if let Some(cutoff) = watermark.checked_add(1) {
            *committed = committed.split_off(&cutoff);
        } else {
            committed.clear();
        }
        committed.insert(
            commit_ts,
            CommittedTxnData {
                write_set,
                read_ts,
                commit_ts,
            },
        );
    }
}

/// RAII guard that registers a read timestamp in the MVCC watermark.
///
/// While the guard is alive, compaction will not GC versions at or above
/// `read_ts`. On drop, the timestamp is unregistered and the watermark
/// advances if this was the oldest reader.
pub(crate) struct ReadGuard {
    read_ts: u64,
    mvcc: Arc<LsmMvccInner>,
}

impl ReadGuard {
    /// Read the latest commit timestamp and register it in the watermark
    /// atomically. Acquires the read lock on `reader_lock` so that concurrent
    /// readers can register simultaneously (shared lock), while `watermark()`
    /// and `can_publish_filter_deletion()` acquire the write lock to ensure no
    /// reader is mid-registration during compaction/GC decisions.
    pub(crate) fn new_latest(mvcc: Arc<LsmMvccInner>) -> Self {
        let read_ts = {
            let _guard = mvcc.reader_lock.read();
            let ts = mvcc.current_ts.load(Ordering::Acquire);
            mvcc.watermark.add_reader(ts);
            ts
        };
        Self { read_ts, mvcc }
    }

    pub(crate) fn read_ts(&self) -> u64 {
        self.read_ts
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.mvcc.watermark.remove_reader(self.read_ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_mvcc_inner_new() {
        let mvcc = LsmMvccInner::new(0);
        assert_eq!(mvcc.latest_commit_ts(), 0);
        assert_eq!(mvcc.read_ts(), 0);
        assert_eq!(mvcc.watermark(), 0); // no readers → returns latest_commit_ts
    }

    #[test]
    fn test_mvcc_inner_write() {
        let mvcc = LsmMvccInner::new(0);
        let memtable = crate::mem_table::MemTable::create(0, false);
        mvcc.write(b"key1", b"val1", &memtable).unwrap();
        assert_eq!(mvcc.latest_commit_ts(), 1);
        mvcc.write(b"key1", b"val2", &memtable).unwrap();
        assert_eq!(mvcc.latest_commit_ts(), 2);
        // Both versions should be in the memtable (use versioned lookup
        // which matches the bloom filter's decoded-user-key hashing)
        // Verify memtable is not empty
        assert!(
            !memtable.is_empty(),
            "memtable should have entries after write"
        );
        let v2 = memtable.get_versioned(b"key1", 2);
        assert!(
            v2.is_some(),
            "ts=2 version should exist, commit_ts={}",
            mvcc.latest_commit_ts()
        );
        assert_eq!(v2.unwrap().as_ref(), b"val2");
    }

    #[test]
    fn test_mvcc_inner_update_commit_ts() {
        let mvcc = LsmMvccInner::new(0);
        mvcc.update_commit_ts(100);
        assert_eq!(mvcc.latest_commit_ts(), 100);
        assert_eq!(mvcc.read_ts(), 100);
    }

    // --- ReadGuard tests ---

    #[test]
    fn test_read_guard_registers_in_watermark() {
        let mvcc = Arc::new(LsmMvccInner::new(50));
        let memtable = crate::mem_table::MemTable::create(0, false);
        mvcc.write(b"k", b"v", &memtable).unwrap(); // ts=51

        // Create guard at ts=51 while latest is still 51
        let guard = mvcc.new_read_guard();
        assert_eq!(guard.read_ts(), 51);
        // Watermark should be 51 (registered reader), not 51 (latest fallback)
        // Both happen to be 51 here, but the reader IS registered
        assert_eq!(mvcc.watermark.watermark(), Some(51));
    }

    #[test]
    fn test_read_guard_drop_unregisters() {
        let mvcc = Arc::new(LsmMvccInner::new(50));
        let memtable = crate::mem_table::MemTable::create(0, false);

        {
            let _guard = mvcc.new_read_guard(); // read_ts=50
            mvcc.write(b"k", b"v1", &memtable).unwrap(); // ts=51
            mvcc.write(b"k", b"v2", &memtable).unwrap(); // ts=52
            // Guard is still alive at ts=50, so watermark is 50
            assert_eq!(mvcc.watermark(), 50);
        }
        // After guard drops, no readers → watermark falls back to latest (52)
        assert_eq!(mvcc.watermark(), 52);
    }

    #[test]
    fn test_read_guard_multiple_readers() {
        let mvcc = Arc::new(LsmMvccInner::new(50));
        // Write to advance the timestamp
        let memtable = crate::mem_table::MemTable::create(0, false);
        mvcc.write(b"k", b"v1", &memtable).unwrap(); // ts=51
        mvcc.write(b"k", b"v2", &memtable).unwrap(); // ts=52

        let guard_old = ReadGuard::new_latest(Arc::clone(&mvcc)); // read_ts=52
        mvcc.write(b"k", b"v3", &memtable).unwrap(); // ts=53
        let guard_new = ReadGuard::new_latest(Arc::clone(&mvcc)); // read_ts=53

        // Watermark should be the oldest reader (52)
        assert_eq!(mvcc.watermark(), 52);

        drop(guard_old);
        // Now the only reader is at 53
        assert_eq!(mvcc.watermark(), 53);

        drop(guard_new);
        // No readers → falls back to latest_commit_ts (53)
        assert_eq!(mvcc.watermark(), 53);
    }

    #[test]
    fn test_read_guard_duplicate_ts() {
        let mvcc = Arc::new(LsmMvccInner::new(10));
        let guard1 = ReadGuard::new_latest(Arc::clone(&mvcc));
        let guard2 = ReadGuard::new_latest(Arc::clone(&mvcc));

        // Both guards have the same read_ts=10, refcount=2
        assert_eq!(guard1.read_ts(), 10);
        assert_eq!(guard2.read_ts(), 10);
        assert_eq!(mvcc.watermark(), 10);

        drop(guard1);
        // Still one reader at ts=10
        assert_eq!(mvcc.watermark(), 10);

        drop(guard2);
        // No readers → falls back to latest_commit_ts
        assert_eq!(mvcc.watermark(), 10);
    }

    #[test]
    fn test_read_guard_watermark_pins_after_write() {
        let mvcc = Arc::new(LsmMvccInner::new(100));
        let guard = mvcc.new_read_guard(); // read_ts=100
        assert_eq!(guard.read_ts(), 100);

        // Advance the timestamp — watermark should stay pinned at 100
        let memtable = crate::mem_table::MemTable::create(0, false);
        mvcc.write(b"k", b"v1", &memtable).unwrap(); // ts=101
        mvcc.write(b"k", b"v2", &memtable).unwrap(); // ts=102
        assert_eq!(mvcc.watermark(), 100);

        // After dropping the guard, watermark advances to latest
        drop(guard);
        assert_eq!(mvcc.watermark(), 102);
    }

    #[test]
    fn test_can_publish_filter_deletion_requires_no_active_readers() {
        let mvcc = Arc::new(LsmMvccInner::new(7));
        assert!(mvcc.can_publish_filter_deletion());

        let guard = mvcc.new_read_guard();
        assert!(!mvcc.can_publish_filter_deletion());
        drop(guard);

        assert!(mvcc.can_publish_filter_deletion());
    }

    #[test]
    fn test_txn_put_get_local() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path(),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"name", b"alice").unwrap();
        assert_eq!(
            txn.get(b"name").unwrap(),
            Some(Bytes::from_static(b"alice"))
        );
        // Not committed yet — engine shouldn't see it.
        assert_eq!(engine.get(b"name").unwrap(), None);
    }

    #[test]
    fn test_txn_delete_local() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path(),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"k", b"v").unwrap();
        assert_eq!(txn.get(b"k").unwrap(), Some(Bytes::from_static(b"v")));
        txn.delete(b"k").unwrap();
        assert_eq!(txn.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_txn_commit_visible() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path(),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"k", b"v").unwrap();
        txn.commit().unwrap();
        // After commit, engine should see the value.
        assert_eq!(engine.get(b"k").unwrap(), Some(Bytes::from_static(b"v")));
    }

    #[test]
    fn test_txn_double_commit_fails() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path(),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"k", b"v").unwrap();
        txn.commit().unwrap();
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_txn_snapshot_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path(),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        // Write initial value.
        engine.put(b"k", b"v1").unwrap();
        // Start txn — should see v1 at its snapshot.
        let txn = engine.new_txn().unwrap();
        assert_eq!(txn.get(b"k").unwrap(), Some(Bytes::from_static(b"v1")));
        // Write a new value outside the txn.
        engine.put(b"k", b"v2").unwrap();
        // Txn should still see v1 (snapshot isolation).
        assert_eq!(txn.get(b"k").unwrap(), Some(Bytes::from_static(b"v1")));
    }

    #[test]
    fn test_txn_local_writes_shadow_engine() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(
            dir.path(),
            crate::lsm_storage::LsmStorageOptions::default_for_test(),
        )
        .unwrap();
        engine.put(b"k", b"engine_val").unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"k", b"txn_val").unwrap();
        // Local write should shadow engine value.
        assert_eq!(txn.get(b"k").unwrap(), Some(Bytes::from_static(b"txn_val")));
    }

    // --- OCC (Optimistic Concurrency Control) tests ---

    fn serializable_opts() -> crate::lsm_storage::LsmStorageOptions {
        crate::lsm_storage::LsmStorageOptions {
            serializable: true,
            ..crate::lsm_storage::LsmStorageOptions::default_for_test()
        }
    }

    #[test]
    fn test_occ_read_write_conflict() {
        // txn reads k, writes other; non-txn writes k after txn's read_ts -> conflict
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"k", b"v0").unwrap();
        let txn = engine.new_txn().unwrap();
        // Read k — records k in read_set.
        assert_eq!(txn.get(b"k").unwrap(), Some(Bytes::from_static(b"v0")));
        // Write something else so write_set is non-empty (triggers OCC check).
        txn.put(b"other", b"v").unwrap();
        // Non-transactional write to k after txn's read_ts.
        engine.put(b"k", b"v1").unwrap();
        // Commit should fail: committed_txn {"k"} ∩ read_set {"k"} ≠ ∅.
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_occ_no_conflict_different_keys() {
        // txn reads k1, non-txn writes k2, txn commits -> ok
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"k1", b"v0").unwrap();
        engine.put(b"k2", b"v0").unwrap();
        let txn = engine.new_txn().unwrap();
        assert_eq!(txn.get(b"k1").unwrap(), Some(Bytes::from_static(b"v0")));
        engine.put(b"k2", b"v1").unwrap();
        assert!(txn.commit().is_ok());
    }

    #[test]
    fn test_occ_no_conflict_before_read_ts() {
        // txn reads k, non-txn writes k before txn's read_ts, txn commits -> ok
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"k", b"v0").unwrap();
        // write happens before txn starts
        engine.put(b"k", b"v1").unwrap();
        let txn = engine.new_txn().unwrap();
        assert_eq!(txn.get(b"k").unwrap(), Some(Bytes::from_static(b"v1")));
        // No writes after txn's read_ts — no conflict.
        assert!(txn.commit().is_ok());
    }

    #[test]
    fn test_occ_negative_read_recorded() {
        // get returns None, txn writes something else, non-txn inserts key -> conflict
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        let txn = engine.new_txn().unwrap();
        // Key doesn't exist — still recorded in read_set.
        assert_eq!(txn.get(b"new_key").unwrap(), None);
        // Write something else so write_set is non-empty (triggers OCC check).
        txn.put(b"other", b"v").unwrap();
        // Non-transactional insert after txn's read_ts.
        engine.put(b"new_key", b"val").unwrap();
        // Should conflict: committed_txn {"new_key"} ∩ read_set {"new_key", "other"} ≠ ∅.
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_occ_negative_read_tombstoned_key() {
        // Reading a tombstoned key (deleted) records a negative read.
        // If another writer inserts that key after read_ts, commit aborts.
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();

        // Write then delete — key is tombstoned.
        engine.put(b"dead_key", b"v1").unwrap();
        engine.delete(b"dead_key").unwrap();

        // Start txn — get returns None (tombstoned), still recorded in read_set.
        let txn = engine.new_txn().unwrap();
        assert_eq!(txn.get(b"dead_key").unwrap(), None);
        // Write something else so write_set is non-empty.
        txn.put(b"other", b"v").unwrap();

        // Non-transactional insert after txn's read_ts.
        engine.put(b"dead_key", b"resurrected").unwrap();

        // Should conflict: committed_txn {"dead_key"} ∩ read_set {"dead_key", "other"} ≠ ∅.
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_occ_scan_rejected_for_serializable() {
        // scan() is not supported for serializable transactions until
        // phantom/range predicate tracking is implemented.
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"a", b"1").unwrap();
        let txn = engine.new_txn().unwrap();
        assert!(
            txn.scan(
                std::ops::Bound::Included(b"a".as_slice()),
                std::ops::Bound::Included(b"z".as_slice()),
            )
            .is_err()
        );
    }

    #[test]
    fn test_occ_mutation_after_commit() {
        // put/delete after commit -> error
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        let txn = engine.new_txn().unwrap();
        txn.put(b"k", b"v").unwrap();
        txn.commit().unwrap();
        assert!(txn.put(b"k2", b"v2").is_err());
        assert!(txn.delete(b"k").is_err());
        assert!(txn.get(b"k").is_err());
    }

    #[test]
    fn test_occ_read_only_commits() {
        // read-only txn skips conflict detection
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"k", b"v").unwrap();
        let txn = engine.new_txn().unwrap();
        txn.get(b"k").unwrap();
        // No writes in txn — commit should succeed even with concurrent writes.
        engine.put(b"k", b"v2").unwrap();
        assert!(txn.commit().is_ok());
    }

    #[test]
    fn test_occ_two_txns_write_same_key() {
        // txn1 and txn2 both read k, both write k; txn1 commits first, txn2 conflicts.
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"k", b"v0").unwrap();
        let txn1 = engine.new_txn().unwrap();
        let txn2 = engine.new_txn().unwrap();
        txn1.get(b"k").unwrap();
        txn2.get(b"k").unwrap();
        txn1.put(b"k", b"v1").unwrap();
        txn2.put(b"k", b"v2").unwrap();
        assert!(txn1.commit().is_ok());
        // txn2's read_set {"k"} intersects txn1's committed write_set {"k"}.
        assert!(txn2.commit().is_err());
    }

    #[test]
    fn test_occ_read_then_conflicting_txn_write() {
        // txn1 reads k; txn2 writes k and commits; txn1 writes k and tries to commit.
        // Conflict: txn2's write_set {"k"} intersects txn1's read_set {"k"}.
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"k", b"v0").unwrap();
        let txn1 = engine.new_txn().unwrap();
        let txn2 = engine.new_txn().unwrap();
        txn1.get(b"k").unwrap();
        txn2.put(b"k", b"v2").unwrap();
        assert!(txn2.commit().is_ok());
        txn1.put(b"k", b"v1").unwrap();
        assert!(txn1.commit().is_err());
    }
}
