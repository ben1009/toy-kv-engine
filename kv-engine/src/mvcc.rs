pub mod txn;
mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    sync::atomic::AtomicBool,
};

use bytes::Bytes;

use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use self::{txn::Transaction, watermark::Watermark};
use crate::{
    key::{KeySlice, encode_internal_key},
    lsm_storage::LsmStorageInner,
    mem_table::MemTable,
};

pub(crate) struct CommittedTxnData {
    pub(crate) write_set: HashSet<Bytes>,
    pub(crate) read_ts: u64,
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    /// Allocate a commit timestamp under the write lock and write to the memtable.
    /// Returns the commit timestamp used.
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
        // Write to the memtable FIRST, then advance ts.0.  This ordering
        // guarantees that a concurrent `new_read_guard()` never observes a
        // read_ts whose version hasn't been written yet (no torn reads).
        let commit_ts = self.ts.lock().0 + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        memtable.put(&encoded_key, value)?;
        self.ts.lock().0 = commit_ts;
        Ok(commit_ts)
    }

    /// Write a tombstone (deletion marker) for the given user key.
    /// Returns the commit timestamp used.
    pub fn write_tombstone(
        &self,
        user_key: &[u8],
        memtable: &MemTable,
    ) -> Result<u64, anyhow::Error> {
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.ts.lock().0 + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        memtable.put_tombstone(&encoded_key)?;
        self.ts.lock().0 = commit_ts;
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
        let commit_ts = self.ts.lock().0 + 1;
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
        memtable.put_raw_batch(&raw)?;
        self.ts.lock().0 = commit_ts;
        Ok(commit_ts)
    }

    /// Get a read timestamp (the latest committed ts).
    /// This does NOT add a reader to the watermark — use `new_read_guard()` instead.
    pub(crate) fn read_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn new_txn(
        self: &Arc<Self>,
        inner: Arc<LsmStorageInner>,
        serializable: bool,
    ) -> Arc<Transaction> {
        let read_guard = self.new_read_guard();
        let occ_sets = if serializable {
            (
                Some(Mutex::new(HashSet::new())),
                Some(Mutex::new(HashSet::new())),
            )
        } else {
            (None, None)
        };
        Arc::new(Transaction {
            read_guard,
            inner,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            read_set: occ_sets.0,
            write_set: occ_sets.1,
        })
    }

    /// Create a `ReadGuard` that atomically reads the latest commit timestamp
    /// and registers it in the watermark. The guard unregisters on drop.
    pub(crate) fn new_read_guard(self: &Arc<Self>) -> ReadGuard {
        ReadGuard::new_latest(Arc::clone(self))
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
    /// Atomically read the latest commit timestamp and register it in the
    /// watermark. The ts mutex is held for both the read and the registration.
    /// Because `write()` also holds this mutex when incrementing the counter,
    /// a ReadGuard will always see a commit_ts that is either already committed
    /// or not yet started — never a partially-applied write.
    pub(crate) fn new_latest(mvcc: Arc<LsmMvccInner>) -> Self {
        let read_ts = {
            let mut ts = mvcc.ts.lock();
            let latest = ts.0;
            ts.1.add_reader(latest);
            latest
        };
        Self { read_ts, mvcc }
    }

    pub(crate) fn read_ts(&self) -> u64 {
        self.read_ts
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        let mut ts = self.mvcc.ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterators::StorageIterator;
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
        let memtable = crate::mem_table::MemTable::create(0);
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
        let memtable = crate::mem_table::MemTable::create(0);
        mvcc.write(b"k", b"v", &memtable).unwrap(); // ts=51

        // Create guard at ts=51 while latest is still 51
        let guard = mvcc.new_read_guard();
        assert_eq!(guard.read_ts(), 51);
        // Watermark should be 51 (registered reader), not 51 (latest fallback)
        // Both happen to be 51 here, but the reader IS registered
        let ts = mvcc.ts.lock();
        assert_eq!(ts.1.watermark(), Some(51));
    }

    #[test]
    fn test_read_guard_drop_unregisters() {
        let mvcc = Arc::new(LsmMvccInner::new(50));
        let memtable = crate::mem_table::MemTable::create(0);

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
        let memtable = crate::mem_table::MemTable::create(0);
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
        let memtable = crate::mem_table::MemTable::create(0);
        mvcc.write(b"k", b"v1", &memtable).unwrap(); // ts=101
        mvcc.write(b"k", b"v2", &memtable).unwrap(); // ts=102
        assert_eq!(mvcc.watermark(), 100);

        // After dropping the guard, watermark advances to latest
        drop(guard);
        assert_eq!(mvcc.watermark(), 102);
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
        let mut opts = crate::lsm_storage::LsmStorageOptions::default_for_test();
        opts.serializable = true;
        opts
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
    fn test_occ_scan_records_keys() {
        // scan yields keys, txn writes something, non-txn writes scanned key -> conflict
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::lsm_storage::KvEngine::open(dir.path(), serializable_opts()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
        engine.put(b"c", b"3").unwrap();
        let txn = engine.new_txn().unwrap();
        // Scan records yielded keys in read_set.
        let mut iter = txn
            .scan(
                std::ops::Bound::Included(b"a".as_slice()),
                std::ops::Bound::Included(b"c".as_slice()),
            )
            .unwrap();
        while iter.is_valid() {
            iter.next().unwrap();
        }
        // Write something else so write_set is non-empty (triggers OCC check).
        txn.put(b"other", b"v").unwrap();
        // Non-transactional write to a scanned key.
        engine.put(b"b", b"updated").unwrap();
        // Should conflict: committed_txn {"b"} ∩ read_set {"a","b","c",...} ≠ ∅.
        assert!(txn.commit().is_err());
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
