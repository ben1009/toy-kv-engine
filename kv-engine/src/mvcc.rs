pub mod txn;
mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use parking_lot::Mutex;

use self::{txn::Transaction, watermark::Watermark};
use crate::{
    key::{KeySlice, encode_internal_key},
    lsm_storage::LsmStorageInner,
    mem_table::MemTable,
};

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
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
    pub fn write(
        &self,
        user_key: &[u8],
        value: &[u8],
        memtable: &MemTable,
    ) -> Result<(), anyhow::Error> {
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
        Ok(())
    }

    /// Write a tombstone (deletion marker) for the given user key.
    pub fn write_tombstone(
        &self,
        user_key: &[u8],
        memtable: &MemTable,
    ) -> Result<(), anyhow::Error> {
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.ts.lock().0 + 1;
        let encoded_key = encode_internal_key(user_key, commit_ts);
        memtable.put_tombstone(&encoded_key)?;
        self.ts.lock().0 = commit_ts;
        Ok(())
    }

    /// Write a batch of operations atomically under a single commit timestamp.
    /// Each entry is `(user_key, value, is_tombstone)`.
    pub fn write_batch(
        &self,
        entries: &[(&[u8], &[u8], bool)],
        memtable: &MemTable,
    ) -> Result<(), anyhow::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let _write_guard = self.write_lock.lock();
        let commit_ts = self.ts.lock().0 + 1;
        let encoded: Vec<(Vec<u8>, &[u8], bool)> = entries
            .iter()
            .map(|(key, value, is_tombstone)| {
                (
                    encode_internal_key(key, commit_ts),
                    *value,
                    *is_tombstone,
                )
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
        Ok(())
    }

    /// Get a read timestamp (the latest committed ts).
    /// This does NOT add a reader to the watermark — use `new_read_guard()` instead.
    pub(crate) fn read_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, serializable: bool) -> Arc<Transaction> {
        unimplemented!()
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
}
