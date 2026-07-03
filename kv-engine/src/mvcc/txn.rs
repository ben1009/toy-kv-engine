use std::{collections::HashSet, ops::Bound, sync::Arc, sync::atomic::AtomicBool};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    blocking_executor::BlockingExecutor,
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{AdmissionGuard, LsmStorageInner, prefix_upper_bound},
    mem_table::map_bound,
    mvcc::ReadGuard,
};

/// An MVCC transaction that provides snapshot isolation.
///
/// Reads see a consistent snapshot at the transaction's `read_ts`.
/// Writes are buffered locally and only become visible to other
/// transactions on [`commit`](Transaction::commit).
pub struct Transaction {
    /// Cached read timestamp for snapshot reads.
    pub(crate) read_ts: u64,
    /// Registers read_ts in the MVCC watermark, preventing GC of visible
    /// versions. Wrapped in Mutex<Option<>> so commit() can drop it early
    /// and unpin the watermark.
    pub(crate) read_guard: Mutex<Option<ReadGuard>>,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Read set for OCC conflict detection (None when not serializable).
    pub(crate) read_set: Option<Mutex<HashSet<Bytes>>>,
    /// Write set for OCC conflict detection (None when not serializable).
    pub(crate) write_set: Option<Mutex<HashSet<Bytes>>>,
    /// Keeps the transaction registered with shutdown tracking until commit or drop.
    pub(crate) lifecycle_guard: Mutex<Option<AdmissionGuard>>,
    /// Bounded blocking executor for offloading sync I/O in async txn methods.
    pub(crate) blocking: BlockingExecutor,
    /// Transaction is intentionally `!Sync` — it must be used from a single
    /// thread. Concurrent access to `local_storage`, `read_set`, and
    /// `write_set` without external synchronization would be unsound.
    /// Uses `Cell<()>` instead of `*const ()` to keep `Send` for async runtimes.
    pub(crate) _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl Transaction {
    fn ensure_not_committed(&self) -> Result<()> {
        anyhow::ensure!(
            !self.committed.load(std::sync::atomic::Ordering::SeqCst),
            "transaction already committed"
        );
        Ok(())
    }

    /// Get a value by key.
    ///
    /// Checks local writes first (shadowing the engine), then falls back
    /// to the engine at the transaction's snapshot timestamp.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_not_committed()?;
        // Record this key in the read set for OCC conflict detection,
        // even if a local write shadows the engine read.
        if let Some(ref rs) = self.read_set {
            let mut guard = rs.lock();
            if !guard.contains(key) {
                guard.insert(Bytes::copy_from_slice(key));
            }
        }
        // Check local writes first — they shadow the engine.
        if let Some(entry) = self.local_storage.get(key) {
            let val = entry.value();
            // Tombstone in local storage means deleted within this txn.
            if crate::vlog::KvKind::is_tombstone_value(val) {
                return Ok(None);
            }
            return Ok(Some(val.clone()));
        }
        // Fall back to engine read at our snapshot timestamp.

        self.inner.get_with_ts(key, self.read_ts)
    }

    /// Get a value by key, asynchronously.
    ///
    /// Checks local writes first (CPU-only), then offloads the
    /// engine read (may do SST pread) to the [`BlockingExecutor`].
    pub async fn get_async(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_not_committed()?;
        if let Some(ref rs) = self.read_set {
            let mut guard = rs.lock();
            if !guard.contains(key) {
                guard.insert(Bytes::copy_from_slice(key));
            }
        }
        if let Some(entry) = self.local_storage.get(key) {
            let val = entry.value();
            if crate::vlog::KvKind::is_tombstone_value(val) {
                return Ok(None);
            }
            return Ok(Some(val.clone()));
        }
        let inner = self.inner.clone();
        let read_ts = self.read_ts;
        let key = Bytes::copy_from_slice(key);
        let blocking = self.blocking.clone();
        blocking
            .run_result(move || inner.get_with_ts(&key, read_ts))
            .await
    }

    /// Scan a range of keys.
    ///
    /// Merges local writes with the engine snapshot at the transaction's
    /// read timestamp, returning entries in sorted order.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.ensure_not_committed()?;
        // Serializable transactions cannot use scan() because range reads
        // would leave phantom keys untracked in the read_set. Reject until
        // range predicate tracking is implemented.
        anyhow::ensure!(
            self.read_set.is_none(),
            "scan() is not supported for serializable transactions until phantom/range tracking is implemented"
        );
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut local_iter = TxnLocalIterator::new(
            self.local_storage.clone(),
            |map| map.range::<Bytes, _>((map_bound(lower), map_bound(upper))),
            (Bytes::new(), Bytes::new()),
        );
        // Position at first entry (same as MemTableIterator::scan).
        local_iter.next()?;
        let merged = TwoMergeIterator::create(local_iter, lsm_iter)?;
        TxnIterator::create(Arc::clone(self), merged)
    }

    /// Return all visible keys whose user key starts with `prefix`, in sorted
    /// key order. An empty prefix is equivalent to a full scan.
    ///
    /// When prefix bloom filters are enabled, irrelevant SSTs are skipped
    /// before creating iterators.
    pub fn prefix_scan(self: &Arc<Self>, prefix: &[u8]) -> Result<TxnIterator> {
        if prefix.is_empty() {
            return self.scan(Bound::Unbounded, Bound::Unbounded);
        }
        self.ensure_not_committed()?;
        anyhow::ensure!(
            self.read_set.is_none(),
            "prefix_scan() is not supported for serializable transactions until phantom/range tracking is implemented"
        );
        let upper_bound = prefix_upper_bound(prefix);
        let lower = Bound::Included(prefix);
        let upper = match &upper_bound {
            Some(upper) => Bound::Excluded(upper.as_slice()),
            None => Bound::Unbounded,
        };
        let lsm_iter = self
            .inner
            .scan_with_prefix_hint(lower, upper, self.read_ts, prefix)?;
        let mut local_iter = TxnLocalIterator::new(
            self.local_storage.clone(),
            |map| map.range::<Bytes, _>((map_bound(lower), map_bound(upper))),
            (Bytes::new(), Bytes::new()),
        );
        local_iter.next()?;
        let merged = TwoMergeIterator::create(local_iter, lsm_iter)?;
        TxnIterator::create(Arc::clone(self), merged)
    }

    /// Buffer a write locally.
    ///
    /// The value is not visible to other transactions until
    /// [`commit`](Transaction::commit) is called.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_not_committed()?;
        anyhow::ensure!(
            !crate::vlog::KvKind::is_tombstone_value(value),
            "value must not be the tombstone marker byte (0x02)"
        );
        // Record in write_set for OCC conflict detection.
        if let Some(ref ws) = self.write_set {
            ws.lock().insert(Bytes::copy_from_slice(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    /// Buffer a deletion locally.
    ///
    /// The deletion is not visible to other transactions until
    /// [`commit`](Transaction::commit) is called.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.ensure_not_committed()?;
        // Record in write_set for OCC conflict detection.
        if let Some(ref ws) = self.write_set {
            ws.lock().insert(Bytes::copy_from_slice(key));
        }
        self.local_storage.insert(
            Bytes::copy_from_slice(key),
            Bytes::from_static(&[crate::vlog::KvKind::Tombstone as u8]),
        );
        Ok(())
    }

    /// Commit the transaction.
    ///
    /// All buffered writes are applied atomically under a single commit
    /// timestamp. Returns an error if the transaction was already committed.
    /// For serializable transactions, performs OCC conflict detection.
    pub fn commit(&self) -> Result<()> {
        if self
            .committed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_err()
        {
            anyhow::bail!("transaction already committed");
        }
        // Collect local writes as Bytes (cheap clone — refcount only).
        let entries: Vec<(Bytes, Bytes, bool)> = self
            .local_storage
            .iter()
            .map(|e| {
                let val = e.value();
                let is_tomb = crate::vlog::KvKind::is_tombstone_value(val);
                (e.key().clone(), val.clone(), is_tomb)
            })
            .collect();
        // Read-only transactions: skip conflict detection and write.
        if entries.is_empty() {
            // Release the read guard to unpin the watermark.
            self.read_guard.lock().take();
            return Ok(());
        }
        // For serializable transactions, perform OCC conflict detection.
        if let (Some(read_set), Some(write_set)) = (&self.read_set, &self.write_set) {
            let read_set_guard = read_set.lock();
            let mut write_set_guard = write_set.lock();
            if !write_set_guard.is_empty() {
                let mvcc = self
                    .inner
                    .mvcc
                    .as_ref()
                    .expect("serializable requires MVCC");
                // Acquire commit_lock to serialize conflict check + write.
                let _commit_guard = mvcc.commit_lock.lock();
                // Prune old committed_txns entries below watermark.
                let watermark = mvcc.watermark();
                let read_ts = self.read_ts;
                {
                    let mut committed = mvcc.committed_txns.lock();
                    if let Some(cutoff) = watermark.checked_add(1) {
                        *committed = committed.split_off(&cutoff);
                    } else {
                        committed.clear();
                    }
                    // Check for conflicts: any committed txn with commit_ts > read_ts
                    // whose write_set intersects our read_set.
                    // Use BTreeMap::range to skip entries <= read_ts (O(log N + K)).
                    for (commit_ts, txn_data) in committed.range((
                        std::ops::Bound::Excluded(read_ts),
                        std::ops::Bound::Unbounded,
                    )) {
                        if txn_data
                            .write_set
                            .intersection(&read_set_guard)
                            .next()
                            .is_some()
                        {
                            // Drop read_guard to unpin watermark before returning.
                            self.read_guard.lock().take();
                            anyhow::bail!(
                                "serializable conflict: key written by another transaction at ts={}",
                                commit_ts
                            );
                        }
                    }
                }
                // No conflict — write batch and record our write_set.
                let borrowed: Vec<(&[u8], &[u8], bool)> = entries
                    .iter()
                    .map(|(k, v, t)| (k.as_ref(), v.as_ref(), *t))
                    .collect();
                let commit_ts = self.inner.mvcc_write_batch_inner(&borrowed)?;
                // Record our write_set in committed_txns.
                mvcc.record_committed_txn(
                    commit_ts,
                    std::mem::take(&mut *write_set_guard),
                    read_ts,
                );
                // Release read_guard to unpin watermark.
                self.read_guard.lock().take();
                // Drop commit_lock before try_freeze to avoid deadlock with
                // non-txn serializable writes that hold active_memtable_lock.read().
                drop(_commit_guard);
                // Freeze memtable if it exceeds target size, matching other write paths.
                self.inner.try_freeze_memtable()?;
                return Ok(());
            }
        }
        // Non-serializable path (or read-only serializable).
        let borrowed: Vec<(&[u8], &[u8], bool)> = entries
            .iter()
            .map(|(k, v, t)| (k.as_ref(), v.as_ref(), *t))
            .collect();
        if let Err(e) = self.inner.mvcc_write_batch(&borrowed) {
            // Revert committed flag so caller can retry.
            self.committed
                .store(false, std::sync::atomic::Ordering::SeqCst);
            return Err(e);
        }
        // Release the read guard to unpin the watermark.
        self.read_guard.lock().take();

        Ok(())
    }

    /// Commit the transaction asynchronously.
    ///
    /// Runs OCC conflict detection inline (CPU-only), then offloads
    /// the blocking WAL write and memtable publish to the
    /// [`BlockingExecutor`].
    pub async fn commit_async(&self) -> Result<()> {
        if self
            .committed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_err()
        {
            anyhow::bail!("transaction already committed");
        }
        let entries: Vec<(Bytes, Bytes, bool)> = self
            .local_storage
            .iter()
            .map(|e| {
                let val = e.value();
                let is_tomb = crate::vlog::KvKind::is_tombstone_value(val);
                (e.key().clone(), val.clone(), is_tomb)
            })
            .collect();
        if entries.is_empty() {
            self.read_guard.lock().take();
            return Ok(());
        }
        // Snapshot all state from self so the future is Send.
        // Transaction is !Sync, so &Transaction is !Send and cannot be
        // held across .await. We clone Arcs and take Mutex guards now.
        let committed = Arc::clone(&self.committed);
        let is_serializable = self.read_set.is_some();
        let blocking = self.blocking.clone();
        let read_ts = self.read_ts;
        // Release read_guard early (before .await) so watermark is unpinned
        // promptly on commit success, matching sync commit behavior.
        let _read_guard = self.read_guard.lock().take();

        if is_serializable {
            let read_set = self.read_set.as_ref().unwrap();
            let write_set = self.write_set.as_ref().unwrap();
            let has_writes = { !write_set.lock().is_empty() };
            if has_writes {
                let mvcc_ref1 = self
                    .inner
                    .mvcc
                    .as_ref()
                    .expect("serializable requires MVCC")
                    .clone();
                let mvcc_ref2 = self
                    .inner
                    .mvcc
                    .as_ref()
                    .expect("serializable requires MVCC")
                    .clone();
                let inner1 = self.inner.clone();
                let inner2 = self.inner.clone();
                let read_set_snapshot: HashSet<Bytes> = read_set.lock().clone();
                let owned: Vec<(Vec<u8>, Vec<u8>, bool)> = entries
                    .iter()
                    .map(|(k, v, t)| (k.to_vec(), v.to_vec(), *t))
                    .collect();

                let result: Result<u64> = blocking
                    .run_result(move || {
                        let _commit_guard = mvcc_ref1.commit_lock.lock();
                        let watermark = mvcc_ref1.watermark();
                        {
                            let mut committed_txns = mvcc_ref1.committed_txns.lock();
                            if let Some(cutoff) = watermark.checked_add(1) {
                                *committed_txns = committed_txns.split_off(&cutoff);
                            } else {
                                committed_txns.clear();
                            }
                            for (commit_ts, txn_data) in committed_txns.range((
                                std::ops::Bound::Excluded(read_ts),
                                std::ops::Bound::Unbounded,
                            )) {
                                if txn_data
                                    .write_set
                                    .intersection(&read_set_snapshot)
                                    .next()
                                    .is_some()
                                {
                                    anyhow::bail!(
                                        "serializable conflict: key written by another transaction at ts={}",
                                        commit_ts
                                    );
                                }
                            }
                        }
                        let refs: Vec<(&[u8], &[u8], bool)> = owned
                            .iter()
                            .map(|(k, v, t)| (k.as_slice(), v.as_slice(), *t))
                            .collect();
                        let commit_ts = inner1.mvcc_write_batch_inner(&refs)?;
                        Ok(commit_ts)
                    })
                    .await;

                match result {
                    Ok(commit_ts) => {
                        mvcc_ref2.record_committed_txn(
                            commit_ts,
                            std::mem::take(&mut *write_set.lock()),
                            read_ts,
                        );
                    }
                    Err(e) => {
                        // Only revert committed for write errors, not OCC conflicts.
                        let msg = format!("{e}");
                        if !msg.contains("serializable conflict") {
                            committed.store(false, std::sync::atomic::Ordering::SeqCst);
                        }
                        return Err(e);
                    }
                }
                blocking
                    .run_result(move || inner2.try_freeze_memtable())
                    .await?;
                return Ok(());
            }
        }
        // Non-serializable path.
        let inner = self.inner.clone();
        let owned: Vec<(Vec<u8>, Vec<u8>, bool)> = entries
            .iter()
            .map(|(k, v, t)| (k.to_vec(), v.to_vec(), *t))
            .collect();
        if let Err(e) = blocking
            .run_result(move || {
                let refs: Vec<(&[u8], &[u8], bool)> = owned
                    .iter()
                    .map(|(k, v, t)| (k.as_slice(), v.as_slice(), *t))
                    .collect();
                inner.mvcc_write_batch(&refs)
            })
            .await
        {
            committed.store(false, std::sync::atomic::Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// Iterator over a transaction's local writes (the SkipMap).
///
/// Uses ouroboros to safely self-reference the skipmap iterator.
#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let n = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
        });
        self.with_mut(|m| *m.item = n);
        Ok(())
    }
}

/// Iterator that merges a transaction's local writes with the engine snapshot.
///
/// Local writes shadow engine entries with the same key. Tombstones
/// (both local and from the engine) are skipped automatically.
pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut s = Self { _txn: txn, iter };
        // Position at first valid entry, skipping tombstones.
        while s.iter.is_valid() && crate::vlog::KvKind::is_tombstone_value(s.iter.value()) {
            s.iter.next()?;
        }
        // Record the first key in read_set for OCC.
        if s.iter.is_valid()
            && let Some(ref rs) = s._txn.read_set
        {
            rs.lock().insert(Bytes::copy_from_slice(s.iter.key()));
        }
        Ok(s)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // Advance past current entry, then skip tombstones.
        self.iter.next()?;
        while self.iter.is_valid() && crate::vlog::KvKind::is_tombstone_value(self.iter.value()) {
            self.iter.next()?;
        }
        // Record the current key in read_set for OCC.
        if self.iter.is_valid()
            && let Some(ref rs) = self._txn.read_set
        {
            rs.lock().insert(Bytes::copy_from_slice(self.iter.key()));
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
