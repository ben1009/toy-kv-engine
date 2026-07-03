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
    /// versions. Wrapped in Arc<Mutex<Option<>>> so async commit paths can
    /// restore it on recoverable failures and drop it early on success.
    pub(crate) read_guard: Arc<Mutex<Option<ReadGuard>>>,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Read set for OCC conflict detection (None when not serializable).
    pub(crate) read_set: Option<Arc<Mutex<HashSet<Bytes>>>>,
    /// Write set for OCC conflict detection (None when not serializable).
    pub(crate) write_set: Option<Arc<Mutex<HashSet<Bytes>>>>,
    /// Keeps the transaction registered with shutdown tracking until commit or drop.
    pub(crate) lifecycle_guard: Arc<Mutex<Option<AdmissionGuard>>>,
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
    ///
    /// Returns `impl Future + Send` — all state extracted from `&self`
    /// before the async block (Transaction is `!Sync`, so `async fn(&self)`
    /// would produce a `!Send` future).
    pub fn get_async(
        &self,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send {
        let committed = Arc::clone(&self.committed);
        if !committed.load(std::sync::atomic::Ordering::SeqCst)
            && let Some(ref rs) = self.read_set
        {
            let mut guard = rs.lock();
            if !guard.contains(key) {
                guard.insert(Bytes::copy_from_slice(key));
            }
        }
        let local_storage = Arc::clone(&self.local_storage);
        let inner = self.inner.clone();
        let read_ts = self.read_ts;
        let key = Bytes::copy_from_slice(key);
        let blocking = self.blocking.clone();
        async move {
            anyhow::ensure!(
                !committed.load(std::sync::atomic::Ordering::SeqCst),
                "transaction already committed"
            );
            if let Some(entry) = local_storage.get(&key[..]) {
                let val = entry.value();
                if crate::vlog::KvKind::is_tombstone_value(val) {
                    return Ok(None);
                }
                return Ok(Some(val.clone()));
            }
            blocking
                .run_result(move || inner.get_with_ts(&key, read_ts))
                .await
        }
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
        TxnIterator::create(
            self.read_set.clone(),
            Arc::clone(&self.read_guard),
            Arc::clone(&self.lifecycle_guard),
            merged,
        )
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
        TxnIterator::create(
            self.read_set.clone(),
            Arc::clone(&self.read_guard),
            Arc::clone(&self.lifecycle_guard),
            merged,
        )
    }

    /// Scan a range of keys, asynchronously.
    ///
    /// Returns an owned async cursor over the transaction snapshot.
    pub fn scan_async(
        self: &Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> impl std::future::Future<Output = Result<AsyncTxnScan>> + Send {
        let read_guard = Arc::clone(&self.read_guard);
        let lifecycle_guard = Arc::clone(&self.lifecycle_guard);
        let read_set = self.read_set.clone();
        let local_storage = Arc::clone(&self.local_storage);
        let inner = Arc::clone(&self.inner);
        let committed = Arc::clone(&self.committed);
        let read_ts = self.read_ts;
        let lower_owned = lower.map(Bytes::copy_from_slice);
        let upper_owned = upper.map(Bytes::copy_from_slice);
        let blocking = self.blocking.clone();
        async move {
            anyhow::ensure!(
                !committed.load(std::sync::atomic::Ordering::SeqCst),
                "transaction already committed"
            );
            anyhow::ensure!(
                read_set.is_none(),
                "scan() is not supported for serializable transactions until phantom/range tracking is implemented"
            );
            use std::ops::Bound::*;
            let lower: Bound<&[u8]> = match &lower_owned {
                Included(b) => Included(b.as_ref()),
                Excluded(b) => Excluded(b.as_ref()),
                Unbounded => Unbounded,
            };
            let upper: Bound<&[u8]> = match &upper_owned {
                Included(b) => Included(b.as_ref()),
                Excluded(b) => Excluded(b.as_ref()),
                Unbounded => Unbounded,
            };
            let lsm_iter = inner.scan_with_ts(lower, upper, read_ts)?;
            let mut local_iter = TxnLocalIterator::new(
                local_storage,
                |map| map.range::<Bytes, _>((map_bound(lower), map_bound(upper))),
                (Bytes::new(), Bytes::new()),
            );
            local_iter.next()?;
            let merged = TwoMergeIterator::create(local_iter, lsm_iter)?;
            Ok(AsyncTxnScan {
                inner: TxnIterator::create(read_set, read_guard, lifecycle_guard, merged)?,
                blocking,
            })
        }
    }

    /// Prefix scan, asynchronously.
    ///
    /// Returns an owned async cursor over the transaction snapshot.
    pub fn prefix_scan_async(
        self: &Arc<Self>,
        prefix: &[u8],
    ) -> impl std::future::Future<Output = Result<AsyncTxnScan>> + Send {
        let read_guard = Arc::clone(&self.read_guard);
        let lifecycle_guard = Arc::clone(&self.lifecycle_guard);
        let read_set = self.read_set.clone();
        let local_storage = Arc::clone(&self.local_storage);
        let inner = Arc::clone(&self.inner);
        let committed = Arc::clone(&self.committed);
        let read_ts = self.read_ts;
        let prefix = Bytes::copy_from_slice(prefix);
        let blocking = self.blocking.clone();
        async move {
            anyhow::ensure!(
                !committed.load(std::sync::atomic::Ordering::SeqCst),
                "transaction already committed"
            );
            anyhow::ensure!(
                read_set.is_none(),
                "prefix_scan() is not supported for serializable transactions until phantom/range tracking is implemented"
            );
            let upper_bound = prefix_upper_bound(&prefix);
            let lower = Bound::Included(prefix.as_ref());
            let upper = match &upper_bound {
                Some(upper) => Bound::Excluded(upper.as_slice()),
                None => Bound::Unbounded,
            };
            let lsm_iter = inner.scan_with_prefix_hint(lower, upper, read_ts, &prefix)?;
            let mut local_iter = TxnLocalIterator::new(
                local_storage,
                |map| map.range::<Bytes, _>((map_bound(lower), map_bound(upper))),
                (Bytes::new(), Bytes::new()),
            );
            local_iter.next()?;
            let merged = TwoMergeIterator::create(local_iter, lsm_iter)?;
            Ok(AsyncTxnScan {
                inner: TxnIterator::create(read_set, read_guard, lifecycle_guard, merged)?,
                blocking,
            })
        }
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
    pub fn commit_async(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        enum SerializableCommitStep {
            Committed,
            TerminalErr(anyhow::Error),
            RestoreErr(anyhow::Error, Option<ReadGuard>),
        }

        enum WriteBatchStep {
            Committed,
            RestoreErr(anyhow::Error, Option<ReadGuard>),
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
        // Snapshot all state from self so the future is Send.
        // Transaction is !Sync, so &Transaction is !Send and cannot be
        // held across .await. We clone Arcs and take Mutex guards now.
        let committed = Arc::clone(&self.committed);
        let is_serializable = self.read_set.is_some();
        let blocking = self.blocking.clone();
        let read_ts = self.read_ts;
        let inner = self.inner.clone();
        let mvcc = inner.mvcc.clone();
        let read_guard_slot = Arc::clone(&self.read_guard);
        // Take read_guard before .await (Send requirement) but defer
        // dropping it until after OCC check + write complete, so the
        // watermark stays pinned across the critical section.
        let read_guard = read_guard_slot.lock().take();
        let read_set_snapshot = self
            .read_set
            .as_ref()
            .map(|read_set| read_set.lock().clone());
        let write_set_snapshot = self
            .write_set
            .as_ref()
            .map(|write_set| write_set.lock().clone());

        async move {
            if committed
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
            if entries.is_empty() {
                drop(read_guard);
                return Ok(());
            }

            if is_serializable {
                let has_writes = write_set_snapshot
                    .as_ref()
                    .is_some_and(|write_set| !write_set.is_empty());
                if has_writes {
                    let mut write_set_snapshot =
                        write_set_snapshot.expect("serializable writes require write_set");
                    let mvcc_ref = mvcc.as_ref().expect("serializable requires MVCC").clone();
                    let inner1 = inner.clone();
                    let inner2 = inner.clone();
                    let read_set_snapshot =
                        read_set_snapshot.expect("serializable writes require read_set");
                    let owned = entries.clone();

                    let result: Result<SerializableCommitStep> = blocking
                        .run_result(move || {
                            let mut read_guard = read_guard;
                            let _commit_guard = mvcc_ref.commit_lock.lock();
                            let watermark = mvcc_ref.watermark();
                            {
                                let mut committed_txns = mvcc_ref.committed_txns.lock();
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
                                        drop(read_guard.take());
                                        return Ok(SerializableCommitStep::TerminalErr(anyhow::anyhow!(
                                            "serializable conflict: key written by another transaction at ts={}",
                                            commit_ts
                                        )));
                                    }
                                }
                            }
                            let refs: Vec<(&[u8], &[u8], bool)> = owned
                                .iter()
                                .map(|(k, v, t)| (k.as_ref(), v.as_ref(), *t))
                                .collect();
                            match inner1.mvcc_write_batch_inner(&refs) {
                                Ok(commit_ts) => {
                                    mvcc_ref.record_committed_txn(
                                        commit_ts,
                                        std::mem::take(&mut write_set_snapshot),
                                        read_ts,
                                    );
                                    Ok(SerializableCommitStep::Committed)
                                }
                                Err(error) => Ok(SerializableCommitStep::RestoreErr(
                                    error,
                                    read_guard,
                                )),
                            }
                        })
                        .await;

                    match result {
                        Ok(SerializableCommitStep::Committed) => {}
                        Ok(SerializableCommitStep::TerminalErr(error)) => return Err(error),
                        Ok(SerializableCommitStep::RestoreErr(error, read_guard)) => {
                            if let Some(read_guard) = read_guard {
                                read_guard_slot.lock().replace(read_guard);
                            }
                            return Err(error);
                        }
                        Err(error) => return Err(error),
                    }
                    blocking
                        .run_result(move || inner2.try_freeze_memtable())
                        .await?;
                    return Ok(());
                }
            }
            // Non-serializable path.
            let owned = entries;
            let result: Result<WriteBatchStep> = blocking
                .run_result(move || {
                    let read_guard = read_guard;
                    let refs: Vec<(&[u8], &[u8], bool)> = owned
                        .iter()
                        .map(|(k, v, t)| (k.as_ref(), v.as_ref(), *t))
                        .collect();
                    match inner.mvcc_write_batch(&refs) {
                        Ok(()) => Ok(WriteBatchStep::Committed),
                        Err(error) => Ok(WriteBatchStep::RestoreErr(error, read_guard)),
                    }
                })
                .await;
            match result {
                Ok(WriteBatchStep::Committed) => {}
                Ok(WriteBatchStep::RestoreErr(error, read_guard)) => {
                    committed.store(false, std::sync::atomic::Ordering::SeqCst);
                    if let Some(read_guard) = read_guard {
                        read_guard_slot.lock().replace(read_guard);
                    }
                    return Err(error);
                }
                Err(error) => {
                    committed.store(false, std::sync::atomic::Ordering::SeqCst);
                    return Err(error);
                }
            }
            Ok(())
        }
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
    _read_guard: Arc<Mutex<Option<ReadGuard>>>,
    _lifecycle_guard: Arc<Mutex<Option<AdmissionGuard>>>,
    read_set: Option<Arc<Mutex<HashSet<Bytes>>>>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub(crate) fn create(
        read_set: Option<Arc<Mutex<HashSet<Bytes>>>>,
        read_guard: Arc<Mutex<Option<ReadGuard>>>,
        lifecycle_guard: Arc<Mutex<Option<AdmissionGuard>>>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut s = Self {
            _read_guard: read_guard,
            _lifecycle_guard: lifecycle_guard,
            read_set,
            iter,
        };
        // Position at first valid entry, skipping tombstones.
        while s.iter.is_valid() && crate::vlog::KvKind::is_tombstone_value(s.iter.value()) {
            s.iter.next()?;
        }
        // Record the first key in read_set for OCC.
        if s.iter.is_valid()
            && let Some(ref rs) = s.read_set
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
            && let Some(ref rs) = self.read_set
        {
            rs.lock().insert(Bytes::copy_from_slice(self.iter.key()));
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

/// Owned async cursor over a transaction snapshot.
pub struct AsyncTxnScan {
    inner: TxnIterator,
    #[allow(dead_code)]
    blocking: BlockingExecutor,
}

impl AsyncTxnScan {
    pub async fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        if !self.inner.is_valid() {
            return Ok(None);
        }
        let kv = (
            Bytes::copy_from_slice(self.inner.key()),
            Bytes::from(self.inner.value().to_vec()),
        );
        self.inner.next()?;
        Ok(Some(kv))
    }
}

impl StorageIterator for AsyncTxnScan {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.inner.key()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}
