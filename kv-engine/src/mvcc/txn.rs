use std::{collections::HashSet, ops::Bound, sync::Arc, sync::atomic::AtomicBool};

use anyhow::{Context, Result};
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
    /// versions. Commit releases the transaction's reference on success;
    /// accepted reads and cursors retain independent references to the pin.
    pub(crate) read_guard: Arc<Mutex<Option<Arc<ReadGuard>>>>,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Linearizes local operations with the owned commit's claim. Released
    /// before engine I/O or awaits, so a claim cannot overtake an accepted mutation.
    pub(crate) operation_lock: Arc<Mutex<()>>,
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
        let _read_pin = {
            let _operation = self.operation_lock.lock();
            self.ensure_not_committed()?;
            // Record this key even if a local write shadows the engine read.
            if let Some(ref rs) = self.read_set {
                let mut guard = rs.lock();
                if !guard.contains(key) {
                    guard.insert(Bytes::copy_from_slice(key));
                }
            }
            self.read_guard
                .lock()
                .clone()
                .context("transaction snapshot is no longer available")?
        };
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
    /// engine read (may do SST pread) to the bounded blocking executor.
    ///
    /// Returns `impl Future + Send` — all state extracted from `&self`
    /// before the async block (Transaction is `!Sync`, so `async fn(&self)`
    /// would produce a `!Send` future).
    pub fn get_async(
        &self,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send + use<> {
        let committed = Arc::clone(&self.committed);
        {
            let _operation = self.operation_lock.lock();
            if !committed.load(std::sync::atomic::Ordering::SeqCst)
                && let Some(ref rs) = self.read_set
            {
                let mut guard = rs.lock();
                if !guard.contains(key) {
                    guard.insert(Bytes::copy_from_slice(key));
                }
            }
        }
        let local_storage = Arc::clone(&self.local_storage);
        let inner = self.inner.clone();
        let read_ts = self.read_ts;
        let key = Bytes::copy_from_slice(key);
        let blocking = self.blocking.clone();
        let read_guard = Arc::clone(&self.read_guard);
        let lifecycle_guard = Arc::clone(&self.lifecycle_guard);
        let operation_lock = Arc::clone(&self.operation_lock);
        let read_set = self.read_set.clone();

        async move {
            let read_guard = {
                let _operation = operation_lock.lock();
                anyhow::ensure!(
                    !committed.load(std::sync::atomic::Ordering::SeqCst),
                    "transaction already committed"
                );
                // Construction may have overlapped a claimed attempt that
                // then failed recoverably. Register every accepted read even
                // when the eager construction-time registration was skipped.
                if let Some(read_set) = read_set {
                    let mut read_set = read_set.lock();
                    if !read_set.contains(key.as_ref()) {
                        read_set.insert(key.clone());
                    }
                }
                read_guard
                    .lock()
                    .clone()
                    .context("transaction snapshot is no longer available")?
            };
            if let Some(entry) = local_storage.get(&key[..]) {
                let val = entry.value();
                if crate::vlog::KvKind::is_tombstone_value(val) {
                    return Ok(None);
                }
                return Ok(Some(val.clone()));
            }
            blocking
                .run_result(move || {
                    let _read_guard = read_guard;
                    let _lifecycle_guard = lifecycle_guard;

                    inner.get_with_ts(&key, read_ts)
                })
                .await
        }
    }

    /// Scan a range of keys.
    ///
    /// Merges local writes with the engine snapshot at the transaction's
    /// read timestamp, returning entries in sorted order.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let read_guard = {
            let _operation = self.operation_lock.lock();
            self.ensure_not_committed()?;
            self.read_guard
                .lock()
                .clone()
                .context("transaction snapshot is no longer available")?
        };
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
            read_guard,
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
        let read_guard = {
            let _operation = self.operation_lock.lock();
            self.ensure_not_committed()?;
            self.read_guard
                .lock()
                .clone()
                .context("transaction snapshot is no longer available")?
        };
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
            read_guard,
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
    ) -> impl std::future::Future<Output = Result<AsyncTxnScan>> + Send + use<> {
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
        let operation_lock = Arc::clone(&self.operation_lock);

        async move {
            let read_guard = {
                let _operation = operation_lock.lock();
                anyhow::ensure!(
                    !committed.load(std::sync::atomic::Ordering::SeqCst),
                    "transaction already committed"
                );
                read_guard
                    .lock()
                    .clone()
                    .context("transaction snapshot is no longer available")?
            };
            anyhow::ensure!(
                read_set.is_none(),
                "scan() is not supported for serializable transactions until phantom/range tracking is implemented"
            );
            let cursor_blocking = blocking.clone();
            blocking
                .run_result(move || {
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
                        inner: Arc::new(Mutex::new(TxnIterator::create(
                            read_set,
                            read_guard,
                            lifecycle_guard,
                            merged,
                        )?)),
                        blocking: cursor_blocking,
                    })
                })
                .await
        }
    }

    /// Prefix scan, asynchronously.
    ///
    /// Returns an owned async cursor over the transaction snapshot.
    pub fn prefix_scan_async(
        self: &Arc<Self>,
        prefix: &[u8],
    ) -> impl std::future::Future<Output = Result<AsyncTxnScan>> + Send + use<> {
        let read_guard = Arc::clone(&self.read_guard);
        let lifecycle_guard = Arc::clone(&self.lifecycle_guard);
        let read_set = self.read_set.clone();
        let local_storage = Arc::clone(&self.local_storage);
        let inner = Arc::clone(&self.inner);
        let committed = Arc::clone(&self.committed);
        let read_ts = self.read_ts;
        let prefix = Bytes::copy_from_slice(prefix);
        let upper_bound = prefix_upper_bound(&prefix);
        let blocking = self.blocking.clone();
        let operation_lock = Arc::clone(&self.operation_lock);

        async move {
            let read_guard = {
                let _operation = operation_lock.lock();
                anyhow::ensure!(
                    !committed.load(std::sync::atomic::Ordering::SeqCst),
                    "transaction already committed"
                );
                read_guard
                    .lock()
                    .clone()
                    .context("transaction snapshot is no longer available")?
            };
            anyhow::ensure!(
                read_set.is_none(),
                "prefix_scan() is not supported for serializable transactions until phantom/range tracking is implemented"
            );
            let cursor_blocking = blocking.clone();
            blocking
                .run_result(move || {
                    let (lsm_iter, lower, upper) = if prefix.is_empty() {
                        (
                            inner.scan_with_ts(Bound::Unbounded, Bound::Unbounded, read_ts)?,
                            Bound::Unbounded,
                            Bound::Unbounded,
                        )
                    } else {
                        let lower = Bound::Included(prefix.as_ref());
                        let upper = match &upper_bound {
                            Some(upper) => Bound::Excluded(upper.as_slice()),
                            None => Bound::Unbounded,
                        };
                        (
                            inner.scan_with_prefix_hint(lower, upper, read_ts, &prefix)?,
                            lower,
                            upper,
                        )
                    };
                    let mut local_iter = TxnLocalIterator::new(
                        local_storage,
                        |map| map.range::<Bytes, _>((map_bound(lower), map_bound(upper))),
                        (Bytes::new(), Bytes::new()),
                    );
                    local_iter.next()?;
                    let merged = TwoMergeIterator::create(local_iter, lsm_iter)?;

                    Ok(AsyncTxnScan {
                        inner: Arc::new(Mutex::new(TxnIterator::create(
                            read_set,
                            read_guard,
                            lifecycle_guard,
                            merged,
                        )?)),
                        blocking: cursor_blocking,
                    })
                })
                .await
        }
    }

    /// Buffer a write locally.
    ///
    /// The value is not visible to other transactions until
    /// [`commit`](Transaction::commit) is called.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let _operation = self.operation_lock.lock();
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
        let _operation = self.operation_lock.lock();
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
        {
            // A mutation that passed its committed check must finish both its
            // local write and OCC metadata before this owner can claim inputs.
            let _operation = self.operation_lock.lock();
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
        }
        // Collect local writes as Bytes (cheap clone — refcount only).
        let entries: Vec<(Bytes, Bytes, crate::mvcc::BatchEntryKind)> = self
            .local_storage
            .iter()
            .map(|e| {
                let val = e.value();
                let kind = if crate::vlog::KvKind::is_tombstone_value(val) {
                    crate::mvcc::BatchEntryKind::Delete
                } else {
                    crate::mvcc::BatchEntryKind::PutRaw
                };
                (e.key().clone(), val.clone(), kind)
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
                let read_ts = self.read_ts;
                // No conflict — write batch and record our write_set.
                let owned: Vec<(bytes::Bytes, bytes::Bytes, crate::mvcc::BatchEntryKind)> = entries
                    .iter()
                    .map(|(k, v, t)| (k.clone(), v.clone(), *t))
                    .collect();
                let mut retries = 0;
                loop {
                    let _commit_guard = mvcc.commit_lock.lock();
                    // Re-run OCC after each WAL rotation while the transaction's
                    // snapshot guard remains pinned.
                    let watermark = mvcc.watermark();
                    {
                        let mut committed = mvcc.committed_txns.lock();
                        if let Some(cutoff) = watermark.checked_add(1) {
                            *committed = committed.split_off(&cutoff);
                        } else {
                            committed.clear();
                        }
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
                                self.read_guard.lock().take();
                                anyhow::bail!(
                                    "serializable conflict: key written by another transaction at ts={}",
                                    commit_ts
                                );
                            }
                        }
                    }

                    let expected_memtable = self.inner.state.load().memtable.clone();
                    match self.inner.mvcc_write_batch_inner(&owned) {
                        Ok(commit_ts) => {
                            mvcc.record_committed_txn(
                                commit_ts,
                                std::mem::take(&mut *write_set_guard),
                                read_ts,
                            );
                            self.read_guard.lock().take();
                            drop(_commit_guard);
                            self.inner.try_freeze_memtable()?;
                            return Ok(());
                        }
                        Err(error) if crate::wal::Wal::is_retryable_full_error(&error) => {
                            drop(_commit_guard);
                            #[cfg(all(test, feature = "chaos-testing"))]
                            crate::chaos::failpoint::before_parallel_wal_transaction_rotation();
                            if let Err(rotation_error) = self.inner.retry_after_wal_full(
                                error,
                                &expected_memtable,
                                &mut retries,
                            ) {
                                self.committed
                                    .store(false, std::sync::atomic::Ordering::SeqCst);
                                return Err(rotation_error);
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        // Non-serializable path (or read-only serializable).
        let owned: Vec<(bytes::Bytes, bytes::Bytes, crate::mvcc::BatchEntryKind)> = entries
            .iter()
            .map(|(k, v, t)| (k.clone(), v.clone(), *t))
            .collect();
        if let Err(e) = self.inner.mvcc_write_batch(&owned) {
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
    /// Runs the same OCC, WAL rotation and publication protocol as
    /// [`commit`](Self::commit) on the bounded blocking executor.
    /// Once dispatched, the owned commit retains its snapshot and shutdown
    /// registration until it settles, even if the caller cancels the wait.
    pub fn commit_async(&self) -> impl std::future::Future<Output = Result<()>> + Send + use<> {
        // Retain the state without consuming the snapshot or copying writes.
        // The blocking owner claims the commit first, then reads its inputs.
        let owned = Self {
            read_ts: self.read_ts,
            read_guard: Arc::clone(&self.read_guard),
            inner: Arc::clone(&self.inner),
            local_storage: Arc::clone(&self.local_storage),
            committed: Arc::clone(&self.committed),
            operation_lock: Arc::clone(&self.operation_lock),
            read_set: self.read_set.clone(),
            write_set: self.write_set.clone(),
            lifecycle_guard: Arc::clone(&self.lifecycle_guard),
            blocking: self.blocking.clone(),
            _not_sync: std::marker::PhantomData,
        };
        let blocking = self.blocking.clone();

        async move { blocking.run_result(move || owned.commit()).await }
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
    _read_guard: Arc<ReadGuard>,
    _lifecycle_guard: Arc<Mutex<Option<AdmissionGuard>>>,
    read_set: Option<Arc<Mutex<HashSet<Bytes>>>>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub(crate) fn create(
        read_set: Option<Arc<Mutex<HashSet<Bytes>>>>,
        read_guard: Arc<ReadGuard>,
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
    inner: Arc<Mutex<TxnIterator>>,
    blocking: BlockingExecutor,
}

impl AsyncTxnScan {
    pub fn try_next(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<(Bytes, Bytes)>>> + Send {
        let inner = Arc::clone(&self.inner);
        let blocking = self.blocking.clone();

        async move {
            blocking
                .run_result(move || {
                    let mut inner = inner.lock();
                    if !inner.is_valid() {
                        return Ok(None);
                    }
                    let kv = (
                        Bytes::copy_from_slice(inner.key()),
                        Bytes::from(inner.value().to_vec()),
                    );
                    inner.next()?;

                    Ok(Some(kv))
                })
                .await
        }
    }
}
