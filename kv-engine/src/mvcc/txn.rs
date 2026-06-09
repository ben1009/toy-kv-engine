use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
    mvcc::ReadGuard,
};

/// An MVCC transaction that provides snapshot isolation.
///
/// Reads see a consistent snapshot at the transaction's `read_ts`.
/// Writes are buffered locally and only become visible to other
/// transactions on [`commit`](Transaction::commit).
pub struct Transaction {
    /// Holds the read timestamp and registers it in the MVCC watermark for
    /// the transaction's lifetime, preventing GC of versions we might read.
    pub(crate) read_guard: ReadGuard,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

fn is_tombstone(val: &[u8]) -> bool {
    val.len() == 1 && val[0] == crate::vlog::KvKind::Tombstone as u8
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
        // Check local writes first — they shadow the engine.
        if let Some(entry) = self.local_storage.get(key) {
            let val = entry.value();
            // Tombstone in local storage means deleted within this txn.
            if is_tombstone(val) {
                return Ok(None);
            }
            return Ok(Some(val.clone()));
        }
        // Fall back to engine read at our snapshot timestamp.
        self.inner.get_with_ts(key, self.read_guard.read_ts())
    }

    /// Scan a range of keys.
    ///
    /// Merges local writes with the engine snapshot at the transaction's
    /// read timestamp, returning entries in sorted order.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.ensure_not_committed()?;
        let lsm_iter = self
            .inner
            .scan_with_ts(lower, upper, self.read_guard.read_ts())?;
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

    /// Buffer a write locally.
    ///
    /// The value is not visible to other transactions until
    /// [`commit`](Transaction::commit) is called.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_not_committed()?;
        anyhow::ensure!(
            !is_tombstone(value),
            "value must not be the tombstone marker byte (0x02)"
        );
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
                let is_tomb = is_tombstone(val);
                (e.key().clone(), val.clone(), is_tomb)
            })
            .collect();
        if entries.is_empty() {
            return Ok(());
        }
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
        Ok(())
    }
}

fn map_bound(b: Bound<&[u8]>) -> Bound<Bytes> {
    match b {
        Bound::Included(v) => Bound::Included(Bytes::copy_from_slice(v)),
        Bound::Excluded(v) => Bound::Excluded(Bytes::copy_from_slice(v)),
        Bound::Unbounded => Bound::Unbounded,
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
        while s.iter.is_valid() && is_tombstone(s.iter.value()) {
            s.iter.next()?;
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
        while self.iter.is_valid() && is_tombstone(self.iter.value()) {
            self.iter.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
