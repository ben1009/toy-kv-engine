//! Public read-only snapshot API (RFC 021).
//!
//! A [`Snapshot`] pins the MVCC watermark at a captured commit timestamp and
//! serves reads from that timestamp, reusing the engine's timestamped read
//! paths. Drop the [`Snapshot`] (and any live [`SnapshotScanIterator`]) to
//! release the watermark pin.

use std::{ops::Bound, sync::Arc};

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{AdmissionGuard, LsmStorageInner, prefix_upper_bound},
    mvcc::ReadGuard,
};

/// A read-only point-in-time snapshot of the engine.
///
/// All reads through a `Snapshot` observe a consistent view of the data at the
/// commit timestamp captured when [`KvEngine::snapshot`](crate::lsm_storage::KvEngine::snapshot)
/// was called. The snapshot pins the MVCC watermark: compaction will not
/// garbage-collect versions at or above the snapshot's `read_ts` while the
/// `Snapshot` (or any derived [`SnapshotScanIterator`]) is alive.
///
/// `Snapshot` is cheap to [`clone`](Clone) (an `Arc` refcount). Drop the last
/// clone — and any live derived iterator — to release the watermark pin.
#[derive(Clone)]
pub struct Snapshot {
    pub(crate) inner: Arc<SnapshotInner>,
}

pub(crate) struct SnapshotInner {
    pub(crate) storage: Arc<LsmStorageInner>,
    pub(crate) read_ts: u64,
    /// Pins the MVCC watermark at `read_ts`. Held for the lifetime of every
    /// clone of the `Snapshot` and every derived iterator/cursor (which hold an
    /// `Arc<SnapshotInner>`), so dropping the `Snapshot` first does NOT release
    /// the watermark until all derived readers are also dropped.
    pub(crate) _read_guard: ReadGuard,
    /// Registers the snapshot with engine lifecycle/shutdown tracking so
    /// `close()` quiescence waits for live snapshots. Same lifetime as
    /// `_read_guard` via the shared `Arc<SnapshotInner>`.
    pub(crate) _lifecycle_guard: AdmissionGuard,
}

impl Snapshot {
    /// The commit timestamp this snapshot reads at.
    pub fn read_ts(&self) -> u64 {
        self.inner.read_ts
    }

    /// Point read at the snapshot timestamp.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.storage.get_with_ts(key, self.inner.read_ts)
    }

    /// Batch point read at the snapshot timestamp. Each key is resolved
    /// independently via the same timestamped point-read path; a batched
    /// timestamped lookup may be added later without changing semantics.
    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>> {
        let read_ts = self.inner.read_ts;
        keys.iter()
            .map(|k| self.inner.storage.get_with_ts(k, read_ts))
            .collect()
    }

    /// Range scan at the snapshot timestamp.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<SnapshotScanIterator> {
        let iter = self
            .inner
            .storage
            .scan_with_ts(lower, upper, self.inner.read_ts)?;
        Ok(SnapshotScanIterator {
            _snap: self.inner.clone(),
            iter,
        })
    }

    /// Return all visible keys whose user key starts with `prefix`, in sorted
    /// order, at the snapshot timestamp. An empty prefix is a full scan. Uses
    /// prefix bloom filter pruning when enabled.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<SnapshotScanIterator> {
        if prefix.is_empty() {
            return self.scan(Bound::Unbounded, Bound::Unbounded);
        }
        let upper_bound = prefix_upper_bound(prefix);
        let lower = Bound::Included(prefix);
        let upper = match &upper_bound {
            Some(upper) => Bound::Excluded(upper.as_slice()),
            None => Bound::Unbounded,
        };
        let iter =
            self.inner
                .storage
                .scan_with_prefix_hint(lower, upper, self.inner.read_ts, prefix)?;
        Ok(SnapshotScanIterator {
            _snap: self.inner.clone(),
            iter,
        })
    }
}

/// Owned sync scan cursor over a [`Snapshot`].
///
/// Holds an `Arc<SnapshotInner>` (not a plain `ReadGuard`) so the watermark
/// pin outlives the originating `Snapshot`: dropping the `Snapshot` first does
/// NOT release the watermark until this iterator is also dropped. Mirrors
/// [`ScanIterator`](crate::lsm_iterator::ScanIterator) but swaps the
/// `Option<ReadGuard>` for the shared `Arc<SnapshotInner>`.
pub struct SnapshotScanIterator {
    _snap: Arc<SnapshotInner>,
    iter: FusedIterator<LsmIterator>,
}

impl StorageIterator for SnapshotScanIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn raw_value(&self) -> &[u8] {
        self.iter.raw_value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
