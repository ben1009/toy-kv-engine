//! Public read-only snapshot API (RFC 021).
//!
//! A [`Snapshot`] pins the MVCC watermark at a captured commit timestamp and
//! serves reads from that timestamp, reusing the engine's timestamped read
//! paths. Drop the [`Snapshot`] (and any live [`SnapshotScanIterator`]) to
//! release the watermark pin.

use std::{ops::Bound, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::{
    blocking_executor::BlockingExecutor,
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{
        AdmissionGuard, LsmStorageInner, ParallelScan, ParallelScanOptions, SnapshotTrackerGuard,
        prefix_upper_bound,
    },
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
    pub(crate) _stats_guard: SnapshotTrackerGuard,
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

    /// Batch point read at the snapshot timestamp. The batch reuses one
    /// storage-state snapshot and the shared timestamped lookup pipeline.
    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>> {
        self.inner
            .storage
            .batch_get_with_ts(keys, self.inner.read_ts)
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

    /// Async point read at the snapshot timestamp.
    ///
    /// Returns a `Send` future; all state is cloned from the `Arc<SnapshotInner>`
    /// before the future is constructed, so the future can outlive the
    /// originating `Snapshot`. Dispatched through the engine's blocking
    /// executor; cancelling the future after dispatch does not halt the blocking
    /// closure (it releases its pin only on completion).
    pub fn get_async(
        &self,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send + 'static {
        let snap = Arc::clone(&self.inner);
        let key = Bytes::copy_from_slice(key);
        let blocking = snap.storage.blocking.clone();
        async move {
            blocking
                .run_result(move || {
                    #[cfg(feature = "chaos-testing")]
                    crate::chaos::failpoint::fail_point!("snapshot.get_async.after_dispatch");
                    snap.storage.get_with_ts(&key, snap.read_ts)
                })
                .await
        }
    }

    /// Async range scan at the snapshot timestamp.
    pub fn scan_async(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> impl std::future::Future<Output = Result<AsyncSnapshotScan>> + Send + 'static {
        let snap = Arc::clone(&self.inner);
        let lower_owned = lower.map(Bytes::copy_from_slice);
        let upper_owned = upper.map(Bytes::copy_from_slice);
        let blocking = self.inner.storage.blocking.clone();
        async move {
            let cursor_blocking = blocking.clone();
            blocking
                .run_result(move || {
                    #[cfg(feature = "chaos-testing")]
                    crate::chaos::failpoint::fail_point!("snapshot.scan_async.after_dispatch");
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
                    let storage = Arc::clone(&snap.storage);
                    let read_ts = snap.read_ts;
                    let iter = storage.scan_with_ts(lower, upper, read_ts)?;
                    Ok(AsyncSnapshotScan {
                        inner: Arc::new(Mutex::new(SnapshotScanIterator { _snap: snap, iter })),
                        blocking: cursor_blocking,
                    })
                })
                .await
        }
    }

    /// Async prefix scan at the snapshot timestamp.
    pub fn prefix_scan_async(
        &self,
        prefix: &[u8],
    ) -> impl std::future::Future<Output = Result<AsyncSnapshotScan>> + Send + 'static {
        let snap = Arc::clone(&self.inner);
        let prefix_owned = Bytes::copy_from_slice(prefix);
        let upper_owned = prefix_upper_bound(prefix).map(Bytes::from);
        let blocking = self.inner.storage.blocking.clone();
        async move {
            let cursor_blocking = blocking.clone();
            blocking
                .run_result(move || {
                    let storage = Arc::clone(&snap.storage);
                    let read_ts = snap.read_ts;
                    let iter = if prefix_owned.is_empty() {
                        storage.scan_with_ts(Bound::Unbounded, Bound::Unbounded, read_ts)?
                    } else {
                        let lower = Bound::Included(prefix_owned.as_ref());
                        let upper = match &upper_owned {
                            Some(u) => Bound::Excluded(u.as_ref()),
                            None => Bound::Unbounded,
                        };
                        storage.scan_with_prefix_hint(
                            lower,
                            upper,
                            read_ts,
                            prefix_owned.as_ref(),
                        )?
                    };
                    Ok(AsyncSnapshotScan {
                        inner: Arc::new(Mutex::new(SnapshotScanIterator { _snap: snap, iter })),
                        blocking: cursor_blocking,
                    })
                })
                .await
        }
    }

    /// Ordered worker-backed async range scan at the snapshot timestamp.
    ///
    /// The returned cursor retains this snapshot's watermark and lifecycle pin
    /// until all worker shards complete.
    pub fn scan_parallel_async(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        options: ParallelScanOptions,
    ) -> impl std::future::Future<Output = Result<ParallelScan>> + Send + 'static {
        let snap = Arc::clone(&self.inner);
        let lower_owned = lower.map(Bytes::copy_from_slice);
        let upper_owned = upper.map(Bytes::copy_from_slice);
        async move {
            use std::ops::Bound::*;
            let lower = match &lower_owned {
                Included(b) => Included(b.as_ref()),
                Excluded(b) => Excluded(b.as_ref()),
                Unbounded => Unbounded,
            };
            let upper = match &upper_owned {
                Included(b) => Included(b.as_ref()),
                Excluded(b) => Excluded(b.as_ref()),
                Unbounded => Unbounded,
            };
            let storage = Arc::clone(&snap.storage);
            storage
                .scan_parallel_async_internal(lower, upper, None, Some(snap), options)
                .await
        }
    }

    /// Ordered worker-backed async prefix scan at the snapshot timestamp.
    pub fn prefix_scan_parallel_async(
        &self,
        prefix: &[u8],
        options: ParallelScanOptions,
    ) -> impl std::future::Future<Output = Result<ParallelScan>> + Send + 'static {
        let snap = Arc::clone(&self.inner);
        let prefix_owned = Bytes::copy_from_slice(prefix);
        let upper_owned = prefix_upper_bound(prefix).map(Bytes::from);
        async move {
            let storage = Arc::clone(&snap.storage);
            if prefix_owned.is_empty() {
                return storage
                    .scan_parallel_async_internal(
                        Bound::Unbounded,
                        Bound::Unbounded,
                        None,
                        Some(snap),
                        options,
                    )
                    .await;
            }
            let lower = Bound::Included(prefix_owned.as_ref());
            let upper = match &upper_owned {
                Some(upper) => Bound::Excluded(upper.as_ref()),
                None => Bound::Unbounded,
            };
            storage
                .scan_parallel_async_internal(
                    lower,
                    upper,
                    Some(prefix_owned.as_ref()),
                    Some(snap),
                    options,
                )
                .await
        }
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

/// Owned async scan cursor over a [`Snapshot`]. Mirrors
/// [`AsyncTxnScan`](crate::mvcc::txn::AsyncTxnScan) and
/// [`AsyncScan`](crate::lsm_storage::AsyncScan): wraps a
/// [`SnapshotScanIterator`] behind an `Arc<Mutex<...>>` so [`try_next`](Self::try_next)
/// can dispatch each step on the engine's blocking executor without holding a
/// mutex across `.await`. The cursor owns the `Arc<SnapshotInner>` pin.
pub struct AsyncSnapshotScan {
    inner: Arc<Mutex<SnapshotScanIterator>>,
    blocking: BlockingExecutor,
}

impl AsyncSnapshotScan {
    /// Fetch the next `(key, value)` pair, or `None` at end.
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
