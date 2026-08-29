use std::ops::Bound;
use std::sync::mpsc;
use std::time::Duration;
#[cfg(feature = "chaos-testing")]
use std::{sync::OnceLock, time::Instant};

use bytes::Bytes;
use tempfile::tempdir;

use super::harness::check_lsm_iter_result_by_key;
#[cfg(feature = "chaos-testing")]
use crate::chaos::failpoint::{self, FailScenario};
use crate::lsm_storage::{KvEngine, LsmStorageOptions, ParallelScanOptions};

#[cfg(feature = "chaos-testing")]
fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("condition was not met within {timeout:?}");
}

/// Creating a snapshot pins the MVCC watermark at the snapshot's read timestamp;
/// further commits cannot advance the pinned watermark until the snapshot drops.
#[test]
fn snapshot_creation_pins_watermark() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();

    let snap = engine.snapshot().unwrap();
    let pinned_ts = snap.read_ts();
    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    assert_eq!(mvcc.watermark(), pinned_ts);

    // Further commits advance current_ts but cannot advance the pinned watermark
    // while the snapshot is live.
    engine.put(b"k", b"v2").unwrap();
    engine.put(b"k", b"v3").unwrap();
    assert_eq!(mvcc.watermark(), pinned_ts);

    drop(snap);
    assert!(mvcc.watermark() > pinned_ts);
}

/// Snapshot stats count each shared view once and report its oldest pin.
#[test]
fn snapshot_stats_track_active_views_and_oldest_timestamp() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let first = engine.snapshot().unwrap();
    let first_ts = first.read_ts();
    let first_clone = first.clone();
    assert_eq!(engine.snapshot_stats().active_snapshots, 1);

    engine.put(b"k", b"v2").unwrap();
    let second = engine.snapshot().unwrap();
    let second_ts = second.read_ts();
    assert_eq!(engine.snapshot_stats().active_snapshots, 2);
    assert_eq!(
        engine.snapshot_stats().oldest_pinned_read_ts,
        Some(first_ts)
    );

    drop(first);
    assert_eq!(engine.snapshot_stats().active_snapshots, 2);
    drop(first_clone);
    assert_eq!(engine.snapshot_stats().active_snapshots, 1);
    assert_eq!(
        engine.snapshot_stats().oldest_pinned_read_ts,
        Some(second_ts)
    );
    drop(second);
    assert_eq!(
        engine.snapshot_stats(),
        crate::lsm_storage::SnapshotStats::default()
    );
}

/// A snapshot point read sees the value committed before the snapshot was taken.
#[test]
fn snapshot_get_sees_committed_value() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(Bytes::from("v1")));
}

/// Writes committed after the snapshot was taken are invisible to it.
#[test]
fn snapshot_isolation_from_post_snapshot_writes() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"k", b"v2").unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k").unwrap(), Some(Bytes::from("v2")));
}

/// A point delete committed after snapshot creation is invisible to that
/// snapshot, while current reads observe the deletion.
#[test]
fn snapshot_isolation_from_post_snapshot_delete() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.delete(b"k").unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k").unwrap(), None);
}

/// A range tombstone committed after snapshot creation is invisible to that
/// snapshot, while current reads observe it.
#[test]
fn snapshot_isolation_from_post_snapshot_range_tombstone() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.delete_range(b"a", b"c").unwrap();

    assert_eq!(snap.get(b"a").unwrap(), Some(Bytes::from("1")));
    assert_eq!(snap.get(b"b").unwrap(), Some(Bytes::from("2")));
    assert_eq!(engine.get(b"a").unwrap(), None);
    assert_eq!(engine.get(b"b").unwrap(), None);
}

/// A range tombstone visible at snapshot creation hides its covered keys.
#[test]
fn snapshot_observes_visible_range_tombstone() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    engine.delete_range(b"a", b"c").unwrap();
    let snap = engine.snapshot().unwrap();

    assert_eq!(snap.get(b"a").unwrap(), None);
    assert_eq!(snap.get(b"b").unwrap(), None);
}

/// A snapshot range scan returns the versions visible at the snapshot timestamp.
#[test]
fn snapshot_scan_returns_snapshot_version() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"v1").unwrap();
    engine.put(b"b", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"a", b"v2").unwrap();

    let mut iter = snap.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("v1")),
            (Bytes::from("b"), Bytes::from("v1")),
        ],
    );
}

/// A snapshot scan respects the supplied lower/upper bounds.
#[test]
fn snapshot_scan_respects_bounds() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    engine.put(b"c", b"3").unwrap();
    engine.put(b"d", b"4").unwrap();
    let snap = engine.snapshot().unwrap();

    let mut iter = snap
        .scan(Bound::Included(b"b"), Bound::Excluded(b"d"))
        .unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("b"), Bytes::from("2")),
            (Bytes::from("c"), Bytes::from("3")),
        ],
    );
}

/// A snapshot prefix scan returns only keys sharing the prefix.
#[test]
fn snapshot_prefix_scan_prunes() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"ca", b"1").unwrap();
    engine.put(b"cb", b"2").unwrap();
    engine.put(b"cd", b"3").unwrap();
    engine.put(b"ex", b"9").unwrap();
    let snap = engine.snapshot().unwrap();

    let mut iter = snap.prefix_scan(b"c").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("ca"), Bytes::from("1")),
            (Bytes::from("cb"), Bytes::from("2")),
            (Bytes::from("cd"), Bytes::from("3")),
        ],
    );
}

/// An empty snapshot prefix is a full scan at the captured timestamp.
#[test]
fn snapshot_empty_prefix_scan_is_full_scan() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    let snap = engine.snapshot().unwrap();

    let mut iter = snap.prefix_scan(b"").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ],
    );
}

/// TTL expiry follows the existing point-read contract: a snapshot point read
/// evaluates expiry at read time, so an already-expired key is hidden.
#[test]
fn snapshot_get_respects_ttl_expiry() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"persistent", b"v").unwrap();
    engine
        .put_with_ttl(b"ephemeral", b"v", Duration::from_secs(0))
        .unwrap();
    let snap = engine.snapshot().unwrap();

    assert_eq!(snap.get(b"ephemeral").unwrap(), None);
    assert_eq!(snap.get(b"persistent").unwrap(), Some(Bytes::from("v")));
}

/// Snapshot scans reuse the engine's fixed-at-creation TTL filtering.
#[test]
fn snapshot_scan_respects_ttl_expiry() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"persistent", b"v").unwrap();
    engine
        .put_with_ttl(b"expired", b"v", Duration::from_secs(0))
        .unwrap();
    let snap = engine.snapshot().unwrap();

    let mut iter = snap.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from("persistent"), Bytes::from("v"))],
    );
}

/// Snapshot handles and their owned async futures meet the public concurrency
/// contract from RFC 021.
#[test]
fn snapshot_async_api_is_send_and_static() {
    fn assert_send_static<T: Send + 'static>(_: T) {}

    static_assertions::assert_impl_all!(crate::mvcc::snapshot::Snapshot: Send, Sync);

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    let snap = engine.snapshot().unwrap();
    assert_send_static(snap.get_async(b"k"));
    assert_send_static(snap.scan_async(Bound::Unbounded, Bound::Unbounded));
    assert_send_static(snap.prefix_scan_async(b"prefix"));
}

/// `batch_get` resolves every key at the same snapshot timestamp.
#[test]
fn snapshot_batch_get_mixed() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k1", b"v1").unwrap();
    engine.put(b"k2", b"v2").unwrap();
    engine.delete(b"k2").unwrap();
    let snap = engine.snapshot().unwrap();

    let results = snap.batch_get(&[b"k1", b"k2", b"absent"]);
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].as_ref().unwrap().clone(),
        Some(Bytes::from("v1"))
    );
    assert!(results[1].as_ref().unwrap().is_none());
    assert!(results[2].as_ref().unwrap().is_none());
}

/// Snapshot batch reads use their shared timestamp for every key after later
/// writes and deletes become visible to the live engine.
#[test]
fn snapshot_batch_get_preserves_snapshot_visibility() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"a1").unwrap();
    engine.put(b"b", b"b1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"a", b"a2").unwrap();
    engine.delete(b"b").unwrap();

    let results = snap.batch_get(&[b"a", b"b", b"missing"]);
    assert_eq!(results[0].as_ref().unwrap(), &Some(Bytes::from("a1")));
    assert_eq!(results[1].as_ref().unwrap(), &Some(Bytes::from("b1")));
    assert_eq!(results[2].as_ref().unwrap(), &None);
}

/// A cloned snapshot shares one timestamp and keeps the watermark pinned until
/// the last clone is dropped.
#[test]
fn snapshot_clone_keeps_pin_alive() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let pinned_ts = snap.read_ts();
    let snap2 = snap.clone();
    drop(snap);
    engine.put(b"k", b"v2").unwrap();

    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    assert_eq!(mvcc.watermark(), pinned_ts);
    assert_eq!(snap2.get(b"k").unwrap(), Some(Bytes::from("v1")));

    drop(snap2);
    assert!(mvcc.watermark() > pinned_ts);
}

/// A scan cursor retains the watermark pin after the originating snapshot is
/// dropped (the cursor holds its own `Arc<SnapshotInner>`).
#[test]
fn snapshot_iterator_outlives_snapshot_drop() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let mut iter = snap.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    drop(snap);
    engine.put(b"k", b"v2").unwrap();

    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("k"), Bytes::from("v1"))]);
}

/// Compaction cannot remove a version visible to an active snapshot: the pinned
/// watermark retains the snapshot's version across a full compaction.
#[test]
fn snapshot_retains_versions_across_compaction() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let pinned_ts = snap.read_ts();
    assert_eq!(snap.get(b"k").unwrap(), Some(Bytes::from("v1")));
    engine.put(b"k", b"v2").unwrap();
    engine.put(b"k", b"v3").unwrap();

    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    assert_eq!(mvcc.watermark(), pinned_ts);

    // Flush to SSTs then compact; the watermark pin must prevent GC of v1.
    engine.drain_flush().unwrap();
    engine.force_full_compaction().unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(mvcc.watermark(), pinned_ts);

    drop(snap);
    assert!(mvcc.watermark() > pinned_ts);
}

/// An async point read observes the snapshot timestamp, ignoring later writes.
#[test]
fn snapshot_get_async_isolation() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"k", b"v2").unwrap();

    assert_eq!(
        crate::future_ext::block_on(snap.get_async(b"k")).unwrap(),
        Some(Bytes::from("v1"))
    );
}

/// An owned async point-read future retains the snapshot watermark after its
/// originating handle is dropped, until the future completes.
#[test]
fn snapshot_get_async_outlives_snapshot_drop() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let pinned_ts = snap.read_ts();
    let future = snap.get_async(b"k");
    drop(snap);
    engine.put(b"k", b"v2").unwrap();

    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    assert_eq!(mvcc.watermark(), pinned_ts);
    assert_eq!(
        crate::future_ext::block_on(future).unwrap(),
        Some(Bytes::from("v1"))
    );
    assert!(mvcc.watermark() > pinned_ts);
}

/// Cancelling an async snapshot read after its blocking closure starts leaves
/// the snapshot pin live until that closure completes.
#[cfg(feature = "chaos-testing")]
#[test]
fn cancelled_dispatched_snapshot_get_keeps_close_waiting() {
    static LOCK: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    let _lock = LOCK.get_or_init(|| parking_lot::Mutex::new(())).lock();
    let scenario = FailScenario::setup();
    failpoint::cfg("snapshot.get_async.after_dispatch", "pause").unwrap();

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let permits_before = engine.inner.blocking.available_permits();
    let future = snap.get_async(b"k");
    drop(snap);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let task = runtime.spawn(future);
    wait_until(Duration::from_secs(2), || {
        engine.inner.blocking.available_permits() < permits_before
    });
    task.abort();

    let (done_tx, done_rx) = mpsc::channel();
    let close_engine = engine.clone();
    let close = std::thread::spawn(move || {
        crate::future_ext::block_on(close_engine.close_async()).unwrap();
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "close_async must wait for the detached blocking snapshot read"
    );

    failpoint::cfg("snapshot.get_async.after_dispatch", "off").unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close_async completes after the blocking read");
    close.join().unwrap();
    runtime.shutdown_timeout(Duration::from_secs(2));
    scenario.teardown();
}

/// Cancelling an async snapshot scan after dispatch likewise retains the
/// snapshot pin until its blocking cursor construction completes.
#[cfg(feature = "chaos-testing")]
#[test]
fn cancelled_dispatched_snapshot_scan_keeps_close_waiting() {
    static LOCK: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    let _lock = LOCK.get_or_init(|| parking_lot::Mutex::new(())).lock();
    let scenario = FailScenario::setup();
    failpoint::cfg("snapshot.scan_async.after_dispatch", "pause").unwrap();

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let permits_before = engine.inner.blocking.available_permits();
    let future = snap.scan_async(Bound::Unbounded, Bound::Unbounded);
    drop(snap);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let task = runtime.spawn(future);
    wait_until(Duration::from_secs(2), || {
        engine.inner.blocking.available_permits() < permits_before
    });
    task.abort();

    let (done_tx, done_rx) = mpsc::channel();
    let close_engine = engine.clone();
    let close = std::thread::spawn(move || {
        crate::future_ext::block_on(close_engine.close_async()).unwrap();
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "close_async must wait for the detached blocking snapshot scan"
    );

    failpoint::cfg("snapshot.scan_async.after_dispatch", "off").unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close_async completes after the blocking scan");
    close.join().unwrap();
    runtime.shutdown_timeout(Duration::from_secs(2));
    scenario.teardown();
}

/// An async scan cursor yields the snapshot's versions and terminates at end.
#[test]
fn snapshot_scan_async_try_next() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"v1").unwrap();
    engine.put(b"b", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"a", b"v2").unwrap();

    let mut cursor =
        crate::future_ext::block_on(snap.scan_async(Bound::Unbounded, Bound::Unbounded)).unwrap();
    let mut got = Vec::new();
    while let Some((k, v)) = crate::future_ext::block_on(cursor.try_next()).unwrap() {
        got.push((k, v));
    }
    assert_eq!(
        got,
        vec![
            (Bytes::from("a"), Bytes::from("v1")),
            (Bytes::from("b"), Bytes::from("v1")),
        ]
    );
}

/// An owned async scan future retains its snapshot until the cursor is built.
#[test]
fn snapshot_scan_async_outlives_snapshot_drop() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let pinned_ts = snap.read_ts();
    let future = snap.scan_async(Bound::Unbounded, Bound::Unbounded);
    drop(snap);
    engine.put(b"k", b"v2").unwrap();

    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    assert_eq!(mvcc.watermark(), pinned_ts);
    let mut cursor = crate::future_ext::block_on(future).unwrap();
    assert_eq!(
        crate::future_ext::block_on(cursor.try_next()).unwrap(),
        Some((Bytes::from("k"), Bytes::from("v1")))
    );
    drop(cursor);
    assert!(mvcc.watermark() > pinned_ts);
}

/// An async prefix scan cursor yields only prefix-matching snapshot versions.
#[test]
fn snapshot_prefix_scan_async() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"ca", b"1").unwrap();
    engine.put(b"cb", b"2").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"ca", b"9").unwrap();

    let mut cursor = crate::future_ext::block_on(snap.prefix_scan_async(b"c")).unwrap();
    let mut got = Vec::new();
    while let Some((k, v)) = crate::future_ext::block_on(cursor.try_next()).unwrap() {
        got.push((k, v));
    }
    assert_eq!(
        got,
        vec![
            (Bytes::from("ca"), Bytes::from("1")),
            (Bytes::from("cb"), Bytes::from("2")),
        ]
    );
}

/// An empty async prefix is a full snapshot scan.
#[test]
fn snapshot_empty_prefix_scan_async() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    let snap = engine.snapshot().unwrap();

    let mut cursor = crate::future_ext::block_on(snap.prefix_scan_async(b"")).unwrap();
    let mut got = Vec::new();
    while let Some((k, v)) = crate::future_ext::block_on(cursor.try_next()).unwrap() {
        got.push((k, v));
    }
    assert_eq!(
        got,
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]
    );
}

/// A parallel snapshot scan retains the originating snapshot timestamp after
/// the handle drops.
#[test]
fn snapshot_parallel_scan_async_outlives_snapshot_drop() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let future = snap.scan_parallel_async(
        Bound::Unbounded,
        Bound::Unbounded,
        ParallelScanOptions::default(),
    );
    drop(snap);
    engine.put(b"k", b"v2").unwrap();

    let mut scan = crate::future_ext::block_on(future).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = crate::future_ext::block_on(scan.try_next_batch()).unwrap() {
        rows.extend(batch);
    }
    assert_eq!(rows, vec![(Bytes::from("k"), Bytes::from("v1"))]);
}

/// A parallel snapshot prefix scan filters keys at the snapshot timestamp.
#[test]
fn snapshot_prefix_parallel_scan_async_filters_prefix() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"ca", b"1").unwrap();
    engine.put(b"cb", b"2").unwrap();
    engine.put(b"other", b"3").unwrap();
    let snap = engine.snapshot().unwrap();

    let mut scan = crate::future_ext::block_on(
        snap.prefix_scan_parallel_async(b"c", ParallelScanOptions::default()),
    )
    .unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = crate::future_ext::block_on(scan.try_next_batch()).unwrap() {
        rows.extend(batch);
    }
    assert_eq!(
        rows,
        vec![
            (Bytes::from("ca"), Bytes::from("1")),
            (Bytes::from("cb"), Bytes::from("2")),
        ]
    );
}

/// Cancelling a parallel snapshot cursor after workers dispatch keeps the
/// worker-held snapshot pin live until those blocking reads stop.
#[cfg(feature = "chaos-testing")]
#[test]
fn cancelled_dispatched_parallel_snapshot_scan_keeps_close_waiting() {
    static LOCK: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    let _lock = LOCK.get_or_init(|| parking_lot::Mutex::new(())).lock();
    let scenario = FailScenario::setup();
    failpoint::cfg("snapshot.parallel_scan.after_dispatch", "pause").unwrap();

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    let permits_before = engine.inner.blocking.available_permits();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let scan = runtime
        .block_on(snap.scan_parallel_async(
            Bound::Unbounded,
            Bound::Unbounded,
            ParallelScanOptions::default(),
        ))
        .unwrap();
    drop(snap);
    wait_until(Duration::from_secs(2), || {
        engine.inner.blocking.available_permits() < permits_before
    });
    // Abort the coordinator task while its spawn_blocking worker remains
    // paused. Only the worker-local SnapshotInner clone can keep close pinned.
    runtime.shutdown_timeout(Duration::from_millis(0));
    drop(scan);

    let (done_tx, done_rx) = mpsc::channel();
    let close_engine = engine.clone();
    let close = std::thread::spawn(move || {
        crate::future_ext::block_on(close_engine.close_async()).unwrap();
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "close_async must wait for the detached parallel snapshot worker"
    );

    failpoint::cfg("snapshot.parallel_scan.after_dispatch", "off").unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close_async completes after the worker exits");
    close.join().unwrap();
    scenario.teardown();
}

/// A live snapshot blocks synchronous close, and snapshot admission is rejected
/// as soon as close begins.
#[test]
fn snapshot_blocks_sync_close_and_closing_rejects_new_snapshots() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    let snapshot = engine.snapshot().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let close_engine = engine.clone();
    let close = std::thread::spawn(move || {
        close_engine.close().unwrap();
        done_tx.send(()).unwrap();
    });

    let mut admission_rejected = false;
    for _ in 0..200 {
        match engine.snapshot() {
            Ok(snapshot) => drop(snapshot),
            Err(error) => {
                let message = error.to_string();
                assert!(message.contains("closing") || message.contains("closed"));
                admission_rejected = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        admission_rejected,
        "close did not begin within the test timeout"
    );
    assert!(
        done_rx.recv_timeout(Duration::from_millis(25)).is_err(),
        "close must wait for the live snapshot"
    );

    drop(snapshot);
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close completes after snapshot drop");
    close.join().unwrap();
}

/// A live snapshot also blocks asynchronous close until its final handle drops.
#[test]
fn snapshot_blocks_async_close() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    let snapshot = engine.snapshot().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let close_engine = engine.clone();
    let close = std::thread::spawn(move || {
        crate::future_ext::block_on(close_engine.close_async()).unwrap();
        done_tx.send(()).unwrap();
    });

    let mut admission_rejected = false;
    for _ in 0..200 {
        if engine.snapshot().is_err() {
            admission_rejected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        admission_rejected,
        "close_async did not begin within the test timeout"
    );
    assert!(
        done_rx.recv_timeout(Duration::from_millis(25)).is_err(),
        "close_async must wait for the live snapshot"
    );

    drop(snapshot);
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close_async completes after snapshot drop");
    close.join().unwrap();
}

/// Logical snapshots and physical checkpoints are independent consistency
/// boundaries: checkpoint creation must not alter the live snapshot view.
#[test]
fn snapshot_and_checkpoint_are_independent() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = KvEngine::open(&db_path, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    let snap = engine.snapshot().unwrap();
    engine.put(b"k", b"v2").unwrap();
    engine.create_checkpoint(&checkpoint_path).unwrap();

    assert_eq!(snap.get(b"k").unwrap(), Some(Bytes::from("v1")));
    let checkpoint =
        KvEngine::open(&checkpoint_path, LsmStorageOptions::default_for_test()).unwrap();
    assert_eq!(checkpoint.get(b"k").unwrap(), Some(Bytes::from("v2")));

    drop(snap);
    engine.close().unwrap();
    checkpoint.close().unwrap();
}

/// Snapshots are runtime-only: close cannot complete while one is live, and a
/// reopen creates a fresh handle rather than reviving the prior snapshot.
#[test]
fn snapshot_does_not_survive_close_and_reopen() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    engine.put(b"k", b"v1").unwrap();

    let snapshot = engine.snapshot().unwrap();
    let prior_inner = std::sync::Arc::downgrade(&snapshot.inner);
    drop(snapshot);
    engine.close().unwrap();
    assert!(prior_inner.upgrade().is_none());
    drop(engine);

    let reopened = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    let fresh_snapshot = reopened.snapshot().unwrap();
    assert_eq!(fresh_snapshot.get(b"k").unwrap(), Some(Bytes::from("v1")));
    drop(fresh_snapshot);
    reopened.close().unwrap();
}
