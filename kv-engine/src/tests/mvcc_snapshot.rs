use std::ops::Bound;
use std::time::Duration;

use bytes::Bytes;
use tempfile::tempdir;

use super::harness::check_lsm_iter_result_by_key;
use crate::lsm_storage::{KvEngine, LsmStorageOptions};

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
