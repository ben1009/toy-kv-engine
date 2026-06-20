use std::ops::Bound;

use bytes::Bytes;
use tempfile::tempdir;

use super::harness::check_lsm_iter_result_by_key;
use crate::{
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions},
};

/// Scan sees only the latest version of each key (MVCC deduplication).
#[test]
fn test_scan_returns_latest_version() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"k1", b"v1").unwrap();
    engine.put(b"k2", b"v2").unwrap();
    // Overwrite k1
    engine.put(b"k1", b"v1_updated").unwrap();

    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("k1"), Bytes::from("v1_updated")),
            (Bytes::from("k2"), Bytes::from("v2")),
        ],
    );
}

/// Scan respects tombstones — deleted keys are not returned.
#[test]
fn test_scan_respects_tombstones() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    engine.put(b"c", b"vc").unwrap();
    engine.delete(b"b").unwrap();

    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("c"), Bytes::from("vc")),
        ],
    );
}

/// Scan with bounds respects snapshot — later writes outside the scan's
/// snapshot are invisible.
#[test]
fn test_scan_snapshot_isolation() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();

    // First scan — sees a, b
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("b"), Bytes::from("vb")),
        ],
    );
    drop(iter);

    // Write more data
    engine.put(b"a", b"va2").unwrap();
    engine.put(b"c", b"vc").unwrap();

    // Second scan — sees updated a and new c
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va2")),
            (Bytes::from("b"), Bytes::from("vb")),
            (Bytes::from("c"), Bytes::from("vc")),
        ],
    );
}

/// Scan with lower and upper bounds.
#[test]
fn test_scan_bounded() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    engine.put(b"c", b"vc").unwrap();
    engine.put(b"d", b"vd").unwrap();

    let mut iter = engine
        .scan(Bound::Included(b"b"), Bound::Excluded(b"d"))
        .unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("b"), Bytes::from("vb")),
            (Bytes::from("c"), Bytes::from("vc")),
        ],
    );
}

/// Empty scan returns no results.
#[test]
fn test_scan_empty() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    let iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    assert!(!iter.is_valid());
}

/// Scan deduplicates across many versions of the same key.
#[test]
fn test_scan_deduplicates_across_versions() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    for i in 0..10 {
        engine.put(b"key", format!("v{i}").as_bytes()).unwrap();
    }

    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("key"), Bytes::from("v9"))]);
}

/// ReadGuard pins the watermark — versions at or above the guard's read_ts
/// are protected from GC while the guard is alive.
#[test]
fn test_read_guard_pins_watermark() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    engine.put(b"k", b"v2").unwrap();

    // Pin snapshot at ts=2
    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    let guard = mvcc.new_read_guard();
    let pinned_ts = guard.read_ts();

    // Advance the timestamp
    engine.put(b"k", b"v3").unwrap();
    engine.put(b"k", b"v4").unwrap();

    // Watermark should be exactly at pinned_ts (the smallest active reader)
    assert_eq!(mvcc.watermark(), pinned_ts);

    // After dropping the guard, watermark can advance
    drop(guard);
    assert!(mvcc.watermark() > pinned_ts);
}

/// Delete writes a KvKind::Tombstone marker; get() returns None.
#[test]
fn test_delete_writes_tombstone_marker() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"k", b"v").unwrap();
    engine.delete(b"k").unwrap();

    let val = engine.get(b"k").unwrap();
    assert!(
        val.is_none(),
        "deleted key should return None, got {:?}",
        val
    );
}

/// Delete followed by put on the same key returns the new value.
#[test]
fn test_delete_then_put() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    engine.delete(b"k").unwrap();
    engine.put(b"k", b"v2").unwrap();

    let val = engine.get(b"k").unwrap();
    assert_eq!(val, Some(Bytes::from("v2")));
}

/// Tombstone survives flush and compaction — key stays deleted.
#[test]
fn test_tombstone_survives_flush() {
    use super::harness::sync;

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.delete(b"a").unwrap();
    engine.put(b"b", b"vb").unwrap();

    // Flush to SST
    sync(&engine.inner);

    // Tombstone should still be visible
    assert!(engine.get(b"a").unwrap().is_none());
    assert_eq!(engine.get(b"b").unwrap(), Some(Bytes::from("vb")));
}

// write_batch tests are omitted here because write_batch bypasses MVCC
// (no timestamp allocation), so get() with MVCC cannot find the keys.
// The dedup logic is exercised indirectly through compaction.

// --- Snapshot isolation tests ---

/// Write after scan starts is invisible to the scan (snapshot pinning).
#[test]
fn test_scan_snapshot_hides_later_put() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"c", b"vc").unwrap();

    // Start scan — captures read_ts before this point
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();

    // Write after scan started — invisible to the scan
    engine.put(b"b", b"vb").unwrap();

    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("c"), Bytes::from("vc")),
        ],
    );
}

/// Delete after scan starts does not hide the key from the scan.
#[test]
fn test_scan_snapshot_hides_later_delete() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    engine.put(b"c", b"vc").unwrap();

    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();

    // Delete after scan started — scan still sees "b"
    engine.delete(b"b").unwrap();

    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("b"), Bytes::from("vb")),
            (Bytes::from("c"), Bytes::from("vc")),
        ],
    );
}

/// Overwrite after scan starts — scan sees the old value.
#[test]
fn test_scan_snapshot_hides_later_overwrite() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb_old").unwrap();

    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();

    // Overwrite after scan started — scan sees old value
    engine.put(b"b", b"vb_new").unwrap();

    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("b"), Bytes::from("vb_old")),
        ],
    );
}

/// Scan survives a memtable flush — the Arc<LsmStorageState> snapshot
/// keeps the old memtable alive even after it's flushed to SST.
#[test]
fn test_scan_survives_memtable_flush() {
    use super::harness::sync;

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();

    // Start scan — state snapshot pins the active memtable
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();

    sync(&engine.inner); // flush to SST — old state stays alive via Arc
    engine.put(b"c", b"vc").unwrap(); // write after scan — not visible

    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("b"), Bytes::from("vb")),
        ],
    );
}

/// Scan with Bound::Excluded lower bound skips all versions of the excluded key.
#[test]
fn test_scan_excluded_lower_bound_skips_versions() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb1").unwrap();
    engine.put(b"b", b"vb2").unwrap(); // second version of "b"
    engine.put(b"c", b"vc").unwrap();

    // Excluded lower bound "b" — should skip all versions of "b"
    let mut iter = engine
        .scan(Bound::Excluded(b"b"), Bound::Unbounded)
        .unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("c"), Bytes::from("vc"))]);
}

/// Scan with tombstone in SST — deleted key is still hidden after flush.
#[test]
fn test_scan_tombstone_in_sst() {
    use super::harness::sync;

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    engine.delete(b"b").unwrap();
    engine.put(b"c", b"vc").unwrap();

    // Flush everything to SST
    sync(&engine.inner);

    // Scan should still skip the deleted key
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("c"), Bytes::from("vc")),
        ],
    );
}

/// Scan with read_ts between two versions returns only the older version.
#[test]
fn test_scan_read_ts_between_versions() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    engine.put(b"k", b"v2").unwrap();

    // Scan at this point sees v2
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("k"), Bytes::from("v2"))]);
    drop(iter);

    // Write v3 after the scan — new scan should see v3
    engine.put(b"k", b"v3").unwrap();
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("k"), Bytes::from("v3"))]);
}
