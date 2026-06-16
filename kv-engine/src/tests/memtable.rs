use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    mem_table::MemTable,
};

#[test]
fn test_task1_memtable_get() {
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
    assert_eq!(
        &memtable.for_testing_get_slice(b"key1").unwrap()[..],
        b"value1"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key2").unwrap()[..],
        b"value2"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key3").unwrap()[..],
        b"value3"
    );
}

#[test]
fn test_task1_memtable_overwrite() {
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").unwrap();
    memtable.for_testing_put_slice(b"key1", b"value11").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value22").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value33").unwrap();
    assert_eq!(
        &memtable.for_testing_get_slice(b"key1").unwrap()[..],
        b"value11"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key2").unwrap()[..],
        b"value22"
    );
    assert_eq!(
        &memtable.for_testing_get_slice(b"key3").unwrap()[..],
        b"value33"
    );
}

#[test]
fn test_task2_storage_integration() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
    storage.delete(b"0").unwrap(); // should NOT report any error
}

#[test]
fn test_task3_storage_integration() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    assert_eq!(storage.state.load().imm_memtables.len(), 1);
    let previous_approximate_size = storage.state.load().imm_memtables[0].approximate_size();
    assert!(previous_approximate_size >= 15);
    storage.put(b"1", b"2333").unwrap();
    storage.put(b"2", b"23333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    assert_eq!(storage.state.load().imm_memtables.len(), 2);
    assert!(
        storage.state.load().imm_memtables[1].approximate_size() == previous_approximate_size,
        "wrong order of memtables?"
    );
    assert!(storage.state.load().imm_memtables[0].approximate_size() > previous_approximate_size);
}

#[test]
fn test_task3_freeze_on_capacity() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.target_sst_size = 1024;
    options.num_memtable_limit = 1000;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());
    for _ in 0..1000 {
        storage.put(b"1", b"2333").unwrap();
    }
    let num_imm_memtables = storage.state.load().imm_memtables.len();
    assert!(num_imm_memtables >= 1, "no memtable frozen?");
    for _ in 0..1000 {
        storage.delete(b"1").unwrap();
    }
    assert!(
        storage.state.load().imm_memtables.len() > num_imm_memtables,
        "no more memtable frozen?"
    );
}

#[test]
fn test_task4_storage_integration() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.delete(b"1").unwrap();
    storage.delete(b"2").unwrap();
    storage.put(b"3", b"2333").unwrap();
    storage.put(b"4", b"23333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"1", b"233333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    assert_eq!(storage.state.load().imm_memtables.len(), 2);
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"2").unwrap(), &None);
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"4").unwrap().unwrap()[..], b"23333");
}

#[test]
fn test_memtable_range_tombstone_is_not_empty() {
    let mt = crate::mem_table::MemTable::create(0);
    assert!(mt.is_empty());
    mt.put_range_tombstone(b"a", b"z", 10, 0).unwrap();
    assert!(!mt.is_empty());
}

#[test]
fn test_memtable_range_tombstone_approximate_size() {
    let mt = crate::mem_table::MemTable::create(0);
    let before = mt.approximate_size();
    mt.put_range_tombstone(b"a", b"z", 10, 0).unwrap();
    let after = mt.approximate_size();
    assert!(
        after > before,
        "approximate_size should increase after adding range tombstone"
    );
}

#[test]
fn test_memtable_range_tombstone_lookup() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone(b"tenant:42:", b"tenant:43:", 20, 0)
        .unwrap();

    // Key inside range is covered.
    assert_eq!(
        mt.range_tombstones()
            .newest_covering_ts(b"tenant:42:key1", 100),
        Some(20)
    );
    // Key at start is covered.
    assert_eq!(
        mt.range_tombstones().newest_covering_ts(b"tenant:42:", 100),
        Some(20)
    );
    // Key at end is NOT covered.
    assert_eq!(
        mt.range_tombstones().newest_covering_ts(b"tenant:43:", 100),
        None
    );
    // Reader before tombstone doesn't see it.
    assert_eq!(
        mt.range_tombstones()
            .newest_covering_ts(b"tenant:42:key1", 19),
        None
    );
}

#[test]
fn test_memtable_put_range_tombstone_batch() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone_batch(&[(b"a", b"m"), (b"x", b"z")], 42, 0)
        .unwrap();

    assert_eq!(mt.range_tombstones().len(), 2);
    assert_eq!(
        mt.range_tombstones().newest_covering_ts(b"b", 100),
        Some(42)
    );
    assert_eq!(
        mt.range_tombstones().newest_covering_ts(b"y", 100),
        Some(42)
    );
    // Between the two ranges — not covered.
    assert_eq!(mt.range_tombstones().newest_covering_ts(b"n", 100), None);
}

#[test]
fn test_memtable_put_range_tombstone_batch_empty() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone_batch(&[], 10, 0).unwrap();
    assert!(mt.range_tombstones().is_empty());
}

#[test]
fn test_memtable_range_overlap_containment() {
    // Query range [a, z] fully contains memtable keys [m, p].
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"m", b"1").unwrap();
    storage.put(b"p", b"2").unwrap();

    let state = storage.state.load();
    // Query [a, z] contains [m, p] — should overlap.
    assert!(state.memtable.range_overlap(
        std::ops::Bound::Included(b"a" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
    // Query [a, b] is entirely before [m, p] — no overlap.
    assert!(!state.memtable.range_overlap(
        std::ops::Bound::Included(b"a" as &[u8]),
        std::ops::Bound::Included(b"b" as &[u8]),
    ));
}

#[test]
fn test_memtable_range_overlap_with_range_tombstones() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone(b"f", b"p", 10, 0).unwrap();

    // Overlapping query.
    assert!(mt.range_overlap(
        std::ops::Bound::Included(b"a" as &[u8]),
        std::ops::Bound::Included(b"g" as &[u8]),
    ));
    // Non-overlapping query.
    assert!(!mt.range_overlap(
        std::ops::Bound::Included(b"q" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));

    // Excluded lower bound with tombstone ending at successor of excluded key.
    // Tombstone [f, p\0) covers keys < p\0. Query (p, z] covers keys > p.
    // No key can be both > p and < p\0, so no overlap.
    let mut p_succ = b"p".to_vec();
    p_succ.push(0);
    let mt2 = crate::mem_table::MemTable::create(0);
    mt2.put_range_tombstone(b"f", &p_succ, 10, 0).unwrap();
    assert!(!mt2.range_overlap(
        std::ops::Bound::Excluded(b"p" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
}

#[test]
fn test_delete_range_vlog_guard() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.value_separation = Some(crate::vlog::ValueSeparationOptions {
        enabled: true,
        ..Default::default()
    });
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());

    // delete_range_internal should be rejected when vlog is enabled.
    let result = storage.delete_range_internal(b"a", b"z");
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("vlog"),
        "error should mention vlog"
    );
}

#[test]
fn test_write_batch_range_only_vlog_guard() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.value_separation = Some(crate::vlog::ValueSeparationOptions {
        enabled: true,
        ..Default::default()
    });
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());

    // Range-only batch should be rejected when vlog is enabled.
    let result = storage.write_batch(&[crate::lsm_storage::WriteBatchRecord::DelRange(
        b"a".as_ref(),
        b"z".as_ref(),
    )]);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("vlog"),
        "error should mention vlog"
    );
}

#[test]
fn test_memtable_range_overlap_unbounded_lower() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone(b"f", b"p", 10, 0).unwrap();

    // Unbounded lower + Included upper that covers the tombstone.
    assert!(mt.range_overlap(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
    // Unbounded lower + Excluded upper that covers the tombstone.
    assert!(mt.range_overlap(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Excluded(b"z" as &[u8]),
    ));
    // Unbounded lower + Included upper that is before the tombstone.
    assert!(!mt.range_overlap(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Included(b"e" as &[u8]),
    ));
    // Unbounded lower + Excluded upper that is before the tombstone.
    assert!(!mt.range_overlap(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Excluded(b"f" as &[u8]),
    ));
    // Both unbounded.
    assert!(mt.range_overlap(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded,));
}

#[test]
fn test_memtable_range_overlap_excluded_lower() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone(b"f", b"p", 10, 0).unwrap();

    // Excluded lower inside the tombstone range.
    assert!(mt.range_overlap(
        std::ops::Bound::Excluded(b"g" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
    // Excluded lower at the tombstone start.
    assert!(mt.range_overlap(
        std::ops::Bound::Excluded(b"f" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
    // Excluded lower at the tombstone end.
    assert!(!mt.range_overlap(
        std::ops::Bound::Excluded(b"p" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
}

#[test]
fn test_memtable_range_overlap_unbounded_upper() {
    let mt = crate::mem_table::MemTable::create(0);
    mt.put_range_tombstone(b"f", b"p", 10, 0).unwrap();

    // Included lower before tombstone + Unbounded upper.
    assert!(mt.range_overlap(
        std::ops::Bound::Included(b"a" as &[u8]),
        std::ops::Bound::Unbounded,
    ));
    // Excluded lower before tombstone + Unbounded upper.
    assert!(mt.range_overlap(
        std::ops::Bound::Excluded(b"a" as &[u8]),
        std::ops::Bound::Unbounded,
    ));
    // Included lower after tombstone + Unbounded upper.
    assert!(!mt.range_overlap(
        std::ops::Bound::Included(b"q" as &[u8]),
        std::ops::Bound::Unbounded,
    ));
    // Excluded lower after tombstone + Unbounded upper.
    assert!(!mt.range_overlap(
        std::ops::Bound::Excluded(b"p" as &[u8]),
        std::ops::Bound::Unbounded,
    ));
}

#[test]
fn test_memtable_range_overlap_empty() {
    let mt = crate::mem_table::MemTable::create(0);
    // Empty memtable: no point entries, no range tombstones.
    assert!(!mt.range_overlap(
        std::ops::Bound::Included(b"a" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
    assert!(!mt.range_overlap(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded,));
}

#[test]
fn test_newest_covering_ts_skip_optimization() {
    let set = crate::range_tombstone::RangeTombstoneSet::new();
    // Add tombstones with different ts at the same start.
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"a"),
            end: bytes::Bytes::from_static(b"z"),
            ts: 10,
        },
        0,
    );
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"a"),
            end: bytes::Bytes::from_static(b"z"),
            ts: 20,
        },
        1,
    );
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"a"),
            end: bytes::Bytes::from_static(b"z"),
            ts: 15,
        },
        2,
    );

    // At read_ts=100, should return the newest (ts=20).
    assert_eq!(set.newest_covering_ts(b"m", 100), Some(20));
    // At read_ts=15, should skip ts=20 and find ts=15.
    assert_eq!(set.newest_covering_ts(b"m", 15), Some(15));
    // At read_ts=9, no tombstones are visible.
    assert_eq!(set.newest_covering_ts(b"m", 9), None);
}

#[test]
fn test_overlaps_with_invalid_range() {
    let set = crate::range_tombstone::RangeTombstoneSet::new();
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"f"),
            end: bytes::Bytes::from_static(b"p"),
            ts: 10,
        },
        0,
    );
    // Empty range: start >= end should return false.
    assert!(!set.overlaps(b"z", b"a", 100));
    assert!(!set.overlaps(b"a", b"a", 100));
}

#[test]
fn test_iter_overlapping_with_invalid_range() {
    let set = crate::range_tombstone::RangeTombstoneSet::new();
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"f"),
            end: bytes::Bytes::from_static(b"p"),
            ts: 10,
        },
        0,
    );
    // Empty range: start >= end should return no results.
    assert_eq!(set.iter_overlapping(b"z", b"a").count(), 0);
    assert_eq!(set.iter_overlapping(b"a", b"a").count(), 0);
}

#[test]
fn test_write_range_batch_through_storage() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // Write a range-only batch.
    storage
        .write_batch(&[
            crate::lsm_storage::WriteBatchRecord::DelRange(b"a".as_ref(), b"m".as_ref()),
            crate::lsm_storage::WriteBatchRecord::DelRange(b"x".as_ref(), b"z".as_ref()),
        ])
        .unwrap();

    // Verify range tombstones are in the active memtable.
    let state = storage.state.load();
    assert_eq!(state.memtable.range_tombstones().len(), 2);
}

#[test]
fn test_delete_range_through_storage() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.delete_range_internal(b"a", b"z").unwrap();

    let state = storage.state.load();
    assert_eq!(state.memtable.range_tombstones().len(), 1);
}

#[test]
fn test_delete_range_serializable_guard() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.serializable = true;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());

    let result = storage.delete_range_internal(b"a", b"z");
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("serializable"),
        "error should mention serializable"
    );
}

#[test]
fn test_write_batch_range_only_serializable_guard() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.serializable = true;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());

    let result = storage.write_batch(&[crate::lsm_storage::WriteBatchRecord::DelRange(
        b"a".as_ref(),
        b"z".as_ref(),
    )]);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("serializable"),
        "error should mention serializable"
    );
}

#[test]
fn test_write_batch_mixed_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    let result = storage.write_batch(&[
        crate::lsm_storage::WriteBatchRecord::Put(b"k".as_ref(), b"v".as_ref()),
        crate::lsm_storage::WriteBatchRecord::DelRange(b"a".as_ref(), b"z".as_ref()),
    ]);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("mixed"),
        "error should mention mixed"
    );
}

#[test]
fn test_delete_range_invalid_range() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // start >= end should be rejected.
    let result = storage.delete_range_internal(b"z", b"a");
    assert!(result.is_err());

    let result = storage.delete_range_internal(b"a", b"a");
    assert!(result.is_err());
}
