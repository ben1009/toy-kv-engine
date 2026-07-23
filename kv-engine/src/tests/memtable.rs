use std::ops::Bound;
use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    iterators::StorageIterator,
    key::KeySlice,
    lsm_storage::{KvEngine, LsmStorageInner, LsmStorageOptions},
    mem_table::MemTable,
};

#[test]
fn test_task1_memtable_get() {
    let memtable = MemTable::create(0, false);
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
    let memtable = MemTable::create(0, false);
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
    let options = LsmStorageOptions {
        target_sst_size: 1024,
        num_memtable_limit: 1000,
        ..LsmStorageOptions::default_for_test()
    };
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
    let mt = crate::mem_table::MemTable::create(0, false);
    assert!(mt.is_empty());
    mt.put_range_tombstone(b"a", b"z", 10, 0).unwrap();
    assert!(!mt.is_empty());
}

#[test]
fn test_memtable_range_tombstone_approximate_size() {
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt = crate::mem_table::MemTable::create(0, false);
    mt.put_range_tombstone_batch(&[], 10, 0).unwrap();
    assert!(mt.range_tombstones().is_empty());
}

#[test]
fn test_memtable_commit_wal_without_writes_is_noop() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty_memtable.wal");
    let mt = crate::mem_table::MemTable::create_with_wal(0, false, &path).unwrap();

    mt.commit_wal().unwrap();
}

#[test]
fn test_memtable_commit_wal_ticket_publishes_after_specific_ticket() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ticket_memtable.wal");
    let mt = crate::mem_table::MemTable::create_with_wal(0, false, &path).unwrap();
    let key = crate::key::encode_internal_key(b"k", 1);
    let value = [crate::vlog::KvKind::Inline as u8, b'v'];

    let ticket = mt
        .write_wal_batch_only(&[(KeySlice::from_slice(&key), value.as_slice())])
        .unwrap();
    assert!(ticket.is_some());
    mt.commit_wal_ticket(ticket).unwrap();
    mt.publish_raw_batch(&[(KeySlice::from_slice(&key), value.as_slice())])
        .unwrap();

    assert_eq!(
        mt.get_raw_exact(&key),
        Some(bytes::Bytes::copy_from_slice(&value))
    );
}

#[test]
fn test_memtable_publish_raw_batch_owned() {
    let mt = crate::mem_table::MemTable::create(0, false);
    let key1 = crate::key::encode_internal_key(b"k1", 10);
    let key2 = crate::key::encode_internal_key(b"k2", 10);
    let value1 = bytes::Bytes::from_static(&[crate::vlog::KvKind::Inline as u8, b'v', b'1']);
    let value2 = bytes::Bytes::from_static(&[crate::vlog::KvKind::Inline as u8, b'v', b'2']);
    let expected_size = key1.len() + key2.len() + value1.len() + value2.len();

    mt.publish_raw_batch_owned(vec![
        (bytes::Bytes::from(key1.clone()), value1.clone()),
        (bytes::Bytes::from(key2.clone()), value2.clone()),
    ])
    .unwrap();

    assert_eq!(mt.get_raw_exact(&key1), Some(value1));
    assert_eq!(mt.get_raw_exact(&key2), Some(value2));
    assert_eq!(mt.approximate_size(), expected_size);
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt2 = crate::mem_table::MemTable::create(0, false);
    mt2.put_range_tombstone(b"f", &p_succ, 10, 0).unwrap();
    assert!(!mt2.range_overlap(
        std::ops::Bound::Excluded(b"p" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
}

#[test]
fn test_delete_range_vlog_guard() {
    let dir = tempfile::tempdir().unwrap();
    let options = LsmStorageOptions {
        value_separation: Some(crate::vlog::ValueSeparationOptions {
            enabled: true,
            ..Default::default()
        }),
        ..LsmStorageOptions::default_for_test()
    };
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
    let options = LsmStorageOptions {
        value_separation: Some(crate::vlog::ValueSeparationOptions {
            enabled: true,
            ..Default::default()
        }),
        ..LsmStorageOptions::default_for_test()
    };
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let mt = crate::mem_table::MemTable::create(0, false);
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
    let options = LsmStorageOptions {
        serializable: true,
        ..LsmStorageOptions::default_for_test()
    };
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
    let options = LsmStorageOptions {
        serializable: true,
        ..LsmStorageOptions::default_for_test()
    };
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

#[test]
fn test_write_batch_point_entries() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // Point-only batch should work.
    storage
        .write_batch(&[
            crate::lsm_storage::WriteBatchRecord::Put(b"k1".as_ref(), b"v1".as_ref()),
            crate::lsm_storage::WriteBatchRecord::Put(b"k2".as_ref(), b"v2".as_ref()),
            crate::lsm_storage::WriteBatchRecord::Del(b"k1".as_ref()),
        ])
        .unwrap();

    assert!(storage.get(b"k1").unwrap().is_none());
    assert_eq!(&storage.get(b"k2").unwrap().unwrap()[..], b"v2");
}

#[test]
fn test_write_batch_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // Duplicate keys should be deduped (last wins).
    storage
        .write_batch(&[
            crate::lsm_storage::WriteBatchRecord::Put(b"k1".as_ref(), b"first".as_ref()),
            crate::lsm_storage::WriteBatchRecord::Put(b"k1".as_ref(), b"second".as_ref()),
        ])
        .unwrap();

    assert_eq!(&storage.get(b"k1").unwrap().unwrap()[..], b"second");
}

#[test]
fn test_write_batch_large_mvcc_publish() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let batch: Vec<_> = (0..520)
        .map(|idx| {
            crate::lsm_storage::WriteBatchRecord::Put(
                format!("k{idx:04}").into_bytes(),
                format!("v{idx:04}").into_bytes(),
            )
        })
        .collect();

    storage.write_batch(&batch).unwrap();

    for idx in [0, 1, 255, 511, 519] {
        let key = format!("k{idx:04}");
        let value = format!("v{idx:04}");
        assert_eq!(
            storage.get(key.as_bytes()).unwrap().as_deref(),
            Some(value.as_bytes())
        );
    }
}

#[test]
fn test_storage_put_get_delete() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"key1", b"val1").unwrap();
    storage.put(b"key2", b"val2").unwrap();
    assert_eq!(&storage.get(b"key1").unwrap().unwrap()[..], b"val1");
    assert_eq!(&storage.get(b"key2").unwrap().unwrap()[..], b"val2");

    storage.delete(b"key1").unwrap();
    assert!(storage.get(b"key1").unwrap().is_none());
    assert_eq!(&storage.get(b"key2").unwrap().unwrap()[..], b"val2");
}

#[test]
fn test_storage_scan() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"2").unwrap();
    storage.put(b"c", b"3").unwrap();

    // scan returns a ScanIterator (not std Iterator), just verify it doesn't panic.
    let _iter = storage
        .scan(
            std::ops::Bound::Included(b"a".as_ref()),
            std::ops::Bound::Excluded(b"c".as_ref()),
        )
        .unwrap();
}

#[test]
fn test_storage_force_freeze_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"key1", b"val1").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    assert_eq!(storage.state.load().imm_memtables.len(), 1);
}

#[test]
fn test_range_tombstone_overlaps_read_ts() {
    let set = crate::range_tombstone::RangeTombstoneSet::new();
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"f"),
            end: bytes::Bytes::from_static(b"p"),
            ts: 10,
        },
        0,
    );

    // Tombstone visible at read_ts >= 10.
    assert!(set.overlaps(b"a", b"z", 10));
    assert!(set.overlaps(b"a", b"z", 100));
    // Tombstone NOT visible at read_ts < 10.
    assert!(!set.overlaps(b"a", b"z", 9));
    assert!(!set.overlaps(b"a", b"z", 0));
}

#[test]
fn test_range_tombstone_iter_overlapping_read_ts() {
    let set = crate::range_tombstone::RangeTombstoneSet::new();
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"f"),
            end: bytes::Bytes::from_static(b"p"),
            ts: 10,
        },
        0,
    );

    // iter_overlapping ignores read_ts (returns all overlapping regardless).
    assert_eq!(set.iter_overlapping(b"a", b"z").count(), 1);
}

#[test]
fn test_range_tombstone_set_len() {
    let set = crate::range_tombstone::RangeTombstoneSet::new();
    assert_eq!(set.len(), 0);
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"a"),
            end: bytes::Bytes::from_static(b"z"),
            ts: 10,
        },
        0,
    );
    assert_eq!(set.len(), 1);
    set.add(
        crate::range_tombstone::RangeTombstone {
            start: bytes::Bytes::from_static(b"b"),
            end: bytes::Bytes::from_static(b"y"),
            ts: 20,
        },
        1,
    );
    assert_eq!(set.len(), 2);
}

// ---------------------------------------------------------------------------
// Phase 2: Read-path integration tests
// ---------------------------------------------------------------------------

/// Helper to collect scan results from a `ScanIterator` (which implements
/// `StorageIterator`, not `std::iter::Iterator`).
fn collect_scan_keys(iter: crate::lsm_iterator::ScanIterator) -> Vec<Vec<u8>> {
    let mut results = Vec::new();
    let mut iter = iter;
    while iter.is_valid() {
        results.push(iter.key().to_vec());
        iter.next().unwrap();
    }
    results
}

fn collect_scan_kv(iter: crate::lsm_iterator::ScanIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut results = Vec::new();
    let mut iter = iter;
    while iter.is_valid() {
        results.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next().unwrap();
    }
    results
}

#[test]
fn test_get_hides_range_deleted_key() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"key1", b"val1").unwrap();
    storage.put(b"key2", b"val2").unwrap();
    storage.put(b"key3", b"val3").unwrap();

    // Delete range [key1, key3) — hides key1 and key2.
    storage.delete_range_internal(b"key1", b"key3").unwrap();

    assert!(storage.get(b"key1").unwrap().is_none());
    assert!(storage.get(b"key2").unwrap().is_none());
    // key3 is at the end boundary — NOT covered (half-open).
    assert_eq!(&storage.get(b"key3").unwrap().unwrap()[..], b"val3");
}

#[test]
fn test_get_preserves_key_at_end_boundary() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"z", b"val").unwrap();
    storage.delete_range_internal(b"a", b"z").unwrap();

    // key == end is NOT covered (half-open [start, end)).
    assert_eq!(&storage.get(b"z").unwrap().unwrap()[..], b"val");
}

#[test]
fn test_get_later_put_recreates_key() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"key1", b"old").unwrap();
    storage.delete_range_internal(b"key1", b"key2").unwrap();
    assert!(storage.get(b"key1").unwrap().is_none());

    // Later put recreates the key.
    storage.put(b"key1", b"new").unwrap();
    assert_eq!(&storage.get(b"key1").unwrap().unwrap()[..], b"new");
}

#[test]
fn test_scan_skips_range_deleted_keys() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"2").unwrap();
    storage.put(b"c", b"3").unwrap();
    storage.put(b"d", b"4").unwrap();

    // Delete [b, d) — hides b and c.
    storage.delete_range_internal(b"b", b"d").unwrap();

    let results = collect_scan_kv(storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap());

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"a");
    assert_eq!(results[1].0, b"d");
}

#[test]
fn test_scan_preserves_uncovered_keys() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"a", b"1").unwrap();
    storage.put(b"m", b"2").unwrap();
    storage.put(b"z", b"3").unwrap();

    // Delete [d, f) — no keys are in this range.
    storage.delete_range_internal(b"d", b"f").unwrap();

    let results = collect_scan_keys(storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap());

    // All keys are outside [d, f), so all are visible.
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], b"a");
    assert_eq!(results[1], b"m");
    assert_eq!(results[2], b"z");
}

#[test]
fn test_prefix_scan_respects_range_tombstones() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"user:1:name", b"alice").unwrap();
    storage.put(b"user:2:name", b"bob").unwrap();
    storage.put(b"user:3:name", b"carol").unwrap();

    // Delete user:2's range.
    storage
        .delete_range_internal(b"user:2:", b"user:3:")
        .unwrap();

    let results = collect_scan_keys(storage.prefix_scan(b"user:").unwrap());

    // user:2:name should be hidden, user:1:name and user:3:name visible.
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], b"user:1:name");
    assert_eq!(results[1], b"user:3:name");
}

#[test]
fn test_get_immutable_memtable_range_tombstone() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"key1", b"val1").unwrap();
    storage.put(b"key2", b"val2").unwrap();

    // Delete range then freeze the memtable.
    storage.delete_range_internal(b"key1", b"key3").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();

    // Both keys should still be hidden from the immutable memtable.
    assert!(storage.get(b"key1").unwrap().is_none());
    assert!(storage.get(b"key2").unwrap().is_none());
}

#[test]
fn test_scan_immutable_memtable_range_tombstone() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"2").unwrap();
    storage.put(b"c", b"3").unwrap();

    // Delete range then freeze.
    storage.delete_range_internal(b"b", b"c").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();

    let results = collect_scan_keys(storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap());

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], b"a");
    assert_eq!(results[1], b"c");
}

#[test]
fn test_get_snapshot_before_delete_range() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"key1", b"val1").unwrap();

    // Pin a read guard BEFORE the delete_range.
    let read_guard = storage.mvcc.as_ref().map(|m| m.new_read_guard());
    let read_ts = read_guard.as_ref().map(|g| g.read_ts());

    storage.delete_range_internal(b"key1", b"key2").unwrap();

    // New readers should not see the key.
    assert!(storage.get(b"key1").unwrap().is_none());

    // The snapshot pinned BEFORE delete_range should still see the key.
    let ts = read_ts.expect("MVCC must be enabled for snapshot visibility test");
    assert!(
        storage.get_with_ts(b"key1", ts).unwrap().is_some(),
        "snapshot before delete_range should still see key1"
    );

    drop(read_guard);
}

#[test]
fn test_fragmenter_with_storage() {
    // Verify that fragments are correctly built from storage range tombstones.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"a", b"1").unwrap();
    storage.put(b"m", b"2").unwrap();
    storage.put(b"z", b"3").unwrap();

    storage.delete_range_internal(b"a", b"m").unwrap();
    storage.delete_range_internal(b"f", b"z").unwrap();

    // Active memtable should have range tombstones.
    let state = storage.state.load();
    let frags = crate::range_tombstone::fragment_range(state.memtable.range_tombstones().raw());
    assert!(!frags.is_empty());

    // Verify fragments cover [a, m) and [f, z).
    // Should have fragments covering the union of [a, m) and [f, z).
    assert!(
        frags
            .iter()
            .any(|f| f.start.as_ref() == b"a" && f.end.as_ref() <= b"m".as_slice())
    );
    assert!(
        frags
            .iter()
            .any(|f| f.start.as_ref() >= b"f".as_slice() && f.end.as_ref() <= b"z".as_slice())
    );
}

#[test]
fn test_range_tombstone_with_point_tombstone_compose() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"2").unwrap();
    storage.put(b"c", b"3").unwrap();

    // Point delete b.
    storage.delete(b"b").unwrap();
    // Range delete [a, c) — hides a (b is already point-deleted).
    storage.delete_range_internal(b"a", b"c").unwrap();

    assert!(storage.get(b"a").unwrap().is_none());
    assert!(storage.get(b"b").unwrap().is_none());
    // c is at end boundary — NOT covered.
    assert_eq!(&storage.get(b"c").unwrap().unwrap()[..], b"3");
}

#[test]
fn test_get_with_ts_range_tombstone() {
    // Transaction point lookups must respect range tombstones.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"key1", b"val1").unwrap();
    storage.put(b"key2", b"val2").unwrap();
    storage.put(b"key3", b"val3").unwrap();

    storage.delete_range_internal(b"key1", b"key3").unwrap();

    // get_with_ts (transaction path) must also hide range-deleted keys.
    // Use a large read_ts to see all committed data.
    let read_ts = u64::MAX - 1;
    assert!(storage.get_with_ts(b"key1", read_ts).unwrap().is_none());
    assert!(storage.get_with_ts(b"key2", read_ts).unwrap().is_none());
    // key3 is at end boundary — NOT covered (half-open).
    assert!(storage.get_with_ts(b"key3", read_ts).unwrap().is_some());
}

#[test]
fn test_flush_writes_range_tombstones_to_sst() {
    let dir = tempdir().unwrap();
    let memtable = MemTable::create(0, false);
    memtable.for_testing_put_slice(b"key1", b"val1").unwrap();
    memtable.put_range_tombstone(b"a", b"z", 10, 0).unwrap();

    let mut builder = crate::table::SsTableBuilder::new(4096);
    memtable.flush(&mut builder).unwrap();
    let sst = builder
        .build_for_test(dir.path().join("test_flush_rt.sst"))
        .unwrap();

    // SST should contain the range tombstones in v4 format.
    assert!(
        sst.has_range_tombstones(),
        "SST should have range tombstones"
    );
    let frags = sst.range_tombstone_fragments().unwrap();
    assert!(!frags.is_empty(), "fragments should not be empty");
    // The tombstone [a, z)@10 should cover key "key1".
    let ts = crate::range_tombstone::find_newest_covering_ts(frags, b"key1", 100);
    assert_eq!(ts, Some(10));
}

#[test]
fn test_scan_range_tombstone_hides_older_not_newer_version() {
    // A range tombstone at ts=T should hide point versions with ts <= T,
    // but NOT newer versions with ts > T.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"k", b"old").unwrap();
    storage.delete_range_internal(b"k", b"l").unwrap();
    storage.put(b"k", b"new").unwrap();

    // Scan should yield "new" (the put after the range tombstone).
    let mut scan = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    assert!(scan.is_valid());
    assert_eq!(scan.key(), b"k");
    assert_eq!(scan.value(), b"new");
    scan.next().unwrap();
    assert!(!scan.is_valid());
}

#[test]
fn test_get_put_in_imm_range_delete_and_put_in_active() {
    // Put in imm_memtable, range delete + new put in active memtable.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"k", b"old").unwrap();
    // Force freeze — moves current memtable to imm_memtables.
    let state_lock = storage.state_lock.lock();
    storage.force_freeze_memtable(&state_lock).unwrap();
    drop(state_lock);

    storage.delete_range_internal(b"k", b"l").unwrap();
    storage.put(b"k", b"new").unwrap();

    // get should return "new" — the put after the range tombstone.
    assert_eq!(&storage.get(b"k").unwrap().unwrap()[..], b"new");
}

#[test]
fn test_get_key_outside_range_tombstones() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"1").unwrap();
    storage.put(b"z", b"2").unwrap();
    storage.delete_range_internal(b"m", b"n").unwrap();

    // Keys outside the tombstone range should be visible.
    assert_eq!(&storage.get(b"a").unwrap().unwrap()[..], b"1");
    assert_eq!(&storage.get(b"z").unwrap().unwrap()[..], b"2");
}

// ---------------------------------------------------------------------------
// Phase 3: SST v4 range-tombstone tests
// ---------------------------------------------------------------------------

#[test]
fn test_flush_with_range_tombstone_sst_read() {
    // put → delete_range → flush → get should return None for covered keys.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"key1", b"val1").unwrap();
    storage.put(b"key2", b"val2").unwrap();
    storage.delete_range_internal(b"key1", b"key3").unwrap();

    // Force flush — the memtable now has both point entries and range tombstones.
    let state_lock = storage.state_lock.lock();
    storage.force_freeze_memtable(&state_lock).unwrap();
    drop(state_lock);
    storage.force_flush_next_imm_memtable().unwrap();

    // Both keys should be hidden by the SST range tombstone.
    assert!(storage.get(b"key1").unwrap().is_none());
    assert!(storage.get(b"key2").unwrap().is_none());

    // Key outside the range should still be visible if present.
    storage.put(b"key4", b"val4").unwrap();
    assert_eq!(&storage.get(b"key4").unwrap().unwrap()[..], b"val4");
}

#[test]
fn test_range_only_sst_flush() {
    // delete_range with no point entries → flush → verify SST opens and
    // range tombstones are readable.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.delete_range_internal(b"a", b"z").unwrap();

    let state_lock = storage.state_lock.lock();
    storage.force_freeze_memtable(&state_lock).unwrap();
    drop(state_lock);
    storage.force_flush_next_imm_memtable().unwrap();

    // The SST should have been created and contain range tombstones.
    let state = storage.state.load();
    assert!(!state.l0_sstables.is_empty(), "should have flushed an SST");
    let sst_id = state.l0_sstables.last().unwrap();
    let sst = state.sstables.get(sst_id).unwrap();
    assert!(
        sst.has_range_tombstones(),
        "SST should have range tombstones"
    );
}

#[test]
fn test_recovery_preserves_range_tombstones() {
    // put → delete_range → flush → close → reopen → verify tombstones survive.
    let dir = tempdir().unwrap();

    {
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
        storage.put(b"key1", b"val1").unwrap();
        storage.delete_range_internal(b"key1", b"key2").unwrap();

        let state_lock = storage.state_lock.lock();
        storage.force_freeze_memtable(&state_lock).unwrap();
        drop(state_lock);
        storage.force_flush_next_imm_memtable().unwrap();
    }

    // Reopen — the range tombstone should have been recovered from the SST.
    {
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
        assert!(
            storage.get(b"key1").unwrap().is_none(),
            "key1 should be hidden by recovered range tombstone"
        );
    }
}

#[test]
fn test_scan_hides_keys_covered_by_sst_range_tombstone() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"2").unwrap();
    storage.put(b"c", b"3").unwrap();
    storage.delete_range_internal(b"a", b"c").unwrap();

    let state_lock = storage.state_lock.lock();
    storage.force_freeze_memtable(&state_lock).unwrap();
    drop(state_lock);
    storage.force_flush_next_imm_memtable().unwrap();

    // Scan — "a" and "b" should be hidden, "c" should be visible.
    let mut iter = storage
        .scan(Bound::Included(b"a"), Bound::Included(b"z"))
        .unwrap();
    let mut keys = Vec::new();
    while iter.is_valid() {
        keys.push(iter.key().to_vec());
        iter.next().unwrap();
    }
    assert_eq!(keys.len(), 1, "expected only 'c', got {:?}", keys);
    assert_eq!(&keys[0], b"c");
}

#[test]
fn test_public_delete_range_api() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();

    storage.put(b"a", b"1").unwrap();
    storage.put(b"b", b"2").unwrap();
    storage.put(b"c", b"3").unwrap();

    // Exercise the public KvEngine::delete_range path.
    storage.delete_range(b"a", b"c").unwrap();

    // "a" and "b" should be hidden, "c" should be visible.
    assert!(storage.get(b"a").unwrap().is_none());
    assert!(storage.get(b"b").unwrap().is_none());
    assert_eq!(storage.get(b"c").unwrap().unwrap().as_ref(), b"3");
}

#[test]
fn test_delete_range_invalid_bounds() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
    // start > end
    assert!(storage.delete_range(b"c", b"a").is_err());
    // start == end
    assert!(storage.delete_range(b"a", b"a").is_err());
}

#[test]
fn test_range_tombstone_stats() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();

    // Initially no range tombstones.
    let stats = storage.range_tombstone_stats();
    assert_eq!(stats.active_count, 0);
    assert_eq!(stats.immutable_count, 0);
    assert_eq!(stats.sst_count, 0);
    assert_eq!(stats.covering_lookups, 0);
    assert_eq!(stats.covering_hits, 0);

    // Add some keys and a range tombstone.
    storage.put(b"m", b"val").unwrap();
    storage.delete_range(b"a", b"z").unwrap();

    let stats = storage.range_tombstone_stats();
    assert_eq!(stats.active_count, 1);
    assert_eq!(stats.immutable_count, 0);

    // Flush — tombstone moves from active to SST.
    storage.force_flush().unwrap();

    let stats = storage.range_tombstone_stats();
    assert_eq!(stats.active_count, 0);
    assert_eq!(stats.immutable_count, 0);
    // The range-only SST should exist with fragments.
    assert!(stats.sst_count >= 1);
    assert!(stats.total_sst_fragment_count >= 1);

    // Exercise runtime counters: get on a covered key.
    let result = storage.get(b"m").unwrap();
    assert!(
        result.is_none(),
        "key 'm' should be hidden by range tombstone"
    );

    let stats = storage.range_tombstone_stats();
    assert!(
        stats.covering_lookups >= 1,
        "should have counted at least one lookup"
    );
    assert!(
        stats.covering_hits >= 1,
        "should have counted at least one hit"
    );
}
