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
