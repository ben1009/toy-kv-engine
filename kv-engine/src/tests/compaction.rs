use std::{ops::Bound, path::Path, sync::Arc};

use bytes::Bytes;
use harness::{TOMBSTONE_VALUE, construct_merge_iterator_over_storage};
use tempfile::tempdir;

use self::harness::{check_iter_result_by_key, check_lsm_iter_result_by_key, sync};
use super::*;
use crate::{
    iterators::{StorageIterator, concat_iterator::SstConcatIterator},
    key::{KeySlice, TS_ENABLED},
    lsm_storage::{CompactionFilterRequest, LsmStorageInner, LsmStorageOptions},
    table::{SsTable, SsTableBuilder},
};

#[test]
fn test_task1_full_compaction() {
    // We do not use LSM iterator in this test because it's implemented as part of task 3
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    #[allow(clippy::let_unit_value)]
    let _txn = storage.new_txn().unwrap();
    storage.put(b"0", b"v1").unwrap();
    sync(&storage);
    storage.put(b"0", b"v2").unwrap();
    storage.put(b"1", b"v2").unwrap();
    storage.put(b"2", b"v2").unwrap();
    sync(&storage);
    storage.delete(b"0").unwrap();
    storage.delete(b"2").unwrap();
    sync(&storage);
    assert_eq!(storage.state.load().l0_sstables.len(), 3);
    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    if TS_ENABLED {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (
                    Bytes::from_static(b"0"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                (
                    Bytes::from_static(b"2"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
            ],
        );
    } else {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (
                    Bytes::from_static(b"0"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                (
                    Bytes::from_static(b"2"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
            ],
        );
    }
    storage.force_full_compaction().unwrap();
    assert!(storage.state.load().l0_sstables.is_empty());
    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    if TS_ENABLED {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (
                    Bytes::from_static(b"0"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                (
                    Bytes::from_static(b"2"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
            ],
        );
    } else {
        check_iter_result_by_key(
            &mut iter,
            vec![(Bytes::from_static(b"1"), Bytes::from_static(b"v2"))],
        );
    }
    storage.put(b"0", b"v3").unwrap();
    storage.put(b"2", b"v3").unwrap();
    sync(&storage);
    storage.delete(b"1").unwrap();
    sync(&storage);
    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    if TS_ENABLED {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                (
                    Bytes::from_static(b"0"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                (
                    Bytes::from_static(b"1"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
                (
                    Bytes::from_static(b"2"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
            ],
        );
    } else {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                (
                    Bytes::from_static(b"1"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
            ],
        );
    }
    storage.force_full_compaction().unwrap();
    assert!(storage.state.load().l0_sstables.is_empty());
    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    if TS_ENABLED {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                (
                    Bytes::from_static(b"0"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"0"), Bytes::from_static(b"v1")),
                (
                    Bytes::from_static(b"1"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"1"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
                (
                    Bytes::from_static(b"2"),
                    Bytes::from_static(TOMBSTONE_VALUE),
                ),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v2")),
            ],
        );
    } else {
        check_iter_result_by_key(
            &mut iter,
            vec![
                (Bytes::from_static(b"0"), Bytes::from_static(b"v3")),
                (Bytes::from_static(b"2"), Bytes::from_static(b"v3")),
            ],
        );
    }
}

fn generate_concat_sst(
    start_key: usize,
    end_key: usize,
    dir: impl AsRef<Path>,
    id: usize,
) -> SsTable {
    let mut builder = SsTableBuilder::new(128);
    for idx in start_key..end_key {
        let key = format!("{idx:05}");
        builder
            .add(
                KeySlice::for_testing_from_slice_no_ts(key.as_bytes()),
                b"test",
            )
            .unwrap();
    }
    let path = dir.as_ref().join(format!("{id}.sst"));
    builder.build_for_test(path).unwrap()
}

#[test]
fn test_task2_concat_iterator() {
    let dir = tempdir().unwrap();
    let mut sstables = Vec::new();
    for i in 1..=10 {
        sstables.push(Arc::new(generate_concat_sst(
            i * 10,
            (i + 1) * 10,
            dir.path(),
            i,
        )));
    }
    for key in 0..120 {
        let iter = SstConcatIterator::create_and_seek_to_key(
            sstables.clone(),
            KeySlice::for_testing_from_slice_no_ts(format!("{key:05}").as_bytes()),
        )
        .unwrap();
        if key < 10 {
            assert!(iter.is_valid());
            assert_eq!(iter.key().for_testing_key_ref(), b"00010");
        } else if key >= 110 {
            assert!(!iter.is_valid());
        } else {
            assert!(iter.is_valid());
            assert_eq!(
                iter.key().for_testing_key_ref(),
                format!("{key:05}").as_bytes()
            );
        }
    }
    let iter = SstConcatIterator::create_and_seek_to_first(sstables.clone()).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key().for_testing_key_ref(), b"00010");
}

#[test]
fn test_task3_integration() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"0", b"2333333").unwrap();
    storage.put(b"00", b"2333333").unwrap();
    storage.put(b"4", b"23").unwrap();
    sync(&storage);

    storage.delete(b"4").unwrap();
    sync(&storage);
    storage.force_full_compaction().unwrap();
    assert!(storage.state.load().l0_sstables.is_empty());
    assert!(!storage.state.load().levels[0].1.is_empty());

    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    sync(&storage);

    storage.put(b"00", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();
    sync(&storage);
    storage.force_full_compaction().unwrap();
    assert!(storage.state.load().l0_sstables.is_empty());
    assert!(!storage.state.load().levels[0].1.is_empty());

    check_lsm_iter_result_by_key(
        &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("0"), Bytes::from("2333333")),
            (Bytes::from("00"), Bytes::from("2333")),
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    assert_eq!(
        storage.get(b"0").unwrap(),
        Some(Bytes::from_static(b"2333333"))
    );
    assert_eq!(
        storage.get(b"00").unwrap(),
        Some(Bytes::from_static(b"2333"))
    );
    assert_eq!(
        storage.get(b"2").unwrap(),
        Some(Bytes::from_static(b"2333"))
    );
    assert_eq!(
        storage.get(b"3").unwrap(),
        Some(Bytes::from_static(b"23333"))
    );
    assert_eq!(storage.get(b"4").unwrap(), None);
    assert_eq!(storage.get(b"--").unwrap(), None);
    assert_eq!(storage.get(b"555").unwrap(), None);
}

#[test]
fn test_watermark_gc_drops_old_versions() {
    // No active readers → watermark = latest_commit_ts.
    // After compaction, only the newest version of each key survives.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"v1").unwrap();
    sync(&storage);
    storage.put(b"a", b"v2").unwrap();
    sync(&storage);
    storage.put(b"a", b"v3").unwrap();
    sync(&storage);

    storage.force_full_compaction().unwrap();

    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    check_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from_static(b"a"), Bytes::from_static(b"v3"))],
    );
}

#[test]
fn test_watermark_gc_preserves_versions_for_active_reader() {
    // Active reader at ts=1 pins the watermark at 1.
    // Versions with ts > watermark (ts=2, ts=3, ts=4) are all kept.
    // The newest version at ts <= watermark (ts=1) is also kept.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"v1").unwrap();
    sync(&storage);

    // Pin a reader at the current latest_commit_ts (ts=1).
    let _reader = storage.new_txn().unwrap();

    storage.put(b"a", b"v2").unwrap();
    sync(&storage);
    storage.put(b"a", b"v3").unwrap();
    sync(&storage);
    storage.put(b"a", b"v4").unwrap();
    sync(&storage);

    storage.force_full_compaction().unwrap();

    // Versions ts=2..=4 are above the watermark (always kept).
    // Version ts=1 is the newest at/below the watermark (kept as visible snapshot).
    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"v4")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"v3")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"v1")),
        ],
    );
}

#[test]
fn test_watermark_gc_drops_tombstone_at_bottom() {
    // Tombstone at the bottom level with ts <= watermark is dropped
    // (no reader can see it, no lower level has older versions).
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"v1").unwrap();
    sync(&storage);
    storage.delete(b"a").unwrap();
    sync(&storage);

    storage.force_full_compaction().unwrap();

    // Both the put and the tombstone are at/below watermark — both dropped.
    let iter = construct_merge_iterator_over_storage(&storage.state.load());
    assert!(!iter.is_valid());
}

#[test]
fn test_watermark_gc_multiple_keys() {
    // Interleaved versions of different keys are handled independently.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"a", b"a1").unwrap();
    sync(&storage);
    storage.put(b"b", b"b1").unwrap();
    storage.put(b"a", b"a2").unwrap();
    sync(&storage);

    storage.force_full_compaction().unwrap();

    // Only the newest version of each key survives.
    let mut iter = construct_merge_iterator_over_storage(&storage.state.load());
    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"a2")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"b1")),
        ],
    );
}

#[test]
fn test_watermark_gc_preserves_tombstone_at_non_bottom_level() {
    // Tombstone at a non-bottom level is preserved even below the watermark,
    // because deeper levels may still have older versions.
    use crate::compact::{CompactionOptions, LeveledCompactionOptions};

    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_compaction_test(CompactionOptions::Leveled(
        LeveledCompactionOptions {
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 1,
            max_levels: 3,
            base_level_size_mb: 128,
        },
    ));
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());

    // Write "a" = "v1" and flush to L0, then compact to L1.
    storage.put(b"a", b"v1").unwrap();
    sync(&storage);
    storage.trigger_compaction().unwrap();

    // Delete "a" (tombstone) and flush to L0.
    storage.delete(b"a").unwrap();
    sync(&storage);

    // Compact L0→L1. L1 is not the bottom level (L2 exists),
    // so the tombstone must be preserved to hide the older "a"=v1 in L1.
    storage.trigger_compaction().unwrap();

    // Tombstone masks the older version — key is deleted.
    assert_eq!(storage.get(b"a").unwrap(), None);
}

#[test]
fn test_compaction_filter_respects_cutoff_ts() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"old:drop", b"v1").unwrap();
    sync(&storage);

    storage
        .add_compaction_filter(CompactionFilterRequest::prefix(Bytes::from_static(b"old:")))
        .unwrap();

    storage.put(b"old:keep", b"v2").unwrap();
    sync(&storage);
    storage.put(b"live", b"v3").unwrap();
    sync(&storage);

    storage.force_full_compaction().unwrap();

    assert_eq!(storage.get(b"old:drop").unwrap(), None);
    assert_eq!(
        storage.get(b"old:keep").unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        storage.get(b"live").unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
}

#[test]
fn test_compaction_filter_keeps_entries_while_reader_is_active() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    storage.put(b"old:visible", b"v1").unwrap();
    sync(&storage);

    storage
        .add_compaction_filter(CompactionFilterRequest::prefix(Bytes::from_static(b"old:")))
        .unwrap();
    let _reader = storage.new_txn().unwrap();

    storage.force_full_compaction().unwrap();

    assert_eq!(
        storage.get(b"old:visible").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
}

#[test]
fn test_compaction_filter_non_bottom_compaction_keeps_matching_entries() {
    use crate::compact::{CompactionOptions, LeveledCompactionOptions};

    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_compaction_test(CompactionOptions::Leveled(
        LeveledCompactionOptions {
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 1,
            max_levels: 3,
            base_level_size_mb: 128,
        },
    ));
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());

    storage.put(b"old:keep", b"v1").unwrap();
    sync(&storage);
    storage.trigger_compaction().unwrap();

    storage
        .add_compaction_filter(CompactionFilterRequest::prefix(Bytes::from_static(b"old:")))
        .unwrap();
    storage.put(b"other", b"v2").unwrap();
    sync(&storage);

    storage.trigger_compaction().unwrap();

    assert_eq!(
        storage.get(b"old:keep").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        storage.get(b"other").unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
}
