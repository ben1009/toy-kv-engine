use std::{ops::Bound, sync::Arc, time::Duration};

use bytes::Bytes;
use tempfile::tempdir;

use self::harness::{check_lsm_iter_result_by_key, sync};
use super::*;
use crate::{
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageInner, LsmStorageOptions},
};

fn wal_files_in_dir(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut wals = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("wal") {
            wals.push(p);
        }
    }
    wals
}

#[test]
fn test_task1_storage_scan() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"0", b"2333333").unwrap();
    storage.put(b"00", b"2333333").unwrap();
    storage.put(b"4", b"23").unwrap();
    sync(&storage);

    storage.delete(b"4").unwrap();
    sync(&storage);

    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"00", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();

    {
        let state = storage.state.read();
        assert_eq!(state.l0_sstables.len(), 2);
        assert_eq!(state.imm_memtables.len(), 2);
    }

    check_lsm_iter_result_by_key(
        &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("0"), Bytes::from("2333333")),
            (Bytes::from("00"), Bytes::from("2333")),
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_lsm_iter_result_by_key(
        &mut storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_task1_storage_get() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"0", b"2333333").unwrap();
    storage.put(b"00", b"2333333").unwrap();
    storage.put(b"4", b"23").unwrap();
    sync(&storage);

    storage.delete(b"4").unwrap();
    sync(&storage);

    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"00", b"2333").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();

    {
        let state = storage.state.read();
        assert_eq!(state.l0_sstables.len(), 2);
        assert_eq!(state.imm_memtables.len(), 2);
    }

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
fn test_task2_auto_flush() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(&dir, LsmStorageOptions::default_for_scan_flush_test()).unwrap();

    let value = "1".repeat(1024); // 1KB

    // approximately 6MB
    for i in 0..6000 {
        storage
            .put(format!("{i}").as_bytes(), value.as_bytes())
            .unwrap();
    }

    std::thread::sleep(Duration::from_millis(500));

    assert!(!storage.inner.state.read().l0_sstables.is_empty());
}

#[test]
fn test_task3_sst_filter() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    for i in 1..=10000 {
        if i % 1000 == 0 {
            sync(&storage);
        }
        storage
            .put(format!("{i:05}").as_bytes(), b"2333333")
            .unwrap();
    }

    let iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    assert!(
        iter.num_active_iterators() >= 10,
        "did you implement num_active_iterators? current active iterators = {}",
        iter.num_active_iterators()
    );
    let max_num = iter.num_active_iterators();
    let iter = storage
        .scan(
            Bound::Excluded(format!("{:05}", 10000).as_bytes()),
            Bound::Unbounded,
        )
        .unwrap();
    assert!(iter.num_active_iterators() < max_num);
    let min_num = iter.num_active_iterators();
    let iter = storage
        .scan(
            Bound::Unbounded,
            Bound::Excluded(format!("{:05}", 1).as_bytes()),
        )
        .unwrap();
    assert_eq!(iter.num_active_iterators(), min_num);
    let iter = storage
        .scan(
            Bound::Unbounded,
            Bound::Included(format!("{:05}", 0).as_bytes()),
        )
        .unwrap();
    assert_eq!(iter.num_active_iterators(), min_num);
    let iter = storage
        .scan(
            Bound::Included(format!("{:05}", 10001).as_bytes()),
            Bound::Unbounded,
        )
        .unwrap();
    assert_eq!(iter.num_active_iterators(), min_num);
    let iter = storage
        .scan(
            Bound::Included(format!("{:05}", 5000).as_bytes()),
            Bound::Excluded(format!("{:05}", 6000).as_bytes()),
        )
        .unwrap();
    assert!(min_num <= iter.num_active_iterators() && iter.num_active_iterators() < max_num);
}

#[test]
fn test_wal_gc_after_flush() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());

    // Write data and freeze the memtable → creates a WAL file
    storage.put(b"key1", b"value1").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();

    let state = storage.state.read();
    assert_eq!(state.imm_memtables.len(), 1);
    let wal_id = state.imm_memtables[0].id();
    drop(state);

    let wal_path = LsmStorageInner::path_of_wal_static(&dir, wal_id);
    assert!(wal_path.exists(), "WAL should exist before flush");

    // Flush the immutable memtable
    storage.force_flush_next_imm_memtable().unwrap();

    // WAL file should be removed after successful flush
    assert!(!wal_path.exists(), "WAL should be removed after flush");

    // Data must still be readable from the SST
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from_static(b"value1"))
    );
}

#[test]
fn test_wal_gc_multiple_flushes() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());

    // Freeze three memtables
    for i in 0..3 {
        storage
            .put(format!("key{i}").as_bytes(), format!("value{i}").as_bytes())
            .unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
    }

    let wal_ids_before: Vec<usize> = {
        let state = storage.state.read();
        state.imm_memtables.iter().map(|m| m.id()).collect()
    };
    assert_eq!(wal_ids_before.len(), 3);

    // Flush all three
    for _ in 0..3 {
        storage.force_flush_next_imm_memtable().unwrap();
    }

    // All WALs should be gone
    for id in wal_ids_before {
        assert!(
            !LsmStorageInner::path_of_wal_static(&dir, id).exists(),
            "WAL {id} should be removed after flush"
        );
    }

    // Data still readable
    for i in 0..3 {
        assert_eq!(
            storage.get(format!("key{i}").as_bytes()).unwrap(),
            Some(Bytes::from(format!("value{i}")))
        );
    }
}

#[test]
fn test_wal_gc_with_background_flush_thread() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_scan_flush_test();
    options.enable_wal = true;
    let storage = KvEngine::open(&dir, options).unwrap();

    // Manually freeze multiple memtables so the background flush thread has
    // work to do. This avoids a race where the flush thread deletes WALs
    // while the write loop is still running.
    let value = Bytes::from("x".repeat(1024));
    for i in 0..3 {
        storage.put(format!("key{i}").as_bytes(), &value).unwrap();
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
    }

    // At this point: 3 imm_memtables + 1 current memtable → 4 WALs created.
    // The background flush thread may have already flushed some of them.
    // Poll until at least one WAL is deleted, proving a flush ran and cleaned up.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut final_wals = wal_files_in_dir(dir.path());
    while final_wals.len() >= 4 {
        assert!(
            std::time::Instant::now() < deadline,
            "timeout waiting for WAL garbage collection"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        final_wals = wal_files_in_dir(dir.path());
    }

    // Verify data integrity
    for i in 0..3 {
        assert_eq!(
            storage.get(format!("key{i}").as_bytes()).unwrap(),
            Some(value.clone())
        );
    }
}

#[test]
#[cfg(not(target_os = "windows"))]
fn test_wal_gc_handles_missing_wal_file() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());

    storage.put(b"key1", b"value1").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();

    let state = storage.state.read();
    let wal_id = state.imm_memtables[0].id();
    drop(state);

    // Manually delete the WAL file before flush to simulate a scenario
    // where it has already been cleaned up (e.g. by crash recovery).
    let wal_path = LsmStorageInner::path_of_wal_static(&dir, wal_id);
    std::fs::remove_file(&wal_path).unwrap();

    // Flush should succeed even though the WAL file is already gone.
    storage.force_flush_next_imm_memtable().unwrap();

    // Data must still be readable from the SST.
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from_static(b"value1"))
    );
}

#[test]
fn test_wal_gc_eprints_on_unexpected_io_error() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());

    storage.put(b"key1", b"value1").unwrap();
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();

    let state = storage.state.read();
    let wal_id = state.imm_memtables[0].id();
    drop(state);

    // Replace the WAL file with a directory so remove_file fails with a
    // non-NotFound error (IsADirectory on Unix, AccessDenied on Windows).
    let wal_path = LsmStorageInner::path_of_wal_static(&dir, wal_id);
    std::fs::remove_file(&wal_path).unwrap();
    std::fs::create_dir(&wal_path).unwrap();

    // Flush should still succeed even though WAL deletion fails with an IO error.
    storage.force_flush_next_imm_memtable().unwrap();

    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from_static(b"value1"))
    );
}
