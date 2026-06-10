use super::*;

#[test]
fn test_end_to_end_large_value_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write a large value (> min_value_size of 16)
    let large_value = vec![b'x'; 256];
    storage.put(b"large_key", &large_value).unwrap();

    // Flush to SST (which should write to vLog)
    force_flush(&storage.inner);

    // Read back — should dereference the ValuePointer
    let result = storage.get(b"large_key").unwrap();
    assert_eq!(result, Some(Bytes::from(large_value)));
}

#[test]
fn test_end_to_end_small_value_inline() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write a small value (< min_value_size of 16)
    storage.put(b"small_key", b"tiny").unwrap();

    // Flush to SST
    force_flush(&storage.inner);

    // Read back — should return inline value
    let result = storage.get(b"small_key").unwrap();
    assert_eq!(result, Some(Bytes::from_static(b"tiny")));
}

#[test]
fn test_end_to_end_tombstone_with_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write then delete
    storage.put(b"key1", b"some_value_long_enough").unwrap();
    force_flush(&storage.inner);

    storage.delete(b"key1").unwrap();
    force_flush(&storage.inner);

    // Should return None (tombstone)
    let result = storage.get(b"key1").unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_end_to_end_mixed_inline_and_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write small and large values
    storage.put(b"small", b"tiny").unwrap();
    storage.put(b"large", &[b'y'; 128]).unwrap();

    force_flush(&storage.inner);

    // Both should be readable
    assert_eq!(
        storage.get(b"small").unwrap(),
        Some(Bytes::from_static(b"tiny"))
    );
    assert_eq!(
        storage.get(b"large").unwrap(),
        Some(Bytes::from(vec![b'y'; 128]))
    );
}

#[test]
fn test_scan_with_vlog_values() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"a", b"aaa").unwrap(); // small
    storage.put(b"b", &[b'b'; 64]).unwrap(); // large
    storage.put(b"c", b"ccc").unwrap(); // small

    force_flush(&storage.inner);

    let mut scan = storage
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();

    let mut results = vec![];
    while scan.is_valid() {
        results.push((scan.key().to_vec(), Bytes::copy_from_slice(scan.value())));
        scan.next().unwrap();
    }

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (b"a".to_vec(), Bytes::from_static(b"aaa")));
    assert_eq!(results[1], (b"b".to_vec(), Bytes::from(vec![b'b'; 64])));
    assert_eq!(results[2], (b"c".to_vec(), Bytes::from_static(b"ccc")));
}

#[test]
fn test_compaction_with_mixed_inline_and_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_and_compaction(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write small (inline) and large (pointer) values
    storage.put(b"aaa", b"tiny_aaa").unwrap();
    storage.put(b"bbb", &[b'b'; 128]).unwrap();
    storage.put(b"ccc", b"tiny_ccc").unwrap();
    storage.put(b"ddd", &[b'd'; 128]).unwrap();

    // Flush to create SSTs
    force_flush(&storage.inner);

    storage.put(b"eee", b"tiny_eee").unwrap();
    storage.put(b"fff", &[b'f'; 128]).unwrap();
    force_flush(&storage.inner);

    // Force full compaction
    storage.inner.force_full_compaction().unwrap();

    // All values should still be readable after compaction
    assert_eq!(
        storage.get(b"aaa").unwrap(),
        Some(Bytes::from_static(b"tiny_aaa"))
    );
    assert_eq!(
        storage.get(b"bbb").unwrap(),
        Some(Bytes::from(vec![b'b'; 128]))
    );
    assert_eq!(
        storage.get(b"ccc").unwrap(),
        Some(Bytes::from_static(b"tiny_ccc"))
    );
    assert_eq!(
        storage.get(b"ddd").unwrap(),
        Some(Bytes::from(vec![b'd'; 128]))
    );
    assert_eq!(
        storage.get(b"eee").unwrap(),
        Some(Bytes::from_static(b"tiny_eee"))
    );
    assert_eq!(
        storage.get(b"fff").unwrap(),
        Some(Bytes::from(vec![b'f'; 128]))
    );
}

#[test]
fn test_recovery_with_vlog_records() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);

    // Write data and flush
    {
        let storage = KvEngine::open(dir.path(), options.clone()).unwrap();
        storage.put(b"key1", &[b'a'; 64]).unwrap();
        storage.put(b"key2", b"small").unwrap();
        storage.put(b"key3", &[b'c'; 128]).unwrap();
        force_flush(&storage.inner);
        storage.close().unwrap();
    }

    // Reopen and verify data survives recovery
    {
        let storage = KvEngine::open(dir.path(), options).unwrap();
        assert_eq!(
            storage.get(b"key1").unwrap(),
            Some(Bytes::from(vec![b'a'; 64]))
        );
        assert_eq!(
            storage.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"small"))
        );
        assert_eq!(
            storage.get(b"key3").unwrap(),
            Some(Bytes::from(vec![b'c'; 128]))
        );
    }
}

#[test]
fn test_value_at_min_value_size_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Value exactly at min_value_size (16 bytes) — should be separated to vLog
    let boundary_value = vec![b'x'; 16];
    storage.put(b"boundary", &boundary_value).unwrap();

    // Value one byte below threshold — should be inline
    let below_value = vec![b'y'; 15];
    storage.put(b"below", &below_value).unwrap();

    force_flush(&storage.inner);

    assert_eq!(
        storage.get(b"boundary").unwrap(),
        Some(Bytes::from(boundary_value))
    );
    assert_eq!(
        storage.get(b"below").unwrap(),
        Some(Bytes::from(below_value))
    );
}

#[test]
fn test_multiple_flushes_different_vlog_files() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // First flush — large values go to vLog file 0
    storage.put(b"a1", &[b'a'; 64]).unwrap();
    storage.put(b"a2", &[b'b'; 64]).unwrap();
    force_flush(&storage.inner);

    // Second flush — large values go to vLog file 1
    storage.put(b"b1", &[b'c'; 64]).unwrap();
    storage.put(b"b2", &[b'd'; 64]).unwrap();
    force_flush(&storage.inner);

    // Third flush
    storage.put(b"c1", &[b'e'; 64]).unwrap();
    force_flush(&storage.inner);

    // All values should be readable from their respective vLog files
    assert_eq!(
        storage.get(b"a1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    assert_eq!(
        storage.get(b"a2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
    assert_eq!(
        storage.get(b"b1").unwrap(),
        Some(Bytes::from(vec![b'c'; 64]))
    );
    assert_eq!(
        storage.get(b"b2").unwrap(),
        Some(Bytes::from(vec![b'd'; 64]))
    );
    assert_eq!(
        storage.get(b"c1").unwrap(),
        Some(Bytes::from(vec![b'e'; 64]))
    );
}

#[test]
fn test_scan_after_compaction_with_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_and_compaction(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write mixed data across multiple flushes
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let value = vec![b'v'; 64]; // all large values
        storage.put(key.as_bytes(), &value).unwrap();
    }
    force_flush(&storage.inner);

    for i in 10..20 {
        let key = format!("key_{:04}", i);
        let value = format!("small_{}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    force_flush(&storage.inner);

    // Compact
    storage.inner.force_full_compaction().unwrap();

    // Scan should return all values in order
    let mut scan = storage
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();

    let mut count = 0;
    while scan.is_valid() {
        let key_str = String::from_utf8(scan.key().to_vec()).unwrap();
        assert!(key_str.starts_with("key_"));
        count += 1;
        scan.next().unwrap();
    }
    assert_eq!(count, 20);
}

#[test]
fn test_write_batch_with_vlog() {
    use crate::lsm_storage::WriteBatchRecord;

    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Batch with puts (large values go to vlog)
    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", &[b'a'; 64]),
        WriteBatchRecord::Put(b"k2", &[b'b'; 64]),
        WriteBatchRecord::Put(b"k3", &[b'c'; 64]),
    ];
    storage.write_batch(&batch).unwrap();

    force_flush(&storage.inner);

    assert_eq!(
        storage.get(b"k1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    assert_eq!(
        storage.get(b"k2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
    assert_eq!(
        storage.get(b"k3").unwrap(),
        Some(Bytes::from(vec![b'c'; 64]))
    );
}

#[test]
fn test_write_batch_with_delete_and_vlog() {
    use crate::lsm_storage::WriteBatchRecord;

    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Put then delete in same batch
    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", &[b'a'; 64]),
        WriteBatchRecord::Put(b"k2", &[b'b'; 64]),
        WriteBatchRecord::Del(b"k1"),
    ];
    storage.write_batch(&batch).unwrap();

    force_flush(&storage.inner);

    assert!(storage.get(b"k1").unwrap().is_none());
    assert_eq!(
        storage.get(b"k2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
}

#[test]
fn test_delete_with_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large value to vlog
    storage.put(b"key1", &[b'v'; 64]).unwrap();
    force_flush(&storage.inner);

    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'v'; 64]))
    );

    // Delete
    storage.delete(b"key1").unwrap();
    force_flush(&storage.inner);

    assert!(storage.get(b"key1").unwrap().is_none());
}

#[test]
fn test_overwrite_large_value_with_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large value
    storage.put(b"key1", &[b'a'; 64]).unwrap();
    force_flush(&storage.inner);

    // Overwrite with another large value
    storage.put(b"key1", &[b'b'; 64]).unwrap();
    force_flush(&storage.inner);

    // Should see the new value
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
}

#[test]
fn test_mixed_inline_and_vlog_values() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Small value (inline)
    storage.put(b"small", b"tiny").unwrap();
    // Large value (vlog)
    storage.put(b"large", &[b'x'; 64]).unwrap();

    force_flush(&storage.inner);

    assert_eq!(
        storage.get(b"small").unwrap(),
        Some(Bytes::from_static(b"tiny"))
    );
    assert_eq!(
        storage.get(b"large").unwrap(),
        Some(Bytes::from(vec![b'x'; 64]))
    );
}
