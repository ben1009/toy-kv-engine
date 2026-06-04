use super::*;

#[test]
fn test_manifest_snapshot_basic() {
    // Write data, trigger snapshot, write more data, close, reopen.
    // Verify all data survives recovery via snapshot + manifest replay.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_enabled(256, 1 << 20);
    // Set a low threshold so snapshot triggers after a few flushes
    options.manifest_snapshot_threshold_bytes = 50;
    let storage = KvEngine::open(dir.path(), options.clone()).unwrap();

    // Write and flush — generates manifest records (NewMemtable + Flush = ~50 bytes)
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage
            .put(key.as_bytes(), &[b'a' + (i as u8 % 26); 64])
            .unwrap();
    }
    force_flush(&storage.inner);

    // Write more data and flush again — snapshot should trigger during these writes
    for i in 5..10 {
        let key = format!("key_{:04}", i);
        storage
            .put(key.as_bytes(), &[b'a' + (i as u8 % 26); 64])
            .unwrap();
    }
    force_flush(&storage.inner);

    storage.close().unwrap();

    // Verify MANIFEST_SNAPSHOT was created
    let snapshot_path = dir.path().join("MANIFEST_SNAPSHOT");
    assert!(
        snapshot_path.exists(),
        "MANIFEST_SNAPSHOT should exist after threshold is exceeded"
    );

    // Reopen and verify all data is intact
    let storage = KvEngine::open(dir.path(), options).unwrap();
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let expected_byte = b'a' + (i as u8 % 26);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(vec![expected_byte; 64])),
            "key {} should survive snapshot recovery",
            key
        );
    }
}

#[test]
fn test_manifest_snapshot_with_vlog() {
    // Verify snapshot correctly captures vLog references.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_enabled(256, 1 << 20);
    options.manifest_snapshot_threshold_bytes = 256;
    let storage = KvEngine::open(dir.path(), options.clone()).unwrap();

    // Write large values (go to vLog)
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.put(key.as_bytes(), &[b'x'; 64]).unwrap();
    }
    force_flush(&storage.inner);

    // Overwrite to create stale entries, flush again
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.put(key.as_bytes(), &[b'y'; 64]).unwrap();
    }
    force_flush(&storage.inner);

    storage.close().unwrap();

    // Reopen — snapshot should have vLog references
    let storage = KvEngine::open(dir.path(), options).unwrap();
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(vec![b'y'; 64])),
            "overwritten key {} should have latest value after snapshot recovery",
            key
        );
    }
}

#[test]
fn test_manifest_snapshot_disabled_by_default() {
    // With threshold 0, no snapshot should be created
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    // threshold is 0 (disabled)
    let storage = KvEngine::open(dir.path(), options.clone()).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    force_flush(&storage.inner);
    storage.close().unwrap();

    // No snapshot should exist
    let snapshot_path = dir.path().join("MANIFEST_SNAPSHOT");
    assert!(
        !snapshot_path.exists(),
        "MANIFEST_SNAPSHOT should NOT exist when threshold is 0"
    );

    // Data should still be recoverable via plain manifest
    let storage = KvEngine::open(dir.path(), options).unwrap();
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
}
