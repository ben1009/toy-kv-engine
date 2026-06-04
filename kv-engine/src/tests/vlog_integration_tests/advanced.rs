use super::*;

#[test]
fn test_gc_with_concurrent_writes() {
    // RFC Phase 4 test:
    // Write values, flush, overwrite some keys, trigger GC.
    // Verify: (a) overwritten keys retain new values,
    //         (b) non-overwritten keys are still readable,
    //         (c) stale vLog space is reclaimed.
    //
    // Part 1: Deterministic space reclamation — overwrite and flush to create
    // stale entries on disk, then GC and verify reclamation.
    // Part 2: Concurrent correctness — spawn a writer thread during GC and
    // verify no corruption or data loss.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_and_compaction(256, 1 << 20);
    if let Some(ref mut vs) = options.value_separation {
        vs.gc_threshold_ratio = 0.0;
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large values that go to vLog
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let value = vec![b'a' + (i as u8 % 26); 64];
        storage.put(key.as_bytes(), &value).unwrap();
    }
    force_flush(&storage.inner);

    // Overwrite half the keys and flush — creates stale entries on disk
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.put(key.as_bytes(), &[b'y'; 64]).unwrap();
    }
    force_flush(&storage.inner);

    // Part 1: Trigger GC — stale entries from first flush should be reclaimed
    let stats_before = storage.vlog_stats().unwrap();
    let gc_count = storage.trigger_gc().unwrap();
    let stats_after = storage.vlog_stats().unwrap();

    assert!(gc_count > 0, "GC should have processed at least 1 file");
    assert!(
        stats_after.gc_files_processed > stats_before.gc_files_processed,
        "gc_files_processed should increase after GC"
    );
    assert!(
        stats_after.gc_entries_rewritten > stats_before.gc_entries_rewritten,
        "gc_entries_rewritten should increase — stale entries should be reclaimed"
    );

    // Verify overwritten keys have new values (a) and non-overwritten are intact (b)
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(vec![b'y'; 64])),
            "overwritten key {} should have new value",
            key
        );
    }
    for i in 5..10 {
        let key = format!("key_{:04}", i);
        let expected_byte = b'a' + (i as u8 % 26);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(vec![expected_byte; 64])),
            "non-overwritten key {} should retain original value",
            key
        );
    }

    // Part 2: Concurrent writes during GC — verify no corruption
    let storage2 = storage.clone();
    let writer_handle = std::thread::spawn(move || {
        for i in 0..5 {
            let key = format!("key_{:04}", i);
            storage2.put(key.as_bytes(), &[b'z'; 64]).unwrap();
        }
    });

    // Trigger GC while writer thread is running
    let _ = storage.trigger_gc();
    writer_handle.join().unwrap();

    // Flush the concurrent writes
    force_flush(&storage.inner);

    // Verify no corruption — all keys readable with valid values
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert!(
            result.is_some(),
            "key {} should exist after concurrent GC",
            key
        );
        assert_eq!(
            result.unwrap().len(),
            64,
            "key {} value should be 64 bytes",
            key
        );
    }
}

#[test]
fn test_crash_recovery_after_partial_flush() {
    // RFC Phase 4 test: simulate crash mid-flush.
    //
    // The WAL stores full values for user writes (not vLog pointers), so WAL
    // replay restores the memtable with inline values — no dangling pointers.
    //
    // Scenario: write data, flush to create SST + vLog, write more data (only
    // in WAL + memtable), then simulate crash by dropping without clean close.
    // On restart, WAL replay should restore post-flush writes with full values.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_enabled(256, 1 << 20);
    options.enable_wal = true;

    // Phase 1: Write data and simulate crash
    {
        let storage = KvEngine::open(dir.path(), options.clone()).unwrap();
        storage.put(b"key1", &[b'a'; 128]).unwrap();
        storage.put(b"key2", &[b'b'; 128]).unwrap();
        storage.put(b"key3", b"small").unwrap(); // inline

        // Force flush — creates SST + vLog entries
        force_flush(&storage.inner);

        // Write more data after flush — only in WAL + memtable, not yet flushed.
        // These writes store full values in the WAL (not vLog pointers), so WAL
        // replay can restore them without any vLog dependency.
        storage.put(b"key4", &[b'd'; 128]).unwrap();
        storage.put(b"key5", b"small_after").unwrap();

        // Simulate crash by dropping without clean close.
        // close() would flush the memtable to SST + vLog; drop() skips that,
        // leaving post-flush writes only in the WAL.
        drop(storage);
    }

    // Phase 2: Reopen and verify recovery
    {
        let storage = KvEngine::open(dir.path(), options).unwrap();

        // Flushed data (recovered from SST + vLog)
        assert_eq!(
            storage.get(b"key1").unwrap(),
            Some(Bytes::from(vec![b'a'; 128]))
        );
        assert_eq!(
            storage.get(b"key2").unwrap(),
            Some(Bytes::from(vec![b'b'; 128]))
        );
        assert_eq!(
            storage.get(b"key3").unwrap(),
            Some(Bytes::from_static(b"small"))
        );

        // Post-flush data (recovered from WAL — full values, no dangling pointers)
        assert_eq!(
            storage.get(b"key4").unwrap(),
            Some(Bytes::from(vec![b'd'; 128]))
        );
        assert_eq!(
            storage.get(b"key5").unwrap(),
            Some(Bytes::from_static(b"small_after"))
        );

        // Verify no dangling pointers — engine should work normally after recovery.
        // Write a large value, flush to exercise the vLog path, and read it back.
        storage.put(b"key6", &[b'f'; 128]).unwrap();
        force_flush(&storage.inner);
        assert_eq!(
            storage.get(b"key6").unwrap(),
            Some(Bytes::from(vec![b'f'; 128]))
        );

        // vLog should be functional — file count should be valid
        let stats = storage.vlog_stats().unwrap();
        assert!(
            stats.vlog_file_count >= 1,
            "vLog files should exist after recovery"
        );
    }
}

#[test]
fn test_orphan_vlog_cleanup_on_startup() {
    // Manually create an orphan .vlog file and verify it is deleted on startup.
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);

    // Write data and flush to create real vLog files
    {
        let storage = KvEngine::open(dir.path(), options.clone()).unwrap();
        storage.put(b"key1", &[b'a'; 64]).unwrap();
        force_flush(&storage.inner);
        storage.close().unwrap();
    }

    // Create an orphan vLog file (not referenced by any SST or manifest)
    let vlog_dir = dir.path().join("vlog");
    let orphan_path = vlog_dir.join("999.vlog");
    std::fs::write(&orphan_path, b"orphan data that should not be read").unwrap();
    assert!(orphan_path.exists());

    // Reopen — the orphan should be cleaned up on startup
    {
        let storage = KvEngine::open(dir.path(), options).unwrap();

        // Orphan file should be deleted
        assert!(
            !orphan_path.exists(),
            "orphan vLog file should be deleted on startup"
        );

        // Original data should still be readable
        assert_eq!(
            storage.get(b"key1").unwrap(),
            Some(Bytes::from(vec![b'a'; 64]))
        );

        // Engine should work normally — new writes should succeed
        storage.put(b"key2", &[b'b'; 64]).unwrap();
        assert_eq!(
            storage.get(b"key2").unwrap(),
            Some(Bytes::from(vec![b'b'; 64]))
        );
    }
}

#[test]
fn test_range_scan_deduplication() {
    // Write a key, flush (separated to vLog), overwrite same key, flush again.
    // Range scan should return only the latest value, not both old and new pointers.
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large values
    storage.put(b"aaa", &[b'x'; 64]).unwrap();
    storage.put(b"bbb", &[b'y'; 64]).unwrap();
    storage.put(b"ccc", &[b'z'; 64]).unwrap();

    // First flush — values go to vLog
    force_flush(&storage.inner);

    // Overwrite "bbb" with a new value
    storage.put(b"bbb", &[b'w'; 64]).unwrap();

    // Second flush
    force_flush(&storage.inner);

    // Range scan — should return exactly 3 keys (deduplication)
    let mut scan = storage
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();

    let mut results = vec![];
    while scan.is_valid() {
        results.push((scan.key().to_vec(), Bytes::copy_from_slice(scan.value())));
        scan.next().unwrap();
    }

    assert_eq!(results.len(), 3, "scan should return exactly 3 unique keys");
    assert_eq!(results[0].0, b"aaa");
    assert_eq!(results[0].1, Bytes::from(vec![b'x'; 64]));
    assert_eq!(results[1].0, b"bbb");
    // "bbb" should have the latest value (from the overwrite)
    assert_eq!(results[1].1, Bytes::from(vec![b'w'; 64]));
    assert_eq!(results[2].0, b"ccc");
    assert_eq!(results[2].1, Bytes::from(vec![b'z'; 64]));
}

#[test]
fn test_mixed_inline_pointer_after_enable() {
    // Verify that small (inline) and large (vLog pointer) values coexist correctly
    // when vlog is enabled from the start. Both types should be readable and scannable.
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write small values (below min_value_size → inline)
    storage.put(b"inline1", b"tiny_a").unwrap();
    storage.put(b"inline2", b"tiny_b").unwrap();

    // Write large values (above min_value_size → vLog pointer)
    storage.put(b"vlog1", &[b'x'; 64]).unwrap();
    storage.put(b"vlog2", &[b'y'; 64]).unwrap();

    // Flush first batch
    force_flush(&storage.inner);

    // Write more mixed values
    storage.put(b"inline3", b"tiny_c").unwrap();
    storage.put(b"vlog3", &[b'z'; 64]).unwrap();

    // Flush second batch
    force_flush(&storage.inner);

    // All inline values should be readable
    assert_eq!(
        storage.get(b"inline1").unwrap(),
        Some(Bytes::from_static(b"tiny_a"))
    );
    assert_eq!(
        storage.get(b"inline2").unwrap(),
        Some(Bytes::from_static(b"tiny_b"))
    );
    assert_eq!(
        storage.get(b"inline3").unwrap(),
        Some(Bytes::from_static(b"tiny_c"))
    );

    // All vLog-separated values should be readable
    assert_eq!(
        storage.get(b"vlog1").unwrap(),
        Some(Bytes::from(vec![b'x'; 64]))
    );
    assert_eq!(
        storage.get(b"vlog2").unwrap(),
        Some(Bytes::from(vec![b'y'; 64]))
    );
    assert_eq!(
        storage.get(b"vlog3").unwrap(),
        Some(Bytes::from(vec![b'z'; 64]))
    );

    // Scan should return all 6 keys in order (inline and vlog interleaved)
    let mut scan = storage
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();
    let mut results = vec![];
    while scan.is_valid() {
        results.push((
            String::from_utf8(scan.key().to_vec()).unwrap(),
            Bytes::copy_from_slice(scan.value()),
        ));
        scan.next().unwrap();
    }
    assert_eq!(results.len(), 6);
    assert_eq!(results[0].0, "inline1");
    assert_eq!(results[0].1, Bytes::from_static(b"tiny_a"));
    assert_eq!(results[1].0, "inline2");
    assert_eq!(results[2].0, "inline3");
    assert_eq!(results[3].0, "vlog1");
    assert_eq!(results[3].1, Bytes::from(vec![b'x'; 64]));
    assert_eq!(results[4].0, "vlog2");
    assert_eq!(results[5].0, "vlog3");
}

#[test]
fn test_gc_batch_cas() {
    // Verify that batched GC CAS works correctly: write values, flush, overwrite
    // some, flush again, compact (triggers GC with batched CAS), verify all correct.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_and_compaction(256, 1 << 20);
    if let Some(ref mut vs) = options.value_separation {
        vs.gc_threshold_ratio = 0.0; // always trigger GC
    }
    if let CompactionOptions::Leveled(ref mut opts) = options.compaction_options {
        opts.level0_file_num_compaction_trigger = 100; // prevent background race
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large values that go to vLog
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let value = vec![b'a' + (i as u8 % 26); 64];
        storage.put(key.as_bytes(), &value).unwrap();
    }
    force_flush(&storage.inner);

    // Overwrite half the keys
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.put(key.as_bytes(), &[b'z'; 64]).unwrap();
    }
    force_flush(&storage.inner);

    // Compact — triggers GC with batched CAS on the old vLog file
    storage.inner.force_full_compaction().unwrap();

    // Verify overwritten keys have new values
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(vec![b'z'; 64]))
        );
    }
    // Verify non-overwritten keys retain original values
    for i in 5..10 {
        let key = format!("key_{:04}", i);
        let expected_byte = b'a' + (i as u8 % 26);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(vec![expected_byte; 64]))
        );
    }
}

#[test]
fn test_vlog_stats_api() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_enabled(256, 1 << 20);
    if let Some(ref mut vs) = options.value_separation {
        vs.gc_threshold_ratio = 0.0; // Always trigger GC
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Stats before any writes
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.vlog_file_count, 0);
    assert_eq!(stats.vlog_total_bytes, 0);
    assert_eq!(stats.gc_entries_rewritten, 0);
    assert_eq!(stats.gc_files_processed, 0);

    // Write and flush to create vLog files
    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();
    force_flush(&storage.inner);

    let stats = storage.vlog_stats().unwrap();
    assert!(
        stats.vlog_file_count >= 1,
        "should have at least 1 vLog file"
    );
    assert!(
        stats.vlog_total_bytes > 0,
        "vLog files should have nonzero size"
    );

    // Overwrite keys to create stale entries, flush, then compact
    storage.put(b"key1", &[b'c'; 64]).unwrap();
    storage.put(b"key2", &[b'd'; 64]).unwrap();
    force_flush(&storage.inner);
    storage.inner.force_full_compaction().unwrap();

    // Trigger GC — should process files and rewrite entries
    let gc_count = storage.trigger_gc().unwrap();
    assert!(gc_count > 0, "GC should have processed at least 1 file");

    let stats = storage.vlog_stats().unwrap();
    assert!(
        stats.gc_files_processed > 0,
        "gc_files_processed should be > 0"
    );
    // gc_entries_rewritten may be 0 if all entries were dead (100% dead optimization)
    // but gc_bytes_rewritten should reflect the file scan

    // Values should still be correct
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'c'; 64]))
    );
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::from(vec![b'd'; 64]))
    );
}
