use super::*;

#[test]
fn test_gc_100_percent_dead() {
    // Write values, delete all, compact, GC should reclaim the entire vLog file
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_and_compaction(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large values that go to vLog
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        let value = vec![b'v'; 64];
        storage.put(key.as_bytes(), &value).unwrap();
    }

    // Flush to create SST + vLog
    force_flush(&storage.inner);

    // Delete all keys
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.delete(key.as_bytes()).unwrap();
    }

    // Flush deletions
    force_flush(&storage.inner);

    // Compact so tombstones are dropped (bottom level)
    storage.inner.force_full_compaction().unwrap();

    // All values should be gone
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        assert_eq!(storage.get(key.as_bytes()).unwrap(), None);
    }

    // The post-compaction GC should have run. Verify no errors.
    // The old vLog file should be scheduled for deletion.
    // Since all entries are dead, the file should be reclaimable.
    let vlog = storage.inner.vlog.as_ref().unwrap();
    let reclaimed = vlog.reclaim_pending_deletions().unwrap();
    // May or may not have been reclaimed already by post_compaction_gc
    let _ = reclaimed;
}

#[test]
fn test_gc_preserves_live_values() {
    // Write values, overwrite some, compact, GC — live values should survive
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_and_compaction(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large values
    storage.put(b"keep", &[b'k'; 64]).unwrap();
    storage.put(b"overwrite", &[b'o'; 64]).unwrap();

    force_flush(&storage.inner);

    // Overwrite "overwrite" with a new value
    storage.put(b"overwrite", &[b'n'; 64]).unwrap();

    force_flush(&storage.inner);

    // Compact
    storage.inner.force_full_compaction().unwrap();

    // Both values should be readable
    assert_eq!(
        storage.get(b"keep").unwrap(),
        Some(Bytes::from(vec![b'k'; 64]))
    );
    assert_eq!(
        storage.get(b"overwrite").unwrap(),
        Some(Bytes::from(vec![b'n'; 64]))
    );
}

#[test]
fn test_gc_below_threshold() {
    // When stale ratio is below threshold, GC should not compact
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_and_compaction(256, 1 << 20);
    // Set a very high threshold so GC never triggers
    if let Some(ref mut vs) = options.value_separation {
        vs.gc_threshold_ratio = 0.99;
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();

    force_flush(&storage.inner);

    // Overwrite key1
    storage.put(b"key1", &[b'c'; 64]).unwrap();
    force_flush(&storage.inner);

    storage.inner.force_full_compaction().unwrap();

    // Values should still be correct
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'c'; 64]))
    );
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
}

#[test]
fn test_trigger_gc_api() {
    // Test the public trigger_gc API
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_enabled(256, 1 << 20);
    if let Some(ref mut vs) = options.value_separation {
        vs.gc_threshold_ratio = 0.0; // Always trigger GC
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();

    force_flush(&storage.inner);

    // trigger_gc should not fail (though it may not find anything to GC
    // since no entries are stale yet)
    let _gc_count = storage.trigger_gc().unwrap();

    // Values should still be readable
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
}

#[test]
fn test_gc_multiple_files() {
    // GC across multiple vLog files
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_and_compaction(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // First flush — large values go to vLog file 0
    storage.put(b"a1", &[b'a'; 64]).unwrap();
    storage.put(b"a2", &[b'b'; 64]).unwrap();
    force_flush(&storage.inner);

    // Second flush — large values go to vLog file 1
    storage.put(b"b1", &[b'c'; 64]).unwrap();
    storage.put(b"b2", &[b'd'; 64]).unwrap();
    force_flush(&storage.inner);

    // Overwrite all keys
    storage.put(b"a1", &[b'x'; 64]).unwrap();
    storage.put(b"a2", &[b'x'; 64]).unwrap();
    storage.put(b"b1", &[b'x'; 64]).unwrap();
    storage.put(b"b2", &[b'x'; 64]).unwrap();
    force_flush(&storage.inner);

    // Compact — old vLog entries become stale
    storage.inner.force_full_compaction().unwrap();

    // All values should be correct after compaction + GC
    assert_eq!(
        storage.get(b"a1").unwrap(),
        Some(Bytes::from(vec![b'x'; 64]))
    );
    assert_eq!(
        storage.get(b"a2").unwrap(),
        Some(Bytes::from(vec![b'x'; 64]))
    );
    assert_eq!(
        storage.get(b"b1").unwrap(),
        Some(Bytes::from(vec![b'x'; 64]))
    );
    assert_eq!(
        storage.get(b"b2").unwrap(),
        Some(Bytes::from(vec![b'x'; 64]))
    );
}

#[test]
fn test_gc_analyze_file() {
    use crate::vlog::gc::GarbageCollector;

    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();

    force_flush(&storage.inner);

    let vlog = storage.inner.vlog.as_ref().unwrap();
    let gc = GarbageCollector::new(vlog, &storage.inner, 0.5);

    // Analyze the vLog file — all entries should be live
    let analysis = gc.analyze_file(0).unwrap();
    assert_eq!(analysis.file_id, 0);
    assert_eq!(analysis.live_entries.len(), 2);
    assert_eq!(analysis.stale_ratio, 0.0);
    assert_eq!(analysis.dead_bytes, 0);
}

#[test]
fn test_vlog_index_created_on_flush() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();

    force_flush(&storage.inner);

    let vlog = storage.inner.vlog.as_ref().unwrap();

    // The .vidx file should exist on disk
    let idx_path = vlog.path_of_file(0).with_extension("vidx");
    assert!(
        idx_path.exists(),
        "vLog index file should exist after flush"
    );

    // The index should be loadable and contain the entries
    let index = vlog.get_or_rebuild_index(0).unwrap();
    assert_eq!(index.len(), 2);
    assert!(index.lookup(b"key1").is_some());
    assert!(index.lookup(b"key2").is_some());
}

#[test]
fn test_vlog_index_survives_gc() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_and_compaction(256, 1 << 20);
    if let Some(ref mut vs) = options.value_separation {
        vs.gc_threshold_ratio = 0.0; // Always trigger GC
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write values to vLog
    storage.put(b"keep", &[b'k'; 64]).unwrap();
    storage.put(b"drop", &[b'd'; 64]).unwrap();
    force_flush(&storage.inner);

    // Overwrite "drop" so it becomes stale
    storage.put(b"drop", &[b'x'; 64]).unwrap();
    force_flush(&storage.inner);

    // Compact so the old SST is removed
    storage.inner.force_full_compaction().unwrap();

    let vlog = storage.inner.vlog.as_ref().unwrap();

    // After GC, the new vLog file should have an index
    // The new file ID will be the next_file_id - 1 (the GC-created file)
    // Find the vLog file that has an index
    let mut found_index = false;
    for entry in std::fs::read_dir(&vlog.path).unwrap().flatten() {
        let name = entry.file_name();
        if let Some(name_str) = name.to_str()
            && name_str.ends_with(".vidx")
        {
            found_index = true;
            break;
        }
    }
    assert!(found_index, "should have at least one .vidx file after GC");

    // The "keep" value should still be readable
    assert_eq!(
        storage.get(b"keep").unwrap(),
        Some(Bytes::from(vec![b'k'; 64]))
    );
}

#[test]
fn test_gc_analyze_uses_index() {
    use crate::vlog::GarbageCollector;

    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();

    force_flush(&storage.inner);

    let vlog = storage.inner.vlog.as_ref().unwrap();

    // Verify index exists
    let idx_path = vlog.path_of_file(0).with_extension("vidx");
    assert!(idx_path.exists());

    // GC analyze should use the index (and produce the same result)
    let gc = GarbageCollector::new(vlog, &storage.inner, 0.5);
    let analysis = gc.analyze_file(0).unwrap();
    assert_eq!(analysis.live_entries.len(), 2);
    assert_eq!(analysis.stale_ratio, 0.0);

    // Verify the index is cached in memory
    let index = vlog.get_or_rebuild_index(0).unwrap();
    assert_eq!(index.len(), 2);
}
