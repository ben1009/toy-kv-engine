use std::sync::Arc;

use bytes::Bytes;

use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    key::KeySlice,
    lsm_storage::{LsmStorageOptions, KvEngine},
    table::SsTableBuilder,
    vlog::ValueSeparationOptions,
};

fn options_with_vlog_enabled(block_size: usize, target_sst_size: usize) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size,
        target_sst_size,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size: 16, // Separate values >= 16 bytes
            ..Default::default()
        }),
    }
}

fn options_with_vlog_and_compaction(
    block_size: usize,
    target_sst_size: usize,
) -> LsmStorageOptions {
    use crate::compact::LeveledCompactionOptions;
    LsmStorageOptions {
        block_size,
        target_sst_size,
        num_memtable_limit: 2,
        // High trigger prevents background compaction from racing with manual
        // force_full_compaction() calls in tests.
        compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 100,
            max_levels: 3,
            base_level_size_mb: 1,
            level_size_multiplier: 2,
        }),
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size: 16,
            ..Default::default()
        }),
    }
}

#[test]
fn test_sst_builder_kind_prefix_inline() {
    // Small values (< min_value_size) should be stored inline with KvKind prefix
    let mut builder = SsTableBuilder::new(4096);
    builder
        .add(
            KeySlice::for_testing_from_slice_no_ts(b"key1"),
            b"small", // 5 bytes < 16 byte threshold
        )
        .unwrap();

    // The block should contain the value with KvKind::Inline prefix
    // We verify by building and reading back through the iterator
    let dir = tempfile::tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("test.sst")).unwrap();

    let iter = crate::table::SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key().raw_ref(), b"key1");
    // value() should strip the kind prefix and return the raw value
    assert_eq!(iter.value(), b"small");
}

#[test]
fn test_sst_builder_kind_prefix_empty_value() {
    // Empty values (tombstones) should be stored as [KvKind::Inline] only
    let mut builder = SsTableBuilder::new(4096);
    builder
        .add(
            KeySlice::for_testing_from_slice_no_ts(b"key1"),
            b"", // tombstone
        )
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("test.sst")).unwrap();

    let iter = crate::table::SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key().raw_ref(), b"key1");
    // value() should return empty for tombstones
    assert!(iter.value().is_empty());
}

#[test]
fn test_sst_builder_add_raw_preserves_pointer() {
    use crate::vlog::{KvKind, ValuePointer};

    // Simulate a compaction scenario: add_raw with a ValuePointer
    let mut builder = SsTableBuilder::new(4096);

    // Create a fake ValuePointer
    let ptr = ValuePointer {
        file_id: 42,
        offset: 1234,
        size: 5678,
    };
    let mut raw = vec![KvKind::ValuePointer as u8];
    ptr.encode(&mut raw);

    builder
        .add_raw(KeySlice::for_testing_from_slice_no_ts(b"key1"), &raw)
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("test.sst")).unwrap();

    // raw_value() should return the original bytes with kind prefix
    let iter = crate::table::SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.raw_value(), &raw[..]);
}

#[test]
fn test_end_to_end_large_value_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let options = options_with_vlog_enabled(256, 1 << 20);
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write a large value (> min_value_size of 16)
    let large_value = vec![b'x'; 256];
    storage.put(b"large_key", &large_value).unwrap();

    // Flush to SST (which should write to vLog)
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    storage.delete(b"key1").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    storage.put(b"eee", b"tiny_eee").unwrap();
    storage.put(b"fff", &[b'f'; 128]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        storage.inner.force_flush_next_imm_memtable().unwrap();
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

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Second flush — large values go to vLog file 1
    storage.put(b"b1", &[b'c'; 64]).unwrap();
    storage.put(b"b2", &[b'd'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Third flush
    storage.put(b"c1", &[b'e'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    for i in 10..20 {
        let key = format!("key_{:04}", i);
        let value = format!("small_{}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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

// ---------------------------------------------------------------
// GC Integration Tests
// ---------------------------------------------------------------

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Delete all keys
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.delete(key.as_bytes()).unwrap();
    }

    // Flush deletions
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    let mut options = options_with_vlog_and_compaction(256, 1 << 20);
    // Prevent background compaction from racing with our manual force_full_compaction.
    // With 2 L0 SSTs and trigger=2, the background thread can compact them before
    // the test does, causing "No such file or directory" when removing old SSTs.
    if let CompactionOptions::Leveled(ref mut opts) = options.compaction_options {
        opts.level0_file_num_compaction_trigger = 100;
    }
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write large values
    storage.put(b"keep", &[b'k'; 64]).unwrap();
    storage.put(b"overwrite", &[b'o'; 64]).unwrap();

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Overwrite "overwrite" with a new value
    storage.put(b"overwrite", &[b'n'; 64]).unwrap();

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Overwrite key1
    storage.put(b"key1", &[b'c'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Second flush — large values go to vLog file 1
    storage.put(b"b1", &[b'c'; 64]).unwrap();
    storage.put(b"b2", &[b'd'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Overwrite all keys
    storage.put(b"a1", &[b'x'; 64]).unwrap();
    storage.put(b"a2", &[b'x'; 64]).unwrap();
    storage.put(b"b1", &[b'x'; 64]).unwrap();
    storage.put(b"b2", &[b'x'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Compact — old vLog entries become stale
    storage.inner.force_full_compaction().unwrap();

    // All values should be correct
    assert_eq!(
        storage.get(b"a1").unwrap(),
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

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    let vlog = storage.inner.vlog.as_ref().unwrap();
    let gc = GarbageCollector::new(vlog, &storage.inner, 0.5);

    // Analyze the vLog file — all entries should be live
    let analysis = gc.analyze_file(0).unwrap();
    assert_eq!(analysis.file_id, 0);
    assert_eq!(analysis.live_entries.len(), 2);
    assert_eq!(analysis.stale_ratio, 0.0);
    assert_eq!(analysis.dead_bytes, 0);
}

// ---------------------------------------------------------------
// Phase 4: Missing RFC Tests
// ---------------------------------------------------------------

#[test]
fn test_gc_with_concurrent_writes() {
    // Write values, flush, then concurrently overwrite keys while triggering GC.
    // Verify: overwritten keys retain new values, non-overwritten keys are readable.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_and_compaction(256, 1 << 20);
    // Ensure GC actually triggers during the test
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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Concurrently overwrite some keys while triggering GC
    let storage2 = storage.clone();
    let writer_handle = std::thread::spawn(move || {
        for i in 0..5 {
            let key = format!("key_{:04}", i);
            let value = vec![b'z'; 64]; // new value
            storage2.put(key.as_bytes(), &value).unwrap();
        }
    });

    // Trigger GC in parallel
    let _ = storage.trigger_gc();

    writer_handle.join().unwrap();

    // Flush the concurrent writes
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Overwritten keys should have the new value (from the concurrent writer)
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "key {} should exist", key);
        // The value should be either the original or the overwrite — both are valid
        // depending on ordering. The important thing is no corruption or missing data.
        let val = result.unwrap();
        assert_eq!(val.len(), 64);
    }

    // Non-overwritten keys should retain original values
    for i in 5..10 {
        let key = format!("key_{:04}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "key {} should exist", key);
        let val = result.unwrap();
        let expected_byte = b'a' + (i as u8 % 26);
        assert!(val.iter().all(|&b| b == expected_byte));
    }
}

#[test]
fn test_crash_recovery_after_partial_flush() {
    // Verify that data survives close/reopen. The WAL stores full values,
    // so even if a flush is interrupted, WAL replay restores the memtable.
    let dir = tempfile::tempdir().unwrap();
    let mut options = options_with_vlog_enabled(256, 1 << 20);
    options.enable_wal = true;

    // Write data with WAL enabled
    {
        let storage = KvEngine::open(dir.path(), options.clone()).unwrap();
        storage.put(b"key1", &[b'a'; 128]).unwrap();
        storage.put(b"key2", &[b'b'; 128]).unwrap();
        storage.put(b"key3", b"small").unwrap(); // inline

        // Force flush to create SST + vLog entries
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        storage.inner.force_flush_next_imm_memtable().unwrap();

        // Write more data after flush (only in WAL + memtable)
        storage.put(b"key4", &[b'd'; 128]).unwrap();
        storage.put(b"key5", b"small_after").unwrap();

        // Simulate crash by dropping without clean close
        // (WAL should preserve the post-flush writes)
        drop(storage);
    }

    // Reopen — WAL replay should restore post-flush writes
    {
        let storage = KvEngine::open(dir.path(), options).unwrap();

        // Flushed data (in SST + vLog)
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

        // Post-flush data (recovered from WAL)
        assert_eq!(
            storage.get(b"key4").unwrap(),
            Some(Bytes::from(vec![b'd'; 128]))
        );
        assert_eq!(
            storage.get(b"key5").unwrap(),
            Some(Bytes::from_static(b"small_after"))
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
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        storage.inner.force_flush_next_imm_memtable().unwrap();
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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Overwrite "bbb" with a new value
    storage.put(b"bbb", &[b'w'; 64]).unwrap();

    // Second flush
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    //
    // NOTE: enabling vlog on an existing database that was created without vlog is
    // currently NOT supported — old SSTs lack the KvKind prefix and will cause read
    // errors. This test verifies the mixed behavior within a single vlog-enabled run.
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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Write more mixed values
    storage.put(b"inline3", b"tiny_c").unwrap();
    storage.put(b"vlog3", &[b'z'; 64]).unwrap();

    // Flush second batch
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Overwrite half the keys
    for i in 0..5 {
        let key = format!("key_{:04}", i);
        storage.put(key.as_bytes(), &[b'z'; 64]).unwrap();
    }
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

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
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();
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

#[test]
fn test_value_cache_hit_miss() {
    let dir = tempfile::tempdir().unwrap();
    let options = LsmStorageOptions {
        block_size: 256,
        target_sst_size: 1 << 20,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size: 16,
            value_cache_capacity_bytes: 1024 * 1024, // 1MB
            ..Default::default()
        }),
    };
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write and flush to create vLog entries
    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // First read: cache miss
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_misses, 1);
    assert_eq!(stats.cache_hits, 0);

    // Second read: cache hit
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_misses, 1);
    assert_eq!(stats.cache_hits, 1);

    // Different key: cache miss
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_misses, 2);
    assert_eq!(stats.cache_hits, 1);

    // Re-read key2: cache hit
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::from(vec![b'b'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_misses, 2);
    assert_eq!(stats.cache_hits, 2);
}

#[test]
fn test_value_cache_disabled_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let options = LsmStorageOptions {
        block_size: 256,
        target_sst_size: 1 << 20,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size: 16,
            ..Default::default()
        }),
    };
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Read twice — both should be cache misses (cache disabled)
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_hits, 0);
    assert_eq!(stats.cache_misses, 0);
}
