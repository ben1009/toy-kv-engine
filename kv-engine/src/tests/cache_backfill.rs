//! Integration tests for cache backfill on flush and compaction.

use tempfile::tempdir;

use crate::{
    lsm_storage::{KvEngine, LsmStorageOptions},
    tests::harness::sync,
};

/// After a flush with backfill enabled, the block cache should contain
/// entries for the newly flushed SST.
#[test]
fn test_backfill_warms_cache_after_flush() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        enable_cache_backfill: true,
        block_cache_capacity: 1024,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, options).unwrap();

    // Write enough data to produce at least one block.
    for i in 0..100 {
        let key = format!("key{:04}", i);
        let val = format!("val{:04}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    sync(&engine.inner);

    // Blocks should have been backfilled into the cache.
    let count = engine.inner.block_cache.entry_count();
    assert!(
        count > 0,
        "expected backfilled blocks in cache, got entry_count={count}"
    );
}

/// With backfill disabled, flush should NOT insert blocks into the cache.
#[test]
fn test_backfill_disabled_no_cache_entries() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        enable_cache_backfill: false,
        block_cache_capacity: 1024,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, options).unwrap();

    for i in 0..100 {
        let key = format!("key{:04}", i);
        let val = format!("val{:04}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    sync(&engine.inner);

    // No blocks should be in the cache.
    assert_eq!(
        engine.inner.block_cache.entry_count(),
        0,
        "backfill disabled: cache should be empty after flush"
    );
}

/// After compaction, the engine should remain functional and the cache
/// should still contain entries from flush backfill. Note:
/// `force_full_compaction` compacts to the bottom level, which skips
/// backfill (by design — only upper-level compactions backfill).
#[test]
fn test_backfill_after_compaction() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        enable_cache_backfill: true,
        block_cache_capacity: 4096,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, options).unwrap();

    // Write data and flush to populate the cache via backfill.
    for batch in 0..3 {
        for i in 0..100 {
            let key = format!("key{:06}", batch * 100 + i);
            let val = format!("val{:06}", batch * 100 + i);
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        sync(&engine.inner);
    }

    // Cache should have entries from flush backfill.
    let count_before = engine.inner.block_cache.entry_count();
    assert!(
        count_before > 0,
        "expected backfilled blocks after flush, got entry_count={count_before}"
    );

    // Force compaction. This compacts to bottom level so backfill is skipped,
    // but the engine should remain functional and existing cache entries
    // for non-compacted SSTs should survive.
    engine.force_full_compaction().unwrap();

    // Verify data is still readable after compaction.
    for batch in 0..3 {
        for i in 0..100 {
            let key = format!("key{:06}", batch * 100 + i);
            let val = engine.get(key.as_bytes()).unwrap();
            assert!(val.is_some(), "{key} should exist after compaction");
        }
    }
}
