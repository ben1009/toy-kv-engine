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

/// Drop `.sst` files in `dir` from the OS page cache.
#[cfg(unix)]
fn drop_sst_page_cache(dir: &std::path::Path) {
    use std::os::unix::io::AsRawFd;
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sst") {
            if let Ok(file) = std::fs::File::open(&path) {
                unsafe {
                    libc::posix_fadvise(
                        file.as_raw_fd(),
                        0,
                        0,
                        libc::POSIX_FADV_DONTNEED,
                    );
                }
            }
        }
    }
}

/// Performance comparison: compaction backfill on vs off.
/// This test prints timing results to stdout (run with --nocapture).
#[test]
fn test_compaction_backfill_perf_comparison() {
    use std::time::Instant;

    let num_entries = 1000;
    let value_size = 4096;
    let block_cache_capacity = 1024;

    let make_options = |backfill: bool| -> LsmStorageOptions {
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 2,
            compaction_options: crate::compact::CompactionOptions::Leveled(
                crate::compact::LeveledCompactionOptions {
                    level0_file_num_compaction_trigger: 2,
                    max_levels: 3,
                    base_level_size_mb: 1,
                    level_size_multiplier: 2,
                },
            ),
            enable_wal: false,
            serializable: false,
            value_separation: None,
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity,
            enable_cache_backfill: backfill,
        }
    };

    let keys: Vec<Vec<u8>> = (0..num_entries)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect();

    let mut results = Vec::new();

    for (label, backfill) in [("enabled", true), ("disabled", false)] {
        let dir = tempdir().unwrap();
        let options = make_options(backfill);
        let engine = KvEngine::open(dir.path(), options).unwrap();
        let value = vec![0xABu8; value_size];

        // Write data in 4 batches, syncing after each to create L0 SSTs.
        let batch_size = num_entries / 4;
        for batch in 0..4 {
            let start = batch * batch_size;
            let end = start + batch_size;
            for i in start..end {
                engine.put(&keys[i], &value).unwrap();
            }
            // Freeze + flush to create L0 SSTs.
            engine.inner.force_freeze_memtable(&engine.inner.state_lock.lock()).unwrap();
            let _ = engine.inner.force_flush_next_imm_memtable();
        }

        // Wait for background L0→L1 compaction to complete.
        let mut last_l0_count = usize::MAX;
        for _ in 0..200 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let state = engine.inner.state.load_full();
            let l0_count = state.l0_sstables.len();
            if l0_count == last_l0_count && l0_count < 2 {
                break;
            }
            last_l0_count = l0_count;
        }

        // Drop OS page cache so reads are cold.
        drop_sst_page_cache(dir.path());

        // Time the reads.
        let start = Instant::now();
        for i in 0..num_entries {
            let result = engine.get(&keys[i]).unwrap();
            assert!(result.is_some());
        }
        let elapsed = start.elapsed();
        results.push((label, elapsed));

        engine.close().unwrap();
    }

    println!("\n=== Compaction Backfill Performance ===");
    for (label, elapsed) in &results {
        println!("backfill {label}: {elapsed:?} ({} µs/read)",
            elapsed.as_micros() / num_entries as u128);
    }
    let (enabled_us, disabled_us) = (
        results[0].1.as_micros() / num_entries as u128,
        results[1].1.as_micros() / num_entries as u128,
    );
    if disabled_us > enabled_us {
        let speedup = disabled_us as f64 / enabled_us as f64;
        println!("speedup: {speedup:.2}x");
    } else {
        let slowdown = enabled_us as f64 / disabled_us as f64;
        println!("slowdown: {slowdown:.2}x (backfill enabled is slower)");
    }
    println!("=====================================\n");
}
