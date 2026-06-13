use super::*;

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
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1024,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        enable_prefetch: true,
        prefetch_block_threshold: 4,
        prefetch_vlog_depth: 3,
        prefetch_pool_threads: 2,
    };
    let storage = KvEngine::open(dir.path(), options).unwrap();

    // Write and flush to create vLog entries
    storage.put(b"key1", &[b'a'; 64]).unwrap();
    storage.put(b"key2", &[b'b'; 64]).unwrap();
    force_flush(&storage.inner);

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
            value_cache_capacity_bytes: 0, // explicitly disabled
            ..Default::default()
        }),
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1024,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        enable_prefetch: true,
        prefetch_block_threshold: 4,
        prefetch_vlog_depth: 3,
        prefetch_pool_threads: 2,
    };
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    force_flush(&storage.inner);

    // Read twice — cache is disabled, so neither hits nor misses are recorded
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

#[test]
fn test_value_cache_enabled_by_default() {
    let dir = tempfile::tempdir().unwrap();
    // Use default ValueSeparationOptions — value_cache_capacity_bytes defaults to 64MB
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
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1024,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        enable_prefetch: true,
        prefetch_block_threshold: 4,
        prefetch_vlog_depth: 3,
        prefetch_pool_threads: 2,
    };
    let storage = KvEngine::open(dir.path(), options).unwrap();

    storage.put(b"key1", &[b'a'; 64]).unwrap();
    force_flush(&storage.inner);

    // First read: cache miss
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_misses, 1);
    assert_eq!(stats.cache_hits, 0);

    // Second read: cache hit (default 64MB cache is active)
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::from(vec![b'a'; 64]))
    );
    let stats = storage.vlog_stats().unwrap();
    assert_eq!(stats.cache_misses, 1);
    assert_eq!(stats.cache_hits, 1);
}
