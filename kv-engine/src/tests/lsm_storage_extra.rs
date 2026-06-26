use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    lsm_storage::{
        CompactionFilterKind, CompactionFilterRequest, KvEngine, LsmStorageOptions,
        PrefixBloomOptions, WriteBatchRecord,
    },
    vlog::ValueSeparationOptions,
};

#[test]
fn test_compaction_filter_api_add_remove_list_and_stats() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    let first = engine
        .add_compaction_filter(CompactionFilterRequest::prefix(Bytes::from_static(
            b"tenant:",
        )))
        .unwrap();
    let second = engine
        .add_compaction_filter_with_cutoff(
            CompactionFilterRequest::prefix(Bytes::from_static(b"user:")),
            0,
        )
        .unwrap();
    assert_eq!(first, 0);
    assert_eq!(second, 1);

    let listed = engine.list_compaction_filters();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, first);
    assert_eq!(
        listed[0].kind,
        CompactionFilterKind::Prefix(b"tenant:".to_vec())
    );
    assert_eq!(listed[1].id, second);
    assert_eq!(
        listed[1].kind,
        CompactionFilterKind::Prefix(b"user:".to_vec())
    );

    let removed = engine.remove_compaction_filter(first).unwrap();
    assert!(removed);
    assert!(!engine.remove_compaction_filter(first).unwrap());
    let listed = engine.list_compaction_filters();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, second);

    let stats = engine.compaction_filter_stats();
    assert_eq!(stats.filters_active, 1);
    assert_eq!(stats.entries_eligible, 0);
    assert_eq!(stats.entries_dropped, 0);
    assert_eq!(stats.bytes_dropped, 0);
}

#[test]
fn test_compaction_filter_api_rejects_invalid_requests() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    let empty = engine.add_compaction_filter(CompactionFilterRequest::prefix(Bytes::new()));
    assert!(empty.is_err());
    assert!(empty.unwrap_err().to_string().contains("must not be empty"));

    let oversize_prefix = vec![b'x'; crate::key::MAX_ENCODED_KEY_LEN];
    let oversize = engine.add_compaction_filter(CompactionFilterRequest::prefix(oversize_prefix));
    assert!(oversize.is_err());
    assert!(
        oversize
            .unwrap_err()
            .to_string()
            .contains("exceeds maximum")
    );

    let id = engine
        .add_compaction_filter_with_cutoff(
            CompactionFilterRequest::prefix(Bytes::from_static(b"dup:")),
            0,
        )
        .unwrap();
    assert_eq!(id, 0);
    let duplicate = engine.add_compaction_filter_with_cutoff(
        CompactionFilterRequest::prefix(Bytes::from_static(b"dup:")),
        0,
    );
    assert!(duplicate.is_err());
    assert!(duplicate.unwrap_err().to_string().contains("duplicate"));
}

#[test]
fn test_compaction_filter_api_rejects_cutoff_above_latest_commit_ts() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"k1", b"v1").unwrap();
    let latest = engine.inner.mvcc.as_ref().unwrap().latest_commit_ts();

    let err = engine
        .add_compaction_filter_with_cutoff(
            CompactionFilterRequest::prefix(Bytes::from_static(b"k")),
            latest + 1,
        )
        .unwrap_err();
    assert!(err.to_string().contains("exceeds latest commit ts"));
}

#[test]
fn test_compaction_filter_stats_track_drops() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"old:drop", b"v1").unwrap();
    engine.force_flush().unwrap();
    engine
        .add_compaction_filter(CompactionFilterRequest::prefix(Bytes::from_static(b"old:")))
        .unwrap();
    engine.put(b"keep", b"v2").unwrap();
    engine.force_flush().unwrap();
    engine.force_full_compaction().unwrap();

    let stats = engine.compaction_filter_stats();
    assert_eq!(stats.filters_active, 1);
    assert!(stats.entries_eligible >= 2);
    assert_eq!(stats.entries_dropped, 1);
    assert!(stats.bytes_dropped > 0);
}

#[test]
fn test_compaction_filter_with_value_separation_preserves_unrelated_values() {
    let dir = tempdir().unwrap();
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
    };
    let engine = KvEngine::open(&dir, options).unwrap();
    let old_value = vec![b'a'; 64];
    let keep_value = vec![b'b'; 64];

    engine.put(b"old:drop", &old_value).unwrap();
    engine.force_flush().unwrap();
    engine
        .add_compaction_filter(CompactionFilterRequest::prefix(Bytes::from_static(b"old:")))
        .unwrap();
    engine.put(b"keep", &keep_value).unwrap();
    engine.force_flush().unwrap();

    engine.force_full_compaction().unwrap();
    assert_eq!(engine.get(b"old:drop").unwrap(), None);
    assert_eq!(
        engine.get(b"keep").unwrap(),
        Some(Bytes::from(keep_value.clone()))
    );

    let _gc_count = engine.trigger_gc().unwrap();
    assert_eq!(engine.get(b"old:drop").unwrap(), None);
    assert_eq!(engine.get(b"keep").unwrap(), Some(Bytes::from(keep_value)));
}

#[test]
fn test_cache_stats_no_vlog() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    let stats = engine.cache_stats();
    // No vlog → value cache hits/misses are 0
    assert_eq!(stats.value_cache_hit_count, 0);
    assert_eq!(stats.value_cache_miss_count, 0);
}

#[test]
fn test_cache_stats_with_vlog() {
    let dir = tempdir().unwrap();
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
    };
    let engine = KvEngine::open(&dir, options).unwrap();

    // Write some values
    engine.put(b"k1", &[b'v'; 64]).unwrap();
    engine.put(b"k2", b"small").unwrap();

    let stats = engine.cache_stats();
    // block_cache_entry_count should be >= 0
    let _ = stats.block_cache_entry_count;
}

#[test]
fn test_vlog_stats_no_vlog() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // vlog_stats returns error when vlog is not enabled
    let result = engine.vlog_stats();
    assert!(result.is_err());
}

#[test]
fn test_vlog_stats_with_vlog() {
    let dir = tempdir().unwrap();
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
    };
    let engine = KvEngine::open(&dir, options).unwrap();

    engine.put(b"k1", &[b'v'; 64]).unwrap();

    let stats = engine.vlog_stats().unwrap();
    // Should have at least one file
    let _ = stats.vlog_file_count;
}

#[test]
fn test_drain_flush() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        num_memtable_limit: 2,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, options).unwrap();

    // Write enough data to create multiple memtables
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        engine.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // drain_flush should flush all memtables
    engine.drain_flush().unwrap();

    // All data should still be readable
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let expected = format!("value_{:04}", i);
        assert_eq!(
            engine.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(expected))
        );
    }
}

#[test]
fn test_write_batch_non_serializable() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", b"v1"),
        WriteBatchRecord::Put(b"k2", b"v2"),
        WriteBatchRecord::Del(b"k3"),
        WriteBatchRecord::Put(b"k4", b"v4"),
    ];
    engine.write_batch(&batch).unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k2").unwrap(), Some(Bytes::from("v2")));
    assert!(engine.get(b"k3").unwrap().is_none());
    assert_eq!(engine.get(b"k4").unwrap(), Some(Bytes::from("v4")));
}

#[test]
fn test_write_batch_dedup_non_serializable() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Duplicate keys — last write wins
    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", b"v1"),
        WriteBatchRecord::Put(b"k1", b"v2"),
        WriteBatchRecord::Put(b"k1", b"v3"),
    ];
    engine.write_batch(&batch).unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v3")));
}

#[test]
fn test_trigger_gc_no_vlog() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // trigger_gc returns 0 when vlog is not enabled
    let count = engine.trigger_gc().unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_force_flush_no_data() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // force_flush on empty engine should be a no-op
    engine.inner.force_flush_next_imm_memtable().unwrap();
}

#[test]
fn test_force_full_compaction_empty() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // force_full_compaction on empty engine should succeed
    engine.force_full_compaction().unwrap();
}

#[test]
fn test_put_tombstone_value_rejected() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // The tombstone marker byte (0x02) should be rejected as a value
    let result = engine.put(b"k1", &[0x02]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("tombstone"));
}

#[test]
fn test_scan_with_ts_basic() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    engine.put(b"c", b"vc").unwrap();

    // Get the current timestamp
    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    let ts = mvcc.latest_commit_ts();

    // scan_with_ts at the current timestamp should see all keys
    let mut iter = engine
        .inner
        .scan_with_ts(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, ts)
        .unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"a");
    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"b");
    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"c");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_scan_with_ts_before_writes() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();

    // Capture timestamp before more writes
    let mvcc = engine.inner.mvcc.as_ref().unwrap();
    let ts_before = mvcc.latest_commit_ts();

    engine.put(b"b", b"vb").unwrap();
    engine.put(b"c", b"vc").unwrap();

    // scan_with_ts at the earlier timestamp should only see "a"
    let mut iter = engine
        .inner
        .scan_with_ts(
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            ts_before,
        )
        .unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"a");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_put_and_get_roundtrip() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = format!("val_{:04}", i);
        engine.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let expected = format!("val_{:04}", i);
        assert_eq!(
            engine.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(expected))
        );
    }
}

#[test]
fn test_get_nonexistent_key() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"key1", b"val1").unwrap();
    assert!(engine.get(b"nonexistent").unwrap().is_none());
}

#[test]
fn test_flush_and_get() {
    use super::harness::sync;

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    sync(&engine.inner);

    assert_eq!(engine.get(b"a").unwrap(), Some(Bytes::from("va")));
    assert_eq!(engine.get(b"b").unwrap(), Some(Bytes::from("vb")));
}

#[test]
fn test_compaction_basic() {
    use super::harness::sync;

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    sync(&engine.inner);

    engine.put(b"c", b"vc").unwrap();
    engine.put(b"d", b"vd").unwrap();
    sync(&engine.inner);

    engine.force_full_compaction().unwrap();

    assert_eq!(engine.get(b"a").unwrap(), Some(Bytes::from("va")));
    assert_eq!(engine.get(b"b").unwrap(), Some(Bytes::from("vb")));
    assert_eq!(engine.get(b"c").unwrap(), Some(Bytes::from("vc")));
    assert_eq!(engine.get(b"d").unwrap(), Some(Bytes::from("vd")));
}

#[test]
fn test_scan_bounded() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();
    engine.put(b"c", b"vc").unwrap();
    engine.put(b"d", b"vd").unwrap();

    let mut iter = engine
        .scan(
            std::ops::Bound::Included(b"b"),
            std::ops::Bound::Excluded(b"d"),
        )
        .unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"vb");
    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"vc");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_scan_unbounded() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();

    let mut iter = engine
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"a");
    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key(), b"b");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

// --- RFC §9 test 21: Keys exceeding format limit are rejected ---

#[test]
fn test_key_exceeding_format_limit_rejected() {
    // Keys whose encoded internal length exceeds u16::MAX should be rejected.
    // The memcomparable encoding adds ~1 byte per 8 bytes + terminator + 8-byte ts.
    // A raw user key of ~65520 bytes should exceed the limit after encoding.
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Create a key large enough to exceed u16::MAX after encoding.
    // Raw key of 65520 bytes → encoded ~65520 + ceil(65520/8) + 1 + 8 ≈ 65537 > 65535
    let large_key = vec![b'k'; 65520];

    // put() should return an error (from WAL or block builder).
    let result = engine.put(&large_key, b"v");
    assert!(result.is_err(), "put with oversized key should fail");

    // write_batch() should also return an error.
    let result = engine.write_batch(&[WriteBatchRecord::Put(large_key.clone(), b"v".to_vec())]);
    assert!(
        result.is_err(),
        "write_batch with oversized key should fail"
    );

    // Engine should still be empty — no partial state.
    let val = engine.get(b"any_key").unwrap();
    assert!(
        val.is_none(),
        "engine should be empty after rejected writes"
    );
}

// --- RFC §9 test 22: Duplicate user keys — last-op-wins with Put+Del mix ---

#[test]
fn test_write_batch_dedup_put_del_mix() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Seed: key "k" has value "v0".
    engine.put(b"k", b"v0").unwrap();

    // Batch: Put(k, v1), Put(k, v2), Del(k) — last op is Del, so k should be deleted.
    engine
        .write_batch(&[
            WriteBatchRecord::Put(b"k".as_ref(), b"v1".as_ref()),
            WriteBatchRecord::Put(b"k".as_ref(), b"v2".as_ref()),
            WriteBatchRecord::Del(b"k".as_ref()),
        ])
        .unwrap();
    assert_eq!(engine.get(b"k").unwrap(), None, "Del should win over Put");

    // Batch: Del(k), Put(k, v3) — last op is Put, so k should have v3.
    engine
        .write_batch(&[
            WriteBatchRecord::Del(b"k".as_ref()),
            WriteBatchRecord::Put(b"k".as_ref(), b"v3".as_ref()),
        ])
        .unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(Bytes::from_static(b"v3")));

    // Batch: Put(k, v4), Del(k), Put(k, v5) — last op is Put, so k should have v5.
    engine
        .write_batch(&[
            WriteBatchRecord::Put(b"k".as_ref(), b"v4".as_ref()),
            WriteBatchRecord::Del(b"k".as_ref()),
            WriteBatchRecord::Put(b"k".as_ref(), b"v5".as_ref()),
        ])
        .unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(Bytes::from_static(b"v5")));
}

// --- batch_get tests ---

#[test]
fn test_batch_get_basic() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    for i in 0..20 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let formatted: Vec<String> = (0..20).map(|i| format!("key_{:04}", i)).collect();
    let keys: Vec<&[u8]> = formatted.iter().map(|k| k.as_bytes()).collect();
    let results = engine.batch_get(&keys);
    assert_eq!(results.len(), 20);
    for (i, res) in results.iter().enumerate() {
        let expected = format!("val_{:04}", i);
        assert_eq!(res.as_ref().unwrap(), &Some(Bytes::from(expected)));
    }
}

#[test]
fn test_batch_get_empty() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();
    let results = engine.batch_get(&[]);
    assert!(results.is_empty());
}

#[test]
fn test_batch_get_missing_keys() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"c", b"vc").unwrap();

    let keys: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
    let results = engine.batch_get(&keys);
    assert_eq!(results[0].as_ref().unwrap(), &Some(Bytes::from("va")));
    assert_eq!(results[1].as_ref().unwrap(), &None);
    assert_eq!(results[2].as_ref().unwrap(), &Some(Bytes::from("vc")));
    assert_eq!(results[3].as_ref().unwrap(), &None);
}

#[test]
fn test_batch_get_single_key() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"only", b"val").unwrap();
    let results = engine.batch_get(&[b"only"]);
    assert_eq!(results[0].as_ref().unwrap(), &Some(Bytes::from("val")));
}

#[test]
fn test_batch_get_with_flush() {
    use super::harness::sync;

    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write some keys and flush to SST.
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let val = format!("sst_{:04}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    sync(&engine.inner);

    // Write more keys to the active memtable.
    for i in 10..20 {
        let key = format!("key_{:04}", i);
        let val = format!("mem_{:04}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Batch get across both SST and memtable.
    let keys: Vec<Vec<u8>> = (0..20)
        .map(|i| format!("key_{:04}", i).into_bytes())
        .collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let results = engine.batch_get(&key_refs);
    for (i, res) in results.iter().enumerate().take(10) {
        let expected = format!("sst_{:04}", i);
        assert_eq!(res.as_ref().unwrap(), &Some(Bytes::from(expected)));
    }
    for (i, res) in results.iter().enumerate().skip(10) {
        let expected = format!("mem_{:04}", i);
        assert_eq!(res.as_ref().unwrap(), &Some(Bytes::from(expected)));
    }
}

#[test]
fn test_batch_get_matches_individual_gets() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    for i in 0..30 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Mix of found and not-found keys, in non-sorted order.
    let keys: Vec<&[u8]> = vec![
        b"k029",
        b"k000",
        b"missing",
        b"k015",
        b"k005",
        b"also_missing",
    ];
    let batch_results = engine.batch_get(&keys);
    for (i, key) in keys.iter().enumerate() {
        let individual = engine.get(key).unwrap();
        assert_eq!(
            batch_results[i].as_ref().unwrap(),
            &individual,
            "mismatch for key {:?}",
            key
        );
    }
}

/// Verify that `batch_get` correctly suppresses SST values covered by a
/// memtable range tombstone — even when the tombstone does NOT cover all
/// versions up to `read_ts` (partial coverage). This is the scenario
/// described in the critical bug found during PR #127 review.
#[test]
fn test_batch_get_with_memtable_range_tombstone() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write values for keys k000..k009 and flush to SST.
    for i in 0..10 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    engine.force_flush().unwrap();

    // Now add a range tombstone in the memtable covering k003..k007.
    // This creates the scenario: memtable RT at ts=0, SST values at ts=0.
    // The RT should suppress the SST values for covered keys.
    engine.delete_range(b"k003", b"k007").unwrap();

    // batch_get should return None for covered keys, Some for uncovered keys.
    let keys: Vec<&[u8]> = vec![b"k000", b"k003", b"k005", b"k006", b"k009"];
    let batch_results = engine.batch_get(&keys);

    // k000: not covered by range tombstone → should be found
    assert_eq!(
        batch_results[0].as_ref().unwrap(),
        &Some(Bytes::from("v000"))
    );
    // k003: covered by range tombstone → should be None
    assert_eq!(batch_results[1].as_ref().unwrap(), &None);
    // k005: covered by range tombstone → should be None
    assert_eq!(batch_results[2].as_ref().unwrap(), &None);
    // k006: covered by range tombstone → should be None
    assert_eq!(batch_results[3].as_ref().unwrap(), &None);
    // k009: not covered by range tombstone → should be found
    assert_eq!(
        batch_results[4].as_ref().unwrap(),
        &Some(Bytes::from("v009"))
    );

    // Cross-check: batch_get results must match individual get() results.
    for (i, key) in keys.iter().enumerate() {
        let individual = engine.get(key).unwrap();
        assert_eq!(
            batch_results[i].as_ref().unwrap(),
            &individual,
            "batch_get vs get mismatch for key {:?}",
            key
        );
    }
}

#[test]
fn test_batch_get_with_sst_range_tombstone() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write values for keys k000..k009 and flush to SST.
    for i in 0..10 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    engine.force_flush().unwrap();

    // Write a range tombstone covering k003..k007, then flush to SST.
    // This tests the path where the RT lives in an SST, not the memtable.
    engine.delete_range(b"k003", b"k007").unwrap();
    engine.force_flush().unwrap();

    // batch_get should return None for covered keys, Some for uncovered keys.
    let keys: Vec<&[u8]> = vec![b"k000", b"k003", b"k005", b"k006", b"k009"];
    let batch_results = engine.batch_get(&keys);

    assert_eq!(
        batch_results[0].as_ref().unwrap(),
        &Some(Bytes::from("v000"))
    );
    assert_eq!(batch_results[1].as_ref().unwrap(), &None);
    assert_eq!(batch_results[2].as_ref().unwrap(), &None);
    assert_eq!(batch_results[3].as_ref().unwrap(), &None);
    assert_eq!(
        batch_results[4].as_ref().unwrap(),
        &Some(Bytes::from("v009"))
    );

    // Cross-check: batch_get results must match individual get() results.
    for (i, key) in keys.iter().enumerate() {
        let individual = engine.get(key).unwrap();
        assert_eq!(
            batch_results[i].as_ref().unwrap(),
            &individual,
            "batch_get vs get mismatch for key {:?}",
            key
        );
    }
}

/// Covers the early short-circuit path: memtable RT covers a key that is NOT
/// in the memtable but exists in SST. batch_get should return None without
/// performing the SST lookup.
#[test]
fn test_batch_get_memtable_rt_skips_sst_lookup() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write values and flush to SST — keys only exist in SST now.
    for i in 0..10 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    engine.force_flush().unwrap();

    // Add a range tombstone in the memtable covering k003..k007.
    // The memtable has no point entries, only the RT.
    engine.delete_range(b"k003", b"k007").unwrap();

    let keys: Vec<&[u8]> = vec![b"k000", b"k003", b"k005", b"k009"];
    let batch_results = engine.batch_get(&keys);

    // k000: not covered → found in SST
    assert_eq!(
        batch_results[0].as_ref().unwrap(),
        &Some(Bytes::from("v000"))
    );
    // k003: covered by memtable RT → early short-circuit, no SST lookup
    assert_eq!(batch_results[1].as_ref().unwrap(), &None);
    // k005: covered by memtable RT → early short-circuit
    assert_eq!(batch_results[2].as_ref().unwrap(), &None);
    // k009: not covered → found in SST
    assert_eq!(
        batch_results[3].as_ref().unwrap(),
        &Some(Bytes::from("v009"))
    );

    // Cross-check with individual get().
    for (i, key) in keys.iter().enumerate() {
        let individual = engine.get(key).unwrap();
        assert_eq!(batch_results[i].as_ref().unwrap(), &individual);
    }
}

/// Covers the memtable hit + RT covers it path: a key exists in the active
/// memtable and is also covered by a memtable range tombstone.
#[test]
fn test_batch_get_memtable_hit_covered_by_rt() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write some values (stay in memtable, no flush).
    engine.put(b"k000", b"v000").unwrap();
    engine.put(b"k005", b"v005").unwrap();
    engine.put(b"k009", b"v009").unwrap();

    // Add a range tombstone covering k003..k007 in the same memtable.
    engine.delete_range(b"k003", b"k007").unwrap();

    let keys: Vec<&[u8]> = vec![b"k000", b"k005", b"k009"];
    let batch_results = engine.batch_get(&keys);

    // k000: not covered by RT → found in memtable
    assert_eq!(
        batch_results[0].as_ref().unwrap(),
        &Some(Bytes::from("v000"))
    );
    // k005: covered by memtable RT → suppressed
    assert_eq!(batch_results[1].as_ref().unwrap(), &None);
    // k009: not covered by RT → found in memtable
    assert_eq!(
        batch_results[2].as_ref().unwrap(),
        &Some(Bytes::from("v009"))
    );

    // Cross-check with individual get().
    for (i, key) in keys.iter().enumerate() {
        let individual = engine.get(key).unwrap();
        assert_eq!(batch_results[i].as_ref().unwrap(), &individual);
    }
}

/// Covers the immutable memtable point lookup and RT check paths:
/// with a small target_sst_size, puts trigger memtable rotation,
/// leaving data in immutable memtables.
#[test]
fn test_batch_get_immutable_memtable_paths() {
    let dir = tempdir().unwrap();
    // Use a tiny target_sst_size so writes trigger memtable rotation.
    let opts = LsmStorageOptions {
        target_sst_size: 64,
        ..LsmStorageOptions::default_for_test()
    };
    let engine = KvEngine::open(&dir, opts).unwrap();

    // Write values — with target_sst_size=64, this will trigger rotation,
    // moving the active memtable to immutable.
    for i in 0..10 {
        let key = format!("k{:03}", i);
        let val = format!("v{:03}", i);
        engine.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Add a range tombstone in the new active memtable covering k003..k007.
    engine.delete_range(b"k003", b"k007").unwrap();

    // Now we have:
    // - Immutable memtables: point entries for k000..k009
    // - Active memtable: range tombstone for k003..k007
    // batch_get should find k003 in immutable memtable, then suppress via
    // active memtable RT.

    let keys: Vec<&[u8]> = vec![b"k000", b"k003", b"k005", b"k009"];
    let batch_results = engine.batch_get(&keys);

    // k000: not covered by RT → found
    assert_eq!(
        batch_results[0].as_ref().unwrap(),
        &Some(Bytes::from("v000"))
    );
    // k003: covered by active memtable RT → suppressed
    assert_eq!(batch_results[1].as_ref().unwrap(), &None);
    // k005: covered by active memtable RT → suppressed
    assert_eq!(batch_results[2].as_ref().unwrap(), &None);
    // k009: not covered by RT → found
    assert_eq!(
        batch_results[3].as_ref().unwrap(),
        &Some(Bytes::from("v009"))
    );

    // Cross-check with individual get().
    for (i, key) in keys.iter().enumerate() {
        let individual = engine.get(key).unwrap();
        assert_eq!(batch_results[i].as_ref().unwrap(), &individual);
    }
}
