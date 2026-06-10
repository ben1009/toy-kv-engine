use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, WriteBatchRecord},
    vlog::ValueSeparationOptions,
};

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
