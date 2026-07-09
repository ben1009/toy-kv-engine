use std::sync::Arc;

use bytes::Bytes;
use tempfile::tempdir;

use super::harness::{check_lsm_iter_result_by_key, sync};
use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, PrefixBloomOptions, prefix_upper_bound},
};

// ── Unit tests for prefix_upper_bound ──────────────────────────────────────

#[test]
fn test_prefix_upper_bound_empty() {
    assert_eq!(prefix_upper_bound(b""), None);
}

#[test]
fn test_prefix_upper_bound_single_byte() {
    assert_eq!(prefix_upper_bound(b"a"), Some(vec![b'b']));
}

#[test]
fn test_prefix_upper_bound_two_bytes() {
    assert_eq!(prefix_upper_bound(b"ab"), Some(vec![b'a', b'c']));
}

#[test]
fn test_prefix_upper_bound_trailing_ff() {
    assert_eq!(prefix_upper_bound(b"a\xff"), Some(vec![b'b']));
}

#[test]
fn test_prefix_upper_bound_all_ff() {
    assert_eq!(prefix_upper_bound(b"\xff"), None);
}

#[test]
fn test_prefix_upper_bound_all_ff_multi() {
    assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
}

// ── Integration tests ─────────────────────────────────────────────────────

#[test]
fn test_prefix_scan_basic() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    for key in &[
        b"a".as_ref(),
        b"aa",
        b"ab",
        b"b",
        b"user:1",
        b"user:10",
        b"user:2",
        b"user2:1",
    ] {
        engine.put(key, key).unwrap();
    }

    // prefix "user:" returns only user:1, user:10, user:2 (sorted)
    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("user:1")),
            (Bytes::from("user:10"), Bytes::from("user:10")),
            (Bytes::from("user:2"), Bytes::from("user:2")),
        ],
    );
}

#[test]
fn test_prefix_scan_broader_prefix() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    for key in &[
        b"a".as_ref(),
        b"aa",
        b"ab",
        b"b",
        b"user:1",
        b"user:10",
        b"user:2",
        b"user2:1",
    ] {
        engine.put(key, key).unwrap();
    }

    // prefix "user" returns both user:* and user2:* (sorted by byte order)
    let mut iter = engine.prefix_scan(b"user").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user2:1"), Bytes::from("user2:1")),
            (Bytes::from("user:1"), Bytes::from("user:1")),
            (Bytes::from("user:10"), Bytes::from("user:10")),
            (Bytes::from("user:2"), Bytes::from("user:2")),
        ],
    );
}

#[test]
fn test_prefix_scan_no_match() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();

    let iter = engine.prefix_scan(b"missing").unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_prefix_scan_empty_prefix_is_full_scan() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"a", b"va").unwrap();
    engine.put(b"b", b"vb").unwrap();

    let mut iter = engine.prefix_scan(b"").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("va")),
            (Bytes::from("b"), Bytes::from("vb")),
        ],
    );
}

#[test]
fn test_prefix_scan_ff_suffix() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Keys with a\xff prefix
    engine.put(b"a\xfe", b"v1").unwrap();
    engine.put(b"a\xff", b"v2").unwrap();
    engine.put(b"a\xff\x00", b"v3").unwrap();
    engine.put(b"a\xff\xff", b"v4").unwrap();
    engine.put(b"b", b"v5").unwrap();

    // prefix "a\xff" should match a\xff, a\xff\x00, a\xff\xff but NOT b
    let mut iter = engine.prefix_scan(b"a\xff").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from_static(b"a\xff"), Bytes::from("v2")),
            (Bytes::from_static(b"a\xff\x00"), Bytes::from("v3")),
            (Bytes::from_static(b"a\xff\xff"), Bytes::from("v4")),
        ],
    );
}

#[test]
fn test_prefix_scan_all_ff_prefix() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"\xff", b"v1").unwrap();
    engine.put(b"\xff\x00", b"v2").unwrap();
    engine.put(b"\xff\xff", b"v3").unwrap();
    engine.put(b"other", b"v4").unwrap();

    // prefix "\xff" — upper bound is unbounded, but only \xff* keys should match
    let mut iter = engine.prefix_scan(b"\xff").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from_static(b"\xff"), Bytes::from("v1")),
            (Bytes::from_static(b"\xff\x00"), Bytes::from("v2")),
            (Bytes::from_static(b"\xff\xff"), Bytes::from("v3")),
        ],
    );
}

#[test]
fn test_prefix_scan_with_delete() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();
    engine.put(b"user:3", b"v3").unwrap();
    engine.delete(b"user:2").unwrap();

    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v1")),
            (Bytes::from("user:3"), Bytes::from("v3")),
        ],
    );
}

// ── MVCC tests (§8.3) ─────────────────────────────────────────────────────

#[test]
fn test_prefix_scan_deduplicates_across_versions() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"user:1", b"v0").unwrap();
    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:1", b"v2").unwrap();
    engine.put(b"user:2", b"vx").unwrap();

    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v2")),
            (Bytes::from("user:2"), Bytes::from("vx")),
        ],
    );
}

#[test]
fn test_prefix_scan_tombstone_in_sst() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();
    engine.delete(b"user:2").unwrap();
    engine.put(b"user:3", b"v3").unwrap();

    // Flush everything to SST so the tombstone lives on disk.
    sync(&engine.inner);

    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v1")),
            (Bytes::from("user:3"), Bytes::from("v3")),
        ],
    );
}

#[test]
fn test_prefix_scan_version_collapse_after_flush() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write multiple versions, then flush.
    engine.put(b"k1", b"v0").unwrap();
    engine.put(b"k1", b"v1").unwrap();
    engine.put(b"k1", b"v2").unwrap();
    sync(&engine.inner);

    // Only the latest version should appear.
    let mut iter = engine.prefix_scan(b"k").unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("k1"), Bytes::from("v2"))]);
}

fn serializable_options() -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        serializable: true,
        value_separation: None,
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1024,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        ttl_read_filtering: false,
        ttl_background_scanner_interval: None,
    }
}

#[test]
fn test_prefix_scan_serializable_rejected() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    let result = txn.prefix_scan(b"user:");
    assert!(
        result.is_err(),
        "prefix_scan should be rejected for serializable transactions"
    );
}

#[test]
fn test_prefix_scan_txn_local_writes_merged() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    // Write one key to the engine.
    engine.put(b"user:1", b"from_engine").unwrap();

    // Start a transaction and add local writes.
    let txn = engine.new_txn().unwrap();
    txn.put(b"user:2", b"from_txn").unwrap();

    // prefix_scan should see both engine and local writes.
    let mut iter = txn.prefix_scan(b"user:").unwrap();
    let mut results = Vec::new();
    while iter.is_valid() {
        results.push((
            Bytes::copy_from_slice(iter.key()),
            Bytes::copy_from_slice(iter.value()),
        ));
        iter.next().unwrap();
    }
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        (Bytes::from("user:1"), Bytes::from("from_engine"))
    );
    assert_eq!(results[1], (Bytes::from("user:2"), Bytes::from("from_txn")));
}

#[test]
fn test_prefix_scan_txn_local_delete_shadows_engine() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap());

    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();

    // Transaction deletes user:1 locally.
    let txn = engine.new_txn().unwrap();
    txn.delete(b"user:1").unwrap();

    let mut iter = txn.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("user:2"), Bytes::from("v2"))]);
}

#[test]
fn test_prefix_scan_binary_keys_with_null_byte() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"\x00user:1", b"v1").unwrap();
    engine.put(b"\x00user:2", b"v2").unwrap();
    engine.put(b"\x01user:1", b"v3").unwrap();

    let mut iter = engine.prefix_scan(b"\x00user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from_static(b"\x00user:1"), Bytes::from("v1")),
            (Bytes::from_static(b"\x00user:2"), Bytes::from("v2")),
        ],
    );
}

#[test]
fn test_prefix_scan_exact_8_byte_user_key() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Exactly 8 bytes — boundary for memcomparable padding.
    engine.put(b"aaaaaaab", b"v1").unwrap();
    engine.put(b"aaaaaaac", b"v2").unwrap();
    engine.put(b"aaaaaaad", b"v3").unwrap();
    engine.put(b"bbbbbbbb", b"v4").unwrap();

    let mut iter = engine.prefix_scan(b"aaaaaaa").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("aaaaaaab"), Bytes::from("v1")),
            (Bytes::from("aaaaaaac"), Bytes::from("v2")),
            (Bytes::from("aaaaaaad"), Bytes::from("v3")),
        ],
    );
}

#[test]
fn test_prefix_scan_empty_user_key() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Empty key is a valid user key.
    engine.put(b"", b"empty_key_val").unwrap();
    engine.put(b"a", b"va").unwrap();

    // Full scan via empty prefix should include the empty key.
    let mut iter = engine.prefix_scan(b"").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from(""), Bytes::from("empty_key_val")),
            (Bytes::from("a"), Bytes::from("va")),
        ],
    );
}

// ── Flush / compaction tests (§8.4) ───────────────────────────────────────

#[test]
fn test_prefix_scan_immutable_memtable() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();

    // Freeze the active memtable (moves it to immutable list) but don't flush.
    engine
        .inner
        .force_freeze_memtable(&engine.inner.state_lock.lock())
        .unwrap();

    // Write more data to the new active memtable.
    engine.put(b"user:3", b"v3").unwrap();

    // All three keys should be visible.
    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v1")),
            (Bytes::from("user:2"), Bytes::from("v2")),
            (Bytes::from("user:3"), Bytes::from("v3")),
        ],
    );
}

#[test]
fn test_prefix_scan_flushed_sst() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();

    // Flush to SST — data no longer in any memtable.
    sync(&engine.inner);

    // Active memtable is empty, but prefix scan should still find the keys.
    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v1")),
            (Bytes::from("user:2"), Bytes::from("v2")),
        ],
    );
}

#[test]
fn test_prefix_scan_multiple_l0_ssts() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write batch 1 and flush.
    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();
    sync(&engine.inner);

    // Write batch 2 and flush.
    engine.put(b"user:3", b"v3").unwrap();
    engine.put(b"user:4", b"v4").unwrap();
    sync(&engine.inner);

    // Two L0 SSTs exist. Prefix scan merges across them.
    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v1")),
            (Bytes::from("user:2"), Bytes::from("v2")),
            (Bytes::from("user:3"), Bytes::from("v3")),
            (Bytes::from("user:4"), Bytes::from("v4")),
        ],
    );
}

#[test]
fn test_prefix_scan_across_flush_with_overwrite() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write and flush batch 1.
    engine.put(b"user:1", b"old").unwrap();
    engine.put(b"user:2", b"v2").unwrap();
    sync(&engine.inner);

    // Overwrite user:1 in a new memtable (not flushed).
    engine.put(b"user:1", b"new").unwrap();

    // Latest value should win.
    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("new")),
            (Bytes::from("user:2"), Bytes::from("v2")),
        ],
    );
}

#[test]
fn test_prefix_scan_post_compaction() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    // Write some data and flush.
    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();
    engine.put(b"other", b"vo").unwrap();
    sync(&engine.inner);

    // Write more and flush again.
    engine.put(b"user:3", b"v3").unwrap();
    sync(&engine.inner);

    // Run full compaction.
    engine.force_full_compaction().unwrap();

    // All prefix-matching keys should survive compaction.
    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("user:1"), Bytes::from("v1")),
            (Bytes::from("user:2"), Bytes::from("v2")),
            (Bytes::from("user:3"), Bytes::from("v3")),
        ],
    );
}

#[test]
fn test_prefix_scan_compaction_with_delete_gc() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, LsmStorageOptions::default_for_test()).unwrap();

    engine.put(b"user:1", b"v1").unwrap();
    engine.put(b"user:2", b"v2").unwrap();
    sync(&engine.inner);

    // Delete user:2 and flush — tombstone in its own SST.
    engine.delete(b"user:2").unwrap();
    sync(&engine.inner);

    // Full compaction should GC the tombstone since both SSTs merge.
    engine.force_full_compaction().unwrap();

    let mut iter = engine.prefix_scan(b"user:").unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("user:1"), Bytes::from("v1"))]);
}

// ── Prefix bloom filter tests ────────────────────────────────────────────

fn prefix_bloom_options(prefix_lengths: Vec<usize>) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 2 << 20,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        num_memtable_limit: 50,
        serializable: false,
        value_separation: None,
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1792,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions {
            enabled: true,
            prefix_lengths,
            false_positive_rate: 0.01,
        },
        ttl_read_filtering: false,
        ttl_background_scanner_interval: None,
    }
}

#[test]
fn test_prefix_bloom_basic_flushed_ssts() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, prefix_bloom_options(vec![6])).unwrap();

    // Write keys with different prefixes and flush to SSTs.
    engine.put(b"tenant:1:user:1", b"v1").unwrap();
    engine.put(b"tenant:1:user:2", b"v2").unwrap();
    engine.put(b"tenant:2:user:1", b"v3").unwrap();
    engine.put(b"other:data", b"v4").unwrap();
    sync(&engine.inner);

    // Prefix scan for "tenant:1:" should return only tenant:1 keys.
    let mut iter = engine.prefix_scan(b"tenant:1:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("tenant:1:user:1"), Bytes::from("v1")),
            (Bytes::from("tenant:1:user:2"), Bytes::from("v2")),
        ],
    );

    // Prefix scan for "tenant:2:" should return only tenant:2 keys.
    let mut iter = engine.prefix_scan(b"tenant:2:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from("tenant:2:user:1"), Bytes::from("v3"))],
    );

    // Missing prefix should return empty.
    let iter = engine.prefix_scan(b"tenant:3:").unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_prefix_bloom_with_multiple_ssts() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, prefix_bloom_options(vec![8])).unwrap();

    // Write and flush multiple SSTs with different tenant prefixes.
    engine.put(b"user:001:data", b"a").unwrap();
    sync(&engine.inner);
    engine.put(b"user:002:data", b"b").unwrap();
    sync(&engine.inner);
    engine.put(b"user:003:data", b"c").unwrap();
    sync(&engine.inner);

    // Each prefix scan should find only the matching key.
    let mut iter = engine.prefix_scan(b"user:001:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from("user:001:data"), Bytes::from("a"))],
    );

    let mut iter = engine.prefix_scan(b"user:002:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from("user:002:data"), Bytes::from("b"))],
    );
}

#[test]
fn test_prefix_bloom_preserves_correctness_after_compaction() {
    let dir = tempdir().unwrap();
    let mut opts = prefix_bloom_options(vec![6]);
    opts.compaction_options =
        CompactionOptions::Simple(crate::compact::SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 2,
            max_levels: 3,
        });
    opts.num_memtable_limit = 2;
    let engine = KvEngine::open(&dir, opts).unwrap();

    // Write data and trigger compaction.
    engine.put(b"tenant:1:key1", b"v1").unwrap();
    engine.put(b"tenant:2:key1", b"v2").unwrap();
    sync(&engine.inner);
    engine.put(b"tenant:1:key2", b"v3").unwrap();
    engine.put(b"tenant:3:key1", b"v4").unwrap();
    sync(&engine.inner);

    // Results should be correct after compaction.
    let mut iter = engine.prefix_scan(b"tenant:1:").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("tenant:1:key1"), Bytes::from("v1")),
            (Bytes::from("tenant:1:key2"), Bytes::from("v3")),
        ],
    );
}

#[test]
fn test_prefix_bloom_short_prefix_fallback() {
    let dir = tempdir().unwrap();
    // Configure prefix length 8, but query with prefix shorter than 8 bytes.
    // The bloom filter should not reject any SST (fallback to range scan).
    let engine = KvEngine::open(&dir, prefix_bloom_options(vec![8])).unwrap();

    engine.put(b"ab:data", b"v1").unwrap();
    engine.put(b"ac:data", b"v2").unwrap();
    engine.put(b"bb:data", b"v3").unwrap();
    sync(&engine.inner);

    // Query with 2-byte prefix (shorter than configured length 8).
    let mut iter = engine.prefix_scan(b"ab").unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("ab:data"), Bytes::from("v1"))]);
}

#[test]
fn test_prefix_bloom_disabled_same_results() {
    // Verify that prefix_scan with prefix bloom enabled returns the same
    // results as with it disabled.
    let dir_disabled = tempdir().unwrap();
    let dir_enabled = tempdir().unwrap();

    let engine_disabled =
        KvEngine::open(&dir_disabled, LsmStorageOptions::default_for_test()).unwrap();
    let engine_enabled = KvEngine::open(&dir_enabled, prefix_bloom_options(vec![6])).unwrap();

    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("user:{:03}:data", i).into_bytes())
        .collect();
    for key in &keys {
        engine_disabled.put(key, b"val").unwrap();
        engine_enabled.put(key, b"val").unwrap();
    }
    sync(&engine_disabled.inner);
    sync(&engine_enabled.inner);

    // Both should return the same results for "user:01:" prefix.
    let mut iter_disabled = engine_disabled.prefix_scan(b"user:01:").unwrap();
    let mut iter_enabled = engine_enabled.prefix_scan(b"user:01:").unwrap();

    let mut results_disabled = vec![];
    while iter_disabled.is_valid() {
        results_disabled.push((
            Bytes::copy_from_slice(iter_disabled.key()),
            Bytes::copy_from_slice(iter_disabled.value()),
        ));
        iter_disabled.next().unwrap();
    }
    let mut results_enabled = vec![];
    while iter_enabled.is_valid() {
        results_enabled.push((
            Bytes::copy_from_slice(iter_enabled.key()),
            Bytes::copy_from_slice(iter_enabled.value()),
        ));
        iter_enabled.next().unwrap();
    }
    assert_eq!(results_disabled, results_enabled);
}

#[test]
fn test_prefix_bloom_with_mvcc_versions() {
    let dir = tempdir().unwrap();
    let engine = KvEngine::open(&dir, prefix_bloom_options(vec![6])).unwrap();

    // Write multiple versions of the same key.
    engine.put(b"user:1:data", b"v1").unwrap();
    engine.put(b"user:1:data", b"v2").unwrap();
    engine.put(b"user:2:data", b"v3").unwrap();
    sync(&engine.inner);

    // Should see only the latest version.
    let mut iter = engine.prefix_scan(b"user:1").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from("user:1:data"), Bytes::from("v2"))],
    );
}

#[test]
fn test_prefix_bloom_v3_reopen_from_disk() {
    let dir = tempdir().unwrap();
    let opts = prefix_bloom_options(vec![6]);

    // Phase 1: write data and flush to v3 SSTs, then drop engine.
    {
        let engine = KvEngine::open(&dir, opts.clone()).unwrap();
        engine.put(b"user:1:data", b"v1").unwrap();
        engine.put(b"user:2:data", b"v2").unwrap();
        engine.put(b"other:data", b"v3").unwrap();
        sync(&engine.inner);
        // engine dropped here — SSTs written to disk with v3 footer
    }

    // Phase 2: reopen — exercises SsTable::open() v3 decode path.
    {
        let engine = KvEngine::open(&dir, opts).unwrap();

        // Prefix scan exercises prefix bloom decode.
        let mut iter = engine.prefix_scan(b"user:1").unwrap();
        check_lsm_iter_result_by_key(
            &mut iter,
            vec![(Bytes::from("user:1:data"), Bytes::from("v1"))],
        );

        // Point get exercises whole-key bloom decode on v3 SST.
        assert_eq!(engine.get(b"user:2:data").unwrap(), Some(Bytes::from("v2")));

        // Missing prefix should return empty.
        let iter = engine.prefix_scan(b"tenant:").unwrap();
        assert!(!iter.is_valid());
    }
}

#[test]
fn test_legacy_v3_sst_bypasses_stale_prefix_bloom() {
    let dir = tempdir().unwrap();
    let opts = prefix_bloom_options(vec![6]);

    {
        let engine = KvEngine::open(&dir, opts.clone()).unwrap();
        engine.put(b"user:1:data", b"v1").unwrap();
        engine.put(b"user:2:data", b"v2").unwrap();
        engine.put(b"other:data", b"v3").unwrap();
        sync(&engine.inner);
    }

    let path = dir.path().join("00000.sst");
    let mut raw = std::fs::read(&path).unwrap();
    let footer_start = raw.len() - 13;
    let prefix_bloom_off =
        u32::from_be_bytes(raw[footer_start - 4..footer_start].try_into().unwrap()) as usize;
    for b in &mut raw[prefix_bloom_off..footer_start - 4] {
        *b = 0;
    }
    let version_idx = raw.len() - 1;
    raw[version_idx] = 3;
    std::fs::write(&path, &raw).unwrap();

    let engine = KvEngine::open(&dir, opts).unwrap();
    let mut iter = engine.prefix_scan(b"user:1").unwrap();
    check_lsm_iter_result_by_key(
        &mut iter,
        vec![(Bytes::from("user:1:data"), Bytes::from("v1"))],
    );
}

#[test]
fn test_prefix_bloom_short_keys_emit_v2() {
    let dir = tempdir().unwrap();
    // Configure prefix length 16, but all keys are shorter than 16 bytes.
    // The builder should emit a v2 SST (no prefix bloom metadata).
    let opts = prefix_bloom_options(vec![16]);
    let engine = KvEngine::open(&dir, opts).unwrap();

    engine.put(b"ab", b"v1").unwrap();
    engine.put(b"cd", b"v2").unwrap();
    sync(&engine.inner);

    // Results should still be correct — prefix bloom simply doesn't apply.
    let mut iter = engine.prefix_scan(b"ab").unwrap();
    check_lsm_iter_result_by_key(&mut iter, vec![(Bytes::from("ab"), Bytes::from("v1"))]);
}
