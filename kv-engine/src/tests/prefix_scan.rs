use std::sync::Arc;

use bytes::Bytes;
use tempfile::tempdir;

use super::harness::{check_lsm_iter_result_by_key, sync};
use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, prefix_upper_bound},
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
