use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    lsm_storage::{KvEngine, LsmStorageOptions, PrefixBloomOptions},
};

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
fn test_serializable_basic_put_get() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    txn.put(b"k1", b"v1").unwrap();
    txn.put(b"k2", b"v2").unwrap();
    txn.commit().unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k2").unwrap(), Some(Bytes::from("v2")));
}

#[test]
fn test_serializable_delete() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    txn.put(b"k1", b"v1").unwrap();
    txn.commit().unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v1")));

    let txn = engine.new_txn().unwrap();
    txn.delete(b"k1").unwrap();
    txn.commit().unwrap();

    assert!(engine.get(b"k1").unwrap().is_none());
}

#[test]
fn test_serializable_conflict_detection_no_conflict() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // T1 writes k1
    let t1 = engine.new_txn().unwrap();
    t1.put(b"k1", b"v1").unwrap();
    t1.commit().unwrap();

    // T2 reads k2 (no overlap), writes k3
    let t2 = engine.new_txn().unwrap();
    t2.get(b"k2").unwrap(); // read k2 — not in t1's write set
    t2.put(b"k3", b"v3").unwrap();
    t2.commit().unwrap(); // should succeed — no conflict

    assert_eq!(engine.get(b"k3").unwrap(), Some(Bytes::from("v3")));
}

#[test]
fn test_serializable_conflict_detection_waw() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // T1 starts, reads k1
    let t1 = engine.new_txn().unwrap();
    t1.get(b"k1").unwrap(); // records k1 in read_set

    // T2 starts, writes k1, commits
    let t2 = engine.new_txn().unwrap();
    t2.put(b"k1", b"v2").unwrap();
    t2.commit().unwrap();

    // T1 tries to write k1 — conflicts with T2 (T2 wrote k1 after T1's read)
    t1.put(b"k1", b"v1").unwrap();
    let result = t1.commit();
    assert!(result.is_err(), "should detect serializable conflict");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("serializable conflict"),
        "error should mention serializable conflict"
    );
}

#[test]
fn test_serializable_double_commit_fails() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    txn.put(b"k1", b"v1").unwrap();
    txn.commit().unwrap();

    let result = txn.commit();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("already committed")
    );
}

#[test]
fn test_serializable_put_after_commit_fails() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    txn.commit().unwrap();

    let result = txn.put(b"k1", b"v1");
    assert!(result.is_err());
}

#[test]
fn test_serializable_read_only_commit() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // Read-only transaction should commit fine
    let txn = engine.new_txn().unwrap();
    txn.get(b"k1").unwrap();
    txn.commit().unwrap();
}

#[test]
fn test_serializable_scan_rejected() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    txn.put(b"a", b"va").unwrap();
    txn.commit().unwrap();

    // Serializable txn scan is rejected (no phantom/range tracking)
    let txn = engine.new_txn().unwrap();
    let result = txn.scan(Bound::Unbounded, Bound::Unbounded);
    assert!(
        result.is_err(),
        "scan should be rejected for serializable txns"
    );
}

#[test]
fn test_serializable_txn_local_get() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // Put initial data
    let txn = engine.new_txn().unwrap();
    txn.put(b"k1", b"v1").unwrap();
    txn.commit().unwrap();

    // New txn: local write shadows engine read
    let txn = engine.new_txn().unwrap();
    assert_eq!(txn.get(b"k1").unwrap(), Some(Bytes::from("v1")));
    txn.put(b"k1", b"v1_local").unwrap();
    assert_eq!(txn.get(b"k1").unwrap(), Some(Bytes::from("v1_local")));
    txn.delete(b"k1").unwrap();
    assert!(txn.get(b"k1").unwrap().is_none());
}

#[test]
fn test_serializable_put_tombstone_value_rejected() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let txn = engine.new_txn().unwrap();
    // The tombstone marker byte (0x02) should be rejected as a value
    let result = txn.put(b"k1", &[0x02]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("tombstone"));
}

#[test]
fn test_serializable_write_conflict_with_read_set() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // Seed data
    let txn = engine.new_txn().unwrap();
    txn.put(b"k1", b"v1").unwrap();
    txn.put(b"k2", b"v2").unwrap();
    txn.commit().unwrap();

    // T1: reads k1 and k2, then tries to write k1
    let t1 = engine.new_txn().unwrap();
    t1.get(b"k1").unwrap(); // adds k1 to read_set
    t1.get(b"k2").unwrap(); // adds k2 to read_set

    // T2: writes k1 between T1's read and commit
    let t2 = engine.new_txn().unwrap();
    t2.put(b"k1", b"v2_wins").unwrap();
    t2.commit().unwrap();

    // T1: write k1 → conflict (T2 wrote k1 which is in T1's read_set)
    t1.put(b"k1", b"v1_loses").unwrap();
    let result = t1.commit();
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("serializable conflict")
    );
}

#[test]
fn test_serializable_no_conflict_when_different_keys() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // Seed
    let txn = engine.new_txn().unwrap();
    txn.put(b"a", b"va").unwrap();
    txn.put(b"b", b"vb").unwrap();
    txn.commit().unwrap();

    // T1 reads "a", T2 writes "b" — no conflict
    let t1 = engine.new_txn().unwrap();
    t1.get(b"a").unwrap();

    let t2 = engine.new_txn().unwrap();
    t2.put(b"b", b"vb2").unwrap();
    t2.commit().unwrap();

    // T1 writes "a" — no conflict since T2 didn't write "a"
    t1.put(b"a", b"va2").unwrap();
    t1.commit().unwrap();

    assert_eq!(engine.get(b"a").unwrap(), Some(Bytes::from("va2")));
    assert_eq!(engine.get(b"b").unwrap(), Some(Bytes::from("vb2")));
}

#[test]
fn test_serializable_write_batch() {
    use crate::lsm_storage::WriteBatchRecord;

    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", b"v1"),
        WriteBatchRecord::Put(b"k2", b"v2"),
        WriteBatchRecord::Put(b"k3", b"v3"),
    ];
    engine.write_batch(&batch).unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k2").unwrap(), Some(Bytes::from("v2")));
    assert_eq!(engine.get(b"k3").unwrap(), Some(Bytes::from("v3")));
}

#[test]
fn test_serializable_write_batch_with_delete() {
    use crate::lsm_storage::WriteBatchRecord;

    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // First put
    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", b"v1"),
        WriteBatchRecord::Put(b"k2", b"v2"),
    ];
    engine.write_batch(&batch).unwrap();

    // Then delete k1 in a batch
    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Del(b"k1"),
        WriteBatchRecord::Put(b"k3", b"v3"),
    ];
    engine.write_batch(&batch).unwrap();

    assert!(engine.get(b"k1").unwrap().is_none());
    assert_eq!(engine.get(b"k2").unwrap(), Some(Bytes::from("v2")));
    assert_eq!(engine.get(b"k3").unwrap(), Some(Bytes::from("v3")));
}

#[test]
fn test_serializable_write_batch_dedup() {
    use crate::lsm_storage::WriteBatchRecord;

    let dir = tempdir().unwrap();
    let engine = Arc::new(KvEngine::open(&dir, serializable_options()).unwrap());

    // Batch with duplicate keys — last write wins
    let batch: Vec<WriteBatchRecord<&[u8]>> = vec![
        WriteBatchRecord::Put(b"k1", b"v1"),
        WriteBatchRecord::Put(b"k1", b"v2"),
        WriteBatchRecord::Put(b"k1", b"v3"),
    ];
    engine.write_batch(&batch).unwrap();

    assert_eq!(engine.get(b"k1").unwrap(), Some(Bytes::from("v3")));
}
