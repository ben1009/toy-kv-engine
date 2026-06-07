use std::sync::Arc;

use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use tempfile::tempdir;

use crate::wal::Wal;

fn new_skiplist() -> Arc<SkipMap<Bytes, Bytes>> {
    Arc::new(SkipMap::new())
}

#[test]
fn test_wal_batch_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Create WAL and write a batch.
    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1, 2, 3], &[4, 5, 6]), (&[7, 8], &[9])], 42)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Recover and verify.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 42);
    assert_eq!(skiplist.len(), 2);
    assert_eq!(
        skiplist.get(&Bytes::from(vec![1, 2, 3])).unwrap().value(),
        &Bytes::from(vec![4, 5, 6])
    );
    assert_eq!(
        skiplist.get(&Bytes::from(vec![7, 8])).unwrap().value(),
        &Bytes::from(vec![9])
    );
}

#[test]
fn test_wal_put_uses_batch_format() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Create WAL and write individual entries via put().
    let wal = Wal::create(&path).unwrap();
    wal.put(b"key1", b"val1").unwrap();
    wal.put(b"key2", b"val2").unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Recover and verify — entries are wrapped in batches with commit_ts=0.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0); // commit_ts=0 for non-MVCC puts
    assert_eq!(skiplist.len(), 2);
    assert_eq!(
        skiplist.get(&Bytes::from_static(b"key1")).unwrap().value(),
        &Bytes::from_static(b"val1")
    );
    assert_eq!(
        skiplist.get(&Bytes::from_static(b"key2")).unwrap().value(),
        &Bytes::from_static(b"val2")
    );
}

#[test]
fn test_wal_max_ts_across_batches() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10])], 5).unwrap();
    wal.put_batch(&[(&[2], &[20])], 10).unwrap();
    wal.put_batch(&[(&[3], &[30])], 3).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 10);
    assert_eq!(skiplist.len(), 3);
}

#[test]
fn test_wal_truncated_batch_skipped() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Write two complete batches, then truncate the file mid-way through a third.
    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10])], 1).unwrap();
    wal.put_batch(&[(&[2], &[20])], 2).unwrap();
    wal.sync().unwrap();

    // Write a partial batch (don't sync, then truncate the file).
    {
        use std::io::Write;
        let mut file = wal.file.lock();
        // Write a batch header but no entries — this is a truncated batch.
        let mut buf = Vec::new();
        buf.put_u64(99u64); // commit_ts
        buf.put_u32(1u32); // entry_count = 1
        buf.put_u32(0u32); // fake CRC
        // Don't write any entry data — truncated.
        file.write_all(&buf).unwrap();
    }
    drop(wal);

    // Recovery should get the two complete batches and stop at the truncated one.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 2);
    assert_eq!(skiplist.len(), 2);
}

#[test]
fn test_wal_empty_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Create an empty WAL (header only, no entries).
    let wal = Wal::create(&path).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0);
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_multiple_entries_per_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10]), (&[2], &[20]), (&[3], &[30])], 7)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 7);
    assert_eq!(skiplist.len(), 3);
}
