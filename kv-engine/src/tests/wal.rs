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

#[test]
fn test_wal_legacy_format_recovery() {
    // Write a WAL file in the old flat format (no header, no batches).
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy.wal");
    let skiplist = new_skiplist();

    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Entry 1: key=[1,2], value=[3,4,5]
        f.write_all(&[0, 2]).unwrap(); // key_len = 2
        f.write_all(&[1, 2]).unwrap(); // key
        f.write_all(&[0, 3]).unwrap(); // value_len = 3
        f.write_all(&[3, 4, 5]).unwrap(); // value
        // Entry 2: key=[6], value=[7,8]
        f.write_all(&[0, 1]).unwrap(); // key_len = 1
        f.write_all(&[6]).unwrap(); // key
        f.write_all(&[0, 2]).unwrap(); // value_len = 2
        f.write_all(&[7, 8]).unwrap(); // value
        f.sync_all().unwrap();
    }

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    // Legacy format has no commit_ts, so max_ts stays 0.
    assert_eq!(max_ts, 0);
    assert_eq!(skiplist.len(), 2);
    assert_eq!(
        skiplist.get(&Bytes::from(vec![1, 2])).unwrap().value(),
        &Bytes::from(vec![3, 4, 5])
    );
    assert_eq!(
        skiplist.get(&Bytes::from(vec![6])).unwrap().value(),
        &Bytes::from(vec![7, 8])
    );
}

#[test]
fn test_wal_legacy_truncated_entry() {
    // Legacy WAL with a truncated entry — recovery should stop at the last complete entry.
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy_trunc.wal");
    let skiplist = new_skiplist();

    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Complete entry
        f.write_all(&[0, 1]).unwrap(); // key_len = 1
        f.write_all(&[10]).unwrap(); // key
        f.write_all(&[0, 2]).unwrap(); // value_len = 2
        f.write_all(&[20, 30]).unwrap(); // value
        // Truncated: key_len says 5 but only 1 byte follows
        f.write_all(&[0, 5]).unwrap(); // key_len = 5
        f.write_all(&[99]).unwrap(); // only 1 byte of key
        f.sync_all().unwrap();
    }

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0);
    // Only the complete entry is recovered.
    assert_eq!(skiplist.len(), 1);
    assert_eq!(
        skiplist.get(&Bytes::from(vec![10])).unwrap().value(),
        &Bytes::from(vec![20, 30])
    );
}

#[test]
fn test_wal_crc_mismatch_stops_recovery() {
    // Write two valid batches then corrupt the CRC of the second.
    let dir = tempdir().unwrap();
    let path = dir.path().join("crc_bad.wal");
    let skiplist = new_skiplist();

    // Write two valid batches.
    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10])], 1).unwrap();
    wal.put_batch(&[(&[2], &[20])], 2).unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Corrupt the CRC of the second batch by flipping a byte in the CRC field.
    {
        use std::fs::OpenOptions;
        use std::io::{Read, Write};
        let mut raw = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut raw)
            .unwrap();
        // Layout: [6-byte header][16-byte batch1 header][6-byte batch1 entries]
        //         [16-byte batch2 header][6-byte batch2 entries]
        // Batch2 CRC is at offset 6 + 16 + 6 + 8 + 4 = 40, bytes 40..44.
        assert_eq!(raw.len(), 6 + 16 + 6 + 16 + 6);
        raw[40] ^= 0xFF; // corrupt one byte of batch2's CRC
        let mut f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&raw).unwrap();
        f.sync_all().unwrap();
    }

    // Recovery should get batch1 only, stop at batch2 due to CRC mismatch.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 1);
    assert_eq!(skiplist.len(), 1);
    assert!(skiplist.get(&Bytes::from_static(&[1])).is_some());
    assert!(skiplist.get(&Bytes::from_static(&[2])).is_none());
}

#[test]
fn test_wal_batch_entry_count_exceeds_data() {
    // Write a batch header claiming 99 entries but don't write any entry data.
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad_count.wal");
    let skiplist = new_skiplist();

    // Write a valid batch first.
    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10])], 5).unwrap();
    wal.sync().unwrap();

    // Append a batch with entry_count=99 but no entry data.
    {
        use std::io::Write;
        let mut file = wal.file.lock();
        let mut buf = Vec::new();
        buf.put_u64(99u64); // commit_ts
        buf.put_u32(99u32); // entry_count = 99 (claims 99 entries)
        buf.put_u32(0u32); // fake CRC
        // No entry data at all — truncated.
        file.write_all(&buf).unwrap();
    }
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 5); // only the first batch is recovered
    assert_eq!(skiplist.len(), 1);
}
