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

#[test]
fn test_wal_entry_data_corruption_detected_by_crc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Write two valid batches.
    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10])], 1).unwrap();
    wal.put_batch(&[(&[2], &[20])], 2).unwrap();
    wal.sync().unwrap();

    // Corrupt a byte in the second batch's entry data (not the CRC field).
    {
        use std::io::{Seek, Write};
        let mut guard = wal.file.lock();
        let file = guard.get_mut();
        // WAL header: 6 bytes.
        // Batch 1: header(16) + entry[key_len:2, key:1, val_len:2, val:1](6) = 22 bytes.
        // Batch 2 starts at offset 6 + 22 = 28.
        // Batch 2 header: commit_ts(8) + entry_count(4) + CRC(4) = 16 bytes.
        // Entry data starts at 28 + 16 = 44.
        // Entry: [key_len:2][key:1][val_len:2][val:1] = 6 bytes.
        // Flip the value byte (offset 44 + 2 + 1 + 2 = 49).
        file.seek(std::io::SeekFrom::Start(49)).unwrap();
        file.write_all(&[0xFF]).unwrap();
    }
    drop(wal);

    // Recovery should get only the first batch — second batch CRC fails.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 1);
    assert_eq!(skiplist.len(), 1);
}

#[test]
fn test_wal_empty_batch_with_commit_ts() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    let wal = Wal::create(&path).unwrap();
    // Write a batch with 0 entries but a non-zero commit_ts.
    wal.put_batch(&[], 42).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 42); // commit_ts should still be recorded
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_recovery_from_tiny_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("tiny.wal");
    let skiplist = new_skiplist();

    // Write a file smaller than WAL_HEADER_SIZE (6 bytes).
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0x01, 0x02, 0x03]).unwrap();
    }

    // Should fall back to legacy format and find nothing.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0);
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_legacy_data_coincidentally_matching_magic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("tricky.wal");
    let skiplist = new_skiplist();

    // Construct a legacy-format WAL where the first 4 bytes happen to be
    // 0x57414C32 (the MVCC magic 'WAL2'). This is a false positive test.
    // key_len=0x5741 (22337) would be huge, so instead we craft bytes that
    // match the magic but are followed by a version that doesn't match.
    // The fix validates version == WAL_FORMAT_VERSION (2), so version=0x0003
    // should cause fallback to legacy.
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Manually write bytes that spell 'WAL2' but with wrong version.
        f.write_all(&[0x57, 0x41, 0x4C, 0x32]).unwrap(); // magic = WAL2
        f.write_all(&[0x00, 0x03]).unwrap(); // version = 3 (not 2)
        // The rest is a valid legacy entry: key=[1], value=[2]
        f.write_all(&[0x00, 0x01]).unwrap(); // key_len = 1
        f.write_all(&[0x01]).unwrap(); // key
        f.write_all(&[0x00, 0x01]).unwrap(); // value_len = 1
        f.write_all(&[0x02]).unwrap(); // value
        f.sync_all().unwrap();
    }

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    // Should fall back to legacy because version doesn't match.
    assert_eq!(max_ts, 0);
    // The first 6 bytes (magic+version) are consumed by the MVCC header check,
    // but since version doesn't match, it falls to legacy. The legacy parser
    // starts at offset 0 and reads key_len from the first 2 bytes (0x57, 0x41)
    // which is 22337 — too large for the remaining data, so it stops.
    // Result: 0 entries recovered (the legacy parser hits the truncated entry).
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_append_after_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Write a batch.
    let wal = Wal::create(&path).unwrap();
    wal.put_batch(&[(&[1], &[10])], 5).unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Recover — gets the WAL handle in append mode.
    let (wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 5);
    assert_eq!(skiplist.len(), 1);

    // Write another batch through the recovered handle.
    wal.put_batch(&[(&[2], &[20])], 10).unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Recover again — should see both batches.
    let skiplist2 = new_skiplist();
    let (_wal, max_ts) = Wal::recover(&path, &skiplist2).unwrap();
    assert_eq!(max_ts, 10);
    assert_eq!(skiplist2.len(), 2);
}

#[test]
fn test_wal_key_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let wal = Wal::create(&path).unwrap();
    // Key exactly at the limit should succeed.
    let big_key = vec![0u8; u16::MAX as usize];
    wal.put(&big_key, b"v").unwrap();

    // Key exceeding u16::MAX should fail.
    let too_big_key = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put(&too_big_key, b"v");
    assert!(result.is_err(), "expected error for oversized key");
    assert!(
        result.unwrap_err().to_string().contains("too large"),
        "error should mention 'too large'"
    );
}

#[test]
fn test_wal_value_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let wal = Wal::create(&path).unwrap();
    let too_big_val = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put(b"k", &too_big_val);
    assert!(result.is_err(), "expected error for oversized value");
    assert!(
        result.unwrap_err().to_string().contains("too large"),
        "error should mention 'too large'"
    );
}

#[test]
fn test_wal_batch_key_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let wal = Wal::create(&path).unwrap();
    let too_big_key = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put_batch(&[(&too_big_key, b"v")], 1);
    assert!(result.is_err(), "expected error for oversized batch key");
    assert!(
        result.unwrap_err().to_string().contains("too large"),
        "error should mention 'too large'"
    );
}

#[test]
fn test_wal_batch_value_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let wal = Wal::create(&path).unwrap();
    let too_big_val = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put_batch(&[(b"k", too_big_val.as_slice())], 1);
    assert!(result.is_err(), "expected error for oversized batch value");
}

#[test]
fn test_wal_recovery_large_key_size_in_entry() {
    // Craft a batch where the entry declares a huge key_size, triggering the
    // new bounds check before reading val_size.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Write MVCC header.
        f.write_all(&0x5741_4C32u32.to_be_bytes()).unwrap(); // WAL_MVCC_MAGIC
        f.write_all(&2u16.to_be_bytes()).unwrap(); // WAL_FORMAT_VERSION
        // Write a batch header with entry_count=1.
        f.write_all(&1u64.to_be_bytes()).unwrap(); // commit_ts=1
        f.write_all(&1u32.to_be_bytes()).unwrap(); // entry_count=1
        // CRC will be wrong but we stop before CRC check.
        f.write_all(&0u32.to_be_bytes()).unwrap(); // fake CRC
        // Write entry: key_len=60000 (but only 2 bytes of key data follow).
        f.write_all(&60000u16.to_be_bytes()).unwrap(); // key_len
        f.write_all(&[0x01, 0x02]).unwrap(); // only 2 bytes of key (truncated)
        f.sync_all().unwrap();
    }

    // Should recover without panic — the bounds check catches the large key_size.
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0); // batch is skipped (truncated)
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_truncates_trailing_garbage_on_recovery() {
    // Write valid MVCC batches, then append garbage (simulating a crash
    // mid-write). Recovery must truncate the file to the last valid byte
    // so subsequent appends don't leave corrupted data in the file.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Write two valid batches.
    {
        let wal = Wal::create(&path).unwrap();
        wal.put_batch(&[(b"k1", b"v1")], 10).unwrap();
        wal.put_batch(&[(b"k2", b"v2")], 20).unwrap();
        wal.sync().unwrap();
        drop(wal);
    }

    // Append garbage bytes (simulating a crash after the last valid batch).
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        // Partial batch header — this is trailing garbage.
        f.write_all(&99u64.to_be_bytes()).unwrap(); // fake commit_ts
        f.write_all(&[0xFF; 10]).unwrap(); // junk bytes
        f.sync_all().unwrap();
    }

    let file_len_before = std::fs::metadata(&path).unwrap().len();

    // Recover — should truncate the file.
    let skiplist2 = new_skiplist();
    let (wal, max_ts) = Wal::recover(&path, &skiplist2).unwrap();
    assert_eq!(max_ts, 20);
    assert_eq!(skiplist2.len(), 2);

    // File should be shorter (garbage removed).
    let file_len_after = std::fs::metadata(&path).unwrap().len();
    assert!(
        file_len_after < file_len_before,
        "expected truncation: before={}, after={}",
        file_len_before,
        file_len_after
    );

    // Append a new batch after recovery — must not corrupt the file.
    wal.put_batch(&[(b"k3", b"v3")], 30).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let file_len_after_write = std::fs::metadata(&path).unwrap().len();
    assert!(
        file_len_after_write > file_len_after,
        "expected new batch to grow the file: after_truncation={}, after_write={}",
        file_len_after,
        file_len_after_write
    );

    // Re-recover and verify all three batches are intact.
    let skiplist3 = new_skiplist();
    let (_wal, max_ts2) = Wal::recover(&path, &skiplist3).unwrap();
    assert_eq!(max_ts2, 30);
    assert_eq!(skiplist3.len(), 3);
    assert_eq!(
        skiplist3
            .get(&Bytes::from(vec![b'k', b'1']))
            .unwrap()
            .value(),
        &Bytes::from(vec![b'v', b'1'])
    );
    assert_eq!(
        skiplist3
            .get(&Bytes::from(vec![b'k', b'3']))
            .unwrap()
            .value(),
        &Bytes::from(vec![b'v', b'3'])
    );
}
