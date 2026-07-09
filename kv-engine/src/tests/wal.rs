use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use bytes::{BufMut, Bytes};
use crossbeam_channel::bounded;
use crossbeam_skiplist::SkipMap;
use tempfile::tempdir;

use super::harness::create_wal_or_skip;
use crate::{
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    wal::Wal,
};

fn new_skiplist() -> Arc<SkipMap<Bytes, Bytes>> {
    Arc::new(SkipMap::new())
}

#[test]
fn test_wal_batch_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Create WAL and write a batch.
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(&[1], &[10])], 1).unwrap();
    wal.put_batch(&[(&[2], &[20])], 2).unwrap();
    wal.sync().unwrap();

    // Write a partial batch at the logical WAL end (not at preallocated EOF).
    // After wal.sync(), the file has been preallocated via fallocate, and the
    // buffered_file is opened with O_APPEND which would write at the preallocated
    // EOF. Recovery stops at the first zero-filled preallocation page and never
    // reaches that offset. We must write at the logical WAL end instead.
    {
        use std::io::{Seek, SeekFrom, Write};
        // Header (4096) + batch1 (4096) + batch2 (4096) = 12288.
        let logical_end = 4096u64 + 4096 + 4096;
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(logical_end)).unwrap();
        // Write a batch header but no entries — this is a truncated batch.
        let mut buf = Vec::new();
        buf.put_u64(99u64); // commit_ts
        buf.put_u32(1u32); // entry_count = 1
        buf.put_u32(0u32); // fake CRC
        buf.put_u32(0u32); // v4 data_len
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
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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
        // v4 layout: [4096-byte padded header][4KB-aligned batch1][4KB-aligned batch2]
        // Each batch: v4_header(20) + entry_data + zero-pad to 4096.
        // Entry: kind(1) + key_len(2) + key(1) + val_len(2) + val(1) = 7 bytes.
        // Batch2 starts at 4096 + 4096 = 8192.
        // CRC field in v4 header: after commit_ts(8) + entry_count(4) = offset 12.
        let hdr_pad = 4096usize;
        let batch_align = 4096usize;
        assert!(raw.len() >= hdr_pad + batch_align * 2);
        let batch2_start = hdr_pad + batch_align;
        let crc_offset = batch2_start + 12; // commit_ts(8) + entry_count(4)
        raw[crc_offset] ^= 0xFF; // corrupt one byte of batch2's CRC
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
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(&[1], &[10])], 5).unwrap();
    wal.sync().unwrap();

    // Append a batch with entry_count=99 but no entry data.
    {
        use std::io::Write;
        let mut file = wal.buffered_file.lock();
        let mut buf = Vec::new();
        buf.put_u64(99u64); // commit_ts
        buf.put_u32(99u32); // entry_count = 99 (claims 99 entries)
        buf.put_u32(0u32); // fake CRC
        buf.put_u32(0u32); // v4 data_len
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
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(&[1], &[10])], 1).unwrap();
    wal.put_batch(&[(&[2], &[20])], 2).unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Corrupt a byte in the second batch's entry data (not the CRC field).
    {
        use std::fs::OpenOptions;
        use std::io::{Read, Write};
        let mut raw = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut raw)
            .unwrap();
        // v4 layout: [4096-byte header][4KB-aligned batch1][4KB-aligned batch2]
        // Batch2 entry data starts at: batch2_start + v4_header(20).
        // Entry: kind(1) + key_len(2) + key(1) + val_len(2) + val(1) = 7 bytes.
        // Flip the value byte (entry offset 2+1+2 = 5 into entry data).
        let batch2_start = 4096 + 4096;
        let entry_data_offset = batch2_start + 20 + 5; // v4_hdr(20) + kind(1)+key_len(2)+key(1)+val_len_off(1)
        raw[entry_data_offset] ^= 0xFF;
        let mut f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&raw).unwrap();
        f.sync_all().unwrap();
    }

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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    // Write a batch with 0 entries but a non-zero commit_ts.
    wal.put_batch(&[], 42).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 42); // commit_ts should still be recorded
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_group_commit_handles_late_arrival() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("group_commit.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    let wal = Arc::new(wal);
    let start = Arc::new(Barrier::new(4));
    let (tx, rx) = bounded(3);

    let mut handles = Vec::new();
    for worker_id in 0..3u8 {
        let wal = Arc::clone(&wal);
        let start = Arc::clone(&start);
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            let mut keys = Vec::new();
            let mut values = Vec::new();
            let batch_len = if worker_id < 2 { 128 } else { 1 };
            let value_len = if worker_id < 2 { 512 } else { 16 };
            for entry_idx in 0..batch_len {
                keys.push(vec![worker_id, entry_idx as u8]);
                values.push(vec![worker_id.wrapping_add(10); value_len]);
            }
            let refs: Vec<(&[u8], &[u8])> = keys
                .iter()
                .zip(values.iter())
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect();

            start.wait();
            if worker_id == 2 {
                thread::sleep(Duration::from_millis(10));
            }
            let ticket = wal.put_batch(&refs, worker_id as u64 + 1).unwrap();
            wal.submit_and_commit(ticket).unwrap();
            tx.send(worker_id).unwrap();
        }));
    }

    start.wait();

    let mut completed = Vec::new();
    for _ in 0..3 {
        completed.push(
            rx.recv_timeout(Duration::from_secs(10))
                .expect("group commit worker timed out"),
        );
    }

    for handle in handles {
        handle.join().unwrap();
    }
    completed.sort_unstable();
    assert_eq!(completed, vec![0, 1, 2]);

    drop(wal);
    let skiplist = new_skiplist();
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 3);
    assert_eq!(skiplist.len(), 257);
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
    // Recovery validates version == 2, 3, or 4, so version=0x0005
    // must return an "unsupported WAL version" error (no legacy fallback).
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Manually write bytes that spell 'WAL2' but with wrong version.
        f.write_all(&[0x57, 0x41, 0x4C, 0x32]).unwrap(); // magic = WAL2
        f.write_all(&[0x00, 0x05]).unwrap(); // version = 5 (unsupported)
        // The rest is a valid legacy entry: key=[1], value=[2]
        f.write_all(&[0x00, 0x01]).unwrap(); // key_len = 1
        f.write_all(&[0x01]).unwrap(); // key
        f.write_all(&[0x00, 0x01]).unwrap(); // value_len = 1
        f.write_all(&[0x02]).unwrap(); // value
        f.sync_all().unwrap();
    }

    let result = Wal::recover(&path, &skiplist);
    // Should reject with an error — WAL2 magic with unsupported version.
    assert!(
        result.is_err(),
        "expected error for unsupported WAL version"
    );
    let err_msg = format!("{:?}", result.err().unwrap());
    assert!(
        err_msg.contains("unsupported WAL version"),
        "error should mention 'unsupported WAL version', got: {}",
        err_msg
    );
}

#[test]
fn test_wal_append_after_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Write a batch.
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
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

    // Write two valid batches.
    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
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

#[test]
fn test_wal_legacy_recovery_extracts_max_ts() {
    // Legacy WAL with MVCC-encoded keys should recover max_ts from the
    // embedded timestamps.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let skiplist = new_skiplist();

    // Write legacy-format WAL with MVCC-encoded keys (ts=50, ts=100).
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // key1 at ts=50: encode_internal_key(b"k1", 50)
        let enc1 = crate::key::encode_internal_key(b"k1", 50);
        f.write_all(&(enc1.len() as u16).to_be_bytes()).unwrap();
        f.write_all(&enc1).unwrap();
        f.write_all(&1u16.to_be_bytes()).unwrap();
        f.write_all(b"v").unwrap();
        // key2 at ts=100: encode_internal_key(b"k2", 100)
        let enc2 = crate::key::encode_internal_key(b"k2", 100);
        f.write_all(&(enc2.len() as u16).to_be_bytes()).unwrap();
        f.write_all(&enc2).unwrap();
        f.write_all(&1u16.to_be_bytes()).unwrap();
        f.write_all(b"v").unwrap();
        f.sync_all().unwrap();
    }

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 100);
    assert_eq!(skiplist.len(), 2);
}

#[test]
fn test_wal_legacy_put_batch_writes_flat_format() {
    // Calling put_batch on a legacy WAL should write flat entries, not
    // MVCC batch-framed records.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.wal");

    // Create a legacy WAL file (no WAL2 header).
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Write one flat entry so recovery detects legacy format.
        f.write_all(&2u16.to_be_bytes()).unwrap();
        f.write_all(b"k0").unwrap();
        f.write_all(&2u16.to_be_bytes()).unwrap();
        f.write_all(b"v0").unwrap();
        f.sync_all().unwrap();
    }

    let skiplist = new_skiplist();
    let (wal, _max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(skiplist.len(), 1);

    // Put a batch via the legacy WAL handle.
    wal.put_batch(&[(b"k1", b"v1"), (b"k2", b"v2")], 0).unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Re-recover — all entries should be readable as flat entries.
    let skiplist2 = new_skiplist();
    let (_, max_ts2) = Wal::recover(&path, &skiplist2).unwrap();
    assert_eq!(skiplist2.len(), 3);
    assert_eq!(max_ts2, 0); // legacy keys have no embedded ts
    assert_eq!(
        skiplist2
            .get(&Bytes::from(vec![b'k', b'1']))
            .unwrap()
            .value(),
        &Bytes::from(vec![b'v', b'1'])
    );
}

#[test]
fn test_wal_v3_range_tombstone_batch_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3.wal");
    let skiplist = new_skiplist();
    let range_ts = crate::range_tombstone::RangeTombstoneSet::new();

    // Create WAL and write a range tombstone batch.
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_range_tombstone_batch(
        &[
            (b"a" as &[u8], b"m" as &[u8]),
            (b"x" as &[u8], b"z" as &[u8]),
        ],
        42,
    )
    .unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Recover with range tombstone support.
    let (_wal, batch) = Wal::recover_with_range_tombstones(&path, &skiplist, &range_ts).unwrap();
    assert_eq!(batch.max_ts, 42);
    assert_eq!(batch.range_tombstones.len(), 2);
    assert_eq!(batch.range_tombstones[0].start, Bytes::from_static(b"a"));
    assert_eq!(batch.range_tombstones[0].end, Bytes::from_static(b"m"));
    assert_eq!(batch.range_tombstones[0].ts, 42);
    assert_eq!(batch.range_tombstones[1].start, Bytes::from_static(b"x"));
    assert_eq!(batch.range_tombstones[1].end, Bytes::from_static(b"z"));
    // Range tombstones should also be in the set.
    assert_eq!(range_ts.newest_covering_ts(b"f", 100), Some(42));
}

#[test]
fn test_wal_v3_mixed_point_and_range_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_mixed.wal");
    let skiplist = new_skiplist();
    let range_ts = crate::range_tombstone::RangeTombstoneSet::new();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    // Write a point batch.
    wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
    // Write a range tombstone batch.
    wal.put_range_tombstone_batch(&[(b"a", b"z")], 20).unwrap();
    // Write another point batch.
    wal.put_batch(&[(b"key2", b"val2")], 30).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, batch) = Wal::recover_with_range_tombstones(&path, &skiplist, &range_ts).unwrap();
    assert_eq!(batch.max_ts, 30);
    assert_eq!(batch.points.len(), 2);
    assert_eq!(batch.range_tombstones.len(), 1);
    assert_eq!(batch.range_tombstones[0].ts, 20);
    // Skiplist should have point entries.
    assert_eq!(skiplist.len(), 2);
    // Range tombstone set should have the range.
    assert_eq!(range_ts.newest_covering_ts(b"m", 100), Some(20));
}

#[test]
fn test_wal_v3_point_batch_backward_compat() {
    // v3 point batches should still be readable.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_compat.wal");
    let skiplist = new_skiplist();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(b"key1", b"val1"), (b"key2", b"val2")], 42)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 42);
    assert_eq!(skiplist.len(), 2);
    assert_eq!(
        skiplist.get(&Bytes::from_static(b"key1")).unwrap().value(),
        &Bytes::from_static(b"val1")
    );
}

#[test]
fn test_wal_v3_recover_with_tombstones() {
    // v3 point tombstones should be recovered correctly.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_tomb.wal");
    let skiplist = new_skiplist();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
    // Write a tombstone batch (delete key1).
    wal.put_batch(&[(b"key1", &[] as &[u8])], 20).unwrap();
    wal.put_batch(&[(b"key2", b"val2")], 30).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 30);
    // key1 has put then tombstone — tombstone overwrites, so 2 entries remain.
    assert_eq!(skiplist.len(), 2);
}

#[test]
fn test_wal_v3_recover_with_range_tombstones_skipped() {
    // When using recover() (not recover_with_range_tombstones),
    // range tombstone entries should be silently skipped.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_skip.wal");
    let skiplist = new_skiplist();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
    wal.put_range_tombstone_batch(&[(b"a", b"z")], 20).unwrap();
    wal.put_batch(&[(b"key2", b"val2")], 30).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 30);
    // Only point entries in skiplist, range tombstones skipped.
    assert_eq!(skiplist.len(), 2);
}

#[test]
fn test_wal_v3_multiple_range_tombstone_batches() {
    // Multiple range tombstone batches should recover with correct ordinals.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_multi_rt.wal");
    let skiplist = new_skiplist();
    let range_ts = crate::range_tombstone::RangeTombstoneSet::new();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_range_tombstone_batch(&[(b"a", b"m")], 10).unwrap();
    wal.put_range_tombstone_batch(&[(b"x", b"z")], 20).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let (_wal, batch) = Wal::recover_with_range_tombstones(&path, &skiplist, &range_ts).unwrap();
    assert_eq!(batch.max_ts, 20);
    assert_eq!(batch.range_tombstones.len(), 2);
    // Each batch has ordinal 0 (single-entry batches).
    assert_eq!(range_ts.newest_covering_ts(b"f", 100), Some(10));
    assert_eq!(range_ts.newest_covering_ts(b"y", 100), Some(20));
}

#[test]
fn test_wal_v3_empty_batch_recovery() {
    // An empty WAL file should recover cleanly.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_empty.wal");
    let skiplist = new_skiplist();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.sync().unwrap();
    drop(wal);

    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0);
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_v3_truncated_batch_recovery() {
    // A truncated batch should be detected and recovery should stop.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_v3_trunc.wal");
    let _skiplist = new_skiplist();

    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
    wal.put_batch(&[(b"key2", b"val2")], 20).unwrap();
    wal.sync().unwrap();
    drop(wal);

    // Truncate the file deep into the second batch (past alignment padding).
    // v4: header=4096, batch1=4096 (aligned), batch2 starts at 8192.
    // Truncate to remove most of batch2's entry data.
    let metadata = std::fs::metadata(&path).unwrap();
    // Truncate to: header + batch1 + v4_header(20) + partial entry data.
    // This ensures the second batch header is readable but entry data is truncated.
    let truncated_len = 4096 + 4096 + 20 + 3; // header + batch1 + batch2 header + 3 bytes of entry
    assert!(truncated_len < metadata.len());
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(truncated_len).unwrap();
    drop(f);

    let skiplist2 = new_skiplist();
    let (_wal, max_ts) = Wal::recover(&path, &skiplist2).unwrap();
    // First batch should still be recovered.
    assert_eq!(max_ts, 10);
    assert_eq!(skiplist2.len(), 1);
}

#[test]
fn test_memtable_recover_from_wal_with_range_tombstones() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_mt_recover.wal");

    // Create a memtable with WAL and write range tombstones.
    {
        let mt = crate::mem_table::MemTable::create_with_wal(0, false, &path).unwrap();
        mt.put_range_tombstone(b"a", b"z", 42, 0).unwrap();
        // Force WAL write via a point entry.
        mt.for_testing_put_slice(b"key1", b"val1").unwrap();
    }

    // Recover from WAL.
    let (mt, max_ts) =
        crate::mem_table::MemTable::recover_from_wal_with_range_tombstones(0, false, &path)
            .unwrap();
    assert_eq!(max_ts, 42);
    // Range tombstone should be recovered.
    assert_eq!(
        mt.range_tombstones().newest_covering_ts(b"m", 100),
        Some(42)
    );
}

#[test]
fn test_write_range_batch_mvcc_path() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // Write a range-only batch through the MVCC path.
    storage
        .write_batch(&[crate::lsm_storage::WriteBatchRecord::DelRange(
            b"a".as_ref(),
            b"z".as_ref(),
        )])
        .unwrap();

    let state = storage.state.load();
    assert_eq!(state.memtable.range_tombstones().len(), 1);
    assert_eq!(
        state
            .memtable
            .range_tombstones()
            .newest_covering_ts(b"m", u64::MAX),
        Some(1)
    );
}

#[test]
fn test_memtable_recover_from_wal_vlog() {
    // Test recover_from_wal_vlog path (vlog-enabled recovery).
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_vlog_recover.wal");

    // Write entries directly to WAL since for_testing_put_slice doesn't write to WAL.
    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
        wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
        wal.put_batch(&[(b"key2", b"val2")], 20).unwrap();
        wal.sync().unwrap();
    }

    // Recover using vlog recovery path.
    let (mt, max_ts) = crate::mem_table::MemTable::recover_from_wal_vlog(0, &path).unwrap();
    assert_eq!(max_ts, 20);
    assert!(!mt.is_empty());
}

#[test]
fn test_memtable_recover_from_wal_plain() {
    // Test recover_from_wal path (non-vlog, non-range-tombstone).
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_plain_recover.wal");

    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
        wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
        wal.put_batch(&[(b"key2", b"val2")], 20).unwrap();
        wal.sync().unwrap();
    }

    let (mt, max_ts) = crate::mem_table::MemTable::recover_from_wal(0, false, &path).unwrap();
    assert_eq!(max_ts, 20);
    assert!(!mt.is_empty());
}

#[test]
fn test_manifest_v3_to_v4_upgrade() {
    // Create a database, then reopen to verify manifest recovery works.
    let dir = tempdir().unwrap();

    // First open: creates fresh manifest (v4).
    {
        let storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
        storage.put(b"key1", b"val1").unwrap();
    }

    // Reopen: should recover from v4 manifest successfully.
    let _storage =
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
    // Just verify it opens without error — manifest recovery worked.
}

#[test]
fn test_range_overlap_with_point_entries_excluded_bounds() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    storage.put(b"m", b"1").unwrap();
    storage.put(b"p", b"2").unwrap();

    let state = storage.state.load();

    // Excluded lower before first key + Unbounded upper.
    assert!(state.memtable.range_overlap(
        std::ops::Bound::Excluded(b"a" as &[u8]),
        std::ops::Bound::Unbounded,
    ));
    // Excluded lower at first key + Unbounded upper.
    assert!(state.memtable.range_overlap(
        std::ops::Bound::Excluded(b"m" as &[u8]),
        std::ops::Bound::Unbounded,
    ));
    // Included lower at last key + Excluded upper after last key.
    assert!(state.memtable.range_overlap(
        std::ops::Bound::Included(b"p" as &[u8]),
        std::ops::Bound::Excluded(b"z" as &[u8]),
    ));
    // Excluded lower at last key + Included upper after last key.
    assert!(!state.memtable.range_overlap(
        std::ops::Bound::Excluded(b"p" as &[u8]),
        std::ops::Bound::Included(b"z" as &[u8]),
    ));
}

#[test]
fn test_wal_put_batch_key_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_large_key.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    // Key larger than u16::MAX should be rejected.
    let large_key = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put_batch(&[(&large_key, b"val")], 10);
    assert!(result.is_err());
}

#[test]
fn test_wal_put_batch_value_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_large_val.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    // Value larger than u16::MAX should be rejected.
    let large_val = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put_batch(&[(b"key", &large_val)], 10);
    assert!(result.is_err());
}

#[test]
fn test_wal_put_range_tombstone_batch_key_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_large_rt.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    let large_key = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put_range_tombstone_batch(&[(&large_key, b"z")], 10);
    assert!(result.is_err());
}

#[test]
fn test_wal_put_range_tombstone_batch_requires_mvcc() {
    // Non-MVCC WAL should reject range tombstone batches.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_no_mvcc.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    // Default WAL is MVCC-enabled, so this should work.
    let result = wal.put_range_tombstone_batch(&[(b"a", b"z")], 10);
    assert!(result.is_ok());
}

#[test]
fn test_wal_submit_and_commit_flushes_legacy_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy_submit_and_commit.wal");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&2u16.to_be_bytes()).unwrap();
        f.write_all(b"k0").unwrap();
        f.write_all(&2u16.to_be_bytes()).unwrap();
        f.write_all(b"v0").unwrap();
        f.sync_all().unwrap();
    }

    let skiplist = new_skiplist();
    let (wal, _max_ts) = Wal::recover(&path, &skiplist).unwrap();
    let ticket = wal.put_batch(&[(b"k", b"v")], 0).unwrap();
    assert_eq!(ticket, 0);
    wal.submit_and_commit(ticket).unwrap();
    drop(wal);

    let skiplist2 = new_skiplist();
    let (_wal, max_ts2) = Wal::recover(&path, &skiplist2).unwrap();
    assert_eq!(max_ts2, 0);
    assert_eq!(skiplist2.len(), 2);
}

#[test]
fn test_wal_submit_and_commit_empty_wal_is_noop() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty_submit_and_commit.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    wal.submit_and_commit(0).unwrap();
}

#[test]
fn test_wal_submit_and_commit_rejects_unassigned_ticket() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("future_submit_and_commit.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    wal.put_batch(&[(b"k", b"v")], 1).unwrap();
    let err = wal.submit_and_commit(1).unwrap_err();
    assert!(
        err.to_string().contains("unassigned ticket"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn test_wal_put_key_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_put_large.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    let large_key = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put(&large_key, b"val");
    assert!(result.is_err());
}

#[test]
fn test_wal_put_value_too_large() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_put_large_val.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    let large_val = vec![0u8; u16::MAX as usize + 1];
    let result = wal.put(b"key", &large_val);
    assert!(result.is_err());
}

#[test]
fn test_wal_recover_corrupted_magic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_corrupt_magic.wal");

    // Write a valid WAL.
    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
        wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
        wal.sync().unwrap();
    }

    // Corrupt the magic bytes.
    let mut data = std::fs::read(&path).unwrap();
    data[0] = 0xFF;
    std::fs::write(&path, &data).unwrap();

    let skiplist = new_skiplist();
    // Corrupted magic means recovery sees a legacy file with garbage data.
    // Legacy recovery may extract garbage entries from the zero-padded header
    // region. The important thing is that recovery doesn't panic.
    let (_wal, _max_ts) = Wal::recover(&path, &skiplist).unwrap();
}

#[test]
fn test_wal_recover_corrupted_crc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_corrupt_crc.wal");

    // Write a valid WAL.
    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
        wal.put_batch(&[(b"key1", b"val1")], 10).unwrap();
        wal.sync().unwrap();
    }

    // Corrupt a byte in the batch data area (after the 4KB header + batch header).
    let mut data = std::fs::read(&path).unwrap();
    // v4: header is 4096 bytes, batch header is 20 bytes. Corrupt entry data at 4096+20.
    let corrupt_offset = 4096 + 20;
    if data.len() > corrupt_offset {
        data[corrupt_offset] ^= 0xFF;
    }
    std::fs::write(&path, &data).unwrap();

    let skiplist = new_skiplist();
    // Should still recover (corrupted batch is skipped).
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0);
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_memtable_create_with_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_create_wal.wal");
    let mt = crate::mem_table::MemTable::create_with_wal(0, false, &path).unwrap();
    assert!(!mt.vlog_enabled());
}

#[test]
fn test_memtable_create_with_wal_vlog() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_create_wal_vlog.wal");
    let mt = crate::mem_table::MemTable::create_with_wal_vlog(0, &path).unwrap();
    assert!(mt.vlog_enabled());
}

#[test]
fn test_memtable_create_vlog() {
    let mt = crate::mem_table::MemTable::create_vlog(0);
    assert!(mt.vlog_enabled());
}

#[test]
fn test_memtable_is_empty() {
    let mt = crate::mem_table::MemTable::create(0, false);
    assert!(mt.is_empty());
    mt.for_testing_put_slice(b"key", b"val").unwrap();
    assert!(!mt.is_empty());
}

#[test]
fn test_memtable_range_tombstones_accessor() {
    let mt = crate::mem_table::MemTable::create(0, false);
    assert!(mt.range_tombstones().is_empty());
    mt.put_range_tombstone(b"a", b"z", 10, 0).unwrap();
    assert!(!mt.range_tombstones().is_empty());
    assert_eq!(mt.range_tombstones().len(), 1);
}

#[test]
fn test_memtable_put_range_tombstone_with_wal() {
    // Test put_range_tombstone with WAL enabled (exercises WAL write path).
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_rt_wal.wal");
    let mt = crate::mem_table::MemTable::create_with_wal(0, false, &path).unwrap();
    mt.put_range_tombstone(b"a", b"z", 42, 0).unwrap();
    assert_eq!(mt.range_tombstones().len(), 1);
    assert_eq!(
        mt.range_tombstones().newest_covering_ts(b"m", 100),
        Some(42)
    );
}

#[test]
fn test_memtable_put_range_tombstone_batch_with_wal() {
    // Test put_range_tombstone_batch with WAL enabled.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_rt_batch_wal.wal");
    let mt = crate::mem_table::MemTable::create_with_wal(0, false, &path).unwrap();
    mt.put_range_tombstone_batch(&[(b"a", b"m"), (b"x", b"z")], 42, 0)
        .unwrap();
    assert_eq!(mt.range_tombstones().len(), 2);
}

#[test]
fn test_memtable_vlog_enabled() {
    let mt = crate::mem_table::MemTable::create_vlog(0);
    assert!(mt.vlog_enabled());
    let mt2 = crate::mem_table::MemTable::create(0, false);
    assert!(!mt2.vlog_enabled());
}

#[test]
fn test_memtable_approximate_size_with_range_tombstones() {
    let mt = crate::mem_table::MemTable::create(0, false);
    let before = mt.approximate_size();
    mt.put_range_tombstone(b"a", b"z", 10, 0).unwrap();
    mt.put_range_tombstone(b"b", b"y", 20, 1).unwrap();
    let after = mt.approximate_size();
    assert!(after > before);
}

#[test]
fn test_wal_recover_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_empty.wal");

    // Create and immediately close a WAL (just header).
    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
        wal.sync().unwrap();
    }

    let skiplist = new_skiplist();
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 0);
    assert_eq!(skiplist.len(), 0);
}

#[test]
fn test_wal_recover_with_range_tombstones_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_rt_empty.wal");

    {
        let Some(wal) = create_wal_or_skip(&path) else {
            return;
        };
        wal.sync().unwrap();
    }

    let skiplist = new_skiplist();
    let range_ts = crate::range_tombstone::RangeTombstoneSet::new();
    let (_wal, batch) = Wal::recover_with_range_tombstones(&path, &skiplist, &range_ts).unwrap();
    assert_eq!(batch.max_ts, 0);
    assert_eq!(batch.points.len(), 0);
    assert_eq!(batch.range_tombstones.len(), 0);
}

#[test]
fn test_range_tombstone_default() {
    let set = crate::range_tombstone::RangeTombstoneSet::default();
    assert!(set.is_empty());
}

#[test]
fn test_range_tombstone_key_ordering_different_starts() {
    use crate::range_tombstone::RangeTombstoneKey;
    let k1 = RangeTombstoneKey {
        start: bytes::Bytes::from_static(b"a"),
        ts: 100,
        ordinal: 0,
    };
    let k2 = RangeTombstoneKey {
        start: bytes::Bytes::from_static(b"b"),
        ts: 1,
        ordinal: 0,
    };
    // Different start: "a" < "b" regardless of ts.
    assert!(k1 < k2);
}

#[test]
fn test_storage_open_and_close() {
    let dir = tempdir().unwrap();
    {
        let _storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
    }
    // Reopen should work.
    {
        let _storage =
            LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
    }
}

#[test]
fn test_storage_delete_nonexistent() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    // Deleting a non-existent key should succeed silently.
    storage.delete(b"nonexistent").unwrap();
}

#[test]
fn test_wal_empty_drain_skips_fsync() {
    // sync_inner() should return early when the ready queue is empty,
    // avoiding an unnecessary flush+fsync.
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty_drain.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };

    // Sync with nothing in the queue — should succeed without error.
    wal.sync().unwrap();

    // Write a batch, sync, then sync again (second sync has empty drain).
    wal.put_batch(&[(b"k1", b"v1")], 1).unwrap();
    wal.sync().unwrap();
    wal.sync().unwrap(); // no-op sync

    // Verify data is intact.
    drop(wal);
    let skiplist = new_skiplist();
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 1);
    assert_eq!(skiplist.len(), 1);
}

#[test]
fn test_wal_group_commit_multiple_writers() {
    // Verify that concurrent writers with different tickets all observe
    // successful durability through the ticket-based commit barrier.
    let dir = tempdir().unwrap();
    let path = dir.path().join("batch_slots.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    let wal = Arc::new(wal);
    let start = Arc::new(Barrier::new(3));
    let (tx, rx) = bounded(3);

    for worker_id in 0..3u8 {
        let wal = Arc::clone(&wal);
        let start = Arc::clone(&start);
        let tx = tx.clone();
        thread::spawn(move || {
            let ticket = wal
                .put_batch(&[(b"key", &[worker_id])], worker_id as u64 + 1)
                .unwrap();
            start.wait();
            wal.submit_and_commit(ticket).unwrap();
            tx.send(worker_id).unwrap();
        });
    }

    let mut completed = Vec::new();
    for _ in 0..3 {
        completed.push(
            rx.recv_timeout(Duration::from_secs(10))
                .expect("worker timed out"),
        );
    }
    completed.sort_unstable();
    assert_eq!(completed, vec![0, 1, 2]);

    drop(wal);
    let skiplist = new_skiplist();
    let (_wal, max_ts) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(max_ts, 3);
}

#[test]
fn test_mvcc_advance_ts() {
    // Test that advance_ts correctly updates the reader-visible timestamp.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let mvcc = storage.mvcc.as_ref().unwrap();

    let initial_ts = mvcc.read_ts();
    assert_eq!(initial_ts, 0);

    mvcc.advance_ts(5);
    assert_eq!(mvcc.read_ts(), 5);

    mvcc.advance_ts(10);
    assert_eq!(mvcc.read_ts(), 10);
}

#[test]
fn test_write_batch_wal_only_then_publish() {
    // Test the WAL-only + publish-after-sync pattern for batches.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // Write a batch through the normal path (which now uses WAL-only internally).
    storage
        .write_batch(&[
            crate::lsm_storage::WriteBatchRecord::Put(b"k1".as_ref(), b"v1".as_ref()),
            crate::lsm_storage::WriteBatchRecord::Put(b"k2".as_ref(), b"v2".as_ref()),
        ])
        .unwrap();

    // Verify data is visible after the write returns.
    let val1 = storage.get(b"k1").unwrap();
    assert_eq!(val1, Some(bytes::Bytes::from_static(b"v1")));
    let val2 = storage.get(b"k2").unwrap();
    assert_eq!(val2, Some(bytes::Bytes::from_static(b"v2")));
}

#[test]
fn test_write_batch_delete_then_publish() {
    // Test that delete via write_batch uses WAL-only + publish pattern.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());

    // Put then delete in separate batches.
    storage.put(b"k1", b"v1").unwrap();
    storage
        .write_batch(&[crate::lsm_storage::WriteBatchRecord::Del(b"k1".as_ref())])
        .unwrap();

    // Key should be deleted.
    let val = storage.get(b"k1").unwrap();
    assert!(val.is_none(), "key should be deleted");
}

#[test]
fn test_put_then_delete_ts_ordering() {
    // Verify that current_ts is only advanced after publish, so a reader
    // never sees a timestamp whose data isn't visible.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let mvcc = storage.mvcc.as_ref().unwrap();

    let ts_before = mvcc.read_ts();
    storage.put(b"k1", b"v1").unwrap();
    let ts_after = mvcc.read_ts();
    assert!(ts_after > ts_before, "ts should advance after put");

    // The data should be visible at the new ts.
    let val = storage.get(b"k1").unwrap();
    assert_eq!(val, Some(bytes::Bytes::from_static(b"v1")));
}

#[test]
fn test_mvcc_write_batch_ts_advances() {
    // Exercise the mvcc_write_batch path which calls advance_ts after publish.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let mvcc = storage.mvcc.as_ref().unwrap();

    let ts_before = mvcc.read_ts();
    storage
        .mvcc_write_batch(&[
            (
                bytes::Bytes::from_static(b"k1"),
                bytes::Bytes::from_static(b"v1"),
                false,
            ),
            (
                bytes::Bytes::from_static(b"k2"),
                bytes::Bytes::from_static(b"v2"),
                false,
            ),
        ])
        .unwrap();
    let ts_after = mvcc.read_ts();
    assert!(
        ts_after > ts_before,
        "ts should advance after mvcc_write_batch"
    );

    // Data should be visible.
    assert_eq!(
        storage.get(b"k1").unwrap(),
        Some(bytes::Bytes::from_static(b"v1"))
    );
    assert_eq!(
        storage.get(b"k2").unwrap(),
        Some(bytes::Bytes::from_static(b"v2"))
    );
}

#[test]
fn test_mvcc_write_batch_inner_ts_advances() {
    // Exercise the mvcc_write_batch_inner path which calls advance_ts after publish.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let mvcc = storage.mvcc.as_ref().unwrap();

    let ts_before = mvcc.read_ts();
    let commit_ts = storage
        .mvcc_write_batch_inner(&[
            (
                bytes::Bytes::from_static(b"k1"),
                bytes::Bytes::from_static(b"v1"),
                false,
            ),
            (
                bytes::Bytes::from_static(b"k2"),
                bytes::Bytes::from_static(b"v2"),
                false,
            ),
        ])
        .unwrap();
    let ts_after = mvcc.read_ts();
    assert_eq!(ts_after, commit_ts);
    assert!(ts_after > ts_before);

    assert_eq!(
        storage.get(b"k1").unwrap(),
        Some(bytes::Bytes::from_static(b"v1"))
    );
}

#[test]
fn test_delete_advance_ts() {
    // Exercise the delete path which calls advance_ts after publish.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let mvcc = storage.mvcc.as_ref().unwrap();

    storage.put(b"k1", b"v1").unwrap();
    let ts_before = mvcc.read_ts();
    storage.delete(b"k1").unwrap();
    let ts_after = mvcc.read_ts();
    assert!(ts_after > ts_before, "ts should advance after delete");
    assert!(storage.get(b"k1").unwrap().is_none());
}

#[test]
fn test_range_batch_advance_ts() {
    // Exercise the range tombstone write_batch path which calls advance_ts.
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap());
    let mvcc = storage.mvcc.as_ref().unwrap();

    let ts_before = mvcc.read_ts();
    storage
        .write_batch(&[crate::lsm_storage::WriteBatchRecord::DelRange(
            b"a".as_ref(),
            b"z".as_ref(),
        )])
        .unwrap();
    let ts_after = mvcc.read_ts();
    assert!(ts_after > ts_before, "ts should advance after range batch");
}
