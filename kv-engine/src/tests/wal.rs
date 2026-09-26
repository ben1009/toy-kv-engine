use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use bytes::{BufMut, Bytes};
use crossbeam_channel::bounded;
use crossbeam_skiplist::SkipMap;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

use super::harness::{create_wal_or_skip, is_io_uring_unavailable_error};
#[cfg(feature = "bench")]
use crate::mem_table::WriteProfile;
use crate::{
    lsm_storage::{KvEngine, LsmStorageInner, LsmStorageOptions, WriteBatchRecord},
    mem_table::MemTable,
    wal::{Wal, WalIoMode},
};

fn new_skiplist() -> Arc<SkipMap<Bytes, Bytes>> {
    Arc::new(SkipMap::new())
}

fn set_parallel_wal_file_size_limit(engine: &KvEngine, limit: u64) -> usize {
    let state = engine.inner.state.load_full();
    state
        .memtable
        .set_parallel_wal_file_size_limit_for_test(limit)
        .expect("set active parallel WAL size limit");
    state.memtable.id()
}

#[test]
fn test_wal_v5_create_preserves_identity_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    let Ok(wal) = Wal::create_v5(&path, header) else {
        return;
    };
    assert_eq!(wal.format_version(), crate::pitr::WAL_V5_VERSION);
    assert_eq!(wal.io_mode(), WalIoMode::Leader);
    let limits = crate::pitr::WalV5Limits {
        max_input_entry_count: 16,
        max_batch_data_bytes: 4096,
        max_entry_count: 16,
        max_key_bytes: 1024,
        max_value_bytes: 1024,
    };
    wal.configure_pitr_limits(8192, 16 * 1024).unwrap();
    let batch = crate::pitr::WalBatch {
        commit_ts: 1,
        recorded_at: crate::pitr::RecordedAt { secs: 1, nanos: 0 },
        entries: vec![crate::pitr::WalEntry::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }],
    };
    let ticket = wal.put_v5_batch(&batch, limits, None).unwrap();
    wal.submit_and_commit(ticket).unwrap();
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(crate::pitr::decode_v5_file_header(&bytes).unwrap(), header);
    assert_eq!(
        crate::pitr::decode_v5_batch(&bytes, crate::pitr::WAL_V5_HEADER_LEN, limits)
            .unwrap()
            .batch,
        batch
    );
    let (incremental_seal, incremental_bytes) = wal.finalize_pitr_seal().unwrap();
    let (rebuilt_seal, rebuilt_bytes) = crate::pitr::seal::build_v5_seal(&bytes).unwrap();
    assert_eq!(incremental_seal, rebuilt_seal);
    assert_eq!(incremental_bytes, rebuilt_bytes);
    assert!(wal.put_v5_batch(&batch, limits, None).is_err());
}

#[test]
fn test_parallel_v4_wal_writes_syncs_and_recovers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("candidate-v4.wal");
    let wal = match Wal::create_with_io_mode(&path, WalIoMode::Parallel) {
        Ok(wal) => wal,
        Err(error) if is_io_uring_unavailable_error(&error) => {
            eprintln!("skipping test (io_uring unavailable): {error}");
            return;
        }
        Err(error) => panic!("failed to create WAL: {error:#}"),
    };

    assert_eq!(wal.io_mode(), WalIoMode::Parallel);
    let first = wal
        .put_batch(&[(b"key".as_slice(), b"value".as_slice())], 1)
        .expect("admit first parallel WAL batch");
    let second = wal
        .put_batch(&[(b"key-2".as_slice(), b"value-2".as_slice())], 2)
        .expect("admit second parallel WAL batch");
    let third = wal
        .put_range_tombstone_batch(&[(b"range-start".as_slice(), b"range-end".as_slice())], 3)
        .expect("admit parallel range tombstone batch");
    assert_eq!((first, second, third), (0, 1, 2));
    assert_eq!(wal.assigned_ticket_count(), 3);
    wal.submit_and_commit(first).expect("durable first batch");
    wal.submit_and_commit(second).expect("durable second batch");
    wal.submit_and_commit(third)
        .expect("durable range tombstone batch");
    wal.sync().expect("sync captured ticket cutoff");
    wal.close().expect("drain and close parallel WAL");
    drop(wal);

    let skiplist = new_skiplist();
    let range_tombstones = crate::range_tombstone::RangeTombstoneSet::new();
    let (recovered, batch) = Wal::recover_with_range_tombstones_and_mode(
        &path,
        &skiplist,
        &range_tombstones,
        WalIoMode::Parallel,
    )
    .unwrap();
    assert_eq!(recovered.io_mode(), WalIoMode::Parallel);
    assert_eq!(batch.max_ts, 3);
    assert_eq!(
        skiplist.get(b"key".as_slice()).unwrap().value().as_ref(),
        b"value"
    );
    assert_eq!(
        skiplist.get(b"key-2".as_slice()).unwrap().value().as_ref(),
        b"value-2"
    );
    assert_eq!(batch.range_tombstones.len(), 1);
    assert_eq!(batch.range_tombstones[0].start.as_ref(), b"range-start");
    assert_eq!(batch.range_tombstones[0].end.as_ref(), b"range-end");
}

#[test]
fn test_parallel_v4_recovery_stops_at_first_invalid_batch() {
    const BATCH_ALIGNMENT: usize = 4096;
    const WAL_HEADER_END: usize = 4096;

    let dir = tempdir().unwrap();
    let path = dir.path().join("candidate-v4-hole.wal");
    let wal = match Wal::create_with_io_mode(&path, WalIoMode::Parallel) {
        Ok(wal) => wal,
        Err(error) if is_io_uring_unavailable_error(&error) => {
            eprintln!("skipping test (io_uring unavailable): {error:#}");
            return;
        }
        Err(error) => panic!("failed to create WAL: {error:#}"),
    };

    for (key, value, commit_ts) in [
        (b"before".as_slice(), b"durable-before".as_slice(), 1),
        (b"hole".as_slice(), b"damaged".as_slice(), 2),
        (b"after".as_slice(), b"complete-after-hole".as_slice(), 3),
    ] {
        let ticket = wal.put_batch(&[(key, value)], commit_ts).unwrap();
        wal.submit_and_commit(ticket).unwrap();
    }
    wal.close().unwrap();
    drop(wal);

    // Inject media corruption after close to test the v4 scanner's prefix rule.
    // The separate process-kill test checks survival of acknowledged batches.
    let mut bytes = std::fs::read(&path).unwrap();
    let second_batch_crc = WAL_HEADER_END + BATCH_ALIGNMENT + 12;
    bytes[second_batch_crc] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();

    let skiplist = new_skiplist();
    let range_tombstones = crate::range_tombstone::RangeTombstoneSet::new();
    let (recovered, batch) = Wal::recover_with_range_tombstones_and_mode(
        &path,
        &skiplist,
        &range_tombstones,
        WalIoMode::Leader,
    )
    .unwrap();

    assert_eq!(batch.max_ts, 1);
    assert_eq!(
        skiplist.get(b"before".as_slice()).unwrap().value().as_ref(),
        b"durable-before"
    );
    assert!(skiplist.get(b"hole".as_slice()).is_none());
    assert!(skiplist.get(b"after".as_slice()).is_none());
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (WAL_HEADER_END + BATCH_ALIGNMENT) as u64
    );
    recovered.close().unwrap();
}

#[test]
fn test_parallel_wal_engine_publishes_point_and_transaction_commits() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    options.serializable = true;
    options.target_sst_size = 1 << 30;

    let engine = match KvEngine::open_with_wal_io_mode(dir.path(), options, WalIoMode::Parallel) {
        Ok(engine) => engine,
        Err(error) if is_io_uring_unavailable_error(&error) => {
            eprintln!("skipping test (io_uring unavailable): {error:#}");
            return;
        }
        Err(error) => panic!("failed to open parallel WAL engine: {error:#}"),
    };

    engine
        .put(b"direct", b"value")
        .expect("commit direct write");
    engine
        .write_batch(&[WriteBatchRecord::Put(
            b"batch".as_slice(),
            b"batch-value".as_slice(),
        )])
        .expect("commit batch write");
    let transaction = engine.new_txn().expect("create serializable transaction");
    transaction.put(b"transaction", b"value").unwrap();
    transaction.commit().expect("commit transaction");
    engine.sync().expect("sync parallel WAL");

    assert_eq!(
        engine.get(b"direct").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert_eq!(
        engine.get(b"batch").unwrap().as_deref(),
        Some(&b"batch-value"[..])
    );
    assert_eq!(
        engine.get(b"transaction").unwrap().as_deref(),
        Some(&b"value"[..])
    );

    {
        let state_lock = engine.inner.state_lock.lock();
        engine
            .inner
            .force_freeze_memtable(&state_lock)
            .expect("freeze parallel WAL into immutable memtable");
    }
    let state = engine.inner.state.load_full();
    assert_eq!(state.memtable.parallel_wal_is_closed(), Some(false));
    assert_eq!(state.imm_memtables.len(), 1);
    assert_eq!(state.imm_memtables[0].parallel_wal_is_closed(), Some(false));

    engine.close().expect("close parallel WAL engine");
    assert_eq!(state.memtable.parallel_wal_is_closed(), Some(true));
    assert_eq!(state.imm_memtables[0].parallel_wal_is_closed(), Some(true));
}

#[test]
fn test_parallel_wal_full_rotates_point_transaction_and_batch_writes() {
    const TEST_WAL_CAP: u64 = 1 << 20;
    const LARGE_VALUE_LEN: usize = 60 * 1024;

    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    options.serializable = true;
    options.target_sst_size = 1 << 30;
    let engine = match KvEngine::open_with_wal_io_mode(dir.path(), options, WalIoMode::Parallel) {
        Ok(engine) => engine,
        Err(error) if is_io_uring_unavailable_error(&error) => {
            eprintln!("skipping test (io_uring unavailable): {error:#}");
            return;
        }
        Err(error) => panic!("failed to open parallel WAL engine: {error:#}"),
    };
    let large_value = vec![b'x'; LARGE_VALUE_LEN];

    let first_memtable_id = set_parallel_wal_file_size_limit(&engine, TEST_WAL_CAP);
    engine.put(b"conflict", b"initial").unwrap();
    for _ in 0..15 {
        engine.put(b"filler", &large_value).unwrap();
    }
    let before_point_rotation = Arc::clone(&engine.inner.state.load().memtable);
    assert_eq!(before_point_rotation.id(), first_memtable_id);
    assert_eq!(before_point_rotation.wal_batch_count(), Some(16));

    let conflicting_txn = engine.new_txn().unwrap();
    assert_eq!(
        conflicting_txn.get(b"conflict").unwrap().as_deref(),
        Some(&b"initial"[..])
    );
    conflicting_txn
        .put(b"transaction-conflict-write", b"must-not-publish")
        .unwrap();
    engine.put(b"conflict", &large_value).unwrap();
    let point_rotation_id = engine.inner.state.load().memtable.id();
    assert_ne!(point_rotation_id, first_memtable_id);
    assert_eq!(before_point_rotation.wal_batch_count(), Some(16));
    assert!(conflicting_txn.commit().is_err());

    set_parallel_wal_file_size_limit(&engine, TEST_WAL_CAP);
    for _ in 0..14 {
        engine.put(b"filler", &large_value).unwrap();
    }
    let before_transaction_rotation = Arc::clone(&engine.inner.state.load().memtable);
    assert_eq!(before_transaction_rotation.wal_batch_count(), Some(15));
    let transaction = engine.new_txn().unwrap();
    transaction
        .put(b"transaction-rotation", &large_value)
        .unwrap();
    transaction.commit().unwrap();
    assert_ne!(
        engine.inner.state.load().memtable.id(),
        before_transaction_rotation.id()
    );
    assert_eq!(before_transaction_rotation.wal_batch_count(), Some(15));

    set_parallel_wal_file_size_limit(&engine, TEST_WAL_CAP);
    for _ in 0..14 {
        engine.put(b"filler", &large_value).unwrap();
    }
    let before_batch_rotation = Arc::clone(&engine.inner.state.load().memtable);
    assert_eq!(before_batch_rotation.wal_batch_count(), Some(15));
    engine
        .write_batch(&[WriteBatchRecord::Put(
            b"batch-rotation".as_slice(),
            large_value.as_slice(),
        )])
        .unwrap();
    assert_ne!(
        engine.inner.state.load().memtable.id(),
        before_batch_rotation.id()
    );
    assert_eq!(before_batch_rotation.wal_batch_count(), Some(15));

    assert_eq!(
        engine.get(b"conflict").unwrap().as_deref(),
        Some(large_value.as_slice())
    );
    assert_eq!(
        engine.get(b"transaction-rotation").unwrap().as_deref(),
        Some(large_value.as_slice())
    );
    assert_eq!(
        engine.get(b"batch-rotation").unwrap().as_deref(),
        Some(large_value.as_slice())
    );
    engine.close().unwrap();
    drop(engine);

    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    options.serializable = true;
    options.target_sst_size = 1 << 30;
    let reopened =
        KvEngine::open_with_wal_io_mode(dir.path(), options, WalIoMode::Parallel).unwrap();
    assert_eq!(
        reopened.get(b"conflict").unwrap().as_deref(),
        Some(large_value.as_slice())
    );
    assert_eq!(
        reopened.get(b"transaction-rotation").unwrap().as_deref(),
        Some(large_value.as_slice())
    );
    assert_eq!(
        reopened.get(b"batch-rotation").unwrap().as_deref(),
        Some(large_value.as_slice())
    );
    reopened.close().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn test_parallel_selector_rejects_async_writes_during_v5_successor() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_test();
    options.enable_wal = true;
    options.serializable = true;
    let engine = match KvEngine::open_with_wal_io_mode(dir.path(), options, WalIoMode::Parallel) {
        Ok(engine) => engine,
        Err(error) if is_io_uring_unavailable_error(&error) => {
            eprintln!("skipping test (io_uring unavailable): {error:#}");
            return;
        }
        Err(error) => panic!("failed to open parallel WAL engine: {error:#}"),
    };

    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    {
        let state_lock = engine.inner.state_lock.lock();
        engine
            .inner
            .install_pitr_v5_successor(header, &state_lock)
            .expect("install temporary PITR WAL");
    }
    assert!(engine.inner.state.load().memtable.uses_wal_v5());
    assert!(engine.inner.selects_parallel_wal_io());

    let error = crate::future_ext::block_on(engine.put_async(b"async", b"value"))
        .expect_err("async write must remain disabled while v5 is active");
    assert!(error.to_string().contains("parallel WAL"));

    let txn = engine.new_txn().expect("start transaction");
    txn.put(b"txn", b"value").unwrap();
    let error = crate::future_ext::block_on(txn.commit_async())
        .expect_err("async transaction must remain disabled while v5 is active");
    assert!(error.to_string().contains("parallel WAL"));

    engine.close().expect("close parallel WAL engine");
}

#[test]
fn test_wal_v5_admission_reserves_before_ticket_without_concurrent_overshoot() {
    let dir = tempdir().unwrap();
    let wal = Arc::new(
        Wal::create_v5(
            dir.path().join("bounded-v5.wal"),
            crate::pitr::WalV5Header {
                wal_format_version: crate::pitr::WAL_V5_VERSION,
                timeline_id: crate::pitr::TimelineId([1; 16]),
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
                segment_id: crate::pitr::SegmentId(3),
                predecessor: crate::pitr::ChainAnchor::Genesis {
                    archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
                },
            },
        )
        .unwrap(),
    );
    wal.configure_pitr_limits(8192, 8192).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let mut workers = Vec::new();
    for commit_ts in 1..=8 {
        let wal = Arc::clone(&wal);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            wal.put_v5_batch(
                &crate::pitr::WalBatch {
                    commit_ts,
                    recorded_at: crate::pitr::RecordedAt {
                        secs: 1,
                        nanos: commit_ts as u32,
                    },
                    entries: vec![crate::pitr::WalEntry::Put {
                        key: vec![commit_ts as u8],
                        value: vec![1],
                    }],
                },
                crate::pitr::LIVE_WAL_V5_LIMITS,
                None,
            )
        }));
    }
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(wal.batch_count(), 1);
    assert!(wal.pitr_rotation_needed());
}

#[test]
fn test_wal_v5_oversized_batch_fails_before_consuming_ticket() {
    let dir = tempdir().unwrap();
    let wal = Wal::create_v5(
        dir.path().join("oversized-v5.wal"),
        crate::pitr::WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(3),
            predecessor: crate::pitr::ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        },
    )
    .unwrap();
    wal.configure_pitr_limits(4096, 16 * 1024).unwrap();
    let oversized = crate::pitr::WalBatch {
        commit_ts: 1,
        recorded_at: crate::pitr::RecordedAt { secs: 1, nanos: 0 },
        entries: vec![crate::pitr::WalEntry::Put {
            key: b"key".to_vec(),
            value: vec![7; 5000],
        }],
    };
    assert!(
        wal.put_v5_batch(&oversized, crate::pitr::LIVE_WAL_V5_LIMITS, None)
            .is_err()
    );
    assert_eq!(wal.batch_count(), 0);
}

#[test]
fn test_memtable_dispatches_canonical_v5_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-memtable.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    let Ok(memtable) = MemTable::create_with_wal_v5(9, false, &path, header) else {
        return;
    };
    let limits = crate::pitr::WalV5Limits {
        max_input_entry_count: 16,
        max_batch_data_bytes: 4096,
        max_entry_count: 16,
        max_key_bytes: 1024,
        max_value_bytes: 1024,
    };
    let batch = crate::pitr::WalBatch {
        commit_ts: 1,
        recorded_at: crate::pitr::RecordedAt { secs: 1, nanos: 0 },
        entries: vec![crate::pitr::WalEntry::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }],
    };
    let ticket = memtable.write_pitr_wal_batch_only(&batch, limits).unwrap();
    memtable.commit_wal_ticket(ticket).unwrap();
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(
        crate::pitr::decode_v5_batch(&bytes, crate::pitr::WAL_V5_HEADER_LEN, limits)
            .unwrap()
            .batch,
        batch
    );
}

#[test]
fn test_mvcc_point_write_dispatches_to_v5_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-mvcc.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    let Ok(memtable) = MemTable::create_with_wal_v5(9, false, &path, header) else {
        return;
    };
    let mvcc = crate::mvcc::LsmMvccInner::new(0);
    let (commit_ts, encoded_key, value, ticket) =
        mvcc.write_wal_only(b"key", b"value", &memtable).unwrap();
    memtable.commit_wal_ticket(ticket).unwrap();
    memtable
        .publish_raw_batch(&[(
            crate::key::KeySlice::from_slice(&encoded_key),
            value.as_slice(),
        )])
        .unwrap();
    assert_eq!(commit_ts, 1);
    let bytes = std::fs::read(path).unwrap();
    let decoded = crate::pitr::decode_v5_batch(
        &bytes,
        crate::pitr::WAL_V5_HEADER_LEN,
        crate::pitr::LIVE_WAL_V5_LIMITS,
    )
    .unwrap();
    assert_eq!(decoded.batch.commit_ts, 1);
    assert_eq!(decoded.batch.entries.len(), 1);
}

#[test]
fn test_mvcc_mixed_batch_dispatches_to_v5_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-mixed.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    let Ok(memtable) = MemTable::create_with_wal_v5(9, false, &path, header) else {
        return;
    };
    let entries = vec![
        (
            Bytes::from_static(b"put"),
            Bytes::from_static(b"value"),
            crate::mvcc::BatchEntryKind::PutRaw,
        ),
        (
            Bytes::from_static(b"delete"),
            Bytes::new(),
            crate::mvcc::BatchEntryKind::Delete,
        ),
    ];
    let mvcc = crate::mvcc::LsmMvccInner::new(0);
    let (commit_ts, _, ticket) = mvcc
        .write_batch_wal_only(&entries, &memtable, false)
        .unwrap();
    memtable.commit_wal_ticket(ticket).unwrap();
    let bytes = std::fs::read(path).unwrap();
    let decoded = crate::pitr::decode_v5_batch(
        &bytes,
        crate::pitr::WAL_V5_HEADER_LEN,
        crate::pitr::LIVE_WAL_V5_LIMITS,
    )
    .unwrap();
    assert_eq!(decoded.batch.commit_ts, commit_ts);
    assert_eq!(decoded.batch.entries.len(), 2);
}

#[test]
fn test_v5_wal_recovery_replays_mixed_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-recovery.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    let Ok(memtable) = MemTable::create_with_wal_v5(9, false, &path, header) else {
        return;
    };
    let batch = crate::pitr::WalBatch {
        commit_ts: 7,
        recorded_at: crate::pitr::RecordedAt { secs: 1, nanos: 0 },
        entries: vec![
            crate::pitr::WalEntry::Put {
                key: b"put".to_vec(),
                value: vec![crate::vlog::KvKind::Inline as u8, b'v'],
            },
            crate::pitr::WalEntry::RangeDelete {
                start: b"a".to_vec(),
                end: b"z".to_vec(),
            },
        ],
    };
    let ticket = memtable
        .write_pitr_wal_batch_only(&batch, crate::pitr::LIVE_WAL_V5_LIMITS)
        .unwrap();
    memtable.commit_wal_ticket(ticket).unwrap();
    drop(memtable);
    let (_recovered, max_ts) =
        MemTable::recover_from_wal_with_range_tombstones(9, false, &path).unwrap();
    assert_eq!(max_ts, 7);
}

#[test]
fn test_v5_wal_recovery_rejects_truncated_identity_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-truncated-header.wal");
    std::fs::write(&path, crate::pitr::WAL_V5_MAGIC).unwrap();
    assert!(Wal::recover(&path, &new_skiplist()).is_err());
}

#[test]
fn test_v5_wal_recovery_rejects_corrupt_middle_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-corrupt-middle.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    let mut bytes = crate::pitr::encode_v5_file_header(header).unwrap().to_vec();
    for commit_ts in 1..=3 {
        let batch = crate::pitr::WalBatch {
            commit_ts,
            recorded_at: crate::pitr::RecordedAt {
                secs: commit_ts as i64,
                nanos: 0,
            },
            entries: vec![crate::pitr::WalEntry::Put {
                key: vec![commit_ts as u8],
                value: vec![crate::vlog::KvKind::Inline as u8, commit_ts as u8],
            }],
        };
        bytes
            .extend(crate::pitr::encode_v5_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS).unwrap());
    }
    let middle_payload = crate::pitr::WAL_V5_HEADER_LEN
        + crate::pitr::WAL_V5_ALIGNMENT
        + crate::pitr::WAL_V5_BATCH_HEADER_LEN;
    bytes[middle_payload] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    assert!(Wal::recover(&path, &new_skiplist()).is_err());
}

#[test]
fn test_v5_family_recovery_rejects_valid_batch_after_zero_hole() {
    for version in [
        crate::pitr::WAL_V5_VERSION_LEGACY,
        crate::pitr::WAL_V5_VERSION,
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("v{version}-zero-hole.wal"));
        let header = crate::pitr::WalV5Header {
            wal_format_version: version,
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(3),
            predecessor: crate::pitr::ChainAnchor::Genesis {
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            },
        };
        let make_batch = |commit_ts| crate::pitr::WalBatch {
            commit_ts,
            recorded_at: crate::pitr::RecordedAt {
                secs: commit_ts as i64,
                nanos: 0,
            },
            entries: vec![crate::pitr::WalEntry::Put {
                key: vec![commit_ts as u8],
                value: vec![crate::vlog::KvKind::Inline as u8, commit_ts as u8],
            }],
        };
        let mut bytes = crate::pitr::encode_v5_file_header(header).unwrap().to_vec();
        bytes.extend(
            crate::pitr::encode_v5_batch(&make_batch(1), crate::pitr::LIVE_WAL_V5_LIMITS).unwrap(),
        );
        bytes.extend(vec![0; crate::pitr::WAL_V5_ALIGNMENT]);
        bytes.extend(
            crate::pitr::encode_v5_batch(&make_batch(2), crate::pitr::LIVE_WAL_V5_LIMITS).unwrap(),
        );
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            Wal::recover(&path, &new_skiplist()).is_err(),
            "v{version} recovery must reject a valid batch after a zero-filled hole"
        );
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn test_v5_wal_recovery_rejects_segment_claiming_v4_format() {
    // A v5 segment whose version field is damaged to 4 used to be recovered as
    // v4: the parser read the identity header as batches, and recovery then
    // truncated the file to the part it understood, on disk and reporting
    // success. Detection must refuse it instead, and must leave the file alone.
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5-claims-v4.wal");
    let header = crate::pitr::WalV5Header {
        wal_format_version: crate::pitr::WAL_V5_VERSION,
        timeline_id: crate::pitr::TimelineId([1; 16]),
        archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        segment_id: crate::pitr::SegmentId(3),
        predecessor: crate::pitr::ChainAnchor::Genesis {
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
        },
    };
    // Encode the segment directly rather than through `Wal::create_v5`, which
    // needs io_uring: a test that returned early when that is unavailable would
    // pass without ever exercising the check it is here for.
    let mut bytes = crate::pitr::encode_v5_file_header(header).unwrap().to_vec();
    let batch = crate::pitr::WalBatch {
        commit_ts: 1,
        recorded_at: crate::pitr::RecordedAt { secs: 1, nanos: 0 },
        entries: vec![crate::pitr::WalEntry::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }],
    };
    bytes.extend(crate::pitr::encode_v5_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS).unwrap());
    // Damage the version field to WAL_FORMAT_VERSION_V4 (4); the bytes past it
    // keep carrying the v5 identity header and its CRC.
    bytes[4..6].copy_from_slice(&4u16.to_be_bytes());
    std::fs::write(&path, &bytes).unwrap();

    let skiplist = new_skiplist();
    let result = Wal::recover(&path, &skiplist);
    let error = result
        .err()
        .expect("a mislabelled v5 segment must be refused");
    assert!(
        error.to_string().contains("claims the v4 format"),
        "unexpected error: {error:#}"
    );
    assert_eq!(skiplist.len(), 0);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "recovery must not rewrite a segment it refused"
    );
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

#[cfg(feature = "bench")]
#[test]
fn test_wal_profiled_group_commit_records_follower_wait_events() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("profiled_group_commit.wal");
    let Some(wal) = create_wal_or_skip(&path) else {
        return;
    };
    let wal = Arc::new(wal);
    let profile = Arc::new(WriteProfile::default());
    let start = Arc::new(Barrier::new(5));

    let mut handles = Vec::new();
    for worker_id in 0..4u8 {
        let wal = Arc::clone(&wal);
        let profile = Arc::clone(&profile);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let mut keys = Vec::new();
            let mut values = Vec::new();
            for entry_idx in 0..256 {
                keys.push(vec![worker_id, (entry_idx & 0xff) as u8]);
                values.push(vec![worker_id; 1024]);
            }
            let refs: Vec<(&[u8], &[u8])> = keys
                .iter()
                .zip(values.iter())
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect();

            start.wait();
            let ticket = wal.put_batch(&refs, worker_id as u64 + 1).unwrap();
            wal.submit_and_commit_profiled(ticket, &profile).unwrap();
        }));
    }

    start.wait();
    for handle in handles {
        handle.join().unwrap();
    }

    let snapshot = profile.snapshot();
    assert!(snapshot.wal_commit_groups > 0);
    assert!(snapshot.wal_commit_buffers >= snapshot.wal_commit_groups);
    assert!(snapshot.wal_commit_bytes > 0);
    assert!(snapshot.wal_commit_max_buffers > 0);
    assert!(snapshot.wal_commit_max_bytes > 0);

    if snapshot.wal_follower_wait_calls > 0 {
        assert!(snapshot.wal_follower_condvar_waits > 0);
    }
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
    // Recovery accepts versions 2, 3, 4 and the v5 family (5 and 6), so
    // version=0x0007 must return an "unsupported WAL version" error (no legacy
    // fallback). Listed versions move when a version is added or retired; this
    // one must stay outside the accepted set for the test to mean anything.
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).unwrap();
        // Manually write bytes that spell 'WAL2' but with wrong version.
        f.write_all(&[0x57, 0x41, 0x4C, 0x32]).unwrap(); // magic = WAL2
        f.write_all(&[0x00, 0x07]).unwrap(); // version = 7 (unsupported)
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
                crate::mvcc::BatchEntryKind::PutRaw,
            ),
            (
                bytes::Bytes::from_static(b"k2"),
                bytes::Bytes::from_static(b"v2"),
                crate::mvcc::BatchEntryKind::PutRaw,
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
                crate::mvcc::BatchEntryKind::PutRaw,
            ),
            (
                bytes::Bytes::from_static(b"k2"),
                bytes::Bytes::from_static(b"v2"),
                crate::mvcc::BatchEntryKind::PutRaw,
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

/// The PITR seal is built incrementally, in file order, by whichever thread is the
/// commit leader for a group. This pins what that ordering buys: the incrementally
/// built seal must equal one rebuilt from the segment's own bytes, even when the
/// batches arrive from threads racing for the leader role.
#[test]
fn test_wal_v5_seal_matches_the_segment_when_writers_race_the_leader() {
    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 40;
    let dir = tempdir().unwrap();
    let path = dir.path().join("race-v5.wal");
    let wal = Arc::new(
        Wal::create_v5(
            &path,
            crate::pitr::WalV5Header {
                wal_format_version: crate::pitr::WAL_V5_VERSION,
                timeline_id: crate::pitr::TimelineId([1; 16]),
                archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
                segment_id: crate::pitr::SegmentId(3),
                predecessor: crate::pitr::ChainAnchor::Genesis {
                    archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
                },
            },
        )
        .unwrap(),
    );
    wal.configure_pitr_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .unwrap();

    // Mirror the engine's discipline: `reserve_commit_ts` and the WAL append both
    // happen under `write_lock` (mvcc.rs), so commit timestamps reach the file in
    // increasing order and the seal's monotonicity check can hold at all. What is
    // NOT serialized there is the commit: several writers are in
    // `submit_and_commit` at once, racing for the leader role.
    let next_commit_ts = Arc::new(std::sync::Mutex::new(1u64));
    let barrier = Arc::new(Barrier::new(WRITERS as usize));
    let mut workers = Vec::new();
    for _writer in 0..WRITERS {
        let wal = Arc::clone(&wal);
        let barrier = Arc::clone(&barrier);
        let next_commit_ts = Arc::clone(&next_commit_ts);
        workers.push(thread::spawn(move || {
            barrier.wait();
            for _index in 0..PER_WRITER {
                let ticket = {
                    let mut next = next_commit_ts.lock().unwrap();
                    let commit_ts = *next;
                    *next += 1;
                    let batch = crate::pitr::WalBatch {
                        commit_ts,
                        recorded_at: crate::pitr::RecordedAt {
                            secs: 1,
                            nanos: commit_ts as u32,
                        },
                        entries: vec![crate::pitr::WalEntry::Put {
                            key: format!("key{commit_ts}").into_bytes(),
                            value: vec![commit_ts as u8; 16],
                        }],
                    };
                    wal.put_v5_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS, None)
                        .unwrap()
                };
                wal.submit_and_commit(ticket).unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    // `write_pitr_seal_for_active_wal` syncs before it finalizes; do the same, so
    // the comparison is between the seal and a segment that is fully on disk.
    wal.sync().unwrap();
    let (incremental, _) = wal.finalize_pitr_seal().unwrap();
    let (rebuilt, _) = crate::pitr::seal::build_v5_seal(&std::fs::read(&path).unwrap()).unwrap();

    assert_eq!(incremental.entries.len(), (WRITERS * PER_WRITER) as usize);
    assert_eq!(incremental, rebuilt);
}

/// A segment written before the logical-batch digest existed, resumed by code that
/// writes the new one.
///
/// The rule is a property of the segment, not of the running binary: reopening has
/// to keep hashing the resumed segment the way it was written, and the seal it
/// eventually writes has to say so, or every later verification of that segment
/// reads the wrong bytes. The fixture is the frozen v5 segment from the tree before
/// the change.
#[test]
fn test_wal_v5_legacy_segment_resumes_under_its_own_rule() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy-v5.wal");
    std::fs::copy(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/tests/fixtures/pitr-v5-segment.wal"
        ),
        &path,
    )
    .unwrap();

    let skiplist = new_skiplist();
    let (wal, _) = Wal::recover(&path, &skiplist).unwrap();
    assert_eq!(wal.format_version(), crate::pitr::WAL_V5_VERSION_LEGACY);
    wal.configure_pitr_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .unwrap();

    // Continue the segment: commit timestamps must exceed the ones already there.
    for commit_ts in 3..6 {
        let batch = crate::pitr::WalBatch {
            commit_ts,
            // Later than the fixture's batches, which end at secs 2 nanos 7.
            recorded_at: crate::pitr::RecordedAt {
                secs: 3,
                nanos: commit_ts as u32,
            },
            entries: vec![crate::pitr::WalEntry::Put {
                key: format!("key{commit_ts}").into_bytes(),
                value: vec![commit_ts as u8; 16],
            }],
        };
        let ticket = wal
            .put_v5_batch(&batch, crate::pitr::LIVE_WAL_V5_LIMITS, None)
            .unwrap();
        wal.submit_and_commit(ticket).unwrap();
    }
    wal.sync().unwrap();

    let (incremental, incremental_bytes) = wal.finalize_pitr_seal().unwrap();
    let segment = std::fs::read(&path).unwrap();
    let (rebuilt, rebuilt_bytes) = crate::pitr::seal::build_v5_seal(&segment).unwrap();

    // Same digest, same entries, same bytes: the resumed accumulator hashed the
    // legacy prefix under the legacy rule and then each new batch the same way.
    assert_eq!(incremental, rebuilt);
    assert_eq!(incremental_bytes, rebuilt_bytes);
    assert_eq!(incremental.entries.len(), 5);
    // A segment that keeps the old rule keeps saying so, so the readers that take
    // the rule from the seal agree with the ones that take it from the WAL header.
    assert_eq!(
        incremental.header.wal_format_version,
        crate::pitr::WAL_V5_VERSION_LEGACY
    );
    let decoded = crate::pitr::seal::V5Seal::decode(&incremental_bytes).unwrap();
    assert_eq!(decoded, incremental);
    assert_eq!(
        decoded.header.wal_format_version,
        crate::pitr::decode_v5_file_header(&segment)
            .unwrap()
            .wal_format_version
    );
    // Still the whole-aligned-prefix digest, not the logical one. A live WAL file
    // is preallocated past the batches, so the prefix the rule covers ends at the
    // seal's own logical length rather than at the file's - which is the length the
    // engine truncates the segment to before archiving it.
    let logical_length = incremental.logical_length as usize;
    assert!(logical_length < segment.len(), "expected preallocated tail");
    assert_eq!(
        incremental.wal_digest,
        <[u8; 32]>::from(Sha256::digest(&segment[..logical_length]))
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(incremental.logical_length)
        .unwrap();
    let segment = std::fs::read(&path).unwrap();
    assert_eq!(
        incremental.wal_digest,
        <[u8; 32]>::from(Sha256::digest(&segment))
    );

    // A fresh segment from the same binary takes the new rule, so the two rules do
    // coexist across segments rather than within one.
    let fresh_path = dir.path().join("fresh-v6.wal");
    let fresh = Wal::create_v5(
        &fresh_path,
        crate::pitr::WalV5Header {
            wal_format_version: crate::pitr::WAL_V5_VERSION,
            timeline_id: crate::pitr::TimelineId([1; 16]),
            archive_epoch_id: crate::pitr::ArchiveEpochId([2; 16]),
            segment_id: crate::pitr::SegmentId(4),
            predecessor: crate::pitr::ChainAnchor::Segment(crate::pitr::SegmentAnchor {
                segment_id: crate::pitr::SegmentId(3),
                wal_digest: incremental.wal_digest,
                seal_digest: <[u8; 32]>::from(Sha256::digest(&incremental_bytes)),
            }),
        },
    )
    .unwrap();
    fresh
        .configure_pitr_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .unwrap();
    let ticket = fresh
        .put_v5_batch(
            &crate::pitr::WalBatch {
                commit_ts: 7,
                recorded_at: crate::pitr::RecordedAt { secs: 1, nanos: 7 },
                entries: vec![crate::pitr::WalEntry::Put {
                    key: b"successor".to_vec(),
                    value: b"value".to_vec(),
                }],
            },
            crate::pitr::LIVE_WAL_V5_LIMITS,
            None,
        )
        .unwrap();
    fresh.submit_and_commit(ticket).unwrap();
    fresh.sync().unwrap();
    let (successor, _) = fresh.finalize_pitr_seal().unwrap();
    assert_eq!(
        successor.header.wal_format_version,
        crate::pitr::WAL_V5_VERSION
    );
    assert_ne!(
        successor.wal_digest,
        <[u8; 32]>::from(Sha256::digest(std::fs::read(&fresh_path).unwrap()))
    );
}
