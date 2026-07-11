//! Tests for the async API surface (RFC 014 Phase 1).
//!
//! Verifies Send contracts, cancellation safety, close semantics,
//! and scan ReadGuard retention.

use std::sync::Arc;
use std::sync::OnceLock;

use bytes::Bytes;
use tempfile::TempDir;

use crate::{
    compact::{CompactionOptions, SimpleLeveledCompactionOptions},
    iterators::StorageIterator,
    lsm_storage::{CacheAdmission, KvEngine, LsmStorageOptions, ParallelScanOptions},
    tests::harness::generate_sst,
    vlog::ValueSeparationOptions,
};

fn value_separation_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| parking_lot::Mutex::new(())).lock()
}

fn compaction_parallel_scan_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| parking_lot::Mutex::new(())).lock()
}

fn collect_parallel_rows(
    scan: &mut crate::lsm_storage::ParallelScan,
) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
    let mut rows = Vec::new();
    while let Some(chunk) = crate::future_ext::block_on(scan.try_next_chunk())? {
        rows.extend(chunk.into_rows());
    }
    Ok(rows)
}

fn seeded_parallel_scan_engine() -> (TempDir, Arc<KvEngine>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = KvEngine::open(
        dir.path(),
        LsmStorageOptions {
            compaction_options: CompactionOptions::NoCompaction,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .expect("open");

    let mut snapshot = engine.inner.state.load().as_ref().clone();
    for shard in 0..4u32 {
        let sst_id = engine.inner.next_sst_id();
        let data = (0..256u32)
            .map(|i| {
                let key = Bytes::from(format!("k{shard:02}-{i:03}"));
                let value = Bytes::from(format!(
                    "value-{shard:02}-{i:03}-payload-{:0>96}",
                    shard * 1_000 + i
                ));
                (key, value)
            })
            .collect::<Vec<_>>();
        let sst = Arc::new(generate_sst(
            sst_id,
            engine.inner.path_of_sst(sst_id),
            data,
            Some(engine.inner.block_cache.clone()),
        ));
        snapshot.levels[0].1.push(sst_id);
        snapshot.sstables.insert(sst_id, sst);
    }
    engine.inner.state.store(Arc::new(snapshot));
    (dir, engine)
}

fn seeded_parallel_prefix_scan_engine() -> (TempDir, Arc<KvEngine>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = KvEngine::open(
        dir.path(),
        LsmStorageOptions {
            compaction_options: CompactionOptions::NoCompaction,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .expect("open");

    let mut snapshot = engine.inner.state.load().as_ref().clone();
    for shard in 0..4u32 {
        let sst_id = engine.inner.next_sst_id();
        let data = (0..256u32)
            .map(|i| {
                let user_prefix = if shard % 2 == 0 { "user:" } else { "other:" };
                let key = Bytes::from(format!("{user_prefix}{shard:02}-{i:03}"));
                let value = Bytes::from(format!(
                    "value-{shard:02}-{i:03}-payload-{:0>96}",
                    shard * 1_000 + i
                ));
                (key, value)
            })
            .collect::<Vec<_>>();
        let sst = Arc::new(generate_sst(
            sst_id,
            engine.inner.path_of_sst(sst_id),
            data,
            Some(engine.inner.block_cache.clone()),
        ));
        snapshot.levels[0].1.push(sst_id);
        snapshot.sstables.insert(sst_id, sst);
    }
    engine.inner.state.store(Arc::new(snapshot));
    (dir, engine)
}

// ── Compile-time Send checks (RFC 014 §15 item 12) ──────────────

#[test]
fn async_scan_is_send() {
    static_assertions::assert_impl_all!(crate::lsm_storage::AsyncScan: Send);
}

#[test]
fn kv_engine_is_send_sync() {
    static_assertions::assert_impl_all!(KvEngine: Send, Sync);
}

// ── Basic async round-trip ──────────────────────────────────────

#[test]
fn async_open_put_get_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.put_async(b"hello", b"world")).expect("put");
    let val = crate::future_ext::block_on(engine.get_async(b"hello")).expect("get");
    assert_eq!(val.as_deref(), Some(b"world".as_ref()));

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn async_batch_put_and_get() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    for i in 0..10u32 {
        crate::future_ext::block_on(
            engine.put_async(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()),
        )
        .expect("put");
    }

    for i in 0..10u32 {
        let val = crate::future_ext::block_on(engine.get_async(format!("k{i:04}").as_bytes()))
            .expect("get");
        assert_eq!(val.as_deref(), Some(format!("v{i:04}").as_bytes()));
    }

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn async_delete_and_delete_range() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.put_async(b"a", b"1")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"b", b"2")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"c", b"3")).expect("put");

    crate::future_ext::block_on(engine.delete_async(b"b")).expect("delete");
    assert!(
        crate::future_ext::block_on(engine.get_async(b"b"))
            .expect("get")
            .is_none()
    );
    assert!(
        crate::future_ext::block_on(engine.get_async(b"a"))
            .expect("get")
            .is_some()
    );

    crate::future_ext::block_on(engine.delete_range_async(b"a", b"c")).expect("delete_range");
    assert!(
        crate::future_ext::block_on(engine.get_async(b"a"))
            .expect("get")
            .is_none()
    );

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn async_write_batch() {
    use crate::lsm_storage::WriteBatchRecord;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    let batch: Vec<WriteBatchRecord<Vec<u8>>> = vec![
        WriteBatchRecord::Put(b"k1".to_vec(), b"v1".to_vec()),
        WriteBatchRecord::Put(b"k2".to_vec(), b"v2".to_vec()),
    ];
    crate::future_ext::block_on(engine.write_batch_async(&batch)).expect("write_batch");

    assert!(
        crate::future_ext::block_on(engine.get_async(b"k1"))
            .expect("get")
            .is_some()
    );
    assert!(
        crate::future_ext::block_on(engine.get_async(b"k2"))
            .expect("get")
            .is_some()
    );

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

// ── Close semantics (RFC 014 §15 items 4, 5) ────────────────────

#[test]
fn close_async_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.close_async()).expect("first close");
    // Second close should be a no-op, not an error.
    crate::future_ext::block_on(engine.close_async()).expect("second close");
}

#[test]
fn closing_rejects_new_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(
        crate::future_ext::block_on(KvEngine::open_async(
            dir.path(),
            LsmStorageOptions::default_for_test(),
        ))
        .expect("open"),
    );

    let e2 = engine.clone();
    let handle = std::thread::spawn(move || {
        crate::future_ext::block_on(e2.close_async()).expect("close");
    });
    handle.join().expect("join");

    let err =
        crate::future_ext::block_on(engine.put_async(b"k", b"v")).expect_err("put after close");
    let msg = format!("{err}");
    assert!(
        msg.contains("closing") || msg.contains("closed"),
        "expected closing/closed error, got: {msg}"
    );
}

// ── Scan ReadGuard retention (RFC 014 §15 item 8) ───────────────

#[test]
fn async_scan_holds_read_guard_across_await() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    for i in 0..10u32 {
        crate::future_ext::block_on(
            engine.put_async(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()),
        )
        .expect("put");
    }

    {
        let mut scan = crate::future_ext::block_on(
            engine.scan_async(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        )
        .expect("scan");

        let mut count = 0;
        while let Some((_k, _v)) = crate::future_ext::block_on(scan.try_next()).expect("try_next") {
            count += 1;
        }
        assert_eq!(count, 10);
        // Drop scan (and its lifecycle guard) before closing
    }

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn parallel_scan_is_send() {
    static_assertions::assert_impl_all!(crate::lsm_storage::ParallelScan: Send);
}

#[test]
fn scan_parallel_async_matches_ordered_scan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    for i in 0..32u32 {
        crate::future_ext::block_on(
            engine.put_async(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()),
        )
        .expect("put");
    }

    let mut scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 3,
            batch_bytes: 64,
            yield_every_rows: 5,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");

    let items = collect_parallel_rows(&mut scan).expect("collect rows");

    let expected = (0..32u32)
        .map(|i| {
            (
                Bytes::from(format!("k{i:04}")),
                Bytes::from(format!("v{i:04}")),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(items, expected);

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn scan_parallel_async_try_next_batch_matches_rowwise() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    for i in 0..32u32 {
        crate::future_ext::block_on(
            engine.put_async(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()),
        )
        .expect("put");
    }

    let mut rowwise = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 4,
            batch_bytes: 64,
            yield_every_rows: 8,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");
    let expected = collect_parallel_rows(&mut rowwise).expect("collect rows");

    let mut batched = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 4,
            batch_bytes: 64,
            yield_every_rows: 8,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");
    let mut actual = Vec::new();
    while let Some(batch) =
        crate::future_ext::block_on(batched.try_next_batch()).expect("try_next_batch")
    {
        actual.extend(batch);
    }

    assert_eq!(actual, expected);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn scan_parallel_async_try_next_chunk_matches_rowwise() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    for i in 0..32u32 {
        crate::future_ext::block_on(
            engine.put_async(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()),
        )
        .expect("put");
    }

    let mut rowwise = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 4,
            batch_bytes: 64,
            yield_every_rows: 8,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");
    let expected = collect_parallel_rows(&mut rowwise).expect("collect rows");

    let mut chunked = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 4,
            batch_bytes: 64,
            yield_every_rows: 8,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");
    let mut actual = Vec::new();
    while let Some(chunk) =
        crate::future_ext::block_on(chunked.try_next_chunk()).expect("try_next_chunk")
    {
        actual.extend(chunk.into_rows());
    }

    assert_eq!(actual, expected);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn prefix_scan_parallel_async_filters_prefix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.put_async(b"user:01", b"a")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"user:02", b"b")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"tenant:01", b"c")).expect("put");

    let mut scan = crate::future_ext::block_on(engine.prefix_scan_parallel_async(
        b"user:",
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 1,
            batch_bytes: 64,
            yield_every_rows: 2,
            channel_capacity: 1,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("prefix parallel scan");

    let items = collect_parallel_rows(&mut scan).expect("collect rows");
    assert_eq!(
        items,
        vec![
            (Bytes::from("user:01"), Bytes::from("a")),
            (Bytes::from("user:02"), Bytes::from("b")),
        ]
    );

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn drop_parallel_scan_releases_scan_admission() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    for i in 0..256u32 {
        crate::future_ext::block_on(
            engine.put_async(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes()),
        )
        .expect("put");
    }

    let scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 1,
            batch_rows: 8,
            batch_bytes: 128,
            yield_every_rows: 16,
            channel_capacity: 1,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");
    drop(scan);

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn parallel_scan_planner_falls_back_when_memtable_or_l0_dominate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).expect("open");

    engine.put(b"a", b"1").expect("put");
    engine.put(b"b", b"2").expect("put");

    let shards = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );

    assert_eq!(shards.len(), 1);
    engine.close().expect("close");
}

#[test]
fn parallel_scan_planner_uses_multiple_l1_splits_after_compaction() {
    let _guard = compaction_parallel_scan_test_lock();
    let (_dir, engine) = seeded_parallel_scan_engine();

    let shards = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );
    let state = engine.inner.state.load();
    let level_counts = state
        .levels
        .iter()
        .map(|(level, ids)| format!("L{level}={}", ids.len()))
        .collect::<Vec<_>>()
        .join(", ");

    assert!(
        shards.len() > 1,
        "expected multiple shards after compaction, got {}; l0={}, memtable_empty={}, imm={}, levels=[{}]",
        shards.len(),
        state.l0_sstables.len(),
        state.memtable.is_empty(),
        state.imm_memtables.len(),
        level_counts,
    );
    engine.close().expect("close");
}

#[test]
fn scan_parallel_async_matches_sync_scan_on_multi_shard_plan() {
    let _guard = compaction_parallel_scan_test_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_compaction_test(CompactionOptions::Simple(
        SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 100,
            max_levels: 2,
        },
    ));
    opts.target_sst_size = 256;
    let engine = KvEngine::open(dir.path(), opts).expect("open");

    for batch in 0..12u32 {
        for i in 0..256u32 {
            let key = format!("k{batch:02}-{i:03}");
            let value = format!("value-{batch:02}-{i:03}-payload-{:0>96}", batch * 1_000 + i);
            engine.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        engine.force_flush().expect("force_flush");
    }
    engine.drain_flush().expect("drain_flush");
    engine
        .force_full_compaction()
        .expect("force_full_compaction");

    let planned = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );
    assert!(planned.len() > 1, "expected multi-shard plan");

    let mut expected = Vec::new();
    let mut sync_scan = engine
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .expect("sync scan");
    while sync_scan.is_valid() {
        expected.push((
            Bytes::copy_from_slice(sync_scan.key()),
            Bytes::from(sync_scan.value().to_vec()),
        ));
        sync_scan.next().expect("sync next");
    }

    let mut parallel_scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");

    let actual = collect_parallel_rows(&mut parallel_scan).expect("collect rows");

    if actual != expected {
        let mismatch = actual
            .iter()
            .zip(expected.iter())
            .position(|(a, e)| a != e)
            .unwrap_or_else(|| actual.len().min(expected.len()));
        panic!(
            "multi-shard scan mismatch at index {mismatch}; actual_len={}, expected_len={}, actual={:?}, expected={:?}",
            actual.len(),
            expected.len(),
            actual.get(mismatch),
            expected.get(mismatch),
        );
    }
    engine.close().expect("close");
}

#[test]
fn prefix_scan_parallel_async_matches_sync_scan_on_multi_shard_plan() {
    let _guard = compaction_parallel_scan_test_lock();
    let (_dir, engine) = seeded_parallel_prefix_scan_engine();

    let planned = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Included(b"user:"),
        std::ops::Bound::Excluded(b"user;"),
        Some(b"user:"),
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );
    assert!(planned.len() > 1, "expected multi-shard plan for prefix");

    let mut expected = Vec::new();
    let mut sync_scan = engine.prefix_scan(b"user:").expect("sync prefix scan");
    while sync_scan.is_valid() {
        expected.push((
            Bytes::copy_from_slice(sync_scan.key()),
            Bytes::from(sync_scan.value().to_vec()),
        ));
        sync_scan.next().expect("sync next");
    }

    let mut parallel_scan = crate::future_ext::block_on(engine.prefix_scan_parallel_async(
        b"user:",
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel prefix scan");

    let actual = collect_parallel_rows(&mut parallel_scan).expect("collect rows");

    assert_eq!(actual, expected);
    engine.close().expect("close");
}

#[test]
fn parallel_scan_empty_range_returns_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.put_async(b"a", b"1")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"b", b"2")).expect("put");

    let mut scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Included(b"z"),
        std::ops::Bound::Unbounded,
        ParallelScanOptions::default(),
    ))
    .expect("parallel scan");

    assert!(
        crate::future_ext::block_on(scan.try_next_chunk())
            .expect("try_next_chunk")
            .is_none()
    );

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn parallel_scan_split_boundary_key_is_returned_once() {
    let _guard = compaction_parallel_scan_test_lock();
    let (_dir, engine) = seeded_parallel_scan_engine();

    let planned = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );
    assert!(planned.len() > 1, "expected multi-shard plan");

    let split_key = match &planned[0].upper {
        std::ops::Bound::Excluded(k) => k.clone(),
        other => panic!("expected first split to be excluded upper bound, got {other:?}"),
    };

    let mut parallel_scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");

    let mut seen = 0usize;
    for (k, _v) in collect_parallel_rows(&mut parallel_scan).expect("collect rows") {
        if k == split_key {
            seen += 1;
        }
    }

    assert_eq!(seen, 1, "split-boundary key should appear exactly once");
    engine.close().expect("close");
}

#[test]
fn scan_parallel_async_matches_sync_scan_with_range_tombstones() {
    let _guard = compaction_parallel_scan_test_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_compaction_test(CompactionOptions::Simple(
        SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 100,
            max_levels: 2,
        },
    ));
    opts.target_sst_size = 256;
    let engine = KvEngine::open(dir.path(), opts).expect("open");

    for batch in 0..12u32 {
        for i in 0..256u32 {
            let key = format!("k{batch:02}-{i:03}");
            let value = format!("value-{batch:02}-{i:03}-payload-{:0>96}", batch * 1_000 + i);
            engine.put(key.as_bytes(), value.as_bytes()).expect("put");
        }
        engine.force_flush().expect("force_flush");
    }
    engine.drain_flush().expect("drain_flush");

    engine
        .delete_range(b"k02-050", b"k05-120")
        .expect("delete_range");
    engine.force_flush().expect("force_flush");
    engine.drain_flush().expect("drain_flush");
    engine
        .force_full_compaction()
        .expect("force_full_compaction");

    let planned = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );
    assert!(planned.len() > 1, "expected multi-shard plan");

    let mut expected = Vec::new();
    let mut sync_scan = engine
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .expect("sync scan");
    while sync_scan.is_valid() {
        expected.push((
            Bytes::copy_from_slice(sync_scan.key()),
            Bytes::from(sync_scan.value().to_vec()),
        ));
        sync_scan.next().expect("sync next");
    }

    let mut parallel_scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");

    let actual = collect_parallel_rows(&mut parallel_scan).expect("collect rows");

    assert_eq!(actual, expected);
    engine.close().expect("close");
}

#[test]
fn scan_parallel_async_matches_sync_scan_with_value_separation() {
    let _guard = compaction_parallel_scan_test_lock();
    let _vlog_guard = value_separation_test_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_compaction_test(CompactionOptions::Simple(
        SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 100,
            max_levels: 2,
        },
    ));
    opts.target_sst_size = 256;
    opts.value_separation = Some(ValueSeparationOptions {
        enabled: true,
        min_value_size: 16,
        ..Default::default()
    });
    let engine = KvEngine::open(dir.path(), opts).expect("open");

    for batch in 0..8u32 {
        for i in 0..192u32 {
            let key = format!("k{batch:02}-{i:03}");
            let value = if i % 2 == 0 {
                format!("small-{batch:02}-{i:03}").into_bytes()
            } else {
                format!("large-{batch:02}-{i:03}-{:0>96}", batch * 1_000 + i).into_bytes()
            };
            engine.put(key.as_bytes(), &value).expect("put");
        }
        engine.force_flush().expect("force_flush");
    }
    engine.drain_flush().expect("drain_flush");
    engine
        .force_full_compaction()
        .expect("force_full_compaction");

    let planned = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
            ..ParallelScanOptions::default()
        },
    );
    assert!(planned.len() > 1, "expected multi-shard plan");

    let mut expected = Vec::new();
    let mut sync_scan = engine
        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .expect("sync scan");
    while sync_scan.is_valid() {
        expected.push((
            Bytes::copy_from_slice(sync_scan.key()),
            Bytes::from(sync_scan.value().to_vec()),
        ));
        sync_scan.next().expect("sync next");
    }

    let mut parallel_scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");

    let actual = collect_parallel_rows(&mut parallel_scan).expect("collect rows");

    assert_eq!(actual, expected);
    engine.close().expect("close");
}

#[test]
fn parallel_scan_surfaces_later_shard_error_in_order() {
    let _guard = compaction_parallel_scan_test_lock();
    let (_dir, engine) = seeded_parallel_scan_engine();

    let planned = engine.inner.plan_parallel_scan_shards(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        None,
        ParallelScanOptions {
            max_parallelism: 4,
            ..ParallelScanOptions::default()
        },
    );
    assert!(planned.len() > 1, "expected multi-shard plan");

    let first = &planned[0];
    let lower = match &first.lower {
        std::ops::Bound::Included(b) => std::ops::Bound::Included(b.as_ref()),
        std::ops::Bound::Excluded(b) => std::ops::Bound::Excluded(b.as_ref()),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    };
    let upper = match &first.upper {
        std::ops::Bound::Included(b) => std::ops::Bound::Included(b.as_ref()),
        std::ops::Bound::Excluded(b) => std::ops::Bound::Excluded(b.as_ref()),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    };

    let mut expected_first_shard = Vec::new();
    let mut sync_scan = engine.scan(lower, upper).expect("sync scan");
    while sync_scan.is_valid() {
        expected_first_shard.push((
            Bytes::copy_from_slice(sync_scan.key()),
            Bytes::from(sync_scan.value().to_vec()),
        ));
        sync_scan.next().expect("sync next");
    }

    let mut scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: Some(1),
        },
    ))
    .expect("parallel scan");

    let mut actual_first_shard = Vec::new();
    loop {
        match crate::future_ext::block_on(scan.try_next_chunk()) {
            Ok(Some(chunk)) => actual_first_shard.extend(chunk.into_rows()),
            Ok(None) => panic!("expected injected shard error"),
            Err(err) => {
                assert!(
                    format!("{err}").contains("injected parallel scan shard failure"),
                    "unexpected error: {err}"
                );
                break;
            }
        }
    }

    assert_eq!(actual_first_shard, expected_first_shard);
    engine.close().expect("close");
}

#[test]
fn parallel_scan_stats_track_single_shard_fallback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).expect("open");

    engine.put(b"a", b"1").expect("put");
    engine.put(b"b", b"22").expect("put");

    let mut scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            ..ParallelScanOptions::default()
        },
    ))
    .expect("parallel scan");

    let rows = collect_parallel_rows(&mut scan)
        .expect("collect rows")
        .len();
    assert_eq!(rows, 2);

    let stats = engine.parallel_scan_stats();
    assert_eq!(stats.planned_scans, 1);
    assert_eq!(stats.single_shard_fallback_scans, 1);
    assert_eq!(stats.total_shards_planned, 1);
    assert_eq!(stats.rows_emitted, 2);
    assert!(stats.bytes_emitted >= 5);
    engine.close().expect("close");
}

#[test]
fn parallel_scan_stats_track_multi_shard_execution() {
    let (_dir, engine) = seeded_parallel_scan_engine();

    let mut scan = crate::future_ext::block_on(engine.scan_parallel_async(
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        ParallelScanOptions {
            max_parallelism: 4,
            batch_rows: 16,
            batch_bytes: 1024,
            yield_every_rows: 32,
            channel_capacity: 2,
            cache_admission: CacheAdmission::Force,
            fail_shard: None,
        },
    ))
    .expect("parallel scan");

    let rows = collect_parallel_rows(&mut scan)
        .expect("collect rows")
        .len();

    let stats = engine.parallel_scan_stats();
    assert_eq!(stats.planned_scans, 1);
    assert_eq!(stats.single_shard_fallback_scans, 0);
    assert!(stats.total_shards_planned > 1);
    assert_eq!(stats.rows_emitted, rows as u64);
    assert!(stats.bytes_emitted > rows as u64);
    engine.close().expect("close");
}

// ── Cancellation safety: dropping a future mid-flight (RFC 014 §15 item 7) ──

#[test]
fn drop_put_future_does_not_publish_partial_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    // Drop the future before it completes — engine must remain consistent.
    let fut = engine.put_async(b"ghost", b"write");
    drop(fut);

    // Engine should still be usable.
    crate::future_ext::block_on(engine.put_async(b"real", b"data")).expect("put");
    let val = crate::future_ext::block_on(engine.get_async(b"real")).expect("get");
    assert_eq!(val.as_deref(), Some(b"data".as_ref()));

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn drop_get_future_does_not_corrupt_engine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.put_async(b"key", b"val")).expect("put");

    let fut = engine.get_async(b"key");
    drop(fut); // cancel the read

    // Subsequent read should still work.
    let val = crate::future_ext::block_on(engine.get_async(b"key")).expect("get");
    assert_eq!(val.as_deref(), Some(b"val".as_ref()));

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

// ── Concurrent close drains in-flight writes (RFC 014 §15 item 3) ──

#[test]
fn close_drains_in_flight_writes() {
    use std::sync::Barrier;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    let barrier = Arc::new(Barrier::new(2));
    let b = barrier.clone();
    let e = engine.clone();

    let handle = std::thread::spawn(move || {
        b.wait(); // signal ready, then close
        crate::future_ext::block_on(e.close_async()).expect("close");
    });

    barrier.wait();
    // Write while close is in progress — should either succeed or fail cleanly.
    let write_result = crate::future_ext::block_on(engine.put_async(b"concurrent", b"write"));

    handle.join().expect("join");

    // Either the write succeeded (admitted before close transitioned)
    // or it was rejected (already closing). Both are correct outcomes.
    match write_result {
        Ok(()) | Err(_) => {}
    }
}

// ── Concurrent close drains in-flight txns ──

#[test]
fn close_rejects_new_txns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(
        crate::future_ext::block_on(KvEngine::open_async(
            dir.path(),
            LsmStorageOptions::default_for_test(),
        ))
        .expect("open"),
    );

    let e2 = engine.clone();
    let handle = std::thread::spawn(move || {
        crate::future_ext::block_on(e2.close_async()).expect("close");
    });
    handle.join().expect("join");

    let result = engine.new_txn_async();
    assert!(result.is_err(), "new_txn after close should fail");
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("closing") || msg.contains("closed"),
        "expected closing/closed error, got: {msg}"
    );
}

// ── Blocking executor bounds (RFC 014 §15 item 1) ───────────────

#[test]
fn blocking_executor_enforces_concurrency_bound() {
    // Default bound is 512 — just verify the executor exists and works.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    let available = engine.inner.blocking.available_permits();
    assert!(
        available > 0,
        "blocking executor should have available permits"
    );

    crate::future_ext::block_on(engine.close_async()).expect("close");
}

// ── Drop without close (RFC 014 §15 item 6) ─────────────────────

#[test]
fn drop_without_close_does_not_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");

    crate::future_ext::block_on(engine.put_async(b"k", b"v")).expect("put");
    // Drop without calling close_async — must not panic.
    drop(engine);
}

// ── Sync migration: sync API still works ────────────────────────

#[test]
fn sync_api_still_works_alongside_async() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).expect("open");

    engine.put(b"sync_key", b"sync_val").expect("put");
    let val = engine.get(b"sync_key").expect("get");
    assert_eq!(val.as_deref(), Some(b"sync_val".as_ref()));

    // Async API should also work on the same engine.
    crate::future_ext::block_on(engine.put_async(b"async_key", b"async_val")).expect("put async");
    let val2 = crate::future_ext::block_on(engine.get_async(b"async_key")).expect("get async");
    assert_eq!(val2.as_deref(), Some(b"async_val".as_ref()));

    engine.close().expect("close");
}

// ── Transaction async methods ──────────────────────────────────────

#[test]
fn txn_commit_async_basic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"key1", b"val1").expect("put");
    txn.put(b"key2", b"val2").expect("put");
    crate::future_ext::block_on(txn.commit_async()).expect("commit");
    drop(txn);
    let val = crate::future_ext::block_on(engine.get_async(b"key1")).expect("get");
    assert_eq!(val.as_deref(), Some(b"val1".as_ref()));
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_get_async_falls_back_to_engine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    crate::future_ext::block_on(engine.put_async(b"base", b"engine_val")).expect("put");
    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"base", b"txn_val").expect("put");
    let val = crate::future_ext::block_on(txn.get_async(b"base")).expect("get_async");
    assert_eq!(val.as_deref(), Some(b"txn_val".as_ref()));
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_commit_async_is_send() {
    fn assert_send<T: Send>(_: T) {}

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    assert_send(txn.commit_async());
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_scan_async_is_send() {
    fn assert_send<T: Send>(_: T) {}

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    let scan = crate::future_ext::block_on(
        txn.scan_async(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
    )
    .expect("scan");
    assert_send(scan);
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_prefix_scan_async_is_send() {
    fn assert_send<T: Send>(_: T) {}

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    let scan = crate::future_ext::block_on(txn.prefix_scan_async(b"p")).expect("prefix_scan");
    assert_send(scan);
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_commit_async_already_committed_is_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"k", b"v").expect("put");
    crate::future_ext::block_on(txn.commit_async()).expect("first commit");
    let err = crate::future_ext::block_on(txn.commit_async()).expect_err("second commit");
    assert!(format!("{err}").contains("already committed"));
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_commit_async_read_only_is_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    crate::future_ext::block_on(txn.get_async(b"any")).expect("get");
    crate::future_ext::block_on(txn.commit_async()).expect("commit read-only");
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_commit_async_serializable_conflict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_test();
    opts.serializable = true;
    let engine = crate::future_ext::block_on(KvEngine::open_async(dir.path(), opts)).expect("open");
    let txn1 = engine.new_txn_async().expect("txn1");
    let txn2 = engine.new_txn_async().expect("txn2");
    txn1.put(b"shared", b"v1").expect("put");
    txn2.put(b"shared", b"v2").expect("put");
    crate::future_ext::block_on(txn2.get_async(b"shared")).expect("txn2 read");
    crate::future_ext::block_on(txn1.commit_async()).expect("txn1 commit");
    let err = crate::future_ext::block_on(txn2.commit_async()).expect_err("txn2 should conflict");
    assert!(format!("{err}").contains("serializable conflict"));
    drop(txn1);
    drop(txn2);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_commit_async_conflict_is_terminal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_test();
    opts.serializable = true;
    let engine = crate::future_ext::block_on(KvEngine::open_async(dir.path(), opts)).expect("open");
    let txn1 = engine.new_txn_async().expect("txn1");
    let txn2 = engine.new_txn_async().expect("txn2");
    txn1.put(b"shared", b"v1").expect("put");
    txn2.put(b"shared", b"v2").expect("put");
    crate::future_ext::block_on(txn2.get_async(b"shared")).expect("txn2 read");
    crate::future_ext::block_on(txn1.commit_async()).expect("txn1 commit");
    crate::future_ext::block_on(txn2.commit_async()).expect_err("txn2 should conflict");
    let err = crate::future_ext::block_on(txn2.commit_async()).expect_err("txn2 retry");
    assert!(format!("{err}").contains("already committed"));
    drop(txn1);
    drop(txn2);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_get_async_after_commit_does_not_mutate_read_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_test();
    opts.serializable = true;
    let engine = crate::future_ext::block_on(KvEngine::open_async(dir.path(), opts)).expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"k", b"v").expect("put");
    crate::future_ext::block_on(txn.commit_async()).expect("commit");
    let before = txn.read_set.as_ref().expect("read_set").lock().len();
    let err = crate::future_ext::block_on(txn.get_async(b"after")).expect_err("get after commit");
    let after = txn.read_set.as_ref().expect("read_set").lock().len();
    assert!(format!("{err}").contains("already committed"));
    assert_eq!(before, after);
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_scan_async_merges_local_and_engine_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    crate::future_ext::block_on(engine.put_async(b"a", b"engine_a")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"b", b"engine_b")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"c", b"engine_c")).expect("put");

    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"b", b"txn_b").expect("put");
    txn.delete(b"c").expect("delete");
    txn.put(b"d", b"txn_d").expect("put");

    let mut scan = crate::future_ext::block_on(
        txn.scan_async(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
    )
    .expect("scan");
    let mut items = Vec::new();
    while let Some((k, v)) = crate::future_ext::block_on(scan.try_next()).expect("try_next") {
        items.push((k, v));
    }
    assert_eq!(
        items,
        vec![
            (bytes::Bytes::from("a"), bytes::Bytes::from("engine_a")),
            (bytes::Bytes::from("b"), bytes::Bytes::from("txn_b")),
            (bytes::Bytes::from("d"), bytes::Bytes::from("txn_d")),
        ]
    );
    drop(scan);
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_prefix_scan_async_filters_prefix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    crate::future_ext::block_on(engine.put_async(b"aa", b"v1")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"ab", b"v2")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"ba", b"v3")).expect("put");

    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"ac", b"v4").expect("put");
    txn.delete(b"ab").expect("delete");

    let mut scan = crate::future_ext::block_on(txn.prefix_scan_async(b"a")).expect("prefix_scan");
    let mut items = Vec::new();
    while let Some((k, v)) = crate::future_ext::block_on(scan.try_next()).expect("try_next") {
        items.push((k, v));
    }
    assert_eq!(
        items,
        vec![
            (bytes::Bytes::from("aa"), bytes::Bytes::from("v1")),
            (bytes::Bytes::from("ac"), bytes::Bytes::from("v4")),
        ]
    );
    drop(scan);
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_prefix_scan_async_empty_prefix_matches_full_scan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    crate::future_ext::block_on(engine.put_async(b"aa", b"v1")).expect("put");
    crate::future_ext::block_on(engine.put_async(b"ba", b"v2")).expect("put");

    let txn = engine.new_txn_async().expect("new_txn");
    txn.put(b"ca", b"v3").expect("put");
    let mut scan = crate::future_ext::block_on(txn.prefix_scan_async(b"")).expect("prefix_scan");
    let mut items = Vec::new();
    while let Some((k, v)) = crate::future_ext::block_on(scan.try_next()).expect("try_next") {
        items.push((k, v));
    }
    assert_eq!(
        items,
        vec![
            (bytes::Bytes::from("aa"), bytes::Bytes::from("v1")),
            (bytes::Bytes::from("ba"), bytes::Bytes::from("v2")),
            (bytes::Bytes::from("ca"), bytes::Bytes::from("v3")),
        ]
    );
    drop(scan);
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

#[test]
fn txn_scan_async_rejects_serializable_transactions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = LsmStorageOptions::default_for_test();
    opts.serializable = true;
    let engine = crate::future_ext::block_on(KvEngine::open_async(dir.path(), opts)).expect("open");
    let txn = engine.new_txn_async().expect("new_txn");
    let err = match crate::future_ext::block_on(
        txn.scan_async(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
    ) {
        Ok(_) => panic!("serializable scan should fail"),
        Err(err) => err,
    };
    assert!(format!("{err}").contains("not supported for serializable transactions"));
    drop(txn);
    crate::future_ext::block_on(engine.close_async()).expect("close");
}

// ── Shutdown tests ─────────────────────────────────────────────────

#[test]
fn engine_drop_without_close_terminates_background_tasks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open");
    crate::future_ext::block_on(engine.put_async(b"k", b"v")).expect("put");
    drop(engine);
}

// ── Async open recovery tests ──────────────────────────────────────

/// Write data, close (triggering flush → SST), re-open with `open_async`,
/// and verify all keys are readable.  Exercises the three-phase async
/// recovery path.
#[test]
fn open_async_recovers_data_after_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    let n = 1000u32;

    // Create and populate the engine, then close to flush to SST.
    {
        let engine =
            KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).expect("open");
        for i in 0..n {
            engine
                .put(
                    format!("key-{i:05}").as_bytes(),
                    format!("value-{i:05}").as_bytes(),
                )
                .expect("put");
        }
        // Force flush so data lands in at least one SST.
        engine.force_flush().expect("force_flush");
        engine.close().expect("close");
    }

    // Re-open with async path — exercises Phase 1 → 2 → 3.
    let engine = crate::future_ext::block_on(KvEngine::open_async(
        dir.path(),
        LsmStorageOptions::default_for_test(),
    ))
    .expect("open_async");

    for i in 0..n {
        let val = crate::future_ext::block_on(engine.get_async(format!("key-{i:05}").as_bytes()))
            .expect("get_async");
        assert_eq!(
            val.as_deref(),
            Some(format!("value-{i:05}").as_bytes()),
            "mismatch at key {i}"
        );
    }

    crate::future_ext::block_on(engine.close_async()).expect("close_async");
}
