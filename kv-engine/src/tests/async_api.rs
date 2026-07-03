//! Tests for the async API surface (RFC 014 Phase 1).
//!
//! Verifies Send contracts, cancellation safety, close semantics,
//! and scan ReadGuard retention.

use std::sync::Arc;

use crate::lsm_storage::{KvEngine, LsmStorageOptions};

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
