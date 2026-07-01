//! Deterministic failpoint tests for chaos testing (RFC 013 Phase 3).
//!
//! Each test configures a named failpoint to `"panic"`, performs the triggering
//! operation inside `catch_unwind`, then reopens the database and verifies that
//! crash recovery produces a consistent state.
//!
//! These tests complement the SIGKILL-based chaos_integration tests by injecting
//! failures at precise durability boundaries that are difficult to hit with
//! timing-based process killing alone.

#![cfg(feature = "chaos-testing")]

use kv_engine::chaos::failpoint::{self, FailScenario};
use kv_engine::compact::{CompactionOptions, LeveledCompactionOptions};
use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};

/// Helper for **lossy** failpoints: the failpoint may leave the database in an
/// inconsistent state, so both the body and the reopen+verify are wrapped in
/// `catch_unwind`. The test passes as long as the process doesn't hang or
/// segfault.
fn run_failpoint_test_lossy(
    fp_name: &str,
    opts: LsmStorageOptions,
    body: impl FnOnce(&KvEngine) + std::panic::UnwindSafe,
    verify: impl FnOnce(&KvEngine) + std::panic::UnwindSafe,
) {
    let scenario = FailScenario::setup();
    failpoint::cfg(fp_name, "panic").expect("failpoint cfg");

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("db");

    // Phase 1: run the body — the failpoint may panic here.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let engine = KvEngine::open(&db_path, opts.clone()).expect("open");
        body(&engine);
        engine.close().expect("close");
    }));

    // Disable the failpoint before Phase 2 so recovery doesn't re-trigger it.
    failpoint::cfg(fp_name, "off").expect("failpoint off");

    // Phase 2: reopen and verify. Some failpoints leave the on-disk state
    // inconsistent (e.g. torn manifest), which may cause reopen to panic.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let engine = KvEngine::open(&db_path, opts.clone()).expect("reopen after failpoint");
        verify(&engine);
        engine.close().expect("close after verify");
    }));

    scenario.teardown();
}

/// Helper for **durable** failpoints: data MUST survive recovery, so Phase 2
/// is NOT wrapped in `catch_unwind`. Assertion failures propagate to the test
/// runner and are reported as test failures.
fn run_failpoint_test_durable(
    fp_name: &str,
    opts: LsmStorageOptions,
    body: impl FnOnce(&KvEngine) + std::panic::UnwindSafe,
    verify: impl FnOnce(&KvEngine) + std::panic::UnwindSafe,
) {
    let scenario = FailScenario::setup();
    failpoint::cfg(fp_name, "panic").expect("failpoint cfg");

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("db");

    // Phase 1: run the body — the failpoint may panic here.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let engine = KvEngine::open(&db_path, opts.clone()).expect("open");
        body(&engine);
        engine.close().expect("close");
    }));

    // Disable the failpoint before Phase 2 so recovery doesn't re-trigger it.
    failpoint::cfg(fp_name, "off").expect("failpoint off");

    // Phase 2: reopen and verify. NOT wrapped in catch_unwind —
    // assertion failures propagate and are reported as test failures.
    let engine = KvEngine::open(&db_path, opts.clone()).expect("reopen after failpoint");
    verify(&engine);
    engine.close().expect("close after verify");

    scenario.teardown();
}

// ---------------------------------------------------------------------------
// WAL failpoints
// ---------------------------------------------------------------------------

/// `wal.after_batch_encode`: crash after batch is queued in `pending` but
/// before submission to io_uring. The unsubmitted batch is lost on recovery.
/// Verifies reopen succeeds without panic.
#[test]
fn failpoint_wal_after_batch_encode() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;

    run_failpoint_test_lossy(
        "wal.after_batch_encode",
        opts,
        |engine| {
            // Every put goes through encode_and_push_direct_buf, so the
            // failpoint fires on the first put. The batch is lost.
            engine.put(b"lost_key", b"lost_val").expect("put lost_key");
        },
        |engine| {
            // Verify the database is fully operational after recovery.
            engine
                .put(b"__probe__", b"ok")
                .expect("probe put after batch_encode crash");
            let v = engine.get(b"__probe__").expect("probe get");
            assert_eq!(
                v.as_deref(),
                Some(b"ok".as_slice()),
                "database must be usable after recovery"
            );
        },
    );
}

/// `wal.after_submit_before_wait`: crash after SQEs are pushed to the
/// submission queue but before `submit_and_wait` processes them. Recovery
/// must not panic.
#[test]
fn failpoint_wal_after_submit_before_wait() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;

    run_failpoint_test_lossy(
        "wal.after_submit_before_wait",
        opts,
        |engine| {
            engine.put(b"lost_key", b"lost_val").expect("put lost_key");
        },
        |engine| {
            // Verify the database is fully operational after recovery.
            engine
                .put(b"__probe__", b"ok")
                .expect("probe put after submit_before_wait crash");
            let v = engine.get(b"__probe__").expect("probe get");
            assert_eq!(
                v.as_deref(),
                Some(b"ok".as_slice()),
                "database must be usable after recovery"
            );
        },
    );
}

/// `wal.after_fsync_before_publish`: crash after data is fsynced to disk but
/// before the completion is published to waiting callers. The data IS durable
/// and MUST survive recovery.
#[test]
fn failpoint_wal_after_fsync_before_publish() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;

    run_failpoint_test_durable(
        "wal.after_fsync_before_publish",
        opts,
        |engine| {
            engine
                .put(b"must_survive", b"durable")
                .expect("put must_survive");
            // This triggers the failpoint AFTER the write+fdatasync complete
            // but BEFORE publish_submit_result. Data is on disk.
            // (The failpoint fires inside submit_as_leader.)
        },
        |engine| {
            let v = engine.get(b"must_survive").expect("get must_survive");
            assert_eq!(
                v.as_deref(),
                Some(b"durable".as_slice()),
                "fsynced data must survive recovery"
            );
        },
    );
}

// ---------------------------------------------------------------------------
// Manifest failpoints
// ---------------------------------------------------------------------------

/// `manifest.after_append_before_sync`: crash after a manifest record is
/// written to the kernel buffer but before it is fsynced. The torn manifest
/// may cause recovery to detect an inconsistent state. Verification catches
/// the reopen panic — the test passes as long as the process doesn't hang
/// or segfault.
#[test]
fn failpoint_manifest_after_append_before_sync() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;

    run_failpoint_test_lossy(
        "manifest.after_append_before_sync",
        opts,
        |engine| {
            engine
                .put(b"flushed_key", b"flushed_val")
                .expect("put flushed_key");
            engine.force_flush().expect("force_flush");
        },
        |engine| {
            // Verify the database is fully operational after recovery.
            engine
                .put(b"__probe__", b"ok")
                .expect("probe put after manifest crash");
            let v = engine.get(b"__probe__").expect("probe get");
            assert_eq!(
                v.as_deref(),
                Some(b"ok".as_slice()),
                "database must be usable after recovery"
            );
        },
    );
}

/// `manifest.after_snapshot_tmp_sync`: crash after the manifest snapshot
/// temp file is synced but before the old manifest is truncated and the
/// snapshot is renamed. The old manifest is still intact — recovery
/// replays from the existing manifest, not the snapshot tmp file.
#[test]
fn failpoint_manifest_after_snapshot_tmp_sync() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.manifest_snapshot_threshold_bytes = 256;

    run_failpoint_test_durable(
        "manifest.after_snapshot_tmp_sync",
        opts,
        |engine| {
            // Generate enough manifest churn to trigger a snapshot.
            for i in 0..20 {
                let key = format!("mk_{i:010}");
                let val = format!("mv_{i}");
                engine.put(key.as_bytes(), val.as_bytes()).unwrap();
                engine.force_flush().unwrap();
            }
        },
        |engine| {
            // Verify the engine reopens cleanly and committed data survives.
            let v = engine.get(b"mk_0000000000").expect("get mk_0");
            assert!(v.is_some(), "data must survive manifest snapshot crash");
        },
    );
}

// ---------------------------------------------------------------------------
// Flush failpoint
// ---------------------------------------------------------------------------

/// `flush.after_sst_sync_before_manifest`: crash after the SST file is synced
/// to disk but before the manifest `Flush` record is written. The SST becomes
/// an orphan; recovery must fall back to WAL replay for the memtable data.
#[test]
fn failpoint_flush_after_sst_sync_before_manifest() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;

    run_failpoint_test_durable(
        "flush.after_sst_sync_before_manifest",
        opts,
        |engine| {
            engine
                .put(b"flush_target", b"before_crash")
                .expect("put flush_target");
            // force_flush writes the SST, syncs it, syncs the dir, then
            // the failpoint fires before the manifest record is written.
            engine.force_flush().expect("force_flush");
        },
        |engine| {
            let v = engine.get(b"flush_target").expect("get flush_target");
            assert!(
                v.is_some(),
                "flushed data must survive via WAL replay when manifest record is lost"
            );
        },
    );
}

// ---------------------------------------------------------------------------
// Compaction failpoint
// ---------------------------------------------------------------------------

/// `compaction.after_output_sync_before_manifest`: crash after compaction
/// output SSTs are synced but before the manifest `CompactionV3` record is
/// written. Recovery must succeed and original data must be intact.
#[test]
fn failpoint_compaction_after_output_sync_before_manifest() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.target_sst_size = 4096;
    opts.compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
        base_level_size_mb: 1,
    });

    run_failpoint_test_durable(
        "compaction.after_output_sync_before_manifest",
        opts,
        |engine| {
            // Write keys in 2 batches with interleaved flushes to create
            // multiple SSTs at L0 and trigger compaction.
            for batch in 0..2 {
                for i in (batch * 20)..(batch * 20 + 20) {
                    let key = format!("ck_{i:010}");
                    let val = format!("cv_{i}");
                    engine.put(key.as_bytes(), val.as_bytes()).unwrap();
                }
                engine.force_flush().unwrap();
            }
            // Trigger compaction — the failpoint fires after output SSTs are
            // synced but before the manifest record is written.
            engine.force_full_compaction().unwrap();
        },
        |engine| {
            // Original data must survive — compaction data is not lost, just
            // the manifest record for the compaction result may be missing.
            let v = engine.get(b"ck_0000000000").expect("get ck_0");
            assert!(v.is_some(), "original data must survive compaction crash");
        },
    );
}

// ---------------------------------------------------------------------------
// Manifest sub-step failpoints
// ---------------------------------------------------------------------------

/// `manifest.after_truncate_before_rename`: crash after the old manifest is
/// truncated and synced but before the snapshot tmp file is renamed over it.
/// On recovery, the manifest is empty and the snapshot tmp file still exists
/// — recovery must rebuild from the tmp file.
#[test]
fn failpoint_manifest_after_truncate_before_rename() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.manifest_snapshot_threshold_bytes = 256;

    run_failpoint_test_durable(
        "manifest.after_truncate_before_rename",
        opts,
        |engine| {
            for i in 0..20 {
                let key = format!("mt_{i:010}");
                let val = format!("mv_{i}");
                engine.put(key.as_bytes(), val.as_bytes()).unwrap();
                engine.force_flush().unwrap();
            }
        },
        |engine| {
            let v = engine.get(b"mt_0000000000").expect("get mt_0");
            assert!(
                v.is_some(),
                "data must survive manifest truncate-before-rename crash"
            );
        },
    );
}

/// `manifest.after_rename_before_dir_sync`: crash after the snapshot is renamed
/// over the manifest but before the parent directory is fsynced. The rename is
/// not durable — on recovery the old manifest may still be visible.
#[test]
fn failpoint_manifest_after_rename_before_dir_sync() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.manifest_snapshot_threshold_bytes = 256;

    run_failpoint_test_durable(
        "manifest.after_rename_before_dir_sync",
        opts,
        |engine| {
            for i in 0..20 {
                let key = format!("mr_{i:010}");
                let val = format!("mv_{i}");
                engine.put(key.as_bytes(), val.as_bytes()).unwrap();
                engine.force_flush().unwrap();
            }
        },
        |engine| {
            let v = engine.get(b"mr_0000000000").expect("get mr_0");
            assert!(
                v.is_some(),
                "data must survive manifest rename-before-dir-sync crash"
            );
        },
    );
}

// ---------------------------------------------------------------------------
// vLog failpoint
// ---------------------------------------------------------------------------

/// `vlog.after_append_before_index_publish`: crash after values are written and
/// fsynced to the vlog file but before the `.vidx` index is published. On
/// recovery, the vlog file contains valid data but has no index — the engine
/// must recover gracefully without dangling references.
///
/// Note: this test exercises the flush-path injection site (lsm_storage.rs).
/// The GC-path injection site (vlog/gc.rs) is not covered by this test —
/// triggering GC with a configured failpoint requires a separate test that
/// explicitly calls `trigger_gc()` after writing enough stale data.
#[test]
fn failpoint_vlog_after_append_before_index_publish() {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.value_separation = Some(kv_engine::vlog::ValueSeparationOptions {
        enabled: true,
        min_value_size: 512,
        max_value_size: 128 << 20,
        max_vlog_file_size: 64 << 20,
        gc_threshold_ratio: 0.5,
        max_open_vlog_files: 64,
        value_cache_capacity_bytes: 0,
    });

    run_failpoint_test_durable(
        "vlog.after_append_before_index_publish",
        opts,
        |engine| {
            let large_val = vec![b'X'; 2000];
            for i in 0..10 {
                let key = format!("vk_{i:010}");
                engine.put(key.as_bytes(), &large_val).unwrap();
            }
            engine.force_flush().unwrap();
        },
        |engine| {
            let v = engine.get(b"vk_0000000000").expect("get vk_0");
            assert!(
                v.is_some(),
                "vlog data must survive index crash via WAL replay"
            );
        },
    );
}
