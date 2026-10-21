//! Integration tests for the chaos-testing harness (RFC 013 Phase 1).
//!
//! These tests spawn the `chaos-child` binary as a real OS process, wait for it
//! to reach a sync point, send SIGKILL, reopen the database in-process, and
//! validate crash invariants using the control log oracle.

#![cfg(feature = "chaos-testing")]

use kv_engine::chaos::control_log::{ControlLogReader, OperationKind};
use kv_engine::chaos::failpoint::{
    PARALLEL_WAL_CRASH_MARKER_ENV, PARALLEL_WAL_CRASH_OCCURRENCE_ENV, PARALLEL_WAL_CRASH_POINT_ENV,
};
use kv_engine::chaos::oracle::{self, BoundedKeyUniverse, ReferenceState};
use kv_engine::chaos::scenarios::ScenarioConfig;
use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};
use kv_engine::wal::WalIoMode;
use kv_engine::{
    BackupOptions, BackupRepository, CreateBackupOutcome, EnablePitrOutcome,
    ImplementationRegistry, PersistedPitrConfig, PitrCloseOutcome, PitrOptions, PitrRestoreOptions,
    PitrRuntimeOptions, PitrStatusOptions, PitrWalIoMode, RecoveryPointOutcome, RecoverySelector,
    RecoveryTarget, RestoreToOutcome, VerifyPitrDepth, VerifyPitrOptions,
};
use std::io::Read;
#[cfg(target_os = "linux")]
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Path to the chaos-child binary (set by cargo for integration tests).
fn chaos_child_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_chaos-child"))
}

/// Run a single chaos scenario.
///
/// Spawns the child, waits for the sync-point marker in the control log, sends
/// SIGKILL, reopens the database, and validates crash invariants.
fn run_chaos_scenario(scenario_name: &str, config: &ScenarioConfig) {
    run_chaos_scenario_with_wal_mode(scenario_name, config, WalIoMode::Leader);
}

fn run_chaos_scenario_with_wal_mode(
    scenario_name: &str,
    config: &ScenarioConfig,
    wal_io_mode: WalIoMode,
) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let db_path = dir.path().join("db");
    let control_log_path = dir.path().join("chaos_control.log");
    let seed: u64 = 42;

    // Spawn the child process — pass paths as OsStr to avoid UTF-8 panics
    let mut child = Command::new(chaos_child_path());
    child
        .arg("--child")
        .arg("--scenario")
        .arg(scenario_name)
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--db-path")
        .arg(&db_path)
        .arg("--control-log-path")
        .arg(&control_log_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if wal_io_mode == WalIoMode::Parallel {
        child.arg("--parallel-wal");
    }
    let mut child = child
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn chaos-child: {e}"));

    // Drain stdout/stderr in background threads to prevent pipe deadlock
    let mut child_stdout_buf = child.stdout.take().unwrap();
    let mut child_stderr_buf = child.stderr.take().unwrap();
    let stdout_handle = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = child_stdout_buf.read_to_string(&mut buf);
        buf
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = child_stderr_buf.read_to_string(&mut buf);
        buf
    });

    // Wait for the sync-point marker in the control log (poll every 100ms).
    // Fast-fail if the child process exits early.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut sync_detected = false;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            eprintln!("chaos-child exited early with status: {status}");
            break;
        }
        if control_log_path.exists()
            && let Ok(reader) = ControlLogReader::open(&control_log_path)
        {
            for rec in reader.records() {
                if matches!(rec.kind, OperationKind::SyncPoint) {
                    sync_detected = true;
                    break;
                }
            }
        }
        if sync_detected {
            std::thread::sleep(Duration::from_millis(50)); // let child reach the sleep window
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Kill the child with SIGKILL (ignore error if already exited) and reap
    let _ = child.kill();
    let _ = child.wait(); // reap zombie

    // Capture child output for diagnostics (must do before assert to make it available on failure)
    let child_stdout = stdout_handle.join().unwrap_or_default();
    let child_stderr = stderr_handle.join().unwrap_or_default();

    assert!(
        sync_detected,
        "sync point not detected within 60s — child may have crashed or hung\nchild_stdout:\n{child_stdout}\nchild_stderr:\n{child_stderr}"
    );

    // --- POST-CRASH VALIDATION ---

    // 1. Open the database (this triggers full recovery)
    let engine = KvEngine::open(&db_path, config.storage_options.clone())
        .unwrap_or_else(|e| panic!("KvEngine::open after crash failed: {e}"));

    // 2. Close the engine before structural checks to avoid concurrent access, then run
    //    reopen-cycle validation with fresh instances.
    engine
        .close()
        .unwrap_or_else(|e| panic!("close before structural checks failed: {e}"));
    drop(engine);
    oracle::structural_checks(&db_path, &config.storage_options)
        .unwrap_or_else(|e| panic!("structural checks failed: {e}"));

    // 3. Reopen for data validation
    let engine = KvEngine::open(&db_path, config.storage_options.clone())
        .unwrap_or_else(|e| panic!("KvEngine::open for validation failed: {e}"));

    // 4. Build the reference state from the control log
    let reader = ControlLogReader::open(&control_log_path)
        .unwrap_or_else(|e| panic!("ControlLogReader::open failed: {e}"));
    let reference = ReferenceState::from_control_log(&reader);

    // 5. Reconcile the bounded key universe
    let universe = BoundedKeyUniverse::new(config.key_prefix, config.num_keys);
    let result = oracle::reconcile(&engine, &universe, &reference)
        .unwrap_or_else(|e| panic!("reconcile failed: {e}"));

    if !result.violations.is_empty() {
        // Build a detailed failure report
        let mut msg = format!(
            "\nCHAOS TEST FAILURE\n  scenario: {}\n  seed: {}\n  db_path: {}\n  committed_ops: {}\n  possibly_visible_ops: {}\n  violations:\n",
            scenario_name,
            seed,
            db_path.display(),
            result.committed_op_count,
            result.possibly_visible_op_count,
        );
        for v in &result.violations {
            let key_str = String::from_utf8_lossy(&v.key);
            msg.push_str(&format!("    - key={key_str} kind={:?}\n", v.kind));
        }
        msg.push_str(&format!("\nchild_stdout:\n{child_stdout}\n"));
        msg.push_str(&format!("\nchild_stderr:\n{child_stderr}\n"));
        // Preserve temp dir for debugging
        let preserved = dir.into_path();
        msg.push_str(&format!("Temp dir preserved at: {}\n", preserved.display()));
        panic!("{msg}");
    }

    // 6. Clean close
    engine.close().expect("close after validation");
    drop(engine);

    // 7. Second reopen pass: reopen, write more data, reopen again to catch latent metadata
    //    inconsistencies that a single reopen might miss (RFC 013 Phase 2).
    let second_pass_prefix = format!("__second_pass_{scenario_name}__");
    let second_pass_keys: Vec<String> = (0..20)
        .map(|i| format!("{second_pass_prefix}_{i:010}"))
        .collect();

    // Reopen, write second-pass keys, close
    {
        let engine = KvEngine::open(&db_path, config.storage_options.clone())
            .unwrap_or_else(|e| panic!("KvEngine::open for second pass failed: {e}"));
        for key in &second_pass_keys {
            let value = format!("second_pass_value_for_{key}");
            engine
                .put(key.as_bytes(), value.as_bytes())
                .unwrap_or_else(|e| panic!("second pass put({key}) failed: {e}"));
        }
        engine.close().expect("close after second pass writes");
    }

    // Reopen again and verify second-pass keys survived
    {
        let engine = KvEngine::open(&db_path, config.storage_options.clone())
            .unwrap_or_else(|e| panic!("KvEngine::open for second pass validation failed: {e}"));
        for key in &second_pass_keys {
            let expected = format!("second_pass_value_for_{key}");
            match engine.get(key.as_bytes()) {
                Ok(Some(v)) if &*v == expected.as_bytes() => {}
                Ok(Some(v)) => {
                    panic!(
                        "second pass key {key}: expected {expected:?}, got {:?}",
                        String::from_utf8_lossy(&v)
                    );
                }
                Ok(None) => {
                    panic!("second pass key {key}: lost after reopen (got None)");
                }
                Err(e) => {
                    panic!("second pass get({key}) failed: {e}");
                }
            }
        }
        engine.close().expect("close after second pass validation");
    }

    eprintln!(
        "chaos '{scenario_name}' passed: {} keys checked, {} committed ops, {} possibly-visible, {} second-pass keys",
        result.total_keys_checked,
        result.committed_op_count,
        result.possibly_visible_op_count,
        second_pass_keys.len(),
    );
}

// ============================================================================
// Test cases — one per scenario
// ============================================================================

#[test]
fn chaos_wal_only() {
    run_chaos_scenario("wal-only", &ScenarioConfig::wal_only());
}

#[cfg(target_os = "linux")]
#[test]
fn chaos_parallel_wal_only() {
    let config = ScenarioConfig::wal_only();
    let probe_dir = tempfile::tempdir().expect("create parallel WAL probe directory");
    match KvEngine::open_with_wal_io_mode(
        probe_dir.path(),
        config.storage_options.clone(),
        WalIoMode::Parallel,
    ) {
        Ok(engine) => engine.close().expect("close parallel WAL probe"),
        Err(error) if io_uring_is_unavailable(&error) => {
            eprintln!("skipping test (io_uring unavailable): {error:#}");
            return;
        }
        Err(error) => panic!("parallel WAL probe failed unexpectedly: {error:#}"),
    }

    run_chaos_scenario_with_wal_mode("wal-only", &config, WalIoMode::Parallel);
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_parallel_wal_crash_boundaries() {
    let config = ScenarioConfig::wal_only();
    let probe_dir = tempfile::tempdir().expect("create parallel WAL crash probe directory");
    match KvEngine::open_with_wal_io_mode(
        probe_dir.path(),
        config.storage_options.clone(),
        WalIoMode::Parallel,
    ) {
        Ok(engine) => engine.close().expect("close parallel WAL crash probe"),
        Err(error) => panic!("parallel WAL crash-boundary gate requires io_uring: {error:#}"),
    }

    run_parallel_wal_crash_case(
        "parallel_wal.offset_reserved",
        2,
        &[(b"unacknowledged-before-crash", b"candidate")],
        CandidateRecovery::Absent,
    );
    run_parallel_wal_crash_case(
        "parallel_wal.later_group_completed_first",
        1,
        &[
            (b"parallel-crash-first", b"first"),
            (b"parallel-crash-second", b"second"),
        ],
        CandidateRecovery::ContiguousPrefix,
    );
    run_parallel_wal_crash_case(
        "parallel_wal.before_fdatasync",
        2,
        &[(b"unacknowledged-before-crash", b"candidate")],
        CandidateRecovery::Either,
    );
    run_parallel_wal_crash_case(
        "parallel_wal.after_fdatasync",
        2,
        &[(b"unacknowledged-before-crash", b"candidate")],
        CandidateRecovery::Present,
    );
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_pitr_crash_recovery_install_rename_boundaries() {
    let probe_dir = tempfile::tempdir().expect("create v7 Parallel crash probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 Parallel crash probe"),
        Err(error) => panic!("v7 Parallel crash qualification requires io_uring: {error:#}"),
    }

    let dir = tempfile::tempdir().expect("create v7 Parallel crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = KvEngine::open(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .expect("open database for PITR setup");
    assert!(matches!(
        engine.create_backup(BackupOptions {
            repository: repository.clone(),
            use_hard_links: false,
        }),
        Ok(CreateBackupOutcome::Committed(_))
    ));
    assert!(matches!(
        engine.enable_pitr(PitrOptions {
            repository: repository.clone(),
            config: PersistedPitrConfig {
                archive_interval: Duration::from_secs(60),
                max_segment_bytes: 1 << 20,
                max_unarchived_bytes: 2 << 20,
                max_source_spool_bytes: 8 << 20,
            },
            runtime: PitrRuntimeOptions::default(),
        }),
        Ok(EnablePitrOutcome::Enabled { .. })
    ));
    engine
        .put(b"acknowledged-before-crash", b"stable")
        .expect("persist acknowledged baseline");
    engine.close().expect("close PITR setup engine");

    let control_log_path = dir.path().join("chaos_control.log");
    let marker_path = dir.path().join("crash.marker");
    let crash_point = "parallel_wal.after_fdatasync";
    let mut child = Command::new(chaos_child_path());
    child
        .arg("--child")
        .arg("--scenario")
        .arg("pitr-v7-parallel-crash")
        .arg("--seed")
        .arg("42")
        .arg("--db-path")
        .arg(&db_path)
        .arg("--pitr-repository")
        .arg(&repository)
        .arg("--control-log-path")
        .arg(&control_log_path)
        .arg("--parallel-wal")
        .arg("--crash-point")
        .arg(crash_point)
        .env(PARALLEL_WAL_CRASH_POINT_ENV, crash_point)
        .env(PARALLEL_WAL_CRASH_OCCURRENCE_ENV, "1")
        .env(PARALLEL_WAL_CRASH_MARKER_ENV, &marker_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = child
        .spawn()
        .expect("spawn v7 Parallel crash child process");

    let mut child_stdout = child.stdout.take().expect("capture child stdout");
    let mut child_stderr = child.stderr.take().expect("capture child stderr");
    let stdout_handle = std::thread::spawn(move || {
        let mut output = String::new();
        let _ = child_stdout.read_to_string(&mut output);
        output
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut output = String::new();
        let _ = child_stderr.read_to_string(&mut output);
        output
    });

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut marker_detected = false;
    let mut early_exit = None;
    while Instant::now() < deadline {
        if marker_path.is_file() {
            marker_detected = true;
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            early_exit = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = child.kill();
    let _ = child.wait();
    let child_stdout = stdout_handle.join().unwrap_or_default();
    let child_stderr = stderr_handle.join().unwrap_or_default();
    assert!(
        marker_detected,
        "v7 Parallel post-fdatasync boundary was not reached within 60s; early exit: {early_exit:?}\nchild_stdout:\n{child_stdout}\nchild_stderr:\n{child_stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(&marker_path)
            .expect("read post-fdatasync crash marker")
            .trim(),
        crash_point
    );

    let active_wal_path = find_v7_pitr_wal(&db_path);
    let source_wal_before_recovery_crash =
        std::fs::read(&active_wal_path).expect("read source WAL before recovery crash");
    let source_wal_inode_before_recovery_crash = std::fs::metadata(&active_wal_path)
        .expect("stat source WAL before recovery crash")
        .ino();
    let recovery_crash_point = "wal_v7.recovery.after_temp_sync";
    let recovery_marker_path = dir.path().join("recovery-crash.marker");
    kill_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &recovery_marker_path,
        recovery_crash_point,
        None,
        None,
    );
    assert_eq!(
        std::fs::read(&active_wal_path).expect("read source WAL after recovery crash"),
        source_wal_before_recovery_crash,
        "crashing before rename must leave the source WAL unchanged"
    );
    assert_eq!(recovery_temp_images(&db_path).len(), 1);

    let first_recovery = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository.clone()),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .expect("complete the first Active recovery normalization");
    first_recovery
        .close()
        .expect("close after the first Active recovery normalization");
    assert!(
        recovery_temp_images(&db_path).is_empty(),
        "successful recovery must clean the interrupted staging image"
    );
    let normalized_wal_path = find_v7_pitr_wal(&db_path);
    assert_eq!(normalized_wal_path, active_wal_path);
    let normalized_image = std::fs::read(&normalized_wal_path)
        .expect("read the first installed normalized Active WAL");
    let normalized_wal_inode = std::fs::metadata(&normalized_wal_path)
        .expect("stat the first installed normalized Active WAL")
        .ino();
    assert_ne!(
        normalized_wal_inode, source_wal_inode_before_recovery_crash,
        "successful normalization must install a fresh inode"
    );
    assert!(
        normalized_image.len() < source_wal_before_recovery_crash.len(),
        "successful normalization must discard the speculative suffix"
    );

    let repeated_recovery_marker_path = dir.path().join("repeated-recovery-crash.marker");
    kill_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &repeated_recovery_marker_path,
        recovery_crash_point,
        None,
        None,
    );
    assert_eq!(
        std::fs::read(&normalized_wal_path)
            .expect("read the normalized source after repeated recovery crash"),
        normalized_image,
        "a second pre-rename crash must preserve the already-installed normalized WAL"
    );
    assert_eq!(
        std::fs::metadata(&normalized_wal_path)
            .expect("stat normalized source after repeated recovery crash")
            .ino(),
        normalized_wal_inode,
        "a pre-rename crash during repeated normalization must keep the canonical inode"
    );
    assert_eq!(recovery_temp_images(&db_path).len(), 1);

    let rename_crash_point = "wal_v7.recovery.after_rename_before_directory_sync";
    let rename_marker_path = dir.path().join("recovery-rename-crash.marker");
    kill_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &rename_marker_path,
        rename_crash_point,
        None,
        None,
    );
    let renamed_wal_inode = std::fs::metadata(&active_wal_path)
        .expect("stat normalized WAL after recovery rename")
        .ino();
    assert_ne!(
        renamed_wal_inode, normalized_wal_inode,
        "the repeated rename boundary must replace the canonical WAL with a fresh recovery inode"
    );
    assert!(
        recovery_temp_images(&db_path).is_empty(),
        "the recovery image must have been renamed before the directory-sync crash"
    );

    let recovery_options = LsmStorageOptions {
        enable_wal: true,
        pitr_repository: Some(repository.clone()),
        ..LsmStorageOptions::default_for_test()
    };
    let recovered =
        KvEngine::open_with_wal_io_mode(&db_path, recovery_options, WalIoMode::Parallel)
            .expect("reopen PITR database after recovery-install process kill");
    assert!(
        recovery_temp_images(&db_path).is_empty(),
        "restart must durably remove the stale recovery image before serving"
    );
    let status = recovered
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(1).expect("nonzero PITR status page size"),
        })
        .expect("read recovered PITR status");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(status.active_wal_io_mode, Some(PitrWalIoMode::Parallel));
    assert_eq!(
        recovered
            .get(b"acknowledged-migration-trigger")
            .expect("read acknowledged legacy migration trigger after recovery")
            .as_deref(),
        Some(b"legacy".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"acknowledged-before-crash")
            .expect("read acknowledged baseline after recovery")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"synced-before-publication")
            .expect("read post-sync candidate after recovery")
            .as_deref(),
        Some(b"candidate".as_slice()),
        "a successful v7 WAL sync must survive even when publication and ACK did not occur"
    );
    recovered
        .close()
        .expect("close recovered v7 Parallel PITR engine");

    let recovered_active_wal_path = find_v7_pitr_wal(&db_path);
    let recovered_active_wal_len = std::fs::metadata(&recovered_active_wal_path)
        .expect("stat recovered Active WAL before continued write")
        .len();
    let continued_write_crash_point = "parallel_wal.after_fdatasync";
    let continued_write_marker_path = dir.path().join("continued-write-crash.marker");
    kill_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &continued_write_marker_path,
        continued_write_crash_point,
        None,
        Some(("post-recovery-synced-write", "durable")),
    );
    let continued_active_wal_path = find_v7_pitr_wal(&db_path);
    assert_eq!(continued_active_wal_path, recovered_active_wal_path);
    assert!(
        std::fs::metadata(&continued_active_wal_path)
            .expect("stat Active WAL after continued-write sync crash")
            .len()
            > recovered_active_wal_len,
        "the post-recovery sync must append to the installed Active WAL"
    );

    let continued_recovery_options = LsmStorageOptions {
        enable_wal: true,
        pitr_repository: Some(repository.clone()),
        ..LsmStorageOptions::default_for_test()
    };
    let continued_recovery =
        KvEngine::open_with_wal_io_mode(&db_path, continued_recovery_options, WalIoMode::Parallel)
            .expect("recover a sync after continuing from the installed v7 recovery image");
    assert_eq!(
        continued_recovery
            .get(b"post-recovery-synced-write")
            .expect("read post-recovery synced write")
            .as_deref(),
        Some(b"durable".as_slice()),
        "a new write synced after recovery must survive a process crash before publication"
    );
    assert_eq!(
        continued_recovery
            .get(b"acknowledged-before-crash")
            .expect("read acknowledged baseline after continued recovery")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        continued_recovery
            .get(b"synced-before-publication")
            .expect("read prior synced candidate after continued recovery")
            .as_deref(),
        Some(b"candidate".as_slice())
    );

    let continued_status = continued_recovery
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read PITR status before sealing the recovered Active WAL");
    let interval = continued_status
        .recoverable_intervals
        .first()
        .expect("PITR setup must retain a recoverable base interval");
    let selector = RecoverySelector {
        timeline_id: interval.timeline_id,
        archive_epoch_id: Some(interval.archive_epoch_id),
        base_backup_id: None,
    };
    let final_point = match continued_recovery
        .close_pitr()
        .expect("seal and archive the recovered Active WAL")
    {
        PitrCloseOutcome::ClosedDurably {
            final_point: Some(point),
        } => point,
        outcome => panic!("PITR close did not durably archive the recovered WAL: {outcome:?}"),
    };
    assert!(
        final_point.commit_ts.is_some(),
        "the continued write must be included in the final archived boundary"
    );

    let restored_path = dir.path().join("restored-after-recovery");
    let backup_repository =
        BackupRepository::open(&repository).expect("open repository after PITR close");
    let restore_info = match backup_repository
        .restore_to(
            RecoveryTarget::Latest,
            &restored_path,
            PitrRestoreOptions {
                selector,
                implementations: ImplementationRegistry,
                executor_threads: NonZeroUsize::new(1).expect("nonzero restore worker count"),
                cache_capacity: 4096,
                storage: LsmStorageOptions::default_for_test(),
            },
        )
        .expect("restore the latest point after recovery continuation")
    {
        RestoreToOutcome::Restored(info) => info,
        outcome => panic!("PITR restore did not produce a restored database: {outcome:?}"),
    };
    assert_eq!(restore_info.resolved_commit_ts, final_point.commit_ts);
    assert!(
        restore_info.replayed_segments > 0,
        "latest restore must replay the archived WAL containing the continued write"
    );

    let restored = KvEngine::open(&restored_path, LsmStorageOptions::default_for_test())
        .expect("open database restored from the continued v7 recovery chain");
    for (key, expected) in [
        (
            b"acknowledged-before-crash".as_slice(),
            b"stable".as_slice(),
        ),
        (
            b"synced-before-publication".as_slice(),
            b"candidate".as_slice(),
        ),
        (
            b"post-recovery-synced-write".as_slice(),
            b"durable".as_slice(),
        ),
    ] {
        assert_eq!(
            restored
                .get(key)
                .expect("read restored PITR value")
                .as_deref(),
            Some(expected),
            "latest restore must retain {key:?}"
        );
    }
    restored.close().expect("close restored database");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_pitr_fallback_archive_restore() {
    let probe_dir = tempfile::tempdir().expect("create v7 Parallel fallback probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 Parallel fallback probe"),
        Err(error) => panic!("v7 Parallel fallback qualification requires io_uring: {error:#}"),
    }

    let dir = tempfile::tempdir().expect("create v7 fallback archive-restore directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = KvEngine::open(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .expect("open database for PITR fallback setup");
    assert!(matches!(
        engine.create_backup(BackupOptions {
            repository: repository.clone(),
            use_hard_links: false,
        }),
        Ok(CreateBackupOutcome::Committed(_))
    ));
    assert!(matches!(
        engine.enable_pitr(PitrOptions {
            repository: repository.clone(),
            config: PersistedPitrConfig {
                archive_interval: Duration::from_secs(60),
                max_segment_bytes: 1 << 20,
                max_unarchived_bytes: 2 << 20,
                max_source_spool_bytes: 8 << 20,
            },
            runtime: PitrRuntimeOptions::default(),
        }),
        Ok(EnablePitrOutcome::Enabled { .. })
    ));
    engine
        .put(b"fallback-acknowledged", b"stable")
        .expect("persist acknowledged baseline");
    engine.close().expect("close PITR fallback setup engine");

    let control_log_path = dir.path().join("fallback-control.log");
    let crash_marker_path = dir.path().join("fallback-crash.marker");
    kill_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &crash_marker_path,
        "parallel_wal.after_fdatasync",
        Some(("fallback-v7-acknowledged", "durable")),
        Some(("synced-before-publication", "candidate")),
    );

    let active_wal_path = find_v7_pitr_wal(&db_path);
    let speculative_wal = std::fs::read(&active_wal_path).expect("read synced candidate WAL");
    let speculative_wal_inode = std::fs::metadata(&active_wal_path)
        .expect("stat synced candidate WAL")
        .ino();
    let candidate_key = b"synced-before-publication";
    let candidate_key_offset = speculative_wal
        .windows(candidate_key.len())
        .position(|window| window == candidate_key)
        .expect("find the unacknowledged candidate in the v7 DATA frames");
    assert!(
        speculative_wal
            .windows(candidate_key.len())
            .filter(|window| *window == candidate_key)
            .count()
            == 1,
        "candidate key must have one unambiguous on-disk occurrence"
    );
    let candidate_key_end = candidate_key_offset + candidate_key.len();
    assert_eq!(
        candidate_key_offset / 4096,
        (candidate_key_end - 1) / 4096,
        "candidate key must fit in one aligned DATA frame"
    );
    let candidate_frame_offset = candidate_key_offset / 4096 * 4096;
    assert_eq!(
        &speculative_wal[candidate_frame_offset..candidate_frame_offset + 8],
        b"TKVW7FR1",
        "candidate bytes must belong to a canonical v7 frame"
    );
    assert_eq!(
        u16::from_be_bytes(
            speculative_wal[candidate_frame_offset + 10..candidate_frame_offset + 12]
                .try_into()
                .expect("v7 frame kind has two bytes")
        ),
        1,
        "candidate bytes must belong to a DATA frame"
    );

    // Make the newest synced FRONTIER fail prefix verification while keeping
    // the preceding acknowledged v7 frontier valid. Active recovery must reject
    // the candidate and fall back to that frontier above the manifest floor.
    let mut wal = std::fs::OpenOptions::new()
        .write(true)
        .open(&active_wal_path)
        .expect("open Active v7 WAL to damage speculative candidate");
    wal.seek(SeekFrom::Start(
        u64::try_from(candidate_key_offset).expect("candidate offset fits u64"),
    ))
    .expect("seek to speculative candidate bytes");
    wal.write_all(&[speculative_wal[candidate_key_offset] ^ 1])
        .expect("damage speculative candidate bytes");
    wal.sync_all()
        .expect("persist speculative candidate corruption");
    drop(wal);

    let recovered = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository.clone()),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .expect("fall back to the manifest-authorized Active v7 boundary");
    assert_eq!(
        recovered
            .get(b"fallback-acknowledged")
            .expect("read acknowledged value after fallback")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"acknowledged-migration-trigger")
            .expect("read acknowledged migration trigger after fallback")
            .as_deref(),
        Some(b"legacy".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"fallback-v7-acknowledged")
            .expect("read acknowledged v7 value after fallback")
            .as_deref(),
        Some(b"durable".as_slice()),
        "fallback must retain the previous durable frontier in the same v7 WAL"
    );
    assert_eq!(
        recovered
            .get(candidate_key)
            .expect("read rejected candidate after fallback"),
        None,
        "fallback must discard the corrupted, unacknowledged candidate"
    );

    let normalized_wal_path = find_v7_pitr_wal(&db_path);
    assert_eq!(normalized_wal_path, active_wal_path);
    let normalized_wal =
        std::fs::read(&normalized_wal_path).expect("read normalized fallback WAL image");
    assert_ne!(
        std::fs::metadata(&normalized_wal_path)
            .expect("stat normalized fallback WAL")
            .ino(),
        speculative_wal_inode,
        "fallback recovery must install a fresh Active WAL inode"
    );
    assert!(
        normalized_wal.len() < speculative_wal.len(),
        "fallback normalization must discard the rejected speculative suffix"
    );

    let status = recovered
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read PITR status before sealing the fallback WAL");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(status.active_wal_io_mode, Some(PitrWalIoMode::Parallel));
    let interval = status
        .recoverable_intervals
        .first()
        .expect("PITR setup must retain a recoverable base interval");
    let selector = RecoverySelector {
        timeline_id: interval.timeline_id,
        archive_epoch_id: Some(interval.archive_epoch_id),
        base_backup_id: None,
    };
    let final_point = match recovered
        .close_pitr()
        .expect("seal and archive the normalized fallback WAL")
    {
        PitrCloseOutcome::ClosedDurably {
            final_point: Some(point),
        } => point,
        outcome => panic!("PITR close did not archive the fallback WAL: {outcome:?}"),
    };

    let restored_path = dir.path().join("restored-after-fallback");
    let backup_repository =
        BackupRepository::open(&repository).expect("open repository after fallback archive");
    let restore_info = match backup_repository
        .restore_to(
            RecoveryTarget::Latest,
            &restored_path,
            PitrRestoreOptions {
                selector,
                implementations: ImplementationRegistry,
                executor_threads: NonZeroUsize::new(1).expect("nonzero restore worker count"),
                cache_capacity: 4096,
                storage: LsmStorageOptions::default_for_test(),
            },
        )
        .expect("restore the latest point after Active v7 fallback")
    {
        RestoreToOutcome::Restored(info) => info,
        outcome => panic!("PITR fallback restore did not produce a database: {outcome:?}"),
    };
    assert_eq!(restore_info.resolved_commit_ts, final_point.commit_ts);
    assert!(
        restore_info.replayed_segments >= 2,
        "latest restore must include both the prior legacy segment and fallback v7 WAL segment"
    );

    let restored = KvEngine::open(&restored_path, LsmStorageOptions::default_for_test())
        .expect("open database restored from the fallback WAL");
    for (key, expected) in [
        (b"fallback-acknowledged".as_slice(), b"stable".as_slice()),
        (
            b"fallback-v7-acknowledged".as_slice(),
            b"durable".as_slice(),
        ),
        (
            b"acknowledged-migration-trigger".as_slice(),
            b"legacy".as_slice(),
        ),
    ] {
        assert_eq!(
            restored
                .get(key)
                .expect("read restored fallback value")
                .as_deref(),
            Some(expected),
            "latest restore must retain {key:?}"
        );
    }
    assert_eq!(
        restored
            .get(candidate_key)
            .expect("read rejected candidate from restored database"),
        None,
        "latest restore must not replay the rejected speculative candidate"
    );
    restored.close().expect("close restored fallback database");
}

#[cfg(target_os = "linux")]
#[test]
fn pitr_v7_empty_segment_archive_restore() {
    let probe_dir = tempfile::tempdir().expect("create v7 Parallel empty-segment probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel empty-segment probe"),
        Err(error) => {
            panic!("v7 Parallel empty-segment qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 empty-segment archive-restore directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = KvEngine::open(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .expect("open database for empty-segment PITR setup");
    assert!(matches!(
        engine.create_backup(BackupOptions {
            repository: repository.clone(),
            use_hard_links: false,
        }),
        Ok(CreateBackupOutcome::Committed(_))
    ));
    assert!(matches!(
        engine.enable_pitr(PitrOptions {
            repository: repository.clone(),
            config: PersistedPitrConfig {
                archive_interval: Duration::from_secs(60),
                max_segment_bytes: 1 << 20,
                max_unarchived_bytes: 2 << 20,
                max_source_spool_bytes: 8 << 20,
            },
            runtime: PitrRuntimeOptions::default(),
        }),
        Ok(EnablePitrOutcome::Enabled { .. })
    ));
    engine
        .put(b"legacy-segment-value", b"restored")
        .expect("persist value in the legacy PITR segment");
    engine.close().expect("close PITR setup engine");

    let engine = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository.clone()),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .expect("reopen PITR database with the v7 Parallel writer");
    engine
        .resume_pitr(&repository)
        .expect("resume PITR before checking the Active format");
    let mut status = engine
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read status after first legacy resume");
    if status.active_wal_format_version != Some(7) {
        engine
            .put(b"legacy-migration-trigger", b"legacy")
            .expect("persist a legacy batch before v7 migration");
        engine
            .resume_pitr(&repository)
            .expect("resume PITR to install the v7 successor");
        status = engine
            .pitr_status(PitrStatusOptions {
                cursor: None,
                page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
            })
            .expect("read status after v7 migration");
    }
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(status.active_wal_io_mode, Some(PitrWalIoMode::Parallel));
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_image = std::fs::read(&active_wal_path).expect("read empty v7 Active WAL");
    assert_eq!(
        active_image.len(),
        2 * 4096,
        "empty v7 image is header plus F0"
    );
    assert_eq!(&active_image[4096..4104], b"TKVW7FR1");
    assert_eq!(
        u16::from_be_bytes([active_image[4106], active_image[4107]]),
        2,
        "the empty Active v7 image must contain generation-zero FRONTIER"
    );
    assert_eq!(
        status.active_wal_bytes,
        u64::try_from(active_image.len()).expect("active image length fits u64")
    );

    let interval = status
        .recoverable_intervals
        .first()
        .expect("PITR setup must retain a recoverable base interval");
    let selector = RecoverySelector {
        timeline_id: interval.timeline_id,
        archive_epoch_id: Some(interval.archive_epoch_id),
        base_backup_id: None,
    };

    assert!(matches!(
        engine
            .create_recovery_point()
            .expect("archive the empty v7 segment before a later segment"),
        RecoveryPointOutcome::Durable(_)
    ));
    engine
        .put(b"after-empty-v7-segment", b"later")
        .expect("persist a commit in the v7 segment after the empty one");
    let final_point = match engine
        .close_pitr()
        .expect("seal and archive the later nonempty v7 segment")
    {
        PitrCloseOutcome::ClosedDurably {
            final_point: Some(point),
        } => point,
        outcome => panic!("empty v7 PITR close did not finish durably: {outcome:?}"),
    };
    assert!(final_point.commit_ts.is_some());

    let mut archived_v7_images = Vec::new();
    for entry in std::fs::read_dir(repository.join("wal")).expect("list archived PITR WAL objects")
    {
        let path = entry
            .expect("read archived PITR WAL directory entry")
            .path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("wal") {
            continue;
        }
        let image = std::fs::read(&path).expect("read archived PITR WAL object");
        if image.len() >= 6 && u16::from_be_bytes([image[4], image[5]]) == 7 {
            archived_v7_images.push((path, image));
        }
    }
    assert_eq!(
        archived_v7_images.len(),
        2,
        "archive must contain the empty v7 segment and its later successor"
    );
    let empty_v7_path = archived_v7_images
        .iter()
        .find(|(_, image)| image.len() == 2 * 4096)
        .map(|(path, _)| path.clone())
        .expect("find the archived empty v7 image");
    let archived_v7_image = archived_v7_images
        .iter()
        .find(|(_, image)| image.len() == 2 * 4096)
        .map(|(_, image)| image)
        .expect("find the archived empty v7 image");
    assert_eq!(&archived_v7_image[4096..4104], b"TKVW7FR1");
    assert_eq!(
        u16::from_be_bytes([archived_v7_image[4106], archived_v7_image[4107]]),
        2,
        "the empty archived v7 image must end at its generation-zero FRONTIER"
    );
    assert!(
        archived_v7_images
            .iter()
            .any(|(_, image)| image.len() > 2 * 4096),
        "archive must contain a later nonempty v7 segment"
    );

    let backup_repository =
        BackupRepository::open(&repository).expect("open repository after empty v7 archive");
    let verification = backup_repository
        .verify_pitr(VerifyPitrOptions {
            depth: VerifyPitrDepth::Deep {
                sampled_targets: NonZeroUsize::new(1).expect("nonzero verification sample count"),
            },
            selector: Some(selector),
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR verification page size"),
        })
        .expect("deeply verify the archived empty v7 segment");
    assert_eq!(
        verification.first_failure, None,
        "the empty v7 WAL and its seal must verify as immutable archive objects"
    );

    let restored_path = dir.path().join("restored-empty-v7-segment");
    let restore_info = match backup_repository
        .restore_to(
            RecoveryTarget::Latest,
            &restored_path,
            PitrRestoreOptions {
                selector,
                implementations: ImplementationRegistry,
                executor_threads: NonZeroUsize::new(1).expect("nonzero restore worker count"),
                cache_capacity: 4096,
                storage: LsmStorageOptions::default_for_test(),
            },
        )
        .expect("restore Latest across the empty v7 segment")
    {
        RestoreToOutcome::Restored(info) => info,
        outcome => panic!("empty v7 segment restore did not produce a database: {outcome:?}"),
    };
    assert_eq!(restore_info.resolved_commit_ts, final_point.commit_ts);
    assert!(restore_info.replayed_segments >= 2);

    let restored = KvEngine::open(&restored_path, LsmStorageOptions::default_for_test())
        .expect("open database restored across the empty v7 segment");
    assert_eq!(
        restored
            .get(b"legacy-segment-value")
            .expect("read value restored from the legacy source segment")
            .as_deref(),
        Some(b"restored".as_slice())
    );
    assert_eq!(
        restored
            .get(b"after-empty-v7-segment")
            .expect("read value restored from the later v7 segment")
            .as_deref(),
        Some(b"later".as_slice())
    );
    restored
        .close()
        .expect("close restored empty-segment database");

    // The empty segment is included as a restore proof because the later v7
    // segment is selected. Corrupting its archived WAL must therefore prevent a
    // second Latest restore, even though there are no batches to replay from it.
    let mut corrupted_empty_image = std::fs::read(&empty_v7_path).expect("read empty v7 archive");
    corrupted_empty_image[4096] ^= 0xff;
    std::fs::write(&empty_v7_path, corrupted_empty_image)
        .expect("corrupt the empty v7 archive proof WAL");
    let corrupt_restore_path = dir.path().join("restored-with-corrupt-empty-proof");
    let corrupt_restore_result = backup_repository.restore_to(
        RecoveryTarget::Latest,
        corrupt_restore_path.clone(),
        PitrRestoreOptions {
            selector,
            implementations: ImplementationRegistry,
            executor_threads: NonZeroUsize::new(1).expect("nonzero restore worker count"),
            cache_capacity: 4096,
            storage: LsmStorageOptions::default_for_test(),
        },
    );
    assert!(
        corrupt_restore_result.is_err(),
        "Latest restore must verify the empty v7 proof segment before publishing the restored database"
    );
    assert!(
        !corrupt_restore_path.exists(),
        "a failed restore must not leave its destination published"
    );
}

#[cfg(target_os = "linux")]
fn kill_pitr_child_at_crash_point(
    db_path: &std::path::Path,
    repository: &std::path::Path,
    control_log_path: &std::path::Path,
    marker_path: &std::path::Path,
    crash_point: &str,
    acknowledged_write: Option<(&str, &str)>,
    write: Option<(&str, &str)>,
) {
    let mut child = Command::new(chaos_child_path());
    child
        .arg("--child")
        .arg("--scenario")
        .arg("pitr-v7-parallel-crash")
        .arg("--seed")
        .arg("42")
        .arg("--db-path")
        .arg(db_path)
        .arg("--pitr-repository")
        .arg(repository)
        .arg("--control-log-path")
        .arg(control_log_path)
        .arg("--parallel-wal")
        .arg("--crash-point")
        .arg(crash_point)
        .env(PARALLEL_WAL_CRASH_POINT_ENV, crash_point)
        .env(
            PARALLEL_WAL_CRASH_OCCURRENCE_ENV,
            if acknowledged_write.is_some() {
                "2"
            } else {
                "1"
            },
        )
        .env(PARALLEL_WAL_CRASH_MARKER_ENV, marker_path);
    if let Some((key, value)) = acknowledged_write {
        child
            .arg("--acknowledged-write-key")
            .arg(key)
            .arg("--acknowledged-write-value")
            .arg(value);
    }
    if let Some((key, value)) = write {
        child
            .arg("--write-key")
            .arg(key)
            .arg("--write-value")
            .arg(value);
    }
    child.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = child
        .spawn()
        .expect("spawn v7 recovery-install crash child process");
    let mut child_stdout = child.stdout.take().expect("capture recovery child stdout");
    let mut child_stderr = child.stderr.take().expect("capture recovery child stderr");
    let stdout_handle = std::thread::spawn(move || {
        let mut output = String::new();
        let _ = child_stdout.read_to_string(&mut output);
        output
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut output = String::new();
        let _ = child_stderr.read_to_string(&mut output);
        output
    });

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut marker_detected = false;
    let mut early_exit = None;
    while Instant::now() < deadline {
        if marker_path.is_file() {
            marker_detected = true;
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            early_exit = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = child.kill();
    let _ = child.wait();
    let child_stdout = stdout_handle.join().unwrap_or_default();
    let child_stderr = stderr_handle.join().unwrap_or_default();
    assert!(
        marker_detected,
        "v7 recovery-install boundary {crash_point} was not reached within 60s; early exit: {early_exit:?}\nchild_stdout:\n{child_stdout}\nchild_stderr:\n{child_stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(marker_path)
            .expect("read recovery-install crash marker")
            .trim(),
        crash_point
    );
}

#[cfg(target_os = "linux")]
fn find_v7_pitr_wal(db_path: &std::path::Path) -> PathBuf {
    let mut paths = std::fs::read_dir(db_path)
        .expect("list database files")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("pitr-") && name.ends_with(".wal"))
        })
        .filter(|path| {
            std::fs::read(path).ok().is_some_and(|bytes| {
                bytes.len() >= 6 && u16::from_be_bytes([bytes[4], bytes[5]]) == 7
            })
        });
    let path = paths.next().expect("find Active v7 PITR WAL");
    assert!(paths.next().is_none(), "expected one Active v7 PITR WAL");
    path
}

#[cfg(target_os = "linux")]
fn recovery_temp_images(db_path: &std::path::Path) -> Vec<PathBuf> {
    std::fs::read_dir(db_path)
        .expect("list database files")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".toy-kv-v7-recovery-"))
        })
        .collect()
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum CandidateRecovery {
    Present,
    Absent,
    Either,
    ContiguousPrefix,
}

#[cfg(target_os = "linux")]
fn run_parallel_wal_crash_case(
    crash_point: &str,
    occurrence: usize,
    candidates: &[(&[u8], &[u8])],
    candidate_recovery: CandidateRecovery,
) {
    let dir = tempfile::tempdir().expect("create parallel WAL crash case directory");
    let db_path = dir.path().join("db");
    let control_log_path = dir.path().join("chaos_control.log");
    let marker_path = dir.path().join("crash.marker");

    let mut child = Command::new(chaos_child_path());
    child
        .arg("--child")
        .arg("--scenario")
        .arg("parallel-wal-crash")
        .arg("--seed")
        .arg("42")
        .arg("--db-path")
        .arg(&db_path)
        .arg("--control-log-path")
        .arg(&control_log_path)
        .arg("--parallel-wal")
        .arg("--crash-point")
        .arg(crash_point)
        .env(PARALLEL_WAL_CRASH_POINT_ENV, crash_point)
        .env(PARALLEL_WAL_CRASH_OCCURRENCE_ENV, occurrence.to_string())
        .env(PARALLEL_WAL_CRASH_MARKER_ENV, &marker_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = child.spawn().unwrap_or_else(|error| {
        panic!("failed to spawn parallel WAL crash child at {crash_point}: {error}")
    });

    let mut child_stdout = child.stdout.take().expect("capture child stdout");
    let mut child_stderr = child.stderr.take().expect("capture child stderr");
    let stdout_handle = std::thread::spawn(move || {
        let mut output = String::new();
        let _ = child_stdout.read_to_string(&mut output);
        output
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut output = String::new();
        let _ = child_stderr.read_to_string(&mut output);
        output
    });

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut marker_detected = false;
    let mut early_exit = None;
    while Instant::now() < deadline {
        if marker_path.is_file() {
            marker_detected = true;
            break;
        }
        if let Ok(Some(status)) = child.try_wait() {
            early_exit = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = child.kill();
    let _ = child.wait();
    let child_stdout = stdout_handle.join().unwrap_or_default();
    let child_stderr = stderr_handle.join().unwrap_or_default();

    assert!(
        marker_detected,
        "parallel WAL crash point {crash_point} was not reached within 60s; early exit: {early_exit:?}\nchild_stdout:\n{child_stdout}\nchild_stderr:\n{child_stderr}"
    );
    let marker = std::fs::read_to_string(&marker_path).expect("read crash marker");
    assert_eq!(marker.trim(), crash_point);

    let config = ScenarioConfig::wal_only();
    let engine = KvEngine::open(&db_path, config.storage_options).unwrap_or_else(|error| {
        panic!("reopen after {crash_point} process kill failed: {error:#}")
    });
    assert_eq!(
        engine
            .get(b"acknowledged-before-crash")
            .expect("read acknowledged baseline after recovery")
            .as_deref(),
        Some(b"stable".as_slice()),
        "acknowledged write was lost after crash at {crash_point}"
    );

    let mut missing_candidate = false;
    for (key, expected) in candidates {
        let actual = engine
            .get(key)
            .unwrap_or_else(|error| panic!("read candidate after {crash_point} failed: {error:#}"));
        match candidate_recovery {
            CandidateRecovery::Present => {
                assert_eq!(actual.as_deref(), Some(*expected), "at {crash_point}");
            }
            CandidateRecovery::Absent => {
                assert!(
                    actual.is_none(),
                    "unwritten candidate recovered at {crash_point}"
                );
            }
            CandidateRecovery::Either => assert!(
                actual.is_none() || actual.as_deref() == Some(*expected),
                "candidate recovered with unexpected value at {crash_point}: {actual:?}"
            ),
            CandidateRecovery::ContiguousPrefix => match actual.as_deref() {
                Some(value) => {
                    assert!(
                        !missing_candidate,
                        "recovery skipped an earlier ticket and recovered {key:?} at {crash_point}"
                    );
                    assert_eq!(value, *expected, "at {crash_point}");
                }
                None => missing_candidate = true,
            },
        }
    }
    engine
        .close()
        .expect("close recovered parallel WAL database");
}

fn io_uring_is_unavailable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
            .is_some_and(|code| matches!(code, libc::EPERM | libc::ENOMEM | libc::ENOSYS))
    })
}

#[test]
fn chaos_flush_boundary() {
    run_chaos_scenario("flush-boundary", &ScenarioConfig::flush_boundary());
}

#[test]
fn chaos_manifest_snapshot() {
    run_chaos_scenario("manifest-snapshot", &ScenarioConfig::manifest_snapshot());
}

#[test]
fn chaos_range_tombstone() {
    run_chaos_scenario("range-tombstone", &ScenarioConfig::range_tombstone());
}

#[test]
fn chaos_vlog() {
    run_chaos_scenario("vlog", &ScenarioConfig::vlog());
}

#[test]
fn chaos_vlog_repeated() {
    for _ in 0..3 {
        run_chaos_scenario("vlog", &ScenarioConfig::vlog());
    }
}

#[test]
fn chaos_leveled_compaction() {
    run_chaos_scenario("leveled-compaction", &ScenarioConfig::leveled_compaction());
}

#[test]
fn chaos_tiered_compaction() {
    run_chaos_scenario("tiered-compaction", &ScenarioConfig::tiered_compaction());
}
