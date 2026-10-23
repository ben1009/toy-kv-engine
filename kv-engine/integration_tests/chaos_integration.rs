//! Integration tests for the chaos-testing harness (RFC 013 Phase 1).
//!
//! These tests spawn the `chaos-child` binary as a real OS process, wait for it
//! to reach a sync point, send SIGKILL, reopen the database in-process, and
//! validate crash invariants using the control log oracle.

#![cfg(feature = "chaos-testing")]

use kv_engine::chaos::control_log::{ControlLogReader, OperationKind};
use kv_engine::chaos::failpoint::{
    FailScenario, PARALLEL_WAL_CRASH_MARKER_ENV, PARALLEL_WAL_CRASH_OCCURRENCE_ENV,
    PARALLEL_WAL_CRASH_POINT_ENV,
};
use kv_engine::chaos::oracle::{self, BoundedKeyUniverse, ReferenceState};
use kv_engine::chaos::scenarios::ScenarioConfig;
use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions, WriteBatchRecord};
use kv_engine::wal::WalIoMode;
use kv_engine::{
    BackupOptions, BackupRepository, CreateBackupOutcome, EnablePitrOutcome,
    ImplementationRegistry, PersistedPitrConfig, PitrCloseOutcome, PitrOptions, PitrRestoreOptions,
    PitrResumeOutcome, PitrRuntimeOptions, PitrStatusOptions, PitrWalIoMode, RecoveryPointOutcome,
    RecoverySelector, RecoveryTarget, RestoreToOutcome, VerifyPitrDepth, VerifyPitrOptions,
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
fn failpoint_v7_leader_pitr_crash_after_sync_before_publish_recovers() {
    let dir = tempfile::tempdir().expect("create v7 Leader crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let control_log_path = dir.path().join("chaos_control.log");

    let engine = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Leader,
    )
    .expect("open Leader database for PITR setup");
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
        .put(b"leader-acknowledged-before-crash", b"stable")
        .expect("persist acknowledged Leader baseline");
    engine.close().expect("close Leader PITR setup engine");

    let crash_point = "wal_v7.leader.after_fdatasync_before_publish";
    let marker_path = dir.path().join("leader-crash.marker");
    kill_pitr_child_at_crash_point_with_mode(
        &db_path,
        &repository,
        &control_log_path,
        &marker_path,
        crash_point,
        WalIoMode::Leader,
        PitrChildCrashOperation::Write {
            acknowledged_write: None,
            write: Some(("leader-synced-before-publish", "candidate")),
        },
    );

    let recovered = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Leader,
    )
    .expect("reopen after successful Leader fdatasync before publication");
    let status = recovered
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(1).expect("nonzero status page size"),
        })
        .expect("read Active Leader status after recovery");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(status.active_wal_io_mode, Some(PitrWalIoMode::Leader));
    assert_eq!(
        recovered
            .get(b"leader-acknowledged-before-crash")
            .expect("read acknowledged Leader baseline after recovery")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"leader-synced-before-publish")
            .expect("read synced Leader candidate after recovery")
            .as_deref(),
        Some(b"candidate".as_slice()),
        "a completed WAL fdatasync must survive a crash before publication"
    );
    recovered.close().expect("close recovered Leader engine");
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

    let (candidate_frontier_offset, trailing_frontier_offset, trailing_frontier) =
        append_speculative_frontier_after_zero_tail(&active_wal_path, &speculative_wal, 1 << 20);
    assert!(trailing_frontier_offset > candidate_frontier_offset);
    assert!(
        u64::from_be_bytes(
            trailing_frontier[80..88]
                .try_into()
                .expect("v7 frontier durable end has eight bytes")
        ) <= u64::try_from(trailing_frontier_offset).expect("frontier offset fits u64"),
        "trailing frontier must certify a prefix ending before its own frame"
    );

    // Prove that recovery discovers the trailing marker, rather than passing
    // the fallback case below by rejecting the original candidate marker.
    // In this copy the DATA remains intact, but the marker at the original
    // frontier offset is removed; only the trailing copy can retain the write.
    let discovery_db_path = dir.path().join("trailing-frontier-discovery-db");
    copy_dir_recursive(&db_path, &discovery_db_path)
        .expect("copy database for trailing-frontier discovery check");
    let discovery_repository = dir.path().join("trailing-frontier-discovery-repository");
    copy_dir_recursive(&repository, &discovery_repository)
        .expect("copy repository for trailing-frontier discovery check");
    let discovery_wal_path = find_v7_pitr_wal(&discovery_db_path);
    let mut discovery_wal = std::fs::OpenOptions::new()
        .write(true)
        .open(&discovery_wal_path)
        .expect("open copied WAL to remove original candidate FRONTIER");
    discovery_wal
        .seek(SeekFrom::Start(
            u64::try_from(candidate_frontier_offset).expect("candidate frontier offset fits u64"),
        ))
        .expect("seek to original candidate FRONTIER");
    discovery_wal
        .write_all(&[0; 4096])
        .expect("remove original candidate FRONTIER from copied WAL");
    discovery_wal
        .sync_all()
        .expect("persist removal of original candidate FRONTIER");
    drop(discovery_wal);

    let discovered = KvEngine::open_with_wal_io_mode(
        &discovery_db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(discovery_repository),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .expect("discover the valid trailing FRONTIER after the zero-filled gap");
    assert_eq!(
        discovered
            .get(candidate_key)
            .expect("read candidate authorized by trailing FRONTIER")
            .as_deref(),
        Some(b"candidate".as_slice()),
        "recovery must discover the trailing FRONTIER to retain its covered candidate"
    );
    discovered
        .close()
        .expect("close trailing-frontier discovery copy");

    // Make the newest synced FRONTIER fail prefix verification while keeping
    // the preceding acknowledged v7 frontier valid. A second complete but
    // speculative FRONTIER sits later in the zero-filled tail, so recovery must
    // scan back through that tail, reject both candidates, and fall back above
    // the manifest floor.
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
    let tailed_wal =
        std::fs::read(&active_wal_path).expect("read WAL with speculative tail marker");
    assert_eq!(
        &tailed_wal[trailing_frontier_offset..trailing_frontier_offset + 8],
        b"TKVW7FR1",
        "the speculative tail must contain a complete FRONTIER frame"
    );
    assert_eq!(
        u64::from_be_bytes(
            tailed_wal[trailing_frontier_offset + 48..trailing_frontier_offset + 56]
                .try_into()
                .expect("v7 frame offset has eight bytes")
        ),
        u64::try_from(trailing_frontier_offset).expect("frontier offset fits u64")
    );
    let mut trailing_frontier_crc = crc32fast::Hasher::new();
    trailing_frontier_crc
        .update(&tailed_wal[trailing_frontier_offset..trailing_frontier_offset + 60]);
    trailing_frontier_crc
        .update(&tailed_wal[trailing_frontier_offset + 64..trailing_frontier_offset + 4096]);
    assert_eq!(
        u32::from_be_bytes(
            tailed_wal[trailing_frontier_offset + 60..trailing_frontier_offset + 64]
                .try_into()
                .expect("v7 frame CRC has four bytes")
        ),
        trailing_frontier_crc.finalize(),
        "the speculative tail FRONTIER must have a valid frame CRC"
    );
    assert!(
        tailed_wal[trailing_frontier_offset + 4096..trailing_frontier_offset + 8192]
            .iter()
            .all(|byte| *byte == 0),
        "the complete speculative FRONTIER must be followed by a zero-filled frame"
    );

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
        normalized_wal.len() < tailed_wal.len(),
        "fallback normalization must discard the rejected DATA and trailing FRONTIER"
    );
    assert!(
        normalized_wal.len() <= trailing_frontier_offset,
        "fallback normalization must not retain the speculative tail FRONTIER"
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
fn failpoint_v7_parallel_later_group_cqe_does_not_advance_past_gap() {
    let probe_dir = tempfile::tempdir().expect("create v7 Parallel CQE probe directory");
    let probe = KvEngine::open_with_wal_io_mode(
        probe_dir.path(),
        LsmStorageOptions {
            enable_wal: true,
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .unwrap_or_else(|error| panic!("v7 Parallel CQE qualification requires io_uring: {error:#}"));
    probe.close().expect("close v7 Parallel CQE probe");

    let dir = tempfile::tempdir().expect("create v7 out-of-order crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"v7-parallel-crash-baseline", b"stable")
        .expect("write the earlier v7 durability baseline");
    engine.sync().expect("sync the earlier v7 baseline");
    engine.close().expect("close v7 out-of-order setup engine");

    let control_log_path = dir.path().join("out-of-order-control.log");
    let crash_marker_path = dir.path().join("out-of-order-crash.marker");
    kill_pitr_child_at_crash_point_with_mode(
        &db_path,
        &repository,
        &control_log_path,
        &crash_marker_path,
        "parallel_wal.later_group_completed_first",
        WalIoMode::Parallel,
        PitrChildCrashOperation::Write {
            acknowledged_write: None,
            write: None,
        },
    );

    let active_wal_path = find_v7_pitr_wal(&db_path);
    let speculative_image =
        std::fs::read(&active_wal_path).expect("read v7 WAL after out-of-order CQE crash");
    let speculative_inode = std::fs::metadata(&active_wal_path)
        .expect("stat v7 WAL before recovery")
        .ino();
    for key in [
        b"v7-parallel-crash-first".as_slice(),
        b"v7-parallel-crash-second",
    ] {
        assert!(
            speculative_image
                .windows(key.len())
                .any(|window| window == key),
            "the later-group crash hook must run after real DATA I/O for {key:?}"
        );
    }

    let recovered = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository.clone()),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .expect("recover v7 after a later group completes before its predecessor");
    for (key, expected) in [
        (
            b"legacy-segment-value".as_slice(),
            Some(b"restored".as_slice()),
        ),
        (
            b"v7-parallel-crash-baseline".as_slice(),
            Some(b"stable".as_slice()),
        ),
        (b"v7-parallel-crash-first".as_slice(), None),
        (b"v7-parallel-crash-second".as_slice(), None),
    ] {
        assert_eq!(
            recovered
                .get(key)
                .unwrap_or_else(|error| panic!("read {key:?} after recovery: {error:#}"))
                .as_deref(),
            expected,
            "recovery must retain only the previously durable contiguous prefix"
        );
    }
    let status = recovered
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read PITR status after out-of-order recovery");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(status.active_wal_io_mode, Some(PitrWalIoMode::Parallel));
    assert_ne!(
        std::fs::metadata(&active_wal_path)
            .expect("stat normalized v7 WAL")
            .ino(),
        speculative_inode,
        "recovery must install a normalized fresh inode"
    );
    recovered.close().expect("close recovered v7 engine");
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
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let status = engine
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read status after v7 migration");
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
#[test]
fn failpoint_v7_empty_successor_directory_sync_failure_reopens() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().expect("create v7 successor-directory-sync directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);

    let failpoint = "wal_v7.active_image.before_directory_sync";
    fail::cfg(failpoint, "return").expect("inject empty Active image directory sync failure");
    let publication = engine.create_recovery_point();
    fail::remove(failpoint);
    let error = publication.expect_err("successor directory sync failure must reach the caller");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("injected empty Active v7 image directory sync failure"),
        "the targeted directory-sync failure must be reported, got: {error_chain}"
    );
    assert!(
        successor_path.exists(),
        "the canonical successor must remain after rename even though directory sync failed"
    );
    assert_empty_v7_active_image(&successor_path);
    assert!(
        engine
            .put(
                b"rejected-after-successor-directory-sync-failure",
                b"must-not-write"
            )
            .is_err(),
        "write admission must stay closed while the runtime still owns the sealed predecessor"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "post-rename failure must not leave the temporary image behind"
    );

    drop(engine);
    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read sealed predecessor after successor-directory-sync recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "reopen must retain the sealed predecessor while verifying the installed successor"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "reopen must not leave a temporary successor image"
    );
    recovered
        .close()
        .expect("close engine after successor-directory-sync recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_rename_failure_reopens() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().expect("create v7 successor-rename directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);

    let failpoint = "wal_v7.active_image.before_rename";
    fail::cfg(failpoint, "return").expect("inject empty Active image rename failure");
    let publication = engine.create_recovery_point();
    fail::remove(failpoint);
    let error = publication.expect_err("successor rename failure must reach the caller");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("injected empty Active v7 image rename failure"),
        "the targeted rename failure must be reported, got: {error_chain}"
    );
    assert!(
        !successor_path.exists(),
        "a failed no-replace rename must not install the successor image"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "a failed no-replace rename must clean up its synced staging image"
    );
    assert!(
        engine
            .put(
                b"rejected-after-successor-rename-failure",
                b"must-not-write"
            )
            .is_err(),
        "write admission must stay closed until the authorized successor is installed"
    );

    drop(engine);
    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read sealed predecessor after successor-rename recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "reopen must install the authorized empty successor and preserve its predecessor"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "reopen must not leave a temporary successor image"
    );
    recovered
        .close()
        .expect("close engine after successor-rename recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_file_sync_failure_reopens() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().expect("create v7 successor-file-sync directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);

    let failpoint = "wal_v7.active_image.before_file_sync";
    fail::cfg(failpoint, "return").expect("inject empty Active image file sync failure");
    let publication = engine.create_recovery_point();
    fail::remove(failpoint);
    let error = publication.expect_err("successor file sync failure must reach the caller");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("injected empty Active v7 image file sync failure"),
        "the targeted file sync failure must be reported, got: {error_chain}"
    );
    assert!(
        !successor_path.exists(),
        "an unsynced staging image must not install as the canonical successor"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "a failed file sync must clean up its staging image"
    );
    assert!(
        engine
            .put(
                b"rejected-after-successor-file-sync-failure",
                b"must-not-write"
            )
            .is_err(),
        "write admission must stay closed until the authorized successor is installed"
    );

    drop(engine);
    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read sealed predecessor after successor-file-sync recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "reopen must install the authorized empty successor and preserve its predecessor"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "reopen must not leave a temporary successor image"
    );
    recovered
        .close()
        .expect("close engine after successor-file-sync recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_segment_sealed_sync_failure_reopens() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().expect("create v7 manifest-sync-failure directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);
    assert!(
        !successor_path.exists(),
        "the successor must not exist before the recovery point"
    );

    let failpoint = "pitr.manifest.segment_sealed_sync_failure";
    fail::cfg(failpoint, "return").expect("inject SegmentSealed manifest sync failure");
    let publication = engine.create_recovery_point();
    fail::remove(failpoint);
    let error = publication.expect_err("SegmentSealed sync failure must reach the caller");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("PITR manifest published but not durable"),
        "the append must be classified as published but uncertain, got: {error_chain}"
    );
    assert!(
        error_chain.contains("injected SegmentSealed manifest sync failure"),
        "the targeted manifest sync failure must be reported, got: {error:#}"
    );
    assert!(
        !successor_path.exists(),
        "the successor image must not install before SegmentSealed is synced"
    );
    assert!(
        engine
            .put(b"rejected-after-uncertain-seal", b"must-not-write")
            .is_err(),
        "write admission must stay closed after uncertain SegmentSealed publication"
    );

    drop(engine);
    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read sealed predecessor after manifest-sync recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "reopen must reconcile the appended SegmentSealed transition"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "manifest-sync recovery must not leave a partial successor staging image"
    );
    recovered
        .close()
        .expect("close engine after manifest-sync recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_segment_sealed_partial_append_reopens() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().expect("create v7 partial-manifest-append directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);

    let failpoint = "pitr.manifest.segment_sealed_partial_append_failure";
    fail::cfg(failpoint, "return").expect("inject a partial SegmentSealed manifest append");
    let publication = engine.create_recovery_point();
    fail::remove(failpoint);
    let error = publication.expect_err("partial SegmentSealed append must reach the caller");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("injected SegmentSealed manifest partial append failure"),
        "the targeted partial append failure must be reported, got: {error_chain}"
    );
    assert!(
        !error_chain.contains("PITR manifest published but not durable"),
        "revalidation must prove and remove the partial append, got: {error_chain}"
    );
    assert!(
        !successor_path.exists(),
        "the successor image must not install before SegmentSealed is complete"
    );
    assert!(
        engine
            .put(b"rejected-after-partial-seal", b"must-not-write")
            .is_err(),
        "write admission must stay closed while the sealing transition is incomplete"
    );

    let manifest_bytes = std::fs::read(db_path.join("MANIFEST"))
        .expect("read manifest after partial append revalidation");
    let manifest_records = serde_json::Deserializer::from_slice(&manifest_bytes)
        .into_iter::<serde_json::Value>()
        .collect::<Result<Vec<_>, _>>()
        .expect("revalidation must leave a parseable manifest stream");
    assert!(
        !manifest_records.is_empty(),
        "the repaired manifest must retain its prior lifecycle records"
    );

    drop(engine);
    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read predecessor after partial-append recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "reopen must finish the sealing transition from its durable intent"
    );
    recovered
        .close()
        .expect("close engine after partial-manifest-append recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_partial_manifest_append_crash_reopens() {
    let probe_dir = tempfile::tempdir().expect("create v7 partial-append crash probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(&probe_dir, probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 partial-append crash probe"),
        Err(error) => {
            panic!("v7 Parallel partial-append crash qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 partial-manifest-append crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let control_log_path = dir.path().join("chaos_control.log");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    engine
        .close()
        .expect("close empty v7 Active setup before partial-append crash");

    let crash_point = "pitr.manifest.after_segment_sealed_partial_append";
    let marker_path = dir.path().join("partial-segment-sealed-append.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &marker_path,
        crash_point,
    );
    assert_eq!(
        std::fs::read_to_string(&marker_path)
            .expect("read partial SegmentSealed append crash marker")
            .trim(),
        crash_point
    );

    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);
    assert!(
        !successor_path.exists(),
        "the successor must not be installed before SegmentSealed is complete"
    );
    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read predecessor after partial-manifest crash recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "recovery must discard the torn tail and finish from the durable sealing intent"
    );
    recovered
        .close()
        .expect("close engine after partial-manifest crash recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_manifest_append_crash() {
    let probe_dir = tempfile::tempdir().expect("create v7 manifest-crash probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(&probe_dir, probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 manifest-crash probe"),
        Err(error) => {
            panic!("v7 Parallel manifest-crash qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 manifest-crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let control_log_path = dir.path().join("chaos_control.log");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let active_wal_path = find_v7_pitr_wal(&db_path);
    let active_segment_id = pitr_segment_id(&active_wal_path);
    engine.close().expect("close empty v7 Active setup engine");

    let crash_point = "pitr.manifest.after_segment_sealed_append_before_sync";
    let marker_path = dir
        .path()
        .join("after-segment-sealed-manifest-append.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &marker_path,
        crash_point,
    );
    assert_eq!(
        std::fs::read_to_string(&marker_path)
            .expect("read SegmentSealed manifest-append crash marker")
            .trim(),
        crash_point
    );
    let successor_path = pitr_wal_path(&db_path, active_segment_id + 1);
    assert!(
        !successor_path.exists(),
        "the successor image must not be installed before SegmentSealed is synced"
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_empty_v7_active_image(&successor_path);
    assert_eq!(
        recovered
            .get(b"legacy-segment-value")
            .expect("read sealed predecessor after manifest-append recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "replaying the appended SegmentSealed transition must preserve the predecessor"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "manifest recovery must not leave a partial successor staging image"
    );
    recovered
        .close()
        .expect("close engine after manifest-append recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn v7_manifest_authorized_empty_successor_corruption_fails_open() {
    let dir = tempfile::tempdir().expect("create v7 corrupted-successor directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    let predecessor_wal_path = find_v7_pitr_wal(&db_path);
    let successor_path = pitr_wal_path(&db_path, pitr_segment_id(&predecessor_wal_path) + 1);

    assert!(matches!(
        engine.create_recovery_point(),
        Ok(RecoveryPointOutcome::Durable(_))
    ));
    assert_empty_v7_active_image(&successor_path);
    engine
        .close()
        .expect("close before corrupting Active image");

    let mut corrupted_image =
        std::fs::read(&successor_path).expect("read manifest-authorized Active image");
    corrupted_image[4096] ^= 0xff;
    std::fs::write(&successor_path, &corrupted_image).expect("corrupt generation-zero FRONTIER");

    let manifest_names = ["MANIFEST", "ENGINE_MANIFEST"];
    let manifest_images = manifest_names.map(|name| {
        let path = db_path.join(name);
        let image = path
            .try_exists()
            .expect("check manifest snapshot presence")
            .then(|| std::fs::read(&path).expect("read manifest snapshot"));
        (path, image)
    });
    assert!(
        manifest_images[0].1.is_some(),
        "MANIFEST must hold PITR authority"
    );

    let open_result = KvEngine::open_with_wal_io_mode(
        &db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    );
    let error = match open_result {
        Err(error) => error,
        Ok(engine) => {
            drop(engine);
            panic!("reopen accepted corruption in the manifest-authorized empty Active v7 image");
        }
    };
    let error_chain = format!("{error:#}").to_lowercase();
    assert!(
        error_chain.contains("invalid v7 wal frame magic"),
        "open must reject the damaged generation-zero FRONTIER, got: {error:#}"
    );
    assert_eq!(
        std::fs::read(&successor_path).expect("reread damaged Active image"),
        corrupted_image,
        "failed recovery must not normalize, replace, or truncate the damaged authorized image"
    );
    for (path, expected_image) in manifest_images {
        let actual_image = path
            .try_exists()
            .expect("check manifest snapshot after failed recovery")
            .then(|| std::fs::read(&path).expect("reread manifest snapshot"));
        assert_eq!(
            actual_image,
            expected_image,
            "failed recovery must not change authoritative manifest file {}",
            path.display()
        );
    }
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "failed recovery must not leave a replacement staging image"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_sealing_startup_stays_fenced_after_seal_started_crash() {
    let probe_dir = tempfile::tempdir().expect("create v7 Parallel sealing probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 Parallel sealing probe"),
        Err(error) => {
            panic!("v7 Parallel Sealing-startup qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 Sealing-startup directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let control_log_path = dir.path().join("chaos_control.log");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"v7-sealed-source-value", b"durable")
        .expect("write value to the Active v7 source");
    engine.sync().expect("sync the Active v7 source");
    let source_wal = find_v7_pitr_wal(&db_path);
    let source_segment_id = pitr_segment_id(&source_wal);
    engine.close().expect("close Sealing-startup setup engine");

    let crash_point = "pitr.v7.after_seal_started";
    let marker_path = dir.path().join("seal-started-crash.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &marker_path,
        crash_point,
    );
    assert_eq!(
        std::fs::read_to_string(&marker_path)
            .expect("read durable SealStarted crash marker")
            .trim(),
        crash_point
    );
    // Reopening an Active v7 WAL may normalize it before writing SealStarted,
    // so take the frozen-image snapshot at the durable manifest boundary.
    let source_at_seal_started =
        std::fs::read(&source_wal).expect("read Sealing source after crash");

    let seal_path = source_wal.with_extension("seal");
    let successor_path = pitr_wal_path(&db_path, source_segment_id + 1);
    assert!(
        !seal_path.exists(),
        "seal install must not precede the crash point"
    );
    assert!(
        !successor_path.exists(),
        "successor install must follow source sealing"
    );

    let options = LsmStorageOptions {
        enable_wal: true,
        pitr_repository: Some(repository.clone()),
        ..LsmStorageOptions::default_for_test()
    };
    let reopened = KvEngine::open_with_wal_io_mode(&db_path, options.clone(), WalIoMode::Parallel)
        .expect("reopen Sealing v7 source without auto-resuming it");
    let status = reopened
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero status page size"),
        })
        .expect("read status while source remains Sealing");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(
        status.active_wal_io_mode, None,
        "the frozen Sealing source must not acquire the requested Parallel writer"
    );
    assert!(
        reopened
            .put(b"blocked-during-sealing-recovery", b"write")
            .is_err(),
        "reopen must keep commits fenced until explicit PITR resume completes Sealing"
    );
    assert_v7_sealed_source_value(&reopened);
    assert!(
        reopened
            .put_with_ttl(
                b"ttl-blocked-during-sealing-recovery",
                b"write",
                Duration::from_secs(1),
            )
            .is_err(),
        "TTL commits must remain fenced during Sealing recovery"
    );
    assert!(
        reopened
            .delete(b"delete-blocked-during-sealing-recovery")
            .is_err(),
        "deletes must remain fenced during Sealing recovery"
    );
    assert!(
        reopened.delete_range(b"range-a", b"range-z").is_err(),
        "range deletes must remain fenced during Sealing recovery"
    );
    assert!(
        reopened
            .write_batch(&[WriteBatchRecord::Put(&b"batch-key"[..], &b"write"[..],)])
            .is_err(),
        "write batches must remain fenced during Sealing recovery"
    );
    let transaction = reopened
        .new_txn()
        .expect("create snapshot transaction while checking synchronous Sealing write fences");
    transaction
        .put(b"transaction-blocked-during-sealing-recovery", b"write")
        .expect("buffer transaction write before commit");
    assert!(
        transaction.commit().is_err(),
        "synchronous transaction commits must remain fenced until explicit PITR resume completes Sealing"
    );
    drop(transaction);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("create runtime for async Sealing fence checks");
    assert_eq!(
        runtime
            .block_on(reopened.get_async(b"v7-sealed-source-value"))
            .expect("read sealed source asynchronously while commits are fenced")
            .as_deref(),
        Some(b"durable".as_slice()),
        "async reads must remain available while Sealing recovery fences commits"
    );
    assert!(
        runtime
            .block_on(reopened.put_async(b"async-blocked-during-sealing-recovery", b"write"))
            .is_err(),
        "async commits must remain fenced until explicit PITR resume completes Sealing"
    );
    assert!(
        runtime
            .block_on(reopened.put_with_ttl_async(
                b"async-ttl-blocked-during-sealing-recovery",
                b"write",
                Duration::from_secs(1),
            ))
            .is_err(),
        "async TTL commits must remain fenced during Sealing recovery"
    );
    assert!(
        runtime
            .block_on(reopened.delete_async(b"async-delete-blocked-during-sealing-recovery"))
            .is_err(),
        "async deletes must remain fenced during Sealing recovery"
    );
    assert!(
        runtime
            .block_on(reopened.delete_range_async(b"async-range-a", b"async-range-z"))
            .is_err(),
        "async range deletes must remain fenced during Sealing recovery"
    );
    assert!(
        runtime
            .block_on(reopened.write_batch_async(&[WriteBatchRecord::Put(
                &b"async-batch-key"[..],
                &b"write"[..],
            )]))
            .is_err(),
        "async write batches must remain fenced during Sealing recovery"
    );
    let transaction = reopened
        .new_txn_async()
        .expect("create snapshot transaction while checking Sealing write fences");
    transaction
        .put(
            b"async-transaction-blocked-during-sealing-recovery",
            b"write",
        )
        .expect("buffer transaction write before commit");
    assert!(
        runtime.block_on(transaction.commit_async()).is_err(),
        "async transaction commits must remain fenced until explicit PITR resume completes Sealing"
    );
    drop(transaction);
    assert!(
        std::fs::read(&source_wal).expect("read source WAL after fenced reopen")
            == source_at_seal_started,
        "the source WAL must stay frozen while startup keeps Sealing writes fenced"
    );
    assert!(!seal_path.exists());
    assert!(!successor_path.exists());

    assert!(matches!(
        reopened.resume_pitr(&repository),
        Ok(PitrResumeOutcome::Resumed)
    ));
    let resumed_status = reopened
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero status page size"),
        })
        .expect("read status after explicit resume");
    assert_eq!(resumed_status.active_wal_format_version, Some(7));
    assert_eq!(
        resumed_status.active_wal_io_mode,
        Some(PitrWalIoMode::Parallel)
    );
    assert!(seal_path.is_file());
    assert!(successor_path.is_file());
    assert_v7_sealed_source_value(&reopened);
    assert_eq!(
        reopened
            .get(b"legacy-segment-value")
            .expect("read predecessor value after Sealing recovery")
            .as_deref(),
        Some(b"restored".as_slice())
    );
    reopened
        .put(b"after-sealing-resume-sync", b"durable")
        .expect("sync writes resume after explicit Sealing recovery");
    runtime
        .block_on(reopened.put_async(b"after-sealing-resume-async", b"durable"))
        .expect("async writes resume after explicit Sealing recovery");
    reopened.sync().expect("sync writes after Sealing recovery");
    for key in [
        b"after-sealing-resume-sync".as_slice(),
        b"after-sealing-resume-async".as_slice(),
    ] {
        assert_eq!(
            reopened
                .get(key)
                .expect("read post-resume write")
                .as_deref(),
            Some(b"durable".as_slice()),
            "explicit resume must reopen write admission"
        );
    }
    reopened
        .close()
        .expect("close engine after Sealing recovery");
    drop(reopened);

    let recovered = KvEngine::open_with_wal_io_mode(&db_path, options, WalIoMode::Parallel)
        .expect("reopen after writes resume from Sealing");
    assert_v7_sealed_source_value(&recovered);
    for key in [
        b"after-sealing-resume-sync".as_slice(),
        b"after-sealing-resume-async".as_slice(),
    ] {
        assert_eq!(
            recovered
                .get(key)
                .expect("read recovered post-resume write")
                .as_deref(),
            Some(b"durable".as_slice()),
            "post-resume writes must survive reopen"
        );
    }
    recovered
        .close()
        .expect("close engine after post-resume recovery");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_sealing_crashes_after_durable_seal_install_with_nonempty_source() {
    let probe_dir = tempfile::tempdir().expect("create v7 Parallel seal-install probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel seal-install probe"),
        Err(error) => {
            panic!("v7 Parallel seal-install qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 seal-install crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let control_log_path = dir.path().join("chaos_control.log");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"v7-sealing-source-value", b"durable")
        .expect("write nonempty v7 sealing source");
    engine.sync().expect("sync nonempty v7 sealing source");
    assert_eq!(
        engine
            .get(b"v7-sealing-source-value")
            .expect("read nonempty v7 sealing source before crash")
            .as_deref(),
        Some(b"durable".as_slice())
    );
    let source_wal = find_v7_pitr_wal(&db_path);
    let source_segment_id = pitr_segment_id(&source_wal);
    engine.close().expect("close v7 seal-install setup engine");

    let seal_path = source_wal.with_extension("seal");
    let successor_path = pitr_wal_path(&db_path, source_segment_id + 1);
    let seal_started_point = "pitr.v7.after_seal_started";
    let seal_started_marker = dir.path().join("after-seal-started.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &seal_started_marker,
        seal_started_point,
    );
    assert_eq!(
        std::fs::read_to_string(&seal_started_marker)
            .expect("read SealStarted crash marker")
            .trim(),
        seal_started_point
    );
    let source_after_seal_started =
        std::fs::read(&source_wal).expect("read source at durable SealStarted boundary");
    assert!(!seal_path.exists());
    assert!(!successor_path.exists());

    let seal_install_point = "pitr.v7.after_seal_sidecar_install";
    let seal_install_marker = dir.path().join("after-seal-sidecar-install.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &seal_install_marker,
        seal_install_point,
    );
    assert_eq!(
        std::fs::read_to_string(&seal_install_marker)
            .expect("read post-install crash marker")
            .trim(),
        seal_install_point
    );

    let seal_after_install = std::fs::read(&seal_path).expect("read durably installed seal");
    assert!(
        !seal_after_install.is_empty(),
        "the crash point must follow complete seal installation"
    );
    assert_eq!(
        std::fs::read(&source_wal).expect("read source after seal install crash"),
        source_after_seal_started,
        "sealing must not rewrite the source WAL"
    );
    assert!(
        !successor_path.exists(),
        "the successor must not be installed before SegmentSealed is durable"
    );

    // Resume the installed seal and crash again after SegmentSealed becomes
    // durable but before its authorized successor image is installed.
    let segment_sealed_point = "wal_v7.successor.after_segment_sealed_before_image_install";
    let segment_sealed_marker = dir.path().join("after-segment-sealed.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &segment_sealed_marker,
        segment_sealed_point,
    );
    assert_eq!(
        std::fs::read_to_string(&segment_sealed_marker)
            .expect("read post-SegmentSealed crash marker")
            .trim(),
        segment_sealed_point
    );
    assert_eq!(
        std::fs::read(&source_wal).expect("read source after SegmentSealed crash"),
        source_after_seal_started,
        "the source WAL must remain byte-identical after sealing"
    );
    assert_eq!(
        std::fs::read(&seal_path).expect("read seal after SegmentSealed crash"),
        seal_after_install,
        "recovery must retain the exact installed seal"
    );
    assert!(
        !successor_path.exists(),
        "the child must crash before installing the successor image"
    );

    let options = LsmStorageOptions {
        enable_wal: true,
        pitr_repository: Some(repository.clone()),
        ..LsmStorageOptions::default_for_test()
    };
    let reopened = KvEngine::open_with_wal_io_mode(&db_path, options, WalIoMode::Parallel)
        .expect("reopen Sealed v7 source and recreate its missing successor");
    assert!(successor_path.is_file());
    let status = reopened
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero status page size"),
        })
        .expect("read status after sealed-successor recovery");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(
        status.active_wal_io_mode,
        Some(PitrWalIoMode::Parallel),
        "the reconstructed Active successor must use the requested Parallel mode"
    );
    for (key, expected) in [
        (b"legacy-segment-value".as_slice(), b"restored".as_slice()),
        (b"v7-sealing-source-value".as_slice(), b"durable".as_slice()),
    ] {
        assert_eq!(
            reopened
                .get(key)
                .expect("read value after seal recovery")
                .as_deref(),
            Some(expected),
            "sealed-source recovery must preserve {key:?}"
        );
    }
    assert!(matches!(
        reopened.resume_pitr(&repository),
        Ok(PitrResumeOutcome::Resumed)
    ));
    reopened
        .close()
        .expect("close engine after nonempty seal recovery");
    assert_eq!(
        std::fs::read(&source_wal).expect("read frozen source after close"),
        source_after_seal_started
    );
    assert_eq!(
        std::fs::read(&seal_path).expect("read frozen seal after close"),
        seal_after_install
    );
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_empty_successor_install_crash_boundaries_preserves_nonempty_source() {
    let probe_dir = tempfile::tempdir().expect("create v7 empty-successor probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(&probe_dir, probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 empty-successor probe"),
        Err(error) => {
            panic!("v7 Parallel empty-successor qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 empty-successor crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let control_log_path = dir.path().join("chaos_control.log");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"v7-sealed-source-value", b"durable")
        .expect("write value to the nonempty v7 source");
    engine.sync().expect("sync the nonempty v7 source");
    let initial_active_wal = find_v7_pitr_wal(&db_path);
    let initial_active_segment_id = pitr_segment_id(&initial_active_wal);
    engine.close().expect("close empty v7 Active setup engine");

    let before_install_point = "wal_v7.successor.after_segment_sealed_before_image_install";
    let before_install_marker = dir.path().join("before-empty-image-install.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &before_install_marker,
        before_install_point,
    );
    assert_eq!(
        std::fs::read_to_string(&before_install_marker)
            .expect("read pre-install crash marker")
            .trim(),
        before_install_point
    );
    let source_seal_path = initial_active_wal.with_extension("seal");
    let sealed_source_wal = std::fs::read(&initial_active_wal).expect("read sealed source WAL");
    let sealed_source_seal = std::fs::read(&source_seal_path).expect("read installed source seal");
    let first_successor_path = pitr_wal_path(&db_path, initial_active_segment_id + 1);
    assert!(
        !first_successor_path.exists(),
        "the manifest-authorized successor must be absent at the pre-install crash boundary"
    );

    let recovered_after_preinstall_crash = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    assert_empty_v7_active_image(&first_successor_path);
    assert_v7_sealed_source_value(&recovered_after_preinstall_crash);
    assert_eq!(
        recovered_after_preinstall_crash
            .get(b"legacy-segment-value")
            .expect("read predecessor value after pre-install recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "recreating the empty successor must preserve its sealed predecessor"
    );
    recovered_after_preinstall_crash
        .close()
        .expect("close engine after pre-install recovery");

    let after_header_write_point =
        "wal_v7.active_image.after_temp_header_write_before_frontier_write";
    let after_header_write_marker = dir.path().join("after-empty-image-header-write.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &after_header_write_marker,
        after_header_write_point,
    );
    assert_eq!(
        std::fs::read_to_string(&after_header_write_marker)
            .expect("read staged-image header crash marker")
            .trim(),
        after_header_write_point
    );
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    let second_successor_path = pitr_wal_path(&db_path, initial_active_segment_id + 2);
    assert!(
        !second_successor_path.exists(),
        "a header-only staging image must not appear at the canonical path"
    );
    let staging_images = pitr_wal_install_staging_files(&db_path);
    assert_eq!(
        staging_images.len(),
        1,
        "the crash after staging-image header write must leave exactly one install temp file"
    );
    let header_only_image =
        std::fs::read(&staging_images[0]).expect("read header-only staging image");
    assert_eq!(
        header_only_image.len(),
        4096,
        "the staging file must contain only the written header"
    );
    assert_eq!(&header_only_image[..4], b"WAL2");
    assert_eq!(
        u16::from_be_bytes([header_only_image[4], header_only_image[5]]),
        7
    );

    let recovered_after_header_write_crash = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    assert_empty_v7_active_image(&second_successor_path);
    assert_v7_sealed_source_value(&recovered_after_header_write_crash);
    let recreated_image =
        std::fs::read(&second_successor_path).expect("read recreated empty v7 successor");
    assert_eq!(
        header_only_image,
        recreated_image[..4096],
        "the partial staging image must contain the exact canonical WAL header"
    );
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "startup must remove the header-only staging image before recreating the successor"
    );
    assert_eq!(
        recovered_after_header_write_crash
            .get(b"legacy-segment-value")
            .expect("read predecessor value after header-only staging recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "recreating the successor after a partial-image crash must preserve its sealed predecessor"
    );
    recovered_after_header_write_crash
        .close()
        .expect("close engine after header-only staging recovery");

    let after_temp_write_point = "wal_v7.active_image.after_temp_write_before_sync";
    let after_temp_write_marker = dir.path().join("after-empty-image-temp-write.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &after_temp_write_marker,
        after_temp_write_point,
    );
    assert_eq!(
        std::fs::read_to_string(&after_temp_write_marker)
            .expect("read staged-image write crash marker")
            .trim(),
        after_temp_write_point
    );
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    let third_successor_path = pitr_wal_path(&db_path, initial_active_segment_id + 3);
    assert!(
        !third_successor_path.exists(),
        "a written staging image must not appear at the canonical path before rename"
    );
    let staging_images = pitr_wal_install_staging_files(&db_path);
    assert_eq!(
        staging_images.len(),
        1,
        "the crash after staging-image write must leave exactly one install temp file"
    );
    assert_empty_v7_active_image(&staging_images[0]);

    let recovered_after_temp_write_crash = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    assert_empty_v7_active_image(&third_successor_path);
    assert_v7_sealed_source_value(&recovered_after_temp_write_crash);
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "startup must remove the stranded staging image before recreating the successor"
    );
    assert_eq!(
        recovered_after_temp_write_crash
            .get(b"legacy-segment-value")
            .expect("read predecessor value after staged-image recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "recreating the successor after a staged-image crash must preserve its sealed predecessor"
    );
    recovered_after_temp_write_crash
        .close()
        .expect("close engine after staged-image recovery");

    let after_temp_sync_point = "wal_v7.active_image.after_temp_sync_before_rename";
    let after_temp_sync_marker = dir.path().join("after-empty-image-temp-sync.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &after_temp_sync_marker,
        after_temp_sync_point,
    );
    assert_eq!(
        std::fs::read_to_string(&after_temp_sync_marker)
            .expect("read staged-image sync crash marker")
            .trim(),
        after_temp_sync_point
    );
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    let fourth_successor_path = pitr_wal_path(&db_path, initial_active_segment_id + 4);
    assert!(
        !fourth_successor_path.exists(),
        "a synced staging image must not appear at the canonical path before rename"
    );
    assert_eq!(
        pitr_wal_install_staging_files(&db_path).len(),
        1,
        "the crash after staging-image sync must leave exactly one install temp file"
    );

    let recovered_after_temp_sync_crash = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    assert_empty_v7_active_image(&fourth_successor_path);
    assert_v7_sealed_source_value(&recovered_after_temp_sync_crash);
    assert!(
        pitr_wal_install_staging_files(&db_path).is_empty(),
        "startup must remove the stranded staging image before recreating the successor"
    );
    assert_eq!(
        recovered_after_temp_sync_crash
            .get(b"legacy-segment-value")
            .expect("read predecessor value after staged-image sync recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "recreating the successor after a synced-image crash must preserve its sealed predecessor"
    );
    recovered_after_temp_sync_crash
        .close()
        .expect("close engine after staged-image sync recovery");

    let after_rename_point = "wal_v7.active_image.after_rename_before_directory_sync";
    let after_rename_marker = dir.path().join("after-empty-image-rename.marker");
    kill_pitr_child_at_empty_successor_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &after_rename_marker,
        after_rename_point,
    );
    assert_eq!(
        std::fs::read_to_string(&after_rename_marker)
            .expect("read post-rename crash marker")
            .trim(),
        after_rename_point
    );
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    let fifth_successor_path = pitr_wal_path(&db_path, initial_active_segment_id + 5);
    assert_empty_v7_active_image(&fifth_successor_path);

    let recovered_after_rename_crash = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    let active_wal_path = fifth_successor_path;
    assert_empty_v7_active_image(&active_wal_path);
    assert_v7_sealed_source_value(&recovered_after_rename_crash);
    assert_eq!(
        recovered_after_rename_crash
            .get(b"legacy-segment-value")
            .expect("read predecessor value after post-rename recovery")
            .as_deref(),
        Some(b"restored".as_slice()),
        "reopening an installed empty successor must retain its sealed predecessor"
    );
    let empty_successor_len = std::fs::metadata(&active_wal_path)
        .expect("stat empty successor before post-recovery write")
        .len();
    recovered_after_rename_crash
        .close()
        .expect("close engine after post-rename recovery");

    let continued_write_crash_point = "parallel_wal.after_fdatasync";
    let continued_write_marker = dir.path().join("after-empty-successor-write-sync.marker");
    kill_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &continued_write_marker,
        continued_write_crash_point,
        None,
        Some(("after-empty-successor-recovery", "durable")),
    );
    assert_eq!(
        std::fs::read_to_string(&continued_write_marker)
            .expect("read post-recovery write crash marker")
            .trim(),
        continued_write_crash_point
    );
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    assert!(
        std::fs::metadata(&active_wal_path)
            .expect("stat Active WAL after post-recovery write sync")
            .len()
            > empty_successor_len,
        "the post-recovery write must extend the recovered empty Active WAL"
    );

    let reopened = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_sealed_source_unchanged(
        &initial_active_wal,
        &source_seal_path,
        &sealed_source_wal,
        &sealed_source_seal,
    );
    assert_eq!(
        reopened
            .get(b"after-empty-successor-recovery")
            .expect("read post-recovery write after process crash")
            .as_deref(),
        Some(b"durable".as_slice()),
        "a synced write after empty-successor recovery must survive process crash before publication"
    );
    assert_v7_sealed_source_value(&reopened);
    assert_eq!(
        reopened
            .get(b"legacy-segment-value")
            .expect("read sealed predecessor after post-recovery write crash")
            .as_deref(),
        Some(b"restored".as_slice())
    );

    let status = reopened
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read PITR status before sealing post-recovery write");
    let interval = status
        .recoverable_intervals
        .first()
        .expect("PITR setup must retain a recoverable base interval");
    let selector = RecoverySelector {
        timeline_id: interval.timeline_id,
        archive_epoch_id: Some(interval.archive_epoch_id),
        base_backup_id: None,
    };
    let final_point = match reopened
        .close_pitr()
        .expect("seal and archive the post-recovery write")
    {
        PitrCloseOutcome::ClosedDurably {
            final_point: Some(point),
        } => point,
        outcome => panic!("PITR close did not durably archive the recovered WAL: {outcome:?}"),
    };
    assert!(
        final_point.commit_ts.is_some(),
        "the post-recovery write must be included in the archived boundary"
    );

    let restored_path = dir.path().join("restored-after-empty-successor-recovery");
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
        .expect("restore latest after empty-successor recovery continuation")
    {
        RestoreToOutcome::Restored(info) => info,
        outcome => panic!("PITR restore did not produce a restored database: {outcome:?}"),
    };
    assert_eq!(restore_info.resolved_commit_ts, final_point.commit_ts);
    let restored = KvEngine::open(&restored_path, LsmStorageOptions::default_for_test())
        .expect("open database restored after empty-successor recovery continuation");
    for (key, expected) in [
        (b"legacy-segment-value".as_slice(), b"restored".as_slice()),
        (b"v7-sealed-source-value".as_slice(), b"durable".as_slice()),
        (
            b"after-empty-successor-recovery".as_slice(),
            b"durable".as_slice(),
        ),
    ] {
        assert_eq!(
            restored
                .get(key)
                .expect("read restored PITR value")
                .as_deref(),
            Some(expected),
            "Latest restore must retain {key:?}"
        );
    }
    restored.close().expect("close restored database");
}

#[cfg(target_os = "linux")]
#[test]
fn v7_parallel_pitr_sync_and_async_write_apis_survive_reopen() {
    let dir = tempfile::tempdir().expect("create v7 public API regression directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);

    engine.put(b"sync-put", b"value").expect("sync put");
    engine
        .put_with_ttl(b"sync-ttl", b"value", Duration::from_secs(3600))
        .expect("sync TTL put");
    engine
        .put_with_ttl(b"sync-expired-ttl", b"gone", Duration::ZERO)
        .expect("sync expired TTL put");
    engine
        .put(b"sync-delete", b"old")
        .expect("seed sync delete");
    engine.delete(b"sync-delete").expect("sync delete");
    for (key, value) in [
        (b"range-a".as_slice(), b"a".as_slice()),
        (b"range-b".as_slice(), b"b".as_slice()),
        (b"range-c".as_slice(), b"c".as_slice()),
        (b"range-d".as_slice(), b"d".as_slice()),
    ] {
        engine.put(key, value).expect("seed sync range delete");
    }
    engine
        .delete_range(b"range-b", b"range-d")
        .expect("sync range delete");
    engine
        .put(b"batch-delete", b"old")
        .expect("seed sync batch delete");
    engine
        .write_batch(&[
            WriteBatchRecord::Put(&b"batch-put"[..], &b"value"[..]),
            WriteBatchRecord::PutWithTtl(&b"batch-ttl"[..], &b"value"[..], 3600),
            WriteBatchRecord::PutWithTtl(&b"batch-expired-ttl"[..], &b"gone"[..], 0),
            WriteBatchRecord::Del(&b"batch-delete"[..]),
        ])
        .expect("sync write batch");
    let transaction = engine.new_txn().expect("create sync transaction");
    transaction
        .put(b"sync-transaction", b"value")
        .expect("buffer sync transaction write");
    transaction.commit().expect("commit sync transaction");
    drop(transaction);
    engine.sync().expect("sync the fixed synchronous v7 prefix");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create async API runtime");
    runtime
        .block_on(engine.put_async(b"async-put", b"value"))
        .expect("async put");
    runtime
        .block_on(engine.put_with_ttl_async(b"async-ttl", b"value", Duration::from_secs(3600)))
        .expect("async TTL put");
    runtime
        .block_on(engine.put_with_ttl_async(b"async-expired-ttl", b"gone", Duration::ZERO))
        .expect("async expired TTL put");
    runtime
        .block_on(engine.put_async(b"async-delete", b"old"))
        .expect("seed async delete");
    runtime
        .block_on(engine.delete_async(b"async-delete"))
        .expect("async delete");
    runtime
        .block_on(engine.put_async(b"async-range-a", b"a"))
        .expect("seed async range start");
    runtime
        .block_on(engine.put_async(b"async-range-b", b"b"))
        .expect("seed async range middle");
    runtime
        .block_on(engine.put_async(b"async-range-c", b"c"))
        .expect("seed async range end");
    runtime
        .block_on(engine.delete_range_async(b"async-range-b", b"async-range-c"))
        .expect("async range delete");
    runtime
        .block_on(engine.write_batch_async(&[
            WriteBatchRecord::Put(&b"async-batch-put"[..], &b"value"[..]),
            WriteBatchRecord::PutWithTtl(&b"async-batch-ttl"[..], &b"value"[..], 3600),
            WriteBatchRecord::PutWithTtl(&b"async-batch-expired-ttl"[..], &b"gone"[..], 0),
        ]))
        .expect("async write batch");
    let transaction = engine.new_txn_async().expect("create async transaction");
    transaction
        .put(b"async-transaction", b"value")
        .expect("buffer async transaction write");
    runtime
        .block_on(transaction.commit_async())
        .expect("commit async transaction");
    drop(transaction);
    for key in [
        b"sync-expired-ttl".as_slice(),
        b"batch-expired-ttl".as_slice(),
        b"async-expired-ttl".as_slice(),
        b"async-batch-expired-ttl".as_slice(),
    ] {
        assert_eq!(
            engine
                .get(key)
                .expect("read expired v7 TTL value before reopen"),
            None,
            "zero-duration TTL must be expired before reopen: {key:?}"
        );
    }
    runtime
        .block_on(engine.sync_async())
        .expect("async sync the fixed v7 commit prefix");
    engine.close().expect("close v7 API regression engine");

    let reopened = open_and_resume_parallel_pitr(&db_path, &repository);
    let status = reopened
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: NonZeroUsize::new(64).expect("nonzero PITR status page size"),
        })
        .expect("read PITR status after reopen");
    assert_eq!(status.active_wal_format_version, Some(7));
    assert_eq!(status.active_wal_io_mode, Some(PitrWalIoMode::Parallel));

    for (key, expected) in [
        (b"legacy-segment-value".as_slice(), b"restored".as_slice()),
        (b"sync-put".as_slice(), b"value".as_slice()),
        (b"sync-ttl".as_slice(), b"value".as_slice()),
        (b"range-a".as_slice(), b"a".as_slice()),
        (b"range-d".as_slice(), b"d".as_slice()),
        (b"batch-put".as_slice(), b"value".as_slice()),
        (b"batch-ttl".as_slice(), b"value".as_slice()),
        (b"sync-transaction".as_slice(), b"value".as_slice()),
        (b"async-put".as_slice(), b"value".as_slice()),
        (b"async-ttl".as_slice(), b"value".as_slice()),
        (b"async-range-a".as_slice(), b"a".as_slice()),
        (b"async-range-c".as_slice(), b"c".as_slice()),
        (b"async-batch-put".as_slice(), b"value".as_slice()),
        (b"async-batch-ttl".as_slice(), b"value".as_slice()),
        (b"async-transaction".as_slice(), b"value".as_slice()),
    ] {
        assert_eq!(
            reopened
                .get(key)
                .expect("read recovered v7 API value")
                .as_deref(),
            Some(expected),
            "reopened v7 database must retain {key:?}"
        );
    }
    for key in [
        b"sync-expired-ttl".as_slice(),
        b"batch-expired-ttl".as_slice(),
        b"async-expired-ttl".as_slice(),
        b"async-batch-expired-ttl".as_slice(),
    ] {
        assert_eq!(
            reopened
                .get(key)
                .expect("read expired TTL value after reopen"),
            None,
            "reopen must preserve expiration for {key:?}"
        );
    }
    for key in [
        b"sync-delete".as_slice(),
        b"range-b".as_slice(),
        b"range-c".as_slice(),
        b"batch-delete".as_slice(),
        b"async-delete".as_slice(),
        b"async-range-b".as_slice(),
    ] {
        assert_eq!(
            reopened.get(key).expect("read deleted v7 API value"),
            None,
            "reopened v7 database must keep {key:?} deleted"
        );
    }
    reopened
        .close()
        .expect("close reopened v7 API regression engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_pitr_checkpoint_captures_its_fixed_cutoff() {
    let _failpoint_guard = lock_v7_async_checkpoint_failpoint();
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().expect("create v7 checkpoint regression directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);

    engine
        .put(b"before-checkpoint-cutoff", b"captured")
        .expect("write before checkpoint cutoff");
    engine
        .create_checkpoint(&checkpoint_path)
        .expect("capture checkpoint at v7 commit cutoff");
    engine
        .put(b"after-checkpoint-cutoff", b"later")
        .expect("write after checkpoint cutoff");

    let checkpoint = KvEngine::open(&checkpoint_path, LsmStorageOptions::default_for_test())
        .expect("open standalone checkpoint");
    for (key, expected) in [
        (b"legacy-segment-value".as_slice(), b"restored".as_slice()),
        (
            b"before-checkpoint-cutoff".as_slice(),
            b"captured".as_slice(),
        ),
    ] {
        assert_eq!(
            checkpoint
                .get(key)
                .expect("read checkpoint value")
                .as_deref(),
            Some(expected),
            "checkpoint must retain {key:?}"
        );
    }
    assert_eq!(
        checkpoint
            .get(b"after-checkpoint-cutoff")
            .expect("read key written after checkpoint cutoff"),
        None,
        "checkpoint must not include writes admitted after its cutoff"
    );
    checkpoint.close().expect("close standalone checkpoint");
    engine.close().expect("close v7 checkpoint source");

    let reopened = open_and_resume_parallel_pitr(&db_path, &repository);
    for (key, expected) in [
        (
            b"before-checkpoint-cutoff".as_slice(),
            b"captured".as_slice(),
        ),
        (b"after-checkpoint-cutoff".as_slice(), b"later".as_slice()),
    ] {
        assert_eq!(
            reopened
                .get(key)
                .expect("read source value after checkpoint and reopen")
                .as_deref(),
            Some(expected),
            "source reopen must retain {key:?}"
        );
    }
    reopened
        .close()
        .expect("close reopened v7 checkpoint source");
}

#[cfg(target_os = "linux")]
fn lock_v7_async_checkpoint_failpoint() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<parking_lot::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| parking_lot::Mutex::new(())).lock()
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_pitr_async_checkpoint_captures_its_fixed_cutoff() {
    let _failpoint_guard = lock_v7_async_checkpoint_failpoint();
    let scenario = FailScenario::setup();
    let (capture_entered_tx, capture_entered_rx) = std::sync::mpsc::channel();
    let (release_capture_tx, release_capture_rx) = std::sync::mpsc::channel();
    let release_capture_rx = std::sync::Mutex::new(release_capture_rx);
    fail::cfg_callback("checkpoint.after_capture_boundary_before_copy", move || {
        capture_entered_tx
            .send(())
            .expect("signal that async checkpoint pinned its cutoff");
        release_capture_rx
            .lock()
            .expect("lock checkpoint capture release channel")
            .recv_timeout(Duration::from_secs(30))
            .expect("release paused async checkpoint capture");
    })
    .expect("pause async checkpoint after its fixed cutoff");

    let dir = tempfile::tempdir().expect("create v7 async checkpoint regression directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);

    engine
        .put(b"before-async-checkpoint-cutoff", b"captured")
        .expect("write before async checkpoint cutoff");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("create async checkpoint runtime");
    let checkpoint_engine = engine.clone();
    let checkpoint_path_for_task = checkpoint_path.clone();
    let checkpoint_task = runtime.spawn(async move {
        checkpoint_engine
            .create_checkpoint_async(&checkpoint_path_for_task)
            .await
    });
    let capture_paused = capture_entered_rx
        .recv_timeout(Duration::from_secs(10))
        .is_ok();
    let post_cutoff_write = if capture_paused {
        runtime.block_on(engine.put_async(b"after-async-checkpoint-cutoff", b"later"))
    } else {
        Err(anyhow::anyhow!(
            "async checkpoint did not pause after pinning its cutoff"
        ))
    };
    let _ = release_capture_tx.send(());
    let checkpoint_result = runtime
        .block_on(checkpoint_task)
        .expect("join async checkpoint capture");
    fail::remove("checkpoint.after_capture_boundary_before_copy");
    scenario.teardown();
    assert!(
        capture_paused,
        "async checkpoint did not reach its post-cutoff pause"
    );
    post_cutoff_write.expect("write after the checkpoint cutoff while file copy is paused");
    checkpoint_result.expect("capture async checkpoint at v7 commit cutoff");

    let checkpoint = KvEngine::open(&checkpoint_path, LsmStorageOptions::default_for_test())
        .expect("open standalone async checkpoint");
    for (key, expected) in [
        (b"legacy-segment-value".as_slice(), b"restored".as_slice()),
        (
            b"before-async-checkpoint-cutoff".as_slice(),
            b"captured".as_slice(),
        ),
    ] {
        assert_eq!(
            checkpoint
                .get(key)
                .expect("read async checkpoint value")
                .as_deref(),
            Some(expected),
            "async checkpoint must retain {key:?}"
        );
    }
    assert_eq!(
        checkpoint
            .get(b"after-async-checkpoint-cutoff")
            .expect("read key written after async checkpoint cutoff"),
        None,
        "async checkpoint must exclude writes admitted after its cutoff"
    );
    checkpoint
        .close()
        .expect("close standalone async checkpoint");
    engine.close().expect("close v7 async checkpoint source");

    let reopened = open_and_resume_parallel_pitr(&db_path, &repository);
    for (key, expected) in [
        (
            b"before-async-checkpoint-cutoff".as_slice(),
            b"captured".as_slice(),
        ),
        (
            b"after-async-checkpoint-cutoff".as_slice(),
            b"later".as_slice(),
        ),
    ] {
        assert_eq!(
            reopened
                .get(key)
                .expect("read source value after async checkpoint and reopen")
                .as_deref(),
            Some(expected),
            "source reopen must retain {key:?}"
        );
    }
    reopened
        .close()
        .expect("close reopened v7 async checkpoint source");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_checkpoint_cancellation_keeps_cutoff_and_close_waiting() {
    let _failpoint_guard = lock_v7_async_checkpoint_failpoint();
    let scenario = FailScenario::setup();
    let (capture_entered_tx, capture_entered_rx) = std::sync::mpsc::channel();
    let (release_capture_tx, release_capture_rx) = std::sync::mpsc::channel();
    let release_capture_rx = std::sync::Mutex::new(release_capture_rx);
    fail::cfg_callback("checkpoint.after_capture_boundary_before_copy", move || {
        capture_entered_tx
            .send(())
            .expect("signal that async checkpoint pinned its cutoff");
        release_capture_rx
            .lock()
            .expect("lock checkpoint capture release channel")
            .recv_timeout(Duration::from_secs(30))
            .expect("release paused async checkpoint capture");
    })
    .expect("pause async checkpoint after its fixed cutoff");

    let dir = tempfile::tempdir().expect("create v7 async checkpoint cancellation directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"before-cancelled-async-checkpoint", b"captured")
        .expect("write before async checkpoint cutoff");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("create async checkpoint cancellation runtime");
    let checkpoint_engine = engine.clone();
    let checkpoint_path_for_task = checkpoint_path.clone();
    let checkpoint_task = runtime.spawn(async move {
        checkpoint_engine
            .create_checkpoint_async(&checkpoint_path_for_task)
            .await
    });
    let capture_paused = capture_entered_rx
        .recv_timeout(Duration::from_secs(10))
        .is_ok();
    checkpoint_task.abort();
    let checkpoint_cancelled = runtime
        .block_on(checkpoint_task)
        .is_err_and(|error| error.is_cancelled());
    let post_cutoff_write = if capture_paused {
        runtime.block_on(engine.put_async(b"after-cancelled-checkpoint-cutoff", b"later"))
    } else {
        Err(anyhow::anyhow!(
            "async checkpoint did not pause after pinning its cutoff"
        ))
    };

    let close_engine = engine.clone();
    let mut close_task = runtime.spawn(async move { close_engine.close_async().await });
    let close_waited_for_checkpoint = runtime.block_on(async {
        tokio::time::timeout(Duration::from_millis(100), &mut close_task)
            .await
            .is_err()
    });
    let _ = release_capture_tx.send(());
    let close_result = runtime
        .block_on(close_task)
        .expect("join async close after checkpoint cancellation");
    fail::remove("checkpoint.after_capture_boundary_before_copy");
    scenario.teardown();

    assert!(
        capture_paused,
        "async checkpoint did not reach its post-cutoff pause"
    );
    assert!(checkpoint_cancelled, "checkpoint caller was not cancelled");
    assert!(
        close_waited_for_checkpoint,
        "close_async must wait for the detached v7 checkpoint worker"
    );
    post_cutoff_write.expect("write after checkpoint cutoff while copied files remain pinned");
    close_result.expect("close v7 source after detached checkpoint completes");

    let checkpoint = KvEngine::open(&checkpoint_path, LsmStorageOptions::default_for_test())
        .expect("open checkpoint completed after caller cancellation");
    assert_eq!(
        checkpoint
            .get(b"before-cancelled-async-checkpoint")
            .expect("read value captured before async checkpoint cutoff")
            .as_deref(),
        Some(b"captured".as_slice())
    );
    assert_eq!(
        checkpoint
            .get(b"after-cancelled-checkpoint-cutoff")
            .expect("read value written after async checkpoint cutoff"),
        None,
        "cancelled checkpoint must preserve its already-pinned cutoff"
    );
    checkpoint
        .close()
        .expect("close checkpoint completed after caller cancellation");

    let reopened = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        reopened
            .get(b"after-cancelled-checkpoint-cutoff")
            .expect("read source write after cancelled checkpoint and close")
            .as_deref(),
        Some(b"later".as_slice())
    );
    reopened
        .close()
        .expect("close reopened v7 source after checkpoint cancellation");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_pitr_async_sync_survives_crash_after_return() {
    let probe_dir = tempfile::tempdir().expect("create v7 async sync crash probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(&probe_dir, probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 async sync crash probe"),
        Err(error) => panic!("v7 Parallel async sync qualification requires io_uring: {error:#}"),
    }

    let dir = tempfile::tempdir().expect("create v7 async sync crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"async-sync-crash-baseline", b"stable")
        .expect("persist acknowledged async sync baseline");
    engine
        .close()
        .expect("close v7 async sync crash setup engine");
    let boundary_before_async_sync = latest_persisted_active_boundary(&db_path);

    kill_pitr_child_at_crash_point_with_operation(
        &db_path,
        &repository,
        &dir.path().join("async-sync-control.log"),
        &dir.path().join("async-sync-returned.marker"),
        "async_sync.returned",
        PitrChildCrashOperation::AsyncSync {
            acknowledged_write: Some(("async-sync-boundary-value", "durable")),
        },
    );
    let boundary_after_async_sync = latest_persisted_active_boundary(&db_path);
    assert!(
        boundary_after_async_sync.0 > boundary_before_async_sync.0,
        "returned sync_async must advance the persisted ticket boundary: before={boundary_before_async_sync:?}, after={boundary_after_async_sync:?}"
    );
    assert!(
        boundary_after_async_sync.1 > boundary_before_async_sync.1,
        "returned sync_async must advance the persisted physical boundary: before={boundary_before_async_sync:?}, after={boundary_after_async_sync:?}"
    );
    assert!(
        boundary_after_async_sync.2 > boundary_before_async_sync.2,
        "returned sync_async must advance the persisted commit timestamp: before={boundary_before_async_sync:?}, after={boundary_after_async_sync:?}"
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"async-sync-crash-baseline")
            .expect("read acknowledged async sync baseline after crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"async-sync-boundary-value")
            .expect("read write covered by returned async sync after crash")
            .as_deref(),
        Some(b"durable".as_slice()),
        "sync_async must persist its Active boundary before returning"
    );
    recovered
        .close()
        .expect("close recovered v7 async sync engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_pitr_crash_after_sync_before_ack_recovers() {
    let probe_dir = tempfile::tempdir().expect("create v7 async crash probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine.close().expect("close v7 Parallel async crash probe"),
        Err(error) => panic!("v7 Parallel async crash qualification requires io_uring: {error:#}"),
    }

    let dir = tempfile::tempdir().expect("create v7 async crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"async-crash-baseline", b"stable")
        .expect("persist acknowledged async-crash baseline");
    engine.close().expect("close v7 async crash setup engine");

    let control_log_path = dir.path().join("async-crash-control.log");
    let marker_path = dir.path().join("async-crash.marker");
    kill_v7_async_pitr_child_at_crash_point(
        &db_path,
        &repository,
        &control_log_path,
        &marker_path,
        "parallel_wal.after_fdatasync",
        Some(("async-synced-before-ack", "candidate")),
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"async-crash-baseline")
            .expect("read acknowledged baseline after async crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"async-synced-before-ack")
            .expect("read async value synced before caller acknowledgement")
            .as_deref(),
        Some(b"candidate".as_slice()),
        "a successful v7 WAL sync must survive a process crash before async publication or return"
    );
    recovered
        .close()
        .expect("close recovered v7 async crash engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_transaction_crash_after_sync_before_ack_recovers() {
    let probe_dir = tempfile::tempdir().expect("create v7 async transaction probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel async transaction crash probe"),
        Err(error) => {
            panic!("v7 Parallel async transaction crash qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 async transaction crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"async-transaction-crash-baseline", b"stable")
        .expect("persist acknowledged async-transaction crash baseline");
    engine
        .close()
        .expect("close v7 async transaction crash setup engine");

    kill_pitr_child_at_crash_point_with_operation(
        &db_path,
        &repository,
        &dir.path().join("async-transaction-crash-control.log"),
        &dir.path().join("async-transaction-crash.marker"),
        "parallel_wal.after_fdatasync",
        PitrChildCrashOperation::AsyncTransaction {
            acknowledged_write: None,
            write: Some(("async-transaction-synced-before-ack", "candidate")),
        },
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"async-transaction-crash-baseline")
            .expect("read acknowledged async-transaction baseline after crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"async-transaction-synced-before-ack")
            .expect("read transaction value synced before caller acknowledgement")
            .as_deref(),
        Some(b"candidate".as_slice()),
        "a successful transaction WAL sync must survive a crash before MVCC publication or acknowledgement"
    );
    recovered
        .close()
        .expect("close recovered v7 async transaction crash engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_batch_crash_after_sync_before_ack_recovers_all_entries() {
    let probe_dir = tempfile::tempdir().expect("create v7 async batch probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel async batch crash probe"),
        Err(error) => {
            panic!("v7 Parallel async batch crash qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 async batch crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"async-batch-crash-baseline", b"stable")
        .expect("persist acknowledged async-batch crash baseline");
    engine
        .close()
        .expect("close v7 async batch crash setup engine");

    kill_pitr_child_at_crash_point_with_operation(
        &db_path,
        &repository,
        &dir.path().join("async-batch-crash-control.log"),
        &dir.path().join("async-batch-crash.marker"),
        "parallel_wal.after_fdatasync",
        PitrChildCrashOperation::AsyncBatch {
            acknowledged_write: None,
            writes: [
                ("async-batch-first-synced-before-ack", "first"),
                ("async-batch-second-synced-before-ack", "second"),
            ],
        },
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"async-batch-crash-baseline")
            .expect("read acknowledged async-batch baseline after crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    for (key, expected) in [
        (
            b"async-batch-first-synced-before-ack".as_slice(),
            b"first".as_slice(),
        ),
        (
            b"async-batch-second-synced-before-ack".as_slice(),
            b"second".as_slice(),
        ),
    ] {
        assert_eq!(
            recovered
                .get(key)
                .expect("read async batch entry synced before caller acknowledgment")
                .as_deref(),
            Some(expected),
            "all entries in a successfully synced async batch must recover after a pre-ack crash"
        );
    }
    recovered
        .close()
        .expect("close recovered v7 async batch crash engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_delete_crash_after_sync_before_ack_recovers_tombstone() {
    let probe_dir = tempfile::tempdir().expect("create v7 async delete probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel async delete crash probe"),
        Err(error) => {
            panic!("v7 Parallel async delete crash qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 async delete crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"async-delete-crash-baseline", b"stable")
        .expect("persist acknowledged async delete baseline");
    engine
        .put(b"async-delete-crash-target", b"prior")
        .expect("persist async delete target before crash");
    engine
        .close()
        .expect("close v7 async delete crash setup engine");

    kill_pitr_child_at_crash_point_with_operation(
        &db_path,
        &repository,
        &dir.path().join("async-delete-crash-control.log"),
        &dir.path().join("async-delete-crash.marker"),
        "parallel_wal.after_fdatasync",
        PitrChildCrashOperation::AsyncDelete {
            acknowledged_write: None,
            key: "async-delete-crash-target",
        },
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"async-delete-crash-baseline")
            .expect("read acknowledged async delete baseline after crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"async-delete-crash-target")
            .expect("read async delete target after crash"),
        None,
        "a synced async point tombstone must replay before caller acknowledgment"
    );
    recovered
        .close()
        .expect("close recovered v7 async delete crash engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_range_delete_crash_after_sync_before_ack_recovers_tombstone() {
    let probe_dir = tempfile::tempdir().expect("create v7 async range-delete probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel async range-delete crash probe"),
        Err(error) => {
            panic!(
                "v7 Parallel async range-delete crash qualification requires io_uring: {error:#}"
            )
        }
    }

    let dir = tempfile::tempdir().expect("create v7 async range-delete crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"0-async-range-crash-baseline", b"stable")
        .expect("persist acknowledged async range-delete baseline");
    engine
        .put(b"async-range-crash-middle", b"inside")
        .expect("persist async range-delete interior value");
    engine
        .put(b"async-range-crash-z", b"endpoint")
        .expect("persist async range-delete exclusive endpoint");
    engine
        .close()
        .expect("close v7 async range-delete crash setup engine");

    kill_pitr_child_at_crash_point_with_operation(
        &db_path,
        &repository,
        &dir.path().join("async-range-crash-control.log"),
        &dir.path().join("async-range-crash.marker"),
        "parallel_wal.after_fdatasync",
        PitrChildCrashOperation::AsyncRangeDelete {
            acknowledged_write: None,
            range: ("async-range-crash-a", "async-range-crash-z"),
        },
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"0-async-range-crash-baseline")
            .expect("read acknowledged async range-delete baseline after crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"async-range-crash-middle")
            .expect("read async range-delete interior key after crash"),
        None,
        "a synced async range tombstone must replay before caller acknowledgment"
    );
    assert_eq!(
        recovered
            .get(b"async-range-crash-z")
            .expect("read async range-delete exclusive endpoint after crash")
            .as_deref(),
        Some(b"endpoint".as_slice()),
        "replaying the half-open range tombstone must preserve its exclusive endpoint"
    );
    recovered
        .close()
        .expect("close recovered v7 async range-delete crash engine");
}

#[cfg(target_os = "linux")]
#[test]
fn failpoint_v7_parallel_async_ttl_crash_after_sync_before_ack_recovers_expiration() {
    let probe_dir = tempfile::tempdir().expect("create v7 async TTL probe directory");
    let probe_options = LsmStorageOptions {
        enable_wal: true,
        ..LsmStorageOptions::default_for_test()
    };
    match KvEngine::open_with_wal_io_mode(probe_dir.path(), probe_options, WalIoMode::Parallel) {
        Ok(engine) => engine
            .close()
            .expect("close v7 Parallel async TTL crash probe"),
        Err(error) => {
            panic!("v7 Parallel async TTL crash qualification requires io_uring: {error:#}")
        }
    }

    let dir = tempfile::tempdir().expect("create v7 async TTL crash directory");
    let db_path = dir.path().join("db");
    let repository = dir.path().join("repository");
    let engine = prepare_empty_v7_parallel_pitr(&db_path, &repository);
    engine
        .put(b"async-ttl-crash-baseline", b"stable")
        .expect("persist acknowledged async TTL baseline");
    engine
        .put(b"async-ttl-synced-before-ack", b"prior")
        .expect("persist prior value before async TTL overwrite");
    engine
        .close()
        .expect("close v7 async TTL crash setup engine");

    kill_pitr_child_at_crash_point_with_operation(
        &db_path,
        &repository,
        &dir.path().join("async-ttl-crash-control.log"),
        &dir.path().join("async-ttl-crash.marker"),
        "parallel_wal.after_fdatasync",
        PitrChildCrashOperation::AsyncTtlWrite {
            acknowledged_write: None,
            write: Some(("async-ttl-synced-before-ack", "candidate")),
            ttl_secs: 0,
        },
    );

    let recovered = open_and_resume_parallel_pitr(&db_path, &repository);
    assert_eq!(
        recovered
            .get(b"async-ttl-crash-baseline")
            .expect("read acknowledged async TTL baseline after crash")
            .as_deref(),
        Some(b"stable".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"async-ttl-synced-before-ack")
            .expect("read async TTL overwrite synced before caller acknowledgment"),
        None,
        "recovery must apply the synced zero-duration expiration rather than restore a permanent value or reveal the overwritten value"
    );
    recovered
        .close()
        .expect("close recovered v7 async TTL crash engine");
}

#[cfg(target_os = "linux")]
fn prepare_empty_v7_parallel_pitr(
    db_path: &std::path::Path,
    repository: &std::path::Path,
) -> std::sync::Arc<KvEngine> {
    let engine = KvEngine::open(
        db_path,
        LsmStorageOptions {
            enable_wal: true,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .expect("open database for empty-segment PITR setup");
    assert!(matches!(
        engine.create_backup(BackupOptions {
            repository: repository.to_path_buf(),
            use_hard_links: false,
        }),
        Ok(CreateBackupOutcome::Committed(_))
    ));
    assert!(matches!(
        engine.enable_pitr(PitrOptions {
            repository: repository.to_path_buf(),
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

    let engine = open_and_resume_parallel_pitr(db_path, repository);
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
            .resume_pitr(repository)
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
    engine
}

#[cfg(target_os = "linux")]
fn open_and_resume_parallel_pitr(
    db_path: &std::path::Path,
    repository: &std::path::Path,
) -> std::sync::Arc<KvEngine> {
    let engine = KvEngine::open_with_wal_io_mode(
        db_path,
        LsmStorageOptions {
            enable_wal: true,
            pitr_repository: Some(repository.to_path_buf()),
            ..LsmStorageOptions::default_for_test()
        },
        WalIoMode::Parallel,
    )
    .expect("open PITR database with the v7 Parallel writer");
    let outcome = engine
        .resume_pitr(repository)
        .expect("resume PITR with the v7 Parallel writer");
    assert!(
        matches!(outcome, PitrResumeOutcome::Resumed),
        "PITR resume must complete reconciliation, got {outcome:?}"
    );
    engine
}

#[cfg(target_os = "linux")]
fn pitr_wal_path(db_path: &std::path::Path, segment_id: u64) -> PathBuf {
    db_path.join(format!("pitr-{segment_id:020}.wal"))
}

#[cfg(target_os = "linux")]
fn pitr_wal_install_staging_files(db_path: &std::path::Path) -> Vec<PathBuf> {
    std::fs::read_dir(db_path)
        .expect("list database files")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".pitr-") && name.contains(".wal.tmp-"))
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn pitr_segment_id(path: &std::path::Path) -> u64 {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("pitr-"))
        .and_then(|name| name.strip_suffix(".wal"))
        .and_then(|segment_id| segment_id.parse().ok())
        .expect("parse PITR WAL segment ID")
}

#[cfg(target_os = "linux")]
fn assert_empty_v7_active_image(path: &std::path::Path) {
    let image = std::fs::read(path).expect("read empty v7 Active WAL");
    assert_eq!(image.len(), 2 * 4096, "empty v7 image is header plus F0");
    assert_eq!(u16::from_be_bytes([image[4], image[5]]), 7);
    assert_eq!(&image[4096..4104], b"TKVW7FR1");
    assert_eq!(
        u16::from_be_bytes([image[4106], image[4107]]),
        2,
        "empty Active v7 image must contain generation-zero FRONTIER"
    );
}

#[cfg(target_os = "linux")]
fn assert_v7_sealed_source_value(engine: &KvEngine) {
    assert_eq!(
        engine
            .get(b"v7-sealed-source-value")
            .expect("read sealed nonempty v7 source value")
            .as_deref(),
        Some(b"durable".as_slice()),
        "successor installation must preserve the nonempty sealed v7 source"
    );
}

#[cfg(target_os = "linux")]
fn assert_sealed_source_unchanged(
    wal_path: &std::path::Path,
    seal_path: &std::path::Path,
    expected_wal: &[u8],
    expected_seal: &[u8],
) {
    assert_eq!(
        std::fs::read(wal_path).expect("read immutable sealed source WAL"),
        expected_wal,
        "successor recovery must not rewrite the sealed source WAL"
    );
    assert_eq!(
        std::fs::read(seal_path).expect("read immutable source seal"),
        expected_seal,
        "successor recovery must not rewrite the source seal"
    );
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum PitrChildCrashOperation<'a> {
    CreateEmptySuccessor,
    Write {
        acknowledged_write: Option<(&'a str, &'a str)>,
        write: Option<(&'a str, &'a str)>,
    },
    AsyncWrite {
        acknowledged_write: Option<(&'a str, &'a str)>,
        write: Option<(&'a str, &'a str)>,
    },
    AsyncTransaction {
        acknowledged_write: Option<(&'a str, &'a str)>,
        write: Option<(&'a str, &'a str)>,
    },
    AsyncBatch {
        acknowledged_write: Option<(&'a str, &'a str)>,
        writes: [(&'a str, &'a str); 2],
    },
    AsyncDelete {
        acknowledged_write: Option<(&'a str, &'a str)>,
        key: &'a str,
    },
    AsyncRangeDelete {
        acknowledged_write: Option<(&'a str, &'a str)>,
        range: (&'a str, &'a str),
    },
    AsyncTtlWrite {
        acknowledged_write: Option<(&'a str, &'a str)>,
        write: Option<(&'a str, &'a str)>,
        ttl_secs: u64,
    },
    AsyncSync {
        acknowledged_write: Option<(&'a str, &'a str)>,
    },
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
    kill_pitr_child_at_crash_point_with_operation(
        db_path,
        repository,
        control_log_path,
        marker_path,
        crash_point,
        PitrChildCrashOperation::Write {
            acknowledged_write,
            write,
        },
    );
}

#[cfg(target_os = "linux")]
fn kill_v7_async_pitr_child_at_crash_point(
    db_path: &std::path::Path,
    repository: &std::path::Path,
    control_log_path: &std::path::Path,
    marker_path: &std::path::Path,
    crash_point: &str,
    write: Option<(&str, &str)>,
) {
    kill_pitr_child_at_crash_point_with_operation(
        db_path,
        repository,
        control_log_path,
        marker_path,
        crash_point,
        PitrChildCrashOperation::AsyncWrite {
            acknowledged_write: None,
            write,
        },
    );
}

#[cfg(target_os = "linux")]
fn kill_pitr_child_at_empty_successor_crash_point(
    db_path: &std::path::Path,
    repository: &std::path::Path,
    control_log_path: &std::path::Path,
    marker_path: &std::path::Path,
    crash_point: &str,
) {
    kill_pitr_child_at_crash_point_with_operation(
        db_path,
        repository,
        control_log_path,
        marker_path,
        crash_point,
        PitrChildCrashOperation::CreateEmptySuccessor,
    );
}

#[cfg(target_os = "linux")]
fn kill_pitr_child_at_crash_point_with_operation(
    db_path: &std::path::Path,
    repository: &std::path::Path,
    control_log_path: &std::path::Path,
    marker_path: &std::path::Path,
    crash_point: &str,
    operation: PitrChildCrashOperation<'_>,
) {
    kill_pitr_child_at_crash_point_with_mode(
        db_path,
        repository,
        control_log_path,
        marker_path,
        crash_point,
        WalIoMode::Parallel,
        operation,
    );
}

#[cfg(target_os = "linux")]
fn kill_pitr_child_at_crash_point_with_mode(
    db_path: &std::path::Path,
    repository: &std::path::Path,
    control_log_path: &std::path::Path,
    marker_path: &std::path::Path,
    crash_point: &str,
    wal_io_mode: WalIoMode,
    operation: PitrChildCrashOperation<'_>,
) {
    let crash_occurrence = match operation {
        PitrChildCrashOperation::Write {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncWrite {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncTransaction {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncBatch {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncDelete {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncRangeDelete {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncTtlWrite {
            acknowledged_write: Some(_),
            ..
        } => "2",
        PitrChildCrashOperation::AsyncSync { .. } => "1",
        PitrChildCrashOperation::CreateEmptySuccessor
        | PitrChildCrashOperation::Write {
            acknowledged_write: None,
            ..
        }
        | PitrChildCrashOperation::AsyncWrite {
            acknowledged_write: None,
            ..
        }
        | PitrChildCrashOperation::AsyncTransaction {
            acknowledged_write: None,
            ..
        }
        | PitrChildCrashOperation::AsyncDelete {
            acknowledged_write: None,
            ..
        } => "1",
        PitrChildCrashOperation::AsyncBatch {
            acknowledged_write: None,
            ..
        } => "1",
        PitrChildCrashOperation::AsyncRangeDelete {
            acknowledged_write: None,
            ..
        } => "1",
        PitrChildCrashOperation::AsyncTtlWrite {
            acknowledged_write: None,
            ..
        } => "1",
    };
    let scenario = match wal_io_mode {
        WalIoMode::Leader => "pitr-v7-leader-crash",
        WalIoMode::Parallel => "pitr-v7-parallel-crash",
    };
    let mut child = Command::new(chaos_child_path());
    child
        .arg("--child")
        .arg("--scenario")
        .arg(scenario)
        .arg("--seed")
        .arg("42")
        .arg("--db-path")
        .arg(db_path)
        .arg("--pitr-repository")
        .arg(repository)
        .arg("--control-log-path")
        .arg(control_log_path)
        .arg("--crash-point")
        .arg(crash_point)
        .env(PARALLEL_WAL_CRASH_POINT_ENV, crash_point)
        .env(PARALLEL_WAL_CRASH_OCCURRENCE_ENV, crash_occurrence)
        .env(PARALLEL_WAL_CRASH_MARKER_ENV, marker_path);
    if wal_io_mode == WalIoMode::Parallel {
        child.arg("--parallel-wal");
    }
    let (acknowledged_write, write) = match operation {
        PitrChildCrashOperation::CreateEmptySuccessor => {
            child.arg("--create-empty-successor");
            (None, None)
        }
        PitrChildCrashOperation::Write {
            acknowledged_write,
            write,
        } => (acknowledged_write, write),
        PitrChildCrashOperation::AsyncWrite {
            acknowledged_write,
            write,
        } => {
            child.arg("--async-write");
            (acknowledged_write, write)
        }
        PitrChildCrashOperation::AsyncTransaction {
            acknowledged_write,
            write,
        } => {
            child.arg("--async-transaction");
            (acknowledged_write, write)
        }
        PitrChildCrashOperation::AsyncBatch {
            acknowledged_write,
            writes: [first, second],
        } => {
            child
                .arg("--async-batch")
                .arg("--second-write-key")
                .arg(second.0)
                .arg("--second-write-value")
                .arg(second.1);
            (acknowledged_write, Some(first))
        }
        PitrChildCrashOperation::AsyncDelete {
            acknowledged_write,
            key,
        } => {
            child.arg("--async-delete").arg("--delete-key").arg(key);
            (acknowledged_write, None)
        }
        PitrChildCrashOperation::AsyncRangeDelete {
            acknowledged_write,
            range,
        } => {
            child.arg("--async-range-delete");
            (acknowledged_write, Some(range))
        }
        PitrChildCrashOperation::AsyncTtlWrite {
            acknowledged_write,
            write,
            ttl_secs,
        } => {
            child
                .arg("--async-ttl-write")
                .arg("--ttl-secs")
                .arg(ttl_secs.to_string());
            (acknowledged_write, write)
        }
        PitrChildCrashOperation::AsyncSync { acknowledged_write } => {
            child
                .arg("--async-sync")
                .arg("--async-sync-completion-marker")
                .arg(marker_path);
            (acknowledged_write, None)
        }
    };
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
fn latest_persisted_active_boundary(manifest_path: &std::path::Path) -> (u64, u64, u64) {
    let manifest_bytes = std::fs::read(manifest_path.join("MANIFEST"))
        .expect("read manifest for persisted Active boundary assertion");
    let records = serde_json::Deserializer::from_slice(&manifest_bytes)
        .into_iter::<serde_json::Value>()
        .collect::<Result<Vec<_>, _>>()
        .expect("decode manifest record stream for Active boundary assertion");
    let boundary = records
        .iter()
        .filter_map(|record| {
            record
                .get("Pitr")?
                .get("ActiveBoundaryAdvanced")?
                .get("boundary")
        })
        .next_back()
        .expect("find persisted ActiveBoundaryAdvanced record");
    (
        boundary["ticket_end"]
            .as_u64()
            .expect("read persisted Active ticket boundary"),
        boundary["durable_end"]
            .as_u64()
            .expect("read persisted Active physical boundary"),
        boundary["last_commit_ts"]
            .as_u64()
            .expect("read persisted Active commit timestamp"),
    )
}

#[cfg(target_os = "linux")]
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else if file_type.is_file() {
            std::fs::copy(entry.path(), target)?;
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn append_speculative_frontier_after_zero_tail(
    wal_path: &std::path::Path,
    image: &[u8],
    max_segment_bytes: usize,
) -> (usize, usize, Vec<u8>) {
    const FRAME_LEN: usize = 4096;
    const HEADER_LEN: usize = 4096;

    assert!(image.len().is_multiple_of(FRAME_LEN));
    assert!(image.len() <= max_segment_bytes);
    let candidate_frontier_offset = image
        .as_chunks::<FRAME_LEN>()
        .0
        .iter()
        .enumerate()
        .filter_map(|(frame_index, frame)| {
            let frame_offset = frame_index * FRAME_LEN;
            (frame_offset >= HEADER_LEN
                && frame[..8] == *b"TKVW7FR1"
                && u16::from_be_bytes([frame[10], frame[11]]) == 2)
                .then_some(frame_offset)
        })
        .next_back()
        .expect("synced candidate WAL must contain a FRONTIER frame");
    assert!(candidate_frontier_offset > HEADER_LEN);
    assert_eq!(
        u64::from_be_bytes(
            image[candidate_frontier_offset + 48..candidate_frontier_offset + 56]
                .try_into()
                .expect("candidate frame offset has eight bytes")
        ),
        u64::try_from(candidate_frontier_offset).expect("candidate offset fits u64")
    );

    let first_tail_slot = candidate_frontier_offset + FRAME_LEN;
    let trailing_frontier_offset = (first_tail_slot..=image.len().saturating_sub(2 * FRAME_LEN))
        .step_by(FRAME_LEN)
        .find(|offset| {
            image[*offset..*offset + FRAME_LEN]
                .iter()
                .all(|byte| *byte == 0)
                && image[*offset + FRAME_LEN..*offset + 2 * FRAME_LEN]
                    .iter()
                    .all(|byte| *byte == 0)
        })
        .unwrap_or_else(|| {
            let offset = image.len();
            let new_len = offset
                .checked_add(2 * FRAME_LEN)
                .expect("speculative tail image length does not overflow");
            assert!(
                new_len <= max_segment_bytes,
                "test WAL must have room for a speculative FRONTIER and zero tail"
            );
            std::fs::OpenOptions::new()
                .write(true)
                .open(wal_path)
                .expect("open Active WAL to create a zero-filled speculative tail")
                .set_len(u64::try_from(new_len).expect("test WAL length fits u64"))
                .expect("extend Active WAL with a zero-filled speculative tail");
            offset
        });

    let mut trailing_frontier =
        image[candidate_frontier_offset..candidate_frontier_offset + FRAME_LEN].to_vec();
    trailing_frontier[48..56].copy_from_slice(
        &u64::try_from(trailing_frontier_offset)
            .expect("trailing frontier offset fits u64")
            .to_be_bytes(),
    );
    let mut crc = crc32fast::Hasher::new();
    crc.update(&trailing_frontier[..60]);
    crc.update(&trailing_frontier[64..]);
    let frame_crc = crc.finalize();
    trailing_frontier[60..64].copy_from_slice(&frame_crc.to_be_bytes());

    let mut wal = std::fs::OpenOptions::new()
        .write(true)
        .open(wal_path)
        .expect("open Active WAL to append a speculative FRONTIER");
    wal.seek(SeekFrom::Start(
        u64::try_from(trailing_frontier_offset).expect("trailing offset fits u64"),
    ))
    .expect("seek to the zero-filled speculative tail slot");
    wal.write_all(&trailing_frontier)
        .expect("write a complete speculative FRONTIER frame");
    wal.sync_all()
        .expect("persist the speculative FRONTIER and zero-filled tail");

    (
        candidate_frontier_offset,
        trailing_frontier_offset,
        trailing_frontier,
    )
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
