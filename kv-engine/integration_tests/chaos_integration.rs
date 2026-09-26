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
use kv_engine::lsm_storage::KvEngine;
use kv_engine::wal::WalIoMode;
use std::io::Read;
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
