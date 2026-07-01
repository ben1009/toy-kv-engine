//! Integration tests for the chaos-testing harness (RFC 013 Phase 1).
//!
//! These tests spawn the `chaos-child` binary as a real OS process, wait for it
//! to reach a sync point, send SIGKILL, reopen the database in-process, and
//! validate crash invariants using the control log oracle.

#![cfg(feature = "chaos-testing")]

use kv_engine::chaos::control_log::{ControlLogReader, OperationKind};
use kv_engine::chaos::oracle::{self, BoundedKeyUniverse, ReferenceState};
use kv_engine::chaos::scenarios::ScenarioConfig;
use kv_engine::lsm_storage::KvEngine;
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
    eprintln!(
        "chaos '{scenario_name}' passed: {} keys checked, {} committed ops, {} possibly-visible",
        result.total_keys_checked, result.committed_op_count, result.possibly_visible_op_count,
    );
}

// ============================================================================
// Test cases — one per scenario
// ============================================================================

#[test]
fn chaos_wal_only() {
    run_chaos_scenario("wal-only", &ScenarioConfig::wal_only());
}

#[test]
fn chaos_flush_boundary() {
    run_chaos_scenario("flush-boundary", &ScenarioConfig::flush_boundary());
}

#[test]
fn chaos_manifest_snapshot() {
    run_chaos_scenario("manifest-snapshot", &ScenarioConfig::manifest_snapshot());
}
