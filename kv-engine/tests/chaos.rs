#![cfg(feature = "chaos-testing")]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use kv_engine::chaos::{
    CONTROL_LOG_FILENAME,
    control_log::{ControlLogReader, OperationKind},
    oracle::{self, BoundedKeyUniverse, ReferenceState},
    stress::{self, StressPhase, StressScenario},
};

struct FailureArtifacts<'a> {
    artifact_root: &'a Path,
    seed: u64,
    cycle: u64,
    db_path: &'a Path,
    log_path: &'a Path,
    child_bin: &'a Path,
    scenario: &'a StressScenario,
    plan: &'a stress::StressCyclePlan,
}

#[test]
fn phase4_stress_crash_loop_smoke() {
    let seed = 0x5eed_f00d_cafe_babe;
    let scenario = StressScenario::from_seed(seed);
    let universe = BoundedKeyUniverse::new(&scenario.key_prefix, scenario.key_space);

    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("db");
    let log_path = tempdir.path().join(CONTROL_LOG_FILENAME);
    let child_bin = std::env::var("CARGO_BIN_EXE_chaos-child")
        .expect("CARGO_BIN_EXE_chaos-child is required for chaos tests");
    let child_bin_path = PathBuf::from(&child_bin);
    let artifact_root = PathBuf::from("target/chaos-artifacts");

    for cycle in 0..3u64 {
        let (planned_scenario, plan) = stress::plan_cycle(seed, cycle);
        assert_eq!(
            scenario.key_prefix,
            planned_scenario.key_prefix,
            "phase planning drifted for {}",
            stress::cycle_report(seed, cycle)
        );
        let mut child = spawn_child(&child_bin, seed, cycle, &db_path, &log_path);
        wait_for_new_sync_point(
            &log_path,
            (cycle + 1).try_into().unwrap(),
            Duration::from_secs(20),
        )
        .unwrap_or_else(|e| {
            let _ = child.kill();
            let _ = child.wait();
            write_failure_artifacts(
                &FailureArtifacts {
                    artifact_root: &artifact_root,
                    seed,
                    cycle,
                    db_path: &db_path,
                    log_path: &log_path,
                    child_bin: &child_bin_path,
                    scenario: &scenario,
                    plan: &plan,
                },
                None,
                false,
                &format!("timed out waiting for sync point: {e}"),
            );
            panic!(
                "timed out waiting for sync point: {e}; {}",
                stress::cycle_report(seed, cycle)
            );
        });
        child.kill().unwrap_or_else(|e| {
            panic!(
                "failed to kill chaos child: {e}; {}",
                stress::cycle_report(seed, cycle)
            )
        });
        child.wait().unwrap_or_else(|e| {
            panic!(
                "failed to reap chaos child: {e}; {}",
                stress::cycle_report(seed, cycle)
            )
        });

        let reader = ControlLogReader::open(&log_path).unwrap_or_else(|e| {
            write_failure_artifacts(
                &FailureArtifacts {
                    artifact_root: &artifact_root,
                    seed,
                    cycle,
                    db_path: &db_path,
                    log_path: &log_path,
                    child_bin: &child_bin_path,
                    scenario: &scenario,
                    plan: &plan,
                },
                None,
                false,
                &format!("failed to read control log: {e}"),
            );
            panic!(
                "failed to read control log: {e}; {}",
                stress::cycle_report(seed, cycle)
            )
        });
        let reference = ReferenceState::from_control_log(&reader);
        let (engine, wal_enabled) =
            stress::open_stress_engine(&db_path, &scenario.storage_options, None).unwrap_or_else(
                |e| {
                    write_failure_artifacts(
                        &FailureArtifacts {
                            artifact_root: &artifact_root,
                            seed,
                            cycle,
                            db_path: &db_path,
                            log_path: &log_path,
                            child_bin: &child_bin_path,
                            scenario: &scenario,
                            plan: &plan,
                        },
                        None,
                        false,
                        &format!("failed to reopen engine: {e}"),
                    );
                    panic!(
                        "failed to reopen engine: {e}; {}",
                        stress::cycle_report(seed, cycle)
                    )
                },
            );

        // WAL-off crash cycles may legitimately lose the active memtable, so
        // the strict key-universe oracle only applies to WAL-backed runs.
        let verify_result = match (plan.phase, wal_enabled) {
            (StressPhase::Stress, _) | (StressPhase::Verify, false) => None,
            (StressPhase::Verify, true) => Some(
                oracle::reconcile(&engine, &universe, &reference).unwrap_or_else(|e| {
                    write_failure_artifacts(
                        &FailureArtifacts {
                            artifact_root: &artifact_root,
                            seed,
                            cycle,
                            db_path: &db_path,
                            log_path: &log_path,
                            child_bin: &child_bin_path,
                            scenario: &scenario,
                            plan: &plan,
                        },
                        Some(&engine),
                        wal_enabled,
                        &format!("reconciliation failed: {e}"),
                    );
                    panic!(
                        "reconciliation failed: {e}; {}",
                        stress::summarize_cycle_failure(seed, cycle, &log_path, &[])
                    )
                }),
            ),
        };
        engine.close().unwrap_or_else(|e| {
            panic!(
                "failed to close reopened engine: {e}; {}",
                stress::cycle_report(seed, cycle)
            )
        });
        if let Some(result) = verify_result
            && !result.violations.is_empty()
        {
            write_failure_artifacts(
                &FailureArtifacts {
                    artifact_root: &artifact_root,
                    seed,
                    cycle,
                    db_path: &db_path,
                    log_path: &log_path,
                    child_bin: &child_bin_path,
                    scenario: &scenario,
                    plan: &plan,
                },
                Some(&engine),
                wal_enabled,
                &format!("violations={:?}", result.violations),
            );
            panic!(
                "{}",
                stress::summarize_cycle_failure(seed, cycle, &log_path, &result.violations)
            );
        }
        oracle::structural_checks(&db_path, &scenario.storage_options).unwrap_or_else(|e| {
            let phase_label = match plan.phase {
                StressPhase::Stress => "stress-phase",
                StressPhase::Verify => "verify-phase",
            };
            write_failure_artifacts(
                &FailureArtifacts {
                    artifact_root: &artifact_root,
                    seed,
                    cycle,
                    db_path: &db_path,
                    log_path: &log_path,
                    child_bin: &child_bin_path,
                    scenario: &scenario,
                    plan: &plan,
                },
                Some(&engine),
                wal_enabled,
                &format!("{phase_label} structural checks failed: {e}"),
            );
            panic!(
                "{phase_label} structural checks failed: {e}; {}",
                stress::cycle_report(seed, cycle)
            )
        });
    }
}

fn spawn_child(child_bin: &str, seed: u64, cycle: u64, db_path: &Path, log_path: &Path) -> Child {
    Command::new(child_bin)
        .arg("--child")
        .arg("--scenario")
        .arg("stress")
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--cycle")
        .arg(cycle.to_string())
        .arg("--db-path")
        .arg(db_path)
        .arg("--control-log-path")
        .arg(log_path)
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "failed to spawn chaos child: {e}; {}",
                stress::cycle_report(seed, cycle)
            )
        })
}

fn wait_for_new_sync_point(
    log_path: &Path,
    expected_sync_points: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let reader = ControlLogReader::open(log_path)
            .map_err(|e| format!("failed to read control log {}: {e}", log_path.display()))?;
        let sync_points = reader
            .records()
            .iter()
            .filter(|record| matches!(record.kind, OperationKind::SyncPoint))
            .count();
        if sync_points >= expected_sync_points {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for sync point {} in {} (saw {sync_points})",
                expected_sync_points,
                log_path.display()
            ));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn write_failure_artifacts(
    ctx: &FailureArtifacts<'_>,
    engine: Option<&kv_engine::lsm_storage::KvEngine>,
    wal_enabled: bool,
    reason: &str,
) {
    let artifact_dir = ctx
        .artifact_root
        .join(format!("seed-{:016x}-cycle-{:04}", ctx.seed, ctx.cycle));
    if let Err(e) = fs::create_dir_all(&artifact_dir) {
        eprintln!(
            "chaos artifact capture failed: could not create {}: {}",
            artifact_dir.display(),
            e
        );
        return;
    }

    if let Err(e) = copy_dir_recursive(ctx.db_path, &artifact_dir.join("db")) {
        eprintln!(
            "chaos artifact capture failed: could not copy db dir {}: {}",
            ctx.db_path.display(),
            e
        );
    }
    if let Err(e) = fs::copy(ctx.log_path, artifact_dir.join("control.log")) {
        eprintln!(
            "chaos artifact capture failed: could not copy control log {}: {}",
            ctx.log_path.display(),
            e
        );
    }

    let mut report = String::new();
    report.push_str(&format!("reason={reason}\n"));
    report.push_str(&format!("seed={}\ncycle={}\n", ctx.seed, ctx.cycle));
    report.push_str(&format!("phase={:?}\n", ctx.plan.phase));
    report.push_str(&format!("scenario={}\n", ctx.scenario.describe()));
    report.push_str(&format!("operations={:#?}\n", ctx.plan.operations));
    report.push_str(&format!("db_path={}\n", ctx.db_path.display()));
    report.push_str(&format!("control_log_path={}\n", ctx.log_path.display()));
    report.push_str(&format!("wal_enabled={wal_enabled}\n"));
    report.push_str(&format!(
        "replay_command={}\n",
        stress::replay_command(
            ctx.child_bin,
            ctx.seed,
            ctx.cycle,
            ctx.db_path,
            ctx.log_path,
            wal_enabled,
        )
    ));
    if let Some(engine) = engine {
        report.push_str("structure=\n");
        report.push_str(&engine.dump_structure_string());
    }
    if let Err(e) = fs::write(artifact_dir.join("report.txt"), report) {
        eprintln!(
            "chaos artifact capture failed: could not write report {}: {}",
            artifact_dir.display(),
            e
        );
    }
    eprintln!(
        "chaos failure artifacts written to {}",
        artifact_dir.display()
    );
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}
