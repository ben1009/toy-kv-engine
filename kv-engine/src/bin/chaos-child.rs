#![cfg(feature = "chaos-testing")]

mod wrapper;

use wrapper::kv_engine_wrapper::chaos::control_log::ControlLogWriter;
use wrapper::kv_engine_wrapper::chaos::scenarios::{self, ScenarioConfig};
use wrapper::kv_engine_wrapper::chaos::stress::{self, StressScenario};
use wrapper::kv_engine_wrapper::lsm_storage::{KvEngine, LsmStorageOptions};
use wrapper::kv_engine_wrapper::pitr_api::{
    PitrStatusOptions, PitrWalIoMode, RecoveryPointOutcome,
};
use wrapper::kv_engine_wrapper::wal::WalIoMode;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if !args.iter().any(|a| a == "--child") {
        return; // pass as test when no --child flag
    }
    if let Err(e) = child_main(&args) {
        eprintln!("chaos-child: fatal: {e}");
        std::process::exit(1);
    }
}

fn child_main(args: &[String]) -> Result<(), String> {
    // Parse args
    let scenario = get_arg(args, "--scenario").ok_or("missing --scenario")?;
    let seed: u64 = get_arg(args, "--seed")
        .ok_or("missing --seed")?
        .parse()
        .map_err(|_| "invalid --seed")?;
    let cycle: u64 = get_arg(args, "--cycle")
        .map(|s| s.parse().map_err(|_| "invalid --cycle"))
        .transpose()?
        .unwrap_or(0);
    let db_path = get_arg(args, "--db-path").ok_or("missing --db-path")?;
    let log_path = get_arg(args, "--control-log-path").ok_or("missing --control-log-path")?;
    let wal_io_mode = if args.iter().any(|arg| arg == "--parallel-wal") {
        WalIoMode::Parallel
    } else {
        WalIoMode::Leader
    };
    let replay = args.iter().any(|a| a == "--replay");
    let effective_wal = get_arg(args, "--effective-wal")
        .map(|value| match value.as_str() {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err("invalid --effective-wal"),
        })
        .transpose()?;
    if replay && get_arg(args, "--cycle").is_none() {
        return Err("--cycle is required in --replay mode".to_string());
    }
    if replay && scenario == "stress" && effective_wal.is_none() {
        return Err("--effective-wal is required in stress --replay mode".to_string());
    }

    if scenario == "stress" {
        let stress_cycles = get_arg(args, "--stress-cycles")
            .map(|value| value.parse::<u64>().map_err(|_| "invalid --stress-cycles"))
            .transpose()?;
        let stress_seconds = get_arg(args, "--stress-seconds")
            .map(|value| value.parse::<u64>().map_err(|_| "invalid --stress-seconds"))
            .transpose()?;
        let progress_every = get_arg(args, "--stress-progress-every")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| "invalid --stress-progress-every")
            })
            .transpose()?
            .unwrap_or(1);
        let scenario_config = StressScenario::from_seed(seed);
        if stress_cycles.is_some() || stress_seconds.is_some() {
            let limits = stress::StressRunLimits {
                max_cycles: stress_cycles,
                max_duration: stress_seconds.map(std::time::Duration::from_secs),
                progress_every: progress_every.max(1),
            };
            let progress = stress::run_loop(
                std::path::Path::new(&db_path),
                std::path::Path::new(&log_path),
                seed,
                cycle,
                limits,
            )?;
            eprintln!(
                "chaos-stress complete: seed={seed} cycles_completed={} last_cycle={}",
                progress.cycles_completed, progress.last_cycle
            );
            return Ok(());
        }
        if replay {
            let mut replay_options = scenario_config.storage_options.clone();
            if let Some(enable_wal) = effective_wal {
                replay_options.enable_wal = enable_wal;
            }
            let (engine, wal_enabled) = stress::open_stress_engine(
                std::path::Path::new(&db_path),
                &replay_options,
                effective_wal,
            )?;
            let mut log = ControlLogWriter::new(log_path.as_str())
                .map_err(|e| format!("ControlLogWriter::new failed: {e}"))?;
            stress::run_cycle(&engine, &mut log, seed, cycle)?;
            eprintln!(
                "chaos-stress replay ready: seed={seed} cycle={cycle} wal_enabled={} {}",
                wal_enabled,
                stress::cycle_report(seed, cycle)
            );
            loop {
                std::thread::park();
            }
        }
        let (engine, wal_enabled) = stress::open_stress_engine(
            std::path::Path::new(&db_path),
            &scenario_config.storage_options,
            None,
        )?;
        let mut log = ControlLogWriter::new(log_path.as_str())
            .map_err(|e| format!("ControlLogWriter::new failed: {e}"))?;
        stress::run_cycle(&engine, &mut log, seed, cycle)?;
        eprintln!(
            "chaos-stress cycle complete: seed={seed} cycle={cycle} wal_enabled={} {}",
            wal_enabled,
            stress::cycle_report(seed, cycle)
        );
        loop {
            std::thread::park();
        }
    }

    // Look up scenario config
    let mut config = match scenario.as_str() {
        "wal-only" => ScenarioConfig::wal_only(),
        "parallel-wal-crash" => ScenarioConfig::wal_only(),
        "pitr-v7-parallel-crash" => ScenarioConfig::wal_only(),
        "flush-boundary" => ScenarioConfig::flush_boundary(),
        "manifest-snapshot" => ScenarioConfig::manifest_snapshot(),
        "range-tombstone" => ScenarioConfig::range_tombstone(),
        "vlog" => ScenarioConfig::vlog(),
        "leveled-compaction" => ScenarioConfig::leveled_compaction(),
        "tiered-compaction" => ScenarioConfig::tiered_compaction(),
        other => return Err(format!("unknown scenario: {other}")),
    };

    if scenario == "pitr-v7-parallel-crash" {
        let repository = get_arg(args, "--pitr-repository")
            .ok_or("missing --pitr-repository for v7 PITR crash scenario")?;
        config.storage_options = LsmStorageOptions {
            pitr_repository: Some(repository.into()),
            ..config.storage_options
        };
    }

    // Open the database
    let engine = KvEngine::open_with_wal_io_mode(
        db_path.as_str(),
        config.storage_options.clone(),
        wal_io_mode,
    )
    .map_err(|e| format!("KvEngine::open failed: {e}"))?;

    // Create the control log writer
    let mut log = ControlLogWriter::new(log_path.as_str())
        .map_err(|e| format!("ControlLogWriter::new failed: {e}"))?;

    // Run the scenario
    match scenario.as_str() {
        "parallel-wal-crash" => {
            run_parallel_wal_crash(&engine, args)?;
            return Err("configured parallel WAL crash boundary was not reached".to_string());
        }
        #[cfg(target_os = "linux")]
        "pitr-v7-parallel-crash" => {
            run_v7_pitr_parallel_crash(&engine, args)?;
            return Err("configured v7 PITR crash boundary was not reached".to_string());
        }
        "wal-only" => scenarios::wal_only_restart(&engine, &mut log, &config, seed)?,
        "flush-boundary" => scenarios::flush_boundary(&engine, &mut log, &config, seed)?,
        "manifest-snapshot" => {
            scenarios::manifest_snapshot_churn(&engine, &mut log, &config, seed)?
        }
        "range-tombstone" => scenarios::range_tombstone_restart(&engine, &mut log, &config, seed)?,
        "vlog" => scenarios::vlog_restart(&engine, &mut log, &config, seed)?,
        "leveled-compaction" => scenarios::compaction_restart(&engine, &mut log, &config, seed)?,
        "tiered-compaction" => scenarios::compaction_restart(&engine, &mut log, &config, seed)?,
        _ => unreachable!(),
    }

    // Keep the child alive until the parent sends SIGKILL after the sync point.
    loop {
        std::thread::park();
    }
}

#[cfg(target_os = "linux")]
fn run_v7_pitr_parallel_crash(engine: &KvEngine, args: &[String]) -> Result<(), String> {
    let repository = get_arg(args, "--pitr-repository")
        .ok_or("missing --pitr-repository for v7 PITR crash scenario")?;
    let mut resume_outcome = engine
        .resume_pitr(&repository)
        .map_err(|error| format!("resume PITR before crash write failed: {error:#}"))?;

    let mut status = engine
        .pitr_status(PitrStatusOptions {
            cursor: None,
            page_size: std::num::NonZeroUsize::new(1).expect("nonzero status page size"),
        })
        .map_err(|error| format!("read Active PITR status failed: {error:#}"))?;
    if status.active_wal_format_version != Some(7) {
        // A clean close can leave an empty legacy Active WAL, which has no
        // source image to migrate. Add one acknowledged legacy batch, then
        // resume once more so the migration path installs the v7 successor.
        engine
            .put(b"acknowledged-migration-trigger", b"legacy")
            .map_err(|error| format!("write legacy migration trigger failed: {error:#}"))?;
        resume_outcome = engine
            .resume_pitr(&repository)
            .map_err(|error| format!("resume PITR to migrate the legacy WAL failed: {error:#}"))?;
        status = engine
            .pitr_status(PitrStatusOptions {
                cursor: None,
                page_size: std::num::NonZeroUsize::new(1).expect("nonzero PITR status page size"),
            })
            .map_err(|error| format!("read migrated PITR status failed: {error:#}"))?;
    }
    if status.active_wal_format_version != Some(7)
        || status.active_wal_io_mode != Some(PitrWalIoMode::Parallel)
    {
        return Err(format!(
            "expected Active v7 Parallel WAL before crash write, got format {:?} and mode {:?}; resume outcome: {resume_outcome:?}; migration capacity: {:?}",
            status.active_wal_format_version,
            status.active_wal_io_mode,
            status.v7_migration_capacity,
        ));
    }

    if args.iter().any(|arg| arg == "--create-empty-successor") {
        match engine.create_recovery_point() {
            Ok(RecoveryPointOutcome::Durable(_)) => return Ok(()),
            Ok(outcome) => {
                return Err(format!(
                    "empty-successor recovery point was not durable: {outcome:?}"
                ));
            }
            Err(error) => {
                return Err(format!("empty-successor recovery point failed: {error:#}"));
            }
        }
    }

    let acknowledged_write_key = get_arg(args, "--acknowledged-write-key");
    let acknowledged_write_value = get_arg(args, "--acknowledged-write-value");
    match (acknowledged_write_key, acknowledged_write_value) {
        (None, None) => {}
        (Some(key), Some(value)) => engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|error| format!("acknowledged v7 write before candidate failed: {error:#}"))?,
        _ => {
            return Err(
                "--acknowledged-write-key and --acknowledged-write-value must be supplied together"
                    .to_string(),
            );
        }
    }

    let write_key = get_arg(args, "--write-key");
    let write_value = get_arg(args, "--write-value");
    let (write_key, write_value) = match (write_key, write_value) {
        (None, None) => (
            "synced-before-publication".to_string(),
            "candidate".to_string(),
        ),
        (Some(key), Some(value)) => (key, value),
        _ => return Err("--write-key and --write-value must be supplied together".to_string()),
    };

    engine
        .put(write_key.as_bytes(), write_value.as_bytes())
        .map_err(|error| format!("post-sync candidate write failed: {error:#}"))
}

fn run_parallel_wal_crash(engine: &KvEngine, args: &[String]) -> Result<(), String> {
    use wrapper::kv_engine_wrapper::chaos::failpoint;

    let crash_point = get_arg(args, "--crash-point").ok_or("missing --crash-point")?;
    engine
        .put(b"acknowledged-before-crash", b"stable")
        .map_err(|error| format!("baseline WAL write failed: {error:#}"))?;

    if crash_point == "parallel_wal.later_group_completed_first" {
        failpoint::enable_deferred_lowest_parallel_wal_group_completion();
        failpoint::reset_parallel_wal_admission_count();

        std::thread::scope(|scope| {
            let first = scope.spawn(|| engine.put(b"parallel-crash-first", b"first"));
            let admission_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while failpoint::parallel_wal_admission_count() == 0 {
                if first.is_finished() {
                    return Err(
                        "first parallel WAL writer finished before admission was observed"
                            .to_string(),
                    );
                }
                if std::time::Instant::now() >= admission_deadline {
                    return Err("first parallel WAL writer was not admitted within 30s".to_string());
                }
                std::thread::yield_now();
            }

            // Admit the second write only after the first owns the lower ticket.
            let second = scope.spawn(|| engine.put(b"parallel-crash-second", b"second"));

            for (name, handle) in [("first", first), ("second", second)] {
                handle
                    .join()
                    .map_err(|_| format!("{name} parallel WAL writer panicked"))?
                    .map_err(|error| format!("{name} parallel WAL writer failed: {error:#}"))?;
            }
            Ok::<(), String>(())
        })?;
    } else {
        engine
            .put(b"unacknowledged-before-crash", b"candidate")
            .map_err(|error| format!("candidate WAL write failed: {error:#}"))?;
    }

    Ok(())
}

fn get_arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find_map(|w| {
        if w[0] == name {
            Some(w[1].clone())
        } else {
            None
        }
    })
}
