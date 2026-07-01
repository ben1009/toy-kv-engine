#![cfg(feature = "chaos-testing")]

mod wrapper;

use wrapper::kv_engine_wrapper::chaos::control_log::ControlLogWriter;
use wrapper::kv_engine_wrapper::chaos::scenarios::{self, ScenarioConfig};
use wrapper::kv_engine_wrapper::chaos::stress::{self, StressScenario};
use wrapper::kv_engine_wrapper::lsm_storage::KvEngine;

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
    let replay = args.iter().any(|a| a == "--replay");
    if replay && get_arg(args, "--cycle").is_none() {
        return Err("--cycle is required in --replay mode".to_string());
    }
    let effective_wal = get_arg(args, "--effective-wal")
        .map(|value| match value.as_str() {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err("invalid --effective-wal"),
        })
        .transpose()?;

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
    let config = match scenario.as_str() {
        "wal-only" => ScenarioConfig::wal_only(),
        "flush-boundary" => ScenarioConfig::flush_boundary(),
        "manifest-snapshot" => ScenarioConfig::manifest_snapshot(),
        "range-tombstone" => ScenarioConfig::range_tombstone(),
        "vlog" => ScenarioConfig::vlog(),
        "leveled-compaction" => ScenarioConfig::leveled_compaction(),
        "tiered-compaction" => ScenarioConfig::tiered_compaction(),
        other => return Err(format!("unknown scenario: {other}")),
    };

    // Open the database
    let engine = KvEngine::open(db_path.as_str(), config.storage_options.clone())
        .map_err(|e| format!("KvEngine::open failed: {e}"))?;

    // Create the control log writer
    let mut log = ControlLogWriter::new(log_path.as_str())
        .map_err(|e| format!("ControlLogWriter::new failed: {e}"))?;

    // Run the scenario
    match scenario.as_str() {
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

fn get_arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find_map(|w| {
        if w[0] == name {
            Some(w[1].clone())
        } else {
            None
        }
    })
}
