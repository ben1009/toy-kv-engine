#![cfg(feature = "chaos-testing")]

mod wrapper;

use wrapper::kv_engine_wrapper::chaos::scenarios::{self, ScenarioConfig};
use wrapper::kv_engine_wrapper::chaos::control_log::ControlLogWriter;
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
    let db_path = get_arg(args, "--db-path").ok_or("missing --db-path")?;
    let log_path = get_arg(args, "--control-log-path").ok_or("missing --control-log-path")?;

    // Look up scenario config
    let config = match scenario.as_str() {
        "wal-only" => ScenarioConfig::wal_only(),
        "flush-boundary" => ScenarioConfig::flush_boundary(),
        "manifest-snapshot" => ScenarioConfig::manifest_snapshot(),
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
        "manifest-snapshot" => scenarios::manifest_snapshot_churn(&engine, &mut log, &config, seed)?,
        _ => unreachable!(),
    }

    // Close cleanly
    engine.close().map_err(|e| format!("close failed: {e}"))?;
    eprintln!("chaos-child: scenario '{scenario}' completed successfully (seed={seed})");
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
