//! Seeded stress harness for Phase 4 chaos testing.
//!
//! This module turns the roadmap into a reusable workload generator:
//! a stable per-seed storage config, a bounded key universe, and a
//! deterministic per-cycle operation plan.

use std::fs;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};

use crate::{
    chaos::control_log::{BatchEntry, BatchOpKind, ControlLogWriter, OperationKind},
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    lsm_storage::{KvEngine, LsmStorageOptions},
    vlog::ValueSeparationOptions,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StressPhase {
    Stress,
    Verify,
}

#[derive(Debug, Clone)]
pub struct StressScenario {
    pub seed: u64,
    pub key_prefix: String,
    pub requested_enable_wal: bool,
    pub key_space: usize,
    pub ops_per_cycle: usize,
    pub flush_stride: usize,
    pub compact_every_flushes: usize,
    pub storage_options: LsmStorageOptions,
    pub allow_delete_range: bool,
    pub min_value_size: usize,
    pub max_value_size: usize,
}

#[derive(Debug, Clone)]
pub struct StressCyclePlan {
    pub phase: StressPhase,
    pub operations: Vec<StressOp>,
}

#[derive(Debug, Clone, Copy)]
pub struct StressRunLimits {
    pub max_cycles: Option<u64>,
    pub max_duration: Option<Duration>,
    pub progress_every: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StressRunProgress {
    pub cycles_completed: u64,
    pub last_cycle: u64,
}

#[derive(Debug, Clone)]
pub enum StressOp {
    Put {
        key_index: usize,
        value: String,
    },
    Delete {
        key_index: usize,
    },
    DeleteRange {
        start_index: usize,
        end_index: usize,
    },
    WriteBatch {
        entries: Vec<BatchEntry>,
    },
    Flush,
    Compact,
}

impl StressScenario {
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed ^ 0x9e3779b97f4a7c15);

        let key_space = rng.gen_range(48..=96);
        let ops_per_cycle = rng.gen_range(18..=32);
        let flush_stride = rng.gen_range(3..=8);
        let compact_every_flushes = rng.gen_range(1..=3);
        let requested_enable_wal =
            StdRng::seed_from_u64(seed ^ 0x4d_57_41_4c_00_00_00_01).gen_bool(0.5);
        let target_sst_size = [4 << 10, 64 << 10, 256 << 10, 2 << 20]
            .choose(&mut rng)
            .copied()
            .unwrap_or(64 << 10);
        let manifest_snapshot_threshold_bytes = [0, 256, 1024, 65536]
            .choose(&mut rng)
            .copied()
            .unwrap_or(1024);
        let storage_options = build_storage_options(
            &mut rng,
            requested_enable_wal,
            target_sst_size,
            manifest_snapshot_threshold_bytes,
        );
        let allow_delete_range = !storage_options.serializable
            && storage_options
                .value_separation
                .as_ref()
                .is_none_or(|opts| !opts.enabled);
        let (min_value_size, max_value_size) = storage_options
            .value_separation
            .as_ref()
            .map(|opts| (opts.min_value_size, opts.max_value_size))
            .unwrap_or((32, 1024));

        Self {
            seed,
            key_prefix: format!("stress_{seed:016x}"),
            requested_enable_wal,
            key_space,
            ops_per_cycle,
            flush_stride,
            compact_every_flushes,
            storage_options,
            allow_delete_range,
            min_value_size,
            max_value_size,
        }
    }

    pub fn key(&self, index: usize) -> String {
        format!("{}_{index:010}", self.key_prefix)
    }

    pub fn describe(&self) -> String {
        let compaction = match &self.storage_options.compaction_options {
            CompactionOptions::NoCompaction => "NoCompaction".to_string(),
            CompactionOptions::Simple(_) => "Simple".to_string(),
            CompactionOptions::Leveled(_) => "Leveled".to_string(),
            CompactionOptions::Tiered(_) => "Tiered".to_string(),
        };
        let vlog = self
            .storage_options
            .value_separation
            .as_ref()
            .map(|opts| {
                format!(
                    "on(min_value_size={}, max_value_size={})",
                    opts.min_value_size, opts.max_value_size
                )
            })
            .unwrap_or_else(|| "off".to_string());
        format!(
            "seed={} key_space={} ops_per_cycle={} flush_stride={} compact_every_flushes={} compaction={} requested_enable_wal={} enable_wal={} serializable={} vlog={} target_sst_size={} manifest_snapshot_threshold_bytes={}",
            self.seed,
            self.key_space,
            self.ops_per_cycle,
            self.flush_stride,
            self.compact_every_flushes,
            compaction,
            self.requested_enable_wal,
            self.storage_options.enable_wal,
            self.storage_options.serializable,
            vlog,
            self.storage_options.target_sst_size,
            self.storage_options.manifest_snapshot_threshold_bytes,
        )
    }
}

pub fn plan_cycle(seed: u64, cycle: u64) -> (StressScenario, StressCyclePlan) {
    let scenario = StressScenario::from_seed(seed);
    let mut rng = StdRng::seed_from_u64(seed ^ cycle.rotate_left(17) ^ 0xa5a5_a5a5_a5a5_a5a5);
    let phase = if cycle.is_multiple_of(2) {
        StressPhase::Stress
    } else {
        StressPhase::Verify
    };
    let data_ops = scenario.ops_per_cycle.saturating_sub(2);
    let mut operations = Vec::with_capacity(data_ops + 4);
    let mut ops_since_flush = 0usize;
    let mut flushes_since_compact = 0usize;
    for _ in 0..data_ops {
        operations.push(random_data_op(&scenario, phase, &mut rng));
        ops_since_flush += 1;
        if ops_since_flush >= scenario.flush_stride {
            operations.push(StressOp::Flush);
            ops_since_flush = 0;
            flushes_since_compact += 1;
            if flushes_since_compact >= scenario.compact_every_flushes {
                operations.push(StressOp::Compact);
                flushes_since_compact = 0;
            }
        }
    }
    if ops_since_flush > 0 || operations.is_empty() {
        operations.push(StressOp::Flush);
        flushes_since_compact += 1;
    }
    if matches!(phase, StressPhase::Verify) && flushes_since_compact > 0 {
        operations.push(StressOp::Flush);
    }
    operations.push(StressOp::Compact);
    (scenario, StressCyclePlan { phase, operations })
}

pub fn run_cycle(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    seed: u64,
    cycle: u64,
) -> Result<StressCyclePlan, String> {
    let plan = execute_cycle_plan(engine, log, seed, cycle)?;
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;
    Ok(plan)
}

fn execute_cycle_plan(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    seed: u64,
    cycle: u64,
) -> Result<StressCyclePlan, String> {
    let (scenario, plan) = plan_cycle(seed, cycle);
    let mut op_id = log.next_op_id();
    for op in &plan.operations {
        match op {
            StressOp::Put { key_index, value } => {
                let key = scenario.key(*key_index);
                write_put(
                    engine,
                    log,
                    &mut op_id,
                    &key,
                    value,
                    scenario.storage_options.serializable,
                )?;
            }
            StressOp::Delete { key_index } => {
                let key = scenario.key(*key_index);
                write_delete(
                    engine,
                    log,
                    &mut op_id,
                    &key,
                    scenario.storage_options.serializable,
                )?;
            }
            StressOp::DeleteRange {
                start_index,
                end_index,
            } => {
                let start = scenario.key(*start_index);
                let end = scenario.key(*end_index);
                write_delete_range(engine, log, &mut op_id, &start, &end)?;
            }
            StressOp::WriteBatch { entries } => {
                write_batch(engine, log, &mut op_id, entries)?;
            }
            StressOp::Flush => {
                write_flush(engine, log, &mut op_id)?;
            }
            StressOp::Compact => {
                write_full_compaction(engine, log, &mut op_id)?;
            }
        }
    }
    Ok(plan)
}

pub fn run_loop(
    db_path: &std::path::Path,
    log_path: &std::path::Path,
    seed: u64,
    start_cycle: u64,
    limits: StressRunLimits,
) -> Result<StressRunProgress, String> {
    let scenario = StressScenario::from_seed(seed);
    let deadline = limits
        .max_duration
        .map(|duration| Instant::now() + duration);
    let mut log = ControlLogWriter::new(log_path.to_path_buf())
        .map_err(|e| format!("ControlLogWriter::new failed: {e}"))?;
    let mut cycles_completed = 0u64;
    let mut last_cycle = start_cycle.saturating_sub(1);

    loop {
        if let Some(max_cycles) = limits.max_cycles
            && cycles_completed >= max_cycles
        {
            break;
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }

        let cycle = start_cycle.saturating_add(cycles_completed);
        let (engine, _wal_enabled) = open_stress_engine(db_path, &scenario.storage_options, None)?;
        let plan = execute_cycle_plan(&engine, &mut log, seed, cycle)?;
        engine.close().map_err(|e| format!("close failed: {e}"))?;
        log.write_sync_point()
            .map_err(|e| format!("write_sync_point failed: {e}"))?;
        cycles_completed = cycles_completed.saturating_add(1);
        last_cycle = cycle;

        if limits.progress_every != 0 && cycles_completed.is_multiple_of(limits.progress_every) {
            eprintln!(
                "chaos-stress progress: cycles_completed={} last_cycle={} phase={:?} ops={}",
                cycles_completed,
                last_cycle,
                plan.phase,
                plan.operations.len()
            );
        }
    }

    Ok(StressRunProgress {
        cycles_completed,
        last_cycle,
    })
}

fn random_data_op(scenario: &StressScenario, phase: StressPhase, rng: &mut StdRng) -> StressOp {
    let allow_delete_range = scenario.allow_delete_range && matches!(phase, StressPhase::Stress);
    let roll = rng.gen_range(0..100);
    if roll < 55 {
        let key_index = rng.gen_range(0..scenario.key_space);
        let value_len = choose_value_len(scenario, rng);
        let value = make_value(rng, value_len);
        StressOp::Put { key_index, value }
    } else if roll < 80 {
        let key_index = rng.gen_range(0..scenario.key_space);
        StressOp::Delete { key_index }
    } else if allow_delete_range && roll < 92 {
        let start_index = rng.gen_range(0..scenario.key_space.saturating_sub(1));
        let max_width = (scenario.key_space - start_index).clamp(1, 8);
        let end_index = start_index + rng.gen_range(1..=max_width);
        StressOp::DeleteRange {
            start_index,
            end_index,
        }
    } else {
        let key_index = rng.gen_range(0..scenario.key_space);
        let value_len = choose_batch_value_len(scenario, rng);
        let batch_len = rng.gen_range(2..=3);
        let mut entries = Vec::with_capacity(batch_len);
        for offset in 0..batch_len {
            let entry_key = scenario.key((key_index + offset) % scenario.key_space);
            let kind = if offset % 2 == 0 {
                BatchOpKind::Put
            } else {
                BatchOpKind::Delete
            };
            let value = if matches!(kind, BatchOpKind::Put) {
                Some(make_value(rng, value_len))
            } else {
                None
            };
            entries.push(BatchEntry {
                kind,
                key: entry_key,
                value,
            });
        }
        StressOp::WriteBatch { entries }
    }
}

fn choose_value_len(scenario: &StressScenario, rng: &mut StdRng) -> usize {
    if scenario.storage_options.value_separation.is_some() {
        [
            64usize,
            scenario.min_value_size.saturating_div(2).max(32),
            scenario.min_value_size,
            scenario
                .max_value_size
                .min(scenario.min_value_size.saturating_mul(2)),
        ]
        .choose(rng)
        .copied()
        .unwrap_or(scenario.min_value_size)
    } else {
        [24usize, 64, 256, 1024].choose(rng).copied().unwrap_or(64)
    }
}

fn choose_batch_value_len(scenario: &StressScenario, rng: &mut StdRng) -> usize {
    if scenario.storage_options.value_separation.is_some() {
        [
            scenario.min_value_size,
            scenario.min_value_size.saturating_add(32),
            2 * scenario.min_value_size,
        ]
        .choose(rng)
        .copied()
        .unwrap_or(scenario.min_value_size)
    } else {
        [16usize, 32, 48, 64].choose(rng).copied().unwrap_or(32)
    }
}

fn make_value(rng: &mut StdRng, len: usize) -> String {
    let fill = ['a', 'b', 'c', 'd', 'e', 'f']
        .choose(rng)
        .copied()
        .unwrap_or('x');
    std::iter::repeat_n(fill, len).collect()
}

fn build_storage_options(
    rng: &mut StdRng,
    requested_enable_wal: bool,
    target_sst_size: usize,
    manifest_snapshot_threshold_bytes: u64,
) -> LsmStorageOptions {
    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = requested_enable_wal;
    opts.target_sst_size = target_sst_size;
    opts.manifest_snapshot_threshold_bytes = manifest_snapshot_threshold_bytes;
    opts.serializable = rng.gen_bool(0.5);
    opts.compaction_options = match rng.gen_range(0..4) {
        0 => CompactionOptions::NoCompaction,
        1 => CompactionOptions::Simple(SimpleLeveledCompactionOptions {
            size_ratio_percent: 10,
            level0_file_num_compaction_trigger: 2,
            max_levels: 3,
        }),
        2 => CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 10,
            level0_file_num_compaction_trigger: 2,
            max_levels: 3,
            base_level_size_mb: 1,
        }),
        _ => CompactionOptions::Tiered(TieredCompactionOptions {
            num_tiers: 3,
            max_size_amplification_percent: 200,
            size_ratio: 10,
            min_merge_width: 2,
            max_merge_width: None,
        }),
    };
    if rng.gen_bool(0.5) {
        let min_value_size = [128usize, 512, 1024, 4096]
            .choose(rng)
            .copied()
            .unwrap_or(512);
        opts.value_separation = Some(ValueSeparationOptions {
            enabled: true,
            min_value_size,
            max_value_size: 128 << 20,
            max_vlog_file_size: 64 << 20,
            gc_threshold_ratio: 0.5,
            max_open_vlog_files: 64,
            value_cache_capacity_bytes: 0,
        });
    }
    opts
}

fn wal_supported() -> bool {
    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        let probe_dir = std::env::temp_dir().join(format!(
            "kv-engine-chaos-wal-probe-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        if let Err(err) = fs::create_dir_all(&probe_dir) {
            eprintln!(
                "chaos-stress: WAL probe could not create {}: {err}",
                probe_dir.display()
            );
            return false;
        }
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        let supported = match KvEngine::open(&probe_dir, opts) {
            Ok(engine) => {
                let _ = engine.close();
                true
            }
            Err(err) => {
                eprintln!("chaos-stress: WAL probe failed: {err}");
                false
            }
        };
        let _ = fs::remove_dir_all(&probe_dir);
        supported
    })
}

pub fn open_stress_engine(
    db_path: &std::path::Path,
    options: &LsmStorageOptions,
    force_wal: Option<bool>,
) -> Result<(Arc<KvEngine>, bool), String> {
    let mut effective_options = options.clone();
    match force_wal {
        Some(enable_wal) => effective_options.enable_wal = enable_wal,
        None if effective_options.enable_wal && !wal_supported() => {
            effective_options.enable_wal = false
        }
        None => {}
    }
    KvEngine::open(db_path, effective_options.clone())
        .map(|engine| (engine, effective_options.enable_wal))
        .map_err(|err| format!("KvEngine::open failed: {err}"))
}

fn write_put(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    op_id: &mut u64,
    key: &str,
    value: &str,
    serializable: bool,
) -> Result<(), String> {
    log.write_intent(
        *op_id,
        OperationKind::Put {
            key: key.to_string(),
            value: value.to_string(),
        },
    )
    .map_err(|e| format!("write_intent failed: {e}"))?;
    if serializable {
        run_txn_put(engine, key.as_bytes(), value.as_bytes())?;
    } else {
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
    }
    log.write_durability_boundary(
        *op_id,
        OperationKind::Put {
            key: key.to_string(),
            value: value.to_string(),
        },
    )
    .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    *op_id = op_id.saturating_add(1);
    Ok(())
}

fn write_delete(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    op_id: &mut u64,
    key: &str,
    serializable: bool,
) -> Result<(), String> {
    log.write_intent(
        *op_id,
        OperationKind::Delete {
            key: key.to_string(),
        },
    )
    .map_err(|e| format!("write_intent failed: {e}"))?;
    if serializable {
        run_txn_delete(engine, key.as_bytes())?;
    } else {
        engine
            .delete(key.as_bytes())
            .map_err(|e| format!("delete failed: {e}"))?;
    }
    log.write_durability_boundary(
        *op_id,
        OperationKind::Delete {
            key: key.to_string(),
        },
    )
    .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    *op_id = op_id.saturating_add(1);
    Ok(())
}

fn write_delete_range(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    op_id: &mut u64,
    start: &str,
    end: &str,
) -> Result<(), String> {
    log.write_intent(
        *op_id,
        OperationKind::DeleteRange {
            start: start.to_string(),
            end: end.to_string(),
        },
    )
    .map_err(|e| format!("write_intent failed: {e}"))?;
    engine
        .delete_range(start.as_bytes(), end.as_bytes())
        .map_err(|e| format!("delete_range failed: {e}"))?;
    log.write_durability_boundary(
        *op_id,
        OperationKind::DeleteRange {
            start: start.to_string(),
            end: end.to_string(),
        },
    )
    .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    *op_id = op_id.saturating_add(1);
    Ok(())
}

fn write_batch(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    op_id: &mut u64,
    entries: &[BatchEntry],
) -> Result<(), String> {
    log.write_intent(
        *op_id,
        OperationKind::WriteBatch {
            entries: entries.to_vec(),
        },
    )
    .map_err(|e| format!("write_intent failed: {e}"))?;
    let mut batch = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry.kind {
            BatchOpKind::Put => {
                let value = entry.value.as_deref().unwrap_or_default();
                batch.push((entry.key.as_bytes(), value.as_bytes(), false));
            }
            BatchOpKind::Delete => {
                batch.push((entry.key.as_bytes(), b"", true));
            }
        }
    }
    if engine.inner.options.serializable {
        run_txn_batch(engine, &batch)?;
    } else {
        for (key, value, is_delete) in batch {
            if is_delete {
                engine
                    .delete(key)
                    .map_err(|e| format!("delete in batch failed: {e}"))?;
            } else {
                engine
                    .put(key, value)
                    .map_err(|e| format!("put in batch failed: {e}"))?;
            }
        }
    }
    log.write_durability_boundary(
        *op_id,
        OperationKind::WriteBatch {
            entries: entries.to_vec(),
        },
    )
    .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    *op_id = op_id.saturating_add(1);
    Ok(())
}

fn write_flush(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    op_id: &mut u64,
) -> Result<(), String> {
    log.write_intent(*op_id, OperationKind::Flush)
        .map_err(|e| format!("write_intent failed: {e}"))?;
    engine
        .force_flush()
        .map_err(|e| format!("force_flush failed: {e}"))?;
    log.write_durability_boundary(*op_id, OperationKind::Flush)
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    *op_id = op_id.saturating_add(1);
    Ok(())
}

fn write_full_compaction(
    engine: &Arc<KvEngine>,
    log: &mut ControlLogWriter,
    op_id: &mut u64,
) -> Result<(), String> {
    log.write_intent(*op_id, OperationKind::FullCompaction)
        .map_err(|e| format!("write_intent failed: {e}"))?;
    engine
        .force_full_compaction()
        .map_err(|e| format!("force_full_compaction failed: {e}"))?;
    log.write_durability_boundary(*op_id, OperationKind::FullCompaction)
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    *op_id = op_id.saturating_add(1);
    Ok(())
}

fn run_txn_put(engine: &Arc<KvEngine>, key: &[u8], value: &[u8]) -> Result<(), String> {
    for _attempt in 0..3 {
        let txn = engine
            .new_txn()
            .map_err(|e| format!("new_txn failed: {e}"))?;
        txn.put(key, value)
            .map_err(|e| format!("txn.put failed: {e}"))?;
        match txn.commit() {
            Ok(_) => return Ok(()),
            Err(e) if e.to_string().contains("serializable conflict") => continue,
            Err(e) if e.to_string().contains("transaction already committed") => continue,
            Err(e) => return Err(format!("txn.commit failed: {e}")),
        }
    }
    Err("txn.commit retried too many times".to_string())
}

fn run_txn_delete(engine: &Arc<KvEngine>, key: &[u8]) -> Result<(), String> {
    for _attempt in 0..3 {
        let txn = engine
            .new_txn()
            .map_err(|e| format!("new_txn failed: {e}"))?;
        txn.delete(key)
            .map_err(|e| format!("txn.delete failed: {e}"))?;
        match txn.commit() {
            Ok(_) => return Ok(()),
            Err(e) if e.to_string().contains("serializable conflict") => continue,
            Err(e) if e.to_string().contains("transaction already committed") => continue,
            Err(e) => return Err(format!("txn.commit failed: {e}")),
        }
    }
    Err("txn.commit retried too many times".to_string())
}

fn run_txn_batch(engine: &Arc<KvEngine>, batch: &[(&[u8], &[u8], bool)]) -> Result<(), String> {
    for _attempt in 0..3 {
        let txn = engine
            .new_txn()
            .map_err(|e| format!("new_txn failed: {e}"))?;
        for (key, value, is_delete) in batch {
            if *is_delete {
                txn.delete(key)
                    .map_err(|e| format!("txn.delete in batch failed: {e}"))?;
            } else {
                txn.put(key, value)
                    .map_err(|e| format!("txn.put in batch failed: {e}"))?;
            }
        }
        match txn.commit() {
            Ok(_) => return Ok(()),
            Err(e) if e.to_string().contains("serializable conflict") => continue,
            Err(e) if e.to_string().contains("transaction already committed") => continue,
            Err(e) => return Err(format!("txn.commit batch failed: {e}")),
        }
    }
    Err("txn.commit batch retried too many times".to_string())
}

pub fn cycle_report(seed: u64, cycle: u64) -> String {
    let (scenario, plan) = plan_cycle(seed, cycle);
    format!(
        "phase={:?} scenario=({}) op_count={}",
        plan.phase,
        scenario.describe(),
        plan.operations.len()
    )
}

pub fn summarize_cycle_failure(
    seed: u64,
    cycle: u64,
    log_path: &std::path::Path,
    violations: &[crate::chaos::oracle::Violation],
) -> String {
    let (scenario, plan) = plan_cycle(seed, cycle);
    let mut message = format!(
        "chaos stress cycle failed: cycle={} phase={:?} scenario=({}) ops={} log_path={}",
        cycle,
        plan.phase,
        scenario.describe(),
        plan.operations.len(),
        log_path.display()
    );
    if !violations.is_empty() {
        message.push_str(" violations=");
        for violation in violations {
            message.push_str(&format!("{:?}; ", violation.kind));
        }
    }
    message
}

pub fn replay_command(
    child_bin: &std::path::Path,
    seed: u64,
    cycle: u64,
    db_path: &std::path::Path,
    log_path: &std::path::Path,
    wal_enabled: bool,
) -> String {
    format!(
        "{} --child --scenario stress --replay --effective-wal {} --seed {} --cycle {} --db-path {} --control-log-path {}",
        child_bin.display(),
        if wal_enabled { "on" } else { "off" },
        seed,
        cycle,
        db_path.display(),
        log_path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_cycle_is_deterministic() {
        let (scenario_a, plan_a) = plan_cycle(42, 0);
        let (scenario_b, plan_b) = plan_cycle(42, 0);
        assert_eq!(scenario_a.key_prefix, scenario_b.key_prefix);
        assert_eq!(scenario_a.key_space, scenario_b.key_space);
        assert_eq!(scenario_a.ops_per_cycle, scenario_b.ops_per_cycle);
        assert_eq!(
            scenario_a.storage_options.enable_wal,
            scenario_b.storage_options.enable_wal
        );
        assert_eq!(plan_a.operations.len(), plan_b.operations.len());
    }

    #[test]
    fn phase_alternates_by_cycle() {
        let (_, plan_0) = plan_cycle(42, 0);
        let (_, plan_1) = plan_cycle(42, 1);
        assert!(matches!(plan_0.phase, StressPhase::Stress));
        assert!(matches!(plan_1.phase, StressPhase::Verify));
    }
}
