mod wrapper;

use parking_lot::Mutex;
use std::fmt::Write as _;
use std::io::Write as _;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wrapper::kv_engine_wrapper;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use kv_engine_wrapper::{
    block_on,
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    iterators::StorageIterator,
    lsm_storage::{
        CacheAdmission, KvEngine, LsmStorageOptions, ParallelScanOptions, PrefixBloomOptions,
    },
    vlog::ValueSeparationOptions,
};
use rand::prelude::*;
use rand::rngs::StdRng;
use serde::Serialize;

const JSON_SCHEMA: &str = "kv-engine.write-perf.v1";
const ENGINE_NAME: &str = "kv-engine";

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    Text,
    Json,
    Both,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Preset {
    Default,
    Large,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum CompactionMode {
    None,
    Simple,
    Leveled,
    Tiered,
}

#[derive(Parser, Debug)]
#[command(name = "write-perf")]
struct Args {
    #[arg(long)]
    bench: Option<String>,
    #[arg(long, value_enum, default_value_t = Preset::Default)]
    preset: Preset,
    #[arg(long)]
    large: bool,
    #[arg(long, default_value = "/tmp/write-perf")]
    path: PathBuf,
    #[arg(long)]
    no_cleanup: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,
    #[arg(long)]
    num: Option<usize>,
    #[arg(long)]
    reads: Option<usize>,
    #[arg(long)]
    duration: Option<u64>,
    #[arg(long)]
    scan_num: Option<usize>,
    #[arg(long)]
    value_size: Option<usize>,
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long)]
    readers: Option<usize>,
    #[arg(long)]
    seeks: Option<usize>,
    #[arg(long)]
    seek_nexts: Option<usize>,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long)]
    cache_capacity: Option<u64>,
    #[arg(long)]
    target_sst_size: Option<usize>,
    #[arg(long)]
    memtable_limit: Option<usize>,
    #[arg(long)]
    parallel_scan_max_parallelism: Option<usize>,
    #[arg(long)]
    parallel_scan_batch_rows: Option<usize>,
    #[arg(long)]
    parallel_scan_batch_bytes: Option<usize>,
    #[arg(long)]
    parallel_scan_yield_every_rows: Option<usize>,
    #[arg(long)]
    parallel_scan_channel_capacity: Option<usize>,
    /// Block cache admission policy for parallel scan: force, admit, bypass.
    #[arg(long)]
    parallel_scan_cache_admission: Option<String>,
    #[arg(long, value_enum, default_value_t = CompactionMode::Leveled)]
    compaction: CompactionMode,
    #[arg(long)]
    wal: bool,
    #[arg(long)]
    vlog: bool,
    #[arg(long)]
    profile: bool,
}

#[derive(Clone, Debug)]
struct HarnessConfig {
    preset_name: &'static str,
    run_id: String,
    base_path: PathBuf,
    cleanup: bool,
    output: OutputFormat,
    num: usize,
    reads: usize,
    duration_secs: u64,
    scan_num: usize,
    value_size: usize,
    threads: usize,
    readers: usize,
    seeks: usize,
    seek_nexts: usize,
    seed: u64,
    cache_capacity: u64,
    target_sst_size: usize,
    memtable_limit: usize,
    parallel_scan_max_parallelism: usize,
    parallel_scan_batch_rows: usize,
    parallel_scan_batch_bytes: usize,
    parallel_scan_yield_every_rows: usize,
    parallel_scan_channel_capacity: usize,
    parallel_scan_cache_admission: String,
    compaction: CompactionMode,
    wal_override: bool,
    vlog_override: bool,
    profile: bool,
    num_overridden: bool,
    value_size_overridden: bool,
}

impl HarnessConfig {
    fn from_args(args: Args) -> Self {
        let preset = if args.large {
            Preset::Large
        } else {
            args.preset
        };
        let (preset_name, num, reads, duration_secs, scan_num) = match preset {
            Preset::Default => ("default", 200_000, 100_000, 5, 100_000),
            Preset::Large => ("large", 2_000_000, 500_000, 10, 1_000_000),
        };
        let run_ms = unix_epoch_ms();

        Self {
            preset_name,
            run_id: format!("{run_ms}"),
            base_path: args.path,
            cleanup: !args.no_cleanup,
            output: args.output,
            num: args.num.unwrap_or(num),
            reads: args.reads.unwrap_or(reads),
            duration_secs: args.duration.unwrap_or(duration_secs),
            scan_num: args.scan_num.unwrap_or(scan_num),
            value_size: args.value_size.unwrap_or(1024),
            threads: args.threads.unwrap_or(4),
            readers: args.readers.unwrap_or(4),
            seeks: args.seeks.unwrap_or(10_000),
            seek_nexts: args.seek_nexts.unwrap_or(10),
            seed: args.seed,
            cache_capacity: args.cache_capacity.unwrap_or(8192),
            target_sst_size: args.target_sst_size.unwrap_or(1 << 20),
            memtable_limit: args.memtable_limit.unwrap_or(2),
            parallel_scan_max_parallelism: args.parallel_scan_max_parallelism.unwrap_or_else(
                || {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(1)
                        .min(8)
                },
            ),
            parallel_scan_batch_rows: args.parallel_scan_batch_rows.unwrap_or(128),
            parallel_scan_batch_bytes: args.parallel_scan_batch_bytes.unwrap_or(256 * 1024),
            parallel_scan_yield_every_rows: args.parallel_scan_yield_every_rows.unwrap_or(1024),
            parallel_scan_channel_capacity: args.parallel_scan_channel_capacity.unwrap_or(4),
            parallel_scan_cache_admission: args
                .parallel_scan_cache_admission
                .unwrap_or_else(|| "bypass".to_string()),
            compaction: args.compaction,
            wal_override: args.wal,
            vlog_override: args.vlog,
            profile: args.profile,
            num_overridden: args.num.is_some(),
            value_size_overridden: args.value_size.is_some(),
        }
    }

    fn path_for(&self, workload: &str) -> PathBuf {
        self.base_path.join(workload)
    }

    fn build_options(&self, wal: bool, vlog: bool) -> LsmStorageOptions {
        let enable_wal = wal || self.wal_override;
        let enable_vlog = vlog || self.vlog_override;
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: self.target_sst_size,
            num_memtable_limit: self.memtable_limit,
            compaction_options: match self.compaction {
                CompactionMode::None => CompactionOptions::NoCompaction,
                CompactionMode::Simple => {
                    CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                        size_ratio_percent: 200,
                        level0_file_num_compaction_trigger: 4,
                        max_levels: 6,
                    })
                }
                CompactionMode::Leveled => CompactionOptions::Leveled(LeveledCompactionOptions {
                    level_size_multiplier: 2,
                    level0_file_num_compaction_trigger: 4,
                    max_levels: 6,
                    base_level_size_mb: 128,
                }),
                CompactionMode::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
                    num_tiers: 3,
                    max_size_amplification_percent: 200,
                    size_ratio: 2,
                    min_merge_width: 2,
                    max_merge_width: None,
                }),
            },
            enable_wal,
            serializable: false,
            value_separation: if enable_vlog {
                Some(ValueSeparationOptions {
                    enabled: true,
                    min_value_size: 1024,
                    max_value_size: 64 * 1024,
                    max_vlog_file_size: 64 * 1024 * 1024,
                    gc_threshold_ratio: 0.4,
                    max_open_vlog_files: 4,
                    value_cache_capacity_bytes: 16 * 1024 * 1024,
                })
            } else {
                None
            },
            manifest_snapshot_threshold_bytes: 4 << 20,
            block_cache_capacity: self.cache_capacity,
            enable_cache_backfill: true,
            prefix_bloom: PrefixBloomOptions::default(),
        }
    }

    fn parallel_scan_options(&self) -> ParallelScanOptions {
        let cache_admission = match self.parallel_scan_cache_admission.as_str() {
            "force" => CacheAdmission::Force,
            "admit" => CacheAdmission::Admit,
            "bypass" => CacheAdmission::Bypass,
            other => panic!(
                "invalid --parallel-scan-cache-admission value: {other:?} \
                 (expected force, admit, or bypass)"
            ),
        };
        #[allow(clippy::needless_update)]
        ParallelScanOptions {
            max_parallelism: self.parallel_scan_max_parallelism,
            batch_rows: self.parallel_scan_batch_rows,
            batch_bytes: self.parallel_scan_batch_bytes,
            yield_every_rows: self.parallel_scan_yield_every_rows,
            channel_capacity: self.parallel_scan_channel_capacity,
            cache_admission,
            ..Default::default()
        }
    }
}

type WorkloadFn = fn(&HarnessConfig) -> Result<Vec<BenchMeasurement>>;

#[derive(Debug)]
struct WorkloadSpec {
    name: &'static str,
    aliases: &'static [&'static str],
    run: WorkloadFn,
}

const WORKLOADS: &[WorkloadSpec] = &[
    WorkloadSpec {
        name: "scan",
        aliases: &[],
        run: run_scan,
    },
    WorkloadSpec {
        name: "parallel_scan",
        aliases: &["pscan"],
        run: run_parallel_scan,
    },
    WorkloadSpec {
        name: "concurrent_rw_no_wal",
        aliases: &["rw_no_wal"],
        run: run_concurrent_rw_no_wal,
    },
    WorkloadSpec {
        name: "concurrent_rw_wal",
        aliases: &["rw_wal"],
        run: run_concurrent_rw_wal,
    },
    WorkloadSpec {
        name: "wal_throughput",
        aliases: &["wal"],
        run: run_wal_throughput,
    },
    WorkloadSpec {
        name: "wal_concurrent",
        aliases: &[],
        run: run_wal_concurrent,
    },
    WorkloadSpec {
        name: "vlog_gc",
        aliases: &[],
        run: run_vlog_gc,
    },
    WorkloadSpec {
        name: "vlog_concurrent_gc",
        aliases: &[],
        run: run_vlog_concurrent_gc,
    },
    WorkloadSpec {
        name: "fillseq",
        aliases: &[],
        run: run_fillseq,
    },
    WorkloadSpec {
        name: "fillrandom",
        aliases: &[],
        run: run_fillrandom,
    },
    WorkloadSpec {
        name: "readrandom",
        aliases: &[],
        run: run_readrandom,
    },
    WorkloadSpec {
        name: "readwhilewriting",
        aliases: &["rww"],
        run: run_readwhilewriting,
    },
    WorkloadSpec {
        name: "readrandomwriterandom",
        aliases: &["rwrw"],
        run: run_readrandomwriterandom,
    },
    WorkloadSpec {
        name: "seekrandom",
        aliases: &[],
        run: run_seekrandom,
    },
    WorkloadSpec {
        name: "overwrite",
        aliases: &[],
        run: run_overwrite,
    },
    WorkloadSpec {
        name: "readseq",
        aliases: &[],
        run: run_readseq,
    },
    WorkloadSpec {
        name: "readseq_validate_order",
        aliases: &["readreverse"],
        run: run_readseq_validate_order,
    },
    WorkloadSpec {
        name: "readmissing",
        aliases: &[],
        run: run_readmissing,
    },
    WorkloadSpec {
        name: "seekrandomwhilewriting",
        aliases: &["seekww"],
        run: run_seekrandomwhilewriting,
    },
    WorkloadSpec {
        name: "deleterandom",
        aliases: &[],
        run: run_deleterandom,
    },
    WorkloadSpec {
        name: "compact",
        aliases: &[],
        run: run_compact,
    },
];

#[derive(Clone)]
struct BenchMeasurement {
    record: MeasurementRecord,
    summary: String,
}

#[derive(Clone, Serialize)]
struct MeasurementRecord {
    schema: &'static str,
    unix_epoch_ms: u64,
    run_id: String,
    workload: String,
    measurement: String,
    preset: &'static str,
    engine: &'static str,
    engine_options: EngineOptionsRecord,
    params: MeasurementParams,
    result: MeasurementResult,
    counters: MeasurementCounters,
}

#[derive(Clone, Serialize)]
struct EngineOptionsRecord {
    wal: bool,
    value_separation: bool,
    compaction: &'static str,
    target_sst_size: usize,
    memtable_limit: usize,
    cache_capacity: u64,
}

#[derive(Clone, Default, Serialize)]
struct MeasurementParams {
    num: Option<usize>,
    reads: Option<usize>,
    value_size: Option<usize>,
    duration_secs: Option<u64>,
    threads: Option<usize>,
    readers: Option<usize>,
    seeks: Option<usize>,
    seek_nexts: Option<usize>,
    seed: Option<u64>,
    parallel_scan_max_parallelism: Option<usize>,
    parallel_scan_batch_rows: Option<usize>,
    parallel_scan_batch_bytes: Option<usize>,
    parallel_scan_yield_every_rows: Option<usize>,
    parallel_scan_channel_capacity: Option<usize>,
    parallel_scan_cache_admission: Option<String>,
}

#[derive(Clone, Default, Serialize)]
struct MeasurementResult {
    load_elapsed_ms: Option<f64>,
    measure_elapsed_ms: f64,
    write_elapsed_ms: Option<f64>,
    gc_elapsed_ms: Option<f64>,
    ops: Option<u64>,
    ops_per_sec: Option<f64>,
    entries: Option<u64>,
    entries_per_sec: Option<f64>,
    reads: Option<u64>,
    reads_per_sec: Option<f64>,
    writes: Option<u64>,
    writes_per_sec: Option<f64>,
    found: Option<u64>,
    total_nexts: Option<u64>,
    gc_rounds: Option<u64>,
    parallel_scan_planned_shards: Option<usize>,
    parallel_scan_max_shard_rows: Option<u64>,
    parallel_scan_min_shard_rows: Option<u64>,
    parallel_scan_max_shard_bytes: Option<u64>,
    parallel_scan_min_shard_bytes: Option<u64>,
    parallel_scan_max_shard_elapsed_ms: Option<f64>,
    parallel_scan_min_shard_elapsed_ms: Option<f64>,
    parallel_scan_max_active_iterators: Option<u64>,
    parallel_scan_max_shard_block_cache_hits: Option<u64>,
    parallel_scan_max_shard_block_cache_misses: Option<u64>,
    parallel_scan_max_shard_cache_admitted: Option<u64>,
    parallel_scan_max_shard_cache_rejected: Option<u64>,
    parallel_scan_max_shard_cache_evicted: Option<u64>,
    parallel_scan_max_shard_block_loads: Option<u64>,
    parallel_scan_max_shard_sst_switches: Option<u64>,
    parallel_scan_coordinator_wait_ms: Option<f64>,
}

#[derive(Clone, Default, Serialize)]
struct MeasurementCounters {
    block_cache_entry_count: u64,
    value_cache_hit_count: u64,
    value_cache_miss_count: u64,
    vlog_total_bytes: Option<u64>,
    vlog_file_count: Option<u32>,
    vlog_gc_entries_rewritten: Option<u64>,
    vlog_gc_bytes_rewritten: Option<u64>,
    vlog_gc_files_processed: Option<u64>,
    compaction_filter_entries_eligible: u64,
    compaction_filter_entries_dropped: u64,
    compaction_filter_bytes_dropped: u64,
    compaction_filter_filters_active: usize,
    range_tombstone_active_count: u64,
    range_tombstone_immutable_count: u64,
    range_tombstone_sst_count: u64,
    range_tombstone_total_sst_fragment_count: u64,
    parallel_scan_planned_scans: u64,
    parallel_scan_single_shard_fallback_scans: u64,
    parallel_scan_total_shards_planned: u64,
    parallel_scan_rows_emitted: u64,
    parallel_scan_bytes_emitted: u64,
}

fn print_write_profile(engine: &KvEngine, label: &str) {
    let p = engine.write_profile();
    if p.op_count == 0 {
        return;
    }
    let total = p.total_ms();
    eprintln!(
        "\n--- write profile: {label} ({} ops) ---\n  \
         wal_write:    {:>8.2} ms  ({:>5.1}%)\n  \
         wal_sync:     {:>8.2} ms  ({:>5.1}%)\n  \
         memtable:     {:>8.2} ms  ({:>5.1}%)\n  \
         total:        {:>8.2} ms",
        p.op_count,
        p.wal_write_ms(),
        if total > 0.0 {
            p.wal_write_ms() / total * 100.0
        } else {
            0.0
        },
        p.wal_sync_ms(),
        p.wal_sync_pct(),
        p.memtable_insert_ms(),
        if total > 0.0 {
            p.memtable_insert_ms() / total * 100.0
        } else {
            0.0
        },
        total,
    );
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bench_arg = args.bench.clone();
    let cfg = HarnessConfig::from_args(args);
    validate_config(&cfg)?;
    let _hotpath_guard = if cfg.profile {
        let guard = kv_engine::profiling::start_hotpath_profile("write-perf");
        if guard.is_some() {
            eprintln!(
                "hotpath-profile enabled; set HOTPATH_TIME_SAMPLING_RATE=0.1 to reduce timing overhead and HOTPATH_METRICS_SERVER_OFF=true in restricted environments"
            );
        }
        guard
    } else {
        None
    };
    let workloads = select_workloads(bench_arg.as_deref())?;

    let mut all_measurements = Vec::new();
    for workload in workloads {
        all_measurements.extend((workload.run)(&cfg)?);
    }

    emit_measurements(&cfg, &all_measurements)
}

fn select_workloads(filter: Option<&str>) -> Result<Vec<&'static WorkloadSpec>> {
    match filter {
        None => Ok(WORKLOADS.iter().collect()),
        Some(filter) => {
            let mut selected = Vec::new();
            for name in filter.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if name == "readreverse" {
                    bail!(
                        "workload `readreverse` is unsupported until reverse iteration exists; use `readseq_validate_order` for the current forward-scan placeholder"
                    );
                }
                let workload = WORKLOADS
                    .iter()
                    .find(|w| w.name == name || w.aliases.contains(&name))
                    .with_context(|| format!("unknown workload: {name}"))?;
                selected.push(workload);
            }
            Ok(selected)
        }
    }
}

fn emit_measurements(cfg: &HarnessConfig, measurements: &[BenchMeasurement]) -> Result<()> {
    for measurement in measurements {
        match cfg.output {
            OutputFormat::Text => {
                println!("{}", measurement.summary);
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string(&measurement.record)?);
            }
            OutputFormat::Both => {
                eprintln!("{}", measurement.summary);
                println!("{}", serde_json::to_string(&measurement.record)?);
            }
        }
    }
    Ok(())
}

fn run_scan(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "scan";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.scan_num, cfg.value_size)?;
    if !matches!(cfg.compaction, CompactionMode::None) {
        engine.force_full_compaction()?;
    }
    let baseline = collect_counters(&engine)?;

    let mut measurements = Vec::new();

    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded)?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    let after_full_scan = collect_counters(&engine)?;
    measurements.push(make_measurement(
        cfg,
        workload,
        "full_scan",
        &options,
        MeasurementParams {
            num: Some(cfg.scan_num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            parallel_scan_max_parallelism: Some(cfg.parallel_scan_max_parallelism),
            parallel_scan_batch_rows: Some(cfg.parallel_scan_batch_rows),
            parallel_scan_batch_bytes: Some(cfg.parallel_scan_batch_bytes),
            parallel_scan_yield_every_rows: Some(cfg.parallel_scan_yield_every_rows),
            parallel_scan_channel_capacity: Some(cfg.parallel_scan_channel_capacity),
            parallel_scan_cache_admission: Some(cfg.parallel_scan_cache_admission.clone()),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(count),
            entries_per_sec: Some(rate(count, elapsed)),
            ..MeasurementResult::default()
        },
        collect_counter_delta(&baseline, &after_full_scan),
    ));

    let hi = cfg.scan_num / 10;
    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(
        Bound::Included(format!("key{:08}", 0).as_bytes()),
        Bound::Excluded(format!("key{:08}", hi).as_bytes()),
    )?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    let after_partial_scan = collect_counters(&engine)?;
    measurements.push(make_measurement(
        cfg,
        workload,
        "scan_10pct",
        &options,
        MeasurementParams {
            num: Some(cfg.scan_num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(count),
            entries_per_sec: Some(rate(count, elapsed)),
            ..MeasurementResult::default()
        },
        collect_counter_delta(&after_full_scan, &after_partial_scan),
    ));

    engine.close()?;
    finalize_path(cfg, &path)?;
    Ok(measurements)
}

fn run_parallel_scan(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "parallel_scan";
    let path = prepare_path(cfg, workload)?;
    let mut options = cfg.build_options(false, false);
    if !matches!(cfg.compaction, CompactionMode::None) {
        options.compaction_options = CompactionOptions::Simple(SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 2,
            max_levels: 2,
        });
    }
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let batches = 8usize.min(cfg.scan_num.max(1));
    let batch_size = cfg.scan_num.div_ceil(batches);
    let load_start = Instant::now();
    for batch in 0..batches {
        let begin = batch * batch_size;
        let end = (begin + batch_size).min(cfg.scan_num);
        for i in begin..end {
            engine.put(format!("key{:08}", i).as_bytes(), &value)?;
        }
        engine.force_flush()?;
    }
    engine.drain_flush()?;
    let load_elapsed = load_start.elapsed();
    if !matches!(cfg.compaction, CompactionMode::None) {
        engine.force_full_compaction()?;
    }
    let baseline = collect_counters(&engine)?;

    let mut measurements = Vec::new();

    let start = Instant::now();
    let mut sync_count = 0u64;
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded)?;
    while iter.is_valid() {
        sync_count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    let after_sync_scan = collect_counters(&engine)?;
    measurements.push(make_measurement(
        cfg,
        workload,
        "sync_full_scan",
        &options,
        MeasurementParams {
            num: Some(cfg.scan_num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(sync_count),
            entries_per_sec: Some(rate(sync_count, elapsed)),
            ..MeasurementResult::default()
        },
        collect_counter_delta(&baseline, &after_sync_scan),
    ));

    let start = Instant::now();
    let mut parallel_chunk_count = 0u64;
    let mut chunk_scan = block_on(engine.scan_parallel_async(
        Bound::Unbounded,
        Bound::Unbounded,
        cfg.parallel_scan_options(),
    ))?;
    while let Some(chunk) = block_on(chunk_scan.try_next_chunk())? {
        parallel_chunk_count += chunk.len() as u64;
    }
    let chunk_stats = chunk_scan.stats();
    let max_shard_rows = chunk_stats.shard_stats.iter().map(|s| s.rows).max();
    let min_shard_rows = chunk_stats.shard_stats.iter().map(|s| s.rows).min();
    let max_shard_bytes = chunk_stats.shard_stats.iter().map(|s| s.bytes).max();
    let min_shard_bytes = chunk_stats.shard_stats.iter().map(|s| s.bytes).min();
    let max_shard_elapsed_ms = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.elapsed_us as f64 / 1000.0)
        .reduce(f64::max);
    let min_shard_elapsed_ms = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.elapsed_us as f64 / 1000.0)
        .reduce(f64::min);
    let max_active_iterators = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.max_active_iterators)
        .max();
    let max_shard_block_cache_hits = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.block_cache_hits)
        .max();
    let max_shard_block_cache_misses = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.block_cache_misses)
        .max();
    let max_shard_cache_admitted = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.cache_admitted)
        .max();
    let max_shard_cache_rejected = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.cache_rejected)
        .max();
    let max_shard_cache_evicted = chunk_stats
        .shard_stats
        .iter()
        .map(|s| s.cache_evicted)
        .max();
    let max_shard_block_loads = chunk_stats.shard_stats.iter().map(|s| s.block_loads).max();
    let max_shard_sst_switches = chunk_stats.shard_stats.iter().map(|s| s.sst_switches).max();
    let elapsed = start.elapsed();
    let after_parallel_chunk = collect_counters(&engine)?;
    measurements.push(make_measurement(
        cfg,
        workload,
        "parallel_chunk_full_scan",
        &options,
        MeasurementParams {
            num: Some(cfg.scan_num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            parallel_scan_max_parallelism: Some(cfg.parallel_scan_max_parallelism),
            parallel_scan_batch_rows: Some(cfg.parallel_scan_batch_rows),
            parallel_scan_batch_bytes: Some(cfg.parallel_scan_batch_bytes),
            parallel_scan_yield_every_rows: Some(cfg.parallel_scan_yield_every_rows),
            parallel_scan_channel_capacity: Some(cfg.parallel_scan_channel_capacity),
            parallel_scan_cache_admission: Some(cfg.parallel_scan_cache_admission.clone()),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(parallel_chunk_count),
            entries_per_sec: Some(rate(parallel_chunk_count, elapsed)),
            parallel_scan_planned_shards: Some(chunk_stats.planned_shards),
            parallel_scan_max_shard_rows: max_shard_rows,
            parallel_scan_min_shard_rows: min_shard_rows,
            parallel_scan_max_shard_bytes: max_shard_bytes,
            parallel_scan_min_shard_bytes: min_shard_bytes,
            parallel_scan_max_shard_elapsed_ms: max_shard_elapsed_ms,
            parallel_scan_min_shard_elapsed_ms: min_shard_elapsed_ms,
            parallel_scan_max_active_iterators: max_active_iterators,
            parallel_scan_max_shard_block_cache_hits: max_shard_block_cache_hits,
            parallel_scan_max_shard_block_cache_misses: max_shard_block_cache_misses,
            parallel_scan_max_shard_cache_admitted: max_shard_cache_admitted,
            parallel_scan_max_shard_cache_rejected: max_shard_cache_rejected,
            parallel_scan_max_shard_cache_evicted: max_shard_cache_evicted,
            parallel_scan_max_shard_block_loads: max_shard_block_loads,
            parallel_scan_max_shard_sst_switches: max_shard_sst_switches,
            parallel_scan_coordinator_wait_ms: Some(
                chunk_stats.coordinator_wait_us as f64 / 1000.0,
            ),
            ..MeasurementResult::default()
        },
        collect_counter_delta(&after_sync_scan, &after_parallel_chunk),
    ));

    engine.close()?;
    finalize_path(cfg, &path)?;
    Ok(measurements)
}

fn run_concurrent_rw_no_wal(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_concurrent_rw(cfg, "concurrent_rw_no_wal", false)
}

fn run_concurrent_rw_wal(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_concurrent_rw(cfg, "concurrent_rw_wal", true)
}

fn run_concurrent_rw(
    cfg: &HarnessConfig,
    workload: &str,
    wal: bool,
) -> Result<Vec<BenchMeasurement>> {
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(wal, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let stop = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let scan_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    let value_size = cfg.value_size;
    let num = cfg.num;
    let seed = cfg.seed;
    for t in 0..cfg.threads {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = write_count.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(t as u64));
            let value = vec![b'x'; value_size];
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num as u64));
                if eng.put(key.as_bytes(), &value).is_ok() {
                    local += 1;
                }
            }
            wc.fetch_add(local, Ordering::Relaxed);
        }));
    }

    for t in 0..cfg.readers {
        let eng = engine.clone();
        let stop = stop.clone();
        let rc = read_count.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1_000 + t as u64));
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num as u64));
                let _ = eng.get(key.as_bytes());
                local += 1;
            }
            rc.fetch_add(local, Ordering::Relaxed);
        }));
    }

    {
        let eng = engine.clone();
        let stop = stop.clone();
        let sc = scan_count.clone();
        handles.push(std::thread::spawn(move || {
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Ok(mut iter) = eng.scan(Bound::Unbounded, Bound::Unbounded) {
                    while iter.is_valid() {
                        if iter.next().is_err() {
                            break;
                        }
                        local += 1;
                    }
                }
            }
            sc.fetch_add(local, Ordering::Relaxed);
        }));
    }

    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(cfg.duration_secs));
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("worker thread panicked"))?;
    }
    let elapsed = start.elapsed();

    let writes = write_count.load(Ordering::Relaxed);
    let reads = read_count.load(Ordering::Relaxed);
    let scan_entries = scan_count.load(Ordering::Relaxed);
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "mixed",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            duration_secs: Some(cfg.duration_secs),
            threads: Some(cfg.threads),
            readers: Some(cfg.readers),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            writes: Some(writes),
            writes_per_sec: Some(rate(writes, elapsed)),
            reads: Some(reads),
            reads_per_sec: Some(rate(reads, elapsed)),
            entries: Some(scan_entries),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_wal_throughput(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "wal_throughput";
    let options = cfg.build_options(true, false);
    let mut results = Vec::new();
    let cases = if cfg.num_overridden || cfg.value_size_overridden {
        vec![("seq".to_string(), cfg.num, cfg.value_size)]
    } else {
        vec![
            ("seq_256b".to_string(), 50_000usize, 256usize),
            ("seq_4096b".to_string(), 20_000usize, 4096usize),
        ]
    };
    for (measurement, num_keys, value_size) in cases {
        let path = prepare_path(cfg, &format!("{workload}_{measurement}"))?;
        let engine = KvEngine::open(&path, options.clone())?;
        let value = vec![b'x'; value_size];
        let start = Instant::now();
        for i in 0..num_keys {
            engine.put(format!("key{:08}", i).as_bytes(), &value)?;
        }
        let elapsed = start.elapsed();
        if cfg.profile {
            print_write_profile(&engine, &format!("{workload}_{measurement}"));
        }
        engine.drain_flush()?;
        let counters = collect_counters(&engine)?;
        engine.close()?;
        finalize_path(cfg, &path)?;
        results.push(make_measurement(
            cfg,
            workload,
            measurement,
            &options,
            MeasurementParams {
                num: Some(num_keys),
                value_size: Some(value_size),
                seed: Some(cfg.seed),
                ..MeasurementParams::default()
            },
            MeasurementResult {
                measure_elapsed_ms: ms(elapsed),
                ops: Some(num_keys as u64),
                ops_per_sec: Some(rate(num_keys as u64, elapsed)),
                ..MeasurementResult::default()
            },
            counters,
        ));
    }
    Ok(results)
}

fn run_wal_concurrent(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "wal_concurrent";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(true, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let num_keys = cfg.num;
    let writer_threads = cfg.threads;
    let baseline = collect_counters(&engine)?;
    let per_thread = num_keys / writer_threads;
    let remainder = num_keys % writer_threads;
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..writer_threads {
        let eng = engine.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || {
            let thread_ops = per_thread + usize::from(t < remainder);
            let start_idx = t * per_thread + remainder.min(t);
            for i in 0..thread_ops {
                eng.put(format!("key{:08}", start_idx + i).as_bytes(), &val)
                    .expect("put failed");
            }
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("writer thread panicked"))?;
    }
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    engine.drain_flush()?;
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "concurrent",
        &options,
        MeasurementParams {
            num: Some(num_keys),
            value_size: Some(cfg.value_size),
            threads: Some(writer_threads),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            measure_elapsed_ms: ms(elapsed),
            ops: Some(num_keys as u64),
            ops_per_sec: Some(rate(num_keys as u64, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_vlog_gc(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "vlog_gc";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, true);
    let engine = KvEngine::open(&path, options.clone())?;
    let entries_per_round = 5_000usize;
    let num_rounds = 3usize;
    let initial_value = vec![b'x'; 4096];
    let load_elapsed = populate_fixed_value(&engine, entries_per_round, &initial_value)?;
    let mut baseline = collect_counters(&engine)?;
    let mut measurements = Vec::new();

    for round in 1..=num_rounds {
        let padding = "x".repeat(4000);
        let value = format!("v{round}_{padding}");
        let write_start = Instant::now();
        for i in 0..entries_per_round {
            engine.put(format!("key{:08}", i).as_bytes(), value.as_bytes())?;
        }
        let write_elapsed = write_start.elapsed();
        engine.drain_flush()?;

        let gc_start = Instant::now();
        let _gc_count = engine.trigger_gc()?;
        let gc_elapsed = gc_start.elapsed();

        let after_round = collect_counters(&engine)?;
        measurements.push(make_measurement(
            cfg,
            workload,
            format!("round_{round}"),
            &options,
            MeasurementParams {
                num: Some(entries_per_round),
                value_size: Some(4096),
                seed: Some(cfg.seed),
                ..MeasurementParams::default()
            },
            MeasurementResult {
                load_elapsed_ms: Some(ms(load_elapsed)),
                measure_elapsed_ms: ms(write_elapsed + gc_elapsed),
                write_elapsed_ms: Some(ms(write_elapsed)),
                gc_elapsed_ms: Some(ms(gc_elapsed)),
                writes: Some(entries_per_round as u64),
                writes_per_sec: Some(rate(entries_per_round as u64, write_elapsed)),
                gc_rounds: Some(1),
                ..MeasurementResult::default()
            },
            collect_counter_delta(&baseline, &after_round),
        ));
        baseline = after_round;
    }

    engine.close()?;
    finalize_path(cfg, &path)?;
    Ok(measurements)
}

fn run_vlog_concurrent_gc(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "vlog_concurrent_gc";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, true);
    let engine = KvEngine::open(&path, options.clone())?;
    let initial_value = vec![b'x'; 4096];
    let load_elapsed = populate_fixed_value(&engine, 5_000, &initial_value)?;
    let baseline = collect_counters(&engine)?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let rc = Arc::new(AtomicU64::new(0));
    let gcc = Arc::new(AtomicU64::new(0));
    let seed = cfg.seed;
    let mut handles = vec![];

    {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed);
            let val = vec![b'y'; 4096];
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..5_000));
                if eng.put(key.as_bytes(), &val).is_ok() {
                    c += 1;
                }
            }
            wc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    {
        let eng = engine.clone();
        let stop = stop.clone();
        let rc = rc.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100));
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..5_000));
                if eng.get(key.as_bytes()).is_ok() {
                    c += 1;
                }
            }
            rc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    {
        let eng = engine.clone();
        let stop = stop.clone();
        let gcc = gcc.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if eng.trigger_gc().is_ok() {
                    c += 1;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
            gcc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(cfg.duration_secs));
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("worker thread panicked"))?;
    }
    let elapsed = start.elapsed();

    let writes = wc.load(Ordering::Relaxed);
    let reads = rc.load(Ordering::Relaxed);
    let gc_rounds = gcc.load(Ordering::Relaxed);
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "mixed",
        &options,
        MeasurementParams {
            duration_secs: Some(cfg.duration_secs),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            writes: Some(writes),
            writes_per_sec: Some(rate(writes, elapsed)),
            reads: Some(reads),
            reads_per_sec: Some(rate(reads, elapsed)),
            gc_rounds: Some(gc_rounds),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_fillseq(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "fillseq";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let start = Instant::now();
    for i in 0..cfg.num {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    engine.drain_flush()?;
    let counters = collect_counters(&engine)?;
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "write",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.num as u64),
            ops_per_sec: Some(rate(cfg.num as u64, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_fillrandom(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "fillrandom";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    for _ in 0..cfg.num {
        let n = rng.gen_range(0..cfg.num as u64);
        write!(&mut key_buf[3..], "{:08}", n).expect("key format");
        engine.put(&key_buf, &value)?;
    }
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    engine.drain_flush()?;
    let counters = collect_counters(&engine)?;
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "write",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.num as u64),
            ops_per_sec: Some(rate(cfg.num as u64, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_readrandom(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "readrandom";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let mut rng = StdRng::seed_from_u64(cfg.seed + 123);
    let mut key = String::with_capacity(11);
    let start = Instant::now();
    let mut found = 0u64;
    for _ in 0..cfg.reads {
        key.clear();
        let _ = write!(&mut key, "key{:08}", rng.gen_range(0..cfg.num as u64));
        if engine.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "point_get",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            reads: Some(cfg.reads),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed + 123),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.reads as u64),
            ops_per_sec: Some(rate(cfg.reads as u64, elapsed)),
            found: Some(found),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_readwhilewriting(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "readwhilewriting";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let rc = Arc::new(AtomicU64::new(0));
    let seed = cfg.seed;
    let mut handles = vec![];

    {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        let value = vec![b'x'; cfg.value_size];
        let num = cfg.num;
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num as u64));
                if eng.put(key.as_bytes(), &value).is_ok() {
                    c += 1;
                }
            }
            wc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    for t in 0..cfg.readers {
        let eng = engine.clone();
        let stop = stop.clone();
        let rc = rc.clone();
        let num = cfg.num;
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1000 + t as u64));
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num as u64));
                if eng.get(key.as_bytes()).is_ok() {
                    c += 1;
                }
            }
            rc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(cfg.duration_secs));
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("worker thread panicked"))?;
    }
    let elapsed = start.elapsed();

    let writes = wc.load(Ordering::Relaxed);
    let reads = rc.load(Ordering::Relaxed);
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "mixed",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            duration_secs: Some(cfg.duration_secs),
            readers: Some(cfg.readers),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            writes: Some(writes),
            writes_per_sec: Some(rate(writes, elapsed)),
            reads: Some(reads),
            reads_per_sec: Some(rate(reads, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_readrandomwriterandom(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "readrandomwriterandom";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let rc = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for t in 0..cfg.threads {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        let rc = rc.clone();
        let value = vec![b'x'; cfg.value_size];
        let num = cfg.num;
        let seed = cfg.seed;
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(t as u64));
            let mut writes = 0u64;
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key{:08}", rng.gen_range(0..num as u64));
                if rng.gen_bool(0.5) {
                    if eng.put(key.as_bytes(), &value).is_ok() {
                        writes += 1;
                    }
                } else if eng.get(key.as_bytes()).is_ok() {
                    reads += 1;
                }
            }
            wc.fetch_add(writes, Ordering::Relaxed);
            rc.fetch_add(reads, Ordering::Relaxed);
        }));
    }

    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(cfg.duration_secs));
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("worker thread panicked"))?;
    }
    let elapsed = start.elapsed();

    let writes = wc.load(Ordering::Relaxed);
    let reads = rc.load(Ordering::Relaxed);
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "mixed",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            duration_secs: Some(cfg.duration_secs),
            threads: Some(cfg.threads),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            writes: Some(writes),
            writes_per_sec: Some(rate(writes, elapsed)),
            reads: Some(reads),
            reads_per_sec: Some(rate(reads, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_seekrandom(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "seekrandom";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_fixed_value(&engine, cfg.num, &vec![b'x'; cfg.value_size])?;
    let baseline = collect_counters(&engine)?;

    let mut rng = StdRng::seed_from_u64(cfg.seed + 999);
    let start = Instant::now();
    let mut total_nexts = 0u64;
    for _ in 0..cfg.seeks {
        let key = format!("key{:08}", rng.gen_range(0..cfg.num as u64));
        if let Ok(mut iter) = engine.scan(Bound::Included(key.as_bytes()), Bound::Unbounded) {
            for _ in 0..cfg.seek_nexts {
                if !iter.is_valid() {
                    break;
                }
                if iter.next().is_err() {
                    break;
                }
                total_nexts += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let counters = collect_counters(&engine)?;
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "seek",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            seeks: Some(cfg.seeks),
            seek_nexts: Some(cfg.seek_nexts),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed + 999),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.seeks as u64),
            ops_per_sec: Some(rate(cfg.seeks as u64, elapsed)),
            total_nexts: Some(total_nexts),
            ..MeasurementResult::default()
        },
        collect_counter_delta(&baseline, &counters),
    )])
}

fn run_overwrite(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "overwrite";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let value = vec![b'y'; cfg.value_size];
    let start = Instant::now();
    for _ in 0..cfg.num {
        let n = rng.gen_range(0..cfg.num as u64);
        write!(&mut key_buf[3..], "{:08}", n).expect("key format");
        engine.put(&key_buf, &value)?;
    }
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    engine.drain_flush()?;
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "update",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.num as u64),
            ops_per_sec: Some(rate(cfg.num as u64, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_readseq(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "readseq";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let start = Instant::now();
    let mut count = 0u64;
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded)?;
    while iter.is_valid() {
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    anyhow::ensure!(
        count == cfg.num as u64,
        "readseq expected {} entries, got {}",
        cfg.num,
        count
    );
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "forward_scan",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(count),
            entries_per_sec: Some(rate(count, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_readseq_validate_order(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "readseq_validate_order";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let start = Instant::now();
    let mut count = 0u64;
    let mut prev_key = Vec::new();
    let mut has_prev = false;
    let mut iter = engine.scan(Bound::Unbounded, Bound::Unbounded)?;
    while iter.is_valid() {
        let current = iter.key();
        if has_prev {
            anyhow::ensure!(
                prev_key.as_slice() < current,
                "readseq_validate_order detected out-of-order scan: prev={:?} current={:?}",
                prev_key,
                current
            );
        }
        prev_key.clear();
        prev_key.extend_from_slice(current);
        has_prev = true;
        count += 1;
        iter.next()?;
    }
    let elapsed = start.elapsed();
    anyhow::ensure!(
        count == cfg.num as u64,
        "readseq_validate_order expected {} entries, got {}",
        cfg.num,
        count
    );
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "forward_placeholder",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(count),
            entries_per_sec: Some(rate(count, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_readmissing(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "readmissing";
    anyhow::ensure!(cfg.num >= 2, "readmissing requires --num >= 2");
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in (0..cfg.num).step_by(2) {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.drain_flush()?;
    let load_elapsed = load_start.elapsed();
    let baseline = collect_counters(&engine)?;

    let mut rng = StdRng::seed_from_u64(cfg.seed + 777);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    let mut found = 0u64;
    for _ in 0..cfg.reads {
        let n = rng.gen_range(0..cfg.num as u64 / 2) * 2 + 1;
        write!(&mut key_buf[3..], "{:08}", n).expect("key format");
        if engine.get(&key_buf)?.is_some() {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    anyhow::ensure!(
        found == 0,
        "readmissing expected 0 found entries, got {}",
        found
    );
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "negative_point_get",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            reads: Some(cfg.reads),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed + 777),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.reads as u64),
            ops_per_sec: Some(rate(cfg.reads as u64, elapsed)),
            found: Some(found),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_seekrandomwhilewriting(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "seekrandomwhilewriting";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_fixed_value(&engine, cfg.num, &vec![b'x'; cfg.value_size])?;
    let baseline = collect_counters(&engine)?;

    let stop = Arc::new(AtomicBool::new(false));
    let wc = Arc::new(AtomicU64::new(0));
    let write_err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let mut handles = vec![];

    {
        let eng = engine.clone();
        let stop = stop.clone();
        let wc = wc.clone();
        let write_err = write_err.clone();
        let num = cfg.num;
        let value_size = cfg.value_size;
        let seed = cfg.seed;
        handles.push(std::thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut key_buf = [0u8; 11];
            key_buf[..3].copy_from_slice(b"key");
            let value = vec![b'x'; value_size];
            let mut c = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let n = rng.gen_range(0..num as u64);
                write!(&mut key_buf[3..], "{:08}", n).expect("key format");
                match eng.put(&key_buf, &value) {
                    Ok(()) => c += 1,
                    Err(e) => {
                        *write_err.lock() = Some(format!("{e}"));
                        break;
                    }
                }
            }
            wc.fetch_add(c, Ordering::Relaxed);
        }));
    }

    let mut rng = StdRng::seed_from_u64(cfg.seed + 999);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    let seek_result: Result<u64> = (|| {
        let mut total_nexts = 0u64;
        for _ in 0..cfg.seeks {
            let n = rng.gen_range(0..cfg.num as u64);
            write!(&mut key_buf[3..], "{:08}", n).expect("key format");
            let mut iter = engine.scan(Bound::Included(&key_buf), Bound::Unbounded)?;
            for _ in 0..cfg.seek_nexts {
                if !iter.is_valid() {
                    break;
                }
                iter.next()?;
                total_nexts += 1;
            }
        }
        Ok(total_nexts)
    })();
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("writer thread panicked"))?;
    }
    if let Some(err) = write_err.lock().take() {
        bail!("writer thread error: {err}");
    }

    let writes = wc.load(Ordering::Relaxed);
    let total_nexts = seek_result?;
    let counters = collect_counters(&engine)?;
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "seek_mixed",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            seeks: Some(cfg.seeks),
            seek_nexts: Some(cfg.seek_nexts),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed + 999),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.seeks as u64),
            ops_per_sec: Some(rate(cfg.seeks as u64, elapsed)),
            writes: Some(writes),
            total_nexts: Some(total_nexts),
            ..MeasurementResult::default()
        },
        collect_counter_delta(&baseline, &counters),
    )])
}

fn run_deleterandom(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "deleterandom";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_fixed_value(&engine, cfg.num, &vec![b'x'; cfg.value_size])?;
    let baseline = collect_counters(&engine)?;

    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut key_buf = [0u8; 11];
    key_buf[..3].copy_from_slice(b"key");
    let start = Instant::now();
    for _ in 0..cfg.num {
        let n = rng.gen_range(0..cfg.num as u64);
        write!(&mut key_buf[3..], "{:08}", n).expect("key format");
        engine.delete(&key_buf)?;
    }
    let elapsed = start.elapsed();
    engine.drain_flush()?;
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "delete",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.num as u64),
            ops_per_sec: Some(rate(cfg.num as u64, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_compact(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "compact";
    let path = prepare_path(cfg, workload)?;
    let mut options = cfg.build_options(false, false);
    options.compaction_options = CompactionOptions::NoCompaction;
    let engine = KvEngine::open(&path, options.clone())?;
    let load_elapsed = populate_range(&engine, cfg.num, cfg.value_size)?;
    let baseline = collect_counters(&engine)?;

    let start = Instant::now();
    engine.force_full_compaction()?;
    let elapsed = start.elapsed();
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "full_compaction",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            entries: Some(cfg.num as u64),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn make_measurement(
    cfg: &HarnessConfig,
    workload: &str,
    measurement: impl Into<String>,
    options: &LsmStorageOptions,
    params: MeasurementParams,
    result: MeasurementResult,
    counters: MeasurementCounters,
) -> BenchMeasurement {
    let measurement = measurement.into();
    let record = MeasurementRecord {
        schema: JSON_SCHEMA,
        unix_epoch_ms: unix_epoch_ms(),
        run_id: cfg.run_id.clone(),
        workload: workload.to_string(),
        measurement: measurement.clone(),
        preset: cfg.preset_name,
        engine: ENGINE_NAME,
        engine_options: EngineOptionsRecord {
            wal: options.enable_wal,
            value_separation: options
                .value_separation
                .as_ref()
                .is_some_and(|vlog| vlog.enabled),
            compaction: compaction_name(&options.compaction_options),
            target_sst_size: options.target_sst_size,
            memtable_limit: options.num_memtable_limit,
            cache_capacity: options.block_cache_capacity,
        },
        params,
        result,
        counters,
    };
    let summary = summary_for(&record);
    BenchMeasurement { record, summary }
}

fn summary_for(record: &MeasurementRecord) -> String {
    let mut summary = format!("{}/{}", record.workload, record.measurement);
    if let Some(num) = record.params.num {
        let _ = write!(summary, " num={num}");
    }
    if let Some(reads) = record.params.reads {
        let _ = write!(summary, " reads={reads}");
    }
    if let Some(duration) = record.params.duration_secs {
        let _ = write!(summary, " duration={}s", duration);
    }
    if let Some(value_size) = record.params.value_size {
        let _ = write!(summary, " value={}B", value_size);
    }
    let _ = write!(
        summary,
        " measure={:.2}ms",
        record.result.measure_elapsed_ms
    );
    if let Some(ops) = record.result.ops {
        let _ = write!(summary, " ops={ops}");
    }
    if let Some(rate) = record.result.ops_per_sec {
        let _ = write!(summary, " ops/s={:.0}", rate);
    }
    if let Some(entries) = record.result.entries {
        let _ = write!(summary, " entries={entries}");
    }
    if let Some(rate) = record.result.entries_per_sec {
        let _ = write!(summary, " entries/s={:.0}", rate);
    }
    if let Some(writes) = record.result.writes {
        let _ = write!(summary, " writes={writes}");
    }
    if let Some(rate) = record.result.writes_per_sec {
        let _ = write!(summary, " writes/s={:.0}", rate);
    }
    if let Some(reads) = record.result.reads {
        let _ = write!(summary, " reads={reads}");
    }
    if let Some(rate) = record.result.reads_per_sec {
        let _ = write!(summary, " reads/s={:.0}", rate);
    }
    if let Some(found) = record.result.found {
        let _ = write!(summary, " found={found}");
    }
    if let Some(nexts) = record.result.total_nexts {
        let _ = write!(summary, " nexts={nexts}");
    }
    if let Some(rounds) = record.result.gc_rounds {
        let _ = write!(summary, " gc_rounds={rounds}");
    }
    if let Some(shards) = record.result.parallel_scan_planned_shards {
        let _ = write!(summary, " shards={shards}");
    }
    if let (Some(max_rows), Some(min_rows)) = (
        record.result.parallel_scan_max_shard_rows,
        record.result.parallel_scan_min_shard_rows,
    ) {
        let _ = write!(summary, " shard_rows={}..{}", min_rows, max_rows);
    }
    if let (Some(max_ms), Some(min_ms)) = (
        record.result.parallel_scan_max_shard_elapsed_ms,
        record.result.parallel_scan_min_shard_elapsed_ms,
    ) {
        let _ = write!(summary, " shard_ms={:.2}..{:.2}", min_ms, max_ms);
    }
    if let Some(max_iters) = record.result.parallel_scan_max_active_iterators {
        let _ = write!(summary, " active_iters_max={max_iters}");
    }
    if let Some(wait_ms) = record.result.parallel_scan_coordinator_wait_ms {
        let _ = write!(summary, " coordinator_wait_ms={:.2}", wait_ms);
    }
    if let (Some(max_hits), Some(max_misses)) = (
        record.result.parallel_scan_max_shard_block_cache_hits,
        record.result.parallel_scan_max_shard_block_cache_misses,
    ) {
        let _ = write!(
            summary,
            " shard_cache_max_hits={} max_misses={}",
            max_hits, max_misses
        );
    }
    if let (Some(max_admitted), Some(max_rejected), Some(max_evicted)) = (
        record.result.parallel_scan_max_shard_cache_admitted,
        record.result.parallel_scan_max_shard_cache_rejected,
        record.result.parallel_scan_max_shard_cache_evicted,
    ) {
        let _ = write!(
            summary,
            " shard_cache_admitted={} rejected={} evicted={}",
            max_admitted, max_rejected, max_evicted
        );
    }
    if let (Some(max_block_loads), Some(max_sst_switches)) = (
        record.result.parallel_scan_max_shard_block_loads,
        record.result.parallel_scan_max_shard_sst_switches,
    ) {
        let _ = write!(
            summary,
            " shard_blocks_max={} shard_sst_switches_max={}",
            max_block_loads, max_sst_switches
        );
    }
    summary
}

fn collect_counters(engine: &KvEngine) -> Result<MeasurementCounters> {
    let cache = engine.cache_stats();
    let range = engine.range_tombstone_stats();
    let filters = engine.compaction_filter_stats();
    let parallel = engine.parallel_scan_stats();
    let vlog = engine.vlog_stats().ok();
    Ok(MeasurementCounters {
        block_cache_entry_count: cache.block_cache_entry_count,
        value_cache_hit_count: cache.value_cache_hit_count,
        value_cache_miss_count: cache.value_cache_miss_count,
        vlog_total_bytes: vlog.as_ref().map(|s| s.vlog_total_bytes),
        vlog_file_count: vlog.as_ref().map(|s| s.vlog_file_count),
        vlog_gc_entries_rewritten: vlog.as_ref().map(|s| s.gc_entries_rewritten),
        vlog_gc_bytes_rewritten: vlog.as_ref().map(|s| s.gc_bytes_rewritten),
        vlog_gc_files_processed: vlog.as_ref().map(|s| s.gc_files_processed),
        compaction_filter_entries_eligible: filters.entries_eligible,
        compaction_filter_entries_dropped: filters.entries_dropped,
        compaction_filter_bytes_dropped: filters.bytes_dropped,
        compaction_filter_filters_active: filters.filters_active,
        range_tombstone_active_count: range.active_count,
        range_tombstone_immutable_count: range.immutable_count,
        range_tombstone_sst_count: range.sst_count,
        range_tombstone_total_sst_fragment_count: range.total_sst_fragment_count,
        parallel_scan_planned_scans: parallel.planned_scans,
        parallel_scan_single_shard_fallback_scans: parallel.single_shard_fallback_scans,
        parallel_scan_total_shards_planned: parallel.total_shards_planned,
        parallel_scan_rows_emitted: parallel.rows_emitted,
        parallel_scan_bytes_emitted: parallel.bytes_emitted,
    })
}

fn collect_counter_delta(
    before: &MeasurementCounters,
    after: &MeasurementCounters,
) -> MeasurementCounters {
    MeasurementCounters {
        block_cache_entry_count: after
            .block_cache_entry_count
            .saturating_sub(before.block_cache_entry_count),
        value_cache_hit_count: after
            .value_cache_hit_count
            .saturating_sub(before.value_cache_hit_count),
        value_cache_miss_count: after
            .value_cache_miss_count
            .saturating_sub(before.value_cache_miss_count),
        vlog_total_bytes: diff_option_u64(before.vlog_total_bytes, after.vlog_total_bytes),
        vlog_file_count: diff_option_u32(before.vlog_file_count, after.vlog_file_count),
        vlog_gc_entries_rewritten: diff_option_u64(
            before.vlog_gc_entries_rewritten,
            after.vlog_gc_entries_rewritten,
        ),
        vlog_gc_bytes_rewritten: diff_option_u64(
            before.vlog_gc_bytes_rewritten,
            after.vlog_gc_bytes_rewritten,
        ),
        vlog_gc_files_processed: diff_option_u64(
            before.vlog_gc_files_processed,
            after.vlog_gc_files_processed,
        ),
        compaction_filter_entries_eligible: after
            .compaction_filter_entries_eligible
            .saturating_sub(before.compaction_filter_entries_eligible),
        compaction_filter_entries_dropped: after
            .compaction_filter_entries_dropped
            .saturating_sub(before.compaction_filter_entries_dropped),
        compaction_filter_bytes_dropped: after
            .compaction_filter_bytes_dropped
            .saturating_sub(before.compaction_filter_bytes_dropped),
        compaction_filter_filters_active: after
            .compaction_filter_filters_active
            .saturating_sub(before.compaction_filter_filters_active),
        range_tombstone_active_count: after
            .range_tombstone_active_count
            .saturating_sub(before.range_tombstone_active_count),
        range_tombstone_immutable_count: after
            .range_tombstone_immutable_count
            .saturating_sub(before.range_tombstone_immutable_count),
        range_tombstone_sst_count: after
            .range_tombstone_sst_count
            .saturating_sub(before.range_tombstone_sst_count),
        range_tombstone_total_sst_fragment_count: after
            .range_tombstone_total_sst_fragment_count
            .saturating_sub(before.range_tombstone_total_sst_fragment_count),
        parallel_scan_planned_scans: after
            .parallel_scan_planned_scans
            .saturating_sub(before.parallel_scan_planned_scans),
        parallel_scan_single_shard_fallback_scans: after
            .parallel_scan_single_shard_fallback_scans
            .saturating_sub(before.parallel_scan_single_shard_fallback_scans),
        parallel_scan_total_shards_planned: after
            .parallel_scan_total_shards_planned
            .saturating_sub(before.parallel_scan_total_shards_planned),
        parallel_scan_rows_emitted: after
            .parallel_scan_rows_emitted
            .saturating_sub(before.parallel_scan_rows_emitted),
        parallel_scan_bytes_emitted: after
            .parallel_scan_bytes_emitted
            .saturating_sub(before.parallel_scan_bytes_emitted),
    }
}

fn populate_range(engine: &KvEngine, num_entries: usize, value_size: usize) -> Result<Duration> {
    let value = vec![b'x'; value_size];
    let start = Instant::now();
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), &value)?;
    }
    engine.drain_flush()?;
    Ok(start.elapsed())
}

fn populate_fixed_value(engine: &KvEngine, num_entries: usize, value: &[u8]) -> Result<Duration> {
    let start = Instant::now();
    for i in 0..num_entries {
        engine.put(format!("key{:08}", i).as_bytes(), value)?;
    }
    engine.drain_flush()?;
    Ok(start.elapsed())
}

fn prepare_path(cfg: &HarnessConfig, workload: &str) -> Result<PathBuf> {
    let path = cfg.path_for(workload);
    remove_path(&path)?;
    Ok(path)
}

fn finalize_path(cfg: &HarnessConfig, path: &Path) -> Result<()> {
    if cfg.cleanup {
        remove_path(path)?;
    }
    Ok(())
}

fn remove_path(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn rate(count: u64, duration: Duration) -> f64 {
    count as f64 / duration.as_secs_f64()
}

fn unix_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

fn compaction_name(options: &CompactionOptions) -> &'static str {
    match options {
        CompactionOptions::NoCompaction => "none",
        CompactionOptions::Simple(_) => "simple",
        CompactionOptions::Leveled(_) => "leveled",
        CompactionOptions::Tiered(_) => "tiered",
    }
}

fn diff_option_u64(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    match (before, after) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        (None, Some(after)) => Some(after),
        _ => None,
    }
}

fn diff_option_u32(before: Option<u32>, after: Option<u32>) -> Option<u32> {
    match (before, after) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        (None, Some(after)) => Some(after),
        _ => None,
    }
}

fn validate_config(cfg: &HarnessConfig) -> Result<()> {
    anyhow::ensure!(cfg.num > 0, "--num must be > 0");
    anyhow::ensure!(cfg.reads > 0, "--reads must be > 0");
    anyhow::ensure!(cfg.duration_secs > 0, "--duration must be > 0");
    anyhow::ensure!(cfg.scan_num > 0, "--scan-num must be > 0");
    anyhow::ensure!(cfg.value_size > 0, "--value-size must be > 0");
    anyhow::ensure!(cfg.threads > 0, "--threads must be > 0");
    anyhow::ensure!(cfg.readers > 0, "--readers must be > 0");
    anyhow::ensure!(cfg.seeks > 0, "--seeks must be > 0");
    anyhow::ensure!(cfg.seek_nexts > 0, "--seek-nexts must be > 0");
    anyhow::ensure!(cfg.cache_capacity > 0, "--cache-capacity must be > 0");
    anyhow::ensure!(cfg.target_sst_size > 0, "--target-sst-size must be > 0");
    anyhow::ensure!(cfg.memtable_limit > 0, "--memtable-limit must be > 0");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_large_alias() {
        let args = Args::try_parse_from(["write-perf", "--large"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        assert_eq!(cfg.preset_name, "large");
        assert_eq!(cfg.num, 2_000_000);
    }

    #[test]
    fn parse_subset_aliases() {
        let selected =
            select_workloads(Some("fillseq,readseq_validate_order")).expect("select workloads");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["fillseq", "readseq_validate_order"]);
    }

    #[test]
    fn reject_unsupported_readreverse_selector() {
        let err = select_workloads(Some("readreverse")).expect_err("readreverse should fail");
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn json_record_contains_measurement_name() {
        let cfg = HarnessConfig {
            preset_name: "default",
            run_id: "1".to_string(),
            base_path: PathBuf::from("/tmp/write-perf"),
            cleanup: true,
            output: OutputFormat::Json,
            num: 1,
            reads: 1,
            duration_secs: 1,
            scan_num: 1,
            value_size: 1,
            threads: 1,
            readers: 1,
            seeks: 1,
            seek_nexts: 1,
            seed: 1,
            cache_capacity: 1,
            target_sst_size: 1,
            memtable_limit: 1,
            parallel_scan_max_parallelism: 1,
            parallel_scan_batch_rows: 1,
            parallel_scan_batch_bytes: 1,
            parallel_scan_yield_every_rows: 1,
            parallel_scan_channel_capacity: 1,
            parallel_scan_cache_admission: "bypass".to_string(),
            compaction: CompactionMode::None,
            wal_override: false,
            vlog_override: false,
            num_overridden: false,
            value_size_overridden: false,
            profile: true,
        };
        let options = cfg.build_options(false, false);
        let measurement = make_measurement(
            &cfg,
            "fillseq",
            "write",
            &options,
            MeasurementParams::default(),
            MeasurementResult {
                measure_elapsed_ms: 1.0,
                ..MeasurementResult::default()
            },
            MeasurementCounters::default(),
        );
        let json = serde_json::to_value(&measurement.record).expect("serialize record");
        assert_eq!(json["measurement"], "write");
        assert_eq!(json["schema"], JSON_SCHEMA);
    }

    #[test]
    fn reject_zero_num() {
        let cfg = HarnessConfig {
            preset_name: "default",
            run_id: "1".to_string(),
            base_path: PathBuf::from("/tmp/write-perf"),
            cleanup: true,
            output: OutputFormat::Json,
            num: 0,
            reads: 1,
            duration_secs: 1,
            scan_num: 1,
            value_size: 1,
            threads: 1,
            readers: 1,
            seeks: 1,
            seek_nexts: 1,
            seed: 1,
            cache_capacity: 1,
            target_sst_size: 1,
            memtable_limit: 1,
            parallel_scan_max_parallelism: 1,
            parallel_scan_batch_rows: 1,
            parallel_scan_batch_bytes: 1,
            parallel_scan_yield_every_rows: 1,
            parallel_scan_channel_capacity: 1,
            parallel_scan_cache_admission: "bypass".to_string(),
            compaction: CompactionMode::None,
            wal_override: false,
            vlog_override: false,
            num_overridden: false,
            value_size_overridden: false,
            profile: false,
        };
        assert!(validate_config(&cfg).is_err());
    }
}
