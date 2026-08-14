mod wrapper;

use parking_lot::Mutex;
use std::fmt::Write as _;
use std::io::Write as _;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
    key::{KeySlice, encode_internal_key},
    lsm_storage::{
        CacheAdmission, KvEngine, LsmStorageOptions, ParallelScanOptions, PrefixBloomOptions,
        WriteBatchRecord,
    },
    mem_table::MemTable,
    vlog::ValueSeparationOptions,
};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand_chacha::ChaCha12Rng;
use serde::Serialize;

const JSON_SCHEMA: &str = "kv-engine.write-perf.v1";
const ENGINE_NAME: &str = "kv-engine";
const STEADY_STATE_SEED_VERSION: &str = "splitmix64-v2";
const STEADY_STATE_OPERATION_STREAM_LABEL: u64 = 0x0180_0001;
const STEADY_STATE_KEY_STREAM_LABEL: u64 = 0x0180_0002;
const STEADY_STATE_VALUE_STREAM_LABEL: u64 = 0x0180_0003;
const STEADY_STATE_WARMUP_STREAM_LABEL: u64 = 0x0180_0010;
const STEADY_STATE_MEASUREMENT_STREAM_LABEL: u64 = 0x0180_0011;
const STEADY_STATE_ZIPFIAN_EXPONENT: f64 = 0.99;
const RAND_CHACHA_VERSION: &str = "0.3.1";

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    Text,
    Json,
    Both,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Preset {
    Smoke,
    Default,
    Large,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Suite {
    Legacy,
    SteadyState,
}

fn suite_arg_name(suite: Suite) -> &'static str {
    match suite {
        Suite::Legacy => "legacy",
        Suite::SteadyState => "steady-state",
    }
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
    #[arg(long, value_enum, default_value_t = Suite::Legacy)]
    suite: Suite,
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
    #[arg(long)]
    wal_batch_size: Option<usize>,
    /// Block cache admission policy for parallel scan: force, admit, bypass.
    #[arg(long)]
    parallel_scan_cache_admission: Option<String>,
    #[arg(long, value_enum, default_value_t = CompactionMode::Leveled)]
    compaction: CompactionMode,
    #[arg(long, conflicts_with = "no_wal")]
    wal: bool,
    #[arg(long, conflicts_with = "wal")]
    no_wal: bool,
    #[arg(long)]
    vlog: bool,
    #[arg(long)]
    profile: bool,
    #[arg(long)]
    clients: Option<usize>,
    #[arg(long)]
    warmup_secs: Option<u64>,
    #[arg(long)]
    measurement_secs: Option<u64>,
    #[arg(long)]
    operation_mix: Option<String>,
    #[arg(long)]
    operation_mix_period: Option<usize>,
    #[arg(long)]
    scan_limit: Option<usize>,
    #[arg(long)]
    latency_sample_every: Option<usize>,
    #[arg(long)]
    settle_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug)]
struct HarnessConfig {
    suite: Suite,
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
    wal_batch_size: usize,
    parallel_scan_cache_admission: String,
    compaction: CompactionMode,
    wal_override: Option<bool>,
    vlog_override: bool,
    profile: bool,
    clients: usize,
    warmup_secs: u64,
    measurement_secs: u64,
    operation_mix: Option<String>,
    operation_mix_period: usize,
    scan_limit: usize,
    latency_sample_every: Option<usize>,
    settle_timeout_secs: Option<u64>,
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
        let (
            preset_name,
            num,
            reads,
            duration_secs,
            scan_num,
            value_size,
            clients,
            warmup_secs,
            measurement_secs,
        ) = match (args.suite, preset) {
            (Suite::Legacy, Preset::Smoke) => ("smoke", 10_000, 10_000, 1, 10_000, 1024, 4, 0, 1),
            (Suite::Legacy, Preset::Default) => {
                ("default", 200_000, 100_000, 5, 100_000, 1024, 4, 0, 5)
            }
            (Suite::Legacy, Preset::Large) => {
                ("large", 2_000_000, 500_000, 10, 1_000_000, 1024, 4, 0, 10)
            }
            (Suite::SteadyState, Preset::Smoke) => {
                ("smoke", 10_000, 10_000, 30, 10_000, 400, 4, 0, 30)
            }
            (Suite::SteadyState, Preset::Default) => (
                "default", 1_000_000, 100_000, 180, 1_000_000, 400, 16, 60, 180,
            ),
            (Suite::SteadyState, Preset::Large) => (
                "large", 2_000_000, 500_000, 900, 2_000_000, 400, 64, 300, 900,
            ),
        };
        let run_ms = unix_epoch_ms();
        let wal_override = match (args.wal, args.no_wal) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            _ => None,
        };

        Self {
            suite: args.suite,
            preset_name,
            run_id: format!("{run_ms}"),
            base_path: args.path,
            cleanup: !args.no_cleanup,
            output: args.output,
            num: args.num.unwrap_or(num),
            reads: args.reads.unwrap_or(reads),
            duration_secs: args.duration.unwrap_or(duration_secs),
            scan_num: args.scan_num.unwrap_or(scan_num),
            value_size: args.value_size.unwrap_or(value_size),
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
            wal_batch_size: args.wal_batch_size.unwrap_or(1000),
            parallel_scan_cache_admission: args
                .parallel_scan_cache_admission
                .unwrap_or_else(|| "bypass".to_string()),
            compaction: args.compaction,
            wal_override,
            vlog_override: args.vlog,
            profile: args.profile,
            clients: args.clients.unwrap_or(clients),
            warmup_secs: args.warmup_secs.unwrap_or(warmup_secs),
            measurement_secs: args.measurement_secs.unwrap_or(measurement_secs),
            operation_mix: args.operation_mix,
            operation_mix_period: args.operation_mix_period.unwrap_or(1000),
            scan_limit: args.scan_limit.unwrap_or(10),
            latency_sample_every: args.latency_sample_every,
            settle_timeout_secs: args.settle_timeout_secs,
            num_overridden: args.num.is_some(),
            value_size_overridden: args.value_size.is_some(),
        }
    }

    fn path_for(&self, workload: &str) -> PathBuf {
        self.base_path.join(workload)
    }

    fn build_options(&self, wal: bool, vlog: bool) -> LsmStorageOptions {
        let enable_wal = if wal {
            true
        } else {
            self.wal_override
                .unwrap_or(matches!(self.suite, Suite::SteadyState))
        };
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
    suite: Suite,
    requires_wal: bool,
    run: WorkloadFn,
}

const WORKLOADS: &[WorkloadSpec] = &[
    WorkloadSpec {
        name: "scan",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_scan,
    },
    WorkloadSpec {
        name: "parallel_scan",
        aliases: &["pscan"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_parallel_scan,
    },
    WorkloadSpec {
        name: "concurrent_rw_no_wal",
        aliases: &["rw_no_wal"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_concurrent_rw_no_wal,
    },
    WorkloadSpec {
        name: "concurrent_rw_wal",
        aliases: &["rw_wal"],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_concurrent_rw_wal,
    },
    WorkloadSpec {
        name: "wal_throughput",
        aliases: &["wal"],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_wal_throughput,
    },
    WorkloadSpec {
        name: "wal_concurrent",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_wal_concurrent,
    },
    WorkloadSpec {
        name: "wal_batch_concurrent",
        aliases: &["wal_batch"],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_wal_batch_concurrent,
    },
    WorkloadSpec {
        name: "wal_batch_delete_concurrent",
        aliases: &["wal_batch_delete"],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_wal_batch_delete_concurrent,
    },
    WorkloadSpec {
        name: "memtable_publish_concurrent",
        aliases: &["memtable_publish"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_memtable_publish_concurrent,
    },
    WorkloadSpec {
        name: "memtable_publish_delete_concurrent",
        aliases: &["memtable_publish_delete"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_memtable_publish_delete_concurrent,
    },
    WorkloadSpec {
        name: "crud_phase_batch_writes",
        aliases: &["crud_phase_batch"],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_crud_phase_batch_writes,
    },
    WorkloadSpec {
        name: "crud_bench_batch_create_100",
        aliases: &["crud_batch_create_100"],
        suite: Suite::Legacy,
        requires_wal: true,
        run: run_crud_bench_batch_create_100,
    },
    WorkloadSpec {
        name: "vlog_gc",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_vlog_gc,
    },
    WorkloadSpec {
        name: "vlog_concurrent_gc",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_vlog_concurrent_gc,
    },
    WorkloadSpec {
        name: "fillseq",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_fillseq,
    },
    WorkloadSpec {
        name: "fillrandom",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_fillrandom,
    },
    WorkloadSpec {
        name: "readrandom",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_readrandom,
    },
    WorkloadSpec {
        name: "readwhilewriting",
        aliases: &["rww"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_readwhilewriting,
    },
    WorkloadSpec {
        name: "readrandomwriterandom",
        aliases: &["rwrw"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_readrandomwriterandom,
    },
    WorkloadSpec {
        name: "seekrandom",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_seekrandom,
    },
    WorkloadSpec {
        name: "overwrite",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_overwrite,
    },
    WorkloadSpec {
        name: "readseq",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_readseq,
    },
    WorkloadSpec {
        name: "readseq_validate_order",
        aliases: &["readreverse"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_readseq_validate_order,
    },
    WorkloadSpec {
        name: "readmissing",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_readmissing,
    },
    WorkloadSpec {
        name: "point_read_missing_in_range",
        aliases: &["readmissing_in_range"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_point_read_missing_in_range,
    },
    WorkloadSpec {
        name: "point_read_uniform",
        aliases: &["readuniform"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_point_read_uniform,
    },
    WorkloadSpec {
        name: "point_read_zipfian",
        aliases: &["readzipfian"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_point_read_zipfian,
    },
    WorkloadSpec {
        name: "range_scan_uniform",
        aliases: &["scanuniform"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_range_scan_uniform,
    },
    WorkloadSpec {
        name: "balanced_zipfian",
        aliases: &["balanced"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_balanced_zipfian,
    },
    WorkloadSpec {
        name: "seekrandomwhilewriting",
        aliases: &["seekww"],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_seekrandomwhilewriting,
    },
    WorkloadSpec {
        name: "deleterandom",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
        run: run_deleterandom,
    },
    WorkloadSpec {
        name: "compact",
        aliases: &[],
        suite: Suite::Legacy,
        requires_wal: false,
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
    suite: Suite,
    workload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'static str>,
    measurement: String,
    preset: &'static str,
    engine: &'static str,
    engine_options: EngineOptionsRecord,
    params: MeasurementParams,
    #[serde(skip_serializing_if = "Option::is_none")]
    task: Option<SteadyStateTaskRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency: Option<LatencyRecord>,
    result: MeasurementResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    validation: Option<ValidationRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain: Option<DrainRecord>,
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
    batch_size: Option<usize>,
    duration_secs: Option<u64>,
    threads: Option<usize>,
    readers: Option<usize>,
    clients: Option<usize>,
    seeks: Option<usize>,
    seek_nexts: Option<usize>,
    seed: Option<u64>,
    rng_algorithm: Option<&'static str>,
    seed_derivation: Option<&'static str>,
    key_selection: Option<&'static str>,
    warmup_secs: Option<u64>,
    measurement_secs: Option<u64>,
    operation_mix: Option<String>,
    operation_mix_period: Option<usize>,
    scan_limit: Option<usize>,
    latency_sample_every: Option<usize>,
    settle_timeout_secs: Option<u64>,
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

#[derive(Clone, Serialize)]
struct SteadyStateTaskRecord {
    clients: usize,
    warmup_secs: u64,
    measurement_secs: u64,
    operation_mix: String,
    operation_mix_period: usize,
    operation_mix_scheduler: &'static str,
    key_selection: &'static str,
    scan_limit: usize,
    seed: u64,
    rng_algorithm: &'static str,
    rng_crate_version: &'static str,
    seed_derivation: &'static str,
    scramble_function: &'static str,
    zipfian_exponent: Option<f64>,
    key_format: &'static str,
}

#[derive(Clone, Serialize)]
struct LatencyRecord {
    sample_every: usize,
    samples: u64,
    unsampled_completed_operations: u64,
    avg_ms: Option<f64>,
    min_ms: Option<f64>,
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
    max_ms: Option<f64>,
}

#[derive(Clone, Serialize)]
struct ValidationRecord {
    errors: u64,
    read_hits: u64,
    read_misses: u64,
    expected_read_hits: Option<u64>,
    expected_read_misses: Option<u64>,
    observed_operation_mix: String,
    scan_count_errors: u64,
    scan_order_errors: u64,
    scan_key_errors: u64,
    transaction_attempts: u64,
    transaction_commits: u64,
    transaction_conflicts: u64,
    selected_operations: u64,
    completed_operations: u64,
    min_completed_operations: u64,
    complete_period_operations: u64,
    tail_operations: u64,
    tail_gets: u64,
    tail_puts: u64,
}

#[derive(Clone, Serialize)]
struct DrainRecord {
    flush_drain_ms: f64,
    background_drain_ms: Option<f64>,
    background_drain_status: &'static str,
}

fn print_write_profile(engine: &KvEngine, label: &str) {
    let p = engine.write_profile();
    print_write_profile_snapshot(&p, label);
}

fn print_write_profile_snapshot(
    p: &kv_engine_wrapper::mem_table::WriteProfileSnapshot,
    label: &str,
) {
    if p.op_count == 0 {
        return;
    }
    let total = p.total_ms();
    eprintln!(
        "\n--- write profile: {label} ({} ops) ---\n  \
         batch_build:  {:>8.2} ms\n  \
         mvcc_wal_only:{:>8.2} ms\n  \
         wal_write:    {:>8.2} ms  ({:>5.1}%)\n  \
         wal_validate: {:>8.2} ms\n  \
         wal_prepare:  {:>8.2} ms\n  \
         wal_encode:   {:>8.2} ms\n  \
         encode_parts: entries={:>7.2} ms  crc_header={:>7.2} ms  finish={:>7.2} ms\n  \
         wal_enqueue:  {:>8.2} ms\n  \
         wal_sync:     {:>8.2} ms  ({:>5.1}%)\n  \
         wal_submit:   {:>8.2} ms\n  \
         fdatasync:    {:>8.2} ms\n  \
         follower_wait:{:>8.2} ms\n  \
         follower_events: calls={:>7}  parks={:>7}  retries={:>7}\n  \
         memtable:     {:>8.2} ms  ({:>5.1}%)\n  \
         publish_parts: ttl={:>7.2} ms  decode={:>7.2} ms  bloom={:>7.2} ms  map={:>7.2} ms\n  \
         publish_map:   copy={:>7.2} ms  skipmap={:>7.2} ms  accounting={:>7.2} ms\n  \
         commit_groups: {:>7}  solo={:>7} ({:>5.1}%)  avg_bufs={:>5.2}  max_bufs={:>3}\n  \
         commit_bytes:  avg={:>8.0} B  max={:>8} B\n  \
        total:        {:>8.2} ms",
        p.op_count,
        p.batch_build_ms(),
        p.mvcc_wal_only_ms(),
        p.wal_write_ms(),
        if total > 0.0 {
            p.wal_write_ms() / total * 100.0
        } else {
            0.0
        },
        p.wal_validate_ms(),
        p.wal_prepare_ms(),
        p.wal_encode_ms(),
        p.wal_encode_entries_ms(),
        p.wal_encode_crc_header_ms(),
        p.wal_encode_finish_ms(),
        p.wal_enqueue_ms(),
        p.wal_sync_ms(),
        p.wal_sync_pct(),
        p.wal_submit_ms(),
        p.wal_fdatasync_ms(),
        p.wal_follower_wait_ms(),
        p.wal_follower_wait_calls,
        p.wal_follower_condvar_waits,
        p.wal_follower_retry_loops,
        p.memtable_insert_ms(),
        if total > 0.0 {
            p.memtable_insert_ms() / total * 100.0
        } else {
            0.0
        },
        p.memtable_publish_ttl_check_ms(),
        p.memtable_publish_decode_ms(),
        p.memtable_publish_bloom_ms(),
        p.memtable_publish_map_ms(),
        p.memtable_publish_copy_ms(),
        p.memtable_publish_skipmap_ms(),
        p.memtable_publish_accounting_ms(),
        p.wal_commit_groups,
        p.wal_commit_solo_groups,
        p.wal_commit_solo_pct(),
        p.wal_commit_avg_buffers(),
        p.wal_commit_max_buffers,
        p.wal_commit_avg_bytes(),
        p.wal_commit_max_bytes,
        total,
    );
}

fn start_hotpath_profile(enabled: bool) -> Option<kv_engine::profiling::HotpathGuard> {
    if !enabled {
        return None;
    }

    let guard = kv_engine::profiling::start_hotpath_profile("write-perf");
    if guard.is_some() {
        eprintln!(
            "hotpath-profile enabled; set HOTPATH_TIME_SAMPLING_RATE=0.1 to reduce timing overhead and HOTPATH_METRICS_SERVER_OFF=true in restricted environments"
        );
    }

    guard
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bench_arg = args.bench.clone();
    let cfg = HarnessConfig::from_args(args);
    validate_config(&cfg)?;
    let _hotpath_guard = start_hotpath_profile(cfg.profile);
    let workloads = select_workloads(bench_arg.as_deref(), &cfg)?;

    let mut all_measurements = Vec::new();
    for workload in workloads {
        all_measurements.extend((workload.run)(&cfg)?);
    }

    emit_measurements(&cfg, &all_measurements)
}

fn select_workloads(
    filter: Option<&str>,
    cfg: &HarnessConfig,
) -> Result<Vec<&'static WorkloadSpec>> {
    match filter {
        None => {
            let skip_wal_rows = cfg.wal_override == Some(false);
            let selected: Vec<_> = WORKLOADS
                .iter()
                .filter(|workload| workload.suite == cfg.suite)
                .filter(|workload| !(skip_wal_rows && workload.requires_wal))
                .collect();
            anyhow::ensure!(
                !selected.is_empty(),
                "no workloads remain for the selected suite and WAL setting"
            );
            Ok(selected)
        }
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
                anyhow::ensure!(
                    workload.suite == cfg.suite,
                    "workload `{}` belongs to the {} suite; rerun with --suite {}",
                    workload.name,
                    suite_arg_name(workload.suite),
                    suite_arg_name(workload.suite)
                );
                anyhow::ensure!(
                    !(workload.requires_wal && cfg.wal_override == Some(false)),
                    "workload `{}` requires WAL and cannot be used with --no-wal",
                    workload.name
                );
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

fn run_wal_batch_concurrent(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "wal_batch_concurrent";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(true, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let num_keys = cfg.num;
    let writer_threads = cfg.threads;
    let baseline = collect_counters(&engine)?;
    let per_thread = num_keys / writer_threads;
    let remainder = num_keys % writer_threads;
    let batch_size = effective_wal_batch_size(num_keys, writer_threads, cfg.wal_batch_size);
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..writer_threads {
        let eng = engine.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || {
            let thread_ops = per_thread + usize::from(t < remainder);
            let start_idx = t * per_thread + remainder.min(t);
            let mut next = 0usize;
            while next < thread_ops {
                let current_batch = (thread_ops - next).min(batch_size);
                let mut keys = Vec::with_capacity(current_batch);
                for i in 0..current_batch {
                    keys.push(format!("key{:08}", start_idx + next + i).into_bytes());
                }
                let batch: Vec<_> = keys
                    .iter()
                    .map(|key| WriteBatchRecord::Put(key.as_slice(), val.as_slice()))
                    .collect();
                eng.write_batch(&batch).expect("write_batch failed");
                next += current_batch;
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
        format!("concurrent_batch_{batch_size}"),
        &options,
        MeasurementParams {
            num: Some(num_keys),
            value_size: Some(cfg.value_size),
            batch_size: Some(batch_size),
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

fn run_wal_batch_delete_concurrent(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "wal_batch_delete_concurrent";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(true, false);
    let engine = KvEngine::open(&path, options.clone())?;
    populate_fixed_value(&engine, cfg.num, &vec![b'x'; cfg.value_size])?;
    #[cfg(feature = "bench")]
    if cfg.profile {
        engine.reset_write_profile();
    }

    let num_keys = cfg.num;
    let writer_threads = cfg.threads;
    let baseline = collect_counters(&engine)?;
    let per_thread = num_keys / writer_threads;
    let remainder = num_keys % writer_threads;
    let batch_size = effective_wal_batch_size(num_keys, writer_threads, cfg.wal_batch_size);
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..writer_threads {
        let eng = engine.clone();
        handles.push(std::thread::spawn(move || {
            let thread_ops = per_thread + usize::from(t < remainder);
            let start_idx = t * per_thread + remainder.min(t);
            let mut next = 0usize;
            while next < thread_ops {
                let current_batch = (thread_ops - next).min(batch_size);
                let mut keys = Vec::with_capacity(current_batch);
                for i in 0..current_batch {
                    keys.push(format!("key{:08}", start_idx + next + i).into_bytes());
                }
                let batch: Vec<_> = keys
                    .iter()
                    .map(|key| WriteBatchRecord::Del(key.as_slice()))
                    .collect();
                eng.write_batch(&batch).expect("write_batch delete failed");
                next += current_batch;
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
        format!("concurrent_delete_batch_{batch_size}"),
        &options,
        MeasurementParams {
            num: Some(num_keys),
            value_size: Some(cfg.value_size),
            batch_size: Some(batch_size),
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

fn run_memtable_publish_concurrent(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_memtable_publish_workload(cfg, "memtable_publish_concurrent", false)
}

fn run_memtable_publish_delete_concurrent(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_memtable_publish_workload(cfg, "memtable_publish_delete_concurrent", true)
}

fn run_memtable_publish_workload(
    cfg: &HarnessConfig,
    workload: &'static str,
    delete_values: bool,
) -> Result<Vec<BenchMeasurement>> {
    let options = cfg.build_options(false, false);
    let memtable = Arc::new(MemTable::create(0, false));
    #[cfg(feature = "bench")]
    {
        memtable.set_write_profile(Arc::new(
            kv_engine_wrapper::mem_table::WriteProfile::default(),
        ));
    }

    let num_keys = cfg.num;
    let writer_threads = cfg.threads;
    let per_thread = num_keys / writer_threads;
    let remainder = num_keys % writer_threads;
    let batch_size = effective_wal_batch_size(num_keys, writer_threads, cfg.wal_batch_size);
    let value = if delete_values {
        vec![kv_engine_wrapper::vlog::KvKind::Tombstone as u8]
    } else {
        let mut value = Vec::with_capacity(1 + cfg.value_size);
        value.push(kv_engine_wrapper::vlog::KvKind::Inline as u8);
        value.extend(std::iter::repeat_n(b'x', cfg.value_size));
        value
    };

    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..writer_threads {
        let mt = memtable.clone();
        let val = value.clone();
        handles.push(std::thread::spawn(move || -> Result<()> {
            let thread_ops = per_thread + usize::from(t < remainder);
            let start_idx = t * per_thread + remainder.min(t);
            let mut next = 0usize;
            while next < thread_ops {
                let current_batch = (thread_ops - next).min(batch_size);
                let mut keys = Vec::with_capacity(current_batch);
                for i in 0..current_batch {
                    let user_key = format!("key{:08}", start_idx + next + i);
                    keys.push(encode_internal_key(user_key.as_bytes(), 1));
                }
                let batch: Vec<_> = keys
                    .iter()
                    .map(|key| (KeySlice::from_slice(key.as_slice()), val.as_slice()))
                    .collect();
                mt.put_raw_batch_no_wal(&batch)
                    .expect("memtable publish failed");
                next += current_batch;
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("writer thread panicked"))??;
    }
    let elapsed = start.elapsed();
    if cfg.profile {
        let snapshot = memtable.write_profile().snapshot();
        print_write_profile_snapshot(&snapshot, workload);
    }

    Ok(vec![make_measurement(
        cfg,
        workload,
        if delete_values {
            format!("concurrent_delete_batch_{batch_size}")
        } else {
            format!("concurrent_batch_{batch_size}")
        },
        &options,
        MeasurementParams {
            num: Some(num_keys),
            value_size: Some(cfg.value_size),
            batch_size: Some(batch_size),
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
        MeasurementCounters::default(),
    )])
}

fn effective_wal_batch_size(num_keys: usize, writer_threads: usize, requested: usize) -> usize {
    let per_thread = num_keys / writer_threads;
    let remainder = num_keys % writer_threads;
    let max_thread_ops = per_thread + usize::from(remainder > 0);
    requested.min(max_thread_ops.max(1))
}

#[derive(Clone, Copy)]
enum BatchWriteOp {
    Put,
    Delete,
}

#[derive(Clone, Copy)]
struct BatchWritePhase {
    op: BatchWriteOp,
    start_key: usize,
}

fn build_crud_bench_toykv_options(cfg: &HarnessConfig) -> LsmStorageOptions {
    let mut options = cfg.build_options(true, true);
    options.block_size = 64 * 1024;
    options.target_sst_size = 256 << 20;
    options.compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level0_file_num_compaction_trigger: 2,
        max_levels: 4,
        base_level_size_mb: 128,
        level_size_multiplier: 2,
    });
    if let Some(vlog) = options.value_separation.as_mut() {
        vlog.min_value_size = 4 * 1024;
    }
    options
}

fn run_crud_phase_batch_writes(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "crud_phase_batch_writes";
    let path = prepare_path(cfg, workload)?;
    let options = build_crud_bench_toykv_options(cfg);

    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let batch_size = effective_wal_batch_size(cfg.num, cfg.threads, cfg.wal_batch_size);

    for i in 0..cfg.num {
        engine.put(format!("key{i:08}").as_bytes(), &value)?;
    }
    for i in 0..cfg.num {
        engine.put(format!("key{i:08}").as_bytes(), &value)?;
    }
    for i in 0..cfg.num {
        engine.delete(format!("key{i:08}").as_bytes())?;
    }

    let measurements = vec![
        run_crud_phase_batch_write_measurement(CrudPhaseMeasurement {
            cfg,
            workload,
            measurement: "batch_create_after_crud_phase",
            engine: &engine,
            options: &options,
            value: &value,
            batch_size,
            phase: BatchWritePhase {
                op: BatchWriteOp::Put,
                start_key: cfg.num,
            },
        })?,
        run_crud_phase_batch_write_measurement(CrudPhaseMeasurement {
            cfg,
            workload,
            measurement: "batch_update_after_crud_phase",
            engine: &engine,
            options: &options,
            value: &value,
            batch_size,
            phase: BatchWritePhase {
                op: BatchWriteOp::Put,
                start_key: cfg.num,
            },
        })?,
        run_crud_phase_batch_write_measurement(CrudPhaseMeasurement {
            cfg,
            workload,
            measurement: "batch_delete_after_crud_phase",
            engine: &engine,
            options: &options,
            value: &value,
            batch_size,
            phase: BatchWritePhase {
                op: BatchWriteOp::Delete,
                start_key: cfg.num,
            },
        })?,
    ];

    engine.drain_flush()?;
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(measurements)
}

fn run_crud_bench_batch_create_100(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    const CRUD_BENCH_BATCH_SIZE: usize = 100;
    const CRUD_BENCH_BATCH_ITERATIONS: usize = 250;

    let workload = "crud_bench_batch_create_100";
    let path = prepare_path(cfg, workload)?;
    let options = build_crud_bench_toykv_options(cfg);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = crud_bench_like_payload(cfg.value_size);

    for i in 0..cfg.num {
        let key = ordered_integer_key_bytes(i);
        engine.put(&key, &value)?;
    }
    for i in 0..cfg.num {
        let key = ordered_integer_key_bytes(i);
        engine.put(&key, &value)?;
    }
    for i in 0..cfg.num {
        let key = ordered_integer_key_bytes(i);
        engine.delete(&key)?;
    }

    #[cfg(feature = "bench")]
    if cfg.profile {
        engine.reset_write_profile();
    }
    let baseline = collect_counters(&engine)?;
    let elapsed = run_crud_bench_batch_create_iterations(
        &engine,
        CRUD_BENCH_BATCH_ITERATIONS,
        cfg.threads,
        CRUD_BENCH_BATCH_SIZE,
        cfg.num,
        &value,
    )?;
    if cfg.profile {
        print_write_profile(&engine, "crud_bench_batch_create_100");
    }
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);

    engine.drain_flush()?;
    engine.close()?;
    finalize_path(cfg, &path)?;

    Ok(vec![make_measurement(
        cfg,
        workload,
        "batch_create_100",
        &options,
        MeasurementParams {
            num: Some(CRUD_BENCH_BATCH_ITERATIONS),
            value_size: Some(cfg.value_size),
            batch_size: Some(CRUD_BENCH_BATCH_SIZE),
            threads: Some(cfg.threads),
            seed: Some(cfg.seed),
            ..MeasurementParams::default()
        },
        MeasurementResult {
            measure_elapsed_ms: ms(elapsed),
            ops: Some((CRUD_BENCH_BATCH_ITERATIONS * CRUD_BENCH_BATCH_SIZE) as u64),
            ops_per_sec: Some(rate(
                (CRUD_BENCH_BATCH_ITERATIONS * CRUD_BENCH_BATCH_SIZE) as u64,
                elapsed,
            )),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn crud_bench_like_payload(value_size: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(value_size.max(1));
    value.push(6);
    value.extend(std::iter::repeat_n(b'x', value_size.saturating_sub(1)));
    value
}

fn ordered_integer_key_bytes(sample_idx: usize) -> [u8; 4] {
    // Matches the external crud-bench ToyKV adapter byte-for-byte; this is not a sortable key
    // encoding.
    (sample_idx as u32 + 1).to_ne_bytes()
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn steady_state_stream_seed(base_seed: u64, label: u64, client_id: u64) -> u64 {
    splitmix64(splitmix64(splitmix64(base_seed) ^ label) ^ client_id)
}

fn steady_state_loaded_key(id: u64) -> [u8; 20] {
    let mut key = [b'0'; 20];
    key[..8].copy_from_slice(&id.to_be_bytes());
    key
}

fn steady_state_missing_key(id: u64) -> [u8; 20] {
    let mut key = steady_state_loaded_key(id);
    key[19] = b'1';
    key
}

#[derive(Clone, Copy)]
enum SteadyStateOperation {
    Get,
    Put,
}

#[derive(Clone, Copy)]
enum SteadyStateWindowPhase {
    Warmup,
    Measurement,
}

impl SteadyStateWindowPhase {
    fn stream_label(self) -> u64 {
        match self {
            Self::Warmup => STEADY_STATE_WARMUP_STREAM_LABEL,
            Self::Measurement => STEADY_STATE_MEASUREMENT_STREAM_LABEL,
        }
    }
}

#[derive(Debug)]
struct SteadyStateOperationMix {
    period: usize,
    get_slots: usize,
    put_slots: usize,
}

impl SteadyStateOperationMix {
    fn parse(spec: &str, period: usize) -> Result<Self> {
        validate_operation_mix(spec, period)?;
        let mut get_slots = None;
        let mut put_slots = None;
        for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (name, value) = entry
                .split_once('=')
                .with_context(|| format!("invalid --operation-mix entry: {entry}"))?;
            let name = name.trim();
            let ratio: f64 = value
                .trim()
                .parse()
                .with_context(|| format!("invalid operation ratio for {name}"))?;
            let slots = (ratio * period as f64).round() as usize;
            match name {
                "get" => get_slots = Some(slots),
                "put" => put_slots = Some(slots),
                other => bail!(
                    "balanced_zipfian supports only get and put in --operation-mix, got {other}"
                ),
            }
        }

        let get_slots = get_slots.unwrap_or(0);
        let put_slots = put_slots.unwrap_or(0);
        anyhow::ensure!(
            get_slots + put_slots == period,
            "--operation-mix slots must fill --operation-mix-period"
        );
        anyhow::ensure!(
            get_slots > 0 && put_slots > 0,
            "balanced_zipfian requires non-zero get and put ratios"
        );

        Ok(Self {
            period,
            get_slots,
            put_slots,
        })
    }

    fn shuffled_schedule(&self, seed: u64, client_id: usize) -> Vec<SteadyStateOperation> {
        let mut schedule = Vec::with_capacity(self.period);
        schedule.extend(std::iter::repeat_n(
            SteadyStateOperation::Get,
            self.get_slots,
        ));
        schedule.extend(std::iter::repeat_n(
            SteadyStateOperation::Put,
            self.put_slots,
        ));
        let mut rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
            seed,
            STEADY_STATE_OPERATION_STREAM_LABEL,
            client_id as u64,
        ));
        schedule.shuffle(&mut rng);
        schedule
    }

    fn validate_complete_periods(&self, selected_gets: u64, selected_puts: u64) -> Result<()> {
        let complete_periods = (selected_gets + selected_puts) / self.period as u64;
        let expected_gets = complete_periods * self.get_slots as u64;
        let expected_puts = complete_periods * self.put_slots as u64;
        anyhow::ensure!(
            selected_gets == expected_gets,
            "selected get operations violate complete-period mix: expected {}, got {}",
            expected_gets,
            selected_gets
        );
        anyhow::ensure!(
            selected_puts == expected_puts,
            "selected put operations violate complete-period mix: expected {}, got {}",
            expected_puts,
            selected_puts
        );
        Ok(())
    }
}

struct ZipfianSampler {
    cdf: Vec<f64>,
}

impl ZipfianSampler {
    fn new(record_count: usize, exponent: f64) -> Result<Self> {
        anyhow::ensure!(record_count > 0, "zipfian sampler requires records");
        anyhow::ensure!(
            exponent.is_finite() && exponent > 0.0,
            "zipfian exponent must be finite and positive"
        );
        let mut cdf = Vec::with_capacity(record_count);
        let mut total = 0.0;
        for rank in 1..=record_count {
            total += 1.0 / (rank as f64).powf(exponent);
            cdf.push(total);
        }
        for cumulative in &mut cdf {
            *cumulative /= total;
        }
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }

        Ok(Self { cdf })
    }

    fn sample(&self, rng: &mut ChaCha12Rng) -> u64 {
        let needle = rng.gen_range(0.0..1.0);
        let rank = match self.cdf.binary_search_by(|probe| probe.total_cmp(&needle)) {
            Ok(idx) | Err(idx) => idx,
        } as u64;
        splitmix64(rank) % self.cdf.len() as u64
    }
}

#[derive(Clone)]
enum SteadyStateKeySelector {
    Uniform { record_count: u64 },
    ScrambledZipfian { sampler: Arc<ZipfianSampler> },
}

impl SteadyStateKeySelector {
    fn uniform(record_count: usize) -> Self {
        Self::Uniform {
            record_count: record_count as u64,
        }
    }

    fn scrambled_zipfian(record_count: usize) -> Result<Self> {
        Ok(Self::ScrambledZipfian {
            sampler: Arc::new(ZipfianSampler::new(
                record_count,
                STEADY_STATE_ZIPFIAN_EXPONENT,
            )?),
        })
    }

    fn sample(&self, rng: &mut ChaCha12Rng) -> u64 {
        match self {
            Self::Uniform { record_count } => rng.gen_range(0..*record_count),
            Self::ScrambledZipfian { sampler } => sampler.sample(rng),
        }
    }
}

#[derive(Default)]
struct SteadyStateWindowStats {
    reads: u64,
    writes: u64,
    scans: u64,
    scan_rows: u64,
    scan_count_errors: u64,
    scan_order_errors: u64,
    scan_key_errors: u64,
    read_hits: u64,
    read_misses: u64,
    selected_gets: u64,
    selected_puts: u64,
    selected_operations: u64,
    completed_operations: u64,
    complete_period_gets: u64,
    complete_period_puts: u64,
    complete_period_operations: u64,
    tail_gets: u64,
    tail_puts: u64,
    tail_operations: u64,
    latency_samples_ns: Vec<u64>,
}

impl SteadyStateWindowStats {
    fn merge(&mut self, other: Self) {
        self.reads += other.reads;
        self.writes += other.writes;
        self.scans += other.scans;
        self.scan_rows += other.scan_rows;
        self.scan_count_errors += other.scan_count_errors;
        self.scan_order_errors += other.scan_order_errors;
        self.scan_key_errors += other.scan_key_errors;
        self.read_hits += other.read_hits;
        self.read_misses += other.read_misses;
        self.selected_gets += other.selected_gets;
        self.selected_puts += other.selected_puts;
        self.selected_operations += other.selected_operations;
        self.completed_operations += other.completed_operations;
        self.complete_period_gets += other.complete_period_gets;
        self.complete_period_puts += other.complete_period_puts;
        self.complete_period_operations += other.complete_period_operations;
        self.tail_gets += other.tail_gets;
        self.tail_puts += other.tail_puts;
        self.tail_operations += other.tail_operations;
        self.latency_samples_ns.extend(other.latency_samples_ns);
    }
}

fn run_steady_state_read_window(
    engine: &Arc<KvEngine>,
    cfg: &HarnessConfig,
    selector: SteadyStateKeySelector,
    duration: Duration,
    phase: SteadyStateWindowPhase,
    sample_latency: bool,
) -> Result<SteadyStateWindowStats> {
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let selector = selector.clone();
        let latency_sample_every = cfg.latency_sample_every.filter(|_| sample_latency);
        let seed = steady_state_stream_seed(cfg.seed, phase.stream_label(), client_id as u64);
        handles.push(std::thread::spawn(
            move || -> Result<SteadyStateWindowStats> {
                let mut key_rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
                    seed,
                    STEADY_STATE_KEY_STREAM_LABEL,
                    0,
                ));
                let mut stats = SteadyStateWindowStats::default();
                while !stop.load(Ordering::Relaxed) {
                    let key = steady_state_loaded_key(selector.sample(&mut key_rng));
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats.selected_operations % sample_every as u64 == 0
                    });
                    let op_start = should_sample.then(Instant::now);
                    match eng.get(&key)? {
                        Some(_) => stats.read_hits += 1,
                        None => stats.read_misses += 1,
                    }
                    stats.reads += 1;
                    stats.selected_gets += 1;
                    stats.selected_operations += 1;
                    stats.completed_operations += 1;
                    if let Some(op_start) = op_start {
                        stats
                            .latency_samples_ns
                            .push(op_start.elapsed().as_nanos() as u64);
                    }
                }
                stats.complete_period_gets = stats.selected_gets;
                stats.complete_period_puts = 0;
                stats.complete_period_operations = stats.selected_operations;
                Ok(stats)
            },
        ));
    }

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let mut stats = SteadyStateWindowStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| anyhow!("steady-state read worker thread panicked"))??;
        stats.merge(worker);
    }
    Ok(stats)
}

fn run_steady_state_scan_window(
    engine: &Arc<KvEngine>,
    cfg: &HarnessConfig,
    duration: Duration,
    phase: SteadyStateWindowPhase,
    sample_latency: bool,
) -> Result<SteadyStateWindowStats> {
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let record_count = cfg.num as u64;
        let scan_limit = cfg.scan_limit;
        let latency_sample_every = cfg.latency_sample_every.filter(|_| sample_latency);
        let seed = steady_state_stream_seed(cfg.seed, phase.stream_label(), client_id as u64);
        handles.push(std::thread::spawn(
            move || -> Result<SteadyStateWindowStats> {
                let mut key_rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
                    seed,
                    STEADY_STATE_KEY_STREAM_LABEL,
                    0,
                ));
                let mut stats = SteadyStateWindowStats::default();
                let mut previous_key = Vec::with_capacity(20);
                while !stop.load(Ordering::Relaxed) {
                    let start_id = key_rng.gen_range(0..record_count);
                    let start_key = steady_state_loaded_key(start_id);
                    let expected_rows = scan_limit.min((record_count - start_id) as usize);
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats.selected_operations % sample_every as u64 == 0
                    });
                    let op_start = should_sample.then(Instant::now);

                    let mut rows = 0usize;
                    previous_key.clear();
                    let mut iter = eng.scan(Bound::Included(&start_key), Bound::Unbounded)?;
                    while iter.is_valid() && rows < scan_limit {
                        let key = iter.key();
                        let expected_key = steady_state_loaded_key(start_id + rows as u64);
                        if key != expected_key.as_slice() {
                            stats.scan_key_errors += 1;
                        }
                        if rows > 0 && previous_key.as_slice() >= key {
                            stats.scan_order_errors += 1;
                        }
                        previous_key.clear();
                        previous_key.extend_from_slice(key);
                        rows += 1;
                        iter.next()?;
                    }

                    if rows != expected_rows {
                        stats.scan_count_errors += 1;
                    }
                    stats.scans += 1;
                    stats.scan_rows += rows as u64;
                    stats.reads += 1;
                    stats.selected_operations += 1;
                    stats.completed_operations += 1;
                    if let Some(op_start) = op_start {
                        stats
                            .latency_samples_ns
                            .push(op_start.elapsed().as_nanos() as u64);
                    }
                }
                stats.complete_period_gets = 0;
                stats.complete_period_puts = 0;
                stats.complete_period_operations = stats.selected_operations;
                Ok(stats)
            },
        ));
    }

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let mut stats = SteadyStateWindowStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| anyhow!("steady-state scan worker thread panicked"))??;
        stats.merge(worker);
    }
    Ok(stats)
}

fn run_steady_state_mixed_window(
    engine: &Arc<KvEngine>,
    cfg: &HarnessConfig,
    mix: &SteadyStateOperationMix,
    sampler: Arc<ZipfianSampler>,
    duration: Duration,
    phase: SteadyStateWindowPhase,
    sample_latency: bool,
) -> Result<SteadyStateWindowStats> {
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let sampler = sampler.clone();
        let schedule = mix.shuffled_schedule(cfg.seed, client_id);
        let period = mix.period;
        let get_slots = mix.get_slots;
        let put_slots = mix.put_slots;
        let value_size = cfg.value_size;
        let latency_sample_every = cfg.latency_sample_every.filter(|_| sample_latency);
        let seed = steady_state_stream_seed(cfg.seed, phase.stream_label(), client_id as u64);
        handles.push(std::thread::spawn(
            move || -> Result<SteadyStateWindowStats> {
                let mut key_rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
                    seed,
                    STEADY_STATE_KEY_STREAM_LABEL,
                    0,
                ));
                let mut value_rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
                    seed,
                    STEADY_STATE_VALUE_STREAM_LABEL,
                    0,
                ));
                let mut value = vec![0u8; value_size];
                let mut stats = SteadyStateWindowStats::default();
                let mut schedule_idx = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let operation = schedule[schedule_idx];
                    schedule_idx = (schedule_idx + 1) % schedule.len();
                    let key = steady_state_loaded_key(sampler.sample(&mut key_rng));
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats.selected_operations % sample_every as u64 == 0
                    });
                    let op_start = should_sample.then(Instant::now);
                    match operation {
                        SteadyStateOperation::Get => {
                            stats.selected_gets += 1;
                            match eng.get(&key)? {
                                Some(_) => stats.read_hits += 1,
                                None => stats.read_misses += 1,
                            }
                            stats.reads += 1;
                        }
                        SteadyStateOperation::Put => {
                            stats.selected_puts += 1;
                            value_rng.fill_bytes(&mut value);
                            eng.put(&key, &value)?;
                            stats.writes += 1;
                        }
                    }
                    stats.selected_operations += 1;
                    stats.completed_operations += 1;
                    if let Some(op_start) = op_start {
                        stats
                            .latency_samples_ns
                            .push(op_start.elapsed().as_nanos() as u64);
                    }
                }
                let complete_periods = stats.selected_operations / period as u64;
                let tail_operations = stats.selected_operations % period as u64;
                let tail_gets = schedule
                    .iter()
                    .take(tail_operations as usize)
                    .filter(|operation| matches!(operation, SteadyStateOperation::Get))
                    .count() as u64;
                let tail_puts = tail_operations - tail_gets;
                let expected_gets = complete_periods * get_slots as u64 + tail_gets;
                let expected_puts = complete_periods * put_slots as u64 + tail_puts;
                anyhow::ensure!(
                    stats.selected_gets == expected_gets,
                    "client {} selected {} gets, expected {} from complete periods plus tail",
                    client_id,
                    stats.selected_gets,
                    expected_gets
                );
                anyhow::ensure!(
                    stats.selected_puts == expected_puts,
                    "client {} selected {} puts, expected {} from complete periods plus tail",
                    client_id,
                    stats.selected_puts,
                    expected_puts
                );
                stats.complete_period_gets = complete_periods * get_slots as u64;
                stats.complete_period_puts = complete_periods * put_slots as u64;
                stats.complete_period_operations = complete_periods * period as u64;
                stats.tail_gets = tail_gets;
                stats.tail_puts = tail_puts;
                stats.tail_operations = tail_operations;
                Ok(stats)
            },
        ));
    }

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let mut stats = SteadyStateWindowStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| anyhow!("steady-state worker thread panicked"))??;
        stats.merge(worker);
    }
    Ok(stats)
}

fn latency_record(
    sample_every: usize,
    completed_operations: u64,
    samples_ns: &[u64],
) -> LatencyRecord {
    if samples_ns.is_empty() {
        return LatencyRecord {
            sample_every,
            samples: 0,
            unsampled_completed_operations: completed_operations,
            avg_ms: None,
            min_ms: None,
            p50_ms: None,
            p95_ms: None,
            p99_ms: None,
            max_ms: None,
        };
    }

    let mut sorted = samples_ns.to_vec();
    sorted.sort_unstable();
    let sum: u128 = sorted.iter().map(|sample| *sample as u128).sum();
    LatencyRecord {
        sample_every,
        samples: sorted.len() as u64,
        unsampled_completed_operations: completed_operations.saturating_sub(sorted.len() as u64),
        avg_ms: Some((sum as f64 / sorted.len() as f64) / 1_000_000.0),
        min_ms: sorted.first().map(|sample| *sample as f64 / 1_000_000.0),
        p50_ms: Some(percentile_latency_ms(&sorted, 0.50)),
        p95_ms: Some(percentile_latency_ms(&sorted, 0.95)),
        p99_ms: Some(percentile_latency_ms(&sorted, 0.99)),
        max_ms: sorted.last().map(|sample| *sample as f64 / 1_000_000.0),
    }
}

fn percentile_latency_ms(sorted_samples_ns: &[u64], percentile: f64) -> f64 {
    debug_assert!(!sorted_samples_ns.is_empty());
    let idx = ((sorted_samples_ns.len() - 1) as f64 * percentile).round() as usize;
    sorted_samples_ns[idx] as f64 / 1_000_000.0
}

fn steady_state_task_record(
    cfg: &HarnessConfig,
    operation_mix: impl Into<String>,
    operation_mix_period: usize,
    operation_mix_scheduler: &'static str,
    key_selection: &'static str,
    zipfian_exponent: Option<f64>,
) -> SteadyStateTaskRecord {
    SteadyStateTaskRecord {
        clients: cfg.clients,
        warmup_secs: cfg.warmup_secs,
        measurement_secs: cfg.measurement_secs,
        operation_mix: operation_mix.into(),
        operation_mix_period,
        operation_mix_scheduler,
        key_selection,
        scan_limit: cfg.scan_limit,
        seed: cfg.seed,
        rng_algorithm: "ChaCha12Rng",
        rng_crate_version: RAND_CHACHA_VERSION,
        seed_derivation: STEADY_STATE_SEED_VERSION,
        scramble_function: zipfian_exponent
            .map(|_| "splitmix64(rank) % record_count")
            .unwrap_or("none"),
        zipfian_exponent,
        key_format: "be_u64_plus_ascii_zero_padding_20b",
    }
}

fn steady_state_validation_record(
    stats: &SteadyStateWindowStats,
    observed_operation_mix: String,
) -> ValidationRecord {
    ValidationRecord {
        errors: stats.scan_count_errors + stats.scan_order_errors + stats.scan_key_errors,
        read_hits: stats.read_hits,
        read_misses: stats.read_misses,
        expected_read_hits: Some(stats.reads),
        expected_read_misses: Some(0),
        observed_operation_mix,
        scan_count_errors: stats.scan_count_errors,
        scan_order_errors: stats.scan_order_errors,
        scan_key_errors: stats.scan_key_errors,
        transaction_attempts: 0,
        transaction_commits: 0,
        transaction_conflicts: 0,
        selected_operations: stats.selected_operations,
        completed_operations: stats.completed_operations,
        min_completed_operations: 1,
        complete_period_operations: stats.complete_period_operations,
        tail_operations: stats.tail_operations,
        tail_gets: stats.tail_gets,
        tail_puts: stats.tail_puts,
    }
}

fn steady_state_drain_record(flush_drain: Duration) -> DrainRecord {
    DrainRecord {
        flush_drain_ms: ms(flush_drain),
        background_drain_ms: None,
        background_drain_status: "not_requested",
    }
}

fn validate_range_scan_window(workload: &str, stats: &SteadyStateWindowStats) -> Result<()> {
    anyhow::ensure!(
        stats.scan_count_errors == 0,
        "{workload} saw {} scan count validation errors",
        stats.scan_count_errors
    );
    anyhow::ensure!(
        stats.scan_order_errors == 0,
        "{workload} saw {} scan order validation errors",
        stats.scan_order_errors
    );
    anyhow::ensure!(
        stats.scan_key_errors == 0,
        "{workload} saw {} scan key validation errors",
        stats.scan_key_errors
    );
    Ok(())
}

fn run_crud_bench_batch_create_iterations(
    engine: &Arc<KvEngine>,
    iterations: usize,
    writer_threads: usize,
    batch_size: usize,
    start_key: usize,
    value: &[u8],
) -> Result<Duration> {
    let next_iteration = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let mut handles = vec![];
    for _ in 0..writer_threads {
        let eng = engine.clone();
        let val = value.to_vec();
        let next = next_iteration.clone();
        handles.push(std::thread::spawn(move || {
            loop {
                let iteration = next.fetch_add(1, Ordering::Relaxed);
                if iteration >= iterations {
                    break;
                }
                let mut keys = Vec::with_capacity(batch_size);
                for offset in 0..batch_size {
                    keys.push(ordered_integer_key_bytes(
                        start_key + iteration * batch_size + offset,
                    ));
                }
                let batch: Vec<_> = keys
                    .iter()
                    .map(|key| WriteBatchRecord::Put(key.as_slice(), val.as_slice()))
                    .collect();
                eng.write_batch(&batch).expect("write_batch failed");
            }
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("writer thread panicked"))?;
    }

    Ok(start.elapsed())
}

struct CrudPhaseMeasurement<'a> {
    cfg: &'a HarnessConfig,
    workload: &'a str,
    measurement: &'a str,
    engine: &'a Arc<KvEngine>,
    options: &'a LsmStorageOptions,
    value: &'a [u8],
    batch_size: usize,
    phase: BatchWritePhase,
}

fn run_crud_phase_batch_write_measurement(
    input: CrudPhaseMeasurement<'_>,
) -> Result<BenchMeasurement> {
    let cfg = input.cfg;
    #[cfg(feature = "bench")]
    if cfg.profile {
        input.engine.reset_write_profile();
    }
    let baseline = collect_counters(input.engine)?;
    let elapsed = run_concurrent_batch_write_phase(
        input.engine,
        cfg.num,
        cfg.threads,
        input.batch_size,
        input.value,
        input.phase,
    )?;
    if cfg.profile {
        print_write_profile(input.engine, input.measurement);
    }
    let counters = collect_counter_delta(&baseline, &collect_counters(input.engine)?);

    Ok(make_measurement(
        cfg,
        input.workload,
        input.measurement,
        input.options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            batch_size: Some(input.batch_size),
            threads: Some(cfg.threads),
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
    ))
}

fn run_concurrent_batch_write_phase(
    engine: &Arc<KvEngine>,
    num_keys: usize,
    writer_threads: usize,
    batch_size: usize,
    value: &[u8],
    phase: BatchWritePhase,
) -> Result<Duration> {
    let per_thread = num_keys / writer_threads;
    let remainder = num_keys % writer_threads;
    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..writer_threads {
        let eng = engine.clone();
        let val = value.to_vec();
        handles.push(std::thread::spawn(move || {
            let thread_ops = per_thread + usize::from(t < remainder);
            let base_idx = t * per_thread + remainder.min(t);
            let mut next = 0usize;
            while next < thread_ops {
                let current_batch = (thread_ops - next).min(batch_size);
                let mut keys = Vec::with_capacity(current_batch);
                for i in 0..current_batch {
                    keys.push(
                        format!("key{:08}", phase.start_key + base_idx + next + i).into_bytes(),
                    );
                }
                match phase.op {
                    BatchWriteOp::Put => {
                        let batch: Vec<_> = keys
                            .iter()
                            .map(|key| WriteBatchRecord::Put(key.as_slice(), val.as_slice()))
                            .collect();
                        eng.write_batch(&batch).expect("write_batch failed");
                    }
                    BatchWriteOp::Delete => {
                        let batch: Vec<_> = keys
                            .iter()
                            .map(|key| WriteBatchRecord::Del(key.as_slice()))
                            .collect();
                        eng.write_batch(&batch).expect("write_batch delete failed");
                    }
                }
                next += current_batch;
            }
        }));
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("writer thread panicked"))?;
    }

    Ok(start.elapsed())
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

    let mut rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
        cfg.seed,
        STEADY_STATE_KEY_STREAM_LABEL,
        0,
    ));
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
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
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

fn run_point_read_missing_in_range(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "point_read_missing_in_range";
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in 0..cfg.num as u64 {
        engine.put(&steady_state_loaded_key(i), &value)?;
    }
    engine.drain_flush()?;
    let load_elapsed = load_start.elapsed();
    let baseline = collect_counters(&engine)?;

    let mut rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
        cfg.seed,
        STEADY_STATE_KEY_STREAM_LABEL,
        0,
    ));
    let start = Instant::now();
    let mut found = 0u64;
    for _ in 0..cfg.reads {
        let id = rng.gen_range(0..cfg.num as u64);
        if engine.get(&steady_state_missing_key(id))?.is_some() {
            found += 1;
        }
    }
    let elapsed = start.elapsed();
    anyhow::ensure!(
        found == 0,
        "point_read_missing_in_range expected 0 found entries, got {}",
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
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some("uniform_absent_reserved_padding"),
            clients: Some(cfg.clients),
            warmup_secs: Some(cfg.warmup_secs),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: cfg.operation_mix.clone(),
            operation_mix_period: Some(cfg.operation_mix_period),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(cfg.reads as u64),
            ops_per_sec: Some(rate(cfg.reads as u64, elapsed)),
            reads: Some(cfg.reads as u64),
            reads_per_sec: Some(rate(cfg.reads as u64, elapsed)),
            found: Some(found),
            ..MeasurementResult::default()
        },
        counters,
    )])
}

fn run_point_read_uniform(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_steady_state_point_read_workload(
        cfg,
        "point_read_uniform",
        "uniform",
        None,
        SteadyStateKeySelector::uniform(cfg.num),
    )
}

fn run_point_read_zipfian(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_steady_state_point_read_workload(
        cfg,
        "point_read_zipfian",
        "scrambled_zipfian_0.99",
        Some(STEADY_STATE_ZIPFIAN_EXPONENT),
        SteadyStateKeySelector::scrambled_zipfian(cfg.num)?,
    )
}

fn open_loaded_steady_state_keyspace(
    cfg: &HarnessConfig,
    workload: &str,
) -> Result<(PathBuf, LsmStorageOptions, Arc<KvEngine>, Duration)> {
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in 0..cfg.num as u64 {
        engine.put(&steady_state_loaded_key(i), &value)?;
    }
    engine.drain_flush()?;

    Ok((path, options, engine, load_start.elapsed()))
}

fn finish_steady_state_measurement(
    cfg: &HarnessConfig,
    path: &Path,
    engine: &Arc<KvEngine>,
    baseline: &MeasurementCounters,
) -> Result<(Duration, MeasurementCounters)> {
    let drain_start = Instant::now();
    engine.drain_flush()?;
    let flush_drain = drain_start.elapsed();
    let counters = collect_counter_delta(baseline, &collect_counters(engine)?);
    engine.close()?;
    finalize_path(cfg, path)?;

    Ok((flush_drain, counters))
}

fn run_steady_state_point_read_workload(
    cfg: &HarnessConfig,
    workload: &'static str,
    key_selection: &'static str,
    zipfian_exponent: Option<f64>,
    selector: SteadyStateKeySelector,
) -> Result<Vec<BenchMeasurement>> {
    if let Some(operation_mix) = &cfg.operation_mix {
        validate_read_only_operation_mix(operation_mix, cfg.operation_mix_period)?;
    }

    let (path, options, engine, load_elapsed) = open_loaded_steady_state_keyspace(cfg, workload)?;

    if cfg.warmup_secs > 0 {
        let _warmup = run_steady_state_read_window(
            &engine,
            cfg,
            selector.clone(),
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
    }

    let baseline = collect_counters(&engine)?;
    let start = Instant::now();
    let window = run_steady_state_read_window(
        &engine,
        cfg,
        selector,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    let (flush_drain, counters) = finish_steady_state_measurement(cfg, &path, &engine, &baseline)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "{workload} completed no measured operations"
    );
    anyhow::ensure!(
        window.read_misses == 0,
        "{workload} expected all reads to hit, got {} misses",
        window.read_misses
    );

    let mut measurement = make_measurement(
        cfg,
        workload,
        "point_get",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some(key_selection),
            clients: Some(cfg.clients),
            warmup_secs: Some(cfg.warmup_secs),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: Some("get=1.0".to_string()),
            operation_mix_period: Some(1),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(window.completed_operations),
            ops_per_sec: Some(rate(window.completed_operations, elapsed)),
            reads: Some(window.reads),
            reads_per_sec: Some(rate(window.reads, elapsed)),
            found: Some(window.read_hits),
            ..MeasurementResult::default()
        },
        counters,
    );
    measurement.record.phase = Some("measurement");
    measurement.record.task = Some(steady_state_task_record(
        cfg,
        "get=1.0",
        1,
        "closed_loop_read_only",
        key_selection,
        zipfian_exponent,
    ));
    measurement.record.latency = cfg.latency_sample_every.map(|sample_every| {
        latency_record(
            sample_every,
            window.completed_operations,
            &window.latency_samples_ns,
        )
    });
    measurement.record.validation = Some(steady_state_validation_record(
        &window,
        "get=1.000000,put=0.000000".to_string(),
    ));
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));

    Ok(vec![measurement])
}

fn run_range_scan_uniform(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "range_scan_uniform";
    if let Some(operation_mix) = &cfg.operation_mix {
        validate_scan_only_operation_mix(operation_mix, cfg.operation_mix_period)?;
    }

    let (path, options, engine, load_elapsed) = open_loaded_steady_state_keyspace(cfg, workload)?;

    if cfg.warmup_secs > 0 {
        let warmup = run_steady_state_scan_window(
            &engine,
            cfg,
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        validate_range_scan_window(workload, &warmup)?;
    }

    let baseline = collect_counters(&engine)?;
    let start = Instant::now();
    let window = run_steady_state_scan_window(
        &engine,
        cfg,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    let (flush_drain, counters) = finish_steady_state_measurement(cfg, &path, &engine, &baseline)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "range_scan_uniform completed no measured operations"
    );
    validate_range_scan_window(workload, &window)?;

    let mut measurement = make_measurement(
        cfg,
        workload,
        "range_scan",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some("uniform"),
            clients: Some(cfg.clients),
            warmup_secs: Some(cfg.warmup_secs),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: Some("scan=1.0".to_string()),
            operation_mix_period: Some(1),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(window.completed_operations),
            ops_per_sec: Some(rate(window.completed_operations, elapsed)),
            entries: Some(window.scan_rows),
            entries_per_sec: Some(rate(window.scan_rows, elapsed)),
            reads: Some(window.scans),
            reads_per_sec: Some(rate(window.scans, elapsed)),
            total_nexts: Some(window.scan_rows),
            ..MeasurementResult::default()
        },
        counters,
    );
    measurement.record.phase = Some("measurement");
    measurement.record.task = Some(steady_state_task_record(
        cfg,
        "scan=1.0",
        1,
        "closed_loop_scan",
        "uniform",
        None,
    ));
    measurement.record.latency = cfg.latency_sample_every.map(|sample_every| {
        latency_record(
            sample_every,
            window.completed_operations,
            &window.latency_samples_ns,
        )
    });
    let mut validation = steady_state_validation_record(&window, "scan=1.000000".to_string());
    validation.expected_read_hits = None;
    validation.expected_read_misses = None;
    measurement.record.validation = Some(validation);
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));

    Ok(vec![measurement])
}

fn run_balanced_zipfian(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "balanced_zipfian";
    let operation_mix = cfg
        .operation_mix
        .clone()
        .unwrap_or_else(|| "get=0.5,put=0.5".to_string());
    let mix = SteadyStateOperationMix::parse(&operation_mix, cfg.operation_mix_period)?;
    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in 0..cfg.num as u64 {
        engine.put(&steady_state_loaded_key(i), &value)?;
    }
    engine.drain_flush()?;
    let load_elapsed = load_start.elapsed();

    let sampler = Arc::new(ZipfianSampler::new(cfg.num, STEADY_STATE_ZIPFIAN_EXPONENT)?);
    if cfg.warmup_secs > 0 {
        let _warmup = run_steady_state_mixed_window(
            &engine,
            cfg,
            &mix,
            sampler.clone(),
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        engine.drain_flush()?;
    }

    let baseline = collect_counters(&engine)?;
    let start = Instant::now();
    let window = run_steady_state_mixed_window(
        &engine,
        cfg,
        &mix,
        sampler,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    let drain_start = Instant::now();
    engine.drain_flush()?;
    let flush_drain = drain_start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    let counters = collect_counter_delta(&baseline, &collect_counters(&engine)?);
    engine.close()?;
    finalize_path(cfg, &path)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "balanced_zipfian completed no measured operations"
    );
    anyhow::ensure!(
        window.read_misses == 0,
        "balanced_zipfian expected all reads to hit, got {} misses",
        window.read_misses
    );
    mix.validate_complete_periods(window.complete_period_gets, window.complete_period_puts)?;

    let mut measurement = make_measurement(
        cfg,
        workload,
        "mixed_closed_loop",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some("scrambled_zipfian_0.99"),
            clients: Some(cfg.clients),
            warmup_secs: Some(cfg.warmup_secs),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: Some(operation_mix.clone()),
            operation_mix_period: Some(cfg.operation_mix_period),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(window.completed_operations),
            ops_per_sec: Some(rate(window.completed_operations, elapsed)),
            reads: Some(window.reads),
            reads_per_sec: Some(rate(window.reads, elapsed)),
            writes: Some(window.writes),
            writes_per_sec: Some(rate(window.writes, elapsed)),
            found: Some(window.read_hits),
            ..MeasurementResult::default()
        },
        counters,
    );
    measurement.record.phase = Some("measurement");
    measurement.record.task = Some(steady_state_task_record(
        cfg,
        operation_mix.clone(),
        cfg.operation_mix_period,
        "per_client_shuffled_period_cycle",
        "scrambled_zipfian_0.99",
        Some(STEADY_STATE_ZIPFIAN_EXPONENT),
    ));
    measurement.record.latency = cfg.latency_sample_every.map(|sample_every| {
        latency_record(
            sample_every,
            window.completed_operations,
            &window.latency_samples_ns,
        )
    });
    measurement.record.validation = Some(steady_state_validation_record(
        &window,
        format!(
            "get={:.6},put={:.6}",
            window.reads as f64 / window.completed_operations as f64,
            window.writes as f64 / window.completed_operations as f64
        ),
    ));
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));

    Ok(vec![measurement])
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
        suite: cfg.suite,
        phase: None,
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
        task: None,
        latency: None,
        result,
        validation: None,
        drain: None,
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

fn validate_operation_mix(spec: &str, period: usize) -> Result<()> {
    debug_assert!(period > 0, "caller must validate --operation-mix-period");
    let mut total = 0.0f64;
    let mut entries = 0usize;
    for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, value) = entry
            .split_once('=')
            .with_context(|| format!("invalid --operation-mix entry: {entry}"))?;
        anyhow::ensure!(!name.trim().is_empty(), "operation name must not be empty");
        let ratio: f64 = value
            .trim()
            .parse()
            .with_context(|| format!("invalid operation ratio for {}", name.trim()))?;
        anyhow::ensure!(
            ratio.is_finite(),
            "operation ratio for {} must be finite",
            name.trim()
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&ratio),
            "operation ratio for {} must be between 0 and 1",
            name.trim()
        );
        let slots = ratio * period as f64;
        anyhow::ensure!(
            (slots - slots.round()).abs() <= 1e-9,
            "operation ratio for {} is not representable by --operation-mix-period {}",
            name.trim(),
            period
        );
        total += ratio;
        entries += 1;
    }

    anyhow::ensure!(
        entries > 0,
        "--operation-mix must contain at least one entry"
    );
    anyhow::ensure!(
        (total - 1.0).abs() <= 1e-9,
        "--operation-mix ratios must sum to 1.0"
    );

    Ok(())
}

fn validate_single_operation_mix(
    spec: &str,
    period: usize,
    required_operation: &str,
    workload_kind: &str,
) -> Result<()> {
    validate_operation_mix(spec, period)?;

    let mut required_ratio = 0.0f64;
    for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, value) = entry
            .split_once('=')
            .with_context(|| format!("invalid --operation-mix entry: {entry}"))?;
        let name = name.trim();
        let ratio: f64 = value
            .trim()
            .parse()
            .with_context(|| format!("invalid operation ratio for {name}"))?;
        anyhow::ensure!(
            matches!(name, "get" | "put" | "scan"),
            "{workload_kind} do not support {name} in --operation-mix"
        );
        if name == required_operation {
            required_ratio += ratio;
        } else {
            anyhow::ensure!(
                ratio == 0.0,
                "{workload_kind} require --operation-mix {required_operation}=1.0"
            );
        }
    }

    anyhow::ensure!(
        (required_ratio - 1.0).abs() <= 1e-9,
        "{workload_kind} require --operation-mix {required_operation}=1.0"
    );
    Ok(())
}

fn validate_read_only_operation_mix(spec: &str, period: usize) -> Result<()> {
    validate_single_operation_mix(spec, period, "get", "point read workloads")
}

fn validate_scan_only_operation_mix(spec: &str, period: usize) -> Result<()> {
    validate_single_operation_mix(spec, period, "scan", "range scan workloads")
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
    anyhow::ensure!(cfg.wal_batch_size > 0, "--wal-batch-size must be > 0");
    anyhow::ensure!(cfg.clients > 0, "--clients must be > 0");
    anyhow::ensure!(cfg.measurement_secs > 0, "--measurement-secs must be > 0");
    anyhow::ensure!(
        cfg.operation_mix_period > 0,
        "--operation-mix-period must be > 0"
    );
    anyhow::ensure!(cfg.scan_limit > 0, "--scan-limit must be > 0");
    if let Some(latency_sample_every) = cfg.latency_sample_every {
        anyhow::ensure!(
            latency_sample_every > 0,
            "--latency-sample-every must be > 0"
        );
    }
    if let Some(settle_timeout_secs) = cfg.settle_timeout_secs {
        anyhow::ensure!(settle_timeout_secs > 0, "--settle-timeout-secs must be > 0");
    }
    if let Some(operation_mix) = &cfg.operation_mix {
        validate_operation_mix(operation_mix, cfg.operation_mix_period)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy_cfg() -> HarnessConfig {
        HarnessConfig::from_args(Args::try_parse_from(["write-perf"]).expect("parse args"))
    }

    fn steady_state_cfg() -> HarnessConfig {
        HarnessConfig::from_args(
            Args::try_parse_from(["write-perf", "--suite", "steady-state"]).expect("parse args"),
        )
    }

    #[test]
    fn parse_large_alias() {
        let args = Args::try_parse_from(["write-perf", "--large"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        assert_eq!(cfg.preset_name, "large");
        assert_eq!(cfg.num, 2_000_000);
    }

    #[test]
    fn steady_state_defaults_follow_rfc_018() {
        let args =
            Args::try_parse_from(["write-perf", "--suite", "steady-state"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);

        assert_eq!(cfg.suite, Suite::SteadyState);
        assert_eq!(cfg.preset_name, "default");
        assert_eq!(cfg.num, 1_000_000);
        assert_eq!(cfg.value_size, 400);
        assert_eq!(cfg.clients, 16);
        assert_eq!(cfg.warmup_secs, 60);
        assert_eq!(cfg.measurement_secs, 180);
    }

    #[test]
    fn steady_state_smoke_defaults_follow_rfc_018() {
        let args =
            Args::try_parse_from(["write-perf", "--suite", "steady-state", "--preset", "smoke"])
                .expect("parse args");
        let cfg = HarnessConfig::from_args(args);

        assert_eq!(cfg.preset_name, "smoke");
        assert_eq!(cfg.num, 10_000);
        assert_eq!(cfg.value_size, 400);
        assert_eq!(cfg.clients, 4);
        assert_eq!(cfg.warmup_secs, 0);
        assert_eq!(cfg.measurement_secs, 30);
    }

    #[test]
    fn explicit_no_wal_overrides_wal_default() {
        let args = Args::try_parse_from(["write-perf", "--suite", "steady-state", "--no-wal"])
            .expect("parse args");
        let cfg = HarnessConfig::from_args(args);

        assert!(!cfg.build_options(false, false).enable_wal);
        assert!(cfg.build_options(true, false).enable_wal);
    }

    #[test]
    fn steady_state_enables_wal_by_default() {
        let args =
            Args::try_parse_from(["write-perf", "--suite", "steady-state"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);

        assert!(cfg.build_options(false, false).enable_wal);
    }

    #[test]
    fn parse_subset_aliases() {
        let selected = select_workloads(Some("fillseq,readseq_validate_order"), &legacy_cfg())
            .expect("select workloads");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["fillseq", "readseq_validate_order"]);
    }

    #[test]
    fn parse_wal_batch_alias() {
        let selected = select_workloads(Some("wal_batch"), &legacy_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["wal_batch_concurrent"]);
    }

    #[test]
    fn parse_wal_batch_delete_alias() {
        let selected =
            select_workloads(Some("wal_batch_delete"), &legacy_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["wal_batch_delete_concurrent"]);
    }

    #[test]
    fn parse_memtable_publish_alias() {
        let selected =
            select_workloads(Some("memtable_publish"), &legacy_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["memtable_publish_concurrent"]);
    }

    #[test]
    fn parse_memtable_publish_delete_alias() {
        let selected = select_workloads(Some("memtable_publish_delete"), &legacy_cfg())
            .expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["memtable_publish_delete_concurrent"]);
    }

    #[test]
    fn parse_crud_phase_batch_alias() {
        let selected =
            select_workloads(Some("crud_phase_batch"), &legacy_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["crud_phase_batch_writes"]);
    }

    #[test]
    fn parse_crud_batch_create_100_alias() {
        let selected = select_workloads(Some("crud_batch_create_100"), &legacy_cfg())
            .expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["crud_bench_batch_create_100"]);
    }

    #[test]
    fn ordered_integer_key_bytes_match_crud_bench() {
        assert_eq!(ordered_integer_key_bytes(0), 1u32.to_ne_bytes());
        assert_eq!(ordered_integer_key_bytes(99), 100u32.to_ne_bytes());
        assert_eq!(ordered_integer_key_bytes(100), 101u32.to_ne_bytes());
    }

    #[test]
    fn steady_state_missing_key_uses_reserved_padding_byte() {
        let loaded = steady_state_loaded_key(7);
        let missing = steady_state_missing_key(7);
        let next_loaded = steady_state_loaded_key(8);

        assert_eq!(loaded.len(), 20);
        assert_eq!(&loaded[..8], &7u64.to_be_bytes());
        assert!(loaded[8..].iter().all(|b| *b == b'0'));
        assert_eq!(&missing[..19], &loaded[..19]);
        assert_eq!(missing[19], b'1');
        assert!(loaded < missing);
        assert!(missing < next_loaded);
    }

    #[test]
    fn parse_point_read_missing_in_range_workload() {
        let selected = select_workloads(Some("readmissing_in_range"), &steady_state_cfg())
            .expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["point_read_missing_in_range"]);
    }

    #[test]
    fn parse_balanced_zipfian_workload() {
        let selected =
            select_workloads(Some("balanced"), &steady_state_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["balanced_zipfian"]);
    }

    #[test]
    fn parse_point_read_steady_state_workloads() {
        let selected = select_workloads(
            Some("readuniform,readzipfian,scanuniform"),
            &steady_state_cfg(),
        )
        .expect("select workloads");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(
            names,
            vec![
                "point_read_uniform",
                "point_read_zipfian",
                "range_scan_uniform"
            ]
        );
    }

    #[test]
    fn balanced_zipfian_operation_mix_has_deterministic_period_counts() {
        let mix = SteadyStateOperationMix::parse("get=0.5,put=0.5", 1000).expect("parse mix");
        let schedule = mix.shuffled_schedule(42, 0);
        let gets = schedule
            .iter()
            .filter(|operation| matches!(operation, SteadyStateOperation::Get))
            .count();
        let puts = schedule
            .iter()
            .filter(|operation| matches!(operation, SteadyStateOperation::Put))
            .count();

        assert_eq!(schedule.len(), 1000);
        assert_eq!(gets, 500);
        assert_eq!(puts, 500);
    }

    #[test]
    fn balanced_zipfian_rejects_unsupported_mix_operation() {
        let err = SteadyStateOperationMix::parse("get=0.5,delete=0.5", 1000)
            .expect_err("unsupported operation should fail");
        assert!(err.to_string().contains("supports only get and put"));
    }

    #[test]
    fn zipfian_sampler_stays_inside_keyspace() {
        let sampler = ZipfianSampler::new(17, STEADY_STATE_ZIPFIAN_EXPONENT).expect("sampler");
        let mut rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
            42,
            STEADY_STATE_KEY_STREAM_LABEL,
            0,
        ));
        for _ in 0..1000 {
            assert!(sampler.sample(&mut rng) < 17);
        }
    }

    #[test]
    fn steady_state_window_phase_separates_streams() {
        let warmup_seed =
            steady_state_stream_seed(42, SteadyStateWindowPhase::Warmup.stream_label(), 0);
        let measurement_seed =
            steady_state_stream_seed(42, SteadyStateWindowPhase::Measurement.stream_label(), 0);

        assert_ne!(warmup_seed, measurement_seed);
        assert_ne!(
            steady_state_stream_seed(warmup_seed, STEADY_STATE_KEY_STREAM_LABEL, 0),
            steady_state_stream_seed(measurement_seed, STEADY_STATE_KEY_STREAM_LABEL, 0)
        );
    }

    #[test]
    fn rand_chacha_version_matches_lockfile() {
        let package = include_str!("../../../Cargo.lock")
            .split("[[package]]")
            .find(|package| package.contains("name = \"rand_chacha\""))
            .expect("rand_chacha package in Cargo.lock");

        assert!(
            package.contains(&format!("version = \"{RAND_CHACHA_VERSION}\"")),
            "update RAND_CHACHA_VERSION when Cargo.lock changes rand_chacha"
        );
    }

    #[test]
    fn default_legacy_selection_excludes_steady_state_workloads() {
        let selected = select_workloads(None, &legacy_cfg()).expect("select workloads");
        assert!(
            selected
                .iter()
                .all(|workload| workload.suite == Suite::Legacy)
        );
        assert!(
            !selected
                .iter()
                .any(|workload| workload.name == "point_read_missing_in_range")
        );
    }

    #[test]
    fn steady_state_workload_requires_steady_state_suite() {
        let err = select_workloads(Some("point_read_missing_in_range"), &legacy_cfg())
            .expect_err("wrong suite should fail");
        assert!(err.to_string().contains("belongs to"));
    }

    #[test]
    fn wal_required_workload_rejects_no_wal() {
        let args =
            Args::try_parse_from(["write-perf", "--bench", "wal", "--no-wal"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = select_workloads(Some("wal"), &cfg).expect_err("wal workload should fail");
        assert!(err.to_string().contains("requires WAL"));
    }

    #[test]
    fn default_no_wal_selection_skips_wal_required_workloads() {
        let args = Args::try_parse_from(["write-perf", "--no-wal"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let selected = select_workloads(None, &cfg).expect("select workloads");

        assert!(!selected.is_empty());
        assert!(selected.iter().all(|workload| !workload.requires_wal));
    }

    #[test]
    fn parse_wal_batch_size() {
        let args =
            Args::try_parse_from(["write-perf", "--wal-batch-size", "100"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        assert_eq!(cfg.wal_batch_size, 100);
    }

    #[test]
    fn effective_wal_batch_size_uses_largest_writer_partition() {
        assert_eq!(effective_wal_batch_size(100, 4, 100), 25);
        assert_eq!(effective_wal_batch_size(101, 4, 100), 26);
        assert_eq!(effective_wal_batch_size(1000, 4, 100), 100);
    }

    #[test]
    fn reject_unsupported_readreverse_selector() {
        let err = select_workloads(Some("readreverse"), &legacy_cfg())
            .expect_err("readreverse should fail");
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn json_record_contains_measurement_name() {
        let cfg = HarnessConfig {
            suite: Suite::Legacy,
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
            wal_batch_size: 1,
            parallel_scan_cache_admission: "bypass".to_string(),
            compaction: CompactionMode::None,
            wal_override: None,
            vlog_override: false,
            profile: true,
            clients: 1,
            warmup_secs: 0,
            measurement_secs: 1,
            operation_mix: None,
            operation_mix_period: 1000,
            scan_limit: 1,
            latency_sample_every: None,
            settle_timeout_secs: None,
            num_overridden: false,
            value_size_overridden: false,
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
        assert!(json.get("phase").is_none());
        assert!(json.get("task").is_none());
        assert!(json.get("latency").is_none());
        assert!(json.get("validation").is_none());
        assert!(json.get("drain").is_none());
    }

    #[test]
    fn steady_state_json_record_contains_task_validation_and_drain() {
        let cfg = steady_state_cfg();
        let options = cfg.build_options(false, false);
        let mut measurement = make_measurement(
            &cfg,
            "balanced_zipfian",
            "mixed_closed_loop",
            &options,
            MeasurementParams::default(),
            MeasurementResult {
                measure_elapsed_ms: 1.0,
                ops: Some(2),
                ..MeasurementResult::default()
            },
            MeasurementCounters::default(),
        );
        measurement.record.phase = Some("measurement");
        measurement.record.task = Some(SteadyStateTaskRecord {
            clients: cfg.clients,
            warmup_secs: cfg.warmup_secs,
            measurement_secs: cfg.measurement_secs,
            operation_mix: "get=0.5,put=0.5".to_string(),
            operation_mix_period: 1000,
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            key_selection: "scrambled_zipfian_0.99",
            scan_limit: cfg.scan_limit,
            seed: cfg.seed,
            rng_algorithm: "ChaCha12Rng",
            rng_crate_version: RAND_CHACHA_VERSION,
            seed_derivation: STEADY_STATE_SEED_VERSION,
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            key_format: "be_u64_plus_ascii_zero_padding_20b",
        });
        measurement.record.validation = Some(ValidationRecord {
            errors: 0,
            read_hits: 1,
            read_misses: 0,
            expected_read_hits: Some(1),
            expected_read_misses: Some(0),
            observed_operation_mix: "get=0.500000,put=0.500000".to_string(),
            scan_count_errors: 0,
            scan_order_errors: 0,
            scan_key_errors: 0,
            transaction_attempts: 0,
            transaction_commits: 0,
            transaction_conflicts: 0,
            selected_operations: 2,
            completed_operations: 2,
            min_completed_operations: 1,
            complete_period_operations: 0,
            tail_operations: 2,
            tail_gets: 1,
            tail_puts: 1,
        });
        measurement.record.drain = Some(DrainRecord {
            flush_drain_ms: 0.0,
            background_drain_ms: None,
            background_drain_status: "not_requested",
        });

        let json = serde_json::to_value(&measurement.record).expect("serialize record");
        assert_eq!(json["phase"], "measurement");
        assert_eq!(json["task"]["key_selection"], "scrambled_zipfian_0.99");
        assert_eq!(
            json["task"]["operation_mix_scheduler"],
            "per_client_shuffled_period_cycle"
        );
        assert_eq!(json["task"]["rng_crate_version"], RAND_CHACHA_VERSION);
        assert_eq!(
            json["task"]["scramble_function"],
            "splitmix64(rank) % record_count"
        );
        assert_eq!(json["validation"]["read_misses"], 0);
        assert_eq!(json["validation"]["tail_operations"], 2);
        assert_eq!(json["drain"]["background_drain_status"], "not_requested");
    }

    #[test]
    fn point_read_task_record_describes_read_only_scheduler() {
        let cfg = steady_state_cfg();
        let task =
            steady_state_task_record(&cfg, "get=1.0", 1, "closed_loop_read_only", "uniform", None);

        assert_eq!(task.operation_mix, "get=1.0");
        assert_eq!(task.operation_mix_period, 1);
        assert_eq!(task.operation_mix_scheduler, "closed_loop_read_only");
        assert_eq!(task.key_selection, "uniform");
        assert_eq!(task.scramble_function, "none");
        assert_eq!(task.zipfian_exponent, None);
    }

    #[test]
    fn point_read_operation_mix_rejects_writes() {
        let err = validate_read_only_operation_mix("get=0.5,put=0.5", 1000)
            .expect_err("point reads should reject write mix");

        assert!(err.to_string().contains("require --operation-mix get=1.0"));
    }

    #[test]
    fn point_read_operation_mix_rejects_unknown_zero_ratio_operation() {
        let err = validate_read_only_operation_mix("get=1.0,delete=0.0", 1000)
            .expect_err("point reads should reject unknown operations");

        assert!(err.to_string().contains("do not support delete"));
    }

    #[test]
    fn point_read_operation_mix_accepts_equivalent_get_only() {
        validate_read_only_operation_mix("get=1.0,put=0.0", 1000).expect("read-only mix");
    }

    #[test]
    fn range_scan_operation_mix_rejects_gets() {
        let err = validate_scan_only_operation_mix("scan=0.5,get=0.5", 1000)
            .expect_err("range scans should reject get mix");

        assert!(err.to_string().contains("require --operation-mix scan=1.0"));
    }

    #[test]
    fn range_scan_operation_mix_rejects_unknown_zero_ratio_operation() {
        let err = validate_scan_only_operation_mix("scan=1.0,delete=0.0", 1000)
            .expect_err("range scans should reject unknown operations");

        assert!(err.to_string().contains("do not support delete"));
    }

    #[test]
    fn range_scan_operation_mix_accepts_equivalent_scan_only() {
        validate_scan_only_operation_mix("scan=1.0,get=0.0", 1000).expect("scan-only mix");
    }

    #[test]
    fn latency_record_reports_min_and_unsampled_operations() {
        let record = latency_record(10, 25, &[3_000_000, 1_000_000, 2_000_000]);

        assert_eq!(record.sample_every, 10);
        assert_eq!(record.samples, 3);
        assert_eq!(record.unsampled_completed_operations, 22);
        assert_eq!(record.min_ms, Some(1.0));
        assert_eq!(record.max_ms, Some(3.0));
    }

    #[test]
    fn reject_zero_num() {
        let cfg = HarnessConfig {
            suite: Suite::Legacy,
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
            wal_batch_size: 1,
            parallel_scan_cache_admission: "bypass".to_string(),
            compaction: CompactionMode::None,
            wal_override: None,
            vlog_override: false,
            profile: false,
            clients: 1,
            warmup_secs: 0,
            measurement_secs: 1,
            operation_mix: None,
            operation_mix_period: 1000,
            scan_limit: 1,
            latency_sample_every: None,
            settle_timeout_secs: None,
            num_overridden: false,
            value_size_overridden: false,
        };
        assert!(validate_config(&cfg).is_err());
    }

    #[test]
    fn reject_zero_wal_batch_size() {
        let args =
            Args::try_parse_from(["write-perf", "--wal-batch-size", "0"]).expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("zero wal batch size should fail");
        assert!(err.to_string().contains("--wal-batch-size"));
    }

    #[test]
    fn validate_operation_mix_rejects_invalid_totals() {
        let args = Args::try_parse_from(["write-perf", "--operation-mix", "get=0.5,put=0.4"])
            .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("invalid mix total should fail");
        assert!(err.to_string().contains("sum to 1.0"));
    }

    #[test]
    fn validate_operation_mix_rejects_unrepresentable_ratio() {
        let args = Args::try_parse_from([
            "write-perf",
            "--operation-mix",
            "get=0.3333,put=0.6667",
            "--operation-mix-period",
            "100",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("unrepresentable mix should fail");
        assert!(err.to_string().contains("not representable"));
    }

    #[test]
    fn validate_operation_mix_accepts_period_slots() {
        let args = Args::try_parse_from([
            "write-perf",
            "--operation-mix",
            "get=0.5,put=0.5",
            "--operation-mix-period",
            "1000",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        validate_config(&cfg).expect("valid mix");
    }
}
