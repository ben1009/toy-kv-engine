mod wrapper;

use parking_lot::Mutex;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::ops::Bound;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wrapper::kv_engine_wrapper;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use kv_engine_wrapper::{
    block_on,
    checkpoint::CheckpointOptions,
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
use serde::{Deserialize, Serialize};

const JSON_SCHEMA_V1: &str = "kv-engine.write-perf.v1";
const JSON_SCHEMA_V2: &str = "kv-engine.write-perf.v2";
const GOLDEN_MANIFEST_SCHEMA_V1: &str = "kv-engine.steady-state-golden.v1";
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

fn schema_for_suite(suite: Suite) -> &'static str {
    match suite {
        Suite::Legacy => JSON_SCHEMA_V1,
        Suite::SteadyState => JSON_SCHEMA_V2,
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
    #[arg(long)]
    transaction_hot_set: Option<usize>,
    #[arg(long)]
    transaction_reads: Option<usize>,
    #[arg(long)]
    transaction_updates: Option<usize>,
    #[arg(long)]
    transaction_retries: Option<usize>,
    #[arg(long)]
    prepare_golden: bool,
    #[arg(long)]
    golden_path: Option<PathBuf>,
    #[arg(long)]
    clone_golden: bool,
    /// Validate a write-perf JSONL artifact and exit without running workloads.
    #[arg(long)]
    validate_json: Option<PathBuf>,
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
    transaction_hot_set: usize,
    transaction_reads: usize,
    transaction_updates: usize,
    transaction_retries: usize,
    prepare_golden: bool,
    golden_path: Option<PathBuf>,
    clone_golden: bool,
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
                ("smoke", 10_000, 10_000, 1, 10_000, 400, 1, 0, 1)
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
            latency_sample_every: args.latency_sample_every.or(match args.suite {
                Suite::Legacy => None,
                Suite::SteadyState => Some(1000),
            }),
            settle_timeout_secs: args.settle_timeout_secs,
            transaction_hot_set: args
                .transaction_hot_set
                .unwrap_or(128.min(args.num.unwrap_or(num))),
            transaction_reads: args.transaction_reads.unwrap_or(5),
            transaction_updates: args.transaction_updates.unwrap_or(5),
            transaction_retries: args.transaction_retries.unwrap_or(0),
            prepare_golden: args.prepare_golden,
            golden_path: args.golden_path,
            clone_golden: args.clone_golden,
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
        name: "idle",
        aliases: &[],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_idle,
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
        name: "read_heavy_zipfian",
        aliases: &["readheavy"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_read_heavy_zipfian,
    },
    WorkloadSpec {
        name: "balanced_zipfian",
        aliases: &["balanced"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_balanced_zipfian,
    },
    WorkloadSpec {
        name: "update_heavy_zipfian",
        aliases: &["updateheavy"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_update_heavy_zipfian,
    },
    WorkloadSpec {
        name: "sustained_ingest",
        aliases: &["ingest"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_sustained_ingest,
    },
    WorkloadSpec {
        name: "transaction_contention",
        aliases: &["txn_contention"],
        suite: Suite::SteadyState,
        requires_wal: false,
        run: run_transaction_contention,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    throughput: Option<ThroughputRecord>,
    result: MeasurementResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    validation: Option<ValidationRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain: Option<DrainRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    golden_manifest: Option<GoldenManifestRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_warmup_baseline: Option<GoldenBaselineRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    counter_snapshots: Option<CounterSnapshotsRecord>,
    counters: MeasurementCounters,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct EngineOptionsRecord {
    wal: bool,
    #[serde(default)]
    serializable: bool,
    value_separation: bool,
    compaction: String,
    target_sst_size: usize,
    memtable_limit: usize,
    cache_capacity: u64,
}

#[derive(Serialize)]
struct LegacyEngineOptionsRecord<'a> {
    wal: bool,
    value_separation: bool,
    compaction: &'a str,
    target_sst_size: usize,
    memtable_limit: usize,
    cache_capacity: u64,
}

impl<'a> From<&'a EngineOptionsRecord> for LegacyEngineOptionsRecord<'a> {
    fn from(options: &'a EngineOptionsRecord) -> Self {
        Self {
            wal: options.wal,
            value_separation: options.value_separation,
            compaction: &options.compaction,
            target_sst_size: options.target_sst_size,
            memtable_limit: options.memtable_limit,
            cache_capacity: options.cache_capacity,
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_hot_set: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_reads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_updates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_retries: Option<usize>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    found: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct MeasurementCounters {
    block_cache_entry_count: u64,
    block_cache_hit_count: u64,
    block_cache_miss_count: u64,
    block_cache_admitted_count: u64,
    block_cache_rejected_count: u64,
    block_cache_evicted_count: u64,
    wal_commit_groups: u64,
    wal_commit_solo_groups: u64,
    wal_commit_buffers: u64,
    wal_commit_bytes: u64,
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

#[derive(Clone, Debug, Serialize)]
struct CounterSnapshotsRecord {
    before: MeasurementCounters,
    after: MeasurementCounters,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_hot_set: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_reads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_updates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_retries: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_conflict_latency: Option<bool>,
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
struct ThroughputRecord {
    window_secs: u64,
    complete_windows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    operations: Option<RateWindowRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reads: Option<RateWindowRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    writes: Option<RateWindowRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logical_bytes: Option<RateWindowRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_logical_bytes: Option<RateWindowRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_logical_bytes: Option<RateWindowRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scan_rows: Option<RateWindowRecord>,
}

#[derive(Clone, Serialize)]
struct RateWindowRecord {
    total: u64,
    avg_per_sec: f64,
    p1_per_sec: f64,
    p50_per_sec: f64,
    p95_per_sec: f64,
    p99_per_sec: f64,
    min_per_sec: f64,
    max_per_sec: f64,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GoldenManifestRecord {
    manifest_schema: String,
    manifest_path: String,
    manifest_digest: String,
    key_count: usize,
    value_size: usize,
    key_format: String,
    engine_options: EngineOptionsRecord,
    engine_options_hash: String,
    source_commit: String,
    sst_file_count: usize,
    vlog_file_count: usize,
    level_layout: String,
    settle_status: String,
    settle_elapsed_ms: f64,
    settle_timeout_secs: Option<u64>,
}

#[derive(Serialize)]
struct GoldenManifestDigestRecord<'a> {
    manifest_schema: &'a str,
    key_count: usize,
    value_size: usize,
    key_format: &'a str,
    engine_options: &'a EngineOptionsRecord,
    engine_options_hash: &'a str,
    source_commit: &'a str,
    sst_file_count: usize,
    vlog_file_count: usize,
    level_layout: &'a str,
    settle_status: &'a str,
    settle_timeout_secs: Option<u64>,
}

#[derive(Serialize)]
struct LegacyGoldenManifestDigestRecord<'a> {
    manifest_schema: &'a str,
    key_count: usize,
    value_size: usize,
    key_format: &'a str,
    engine_options: &'a LegacyEngineOptionsRecord<'a>,
    engine_options_hash: &'a str,
    source_commit: &'a str,
    sst_file_count: usize,
    vlog_file_count: usize,
    level_layout: &'a str,
    settle_status: &'a str,
    settle_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct GoldenBaselineRecord {
    sst_file_count: usize,
    vlog_file_count: usize,
    level_layout: String,
    counters: MeasurementCounters,
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
    if let Some(path) = args.validate_json.as_ref() {
        return validate_json_artifact(path);
    }

    let bench_arg = args.bench.clone();
    let cfg = HarnessConfig::from_args(args);
    validate_config(&cfg)?;
    validate_run_mode(&cfg, bench_arg.as_deref())?;
    let _hotpath_guard = start_hotpath_profile(cfg.profile);
    if cfg.prepare_golden {
        let measurements = vec![prepare_steady_state_golden(&cfg)?];
        return emit_measurements(&cfg, &measurements);
    }
    let workloads = select_workloads(bench_arg.as_deref(), &cfg)?;
    validate_selected_workloads(&cfg, &workloads)?;

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
                .filter(|workload| {
                    !(cfg.clone_golden
                        && matches!(workload.name, "sustained_ingest" | "transaction_contention"))
                })
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

fn validate_selected_workloads(cfg: &HarnessConfig, workloads: &[&WorkloadSpec]) -> Result<()> {
    if cfg.clone_golden {
        for workload in workloads {
            anyhow::ensure!(
                !matches!(workload.name, "sustained_ingest" | "transaction_contention"),
                "--clone-golden is not supported for {}; select a read or mixed steady-state workload",
                workload.name
            );
        }
    }

    Ok(())
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

fn validate_json_artifact(path: &Path) -> Result<()> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = 0usize;
    for (idx, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        rows += 1;
        let record: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("{}:{} is not valid JSON", path.display(), idx + 1))?;
        validate_json_record(&record)
            .with_context(|| format!("{}:{} failed validation", path.display(), idx + 1))?;
    }
    anyhow::ensure!(
        rows > 0,
        "{} contained no JSON records to validate",
        path.display()
    );
    eprintln!("validated {rows} JSON record(s) from {}", path.display());
    Ok(())
}

fn validate_json_record(record: &serde_json::Value) -> Result<()> {
    let schema = json_str(record, "schema")?;
    match schema {
        JSON_SCHEMA_V1 => Ok(()),
        JSON_SCHEMA_V2 => validate_steady_state_json_record(record),
        other => bail!("unsupported schema `{other}`"),
    }
}

fn validate_steady_state_json_record(record: &serde_json::Value) -> Result<()> {
    anyhow::ensure!(
        json_str(record, "suite")? == "steady_state",
        "steady-state v2 rows must use suite steady_state"
    );
    let phase = json_str(record, "phase")?;
    match phase {
        "prepare" => {
            anyhow::ensure!(
                json_str(record, "workload")? == "prepare_golden",
                "prepare rows must use workload prepare_golden"
            );
            anyhow::ensure!(
                json_str(record, "measurement")? == "bulk_load",
                "prepare rows must use measurement bulk_load"
            );
            let manifest = json_object(record, "golden_manifest")?;
            validate_golden_manifest_json(manifest)?;
            validate_golden_manifest_matches_record(manifest, record)?;
            let drain = json_object(record, "drain")?;
            validate_drain_json(drain)?;
            Ok(())
        }
        "measurement" => validate_steady_state_measurement_json(record),
        other => bail!("unsupported steady-state phase `{other}`"),
    }
}

fn validate_steady_state_measurement_json(record: &serde_json::Value) -> Result<()> {
    let workload = json_str(record, "workload")?;
    validate_known_steady_state_workload(workload)?;
    validate_measurement_label(workload, json_str(record, "measurement")?)?;
    let params = json_object(record, "params")?;
    let task = json_object(record, "task")?;
    validate_steady_state_task_json(task, params, workload)?;
    validate_transaction_engine_options_json(record, workload)?;
    validate_counter_snapshots_json(record)?;
    let validation = json_object(record, "validation")?;
    let drain = json_object(record, "drain")?;
    validate_validation_json(validation)?;
    validate_workload_validation_json(validation, workload, task)?;
    validate_drain_json(drain)?;
    if let Some(manifest) = record.get("golden_manifest") {
        validate_golden_manifest_json(manifest)?;
        validate_golden_manifest_matches_record(manifest, record)?;
    }
    if workload != "idle" {
        let completed_operations = json_u64(validation, "completed_operations")?;
        anyhow::ensure!(
            completed_operations > 0,
            "validation.completed_operations must be greater than zero for non-idle measurement rows"
        );
        validate_non_idle_throughput_json(
            json_object(record, "throughput")?,
            workload,
            task,
            completed_operations,
        )?;
        validate_non_idle_latency_json(
            json_object(record, "latency")?,
            json_u64(validation, "selected_operations")?,
            json_u64(task, "clients")?,
        )?;
        let result_ops = json_u64(json_object(record, "result")?, "ops")?;
        anyhow::ensure!(
            result_ops == completed_operations,
            "result.ops must match validation.completed_operations"
        );
    } else {
        anyhow::ensure!(
            json_u64(validation, "completed_operations")? == 0,
            "idle validation.completed_operations must be zero"
        );
        anyhow::ensure!(
            record.get("throughput").is_none(),
            "idle measurement rows must not include throughput"
        );
        anyhow::ensure!(
            record.get("latency").is_none(),
            "idle measurement rows must not include latency"
        );
        let result = json_object(record, "result")?;
        let result_ops = json_u64(result, "ops")?;
        anyhow::ensure!(
            result_ops == 0,
            "idle result.ops must match validation.completed_operations"
        );
    }
    Ok(())
}

fn validate_transaction_engine_options_json(
    record: &serde_json::Value,
    workload: &str,
) -> Result<()> {
    if workload != "transaction_contention" {
        return Ok(());
    }

    let engine_options: EngineOptionsRecord =
        serde_json::from_value(json_object(record, "engine_options")?.clone())
            .context("engine_options must match engine options schema")?;
    anyhow::ensure!(
        engine_options.serializable,
        "transaction_contention engine_options.serializable must be true"
    );
    Ok(())
}

fn validate_measurement_label(workload: &str, measurement: &str) -> Result<()> {
    let expected = match workload {
        "idle" => "idle_wait",
        "point_read_missing_in_range" => "negative_point_get",
        "point_read_uniform" | "point_read_zipfian" => "point_get",
        "range_scan_uniform" => "range_scan",
        "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => "mixed_closed_loop",
        "sustained_ingest" => "write_closed_loop",
        "transaction_contention" => "serializable_hot_set",
        other => bail!("unknown steady-state workload `{other}`"),
    };
    anyhow::ensure!(
        measurement == expected,
        "{workload} measurement must be {expected}"
    );
    Ok(())
}

fn validate_known_steady_state_workload(workload: &str) -> Result<()> {
    anyhow::ensure!(
        matches!(
            workload,
            "idle"
                | "point_read_missing_in_range"
                | "point_read_uniform"
                | "point_read_zipfian"
                | "range_scan_uniform"
                | "read_heavy_zipfian"
                | "balanced_zipfian"
                | "update_heavy_zipfian"
                | "sustained_ingest"
                | "transaction_contention"
        ),
        "unknown steady-state workload `{workload}`"
    );
    Ok(())
}

fn validate_steady_state_task_json(
    task: &serde_json::Value,
    params: &serde_json::Value,
    workload: &str,
) -> Result<()> {
    let clients = json_u64(task, "clients")?;
    if workload == "idle" {
        anyhow::ensure!(clients == 0, "idle task.clients must be zero");
    } else {
        anyhow::ensure!(clients > 0, "task.clients must be greater than zero");
    }
    validate_task_u64_matches_params(task, params, "clients")?;
    validate_task_u64_matches_params(task, params, "warmup_secs")?;
    validate_task_u64_matches_params(task, params, "measurement_secs")?;
    validate_task_u64_matches_params(task, params, "operation_mix_period")?;
    validate_task_u64_matches_params(task, params, "scan_limit")?;
    validate_task_u64_matches_params(task, params, "seed")?;
    validate_task_str_matches_params(task, params, "operation_mix")?;
    validate_task_str_matches_params(task, params, "key_selection")?;
    validate_task_str_matches_params(task, params, "rng_algorithm")?;
    if json_optional_str(params, "seed_derivation")?.is_some() {
        validate_task_str_matches_params(task, params, "seed_derivation")?;
    }
    validate_steady_state_task_shape(task, workload)?;

    for field in [
        "operation_mix",
        "operation_mix_scheduler",
        "key_selection",
        "rng_algorithm",
        "rng_crate_version",
        "seed_derivation",
        "scramble_function",
        "key_format",
    ] {
        anyhow::ensure!(
            !json_str(task, field)?.is_empty(),
            "task.{field} must be non-empty"
        );
    }
    anyhow::ensure!(
        json_str(task, "key_format")? == "be_u64_plus_ascii_zero_padding_20b",
        "task.key_format must be be_u64_plus_ascii_zero_padding_20b"
    );
    if let Some(zipfian_exponent) = task.get("zipfian_exponent") {
        anyhow::ensure!(
            zipfian_exponent.is_null() || zipfian_exponent.as_f64().is_some(),
            "task.zipfian_exponent must be numeric or null"
        );
    }

    if workload == "transaction_contention" {
        validate_task_u64_matches_params(task, params, "transaction_hot_set")?;
        validate_task_u64_matches_params(task, params, "transaction_reads")?;
        validate_task_u64_matches_params(task, params, "transaction_updates")?;
        validate_task_u64_matches_params(task, params, "transaction_retries")?;
        anyhow::ensure!(
            json_u64(task, "transaction_hot_set")? > 0,
            "task.transaction_hot_set must be greater than zero"
        );
        anyhow::ensure!(
            json_u64(task, "transaction_reads")? > 0,
            "task.transaction_reads must be greater than zero"
        );
        anyhow::ensure!(
            json_u64(task, "transaction_updates")? > 0,
            "task.transaction_updates must be greater than zero"
        );
        anyhow::ensure!(
            json_bool(task, "transaction_conflict_latency")?,
            "task.transaction_conflict_latency must be true"
        );
    } else {
        for field in [
            "transaction_hot_set",
            "transaction_reads",
            "transaction_updates",
            "transaction_retries",
            "transaction_conflict_latency",
        ] {
            anyhow::ensure!(
                json_required(task, field).is_err() || json_required(task, field)?.is_null(),
                "task.{field} is only valid for transaction_contention"
            );
        }
    }

    Ok(())
}

fn validate_steady_state_task_shape(task: &serde_json::Value, workload: &str) -> Result<()> {
    let period = usize::try_from(json_u64(task, "operation_mix_period")?)
        .context("task.operation_mix_period does not fit usize")?;
    let operation_mix = json_str(task, "operation_mix")?;
    match workload {
        "idle" => {
            validate_task_fixed_shape(task, "idle_wait", "none", "none", None)?;
            anyhow::ensure!(
                operation_mix == "idle=1.0",
                "idle task.operation_mix must be idle=1.0"
            );
            anyhow::ensure!(period == 1, "idle task.operation_mix_period must be 1");
        }
        "point_read_uniform" => {
            validate_task_fixed_shape(task, "closed_loop_read_only", "uniform", "none", None)?;
            validate_read_only_operation_mix(operation_mix, period)?;
        }
        "point_read_missing_in_range" => {
            validate_task_fixed_shape(
                task,
                "closed_loop_read_only",
                "uniform_absent_reserved_padding",
                "none",
                None,
            )?;
            validate_read_only_operation_mix(operation_mix, period)?;
        }
        "point_read_zipfian" => {
            validate_task_fixed_shape(
                task,
                "closed_loop_read_only",
                "scrambled_zipfian_0.99",
                "splitmix64(rank) % record_count",
                Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            )?;
            validate_read_only_operation_mix(operation_mix, period)?;
        }
        "range_scan_uniform" => {
            validate_task_fixed_shape(task, "closed_loop_scan", "uniform", "none", None)?;
            validate_scan_only_operation_mix(operation_mix, period)?;
        }
        "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => {
            validate_task_fixed_shape(
                task,
                "per_client_shuffled_period_cycle",
                "scrambled_zipfian_0.99",
                "splitmix64(rank) % record_count",
                Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            )?;
            SteadyStateOperationMix::parse(operation_mix, period)?;
        }
        "sustained_ingest" => {
            validate_task_fixed_shape(
                task,
                "closed_loop_unique_sequential",
                "unique_sequential",
                "none",
                None,
            )?;
            validate_write_only_operation_mix(operation_mix, period)?;
        }
        "transaction_contention" => {
            validate_task_fixed_shape(
                task,
                "closed_loop_serializable_transaction",
                "uniform_hot_set",
                "none",
                None,
            )?;
            let expected_mix = format!(
                "txn_reads={},txn_updates={},txn_retries={}",
                json_u64(task, "transaction_reads")?,
                json_u64(task, "transaction_updates")?,
                json_u64(task, "transaction_retries")?
            );
            anyhow::ensure!(
                operation_mix == expected_mix,
                "transaction_contention task.operation_mix must match transaction task fields"
            );
            anyhow::ensure!(
                period == 1,
                "transaction_contention task.operation_mix_period must be 1"
            );
        }
        other => bail!("unknown steady-state workload `{other}`"),
    }
    Ok(())
}

fn validate_task_fixed_shape(
    task: &serde_json::Value,
    scheduler: &str,
    key_selection: &str,
    scramble_function: &str,
    zipfian_exponent: Option<f64>,
) -> Result<()> {
    anyhow::ensure!(
        json_str(task, "operation_mix_scheduler")? == scheduler,
        "task.operation_mix_scheduler must match workload"
    );
    anyhow::ensure!(
        json_str(task, "key_selection")? == key_selection,
        "task.key_selection must match workload"
    );
    anyhow::ensure!(
        json_str(task, "scramble_function")? == scramble_function,
        "task.scramble_function must match workload"
    );
    match (task.get("zipfian_exponent"), zipfian_exponent) {
        (Some(value), Some(expected)) => {
            let actual = value
                .as_f64()
                .context("task.zipfian_exponent must be numeric")?;
            anyhow::ensure!(
                (actual - expected).abs() <= f64::EPSILON,
                "task.zipfian_exponent must match workload"
            );
        }
        (Some(value), None) => {
            anyhow::ensure!(
                value.is_null(),
                "task.zipfian_exponent must be null for this workload"
            );
        }
        (None, Some(_)) => bail!("missing `zipfian_exponent`"),
        (None, None) => {}
    }
    Ok(())
}

fn validate_task_u64_matches_params(
    task: &serde_json::Value,
    params: &serde_json::Value,
    field: &str,
) -> Result<()> {
    let task_value = json_u64(task, field)?;
    let params_value = json_u64(params, field)?;
    anyhow::ensure!(
        task_value == params_value,
        "task.{field} must match params.{field}"
    );
    Ok(())
}

fn validate_task_str_matches_params(
    task: &serde_json::Value,
    params: &serde_json::Value,
    field: &str,
) -> Result<()> {
    let task_value = json_str(task, field)?;
    let params_value = json_str(params, field)?;
    anyhow::ensure!(
        task_value == params_value,
        "task.{field} must match params.{field}"
    );
    Ok(())
}

fn validate_non_idle_throughput_json(
    throughput: &serde_json::Value,
    workload: &str,
    task: &serde_json::Value,
    completed_operations: u64,
) -> Result<()> {
    let window_secs = json_u64(throughput, "window_secs")?;
    anyhow::ensure!(window_secs == 1, "throughput.window_secs must be 1");
    let complete_windows = json_u64(throughput, "complete_windows")?;
    anyhow::ensure!(
        complete_windows > 0,
        "throughput.complete_windows must be positive"
    );
    let operations = json_object(throughput, "operations")?;
    let operations_total =
        validate_rate_window_total_json(operations, "throughput.operations", complete_windows)?;
    anyhow::ensure!(
        operations_total > 0,
        "throughput.operations.total must be greater than zero"
    );
    anyhow::ensure!(
        operations_total <= completed_operations,
        "throughput.operations.total must not exceed validation.completed_operations"
    );

    for field in ["logical_bytes", "read_logical_bytes", "write_logical_bytes"] {
        if let Some(value) = throughput.get(field) {
            validate_rate_window_json(value, &format!("throughput.{field}"), complete_windows)?;
        }
    }

    match workload {
        "point_read_missing_in_range" | "point_read_uniform" | "point_read_zipfian" => {
            let reads_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "reads",
                complete_windows,
            )?;
            ensure_throughput_rate_window_absent(throughput, workload, "writes")?;
            ensure_throughput_rate_window_absent(throughput, workload, "scan_rows")?;
            anyhow::ensure!(
                reads_total == operations_total,
                "{workload} throughput.reads.total must equal throughput.operations.total"
            );
        }
        "range_scan_uniform" => {
            let reads_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "reads",
                complete_windows,
            )?;
            require_throughput_rate_window_total(
                throughput,
                workload,
                "scan_rows",
                complete_windows,
            )?;
            ensure_throughput_rate_window_absent(throughput, workload, "writes")?;
            anyhow::ensure!(
                reads_total == operations_total,
                "range_scan_uniform throughput.reads.total must equal throughput.operations.total"
            );
        }
        "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => {
            let reads_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "reads",
                complete_windows,
            )?;
            let writes_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "writes",
                complete_windows,
            )?;
            ensure_throughput_rate_window_absent(throughput, workload, "scan_rows")?;
            let mixed_total = reads_total
                .checked_add(writes_total)
                .context("mixed throughput read/write total overflowed")?;
            anyhow::ensure!(
                mixed_total == operations_total,
                "{workload} throughput.reads.total plus throughput.writes.total must equal throughput.operations.total"
            );
        }
        "sustained_ingest" => {
            let writes_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "writes",
                complete_windows,
            )?;
            ensure_throughput_rate_window_absent(throughput, workload, "reads")?;
            ensure_throughput_rate_window_absent(throughput, workload, "scan_rows")?;
            anyhow::ensure!(
                writes_total == operations_total,
                "sustained_ingest throughput.writes.total must equal throughput.operations.total"
            );
        }
        "transaction_contention" => {
            let reads_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "reads",
                complete_windows,
            )?;
            let writes_total = require_throughput_rate_window_total(
                throughput,
                workload,
                "writes",
                complete_windows,
            )?;
            ensure_throughput_rate_window_absent(throughput, workload, "scan_rows")?;
            let transaction_reads = json_u64(task, "transaction_reads")?;
            let transaction_updates = json_u64(task, "transaction_updates")?;
            let expected_reads = operations_total
                .checked_mul(transaction_reads)
                .context("transaction throughput read total overflowed")?;
            let expected_writes = operations_total
                .checked_mul(transaction_updates)
                .context("transaction throughput write total overflowed")?;
            anyhow::ensure!(
                reads_total == expected_reads,
                "transaction_contention throughput.reads.total must equal operations.total * task.transaction_reads"
            );
            anyhow::ensure!(
                writes_total == expected_writes,
                "transaction_contention throughput.writes.total must equal operations.total * task.transaction_updates"
            );
        }
        other => bail!("unknown steady-state workload `{other}`"),
    }

    Ok(())
}

fn validate_rate_window_total_json(
    rate_window: &serde_json::Value,
    field_name: &str,
    complete_windows: u64,
) -> Result<u64> {
    validate_rate_window_json(rate_window, field_name, complete_windows)?;
    json_u64(rate_window, "total")
}

fn require_throughput_rate_window_total(
    throughput: &serde_json::Value,
    workload: &str,
    field: &str,
    complete_windows: u64,
) -> Result<u64> {
    let value = throughput
        .get(field)
        .with_context(|| format!("{workload} throughput.{field} must be present"))?;
    validate_rate_window_total_json(value, &format!("throughput.{field}"), complete_windows)
}

fn ensure_throughput_rate_window_absent(
    throughput: &serde_json::Value,
    workload: &str,
    field: &str,
) -> Result<()> {
    anyhow::ensure!(
        throughput.get(field).is_none(),
        "{workload} throughput.{field} must be absent"
    );
    Ok(())
}

fn validate_rate_window_json(
    rate_window: &serde_json::Value,
    field_name: &str,
    complete_windows: u64,
) -> Result<()> {
    anyhow::ensure!(rate_window.is_object(), "{field_name} must be an object");
    let total = json_u64(rate_window, "total")?;
    anyhow::ensure!(total > 0, "{field_name}.total must be positive");
    let avg = json_f64(rate_window, "avg_per_sec")?;
    let expected_avg = total as f64 / complete_windows as f64;
    anyhow::ensure!(
        (avg - expected_avg).abs() <= expected_avg.abs().max(1.0) * 1e-9,
        "{field_name}.avg_per_sec must equal total / complete_windows"
    );
    let min = json_f64(rate_window, "min_per_sec")?;
    let p1 = json_f64(rate_window, "p1_per_sec")?;
    let p50 = json_f64(rate_window, "p50_per_sec")?;
    let p95 = json_f64(rate_window, "p95_per_sec")?;
    let p99 = json_f64(rate_window, "p99_per_sec")?;
    let max = json_f64(rate_window, "max_per_sec")?;
    anyhow::ensure!(
        min <= p1 && p1 <= p50 && p50 <= p95 && p95 <= p99 && p99 <= max,
        "{field_name} percentile rates must be ordered"
    );
    Ok(())
}

fn validate_non_idle_latency_json(
    latency: &serde_json::Value,
    selected_operations: u64,
    clients: u64,
) -> Result<()> {
    let sample_every = json_u64(latency, "sample_every")?;
    anyhow::ensure!(sample_every > 0, "latency.sample_every must be positive");
    let samples = json_u64(latency, "samples")?;
    anyhow::ensure!(
        samples > 0,
        "latency.samples must be greater than zero for non-idle measurement rows"
    );
    let unsampled = json_u64(latency, "unsampled_completed_operations")?;
    let accounted_operations = samples
        .checked_add(unsampled)
        .context("latency sample accounting overflowed")?;
    anyhow::ensure!(
        accounted_operations == selected_operations,
        "latency samples plus unsampled operations must match validation.selected_operations"
    );
    let max_samples = max_latency_samples(selected_operations, clients, sample_every)?;
    anyhow::ensure!(
        samples <= max_samples,
        "latency.samples exceeds maximum possible samples for task.clients and latency.sample_every"
    );
    let avg = json_f64(latency, "avg_ms")?;
    let min = json_f64(latency, "min_ms")?;
    let p50 = json_f64(latency, "p50_ms")?;
    let p95 = json_f64(latency, "p95_ms")?;
    let p99 = json_f64(latency, "p99_ms")?;
    let max = json_f64(latency, "max_ms")?;
    anyhow::ensure!(
        min <= p50 && p50 <= p95 && p95 <= p99 && p99 <= max,
        "latency percentile values must be ordered"
    );
    anyhow::ensure!(
        min <= avg && avg <= max,
        "latency.avg_ms must be between latency.min_ms and latency.max_ms"
    );
    Ok(())
}

fn max_latency_samples(selected_operations: u64, clients: u64, sample_every: u64) -> Result<u64> {
    if selected_operations == 0 || clients == 0 {
        return Ok(0);
    }

    let active_clients = clients.min(selected_operations);
    let remaining_operations = selected_operations - active_clients;
    active_clients
        .checked_add(remaining_operations / sample_every)
        .context("maximum latency sample count overflowed")
}

fn validate_counter_snapshots_json(record: &serde_json::Value) -> Result<()> {
    let counter_snapshots = json_object(record, "counter_snapshots")?;
    let before: MeasurementCounters =
        serde_json::from_value(json_object(counter_snapshots, "before")?.clone())
            .context("counter_snapshots.before must match measurement counters schema")?;
    let after: MeasurementCounters =
        serde_json::from_value(json_object(counter_snapshots, "after")?.clone())
            .context("counter_snapshots.after must match measurement counters schema")?;
    validate_monotonic_counter_snapshots(&before, &after)?;
    let counters: MeasurementCounters =
        serde_json::from_value(json_object(record, "counters")?.clone())
            .context("counters must match measurement counters schema")?;
    let expected = collect_counter_delta(&before, &after);
    anyhow::ensure!(
        counters == expected,
        "counters must equal counter_snapshots.after minus counter_snapshots.before"
    );
    Ok(())
}

fn validate_monotonic_counter_snapshots(
    before: &MeasurementCounters,
    after: &MeasurementCounters,
) -> Result<()> {
    ensure_counter_monotonic(
        "block_cache_hit_count",
        before.block_cache_hit_count,
        after.block_cache_hit_count,
    )?;
    ensure_counter_monotonic(
        "block_cache_miss_count",
        before.block_cache_miss_count,
        after.block_cache_miss_count,
    )?;
    ensure_counter_monotonic(
        "block_cache_admitted_count",
        before.block_cache_admitted_count,
        after.block_cache_admitted_count,
    )?;
    ensure_counter_monotonic(
        "block_cache_rejected_count",
        before.block_cache_rejected_count,
        after.block_cache_rejected_count,
    )?;
    ensure_counter_monotonic(
        "block_cache_evicted_count",
        before.block_cache_evicted_count,
        after.block_cache_evicted_count,
    )?;
    ensure_counter_monotonic(
        "wal_commit_groups",
        before.wal_commit_groups,
        after.wal_commit_groups,
    )?;
    ensure_counter_monotonic(
        "wal_commit_solo_groups",
        before.wal_commit_solo_groups,
        after.wal_commit_solo_groups,
    )?;
    ensure_counter_monotonic(
        "wal_commit_buffers",
        before.wal_commit_buffers,
        after.wal_commit_buffers,
    )?;
    ensure_counter_monotonic(
        "wal_commit_bytes",
        before.wal_commit_bytes,
        after.wal_commit_bytes,
    )?;
    ensure_counter_monotonic(
        "value_cache_hit_count",
        before.value_cache_hit_count,
        after.value_cache_hit_count,
    )?;
    ensure_counter_monotonic(
        "value_cache_miss_count",
        before.value_cache_miss_count,
        after.value_cache_miss_count,
    )?;
    ensure_optional_counter_monotonic(
        "vlog_gc_entries_rewritten",
        before.vlog_gc_entries_rewritten,
        after.vlog_gc_entries_rewritten,
    )?;
    ensure_optional_counter_monotonic(
        "vlog_gc_bytes_rewritten",
        before.vlog_gc_bytes_rewritten,
        after.vlog_gc_bytes_rewritten,
    )?;
    ensure_optional_counter_monotonic(
        "vlog_gc_files_processed",
        before.vlog_gc_files_processed,
        after.vlog_gc_files_processed,
    )?;
    ensure_counter_monotonic(
        "compaction_filter_entries_eligible",
        before.compaction_filter_entries_eligible,
        after.compaction_filter_entries_eligible,
    )?;
    ensure_counter_monotonic(
        "compaction_filter_entries_dropped",
        before.compaction_filter_entries_dropped,
        after.compaction_filter_entries_dropped,
    )?;
    ensure_counter_monotonic(
        "compaction_filter_bytes_dropped",
        before.compaction_filter_bytes_dropped,
        after.compaction_filter_bytes_dropped,
    )?;
    ensure_counter_monotonic(
        "parallel_scan_planned_scans",
        before.parallel_scan_planned_scans,
        after.parallel_scan_planned_scans,
    )?;
    ensure_counter_monotonic(
        "parallel_scan_single_shard_fallback_scans",
        before.parallel_scan_single_shard_fallback_scans,
        after.parallel_scan_single_shard_fallback_scans,
    )?;
    ensure_counter_monotonic(
        "parallel_scan_total_shards_planned",
        before.parallel_scan_total_shards_planned,
        after.parallel_scan_total_shards_planned,
    )?;
    ensure_counter_monotonic(
        "parallel_scan_rows_emitted",
        before.parallel_scan_rows_emitted,
        after.parallel_scan_rows_emitted,
    )?;
    ensure_counter_monotonic(
        "parallel_scan_bytes_emitted",
        before.parallel_scan_bytes_emitted,
        after.parallel_scan_bytes_emitted,
    )?;
    Ok(())
}

fn ensure_counter_monotonic(field: &str, before: u64, after: u64) -> Result<()> {
    anyhow::ensure!(
        after >= before,
        "counter_snapshots.{field} must be monotonic"
    );
    Ok(())
}

fn ensure_optional_counter_monotonic(
    field: &str,
    before: Option<u64>,
    after: Option<u64>,
) -> Result<()> {
    if let (Some(before), Some(after)) = (before, after) {
        ensure_counter_monotonic(field, before, after)?;
    }
    Ok(())
}

fn validate_workload_validation_json(
    validation: &serde_json::Value,
    workload: &str,
    task: &serde_json::Value,
) -> Result<()> {
    let read_hits = json_u64(validation, "read_hits")?;
    let read_misses = json_u64(validation, "read_misses")?;
    let selected_operations = json_u64(validation, "selected_operations")?;
    let completed_operations = json_u64(validation, "completed_operations")?;
    let complete_period_operations = json_u64(validation, "complete_period_operations")?;
    let tail_operations = json_u64(validation, "tail_operations")?;
    let tail_gets = json_u64(validation, "tail_gets")?;
    let tail_puts = json_u64(validation, "tail_puts")?;

    if workload != "transaction_contention" {
        let selected_from_periods = complete_period_operations
            .checked_add(tail_operations)
            .context("validation complete-period and tail operation sum overflowed")?;
        anyhow::ensure!(
            selected_from_periods == selected_operations,
            "validation complete-period and tail operations must sum to selected_operations"
        );
        let tail_selected_operations = tail_gets
            .checked_add(tail_puts)
            .context("validation tail get/put operation sum overflowed")?;
        anyhow::ensure!(
            tail_selected_operations == tail_operations,
            "validation tail_gets plus tail_puts must equal tail_operations"
        );
    }

    match workload {
        "idle" => {
            anyhow::ensure!(read_hits == 0, "idle validation.read_hits must be zero");
            anyhow::ensure!(read_misses == 0, "idle validation.read_misses must be zero");
            anyhow::ensure!(
                selected_operations == 0,
                "idle validation.selected_operations must be zero"
            );
            anyhow::ensure!(
                completed_operations == 0,
                "idle validation.completed_operations must be zero"
            );
        }
        "point_read_uniform" | "point_read_zipfian" => {
            anyhow::ensure!(
                read_hits > 0,
                "{workload} validation.read_hits must be positive"
            );
            anyhow::ensure!(
                read_misses == 0,
                "{workload} validation.read_misses must be zero"
            );
            anyhow::ensure!(
                read_hits == completed_operations,
                "{workload} validation.read_hits must equal completed_operations"
            );
            validate_expected_u64(validation, "expected_read_hits", read_hits)?;
            validate_expected_u64(validation, "expected_read_misses", 0)?;
            anyhow::ensure!(
                selected_operations == completed_operations,
                "{workload} selected_operations must equal completed_operations"
            );
        }
        "point_read_missing_in_range" => {
            anyhow::ensure!(
                read_hits == 0,
                "point_read_missing_in_range validation.read_hits must be zero"
            );
            anyhow::ensure!(
                read_misses > 0,
                "point_read_missing_in_range validation.read_misses must be positive"
            );
            anyhow::ensure!(
                read_misses == completed_operations,
                "point_read_missing_in_range validation.read_misses must equal completed_operations"
            );
            validate_expected_u64(validation, "expected_read_hits", 0)?;
            validate_expected_u64(validation, "expected_read_misses", read_misses)?;
            anyhow::ensure!(
                selected_operations == completed_operations,
                "point_read_missing_in_range selected_operations must equal completed_operations"
            );
        }
        "range_scan_uniform" => {
            anyhow::ensure!(
                read_hits == 0,
                "range_scan_uniform validation.read_hits must be zero"
            );
            anyhow::ensure!(
                read_misses == 0,
                "range_scan_uniform validation.read_misses must be zero"
            );
            validate_expected_null(validation, "expected_read_hits")?;
            validate_expected_null(validation, "expected_read_misses")?;
            validate_scan_error_counters(validation, workload)?;
            anyhow::ensure!(
                selected_operations == completed_operations,
                "range_scan_uniform selected_operations must equal completed_operations"
            );
        }
        "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => {
            anyhow::ensure!(
                read_misses == 0,
                "{workload} validation.read_misses must be zero"
            );
            validate_expected_u64(validation, "expected_read_hits", read_hits)?;
            validate_expected_u64(validation, "expected_read_misses", 0)?;
            validate_mixed_workload_validation(validation, task, workload)?;
        }
        "sustained_ingest" => {
            anyhow::ensure!(
                read_hits == 0,
                "sustained_ingest validation.read_hits must be zero"
            );
            anyhow::ensure!(
                read_misses == 0,
                "sustained_ingest validation.read_misses must be zero"
            );
            validate_expected_null(validation, "expected_read_hits")?;
            validate_expected_null(validation, "expected_read_misses")?;
            anyhow::ensure!(
                selected_operations == completed_operations,
                "sustained_ingest selected_operations must equal completed_operations"
            );
        }
        "transaction_contention" => {
            anyhow::ensure!(
                read_misses == 0,
                "transaction_contention validation.read_misses must be zero"
            );
            let transaction_attempts = json_u64(validation, "transaction_attempts")?;
            let transaction_commits = json_u64(validation, "transaction_commits")?;
            let transaction_reads = json_u64(task, "transaction_reads")?;
            anyhow::ensure!(
                selected_operations == transaction_attempts,
                "transaction_contention selected_operations must equal transaction_attempts"
            );
            anyhow::ensure!(
                completed_operations == transaction_commits,
                "transaction_contention completed_operations must equal transaction_commits"
            );
            anyhow::ensure!(
                read_hits
                    == completed_operations
                        .checked_mul(transaction_reads)
                        .context("transaction_contention read hit expectation overflowed")?,
                "transaction_contention validation.read_hits must equal completed_operations * task.transaction_reads"
            );
            anyhow::ensure!(
                complete_period_operations == completed_operations,
                "transaction_contention complete_period_operations must equal completed_operations"
            );
            anyhow::ensure!(
                tail_operations == 0 && tail_gets == 0 && tail_puts == 0,
                "transaction_contention tail operation counters must be zero"
            );
            validate_expected_u64(validation, "expected_read_hits", read_hits)?;
            validate_expected_u64(validation, "expected_read_misses", 0)?;
        }
        other => bail!("unknown steady-state workload `{other}`"),
    }

    Ok(())
}

fn validate_scan_error_counters(validation: &serde_json::Value, workload: &str) -> Result<()> {
    for field in ["scan_count_errors", "scan_order_errors", "scan_key_errors"] {
        anyhow::ensure!(
            json_u64(validation, field)? == 0,
            "{workload} validation.{field} must be zero"
        );
    }
    Ok(())
}

fn validate_mixed_workload_validation(
    validation: &serde_json::Value,
    task: &serde_json::Value,
    workload: &str,
) -> Result<()> {
    let period = json_u64(task, "operation_mix_period")?;
    let selected_operations = json_u64(validation, "selected_operations")?;
    let completed_operations = json_u64(validation, "completed_operations")?;
    let read_hits = json_u64(validation, "read_hits")?;
    let complete_period_operations = json_u64(validation, "complete_period_operations")?;
    let tail_operations = json_u64(validation, "tail_operations")?;
    let tail_gets = json_u64(validation, "tail_gets")?;
    let tail_puts = json_u64(validation, "tail_puts")?;
    anyhow::ensure!(
        selected_operations == completed_operations,
        "{workload} selected_operations must equal completed_operations"
    );
    anyhow::ensure!(
        read_hits <= completed_operations,
        "{workload} validation.read_hits must not exceed completed_operations"
    );
    anyhow::ensure!(
        complete_period_operations % period == 0,
        "{workload} complete_period_operations must be a multiple of task.operation_mix_period"
    );
    let clients = json_u64(task, "clients")?;
    let max_tail_operations = clients
        .checked_mul(period.saturating_sub(1))
        .context("maximum mixed tail operation count overflowed")?;
    anyhow::ensure!(
        tail_operations <= max_tail_operations,
        "{workload} tail_operations must not exceed per-client task.operation_mix_period tails"
    );
    let operation_mix = json_str(task, "operation_mix")?;
    let mix = SteadyStateOperationMix::parse(
        operation_mix,
        usize::try_from(period).context("task.operation_mix_period does not fit usize")?,
    )?;
    let complete_periods = complete_period_operations / period;
    let expected_gets = complete_periods
        .checked_mul(mix.get_slots as u64)
        .and_then(|complete_gets| complete_gets.checked_add(tail_gets))
        .context("mixed workload expected get count overflowed")?;
    let expected_puts = complete_periods
        .checked_mul(mix.put_slots as u64)
        .and_then(|complete_puts| complete_puts.checked_add(tail_puts))
        .context("mixed workload expected put count overflowed")?;
    anyhow::ensure!(
        read_hits == expected_gets,
        "{workload} validation.read_hits must match task operation mix and tail_gets"
    );
    anyhow::ensure!(
        completed_operations - read_hits == expected_puts,
        "{workload} inferred put count must match task operation mix and tail_puts"
    );
    let observed_operation_mix = json_str(validation, "observed_operation_mix")?;
    let (observed_get_slots, observed_put_slots) =
        parse_observed_get_put_slots(observed_operation_mix, 1_000_000)?;
    let expected_get_slots =
        ((read_hits as f64 / completed_operations as f64) * 1_000_000.0).round() as u64;
    let expected_put_slots = 1_000_000 - expected_get_slots;
    anyhow::ensure!(
        observed_get_slots == expected_get_slots && observed_put_slots == expected_put_slots,
        "{workload} observed_operation_mix must match observed get/put counts"
    );
    Ok(())
}

fn parse_observed_get_put_slots(spec: &str, period: u64) -> Result<(u64, u64)> {
    let period_usize =
        usize::try_from(period).context("operation mix period does not fit usize")?;
    validate_operation_mix(spec, period_usize)?;
    let mut get_slots = 0u64;
    let mut put_slots = 0u64;
    for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, value) = entry
            .split_once('=')
            .with_context(|| format!("invalid --operation-mix entry: {entry}"))?;
        let name = name.trim();
        let ratio: f64 = value
            .trim()
            .parse()
            .with_context(|| format!("invalid operation ratio for {name}"))?;
        let slots = (ratio * period as f64).round() as u64;
        match name {
            "get" => get_slots += slots,
            "put" => put_slots += slots,
            other => bail!("observed mixed operation mix does not support {other}"),
        }
    }
    anyhow::ensure!(
        get_slots + put_slots == period,
        "observed mixed operation mix must contain only get and put ratios"
    );
    Ok((get_slots, put_slots))
}

fn validate_expected_u64(record: &serde_json::Value, field: &str, expected: u64) -> Result<()> {
    anyhow::ensure!(
        json_u64(record, field)? == expected,
        "validation.{field} must match observed workload count"
    );
    Ok(())
}

fn validate_expected_null(record: &serde_json::Value, field: &str) -> Result<()> {
    let value = json_required(record, field)?;
    anyhow::ensure!(
        value.is_null(),
        "validation.{field} must be null for this workload"
    );
    Ok(())
}

fn validate_validation_json(validation: &serde_json::Value) -> Result<()> {
    anyhow::ensure!(
        json_u64(validation, "errors")? == 0,
        "validation.errors must be zero"
    );
    let completed = json_u64(validation, "completed_operations")?;
    let min_completed = json_u64(validation, "min_completed_operations")?;
    anyhow::ensure!(
        completed >= min_completed,
        "validation.completed_operations must be >= min_completed_operations"
    );
    let attempts = json_u64(validation, "transaction_attempts")?;
    let commits = json_u64(validation, "transaction_commits")?;
    let conflicts = json_u64(validation, "transaction_conflicts")?;
    let reconciled_attempts = commits
        .checked_add(conflicts)
        .context("transaction commit/conflict sum overflowed")?;
    anyhow::ensure!(
        attempts == reconciled_attempts,
        "transaction attempts must equal commits plus conflicts"
    );
    Ok(())
}

fn validate_drain_json(drain: &serde_json::Value) -> Result<()> {
    let status = json_str(drain, "background_drain_status")?;
    anyhow::ensure!(
        status != "timed_out",
        "drain.background_drain_status must not be timed_out"
    );
    Ok(())
}

fn validate_golden_manifest_json(manifest: &serde_json::Value) -> Result<()> {
    let manifest: GoldenManifestRecord = serde_json::from_value(manifest.clone())
        .context("golden_manifest must match the golden manifest schema")?;
    anyhow::ensure!(
        manifest.manifest_schema == GOLDEN_MANIFEST_SCHEMA_V1,
        "unsupported golden manifest schema `{}`",
        manifest.manifest_schema
    );
    anyhow::ensure!(
        manifest.settle_status != "timed_out",
        "golden_manifest.settle_status must not be timed_out"
    );
    anyhow::ensure!(
        golden_manifest_digest_matches(&manifest)?,
        "golden_manifest.manifest_digest does not match manifest contents"
    );
    let current_options_hash = engine_options_hash(&manifest.engine_options)?;
    let legacy_options_hash = legacy_engine_options_hash(&manifest.engine_options)?;
    if manifest.engine_options_hash == current_options_hash {
        return Ok(());
    }
    anyhow::ensure!(
        !manifest.engine_options.serializable
            && manifest.engine_options_hash == legacy_options_hash
            && manifest.manifest_digest == legacy_golden_manifest_digest(&manifest)?,
        "golden_manifest.engine_options_hash does not match engine_options"
    );
    Ok(())
}

fn validate_golden_manifest_matches_record(
    manifest: &serde_json::Value,
    record: &serde_json::Value,
) -> Result<()> {
    let manifest: GoldenManifestRecord = serde_json::from_value(manifest.clone())
        .context("golden_manifest must match the golden manifest schema")?;
    let params = json_object(record, "params")?;
    let num = json_u64(params, "num")?;
    anyhow::ensure!(
        num == manifest.key_count as u64,
        "params.num must match golden_manifest.key_count"
    );
    let value_size = json_u64(params, "value_size")?;
    anyhow::ensure!(
        value_size == manifest.value_size as u64,
        "params.value_size must match golden_manifest.value_size"
    );
    let row_options: EngineOptionsRecord =
        serde_json::from_value(json_required(record, "engine_options")?.clone())
            .context("engine_options must match the engine options schema")?;
    anyhow::ensure!(
        row_options == manifest.engine_options,
        "engine_options must match golden_manifest.engine_options"
    );
    Ok(())
}

fn json_required<'a>(record: &'a serde_json::Value, field: &str) -> Result<&'a serde_json::Value> {
    record
        .get(field)
        .with_context(|| format!("missing `{field}`"))
}

fn json_object<'a>(record: &'a serde_json::Value, field: &str) -> Result<&'a serde_json::Value> {
    let value = json_required(record, field)?;
    anyhow::ensure!(value.is_object(), "`{field}` must be an object");
    Ok(value)
}

fn json_str<'a>(record: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    json_required(record, field)?
        .as_str()
        .with_context(|| format!("`{field}` must be a string"))
}

fn json_optional_str<'a>(record: &'a serde_json::Value, field: &str) -> Result<Option<&'a str>> {
    let Some(value) = record.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(Some)
        .with_context(|| format!("`{field}` must be a string"))
}

fn json_u64(record: &serde_json::Value, field: &str) -> Result<u64> {
    json_required(record, field)?
        .as_u64()
        .with_context(|| format!("`{field}` must be an unsigned integer"))
}

fn json_f64(record: &serde_json::Value, field: &str) -> Result<f64> {
    let value = json_required(record, field)?
        .as_f64()
        .with_context(|| format!("`{field}` must be a number"))?;
    anyhow::ensure!(
        value.is_finite() && value >= 0.0,
        "`{field}` must be a finite non-negative number"
    );
    Ok(value)
}

fn json_bool(record: &serde_json::Value, field: &str) -> Result<bool> {
    json_required(record, field)?
        .as_bool()
        .with_context(|| format!("`{field}` must be a boolean"))
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
                    "mixed steady-state workloads support only get and put in --operation-mix, got {other}"
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
            "mixed steady-state workloads require non-zero get and put ratios"
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
    UniformMissingInRange { record_count: u64 },
    ScrambledZipfian { sampler: Arc<ZipfianSampler> },
}

impl SteadyStateKeySelector {
    fn uniform(record_count: usize) -> Self {
        Self::Uniform {
            record_count: record_count as u64,
        }
    }

    fn uniform_missing_in_range(record_count: usize) -> Self {
        Self::UniformMissingInRange {
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
            Self::Uniform { record_count } | Self::UniformMissingInRange { record_count } => {
                rng.gen_range(0..*record_count)
            }
            Self::ScrambledZipfian { sampler } => sampler.sample(rng),
        }
    }

    fn key(&self, rng: &mut ChaCha12Rng) -> [u8; 20] {
        let id = self.sample(rng);
        match self {
            Self::UniformMissingInRange { .. } => steady_state_missing_key(id),
            Self::Uniform { .. } | Self::ScrambledZipfian { .. } => steady_state_loaded_key(id),
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
    transaction_attempts: u64,
    transaction_commits: u64,
    transaction_conflicts: u64,
    transaction_errors: u64,
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
    rate_windows: Vec<SteadyStateRateWindow>,
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
        self.transaction_attempts += other.transaction_attempts;
        self.transaction_commits += other.transaction_commits;
        self.transaction_conflicts += other.transaction_conflicts;
        self.transaction_errors += other.transaction_errors;
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
        if self.rate_windows.len() < other.rate_windows.len() {
            self.rate_windows
                .resize(other.rate_windows.len(), SteadyStateRateWindow::default());
        }
        for (idx, other_window) in other.rate_windows.into_iter().enumerate() {
            self.rate_windows[idx].merge(other_window);
        }
    }
}

#[derive(Clone, Default)]
struct SteadyStateRateWindow {
    operations: u64,
    reads: u64,
    writes: u64,
    logical_bytes: u64,
    read_logical_bytes: u64,
    write_logical_bytes: u64,
    scan_rows: u64,
}

impl SteadyStateRateWindow {
    fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        self.reads += other.reads;
        self.writes += other.writes;
        self.logical_bytes += other.logical_bytes;
        self.read_logical_bytes += other.read_logical_bytes;
        self.write_logical_bytes += other.write_logical_bytes;
        self.scan_rows += other.scan_rows;
    }
}

fn new_steady_state_rate_windows(duration: Duration) -> Vec<SteadyStateRateWindow> {
    vec![SteadyStateRateWindow::default(); duration.as_secs() as usize]
}

fn record_steady_state_rate_window(
    windows: &mut [SteadyStateRateWindow],
    bucket: usize,
    delta: SteadyStateRateWindow,
) {
    if let Some(window) = windows.get_mut(bucket) {
        window.merge(delta);
    }
}

fn wait_for_steady_state_window_start(
    start_barrier: &Barrier,
    window_start: &Mutex<Option<Instant>>,
) -> Instant {
    start_barrier.wait();
    (*window_start.lock()).expect("steady-state window start set before worker release")
}

fn release_steady_state_workers(start_barrier: &Barrier, window_start: &Mutex<Option<Instant>>) {
    *window_start.lock() = Some(Instant::now());
    start_barrier.wait();
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
    let start_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    let window_start = Arc::new(Mutex::new(None));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let selector = selector.clone();
        let start_barrier = start_barrier.clone();
        let window_start = window_start.clone();
        let latency_sample_every = cfg.latency_sample_every.filter(|_| sample_latency);
        let seed = steady_state_stream_seed(cfg.seed, phase.stream_label(), client_id as u64);
        handles.push(std::thread::spawn(
            move || -> Result<SteadyStateWindowStats> {
                let mut key_rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
                    seed,
                    STEADY_STATE_KEY_STREAM_LABEL,
                    0,
                ));
                let mut stats = SteadyStateWindowStats {
                    rate_windows: new_steady_state_rate_windows(duration),
                    ..SteadyStateWindowStats::default()
                };
                let window_start =
                    wait_for_steady_state_window_start(&start_barrier, &window_start);
                while !stop.load(Ordering::Relaxed) {
                    let rate_window_bucket = window_start.elapsed().as_secs() as usize;
                    let key = selector.key(&mut key_rng);
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats
                            .selected_operations
                            .is_multiple_of(sample_every as u64)
                    });
                    let op_start = should_sample.then(Instant::now);
                    let value = eng.get(&key)?;
                    let logical_bytes = key.len() as u64
                        + value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
                    match value {
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
                    record_steady_state_rate_window(
                        &mut stats.rate_windows,
                        rate_window_bucket,
                        SteadyStateRateWindow {
                            operations: 1,
                            reads: 1,
                            logical_bytes,
                            read_logical_bytes: logical_bytes,
                            ..SteadyStateRateWindow::default()
                        },
                    );
                }
                stats.complete_period_gets = stats.selected_gets;
                stats.complete_period_puts = 0;
                stats.complete_period_operations = stats.selected_operations;
                Ok(stats)
            },
        ));
    }

    release_steady_state_workers(&start_barrier, &window_start);
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
    let start_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    let window_start = Arc::new(Mutex::new(None));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let start_barrier = start_barrier.clone();
        let window_start = window_start.clone();
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
                let mut stats = SteadyStateWindowStats {
                    rate_windows: new_steady_state_rate_windows(duration),
                    ..SteadyStateWindowStats::default()
                };
                let window_start =
                    wait_for_steady_state_window_start(&start_barrier, &window_start);
                let mut previous_key = Vec::with_capacity(20);
                while !stop.load(Ordering::Relaxed) {
                    let rate_window_bucket = window_start.elapsed().as_secs() as usize;
                    let start_id = key_rng.gen_range(0..record_count);
                    let start_key = steady_state_loaded_key(start_id);
                    let expected_rows = scan_limit.min((record_count - start_id) as usize);
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats
                            .selected_operations
                            .is_multiple_of(sample_every as u64)
                    });
                    let op_start = should_sample.then(Instant::now);

                    let mut rows = 0usize;
                    let mut logical_bytes = 0u64;
                    previous_key.clear();
                    let mut iter = eng.scan(Bound::Included(&start_key), Bound::Unbounded)?;
                    while iter.is_valid() && rows < scan_limit {
                        let key = iter.key();
                        logical_bytes += key.len() as u64 + iter.value().len() as u64;
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
                    record_steady_state_rate_window(
                        &mut stats.rate_windows,
                        rate_window_bucket,
                        SteadyStateRateWindow {
                            operations: 1,
                            reads: 1,
                            logical_bytes,
                            read_logical_bytes: logical_bytes,
                            scan_rows: rows as u64,
                            ..SteadyStateRateWindow::default()
                        },
                    );
                }
                stats.complete_period_gets = 0;
                stats.complete_period_puts = 0;
                stats.complete_period_operations = stats.selected_operations;
                Ok(stats)
            },
        ));
    }

    release_steady_state_workers(&start_barrier, &window_start);
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

fn run_steady_state_ingest_window(
    engine: &Arc<KvEngine>,
    cfg: &HarnessConfig,
    next_key_id: &Arc<AtomicU64>,
    operation_mix_period: usize,
    duration: Duration,
    phase: SteadyStateWindowPhase,
    sample_latency: bool,
) -> Result<SteadyStateWindowStats> {
    debug_assert!(operation_mix_period > 0);
    let stop = Arc::new(AtomicBool::new(false));
    let start_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    let window_start = Arc::new(Mutex::new(None));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let next_key_id = next_key_id.clone();
        let start_barrier = start_barrier.clone();
        let window_start = window_start.clone();
        let value_size = cfg.value_size;
        let latency_sample_every = cfg.latency_sample_every.filter(|_| sample_latency);
        let seed = steady_state_stream_seed(cfg.seed, phase.stream_label(), client_id as u64);
        let operation_mix_period = operation_mix_period as u64;
        handles.push(std::thread::spawn(
            move || -> Result<SteadyStateWindowStats> {
                let mut value_rng = ChaCha12Rng::seed_from_u64(steady_state_stream_seed(
                    seed,
                    STEADY_STATE_VALUE_STREAM_LABEL,
                    0,
                ));
                let mut value = vec![0u8; value_size];
                let mut stats = SteadyStateWindowStats {
                    rate_windows: new_steady_state_rate_windows(duration),
                    ..SteadyStateWindowStats::default()
                };
                let window_start =
                    wait_for_steady_state_window_start(&start_barrier, &window_start);
                while !stop.load(Ordering::Relaxed) {
                    let rate_window_bucket = window_start.elapsed().as_secs() as usize;
                    let key_id = next_key_id.fetch_add(1, Ordering::Relaxed);
                    let key = steady_state_loaded_key(key_id);
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats
                            .selected_operations
                            .is_multiple_of(sample_every as u64)
                    });
                    let op_start = should_sample.then(Instant::now);

                    value_rng.fill_bytes(&mut value);
                    eng.put(&key, &value)?;
                    stats.selected_puts += 1;
                    stats.writes += 1;
                    stats.selected_operations += 1;
                    stats.completed_operations += 1;
                    if let Some(op_start) = op_start {
                        stats
                            .latency_samples_ns
                            .push(op_start.elapsed().as_nanos() as u64);
                    }
                    record_steady_state_rate_window(
                        &mut stats.rate_windows,
                        rate_window_bucket,
                        SteadyStateRateWindow {
                            operations: 1,
                            writes: 1,
                            logical_bytes: key.len() as u64 + value.len() as u64,
                            write_logical_bytes: key.len() as u64 + value.len() as u64,
                            ..SteadyStateRateWindow::default()
                        },
                    );
                }
                let complete_period_operations =
                    stats.selected_operations / operation_mix_period * operation_mix_period;
                stats.complete_period_puts = complete_period_operations;
                stats.complete_period_operations = complete_period_operations;
                stats.tail_puts = stats.selected_puts - complete_period_operations;
                stats.tail_operations = stats.selected_operations - complete_period_operations;
                Ok(stats)
            },
        ));
    }

    release_steady_state_workers(&start_barrier, &window_start);
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let mut stats = SteadyStateWindowStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| anyhow!("steady-state ingest worker thread panicked"))??;
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
    let start_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    let window_start = Arc::new(Mutex::new(None));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let sampler = sampler.clone();
        let start_barrier = start_barrier.clone();
        let window_start = window_start.clone();
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
                let mut stats = SteadyStateWindowStats {
                    rate_windows: new_steady_state_rate_windows(duration),
                    ..SteadyStateWindowStats::default()
                };
                let window_start =
                    wait_for_steady_state_window_start(&start_barrier, &window_start);
                let mut schedule_idx = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let rate_window_bucket = window_start.elapsed().as_secs() as usize;
                    let operation = schedule[schedule_idx];
                    schedule_idx = (schedule_idx + 1) % schedule.len();
                    let key = steady_state_loaded_key(sampler.sample(&mut key_rng));
                    let should_sample = latency_sample_every.is_some_and(|sample_every| {
                        stats
                            .selected_operations
                            .is_multiple_of(sample_every as u64)
                    });
                    let op_start = should_sample.then(Instant::now);
                    match operation {
                        SteadyStateOperation::Get => {
                            stats.selected_gets += 1;
                            let value = eng.get(&key)?;
                            let logical_bytes = key.len() as u64
                                + value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
                            match value {
                                Some(_) => stats.read_hits += 1,
                                None => stats.read_misses += 1,
                            }
                            stats.reads += 1;
                            record_steady_state_rate_window(
                                &mut stats.rate_windows,
                                rate_window_bucket,
                                SteadyStateRateWindow {
                                    operations: 1,
                                    reads: 1,
                                    logical_bytes,
                                    read_logical_bytes: logical_bytes,
                                    ..SteadyStateRateWindow::default()
                                },
                            );
                        }
                        SteadyStateOperation::Put => {
                            stats.selected_puts += 1;
                            value_rng.fill_bytes(&mut value);
                            eng.put(&key, &value)?;
                            stats.writes += 1;
                            record_steady_state_rate_window(
                                &mut stats.rate_windows,
                                rate_window_bucket,
                                SteadyStateRateWindow {
                                    operations: 1,
                                    writes: 1,
                                    logical_bytes: key.len() as u64 + value.len() as u64,
                                    write_logical_bytes: key.len() as u64 + value.len() as u64,
                                    ..SteadyStateRateWindow::default()
                                },
                            );
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

    release_steady_state_workers(&start_barrier, &window_start);
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

fn is_serializable_conflict(error: &anyhow::Error) -> bool {
    error.to_string().contains("serializable conflict:")
}

fn run_steady_state_transaction_window(
    engine: &Arc<KvEngine>,
    cfg: &HarnessConfig,
    duration: Duration,
    phase: SteadyStateWindowPhase,
    sample_latency: bool,
) -> Result<SteadyStateWindowStats> {
    let stop = Arc::new(AtomicBool::new(false));
    let start_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    let window_start = Arc::new(Mutex::new(None));
    let mut handles = Vec::with_capacity(cfg.clients);
    for client_id in 0..cfg.clients {
        let eng = engine.clone();
        let stop = stop.clone();
        let start_barrier = start_barrier.clone();
        let window_start = window_start.clone();
        let hot_set = cfg.transaction_hot_set as u64;
        let transaction_reads = cfg.transaction_reads;
        let transaction_updates = cfg.transaction_updates;
        let transaction_retries = cfg.transaction_retries;
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
                let mut operations = Vec::with_capacity(transaction_reads + transaction_updates);
                let mut value = vec![0u8; value_size];
                let mut stats = SteadyStateWindowStats {
                    rate_windows: new_steady_state_rate_windows(duration),
                    ..SteadyStateWindowStats::default()
                };
                let window_start =
                    wait_for_steady_state_window_start(&start_barrier, &window_start);
                while !stop.load(Ordering::Relaxed) {
                    let mut retries_left = transaction_retries;
                    loop {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let rate_window_bucket = window_start.elapsed().as_secs() as usize;
                        let should_sample = latency_sample_every.is_some_and(|sample_every| {
                            stats
                                .selected_operations
                                .is_multiple_of(sample_every as u64)
                        });
                        let op_start = should_sample.then(Instant::now);
                        operations.clear();
                        operations.extend(std::iter::repeat_n(
                            SteadyStateOperation::Get,
                            transaction_reads,
                        ));
                        operations.extend(std::iter::repeat_n(
                            SteadyStateOperation::Put,
                            transaction_updates,
                        ));
                        operations.shuffle(&mut value_rng);

                        let txn = eng.new_txn()?;
                        let mut read_hits = 0u64;
                        let mut read_misses = 0u64;
                        let mut writes = 0u64;
                        let mut logical_bytes = 0u64;
                        for operation in &operations {
                            let key = steady_state_loaded_key(key_rng.gen_range(0..hot_set));
                            match operation {
                                SteadyStateOperation::Get => {
                                    let value = txn.get(&key)?;
                                    logical_bytes += key.len() as u64
                                        + value
                                            .as_ref()
                                            .map(|value| value.len() as u64)
                                            .unwrap_or(0);
                                    if value.is_some() {
                                        read_hits += 1;
                                    } else {
                                        read_misses += 1;
                                    }
                                }
                                SteadyStateOperation::Put => {
                                    value_rng.fill_bytes(&mut value);
                                    txn.put(&key, &value)?;
                                    logical_bytes += key.len() as u64 + value.len() as u64;
                                    writes += 1;
                                }
                            }
                        }

                        stats.transaction_attempts += 1;
                        stats.selected_operations += 1;
                        let should_retry = match txn.commit() {
                            Ok(()) => {
                                stats.transaction_commits += 1;
                                stats.completed_operations += 1;
                                stats.reads += read_hits + read_misses;
                                stats.read_hits += read_hits;
                                stats.read_misses += read_misses;
                                stats.writes += writes;
                                record_steady_state_rate_window(
                                    &mut stats.rate_windows,
                                    rate_window_bucket,
                                    SteadyStateRateWindow {
                                        operations: 1,
                                        reads: read_hits + read_misses,
                                        writes,
                                        logical_bytes,
                                        read_logical_bytes: read_hits * (20 + value_size as u64),
                                        write_logical_bytes: writes * (20 + value_size as u64),
                                        ..SteadyStateRateWindow::default()
                                    },
                                );
                                false
                            }
                            Err(error) if is_serializable_conflict(&error) => {
                                stats.transaction_conflicts += 1;
                                retries_left > 0
                            }
                            Err(error) => {
                                stats.transaction_errors += 1;
                                return Err(error.context("transaction_contention commit failed"));
                            }
                        };
                        if let Some(op_start) = op_start {
                            stats
                                .latency_samples_ns
                                .push(op_start.elapsed().as_nanos() as u64);
                        }
                        if should_retry {
                            retries_left -= 1;
                        } else {
                            break;
                        }
                    }
                }
                stats.selected_gets = stats.reads;
                stats.selected_puts = stats.writes;
                stats.complete_period_gets = stats.reads;
                stats.complete_period_puts = stats.writes;
                stats.complete_period_operations = stats.completed_operations;
                Ok(stats)
            },
        ));
    }

    release_steady_state_workers(&start_barrier, &window_start);
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let mut stats = SteadyStateWindowStats::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| anyhow!("steady-state transaction worker thread panicked"))??;
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

fn throughput_record(stats: &SteadyStateWindowStats) -> Option<ThroughputRecord> {
    if stats.rate_windows.is_empty() {
        return None;
    }

    Some(ThroughputRecord {
        window_secs: 1,
        complete_windows: stats.rate_windows.len() as u64,
        operations: rate_window_record(stats.rate_windows.iter().map(|window| window.operations)),
        reads: rate_window_record(stats.rate_windows.iter().map(|window| window.reads)),
        writes: rate_window_record(stats.rate_windows.iter().map(|window| window.writes)),
        logical_bytes: rate_window_record(
            stats.rate_windows.iter().map(|window| window.logical_bytes),
        ),
        read_logical_bytes: rate_window_record(
            stats
                .rate_windows
                .iter()
                .map(|window| window.read_logical_bytes),
        ),
        write_logical_bytes: rate_window_record(
            stats
                .rate_windows
                .iter()
                .map(|window| window.write_logical_bytes),
        ),
        scan_rows: rate_window_record(stats.rate_windows.iter().map(|window| window.scan_rows)),
    })
}

fn rate_window_record(values: impl Iterator<Item = u64>) -> Option<RateWindowRecord> {
    let mut sorted: Vec<_> = values.collect();
    let total: u64 = sorted.iter().sum();
    if total == 0 {
        return None;
    }

    sorted.sort_unstable();
    let len = sorted.len();
    Some(RateWindowRecord {
        total,
        avg_per_sec: total as f64 / len as f64,
        p1_per_sec: percentile_rate(&sorted, 0.01),
        p50_per_sec: percentile_rate(&sorted, 0.50),
        p95_per_sec: percentile_rate(&sorted, 0.95),
        p99_per_sec: percentile_rate(&sorted, 0.99),
        min_per_sec: sorted[0] as f64,
        max_per_sec: sorted[len - 1] as f64,
    })
}

fn percentile_rate(sorted_values: &[u64], percentile: f64) -> f64 {
    debug_assert!(!sorted_values.is_empty());
    let idx = ((sorted_values.len() - 1) as f64 * percentile).round() as usize;
    sorted_values[idx] as f64
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
        transaction_hot_set: None,
        transaction_reads: None,
        transaction_updates: None,
        transaction_retries: None,
        transaction_conflict_latency: None,
    }
}

fn steady_state_validation_record(
    stats: &SteadyStateWindowStats,
    observed_operation_mix: String,
) -> ValidationRecord {
    ValidationRecord {
        errors: stats.scan_count_errors
            + stats.scan_order_errors
            + stats.scan_key_errors
            + stats.transaction_errors,
        read_hits: stats.read_hits,
        read_misses: stats.read_misses,
        expected_read_hits: Some(stats.reads),
        expected_read_misses: Some(0),
        observed_operation_mix,
        scan_count_errors: stats.scan_count_errors,
        scan_order_errors: stats.scan_order_errors,
        scan_key_errors: stats.scan_key_errors,
        transaction_attempts: stats.transaction_attempts,
        transaction_commits: stats.transaction_commits,
        transaction_conflicts: stats.transaction_conflicts,
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
    if let Some(operation_mix) = &cfg.operation_mix {
        validate_read_only_operation_mix(operation_mix, cfg.operation_mix_period)?;
    }

    let selector = SteadyStateKeySelector::uniform_missing_in_range(cfg.num);
    let opened = open_loaded_steady_state_keyspace(cfg, workload)?;

    if cfg.warmup_secs > 0 {
        let warmup = run_steady_state_read_window(
            &opened.engine,
            cfg,
            selector.clone(),
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        anyhow::ensure!(
            warmup.read_hits == 0,
            "{workload} expected warmup reads to miss, got {} hits",
            warmup.read_hits
        );
    }

    let baseline = collect_counters(&opened.engine)?;
    let start = Instant::now();
    let window = run_steady_state_read_window(
        &opened.engine,
        cfg,
        selector,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    anyhow::ensure!(
        window.completed_operations >= 1,
        "{workload} completed no measured operations"
    );
    anyhow::ensure!(
        window.read_hits == 0,
        "point_read_missing_in_range expected 0 found entries, got {}",
        window.read_hits
    );
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &opened.path, &opened.engine, &baseline)?;

    let mut measurement = make_measurement(
        cfg,
        workload,
        "negative_point_get",
        &opened.options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some("uniform_absent_reserved_padding"),
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
            load_elapsed_ms: Some(ms(opened.load_elapsed)),
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
        "uniform_absent_reserved_padding",
        None,
    ));
    measurement.record.latency = cfg.latency_sample_every.map(|sample_every| {
        latency_record(
            sample_every,
            window.completed_operations,
            &window.latency_samples_ns,
        )
    });
    measurement.record.throughput = throughput_record(&window);
    measurement.record.golden_manifest = opened.golden_manifest;
    let mut validation =
        steady_state_validation_record(&window, "get=1.000000,put=0.000000".to_string());
    validation.expected_read_hits = Some(0);
    validation.expected_read_misses = Some(window.reads);
    measurement.record.validation = Some(validation);
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.counter_snapshots = Some(counter_snapshots);

    Ok(vec![measurement])
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

struct OpenedSteadyStateKeyspace {
    path: PathBuf,
    options: LsmStorageOptions,
    engine: Arc<KvEngine>,
    load_elapsed: Duration,
    golden_manifest: Option<GoldenManifestRecord>,
}

fn prepare_steady_state_golden(cfg: &HarnessConfig) -> Result<BenchMeasurement> {
    let workload = "prepare_golden";
    let path = cfg
        .golden_path
        .as_ref()
        .context("--prepare-golden requires --golden-path")?;
    validate_golden_clone_paths(path, &cfg.base_path)?;
    remove_path(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let options = cfg.build_options(false, false);
    let source_path = checkpoint_clone_source_path(path);
    remove_path(&source_path)?;
    let engine = KvEngine::open(&source_path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in 0..cfg.num as u64 {
        engine.put(&steady_state_loaded_key(i), &value)?;
    }
    let load_elapsed = load_start.elapsed();
    let settle_start = Instant::now();
    engine.drain_flush()?;
    let settle_elapsed = settle_start.elapsed();
    let settle_timed_out = cfg
        .settle_timeout_secs
        .is_some_and(|timeout_secs| settle_elapsed > Duration::from_secs(timeout_secs));
    let level_layout = engine.dump_structure_string();
    let counters = collect_counters(&engine)?;
    let checkpoint_result = engine.create_checkpoint_with_options(
        path,
        CheckpointOptions {
            use_hard_links: true,
            include_vlog_indexes: true,
            ..CheckpointOptions::default()
        },
    );
    let close_result = engine.close();
    let cleanup_result = remove_path(&source_path);
    checkpoint_result.with_context(|| {
        format!(
            "failed to checkpoint prepared golden database from {} to {}",
            source_path.display(),
            path.display()
        )
    })?;
    close_result?;
    cleanup_result?;
    remove_file_if_exists(&checkpoint_target_lock_path(path))?;
    let manifest = build_golden_manifest_record(
        path,
        cfg,
        &options,
        level_layout,
        if settle_timed_out {
            "timed_out"
        } else {
            "settled"
        },
        settle_elapsed,
    )?;
    write_golden_manifest(path, manifest.clone())?;
    anyhow::ensure!(
        !settle_timed_out,
        "golden preparation timed out after {:.3} ms",
        ms(settle_elapsed)
    );

    let mut measurement = make_measurement(
        cfg,
        workload,
        "bulk_load",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("none"),
            seed_derivation: None,
            key_selection: Some("ordered"),
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(load_elapsed)),
            measure_elapsed_ms: ms(load_elapsed + settle_elapsed),
            ops: Some(cfg.num as u64),
            ops_per_sec: Some(rate(cfg.num as u64, load_elapsed)),
            writes: Some(cfg.num as u64),
            writes_per_sec: Some(rate(cfg.num as u64, load_elapsed)),
            entries: Some(cfg.num as u64),
            entries_per_sec: Some(rate(cfg.num as u64, load_elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    );
    measurement.record.phase = Some("prepare");
    measurement.record.golden_manifest = Some(manifest);
    measurement.record.drain = Some(DrainRecord {
        flush_drain_ms: ms(settle_elapsed),
        background_drain_ms: None,
        background_drain_status: "not_requested",
    });

    Ok(measurement)
}

fn open_loaded_steady_state_keyspace(
    cfg: &HarnessConfig,
    workload: &str,
) -> Result<OpenedSteadyStateKeyspace> {
    let path = cfg.path_for(workload);
    let options = cfg.build_options(false, false);
    if let Some(golden_path) = &cfg.golden_path {
        validate_golden_clone_paths(golden_path, &path)?;
        remove_path(&path)?;
        let start = Instant::now();
        let golden_manifest = read_and_validate_golden_manifest(cfg, golden_path, &options)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        copy_dir_recursively(golden_path, &path)?;
        let engine = KvEngine::open(&path, options.clone())?;
        return Ok(OpenedSteadyStateKeyspace {
            path,
            options,
            engine,
            load_elapsed: start.elapsed(),
            golden_manifest: Some(golden_manifest),
        });
    }

    let path = prepare_path(cfg, workload)?;
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in 0..cfg.num as u64 {
        engine.put(&steady_state_loaded_key(i), &value)?;
    }
    engine.drain_flush()?;

    Ok(OpenedSteadyStateKeyspace {
        path,
        options,
        engine,
        load_elapsed: load_start.elapsed(),
        golden_manifest: None,
    })
}

fn finish_steady_state_measurement(
    cfg: &HarnessConfig,
    path: &Path,
    engine: &Arc<KvEngine>,
    baseline: &MeasurementCounters,
) -> Result<(Duration, MeasurementCounters, CounterSnapshotsRecord)> {
    let after = collect_counters(engine)?;
    let counters = collect_counter_delta(baseline, &after);
    let drain_start = Instant::now();
    engine.drain_flush()?;
    let flush_drain = drain_start.elapsed();
    engine.close()?;
    finalize_path(cfg, path)?;

    Ok((
        flush_drain,
        counters,
        CounterSnapshotsRecord {
            before: baseline.clone(),
            after,
        },
    ))
}

fn golden_manifest_path(path: &Path) -> PathBuf {
    path.join("steady-state-golden-manifest.json")
}

fn build_golden_manifest_record(
    path: &Path,
    cfg: &HarnessConfig,
    options: &LsmStorageOptions,
    level_layout: String,
    settle_status: &str,
    settle_elapsed: Duration,
) -> Result<GoldenManifestRecord> {
    let engine_options = engine_options_record(options);
    let mut manifest = GoldenManifestRecord {
        manifest_schema: GOLDEN_MANIFEST_SCHEMA_V1.to_string(),
        manifest_path: golden_manifest_path(path).display().to_string(),
        manifest_digest: String::new(),
        key_count: cfg.num,
        value_size: cfg.value_size,
        key_format: "be_u64_plus_ascii_zero_padding_20b".to_string(),
        engine_options_hash: engine_options_hash(&engine_options)?,
        engine_options,
        source_commit: source_commit(),
        sst_file_count: count_files_with_extension(path, "sst")?,
        vlog_file_count: count_files_with_extension(&path.join("vlog"), "vlog")?,
        level_layout,
        settle_status: settle_status.to_string(),
        settle_elapsed_ms: ms(settle_elapsed),
        settle_timeout_secs: cfg.settle_timeout_secs,
    };
    manifest.manifest_digest = golden_manifest_digest(&manifest)?;

    Ok(manifest)
}

fn write_golden_manifest(path: &Path, manifest: GoldenManifestRecord) -> Result<()> {
    let manifest_path = golden_manifest_path(path);
    let contents = serde_json::to_vec_pretty(&manifest)?;
    fs::write(&manifest_path, contents)
        .with_context(|| format!("failed to write {}", manifest_path.display()))
}

fn validate_golden_clone_paths(golden_path: &Path, target_path: &Path) -> Result<()> {
    let source = absolute_lexical_path(golden_path)?;
    let target = absolute_lexical_path(target_path)?;
    anyhow::ensure!(
        source != target,
        "workload path {} must differ from --golden-path {}",
        target_path.display(),
        golden_path.display()
    );
    anyhow::ensure!(
        !target.starts_with(&source),
        "workload path {} must not be inside --golden-path {}",
        target_path.display(),
        golden_path.display()
    );
    anyhow::ensure!(
        !source.starts_with(&target),
        "--golden-path {} must not be inside workload path {}",
        golden_path.display(),
        target_path.display()
    );

    Ok(())
}

fn absolute_lexical_path(path: &Path) -> Result<PathBuf> {
    let raw_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("failed to read current directory")?
            .join(path)
    };

    let components: Vec<_> = raw_path.components().collect();
    let mut candidate = PathBuf::new();
    let mut existing_prefix = PathBuf::new();
    let mut remaining_start = 0usize;
    for (idx, component) in components.iter().copied().enumerate() {
        push_lexical_component(&mut candidate, component);
        if candidate.exists() {
            existing_prefix = fs::canonicalize(&candidate)
                .with_context(|| format!("failed to canonicalize {}", candidate.display()))?;
            remaining_start = idx + 1;
        }
    }

    let mut normalized = existing_prefix;
    for component in components.into_iter().skip(remaining_start) {
        push_lexical_component(&mut normalized, component);
    }

    Ok(normalized)
}

fn push_lexical_component(path: &mut PathBuf, component: Component<'_>) {
    match component {
        Component::Prefix(prefix) => path.push(prefix.as_os_str()),
        Component::RootDir => path.push(component.as_os_str()),
        Component::CurDir => {}
        Component::ParentDir => {
            path.pop();
        }
        Component::Normal(part) => path.push(part),
    }
}

fn read_and_validate_golden_manifest(
    cfg: &HarnessConfig,
    golden_path: &Path,
    options: &LsmStorageOptions,
) -> Result<GoldenManifestRecord> {
    let manifest_path = golden_manifest_path(golden_path);
    let contents = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest: GoldenManifestRecord = serde_json::from_slice(&contents)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
    anyhow::ensure!(
        golden_manifest_digest_matches(&manifest)?,
        "golden manifest digest does not match manifest contents"
    );
    anyhow::ensure!(
        manifest.manifest_schema == GOLDEN_MANIFEST_SCHEMA_V1,
        "unsupported golden manifest schema {}",
        manifest.manifest_schema
    );
    let expected_options = engine_options_record(options);
    anyhow::ensure!(
        manifest.key_count == cfg.num,
        "golden key_count {} does not match requested {}",
        manifest.key_count,
        cfg.num
    );
    anyhow::ensure!(
        manifest.value_size == cfg.value_size,
        "golden value_size {} does not match requested {}",
        manifest.value_size,
        cfg.value_size
    );
    anyhow::ensure!(
        manifest.key_format == "be_u64_plus_ascii_zero_padding_20b",
        "unsupported golden key_format {}",
        manifest.key_format
    );
    let current_source_commit = source_commit();
    anyhow::ensure!(
        manifest.source_commit != "unknown" && current_source_commit != "unknown",
        "golden source_commit must be known for --clone-golden validation"
    );
    anyhow::ensure!(
        manifest.source_commit == current_source_commit,
        "golden source_commit {} does not match current source_commit {}",
        manifest.source_commit,
        current_source_commit
    );
    anyhow::ensure!(
        manifest.engine_options == expected_options,
        "golden engine options do not match requested options"
    );
    anyhow::ensure!(
        golden_manifest_engine_options_hash_matches(&manifest, &expected_options)?,
        "golden engine options hash does not match requested options"
    );
    anyhow::ensure!(
        manifest.settle_status == "settled",
        "golden database is not settled: {}",
        manifest.settle_status
    );

    Ok(manifest)
}

fn engine_options_record(options: &LsmStorageOptions) -> EngineOptionsRecord {
    EngineOptionsRecord {
        wal: options.enable_wal,
        serializable: options.serializable,
        value_separation: options
            .value_separation
            .as_ref()
            .is_some_and(|vlog| vlog.enabled),
        compaction: compaction_name(&options.compaction_options).to_string(),
        target_sst_size: options.target_sst_size,
        memtable_limit: options.num_memtable_limit,
        cache_capacity: options.block_cache_capacity,
    }
}

fn engine_options_hash(options: &EngineOptionsRecord) -> Result<String> {
    Ok(stable_digest(&serde_json::to_vec(options)?))
}

fn legacy_engine_options_hash(options: &EngineOptionsRecord) -> Result<String> {
    let legacy = LegacyEngineOptionsRecord::from(options);
    Ok(stable_digest(&serde_json::to_vec(&legacy)?))
}

fn golden_manifest_engine_options_hash_matches(
    manifest: &GoldenManifestRecord,
    expected_options: &EngineOptionsRecord,
) -> Result<bool> {
    if manifest.engine_options_hash == engine_options_hash(expected_options)? {
        return Ok(true);
    }
    Ok(!expected_options.serializable
        && !manifest.engine_options.serializable
        && manifest.engine_options_hash == legacy_engine_options_hash(expected_options)?)
}

fn golden_baseline_record(path: &Path, engine: &KvEngine) -> Result<GoldenBaselineRecord> {
    Ok(GoldenBaselineRecord {
        sst_file_count: count_files_with_extension(path, "sst")?,
        vlog_file_count: count_files_with_extension(&path.join("vlog"), "vlog")?,
        level_layout: engine.dump_structure_string(),
        counters: collect_counters(engine)?,
    })
}

fn count_files_with_extension(path: &Path, extension: &str) -> Result<usize> {
    let mut count = 0usize;
    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                let entry =
                    entry.with_context(|| format!("failed to read entry in {}", path.display()))?;
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    count += count_files_with_extension(&entry_path, extension)?;
                } else if entry_path.extension().is_some_and(|ext| ext == extension) {
                    count += 1;
                }
            }
            Ok(count)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn copy_dir_recursively(source: &Path, target: &Path) -> Result<()> {
    fs::create_dir_all(target).with_context(|| format!("failed to create {}", target.display()))?;
    for entry in
        fs::read_dir(source).with_context(|| format!("failed to read {}", source.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", source.display()))?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if source_path.is_dir() {
            copy_dir_recursively(&source_path, &target_path)?;
        } else {
            fs::copy(&source_path, &target_path).with_context(|| {
                format!(
                    "failed to copy {} to {}",
                    source_path.display(),
                    target_path.display()
                )
            })?;
        }
    }

    Ok(())
}

fn stable_digest(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn golden_manifest_digest(manifest: &GoldenManifestRecord) -> Result<String> {
    let digest_record = GoldenManifestDigestRecord {
        manifest_schema: &manifest.manifest_schema,
        key_count: manifest.key_count,
        value_size: manifest.value_size,
        key_format: &manifest.key_format,
        engine_options: &manifest.engine_options,
        engine_options_hash: &manifest.engine_options_hash,
        source_commit: &manifest.source_commit,
        sst_file_count: manifest.sst_file_count,
        vlog_file_count: manifest.vlog_file_count,
        level_layout: &manifest.level_layout,
        settle_status: &manifest.settle_status,
        settle_timeout_secs: manifest.settle_timeout_secs,
    };
    Ok(stable_digest(&serde_json::to_vec(&digest_record)?))
}

fn legacy_golden_manifest_digest(manifest: &GoldenManifestRecord) -> Result<String> {
    let legacy_options = LegacyEngineOptionsRecord::from(&manifest.engine_options);
    let digest_record = LegacyGoldenManifestDigestRecord {
        manifest_schema: &manifest.manifest_schema,
        key_count: manifest.key_count,
        value_size: manifest.value_size,
        key_format: &manifest.key_format,
        engine_options: &legacy_options,
        engine_options_hash: &manifest.engine_options_hash,
        source_commit: &manifest.source_commit,
        sst_file_count: manifest.sst_file_count,
        vlog_file_count: manifest.vlog_file_count,
        level_layout: &manifest.level_layout,
        settle_status: &manifest.settle_status,
        settle_timeout_secs: manifest.settle_timeout_secs,
    };
    Ok(stable_digest(&serde_json::to_vec(&digest_record)?))
}

fn golden_manifest_digest_matches(manifest: &GoldenManifestRecord) -> Result<bool> {
    if manifest.manifest_digest == golden_manifest_digest(manifest)? {
        return Ok(true);
    }
    let legacy_options_hash = legacy_engine_options_hash(&manifest.engine_options)?;
    Ok(!manifest.engine_options.serializable
        && manifest.engine_options_hash == legacy_options_hash
        && manifest.manifest_digest == legacy_golden_manifest_digest(manifest)?)
}

fn source_commit() -> String {
    if let Some(commit) = option_env!("GITHUB_SHA").filter(|commit| !commit.trim().is_empty()) {
        return commit.to_string();
    }
    runtime_git_source_commit().unwrap_or_else(|| "unknown".to_string())
}

fn runtime_git_source_commit() -> Option<String> {
    let repo_dir = option_env!("CARGO_MANIFEST_DIR").unwrap_or(".");
    let output = Command::new("git")
        .args(["-C", repo_dir, "rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?;
    let commit = commit.trim();
    (!commit.is_empty()).then(|| commit.to_string())
}

fn run_idle(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "idle";
    if cfg.operation_mix.is_some() {
        bail!("idle does not support --operation-mix");
    }

    let opened = open_loaded_steady_state_keyspace(cfg, workload)?;
    let baseline = collect_counters(&opened.engine)?;
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(cfg.measurement_secs));
    let elapsed = start.elapsed();
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &opened.path, &opened.engine, &baseline)?;

    let mut measurement = make_measurement(
        cfg,
        workload,
        "idle_wait",
        &opened.options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("none"),
            seed_derivation: None,
            key_selection: Some("none"),
            clients: Some(0),
            warmup_secs: Some(0),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: Some("idle=1.0".to_string()),
            operation_mix_period: Some(1),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            load_elapsed_ms: Some(ms(opened.load_elapsed)),
            measure_elapsed_ms: ms(elapsed),
            ops: Some(0),
            ops_per_sec: Some(0.0),
            reads: Some(0),
            reads_per_sec: Some(0.0),
            writes: Some(0),
            writes_per_sec: Some(0.0),
            ..MeasurementResult::default()
        },
        counters,
    );
    measurement.record.phase = Some("measurement");
    measurement.record.task = Some(SteadyStateTaskRecord {
        clients: 0,
        warmup_secs: 0,
        measurement_secs: cfg.measurement_secs,
        operation_mix: "idle=1.0".to_string(),
        operation_mix_period: 1,
        operation_mix_scheduler: "idle_wait",
        key_selection: "none",
        scan_limit: cfg.scan_limit,
        seed: cfg.seed,
        rng_algorithm: "none",
        rng_crate_version: RAND_CHACHA_VERSION,
        seed_derivation: "none",
        scramble_function: "none",
        zipfian_exponent: None,
        key_format: "be_u64_plus_ascii_zero_padding_20b",
        transaction_hot_set: None,
        transaction_reads: None,
        transaction_updates: None,
        transaction_retries: None,
        transaction_conflict_latency: None,
    });
    let mut validation = steady_state_validation_record(
        &SteadyStateWindowStats::default(),
        "idle=1.000000".to_string(),
    );
    validation.expected_read_hits = None;
    validation.expected_read_misses = None;
    validation.min_completed_operations = 0;
    measurement.record.validation = Some(validation);
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.golden_manifest = opened.golden_manifest;
    measurement.record.counter_snapshots = Some(counter_snapshots);

    Ok(vec![measurement])
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

    let opened = open_loaded_steady_state_keyspace(cfg, workload)?;

    if cfg.warmup_secs > 0 {
        let _warmup = run_steady_state_read_window(
            &opened.engine,
            cfg,
            selector.clone(),
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
    }

    let baseline = collect_counters(&opened.engine)?;
    let start = Instant::now();
    let window = run_steady_state_read_window(
        &opened.engine,
        cfg,
        selector,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &opened.path, &opened.engine, &baseline)?;

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
        &opened.options,
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
            load_elapsed_ms: Some(ms(opened.load_elapsed)),
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
    measurement.record.throughput = throughput_record(&window);
    measurement.record.validation = Some(steady_state_validation_record(
        &window,
        "get=1.000000,put=0.000000".to_string(),
    ));
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.golden_manifest = opened.golden_manifest;
    measurement.record.counter_snapshots = Some(counter_snapshots);

    Ok(vec![measurement])
}

fn run_range_scan_uniform(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "range_scan_uniform";
    if let Some(operation_mix) = &cfg.operation_mix {
        validate_scan_only_operation_mix(operation_mix, cfg.operation_mix_period)?;
    }

    let opened = open_loaded_steady_state_keyspace(cfg, workload)?;

    if cfg.warmup_secs > 0 {
        let warmup = run_steady_state_scan_window(
            &opened.engine,
            cfg,
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        validate_range_scan_window(workload, &warmup)?;
    }

    let baseline = collect_counters(&opened.engine)?;
    let start = Instant::now();
    let window = run_steady_state_scan_window(
        &opened.engine,
        cfg,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &opened.path, &opened.engine, &baseline)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "range_scan_uniform completed no measured operations"
    );
    validate_range_scan_window(workload, &window)?;

    let mut measurement = make_measurement(
        cfg,
        workload,
        "range_scan",
        &opened.options,
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
            load_elapsed_ms: Some(ms(opened.load_elapsed)),
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
    measurement.record.throughput = throughput_record(&window);
    let mut validation = steady_state_validation_record(&window, "scan=1.000000".to_string());
    validation.expected_read_hits = None;
    validation.expected_read_misses = None;
    measurement.record.validation = Some(validation);
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.golden_manifest = opened.golden_manifest;
    measurement.record.counter_snapshots = Some(counter_snapshots);

    Ok(vec![measurement])
}

fn run_read_heavy_zipfian(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_mixed_zipfian_workload(cfg, "read_heavy_zipfian", "get=0.95,put=0.05")
}

fn run_balanced_zipfian(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_mixed_zipfian_workload(cfg, "balanced_zipfian", "get=0.5,put=0.5")
}

fn run_update_heavy_zipfian(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    run_mixed_zipfian_workload(cfg, "update_heavy_zipfian", "get=0.05,put=0.95")
}

fn resolved_write_only_operation_mix(cfg: &HarnessConfig) -> Result<(String, usize)> {
    match &cfg.operation_mix {
        Some(operation_mix) => {
            validate_write_only_operation_mix(operation_mix, cfg.operation_mix_period)?;
            Ok((operation_mix.clone(), cfg.operation_mix_period))
        }
        None => Ok(("put=1.0".to_string(), 1)),
    }
}

fn run_mixed_zipfian_workload(
    cfg: &HarnessConfig,
    workload: &'static str,
    default_operation_mix: &'static str,
) -> Result<Vec<BenchMeasurement>> {
    let operation_mix = cfg
        .operation_mix
        .clone()
        .unwrap_or_else(|| default_operation_mix.to_string());
    let mix = SteadyStateOperationMix::parse(&operation_mix, cfg.operation_mix_period)?;
    let opened = open_loaded_steady_state_keyspace(cfg, workload)?;

    let sampler = Arc::new(ZipfianSampler::new(cfg.num, STEADY_STATE_ZIPFIAN_EXPONENT)?);
    let mut post_warmup_baseline = None;
    if cfg.warmup_secs > 0 {
        let _warmup = run_steady_state_mixed_window(
            &opened.engine,
            cfg,
            &mix,
            sampler.clone(),
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        opened.engine.drain_flush()?;
        post_warmup_baseline = Some(golden_baseline_record(&opened.path, &opened.engine)?);
    }

    let baseline = collect_counters(&opened.engine)?;
    let start = Instant::now();
    let window = run_steady_state_mixed_window(
        &opened.engine,
        cfg,
        &mix,
        sampler,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&opened.engine, workload);
    }
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &opened.path, &opened.engine, &baseline)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "{workload} completed no measured operations"
    );
    anyhow::ensure!(
        window.read_misses == 0,
        "{workload} expected all reads to hit, got {} misses",
        window.read_misses
    );
    mix.validate_complete_periods(window.complete_period_gets, window.complete_period_puts)?;

    let mut measurement = make_measurement(
        cfg,
        workload,
        "mixed_closed_loop",
        &opened.options,
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
            load_elapsed_ms: Some(ms(opened.load_elapsed)),
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
    measurement.record.throughput = throughput_record(&window);
    measurement.record.validation = Some(steady_state_validation_record(
        &window,
        format!(
            "get={:.6},put={:.6}",
            window.reads as f64 / window.completed_operations as f64,
            window.writes as f64 / window.completed_operations as f64
        ),
    ));
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.golden_manifest = opened.golden_manifest;
    measurement.record.post_warmup_baseline = post_warmup_baseline;
    measurement.record.counter_snapshots = Some(counter_snapshots);

    Ok(vec![measurement])
}

fn run_sustained_ingest(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "sustained_ingest";
    let (operation_mix, operation_mix_period) = resolved_write_only_operation_mix(cfg)?;

    let path = prepare_path(cfg, workload)?;
    let options = cfg.build_options(false, false);
    let engine = KvEngine::open(&path, options.clone())?;
    let next_key_id = Arc::new(AtomicU64::new(0));

    if cfg.warmup_secs > 0 {
        let _warmup = run_steady_state_ingest_window(
            &engine,
            cfg,
            &next_key_id,
            operation_mix_period,
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        engine.drain_flush()?;
    }

    let baseline = collect_counters(&engine)?;
    let start = Instant::now();
    let window = run_steady_state_ingest_window(
        &engine,
        cfg,
        &next_key_id,
        operation_mix_period,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &path, &engine, &baseline)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "sustained_ingest completed no measured operations"
    );

    let mut measurement = make_measurement(
        cfg,
        workload,
        "write_closed_loop",
        &options,
        MeasurementParams {
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some("unique_sequential"),
            clients: Some(cfg.clients),
            warmup_secs: Some(cfg.warmup_secs),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: Some(operation_mix.clone()),
            operation_mix_period: Some(operation_mix_period),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            ..MeasurementParams::default()
        },
        MeasurementResult {
            measure_elapsed_ms: ms(elapsed),
            ops: Some(window.completed_operations),
            ops_per_sec: Some(rate(window.completed_operations, elapsed)),
            entries: Some(window.writes),
            entries_per_sec: Some(rate(window.writes, elapsed)),
            writes: Some(window.writes),
            writes_per_sec: Some(rate(window.writes, elapsed)),
            ..MeasurementResult::default()
        },
        counters,
    );
    measurement.record.phase = Some("measurement");
    measurement.record.task = Some(steady_state_task_record(
        cfg,
        operation_mix.clone(),
        operation_mix_period,
        "closed_loop_unique_sequential",
        "unique_sequential",
        None,
    ));
    measurement.record.latency = cfg.latency_sample_every.map(|sample_every| {
        latency_record(
            sample_every,
            window.completed_operations,
            &window.latency_samples_ns,
        )
    });
    measurement.record.throughput = throughput_record(&window);
    let mut validation = steady_state_validation_record(
        &window,
        format!(
            "put={:.6}",
            window.writes as f64 / window.completed_operations as f64
        ),
    );
    validation.expected_read_hits = None;
    validation.expected_read_misses = None;
    measurement.record.validation = Some(validation);
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.counter_snapshots = Some(counter_snapshots);

    Ok(vec![measurement])
}

fn run_transaction_contention(cfg: &HarnessConfig) -> Result<Vec<BenchMeasurement>> {
    let workload = "transaction_contention";
    if cfg.operation_mix.is_some() {
        bail!("transaction_contention does not support --operation-mix");
    }

    let path = prepare_path(cfg, workload)?;
    let mut options = cfg.build_options(false, false);
    options.serializable = true;
    let engine = KvEngine::open(&path, options.clone())?;
    let value = vec![b'x'; cfg.value_size];
    let load_start = Instant::now();
    for i in 0..cfg.num as u64 {
        engine.put(&steady_state_loaded_key(i), &value)?;
    }
    engine.drain_flush()?;
    let load_elapsed = load_start.elapsed();

    if cfg.warmup_secs > 0 {
        let warmup = run_steady_state_transaction_window(
            &engine,
            cfg,
            Duration::from_secs(cfg.warmup_secs),
            SteadyStateWindowPhase::Warmup,
            false,
        )?;
        anyhow::ensure!(
            warmup.transaction_attempts
                == warmup.transaction_commits + warmup.transaction_conflicts,
            "transaction_contention warmup attempts {} did not equal commits {} plus conflicts {}",
            warmup.transaction_attempts,
            warmup.transaction_commits,
            warmup.transaction_conflicts
        );
        anyhow::ensure!(
            warmup.read_misses == 0,
            "transaction_contention warmup expected all reads to hit, got {} misses",
            warmup.read_misses
        );
        engine.drain_flush()?;
    }

    let baseline = collect_counters(&engine)?;
    let start = Instant::now();
    let window = run_steady_state_transaction_window(
        &engine,
        cfg,
        Duration::from_secs(cfg.measurement_secs),
        SteadyStateWindowPhase::Measurement,
        true,
    )?;
    let elapsed = start.elapsed();
    if cfg.profile {
        print_write_profile(&engine, workload);
    }
    let (flush_drain, counters, counter_snapshots) =
        finish_steady_state_measurement(cfg, &path, &engine, &baseline)?;

    anyhow::ensure!(
        window.completed_operations >= 1,
        "transaction_contention completed no measured operations"
    );
    anyhow::ensure!(
        window.transaction_attempts == window.transaction_commits + window.transaction_conflicts,
        "transaction_contention attempts {} did not equal commits {} plus conflicts {}",
        window.transaction_attempts,
        window.transaction_commits,
        window.transaction_conflicts
    );
    anyhow::ensure!(
        window.read_misses == 0,
        "transaction_contention expected all reads to hit, got {} misses",
        window.read_misses
    );

    let operation_mix = format!(
        "txn_reads={},txn_updates={},txn_retries={}",
        cfg.transaction_reads, cfg.transaction_updates, cfg.transaction_retries
    );
    let mut measurement = make_measurement(
        cfg,
        workload,
        "serializable_hot_set",
        &options,
        MeasurementParams {
            num: Some(cfg.num),
            value_size: Some(cfg.value_size),
            seed: Some(cfg.seed),
            rng_algorithm: Some("ChaCha12Rng"),
            seed_derivation: Some(STEADY_STATE_SEED_VERSION),
            key_selection: Some("uniform_hot_set"),
            clients: Some(cfg.clients),
            warmup_secs: Some(cfg.warmup_secs),
            measurement_secs: Some(cfg.measurement_secs),
            operation_mix: Some(operation_mix.clone()),
            operation_mix_period: Some(1),
            scan_limit: Some(cfg.scan_limit),
            latency_sample_every: cfg.latency_sample_every,
            settle_timeout_secs: cfg.settle_timeout_secs,
            transaction_hot_set: Some(cfg.transaction_hot_set),
            transaction_reads: Some(cfg.transaction_reads),
            transaction_updates: Some(cfg.transaction_updates),
            transaction_retries: Some(cfg.transaction_retries),
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
    let mut task = steady_state_task_record(
        cfg,
        operation_mix.clone(),
        1,
        "closed_loop_serializable_transaction",
        "uniform_hot_set",
        None,
    );
    task.transaction_hot_set = Some(cfg.transaction_hot_set);
    task.transaction_reads = Some(cfg.transaction_reads);
    task.transaction_updates = Some(cfg.transaction_updates);
    task.transaction_retries = Some(cfg.transaction_retries);
    task.transaction_conflict_latency = Some(true);
    measurement.record.task = Some(task);
    measurement.record.latency = cfg.latency_sample_every.map(|sample_every| {
        latency_record(
            sample_every,
            window.transaction_attempts,
            &window.latency_samples_ns,
        )
    });
    measurement.record.throughput = throughput_record(&window);
    let mut validation = steady_state_validation_record(&window, operation_mix);
    validation.expected_read_hits = Some(window.reads);
    validation.expected_read_misses = Some(0);
    measurement.record.validation = Some(validation);
    measurement.record.drain = Some(steady_state_drain_record(flush_drain));
    measurement.record.counter_snapshots = Some(counter_snapshots);

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
        schema: schema_for_suite(cfg.suite),
        unix_epoch_ms: unix_epoch_ms(),
        run_id: cfg.run_id.clone(),
        workload: workload.to_string(),
        suite: cfg.suite,
        phase: None,
        measurement: measurement.clone(),
        preset: cfg.preset_name,
        engine: ENGINE_NAME,
        engine_options: engine_options_record(options),
        params,
        task: None,
        latency: None,
        throughput: None,
        result,
        validation: None,
        drain: None,
        golden_manifest: None,
        post_warmup_baseline: None,
        counter_snapshots: None,
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
    let write_profile = engine.write_profile();
    let range = engine.range_tombstone_stats();
    let filters = engine.compaction_filter_stats();
    let parallel = engine.parallel_scan_stats();
    let vlog = engine.vlog_stats().ok();
    Ok(MeasurementCounters {
        block_cache_entry_count: cache.block_cache_entry_count,
        block_cache_hit_count: cache.block_cache_hit_count,
        block_cache_miss_count: cache.block_cache_miss_count,
        block_cache_admitted_count: cache.block_cache_admitted_count,
        block_cache_rejected_count: cache.block_cache_rejected_count,
        block_cache_evicted_count: cache.block_cache_evicted_count,
        wal_commit_groups: write_profile.wal_commit_groups,
        wal_commit_solo_groups: write_profile.wal_commit_solo_groups,
        wal_commit_buffers: write_profile.wal_commit_buffers,
        wal_commit_bytes: write_profile.wal_commit_bytes,
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
        block_cache_hit_count: after
            .block_cache_hit_count
            .saturating_sub(before.block_cache_hit_count),
        block_cache_miss_count: after
            .block_cache_miss_count
            .saturating_sub(before.block_cache_miss_count),
        block_cache_admitted_count: after
            .block_cache_admitted_count
            .saturating_sub(before.block_cache_admitted_count),
        block_cache_rejected_count: after
            .block_cache_rejected_count
            .saturating_sub(before.block_cache_rejected_count),
        block_cache_evicted_count: after
            .block_cache_evicted_count
            .saturating_sub(before.block_cache_evicted_count),
        wal_commit_groups: after
            .wal_commit_groups
            .saturating_sub(before.wal_commit_groups),
        wal_commit_solo_groups: after
            .wal_commit_solo_groups
            .saturating_sub(before.wal_commit_solo_groups),
        wal_commit_buffers: after
            .wal_commit_buffers
            .saturating_sub(before.wal_commit_buffers),
        wal_commit_bytes: after
            .wal_commit_bytes
            .saturating_sub(before.wal_commit_bytes),
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
        remove_file_if_exists(&checkpoint_target_lock_path(path))?;
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

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn checkpoint_clone_source_path(target_path: &Path) -> PathBuf {
    let name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "workload".to_string());
    target_path.with_file_name(format!(
        "{name}.golden-source-{}-{}.tmp",
        std::process::id(),
        unix_epoch_ms()
    ))
}

fn checkpoint_target_lock_path(target_path: &Path) -> PathBuf {
    let name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "checkpoint".to_string());
    target_path.with_file_name(format!("{name}.checkpoint.lock"))
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
    anyhow::ensure!(
        period > 0,
        "task.operation_mix_period must be greater than zero"
    );
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

fn validate_write_only_operation_mix(spec: &str, period: usize) -> Result<()> {
    validate_single_operation_mix(spec, period, "put", "write-only workloads")
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
    anyhow::ensure!(
        cfg.transaction_hot_set > 0,
        "--transaction-hot-set must be > 0"
    );
    anyhow::ensure!(
        cfg.transaction_hot_set <= cfg.num,
        "--transaction-hot-set must be <= --num"
    );
    anyhow::ensure!(cfg.transaction_reads > 0, "--transaction-reads must be > 0");
    anyhow::ensure!(
        cfg.transaction_updates > 0,
        "--transaction-updates must be > 0"
    );
    if let Some(latency_sample_every) = cfg.latency_sample_every {
        anyhow::ensure!(
            latency_sample_every > 0,
            "--latency-sample-every must be > 0"
        );
    }
    if let Some(settle_timeout_secs) = cfg.settle_timeout_secs {
        anyhow::ensure!(settle_timeout_secs > 0, "--settle-timeout-secs must be > 0");
    }
    if cfg.prepare_golden || cfg.golden_path.is_some() || cfg.clone_golden {
        anyhow::ensure!(
            cfg.suite == Suite::SteadyState,
            "golden dataset options require --suite steady-state"
        );
    }
    if cfg.prepare_golden {
        anyhow::ensure!(
            cfg.golden_path.is_some(),
            "--prepare-golden requires --golden-path"
        );
        anyhow::ensure!(
            !cfg.clone_golden,
            "--prepare-golden cannot be combined with --clone-golden"
        );
    }
    if cfg.clone_golden {
        anyhow::ensure!(
            cfg.golden_path.is_some(),
            "--clone-golden requires --golden-path"
        );
    }
    if cfg.golden_path.is_some() && !cfg.prepare_golden && !cfg.clone_golden {
        bail!("--golden-path requires either --prepare-golden or --clone-golden");
    }
    if let Some(operation_mix) = &cfg.operation_mix {
        validate_operation_mix(operation_mix, cfg.operation_mix_period)?;
    }

    Ok(())
}

fn validate_run_mode(cfg: &HarnessConfig, bench_arg: Option<&str>) -> Result<()> {
    anyhow::ensure!(
        !(cfg.prepare_golden && bench_arg.is_some()),
        "--prepare-golden does not support --bench"
    );

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

    fn steady_state_schema_cfg() -> HarnessConfig {
        HarnessConfig::from_args(
            Args::try_parse_from(["write-perf", "--suite", "steady-state", "--num", "1000"])
                .expect("parse args"),
        )
    }

    #[derive(Debug, Deserialize)]
    struct SchemaFixtureRecord {
        schema: String,
        workload: String,
        phase: Option<String>,
        task: Option<SchemaFixtureTask>,
        result: SchemaFixtureResult,
        validation: Option<SchemaFixtureValidation>,
    }

    #[derive(Debug, Deserialize)]
    struct SchemaFixtureTask {
        operation_mix: String,
        operation_mix_scheduler: String,
        key_selection: String,
        scramble_function: String,
        zipfian_exponent: Option<f64>,
        transaction_hot_set: Option<usize>,
        transaction_reads: Option<usize>,
        transaction_updates: Option<usize>,
        transaction_retries: Option<usize>,
        transaction_conflict_latency: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    struct SchemaFixtureResult {
        ops: Option<u64>,
        found: Option<u64>,
        total_nexts: Option<u64>,
    }

    #[derive(Debug, Deserialize)]
    struct SchemaFixtureValidation {
        read_hits: u64,
        read_misses: u64,
        completed_operations: u64,
    }

    struct SteadyStateSchemaFixtureCase {
        workload: &'static str,
        measurement: &'static str,
        key_selection: &'static str,
        operation_mix: &'static str,
        operation_mix_scheduler: &'static str,
        scramble_function: &'static str,
        zipfian_exponent: Option<f64>,
        result_found: Option<u64>,
        total_nexts: Option<u64>,
        read_hits: u64,
        read_misses: u64,
        completed_operations: u64,
    }

    fn parse_schema_fixture(json: serde_json::Value) -> SchemaFixtureRecord {
        serde_json::from_value(json).expect("parse schema fixture")
    }

    fn fixture_rate_window(total: u64) -> Option<RateWindowRecord> {
        if total == 0 {
            return None;
        }

        Some(RateWindowRecord {
            total,
            avg_per_sec: total as f64,
            p1_per_sec: total as f64,
            p50_per_sec: total as f64,
            p95_per_sec: total as f64,
            p99_per_sec: total as f64,
            min_per_sec: total as f64,
            max_per_sec: total as f64,
        })
    }

    fn steady_state_schema_fixture(case: SteadyStateSchemaFixtureCase) -> serde_json::Value {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let clients = if case.workload == "idle" { 0 } else { 4 };
        let operation_mix_period = match case.workload {
            "idle"
            | "point_read_missing_in_range"
            | "point_read_uniform"
            | "point_read_zipfian"
            | "range_scan_uniform"
            | "transaction_contention" => 1,
            _ => 1000,
        };
        let transaction_hot_set = (case.workload == "transaction_contention").then_some(128);
        let transaction_reads = (case.workload == "transaction_contention").then_some(5);
        let transaction_updates = (case.workload == "transaction_contention").then_some(5);
        let transaction_retries = (case.workload == "transaction_contention").then_some(0);
        let transaction_conflict_latency =
            (case.workload == "transaction_contention").then_some(true);
        let transaction_attempts = if case.workload == "transaction_contention" {
            case.completed_operations
        } else {
            0
        };
        let transaction_commits = if case.workload == "transaction_contention" {
            case.completed_operations
        } else {
            0
        };
        let expected_read_hits = match case.workload {
            "range_scan_uniform" | "sustained_ingest" => None,
            _ => Some(case.read_hits),
        };
        let expected_read_misses = match case.workload {
            "range_scan_uniform" | "sustained_ingest" => None,
            _ => Some(case.read_misses),
        };
        let complete_period_operations =
            case.completed_operations / operation_mix_period as u64 * operation_mix_period as u64;
        let tail_operations = case.completed_operations - complete_period_operations;
        let (tail_gets, tail_puts) = match case.workload {
            "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => {
                let mix = SteadyStateOperationMix::parse(case.operation_mix, operation_mix_period)
                    .expect("fixture operation mix should parse");
                let complete_periods = complete_period_operations / operation_mix_period as u64;
                let complete_gets = complete_periods * mix.get_slots as u64;
                let complete_puts = complete_periods * mix.put_slots as u64;
                let tail_gets = case.read_hits - complete_gets;
                let tail_puts = case.completed_operations - case.read_hits - complete_puts;
                assert_eq!(
                    tail_gets + tail_puts,
                    tail_operations,
                    "fixture mixed tail counters must reconcile"
                );
                (tail_gets, tail_operations - tail_gets)
            }
            "sustained_ingest" => (0, tail_operations),
            workload if workload.contains("point_read") => (tail_operations, 0),
            _ => (0, 0),
        };
        let mut measurement = make_measurement(
            &cfg,
            case.workload,
            case.measurement,
            &options,
            MeasurementParams {
                num: Some(1000),
                value_size: Some(400),
                seed: Some(42),
                rng_algorithm: Some("ChaCha12Rng"),
                seed_derivation: Some(STEADY_STATE_SEED_VERSION),
                key_selection: Some(case.key_selection),
                clients: Some(clients),
                warmup_secs: Some(0),
                measurement_secs: Some(1),
                operation_mix: Some(case.operation_mix.to_string()),
                operation_mix_period: Some(operation_mix_period),
                scan_limit: Some(10),
                latency_sample_every: Some(1000),
                transaction_hot_set,
                transaction_reads,
                transaction_updates,
                transaction_retries,
                ..MeasurementParams::default()
            },
            MeasurementResult {
                measure_elapsed_ms: 1000.0,
                ops: Some(case.completed_operations),
                ops_per_sec: Some(case.completed_operations as f64),
                entries: case.total_nexts,
                entries_per_sec: case.total_nexts.map(|nexts| nexts as f64),
                reads: Some(case.read_hits + case.read_misses),
                reads_per_sec: Some((case.read_hits + case.read_misses) as f64),
                found: case.result_found,
                total_nexts: case.total_nexts,
                ..MeasurementResult::default()
            },
            MeasurementCounters::default(),
        );
        if case.workload == "transaction_contention" {
            measurement.record.engine_options.serializable = true;
        }
        measurement.record.phase = Some("measurement");
        measurement.record.task = Some(SteadyStateTaskRecord {
            clients,
            warmup_secs: 0,
            measurement_secs: 1,
            operation_mix: case.operation_mix.to_string(),
            operation_mix_period,
            operation_mix_scheduler: case.operation_mix_scheduler,
            key_selection: case.key_selection,
            scan_limit: 10,
            seed: 42,
            rng_algorithm: "ChaCha12Rng",
            rng_crate_version: RAND_CHACHA_VERSION,
            seed_derivation: STEADY_STATE_SEED_VERSION,
            scramble_function: case.scramble_function,
            zipfian_exponent: case.zipfian_exponent,
            key_format: "be_u64_plus_ascii_zero_padding_20b",
            transaction_hot_set,
            transaction_reads,
            transaction_updates,
            transaction_retries,
            transaction_conflict_latency,
        });
        measurement.record.latency = Some(LatencyRecord {
            sample_every: 1000,
            samples: 1,
            unsampled_completed_operations: case.completed_operations.saturating_sub(1),
            avg_ms: Some(0.01),
            min_ms: Some(0.01),
            p50_ms: Some(0.01),
            p95_ms: Some(0.01),
            p99_ms: Some(0.01),
            max_ms: Some(0.01),
        });
        let fixture_reads = match case.workload {
            "point_read_missing_in_range" | "point_read_uniform" | "point_read_zipfian" => {
                case.completed_operations
            }
            "range_scan_uniform" => case.completed_operations,
            "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => case.read_hits,
            "transaction_contention" => case.read_hits,
            _ => 0,
        };
        let fixture_writes = match case.workload {
            "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian" => {
                case.completed_operations - case.read_hits
            }
            "sustained_ingest" => case.completed_operations,
            "transaction_contention" => {
                case.completed_operations * transaction_updates.unwrap_or(0) as u64
            }
            _ => 0,
        };
        measurement.record.throughput = Some(ThroughputRecord {
            window_secs: 1,
            complete_windows: 1,
            operations: fixture_rate_window(case.completed_operations),
            reads: fixture_rate_window(fixture_reads),
            writes: fixture_rate_window(fixture_writes),
            logical_bytes: None,
            read_logical_bytes: None,
            write_logical_bytes: None,
            scan_rows: case.total_nexts.and_then(fixture_rate_window),
        });
        measurement.record.validation = Some(ValidationRecord {
            errors: 0,
            read_hits: case.read_hits,
            read_misses: case.read_misses,
            expected_read_hits,
            expected_read_misses,
            observed_operation_mix: case.operation_mix.to_string(),
            scan_count_errors: 0,
            scan_order_errors: 0,
            scan_key_errors: 0,
            transaction_attempts,
            transaction_commits,
            transaction_conflicts: 0,
            selected_operations: case.completed_operations,
            completed_operations: case.completed_operations,
            min_completed_operations: if case.workload == "idle" { 0 } else { 1 },
            complete_period_operations,
            tail_operations,
            tail_gets,
            tail_puts,
        });
        measurement.record.drain = Some(DrainRecord {
            flush_drain_ms: 0.0,
            background_drain_ms: None,
            background_drain_status: "not_requested",
        });
        measurement.record.counter_snapshots = Some(CounterSnapshotsRecord {
            before: MeasurementCounters::default(),
            after: MeasurementCounters::default(),
        });

        serde_json::to_value(&measurement.record).expect("serialize schema fixture")
    }

    fn steady_state_prepare_schema_fixture(dir: &Path) -> serde_json::Value {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let manifest = valid_golden_manifest(dir, &cfg, &options);
        let mut measurement = make_measurement(
            &cfg,
            "prepare_golden",
            "bulk_load",
            &options,
            MeasurementParams {
                num: Some(cfg.num),
                value_size: Some(cfg.value_size),
                seed: Some(cfg.seed),
                rng_algorithm: Some("none"),
                seed_derivation: None,
                key_selection: Some("ordered"),
                settle_timeout_secs: cfg.settle_timeout_secs,
                ..MeasurementParams::default()
            },
            MeasurementResult {
                load_elapsed_ms: Some(1.0),
                measure_elapsed_ms: 1.0,
                ops: Some(cfg.num as u64),
                ops_per_sec: Some(cfg.num as f64),
                writes: Some(cfg.num as u64),
                writes_per_sec: Some(cfg.num as f64),
                entries: Some(cfg.num as u64),
                entries_per_sec: Some(cfg.num as f64),
                ..MeasurementResult::default()
            },
            MeasurementCounters::default(),
        );
        measurement.record.phase = Some("prepare");
        measurement.record.golden_manifest = Some(manifest);
        measurement.record.drain = Some(DrainRecord {
            flush_drain_ms: 0.0,
            background_drain_ms: None,
            background_drain_status: "not_requested",
        });

        serde_json::to_value(&measurement.record).expect("serialize prepare fixture")
    }

    fn valid_golden_manifest(
        dir: &Path,
        cfg: &HarnessConfig,
        options: &LsmStorageOptions,
    ) -> GoldenManifestRecord {
        let engine_options = engine_options_record(options);
        let mut manifest = GoldenManifestRecord {
            manifest_schema: GOLDEN_MANIFEST_SCHEMA_V1.to_string(),
            manifest_path: golden_manifest_path(dir).display().to_string(),
            manifest_digest: String::new(),
            key_count: cfg.num,
            value_size: cfg.value_size,
            key_format: "be_u64_plus_ascii_zero_padding_20b".to_string(),
            engine_options_hash: stable_digest(
                &serde_json::to_vec(&engine_options).expect("serialize options"),
            ),
            engine_options,
            source_commit: source_commit(),
            sst_file_count: 1,
            vlog_file_count: 0,
            level_layout: "L0 (1): [0]\n".to_string(),
            settle_status: "settled".to_string(),
            settle_elapsed_ms: 0.0,
            settle_timeout_secs: None,
        };
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("digest manifest");
        manifest
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
        assert_eq!(cfg.clients, 1);
        assert_eq!(cfg.warmup_secs, 0);
        assert_eq!(cfg.measurement_secs, 1);
    }

    #[test]
    fn parse_golden_dataset_options() {
        let prepare_args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--prepare-golden",
            "--golden-path",
            "/tmp/toykv-golden",
        ])
        .expect("parse args");
        let prepare_cfg = HarnessConfig::from_args(prepare_args);

        assert!(prepare_cfg.prepare_golden);
        assert_eq!(
            prepare_cfg.golden_path,
            Some(PathBuf::from("/tmp/toykv-golden"))
        );
        assert!(!prepare_cfg.clone_golden);
        validate_config(&prepare_cfg).expect("prepare golden config");

        let clone_args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--golden-path",
            "/tmp/toykv-golden",
            "--clone-golden",
        ])
        .expect("parse args");
        let clone_cfg = HarnessConfig::from_args(clone_args);

        assert!(!clone_cfg.prepare_golden);
        assert_eq!(
            clone_cfg.golden_path,
            Some(PathBuf::from("/tmp/toykv-golden"))
        );
        assert!(clone_cfg.clone_golden);
        validate_config(&clone_cfg).expect("clone golden config");
    }

    #[test]
    fn prepare_golden_requires_golden_path() {
        let args =
            Args::try_parse_from(["write-perf", "--suite", "steady-state", "--prepare-golden"])
                .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("prepare without path should fail");

        assert!(err.to_string().contains("--prepare-golden requires"));
    }

    #[test]
    fn golden_options_require_steady_state_suite() {
        let args = Args::try_parse_from(["write-perf", "--golden-path", "/tmp/toykv-golden"])
            .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("legacy golden config should fail");

        assert!(err.to_string().contains("--suite steady-state"));
    }

    #[test]
    fn golden_path_requires_explicit_lifecycle() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--golden-path",
            "/tmp/toykv-golden",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("golden path without lifecycle should fail");

        assert!(
            err.to_string()
                .contains("--prepare-golden or --clone-golden")
        );
    }

    #[test]
    fn prepare_golden_rejects_clone_and_bench_filter() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--prepare-golden",
            "--clone-golden",
            "--golden-path",
            "/tmp/toykv-golden",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("prepare plus clone should fail");

        assert!(err.to_string().contains("cannot be combined"));

        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--prepare-golden",
            "--golden-path",
            "/tmp/toykv-golden",
            "--bench",
            "balanced",
        ])
        .expect("parse args");
        let bench_arg = args.bench.clone();
        let cfg = HarnessConfig::from_args(args);

        validate_config(&cfg).expect("prepare config");
        let err =
            validate_run_mode(&cfg, bench_arg.as_deref()).expect_err("prepare bench should fail");

        assert!(err.to_string().contains("does not support --bench"));
    }

    #[test]
    fn golden_clone_paths_reject_overlaps() {
        validate_golden_clone_paths(
            Path::new("/tmp/toykv-golden"),
            Path::new("/tmp/toykv-workload/point_read_uniform"),
        )
        .expect("separate paths");

        let err = validate_golden_clone_paths(
            Path::new("/tmp/toykv-golden"),
            Path::new("/tmp/toykv-golden"),
        )
        .expect_err("same path should fail");
        assert!(err.to_string().contains("must differ"));

        let err = validate_golden_clone_paths(
            Path::new("/tmp/toykv-golden"),
            Path::new("/tmp/toykv-golden/point_read_uniform"),
        )
        .expect_err("target inside source should fail");
        assert!(err.to_string().contains("must not be inside"));

        let err = validate_golden_clone_paths(
            Path::new("/tmp/toykv-workload/point_read_uniform/golden"),
            Path::new("/tmp/toykv-workload/point_read_uniform"),
        )
        .expect_err("source inside target should fail");
        assert!(err.to_string().contains("must not be inside workload path"));
    }

    #[cfg(unix)]
    #[test]
    fn golden_clone_paths_reject_symlink_overlaps() {
        let dir = tempfile::tempdir().expect("tempdir");
        let golden = dir.path().join("golden");
        let link = dir.path().join("golden-link");
        fs::create_dir(&golden).expect("create golden");
        std::os::unix::fs::symlink(&golden, &link).expect("create symlink");

        let err = validate_golden_clone_paths(&link, &golden.join("point_read_uniform"))
            .expect_err("symlink target overlap should fail");

        assert!(err.to_string().contains("must not be inside"));
    }

    #[test]
    fn clone_golden_rejects_sustained_ingest_selection() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--bench",
            "sustained_ingest",
            "--golden-path",
            "/tmp/toykv-golden",
            "--clone-golden",
        ])
        .expect("parse args");
        let bench_arg = args.bench.clone();
        let cfg = HarnessConfig::from_args(args);

        validate_config(&cfg).expect("clone config");
        validate_run_mode(&cfg, bench_arg.as_deref()).expect("run mode");
        let workloads = select_workloads(bench_arg.as_deref(), &cfg).expect("select workloads");
        let err = validate_selected_workloads(&cfg, &workloads)
            .expect_err("sustained ingest clone should fail");

        assert!(err.to_string().contains("sustained_ingest"));
    }

    #[test]
    fn clone_golden_rejects_transaction_contention_selection() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--bench",
            "transaction_contention",
            "--golden-path",
            "/tmp/toykv-golden",
            "--clone-golden",
        ])
        .expect("parse args");
        let bench_arg = args.bench.clone();
        let cfg = HarnessConfig::from_args(args);

        validate_config(&cfg).expect("clone config");
        validate_run_mode(&cfg, bench_arg.as_deref()).expect("run mode");
        let workloads = select_workloads(bench_arg.as_deref(), &cfg).expect("select workloads");
        let err = validate_selected_workloads(&cfg, &workloads)
            .expect_err("transaction contention clone should fail");

        assert!(err.to_string().contains("transaction_contention"));
    }

    #[test]
    fn prepare_golden_uses_checkpoint_and_clone_preserves_golden() {
        let dir = tempfile::tempdir().expect("tempdir");
        let golden = dir.path().join("golden");
        let base = dir.path().join("workloads");
        let golden_arg = golden.to_string_lossy().into_owned();
        let base_arg = base.to_string_lossy().into_owned();

        let prepare_args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--preset",
            "smoke",
            "--num",
            "4",
            "--value-size",
            "8",
            "--no-wal",
            "--prepare-golden",
            "--golden-path",
            golden_arg.as_str(),
            "--path",
            base_arg.as_str(),
        ])
        .expect("parse prepare args");
        let prepare_cfg = HarnessConfig::from_args(prepare_args);
        validate_config(&prepare_cfg).expect("prepare config");
        prepare_steady_state_golden(&prepare_cfg).expect("prepare golden");
        assert!(golden.join("CHECKPOINT").exists());
        assert!(!checkpoint_target_lock_path(&golden).exists());
        let golden_manifest_before =
            fs::read_to_string(golden_manifest_path(&golden)).expect("read golden manifest");

        let clone_args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--preset",
            "smoke",
            "--num",
            "4",
            "--value-size",
            "8",
            "--no-wal",
            "--clone-golden",
            "--golden-path",
            golden_arg.as_str(),
            "--path",
            base_arg.as_str(),
        ])
        .expect("parse clone args");
        let clone_cfg = HarnessConfig::from_args(clone_args);
        validate_config(&clone_cfg).expect("clone config");

        let opened = open_loaded_steady_state_keyspace(&clone_cfg, "point_read_uniform")
            .expect("open cloned golden");

        assert!(opened.path.join("CHECKPOINT").exists());
        assert_eq!(
            opened
                .engine
                .get(&steady_state_loaded_key(0))
                .expect("get cloned key")
                .as_deref(),
            Some(vec![b'x'; 8].as_slice())
        );
        opened.engine.close().expect("close cloned engine");
        finalize_path(&clone_cfg, &opened.path).expect("cleanup clone");
        assert!(!checkpoint_target_lock_path(&opened.path).exists());
        assert_eq!(
            fs::read_to_string(golden_manifest_path(&golden)).expect("read golden manifest"),
            golden_manifest_before
        );
    }

    #[test]
    fn golden_manifest_validation_rejects_incompatible_value_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.value_size += 1;
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("digest manifest");
        write_golden_manifest(dir.path(), manifest).expect("write manifest");

        let err = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect_err("incompatible value size should fail");

        assert!(err.to_string().contains("value_size"));
    }

    #[test]
    fn golden_manifest_validation_rejects_stale_source_commit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.source_commit = "stale-commit".to_string();
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("digest manifest");
        write_golden_manifest(dir.path(), manifest).expect("write manifest");

        let err = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect_err("stale manifest should fail");

        assert!(
            err.to_string()
                .contains("does not match current source_commit")
        );
    }

    #[test]
    fn golden_manifest_validation_rejects_unknown_source_commit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.source_commit = "unknown".to_string();
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("digest manifest");
        write_golden_manifest(dir.path(), manifest).expect("write manifest");

        let err = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect_err("unknown source commit should fail");

        assert!(
            err.to_string()
                .contains("golden source_commit must be known")
        );
    }

    #[test]
    fn source_commit_uses_build_or_git_metadata() {
        assert_ne!(source_commit(), "unknown");
    }

    #[test]
    fn golden_manifest_validation_rejects_incompatible_schema() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.manifest_schema = "kv-engine.steady-state-golden.v0".to_string();
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("digest manifest");
        write_golden_manifest(dir.path(), manifest).expect("write manifest");

        let err = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect_err("incompatible schema should fail");

        assert!(
            err.to_string()
                .contains("unsupported golden manifest schema")
        );
    }

    #[test]
    fn golden_manifest_validation_rejects_tampered_digest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.key_count += 1;
        write_golden_manifest(dir.path(), manifest).expect("write manifest");

        let err = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect_err("tampered manifest should fail");

        assert!(err.to_string().contains("digest does not match"));
    }

    #[test]
    fn golden_manifest_validation_accepts_pre_serializable_v1_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.engine_options_hash =
            legacy_engine_options_hash(&manifest.engine_options).expect("legacy options hash");
        manifest.manifest_digest =
            legacy_golden_manifest_digest(&manifest).expect("legacy manifest digest");
        let mut json = serde_json::to_value(&manifest).expect("serialize manifest");
        json["engine_options"]
            .as_object_mut()
            .expect("engine options object")
            .remove("serializable");
        fs::write(
            golden_manifest_path(dir.path()),
            serde_json::to_vec_pretty(&json).expect("serialize legacy manifest"),
        )
        .expect("write manifest");

        let parsed = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect("legacy v1 manifest should validate");

        assert!(!parsed.engine_options.serializable);
    }

    #[test]
    fn golden_manifest_validation_rejects_hybrid_legacy_digest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.manifest_digest =
            legacy_golden_manifest_digest(&manifest).expect("legacy manifest digest");
        let mut json = serde_json::to_value(&manifest).expect("serialize manifest");
        json["engine_options"]
            .as_object_mut()
            .expect("engine options object")
            .remove("serializable");
        fs::write(
            golden_manifest_path(dir.path()),
            serde_json::to_vec_pretty(&json).expect("serialize hybrid manifest"),
        )
        .expect("write manifest");

        let err = read_and_validate_golden_manifest(&cfg, dir.path(), &options)
            .expect_err("hybrid legacy manifest should fail");

        assert!(err.to_string().contains("digest does not match"));
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
    fn parse_idle_workload() {
        let selected =
            select_workloads(Some("idle"), &steady_state_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["idle"]);
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
    fn parse_mixed_zipfian_workloads() {
        let selected =
            select_workloads(Some("readheavy,balanced,updateheavy"), &steady_state_cfg())
                .expect("select workloads");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(
            names,
            vec![
                "read_heavy_zipfian",
                "balanced_zipfian",
                "update_heavy_zipfian"
            ]
        );
    }

    #[test]
    fn parse_sustained_ingest_workload() {
        let selected =
            select_workloads(Some("ingest"), &steady_state_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["sustained_ingest"]);
    }

    #[test]
    fn parse_transaction_contention_workload() {
        let selected =
            select_workloads(Some("txn_contention"), &steady_state_cfg()).expect("select workload");
        let names: Vec<_> = selected.iter().map(|w| w.name).collect();
        assert_eq!(names, vec!["transaction_contention"]);
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
    fn mixed_zipfian_operation_mixes_have_deterministic_period_counts() {
        let read_heavy =
            SteadyStateOperationMix::parse("get=0.95,put=0.05", 1000).expect("parse mix");
        assert_eq!(read_heavy.get_slots, 950);
        assert_eq!(read_heavy.put_slots, 50);

        let update_heavy =
            SteadyStateOperationMix::parse("get=0.05,put=0.95", 1000).expect("parse mix");
        assert_eq!(update_heavy.get_slots, 50);
        assert_eq!(update_heavy.put_slots, 950);
    }

    #[test]
    fn balanced_zipfian_rejects_unsupported_mix_operation() {
        let err = SteadyStateOperationMix::parse("get=0.5,delete=0.5", 1000)
            .expect_err("unsupported operation should fail");
        assert!(err.to_string().contains("support only get and put"));
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
    fn default_clone_golden_selection_excludes_mutating_steady_state_workloads() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--golden-path",
            "/tmp/toykv-golden",
            "--clone-golden",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let selected = select_workloads(None, &cfg).expect("select workloads");
        let names: Vec<_> = selected.iter().map(|workload| workload.name).collect();

        assert!(!names.is_empty());
        assert!(names.contains(&"point_read_uniform"));
        assert!(names.contains(&"balanced_zipfian"));
        assert!(!names.contains(&"sustained_ingest"));
        assert!(!names.contains(&"transaction_contention"));
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
            transaction_hot_set: 1,
            transaction_reads: 1,
            transaction_updates: 1,
            transaction_retries: 0,
            prepare_golden: false,
            golden_path: None,
            clone_golden: false,
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
        assert_eq!(json["schema"], JSON_SCHEMA_V1);
        assert!(json.get("phase").is_none());
        assert!(json.get("task").is_none());
        assert!(json.get("latency").is_none());
        assert!(json.get("validation").is_none());
        assert!(json.get("drain").is_none());
    }

    #[test]
    fn steady_state_json_record_contains_task_validation_and_drain() {
        let cfg = steady_state_schema_cfg();
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
            transaction_hot_set: None,
            transaction_reads: None,
            transaction_updates: None,
            transaction_retries: None,
            transaction_conflict_latency: None,
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
        measurement.record.counter_snapshots = Some(CounterSnapshotsRecord {
            before: MeasurementCounters::default(),
            after: MeasurementCounters {
                block_cache_entry_count: 10,
                block_cache_hit_count: 7,
                block_cache_miss_count: 3,
                block_cache_admitted_count: 4,
                block_cache_rejected_count: 1,
                block_cache_evicted_count: 2,
                wal_commit_groups: 5,
                wal_commit_solo_groups: 2,
                wal_commit_buffers: 8,
                wal_commit_bytes: 4096,
                value_cache_hit_count: 2,
                ..MeasurementCounters::default()
            },
        });
        measurement.record.throughput = Some(ThroughputRecord {
            window_secs: 1,
            complete_windows: 2,
            operations: Some(RateWindowRecord {
                total: 10,
                avg_per_sec: 5.0,
                p1_per_sec: 4.0,
                p50_per_sec: 5.0,
                p95_per_sec: 6.0,
                p99_per_sec: 6.0,
                min_per_sec: 4.0,
                max_per_sec: 6.0,
            }),
            reads: Some(RateWindowRecord {
                total: 5,
                avg_per_sec: 2.5,
                p1_per_sec: 2.0,
                p50_per_sec: 2.0,
                p95_per_sec: 3.0,
                p99_per_sec: 3.0,
                min_per_sec: 2.0,
                max_per_sec: 3.0,
            }),
            writes: Some(RateWindowRecord {
                total: 5,
                avg_per_sec: 2.5,
                p1_per_sec: 2.0,
                p50_per_sec: 2.0,
                p95_per_sec: 3.0,
                p99_per_sec: 3.0,
                min_per_sec: 2.0,
                max_per_sec: 3.0,
            }),
            logical_bytes: Some(RateWindowRecord {
                total: 4200,
                avg_per_sec: 2100.0,
                p1_per_sec: 1680.0,
                p50_per_sec: 1680.0,
                p95_per_sec: 2520.0,
                p99_per_sec: 2520.0,
                min_per_sec: 1680.0,
                max_per_sec: 2520.0,
            }),
            read_logical_bytes: Some(RateWindowRecord {
                total: 2100,
                avg_per_sec: 1050.0,
                p1_per_sec: 840.0,
                p50_per_sec: 840.0,
                p95_per_sec: 1260.0,
                p99_per_sec: 1260.0,
                min_per_sec: 840.0,
                max_per_sec: 1260.0,
            }),
            write_logical_bytes: Some(RateWindowRecord {
                total: 2100,
                avg_per_sec: 1050.0,
                p1_per_sec: 840.0,
                p50_per_sec: 840.0,
                p95_per_sec: 1260.0,
                p99_per_sec: 1260.0,
                min_per_sec: 840.0,
                max_per_sec: 1260.0,
            }),
            scan_rows: None,
        });

        let json = serde_json::to_value(&measurement.record).expect("serialize record");
        assert_eq!(json["schema"], JSON_SCHEMA_V2);
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
        assert_eq!(json["throughput"]["window_secs"], 1);
        assert_eq!(json["throughput"]["operations"]["total"], 10);
        assert_eq!(json["throughput"]["logical_bytes"]["p95_per_sec"], 2520.0);
        assert_eq!(
            json["throughput"]["read_logical_bytes"]["p95_per_sec"],
            1260.0
        );
        assert_eq!(
            json["throughput"]["write_logical_bytes"]["p95_per_sec"],
            1260.0
        );
        assert!(json["throughput"].get("scan_rows").is_none());
        assert_eq!(json["drain"]["background_drain_status"], "not_requested");
        assert_eq!(
            json["counter_snapshots"]["before"]["block_cache_entry_count"],
            0
        );
        assert_eq!(
            json["counter_snapshots"]["after"]["block_cache_entry_count"],
            10
        );
        assert_eq!(
            json["counter_snapshots"]["after"]["block_cache_hit_count"],
            7
        );
        assert_eq!(
            json["counter_snapshots"]["after"]["block_cache_miss_count"],
            3
        );
        assert_eq!(
            json["counter_snapshots"]["after"]["block_cache_admitted_count"],
            4
        );
        assert_eq!(
            json["counter_snapshots"]["after"]["block_cache_rejected_count"],
            1
        );
        assert_eq!(
            json["counter_snapshots"]["after"]["block_cache_evicted_count"],
            2
        );
        assert_eq!(json["counter_snapshots"]["after"]["wal_commit_groups"], 5);
        assert_eq!(
            json["counter_snapshots"]["after"]["wal_commit_solo_groups"],
            2
        );
        assert_eq!(json["counter_snapshots"]["after"]["wal_commit_buffers"], 8);
        assert_eq!(json["counter_snapshots"]["after"]["wal_commit_bytes"], 4096);
        assert_eq!(
            json["counter_snapshots"]["after"]["value_cache_hit_count"],
            2
        );
    }

    #[test]
    fn legacy_write_perf_v1_fixture_parses_without_steady_state_fields() {
        let cfg = legacy_cfg();
        let options = cfg.build_options(false, false);
        let measurement = make_measurement(
            &cfg,
            "fillseq",
            "write",
            &options,
            MeasurementParams {
                num: Some(1000),
                value_size: Some(400),
                threads: Some(1),
                seed: Some(42),
                ..MeasurementParams::default()
            },
            MeasurementResult {
                measure_elapsed_ms: 1.0,
                ops: Some(1000),
                ops_per_sec: Some(1000.0),
                entries: Some(1000),
                entries_per_sec: Some(1000.0),
                writes: Some(1000),
                writes_per_sec: Some(1000.0),
                ..MeasurementResult::default()
            },
            MeasurementCounters::default(),
        );
        let json = serde_json::to_value(&measurement.record).expect("serialize v1 fixture");
        assert!(json.get("counter_snapshots").is_none());
        let record = parse_schema_fixture(json);

        assert_eq!(record.schema, JSON_SCHEMA_V1);
        assert_eq!(record.workload, "fillseq");
        assert!(record.phase.is_none());
        assert!(record.task.is_none());
        assert!(record.validation.is_none());
        assert_eq!(record.result.ops, Some(1000));
    }

    #[test]
    fn validate_json_artifact_accepts_legacy_and_steady_state_rows() {
        let cfg = legacy_cfg();
        let options = cfg.build_options(false, false);
        let legacy = make_measurement(
            &cfg,
            "fillseq",
            "write",
            &options,
            MeasurementParams {
                num: Some(1000),
                value_size: Some(400),
                ..MeasurementParams::default()
            },
            MeasurementResult {
                measure_elapsed_ms: 1.0,
                ops: Some(1000),
                ..MeasurementResult::default()
            },
            MeasurementCounters::default(),
        );
        let steady_state = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        let dir = tempfile::tempdir().expect("tempdir");
        let artifact = dir.path().join("write-perf.jsonl");
        fs::write(
            &artifact,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&legacy.record).expect("serialize legacy"),
                serde_json::to_string(&steady_state).expect("serialize steady-state")
            ),
        )
        .expect("write artifact");

        validate_json_artifact(&artifact).expect("artifact should validate");
    }

    #[test]
    fn validate_json_record_accepts_valid_prepare_golden_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let record = steady_state_prepare_schema_fixture(dir.path());

        validate_json_record(&record).expect("prepare row should validate");
    }

    #[test]
    fn validate_json_record_rejects_prepare_golden_manifest_param_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_prepare_schema_fixture(dir.path());
        record["params"]["value_size"] = serde_json::json!(401);

        let err = validate_json_record(&record).expect_err("prepare row should fail");

        assert!(
            err.to_string()
                .contains("params.value_size must match golden_manifest.value_size")
        );
    }

    #[test]
    fn validate_json_record_rejects_prepare_label_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_prepare_schema_fixture(dir.path());
        record["workload"] = serde_json::json!("point_read_uniform");

        let err = validate_json_record(&record).expect_err("prepare label should fail");

        assert!(
            err.to_string()
                .contains("prepare rows must use workload prepare_golden")
        );
    }

    #[test]
    fn validate_json_record_accepts_valid_embedded_golden_manifest() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] =
            serde_json::to_value(valid_golden_manifest(dir.path(), &cfg, &options))
                .expect("serialize manifest");

        validate_json_record(&record).expect("embedded manifest should validate");
    }

    #[test]
    fn validate_json_record_rejects_tampered_embedded_golden_manifest_digest() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.key_count += 1;
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] = serde_json::to_value(manifest).expect("serialize manifest");

        let err = validate_json_record(&record).expect_err("tampered manifest should fail");

        assert!(
            err.to_string()
                .contains("golden_manifest.manifest_digest does not match")
        );
    }

    #[test]
    fn validate_json_record_rejects_tampered_embedded_golden_manifest_options_hash() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.engine_options_hash = "fnv1a64:0000000000000000".to_string();
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("digest manifest");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] = serde_json::to_value(manifest).expect("serialize manifest");

        let err = validate_json_record(&record).expect_err("tampered options hash should fail");

        assert!(
            err.to_string()
                .contains("golden_manifest.engine_options_hash does not match")
        );
    }

    #[test]
    fn validate_json_record_rejects_legacy_hash_with_current_manifest_digest() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut manifest = valid_golden_manifest(dir.path(), &cfg, &options);
        manifest.engine_options_hash =
            legacy_engine_options_hash(&manifest.engine_options).expect("legacy options hash");
        manifest.manifest_digest = golden_manifest_digest(&manifest).expect("current digest");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] = serde_json::to_value(manifest).expect("serialize manifest");

        let err = validate_json_record(&record).expect_err("hybrid legacy hash should fail");

        assert!(
            err.to_string()
                .contains("golden_manifest.engine_options_hash does not match")
        );
    }

    #[test]
    fn validate_json_record_rejects_embedded_golden_manifest_param_mismatch() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] =
            serde_json::to_value(valid_golden_manifest(dir.path(), &cfg, &options))
                .expect("serialize manifest");
        record["params"]["num"] = serde_json::json!(999);

        let err = validate_json_record(&record).expect_err("manifest params should fail");

        assert!(
            err.to_string()
                .contains("params.num must match golden_manifest.key_count")
        );
    }

    #[test]
    fn validate_json_record_rejects_embedded_golden_manifest_missing_num() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] =
            serde_json::to_value(valid_golden_manifest(dir.path(), &cfg, &options))
                .expect("serialize manifest");
        record["params"]
            .as_object_mut()
            .expect("params object")
            .remove("num");

        let err = validate_json_record(&record).expect_err("missing manifest num should fail");

        assert!(err.to_string().contains("missing `num`"));
    }

    #[test]
    fn validate_json_record_rejects_embedded_golden_manifest_missing_value_size() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] =
            serde_json::to_value(valid_golden_manifest(dir.path(), &cfg, &options))
                .expect("serialize manifest");
        record["params"]
            .as_object_mut()
            .expect("params object")
            .remove("value_size");

        let err =
            validate_json_record(&record).expect_err("missing manifest value size should fail");

        assert!(err.to_string().contains("missing `value_size`"));
    }

    #[test]
    fn validate_json_record_rejects_embedded_golden_manifest_engine_options_mismatch() {
        let cfg = steady_state_schema_cfg();
        let options = cfg.build_options(false, false);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["golden_manifest"] =
            serde_json::to_value(valid_golden_manifest(dir.path(), &cfg, &options))
                .expect("serialize manifest");
        record["engine_options"]["cache_capacity"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("engine options mismatch should fail");

        assert!(
            err.to_string()
                .contains("engine_options must match golden_manifest.engine_options")
        );
    }

    #[test]
    fn validate_json_record_rejects_steady_state_validation_errors() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["validation"]["errors"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("validation errors should fail");

        assert!(err.to_string().contains("validation.errors must be zero"));
    }

    #[test]
    fn validate_json_record_rejects_missing_counter_snapshots() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record
            .as_object_mut()
            .expect("record object")
            .remove("counter_snapshots");

        let err = validate_json_record(&record).expect_err("missing snapshots should fail");

        assert!(err.to_string().contains("missing `counter_snapshots`"));
    }

    #[test]
    fn validate_json_record_rejects_empty_counter_snapshots() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["counter_snapshots"] = serde_json::json!({});

        let err = validate_json_record(&record).expect_err("empty snapshots should fail");

        assert!(err.to_string().contains("missing `before`"));
    }

    #[test]
    fn validate_json_record_rejects_empty_inner_counter_snapshots() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["counter_snapshots"]["before"] = serde_json::json!({});

        let err = validate_json_record(&record).expect_err("empty inner snapshot should fail");

        assert!(
            err.to_string()
                .contains("counter_snapshots.before must match measurement counters schema")
        );
    }

    #[test]
    fn validate_json_record_rejects_counter_delta_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["counter_snapshots"]["after"]["wal_commit_groups"] = serde_json::json!(5);

        let err = validate_json_record(&record).expect_err("counter delta mismatch should fail");

        assert!(err.to_string().contains(
            "counters must equal counter_snapshots.after minus counter_snapshots.before"
        ));
    }

    #[test]
    fn validate_json_record_rejects_decreasing_cumulative_counter_snapshot() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["counter_snapshots"]["before"]["wal_commit_groups"] = serde_json::json!(2);

        let err = validate_json_record(&record).expect_err("decreasing counter should fail");

        assert!(
            err.to_string()
                .contains("counter_snapshots.wal_commit_groups must be monotonic")
        );
    }

    #[test]
    fn validate_json_record_rejects_null_required_object() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["task"] = serde_json::Value::Null;

        let err = validate_json_record(&record).expect_err("null task should fail");

        assert!(err.to_string().contains("`task` must be an object"));
    }

    #[test]
    fn validate_json_record_rejects_empty_task() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["task"] = serde_json::json!({});

        let err = validate_json_record(&record).expect_err("empty task should fail");

        assert!(err.to_string().contains("missing `clients`"));
    }

    #[test]
    fn validate_json_record_rejects_task_param_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["task"]["operation_mix"] = serde_json::json!("get=1.0");

        let err = validate_json_record(&record).expect_err("task/params mismatch should fail");

        assert!(
            err.to_string()
                .contains("task.operation_mix must match params.operation_mix")
        );
    }

    #[test]
    fn validate_json_record_rejects_unknown_steady_state_workload() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["workload"] = serde_json::json!("unknown_steady_state");

        let err = validate_json_record(&record).expect_err("unknown workload should fail");

        assert!(
            err.to_string()
                .contains("unknown steady-state workload `unknown_steady_state`")
        );
    }

    #[test]
    fn validate_json_record_rejects_steady_state_v2_suite_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["suite"] = serde_json::json!("legacy");

        let err = validate_json_record(&record).expect_err("suite mismatch should fail");

        assert!(
            err.to_string()
                .contains("steady-state v2 rows must use suite steady_state")
        );
    }

    #[test]
    fn validate_json_record_rejects_task_shape_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["task"]["operation_mix_scheduler"] = serde_json::json!("closed_loop_scan");

        let err = validate_json_record(&record).expect_err("task shape should fail");

        assert!(
            err.to_string()
                .contains("task.operation_mix_scheduler must match workload")
        );
    }

    #[test]
    fn validate_json_record_rejects_zero_task_operation_mix_period() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["params"]["operation_mix_period"] = serde_json::json!(0);
        record["task"]["operation_mix_period"] = serde_json::json!(0);

        let err = validate_json_record(&record).expect_err("zero period should fail");

        assert!(
            err.to_string()
                .contains("task.operation_mix_period must be greater than zero")
        );
    }

    #[test]
    fn validate_json_record_rejects_measurement_label_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["measurement"] = serde_json::json!("range_scan");

        let err = validate_json_record(&record).expect_err("measurement mismatch should fail");

        assert!(
            err.to_string()
                .contains("point_read_uniform measurement must be point_get")
        );
    }

    #[test]
    fn validate_json_record_rejects_transaction_without_serializable_engine_options() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["engine_options"]["serializable"] = serde_json::json!(false);

        let err = validate_json_record(&record).expect_err("serializable option should fail");

        assert!(
            err.to_string()
                .contains("transaction_contention engine_options.serializable must be true")
        );
    }

    #[test]
    fn validate_json_record_rejects_transaction_task_fields_on_non_transaction() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["task"]["transaction_hot_set"] = serde_json::json!(128);

        let err =
            validate_json_record(&record).expect_err("stray transaction task field should fail");

        assert!(
            err.to_string()
                .contains("task.transaction_hot_set is only valid for transaction_contention")
        );
    }

    #[test]
    fn validate_json_record_rejects_point_read_hit_count_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["validation"]["read_hits"] = serde_json::json!(1);
        record["validation"]["expected_read_hits"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("hit count mismatch should fail");

        assert!(
            err.to_string().contains(
                "point_read_uniform validation.read_hits must equal completed_operations"
            )
        );
    }

    #[test]
    fn validate_json_record_rejects_zero_non_idle_operations() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "sustained_ingest",
            measurement: "write_closed_loop",
            key_selection: "unique_sequential",
            operation_mix: "put=1.0",
            operation_mix_scheduler: "closed_loop_unique_sequential",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: None,
            read_hits: 0,
            read_misses: 0,
            completed_operations: 0,
        });
        record["result"]["ops"] = serde_json::json!(0);
        record["validation"]["min_completed_operations"] = serde_json::json!(0);

        let err = validate_json_record(&record).expect_err("zero operations should fail");

        assert!(
            err.to_string()
                .contains("validation.completed_operations must be greater than zero for non-idle")
        );
    }

    #[test]
    fn validate_json_record_rejects_missing_non_idle_result_ops() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["result"]
            .as_object_mut()
            .expect("result object")
            .remove("ops");

        let err = validate_json_record(&record).expect_err("missing result ops should fail");

        assert!(err.to_string().contains("missing `ops`"));
    }

    #[test]
    fn validate_json_record_rejects_impossible_throughput_total() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["throughput"]["operations"]["total"] = serde_json::json!(129);
        record["throughput"]["operations"]["avg_per_sec"] = serde_json::json!(129.0);
        record["throughput"]["operations"]["p1_per_sec"] = serde_json::json!(129.0);
        record["throughput"]["operations"]["p50_per_sec"] = serde_json::json!(129.0);
        record["throughput"]["operations"]["p95_per_sec"] = serde_json::json!(129.0);
        record["throughput"]["operations"]["p99_per_sec"] = serde_json::json!(129.0);
        record["throughput"]["operations"]["min_per_sec"] = serde_json::json!(129.0);
        record["throughput"]["operations"]["max_per_sec"] = serde_json::json!(129.0);

        let err = validate_json_record(&record).expect_err("impossible throughput should fail");

        assert!(
            err.to_string()
                .contains("throughput.operations.total must not exceed")
        );
    }

    #[test]
    fn validate_json_record_rejects_inconsistent_throughput_average() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["throughput"]["operations"]["avg_per_sec"] = serde_json::json!(1.0);

        let err = validate_json_record(&record).expect_err("bad throughput avg should fail");

        assert!(
            err.to_string()
                .contains("throughput.operations.avg_per_sec must equal total / complete_windows")
        );
    }

    #[test]
    fn validate_json_record_rejects_non_one_second_throughput_windows() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["throughput"]["window_secs"] = serde_json::json!(60);

        let err = validate_json_record(&record).expect_err("window size should fail");

        assert!(err.to_string().contains("throughput.window_secs must be 1"));
    }

    #[test]
    fn validate_json_record_rejects_point_read_write_throughput() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["throughput"]["writes"] = record["throughput"]["operations"].clone();

        let err = validate_json_record(&record).expect_err("point-read writes should fail");

        assert!(
            err.to_string()
                .contains("point_read_uniform throughput.writes must be absent")
        );
    }

    #[test]
    fn validate_json_record_rejects_ingest_read_throughput() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "sustained_ingest",
            measurement: "write_closed_loop",
            key_selection: "unique_sequential",
            operation_mix: "put=1.0",
            operation_mix_scheduler: "closed_loop_unique_sequential",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: None,
            read_hits: 0,
            read_misses: 0,
            completed_operations: 128,
        });
        record["throughput"]["reads"] = record["throughput"]["operations"].clone();

        let err = validate_json_record(&record).expect_err("ingest reads should fail");

        assert!(
            err.to_string()
                .contains("sustained_ingest throughput.reads must be absent")
        );
    }

    #[test]
    fn validate_json_record_rejects_transaction_throughput_ratio_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["throughput"]["writes"] =
            serde_json::to_value(fixture_rate_window(49).expect("rate window"))
                .expect("serialize rate window");

        let err = validate_json_record(&record).expect_err("transaction writes should fail");

        assert!(err.to_string().contains(
            "transaction_contention throughput.writes.total must equal operations.total * task.transaction_updates"
        ));
    }

    #[test]
    fn validate_json_record_rejects_transaction_reconciliation_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["validation"]["transaction_attempts"] = serde_json::json!(0);
        record["validation"]["transaction_commits"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("transaction mismatch should fail");

        assert!(
            err.to_string()
                .contains("transaction attempts must equal commits plus conflicts")
        );
    }

    #[test]
    fn validate_json_record_rejects_transaction_attempt_overflow() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["validation"]["transaction_attempts"] = serde_json::json!(0);
        record["validation"]["transaction_commits"] = serde_json::json!(u64::MAX);
        record["validation"]["transaction_conflicts"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("attempt overflow should fail");

        assert!(
            err.to_string()
                .contains("transaction commit/conflict sum overflowed")
        );
    }

    #[test]
    fn validate_json_record_rejects_transaction_read_hit_overflow() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        let commits = u64::MAX / 5 + 1;
        record["validation"]["transaction_attempts"] = serde_json::json!(commits);
        record["validation"]["transaction_commits"] = serde_json::json!(commits);
        record["validation"]["selected_operations"] = serde_json::json!(commits);
        record["validation"]["completed_operations"] = serde_json::json!(commits);
        record["validation"]["complete_period_operations"] = serde_json::json!(commits);

        let err = validate_json_record(&record).expect_err("transaction reads should overflow");

        assert!(
            err.to_string()
                .contains("transaction_contention read hit expectation overflowed")
        );
    }

    #[test]
    fn validate_json_record_rejects_transaction_operation_counter_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["validation"]["selected_operations"] = serde_json::json!(9);

        let err = validate_json_record(&record).expect_err("transaction counters should fail");

        assert!(err.to_string().contains(
            "transaction_contention selected_operations must equal transaction_attempts"
        ));
    }

    #[test]
    fn validate_json_record_accepts_transaction_latency_counted_by_attempts() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["validation"]["transaction_attempts"] = serde_json::json!(12);
        record["validation"]["transaction_conflicts"] = serde_json::json!(2);
        record["validation"]["selected_operations"] = serde_json::json!(12);
        record["latency"]["unsampled_completed_operations"] = serde_json::json!(11);

        validate_json_record(&record).expect("transaction latency should count attempts");
    }

    #[test]
    fn validate_json_record_rejects_transaction_read_hit_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "transaction_contention",
            measurement: "serializable_hot_set",
            key_selection: "uniform_hot_set",
            operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
            operation_mix_scheduler: "closed_loop_serializable_transaction",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(50),
            total_nexts: None,
            read_hits: 50,
            read_misses: 0,
            completed_operations: 10,
        });
        record["validation"]["read_hits"] = serde_json::json!(49);
        record["validation"]["expected_read_hits"] = serde_json::json!(49);

        let err = validate_json_record(&record).expect_err("transaction reads should fail");

        assert!(
            err.to_string().contains(
                "transaction_contention validation.read_hits must equal completed_operations * task.transaction_reads"
            )
        );
    }

    #[test]
    fn validate_json_record_rejects_missing_read_hit_count() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_missing_in_range",
            measurement: "negative_point_get",
            key_selection: "uniform_absent_reserved_padding",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(0),
            total_nexts: None,
            read_hits: 0,
            read_misses: 64,
            completed_operations: 64,
        });
        record["validation"]["read_hits"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("missing-read hit count should fail");

        assert!(
            err.to_string()
                .contains("point_read_missing_in_range validation.read_hits must be zero")
        );
    }

    #[test]
    fn validate_json_record_rejects_missing_read_miss_count_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_missing_in_range",
            measurement: "negative_point_get",
            key_selection: "uniform_absent_reserved_padding",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(0),
            total_nexts: None,
            read_hits: 0,
            read_misses: 64,
            completed_operations: 64,
        });
        record["validation"]["read_misses"] = serde_json::json!(1);
        record["validation"]["expected_read_misses"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("missing-read miss count should fail");

        assert!(err.to_string().contains(
            "point_read_missing_in_range validation.read_misses must equal completed_operations"
        ));
    }

    #[test]
    fn validate_json_record_rejects_range_scan_errors_with_zero_validation_errors() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "range_scan_uniform",
            measurement: "range_scan",
            key_selection: "uniform",
            operation_mix: "scan=1.0",
            operation_mix_scheduler: "closed_loop_scan",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: Some(320),
            read_hits: 0,
            read_misses: 0,
            completed_operations: 32,
        });
        record["validation"]["scan_count_errors"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("scan counter should fail");

        assert!(
            err.to_string()
                .contains("range_scan_uniform validation.scan_count_errors must be zero")
        );
    }

    #[test]
    fn validate_json_record_rejects_mixed_tail_reconciliation_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["validation"]["tail_operations"] = serde_json::json!(80);
        record["validation"]["tail_gets"] = serde_json::json!(39);
        record["validation"]["tail_puts"] = serde_json::json!(40);

        let err = validate_json_record(&record).expect_err("tail mismatch should fail");

        assert!(
            err.to_string()
                .contains("validation tail_gets plus tail_puts must equal tail_operations")
        );
    }

    #[test]
    fn validate_json_record_rejects_complete_period_tail_sum_overflow() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["validation"]["complete_period_operations"] = serde_json::json!(u64::MAX);
        record["validation"]["tail_operations"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("period/tail overflow should fail");

        assert!(
            err.to_string()
                .contains("validation complete-period and tail operation sum overflowed")
        );
    }

    #[test]
    fn validate_json_record_rejects_tail_get_put_sum_overflow() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["validation"]["selected_operations"] = serde_json::json!(0);
        record["validation"]["complete_period_operations"] = serde_json::json!(0);
        record["validation"]["tail_operations"] = serde_json::json!(0);
        record["validation"]["tail_gets"] = serde_json::json!(u64::MAX);
        record["validation"]["tail_puts"] = serde_json::json!(1);

        let err = validate_json_record(&record).expect_err("tail counter overflow should fail");

        assert!(
            err.to_string()
                .contains("validation tail get/put operation sum overflowed")
        );
    }

    #[test]
    fn validate_json_record_rejects_mixed_operation_count_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["validation"]["read_hits"] = serde_json::json!(41);
        record["validation"]["expected_read_hits"] = serde_json::json!(41);

        let err = validate_json_record(&record).expect_err("mixed counts should fail");

        assert!(err.to_string().contains(
            "balanced_zipfian validation.read_hits must match task operation mix and tail_gets"
        ));
    }

    #[test]
    fn validate_json_record_accepts_aggregate_mixed_tail_from_multiple_clients() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["result"]["ops"] = serde_json::json!(2200);
        record["result"]["found"] = serde_json::json!(1100);
        record["validation"]["read_hits"] = serde_json::json!(1100);
        record["validation"]["expected_read_hits"] = serde_json::json!(1100);
        record["validation"]["selected_operations"] = serde_json::json!(2200);
        record["validation"]["completed_operations"] = serde_json::json!(2200);
        record["validation"]["complete_period_operations"] = serde_json::json!(1000);
        record["validation"]["tail_operations"] = serde_json::json!(1200);
        record["validation"]["tail_gets"] = serde_json::json!(600);
        record["validation"]["tail_puts"] = serde_json::json!(600);
        record["latency"]["unsampled_completed_operations"] = serde_json::json!(2199);
        record["throughput"]["operations"] =
            serde_json::to_value(fixture_rate_window(2200).expect("rate window"))
                .expect("serialize rate window");
        record["throughput"]["reads"] =
            serde_json::to_value(fixture_rate_window(1100).expect("rate window"))
                .expect("serialize rate window");
        record["throughput"]["writes"] =
            serde_json::to_value(fixture_rate_window(1100).expect("rate window"))
                .expect("serialize rate window");

        validate_json_record(&record).expect("aggregate mixed tail should validate");
    }

    #[test]
    fn validate_json_record_rejects_mixed_observed_mix_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "balanced_zipfian",
            measurement: "mixed_closed_loop",
            key_selection: "scrambled_zipfian_0.99",
            operation_mix: "get=0.5,put=0.5",
            operation_mix_scheduler: "per_client_shuffled_period_cycle",
            scramble_function: "splitmix64(rank) % record_count",
            zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
            result_found: Some(40),
            total_nexts: None,
            read_hits: 40,
            read_misses: 0,
            completed_operations: 80,
        });
        record["validation"]["observed_operation_mix"] =
            serde_json::json!("get=1.000000,put=0.000000");

        let err = validate_json_record(&record).expect_err("observed mix should fail");

        assert!(err.to_string().contains(
            "balanced_zipfian observed_operation_mix must match observed get/put counts"
        ));
    }

    #[test]
    fn validate_json_record_rejects_missing_non_idle_latency() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "range_scan_uniform",
            measurement: "range_scan",
            key_selection: "uniform",
            operation_mix: "scan=1.0",
            operation_mix_scheduler: "closed_loop_scan",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: Some(320),
            read_hits: 0,
            read_misses: 0,
            completed_operations: 32,
        });
        record
            .as_object_mut()
            .expect("record object")
            .remove("latency");

        let err = validate_json_record(&record).expect_err("missing latency should fail");

        assert!(err.to_string().contains("missing `latency`"));
    }

    #[test]
    fn validate_json_record_rejects_latency_count_mismatch() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["latency"]["unsampled_completed_operations"] = serde_json::json!(0);

        let err = validate_json_record(&record).expect_err("latency count should fail");

        assert!(err.to_string().contains(
            "latency samples plus unsampled operations must match validation.selected_operations"
        ));
    }

    #[test]
    fn validate_json_record_rejects_impossible_latency_sample_count() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["latency"]["samples"] = serde_json::json!(128);
        record["latency"]["unsampled_completed_operations"] = serde_json::json!(0);

        let err = validate_json_record(&record).expect_err("sample cadence should fail");

        assert!(
            err.to_string().contains(
                "latency.samples exceeds maximum possible samples for task.clients and latency.sample_every"
            )
        );
    }

    #[test]
    fn validate_json_record_rejects_unordered_latency_percentiles() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "point_read_uniform",
            measurement: "point_get",
            key_selection: "uniform",
            operation_mix: "get=1.0",
            operation_mix_scheduler: "closed_loop_read_only",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: Some(128),
            total_nexts: None,
            read_hits: 128,
            read_misses: 0,
            completed_operations: 128,
        });
        record["latency"]["p99_ms"] = serde_json::json!(0.0);

        let err = validate_json_record(&record).expect_err("latency order should fail");

        assert!(
            err.to_string()
                .contains("latency percentile values must be ordered")
        );
    }

    #[test]
    fn validate_json_record_accepts_idle_without_latency_or_throughput() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "idle",
            measurement: "idle_wait",
            key_selection: "none",
            operation_mix: "idle=1.0",
            operation_mix_scheduler: "idle_wait",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: None,
            read_hits: 0,
            read_misses: 0,
            completed_operations: 0,
        });
        record
            .as_object_mut()
            .expect("record object")
            .remove("latency");
        record
            .as_object_mut()
            .expect("record object")
            .remove("throughput");
        record["result"]["ops"] = serde_json::json!(0);
        record["validation"]["min_completed_operations"] = serde_json::json!(0);

        validate_json_record(&record).expect("idle row should validate");
    }

    #[test]
    fn validate_json_record_rejects_idle_with_completed_operations() {
        let mut record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "idle",
            measurement: "idle_wait",
            key_selection: "none",
            operation_mix: "idle=1.0",
            operation_mix_scheduler: "idle_wait",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: None,
            read_hits: 0,
            read_misses: 0,
            completed_operations: 0,
        });
        record
            .as_object_mut()
            .expect("record object")
            .remove("latency");
        record
            .as_object_mut()
            .expect("record object")
            .remove("throughput");
        record["result"]["ops"] = serde_json::json!(100);
        record["validation"]["completed_operations"] = serde_json::json!(100);
        record["validation"]["min_completed_operations"] = serde_json::json!(0);

        let err = validate_json_record(&record).expect_err("busy idle row should fail");

        assert!(
            err.to_string()
                .contains("idle validation.completed_operations must be zero")
        );
    }

    #[test]
    fn steady_state_schema_fixtures_parse_required_workload_shapes() {
        let fixtures = [
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "point_read_uniform",
                measurement: "point_get",
                key_selection: "uniform",
                operation_mix: "get=1.0",
                operation_mix_scheduler: "closed_loop_read_only",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: Some(128),
                total_nexts: None,
                read_hits: 128,
                read_misses: 0,
                completed_operations: 128,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "point_read_zipfian",
                measurement: "point_get",
                key_selection: "scrambled_zipfian_0.99",
                operation_mix: "get=1.0",
                operation_mix_scheduler: "closed_loop_read_only",
                scramble_function: "splitmix64(rank) % record_count",
                zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
                result_found: Some(128),
                total_nexts: None,
                read_hits: 128,
                read_misses: 0,
                completed_operations: 128,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "point_read_missing_in_range",
                measurement: "negative_point_get",
                key_selection: "uniform_absent_reserved_padding",
                operation_mix: "get=1.0",
                operation_mix_scheduler: "closed_loop_read_only",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: Some(0),
                total_nexts: None,
                read_hits: 0,
                read_misses: 64,
                completed_operations: 64,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "range_scan_uniform",
                measurement: "range_scan",
                key_selection: "uniform",
                operation_mix: "scan=1.0",
                operation_mix_scheduler: "closed_loop_scan",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: None,
                total_nexts: Some(320),
                read_hits: 0,
                read_misses: 0,
                completed_operations: 32,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "read_heavy_zipfian",
                measurement: "mixed_closed_loop",
                key_selection: "scrambled_zipfian_0.99",
                operation_mix: "get=0.95,put=0.05",
                operation_mix_scheduler: "per_client_shuffled_period_cycle",
                scramble_function: "splitmix64(rank) % record_count",
                zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
                result_found: Some(76),
                total_nexts: None,
                read_hits: 76,
                read_misses: 0,
                completed_operations: 80,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "balanced_zipfian",
                measurement: "mixed_closed_loop",
                key_selection: "scrambled_zipfian_0.99",
                operation_mix: "get=0.5,put=0.5",
                operation_mix_scheduler: "per_client_shuffled_period_cycle",
                scramble_function: "splitmix64(rank) % record_count",
                zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
                result_found: Some(40),
                total_nexts: None,
                read_hits: 40,
                read_misses: 0,
                completed_operations: 80,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "update_heavy_zipfian",
                measurement: "mixed_closed_loop",
                key_selection: "scrambled_zipfian_0.99",
                operation_mix: "get=0.05,put=0.95",
                operation_mix_scheduler: "per_client_shuffled_period_cycle",
                scramble_function: "splitmix64(rank) % record_count",
                zipfian_exponent: Some(STEADY_STATE_ZIPFIAN_EXPONENT),
                result_found: Some(4),
                total_nexts: None,
                read_hits: 4,
                read_misses: 0,
                completed_operations: 80,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "sustained_ingest",
                measurement: "write_closed_loop",
                key_selection: "unique_sequential",
                operation_mix: "put=1.0",
                operation_mix_scheduler: "closed_loop_unique_sequential",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: None,
                total_nexts: None,
                read_hits: 0,
                read_misses: 0,
                completed_operations: 128,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "transaction_contention",
                measurement: "serializable_hot_set",
                key_selection: "uniform_hot_set",
                operation_mix: "txn_reads=5,txn_updates=5,txn_retries=0",
                operation_mix_scheduler: "closed_loop_serializable_transaction",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: Some(640),
                total_nexts: None,
                read_hits: 640,
                read_misses: 0,
                completed_operations: 128,
            }),
            steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "idle",
                measurement: "idle_wait",
                key_selection: "none",
                operation_mix: "idle=1.0",
                operation_mix_scheduler: "idle_wait",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: None,
                total_nexts: None,
                read_hits: 0,
                read_misses: 0,
                completed_operations: 0,
            }),
        ];

        for fixture in fixtures {
            if fixture["workload"] == "point_read_uniform" {
                assert!(
                    !fixture["result"]
                        .as_object()
                        .unwrap()
                        .contains_key("total_nexts")
                );
            }
            let record = parse_schema_fixture(fixture);
            let task = record.task.expect("steady-state fixture includes task");
            let validation = record
                .validation
                .expect("steady-state fixture includes validation");

            assert_eq!(record.schema, JSON_SCHEMA_V2);
            assert_eq!(record.phase.as_deref(), Some("measurement"));
            assert!(!record.workload.is_empty());
            assert!(!task.operation_mix.is_empty());
            assert!(!task.key_selection.is_empty());
            assert!(!task.operation_mix_scheduler.is_empty());
            assert!(!task.scramble_function.is_empty());
            assert_eq!(record.result.ops, Some(validation.completed_operations));
            if record.workload == "idle" {
                assert_eq!(validation.completed_operations, 0);
            } else {
                assert!(validation.completed_operations >= 1);
            }
            if matches!(
                record.workload.as_str(),
                "point_read_zipfian"
                    | "read_heavy_zipfian"
                    | "balanced_zipfian"
                    | "update_heavy_zipfian"
            ) {
                assert_eq!(task.scramble_function, "splitmix64(rank) % record_count");
                assert_eq!(task.zipfian_exponent, Some(STEADY_STATE_ZIPFIAN_EXPONENT));
            }
            if matches!(
                record.workload.as_str(),
                "read_heavy_zipfian" | "balanced_zipfian" | "update_heavy_zipfian"
            ) {
                assert_eq!(
                    task.operation_mix_scheduler,
                    "per_client_shuffled_period_cycle"
                );
            }
            if record.workload == "transaction_contention" {
                assert_eq!(task.transaction_hot_set, Some(128));
                assert_eq!(task.transaction_reads, Some(5));
                assert_eq!(task.transaction_updates, Some(5));
                assert_eq!(task.transaction_retries, Some(0));
                assert_eq!(task.transaction_conflict_latency, Some(true));
            }
        }
    }

    #[test]
    fn missing_read_fixture_keeps_found_distinct_from_read_hits() {
        let record =
            parse_schema_fixture(steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
                workload: "point_read_missing_in_range",
                measurement: "negative_point_get",
                key_selection: "uniform_absent_reserved_padding",
                operation_mix: "get=1.0",
                operation_mix_scheduler: "closed_loop_read_only",
                scramble_function: "none",
                zipfian_exponent: None,
                result_found: Some(0),
                total_nexts: None,
                read_hits: 0,
                read_misses: 64,
                completed_operations: 64,
            }));
        let validation = record.validation.expect("validation");

        assert_eq!(record.result.found, Some(0));
        assert_eq!(validation.read_hits, 0);
        assert_eq!(validation.read_misses, 64);
        assert_ne!(record.result.found, Some(validation.read_misses));
    }

    #[test]
    fn scan_fixture_keeps_found_unmapped_from_read_hits() {
        let record = steady_state_schema_fixture(SteadyStateSchemaFixtureCase {
            workload: "range_scan_uniform",
            measurement: "range_scan",
            key_selection: "uniform",
            operation_mix: "scan=1.0",
            operation_mix_scheduler: "closed_loop_scan",
            scramble_function: "none",
            zipfian_exponent: None,
            result_found: None,
            total_nexts: Some(320),
            read_hits: 0,
            read_misses: 0,
            completed_operations: 32,
        });
        assert!(!record["result"].as_object().unwrap().contains_key("found"));
        let record = parse_schema_fixture(record);
        let validation = record.validation.expect("validation");

        assert_eq!(record.result.found, None);
        assert_ne!(record.result.found, Some(validation.read_hits));
        assert_eq!(record.result.total_nexts, Some(320));
    }

    #[test]
    fn throughput_record_reports_complete_window_percentiles() {
        let stats = SteadyStateWindowStats {
            rate_windows: vec![
                SteadyStateRateWindow {
                    operations: 10,
                    reads: 8,
                    writes: 2,
                    logical_bytes: 840,
                    read_logical_bytes: 672,
                    write_logical_bytes: 168,
                    scan_rows: 0,
                },
                SteadyStateRateWindow {
                    operations: 20,
                    reads: 12,
                    writes: 8,
                    logical_bytes: 1680,
                    read_logical_bytes: 1008,
                    write_logical_bytes: 672,
                    scan_rows: 0,
                },
                SteadyStateRateWindow {
                    operations: 30,
                    reads: 20,
                    writes: 10,
                    logical_bytes: 2520,
                    read_logical_bytes: 1680,
                    write_logical_bytes: 840,
                    scan_rows: 0,
                },
            ],
            ..SteadyStateWindowStats::default()
        };

        let record = throughput_record(&stats).expect("throughput record");
        let operations = record.operations.expect("operation windows");
        assert_eq!(record.complete_windows, 3);
        assert_eq!(operations.total, 60);
        assert_eq!(operations.avg_per_sec, 20.0);
        assert_eq!(operations.p50_per_sec, 20.0);
        assert_eq!(operations.p95_per_sec, 30.0);
        assert_eq!(record.read_logical_bytes.expect("read bytes").total, 3360);
        assert_eq!(record.write_logical_bytes.expect("write bytes").total, 1680);
        assert!(record.scan_rows.is_none());
    }

    #[test]
    fn merge_grows_rate_windows_to_the_longest_worker() {
        let mut stats = SteadyStateWindowStats::default();
        stats.merge(SteadyStateWindowStats {
            rate_windows: vec![
                SteadyStateRateWindow {
                    operations: 3,
                    ..SteadyStateRateWindow::default()
                },
                SteadyStateRateWindow {
                    operations: 4,
                    ..SteadyStateRateWindow::default()
                },
            ],
            ..SteadyStateWindowStats::default()
        });
        stats.merge(SteadyStateWindowStats {
            rate_windows: vec![SteadyStateRateWindow {
                operations: 5,
                ..SteadyStateRateWindow::default()
            }],
            ..SteadyStateWindowStats::default()
        });

        assert_eq!(stats.rate_windows.len(), 2);
        assert_eq!(stats.rate_windows[0].operations, 8);
        assert_eq!(stats.rate_windows[1].operations, 4);
    }

    #[test]
    fn throughput_record_is_absent_without_windows() {
        assert!(throughput_record(&SteadyStateWindowStats::default()).is_none());
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
    fn write_only_operation_mix_rejects_reads() {
        let err = validate_write_only_operation_mix("put=0.5,get=0.5", 1000)
            .expect_err("write-only workloads should reject read mix");

        assert!(err.to_string().contains("require --operation-mix put=1.0"));
    }

    #[test]
    fn write_only_operation_mix_rejects_unknown_zero_ratio_operation() {
        let err = validate_write_only_operation_mix("put=1.0,delete=0.0", 1000)
            .expect_err("write-only workloads should reject unknown operations");

        assert!(err.to_string().contains("do not support delete"));
    }

    #[test]
    fn write_only_operation_mix_accepts_equivalent_put_only() {
        validate_write_only_operation_mix("put=1.0,get=0.0", 1000).expect("write-only mix");
    }

    #[test]
    fn sustained_ingest_defaults_to_canonical_put_mix() {
        let cfg = steady_state_cfg();
        let (operation_mix, operation_mix_period) =
            resolved_write_only_operation_mix(&cfg).expect("resolve mix");

        assert_eq!(operation_mix, "put=1.0");
        assert_eq!(operation_mix_period, 1);
    }

    #[test]
    fn sustained_ingest_preserves_user_write_only_mix_metadata() {
        let cfg = HarnessConfig::from_args(
            Args::try_parse_from([
                "write-perf",
                "--suite",
                "steady-state",
                "--operation-mix",
                "put=1.0,get=0.0",
                "--operation-mix-period",
                "1000",
            ])
            .expect("parse args"),
        );
        let (operation_mix, operation_mix_period) =
            resolved_write_only_operation_mix(&cfg).expect("resolve mix");

        assert_eq!(operation_mix, "put=1.0,get=0.0");
        assert_eq!(operation_mix_period, 1000);
    }

    #[test]
    fn transaction_contention_parses_config_knobs() {
        let cfg = HarnessConfig::from_args(
            Args::try_parse_from([
                "write-perf",
                "--suite",
                "steady-state",
                "--bench",
                "transaction_contention",
                "--transaction-hot-set",
                "64",
                "--transaction-reads",
                "3",
                "--transaction-updates",
                "2",
                "--transaction-retries",
                "1",
            ])
            .expect("parse args"),
        );

        assert_eq!(cfg.transaction_hot_set, 64);
        assert_eq!(cfg.transaction_reads, 3);
        assert_eq!(cfg.transaction_updates, 2);
        assert_eq!(cfg.transaction_retries, 1);
        validate_config(&cfg).expect("transaction config");
    }

    #[test]
    fn transaction_contention_rejects_oversized_hot_set() {
        let cfg = HarnessConfig::from_args(
            Args::try_parse_from([
                "write-perf",
                "--suite",
                "steady-state",
                "--num",
                "10",
                "--transaction-hot-set",
                "11",
            ])
            .expect("parse args"),
        );
        let err = validate_config(&cfg).expect_err("hot set above record count should fail");

        assert!(err.to_string().contains("--transaction-hot-set"));
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
            transaction_hot_set: 1,
            transaction_reads: 1,
            transaction_updates: 1,
            transaction_retries: 0,
            prepare_golden: false,
            golden_path: None,
            clone_golden: false,
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

    #[test]
    fn steady_state_rejects_zero_clients() {
        let args =
            Args::try_parse_from(["write-perf", "--suite", "steady-state", "--clients", "0"])
                .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("zero clients should fail");

        assert!(err.to_string().contains("--clients must be > 0"));
    }

    #[test]
    fn steady_state_rejects_zero_latency_sample_every() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--latency-sample-every",
            "0",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("zero latency sample period should fail");

        assert!(
            err.to_string()
                .contains("--latency-sample-every must be > 0")
        );
    }

    #[test]
    fn steady_state_rejects_zero_settle_timeout() {
        let args = Args::try_parse_from([
            "write-perf",
            "--suite",
            "steady-state",
            "--prepare-golden",
            "--golden-path",
            "/tmp/toykv-golden",
            "--settle-timeout-secs",
            "0",
        ])
        .expect("parse args");
        let cfg = HarnessConfig::from_args(args);
        let err = validate_config(&cfg).expect_err("zero settle timeout should fail");

        assert!(
            err.to_string()
                .contains("--settle-timeout-secs must be > 0")
        );
    }
}
