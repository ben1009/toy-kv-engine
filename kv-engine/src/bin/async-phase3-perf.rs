mod wrapper;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use wrapper::kv_engine_wrapper;

use kv_engine_wrapper::{
    block_on,
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    lsm_storage::{KvEngine, LsmStorageOptions, PrefixBloomOptions},
    vlog::ValueSeparationOptions,
};

const JSON_SCHEMA: &str = "kv-engine.async-phase3-perf.v1";
const ENGINE_NAME: &str = "kv-engine";

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum OutputFormat {
    Text,
    Json,
    Both,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum OpenMode {
    Sync,
    Async,
    Both,
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
#[command(name = "async-phase3-perf")]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Prepare(PrepareArgs),
    MeasureOpenClose(MeasureOpenCloseArgs),
}

#[derive(Parser, Debug, Clone)]
struct CommonArgs {
    #[arg(long, default_value = "/tmp/async-phase3-perf")]
    path: PathBuf,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,
    #[arg(long, value_enum, default_value_t = CompactionMode::Leveled)]
    compaction: CompactionMode,
    #[arg(long, default_value_t = false)]
    wal: bool,
    #[arg(long, default_value_t = false)]
    vlog: bool,
    #[arg(long, default_value_t = 8192)]
    cache_capacity: u64,
    #[arg(long, default_value_t = 1 << 20)]
    target_sst_size: usize,
    #[arg(long, default_value_t = 2)]
    memtable_limit: usize,
}

#[derive(Parser, Debug, Clone)]
struct PrepareArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, default_value_t = 2_000_000)]
    num_keys: usize,
    #[arg(long, default_value_t = 1024)]
    value_size: usize,
    #[arg(long, default_value_t = 50_000)]
    flush_every: usize,
    #[arg(long, default_value_t = true)]
    reset: bool,
}

#[derive(Parser, Debug, Clone)]
struct MeasureOpenCloseArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, value_enum, default_value_t = OpenMode::Both)]
    mode: OpenMode,
    #[arg(long, default_value_t = 5)]
    repetitions: usize,
}

#[derive(Clone)]
struct RunConfig {
    path: PathBuf,
    compaction: CompactionMode,
    wal: bool,
    vlog: bool,
    cache_capacity: u64,
    target_sst_size: usize,
    memtable_limit: usize,
}

impl RunConfig {
    fn from_common(common: &CommonArgs) -> Result<Self> {
        if common.cache_capacity == 0 {
            bail!("--cache-capacity must be greater than 0");
        }
        Ok(Self {
            path: common.path.clone(),
            compaction: common.compaction,
            wal: common.wal,
            vlog: common.vlog,
            cache_capacity: common.cache_capacity,
            target_sst_size: common.target_sst_size,
            memtable_limit: common.memtable_limit,
        })
    }

    fn build_options(&self) -> LsmStorageOptions {
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
            enable_wal: self.wal,
            serializable: false,
            value_separation: if self.vlog {
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
}

#[derive(Serialize)]
struct PrepareRecord {
    schema: &'static str,
    unix_epoch_ms: u64,
    engine: &'static str,
    command: &'static str,
    path: String,
    wal: bool,
    value_separation: bool,
    compaction: &'static str,
    num_keys: usize,
    value_size: usize,
    flush_every: usize,
    prepare_elapsed_ms: f64,
    close_elapsed_ms: f64,
    sst_files: usize,
    vlog_files: usize,
}

#[derive(Serialize)]
struct MeasureRecord {
    schema: &'static str,
    unix_epoch_ms: u64,
    engine: &'static str,
    command: &'static str,
    mode: &'static str,
    path: String,
    wal: bool,
    value_separation: bool,
    compaction: &'static str,
    repetitions: usize,
    open_samples_ms: Vec<f64>,
    close_samples_ms: Vec<f64>,
    open_median_ms: f64,
    close_median_ms: f64,
    sst_files: usize,
    vlog_files: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Command::Prepare(args) => run_prepare(args),
        Command::MeasureOpenClose(args) => run_measure_open_close(args),
    }
}

fn run_prepare(args: PrepareArgs) -> Result<()> {
    if args.flush_every == 0 {
        bail!("--flush-every must be greater than 0");
    }

    let cfg = RunConfig::from_common(&args.common)?;
    if args.reset && cfg.path.exists() {
        fs::remove_dir_all(&cfg.path)
            .with_context(|| format!("remove existing dataset {}", cfg.path.display()))?;
    }
    fs::create_dir_all(&cfg.path)
        .with_context(|| format!("create dataset dir {}", cfg.path.display()))?;

    let options = cfg.build_options();
    let value = vec![b'x'; args.value_size];
    let prepare_start = Instant::now();
    let engine = KvEngine::open(&cfg.path, options)?;
    for i in 0..args.num_keys {
        let key = format!("key{i:08}");
        engine.put(key.as_bytes(), &value)?;
        if (i + 1) % args.flush_every == 0 {
            engine.force_flush()?;
        }
    }
    engine.drain_flush()?;
    let prepare_elapsed = prepare_start.elapsed();
    let close_start = Instant::now();
    engine.close()?;
    let close_elapsed = close_start.elapsed();

    let (sst_files, vlog_files) = count_dataset_files(&cfg.path)?;
    let record = PrepareRecord {
        schema: JSON_SCHEMA,
        unix_epoch_ms: unix_epoch_ms(),
        engine: ENGINE_NAME,
        command: "prepare",
        path: cfg.path.display().to_string(),
        wal: cfg.wal,
        value_separation: cfg.vlog,
        compaction: compaction_name(cfg.compaction),
        num_keys: args.num_keys,
        value_size: args.value_size,
        flush_every: args.flush_every,
        prepare_elapsed_ms: ms(prepare_elapsed),
        close_elapsed_ms: ms(close_elapsed),
        sst_files,
        vlog_files,
    };
    emit_prepare(args.common.output, &record)
}

fn run_measure_open_close(args: MeasureOpenCloseArgs) -> Result<()> {
    if args.repetitions == 0 {
        bail!("--repetitions must be greater than 0");
    }
    let cfg = RunConfig::from_common(&args.common)?;
    let options = cfg.build_options();
    let (sst_files, vlog_files) = count_dataset_files(&cfg.path)?;

    let mut records = Vec::new();
    match args.mode {
        OpenMode::Sync => records.push(measure_mode(
            "sync",
            args.repetitions,
            &cfg,
            &options,
            false,
        )?),
        OpenMode::Async => records.push(measure_mode(
            "async",
            args.repetitions,
            &cfg,
            &options,
            true,
        )?),
        OpenMode::Both => {
            // Coin-flip the order so results aren't systematically biased
            // toward whichever path runs first on a warm page cache.
            let sync_first = unix_epoch_ms().is_multiple_of(2);
            let (first, second) = if sync_first {
                ("sync", "async")
            } else {
                ("async", "sync")
            };
            records.push(measure_mode(
                first,
                args.repetitions,
                &cfg,
                &options,
                !sync_first,
            )?);
            records.push(measure_mode(
                second,
                args.repetitions,
                &cfg,
                &options,
                sync_first,
            )?);
        }
    }

    for record in &mut records {
        record.sst_files = sst_files;
        record.vlog_files = vlog_files;
    }
    emit_measure(args.common.output, &records)
}

fn run_one_cycle(
    cfg: &RunConfig,
    options: &LsmStorageOptions,
    asynchronous: bool,
) -> Result<(f64, f64)> {
    if asynchronous {
        let open_start = Instant::now();
        let engine = block_on(KvEngine::open_async(&cfg.path, options.clone()))?;
        let open_ms = ms(open_start.elapsed());
        let close_start = Instant::now();
        block_on(engine.close_async())?;
        let close_ms = ms(close_start.elapsed());
        Ok((open_ms, close_ms))
    } else {
        let open_start = Instant::now();
        let engine = KvEngine::open(&cfg.path, options.clone())?;
        let open_ms = ms(open_start.elapsed());
        let close_start = Instant::now();
        engine.close()?;
        let close_ms = ms(close_start.elapsed());
        Ok((open_ms, close_ms))
    }
}

fn measure_mode(
    mode_name: &'static str,
    repetitions: usize,
    cfg: &RunConfig,
    options: &LsmStorageOptions,
    asynchronous: bool,
) -> Result<MeasureRecord> {
    // Warmup round: prime the page cache and filesystem so that recorded
    // samples reflect a steady state rather than cold-start variance from
    // the very first access. The warmup sample is discarded.
    run_one_cycle(cfg, options, asynchronous)?;

    let mut open_samples = Vec::with_capacity(repetitions);
    let mut close_samples = Vec::with_capacity(repetitions);

    for _ in 0..repetitions {
        let (open_ms, close_ms) = run_one_cycle(cfg, options, asynchronous)?;
        open_samples.push(open_ms);
        close_samples.push(close_ms);
    }

    Ok(MeasureRecord {
        schema: JSON_SCHEMA,
        unix_epoch_ms: unix_epoch_ms(),
        engine: ENGINE_NAME,
        command: "measure_open_close",
        mode: mode_name,
        path: cfg.path.display().to_string(),
        wal: cfg.wal,
        value_separation: cfg.vlog,
        compaction: compaction_name(cfg.compaction),
        repetitions,
        open_median_ms: median_ms(&open_samples),
        close_median_ms: median_ms(&close_samples),
        open_samples_ms: open_samples,
        close_samples_ms: close_samples,
        sst_files: 0,
        vlog_files: 0,
    })
}

fn count_dataset_files(path: &Path) -> Result<(usize, usize)> {
    let mut sst = 0usize;
    let mut vlog = 0usize;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            match entry.path().extension().and_then(|ext| ext.to_str()) {
                Some("sst") => sst += 1,
                Some("vlog") => vlog += 1,
                _ => {}
            }
        }
    }
    Ok((sst, vlog))
}

fn emit_prepare(output: OutputFormat, record: &PrepareRecord) -> Result<()> {
    let summary = format!(
        "prepared dataset path={} keys={} value_size={} flush_every={} ssts={} vlogs={} prepare_ms={:.2} close_ms={:.2}",
        record.path,
        record.num_keys,
        record.value_size,
        record.flush_every,
        record.sst_files,
        record.vlog_files,
        record.prepare_elapsed_ms,
        record.close_elapsed_ms,
    );
    match output {
        OutputFormat::Text => println!("{summary}"),
        OutputFormat::Json => println!("{}", serde_json::to_string(record)?),
        OutputFormat::Both => {
            eprintln!("{summary}");
            println!("{}", serde_json::to_string(record)?);
        }
    }
    Ok(())
}

fn emit_measure(output: OutputFormat, records: &[MeasureRecord]) -> Result<()> {
    for record in records {
        let summary = format!(
            "{} path={} reps={} ssts={} vlogs={} open_median_ms={:.2} close_median_ms={:.2}",
            record.mode,
            record.path,
            record.repetitions,
            record.sst_files,
            record.vlog_files,
            record.open_median_ms,
            record.close_median_ms,
        );
        match output {
            OutputFormat::Text => println!("{summary}"),
            OutputFormat::Json => println!("{}", serde_json::to_string(record)?),
            OutputFormat::Both => {
                eprintln!("{summary}");
                println!("{}", serde_json::to_string(record)?);
            }
        }
    }
    Ok(())
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn median_ms(samples: &[f64]) -> f64 {
    let mut values = samples.to_vec();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        let hi = values.len() / 2;
        (values[hi - 1] + values[hi]) / 2.0
    }
}

fn compaction_name(mode: CompactionMode) -> &'static str {
    match mode {
        CompactionMode::None => "none",
        CompactionMode::Simple => "simple",
        CompactionMode::Leveled => "leveled",
        CompactionMode::Tiered => "tiered",
    }
}

fn unix_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before unix epoch")
        .as_millis() as u64
}
