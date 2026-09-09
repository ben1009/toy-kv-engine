//! RFC 022 operational benchmarks.
//!
//! Each scenario measures the backup call itself after setup has prepared the
//! source engine and repository:
//! - `full_first`: first backup into a new repository;
//! - `incremental_unchanged`: backup after an identical generation;
//! - `incremental_changed`: backup after updating a subset of keys.
//!
//! The routine also black-boxes `(logical_bytes, new_object_bytes)` from the
//! committed `BackupInfo`, so benchmark runs retain the byte-accounting signal
//! alongside latency.

use std::{
    collections::HashSet,
    env, fs,
    hint::black_box,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kv_engine::{
    BackupInfo, BackupOptions, CreateBackupOutcome,
    lsm_storage::{KvEngine, LsmStorageOptions},
    vlog::ValueSeparationOptions,
};
use serde::Serialize;

const ENTRY_COUNT: usize = 500;
const INLINE_VALUE_SIZE: usize = 4 * 1024;
const VLOG_VALUE_SIZE: usize = 16 * 1024;
const VLOG_THRESHOLD: usize = 1024;
const CHANGED_KEYS: usize = 50;

static ACCOUNTING: OnceLock<Mutex<Vec<Accounting>>> = OnceLock::new();
static ACCOUNTING_KEYS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

#[derive(Serialize)]
struct Accounting {
    scenario: String,
    value_separation: bool,
    entry_count: usize,
    logical_bytes: u64,
    new_object_bytes: u64,
}

struct Scenario {
    _dir: tempfile::TempDir,
    engine: Arc<KvEngine>,
    repository: std::path::PathBuf,
}

fn options(value_separation: bool) -> LsmStorageOptions {
    let mut options = LsmStorageOptions::default_for_test();
    options.value_separation = value_separation.then(|| ValueSeparationOptions {
        enabled: true,
        min_value_size: VLOG_THRESHOLD,
        ..Default::default()
    });
    options
}

fn seed(value_separation: bool, value_size: usize) -> Scenario {
    let dir = tempfile::tempdir().unwrap();
    let engine = KvEngine::open(dir.path().join("db"), options(value_separation)).unwrap();
    let value = vec![0xAB; value_size];
    for index in 0..ENTRY_COUNT {
        let key = format!("key-{index:06}");
        engine.put(key.as_bytes(), &value).unwrap();
    }
    Scenario {
        repository: dir.path().join("repository"),
        _dir: dir,
        engine,
    }
}

fn backup_options(repository: &std::path::Path) -> BackupOptions {
    BackupOptions {
        repository: repository.to_path_buf(),
        use_hard_links: false,
    }
}

fn committed_info(outcome: CreateBackupOutcome) -> BackupInfo {
    match outcome {
        CreateBackupOutcome::Committed(info) => info,
        other => panic!("benchmark backup did not commit: {other:?}"),
    }
}

fn backup_once(engine: &Arc<KvEngine>, repository: &std::path::Path) -> BackupInfo {
    committed_info(engine.create_backup(backup_options(repository)).unwrap())
}

fn run_backup(
    scenario: Scenario,
    scenario_name: &str,
    value_separation: bool,
    prepare: impl FnOnce(&Arc<KvEngine>),
) {
    prepare(&scenario.engine);
    let info = backup_once(&scenario.engine, &scenario.repository);
    let keys = ACCOUNTING_KEYS.get_or_init(|| Mutex::new(HashSet::new()));
    if keys.lock().unwrap().insert(scenario_name.to_owned()) {
        ACCOUNTING
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push(Accounting {
                scenario: scenario_name.to_owned(),
                value_separation,
                entry_count: ENTRY_COUNT,
                logical_bytes: info.logical_bytes,
                new_object_bytes: info.new_object_bytes,
            });
    }
    black_box((info.logical_bytes, info.new_object_bytes));
    scenario.engine.close().unwrap();
}

fn write_accounting_report() {
    let Some(path) = env::var_os("TOYKV_BACKUP_BENCH_REPORT") else {
        return;
    };
    let report = ACCOUNTING
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    fs::write(path, serde_json::to_vec_pretty(&*report).unwrap()).unwrap();
}

fn bench_backup(c: &mut Criterion) {
    let mut group = c.benchmark_group("rfc022_backup_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(ENTRY_COUNT as u64));

    for (kind, value_separation, value_size) in [
        ("inline", false, INLINE_VALUE_SIZE),
        ("vlog", true, VLOG_VALUE_SIZE),
    ] {
        for (phase, prepare) in [
            ("full_first", 0_u8),
            ("incremental_unchanged", 1_u8),
            ("incremental_changed", 2_u8),
        ] {
            group.bench_with_input(
                BenchmarkId::new(format!("{kind}/{phase}"), ENTRY_COUNT),
                &prepare,
                |benchmark, prepare| {
                    let scenario_name = format!("{kind}/{phase}");
                    benchmark.iter_batched(
                        || {
                            let scenario = seed(value_separation, value_size);
                            if *prepare != 0 {
                                let _ = backup_once(&scenario.engine, &scenario.repository);
                            }
                            scenario
                        },
                        |scenario| {
                            run_backup(scenario, &scenario_name, value_separation, |engine| {
                                if *prepare == 2 {
                                    let value = vec![0xCD; value_size];
                                    for index in 0..CHANGED_KEYS {
                                        let key = format!("key-{index:06}");
                                        engine.put(key.as_bytes(), &value).unwrap();
                                    }
                                }
                            });
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
    write_accounting_report();
}

criterion_group!(benches, bench_backup);
criterion_main!(benches);
