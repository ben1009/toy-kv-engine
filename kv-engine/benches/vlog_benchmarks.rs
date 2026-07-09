//! Performance benchmarks for key-value separation (vLog).
//!
//! Compares inline (no vLog) vs vLog-enabled configurations across:
//! - Write throughput
//! - Compaction time
//! - Read latency (point get + scan)
//! - Write amplification ratio

use std::{hint::black_box, ops::Bound, path::Path, sync::Arc};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kv_engine::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, PrefixBloomOptions},
    vlog::ValueSeparationOptions,
};

fn make_options(vlog_enabled: bool, min_value_size: usize) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 2 << 20,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        serializable: false,
        value_separation: if vlog_enabled {
            Some(ValueSeparationOptions {
                enabled: true,
                min_value_size,
                value_cache_capacity_bytes: 0, // uncached baseline
                ..Default::default()
            })
        } else {
            None
        },
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1792,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        ttl_read_filtering: false,
    }
}

fn make_options_with_compaction(vlog_enabled: bool, min_value_size: usize) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 2 << 20,
        num_memtable_limit: 2,
        // High trigger prevents background compaction; force_full_compaction() still works.
        compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 1000,
            max_levels: 3,
            base_level_size_mb: 1,
            level_size_multiplier: 2,
        }),
        enable_wal: false,
        serializable: false,
        value_separation: if vlog_enabled {
            Some(ValueSeparationOptions {
                enabled: true,
                min_value_size,
                value_cache_capacity_bytes: 0, // uncached baseline
                ..Default::default()
            })
        } else {
            None
        },
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1792,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        ttl_read_filtering: false,
    }
}

fn flush_all(lsm: &KvEngine) {
    // Loop runs at most 5 times; force_flush returns Ok(()) on empty list.
    for _ in 0..5 {
        if lsm.force_flush().is_err() {
            break;
        }
    }
}

fn load_data(lsm: &KvEngine, n: usize, value_size: usize) {
    let value = vec![0xABu8; value_size];
    for i in 0..n {
        let key = format!("key{:08}", i);
        lsm.put(key.as_bytes(), &value).unwrap();
    }
    flush_all(lsm);
}

fn dir_size(path: &Path, extension: &str) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == extension))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn make_options_with_cache(min_value_size: usize, cache_bytes: u64) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 2 << 20,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 1000,
            max_levels: 3,
            base_level_size_mb: 1,
            level_size_multiplier: 2,
        }),
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size,
            value_cache_capacity_bytes: cache_bytes,
            ..Default::default()
        }),
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1792,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
        ttl_read_filtering: false,
    }
}

fn setup_instance(
    vlog_enabled: bool,
    min_value_size: usize,
    compaction: bool,
) -> (tempfile::TempDir, Arc<KvEngine>) {
    let dir = tempfile::tempdir().unwrap();
    let options = if compaction {
        make_options_with_compaction(vlog_enabled, min_value_size)
    } else {
        make_options(vlog_enabled, min_value_size)
    };
    let lsm = KvEngine::open(dir.path(), options).unwrap();
    (dir, lsm)
}

fn setup_instance_with_data(
    vlog_enabled: bool,
    min_value_size: usize,
    num_entries: usize,
    value_size: usize,
    compaction: bool,
) -> (tempfile::TempDir, Arc<KvEngine>) {
    let (dir, lsm) = setup_instance(vlog_enabled, min_value_size, compaction);
    load_data(&lsm, num_entries, value_size);
    (dir, lsm)
}

// ---------------------------------------------------------------------------
// Benchmark: write throughput
// ---------------------------------------------------------------------------

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    // Max inline value is u16::MAX minus 1-byte KvKind prefix = 65534.
    for value_size in [1024, 4096, 16384, 65534] {
        let label = format!("{}kb", value_size / 1024);

        group.bench_with_input(BenchmarkId::new("inline", &label), &value_size, |b, &vs| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let options = make_options(false, 1024);
                    let lsm = KvEngine::open(dir.path(), options).unwrap();
                    (dir, lsm, 0usize)
                },
                |(_dir, lsm, mut i)| {
                    let value = vec![0xABu8; vs];
                    for _ in 0..1000 {
                        let key = format!("key{:08}", i);
                        lsm.put(key.as_bytes(), &value).unwrap();
                        i += 1;
                    }
                    black_box(i);
                    lsm.close().unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("vlog", &label), &value_size, |b, &vs| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let options = make_options(true, 16);
                    let lsm = KvEngine::open(dir.path(), options).unwrap();
                    (dir, lsm, 0usize)
                },
                |(_dir, lsm, mut i)| {
                    let value = vec![0xABu8; vs];
                    for _ in 0..1000 {
                        let key = format!("key{:08}", i);
                        lsm.put(key.as_bytes(), &value).unwrap();
                        i += 1;
                    }
                    black_box(i);
                    lsm.close().unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: compaction time
// ---------------------------------------------------------------------------

fn bench_compaction(c: &mut Criterion) {
    let num_entries = 5000;
    let value_size = 16384; // 16KB

    let mut group = c.benchmark_group("compaction");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    for (label, vlog_enabled) in [("inline", false), ("vlog", true)] {
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let (dir, lsm) =
                        setup_instance_with_data(vlog_enabled, 16, num_entries, value_size, true);
                    (dir, lsm)
                },
                |(dir, lsm)| {
                    lsm.force_full_compaction().unwrap();
                    let sst_bytes = dir_size(dir.path(), "sst");
                    let vlog_bytes = lsm.vlog_stats().map(|s| s.vlog_total_bytes).unwrap_or(0);
                    eprintln!(
                        "[{label}] post-compaction SST={sst_bytes} vLog={vlog_bytes} total={}",
                        sst_bytes + vlog_bytes
                    );
                    black_box(());
                    lsm.close().unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: point-get read latency
// ---------------------------------------------------------------------------

fn bench_read_point_get(c: &mut Criterion) {
    let num_entries = 5000;
    let value_size = 16384;

    let mut group = c.benchmark_group("read_point_get");
    group.sample_size(50);

    for (label, vlog_enabled) in [("inline", false), ("vlog", true)] {
        let (dir, lsm) = setup_instance_with_data(vlog_enabled, 16, num_entries, value_size, true);
        lsm.force_full_compaction().unwrap();

        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("key{:08}", i).into_bytes())
            .collect();

        group.bench_function(label, |b| {
            let mut i = 0usize;
            b.iter(|| {
                let result = lsm.get(&keys[i % keys.len()]).unwrap();
                black_box(result);
                i += 1;
            })
        });

        lsm.close().unwrap();
        drop(dir);
    }

    // vlog with value cache (10K entries)
    {
        let dir = tempfile::tempdir().unwrap();
        let options = make_options_with_cache(16, 256 << 20); // 256MB cache
        let lsm = KvEngine::open(dir.path(), options).unwrap();
        load_data(&lsm, num_entries, value_size);
        lsm.force_full_compaction().unwrap();

        let keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("key{:08}", i).into_bytes())
            .collect();

        group.bench_function("vlog_cached", |b| {
            let mut i = 0usize;
            b.iter(|| {
                let result = lsm.get(&keys[i % keys.len()]).unwrap();
                black_box(result);
                i += 1;
            })
        });

        let stats = lsm.vlog_stats().unwrap();
        eprintln!(
            "[vlog_cached] cache_hits={} cache_misses={} hit_rate={:.1}%",
            stats.cache_hits,
            stats.cache_misses,
            if stats.cache_hits + stats.cache_misses > 0 {
                stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64 * 100.0
            } else {
                0.0
            }
        );

        lsm.close().unwrap();
        drop(dir);
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: full scan throughput
// ---------------------------------------------------------------------------

fn bench_read_scan(c: &mut Criterion) {
    let num_entries = 5000;
    let value_size = 16384;

    let mut group = c.benchmark_group("read_scan");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    for (label, vlog_enabled) in [("inline", false), ("vlog", true)] {
        let (dir, lsm) = setup_instance_with_data(vlog_enabled, 16, num_entries, value_size, true);
        lsm.force_full_compaction().unwrap();

        group.bench_function(label, |b| {
            b.iter(|| {
                let mut scan = lsm.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
                while scan.is_valid() {
                    black_box(scan.value());
                    scan.next().unwrap();
                }
            })
        });

        lsm.close().unwrap();
        drop(dir);
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Prefix scan: with and without prefix bloom filters
// ---------------------------------------------------------------------------

fn bench_prefix_scan(c: &mut Criterion) {
    // Workload: 100 prefixes × 5,000 entries each = 500k total.
    // Data is interleaved so each SST contains entries from many prefixes.
    // Query a single prefix (5,000 entries) — bloom should skip ~99% of SSTs.
    let num_entries = 500_000;
    let value_size = 1024;
    let num_prefixes = 100;
    let entries_per_prefix = num_entries / num_prefixes; // 5000

    let mut group = c.benchmark_group("prefix_scan");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    for (label, bloom_enabled) in [("no_bloom", false), ("bloom", true)] {
        let dir = tempfile::tempdir().unwrap();
        let options = LsmStorageOptions {
            ttl_read_filtering: false,
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 2,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            serializable: false,
            value_separation: None,
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity: 1792,
            enable_cache_backfill: true,
            prefix_bloom: PrefixBloomOptions {
                enabled: bloom_enabled,
                prefix_lengths: vec![8],
                false_positive_rate: 0.01,
            },
        };
        let lsm = KvEngine::open(dir.path(), options).unwrap();

        // Interleave prefixes: write round-robin so each SST has many prefixes.
        let value = vec![0xABu8; value_size];
        for i in 0..entries_per_prefix {
            for p in 0..num_prefixes {
                let key = format!("user{:06}_{:06}", p, i);
                lsm.put(key.as_bytes(), &value).unwrap();
            }
        }
        flush_all(&lsm);

        // Benchmark prefix_scan on a single prefix — drop page cache each iteration
        group.bench_function(label, |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    drop_os_page_cache(dir.path());
                    let start = std::time::Instant::now();
                    let mut scan = lsm.prefix_scan(b"user005000").unwrap();
                    while scan.is_valid() {
                        black_box(scan.value());
                        scan.next().unwrap();
                    }
                    total += start.elapsed();
                }
                total
            })
        });

        lsm.close().unwrap();
        drop(dir);
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Write amplification: multi-round compaction measurement
// ---------------------------------------------------------------------------

fn measure_write_amplification(vlog_enabled: bool, min_value_size: usize) {
    let label = if vlog_enabled { "vlog" } else { "inline" };
    let num_entries = 1000;
    let value_size = 16384;
    let key_size = 12; // "key" + 8 digits

    let (dir, lsm) =
        setup_instance_with_data(vlog_enabled, min_value_size, num_entries, value_size, true);

    // Measure SST size before compaction (data is in L0 SSTs).
    let sst_before = dir_size(dir.path(), "sst") as f64;

    lsm.force_full_compaction().unwrap();
    flush_all(&lsm);

    let sst_after = dir_size(dir.path(), "sst") as f64;
    let vlog_bytes = lsm
        .vlog_stats()
        .map(|s| s.vlog_total_bytes as f64)
        .unwrap_or(0.0);
    let live_bytes = (num_entries * (key_size + value_size)) as f64;
    let total = sst_after + vlog_bytes;

    eprintln!(
        "[{label}] {num_entries} entries @ {value_size}B  \
         sst_before={:.1}MB  sst_after={:.1}MB  vlog={:.1}MB  \
         live={:.1}MB  on_disk_ratio={:.2}x  (compaction rewrites {:.1}MB SST data)",
        sst_before / (1024.0 * 1024.0),
        sst_after / (1024.0 * 1024.0),
        vlog_bytes / (1024.0 * 1024.0),
        live_bytes / (1024.0 * 1024.0),
        total / live_bytes,
        sst_before / (1024.0 * 1024.0),
    );

    lsm.close().unwrap();
    drop(dir);
}

fn bench_write_amplification(_c: &mut Criterion) {
    eprintln!("\n=== Write Amplification ===");
    measure_write_amplification(false, 1024);
    measure_write_amplification(true, 16);
    eprintln!("==========================\n");
}

// ---------------------------------------------------------------------------
// Benchmark: cold-cache point-get (stresses bloom hash pass-through + block
// decode from_vec + block iterator binary search)
//
// Uses a tiny block cache (4 blocks) so every SST probe reads from disk.
// With 20K entries spread across multiple L0 SSTs, each `get()` that misses
// the memtable must: hash the bloom filter per SST (#1), read+decode the
// block (#3), and binary-search within it (#4).
// ---------------------------------------------------------------------------

fn bench_cold_point_get(c: &mut Criterion) {
    let num_entries = 20_000;
    let value_size = 256; // small values → many blocks per SST → more binary search

    let mut group = c.benchmark_group("cold_point_get");
    group.sample_size(50);

    // Tiny block cache: only 4 blocks fit → almost every probe is a disk read
    let make_cold_options = |vlog_enabled: bool| -> LsmStorageOptions {
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 2,
            compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 1000,
                max_levels: 3,
                base_level_size_mb: 1,
                level_size_multiplier: 2,
            }),
            enable_wal: false,
            serializable: false,
            value_separation: if vlog_enabled {
                Some(ValueSeparationOptions {
                    enabled: true,
                    min_value_size: 16,
                    value_cache_capacity_bytes: 0,
                    ..Default::default()
                })
            } else {
                None
            },
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity: 4, // tiny — forces disk reads
            enable_cache_backfill: true,
            prefix_bloom: PrefixBloomOptions::default(),
            ttl_read_filtering: false,
        }
    };

    // Keys that are NOT in the memtable (they've been flushed to SSTs).
    // We query keys from the first half of the dataset, which are guaranteed
    // to have been flushed.
    let sst_keys: Vec<Vec<u8>> = (0..5000)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect();

    for (label, vlog_enabled) in [("inline", false), ("vlog", true)] {
        let dir = tempfile::tempdir().unwrap();
        let options = make_cold_options(vlog_enabled);
        let lsm = KvEngine::open(dir.path(), options).unwrap();
        load_data(&lsm, num_entries, value_size);

        group.bench_function(label, |b| {
            let mut i = 0usize;
            b.iter(|| {
                let result = lsm.get(&sst_keys[i % sst_keys.len()]).unwrap();
                black_box(result);
                i += 1;
            })
        });

        drop(lsm);
        drop(dir);
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: flush throughput (stresses BlockBuilder pre-allocation)
//
// Uses a small target SST size (64KB) so every ~25 puts trigger a flush,
// creating many BlockBuilders. Pre-allocating data/offsets Vecs eliminates
// the doubling reallocations on every flush.
// ---------------------------------------------------------------------------

fn bench_flush_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_throughput");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    let value_size = 1024; // 1KB values → ~25 entries per 64KB SST

    for (label, vlog_enabled) in [("inline", false), ("vlog", true)] {
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let options = LsmStorageOptions {
                        block_size: 4096,
                        target_sst_size: 64 << 10, // 64KB → frequent flushes
                        num_memtable_limit: 2,
                        compaction_options: CompactionOptions::NoCompaction,
                        enable_wal: false,
                        serializable: false,
                        value_separation: if vlog_enabled {
                            Some(ValueSeparationOptions {
                                enabled: true,
                                min_value_size: 16,
                                value_cache_capacity_bytes: 0,
                                ..Default::default()
                            })
                        } else {
                            None
                        },
                        manifest_snapshot_threshold_bytes: 0,
                        block_cache_capacity: 1792,
                        enable_cache_backfill: true,
                        prefix_bloom: PrefixBloomOptions::default(),
                        ttl_read_filtering: false,
                    };
                    let lsm = KvEngine::open(dir.path(), options).unwrap();
                    (dir, lsm, 0usize)
                },
                |(_dir, lsm, mut i)| {
                    let value = vec![0xABu8; value_size];
                    // 2500 puts → ~100 flushes at 64KB target
                    for _ in 0..2500 {
                        let key = format!("key{:08}", i);
                        lsm.put(key.as_bytes(), &value).unwrap();
                        i += 1;
                    }
                    black_box(i);
                    drop(lsm);
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: cold-cache scan (stresses block decode from_vec + binary search)
//
// Full scan with a tiny block cache. Every block is read from disk and
// decoded, exercising the decode_from_vec path. The scan also triggers
// seek_to_key on every block boundary, exercising the optimized binary search.
// ---------------------------------------------------------------------------

fn bench_cold_scan(c: &mut Criterion) {
    let num_entries = 10_000;
    let value_size = 1024;

    let mut group = c.benchmark_group("cold_scan");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    let make_cold_scan_options = |vlog_enabled: bool| -> LsmStorageOptions {
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 2,
            compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 1000,
                max_levels: 3,
                base_level_size_mb: 1,
                level_size_multiplier: 2,
            }),
            enable_wal: false,
            serializable: false,
            value_separation: if vlog_enabled {
                Some(ValueSeparationOptions {
                    enabled: true,
                    min_value_size: 16,
                    value_cache_capacity_bytes: 0,
                    ..Default::default()
                })
            } else {
                None
            },
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity: 4, // tiny — every block read hits disk
            enable_cache_backfill: true,
            prefix_bloom: PrefixBloomOptions::default(),
            ttl_read_filtering: false,
        }
    };

    for (label, vlog_enabled) in [("inline", false), ("vlog", true)] {
        let dir = tempfile::tempdir().unwrap();
        let options = make_cold_scan_options(vlog_enabled);
        let lsm = KvEngine::open(dir.path(), options).unwrap();
        load_data(&lsm, num_entries, value_size);
        lsm.force_full_compaction().unwrap();

        group.bench_function(label, |b| {
            b.iter(|| {
                let mut scan = lsm.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
                while scan.is_valid() {
                    black_box(scan.value());
                    scan.next().unwrap();
                }
            })
        });

        drop(lsm);
        drop(dir);
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: cache backfill on vs off (flush cliff)
//
// Writes data, flushes to SST, then immediately reads back.
// With backfill enabled, blocks are warm in cache.
// With backfill disabled, every read is a cache miss → disk I/O.
// ---------------------------------------------------------------------------

/// Drop all `.sst` files in `dir` from the OS page cache using
/// `posix_fadvise(POSIX_FADV_DONTNEED)`. This makes subsequent reads
/// genuinely cold (disk I/O) without needing root.
#[cfg(target_os = "linux")]
fn drop_os_page_cache(dir: &Path) {
    use std::os::unix::io::AsRawFd;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sst")
            && let Ok(file) = std::fs::File::open(&path)
        {
            let rc =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            assert_eq!(rc, 0, "posix_fadvise failed for {}", path.display());
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn drop_os_page_cache(_dir: &Path) {
    // posix_fadvise is only available on Linux.
}

fn bench_backfill_comparison(c: &mut Criterion) {
    // Working set: 1000 entries × 4KB value ≈ 1000 blocks.
    // Block cache: 1024 blocks — large enough to hold the entire dataset.
    // With backfill enabled, all flushed blocks stay warm.
    // With backfill disabled, every read is a cold cache miss → disk I/O.
    let num_entries = 1000;
    let value_size = 4096;
    let block_cache_capacity = 1024;

    let make_options = |backfill: bool| -> LsmStorageOptions {
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 2,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            serializable: false,
            value_separation: None,
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity,
            enable_cache_backfill: backfill,
            prefix_bloom: PrefixBloomOptions::default(),
            ttl_read_filtering: false,
        }
    };

    let keys: Vec<Vec<u8>> = (0..num_entries)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect();

    let mut group = c.benchmark_group("backfill_flush_cliff");
    group.sample_size(50);

    for (label, backfill) in [("enabled", true), ("disabled", false)] {
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let options = make_options(backfill);
                    let lsm = KvEngine::open(dir.path(), options).unwrap();
                    let value = vec![0xABu8; value_size];
                    for key in &keys {
                        lsm.put(key, &value).unwrap();
                    }
                    // Flush all immutable memtables
                    for _ in 0..5 {
                        if lsm.force_flush().is_err() {
                            break;
                        }
                    }
                    // Drop OS page cache so reads are genuinely cold
                    drop_os_page_cache(dir.path());
                    (dir, lsm)
                },
                |pair| {
                    let lsm = &pair.1;
                    for key in &keys {
                        let result = lsm.get(key).unwrap();
                        black_box(result);
                    }
                    pair
                },
                criterion::BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: compaction backfill on vs off
//
// Writes data, flushes to create L0 SSTs, waits for background L0→L1
// compaction, drops OS page cache, then reads back.
// L0→L1 is an upper-level compaction (not bottom level), so backfill
// is active when enabled.
// ---------------------------------------------------------------------------

fn bench_compaction_backfill(c: &mut Criterion) {
    let num_entries = 1000;
    let value_size = 4096;
    let block_cache_capacity = 1024;

    let make_options = |backfill: bool| -> LsmStorageOptions {
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 2,
            compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2, // trigger after 2 L0 files
                max_levels: 3,
                base_level_size_mb: 1,
                level_size_multiplier: 2,
            }),
            enable_wal: false,
            serializable: false,
            value_separation: None,
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity,
            enable_cache_backfill: backfill,
            prefix_bloom: PrefixBloomOptions::default(),
            ttl_read_filtering: false,
        }
    };

    let keys: Vec<Vec<u8>> = (0..num_entries)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect();

    let mut group = c.benchmark_group("backfill_compaction");
    group.sample_size(30);
    group.measurement_time(std::time::Duration::from_secs(10));

    for (label, backfill) in [("enabled", true), ("disabled", false)] {
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let options = make_options(backfill);
                    let lsm = KvEngine::open(dir.path(), options).unwrap();
                    let value = vec![0xABu8; value_size];
                    for key in &keys {
                        lsm.put(key, &value).unwrap();
                    }
                    // Flush all immutable memtables to create L0 SSTs
                    for _ in 0..5 {
                        if lsm.force_flush().is_err() {
                            break;
                        }
                    }
                    // Wait for background L0→L1 compaction to complete.
                    // force_full_compaction() cannot be used here because it
                    // sets compact_to_bottom_level=true, disabling cache backfill.
                    // The compaction thread ticks every 50ms; 3s is generous.
                    std::thread::sleep(std::time::Duration::from_secs(3));
                    drop_os_page_cache(dir.path());
                    (dir, lsm)
                },
                |pair| {
                    let lsm = &pair.1;
                    for key in &keys {
                        let result = lsm.get(key).unwrap();
                        black_box(result);
                    }
                    pair
                },
                criterion::BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_compaction,
    bench_read_point_get,
    bench_read_scan,
    bench_prefix_scan,
    bench_write_amplification,
    bench_cold_point_get,
    bench_flush_throughput,
    bench_cold_scan,
    bench_backfill_comparison,
    bench_compaction_backfill
);
criterion_main!(benches);
