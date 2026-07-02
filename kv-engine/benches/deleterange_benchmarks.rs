//! DeleteRange performance benchmarks (RFC 010 §12.5).
//!
//! Measures read-path overhead from range tombstones at various scales
//! and data locations. Acceptance gate: ≤10% p95 regression for point-get,
//! ≤15% p95 for scan/prefix-scan at 100 non-covering tombstones.

use std::{hint::black_box, ops::Bound};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kv_engine::{
    FutureResultExt,
    compact::{CompactionOptions, LeveledCompactionOptions},
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, PrefixBloomOptions},
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_options() -> LsmStorageOptions {
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
        value_separation: None,
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1792,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
    }
}

fn make_options_wal() -> LsmStorageOptions {
    LsmStorageOptions {
        enable_wal: true,
        ..make_options()
    }
}

fn flush_all(lsm: &KvEngine) {
    for _ in 0..5 {
        lsm.force_flush().unwrap();
    }
}

/// Load `n` entries with key format `keyNNNNNN` and a 100-byte value.
fn load_entries(lsm: &KvEngine, n: usize) {
    let value = vec![0xABu8; 100];
    for i in 0..n {
        let key = format!("key{:06}", i);
        lsm.put(key.as_bytes(), &value).unwrap();
    }
}

/// Insert `n` non-covering range tombstones in disjoint small ranges.
/// Each tombstone covers `delNNNN-delNNN1` (4-byte range), placed
/// between data keys so they don't cover any `key*` entries.
fn insert_noncovering_tombstones(lsm: &KvEngine, n: usize) {
    for i in 0..n {
        let start = format!("del{:06}", i);
        let end = format!("del{:06}", i + 1);
        lsm.delete_range(start.as_bytes(), end.as_bytes()).unwrap();
    }
}

/// Count entries from a scan iterator.
fn count_scan(iter: impl StorageIterator) -> usize {
    let mut iter = iter;
    let mut count = 0;
    while iter.is_valid() {
        count += 1;
        iter.next().unwrap();
    }
    count
}

// ---------------------------------------------------------------------------
// Group 1: get baseline with non-covering tombstones
// ---------------------------------------------------------------------------

fn bench_get_noncovering(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_noncovering_tombstones");
    group.sample_size(100);

    for n_tombstones in [0, 1, 100, 10_000] {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();

        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();

        // Add tombstones after compaction so they live in memtable/imm
        insert_noncovering_tombstones(&lsm, n_tombstones);

        let label = format!("tombstones={}", n_tombstones);
        group.bench_function(BenchmarkId::new("get", &label), |b| {
            b.iter(|| {
                let result = lsm.get(b"key002500").unwrap();
                black_box(result);
            })
        });

        lsm.close().unwrap();
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2: get with covering tombstones at different levels
// ---------------------------------------------------------------------------

fn bench_get_covering(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_covering_tombstones");
    group.sample_size(100);

    // Active memtable: tombstone in current memtable, no flush
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        // Cover key002500 in active memtable
        lsm.delete_range(b"key002500", b"key002501").unwrap();

        group.bench_function("active_memtable", |b| {
            b.iter(|| {
                let result = lsm.get(b"key002500").unwrap();
                black_box(result);
            })
        });
        lsm.close().unwrap();
    }

    // Immutable memtable: tombstone frozen to immutable but not flushed to SST.
    // Fill active memtable with data to trigger a freeze (num_memtable_limit=2).
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        // Write tombstone into active memtable
        lsm.delete_range(b"key002500", b"key002501").unwrap();
        // Write enough data to fill the memtable and trigger freeze.
        // target_sst_size is 2MB, values are 100 bytes -> ~20k entries.
        // Write in the filler range so they don't overlap with our target key.
        let filler = vec![0xCDu8; 100];
        for i in 0..25_000 {
            let key = format!("fill{:06}", i);
            lsm.put(key.as_bytes(), &filler).unwrap();
        }
        // Tombstone is now in an immutable memtable (frozen when filler
        // exceeded the memtable capacity). Do NOT flush — keep it in memory.

        group.bench_function("immutable_memtable", |b| {
            b.iter(|| {
                let result = lsm.get(b"key002500").unwrap();
                black_box(result);
            })
        });
        lsm.close().unwrap();
    }

    // L0: tombstone flushed to L0 SST
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        lsm.delete_range(b"key002500", b"key002501").unwrap();
        flush_all(&lsm);

        group.bench_function("l0", |b| {
            b.iter(|| {
                let result = lsm.get(b"key002500").unwrap();
                black_box(result);
            })
        });
        lsm.close().unwrap();
    }

    // Lower level: tombstone compacted to L1+.
    // Pin the MVCC watermark with a transaction to prevent GC of the
    // tombstone and covered entries during compaction.
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        lsm.delete_range(b"key002500", b"key002501").unwrap();
        flush_all(&lsm);
        // Pin watermark before compaction to prevent GC
        let _txn = lsm.new_txn().unwrap();
        lsm.force_full_compaction().unwrap();

        group.bench_function("lower_level", |b| {
            b.iter(|| {
                let result = lsm.get(b"key002500").unwrap();
                black_box(result);
            })
        });
        lsm.close().unwrap();
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: scan over fully deleted dense range
// ---------------------------------------------------------------------------

fn bench_scan_dense_deleted(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_dense_deleted_range");
    group.sample_size(50);

    // Deleted range: tombstone in memtable, point entries in SSTs.
    // Do NOT compact after adding the tombstone — compaction would GC
    // both the tombstone and the covered entries, making the scan trivial.
    let dir = tempfile::tempdir().unwrap();
    let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
    load_entries(&lsm, 5000);
    flush_all(&lsm);
    lsm.force_full_compaction().unwrap();
    lsm.delete_range(b"key001000", b"key002000").unwrap();

    group.bench_function("scan_deleted_1000", |b| {
        b.iter(|| {
            let iter = lsm
                .scan(Bound::Included(b"key001000"), Bound::Excluded(b"key002000"))
                .unwrap();
            let count = count_scan(iter);
            black_box(count);
        })
    });

    // Baseline: scan same range without tombstone
    let dir2 = tempfile::tempdir().unwrap();
    let lsm2 = KvEngine::open(dir2.path(), make_options()).unwrap();
    load_entries(&lsm2, 5000);
    flush_all(&lsm2);
    lsm2.force_full_compaction().unwrap();

    group.bench_function("scan_baseline_1000", |b| {
        b.iter(|| {
            let iter = lsm2
                .scan(Bound::Included(b"key001000"), Bound::Excluded(b"key002000"))
                .unwrap();
            let count = count_scan(iter);
            black_box(count);
        })
    });

    lsm.close().unwrap();
    lsm2.close().unwrap();
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3b: scan with non-covering tombstones (§12.5 acceptance gate)
// ---------------------------------------------------------------------------

fn bench_scan_noncovering(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_noncovering_tombstones");
    group.sample_size(50);

    // 100 non-covering tombstones in active memtable, scan surviving keys
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        insert_noncovering_tombstones(&lsm, 100);

        group.bench_function("noncovering_100", |b| {
            b.iter(|| {
                let iter = lsm
                    .scan(Bound::Included(b"key000000"), Bound::Excluded(b"key004000"))
                    .unwrap();
                let count = count_scan(iter);
                black_box(count);
            })
        });
        lsm.close().unwrap();
    }

    // Baseline: same scan range without tombstones
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        load_entries(&lsm, 5000);
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();

        group.bench_function("baseline", |b| {
            b.iter(|| {
                let iter = lsm
                    .scan(Bound::Included(b"key000000"), Bound::Excluded(b"key004000"))
                    .unwrap();
                let count = count_scan(iter);
                black_box(count);
            })
        });
        lsm.close().unwrap();
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 4: prefix_scan with tombstones
// ---------------------------------------------------------------------------

fn bench_prefix_scan_tombstones(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix_scan_tombstones");
    group.sample_size(50);

    // Non-overlapping: tombstone in active memtable, data in SSTs
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        let value = vec![0xABu8; 100];
        for i in 0..5000 {
            let key = format!("pre{:04}item{:04}", i / 50, i % 50);
            lsm.put(key.as_bytes(), &value).unwrap();
        }
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        // Tombstone in active memtable (not flushed to SST)
        lsm.delete_range(b"pre9999", b"pre999a").unwrap();

        group.bench_function("non_overlapping", |b| {
            b.iter(|| {
                let iter = lsm.prefix_scan(b"pre0025").unwrap();
                let count = count_scan(iter);
                black_box(count);
            })
        });
        lsm.close().unwrap();
    }

    // Overlapping: tombstone in active memtable, data in SSTs
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        let value = vec![0xABu8; 100];
        for i in 0..5000 {
            let key = format!("pre{:04}item{:04}", i / 50, i % 50);
            lsm.put(key.as_bytes(), &value).unwrap();
        }
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        // Tombstone in active memtable (not flushed to SST)
        lsm.delete_range(b"pre0025", b"pre0026").unwrap();

        group.bench_function("overlapping", |b| {
            b.iter(|| {
                let iter = lsm.prefix_scan(b"pre0025").unwrap();
                let count = count_scan(iter);
                black_box(count);
            })
        });
        lsm.close().unwrap();
    }

    // 100 non-covering tombstones (§12.5 gate workload)
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        let value = vec![0xABu8; 100];
        for i in 0..5000 {
            let key = format!("pre{:04}item{:04}", i / 50, i % 50);
            lsm.put(key.as_bytes(), &value).unwrap();
        }
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();
        insert_noncovering_tombstones(&lsm, 100);

        group.bench_function("non_overlapping_100", |b| {
            b.iter(|| {
                let iter = lsm.prefix_scan(b"pre0025").unwrap();
                let count = count_scan(iter);
                black_box(count);
            })
        });
        lsm.close().unwrap();
    }

    // Baseline: same prefix scan without tombstones
    {
        let dir = tempfile::tempdir().unwrap();
        let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
        let value = vec![0xABu8; 100];
        for i in 0..5000 {
            let key = format!("pre{:04}item{:04}", i / 50, i % 50);
            lsm.put(key.as_bytes(), &value).unwrap();
        }
        flush_all(&lsm);
        lsm.force_full_compaction().unwrap();

        group.bench_function("baseline", |b| {
            b.iter(|| {
                let iter = lsm.prefix_scan(b"pre0025").unwrap();
                let count = count_scan(iter);
                black_box(count);
            })
        });
        lsm.close().unwrap();
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 5: flush and compaction with tombstones
// ---------------------------------------------------------------------------

fn bench_flush_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_compaction_tombstones");
    group.sample_size(20);

    // Mixed flush: point entries + range tombstone
    group.bench_function("flush_mixed", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
                let value = vec![0xABu8; 100];
                for i in 0..1000 {
                    let key = format!("key{:06}", i);
                    lsm.put(key.as_bytes(), &value).unwrap();
                }
                (dir, lsm)
            },
            |(dir, lsm)| {
                lsm.delete_range(b"key000500", b"key000600").unwrap();
                flush_all(&lsm);
                lsm.close().unwrap();
                drop(dir);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Range-only flush: just a tombstone, no point entries
    group.bench_function("flush_range_only", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
                (dir, lsm)
            },
            |(dir, lsm)| {
                lsm.delete_range(b"key000000", b"key001000").unwrap();
                flush_all(&lsm);
                lsm.close().unwrap();
                drop(dir);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Compaction with mixed data
    group.bench_function("compaction_mixed", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
                let value = vec![0xABu8; 100];
                for i in 0..5000 {
                    let key = format!("key{:06}", i);
                    lsm.put(key.as_bytes(), &value).unwrap();
                }
                lsm.delete_range(b"key001000", b"key002000").unwrap();
                flush_all(&lsm);
                (dir, lsm)
            },
            |(dir, lsm)| {
                lsm.force_full_compaction().unwrap();
                lsm.close().unwrap();
                drop(dir);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Compaction with range-only data (§12.5 range-only compaction path)
    group.bench_function("compaction_range_only", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
                // Write some entries first so there's a lower level to compact into
                let value = vec![0xABu8; 100];
                for i in 0..1000 {
                    let key = format!("key{:06}", i);
                    lsm.put(key.as_bytes(), &value).unwrap();
                }
                flush_all(&lsm);
                lsm.force_full_compaction().unwrap();
                // Now add a range-only tombstone and flush
                lsm.delete_range(b"key000500", b"key000800").unwrap();
                flush_all(&lsm);
                (dir, lsm)
            },
            |(dir, lsm)| {
                lsm.force_full_compaction().unwrap();
                lsm.close().unwrap();
                drop(dir);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 6: recovery/open time with tombstones
// ---------------------------------------------------------------------------

fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery_tombstones");
    group.sample_size(20);

    for n_tombstones in [0, 1, 100, 10_000] {
        // Prepare data directory with tombstones (WAL disabled, flushed)
        let dir = tempfile::tempdir().unwrap();
        {
            let lsm = KvEngine::open(dir.path(), make_options()).unwrap();
            load_entries(&lsm, 5000);
            insert_noncovering_tombstones(&lsm, n_tombstones);
            flush_all(&lsm);
            lsm.close().unwrap();
        }

        let path = dir.path().to_path_buf();
        let label = format!("tombstones={}", n_tombstones);
        group.bench_function(BenchmarkId::new("open_sst", &label), |b| {
            b.iter_batched(
                || {
                    let temp_dir = tempfile::tempdir().unwrap();
                    for entry in std::fs::read_dir(&path).unwrap() {
                        let entry = entry.unwrap();
                        std::fs::copy(entry.path(), temp_dir.path().join(entry.file_name()))
                            .unwrap();
                    }
                    temp_dir
                },
                |temp_dir| {
                    let lsm = KvEngine::open(temp_dir.path(), make_options()).unwrap();
                    black_box(&lsm);
                    // Drop without close() to exclude shutdown work from timing
                    drop(lsm);
                    temp_dir
                },
                criterion::BatchSize::SmallInput,
            )
        });

        drop(dir);
    }

    // WAL recovery: tombstones in WAL (not flushed)
    let dir_wal = tempfile::tempdir().unwrap();
    {
        let lsm = KvEngine::open(dir_wal.path(), make_options_wal()).unwrap();
        load_entries(&lsm, 5000);
        lsm.sync().unwrap();
        insert_noncovering_tombstones(&lsm, 100);
        lsm.sync().unwrap();
        // Do NOT flush — tombstones live in WAL only
        lsm.close().unwrap();
    }

    let path_wal = dir_wal.path().to_path_buf();
    group.bench_function(BenchmarkId::new("open_wal", "tombstones=100"), |b| {
        b.iter_batched(
            || {
                let temp_dir = tempfile::tempdir().unwrap();
                for entry in std::fs::read_dir(&path_wal).unwrap() {
                    let entry = entry.unwrap();
                    std::fs::copy(entry.path(), temp_dir.path().join(entry.file_name())).unwrap();
                }
                temp_dir
            },
            |temp_dir| {
                let lsm = KvEngine::open(temp_dir.path(), make_options_wal()).unwrap();
                black_box(&lsm);
                // Drop without close() to exclude shutdown work from timing
                drop(lsm);
                temp_dir
            },
            criterion::BatchSize::SmallInput,
        )
    });

    drop(dir_wal);
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_get_noncovering,
    bench_get_covering,
    bench_scan_dense_deleted,
    bench_scan_noncovering,
    bench_prefix_scan_tombstones,
    bench_flush_compaction,
    bench_recovery,
);
criterion_main!(benches);
