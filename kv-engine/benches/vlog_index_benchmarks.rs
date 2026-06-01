//! Benchmarks for VlogIndex operations.
//!
//! Measures:
//! - save: persist index to .vidx file
//! - load: read and validate .vidx file (CRC + parse)
//! - rebuild_from_reader: scan vLog headers to build index
//! - load_or_rebuild: combined path (load hit vs rebuild miss)
//! - lookup: linear scan key lookup

use std::hint::black_box;
use std::path::Path;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kv_engine::vlog::VlogIndex;
use kv_engine::vlog::builder::ValueLogWriter;
use kv_engine::vlog::reader::ValueLogReader;

/// Construct the benchmark key for entry `i` at the given key size.
fn make_key(i: usize, key_size: usize) -> Vec<u8> {
    let key = format!("key_{:0width$}", i, width = key_size.saturating_sub(4));
    key.into_bytes()[..key_size].to_vec()
}

/// Build a VlogIndex with `n` entries of the given key/value size.
fn make_index(n: usize, key_size: usize, value_size: usize) -> VlogIndex {
    let mut idx = VlogIndex::new(1);
    for i in 0..n {
        idx.add_entry((i as u64) * 100, make_key(i, key_size), value_size as u32);
    }
    idx
}

/// Write a vLog file with `n` entries.
fn write_vlog(path: &Path, file_id: u32, n: usize, value_size: usize) {
    let mut writer = ValueLogWriter::create(path.to_path_buf(), file_id).unwrap();
    let value = vec![0xABu8; value_size];
    for i in 0..n {
        let key = format!("key_{:08}", i);
        writer.append(key.as_bytes(), &value).unwrap();
    }
    writer.close().unwrap();
}

// ---------------------------------------------------------------------------
// Benchmark: save
// ---------------------------------------------------------------------------

fn bench_save(c: &mut Criterion) {
    let mut group = c.benchmark_group("vlog_index_save");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for n in [100, 1_000, 10_000, 100_000] {
        let idx = make_index(n, 16, 128);
        group.bench_with_input(BenchmarkId::from_parameter(n), &idx, |b, idx| {
            b.iter_batched(
                || tempfile::tempdir().unwrap(),
                |dir| {
                    let path = dir.path().join("bench.vidx");
                    idx.save(&path).unwrap();
                    dir // keep TempDir alive to avoid measuring cleanup
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: load
// ---------------------------------------------------------------------------

fn bench_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("vlog_index_load");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for n in [100, 1_000, 10_000, 100_000] {
        let dir = tempfile::tempdir().unwrap();
        let idx = make_index(n, 16, 128);
        let path = dir.path().join("bench.vidx");
        idx.save(&path).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &path, |b, path| {
            b.iter(|| {
                let loaded = VlogIndex::load_from_file(path, 1).unwrap();
                black_box(loaded.len());
            })
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: rebuild_from_reader
// ---------------------------------------------------------------------------

fn bench_rebuild(c: &mut Criterion) {
    let mut group = c.benchmark_group("vlog_index_rebuild");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    for n in [100, 1_000, 10_000] {
        let dir = tempfile::tempdir().unwrap();
        let vlog_path = dir.path().join("1.vlog");
        write_vlog(&vlog_path, 1, n, 128);
        let reader = ValueLogReader::open(vlog_path.clone())
            .unwrap()
            .with_file_id(1);

        group.bench_with_input(BenchmarkId::from_parameter(n), &reader, |b, reader| {
            b.iter(|| {
                let idx = VlogIndex::rebuild_from_reader(reader, 1).unwrap();
                black_box(idx.len());
            })
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: load_or_rebuild (cache hit vs miss)
// ---------------------------------------------------------------------------

fn bench_load_or_rebuild(c: &mut Criterion) {
    let mut group = c.benchmark_group("vlog_index_load_or_rebuild");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    let n = 10_000;
    let dir = tempfile::tempdir().unwrap();
    let vlog_path = dir.path().join("1.vlog");
    write_vlog(&vlog_path, 1, n, 128);
    let reader = ValueLogReader::open(vlog_path.clone())
        .unwrap()
        .with_file_id(1);

    // Case: index file exists (load hit)
    {
        let idx = VlogIndex::rebuild_from_reader(&reader, 1).unwrap();
        let idx_path = dir.path().join("1.vidx");
        idx.save(&idx_path).unwrap();

        group.bench_function("load_hit", |b| {
            b.iter(|| {
                let loaded = VlogIndex::load_or_rebuild(&idx_path, &reader, 1).unwrap();
                black_box(loaded.len());
            })
        });
    }

    // Case: index file missing (rebuild)
    {
        let idx_path = dir.path().join("missing.vidx");

        group.bench_function("rebuild_miss", |b| {
            b.iter(|| {
                let loaded = VlogIndex::load_or_rebuild(&idx_path, &reader, 1).unwrap();
                black_box(loaded.len());
            })
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: lookup (linear scan)
// ---------------------------------------------------------------------------

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("vlog_index_lookup");
    group.sample_size(50);

    for n in [100, 1_000, 10_000, 100_000] {
        let idx = make_index(n, 16, 128);

        // Hit: look up last entry (worst case for linear scan)
        let last_key = make_key(n - 1, 16);
        group.bench_with_input(
            BenchmarkId::new("hit_last", n),
            &(&idx, &last_key),
            |b, (idx, key)| {
                b.iter(|| {
                    black_box(idx.lookup(key));
                })
            },
        );

        // Miss: look up non-existent key
        let miss_key = b"nonexistent_key_xxxx".to_vec();
        group.bench_with_input(
            BenchmarkId::new("miss", n),
            &(&idx, &miss_key),
            |b, (idx, key)| {
                b.iter(|| {
                    black_box(idx.lookup(key));
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_save,
    bench_load,
    bench_rebuild,
    bench_load_or_rebuild,
    bench_lookup
);
criterion_main!(benches);
