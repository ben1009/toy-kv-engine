//! WAL throughput benchmarks.
//!
//! All benchmarks use MVCC WALs (io_uring + O_DIRECT). Since `sync()` delegates
//! to `submit_and_commit()` for MVCC WALs, the "sync" and "submit_and_commit"
//! groups measure the same code path. They are kept as separate groups so that
//! criterion can report per-label statistics; the numbers should be identical.
//!
//! To benchmark against a legacy (non-io_uring) WAL, construct one with
//! `Wal::create_legacy()` instead of `Wal::create()`.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use kv_engine::wal::Wal;
use std::sync::Arc;
use tempfile::tempdir;

fn make_entries(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![0u8; 100]; // 100-byte values
            (key, value)
        })
        .collect()
}

/// Single-threaded WAL throughput (both labels hit the same io_uring path).
fn bench_wal_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_single_thread");

    for batch_size in [1, 10, 100] {
        let label = format!("batch_{}", batch_size);
        let entries = make_entries(batch_size);
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        // Label A: sync() — for MVCC WALs this delegates to submit_and_commit().
        group.bench_with_input(BenchmarkId::new("sync", &label), &refs, |b, refs| {
            let dir = tempdir().unwrap();
            let path = dir.path().join("bench.wal");
            let wal = Wal::create(&path).unwrap();
            let mut ts = 1u64;
            b.iter(|| {
                wal.put_batch(black_box(refs), ts).unwrap();
                wal.sync().unwrap();
                ts += 1;
            });
            wal.close().unwrap();
        });

        // Label B: submit_and_commit() — same path as sync() for MVCC WALs.
        group.bench_with_input(
            BenchmarkId::new("submit_and_commit", &label),
            &refs,
            |b, refs| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.wal");
                let wal = Wal::create(&path).unwrap();
                let mut ts = 1u64;
                b.iter(|| {
                    wal.put_batch(black_box(refs), ts).unwrap();
                    wal.submit_and_commit().unwrap();
                    ts += 1;
                });
                wal.close().unwrap();
            },
        );
    }

    group.finish();
}

/// Multi-threaded WAL throughput (both labels hit the same io_uring path).
fn bench_wal_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_multi_thread");
    group.sample_size(30);

    for num_threads in [2, 4, 8] {
        let label = format!("{}_threads", num_threads);

        // Label A: each thread calls sync() independently.
        group.bench_with_input(
            BenchmarkId::new("sync", &label),
            &num_threads,
            |b, &num_threads| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.wal");
                let wal = Arc::new(Wal::create(&path).unwrap());

                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|i| {
                            let wal = Arc::clone(&wal);
                            std::thread::spawn(move || {
                                let entries = make_entries(10);
                                let refs: Vec<(&[u8], &[u8])> = entries
                                    .iter()
                                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                                    .collect();
                                let ts = (i + 1) as u64;
                                wal.put_batch(&refs, ts).unwrap();
                                wal.sync().unwrap();
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });

                wal.close().unwrap();
            },
        );

        // Label B: each thread calls submit_and_commit() independently.
        group.bench_with_input(
            BenchmarkId::new("group_commit", &label),
            &num_threads,
            |b, &num_threads| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.wal");
                let wal = Arc::new(Wal::create(&path).unwrap());

                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|i| {
                            let wal = Arc::clone(&wal);
                            std::thread::spawn(move || {
                                let entries = make_entries(10);
                                let refs: Vec<(&[u8], &[u8])> = entries
                                    .iter()
                                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                                    .collect();
                                let ts = (i + 1) as u64;
                                wal.put_batch(&refs, ts).unwrap();
                                wal.submit_and_commit().unwrap();
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });

                wal.close().unwrap();
            },
        );
    }

    group.finish();
}

/// Sustained throughput (both labels hit the same io_uring path).
fn bench_wal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_throughput");
    group.sample_size(10);

    for num_batches in [100, 500] {
        let label = format!("{}_batches", num_batches);

        // Label A: sync().
        group.bench_with_input(
            BenchmarkId::new("sync", &label),
            &num_batches,
            |b, &num_batches| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.wal");
                let wal = Wal::create(&path).unwrap();
                let entries = make_entries(100);
                let refs: Vec<(&[u8], &[u8])> = entries
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect();

                b.iter(|| {
                    for i in 0..num_batches {
                        wal.put_batch(&refs, (i + 1) as u64).unwrap();
                        wal.sync().unwrap();
                    }
                });

                wal.close().unwrap();
            },
        );

        // Label B: submit_and_commit().
        group.bench_with_input(
            BenchmarkId::new("submit_and_commit", &label),
            &num_batches,
            |b, &num_batches| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.wal");
                let wal = Wal::create(&path).unwrap();
                let entries = make_entries(100);
                let refs: Vec<(&[u8], &[u8])> = entries
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect();

                b.iter(|| {
                    for i in 0..num_batches {
                        wal.put_batch(&refs, (i + 1) as u64).unwrap();
                        wal.submit_and_commit().unwrap();
                    }
                });

                wal.close().unwrap();
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_single_thread,
    bench_wal_multi_thread,
    bench_wal_throughput
);
criterion_main!(benches);
