//! WAL throughput benchmarks.
//!
//! Each test has a control group:
//! - sync: traditional fsync (serial, one fsync per batch)
//! - submit_and_commit: io_uring group commit (amortizes fsync across threads)

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

/// Single-threaded: sync (control) vs submit_and_commit (experiment).
fn bench_wal_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_single_thread");

    for batch_size in [1, 10, 100] {
        let label = format!("batch_{}", batch_size);
        let entries = make_entries(batch_size);
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        // Control: traditional sync (fsync after every batch).
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

        // Experiment: io_uring submit_and_commit (amortizes fsync across batches).
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

/// Multi-threaded: sync (control) vs group_commit (experiment).
fn bench_wal_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_multi_thread");
    group.sample_size(30);

    for num_threads in [2, 4, 8] {
        let label = format!("{}_threads", num_threads);

        // Control: each thread syncs independently (serial fsync, no group commit).
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

        // Experiment: io_uring group commit (merge multiple threads' data into one fsync).
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

/// Sustained throughput: sync (control) vs submit_and_commit (experiment).
fn bench_wal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_throughput");
    group.sample_size(10);

    for num_batches in [100, 500] {
        let label = format!("{}_batches", num_batches);

        // Control: traditional sync.
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

        // Experiment: io_uring submit_and_commit.
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
