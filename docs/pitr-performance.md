# PITR Performance Baseline

Measured on 2026-09-16 with commit `657576bc` plus the in-tree `pitr-perf`
harness changes.

## Method

```bash
cargo run --release --bin pitr-perf -- --operations 10000
```

The command was run three times. Each run alternates PITR-disabled and
PITR-enabled cases at 1, 4, 8, 16, and 32 writer threads. Each case creates a
fresh database and writes 10,000 unique keys with 128-byte values. WAL is
enabled in both modes. PITR uses an unlimited archive-I/O rate and a segment
large enough that foreground timing contains WAL-v5 admission/encoding but no
automatic archive boundary. The enabled case then times an explicit durable
recovery point separately as `catchup_seconds`.

Host: Linux 6.18.9, Intel Core i9-13900T, 32 logical CPUs. The database and
repository were under `/tmp` on tmpfs, so these numbers are a reproducible CPU
and synchronization baseline, not a storage-device durability benchmark.

## Median of three alternating runs

| Writers | PITR disabled writes/s | PITR enabled writes/s | Enabled / disabled | Catch-up seconds |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 149,531 | 132,938 | 88.9% | 0.126 |
| 4 | 150,149 | 169,065 | 112.6% | 0.122 |
| 8 | 153,272 | 127,252 | 83.0% | 0.126 |
| 16 | 83,780 | 92,204 | 110.1% | 0.133 |
| 32 | 15,532 | 14,189 | 91.4% | 0.144 |

The 4- and 16-writer inversions show that this short tmpfs workload is noisy;
they are not evidence that PITR improves throughput. The stable conclusion is
that the enabled foreground path stayed in the same order of magnitude, while
copying, catalog publication, and source-manifest completion were isolated in
the explicit 0.12–0.15 second catch-up measurement.

Device-backed same-device/separate-device archive tests and configured-rate
backlog tests remain environment-specific follow-ups; this baseline does not
claim their results.
