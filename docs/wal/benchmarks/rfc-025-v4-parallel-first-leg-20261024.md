# RFC 025 Stage 8: exploratory v4 Parallel first leg, 2026-10-24

**Status: exploratory; not a qualification result.** This records the first
RFC 025 performance comparison: a pre-v7 v4 Parallel baseline against the
current v4 Parallel implementation, with PITR disabled. Three paired repeats
per workload and filesystem are too few to establish stability, and this run
did not include same-binary controls or the full workload matrix.

## Executables and environment

- Baseline: `c7aa39cd56f37b767352d946bab551727b64b4a5`, the parent of the first
  v7 WAL codec commit. It already defaults ordinary v4 WALs to Parallel.
- Candidate: `f08f4cd7f0bac424d9732837401a89a76ec5b5b6` (`main` after PR #524).
- Both were built with
  `cargo build --locked --release --package kv-engine --bin write-perf --features bench`
  using `rustc 1.100.0-nightly (6bb1652a0 2026-09-22)`.
- Binary SHA-256: baseline
  `eec8b0e3e34315b4e5d991bc08c82ce10143a3486a4e1054591223d944d6b589`;
  candidate `d12902ba53e16f4a5c3b18dde6b2fd758169140e14ea9b6ab98f3f369c927023`.
- Host: x86_64 Linux 6.12.71-1-lts, Intel Core i9-13900T, 32 logical CPUs.
- Storage: `/dev/shm` tmpfs (32 GiB); ext4 on `/dev/nvme0n1p3`, the
  Solidigm SSDPFKNU010TZ. Both cases used fresh databases and no PITR.

## Protocol

The scored workloads were:

| Workload | Configuration | Measurements |
| --- | --- | --- |
| Single-writer WAL puts | 5,000 puts, 1 writer, 1 KiB values, 1 GiB SST target | Throughput and sampled put p50/p99 (every 100 puts) |
| Four-writer WAL puts | 50,000 puts, 4 writers, 1 KiB values, 1 GiB SST target | Throughput and sampled put p50/p99 (every 100 puts) |
| Mixed WAL workload | 5,000-key setup, 2 writers, 2 readers, one scan worker, 3 seconds, 1 KiB values | Write rate, read rate, and scan rows; this workload does not sample p50/p99 |

Each filesystem/workload cell had one unscored baseline and candidate warmup,
then three pairs in baseline/candidate, candidate/baseline, baseline/candidate
order, with a fixed 3-second idle between runs. The write-only commands selected
`--wal-io-mode parallel` explicitly. The mixed command used `KvEngine::open`,
whose `WalIoMode::default()` is Parallel in both source revisions. Mixed-run
latency is not available from this harness.

The exact per-run commands, run IDs, raw-line mapping, and checksums are in the
[run manifest](artifacts/rfc-025-v4-first-leg-20261024-runs.tsv). The driver is
[here](artifacts/rfc-025-v4-first-leg-20261024-run.sh), and the original
`write-perf` JSON stdout records are in the [JSONL artifact](artifacts/rfc-025-v4-first-leg-20261024.jsonl).
The artifact contains 12 warmups and 36 scored records. Its SHA-256 is
`31d845c7046446948cdebb1b3ad1122823679f72c5a3662173a7f613c41f6a55`;
the manifest SHA-256 is
`27c3d0c19eef36d64c4680dac3318652dd27875c702f1ae1b5edb151dd90ef7d`.

## WAL put results

Throughput and latency values are medians across the three runs per arm.
Paired ratios are candidate divided by baseline for each pair; the median is
shown after the list. A value above 1 in a latency column is worse.

| Filesystem / writers | Baseline / candidate puts/s | Paired throughput ratios (median) | Baseline / candidate p50 ms | Paired p50 ratios (median) | Baseline / candidate p99 ms | Paired p99 ratios (median) |
| --- | ---: | --- | ---: | --- | ---: | --- |
| tmpfs / 1 | 34,578 / 37,580 | 1.224, 1.228, 0.831 (1.224) | 0.0159 / 0.0139 | 0.752, 0.919, 1.224 (0.919) | 1.396 / 1.294 | 0.860, 1.046, 0.955 (0.955) |
| tmpfs / 4 | 153,306 / 150,605 | 0.982, 1.001, 0.948 (0.982) | 0.0185 / 0.0188 | 1.005, 1.016, 0.963 (1.005) | 0.187 / 0.182 | 1.141, 0.490, 1.156 (1.141) |
| ext4 / 1 | 770 / 770 | 1.014, 0.961, 1.000 (1.000) | 0.926 / 0.898 | 0.986, 0.969, 0.980 (0.980) | 8.245 / 9.557 | 0.964, 1.783, 1.220 (1.220) |
| ext4 / 4 | 4,345 / 3,682 | 0.386, 1.020, 2.239 (1.020) | 0.676 / 0.890 | 2.364, 1.002, 0.539 (1.002) | 9.342 / 8.659 | 9.808, 1.015, 0.113 (1.015) |

The four-writer ext4 pairs span 0.386×–2.239× in throughput, with p99 ratios
from 0.113× to 9.808×. That spread makes the median uninformative as a
regression or improvement estimate. The single-writer ext4 p99 pairs also vary
from 0.964× to 1.783×. No stability threshold or confidence interval is claimed.

## Mixed WAL results

| Filesystem | Baseline / candidate writes/s | Paired write-rate ratios (median) | Baseline / candidate reads/s | Paired read-rate ratios (median) |
| --- | ---: | --- | ---: | --- |
| tmpfs | 99,036 / 94,906 | 1.021, 0.964, 0.896 (0.964) | 1,773,832 / 1,657,598 | 0.934, 1.039, 0.911 (0.934) |
| ext4 | 1,520 / 1,521 | 0.952, 1.001, 1.001 (1.001) | 2,570,085 / 2,371,351 | 0.909, 0.923, 0.985 (0.923) |

These are descriptive three-pair measurements. Mixed-run p50/p99 are not
provided by `concurrent_rw_wal`; scan row counts are retained in the raw JSON.

## Interpretation and remaining gate

This screen shows near-parity medians for the simple ext4 put cases, but the
device results are too variable to conclude parity. The tmpfs four-writer put
median is 1.8% lower, and mixed read-rate medians are lower in both filesystems;
the three-pair sample cannot distinguish a code effect from run-to-run noise.
The results neither qualify nor reject the RFC change.

Stage 8 remains open. The other RFC comparisons, same-binary controls,
confidence intervals, workload matrix (including batch64, value-size variants,
rotation, scans, and PITR), archive-I/O controls, and recovery/RTO measurements
are still required. No adoption decision follows from this exploratory leg.
