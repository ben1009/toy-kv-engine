# Issue #570 V8 Point Lock-Merge + ACK Wakeup Cumulative GCP A/B

- **Date**: 2026-11-26
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#571](https://github.com/ben1009/toy-kv-engine/pull/571)
- **Baseline commit**: `20d83e8146abcbad9d8488a9c2e3d3dbee99f35c` (`main`, V8 Candidate Target8 default)
- **Candidate commit**: `30c9092d7ae33aa3182aa74a4741ff3cc9dfdb4d` (`perf/v8-point-wait-durable-lock-merge`)
- **Environment**: `c4-standard-16-lssd` (`us-central1-b`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/test-ssd`, ext4), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (Candidate Target8 defaults, no env overrides)
- **Local archive**: `bench-results/gcp-issue570-ab-20261126/issue570-ab.tar.gz` (`sha256: a17e4e2bbbe0eed46fafd23a3c548f328892ef08e896eef51602fc22f16b6939`)

## 1. 40-Run Counterbalanced A/B Results (5 Pairs per Workload)

| Workload | Baseline Median | Candidate Median | Paired Median Delta | Wins (C vs B) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (20k puts, 1t) | 8,128.0 puts/s · 52.28 μs/put · 0.541 ms p99 | 8,211.8 puts/s · 52.61 μs/put · 0.626 ms p99 | +0.54% ops/s · +0.71% CPU/put | 3/5 ops/s · 1/5 CPU |
| **Point8** (100k puts, 8t) | 33,605.6 puts/s · 47.58 μs/put · 0.846 ms p99 | 34,001.8 puts/s · 47.11 μs/put · 0.828 ms p99 | +1.18% ops/s · -0.93% CPU/put | 3/5 ops/s · 3/5 CPU |
| **Point16** (100k puts, 16t) | 49,100.8 puts/s · 74.75 μs/put · 1.048 ms p99 | **49,642.0 puts/s** · **70.92 μs/put** · **0.948 ms p99** | **+1.86% ops/s · -5.78% CPU/put** | **5/5 ops/s · 5/5 CPU** |
| **Batch64/w16** (100k puts, 16t) | 627,235.3 puts/s · 4.88 μs/put · 2.337 ms p99 | 629,128.6 puts/s · 4.77 μs/put · 2.440 ms p99 | +0.30% ops/s · -2.12% CPU/put | 4/5 ops/s · 3/5 CPU |

- At **Point16**, Candidate won **5/5** pairs on both throughput (**+1.86%**) and CPU/Put (**-5.78%**, `74.75 μs` → `70.92 μs`), while reducing sampled p99 median from `1.048 ms` to `0.948 ms`.
- **Batch64/w16** had no regression (+0.30% ops/s, -2.12% CPU/put).

## 2. Point16 Diagnostic Comparison (`--wal-v8-durability-sample-every 64`, 100k puts)

| Metric | Baseline (`20d83e81`) | Candidate (`30c9092d`) |
| --- | ---: | ---: |
| Sync count | 11,943 | 11,961 |
| Sampled follower waits | 1,549 | 1,534 |
| `condvar_wait_count` dist | `{1: 505, 2: 709, 3: 332, 4: 3}` | `{1: 556, 2: 956, 3: 22}` |
| Avg condvar waits / follower | 1.892 | **1.652** (-12.7%) |
| Followers with >= 3 waits | 21.6% (335/1,549) | **1.4%** (22/1,534) |
| `publish_notify_all_count` | — | 11,923 |
| `publish_notify_skipped_count` | — | 26 |
| `driver_drop_notify_all_count` | — | **0** |
| `driver_drop_notify_one_count` | — | **11,664** |

- All 11,664 driver-drop handoffs with waiting followers used `notify_one()` instead of `notify_all()`, eliminating thundering-herd wakeups on driver handoff and cutting 3+ condvar waits from 21.6% to 1.4%.
- `point16-p1-candidate` reported `initialized_bytes = 421,527,552` (~402 MiB), confirming `PointOnlyLookahead` on ext4 RAID0.
- Note: the two sampled diagnostic runs are diagnostic-only; only the 40 unscored runs above are used for throughput/CPU comparisons.

## 3. Cleanup Verification

- Verified `issue570-ab.tar.gz` SHA-256 checksum locally before teardown.
- Destroyed all 5 Terraform resources (`gcp-issue570-bench`, `gcp-issue570-bench-ssh`, `gcp-issue570-subnet`, `gcp-issue570-net`, `environment_lifetime`) and verified `0` instances and `0` `gcp-issue570-net` networks remain.
