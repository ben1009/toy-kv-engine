# Issue #570 V8 Point Frame Finish Optimization GCP A/B

- **Date**: 2026-11-27
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#578](https://github.com/ben1009/toy-kv-engine/pull/578)
- **Baseline commit**: `1cb5a28456c1c6bd02c6e851b0ec311f88733a33` (`main`)
- **Candidate commit**: `24c52ae8840beb25f2ef80aada9ffb9db52778af` (`perf/issue-570-frame-finish`)
- **Patch SHA-256** (5 tracked A/B diff files): `1fa870ebbcc3768685ba20ae95b79c741ead98002e6778ca0d8810eab5af1721`
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, `ext4`), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`)
- **Local archive**: `bench-results/gcp-wal-20261127T052226Z/collected/results` (`sha256: b2747170c7f9eb8281bf5c7d05b6d0bff36e2fd74cc15ae2352b8986522ca3b2`, 50/50 runs verified)

## 1. 50-Run Counterbalanced A/B Results (5 Pairs per Workload, Unprofiled)

| Workload | Baseline Median | Candidate Median | Paired Median Delta (ops/s · CPU/put · p99) | Wins (C vs B: ops/s · CPU · p99) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (100k puts, 1t) | 8,742 puts/s · 52.01 μs/put · 0.471 ms p99 | 8,762 puts/s · 52.14 μs/put · 0.435 ms p99 | -0.28% · +0.46% · -7.58% | 2/5 · 1/5 · 3/5 |
| **Point8** (100k puts, 8t) | 37,257 puts/s · 44.10 μs/put · 0.864 ms p99 | 37,121 puts/s · 44.19 μs/put · **0.797 ms p99** | +0.35% · +0.21% · **-9.40%** | 3/5 · 2/5 · **5/5** |
| **Point16** (100k puts, 16t) | 51,612 puts/s · 68.46 μs/put · 1.291 ms p99 | **53,061 puts/s** · **65.97 μs/put** · **0.949 ms p99** | **+2.48% · -2.98% · -13.95%** | **5/5 · 5/5 · 4/5** |
| **Batch16/w16** (100k puts, 16t) | 416,315 puts/s · 8.26 μs/put · 1.087 ms p99 | 418,862 puts/s · 8.05 μs/put · **1.015 ms p99** | +0.40% · -3.54% · **-6.56%** | 3/5 · 3/5 · **5/5** |
| **Batch64/w16** (100k puts, 16t) | 630,228 puts/s · 4.76 μs/put · 2.549 ms p99 | 633,571 puts/s · 4.80 μs/put · 2.510 ms p99 | +0.38% · +0.68% · -2.70% | 3/5 · 1/5 · 3/5 |

- **Point16**: Candidate won **5/5** pairs on both throughput (**+2.48%**, `51,612 → 53,061 puts/s`) and CPU/Put (**-2.98%**, `68.46 → 65.97 μs/put`), and **4/5** on sampled p99 (**-13.95%**, `1.291 → 0.949 ms`).
- **Fast-path hit rate (`merged_digest_frames_ready_count`)**: `100,000 / 100,000` (100%) on `Point1` and `Point16`, and `0` on `Batch16/w16` and `Batch64/w16`.
- **Batch guardrails**: `Batch16/w16` and `Batch64/w16` showed no regression (`+0.40%` and `+0.38%` throughput).

## 2. Point16 CPU Attribution Diagnostics (3 Counterbalanced Pairs, `--wal-cpu-attribution-sample-every 64`)

> [!NOTE]
> Diagnostic runs only (`bench-results/gcp-wal-20261127T052226Z/cpu-attribution`); not scored as unprofiled throughput. Because the merged Cursor path records `v8_digest_ready_cpu_ns = 0` and attributes the combined lock/publish to `v8_frames_ready_submit_cpu_ns`, both are compared as a sum.

| Metric (Median of 3 Paired Runs) | Baseline (`1cb5a284`) | Candidate (`24c52ae8`) | Delta |
| --- | ---: | ---: | ---: |
| `frame_finish_cpu_ns` | 9,146.5 ns | **8,237.5 ns** | **-9.94% (-909.0 ns)** |
| `v8_digest_ready_cpu_ns + v8_frames_ready_submit_cpu_ns` | 3,354.5 ns | **2,808.0 ns** | **-16.29% (-546.5 ns)** |
| `v8_logical_encode_cpu_ns` | 2,122.0 ns | 2,126.0 ns | +0.19% |
| `v8_frame_encode_cpu_ns` | 615.5 ns | 619.0 ns | +0.57% |
| `producer_total_cpu_ns` | 22,987.5 ns | **22,155.5 ns** | **-3.62% (-832.0 ns)** |
| `io_worker_completion_processing` CPU / item | 1,375.5 ns | 1,358.0 ns | -1.27% |
| Process CPU / put (diagnostic run) | 67.3 μs | **65.2 μs** | **-3.16% (-2.1 μs)** |

- `frame_finish_cpu_ns` dropped by **9.94%** (`-909 ns/put`) and `v8_digest_ready_cpu_ns + v8_frames_ready_submit_cpu_ns` dropped by **16.29%** (`-546.5 ns/put`), while `v8_logical_encode_cpu_ns` (`+0.19%`), `v8_frame_encode_cpu_ns` (`+0.57%`), and `io_worker_completion_processing` (`-1.27%`) remained flat or slightly improved, confirming real CPU reduction without cost shifting.

## 3. Cleanup Verification

- Verified local archive checksum (`b2747170c7f9eb8281bf5c7d05b6d0bff36e2fd74cc15ae2352b8986522ca3b2`), `summary.json` (`complete: true`, `50/50`), and `remote-exit.json` (`failed: false`, `runs_started: 50`) before teardown.
- Deleted the benchmark VM (`walbench-261127-052346-55fdd425`), temporary SSH firewall rule (`walbench-ssh-55fdd425`), and destroyed the isolated Terraform VPC (`walbench-isolated-vpc`), subnet (`walbench-isolated-subnet`), and egress route (`Listed 0 items` across instances, disks, walbench firewalls, and walbench networks).
