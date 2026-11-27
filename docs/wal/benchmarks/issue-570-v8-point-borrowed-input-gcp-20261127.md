# Issue #570 V8 Single-Entry Point Put Borrowed-Input GCP A/B

- **Date**: 2026-11-27
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#576](https://github.com/ben1009/toy-kv-engine/pull/576)
- **Baseline commit**: `edc8140d2ab2004bd085a3c082d7ceb378dc67a5` (`main`, post-#575)
- **Candidate commit**: `178afa4598793ce78b85066323ea2c4a34ff95d5` (`perf/issue-570-single-entry-point`)
- **Patch SHA-256**: `153eeaa171aa99cc476a23de8d1591d9d58e56599470b49cb6eeee681da6c449`
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/dev/md127`, `/mnt/gcp-wal-bench`, `ext4`, `rw,noatime,stripe=256`), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`)
- **Local archive**: `bench-results/gcp-wal-20261127T024813Z/collected/results` (`sha256: a0b6fa8e046fae79d6142ceb6444f432951757d7861bfc6f221877e9a22854bf`, 50/50 runs verified)

## 1. 50-Run Counterbalanced A/B Results (5 Pairs per Workload, Unprofiled)

| Workload | Baseline Median | Candidate Median | Paired Median Delta (ops/s · CPU/put · p99) | Wins (C vs B: ops/s · CPU · p99) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (100k puts, 1t) | 9,007 puts/s · 51.65 μs/put · 0.545 ms p99 | 8,944 puts/s · 51.93 μs/put · 0.546 ms p99 | -0.75% · +0.78% · +24.95% | 2/5 · 1/5 · 2/5 |
| **Point8** (100k puts, 8t) | 38,064 puts/s · 44.80 μs/put · 0.798 ms p99 | 37,577 puts/s · 45.19 μs/put · **0.747 ms p99** | -0.60% · +0.92% · **-8.46%** | 0/5 · 1/5 · **5/5** |
| **Point16** (100k puts, 16t) | 53,436 puts/s · 67.26 μs/put · 0.965 ms p99 | 53,138 puts/s · 68.60 μs/put · 0.916 ms p99 | -0.90% · +1.93% · -9.86% | 2/5 · 0/5 · 3/5 |
| **Batch16/w16** (100k puts, 16t) | 429,720 puts/s · 8.31 μs/put · 1.025 ms p99 | 425,405 puts/s · 8.26 μs/put · 1.332 ms p99 | -1.12% · +0.01% · +33.27% | 2/5 · 2/5 · 1/5 |
| **Batch64/w16** (100k puts, 16t) | 632,778 puts/s · 4.90 μs/put · 2.452 ms p99 | 629,766 puts/s · 4.89 μs/put · 2.506 ms p99 | -0.31% · +0.30% · +3.69% | 2/5 · 2/5 · 2/5 |

- **No net improvement observed**: Borrowing `(user_key, &prefixed)` and bypassing `WalBatch`/`CanonicalWalEntryStream` did not reduce measured CPU/Put or improve throughput on `Point1` (`-0.75%` ops/s, `+0.78%` CPU/put), `Point8` (`-0.60%` ops/s, `+0.92%` CPU/put), or `Point16` (`-0.90%` ops/s, `+1.93%` CPU/put, `0/5` CPU wins).
- **Sampled latency**: `Point8` p99 improved `-8.46%` (`5/5` wins under `--latency-sample-every 100`), while `Point1` and `Batch16` p99 medians varied within `1:100` sampling noise.
- **Batch guardrails**: `Batch16/w16` and `Batch64/w16` CPU/put stayed flat (`+0.01%` and `+0.30%`).

## 2. Cleanup Verification

- Verified local archive checksum (`a0b6fa8e046fae79d6142ceb6444f432951757d7861bfc6f221877e9a22854bf`), `summary.json` (`complete: true`, `50/50`), and `remote-exit.json` (`failed: false`, `runs_started: 50`) before teardown.
- Deleted the benchmark VM (`walbench-261127-024934-917853a0`), temporary SSH firewall rule (`walbench-ssh-917853a0`), and destroyed the isolated Terraform VPC (`walbench-isolated-vpc`), subnet (`walbench-isolated-subnet`), and egress route (`Listed 0 items` for instances, disks, walbench firewalls, and walbench networks).
