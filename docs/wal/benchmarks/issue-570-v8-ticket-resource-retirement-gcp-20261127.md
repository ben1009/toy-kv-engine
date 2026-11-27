# Issue #570 V8 Ticket Resource Retirement & Map Consolidation GCP A/B

- **Date**: 2026-11-27
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#579](https://github.com/ben1009/toy-kv-engine/pull/579)
- **Baseline commit**: `2de2c84ae6b818debc254e50c332dcc8a87e3a3a` (`main`, post-#578)
- **Candidate commit**: `db657aa0a792644a96137bc556f1101e14663830` (`perf/issue-570-ticket-resource-retirement`)
- **Patch SHA-256** (5 tracked A/B diff files): `b6daa64947fd61baae69f0226f03b824337bf264e50cce566463ad069caef266`
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, `ext4`), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`)
- **Local archive**: `bench-results/gcp-wal-20261127T065727Z/collected/results` (`sha256: cf88e713c0da89976dec2e2321d54b4338d978fef81a76608b115e4481bb521e`, 50/50 runs verified)

## 1. 50-Run Counterbalanced A/B Results (5 Pairs per Workload, Unprofiled)

| Workload | Baseline Median | Candidate Median | Paired Median Delta (ops/s · CPU/put · p99) | Wins (C vs B: ops/s · CPU · p99) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (100k puts, 1t) | 8,687 puts/s · 51.62 μs/put · 0.475 ms p99 | 8,656 puts/s · 51.49 μs/put · 0.502 ms p99 | -0.28% · -0.28% · +7.53% | 2/5 · 3/5 · 2/5 |
| **Point8** (100k puts, 8t) | 36,884 puts/s · 43.44 μs/put · 0.800 ms p99 | **37,260 puts/s** · **43.18 μs/put** · **0.769 ms p99** | **+1.02% · -0.68% · -6.52%** | 3/5 · 4/5 · 4/5 |
| **Point16** (100k puts, 16t) | 54,006 puts/s · 64.66 μs/put · 0.945 ms p99 | **54,134 puts/s** · 64.54 μs/put · **0.907 ms p99** | **+0.68%** · +0.36% · **-11.50%** | **4/5** · 2/5 · 3/5 |
| **Batch16/w16** (100k puts, 16t) | 424,374 puts/s · 7.88 μs/put · 1.249 ms p99 | 413,095 puts/s · 7.92 μs/put · **1.056 ms p99** | -1.60% · -0.11% · **-17.62%** | 1/5 · 3/5 · 3/5 |
| **Batch64/w16** (100k puts, 16t) | 633,471 puts/s · 4.80 μs/put · 2.512 ms p99 | 633,070 puts/s · **4.60 μs/put** · 2.561 ms p99 | -0.05% · **-3.26%** · +1.92% | 2/5 · **5/5** · 2/5 |

- **Point16**: Throughput improved **+0.68%** (`4/5` wins, `54,006 → 54,134 puts/s`) and sampled p99 improved **-11.50%** (`0.945 → 0.907 ms`), while overall process CPU/Put was flat (`+0.36%` paired median, `64.66 → 64.54 μs/put`).
- **Point8**: Throughput (`+1.02%`), CPU/Put (`-0.68%`, `4/5` wins), and p99 (`-6.52%`, `4/5` wins) improved slightly.
- **Batch guardrails**: `Batch64/w16` CPU/Put improved **-3.26%** (`5/5` wins, `4.80 → 4.60 μs/put`) with flat throughput (`-0.05%`); `Batch16/w16` throughput was `-1.60%` (`1/5` wins) with flat CPU/Put (`-0.11%`) and `-17.62%` p99.

## 2. Point16 CPU Attribution & Durability Diagnostics (3 Counterbalanced Pairs, `--wal-cpu-attribution-sample-every 64 --wal-v8-durability-sample-every 64`)

> [!NOTE]
> Diagnostic runs only (`bench-results/gcp-wal-20261127T065727Z/diagnostics`); not scored as unprofiled throughput.

| Metric (Median of 3 Paired Runs) | Baseline (`2de2c84a`) | Candidate (`db657aa0`) | Delta |
| --- | ---: | ---: | ---: |
| `sequencer_hold_cpu_ns` | 4,016.0 ns | **3,941.5 ns** | **-1.86%** |
| `frame_finish_cpu_ns` | 8,097.5 ns | **7,716.5 ns** | **-4.71% (-381.0 ns)** |
| `producer_total_cpu_ns` | 22,018.5 ns | **21,235.0 ns** | **-3.56% (-783.5 ns)** |
| `res_retire` CPU / ticket (`before_publish`) | 1,861.9 ns | **1,402.4 ns** | **-24.68% (-459.5 ns)** |
| `res_retire` lock hold / ticket | 1,682.5 ns | **1,256.9 ns** | **-25.30%** |
| `res_retire` lock wait / ticket | 37.8 ns | 42.2 ns | +11.92% (+4.4 ns) |
| `res_retire` remove CPU / ticket | 299.1 ns | **0.0 ns** | **-100.00%** |
| `guard_retire` CPU / ticket (`after_publish`) | 1,358.6 ns | **1,316.0 ns** | **-3.13%** |
| `guard_retire` collect CPU / ticket | 103.1 ns | **34.0 ns** | **-67.03%** |
| `guard_retire` lock wait / ticket | 42.9 ns | **35.9 ns** | **-16.18%** |
| Process CPU / put (diagnostic run) | 66.9 μs | 66.5 μs | -0.65% |

- **Lock contention check**: Consolidating `frontier_resources` and `producer_guards` into `ticket_resources` added only `+4.4 ns/ticket` to `res_retire` lock wait (`37.8 → 42.2 ns`) while reducing `res_retire` lock hold by **25.30%** (`1,682.5 → 1,256.9 ns`), `guard_retire` lock wait by **16.18%** (`42.9 → 35.9 ns`), `frame_finish_cpu_ns` by **4.71%** (`-381.0 ns/put`), and `producer_total_cpu_ns` by **3.56%** (`-783.5 ns/put`).

## 3. Cleanup Verification

- Verified local archive checksum (`cf88e713c0da89976dec2e2321d54b4338d978fef81a76608b115e4481bb521e`), `summary.json` (`complete: true`, `50/50`), and `remote-exit.json` (`failed: false`, `runs_started: 50`) before teardown.
- Deleted the benchmark VM (`walbench-261127-065848-25db734f`), temporary SSH firewall rule (`walbench-ssh-25db734f`), and destroyed the isolated Terraform VPC (`walbench-isolated-vpc`), subnet (`walbench-isolated-subnet`), and egress route (`Listed 0 items` across instances, disks, walbench firewalls, and walbench networks).
