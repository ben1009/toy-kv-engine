# Issue #570 V8 Point Sequencer Critical-Section GCP A/B

- **Date**: 2026-11-27
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#574](https://github.com/ben1009/toy-kv-engine/pull/574)
- **Baseline commit**: `2562767ba4269532a5f399048071bf6f68558c49` (`main`, V8 Candidate Target8 default)
- **Candidate commit**: `70ec99bdf8e189bfcc122d36e220c4ac94d5c177` (`perf/issue-570-producer-sequencer`)
- **Patch SHA-256**: `af73b5b5323ab082b3574b406d023f4b610e010f9977df66ee9e6e8189d77360`
- **Environment**: `c4-standard-16-lssd` (`us-central1-b`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, ext4), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`)
- **Local archive**: `bench-results/gcp-pr574-ab-20261127-b3/collection-attempt-01` (`sha256: 639f83c4a52f6ad0fbe38b956b5e209dcacb6d5e6edae78ffebe042518b6e524`, 50/50 runs verified)

## 1. 50-Run Counterbalanced A/B Results (5 Pairs per Workload, Unprofiled)

| Workload | Baseline Median | Candidate Median | Paired Median Delta (ops/s · CPU/put · p99) | Wins (C vs B: ops/s · CPU · p99) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (100k puts, 1t) | 8,532 puts/s · 52.11 μs/put · 0.464 ms p99 | 8,444 puts/s · 52.57 μs/put · 0.520 ms p99 | -1.34% · +0.55% · +13.67% | 2/5 · 1/5 · 1/5 |
| **Point8** (100k puts, 8t) | 33,532 puts/s · 46.85 μs/put · 0.847 ms p99 | 33,800 puts/s · 46.37 μs/put · 0.837 ms p99 | +1.02% · -1.10% · +0.07% | 3/5 · 3/5 · 2/5 |
| **Point16** (100k puts, 16t) | 49,758 puts/s · 72.42 μs/put · 1.117 ms p99 | **50,664 puts/s** · **71.72 μs/put** · **0.843 ms p99** | **+1.68% · -0.52% · -21.92%** | **5/5 · 3/5 · 5/5** |
| **Batch16/w16** (100k puts, 16t) | 407,480 puts/s · 8.24 μs/put · 1.126 ms p99 | 417,277 puts/s · 8.39 μs/put · 1.197 ms p99 | +0.88% · +1.41% · +2.71% | 4/5 · 1/5 · 1/5 |
| **Batch64/w16** (100k puts, 16t) | 628,935 puts/s · 4.85 μs/put · 2.662 ms p99 | 630,524 puts/s · 4.83 μs/put · 2.515 ms p99 | +0.21% · -0.50% · -12.00% | 4/5 · 3/5 · 4/5 |

- At **Point16**, Candidate won **5/5** pairs on both throughput (**+1.68%**, `49,758 → 50,664 puts/s`) and sampled p99 (**-21.92%**, `1.117 → 0.843 ms`), with modest CPU/Put improvement (`72.42 → 71.72 μs`, `-0.52%` paired median, `3/5` wins).
- At **Point1**, Candidate lost `4/5` pairs on CPU (`+0.55%`) and `4/5` on p99 (`+13.67%`); **Batch16/w16** and **Batch64/w16** guardrails showed no throughput regression (`+0.88%` and `+0.21%`).

## 2. Point16 Producer CPU Attribution Diagnostic (3 Paired Runs, `--wal-cpu-attribution-sample-every 64`)

> [!NOTE]
> Diagnostic runs only; not scored as unprofiled throughput.

| Phase | Baseline Median | Candidate Median | Delta |
| --- | ---: | ---: | ---: |
| Preflight | 1.928 μs | 2.167 μs | +0.240 μs (+12.44%) |
| Stage A Prealloc | 1.247 μs | 1.148 μs | -0.099 μs (-7.94%) |
| **Sequencer Wait** | 4.939 μs | **4.360 μs** | **-0.579 μs (-11.73%)** |
| **Sequencer Hold** | 6.572 μs | **6.172 μs** | **-0.400 μs (-6.08%)** |
| Commit Setup | 0.856 μs | 0.782 μs | -0.074 μs (-8.67%) |
| **Admission** | 3.802 μs | **3.434 μs** | **-0.368 μs (-9.68%)** |
| **Frame Finish** | 10.661 μs | **11.518 μs** | **+0.857 μs (+8.03%)** |
| **Producer Total** | 25.044 μs | 25.302 μs | +0.258 μs (+1.03%) |

- Moving `encode_internal_key()` and `frontier_resources` registration out of `write_lock` reduced `Sequencer Hold` by `0.400 μs/put` (`-6.08%`) and `Sequencer Wait` by `0.579 μs/put` (`-11.73%`), while shifting `frontier_resources` registration cost from `Admission` (`-0.368 μs`) into `Frame Finish` (`+0.857 μs`).
- Total Producer CPU remained essentially flat (`25.044 → 25.302 μs`), while the shorter `write_lock` critical section improved Point16 throughput and p99 tail latency across all 5 unprofiled pairs.

## 3. Cleanup Verification

- Verified local archive checksum (`639f83c4a52f6ad0fbe38b956b5e209dcacb6d5e6edae78ffebe042518b6e524`), `summary.json` (`complete: true`, `50/50`), and `remote-exit.json` (`failed: false`, `runs_started: 50`) before teardown.
- Deleted the benchmark VM, temporary SSH firewall rule, and destroyed the isolated Terraform VPC, subnet, and egress route (`Listed 0 items` for instances, firewalls, and networks).
