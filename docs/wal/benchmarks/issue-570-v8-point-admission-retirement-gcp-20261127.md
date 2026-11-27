# Issue #570 V8 Point Admission Coordination & Resource Retirement Consolidation GCP A/B

- **Date**: 2026-11-27
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#575](https://github.com/ben1009/toy-kv-engine/pull/575)
- **Baseline commit**: `84c997b0f1b11c2e2eeb5eb76ccfa923f9b45995` (`main`, post-#574)
- **Candidate commit**: `78d86272e9996304aa68fd8136278c1b1a790eac` (`perf/issue-570-point-admission-coordination`)
- **Patch SHA-256**: `89debd6c2a12bd2b659efd834c9e9cca3b1c1539a6e2ff7434067b1e1c77c844`
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, ext4), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`)
- **Local archive**: `bench-results/gcp-pr575-ab-20261127-c2/collection-attempt-01` (`sha256: 11578d2c266ac19de3bf660858568cc9f37dc8f0b6d6b90e82b87e716f684af2`, 50/50 runs verified)

## 1. 50-Run Counterbalanced A/B Results (5 Pairs per Workload, Unprofiled)

| Workload | Baseline Median | Candidate Median | Paired Median Delta (ops/s · CPU/put · p99) | Wins (C vs B: ops/s · CPU · p99) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (100k puts, 1t) | 8,034 puts/s · 51.32 μs/put · 0.769 ms p99 | 8,335 puts/s · 51.22 μs/put · 0.627 ms p99 | +0.65% · -1.17% · -18.58% | 4/5 · 3/5 · 5/5 |
| **Point8** (100k puts, 8t) | 32,077 puts/s · 45.68 μs/put · 0.874 ms p99 | 32,407 puts/s · **44.13 μs/put** · 0.918 ms p99 | +0.96% · **-2.68%** · +8.01% | 3/5 · **5/5** · 1/5 |
| **Point16** (100k puts, 16t) | 52,059 puts/s · 70.22 μs/put · 1.344 ms p99 | **53,641 puts/s** · **66.66 μs/put** · **1.096 ms p99** | **+2.89% · -5.01% · -1.57%** | **5/5 · 5/5 · 4/5** |
| **Batch16/w16** (100k puts, 16t) | 413,509 puts/s · 8.28 μs/put · 0.997 ms p99 | 426,583 puts/s · 8.13 μs/put · 1.037 ms p99 | +2.42% · -1.43% · +2.51% | 5/5 · 4/5 · 2/5 |
| **Batch64/w16** (100k puts, 16t) | 631,796 puts/s · 4.77 μs/put · 2.599 ms p99 | 632,505 puts/s · 4.76 μs/put · 2.476 ms p99 | +0.06% · +0.26% · -3.34% | 4/5 · 1/5 · 4/5 |

- At **Point16**, Candidate won **5/5** pairs on both throughput (**+2.89%**, `52,059 → 53,641 puts/s`) and CPU/Put (**-5.01%**, `70.22 → 66.66 μs/put`, `-3.56 μs/put`), while winning `4/5` on sampled p99 (`1.344 → 1.096 ms`).
- At **Point8**, Candidate won **5/5** pairs on CPU/Put (`45.68 → 44.13 μs/put`, `-2.68%`).
- At **Point1**, Candidate improved throughput (`+0.65%`, `4/5`), CPU/Put (`-1.17%`, `3/5`), and sampled p99 (`-18.58%`, `5/5`).
- **Batch16/w16** and **Batch64/w16** guardrails showed no regression.

## 2. Point16 Durability, Lock-Count, and Fast-Path Diagnostics (3 Paired Runs, `--wal-v8-durability-sample-every 64 --mutex-lock-sample-every 64`)

> [!NOTE]
> Diagnostic runs only; not scored as unprofiled throughput. Per P2-1 review guidance, Retirement CPU compares `Before: release_cpu_ns + finish_cpu_ns` against `After: finish_cpu_ns` (since `retire_durable` folds credit release into `finish_cpu_ns`, leaving `release_cpu_ns = 0`).

| Metric | Baseline (`84c997b0`) | Candidate (`78d86272`) | Delta |
| --- | ---: | ---: | ---: |
| `resource_ledger` acquisitions / put | 5.00 | **3.11** | **-37.7%** (wait `0.352 → 0.217 μs`) |
| `reservation_owner` acquisitions / put | 4.00 | **3.11** | **-22.1%** (wait `0.554 → 0.361 μs`) |
| `retire_durable_fast_path_count` | — | **100,000 / 100,000 (100.0%)** | `0` pinned fallbacks across all 3 runs |
| `resource_retire_before_publish` CPU / put | 2.557 μs | **2.009 μs** | **-0.548 μs (-21.4%)** |
| `release_cpu_ns + finish_cpu_ns` / put | 1.345 μs | **0.948 μs** | **-0.397 μs (-29.5%)** |

- Every durable ticket across all 3 sampled Point16 runs hit the consolidated `retire_durable` fast path (`100,000 / 100,000`, `0` pinned fallbacks).
- Eliminating redundant coordinator/owner/ledger locks cut `resource_ledger` lock acquisitions per put by `37.7%` and `reservation_owner` lock acquisitions per put by `22.1%`, driving the `3.56 μs/put` (`-5.01%`) reduction in Point16 CPU/Put.

## 3. Cleanup Verification

- Verified local archive checksum (`11578d2c266ac19de3bf660858568cc9f37dc8f0b6d6b90e82b87e716f684af2`), `summary.json` (`complete: true`, `50/50`), and `remote-exit.json` (`failed: false`, `runs_started: 50`) before teardown.
- Deleted the benchmark VM, temporary SSH firewall rule, and destroyed the isolated Terraform VPC, subnet, and egress route (`Listed 0 items` for instances, firewalls, and networks).
