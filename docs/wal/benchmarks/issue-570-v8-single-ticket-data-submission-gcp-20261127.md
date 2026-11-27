# Issue #570 V8 Ordered Single-Frame DATA Submission GCP A/B

- **Date**: 2026-11-27
- **Issue / PR**: [#570](https://github.com/ben1009/toy-kv-engine/issues/570) / PR [#580](https://github.com/ben1009/toy-kv-engine/pull/580)
- **Baseline commit**: `4a96c1b6bcd08ff91471c5fa76472837134501ce` (`main`, post-#579)
- **Candidate commit**: `835942906431a4f1883361d92845969a56f954b1` (`perf/issue-570-single-ticket-data-submission`)
- **Patch SHA-256** (5 tracked A/B diff files): `8f7b57d20c9bb2ca9a1e5964d2b5b7400776195db6f711e51f1eb0416025cf59`
- **Environment**: `c4-standard-16-lssd` (`us-central1-c`), 16 vCPUs, 2× NVMe Local SSD RAID0 (`/mnt/gcp-wal-bench`, `ext4`), `write-perf --wal --pitr --pitr-format v8 --wal-io-mode parallel --value-size 1024 --compaction none --latency-sample-every 100` (`TOYKV_BENCH_V8_EXTENT_PREP=sliding-point-lookahead`, `TOYKV_BENCH_V8_EXTENT_PREP_BYTES=1048576`, `TOYKV_WAL_V8_FRONTIER_MODE=direct`, `TOYKV_BENCH_V8_POINT_DIRECT_FRAME=0`)
- **Local archive**: `bench-results/gcp-wal-20261127T080829Z/collected/results` (`sha256: 208fb9e7854dc9d44ad6ba2368cbd96a9dac2064ba4655e8c971888b9a428d69`, 50/50 runs verified)

## 1. 50-Run Counterbalanced A/B Results (5 Pairs per Workload, Unprofiled)

| Workload | Baseline Median | Candidate Median | Paired Median Delta (ops/s · CPU/put · p99) | Wins (C vs B: ops/s · CPU · p99) |
| --- | ---: | ---: | ---: | ---: |
| **Point1** (100k puts, 1t) | 8,603 puts/s · 51.38 μs/put · 0.530 ms p99 | **8,682 puts/s** · **51.20 μs/put** · 0.561 ms p99 | **+1.58%** · -0.39% · -0.20% | **4/5** · 3/5 · 3/5 |
| **Point8** (100k puts, 8t) | 37,022 puts/s · 43.01 μs/put · 0.799 ms p99 | **37,503 puts/s** · **42.55 μs/put** · **0.750 ms p99** | **+1.68% · -1.58% · -8.06%** | **5/5** · **4/5** · **5/5** |
| **Point16** (100k puts, 16t) | 54,004 puts/s · 64.84 μs/put · 1.098 ms p99 | 54,187 puts/s · 64.56 μs/put · **0.893 ms p99** | -0.29% · -0.58% · **-30.22%** | 2/5 · **4/5** · 3/5 |
| **Batch16/w16** (100k puts, 16t) | 416,089 puts/s · 7.88 μs/put · 1.082 ms p99 | 404,208 puts/s · 7.85 μs/put · 1.142 ms p99 | -2.86% · -0.42% · +0.97% | 2/5 · 3/5 · 2/5 |
| **Batch64/w16** (100k puts, 16t) | 633,978 puts/s · 4.58 μs/put · 2.536 ms p99 | 631,050 puts/s · 4.57 μs/put · 2.498 ms p99 | -0.22% · -0.32% · +1.27% | 2/5 · 3/5 · 2/5 |

- **Point8**: Consistent improvement across all metrics: throughput **+1.68%** (`5/5` wins, `37,022 → 37,503 puts/s`), CPU/Put **-1.58%** (`4/5` wins, `43.01 → 42.55 μs/put`), and p99 **-8.06%** (`5/5` wins, `0.799 → 0.750 ms`).
- **Point1**: Throughput improved **+1.58%** (`4/5` wins, `8,603 → 8,682 puts/s`) with slightly lower CPU/Put (`-0.39%`).
- **Point16**: CPU/Put improved **-0.58%** (`4/5` wins, `64.84 → 64.56 μs/put`) and p99 improved **-30.22%** (`1.098 → 0.893 ms`), while paired throughput was essentially flat (`-0.29%`, `2/5` wins, `54,004 → 54,187 puts/s`).
- **Batch guardrails**: `Batch64/w16` was flat (`-0.22%` ops/s, `-0.32%` CPU/Put); `Batch16/w16` showed `-2.86%` paired median ops/s (`2/5` wins) with flat CPU/Put (`-0.42%`, `3/5` wins).

## 2. Point16 CPU Attribution & Durability Diagnostics (3 Counterbalanced Pairs, `--wal-cpu-attribution-sample-every 64 --wal-v8-durability-sample-every 64 --mutex-lock-sample-every 64`)

> [!NOTE]
> Diagnostic runs only (`bench-results/gcp-wal-20261127T080829Z/diagnostics`); not scored as unprofiled throughput.
> Note that the remote A/B runner builds both arms with the baseline `write-perf` harness, so `ordered_single_frame_submit_count` was not emitted in the remote JSONs.

| Metric (Median of 3 Paired Runs) | Baseline (`4a96c1b6`) | Candidate (`83594290`) | Delta |
| --- | ---: | ---: | ---: |
| `frame_finish_cpu_ns` | 7,852.0 ns | 7,873.0 ns | +0.27% (+21.0 ns) |
| `v8_frames_ready_submit_cpu_ns` | 3,013.0 ns | **2,993.0 ns** | **-0.66% (-20.0 ns)** |
| `producer_total_cpu_ns` | 21,705.0 ns | 21,721.5 ns | +0.08% (+16.5 ns) |
| Process CPU / put (diagnostic run) | 67.01 μs | 67.37 μs | +0.55% |

## 3. Candidate-Only Fast Path Hit Rate, Fallback Reasons, and DATA Group Size Distribution (`d7dc9918`)

> [!NOTE]
> Diagnostic runs only (`bench-results/gcp-wal-20261127T084357Z/diagnostics`, 2 repetitions per workload, built with `--features bench`).

### Fast Path Hit Rate & Mutually Exclusive Fallback Reasons

| Workload (Rep 1 / Rep 2) | Total `publish_data` | Fast Path Hit | `out_of_order` | `pending_nonempty` | `frontier_gap` | `packing` | `multi_frame` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Point1** (1t) | 100,000 / 100,000 | **0.00% / 0.00%** (1 / 1) | 0.00% / 0.00% | 0.00% / 0.00% | **100.00% / 100.00%** (99,999 / 99,999) | 0.00% | 0.00% |
| **Point8** (8t) | 100,000 / 100,000 | **80.00% / 79.29%** (80,001 / 79,289) | 4.03% / 4.40% | 3.34% / 3.69% | 12.63% / 12.62% | 0.00% | 0.00% |
| **Point16** (16t) | 100,000 / 100,000 | **76.08% / 75.38%** (76,080 / 75,383) | 7.82% / 8.18% | 5.90% / 6.22% | 10.20% / 10.22% | 0.00% | 0.00% |
| **Batch16** (16t) | 6,256 / 6,256 | **0.00% / 0.00%** (0 / 0) | **54.09% / 53.88%** (3,384 / 3,371) | 3.74% / 3.40% (234 / 213) | 0.00% | 0.00% | **42.17% / 42.71%** (2,638 / 2,672) |

### DATA `WriteGroup` Size Distribution (Tickets per Group)

| Workload (Rep 1 / Rep 2) | Total Groups | Size 1 | Size 2 | Size 3–4 | Size 5–8 | Size 9+ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Point1** (1t) | 100,000 / 100,000 | **100.00% / 100.00%** | 0.00% | 0.00% | 0.00% | 0.00% |
| **Point8** (8t) | 95,973 / 95,603 | **96.96% / 96.64%** | 2.23% / 2.50% | 0.73% / 0.77% | 0.08% / 0.09% | 0.00% |
| **Point16** (16t) | 92,202 / 91,841 | **94.29% / 94.02%** | 4.07% / 4.22% | 1.39% / 1.49% | 0.25% / 0.27% | 0.00% |
| **Batch16** (16t) | 2,875 / 2,885 | 24.97% / 23.99% | **45.57% / 46.97%** | 26.85% / 26.97% | 2.61% / 2.08% | 0.00% |

- **Interpretation**:
  - **Point1** hits `fallback_frontier_gap_count` on `99,999 / 100,000` calls because a single writer synchronously emits a FRONTIER frame after each put, leaving a 1-frame physical offset gap before the next DATA frame.
  - **Point16** already achieves a **75.38%–76.08%** fast-path hit rate (and **94.02%–94.29%** of groups are size 1), while **Point8** achieves **79.29%–80.00%**. Because Point16 hit rate is already high yet 50-run CPU/Put and 3-pair CPU attribution (`-0.66%` / `-20 ns` on `v8_frames_ready_submit_cpu_ns`) show minimal net CPU reduction, bypassing `pending` + `notify_all` + `Vec` on ordered single-frame submission yields only marginal (~20 ns/put) CPU savings at 16 writers.
  - **Batch16** never enters the fast path (`0.00%`).

## 4. Cleanup Verification

- Verified local archive checksum (`208fb9e7854dc9d44ad6ba2368cbd96a9dac2064ba4655e8c971888b9a428d69`), `summary.json` (`complete: true`, `50/50`), `remote-exit.json` (`failed: false`, `runs_started: 50`), and all 8 diagnostic JSON files before teardown.
- Deleted the benchmark VM (`walbench-261127-080944-79d26db2`), temporary SSH firewall rule (`walbench-ssh-79d26db2`), and destroyed the isolated Terraform VPC (`walbench-isolated-vpc`), subnet (`walbench-isolated-subnet`), and egress route (`walbench-isolated-egress`), verifying `Listed 0 items.` across instances, disks, walbench firewalls, and walbench networks.
