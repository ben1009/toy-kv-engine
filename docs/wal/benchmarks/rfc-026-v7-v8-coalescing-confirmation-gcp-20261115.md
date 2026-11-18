# RFC 026 V8 Coalescing 0 vs 400 μs Confirmation with V7 Control on GCP

**Date:** 2026-11-15 UTC

**Source commit:** 050c7daae6ef2d521ef2a031e4db0072fc25b5bf

**Source patch SHA-256:** d683ac59358c9473becd48af039159b4560d7babe85d219a3b71b794a954ebdd

**Benchmark binary SHA-256:** 857ae4469ae0daab5623a9ac20a683c16bf521122ebe16b087e7aee68d0d1819

## Finding

Ten same-host, counterbalanced repetitions confirmed that a 0 μs V8 coalescing budget can improve Point throughput over 400 μs by a modest amount. The paired median gain was 5.71%, with 9 of 10 repetitions faster. The tradeoff was substantial: 23.0% more process CPU per put and 2.02× as many Syncs.

The V7 control remained faster in every repetition. V8 at 0 μs reached 72.9% of V7's median throughput; V8 at 400 μs reached 69.2%. Coalescing budget is therefore a tuning tradeoff, not an explanation for the remaining V8 Point gap.

## GCP experiment

The experiment used one 16-vCPU C4 Local SSD host, with two Local SSDs in RAID0 and ext4 mounted rw,noatime,stripe=256. The kernel was 7.0.0-1017-gcp; the Rust toolchain was nightly-2026-09-23. No CPU affinity pinning was applied.

Each of 10 rounds ran three 16-writer Point workloads: V7 Parallel, V8 Direct FRONTIER with a 0 μs coalescing override, and V8 Direct FRONTIER with a 400 μs override. Each run performed 100,000 puts with 1 KiB values. The run order was counterbalanced across the triplets. V8 used a 4 MiB Sliding Fallocate window. The benchmark-only source patch permits the 0 μs override; it did not change production behavior.

The 30 score runs did not enable profiling, detailed Sync diagnostics, or commit-order tracing. Aggregate WAL counters were enabled, and operation latency was sampled every 100 completed operations. Each run recorded its actual V8 I/O handle metadata: all 20 V8 runs reported DATA and FRONTIER O_DIRECT=true, the Sync handle O_DIRECT=false, and the three handles on one device and inode. All 20 reported Sliding Fallocate with a 4 MiB window and no zero-initialization chunks.

The first remote setup attempt did not start any benchmark runs because the temporary orchestration command contained an incorrect commit SHA. The VM and results were preserved, the source SHA was corrected, and the benchmark-only stage was resumed on that same VM in separate work and results directories. The 30-run result set below is from the successful resumed run.

## Results

Medians across 10 runs per arm:

| WAL configuration | Throughput (k puts/s) | CPU (μs/put) | Sync count | Puts/Sync | Put p50 (ms) | Put p95 (ms) | Sampled p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| V7 Parallel | 48.16 | 59.24 | 8,261 | 12.11 | 0.301 | 0.560 | 0.803 |
| V8 Direct / 0 μs | 35.10 | 85.16 | 12,786 | 7.82 | 0.445 | 0.594 | 1.011 |
| V8 Direct / 400 μs | 33.31 | 69.25 | 6,333 | 15.79 | 0.477 | 0.621 | 0.887 |

Matched throughput comparisons, pairing by round:

| Comparison | Median paired throughput change | Faster in |
| --- | ---: | ---: |
| V8 0 μs vs V8 400 μs | +5.71% | 9/10 |
| V8 0 μs vs V7 | −26.79% | 0/10 |
| V8 400 μs vs V7 | −30.79% | 0/10 |

The V8 0 μs arm used 22.97% more CPU per put and 101.83% more Syncs than V8 400 μs in the median paired comparison. It covered about half as many puts per Sync (7.82 vs 15.79). Its median p50 and p95 were lower than the 400 μs arm, but sampled p99 varied substantially between repetitions and does not establish a stable tail-latency change.

Per-round throughput, in thousands of puts per second:

| Round | V7 | V8 0 μs | V8 400 μs | V8 0 μs vs 400 μs |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 48.04 | 32.90 | 33.18 | −0.84% |
| 2 | 48.21 | 35.35 | 33.88 | +4.34% |
| 3 | 48.38 | 35.58 | 33.61 | +5.86% |
| 4 | 47.19 | 34.77 | 33.52 | +3.74% |
| 5 | 47.83 | 35.57 | 33.33 | +6.72% |
| 6 | 47.67 | 35.03 | 33.07 | +5.93% |
| 7 | 48.60 | 35.17 | 33.31 | +5.57% |
| 8 | 48.56 | 35.50 | 33.30 | +6.61% |
| 9 | 48.85 | 34.89 | 32.92 | +5.96% |
| 10 | 48.10 | 33.73 | 32.48 | +3.84% |

## Interpretation

This confirms the direction seen in the earlier five-run sweep: removing the eligible branch's extra coalescing wait can buy a few percent of Point throughput, at the cost of roughly doubling durability advances and increasing CPU per put. The 10-run result makes that gain more credible, but does not establish that 0 μs is a good general default. It is a throughput-versus-CPU/Sync tradeoff, and this experiment covers only 16-writer Point writes.

The V7 control shows that this tuning does not close the larger gap. Even with the 0 μs override, V8 remained 26.8% slower by paired median and lost all 10 matched rounds. Do not change the production default or WAL/MVCC protocol based on this experiment alone. The next work should return to V8's fixed CPU and coordination costs rather than continue sweeping coalescing budgets.

## Result verification and retention

All 30 planned runs completed successfully. summary.json reported complete=true with 30 planned and completed runs; remote-exit.json reported failed=false and runs_started=30. The local result validator confirmed each planned run had a successful result.json and matching run-metadata.json.

The local archive and checksum sidecar were retained at /tmp/gcp-coalescing-confirmation-results-20261115/collection-attempt-04/. The local archive SHA-256 is 0e493adf0d2a1da3113d050b08f3c082d66d6f3fa030f7d80112133242791c44, matching the VM archive. The raw archive and temporary benchmark-only source patch were not added to GitHub.

The VM, restricted SSH firewall, isolated network, subnet, route, and Terraform state were left in place. No cloud cleanup or Terraform destroy was run; the VM retains its original 60-minute server-side deletion deadline.
