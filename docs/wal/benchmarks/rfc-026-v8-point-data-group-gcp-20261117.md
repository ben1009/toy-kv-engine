# RFC 026 V8 Point DATA group join experiment on GCP, 2026-11-17

## Result

Allowing single-frame Point writes to wait briefly for already-admitted Point
tickets did not improve throughput. At 16 writers, every nonzero join budget
lost to the zero-wait baseline in all five paired runs. The largest tested
budget reduced sync count and CPU per put, but throughput fell 3.45%.

The experiment does not justify retaining a Point-specific DATA submission
wait policy. The normal V8 immediate-submit behavior for one-frame writes is
retained. Point/Batch API classification remains for the benchmark Point-only
extent preparation policy; it does not change DATA grouping.

## Setup

- GCP `c4-standard-16-lssd`, 16 vCPUs, two Local SSDs in RAID0, ext4.
- V8 Parallel, 1 KiB Point puts, 100,000 puts per run, no compaction.
- All Point arms used 1 MiB Sliding Point Lookahead.
- Frontier mode was the default Buffered mode, and coalescing used the
  default Dynamic 100/400 μs policy; neither had an override.
- The only Point-arm change was the benchmark-only join budget: 0, 5, 10 or
  20 μs. Each writer-count/budget combination ran five times in counterbalanced
  order.
- Writer counts were 1, 4, 8 and 16: 80 Point score runs.
- Batch16 and Batch64 ran five paired guardrail repetitions at 16 writers:
  20 additional score runs. The Batch path remained Fallocate-only.
- No broad profiler, eBPF or perf collection was enabled. Latency percentiles
  use the benchmark's sampled operations (one observation per 100 puts for
  Point runs); aggregate DATA-group counters were enabled to compare grouping.
  The p99 is directional because each run contributes about 1,000 samples.

The binary was built from source commit
`00517de1336163bdfb2e2f32e4caa0dd08a3cb32`, SHA-256
`14b3ee4bd6f63011e6e9c972f2141f6c7197cf00808b1e53b67d80617c5cf203`. The
commit patch checksum (`git show --format= --binary <commit> | sha256sum`) is
`81174b4ddb235dff98b54af03e3a4b251efc0e204ec17517bfbb611addefbbdd`.

## Point throughput

Throughput is the median across five runs. Paired change is the median of the
five repetition-matched percentage changes against the 0 μs arm.

| Writers | Join budget | Throughput | Paired change | Wins | CPU/put |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0 μs | 6.76k/s | — | — | 63.70 μs |
| 1 | 5 μs | 6.76k/s | +1.00% | 4/5 | 63.69 μs |
| 1 | 10 μs | 6.64k/s | −1.69% | 2/5 | 64.12 μs |
| 1 | 20 μs | 6.73k/s | −2.41% | 1/5 | 64.24 μs |
| 4 | 0 μs | 17.17k/s | — | — | 45.41 μs |
| 4 | 5 μs | 17.59k/s | +2.40% | 4/5 | 51.73 μs |
| 4 | 10 μs | 17.50k/s | +1.96% | 4/5 | 51.23 μs |
| 4 | 20 μs | 17.10k/s | −0.70% | 2/5 | 51.65 μs |
| 8 | 0 μs | 28.47k/s | — | — | 53.95 μs |
| 8 | 5 μs | 27.91k/s | −0.23% | 2/5 | 56.10 μs |
| 8 | 10 μs | 28.14k/s | −0.20% | 1/5 | 55.45 μs |
| 8 | 20 μs | 28.02k/s | −0.85% | 2/5 | 55.48 μs |
| 16 | 0 μs | 38.29k/s | — | — | 80.19 μs |
| 16 | 5 μs | 37.52k/s | −1.88% | 0/5 | 79.06 μs |
| 16 | 10 μs | 37.30k/s | −2.47% | 0/5 | 78.87 μs |
| 16 | 20 μs | 36.96k/s | −3.45% | 0/5 | 76.51 μs |

At 16 writers, average DATA tickets per submitted group rose from 1.07 at 0 μs
to 1.57, 1.66 and 1.75. Sync count fell from 9,729 to 9,617, 9,396 and 9,158;
puts per sync rose from 10.28 to 10.40, 10.64 and 10.92. These grouping
changes did not translate into higher throughput.

Median sampled Point latency at 16 writers was:

| Join budget | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: |
| 0 μs | 0.367 ms | 0.921 ms | 3.046 ms |
| 5 μs | 0.376 ms | 0.862 ms | 1.583 ms |
| 10 μs | 0.375 ms | 0.885 ms | 1.820 ms |
| 20 μs | 0.387 ms | 0.889 ms | 1.817 ms |

The sampled p99 is lower for the nonzero budgets in this run, but the result is
not enough to justify the throughput loss or to claim a stable tail-latency
improvement.

## Batch guardrails

Both arms use the same Batch path; the Point join setting must not affect Batch
requests.

| Workload | 0 μs arm | 20 μs arm | Paired change | Wins |
| --- | ---: | ---: | ---: | ---: |
| Batch16 | 372.38k/s | 369.44k/s | −0.98% | 1/5 |
| Batch64 | 620.66k/s | 620.43k/s | +0.02% | 3/5 |

There is no material Batch change, as expected. These are regression guardrails,
not a mixed Point/Batch concurrency test.

## Validation and retained results

All 100 planned runs completed successfully. `summary.json` reports
`complete: true` and 100/100 runs; `remote-exit.json` reports
`failed: false` and 100 runs started. Local verification found a successful
`result.json` and matching `run-metadata.json` for every planned run ID.

The downloaded archive remains local and was not added to Git. Its SHA-256
`8c08c393e0117809247940d957dda37b863db985a69f947375a1438f3c70ebdb` matches
both the remote and local bundle metadata. The VM, boot disk, temporary SSH
rule, isolated VPC, subnet and route were checked absent after cleanup; the
temporary Terraform state is retained outside the repository.

## Decision

Reject the Point DATA join wait as a throughput optimization. It can enlarge
DATA submission groups slightly, but at 16 writers it reduces throughput for
every tested nonzero budget. Keep Point/Batch classification for extent
preparation and continue optimizing the Point path without adding this DATA
submission delay. Batch scheduling remains unchanged.
