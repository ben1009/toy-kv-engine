# RFC 026 Full WAL Matrix Rerun on GCP — 2026-11-15

Source commit: `3dd66c157f4bba228c3d35a952fe503f5a184d09`

Binary SHA-256: `a312a6ff8920178a9429c2422885c8211fa551861236ca9d29a66ad354709c23`

## Scope and setup

This reruns the 16-writer RFC 026 matrix after the benchmark runner's TSV
line-ending fix in `bb5dbf50`. It compares V4 Parallel, V7 Parallel, V8
Parallel, and V8 Leader across Point, Batch8, Batch16, and Batch64. Each of the
16 workload/format cells ran four counterbalanced repetitions, for 64 runs.

The VM was `c4-standard-16-lssd` with two Local SSDs in RAID0, ext4 mounted
`rw,noatime,stripe=256`, and Linux `7.0.0-1017-gcp`. Each run used 1 KiB
values, no compaction, and the matrix's fixed operation counts: 100,000 Point,
300,000 Batch8, 500,000 Batch16, and 750,000 Batch64 puts. Point latency was
sampled every 100 puts; batch latency was sampled every 10 puts. `perf`
profiling and detailed WAL diagnostics were off. The runner captured a WAL
timeline every 100 ms for each run.

The runner set no V8-specific FRONTIER, coalescing-budget, or extent-preparation
overrides. These are runner-default V8 results and are not the explicit Direct
FRONTIER/400 μs/sliding-`fallocate` configuration from the separate paired
guard-check experiment. Timeline capture and latency sampling also mean these
results are exploratory, not uninstrumented qualification scores.

## Throughput results

Values are median puts/s across the four repetitions, in thousands. The final
column is the V8 Parallel change relative to V7 Parallel in this matrix.

| Workload | V4 Parallel | V7 Parallel | V8 Parallel | V8 Leader | V8 vs V7 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point | 77.60k | 46.21k | 19.92k | 3.87k | −56.9% |
| Batch8 | 220.92k | 205.40k | 140.91k | 27.97k | −31.4% |
| Batch16 | 266.03k | 257.80k | 262.52k | 51.31k | +1.8% |
| Batch64 | 623.18k | 597.77k | 599.65k | 142.27k | +0.3% |

V8 remains substantially behind V7 on Point and Batch8 in this default matrix.
Batch16 and Batch64 are close to V7; four repetitions are not enough to treat
their small differences as stable wins. These cross-version results do not
isolate the durable-guard optimization, because the suite did not build and run
the pre-change V8 as a control. The separate five-pair guard-check A/B remains
the comparison for that change and found no demonstrated throughput gain.

## Completion and artifact verification

The runner exited successfully. `summary.json` reports `complete: true` and
64/64 runs; `remote-exit.json` reports `failed: false` and 64 runs started.
The matrix validator confirmed that all planned run IDs have successful
`result.json` and matching `run-metadata.json` files in the corresponding run
directories. The transferred archive checksum matched its retained metadata:

`ccbb5509ce484cc3ba1f347fa7ae348f176d9fc74a30ce96ef16ced8ea0588d0`

The raw archive and checksum metadata remain local and were not uploaded to
GitHub. The VM and restricted SSH rule were retained as requested; no cloud or
Terraform cleanup was performed.
