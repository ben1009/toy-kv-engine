# GCP V8 sliding extent preparation validation

## Run

- Source commit: `233ec1c554e01de9837641021745739611326320`.
- Benchmark: `rfc026-full`, 16 writers, 64/64 runs completed, four repetitions per workload/arm; all run exit codes were zero.
- VM: `c4-standard-16-lssd` in `us-east1-b`; maximum lifetime 60 minutes.
- Benchmark-only setting: `TOYKV_BENCH_V8_EXTENT_PREP=sliding-fallocate`, window 1,048,576 bytes, applied to V8 Parallel runs only. Per-run timeline capture was off.
- Temporary remote-runner patch SHA-256: `8c2b89fc8864e3529262c02b58223711180d3a8a3445965f1eeaee6cc7a3db63`. No Rust source patch was used.
- No broad profiling or timeline tracing; sampled latency (`--latency-sample-every 10`) and aggregate WAL counters were enabled.

## Throughput and accompanying medians

Point writes are 100,000 operations per run. Latency p99 is the median of the four run-level sampled p99 values; it is not an endpoint-wide percentile.

| Arm | Throughput | Sampled p99 | Process CPU | WAL syncs |
| --- | ---: | ---: | ---: | ---: |
| V4 Parallel | 82.02k puts/s | 0.630 ms | 6.357 s | 26,548.5 |
| V7 Parallel | 48.71k puts/s | 0.876 ms | 6.023 s | 8,291.5 |
| V8 Parallel + sliding fallocate | 32.31k puts/s | 2.151 ms | 7.074 s | 6,331.0 |
| V8 Leader | 4.08k puts/s | 25.503 ms | 11.993 s | 100,000.0 |

V8 Parallel Point throughput was 66.3% of the same-run V7 result. Compared with the earlier unprepared V8 Point result of 21.32k puts/s, this is approximately 51.6% higher; that earlier measurement was not rerun as a matched arm here.

Batch throughput (same GCP host; V8 Parallel has sliding fallocate enabled):

| Workload | V7 Parallel | V8 Parallel | V8 vs V7 |
| --- | ---: | ---: | ---: |
| batch8 | 212.80k ops/s | 215.06k ops/s | +1.1% |
| batch16 | 261.71k ops/s | 367.75k ops/s | +40.5% |
| batch64 | 614.04k ops/s | 619.36k ops/s | +0.9% |

## I/O evidence that preparation ran

The copied V8 Parallel Batch16 run `result.json` reports `sliding-fallocate` with a 1 MiB window, 619 actual `fallocate` calls, 649,068,544 allocated bytes, 22.18 ms aggregate allocation time, zero initialization chunks, and 0 preparation waits. DATA used `O_DIRECT`; FRONTIER and Sync handles were buffered. All three handles resolved to the same device/inode and `wal_io_stable` was true.

## Result and limits

Sliding fallocate is the right change for the Point bottleneck: the current result materially exceeds the earlier unprepared V8 Point baseline while keeping V8 Batch8/16/64 at or above V7. V8 Point still trails V7 by about one third, and the sampled Point p99 remains higher. These results support the extent-preparation change; they do not qualify Point latency parity with V7.

The remote benchmark completed all 64 runs, and the complete `summary.json` was copied locally and validated. The runner’s recursive result transfer timed out twice after 240 seconds, so `artifact-collection.json` records the transfer as incomplete and some per-run JSON files are missing. The reported medians come from the complete summary; the I/O-preparation fields above come from one copied representative run, not every Point run.

## Cleanup

The VM, boot disk, temporary SSH firewall rule, isolated VPC, subnet, and associated routes were verified absent after teardown. Terraform state is empty. Preflight estimated maximum spend at $1.25 against the authorized $2 ceiling; the final billing record was not available during cleanup. No billing, API, or quota settings were changed.

The benchmark did not modify runtime source; this file records its validation results.
