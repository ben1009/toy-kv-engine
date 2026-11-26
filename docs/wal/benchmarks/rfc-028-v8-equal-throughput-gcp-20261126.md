# RFC 028 V8 equal-throughput (coordinated-omission-free) GCP latency comparison, 2026-11-26

## Result

Measuring end-to-end latency from scheduled request arrival timestamps (`--offered-rate-ops-per-sec`) separates saturated-queue effects at `Point8` from tail behavior at `Point16`, while **preserving the original 80-run `point_p99_vs_legacy_le_105pct = false` gate result** (the unpaced P99 difference cannot be attributed solely to queue depth):

- **Point8 @ 20,000 puts/s** (near `v8_legacy`'s ~21,390 puts/s saturation limit):
  - `v8_legacy` fell behind its arrival schedule (`e2e P50 = 50.244 ms`, `P95 = 107.632 ms`, `P99 = 111.666 ms`, schedule lag `P99 = 111.299 ms`, service `P99 = 1.343 ms`).
  - `v8_candidate_dynamic` sustained 19,999 puts/s with **`e2e P50 = 0.245 ms`, `P95 = 0.590 ms`, `P99 = 0.824 ms`** (service `P99 = 0.613 ms`).
  - `v8_candidate_target8` sustained 19,999 puts/s with **`e2e P50 = 0.250 ms`, `P95 = 0.603 ms`, `P99 = 2.393 ms`** (service `P99 = 0.597 ms`).
- **Point16 @ 28,000 puts/s** (below `v8_legacy`'s ~30,415 puts/s saturation limit):
  - `v8_legacy`: `e2e P50 = 0.644 ms`, `P95 = 1.204 ms`, `P99 = 4.149 ms` (service `P50 = 0.530 ms`, `P99 = 0.769 ms`, `lag P99 = 3.626 ms`, median outstanding tickets at sync `16.0`, coalescing wait `204.1 μs`).
  - `v8_candidate_dynamic`: `e2e P50 = 0.282 ms`, `P95 = 0.799 ms`, `P99 = 5.201 ms` (service `P50 = 0.279 ms`, `P99 = 0.822 ms`, `lag P99 = 4.786 ms`, median outstanding tickets `7.0`, coalescing wait `117.9 μs`). Dynamic still exhibited higher `e2e P99` than Legacy at 28,000 puts/s, driven primarily by `schedule_lag` rather than `service_latency`.
  - `v8_candidate_target8`: **`e2e P50 = 0.273 ms`, `P95 = 0.565 ms`, `P99 = 0.793 ms`** (~81% lower `e2e P99` than `v8_legacy` and ~85% lower than `v8_candidate_dynamic`; service `P50 = 0.270 ms`, `P99 = 0.678 ms`, `lag P99 = 0.273 ms`, median outstanding tickets `7.0`, coalescing wait `113.5 μs`, `2,949` median Target8 hits).

Note on pacing interference: `write-perf` uses `spin_loop()` during the final `250 μs` before each scheduled arrival timestamp. On 16 writers / 16 vCPUs, this spinning competes with WAL background workers for CPU; consequently, paced `cpu_us_per_put` (`~160–172 μs/Put` for Candidate, `60.9 / 109.4 μs/Put` for Legacy) does not measure WAL CPU cost, and `schedule_lag` tails may include thread-scheduling interference. Unpaced CPU/Put and throughput are reported in [rfc-028-v8-d2-qualification-gcp-20261126.md](rfc-028-v8-d2-qualification-gcp-20261126.md).

## Experiment

- Source commit (binary): `bd7ebf4bbea13444fe56c0ddbccc98156bd27244`
- Runner fix commit: `6ce1ba70` (`allow sync diagnostics in equal-throughput validator`)
- Host: GCP `c4-standard-16-lssd`, `us-central1-c`, 16 vCPUs, two 375 GiB NVMe Local SSDs (RAID0 ext4)
- Schedule: `v8_legacy`, `v8_candidate_dynamic`, `v8_candidate_target8`; 3 counterbalanced repetitions for `Point8` (`20,000 puts/s`) and `Point16` (`28,000 puts/s`): 18 runs total, 100,000 puts/run, 1-in-10 latency sampling, `--wal-sync-diagnostics` enabled.
- Archive verification: `bundle_checksum_verified=true`, `summary.complete=true` (`18/18`), `remote-exit.json` (`failed=false`, `runs_started=18`), `verify_v8_equal_throughput_results_directory` passed with zero errors. All temporary GCP resources were deleted and verified absent.

## Per-arm medians

| Workload (Offered Load) | Arm | Achieved ops/s | e2e P50 (ms) | e2e P95 (ms) | e2e P99 (ms) | Svc P99 (ms) | Lag P99 (ms) | Sync count | Puts/Sync | Max I/O QDepth | Med. Outstanding Tickets | Med. Coalesce Wait (μs) | Med. D2F Wait (μs) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point8 (20k/s) | V8 Legacy | 19,996.8 | 50.244 | 107.632 | 111.666 | 1.343 | 111.299 | 12,801 | 7.81 | 8 | 8.0 | 64.97 | 19.04 |
| Point8 (20k/s) | V8 Candidate Dynamic | 19,998.9 | 0.245 | 0.590 | 0.824 | 0.613 | 0.404 | 25,760 | 3.88 | 8 | 4.0 | 107.93 | 21.12 |
| Point8 (20k/s) | V8 Candidate Target8 | 19,999.2 | 0.250 | 0.603 | 2.393 | 0.597 | 2.148 | 25,715 | 3.89 | 8 | 4.0 | 109.16 | 21.24 |
| Point16 (28k/s) | V8 Legacy | 27,993.8 | 0.644 | 1.204 | 4.149 | 0.769 | 3.626 | 6,469 | 15.46 | 16 | 16.0 | 204.06 | 24.00 |
| Point16 (28k/s) | V8 Candidate Dynamic | 27,997.4 | 0.282 | 0.799 | 5.201 | 0.822 | 4.786 | 15,563 | 6.43 | 16 | 7.0 | 117.92 | 24.91 |
| Point16 (28k/s) | V8 Candidate Target8 | 27,994.9 | 0.273 | 0.565 | 0.793 | 0.678 | 0.273 | 17,251 | 5.80 | 15 | 7.0 | 113.55 | 24.81 |
