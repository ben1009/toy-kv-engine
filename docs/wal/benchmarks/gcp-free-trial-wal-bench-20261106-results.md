# GCP Free Trial WAL benchmark results — 2026-11-06

This captures the completed GCP runs used to check the benchmark runner changes in PR #541. The matrix outputs below retain every per-run metric and the generated plans; they omit raw command logs and VM/project identifiers.

## RFC 026 full matrix

- **Status:** 320/320 runs completed successfully.
- **Machine:** `c4-standard-16-lssd`, 16 vCPUs, two bundled Local SSDs in RAID0.
- **Matrix:** writers 1/2/4/8/16; point, batch8, batch16, batch64; V4 Parallel, V7 Parallel, V8 Parallel, V8 Leader; four counterbalanced repetitions per cell.
- **Source commit:** `bdf9a0455a91fcd1607fd1ff5cf978aa9501aeff`.
- **write-perf SHA-256:** `492734d4ea5df5f5f83650505b700f1f7fa2b9f582d54930b1e7b36a7dbbfae9`.
- **Performance note:** V8 Parallel median throughput met or exceeded V4 in 1/20 cells and V7 in 2/20 cells. This is a same-host capture, with no device-state matching, so it is exploratory and does not establish RFC performance qualification.

Every run's throughput, p99, CPU time, sync count, duration, and success status are in [the run TSV](artifacts/gcp-free-trial-wal-bench-20261106/rfc026-full-16vcpu-runs.tsv) and [the JSON summary](artifacts/gcp-free-trial-wal-bench-20261106/rfc026-full-16vcpu-summary.json). The [comparison TSV](artifacts/gcp-free-trial-wal-bench-20261106/rfc026-full-16vcpu-comparison.tsv) contains the four-run medians per writer/workload/arm and changes relative to V4. The [benchmark plan](artifacts/gcp-free-trial-wal-bench-20261106/rfc026-full-16vcpu-plan.json) records the exact schedule and parameters.

## Legacy batch64 matrix

- **Status:** 36/36 runs completed successfully on `c4-standard-8-lssd`.
- **Matrix:** writers 1, 4, 8; V4/V7/V8; two reversed six-run blocks.

All 36 measurements are in the [batch64 TSV](artifacts/gcp-free-trial-wal-bench-20261106/batch64-8vcpu-runs.tsv) and [summary JSON](artifacts/gcp-free-trial-wal-bench-20261106/batch64-8vcpu-summary.json), with its [plan](artifacts/gcp-free-trial-wal-bench-20261106/batch64-8vcpu-plan.json).

## PITR catch-up capacity check

- **Status:** 6/6 cases completed; exit code 0.
- **Configured source-spool limit:** 640 MiB.
- **Workload:** 10,000 operations per writer count, 128-byte values, writers 1/4/8.

For PITR-enabled cases, write throughput / catch-up time were:

| Writers | Writes/s | Catch-up |
|---:|---:|---:|
| 1 | 3568 | 1.257s |
| 4 | 9777 | 0.587s |
| 8 | 15651 | 0.507s |

The complete case data is in the [PITR catch-up summary](artifacts/gcp-free-trial-wal-bench-20261106/pitr-catchup-8vcpu-summary.json). These short runs verify the configured workload/catch-up path; they are not a performance qualification.
