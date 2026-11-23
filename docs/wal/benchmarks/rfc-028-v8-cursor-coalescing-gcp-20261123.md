# RFC 028 — V8 Cursor Coalescing Diagnostic

- Source commit: `b5e93d5d761f935489deda35fc625bdfca926ebc`
- Source archive SHA-256: `cdab061698b7b8aae6fcbcd4d2db35a3054cf36f0958c41a0adc1df67f575258`
- Benchmark binary SHA-256: `3a9121b4216b037e3af2dbc7c4e9dd98401971e8ccc97da4ce8a7c048e70f86e`
- Machine: GCP C4 16 vCPU + 2 Local SSDs, ext4 RAID0
- Workload: 100,000 Point puts, 1 KiB values; Cursor mode; 128-advance phase sampling
- Configurations: 8/16 writers × Relay/Direct Completion; Singleton Submission disabled
- Runs: 4 planned, 4 completed, 4 validated; one diagnostic run per configuration
- This is instrumented diagnostic data, not an unprofiled throughput qualification.

## Run results

| Writers | Completion | Puts/s (diagnostic) | CPU/Put (μs) | Syncs | Puts/Sync | Phase samples |
|---:|---|---:|---:|---:|---:|---:|
| 8 | Relay | 30,447 | 56.38 | 13,029 | 7.68 | 101 |
| 8 | Direct | 32,308 | 52.38 | 12,949 | 7.72 | 101 |
| 16 | Relay | 43,064 | 77.78 | 9,900 | 10.10 | 77 |
| 16 | Direct | 44,004 | 72.80 | 10,027 | 9.97 | 78 |

## Durability phase p50 / p95 / p99

Wall values are μs except CPU fields are thread CPU μs; each configuration has only one run. Percentiles are per sampled advance and must not be summed as an end-to-end cycle.

| Writers / completion | Coalesce wall p50/p95/p99 | Progress wait wall p50/p95/p99 | Progress wait CPU p50/p95/p99 | Completion drain wall p50 | Coordinator wait / hold p50 | Digest fold CPU p50 | Cursor advance CPU p50 | Tickets gained / covered p50 | fdatasync wall p50/p95/p99 | Driver wall p50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 / Relay | 70.12/125.72/134.62 | 57.63/112.10/117.62 | 20.80/52.64/61.51 | 1.24 | 1.03 / 4.15 | 0.51 | 0.57 | 4 / 8 | 1.12/111.28/140.09 | 173.13 |
| 8 / Direct | 73.93/133.06/173.22 | 52.85/114.30/165.47 | 21.62/49.56/59.95 | 1.29 | 1.00 / 4.97 | 0.86 | 0.63 | 4 / 8 | 1.09/102.58/131.75 | 167.59 |
| 16 / Relay | 114.89/176.26/212.97 | 94.29/157.15/188.50 | 28.97/52.38/57.32 | 1.40 | 1.21 / 8.15 | 2.50 | 0.76 | 6 / 11 | 1.64/155.37/164.09 | 230.68 |
| 16 / Direct | 110.48/170.09/176.11 | 89.02/152.75/169.83 | 29.22/57.24/75.64 | 1.41 | 1.58 / 9.53 | 2.82 | 0.70 | 6 / 10 | 1.63/122.75/175.89 | 224.84 |

## Reading the result

- Progress-token wait dominates the sampled Coalescing wall time: p50 is about 53–94 μs across these runs, while completion drain is about 1.2–1.4 μs and coordinator lock wait is about 1.0–1.6 μs. This points toward waiting for progress rather than an expensive completion-apply or cursor-advance operation.
- The wait also consumes measurable thread CPU (about 21–29 μs p50), so it is not pure sleeping. Completion apply, Digest Fold, and Cursor Advance each have low single-digit μs CPU p50.
- `fdatasync` p50 remains about 1–2 μs, with a long tail (roughly 103–155 μs p95). Frontier write p50 is about 33–36 μs. Do not add stage percentiles to infer cycle time.
- Direct Completion changed CPU/Put and diagnostic throughput in these single runs, but Sync count and Puts/Sync stayed close. One run per cell is not enough to claim a performance win.
- The output field `coalescing_wait_breakdown_available` is false in a subset of Cursor samples even though all detailed phase fields are populated. This appears to be a reporting-flag inconsistency; the numeric optional fields are retained and were used for these summaries. Fix the flag before relying on it as a validator.

## Memory and cleanup

- Guest before test: 58 GiB RAM total, approximately 57 GiB available; no swap; Local SSD had 737 GiB free.
- Guest after test: 58 GiB total, approximately 57 GiB available; no swap; Local SSD had about 736 GiB available.
- Result archive SHA-256 verified locally; `summary.json` is complete (4/4), `remote-exit.json` reports no failed benchmark process (4 started/4 completed), and each planned run has `result.json` plus `run-metadata.json`.
- The verified raw archive is retained locally at `bench-results/gcp-v8-cursor-coalescing-20261123/rfc028-cursor-coalescing-results.tar.gz` (SHA-256: `8f4e6a8f1b3e9ce8b9bd80a98ca27cad86436b9d90b9b8e3db1e4cb0eafcfd31`); raw results are not committed.
- Cleanup was verified: the temporary VM, boot disk, SSH firewall rule, subnet, and isolated VPC are absent. Terraform state and the temporary SSH key were removed.
