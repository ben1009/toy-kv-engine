# RFC 028 V8 Cursor Publication D2a/D2b GCP A/B

Date: 2026-11-24
Source: `9bdb5fa7a89730c6d0be5df6bcf05246e67176ab`
Release binary SHA-256: `c886c769d011e6bfb70159daa963e17b18b52e4e2ee8272e40a45d2333d2a4c2`

## Setup

- One `c4-standard-16-lssd` host (16 vCPUs), with two NVMe Local SSDs in RAID0.
- DATA and FRONTIER used `O_DIRECT`; the sync descriptor was buffered, as confirmed by the per-run WAL I/O metadata.
- Point workloads: 1, 8, and 16 writers; 100,000 puts per run; 1 KiB values; five repetitions per arm.
- Batch64 guardrail: 16 writers; five repetitions per arm.
- Four arms, interleaved on the same host and release binary:
  - `baseline`: D2a off, D2b off.
  - `admission_only`: D2a admission cursor publication fast path on.
  - `cursor_dedup_only`: D2b cursor publication dedup on.
  - `admission_and_cursor_dedup`: both on.
- Cursor + Direct Completion, Direct FRONTIER, dynamic coalescing, 1 MiB sliding Point Lookahead, Stage A Point buffer preallocation on, retirement mode 0, singleton submission off. Batch64 did not enter Point preallocation.
- No CPU attribution, durability sampling, sync diagnostics, or detailed tracing. Point latency was sampled every 10 operations; p99 is based on those samples. CPU/Put is process CPU divided by completed puts.
- Total: 80 runs (60 Point, 20 Batch64).

## Point results

Absolute columns are per-arm medians across five runs. Paired throughput and p99 changes are the medians of the five within-repetition changes, so they need not equal a ratio of the absolute medians.

| Writers | Arm | Throughput (k puts/s) | Paired throughput change (wins) | CPU (μs/put) | Puts/sync | p99 (ms) | Paired p99 change |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | Baseline | 8.015 | — | 52.62 | 1.00 | 0.646 | — |
| 1 | D2a | 8.128 | +0.97% (3/5) | 52.85 | 1.00 | 0.545 | −11.06% |
| 1 | D2b | 8.263 | +2.98% (4/5) | 52.84 | 1.00 | 0.513 | −20.63% |
| 1 | D2a + D2b | 8.380 | +3.80% (5/5) | 52.23 | 1.00 | 0.465 | −24.27% |
| 8 | Baseline | 33.028 | — | 47.17 | 7.75 | 0.757 | — |
| 8 | D2a | 33.541 | +1.18% (4/5) | 46.99 | 7.76 | 0.776 | −0.15% |
| 8 | D2b | 33.064 | −1.16% (2/5) | 51.02 | 7.76 | 0.771 | +0.31% |
| 8 | D2a + D2b | 32.257 | −2.33% (1/5) | 50.28 | 7.78 | 0.785 | +5.65% |
| 16 | Baseline | 45.780 | — | 67.95 | 11.19 | 0.923 | — |
| 16 | D2a | 46.423 | +1.73% (5/5) | 68.20 | 11.65 | 0.921 | −0.26% |
| 16 | D2b | 43.584 | −5.39% (0/5) | 72.65 | 11.54 | 0.964 | +4.88% |
| 16 | D2a + D2b | 44.156 | −3.55% (0/5) | 73.03 | 11.80 | 0.984 | +7.53% |

## Batch64 guardrail

Baseline median throughput was 630.66k puts/s and CPU was 4.89 μs/put. Paired throughput changes versus baseline were:

| Arm | Throughput (k puts/s) | Paired throughput change (wins) | CPU (μs/put) |
|---|---:|---:|---:|
| D2a | 633.60 | +0.26% (3/5) | 4.84 |
| D2b | 631.46 | +0.38% (3/5) | 4.92 |
| D2a + D2b | 634.71 | +0.17% (4/5) | 4.93 |

Batch64 showed no material throughput regression in this run set.

## Conclusion

Neither D2 change passed the 5% Point8 and Point16 throughput gate.

D2a alone was modestly positive at Point8 (+1.18%, 4/5 wins) and Point16 (+1.73%, 5/5 wins), with no measured p99 regression at those writer counts. The effect is below the retention threshold and CPU/Put was nearly unchanged. This does not qualify D2a for default enablement.

D2b alone regressed Point16 by 5.39% (0/5 wins) and increased CPU/Put by 4.78 μs. Combining D2a and D2b also regressed Point8 and Point16, with Point p99 worsening by 5.65% and 7.53%, respectively. Do not enable D2b or the combined path by default; the current results argue against further D2b tuning without a new, specific hypothesis.

No production default was changed. These results are a same-host factorial comparison of the two bench-only switches, not a V7 comparison or a cross-host qualification.

## Verification and cleanup

- `summary.json`: `complete=true`, 80 planned / 80 started / 80 attempted / 80 completed.
- `remote-exit.json`: `failed=false`, 80 started and attempted.
- The strict cursor-publication verifier found all 80 scheduled run directories, successful `result.json` files, matching `run-metadata.json`, runtime flags, and raw metrics; result: 80/80 verified with no errors.
- Archive: 580 files, 88,999 bytes, retained locally at `bench-results/gcp-v8-cursor-publication-d2-20261124-b/collection-attempt-01/results.tar.gz`. Local and remote SHA-256 match: `218674138490a9f4fd72e92942ef273dd90cec616c41387e30f188845b2d0243`. The raw archive and checksum metadata are not committed.
- A first capacity attempt in another zone stopped before VM creation; the completed 80-run matrix was rerun in a zone with available C4 capacity.
- Cleanup completed after archive verification. The benchmark VM and temporary SSH firewall were absent, and the isolated VPC, subnet, route, and Terraform-managed resources were destroyed and verified absent. Temporary Terraform state and run logs were removed.
