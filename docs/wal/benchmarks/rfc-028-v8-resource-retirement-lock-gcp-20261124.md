# V8/V7 Resource Retirement Ledger Lock A/B — 2026-11-24

## Result

The merged Ledger lock-scope change did not produce a clear throughput win. Candidate Point throughput improved by a paired median **2.20% at 1 writer, 1.71% at 8 writers, and 0.36% at 16 writers**, with 3/5 paired wins in each cell. CPU per operation was effectively unchanged, and Point16 sync count and Puts/Sync were identical. The measurement does not establish a performance reason to retain the change; it leaves the code-review decision separate from the performance result. No runtime source was changed by this benchmark.

## Experiment

- Source commit: `96aa2927ebec8f95c401f98a786af0199417197a` (`PR #562`, branch `perf/v8-point-progress-ring-f05-qualification`).
- Baseline binary: same source commit with only `finish_batch_operation()` restored to its pre-`e391a5db` body.
- Candidate binary: unmodified source at the source commit, including the merged Ledger lock-scope change.
- Baseline patch SHA-256: `34374c31851fe6d51fba3f3637e2623481d145fbd3c0d8f08dfa095d0250942c`.
- Baseline binary SHA-256: `465529b6c4b98ee6aae18d7a36ac55f15c514ce94783295ffe8302629ab1f75c`.
- Candidate binary SHA-256: `4957352db9a212b64920c4e91b00293209adf82f722f8b95039b4dc0352647d6`.
- One C4 Local SSD VM, five counterbalanced baseline/candidate pairs per cell: V8 Point1, Point8, Point16; V7 Point16 sentinel; V8 Batch64 guardrail.
- Each run: 100,000 operations, 1 KiB value, release `write-perf` with `bench` feature; no CPU attribution, mutex sampling, perf, or WAL diagnostics. Point latency sampled 1-in-10 (10,000 samples/run).
- V8 settings held fixed: Cursor progress mode, Direct Completion, Direct FRONTIER, 1 MiB sliding Point Lookahead, Dynamic coalescing, singleton submission off, Point buffer preallocation on. Every V8 Point run recorded 100,000/100,000 preallocation hits; Batch64 recorded zero. DATA and FRONTIER were O_DIRECT; sync handle was buffered, and DATA/FRONTIER file identities remained stable during each run.
- Benchmark storage: two 375 GiB NVMe Local SSDs in RAID0, ext4, 749.7 GiB mounted at `/mnt/gcp-wal-bench`; Ubuntu 24.04, Linux `7.0.0-1017-gcp`, Rust `nightly-2026-09-23`; C4 `c4-standard-16-lssd`, 16 vCPU Intel Xeon 6985P-C.
- Temporary SSH access was restricted to the current public `/32` in an isolated VPC. Billing/APIs/quotas were not changed. VM deadline was 60 minutes and estimate was $1.25, below the $2 ceiling.

## Results

The first value in each metric cell is the baseline median; the second is the candidate median. The paired change is the median of five within-repetition candidate/baseline ratios, so it need not equal the ratio of the two medians.

| Workload | Throughput ops/s (base / candidate) | Paired throughput change; wins | CPU μs/op (base / candidate) | Syncs (base / candidate) | Puts/Sync (base / candidate) | p50 ms (base / candidate) | p95 ms (base / candidate) | p99 ms (base / candidate) | Paired p99 change |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V8 Point1 | 8,218.77 / 8,499.64 | +2.20%; 3/5 | 53.63 / 53.64 | 100,000 / 100,000 | 1.00 / 1.00 | 0.108 / 0.108 | 0.153 / 0.131 | 0.552 / 0.525 | +7.27% |
| V8 Point8 | 33,628.92 / 34,052.76 | +1.71%; 3/5 | 48.43 / 48.19 | 12,902 / 12,899 | 7.75 / 7.75 | 0.207 / 0.207 | 0.415 / 0.401 | 0.770 / 0.711 | -6.91% |
| V8 Point16 | 45,576.73 / 45,783.98 | +0.36%; 3/5 | 69.62 / 69.50 | 8,839 / 8,839 | 11.31 / 11.31 | 0.309 / 0.308 | 0.630 / 0.622 | 0.932 / 0.920 | -1.63% |
| V7 Point16 | 48,238.08 / 48,041.25 | -0.86%; 1/5 | 61.30 / 61.62 | 8,313 / 8,374 | 12.03 / 11.94 | 0.298 / 0.300 | 0.541 / 0.538 | 0.769 / 0.768 | +1.17% |
| V8 Batch64 | 626,852.94 / 629,127.51 | +0.29%; 5/5 | 4.91 / 4.94 | 196 / 193 | 510.20 / 518.13 | — / — | — / — | — / — | — |

### Paired throughput deltas

Each vector is ordered by repetition 1–5 (candidate vs. baseline).

| Workload | Paired throughput changes |
|---|---|
| V8 Point1 | +2.20%, -0.92%, +4.05%, +5.56%, -1.08% |
| V8 Point8 | +4.03%, -1.55%, +1.71%, -3.61%, +3.01% |
| V8 Point16 | +0.57%, +0.36%, -0.86%, -0.70%, +0.60% |
| V7 Point16 | -0.62%, -1.21%, -1.41%, -0.86%, +0.53% |
| V8 Batch64 | +1.07%, +0.13%, +0.07%, +0.29%, +1.22% |

### Interpretation

- V8 Point1: +2.20% paired median, 3/5 wins; CPU/op is unchanged. The paired sampled p99 median change is +7.27%; the 1-in-10 tail samples are noisy and do not show a repeatable direction.
- V8 Point8: +1.71%, 3/5 wins; CPU/op −0.25 μs. Sync count and Puts/Sync are effectively unchanged. Paired sampled p99 median is −6.91%.
- V8 Point16: +0.36%, 3/5 wins; CPU/op −0.12 μs. Sync count and Puts/Sync are exactly equal at the medians (8,839 syncs; 11.31 puts/sync). Paired sampled p99 median is −1.63%.
- V7 Point16 sentinel: −0.86%, 1/5 wins; the shared-path change did not show a V7 benefit.
- V8 Batch64: +0.29%, 5/5 wins; CPU/op changed by +0.03 μs. No material Batch regression was observed.
- Lock wait/acquisition counters were not enabled in these unprofiled scoring runs, to avoid perturbing the primary throughput result. This A/B measures user-visible throughput/CPU/latency and sync behavior, not lock-wait attribution.

## Verification and cleanup

- Remote suite: `summary.json` reports `complete: true`, 50/50 runs; `remote-exit.json` reports `failed: false`, `runs_started: 50`.
- The custom experiment-plan verifier passed all 50 run IDs, result/metadata pairs, binary identities, runtime flags, preallocation counters, metric schemas, and summary TSV rows.
- Both downloaded tar archives matched their remote and local SHA-256 metadata:
  - Attempt 1: `2d65dd00b7f03fd604f9b6e1d0c2780713f72c70ba3aa71da41f2011099ec504` (63900 bytes).
  - Attempt 2: `b613750daa4808275c0fc2bceb1c81f55ca29a641017aa6ed95b89b7881ecbdd` (63900 bytes).
- The stock outer runner returned nonzero because the suite name reused its built-in 55-run preallocation validator, which rejected this valid custom 50-run plan. The run itself completed; the experiment-specific verifier passed. The raw archive omitted `retirement-baseline.patch` due to the stock archive allowlist, so the extracted verification directory was supplemented with the locally retained patch whose hash matches the archived `source-patch.sha256`. The original raw archives and metadata remain unchanged.
- The runner retained resources on its initial validator failure. After custom verification, the VM, boot disk, restricted SSH firewall rule, isolated VPC, subnet, and egress route were deleted and independently confirmed absent. Temporary SSH credentials were removed. No incomplete cloud resources remain.
- Detailed raw results, logs, schedule, metadata, checksums, lifecycle records, and the verification note are in `bench-results/resource-retirement-lock-ab-96aa2927-20261124/` (git-ignored).
