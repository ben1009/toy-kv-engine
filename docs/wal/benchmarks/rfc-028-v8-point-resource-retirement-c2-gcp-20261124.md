# RFC 028 V8 Point Resource Retirement C2 GCP A/B — 2026-11-24

## Result

C2 batch retirement improved paired median Point throughput by **2.07% at 8 writers** and **3.27% at 16 writers** versus per-ticket retirement. It won 4/5 and 5/5 pairs, respectively. Compared with C1 detach-only, the incremental gains were **1.48% at 8 writers** and **2.77% at 16 writers**. These results are below the pre-set 5% retention threshold, so C2 is not performance-qualified for default use. Keep the result experimental and do not change the runtime default on this evidence alone.

Sampled p99 improved modestly with C2: −3.30% at Point8 and −2.78% at Point16 versus mode 0. Batch64 showed no material regression. Puts/Sync stayed nearly unchanged, so the small throughput gains did not come from forming larger durability groups.

## Experiment

- Source commit: `22c357a9ca26cec19011c585ff5b94a47945cb8e` on PR #562 branch `perf/v8-point-progress-ring-f05-qualification`.
- One release binary was used for every arm; SHA-256: `30e43e2af20df504b70e84648b260e5f6f5ae86e538594ebe8c099f12c9c685d`.
- Retirement modes: `0` per-ticket; `1` C1 detached Point prefix; `2` C1 detach plus C2 batch retirement.
- Counterbalanced five-repetition schedule: Point8, Point16, and 16-writer Batch64; 100,000 operations per run. Total: 45 scored runs (15 per workload).
- Point latency was sampled once per 10 operations (10,000 samples/run). Batch64 latency was not sampled. No CPU attribution, mutex sampling, perf, or detailed WAL tracing was enabled.
- Fixed runtime configuration across V8 arms: Cursor progress mode, Direct Completion, Direct FRONTIER, dynamic coalescing, 1 MiB sliding Point Lookahead, ordered DATA submission, extent-policy Vec, Point buffer preallocation off, singleton submission off, and credit batching off. Runtime proof counters confirmed the planned retirement mode on each run.
- Host: GCP C4 `c4-standard-16-lssd`, 16 vCPU, Intel Xeon 6985P-C at 2.30 GHz, Ubuntu 24.04, Linux `7.0.0-1017-gcp`, Rust `nightly-2026-09-23`.
- Storage observed by the runner: two 375 GiB NVMe Local SSDs in RAID0, ext4 mounted at `/mnt/gcp-wal-bench` (`/dev/md127`, 749.7 GiB). DATA and FRONTIER reported `O_DIRECT=true`; the sync handle reported `O_DIRECT=false`.
- The run used the isolated benchmark VPC and a temporary SSH rule restricted to the current `/32`. Billing, APIs, and quotas were not changed. The VM lifetime cap was 60 minutes; estimated spend was $1.50 against the authorized $2 ceiling. Two earlier zone placements failed with `ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS` before a benchmark VM was created; only the successful `us-central1-c` run is included here.

## Results

Per-arm values are medians across five runs. Paired changes are medians of the five within-repetition ratios, so they need not equal ratios of the displayed per-arm medians.

| Workload | Mode 0 throughput | Mode 1 throughput | Mode 2 throughput | Mode 2 vs 0; wins | Mode 2 vs 1; wins |
|---|---:|---:|---:|---:|---:|
| Point8 | 33,012 ops/s | 33,196 ops/s | 33,618 ops/s | +2.07%; 4/5 | +1.48%; 4/5 |
| Point16 | 44,275 ops/s | 44,335 ops/s | 45,722 ops/s | +3.27%; 5/5 | +2.77%; 5/5 |
| Batch64 / 16 writers | 630,164 ops/s | 633,027 ops/s | 631,344 ops/s | +0.24%; 3/5 | −0.22%; 1/5 |

| Point workload | Mode | Process CPU μs/Put | Puts/Sync | Sampled p99 ms |
|---|---:|---:|---:|---:|
| Point8 | 0 | 51.083 | 7.711 | 0.724 |
| Point8 | 1 | 51.078 | 7.721 | 0.714 |
| Point8 | 2 | 50.629 | 7.707 | 0.694 |
| Point16 | 0 | 70.467 | 10.257 | 0.912 |
| Point16 | 1 | 71.143 | 10.177 | 0.915 |
| Point16 | 2 | 70.185 | 10.203 | 0.889 |

Mode 2 versus mode 0 paired p99 changes were −3.30% at Point8 and −2.78% at Point16. The latency figures are sampled tails, not full-operation histograms.

The runtime counters confirmed that the intended paths were measured:

- Mode 0: zero detached or batched Point prefixes.
- Mode 1: 112,382 detached prefixes and zero batched prefixes across the ten Point runs.
- Mode 2: 112,433 detached and batched prefixes, covering 998,504 detached tickets. Every Point run recorded at least 9,508 batched prefixes.
- Batch64: all 15 runs recorded zero detached and zero batched Point prefixes in every mode.

## Interpretation

- C1 detach-only was effectively neutral: +0.56% Point8 and +0.23% Point16 paired throughput versus mode 0.
- C2 added a repeatable but sub-threshold Point16 improvement over either mode 0 or C1. At Point8 it also remained below the 5% gate.
- Process CPU fell by 0.45 μs/Put at Point8 and 0.28 μs/Put at Point16 versus mode 0. Sync count and Puts/Sync were close across modes, consistent with a small resource-retirement cost reduction rather than changed group formation.
- Batch64 mode 2 was within 0.24% of mode 0 and 0.22% of mode 1; no material regression was observed.
- This single-host qualification measures the configured Cursor + Direct Completion + Direct FRONTIER Point path. It is not a V7 comparison or a production-default qualification.

## Verification and cleanup

- `summary.json`: `complete: true`, 45/45 planned runs completed.
- `remote-exit.json`: `failed: false`, `runs_started: 45`, `runs_attempted: 45`.
- The retirement-suite verifier passed: 45/45 planned run IDs, each with a successful `result.json` and matching `run-metadata.json`; mode, runtime flags, metrics, and summary rows validated.
- Downloaded `results.tar.gz` SHA-256: `79ec384c7ff34f19f44ad037aae1ad75becef8e35bf6fffd6e2fa81e4333593d` (55,468 bytes). The local hash matches both the remote hash and retained `bundle-metadata.json`; the extracted archive contains 335 files. Raw results, schedule, logs, archive, checksum metadata, and lifecycle records are retained under the git-ignored `bench-results/v8-point-resource-retirement-20261124-us-central1-c/` directory.
- After verification, the VM and auto-delete boot disk were deleted, the temporary SSH rule was deleted, and Terraform destroyed the isolated VPC, subnet, and egress route. GCP inventory queries confirmed all named resources absent; Terraform state is empty. Temporary SSH credentials and reconnect instructions were removed. Cleanup evidence is in the local `cleanup-verification.json`.
