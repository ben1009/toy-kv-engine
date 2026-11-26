# RFC 028 V8 Cursor Coalescing Ready-Target GCP A/B — Point16 Target8 gain, overall gate failed, 2026-11-25

## Result

The ready-prefix target did not pass the overall promotion gate. Target8 improved
Point16 throughput by 7.57% in all five pairs and reduced its p99 by 10.87%, but
Point8 throughput fell 3.44%. Target12 improved Point16 by only 2.42% and did
not improve Point8. Target8 also exceeded the Point1 regression guard at
−2.49%. Keep Dynamic as the default; the Point16 Target8 result is a scoped
candidate, not evidence for enabling the policy across workloads.

Target8 caused earlier coalescing exits at Point16: it recorded 38,367 target
hits across 59,497 syncs (64.5%), while Puts/Sync fell from 11.08 to 8.41.
Throughput and p99 improved despite more frequent, smaller durability cycles;
the result is not explained by larger groups.

## Experiment

- Source commit: `9dba5f409de609a5cf2ff14ff107cc4cc7dee252`
- Release `write-perf` SHA-256: `d9051081dd27ed0a72339e6a84d8dbd3626193bd6247d3bdcae2e406eadfc5e0`
- Host: GCP `c4-standard-16-lssd`, `us-central1-b`, 16 vCPUs
- Storage: two 375 GiB NVMe Local SSDs in RAID0 (`md127`), ext4 with
  `noatime,stripe=256`, mounted at `/mnt/gcp-wal-bench` (749.7 GiB usable)
- Schedule: Dynamic, Target8, and Target12; five counterbalanced repetitions of
  Point1, Point8, Point16, and Batch64 / 16 writers; 60 scoring runs total
- Workload: 100,000 operations per run; 1 KiB Point values; Batch64 has 64
  entries per operation; latency sampled every 10 operations
- Fixed configuration: Cursor progress mode, Direct Completion, CallerDriven
  durability, Legacy ACK, direct FRONTIER, Dynamic coalescing with the optional
  ready target set to 8 or 12 for candidate arms, ordered DATA submission,
  singleton submission disabled, and 1 MiB sliding Point lookahead. Stage A
  buffer preallocation was enabled for Point workloads and disabled for
  Batch64. Detailed profiling and CPU attribution were disabled during scoring.

## Scoring results

Per-arm throughput, CPU/Put, Puts/Sync, sync count, and p99 are medians across
five runs. Paired changes are medians of the five within-round changes; because
of the paired calculation they need not equal ratios of the per-arm medians.
Throughput is puts/s and latency is milliseconds.

| Workload | Dynamic puts/s | Target8 puts/s | Target12 puts/s |
| --- | ---: | ---: | ---: |
| Point1 | 8,391 | 8,171 | 7,913 |
| Point8 | 32,803 | 32,523 | 33,504 |
| Point16 | 45,064 | 48,558 | 46,399 |
| Batch64 / 16 writers | 629,258 | 628,023 | 627,205 |

| Workload | Candidate | Paired throughput change / wins | CPU/Put change | p99 change | Puts/Sync change |
| --- | --- | ---: | ---: | ---: | ---: |
| Point1 | Target8 | −2.49% / 1 of 5 | +0.03 μs | +0.67% | 0.00 |
| Point1 | Target12 | −1.40% / 1 of 5 | +0.28 μs | +16.89% | 0.00 |
| Point8 | Target8 | −3.44% / 2 of 5 | −0.63 μs | +2.48% | +0.01 |
| Point8 | Target12 | −0.52% / 2 of 5 | −2.05 μs | +0.84% | +0.02 |
| Point16 | Target8 | **+7.57% / 5 of 5** | +6.98 μs | **−10.87%** | −2.67 |
| Point16 | Target12 | +2.42% / 5 of 5 | +1.40 μs | −4.75% | −1.57 |
| Batch64 / 16 writers | Target8 | +0.01% / 3 of 5 | −0.11 μs | Not sampled | −48.33 |
| Batch64 / 16 writers | Target12 | −0.54% / 1 of 5 | −0.04 μs | Not sampled | −37.63 |

Target hits by workload, summed across the five runs:

| Workload | Arm | Target hits / syncs | Hit rate |
| --- | --- | ---: | ---: |
| Point1 | Target8 / Target12 | 0 / 500,000 each | 0% |
| Point8 | Target8 | 0 / 64,470 | 0% |
| Point8 | Target12 | 0 / 64,441 | 0% |
| Point16 | Target8 | 38,367 / 59,497 | 64.5% |
| Point16 | Target12 | 21,340 / 52,570 | 40.6% |
| Batch64 / 16 writers | Target8 | 297 / 1,036 | 28.7% |
| Batch64 / 16 writers | Target12 | 170 / 1,030 | 16.5% |

Point8 did not reach either target in any run, and its Puts/Sync remained near
7.7. Point16 reached both targets frequently; Target8 increased the median
sync count from 9,025 to 11,894 and reduced median Puts/Sync. This is consistent
with reducing wait time at the cost of more durability cycles. Target12 made a
smaller throughput tradeoff and also did not meet the 5% Point16 gate.

## Verification, archive recovery, and cleanup

- `summary.json`: `complete: true`; 60 planned, started, attempted, and
  completed runs. `remote-exit.json`: `failed: false`; 60 started and attempted.
- The strict ready-target verifier passed: all 60 scheduled run IDs have
  successful `result.json` files and matching `run-metadata.json`; the fixed
  plan, runtime configuration, target counters, and recomputed comparison all
  match.
- The runner exited nonzero during local verification because its two result
  archives omitted `cursor-coalescing-ready-target-schedule.tsv`. The full
  workload matrix had completed. The schedule was recovered from the retained
  VM over SSH with a pinned host key; its remote and local SHA-256 match:
  `5d41f1b45a38f01189a32faefe25c67d6830506bb236504b9d7b6ed1ce72ab79`.
- Both transferred archives and checksum sidecars are retained locally under
  `bench-results/gcp-e3-ready-target-9dba5f40/score-results-us-central1-b-retry2/`:
  - Attempt 1: 68,820 bytes, SHA-256
    `569fb22f5ffaf1402d5fb65a504311d557317386e7d6a7be83fbb88b537401b1`.
  - Attempt 2: 68,820 bytes, SHA-256
    `6e24a50353357bd689de72669c26131d3e9e42911fce09d8ed76a649953b1f99`.
  Raw result archives and the separately recovered schedule are not committed.
- Provisioning retries before scoring: C4 Local SSD capacity was unavailable in
  `us-central1-a`; one `us-central1-b` attempt stopped on a transient TLS EOF
  before VM creation. Neither attempt ran benchmark work.
- Cleanup verification confirms the benchmark VM, auto-delete boot disk,
  restricted SSH rule, isolated VPC, subnet, and egress route are absent. The
  Terraform state is retained outside the repository and contains no resources.
  The default VPC was not used or changed.

Results and the recovered schedule remain in the ignored local
`bench-results/gcp-e3-ready-target-9dba5f40/score-results-us-central1-b-retry2/`
directory. No runtime code was changed by this benchmark report.
