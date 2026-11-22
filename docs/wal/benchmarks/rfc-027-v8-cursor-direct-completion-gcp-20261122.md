# RFC 027 V8 Cursor direct-completion GCP qualification (2026-11-22)

## Decision

Direct completion improves Point throughput at one and eight writers, but it
does not pass the promotion gate because its 16-writer paired gain is only
2.96% and it wins three of five pairs. Keep the bypass benchmark-only; do not
change the production default. Batch64 stays within its regression guard.

## Results

One `c4-standard-16-lssd` host in `us-central1-b` ran a counterbalanced
five-repetition comparison between Cursor relay and Cursor direct completion.
Each run performed 100,000 operations with 1 KiB values. Point ran at 1, 8,
and 16 writers; Batch64 at 16 writers was a throughput guard. Both arms used
the same release binary, Cursor progress mode 2, Direct FRONTIER, O_DIRECT
DATA and FRONTIER, dynamic coalescing, and 1 MiB Point-only sliding lookahead.
Only `TOYKV_BENCH_V8_CURSOR_DIRECT_COMPLETION` changed. Detailed profiling,
sync diagnostics, progress counters, and worker-group histograms were disabled.
Point latency was sampled every 10 completed puts.

Throughput and CPU/Put are medians across five runs per arm. Paired change is
the median of the five within-repetition percentage changes, not the ratio of
the arm medians.

| Workload | Writers | Cursor relay kputs/s | Direct completion kputs/s | Paired change | Direct wins | CPU/Put relay → direct (μs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 7.317 | 8.312 | +13.89% | 5/5 | 63.88 → 53.95 |
| Point | 8 | 28.325 | 30.328 | +8.23% | 5/5 | 57.62 → 52.89 |
| Point | 16 | 40.393 | 41.897 | +2.96% | 3/5 | 79.77 → 73.36 |
| Batch64 | 16 | 625.848 | 631.050 | +0.86% | 3/5 | 5.15 → 5.01 |

| Workload | Writers | Puts/sync relay → direct | p50 ms relay → direct | p95 ms relay → direct | p99 ms relay → direct | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 1.000 → 1.000 | 0.125 → 0.108 | 0.220 → 0.210 | 0.420 → 0.416 | +0.35% |
| Point | 8 | 7.55 → 7.59 | 0.249 → 0.233 | 0.488 → 0.453 | 0.821 → 0.782 | −2.30% |
| Point | 16 | 10.13 → 10.12 | 0.347 → 0.333 | 0.713 → 0.714 | 1.009 → 1.028 | +1.92% |
| Batch64 | 16 | 523.56 → 512.82 | — | — | — | — |

Percentiles are medians of per-run sampled percentiles, not percentiles
computed from pooled raw samples. Batch64 was throughput-only. The candidate
passed the Point1/Batch64 paired-throughput guard (no worse than 2%) and the
Point8/Point16 p99 guard (no more than 5% regression). It failed the Point
throughput gate: both Point8 and Point16 must gain at least 5% and win at
least four of five pairs. Point16 gained 2.96% and won three pairs, so
`candidate_qualified` is false.

## Run verification and provenance

- Planned, started, attempted, and completed: 40/40 runs.
- `summary.json`: `complete: true`, 40 planned and completed, matrix
  validation complete. `remote-exit.json`: `failed: false`, 40 runs started.
- The strict direct-completion validator confirmed all 40 planned run IDs have
  a successful `result.json` and matching `run-metadata.json`.
- Runtime source commit: `8320463fbcf4dab6e9c3830edfd046834f041aa3`.
- Release `write-perf` SHA-256:
  `00c553e07d775dccebc150971724f9028d47d62e0e4320f2e943855e41b54050`.
- Host: 16-vCPU C4, two 375 GiB Local SSD devices in RAID0, ext4
  (`rw,noatime,stripe=256`), Linux `7.0.0-1017-gcp`, pinned nightly
  `nightly-2026-09-23`, zone `us-central1-b`. The mounted filesystem was
  `/dev/md127` at `/mnt/gcp-wal-bench`.
- The downloaded archive matched both the remote and local SHA-256 values:
  `150622c0c14f85347f8f91992790bac710868c509ca6499c7206d24457d3a0cc`.
  The verified archive contained 300 files. Raw archives and per-run
  environment metadata remain local, not committed; the result set is under
  `bench-results/gcp-v8-cursor-direct-completion-8320463f-us-central1-b-run/`.
- Cleanup verification confirms the benchmark VM, boot disk, temporary SSH
  firewall rule, isolated VPC, subnet, and egress route are absent.

This is a same-host benchmark-only comparison. It does not qualify mixed
Point/Batch concurrency or production behavior, and it does not change the
Cursor or V8 production defaults. See the
[RFC 027 implementation plan](../rfc-027-v8-point-progress-ring-implementation-plan.md)
for the staged candidate status.
