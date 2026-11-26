# RFC 027 V8 Point Progress Cursor GCP qualification (2026-11-22)

## Decision

Cursor improves on the first progress-ring implementation at 8 and 16 Point
writers, but it does not pass the promotion gate against Eager. Keep Cursor
bench-only and opt-in; do not change the production default. The 8-writer
result is effectively flat against Eager, and the 16-writer paired gain is
only 1.36%, below the required 5% gain.

## Results

One `c4-standard-16-lssd` host ran a counterbalanced five-repetition,
three-arm comparison: Eager (mode 0), Ring (mode 1), and Cursor (mode 2).
Workloads were Point at 1, 8, and 16 writers and Batch64 at 16 writers; each
run performed 100,000 operations with 1 KiB values. All arms used the same
release binary, two Local SSDs in ext4 RAID0, Direct FRONTIER, 1 MiB Point
Lookahead, dynamic coalescing, and identical digest hash sampling. Only the
progress mode varied. Detailed profiling, completion timelines, sync
diagnostics, progress counters, and worker-group histograms were disabled.
Point latency was sampled every 10 operations (10,000 samples per run).

Throughput and CPU/Put are medians across five runs per arm. Paired change is
the median of five within-repetition percentage changes, rather than the
percentage change between the two arm medians.

| Workload | Writers | Eager kputs/s | Ring kputs/s | Cursor kputs/s | Eager → Cursor paired change | Cursor wins | CPU/Put Eager → Ring → Cursor (μs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 7.630 | 7.711 | 7.762 | +3.94% | 5/5 | 60.04 → 61.90 → 62.06 |
| Point | 8 | 31.998 | 31.145 | 31.924 | −0.14% | 1/5 | 52.34 → 61.27 → 53.95 |
| Point | 16 | 43.062 | 42.408 | 43.466 | +1.36% | 5/5 | 77.02 → 85.52 → 76.03 |
| Batch64 | 16 | 630.246 | 631.783 | 630.145 | −0.22% | 2/5 | 5.04 → 5.21 → 5.07 |

Cursor's paired throughput changes against Ring were +0.68% at Point1 (4/5
wins), +2.98% at Point8 (5/5), +2.46% at Point16 (5/5), and −0.22% at
Batch64 (2/5). This recovers much of Ring's Point8 regression, but does not
show a material gain over the Eager baseline.

| Workload | Writers | Syncs Eager → Cursor | Puts/sync Eager → Cursor | p50 ms Eager → Cursor | p95 ms Eager → Cursor | p99 ms Eager → Cursor | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 1 | 100,000 → 100,000 | 1.000 → 1.000 | 0.1224 → 0.1203 | 0.2000 → 0.1410 | 0.4132 → 0.4070 | −5.39% |
| Point | 8 | 13,043 → 12,947 | 7.667 → 7.724 | 0.2277 → 0.2305 | 0.4030 → 0.3939 | 0.7111 → 0.6974 | −1.50% |
| Point | 16 | 9,985 → 9,774 | 10.015 → 10.231 | 0.3307 → 0.3257 | 0.6583 → 0.6391 | 0.9027 → 0.9257 | +3.11% |
| Batch64 | 16 | 196 → 193 | 510.204 → 518.135 | — | — | — | — |

Percentiles are medians of per-run sampled percentiles, not percentiles
computed from pooled raw latency samples. Batch64 was a throughput-only
guard. Cursor passed the Point1 and Batch64 paired-median regression guards
(no worse than 2%) and the Point8/Point16 p99 guard (no more than 5%
regression). It failed the Point throughput gate: both Point8 and Point16
must gain at least 5% and win at least four of five pairs. Point8 was flat
and won one pair; Point16 gained 1.36% and won all five. The overall
`candidate_qualified` result is therefore false.

## Run verification and provenance

- Planned, started, attempted, and completed: 60/60 runs; five counterbalanced
  repetitions per arm and workload cell.
- `summary.json`: `complete: true`; 60 planned and completed; matrix
  validation complete. `remote-exit.json`: `failed: false`, 60 runs started.
- The strict Cursor validator confirmed all 60 planned run IDs have a
  successful `result.json` and matching `run-metadata.json`. The generic and
  Cursor validators both passed after reconstructing the schedule artifact.
- Runtime source commit: `99d6a3b7a931fb3908e14c212f6513b49294ac10`.
- Release `write-perf` SHA-256:
  `91856a49ef2f6aa6f2df88b98c564886dd0bf5d41267f0ee83041217fb4cb330`.
- Host: 16-vCPU C4, two 375 GiB Local SSD devices in RAID0, ext4
  (`rw,noatime,stripe=256`), Linux `7.0.0-1017-gcp`, pinned nightly
  `nightly-2026-09-23`, zone `us-central1-b`.
- Two earlier provisioning attempts in `us-central1-a` stopped before scoring
  with `ZONE_RESOURCE_POOL_EXHAUSTED`. The successful run used the alternate
  zone; it did not change the benchmark matrix or binary.
- Both downloaded result archives matched their remote SHA-256 values. The
  second archive SHA-256 is
  `d5d53a9138a1fa10f68e6eb12b723dc4980c8eb3bd19811f620859519f5f60e1`.
  However, the archive allowlist omitted
  `point-progress-cursor-schedule.tsv`, so automated archive validation
  could not complete from the archive alone. The schedule was recovered from
  the retained VM before teardown and independently hashed:
  `b1df6ea924a37999dbd21ad543c4bf2e7f2c526580228907f112d263dd7a5743`.
  After combining the verified archive with that recovered schedule, both
  strict validators passed all 60 runs. The allowlist omission is fixed and
  covered by an archive round-trip test in this commit.
- Raw archives and per-run environment metadata remain local, not committed.
  The verified result set is under
  `bench-results/gcp-v8-progress-cursor-20261122-us-central1-b/`.
- Cleanup verification confirms the benchmark VM, boot disk, temporary SSH
  firewall rule, isolated VPC, subnet, and egress route are absent. Terraform
  state remains outside the repository.

This qualification covers only the stated host and workloads. It does not
establish behavior on other filesystems or machines, mixed Point/Batch
workloads, or production deployment. See the
[RFC 027 implementation plan](../rfc-027-v8-point-progress-ring-implementation-plan.md)
for the candidate's staged status.
