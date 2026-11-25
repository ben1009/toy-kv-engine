# RFC 028 V8 Cursor Direct Completion Wake Token/Select GCP A/B — No throughput win, 2026-11-25

## Result

Select wake delivery did not produce a stable throughput improvement. Point8
regressed in all five pairs, while Point16 was effectively neutral. Keep Token
as the default wake mode; this experiment does not qualify Select for promotion.
The change remains a benchmark-only option, and no production WAL behavior was
changed by this experiment.

## Experiment

- Source commit: `01bdf812250c3746d5e44853c2ec0f72b28160e8`
- Release `write-perf` SHA-256: `4a316d6489ed4ca2ff3ba5f3aaf6bef94f3a594614b3c3d04fc6f7f3e1de9d9b`
- Host: GCP `c4-standard-16-lssd`, `us-central1-c`, 16 vCPUs
- Storage: two 375 GiB NVMe Local SSDs, RAID0 (`md127`), ext4, 749.7 GiB
  mounted at `/mnt/gcp-wal-bench`
- Schedule: 40 unprofiled scoring runs; five counterbalanced Token/Select pairs
  for Point1, Point8, Point16, and Batch64 / 16 writers
- Workload: 100,000 operations per run; 1 KiB Point values; Batch64 uses 64
  entries per operation
- Fixed configuration: Cursor progress mode, Direct Completion, CallerDriven
  durability, Legacy ACK, dynamic coalescing, direct FRONTIER, ordered DATA
  submission, and 1 MiB Point lookahead. Stage A Point buffer preallocation was
  enabled for Point workloads and disabled for Batch64.
- The only A/B change was direct-completion wake delivery: Token versus Select
  over the completion queue and control channel. Detailed profiling and worker
  group histograms were disabled during scoring.

The zone capacity preflight passed in `us-central1-b` and `us-central1-a`, but
both instance requests failed with a C4 Local SSD stockout before any benchmark
run began. The same isolated VPC passed the runner preflight in all attempted
zones; scoring completed in `us-central1-c`.

## Scoring results

Throughput, CPU/Put, and Puts/Sync are the median across five runs per arm.
Paired changes are the median of the five within-round percentage changes.
Latency is milliseconds; p99 change is paired Select versus Token.

| Workload | Token ops/s | Select ops/s | Paired throughput change / wins | CPU/Put, Token → Select | p99 change | Puts/Sync, Token → Select |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Point1 | 8,675 | 8,697 | +0.49% / 4 of 5 | 52.89 → 50.93 μs | −3.28% | 1.00 → 1.00 |
| Point8 | 35,290 | 34,328 | −2.63% / 0 of 5 | 48.66 → 48.90 μs | −2.26% | 7.74 → 7.74 |
| Point16 | 44,951 | 44,850 | +0.12% / 3 of 5 | 70.31 → 69.51 μs | +0.45% | 11.02 → 10.95 |
| Batch64 / 16 writers | 632,092 | 628,294 | −0.67% / 1 of 5 | 5.12 → 4.93 μs | Not sampled | 507.61 → 512.82 |

The paired Point8 p99 improved slightly while throughput fell; Point16 CPU/Put
fell by 0.79 μs without a corresponding throughput or group-size improvement.
These results do not show that fewer completion notifications improve the
Point critical path. No separate progress-wait/token-attempt diagnostic matrix
was run, so this A/B does not attribute the neutral or negative outcomes to a
specific wake or scan cost.

## Verification and cleanup

- `summary.json`: `complete: true`; 40 planned, started, attempted, and
  completed runs
- `remote-exit.json`: `failed: false`; 40 runs started and attempted
- Strict completion-wake verifier: passed with no errors; all 40 run IDs have
  successful `result.json` files and matching `run-metadata.json`
- Transferred archive: 300 files, 51,288 bytes; SHA-256:
  `9cd86aed645c4c865472e2248b248efe1233b359cf2c0df3ca474acff5e2fde2`
- Local archive SHA-256 matched both remote and local bundle metadata. The raw
  archive and extracted results remain in the ignored local
  `bench-results/gcp-v8-cursor-direct-completion-wake-ab-20261125-us-central1-c/`
  directory; raw run data is not committed.
- Cleanup verification confirms the VM, temporary SSH firewall, isolated VPC,
  subnet, and egress route are absent; Terraform state is empty. The default
  VPC was not used or changed.
