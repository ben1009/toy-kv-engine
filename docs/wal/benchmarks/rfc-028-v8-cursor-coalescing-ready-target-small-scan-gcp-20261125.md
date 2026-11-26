# RFC 028 V8 Cursor Coalescing Ready-Target Small Scan on GCP — Target4 led the scan, Point8 gate missed, 2026-11-25

## Result

Target4 had the largest paired median throughput gain at both writer counts:
3.68% at Point8 and 7.22% at Point16. It won 3/5 Point8 pairs and 4/5
Point16 pairs. No target met the 5% throughput gate at both writer counts.
Keep Dynamic as the default. Target4 is a Point16 candidate for a broader
qualification, not a generally qualified policy.

All targets reduced paired p99 by roughly 10–12%, but raised CPU/Put and made
durability groups smaller. At Point16, the targets increased CPU by 14.18–17.75
μs/Put and reduced Puts/Sync from a 11.29 median to 6.24–6.81. The throughput
gain therefore came with substantially more CPU and sync work. Target4 was the
strongest throughput choice in this scan; Target3 had a 5/5 Point16 win rate
with a slightly smaller CPU increase.

The scan only covered Point8 and Point16. It does not qualify Target4 for
Point1, Batch64, or production defaults.

## Experiment

- Source commit: `89cd5630a541e065477bb08a0ac88f51c1267869`
- Release `write-perf` SHA-256:
  `40b8645e59edd0d6874a1a90ce775ba1fa4ebf8732e54d89796a993115aa5f6f`
- Host: GCP `c4-standard-16-lssd`, `us-central1-b`, 16 vCPUs
- Storage: two 375 GiB NVMe Local SSDs in RAID0 (`md127`), ext4 with
  `noatime,stripe=256`, mounted at `/mnt/gcp-wal-bench` (749.7 GiB usable)
- Schedule: Dynamic and Targets 1–4; five counterbalanced repetitions at
  Point8 and Point16; 50 scoring runs total
- Workload: 100,000 Point Puts per run, 1 KiB values, latency sampled every 10
  Puts. Stage A buffer preallocation was enabled and recorded at run start and
  end; every run had 100,000 hits and no fallback.
- Fixed configuration: Cursor progress mode, Direct Completion, CallerDriven
  durability, Legacy ACK, direct FRONTIER, ordered DATA submission, singleton
  submission disabled, and 1 MiB sliding Point lookahead. Candidate arms set
  the ready target to 1, 2, 3, or 4; Dynamic leaves it unset. Detailed
  profiling and CPU attribution were disabled during scoring.
- Observed I/O metadata: DATA and FRONTIER used O_DIRECT; Sync did not.
- Schedule SHA-256:
  `fbcf702a0d3d6fabeb8ad62feda2efa043531fa71c746b0144cd8b50f6f817fe`
- Benchmark plan SHA-256:
  `47aac257884e381a846dee007d70a9eb3b4c27eeae05a302efadba5eb4fafddc`

## Per-arm scoring medians

These are medians across five runs. Paired changes below are medians of the
five within-repetition changes and therefore need not equal ratios or
differences of per-arm medians. Throughput is puts/s, CPU is μs/Put, p99 is
milliseconds, and sync count is per 100,000 Puts.

| Workload | Arm | Throughput | CPU/Put | p99 | Puts/Sync | Sync count |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point8 | Dynamic | 34,448 | 47.95 | 0.711 | 7.76 | 12,882 |
| Point8 | Target1 | 33,854 | 58.06 | 0.627 | 3.59 | 27,847 |
| Point8 | Target2 | 34,683 | 56.72 | 0.625 | 3.73 | 26,813 |
| Point8 | Target3 | 34,827 | 56.27 | 0.636 | 3.95 | 25,320 |
| Point8 | Target4 | 35,179 | 53.71 | 0.627 | 4.25 | 23,530 |
| Point16 | Dynamic | 47,008 | 67.52 | 0.901 | 11.29 | 8,861 |
| Point16 | Target1 | 49,677 | 82.67 | 0.811 | 6.24 | 16,014 |
| Point16 | Target2 | 49,463 | 84.63 | 0.801 | 6.33 | 15,796 |
| Point16 | Target3 | 50,197 | 82.54 | 0.800 | 6.53 | 15,311 |
| Point16 | Target4 | 50,465 | 82.80 | 0.792 | 6.81 | 14,694 |

## Paired changes versus Dynamic

| Workload | Candidate | Throughput change / wins | CPU/Put change | p99 change | Puts/Sync change | Target hits / syncs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point8 | Target1 | +0.35% / 3 of 5 | +10.11 μs | −11.64% | −4.16 | 85,908 / 139,239 (61.7%) |
| Point8 | Target2 | +0.68% / 3 of 5 | +9.02 μs | −11.38% | −4.03 | 80,573 / 134,110 (60.1%) |
| Point8 | Target3 | +2.11% / 3 of 5 | +7.32 μs | −11.40% | −3.81 | 73,048 / 126,752 (57.6%) |
| Point8 | Target4 | +3.68% / 3 of 5 | +5.97 μs | −11.60% | −3.51 | 61,469 / 117,634 (52.3%) |
| Point16 | Target1 | **+6.28% / 5 of 5** | +15.67 μs | −9.85% | −5.04 | 65,048 / 80,072 (81.2%) |
| Point16 | Target2 | **+5.00% / 5 of 5** | +17.75 μs | −10.70% | −5.04 | 64,717 / 79,132 (81.8%) |
| Point16 | Target3 | **+6.54% / 5 of 5** | +14.18 μs | −11.28% | −4.80 | 62,162 / 76,660 (81.1%) |
| Point16 | Target4 | **+7.22% / 4 of 5** | +15.42 μs | −11.77% | −4.48 | 58,769 / 73,390 (80.1%) |

Target hits are summed across the five runs. Paired throughput and p99 changes
are medians of within-repetition changes. Negative Puts/Sync change means
smaller durability groups.

## Verification, archive, and cleanup

- `summary.json`: `complete: true`; 50 planned, started, attempted, and
  completed runs. `remote-exit.json`: `failed: false`; all 50 runs started.
- The strict small-scan verifier passed: all 50 scheduled run IDs have
  successful `result.json` files and matching `run-metadata.json`; target
  settings, counters, and the recomputed comparison match the plan and raw
  results.
- Transferred archive: 63,355 bytes, 370 files. Remote and local SHA-256
  match:
  `163bff267da1b0fb36b47f766da2d57a9ea0dc626bcfaec683dea85a4b4ed8fc`.
  The archive and checksum sidecar are retained locally with the extracted
  results under
  `bench-results/gcp-v8-ready-target-small-scan-89cd5630-run/`; raw data is not
  committed.
- The Free Trial preflight passed before VM creation. It made no billing,
  API, or quota changes.
- Terraform destroyed the temporary VPC, subnet, and egress route; final
  lookups confirmed those resources, the VM, boot disk, and temporary SSH
  firewall are absent. Terraform state is empty. The first destroy attempt
  used an incorrect variable name and the next hit a transient Compute API EOF;
  a retry completed cleanup. The default VPC was not used or changed.

The ready-target policy shortens durability cycles and improves Point16
throughput and p99 in this scan, at a substantial CPU and sync-frequency cost.
Dynamic remains the default pending a broader same-host comparison with V7,
Point1, and Batch64.
