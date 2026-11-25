# RFC 028 V8 Cursor Coalescing Ready-Target Scan on GCP — Target4 led Point16, Point8 gate missed, 2026-11-25

## Result

Target4 was the strongest of the scanned settings at both writer counts. It
improved Point16 throughput by 10.02% in all five pairs and reduced paired p99
by 16.65%. Point8 improved by 3.03% in all five pairs, below the 5% throughput
gate. Target6 and Target8 also improved Point16 in all five pairs, but neither
beat Target4's throughput. Keep Dynamic as the default: no single scanned
target met the 5% gate at both writer counts, and Point1 and Batch64 were not
part of this parameter scan.

The speedup came with higher CPU cost and smaller groups. At Point16, Target4
raised paired CPU/Put by 14.44 μs, increased the median sync count from 8,830 to
14,588, and reduced Puts/Sync from 11.33 to 6.85. Its paired p99 improved, so
the result supports a durability-cycle turnover effect in this workload; it
does not show lower CPU cost or larger Group Commit batches. At Point8, Target4
also increased sync frequency substantially, while its throughput gain stayed
below the promotion threshold.

Target8 did not trigger at Point8: it recorded zero target hits across the
five runs. Its small throughput change there is not evidence that the target
policy helped that workload.

## Experiment

- Source commit: `e2ccf8c4a2f64e27c4eab33a3e29ca3f46aa7568`
- Release `write-perf` SHA-256:
  `c80b662d46d88fbd78b69d9b7205e42ad6128f2918dde65b2c19c3a910c96c8b`
- Host: GCP `c4-standard-16-lssd`, `us-central1-b`, 16 vCPUs
- Storage: two 375 GiB NVMe Local SSDs in RAID0 (`md127`), ext4 with
  `noatime,stripe=256`, mounted at `/mnt/gcp-wal-bench` (749.7 GiB usable)
- Schedule: Dynamic, Target4, Target6, and Target8; five counterbalanced
  repetitions at Point8 and Point16; 40 scoring runs total
- Workload: 100,000 Point Puts per run, 1 KiB values, latency sampled every 10
  Puts. Point buffer preallocation (Stage A) was enabled and recorded as
  enabled at run start and end; each run recorded 100,000 hits and no fallback.
- Fixed configuration: Cursor progress mode, Direct Completion, CallerDriven
  durability, Legacy ACK, direct FRONTIER, Dynamic coalescing with the optional
  ready target set to 4, 6, or 8 for candidate arms, ordered DATA submission,
  singleton submission disabled, and 1 MiB sliding Point lookahead. Detailed
  profiling and CPU attribution were disabled during scoring.
- Observed I/O metadata: DATA and FRONTIER used O_DIRECT; Sync did not.
- Schedule SHA-256:
  `2ddf1248a6c5ea1a21d34e72bacba2654c8e24232c2a05846055282cc385dff6`
- Benchmark plan SHA-256:
  `dfeaa017bd8a9cb86e0745ae5dee84d0c057aef4c4360cf023481049cd4371c1`

## Per-arm scoring medians

These are medians across five runs. Paired changes below are medians of the
five within-repetition changes and therefore need not equal ratios or
differences of the per-arm medians. Throughput is puts/s, CPU is μs/Put, p99 is
milliseconds, and sync count is per 100,000 Puts.

| Workload | Arm | Throughput | CPU/Put | p99 | Puts/Sync | Sync count |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point8 | Dynamic | 32,245 | 48.43 | 0.782 | 7.70 | 12,990 |
| Point8 | Target4 | 33,289 | 54.43 | 0.676 | 4.28 | 23,378 |
| Point8 | Target6 | 32,923 | 50.69 | 0.711 | 5.81 | 17,223 |
| Point8 | Target8 | 32,796 | 47.55 | 0.753 | 7.71 | 12,966 |
| Point16 | Dynamic | 44,564 | 68.01 | 0.995 | 11.33 | 8,830 |
| Point16 | Target4 | 49,030 | 81.68 | 0.830 | 6.85 | 14,588 |
| Point16 | Target6 | 48,910 | 79.96 | 0.866 | 7.55 | 13,243 |
| Point16 | Target8 | 48,088 | 74.88 | 0.862 | 8.40 | 11,899 |

## Paired changes versus Dynamic

| Workload | Candidate | Throughput change / wins | CPU/Put change | p99 change | Puts/Sync change | Target hits / syncs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point8 | Target4 | **+3.03% / 5 of 5** | +6.25 μs | −13.03% | −3.42 | 61,763 / 116,709 (52.9%) |
| Point8 | Target6 | +2.30% / 3 of 5 | +2.11 μs | −7.22% | −1.90 | 33,163 / 86,150 (38.5%) |
| Point8 | Target8 | +1.71% / 3 of 5 | −0.63 μs | +0.90% | +0.01 | 0 / 64,945 (0%) |
| Point16 | Target4 | **+10.02% / 5 of 5** | +14.44 μs | **−16.65%** | −4.47 | 57,959 / 73,009 (79.4%) |
| Point16 | Target6 | +9.75% / 5 of 5 | +11.64 μs | −13.02% | −3.78 | 49,686 / 66,127 (75.1%) |
| Point16 | Target8 | +8.80% / 5 of 5 | +6.68 μs | −15.26% | −2.93 | 37,976 / 59,432 (63.9%) |

Target hits are the sum over five runs. The paired throughput and p99 changes
are medians of within-repetition changes. A negative p99 change means lower
latency; a negative Puts/Sync change means smaller durability groups.

## Verification, archive, and cleanup

- `summary.json`: `complete: true`, 40 planned and 40 completed runs.
  `remote-exit.json`: `failed: false`, 40 runs started and attempted.
- The strict ready-target scan verifier passed: all scheduled run IDs have
  successful results and matching metadata, target settings and counters match
  the plan, and the comparison was recomputed from the verified run records.
- Transferred archive: 53,341 bytes, 300 files. Remote and local SHA-256 both
  match:
  `3ea225680f985b1f99977ee09898c00dc15ae4d8eb7a35d50d3e9e25f4d787e9`.
- The archive, checksum sidecar, extracted raw results, schedule, and plan are
  retained locally under
  `bench-results/gcp-v8-ready-target-scan-e3b-20261125-zone-b-run/`; raw data
  is not committed.
- A preceding placement attempt in `us-central1-a` could not allocate the C4
  plus Local SSD capacity. It ran no benchmark work and left no resources. The
  completed run used `us-central1-b`.
- Cleanup verification confirmed the VM, boot disk, temporary SSH firewall,
  isolated VPC, subnet, and egress route are absent. The temporary Terraform
  state and local cloud credentials were removed. The default VPC was not used
  or changed.

The scan commit only expanded the accepted ready-target values; this report
adds no runtime changes. Target4 is a Point16 candidate, but it missed the
Point8 throughput gate; do not enable it by default based on this scan alone. A
broader comparison would need to include Point1, Batch64, and a same-host V7
reference.
