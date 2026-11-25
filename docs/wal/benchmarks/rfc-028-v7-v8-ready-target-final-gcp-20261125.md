# RFC 028 V7/V8 final same-host GCP comparison — Ready Target4/8, 2026-11-25

## Result

Target4 brought Point16 throughput slightly above V7 in the paired median, but
the gain was only 2.22% with 3/5 pair wins. Point8 remained 1.48% below V7,
and sampled p99 was higher for every V8 Point arm. Target8 was effectively tied
with V7 at Point16 but lost at Point8. No V8 policy matched V7 across Point
throughput, tail latency, and CPU cost.

Batch64 remained close to V7: all V8 arms were within 0.9% paired throughput,
and CPU/Put was slightly lower. This comparison does not qualify Target4 or
Target8 as defaults; Dynamic remains the default.

## Experiment

- Source commit: `21c83832e91ed23b2f8da977f3e9fe089ae1fa41`
- Release `write-perf` SHA-256:
  `40b8645e59edd0d6874a1a90ce775ba1fa4ebf8732e54d89796a993115aa5f6f`
- Host: GCP `c4-standard-16-lssd`, `us-central1-c`, 16 vCPUs, Intel Xeon
  6985P-C
- Storage: two 375 GiB NVMe Local SSDs, RAID0 `/dev/md127`, ext4 with
  `noatime,stripe=256`, mounted at `/mnt/gcp-wal-bench` (749.7 GiB usable).
- Schedule: V7 Parallel, V8 Dynamic, V8 Target4, and V8 Target8; five
  counterbalanced repetitions for Point1, Point8, Point16, and Batch64/16
  writers: 80 runs total.
- Workload: 100,000 operations per run, 1 KiB Point values, 64 entries per
  Batch64 operation. Point latency was sampled every 10 puts; Batch64 latency
  was not sampled.
- V8 configuration: Cursor progress mode 2, Direct Completion, CallerDriven
  durability, Legacy ACK, direct FRONTIER, ordered DATA submission, 1 MiB
  sliding Point lookahead, singleton submission off. Stage A buffer
  preallocation was enabled for Point only. All three V8 Point arms recorded
  100,000 preallocation hits and zero fallbacks per run; Batch64 recorded zero
  attempts.
- The target was unset for Dynamic and set to 4 or 8 for the other V8 arms.
  Detailed profiling, CPU attribution, progress counters, and worker histogram
  were disabled. V8 DATA and FRONTIER used `O_DIRECT`; sync did not. V7 DATA,
  FRONTIER, and sync were buffered.
- Runtime and WAL behavior were held constant across arms. The corrected final
  runner revision checked out the source commit above and built one release
  binary for all 80 runs. An earlier partial attempt was excluded after its
  V7 record reached a V8-only live validator; the fix is included in the source
  commit above. A subsequent launcher retry stopped before scoring because the
  Local SSD was already mounted. The final matrix ran directly on the retained
  host without reinitializing the mounted device.

## Per-arm medians

Throughput is puts/s, CPU is μs/Put, p99 is milliseconds, and sync count is per
100,000 operations. These are medians across each arm's five runs. Paired
changes below are medians of within-repetition changes and may differ from the
ratios of per-arm medians.

| Workload | Arm | Throughput | CPU/Put | p99 | Puts/Sync | Sync count |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point1 | V7 Parallel | 6,004 | 70.38 | 0.350 | 1.00 | 100,000 |
| Point1 | V8 Dynamic | 7,746 | 52.85 | 0.529 | 1.00 | 100,000 |
| Point1 | V8 Target4 | 8,314 | 52.54 | 0.492 | 1.00 | 100,000 |
| Point1 | V8 Target8 | 8,321 | 52.34 | 0.444 | 1.00 | 100,000 |
| Point8 | V7 Parallel | 34,947 | 40.64 | 0.535 | 7.69 | 13,008 |
| Point8 | V8 Dynamic | 34,034 | 49.15 | 0.785 | 7.68 | 13,026 |
| Point8 | V8 Target4 | 33,476 | 55.46 | 0.680 | 4.24 | 23,575 |
| Point8 | V8 Target8 | 32,441 | 49.58 | 0.830 | 7.68 | 13,015 |
| Point16 | V7 Parallel | 48,899 | 60.11 | 0.774 | 12.14 | 8,236 |
| Point16 | V8 Dynamic | 44,682 | 68.86 | 0.997 | 11.38 | 8,788 |
| Point16 | V8 Target4 | 49,102 | 81.55 | 0.820 | 6.81 | 14,674 |
| Point16 | V8 Target8 | 48,718 | 75.07 | 0.841 | 8.40 | 11,911 |
| Batch64/16 | V7 Parallel | 620,884 | 5.19 | — | 598.80 | 167 |
| Batch64/16 | V8 Dynamic | 627,418 | 5.05 | — | 512.82 | 195 |
| Batch64/16 | V8 Target4 | 623,320 | 4.91 | — | 434.78 | 230 |
| Batch64/16 | V8 Target8 | 626,406 | 4.84 | — | 473.93 | 211 |

## Paired changes versus V7

Positive p99 means V8 latency is higher. Batch64 p99 was not sampled.

| Workload | V8 arm | Throughput change / wins | p99 change |
| --- | --- | ---: | ---: |
| Point1 | Dynamic | +27.52% / 5 of 5 | +69.67% |
| Point1 | Target4 | +34.79% / 5 of 5 | +47.03% |
| Point1 | Target8 | +36.18% / 5 of 5 | +45.99% |
| Point8 | Dynamic | −3.15% / 1 of 5 | +50.91% |
| Point8 | Target4 | −1.48% / 1 of 5 | +22.96% |
| Point8 | Target8 | −4.47% / 2 of 5 | +50.07% |
| Point16 | Dynamic | −8.63% / 1 of 5 | +25.93% |
| Point16 | Target4 | +2.22% / 3 of 5 | +5.95% |
| Point16 | Target8 | −0.44% / 0 of 5 | +12.03% |
| Batch64/16 | Dynamic | +0.86% / 4 of 5 | — |
| Batch64/16 | Target4 | +0.02% / 3 of 5 | — |
| Batch64/16 | Target8 | +0.53% / 4 of 5 | — |

Against V8 Dynamic, Target4 improved Point16 throughput by 9.81% (5/5 wins)
and reduced paired p99 by 15.86%; Target8 improved throughput by 9.29% (4/5)
and reduced paired p99 by 15.80%. At Point8, Target4 gained 1.72% (3/5) and
Target8 changed throughput by −0.17% (2/5). Target4 substantially increased
sync count and CPU/Put at Point16, so its within-V8 gain did not translate to a
stable overall win over V7.

## Archive and cleanup

- `summary.json`: complete; 80 planned, started, attempted, and completed runs.
  `remote-exit.json`: `failed: false`, 80 runs started.
- The strict result validator passed all 80 schedule IDs, per-run result and
  metadata pairs, environment settings, and recomputed comparisons.
- Plan SHA-256:
  `c7d8ad3c7a715ecbdf5ce808b6d62e6e03db81ada40e05e308f101b1c0f4ebf1`
- Schedule SHA-256:
  `0ecf3e59fe691648749fa322669d673c1723fa18144e06e64b150f0a993af758`
- Transferred archive: 109,987 bytes, 570 files. Remote and local SHA-256
  match:
  `d2bc289f1e6afa24cf7ea88684df7b895d2f3813398d580999de7a9f80c83e8e`.
  The archive and checksum metadata are retained locally under
  `bench-results/gcp-v7-v8-ready-target-final-21c83832-c-retries/run-1795626335668854390/`;
  raw run data is not committed.
- Host and Local SSD observations are saved beside the local archive; their
  `host-and-io.txt` SHA-256 is
  `a8e211aff8cd336307bf2d25eb67365e3d25f774177919681006d42699533c10`.
- The Free Trial run made no billing, API, or quota changes. After checksum and
  result verification, the VM, boot disk, temporary SSH firewall, isolated
  VPC, subnet, and egress route were deleted. Final GCP lookups confirmed all
  six resource classes absent.
