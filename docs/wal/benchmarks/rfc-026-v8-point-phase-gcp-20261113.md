# GCP V7/V8 Point and Batch16 Pipeline Diagnostic

## Run configuration

- Source commit: `fb51a88a97be087361285a35bb5a6fad58109d30` (merged PR #549 baseline).
- Machine: `c4-standard-16-lssd`, 16 writers, two NVMe Local SSDs in RAID0. The benchmark ran on Linux kernel `7.0.0-1017-gcp` (`#19~24.04.1-Ubuntu`, x86_64). The benchmark filesystem was `/dev/md127` mounted at `/mnt/gcp-wal-bench` as ext4 with `rw,noatime,stripe=256`.
- Workloads: Point (100,000 puts) and Batch16 (500,000 puts); one V7 and one V8 run for each, arm order counterbalanced.
- V8 only: `sliding-fallocate`, 1 MiB window, validated from runtime metadata; DATA `O_DIRECT=true`, FRONTIER and sync handles buffered.
- No CPU pinning, perf capture, or Hotpath sampling. V8 sync/HOL/completion-timeline diagnostics were enabled.
- Isolated runner helper SHA-256: `2c7a9ec9b7782dbe32703f0e353e4a6f2f7704397547283513507a69022992b7`; runner patch SHA-256: `49c9ccc78e4dad3b2a4eae6e169aa1f818fd0bcd3af3f530c55a288d44d23eef`. The patch and raw per-run artifacts remain local; this PR contains the summary only.

## Diagnostic results

| Workload | V7 puts/s | V8 Sliding puts/s | V8/V7 | V7 CPU / syncs | V8 CPU / syncs | Sampled p99 V7 / V8 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| point | 48.04k | 30.09k | 0.626 (-37.4%) | 5.92s / 8128 | 7.82s / 6337 | 0.814 / 1.581 ms |
| batch16 | 260.95k | 363.85k | 1.394 (+39.4%) | 4.34s / 4061 | 4.61s / 1993 | 2.296 / 1.200 ms |

Rates are diagnostic, not throughput qualification: V8 had additional per-sync/HOL/timeline instrumentation that V7 did not. Sampled p99 values use workload-specific sampling and are not stable tail-latency estimates.

## Point: Sliding Fallocate versus no preparation

- V8 Point increased from 19.44k to 30.09k puts/s (+54.8%) across the two diagnostic runs; CPU fell from 12.04s to 7.82s and sync count fell from 7496 to 6337.
- Runtime metadata confirmed 416 `fallocate` calls, 416 MiB allocated, 20.66 ms cumulative preparation time, and 0 preparation waits.
- Cumulative DATA Completion HOL ticket-time fell from 4.545s across 26,847 counted tickets to 12.713ms across 1,447; the sampled Submit→CQE-observed p50 fell from 262.6µs to 42.1µs.
- This is strong directional evidence that extent preparation improves the DATA completion path, but each arm ran once on a separate disposable VM; this does not establish a repeatable effect size.

## V8 Point phase measurements

Point V8 contains 6337 sync observations and 782 completion-timeline samples (0 dropped).

| Phase | p50 | p95 |
| --- | ---: | ---: |
| DATA completion → FRONTIER start | 21.3 µs | 32.5 µs |
| FRONTIER write | 3.2 µs | 6.8 µs |
| fdatasync | 163.8 µs | 189.3 µs |
| FRONTIER write + fdatasync | 167.5 µs | 194.3 µs |
| Coalescing wait | 159.9 µs | 252.3 µs |
| Tickets per durability advance | 16.0 tickets | 16.0 tickets |

Sampled completion timeline (segments are not one matched end-to-end path):

| Segment | p50 | p95 |
| --- | ---: | ---: |
| Submit → CQE observed | 42.1 µs | 90.3 µs |
| CQE → group finished | 0.8 µs | 2.8 µs |
| Group finished → result send | 1.8 µs | 3.8 µs |
| Result send → Reaper | 9.2 µs | 21.0 µs |
| Reaper → prefix ready | 1.5 µs | 6.1 µs |
| Prefix ready → durable | 269.8 µs | 406.2 µs |

## Interpretation

- Sliding Fallocate removed most measured DATA completion HOL and reduced Submit→CQE latency, with no producer wait attributed to preparation. This lowers the priority of DATA submission reordering on this prepared path.
- Point V8 still trails V7 in this diagnostic capture despite fewer syncs; V8 averaged 15.8 tickets per durability advance. Further work should focus on the Point ACK/durability path and CPU cost, then verify with repeated low-overhead matched runs before changing runtime behavior.
- Batch16 continues to favor V8 in this single capture, consistent with the earlier matrix; this is not a substitute for repeated qualification.

## Cleanup

- Benchmark summary reports 4/4 successful cells and complete result collection. A preceding complete diagnostic run omitted the Sliding Fallocate setting; it is used only as the single-run unprepared comparator. A separate earlier artifact copy hit `/tmp` quota and is not used; the corrected artifacts were collected under the workspace results directory.
- Terraform first could not create its state lock in quota-limited `/tmp`; state was relocated to `/var/tmp`, destroy ran with locking enabled, and the state was empty before the temporary state copy was removed.
- VM, auto-delete boot disk, temporary SSH rule, egress route, subnet, and isolated VPC were verified absent; Terraform state is empty. See `cleanup-verification.json` and `lifecycle.json`.
- Billing/API/quota settings were not changed. The run used the existing Free Trial attestation and a $1.50 maximum-estimated-spend guard under the previously authorized $2 ceiling.

## Follow-up: low-overhead V8 FRONTIER/coalescing comparison

The follow-up uses the current branch source commit `ac076aa64d171ccfd434d4c935b3ebad4823e546`. It isolates the four FRONTIER/coalescing configurations proposed above while keeping V8 Parallel and 1 MiB Sliding Fallocate fixed.

### Run configuration

- GCP `c4-standard-16-lssd`, 16 writers, 100,000 Point puts per run, 1 KiB values; five counterbalanced blocks, 20 runs total.
- V8 `sliding-fallocate`, 1 MiB window, in all arms.
- No `perf`, Hotpath, per-sync diagnostics, or completion timeline. The `bench` feature's aggregate WAL counters remained enabled. Latency sampling was every 100 operations; p99 is a sampled estimate.
- Kernel `7.0.0-1017-gcp`; ext4 on `/dev/md127`, mounted at `/mnt/gcp-wal-bench` with `rw,noatime,stripe=256`.
- All 20 runs reported stable WAL handle metadata: DATA `O_DIRECT=true`, Sync buffered, FRONTIER mode matching the arm, and all three handles on the same WAL inode. Sliding Fallocate mode and the 1 MiB window were confirmed in every run.

### Results

Medians across five runs per arm:

| FRONTIER / coalescing | Throughput (k puts/s) | Change vs A | p50 (ms) | p95 (ms) | Sampled p99 (ms) | Sync count | Process CPU (s) | FRONTIER write + `fdatasync` (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A: Buffered / Dynamic | 31.84 | — | 0.500 | 0.674 | 0.851 | 6,332 | 7.07 | 1.054 |
| B: Buffered / 100 μs | 30.82 | −3.23% | 0.494 | 0.786 | 1.656 | 9,467 | 7.77 | 1.612 |
| C: Direct / Dynamic | 33.30 | +4.56% | 0.460 | 0.671 | 1.245 | 8,162 | 7.41 | 1.171 |
| D: Direct / 400 μs | 33.81 | +6.16% | 0.464 | 0.611 | 0.842 | 6,321 | 7.12 | 0.883 |

The durability column is the median, across runs, of each run's aggregate FRONTIER write time plus aggregate `fdatasync` time. It is not wall-clock time and overlaps other work.

Paired throughput changes by block:

- D vs A: median **+6.81%**, positive in **5/5** blocks.
- C vs A: median **+5.79%**, positive in **5/5** blocks.
- D vs C: median **+1.30%**, positive in **4/5** blocks. The additional gain from fixing the budget at 400 μs over Direct/Dynamic was modest in this run.
- B vs A: median **−3.23%**, positive in **1/5** blocks; B's median Sync count was about **49.5%** higher.

Direct/400 reduced median aggregate FRONTIER-write-plus-Sync time about **16.2%** versus Buffered/Dynamic while keeping the Sync count nearly unchanged. Its sampled p50/p95 were lower than A, while sampled p99 was effectively unchanged. Five runs do not qualify a stable tail-latency improvement. Per-sync dynamic budget-choice counts were not collected, so the comparison measures end-to-end results rather than how often the dynamic policy selects each budget.

### Conclusion and cleanup

Direct FRONTIER improved throughput over Buffered/Dynamic in all five paired blocks for this 16-writer Point workload. Direct/400 had the highest median throughput, but its incremental paired gain over Direct/Dynamic was small. This supports keeping Direct/400 as a candidate for this workload; it does not qualify it as a general default for other writer counts or workloads.

All 20 runs passed and their JSON parameters and I/O metadata were independently checked. The aggregate report is included here; raw per-run JSON remains local and no archive was uploaded. The VM, auto-delete boot disk, temporary SSH firewall, route, subnet, and isolated VPC were verified absent after cleanup. The run used the existing Free Trial attestation and a $1.50 estimated-spend guard; billing, APIs, and quotas were not changed.
