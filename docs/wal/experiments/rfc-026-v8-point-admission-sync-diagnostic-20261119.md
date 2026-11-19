# V7/V8 Point Admission and Sync Diagnostic

- Source commit: `dcb32e3d3aba9156d605bf93624f4d2bccd49515`
- Diagnostic patch SHA-256: `c903e2df2b91d2770e5552853e5176eb3b90a605288f8c8ebbfb423ae7ba1ac5`
- Source archive SHA-256: `9caff3114615a094b2915014f935060314545194b5defbb213cbb7074590b40e`
- Host: `c4-standard-16-lssd`, 16 vCPU, Local SSD RAID0, ext4
- Workload: 100,000 Point puts, 1 KiB values; one instrumented run per arm/writer cell
- Archive coverage: 8/8 cells (1, 4, 8, and 16 writers × V7/V8); the detailed lock table below shows only the 8- and 16-writer cells
- Artifact bundle SHA-256: `338266de7deac26e21ee25e967ccfc85dae16616b5d1ab4f806a4cc78ba1e23b`; checksum metadata retained
- Diagnostic only; these are not uninstrumented throughput scores. Each cell ran once.

## Results

| Writers | WAL | puts/s | CPU μs/put | syncs | puts/sync | lock wait p50/p95/p99 μs | estimated lock hold p50/p95/p99 μs | estimated lock utilization | V8 tickets covered mean/p50/p95 |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | v7_parallel | 36.03k | 40.15 | 13,016 | 7.68 | 0.31/10.62/37.31 | 2.38/5.01/12.85 | 9.9% | — |
| 8 | v8_point_lookahead_direct | 30.80k | 55.11 | 13,087 | 7.64 | 11.66/78.41/102.53 | 3.33/11.56/21.17 | 16.7% | 7.69/8/8 |
| 16 | v7_parallel | 49.29k | 59.38 | 8,242 | 12.13 | 0.38/59.94/104.69 | 2.69/10.30/18.65 | 20.7% | — |
| 16 | v8_point_lookahead_direct | 41.95k | 80.76 | 10,179 | 9.82 | 43.66/164.87/236.76 | 3.65/19.68/42.02 | 29.0% | 8.74/9/16 |

The archive contains four additional cells, omitted from the lock-detail table because that comparison was focused on 8 and 16 writers:

| Writers | WAL | puts/s | CPU μs/put | syncs | puts/sync |
|---:|---|---:|---:|---:|---:|
| 1 | v7_parallel | 6.51k | 69.42 | 100,000 | 1.00 |
| 1 | v8_point_lookahead_direct | 7.39k | 62.70 | 100,000 | 1.00 |
| 4 | v7_parallel | 22.00k | 38.30 | 25,038 | 3.99 |
| 4 | v8_point_lookahead_direct | 20.57k | 45.18 | 25,151 | 3.98 |

Thus “eight-run archive” means all eight writer/version combinations, not eight repetitions of the four rows in the lock-detail table. Each cell is still a single diagnostic run.

Lock hold is estimated by adding sampled `commit_setup_wall_ns` and `admission_wall_ns`, then extrapolating their mean across the measured write-phase duration. It is not a direct aggregate lock-hold counter and omits small timer gaps.

## Interpretation

- At 8 and 16 writers, V8 sequencer wait is much higher than V7. V8 sampled wait-thread CPU p50 is only 2.85 μs and 3.46 μs, versus wall p50 11.66 μs and 43.66 μs: most wait is queueing/scheduling rather than active CPU.
- Estimated lock utilization is 16.7% for V8/8 and 29.0% for V8/16, versus 9.9% and 20.7% for V7. Lock contention is real, but this estimate does not support a continuously saturated lock as the sole throughput cap.
- At 8 writers, puts/sync is nearly equal (7.68 V7, 7.64 V8). At 16 it is lower for V8 (9.82 vs 12.13); sampled V8 advances cover mean 8.74 tickets (p50 9, p95 16).
- The per-sync V7 admitted/frames-ready snapshot was not serialized: the runner helper omitted `--wal-sync-diagnostics`. Therefore the cause of the 16-writer group-size difference remains open. A corrected repeat did not start before the VM safe cutoff.
- In the archived `dcb32e3d` run, the source commit itself had no WAL changes and the diagnostic instrumentation was a temporary patch. That exact patch is now retained below with its checksum. This PR now includes bench-only V7/V8 sync-decision snapshots and a runner that passes `--wal-sync-diagnostics`; these measurements do not change production WAL behavior. The archived invocation omitted the flag, so it still has no serialized decision snapshots.

## Sync-observation gap

The V7 per-sync admitted-ticket and readiness snapshots were not serialized. The runner command omitted `--wal-sync-diagnostics`, so no per-sync decision snapshots were emitted for either arm. V8's sampled `tickets_covered` values were present, but they do not provide the matching V7 decision-time state needed to explain the smaller V8 groups at 16 writers.

A corrected rerun did not start before the VM's conservative lifetime cutoff. No corrected per-sync data is claimed here. The exact diagnostic patch associated with this attempt is now retained at [`rfc-026-v8-admission-sync-diagnostics-dcb32e3d.patch`](../benchmarks/artifacts/rfc-026-v8-admission-sync-diagnostics-dcb32e3d.patch), with SHA-256 `c903e2df2b91d2770e5552853e5176eb3b90a605288f8c8ebbfb423ae7ba1ac5`. This preserves the attempted patch; it does not change the fact that the old invocation omitted the enabling flag.

The corrected runner now passes `--wal-sync-diagnostics`, validates that every V7/V8 result contains nonempty successful per-sync snapshots, and saves them as `sync-observations.json` in each run directory. The V7 snapshot is taken from the actual V7 Parallel runtime and records admitted, frame-ready, DATA-complete, and sync-ready contiguous prefix lengths plus the first later ticket's readiness. V8 records the corresponding prefixes from its coordinator snapshot. Those fields are new instrumentation and are not retroactively inferred for this archive.

## Cleanup

After local artifact verification, the VM, boot disk, temporary SSH firewall rule, isolated VPC, subnet and route were deleted. GCP inventory confirmed each resource absent, and the saved Terraform state is empty.
