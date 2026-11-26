# RFC 028 V8 D2 same-host GCP qualification — Legacy vs Candidate Dynamic/Target8 vs V7, 2026-11-26

## Result

Both Candidate configurations (`v8_candidate_dynamic` and `v8_candidate_target8`)
substantially outperformed `v8_legacy` on Point throughput across all writer counts:

- **Point1**: `v8_candidate_dynamic` reached **8,525 puts/s** (**+133.73%**, 5/5 pair wins, **-25.01%** CPU/Put, **-12.39%** p99 vs Legacy) and `v8_candidate_target8` reached **8,454 puts/s** (**+131.56%**, 5/5 pair wins, **-24.98%** CPU/Put, **-2.53%** p99 vs Legacy), both exceeding `v7_parallel` (6,254 puts/s).
- **Point8**: `v8_candidate_dynamic` (**+61.43%**) and `v8_candidate_target8` (**+63.40%**) reached parity with `v7_parallel` (34,536 / 34,958 vs 35,006 puts/s) with lower CPU/Put than Legacy (-10.87% / -12.03%).
- **Point16**: `v8_candidate_target8` reached **48,788 puts/s** (**+58.87%** vs Legacy, **+7.56%** vs `v8_candidate_dynamic` with 5/5 pair wins, **+2.40%** vs `v7_parallel` with 5/5 pair wins) and **0.815 ms** p99 (**-11.30%** vs Dynamic, +1.70% vs `v7_parallel`), at **+7.42 μs/Put (+10.87%)** CPU/Put relative to Dynamic (`75.65` vs `67.98` μs/Put). Target8 recorded **38,107** total hits at Point16 (median **7,652** per run) and **0** hits at Point1, Point8, and Batch64.
- **Batch64/w16**: `v8_candidate_dynamic` (631,481 puts/s, **-0.48%**) and `v8_candidate_target8` (628,404 puts/s, **-1.09%**) passed the `>= 98%` throughput gate relative to `v8_legacy` (633,725 puts/s) with slightly lower CPU/Put (-1.51% / -3.02%) and zero Stage A or Target8 hits.
- **Automated Gate Summary**: `point1_throughput_vs_legacy_ge_98pct = true`, `batch64_throughput_vs_legacy_ge_98pct = true`, `point16_target8_hits_observed = true`, while `point_p99_vs_legacy_le_105pct = false` because `v8_legacy` operates at 38–40% lower Point8/Point16 throughput (21,390 / 30,415 puts/s) with lower queue depth (0.541 / 0.747 ms p99). Because of that gate, `candidate_dynamic_qualified` and `candidate_target8_qualified` both report `false` in `summary.json`.

## Experiment

- Source commit: `a3a2886550456df62e85d9b52b979528b27410b2`
- Release `write-perf` SHA-256:
  `7cae99dc1927f73308e7afd11a84a8d0295070e3a93be03086b6c6458a6999dd`
- Host: GCP `c4-standard-16-lssd`, `us-central1-c`, 16 vCPUs
- Storage: two 375 GiB NVMe Local SSDs, RAID0, ext4, mounted at `/mnt/gcp-wal-bench`.
- Schedule: `v8_legacy`, `v8_candidate_dynamic`, `v8_candidate_target8`, and `v7_parallel`; five counterbalanced repetitions for Point1, Point8, Point16, and Batch64/16 writers (80 runs total).
- Workload: 100,000 operations per run, 1 KiB Point values, 64 entries per Batch64 operation. Point latency was sampled every 10 puts; Batch64 latency was not sampled.
- Observed I/O & configuration:
  - `v8_legacy`: Eager progress, Relay completion, Buffered FRONTIER (`data_o_direct=true`, `frontier_o_direct=false`, `sync_o_direct=false`), `sliding-fallocate` (`initialized_bytes=0`), Stage A off (`0` hits), Dynamic coalescing (`0` Target8 hits).
  - `v8_candidate_dynamic`: `TOYKV_WAL_V8_PROFILE=candidate`, `TOYKV_WAL_V8_COALESCING=dynamic`, Cursor progress, Direct completion, Direct FRONTIER (`data_o_direct=true`, `frontier_o_direct=true`, `sync_o_direct=false`), `sliding-point-lookahead` (`initialized_bytes=413,138,944` at Point16), Stage A (`100,000` hits for Point, `0` for Batch64), `0` Target8 hits.
  - `v8_candidate_target8`: `TOYKV_WAL_V8_PROFILE=candidate`, `TOYKV_WAL_V8_COALESCING=point-target8`, identical to `v8_candidate_dynamic` except `coalesce_target_policy=point_only` (`7,652` median Target8 hits at Point16, `0` at Point1/Point8/Batch64).
  - `v7_parallel`: V7 Parallel (`data_o_direct=false`, `frontier_o_direct=false`, `sync_o_direct=false`).
- Archive verification: `bundle_checksum_verified=true` (`4324c5d4a1bbc3ec9f0726d9d4c7f86f316478aebee30f68f773049478d874a8`), `summary.complete=true` (80/80), `remote-exit.json` (`failed=false`, `runs_started=80`), `verify_v8_d2_qualification_results_directory` passed with zero errors. All temporary cloud resources (VM, boot disk, temporary SSH firewall rule, isolated VPC, subnet, and egress route) were deleted and verified absent.

## Per-arm medians

Throughput is puts/s, CPU is μs/Put, p99 is milliseconds, and sync count is per 100,000 operations.

| Workload | Arm | Throughput | CPU/Put | p99 | Puts/Sync | Sync count |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Point1 | V8 Legacy | 3,671 | 70.70 | 0.455 | 1.00 | 100,000 |
| Point1 | V8 Candidate Dynamic | 8,525 | 53.54 | 0.399 | 1.00 | 100,000 |
| Point1 | V8 Candidate Target8 | 8,454 | 53.07 | 0.443 | 1.00 | 100,000 |
| Point1 | V7 Parallel | 6,254 | 72.23 | 0.277 | 1.00 | 100,000 |
| Point8 | V8 Legacy | 21,390 | 53.89 | 0.541 | 7.98 | 12,532 |
| Point8 | V8 Candidate Dynamic | 34,536 | 47.52 | 0.781 | 7.74 | 12,916 |
| Point8 | V8 Candidate Target8 | 34,958 | 47.52 | 0.748 | 7.75 | 12,907 |
| Point8 | V7 Parallel | 35,006 | 40.85 | 0.475 | 7.68 | 13,020 |
| Point16 | V8 Legacy | 30,415 | 70.34 | 0.747 | 15.75 | 6,348 |
| Point16 | V8 Candidate Dynamic | 45,376 | 67.98 | 0.923 | 11.22 | 8,916 |
| Point16 | V8 Candidate Target8 | 48,788 | 75.65 | 0.815 | 8.40 | 11,909 |
| Point16 | V7 Parallel | 47,569 | 61.14 | 0.811 | 11.99 | 8,337 |
| Batch64/16 | V8 Legacy | 633,725 | 4.86 | — | 543.48 | 184 |
| Batch64/16 | V8 Candidate Dynamic | 631,481 | 4.78 | — | 512.82 | 195 |
| Batch64/16 | V8 Candidate Target8 | 628,404 | 4.72 | — | 518.13 | 193 |
| Batch64/16 | V7 Parallel | 626,104 | 5.02 | — | 621.12 | 161 |

## Paired changes

| Workload | Comparison | Throughput change / wins | p99 change | CPU/Put delta (μs / %) |
| --- | --- | ---: | ---: | ---: |
| Point1 | Candidate Dynamic vs Legacy | +133.73% / 5 of 5 | -12.39% | -17.68 / -25.01% |
| Point1 | Candidate Target8 vs Legacy | +131.56% / 5 of 5 | -2.53% | -17.80 / -24.98% |
| Point1 | Candidate Target8 vs Dynamic | -0.77% / 2 of 5 | +12.24% | -0.65 / -1.22% |
| Point8 | Candidate Dynamic vs Legacy | +61.43% / 5 of 5 | +50.63% | -5.82 / -10.87% |
| Point8 | Candidate Target8 vs Legacy | +63.40% / 5 of 5 | +33.77% | -6.45 / -12.03% |
| Point8 | Candidate Target8 vs Dynamic | +1.22% / 5 of 5 | -9.70% | +0.11 / +0.24% |
| Point16 | Candidate Dynamic vs Legacy | +47.70% / 5 of 5 | +22.77% | -2.07 / -2.94% |
| Point16 | Candidate Target8 vs Legacy | +58.87% / 5 of 5 | +9.79% | +5.06 / +7.31% |
| Point16 | Candidate Target8 vs Dynamic | +7.56% / 5 of 5 | -11.30% | +7.42 / +10.87% |
| Point16 | Candidate Target8 vs V7 Parallel | +2.40% / 5 of 5 | +1.70% | +14.35 / +23.41% |
| Batch64/16 | Candidate Dynamic vs Legacy | -0.48% / 1 of 5 | — | -0.07 / -1.51% |
| Batch64/16 | Candidate Target8 vs Legacy | -1.09% / 1 of 5 | — | -0.15 / -3.02% |
| Batch64/16 | Candidate Target8 vs Dynamic | -0.52% / 1 of 5 | — | -0.10 / -2.03% |
