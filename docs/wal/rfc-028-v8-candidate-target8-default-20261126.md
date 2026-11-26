# RFC 028 Closeout: V8 Candidate Target8 Default Promotion

- **Date**: 2026-11-26
- **Status**: Promoted as default (`WalV8Profile::Candidate` + `PointOnly(8)`)

## 1. Promoted Default Configuration

`WalV8Profile` and `WalV8RequestedConfig::default()` now default to `Candidate` (`PointOnly(8)`):

| Component | Default Setting | Fallback / Override |
| --- | --- | --- |
| Progress | `Cursor` | `TOYKV_WAL_V8_PROFILE=legacy` (`Eager`) |
| DATA Completion | `Direct` | `TOYKV_WAL_V8_PROFILE=legacy` (`Relay`) |
| FRONTIER | `Direct` (with `Buffered` fallback if DATA is not `O_DIRECT`) | `TOYKV_WAL_V8_FRONTIER_MODE=buffered` |
| Extent Preparation | `PointOnlyLookahead` (1 MiB, on eligible ext-family `O_DIRECT` files) | `TOYKV_WAL_V8_PROFILE=legacy` (`FallocateOnly`) |
| Stage A Preallocation | Enabled (`Point` single-frame only) | `TOYKV_WAL_V8_PROFILE=legacy` (Off) |
| Coalescing | `Dynamic` (100/400 μs) + `PointOnly(8)` | `TOYKV_WAL_V8_COALESCING=dynamic` |
| Durability Driver | `CallerDriven` | — |
| ACK | `Legacy` | — |

`TOYKV_WAL_V8_PROFILE=legacy` remains supported as a full profile fallback, and `TOYKV_WAL_V8_COALESCING=dynamic` remains supported as an independent coalescing fallback.

## 2. Qualification Evidence & Gate Decision

### 2.1 80-Run Saturated Qualification (`c4-standard-16-lssd`, 2× NVMe RAID0 ext4)

Report: [rfc-028-v8-d2-qualification-gcp-20261126.md](benchmarks/rfc-028-v8-d2-qualification-gcp-20261126.md) (commit `a3a2886550456df62e85d9b52b979528b27410b2`, 80/80 verified):

- **Point1**: Target8 **8,454 puts/s** (+131.56% vs Legacy, -24.98% CPU/Put, -2.53% p99); Dynamic **8,525 puts/s** (+133.73%).
- **Point8**: Target8 **34,958 puts/s** (+63.40% vs Legacy, parity with V7 35,006 puts/s); Dynamic **34,536 puts/s** (+61.43%).
- **Point16**: Target8 **48,788 puts/s** (+58.87% vs Legacy, +7.56% vs Dynamic, +2.40% vs V7 47,569 puts/s), **0.815 ms** p99 (vs V7 0.811 ms), **75.65 μs** CPU/Put (+7.42 μs vs Dynamic's 67.98 μs).
- **Batch64/w16**: Target8 **628,404 puts/s** (-1.09% vs Legacy, >= 98% gate passed, zero Stage A or Target8 hits); Dynamic **631,481 puts/s** (-0.48%).

### 2.2 Acceptance of Saturated P99 Gate (`point_p99_vs_legacy_le_105pct = false`)

The unpaced 80-run `point_p99_vs_legacy_le_105pct` gate failed (`v8_legacy` ran at 21,390 / 30,415 puts/s with 0.541 / 0.747 ms p99 vs Candidate Target8 at 34,958 / 48,788 puts/s with 0.748 / 0.815 ms p99). To test tail latency at matched offered load without coordinated omission, we ran the 18-run equal-throughput suite ([rfc-028-v8-equal-throughput-gcp-20261126.md](benchmarks/rfc-028-v8-equal-throughput-gcp-20261126.md), commit `bd7ebf4b`, 18/18 verified):

- **Point8 @ 20,000 puts/s**: `v8_legacy` fell behind schedule (`e2e P99 = 111.666 ms`, `lag P99 = 111.299 ms`), while `v8_candidate_target8` (`2.393 ms`, `svc P99 = 0.597 ms`) and `v8_candidate_dynamic` (`0.824 ms`, `svc P99 = 0.613 ms`) sustained the load cleanly.
- **Point16 @ 28,000 puts/s**: `v8_candidate_target8` achieved **`e2e P99 = 0.793 ms`** (~81% lower than `v8_legacy`'s `4.149 ms` and ~85% lower than `v8_candidate_dynamic`'s `5.201 ms`; `svc P99 = 0.678 ms`, `2,949` median Target8 hits).

Based on the saturated throughput/latency parity with V7 (`48.8k` puts/s, `0.815 ms` P99) and the Point16 equal-throughput P99 improvement (`0.793 ms` vs `4.149 ms` Legacy / `5.201 ms` Dynamic), we accept the saturated P99 gate waiver and promote **Candidate Target8** as the default V8 profile, retaining `Dynamic` as the explicit fallback.

### 2.3 Safety & Functional Verification

- **Non-bench & ext4**: Default-feature (`--lib`) and `--all-features` tests run on local ext4 (`wal::v8::leader::tests`, `lsm_storage::tests::v8_candidate_*`, `v8_pitr_rotation_inherits_requested_config_and_reopen_reparses`) verify Cursor, Direct completion, Direct FRONTIER + Buffered fallback, Point-only Lookahead eligibility, Stage A preallocation, rotation inheritance, and reopen re-parsing. Real `initialized_bytes > 0` on ext4 RAID0 is verified in the GCP D2 qualification (`413,138,944` / `405,798,912` bytes at Point16).
- **ext4 Crashpoint**: `lsm_storage::tests::failpoint_v8_candidate_profile_crash_recovery` verifies Candidate profile abrupt-exit crash recovery on ext4, and `wal::v8::leader::tests::failpoint_parallel_writer_crashes_at_durability_boundaries` verifies durability-boundary crash recovery.
- **PITR Seal Quiesce**: `lsm_storage::tests::v8_pitr_seal_quiesces_extent_preparation_and_keeps_sealed_length` and `backup::tests::pitr_restore_cancellation_discards_the_staging_directory` verify that PITR sealing quiesces background extent preparation before truncating/syncing the sealed predecessor so its file length remains equal to `boundary.sealed_end`.
- **Mixed Workload**: `wal::v8::leader::tests::v8_candidate_mixed_target8_falls_back_to_dynamic_on_real_admission` verifies that concurrent Point + Batch admissions fall back to Dynamic coalescing without Target8 early exit.
- **PITR / Recovery**: `v8_candidate_profile_point_mixed_and_rotations` and `v8_candidate_profile_auto_pitr_rotation_inherits_config` verify PITR barriers, manual/automatic rotations, and full data recovery.

## 3. RFC 029 Phase 1 Targets (Point Durability CPU Optimization)

Target8's primary remaining gap is concurrent Point CPU cost (`75.65 μs/Put` vs V7's `61.14 μs/Put`). RFC 029 Phase 1 targets:

| Point16 Metric | RFC 028 Target8 Baseline | RFC 029 Phase 1 Target |
| --- | ---: | ---: |
| CPU/Put | 75.65 μs | <= 60 μs |
| Throughput | 48,788 puts/s | >= 48,788 puts/s |
| P99 | 0.815 ms | <= 0.815 ms |
| Batch64 | 628,404 puts/s | No regression |
