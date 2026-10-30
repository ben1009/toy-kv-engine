# RFC 025: current v4/v7 Parallel comparison, 2026-10-27

The fresh comparison completes all **36 fixed invocations** using one current executable. After separating obvious stalls as requested, there is insufficient matched data to establish a stable paired gap across these workloads. The best available unflagged scored observations from this exact executable still put v7 about **7–20% below v4 throughput**, depending on workload. That is a descriptive peak comparison, with separate cohorts and unequal sample counts; it does not qualify parity or establish a uniform source-level penalty.

The source stays at retained `d5d23e95`; no implementation, build, runtime, allocator or default changes are made. Canonical/saved executable SHA-256 is `e9abcc91b499c3cecd0acc137a0c9b8f65228b76ac3cb31ba3894bf9a0d06d40` for both formats. Existing matching 1,972-test/six-focused-test/full-check/release evidence is reused after verifying all 128 input hashes. There is no new correctness rerun for unchanged inputs.

## Best observations after excluding flagged runs

The maintainer reiterated “ignore the obvious stall result” during capture and had previously requested highest-score comparisons. The table therefore shows individually unflagged **scored** peaks from this fresh cohort plus the [earlier matching current-v7 cohort](rfc-025-v7-eligible-coalescing-20261027.md). Both use the exact executable above and matching settings. Warmups are excluded; p99 belongs to the listed peak-throughput run. This post-capture descriptive view is separate from the frozen fresh paired analysis, which remains unchanged.

| Workload | V4 puts/s | V7 puts/s | V7 throughput gap | Peak-run p99 v4 / v7, ms |
|---|---:|---:|---:|---:|
| Batch64, 1 writer | 85,857 | 68,426 | -20.3% | 1.176 / 1.349 |
| Batch64, 8 writers | 384,552 | 306,533 | -20.3% | 2.358 / 8.349 |
| Batch64, 16 writers | 584,858 | 544,147 | -7.0% | 3.130 / 4.730 |
| Point, 8 writers | 17,470 | 16,001 | -8.4% | 1.310 / 1.350 |

| Workload | Unflagged scored observations: v4 / v7 |
|---|---:|
| Batch64, 1 writer | 1 / 6 |
| Batch64, 8 writers | 2 / 3 |
| Batch64, 16 writers | 3 / 7 |
| Point, 8 writers | 3 / 6 |

The v7 batch8, batch16 and point8 winners come from the earlier cohort; the batch1 winner and all v4 winners are fresh. These earlier v7 observations are reused, not new invocations. Maximum selection over unequal sample counts can favor the more-sampled format. Passing the mean-I/O mask does not prove a run free of stalls or queueing. In particular, the batch8 peak's p99 is still much higher for v7. This table answers which best rates were observed, not how much slower v7 is under controlled steady-state storage conditions.

## What the fresh paired data support

| Workload | Combined-mask matched pairs | Sync-only matched pairs |
|---|---:|---:|
| Batch64, 1 writer | 1 | 1 |
| Batch64, 8 writers | 0 | 1 |
| Batch64, 16 writers | 2 | 3 |
| Point, 8 writers | 3 | 3 |

The combined mask flags a pair if either arm has mean fdatasync >=1.5 ms, or v7 has mean FRONTIER write >=1 ms. V4 has no FRONTIER write, so the additional criterion is asymmetric and could hide a real v7-specific cost. A symmetric sync-only view is preserved alongside it. Both are aggregate masks: they miss short outliers and DATA/preparation delays. The point runs illustrate that limit: all three pairs pass the mean-call thresholds, yet v7 p99 remains around 17 ms in this cohort, compared with about 1.35 ms in the earlier identical-executable cohort. This unresolved variation prevents a stable source attribution from those ratios.

As requested, flagged runs are not used as representative performance in the main interpretation. All original raw ratios, paired masks, per-format flags, fresh-only peaks, timing counters and resource usage remain in the [summary](artifacts/rfc-025-v7-current-v4-parallel-20261027-summary.json). No slow record is deleted, replaced or selectively retried. The [interpretation note](artifacts/rfc-025-v7-current-v4-parallel-20261027-interpretation.json) records the later user steering without changing the original plan.

## Configuration and capture

Both formats run `write-perf-v7-eligible-coalescing` with explicit Parallel I/O, fresh local ext4 databases, 1-KiB values, a 2-GiB SST target and no compaction. V4 uses ordinary WAL without PITR; selecting v7 enables PITR. The comparison therefore includes those supported configuration differences, beyond wire framing alone.

Batch64 writes 750,000 entries and samples every ten batches at 1/8/16 writers. Point8 writes 100,000 puts and samples every hundred. Batch16 has two warmups plus five alternating pairs; batch8, point8 and batch1 each have two warmups plus three alternating pairs. Odd pairs run v4 then v7, even pairs reverse. All four cells run regardless of ratios. There are 30-second initial cooldowns per cell and ten-second gaps. Host audits before, during at 1 Hz, and after each invocation see no recognized competing work. No build/test/source edit/profile or active SMART polling overlaps capture. Sampling cannot exclude all brief or unrecognized interference.

All 36 invocations remain: **28 scored and eight warmups**. Adding them to the preceding verified 489 gives **525 unique invocations** in this measurement lineage. The earlier v7 peak context is counted once. Scratch databases are removed; raw records, manifests, host audits and resource-usage logs remain. Source and canonical executable are unchanged. There is no new optimization, rollback, PR, push, adoption or full RFC qualification in this slice.

## Evidence

- [Frozen plan](artifacts/rfc-025-v7-current-v4-parallel-20261027-plan.json), [matrix](artifacts/rfc-025-v7-current-v4-parallel-20261027-matrix.json)
- [Executable identities](artifacts/rfc-025-v7-current-v4-parallel-20261027-binaries.json), [128-input manifest](artifacts/rfc-025-v7-current-v4-parallel-20261027-source-sha256.tsv), [environment](artifacts/rfc-025-v7-current-v4-parallel-20261027-environment.json)
- [Batch16](artifacts/rfc-025-v7-current-v4-parallel-20261027-ext4-batch16.jsonl), [batch8](artifacts/rfc-025-v7-current-v4-parallel-20261027-ext4-batch8.jsonl), [point8](artifacts/rfc-025-v7-current-v4-parallel-20261027-ext4-point8.jsonl), [batch1](artifacts/rfc-025-v7-current-v4-parallel-20261027-ext4-batch1.jsonl); each has matching `-runs.tsv` and `-host.jsonl`
- [Paired analyzer](artifacts/rfc-025-v7-current-v4-parallel-20261027-analyze.py), [summary](artifacts/rfc-025-v7-current-v4-parallel-20261027-summary.json)
- [Best-observation analyzer](artifacts/rfc-025-v7-current-v4-parallel-20261027-best.py), [winners, masks and source hashes](artifacts/rfc-025-v7-current-v4-parallel-20261027-best.json)
- [Verifier](artifacts/rfc-025-v7-current-v4-parallel-20261027-verify.py), [verification result](artifacts/rfc-025-v7-current-v4-parallel-20261027-verification.json)

[WAL documentation](../README.md).
