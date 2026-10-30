# RFC 025: completion priority for small writes, 2026-10-28

**Not retained.** The size-restricted candidate passes the point8 screen and all five regression guards, but independent confirmation misses the required p99 benefit: **+1.65% p99**. The retained source and executable are restored exactly. The apparent large throughput gains coincide with delayed baseline runs; they are not established source-only improvements.

## Narrower policy

The preceding [completion-first experiment](rfc-025-v7-ready-completion-first-20261028.md) fails batch latency checks while one point pair with low mean I/O times improves slightly. This separate candidate tests a narrower routing rule without pooling those earlier measurements.

Only active ext-filesystem v7 workers are eligible. Before taking/submitting later commands, the worker retires already-ready CQEs if **every write it currently owns is smaller than its 64-KiB async threshold**. It does not wait or spin for a CQE. An empty worker, a worker owning any large write, and other-format workers keep the normal ordering. Completed large-request metadata conservatively disables priority until its whole mixed group retires. The original post-submit drain remains.

The predicate classifies individual writes, not aggregate group bytes. It examines currently owned work, not future commands still in the channel. Consequently a small pending tail of an otherwise large-write workload can qualify after its large groups retire. There is no claim that the branch is disabled for every iteration of every batch invocation.

The candidate adds no DATA or initialization writes, allocation, dispatch flag, codec or durability-protocol change. CQE safety and sync-before-ACK remain independent of this scheduling hint. Realized grouping can vary with completion timing. No new mechanism counter records how many scored iterations select the early drain.

## Frozen staged comparison

The [plan](artifacts/rfc-025-v7-small-completion-priority-20261028-plan.json) is fixed before edits/captures. Every arm uses v7 Parallel, fresh repository-local ext4 databases on the Solidigm SSDPFKNU010TZ, 1-KiB values, no compaction and a 2-GiB SST target. Host: Linux 6.12.71-1-lts, `/dev/nvme0n1p3`. Point8/16 use 100,000 puts sampled every hundred calls; point1 uses 20,000 sampled every ten. Batch64 guards use 750,000 puts sampled every ten batch calls.

Point8 screen: two warmups and five alternating pairs. A pass enables five guards, each with two warmups and three pairs: point1, point16, batch8, batch16 and batch1. All enabled guards run regardless of interim results. Passing all guards enables independent point8 confirmation, with two warmups and five pairs. Failed prerequisites cancel dependents; no replacement runs or score-selected retries occur. Here every stage runs and all seven cells finish.

Each cell has a thirty-second initial cooldown and ten-second gaps. No source edit, build, test, profile or SSD health polling overlaps captures. Sampled audits before/1 Hz/after detect no recognized competing workload; this does not exclude every brief or unrelated task.

The unchanged aggregate stall flag is **mean FRONTIER write >= 1 ms or mean fdatasync >= 1.5 ms**; either arm excludes its pair. No scored invocation reaches these thresholds in this cohort, so no pair is excluded. This does **not** establish stall-free conditions: large shorter delays remain below the mask. Every raw run remains preserved.

Both screen and confirmation require at least three matched pairs, paired and arm-median throughput above baseline, and paired p99 below baseline. Guards require at least two pairs, both throughput ratios >= 0.95 and paired p99 <= 1.10. DATA bytes must match. CPU/RSS are diagnostic; no CPU veto applies. Retention requires all seven cells to pass.

## Results

Changes are median paired candidate/baseline ratios. The arm-median throughput ratio is separate. Initial and confirmation cohorts are not pooled.

| Workload | Matched pairs | Throughput | p99 | Sync count | Arm-median throughput | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Point8 screen | 5 | +63.5% | -10.0% | -1.8% | +69.0% | Pass |
| Point1 guard | 3 | +3.0% | +0.9% | +0.0% | +87.5% | Pass |
| Point16 guard | 3 | +7.8% | -1.5% | -1.4% | +154.0% | Pass |
| Batch64, 8 guard | 3 | -0.5% | +0.3% | -1.6% | +1.3% | Pass |
| Batch64, 16 guard | 3 | -1.0% | -10.2% | -1.6% | -0.3% | Pass |
| Batch64, 1 guard | 3 | +9.7% | -1.8% | +0.0% | +167.4% | Pass |
| Point8 confirmation | 5 | +78.3% | +1.7% | -1.6% | +198.5% | Fail p99 benefit |

The restriction avoids the preceding candidate's batch-guard failure in this cohort. Batch8 is almost flat (-0.5% throughput/+0.3% p99); batch16 is -1.0%/-10.2%. Those observations do not prove a batch speedup from a policy designed to exclude large owned writes.

The screen's +63.5% throughput and confirmation's +78.3% must not be presented as repeatable source gains. All five screen baselines have substantially longer API tails than earlier quiet retained-binary captures. Confirmation pairs 1, 2 and 4 strongly favor the candidate's throughput while their I/O timing differs; pair 5 slightly favors baseline in both throughput and p99.

Confirmation pair 3 has low mean I/O times in both arms: **15,414 → 15,713 puts/s** (+1.9%), p99 **1.236 → 1.263 ms** (+2.2%). Mean FRONTIER writes are 20.0/19.6 us and syncs 255.3/252.7 us. This descriptive pair shows a modest throughput/latency tradeoff. It does not rescue the missing p99 benefit or isolate a statistically significant 1.65% source regression. Initial/confirmation p99 benefit fails to repeat under the frozen rule, so the candidate is restored without changing thresholds.

### Every scored pair

Values show baseline → candidate; times are milliseconds. All pairs remain in the declared comparison. Warmups are preserved separately in the same raw files.

| Workload | Pair | Puts/s | p99 ms | Mean FRONTIER ms | Mean sync ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point8 screen | 1 | 2,826 → 16,052 | 24.991 → 1.376 | 0.440 → 0.018 | 0.623 → 0.253 |
| Point8 screen | 2 | 5,279 → 8,883 | 9.758 → 8.787 | 0.114 → 0.058 | 0.389 → 0.313 |
| Point8 screen | 3 | 5,164 → 5,603 | 10.536 → 10.997 | 0.127 → 0.112 | 0.410 → 0.389 |
| Point8 screen | 4 | 9,948 → 16,263 | 8.854 → 1.261 | 0.037 → 0.021 | 0.298 → 0.249 |
| Point8 screen | 5 | 5,255 → 5,261 | 10.376 → 10.215 | 0.110 → 0.111 | 0.395 → 0.394 |
| Point1 guard | 1 | 1,978 → 2,034 | 2.420 → 2.442 | 0.019 → 0.017 | 0.243 → 0.244 |
| Point1 guard | 2 | 781 → 1,465 | 9.243 → 2.733 | 0.018 → 0.018 | 0.262 → 0.248 |
| Point1 guard | 3 | 781 → 804 | 9.041 → 9.530 | 0.019 → 0.019 | 0.264 → 0.262 |
| Point16 guard | 1 | 20,479 → 20,559 | 3.521 → 3.468 | 0.058 → 0.057 | 0.307 → 0.306 |
| Point16 guard | 2 | 8,007 → 20,337 | 11.452 → 3.688 | 0.302 → 0.055 | 0.601 → 0.309 |
| Point16 guard | 3 | 7,210 → 7,772 | 11.276 → 11.617 | 0.304 → 0.252 | 0.637 → 0.709 |
| Batch64, 8 guard | 1 | 384,743 → 333,774 | 3.511 → 5.382 | 0.039 → 0.066 | 0.314 → 0.372 |
| Batch64, 8 guard | 2 | 329,441 → 347,351 | 7.556 → 7.575 | 0.103 → 0.053 | 0.376 → 0.395 |
| Batch64, 8 guard | 3 | 319,700 → 318,183 | 11.242 → 8.865 | 0.104 → 0.141 | 0.384 → 0.386 |
| Batch64, 16 guard | 1 | 396,549 → 383,453 | 17.755 → 17.045 | 0.170 → 0.158 | 0.482 → 0.498 |
| Batch64, 16 guard | 2 | 372,380 → 371,171 | 16.742 → 15.035 | 0.150 → 0.162 | 0.477 → 0.557 |
| Batch64, 16 guard | 3 | 361,106 → 357,469 | 16.446 → 14.532 | 0.170 → 0.172 | 0.532 → 0.585 |
| Batch64, 1 guard | 1 | 22,959 → 25,185 | 11.282 → 11.082 | 0.121 → 0.136 | 1.430 → 1.246 |
| Batch64, 1 guard | 2 | 18,608 → 61,403 | 16.733 → 1.410 | 0.650 → 0.033 | 1.296 → 0.313 |
| Batch64, 1 guard | 3 | 62,743 → 61,888 | 1.407 → 1.405 | 0.027 → 0.027 | 0.318 → 0.310 |
| Point8 confirmation | 1 | 5,212 → 9,293 | 10.147 → 10.315 | 0.107 → 0.050 | 0.407 → 0.301 |
| Point8 confirmation | 2 | 2,940 → 15,964 | 18.868 → 1.049 | 0.387 → 0.019 | 0.600 → 0.253 |
| Point8 confirmation | 3 | 15,414 → 15,713 | 1.236 → 1.263 | 0.020 → 0.020 | 0.255 → 0.253 |
| Point8 confirmation | 4 | 7,400 → 15,714 | 9.408 → 1.266 | 0.076 → 0.023 | 0.347 → 0.253 |
| Point8 confirmation | 5 | 5,263 → 5,105 | 10.078 → 10.255 | 0.107 → 0.118 | 0.404 → 0.409 |

## Correctness, identities and restoration

The candidate passes **1,976 full tests and 50 focused WAL tests**, zero retries or selected I/O-path skips, plus formatting, strict Clippy, dependency/spelling checks and release build outside the sandbox. The full check reports one slow test.

A deterministic core fixture checks the 64-KiB boundary, individual versus aggregate group bytes, idle/other-format routing, staged/submitted requests, and conservative mixed-group routing through partial and out-of-order completion. Real O_DIRECT fixtures submit 96 mixed groups and 96 small-only groups through the 32-group admission limit, close the worker and verify every completion ticket/length and byte. They exercise close after submission but do not assert a particular number of unfinished requests at the instant of close. Existing controlled close, poison, partial-submit, stale-CQE, failpoint and recovery tests also pass.

After rejection, all **128 baseline build-input hashes** match exactly. The Rust mtime is refreshed and the canonical executable is restored by atomic replacement. A fresh full restored check passes **1,973 tests**, zero retries/skips. No failed validation attempt or interrupted capture occurs in this slice.

| Executable | SHA-256 |
| --- | --- |
| Retained baseline | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Rejected candidate | `876e5c9c42812d59d8bd481db75d4a737acdac166e8a389964d71c29eabac38e` |

The source base is `b24cd80c114041566bc2c1f8075f76ce9e4b45c3`. Only `wal/parallel/worker.rs` differs in the preserved [candidate patch](artifacts/rfc-025-v7-small-completion-priority-20261028-candidate.patch). The earlier large-DATA async dispatch remains provisionally retained. No new runtime optimization is retained here; v4 parity remains open.

The **64 new unique invocations** comprise fourteen warmups and fifty scored observations, without new excluded diagnostics. The lineage advances from 739 to **803**, plus the earlier interrupted invocation without JSON/run ID. Prior captures are not counted again or pooled into the decision. Disposable benchmark databases are removed; saved binaries and check logs stay under ignored local `target/` paths.

Reproduce and verify:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-small-completion-priority-20261028-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-small-completion-priority-20261028-verify.py
```

The [summary](artifacts/rfc-025-v7-small-completion-priority-20261028-summary.json), [checks](artifacts/rfc-025-v7-small-completion-priority-20261028-checks.json), [decision](artifacts/rfc-025-v7-small-completion-priority-20261028-decision.json) and [verification receipt](artifacts/rfc-025-v7-small-completion-priority-20261028-verification.json) retain the result. The artifact family includes the fixed matrix/controller, capture drivers, raw JSONL/TSV records, host audits, 128-input source manifests, executable identities and descriptive scored peaks.
