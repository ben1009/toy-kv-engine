# RFC 025: report ready completions before later submissions, 2026-10-28

**Not retained.** The candidate reports ready v7 DATA completions earlier, but fails the SSD batch p99 checks and solo throughput bound. The retained source and canonical executable are restored exactly.

## Scheduling hypothesis

The worker normally receives commands, stages/submits writes, and then drains completions. A previously ready CQE can therefore wait behind a later submission round before the durability coordinator learns about it. Earlier trace investigations locate staging/handoff delays, but do not establish that this ordering is the dominant current SSD bottleneck. This experiment tests the source-level scheduling hypothesis without another excluded diagnostic.

For active ext-filesystem v7 workers, the candidate checks for ready CQEs at the start of the loop and retires them before receiving/submitting new commands. It does this only with no staged SQEs outstanding and never waits or spins for a CQE. The original post-submit drain remains. A shared helper preserves completion profiling, failpoint deferral, poison/error handling, buffer retirement and group-slot release for both drains. Other runtimes retain their drain ordering.

The change adds no DATA or initialization writes and changes no codec, allocation, forced-async dispatch, coalescing policy or one-sync-before-ACK rule. Realized grouping can still change with completion timing. This is separate from the rejected suffix-zeroing and sync-priority policies.

## Fixed SSD comparison

The [plan](artifacts/rfc-025-v7-ready-completion-first-20261028-plan.json) is saved before edits and captures. Both arms use v7 Parallel, fresh repository-local ext4 databases on the Solidigm SSDPFKNU010TZ, 1-KiB values, no compaction and a 2-GiB SST target. Batch64 performs 750,000 puts and samples every ten batch calls. Point8 performs 100,000 puts and samples every hundred calls. Host: Linux 6.12.71-1-lts, `/dev/nvme0n1p3`.

Batch8 has two warmups and five alternating pairs. Batch16, point8 and batch1 each have two warmups and three alternating pairs. All four cells run regardless of interim performance, with thirty-second initial cooldowns and ten-second gaps. No source edits, builds, tests, profiles or SSD health polling overlap captures. Sampled audits detect no recognized competing workload. No replacements or score-selected repeats occur.

The fixed stall flag remains **mean FRONTIER write >= 1 ms or mean fdatasync >= 1.5 ms**; either arm excludes its whole pair. Every raw record remains. Batch8 requires three matched pairs, paired and arm-median throughput above baseline, and paired p99 below baseline. Other cells require two pairs, both throughput ratios >= 0.95 and paired p99 <= 1.10. DATA bytes must match. CPU and RSS are diagnostic.

## Results

Changes are median paired candidate/baseline ratios after the filter. The arm-median throughput ratio is calculated separately.

| Workload | Matched pairs | Throughput | p99 | Sync count | Arm-median throughput | Check |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Batch64, 8 writers | 2, 3, 4, 5 | +2.1% | +34.5% | -1.7% | +6.7% | Fail p99 benefit |
| Batch64, 16 writers | 1, 2, 3 | -4.3% | +21.0% | +4.8% | -2.4% | Fail p99 bound |
| Point, 8 writers | 1, 2, 3 | +68.3% | -44.8% | -2.3% | +192.2% | Pass |
| Batch64, 1 writer | 1, 2, 3 | -13.8% | -6.8% | +0.0% | -23.4% | Fail throughput bound |

Only batch8 pair 1 is excluded, with both arms flagged. The other runs still contain shorter delays below the aggregate cutoffs: passing the mask is not proof of stall-free storage, and these ratios do not isolate a universal source effect.

The fastest matching batch8 pair is **463,805 → 448,313 puts/s**, p99 **2.608 → 2.764 ms**, which also favors the retained version. Later batch8 pairs vary substantially. Batch16 p99 ranges from 14.6 to 30.9 ms despite no aggregate flag; the experiment does not separate kernel/device delay from scheduling effects in that tail.

The large apparent point gain must not be advertised as a repeatable 68% optimization. Point pair 1 is 5,496 → 16,098 puts/s with p99 10.360 → 1.325 ms; pair 2 is 3,958 → 6,663 with 18.105 → 9.995 ms. In pair 3, where both have low mean I/O latency, rates are **15,893 → 16,060** (+1.1%) and p99 **1.230 → 1.138 ms** (-7.5%). That one descriptive pair motivates investigation but does not override the fixed batch failures or establish a qualified point-only policy.

### All scored pairs

Values show baseline → candidate; times are milliseconds. Warmups remain in the raw records.

| Workload | Pair | Puts/s | p99 ms | Mean FRONTIER ms | Mean sync ms | Excluded |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Batch64, 8 | 1 | 25,780 → 52,531 | 62.221 → 44.662 | 4.290 → 2.261 | 5.404 → 3.039 | Yes |
| Batch64, 8 | 2 | 463,805 → 448,313 | 2.608 → 2.764 | 0.025 → 0.059 | 0.313 → 0.311 | No |
| Batch64, 8 | 3 | 322,687 → 274,591 | 3.520 → 8.623 | 0.055 → 0.128 | 0.315 → 0.403 | No |
| Batch64, 8 | 4 | 286,698 → 308,117 | 7.939 → 8.540 | 0.117 → 0.121 | 0.437 → 0.447 | No |
| Batch64, 8 | 5 | 293,983 → 349,732 | 8.288 → 13.385 | 0.124 → 0.125 | 0.423 → 0.432 | No |
| Batch64, 16 | 1 | 346,878 → 331,858 | 16.390 → 19.835 | 0.262 → 0.249 | 0.582 → 0.583 | No |
| Batch64, 16 | 2 | 339,859 → 338,764 | 21.237 → 15.419 | 0.233 → 0.184 | 0.551 → 0.589 | No |
| Batch64, 16 | 3 | 328,180 → 167,259 | 14.626 → 30.876 | 0.275 → 0.976 | 0.588 → 0.777 | No |
| Point, 8 | 1 | 5,496 → 16,098 | 10.360 → 1.325 | 0.108 → 0.019 | 0.379 → 0.252 | No |
| Point, 8 | 2 | 3,958 → 6,663 | 18.105 → 9.995 | 0.255 → 0.129 | 0.525 → 0.391 | No |
| Point, 8 | 3 | 15,893 → 16,060 | 1.230 → 1.138 | 0.023 → 0.019 | 0.251 → 0.250 | No |
| Batch64, 1 | 1 | 30,366 → 44,432 | 10.828 → 10.003 | 0.080 → 0.044 | 1.034 → 0.625 | No |
| Batch64, 1 | 2 | 26,980 → 23,258 | 11.704 → 10.903 | 0.102 → 0.100 | 1.210 → 1.457 | No |
| Batch64, 1 | 3 | 53,559 → 23,211 | 9.570 → 11.100 | 0.034 → 0.103 | 0.488 → 1.441 | No |

## Correctness and final state

The candidate passes **1,974 full tests and 48 focused WAL tests**, with no retries or selected I/O-path skips, plus formatting, strict Clippy, dependency/spelling checks and release build outside the sandbox. The real O_DIRECT fixture now verifies every byte and completion length/ticket after closing with queued work. A new 96-group mixed-large/small case crosses the 32-group admission bound, exercising progress and drain ownership. Existing partial-submission, stale-CQE, out-of-order, poisoning, failpoint, recovery and close coverage passes. No new mechanism counter measures how many scored loop iterations actually take the early drain.

After rejection, all **128 baseline build-input hashes** match exactly. Rust source mtime is refreshed and the canonical executable is atomically replaced from the retained binary. A fresh full restored check passes **1,973 tests**, zero retries/skips, with one slow test. Candidate and restored checks both succeed on their first attempts. No runtime optimization is retained in this slice.

| Executable | SHA-256 |
| --- | --- |
| Retained baseline | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Rejected candidate | `1f5cc447edbd82e57951013ea2c481b40982316e91e13467c5e8cd9c6de009f4` |

The source base is `37bc99d388da6157962901e1c7f59589a236223c`. The preserved [patch](artifacts/rfc-025-v7-ready-completion-first-20261028-candidate.patch) changes only `wal/parallel/worker.rs`. The earlier large-DATA async-dispatch implementation remains provisional; this trial does not establish v4 parity.

All **36 new invocations** complete: eight warmups and 28 scored observations. There are no new excluded diagnostics. The lineage advances from 703 to **739**, plus the earlier interrupted invocation without JSON/run ID. Disposable benchmark databases are removed; saved executables and check logs remain in ignored local `target/` paths.

Reproduce and verify:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-ready-completion-first-20261028-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-ready-completion-first-20261028-verify.py
```

The [summary](artifacts/rfc-025-v7-ready-completion-first-20261028-summary.json), [checks](artifacts/rfc-025-v7-ready-completion-first-20261028-checks.json), [decision](artifacts/rfc-025-v7-ready-completion-first-20261028-decision.json) and [verification receipt](artifacts/rfc-025-v7-ready-completion-first-20261028-verification.json) bind the result. Their artifact family also preserves the fixed matrix, drivers, raw JSONL/TSV records, host audits, source manifests, executable identities and descriptive scored peaks.
