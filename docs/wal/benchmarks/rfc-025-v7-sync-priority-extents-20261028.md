# RFC 025: sync priority over optional extent initialization, 2026-10-28

**Not retained.** Deferring new background zero-fill chunks during the v7 FRONTIER/sync cycle removes background starts during sync in the excluded diagnostic, but the fixed SSD matrix fails its performance checks. The retained large-DATA async-dispatch source `f7f39b1b` and canonical executable are restored exactly.

## Trace-backed hypothesis

The retained executable’s existing excluded native diagnostics show **137 of 398** background zero writes starting during `fdatasync` at eight writers, and **89 of 224** at sixteen writers. The background initializer writes the same WAL inode used by the durability cycle. These observations motivate reducing optional I/O competition; they do not establish that it causes every long tail. Both baseline diagnostics were already counted in the measurement lineage.

This differs from the [historical v4 background-I/O gate](../experiments/rfc-024-bg-io-gate-20261002.md), which targeted SST/compaction bursts that almost never started during sync. The new candidate targets v7’s optional extent initialization within its already allocated, charged suffix.

A scope guard marks priority from before FRONTIER construction through its whole-WAL sync and publication. The background worker checks this hint before selecting another chunk and returns to its bounded wake channel when deferred. An already selected chunk may finish. Foreground demand continues to prepare its required prefix without waiting for priority. The last guard wakes a pending target only if it remains beyond the prepared cursor. Nested scopes, all return paths and unwind use the same guard cleanup.

The candidate keeps the DATA/FRONTIER codec, ordered certificate, existing allocation caps, shared preparation cursor, sticky errors, DATA worker policy and coalescing policy. Realized grouping can still change with timing. It applies only where the retained v7 ext-filesystem asynchronous initializer exists.

## Mechanism observation

One fixed excluded batch64/eight-writer trace uses the candidate executable and the existing in-memory native interposer. The [diagnosis](artifacts/rfc-025-v7-sync-priority-extents-20261028-diagnosis.json) recomputes exact same-inode intervals:

| Excluded diagnostic | Background writes | Starts during sync | Any overlap with sync | Starts during FRONTIER + sync |
| --- | ---: | ---: | ---: | ---: |
| Retained, 8 writers | 398 | 137 | 196 | 187 |
| Retained, 16 writers | 224 | 89 | 121 | 112 |
| Candidate, 8 writers | 151 | 0 | 18 | 0 |

The candidate records **zero starts during sync**, with eighteen already-running intervals still overlapping it. It also records fewer background zero bytes. The traces come from different invocations with different realized FRONTIER counts; they are not paired performance evidence and are excluded from scoring. The advisory selection check permits a chunk selected just before priority to begin or finish I/O later. No hard exclusion of every overlapping syscall is claimed.

## Frozen SSD protocol

The [plan](artifacts/rfc-025-v7-sync-priority-extents-20261028-plan.json) is written before source edits and timing. Each arm uses v7 Parallel, fresh repository-local ext4 databases, 1-KiB values, no compaction and a 2-GiB SST target. Batch64 cells perform 750,000 puts and sample every ten batch commits; point8 performs 100,000 puts and samples every hundred calls. The host is Linux 6.12.71-1-lts on the Solidigm SSDPFKNU010TZ, `/dev/nvme0n1p3`.

Batch8 is the screen: two warmups then five alternating pairs. Batch16, point8 and batch1 each have two warmups and three alternating pairs. All four fixed cells run regardless of intermediate performance. Initial cooldown is thirty seconds with ten-second gaps. The checked launcher completes its excluded diagnostic before scoring; no replacement runs or score-selected repeats occur. No build, test, source edit, profile or SSD health/temperature polling overlaps captures. Sampled host audits detect no competing recognized workload.

The unchanged filter flags **mean FRONTIER write >= 1 ms or mean fdatasync >= 1.5 ms**. A pair is excluded if either arm is flagged; every raw record remains. The screen needs at least three matched pairs with paired-median and arm-median throughput above baseline and paired p99 below baseline. Other cells need at least two matched pairs, both throughput ratios at least 0.95, and paired p99 at most 1.10. DATA bytes must match. CPU and RSS are diagnostic.

These aggregate cutoffs miss shorter tails. Below-threshold runs are not established as stall-free, and differences cannot all be assigned to either the candidate or external storage variation.

## Fixed results

Percentages below are **median paired candidate/baseline changes** after the frozen filter. Arm-median throughput is calculated separately.

| Workload | Matched pairs | Throughput | p99 | Sync count | Arm-median throughput | Check |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Batch64, 8 writers | 2, 3, 4, 5 | -1.4% | +4.6% | +1.5% | -3.2% | Fail |
| Batch64, 16 writers | 1, 3 | -15.4% | +52.3% | +3.2% | -12.4% | Fail |
| Point, 8 writers | 1, 2, 3 | -56.3% | +88.5% | +3.1% | -78.4% | Fail |
| Batch64, 1 writer | 1, 2, 3 | -43.8% | +3.6% | +0.0% | -43.8% | Fail |

Batch8 excludes pair 1 because the baseline’s FRONTIER and sync means exceed the limits. The remaining four pairs miss all three benefit checks: paired throughput -1.4%, arm-median throughput -3.2%, and paired p99 +4.6%. Batch16 excludes pair 2 because both arms are flagged; its two remaining pairs fail the regression bounds. The faster matched batch16 pair is 589,950 → 575,213 puts/s with 5.044 → 4.496 ms p99, showing a throughput/latency tradeoff rather than a consistent win.

Point and solo results contain large delays below the aggregate mask. Point pair 2 is almost flat at **16,249 → 16,303 puts/s**, with p99 **1.452 → 1.223 ms**; pairs 1 and 3 have much longer candidate delays. Solo pair 1 strongly favors the candidate while pairs 2 and 3 favor baseline. The table preserves the declared statistics; it does not establish universal source-only slowdowns of 56% or 44%. No post-result filter removes those inconvenient observations.

Removing background starts during sync is therefore insufficient evidence to retain this scheduling policy. Deferral may move preparation work back onto demand; the experiment does not isolate that as the cause of the failures.

### Every scored pair

Each metric cell shows **baseline → candidate**, with times in milliseconds. Exclusion applies to the whole pair. The eight warmups and excluded diagnostic remain in the raw artifacts.

| Workload | Pair | Puts/s | p99 ms | Mean FRONTIER ms | Mean sync ms | Excluded |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Batch64, 8 writers | 1 | 49,250 → 396,123 | 46.381 → 3.013 | 2.756 → 0.069 | 3.533 → 0.312 | Yes |
| Batch64, 8 writers | 2 | 358,970 → 436,224 | 8.770 → 2.522 | 0.150 → 0.030 | 0.451 → 0.304 | No |
| Batch64, 8 writers | 3 | 335,442 → 325,143 | 8.580 → 9.125 | 0.136 → 0.134 | 0.434 → 0.424 | No |
| Batch64, 8 writers | 4 | 319,662 → 307,635 | 8.386 → 8.857 | 0.148 → 0.163 | 0.383 → 0.411 | No |
| Batch64, 8 writers | 5 | 308,363 → 308,907 | 8.678 → 8.987 | 0.161 → 0.152 | 0.554 → 0.509 | No |
| Batch64, 16 writers | 1 | 369,241 → 264,745 | 17.130 → 36.918 | 0.239 → 0.552 | 0.566 → 0.810 | No |
| Batch64, 16 writers | 2 | 159,014 → 35,501 | 47.502 → 64.521 | 1.362 → 6.755 | 1.164 → 4.812 | Yes |
| Batch64, 16 writers | 3 | 589,950 → 575,213 | 5.044 → 4.496 | 0.054 → 0.055 | 0.348 → 0.342 | No |
| Point, 8 writers | 1 | 7,991 → 3,488 | 9.892 → 18.651 | 0.096 → 0.396 | 0.339 → 0.521 | No |
| Point, 8 writers | 2 | 16,249 → 16,303 | 1.452 → 1.223 | 0.017 → 0.021 | 0.248 → 0.251 | No |
| Point, 8 writers | 3 | 16,181 → 2,890 | 1.256 → 18.940 | 0.018 → 0.498 | 0.249 → 0.594 | No |
| Batch64, 1 writer | 1 | 18,911 → 70,624 | 18.636 → 1.289 | 0.635 → 0.025 | 1.291 → 0.305 | No |
| Batch64, 1 writer | 2 | 41,686 → 23,440 | 11.225 → 11.632 | 0.188 → 0.472 | 0.555 → 1.074 | No |
| Batch64, 1 writer | 3 | 72,647 → 18,926 | 1.373 → 16.619 | 0.023 → 0.607 | 0.315 → 1.328 | No |

## Correctness, corrections and restoration

The final candidate passes **1,975 full tests and 49 focused WAL tests**, formatting, strict Clippy, dependency/spelling checks and the release build outside the sandbox. The two new real-ext4 tests prove nested priority defers unused-tail initialization while actual foreground writes cross the ready prefix, resume the pending target without another producer, preserve acknowledged bytes, recover every batch exactly, and close while priority remains held. Existing tests cover serial rollover, sticky failures, poison, physical caps, large-DATA skips and sealing joins. No selected I/O test returns through an unavailable-path skip.

Pre-score validation catches and preserves two corrections. The initial guard wakes work after every sync, violating the existing serial test; the final guard wakes only an unprepared target. The new canary fixture initially uses six bytes through an O_DIRECT descriptor; it is corrected to use a separate buffered handle for the unused tail. A text substitution first misses the formatted fixture calls, so its second failing run remains too. A reused executable name is then caught by an existence assertion after successful checks/build; the destination is corrected before metadata publication, preserving the older candidate binary. No benchmark runs occur under those incomplete attempts. Their source/check/log snapshots and hashes are in the [attempt record](artifacts/rfc-025-v7-sync-priority-extents-20261028-attempts.json).

After rejection, all **128 baseline source-input hashes** match exactly and the canonical executable is restored by atomic replacement. A fresh `cargo make check` passes **1,973 tests**, zero skipped or retried, with one slow test. A post-check parser initially rejects the optional “1 slow” summary label despite success; the verifier accepts that label without rerunning passing tests. The candidate changes only `kv-engine/src/wal/parallel/runtime.rs`; its patch and tested 128-input manifest remain preserved.

| Executable | SHA-256 |
| --- | --- |
| Retained baseline | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Rejected candidate | `6bf28f838a0d97b3f6ee9c4c7439418c2381e995252d4feb67c3647dab1611ea` |

The source base is `6cb51d522559585428c56d0f9e674e68320d3dd2`. The kept [large-DATA async-dispatch optimization](rfc-025-v7-large-data-async-dispatch-20261027.md) remains provisional. This experiment does not qualify v4 parity or default activation.

This slice adds **37 completed unique invocations**: one excluded native diagnostic, eight warmups and 28 scored observations. Its two earlier baseline diagnostics are reused and counted once. The lineage advances from 628 to **665**, plus the previously preserved interrupted invocation without JSON/run ID. No new capture is interrupted.

Reproduce the diagnosis, analysis and final-state verification:

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-sync-priority-extents-20261028-diagnose.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-sync-priority-extents-20261028-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-sync-priority-extents-20261028-verify.py
```

The [decision](artifacts/rfc-025-v7-sync-priority-extents-20261028-decision.json), [summary](artifacts/rfc-025-v7-sync-priority-extents-20261028-summary.json), [checks](artifacts/rfc-025-v7-sync-priority-extents-20261028-checks.json) and [verification receipt](artifacts/rfc-025-v7-sync-priority-extents-20261028-verification.json) bind the result. Native traces, check logs and exact saved executables remain in ignored local `target/` paths required by the verifier.
