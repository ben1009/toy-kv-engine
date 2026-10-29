# RFC 025: admission-driven producer framing, 2026-10-29

**Candidate not retained.** Scheduling the packer at admission instead of at producer completion did not restore the original batch grouping. The eight-writer batch candidate lost 5.8% throughput and increased p99 by 17.7% in the selected fresh runs. The tested production changes were restored; the candidate executable, source patch and all captures are preserved.

The result does not establish a v7 wire-format or one-sync problem. The candidate kept producer-side DATA framing and post-submit ordered SHA for large batches, but used admission to schedule the packer. If the head ticket was still being framed, the packer waited for that ticket; after it was ready, the packer immediately drained the contiguous ready prefix. No new batching wait, FRONTIER format, sync policy or digest definition was added. Small requests retained their original MVCC write-guard lifetime; only a large producer frame job released the guard early.

## Fresh SSD results

Each cell selects the highest-throughput scored run per exact binary that passes the predeclared mean FRONTIER-write and `fdatasync` thresholds, with p99 from the same run. Every arm has three scored runs; sixteen-writer batch has two eligible runs for the earlier candidate and the revised candidate. All other arms have three. The independent guard-only executable differs from the earlier producer-framing candidate in small-write guard lifetime and corresponding tests. Each table entry is puts/s / p99 milliseconds.

| Workload | Retained v7 | Earlier candidate | Guard-only control | Revised candidate | Revised vs retained |
| --- | ---: | ---: | ---: | ---: | --- |
| batch64, 1 writer | 64,139 / 1.343 | 78,041 / 1.250 | — | 71,821 / 1.232 | +12.0% throughput; -8.2% p99 |
| batch64, 8 writers | 402,945 / 2.755 | 401,042 / 2.222 | — | 379,549 / 3.244 | -5.8%; +17.7% |
| batch64, 16 writers | 363,455 / 15.371 | 415,672 / 15.205 | — | 325,727 / 17.948 | -10.4%; +16.8% |
| point, 1 writer | 1,976 / 2.387 | 1,914 / 2.473 | 2,243 / 0.753 | 1,301 / 8.740 | -34.2%; +266.1% |
| point, 8 writers | 15,381 / 1.154 | 16,095 / 1.166 | 15,393 / 1.202 | 15,523 / 1.303 | +0.9%; +12.9% |
| point, 16 writers | 22,706 / 1.642 | 22,745 / 2.075 | 23,296 / 1.442 | 9,745 / 10.364 | -57.1%; +531.0% |

The fixed mask flags a mean FRONTIER write of at least 1 ms or mean `fdatasync` of at least 1.5 ms. It does not identify every intermittent stall. The sixteen-writer batch winners have 15–18 ms p99, much worse than prior clean runs, and the selected revised sixteen-writer point run has a 0.246 ms mean FRONTIER write versus 0.024 ms retained. The revised one-writer point winner spent 15.03 s waiting for WAL sync versus 9.78 s retained despite similar mean `fdatasync` times. These results are contaminated by storage or wait-time variability and should not be used as clean estimates of the source change. Raw samples are preserved; no replacement runs or post-hoc masks were used.

The eight-writer batch result has a more useful grouping comparison. In the selected runs:

| Binary | Groups | Solo groups | DATA buffers |
| --- | ---: | ---: | ---: |
| Retained v7 | 7,254 | 4,017 | 11,720 |
| Earlier candidate | 11,324 | 11,044 | 11,720 |
| Revised candidate | 11,315 | 11,025 | 11,720 |

The revised candidate still formed nearly one group per batch. This was consistent across its three eight-writer batch runs, which had 11,315–11,478 groups and 11,025–11,294 solo groups. The retained runs had 7,067–7,554 groups. The retained queue receives a batch when it is admitted, before framing; the candidate's queue receives a large batch only after its producer finishes framing. Thus changing who wakes the packer does not preserve the set of admitted tickets visible to group formation. The old packer's serial work may also have provided time for admissions to accumulate; that is an inference, not an isolated measurement.

The guard-only control demonstrates that restoring the old small-write guard lifetime is implementable and was tested independently. Its best one-writer and sixteen-writer point samples outperform the earlier candidate's selected samples, while its eight-writer sample is slightly slower. The noisy cohorts and small sample size do not isolate the guard lifetime as the cause of the earlier point regression.

## Historical context

These reference scores are the best eligible runs for the exact retained executable, across unequal historical cohorts. They are context, not a paired source-effect estimate.

| Batch64 writers | v4 Parallel puts/s / p99 ms | Retained v7 historical best | Revised candidate fresh best |
| --- | ---: | ---: | ---: |
| 1 | 85,618 / 1.195 | 72,647 / 1.373 | 71,821 / 1.232 |
| 8 | 388,372 / 2.511 | 478,798 / 2.653 | 379,549 / 3.244 |
| 16 | 595,941 / 2.967 | 606,948 / 4.065 | 325,727 / 17.948 |

The revised one-writer batch winner is 16.1% below the historical v4 throughput high. The sixteen-writer result is not a fair performance conclusion because of the high tail latencies described above.

## Correctness, method and next experiment

The candidate passed **1,985 full tests** and **65 focused tests**, with retries disabled, as well as formatting, Clippy, dependency and spelling checks. Five ordering and poisoned-close tests passed ten consecutive stress runs; a separately fixed child-process recovery fixture passed 32 concurrent candidate runs. One earlier full check at eight test processes timed out a PITR boundary test under SSD load; that test passed ten isolated runs, and the complete check passed at four test processes. The final source retains the independently committed crash-fixture correction. After restoration, a four-process full check timed out the existing simple-leveled compaction integration test; it then passed two of three isolated runs and timed out once. A fresh two-process full check passed **1,973 tests** with retries disabled. The timeout and isolated results are recorded in the disposition.

The frozen matrix covers 84 invocations: 63 scored and 21 warmups. Batch cells write 750,000 puts in groups of 64; point cells write 20,000 puts. All use 1-KiB values, fresh repository-local databases, a two-minute SSD cooldown before each cell and 15-second gaps. Each workload runs three alternating rounds, and sampled process audits detected no competing builds, tests or benchmarks. Selected comparisons include each run's own p99; averages can hide SSD stalls.

A useful next experiment is to make admitted tickets visible to grouping before producer framing completes, and separately gate submission on the required frames becoming ready. It must preserve ticket ordering, bounded progress when a producer is slow or fails, poisoning semantics and the single-sync publication rule. This is a proposed follow-up; the retained implementation does not contain it.

The [plan](artifacts/rfc-025-v7-admission-grouping-20261029-plan.json), [matrix](artifacts/rfc-025-v7-admission-grouping-20261029-matrix.json), [fresh results](artifacts/rfc-025-v7-admission-grouping-20261029-summary.json), [historical comparison](artifacts/rfc-025-v7-admission-grouping-20261029-best.json), [candidate patch](artifacts/rfc-025-v7-admission-grouping-20261029-candidate.patch), [checks](artifacts/rfc-025-v7-admission-grouping-20261029-checks.json), [guard control](artifacts/rfc-025-v7-admission-grouping-20261029-guard.patch), [disposition](artifacts/rfc-025-v7-admission-grouping-20261029-outcome.json), and [verification receipt](artifacts/rfc-025-v7-admission-grouping-20261029-verification.json) provide the reproduction details. Raw captures: [batch1](artifacts/rfc-025-v7-admission-grouping-20261029-ext4-batch1.jsonl), [batch8](artifacts/rfc-025-v7-admission-grouping-20261029-ext4-batch8.jsonl), [batch16](artifacts/rfc-025-v7-admission-grouping-20261029-ext4-batch16.jsonl), [point1](artifacts/rfc-025-v7-admission-grouping-20261029-ext4-point1.jsonl), [point8](artifacts/rfc-025-v7-admission-grouping-20261029-ext4-point8.jsonl), and [point16](artifacts/rfc-025-v7-admission-grouping-20261029-ext4-point16.jsonl).
