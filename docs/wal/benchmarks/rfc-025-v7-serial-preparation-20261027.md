# RFC 025: serial extent preparation and 16-writer SSD coverage, 2026-10-27

**The single-writer objective passes. The hybrid fix is preserved for local review in `71bd7c23`, alongside the maintainer-kept async core. Two v4-normalized guards fail, so the full performance qualification remains failed.**

The maintainer explicitly keeps the [asynchronous extent optimization](rfc-025-v7-async-extent-20261026.md) and requests a one-writer latency fix plus 16-writer measurements. The retained source is `0ffebd516b865b01371ed65014ac0347ae2fb420`. Its background initializer uses a dedicated OS thread with a bounded wake channel. This follow-up changes preparation scheduling, while preserving its codec, ticket ordering, resource bounds, shared initialization cursor, failure handling, sealing join and one whole-WAL `fdatasync` protocol.

## First attempt: full synchronous extent for a serial operation

The first prototype checks the existing resource owner's outstanding batch-operation count before preparation. With zero or one operation, it synchronously prepares the entire allocated 1-MiB extent; concurrent operations retain the asynchronous 128-KiB demand prefix. Its exact source and executable are preserved under the `rfc-025-v7-serial-extent-20261027` artifact prefix. The prototype executable is `c51dbed65ed734aee51380b64c3d67ac05efdb7ba31cab547d0e0446b7a09caa`.

Both 16-writer cells complete three alternating four-arm rounds and four excluded warmups. Point throughput changes -1.1% raw / -4.0% normalized, but p99 rises 32.9%. Batch64 changes -3.3% raw / -5.9% normalized, with 2.2% lower p99 and 7.3% more CPU. The point p99 guard and batch normalized floor fail. After completing the two requested 16-writer cells, the failed prototype is stopped before the unrun one/eight-writer cells. The cancellation is explicit: 24 scored records, eight warmups, no single-writer result and no passing qualification. No completed run is removed or selected by score.

A whole-extent first preparation can stall a concurrent wave before other producers have registered. That is the scheduling hypothesis motivating the second version; the comparison does not isolate every kernel-level cause of its tail variation. The failed source is replaced, while the original asynchronous optimization stays retained.

The kept asynchronous build itself supplies these highest scored 16-writer rates from the same source binary:

| Workload | V4 Parallel | Kept async v7 Parallel |
| --- | ---: | ---: |
| Point, 100,000 puts | 27,052 | 24,630 |
| Batch64, 750,000 puts | 562,626 | 568,949 |

These are highest rates from three rounds, excluding warmups. Every slower observation remains in the raw records and paired summaries. Point throughput is still about 9% behind this matching v4 peak. Batch64 is close. These observations are separate from later cohorts and historical highs.

## Second attempt: repeated demand prefixes for serial writes

The revised prototype always prepares the required 128-KiB-aligned prefix within the same already allocated extent. It requests background suffix work only when more than one batch operation is outstanding. Serial commits never schedule another target. A previously queued request remains bounded by its existing allocated range, and the shared cursor prevents it from overwriting DATA or FRONTIER after a transition to serial demand. Concurrency is a conservative scheduling hint, never a durability boundary.

The resource-owner hint is queried before taking the preparation mutex. Readiness fast paths remain unchanged. Allocation limits, large-DATA allocation-only skips and sticky error checks retain their existing semantics. The revised executable is `b1f2940d1f709ffbddbe8665796c69140d138a4cfd776b54d4aebf4642561ae3`.

The fixed SSD matrix has five pairs at one writer and three pairs in each 8/16-writer point/batch64 cell. Every cell has one warmup per arm, a 30-second initial cooldown and ten seconds between commands. Values are 1 KiB, the SST target is 2 GiB, formats and Parallel mode are explicit, and all stalls remain included. Recognized competing builds, tests and benchmarks are sampled before, once per second during, and after every run. There is no build/test/profile overlap and no RAM performance capture.

The one-writer cell has 20,000 puts and samples every ten operations, giving 2,000 latency samples per run. Five arms compare pre-async v7, kept-async v4/v7 and revised v4/v7. Point8/16 use 100,000 puts and sample every 100 operations (1,000 / 1,008 samples). Batch64 uses 750,000 puts and samples every ten batches (1,176 / 1,184 samples). Each high-concurrency cell has four arms, comparing the kept-async and revised binaries in v4/v7 modes. Differing per-thread remainder counts explain the 16-writer sample totals; they are verified from each raw record.

Each cell requires paired raw, normalized and arm-median throughput ratios at least 0.95, p99 at most 1.10, identical DATA bytes and sync-count ratio at most 1.05. One writer additionally requires p99 at least 5% lower than kept async and at most 10% above pre-async v7, with throughput at least 95% of pre-async. CPU is reported. These are exploratory scheduling-fix guards, not complete Stage 8 qualification.

## Correctness and source identity

Both prototypes pass all 1,969 full-check tests with zero skips/retries, plus nine focused extent tests outside the sandbox. The concurrent fixtures hold a real extra producer reservation to exercise background work. A new test writes 65 acknowledged batches through multiple extent rollovers in both a serial start and a concurrent-to-serial transition, holds an earlier background request paused, checks that serial demand schedules no new target, verifies preserved bytes, closes without resuming the worker, and recovers every batch exactly. The revised readiness expectation is the required 128-KiB prefix, while allocation remains bounded to the existing 1-MiB target.

The first revised run catches a stale whole-extent test expectation from the failed prototype. The corrected final run executes all nine focused tests, and the earlier failure log remains identifiable. Direct spelling checks and both release builds pass. Every source manifest has 128 inputs based on `0ffebd51`; exact patches, source reconstruction and all three relevant saved executable hashes are preserved. No on-disk format or durability rule changes.

## Third attempt: small startup prefix, complete later serial extents

The repeated-prefix attempt completes both 16-writer cells and all five serial rounds: 49 scored records and 13 warmups. Its one-writer p99 is effectively unchanged against the kept async build, 19.1% above the pre-async reference, and raw throughput is 5.2% below that reference. Median paired preparation wait is 6.64 times the async baseline. Both 16-writer cells pass, but the serial failure cancels the unrun eight-writer cells. Every completed run remains included.

The final hybrid adds an immutable initial prepared boundary. The first preparation remains a 128-KiB-aligned demand prefix. On later serial readiness misses, it initializes the complete existing allocated extent before writing DATA/FRONTIER. Multiple outstanding batch operations retain the asynchronous path. Serial operations do not enqueue another background target. This combines small startup work with the original serial preparation frequency, while sharing the existing cursor, caps and sticky errors.

All 1,969 tests and nine focused tests pass for this exact source. The rollover test verifies initial 128-KiB readiness and complete allocated readiness after later serial rollovers, preserving every acknowledged batch across a concurrent-to-serial transition. The frozen executable is `e7f7712825d2166c14b5b67601388a8e29697f8fc034832e457061cf6e30ae19`.

The frozen matrix runs one writer first, then 16-writer point/batch64 and eight-writer point/batch64. The serial phase has five pairs with five arms; each high cell has three pairs with four arms. The complete final cohort contains 73 scored records and 21 warmups. Together, the three source variants preserve **146 scored records, 42 warmups, 188 unique IDs and 1,870 clear host samples**. There are no score-selected retries. Their source patches and summaries remain separate.

## Final hybrid results

Rates are per-arm medians. Changes are medians of same-round ratios. Normalization is `(hybrid v7 / hybrid v4) / (kept async v7 / kept async v4)`.

| SSD workload | Async / hybrid median puts/s | Paired raw | V4-normalized | CPU | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point, 1 writer | 2,248 / 2,283 | +2.6% | +2.4% | -6.3% | -9.2% |
| Point, 8 writers | 15,882 / 15,939 | +0.4% | -51.9% | +2.4% | -0.1% |
| Batch64, 8 writers | 371,534 / 369,520 | +2.7% | -0.7% | -2.1% | +2.8% |
| Point, 16 writers | 24,297 / 23,745 | -2.6% | -3.7% | +7.0% | +6.7% |
| Batch64, 16 writers | 547,729 / 535,783 | -3.3% | -7.7% | +6.9% | -7.4% |

At one writer, median p99 is **0.834 ms for kept async, 0.758 ms for the hybrid and 0.758 ms for pre-async**. Paired p99 is 9.2% lower than kept async and effectively unchanged against pre-async. Paired throughput gains 2.6% against async and 0.9% against pre-async. CPU falls 6.3%. All serial guards pass. Increased foreground preparation wait is intentional: the hybrid performs that work before serial I/O rather than scheduling an unused suffix concurrently with it.

All direct throughput, arm-median, p99, DATA-byte and sync-count guards pass. The normalized point8 and batch16 floors fail. Point8 v4 controls change by a median paired 2.124 times despite flat v7 throughput; batch16 v4 controls gain 7.6% while v7 loses 3.3% raw. These are recorded control movements, not grounds for deleting observations or assigning all differences to storage variation. Sixteen-writer CPU rises about 7%, and its raw throughput is about 3% lower. Those costs remain part of the review.

The automatic frozen rule would reject the final prototype. Under the user's instruction to keep async work and optimize one writer, the agent preserves this tested single-writer fix in a **local review commit**. This is a documented deviation from the automatic retention rule, not an assertion that the user explicitly approved the new failed limits. The [decision record](artifacts/rfc-025-v7-serial-startup-20261027-decision.json) keeps `guards_passed: false`, both failed guards and the local-review scope. No default activation, PR or push follows from this result. Point parity and the complete Stage 8 gate remain unmet.

## Highest matching scores

The following descriptive peaks span only the four named extent follow-up cohorts: original async, full serial fallback, repeated demand prefix and final hybrid. Workload, writers, operation count, value size, SST target, mode and sampling must match exactly. Warmups are excluded. Kept-async and hybrid v7 rates are selected only from their respective executable identities; candidate binaries are never pooled into a paired estimate.

| SSD workload | Highest v4 control | Kept async v7 | Hybrid v7 |
| --- | ---: | ---: | ---: |
| Point, 1 writer | 2,561 | 2,291 | 2,319 |
| Point, 8 writers | 17,185 | 16,069 | 16,005 |
| Batch64, 8 writers | 395,269 | 418,117 | 381,380 |
| Point, 16 writers | 28,430 | 24,690 | 23,779 |
| Batch64, 16 writers | 610,718 | 570,779 | 538,834 |

The [peak generator](artifacts/rfc-025-v7-serial-preparation-20261027-peaks.py) and [peak identities](artifacts/rfc-025-v7-serial-preparation-20261027-peaks.json) retain each run ID, run manifest and raw hash. They do not override the failed paired guards. Earlier cohorts remain separate: for example, the pre-async v7 batch8 peak of 480,373 puts/s in the [earlier SSD rerun](rfc-025-v7-durability-cycle-cost-ssd-rerun-20261026.md) is not pooled with these variants. The final source does not establish v4 parity or a new global throughput high.

## Reproduction

Each variant uses the `rfc-025-v7-{variant}-20261027` prefix in the artifacts directory:

| Variant | Plan | Exact source | Summary | Checks |
| --- | --- | --- | --- | --- |
| serial-extent | [plan](artifacts/rfc-025-v7-serial-extent-20261027-plan.json) | [patch](artifacts/rfc-025-v7-serial-extent-20261027-candidate.patch), [128 hashes](artifacts/rfc-025-v7-serial-extent-20261027-source-sha256.tsv) | [summary](artifacts/rfc-025-v7-serial-extent-20261027-summary.json) | [checks](artifacts/rfc-025-v7-serial-extent-20261027-checks.json) |
| demand-extent | [plan](artifacts/rfc-025-v7-demand-extent-20261027-plan.json) | [patch](artifacts/rfc-025-v7-demand-extent-20261027-candidate.patch), [128 hashes](artifacts/rfc-025-v7-demand-extent-20261027-source-sha256.tsv) | [summary](artifacts/rfc-025-v7-demand-extent-20261027-summary.json) | [checks](artifacts/rfc-025-v7-demand-extent-20261027-checks.json) |
| serial-startup | [plan](artifacts/rfc-025-v7-serial-startup-20261027-plan.json) | [patch](artifacts/rfc-025-v7-serial-startup-20261027-candidate.patch), [128 hashes](artifacts/rfc-025-v7-serial-startup-20261027-source-sha256.tsv) | [summary](artifacts/rfc-025-v7-serial-startup-20261027-summary.json) | [checks](artifacts/rfc-025-v7-serial-startup-20261027-checks.json) |

Each prefix also has executable identities, environment, driver and analyzer. Every completed cell has raw `.jsonl`, `-runs.tsv`, `-host.jsonl` and exact `-run.sh` files under `rfc-025-v7-{variant}-initial-{cell}-20261027`. The first two variants have explicit cancellation records for their unrun cells. Their completed benchmark processes all return success; the orchestration stop after a necessary failed guard is recorded separately.

The [combined verifier](artifacts/rfc-025-v7-serial-preparation-20261027-verify.py) reconstructs every 128-input source state, verifies saved executable and raw/audit hashes, regenerates all summaries and peaks, checks the failed guards and local-review exception, and confirms removal of benchmark/reconstruction scratch directories. The [verification result](artifacts/rfc-025-v7-serial-preparation-20261027-verification.json) retains its fingerprints. The source fix and measured evidence are separate local commits.

[WAL documentation](../README.md).
