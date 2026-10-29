# RFC 025: ordinary completion notification for v7, 2026-10-29

**Inconclusive; not retained.** The point8 screen and four regression guards pass, but the 16-writer batch guard has **zero matched pairs** after the frozen I/O-stall filter. Independent confirmation is cancelled as planned. The retained source and executable are restored exactly.

## Diagnosis and source change

The preceding [point-handoff diagnosis](rfc-025-v7-point-handoff-20261029.md) maps every API call in two excluded current-policy traces. DATA submission-return-to-reap averages 843/768 us at one/eight writers, while submission itself averages 6–10 us and publication/wakeup is much smaller. That locates a completion interval; it does not isolate device service, kernel completion work or reaping/scheduling delay.

The candidate requests `SINGLE_ISSUER` without `COOP_TASKRUN` only for active ext-filesystem v7 workers. Other runtimes keep their cooperative setup. The existing `EINVAL` fallback remains. The installed io_uring crate's builder explicitly sets these flags; the worker still owns creation, submission and reaping on its thread.

In the pinned [Linux 6.12.71 source](https://github.com/gregkh/linux/blob/v6.12.71/io_uring/io_uring.c), cooperative setup selects `TWA_SIGNAL_NO_IPI`; ordinary setup selects `TWA_SIGNAL`. The exact locally inspected source is bound by `rfc-025-v7-async-worker-limit-20261028-interrupted.json`. This difference motivates a notification experiment, not a claim that cooperative task work caused all of the traced gap.

The candidate preserves the existing external poll/command wake loop, forced async dispatch of large DATA, buffer ownership, allocation/initialization, DATA/FRONTIER codec, batching policy and one-sync-before-ACK. There is no production tracing or earlier-completion scheduling candidate included.

## Frozen staged SSD comparison

The [plan](artifacts/rfc-025-v7-signal-taskwork-20261029-plan.json) follows the completed diagnostic and precedes this source edit/capture. All arms use v7 Parallel, fresh repository-local ext4 databases, 1-KiB values, no compaction and a 2-GiB SST target on the Solidigm SSDPFKNU010TZ, Linux 6.12.71-1-lts, `/dev/nvme0n1p3`.

Point8 screen: two warmups and five alternating pairs. A pass enables five guards, each with two warmups and three pairs: point1, point16, batch8, batch16 and batch1. All enabled guards finish regardless of interim results. Only passing all guards enables a separate five-pair point8 confirmation. Here the screen and all five guards finish; confirmation is cancelled, with no replacements or score-selected retries.

Point8/16 use 100,000 puts sampled every hundred calls; point1 uses 20,000 sampled every ten. Batch64 uses 750,000 puts sampled every ten batch calls. Cooldowns are thirty seconds initially and ten seconds between commands. No tracing, profile, build, test, source edit or SSD health polling overlaps scoring. Sampled audits before/1 Hz/after detect no recognized competing work; that does not exclude every unrelated or brief task.

The unchanged stall flag is **mean FRONTIER write >= 1 ms or mean fdatasync >= 1.5 ms**; either arm excludes its whole pair. The screen and confirmation require three matched pairs, paired and arm-median throughput above baseline, and paired p99 below baseline. Guards require two pairs, both throughput ratios >= 0.95 and paired p99 <= 1.10. DATA bytes must match. CPU/RSS are diagnostic, without a veto.

## Results

Changes are median paired candidate/baseline ratios after the filter. Arm-median throughput is separate. Every raw run, including excluded observations and warmups, remains preserved.

| Workload | Matched pairs | Throughput | p99 | CPU | Arm-median throughput | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Point8 screen | 5 | +76.9% | -3.7% | -13.3% | +50.1% | Pass |
| Point1 guard | 3 | -1.6% | -0.7% | +5.8% | +57.7% | Pass |
| Point16 guard | 3 | +2.0% | -28.7% | -5.0% | +75.1% | Pass |
| Batch64, 8 guard | 3 | +5.2% | -24.4% | +7.4% | +1.1% | Pass |
| Batch64, 16 guard | 0 | — | — | — | — | Inconclusive |
| Batch64, 1 guard | 3 | +155.8% | -77.1% | -5.5% | +150.6% | Pass |

At batch16 the candidate is flagged in all three pairs; baseline is flagged in pairs 2 and 3. Thus each of the three pairs is excluded and none meets the required matched-pair minimum. Candidate mean FRONTIER writes are 1.286/1.707/1.266 ms; baseline pairs 2/3 are 1.576/1.652 ms. No fresh run replaces them, and the apparent unfiltered pair ratios do not qualify this guard.

The other positive ratios are also storage-state sensitive. Point8's large apparent throughput gain coincides with delayed baselines, rather than establishing a repeatable 76.9% source improvement. Solo batch baselines have mean syncs around 1.41–1.49 ms, just below the cutoff, while some candidates have much shorter syncs; the apparent 155.8% gain is not isolated. Passing an aggregate mask does not establish stall-free conditions or assign every difference to source code.

The notification hypothesis therefore remains unresolved. This is missing qualification evidence, rather than proof of a universal candidate slowdown. It does not establish v4 parity or qualify default activation.

### Every scored pair

Values show baseline → candidate; times are milliseconds. Exclusion applies to the whole pair.

| Workload | Pair | Puts/s | p99 ms | Mean FRONTIER ms | Mean sync ms | Excluded |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Point8 screen | 1 | 5,215 → 16,025 | 21.467 → 2.019 | 0.231 → 0.019 | 0.435 → 0.252 | No |
| Point8 screen | 2 | 5,026 → 6,297 | 10.451 → 10.067 | 0.133 → 0.105 | 0.415 → 0.348 | No |
| Point8 screen | 3 | 4,383 → 7,756 | 11.007 → 10.815 | 0.190 → 0.063 | 0.542 → 0.333 | No |
| Point8 screen | 4 | 6,705 → 16,075 | 9.335 → 1.187 | 0.092 → 0.020 | 0.344 → 0.251 | No |
| Point8 screen | 5 | 5,167 → 5,197 | 9.809 → 10.669 | 0.125 → 0.123 | 0.394 → 0.402 | No |
| Point1 guard | 1 | 1,972 → 1,351 | 2.423 → 2.673 | 0.019 → 0.021 | 0.245 → 0.252 | No |
| Point1 guard | 2 | 782 → 770 | 8.975 → 8.913 | 0.019 → 0.022 | 0.263 → 0.265 | No |
| Point1 guard | 3 | 856 → 2,282 | 9.318 → 0.747 | 0.021 → 0.020 | 0.261 → 0.244 | No |
| Point16 guard | 1 | 19,972 → 16,867 | 3.592 → 6.645 | 0.065 → 0.095 | 0.292 → 0.329 | No |
| Point16 guard | 2 | 7,679 → 13,444 | 17.192 → 10.097 | 0.339 → 0.134 | 0.612 → 0.410 | No |
| Point16 guard | 3 | 7,605 → 7,754 | 15.836 → 11.299 | 0.354 → 0.329 | 0.599 → 0.596 | No |
| Batch64, 8 guard | 1 | 291,072 → 306,278 | 8.643 → 6.530 | 0.124 → 0.073 | 0.432 → 0.371 | No |
| Batch64, 8 guard | 2 | 314,216 → 350,588 | 9.099 → 6.500 | 0.114 → 0.080 | 0.390 → 0.385 | No |
| Batch64, 8 guard | 3 | 478,798 → 317,543 | 2.653 → 9.270 | 0.022 → 0.090 | 0.315 → 0.431 | No |
| Batch64, 16 guard | 1 | 358,552 → 112,914 | 18.288 → 32.331 | 0.193 → 1.286 | 0.528 → 1.189 | Yes |
| Batch64, 16 guard | 2 | 93,275 → 94,185 | 30.409 → 34.713 | 1.576 → 1.707 | 1.468 → 1.252 | Yes |
| Batch64, 16 guard | 3 | 93,438 → 124,613 | 39.710 → 31.280 | 1.652 → 1.266 | 1.395 → 1.099 | Yes |
| Batch64, 1 guard | 1 | 23,462 → 23,367 | 11.883 → 11.775 | 0.083 → 0.089 | 1.458 → 1.462 | No |
| Batch64, 1 guard | 2 | 24,334 → 67,176 | 10.567 → 1.331 | 0.071 → 0.028 | 1.410 → 0.311 | No |
| Batch64, 1 guard | 3 | 22,985 → 58,806 | 11.462 → 2.629 | 0.097 → 0.040 | 1.487 → 0.409 | No |

## Correctness and final state

The candidate passes **1,975 full tests and 49 focused WAL tests**, zero retries or selected I/O-path skips, plus formatting, strict Clippy, dependency/spelling checks and release build outside the sandbox. The full check reports one slow test. Real O_DIRECT fixtures submit 96 mixed large/small groups and 96 small-only groups, crossing the 32-group admission limit, close after submission, and verify every completion ticket/length and byte. They do not assert a particular number of unfinished requests at the instant of close. Existing failure, partial-submit, stale-CQE, out-of-order, poison, controlled close and recovery tests pass.

After the inconclusive guard, all **128 baseline build-input hashes** match exactly. Rust source mtime is refreshed and the canonical executable is restored by atomic replacement. A fresh restored full check passes **1,973 tests**, zero retries/skips. No failed validation attempt or interrupted capture occurs in this slice.

| Executable | SHA-256 |
| --- | --- |
| Retained baseline | `f353022d846530a6754330aaea11f295319ea8f4de20afa09b6d2a3b57b021d8` |
| Candidate | `79592a3cbf0c4f9e5411690cd5e369e62b9abdc6991f88b03b0b0d9b42071889` |

The source base is `05dd9c2c74901741d635a8b9559638a88594f143`. The preserved [patch](artifacts/rfc-025-v7-signal-taskwork-20261029-candidate.patch) changes only `wal/parallel/worker.rs`, including the real-I/O fixture extension. The prior large-DATA async-dispatch source remains provisionally retained. No new runtime optimization is kept.

The trial adds **52 completed unique invocations**: twelve warmups and forty scored observations. Its preceding two excluded diagnostic invocations are counted once. The lineage advances from diagnostic-end 805 to **857**, or 803 + 54 for this combined investigation, plus the earlier interrupted invocation without JSON/run ID. Temporary benchmark databases are removed; exact executables, traces and check logs stay under ignored local `target/` paths.

```sh
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-signal-taskwork-20261029-analyze.py
python3 docs/wal/benchmarks/artifacts/rfc-025-v7-signal-taskwork-20261029-verify.py
```

The [summary](artifacts/rfc-025-v7-signal-taskwork-20261029-summary.json), [checks](artifacts/rfc-025-v7-signal-taskwork-20261029-checks.json), [decision](artifacts/rfc-025-v7-signal-taskwork-20261029-decision.json) and [verification receipt](artifacts/rfc-025-v7-signal-taskwork-20261029-verification.json) retain the result. Their artifact family includes fixed plans/matrix/controller, drivers, raw JSONL/TSV records, host audits, source manifests, executable identities and descriptive scored peaks.
