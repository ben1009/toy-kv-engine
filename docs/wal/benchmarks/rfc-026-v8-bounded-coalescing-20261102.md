# RFC 026 V8 bounded durability coalescing (2026-11-02)

## Decision

Keep this candidate for review as a provisional V8 scheduling improvement.
Concurrent workloads improve substantially, while the final single-writer
batch control is essentially flat. The fastest eight-writer batch sample has
a slightly worse p99 than the fastest control, so this is not an unconditional
tail-latency win or RFC 026 qualification. V7 was the default during this
screen; the later [V8 adoption decision](../rfc-026-v8-default-20261103.md)
did not claim performance qualification.

The change gives an already-ready durability prefix a bounded opportunity to
include admitted, unfinished successors. It uses the previous sync latency:
100 microseconds after a sync below 100 microseconds, otherwise 400 microseconds.
The first sync, a lone producer, a fully ready window, close and poison skip
the optional wait. Notifications recheck readiness against one fixed deadline.
The final version captures readiness once before deciding to coalesce, avoiding
an extra coordinator lock on the solo path. DATA submission and the RFC digest,
frame, recovery and single-sync durability rules are preserved.

## Method

The baseline executable is the uncommitted worker-backed V8 implementation on
top of `6e5989a2`; its SHA-256 is recorded in the plan and the local executable
still matches it. Commit `d6003ed1` preserves the later retained implementation,
including coalescing and admission-aware grouping, so it is not the exact
baseline source. To recover the affected baseline V8 source files from a copy
of `d6003ed1`, reverse the archived admission-aware grouping patch and then
the bounded-coalescing patch; both apply cleanly. This does not establish that
every other baseline source file can be reconstructed byte-for-byte.

Both arms use PITR V8 Parallel on `/dev/nvme0n1p3` (ext4),
1 KiB values, no compaction, a 2 GiB target SST and latency sampling every ten
operations. Batch-64 runs use 750,000 puts. Point runs use 20,000 puts at eight
and sixteen writers and 5,000 puts at one writer. Migration/setup completes
before timing. The benchmark cleans up database and repository data after each
run. Executables and working files remain under the local repository's target
directory. The host was a 13th Gen Intel Core i9-13900T running Linux
`6.12.71-1-lts`; the current boot began on 2026-10-04, before these runs.

The [initial runner](artifacts/rfc-026-v8-bounded-coalescing-20261102-run.txt),
[guards](artifacts/rfc-026-v8-bounded-coalescing-20261102-guards.txt),
[guard repeat](artifacts/rfc-026-v8-bounded-coalescing-20261102-guard-repeat.txt),
[final screen](artifacts/rfc-026-v8-bounded-coalescing-20261102-final.txt), and
[confirmation](artifacts/rfc-026-v8-bounded-coalescing-20261102-confirmation.txt)
preserve the literal executable paths and arguments. These text copies were
saved from the original local run directory without changing the scripts; the schedule and
binary hashes identify each invocation. All variants use this command shape:

```text
write-perf-{arm} --bench {wal_batch_concurrent|wal_concurrent} --wal-batch-size 64
  --pitr --pitr-format v8 --wal-io-mode parallel --num {num} --threads {threads}
  --value-size 1024 --target-sst-size 2147483648 --compaction none
  --output json --latency-sample-every 10 --path {sample-db}
```

The initial schedule alternated two pairs each at eight and sixteen writers.
Solo and point checks followed. Borderline checks were repeated in reversed
order. Removing the unnecessary solo-path lock produced a distinct final
binary, which was checked across all six workloads. A final fixed pair for
solo batch and eight-writer batch followed because their controls stalled.
Every completed measurement is preserved in the
[raw records](artifacts/rfc-026-v8-bounded-coalescing-20261102-raw.jsonl).
The [plans and binary hashes](artifacts/rfc-026-v8-bounded-coalescing-20261102-plans.json)
record each schedule. The final runner was paused while the current child
finished, then resumed after a cooldown as I/O PSI avg10 fell from about 10%
to 0.06%.

Before running, the screen flagged p99 above 10 ms or mean fdatasync above
1 ms as obvious stalls. Flagged measurements remain in the archive but are
excluded below. This aggregate rule cannot identify every storage outlier;
several included runs still have maxima around 30 ms. No profiled run from the
preceding investigation is used here.

## Best eligible final-binary results

The fastest eligible throughput selects each row's accompanying p99. Baseline
samples share one exact executable across phases. Initial-candidate scores
are kept separate from the final candidate. These best samples can come from
different cohorts and do not establish a paired causal estimate.

| Workload | Writers | Baseline ops/s | Final ops/s | Throughput change | Baseline p99 ms | Final p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| Batch 64 | 1 | 74,374 | 73,895 | -0.6% | 1.317 | 1.294 |
| Batch 64 | 8 | 234,188 | 351,240 | +50.0% | 3.915 | 4.055 |
| Batch 64 | 16 | 346,701 | 419,749 | +21.1% | 5.444 | 4.589 |
| Point | 1 | 1,592 | 1,595 | +0.2% | 1.305 | 1.241 |
| Point | 8 | 5,282 | 9,053 | +71.4% | 3.329 | 1.720 |
| Point | 16 | 7,539 | 8,827 | +17.1% | 4.156 | 3.670 |

The eight-writer batch p99 at the best throughput is 3.6% higher. Across its
eligible samples, baseline throughput spans 233,085–234,188 ops/s and p99
3.915–4.424 ms (2/4 eligible). The final candidate spans 319,844–351,240 ops/s
and p99 3.243–4.055 ms (2/2 eligible). Its median throughput is 43.6% higher and
median p99 12.5% lower. Both adjacent final-binary baseline runs stalled, so
the final eight-writer batch comparison has no eligible adjacent pair.

At sixteen-writer batch, the final candidate's eligible adjacent control is
336,253 ops/s / 5.643 ms versus 419,749 / 4.589: +24.8% throughput and -18.7%
p99. All three baseline samples are eligible; the final candidate has one.
The earlier candidate also improved throughput and p99 in both sixteen-writer
pairs. The single-writer batch confirmation is an eligible adjacent pair;
neither arm spent any time in coalescing, and each performed 11,719 syncs.

Point baselines have respectively 2/3, 3/3 and 3/3 eligible samples at one,
eight and sixteen writers. The final candidate has one eligible sample per
point workload. See the [summary](artifacts/rfc-026-v8-bounded-coalescing-20261102-summary.json)
for each variant's eligible counts, best samples and medians.

## Mechanism and remaining work

At the fastest eight-writer batch samples, syncs fall from 3,900 to 1,621 and
process CPU from 8.815 to 7.388 seconds. DATA bytes and tickets match. Worker
eventfd writes are 6,543 versus 6,789; this change does not solve the DATA
submission wakeup overhead. At sixteen writers syncs fall from 2,272 to 1,677.
V8's commit-group counter describes durability groups, whereas V7's describes
DATA submission groups, so those group counts cannot be compared directly.

The final V8 batch peaks remain about 29% below historical V7 peaks of
498,401 and 590,273 ops/s at eight and sixteen writers. Those historical
scores are not fresh controls. This screen does not cover the complete RFC
matrix, recovery performance, or an adoption decision.

All 78 V8 module tests pass outside the sandbox, including new tests for an
unfinished successor reaching the wait deadline, completion waking the wait,
close/poison waking it, and the wait never publishing durability. Clippy with
all features/targets and the ordinary library check pass. The
[candidate patch](artifacts/rfc-026-v8-bounded-coalescing-20261102-candidate.patch)
is relative to the baseline V8 source described above.
