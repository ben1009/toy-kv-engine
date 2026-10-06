# RFC 024: native commit waits and completion drain, 2026-10-06

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

The parallel WAL coordinator now processes a bounded snapshot of queued
completions after coalescing and before capturing its next sync target.
The maintainer selected this change for provisional retention: the native
batch64 screen reports **+2.1% throughput and -2.4% p99 at sixteen writers**,
but it fails the original +5% retention threshold and repeat controls.
This is not a qualified performance gain or an RFC gate pass.

A separate moving-cutoff experiment remains private. Its initial
sixteen-writer screen reports +4.2%; an independent rerun reports +3.1%.
Neither session passes its frozen screen. The implementation includes
only the bounded completion drain.

## Commit critical path

Private diagnostic builds sample every 61st native commit and timestamp
every sync. Each diagnostic case executes 524,288 puts as 8,192 batches
of 64, with 1 KiB values, PITR and serializable transactions disabled,
eight Tokio workers, and a 1 GiB memtable target. There is no timed
rotation. Three observations per case follow a warmup; diagnostic
instrumentation is absent from the performance comparisons.

For each sampled ticket, the audit finds the first sync whose
acknowledged range covers it. Its stages sum exactly to the sampled
native task's duration. These are per-commit elapsed intervals, rather
than sums of concurrent insertion timers. Caller input preparation and
batch construction precede this task and are excluded.

The sixteen-writer observations contain 134 sampled commits each.
Their mean stage times, in microseconds, are:

| Stage | Observation 1 | Observation 2 | Observation 3 |
| --- | ---: | ---: | ---: |
| Preparation permit | 45.8 | 51.6 | 49.8 |
| Memtable lease | 0.3 | 0.3 | 0.3 |
| WAL buffer preparation | 0.9 | 0.9 | 0.9 |
| WAL encoding and admission | 31.0 | 34.7 | 34.2 |
| Admission to durability acknowledgement | 1,073.9 | 1,061.1 | 1,365.8 |
| Durable-ready to task resumption | 64.6 | 74.9 | 63.1 |
| Memtable publication | 179.6 | 192.8 | 183.8 |
| Ordered MVCC publication wait | 88.8 | 110.0 | 108.1 |
| Native task total | 1,484.8 | 1,526.3 | 1,806.0 |

The largest sampled interval is waiting for WAL durability. Of that
interval, 564–848 microseconds pass before the ticket's covering sync
begins, and 492–517 microseconds pass inside that sync. Acknowledgement
bookkeeping adds less than one microsecond. The first interval includes
write completion, coordinator processing, coalescing, and earlier syncs;
these timestamps do not isolate one exclusive cause within it.

The durable-ready interval includes notification, mutex release,
executor scheduling, and the waiter's predicate check. It is not a pure
Tokio scheduler measurement. Notification while holding the durability
mutex takes about four microseconds per sync in these diagnostics.
These results support investigating completion batching before adding
another preparation or publication optimization.

## Queued completions and retained change

Three additional sixteen-writer diagnostic observations record the
completion queue when coalescing returns. A cycle is eligible when its
written frontier exceeds its durable frontier.

| Observation | Eligible cycles | Cycles with queued completions | Fraction | Admission ahead of written frontier |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1,215 | 141 | 11.6% | 42.8% |
| 2 | 1,251 | 161 | 12.9% | 41.6% |
| 3 | 1,223 | 164 | 13.4% | 43.9% |

The existing coalescer can reach its captured admission cutoff while
later groups have already queued completion results. Processing those
results can extend the contiguous written prefix before the next sync.
It does not guarantee fewer syncs: a queued completion can remain
outside that prefix when an earlier group is incomplete.

The new drain snapshots the queue length once and processes at most
that many results with the existing completion handler. It adds no
waiting interval. The 400-microsecond coalescing deadline, preceding-sync
threshold, public barriers, poison boundary, buffer ownership, and
once-captured `fdatasync` acknowledgement target retain their semantics.
WAL durability still precedes ordered MVCC publication.

## Frozen performance screen

Both arms use the public native `write_batch_async` API and explicit
parallel WAL on `/dev/nvme0n1p3`, ext4. Each primary observation uses
524,288 puts, batch64, 1 KiB values, eight Tokio workers, and a 1 GiB
memtable target. Each case has two warmups per arm, three fixed
ABBA/BAAB/ABBA blocks, and an identical-baseline null pair. Executables
are staged in RAM, databases are fresh, and runs have five-second idle
gaps. No build, test, diagnostic trace, profiler, or device administration
runs alongside measurement.

Each block compares its two candidate observations with its two baseline
observations using geometric means. The paired estimate is the median
of all three block ratios. Absolute arm medians are reported separately;
their quotient is not the paired estimate. Repeat controls require
both arms' throughput max/min to stay within 5% and p99 within 10%.

| Writers | Baseline puts/s | Drain puts/s | Paired throughput | Paired p99 | Paired CPU/put | Passing repeat blocks | Null |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 8 | 343,729 | 346,713 | +2.4% | -4.0% | +0.3% | 1/3 | Pass |
| 16 | 607,435 | 616,392 | +2.1% | -2.4% | +0.5% | 1/3 | Pass |

The sixteen-writer block throughput changes are **+2.1%, +3.1%, and
-0.5%**. The final block remains in the estimate. Sync counts have paired
changes of -4.7% at eight writers and -0.8% at sixteen writers. Neither
these counts nor the sampled queue backlog prove that the small
throughput change is caused entirely by the drain.

All 36 observations complete: eight warmups, 24 primary observations,
and four null observations. No retry is triggered. Both workload guard
medians pass the -5% throughput / +10% p99 limits. The +5% target and
minimum two passing repeat blocks per case fail. The original screen
therefore remains **failed**, even after the maintainer's retention decision.

## Independent workload guards

A separate fixed comparison uses the same executables for single-writer
batch64 and sixteen-writer one-put. It uses the same block orders,
repeat and null controls, and idle gaps. Single-writer primary runs
execute 131,072 puts with one Tokio worker; one-put runs execute 50,000
puts with eight workers. Both use 1 KiB values and fresh ext4 databases.

| Workload | Baseline puts/s | Drain puts/s | Paired throughput | Paired p99 | Passing repeat blocks | Null |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Batch64, 1 writer | 79,902 | 82,237 | +2.4% | -2.6% | 0/3 | Pass |
| One-put, 16 writers | 25,054 | 25,500 | +1.8% | -4.2% | 1/3 | Pass |

All 36 guard observations complete without a retry. Their medians pass
the regression limits, but repeat controls fail. The single-writer arms
both perform exactly 2,048 syncs per primary run: this case demonstrates
why small timing differences must not automatically be attributed to
better sync batching. These guards establish no qualified gain.

## Separate moving-cutoff experiment and rerun

This candidate rereads the optional coalescing admission cutoff on each
completion-loop iteration. Its deadline is still 400 microseconds, and
the actual durability target is still captured once before `fdatasync`.
It contains no extra completion drain, metadata padding, or pooling.
The same one-line move appeared in the earlier
[synchronous-path investigation](rfc-024-sync-cutoff-investigation-20261003.md);
this comparison tests it on the current native async batch workload.

| Session | Writers | Baseline puts/s | Candidate puts/s | Paired throughput | Paired p99 | Paired CPU/put | Passing repeat blocks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Initial | 8 | 347,665 | 353,338 | +3.0% | -9.5% | +4.5% | 2/3 |
| Initial | 16 | 581,539 | 603,628 | +4.2% | -2.1% | -2.6% | 2/3 |
| Independent rerun | 8 | 346,598 | 360,077 | +5.8% | -7.3% | +2.8% | 1/3 |
| Independent rerun | 16 | 599,881 | 626,289 | +3.1% | +0.5% | -6.4% | 3/3 |

All null pairs pass. The initial session has 42 observations, including
two excluded single retries and four excluded reversed-block retries.
Its final primary sixteen-writer block contains stalls in both arms and
has a -37.4% throughput ratio change. Both stalled originals remain primary.
The independent rerun has 37 observations, including one excluded
single retry. Its first eight-writer block contains a stalled baseline;
that original also remains primary.

The rerun's sixteen-writer block changes are **+7.3%, +3.0%, and +3.1%**,
all with passing repeat controls. This is stronger target repeatability,
but its median remains below +5%, and eight-writer controls fail. Both
sessions fail their frozen screen. The rerun does not replace the
earlier session, and the cutoff change is not retained. Neither
experiment compares against leader WAL or changes the existing
native-versus-leader report.

## Verification and reproduction

Both private candidates pass 1,292 all-feature engine library tests,
all-target Clippy with default and all features under `-D warnings`,
and the probe's three API, flush, close, and recovery tests. io_uring
tests and measurements run outside the sandbox. The promoted runtime
source matches the measured drain candidate byte for byte.

After promotion, `cargo make check` passes all 1,465 tests, both Clippy
feature configurations, formatting, dependency sorting, unused-dependency
checks, and typos. Another 1,198 default-feature library tests pass.
Explicit `cargo fmt --all -- --check` and a direct `typos` run also pass.

Every measurement reconciles batch samples, buffers, and completed
write CQEs. Diagnostic timestamps are monotonic and acknowledged ranges
cover all tickets exactly once. Frozen driver, executable, and input
hashes match after each session. All 114 production input hashes match
through measurement; promotion subsequently changes only the runtime
source. Owned databases and RAM stages are removed.

Local artifacts remain under ignored `target/` for reproduction:

- Critical-path observations: `target/rfc024-native-critical-path-20261006/diagnostic-records.json` and audited stage summary: `target/rfc024-native-critical-path-20261006/critical-path-audit-summary.json`.
- Queue-backlog observations: `target/rfc024-completion-drain-20261006/diagnostic-records.json` and backlog summary: `target/rfc024-completion-drain-20261006/backlog-summary.json`.
- Drain protocol: `target/rfc024-completion-drain-20261006/screen-protocol.json`, summary: `target/rfc024-completion-drain-20261006/screen-summary.json`, all observations: `target/rfc024-completion-drain-20261006/screen-records.json`, and independent audit: `target/rfc024-completion-drain-20261006/verification.json`.
- Guard protocol: `target/rfc024-completion-drain-guards-20261006/guard-protocol.json`, summary: `target/rfc024-completion-drain-guards-20261006/guard-summary.json`, all observations: `target/rfc024-completion-drain-guards-20261006/guard-records.json`, and independent audit after promotion: `target/rfc024-completion-drain-guards-20261006/verification.json`.
- Initial cutoff summary: `target/rfc024-native-moving-cutoff-20261006/screen-summary.json`, observations: `target/rfc024-native-moving-cutoff-20261006/screen-records.json`, and audit: `target/rfc024-native-moving-cutoff-20261006/verification.json`.
- Independent cutoff rerun protocol: `target/rfc024-native-moving-cutoff-rerun-20261006/screen-protocol.json`, summary: `target/rfc024-native-moving-cutoff-rerun-20261006/screen-summary.json`, observations: `target/rfc024-native-moving-cutoff-rerun-20261006/screen-records.json`, and audit: `target/rfc024-native-moving-cutoff-rerun-20261006/verification.json`.
- Full local check: `target/rfc024-completion-drain-20261006/production-check.log`, default-feature tests: `target/rfc024-completion-drain-20261006/production-tests-default.log`, and retention decision: `target/rfc024-completion-drain-20261006/retention-decision-20261006.json`.

The full RFC performance gate remains **UNQUALIFIED**. Retention of the
bounded drain is a maintainer decision, separate from the failed
experimental retention screens.
