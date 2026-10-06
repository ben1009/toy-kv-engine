# RFC 024: native WAL coalescing cutoff refresh, 2026-10-06

Retain the coalescing cutoff refresh on top of the
[bounded completion drain](rfc-024-native-completion-drain-20261006.md).
The maintainer explicitly selected provisional retention after the frozen
screens failed their stability controls. The confirmation reports **+4.2%
paired throughput at sixteen writers**, with all three target blocks passing
repeat controls, but its identical-baseline null pair fails. This remains
an unqualified performance result; the full RFC adoption gate is unchanged.

The single-writer batch64 guard reports +0.1% throughput and +2.6% p99.
The sixteen-writer one-put guard reports +13.4% throughput and -11.7% p99.
Both guard medians pass the regression limits. Single-writer controls fail,
so these guards do not repair the original failed screens.

## Change and ordering

Previously the optional coalescing wait captured an admitted-ticket cutoff
once. The coordinator could reach that cutoff while the rest of a producer
wave was still being admitted. It then captured a smaller written prefix
for the next sync.

The coordinator now refreshes the optional cutoff only when its written
frontier reaches the preceding cutoff. If the refreshed cutoff is already
written, it returns immediately. Otherwise it receives more completion
results using the time remaining on the original deadline. This avoids
reading the admission mutex on every completion-loop iteration, as the
earlier standalone moving-cutoff experiment did.

The deadline remains 400 microseconds and is never reset by a refresh.
The preceding-sync eligibility threshold remains 100 microseconds, and
the bounded completion drain still runs afterward. The actual durability
target is captured once immediately before `fdatasync`; refreshing the
optional batching cutoff cannot acknowledge later writes through an
earlier sync. Public barrier and rotation cutoffs, ticket/offset ordering,
poison handling, buffer lifetimes, and MVCC publication retain their
existing contracts. PITR continues to use its existing I/O path.

This is a combined experiment on the retained drain baseline. The earlier
standalone moving-cutoff experiments and their failed +5% thresholds retain
their original outcomes. No notification experiment or forced-asynchronous
io_uring submission is included.

## Diagnostics before the change

Two private diagnostic builds use the retained drain baseline. Each has
one warmup and three observations at eight and sixteen writers. Every
primary observation executes 524,288 puts as 8,192 batches of 64, with
1 KiB values, eight Tokio workers, and a 1 GiB memtable target. They sample
every 61st native commit and timestamp all syncs and group completions.
These instrumented runs are descriptive and are excluded from scored timing.

The first build separates group readiness from contiguous-prefix readiness,
coordinator processing, and the covering sync. Sixteen-writer sampled mean
intervals, in microseconds, are:

| Interval | Observation 1 | Observation 2 | Observation 3 |
| --- | ---: | ---: | ---: |
| Admission to group write readiness | 326.1 | 620.1 | 321.7 |
| Wait for earlier groups to form a contiguous prefix | 7.3 | 11.8 | 4.7 |
| Ready prefix to coordinator processing | 107.6 | 108.4 | 119.8 |
| Coordinator processing to covering sync | 119.7 | 124.7 | 141.7 |
| Covering sync | 485.8 | 485.1 | 496.6 |

Approximately 102–113 microseconds of the ready-to-processed interval
overlaps earlier sync syscalls. Approximately 118–140 microseconds of the
processed-to-sync interval overlaps coalescing. These are elapsed intervals
on sampled commit paths, rather than sums of concurrent writer timers.

At sixteen writers, 89.9–93.8% of coalescing cycles reach the fixed cutoff;
only 6.2–10.1% hit the deadline. Increasing the timeout alone therefore does
not address the common cutoff exit. Out-of-order completion holes are a
small part of these sampled waits.

The second build separates packer dispatch, extent readiness, worker
staging, submission, and CQE consumption. Mean submission-call time across
all writes is 15.0–16.0 microseconds at sixteen writers and 19.2–23.8 at
eight. The sixteen-writer sampled submission-return-to-CQE-consumption
interval is 308.5–333.0 microseconds. This includes kernel I/O and completion
delivery/consumption; it is not a direct SSD service-time measurement.
These results do not support submission blocking as the main bottleneck.

Admission is timestamped when the WAL helper returns, so a packer can begin
before that sample. The detailed dispatch analysis clips such overlapping
stages at the admission sample. Submission-call timings across all writes
do not depend on that clipping. Ticket ranges, group/request identities,
sync coverage, and the decomposed elapsed intervals were checked.

## Frozen comparisons

Both uninstrumented arms use public native `write_batch_async`, explicit
parallel WAL, PITR and serializable transactions off, and fresh ext4
databases on `/dev/nvme0n1p3`. Each primary observation uses the diagnostic
workload size and configuration above. No rotation occurs during timing.
The measured kernel is Linux `6.12.71-1-lts`.

Each session has two warmups per arm/case, three fixed ABBA/BAAB/ABBA blocks,
and one identical-baseline pair per case. Executables are staged in RAM;
runs have five-second idle gaps. No builds, tests, traces, profiling, or
device administration overlap measurement. An anomalous run triggers an
extra one-second pause and an excluded retry. Every original stays scored.

The incremental rule, frozen before timing, requires at least +2% target
throughput, improvement in all three target blocks, at least two passing
repeat blocks per case, all null controls passing, and both case medians
within -5% throughput / +10% p99. The +2% threshold follows the maintainer's
preference for small incremental gains; it does not change older thresholds
or the RFC's leader-relative gate.

Paired changes are medians of the three block geometric-mean ratios.
Absolute arm medians use all six primary observations per arm; their quotient
is not the paired estimate. Repeat controls require each arm's throughput
spread to stay within 5% and p99 within 10% inside a block.

| Session | Writers | Baseline puts/s | Refresh puts/s | Paired throughput | Paired p99 | Paired CPU/put | Repeat blocks | Null |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Initial | 8 | 344,073 | 348,297 | +5.5% | -7.7% | +3.7% | 1/3 | Pass |
| Initial | 16 | 628,820 | 656,911 | +4.7% | -3.5% | -4.9% | 1/3 | Pass |
| Confirmation | 8 | 354,115 | 358,182 | +2.4% | -6.0% | +0.8% | 1/3 | Pass |
| Confirmation | 16 | 604,161 | 629,743 | +4.2% | -0.9% | -7.0% | 3/3 | Fail |

The initial session has 39 observations, including three excluded single
retries. Its final sixteen-writer baseline stalls at 83,745 puts/s with
45.877 ms p99; the retry also stalls. The original stays primary.
The target block throughput changes are +4.7%, +2.6%, and +182.5%.

Exactly one independent confirmation uses the same executables and criteria.
It has 37 observations, including one excluded single retry. Its target
block changes are **+4.2%, +5.6%, and +3.9%**, all with passing repeat controls.
However, the target null pair has p99 33.503 versus 38.434 ms, exceeding the
10% bound. Eight-writer repeat controls also fail. Both complete screens
therefore remain failed. Confirmation does not replace the initial session.

Paired sync-count reductions are 25.9% / 8.2% at eight / sixteen writers
initially, and 22.0% / 8.8% in confirmation. These counters support the batching
hypothesis; failed timing controls prevent attributing all observed speedup
to the code change. They are not device queue-depth measurements.

## Independent workload guards

The same executables receive a separate frozen guard session with the same
block, retry, control, and idle policies. Single-writer batch64 uses 131,072
puts and one Tokio worker; sixteen-writer one-put uses 50,000 puts and eight
workers. Both use 1 KiB values and fresh ext4 databases.

| Workload | Baseline puts/s | Refresh puts/s | Paired throughput | Paired p99 | Repeat blocks | Null |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Batch64, 1 writer | 78,854 | 79,041 | +0.1% | +2.6% | 0/3 | Fail |
| One-put, 16 writers | 25,235 | 28,305 | +13.4% | -11.7% | 2/3 | Pass |

All 39 guard observations finish, including three excluded single retries.
Both medians pass the regression limits, but the combined guard controls
fail. Single-writer runs have exactly 2,048 syncs in both arms. One-put has
a 41.3% paired sync-count reduction, with absolute sync-count medians of
5,493.5 versus 3,230.5. This comparison uses the previous parallel backend
as its baseline and establishes no new leader-relative result.

## Validation and artifacts

The private candidate passes formatting, default and all-feature Clippy
with `-D warnings`, all 1,292 library tests without skips or retries, and
three public-API probe tests. Promotion copies the exact measured runtime;
all other 114 recorded production inputs remain unchanged.

After promotion, `cargo make check` passes both Clippy configurations,
formatting, dependency checks, typos, and all 1,465 all-target tests with
no skips or retries. The separate default-feature library run passes all
1,198 tests, also without skips or retries.

The archives retain every one of the 115 uninstrumented observations and
16 diagnostic observations, source snapshots and hashes, both timing
binaries, frozen protocols, drivers, raw outputs, summaries, and independent
audits. Temporary databases and owned RAM stages were removed.

- `target/rfc024-native-sync-readiness-20261006/`: prefix/coalescing diagnostic.
- `target/rfc024-native-io-readiness-20261006/`: dispatch/submission diagnostic.
- `target/rfc024-native-cutoff-refresh-drain-20261006/`: initial screen, patch,
  checks, promotion record, and audit of all five sessions.
- `target/rfc024-native-cutoff-refresh-drain-confirmation-20261006/`: confirmation.
- `target/rfc024-native-cutoff-refresh-drain-guards-20261006/`: workload guards.

Baseline timing executable SHA-256:
`b7ab24282c690bd7289d586b13885d2d3325aa48e3787a585acbbcb89160b817`.
Retained candidate SHA-256:
`7f113ba4b8c116ea7adfb3bc058b983f7b10ad64db4023a5e93ecc62f8ecef76`.

The subsequent [submission and key-preparation experiments](rfc-024-native-submission-and-key-preparation-20261006.md)
retain this baseline. Neither additional candidate passes its frozen
incremental screen; their failed controls and original observations remain
reported separately.
