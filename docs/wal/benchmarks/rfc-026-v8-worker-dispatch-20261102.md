# RFC 026 V8 worker handoff and DATA dispatch (2026-11-02)

## Result

Neither candidate is retained. The worker, producer framing, admitted DATA
groups and durability coalescing have been restored to the implementation
present before this screen. No RFC digest, wire, recovery, FRONTIER or sync
rule changed during either experiment.

Publishing DATA completion directly from the I/O worker removed a thread
handoff and reduced point-write CPU, but worsened concurrent point-write p99.
Forcing large DATA writes through io_uring async dispatch also failed the
batch throughput/p99 guards. A mechanism that helps V7 does not establish a
V8 improvement without measurement.

The preserved baseline's best eligible batch throughput in this screen is
81,458 / 327,763 / 447,987 puts/s at 1 / 8 / 16 writers. V8 has not demonstrated
V4 performance parity. The RFC removes payload hashing from the ordered
coordinator; that alone does not establish an end-to-end win over V4, which
does not calculate the cryptographic prefix or write FRONTIERs.

## Method

All measurements use release `write-perf --features bench`, 1 KiB values,
Parallel I/O, no compaction, a 2 GiB target SST and latency sampling every ten
operations. Batch measurements contain 750,000 puts in batches of 64. Point
measurements contain 5,000 puts at one writer and 20,000 at eight or sixteen.
Database paths and preserved executables remain in the local repository.
Storage is ext4 on `/dev/nvme0n1p3`, a Solidigm P41 Plus.

The [plans and executable hashes](artifacts/rfc-026-v8-worker-dispatch-20261102-plans.json)
record each schedule before it ran. Initial A/B batch comparisons reverse
execution order in the second round. One explicitly planned cooled follow-up
repeats the inline candidate's missing solo/point guards after a 45-second
pause. The async-dispatch experiment restores the original reaper and changes
only the large-DATA dispatch option. Experiments and cooled follow-ups are
reported separately in the [summary](artifacts/rfc-026-v8-worker-dispatch-20261102-summary.json).

The unchanged exploratory stall flag is p99 above 10 ms or mean fdatasync
above 1 ms. All 50 completed records, including flagged samples, are preserved
in the [raw data](artifacts/rfc-026-v8-worker-dispatch-20261102-raw.jsonl).
Passing this aggregate flag does not prove absence of isolated stalls: some
eligible samples have maxima above 30 ms. No sample is reclassified because
its result is unfavorable.

Tables compare the highest eligible throughput for each arm with the p99
from that same measurement. Medians and eligible/total counts remain in the
summary. These small, noisy cohorts are optimization screens, not RFC adoption
qualification or confidence intervals.

## Inline completion publication: rejected

The baseline sends each `GroupWriteResult` through a channel to a dedicated
V8 completion reaper. An exploratory user-cycle profile showed work in both
the worker command receiver and this completion receiver. The profile includes
lifecycle work outside the measured interval, so its percentages are not
precise timed-region attribution.

The [candidate](artifacts/rfc-026-v8-worker-dispatch-20261102-inline-completion.patch)
adds an optional inline completion handler to the shared worker. It releases
the group's permit before publishing DATA readiness and notifies the V8
coordinator if the worker stops, including during unwinding. V4/V7 retain
their existing completion channel. Admission, grouping, producer hashing and
the one-sync durability cycle are unchanged.

| Workload | Writers | Baseline puts/s | Baseline p99 ms | Candidate puts/s | Candidate p99 ms | Throughput | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Batch 64 | 1 | 73,721 | 1.326 | 71,958 | 1.320 | -2.4% | -0.5% |
| Batch 64 | 8 | 327,763 | 2.815 | 332,713 | 2.683 | +1.5% | -4.7% |
| Batch 64 | 16 | 440,634 | 4.899 | 445,352 | 4.883 | +1.1% | -0.3% |
| Point | 1 | 1,575 | 1.319 | 1,641 | 1.245 | +4.2% | -5.6% |
| Point | 8 | 8,771 | 1.735 | 8,988 | 1.880 | +2.5% | +8.3% |
| Point | 16 | 8,561 | 3.791 | 8,531 | 4.363 | -0.4% | +15.1% |

Eligible counts for baseline/candidate are 1/3 and 1/3 for solo batch, 2/2
and 2/2 for each concurrent batch workload, 2/2 and 2/2 for solo point, and
1/2 and 1/2 for each concurrent point workload. The cooled solo-batch and
concurrent-point comparisons are adjacent pairs. Point-eight CPU falls from
4.385 to 3.890 seconds; point-sixteen CPU falls from 7.179 to 6.749 seconds.
Those CPU reductions do not compensate for the p99 failures.

The two eight-writer batch rounds are also not uniformly positive. The first
pair improves throughput 4.4%; the reverse-order pair loses 1.2% throughput
and raises p99 7.8%. At sixteen writers, median p99 rises from 4.876 to
5.002 ms even though the fastest-score comparison is slightly better.
The extra shared-worker API and thread-lifecycle changes are not justified
by this screen, so the complete candidate is reverted.

## Forced async DATA dispatch: rejected

V7's ext-family writer uses `IOSQE_ASYNC` for DATA writes at least 64 KiB,
keeping large allocation/conversion work off the ring's issuing thread. V8
uses ordinary dispatch. The [second candidate](artifacts/rfc-026-v8-worker-dispatch-20261102-async-dispatch.patch)
tests that same size threshold for V8, with the original completion reaper
and all grouping policy restored. It does not import V7 extent lookahead or
initialization behavior.

| Writers | Baseline batch puts/s | Baseline p99 ms | Candidate puts/s | Candidate p99 ms | Throughput | p99 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 81,458 | 1.284 | 80,027 | 1.385 | -1.8% | +7.9% |
| 8 | 319,949 | 2.358 | 305,250 | 3.285 | -4.6% | +39.3% |
| 16 | 447,987 | 4.477 | 405,904 | 4.945 | -9.4% | +10.5% |

Solo eligibility is 2/2 for each arm; at eight writers it is 1/2 baseline
and 2/2 candidate; at sixteen it is 1/2 for each. The first eligible paired
eight-writer run loses 12.6% throughput, while syncs rise from 1,497 to 1,729
and coalescing wait from 142 to 374 ms. At sixteen writers, syncs rise from
1,302 to 1,515 and CPU from 10.223 to 11.116 seconds. These counters support
a changed completion/coalescing pattern, not a proven reduction in issuing
thread cost.

The point guards are preserved too. Point-eight best throughput changes
9,504 to 9,569 puts/s with p99 1.523 to 1.566 ms. Point-sixteen changes
10,163 to 11,554 puts/s with p99 3.535 to 3.510 ms. These single-frame writes
never meet the candidate's size threshold, so the apparent point-sixteen win
cannot be attributed to forcing those DATA writes async. It also illustrates
the environment/scheduling variation in this screen. Solo point has no
eligible baseline in this experiment.

## Baseline versus V4/V7 context

The V4/V7 context runs use the earlier inline-candidate executable; their
format paths retain completion channels. The V8 column selects the best
eligible preserved-baseline executable result across both experiments and
the cooled follow-up. These are different timing cohorts and executables,
not a simultaneous paired format comparison.

| Writers | V4 puts/s / p99 ms | V7 puts/s / p99 ms | V8 baseline puts/s / p99 ms |
|---|---:|---:|---:|
| 1 | 79,367 / 1.205 | stall-flagged | 81,458 / 1.284 |
| 8 | 377,648 / 2.691 | 351,321 / 2.825 | 327,763 / 2.815 |
| 16 | 564,614 / 3.250 | 540,844 / 4.746 | 447,987 / 4.477 |

For the user's best-score comparison, the earlier
[historical V4 highs](rfc-025-v7-dynamic-group-comparison-20261030.md) are
85,618 / 388,372 / 595,941 puts/s. This screen's V8 baseline best is 4.9% /
15.6% / 24.8% below those at 1 / 8 / 16 writers. Historical scores are
directional context; they are not current source-effect estimates.

### CPU gap and remaining hypotheses

The selected V8 scores also expose a concurrency-dependent user-CPU gap. The
V8 numbers below are the best eligible preserved-baseline records in this
screen; the V4 numbers are the single format-control records. They ran in
different rounds and are directional comparisons, not paired source-effect
measurements.

| Writers | V8 user CPU s | V4 user CPU s | V8/V4 user CPU | V8/V4 syncs |
|---:|---:|---:|---:|---:|
| 1 | 3.569 | 3.578 | 1.00x | 11,719 / 11,719 |
| 8 | 6.380 | 3.914 | 1.63x | 1,541 / 2,619 |
| 16 | 8.095 | 4.310 | 1.88x | 1,302 / 1,600 |

These user-CPU readings come from the timed write interval. V8's extra CPU
appears as writer concurrency rises, while its selected 8- and 16-writer runs
do not have more syncs than the V4 controls. Sync count alone therefore does
not explain the gap. The V8 payload-hash and ordered-link CPU counters are
sampled at one ticket in 64; they are not full-workload totals and must not be
compared directly with process CPU.

Code inspection gives a concrete profiling target, not a proven cause: the
V8 coordinator serializes admission and readiness/status publication through
one state mutex. Frame-ready, digest-ready and DATA-completion events each
signal a shared condition variable with `notify_all`; several status reads
also take the state mutex, and a frontier snapshot copies the ready prefix.
These paths are in `kv-engine/src/wal/v8/parallel.rs` and
`kv-engine/src/wal/v8/leader.rs`. Measure timed-region lock wait, wakeups and
per-thread CPU before changing them. The RFC's ordered digest fold is small,
but V8 still performs payload hashing, frame CRCs, and FRONTIER work that V4
does not; the RFC removes ordered payload hashing, not those costs or all
coordination overhead.

The V8 digest counters still report zero post-encode hash reread bytes and
producer-side entry hashing. Hash CPU fields are raw samples from one ticket
in 64, not full-workload CPU totals. The remaining performance gap needs
further attribution; neither worker change establishes its cause or solves it.

## Correctness and final state

The inline prototype passed 179 V8/shared-worker tests, including real
io_uring completion, failed-write and worker-panic shutdown probes, plus the
249-test existing WAL/runtime suite. All-feature/all-target Clippy and ordinary
library checking passed. After reverting both candidates, the combined V8,
shared-worker and WAL/runtime selection passed all 392 tests outside the
sandbox. The earlier, uncommitted implementation work is preserved exactly.
This screen retains no implementation change; it records the rejected
prototypes and their evidence.
