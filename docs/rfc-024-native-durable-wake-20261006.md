# RFC 024: native durability wakeups, 2026-10-06

Neither notification experiment is retained. Ticket-indexed Tokio notifications
remove premature durability resumptions in the diagnostic, but the uninstrumented
sixteen-writer screen reports **-4.6% paired throughput**, with failed repeat
controls. Moving successful-sync notifications after the durability mutex is
released initially reports +2.4%; an independent confirmation reports **-0.4%
throughput and +1.3% p99**, with all three target repeat controls passing.
The confirmation rejects that candidate.

Both comparisons use the retained
[bounded completion drain](rfc-024-native-completion-drain-20261006.md) as their
baseline. Its retention commit is `ecd07b92`; the measured working-tree inputs
are separately frozen for reproduction. Every arm explicitly selects native
async parallel WAL. These are incremental parallel comparisons, rather than
new leader comparisons or a full RFC performance qualification.

## Measured wakeups

The existing successful-sync path wakes all async durability waiters. Each
waiter then checks its own ticket against the acknowledged durable frontier.
A wake can therefore resume a writer whose ticket belongs to a later sync.

Private builds count completed waits, immediate successes, resumptions, and
resumptions whose ticket remains outside the durable prefix. Each case has
three observations after one warmup: 524,288 puts per observation, batch64,
1 KiB values, eight Tokio workers, PITR and serializable transactions disabled,
and a 1 GiB memtable target. There is no timed rotation. The counters are absent
from all performance-screen executables.

The following totals exclude warmups and cover 24,576 successful commits per
arm and writer count. Every measured wait resumes at least once; none completes
immediately.

| Writers | Baseline resumptions | Baseline premature resumptions | Premature fraction | Ticket-indexed resumptions | Ticket-indexed premature resumptions |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 | 42,716 | 18,140 | 42.5% | 24,576 | 0 |
| 16 | 47,946 | 23,370 | 48.7% | 24,576 | 0 |

In every observation, `resumptions = completed - immediate + premature`.
Completed waits, admitted buffers, batch samples, and write CQEs also reconcile.
Notification rounds can include final shutdown wakeups; they are not a count
of device requests. These counters demonstrate redundant wakeups, not the
fraction of elapsed commit time spent handling them.

The earlier
[critical-path diagnostic](rfc-024-native-completion-drain-20261006.md#commit-critical-path)
places most sampled commit time between WAL admission and durability
acknowledgement, around 1.1–1.4 ms at sixteen writers. Durable-ready to task
resumption takes about 63–75 microseconds and includes notification, mutex
release, scheduling, and predicate checking. Notification while holding the
mutex takes about four microseconds per sync. Removing redundant resumptions
does not remove the preceding wait for durability.

## Private candidates

The ticket-indexed candidate replaces the single async `Notify` with 64 fixed
slots. A ticket chooses its slot modulo 64. Successful syncs notify only slots
covered by the newly acknowledged interval, `[previous_durable, durable)`;
advances spanning at least 64 tickets visit every slot once. Slot collisions
can cause an extra predicate check, but never bypass it. Poison and terminal
shutdown still notify every slot, and synchronous condition-variable wakeups
retain their existing behavior.

It preserves registration before predicate checking, the captured sync target,
poison boundaries, and cancellation behavior. Two private unit tests cover
slot wraparound, sleeping later tickets, global terminal wakeups, empty
advances, and advances larger than the slot count. There is no per-wait
allocation or additional timed wait.

A historical ticket-indexed **condition-variable** experiment was rejected
before native async integration; see the
[earlier benchmark record](rfc-024-parallel-wal-benchmark.md#ticket-indexed-durability-wakeups-rejected-2026-09-29).
This experiment measures Tokio notifications on the current native path and
does not change that historical result.

The second candidate starts independently from the same drain baseline and
has no ticket-indexed slots. It moves the successful-sync `notify_change()`
call immediately after `drop(state)`. The frontier still advances under the
durability mutex; awakened waiters read that state using the same predicate
and registration protocol. Failure notifications retain their ordering after
admission poison is installed. No buffer, I/O, coalescing, or publication
protocol changes accompany either candidate.

## Frozen performance comparisons

All three sessions run on ext4 on `/dev/nvme0n1p3`, kernel `6.12.71-1-lts`,
with CPU affinity 0–31. Primary observations use the diagnostic workload above,
but contain no added diagnostic counters or timestamps. Both arms call the
public native `write_batch_async` API. Databases are fresh, executables are
staged in RAM, and runs have five-second idle gaps. Builds, tests, profiling,
tracing, and device administration run separately from measurement.

Each case has two warmups per arm, three fixed ABBA/BAAB/ABBA blocks, and one
identical-baseline null pair after block 1. A block compares the geometric
means of its two observations per arm. The paired estimate is the median of
all three block ratios; the quotient of absolute arm medians is a different
estimator. Repeat controls require each arm's throughput max/min to stay
within 5%, and its p99 within 10%.

Before timing, the incremental retention target is frozen at **+2%** for
sixteen-writer paired throughput, following the maintainer's preference for
keeping modest gains. All three target blocks must be positive, each case
must have at least two passing repeat blocks, and all null controls must pass.
Both cases must also remain within -5% throughput and +10% p99. Independent
single-writer and one-put workload guards are required before promotion.
This rule does not revise earlier experiments' +5% targets or the RFC gate.

An obvious anomaly pauses for an extra second and triggers one separately
labelled retry. The original always remains primary. A severely regressed
comparison block triggers an excluded reversed-order block retry. Protocols
and scripts specify the thresholds before measurement.

| Session | Writers | Baseline puts/s | Candidate puts/s | Paired throughput | Paired p99 | Paired CPU/put | Passing repeat blocks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Ticket-indexed | 8 | 358,094 | 358,956 | +1.1% | +0.1% | -5.8% | 1/3 |
| Ticket-indexed | 16 | 612,205 | 600,845 | -4.6% | +6.3% | +5.4% | 1/3 |
| Notify after unlock, initial | 8 | 350,429 | 360,721 | +1.7% | -1.9% | -4.5% | 2/3 |
| Notify after unlock, initial | 16 | 593,244 | 618,177 | +2.4% | -8.3% | -8.2% | 1/3 |
| Notify after unlock, confirmation | 8 | 351,994 | 348,316 | -3.2% | -0.7% | +5.9% | 2/3 |
| Notify after unlock, confirmation | 16 | 628,266 | 616,307 | -0.4% | +1.3% | +0.6% | 3/3 |

All null pairs and paired workload-regression medians pass. Neither initial
screen passes its complete retention rule.

The ticket-indexed sixteen-writer blocks are -1.4%, -50.8%, and -4.6%. One
primary candidate observation reaches 44.6 ms p99; its excluded single retry
reaches 27.8 ms. The session completes 41 observations: eight warmups,
24 primary runs, four null runs, one single retry, and four block retries.
The candidate remains private.

The initial notify-after-unlock target blocks are +2.4%, +2.0%, and +187.2%.
The last contains a baseline observation at 76,231 puts/s and 79.4 ms p99,
followed by an excluded retry at 96,072 puts/s and 22.9 ms p99. That large block
ratio is not an optimization gain. All originals remain in the paired median.
The session completes 37 observations, including one excluded single retry.
Its target repeat controls fail despite the positive median.

One independently frozen confirmation then uses the identical binaries,
workload, estimator, and controls. It preserves the original failed screen
and allows no further target rerun in this investigation. The sixteen-writer
blocks are **+0.8%, -1.9%, and -0.4%**, all with passing repeat controls.
The confirmation passes its controls and regression medians, but fails both
the +2% target and the requirement for all target blocks to be positive.

The confirmation completes 38 observations, including two excluded single
retries after its last eight-writer block encounters large tails in both
arms. A baseline warmup also has a large tail. Large tails in both binary
families do not establish their cause or justify discarding any observation.
The confirmation does not reproduce the initial gain, so the unlock candidate
also remains private. Single-writer and one-put promotion guards are not run
because the performance screens already fail.

## Verification and reproduction

The ticket-indexed candidate passes 1,294 all-feature library tests, including
its two additional notification tests. One existing compaction integration
test times out once and passes the configured retry. The unlock candidate
passes 1,292 all-feature library tests without a retry. Each passes all-target
Clippy with default and all features under `-D warnings`, formatting, and the
probe's three API, flush, close, and recovery tests. Both counter-instrumented
probe builds also pass their three regression tests. io_uring checks and
measurements run outside the sandbox.

Independent audits reconcile every raw observation, the fixed block orders,
retry roles, absolute and paired estimates, null controls, and frozen inputs.
The input manifests contain 250 files for each initial screen and 257 for
confirmation. All 114 production input hashes match throughout and after the
investigation. Executable and driver hashes also match. Owned databases and
RAM stages are removed; all observations and frozen sources remain under
ignored `target/`.

| Executable | SHA-256 |
| --- | --- |
| Retained drain baseline | `b7ab24282c690bd7289d586b13885d2d3325aa48e3787a585acbbcb89160b817` |
| Ticket-indexed candidate | `a1f5a4f8d1f435c55288a07e45fa4017685d785976b56e96312a1d824636d812` |
| Notify-after-unlock candidate, both sessions | `d0553f5092e4abeabece09fd7c6a81c0b62e25f1d03946d6a80055add2303b3e` |

Local artifacts:

- Wake-counter observations: `target/rfc024-native-durable-wake-20261006/diagnostic-records.json`, candidate counter observations: `target/rfc024-native-durable-wake-20261006/candidate-diagnostic/diagnostic-records.json`, and counter audit: `target/rfc024-native-durable-wake-20261006/diagnostic-verification.json`.
- Ticket-indexed protocol: `target/rfc024-native-durable-wake-20261006/screen-protocol.json`, summary: `target/rfc024-native-durable-wake-20261006/screen-summary.json`, all observations: `target/rfc024-native-durable-wake-20261006/screen-records.json`, and audit: `target/rfc024-native-durable-wake-20261006/verification.json`.
- Unlock initial protocol: `target/rfc024-native-wake-after-unlock-20261006/screen-protocol.json`, summary: `target/rfc024-native-wake-after-unlock-20261006/screen-summary.json`, all observations: `target/rfc024-native-wake-after-unlock-20261006/screen-records.json`, and audit: `target/rfc024-native-wake-after-unlock-20261006/verification.json`.
- Independent confirmation protocol: `target/rfc024-native-wake-after-unlock-confirmation-20261006/screen-protocol.json`, summary: `target/rfc024-native-wake-after-unlock-confirmation-20261006/screen-summary.json`, all observations: `target/rfc024-native-wake-after-unlock-confirmation-20261006/screen-records.json`, and audit: `target/rfc024-native-wake-after-unlock-confirmation-20261006/verification.json`.

This round establishes no additional retained throughput improvement. The
bounded drain remains the previously selected optimization; full RFC
performance qualification remains **UNQUALIFIED**.
