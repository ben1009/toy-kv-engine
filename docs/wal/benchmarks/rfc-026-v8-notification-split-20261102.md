# RFC 026 V8 notification separation (2026-11-02)

## Decision (updated after maintainer review)

The combined notification and published-cursor implementation is restored and
**kept locally at the maintainer's explicit request**. The stream-only and
driver-only variants are not retained independently. See the
[follow-up rerun](rfc-026-v8-notification-rerun-20261102.md) for the new
measurements and current retention decision. No format default is changed,
and this experiment does not merge PR #532.

The original decision was to revert: the initial solo confirmation lost 3.2%
throughput, raised p99 5.2% and used 4.2% more CPU, while sixteen-writer point
samples were entirely stall-flagged. Those measurements and limitations below
are preserved. Maintainer retention supersedes the original revert; it does
not reclassify those samples or turn them into a passed performance gate.

This experiment follows the
[timed V8/V7 attribution](rfc-026-v8-coordination-attribution-20261102.md).
It changes only notification subscriptions, frontier-driver handoff and how
completing callers consume a published cursor. Digest formulas, canonical
encoding, frame bytes, producer framing, DATA grouping limits/waits and the
existing 100/400 µs durability coalescing policy are unchanged.

## Method

Release `write-perf --features bench`, V8 Parallel, 1 KiB values, no compaction,
a 2 GiB target SST, latency sampling every ten operations, ext4 on the Solidigm
P41 Plus. Batch tests contain 750,000 puts in batches of 64. Point guards use
5,000 puts at one writer and 20,000 at eight or sixteen writers. All databases,
executables and complete source snapshots remain in the local repository under
`target/rfc-026-v8-notification-split-20261102/`.

[Plans and executable hashes](artifacts/rfc-026-v8-notification-split-20261102-plans.json)
preserve the schedules written before execution. Each cohort reverses order
in its second round. Confirmation and guard cohorts use their recorded fixed
cooldowns; no wait, grouping, hash or storage parameter was tuned between
samples of an arm. Compilers and tests do not run during measurements.

The exploratory stall flag remains **p99 above 10 ms or mean fdatasync above
1 ms**. The tables select the highest eligible throughput per arm in that
cohort, with p99, CPU and counters from that same sample. No unfavorable
sample is reclassified, and no missing eligible arm is filled from another
cohort. Passing this aggregate flag does not establish an absence of isolated
stalls. These small cohorts are optimization screens, not confidence intervals
or RFC adoption qualification.

All **96 measurements**, including **52 stall-flagged samples**, are in the
[full measurement archive](artifacts/rfc-026-v8-notification-split-20261102-measurements.jsonl).
The [summary](artifacts/rfc-026-v8-notification-split-20261102-summary.json)
includes sample identities, eligible/total counts and medians. Raw source paths
and file hashes are recorded. The guard runner was resumed after its first
completed sample because its reporting loop shadowed Python's `round` function.
That sample was preserved; the original schedule and executable hashes were
verified before resuming. The resulting unplanned pause is another limitation
of that guard cohort. No source or benchmark parameters changed during resume.

The temporary [context-switch probe](artifacts/rfc-026-v8-notification-split-20261102-context-probe.patch)
reads `getrusage(RUSAGE_SELF)` before and after the timed batch interval and
writes its result after timing ends. It adds no per-ticket instrumentation.
The [formatted probe](artifacts/rfc-026-v8-notification-split-20261102-formatted-context-probe.patch)
has the same behavior in the candidate builds. Point measurements have no
context-switch probe; missing context values mean unmeasured, not zero.
Total CPU is the measured process user plus system CPU.

## 1. Separate preparation from durability progress

The [first patch](artifacts/rfc-026-v8-notification-split-20261102-split.patch)
adds a durability-progress generation and condition variable. Frontier waits
and the existing coalescer subscribe to this stream. Admission, frame readiness
and FRONTIER reservation no longer wake those subscribers. DATA completion,
durable publication, close and poison do. A late digest also notifies when
its ordered fold extends an already DATA-complete contiguous prefix; otherwise
recovery-independent runtime progress could wait for a nonexistent future CQE.

The original broad stream still serves admission/capacity and the staged
generic wait API. This patch neither introduces atomic status mirrors nor
replaces the already preallocated ticket queue.

| Writers | Baseline puts/s | Candidate puts/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU s | Candidate CPU s | Voluntary switches, baseline → candidate |
|---|---:|---:|---:|---:|---:|---:|---:|
| 8 | 338,140 | 320,086 | 3.323 | 3.665 | 7.796 | 8.037 | 143,510 → 103,587 |
| 16 | 429,006 | 438,652 | 5.351 | 4.728 | 10.861 | 10.463 | 176,838 → 125,698 |

The initial screen has one eligible eight-writer sample per arm and one
eligible sixteen-writer candidate. Its unchanged-source confirmation has
eight stall-flagged samples and supplies no new eligible score. The narrower
stream reduces context switches, but alone does not pass the eight-writer
throughput/p99 guard.

## 2. Separate the frontier driver from durable followers

In the first patch, every waiting caller still reacts to DATA completion.
The [second patch](artifacts/rfc-026-v8-notification-split-20261102-roles.patch)
elects one caller to drive FRONTIER formation. Other callers subscribe to a
separate durable-publication condition variable and recheck their own ticket
boundary under the coordinator mutex. They wake for durable publication,
driver handoff, close or poison, rather than producer preparation or CQEs.

Driver ownership is independent of the physical FRONTIER mutex. A follower
can finish while the driver still owns a later ticket. The physical mutex
remains scoped to one existing FRONTIER/sync iteration. This differs from
the rejected experiment that held that mutex across a caller's entire wait.
A failed or unwinding driver poisons the coordinator before releasing its
role, so another caller cannot append a FRONTIER after an uncertain cycle.
Already-admitted ready work can still drain after admission closes.

### Publication CPU diagnostic

The [temporary Linux-only publication probe](artifacts/rfc-026-v8-notification-split-20261102-publication-cpu-probe.patch)
uses the previous attribution backend but enables only memtable and MVCC
publication scopes. It does not install the allocator wrapper, sample stacks,
or enable detailed WAL diagnostics. Each phase samples one call per 64 per
thread. Raw sampled CPU/wall values and scaled estimates are preserved in the
[publication data](artifacts/rfc-026-v8-notification-split-20261102-publication-cpu.json)
and measurement archive. Scope timings include measurement overhead and are
inclusive; do not add arbitrary nested estimates.

Only one sample per arm is eligible in this four-run reversed diagnostic
cohort. It is not a normal-binary retention score.

| Metric | Baseline V8 | Role-separated V8 |
|---|---:|---:|
| Puts/s | 433,557 | 430,123 |
| p99 ms | 5.241 | 4.743 |
| Total CPU s | 10.913 | 9.651 |
| Voluntary context switches | 174,672 | 108,290 |
| Sampled/scaled memtable CPU s | 3.450 | 3.354 |
| Sampled/scaled MVCC publication CPU s | 0.933 | 0.473 |
| Syncs | 1,297 | 1,297 |
| Actual eventfd writes | 4,205 | 4,220 |

This pair supports a reduction in MVCC publication spinning/coordination
cost: total CPU falls 11.6%, switches 38%, and estimated MVCC publication CPU
about 49%. It does **not** establish a comparable reduction in memtable cost
or eventfd writes. The whole proposed causal chain is not demonstrated.
These phase numbers belong to the second patch, before the cursor change
below, and must not be relabeled as final-candidate phase measurements.

Its normal-binary guard cohort still loses batch throughput: best eligible
eight-writer throughput is 293,847 versus 334,755 puts/s; sixteen-writer
throughput is 402,206 versus 438,670. Point-eight CPU improves, but this
variant is not retained independently.

## 3. Complete without rejoining a later physical FRONTIER cycle

The follower's return path still called `active_boundaries()` and `image_len()`
inside `Wal::refresh_v8_parallel_cursor`. Both acquire the physical FRONTIER
mutex. A ticket could therefore be durable and its follower notified, yet
completion would still wait behind the next caller's coalescing/sync cycle.

The [combined patch](artifacts/rfc-026-v8-notification-split-20261102-cursor.patch)
consumes the existing coordinator's published durable ticket end, published
FRONTIER image end and admitted ticket end from one mutex snapshot. The
published image end is the covering marker's physical end, not the DATA
`durable_end` inside that marker. DATA reservations and a reserved but
unpublished FRONTIER cannot advance the published cursor. The existing
coordinator validates these fields at construction/publication. Checkpoint,
sealing and anchor capture retain their physical-boundary validation paths.

This is not an atomic status mirror or a new digest/cache representation.
It makes durable-only notification useful through the complete caller path.
The cursor helper is now infallible because it reads already-published state;
WAL I/O errors, poison handling and the final ticket-before-ACK check remain.

The first combined screen is promising but incomplete:

| Writers | Baseline puts/s | Candidate puts/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU s | Candidate CPU s | Voluntary switches, baseline → candidate |
|---|---:|---:|---:|---:|---:|---:|---:|
| 8 | 317,057 | 317,490 | 3.030 | 2.595 | 8.419 | 8.022 | 131,425 → 85,661 |
| 16 | 449,578 | 504,247 | 4.163 | 3.679 | 9.988 | 8.428 | 175,690 → 96,246 |

There is one eligible baseline-eight sample and one eligible candidate-sixteen
sample. The other two arms each have two. CPU falls 4.7% and 15.6%; sixteen-
writer throughput rises 12.2%. Eventfd writes at sixteen writers barely
change: 4,193 versus 4,155. No V4/V7 performance-parity claim follows from
this cohort.

### Full guards and final confirmation

The full combined guard cohort runs all six point/batch cases in reversed
order. Each arm has two samples per case. Most batch cases have only one
eligible sample per arm; neither sixteen-writer point arm has one.

| Workload | Writers | Baseline puts/s | Candidate puts/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU s | Candidate CPU s |
|---|---:|---:|---:|---:|---:|---:|---:|
| Batch 64 | 1 | 73,191 | 71,299 | 1.308 | 1.341 | 5.787 | 5.981 |
| Batch 64 | 8 | 323,287 | 315,373 | 2.601 | 2.664 | 8.285 | 8.153 |
| Batch 64 | 16 | 379,867 | 486,532 | 5.446 | 4.059 | 10.970 | 9.501 |
| Point | 1 | 1,562 | 1,594 | 1.254 | 1.239 | 0.869 | 0.826 |
| Point | 8 | 9,103 | 9,510 | 1.640 | 1.579 | 4.140 | 3.165 |
| Point | 16 | no eligible sample | no eligible sample | — | — | — | — |

A final focused cohort repeats batch-one, batch-eight and point-sixteen with
the **same binaries**, two reversed rounds and one fixed cooldown. This
completed the original experiment; the later maintainer-requested rerun is
reported separately.

| Workload | Writers | Baseline puts/s | Candidate puts/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU s | Candidate CPU s |
|---|---:|---:|---:|---:|---:|---:|---:|
| Batch 64 | 1 | 71,064 | 68,758 | 1.314 | 1.382 | 6.133 | 6.393 |
| Batch 64 | 8 | 311,596 | 315,546 | 3.381 | 2.695 | 8.619 | 8.103 |
| Point | 16 | no eligible sample | no eligible sample | — | — | — | — |

Both solo arms have two eligible samples. The solo regression repeats the
direction of the full guard cohort and fails the retention gate. Eight-writer
batch results remain mixed across cohorts. All four sixteen-writer point
samples in this last cohort are flagged; no conclusion uses them as a win or
loss. Clear CPU savings in other cases do not fill those missing guards or
cancel the solo failure.

## Correctness and reproduction

The stream-only patch passes 151 V8-related tests; adding driver/follower
roles passes 155; completing the cursor path passes 157. All-target,
all-feature Clippy passes for each variant, outside the scored intervals.
Tests cover preparation not releasing durability subscribers, late digests
after DATA completion, close/poison wakeups, driver handoff and failure, an
earlier follower finishing while a driver owns a later ticket, cursor bounds,
and cursor reads while the physical FRONTIER mutex is held. Existing V8
recovery, checkpoint, migration and crash-boundary tests also pass.

Before reverting, the combined prototype passes the complete `./dev check`:
**2,151 tests pass, 32 are skipped, one is slow**; formatting, dependency
sorting, Clippy, unused-dependency and typo checks pass. The
[check record](artifacts/rfc-026-v8-notification-split-20261102-checks.json)
points to the complete local log. That check was followed by the original
revert to `fdde0670`. The maintainer-requested restoration now reinstates the
same checked combined implementation byte-for-byte. The archived check record
describes the original experiment; the follow-up report records the current
kept state.

Patches apply independently to `fdde0670`; the role and cursor patches include
their preceding steps and must not be applied on top of the earlier patch.
The normal binaries add only the optional context-switch probe. The CPU
diagnostic additionally enables the two publication scopes. All temporary
probes are removed from the working source.

Recording scripts are preserved with the plans:
[stream screen](artifacts/rfc-026-v8-notification-split-20261102-run.py),
[stream confirmation](artifacts/rfc-026-v8-notification-split-20261102-retry.py),
[role screen](artifacts/rfc-026-v8-notification-split-20261102-roles.py),
[publication CPU](artifacts/rfc-026-v8-notification-split-20261102-cpu.py),
[role guards](artifacts/rfc-026-v8-notification-split-20261102-guards.py),
[cursor screen](artifacts/rfc-026-v8-notification-split-20261102-cursor.py),
[cursor guards](artifacts/rfc-026-v8-notification-split-20261102-cursor-guards.py)
and [final confirmation](artifacts/rfc-026-v8-notification-split-20261102-confirmation.py).
Their executable/artifact paths refer to the original local target directory
and need adjustment when reproducing elsewhere. Build with
`cargo build --release --package kv-engine --features bench --bin write-perf`.

The useful result is narrower than the initial hypothesis: unrelated
notifications do contribute to coordination and MVCC publication cost, and
the completion-metadata mutex can defeat early durable notification. These
original measurements did not establish preserved solo performance or qualify
the missing concurrent-point guard. The follow-up retains the implementation
by maintainer decision while continuing to report those limits explicitly.
