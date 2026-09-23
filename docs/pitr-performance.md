# PITR Performance Baseline

Measured on 2026-09-17 with commit `4dce423d` and the in-tree `pitr-perf`
harness.

## Method (the current harness)

```bash
cargo run --release --bin pitr-perf -- --operations 10000
```

The command was run three times. For each writer count the harness runs one
PITR-disabled and one PITR-enabled case, and the order of the two alternates by
case position - PITR first in the first, third and fifth cases (1, 8 and 32
writers), second in the others - so that host state drifting over a run -
thermal, page cache, neighbouring load - cannot land on one mode only. That
order is a property of the case rather than of a run, so each result carries the
same `mode_order` field naming PITR's position in its case, and a pair is read
against it. Each case creates a fresh database and writes 10,000 unique keys
with 128-byte values. WAL is enabled in both modes. PITR uses an unlimited
archive-I/O rate and a segment large enough that foreground timing contains
WAL-v5 admission/encoding but no automatic archive boundary. The enabled case
then times an explicit durable recovery point separately as `catchup_seconds`.

The write timer starts before the writer barrier is released, so it covers the
whole window the writers are running. **The table below is not comparable to a
fresh run of the current harness.** It was measured with an earlier revision that
differed in three ways: it started the timer after the barrier, so writes a
writer completed in between fell outside the interval and inflated
`writes_per_second`; it ran PITR-disabled before PITR-enabled for every writer
count, with no alternation to counterbalance ordering; and it emitted no
`mode_order` field at all.

Host: Linux 6.18.9, Intel Core i9-13900T, 32 logical CPUs. The database and
repository were under `/tmp` on tmpfs, so these numbers are a reproducible CPU
and synchronization baseline, not a storage-device durability benchmark.

## Median of three runs, fixed order

| Writers | PITR disabled writes/s | PITR enabled writes/s | Enabled / disabled | Catch-up seconds |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 125,610 | 156,540 | 124.6% | 0.140 |
| 4 | 145,318 | 122,176 | 84.1% | 0.140 |
| 8 | 194,633 | 166,998 | 85.8% | 0.142 |
| 16 | 72,562 | 60,686 | 83.6% | 0.156 |
| 32 | 16,005 | 14,893 | 93.1% | 0.154 |

The 4- and 16-writer inversions show that this short tmpfs workload is noisy;
they are not evidence that PITR improves throughput. The stable conclusion is
that the enabled foreground path stayed in the same order of magnitude, while
copying, catalog publication, and source-manifest completion were isolated in
the explicit 0.14–0.16 second catch-up measurement.

Device-backed same-device/separate-device archive tests and configured-rate
backlog tests remain environment-specific follow-ups; this baseline does not
claim their results.

## Regression check at the merged PITR head (2026-09-22)

The table above is the slice-12 baseline. This section answers the question asked
after the last PITR layer merged - did that work slow the engine down for the default,
PITR-disabled configuration? - then two follow-ups to it: whether any other workload
moved, and what enabling PITR costs at the head.

Two baselines, both on `main`:

- `b5ac2064` - main immediately before PR #330 merged, so a difference against it is
  attributable to that PR's 14 files.
- `2f556ccb` - main's tip before the first PITR commit (`#294`) landed, so a
  difference against it covers the whole PITR line of work *and* everything else
  merged alongside it.

Head under test: `494a20ab`.

### Method

Three measurements, one process per run:

1. **Steady state** - `write-perf --suite legacy --no-wal`, 200,000 writes and
   100,000 reads at 1 KiB values on 4 threads. The three revisions are run
   **interleaved** - every repetition runs all three back to back - so host drift
   lands on all of them rather than on whichever ran last. 9 repetitions.
2. **All legacy workloads** - `write-perf --suite legacy --wal`, WAL on so the rows
   that require it are not skipped. That is every legacy workload except `compact`,
   which reports elapsed times rather than a rate. A first pass ran all of them at 5
   repetitions; everything it flagged, plus controls, was re-run at 11 repetitions
   interleaved across all three revisions, and the rest were brought to 11 as well so
   that every workload has a direct head-versus-`b5ac2064` comparison.
   `wal_concurrent` was then run again at 25.
3. **PITR on/off** - `pitr-perf --operations 10000`, run three times against a disk
   and nine times against tmpfs. The harness alternates which mode goes first by case
   position, so each result carries the `mode_order` it was produced under. That
   counterbalancing is *across* writer-count cases, not within one: every repetition
   of a given writer count runs its two modes in the same order, so a ratio read for a
   single writer count here is potentially order-confounded and `mode_order` records
   the design without removing it. The seal comparison further down alternates the
   order every repetition for that reason.

Medians are reported throughout, with quartiles where the spread decides the reading,
because the run-to-run spread is large enough that a single median over a handful of
runs can produce a difference that is not there (see the note below).

WAL is disabled in (1) deliberately: with the database on a real disk each durable
write is sync-bound at roughly 1,700 ops/s, which hides CPU differences entirely.
That cuts both ways, and it is why (3) is run twice. On `/home` every write pays an
fsync, so a few microseconds of extra CPU work per commit is invisible by
construction; on tmpfs the same writes are sync-cheap, and that is the regime in
which the cost becomes measurable.

Measurements (2) and the tmpfs half of (3) ran under `/tmp`, a 32 GiB tmpfs. That
filesystem was full when the section above was written - `fallocate` failed there
with `EDQUOT` - which is why the slice-12 baseline could not be re-run at the time.
It has since been cleared, so the method is reproducible again; absolute numbers
still are not comparable to the section above, which used a different harness
revision on different storage.

Host: Linux 6.18.9, Intel Core i9-13900T, 32 logical CPUs, database under `/home`
for (1) and the disk half of (3), under `/tmp` for (2) and the tmpfs half of (3).

### Steady-state write and read paths (WAL off, 9 repetitions)

| Workload | `2f556ccb` (pre-PITR) | `b5ac2064` (before #330) | `494a20ab` (head) | head vs before #330 | head vs pre-PITR |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fillrandom` | 700,747 | 1,143,294 | 1,145,238 | **+0.2%** | +63.4% |
| `overwrite` | 880,373 | 1,349,285 | 1,405,993 | **+4.2%** | +59.7% |
| `readrandom` | 205,922 | 208,759 | 205,961 | **-1.3%** | +0.0% |
| `seekrandom` | 68,661 | 72,556 | 71,566 | **-1.4%** | +4.2% |

Medians in ops/s. Quartiles were recorded for `fillrandom`, whose head p25-p75
(1,059k-1,215k) overlaps `b5ac2064`'s (1,079k-1,220k) - so its +0.2% is noise. The
remaining rows are medians-only comparisons at 11 repetitions: their single-digit
percentages are read as "no resolved movement", not as measured differences.

### WAL path (WAL on, 20,000 writes, 3 repetitions)

| Revision | Runs (ops/s) | Median |
| --- | --- | ---: |
| `b5ac2064` | 1,751 / 1,740 / 1,745 | 1,745 |
| `494a20ab` | 1,735 / 1,744 / 1,741 | 1,741 |

**-0.2%**, with every run inside 0.9% of every other. This is the path the PITR work
changed (batch encoding and group commit), and on a disk it is unchanged.

This row is **sync-bound**: 1,745 ops/s is one fsync per write, of which nearly all
is kernel time. It therefore cannot see a per-commit CPU cost - and there is one, on
a different workload, in the tmpfs measurement below. Read this as "the WAL path is
unchanged on a disk", not as "the WAL path is unchanged".

### All legacy workloads (WAL on, tmpfs)

26 of the 27 legacy workloads were compared; `compact` reports elapsed times rather
than a rate, so it has no column. At 5 repetitions one row stood out with
non-overlapping ranges. Every row whose median had moved more than 5% was re-run at
11 repetitions across all three revisions - this is that pass, medians in ops/s:

| Workload | Metric | `2f556ccb` (pre-PITR) | `b5ac2064` (before #330) | `494a20ab` (head) | head vs before #330 |
| --- | --- | ---: | ---: | ---: | ---: |
| `fillrandom` | ops/s | 168,745 | 167,713 | 165,515 | -1.3% |
| `fillseq` | ops/s | 175,228 | 175,738 | 171,846 | -2.2% |
| `readrandom` | ops/s | 178,812 | 172,878 | 176,179 | +1.9% |
| `memtable_publish_delete_concurrent` | ops/s | 4,785,484 | 5,092,244 | 5,103,429 | +0.2% |
| `seekrandomwhilewriting` | ops/s | 17,932 | 18,778 | 15,582 | -17.0% |
| `wal_batch_concurrent` | ops/s | 854,265 | 687,349 | 856,511 | +24.6% |
| `wal_concurrent` | ops/s | 173,650 | 148,767 | 150,698 | +1.3% |
| `wal_throughput` | ops/s | 143,954 | 142,631 | 139,623 | -2.1% |

The two large numbers in that table are the cautionary ones, not the findings.
`seekrandomwhilewriting` at -17% has runs spanning 12,178-26,268 ops/s across the same
revision, and `wal_batch_concurrent` at +24.6% has its `b5ac2064` runs reaching
1,107,425 - both are the harness moving, not the engine. Every row except
`wal_concurrent` has overlapping ranges against both baselines.

### The one workload that moved: `wal_concurrent`

25 interleaved repetitions of nothing but this workload, on tmpfs, WAL on, with no
other process on the machine, ops/s:

| Revision | min | p25 | median | p75 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `2f556ccb` (pre-PITR) | 161,139 | 170,517 | **177,411** | 182,608 | 207,475 |
| `494a20ab` (head) | 134,970 | 151,708 | **159,336** | 164,981 | 194,559 |

Median **-10.2%**, and the interquartile ranges are disjoint - the head's p75 sits
below the pre-PITR p25 - even though the tails overlap. It is the only regression the
survey found.

It is **not** from the last layer. Against `b5ac2064` the head is +1.3% at 11
repetitions, so the change landed earlier in the PITR line. Of the 128 commits between
the baseline and `b5ac2064`, exactly five touch a file on this path (`mvcc.rs`,
`wal.rs`, `mem_table.rs`), and all five are PITR layers: the ordered commit sequencer
(`#296`), the base-capture barrier (`#310`), the WAL-v5 integration (`#314`), and the
two recreate layers above them.

It is `#296` by attribution, not by isolation - no bisect was run, so this rests on
source analysis plus the diagnostic builds below, not on a controlled comparison that
holds the other four layers fixed. What the source shows: that commit put
`reserve_commit_ts` and `publish_commit_ts` on the write path, and the second of them
makes every write wait for every earlier timestamp to publish first. That wait both
costs its own time and de-phases the writers, which is where the throughput went - the
solo-group signature above is the group commit losing cohesion, not extra work being
done. See the note on the fix below.

The engine's own counters say what changed. Over the same 25 runs, median
`wal_commit_solo_groups` is **10,859 (pre-PITR) against 26,735 (head)** - groups of
one - while buffers per group fall only from 2.27 to 2.17. The head does not write
more (`wal_commit_bytes` is 819,200,000 in both, `wal_commit_buffers` 200,000 in
both); its commits simply coalesce less often, so more of them pay the submit-and-sync
path alone.

The workload is 4 threads each issuing single `put`s, which is the shape where a
per-commit serialization point shows: the batched variant
(`wal_batch_concurrent`) and the single-threaded one (`wal_throughput`) are both
within noise. `write-perf` runs with `pitr_repository: None`, so this is the
**PITR-disabled** path - enabling PITR is not a precondition for the cost.

### PITR enabled versus disabled at the head

Same harness, `--operations 10000`, medians over 3 whole runs against a disk and 9
against tmpfs. The harness alternates which mode goes first by case position, and
that position is fixed per writer count, so the column records it:

| Writers | Mode order | Disk: enabled/disabled | Disk: disabled -> enabled (writes/s) | tmpfs: enabled/disabled | tmpfs: disabled -> enabled (writes/s) |
| ---: | --- | ---: | --- | ---: | --- |
| 1 | PITR first | 99.3% | 1,769 -> 1,757 | **70.2%** | 145,959 -> 102,532 |
| 4 | PITR second | 91.2% | 3,905 -> 3,563 | **61.1%** | 161,396 -> 98,684 |
| 8 | PITR first | 101.2% | 6,771 -> 6,851 | **40.9%** | 175,971 -> 72,037 |
| 16 | PITR second | 102.1% | 12,738 -> 13,005 | 101.3% | 60,189 -> 60,996 |
| 32 | PITR first | 92.4% | 24,016 -> 22,186 | 101.7% | 15,999 -> 16,263 |
| **median** | | **99.3%** | | **70.2%** | |

The two columns disagree because they measure different regimes. The disk column
reproduces the 2,000-operation table this section replaces (median 100.8% there,
99.3% here): with an fsync in every write, PITR's added CPU work is not visible. On
tmpfs it is. At 8 writers the two modes are fully disjoint - disabled
156,610-201,079 against enabled 65,286-114,627 - and PITR runs **first** there, so
the order is not what is doing the work. At 16 and 32 writers both modes fall to
about 60k and 16k writes/s: the writers are contending with each other, and the
difference disappears into that.

So: enabling PITR measured free where fsync latency dominates, and 30-60% of
foreground write throughput on a sync-cheap filesystem at 1-8 writers. The harness
holds the archive rate unlimited and the segment large enough that no archive boundary
is crossed in 10,000 operations, so what is measured is per-commit v5 admission and
encoding, not archive I/O. Catch-up - the explicit recovery point, not a steady-state
cost - is 0.085-0.098 s on tmpfs and 0.132-0.207 s on disk.

### A note on sample size

A first pass at 5 repetitions reported `fillrandom` on the head as **-6.9%** against
`b5ac2064`, which would have been a real write-path regression. Re-measuring at 9
repetitions and reporting quartiles refuted it: the medians land 0.2% apart and the
distributions overlap almost entirely.

The all-workload survey is the same lesson from the other side. At 5 repetitions it
flagged ten workloads as having moved more than 5% between the pre-PITR baseline and
the head. All ten were re-run at 11 repetitions and nine of them dissolved into
overlapping ranges - including `seekrandomwhilewriting`, whose -17% against
`b5ac2064` sits on runs spanning 12,178-26,268 ops/s within a single revision, and
`parallel_scan` at +10.4% against the pre-PITR baseline, which is +1.1% against the
nearer one. `wal_concurrent` survived, and even it needed 25 repetitions before its
interquartile ranges separated. Anything read from this harness needs the spread
beside it: a bare median over a few runs is not evidence, and neither is a range that
overlaps only in the tails.

### What this does and does not show

What it shows: no regression from the last PITR layer (#330) on any of the 26
comparable legacy workloads; no measurable cost from enabling PITR where fsync
latency dominates; and one regression that predates #330 - `wal_concurrent`, about
10% on tmpfs, on the PITR-disabled path, with the counter signature of reduced commit
coalescing.

What it does not show:

- Any production reading of the tmpfs PITR overhead. A filesystem on which a durable
  write costs microseconds is a benchmark, not a deployment. (The cause of the one
  regression is attributed above, and fixed below.)
- The +63% write throughput against the pre-PITR revision is **not** attributable to
  PITR. Those 128 commits carry unrelated work, and the pre-PITR build was both
  slower and far more variable (its `fillrandom` floor was 65,796 ops/s against a
  1,050,405 floor at the head), so part of the gap is an old build behaving badly
  rather than a PITR improvement.
- vLog, TTL, range-tombstone, and compaction-filter paths are not covered here.
- Long compaction-heavy runs, archive backpressure with a configured rate limit, and
  same-device versus separate-device repositories are not covered; the table above
  says the same, and this section does not claim them either.

### Cause and fix (2026-09-23)

The ordering is not removable. RFC 023 section 5.1 makes it an invariant: the
sequencer owns "advancement of one contiguous published frontier", and "a later
commit may not become visible or advance `latest_commit_ts` while an earlier
reservation is unresolved". A version of PR #337 that published without ordering
until the first barrier - reading -1.3% against this baseline - was reverted for
that reason: it let a concurrent reader's snapshot change under a fixed `read_ts`,
and it made `publish_pitr_base`'s declared `included_commit_ts` a bare high-water
mark rather than a boundary.

What the RFC does not prescribe is how the wait is implemented, and that was where
the cost sat. Every waiter parked on the publication condvar, and every frontier step
signalled all of them, so each recheck needed the lock the publisher was holding: the
wait cost more than the ordering it enforced. PR #337 now mirrors the frontier into
an atomic that waiters spin on outside the lock, falling back to parking - with the
drain's timeout - only when a predecessor outlasts the spin budget.

Same method, 15 interleaved repetitions:

| Revision | median ops/s | solo groups | vs pre-PITR |
| --- | ---: | ---: | ---: |
| `2f556ccb` (pre-PITR) | 174,769 | 10,879 | - |
| `494a20ab` (before the fix) | 156,361 | 25,592 | -10.5% |
| with PR #337 | **167,765** | 24,149 | **-4.0%** |

That is one session's reading and the absolute gap moves with machine state: across
five sessions the same binaries read the fix between -1.3% and -10.2% against the
baseline, and the unfixed head between -9.4% and -12.3%. What holds in every session
is that the fix beats the unfixed head, by 2.1 to 6.5 points, with the interquartile
ranges overlapping the baseline rather than sitting below it. Anything cited from
this section should be a same-session comparison.

What is left afterwards is the ordering itself, not its implementation. Three
implementations were measured against each other in single sessions: parking on the
condvar (the unfixed head), a tight spin on a mirrored frontier (PR #337), and a
yield-backoff variant, which read 4% *worse* than the spin - a yield is a syscall and
the wait is only about 4.8 us. Instrumenting the wait put it at 28% of commits, mean
4.8 us, and a diagnostic build that kept the machinery but never waited matched the
baseline, so the machinery is free and the wait is the whole cost.

Two structural attempts to remove the wait were built and measured, both null:

- a frontier that any publisher may advance over a contiguous run of ready commits,
  so a WAL commit group becomes visible in one movement instead of one step per
  member: -0.2% against the spin, interquartile ranges overlapping;
- reserving earlier or later relative to the WAL append, to keep timestamp order and
  durability order together: not attempted, because the append already happens under
  the same lock as the reservation, so the inversion is in the insert-and-publish
  tail, not in the append.

That is where the cost sits: a writer waits for its predecessor's skiplist insert and
publication, which is one commit of pipeline depth that any design enforcing the
ordered frontier has to pay.

All 1314 tests pass on that revision, including the sequencer tests that pin ordered
publication, and
the exhaustion, poison-visibility and memory-ordering defects the review found are
fixed alongside.

The enabled/disabled sweep with this version reads **55.4%** (median) on tmpfs and
**98.0%** on disk, against 70.2% and 99.3% before the fix - the tmpfs movement is the
disabled baseline no longer being throttled by the same convoy it was measured
against.

Re-running needs four revisions built with `cargo build --release --bin write-perf`:
`2f556ccb` (pre-PITR), `b5ac2064` (before #330), `494a20ab` (the unfixed head), and the
ordered-publication fix. The fix row above was measured on PR #337's branch tip in that
session and no commit id was recorded with the numbers - if you reproduce today, use
`f21d421c`, that branch's final tip, merged to main as `58e587fa`: `write-perf --bench
wal_concurrent`, repetitions alternated between builds, one session. The ratio is
session-dependent - the same comparison in a later session read -10.3% rather than
-4.0%, because its pre-PITR baseline ran faster (192k rather than 175k ops/s) while the
fixed head held at ~171k. What reproduces is the fix's *own* contribution: about +5
points on this workload (17/20 paired repetitions). Pass `--path` under `/tmp` for the tmpfs
regime and under `/home` for the disk one. Keeping other work off the machine matters:
one intermediate pass was discarded because a benchmark was still running in the
background.

### What the remaining tmpfs gap was (2026-09-23)

The 55.4% above is the enabled path against the disabled one at the same head, so it is
not the publication ordering: it is the seal. `PitrSealAccumulator::append` feeds each
group's buffers into the segment's streaming SHA-256, and because every batch is
padded to the 4 KiB `O_DIRECT` alignment, the digest covers far more than the batch
holds - a single 128-byte value sits in a 4096-byte buffer, so roughly 23x. It ran on the commit leader, inside the window where the leader holds
`submitting` - the window every other writer waits to enter - at 5.8 us per group.

It was invisible until `pitr-perf --profile` existed (PR #340): `wal_submit` is
recorded before the fdatasync, while the seal block runs after it, so no counter
covered it. The `pitr_seal` line in that report is this cost.

Moving it off the leader's window closed the gap. Two placements were measured against
each other, and the first one was wrong:

- hashing on the append path (`put_v5_batch`) takes the work out of the submit chain
  and is offset-ordered by construction, but it also lands inside `pitr_reserved_end`,
  the offset-reservation mutex every writer takes. At 4 writers that reads +6%; at 8
  and 16 the enabled path collapses, because the mutex now serializes a SHA-256 of
  every aligned buffer across all threads.
- what shipped takes a `pitr_seal_append` lock *inside* the `submitting` window,
  publishes the group (releasing `submitting`), and then hashes off that window. The
  window is exclusive and ordered by group, so the lock is acquired in file order and
  the digest stays in file order; hashing after the group's fdatasync is what keeps the
  digest to durable bytes, which the append placement had traded away.

Same-session, 28,000 operations, interleaved repetitions, PITR disabled as the control,
against the append-path build:

| writers | delta | repetitions | control |
| --- | ---: | --- | ---: |
| 8 | **+2.4x** | 14/14 (1.7-3.5x) | 0.97x |
| 16 | **+1.4x** | 14/14 (1.30-1.80x) | 0.89x |
| 4 | +6% | 10/14 | 1.04x |
| 1 | 0.998x | 15/30 | 0.97x |
| 32 | neutral | 5/14 | 0.98x |

Against the original leader-side build it is 3.2x at 8 writers (control 1.005x). The
1-writer row is the reason to distrust small samples here: at 14 repetitions the same
comparison read -11%, and the 30-repetition run above is what settled it.

#### The digest covers logical bytes now (2026-09-23)

With the seal off the commit chain, the digest itself was the remaining PITR-on cost at
low writer counts. The accumulator hashed the batch's 4 KiB-aligned buffer, and
`encode_v5_batch` pads every batch up to that alignment, so a batch holding one
128-byte value - about 180 bytes - was hashed as 4096. The digest now covers each
batch's own bytes (its fixed header plus the data length it carries) instead of the
padding that follows them.

**How it is carried.** The v5 file header has no spare byte - flags, reserved fields
and the region past 132 are all asserted zero, and the header CRC covers `0..128` - so
the only field an old decoder tolerates is the version itself. New segments therefore
claim WAL **version 6**; **version 5 is read and verified forever** under the rule it
was written with. `wal_digest_rule(version)` is the single mapping, the rule is taken
from the segment's own header rather than from the running binary, and the seal echoes
the version it was built for in the field it already had (its own `VERSION` stays 1).
`wal_digest(wal, rule)` refuses a rule that disagrees with the file's version, so a
wrong rule threaded through a caller is a named error rather than a digest that never
matches. `logical_length` is unchanged and still alignment-multiple, so truncation,
manifest accounting and `validate` are untouched.

**What it measures.** Same session, `pitr-perf --profile --operations 28000`, three
repetitions per build, alternating which build ran first, medians of `pitr_seal`:

| writers | before | after | ratio | off-control |
| --- | ---: | ---: | ---: | ---: |
| 1 | 176.5 ms | 15.5 ms | **11.4x** | 0.99x |
| 4 | 171.3 ms | 15.6 ms | 11.0x | 1.00x |
| 8 | 177.0 ms | 19.0 ms | 9.3x | 1.02x |
| 16 | 179.9 ms | 17.8 ms | 10.1x | 1.00x |
| 32 | 203.8 ms | 20.1 ms | 10.1x | 1.00x |

The off-control is the PITR-**disabled** case's `fdatasync` between the two binaries -
a code path this change does not touch - so it is the check that a ratio is not just
run order. It is what caught the first ordering: with the old build running first in
every pair, the 1-writer control read 2.34x and its 12.3x ratio was inflated; run the
other way the control reads 0.99x and the ratio 11.4x. The 4-32 writer rows agree
across both orders, so only the 1-writer row needed the second ordering to settle. As
everywhere else in this document, compare ratios within a session, not absolutes: the
same `pitr_seal` case read 64.6 ms in the #340 session and 176-184 ms in this one.

The ratio is large and the share is not, and both belong in the same sentence: the
saving is 0.5-1.0% of the enabled path's wall clock at these writer counts (155.8 ms of
171.3 ms at 4 writers, 161.0 ms of 176.5 ms at 1, against ~8 s and ~17 s of wall for
28,000 operations). What the seal cost was the last *PITR-specific* phase at low writer
counts, not a large share of the run - the rest of the enabled path's cost is the
ordered-commit machinery, which is present with PITR off too.

The mechanism is visible in one more counter: `seal_bytes` (added here) reads
**205 B/op** after, against the 4096 B/op the old rule hashed - the harness's own
`commit_bytes avg = 4096 B` at 1 writer and `avg_bufs = 1.00`. So the bytes hashed per
operation fell ~20x while the time fell ~10x. The residual is per-group call overhead,
not hashing: 15.0 ms per 28,000 operations is 0.54 us/op, of which the 205 bytes at
this machine's 2.2 GB/s account for ~0.09 us. What is left of `pitr_seal` is the lock,
the entry push and the per-buffer call - a floor that the 4096-byte case's hash used to
hide.

**What the rule does not attest any more.** A v6 digest skips the alignment gaps, so a
byte flipped inside one is no longer a digest mismatch. It is still refused, by the
parser's nonzero-gap check before the bytes are used - but that is a parse-time
property, so a verification depth that decodes nothing no longer detects padding
corruption in a v6 segment. v5 segments are unaffected: their rule covers every byte.
Pinning that narrowing is what one of the new tests does.

**Keeping v5 forever is tested against bytes, not intentions.** `src/tests/fixtures/`
holds a two-batch v5 segment and its seal, generated by the tree from *before* this
change (`438903ac`) and committed as bytes, with their SHA-256s pinned. The test
rebuilds the seal from the frozen WAL and requires the frozen seal back, byte for byte;
checks the legacy rule still equals `SHA256(file)`; and checks the new rule is refused
on that file rather than silently producing a different digest. A segment resumed after
the upgrade keeps the old rule and keeps saying so in the seal it writes - also tested,
including that its digest still covers the preallocated tail up to `logical_length`,
which is the length the engine truncates to before archiving.

**Upgrade and rollback.** New segments are v6; a repository that holds both verifies
each under its own rule, which the mixed-version archive-and-restore test drives end to
end (the successor's predecessor anchor is a *stored* v5 digest inside a v6 header, and
object names are built from those stored digests). An *older* binary cannot open a
repository whose active segment is v6 - it refuses at `open_and_detect` rather than
misreading it - so rolling back needs the active segment closed with `disable_pitr`
(which installs a plain v4 WAL) or the repository restored from backup.
`PITR_BASE_WAL_REPLAY_VERSION` is deliberately **not** bumped: it describes the base
snapshot's replay contract, not a segment's wire format, and bumping it would
invalidate existing bases.

The counter note: `pitr_seal` used to be timed around a block that begins by taking the
seal mutex, so at high writer counts it reported the wait for that mutex as well as the
hashing - 134.0 ms against 110.6 ms for the same case once the timer moved inside the
lock. Numbers quoted from it before 2026-09-23 include the wait.

One harness limit to know before re-running this: `pitr-perf` aborts every
PITR-enabled case above roughly 30,000 operations with `PITR batch would cross maximum
segment bytes` - each operation occupies one 4 KiB-aligned buffer, whatever the
value's size, so the 128 MiB `max_segment_bytes` admits about 32,768 operations - and
an aborted case prints nothing on stdout. A run that produces
an empty result file is that, not a crash.
