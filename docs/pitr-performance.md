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
   position, so each result carries the `mode_order` it was produced under.

Medians and quartiles are reported throughout, because the run-to-run spread is large
enough that a single median over a handful of runs can produce a difference that is
not there (see the note below).

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

Medians in ops/s. The head's spread against `b5ac2064` overlaps on every row - for
`fillrandom`, head p25-p75 is 1,059k-1,215k against 1,079k-1,220k - so the -1.3% and
-1.4% read-path differences are inside the noise, not a cost.

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
two recreate layers above them. Which one of the five is not established here; a
bisect over them would settle it.

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

- **Which commit** in the PITR line caused `wal_concurrent`. Five candidates touch the
  hot path; no bisect was run, so the finding is a floor on what the PITR work cost,
  not an attribution.
- Any production reading of the tmpfs PITR overhead. A filesystem on which a durable
  write costs microseconds is a benchmark, not a deployment.
- The +63% write throughput against the pre-PITR revision is **not** attributable to
  PITR. Those 128 commits carry unrelated work, and the pre-PITR build was both
  slower and far more variable (its `fillrandom` floor was 65,796 ops/s against a
  1,050,405 floor at the head), so part of the gap is an old build behaving badly
  rather than a PITR improvement.
- vLog, TTL, range-tombstone, and compaction-filter paths are not covered here.
- Long compaction-heavy runs, archive backpressure with a configured rate limit, and
  same-device versus separate-device repositories are not covered; the table above
  says the same, and this section does not claim them either.

Re-running needs the three revisions built - `2f556ccb`, `b5ac2064`, and the head,
each with `cargo build --release --bin write-perf` - plus the head with
`cargo build --release --bin pitr-perf`. Pass `--path` under `/tmp` for the tmpfs
regime and under `/home` for the disk one. Keeping other work off the machine matters:
one intermediate pass was discarded because a benchmark was still running in the
background.

