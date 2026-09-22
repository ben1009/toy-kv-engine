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

The table above is the slice-12 baseline. This section answers a narrower question
asked after the last PITR layer merged: did that work slow the engine down for the
default, PITR-disabled configuration?

Two baselines, both on `main`:

- `b5ac2064` - main immediately before PR #330 merged, so a difference against it is
  attributable to that PR's 14 files.
- `2f556ccb` - main's tip before the first PITR commit (`#294`) landed, so a
  difference against it covers the whole PITR line of work *and* everything else
  merged alongside it.

Head under test: `494a20ab`.

### Method

`write-perf --suite legacy`, one process per run, 200,000 writes and 100,000 reads
at 1 KiB values on 4 threads, `--no-wal`. The three revisions are run **interleaved**
- every repetition runs all three back to back - so host drift lands on all of them
rather than on whichever ran last. 9 repetitions; medians and quartiles are reported
because the run-to-run spread is large enough that a single median over a handful of
runs can produce a difference that is not there (see the note below).

WAL is disabled for these rows deliberately: with the database on a real disk each
durable write is sync-bound at roughly 1,700 ops/s, which hides CPU differences
entirely. The WAL path is measured separately, and the tmpfs method used for the
table above is **not reproducible on this host**: `/tmp` is a full tmpfs and
`fallocate` fails there with `EDQUOT`, so absolute numbers from the two sections are
not comparable - only within-run ratios in each are.

Host: Linux 6.18.9, Intel Core i9-13900T, 32 logical CPUs, database under `/home`.

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
changed (batch encoding and group commit), and it is unchanged.

### PITR enabled versus disabled at the head

Same harness, `--operations 2000`, one run per writer count:

| Writers | Disabled writes/s | Enabled writes/s | Enabled / disabled | Catch-up seconds |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1,778 | 1,792 | 100.8% | 0.060 |
| 4 | 3,847 | 3,536 | 91.9% | 0.050 |
| 8 | 6,053 | 6,521 | 107.7% | 0.046 |
| 16 | 13,423 | 14,436 | 107.5% | 0.042 |
| 32 | 24,447 | 22,004 | 90.0% | 0.042 |

Median **100.8%**, with the spread running in both directions - so on this workload
the archive path costs nothing measurable rather than costing a consistent few
percent. The catch-up figure is the explicit recovery point, not a steady-state cost.

### A note on sample size

A first pass at 5 repetitions reported `fillrandom` on the head as **-6.9%** against
`b5ac2064`, which would have been a real write-path regression. Re-measuring at 9
repetitions and reporting quartiles refuted it: the medians land 0.2% apart and the
distributions overlap almost entirely. Anything read from this harness needs the
spread beside it; a bare median over a few runs is not evidence.

### What this does and does not show

What it shows: no regression from the last PITR layer on the write path (WAL on and
off), the point-read path, or the iterator/seek path, and no measurable steady-state
cost from enabling PITR.

What it does not show:

- The +63% write throughput against the pre-PITR revision is **not** attributable to
  PITR. Those ~200 commits carry unrelated work, and the pre-PITR build was both
  slower and far more variable (its `fillrandom` floor was 65,796 ops/s against a
  1,050,405 floor at the head), so part of the gap is an old build behaving badly
  rather than a PITR improvement.
- vLog, TTL, range-tombstone, and compaction-filter paths are not covered here.
- Long compaction-heavy runs, archive backpressure with a configured rate limit, and
  same-device versus separate-device repositories are not covered; the table above
  says the same, and this section does not claim them either.

Re-running needs the three revisions built: `2f556ccb`, `b5ac2064`, and the head, each
with `cargo build --release --bin write-perf`, then the same command with `--path` on
a filesystem that is not a full tmpfs.

