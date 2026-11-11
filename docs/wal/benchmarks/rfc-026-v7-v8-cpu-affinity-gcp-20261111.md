# RFC 026 V7/V8 CPU Affinity Results

## Summary

Pinning the persistent WAL helper threads produced a large V8 throughput gain at
16 writers, while the same change had little effect on V7. Pinning writers as
well reduced CPU migrations further, but did not outperform helper-only pinning
at 16 writers. This is evidence that CPU placement matters to this V8 workload,
but it does not isolate userspace locality: pinning the WAL I/O worker also
restricted every observed io_uring worker to CPU 8.

The experiment does not justify adding a dedicated durability-driver thread.
The V8 driver still ran inline on writer threads, and the writer-pinned arm did
not beat the WAL-helper-pinned arm at 16 writers. First separate the io_uring
worker affinity effect from userspace WAL-thread affinity.

## Method

- Host: GCP `c4-standard-16-lssd`, `us-east1-b`; 16 logical CPUs arranged as
  eight SMT sibling pairs: `0,8` through `7,15`.
- Storage: two Local SSDs in RAID0, ext4 mounted with `noatime,stripe=256`.
- Workload: `wal_concurrent`, PITR V7 or V8 Parallel, 100,000 Point puts per
  run, 1 KiB values, no compaction, 2 GiB target SST size.
- V8 used Direct FRONTIER and a fixed 400 μs coalescing budget. In every scored
  V8 run, metadata confirmed DATA and FRONTIER were `O_DIRECT`, Sync was
  buffered, and all three handles referred to a stable inode.
- Three placement arms: A `none` (no affinity changes), B `wal` (pin persistent
  WAL helpers), and C `wal_writers` (pin helpers and every writer). Both V7 and
  V8 ran at 8 and 16 writers, with five counterbalanced repetitions per cell:
  60 scored runs total.
- Each arm used the same affinity capture and synchronized start barrier; the
  timer began after thread placement. No broad profiler or per-sync diagnostics
  were enabled. `perf stat` collected process CPU migrations and context
  switches. Because `perf_event_paranoid=4`, both perf and its benchmark child
  ran as root; the sysctl was not changed. Aggregate WAL counters and sampled
  latency remained enabled.
- Source commit: `a2c35377`. Binary SHA-256:
  `08352f8ca3eac0472d598a0e679d09908fc941293412a9f82f4b3768d9f7b5c6`.
  A six-case 8-writer smoke matrix passed before scoring. The scored matrix
  validated all 60 runs and exactly 100,000 operations per run.

## Throughput

Values are median puts/s across five runs. Parentheses show the median of the
five paired percentage changes against that format's no-pin arm; the paired
range is included to show run-to-run consistency.

| Writers | WAL | A: No pin | B: WAL helpers pinned | C: WAL helpers + writers pinned |
| ---: | :--- | ---: | ---: | ---: |
| 8 | V7 | 33.25k | 33.93k (+2.43%; +1.23% to +5.48%) | 33.95k (+2.83%; +0.57% to +6.69%) |
| 8 | V8 | 14.57k | 15.18k (+4.21%; +1.60% to +7.57%) | 15.51k (+6.45%; +4.93% to +7.99%) |
| 16 | V7 | 47.33k | 47.89k (+0.74%; -0.63% to +1.81%) | 47.54k (+1.55%; -0.19% to +4.31%) |
| 16 | V8 | 20.41k | 24.92k (+21.01%; +15.75% to +25.06%) | 24.23k (+17.70%; +14.56% to +25.11%) |

At 16 writers, all five V8 helper-pinned runs beat their paired no-pin runs.
The median V8 p50 fell from 0.763 ms to 0.608 ms in B and 0.632 ms in C.
Sampled p99 was noisy: the V8 medians were 2.715 ms, 2.122 ms, and 2.921 ms
for A, B, and C respectively, so this does not establish a stable tail-latency
improvement.

## CPU and WAL counters

Medians per run:

| Writers / WAL | CPU migrations A / B / C | Context switches A / B / C | Process CPU seconds A / B / C |
| :--- | ---: | ---: | ---: |
| 8 / V7 | 866 / 76 / 27 | 260k / 259k / 268k | 4.09 / 4.03 / 4.16 |
| 8 / V8 | 10,674 / 89 / 24 | 713k / 849k / 852k | 9.54 / 7.54 / 7.72 |
| 16 / V7 | 39,962 / 25,387 / 55 | 341k / 349k / 345k | 6.08 / 5.86 / 5.22 |
| 16 / V8 | 104,459 / 18,541 / 67 | 765k / 820k / 830k | 11.77 / 8.38 / 7.87 |

For V8 at 16 writers, B reduced migrations by 82% and process CPU by 29% while
raising median paired throughput by 21%. C reduced migrations by more than
99.9%, but its median throughput was 2.7% below B. Pinning clearly reduces migrations, but the
remaining context switches did not fall; at 16 writers they rose slightly in
both pinned V8 arms.

V8 WAL counters also changed with placement. At 16 writers, median Sync count
fell from 7,882 in A to 6,908 in B and 6,965 in C. Eventfd writes fell from
38,940 to 31,380 and 30,591 respectively. At 8 writers, Sync count stayed
nearly constant while eventfd writes fell. These counters suggest that
placement changed coordination and group formation as well as CPU migration.

## Interpretation and limits

The affinity arms changed io_uring worker placement too. In A, every observed
`iou-wrk-*` thread was allowed CPUs 0–15. In B and C, every observed io_uring
worker was allowed only CPU 8, matching the pinned WAL I/O worker. This was not
an independently controlled variable, so the throughput gain cannot be
attributed to userspace CPU locality alone.

At 8 writers, helper CPUs 8–11 (V7) or 8–9 (V8) were SMT siblings of writer
CPUs 0–7. At 16 writers, C pinned writers across CPUs 0–15, so helpers shared
the same logical CPUs 8–11 (V7) or 8–9 (V8) with writers. This makes C a
placement stress case rather than a dedicated-core comparison, and may explain
why it failed to beat B at 16 writers.

The results support a controlled follow-up that pins WAL helpers while
explicitly controlling io_uring worker masks. They do not yet show whether the
benefit comes from helper locality, io_uring placement, changed group
formation, or their interaction. Since C did not outperform B at 16 writers,
there is not yet evidence that pinning the inline durability driver through its
writer thread adds throughput beyond pinning the persistent WAL helpers. A
fixed-driver-thread experiment should wait until these effects are separated.

Raw JSON, affinity snapshots, and perf outputs are retained locally and were
not added to GitHub.
