# RFC 026 V7/V8 io-wq affinity sweep on GCP, 2026-11-11

## Result

Restricting io-wq workers to one logical CPU improved V8 Point throughput, most
clearly at 16 writers. CPU 8 was not special: CPU 0 (its SMT sibling) and CPU 9
(a different physical core) produced similar gains. Allowing CPUs 8 and 9 was
slower than leaving the mask unrestricted. The same masks had little effect on
V7.

This supports the earlier finding that io-wq placement changes V8 completion
and group-formation timing. It does not establish whether the gain comes from
locality, limiting runnable worker execution, or another scheduling interaction.
The results do not justify a production affinity default or a TPC rewrite.

## Method

- Machine: GCP `c4-standard-16-lssd`, Intel Granite Rapids, 16 logical CPUs,
  8 physical cores, two Local SSDs in RAID0, ext4.
- Source and benchmark binary: PR branch commit `5f0f2b99`.
- Workload: V7 and V8 Parallel WAL; 100,000 Point puts per run, 1 KiB values,
  8 or 16 writers, compaction disabled, fresh database path on Local SSD.
- V8 used Direct FRONTIER and a fixed 400 μs coalescing budget. V7 used its
  regular Parallel path.
- WAL helper and writer affinity were disabled. The only changed variable was
  the registered io-wq CPU mask: all CPUs 0–15, CPU 8, CPU 0, CPU 9, or CPUs
  8 and 9.
- Every arm used a fixed 32,768-put warmup before measurement. Five repetitions
  covered each version, writer count, and mask: 100 scored runs total. Mask and
  format order rotated across repetitions. Runs used no broad profiler and no
  per-sync trace; aggregate WAL counters and `perf stat` CPU-migration and
  context-switch counters were collected.
- The first SSH session ended after 98 records. The final unpinned V7/V8
  16-writer pair was rerun in its scheduled order on the same VM and binary.
  All 100 records were then validated for workload parameters, warmup size,
  requested and observed io-wq masks, and V8 WAL handle stability and I/O modes.
  No raw result bundle was added to the repository; the summary is retained
  locally.

## Throughput

Median throughput in thousands of puts per second:

| io-wq mask | V7, 8 writers | V8, 8 writers | V7, 16 writers | V8, 16 writers |
| --- | ---: | ---: | ---: | ---: |
| All CPUs 0–15 | 33.25 | 14.82 | 48.20 | 20.93 |
| CPU 8 | 33.72 | 16.45 | 48.23 | 25.26 |
| CPU 0 | 33.25 | 15.95 | 48.23 | 25.07 |
| CPU 9 | 33.51 | 16.13 | 48.41 | 24.68 |
| CPUs 8 and 9 | 33.33 | 14.07 | 48.16 | 20.11 |

Paired V8 throughput changes versus the unrestricted mask were consistent across
all five repetitions:

| Writers | CPU 8 | CPU 0 | CPU 9 | CPUs 8 and 9 |
| ---: | ---: | ---: | ---: | ---: |
| 8 | +11.3% (5/5 up) | +8.9% (5/5 up) | +10.1% (5/5 up) | −5.0% (0/5 up) |
| 16 | +21.0% (5/5 up) | +19.8% (5/5 up) | +17.8% (5/5 up) | −4.0% (0/5 up) |

At 16 writers, the paired CPU 8 gains ranged from +16.4% to +24.2%; CPU 0
ranged from +17.5% to +25.2%, and CPU 9 from +15.3% to +25.0%. V7 stayed near
its unrestricted throughput: its paired median changes across masks were between
−0.6% and +0.7% at both writer counts. CPU 9 was slightly faster in all five
16-writer comparisons, but the effect was small beside the V8 gains.

## V8 16-writer counters

Medians across five runs per arm:

| Metric | All CPUs | CPU 8 | CPU 0 | CPU 9 | CPUs 8 and 9 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Throughput, k puts/s | 20.93 | 25.26 | 25.07 | 24.68 | 20.11 |
| CPU migrations | 215,839 | 105,448 | 105,643 | 106,568 | 224,490 |
| Context switches | 1,103,826 | 1,007,786 | 1,007,334 | 1,012,206 | 1,093,587 |
| Sync count | 7,713 | 6,322 | 6,319 | 6,329 | 7,446 |
| Eventfd writes | 38,692 | 35,123 | 34,944 | 34,844 | 37,694 |
| Process CPU, ms | 11,668 | 8,884 | 8,897 | 8,960 | 11,131 |
| p50 latency, ms (sampled) | 0.758 | 0.618 | 0.627 | 0.637 | 0.777 |
| p99 latency, ms (sampled) | 1.162 | 0.939 | 0.996 | 1.003 | 1.384 |
| `fdatasync` time per sync, μs | 104.1 | 114.3 | 117.8 | 112.7 | 124.3 |

For the single-CPU masks, migrations fell by about 51%, sync count by about
18%, eventfd writes by 9–10%, and process CPU time by about 23–24%. The
per-sync `fdatasync` time did not improve; it was higher in these runs. The
throughput gain co-occurred with fewer durability advances and less CPU work,
not faster individual sync calls. The two-CPU mask did not show these changes
and was slower than baseline.

At 8 writers, single-CPU masks improved V8 throughput by 9–11%, but the paired
gains were smaller than at 16 writers. V7 throughput was effectively unchanged.

## Interpretation and limits

The affinity control allowed every io-wq worker to run on the selected CPU(s);
it did not dedicate one worker to each CPU. The post-warmup captures showed
roughly 29–31 workers for the 16-writer V8 runs across both single-CPU and
unrestricted masks. Thus the throughput result should not be described as a
worker-count reduction.

CPU 0 and CPU 8 are SMT siblings, while CPU 9 is on another physical core. Their
similar single-CPU results weaken the idea that CPU 8 itself is uniquely
favorable. The slower two-CPU mask also shows that fewer migrations alone do not
explain throughput: it had more migrations than the unrestricted arm and a
larger sync count. These measurements do not identify a single causal scheduler
mechanism.

Latency percentiles are sampled (`--latency-sample-every 100`) and should be
treated as directional. The benchmark still collected aggregate counters, so
these are low-instrumentation measurements rather than a zero-overhead run.
The result covers Point writes on one machine and two writer counts; it does not
qualify other workloads, host topologies, or a production affinity setting.

## Follow-up

Keep the single-CPU io-wq mask as a promising benchmark control, not a runtime
default. The next useful experiment is a targeted timeline of DATA CQE arrivals,
ready-prefix formation, and durability advances under unrestricted and
single-CPU masks. That can test whether io-wq placement improves throughput by
changing completion bursts and group sizes. A broader CPU-location sweep or a
worker-model change should follow only if that mechanism is confirmed.
