# GCP V7/V8 Point MVCC publication path diagnostics

This follow-up adds path-level measurements to the earlier
[V7/V8 MVCC publication comparison](rfc-026-v7-v8-mvcc-publication-gcp-20261114.md).
It records publication order, predecessor distance, mutex wait, spin behavior,
condition-variable waits, and per-call wall/thread CPU time. It does not change
the WAL or MVCC runtime algorithms.

## Test setup

- GCP `c4-standard-16-lssd`, 16 vCPUs, `us-east1-b`; Linux `7.0.0-1017-gcp`.
  The Local SSD RAID0 device was ext4, mounted with `noatime,stripe=256`.
- 16 writer threads; 100,000 Point puts per run; 1 KiB values; five
  counterbalanced V7/V8 pairs on the same VM.
- V7 Parallel was compared with V8 Parallel using Direct FRONTIER, fixed
  400 μs coalescing, and 4 MiB sliding `fallocate`. io-wq was unpinned.
- Both arms sampled every 64th commit timestamp. No `--profile`, Hotpath,
  `perf`, or WAL sync diagnostics were enabled. Put latency was sampled every
  100 operations.
- Source commit: `cbb9017587fa882deb0a0902711d87a7f3e26f0c`. The benchmark
  binary was built on the GCP VM with the `bench` feature.

The five pairs alternated which WAL format ran first. All V8 runs reported
DATA and FRONTIER as `O_DIRECT`, the sync handle as buffered, and stable WAL
handle metadata. Sliding extent preparation reported the planned 4 MiB
window.

## Throughput and process CPU

Values are medians across five runs per arm. CPU/put is total process CPU time
divided by the 100,000 completed puts.

| Metric | V7 Parallel | V8 Direct/400 | V8 vs V7 |
| --- | ---: | ---: | ---: |
| Throughput | 44.82k puts/s | 31.58k puts/s | −29.5% |
| Process CPU per put | 60.39 μs | 72.19 μs | +19.5% |
| Put latency p50 | 0.313 ms | 0.483 ms | — |
| Put latency p95 | 0.667 ms | 0.736 ms | — |
| Put latency p99 (sampled) | 1.959 ms | 2.064 ms | — |
| Sync count | 8,308 | 6,333 | — |

V8 was slower in all five paired throughput comparisons: −29.54%, −30.16%,
−25.51%, −29.38%, and −31.06% (median −29.54%). These instrumented runs are
useful for the matched V7/V8 comparison, but are not uninstrumented throughput
qualification.

## Publication order and predecessor distance

Each arm contributed 7,810 samples, with 1,562 samples in every run and no
dropped samples. Percentages below are pooled over those samples.

| Sample path | V7 samples | V7 share | V8 samples | V8 share |
| --- | ---: | ---: | ---: | ---: |
| In order | 3,774 | 48.3% | 2,087 | 26.7% |
| Out of order | 4,036 | 51.7% | 5,723 | 73.3% |

For out-of-order samples, `distance_at_entry` is
`commit_ts - next_to_publish_at_entry`; the decision distance is measured from
the same commit timestamp to the frontier observed when the publication lock
is acquired:

| Out-of-order distance | V7 | V8 |
| --- | ---: | ---: |
| Entry p50 / p95 | 4 / 11 | 7 / 15 |
| Publication-decision p50 / p95 | 4 / 11 | 6 / 15 |

V8 entered the out-of-order path more often and further ahead of the current
publication frontier. In addition, the median in-order call advanced 1 ready
timestamp in V7 and 4 in V8; more than one timestamp was advanced in 48.0% and
82.0% of the respective in-order samples. This is consistent with a larger
backlog of ready commit timestamps being drained together in V8, but does not
identify why those timestamps arrived out of order.

## Cost by publication path

Percentiles are pooled nearest-rank values over the samples for each path.
Thread CPU is measured with `CLOCK_THREAD_CPUTIME_ID`.

| Sample metric | V7 p50 / p95 | V8 p50 / p95 |
| --- | ---: | ---: |
| All paths: thread CPU | 5.48 / 63.38 μs | 13.96 / 62.21 μs |
| All paths: wall time | 6.05 / 96.18 μs | 17.22 / 99.72 μs |
| In-order: thread CPU | 1.55 / 8.55 μs | 2.89 / 13.51 μs |
| Out-of-order: thread CPU | 17.54 / 69.94 μs | 19.09 / 65.78 μs |
| Publication mutex wait: wall time | 0.83 / 33.48 μs | 2.23 / 57.34 μs |

The aggregate p50 difference is strongly affected by the path mix: V8 has many
more out-of-order samples. When conditioned on the out-of-order path, CPU p50
is similar and V8's p95 is slightly lower. The measurements therefore support
a difference in publication arrival order and backlog more strongly than a
large per-call CPU penalty unique to V8's out-of-order path. Mutex wait tails
are higher in V8, but the median waits remain small.

Out-of-order samples that entered a condition-variable wait were 568/4,036
(14.1%) for V7 and 393/5,723 (6.9%) for V8. Including samples with no wait,
condition-variable wall-time p95 was 59.19 μs for V7 and 29.58 μs for V8.
The spin loop reached its 4,096-iteration limit for 14.7% of V7 and 7.3% of
V8 out-of-order samples; otherwise the sampled loop observed the publication
frontier advance. These results do not support condition-variable sleeping as
the main V8-specific cost.

## Interpretation and next measurement

The earlier hypothesis that V8's higher `publish_commit_ts()` CPU is related
to more frequent out-of-order publication is supported by the path shares and
larger predecessor distances. The data does not prove that WAL completion
timing causes the difference, nor that MVCC publication explains the full
throughput gap. The near-similar out-of-order CPU percentiles and higher
multi-timestamp advances on V8 suggest the path distribution and batching of
ready timestamps matter more than an isolated slow MVCC primitive.

Next, correlate sampled `commit_ts` with its allocation, WAL ticket and durable
return, then measure time from WAL return to publication entry and predecessor
publication. That will show whether V8's additional out-of-order samples come
from WAL return ordering or from scheduling after WAL completion. Separately
run an off/on control for this detailed instrumentation before using its CPU
figures to guide an optimization. Do not change the spin policy or publication
algorithm based on these data alone.

## Validation and retained results

- The run plan completed 10/10 runs. Every run directory contains a successful
  `result.json` and matching `run-metadata.json`; all ten outputs passed
  `write-perf --validate-json`. The archive validator confirmed the planned
  run IDs, 100,000 operations per run, 1,562 selected samples per run, and zero
  dropped samples.
- The raw result archive and checksum remain in local temporary storage and
  were intentionally not added to GitHub. Archive SHA-256:
  `b80ab4a8806ae1f3b53972fb84f31a5e03314b452fa122f847316f47f0f9d038`.
- The temporary VM and isolated network state were retained; no destroy or
  cleanup command was run. The VM remains subject to its original 60-minute
  server-side lifetime.

The sampled p99 values and path-conditioned percentiles are diagnostic
estimates, not tail-latency qualification. Periodic commit-timestamp sampling
is deterministic rather than randomized, and both arms used the same
instrumentation.
