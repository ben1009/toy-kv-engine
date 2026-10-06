# RFC 024: batch64 parallel WAL versus Leader rerun, 2026-10-06

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

The current parallel WAL was rerun against Leader using batch64 at eight and
sixteen writers. This session **does not establish a reliable parallel-WAL
win**. Only one of five blocks passes repeat controls at each writer count;
the eight-writer Leader/Leader control fails. At sixteen writers the paired
median is +6.9%, but four blocks disagree substantially and the absolute
primary medians are nearly tied. The full RFC performance gate remains
unqualified.

## Workload and comparison

Both arms use the same freshly staged benchmark executable,
SHA-256 `7f113ba4b8c116ea7adfb3bc058b983f7b10ad64db4023a5e93ecc62f8ecef76`.
Its 115 recorded input hashes identify the measurement working tree based on
`9e11e522`, including the 400-microsecond cutoff. That tree also contains pending
default-selection edits in `lsm_storage.rs`, `mem_table.rs`, `wal.rs`,
`bin/write-perf.rs` and `tests/wal.rs`; it is not an unmodified committed PR
head. Both arms select their WAL mode explicitly, so the default selector is
not the comparison variable. Leader calls public `KvEngine::write_batch()` from one
OS thread per client with `WalIoMode::Leader`. Parallel calls public
`write_batch_async()` from client tasks, using eight Tokio workers and
`WalIoMode::Parallel`. The result compares the public APIs, execution models,
and WAL modes together; it does not isolate the WAL pipeline alone.

Each scored observation writes 524,288 values of 1 KiB, in batches of 64.
PITR and serializable transactions are off, compaction is disabled, and a 1 GiB
memtable target prevents timed rotation. Five blocks alternate ABBA and BAAB;
each arm has two observations per block. Client-count order rotates between
eight and sixteen writers. Each arm has two warmups per case. One Leader/Leader
control pair per case runs after all scored blocks, rather than after block 2
as stated in the frozen protocol; the frozen driver and raw records establish
the actual order. These end-of-session controls cannot establish stability
during each earlier block. Each fresh database is removed after
its run, followed by five seconds idle. No build, test, profiler, or trace runs
during measurement.

A block repeat control requires both observations in each arm to stay within
5% throughput and 10% p99. Leader control pairs use the same bounds. Runs with
p99 above 6 ms, or large rate/tail changes after two earlier same-arm
observations, receive one separate retry after a pause. Paired blocks with
throughput below 0.90 or p99 above 1.20 receive one excluded reversed-order
retry. Originals always remain in the estimate.

Paired throughput and p99 are medians of the five block geometric-mean
parallel/Leader ratios. Absolute medians are reported separately. Retries,
warmups, and null controls do not enter either estimate.

## Results

| Writers | Leader median puts/s | Parallel median puts/s | Paired throughput | Leader p99 ms | Parallel p99 ms | Paired p99 | Passing blocks | Leader null |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 8 | 382,095 | 325,524 | -32.4% | 2.342 | 2.616 | +138.6% | 1/5 | Fail |
| 16 | 535,594 | 536,402 | +6.9% | 3.172 | 3.473 | -0.7% | 1/5 | Pass |

### Highest scored primary run (descriptive)

| Writers | Leader peak puts/s (batch p99 ms, run) | Parallel peak puts/s (batch p99 ms, run) | Parallel peak delta |
| ---: | --- | --- | ---: |
| 8 | 394,917 (2.310, `w8-b0-s0-leader`) | 365,473 (2.431, `w8-b4-s2-parallel`) | -7.5% |
| 16 | 566,215 (3.049, `w16-b2-s3-leader`) | 625,347 (3.187, `w16-b3-s3-parallel`) | +10.4% |

Each peak is selected independently from ten scored primary runs in that arm;
the delta is a comparison of maxima, not a paired estimate. It is descriptive
and does not establish a repeatable gain. The p99 shown belongs to the run
with that arm's peak throughput.

The paired block throughput changes are:

| Writers | Block 1 | Block 2 | Block 3 | Block 4 | Block 5 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | -60.8% | -32.4% | -52.9% | -12.4% | -8.0% |
| 16 | +221.2% | +127.6% | +6.9% | -25.3% | -58.3% |

Only eight-writer block 4 and sixteen-writer block 3 pass repeat controls.
The sixteen-writer +6.9% paired result comes with one passing positive block;
the other blocks include stalled Leader or parallel runs. It is not a stable
throughput estimate. The absolute medians differ by only +0.2% for parallel,
while its p99 median is 9.5% higher.

At eight writers, the one passing block favors Leader by 12.4%. The null pair
also fails repeatability: its Leader throughput changes from 62,923 to
78,847 puts/s, outside the 5% limit. Across primary runs, batch p99 exceeds
6 ms in four of ten eight-writer parallel runs, three of ten sixteen-writer
Leader runs and two of ten sixteen-writer parallel runs. Stalls occur in both
modes, so the test cannot assign every slow run to one WAL mode.

The median sync counts are lower for parallel at both writer counts: 1,856.5
versus 2,084 at eight writers and 1,115 versus 1,236.5 at sixteen. This did
not produce a repeatable throughput gain here. The recorded software
in-flight-group peaks are one for Leader and eight/sixteen for parallel at
eight/sixteen writers. These are not device queue-depth measurements.

## Audit and limits

The five-block batch64 comparison does not measure one-put, one-writer or
four-writer guards, so it cannot qualify the full RFC adoption gate. In
particular, the sixteen-writer paired estimate is below the existing +10%
leader-relative threshold and fails the repeat-block requirement.

All 86 observations are preserved: eight warmups, 40 primaries, ten single
retries, 24 reversed-block retries and four Leader null observations. An
independent audit rechecks modes, workload counters, all 643,072 write CQEs,
block and null estimates, input hashes, and cleanup. The probe, 115 production
inputs, 251 source-manifest inputs and frozen driver remain unchanged throughout
measurement. The source manifests are archived under
`target/rfc024-native-sync-batching-20261006/` as
`production-input-hashes.json` and `candidate-input-hashes.json`. The
ext4 databases and RAM executable stage are removed.

Archive: `target/rfc024-native-leader-batch-rerun-20261006/`. It contains the
frozen protocol, driver, exact executable, environment, every stdout/stderr,
raw observations, retries, paired summaries, audit and cleanup record.
