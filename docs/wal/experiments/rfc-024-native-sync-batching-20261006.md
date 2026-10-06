# RFC 024: native sync-batching limit, 2026-10-06

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Increasing the parallel WAL's conditional coalescing limit from 400 to 600
microseconds is **not qualified for retention**. Both identical-baseline
controls fail, only one of six comparison blocks passes its repeat control,
and the eight-writer throughput and p99 guards fail. Production remains at
400 microseconds; the candidate and its patch stay in the local archive.

The one repeatable block, at eight writers, reduces mean sync count from
1,874 to 1,632.5, but throughput changes by -0.06%. Its batch p99 improves
by 5.0%. Fewer sync calls alone do not establish a throughput improvement.
That block also cannot qualify retention independently of the failed session
controls. There is no qualified sixteen-writer improvement in this experiment.

## Current-policy diagnostic

The [preceding kernel-dispatch diagnostic](rfc-024-native-async-kernel-dispatch-20261006.md)
shows limited benefit from forced-async submission and substantial sync elapsed
time. This experiment first instruments the current coordinator, including
its retained cutoff refresh and bounded completion drain. An earlier fixed-cutoff
diagnostic cannot describe the refreshed policy's batching exits.

An isolated copy records ticket admission timestamps, coalescing entry/exit,
initial and final cutoff, written frontier, refresh count, exit reason and
queued results. It also records each captured sync range and approximate
syscall endpoints. The diagnostic adds timestamp and collection work, including
an admission-side collection mutex; its timing is descriptive and never scores
the candidate. It does not change the coalescing policy.

Each primary observation uses 524,288 puts as 8,192 batches of 64, 1 KiB values,
eight Tokio workers, explicit native parallel WAL and a 1 GiB memtable target.
PITR and serializable transactions are off; there is no timed rotation. Fresh
databases use ext4 `/dev/nvme0n1p3` on Linux `6.12.71-1-lts`. One 32,768-put
warmup per case precedes three rotating eight/sixteen-writer rounds. Runs have
a five-second idle gap. A primary batch p99 above 6 ms triggers an extra
one-second pause and one separate retry. All six primaries, both warmups and
the one retry are preserved.

All primary diagnostic observations are below. The ticket count is the mean
admitted-but-unwritten count at deadline exits, rather than a device queue
depth. Time columns are within-run medians in microseconds.

| Writers | Round | Deadline exits | Unwritten tickets at deadline | Coalescing wait | Sync interval | Batch p99, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 7.0% | 1.88 | 31.9 | 914.5 | 3.592 |
| 8 | 2 | 7.1% | 1.71 | 124.6 | 318.9 | 2.492 |
| 8 | 3 | 7.0% | 1.87 | 93.8 | 342.3 | 2.417 |
| 16 | 1 | 30.7% | 2.80 | 297.3 | 541.2 | 4.025 |
| 16 | 2 | 38.1% | 4.90 | 303.0 | 1,028.7 | 29.845 |
| 16 | 3 | 31.2% | 3.33 | 228.2 | 451.4 | 3.039 |

The sixteen-writer round-two retry also stalls: 38.573 ms p99, 38.4% deadline
exits and 4.53 unwritten tickets on average at those exits. The two sixteen-writer
observations below the 6 ms threshold hit the limit in about 31% of cycles with
roughly three admitted tickets still unwritten. This motivates testing a slightly
longer existing wait. It does not justify a quiet grace period after catching
admission: most caught-up exits are followed by the next admission hundreds
of microseconds later, and writers may be waiting for the current sync.

The sync interval includes surrounding timestamp work and the syscall; it
does not isolate device service time. Coalescing timeout elapsed time can
exceed its logical deadline because of scheduling. Concurrent thread timers
cannot be added to reconstruct end-to-end batch latency.

## Candidate and frozen comparison

The uninstrumented candidate changes only `SYNC_COALESCE_WAIT` from 400 to
600 microseconds. The previous-sync threshold stays at 100 microseconds;
cutoff refresh cannot extend the fixed deadline. Ticket admission, group
packing, ring setup, SQE flags, buffer ownership, captured sync targets and
poison boundaries are unchanged. No quiet grace, mode fallback, Leader change
or production selector is added.

The baseline is the exact current ordinary-SQE timing executable from the
[submission experiment](rfc-024-native-submission-and-key-preparation-20261006.md).
All 115 production Rust/Cargo/toolchain inputs match that experiment. The
candidate probe uses the same public native async workload without the new
instrumentation. Both binaries run from RAM with fresh ext4 databases, and
builds, tests and tracing finish before comparison timing begins.

The workload matches the primary diagnostic above. Each case has two warmups
per arm, then three ABBA/BAAB/ABBA blocks with alternating case order. Each
arm has two runs per block. One identical-baseline pair per case follows the
second block. The estimate is the median of the three block geometric-mean
ratios; absolute medians are a different statistic.

The frozen retention gate requires at least +2% paired target throughput at
sixteen writers, all three target blocks positive, at least two passing repeat
controls per case, both null controls passing, and both cases within -5%
throughput and +10% p99. Repeat controls require each arm's within-block
throughput max/min to be at most 1.05 and p99 max/min at most 1.10. Separate
single-writer and one-put guards are required only after this screen passes.
This incremental threshold does not replace the full RFC qualification gate.

Every run has a five-second idle gap. A primary p99 above 6 ms, or a rate below
70% or p99 above 150% of the median after two earlier same-arm/case primaries,
triggers an extra one-second pause and one excluded retry. A block throughput
ratio below 0.90 or p99 ratio above 1.20 triggers one excluded reversed-block
retry. Originals always remain scored; retries never replace them.

## Comparison results and controls

All original block results are reported below. Failed controls make these
observations unsuitable as estimates of the candidate's causal effect. In
particular, the large positive sixteen-writer ratios compare stalled baseline
runs with substantially faster candidate runs; they are not qualified gains.

| Writers | Block | Paired throughput change | Paired p99 change | Repeat control |
| --- | ---: | ---: | ---: | --- |
| 8 | 1 | -63.4% | +586.2% | Fail |
| 8 | 2 | -55.4% | +206.1% | Fail |
| 8 | 3 | -0.06% | -5.0% | Pass |
| 16 | 1 | +3.2% | -46.6% | Fail |
| 16 | 2 | +500.3% | -91.7% | Fail |
| 16 | 3 | +66.9% | -10.3% | Fail |

Identical-binary controls independently demonstrate the session's instability:

| Writers | First baseline, puts/s | Second baseline, puts/s | First p99, ms | Second p99, ms | Null control |
| --- | ---: | ---: | ---: | ---: | --- |
| 8 | 347,531 | 71,387 | 2.540 | 23.915 | Fail |
| 16 | 84,140 | 596,384 | 36.048 | 3.233 | Fail |

Of the six primary runs per arm/case, p99 exceeds 6 ms in three eight-writer
candidate runs, four sixteen-writer baseline runs and one sixteen-writer
candidate run. None of the eight-writer baseline primaries crosses that
threshold, but its identical-binary control and a block retry do. These
observations cannot isolate all stalls to either candidate code or scheduling.

The only repeatable block has geometric-mean throughput of 340,913 versus
340,713 puts/s and p99 of 2.556 versus 2.429 ms for baseline/candidate. Mean
sync count falls 12.9%, while throughput stays flat. This is evidence against
promoting the larger limit simply because it batches more tickets.

The numerical sixteen-writer throughput threshold passes, but the repeat/null
controls and eight-writer guards fail. The screen fails; full RFC performance
qualification remains **UNQUALIFIED**. No additional single-writer, one-put or
rotation retention guards are run after this failure. Software in-flight-group
peaks are eight at eight writers and fifteen or sixteen at sixteen writers;
the configured 32-group limit is unchanged. These counts are not device queue
depth.

## Validation, audit and archive

The private diagnostic passes formatting, default/all-feature all-target
Clippy, 83 focused parallel-WAL tests and three probe/recovery tests. The
uninstrumented candidate passes formatting, both Clippy configurations, all
1,292 all-feature library tests and the three probe tests. Native io_uring
tests and benchmarks run outside the sandbox. These checks establish no
performance qualification.

An independent recomputation reconciles all 52 comparison observations:
eight warmups, 24 scored primaries, four null observations, eight single retries
and eight block-retry observations. All 364,544 terminal write CQEs match
batch and buffer counts. Block ratios, controls, case medians and gate results
are recomputed from saved stdout. All original observations remain scored.
The frozen source manifests, binaries and driver remain unchanged; all owned
temporary databases and RAM stages are removed. Production inputs stay
unchanged throughout the experiment.

Archive: `target/rfc024-native-sync-batching-20261006/`. It contains the private
instrumented and candidate sources, candidate patch, frozen protocols and
hashes, release/check logs, all raw outputs, diagnostic analysis, comparison
records, controls, gate summary, independent audit and cleanup records.
The exact uninstrumented timing binaries have these SHA-256 hashes:

- Baseline: `7f113ba4b8c116ea7adfb3bc058b983f7b10ad64db4023a5e93ecc62f8ecef76`.
- Candidate: `c280c4d3205c8cd18ec7d5c35c1a5625e877a8bd8ea15b0a81f832031d473f0b`.
