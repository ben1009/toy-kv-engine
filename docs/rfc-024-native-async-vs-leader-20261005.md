# RFC 024: native async parallel WAL versus leader, 2026-10-05

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Measured revision: `d7d124b88d73c3ddc69705cf44701fd3240d51df`.

The reviewed implementation at that revision was measured directly against leader WAL
for one put per commit and 64 puts per commit, at 1, 4, 8, and 16 writers.
Native async has higher peak one-put throughput at every tested writer count
(+36.3% to +129.8%), with lower p99 in each selected peak run. Batch64 remains
mixed: peak throughput changes range from -8.8% to +8.9%. These observations
do not qualify the full RFC adoption gate. Repeatability is incomplete, the
batch64 one-writer paired throughput and eight-writer p99 guards fail.
Ordinary v4 WALs now default to
parallel by maintainer decision; see the [default adoption note](rfc-024-parallel-wal-default-20261005.md).

The subsequent [native batch ownership follow-up](rfc-024-native-batch-ownership-followup-20261005.md)
profiles publication copying and tests three incremental candidates. None
passes its retention rule; the measured implementation below remains intact.

The [bounded pooling follow-up](rfc-024-native-batch-pooling-20261006.md)
reduces measured Rust allocation calls by up to 77.1%, but its two candidates
regress sixteen-writer throughput by 3.9% and 3.0%. Both are reverted.
The [root-cause investigation](rfc-024-native-batch-pooling-cause-20261006.md)
subsequently isolates skip-list metadata contention and a shift in memory
first-touch work. Its private dependency experiments do not alter the
leader comparison or qualify a retained optimization.

The [metadata-only native screen](rfc-024-native-metadata-padding-20261006.md)
then tests padding without pooling or an external profiler. Sixteen-writer
paired throughput changes by -1.5%, with all target repeat controls passing;
the private dependency patch fails its retention rule and is rejected.
The leader comparison below remains intact.

The subsequent [completion-drain follow-up](rfc-024-native-completion-drain-20261006.md)
retains a bounded completion drain by maintainer decision. Its incremental
sixteen-writer batch64 estimate is +2.1% throughput and -2.4% p99, with failed
repeat controls. A separate moving-cutoff rerun reports +3.1% throughput at
sixteen writers and remains private. These observations compare parallel
variants; they do not update the measured native-versus-leader results below.

The [durability-wakeup investigation](rfc-024-native-durable-wake-20261006.md)
then tests targeted async notifications and notification after mutex release.
Neither is retained: the latter's initial +2.4% sixteen-writer estimate becomes
-0.4% in an independent confirmation with all target repeat controls passing.
These incremental experiments leave the measured leader comparison below intact.

The [cutoff-refresh follow-up](rfc-024-native-cutoff-refresh-20261006.md)
subsequently retains optional coalescing cutoff refresh on top of the bounded
drain by maintainer decision. Confirmation reports +4.2% sixteen-writer batch64
throughput; its null control fails. The one-put guard reports +13.4% throughput
against the preceding parallel backend. These incremental results preserve the
leader comparison below and leave the full RFC gate unqualified.

The [October 6 batch64 versus leader rerun](rfc-024-native-batch64-leader-rerun-20261006.md)
measures that retained backend at eight and sixteen writers. It records +6.9%
paired throughput at sixteen writers, with only one of five repeat blocks
passing. Its results are separate from the October 5 observations below.

## Measured paths and environment

Both datasets use the same freshly rebuilt release probe with `bench` enabled.
It includes the shutdown, transaction, and checkpoint-publication review fixes.
The executable SHA-256 is
`004e9ac8b869c989dce2cf60b5f9f8383bcad5d58627fc4a5aebb0c3c8362725`.

| Setting | Value |
| --- | --- |
| WAL filesystem | ext4 on `/dev/nvme0n1p3`, mounted at `/home`, `rw,relatime` |
| Kernel | `6.12.71-1-lts` |
| Toolchain | `nightly-2026-09-23`; rustc `1.100.0-nightly (6bb1652a0 2026-09-22)` |
| CPU affinity | CPUs 0–31 |
| Values | 1 KiB |
| Engine options | Ordinary v4 WAL, PITR off, serializable off, NoCompaction |
| Memtable target | 1 GiB; no rotation in the timed window |
| Parallel limits | 32 in-flight groups, 256 ring entries |
| Detailed profiling | Disabled |

The leader arm calls public `write_batch()` on one OS thread per client with
`WalIoMode::Leader`. The native arm calls public `write_batch_async()` on one
Tokio task per client with `WalIoMode::Parallel`. Its runtime uses
`min(clients, 8)` workers. The one-put case supplies one entry to these batch
APIs. This is a combined API, scheduler, and WAL-mode comparison; it does not
isolate the WAL pipeline or establish that Tokio alone caused the difference.

## Frozen protocol and estimators

Each case has five balanced blocks, alternating ABBA and BAAB, with two
observations per arm per block. Writer-count order rotates between blocks.
Each arm/case has two warmups. One-put primary observations contain 50,000
puts; warmups contain 12,500. Batch64 observations contain 262,144 puts
(4,096 commits); warmups contain 32,768 puts. Every commit latency is sampled.

Latency includes record construction, commit, and caller resumption. Key
formatting is outside the latency window but inside the throughput window.
Flush and close occur after timing. Every run uses an owned fresh database,
then removes it and idles for five seconds. Executables are staged in RAM;
no concurrent build, tracing, test, or benchmark work runs during measurement.

A leader/leader control pair runs for each writer count after block 2.
A block passes repeatability only if both repeats of each arm stay within
5% throughput and 10% p99. The same limits apply to identical-leader controls.

An obvious regression triggers an extra one-second pause and one separately
labelled retry: p99 above 6 ms, or, after two previous primary observations
for the same arm/case, throughput below 70% or p99 above 150% of their median.
A block with native/leader throughput below 0.90 or p99 above 1.20 triggers
one reversed-order block retry after the extra pause. All originals remain
primary. Warmups, controls, and retries are excluded from scored results.

Three summaries answer different questions:

- **Peak:** the highest of ten primary throughput observations per arm/case.
  Each accompanying p99 belongs to that fastest run, rather than an
  independently selected lowest-latency run. Peaks are selected independently
  and are descriptive, not paired estimates.
- **Absolute median:** the median of ten primary observations per arm/case.
- **Paired change:** the median of five block geometric-mean native/leader
  ratios. It need not equal the ratio of absolute medians. Positive p99 or
  process CPU change means worse latency or greater CPU cost.

## One put per commit

### Highest scored throughput

| Writers | Leader puts/s | Native puts/s | Throughput change | Leader p99 ms | Native p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,853 | 2,525 | +36.3% | 1.159 | 0.715 |
| 4 | 4,074 | 9,360 | +129.8% | 2.328 | 0.847 |
| 8 | 7,504 | 16,855 | +124.6% | 2.347 | 1.068 |
| 16 | 13,975 | 26,067 | +86.5% | 2.422 | 1.385 |

### Paired estimates and controls

| Writers | Throughput change | Commit p99 change | Process CPU/put change | Passing blocks | Leader/leader control |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | +67.3% | -76.4% | +345.1% | 0/5 | Failed |
| 4 | +136.4% | -62.6% | +80.4% | 2/5 | Passed |
| 8 | +121.7% | -51.2% | +60.0% | 4/5 | Passed |
| 16 | +82.5% | -41.6% | +15.4% | 4/5 | Passed |

### Absolute primary medians

| Writers | Arm | Puts/s | Commit p99 ms | Process CPU µs/put | Sync calls/run |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Leader | 1,844 | 1.163 | 32.534 | 50,000.0 |
| 1 | Native async parallel | 2,503 | 0.734 | 149.185 | 50,000.0 |
| 4 | Leader | 3,943 | 2.341 | 44.808 | 22,708.5 |
| 4 | Native async parallel | 9,237 | 0.870 | 84.209 | 12,574.0 |
| 8 | Leader | 7,395 | 2.369 | 47.711 | 11,963.0 |
| 8 | Native async parallel | 16,512 | 1.162 | 75.487 | 6,973.0 |
| 16 | Leader | 13,727 | 2.442 | 63.966 | 6,124.0 |
| 16 | Native async parallel | 25,118 | 1.404 | 73.941 | 5,866.0 |

The one-writer paired gain is affected by slow leader runs. Its identical
leader control falls to 0.318× throughput with 14.374× p99, and a late native
primary run also stalls. All four paired cases use more process CPU per put.
The eight- and sixteen-writer cases pass four of five repeat blocks and their
identical-leader controls. These are encouraging observations, with no control
limits waived.

## 64 puts per commit

### Highest scored throughput

| Writers | Leader puts/s | Native puts/s | Throughput change | Leader p99 ms | Native p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 88,553 | 84,490 | -4.6% | 1.225 | 1.219 |
| 4 | 214,234 | 215,021 | +0.4% | 2.357 | 2.387 |
| 8 | 392,335 | 357,997 | -8.8% | 2.335 | 2.694 |
| 16 | 556,375 | 605,643 | +8.9% | 3.019 | 3.082 |

### Paired estimates and controls

| Writers | Throughput change | Commit p99 change | Process CPU/put change | Passing blocks | Leader/leader control |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | -5.6% | +3.3% | +36.7% | 2/5 | Passed |
| 4 | -1.1% | +3.0% | +32.4% | 5/5 | Passed |
| 8 | -9.8% | +13.8% | +39.4% | 2/5 | Failed |
| 16 | +7.1% | +2.2% | -31.2% | 2/5 | Failed |

### Absolute primary medians

| Writers | Arm | Puts/s | Commit p99 ms | Process CPU µs/put | Sync calls/run |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Leader | 84,519 | 1.210 | 5.056 | 4,096.0 |
| 1 | Native async parallel | 80,070 | 1.250 | 6.779 | 4,096.0 |
| 4 | Leader | 211,156 | 2.347 | 6.233 | 2,061.5 |
| 4 | Native async parallel | 206,971 | 2.401 | 8.199 | 1,978.0 |
| 8 | Leader | 376,983 | 2.372 | 6.631 | 1,056.0 |
| 8 | Native async parallel | 343,832 | 2.692 | 9.017 | 1,176.5 |
| 16 | Leader | 535,631 | 3.162 | 9.493 | 652.0 |
| 16 | Native async parallel | 573,158 | 3.292 | 6.395 | 628.5 |

All five sixteen-writer block throughput ratios favor native, but only two
blocks pass repeatability and the identical-leader control fails. Its +7.1%
paired throughput gain remains below the RFC’s +10% threshold. The eight-writer
leader/leader control changes from 65,204 to 376,538 puts/s (5.775×). The
one-writer paired throughput regression is 5.6%; eight-writer paired p99
regresses 13.8%. Batch64 therefore does not meet the adoption gate.

## Pipeline depth and durability work

Ranges below are the observed per-run maxima across ten primary runs.
Leader keeps one group in flight but may have several write SQEs in that
group. Native reaches multiple in-flight groups. These are software counters;
NVMe device queue depth was not measured.

| Workload | Writers | Leader groups | Native groups | Leader outstanding write SQEs | Native outstanding write SQEs |
| --- | ---: | ---: | ---: | ---: | ---: |
| One put | 1 | 1 | 1 | 1 | 1 |
| One put | 4 | 1 | 4 | 4 | 4 |
| One put | 8 | 1 | 8 | 8 | 8 |
| One put | 16 | 1 | 16 | 14–16 | 16 |
| Batch64 | 1 | 1 | 1 | 1 | 1 |
| Batch64 | 4 | 1 | 4 | 3 | 4 |
| Batch64 | 8 | 1 | 7–8 | 7 | 7–8 |
| Batch64 | 16 | 1 | 15–16 | 13–15 | 15–16 |

At most 16 logical commits are outstanding here, so these runs cannot fill
all 32 configured group slots. At one writer, both arms make one sync per
commit: the observed one-put improvement cannot be attributed to concurrent
client commits or fewer sync calls. Sync counts describe coalescing at higher
writer counts; they do not prove that writes advanced during each sync.
The full overlap and other RFC matrix requirements remain separate.

## Relation to earlier async results

The [integration study](rfc-024-native-async-integration-20261005.md) reported
+13.7% paired throughput against integrated synchronous **parallel** WAL and
+13.6% against a retained **parallel** executable at sixteen writers/batch64.
Those controls were not leader WAL. Its native absolute median was 603,677
puts/s; this October 5 direct study records 573,158 puts/s. Different
source revisions and sessions prevent attributing that difference to one fix.
The archived integration repeat controls also failed. Earlier native-wait
prototype and synchronous leader/parallel studies remain historical evidence;
their observations are not pooled into this report or its peak selection.

This direct comparison shows that the measured native code can reach
605,643 puts/s at sixteen writers/batch64, versus a leader peak of 556,375
puts/s, but peak selection is not the RFC’s paired-median gate. The one-put
gains do not remove batch64 regressions, failed controls, or missing full-matrix
qualification. The later default change is an explicit adoption decision,
not a new performance measurement.

## Verification and reproduction artifacts

Independent verification reconciled 237 raw observations across the two
datasets: 117 one-put and 120 batch64. Every record agrees with parsed raw
stdout. Commit sample, WAL buffer, and write CQE counts reconcile in every
observation. Paired ratios, medians, repeat controls, and peak selections were
recomputed. All 118 source/config hashes, the executable hash, and each
driver hash remained unchanged during the corresponding run. Owned database
directories and RAM executable stages were cleaned.

| Dataset | Primary | Warmup | Identical controls | Smoke | Single retries | Block-retry observations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One put | 80 | 16 | 8 | 2 | 7 | 4 |
| Batch64 | 80 | 16 | 8 | 2 | 2 | 12 |

Local artifact roots (ignored by Git):

- **One put:** protocol: `target/rfc024-native-vs-leader-single-d7d124b8/protocol.json`, observations: `target/rfc024-native-vs-leader-single-d7d124b8/records.json`, blocks: `target/rfc024-native-vs-leader-single-d7d124b8/blocks.json`, summary: `target/rfc024-native-vs-leader-single-d7d124b8/summary.json`, peak selections: `target/rfc024-native-vs-leader-single-d7d124b8/peak-comparison.json`, verification: `target/rfc024-native-vs-leader-single-d7d124b8/verification.json`, and runner: `target/rfc024-native-vs-leader-single-d7d124b8/run.py`.
- **Batch64:** protocol: `target/rfc024-native-vs-leader-batch64-d7d124b8/protocol.json`, observations: `target/rfc024-native-vs-leader-batch64-d7d124b8/records.json`, blocks: `target/rfc024-native-vs-leader-batch64-d7d124b8/blocks.json`, summary: `target/rfc024-native-vs-leader-batch64-d7d124b8/summary.json`, peak selections: `target/rfc024-native-vs-leader-batch64-d7d124b8/peak-comparison.json`, verification: `target/rfc024-native-vs-leader-batch64-d7d124b8/verification.json`, and runner: `target/rfc024-native-vs-leader-batch64-d7d124b8/run.py`.
- Probe source: `target/rfc024-native-vs-leader-batch64-d7d124b8/probe/src/main.rs` and compiled input hashes: `target/rfc024-native-vs-leader-batch64-d7d124b8/compiled-input-hashes.json`.

Both roots retain the measured executable, raw stdout/stderr, retry records,
and identical-control records. The tables and method in this tracked report
preserve the results for repository readers; local artifact paths require the
original workspace. The measured runs changed no production default or runtime
configuration; the subsequent default adoption is documented separately.
