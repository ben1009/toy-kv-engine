# RFC 024: native async batch ownership follow-up, 2026-10-05

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

No additional production optimization is retained. Three candidates removed
the post-durability key/value copy for native async batches of at least 64
input records. None met the frozen incremental retention rule. The current
[parallel v4 default](../rfc-024-parallel-wal-default-20261005.md) and the
previously validated implementation remain intact.

## Why batches need a different optimization

The [October 5 direct leader comparison](../benchmarks/rfc-024-native-async-vs-leader-20261005.md)
shows substantial one-put gains but mixed batch64 results. One-put commits
pay synchronization and scheduling costs per logical put; batches amortize
those costs across 64 puts. A large one-put gain does not imply the same
batch gain.

Four fresh, separate baseline diagnostics locate a substantial batch cost:
memtable publication takes 679–843 ms of aggregate phase wall time per
262,144 puts; its copy phase takes 280–336 ms, approximately 40% of that
publication time. Batch preparation takes 157–206 ms. These are sums across
concurrent tasks, not process CPU or elapsed time available to save.

The caller already prefixes values before MVCC admission. The redundant
copy examined here is from deferred publication entries into the memtable,
after WAL durability. No candidate changes sync batching, WAL admission,
physical ordering, fdatasync targets, or the PITR path.

## Candidates and protocol

Candidate A lowers the native owned-publication threshold from 512 to 64
records and prepares shared value buffers early. Candidate B preserves the
previous caller-side value allocation policy while still transferring the
owned publication entries. Candidate C additionally avoids eager shared
internal-key allocations for prepared native admissions: WAL encoding borrows
those keys, and publication moves them. Synchronous APIs retain their previous
policy throughout.

All screens use release builds with `bench`, native io_uring outside the
sandbox, fresh NVMe-backed ext4 databases on `/dev/nvme0n1p3`, 1 KiB values,
batch64, 8 Tokio runtime workers, PITR and serializable mode off, NoCompaction,
and a 1 GiB memtable target. Both scored arms use native async parallel WAL.
The baseline includes the already validated default switch and is identified
by its saved source manifest and executable hash.

Each screen has two warmups per arm/case, three balanced ABBA/BAAB/ABBA
blocks, two observations per arm per block, rotating 8/16-writer case order,
an identical-baseline pair per case, and two separate leader references per
case. The references are descriptive and do not enter incremental estimates.
A and B score 262,144 puts per run; C scores 524,288 to lengthen observations
without timed memtable rotation. Warmups contain 32,768 puts.

The rule is fixed before each screen: at least +5% sixteen-writer paired
median throughput, all three target blocks positive, and at least two passing
target repeat controls. Guard changes must stay within -5% throughput and
+10% p99; independent guard confirmation is required before retention.
A repeat control requires both repeats of each arm within 5% throughput and
10% p99. The same limits apply to identical-baseline pairs.

Every run is followed by five seconds idle. Obvious single-run anomalies
trigger an extra one-second pause and one separately recorded retry. A block
below 0.90 throughput ratio or above 1.20 p99 ratio triggers a reversed block
retry after that pause. Originals remain primary; retries are excluded from
scoring. No compilation, tests, tracing, or device administration overlaps
timed runs. Executables are staged in RAM and owned databases are cleaned.

## Scored results

Changes are medians of all three block geometric-mean candidate/baseline
ratios. Positive p99 or CPU change means worse. No failed control is waived.

| Candidate | Writers | Throughput | Commit p99 | Process CPU/put | Passing blocks | Baseline/baseline control | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| A: early shared values and keys | 8 | +6.1% | -3.9% | -10.3% | 2/3 | Failed | Revert |
| A: early shared values and keys | 16 | +3.6% | -7.0% | +2.4% | 0/3 | Passed | Revert |
| B: existing value preparation, shared keys | 8 | +3.9% | -4.3% | -12.2% | 1/3 | Failed | Revert |
| B: existing value preparation, shared keys | 16 | -1.0% | -4.8% | +5.6% | 1/3 | Failed | Revert |
| C: existing value preparation, cheap native keys | 8 | +2.4% | -3.2% | -12.4% | 0/3 | Failed | Revert |
| C: existing value preparation, cheap native keys | 16 | +4.1% | -5.2% | +2.0% | 1/3 | Passed | Revert |

A has a severe sixteen-writer stall in both arms: its second block ratio is
3.394, compared with 1.002 and 1.036 in the other blocks. C has eight-writer
block ratios of 1.585, 0.439, and 1.024, including a stalled baseline and a
later stalled candidate. Their retries recover, but do not replace originals.
C starts with stalls in every warmup, in both arms. These observations do
not identify the cause of the recurring storage stalls.

C has the most consistent target ratios: **1.037, 1.043, 1.041**, producing
+4.1% throughput and -5.2% p99. Its target still fails the +5% threshold and
has only one passing repeat block; process CPU per put increases 2.0%.
Independent guard confirmation is not run for any rejected candidate.

## Cost placement

Absolute phase medians below use primary observations at sixteen writers.
Values are aggregate phase wall time divided by commits, in microseconds
per commit; normalization allows comparison of the different run lengths.
The table is diagnostic rather than a paired estimate of CPU or elapsed
time saved.

| Screen | Arm | Batch preparation | MVCC/WAL preparation | Memtable publication | Publication copy phase |
| --- | --- | ---: | ---: | ---: | ---: |
| A | Baseline | 38.9 | 35.0 | 168.9 | 74.6 |
| A | Candidate | 110.2 | 44.4 | 137.3 | 2.4 |
| B | Baseline | 37.0 | 33.9 | 161.0 | 67.9 |
| B | Candidate | 107.9 | 49.1 | 134.3 | 2.3 |
| C | Baseline | 35.5 | 32.0 | 159.4 | 64.4 |
| C | Candidate | 97.2 | 41.8 | 133.5 | 2.3 |

The copy phase falls sharply, but preparation grows. Even B retains this
preparation increase despite using the same value-builder policy as its
baseline. Retaining the original allocations in the memtable changes their
reuse and lifetime; allocation placement is a plausible explanation that
needs allocation-level profiling. The phase measurements alone do not prove
that allocator reuse causes the increase. No allocation-count reduction is
implemented or qualified here.

The next experiment should reduce allocation count while preserving ownership
across cancellation, WAL retry, publication, and returned-value lifetimes.
Simply moving the existing copy or sharing policy again is not supported by
these results. Bounded pooling would also need explicit memory-retention
checks; the [earlier synchronous pooling experiment](rfc-024-batch-allocation-followup-20261004.md)
failed its own confirmation and is not retained.

The subsequent [bounded pooling follow-up](rfc-024-native-batch-pooling-20261006.md)
measures allocation counts and tests that hypothesis. It removes up to 77.1%
of Rust allocation calls, but preparation and insertion costs grow at
sixteen writers. Both pooling candidates fail their retention rule and are
reverted.

## Verification and artifacts

All three candidates passed the native async selection: **28 tests each**,
including temporary ownership/recovery checks around 63/64/65 records and
dedup/TTL/delete coverage at the owned-publication boundary. C additionally
covers 512/513 records. The temporary code and tests are archived with their
candidate patches and removed from production sources. A fresh rebuild of
the restored path passes **27 native async tests**.

All **129 screen observations** reconcile with raw JSON, sampled commit
counts, completed write CQEs, throughput, modes, and runtime settings. All
18 scored blocks are recomputed. The screen drivers verify source, binary,
and driver hashes at completion. The baseline executable SHA-256 is
`95460bcf994a1f9d515b08af80374b18570a55a0945cf4665f26a2572bac6c4d`.

The first A driver attempt failed after one warmup because a Python loop
variable shadowed `round()`. That attempt is archived separately under
`failed-driver-start/`; it contains no scored runs. The corrected driver
started with fresh output files. Four baseline diagnostic runs are also
separate from the 129 screen observations.

Only the three experimental source files were restored, preserving previous
default and documentation work. All **114 baseline input hashes** match.
Restored source timestamps are refreshed before rebuilding to prevent Cargo
from reusing candidate artifacts. Temporary databases and RAM stages are
removed; raw observations, protocols, binaries, source snapshots, tests,
drivers, verification records, and rejected patches remain available locally.

- **A:** protocol: `target/rfc024-native-batch-owned-20261005/screen-protocol.json`, observations: `target/rfc024-native-batch-owned-20261005/screen-records.json`, summary: `target/rfc024-native-batch-owned-20261005/screen-summary.json`, verification: `target/rfc024-native-batch-owned-20261005/verification.json`, and rejected patch: `target/rfc024-native-batch-owned-20261005/candidate.patch`.
- **B:** protocol: `target/rfc024-native-batch-owned-late-20261005/screen-protocol.json`, observations: `target/rfc024-native-batch-owned-late-20261005/screen-records.json`, summary: `target/rfc024-native-batch-owned-late-20261005/screen-summary.json`, verification: `target/rfc024-native-batch-owned-late-20261005/verification.json`, and rejected patch: `target/rfc024-native-batch-owned-late-20261005/candidate.patch`.
- **C:** protocol: `target/rfc024-native-batch-owned-keys-20261005/screen-protocol.json`, observations: `target/rfc024-native-batch-owned-keys-20261005/screen-records.json`, summary: `target/rfc024-native-batch-owned-keys-20261005/screen-summary.json`, verification: `target/rfc024-native-batch-owned-keys-20261005/verification.json`, and rejected patch: `target/rfc024-native-batch-owned-keys-20261005/candidate.patch`.
- Baseline diagnostics: `target/rfc024-native-batch-owned-20261005/diagnostic-records.json` and restoration: `target/rfc024-native-batch-owned-keys-20261005/restoration.json`.
