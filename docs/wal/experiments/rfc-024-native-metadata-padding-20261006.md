# RFC 024: native skip-list metadata padding screen

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Separating the skip-list's shared metadata does **not establish an end-to-end
gain** sufficient to retain a dependency patch. Sixteen-writer paired throughput
has a **-1.5% point estimate**, with all three repeat controls passing.
Recorded skip-list insertion falls **11.7%**, but total publication falls only
**3.9%**. The eight-writer comparison fails its stability controls.

The candidate remains private. Production code and dependencies, including
the existing ordinary-v4 parallel default, are unchanged. This is an incremental
native-parallel experiment; it neither repeats the leader comparison nor
qualifies the full RFC performance gate.

## Candidate and baseline

The [previous root-cause investigation](rfc-024-native-batch-pooling-cause-20261006.md)
found contention among crossbeam-skiplist 0.1.3's neighboring `seed`, `len`,
and `max_height` atomics. A private layout intervention improved isolated
eight-thread insertion by 88.2%; its instrumented native observations did
not qualify an end-to-end gain.

This screen tests that layout intervention alone against the current native
implementation. Each of the three atomics receives its own `CachePadded`
wrapper. The RNG, length counter, atomic operations, memory orders, node
layout, and public API are preserved. Two raw-pointer tower-index autorefs
are spelled explicitly, and a deprecated constant becomes `isize::MAX`,
to compile with the pinned nightly. These compatibility changes preserve
their behavior.

Neither input pooling nor internal-key pooling is present. Both executables
use identical probe source and the production native commit path. The freshly
built baseline also matches the previously archived baseline executable hash.
The candidate is selected by a dependency patch in a private probe workspace;
the production Cargo manifest and lockfile are untouched.

An independently inspected [upstream draft proposal](https://github.com/crossbeam-rs/crossbeam/pull/1254)
reports contention in the length counter and random-height seed. Its removal
of the length counter and change to thread-local RNG are outside this experiment.

## Frozen comparison

| Setting | Value |
| --- | --- |
| API and WAL | Public `write_batch_async()`, parallel ordinary v4 WAL |
| Clients | 8 and 16 |
| Runtime | Eight Tokio workers, no blocking job per batch |
| Primary puts | 524,288 per observation |
| Batch and values | 64 puts per commit, 1 KiB values |
| Engine | PITR and serializable off, NoCompaction, 1 GiB memtable target |
| Timed rotation | None |
| Storage | ext4 on `/dev/nvme0n1p3`, `rw,relatime` |
| Host | Intel i9-13900T, Linux `6.12.71-1-lts`, CPUs 0–31 allowed |
| Rust | `nightly-2026-09-23` |

There are two warmups per arm/case, each with 32,768 puts, followed by three
fixed ABBA/BAAB/ABBA blocks. Each block has two observations per arm; client
order rotates. An identical-baseline pair runs for each case after block 1.
Executables are staged in RAM, databases are fresh, and each run is followed
by five seconds idle. No build, test, profiler, allocation counter, or device
administration overlaps measurement. Normal engine benchmark counters remain
enabled identically in both arms.

The throughput window includes client preparation and completion of all
commits; flush and close follow measurement. Every commit latency is sampled.
The estimator is the median of three block ratios, each computed from the
geometric mean of its two candidate observations and two baseline observations.
Absolute medians are reported separately and are not the paired estimator.

Retention requires at least **+5% sixteen-writer paired throughput**, all three
target blocks positive, at least two passing repeat controls per case, and
both identical-baseline controls passing. Both cases must stay within -5%
throughput and +10% p99. Independent single-writer and one-put confirmation
would be required before adoption. A block repeat control requires both arms'
repeats within 5% throughput and 10% p99; null pairs use the same limits.

An obvious regression receives an extra one-second pause followed by one
separately labeled retry. The trigger is p99 above 6 ms, or, after two earlier
same-arm/case primary observations, throughput below 70% or p99 above 150%
of their median. A block ratio below 0.90 throughput or above 1.20 p99 would
trigger one reversed-order retry block. Originals remain primary; retries
never replace them or enter the estimates.

## Results

| Clients | Paired throughput | Paired p99 | Paired CPU/put | Passing repeat blocks | Null control |
| --- | ---: | ---: | ---: | ---: | --- |
| 8 | +1.3% descriptive | +4.0% | -6.2% | 1/3 | Fail |
| 16 | -1.5% | -1.5% | -1.8% | 3/3 | Pass |

The sixteen-writer throughput blocks are **-2.9%, +2.4%, and -1.5%**.
This fails both the +5% target and the requirement that every target block
be positive, even though all its repeat controls pass. Independent adoption
confirmation therefore does not run.

Passing a 5% within-block repeat limit does not establish statistical significance
for a -1.5% estimate. The blocks change sign, so the retention decision is
**no demonstrated gain**, rather than proof that padding causes a slowdown.

| Clients | Baseline median puts/s | Candidate median puts/s | Baseline median p99 | Candidate median p99 |
| --- | ---: | ---: | ---: | ---: |
| 8 | 343,700 | 341,670 | 2.650 ms | 2.692 ms |
| 16 | 594,010 | 598,464 | 3.151 ms | 3.072 ms |

The sixteen-writer absolute throughput medians differ by +0.7%, while the
paired block estimator is -1.5%. These are different estimators over the
same observations; the paired result determines retention. Neither meets
the +5% threshold.

Eight-writer blocks are -4.1%, +88.4%, and +1.3%. The large middle result
includes stalls in both arms, including a baseline writer window of 22.46 s
with 19.78 s of fdatasync wall time. Its retry returns to 1.50 s and 0.99 s,
respectively. The stalled candidate takes 6.11 s with 5.40 s of fdatasync;
its retry remains slow at 6.26 s. All originals and retries are retained.
The identical-baseline throughput spread is 5.07%, also outside the frozen
5% limit. The eight-writer estimates establish no gain.

## What insertion savings changed

The sixteen-writer paired phase changes are:

| Recorded measure | Change |
| --- | ---: |
| Skip-list insertion subphase | -11.7% |
| Total memtable publication phase | -3.9% |
| Publication-copy subphase | +3.3% |
| Native batch preparation | -0.1% |
| fdatasync wall time | +2.6% |
| Sync count | -1.1% |

These are summed phase timers, including scheduling within their scopes,
rather than hardware CPU attribution. Publication contains its subphases;
do not add them again to its total. fdatasync overlaps other work, so its
wall time is not a measure of serialized client latency.

For example, the final sixteen-writer block has these geometric means of
the two observations per arm:

| Measure | Baseline | Candidate | Scope |
| --- | ---: | ---: | --- |
| Writer-window elapsed time | 887.5 ms | 900.8 ms | End-to-end wall time |
| Skip-list insertion | 493.0 ms | 434.2 ms | Summed across concurrent publishers |
| Publication copies | 516.6 ms | 533.9 ms | Summed across concurrent publishers |
| fdatasync | 641.8 ms | 658.4 ms | Wall time of the single sync coordinator's calls |

The 58.8 ms insertion reduction is accumulated across overlapping tasks;
it is not a 58.8 ms reduction of the writer window. Copy and sync timings also
increase in this block. These overlapping rows cannot be added or subtracted
to reconstruct elapsed time. The sync coordinator spends approximately 72%
of the writer window inside fdatasync in both arms, while writes and publication
can overlap those calls.

This is consistent with a small insertion saving being masked by other work
and sync variation. The existing counters do not distinguish ordinary run
variation from a change in submission scheduling or filesystem interaction
caused by padding. Proving that mechanism requires tracing the commit critical
path; a lower insertion timer alone cannot establish it.

Padding lowers insertion time in every sixteen-writer block. That supports
the earlier contention finding, but the storage-independent insertion probe
keeps all inserters continuously active. Native commits also prepare buffers,
wait for durability, and copy publication data. This screen demonstrates that
the local insertion saving does not produce a retained end-to-end benefit;
it does not identify one exclusive remaining bottleneck.

Both arms reach eight in-flight groups/write SQEs at eight clients. At sixteen
clients, runs reach fifteen or sixteen. These are software pipeline counts,
not block-device queue depth. The layout change neither raises pipeline
limits nor establishes increased SSD utilization.

## Verification and evidence

Before measurement, the private dependency passes **118 upstream tests**:
23 base, 33 map, 22 set, and 40 documentation tests. A private copy of the
current engine passes **1,292 all-feature library tests** and **1,198
default-feature library tests** with the dependency patch. Both feature
configurations pass all-target Clippy with `-D warnings`. The candidate
probe's three tests also pass, covering both APIs, remainder batches,
clients with no work, flush, close, and recovery. io_uring checks run outside
the sandbox.

All **38 observations** complete: 8 warmups, 24 primary comparisons, 4 null
observations, and 2 excluded single retries. Every observation reconciles
commit samples, completed write CQEs, and buffers. All 114 production input
hashes and all 144 frozen source/protocol hashes match after measurement.
Executable and driver hashes also match. Owned databases and RAM stages
are removed. The private source and executables remain for reproduction.

- Frozen protocol: `target/rfc024-native-metadata-padding-20261006/screen-protocol.json` and driver: `target/rfc024-native-metadata-padding-20261006/screen.py`.
- Summary: `target/rfc024-native-metadata-padding-20261006/screen-summary.json`, block results: `target/rfc024-native-metadata-padding-20261006/screen-blocks.json`, and null controls: `target/rfc024-native-metadata-padding-20261006/screen-nulls.json`.
- All observations: `target/rfc024-native-metadata-padding-20261006/screen-records.json` and phase summary: `target/rfc024-native-metadata-padding-20261006/screen-phase-summary.json`.
- Environment and executable hashes: `target/rfc024-native-metadata-padding-20261006/screen-environment.json`, input hashes: `target/rfc024-native-metadata-padding-20261006/candidate-input-hashes.json`, and patch scope: `target/rfc024-native-metadata-padding-20261006/patch-scope.json`.
- Independent audit: `target/rfc024-native-metadata-padding-20261006/verification.json` and analysis driver: `target/rfc024-native-metadata-padding-20261006/analysis.py`.
- Upstream tests: `target/rfc024-native-metadata-padding-20261006/skiplist-tests.log`, all-feature engine tests: `target/rfc024-native-metadata-padding-20261006/engine-tests-all-features.log`, and default-feature engine tests: `target/rfc024-native-metadata-padding-20261006/engine-tests-default.log`.
- Default-feature Clippy: `target/rfc024-native-metadata-padding-20261006/clippy-default.log`, all-feature Clippy: `target/rfc024-native-metadata-padding-20261006/clippy-all-features.log`, and probe recovery tests: `target/rfc024-native-metadata-padding-20261006/probe-tests.log`.

The dependency candidate is rejected. Existing pooling rejections and the
full RFC gate's **UNQUALIFIED** status remain unchanged.
