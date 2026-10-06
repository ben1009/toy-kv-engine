# RFC 024: native async batch pooling follow-up

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Two bounded pooling candidates were tested after the
[native batch ownership investigation](rfc-024-native-batch-ownership-followup-20261005.md).
Neither meets its frozen sixteen-writer retention rule. Both are reverted;
the existing parallel default and native async implementation remain intact.
Allocation count falls substantially, but sixteen-writer preparation and
skip-list insertion grow enough to offset the publication-copy savings.

The sessions began on October 5 and finished on October 6 local time. This
is an incremental native-parallel comparison, not a new leader comparison
or qualification of the full RFC adoption gate. Separate leader observations
remain descriptive references.

## Candidates and ownership bounds

- **A, input pooling:** For native point batches of at least 64 records, pack
  user keys and prefixed values into one owned allocation with at most
  128 KiB requested capacity. Deduplicate before packing, preserving the
  last occurrence of each key. Fall back to the existing builder for smaller
  or oversized inputs. Publication transfers the owned entries after WAL
  durability, avoiding another value copy.
- **B, input and internal-key pooling:** Add a separate allocation for encoded
  internal keys, also bounded at 128 KiB, during native MVCC preparation.
  The commit timestamp and existing key encoding are preserved. Oversized
  encoded-key sets use the existing path.

A returned value or iterator key can keep its entire shared allocation
alive. These requested-capacity limits bound that consequence for each pool;
they do not bound total engine memory. The experiments preserve synchronous
API behavior, WAL framing, durability acknowledgement, publication order,
rotation, cancellation ownership, and PITR handling.

## Allocation diagnosis

A separate executable wraps Rust's `System` allocator and counts successful
allocation calls only during the writer window. Reallocation calls are
recorded separately. Requested bytes include the new size of each successful
reallocation and are not live or peak memory. DirectBuf's direct libc
allocations are outside these counters. Atomics affect execution, so **none
of these instrumented timings enters the performance estimates**.

Each arm runs 262,144 puts, batch64, 1 KiB values, at 8 and 16 writers. The
same two baseline diagnostic runs are reused for A and B: six diagnostic
runs in total.

| Writers | Baseline allocations/put | A allocations/put | B allocations/put | A reduction | B reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 | 10.313 | 4.333 | 2.364 | 58.0% | 77.1% |
| 16 | 10.317 | 4.332 | 2.362 | 58.0% | 77.1% |

At sixteen writers, requested bytes per put fall from 2,988 to 1,805 for A
and 1,772 for B. These counts confirm that pooling removes allocations;
they do not establish a throughput gain.

## Uninstrumented ext4 screen

Both screens use native `write_batch_async()`, parallel v4 WAL, eight Tokio
workers, PITR and serializable mode off, NoCompaction, and a 1 GiB memtable
target. Each scored run has 524,288 puts, batch64 and 1 KiB values, without
timed rotation. WALs reside on ext4 at `/dev/nvme0n1p3`. Executables are staged
in RAM; fresh databases are removed after each run.

Each case has two warmups per arm and three fixed ABBA/BAAB/ABBA blocks,
with two observations per arm per block. Writer-count order rotates. There
is one identical-baseline pair per case and two separate leader references
per case. Each run is followed by five seconds idle. No compilation, test,
tracing, or device-administration work overlaps timed runs.

The frozen retention rule requires at least **+5% sixteen-writer paired
median throughput**, all three target blocks positive, and at least two
passing target repeat controls. Guard cases must stay within -5% throughput
and +10% p99, with independent confirmation before retention. A repeat
control requires both repeats of each arm within 5% throughput and 10% p99;
identical-baseline pairs use the same limits.

An obvious anomaly triggers an extra one-second pause and one separately
recorded retry. A block below 0.90 throughput ratio or above 1.20 p99 ratio
triggers a reversed retry block. **All originals remain primary; retries
are excluded from scoring.** Failed controls are not waived.

The changes below are medians of the three block geometric-mean
candidate/baseline ratios. Positive p99 or CPU change means worse.

| Candidate | Writers | Throughput | Commit p99 | Process CPU/put | Passing blocks | Baseline/baseline control | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| A: input pooling | 8 | +10.6% | -4.3% | -25.8% | 2/3 | Passed | Revert |
| A: input pooling | 16 | -3.9% | -3.6% | +14.0% | 2/3 | Failed | Revert |
| B: input and internal-key pooling | 8 | +8.7% | -2.9% | -19.0% | 1/3 | Passed | Revert |
| B: input and internal-key pooling | 16 | -3.0% | -2.7% | +9.8% | 1/3 | Passed | Revert |

The sixteen-writer block throughput ratios are `0.9614 / 0.9424 / 0.9785`
for A and `0.9696 / 1.0148 / 0.9549` for B. Both miss the target even before
independent confirmation, which is therefore not run. The eight-writer
changes do not replace the frozen target or qualify a guard-only gain.

Both screens contain an eight-writer run with a large slowdown, followed
by separately recorded retries. Across both screens there are **91
observations**: 48 primary, 16 warmups, 8 null-control runs, 8 leader
references, 3 single retries, and 8 block-retry runs. No retry replaces an
original observation or becomes a new scored block.

## Where the savings went

These values are medians of aggregate phase wall time divided by commit
count, in microseconds per batch64 commit. They are not CPU samples, and
publication includes its copy and skip-list phases; do not sum that total
with its subphases.

| Sixteen-writer path | Batch preparation | MVCC/WAL preparation | Memtable publication | Publication copy | Skip-list insertion |
| --- | ---: | ---: | ---: | ---: | ---: |
| A baseline | 34.6 | 31.0 | 152.9 | 64.1 | 59.3 |
| A candidate | 67.7 | 49.3 | 175.8 | 3.0 | 119.7 |
| B baseline | 34.7 | 30.7 | 154.5 | 64.4 | 58.4 |
| B candidate | 66.3 | 39.5 | 180.8 | 2.9 | 122.0 |

The copy phase nearly disappears, but preparation and insertion grow.
Adding the encoded-key pool reduces allocation count further without
recovering the sixteen-writer throughput. Sync counts stay close: A has
1,215 versus 1,211.5 median sync calls; B has 1,201.5 versus 1,197. Aggregate
fdatasync wall time per commit changes from 77.7 to 75.6 microseconds for A
and 79.4 to 77.7 for B. These observations do not point to extra sync calls
as the explanation for this particular regression.

The measured regression is in preparation and insertion. Pooling also
changes reference-count operations, allocation lifetimes and memory layout,
but these phase timers do not identify which causes the slowdown. Further
work should sample those paths within the timed window before choosing
another ownership or allocation change. Fewer allocation calls alone are
insufficient evidence to retain an optimization.

The subsequent [root-cause investigation](rfc-024-native-batch-pooling-cause-20261006.md)
identifies shared skip-list metadata contention with private cache-line
interventions. It also samples the first-touch work moving from publication
into preparation. The experiments do not change this screen's rejection or
establish a retained end-to-end improvement.

## Verification and restoration

A passes **31 native async tests**; B passes **32**, including bounded-pool,
last-write deduplication, TTL/delete, caller-buffer mutation, retained-value,
flush/recovery, cancellation/drain, and encoded-key lifetime coverage. The
experimental tests and implementations remain in archived source snapshots.

All **91 screen observations** reconcile with raw JSON, mode, worker count,
sampled commits, completed write CQEs, and throughput. All 12 scored blocks
and four identical-baseline pairs are recomputed. Both drivers confirm
source, executable and driver integrity at completion. Owned test databases
and RAM stages are removed.

Only the three experimental source files are restored. All **114 baseline
input hashes** match, preserving the prior default and documentation changes.
Source timestamps are refreshed before rebuilding. The fresh rebuilt
executable matches the baseline SHA-256:
`95460bcf994a1f9d515b08af80374b18570a55a0945cf4665f26a2572bac6c4d`.
The restored code passes **27 native async tests**. Formatting and spelling
checks cover the resulting documentation changes.

- **A:** protocol: `target/rfc024-native-batch-pool-20261005/screen-protocol.json`, observations: `target/rfc024-native-batch-pool-20261005/screen-records.json`, summary: `target/rfc024-native-batch-pool-20261005/screen-summary.json`, allocation counts: `target/rfc024-native-batch-pool-20261005/allocation-summary.json`, verification: `target/rfc024-native-batch-pool-20261005/verification.json`, and rejected patch: `target/rfc024-native-batch-pool-20261005/candidate.patch`.
- **B:** protocol: `target/rfc024-native-batch-pool-keys-20261005/screen-protocol.json`, observations: `target/rfc024-native-batch-pool-keys-20261005/screen-records.json`, summary: `target/rfc024-native-batch-pool-keys-20261005/screen-summary.json`, allocation counts: `target/rfc024-native-batch-pool-keys-20261005/allocation-summary.json`, verification: `target/rfc024-native-batch-pool-keys-20261005/verification.json`, and rejected patch: `target/rfc024-native-batch-pool-keys-20261005/candidate.patch`.
- Restoration and fresh executable verification: `target/rfc024-native-batch-pool-keys-20261005/restoration.json`.

The protocols, observations, binaries, diagnostic sources, test logs and
rejected patches are retained locally under ignored `target/` directories.
