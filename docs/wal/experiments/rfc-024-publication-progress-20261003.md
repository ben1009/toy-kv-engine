# RFC 024: Progress-aware publication waiting

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

**Decision:** Reject the prototype and restore the retained runtime. The
32-writer comparison records 22.1% lower CPU per put, but only one of three
blocks passes the required repeat controls. The rotation p99 guard also fails.
Parallel WAL remains opt-in; the
[full qualification gate](../benchmarks/rfc-024-wal-qualification-20261001.md) remains unmet.

## Hypothesis and validation

The earlier [publication profile](rfc-024-publication-yield-20261002.md)
concentrates sampled user cycles in frontier loads, comparisons, and pause
instructions. Its MVCC source matches this experiment's baseline byte for byte.
These samples describe CPU work, rather than wall-time or device attribution.

Test a progress-aware spin: after each 256 iterations, park if the mirrored
publication frontier has not advanced. If it advances, continue within the
existing total bound of 4,096 iterations. The contention/distance hint still
selects a 256-iteration bound. Progress never resets either total bound.

Ready-prefix registration, acquire/release ordering, the publication mutex,
poison predicates, live reservation counting, barrier draining, WAL writes,
and data-sync policy remain unchanged. Only the scheduling of publication
waiters changes. In-order publication does not enter the modified wait.

Before timing, default-feature and all-feature Clippy pass with `-D warnings`,
and all 258 selected parallel-WAL, MVCC, and transaction tests pass outside
the io_uring sandbox without retries. A new concurrency test waits until a
follower parks behind a missing predecessor, then poisons its timestamp. It
checks that the follower wakes with an error, does not advance visibility,
and retains the failed reservation. The test also passes in a separate run.

## Frozen comparison

Compare release builds with `bench` against `4117189c`, whose runtime matches
the preceding retained baseline. Use Linux 6.18.9-arch1-2, the existing SSD's
ext4 filesystem, and tmpfs under `/dev/shm`. Both arms use parallel WAL, PITR
off, 1 KiB values, and fresh paths. The leader row guards the shared MVCC
change; it is not a parallel-versus-leader comparison.

Each case has three ABBA/BAAB blocks, with rotated case order, two scored
runs per arm per block, and one fixed warmup per arm: 156 scored runs plus
26 warmups. All runs complete in 938 seconds. The SST target is 1 GiB except
for rotation's 1 MiB target. Sample every tenth single put or every batch
commit. Binaries and raw outputs stay in RAM during timing; controller progress
is logged between runs. No builds, tests, tracing, or administrative polling
overlap scored runs.

Repeat controls allow at most 5% within-arm throughput spread and 10% p99
spread. Include every block, failed control, and stall. Retention first
requires an ext4 16/32/64-writer case to improve median throughput or CPU
per put by at least 5%, improve that metric in all three blocks, and pass at
least two control blocks. Every case must stay within 5% median throughput
loss and 10% median p99 regression. Concurrent candidate runs must reach at
least two in-flight groups.

Only a passing initial screen would trigger one predefined confirmation at
16/32/64 writers, batch64 with eight writers, tmpfs solo/rotation, and the
shared-MVCC guard. The initial screen fails, so that confirmation is not run.
There is no timing extension or confidence-interval qualification claim.

| Case | Puts | Throughput change | p99 change | CPU/put change | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4, single puts, 4 writers | 100k | +0.3% | -1.1% | -2.0% | 2/3 |
| Ext4, single puts, 8 writers | 100k | +0.1% | -0.4% | -0.4% | 2/3 |
| Ext4, single puts, 16 writers | 200k | -0.8% | -1.3% | -4.9% | 2/3 |
| Ext4, single puts, 32 writers | 200k | +0.5% | +4.2% | -22.1% | 1/3 |
| Ext4, single puts, 64 writers | 200k | -1.2% | +2.6% | +0.5% | 2/3 |
| Ext4, rotation, 4 writers | 20k | -1.3% | +17.4% | +1.9% | 0/3 |
| Ext4, single puts, 1 writer | 10k | +0.4% | +4.4% | -2.3% | 1/3 |
| Ext4, batch64, 8 writers | 524,288 | +1.6% | -36.4% | -0.2% | 0/3 |
| Tmpfs, single puts, 1 writer | 50k | +0.7% | -11.1% | -1.8% | 0/3 |
| Tmpfs, rotation, 4 writers | 200k | -1.7% | +3.5% | +2.4% | 1/3 |
| Ext4, leader guard, 32 writers | 200k | -0.2% | +1.2% | -23.6% | 3/3 |
| Ext4, batch64, 1 writer, exact case | 65,536 | +1.1% | +2.7% | -1.0% | 0/3 |
| Ext4, batch64, 1 writer, longer run | 262,144 | +9.7% | -33.6% | -1.0% | 0/3 |

Changes are medians of the three block-level geometric-mean
candidate/baseline ratios. At 32 writers, CPU/put ratios are 0.847, 0.779,
and 0.767. The second block passes both repeat controls; the first has a
candidate p99 swing from 1.98 to 39.96 ms, and the third fails the baseline
p99 spread check. Their throughput ratios are 0.434, 1.033, and 1.005; p99
ratios are 4.397, 1.021, and 1.042. All remain included.

The 16-writer CPU reduction is 4.9385%, below the frozen 5% threshold. At
64 writers CPU is near parity. The rotation p99 ratios are 0.908, 1.174,
and 1.743, with no passing controls; they fail the median guard without
establishing how much is caused by this scheduling change. The favorable
batch-tail medians also have no passing controls. One-writer comparisons
do not enter the changed wait and cannot validate its CPU mechanism.

Both arms reach peaks of 16 groups/write SQEs at 16 writers and 32 at
32/64 writers. At 32 writers, median sync counts are 11,671.5 baseline
and 11,560 candidate. Software depth is preserved; device queue depth was
not measured. The lower CPU observations do not establish a device-backed
throughput gain or resolve the full RFC gate.

## Artifacts and restoration

`target/rfc024-publication-progress-20261003-nkczwpjo/` preserves source
snapshots, both binaries, the rejected patch and test, profile provenance,
build/test/Clippy logs, the frozen protocol and driver, all 182 run records,
39 block records, raw outputs, pipeline summaries, and an independent
schedule/metadata/ratio/control/depth verifier. Disposable databases and
RAM copies are removed.

Protocol SHA-256:
`bc44cf308bcaa89d69a4359fac75c7e1922b2fafea3139e1a6b54d25e6ea6006`.
Baseline binary SHA-256:
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.
Prototype binary SHA-256:
`9c5de8a626f19b66e92d575e92b87e19ff3094d9665e94fa41a75e5182726f2a`.
Artifact manifest SHA-256 (395 files verified):
`855854618001f66ad6137b32e27ae379f7e42ad64e62409abd374ca9d6fc8899`.

The MVCC source was restored byte for byte, and the rebuilt release binary
matches the saved baseline. This follow-up retains documentation only.
