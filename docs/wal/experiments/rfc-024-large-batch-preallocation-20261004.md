# RFC 024: extent preparation for large batches

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

Retain the large-batch extent-preparation change. The frozen confirmation
screen passed: ext4 batch64 at 16 writers improved paired median throughput
by **18.3%**, reduced p99 by **18.5%**, and reduced CPU per put by **16.6%**.
All three target blocks passed the repeatability controls. This is an
optimization of the existing parallel backend; it does not establish the
full RFC adoption gate or a qualified gain over leader.

## Change

The ext4 initializer previously zero-filled every allocated extent before
the WAL worker wrote its batches there. The retained optimization from
September helped small writes, but also wrote almost another WAL's worth of
data for the large-batch case. An allocation-only diagnostic first showed
a +19.2% paired throughput median; only one of its three blocks passed repeat
controls, so the targeted candidate received a separate confirmation.

For a group containing only batches with aligned encoded lengths of at least
64 KiB, extent preparation now requests allocation without zero-fill. Groups
containing smaller batches request zero-fill. The first lookahead extent
still uses zero-fill. The threshold is a conservative policy tested against
the 64-entry, 1 KiB-value workload, whose WAL writes are 68 KiB; it is not a
measured optimum for every batch size or filesystem.

The decision applies to newly requested ranges. An outstanding lookahead
retains its captured policy, so a workload transition can lag by an extent.
Allocation-only ranges advance the initializer's ready offset exactly as
zero-filled ranges do. Later small writes must never trigger retroactive
zero-fill over a prefix that may already contain submitted WAL data.

Allocation remains outside the producer queue mutex, bounded by the file
cap and owned by the existing initializer thread. Multiple groups still use
the existing io_uring worker and captured-prefix durability coordinator.
The WAL format, poison boundary, recovery contract, public mode selection,
and PITR path are unchanged. Other filesystems retain allocation only.

## Confirmation

Release builds with `bench`, native io_uring outside the sandbox, fresh
databases on the existing NVMe-backed ext4 filesystem, 1 KiB values, PITR
off, and a 1 GiB SST target. The unchanged baseline executable has SHA-256
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`;
candidate `9d9091390f8ba7fdff6f2aaf5c0846a6701b647c6e94c5c571e50d4594690031`.

There are 54 scored runs plus nine fixed warmups. Each case has three balanced
blocks, two observations per arm per block, with rotating case order and five
seconds idle after every run. At 16 writers each block also has two leader
reference runs on the baseline binary. Binaries and outputs remain in RAM
during timing. No builds, tests, profiling, or device administration overlap
scored runs. Every scored observation is retained.

| Case | Puts/run | Baseline puts/s | Candidate puts/s | Paired throughput change | Paired p99 change | Paired CPU/put change | Repeat controls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch64, 16 writers | 262,144 | 457,231 | 542,174 | +18.3% | -18.5% | -16.6% | 3/3 |
| Batch64, 8 writers | 262,144 | 338,563 | 354,570 | +4.4% | -11.4% | -8.0% | 0/3 |
| Batch64, 1 writer | 65,536 | 82,999 | 80,092 | -3.2% | -21.6% | 0.0% | 1/3 |
| Single puts, 16 writers | 50,000 | 23,728 | 23,900 | 0.0% | +0.6% | -1.5% | 1/3 |

Absolute columns are medians of six scored runs per arm. Changes are medians
of three block-level geometric-mean candidate/baseline ratios. A control
passes when both arms' within-block throughput spread is at most 5% and p99
spread at most 10%. Failed guard controls limit interpretation of their small
changes; they do not qualify additional performance gains or equivalence.

The predeclared retention rule required at least +10% target throughput,
improvement in all three target blocks, at least two passing target control
blocks, and no guard median worse than -5% throughput or +10% p99. It passed.
Target throughput ratios were 1.173, 1.183, and 1.267. Candidate throughput
spread across all six target runs was 1.7%; baseline spread was 9.5%.

The leader reference median was 549,095 puts/s, p99 3.075 ms, versus candidate
542,174 puts/s and 3.175 ms. These descriptive medians are close; leader's
session throughput spread was 11.2%. They establish neither a qualified
leader-parity result nor the RFC's +10% leader-relative adoption requirement.

Both parallel arms completed exactly 4,096 write CQEs per target run. Baseline
peak in-flight groups were 16; candidate peaks were 13–15. These are software
counters. Median sync counts were 642 versus 652.5, and aggregate fdatasync
time 262.6 versus 264.5 ms. The gain therefore did not come from fewer syncs
or greater peak pipeline depth; removal of zero-fill is the isolated change.

## Validation and artifacts

`cargo make check` passed outside the sandbox: default and all-feature
Clippy, formatting, dependency checks, typos, and all 1,421 tests with no
skips. New tests prove that allocation-only requests preserve marked bytes,
that resuming zero-fill never overwrites the skipped prefix, and that a
group containing a small batch retains the zero-fill policy. Existing
initialization failure, shutdown, file-cap, ownership, rotation, crash,
and recovery tests also pass.

Artifacts: `target/rfc024-batch-preallocation-20261004/` for the diagnostic
and checks, and `target/rfc024-batch-preallocation-confirm-20261004/` for the
candidate confirmation. They retain binaries, source manifests, drivers,
frozen protocols, raw outputs, per-run records, and block summaries. All 63
confirmation raw records, rates, CQE counts, mode settings, source/binary
hashes, and multi-group depth were checked. Disposable database trees and
RAM staging directories were removed.
