# RFC 024: batch allocation follow-up, 2026-10-04

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

No additional production change is retained. Four experiments targeted the
remaining batch cost after commit `422d6616`. The most promising candidate
pooled publication copies, but its separate confirmation failed the frozen
retention rule. The [previously validated extent-preparation improvement](rfc-024-large-batch-preallocation-20261004.md)
remains intact. This session establishes neither a new 16-writer throughput
gain nor the full RFC adoption gate.

## Experiments

Native io_uring outside the sandbox, fresh databases on the existing NVMe-backed
ext4 filesystem, PITR off, release builds with `bench`, batch64, 1 KiB values,
16 writers, 262,144 puts, and a 1 GiB SST target. Each screen has three fixed
warmups, three balanced ABBA/BAAB/ABBA blocks with two runs per arm per block,
and two leader reference runs. Executables and output remain in RAM during
measurement; five seconds idle follows every run. No builds, tests, profiling,
or device administration overlap scored timing. Every observation is retained.

| Candidate | Paired median throughput | Paired median p99 | Passing repeat controls | Decision |
| --- | ---: | ---: | ---: | --- |
| Owned publication with shared buffers prepared early | -6.4% | +2.1% | 0/3 | Revert |
| Nonblocking completion drain after coalescing | +3.3% | -7.0% | 1/3 | Revert |
| Consume prepared entries without an intermediate value clone | -3.6% | -2.8% | 0/3 | Revert |
| Pool publication copies into a bounded shared buffer | +7.5% | -10.9% | 0/3 | Separate confirmation; then revert |

Changes are medians of three block-level geometric-mean candidate/baseline
ratios. A repeat control requires both arms' within-block throughput spread
at most 5% and p99 spread at most 10%. Failed controls prevent qualifying
these screening gains. The first ownership screen also encountered a latency
cliff in all three warmups, including the unchanged baseline and leader.

## Cost placement

Separate diagnostic profiles suggest that removing a publication copy can
move allocation cost to preparation before WAL admission. They are aggregate
per-thread wall times, not elapsed time saved or controlled CPU comparisons.

| Diagnostic profile, 262,144 puts | Batch preparation | Publication copy | Entire memtable publication | Sync calls |
| --- | ---: | ---: | ---: | ---: |
| Unchanged parallel baseline | 91 ms | 914 ms | 1,621 ms | 643 |
| Shared buffers and owned publication | 1,433 ms | 14 ms | 832 ms | 692 |
| Consuming prepared entries | 1,208 ms | 13 ms | 836 ms | 666 |
| Pooled publication copies | 89 ms | 306 ms | 1,129 ms | 615 |

Pooling retained the existing preparation stage and copied publication keys
and values into one shared allocation, then sliced it for skip-list insertion.
It was restricted to parallel WAL batches with at least 64 entries and at most
128 KiB of encoded keys and values. Larger batches used the existing path.
A returned value could retain the entire shared allocation; this bounded
memory-retention tradeoff would need review if the approach is revisited.

The experiments preserved WAL ordering, captured-prefix synchronization,
poison semantics, and the PITR path. None extended the coalescing cutoff or
its deadline. Pooling changed memtable ownership only after WAL durability.

## Independent confirmation

The final pooled candidate received a frozen four-case confirmation: 54 scored
runs plus nine warmups, three balanced blocks with rotating case order, two
observations per arm per block, and two leader reference runs per target block.
The incremental retention rule, declared before confirmation, required at least
+5% target paired median throughput, all three target blocks positive, at least
two passing target repeat controls, and no guard median worse than -5%
throughput or +10% p99. This is separate from RFC adoption. It failed.

| Case | Baseline puts/s | Candidate puts/s | Paired throughput | Paired p99 | Paired process CPU/put | Repeat controls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Batch64, 16 writers | 517,092 | 536,465 | +2.8% | -3.4% | -23.1% | 0/3 |
| Batch64, 8 writers | 359,695 | 374,865 | +3.9% | -5.4% | -14.3% | 2/3 |
| Batch64, 1 writer | 80,136 | 84,477 | +5.8% | -2.4% | -11.1% | 3/3 |
| Single puts, 16 writers | 23,724 | 24,072 | 0.0% | -4.0% | -0.5% | 1/3 |

Absolute columns use six-run medians; paired changes use all three block
ratios. Process CPU covers the concurrent-write interval, including writer
startup and any worker or background activity during that interval, and
is normalized by logical puts. It excludes engine setup and post-timing
flush/close work. Lower-concurrency results do not replace the predeclared target.

The first target block stalled in every arm. Baseline measured 95,136 and
371,461 puts/s; candidate 94,534 and 37,682; leader 34,178 and 24,212.
The target block throughput ratios were **0.317, 1.078, 1.028**, all included.
The recovered blocks also failed repeat controls. These observations do not
identify the cause of the existing storage stalls or establish leader parity.

## Validation and restoration

The final candidate passed `cargo make check` outside the sandbox: formatting,
default and all-feature Clippy, dependencies, typos, and **1,423 tests with no
skips**. Temporary tests covered the 128 KiB limit, WAL preparation without memtable
publication, lookup/bloom correctness, size accounting, and value ownership
after the original buffers and memtable were dropped. They are archived with
the rejected patch and are not left in production sources.

Every confirmation raw record, reported rate, completed write CQE count,
mode setting, source manifest, and database cleanup was checked. All Rust
sources were restored to the baseline manifest and the normal release
executable was rebuilt and checked against the saved baseline.

Baseline executable SHA-256:
`9d9091390f8ba7fdff6f2aaf5c0846a6701b647c6e94c5c571e50d4594690031`.
Final pooled candidate:
`8a42c086d85859f7c2e5f9f2a37c190988565b04adc34e2a4125bc93e421f413`.

Artifacts retain the binaries, drivers, frozen protocols, source snapshots,
raw outputs, summaries, profiles, and rejected patches under
`target/rfc024-batch-next-profile-20261004/`,
`target/rfc024-batch-owned-screen-20261004/`,
`target/rfc024-batch-ready-drain-screen-20261004/`,
`target/rfc024-batch-consume-screen-20261004/`,
`target/rfc024-batch-packed-screen-20261004/`, and
`target/rfc024-batch-packed-confirm-20261004/`.
Disposable databases and RAM staging directories were removed.
