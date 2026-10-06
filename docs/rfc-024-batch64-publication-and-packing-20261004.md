# RFC 024: batch64 publication and packing follow-up, 2026-10-04

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

No additional production change is retained. Three isolated candidates targeted
publication ownership, publication spinning, and packing under the MVCC ordering
mutex. None passed the frozen incremental retention rule. Existing optimizations,
including the previously confirmed large-batch extent preparation, remain intact.
This round establishes no new leader-relative batch win or RFC adoption result.

## Protocol and decisions

All three experiments used the same starting Rust sources at `ca4cfcca`, native
io_uring outside the sandbox, NVMe-backed ext4, PITR off, release builds with
`bench`, 16 writers, batch64, 1 KiB values, 262,144 logical puts, and a 1 GiB SST
target. Latency is per batch; throughput counts logical puts.

Each experiment had three warmups per arm and three fixed six-run primary blocks
with two observations each of retained parallel, candidate parallel, and unchanged
leader WAL. Block orders were B/C/L/L/C/B, C/L/B/B/L/C, and L/B/C/C/B/L. B and L
used the same saved executable. Executables ran from RAM; output was captured in
memory during execution, and five seconds idle followed each run. Builds, tests,
profiles, and device administration did not overlap scored timing.

| Candidate | Throughput vs retained parallel | Batch p99 | CPU per logical put | Passing target controls | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Transfer already-owned entries after durability | -2.2% | -1.4% | +9.3% | 0/3 | Revert |
| Cap large-batch publication spinning at 256 iterations | +0.2% | +2.0% | +3.5% | 1/3 | Revert |
| Move large pooled-buffer packing to the durability wait | +2.2% | -2.4% | -7.7% | 1/3 | Revert |

Changes are medians of three block-level geometric-mean ratios. Repeat controls
require each comparison arm's within-block throughput spread at most 5% and p99
spread at most 10%. Retention requires at least +5% target paired median throughput,
every original target block positive, at least two passing target controls, and
independent guards no worse than -5% throughput or +10% p99. None qualified for
guard testing. This rule is separate from the RFC's full adoption gate.

The ownership candidate's throughput changes were -1.7%, -4.2%, and -2.2%.
The spin candidate's were -1.2%, +0.2%, and +19.7%; the last block contained
baseline storage stalls. The packing candidate's were -0.3%, +7.0%, and +2.2%.
These observations do not justify retaining a candidate on its best run.

Against leader, the ownership candidate's paired median was -9.1% throughput and
+5.4% p99, with one passing comparison control. The spin candidate's apparent
+17.5% throughput was contaminated by leader stalls and had zero passing controls.
The packing candidate was approximately tied in throughput (+0.03%) with +7.7%
p99 and zero passing controls. No leader-relative gain is established.

## What changed in each rejected candidate

The ownership experiment kept batch preparation unchanged and transferred the
already-owned deferred publication entries into the existing owned memtable
publication path after WAL durability. It applied only to actual parallel-WAL
batches with at least 64 entries. Small writes, leader WAL, PITR, WAL encoding,
and synchronization policy kept their existing behavior.

The spin experiment limited publication waiting to 256 spin iterations for those
large parallel batches. It preserved ready registration, ordered prefix
advancement, poison handling, timestamp retirement, and condition-variable waits.
It did not include the rejected ownership change.

The packing experiment deferred packing records of at least 64 KiB held in the
existing 256 KiB pooled buffers until their durability waiter, after the caller
released the MVCC ordering mutex. The eighth queued record forced dispatch.
Before blocking for buffer budget, a reservation dispatched pending work; while
any budget reservation waited, admission dispatched immediately. Packing never
held the durability mutex. Sync and close drained pending records through the
existing packer. It added no worker thread or batching deadline and retained the
existing 400 microsecond sync coalescing policy.

This handoff reduced median sync calls from 661.5 to 622, but median I/O groups
remained 4,096 versus 4,095.5. Moving packing did not materially combine records
into larger I/O groups in this workload. Its CPU reduction alone does not prove
the required end-to-end throughput gain.

## One-second retries and storage stalls

A primary observation triggered one extra same-arm retry after a one-second pause
if p99 exceeded 6 ms, or, after two previous primary observations of that arm,
throughput fell more than 10% or p99 rose more than 20% relative to their median.
A candidate comparison worse than -10% throughput or +20% p99 would trigger one
whole-block retry with rotated arm order. Retries never replaced originals.

Only the spin experiment triggered retries: leader fell to 24,395 puts/s with
70.893 ms p99; its retry reached 55,549 puts/s with 70.286 ms p99. Later baseline
and leader p99 reached 19.280 and 16.512 ms; their retries remained elevated at
16.457 and 18.556 ms. The pauses did not reliably clear these stalls. There were
no full-block retries. All three experiments retain 84 observations: 27 warmups,
54 primary scored runs, and three single retries.

## CPU diagnosis and a build without `bench`

The first short CPU capture lost 3,356 samples, so its percentages are unsuitable
for quantitative attribution. A larger perf mapping exceeded the host's memory
allowance. Subsequent captures used 16 mmap pages, 99 Hz userspace cycle sampling,
and 1,024-byte DWARF stacks. Parallel and leader captured 303 and 316 samples,
respectively, with zero lost samples. These short process captures point to
memtable publication, skip-list searches, MVCC publication waiting, and clock
reads; they are diagnostic observations, not a precise decomposition of the timed
write window or paired CPU savings.

Inspection found that compiling with `bench` enables per-entry publication timing
even without `--profile`. The flag controls reporting and detailed sync
observations, but does not remove those phase clock calls.

A separate diagnostic therefore built the unchanged retained sources with the
default features, without `bench`. It used the same batch64/16-writer target,
three warmups per mode, and three fixed four-run P/L/L/P, L/P/P/L, P/L/L/P blocks.
Its estimator and repeat controls were unchanged. No retry triggered.

| Diagnostic | Parallel | Leader |
| --- | ---: | ---: |
| Median logical puts/s across six primary observations | 519,402 | 546,492 |
| Median batch p99 | 3.304 ms | 3.177 ms |
| Median process CPU per logical put | 9.747 microseconds | 8.532 microseconds |

The block-paired median was -3.5% throughput, +4.1% p99, and +16.4% CPU per put for
parallel, with zero of three passing repeat controls. These values do not prove a
stable regression, but removing instrumentation alone did not establish a batch
win. This build lacks software pipeline counters and detailed sync observations,
so it cannot qualify simultaneous in-flight groups or write/sync overlap. Its 18
observations are separate from the optimization screens.

## Validation and restoration

Each final candidate passed `cargo make check` outside the sandbox, including
formatting, dependency checks, default and all-feature Clippy, typos, and nextest:
1,422 tests for ownership, 1,421 for spinning, and 1,424 for packing, all with no
skips. Ownership coverage included caller-buffer mutation, snapshots, mixed
operations, and recovery. Packing coverage included deferred sync/close recovery,
oversized-allocation progress, and admission racing a budget reservation.
Earlier compile and test-fixture failures remain archived separately from the
successful final check logs.

All raw optimization observations were checked against their saved stdout,
operation count, mode, reported rate, batch sample count, completed write CQEs,
commit buffers, and executable checksums. All 118 Rust files were restored to the
starting source manifest. The normal release executable was rebuilt with `bench`
and compared with the retained baseline. Disposable databases and RAM staging
directories were removed.

Retained benchmark executable SHA-256:
`72891a06445b1e08a5ec187b4cbed8c41cbf0377c80b0e2aa5fc4748c40b0eb3`.

Binaries, rejected patches, source manifests, fixed protocols, raw outputs,
profiles, summaries, and check logs remain under:

- `target/rfc024-batch64-followup-ca4cfcca-20261004/`
- `target/rfc024-bulk-publication-spin-ca4cfcca-20261004/`
- `target/rfc024-large-batch-packing-handoff-ca4cfcca-20261004/`
- `target/rfc024-batch64-production-diagnostic-ca4cfcca-20261004/`
