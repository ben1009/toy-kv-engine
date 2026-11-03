# RFC 026: paired V7/V8 captures with storage-state probes

Both full comparison blocks are rejected because storage state changes
inside their measurement windows. The new runner preserves their CPU and
device measurements but produces **no V8/V7 ratios and no qualified score**.
This round changes measurement tooling and documentation only; no runtime,
digest, grouping, sync, affinity default or device feature changes.

The [method](paired-v7-v8-method.md), [runner](paired_v7_v8.py) and
[classification tests](test_paired_v7_v8.py) separate CPU attribution,
completion-pattern-controlled runtime work and real-device qualification.
The last two are distinct: matching endpoint probes is not a replay of the
same I/O completion schedule. Such a replay is not implemented in this slice.

## Fixed controls

Every full capture has 750,000 puts, sixteen writers, batch64, 1 KiB values,
Parallel WAL, no compaction, a 2 GiB SST target and latency sampling every
ten batches. Both formats use the same preserved instrumented executable,
SHA256 `fbb90ea8a6f20bf476760e79ae60e04cda3edae04138d45ae5a9e6346da299f1`.
The build and instrumentation are described in the
[producer attribution report](rfc-026-v8-producer-attribution-20261102.md).

All workload child threads inherit CPU mask `0-15` on the same boot, Linux
6.12.71-1-lts. This run records the mask, rather than collecting placement
samples or proving residence on particular cores. A probe on CPU 8 precedes
and follows every workload; adjacent workloads share a boundary probe.
The ten probes write 40 MiB through the existing initialized 8 MiB scratch
inode on ext4 `/dev/nvme0n1p3`. No raw-device writes or admin queries occur.
Benchmark output and phase JSON remain in RAM until each whole block ends.

The policy is fixed before measurement: first-post-fdatasync median below
100 µs is fast, 100 to below 500 µs intermediate, and at least 500 µs slow.
Quartile drift, endpoint latency/tail drift and identity mismatches reject
the entire block. The [plan](artifacts/rfc-026-v8-state-paired-20261103-plan.json)
records all tolerances. A short 4,096-put smoke verifies commands, phase
output and digest assertions before the two full blocks; it is not a score.

## Device observations

Each table entry lists the probe's median first write after fdatasync.
All eight workload results are preserved, including those within a rejected
block that pass the previous per-run storage filter.

| Block 0 order | Puts/s | p99, ms | CPU, s | Syncs | Probe before → after, µs |
|---|---:|---:|---:|---:|---|
| V7 | 169,527 | 47.739 | 4.429 | 1,365 | 640.596 → 28.915 |
| V8 | 698,076 | 3.473 | 5.030 | 888 | 28.915 → 28.988 |
| V8 | 731,271 | 3.036 | 5.010 | 847 | 28.988 → 28.954 |
| V7 | 619,953 | 3.071 | 3.758 | 1,372 | 28.954 → 28.919 |

The first probe has 32 writes over 5 ms; the remaining four have none. The
same V7 invocation changes from 170k to 620k puts/s, while the probes show a
slow-to-fast transition. The two fast V8 captures are observations from this
rejected block, not a new best-score or parity claim. Selecting a favorable
subset after seeing the transition would violate the declared block rule.

| Block 1 order | Puts/s | p99, ms | CPU, s | Syncs | Probe before → after, µs |
|---|---:|---:|---:|---:|---|
| V8 | 576,779 | 9.936 | 5.205 | 924 | 28.873 → 640.401 |
| V7 | 116,387 | 26.883 | 8.000 | 1,519 | 640.401 → 640.170 |
| V7 | 112,517 | 27.224 | 7.949 | 1,519 | 640.170 → 640.439 |
| V8 | 103,946 | 27.392 | 9.570 | 1,488 | 640.439 → 640.391 |

This block changes back from fast to slow. The probes have respectively
0, 9, 8, 8 and 2 writes over 5 ms, with first-post-sync p99 ranging from
29.485 µs initially to 4.134–8.323 ms afterward. The first V8 capture still
passes the old filter, p99 ≤10 ms and mean fdatasync ≤1 ms, but its surrounding
probes detect the transition. It therefore cannot be compared with the later
slow V7 captures. The transitions do not identify their internal controller
trigger or prove that a particular workload caused either transition.

## CPU attribution remains diagnostic

The phase data are retained separately from device eligibility. For example,
the unchanged V8 captures below show different CPU costs despite the same
allowed mask and exactly the same payload count:

| V8 scope | Block 0, second V8 | Block 1, final V8 |
|---|---:|---:|
| Process CPU, s | 5.010 | 9.570 |
| Logical encode parent, s | 1.084 | 1.887 |
| Frame materialization, s | 0.217 | 0.373 |
| Memtable publication, s | 1.674 | 3.251 |
| MVCC publication, s | 0.432 | 1.063 |
| Voluntary context switches | 92,285 | 111,576 |

These phase estimates scale samples independently per thread and are
inclusive. Do not add encoding to its allocation/entry-encode children. All
four V8 captures hash 787,500,000 payload bytes with zero post-encode hash
rereads. A zero-call coordinator phase in this preserved instrumentation is
unobserved, not evidence that coordinator CPU is zero. Hardware cycles and
instructions are not collected; no IPC conclusion follows.

This reinforces the need for CPU-only preparation comparisons and a genuinely
controlled runtime completion pattern before changing source based on these
differences. Holding the allowed CPU mask constant does not hold frequency,
contention, placement within that mask or device timing constant.

## Counter labels and reproduction

Inspection found that `wal_pipeline.commit_groups` is not the same grouping
layer in both formats: V8 records it at FRONTIER publication in
`wal/v8/leader.rs`, while the parallel V7 worker path records DATA groups.
For example, the fastest V8 row reports 847 commit groups, 847 syncs and
5,013 worker notification requests. The final V7 row of block 0 reports
4,968 commit groups, 1,372 syncs and 4,968 notification requests.
These numbers do not demonstrate that V8 submits only 847 DATA groups.

The exact [executed runner](artifacts/rfc-026-v8-state-paired-20261103-runner-as-executed.py)
and raw blocks are preserved unchanged. Its derived `DATA_groups` key aliases
the raw `commit_groups` field and is incorrectly named; the current runner
uses `reported_commit_groups` and reports worker requests separately. It also
checks workload identity and handles incomplete analysis/failed children more
explicitly. These are post-capture tooling corrections, not rerun results.
The [notes](artifacts/rfc-026-v8-state-paired-20261103-notes.json) identify them.

The [first raw block](artifacts/rfc-026-v8-state-paired-20261103-block0.json),
[reverse raw block](artifacts/rfc-026-v8-state-paired-20261103-block1.json),
[summary](artifacts/rfc-026-v8-state-paired-20261103-summary.json) and smoke
artifacts retain every result and probe. Raw probe arrays, phase estimates,
binary/boot/mask identities and V8 digest accounting are checked. Replaying
the summaries with the current analyzer yields the same two rejections.
The [check record](artifacts/rfc-026-v8-state-paired-20261103-checks.json) and
[hash manifest](artifacts/rfc-026-v8-state-paired-20261103-hashes.json) cover
validation and reproduction. Twenty classifier/analysis tests pass. Rust
sources and dependencies are unchanged, so the runtime suite is not repeated.

The result is a usable measurement method and a direct demonstration of why
isolated best runs are insufficient on this SSD. There is no retained runtime
optimization or change to the RFC performance gate from these captures.
