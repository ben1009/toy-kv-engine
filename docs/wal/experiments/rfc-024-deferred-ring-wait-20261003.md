# RFC 024: Deferred task work with native completion waiting

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

**Decision:** Reject the prototype. It does not reduce the targeted one-writer
batch64 tail and misses throughput/tail guards elsewhere. Restore the retained
runtime; the full RFC performance gate remains unmet.

## Hypothesis and implementation

Follow the [cooperative-taskwork completion-wait screen](rfc-024-ring-command-wait-20261003.md)
by testing `SINGLE_ISSUER | DEFER_TASKRUN`. The worker waits inside
`io_uring_enter(GETEVENTS)` for write or command CQEs, with a one-shot `PollAdd`
on its existing command eventfd. One of the shared 256 SQEs is reserved for
that control request; its CQE is excluded from write counters. Commands can
interrupt the wait, and multiple groups can remain outstanding.

The [September deferred-taskwork experiment](../benchmarks/rfc-024-parallel-wal-benchmark.md#deferred-task-work-with-a-single-issuer-rejected-2026-09-28)
already used ring-native command waiting. This follow-up differs: it uses the
current optimized runtime, includes batch64, omits `TASKRUN_FLAG`, and omits
the extra nonblocking `GETEVENTS` calls before CQ consumption. It tests this
driver variant, rather than treating native waiting as an untried idea.

Deferred work requires the issuer to enter the kernel for completion
processing, as documented by [io_uring setup](https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_setup.2).
The prototype uses native waits and falls back to an ordinary ring when setup
is unsupported or the extended timed-wait interface is unavailable. Durability,
prefix failure handling, and MVCC publication remain unchanged.

Before timing, default and all-feature Clippy pass with `-D warnings`, and all
258 selected parallel-WAL, MVCC, and transaction tests pass outside the io_uring
sandbox without retries. Successful output confirms the command-CQE
consumption/rearming test executed without skipping.

## Frozen comparison

Both arms use parallel WAL with PITR off, 1 KiB values, and fresh paths on
Linux 6.18.9-arch1-2. The unchanged runtime is from `98aa2128`; the snapshot
head `1821a72f` only adds documentation. Ext4 uses the existing SSD; tmpfs
uses `/dev/shm`. SST targets are 1 GiB except for the 1 MiB tmpfs rotation guard.

Use the same eight cases and decision rule as the cooperative screen: three
ABBA/BAAB blocks per case, rotated case order, one fixed warmup per arm per
case, 96 scored runs plus 16 warmups. Sample every batch commit or every tenth
single put. Executables, raw outputs, and progress logs remain in RAM during
timing. No builds, tests, tracing, or administrative polling overlap scored
runs. All runs complete in 566 seconds; there is no extension or selective rerun.

Within-arm controls allow 5% throughput spread and 10% p99 spread. Retention
requires both one-writer batch cases to lower median p99 by at least 10%,
improve in all three blocks, and pass at least two control blocks each. Every
case must stay within 5% median throughput loss and 10% median p99 regression;
multiwriter cases must demonstrate at least two in-flight groups. Include every
block and stall. This screen does not claim RFC confidence-interval qualification.

| Case | Puts | Throughput change | p99 change | CPU/put change | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4, batch64, 1 writer, exact case | 65,536 | -2.3% | +5.2% | -1.4% | 2/3 |
| Ext4, batch64, 1 writer, longer run | 262,144 | +1.2% | +1.7% | 0.0% | 1/3 |
| Ext4, batch64, 8 writers | 262,144 | -6.9% | +6.0% | +0.2% | 1/3 |
| Ext4, single puts, 4 writers | 100,000 | -0.2% | 0.0% | -0.4% | 1/3 |
| Ext4, single puts, 8 writers | 100,000 | +2.7% | -2.4% | -2.9% | 1/3 |
| Ext4, single puts, 16 writers | 200,000 | -2.6% | +6.3% | 0.0% | 2/3 |
| Tmpfs, single puts, 1 writer | 50,000 | -0.7% | +14.8% | +1.0% | 0/3 |
| Tmpfs, rotation, 4 writers | 200,000 | -5.3% | -2.0% | +4.8% | 0/3 |

Changes are medians of all three block-level geometric-mean candidate/baseline
ratios. Exact one-writer p99 ratios are 1.158, 1.052, and 1.001; longer-run
ratios are 0.966, 1.035, and 1.017. Neither meets the frozen target. Ext4's
batch64 eight-writer throughput and both tmpfs cases also miss a median guard.

Host conditions still vary. Ext4's four-writer single-put throughput is about
9,700–10,800 puts/s in the second round and 4,300 puts/s in both arms in the
third round. Eight-writer batch64 p99 spans 2.73–13.88 ms in the baseline and
2.56–12.38 ms in the candidate. These runs remain included. Failed controls
prevent interpreting small median differences as isolated code effects.

Concurrent cases retain peaks of 4, 8, and 16 in-flight groups and outstanding
write SQEs in both arms; all write-CQE counts match logical commits. One-writer
cases remain at one group and one write SQE. These are software counters, not
measured device queue depth. Rejection of this variant does not establish that
every deferred-taskwork or completion-wait design would fail.

## Artifacts and restoration

`target/rfc024-deferred-ring-command-wait-20261003/` retains both binaries,
source snapshots, the rejected patch and test, build/test/Clippy logs, frozen
protocol, all raw outputs, 112 run records, 24 blocks, summaries, and independent
verification of metadata, schedule, ratios, controls, decision, and cleanup.

Protocol SHA-256:
`86e92bb190ef393a8bf3126a5b35d8d6a1375f5f7db8c5082fda488708faf9dc`.
Baseline binary SHA-256:
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.
Prototype binary SHA-256:
`34f8bb3d6eab55a72abd1c066c12d313160170427929bf182a58d31ddeec5fcc`.

Restore the worker byte for byte, update its timestamp, and verify the rebuilt
release executable matches the baseline. Disposable databases and RAM copies
are removed. No Rust changes from either October 3 completion-wait prototype
are retained.
