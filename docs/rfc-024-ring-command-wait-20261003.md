# RFC 024: Ring command and completion waiting

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

**Decision:** Reject the cooperative-taskwork prototype. Waiting inside
io_uring does not establish the targeted one-writer batch64 tail improvement.
Restore the retained runtime; the full RFC performance gate remains unmet.

## Hypothesis and implementation

The [batch64 hop measurements](rfc-024-w1-batch64-p99-hops-20261002.md)
identify write-stage tails before the coordinator's data sync. A previous
registered-eventfd experiment retained an external poll wait. This prototype
instead waits in `io_uring_enter(GETEVENTS)` for any write or command CQE.

A one-shot `PollAdd` watches the existing command eventfd. Its reserved
request ID cannot overlap write IDs; its CQE is consumed separately from
write accounting. Reserve one of the shared 256 SQEs for that request.
Commands still interrupt the completion wait, multiple groups can be
submitted before waiting, and the one-second signaling-failure fallback is
retained. The worker keeps `SINGLE_ISSUER | COOP_TASKRUN`.

This follows the kernel interfaces for
[poll requests](https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_prep_poll_add.3)
and [completion waits](https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_submit_and_wait.3).
It changes the completion wait, not the durability protocol. No producer
leader path or backend selector is introduced.

Before timing, all 258 selected parallel-WAL, MVCC, and transaction tests
pass outside the io_uring sandbox. Successful test output confirms the new
command-CQE consumption/rearming test executed without skipping. Default and
all-feature Clippy pass with `-D warnings`.

## Frozen comparison

Compare release builds with `bench` against `98aa2128` on Linux
6.18.9-arch1-2, ext4 on the existing SSD, and tmpfs under `/dev/shm`.
Every case uses parallel WAL, PITR off, 1 KiB values, and fresh paths.
The SST target is 1 GiB except for the tmpfs rotation guard's 1 MiB target.

Each case has three four-run ABBA/BAAB blocks, with rotated case order and
one fixed warmup per arm: 96 scored runs and 16 warmups. Latency sampling is
every batch commit or every tenth single put. Executables, raw outputs, and
progress logs stay in RAM during timing. No builds, tests, tracing, or
administrative polling overlap scored runs. All runs complete in 572 seconds.

Repeat controls require each arm's throughput spread to stay within 5% and
p99 spread within 10%. Every block contributes to the reported medians.
Before timing, require both one-writer batch cases to lower median p99 by at
least 10%, improve its direction in all three blocks, and pass at least two
control blocks each. Guard medians may not lose over 5% throughput or gain
over 10% p99. Multiwriter cases must still reach at least two in-flight groups.
There is no extension of this screen or confidence-interval qualification claim.

| Case | Puts | Throughput change | p99 change | CPU/put change | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4, batch64, 1 writer, exact case | 65,536 | +2.9% | +3.0% | -0.2% | 1/3 |
| Ext4, batch64, 1 writer, longer run | 262,144 | -2.2% | +5.7% | +1.1% | 0/3 |
| Ext4, batch64, 8 writers | 262,144 | +0.5% | +3.3% | -0.3% | 0/3 |
| Ext4, single puts, 4 writers | 100,000 | +0.2% | -2.4% | -0.3% | 1/3 |
| Ext4, single puts, 8 writers | 100,000 | -0.8% | +5.9% | -0.8% | 1/3 |
| Ext4, single puts, 16 writers | 200,000 | -1.2% | +1.9% | +0.1% | 2/3 |
| Tmpfs, single puts, 1 writer | 50,000 | +5.5% | -10.7% | -2.4% | 0/3 |
| Tmpfs, rotation, 4 writers | 200,000 | -3.4% | +3.7% | +5.4% | 2/3 |

Changes are medians of three block-level geometric-mean candidate/baseline
ratios. The exact one-writer p99 ratios are 1.030, 1.160, and 0.898; the longer
case's are 1.084, 0.960, and 1.057. Neither satisfies the frozen target.
The tmpfs-solo apparent improvement has no passing repeat controls.

Both arms encounter a large latency cliff. In the exact case's second block,
baseline p99 spans 3.08–20.75 ms and candidate p99 spans 2.96–29.11 ms.
These runs remain included and do not isolate a code effect. The guard medians
pass, but that does not establish equivalence under failed controls.
Concurrent cases retain peaks of 4, 8, and 16 groups and outstanding write
SQEs; write-CQE counts match logical commits. These are software counters,
not measured device queue depth.

## Artifacts and restoration

`target/rfc024-ring-command-wait-20261003/` retains both binaries, worker
snapshots, the rejected patch and test, build/test/Clippy logs, frozen protocol,
all raw outputs, 112 run records, 24 block records, summaries, and independent
verification of the schedule, metadata, ratios, controls, decision, and cleanup.

Protocol SHA-256:
`1946eecd11308f8293cd17faecb1674dddc6491e6942ff47c91c59bb67e5046b`.
Baseline binary SHA-256:
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.
Prototype binary SHA-256:
`2cd11e8c72b6703bc8603fb76e209437d1976f6211c611f47420c57b98a0b420`.

Restore the worker source byte for byte, update its timestamp, and verify the
rebuilt executable matches the baseline. Disposable databases and RAM copies
are removed. This screen retains documentation only.
