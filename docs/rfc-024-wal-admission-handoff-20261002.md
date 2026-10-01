# RFC 024: Release write ordering before producer packing

**Decision:** Reject this prototype and restore the runtime at `2cd59bbb`.
Across 108 scored runs, the 64-writer ext4 case loses throughput in every
block, and tmpfs rotation loses 9.1–14.4%. Parallel WAL remains opt-in; the
[full qualification gate](rfc-024-wal-qualification-20261001.md) remains unmet.

## Hypothesis and correctness

The existing point-put path holds the MVCC write-order mutex through
timestamp allocation, encoding, ticket admission, and producer-side packing.
Packing can wait for extent readiness or an available group slot. The
prototype transfers ownership of that mutex guard into WAL admission,
releasing it after ticket assignment and enqueue, before packing starts.

The admission/packer handshake is preserved: a producer claims the packer
with a nonblocking attempt while holding admission, and the packer releases
its mutex while admission is still held when its queue becomes empty.
Buffers cannot be stranded. Admission failures release the ordering guard
without assigning a ticket; failures after admission retain the existing
ticket outcome and poison-prefix contract. No new worker, backend selector,
format, sync policy, or publication policy is added. Only ordinary MVCC
point puts use the guard transfer; TTL, delete, batch, range, and PITR paths
keep their existing submission sequence. The leader guard checks shared
point-put plumbing, rather than testing a leader optimization.

Before timing, 257 focused parallel-WAL/MVCC/transaction tests passed, as did
default-feature and all-feature Clippy with `-D warnings`. A controlled test
pauses the first packer at preallocation, verifies the ordering mutex is
already available, admits later ordered tickets, and injects a packing
failure after the earlier group. The earlier ticket still becomes durable
and recovers; later tickets fail. The test also checks that validation
failure releases the guard and consumes no ticket. A separate run confirmed
that this test exercised io_uring without an unavailable-ring skip.

## Fixed comparison

Run on 2026-10-02, with release builds and `bench`, Linux 6.18.9-arch1-2,
the existing ext4 filesystem and SSD, and `/dev/shm` for tmpfs. Both arms use
the same explicit backend per case, 1 KiB values, PITR off, and identical
sampling. Each case has three four-run ABBA/BAAB blocks, two scored runs per
arm per block, and one predetermined warmup per arm: 108 scored runs and
18 warmups in total. Case order rotates across rounds. Logs and executables
reside in RAM during timing; no builds, tests, tracing, or administrative
polling overlap the measured runs.

Except for the rotation cases, the SST target is 1 GiB. Rotation uses 1 MiB.
Latency sampling is every 10 puts, or every commit in the batch case. The
batch case commits 64 puts per ticket. A block passes the control screen
only when both arms' throughput spread is at most 5% and p99 spread at most
10%. Every block, including failed controls and latency swings, contributes
to the reported medians; ranges are descriptive, not confidence intervals.

The decision rule was frozen before timing: an ext4 16/32/64-writer case must
improve median throughput or CPU per put by at least 5%, maintain that
improvement direction across all three blocks, and have at least two
passing control blocks. No case may lose over 5% median throughput or gain
over 10% median p99. The screen is not extended for favorable results.

| Case | Puts | Throughput change | p99 change | CPU/put change | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4, 16 writers | 200k | +3.0% | -2.3% | -3.6% | 1/3 |
| Ext4, 32 writers | 200k | -1.0% | -0.1% | +3.0% | 2/3 |
| Ext4, 64 writers | 200k | -4.6% | -1.8% | +2.8% | 3/3 |
| Ext4, rotation, 4 writers | 20k | -0.1% | -10.1% | +0.4% | 1/3 |
| Ext4, 1 writer | 10k | -0.5% | -8.1% | +0.1% | 3/3 |
| Ext4, batch64, 8 writers | 524,288 | +3.1% | -48.5% | -5.5% | 0/3 |
| Tmpfs, 1 writer | 50k | +2.4% | -6.1% | -1.2% | 0/3 |
| Tmpfs, rotation, 4 writers | 200k | -14.3% | +9.7% | +12.7% | 1/3 |
| Ext4, leader guard, 16 writers | 100k | +0.5% | -6.2% | -1.3% | 1/3 |

Each change is the median of three block-level geometric-mean
candidate/baseline ratios. The ext4 64-writer throughput ratios are 0.938,
0.954, and 0.961, with all three control blocks passing. Its CPU/put ratios
are 1.015, 1.053, and 1.028. Tmpfs rotation throughput ratios are 0.857,
0.856, and 0.909; its one passing block loses 14.4% throughput and increases
p99 by 23.6%. These regressions reject the prototype.

The batch and tmpfs-solo apparent gains have no passing control blocks.
One late ext4 16-writer block appears 23.3% faster, but its two baseline
throughputs differ by 46.3%; it fails the control screen. Ext4 rotation
ratios range from 0.791 to 1.169. These comparisons establish no gain or
resolution of the previously observed environment instability.

## Pipeline observations and artifacts

Per-run software counters show that additional outstanding requests alone
did not improve this workload. Values below are medians of the six scored
runs per arm for each metric, independently of the block-level ratios above.

| Ext4, 64 writers, 200k puts | Baseline | Prototype |
| --- | ---: | ---: |
| I/O groups | 200,000 | 187,662.5 |
| Sync calls | 6,203.5 | 6,199 |
| Peak in-flight groups | 32 | 32 |
| Peak outstanding write SQEs | 32 | 61 |

Group count fell about 6.2%, but sync count stayed essentially unchanged,
and throughput fell. Outstanding SQEs measure the software pipeline; device
queue depth was not collected and must not be inferred from this table.

Separate baseline profiling samples attribute 54–58% of user cycles at
32 writers to MVCC publication, versus 25–39% at 64 writers across the two
CPU PMUs. Unwinding the optimized release binary has incomplete call stacks,
so generic mutex samples cannot be attributed to the write-order mutex.
The initial profiling attempt failed with an io_uring `ENOMEM` during
post-measurement cleanup; its capture and error are preserved separately.
Both follow-up captures completed with smaller per-thread perf buffers.
These diagnostic runs are excluded from scored comparisons. The results
support investigating publication and durability handoffs further; they do
not establish a new bottleneck cause or a device-level fix.

Artifacts reside in `target/rfc024-commit-handoff-20261001/`, whose directory
was created before the date change. They include baseline/candidate source
snapshots, the rejected patch, both binaries, profiling captures and commands,
build/test/Clippy logs, the frozen protocol and driver, all raw outputs and
run records, block/control calculations, summaries, and verification results.
The protocol SHA-256 is
`752086bd6c446ec36c14420f1171121916a3caf36c3ce26148860341a2d3cca3`.
The baseline binary SHA-256 is
`b756794387130d202c849b745c236d37a7ebcd8ea2625fdb5ea38d0a1d6686f3`;
the prototype is
`96e70f581cc417f08c1ae1ad8a607e8a3b32efd3b9ac7075c13bdf4c81ebf54e`.
After source restoration, the rebuilt release binary matches the baseline
hash. Disposable benchmark databases and RAM artifacts are removed after
their outputs are preserved. This follow-up retains documentation only.
