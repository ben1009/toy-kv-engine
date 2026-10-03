# RFC 024: Cooperative publication wait

**Decision:** Reject the prototype and restore `67dd7554`'s runtime. A
cooperative yield gives only a small 32-writer improvement, below the frozen
retention threshold, and fails the shared-MVCC p99 guard. Parallel WAL
remains opt-in; the [full qualification gate](rfc-024-wal-qualification-20261001.md)
remains unmet.

## Hypothesis and validation

Re-annotating the unchanged baseline's existing 32-writer capture places the
hot publication instructions in repeated frontier loads, comparisons, and
`pause` instructions. Publication accounts for 54–58% of sampled user cycles
across the two CPU PMUs. Ready-set searches do not appear above the report's
0.5% symbol threshold. These are user-CPU samples, not wall-time attribution.

Test one `yield_now()` after the first 256 iterations of a longer publication
wait. The caller then uses the remaining iterations of its original 4,096
budget before parking. A wait assigned the shorter 256-iteration budget
does not yield. The scheduling hypothesis is that an earlier publisher gets
an opportunity to run without immediately parking its followers. Neither
scheduler progress nor frontier progress resets the bounded spin budget.

The prototype preserves ready-prefix registration, release/acquire ordering,
the publication mutex, retired gaps, poison predicates, reservation lifetime,
and barrier draining. No WAL format, backend selection, sync policy, or
production default changes. The leader case checks the shared MVCC change.

Before timing, all 257 selected parallel-WAL, MVCC, and transaction tests
passed, along with default-feature and all-feature Clippy using `-D warnings`.
A separate successful-output run confirmed the engine publication test used
io_uring without skipping and the 64-publication barrier test passed.

## Frozen comparison

On 2026-10-02, compare release builds with `bench`, Linux 6.18.9-arch1-2,
the existing ext4 filesystem and SSD, and `/dev/shm` for tmpfs. Both arms use
the same explicit backend per case, 1 KiB values, and PITR off. Each case
has three four-run ABBA/BAAB blocks, with case order rotated across rounds,
and one predetermined warmup per arm: 132 scored runs and 22 warmups. All
runs completed in about 764 seconds. No timing extension was made.

Executables and logs reside in RAM during timing. No builds, tests, tracing,
or administrative polling overlap scored runs. Latency sampling is every
10 puts, or every commit for batch64. The SST target is 1 GiB, except for
rotation cases, which use 1 MiB. A block passes its repeat controls only if
both arms' throughput spread is at most 5% and p99 spread at most 10%.
Every block, including failures and stalls, contributes to the medians.

The decision rule was frozen before timing: an ext4 16/32/64-writer case must
improve median throughput or CPU per put by at least 5%, maintain that
improvement direction in all three blocks, and have at least two passing
control blocks. No case may lose over 5% median throughput or gain over 10%
median p99. This optimization screen does not establish the full RFC gate.

| Case | Puts | Throughput change | p99 change | CPU/put change | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4, 4 writers | 100k | -0.7% | +2.2% | +0.4% | 1/3 |
| Ext4, 8 writers | 100k | +0.7% | +1.1% | +1.1% | 1/3 |
| Ext4, 16 writers | 200k | +0.2% | +3.6% | +1.4% | 1/3 |
| Ext4, 32 writers | 200k | +1.4% | +2.0% | -2.2% | 3/3 |
| Ext4, 64 writers | 200k | +1.3% | -5.7% | -2.7% | 1/3 |
| Ext4, rotation, 4 writers | 20k | -0.4% | +8.6% | +1.3% | 2/3 |
| Ext4, 1 writer | 10k | +0.7% | -3.3% | +0.0% | 3/3 |
| Ext4, batch64, 8 writers | 524,288 | +0.7% | -0.2% | -0.6% | 1/3 |
| Tmpfs, 1 writer | 50k | +0.1% | -15.0% | -1.9% | 0/3 |
| Tmpfs, rotation, 4 writers | 200k | -0.3% | -4.4% | -0.1% | 1/3 |
| Ext4, leader guard, 16 writers | 100k | -2.3% | +15.3% | -0.2% | 1/3 |

Each change is the median of three block-level geometric-mean
candidate/baseline ratios. Ranges are descriptive, not confidence intervals.
At 32 writers, throughput ratios are 1.014, 1.011, and 1.036; CPU/put ratios
are 0.973, 0.978, and 0.983. All three repeat controls pass. The CPU reduction
repeats, but does not reach the frozen threshold. Median sync counts are
11,646 versus 11,628.5, with 200,000 groups and peaks of 32 in-flight groups
and 32 outstanding write SQEs in both arms. These software counters do not
measure device queue depth.

The leader guard's p99 ratios are 1.153, 0.986, and 1.269. Only the middle
block passes its repeat controls; the aggregate result fails the guard and
does not isolate how much of the change comes from this prototype.
The first 16-writer block has a 0.492 throughput ratio and 4.244 p99 ratio,
and remains included. The last ext4 rotation block loses 15.5% throughput
and has 42.3% worse p99. Neither the smaller median differences nor the
tmpfs-solo apparent tail improvement resolves the host's earlier variability.
The in-order single-writer path does not enter the modified wait.

## Artifacts and restoration

`target/rfc024-publication-yield-20261002/` preserves both binaries and source
snapshots, the rejected patch, baseline annotation, build/test/Clippy logs,
the frozen protocol and driver, all raw outputs and run records, block
calculations, summaries, and independent verification. Verification checks
all 154 records, backend/workload/sample metadata, source and binary hashes,
the schedule, ratios, controls, decision, and cleanup.

The protocol SHA-256 is
`03a8d9550caedd01e85f208d1e86edf7d6aa57d8e96fda4e308d55003b2a27e9`.
The baseline binary SHA-256 is
`b756794387130d202c849b745c236d37a7ebcd8ea2625fdb5ea38d0a1d6686f3`;
the prototype is
`bf0cc02236b7e20ac80ddf861f2e8be74a87504c886452f990b4e477b9b0c0b3`.
After source restoration and rebuilding, the release binary matches the
baseline hash. Disposable benchmark databases, test databases, and live
RAM artifacts are removed. This follow-up retains documentation only.
