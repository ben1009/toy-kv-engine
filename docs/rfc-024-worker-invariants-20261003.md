# RFC 024: Release-mode worker invariant bookkeeping

**Decision:** Reject the prototype and restore the retained runtime. Removing
release-mode diagnostic work does not reach the frozen improvement threshold.
The short one-writer batch64 case also exceeds the p99 guard. Parallel WAL
remains opt-in, and the [full qualification gate](rfc-024-wal-qualification-20261001.md)
remains unmet.

## Hypothesis and validation

A baseline ext4 profile uses 200,000 single puts, 16 writers, 1 KiB values,
and a 1 GiB SST target. Its 4,280 user-cycle samples have zero reported loss.
The worker's `assert_invariants()` accounts for 0.32% of atom-PMU and 1.34%
of core-PMU sampled user cycles. These are separate CPU sample shares,
rather than wall-time attribution. A separate tmpfs capture loses 71.49%
of samples and is excluded from attribution; both captures are preserved.

The checker contains `debug_assert!` calls, but its surrounding vector
allocation and adjacent-group lookups still execute in release builds.
The prototype places the whole checker body under `cfg(debug_assertions)`.
Every original debug assertion remains. Admission, buffer ownership,
submission, completion, synchronization, and publication are unchanged.
The baseline release executable contains the checker symbol; the candidate
does not.

Before timing, default-feature, all-feature, and release all-feature Clippy
pass with `-D warnings`. All 257 selected debug parallel-WAL, MVCC, and
transaction tests pass, as do 95 selected release parallel-WAL tests with
`bench`. Tests run outside the io_uring sandbox, without retries.

## Frozen comparison

Compare release builds with `bench` against `f86cf9ef`, whose Rust runtime
matches the preceding retained baseline. Use Linux 6.18.9-arch1-2, the
existing SSD's ext4 filesystem, and tmpfs under `/dev/shm`. Both arms use
parallel WAL in every case, PITR off, 1 KiB values, and fresh database paths.
The SST target is 1 GiB except for rotation's 1 MiB target. Sample every
tenth single put or every batch commit.

Each of 12 cases has three ABBA/BAAB blocks, two scored runs per arm per
block, and one fixed warmup per arm: 144 scored runs and 24 warmups.
Case order rotates between rounds. All runs complete in 728 seconds.
Executables and raw outputs stay in RAM during timing; controller progress
is logged between runs. No builds, tests, tracing, or administrative polling
overlap scored runs.

Repeat controls allow at most 5% within-arm throughput spread and 10% p99
spread. Retention requires an ext4 16/32/64-writer case to improve median
throughput or CPU per put by at least 5%, improve that metric in all three
blocks, and pass at least two control blocks. Every case must stay within
5% median throughput loss and 10% median p99 regression. Concurrent candidate
runs must reach at least two in-flight groups. Include every block and stall.

Only a passing initial screen would trigger one predefined confirmation:
three additional blocks for ext4 16/32/64 writers, ext4 batch64 with eight
writers, ext4 rotation, and tmpfs solo/rotation. The qualifying metric must
improve in every confirmation block, at least four of six target controls
must pass, and combined medians must pass the same guards. The initial
screen fails, so confirmation is not run. There is no selective rerun or
confidence-interval qualification claim.

| Case | Puts | Throughput change | p99 change | CPU/put change | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4, single puts, 4 writers | 100k | +0.6% | -1.7% | -1.5% | 0/3 |
| Ext4, single puts, 8 writers | 100k | +0.8% | +1.0% | -0.2% | 2/3 |
| Ext4, single puts, 16 writers | 200k | +0.4% | +1.6% | -0.9% | 2/3 |
| Ext4, single puts, 32 writers | 200k | -2.7% | +5.2% | +4.5% | 0/3 |
| Ext4, single puts, 64 writers | 200k | +2.4% | -2.3% | -0.2% | 1/3 |
| Ext4, rotation, 4 writers | 20k | +1.0% | -5.0% | -3.0% | 0/3 |
| Ext4, single puts, 1 writer | 10k | +0.3% | -2.3% | -1.2% | 0/3 |
| Ext4, batch64, 8 writers | 524,288 | -2.6% | -3.5% | -0.8% | 0/3 |
| Tmpfs, single puts, 1 writer | 50k | -2.2% | +7.2% | +3.0% | 0/3 |
| Tmpfs, rotation, 4 writers | 200k | +1.3% | +0.1% | -2.2% | 0/3 |
| Ext4, batch64, 1 writer, exact case | 65,536 | -2.7% | +16.0% | +0.9% | 0/3 |
| Ext4, batch64, 1 writer, longer run | 262,144 | +0.4% | -6.4% | +1.1% | 2/3 |

Changes are medians of all three block-level geometric-mean candidate/baseline
ratios. At 16 writers, CPU/put ratios are 0.991, 0.994, and 0.990; the
consistent decrease is below the 5% threshold. At 64 writers, throughput
ratios are 1.024, 1.020, and 1.163, with only the second block passing controls.
Its median improvement is below the threshold, and CPU is near parity.

Large within-block swings remain included. In the second 32-writer block,
candidate throughput changes from 34,244 to 6,410 puts/s and p99 from
1.85 to 41.51 ms, while the two baseline throughputs stay near 35,000 puts/s.
In the last eight-writer block, baseline throughput changes from 2,947 to
14,969 puts/s, while both candidate runs stay near 15,000 puts/s. Neither
block passes controls. The short batch64 p99 ratios are 1.866, 0.972, and
1.160, with zero passing controls. They fail the median guard without
isolating how much of the tail difference this code change causes.

Both arms retain peaks of 16 groups/write SQEs at 16 writers and 32 at
32/64 writers. All write-CQE counts match logical commits. These are software
pipeline metrics; device queue depth was not measured. This experiment
does not establish an end-to-end benefit from removing this bookkeeping.

## Artifacts and restoration

`target/rfc024-worker-scratch-20261003-bntj1ivr/` preserves baseline/candidate
source snapshots and binaries, the rejected patch, profiling captures and
commands, symbol checks, build/test/Clippy logs, the frozen protocol and
confirmation driver, all 168 run records, 36 block records, raw outputs,
pipeline summaries, and independent schedule/metadata/ratio/control/depth
verification. Disposable databases and RAM copies are removed.

Protocol SHA-256:
`0169c0e95a847887370390a20a2bd21edc7ad306f3ca4c8dde180bdbfc5662d5`.
Baseline binary SHA-256:
`6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da`.
Prototype binary SHA-256:
`d0bfebaba5e09446c6d967607f498b04ae7a7cc6b1da2c2f0832e4b252feb517`.
Artifact manifest SHA-256 (385 files verified):
`ec8353e2c86bec5c3a96aa9777f259efca39af673ddd5ae92bcd4487735fdeb5`.

The worker source was restored byte for byte, and the rebuilt release binary
matches the saved baseline. This follow-up retains documentation only.
