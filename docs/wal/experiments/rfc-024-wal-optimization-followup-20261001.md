# RFC 024: WAL optimization follow-up, 2026-10-01

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](../rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

**Decision:** Reject both the byte-limited sync-coalescing prototype and the
worker integer-map prototype. Neither establishes an improvement across its
measured workloads. Restore the `d7171c80` runtime and worker sources. Parallel
WAL remains opt-in, and the [full qualification gate](../benchmarks/rfc-024-wal-qualification-20261001.md)
remains unmet.

## Batch profiles

Profile the current parallel v4 WAL on ext4 with 262,144 puts, 64 entries per
commit, 1 KiB values, PITR off, a 1 GiB SST target, and every commit sampled.
These diagnostic runs precede the comparisons and are excluded from scoring.

| Writers | Puts/s | Commit p99 (ms) | Sync calls | Groups/sync | Total fdatasync time (ms) | Aggregate durability-wait time (ms) | Aggregate extent-readiness wait (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 94,150 | 1.655 | 4,096 | 1.00 | 1,216.5 | 1,732.0 | 0.9 |
| 8 | 334,898 | 2.944 | 1,116 | 3.67 | 455.6 | 4,132.3 | 6.5 |
| 16 | 417,148 | 4.459 | 676 | 6.06 | 303.7 | 5,703.7 | 21.0 |

Durability waits are the largest instrumented commit stage. Extent readiness
does not dominate these profiles. The wait counters add time across concurrent
writers; they are not wall-time percentages. Detailed sync bookkeeping also
adds overhead. These profiles motivate checking the coalescing policy without
establishing that its deadline causes the batch-tail regression.

## Comparison method

Each case has three four-run ABBA/BAAB blocks, with case order rotated between
rounds. Both arms use parallel WAL and fresh database paths. Build with
`cargo build --release --locked --offline -p kv-engine --features bench --bin write-perf`,
snapshot both executables, and freeze their hashes and run schedule before
timing. No builds, tests, tracing, or device-admin polling overlap scored runs.
Raw stdout and stderr stay in RAM during timing and are copied to the experiment
directory afterward.

Each arm appears twice per block. A block's ratio is the geometric mean of its
two candidate measurements divided by the geometric mean of its two baseline
measurements. Tables report the median of all three block ratios. Repeat
controls require each arm's throughput max/min to be at most 1.05 and its p99
max/min to be at most 1.10. Failed controls and slow runs remain in every
reported result. Three blocks provide exploratory evidence; no confidence
interval or adoption-gate claim is made.

## Stop coalescing after 256 KiB of written bytes

The prototype counts bytes in the contiguous written prefix since the preceding
successful sync. It exits the existing coalescing receive loop when that count
reaches 256 KiB. The fixed admission cutoff, 400-microsecond deadline, prior-sync
latency condition, and durability acknowledgement remain unchanged. The
threshold exceeds the 128 KiB contributed by 32 ordinary 4 KiB writes, preserving
their existing coalescing policy.

Both ext4 cases use 524,288 puts in 64-entry batches, 1 KiB values, a 1 GiB SST
target, and latency sampling on every commit: 8,192 samples per scored run.
Exactly one 262,144-put warmup per arm precedes each case's first block.
All 24 scored runs and four warmups complete.

| Writers | Throughput change | p99 change | CPU/put change | Passing repeat-control blocks |
| ---: | ---: | ---: | ---: | ---: |
| 8 | +0.4% | +2.5% | +0.2% | 0/3 |
| 16 | +0.4% | -3.1% | -9.1% | 1/3 |

Eight-writer throughput ratios are 1.004, 1.016, and 0.993; p99 ratios are
1.025, 0.853, and 1.543. Sixteen-writer throughput ratios are 1.004, 0.970,
and 1.092; p99 ratios are 0.969, 1.002, and 0.453. The latter's final block
includes large tail swings in both arms. The small throughput medians and
failed controls do not establish a useful gain; the apparent sixteen-writer
CPU reduction is also insufficient to retain this policy.

All 49 focused parallel-WAL tests pass before timing. Remove the prototype
and verify that `runtime.rs` matches the baseline snapshot byte for byte.

Local artifacts: `target/rfc024-batch-sync-20261001/` contains `run.py`,
`protocol.json`, both binaries, source snapshots, the rejected patch, raw
outputs, `runs.jsonl`, `blocks.jsonl`, `summary.json`, and `artifact-manifest.json`.
Protocol SHA-256: `329d386af95a312ce9961ff5bd0197c3da528304c3269591ba71b1879d6a6ac0`.

## Worker integer maps

After restoring the first prototype, profile the unchanged baseline with
200,000 single puts, 64 writers, 1 KiB values, and a 1 GiB SST target on ext4.
Attach `perf record -e cycles:u -F 99 -m 1 -p <pid>` after 0.5 seconds of startup.
The capture has 1,673 samples and zero reported loss. Request-ID hashing and
SipHash account for 2.92% of atom-PMU and 3.63% of core-PMU sampled user cycles.
MVCC publication accounts for 23.88% and 31.87%, respectively. These are CPU
sample shares, not wall-time shares, and the capture does not attribute generic
mutex samples to a particular lock.

Test `ahash::AHashMap` for worker-owned group, request, permit, and cancellation
maps. Their keys are internal integer counters. Use the existing dependency;
preserve ordering, buffer ownership, queue depth, and sync policy.

Single-put ext4 cases use 200,000 puts and sample every tenth operation; each
arm has one 50,000-put warmup per case. The ext4 batch case uses the same
524,288-put parameters as above and one 262,144-put warmup per arm. The tmpfs
guard uses 50,000 single puts, samples every tenth operation, and has one
20,000-put warmup per arm. All cases use a 1 GiB SST target. All 48 scored runs
and eight warmups complete.

| Case | Writers | Throughput change | p99 change | CPU/put change | Passing repeat-control blocks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ext4 single puts | 16 | +0.3% | -2.9% | +0.4% | 3/3 |
| Ext4 single puts | 64 | +1.8% | -0.1% | -2.6% | 3/3 |
| Ext4 batch 64 | 8 | -1.0% | +23.9% | -4.3% | 1/3 |
| Tmpfs single puts | 1 | +0.8% | -8.2% | +0.0% | 0/3 |

The 64-writer throughput ratios are 1.024, 1.018, and 0.983; CPU/put ratios
are 0.954, 0.974, and 1.045. Its final block passes the repeat controls but
reverses the earlier throughput and CPU improvements and increases p99 by
26.8%. The batch guard's p99 ratios are 1.024, 1.239, and 1.645. Device stalls
and scheduling variation remain unresolved, so these comparisons do not
establish why tails differ. The inconsistent primary gain and unresolved batch
regression do not support retaining the map change.

All 49 focused parallel-WAL tests pass before timing. Restore `worker.rs`
byte for byte and rebuild the release executable from restored source.

Local artifacts: `target/rfc024-worker-hash-20261001/` contains the frozen
protocol, binaries, source snapshots, rejected patch, raw outputs, block and
run records, summary, and SHA-256 manifest. The CPU capture and batch profiles
are in the preceding experiment directory. Worker-map protocol SHA-256:
`f90bd7a879cfcd578f061585bcf8a8dd0f0ab4da3b96c1250cc1a2a3580110bb`.
