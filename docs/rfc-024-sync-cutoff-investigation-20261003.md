# RFC 024: sync cutoff regression investigation, 2026-10-03

**Historical report:** Measurements and default/opt-in recommendations below
describe the revision tested. Ordinary v4 WALs now default to parallel by
maintainer decision; see the [current adoption note](rfc-024-parallel-wal-default-20261005.md).
The original performance gate remains unqualified.

The current parallel WAL has a synchronization batching regression.
Commit `233a8a85` moved the coalescing cutoff snapshot outside the
completion loop. A single-variable diagnostic restores larger sync batches
while retaining the current worker and lifecycle fixes. In all twelve
comparison blocks it reduces sync calls. The observed paired throughput
medians rise **21.5% at eight writers** and **18.2% at sixteen**, but both
cases fail the frozen stability screen. These are descriptive results,
not qualified gains or an RFC adoption pass.

The diagnostic is not retained in the implementation. All original Rust
sources and the normal release executable were restored before scored
timing. The intermittent stalls remain unresolved.

## Code change isolated

The [preceding comparison](rfc-024-parallel-comparison-20261003.md)
found repeatable throughput regressions of 16.8% and 15.4% against the
archived `067d4b09` parallel executable. It also found substantially more
sync calls, despite identical peak software pipeline depth.

The runtime source at `067d4b09` and `925ed07d` is identical.
[Commit 233a8a85](https://github.com/ben1009/toy-kv-engine/commit/233a8a85d3861ba828b683756a5dd2612b5c98fe)
changes the successful coalescing path as follows:

```diff
 let deadline = Instant::now() + SYNC_COALESCE_WAIT;
+let cutoff = inner.admission.lock().next_ticket;
 loop {
     let state = inner.durability.state.lock();
-    let cutoff = inner.admission.lock().next_ticket;
```

The fixed snapshot can be reached while later-admitted tickets still
have completions pending. Coalescing then stops, and the coordinator
synchronizes a smaller written prefix. Tickets outside that prefix
require subsequent sync calls. Rereading the batching cutoff allows
more admitted tickets to join within the same fixed deadline.

The experiment uses the latest retained source at
`75c39487a24f1e1cd3f3b637e05841f93a117d92`, with only the inverse
one-line move for the diagnostic. Both arms retain the 400 microsecond
deadline and the preceding-sync latency threshold of 100 microseconds.
The worker, io_uring settings, buffer ownership, shutdown, failure
handling, public `sync()` and close cutoffs, and the target captured
immediately before `fdatasync` are otherwise identical.

This distinguishes two cutoffs: the optional wait's batching cutoff
and the durability target captured before the syscall. The experiment
changes only the former. It does not acknowledge writes that complete
after the syscall's target capture.

## Diagnostic profiles

Four separate 50,000-put runs enable per-sync observations: one per
arm at eight and sixteen writers. They are not scored timing runs.
All use parallel WAL, PITR off, 1 KiB values, and a 1 GiB SST target.
In these runs each put produces one ticket, one solo I/O group, and
one write SQE. Thus groups per sync also measures tickets per sync
for this workload.

| Writers | Arm | Sync calls | Groups/sync | Syncs covering the full writer width |
| --- | --- | ---: | ---: | ---: |
| 8 | Current fixed cutoff | 10,276 | 4.866 | 2,007 / 10,276 (19.5%) |
| 8 | Diagnostic moving cutoff | 6,419 | 7.789 | 6,061 / 6,419 (94.4%) |
| 16 | Current fixed cutoff | 5,439 | 9.193 | 982 / 5,439 (18.1%) |
| 16 | Diagnostic moving cutoff | 3,493 | 14.314 | 2,911 / 3,493 (83.3%) |

Every observation succeeds, the captured targets advance monotonically,
and the observations cover all 50,000 groups exactly once. The source
change replaces many partial-wave syncs with full-width syncs. This
directly identifies a batching cost rather than a failure to reach
the configured software write depth. These counters do not measure
NVMe device queue depth.

## Frozen comparison

Use native io_uring on the existing NVMe-backed ext4 filesystem,
Linux 6.18.9-arch1-2. Each scored run performs 200,000 single puts with
8 or 16 writers, 1 KiB values, a 1 GiB SST target, PITR off, and
every-tenth-operation latency sampling. Each has 20,000 latency
samples and 200,000 completed buffers and write CQEs.

Each case has six predeclared ABBA/BAAB blocks, two runs per arm
per block, with rotating case order. Exactly one 50,000-put warmup
per arm precedes each case's first block. Each run uses a fresh
database and a fixed five-second idle gap afterward. Executables
and output remain in RAM until timing ends. No builds, tests,
profiling, tracing, storage-setting changes, or controller
administrative polling run alongside the comparison. The quiet
preflight does not establish exclusive device access.

All **48 scored runs and four warmups** complete in **864 seconds**.
Every block, including stalled runs and failed controls, remains
included. The schedule is not extended or selectively repeated.

Within each block, both arms must have maximum/minimum throughput
at most 1.05 and p99 at most 1.10. Full stability additionally
requires all six blocks to pass, and each arm's session-wide
throughput spread at most 1.10 and p99 spread at most 1.20.
Neither case passes.

| Arm | Executable SHA-256 |
| --- | --- |
| A, current fixed cutoff | `6d6987a52fcf0cf013ff7a9345fb948d6fe2b6018c0be3a473aa041437fb96da` |
| B, diagnostic moving cutoff | `ef1e8d8f03d439684870b8432960b6cf23342ff9577ce36f487aad7a281819ec` |

## Results including every block

Paired changes are medians of all six block ratios. Each block
divides the geometric mean of its two B measurements by that of
its two A measurements. Absolute arm medians use all twelve scored
runs per arm; their quotient is not the paired estimate. CPU per
put includes process user and system CPU. All timing changes below
remain descriptive because the controls fail.

| Writers | Current puts/s | Diagnostic puts/s | Paired throughput change | Paired p99 change | Paired CPU/put change | Throughput controls | p99 controls | Both controls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 15,002 | 18,371 | +21.5% | -24.8% | -14.0% | 3/6 | 2/6 | 2/6 |
| 16 | 23,373 | 27,497 | +18.2% | -24.1% | -12.3% | 5/6 | 1/6 | 1/6 |

| Writers | All throughput block ratios, diagnostic/current | Exploratory 95% throughput interval |
| --- | --- | --- |
| 8 | 2.163, 0.635, 1.208, 1.221, 1.227, 1.200 | 0.917–1.695 |
| 16 | 1.187, 0.516, 1.185, 1.221, 1.179, 1.147 | 0.831–1.204 |

Intervals use 20,000 percentile block-bootstrap resamples with
seed 24. Both include parity. They do not override failed controls
or establish adoption.

### Synchronization work

| Writers | Current median sync calls | Diagnostic median sync calls | Paired sync-call change | Current median tickets/sync | Diagnostic median tickets/sync |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 | 40,482.0 | 25,451.5 | -37.0% | 4.940 | 7.858 |
| 16 | 21,477.5 | 14,017.5 | -34.6% | 9.312 | 14.268 |

All six sync-count block ratios lie between 0.623 and 0.650 at
eight writers and between 0.650 and 0.686 at sixteen. The effect
persists in blocks affected by stalls.

Median aggregate sync time falls from 9.758 to 6.795 seconds at
eight writers and 5.703 to 3.717 seconds at sixteen. Median mean
time per sync is 239 versus 267 microseconds at eight writers
and 266 versus 265 at sixteen. The diagnostic does less
synchronization work; it does not consistently make each sync
faster. The paired aggregate-sync-time changes are -29.8% and
-35.1%, respectively, and remain descriptive timing results.

Every scored run in both arms reaches the same software peaks:
eight or sixteen in-flight groups and outstanding write SQEs,
matching the writer count. Increasing peak depth is not what
recovers the batching in this experiment.

### Stalls remain

At eight writers, current throughput ranges from 4,945 to
15,571 puts/s and diagnostic throughput from 5,188 to 18,734.
Their p99 ranges are 1.324–17.604 ms and 1.017–18.062 ms.
At sixteen writers, current throughput ranges from 21,888 to
23,969 and diagnostic throughput from 10,016 to 28,584;
p99 ranges are 1.555–3.100 ms and 1.236–28.081 ms.

The eight-writer baseline stalls in round zero, while the diagnostic
stalls in round one. Both sixteen-writer diagnostic repeats stall
in round one. These failures are part of the experiment, not
excluded environmental outliers. Later rounds also show latency
drift. The test does not attribute the stalls to a specific
filesystem, SSD firmware, controller, or scheduler issue, and it
does not resolve the [existing investigation](rfc-024-wal-stall-diagnosis.md).
Four-writer rotation is outside this diagnostic's scope.

## Implementation implications and restoration

The isolated change establishes the cause of the increased sync
count and supports it as the principal software explanation for
the preceding throughput regression. The exact recovered throughput
and tail-latency benefit are not qualified by this unstable matrix.

The [current RFC](../rfcs/024-dedicated-wal-pipeline.md#one-ordered-durability-coordinator)
explicitly forbids new admission from extending the coalescing
cutoff. Consequently this diagnostic is not a production fix.
A future change should distinguish optional sync batching from
public barriers, preserve a fixed maximum wait and the captured
syscall acknowledgment target, and test late admission, deadline
expiry, poison, and shutdown. A broader guard matrix is still
needed before retaining a new batching policy.

Before scored timing, the original runtime SHA-256 was restored to
`435e4a6e9f5324b0e275fa02c784dc35e54cc6bab2571ffe50eb1e0de0c90aca`.
All 107 original Rust source hashes match. Rebuilding the normal
release executable reproduces arm A's exact SHA-256. The candidate
exists only as an archived diagnostic executable and patch.

Default-feature and all-feature workspace Clippy pass with
`-D warnings`. Native nextest passes 240 selected debug tests
(parallel WAL, MVCC, and serializable transactions) and 167 selected
release tests (parallel WAL and WAL), both with retries disabled.
These checks run before scored timing on the diagnostic; they do
not approve a change to the RFC's batching policy.

## Local evidence

The archive is
`target/rfc024-sync-cutoff-investigation-20261003-p6goh3nh/`.
It contains `protocol.json`, `source-manifest.json`,
`baseline-manifest.json`, `candidate-manifest.json`,
`diagnostic.patch`, both executable snapshots, `run.py`,
all stdout/stderr files, `runs.jsonl`, `blocks.jsonl`,
`summary.json`, `completion.json`, the four raw profiles,
validation logs, and `restoration.json`.

The independent `verify.py` reconstructs all 52 raw run records,
12 blocks, seven metrics, repeat controls, intervals, four
profile histograms, and 25,627 sync observations. It verifies
protocol and executable hashes, restored sources and normal
release executable, sequential timing with the fixed gaps, and
scratch cleanup. Results and artifact digests are in
`verification.json` and `artifact-manifest.json`.
