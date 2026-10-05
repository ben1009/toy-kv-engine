# RFC 024: Tokio client experiment, 2026-10-04

The Tokio blocking bridge did not establish a repeatable performance improvement.
An initial +5.8% paired throughput result failed all repeat controls; an independent
confirmation was -0.8%, with one passing control. Both runs showed substantially
more process context switches. No engine, WAL, dependency, or production API change
was made.

## What was tested

An isolated diagnostic crate used the retained engine sources at `ca4cfcca` and
the repository's Tokio version, 1.52.3. Every arm used the same release executable,
common batch construction/commit helper, keys, shared values, and unchanged
`WalIoMode::Parallel` implementation.

| Arm | Client execution |
| --- | --- |
| Threads | 16 OS threads, each running its full synchronous writer loop |
| Persistent | Four Tokio runtime workers; up to 16 blocking workers, one job per full writer loop |
| Tasks | 16 Tokio client tasks on four runtime workers; one `spawn_blocking` job per batch, with at most 16 blocking jobs |

The tasks arm used a semaphore, and each blocking closure owned its permit, engine
reference, and inputs until the synchronous commit finished. Every arm joined all
writers before runtime shutdown, flush, and close, including when a writer failed.

This is a blocking bridge, not task-level WAL durability waiting. `spawn_blocking`
occupies a blocking-pool thread until its job returns; the library's durability
and ordered publication waits remain synchronous. See the
[Tokio documentation](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
The existing async API's rejection of the parallel WAL selector remains unchanged.

## Protocol

Both comparisons ran natively outside the io_uring-restricted sandbox on
NVMe-backed ext4: 16 clients, batch64, 1 KiB values, 262,144 logical puts, a 1 GiB
SST target, PITR off, release with `bench`, and no detailed profile. Latency starts
after key preparation and covers batch-reference construction, commit, and reply;
the tasks arm includes blocking-pool handoff and task resumption. This probe's
absolute latency and throughput are not interchangeable with earlier `write-perf`
measurements from a different harness.

Executables ran from RAM, output was captured in memory, and five seconds idle
followed each run. Builds, tests, and tracing did not overlap scoring. Process CPU,
voluntary/involuntary switches, and minor faults are `RUSAGE_SELF` deltas over the
writer window, including WAL and Tokio threads.

The initial comparison had three warmups per arm and three fixed six-run blocks,
with two observations per arm per block. Orders were B/P/T/T/P/B, P/T/B/B/T/P,
and T/B/P/P/B/T. B means threads, P persistent, and T per-batch tasks.

Before collecting new observations, a separate confirmation was registered for
threads versus tasks: three warmups per arm and B/T/T/B, T/B/B/T, B/T/T/B blocks.
It used the same executable. Initial observations were preserved and were not
replaced by the confirmation.

The estimator is the median of three block-level geometric-mean ratios. Repeat
controls require both arms' within-block throughput spread at most 5% and p99
spread at most 10%. A positive screen requires at least +5% median throughput,
every primary block positive, at least two passing controls, and median p99 no
worse than +10%. It does not qualify production adoption or the full RFC gate.

The frozen retry rules allow one same-arm retry after an extra one-second pause
for an obvious regression, and one rotated whole-block retry for an obvious
paired regression. Originals remain primary. Neither comparison triggered a retry.

## Results

Changes below are paired block medians relative to threads in the same comparison.

| Comparison | Throughput | Batch p99 | CPU per put | Process switches per batch | Passing controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initial: persistent Tokio | +1.7% | -2.6% | +1.1% | +0.4% | 0/3 |
| Initial: per-batch Tokio tasks | +5.8% | -1.5% | +5.9% | +71.4% | 0/3 |
| Confirmation: per-batch Tokio tasks | -0.8% | +4.8% | +14.8% | +71.7% | 1/3 |

The initial tasks throughput changes were +5.8%, +4.2%, and +6.2%.
Confirmation changes were -0.81%, -1.28%, and -0.70%: all three were negative.
These observations do not establish either a stable gain or a precisely quantified
regression. The initial positive result did not reproduce.

Confirmation's median process switches per batch were 5.04 for threads and 8.63
for tasks. Software peak in-flight groups were 13–14 and 14–16, respectively;
neither path was limited to a single outstanding group. The additional context
switches and CPU work did not produce a confirmed throughput benefit.

No inference about a native cooperative WAL implementation follows from this
bridge test. Testing that architecture requires task-level durability and ordered
publication waits, with correct rotation, cancellation, and shutdown ownership.

## Validation and artifacts

The isolated crate passed formatting, Clippy with warnings denied, and native
nextest with two tests and zero skips. Those tests exercised all three schedulers,
uneven batches, clients with no work, visible results, and WAL reopen recovery.

All 45 observations were reconciled against saved stdout, requested mode, puts,
batch samples, completed CQEs, commit buffers, and reported throughput: 27 initial
observations and 18 confirmation observations. All 123 engine/build input hashes,
including 118 Rust files, matched their pre-experiment values. The normal release
benchmark executable was unchanged. Disposable databases and RAM staging were
removed.

Probe executable SHA-256:
`5e70490ccf8429cb38b57d300b04125867540cf2d3f8feaa37aa02aeb77e0639`.

The probe source, locked manifest, binary, protocols, raw outputs, summaries,
and check logs are under `target/rfc024-tokio-client-ca4cfcca-20261004/`.
The independent confirmation is in its `confirmation/` subdirectory.
