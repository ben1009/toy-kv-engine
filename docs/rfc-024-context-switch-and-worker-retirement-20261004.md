# RFC 024: context switches and worker retirement, 2026-10-04

The measurements do not establish context-switch overhead as the main bottleneck.
They show substantial resource waiting, including futex waits and ext4 journal
commit waits. A separate worker-bookkeeping optimization did not pass its frozen
retention rule and was reverted. No new production optimization is retained.

## Context-switch diagnostic

The saved retained executable at `ca4cfcca` ran natively with io_uring on
NVMe-backed ext4: 16 writers, batch64, 1 KiB values, 262,144 logical puts, a 1 GiB
SST target, PITR off, release with `bench`, and no `--profile`.

A diagnostic `LD_PRELOAD` wrapper measured `RUSAGE_THREAD` at pthread entry and
return, plus monotonic lifetime and thread CPU time. Two warmups preceded four
P/L/L/P runs; one additional run per mode sampled thread state and wait locations
every 10 ms. Executables and the wrapper were staged in RAM. These instrumented
runs are separate from optimization scores.

| Writer metric | Retained parallel | Leader |
| --- | ---: | ---: |
| CPU time / combined writer thread lifetime | 30.0% | 31.6% |
| Voluntary switches per batch | 2.94 | 3.34 |
| Involuntary switches per batch | 0.067 | 0.091 |

Values are medians of the two unsampled diagnostic runs per mode. Combined writer
thread lifetime sums the lifetimes of 16 concurrent threads; it is not benchmark
wall time. Approximately 70% off CPU therefore does not mean 70% of benchmark time
was spent performing context switches. Voluntary switches normally reflect waits
for resources; counts do not measure their duration or switching overhead.
See the [Linux getrusage documentation](https://man7.org/linux/man-pages/man2/getrusage.2.html).

The sampled parallel run captured 643 writer observations: 422 showed sleeping
in `futex_wait_queue`, 38 showed sleeping elsewhere or an unresolved wait location,
and 183 showed runnable state. Of 42 sync-coordinator observations, 24 reported
`jbd2_log_wait_commit`. These are sparse occupancy hints. Runnable state includes
both running and waiting to run; reading state and wait location is not atomic.
Neither these observations nor switch counts quantify scheduling overhead.

WAL threads have additional switches and are excluded from the writer table.
Their entry-to-return counters include setup and shutdown, whereas state samples
were filtered to the writer window. The main thread and subsequent pthread TLS
destructors are excluded. Logging at thread exit and sampling can perturb timing.

The host's `kernel.sched_schedstats` was `0`. No claim about exact runnable-queue
delay is made from disabled counters, and no global scheduler setting was changed.
The current evidence supports investigating WAL durability and ordered publication
waits; establishing actual context-switch CPU cost requires further tracing.

## Rejected completion candidate

The candidate retired only the successful CQE's own completed group and reserved
space for its two possible result events. This removed the success-path scan of
all active groups and temporary completion vectors. Poison cleanup retained the
full scan. Buffer ownership, submission, sync policy, and MVCC publication were
unchanged. A new test covered out-of-order retirement of multi-request groups,
stale CQE rejection, buffer release, and reuse of the freed group slot.

Native `cargo make check` passed, including default and all-feature Clippy and
all 1,422 tests, with zero skips.

The comparison used three warmups per arm and three fixed six-run primary blocks
of retained parallel B, candidate parallel C, and unchanged leader L:
B/C/L/L/C/B, C/L/B/B/L/C, and L/B/C/C/B/L. Each arm had two observations per block.
Five seconds idle followed each run. No builds, tests, or tracing overlapped the
scored runs.

Retention required at least +5% paired median target throughput, every original
target block positive, at least two passing repeat controls, and independent
guards. Controls required at most 5% throughput spread and 10% p99 spread within
each comparison arm. Target throughput changes were +0.26%, -94.54%, and +4.88%.
The paired median was +0.26% throughput and +3.46% p99, with zero passing controls.
These observations establish no repeatable gain or causal candidate regression.

The stalled candidate runs spent 7.41 and 9.09 seconds in accumulated `fdatasync`,
versus about 0.28 seconds in a normal candidate run. Their elapsed times were
8.14 and 10.00 seconds. One same-arm retry remained stalled; another returned to
normal. Leader also had slow runs with 63–70 ms batch p99.

Obvious regressions received one same-arm retry after an extra one-second pause.
An obvious paired regression also received one full rotated-block retry. Originals
remained primary. The rotated retry was approximately tied with retained parallel
(+0.01% throughput), with no passing control. All 36 observations remain archived:
nine warmups, 18 primary runs, three single retries, and six block-retry runs.

All raw records were checked against saved stdout, mode, operation count, batch
samples, completed write CQEs, commit buffers, and reported throughput. All 118
Rust sources were restored to the retained manifest, and the normal release
benchmark executable was rebuilt. Disposable databases and RAM staging directories
were removed.

Executable SHA-256 for the retained baseline:
`72891a06445b1e08a5ec187b4cbed8c41cbf0377c80b0e2aa5fc4748c40b0eb3`.

Candidate SHA-256:
`c22788090e8634090a989dab78dfcc123b158eac18ad91bcddc96cd74fd7c01e`.

Protocols, binaries, source manifests, patch, raw observations, summaries, and check
logs are in `target/rfc024-worker-direct-retirement-ca4cfcca-20261004/`.
The probe source, shared library, thread counters, and state samples are in its
`context-switch-diagnostic/` subdirectory.
