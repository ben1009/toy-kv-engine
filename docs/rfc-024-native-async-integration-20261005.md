# RFC 024: native async point-write integration

Ordinary v4 parallel WAL point writes now use cooperative waits through the
public `put_async`, `delete_async`, and `write_batch_async` APIs. This replaces
the restricted [native-wait prototype](rfc-024-native-async-waits-20261005.md)
with engine lifecycle, memtable freeze, rotation, and cancellation support.
Parallel WAL is now the default for ordinary v4 WALs; PITR and legacy formats
retain their existing paths. See the [default adoption note](rfc-024-parallel-wal-default-20261005.md).
The RFC performance adoption gate is still unqualified.

The [October 5 direct leader comparison](rfc-024-native-async-vs-leader-20261005.md)
measures reviewed revision `d7d124b8` at 1, 4, 8, and 16 writers for one put
and 64 puts per commit. It records peak results, paired estimates, CPU cost,
pipeline depth, and repeatability separately. The integration measurements
below use earlier inputs and parallel-WAL controls; they remain historical.
The [October 6 batch64 rerun](rfc-024-native-batch64-leader-rerun-20261006.md)
measures the retained completion-drain and cutoff-refresh backend; it also
leaves the full performance gate unqualified.

## Commit and lifetime rules

The ordinary non-serializable point path is:

```text
owned entries and lifecycle admission
  -> fair native preparation permit
  -> memtable lease
  -> await DirectBuf budget and allocate
  -> shared sequencer: timestamp + v4 encoding + WAL ticket + enqueue
  -> release preparation permit
  -> dedicated ordered packer and concurrent io_uring writes
  -> await contiguous WAL durable prefix
  -> insert memtable entries
  -> await contiguous MVCC publication prefix
  -> release memtable lease and return
```

The preparation permit carries no timestamp or WAL ticket and is acquired
before the memtable lease. It serializes native preparation, while admitted
groups continue through I/O concurrently. Native writers try the shared
sequencer lock: if a synchronous writer owns it, they release the prepared
buffer and retry without consuming a timestamp. This avoids a cycle between
a synchronous writer waiting for buffer budget and a native writer retaining
that budget while waiting for the sequencer.

An owned task retains the public engine handle, entries, and lifecycle
admission after caller cancellation. Close drains that owner. Destruction of
an admitted task poisons an unknown commit; it preserves a prefix already made
visible by a predecessor. Notifications are registered before locked predicate
checks. Kernel buffer ownership still ends only at terminal write completion;
the dedicated worker and sync coordinator retain their existing teardown rules.

Memtable leases make freeze, checkpoint, PITR successor installation, and GC
CAS wait for pending publication without holding a blocking read guard across
an await. WAL-full retry releases the lease, performs rotation on the bounded
blocking executor, and reserves a fresh timestamp on the successor.

After those leases drain, freeze and checkpoint reject a poisoned publication
sequencer, including when the active memtable is empty. A healthy WAL does not
make a hidden successor commit safe to persist in an SST or checkpoint. The
active WAL remains available for recovery, and close still drains its runtime.

Range batches, transaction commits, and actual PITR v5/v6 WALs use the bounded
blocking path. Async transaction commit now owns the synchronous commit
protocol: it claims the attempt before collecting writes/OCC sets, retains the
snapshot through cancellation and retry, and reruns OCC after rotation. A
discarded unpolled commit future leaves the original transaction usable.
Local mutations and commit claims share a per-transaction mutex, so a checked
mutation finishes before input capture. Accepted reads and cursors retain the
actual snapshot pin independently of the transaction's commit-time reference.

Blocking async reads and maintenance closures retain lifecycle admission until
they finish. Async close shares one engine-owned shutdown thread independently
of the caller's Tokio blocking pool. Pending freeze and rotation can still use
that pool while close drains their owners. Close saves its terminal result,
including errors, for subsequent callers; a rejected precondition that leaves
admission open permits a later retry. A background join failure still allows
safe WAL teardown.

For a non-PITR engine moved out of its original `Arc`, shutdown retains shared
storage and worker handles independently. Dropping its caller leaves the owned
shutdown running until it has drained work and saved the final result. Async
close exposes `AsyncCloseErrorKind` even when a synchronous caller settled close
first: that caller receives its original error, while subsequent callers use a
cached classification and formatted chain. An async shutdown owner instead
retains the original error chain for its waiters.

Point writes from a `KvEngine` moved out of its constructor `Arc`, or rewrapped
in another `Arc`, use the owned blocking compatibility path. Their lifecycle
admission and storage stay owned until completion; losing the original weak
public owner does not make a valid handle unusable.

## Ext4 measurements

Fresh databases on `/dev/nvme0n1p3`, ext4, 16 clients, 64 puts per batch,
1 KiB values, 262,144 puts, ordinary v4 parallel WAL, no serializable mode or
PITR. The native arm uses eight Tokio runtime workers. A 1 GiB memtable target
keeps this performance workload within one active memtable; rotation has
separate correctness coverage. The configured limits remain 32 in-flight
groups and 256 ring entries.

Three predefined mirrored/rotated blocks contain two measurements per arm,
after three warmups per arm. Executables are staged in RAM. Each observation
flushes and closes outside the measured writer window, removes only its own
database, and idles for five seconds. No compilation, tracing, or tests run
concurrently. Obvious regressions trigger an additional one-second pause and
separately labelled retry; original observations always remain primary.

The retained reference is the unchanged `ca4cfcca` parallel-WAL executable.
The integrated synchronous and native arms use the same measured binary and
public engine API. Its archived inputs precede the shutdown, transaction, and
checkpoint-publication review fixes described above. The later
[direct leader study](rfc-024-native-async-vs-leader-20261005.md) includes those
fixes with a different control; it does not repeat this three-arm study.
Reference comparisons include packing and lifecycle
changes; they do not isolate async waits. Each paired estimate below is the median of three fixed
block geometric-mean ratios.

| Native async compared with | Throughput | Batch p99 | CPU per put | Switches per batch | Repeat controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Integrated synchronous parallel API | +13.7% | -8.6% | -42.6% | +152.7% | 0/3 |
| Retained parallel WAL reference | +13.6% | -5.2% | -38.1% | +212.7% | 1/3 |

Native/synchronous throughput gains by block were +13.7%, +20.1%, and +13.7%;
p99 improved in every block. Repeat controls require both repeats of both arms
within 5% throughput and 10% p99. These controls failed, so the positive screen
did not pass. The integrated synchronous/reference paired throughput change
was -1.8%, with +3.8% p99 and no passing repeat controls. Preserve this possible
synchronous-path cost when evaluating the dedicated packer.

Descriptive medians of six primary observations per arm are separate from the
paired estimator:

| Arm | Puts/s | Batch p99 | CPU µs/put | Switches/batch | Peak in-flight groups |
| --- | ---: | ---: | ---: | ---: | ---: |
| Retained parallel reference | 536,290 | 3.206 ms | 9.912 | 5.036 | 13–16 |
| Integrated synchronous API | 517,224 | 3.405 ms | 10.564 | 6.211 | 12–15 |
| Integrated native async API | 603,677 | 3.059 ms | 6.152 | 15.769 | 15–16 |

Every measured arm reconciled 4,096 batch samples, buffers, and write CQEs.
Native peak outstanding write SQEs were 15–16. These are software counters;
device queue depth was not measured. This workload has only 16 logical commits
outstanding and cannot demonstrate use of all 32 group slots. Native median
`fdatasync` time per call was 501 µs, versus 431 µs for integrated synchronous
and 408 µs for the reference. The observed gain does not come from a faster
sync syscall. Switch counts rose despite lower CPU use; counts alone do not
establish context switching as the main bottleneck.

The first integration run, before fair preparation, showed +13.4% throughput
but +76.7% p99 against the integrated synchronous control. Unrestricted
try-lock retries repeatedly discard native prepared buffers and can starve
tasks. The fair permit removes native-on-native retries, while retaining the
mixed synchronous-writer safety check. Its separate rerun preserved throughput
and lower p99. The datasets are not pooled, and this comparison does not prove
that preparation fairness explains every latency difference. The first run
retains one same-arm retry and 18 block-retry observations, including severe
slow intervals; the fair run triggered no retries.

At measurement time, these results supported keeping the integration opt-in
for further review. They
do not qualify a stable production gain: this historical study has no leader
workload matrix or passing repeatability screen. The later direct leader matrix
also leaves the full RFC adoption gate unqualified.

## Validation

The integration review passes `cargo make check`: formatting, dependency order,
Clippy with default and all features using `-D warnings`, unused-dependency and
typo checks, and 1,457 tests with no skips. A default-feature nextest selection
passes 90 async API, native-write, and memtable-gate tests. AddressSanitizer
passes 24 native async, 21 async-transaction, and one shutdown-precondition test
with failpoints excluded; leak detection is disabled for these address checks.
The public-API probe passes strict Clippy
and three tests covering remainder batches, idle clients, flush, and recovery.

Regression cases include a current-thread executor with every blocking slot
occupied, caller cancellation, last-handle drop, task destruction before and
after WAL durability, publication poison, checkpoint drain, queued preparation
during freeze, invalid input without ticket holes, mixed sync/native budget
pressure, automatic rotation with value separation, WAL-full OCC retry, PITR
v5/v6 successor writes, cancelled blocking maintenance/read/transaction/close,
and terminal shutdown errors. Review regressions also cover concurrent close
with one caller blocking thread, checked mutations racing commit dispatch,
accepted reads/cursors retaining their pins across commit, and first-poll OCC
registration after a real recoverable rotation failure. Existing crash-prefix
and buffer-lifetime tests also run in the full suite.

A subsequent shutdown review passes `cargo make check` with all 1,462 tests
and no skips. Added regressions preserve I/O error classification after a
synchronous close and verify graceful close after moving the engine out of its
original `Arc`, including caller cancellation and drop. The async WAL frontier
test now handles unavailable `io_uring` consistently with the other WAL tests;
it executes on the unrestricted host during this check.

The checkpoint-publication review passes `cargo make check` with all 1,464 tests
and no skips. One existing simple-leveled compaction integration test timed out
once and passed on its next attempt. Both new checkpoint regressions fail on
the old source and pass with the fix: cancellation while capture waits for
writers cannot persist a hidden successor, and an empty poisoned memtable
cannot bypass rejection. Failed freeze leaves the old WAL available; explicit
close still drains its runtime, and reopening replays the complete WAL history.
Scoped subagent verification found no further issue in the fix.

## Reproduction artifacts

- Fair-run protocol: `target/rfc024-native-async-integration-20261005/fair-comparison/protocol.json`, raw observations: `target/rfc024-native-async-integration-20261005/fair-comparison/records.json`, analysis: `target/rfc024-native-async-integration-20261005/fair-comparison/analysis.json`, and input integrity: `target/rfc024-native-async-integration-20261005/fair-comparison/final-integrity.json`.
- First-run protocol: `target/rfc024-native-async-integration-20261005/comparison/protocol.json`, observations including retries: `target/rfc024-native-async-integration-20261005/comparison/records.json`, and analysis: `target/rfc024-native-async-integration-20261005/comparison/analysis.json`.
- Public-API probe: `target/rfc024-native-async-integration-20261005/probe/src/main.rs`, runner: `target/rfc024-native-async-integration-20261005/fair-comparison/run.py`, and compiled input hashes: `target/rfc024-native-async-integration-20261005/fair-comparison/compiled-input-hashes.json`.
- Full local check after review: `target/rfc024-native-async-integration-20261005/full-check-review.log`, default-feature tests: `target/rfc024-native-async-integration-20261005/default-tests-review.log`, native AddressSanitizer: `target/rfc024-native-async-integration-20261005/asan-native-review.log`, transaction AddressSanitizer: `target/rfc024-native-async-integration-20261005/asan-transaction-review.log`, and shutdown AddressSanitizer: `target/rfc024-native-async-integration-20261005/asan-close-review.log`.
- Review validation and source hashes: `target/rfc024-native-async-integration-20261005/review-validation.json`, probe Clippy: `target/rfc024-native-async-integration-20261005/probe-clippy-review.log`, and probe tests: `target/rfc024-native-async-integration-20261005/probe-tests-review.log`.
- Full local check after checkpoint-publication review: `target/rfc024-native-async-integration-20261005/round4-check.log`.

Artifacts are local and ignored by Git. Both executable versions and all
observations are preserved. Source hashes remained unchanged during each run;
the normal `write-perf` binary retains SHA-256
`72891a06445b1e08a5ec187b4cbed8c41cbf0377c80b0e2aa5fc4748c40b0eb3`.
