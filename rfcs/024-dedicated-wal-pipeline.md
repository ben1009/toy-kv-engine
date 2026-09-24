# RFC 024: Dedicated WAL I/O Pipeline

| Field | Value |
| --- | --- |
| Status | Proposal (benchmark gated) |
| Date | 2026-09-24 |
| Author | kv-engine Contributors |

## Summary

After the PITR commit-sequencer work, the PITR-disabled, four-writer
`wal_concurrent` workload regressed by about 10% on tmpfs against the pre-PITR
revision. Its writers now reach the WAL in a different pattern because every
commit must publish behind earlier timestamps. The MVCC WAL still submits
multiple `O_DIRECT` write SQEs *within* one group, but its elected leader waits
for that group's writes and `fdatasync` before the next group can submit.
This RFC proposes a dedicated logging pipeline with a thin ordered packer,
several ticket-ordered groups writing concurrently, and one contiguous
durability frontier. Clients enqueue and wait; they no longer elect a WAL
leader.

The first implementation would apply to ordinary v4 MVCC WALs, which are the
path on which that regression was measured. PITR v5/v6 WALs would continue
using the current path: their strict recovery parser rejects a valid batch
after an invalid earlier batch, a layout concurrent groups could create after
a crash. A separate format and recovery decision is required before enabling
overlap for PITR segments.

Parallel submission is a *hypothesis*, not an established fix. A later
standalone no-leader experiment was neutral at four writers and worse at eight
on tmpfs. The ordered publication wait also remains after this change, so
success must be measured end to end against the current WAL and the pre-PITR
baseline, not inferred from time spent in WAL follower waits.

## Regression and scope

[The PITR performance study](../docs/pitr-performance.md) compared 25
interleaved `wal_concurrent` runs: the pre-PITR revision `2f556ccb` had a
177,411 ops/s median, and the post-PITR head `494a20ab` had 159,336 ops/s,
or 10.2% less. Both legs had PITR **disabled**, four writers, 1 KiB values,
200,000 individual puts, and a tmpfs WAL. Solo commit groups rose from 10,859
to 26,735 while the total 200,000 buffers and 819,200,000 aligned WAL bytes
were unchanged. The study attributes the change to ordered publication
dephasing writers, but did not bisect the PITR series, so the exact introducing
commit remains an attribution rather than an isolated result.

The publication-spin fix later improved its unfixed head in paired runs, but
its remaining gap against the pre-PITR revision varied by session. On a real
disk the surveyed one-writer WAL path was 1,745 versus 1,741 ops/s, within
noise, because per-commit sync dominated. These facts define the target:
recover lost throughput for concurrent small puts on the default v4 path
without weakening the ordering required by [RFC 023](023-point-in-time-recovery.md).
They do not establish a device-backed WAL regression or a PITR-on regression
that parallel submission will fix.

The commit sequence in [`LsmStorageInner::put`](../kv-engine/src/lsm_storage.rs)
is timestamp reservation and WAL enqueue, WAL ticket durability, memtable
publication, then `publish_commit_ts`. The last step waits for earlier
reservations through the frontier in [`mvcc.rs`](../kv-engine/src/mvcc.rs).
The WAL pipeline owns the written and durable ticket frontiers; the MVCC
sequencer owns the later publication frontier. This RFC changes only WAL
scheduling. It does not let a later timestamp become visible early, and it
cannot remove a wait for an earlier skiplist insertion.
The experiment must therefore measure `wal_concurrent` end to end, including
commit-group formation and publication wait, before claiming to address the
regression.

## Current design and evidence

[`Wal::put_batch`](../kv-engine/src/wal.rs) and the PITR batch encoder prepare
4096-byte-aligned buffers, then assign monotonically increasing tickets under
the pending-queue mutex. The pool starts with 64 buffers of 256 KiB each;
larger batches can allocate larger buffers. `submit_and_commit` elects one
leader through `submitting.compare_exchange`. The leader drains the queue in
ticket order, preallocates space in 1 MiB increments with `fallocate` (or
`ftruncate` for `EOPNOTSUPP`/`ENOSYS`), assigns aligned offsets, and submits
write SQEs through one registered 256-entry io_uring ring. It waits and reaps
each chunk, calls `fdatasync(2)` once, then publishes `durable_ticket` and wakes
followers. The next group cannot start its I/O until this barrier releases.
Regular MVCC WALs use the v4 batch format: a 20-byte header carries the commit
timestamp, entry count, entry CRC32, and data length, and each batch is padded
to 4096 bytes. PITR uses the versioned format specified in
[RFC 023](023-point-in-time-recovery.md). MVCC WAL creation
or reopen fails if io_uring or `O_DIRECT` initialization fails. Legacy
non-MVCC WALs use buffered I/O.

On a four-writer tmpfs `wal_concurrent` run, the profile recorded 596.53 ms in
`wal_submit`, 17.37 ms in `fdatasync`, and 65% of aggregate thread time in
follower waits. That made the leader a plausible target, but wall-clock
throughput did not follow thread-time attribution. A standalone design where
every writer submitted, reaped, and synced its own group measured 0.99x the
current shape at four writers and 0.73x at eight. `IO_DRAIN` sync variants were
also slower. An in-engine variant that kept the leader serving up to eight
successive groups measured 0.982x the control in 20 paired tmpfs runs, with
overlapping interquartile ranges. These results do not cover a physical
storage device, and the no-leader design lost the current group's sync
coalescing. Four synchronous writers can have at most four operations awaiting
durability, so this regression workload cannot exercise eight groups in
flight; eight is a ceiling for higher-concurrency workloads. The earlier
[single-operation io_uring benchmark](../docs/io-uring-bench.md) likewise does
not measure the batched WAL path. The
[CRUD benchmark report](../docs/bench-report-crud-bench-fjall.md) compares named
historical revisions; it does not isolate concurrent commit groups.

RFC 012 preserves the original proposal. Its no-leader, `IOSQE_IO_DRAIN`, and
buffered-fallback sketches are not the shipped implementation. This RFC uses
the current implementation and the later measurements as its baseline.

[SpanDB](https://www.usenix.org/system/files/fast21-chen-hao.pdf) provides the
architectural reference, not an expected speedup. It takes WAL work from a
queue with dedicated loggers and permits multiple groups in flight; its
`2L4R` example has two loggers with four outstanding requests each. Its raw
NVMe/SPDK log uses atomic 4 KiB pages, log tag numbers, and sequence-based
replay. This engine writes a filesystem file with `O_DIRECT` and
`fdatasync`, and PITR binds physical batches into a seal and archive chain.
The reusable idea is a dedicated, continuously fed logging pipeline, not
SpanDB's page format or its durability and recovery protocol.

## Goals

1. Test whether overlapping writes from several ticket-ordered WAL groups
   recovers the measured post-PITR throughput loss on the default v4 path,
   while keeping file offsets non-overlapping and in ticket order.
2. Advance one contiguous durable ticket frontier only after all writes in
   its prefix have completed and a subsequent `fdatasync` has succeeded.
3. Preserve opportunistic sync coalescing: one `fdatasync` may make several
   completed groups durable.
4. Bound ring, buffer, queued-batch, and reserved-file-space use under slow or
   failed I/O.
5. Keep v4 encoding and recovery compatible, including recovery after a later
   group reaches disk before an earlier group; preserve ordered MVCC
   publication and PITR barriers.

The proposal does not change the public synchronous API, add WAL shards,
replace direct I/O, use `IORING_OP_FSYNC` or `IOSQE_IO_DRAIN`, or enable
concurrent groups for PITR. It does not add a buffered fallback for MVCC WALs.

## Proposed protocol

### Ticketed queue and ordered packer

Writers keep the existing encoding, then enqueue the owned buffer and wait
for their ticket; no client becomes a WAL leader or polls a ring. A dedicated
WAL I/O worker consumes the queue even if every client is waiting. The first
implementation may keep the current short `pending` mutex: SpanDB's
lock-free queue is a benchmarked option, not a correctness requirement here.
Ticket assignment and insertion into the queue must remain one ordered
operation; a separately reserved ticket that arrives late cannot let a
higher ticket leapfrog it.

Preparation and WAL admission are distinct phases:

1. **Buffer admission, without a ticket:** validate the batch size, reserve
   resident-buffer budget, obtain a suitable `DirectBuf` from the pool or
   allocator, and finish encoding outside the admission mutex. Charge the
   actual capacity before encoding if a recycled buffer is larger than
   expected. Failed allocation or encoding releases its budget reservation;
   neither failure leaves a ticket or file-range hole.
2. **WAL admission, under the mutex:** recheck that this WAL is open and not
   poisoned and that the ready batch fits at the current `admitted_end`. If
   not, unlock and release its buffer and budget without consuming a ticket;
   return the appropriate `WAL full`, closed, or terminal error. Otherwise,
   assign the next ticket, advance `admitted_end`, and enqueue the buffer
   before unlocking. Ownership of the buffer and its budget reservation
   transfers to the queue.

The admission queue state is the linearization point for writes and barriers.
`admitted_end` bounds the file size at WAL admission;
the packer's `reserved_end` assigns the actual offsets to those tickets later.
`sync()` takes that mutex to capture the last assigned ticket, then releases
it before waiting for durability. Close and rotation take the same mutex to
stop admission and capture their final ticket. Thus a write is wholly before
or after each cutoff, including when it races ticket assignment. Rotation
drains the old WAL through that ticket before installing the successor; a
writer cannot be assigned to both files.

An ordered packer drains contiguous tickets, forms groups, and assigns aligned
physical offsets in ticket order. This is the thin serialization point. Under
the queue/offset lock it reserves the logical file range and advances
`reserved_end`, then releases the lock. The WAL worker ensures the required
1 MiB extent is preallocated before submitting writes to that range; it may
advance a separate `preallocated_end` under worker-side serialization. Neither
`fallocate` nor its `ftruncate` fallback runs under the producer queue mutex.
The packer never waits for write completion or sync while holding that mutex.
Offset order must follow ticket order even if workers are scheduled
differently; a free-running `fetch_add` by each logger would not guarantee
that. A group descriptor records its ticket interval, file range, outstanding
buffer references, and state (`assigned`, `writing`, `written`, `durable`, or
`failed`). A group never contains a ticket gap.

Buffer admission reserves **resident `DirectBuf` capacity**, not just encoded WAL
bytes, against an initial 64 MiB queued-and-in-flight budget, plus a separate
initial limit of 256 queued batches. Today even a 4 KiB write normally owns
a 256 KiB pooled buffer, and a pool miss allocates another 256 KiB buffer;
charging only 4 KiB would allow gigabytes of resident buffers. Reserve before allocating,
charge the actual capacity of any recycled oversized buffer, and account for
the fixed pool and ring memory separately. A single valid larger batch may
occupy the dynamic budget exclusively. The worker is independent of blocked
producers, so backpressure cannot wait for an uncalled `submit_and_commit`.
Pressure also wakes the packer; it must not rely on a client arriving to
trigger dispatch.
Release each buffer's budget when its full-length write CQE confirms that the
kernel has finished reading it. Recycle its `DirectBuf` immediately, even if
the group is still waiting for `fdatasync`; the group descriptor retains only
ticket, file-range, completion, and error metadata until it is durable or
failed. An ambiguous submission or completion retains the buffer until kernel
ownership is resolved. The v4 file-size limit is checked before offset
reservation; any later PITR implementation must likewise check its segment
and spool limits before reservation.

The queue lock accounts for the aligned end of every accepted ticket.
Admission first checks whether the batch fits in an *empty* v4 WAL, including
its 4 KiB file header, 4 KiB batch alignment, and the rounded preallocation
extent. If even that exceeds the 1 GiB recovery cap, return a terminal
`batch too large for WAL` error; rotation cannot make it fit. The current
per-batch payload check is not sufficient for this decision.

If a batch fits an empty WAL but would cross the *current* file cap,
admission returns a distinct retryable `WAL full` result **before assigning
a ticket**. The engine must then drop its current
`active_memtable_lock` read guard, freeze and drain the old memtable/WAL,
install the successor, and retry the write with a fresh reservation. This is
an engine change, not a task the I/O worker can wait on: current writers hold
that read guard through `commit_wal_ticket`, while freeze needs the write
guard. The existing MVCC error path can retire a timestamp when WAL admission
fails, but callers need an explicit capacity-retry path. Other errors must
not be retried as a capacity event.

### Dedicated I/O with several groups in flight

Start with **one dedicated WAL I/O thread, one registered 256-entry io_uring,
and up to eight outstanding groups**. This tests the useful part of SpanDB's
`2L4R` shape without assuming that two OS threads are needed for io_uring.
The worker submits SQEs from more than one group before waiting for all of
any one group's CQEs. It tags each SQE with its group and buffer identity,
tracks partial submission, and reaps completions across groups. It waits for
events only when there is no runnable dispatch or completion work; it must not
call the current `submit_and_wait(group_len)` for each group, which would
recreate the serial group barrier. The 256-SQE limit applies across all
in-flight groups, and oversized groups are chunked without freeing a buffer
until its own CQE has been accounted for.

If one worker or ring cannot keep the device fed, a second measured phase may
use two logger workers with separate registered rings and four outstanding
groups per worker. The same ordered packer assigns ranges before either
worker writes; a worker owns its CQEs and buffers. The total group and byte
bounds remain eight and 64 MiB. The first ring stays mandatory for MVCC WALs;
an unavailable extra ring reduces overlap and must be reported, not silently
treated as a parallel result.

A group becomes `written` only after every submitted CQE reports the full
aligned write length. A short write, negative CQE, or ambiguous submission
result poisons the WAL. On an ambiguous result, the worker must retain any
buffer the kernel could still read; it may not return it to the pool merely
because a syscall failed. Shutdown must drain or cancel and confirm every
possibly submitted request before reclaiming its buffer. If completion cannot
be established, it must retain those buffers rather than free them, fail
`close()`, and never report the affected tickets durable.

This ownership rule also applies after `close()` returns an error and the
`Wal` handle is dropped. The dedicated worker owns the ring, file descriptor,
and all submitted buffers until it has consumed terminal CQEs or torn down
the ring with a guarantee that the kernel no longer references them. Normal
shutdown joins that worker before freeing its buffers. An unsuccessful
shutdown must transfer unresolved buffers to ownership that outlives the
`Wal` handle, retaining the worker, ring, and descriptor as needed. If kernel
ownership cannot be proven released, it must deliberately leak that state
and its buffer allocations rather than run `DirectBuf::drop`, which calls
`free`. No `Wal` destruction path may free a possibly kernel-owned `DirectBuf`.

### One ordered durability coordinator

Write completion is not durability. A dedicated sync coordinator tracks the
largest contiguous `written` group prefix, independently of completion order.
When that frontier moves beyond the durable frontier, it captures the last
ticket in the prefix and calls `fdatasync(2)` on the WAL descriptor. There is
at most one sync in flight. The I/O worker continues submitting later groups
while that call runs; this is why the coordinator cannot block the ring worker
on `fdatasync`. Whether a filesystem lets the sync overlap later direct
writes efficiently is a device-backed measurement, not a guarantee of this
design. The coordinator may coalesce several completed groups into one call but
does not add a fixed batching delay: the measured widened solo-leader window
reduced throughput.

On successful sync, the coordinator advances `durable_ticket` to one past the
captured ticket and wakes only tickets now covered. A later group may also
reach the device during that sync, but it is not acknowledged unless its
writes were completed before the coordinator captured the target; it needs a
later sync decision. The syscall may persist more bytes than the captured
prefix, so this is an acknowledgement rule, not a claim about the exact
physical bytes flushed. If the written frontier advanced during the call,
the coordinator immediately considers another sync. If `fdatasync` fails,
it poisons the WAL and fails all unacknowledged tickets without retracting
an already acknowledged durable prefix.

`sync()` and `close()` use the admission mutex cutoff defined above. `sync()`
waits until the durable frontier passes its captured ticket. `close()` drains
queue, rings, sync, and seal work through its captured final ticket, then
releases the file. On a poisoned WAL, close still resolves or retains
in-flight buffer ownership before tearing down the ring and descriptor. An
empty `sync()` remains a no-op.

```text
clients: encode -> ticketed queue -> wait for durable ticket
                           |
                  ordered packer / offset allocator
                           |
              WAL I/O worker: up to 8 groups in flight
                           |
                   contiguous written frontier
                           |
               sync coordinator: one fdatasync
                           |
                   contiguous durable frontier
                           |
              wake clients -> ordered MVCC publication
```

## Ordering, recovery, and errors

Group write completion may be out of order. The durable frontier must never
skip a group: if group 2 finishes first, its writers remain blocked until
group 1 has also completed and a sync covers both. A failed write group
poisons the WAL, stops new admission, and sets `poison_ticket` to the first
ticket of the earliest failed group. If an earlier group subsequently fails,
the boundary moves back to that group's first ticket; it never moves forward.
Tickets already acknowledged durable remain successful and cannot be
retracted. Any contiguous written prefix strictly before `poison_ticket` may
still be synchronized and acknowledged, even if no sync was in flight when
the later group failed. Tickets at or after `poison_ticket` fail, even if some
later writes physically completed; `durable_ticket` must never advance past
`poison_ticket`. WAL failure notifications and frontier advancement must be
ordered under the same state lock.

For example, if groups 1 and 2 are written, group 3 fails, and group 4 is
written, the coordinator may still sync and acknowledge groups 1 and 2,
whether or not their sync had started before the failure. Groups 3 and 4
cannot be acknowledged. A failed `fdatasync` leaves its covered tickets
unacknowledged because the durability outcome is unknown; reopen and recovery
determine which complete records are present.

WAL durability and MVCC publication are separate stages. A writer whose WAL
ticket was acknowledged durable may still insert into the memtable and
publish its commit timestamp after a later WAL group poisons the WAL, as long
as its timestamp precedes the MVCC sequencer's `poisoned_at` boundary. The
WAL must not fail that writer merely because it has not yet published.

The v4 recovery scanner accepts only a contiguous sequence of valid framed,
CRC-checked batches and truncates at the first invalid or zero-filled batch.
Later groups may have written beyond that point, but none can have been
acknowledged after the missing group because the live durable frontier is
contiguous. Recovery may include complete unacknowledged batches as an
unknown-outcome suffix, as the current WAL can; it must not lose an
acknowledged batch or replay a batch after a gap. The implementation must test
this case with a later group reaching disk first and a crash before the earlier
group finishes.

The existing recovery reader rejects WAL files whose physical length exceeds
1 GiB. Before rollout, admission must bound both the reserved logical end
and the rounded-up `fallocate` length to that limit. The current per-batch
size check is insufficient to enforce this file-level limit, especially with
several groups pending. Rotation must stop admission to the old WAL, drain
its assigned tickets, and only then let new tickets use the successor WAL;
the packer must never block those tickets while waiting for rotation.

PITR WALs cannot use that v4 truncation rule. Their v5/v6-family reader checks
for a valid later batch after a damaged one and reports corruption instead of
silently discarding later data. The seal builder also walks and validates the
whole segment in physical order. For example, if groups A and C are complete
on disk while B has a torn or zero-filled page, simply saying "stop at B"
would change the v5/v6 corruption contract: current recovery calls
`has_valid_v5_batch_after` and fails when it finds C. An in-memory
`written_frontier` or ordered hash cannot repair that after a crash. PITR
therefore stays on the current group-submit path in this RFC. Enabling it
later requires a persisted durable-prefix or equivalent on-disk generation
rule that can distinguish an unacknowledged out-of-order suffix from actual
corruption, plus tests for seal digest, rotation, archive, restore, and
publication barriers. The historical v5 and v6 formats must keep their
existing recovery meaning.

Two possible follow-on protocols are a new WAL format with an authenticated
durable-prefix boundary, or a separately synced sidecar that names the last
acknowledged byte and ticket. Neither is free: a marker must be made durable
*after* its covered WAL writes and *before* acknowledgements, and recovery
must distinguish a stale marker, a missing marker, and damaged data inside
the marked prefix. A sidecar may add a second sync per frontier advance.
This RFC does not assume that an in-memory frontier or a later valid batch
alone proves which writes were acknowledged before a crash.

## Alternatives considered

- Keep the current one-ring leader. It is the correctness and performance
  baseline, and remains the choice if the adoption gate is not met.
- Let each writer submit and `fdatasync` its own group. The measured tmpfs
  shape lost sync coalescing and was neutral or slower; it is not the same as
  dedicated workers with several groups queued before a shared sync.
- Use `IORING_OP_FSYNC` with `IOSQE_IO_DRAIN`. The measured drain shapes were
  slower, and the barrier holds later SQEs behind the sync.
- Begin with two logger threads and two rings, as in SpanDB's `2L4R` example.
  The first experiment uses one ring and eight groups to isolate in-flight
  depth from thread count; add the second worker only if measurements show
  one ring cannot maintain enough outstanding writes.
- Make the ticketed queue lock-free immediately. The current queue mutex
  already serializes ticket assignment with enqueue, and the measured WAL
  work is dominated by `submit_and_wait`. Replace it only if queue-lock
  measurements show it limits the dedicated pipeline.

## Validation and adoption gate

The implementation should first separate queue/packer, CQE completion, and
durability-frontier state so ordering can be tested without io_uring. State
tests must cover out-of-order CQEs and groups, a missing early group, partial
SQE submission, a short write, `fdatasync` failure, late arrivals, slot
exhaustion, queue-budget pressure before an explicit sync, idle-worker wakeup,
and `sync()`/admission and close/rotation/admission races. Hold a sync open
after full write CQEs and verify its buffers can be reused while group
durability metadata stays resident. Hold preallocation open and verify it
does not hold the producer queue mutex. Fail buffer allocation or encoding,
and race a prepared buffer against close and rotation: none may consume a
ticket or leave a file-range hole on rejection. Inject an ambiguous submission,
make `close()` return an error, then drop the `Wal` handle; an ownership test
and the address-sanitized suite must show that the buffer is not freed before
request resolution or proven ring teardown. A batch that cannot fit even in
a fresh WAL must fail without assigning a ticket or looping through rotations.
Also fail a later group both before and during an earlier group's sync: the
earlier contiguous written prefix may advance the durable frontier on sync
success, but no ticket at or after `poison_ticket` may be acknowledged. Fail
multiple groups out of order and verify the boundary moves to the earliest
failed group. Delay an already durable writer's memtable publication until
after that later failure and verify it can still publish below the MVCC poison
timestamp.
Assert that at least two groups have writes outstanding simultaneously under
a controlled completion schedule; otherwise the implementation may
accidentally preserve the current serial barrier.
Process crash tests should inject failures after offset reservation, after a
later group completes first, during sync, and after sync but before frontier
publication. Reopen must recover a contiguous prefix and preserve every
acknowledged ticket. Existing WAL, range-tombstone, and PITR suites must still
pass; PITR must demonstrably stay on the current path. A sandbox that denies
io_uring with `EPERM` may skip current WAL tests and cannot satisfy this gate;
the direct-I/O cases must run on a host that permits ring creation.

The performance comparison must use the same binary and workload parameters
with the current one-ring group leader as control. Run paired, alternating
repetitions on a device-backed WAL filesystem and a tmpfs control at 1, 4, 8,
and 16 writers, with small single puts and large batches. The primary regression
case must reproduce `write-perf --bench wal_concurrent` with four writers,
200,000 puts, 1 KiB values, WAL enabled, PITR disabled, and the original
1 MiB SST target. Rebuild and run the pre-PITR `2f556ccb` revision in the
*same session* as a historical reference; do not compare a new result with
the old document's absolute ops/s. Also run
a same-binary null pair to estimate harness drift, as in the PITR performance
study. Run a second, explicitly labeled WAL-isolated case with the SST target
above the run's footprint; do not call that altered workload a reproduction
of the original regression.

Report throughput, p50/p99 commit latency, CPU, solo groups, buffers per group,
groups per sync, publication-wait time, `outstanding_write_sqes`, CQE counts,
ring count, worker wakeups, `inflight_groups`, preallocation time,
`fdatasync` calls, and physical WAL bytes. Outstanding SQEs and groups measure
the software pipeline, not block-device queue depth; report device queue depth
separately only when it is measured at the device.
The WAL profile's `wal_group_gap` includes time with no pending batch, and
`wal_submit` is dominated by `submit_and_wait`; neither counter alone proves
that deeper queuing or more rings creates useful device overlap. Include
PITR-off and PITR-on controls so an improvement on one path is not credited
to the other. PITR-on must remain on the existing WAL path and show no
material regression. The earlier no-leader tmpfs result is the reason this
gate comes before a default-on change.

The experiment starts behind an internal opt-in setting. Its first shape is
one WAL I/O worker, one ring, one sync coordinator, and at most eight groups
in flight; the existing client-leader path remains the default control. The
setting must expose active worker/ring counts and a group-overlap counter in
benchmark output so a run that never overlaps groups cannot be mistaken for
a parallel result.

Keep the implementation experimental unless the four-writer regression case
shows a repeatable paired improvement beyond its same-session null spread,
and a representative device-backed write workload shows at least a 10% paired
median throughput gain with a 95% confidence interval excluding parity.
One-writer throughput must stay within 5% of the current control and p99
commit latency within 10% across the tested matrix. Show whether the proposed
path actually closes the same-session gap to the pre-PITR revision; a faster
WAL phase that leaves end-to-end `wal_concurrent` unchanged does not meet this
RFC's motivation. The crash tests must establish the prefix invariant. If
these gates are not met, retain the current one-ring group-commit path and
report the measured bottleneck rather than claiming the regression is fixed.

## Related documents

- [RFC 012](012-parallel-wal.md): original parallel-WAL proposal and historical sketches.
- [RFC 023](023-point-in-time-recovery.md): PITR WAL format, commit ordering, and recovery contract.
- [SpanDB paper](https://www.usenix.org/system/files/fast21-chen-hao.pdf):
  dedicated loggers and multiple outstanding WAL groups on raw NVMe.
- [PITR performance study](../docs/pitr-performance.md): measured default-path regression and publication fix.
- [WAL group-commit profile](../docs/perf-profile.md): measured submit shapes and limits.
- [io_uring benchmark](../docs/io-uring-bench.md): earlier single-operation comparison.
- [WAL tests](../kv-engine/src/tests/wal.rs) and
  [chaos failpoint tests](../kv-engine/integration_tests/chaos_failpoint.rs):
  current recovery and crash-test coverage.
