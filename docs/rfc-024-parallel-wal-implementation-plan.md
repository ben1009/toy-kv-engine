# RFC 024: Parallel WAL Implementation Plan

**RFC:** [RFC 024: Dedicated WAL I/O Pipeline](../rfcs/024-dedicated-wal-pipeline.md)

**Status:** Slices 1–5 implemented; Slice 6 crash-boundary coverage is
implemented, with sync-failure and poison-race coverage still open; Slice 7
remains gated

**Last updated:** 2026-09-26

## Purpose and boundary

Implement the RFC as a sequence of reviewable changes. The first candidate is
an internal, opt-in path for ordinary v4 MVCC WALs: one dedicated io_uring
worker, one independent `fdatasync` coordinator, one ring, and at most eight
write groups in flight. Keep the current client-leader path as the control and
default until the RFC's crash-safety and device-backed performance gates pass.
Legacy buffered WALs and PITR v5/v6 WALs keep their existing paths.

The write result still means **WAL durable**, followed by memtable insertion
and ordered MVCC publication. Do not make the WAL worker publish commit
timestamps or weaken the sequencer's `poisoned_at` rule.

## Current code seams

| Seam | Current behavior | Planned change |
| --- | --- | --- |
| [`Wal::put_batch`](../kv-engine/src/wal.rs) and range-batch encoding | Encode into `DirectBuf`, then assign a ticket and append to `pending`. | The opt-in path shares the v4 encoder, reserves buffer capacity before ticket assignment, and atomically assigns ticket/file offset and enqueues. |
| [`Wal::submit_and_commit`](../kv-engine/src/wal.rs) | A client wins `submitting`, drains one group, submits and waits for its CQEs, calls `fdatasync`, then wakes followers. | The opt-in path waits for its ticket on the dedicated worker's durable frontier; the old branch remains the control and PITR path. |
| [`Wal::sync` / `Wal::close`](../kv-engine/src/wal.rs) | Read `next_ticket` and use the client-leader barrier. | The opt-in path captures a cutoff under the admission mutex, drains through it, and settles worker and sync coordinator ownership before teardown. |
| [`MemTable::commit_wal_ticket`](../kv-engine/src/mem_table.rs) | Waits for the caller's ticket before publication. | Keep its contract; dispatch to the selected WAL path inside `Wal`. |
| Memtable WAL creation/recovery in [`mem_table.rs`](../kv-engine/src/mem_table.rs) and rotation in [`lsm_storage.rs`](../kv-engine/src/lsm_storage.rs) | Create or reopen a WAL, freeze a full memtable, and install a successor. | Carry the per-WAL selector through create/reopen; retry the explicit `WAL full` result after releasing the active-memtable guard, forcing and coalescing v4 rotations. |
| [`write-perf`](../kv-engine/src/bin/write-perf.rs) | Measures `wal_concurrent` and existing WAL profile fields. | Select either path in the same binary and report actual group/SQE overlap and sync behavior. |

The current `pending` queue, `next_ticket`, `alloc_offset`, ring lock,
`CompletionState`, and `submitting` flag are coupled to the leader path. Keep
that path intact while adding a private v4 pipeline module (for example,
`wal/parallel/`) with a separately testable state machine. A per-WAL selector
must not depend on a process-global environment variable: concurrent tests
and PITR control runs need independent choices.

## Existing-code prerequisites and scope

Keep the parallel WAL work focused on ticket admission, ordered offsets,
concurrent I/O, durability, recovery, and WAL-full rotation. The reviews also
found existing async/lifecycle defects on the current leader path. Fix these in
separate PRs with their own tests; they are not evidence that the RFC's WAL
state machine is incomplete.

| Existing defect | Separate fix and gate |
| --- | --- |
| [`Transaction::commit_async`](../kv-engine/src/mvcc/txn.rs) takes its MVCC snapshot guard and copies writes/OCC sets when the future is created, before it claims the commit on first poll. | Move ownership and state capture to the winning attempt. An unpolled/losing future must not unpin the snapshot; a cancelled, spawned commit must retain its snapshot and engine admission until its outcome is known. Verify mutation before first poll, two competing futures, cancellation, and close. Required before exposing the candidate through async transactions. |
| Async engine methods in [`lsm_storage.rs`](../kv-engine/src/lsm_storage.rs) can leave admission guards in cancellable futures while detached blocking closures continue; `batch_get_async` discards its guard. | Audit all blocking engine APIs and transfer each guard to the spawned task or returned cursor. Test cancellation against close for a write, maintenance task, and batch read. Required before exposing the candidate through async APIs. |
| Async lifecycle waits can lose a [`Notify` wakeup](../kv-engine/src/lsm_storage.rs); `close_async` cancellation or a background-worker join error can leave `Closing` unresolved. | Use state-aware wait registration and a shutdown owner that records a terminal success or error after safe teardown. Test cancellation at each await, failed joins, and a second close caller. Required before relying on async close for candidate teardown. |

The candidate now runs through synchronous v4 WAL and engine paths. Do not
expose it through engine async APIs until the prerequisite tests pass.

## Implementation slices

### 1. Baseline, selection, and observability

- Capture paired current-path `wal_concurrent` numbers and p50/p99 on tmpfs and
  a device-backed WAL filesystem before changing scheduling. Record the exact
  binary, revision, filesystem, device, writer count, and workload arguments.
- Add an internal runtime selector carried through v4 WAL creation and reopen.
  The benchmark harness must choose candidate or control in the same binary;
  v5/v6 and legacy constructors must reject or ignore candidate selection
  explicitly. Keep the default on the existing leader path.
- Define counters for `inflight_groups`, `outstanding_write_sqes`, CQEs,
  groups per sync, worker wakeups, and preallocation time. Label software
  outstanding depth separately from measured block-device queue depth.

**Exit:** The selector and counters compile, the control path is unchanged,
and an opt-in PITR run demonstrably uses the old path. No candidate I/O is live.

### 2. Pure ordering and failure state machine

- Implement a model independent of io_uring for ticket intervals, assigned
  file ranges, per-group write status, `written_frontier`, `durable_ticket`,
  one captured sync target, and `poison_ticket`.
- Accept CQEs and sync results in arbitrary order. Advance only contiguous
  written and durable prefixes. A sync acknowledges only the written prefix
  captured **before** it starts, even if later writes complete during the
  syscall.
- On write failure, set `poison_ticket` to the first ticket of the earliest
  failed group and stop admission. Permit a fully written prefix below it to
  finish syncing, including when the sync starts after the failure. Fail all
  tickets at or above it. A failed sync leaves its target unacknowledged and
  poisons the remaining undurable suffix.
- Give waiters an unambiguous terminal result, including when a later group
  fails after an earlier ticket was already durable. State changes and waiter
  notification must use the same lock/condition protocol.

**Exit:** Deterministic state tests cover out-of-order completions, two
failures arriving in reverse order, sync capture, short writes, and a durable
ticket whose MVCC publication is delayed until after a later WAL failure.

### 3. Buffer admission, packer, and file-cap handling

- Before ticket assignment, validate the batch's encoded size and calculate
  its aligned `DirectBuf` capacity. Reserve that capacity, allocate or recycle
  the buffer, and finish encoding. Keep the normal 64 MiB active-buffer budget
  and 256 queued-batch limit. A single batch whose aligned capacity exceeds
  64 MiB may reserve active capacity exclusively after other active buffers
  retire, up to 240 MiB; block other active-buffer reservations until its write
  CQE retires it. The fixed 16 MiB prefilled pool remains resident under the
  256 MiB hard per-WAL `DirectBuf` cap. Reject a batch whose aligned capacity
  exceeds 240 MiB with a terminal buffer-limit error before allocation or
  ticket assignment.
- Under one short admission mutex, recheck open/poisoned state and the current
  file cap, then assign `ticket`, advance `admitted_end`, and enqueue the ready
  buffer and its encoded aligned length atomically. `sync`, close, and rotation
  capture cutoffs using that mutex. Rejection consumes neither a ticket nor a
  file range.
- Pack contiguous tickets into groups and reserve aligned offsets in ticket
  order. Include the 4 KiB header, alignment, and rounded 1 MiB preallocation
  extent in the v4 1 GiB cap check. Reserve the group range and transfer its
  queued buffers before preallocation; a preallocation failure must fail that
  assigned group and poison its ticket boundary. Move `fallocate` and its
  `ftruncate` fallback outside the producer mutex and serialize only the
  worker-side preallocation watermark. Assert `header_end <= reserved_end <=
  admitted_end <= WAL_CAP` and `header_end <= preallocated_end <= WAL_CAP`;
  before write submission, assert the group's file end is at or below
  `preallocated_end`. The packer must use the aligned length stored at
  admission; it must not re-encode a batch or recompute its length.
- Return a distinct retryable `WAL full` only when the batch fits an empty WAL.
  A batch too large for an empty WAL or the buffer cap is a terminal error.
  Do not let the worker wait for engine rotation.

**Exit:** Allocation/encoding failures and prepared-buffer races with close
or rotation leave no ticket or offset hole. Mixed batch sizes preserve the
stored admission lengths through packing. The accounting model verifies the
normal-budget and exclusive oversized-batch paths, including rejection above
the 240 MiB aligned-capacity limit. A blocked `fallocate` does not block
producer admission. Worker wakeups and buffer recycling are verified after the
worker exists.

### 4. Dedicated write worker and buffer ownership

- Give the candidate worker exclusive ownership of its registered 256-SQE
  ring, submitted buffers, and CQEs. The worker and sync coordinator must each
  hold an owned reference to the WAL file (`Arc<File>` or an owned cloned
  `File`); never hand the coordinator a borrowed `RawFd`. Submit SQEs from at
  least two groups before waiting for all of either group's completions; never
  call `submit_and_wait(group_len)` for each group. Bound the entire ring to
  eight groups and 256 SQEs, including chunked large groups.
- Tag each request with group and buffer identity. Handle partial submission,
  `EINTR`, negative or short CQEs, stale identities, and completions arriving
  out of group order. Mark a group written only after all its writes have
  full-length CQEs.
- Return a buffer to the 256 KiB pool, or free a larger buffer, as soon as its
  own full-length CQE proves kernel ownership ended. Keep only group metadata
  while waiting for sync. For ambiguous submission or completion, retain every
  possibly kernel-owned buffer until terminal CQEs or guaranteed ring teardown.

**Exit:** A controlled completion schedule observes at least two groups with
simultaneous outstanding writes. An oversized buffer is freed after its CQE.
An ambiguous submit followed by `close()` error and `Wal` drop cannot free a
buffer still reachable by the kernel, including under ASan. Sync overlap is
verified after the coordinator exists.

### 5. Independent durability coordinator and lifecycle — implemented

- Run at most one `fdatasync` at a time on a coordinator that cannot block the
  ring worker. Capture the largest contiguous written ticket before each
  call; on success, advance `durable_ticket` only through that captured target
  and below `poison_ticket`. Immediately reconsider another sync if the
  written frontier moved during the call. Add no fixed batching delay.
- Make `submit_and_commit(ticket)` wait for **its own** durable result. Preserve
  an already acknowledged ticket if a later group fails. Keep `sync()`'s
  captured cutoff and empty no-op behavior.
- Close admission and capture the final cutoff. Have the packer/worker submit
  the admitted groups through that cutoff, drain or cancel terminal requests,
  and consume all CQEs. Once no writes remain, stop and join the WAL worker;
  then have the sync coordinator complete the final captured written prefix
  and stop and join it. Drop the shared file references only after both threads
  have joined. On failure, retain any thread, file, and buffer ownership whose
  safe release is unproven; leak unresolved state rather than free a possibly
  referenced `DirectBuf`. Integrate with the separate async-close prerequisite
  before enabling this path through `close_async`.
- Integrate retryable `WAL full` across point writes, TTL writes, deletes,
  batches, range tombstones, and transaction commits. After releasing
  `active_memtable_lock`, force a v4 memtable/WAL rotation under the existing
  checkpoint and state lock order, then retry against the successor with a
  fresh MVCC reservation. `try_freeze_memtable()` is insufficient: it checks
  the memtable's SST-size threshold, which may remain below the threshold
  when the WAL reaches its 1 GiB cap. Concurrent capacity requests must
  coalesce after rechecking the active WAL identity, not rotate a new
  successor for every waiting writer.
- Every serializable write entry point, including ordinary `put`, TTL put,
  `delete`, `write_batch`, and transaction commit, must release both its
  memtable read guard and `commit_lock` before forced rotation, then reacquire
  the lock and restart its serialized write attempt on the successor WAL.
  Both sync and async transaction commit must rerun OCC after rotation, before
  reserving a new timestamp or admitting a batch. Keep the MVCC snapshot
  `ReadGuard` pinned across this retry; it is distinct from the memtable read
  guard that rotation must release. Restore transaction state only when failure
  occurred before WAL admission. Async use also depends on the separate
  prerequisites above. Apply the same admission cutoff rule to explicit sync
  and close.

**Exit:** No writer can straddle old and successor WALs. A later poisoned
group cannot retract an earlier durable ticket, and no worker survives a
normal successful close. Holding sync open does not hold completed `DirectBuf`s,
and queue pressure wakes the worker without another client arrival. The
opt-in path is usable end to end for v4 WALs.

### 6. Recovery, crash, and compatibility gate — in progress

- The parallel WAL process-kill test kills a child after sequential writes
  return and verifies all acknowledged operations after reopen. A separate
  deterministic process-kill test covers four boundaries: after offset
  reservation and before preallocation/write; after a later-group result is
  delivered while the lowest-ticket group remains unresolved; after capturing
  the sync target and immediately before calling `fdatasync`; and after a
  successful `fdatasync` but before publishing the durable frontier. Every
  case verifies the acknowledged baseline survives. The candidate must be
  absent when killed before its write, may recover at the other
  pre-acknowledgement boundaries, and must recover after successful `fdatasync`.
- The later-group case admits the first candidate before starting the second,
  fixing their ticket order, and defers processing the lowest group's CQE so
  the coordinator receives the later group's result first. Recovery must
  return a contiguous prefix of those candidates under this controlled
  schedule.
  The pre-`fdatasync` case stops after target capture and before entering the
  syscall; it does not claim to pause inside the kernel call. Together, the
  pre-call and post-success cases cover the allowed recovery outcomes for an
  unacknowledged suffix.
- A separate v4 scanner test corrupts a middle batch after close and verifies
  recovery truncates at the first invalid batch. The v5/v6 `A complete / B
  hole / C complete` test verifies the unchanged recovery path rejects the
  file without modifying it.
- Run existing WAL, range-tombstone, MVCC, async, freeze/rotation, and PITR
  suites against the candidate where applicable. Test the v5/v6
  `A complete / B hole / C complete` corruption rule on the unchanged path.
- Exercise `sync`/admission and close/rotation/admission races, queue and ring
  exhaustion, oversized batches, short writes, failed sync, and poison while
  an earlier sync is pending or has not yet started.
- Fill a WAL through repeated overwrites while the memtable remains below its
  SST-size threshold; verify one forced rotation, bounded retries, and no
  ticket loss. Race that rotation with a serializable transaction and a
  conflicting commit; the transaction must rerun OCC and reject the conflict.
  Also force rotation through ordinary serializable point and batch writes to
  verify they release both locks and retry on the successor without deadlock.
  Repeat the transaction conflict with async commit after its prerequisite fix;
  cancel a spawned commit during rotation and verify close waits for its
  terminal outcome. Run the separate async/lifecycle prerequisite test suites
  before enabling candidate WAL selection through those paths.

**Exit:** The deterministic process-crash/reopen cases, sync-failure and
poison-race cases above, model tests, applicable nextest suites, and sanitizer
jobs must pass on a host that permits io_uring. `EPERM` in a sandbox is not a
passing result for this gate.

**Verification to date (2026-09-26):** `cargo make check` passed (1,381
nextest tests); the parallel WAL process-kill/reopen test passed with 171
acknowledged operations; the deterministic crash-boundary process test passed
on a host that permits io_uring; and the repository's AddressSanitizer and
LeakSanitizer test commands passed. The v4 invalid-middle-batch scanner test
and v5/v6 zero-filled `A / hole / C` compatibility test passed. Async
candidate writes and close remain disabled until their separate lifecycle
prerequisites are met. Sync-failure and pending-sync poison-race coverage is
still outstanding, so Slice 6's exit gate remains open; Slice 7's performance
and adoption gate also remains open.

### 7. Paired benchmark and adoption decision

- Run the current leader and opt-in candidate in the same binary, alternating
  paired runs on tmpfs and a device-backed WAL filesystem at 1, 4, 8, and 16
  writers, with small puts and large batches. Reproduce the original
  four-writer, 200,000-put, 1 KiB-value, 1 MiB-SST-target regression case.
- Rebuild pre-PITR `2f556ccb` in the same session as a historical reference;
  run a same-binary null pair to measure drift. Label a separate case with
  an SST target above the run footprint as WAL-isolated, not as reproduction.
- Report end-to-end throughput and p50/p99, CPU, publication wait, group and
  sync coalescing, `inflight_groups`, `outstanding_write_sqes`, CQEs, worker
  wakeups, preallocation, physical WAL bytes, and measured device queue depth
  only when available. For every sync, record wall time, captured target,
  `written_frontier` at sync start/end, and counts of write SQEs submitted,
  CQEs completed, and groups completed during the call. This distinguishes
  software concurrency from writes that actually progress while `fdatasync`
  runs. Include PITR-on controls, run on a device-backed filesystem (ext4 or
  XFS on NVMe where available), and prove the candidate reaches two groups in
  flight.

**Adopt only if:** The four-writer case improves beyond the same-session null
spread; a representative device-backed workload gains at least 10% paired
median throughput with a 95% confidence interval excluding parity; one-writer
throughput regresses by no more than 5%; p99 regresses by no more than 10%;
and the crash/prefix gate passes. Show whether end-to-end performance closes
the same-session pre-PITR gap. Otherwise keep the leader path as default and
publish the measured bottleneck. A second worker/ring is a later experiment
only if the one-ring candidate demonstrably cannot sustain useful overlap.

## Review and verification cadence

Slices 1-3 stayed dormant or test-only until the worker and coordinator were
integrated. The v4 candidate is now opt-in behind the internal selector; keep
the leader path as default until recovery and benchmark gates pass. Each
implementation PR should state its invariant, affected WAL format, failure
behavior, and evidence from the matching slice. Run `cargo make check` for code
changes, focused nextest and failpoint tests while iterating, then the
all-feature suite and sanitizers for the crash-safety gate. Do not interpret a
benchmark result as an adoption decision until correctness and device-backed
runs are complete.
