# RFC 024: Parallel WAL Implementation Plan

**RFC:** [RFC 024: Dedicated WAL I/O Pipeline](../rfcs/024-dedicated-wal-pipeline.md)

**Status:** Proposed; no parallel WAL implementation is enabled

**Last updated:** 2026-09-24

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
| [`Wal::put_batch`](../kv-engine/src/wal.rs) and range-batch encoding | Encode into `DirectBuf`, then assign a ticket and append to `pending`. | Share the v4 encoder; add pre-ticket budget reservation and atomic ticket/file-cap admission for the opt-in path. |
| [`Wal::submit_and_commit`](../kv-engine/src/wal.rs) | A client wins `submitting`, drains one group, submits and waits for its CQEs, calls `fdatasync`, then wakes followers. | On the opt-in path, wait for a dedicated worker's durable frontier; preserve the old branch unchanged for control and PITR. |
| [`Wal::sync` / `Wal::close`](../kv-engine/src/wal.rs) | Read `next_ticket` and use the client-leader barrier. | Capture a cutoff under the same admission mutex as ticket assignment; drain through that cutoff and settle worker ownership before teardown. |
| [`MemTable::commit_wal_ticket`](../kv-engine/src/mem_table.rs) | Waits for the caller's ticket before publication. | Keep its contract; dispatch to the selected WAL path inside `Wal`. |
| Memtable WAL creation/recovery in [`mem_table.rs`](../kv-engine/src/mem_table.rs) and rotation in [`lsm_storage.rs`](../kv-engine/src/lsm_storage.rs) | Create or reopen a WAL, freeze a full memtable, and install a successor. | Carry a per-WAL internal path choice through create/reopen; handle retryable `WAL full` without holding `active_memtable_lock` while freezing. |
| [`write-perf`](../kv-engine/src/bin/write-perf.rs) | Measures `wal_concurrent` and existing WAL profile fields. | Select either path in the same binary and report actual group/SQE overlap and sync behavior. |

The current `pending` queue, `next_ticket`, `alloc_offset`, ring lock,
`CompletionState`, and `submitting` flag are coupled to the leader path. Keep
that path intact while adding a private v4 pipeline module (for example,
`wal/parallel/`) with a separately testable state machine. A per-WAL selector
must not depend on a process-global environment variable: concurrent tests
and PITR control runs need independent choices.

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

- Before ticket assignment, validate the encoded size, reserve `DirectBuf`
  capacity, allocate or recycle the buffer, and finish encoding. Keep the
  normal 64 MiB active-buffer budget, 256 queued-batch limit, and 256 MiB hard
  resident `DirectBuf` cap per WAL, including the 16 MiB prefilled pool.
  A batch exceeding the hard cap fails before allocation or ticket assignment.
- Under one short admission mutex, recheck open/poisoned state and the current
  file cap, then assign `ticket`, advance `admitted_end`, and enqueue the ready
  buffer atomically. `sync`, close, and rotation capture cutoffs using that
  mutex. Rejection consumes neither a ticket nor a file range.
- Pack contiguous tickets into groups and reserve aligned offsets in ticket
  order. Include the 4 KiB header, alignment, and rounded 1 MiB preallocation
  extent in the v4 1 GiB cap check. Move `fallocate` and its `ftruncate`
  fallback outside the producer mutex and serialize only the worker-side
  preallocation watermark.
- Return a distinct retryable `WAL full` only when the batch fits an empty WAL.
  A batch too large for an empty WAL or the buffer cap is a terminal error.
  Do not let the worker wait for engine rotation.

**Exit:** Allocation/encoding failures and prepared-buffer races with close
or rotation leave no ticket or offset hole. A blocked `fallocate` does not
block producer admission. The accounting model enforces both resident limits,
including an oversized batch; worker wakeups and buffer recycling are verified
after the worker exists.

### 4. Dedicated write worker and buffer ownership

- Give the candidate worker exclusive ownership of its registered 256-SQE
  ring, direct file descriptor, submitted buffers, and CQEs. Submit SQEs from
  at least two groups before waiting for all of either group's completions;
  never call `submit_and_wait(group_len)` for each group. Bound the entire
  ring to eight groups and 256 SQEs, including chunked large groups.
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

### 5. Independent durability coordinator and lifecycle

- Run at most one `fdatasync` at a time on a coordinator that cannot block the
  ring worker. Capture the largest contiguous written ticket before each
  call; on success, advance `durable_ticket` only through that captured target
  and below `poison_ticket`. Immediately reconsider another sync if the
  written frontier moved during the call. Add no fixed batching delay.
- Make `submit_and_commit(ticket)` wait for **its own** durable result. Preserve
  an already acknowledged ticket if a later group fails. Keep `sync()`'s
  captured cutoff and empty no-op behavior.
- Close admission, drain the captured cutoff, stop the worker, consume or
  cancel terminal requests, and join it before releasing buffers or the file.
  If kernel ownership cannot be proved released after a failed shutdown,
  retain or deliberately leak the worker-owned state rather than free a
  possibly referenced `DirectBuf`.
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
  Both `Transaction::commit` and the separately implemented
  `Transaction::commit_async` must rerun OCC conflict validation against commits
  made during rotation before reserving a new timestamp or admitting a WAL
  batch. Keep the transaction's MVCC snapshot `ReadGuard` pinned across the
  retry so OCC history cannot be pruned; it is distinct from the active
  memtable read guard that rotation requires the writer to release. Preserve
  the write set and restore `committed`/snapshot-guard state on retryable
  pre-admission errors. Define async cancellation by its admission outcome:
  cancellation must not mark a possibly admitted or durable commit retryable,
  and the snapshot guard must remain pinned until the blocking commit resolves.
  Apply the same admission cutoff rule to explicit sync and close.

**Exit:** No writer can straddle old and successor WALs. A later poisoned
group cannot retract an earlier durable ticket, and no worker survives a
normal successful close. Holding sync open does not hold completed `DirectBuf`s,
and queue pressure wakes the worker without another client arrival. The
opt-in path is usable end to end for v4 WALs.

### 6. Recovery, crash, and compatibility gate

- Reopen files after failures at offset reservation, a later group completing
  first, sync in flight, and sync success before frontier publication. The v4
  scanner must stop at the first invalid/zero batch and recover a contiguous
  prefix; every acknowledged ticket must survive, while a complete
  unacknowledged suffix remains an allowed unknown outcome.
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
  Cover both sync and async transaction commit with a conflict during rotation;
  exercise async cancellation and error restoration while the retry is pending.

**Exit:** The model tests, nextest suites, process crash tests, and sanitizer
jobs pass on a host that permits io_uring. `EPERM` in a sandbox is not a
passing result for this gate.

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
  only when available. Include PITR-on control runs and prove the candidate
  actually reaches two simultaneous in-flight groups.

**Adopt only if:** The four-writer case improves beyond the same-session null
spread; a representative device-backed workload gains at least 10% paired
median throughput with a 95% confidence interval excluding parity; one-writer
throughput regresses by no more than 5%; p99 regresses by no more than 10%;
and the crash/prefix gate passes. Show whether end-to-end performance closes
the same-session pre-PITR gap. Otherwise keep the leader path as default and
publish the measured bottleneck. A second worker/ring is a later experiment
only if the one-ring candidate demonstrably cannot sustain useful overlap.

## Review and verification cadence

Keep slices 1-3 dormant or test-only; enable the candidate for v4 behind the
internal selector only after slices 4-5 are complete. Each implementation PR
should state its invariant, affected WAL format, failure behavior, and evidence
from the matching slice. Run `cargo make check` for code changes, focused
nextest and failpoint tests while iterating, then the all-feature suite and
sanitizers for the crash-safety gate. Do not interpret a benchmark result as
an adoption decision until correctness and device-backed runs are complete.
