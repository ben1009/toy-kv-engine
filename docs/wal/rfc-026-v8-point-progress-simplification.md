# RFC 026 Follow-up: Simplifying the V8 Parallel Point Progress Path

| Field | Value |
| --- | --- |
| Status | **Historical alternatives; Fast Lane is the primary >=10% performance proposal** |
| Date | 2026-11-21 |
| Base | `main` at `58c79208` (squash merge of PR #558); source audit originally pinned to `14df402b` |
| Applies to | V8 Parallel Point Put first; any writer-wide durability-owner experiment also affects V8 batches. V7 and V8 Leader unchanged |
| Parent | [RFC 026](../../rfcs/026-composable-wal-prefix-digest.md) and [implementation plan](rfc-026-implementation-plan.md) |

## 1. Prior optimization candidates — superseded by Fast Lane

**Current direction:** The larger-impact structural design and benchmark gate are in [V8 Point Fast Lane](rfc-026-v8-point-fast-lane.md). It targets repeated Coordinator-lock ready/completion publications with safe per-ticket slots and a single ordered prefix fold; see that proposal for implementation. This document retains **A0/P0/P1 as historical or optional alternatives**, not a recommendation to implement A0 first. Moving the durability driver alone remains deferred; pre-admission resource moves have small sampled scope and substantial quota/rotation risk.

Previously reviewed candidates, retained for context (not an active execution order):

1. **A0 — Conditional pre-admission reservation/buffer preparation (not prioritized for the large-win goal):** V7 has a working pre-sequencer reserved-buffer precedent. For V8, first prove that the SourceSpoolLimit lookahead-tail reclaim path and the bounded credit semantics remain correct when speculative writers reserve before the sequencer. Move only the safe fast-path ticket-independent work; keep timestamp/ticket order, physical allocation and WAL publication under `write_lock`. If the safety proof needs a new quota/fairness protocol, **reject A0 before coding**. See Section 2.1.
2. **P0 — Optional Point-ready fusion (low expected gain):** combine one-frame Point digest/frame readiness under one coordinator lock. This removes a transition but delays the early digest fold. Test locally before considering GCP.
3. **P1 — Dedicated V8 durability owner (deferred design, not approved prototype):** retain the existing Completion Reaper and move writer-elected sync to a distinct durability thread only if measured evidence can tie the current election/driver handoff to Point critical-path throughput or tail latency. No such causal demonstration is established by the existing role CPU data. Section 4 preserves concurrency/lifecycle design constraints for future consideration.

**Current execution gate:** Do not launch A0, P0, or P1 by default. Review and implement only the [Fast Lane F0](rfc-026-v8-point-fast-lane.md) prototype if its slot-lifetime and wakeup safety proof passes. A0 is no longer the next code task. The historical tests and fallback details below remain useful if the smaller alternatives are reconsidered.

### Reviewed design issues retained

- **No combined Reaper + fdatasync thread.** `fdatasync` would stall CQE publication; the existing Reaper must stay independent.
- **No optimistic assumption about P0 fusion.** It loses early digest-prefix folding overlap.
- **No wakeup-only durability owner.** An eventual P1 requires a locked requested-cutoff predicate, no-lost-wakeup proof, and explicit lifetime/failure handling.
- **Nonterminal `Wal::close()` is a sync barrier, not an admission fence.** Distinguish it from PITR rotation and final owner teardown.
- **P1 is not Point-only.** A dedicated owner is per V8 Parallel writer and necessarily changes the scheduling of Batch and mixed operations unless a separate mode-routing architecture is introduced. This RFC does not propose that extra architecture.
- **PR hygiene:** Parent #558 was squash-merged; the RFC branch is restacked on `main`, with only two documentation files changed.

## 2. Source audit: today's Point path

At the original audited source revision (`14df402b`, whose changes have since been squash-merged into `main`):

| Operation | Implementation | Responsibility |
| --- | --- | --- |
| Admit before payload encoding | [WalV8ParallelCoordinator::admit_and_publish_with](../../kv-engine/src/wal/v8/parallel.rs) | Assign ticket, register physical reservation, append TicketState and publish admission |
| Producer finish | [WalV8FrameJob::finish_with_point_direct_frame_and_phases](../../kv-engine/src/wal/v8/leader.rs) | Encode, call mark_digest_ready, materialize frame, call mark_frames_ready, retain producer guard, submit DATA |
| Digest publication | [mark_digest_ready_with_cpu](../../kv-engine/src/wal/v8/parallel.rs) | Lock coordinator, find/validate TicketState, fold ordered digest prefix, notify |
| Frame publication | [mark_frames_ready_inner](../../kv-engine/src/wal/v8/parallel.rs) | Lock the same coordinator, find ticket again, set frames_ready, notify |
| DATA submission | [WalV8ParallelIo::publish_data](../../kv-engine/src/wal/v8/leader.rs) | Queue under separate submission lock, enforce contiguous offsets and group membership, send to bounded IoWorker |
| Physical completion | [completion thread and reap_v8_group_result](../../kv-engine/src/wal/v8/leader.rs) | Receive GroupWriteResult, publish DATA completion in CoordinatorState |
| Durability | [advance_frontier_through / drive_frontier_through](../../kv-engine/src/wal/v8/leader.rs) | A completing writer wins driver election, forms FRONTIER, writes it, fdatasync, retires resources, publishes durable watermark |
| Other writers | [acquire_durability_driver / published_cursor](../../kv-engine/src/wal/v8/parallel.rs) | Wait for durable-published notification, acknowledge only after own exclusive ticket end is covered |

The existing implementation **already** separates generic, durability-progress and durable-publication notifications, and **already** supplies a published cursor so followers do not reacquire the physical FRONTIER mutex. Keep those changes; this proposal is not a rerun of the 2026-11-02 notification patch.

The format requires admission to become visible before payload encoding. It requires ordered digest folding and DATA completion to remain independently valid. **It does not require that an already-completely-prepared one-frame Point issue two separate coordinator publications.** Moving the Point digest notification until its frame is built can reduce one state-lock round trip, one ticket validation and one generic notification, **but it also delays digest-prefix folding** and loses some overlap. The earlier 2026-11-20 Point Stage CPU report is not an estimate of what the fusion can save: that stage includes frame readiness, guard registration and Worker submission. P0 is a low-cost proof-of-concept, not a plausible stand-alone explanation for the ~14% V7/V8 throughput gap.

### 2.1 What the code and clean comparisons actually prioritize

The existing [Point producer critical-path report](benchmarks/rfc-026-v7-v8-point-producer-critical-path-gcp-20261119.md) measures a much larger V8 wait at the **MVCC commit sequencer** than V7: sampled 16-writer `write_lock` wait p50 is about 44.5 microseconds versus 0.36 microseconds in V7; the V8 serialized region holds the lock for around 8–10 microseconds median (see also [admission breakdown](benchmarks/rfc-026-v8-point-admission-sequencer-gcp-20261119.md)). These are *instrumented wall-time distributions*, not a direct estimate of recoverable throughput, but they are evidence of a concurrency-sensitive critical section.

Code sites at the merged baseline:

- `kv-engine/src/mvcc.rs::write_v8_parallel_wal_only`: perform preflight before `self.write_lock.lock()`, then reserve timestamp, build metadata and call `memtable.admit_prepared_v8_pitr_wal_batch_only` while the sequencer lock is held; release the lock before Frame Finish.
- `kv-engine/src/wal/v8/leader.rs::WalV8ParallelWriter::admit_prepared_batch_inner`: under that path, call `reserve_batch_resources(logical_bytes, frame_bytes)`, allocate `DirectBufAllocation::new(frame_bytes)`, then `prepared.admit_with_metadata(commit_ts, recorded_at, &publisher)`; only the final admission needs ordered ticket/timestamp/physical offset publication.
- The diagnostic breakdown at 16 writers found resource reservation roughly 0.90 microseconds median CPU and `DirectBufAllocation` about 0.48 microseconds, within a larger roughly 7.96 microseconds median admission CPU. Moving only these two operations does **not** promise a 14% throughput recovery; it is a contained experiment to shorten the serialized hold without changing durability scheduling.

**Fourth-review source-level comparison:**

- **V7 precedent:** `kv-engine/src/wal/parallel/runtime.rs::prepare_v7_batch` obtains its `WalV7OwnedBatchReservation` and allocated `ParallelBuffer` *before* the MVCC sequencer; `WalV7PreparedBatch` carries both as owned values through subsequent ticket admission. This demonstrates the overall admission/ownership split can be sound, **not** that the V8 case is an unchanged copy.
- **V8 existing preflight is narrower:** `WalV8PreparedBatch::preflight_with_write_class` reserves the canonicalization preparation-index charge. That is **not** the physical/logical/source-spool + DATA/FRONTIER buffer reservation required by `WalV8ReservationOwner::reserve_batch`. Both the coordinator admission window and shared PITR resource ledger must remain bounded and correctly accounted.
- **V8 new hazard:** `WalV8ParallelWriter::reserve_batch_resources` handles `SourceSpoolLimit` via `with_allocation_tail(|tail| discard_speculative_tail(tail))`; the latter holds lookahead/extent locks, truncates the inode (`set_len`), synchronizes it (`sync_data`), and only then releases unused extent-slack credit. A0 must **not** move this recovery path verbatim to unsynchronized preflight. The final allocated tail, registered DATA/FRONTIER, and ledger charges must be consistent across all concurrent admitters.
- **Admission-order and failure-mode change:** a producer may reserve quota before obtaining a commit timestamp/ticket, so pressure among unordered producers can change which concurrent request succeeds. **There is no deterministic 'earlier writer' until the existing MVCC `write_lock` orders timestamps; do not require first-preflight-first-admit or invent a new fairness queue.** The invariant is bounded ownership, no deadlock/starvation, no invalid success/ACK, no charge leak and no incorrect terminal capacity result based *only* on speculative reservations. V7's pre-sequencer pattern is a precedent for this scheduling tradeoff, not proof that V8 quota exhaustion is equivalent.
- **Small measured scope, not an upper bound:** independent sampled Point16 stage medians for resource reservation (about 0.90 μs CPU) and `DirectBufAllocation` (about 0.48 μs CPU) sum arithmetically to 1.38 μs, **but stage medians are not a valid upper bound on removable serialized hold time**; allocation stalls, tail reclaim and CPU/wall divergence can affect it. A0 is a testable small candidate, not an explanation of the ~14% gap. **Do not extrapolate the 44.5 μs sequencer wait into a 44.5 μs opportunity.**

**MVCC error classification (verified against code):** `mvcc.rs::record_wal_append_failure` calls `memtable.versioned_wal_requires_recovery()`, and `wal.rs::v8_requires_recovery` is true **only when the V8 WAL/parallel writer has been poisoned**, not merely because an unadmitted reservation failed. Therefore a clean, pre-ticket quota-pressure error retires its Commit TS rather than automatically poisoning the whole commit gate. A0 must preserve that distinction: a failed speculative reservation before `write_lock` must not allocate a Commit TS at all; an authoritative fallback error after TS allocation follows the existing `record_v8_parallel_error` and MVCC retirement/poison path. `Wal::record_v8_parallel_error` must still set `pitr_rotation_needed` on `RotationRequired`. This corrects the possibility of overinterpreting all post-TS errors as forced recovery.

**A0 implementation contract (conditional; not yet approved):**

1. Model the fast-path owner on V7's `WalV7PreparedBatch`: it must contain the correct **V8** `WalV7OwnedBatchReservation` (from `WalV8ReservationOwner::reserve_batch`, not only the preflight index token) and the aligned `DirectBufAllocation`, with drop order ensuring buffer lifetime is covered by the ledger charge. No ticket, timestamp, hash, DATA encoding, or speculative extent truncation may occur in this pre-sequencer stage. Fast-path pre-reservation must be bounded.
2. For a successful pre-reservation, the second phase under `write_lock` performs ordered timestamp/ticket assignment, physical range registration, `WalV8AdmissionPublisher` publication and the unchanged `WalV8FrameJob` guard transfer. **Publish no ticket until the resources required to finish are owned.** Preserve Coordinator -> Submission lock order and the `frontier_resources` map insert/poison behavior.
3. **All quota-pressure failures must use a serialized fallback, not only SourceSpoolLimit.** A tentative fast-path call to `WalV8ReservationOwner::reserve_batch` may fail with `LogicalWalLimit`, `SourceSpoolLimit`, `BufferMemoryLimit` or `SealIndexLimit` because other unordered producers hold speculative tokens. Such failures are **not authoritative** before the MVCC sequencer: retain the unadmitted prepared batch, acquire `write_lock`, then retry the complete existing `reserve_batch_resources` admission path (including its SourceSpoolLimit tail reclaim under `with_allocation_tail`). Preflight must neither truncate nor sync the WAL. A retry may still validly fail if quotas are exhausted, so preserve the original rotation/error mapping and pre-/post-ticket poison classification. Validation/overflow/foreign-owner errors are not transient quota pressure and must not be converted into silent retries.

    **Critical ordering constraint:** do not wait under `write_lock` for an unadmitted *other* producer to drop its speculative resource token: that producer may be blocked on the very same `write_lock`. Fallback is one original serialized reservation/reclaim attempt, not an indefinite wait loop. If preserving sensible failure behavior under these conditions requires new priority bookkeeping, reject A0.
4. **Ownership/rotation/rollback:** validate that the source WAL writer, inode/segment incarnation, quota owner and coordinator match when transferring the token to its admitted ticket. Never rehome an old-inode reservation after rotation. A failed preflight, failed timestamp allocation, rotation/capacity refusal, or dropped preparer releases only unadmitted resources and never poisons a nonexistent ticket. The existing `WalV8LeaderAdmissionPublisher::admit_and_publish` marks the charge as admitted **after** coordinator publication; preserve this exact transition. After ticket publication, preserve poison and retained source charge on uncertain I/O. Any new permanent resource error caused by a still-live but unadmitted competing preparer must be explicitly justified against V7-style admission semantics; no indefinite lock-held waiting is permitted.

5. Keep this **Point-only and bench-controlled**, with Batch, V7, V8 Leader and recovery untouched. Before any GCP, deterministically test *all four* quota-pressure categories, specifically an outstanding unadmitted reservation while another producer holds `write_lock`, plus SourceSpoolLimit reclaim (`set_len`/`sync_data` failure), rotation, cancellation, buffer lifetime and `mark_admitted` handoff. Ensure the retained-preparation path does not double reserve on fast-path success and does not double-release on retry. Record `write_lock` diagnostics outside scored runs.

**Rejection criterion:** Reject A0 if it requires new quota-priority bookkeeping to avoid lock-held waiting, changes quota/rotation error classification in a way the existing API cannot tolerate, performs speculative file truncation/sync before ordered admission, introduces a double charge/early release, or needs a large resource-ownership rewrite. **No GCP run for a candidate that fails these safety checks.**

### Existing experimental evidence and exclusions

- [Per-role CPU](benchmarks/rfc-026-v7-v8-thread-role-cpu-gcp-20261120.md): V8 adds about +8.42 microseconds/Put in Writer plus ordered packer, +6.79 in completion plus durability, and +2.86 in I/O worker at Point16; role boundaries differ between V7 and V8.
- [Writer stages](benchmarks/rfc-026-v7-v8-writer-stage-cpu-gcp-20261120.md): the sampled frame-readiness/submission difference was +3.39 microseconds/Put at 16 writers, but the V7/V8 timing scopes are asymmetric.
- [Point 3-microsecond group join](benchmarks/rfc-026-v8-point-short-group-join-gcp-20261120.md): 31% fewer worker groups but -3.13% paired Point16 throughput; **do not repeat timed DATA joining**.
- [O(1) ticket lookup](benchmarks/rfc-026-v8-ticket-index-gcp-20261120.md): +0.21% Point16 and -0.81% Point8 paired median; rejected and reverted. **Do not propose deque indexing alone again**.
- [Notification separation rerun](benchmarks/rfc-026-v8-notification-rerun-20261102.md): retained combined notification/driver/follower/cursor logic, with incomplete performance qualification. **Do not regress to the pre-separation state**.
- [Inline worker completion](benchmarks/rfc-026-v8-worker-dispatch-20261102.md): prior direct publication from the I/O worker reduced CPU but worsened concurrent Point p99. **P1 must not run arbitrary coordinator work on the io_uring worker thread**.

## 3. P0: one coordinator event for a fully prepared Point

### Eligibility and switch

Only use P0 when all are true:

- Parallel V8, a Point-class operation supported by the existing single-frame encoding classification;
- precisely one 4096-byte DATA frame;
- benchmark-only runtime switch TOYKV_BENCH_V8_POINT_FUSED_READY=1.

Other Point shapes, multi-frame writes, batch writes, V8 Leader and V7 use unchanged methods. Initially default the switch to off, even in benchmark builds. Use the same binary for A/B to avoid unrelated differences.

### Control-flow change

**Existing:** encode logical batch -> mark_digest_ready -> materialize frame -> mark_frames_ready -> register producer guard -> publish_data.

**P0:** encode logical batch -> obtain immutable digest -> materialize complete frame -> publish_point_prepared(digest, ticket) -> register producer guard -> publish_data.

Implement publish_point_prepared in kv-engine/src/wal/v8/parallel.rs as a **single coordinator-lock transaction**:

1. Check not poisoned; find the same admitted ticket and validate the incarnation, logical byte length, digest identity, and that the ticket has not been durably retired.
2. Prevalidate both updates before mutating either one. Preserve the existing semantics for duplicate-identical digest and duplicate frames_ready; reject conflicting or inconsistent partially published state. Do not silently complete a missing, failed or foreign ticket.
3. Store digest_ready and frames_ready, perform the unchanged fold_ready_digests over the contiguous prefix, and retain exactly the same prefix digest formula.
4. Emit one generic state-change signal; emit a durability-progress signal if the existing folded-prefix predicate requires it. **Do not drop the conditional late-digest wakeup**, which is needed if DATA completion preceded digest readiness in other paths.
5. Keep the diagnostic timeline/frame-ready timestamp semantics coherent when bench sampling is enabled; do not claim CPU-stage comparability when P0 combines stages.

Share validation/fold helpers with the existing mark_digest_ready and mark_frames_ready paths instead of duplicating the digest state machine. The combined method must not acquire the submission mutex, perform extent I/O, or call IoWorker while holding the coordinator lock.

The producer continues to transfer EncodedWalV8Batch admission-guard ownership to producer_guards **before** publishing DATA. Failure or unwind after admission must poison the coordinator exactly as today. The successful DATA CQE is still the only route to data_complete, and FRONTIER remains gated by frame, digest, and data completion.

### Why this is semantically legal

- The ticket was already admitted and physically reserved before encoding (RFC 026 Section 4).
- P0 does not submit DATA before the frame is ready; it only delays digest publication until that same producer finishes its frame.
- An out-of-order later producer still may publish first. The coordinator continues to fold only the contiguous digest prefix.
- No additional success, retirement or acknowledgment path is introduced: durability requires verified FRONTIER write, successful fdatasync, durable-frontier publication and own ticket coverage.
- If frame construction fails, the old path could have published a digest without a frame; the new path publishes neither and poisons. Neither case may become durable.
- Existing DATA-join policy, timestamp order, frontier physical offsets, reservation credits, recovery and on-disk digest are unchanged.

**Risk:** early digest folding may currently overlap frame materialization. P0 postpones that opportunity. If P0 increases head-of-line wait, throughput or p99, reject it rather than compensating with a new timer.

## 4. P1: deferred durability-owner design, Completion Reaper stays separate

### Motivation and hard design constraint

V8 currently has an independent Completion Reaper but no dedicated durability thread. A writer that wins `acquire_durability_driver` performs the FRONTIER/`fdatasync` cycle; other writers wait on `durable_published`. The completion reaper independently calls `reap_v8_group_result` and `mark_data_group_complete` while a writer synchronizes.

The **original proposal** to combine the Reaper and `fdatasync` into one owner was rejected during review. Synchronous `fdatasync` would prevent processing additional DATA CQEs, potentially saturating bounded IoWorker group slots, delaying ready-prefix growth and even causing an indirect liveness dependency if the driver waits for completions it cannot consume. This may not be implemented as the P1 design.

The revised architecture is:

```text
Producer(s) -> Submission -> IoWorker -> Completion Reaper -> Coordinator
     |                                                |           |
     +-- durable target request -----------------------+-----------+
                                                                  |
                                             dedicated V8 Durability Owner
                                              FRONTIER write + fdatasync
                                                                  |
                                                       publish durable cursor
                                                                  |
                                                    Producer follower ACK
```

This **deferred** design leaves the Reaper and IoWorker independent so they can process completions during a sync. A dedicated durability owner adds one background thread, so it could *increase* CPU and context switching. **This is a scheduling/critical-path experiment, not an elimination of work:** previously one elected writer did the sync while followers waited; now all writers must publish their durability targets and wait, and the owner must wake. The extra thread and target notifications might be more expensive than the election/handoff they replace. No improvement is presumed, and the +6.79 μs/Put role difference is *not* the potential saving of P1.

### Exact state and operations (only if P1 is explicitly approved)

1. Introduce one monotonic `requested_durable_end` (exclusive segment ticket end) in authoritative `CoordinatorState`. Initialize it to the recovered `durable_ticket_end`. A writer or `sync()` requests its target under the same coordinator lock; validate `target <= next_ticket`, then set `requested_durable_end = max(old, target)`. Do **not** use a per-Put unbounded notification queue.
2. The owner checks under the coordinator lock whether `durable_ticket_end >= requested_durable_end`, `poisoned`, `closed`, and whether the contiguous FRONTIER-ready prefix can advance. It sleeps using an existing appropriate condition variable and generation **after checking the locked predicate**, so requests or progress cannot be lost between check and wait. Request insertion must notify this waiter. Use the existing durability-progress notifications for successful DATA completion and for the late-digest-fold case; generic admission/frame-only notifications do not suffice as the only wakeup.
3. The owner drives the **existing** `drive_frontier_through(target)` logic, with the same adaptive coalescing, reservation, physical FRONTIER gaps, source credits, `WalV8FrontierCycleGuard`, sync and resource-retirement boundaries. The owner must not hold the coordinator mutex while writing or syncing. Where possible, extract only the minimal shared `drive_frontier_through` state into an owned core rather than copying this long function.
4. `WalV8ParallelWriter::wait_durable(ticket)` becomes request `ticket+1` then wait on `durable_published` until `durable_ticket_end >= ticket+1` or poisoned; `sync()` requests the current `next_ticket` snapshot and waits for that exact cutoff. **Do not redefine public `Wal::close()` as a coordinator shutdown:** today both `Wal::sync()` and the V8 branch of `Wal::close()` call `wait_for_v8_parallel(writer, None)`, which invokes `writer.sync()`; neither calls `coordinator.close()` or fences admission. P1 must preserve that observable barrier behavior unless a *separate* high-level lifecycle change is explicitly reviewed. Separately, segment sealing/rotation must use the existing higher-level admission fence and durability cutoff; object destruction must use an explicit termination protocol. **Current `drive_frontier_through` aborts on `wake.closed` when its target remains undurable**, so a shutdown sequence cannot simply mark `coordinator.closed` before the target drains. Test the three different cases: a nonterminal `Wal::close()` barrier, fenced rotation/seal, and actual last-owner teardown.
5. Preserve the existing independent Completion Reaper and `IoWorker` lifetime, `WorkerWake` behavior, capacity accounting and completion publication. Poison on channel closure, worker failure or unexpected owner termination and wake all waiters. The owner must not depend on a periodic polling loop for correctness. **An unexpected owner panic before acquiring `WalV8FrontierCycleGuard` must also poison:** that guard protects only an active reserved frontier cycle. Add an outer RAII poison-on-abnormal-exit guard around the complete owner loop.
6. Use an owned durability core holding only required `Arc`/file/resource handles and an explicit shutdown/JoinHandle owner. **Never put an Arc of the whole writer (containing its own JoinHandle) into that thread**, which creates ownership cycles. Current `WalV8ParallelIo::Drop` closes the worker **before** joining the Completion Reaper; P1's new owner cannot be stopped by dropping `io` or closing its completion channel, since the owner may itself hold an `Arc<WalV8ParallelIo>`. Specify an explicit one-shot shutdown request and join order on `WalV8ParallelWriter` teardown, with a separate poison path for unresolved producer work. All required file handles, progress/guard maps, and Worker+Reaper must outlive owner completion or safe termination. Do not block in `Drop` on producer work that could be retained by a caller after the writer's last public reference is released; poison/exit according to the existing admitted-guard fail-stop contract. Validate that no join path holds `frontier`, coordinator or submission mutex while waiting.

### Mandatory lifecycle state machine (second review)

The prototype must explicitly distinguish these operations:

| Operation | Admission | Required action | Coordinator closed? | Exit / failure |
| --- | --- | --- | --- | --- |
| `wait_durable(t)` | open | Request exclusive cutoff `t+1`; wait for durable publication | No | Return only after own ticket covered; poison on failure |
| `Wal::sync()` | unchanged | Snapshot currently admitted `next_ticket`, drive and wait for that cutoff | No | Do not silently skip in-flight prepared tickets |
| `Wal::close()` existing V8 branch | unchanged by this RFC | Same V8 `writer.sync()` barrier as today | No | Keep API semantics; no forced termination or implicit admission fence |
| PITR rotation/seal/checkpoint | externally fenced by PITR lifecycle | Finish exact cutoff and maintain immutable boundary/anchor rules | Separate lifecycle decision | Respect recovery and manifest authority |
| Final owner teardown | no new public admissions after final ownership is released | Explicit stop + join; retain or safely fail-stop unresolved work | Terminal transition only when safe | No self-join, no worker/reaper prematurely closed, no lost waiter |

**Required implementation plan before coding:** write down who owns the durability thread's `JoinHandle`, the file descriptors, `frontier` mutex, `frontier_resources`, `producer_guards`, `WalV8ParallelIo`, and the shutdown signal. A simple shared `Arc` of the entire `WalV8ParallelWriter` is forbidden. Show an explicit refcount/liveness graph and a teardown sequence. The owner may not wait indefinitely in `Drop` for an admitted `WalV8FrameJob` that is still externally held or has failed.

A `requested_durable_end` update must be under the coordinator mutex and notify the durability-progress subscriber on a raised target. The dedicated owner also must wake for DATA completion, late digest folding, poison and terminal lifecycle signaling; `notify_all` by itself is not a condition. Keep physical FRONTIER lock, coordinator lock and submission lock acquisition order compatible with the current `reserve_frontier_with` callback (coordinator -> submission); never reverse these edges in the owner.

**Proof of value before building a large core extraction:** show which writer-side mutex acquisitions or wakeups disappear. Every `wait_durable` still needs a target registration and durable wait; P1 introduces a new scheduler handoff. If the prototype only relocates `drive_frontier_through` CPU to a new thread without measurable throughput or p99 improvement, reject without further tuning.

### Scheduling semantics and reject conditions

The existing `drive_frontier_through` runs until the elected writer's ticket is durable; it may cover additional ready tickets in a coalesced FRONTIER. P1 must preserve this property and avoid introducing an eager-per-ticket sync policy. It may use the current maximal requested cutoff as the drive target; when all requests have been covered, the owner parks until a new request. It must recheck requested target and readiness under the coordinator lock after every cycle.

P1 does **not** reduce the number of necessary DATA-completion events or format-required FRONTIERs. A new enqueue/wakeup on each Point, worse p99 due to additional scheduling, or a higher sync count can outweigh any decrease in writer CPU. The first bounded benchmark must report these counts. Reject P1 if any of these liveness properties are unproven; do not "fix" it by moving the blocking sync onto the Reaper, the io_uring worker, or adding a second ad-hoc notification queue.

### No-lost-wakeup proof obligation

A correct proposal must spell out all three races:

- **Request versus sleep:** an arriving `requested_durable_end` update is visible in the same locked predicate the owner checks before waiting, and notification cannot be lost.
- **DATA complete before digest:** the Reaper publishes `data_complete`; the later `mark_digest_ready` performs ordered fold and signals durability progress when that changes a frontier-ready prefix.
- **Close/poison and owner failure:** all followers and the owner wake, no unacknowledged ticket is declared durable, and the fail-stop guard prevents an uncertain FRONTIER from being appended after failure.

A smoke test must deterministically pause each thread at these boundaries and assert progress or poison, not merely wait for a timeout.

## 5. Invariants / acceptance tests

For each candidate, preserve all of the following:

1. Admission publication precedes payload-sized encoding and hashing.
2. Timestamp/ticket order, reserved physical ranges and FRONTIER gap registration remain intact.
3. V8 BLAKE3 entry/batch/ordered-prefix vectors, 4096-byte DATA/FRONTIER frames and existing header identity remain byte-for-byte equivalent at equal assigned offsets.
4. Out-of-order digest, frame and DATA readiness cannot skip a missing earlier ticket. No successful CQE is accepted for a failed or unknown ticket.
5. Exactly one durable frontier chain: physical FRONTIER write then successful fdatasync then durable-ticket publication then ACK. Failure after a reserved FRONTIER poisons the entire writer, including during unwind.
6. Producer guards and reservation credits remain live until the existing durable-retirement boundary; no early release, leaked admitted ticket or silent cancellation.
7. Close/poison/worker exit always wakes followers; no missed progress event when DATA completes before digest, digest folds late, or a second producer becomes ready between predicate check and sleep.
8. V7/V8 Leader, crash recovery, normalized Active replay, migration, seal/checkpoint/backup and old-reader compatibility stay unchanged.

Tests: two producers finish out of order; late digest after successful DATA completion on the legacy path; one-frame plus multi-frame mixed workload; failed frame encode / worker write / FRONTIER write / fdatasync; panic/cancellation during prepare/submit/drive; bounded admission and Worker-group saturation; final-ticket/close/rotation race; crash cuts before/after DATA and FRONTIER sync; source-credit and producer-guard retirement; checksum/golden-vector equivalence; and the existing V8 + WAL recovery suites. P1 additionally needs deterministic request-versus-sleep, late-digest-after-DATA, in-flight-fdatasync-with-new-CQE, `Wal::sync()` and `Wal::close()` nonterminal barriers, externally fenced rotation, owner panic before/after FRONTIER reservation, externally held producer jobs at teardown, and shutdown/join cycle tests. The existing Completion Reaper must remain runnable while the durability owner is stalled inside a test-controlled sync.

## 6. Performance gate and stop condition

Use GCP c4-standard-16-lssd, 16 vCPUs, two Local SSDs in ext4 RAID0; release --features bench; 1 KiB Point; the same Direct FRONTIER + 1 MiB Point-only Lookahead + dynamic coalescing profile as the November 20 comparisons. Disable perf, detailed CPU attribution, sync diagnostics, timeline tracing, and the bench-only extent-policy scan. Maintain one source snapshot and vary only the experimental switch when possible. Exclude warmup from scores; record executable SHA-256, source SHA, filesystem settings and actual selected branch.

**A0:** first run a short *uninstrumented* local Point8/16 smoke A/B; if not encouraging, revert without GCP. Otherwise run five counterbalanced GCP pairs at each Point8 and Point16, plus Point1/Batch64/16 regression guards (100k logical Puts per run). **P0:** optional second candidate with the same local-then-GCP gate. **P1:** *no benchmark authorized by this RFC revision*; first requires a new explicit decision after proving its measured critical-path value and completing the concurrency/lifecycle review. Record within-pair throughput deltas, p50/p95/p99, process CPU/Put, syncs/Put, DATA groups/Put, actual eventfd writes and tail outliers. Sample Point latency every 10 operations (about 10,000 observations per 100,000 puts), identically in both arms. This still needs paired-run consistency for p99; do not interpret Batch64 p99 from a handful of sampled operations.

Retain a candidate only if:
- Point8 and Point16 show >=3% **paired median throughput gain**, with >=4/5 wins each;
- no systematic >5% p99 regression or >2% throughput regression in Point1/Batch64 guards; no correctness failures;
- the result persists without the temporary scope instrumentation.

Reject when the paired gain is <2%, mixed/unstable after the predeclared five pairs, or latency/correctness gates fail. Do not keep retuning 1/3/5/10 microsecond waits, containers or worker notification flags until a favorable sample appears. When 2-3% is ambiguous, record as inconclusive; a separate independent confirmation requires a new explicit decision.

If A0 and the optional P0 both fail, retain the current V8 implementation, document the ~10-15% Point gap in the qualification report, and request an explicit architecture/product decision rather than running more microscopic attribution tests. P1 is not the default next candidate.

## 7. Implementation slices and review gates

1. **A0 safety gate before code:** compare against V7's `prepare_v7_batch` ownership model; prove fast-path V8 charge + aligned buffer ownership, **all four quota-limit error fallbacks**, no lock-held wait for another unadmitted producer, maintained ticket/commit-TS ordering, and exact `mark_admitted` handoff. Test pressure/error paths for LogicalWalLimit, SourceSpoolLimit, BufferMemoryLimit and SealIndexLimit, plus rotation, cancellation, and reclaim (`set_len`/`sync_data`) failures. If these need invasive quota priority changes, reject A0.
2. **A0 conditional patch and validation:** only after gate 1 passes, implement a Point-only fast path with owned charged buffer and an unchanged serialized SourceSpoolLimit fallback. Run correctness/chaos tests and local Point8/16; only promising, non-regressing changes advance to five paired GCP runs. Otherwise record the rejection without GCP.
3. **P0 optional second screen:** implement atomic `publish_point_prepared` for qualifying V8 Parallel Point, validate that delayed digest folding is correct, and run local screen before any GCP. It is independent of A0.
4. **P1 (deferred):** the owner topology/teardown constraints in Section 4 remain for documentation. Do not build P1, make it the fallback to A0/P0 or rent GCP for it until a separate decision based on critical-path evidence.
5. **Promotion (only after passing):** remove chosen benchmark switch, update the RFC 026 implementation plan and performance qualification; keep V7/Leader semantics and wire-format version intact.

## 8. Open questions for code review

- Does moving the one-frame digest fold until after frame materialization reduce or increase ready-prefix head-of-line blocking on Point16? The A/B, not a single CPU p50, decides.
- Can A0 use V7-style pre-sequencer charged ownership while treating **every pressure error** as a fallback to the existing serialized resource admission (including V8 SourceSpoolLimit tail reclamation), with no wait under the sequencer for a pending preflight token? If not, reject A0 rather than changing the resource protocol for small sampled CPU savings.
- If P1 is ever reopened, does a dedicated owner cause more syncs because it runs without an elected writer's specific target? Use the monotonic requested-durable cutoff and preserve the ready-prefix/coalescing policy.
- Can the dedicated owner wait on the coordinator's existing durability-progress generation while also receiving explicit target-request notifications, with a single locked predicate? Prove the ordering and avoid a per-Point queue.
- How should P1 detect a disappearing IoWorker while no DATA completion is forthcoming, without a polling-only liveness dependency? Define worker-exit/error signaling and joining explicitly.
- Does adding a new thread materially increase runnable contention or process CPU on 8-physical-core/16-vCPU GCP C4? This is a performance gate, not assumed free.

This document archives the smaller candidates and their safety gates. The large-impact proposal is [V8 Point Fast Lane](rfc-026-v8-point-fast-lane.md). No runtime code, production default or benchmark qualification is changed by either RFC.
