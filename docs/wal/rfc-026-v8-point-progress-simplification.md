# RFC 026 Follow-up: Simplifying the V8 Parallel Point Progress Path

| Field | Value |
| --- | --- |
| Status | **Revised after self-review — design only; no runtime change** |
| Date | 2026-11-21 |
| Base | `main` at `58c79208` (squash merge of PR #558); source audit originally pinned to `14df402b` |
| Applies to | V8 Parallel Point Put; V7 and V8 Leader unaffected |
| Parent | [RFC 026](../../rfcs/026-composable-wal-prefix-digest.md) and [implementation plan](rfc-026-implementation-plan.md) |

## 1. Decision requested

Evaluate **two independent, bounded candidates**; P0 is an optional screen, while P1 is the architecture candidate:

1. **P0 — Optional small Point-ready fusion screen:** for a qualifying one-frame Point, atomically publish batch digest and materialized-frame readiness under one coordinator lock. Keep DATA completion independent. This is *not* the architecture redesign and may lose early digest-fold overlap; do not spend a GCP campaign on it without an encouraging short local A/B.
2. **P1 — Independent dedicated V8 durability owner:** keep the existing independent Completion Reaper and io_uring worker; move writer-elected FRONTIER/fdatasync driving into a separate V8 durability thread. Writers publish a desired ticket end and wait for the durable watermark. P1 is the **architecture candidate** and has an independent benchmark gate.

Do **not** make P1 contingent on P0 winning or losing. P1 needs a completed concurrency, shutdown and liveness design review before implementation. At most two bounded source-level candidates and one independent GCP A/B per surviving candidate; do not start another open-ended profiling campaign.

### Self-review blockers corrected in this revision

- **No combined Reaper + fdatasync thread.** A syncing thread cannot drain DATA completions, creating completion backlog and potentially a progress cycle. P1 keeps the current Reaper distinct.
- **No assumption that ready fusion is free.** P0 delays a previously earlier digest-ready notification; that can reduce overlap and create HOL even if one lock/notification is saved.
- **No wakeup-only design.** P1 must publish its requested durable ticket end under the coordinator mutex and sleep on a predicate/generation protocol that cannot miss a late DATA completion or digest fold.
- **Clean PR diff restored.** PR #558 was squash-merged while this RFC was stacked on it. The branch has been restacked onto current `main`; PR #559 now changes only this RFC and its README link.
- **No coarse p99 gate.** Sample Point latency at least once per 10 Puts for a meaningful, still approximate tail comparison. Avoid overinterpreting Batch64 p99.

The purpose is to test whether fewer synchronization transitions, rather than faster ticket lookup or extra DATA batching, improve the Point critical path. **Neither candidate is assumed to win.**

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

## 4. P1: dedicated durability owner, Completion Reaper stays separate

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

The Reaper and IoWorker are **unchanged** and can process completions during a sync. A dedicated durability owner adds one background thread, so it could *increase* CPU and context switching; its hypothesized gain is removing per-Writer driver-election, ownership handoffs and physical sync work from the Point request path. No improvement is presumed.

### Exact state and operations

1. Introduce one monotonic `requested_durable_end` (exclusive segment ticket end) in authoritative `CoordinatorState`. Initialize it to the recovered `durable_ticket_end`. A writer or `sync()` requests its target under the same coordinator lock; validate `target <= next_ticket`, then set `requested_durable_end = max(old, target)`. Do **not** use a per-Put unbounded notification queue.
2. The owner checks under the coordinator lock whether `durable_ticket_end >= requested_durable_end`, `poisoned`, `closed`, and whether the contiguous FRONTIER-ready prefix can advance. It sleeps using an existing appropriate condition variable and generation **after checking the locked predicate**, so requests or progress cannot be lost between check and wait. Request insertion must notify this waiter. Use the existing durability-progress notifications for successful DATA completion and for the late-digest-fold case; generic admission/frame-only notifications do not suffice as the only wakeup.
3. The owner drives the **existing** `drive_frontier_through(target)` logic, with the same adaptive coalescing, reservation, physical FRONTIER gaps, source credits, `WalV8FrontierCycleGuard`, sync and resource-retirement boundaries. The owner must not hold the coordinator mutex while writing or syncing. Where possible, extract only the minimal shared `drive_frontier_through` state into an owned core rather than copying this long function.
4. `WalV8ParallelWriter::wait_durable(ticket)` becomes request `ticket+1` then wait on `durable_published` until `durable_ticket_end >= ticket+1` or poisoned; `sync()` requests the current `next_ticket` snapshot and waits for that exact cutoff. Close/rotation must first stop admission, then drain already admitted work or poison on terminal failure as existing code does; the background owner must also handle `sync()` with an outstanding earlier ticket even when no new producer event will arrive. **Current `drive_frontier_through` aborts on `wake.closed` before its requested ticket becomes durable; therefore P1 cannot just call `coordinator.close()` and expect the owner to drain.** Define a shutdown barrier that finishes valid in-flight targets before publishing the terminal closed state, or prove an equivalent fail-stop transition. Test both graceful close and cancellation/poison.
5. Preserve the existing independent Completion Reaper and `IoWorker` lifetime, `WorkerWake` behavior, capacity accounting and completion publication. Poison on channel closure, worker failure or unexpected owner termination and wake all waiters. The owner must not depend on a periodic polling loop for correctness.
6. Use an owned progress core holding only required `Arc`/file/resource handles and an explicit shutdown/JoinHandle owner. **Never put an Arc of the whole writer (containing its own JoinHandle) into that thread**, which creates ownership cycles. Define the teardown order so resources required to drain admitted tickets outlive both worker and durability thread. Validate that no join path holds `frontier` or coordinator mutex while waiting.

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

Tests: two producers finish out of order; late digest after successful DATA completion on the legacy path; one-frame plus multi-frame mixed workload; failed frame encode / worker write / FRONTIER write / fdatasync; panic/cancellation during prepare/submit/drive; bounded admission and Worker-group saturation; final-ticket/close/rotation race; crash cuts before/after DATA and FRONTIER sync; source-credit and producer-guard retirement; checksum/golden-vector equivalence; and the existing V8 + WAL recovery suites. P1 additionally needs deterministic request-versus-sleep, late-digest-after-DATA, in-flight-fdatasync-with-new-CQE, sync()/close cutoff, owner panic/failure, and shutdown/join cycle tests. The existing Completion Reaper must remain runnable while the durability owner is stalled inside a test-controlled sync.

## 6. Performance gate and stop condition

Use GCP c4-standard-16-lssd, 16 vCPUs, two Local SSDs in ext4 RAID0; release --features bench; 1 KiB Point; the same Direct FRONTIER + 1 MiB Point-only Lookahead + dynamic coalescing profile as the November 20 comparisons. Disable perf, detailed CPU attribution, sync diagnostics, timeline tracing, and the bench-only extent-policy scan. Maintain one source snapshot and vary only the experimental switch when possible. Exclude warmup from scores; record executable SHA-256, source SHA, filesystem settings and actual selected branch.

**P0:** first run a short *uninstrumented* local Point8/16 smoke A/B; if both are neutral or worse, revert without renting GCP. Only after an encouraging direction, use five counterbalanced GCP pairs each for Point8 and Point16, plus Point1 and Batch64/16 guards (100k logical Puts per run). **P1:** independently run that GCP matrix only after the concurrency/lifecycle review and tests pass. Record within-pair throughput deltas, p50/p95/p99, process CPU/Put, syncs/Put, DATA groups/Put, actual eventfd writes and tail outliers. Sample Point latency every 10 operations (about 10,000 observations per 100,000 puts), identically in both arms. This still needs paired-run consistency for p99; do not interpret Batch64 p99 from a handful of sampled operations.

Retain a candidate only if:
- Point8 and Point16 show >=3% **paired median throughput gain**, with >=4/5 wins each;
- no systematic >5% p99 regression or >2% throughput regression in Point1/Batch64 guards; no correctness failures;
- the result persists without the temporary scope instrumentation.

Reject when the paired gain is <2%, mixed/unstable after the predeclared five pairs, or latency/correctness gates fail. Do not keep retuning 1/3/5/10 microsecond waits, containers or worker notification flags until a favorable sample appears. When 2-3% is ambiguous, record as inconclusive; a separate independent confirmation requires a new explicit decision.

If both bounded candidates fail, retain the current V8 implementation, document the ~10-15% Point gap in the qualification report, and request an explicit architecture/product decision rather than running more microscopic attribution tests.

## 7. Implementation slices and review gates

1. **P0 patch (bench-only, off by default):** factor shared coordinator validation; add atomic publish_point_prepared; update only qualifying V8 Parallel one-frame Point producer branch. Run focused V8 tests, failpoints, Clippy, and ./dev check. Review ordering and guard transfer.
2. **P0 performance A/B:** run predeclared matrix; record raw results, patch hash, and keep/revert decision before doing anything else.
3. **P1 design and prototype (independent):** keep the Reaper; extract an owned durability-drive core and add one dedicated durability owner with a monotonic requested cutoff and locked-predicate notification. Implement a benchmark-only mode; prove shutdown, no-lost-wakeup, in-flight-CQE and crash-boundary behavior before performance measurement.
4. **P1 performance A/B:** separate cohort and separate decision. Do not promote either path from CPU savings alone.
5. **Promotion (only after passing):** remove switch, update the RFC 026 implementation plan and performance qualification; keep V7/Leader semantics and format version intact.

## 8. Open questions for code review

- Does moving the one-frame digest fold until after frame materialization reduce or increase ready-prefix head-of-line blocking on Point16? The A/B, not a single CPU p50, decides.
- Does a dedicated owner cause more syncs because it runs without an elected writer's specific target? Use the monotonic requested-durable cutoff and preserve the ready-prefix/coalescing policy.
- Can the dedicated owner wait on the coordinator's existing durability-progress generation while also receiving explicit target-request notifications, with a single locked predicate? Prove the ordering and avoid a per-Point queue.
- How should P1 detect a disappearing IoWorker while no DATA completion is forthcoming, without a polling-only liveness dependency? Define worker-exit/error signaling and joining explicitly.
- Does adding a new thread materially increase runnable contention or process CPU on 8-physical-core/16-vCPU GCP C4? This is a performance gate, not assumed free.

This RFC is deliberately performance-conservative: a simpler control-flow graph is useful only if it produces better latency/throughput and preserves the v8 recovery contract.
