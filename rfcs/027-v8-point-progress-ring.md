# RFC 027: V8 Point Progress Ring (PolarDB-Inspired Fast Lane)

| Field | Value |
| --- | --- |
| Status | F0 design proposal. Prototype, runtime tests and A/B pending. |
| Date | 2026-11-21 |
| Scope | V8 Parallel only; Point throughput target, shared Point/Batch correctness |
| Base | [PR #558 squash commit](https://github.com/ben1009/toy-kv-engine/commit/58c79208a760bbb9cdeb02341107eb49c2a0ae7f) |
| Parent | [RFC 026](026-composable-wal-prefix-digest.md), [RFC 026 implementation plan](../docs/wal/rfc-026-implementation-plan.md), [Progress Ring implementation plan](../docs/wal/rfc-027-v8-point-progress-ring-implementation-plan.md) |
| Target | >=10% Point16 throughput gain in scored runs that meet the Section 8 promotion gates |

## 1. Motivation and prior evidence

V7 Point16 is roughly 48–49k/s, while V8 Parallel Point16 is roughly 41–42k/s in the existing paired GCP results. Returning to ~49k/s needs ~18% improvement from a 41.5k/s V8 baseline. V8 also consumes roughly 17–20 additional process-CPU microseconds/Put across several roles, not one proven bottleneck. Previously measured admission preparation sampled medians (~0.90us reservation, ~0.48us buffer allocation) are too small and safety-sensitive to justify a major A0 quota redesign.

The existing V8 Producer calls `mark_digest_ready_with_cpu` and `mark_frames_ready_inner`, and the Reaper calls `mark_data_group_complete_inner`; these acquire `CoordinatorInner.state` and update per-Ticket metadata. The Coordinator also handles ordered digest folding, reservation, FRONTIER publication, and wait notifications. Point puts do not have the logical-Batch amortization of Batch64.

Do NOT repeat the failed ~3us Point DATA group join: it reduced Worker Groups by ~31% yet decreased Point16 throughput by ~3.13%. Avoid a new per-Point heap allocation, one lock per global group registry lookup, and an extra per-Point background-thread hop.

**Performance hypothesis:** keep immediate Point DATA submission and existing dynamic durability coalescing, but allow producers to publish *progress* locally and allow the elected owner to consume already-ready progress in bounded batches. The aim is fewer shared Coordinator-lock acquisitions and wakeups per Put; it is not a prediction of >=10% throughput.

## 2. PolarDB analogy — use progress tracking, not its physical WAL

PolarDB for PostgreSQL's WAL Pipeline lets backends publish recent-written progress; an advance stage recognizes a hole-free contiguous WAL range, followed by write, flush and notify stages. The mechanism supports independently committing transactions without the application supplying a logical batch. It may use dedicated workers and OS-cache writes; those are **not** the V8 baseline.

V8 is different: each admitted Ticket owns DATA frames and an ordered logical prefix digest, DATA is written with O_DIRECT/io_uring, DATA groups can contain multiple tickets, and a durable FRONTIER frame plus fdatasync establishes the ACK boundary. Therefore **adapt only the bounded progress ring and contiguous-prefix advancement**. Do not copy the shared WAL buffer, per-LSN data layout, flush-thread count, WAL wire format or reported PolarDB performance gains.

Source: [PolarDB for PostgreSQL WAL Pipeline](https://docpolardbpg.openpolardb.com/PolarDB-for-PostgreSQL/WAL-Pipeline.html); [Tantor Polar WAL Pipelining detail](https://docs.tantorlabs.ru/polardb/en/15.0.0-en/features/server/wal-pipelining.html).

### Intended stages (F0)

1. Ordered MVCC/Coordinator admission allocates Commit TS, absolute Ticket, DATA Offset and bounded ring slot.
2. Producer encodes the logical batch, publishes Digest Ready to its slot, then publishes Frame Ready after materialization. A one-frame Point can still submit DATA immediately.
3. Existing io_uring worker submits DATA and emits one `GroupWriteResult` per completed `WriteGroup`. The independent Reaper enqueues *successful group-completion events* in a bounded queue instead of grabbing the Coordinator for every completion.
4. The existing writer-elected durability owner drains pending group events, folds all contiguous Digest Ready tickets in order, derives the contiguous DATA-complete/Frame-ready/digest-folded prefix, and reserves FRONTIER through the existing Coordinator rules.
5. FRONTIER write + fdatasync + durable-watermark publication + per-writer ACK remain exactly as today. No new owner thread in F0.

## 3. Single-authority bounded Ticket Ring

Allocate the Ring **once per opened WAL segment**, with `capacity == max_inflight_tickets` and `index(ticket) = ticket % capacity`. The cell identity is the segment incarnation and absolute ticket. Keep the backing allocation stable for the writer lifetime. Each writer constructs a private ring and completion pipeline under the ownership rules in Section 4. A same-segment reopen can retain the persisted incarnation and reuse an absolute ticket from beyond the recovered durable prefix.

A cell moves from free to admitted to durable and then returns to free. Within an admitted cell, Digest Ready and Frame Ready are independent monotonic events. The producer path in Section 2 publishes the digest before the frame. The ring also supports DATA completion before Digest Ready. DATA completion requires Frame Ready, and a durable ticket requires both successful DATA and a folded digest. Terminal errors poison the writer and end admission and recycling.

The Ring is the **only mutable source** for producer Digest Ready and Frames Ready. The existing Coordinator deque retains owner-only `data_complete`, its sole authoritative copy. The Coordinator retains the immutable admission plan, resource charges, order cursors, `folded_prefix_digest`, `prefix_digest_after`, FRONTIER reservations and durable ticket/image boundaries. Remove separately mutable `TicketState.frames_ready` and `TicketState.digest_ready` replicas from the *fast-mode* decision path; do not add a second DATA-complete flag to the ring. The Coordinator's prefix validation, `frontier_ready_snapshot`, `reserve_frontier_with` and `publish_durable_frontier` must all use the same generation-checked ring accessor plus the ordered folded-digest cursor. **No mirror-back of each producer update under the Coordinator mutex.**

### Concrete F0 safety decisions (supersede the previous review alternatives)

**1. Poison is a durability gate, not an I/O cancellation fence.** Existing `mark_digest_ready_with_cpu` and `mark_frames_ready_inner` check poison under Coordinator state, but `WalV8FrameJob::finish_with_point_direct_frame_and_phases` releases that lock before inserting the producer guard and calling `publish_data`; the latter does not hold the Coordinator lock across submission. Thus even the baseline can submit previously admitted DATA while racing terminal poison. Do not introduce a new global submission lock, per-event global CAS reservation protocol, or stronger guarantee of no physical I/O after poison. Preserve the real invariant: no new admission or durable-frontier publication ordered after terminal poison; no successful acknowledgement of an unpublished ticket. A success already linearized at a published durable boundary need not be revoked by later poison. Keep `poison_locked` and existing reservation/publication/driver/ACK predicates authoritative. A monotonic atomic poison hint, Release-set under the Coordinator poison transition and Acquire-read by producers, may reject already-poisoned work early; it is not a separate commit authority. Already admitted in-flight work can race with poison; retain existing guards and recovery semantics. Never acknowledge from the ring or event queue. Clean close follows existing driver/close semantics, not a new unconditional drain guarantee.

**2. Release admission capacity at durable publication.** The current FrameJob publishes Frame Ready before inserting its producer guard and calling `self.io.publish_data`. The driver must apply DATA completion and consume the accepted digest before publishing durability, including when the digest arrives late. Producers carry their absolute assignment and access a cell only under its mutex. A later access revalidates the ticket at that point. Workers own DATA buffers, and the Reaper queues ticket ranges. Old `WalV8ParallelAdmissionGuard` cleanup checks its absolute ticket against the durable watermark and can run after the cell is reused. Retire resource credits, publish the durable boundary and release admission capacity, then retire guards. Admission can proceed while an old guard is being disarmed.

**3. Protect cell payload and generation under one mutex.** Keep `data_complete` and `prefix_digest_after` in the Coordinator deque under its state mutex. The driver uses these fields with the generation-checked cell to evaluate frontier readiness. A producer validates identity and updates the cell under the same mutex, copying any values it needs after unlock. Allocate the cells with the ring and apply this ownership rule through reset and reuse.

Test poison before and after local publication, guard registration, DATA submission and FRONTIER publication. Verify that later poison preserves a success already reported from the durable watermark. Force capacity-one and capacity-two reuse with delayed guard disarm and a stale producer event. Inject a retired group range into the same writer and verify it poisons without changing the new cell. Section 4 specifies the separate same-segment reopen test.

### Safety and layout requirements

- Start with preallocated slot cells (one array allocation per WAL) and **safe per-slot owned payload**. A small per-slot mutex protecting the complete producer payload, generation, and readiness fields is the chosen safe F0 prototype; mutex release/acquire provides publication ordering. Do not introduce a new `Arc<TicketSlot>` allocation for every Point or `UnsafeCell`-based recycling in F0.
- Admission initializes the cell under the Coordinator lock before handing the assignment to the producer. Store the absolute ticket and the admitted logical byte count from the immutable plan. Under the cell mutex, accept Digest Ready only if its incarnation, absolute ticket and `logical_batch_bytes` match. The first accepted `WalV8DigestReady` becomes immutable and triggers the wake in Section 5. An identical repeat succeeds without a second write or wake. A mismatched payload or conflicting repeat poisons through `poison_locked` after releasing the cell mutex. Leave the cell unchanged on failure. Test the wrong admitted byte count, an identical repeat and a conflicting repeat, including after the original digest has been folded.
- Reclaim a cell as part of durable-prefix retirement under the Coordinator lock. At that point the driver has verified Frame Ready, applied the completion event and folded the accepted digest. Expose the freed admission slot with the new durable boundary. Old guard cleanup only checks that boundary.
- Keep the reservation/byte-window and `max_inflight_tickets` backpressure under the current serialized Admission. Do not create speculative physical offsets or pre-admission quota holds. Reuse must not require spinning while holding `write_lock`.
- Do not hold a slot mutex while acquiring the Coordinator mutex. The owner may take Coordinator then a slot mutex for read/retire; a producer must drop the slot mutex before any call that could poison or notify through Coordinator.
- Keep a **single WAL-wide off-by-default benchmark mode** for this new representation. Point and Batch share the same ticket order/digest chain, so do not mix the old and new state backends for individual tickets in one segment.

Design sketch, NOT compilable Rust (payload lifetimes and lock ordering require tests):

~~~rust
struct TicketRing {
    segment_incarnation: [u8; 16],
    cells: Box<[TicketCell]>, // preallocated, length = max_inflight_tickets
}
struct TicketCell {
    payload: Mutex<TicketPayload>, // ticket, expected logical length, Option<WalV8DigestReady>, frames_ready
    // No independently updated generation/flags in this prototype.
}
// Coordinator remains the only writer of folded prefix digests / FRONTIER.
~~~

## 4. Reaper completion batching — do not invent CompletionGroup handles

The real `parallel/worker.rs::GroupWriteResult` contains `group_id`, `tickets: Range<u64>`, `write_count`, `write_bytes`, and `error`. It **does not carry a CompletionGroup pointer**, and `group_id` is allocated by the worker. Requiring every Reaper completion to look up a new mutex-protected group registry simply replaces the old Coordinator mutex with another contested object.

Instead, reuse the existing **single Completion Reaper** as the producer of a bounded `CompletedGroupEvent { group_id, tickets, write_count, write_bytes }` channel. The *single elected prefix owner* consumes and validates each event under one Coordinator critical section when scanning progress. Apply a successful event to the existing owner-only `data_complete` field over its entire interval under Coordinator lock. Validate all members before changing any. Fully already-completed live ranges are idempotent in the baseline non-diagnostic path; partial duplicates, unknown/retired ranges, invalid frame readiness or invalid identity poison. A valid worker emits one successful result per disjoint submitted group; injected duplicates must distinguish live from retired ranges.

- The queue must have room for **at least `max_inflight_tickets`** successful completion events: each valid outstanding group covers at least one admitted Ticket, and tickets cannot be durably retired before their completion is applied. Capacity proof for valid traffic: assign each queued/in-delivery success one distinct still-admitted ticket in its nonempty disjoint worker range. Retirement cannot precede event application. Therefore queued plus in-delivery successes fit the admission window. Use nonblocking `try_send`; overflow on duplicate/injected events fail-stops rather than blocking. Never wait while holding Coordinator or submission locks. Queue overflow is a correctness/configuration failure: poison and wake followers; the Reaper must not block indefinitely behind an owner waiting for completions.
- The Reaper still **immediately poisons** on a failed DATA group, unexpected channel closure or worker exit, instead of waiting for a driver that might not be active. Preserve `poison_locked` as the terminal-state authority. Normal success only needs the bounded event queue and progress wake, not the Coordinator lock.
- Keep `GroupWriteResult.group_id` for diagnostics. Validate the absolute range and its live cell identities, frame readiness, and duplicate or partial completion state during the owner drain. Apply the complete group under the Coordinator lock after all checks succeed.
- Never treat a successful DATA CQE as durable. The owner must apply all group results covering the selected contiguous prefix, fold digests and verify a contiguous prefix before reserving FRONTIER. A multi-ticket `WriteGroup` is completed atomically by applying one event while the Coordinator lock is held.
- Existing completion timeline and HOL sampling hooks must be adapted or explicitly disabled only for the opt-in experiment; production correctness and scored uninstrumented tests must not depend on diagnostic code.

### Completion identity and range validation

The completion queue belongs to one writer instance. At writer construction, bind its io_uring result receiver to its Reaper and give that Reaper the only sender for that writer's completion queue. Only the elected driver of the same writer reads the queue. Close and join the old worker and Reaper through the existing teardown path. A reopen creates a fresh pipeline, ring and queue. The old sender remains bound to its original receiver until disconnection.

The queue endpoints establish the writer identity. `CompletedGroupEvent` carries the absolute ticket range and diagnostic fields. The drain checks each member against the live deque and that writer's ring. The existing `mark_data_group_complete_inner` range checks treat an entirely completed live range as an idempotent repeat and reject a mix of completed and incomplete tickets. The worker's `group_id` remains a diagnostic label.

In a same-segment reopen test, reuse the persisted incarnation and an absolute ticket range. Verify an event from the old pipeline can reach only its original receiver or fails with disconnection following teardown. Verify that the new driver sees progress only from its own queue. Exercise retired and partially duplicate ranges within a single writer.

## 5. One ordered Advance operation and no-lost-wakeup signal

The existing driver elected by `acquire_durability_driver` remains the **only logical Advancer** in F0. Give it an explicit `advance_and_snapshot(&DriverGuard)` operation; read-only `frontier_ready_snapshot` and `prefix_digest_at` cannot consume events or fold from a non-owner. On a wake it performs one bounded action while holding Coordinator state:

1. Drain all currently queued successful `CompletedGroupEvent`s, validate each range, and mark the Coordinator's owner-only DATA-complete fields as one atomic group operation.
2. Fold Digest Ready slots from `folded_ticket_end` forward **in strict ticket order**, including digests whose DATA has not finished yet; preserve the original early-digest-fold overlap as far as current scheduling permits.
3. Scan the contiguous prefix in which **Frames Ready AND DATA Complete AND folded Digest** hold. An out-of-order completed ticket cannot bypass a gap. Produce a snapshot consistent with the same Coordinator-held admission, retirement and FRONTIER cursors.
4. Retain `reserve_frontier_with` -> verified FRONTIER physical write -> `fdatasync` -> existing resource retirement and `publish_durable_frontier` -> existing guard cleanup. Followers may observe the published watermark before guard disarm, just as today. No acknowledgement before durable publication.

The old `mark_digest_ready_with_cpu` eagerly folds in the producer thread. New elected-owner folding may add Digest HOL latency or delay a prefix during fdatasync: this is a **performance risk**, not a free win. Record owner lock tenure and how many digests/groups were drained per wake. If this regression dominates, reject F0 before adding another worker or hidden sync on Producer.

### One persistent wake token, no global epoch RMW on every Point

Both `wait_for_durability_change` and `coalesce_frontier_ready` must use the same fast-mode readiness protocol. Keep the existing `durable_published` condition for followers and `changed` for admission backpressure. Do not keep a fast-mode readiness waiter on the old Condvar.

Use one preallocated capacity-one bounded channel as the persistent wake token. No mandatory global `progress_epoch.fetch_add` on each producer/Reaper publication: it is another contended cache line, and the mutex-protected cell and completion channel already synchronize payload visibility.

- Publish state first (release cell mutex, or enqueue successful group event), then nonblocking `try_send(())`. Full means a token is already pending. Never wait for token capacity. Preserve normal disconnection/terminal handling. The Reaper signals after enqueuing the result, never before.
- Only the elected owner may receive tokens. At entry, try to consume one pending token, then advance and take the snapshot under Coordinator and cell locks. After a receive or timeout, scan again before the next receive. On poison or close, return to the existing terminal-state handling. When waiting for an empty prefix, return as soon as the prefix becomes ready. During coalescing, end the optional wait when the prefix is empty or covers the admitted window. Both waits use their original absolute deadline. Release all Coordinator and cell locks before `recv_timeout(remaining)`.
- Proof: publication before token consumption is visible in the subsequent cell-locked/queue scan; if the scan misses concurrent publication, its later notification either occupies an empty token channel or finds an already pending token, either of which wakes the next receive. A publication delayed between state update and notify cannot lose progress; if the publisher fails before notifying an admitted operation, existing poison/error paths must signal terminal failure. On owner handoff, the next elected owner begins with consume + scan and any pending token persists. No unbounded spin or extra polling deadline.
- Signal after every successfully enqueued DATA completion and after the first accepted Digest Ready for each ticket. The digest wake is required even if the driver already consumed the completion token and found a gap. Run `try_send(())` after releasing the cell mutex. Frame Ready and admission alone leave the durability token untouched. Poison and close signal the token and the existing follower and admission waiters.
- Apply the same predicate and single absolute deadline as current adaptive coalescing; do not rescan indefinitely under a continuous stream of unrelated events. Each advance drains at most the bounded window of events, applies whole groups and releases the Coordinator lock before another round. If ready, use existing coalescing decision rather than chase token traffic.

Deterministic tests must cover full-token concurrent publishers, publication between scan and park, close and poison, driver handoff and coalescing timeout under continued arrivals. For late digest, let the driver consume the only DATA completion token and observe the missing digest, then publish the digest with no later DATA event. Verify that progress resumes from that wake before the timeout. Run the case with an empty prefix and with a ready prefix in coalescing. Use barriers or explicit scheduling hooks to force the interleaving.

### Enablement is atomic at WAL-instance granularity

F0.1 may introduce the ring representation and isolated tests without routing the live pipeline. F0.2/F0.3 are implementation slices, not independently selectable runtime modes. Enable the whole-WAL bench switch only when **both** readiness wait sites, all producer publications, Reaper queue, advance, reservation/publication predicates, lifecycle and error paths are integrated and tested. No intermediate executable may defer Reaper completion while still parking on the old readiness Condvar. V8 Leader shares the Coordinator type but must continue using the original eager publication mode; do not change its synchronous group producer semantics.

## 6. Source-level patch sequence

| Patch | Files | Minimum acceptable change |
| --- | --- | --- |
| F0.1 Ring data model | `kv-engine/src/wal/v8/parallel.rs` | Preallocate generation-checked producer cells. Verify digest validation, duplicates and slot reuse in isolation. |
| F0.2 Completion events | `kv-engine/src/wal/v8/leader.rs`, `parallel.rs` | Bind the Reaper queue to the writer instance. Apply whole groups atomically and poison immediately on error. Test reopen isolation. |
| F0.3 Producer progress | `leader.rs`, `parallel.rs` | Publish Digest/Frame via ring, serial owner folds and checks ready prefix; replace old per-Ticket Coordinator notifications without duplicating readiness fields |
| F0.4 Unified waits | `parallel.rs`, `leader.rs` | Rewire both waits to persistent token + consume-before-scan protocol; enable the whole F0 runtime mode only after integrated tests |
| F0.5 Local & GCP A/B | existing bench/test harness | Deterministic concurrency + crash tests, then bounded local filter and paired uninstrumented GCP tests |

Do not add a dedicated Advance/Write/Flush/Notify thread in F0. Do not change DATA packing, `MAX_GROUP_TICKETS`, io_uring SQ/CQ, worker queue depth, extent preparation, default fdatasync coalescing, logical Batch API, on-disk format, LSN/Ticket meaning or backup/recovery semantics. V7 and V8 Leader remain untouched.

## 7. Required correctness gates

1. **Single state authority:** no stale `CoordinatorState.tickets` boolean can authorize/reject a FRONTIER differently from the Ring; all public snapshot, debug and digest-at-boundary queries are consistent.
2. **Cell validation.** Validate the absolute generation and digest payload under the same mutex used to publish or reset the cell. An identical digest succeeds without mutation. Conflicts and invalid identity or logical length poison the writer.
3. **Ordered folding:** Digest chain contains every ticket exactly once, even if DATA completes out of order; early folded digest must not authorize durability before successful DATA and frames.
4. **Atomic DATA group.** Apply each completion range in one Coordinator transaction. Accept a fully completed live range as an idempotent repeat. Reject unknown, partially completed or retired ranges. Verify isolation of old and new queues on same-segment reopen.
5. **Resource limits/lifetime:** in-flight Ticket/byte bounds, source-spool charges, FRONTIER credits and slot recycling preserve the existing safety and admission backpressure. No bounded channel can deadlock the Reaper.
6. **Wakeup.** Use the persistent token and consume-before-scan protocol for both waits. A first accepted digest wakes an owner whose DATA completion has already been applied. Cover concurrent publication, handoff, poison and shutdown.
7. **Durable ACK/crash:** no ACK before verified FRONTIER+fdatasync; correct source-charge retention on uncertain I/O; same PITR rotation, Active/Sealing/Sealed, crash cuts, recovery and old readers.
8. **Mixed Point/Batch:** shared Ticket Ring for any workload in one WAL segment; repeated interleaved Point/Batch, concurrent transactions and Batch64 regression tests. Whole-WAL-instance flag only.
9. **Resource and scheduling costs.** Preallocate the ring and use the same admission limit. Check for capacity starvation and apply Section 8's CPU measurement and latency gates.
10. **Terminal-state authority:** preserve the shared terminal poison decision across old Coordinator operations and new lock-light publishers; no new durable publication after poison; in-flight DATA may race with poison as in baseline. Normal close is not a poison.
11. **Recycling phase:** preserve `publish_durable_frontier` -> admission wake -> `retire_frontier_guards`; old guards never touch the ring.

## 8. Performance gates and stop rules

After deterministic correctness tests, screen Point8 and Point16 locally with both arms built into the same binary. Continue to GCP if both workloads gain throughput and lower p99, and Producer and Reaper progress publication avoids a shared mutex on each ticket.

Run five counterbalanced pairs per workload on one `c4-standard-16-lssd` with two Local SSDs in ext4 RAID0. Score Point1, Point8, Point16 and Batch64. Run Batch64 at 16 writers. Use 100,000 logical Puts and 1 KiB values. Build once in release mode with `bench`, use Direct FRONTIER, 1 MiB Point-only Lookahead and dynamic coalescing, and vary only the F0 flag. Exclude warm-up and use `--latency-sample-every 10` for each Point arm.

Set `TOYKV_BENCH_V8_EXTENT_POLICY_SCAN_DIAGNOSTICS=0` in both arms. The bench build enables these counters when the variable is absent. Reject a scored result unless `sliding_extent_preparation.extent_policy_scan_diagnostics_enabled` is `false` in both `wal_io_start` and `wal_io_end`. Keep CPU attribution, sync diagnostics, completion timelines and `perf` off for scoring. Use separate diagnostic runs for role CPU, owner lock hold time, per-wake drain counts and ring occupancy. See the [GCP benchmark runbook](../docs/wal/benchmarks/gcp-free-trial-wal-bench.md#scoring-v8-point-candidates) for scoring setup and artifact validation.

Archive raw results, the source revision and executable hash, the configuration and each pair. Report throughput, CPU/Put, Puts/Sync, DATA Groups/Put and Point p50/p95/p99. Use Batch64 throughput for its guard. Keep paired percentages as the median of the five within-pair changes, calculated as `(F0 / baseline - 1) × 100`.

Promote F0 only when all of these conditions hold.

| Check | Required result |
| --- | --- |
| Point8 and Point16 throughput | At least +5% paired median and at least four wins in five pairs for each workload |
| Point8 and Point16 p99 | At most +5% paired median for each workload |
| Point1 and Batch64 throughput | At least −2% paired median for each workload |
| Correctness | All Section 7 gates pass with F0 enabled, including the crash and recovery suite |

The throughput target is +10% Point16 under the same gates. If F0 fails the local screen or the promotion criteria, record the result and stop the candidate. A dedicated Advance/Flush owner belongs to a separate F1 proposal after F0 has passed.

## 9. Implementation status

The proposal specifies F0.1 as an isolated ring prototype. The integrated
runtime experiment requires the ownership, wakeup and crash tests in Section 7.
Section 8 defines when to advance from local testing to GCP and what constitutes
a passing A/B.

## Appendix A. Historical alternatives — not part of the implementation plan

This appendix consolidates the former progress-simplification RFC. F0 (Sections 3–8) is the **only active candidate**; neither A0 nor P0 is a prerequisite or fallback, and P1 requires an independently approved experiment. The [archived full analysis at commit 01c6c6e8](https://github.com/ben1009/toy-kv-engine/blob/01c6c6e87317d8e00379b5e1ca67c63fe0d52a5b/docs/wal/rfc-026-v8-point-progress-simplification.md) preserves the earlier detailed review history, not an alternative implementation specification.

| Historical candidate | Decision and rationale |
| --- | --- |
| A0: move resource reservation and aligned buffer allocation before MVCC sequencer | Deferred. Sampled CPU medians ~0.90us + ~0.48us at Point16 are not an upper bound on removable wall time and do not establish a large gain. V7 has an owned pre-reservation precedent, but V8 source-spool reclamation has different safety requirements. |
| P0: fuse digest/frame readiness in one Coordinator transaction | Superseded by F0. One fewer lock entry, but delays eager ordered digest folding; benefit unproven. |
| P1: dedicated durability owner | Deferred. Adds a thread and per-request handoff; does not remove existing coordination work by itself. Consider only as separately reviewed F1 after F0 demonstrates value. |
| Timed 3us DATA-group join | Rejected: ~31% fewer groups, Point16 -3.13%. |
| O(1) deque lookup alone | Rejected and reverted: Point16 +0.21%, Point8 -0.81%. |
| Inline completion on io_uring worker | Not selected: reduced CPU in some measurements, worsened concurrent Point tail latency. |

### Safety conclusions to preserve

- A0, if separately reconsidered, must keep all four quota-pressure classes (`LogicalWalLimit`, `SourceSpoolLimit`, `BufferMemoryLimit`, `SealIndexLimit`) on the original serialized fallback path. Never perform speculative `set_len`/sync ahead of ordered admission, or wait under MVCC `write_lock` for another unadmitted producer's reservation. Preserve exact reservation ownership, `mark_admitted` transition, clean pre-ticket timestamp retirement vs poisoned uncertain write, and `RotationRequired` mapping. These constraints do not authorize an A0 implementation in F0.
- P0 does not eliminate the need for ordered digest folding, and its conditional late-digest progress wake cannot be dropped. Preserve admission before encoding, producer guard registration before DATA submit, and existing durable publication.
- If F1 is ever separately approved, retain independently runnable Reaper and io_uring worker during fdatasync. Use a monotonic requested durable cutoff, explicit ownership and stop/join protocol, and outer poison-on-unexpected-exit guard. Do not give a background thread an Arc of a writer owning its own JoinHandle. Never hold Coordinator/submission/frontier locks while joining. No new thread is authorized by F0.
- Preserve the existing distinction between `Wal::sync()` / public `Wal::close()` (V8 sync barriers, not admission fences), externally fenced PITR rotation/seal, and final owner teardown. A closed Coordinator is not an unconditional drain guarantee: current drive logic can abort if its target remains undurable.
- Existing separated admission/readiness/follower notifications and published-cursor follower fast path are part of the baseline; do not undo retained changes while implementing F0.

### Prior measurements and reproducibility

- [Point producer critical path](../docs/wal/benchmarks/rfc-026-v7-v8-point-producer-critical-path-gcp-20261119.md) and [admission breakdown](../docs/wal/benchmarks/rfc-026-v8-point-admission-sequencer-gcp-20261119.md): instrumented V8 Point16 MVCC `write_lock` p50 wait ~44.5us vs V7 ~0.36us; hold ~8–10us. Wait time is not directly recoverable CPU or throughput.
- [Per-role CPU](../docs/wal/benchmarks/rfc-026-v7-v8-thread-role-cpu-gcp-20261120.md): Writer/packer +8.42us, completion/durability +6.79us, I/O worker +2.86us per Put; role scopes differ. [Writer stage CPU](../docs/wal/benchmarks/rfc-026-v7-v8-writer-stage-cpu-gcp-20261120.md): frame-readiness/submission +3.39us/Put, asymmetric scopes.
- [Timed group join](../docs/wal/benchmarks/rfc-026-v8-point-short-group-join-gcp-20261120.md), [ticket lookup](../docs/wal/benchmarks/rfc-026-v8-ticket-index-gcp-20261120.md), [notification separation](../docs/wal/benchmarks/rfc-026-v8-notification-rerun-20261102.md), [inline completion](../docs/wal/benchmarks/rfc-026-v8-worker-dispatch-20261102.md).

Section 8 defines the score configuration and promotion gates for F0.
