# RFC 026 Follow-up: V8 Point Progress Ring (PolarDB-Inspired Fast Lane)

| Field | Value |
| --- | --- |
| Status | **Reviewed design; concrete F0 prototype contract; implementation/tests pending** |
| Date | 2026-11-21 |
| Scope | V8 Parallel only; Point throughput target, shared Point/Batch correctness |
| Base | main following merged PR #558; supersedes the per-Ticket Arc/CompletionGroup proposal in the previous draft of this RFC |
| Parent | [RFC 026](../../rfcs/026-composable-wal-prefix-digest.md), [implementation plan](rfc-026-implementation-plan.md) |
| Target | >=10% uninstrumented Point16 gain without Point8 or p99 regression; not an expected/verified result |

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

Allocate the Ring **once per opened WAL segment**, with `capacity == max_inflight_tickets`; define `index(ticket) = ticket % capacity`. The segment incarnation plus *absolute ticket* is the identity; the modulo index alone is never a valid lookup proof. The backing allocation must be stable while the segment is live. Every writer owns a distinct in-memory ring and completion channel. Reopening the same persisted segment does not necessarily change its on-disk incarnation; do not alter that recovery contract. Drain/join the old worker and Reaper through the existing lifecycle, and never route old-writer events into a new ring.

Logical states: `FREE -> ADMITTED -> DIGEST_READY and/or FRAMES_READY -> DATA_GROUP_COMPLETE -> DURABLE -> FREE`. Digest Ready and Frames Ready are independent monotonic producer events; the ordering constraint is that DATA group completion requires Frame Ready, while DATA may complete before the producer digest becomes available. A terminal error poisons the WAL; there is no retry or slot recycling through a poisoned generation.

The Ring is the **only mutable source** for producer Digest Ready and Frames Ready. The existing Coordinator deque retains owner-only `data_complete`, its sole authoritative copy. The Coordinator retains the immutable admission plan, resource charges, order cursors, `folded_prefix_digest`, `prefix_digest_after`, FRONTIER reservations and durable ticket/image boundaries. Remove separately mutable `TicketState.frames_ready` and `TicketState.digest_ready` replicas from the *fast-mode* decision path; do not add a second DATA-complete flag to the ring. The Coordinator's prefix validation, `frontier_ready_snapshot`, `reserve_frontier_with` and `publish_durable_frontier` must all use the same generation-checked ring accessor plus the ordered folded-digest cursor. **No mirror-back of each producer update under the Coordinator mutex.**

### Concrete F0 safety decisions (supersede the previous review alternatives)

**1. Poison is a durability gate, not an I/O cancellation fence.** Existing `mark_digest_ready_with_cpu` and `mark_frames_ready_inner` check poison under Coordinator state, but `WalV8FrameJob::finish_with_point_direct_frame_and_phases` releases that lock before inserting the producer guard and calling `publish_data`; the latter does not hold the Coordinator lock across submission. Thus even the baseline can submit previously admitted DATA while racing terminal poison. Do not introduce a new global submission lock, per-event global CAS reservation protocol, or stronger guarantee of no physical I/O after poison. Preserve the real invariant: no new admission or durable-frontier publication ordered after terminal poison; no successful acknowledgement of an unpublished ticket. A success already linearized at a published durable boundary need not be revoked by later poison. Keep `poison_locked` and existing reservation/publication/driver/ACK predicates authoritative. A monotonic atomic poison hint, Release-set under the Coordinator poison transition and Acquire-read by producers, may reject already-poisoned work early; it is not a separate commit authority. Already admitted in-flight work can race with poison; retain existing guards and recovery semantics. Never acknowledge from the ring or event queue. Clean close follows existing driver/close semantics, not a new unconditional drain guarantee.

**2. Keep existing admission release timing; no new retirement gate.** The last mutable producer-slot operation is Frame Ready publication, before producer guard insertion and `self.io.publish_data`. A producer holds only its absolute assignment and writer/coordinator lifetime, not a retained mutable cell reference. No readiness write after DATA submission. The worker owns DATA buffers, not slots; the Reaper only queues absolute ticket ranges. The elected owner alone applies completion and consumes the prefix digests before durable publication. Consequently a valid old-generation mutating access cannot still be outstanding when its ticket becomes durable. Old `WalV8ParallelAdmissionGuard` cleanup uses absolute tickets and `poison_ticket_prefix_if_pending`, not the ring, and may run after index reuse. Keep current ordering: retire resource credits -> publish durable boundary/free admission window -> retire guards. Never wait for guard disarm under MVCC `write_lock`.

**3. Safe local ownership, not premature lock-free code.** Use preallocated cells with one per-cell mutex protecting the whole generation-tagged payload: occupied/free state, digest and frame bit. All reads/writes validate the absolute ticket inside that mutex. No check-then-unconditional atomic update against a recycled cell and no references escaping the cell lock. Keep `data_complete` owner-only in the existing Coordinator deque (the sole group-complete authority); all its readers hold the Coordinator mutex. Keep `prefix_digest_after` there as well. All frontier predicates consult this common representation. Avoid independent generation/flag atomics, unsafe payload reuse, and per-Ticket heap allocations in the first prototype. A truly lock-free cell layout is a separate experiment, not a prerequisite.

Test poison before/after local publication, guard insertion, DATA submission, and FRONTIER publication; published success vs later poison; capacity-one/two wrap with delayed guard disarm, stale producer event and stale group event; and same-segment reopen with separate runtime rings. Compare to existing fail-stop behavior.

### Safety and layout requirements

- Start with preallocated slot cells (one array allocation per WAL) and **safe per-slot owned payload**. A small per-slot mutex protecting the complete producer payload, generation, and readiness fields is the chosen safe F0 prototype; mutex release/acquire provides publication ordering. Do not introduce a new `Arc<TicketSlot>` allocation for every Point or `UnsafeCell`-based recycling in F0.
- Admission assigns the exact segment incarnation, absolute ticket and immutable plan under the existing Coordinator lock *before* the producer can publish. Within the cell mutex, the producer checks full identity and publishes the immutable digest or frame-ready state. The owner takes the same mutex to read it. Never hold borrowed payload after unlocking.
- Reclaim a cell as part of locked durable-prefix retirement; expose new admission capacity together with the published durable boundary. The last producer cell operation precedes DATA submit, completion was consumed and guard cleanup never touches the cell. A not-yet-disarmed guard is not a blocker. Avoid a `strong_count` heuristic or modulo-only lookup.
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
    payload: Mutex<TicketPayload>, // Option<absolute_ticket>, digest, frames_ready
    // No independently updated generation/flags in this prototype.
}
// Coordinator remains the only writer of folded prefix digests / FRONTIER.
~~~

## 4. Reaper completion batching — do not invent CompletionGroup handles

The real `parallel/worker.rs::GroupWriteResult` contains `group_id`, `tickets: Range<u64>`, `write_count`, `write_bytes`, and `error`. It **does not carry a CompletionGroup pointer**, and `group_id` is allocated by the worker. Requiring every Reaper completion to look up a new mutex-protected group registry simply replaces the old Coordinator mutex with another contested object.

Instead, reuse the existing **single Completion Reaper** as the producer of a bounded `CompletedGroupEvent { group_id, tickets, write_count, write_bytes }` channel. The *single elected prefix owner* consumes and validates each event under one Coordinator critical section when scanning progress. Apply a successful event to the existing owner-only `data_complete` field over its entire interval under Coordinator lock. Validate all members before changing any. Fully already-completed live ranges are idempotent in the baseline non-diagnostic path; partial duplicates, unknown/retired ranges, invalid frame readiness or invalid identity poison. A valid worker emits one successful result per disjoint submitted group; injected duplicates must distinguish live from retired ranges.

- The queue must have room for **at least `max_inflight_tickets`** successful completion events: each valid outstanding group covers at least one admitted Ticket, and tickets cannot be durably retired before their completion is applied. Capacity proof for valid traffic: assign each queued/in-delivery success one distinct still-admitted ticket in its nonempty disjoint worker range. Retirement cannot precede event application. Therefore queued plus in-delivery successes fit the admission window. Use nonblocking `try_send`; overflow on duplicate/injected events fail-stops rather than blocking. Never wait while holding Coordinator or submission locks. Queue overflow is a correctness/configuration failure: poison and wake followers; the Reaper must not block indefinitely behind an owner waiting for completions.
- The Reaper still **immediately poisons** on a failed DATA group, unexpected channel closure or worker exit, instead of waiting for a driver that might not be active. Preserve `poison_locked` as the terminal-state authority. Normal success only needs the bounded event queue and progress wake, not the Coordinator lock.
- Keep `GroupWriteResult.group_id` for observability only; **existing validation is range-based**, and no submitted-group-ID registry is present. Validate absolute ticket range, incarnation, framing, duplicate/partial completion and whole-Group atomicity during the **batched owner drain**; do not claim to authenticate `group_id`. This intentionally does **not** require new generic `IoWorker` protocol fields or per-group `Arc` ownership.
- Never treat a successful DATA CQE as durable. The owner must apply all group results covering the selected contiguous prefix, fold digests and verify a contiguous prefix before reserving FRONTIER. A multi-ticket `WriteGroup` is completed atomically by applying one event while the Coordinator lock is held.
- Existing completion timeline and HOL sampling hooks must be adapted or explicitly disabled only for the opt-in experiment; production correctness and scored uninstrumented tests must not depend on diagnostic code.

### Event authenticity and exact validation

**P1 — `group_id` cannot be authenticated without a submission mapping.** The worker assigns `GroupWriteResult.group_id` inside `WorkerCore::enqueue` and the Reaper receives only the resulting ID, ticket range and counters. The existing `mark_data_group_complete_inner` validates contiguous admission, `frames_ready`, whole-group repetition and partial duplicates by **ticket range**, not by a stored `group_id -> range` registry. A proposed Reaper event can therefore retain `group_id` for diagnostics, but must **not claim** to prove its relationship to an admitted range without adding a durable/immutable submission descriptor.

For strict parity, validate ticket span, expected absolute ticket identities, complete Frame Ready, no unexpected segment/slot incarnation, and existing duplicate/partial-group semantics; `group_id` is diagnostic only. If a stronger unique group-ID check is desired, state its added submission metadata/CPU overhead and test separately. The event channel is internal to the same trusted worker/reaper pipeline, not an untrusted remote message boundary.

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
- Only the elected owner may receive tokens. To wait: consume a pending token if present, then **always** advance/rescan under Coordinator and cell locks. If a suitable prefix exists or the original deadline expired, return that snapshot. Otherwise release Coordinator/cell locks and `recv_timeout(remaining)`; after a received token, loop and rescan. Never discard/drain a token after the final scan and then park without scanning again. Never hold Coordinator state while blocking in either queue or token receive.
- Proof: publication before token consumption is visible in the subsequent cell-locked/queue scan; if the scan misses concurrent publication, its later notification either occupies an empty token channel or finds an already pending token, either of which wakes the next receive. A publication delayed between state update and notify cannot lose progress; if the publisher fails before notifying an admitted operation, existing poison/error paths must signal terminal failure. On owner handoff, the next elected owner begins with consume + scan and any pending token persists. No unbounded spin or extra polling deadline.
- Normal Frame Ready publication alone does not make a ticket FRONTIER-ready; baseline only signals general `changed`, not `durability_changed`, in `mark_frames_ready_inner`. Do not generate a dedicated durability wake for it. DATA success triggers a token. Digest publication can signal to preserve early folding and handle late-digest cases; test unnecessary wakes versus baseline. Admission does not need a new readiness wake merely because it allocated a slot; preserve existing coalescing policy. Poison/close must signal both new readiness token and existing follower/admission waiters.
- Apply the same predicate and single absolute deadline as current adaptive coalescing; do not rescan indefinitely under a continuous stream of unrelated events. Each advance drains at most the bounded window of events, applies whole groups and releases the Coordinator lock before another round. If ready, use existing coalescing decision rather than chase token traffic.

Deterministic tests must cover full-token concurrent producers, publication between scan and park, late digest after DATA, close/poison, handoff and coalescing timeout under continued arrivals. Do not rely on an unguarded Condvar notify or timing-based sleeps for the correctness proof.

### Enablement is atomic at WAL-instance granularity

F0.1 may introduce the ring representation and isolated tests without routing the live pipeline. F0.2/F0.3 are implementation slices, not independently selectable runtime modes. Enable the whole-WAL bench switch only when **both** readiness wait sites, all producer publications, Reaper queue, advance, reservation/publication predicates, lifecycle and error paths are integrated and tested. No intermediate executable may defer Reaper completion while still parking on the old readiness Condvar. V8 Leader shares the Coordinator type but must continue using the original eager publication mode; do not change its synchronous group producer semantics.

## 6. Source-level patch sequence

| Patch | Files | Minimum acceptable change |
| --- | --- | --- |
| F0.1 Ring data model | `kv-engine/src/wal/v8/parallel.rs` | Preallocate mutex-protected generation-tagged producer cells; isolated wrap/identity tests; existing mode unchanged. No per-Point heap allocation or unsafe reuse |
| F0.2 Completion events | `kv-engine/src/wal/v8/leader.rs`, `parallel.rs` | Successful Reaper Group results into bounded event queue; owner batch-drains and applies whole groups atomically; error still poisons immediately |
| F0.3 Producer progress | `leader.rs`, `parallel.rs` | Publish Digest/Frame via ring, serial owner folds and checks ready prefix; replace old per-Ticket Coordinator notifications without duplicating readiness fields |
| F0.4 Unified waits | `parallel.rs`, `leader.rs` | Rewire both waits to persistent token + consume-before-scan protocol; enable the whole F0 runtime mode only after integrated tests |
| F0.5 Local & GCP A/B | existing bench/test harness | Deterministic concurrency + crash tests, then bounded local filter and paired uninstrumented GCP tests |

Do not add a dedicated Advance/Write/Flush/Notify thread in F0. Do not change DATA packing, `MAX_GROUP_TICKETS`, io_uring SQ/CQ, worker queue depth, extent preparation, default fdatasync coalescing, logical Batch API, on-disk format, LSN/Ticket meaning or backup/recovery semantics. V7 and V8 Leader remain untouched.

## 7. Required correctness gates

1. **Single state authority:** no stale `CoordinatorState.tickets` boolean can authorize/reject a FRONTIER differently from the Ring; all public snapshot, debug and digest-at-boundary queries are consistent.
2. **Memory model:** complete producer cell fields and absolute generation validated under the same cell mutex; owner reads under that mutex. No ABA, stale producer overwrite, torn digest or data race on reset.
3. **Ordered folding:** Digest chain contains every ticket exactly once, even if DATA completes out of order; early folded digest must not authorize durability before successful DATA and frames.
4. **Atomic DATA Group:** a multi-ticket Worker completion is applied as one transaction under Coordinator lock; identical fully completed live ranges remain idempotent; unknown, partial duplicate or retired ranges poison as in the old implementation.
5. **Resource limits/lifetime:** in-flight Ticket/byte bounds, source-spool charges, FRONTIER credits and slot recycling preserve the existing safety and admission backpressure. No bounded channel can deadlock the Reaper.
6. **No-lost-wakeup:** persistent-token consume-before-scan protocol covers both durability wait and coalescing, concurrent publishing, handoff, poison, channel close and shutdown. No unbounded busy spin.
7. **Durable ACK/crash:** no ACK before verified FRONTIER+fdatasync; correct source-charge retention on uncertain I/O; same PITR rotation, Active/Sealing/Sealed, crash cuts, recovery and old readers.
8. **Mixed Point/Batch:** shared Ticket Ring for any workload in one WAL segment; repeated interleaved Point/Batch, concurrent transactions and Batch64 regression tests. Whole-WAL-instance flag only.
9. **CPU and fairness:** one ring array is preallocated, no per-Point `Arc`/group registry allocation; no pathological cache-line ping-pong, ring-capacity starvation or tail-latency regression.
10. **Terminal-state authority:** preserve the shared terminal poison decision across old Coordinator operations and new lock-light publishers; no new durable publication after poison; in-flight DATA may race with poison as in baseline. Normal close is not a poison.
11. **Recycling phase:** preserve `publish_durable_frontier` -> admission wake -> `retire_frontier_guards`; old guards never touch the ring.

## 8. Performance gates and stop rules

Use the existing same-machine, same-binary, off-by-default switch. After correctness tests, perform a **local Point8/Point16 filter**. Stop immediately if negative or if Reaper/Producer still enter a new shared mutex once per Ticket. A lowered lock-acquisition count is necessary but not sufficient; require actual throughput and p99 improvement.

Only if promising, run **five counterbalanced, uninstrumented paired GCP repetitions** per Point8 and Point16 using the existing c4-standard-16-lssd / 2 Local SSD ext4 profile. Preserve raw output, exact hashes, 1-KiB 100k-Put workloads, CPU/Put, Puts/Sync, DATA Groups/Put, p99, Point1 and Batch64 guards. Run diagnostic counters outside scored runs; include Producer/Owner/Reaper CPU breakdown, owner lock tenure, per-wake digest/group drain counts and ring occupancy.

- **Large-win target (not prediction):** >=10% stable Point16 throughput gain with no Point8 loss or systematic >5% Point p99 regression.
- **Minimum promotion threshold:** >=5% stable improvement on both Point8 and Point16 (at least 4/5 paired wins each), no Batch64 regression and all crash/correctness tests pass.
- If the ring adds per-Point costs and fails locally, **reject the design**. No speculative new dedicated thread or repeated microbenchmark tuning to rescue it.
- If this passes and high-concurrency throughput still lags, separately propose an F1 dedicated Advance/Flush topology, with its own ownership/close/Poison tests and A/B. It is explicitly *not* part of this F0.

## 9. Review disposition

**Primary proposal is now PolarDB-inspired bounded Progress Ring + batched Reaper events + elected-owner Advance.** The previous per-Ticket `Arc` and shared `CompletionGroup` pointer scheme is rejected as the default implementation, because it creates per-request ownership overhead and a new Group lookup contract.

This is a design-only Draft. It does not claim the PolarDB reported TPS uplift applies to V8 and it makes **no Rust runtime or production-default changes**. F0.1 isolated state-machine implementation is now specified. Runtime enablement requires the deterministic correctness tests above; no performance claim until A/B.

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

- [Point producer critical path](benchmarks/rfc-026-v7-v8-point-producer-critical-path-gcp-20261119.md) and [admission breakdown](benchmarks/rfc-026-v8-point-admission-sequencer-gcp-20261119.md): instrumented V8 Point16 MVCC `write_lock` p50 wait ~44.5us vs V7 ~0.36us; hold ~8–10us. Wait time is not directly recoverable CPU or throughput.
- [Per-role CPU](benchmarks/rfc-026-v7-v8-thread-role-cpu-gcp-20261120.md): Writer/packer +8.42us, completion/durability +6.79us, I/O worker +2.86us per Put; role scopes differ. [Writer stage CPU](benchmarks/rfc-026-v7-v8-writer-stage-cpu-gcp-20261120.md): frame-readiness/submission +3.39us/Put, asymmetric scopes.
- [Timed group join](benchmarks/rfc-026-v8-point-short-group-join-gcp-20261120.md), [ticket lookup](benchmarks/rfc-026-v8-ticket-index-gcp-20261120.md), [notification separation](benchmarks/rfc-026-v8-notification-rerun-20261102.md), [inline completion](benchmarks/rfc-026-v8-worker-dispatch-20261102.md).

For F0, follow Section 8, **not** the historical A0/P0 promotion threshold. Preserve the November 20 GCP profile: 16 vCPU C4, two Local SSDs ext4 RAID0, release with bench feature, Direct FRONTIER, 1 MiB Point lookahead and dynamic coalescing. Same binary and flags except F0, no scored detailed instrumentation, record executable SHA-256 and exact source/environment, exclude warm-up, sample Point latency identically (every tenth operation), report paired deltas and p50/p95/p99. Include Point1 and Batch64 throughput guards (no systematic >2% loss) and do not infer Batch64 p99 from too few samples.
