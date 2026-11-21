# RFC 026 Follow-up: V8 Point Progress Ring (PolarDB-Inspired Fast Lane)

| Field | Value |
| --- | --- |
| Status | **Proposed; design-only; blocked on ownership/wakeup proof** |
| Date | 2026-11-21 |
| Scope | V8 Parallel only; Point throughput target, shared Point/Batch correctness |
| Base | main following merged PR #558; supersedes the per-Ticket Arc/CompletionGroup proposal in the previous draft of this RFC |
| Parent | [RFC 026](../../rfcs/026-composable-wal-prefix-digest.md), [prior small-candidate decision record](rfc-026-v8-point-progress-simplification.md) |
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

Allocate the Ring **once per opened WAL segment**, with `capacity == max_inflight_tickets`; define `index(ticket) = ticket % capacity`. The segment incarnation plus *absolute ticket* is the identity; the modulo index alone is never a valid lookup proof. The backing allocation must be stable while the segment is live. Segment rotation or reopening creates a distinct incarnation and ring.

Logical states: `FREE -> ADMITTED -> DIGEST_READY and/or FRAMES_READY -> DATA_GROUP_COMPLETE -> DURABLE -> FREE`. Digest Ready and Frames Ready are independent monotonic producer events; the ordering constraint is that DATA group completion requires Frame Ready, while DATA may complete before the producer digest becomes available. A terminal error poisons the WAL; there is no retry or slot recycling through a poisoned generation.

The Ring is the **only mutable source** for raw Digest Ready, Frames Ready, and DATA Group Complete. The Coordinator retains the immutable admission plan, resource charges, order cursors, `folded_prefix_digest`, `prefix_digest_after`, FRONTIER reservations and durable ticket/image boundaries. Remove the separately mutable `TicketState.frames_ready`, `TicketState.digest_ready` and `TicketState.data_complete` replicas from the *fast-mode* decision path. The Coordinator's prefix validation, `frontier_ready_snapshot`, `reserve_frontier_with` and `publish_durable_frontier` must all use the same generation-checked ring accessor plus the ordered folded-digest cursor. **No mirror-back of each producer update under the Coordinator mutex.**

### Sixth review: publication/retirement linearization and terminal-state races

**P0 — Poison is not merely an asynchronous notification.** In current `parallel.rs`, `mark_digest_ready_with_cpu`, `mark_frames_ready_inner` and `mark_data_group_complete_inner` all validate `!state.poisoned` while holding the same Coordinator lock that publishes `poisoned` and wakes callers. A Ring publisher that only stores a Release-ready flag and increments `progress_epoch` can race with poison and appear to succeed after a terminal failure; a Producer could then continue toward `publish_data` using an already-poisoned ticket. The fast path must specify a **linearizable terminal-state gate** (a monotonic atomic poison state and/or per-slot state transition integrated with `poison_locked`) and prove every post-admission failure still prevents successful durability acknowledgement. A bare check before writing the flag is not enough: explicitly test poison between digest publication, frame publication, Producer Guard registration, DATA submit, Reaper dequeue and Advance. Clean `close` differs: it stops new admissions but permits previously admitted work to drain as allowed by the existing lifecycle.

**P0 — Existing durable publication opens the admission window *before* producer guards are disarmed.** `publish_durable_frontier` pops `CoordinatorState.tickets`, advances `durable_ticket_end`, and `signal_change` wakes capacity waiters under its Coordinator lock. The actual V8 Leader subsequently calls `retire_frontier_guards` and the guards' `disarm_after_durable_prefix_check`. A Ring keyed by `ticket % capacity` must allow new admission as soon as the old slot's **last mutable producer/worker reference is provably gone**, *not* simply when a guard was disarmed. For the first prototype, choose and document **one** of:

- Keep current admission-release timing and prove that after successful DATA group CQE and folded-digest consumption no old Producer, Reaper, worker or retained Group event can mutate the slot. Old `WalV8ParallelAdmissionGuard` objects may still exist, but must only consult their absolute ticket/durable cutoff and must never dereference the recycled slot.
- Or add a separate, explicitly signaled, bounded slot-retirement gate before reuse. This changes admission backpressure and may create an MVCC sequencer cycle if it waits for guard cleanup; prove no wait under `write_lock` for a guard held by a future/follower. Do not silently change the current `publish_durable_frontier` capacity guarantee.

In both cases, reset generation/payload/flags while admission owns the slot, publish the new absolute ticket with Release semantics **last**, and require every Producer/Reaper lookup to validate that identity before *and after* acquiring any cell guard. Add a forced-wrap test where durable publication, guard disarm, old-group completion delivery, and a new admission on the same index are interleaved. A terminal error must never overwrite the successor generation.

### Safety and layout requirements

- Start with preallocated slot cells (one array allocation per WAL) and **safe per-slot owned payload**. A small per-slot mutex protecting digest publication/reset, plus a Release-published readiness bit, is acceptable for the first correctness prototype; it contends only on a single ticket. Do not introduce a new `Arc<TicketSlot>` allocation for every Point or `UnsafeCell`-based recycling in F0.
- Admission assigns the exact segment incarnation, absolute ticket and immutable plan under the existing Coordinator lock *before* the producer can publish. The producer checks identity and publishes the entire Digest before the `DIGEST_READY` Release flag. Frame writes are complete before `FRAMES_READY` Release. The owner uses Acquire reads.
- Reclaim a slot **only after** its ticket falls below published durable end, all DATA results covering it have been applied, and no old producer/worker reference can still *mutate* the slot. A not-yet-disarmed guard may be safe only if it never touches the ring; see the explicit pre/post-guard-retirement race above. Avoid a `strong_count` heuristic or modulo-only lookup.
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
    ticket_generation: AtomicU64, // never treat index alone as ownership
    flags: AtomicU8,             // producer digest/frame, owner group-complete
    payload: Mutex<TicketPayload>, // per-cell, not Coordinator-wide
}
// Coordinator remains the only writer of folded prefix digests / FRONTIER.
~~~

## 4. Reaper completion batching — do not invent CompletionGroup handles

The real `parallel/worker.rs::GroupWriteResult` contains `group_id`, `tickets: Range<u64>`, `write_count`, `write_bytes`, and `error`. It **does not carry a CompletionGroup pointer**, and `group_id` is allocated by the worker. Requiring every Reaper completion to look up a new mutex-protected group registry simply replaces the old Coordinator mutex with another contested object.

Instead, reuse the existing **single Completion Reaper** as the producer of a bounded `CompletedGroupEvent { group_id, tickets, write_count, write_bytes }` channel. The *single elected prefix owner* consumes and validates each event under one Coordinator critical section when scanning progress. A successful event is applied to its entire ticket interval as a unit (no visible partial per-ticket completion); on partial duplicate, stale generation, wrong range, invalid frame state or conflicting result, poison using the same fail-stop contract as `mark_data_group_complete_inner`.

- The queue must have room for **at least `max_inflight_tickets`** successful completion events: each valid outstanding group covers at least one admitted Ticket, and tickets cannot be durably retired before their completion is applied. Formally prove capacity from the admission window and rejected duplicates. Queue overflow is a correctness/configuration failure: poison and wake followers; the Reaper must not block indefinitely behind an owner waiting for completions.
- The Reaper still **immediately poisons** on a failed DATA group, unexpected channel closure or worker exit, instead of waiting for a driver that might not be active. It must also use the same terminal poison state as lock-free Producer publication. Normal success only needs the bounded event queue and progress wake, not the Coordinator lock.
- Keep `GroupWriteResult.group_id` for observability only; **existing validation is range-based**, and no submitted-group-ID registry is present. Validate absolute ticket range, incarnation, framing, duplicate/partial completion and whole-Group atomicity during the **batched owner drain**; do not claim to authenticate `group_id`. This intentionally does **not** require new generic `IoWorker` protocol fields or per-group `Arc` ownership.
- Never treat a successful DATA CQE as durable. The owner must apply all earlier group results, fold digests and verify a contiguous prefix before reserving FRONTIER. A multi-ticket `WriteGroup` is completed atomically by applying one event while the Coordinator lock is held.
- Existing completion timeline and HOL sampling hooks must be adapted or explicitly disabled only for the opt-in experiment; production correctness and scored uninstrumented tests must not depend on diagnostic code.

### Event authenticity and exact validation

**P1 — `group_id` cannot be authenticated without a submission mapping.** The worker assigns `GroupWriteResult.group_id` inside `WorkerCore::enqueue` and the Reaper receives only the resulting ID, ticket range and counters. The existing `mark_data_group_complete_inner` validates contiguous admission, `frames_ready`, whole-group repetition and partial duplicates by **ticket range**, not by a stored `group_id -> range` registry. A proposed Reaper event can therefore retain `group_id` for diagnostics, but must **not claim** to prove its relationship to an admitted range without adding a durable/immutable submission descriptor.

For strict parity, validate ticket span, expected absolute ticket identities, complete Frame Ready, no unexpected segment/slot incarnation, and existing duplicate/partial-group semantics; `group_id` is diagnostic only. If a stronger unique group-ID check is desired, state its added submission metadata/CPU overhead and test separately. The event channel is internal to the same trusted worker/reaper pipeline, not an untrusted remote message boundary.

## 5. One ordered Advance operation and no-lost-wakeup signal

The existing driver elected by `acquire_durability_driver` remains the **only logical Advancer** in F0. On a wake or call to `frontier_ready_snapshot`, it performs one bounded action while holding Coordinator state:

1. Drain all currently queued successful `CompletedGroupEvent`s, validate each range, and mark their slots DATA-complete as one atomic group operation.
2. Fold Digest Ready slots from `folded_ticket_end` forward **in strict ticket order**, including digests whose DATA has not finished yet; preserve the original early-digest-fold overlap as far as current scheduling permits.
3. Scan the contiguous prefix in which **Frames Ready AND DATA Complete AND folded Digest** hold. An out-of-order completed ticket cannot bypass a gap. Produce a snapshot consistent with the same Coordinator-held admission, retirement and FRONTIER cursors.
4. Retain `reserve_frontier_with` -> verified FRONTIER physical write -> `fdatasync` -> `publish_durable_frontier` -> producer guard disarm -> ACK. No earlier acknowledgement or durable-watermark update.

The old `mark_digest_ready_with_cpu` eagerly folds in the producer thread. New elected-owner folding may add Digest HOL latency or delay a prefix during fdatasync: this is a **performance risk**, not a free win. Record owner lock tenure and how many digests/groups were drained per wake. If this regression dominates, reject F0 before adding another worker or hidden sync on Producer.

### Replace both readiness waits, not only their notifications

Current `wait_for_durability_change` and `coalesce_frontier_ready` park on Coordinator-locked conditions (`durability_changed` Condvar). Replacing their producer-side updates with atomics **without changing both wait paths** can lose progress indefinitely. The F0 fast mode needs one persistent, bounded progress notification channel plus a monotonic event epoch. Do not add a second channel while leaving old waits in place.

- Publisher: complete payload stores with Release ordering; increment `progress_epoch` with AcqRel; `try_send(())` to a capacity-one event token channel (a full token means work is pending, never block the Producer/Reaper). This also applies when Reaper successfully queues a completed group.
- Owner: read an Acquire epoch before scanning, drain events and contiguous prefix, then compare with an Acquire epoch after scanning. If the epoch changed, rescan without parking. If unchanged, consume/drain a pending token and recheck the epoch/prefix; only then wait for the token or existing timeout. A publish between the final check and parking must leave a token that wakes the owner. Verify races with a deterministic scheduler; an unguarded `Condvar::notify_all` is not the protocol.
- Existing adaptive FRONTIER coalescing remains bounded by the same deadline and rechecks the *same* event epoch/prefix. A new event does not restart the full coalescing budget. Close/poison signals both the token path and all durable followers; waiters recheck terminal flags under Coordinator.
- Only one elected driver may consume these events at a time. The wake token can persist across driver handoff. Followers of an already-elected driver retain the existing `durable_published` guard/condition logic; do not introduce per-Point notification tasks or polling timers.
- The driver must drain the completion queue even when no Digest Ready prefix exists; the queue cannot be allowed to fill merely because an earlier digest is pending.

- **Post-admission poison/close:** terminal poison races with Producer Digest/Frame publish and completed-group queue delivery cannot appear as a valid new success or submit an unguarded DATA request; normal close still permits its existing in-flight drain path.
- **Durable-publication/reuse interleave:** `publish_durable_frontier` wakes admission before `retire_frontier_guards`; reused Ring cell cannot be written by delayed old-generation activity.
- **Group ID parity:** `GroupWriteResult.group_id` remains diagnostics-only absent a new submission registry; test ticket-range duplicate and conflict outcomes.

Proof cases: event before first scan; between initial epoch and scan; after scan/before park; token full while another Producer publishes; DATA before Digest; later DIGEST before earlier ticket; coalescing with deadline; driver exit/poison; close; new admission while parked; ring reuse at window wrap.

## 6. Source-level patch sequence

| Patch | Files | Minimum acceptable change |
| --- | --- | --- |
| F0.1 Ring data model | `kv-engine/src/wal/v8/parallel.rs` | Preallocate generation-tagged slot ring. Keep old Coordinator publication as control mode; add parity tests, no per-Point heap allocation/unsafe reuse. Explicit single-source-of-truth transition required before F0.2 |
| F0.2 Completion events | `kv-engine/src/wal/v8/leader.rs`, `parallel.rs` | Successful Reaper Group results into bounded event queue; owner batch-drains and applies whole groups atomically; error still poisons immediately |
| F0.3 Producer progress | `leader.rs`, `parallel.rs` | Publish Digest/Frame via ring, serial owner folds and checks ready prefix; replace old per-Ticket Coordinator notifications without duplicating readiness fields |
| F0.4 Unified waits | `parallel.rs`, `leader.rs` | Rewire `wait_for_durability_change` AND `coalesce_frontier_ready` to epoch/token protocol, preserve follower/close/poison semantics |
| F0.5 Local & GCP A/B | existing bench/test harness | Deterministic concurrency + crash tests, then bounded local filter and paired uninstrumented GCP tests |

Do not add a dedicated Advance/Write/Flush/Notify thread in F0. Do not change DATA packing, `MAX_GROUP_TICKETS`, io_uring SQ/CQ, worker queue depth, extent preparation, default fdatasync coalescing, logical Batch API, on-disk format, LSN/Ticket meaning or backup/recovery semantics. V7 and V8 Leader remain untouched.

## 7. Required correctness gates

1. **Single state authority:** no stale `CoordinatorState.tickets` boolean can authorize/reject a FRONTIER differently from the Ring; all public snapshot, debug and digest-at-boundary queries are consistent.
2. **Memory model:** producer payload written before Release Ready; owner Acquire; generation checked on every lookup. No ABA, stale producer overwrite, torn digest or data race on reset.
3. **Ordered folding:** Digest chain contains every ticket exactly once, even if DATA completes out of order; early folded digest must not authorize durability before successful DATA and frames.
4. **Atomic DATA Group:** a multi-ticket Worker completion is applied as one transaction under Coordinator lock; unknown, duplicate, conflicting partial or late group responses poison identically to the old implementation.
5. **Resource limits/lifetime:** in-flight Ticket/byte bounds, source-spool charges, FRONTIER credits and slot recycling preserve the existing safety and admission backpressure. No bounded channel can deadlock the Reaper.
6. **No-lost-wakeup:** progress epoch/token protocol covers both durability wait and coalescing, concurrent publishing, handoff, poison, channel close and shutdown. No unbounded busy spin.
7. **Durable ACK/crash:** no ACK before verified FRONTIER+fdatasync; correct source-charge retention on uncertain I/O; same PITR rotation, Active/Sealing/Sealed, crash cuts, recovery and old readers.
8. **Mixed Point/Batch:** shared Ticket Ring for any workload in one WAL segment; repeated interleaved Point/Batch, concurrent transactions and Batch64 regression tests. Whole-WAL-instance flag only.
9. **CPU and fairness:** one ring array is preallocated, no per-Point `Arc`/group registry allocation; no pathological cache-line ping-pong, ring-capacity starvation or tail-latency regression.
10. **Terminal-state authority:** preserve the shared terminal poison decision across old Coordinator operations and new lock-light publishers; no ACK or invalid late DATA submit after poison. Normal close is not a poison.
11. **Recycling phase:** preserve safe `publish_durable_frontier` -> admission wake -> `retire_frontier_guards` ordering, or explicitly justify a new retirement gate and prove no deadlock.

## 8. Performance gates and stop rules

Use the existing same-machine, same-binary, off-by-default switch. After correctness tests, perform a **local Point8/Point16 filter**. Stop immediately if negative or if Reaper/Producer still enter a new shared mutex once per Ticket. A lowered lock-acquisition count is necessary but not sufficient; require actual throughput and p99 improvement.

Only if promising, run **five counterbalanced, uninstrumented paired GCP repetitions** per Point8 and Point16 using the existing c4-standard-16-lssd / 2 Local SSD ext4 profile. Preserve raw output, exact hashes, 1-KiB 100k-Put workloads, CPU/Put, Puts/Sync, DATA Groups/Put, p99, Point1 and Batch64 guards. Run diagnostic counters outside scored runs; include Producer/Owner/Reaper CPU breakdown, owner lock tenure, per-wake digest/group drain counts and ring occupancy.

- **Large-win target (not prediction):** >=10% stable Point16 throughput gain with no Point8 loss or systematic >5% Point p99 regression.
- **Minimum promotion threshold:** >=5% stable improvement on both Point8 and Point16 (at least 4/5 paired wins each), no Batch64 regression and all crash/correctness tests pass.
- If the ring adds per-Point costs and fails locally, **reject the design**. No speculative new dedicated thread or repeated microbenchmark tuning to rescue it.
- If this passes and high-concurrency throughput still lags, separately propose an F1 dedicated Advance/Flush topology, with its own ownership/close/Poison tests and A/B. It is explicitly *not* part of this F0.

## 9. Review disposition

**Primary proposal is now PolarDB-inspired bounded Progress Ring + batched Reaper events + elected-owner Advance.** The previous per-Ticket `Arc` and shared `CompletionGroup` pointer scheme is rejected as the default implementation, because it creates per-request ownership overhead and a new Group lookup contract.

This is a design-only Draft. It does not claim the PolarDB reported TPS uplift applies to V8 and it makes **no Rust runtime or production-default changes**. F0 coding requires the above ownership, memory ordering and no-lost-wakeup proof first.
