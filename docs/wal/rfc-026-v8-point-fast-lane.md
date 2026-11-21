# RFC 026 Follow-up: V8 Point Fast Lane (Structural Performance Candidate)

| Field | Value |
| --- | --- |
| Status | **Proposed; design-only, no implementation** |
| Date | 2026-11-21 |
| Baseline | Current V8 Parallel at merged PR #558, source audited on `main` |
| Parent | [RFC 026](../../rfcs/026-composable-wal-prefix-digest.md), [earlier optimization decision record](rfc-026-v8-point-progress-simplification.md) |
| Objective | **Attempt >=10% uninstrumented Point16 throughput improvement without a p99 or Point8 regression** |

## Why a different candidate

At 16 concurrent 1-KiB Point puts, five uninstrumented paired GCP comparisons measured V7 about 48k/s and V8 about 41k/s (V8 about 14% behind V7, equivalent to needing roughly 16-20% uplift from its own baseline to catch V7). V8 spends around 17-20 more process CPU microseconds per Put. The difference is spread across producer, completion, I/O and durability roles; these CPU scopes do not add up to a proven critical path.

The existing RFC #559 A0 proposal only moves `reserve_batch_resources` and `DirectBufAllocation::new` outside the MVCC sequencer; their independently sampled CPU medians were roughly 0.90 and 0.48 microseconds at Point16. That is not a credible stand-alone explanation for the measured total gap, and quota/rotation safety makes the change less attractive.

Other failed approaches:
- O(1) deque ticket lookup: +0.21% at Point16, -0.81% at Point8; reverted.
- Point 3-us DATA group join: -3.13% at Point16 even though worker groups fell 31% and Puts/Sync barely changed.
- Inline DATA completion on the io_uring worker: sometimes reduced CPU but worsened concurrent Point p99.
- P1 dedicated durability driver **alone** relocates work and introduces another scheduling step; it does not remove per-ticket status transitions.

The structural target is the repeated **shared-coordinator state publication** after admission. Today the producer acquires the Coordinator mutex to publish digest readiness and again to publish frame readiness; after I/O, the Completion Reaper acquires the same Coordinator mutex to mark DATA completion. Each publication validates/searches tickets and may notify waiters. The driver acquires the Coordinator to fold the prefix, reserve and publish a FRONTIER. This design makes concurrent Point progress contend with serial ticket admission on the same state mutex.

## Exact batching objective: batch **coordination**, not Point DATA

A Point remains one API operation with one ordered commit timestamp and ticket. The design does not require callers to form a logical KV batch and does not delay a one-frame DATA write in order to pack a larger `WriteGroup`. Historical 3-us grouping reduced worker groups by about 31% while hurting Point16 throughput by 3.13%, so no timed DATA join belongs in F0.

The real control-plane target is:

- **Current:** one Coordinator lock acquisition for each producer digest-ready event, another for frame-ready, and another for each Reaper DATA-completion group, plus frontiers and admission.
- **F0:** producer and Reaper publish state in stable ticket/group storage without acquiring that Coordinator lock; the **existing elected durability driver**, not a new thread, drains a contiguous ready prefix in ticket order and updates the folded digest and frontier-eligible prefix in a **single bounded scan**. It may cover many ready tickets without waiting for additional arrivals.
- **Unchanged:** ordered admission, immediate Point DATA submit, MAX_GROUP_TICKETS and byte-window limits, io_uring worker, separate reaper, adaptive FRONTIER coalescing, `fdatasync`, durable ACK, recovery and mixed Point/Batch ticket order.

The goal is fewer **shared coordinator mutex entries per Point**, not fewer syscalls via artificial data-group waiting. Reduced CPU/Put or worker notifications alone is not sufficient to accept the architecture; require a throughput and p99 gain.

## Candidate F0: bounded lock-light ticket progress slots

**Do not alter the V8 physical record format, fixed 4-KiB DATA/FRONTIER frame, digest algorithm, admission-before-encoding rule, physical offsets, crash recovery or ACK requirement.**

### Blocking code-review findings: one authority and one wake path

The merged V8 implementation's `CoordinatorState.tickets: VecDeque<TicketState>` stores `frames_ready`, `digest_ready`, `data_complete` and `prefix_digest_after`. `frontier_ready_snapshot`, `reserve_frontier_with` and `publish_durable_frontier` all validate those fields under the same Coordinator lock. **Copying status into separate atomics without changing these readers is incorrect; copying status back under that lock on every producer/Reaper publication erases the intended gain.**

**Required representation decision before F0.1:** make the stable TicketSlot **the sole owner** of published raw digest, frame-ready and group-complete state. Keep only ordered prefix digest (`prefix_digest_after`), ticket assignment, reservation credit and durable-frontier ownership in `CoordinatorState`, since the elected driver advances these under its serializing mutex. Replace every `TicketState::is_frontier_ready` and prefix/snapshot/FRONTIER validation path with one common accessor that consults that immutable slot/group status plus the authoritative folded digest. Never maintain a second, independently mutable `frames_ready` or `data_complete` bit in the Coordinator.

Current `mark_digest_ready_with_cpu` also **folds predecessor digests immediately in the producer thread**, before the producer frames the DATA. F0 changes this scheduling: the elected driver must perform the same ordered fold before reserving FRONTIER, including a full contiguous digest prefix that extends beyond the current DATA-ready prefix. Account for possible extra digest HOL latency and changed CPU role; the design does **not** simply eliminate hash work.

**The notification change is a second implementation blocker:** both `coalesce_frontier_ready` and `wait_for_durability_change` currently park on `CoordinatorInner.durability_changed` after inspecting coordinator-locked readiness predicates. A fast-mode slot publisher cannot continue relying on those waits unchanged. Route *both* waits through one event-epoch + persistent-token protocol, keeping `durable_published` for followers and the Coordinator lock for close/poison. An atomic event epoch alone or an unguarded Condvar notify is insufficient. If preserving adaptive coalescing requires the old per-Point locked notifier, reject F0 before the benchmark.

### Reaper handle handoff is not free

The actual worker output is `parallel/worker.rs::GroupWriteResult { group_id, tickets, write_count, write_bytes, error }`; it **does not contain an owned CompletionGroup handle**. Today the reaper thread receives only that result and a clone of `WalV8ParallelCoordinator` (see `WalV8ParallelIo::spawn` / `reap_v8_group_result`).

Before claiming that Reaper completion can bypass the Coordinator lock, the prototype must choose one concrete no-serialization lookup:

- Have `submit_group_locked` install an immutable shared group descriptor for its entire ticket range in stable per-ticket slots **before** calling `client.submit_group`; the Reaper obtains that group through an Acquire-read of the bounded slot table using `result.tickets.start`, checks the full ticket interval, incarnation, buffer count and group completion generation, then Release-marks the group complete.
- Or extend the generic worker completion protocol to carry an opaque group descriptor end-to-end, with documented impact on V7/other worker clients. Do **not** pretend that is limited to `v8/parallel.rs` and `v8/leader.rs`.

A separate mutex-protected `group_id -> CompletionGroup` map on each single-Ticket result is **not an accepted F0 fast path** unless measurements show it is materially cheaper than the removed Coordinator lock. The worker assigns `group_id` on enqueue, so a pre-submission descriptor cannot validate a group ID that does not yet exist; use a validated ticket interval + segment incarnation for the attachment and check worker-issued identity at completion where available.

The slot/range table must not be retired while any producer, submitted worker group or delayed result can still reference it. On duplicate, unknown, retired-generation or conflicting group results, preserve existing fail-stop behavior. Publication of one group marker remains atomic for the entire WriteGroup.

### Slot identity and lifetime

On ordered admission under the existing MVCC sequencer, reserve a stable, bounded `TicketSlot` for each assigned ticket:

```rust
// Design sketch, not compile-ready implementation.
struct TicketSlot {
    assignment: WalV8BatchAssignment,   // immutable after admission
    digest: OnceLock<WalV8DigestReady>, // written once by its producer
    readiness: AtomicU8,                // DIGEST, FRAMES, DATA_CQE
    // resource/producer guard ownership remains in the existing structures
}
```

The slot's identity includes the full segment incarnation and absolute ticket; an old slot from a wrapped window must never alias a new admission. A straightforward safe prototype can use a fixed admission-window table of safely published `Arc<TicketSlot>` handles (for example an `ArcSwapOption`-style slot holder with generation checks). Producers and submission groups retain ownership as necessary; do not require the Reaper to reacquire the Coordinator mutex to map a completed ticket range. **Do not start with unsafe slot recycling.** If lookup by ticket still requires the Coordinator mutex or copies the window on every readiness event, the candidate has failed its main objective.

Admission must publish the initialized slot before any producer can submit DATA for that ticket. Bounds must remain `max_inflight_tickets` and `max_inflight_bytes`; recycle only after successful durable-frontier publication *and* any required producer/worker references are gone.

### Producers and Completion Reaper

- On logical encode completion, publish the immutable digest into the ticket slot and set `DIGEST_READY` with Release ordering, preserving the current **early digest-ready** opportunity rather than delaying it until framing.
- After the frame is wholly materialized, publish `FRAMES_READY` with Release ordering, then register the existing producer guard and submit DATA exactly as now.
- The independent Completion Reaper validates successful CQE, group ticket range and slot incarnation. **Crucial: retain the existing atomic GROUP completion semantics, not merely individual slot flags.** Associate every submitted `WriteGroup` with a bounded immutable ticket-range group-completion handle. The packer installs this group identity on all its member slots before submission; after successful CQE, the Reaper validates the entire range and publishes **one Release group-complete marker**. The prefix owner treats `DATA_CQE` as true for a ticket **only when its containing group's marker is published** (Acquire). This prevents observing a partially completed group. A single-ticket group naturally has the same semantics. Never publish completion on failed or uncertain writes.
- The ordered prefix owner reads flags and group-completion markers with Acquire ordering. Preserve duplicate/conflicting event rejection, poison semantics and dependency ordering: a published DATA group completion requires all its tickets to be frame ready, while digest and DATA completion may arrive in either order. A group-completion failure poisons all tickets, not just a prefix.
- Keep existing `WriteGroup`, io_uring worker, physical DATA layout and bounded completion channel unchanged for F0. Do not add a timed DATA group join.

### Prefix folding, signals and durability

Exactly **one logical prefix owner** performs ordered `fold_ready_digests` over slots with `DIGEST_READY`. It separately advances the contiguous frontier-eligible prefix over `FRAMES_READY | DATA_CQE | folded_digest`. It may fold a digest before the corresponding DATA write is complete, preserving today's overlap.

For the first F0 prototype, **keep the existing writer-elected durability driver**; the purpose is to remove multiple per-ticket Coordinator-lock publications, not to test new threading and status storage at the same time. Extract a bounded ticket-slot front and prefix scanner that the existing driver can consume. The remaining Coordinator mutex must still serialize admission, physical FRONTIER reservations, durable-watermark publication, and terminal poison/close decisions. Moving every function to atomics would weaken correctness without a clean performance hypothesis.

If per-slot events no longer take the coordinator mutex, the durability driver needs an explicit no-lost-wakeup mechanism. A bounded one-token progress channel (send nonblocking on status advancement; consume/drain then recheck predicates) is one possible implementation. Before coding, specify how the current elected driver and any future owner receive these events, and show that completion-before-digest, digest-before-frame, ticket-window reuse, channel closure, poison and close cannot leave all waiters asleep. Never assume `Condvar::notify_all` without the corresponding locked predicate is sufficient.

**Semantics remain:** admitted Ticket -> successful DATA CQE -> contiguous digest fold + frame readiness -> verified FRONTIER write -> successful `fdatasync` -> publication of durable ticket end -> user ACK. No earlier success is allowed.

### Submission/CompletionGroup ownership and race proof

The producer's `DIGEST_READY` and `FRAMES_READY` bits are independent of I/O grouping. The `CompletionGroup` handle is attached only when `submit_group_locked` has selected a contiguous ticket range and validated physical FRONTIER gaps. The group identity is immutable once attached; the Reaper must not receive a successful group result before every member slot has its group identity safely published. On any post-admission error, poison and wake all followers rather than leaving a slot with an orphaned group handle.

Check these exact interleavings deterministically: (1) DATA CQE arrives while the last group member is being registered; (2) a group covers tickets N..N+K and the scanner is simultaneously reading the head; (3) group error after other groups complete out of order; (4) an identical completion is repeated versus a conflicting partial completion; (5) durable retirement releases group handles only after the published watermark covers the whole included prefix. A per-ticket `fetch_or(DATA_CQE)` loop without a group-level release barrier is **not an acceptable F0 prototype**.

### F0 wakeup integration with current writer-elected driver

The existing `drive_frontier_through` calls `frontier_ready_snapshot()` and waits in `wait_for_durability_change()` when no ready prefix exists; its `coalesce_frontier_ready()` path also relies on coordinator-locked notifications. **Fast mode must replace both readiness wait sites, not run a parallel signal channel beside the unmodified Condvar.** **Simply replacing producers' `signal_change/signal_durability_progress` with atomic flags would introduce a lost-wakeup risk**, since the current Condvar predicate would no longer be updated under its mutex.

F0 must implement *one* explicit progress notification protocol rather than a new per-event blocking lock:

1. Publish slot fields with Release ordering, then notify using a bounded persistent token (e.g., nonblocking `try_send` into a capacity-one channel). A full channel means progress is already pending, not dropped; the token must remain observable until an elected driver drains and rescans.
2. The elected driver scans and folds all currently contiguous digests, derives the DATA-complete/frame-ready prefix, then checks for the token **after** its scan before parking. Handle a token published between scan and park and one published during coalescing. Do not block a writer on token capacity. It must keep all current poison/closed/durable watermark checks.
3. Integrate with existing adaptive coalescing rather than adding another timeout or new DATA join. This needs one authoritative logic path for the driver when digest-ready or DATA-complete arrives late.
4. **Do not add a new progress thread in F0.** The elected writer remains the only one that folds/reserves FRONTIER. Followers continue waiting for the durable-published cutoff; poison and shutdown must continue waking them using the existing authoritative state.

Before implementation, specify a small state-machine proof for: notification arrives before initial scan, between scan/park, between coalescing snapshot/recheck, DATA CQE before a missing digest, and newly admitted ticket after the driver has parked. If no safe integration is possible without effectively restoring a shared lock on every Point event, reject F0 rather than adding a polling loop.

For the prototype, use a **whole-WAL-instance off-by-default mode switch** so Batch and Point ticket streams never use incompatible status backends within the same Segment. The initial performance target is Point8/16, but mixed Point/Batch and Batch64 are mandatory correctness/regression guards. A whole-WAL mode is not a promise that Batch throughput improves.

## Optional F1 only after F0 wins: dedicated progress owner

If F0 demonstrates a real reduction in CPU and shared-state contention but writer-driven progress still holds Point latency back, **independently** evaluate moving only the ordered fold/FRONTIER drive to a dedicated owner. Keep the independent Completion Reaper alive while sync blocks. The progress owner then combines coalesced ready-event handling with durability publication; it does not own or drain io_uring CQEs.

F1 must preserve the existing dynamic coalescing/sync policy and current `Wal::close()` semantics (a sync barrier, not Coordinator shutdown). Add a monotonic requested-durable cutoff and prove request-versus-park, recovery after unexpected owner exit, explicit stop/join, quota/producer-guard retirement and no self-`Arc` cycle. Unlike the old P1 proposal, F1 is **not permitted before F0 demonstrates that its change removes actual coordination work**. Moving `fdatasync` alone is not the experiment.

## Code slices and data ownership

| Slice | Files | Intended change |
| --- | --- | --- |
| F0.1 | `kv-engine/src/wal/v8/parallel.rs` | Stable bounded TicketSlot; immutable identities; split transient ready flags from admission/durable state |
| F0.2 | `kv-engine/src/wal/v8/leader.rs` | Publish digest/frame flags without Coordinator-wide mutex; pass/locate exact TicketSlot through reaper while maintaining group CQE validation |
| F0.3 | `kv-engine/src/wal/v8/parallel.rs`, `leader.rs` | Single ordered prefix-fold path, no-lost-wakeup progress event, unchanged FRONTIER reserve/write/sync/ACK |
| F0.4 | Existing V8 tests and chaos suite | Prove failure, rollover, rotation, group CQE and crash invariants |
| F1, later | `leader.rs`, `parallel.rs` | Optional independently gated durability scheduling topology; NOT included in F0 A/B |

### Implementation reviews that block F0

1. **Slot reuse:** no in-flight producer, CQE, digest computation or FRONTIER retirement can access a recycled slot. Typed `Arc` or generational safety is mandatory.
2. **Release/Acquire:** all digest bytes and complete DATA-frame construction precede their published status flags, and status reads do not see an earlier generation.
3. **Admitted work cannot vanish:** a dropped FrameJob after admission poisons and wakes every waiter; canceling an unadmitted preparation releases its resource charge.
4. **Atomic DATA group completion:** a completed WriteGroup may contain multiple tickets and physical FRONTIER gaps. The current `mark_data_group_complete_inner` updates the entire group atomically under a single Coordinator lock; F0 MUST preserve that observable behavior using one release-published completion-group marker and never expose partial per-ticket completion to the frontier scanner. Validate ticket range, incarnation and duplicate group result; do not confuse CQE with durability.
5. **Ordered digest fold:** a later ready ticket cannot bypass an earlier missing digest; after an out-of-order completion, progress must occur when the missing predecessor eventually publishes.
6. **No lost progress:** a readiness event arriving between a driver's last scan and its wait cannot be missed. Poison/close/worker exit must also wake followers.
7. **Durable and crash semantics:** immutable predecessor digest, FRONTIER offset and reservation credit must match the existing code. Preserve all crash cuts, failpoints, old-reader compatibility, Active normalization, PITR seal/rotation and restart.
8. **Batch/mixed usage:** first benchmark F0 under an explicitly chosen, off-by-default, **Point-only mode**. This is NOT production-ready until mixed Point+Batch tickets use the same safe slot protocol or switch to the unchanged path with proven lifetime/ordering equivalence. Do not silently change Batch's semantics.

## Smallest implementation order

**PR-F0.1 — safe state representation only:** Introduce a bounded, incarnation-checked TicketSlot/CompletionGroup representation and baseline parity tests. Keep the old coordinator publications for this first patch so wire format, resource ownership and diagnostics are unchanged. Measure any new Arc/allocation/cache-line cost; do not introduce unsafe slot reuse or per-Ticket heap allocation into the final design without demonstrating it is affordable.

**PR-F0.2 — remove hot-path publications:** Once F0.1 is correct, route `mark_digest_ready`, `mark_frames_ready` and group CQE completion through the stable slot/group handles. Refactor `fold_ready_digests` and the current `frontier_ready_snapshot` to drain all contiguous ready entries once under the existing owner's serialized control. Leave `reserve_frontier_with` and `publish_durable_frontier` behind the existing Coordinator lock. Keep the Reaper independent and preserve multi-ticket group atomicity.

**PR-F0.3 — wakeup correctness and performance A/B:** Wire the durable-progress event to the existing elected driver's readiness/coalescing wait. Add deterministic missed-wakeup, mixed-mode, out-of-order, poison, crash-cut, retirement/reuse and close tests. Only after those pass perform a small local screen, then five counterbalanced no-tracing GCP pairs. The benchmark must report at least Coordinator acquisitions/Put (a separate diagnostic run), process CPU/Put, worker groups/Put, Puts/Sync, Point8/16 throughput and p99, Point1 and Batch64 regressions.

Never change logical API batching, DATA join wait, queue depth, I/O worker notification, durability scheduler and readiness state backend in the same experiment. F0 does not earn promotion by lowering mutex count alone.

## Performance experiment and stop

Do **not** add another multi-stage CPU attribution campaign. First implement and test only F0 behind an off-by-default benchmark flag, then one local Point8/16 screen. If the local screen is clearly negative or if the candidate leaves the same Coordinator-lock round trips in place, revert without cloud spend.

If promising, use five counterbalanced, uninstrumented paired GCP runs per Point8 and Point16 on the existing c4-standard-16-lssd / 2 Local SSD ext4 profile, 100k 1-KiB puts, identical binary/source when toggling the benchmark mode. Include Point1 and Batch64 regression guards, sufficient Point p99 samples (1/10), CPU/Put and Puts/Sync. Preserve raw runs and hashes. Compare with the unchanged V7 baseline separately to show whether the target is recovered.

- **Big-win objective:** >=10% stable Point16 throughput gain, no Point8 loss, no systematic >5% Point p99 regression, no Batch64 regression or crash/correctness failure. The actual V7 gap may require more than 10%; this is a **target**, not a prediction.
- **Promotion threshold:** >=5% stable gain at Point16 *and* Point8, at least 4/5 paired wins per cell, p99 guard passed and safety proof complete. Otherwise the large complexity of this architectural change is not justified.
- If F0 produces an unambiguous improvement but below the big-win target, document it and independently decide whether F1 is worth its extra thread; do not automatically chain flags, tune wait budgets or run F1 without design review.
- If F0 fails, retain current V8 and explicitly reconsider the format/runtime complexity and product need for V8; do not fall back to another 1-us hotspot search.

**This RFC changes no runtime or production default. It is an architectural hypothesis with an explicit correctness and performance rejection gate.**
