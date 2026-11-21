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

## Candidate F0: bounded lock-light ticket progress slots

**Do not alter the V8 physical record format, fixed 4-KiB DATA/FRONTIER frame, digest algorithm, admission-before-encoding rule, physical offsets, crash recovery or ACK requirement.**

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

The slot's identity includes the full segment incarnation and absolute ticket; an old slot from a wrapped window must never alias a new admission. A straightforward safe prototype can use a fixed admission-window array of `ArcSwapOption<Arc<TicketSlot>>`-equivalent slots, or per-ticket `Arc<TicketSlot>` handles held by producer and reaper. **Do not start with unsafe slot recycling.** If lookup by ticket still requires the Coordinator mutex or copies the window on every readiness event, the candidate has failed its main objective.

Admission must publish the initialized slot before any producer can submit DATA for that ticket. Bounds must remain `max_inflight_tickets` and `max_inflight_bytes`; recycle only after successful durable-frontier publication *and* any required producer/worker references are gone.

### Producers and Completion Reaper

- On logical encode completion, publish the immutable digest into the ticket slot and set `DIGEST_READY` with Release ordering, preserving the current **early digest-ready** opportunity rather than delaying it until framing.
- After the frame is wholly materialized, publish `FRAMES_READY` with Release ordering, then register the existing producer guard and submit DATA exactly as now.
- The independent Completion Reaper validates successful CQE, group ticket range and slot incarnation, then publishes `DATA_CQE` with Release ordering to each slot. Never publish DATA completion on failed or uncertain writes.
- The ordered prefix owner reads flags with Acquire ordering. Preserve duplicate/conflicting event rejection, poison semantics and dependency ordering: `DATA_CQE` requires `FRAMES_READY`, while digest and DATA completion may arrive in either order.
- Keep existing `WriteGroup`, io_uring worker, physical DATA layout and bounded completion channel unchanged for F0. Do not add a timed DATA group join.

### Prefix folding, signals and durability

Exactly **one logical prefix owner** performs ordered `fold_ready_digests` over slots with `DIGEST_READY`. It separately advances the contiguous frontier-eligible prefix over `FRAMES_READY | DATA_CQE | folded_digest`. It may fold a digest before the corresponding DATA write is complete, preserving today's overlap.

For the first F0 prototype, **keep the existing writer-elected durability driver**; the purpose is to remove multiple per-ticket Coordinator-lock publications, not to test new threading and status storage at the same time. Extract a bounded ticket-slot front and prefix scanner that the existing driver can consume. The remaining Coordinator mutex must still serialize admission, physical FRONTIER reservations, durable-watermark publication, and terminal poison/close decisions. Moving every function to atomics would weaken correctness without a clean performance hypothesis.

If per-slot events no longer take the coordinator mutex, the durability driver needs an explicit no-lost-wakeup mechanism. A bounded one-token progress channel (send nonblocking on status advancement; consume/drain then recheck predicates) is one possible implementation. Before coding, specify how the current elected driver and any future owner receive these events, and show that completion-before-digest, digest-before-frame, ticket-window reuse, channel closure, poison and close cannot leave all waiters asleep. Never assume `Condvar::notify_all` without the corresponding locked predicate is sufficient.

**Semantics remain:** admitted Ticket -> successful DATA CQE -> contiguous digest fold + frame readiness -> verified FRONTIER write -> successful `fdatasync` -> publication of durable ticket end -> user ACK. No earlier success is allowed.

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
4. **Data-complete identity:** a completed group may include multiple tickets and physical FRONTIER gaps. Handle each ticket with its correct segment incarnation and reject unknown/partially duplicated completions; do not confuse a CQE with durable DATA.
5. **Ordered digest fold:** a later ready ticket cannot bypass an earlier missing digest; after an out-of-order completion, progress must occur when the missing predecessor eventually publishes.
6. **No lost progress:** a readiness event arriving between a driver's last scan and its wait cannot be missed. Poison/close/worker exit must also wake followers.
7. **Durable and crash semantics:** immutable predecessor digest, FRONTIER offset and reservation credit must match the existing code. Preserve all crash cuts, failpoints, old-reader compatibility, Active normalization, PITR seal/rotation and restart.
8. **Batch/mixed usage:** first benchmark F0 under an explicitly chosen, off-by-default, **Point-only mode**. This is NOT production-ready until mixed Point+Batch tickets use the same safe slot protocol or switch to the unchanged path with proven lifetime/ordering equivalence. Do not silently change Batch's semantics.

## Performance experiment and stop

Do **not** add another multi-stage CPU attribution campaign. First implement and test only F0 behind an off-by-default benchmark flag, then one local Point8/16 screen. If the local screen is clearly negative or if the candidate leaves the same Coordinator-lock round trips in place, revert without cloud spend.

If promising, use five counterbalanced, uninstrumented paired GCP runs per Point8 and Point16 on the existing c4-standard-16-lssd / 2 Local SSD ext4 profile, 100k 1-KiB puts, identical binary/source when toggling the benchmark mode. Include Point1 and Batch64 regression guards, sufficient Point p99 samples (1/10), CPU/Put and Puts/Sync. Preserve raw runs and hashes. Compare with the unchanged V7 baseline separately to show whether the target is recovered.

- **Big-win objective:** >=10% stable Point16 throughput gain, no Point8 loss, no systematic >5% Point p99 regression, no Batch64 regression or crash/correctness failure. The actual V7 gap may require more than 10%; this is a **target**, not a prediction.
- **Promotion threshold:** >=5% stable gain at Point16 *and* Point8, at least 4/5 paired wins per cell, p99 guard passed and safety proof complete. Otherwise the large complexity of this architectural change is not justified.
- If F0 produces an unambiguous improvement but below the big-win target, document it and independently decide whether F1 is worth its extra thread; do not automatically chain flags, tune wait budgets or run F1 without design review.
- If F0 fails, retain current V8 and explicitly reconsider the format/runtime complexity and product need for V8; do not fall back to another 1-us hotspot search.

**This RFC changes no runtime or production default. It is an architectural hypothesis with an explicit correctness and performance rejection gate.**
