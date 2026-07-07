# RFC 015: Parallel Scan

**Status:** Implemented (PR #158)
**Date:** 2026-07-04 (RFC), 2026-07-07 (implementation)
**Author:** kv-engine Contributors
**References:**
- RFC 006: Prefix Search
- RFC 008: Block and Value Prefetching
- RFC 014: Async Operations End-to-End
- Apache DataFusion blog, "Using Cancellation in DataFusion", 2025-06-30

---

## 1. Summary

This RFC proposes adding a **parallel async scan** path to kv-engine for
large range reads and prefix reads:

1. partition one logical scan into multiple non-overlapping key subranges;
2. run each subrange on the existing synchronous iterator stack inside
   engine-owned blocking workers;
3. stream results back through a single ordered async cursor; and
4. make cancellation and scheduler cooperation explicit, rather than assuming
   that "async" alone makes a long scan interruptible.

The proposed MVP adds new APIs rather than changing the behavior of existing
`scan()` / `scan_async()`:

```rust
let mut scan = engine
    .scan_parallel_async(lower, upper, ParallelScanOptions::default())
    .await?;

while let Some((k, v)) = scan.try_next().await? {
    // consume ordered results
}
```

The design keeps the current storage layout, MVCC visibility rules, prefix
bloom pruning, range-tombstone filtering, and value-log resolution. The main
change is execution structure: one large scan becomes several bounded workers
plus an ordered async coordinator.

The MVP also requires one small but explicit internal refactor: the engine
needs a way to build shard iterators from one externally owned snapshot
timestamp (`read_ts`) while a coordinator-owned `ReadGuard` pins the MVCC
watermark for the full scan lifetime, rather than letting each shard allocate
its own scan guard independently.

---

## 2. Motivation

### 2.1 Current `scan_async()` Is Async Construction, Sync Consumption

RFC 014 added async scan entrypoints, but the current `AsyncScan` cursor still
advances synchronously:

1. `KvEngine::scan_async()` builds the cursor on the blocking executor.
2. `AsyncScan::try_next()` copies the current key/value and then calls
   `inner.next()?` directly.
3. The returned future has no meaningful internal yield point while the scan
   advances.

That is acceptable for small scans, but it is weak for large scans in async
applications:

1. one task can monopolize progress for a long time;
2. cancellation is only observed at coarse future boundaries;
3. a single slow scan cannot exploit multiple cores even when the read range
   spans many SSTs.

The DataFusion cancellation work is relevant here: cancellation is not a free
property of `async`; the operator has to cooperate by checking for cancellation
and yielding often enough for the runtime to observe it.

### 2.2 The Scan Path Already Has Natural Parallel Structure

The current scan stack already separates concerns cleanly:

1. `scan_inner()` builds a snapshot-local iterator graph.
2. L1+ SSTs are already organized as disjoint key-range runs.
3. Prefix scans are already reduced to ordinary range scans.
4. MVCC snapshot visibility is already expressed as one `read_ts`.

This makes parallelization possible without inventing a new on-disk format or
rewriting compaction, WAL, or memtable semantics.

### 2.3 The Engine Already Has the Right Runtime Boundary

RFC 014 introduced `BlockingExecutor` so the engine owns the sync-to-async
boundary. Parallel scan should reuse that boundary:

1. scan workers remain synchronous iterators internally;
2. callers do not need to wrap scans in their own `spawn_blocking`;
3. cancellation and backpressure stay in engine-owned code.

---

## 3. Goals

1. Add an ordered parallel async scan path for large range reads.
2. Add the same capability for prefix scans by reusing existing prefix-to-range
   mapping.
3. Preserve the exact visible result set and ordering of today's single-threaded
   `scan()` and `prefix_scan()`.
4. Preserve MVCC snapshot semantics, range-tombstone filtering, prefix bloom
   pruning, and value-log dereference behavior.
5. Make cancellation explicit and prompt enough for async service workloads.
6. Bound memory growth with batched channels and bounded worker fan-out.
7. Fall back cleanly to a single-shard scan when the range is too small or the
   LSM shape does not justify parallelism.

## 4. Non-Goals

1. Replacing existing synchronous `scan()` or `scan_async()` in the MVP.
2. Rewriting the iterator stack around `Stream` or kernel-async I/O.
3. Parallel transaction scans in the MVP.
4. Reverse scans or unordered scans.
5. New on-disk indexes or partition metadata in the MVP.
6. Claiming universal speedups for tiny scans or point-read-heavy workloads.

---

## 5. Existing Baseline

Today the scan path is built by `LsmStorageInner::scan_inner()`:

1. memtable and immutable memtables become a `MergeIterator`;
2. overlapping L0 SSTs become another `MergeIterator`;
3. L1+ SSTs become per-level `SstConcatIterator`s because their key ranges are
   already non-overlapping within each level;
4. those layers are merged into `LsmIterator`;
5. `ScanIterator` holds the MVCC `ReadGuard` for the scan lifetime.

The async API from RFC 014 wraps this with `AsyncScan`, but does not yet make
iteration itself cooperative or parallel.

That baseline is important:

1. correctness already lives in the existing iterator stack;
2. the RFC should parallelize execution around that stack, not replace it;
3. any design that duplicates visibility logic would be unnecessarily risky.

---

## 6. Proposed API

Add a dedicated parallel cursor and option struct:

```rust
pub struct ParallelScanOptions {
    pub max_parallelism: usize,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub yield_every_rows: usize,
    pub channel_capacity: usize,
}

pub struct ParallelScan {
    // opaque
}

impl KvEngine {
    pub fn scan_parallel_async(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        options: ParallelScanOptions,
    ) -> Result<ParallelScan>;

    pub fn prefix_scan_parallel_async(
        &self,
        prefix: &[u8],
        options: ParallelScanOptions,
    ) -> Result<ParallelScan>;
}

impl ParallelScan {
    pub async fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>>;
}
```

Recommended defaults:

1. `max_parallelism = num_cpus::get().min(8)`;
2. `batch_rows = 128`;
3. `batch_bytes = 256 * 1024`;
4. `yield_every_rows = 1024`;
5. `channel_capacity = 4`.

Required option handling:

1. `max_parallelism <= 1` must fall back to the single-shard path rather than
   erroring;
2. `batch_rows == 0` must be normalized or rejected before execution;
3. `batch_bytes == 0` must be normalized or rejected before execution;
4. `yield_every_rows == 0` must be normalized or rejected before execution; and
5. `channel_capacity == 0` must be normalized or rejected before execution.

The MVP may choose either strict validation or documented normalization for the
positive-only knobs, but it must not permit zero values to create empty batches
or non-progressing worker loops.

The constructor should remain synchronous in the MVP. Planning, guard
registration, and worker/coordinator setup are in-memory control-path work; the
asynchronous boundary belongs at `try_next().await`, where the caller actually
waits for ordered batches to arrive.

The exact API shape may later converge with a general `ScanOptions` type, but
the MVP should keep the surface explicit so behavior changes are opt-in.

### 6.1 Why a New API Instead of Mutating `scan_async()`

The current `scan_async()` is already used as a simple ordered cursor. Making it
silently parallel would introduce:

1. different scheduling behavior;
2. different cancellation timing;
3. new buffering and memory tradeoffs.

That should be an explicit opt-in first.

---

## 7. Design Overview

The engine executes a parallel scan in four phases:

1. acquire one scan admission guard and one snapshot read timestamp;
2. plan `N` non-overlapping user-key subranges inside the requested bounds;
3. spawn `N` blocking workers, one per subrange, each using the existing scan
   machinery with narrowed bounds; and
4. expose one ordered async cursor that drains worker batches in subrange order
   and does not release its guard until every worker has quiesced.

Because `Drop` cannot block unboundedly, the guards cannot live only in the
public cursor handle. The implementation must keep:

1. worker-shared state that contains only `Send + Sync` coordination data such
   as cancellation, bookkeeping, and `read_ts`; and
2. coordinator-owned state that keeps the admission guard and MVCC `ReadGuard`
   alive until worker/coordinator shutdown finishes.

Because the subranges are non-overlapping and sorted:

1. workers do not need a heap merge on every row; and
2. the coordinator can preserve global ordering by draining shard `0`, then
   shard `1`, and so on, while later shards run ahead in the background.

This gives useful overlap without requiring a row-by-row async merge operator.

---

## 8. Detailed Design

### 8.1 Planning Subranges

The planner derives split points from the current snapshot:

1. start from the requested user-key bounds;
2. inspect L1+ SST key-range boundaries, because those levels are already
   organized as sorted non-overlapping runs;
3. choose up to `max_parallelism` user-key subranges whose concatenation is
   exactly the requested range; and
4. coalesce empty or obviously tiny partitions into adjacent shards so the
   final shard coverage still exactly matches the requested range.

Example:

```text
requested scan: [b, z)
split points:   [b, g) [g, m) [m, t) [t, z)
```

Each shard then runs a normal bounded scan over its own subrange:

```text
shard 0 => scan([b, g))
shard 1 => scan([g, m))
shard 2 => scan([m, t))
shard 3 => scan([t, z))
```

This keeps correctness simple:

1. each key belongs to at most one shard;
2. existing lower/upper-bound enforcement remains the source of truth; and
3. memtable, L0, and L1+ data all participate naturally because every worker
   still calls the existing scan path.

### 8.2 Why Not Partition by SST Ownership Alone

Assigning SSTs directly to workers would be incorrect for the full engine view:

1. memtables and L0 overlap arbitrarily;
2. MVCC version collapse happens across sources;
3. range tombstones can cover keys from many sources.

Range partitioning is the right abstraction boundary because the current engine
already knows how to answer "what keys are visible in this user-key interval?"

### 8.3 Worker Execution Model

Each worker:

1. runs inside the existing `BlockingExecutor`;
2. creates one narrowed-bounds iterator from a new internal constructor that
   reuses the parent scan's shared `read_ts`;
3. advances synchronously inside that worker thread; and
4. sends result batches through a bounded channel back to the async
   coordinator.

The worker emits batches, not single rows, to reduce cross-thread overhead:

```rust
struct ScanBatch {
    rows: Vec<(Bytes, Bytes)>,
    is_last: bool,
}
```

The worker stops when:

1. its iterator is exhausted;
2. it observes cancellation;
3. channel closure indicates the parent cursor was dropped; or
4. it hits an error, which is forwarded to the coordinator.

For MVCC-enabled engines, workers must not call ordinary `scan()`, must not
allocate per-shard `ReadGuard`s, and do not need direct access to the
`ReadGuard` itself. The whole parallel scan remains one logical engine snapshot
because the coordinator keeps one `ReadGuard` alive while workers build
iterators from the shared `read_ts`.

### 8.4 Ordered Coordinator

`ParallelScan` owns one receiver per shard plus coordinator-owned guard state.

Concretely, the design should split state in two:

```rust
struct ParallelScan {
    shared: Arc<ParallelScanShared>,
    coordinator: ParallelScanCoordinator,
}
```

`ParallelScanShared` owns only worker-safe shared state:

1. the cancellation token;
2. worker task bookkeeping;
3. the shared `read_ts`; and
4. any other `Send + Sync` coordination state needed by workers.

`ParallelScanCoordinator` owns:

1. the admission guard;
2. the optional MVCC `ReadGuard`;
3. the ordered-drain receive state; and
4. any non-shared state needed to finish shutdown safely.

This coordinator must be a long-lived task or state machine that outlives the
public `ParallelScan` handle when the handle is dropped early. The public handle
therefore owns a way to signal that coordinator, not the only copy of the
coordinator itself.

Dropping the public cursor then:

1. marks the shared state cancelled;
2. closes the public receive path; and
3. leaves the long-lived coordinator task holding the admission guard and
   optional `ReadGuard` alive until the workers observe cancellation and exit.

The coordinator should detect worker quiescence explicitly by observing all
worker-side batch senders disconnect or otherwise report completion. That gives
the long-lived coordinator a concrete, leak-free condition for releasing the
admission guard and optional `ReadGuard`.

The coordinator:

1. tracks the current shard index;
2. pulls batches from that shard until it is exhausted;
3. then advances to the next shard; and
4. yields rows from the current batch one by one through `try_next().await`.

`ParallelScan` and the future returned by `try_next()` must remain
`Send`-compatible. Any state carried across `.await` points in the coordinator
path must therefore be owned `Send` data rather than borrowed or thread-confined
state.

This preserves the same global key order as a single scan because shard ranges
are disjoint and already sorted by key.

Later shards are allowed to precompute and queue batches early, but bounded
channels cap how far they can get ahead.

If a later shard fails early, the coordinator still preserves ordered delivery:

1. rows already buffered from earlier shards may still be returned;
2. the error is surfaced when the cursor reaches the failing shard in key-range
   order; and
3. no later-shard rows are returned after the error becomes visible.

This matches the semantics of one logical ordered scan more closely than
"fail immediately on any background shard error".

### 8.5 Cancellation and Cooperation

This is the most important behavioral requirement.

Parallel scan needs **cooperative cancellation**, inspired by the DataFusion
lesson that cancellation only works if long-running operators check for it.

Add an engine-owned cancellation token shared by:

1. all worker tasks;
2. the async coordinator; and
3. the cursor `Drop` path.

Workers must check the token:

1. after every emitted batch;
2. after every `yield_every_rows` rows;
3. after expensive iterator transitions such as block boundaries or SST
   switches, once the iterator stack exposes those hooks.

The coordinator must also cooperate:

1. `try_next().await` should call `tokio::task::yield_now().await` after
   returning enough rows from buffered batches;
2. when cancellation is requested, it stops draining and surfaces a dedicated
   cancellation error.

This is the core difference between "async-looking" and actually async-friendly
scan behavior.

The MVP should not claim stronger responsiveness than the current iterator
surface permits. Today the outer scan loop can reliably check cancellation only
between `next()` calls and batch sends. More prompt interruption at block
boundaries, SST handoff, or value-log read boundaries requires follow-up
iterator-surface changes and should be documented as such.

### 8.6 MVCC Snapshot Semantics

Parallel scan must preserve one logical snapshot.

For MVCC-enabled engines:

1. acquire one `ReadGuard` for the overall scan;
2. capture one `read_ts`;
3. create all shard iterators against that same `read_ts`; and
4. keep the single outer guard alive in the long-lived coordinator task until
   all workers have quiesced, even if the public `ParallelScan` handle is
   dropped early.

Workers must not allocate independent read timestamps, or the scan would no
longer be one consistent snapshot.

This implies an internal constructor, roughly:

```rust
fn scan_inner_with_snapshot(
    &self,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    mvcc_read_ts: Option<u64>,
    prefix_hint: Option<&[u8]>,
) -> Result<FusedIterator<LsmIterator>>;
```

paired with a parent-owned guard lifecycle. The exact name can differ, and the
implementation will likely reuse or slightly refactor the existing
`LsmStorageInner::scan_inner` path rather than introducing a wholly separate
iterator-construction stack. The capability is what the MVP requires.

The guard itself should stay coordinator-owned, not worker-shared. Workers only
need `read_ts`; the `ReadGuard` exists solely to pin the watermark for the
duration of the scan and must therefore outlive any still-running workers.

### 8.7 Prefix Scans

`prefix_scan_parallel_async()` is a thin wrapper:

1. convert the prefix to `[lower, upper)` using the RFC 006 helper;
2. pass the original prefix as a prefix hint so existing prefix bloom pruning
   still applies; and
3. plan subranges only inside that prefix range.

This keeps exact byte-prefix semantics unchanged.

### 8.8 Range Tombstones

Each shard continues using the existing `build_scan_range_tombstone_iterator()`
logic for its bounded range.

That means:

1. shard-local scans still honor the newest covering tombstone;
2. no new tombstone merge format is needed; and
3. cancellation checks happen outside the visibility rules, not inside them.

### 8.9 Value Separation and Prefetching

Parallel scan composes naturally with existing and planned read optimizations:

1. if value separation is enabled, each shard still resolves values through the
   current vLog path;
2. if RFC 008 prefetching lands, it benefits each worker independently; and
3. the scan RFC does not require prefetching to land first.

The RFCs are complementary:

1. RFC 008 reduces per-worker stalls;
2. RFC 015 increases inter-worker overlap.

### 8.10 Error Handling

On the first worker error:

1. the coordinator records that error;
2. cancellation is broadcast to the other workers; and
3. the error is returned once the cursor drains all already-buffered rows from
   earlier shards and reaches the failing shard in key-range order.

The coordinator must preserve and return the original triggering worker error.
Later shards may observe cancellation and stop early, but those secondary
shutdown/cancellation results must not overwrite the root-cause error recorded
first.

Workers should fail closed. Partial continuation after a shard error is not
useful because the caller requested one logical ordered scan.

### 8.11 Fallback Behavior

The planner should fall back to one shard when:

1. `max_parallelism <= 1`;
2. the requested range is tiny;
3. there are too few useful L1+ split points; or
4. the engine shape suggests parallelism would only add overhead, especially
   when memtable and overlapping L0 work dominate the scan.

This keeps the feature safe for small scans and low-fanout databases.

In particular, the planner should treat the following as strong single-shard
signals:

1. very large active/immutable memtables relative to L1+ coverage;
2. wide L0 overlap where every shard would repeat nearly the same L0 work; and
3. short prefix ranges that touch only a few SSTs after prefix-bloom pruning.

---

## 9. Transaction Scans

Parallel transaction scans are intentionally deferred.

The main complications are:

1. transaction scans merge engine state with local writes;
2. the local-write iterator is not naturally range-partitioned today;
3. serializable transactions already reject scans due to missing phantom/range
   tracking; and
4. the MVP should not broaden the surface area of those correctness questions.

If engine-level parallel scan proves worthwhile, transaction scans can follow
with a separate design that preserves local-write merge semantics explicitly.

---

## 10. Metrics and Tuning

Add lightweight counters so the feature can be validated instead of assumed:

1. number of planned shards;
2. number of fallback-to-single-shard scans;
3. rows and bytes produced per shard;
4. worker cancellation count;
5. coordinator yield count;
6. channel backpressure count or blocked-send count; and
7. ordered-error deferral count; and
8. scan wall time split into planning, execution, and drain time.

These should feed the benchmark work from RFC 011.

Recommended knobs:

1. `max_parallelism`;
2. `batch_rows`;
3. `batch_bytes`;
4. `yield_every_rows`;
5. per-scan bounded channel depth.

---

## 11. Testing Plan

### 11.1 Correctness

Add tests that compare:

1. `scan()` vs `scan_parallel_async()` over the same dataset;
2. `prefix_scan()` vs `prefix_scan_parallel_async()`;
3. MVCC snapshot scans with interleaved versions;
4. datasets with range tombstones;
5. datasets with value separation enabled;
6. empty-range and empty-prefix cases;
7. shard-boundary edge cases where a key falls exactly on the split point; and
8. compile-time `Send` compatibility checks for `ParallelScan`.

### 11.2 Cancellation

Add dedicated async tests that:

1. start a large parallel scan;
2. consume a few rows;
3. drop the cursor or cancel the future; and
4. verify workers stop promptly and the engine remains usable.

This should extend the cancellation-safety coverage introduced with RFC 014.

Add lifecycle tests that:

1. drop a `ParallelScan` mid-stream;
2. verify the scan does not release its admission/reader guard until workers
   have observed cancellation and exited; and
3. verify `close_async()` does not finish while scan workers are still active.

### 11.3 Performance Validation

Use the RFC 011 benchmark harness to measure:

1. full-range scan throughput on large leveled datasets;
2. bounded prefix scans over many SSTs;
3. cold-cache vs warm-cache behavior;
4. CPU utilization and tail latency impact under concurrent request load.

Success is not "parallel scan is always faster". Success is:

1. large scans speed up on multi-core workloads where the LSM shape allows it;
2. cancellation latency is materially better than the current cursor; and
3. small scans do not regress due to fallback behavior.

---

## 12. Alternatives Considered

### 12.1 Make `AsyncScan::try_next()` Call `spawn_blocking` Per Row

Rejected because:

1. it adds one cross-thread hop per row;
2. it does not exploit whole-range parallelism; and
3. it still lacks a good batching/backpressure story.

### 12.2 Rewrite the Iterator Stack as `Stream`

Rejected for the MVP because:

1. it is a much larger change than needed;
2. it would entangle correctness work with executor mechanics; and
3. RFC 014 explicitly chose staged migration over an immediate full rewrite.

### 12.3 Parallelize Only L1+ and Scan Memtable/L0 Separately

Possible, but not chosen for the MVP because:

1. it complicates ordered merge behavior;
2. it creates a special-case execution model for different LSM layers; and
3. range partitioning already lets the existing engine answer the full query
   correctly.

---

## 13. Rollout Plan

Recommended landing order:

1. add the internal shared-snapshot scan constructor needed to build shard
   iterators from one parent-owned `ReadGuard` and `read_ts`;
2. add `ParallelScanOptions`, `ParallelScan`, single-shard worker-backed
   plumbing, cooperative cancellation, worker quiescence on drop, and the
   shared-state guard ownership model as one coherent slice before the public
   API is exposed;
3. add multi-shard planning from L1+ boundaries plus explicit single-shard
   fallback for L0/memtable-heavy shapes;
4. add metrics and benchmark coverage;
5. tune defaults using RFC 011 workloads.

This order keeps every intermediate step testable and avoids coupling
partitioning logic to the first cursor plumbing patch.

---

## 14. Open Questions

1. Should cancellation surface as `Ok(None)` or as a dedicated error variant?
   This RFC chooses a dedicated error variant because `Ok(None)` must remain
   true end-of-stream. The remaining question is only the concrete error type
   and whether cancellation gets its own enum variant or reuses a broader
   engine error category.
2. Should the planner consider only L1+ boundaries, or also sampled memtable/L0
   boundaries when those are large enough to matter?
3. Should `scan_async()` eventually become an alias for the parallel cursor with
   `max_parallelism = 1` so all async scans share the same cooperative
   implementation?
4. Should worker batching be row-count based, byte-size based, or whichever
   limit is hit first? The last option is likely the most robust.

---

## 15. Conclusion

Parallel scan should be treated as an execution-structure improvement, not a
new storage feature.

The engine already knows how to answer a bounded scan correctly. The missing
piece is how to execute large scans in async applications without:

1. pinning all work to one cursor loop;
2. delaying cancellation indefinitely; or
3. leaving available CPU parallelism unused.

By partitioning scans into ordered subranges, reusing the existing iterator
stack inside engine-owned blocking workers, and making cancellation cooperative,
kv-engine can add a practical parallel scan path without destabilizing the rest
of the storage engine.

---

## 16. Implementation Notes (2026-07-07)

The implementation shipped in PR #158.  Key divergences from the original
proposal:

### API Changes

- `try_next()` (row-at-a-time) was **removed**.  The API is chunk-first:
  `try_next_chunk().await` and `try_next_batch().await`.
- `ParallelScanOptions::cache_admission` added — defaults to `Bypass`.

### Cache Admission

The RFC did not anticipate cross-shard cache pollution.  The implementation
adds `CacheAdmission` (Force/Admit/Bypass) threaded through the iterator
stack.  `Bypass` is the default — it eliminates cross-shard cache eviction
and is the winning policy in benchmarks.

### Coordinator Drain

The original design proposed strictly-ordered per-shard drain.  The
implementation uses **concurrent drain**: polls all remaining shard receivers
non-blocking before falling back to a blocking await.  Future-shard batches
are buffered for instant delivery.

### Readahead

`posix_fadvise(WILLNEED)` is called at each block boundary in
`SsTableIterator` — not in the RFC, added during implementation.

### What Was Deferred

- Transaction scans (§9) — still deferred.
- Reverse/unordered scans (non-goal §4).
- `scan_async()` as alias for `max_parallelism=1` (open question §14.3).

### Benchmark Results

See `docs/parallel-scan-findings.md` for full results.  Parallel + Bypass is
1.3-1.5× faster than sync scan at 50K-100K rows with balanced shards.
