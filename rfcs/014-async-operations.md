# RFC 014: Async Operations End-to-End

**Status:** Active (partially implemented)
**Date:** 2026-07-02
**Author:** kv-engine Contributors
**References:**
- RFC 003: Thread-per-Core with compio
- RFC 012: Parallel WAL
- `docs/perf-profile.md`

---

## 1. Summary

This RFC proposes changing kv-engine's **user-visible operation surface** from
fully synchronous methods to a **dual-surface API with engine-owned async
entrypoints**:

1. `KvEngine` exposes async entrypoints such as `open_async`, `close_async`,
   `get_async`, `batch_get_async`, `put_async`, `delete_async`,
   `delete_range_async`, `write_batch_async`, `sync_async`, and async scan
   cursors.
2. Transaction methods that may observe engine state or publish changes
   (`get`, `commit`, and eventually scan operations) gain async variants, while
   purely in-memory helpers may remain synchronous.
3. Range iteration moves from today's synchronous iterator stack to an
   async-aware cursor API.
4. Test-only control paths such as `force_flush` and `drain_flush` remain
   explicitly test-only even if async wrappers are later added.
5. Maintenance-heavy operations such as full compaction are treated as
   admin-only APIs rather than ordinary request-path methods.

This is **not** a proposal to immediately rewrite every internal subsystem
around a kernel-async runtime. The current repository data says that raw
throughput is usually limited by CPU, memtable/iterator work, compaction
pressure, and write-path structure, not by the absence of `async` in public
signatures. The value of this RFC is instead:

1. making kv-engine composable inside async applications;
2. giving the engine a clean path to overlap long-running work with caller
   tasks;
3. removing the need for embedding applications to wrap every engine call in
   `spawn_blocking`;
4. creating a staged path toward async background execution where it is useful.

The recommended implementation is therefore **staged**:

1. land engine-owned async entrypoints first;
2. preserve current storage semantics and correctness invariants;
3. migrate internals to async execution only where profiling or integration
   pressure justifies it.

---

## 2. Motivation

kv-engine is synchronous today:

1. foreground operations block the caller thread;
2. shutdown joins background threads synchronously;
3. scans are synchronous iterators;
4. transactions expose synchronous methods;
5. embedding kv-engine inside async servers requires a blocking bridge around
   every call.

That creates three practical problems.

### 2.1 Embedding Friction

Async applications want to compose storage calls with:

1. request timeouts;
2. cancellation;
3. bounded concurrency;
4. structured backpressure;
5. multiplexing with network, RPC, or task orchestration.

Today, an async caller must either:

1. block an executor thread directly, which is incorrect; or
2. wrap kv-engine operations in `spawn_blocking`, which is noisy, easy to get
   wrong, and pushes engine-specific scheduling decisions into every caller.

### 2.2 Long-Running Engine Operations Already Exist

Several operations are naturally long-lived from the caller's point of view:

1. `open()` can replay WALs, recover manifests, rebuild references, and load
   metadata.
2. `close()` can wait on flush, compaction, GC, WAL sync, and final freeze.
3. `force_flush()` and `force_full_compaction()` can perform large amounts of
   disk work.
4. scans and transaction scans may read many blocks over time.

Treating all of these as synchronous forces the caller to dedicate OS threads
to waiting.

### 2.3 Public Async Does Not Require Immediate Full Async I/O

The repository's own measurements matter here.

`docs/perf-profile.md` explicitly concludes that:

1. I/O is not the main bottleneck in the profiled workloads;
2. a full async rewrite is not justified on throughput alone;
3. several large wins came from better batching, cache behavior, and reduced
   CPU overhead rather than executor-driven async conversion.

That means this RFC should not pretend that "make everything async" is a free
performance win. The primary motivation is **composability and execution model
correctness**, with internal async overlap as a secondary, targeted benefit.

---

## 3. Goals

1. Provide engine-owned async entrypoints that integrate cleanly with async
   Rust applications without forcing callers to wrap every operation in
   `spawn_blocking`.
2. Preserve current durability, visibility, MVCC, and crash-recovery
   guarantees.
3. Keep the migration incremental enough that correctness can be maintained
   with the existing test and chaos suite.
4. Avoid forcing a full iterator, WAL, manifest, compaction, and read-path
   rewrite in one step.
5. Make cancellation and shutdown behavior explicit.
6. Leave room for future async execution of flush, compaction, manifest, and
   vLog paths where that is actually useful.

## 4. Non-Goals

1. Claiming that async public signatures alone will improve single-thread
   throughput.
2. Replacing the current WAL design from RFC 012.
3. Requiring every internal helper to become `async fn` in the first landing.
4. Rewriting lock-free memtable structures around async primitives.
5. Solving range-serializable phantom tracking. That remains a separate MVCC
   concern.
6. Guaranteeing cancellation safety for arbitrary mid-operation aborts. The
   engine must remain correct, but cancellation points must be explicit.

---

## 5. Current Baseline

The current top-level API in `kv-engine/src/lsm_storage.rs` is synchronous:

1. `KvEngine::open() -> Result<Arc<KvEngine>>`
2. `KvEngine::close() -> Result<()>`
3. `get`, `batch_get`, `put`, `delete`, `delete_range`, `write_batch`
4. `scan`, `prefix_scan`
5. `force_flush`, `drain_flush`, `force_full_compaction`
6. `new_txn`

Transaction APIs in `kv-engine/src/mvcc/txn.rs` are synchronous as well:

1. `get`
2. `scan`
3. `prefix_scan`
4. `put`
5. `delete`
6. `commit`

The iterator stack is also synchronous:

1. `StorageIterator::next(&mut self) -> Result<()>`
2. `ScanIterator` and `TxnIterator` are built on synchronous iteration
   contracts.

Background work currently uses dedicated threads plus channels:

1. flush thread;
2. compaction thread;
3. ad-hoc GC threads;
4. synchronous `close()` joining them all.

This is important because it shows that "change the public execution model"
is not just
signature churn. It crosses the public API, iterator model, transaction model,
and lifecycle model.

---

## 6. Design Constraints

Any acceptable design must respect the following constraints.

### 6.1 Public Scans Cannot Stay Exactly As-Is

Today a scan returns a synchronous iterator object. Once the API becomes async,
there are only three realistic choices:

1. expose `Stream`;
2. expose an async cursor with `try_next().await`;
3. materialize all scan results before returning.

Option 3 is unacceptable for large scans. This RFC chooses an async cursor.

### 6.2 `Drop` Cannot Be Async

The engine currently performs meaningful cleanup in `close()` and best-effort
thread joining in `Drop`. After async conversion:

1. graceful shutdown must live in `async fn close(&self)`;
2. `Drop` still needs a synchronous, bounded cleanup path for the current
   thread-join contract; it cannot simply degrade into fire-and-forget cancel.
3. the documentation must clearly state that durability-sensitive users must
   call `close().await`, but the migration must not silently weaken today's
   drop-time cleanup guarantees.

More concretely, the destructor path must continue to preserve the current
shutdown safety contract for engine-owned background workers:

1. `Drop` still performs synchronous, bounded cleanup for engine-owned
   maintenance workers such as flush/compaction ownership that would otherwise
   outlive the handle;
2. this includes bounded worker quiescing and thread/task joining where the
   current implementation already does so to avoid data loss when `close()` is
   skipped; and
3. `close().await` remains the stronger, graceful path for
   durability-sensitive callers because it performs the full shutdown sequence,
   not merely destructor-grade bounded cleanup.

Because `Drop` may run on an async runtime worker, any fallback cleanup must
stay strictly bounded and should avoid waiting on unbounded I/O or lengthy
coordination. Anything that could starve the executor belongs in
`close().await`, not in the destructor.

### 6.3 The Current Read Path Is Mostly CPU and `pread` Driven

The current read path is heavily intertwined with:

1. `StorageIterator`;
2. merge/concat iterators;
3. block cache lookups;
4. memtable bloom and skiplist fast paths;
5. point reads that are often CPU-bound rather than syscall-bound.

This RFC therefore does **not** require the first implementation to convert
every read-path helper to kernel-async I/O.

### 6.4 Async Must Not Weaken Crash Semantics

Every staged change must preserve:

1. WAL-before-visibility rules;
2. manifest publish ordering;
3. flush and compaction crash boundaries;
4. MVCC read/write visibility;
5. chaos-test reopen guarantees.

If an async refactor makes those less obvious, the refactor is incomplete.

---

## 7. Proposed API

### 7.1 `KvEngine`

The shipped Phase 1 surface is dual-surface rather than canonical-async:

```rust
impl KvEngine {
    pub async fn open_async(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>>;
    pub async fn close_async(&self) -> Result<()>;

    pub async fn get_async(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub async fn batch_get_async(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>>;
    pub async fn put_async(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn delete_async(&self, key: &[u8]) -> Result<()>;
    pub async fn delete_range_async(&self, start: &[u8], end: &[u8]) -> Result<()>;
    pub async fn write_batch_async<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()>;
    pub async fn sync_async(&self) -> Result<()>;

    pub async fn scan_async(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<AsyncScan>;
    pub async fn prefix_scan_async(&self, prefix: &[u8]) -> Result<AsyncScan>;

    pub fn new_txn_async(&self) -> Result<Arc<Transaction>>;
}
```

The synchronous API remains available for compatibility, CLI use, tests, and
benchmarks. This RFC now treats that dual-surface shape as the adopted Phase 1
design rather than a temporary naming accident.

Compaction-filter management should eventually gain async symmetry for
consistency, even if some implementations remain synchronous in early phases.

`force_flush` and `drain_flush` are intentionally omitted from the normal async
surface. In the current engine they are race-prone control hooks used only in
tests or quiescent maintenance contexts, not ordinary application APIs.

Likewise, full compaction should not be presented as a routine request-path API.
If it remains public, it should move behind an explicit maintenance/admin handle:

```rust
impl KvEngine {
    pub fn admin(&self) -> AdminHandle<'_>;
}

impl AdminHandle<'_> {
    pub async fn force_full_compaction(&self) -> Result<()>;
    pub async fn force_flush_for_test(&self) -> Result<()>;
    pub async fn drain_flush_for_test(&self) -> Result<()>;
}
```

The important contract is semantic, not naming: these operations are
maintenance/test controls, not ordinary application APIs.

### 7.2 `AsyncScan`

The public scan result becomes an async cursor rather than a synchronous
iterator trait object:

```rust
pub struct AsyncScan { /* opaque */ }

impl AsyncScan {
    pub async fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>>;
}
```

Why a cursor instead of `impl Stream<Item = Result<_>>`?

1. it keeps the public API simple;
2. it avoids pin/project ergonomics in common call sites;
3. it maps more directly onto the engine's existing iterator style;
4. it leaves room for an internal `Stream` implementation later if needed.

`AsyncScan` must retain the same MVCC snapshot pinning that `ScanIterator`
provides today:

1. acquire the non-transactional `ReadGuard` before building the scan snapshot;
2. retain that guard for the entire cursor lifetime, across all `try_next`
   await points;
3. release it only when the cursor is dropped or exhausted.

Anything weaker is a correctness regression because compaction/GC could reclaim
versions still needed by an active scan.

`AsyncScan` should stay `Send`-compatible in Phase 1.

The default implementation should audit the iterator stack and move any
required state into owned values before the first `await`, so the cursor does
not have to borrow non-`Send` state across suspension points. A `!Send` cursor
would be a deliberate compatibility regression, not the default design.

### 7.3 Transactions

Transactions are currently selectively async:

```rust
impl Transaction {
    pub fn get_async(&self, key: &[u8]) -> impl Future<Output = Result<Option<Bytes>>> + Send;
    pub fn scan_async(
        self: &Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> impl Future<Output = Result<AsyncTxnScan>> + Send;
    pub fn prefix_scan_async(
        self: &Arc<Self>,
        prefix: &[u8],
    ) -> impl Future<Output = Result<AsyncTxnScan>> + Send;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;
    pub fn commit_async(&self) -> impl Future<Output = Result<()>> + Send;
}
```

This split is intentional:

1. `new_txn`, `Transaction::put`, and `Transaction::delete` are purely
   in-memory operations in the current architecture and should remain
   synchronous unless their semantics change materially.
2. `get` and `commit` gain async variants because they may consult engine
   state, cross blocking boundaries, or publish changes.
3. transaction scan operations also expose async owned-cursor variants, while
   the existing synchronous scan APIs remain available for compatibility.

Transaction confinement remains part of the contract:

1. `Transaction` stays single-thread confined just as it is today;
2. async transaction methods should preserve `Send` futures where possible by
   snapshotting the owned state they need before the first `await`, rather than
   borrowing the transaction across suspension points;
3. synchronous local-write helpers (`put`, `delete`) should stay allocation and
   coordination-light, with no executor hop in the common case;
4. documentation and type design must make the confinement model explicit so callers do not
   assume a transaction future can freely move across executor threads.
5. `AsyncTxnScan` is an owned cursor and should follow the same
   `Send`-compatible default in Phase 1, without borrowing the transaction
   across `await` points.

Serializable-mode limitations also remain unchanged:

1. `scan()` and `prefix_scan()` continue to return an error in serializable
   mode until phantom/range tracking is implemented;
2. the async migration must not widen that capability by accident.

---

## 8. Execution Model

### 8.1 Chosen Direction: Async Public Surface, Staged Internal Migration

This RFC chooses the following architectural direction:

1. **Public API becomes async immediately.**
2. **Internals migrate in phases.**
3. **CPU-only and trivial paths may stay internally synchronous at first.**
4. **Blocking storage work is hidden behind engine-owned async execution rather
   than pushed to every caller.**

This is intentionally more pragmatic than a full "everything becomes async in
one PR" rewrite.

### 8.2 Runtime Strategy

This RFC separates **task orchestration** from **storage I/O backend choice**.

Default implementation guidance:

1. Tokio may be used for task orchestration, shutdown, cancellation, and async
   API integration.
2. Storage I/O must remain behind engine-owned traits so that WAL, SST, and
   vLog paths can choose their own backend independently.
3. That backend may be `std::fs`, Tokio blocking-backed filesystem access, or a
   specialized `io_uring` path, depending on the subsystem.

This split is required because the repository already has subsystem-specific
direction:

1. RFC 012 gives WAL a specialized manual `io_uring + O_DIRECT` path.
2. RFC 003 explores a broader completion-oriented runtime direction for async
   I/O.
3. The async public API should not force all storage paths onto one executor or
   one filesystem abstraction prematurely.

The practical rule is:

1. Tokio may own task lifecycle.
2. Tokio does **not** automatically own every storage syscall.
3. WAL submission/completion, SST reads/writes, and vLog I/O must stay
   replaceable behind engine-owned interfaces.

This keeps the default implementation pragmatic without freezing the storage
backend design too early.

### 8.3 Background Work

Flush, compaction, and GC should move from "threads + crossbeam notifications"
to "tasks + async notifications" over time.

Phase 1 does not need to rewrite all of them. A valid staged end state is:

1. foreground async methods run on an async runtime;
2. long-running maintenance work is owned by engine tasks;
3. shutdown awaits those tasks explicitly in `close().await`.

### 8.4 Phase 1 Blocking Policy

Phase 1 must not land as a pure signature-only conversion.

In particular, this is **not acceptable**:

```rust
pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
    self.get_sync(key)
}
```

when `get_sync` may block on disk or maintenance coordination.

Phase 1 therefore requires an explicit blocking policy:

1. CPU-only fast paths may execute inline.
2. Potentially blocking disk paths must run via an engine-owned blocking
   executor or equivalent internal boundary.
3. The engine must bound blocking concurrency rather than spawning unbounded
   background work.
4. The engine must document which methods may still perform internal blocking
   work in Phase 1.

At minimum, the implementation must treat these as potentially blocking:

1. `open`;
2. `close`;
3. `scan` / `prefix_scan`;
4. maintenance/admin APIs such as full compaction;
5. write paths whose sync policy can wait on durable WAL completion.

---

## 9. Detailed Design

### 9.1 Phase 1: Async Surface Over Current Core

Phase 1 is the minimum acceptable landing:

1. convert public engine methods to `async`;
2. convert transaction methods to `async`;
3. add async scan cursor types;
4. keep current correctness behavior identical;
5. centralize any required blocking bridge inside the engine rather than in
   application code.

This phase does **not** require:

1. rewriting memtable internals;
2. replacing the WAL implementation;
3. replacing flush/compaction code with fully async code;
4. rewriting every iterator implementation as an async trait immediately.

The goal is to change **who owns the blocking boundary**. After this RFC, the
engine owns it. The caller does not.

This RFC must not land as a pure signature-only conversion. Phase 1 must
include:

1. a clearly bounded engine-owned blocking executor or equivalent mechanism;
2. explicit cancellation tests for write and shutdown paths;
3. close-state tests covering concurrent foreground operations.

### 9.2 Phase 2: Async Cursor Internals

The current iterator stack is built around synchronous `next()`. That API is
deeply embedded in:

1. `SsTableIterator`;
2. `SstConcatIterator`;
3. `MergeIterator`;
4. `TwoMergeIterator`;
5. `LsmIterator`;
6. `TxnIterator`.

The migration plan is:

1. keep internal iterator traits synchronous in Phase 1 if needed;
2. wrap the public scan cursor around an engine-owned task boundary;
3. only introduce an async internal iterator trait after public behavior is
   stable and benchmarked.

This keeps the first landing tractable. It also isolates the most invasive part
of the rewrite instead of coupling it to every other async change.

If Phase 2 introduces engine-owned scan tasks, the implementation must also
define:

1. a bounded resource policy for concurrent scan tasks;
2. cleanup behavior when a caller drops the cursor or abandons `try_next()`
   futures;
3. whether scan work is per-cursor, per-batch, or pooled; and
4. how hidden scan-task cancellation interacts with `ReadGuard` lifetime.

The current passive iterator model does not allocate hidden background tasks per
scan. Any move away from that model must preserve boundedness and cleanup
explicitly.

### 9.3 Phase 3: Async Lifecycle and Shutdown

`close()` becomes the only graceful-shutdown API:

1. cancel background maintenance intake;
2. await flush/compaction/GC completion;
3. preserve today's split close contract:
   WAL-enabled close performs final WAL + directory sync and relies on normal
   recovery semantics, while non-WAL close freezes and drains remaining
   memtables to SSTs;
4. return only when the engine reaches the same durable state that today's
   synchronous `close()` guarantees.

`Drop` becomes best-effort:

1. log that the engine was dropped without `close().await`;
2. preserve today's bounded synchronous cleanup contract where feasible, such
   as joining or otherwise quiescing owned maintenance workers;
3. avoid pretending it performed the full graceful close sequence when it did
   not.

The lifecycle also needs an explicit state machine:

```text
Open -> Closing -> Closed
```

Required semantics:

1. `close().await` is idempotent.
2. Once the engine enters `Closing`, new foreground operations must fail with a
   stable shutdown/closing error.
3. `new_txn`, `write_batch`, `put`, `delete`, and new scans must not start once
   closing begins.
4. Foreground operations that were already admitted before the `Closing`
   transition must be accounted for explicitly: `close().await` must either
   wait for them to reach a documented completion point or reject them before
   they cross a publish/durability boundary. It must not race its final
   WAL-sync or non-WAL freeze/drain step against already-admitted writes.
5. Active scans and transactions must be tracked explicitly if `close().await`
   is expected to wait for or reject them authoritatively, because these
   handles have independent lifetime today.
6. The implementation must define whether active scans/transactions are waited
   for, cancelled, or rejected at close, and test that contract explicitly.
7. `Drop` with outstanding handles must remain bounded and must not deadlock on
   other `Arc<KvEngine>` owners.

The minimum acceptable design is a registered-handle model:

1. admitted foreground writes participate in a tracked in-flight operation set;
2. scans/transactions that may outlive the initiating call are registered as
   live handles; and
3. `close().await` transitions to `Closing`, rejects new admissions, then waits
   for the tracked set to reach the documented shutdown point before executing
   the final durability step.

The tracking registry must remain low contention on the hot path. A single
global mutex-protected set is not acceptable; use sharded state, atomic
counters, or an equivalent low-contention structure.

### 9.4 Phase 4: Selective Internal Async Conversion

Once the async public API exists, the following internal paths become eligible
for targeted async execution when measurements justify it:

1. `open()` recovery work;
2. flush task scheduling;
3. compaction output writing and manifest publication;
4. vLog GC rewrite orchestration;
5. range scan block fetch overlap.

This phase should be driven by measurement and integration need, not ideology.

---

## 10. Durability and Correctness Rules

Async conversion must preserve the following invariants.

### 10.1 Write Visibility

The existing two-phase rules remain:

1. a write must not become visible before the WAL durability boundary that
   today's code enforces;
2. memtable publication ordering must remain unchanged from the caller's point
   of view;
3. async task switching must not create a visibility-before-durability window.

### 10.2 Flush and Manifest Ordering

The async design must continue to enforce:

1. SST output sync before manifest durable publish;
2. manifest durable publish before WAL reclamation for flushed memtables;
3. no orphaned state that violates current chaos-test assumptions.

### 10.3 Cancellation Semantics

Cancellation must be explicit.

1. Before the durability boundary, an operation may fail and leave no visible
   change.
2. After the durability boundary, the operation must either complete normally
   or surface an outcome that is safe to retry/reopen.
3. Mid-flight future drop must not expose partially published state.

This is especially important for:

1. `write_batch`;
2. `delete_range`;
3. `commit`;
4. `force_flush`;
5. `force_full_compaction`;
6. `close`.

The first implementation should treat cancellation with the following matrix:

| Operation | Cancellation point | Allowed? | Required recovery semantics |
|-----------|--------------------|----------|-----------------------------|
| `put` / `write_batch` | Before WAL append | Yes | No visible change |
| `put` / `write_batch` | After WAL append, before durable sync | Bounded by sync policy | Reopen follows WAL semantics; no half-published memtable state |
| `put` / `write_batch` | After durable sync, before memtable publish | No externally visible half-state | Must finish publish or recover via reopen without duplicate/partial visibility |
| `commit` | After conflict check, before publish | High risk | Must remain atomic with respect to publish |
| `delete_range` | After tombstone WAL record, before publish | High risk | Same atomicity contract as write batch |
| full compaction | Before manifest publish | Yes | Reopen may choose old state; manifest must remain clean |
| full compaction | After manifest publish | Not partial | Reopen must observe either old or new published state, never mixed metadata |
| `close` | After shutdown begins | Not partial | Must end in a documented Open/Closing/Closed outcome |

The key rule is that dropping a future must not trick the caller into believing
an operation was cancelled if an engine-owned background task can still commit
it. If work may outlive the future, that outcome must be documented and tested.

For completion-based I/O specifically, cancellation/drop paths must also ensure
that buffers still owned by active kernel submissions are not freed
prematurely. If the kernel may still reference an in-flight WAL/SST/vLog
buffer, the implementation must defer or suppress that buffer's destruction
until submission/completion ownership is unquestionably released.

For shutdown specifically:

1. once `close().await` begins, already-admitted foreground writes must either
   be drained to their documented completion point or fail before publish;
2. active transactions/scans must follow the chosen registered-handle policy;
   and
3. `close().await` must not report success before the tracked in-flight set is
   reconciled with the final durability action.

### 10.4 Transactions

Async transaction methods must preserve current semantics:

1. snapshot reads stay tied to `read_ts`;
2. the read watermark stays pinned for the transaction lifetime;
3. commit remains single-use;
4. serializable-mode conflict detection remains atomic with publish.

The async conversion must not accidentally permit multi-threaded use of the
same `Transaction` object in ways the current design intentionally avoids.

For non-transaction scans, the equivalent rule is also mandatory: the scan
cursor must hold the non-transaction `ReadGuard` for its full lifetime.

---

## 11. Alternatives Considered

### 11.1 Keep the Public API Synchronous

This keeps the engine simple, but it permanently pushes async integration cost
to every caller. That is increasingly the wrong ownership boundary for a
library that may be embedded into async services.

### 11.1b Add Only a Blocking Facade

A blocking facade is useful for migration, but not sufficient as the main
design:

```rust
pub struct BlockingKvEngine {
    inner: Arc<KvEngine>,
    runtime: EngineRuntime,
}
```

This can remain as an optional compatibility layer or feature-gated facade for
CLI/tests/benchmarks, but it should not replace the async-first surface.

### 11.2 RFC 003 Option A: Internal Runtime + Synchronous Public API

RFC 003 discussed embedding a runtime internally while keeping a synchronous
API via `block_on`.

That avoids a breaking public change, but it has two major downsides here:

1. it preserves the external illusion that engine operations are cheap to block
   on, which is not true for `open`, `close`, scans, flush, and compaction;
2. it makes async callers pay the worst of both worlds, because they still have
   to route through blocking wrappers around a library that already has async
   internals.

For this RFC's goal, that is the wrong surface.

### 11.3 Pick Tokio Up Front

This RFC rejects locking in Tokio at the design level before the runtime split
with RFC 003 and RFC 012 is resolved. A concrete implementation may still use
Tokio, but only after the WAL/runtime ownership model is spelled out.

### 11.4 Full compio Rewrite Before Any Public Change

This is too much coupling.

The repository's own measurements do not justify blocking async API adoption on
an immediate full-storage-runtime rewrite. A compio migration can still happen
later if it proves valuable.

### 11.5 Expose `Stream` Directly for Scans

This is viable, but a bespoke async cursor keeps the public API simpler and
closer to the engine's existing iterator style.

---

## 12. Drawbacks

1. This is a breaking public API change.
2. The scan/iterator conversion is invasive even with a staged approach.
3. The engine must define and test cancellation behavior much more carefully
   than it does today.
4. Async can hide blocking if implemented lazily; Phase 1 must be honest about
   which paths still use engine-owned blocking work.
5. The runtime/executor decision remains open and must be resolved in the
   implementation plan rather than assumed away.

---

## 13. Compatibility and Migration

The current implementation does not treat this migration as an immediate major
API break. Instead it ships async entrypoints alongside the synchronous API.

Caller migration is therefore incremental:

1. async callers can move operation-by-operation onto `*_async` methods;
2. non-transactional scan loops switch to
   `while let Some((k, v)) = scan.try_next().await?` once they adopt
   `scan_async`;
3. transaction call sites can adopt `get_async` and `commit_async` first;
4. explicit graceful shutdown becomes `engine.close_async().await?`.

The CLI, tests, and benchmark harnesses may continue using the synchronous
surface or introduce explicit runtime boundaries where async coverage is needed.

This dual-surface design reduces migration pressure at the cost of carrying
both APIs for a period of time.

---

## 14. Implementation Plan

### Phase 0: Proof and Guardrails

1. add benchmark coverage for async wrapper overhead on point reads/writes;
2. define cancellation expectations per operation;
3. define the runtime/WAL ownership model before choosing an executor;
4. add doc-level rationale that async is for composability first, not claimed
   throughput wins.
5. define the Open/Closing/Closed lifecycle state machine and error surface.

### Phase 1: Public Async API

1. add `KvEngine::*_async` entrypoints. Completed.
2. add async transaction `get`/`commit` entrypoints while keeping local-write
   helpers synchronous. Completed.
3. add `AsyncScan` and `AsyncTxnScan`. Completed.
4. keep existing correctness behavior and reuse as much synchronous core logic
   as possible;
5. preserve serializable-mode scan rejections;
6. preserve scan `ReadGuard` lifetime rules.
7. define `Send` / `!Send` contracts for cursor and transaction futures.
8. land the engine-owned blocking executor/concurrency bound together with the
   async API. Completed.
9. define how admitted foreground writes are tracked across `Closing`.

Phase 1 is effectively complete on the current main branch: engine async
entrypoints, async engine scans, async transaction get/commit, async
transaction scans, and the bounded blocking executor are all in place while
preserving the sync surface.

### Phase 2: Async Shutdown and Background Ownership

1. replace notifier/join lifecycle with async-visible state management and
   admission tracking. Completed.
2. make `close_async().await` the canonical graceful shutdown path. Completed.
3. preserve the current split close contract for WAL and non-WAL engines.
   Completed.
4. preserve a bounded `Drop` cleanup story rather than pure fire-and-forget
   cancellation. Completed.
5. enforce and test the Open/Closing/Closed state machine. Completed.
6. add explicit registration/tracking for live scans, transactions, and
   admitted foreground writes if shutdown semantics depend on them. Completed.

Phase 2 now includes runtime-owned background ownership for periodic
flush/compaction work and its shutdown path, while preserving bounded drop
cleanup and explicit graceful close semantics.

### Phase 3: Targeted Internal Async Execution

1. move `open()` recovery orchestration behind async tasks. Completed.
2. move flush/compaction orchestration to async tasks. Completed.
3. measure whether selective overlap improves tail latency or throughput.

Phase 3 is now substantially landed: `open_async()` uses staged blocking-task
recovery plus parallel SST open, while periodic flush/compaction and
post-compaction vLog GC run under the engine-owned runtime instead of ad hoc
background threads. The remaining work is measurement and any follow-on
internal async overlap that benchmarks justify. See
`docs/async-phase3-measurement.md` for the measurement plan.

### Phase 4: Internal Iterator Rework

1. evaluate whether the internal iterator trait itself should become async;
2. only do this after public async scans are stable and benchmarked.

---

## 15. Testing Plan

The async migration is only acceptable if it preserves the repository's current
correctness bar.

Required coverage:

1. existing unit and integration tests continue to pass after async conversion;
2. transaction tests preserve snapshot and serializable semantics;
3. chaos tests still validate reopen and crash invariants;
4. new tests verify cancellation boundaries and double-close/drop behavior;
5. scan tests cover async cursor behavior across memtable, SST, and mixed
   sources, including `ReadGuard` retention across await points;
6. shutdown tests verify `close().await` preserves today's WAL and non-WAL
   close contracts;
7. tests confirm serializable transaction scans still reject until phantom
   tracking exists;
8. tests verify blocking-concurrency bounds and confirm Phase 1 does not block
   async executor workers on known disk-bound paths;
9. tests verify the documented cancellation matrix for write, range-delete,
   compaction, and close paths;
10. tests verify `close().await` drains or rejects already-admitted foreground
    writes according to the documented rule; and
11. tests verify registered active scans/transactions follow the documented
    shutdown policy without leaking hidden scan tasks; and
12. compile-time checks confirm the documented `Send` / `!Send` contracts for
    scan cursors and transaction futures in Phase 1.

---

## 16. Success Criteria

The async conversion is successful if:

1. async callers can use kv-engine without external `spawn_blocking` wrappers
   around ordinary operations;
2. all current correctness and chaos invariants still hold;
3. the engine documents explicit cancellation and shutdown behavior;
4. point-operation overhead remains acceptable;
5. the design leaves room for future internal async overlap without forcing a
   full rewrite up front;
6. the first landing is not merely a signature-only async veneer over
   executor-blocking synchronous code.

---

## 17. Recommendation

Adopt this RFC **only as a staged async-surface migration**, not as a blanket
"rewrite the whole engine around async right now" mandate.

That recommendation follows directly from the current codebase and documents:

1. async integration pressure is real;
2. a fully synchronous public API is awkward for modern embedding scenarios;
3. the repository's own perf notes do **not** support claiming that a full
   async rewrite is automatically a throughput win;
4. the safe path is to separate the public async contract from the much larger
   question of how much internal storage execution should become async.
