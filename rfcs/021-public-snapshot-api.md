# RFC 021: Public Snapshot API

**Status:** Proposed
**Date:** 2026-08-28
**Author:** kv-engine Contributors
**References:**
- RFC 005: MVCC
- RFC 014: Async Operations
- RFC 015: Parallel Scan
- RFC 017: MVCC Garbage Collection
- RFC 019: Checkpoint and Backup API
- RocksDB Basic Operations: https://github.com/facebook/rocksdb/wiki/Basic-Operations
- RocksDB Snapshot: https://github.com/facebook/rocksdb/wiki/Snapshot
- RocksDB Snapshot API: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/snapshot.h

---

## 1. Summary

This RFC adds a public, read-only point-in-time `Snapshot` API to kv-engine.
A snapshot captures the current MVCC commit timestamp, pins it in the existing
watermark, and serves reads from that timestamp while concurrent writes,
flushes, and compactions continue.

The proposed API is:

```rust
pub struct Snapshot {
    // private
}

impl KvEngine {
    pub fn snapshot(&self) -> Result<Snapshot>;
}

impl Snapshot {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>>;
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<SnapshotScanIterator>;
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<SnapshotScanIterator>;

    pub fn get_async(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'static;
    pub fn scan_async(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> impl Future<Output = Result<AsyncSnapshotScan>> + Send + 'static;
    pub fn prefix_scan_async(
        &self,
        prefix: &[u8],
    ) -> impl Future<Output = Result<AsyncSnapshotScan>> + Send + 'static;
}
```

This matches RocksDB's core snapshot behavior: reads use a consistent,
read-only view until the snapshot is released, while compaction preserves
versions that remain visible to that view. Snapshots are process-local runtime
objects. They are not persisted and do not survive completed engine close or
database reopen.

---

## 2. Motivation

kv-engine has MVCC internally and transactions already read from a fixed
timestamp. The only public way to obtain such a view today is a transaction,
which also carries local-write state, commit behavior, and serializable OCC
rules that read-only callers do not need.

Applications need read-only consistency for tasks such as:

1. reading related keys without observing a mixture of old and new values;
2. scanning a range while writers continue to update the same keyspace;
3. exporting or checking application state before creating a physical
   checkpoint;
4. serving one logical read request through multiple point reads and scans.

RocksDB exposes this through `GetSnapshot`, `ReadOptions::snapshot`, and
`ReleaseSnapshot`. A first-class kv-engine API closes that compatibility gap
without adding a new on-disk format or duplicating transaction machinery.

---

## 3. Goals

1. Add a public read-only `Snapshot` handle created by `KvEngine::snapshot()`.
2. Preserve one stable MVCC timestamp across snapshot point reads, batch reads,
   range scans, prefix scans, and async scan cursors.
3. Pin the timestamp in the MVCC watermark until the last snapshot handle and
   every iterator created from it are dropped.
4. Keep the snapshot API free of transaction local-write state and commit paths.
5. Reject new snapshot creation after the engine begins closing while allowing
   existing snapshots to finish safely.
6. Reuse current timestamped read and iterator construction paths rather than
   creating a second visibility implementation.
7. Add deterministic tests for point reads, scans, clone/drop lifetime,
   compaction retention, async reads, and shutdown interaction.

---

## 4. Non-Goals

1. **Persistent snapshots.** A snapshot is invalid after process exit or
   database reopen. Use RFC 019 checkpoints for reopenable physical copies.
2. **Write operations.** `Snapshot` has no `put`, `delete`, `write_batch`, or
   `commit` methods. Use `Transaction` for writes.
3. **Serializable validation.** Snapshots do not build read sets or detect
   conflicts. They provide snapshot isolation for reads only.
4. **Time-travel snapshots.** Callers cannot choose an arbitrary historical
   timestamp. The handle captures the latest committed timestamp at creation.
5. **Cross-column-family views.** kv-engine remains a single-keyspace engine.
6. **Snapshot persistence in the manifest or WAL.** Runtime pins must never
   affect recovery format or checkpoint contents.
7. **Parallel snapshot scans in the MVP.** Existing parallel scan machinery may
   be added after the single-cursor contract is stable.

---

## 5. Existing Building Blocks

### 5.1 MVCC Read Guard

`LsmMvccInner::new_read_guard()` already atomically captures the current commit
timestamp and registers it in the watermark. Its `ReadGuard` unregisters on
drop. Compaction GC uses the watermark to retain versions visible to active
readers.

The snapshot implementation reuses this mechanism. It must not allocate a
timestamp itself or reconstruct visibility through a separate counter.

### 5.2 Timestamped Reads

`LsmStorageInner::get_with_ts()` already resolves one key at a supplied read
timestamp. `scan_with_ts()` and `scan_with_prefix_hint()` already construct
iterators at a supplied timestamp for transaction reads. They handle current
MVCC ordering, range tombstone visibility, and TTL filtering.

The snapshot API calls these paths with the timestamp captured by its guard.

### 5.3 Lifecycle Admission

The engine has lifecycle admission guards for writes, scans, and transactions.
A snapshot must retain an admission guard for its shared lifetime.

For the MVP, a snapshot reuses the existing `Scan` admission kind. This is safe:
`begin_close()` flips the lifecycle state out of `OPEN`, after which `admit()`
(via `ensure_open()`) rejects admission of *every* kind, so a new `snapshot()` is
rejected once close begins. Separately, `is_quiescent()` waits on `active_scans`,
so a live snapshot blocks `close_async()` from finishing until it drops today;
once this RFC routes sync `close()` through the same path (below), it blocks
`close()` too. A dedicated `AdmissionKind::Snapshot` plus counter may be
introduced in Phase 3 alongside distinct snapshot metrics; that change must add
the variant and update `increment`, `decrement`, and `is_quiescent` together, or
a live snapshot would no longer hold close.

This RFC also requires the synchronous `close()` path to join the lifecycle
transition already used by `close_async()`: both close methods reject new
admission, wait for snapshot handles, owned async futures, and derived
iterators to quiesce, then finish closed. A new `snapshot()` call is rejected
after either close method begins. This is a deliberate behavior change for `close()` itself — today it only
joins background workers and syncs/flushes, and neither rejects new
writes/scans/transactions nor waits for in-flight ones to drain — so it affects
every admitted operation, not only snapshots. After this RFC, `close()` gains the
same release-before-close contract that `close_async()` already has.

---

## 6. Public API

### 6.1 Snapshot Handle

```rust
#[derive(Clone)]
pub struct Snapshot {
    inner: Arc<SnapshotInner>,
}

struct SnapshotInner {
    storage: Arc<LsmStorageInner>,
    read_ts: u64,
    _read_guard: ReadGuard,
    _lifecycle_guard: AdmissionGuard,
}
```

`Snapshot` is a small, cloneable shared handle. Cloning does not create a new
timestamp or watermark registration. The captured timestamp remains pinned until
the last `Snapshot`, `SnapshotScanIterator`, or `AsyncSnapshotScan` sharing its
`SnapshotInner` is dropped.

An async operation that has already been dispatched to the blocking executor
also retains `SnapshotInner` until its blocking closure completes. Dropping its
future cancels caller observation, not an in-flight blocking read or scan. This
can delay close and watermark release by the bounded duration of that operation.

The implementation should expose `Snapshot: Send + Sync` when the contained
storage and guards satisfy those bounds. The public type does not expose the
internal timestamp or guards.

### 6.2 Creation

```rust
impl KvEngine {
    /// Capture the latest committed state as a read-only snapshot.
    pub fn snapshot(&self) -> Result<Snapshot>;
}
```

Creation order is required:

0. If `mvcc` is `None`, return an unsupported error before acquiring admission or
   touching the watermark. `new_read_guard()` lives behind `mvcc: Option<_>`, so
   this check must precede it; performing it first also avoids acquiring a
   lifecycle guard on the error path.
1. Acquire snapshot lifecycle admission.
2. Capture and register `ReadGuard` through `new_read_guard()`.
3. Store its timestamp and both guards inside one shared `SnapshotInner`.

The guard must be captured before any later state snapshot used by an operation.
This preserves the existing invariant: every write visible to a snapshot's
timestamp is either present in the operation's state view or excluded by the
timestamp filter.

The `mvcc` field is `Option<_>` for forward compatibility; every `open()` path
sets it to `Some` today, so the step 0 error is not currently reachable but keeps
the API correct if a non-MVCC configuration is added later. The creation ordering
above is a new composition, not a direct reuse of an existing path: unlike
`new_txn`, which assumes MVCC and panics if it is absent, `snapshot()` returns an
error; and unlike `new_txn_async`, it checks `mvcc` before acquiring admission,
so the unsupported-error path never holds a guard. The timestamped read paths
(`get_with_ts`, `scan_with_ts`, `scan_with_prefix_hint`) are reused as-is.

### 6.3 Reads

```rust
impl Snapshot {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>>;
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<SnapshotScanIterator>;
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<SnapshotScanIterator>;
}
```

`get()` uses `get_with_ts(key, read_ts)`. `batch_get()` may initially call the
same timestamped point-read path per key; a batched timestamped lookup can be
added later without changing semantics.

`scan()` and `prefix_scan()` construct their internal iterators with the fixed
`read_ts`. The resulting cursor retains `Arc<SnapshotInner>` so dropping the
original `Snapshot` before the cursor is exhausted does not release the
watermark pin early.

All read methods preserve current semantics for tombstones, range tombstones,
TTL expiration, vLog dereference, prefix bloom pruning, and iterator errors.

### 6.4 Async Reads

```rust
impl Snapshot {
    pub fn get_async(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'static;
    pub fn scan_async(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> impl Future<Output = Result<AsyncSnapshotScan>> + Send + 'static;
    pub fn prefix_scan_async(
        &self,
        prefix: &[u8],
    ) -> impl Future<Output = Result<AsyncSnapshotScan>> + Send + 'static;
}
```

Async methods follow the existing transaction async pattern. Each method clones
`Arc<SnapshotInner>` and copies borrowed keys or bounds before returning its
future, so the future is `Send + 'static` and can outlive the originating
`Snapshot` handle. The future/cursor invokes the same synchronous timestamped
paths through the engine-owned blocking executor and must not hold a mutex or
state lock across `.await`. Cancellation before dispatch drops the cloned inner
normally; cancellation after dispatch does not stop the blocking closure, which
releases its inner only after completing.

### 6.5 Iterator Types

`SnapshotScanIterator` and `AsyncSnapshotScan` mirror the current scan cursor
interfaces but own `Arc<SnapshotInner>` instead of allocating a new read guard.
They return the same key/value types and propagate iterator errors unchanged.

Parallel snapshot scans are deferred. They need one coordinator-owned snapshot
inner shared by every worker and an ordered result cursor, but do not require a
new timestamp or separate visibility rules.

---

## 7. Semantics

### 7.1 Consistent View

At creation, a snapshot captures timestamp `T`. Every read through that snapshot
observes exactly the state visible at `T`:

```text
snapshot = db.snapshot()     // captures T
db.put(k, new_value)         // commits at T + 1 or later
snapshot.get(k)              // never observes new_value
```

Writes committed at timestamps greater than `T` are invisible. Versions at or
below `T` remain visible according to existing MVCC, tombstone, TTL, and
range-tombstone rules.

Snapshots pin MVCC visibility, not wall-clock time. TTL follows the engine's
existing read contract: a point read evaluates expiry at the time of that read,
while a scan fixes its wall-clock value when the cursor is created. This matches
current transaction behavior and avoids adding a second TTL clock model in the
MVP.

### 7.2 Lifetime and Compaction

The snapshot's read guard remains in the watermark until all shared users drop.
Compaction may flush and rewrite files while the snapshot exists, but it must not
remove any version visible at `T`.

Long-lived snapshots therefore increase retained-version and vLog-GC pressure.
This is expected and matches current transaction read guards. The engine should
document this cost but must not expire a snapshot automatically.

### 7.3 Shutdown

After this RFC, both `close()` and `close_async()` call `lifecycle.begin_close()`
before shutting down background workers; they reject new snapshot creation, wait
for every snapshot handle, owned async future, and derived iterator to release
admission, then flush or sync and call `finish_close()`. Concurrent close calls
wait for the same closed state and remain idempotent. `close_async()` already
does this today; `close()` does not (see §5.3) — routing it through this path is
the deliberate behavior change described there: previously it only joined
background workers and synced/flushed, neither rejecting new
writes/scans/transactions nor waiting for in-flight ones to drain.

Callers must release every snapshot, snapshot-derived cursor, and owned async
operation that they control before calling either close method. Close waits for
such admissions by design; awaiting close while retaining one of those values in
the same task would otherwise wait for a drop that cannot run. Handles owned by
other tasks may remain live while close waits for those tasks to release them.

Drop remains bounded and best-effort; callers that need deterministic completion
should use either explicit close API.

### 7.4 Checkpoints

A logical read snapshot and a physical checkpoint are different operations.
Creating a snapshot does not create files or a recoverable backup. Creating a
checkpoint while a snapshot exists is allowed, but each operation independently
captures its own consistency boundary under RFC 019.

---

## 8. Implementation Plan

### Phase 1: Synchronous Core

1. Add `Snapshot` and private shared `SnapshotInner` types.
2. Add `KvEngine::snapshot()` with lifecycle admission and `ReadGuard` capture.
3. Add synchronous `get`, `batch_get`, `scan`, and `prefix_scan` methods.
4. Add snapshot-backed synchronous iterator ownership.
5. Route synchronous `close()` through the lifecycle begin/quiesce/finish path
   used by `close_async()`.
6. Add point-read, scan, MVCC-watermark, shutdown, and drop-lifetime tests.

### Phase 2: Async API

1. Add owned `Send + 'static` `get_async`, `scan_async`, and
   `prefix_scan_async` futures.
2. Add async cursor lifetime tests where the originating `Snapshot` is dropped.
3. Verify close admission and cancellation behavior.

### Phase 3: Performance and Parallel Follow-Up

1. Add timestamped batch-get optimization if per-key reads are material in
   profiling.
2. Add parallel snapshot scans using one shared `SnapshotInner`.
3. Add metrics for active snapshot count and oldest pinned read timestamp if
   operational observability becomes necessary.

---

## 9. Test Plan

Required focused tests:

1. snapshot point read sees the value present at creation;
2. later put and delete are invisible to an existing snapshot;
3. snapshot scan and prefix scan return one stable logical key set;
4. range tombstones after snapshot creation are invisible;
5. a range tombstone visible at creation hides covered values;
6. TTL follows the existing point-read and fixed-time scan contracts rather than
   introducing a snapshot-specific wall-clock model;
7. `batch_get` uses the same timestamp across all requested keys;
8. cloned snapshots share one timestamp and retain the watermark until the last
   clone is dropped;
9. a scan retains the watermark after the originating snapshot is dropped;
10. compaction cannot remove a version visible to an active snapshot;
11. both close APIs reject snapshot creation after closing begins and wait for
    active snapshot admission before returning;
12. async point and scan futures retain the snapshot view after the originating
    snapshot handle is dropped;
13. cancelling an already-dispatched async read or scan retains its pin only
    until the blocking closure completes, after which shutdown completes;
14. snapshot-derived values controlled by the caller are released before close,
    and close completes after that release;
15. dropping an async cursor releases the final snapshot pin;
16. snapshots do not survive completed close and reopen;
17. checkpoint creation remains independent from snapshot lifetime.

Required regression checks:

```bash
cargo fmt --all -- --check
cargo test --package kv-engine snapshot
cargo nextest run --workspace --all-features --all-targets
```

---

## 10. Acceptance Criteria

This RFC is implemented when:

1. `KvEngine::snapshot()` returns a documented public read-only handle.
2. Point reads, batch reads, range scans, and prefix scans use one fixed MVCC
   timestamp until the snapshot and all derived cursors are dropped.
3. Snapshot lifetime correctly pins the MVCC watermark through compaction.
4. Async snapshot reads and cursors follow the same timestamp and lifetime
   contract.
5. Both engine close APIs and snapshot creation interact through lifecycle
   admission without leaked pins; their documented release-before-close
   precondition prevents caller-owned handles from blocking their own close.
6. Snapshots remain runtime-only and do not alter WAL, manifest, checkpoint, or
   recovery formats.
7. Focused snapshot tests and the full workspace nextest target pass.
