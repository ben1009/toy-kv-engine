# RFC 003: Thread-per-Core with compio — Moving to Async io_uring

**Date:** 2026-06-02
**Status:** Proposal — see [perf-profile.md](../docs/perf-profile.md) for bottleneck analysis

## 1. Why This Document

We benchmarked io_uring via the low-level `io-uring` crate (RFC 002) and found it **3-12x slower** than `std::fs` for single-operation patterns. The root cause: per-operation overhead of `io_uring_enter` + CQE drain (~5-6µs) negates any benefit when each I/O is submitted and waited on individually.

io_uring only wins with **batching and true async** — submit many operations, continue working, reap completions later. That requires an async runtime, not just swapping `fsync(2)` for `io_uring_enter`. This document proposes adopting **compio** as the async I/O runtime.

### Simpler Alternatives First

Before proposing a full async migration, consider what can be done with `std::fs` alone:

| Alternative | Effort | Benefit | Limitation |
|-------------|--------|---------|------------|
| Manifest batching (buffer N records, 1 `sync_all`) | 10 lines | Reduces manifest fsyncs N:1 | Doesn't help flush-thread-blocking-on-fsync |
| Increase `BufWriter` buffer for vLog | 1 config change | Fewer buffer-full flushes | Doesn't coalesce header+key+value+padding |
| `rayon::spawn` for parallel flush/compact | Small | Core affinity without `!Send` | No io_uring, no async overlap |

**Manifest batching with `std::fs` should be done regardless of this RFC.** It is a standalone improvement. The rest of this RFC addresses the harder problem: overlapping SST fsync with next-SST construction, which requires async I/O.

## 2. Current Architecture

```
┌─────────────────────────────────────────────────┐
│                  KvEngine                        │
│                                                  │
│  put() ──► WAL (mutex + BufWriter + sync_all)   │
│  get() ──► MemTable (lock-free skiplist)        │
│            └──► SST (pread, no lock)             │
│            └──► vLog (pread, no lock)            │
│                                                  │
│  Background threads:                             │
│    flush_thread  (polls 50ms, freeze + write SST)│
│    compact_thread (polls 50ms, merge SSTs)       │
│    gc_threads[]  (ad-hoc, per compaction)        │
│                                                  │
│  All I/O: synchronous std::fs                    │
│  Synchronization: parking_lot, crossbeam         │
└─────────────────────────────────────────────────┘
```

**Problems:**
1. Flush thread blocks on `sync_all()` — can't start next SST until fsync completes
2. Manifest writes `sync_all()` on every record — 2 syscalls per record, zero batching
3. WAL mutex holds during entire write+sync cycle
4. vLog does 4 separate `write_all` calls per entry (header, key, value, padding)
5. No batching anywhere — every I/O is submit-wait-submit-wait

### Profiling

See [docs/perf-profile.md](../docs/perf-profile.md) for full profiling results across sequential, random, mixed read/write, and concurrent workloads.

## 3. What Is compio?

[compio](https://github.com/compio-rs/compio) is a **completion-based** async I/O proactor for Rust. Unlike Tokio (poll-based reactor), compio submits I/O operations to the kernel and receives completions — the natural model for io_uring.

| Property | compio | Tokio | glommio | monoio |
|----------|--------|-------|---------|--------|
| Model | Completion (proactor) | Poll (reactor) | Completion | Completion |
| Linux backend | io_uring | epoll | io_uring | io_uring |
| Windows backend | IOCP | afd (undocumented) | — | — |
| macOS fallback | polling | kqueue | — | kqueue |
| Thread model | Thread-per-core | Work-stealing | Thread-per-core | Thread-per-core |
| Version | 0.19.0 | 1.x | 0.7.0 | 0.2.4 |
| Last release | May 2026 | Active | Feb 2022 | Active |
| Cross-platform | Yes (Linux/Win/Mac) | Yes | Linux only | Linux + Mac |

### Key compio API Differences from std::fs

compio's `File` is **not** a drop-in replacement for `std::fs::File`:

1. **Positional I/O only** — no internal cursor. Every read/write takes an explicit offset (`read_at`, `write_all_at`).
2. **Buffer ownership** — write operations return `BufResult<T, B>` = `(io::Result<T>, B)`, giving the buffer back after the I/O completes. You must handle the tuple.
3. **No `write_all`/`read_all`** — these are `write_all_at`/`read_all_at` with an offset parameter.
4. **Vectored writes** — `write_vectored_at(bufs, pos)` via `AsyncWriteAt` trait, not `writev`.

### Maturity Risk

compio is pre-1.0 (v0.19.0). The Rust async ecosystem has many abandoned projects. Mitigations:
- Abstract I/O behind a trait so compio can be swapped for `std::fs` or another runtime
- Pin the version; upgrade deliberately
- The trait abstraction is the real deliverable — compio is one possible implementation

### Thread-per-Core Constraints

compio, glommio, and monoio all share the thread-per-core model. Tasks spawned on one core's executor cannot move to another. State that was previously `Send` (shared via `Arc<Mutex<T>>`) cannot freely cross core boundaries. This is inherent to the thread-per-core pattern, not specific to glommio. The real differentiator for compio is cross-platform support and active maintenance.

**Why compio over glommio:**
- Cross-platform (Windows IOCP, macOS polling fallback) — we don't lose CI on non-Linux
- Active development (1.7k stars, 1784 commits, last release May 2026)
- glommio is effectively unmaintained (last release **Feb 2022**, 70 open issues)
- Modular workspace (18 sub-crates) — can use only `compio-fs` without pulling in networking

**Why compio over monoio:**
- Cross-platform Windows support via IOCP
- More active recent development

**Why not Tokio:**
- Poll-based (reactor), not completion-based (proactor) — wrong model for io_uring
- `tokio-uring` exists but is "very young" and requires a separate executor
- Work-stealing model conflicts with thread-per-core SQ/CQ locality

## 4. Target Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    KvEngine (compio)                      │
│                                                           │
│  put() ──► WAL ──► submit write+fsync to ring            │
│                    (mutex held only for submit, not sync) │
│                                                           │
│  get() ──► MemTable (unchanged, lock-free skiplist)      │
│            └──► SST (pread, sync — see §6 Read Path)     │
│            └──► vLog (pread, sync — see §6 Read Path)    │
│                                                           │
│  Core 0: flush + manifest task                            │
│    - freeze memtable                                       │
│    - write SST (write_vectored_at, coalesced blocks)      │
│    - submit fsync to ring                                  │
│    - start next SST immediately (don't wait for fsync)    │
│    - reap fsync CQE before manifest commit                │
│    - batch manifest records, single fsync per batch       │
│                                                           │
│  Core 1: compaction task                                  │
│    - merge SSTs with async reads + writes                 │
│    - batch fsync for output SSTs                          │
│                                                           │
│  Core 2: GC task                                          │
│    - async vLog reads + rewrite                           │
│                                                           │
│  Core 3: engine API (accepts put/get from clients)        │
│    - writes WAL, submits to flush task                    │
└──────────────────────────────────────────────────────────┘
```

### Core Count Scaling

The 4-core layout is a default. On machines with fewer cores, tasks are co-located:
- 1 core: all tasks share one core (degraded, but functional)
- 2 cores: API+WAL on core 0, flush+compact+GC on core 1
- 3 cores: API+WAL on 0, flush+manifest on 1, compact+GC on 2
- 4+: as shown above

## 5. Public API Strategy

The current public API is synchronous: `fn put()`, `fn get()`, `fn close()`. compio requires an async runtime. Three options:

### Option A: Internal Runtime (Chosen)

Keep the public API synchronous. Embed a `compio::Runtime` inside `KvEngine`. Each public method calls `runtime.block_on(async_operation)`.

```rust
pub struct KvEngine {
    runtime: compio::runtime::Runtime,
    inner: Arc<KvEngineInner>,
    // ...
}

impl KvEngine {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.runtime.block_on(self.inner.put_async(key, value))
    }
}
```

**Pros:** Existing tests and CLI work unchanged. No async infection.
**Cons:** `block_on` blocks the calling thread. Nested `block_on` (calling `put` from within a compio task) deadlocks. Must ensure no public method is called from within an async context.

### Option B: Fully Async API

Change all public methods to `async fn`. Push the runtime burden to the caller.

```rust
impl KvEngine {
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> { ... }
}
```

**Pros:** No `block_on` overhead. Caller controls concurrency.
**Cons:** Every test, the CLI, and any user code must become async. Major API break.

### Option C: Channel Bridge

Run compio on a background thread. Public API sends requests over a channel, receives results back.

**Pros:** Clean separation. No `block_on`.
**Cons:** Channel overhead per operation. Latency for every put/get.

**Decision: Option A** — minimal disruption. The CLI and all 60+ tests continue to work. The `block_on` overhead is acceptable because the actual I/O (the slow part) happens asynchronously inside the runtime.

## 6. Read Path

The read path uses `pread` via `FileExt::read_exact_at` — it is already positional and lock-free. **No change in Phases 1-3.** Reads stay synchronous with `std::fs`.

Phase 4 considerations (if reads are migrated to async):
- `SsTableIterator`, `SstConcatIterator`, `MergeIterator` all perform reads during iteration. Making them async cascades through the entire iterator chain.
- `moka::sync::Cache` (block cache) is synchronous. An async read blocking on a sync cache lookup defeats the purpose.
- The fast path (`MemTable::get()`) is lock-free, no I/O — async adds overhead with no benefit.

**Decision: Keep reads synchronous.** The read path is not a bottleneck (pread is efficient for random access). Async reads would cascade through the iterator stack for minimal gain.

## 7. close() / Drop Lifecycle

Current `close()` does synchronous `join()` on flush, compaction, and GC threads. `Drop` sends shutdown signals via `crossbeam_channel`.

In the compio world:
- `close()` must await async tasks, but `close()` is `fn` (not `async fn`)
- `Drop` cannot be `async` — you cannot `await` in `Drop`

### Shutdown Protocol

```rust
impl KvEngine {
    /// Graceful shutdown. Must be called before Drop.
    pub fn close(&self) -> Result<()> {
        // 1. Send shutdown signal to all tasks
        self.shutdown_token.cancel();
        // 2. Block on task completion (safe: we're not inside a compio task)
        self.runtime.block_on(async {
            // await all spawned tasks
        });
        // 3. Final flush: write remaining memtable to disk
        self.runtime.block_on(self.inner.final_flush())
    }
}

impl Drop for KvEngine {
    fn drop(&mut self) {
        // If close() was not called, force-shutdown:
        // - Cancel all tasks
        // - Do NOT await (can't await in Drop)
        // - Log a warning: "KvEngine dropped without close()"
        //
        // This is acceptable because Drop is a safety net,
        // not the normal shutdown path.
    }
}
```

**Risk:** If the user forgets `close()`, in-flight writes may be lost. This matches the current behavior (Drop sends stop signals but doesn't guarantee completion). Document that `close()` must be called for durability.

## 8. Durability Guarantees

### Current Semantics

Every manifest record is individually durable after `sync_all()`. Every SST is individually durable after `sync_all()`. A crash at any point leaves a consistent (possibly stale) state that `open()` can recover.

### Phase 2 Batching Changes

Batching manifest records (N writes + 1 fsync) changes the recovery boundary:
- A crash between the first write and the fsync loses all N buffered records
- The manifest may be missing records for SSTs that were flushed

**Mitigation:** The recovery protocol in `open()` already handles partial manifests — it reads the manifest up to the last valid record and rebuilds state. Lost manifest records mean those SSTs are orphaned (exist on disk but not referenced). The existing GC path handles orphaned SSTs.

**Ordering constraint:** The manifest fsync MUST complete before the corresponding SST WAL entries are deleted. This is already the case (WAL deletion happens after manifest commit).

### Phase 3 Async Pipeline Changes

The flush task submits SST fsync asynchronously and starts the next SST. The `ManifestRecord::Flush` and WAL cleanup for the flushed SST **must NOT be committed until the async fsync is confirmed complete**. Otherwise a crash could leave the manifest pointing to an SST whose data was never made durable.

```rust
// CORRECT: await fsync before manifest commit
let fsync_future = file.sync_all();
// ... start building next SST ...
fsync_future.await?;  // reap completion
manifest.add_record(ManifestRecord::Flush(sst_id))?;  // NOW safe to commit
```

## 9. Dependency Disposition

| Dependency | Current Use | Phase 1-3 | Phase 4 |
|-----------|------------|-----------|---------|
| `parking_lot` | Mutex/RwLock in 9 files | Kept as-is | Partially removed (WAL mutex eliminated; `state_lock`, manifest, vLog, MVCC kept with "no await while held" rule) |
| `crossbeam-channel` | Flush/compact thread communication | Kept | Replaced by compio async notification |
| `crossbeam-skiplist` | Memtable (lock-free) | Kept | Kept (foundation, no replacement) |
| `crossbeam-epoch` | Epoch GC for skiplist | Kept | Kept (transitive dep of skiplist) |
| `moka` | Block cache | Kept | Kept (sync cache, reads stay sync) |
| `ouroboros` | Self-referential structs in iterators | Kept | May need `Pin`-safe wrappers for async iteration |

### parking_lot Across .await

`parking_lot::MutexGuard` is `!Send`. Holding it across an `.await` point pins the task to that thread and blocks other tasks on the same core. Rule: **never hold a `parking_lot` guard across `.await`**. Each mutex site:

| Mutex | File | Async Fate |
|-------|------|-----------|
| `state_lock: Mutex<()>` | lsm_storage.rs | Held only in sync sections; drop before any `.await` |
| `state: RwLock<Arc<...>>` | lsm_storage.rs | Read lock is brief, no `.await` while held |
| WAL `Mutex<BufWriter<File>>` | wal.rs | Eliminated in Phase 4 (single-writer task on API core) |
| Manifest `Mutex<File>` | manifest.rs | Kept; manifest writes are serialized, no `.await` while held |
| vLog `Mutex`/`RwLock` | vlog/mod.rs | Kept; vLog mutations are brief |
| MVCC `Mutex` | mvcc.rs, mvcc/txn.rs | Kept; no `.await` while held |

## 10. Migration Plan

### Phase 1: compio I/O Foundation

**Not a drop-in replacement.** compio's `File` uses positional I/O and buffer-ownership returns. Every call site must track file offsets and handle `BufResult` tuples.

**Scope:** Manifest writes, SST `sync_all`, vLog writes.

```rust
// Before (manifest.rs)
file.write_all(&data)?;
file.sync_all()?;

// After (compio) — positional I/O, buffer ownership
let file = compio::fs::File::create(&path).await?;
let (res, _buf) = file.write_all_at(data, offset).await?;
res?;
file.sync_all().await?;
```

```rust
// Before (vLog builder)
file.write_all(&header)?;
file.write_all(&key)?;
file.write_all(&value)?;
file.write_all(&padding)?;

// After (compio) — vectored write, single syscall
let bufs = [header.as_slice(), key, value, padding.as_slice()];
let (res, _bufs) = file.write_vectored_at(bufs, offset).await?;
res?;
```

**Sync/async bridging:** Phase 1 runs inside the background flush/compact threads. Each thread creates a `compio::runtime::Runtime` and calls `block_on()` for its async I/O operations. This is wasteful (multiple runtimes) but unblocks Phase 1 without restructuring the threading model.

**Risk:** Medium. API restructuring required at every call site. Each change is isolated and testable independently.

**Validation:** All existing tests pass. `cargo bench` shows no more than 5% regression vs `std::fs` baseline.

### Phase 2: Batching

Batch manifest records and coalesce vLog entries.

```rust
// Manifest: batch N records, single fsync
for record in records {
    let (res, _) = file.write_all_at(record.encode(), offset).await?;
    res?;
    offset += record.size();
}
file.sync_all().await?;  // one fsync for all N records
```

**Durability impact:** See §8. Recovery protocol handles partial manifests. Ordering: manifest fsync before WAL deletion.

**Validation:** Batched manifest benchmarks. Crash-recovery test with simulated power loss (kill -9 during write burst, verify `open()` recovers).

### Phase 3: Async Flush Pipeline

The flush task submits SST fsync asynchronously and starts the next SST.

```
freeze memtable
  → write SST (async write_vectored_at)
  → submit fsync (don't await yet)
  → start building next SST from new frozen memtable
  → await fsync completion
  → commit ManifestRecord::Flush (only after fsync confirmed)
```

**This is the key throughput gain.** The flush thread currently blocks on fsync for every SST. With async fsync, it overlaps SST writes with memtable accumulation.

**Validation:** Throughput benchmark: writes/sec with concurrent put() and flush. Target: ≥30% improvement over Phase 2.

### Phase 4: Full Thread-per-Core

Migrate flush, compaction, and GC from `std::thread::spawn` to compio tasks pinned to cores. Replace `crossbeam_channel` with compio async notification.

```
Core 0: engine API (accepts put/get, writes WAL)
Core 1: flush + manifest
Core 2: compaction
Core 3: GC + background reads
```

This eliminates:
- `parking_lot::Mutex` on WAL (WAL write happens on API core, flush reads via shared state)
- `crossbeam_channel` overhead (replaced by in-task async)
- Thread context switching between flush/compact/GC

**WAL ownership in thread-per-core:** The `put()` path on Core 0 writes to the WAL directly (single-writer, no mutex needed). The flush task on Core 1 reads WAL data via the shared `LsmStorageState` (which contains the memtable, not the WAL file). WAL file access is only needed during recovery (`open()`), not during normal operation.

**Risk:** High. `!Send` constraints mean `parking_lot` mutexes cannot be held across core boundaries. `ouroboros` self-referential structs in iterators may need `Pin`-safe wrappers. 60+ tests may need `#[compio::test]` migration if the internal runtime model changes.

**Validation:** All tests pass. Throughput benchmark shows improvement over Phase 3. Flamegraph confirms I/O overlap.

## 11. Expected Impact

Based on RFC 002 benchmark data (kernel 6.18.9, `io-uring` 0.7.12). Note: the 301ns manifest figure was measured on tmpfs; real NVMe fsync is typically 10-100µs. The **relative** improvement from batching still holds.

| Operation | Current | After Phase 2 | After Phase 3 |
|-----------|---------|---------------|---------------|
| Manifest write+fsync | N × (write + fsync) | N × write + 1 fsync | Same, but flush not blocked |
| SST fsync | Blocks flush thread | Same | Flush thread free immediately |
| vLog entry | 4 `write_all` calls | 1 `write_vectored_at` | Same, but not blocked by flush |
| WAL sync | Mutex held during sync | Mutex held for submit only | Same |

**The big win is throughput from overlapping I/O with computation, not per-operation latency.** The flush thread currently blocks on fsync for every SST. With async fsync, it can start building the next SST immediately. The magnitude of this gain depends on SST size and fsync latency on the target hardware — it must be measured, not assumed.

## 12. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| compio is pre-1.0 (v0.19) | API instability | Pin version, abstract behind trait. The trait is the deliverable, not compio. |
| `!Send` constraints from thread-per-core | Can't share state across cores | Message passing for mutable state, `Arc` for read-only state. Phase 4 only. |
| `parking_lot::MutexGuard` held across `.await` | Blocks core, deadlocks | Rule: never hold guard across `.await`. Enforce in code review. |
| Linux-only for io_uring | Lose macOS CI | compio has polling fallback; gate io_uring behind `cfg(target_os = "linux")` |
| `ouroboros` self-referential structs + async | Borrow-checker issues at `.await` points | May need `Pin`-safe wrappers or restructuring |
| Debugging async I/O | Harder to trace | Use `compio-log` sub-crate, add tracing |
| Existing tests are sync | Need async test harness | Option A (internal runtime) keeps tests sync. Phase 4 may need `#[compio::test]`. |
| I/O is not the actual bottleneck | RFC provides no benefit | Profile first (flamegraph). Defer if CPU-bound. |
| Crash recovery with batched writes | Lost manifest records | Recovery handles partial manifests. Ordering: fsync before WAL deletion. |
| Minimum kernel version | io_uring features require 5.4+ | compio falls back to polling on older kernels. Target 5.8+ for full feature set. |

## 13. Why Not glommio?

- **Effectively unmaintained** — last release **Feb 2022** (4+ years), 70 open issues, 17 open PRs
- **Linux-only** — no Windows/macOS fallback, breaks cross-platform CI
- **`!Send`/`!Sync`** — same thread-per-core constraints as compio; glommio's `LocalExecutor` is more explicit about it, but the constraints are inherent to the model
- **No modular install** — must take the entire runtime even if only needing file I/O

## 14. Decision

**Status: Proposal.** Profiling (see [perf-profile.md](../docs/perf-profile.md)) shows the engine is currently CPU-bound, not I/O-bound. The RFC remains as documentation for when CPU bottlenecks are resolved and I/O becomes the dominant cost.

**Prerequisites before implementing:**
1. Optimize CPU bottlenecks (SST building, moka overhead, skiplist ops)
2. Re-profile to confirm I/O has become dominant
3. Then proceed with compio migration

**Do manifest batching with `std::fs` regardless** — it's a standalone 10-line improvement that reduces syscall overhead.

## 15. References

- [RFC 002: io_uring for Disk Writes](./002-io-uring-disk-writes.md) — research and crate comparison
- [io_uring Benchmark Results](../docs/io-uring-bench.md) — why per-operation io_uring is slower
- [Performance Profiling Report](../docs/perf-profile.md) — bottleneck analysis across all workloads
- [compio GitHub](https://github.com/compio-rs/compio) — runtime source and docs
