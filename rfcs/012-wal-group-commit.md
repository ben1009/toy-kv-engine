# WAL Group Commit — Lock-Free Buffer Pool + Leader/Follower Sync

- **Status**: Implemented (PR #130)
- **Author**: ben1009
- **Created**: 2026-06-23
- **Updated**: 2026-06-26
- **Related**: WAL lock release (MVCC lock release at begin_write); MVCC commit_ts

---

## Problem

Every write batch called `wal.put_batch()` which held `self.file.lock()` (a `parking_lot::Mutex<BufWriter<File>>`) for the entire duration: encode → write → flush. Then the caller did a separate `wal.sync()` which acquired the same mutex again for `fsync`. Under concurrent writes, this serialized all encoding work behind the file lock, and each writer paid the full `fsync` cost independently.

**Goal**: Amortize `fsync` across concurrent writers. Encode without holding any lock. Hold the file lock only for the final drain + flush + fsync.

---

## Design

### Architecture: Lock-Free Pool + Ready Queue + Leader/Follower Barrier

```
Thread A ──┐
Thread B ──┼─→ [buf_pool: ArrayQueue<Vec<u8>>] ──→ [ready_queue: SegQueue<Vec<u8>>]
Thread C ──┘                                              │
                                                          ▼
                                          Leader drains + fsyncs
                                                          │
                                                          ▼
                                          [batch_results ring + condvar] ──→ Followers wake
```

### Components

#### 1. Lock-Free Buffer Pool (`buf_pool: ArrayQueue<Vec<u8>>`)

- **Type**: `crossbeam_queue::ArrayQueue<Vec<u8>>`
- **Capacity**: 16 buffers, each pre-allocated to 64 KB (`BUFFER_POOL_BUF_SIZE`)
- **Purpose**: Threads pop a buffer, encode their batch into it, then push to the ready queue. Avoids `Vec` allocation on every write.
- **Fallback**: If pool is empty, allocate a new `Vec` with `capacity = max(batch_size, 64 KB)`. This handles burst traffic beyond pool capacity.
- **Recycling**: After successful `fsync`, drained buffers are returned to the pool (if capacity ≤ 128 KB). Failed-sync buffers are dropped (not returned to pool) to avoid corrupt data reuse.

#### 2. Ready Queue (`ready_queue: crossbeam_queue::SegQueue<Vec<u8>>`)

- **Type**: `crossbeam_queue::SegQueue<Vec<u8>>` (unbounded lock-free queue)
- **Purpose**: Threads push their filled buffers here. The leader drains all pending buffers in one pass.
- **Ordering**: Push happens *before* incrementing `commit_waiters`. This guarantees that when the leader captures `batch_end = commit_waiters.load()`, all buffers for tickets < `batch_end` are already in the queue.
- **Known limitation**: Queue is unbounded. Under sustained I/O stall, buffers accumulate. In practice, pool size (16 × 64 KB = 1 MB) bounds steady-state memory.

#### 3. Ticket System (`commit_waiters: AtomicU64`)

- Each thread calls `commit_waiters.fetch_add(1, AcqRel)` to get a unique ticket.
- Tickets are monotonically increasing and never wrap (u64).
- The ticket determines which batch a thread belongs to: batch boundaries are defined by the leader at election time.
- **`committed_gen` advances on both success and failure.** If the leader's `sync_inner()` fails, `committed_gen` is still updated to `batch_end` and followers see `BatchOutcome::Err` via the ring buffer. No thread retries the failed batch — the data is lost. This is intentional: the caller (MVCC `write_batch`) propagates the error upward.

#### 4. Leader Election (`leader_active: AtomicBool`)

- The first thread whose `ticket == committed_gen` wins the leader race via `compare_exchange(false, true, AcqRel, Acquire)`.
- Only one leader per generation. A new leader can be elected immediately after the previous one clears `leader_active`. The leader:
  1. Captures `batch_end = commit_waiters.load(Acquire)` (fence ensures all prior pushes are visible).
  2. Calls `sync_inner()` which drains → writes → flushes → fsyncs under the file lock.
  3. Writes batch results to the ring buffer.
  4. Updates `committed_gen = batch_end`, clears `leader_active`, signals condvar.

#### 5. Batch Result Ring (`batch_results: parking_lot::Mutex<[BatchResult; 256]>`)

- **Problem solved**: A single `last_failed_gen`/`last_succeeded_gen` pair cannot represent interleaved success/failure. E.g., batch [0,5) fails, batch [5,10) succeeds — threads 0-4 would incorrectly see success.
- **Design**: Ring buffer of 256 `BatchResult` entries, indexed by `ticket % 256`. Each entry records `(batch_start, batch_end, outcome)`.
- **Lookup**: `lookup_batch_result(ticket)` checks if `ticket ∈ [batch_start, batch_end)`. If slot was overwritten (ring wrapped), treats as success (generation has advanced far past the ticket). This is a probabilistic guarantee — with 16-64 concurrent threads, 256 separate leader elections must complete before wrap, which is unlikely under normal load.
- **Size**: 256 slots is sufficient for typical concurrency (16-64 threads).
- Uses `parking_lot::Mutex` (no poisoning, lower overhead than `std::sync::Mutex`).

#### 6. Condvar + Mutex (`commit_cond`, `commit_mutex`)

- Both use `parking_lot::Mutex` / `parking_lot::Condvar`.
- Followers wait on `commit_cond` under `commit_mutex`.
- **Lost wakeup prevention**: Leader updates `committed_gen` *under* `commit_mutex` before calling `notify_all()`. Followers check `committed_gen` inside the mutex before waiting.
- **Two wait patterns**:
  - **Case 3** (same generation, another leader): Wait while `leader_active` is true. Early exit if `my_ticket < committed_gen` (batch completed by a subsequent leader while we were waiting).
  - **Case 4** (future generation): Wait while `committed_gen == observed_gen`. On wakeup, loop back to the top and re-evaluate — the thread may land in Case 1, 2, or 3 on the next iteration.

### Data Flow: `put_batch()`

```
1. Validate key/value lengths (no lock)
2. Compute entries_size, total_size (no lock)
3. Pop buffer from buf_pool (lock-free)
4. Encode: [BATCH_HEADER:16][entries...] with CRC32 (no lock)
5. Push buffer to ready_queue (lock-free)
6. Return Ok(())
```

The caller then calls `submit_and_commit()` which handles the leader/follower barrier.

### Data Flow: `put_range_tombstone_batch()`

A second encoding path that also pushes to `ready_queue`:

```
1. Validate start/end lengths (no lock)
2. Allocate fresh Vec (NOT from buf_pool) — range tombstone batches are rare
3. Encode: [BATCH_HEADER:16][kind=2, start_len, start, end_len, end, ...] with CRC32
4. Push buffer to ready_queue (lock-free)
5. Return Ok(())
```

Unlike `put_batch`, this path does not use the buffer pool (allocates a fresh `Vec`). Range tombstone writes are infrequent, so pool recycling is not worthwhile. The buffer is flushed by either `sync()` (sync variant) or `submit_and_commit()` (wal-only variant via `commit_wal()`), depending on the call path.

### Data Flow: `sync()`

Two entry points drain the `ready_queue`:

1. **`submit_and_commit()`** — group commit barrier (leader/follower). Used by MVCC `write_batch`. Also used by `put_range_tombstone_batch_wal_only` callers via `commit_wal()`.
2. **`sync()`** (`pub(crate)`) — direct drain + fsync + buffer recycling. Used by `sync_wal()` and the sync variant of `put_range_tombstone_batch`.

Both call `sync_inner()`. The difference is that `sync()` does not participate in the ticket/barrier system — it simply locks the file, drains, writes, fsyncs, and returns buffers to the pool.

**Concurrency interaction**: If `sync()` and `submit_and_commit()` run concurrently, both compete for the file lock in `sync_inner()`. The first to acquire it drains all buffers. The second finds the queue empty and returns immediately (no-op). This is safe — the file lock prevents double-writing — but means the second caller does not wait for the first to finish. In practice this is fine: `sync()` is called at memtable rotation time, which does not overlap with batch commit.

### Data Flow: `submit_and_commit()`

```
1. my_ticket = commit_waiters.fetch_add(1, AcqRel)
2. Infinite loop (retry until resolved):
   a. If my_ticket < committed_gen → lookup batch_results ring → return outcome
   b. If my_ticket == committed_gen && CAS(leader_active, false→true) → become leader:
      - batch_end = commit_waiters.load(Acquire)   // capture BEFORE sync
      - sync_inner(): lock file → drain queue → write_all → flush → fsync
      - Write BatchResult[batch_start..batch_end] to ring
      - Return buffers to pool (on success only)
      - commit_mutex.lock() → committed_gen = batch_end → leader_active = false → notify_all()
      - Return result
   c. If my_ticket == committed_gen but another is leader → wait on condvar, then retry (goto 2)
   d. If my_ticket > committed_gen → wait for generation to advance on condvar, then retry (goto 2)
```

A thread can cycle through multiple iterations. E.g., a thread in Case 4 wakes when `committed_gen` advances, re-reads it, and may land in Case 1 (already committed), Case 2 (can become leader), or Case 3 (another leader for this generation).

### Data Flow: `sync_inner()`

```
1. file.lock() — BufWriter<File> mutex
2. Drain all buffers from ready_queue into local Vec
3. For each buffer: file.write_all(buf)
4. file.flush()
5. file.get_ref().sync_all()   // fsync
6. Return drained buffers (caller returns to pool on success)
```

**Key invariant**: File lock is held *only* during drain + write + flush + fsync. Encoding is fully lock-free.

**Concurrency**: The file lock is taken *before* draining so that concurrent `sync()` and `submit_and_commit()` callers cannot steal each other's buffers. The first to acquire the lock drains everything; the second finds the queue empty and returns immediately.

---

## Thread Safety Analysis

### Acquire/Release Ordering

| Operation | Ordering | Rationale |
|-----------|----------|-----------|
| `commit_waiters.fetch_add` | AcqRel | Publish buffer push; observe prior pushes |
| `commit_waiters.load` (batch_end) | Acquire | See all buffers pushed before ticket increment |
| `committed_gen.load` | Acquire | See latest committed batch |
| `committed_gen.store` | Release | Publish batch completion to followers |
| `leader_active.CAS` | AcqRel/Acquire | Mutual exclusion for leader role |
| `leader_active.store(false)` | Release | Publish leader completion |
| `leader_active.load` (in wait loop) | Acquire | See leader completion |

### Memory Ordering Proof

For thread T_writer (ticket N) to see its buffer synced by thread T_leader (ticket M, M ≤ N):

1. T_writer: `ready_queue.push(buf)` (crossbeam's `SegQueue::push` is inherently release-ordered via internal AcqRel fences) → `commit_waiters.fetch_add(1, AcqRel)` (release)
2. T_leader: `commit_waiters.load(Acquire)` → sees N < batch_end → drains queue (acquire on pop)
3. T_leader: `sync_inner()` → `committed_gen.store(batch_end, Release)`
4. T_writer: `committed_gen.load(Acquire)` → sees N < committed_gen → reads batch_results

The AcqRel on `fetch_add` establishes a happens-before: push(buf) happens-before the leader's load of `commit_waiters` which captures `batch_end`.

### Lost Wakeup Prevention

```
Leader:                              Follower:
  commit_mutex.lock()                  commit_mutex.lock()
  committed_gen = batch_end            while leader_active:
  leader_active = false                    commit_cond.wait()  // releases mutex
  commit_cond.notify_all()               // wakes up, reacquires mutex
  commit_mutex.unlock()                  // checks committed_gen → exits
```

The follower checks `committed_gen` *inside* the mutex before waiting. The leader updates `committed_gen` *under* the same mutex. This prevents the race where the follower checks `committed_gen` (sees old value), then the leader updates and signals, then the follower waits (misses the signal).

---

## Benchmark Results (PR #130)

| Benchmark | Before | After | Δ |
|-----------|--------|-------|---|
| batch_create_1000 sync | 5.70 ms | 3.09 ms | **-45.8%** |
| batch_read_100 | 225 µs | 142 µs | **-36.8%** |
| point_write_stress | 8.93 ms | 8.26 ms | -7.5% |
| fsync_latency (external) | 159 µs | 188 µs | +18.6% (expected — sync path now heavier) |

The batch_create improvement comes from amortizing fsync across 1000 entries in a single batch. The batch_read improvement is a side effect of reduced lock contention on the WAL file mutex.

---

## Alternatives Considered

### A. Simple Condvar Barrier (No Lock-Free Queue)

Use a `Mutex<Vec<Buffer>>` + condvar. Threads push to the shared vec under the mutex; leader drains under the same mutex.

**Rejected**: Still serializes encoding behind the mutex (push must lock). The lock-free pool + queue eliminates all mutex contention during encoding.

### B. Crossbeam Channel Instead of ArrayQueue + SegQueue

Use `crossbeam_channel::bounded(16)` for the pool and `unbounded()` for the ready queue.

**Rejected**: `ArrayQueue` is simpler and has lower overhead for the fixed-size pool pattern. `SegQueue` is already used for the ready queue (same as channel's unbounded queue).

### C. Single Global Buffer (No Pool)

All threads encode into a single shared buffer protected by a mutex.

**Rejected**: Serializes all encoding. Defeats the purpose of group commit.

---

## Open Questions

1. **Q: Why not use `io_uring` for async fsync?**
   A: `io_uring` requires kernel 5.1+ and adds significant complexity. The condvar-based barrier achieves the main goal (amortized fsync) with simpler code. Can revisit if fsync latency becomes the bottleneck.

2. **Q: What happens if the leader crashes mid-sync?**
   A: The `BufWriter` is flushed before `fsync`. If the process crashes after `write_all` but before `fsync`, the data is in the OS page cache but not on disk. On recovery, the incomplete batch's CRC32 check will fail and recovery will stop at the last complete batch. This is the same guarantee as the pre-group-commit design.

3. **Q: Why ring buffer instead of a simple `last_outcome` pair?**
   A: With `last_failed_gen`/`last_succeeded_gen`, interleaved failures are masked. E.g., batch [0,5) fails, batch [5,10) succeeds — threads 0-4 would read `last_succeeded_gen=10` and incorrectly return Ok. The ring buffer records per-batch outcomes.

---

## References

- WAL lock release — MVCC lock release at begin_write, not at WAL sync
- MVCC layer design — commit_ts, write_batch
- Performance optimization plan — group commit was item 6 in the optimization list
- crossbeam_queue::ArrayQueue — https://docs.rs/crossbeam-queue
- crossbeam_queue::SegQueue — https://docs.rs/crossbeam-queue
