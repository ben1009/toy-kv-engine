# RFC 012: Parallel WAL — Ordered Commit with Piggyback Fsync

**Status:** Proposed
**Date:** 2026-06-26
**Author:** kv-engine Contributors
**Reference:** SpanDB (FAST 2021) — "SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"

---

## 1. Summary

This RFC proposes improving WAL write throughput by replacing the current
group-commit leader-follower model with an **ordered commit sequencer** that
piggybacks fsyncs. The key insight from the SpanDB paper is that `fsync` flushes
all dirty pages for the entire file — so multiple batches written between two
fsyncs get committed for free.

The design:

1. **Atomic offset allocator** — each batch gets a sequential file offset via `fetch_add`.
2. **Single writer** — batches are written sequentially to the WAL file (same as current).
3. **Record checksum** — CRC32 per batch (already exists, unchanged).
4. **Ordered commit sequencer** — batches are committed in order; fsync is skipped when a prior fsync already covered the data.
5. **Recovery scan** — sequential scan, validate CRC, stop at first invalid entry (unchanged).

This is Phase 1 of a two-phase plan. Phase 2 (future, RFC 002) adds io_uring
for parallel I/O submission and kernel-level fsync batching.

---

## 2. Motivation

### Current Bottleneck

The current WAL uses a single file with a leader-follower group commit:

```
Writer threads → ready_queue (SegQueue) → leader drains → BufWriter → fsync → wake followers
```

The bottleneck is **fsync** (50-100µs on NVMe, 1-10ms on SATA). Every batch
triggers one fsync, even when multiple batches arrive between fsyncs:

```
Time:   0µs        50µs       100µs      150µs
        Batch 0    Batch 1    Batch 2    Batch 3
        ───fsync─── ───fsync─── ───fsync─── ───fsync───
        4 fsyncs for 4 batches = 200µs total
```

### The Piggyback Insight

`fsync` flushes **all** dirty pages for the file, not just the last write.
If batches 0, 1, 2 are all written before the first fsync, one fsync covers all three:

```
Time:   0µs        50µs       100µs
        Batch 0    Batch 1    Batch 2
        ───fsync───────────────────────
        1 fsync for 3 batches = 50µs total (4× faster)
```

The sequencer ensures ordering: a batch is only committed after all prior
batches are durably written. This prevents the race where a later batch's
fsync completes before an earlier batch's write.

---

## 3. Goals

1. Replace leader-follower group commit with an ordered commit sequencer.
2. Piggyback fsyncs — skip fsync when a prior fsync already covered the data.
3. No changes to the public `KvEngine` API.
4. Backward compatible WAL file format (v3, unchanged).
5. Recovery unchanged — sequential scan + CRC validation.

## 4. Non-Goals

1. Parallel pwrite (deferred to Phase 2 with io_uring).
2. Multiple WAL files / shards (unnecessary with piggyback fsync).
3. SPDK integration.
4. Changing the WAL file format.

---

## 5. Design

### 5.1 Current vs Proposed

| Aspect | Current (Group Commit) | Proposed (Sequencer) |
|--------|----------------------|---------------------|
| Writer role | Push buffer, wait for leader | Push buffer, write, register with sequencer |
| Leader | Drains queue, writes, fsyncs, wakes followers | No leader |
| Fsync trigger | Every batch | Only when no prior fsync covers this batch |
| Ordering | Implicit (leader writes in queue order) | Explicit (sequencer tracks committed offset) |
| Followers | Block on condvar | Block on sequencer (check if prior batch committed) |

### 5.2 Sequencer State

```rust
/// Tracks the commit state of the WAL.
struct CommitSequencer {
    /// Highest file offset that has been fsynced to disk.
    synced_offset: AtomicU64,
    /// Highest file offset that has been written (but not necessarily synced).
    written_offset: AtomicU64,
    /// Mutex + condvar for commit signaling.
    commit_mutex: Mutex<()>,
    commit_cond: Condvar,
}
```

### 5.3 Write + Commit Flow

```rust
fn write_and_commit(&self, buf: &[u8]) -> Result<()> {
    // 1. Allocate file offset atomically.
    let offset = self.sequencer.written_offset.fetch_add(
        buf.len() as u64, Ordering::AcqRel
    );

    // 2. Write to file at allocated offset (single writer, sequential).
    //    Currently: BufWriter under mutex (same as today).
    //    Phase 2: io_uring SQE submission (parallel).
    {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;
    }

    // 3. Register with sequencer and commit.
    self.sequencer.commit(offset + buf.len() as u64)
}
```

### 5.4 Sequencer Commit Logic

```rust
impl CommitSequencer {
    fn commit(&self, my_end_offset: u64) -> Result<()> {
        // Wait for all prior batches to be committed.
        let mut guard = self.commit_mutex.lock();
        while self.synced_offset.load(Ordering::Acquire) < my_end_offset {
            // Check if we need to fsync.
            let synced = self.synced_offset.load(Ordering::Acquire);
            let written = self.written_offset.load(Ordering::Acquire);

            // If there are unwritten-or-unsynced bytes before us, we may need
            // to fsync. But only the first thread in a contiguous group does it.
            if synced < my_end_offset {
                // Fsync — this covers ALL dirty pages, including prior batches.
                self.file.lock().sync_all()?;

                // Advance synced_offset to cover all written data.
                // This wakes all waiters whose batches are now durable.
                self.synced_offset.store(written, Ordering::Release);
                self.commit_cond.notify_all();
                return Ok(());
            }

            // Another thread is fsyncing for us — wait.
            self.commit_cond.wait(&mut guard);
        }

        // Our batch was already covered by a prior fsync.
        Ok(())
    }
}
```

### 5.5 How Piggyback Works

Example with 3 concurrent writers:

```
Writer A: offset=0,   len=100  → pwrite 0-100   → commit(100)
Writer B: offset=100, len=50   → pwrite 100-150  → commit(150)
Writer C: offset=150, len=80   → pwrite 150-230  → commit(230)

Sequencer state: synced_offset=0, written_offset=230

Writer A arrives at commit(100):
  synced_offset (0) < my_end_offset (100) → fsync
  fsync flushes pages 0-230 (all dirty data)
  synced_offset = 230 (written_offset)
  notify_all
  → Writer A returns ✓

Writer B arrives at commit(150):
  synced_offset (230) >= my_end_offset (150) → already committed
  → Writer B returns immediately ✓ (no fsync)

Writer C arrives at commit(230):
  synced_offset (230) >= my_end_offset (230) → already committed
  → Writer C returns immediately ✓ (no fsync)

Result: 1 fsync for 3 batches (vs 3 fsyncs in current design)
```

### 5.6 The Ordering Race (Why Sequencer Is Needed)

Without the sequencer, a dangerous race exists:

```
Writer C: pwrite at 150 → fsync → returns to caller (thinks data is safe)
Writer A: pwrite at 0   → NOT DONE YET
  crash → Batch 0 lost, but Writer C's caller was told it's safe ✗
```

With the sequencer:

```
Writer C: pwrite at 150 → commit(230) → waits for synced_offset >= 230
Writer A: pwrite at 0   → commit(100) → fsync → synced_offset = 230
  → Writer C's commit(230) satisfied → returns ✓
  → All data 0-230 is on disk before either caller returns ✓
```

The rule: **a batch is committed only after all prior bytes are fsynced**.

### 5.7 Recovery

No changes. Recovery scans the file sequentially:

1. Read batch header (commit_ts, entry_count, crc32).
2. Validate CRC32 over entry data.
3. If valid, replay entries into skiplist.
4. If invalid (CRC mismatch, truncated), stop — this is the recovery point.
5. Truncate file to last valid byte.

This works because:
- Batches are written in offset order (atomic allocator guarantees this).
- The sequencer ensures all bytes up to `synced_offset` are on disk.
- On crash, bytes beyond `synced_offset` may be partial — CRC catches this.

---

## 6. Trade-offs

### Advantages

1. **Fewer fsyncs** — Under concurrency, multiple batches piggyback on one fsync. The higher the concurrency, the bigger the win.
2. **Simpler code** — No leader election, no condvar wake-up storms, no batch results ring buffer. Just atomic offset + sequencer.
3. **No new files** — Single WAL file, same format, same recovery.
4. **Phase 2 ready** — The atomic offset allocator maps directly to io_uring SQE offsets.

### Disadvantages

1. **Single writer still** — Batches are written sequentially under the file mutex. Same as current design. (Phase 2 with io_uring adds parallel writes.)
2. **Fsync latency unchanged** — Each fsync still takes 50-100µs on NVMe. The win is fewer fsyncs, not faster fsyncs.
3. **Low concurrency regression risk** — With 1 writer, every batch fsyncs (same as now). The sequencer overhead (atomic ops, mutex) is pure overhead. Mitigation: short-circuit when `num_writers == 1`.

### When It Helps Most

- **High concurrency** (4+ writer threads) — more batches arrive between fsyncs.
- **Fast NVMe** — fsync is cheap but not free; fewer fsyncs = less total time.
- **Small batches** — each batch's write time is negligible; fsync dominates.

### When It Helps Least

- **Single writer** — no batching opportunity; sequencer overhead is wasted.
- **SATA/HDD** — fsync is so slow (1-10ms) that the software overhead is negligible.

---

## 7. Phase 2: io_uring (Future, RFC 002)

Phase 2 replaces the sequential write under mutex with parallel io_uring submissions:

```
Phase 1 (this RFC):
  Writer A: lock → seek → write_all → unlock → commit
  Writer B: lock → seek → write_all → unlock → commit   (serialized)

Phase 2 (RFC 002):
  Writer A: submit SQE [write@0]   → commit (poll CQE)
  Writer B: submit SQE [write@100] → commit (poll CQE)   (parallel)
```

Benefits of Phase 2:
- **No file mutex** — io_uring ring is lock-free (MPSC queue).
- **Parallel writes** — kernel dispatches to NVMe channels.
- **Linked SQEs** — chain write→fsync, kernel handles sequencing.
- **Poll mode** — near-SPDK latency without SPDK's access restrictions.

The atomic offset allocator from Phase 1 is reused directly as the SQE offset.

---

## 8. Implementation Plan

| Phase | Description | Files |
|-------|-------------|-------|
| 1 | Add `CommitSequencer` struct to `wal.rs` | `wal.rs` |
| 2 | Replace `submit_and_commit()` with sequencer-based commit | `wal.rs` |
| 3 | Remove leader-follower group commit code | `wal.rs` |
| 4 | Add short-circuit for single-writer case | `wal.rs` |
| 5 | Add tests | `tests/wal.rs` |
| 6 | Benchmark: `write-perf --workload wal_concurrent` | — |

---

## 9. Testing Strategy

1. **Correctness:** All existing WAL tests pass (format unchanged).
2. **Piggyback:** Verify that concurrent writers trigger fewer fsyncs (instrument with counter).
3. **Ordering:** Crash-injection test — verify no batch is committed before prior batches are durable.
4. **Single writer:** Verify no regression (short-circuit path).
5. **Benchmark:** `write-perf --workload wal_concurrent --threads 1,2,4,8` — compare vs current group commit.

---

## 10. Open Questions

1. **Mutex vs RwLock for file access:** The file is currently under `Mutex`. With the sequencer, only one writer writes at a time (sequential), so `Mutex` is correct. Phase 2 (io_uring) removes the mutex entirely.
2. **BufWriter retention:** Should we keep `BufWriter` or switch to raw `File`? `BufWriter` batches small writes into larger ones, reducing syscall count. Recommendation: keep `BufWriter` for Phase 1, switch to raw `File` with io_uring in Phase 2.
3. **Fsync batching window:** Should we add a small delay (e.g., 10µs) to let more writers arrive before fsyncing? This trades latency for throughput. Recommendation: no delay for Phase 1 — the sequencer already batches naturally under concurrency.
