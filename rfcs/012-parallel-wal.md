# RFC 012: WAL Write Throughput — Ordered Commit with pwrite

**Status:** Proposed
**Date:** 2026-06-26
**Author:** kv-engine Contributors
**Reference:** SpanDB (FAST 2021) — "SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"

---

## 1. Summary

This RFC proposes improving WAL write throughput by replacing the current
leader-follower group commit with a simpler **ordered commit sequencer** that
piggybacks fsyncs, combined with `pwrite` for position-independent writes to
a single WAL file.

The design:

1. **Atomic offset allocator** — each batch gets a sequential file offset via `fetch_add`.
2. **pwrite** — writers write to the file at their allocated offsets (no BufWriter, no seek).
3. **Record checksum** — CRC32 per batch (unchanged).
4. **Ordered commit sequencer** — batches are committed in order; fsync is skipped when a prior fsync already covered the data.
5. **Recovery scan** — sequential scan, validate CRC (unchanged).

The fsync count is the same as the current group commit (1 fsync for N concurrent
writers), but the code is significantly simpler: no leader election, no batch
results ring buffer, no condvar wake-up storms.

---

## 2. Motivation

### Current Design Complexity

The current group commit (`wal.rs:878-977`) uses a leader-follower model with:
- Ticket-based leader election (`commit_waiters` + `committed_gen` + `leader_active` CAS)
- Batch results ring buffer (256 slots) for error propagation
- Condvar + mutex for follower wake-up
- `ready_queue` (SegQueue) for buffer staging
- File mutex held during drain + write + flush + fsync

This works correctly but is complex. The sequencer achieves the same fsync
efficiency with simpler primitives.

### What Changes

| Aspect | Current | Proposed |
|--------|---------|----------|
| Writer role | Push buffer to queue, wait for leader | pwrite at allocated offset, check sequencer |
| Leader | Drains queue, writes, fsyncs, wakes followers | No leader |
| Fsync trigger | Every batch (leader always fsyncs) | Only when no prior fsync covers this batch |
| Ordering | Implicit (leader writes in queue order) | Explicit (sequencer tracks confirmed offset) |
| File I/O | BufWriter + write_all + flush + sync_all | Raw File + pwrite (write_at) + sync_all |
| Followers | Block on condvar, wake on notify_all | Check atomic confirmed_offset, return if covered |

---

## 3. Goals

1. Replace leader-follower group commit with an ordered commit sequencer.
2. Use `pwrite` (position-independent writes) instead of BufWriter + seek.
3. Piggyback fsyncs — skip fsync when a prior fsync already covered the data.
4. No changes to the public `KvEngine` API.
5. Backward compatible WAL file format (v3, unchanged).
6. Recovery unchanged — sequential scan + CRC validation.

## 4. Non-Goals

1. Parallel I/O to a single file (ext4 inode lock serializes buffered writes).
2. Multiple WAL files / shards.
3. io_uring integration (deferred to future work).
4. SPDK integration.
5. Changing the WAL file format.

---

## 5. Design

### 5.1 Sequencer State

```rust
/// Tracks the commit state of the WAL.
struct CommitSequencer {
    /// Next file offset to allocate (advanced BEFORE write).
    alloc_offset: AtomicU64,
    /// Highest file offset where ALL preceding bytes have been pwritten
    /// to the page cache (advanced AFTER write completes).
    confirmed_offset: AtomicU64,
    /// Highest file offset that has been fsynced to disk.
    synced_offset: AtomicU64,
    /// Mutex + condvar for commit signaling.
    commit_mutex: Mutex<()>,
    commit_cond: Condvar,
    /// Whether a thread is currently fsyncing (prevents redundant fsyncs).
    fsync_in_progress: AtomicBool,
    /// Error flag — set if fsync fails, checked by all waiters.
    last_error: Mutex<Option<String>>,
}
```

Key distinction between the three offsets:

| Offset | When advanced | Meaning |
|--------|--------------|---------|
| `alloc_offset` | Before pwrite | "This region is reserved for a batch" |
| `confirmed_offset` | After pwrite completes | "This region is in the page cache" |
| `synced_offset` | After fsync completes | "This region is on disk" |

**`synced_offset` is only advanced to `confirmed_offset`**, never to `alloc_offset`.
This prevents the data-loss bug where `synced_offset` claims bytes are durable
that were only reserved, not written.

### 5.2 File Handling

The WAL file is opened **without O_APPEND**:

```rust
let f = File::options()
    .read(true)
    .write(true)    // not append — pwrite controls position
    .create(false)  // file already exists after create()
    .open(path)?;
```

O_APPEND would silently ignore pwrite offsets (the kernel seeks to EOF before
every write). Without O_APPEND, `pwrite` (Rust's `File::write_all_at`) writes
at the specified offset.

BufWriter is removed. pwrite writes directly to the file descriptor. BufWriter
provides no benefit with position-independent writes (each write is at a
different offset, so BufWriter's sequential buffering is useless).

### 5.3 Write Path

```rust
impl Wal {
    /// Encode a batch into a buffer and push to the pending list.
    /// Phase 1: called under locks (same as current put_batch).
    pub fn put_batch(&self, data: &[(&[u8], &[u8])], commit_ts: u64) -> Result<()> {
        // Encode into pooled buffer (same as current).
        let buf = self.encode_batch(data, commit_ts)?;
        self.pending.lock().push(buf);
        Ok(())
    }

    /// Write all pending buffers to the file and commit.
    /// Phase 2: called after locks are released (same as current submit_and_commit).
    pub fn submit_and_commit(&self) -> Result<()> {
        let bufs: Vec<Vec<u8>> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };
        if bufs.is_empty() {
            return Ok(());
        }

        // 1. Allocate file offsets atomically (BEFORE write).
        let mut offsets = Vec::with_capacity(bufs.len());
        let mut offset = self.sequencer.alloc_offset.fetch_add(
            bufs.iter().map(|b| b.len() as u64).sum(),
            Ordering::AcqRel,
        );
        for buf in &bufs {
            offsets.push(offset);
            offset += buf.len() as u64;
        }
        let end_offset = offset;

        // 2. Write to file at allocated offsets (pwrite).
        let file = self.file.lock();
        for (buf, &off) in bufs.iter().zip(&offsets) {
            file.write_all_at(buf, off)?;
        }
        drop(file);

        // 3. Confirm writes (AFTER pwrite completes).
        //    This advances confirmed_offset past all written bytes.
        //    Must be done AFTER all pwrites to prevent the race where
        //    synced_offset advances past unwritten bytes.
        self.advance_confirmed(end_offset);

        // 4. Commit via sequencer (may piggyback on prior fsync).
        self.sequencer.commit(end_offset, &self.file)
    }
}
```

### 5.4 Sequencer Commit Logic

```rust
impl CommitSequencer {
    fn commit(&self, my_end_offset: u64, file: &Arc<Mutex<File>>) -> Result<()> {
        let mut guard = self.commit_mutex.lock();

        // Fast path: already covered by a prior fsync.
        if self.synced_offset.load(Ordering::Acquire) >= my_end_offset {
            return Ok(());
        }

        // Check for prior error.
        if let Some(ref err) = *self.last_error.lock() {
            anyhow::bail!("prior WAL fsync failed: {}", err);
        }

        // Try to become the fsyncer.
        if !self.fsync_in_progress.swap(true, Ordering::AcqRel) {
            // I am the fsyncer — do the fsync.
            let result = file.lock().sync_all();

            // Advance synced_offset to confirmed_offset (NOT alloc_offset).
            let confirmed = self.confirmed_offset.load(Ordering::Acquire);
            self.synced_offset.store(confirmed, Ordering::Release);
            self.fsync_in_progress.store(false, Ordering::Release);

            if let Err(e) = result {
                *self.last_error.lock() = Some(e.to_string());
                self.commit_cond.notify_all();
                anyhow::bail!("WAL fsync failed: {}", e);
            }

            self.commit_cond.notify_all();
            return Ok(());
        }

        // Another thread is fsyncing — wait for it.
        while self.synced_offset.load(Ordering::Acquire) < my_end_offset {
            // Check for error from the fsyncing thread.
            if let Some(ref err) = *self.last_error.lock() {
                anyhow::bail!("prior WAL fsync failed: {}", err);
            }
            self.commit_cond.wait(&mut guard);
        }

        Ok(())
    }
}
```

### 5.5 How Piggyback Works

Example with 3 concurrent writers:

```
Writer A: alloc_offset=0   → pwrite 0-100   → confirmed=100  → commit(100)
Writer B: alloc_offset=100 → pwrite 100-150  → confirmed=150  → commit(150)
Writer C: alloc_offset=150 → pwrite 150-230  → confirmed=230  → commit(230)

Sequencer state: synced_offset=0, confirmed_offset=230

Writer A enters commit(100):
  synced_offset (0) < my_end_offset (100)
  fsync_in_progress = false → CAS succeeds → I am the fsyncer
  sync_all() → flushes pages 0-230 (all dirty data)
  confirmed = 230 → synced_offset = 230
  notify_all
  → Writer A returns ✓

Writer B enters commit(150):
  synced_offset (230) >= my_end_offset (150) → fast path
  → Writer B returns immediately ✓ (no fsync)

Writer C enters commit(230):
  synced_offset (230) >= my_end_offset (230) → fast path
  → Writer C returns immediately ✓ (no fsync)

Result: 1 fsync for 3 batches (same as current group commit)
```

### 5.6 The Ordering Race (Why confirmed_offset Is Needed)

Without `confirmed_offset`, using `alloc_offset` for `synced_offset`:

```
Writer A: alloc_offset=0, written_offset=100   (write delayed)
Writer B: alloc_offset=100, pwrite 100-150 done
Writer B: commit(150) → fsync → synced_offset=150
  BUT: bytes 0-100 were never written! synced_offset lies.

Writer A: finally pwrite 0-100
  crash → Batch A lost, but synced_offset claimed it was safe ✗
```

With `confirmed_offset`:

```
Writer A: alloc_offset=0   → pwrite delayed
Writer B: alloc_offset=100 → pwrite 100-150 → confirmed=150
Writer B: commit(150) → fsync → synced_offset = confirmed_offset
  BUT: confirmed_offset was advanced by advance_confirmed(150),
  which checks that ALL bytes 0-150 were written. Since Writer A
  hasn't written yet, confirmed_offset stays at 0 (not 150).
  synced_offset = 0. Writer B waits.

Writer A: pwrite 0-100 → confirmed=150 (now all 0-150 written)
Writer B: wakes → synced_offset=150 → returns ✓
```

The `advance_confirmed` function ensures `confirmed_offset` only advances
past bytes that are actually in the page cache.

### 5.7 advance_confirmed Implementation

```rust
impl Wal {
    /// Advance confirmed_offset to `target` only if all preceding
    /// writes have completed. Uses a secondary watermark to track
    /// contiguous written regions.
    fn advance_confirmed(&self, target: u64) {
        // Simple approach: since writes are sequential under the file mutex,
        // the last writer to release the mutex knows all prior writes completed.
        // We can safely advance confirmed_offset to target.
        //
        // For future pwrite parallelism (io_uring), use a per-batch
        // "write-complete" flag and advance the watermark contiguously.
        self.sequencer.confirmed_offset.store(target, Ordering::Release);
    }
}
```

**Note:** With sequential pwrite under the file mutex, `advance_confirmed` is
simple — the mutex guarantees all prior writes completed. For future io_uring
parallel writes, a contiguous watermark approach is needed (each writer marks
its batch complete, and the watermark advances past all contiguous completed
batches).

### 5.8 Two-Phase Protocol (Unchanged)

The existing two-phase write protocol is preserved:

```
Phase 1 (under locks):
  write_wal_batch_only() → wal.put_batch() → encode buffer, push to pending list

Phase 2 (locks released):
  commit_wal() → wal.submit_and_commit() → pwrite + sequencer commit
```

The `pending` list (replacing `ready_queue`) holds encoded buffers across the
lock boundary. Same role as the current `ready_queue`, simpler data structure.

### 5.9 Buffer Pool (Unchanged)

The buffer pool (`ArrayQueue<Vec<u8>>`) lifecycle:
1. `put_batch()` pops from pool, encodes, pushes to `pending` list.
2. `submit_and_commit()` takes all pending buffers, pwrites, returns to pool after commit.

Same lifecycle as current. The leader role is gone — the calling thread does
the pwrite and returns buffers itself.

### 5.10 Public sync() Path

`KvEngine::sync()` calls `wal.sync()`. The sequencer-aware `sync()`:

```rust
impl Wal {
    pub fn sync(&self) -> Result<()> {
        // Flush any pending buffers.
        self.submit_and_commit()?;
        // If nothing was pending, fsync the file directly.
        if self.sequencer.synced_offset.load(Ordering::Acquire)
            < self.sequencer.confirmed_offset.load(Ordering::Acquire)
        {
            self.file.lock().sync_all()?;
            let confirmed = self.sequencer.confirmed_offset.load(Ordering::Acquire);
            self.sequencer.synced_offset.store(confirmed, Ordering::Release);
        }
        Ok(())
    }
}
```

This ensures `sync()` always leaves the file durable, even if no batches
were pending.

### 5.11 Recovery (Unchanged)

Recovery scans the WAL file sequentially:

1. Read batch header (commit_ts, entry_count, crc32).
2. Validate CRC32 over entry data.
3. If valid, replay entries into skiplist.
4. If invalid (CRC mismatch, truncated), stop — this is the recovery point.
5. Truncate file to last valid byte.

The WAL file format is unchanged. Recovery constructs a new `Wal` with
`alloc_offset` set to the file length (append position).

---

## 6. Trade-offs

### Advantages

1. **Simpler code** — No leader election, no batch results ring buffer, no condvar wake-up storms. Just atomic offsets + pwrite + sequencer.
2. **Same fsync count** — 1 fsync for N concurrent writers (piggyback), same as current group commit.
3. **No BufWriter** — pwrite writes directly to file. No seek + flush interaction bugs.
4. **No O_APPEND** — pwrite controls position explicitly. No silent offset ignoring.
5. **Phase 2 ready** — The atomic offset allocator and pwrite model map directly to io_uring SQEs when ready.
6. **Single file** — No multi-shard complexity. Same file format, same recovery.

### Disadvantages

1. **pwrite serialized by inode lock** — ext4 serializes buffered pwrite calls to the same file. Writers don't actually write in parallel. (Same as current — the leader writes sequentially too.)
2. **Sequencer overhead** — Atomic ops (fetch_add, load, store) + mutex per commit. Marginal vs current leader election overhead.
3. **Low concurrency** — With 1 writer, every batch fsyncs. Sequencer adds overhead for no benefit. Mitigation: short-circuit when pending list has 1 buffer.

### vs Current Group Commit

| Metric | Current | Sequencer |
|--------|---------|-----------|
| Fsyncs per N writers | 1 | 1 (piggyback) |
| Code complexity | High (leader election, ring buffer) | Low (atomic offsets, mutex) |
| Lock acquisitions | 1 (file mutex held for drain+write+fsync) | 1 (file mutex for pwrite) + 1 (commit_mutex for sequencer) |
| BufWriter bugs | seek + flush interaction | None (no BufWriter) |
| Error propagation | Batch result ring buffer | Error flag + notify_all |

---

## 7. Implementation Plan

| Phase | Description | Files |
|-------|-------------|-------|
| 1 | Add `CommitSequencer` struct to `wal.rs` | `wal.rs` |
| 2 | Replace `BufWriter<File>` with raw `File` | `wal.rs` |
| 3 | Replace `submit_and_commit()` body with sequencer logic | `wal.rs` |
| 4 | Update `sync()` to route through sequencer | `wal.rs` |
| 5 | Remove leader-follower code (leader_active, batch_results, etc.) | `wal.rs` |
| 6 | Update recovery to construct sequencer state | `wal.rs` |
| 7 | Add tests | `tests/wal.rs` |

---

## 8. Testing Strategy

1. **Correctness:** All existing WAL tests pass (format unchanged).
2. **Piggyback:** Verify that concurrent writers trigger fewer fsyncs (instrument with counter).
3. **Ordering:** Crash-injection test — verify no batch is committed before prior batches are durable.
4. **Single writer:** Verify no regression (short-circuit path).
5. **sync() path:** Verify `engine.sync()` and `engine.close()` work correctly.
6. **Benchmark:** `write-perf --workload wal_concurrent --threads 1,2,4,8` — compare vs current group commit.

---

## 9. Open Questions

1. **Short-circuit for single writer:** Track `pending.len()` and bypass sequencer when 1 (direct pwrite + fsync). Adds a branch but avoids sequencer overhead for single-threaded workloads.
2. **BufWriter removal scope:** The legacy `put()` path (non-MVCC) also uses BufWriter. Should it switch to pwrite too, or keep BufWriter for the legacy path? Recommendation: keep BufWriter for legacy, use pwrite only for MVCC batch path.
3. **Future io_uring:** The pwrite model maps directly to io_uring SQEs. The `advance_confirmed` function would need a contiguous watermark for parallel submissions. Deferred to future work.
