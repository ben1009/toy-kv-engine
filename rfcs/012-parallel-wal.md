# RFC 012: WAL Write Throughput — Parallel I/O via io_uring

**Status:** Proposed
**Date:** 2026-06-26
**Author:** kv-engine Contributors
**Reference:** SpanDB (FAST 2021) — "SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"

---

## 1. Summary

This RFC proposes improving WAL write throughput by replacing the current
synchronous leader-follower group commit with **io_uring-based parallel I/O**,
inspired by the SpanDB paper's multi-logger design.

The key insight: the current group commit already batches concurrent writers
efficiently (1 fsync for N writers). The bottleneck is not fsync count, but
**single-threaded I/O** — one leader writes all buffers sequentially. With
io_uring, multiple writers submit I/O in parallel, and the kernel dispatches
to NVMe channels concurrently.

The design:

1. **Atomic offset allocator** — each batch gets a sequential file offset.
2. **io_uring submission** — writers submit write SQEs at their allocated offsets.
3. **Record checksum** — CRC32 per batch (unchanged).
4. **Ordered commit** — writers poll for completion; fsync chained via `IOSQE_IO_LINK`.
5. **Recovery scan** — sequential scan, validate CRC (unchanged).

---

## 2. Motivation

### Current Design Is Already Good

The current group commit (`wal.rs:878-977`) is well-designed:

```
8 concurrent writers → leader drains all 8 → 1 sequential write → 1 fsync → wake all
```

This already achieves **1 fsync per N concurrent writers**. Under burst
concurrency (the common case), the current design is efficient.

### Where It Falls Short

The bottleneck is the **single leader thread** doing all the work:

```
Leader: drain queue → seek → write_all(buf[0]) → write_all(buf[1]) → ... → flush → fsync
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        All sequential, single thread, single file descriptor
```

With 8 writers each producing 4KB batches:
- **Current:** 1 thread writes 32KB sequentially + 1 fsync ≈ 20µs + 50µs = 70µs
- **io_uring:** 8 threads submit 8 SQEs + kernel writes in parallel to NVMe + 1 fsync ≈ 5µs + 50µs = 55µs

The savings come from parallelizing the write I/O, not from reducing fsync count.

### What About a Sequencer (Piggyback Fsync)?

An earlier version of this RFC proposed replacing the leader-follower model with
an "ordered commit sequencer" that piggybacks fsyncs. After review, this was
found to be **not an improvement** over the current design:

| Scenario | Current (group commit) | Sequencer |
|----------|----------------------|-----------|
| 8 writers arrive together | 1 leader drains all 8 → 1 fsync | 1 thread fsyncs, 7 piggyback → 1 fsync |
| 1 writer arrives alone | 1 fsync | 1 fsync + sequencer overhead |
| Writers arrive during fsync | New group forms → next leader does 1 fsync | Piggyback if offset already covered |

**The fsync count is the same.** The sequencer adds complexity (atomic offset
tracking, commit_mutex, condvar) without reducing fsyncs. The current leader
election is simpler and equally efficient for batching.

The sequencer also introduced critical correctness issues:
- `written_offset` (allocated) ≠ "bytes in page cache" — data loss if fsync
  claims bytes that were only reserved, not written.
- BufWriter + seek defeats buffering (each seek flushes the internal buffer).
- Missing `flush()` before `sync_all()` leaves data in userspace buffer.

**Conclusion:** The sequencer is not the right optimization. The real win is
io_uring, which parallelizes the I/O itself.

---

## 3. Goals

1. Replace single-leader writes with parallel io_uring submissions.
2. Maintain the existing WAL file format (v3, unchanged).
3. Maintain the two-phase write protocol (`write_wal_batch_only` → `commit_wal` → `publish_raw_batch`).
4. Recovery unchanged — sequential scan + CRC validation.
5. Linux-only (gated by `cfg(target_os = "linux")`), with fallback to current design.

## 4. Non-Goals

1. SPDK integration.
2. Multiple WAL files / shards.
3. Changing the WAL file format.
4. io_uring for reads (separate concern).

---

## 5. Design

### 5.1 Overview

```
Current:
  Writers → ready_queue → leader → BufWriter → write_all → flush → fsync → wake

Proposed:
  Writers → ready_queue → drain → allocate offsets → submit SQEs → io_uring_enter
            → poll CQE completions → wake
```

The key change: instead of one leader writing all buffers sequentially, the
drained buffers are submitted as parallel io_uring SQEs.

### 5.2 io_uring Setup

```rust
use io_uring::{opcode, types, IoUring};

struct IoUringWal {
    ring: IoUring,
    file_fd: types::Fixed,
    // ... other fields
}
```

- Create an `IoUring` instance with a shared submission/completion queue.
- Register the WAL file descriptor with `ring.submitter().register_files()`.
- Use a fixed number of SQE slots (e.g., 64) to bound memory usage.

### 5.3 Write Path

```rust
fn write_and_commit(&self, bufs: &[Vec<u8>]) -> Result<()> {
    let mut offset = self.alloc_offset(bufs);  // atomic fetch_add for total size

    // Submit one SQE per buffer (parallel I/O).
    for buf in bufs {
        let write_e = opcode::Write::new(self.file_fd, buf.as_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(offset as u64);  // tag with offset for completion tracking

        unsafe { self.ring.submission().push(&write_e)?; }
        offset += buf.len() as u64;
    }

    // Chain fsync after the last write.
    let fsync_e = opcode::Fsync::new(self.file_fd)
        .build()
        .user_data(FSYNC_TOKEN);

    unsafe { self.ring.submission().push(&fsync_e)?; }

    // Submit all SQEs in one syscall.
    self.ring.submit_and_wait(bufs.len() + 1)?;

    // Poll for completions.
    for _ in 0..bufs.len() + 1 {
        let cqe = self.ring.completion().next()
            .ok_or_else(|| anyhow!("io_uring: missing CQE"))?;
        if cqe.result() < 0 {
            anyhow::bail!("io_uring I/O error: {}", cqe.result());
        }
    }

    Ok(())
}
```

### 5.4 Atomic Offset Allocator

```rust
struct OffsetAllocator {
    next_offset: AtomicU64,
}

impl OffsetAllocator {
    fn alloc(&self, size: u64) -> u64 {
        self.next_offset.fetch_add(size, Ordering::AcqRel)
    }
}
```

Each batch gets a unique, sequential file region. The allocator is updated
**before** submission, but the I/O is submitted in parallel. The completion
poll ensures all data is on disk before returning.

**Key difference from the sequencer design:** The offset allocator only tracks
"where to write." The actual durability is confirmed by CQE completion, not by
a `synced_offset` atomic. This avoids the data-loss bug in the sequencer design.

### 5.5 Ordered Commit

Ordering is maintained by the file offset allocation:
- Buffers are drained from `ready_queue` in FIFO order (same as current).
- Each buffer gets a sequential offset (atomic fetch_add).
- io_uring writes them in parallel, but the offsets guarantee non-overlapping regions.
- Fsync is chained after the last write, so all data is durable when the CQE arrives.

### 5.6 Two-Phase Protocol (Unchanged)

The existing two-phase write protocol is preserved:

```
Phase 1 (under locks):
  write_wal_batch_only() → encode buffer, push to ready_queue

Phase 2 (locks released):
  commit_wal() → drain ready_queue → submit io_uring SQEs → poll completions → wake
```

The ready_queue survives as the buffer staging area between lock release and
io_uring submission. This is the same role it plays today — the only change is
how the leader writes the drained buffers.

### 5.7 Buffer Pool (Unchanged)

The buffer pool (`ArrayQueue<Vec<u8>>`) lifecycle is unchanged:
1. `put_batch()` pops from pool, encodes, pushes to ready_queue.
2. After io_uring completions are polled, buffers are returned to the pool.

### 5.8 Recovery (Unchanged)

Recovery scans the WAL file sequentially, validates CRC per batch, and stops
at the first invalid entry. The WAL file format is unchanged — io_uring writes
the same bytes to the same offsets as the current `BufWriter::write_all`.

### 5.9 Fallback

On non-Linux platforms (or kernels < 5.6), fall back to the current
leader-follower group commit. Gated by:

```rust
#[cfg(target_os = "linux")]
mod io_uring_wal { ... }

#[cfg(not(target_os = "linux"))]
mod io_uring_wal {
    // Re-export current group commit as fallback.
}
```

---

## 6. Trade-offs

### Advantages

1. **Parallel I/O** — Multiple buffers written simultaneously to NVMe channels.
2. **Reduced syscall overhead** — Single `io_uring_enter` for N submissions vs N `write_all` syscalls.
3. **Near-SPDK latency** — io_uring poll mode eliminates kernel transition overhead.
4. **No correctness risks** — CQE completion confirms durability. No `synced_offset` race.
5. **Minimal code change** — Only the write+commit path changes. Recovery, buffer pool, two-phase protocol all unchanged.

### Disadvantages

1. **Linux-only** — Requires kernel 5.6+. Fallback needed for other platforms.
2. **Complexity** — io_uring's SQE/CQE model is more complex than `write_all` + `fsync`.
3. **Debugging** — Async I/O errors are harder to diagnose than synchronous ones.
4. **Marginal gain for small writes** — The overhead of io_uring setup may exceed savings for very small batches.

### When It Helps Most

- Fast NVMe SSDs with internal parallelism.
- High concurrency (many writer threads).
- Moderate to large batch sizes (4KB+).

### When It Helps Least

- SATA SSDs or HDDs (I/O latency dominates).
- Single writer (no parallelism to exploit).
- Very small batches (io_uring overhead > savings).

---

## 7. Implementation Plan

| Phase | Description | Files |
|-------|-------------|-------|
| 1 | Add `io-uring` dependency (feature-gated) | `Cargo.toml` |
| 2 | Implement `IoUringWal` write path | `wal.rs` |
| 3 | Integrate with existing `submit_and_commit()` | `wal.rs` |
| 4 | Add fallback to current group commit | `wal.rs` |
| 5 | Benchmark: `write-perf --workload wal_concurrent` | — |
| 6 | Add tests | `tests/wal.rs` |

---

## 8. Testing Strategy

1. **Correctness:** All existing WAL tests pass (format unchanged).
2. **io_uring path:** Test with `cfg(target_os = "linux")` — write, read back, verify.
3. **Fallback path:** Test with forced fallback — same correctness guarantees.
4. **Benchmark:** `write-perf --workload wal_concurrent --threads 1,2,4,8` — compare io_uring vs fallback.
5. **Stress test:** High concurrency (16+ threads) with crash injection.

---

## 9. Open Questions

1. **io_uring ring size:** How many SQE slots? Recommendation: 64 (matches typical max concurrent batches).
2. **Poll mode:** Should we use `IORING_SETUP_SQPOLL` for kernel-side polling? Trade-off: lower latency vs higher CPU usage. Recommendation: start without poll mode, add as optimization.
3. **Fixed buffers:** Should we register fixed buffers (`IORING_REGISTER_BUFFERS`) to avoid per-SQE buffer setup? Recommendation: not in initial implementation — the variable-sized WAL batches make fixed buffers awkward.
4. **Kernel version:** Minimum 5.6 for basic io_uring. 5.11+ for `IORING_OP_FSYNC`. 5.19+ for `IORING_SETUP_SINGLE_ISSUER` (optimization). Recommendation: target 5.11+.
