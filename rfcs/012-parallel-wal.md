# RFC 012: Parallel WAL — io_uring + O_DIRECT

**Status:** Proposed
**Date:** 2026-06-26
**Author:** kv-engine Contributors
**Reference:** SpanDB (FAST 2021) — "SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage"

---

## 1. Summary

This RFC proposes replacing the current leader-follower group commit with
**io_uring + O_DIRECT** for parallel WAL writes, directly implementing the
SpanDB paper's design:

1. **Log batching** — multiple writers' entries collected into batches (existing).
2. **Parallel writers** — batches submitted as io_uring SQEs, dispatched to NVMe channels in parallel.
3. **Atomic page allocation** — each batch gets a unique, non-overlapping file offset.

O_DIRECT bypasses the page cache and the ext4 inode lock, enabling true
parallel writes to the same file. io_uring's `IOSQE_IO_DRAIN` ensures the
fsync waits for all prior writes, replacing the need for an explicit sequencer.

---

## 2. Motivation

### Current Bottleneck

The current WAL uses a single-leader group commit:

```
8 writers → leader drains → sequential BufWriter::write_all → flush → fsync → wake 8
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            Single thread, serialized, held under file mutex
```

The leader does all the work. The other 7 threads wait.

### What Changes

```
8 writers → encode buffers → submit 8 SQEs + 1 fsync(DRAIN) → io_uring_enter
            → kernel dispatches to NVMe channels in parallel
            → poll fsync CQE → all 8 batches durable → wake all
```

No leader election. No BufWriter. Just SQE submission + CQE polling. The
submitting thread blocks on `submit_and_wait` (equivalent to the current
leader's fsync wait), but the kernel parallelizes the underlying NVMe writes
during that wait.

### Why Not Buffered I/O?

Buffered writes to the same file are serialized by the ext4 inode lock
(`inode_lock` in `ext4_file_write_iter`). io_uring SQEs submit in parallel,
but the kernel processes them sequentially. No actual parallelism.

O_DIRECT bypasses the page cache and the inode lock. Writes go directly to
the NVMe device, which has internal parallelism (multiple channels). The
kernel dispatches SQEs to different NVMe submission queues concurrently.

---

## 3. Goals

1. Replace leader-follower group commit with io_uring + O_DIRECT.
2. Parallel write I/O to the NVMe device.
3. Single fsync per batch group via `IOSQE_IO_DRAIN`.
4. Maintain the two-phase write protocol (`write_wal_batch_only` → `commit_wal`).
5. Backward compatible WAL file format (v3, unchanged).
6. Recovery unchanged — sequential scan + CRC validation.

## 4. Non-Goals

1. Multiple WAL files / shards.
2. SPDK integration.
3. Changing the WAL file format.
4. io_uring for reads (separate concern).

---

## 5. Design

### 5.1 Overview

```
Phase 1 (under locks):
  write_wal_batch_only() → encode buffer → zero-pad to 4KB → push to pending list

Phase 2 (locks released):
  submit_and_commit():
    1. Drain pending list (one thread becomes submitter, others wait)
    2. Allocate offsets (atomic fetch_add, using aligned sizes)
    3. Submit write SQEs (one per buffer)
    4. Submit fsync SQE with IOSQE_IO_DRAIN
    5. io_uring_enter (one syscall for all SQEs)
    6. Poll fsync CQE (confirms all writes are durable)
    7. Return buffers to pool
    8. Signal waiting threads via condvar
```

### 5.2 O_DIRECT Buffer Requirements

O_DIRECT requires page-aligned buffers (typically 4KB aligned, 4KB multiple
sizes). The current buffer pool allocates `Vec<u8>` with 64KB capacity, which
is page-aligned on most systems but not guaranteed.

```rust
/// Allocate a page-aligned buffer for O_DIRECT I/O.
fn alloc_direct_buf(size: usize) -> Vec<u8> {
    let layout = Layout::from_size_align(size.max(4096), 4096).unwrap();
    let ptr = unsafe { std::alloc::alloc(layout) };
    unsafe { Vec::from_raw_parts(ptr, 0, size.max(4096)) }
}
```

Or use `libc::posix_memalign` for portability.

**Buffer pool change:** Replace `ArrayQueue<Vec<u8>>` with page-aligned
allocations. The pool size (16 buffers × 64KB) and lifecycle are unchanged.

### 5.3 File Handling

The WAL file requires **two handles**:

1. **Buffered handle** — for recovery reads and WAL header write at creation.
2. **O_DIRECT handle** — for batch writes via io_uring.

```rust
// Recovery: buffered handle (benefits from page cache).
let recovery_file = File::options()
    .read(true)
    .append(true)
    .open(path)?;

// Write path: O_DIRECT handle (bypasses page cache + inode lock).
let write_file = File::options()
    .read(true)
    .write(true)    // NOT append — pwrite controls position
    .custom_flags(libc::O_DIRECT)
    .open(path)?;
```

**WAL creation:** The 6-byte WAL header is written via a temporary buffered
handle (O_DIRECT requires ≥4KB writes). After the header is written and
flushed, the buffered handle is closed and the O_DIRECT handle is opened.

**Recovery:** Reads via the buffered handle. After recovery truncation,
`alloc_offset` is initialized to the file length (the byte after the last
valid batch). The buffered handle is dropped; the O_DIRECT handle is used
for subsequent writes.

### 5.4 io_uring Setup

```rust
use io_uring::{opcode, types, IoUring, Builder};

let mut builder = Builder::new(64);  // 64 SQE slots

// IORING_SETUP_SINGLE_ISSUER requires kernel 5.19+.
// Probe: try with flag, fall back without if it fails.
if kernel_version() >= (5, 19, 0) {
    builder.setup_single_issuer();
}

let ring = builder.build()?;
ring.submitter().register_files(&[raw_fd])?;  // register WAL file FD
```

The ring is created once at WAL open and destroyed at WAL close. It is used
exclusively for WAL writes — not shared with other subsystems.

**FD registration failure:** If `register_files` fails (EMFILE, ENOMEM), bail
with a clear error. The registered FD must remain valid for the ring's
lifetime. The `File` that owns the FD must not be dropped while the ring
is active.

### 5.5 Write + Commit Flow

```rust
impl Wal {
    pub fn submit_and_commit(&self) -> Result<()> {
        let bufs: Vec<Vec<u8>> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };
        if bufs.is_empty() {
            return Ok(());
        }

        // 1. Allocate file offsets atomically.
        //    CRITICAL: use aligned sizes, not raw buf.len().
        let total_size: u64 = bufs.iter()
            .map(|b| Self::align_up(b.len()) as u64)
            .sum();
        let base_offset = self.alloc_offset.fetch_add(total_size, Ordering::AcqRel);

        // 2. Submit write SQEs (parallel I/O to NVMe).
        let mut offset = base_offset;
        for buf in &bufs {
            let aligned_len = Self::align_up(buf.len());
            // SAFETY: buf is a page-aligned allocation from alloc_direct_buf.
            // The buffer remains valid until all CQEs are polled (bufs is a
            // local variable that outlives the CQE loop). The registered FD
            // (Fixed(0)) is valid for the ring's lifetime.
            let sqe = opcode::Write::new(
                types::Fixed(0),       // registered file FD
                buf.as_ptr(),
                aligned_len as u32,
            )
            .offset(offset)
            .build();
            unsafe { self.ring.submission().push(&sqe)?; }
            offset += aligned_len as u64;
        }

        // 3. Submit fsync with IOSQE_IO_DRAIN.
        //    DRAIN: "do not start this SQE until all prior SQEs complete."
        //    This ensures the fsync covers all preceding writes.
        //    On Linux, fdatasync flushes file size metadata, so it is safe
        //    for WAL files that extend (see fdatasync(2)).
        let fsync_sqe = opcode::Fsync::new(types::Fixed(0))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .flags(types::Flags::IO_DRAIN);
        unsafe { self.ring.submission().push(&fsync_sqe)?; }

        // 4. Submit all SQEs in one syscall.
        self.ring.submit_and_wait(bufs.len() + 1)?;

        // 5. Poll for all CQEs.
        for _ in 0..bufs.len() {
            let cqe = self.ring.completion().next()
                .ok_or_else(|| anyhow!("io_uring: missing write CQE"))?;
            if cqe.result() < 0 {
                anyhow::bail!("io_uring write error: {}", cqe.result());
            }
        }
        // Fsync CQE — confirms all data is durable.
        let fsync_cqe = self.ring.completion().next()
            .ok_or_else(|| anyhow!("io_uring: missing fsync CQE"))?;
        if fsync_cqe.result() < 0 {
            anyhow::bail!("io_uring fsync error: {}", fsync_cqe.result());
        }

        // 6. Return buffers to pool.
        //    Buffers MUST NOT be returned before CQE confirmation.
        //    This is safe because bufs is a local variable that outlives
        //    the CQE polling loop above.
        for buf in bufs {
            let _ = self.buf_pool.push(buf);
        }

        Ok(())
    }
}
```

### 5.6 Completion Barrier (Fate-Sharing)

Multiple threads call `submit_and_commit()` concurrently. Only one thread
drains `pending` and submits SQEs. The others must wait for the CQE result
before returning — otherwise they may call `publish_raw_batch()` before the
data is durable.

```rust
impl Wal {
    pub fn submit_and_commit(&self) -> Result<()> {
        let bufs = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };

        if bufs.is_empty() {
            // Our data was already taken by a prior submitter.
            // Wait for the completion signal.
            return self.wait_for_completion();
        }

        // I am the submitter — do the I/O.
        let result = self.submit_sqes_and_poll(bufs);

        // Signal all waiters (even on error).
        {
            let mut guard = self.completion_mutex.lock();
            self.last_completion_result.lock().replace(result.clone());
            self.completion_cond.notify_all();
        }

        result
    }

    fn wait_for_completion(&self) -> Result<()> {
        let mut guard = self.completion_mutex.lock();
        while self.last_completion_result.lock().is_none() {
            self.completion_cond.wait(&mut guard);
        }
        let result = self.last_completion_result.lock().take().unwrap();
        result
    }
}
```

This ensures all callers see the same result — success or failure. The
submitter broadcasts the CQE outcome to all waiters via condvar.

### 5.7 How DRAIN Replaces the Sequencer

The SpanDB paper's "atomic page allocation ordering" ensures batches are
committed in order. With io_uring, `IOSQE_IO_DRAIN` on the fsync SQE
achieves this:

```
SQE[0]: write batch A at offset 0     ─┐
SQE[1]: write batch B at offset 100   ─┤ parallel to NVMe
SQE[2]: write batch C at offset 250   ─┘
SQE[3]: fsync (DRAIN)                 ← waits for SQE[0], [1], [2] to complete
                                        then fsyncs → CQE confirms all durable
```

All callers wait for the fsync CQE (via the completion barrier). No caller
returns until all data is on disk.

### 5.8 Atomic Offset Allocator

```rust
struct OffsetAllocator {
    next_offset: AtomicU64,
}

impl OffsetAllocator {
    /// Allocate `size` bytes at the current end of the WAL.
    /// `size` must be 4KB-aligned (O_DIRECT requirement).
    fn alloc(&self, size: u64) -> u64 {
        self.next_offset.fetch_add(size, Ordering::AcqRel)
    }
}
```

**Initialization after recovery:** `next_offset` is set to the WAL file length
after recovery truncation. The `recover_mvcc()` function already computes
`valid_file` (wal.rs:443) — this value is passed through to the allocator.

### 5.9 Alignment Gap Handling

O_DIRECT requires 4KB-aligned write sizes. Each batch buffer is zero-padded
to the aligned length after encoding:

```rust
fn encode_batch(&self, data: &[(&[u8], &[u8])], commit_ts: u64) -> Result<Vec<u8>> {
    // ... encode batch data into buf ...

    // Zero-pad to 4KB alignment (O_DIRECT requirement).
    // This also ensures recovery doesn't read stale bytes as batch data.
    let aligned_len = Self::align_up(buf.len());
    buf.resize(aligned_len, 0);

    Ok(buf)
}
```

**Why zero-pad?** Without padding, the alignment gap contains stale data from
previous buffer reuse. The recovery scanner would read this garbage as batch
headers, fail CRC validation, and truncate the WAL — losing all batches
after the first gap.

**Recovery handling of zero-padded regions:** The recovery scanner already
handles this correctly. A zero-padded region reads as `entry_count=0` with
`data_crc32=0`. The CRC of zero bytes is 0, so the batch is valid (it's an
empty batch). Recovery continues past it to the next batch. The `entry_count`
field is the authoritative batch size indicator — padding doesn't affect it.

### 5.10 Record Checksum (Unchanged)

Each batch has a CRC32 checksum over the entry data (existing `BATCH_HEADER_SIZE`
= 16 bytes: commit_ts + entry_count + data_crc32). O_DIRECT does not affect
the checksum — it covers only the valid bytes (before zero-padding).

### 5.11 Recovery Scan (Unchanged)

Recovery reads the WAL file sequentially using the buffered file handle,
validates CRC per batch, and stops at the first invalid entry. The WAL file
format is unchanged.

The buffered handle is opened with `.append(true)` for recovery. After
recovery truncation, `alloc_offset` is initialized to the file length. The
buffered handle is dropped; the O_DIRECT handle is used for writes.

### 5.12 Two-Phase Protocol (Unchanged)

```
Phase 1 (under locks):
  write_wal_batch_only() → encode into page-aligned buffer → zero-pad → push to pending list

Phase 2 (locks released):
  submit_and_commit() → drain pending → allocate offsets → submit SQEs → poll CQE
```

The `pending` list (replacing `ready_queue`) holds encoded buffers across the
lock boundary. Same role as the current `ready_queue`.

### 5.13 Buffer Pool (Modified)

The buffer pool allocates page-aligned buffers for O_DIRECT:

```rust
fn new_buf_pool() -> ArrayQueue<Vec<u8>> {
    let pool = ArrayQueue::new(BUFFER_POOL_CAPACITY);
    for _ in 0..BUFFER_POOL_CAPACITY {
        let _ = pool.push(alloc_direct_buf(BUFFER_POOL_BUF_SIZE));
    }
    pool
}
```

Buffer lifecycle is unchanged: pop → encode → zero-pad → push to pending →
drain → submit SQE → poll CQE → return to pool.

### 5.14 Legacy put() Path

The legacy `put()` method (non-MVCC format) writes directly to a BufWriter.
O_DIRECT is incompatible with this path (non-aligned writes, arbitrary sizes).

**Solution:** The `Wal` struct retains both handles:
- `direct_file: File` — O_DIRECT handle for MVCC batch writes via io_uring.
- `buf_writer: BufWriter<File>` — buffered handle for legacy `put()` and WAL header writes.

The legacy path is routed to `buf_writer` when `!self.mvcc_format`. MVCC
batch writes use `direct_file` via io_uring. Both handles point to the same
inode — this is safe on Linux.

### 5.15 Public sync() Path

```rust
impl Wal {
    pub fn sync(&self) -> Result<()> {
        self.submit_and_commit()
    }
}
```

`sync()` is equivalent to `submit_and_commit()`. If there are pending buffers,
they are drained and committed. If not, the completion barrier waits for any
in-flight commit from another thread.

### 5.16 Engine Close

```rust
impl Wal {
    pub fn close(&self) -> Result<()> {
        // 1. Drain any pending buffers.
        self.submit_and_commit()?;

        // 2. Drain all remaining CQEs from the ring.
        while let Some(cqe) = self.ring.completion().next() {
            if cqe.result() < 0 {
                log::error!("io_uring: stale CQE with error: {}", cqe.result());
            }
        }

        // 3. The ring is dropped when Wal is dropped.
        Ok(())
    }
}
```

Called from `KvEngine::close()` before `sync_dir()`. Ensures no in-flight
SQEs are lost.

### 5.17 Fallback

On systems without io_uring support (kernel < 5.11, non-Linux), fall back to
the current leader-follower group commit:

```rust
#[cfg(target_os = "linux")]
mod io_uring_wal { ... }

#[cfg(not(target_os = "linux"))]
mod io_uring_wal {
    // Re-export current group commit as fallback.
}
```

On Linux, attempt `IoUring::new()` at WAL open. If it fails (kernel too old),
fall back to the synchronous path. Store `use_io_uring: bool` in the `Wal`
struct. Feature detection is runtime, not compile-time — a Linux binary works
on kernels 5.11+ (io_uring) and <5.11 (fallback).

---

## 6. Trade-offs

### Advantages

1. **True parallel I/O** — O_DIRECT bypasses inode lock; NVMe channels process writes concurrently.
2. **Single syscall** — `io_uring_enter` submits all SQEs (vs BufWriter's ~66 syscalls for 8 × 64KB buffers).
3. **No leader election** — Completion barrier replaces leader_active CAS, batch_results ring, condvar wake-up storms.
4. **No BufWriter** — O_DIRECT writes directly to device. No userspace buffering bugs.
5. **No sequencer** — IOSQE_IO_DRAIN handles ordering. CQE polling handles commit barrier.
6. **Faithful to paper** — Directly implements SpanDB's parallel WAL design with kernel I/O instead of SPDK.

### Disadvantages

1. **Linux-only, kernel 5.11+** — Fallback needed for other platforms/kernels.
2. **O_DIRECT constraints** — Page-aligned buffers, page-aligned offsets, zero-padding.
3. **Complexity** — io_uring SQE/CQE model, buffer lifetime management, dual file handles.
4. **No page cache** — Recovery reads must use a separate buffered file handle.
5. **Debugging** — Async I/O errors are harder to diagnose.

### Expected Performance

| Metric | Current (group commit) | io_uring + O_DIRECT |
|--------|----------------------|-------------------|
| Write parallelism | None (single leader) | Full (NVMe channels) |
| Syscalls per batch group | ~66 (BufWriter flushes + fsync) | 1 (io_uring_enter) |
| Fsync count | 1 per group | 1 per group (DRAIN) |
| Expected throughput gain | Baseline | 1.2-2× consumer NVMe, 2-3× enterprise NVMe |

The gain depends on NVMe internal parallelism. Consumer NVMe SSDs have 1-2
channels; enterprise drives have 4-8+. The primary source of throughput gain
is write-phase parallelism (kernel dispatches to NVMe channels), not syscall
reduction (fsync dominates total latency).

Note: Per-operation io_uring is slower than std::fs (see `docs/io-uring-bench.md`).
The gain comes entirely from batching — amortizing `io_uring_enter` overhead
across N SQEs.

---

## 7. Implementation Plan

| Phase | Description | Files |
|-------|-------------|-------|
| 1 | Add `io-uring` dependency (feature-gated) | `Cargo.toml` |
| 2 | Add page-aligned buffer allocator + zero-padding | `wal.rs` |
| 3 | Add dual file handles (buffered + O_DIRECT) | `wal.rs` |
| 4 | Implement io_uring write path with completion barrier | `wal.rs` |
| 5 | Add legacy `put()` fallback to BufWriter | `wal.rs` |
| 6 | Update recovery to use buffered handle | `wal.rs` |
| 7 | Add `Wal::close()` for ring cleanup | `wal.rs` |
| 8 | Add fallback to current group commit | `wal.rs` |
| 9 | Add tests | `tests/wal.rs` |
| 10 | Benchmark | — |

---

## 8. Testing Strategy

1. **Correctness:** All existing WAL tests pass (format unchanged).
2. **io_uring path:** Write, read back, verify. Concurrent writers. Recovery after crash.
3. **Fallback path:** Force fallback, verify same correctness.
4. **O_DIRECT alignment:** Test with various batch sizes (4KB, 8KB, 12KB — verify zero-padding).
5. **Legacy put():** Test non-MVCC WAL writes still work via BufWriter path.
6. **Engine close:** Verify all in-flight SQEs are drained before return.
7. **Benchmark:** `write-perf --workload wal_concurrent --threads 1,2,4,8`.
8. **Stress test:** 16+ concurrent writers with crash injection.

---

## 9. Open Questions

1. **Ring size:** 64 SQE slots. If >64 batches accumulate, submit in chunks (submit 64, wait for CQEs, submit next 64).
2. **Poll mode:** `IORING_SETUP_SQPOLL` for kernel-side polling. Trade-off: lower latency vs higher CPU. Recommendation: start without, add as optimization.
3. **Buffer pool sizing:** 16 buffers × 64KB = 1MB. Is 16 enough for high concurrency? Recommendation: benchmark with 16, 32, 64.
