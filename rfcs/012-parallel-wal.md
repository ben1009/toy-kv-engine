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

No leader. No mutex (for I/O). No condvar. Just SQE submission + CQE polling.

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
  write_wal_batch_only() → encode buffer → push to pending list

Phase 2 (locks released):
  submit_and_commit():
    1. Drain pending list
    2. Allocate offsets (atomic fetch_add)
    3. Submit write SQEs (one per buffer)
    4. Submit fsync SQE with IOSQE_IO_DRAIN
    5. io_uring_enter (one syscall for all SQEs)
    6. Poll fsync CQE (confirms all writes are durable)
    7. Return buffers to pool
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

Or use the `aligned_alloc` / `posix_memalign` APIs via the `libc` crate.

**Buffer pool change:** Replace `ArrayQueue<Vec<u8>>` with page-aligned
allocations. The pool size (16 buffers × 64KB) and lifecycle are unchanged.

### 5.3 File Handling

```rust
let f = File::options()
    .read(true)
    .write(true)
    .custom_flags(libc::O_DIRECT)  // bypass page cache + inode lock
    .open(path)?;
```

O_DIRECT requirements:
- Buffer address must be page-aligned (4KB).
- Buffer size must be a multiple of the logical block size (typically 512B or 4KB).
- File offset must be page-aligned.

All WAL batches are encoded into page-aligned buffers with sizes rounded up
to 4KB. The actual data length is recorded in the batch header (CRC covers
only the valid bytes).

### 5.4 io_uring Setup

```rust
use io_uring::{opcode, types, IoUring, Builder};

let ring = Builder::new(64)          // 64 SQE slots
    .setup_single_issuer()           // single-threaded submission (kernel optimization)
    .build()?;
ring.submitter().register_files(&[file_fd])?;  // register WAL file FD
```

The ring is created once at WAL open and destroyed at WAL close. It is used
exclusively for WAL writes — not shared with other subsystems.

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
        let total_size: u64 = bufs.iter().map(|b| b.len() as u64).sum();
        let base_offset = self.alloc_offset.fetch_add(total_size, Ordering::AcqRel);

        // 2. Submit write SQEs (parallel I/O to NVMe).
        let mut offset = base_offset;
        for buf in &bufs {
            let aligned_len = Self::align_up(buf.len());
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
        let fsync_sqe = opcode::Fsync::new(types::Fixed(0))
            .flags(types::FsyncFlags::FDATASYNC)  // fdatasync is sufficient for WAL
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
        for buf in bufs {
            let _ = self.buf_pool.push(buf);
        }

        Ok(())
    }
}
```

### 5.6 How DRAIN Replaces the Sequencer

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

All callers wait for the fsync CQE. No caller returns until all data is on
disk. This is the same guarantee as the sequencer, but implemented by the
kernel instead of userspace code.

### 5.7 Atomic Offset Allocator

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

Prevents overlapping writes. The allocator is updated before SQE submission.
The offset is encoded in each SQE's `offset` field.

### 5.8 Record Checksum (Unchanged)

Each batch has a CRC32 checksum over the entry data (existing `BATCH_HEADER_SIZE`
= 16 bytes: commit_ts + entry_count + data_crc32). O_DIRECT does not affect
the checksum — it covers the same bytes.

### 5.9 Recovery Scan (Unchanged)

Recovery reads the WAL file sequentially (using buffered I/O — O_DIRECT is
not needed for recovery), validates CRC per batch, and stops at the first
invalid entry. The WAL file format is unchanged.

**Note:** Recovery uses a separate `File` handle opened without O_DIRECT,
since recovery reads are sequential and benefit from the page cache.

### 5.10 Two-Phase Protocol (Unchanged)

```
Phase 1 (under locks):
  write_wal_batch_only() → encode into page-aligned buffer → push to pending list

Phase 2 (locks released):
  submit_and_commit() → drain pending → allocate offsets → submit SQEs → poll CQE
```

The `pending` list (replacing `ready_queue`) holds encoded buffers across the
lock boundary. Same role as the current `ready_queue`.

### 5.11 Buffer Pool (Modified)

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

Buffer lifecycle is unchanged: pop → encode → push to pending → drain →
pwrite via io_uring → return to pool after CQE.

### 5.12 Public sync() Path

```rust
impl Wal {
    pub fn sync(&self) -> Result<()> {
        // Flush any pending buffers via io_uring.
        self.submit_and_commit()?;
        Ok(())
    }
}
```

Since every `submit_and_commit()` includes a fsync (via DRAIN), `sync()` is
just a thin wrapper. If nothing was pending, it's a no-op (the last
`submit_and_commit()` already fsynced).

### 5.13 Fallback

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
fall back to the synchronous path. Store `use_io_uring: bool` in the `Wal` struct.

---

## 6. Trade-offs

### Advantages

1. **True parallel I/O** — O_DIRECT bypasses inode lock; NVMe channels process writes concurrently.
2. **Single syscall** — `io_uring_enter` submits all SQEs (vs N `write_all` calls).
3. **No leader election** — No leader_active CAS, no batch_results ring buffer, no condvar.
4. **No BufWriter** — O_DIRECT writes directly to device. No userspace buffering bugs.
5. **No sequencer** — IOSQE_IO_DRAIN handles ordering. Fsync CQE polling handles commit barrier.
6. **Faithful to paper** — Directly implements SpanDB's parallel WAL design with kernel I/O instead of SPDK.

### Disadvantages

1. **Linux-only, kernel 5.11+** — Fallback needed for other platforms/kernels.
2. **O_DIRECT constraints** — Page-aligned buffers, page-aligned offsets, no partial writes.
3. **Complexity** — io_uring SQE/CQE model, buffer lifetime management.
4. **No page cache** — Recovery reads must use a separate buffered file handle.
5. **Debugging** — Async I/O errors are harder to diagnose.

### Expected Performance

Based on the SpanDB paper and io_uring benchmarks:

| Metric | Current (group commit) | io_uring + O_DIRECT |
|--------|----------------------|-------------------|
| Write parallelism | None (single leader) | Full (NVMe channels) |
| Syscalls per batch group | N write + 1 flush + 1 fsync | 1 io_uring_enter |
| Fsync count | 1 per group | 1 per group (DRAIN) |
| Expected throughput gain | Baseline | 1.5-3× on NVMe |

The gain depends on NVMe internal parallelism. Single-channel SSDs see less
benefit; multi-channel enterprise SSDs see more.

---

## 7. Implementation Plan

| Phase | Description | Files |
|-------|-------------|-------|
| 1 | Add `io-uring` dependency (feature-gated) | `Cargo.toml` |
| 2 | Add page-aligned buffer allocator | `wal.rs` |
| 3 | Implement io_uring write path in `submit_and_commit()` | `wal.rs` |
| 4 | Add fallback to current group commit | `wal.rs` |
| 5 | Update recovery to use buffered file handle | `wal.rs` |
| 6 | Update `sync()` path | `wal.rs` |
| 7 | Add tests | `tests/wal.rs` |
| 8 | Benchmark | — |

---

## 8. Testing Strategy

1. **Correctness:** All existing WAL tests pass (format unchanged).
2. **io_uring path:** Write, read back, verify. Concurrent writers. Recovery after crash.
3. **Fallback path:** Force fallback, verify same correctness.
4. **O_DIRECT alignment:** Test with various batch sizes (4KB, 8KB, 12KB — verify padding).
5. **Benchmark:** `write-perf --workload wal_concurrent --threads 1,2,4,8`.
6. **Stress test:** 16+ concurrent writers with crash injection.

---

## 9. Open Questions

1. **Ring size:** 64 SQE slots. Is this enough? If >64 batches accumulate, submit in chunks.
2. **fdatasync vs fsync:** `fdatasync` skips metadata sync (faster). Recommendation: use `fdatasync` for WAL.
3. **Poll mode:** `IORING_SETUP_SQPOLL` for kernel-side polling. Trade-off: lower latency vs higher CPU. Recommendation: start without, add as optimization.
4. **Single issuer:** `IORING_SETUP_SINGLE_ISSUER` (kernel 5.19+) optimizes for single-threaded submission. Recommendation: enable if kernel supports.
5. **Buffer pool sizing:** 16 buffers × 64KB = 1MB. With O_DIRECT, each buffer is page-aligned. Is 16 enough for high concurrency? Recommendation: benchmark with 16, 32, 64.
