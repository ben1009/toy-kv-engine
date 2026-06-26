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

```text
8 writers → leader drains → sequential BufWriter::write_all → flush → fsync → wake 8
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            Single thread, serialized, held under file mutex
```

The leader does all the work. The other 7 threads wait.

### What Changes

```text
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
5. Backward compatible WAL file format — v3 files still readable; new v4
   format adds `data_len` field for O_DIRECT alignment-gap skipping.
6. Recovery compatible — sequential scan + CRC validation, with alignment-gap
   skipping for v4 O_DIRECT padded batches.

## 4. Non-Goals

1. Multiple WAL files / shards.
2. SPDK integration.
3. io_uring for reads (separate concern).

**Note:** The WAL file format gains a 4-byte `data_len` field in the batch
header (§5.10), introduced as `WAL_FORMAT_VERSION_V4`. Old v3 files (16-byte
headers) are still readable. New batches are written with v4 headers. The
version is stored in the WAL file header and detected by recovery.

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
/// A page-aligned buffer for O_DIRECT I/O.
/// Vec<u8> cannot be used because its global allocator deallocates with
/// alignment 1, but O_DIRECT requires 4096-byte alignment. Dropping a
/// Vec backed by a 4096-aligned allocation is undefined behavior.
struct DirectBuf {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

// SAFETY: DirectBuf owns its allocation and is not shared across threads
// without synchronization. The raw pointer is not aliased.
// Send is required for transfer via ArrayQueue. Sync is NOT implemented
// because DirectBuf provides &mut access (as_mut_slice) — sharing a
// &DirectBuf across threads without synchronization would be unsound.
unsafe impl Send for DirectBuf {}

impl DirectBuf {
    fn new(size: usize) -> Self {
        // Cap must be 4KB-aligned to prevent io_uring out-of-bounds reads.
        // align_up() rounds write sizes to 4KB; if cap is smaller, io_uring
        // reads past the allocation.
        let cap = Self::align_up(size.max(4096));
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let ret = unsafe { libc::posix_memalign(&mut ptr, 4096, cap) };
        assert_eq!(ret, 0, "posix_memalign failed");
        // Zero-initialize immediately: creating a &mut [u8] over
        // uninitialized memory is UB in Rust. This also prevents stale
        // data leakage if the buffer is read before being fully written.
        unsafe { libc::memset(ptr, 0, cap) };
        Self {
            ptr: ptr as *mut u8,
            len: 0,
            cap,
        }
    }

    fn align_up(size: usize) -> usize {
        (size + 4095) & !4095
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
    }

    fn as_ptr(&self) -> *const u8 { self.ptr }
    fn len(&self) -> usize { self.len }
    fn set_len(&mut self, len: usize) { self.len = len; }
}

impl Drop for DirectBuf {
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr as *mut libc::c_void) };
    }
}
```

**Buffer pool change:** Replace `ArrayQueue<Vec<u8>>` with `ArrayQueue<DirectBuf>`.
The pool size (16 buffers × 64KB) and lifecycle are unchanged.

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
handle (O_DIRECT requires ≥4KB writes). The header is padded to 4096 bytes
with zeros so that `alloc_offset` starts at a 4KB boundary — O_DIRECT writes
at unaligned file offsets fail with `EINVAL`. After the padded header is
written and flushed, the buffered handle is closed and the O_DIRECT handle is
opened.

**v3 WAL handling:** Existing v3 WALs continue writing v3-format batches
(16-byte headers, no alignment padding) for the lifetime of that WAL file.
Only newly created WAL files use v4 format. This avoids mixing v3 and v4
records in the same file, which would break recovery (the file header declares
one version but the body contains both). WAL rotation creates a new v4 file
when the current v3 file is flushed and archived.

**Recovery:** Reads via the buffered handle. After recovery truncation,
`alloc_offset` is initialized to the file length (the byte after the last
valid batch). The buffered handle is dropped; the O_DIRECT handle is used
for subsequent writes.

### 5.4 io_uring Setup

```rust
use io_uring::{opcode, types, IoUring, Builder};

/// Fixed-file index for the WAL fd. Matches the single-element slice
/// passed to register_files(&[raw_fd]). If additional FDs are registered
/// later, this constant must stay in sync.
const WAL_FD_INDEX: u32 = 0;

let mut builder = Builder::new(64);  // 64 SQE slots

// NOTE: IORING_SETUP_SINGLE_ISSUER is NOT used here.
// Multiple threads call submit_and_commit() and any of them may become
// the submitter (whichever drains pending first). SINGLE_ISSUER requires
// that only the creating task submits SQEs — violating this returns -EEXIST.

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
        let bufs: Vec<DirectBuf> = {
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

        // 2. Chunked submission: if the batch exceeds ring capacity (64 SQEs),
        //    submit in chunks of 63 writes + 1 fsync, waiting for completions
        //    between chunks. The entire set of drained buffers is a single
        //    logical commit group — the generation counter is only incremented
        //    once after ALL chunks complete. This prevents callers in later
        //    chunks from returning prematurely before their data is durable.
        //
        //    user_data uses global indices (not chunk-relative) so that stale
        //    CQEs from a failed chunk can be detected and discarded.
        let max_writes_per_chunk = 63;  // leave 1 slot for fsync
        let chunks: Vec<&[DirectBuf]> = bufs.chunks(max_writes_per_chunk).collect();
        let mut offset = base_offset;
        let mut global_idx: u64 = 0;
        for chunk in chunks {
            let total_sqes = chunk.len() + 1; // writes + fsync

            // 3. Submit write SQEs (parallel I/O to NVMe).
            //    Each SQE is tagged with its index via user_data so that CQEs
            //    can be matched back to the correct buffer regardless of
            //    completion order (io_uring does not guarantee submission order).
            for buf in chunk.iter() {
                let aligned_len = Self::align_up(buf.len());
                let sqe = opcode::Write::new(
                    types::Fixed(WAL_FD_INDEX),
                    buf.as_ptr(),
                    aligned_len as u32,
                )
                .offset(offset)
                .build()
                .user_data(global_idx);  // global index for stale CQE detection
                unsafe { self.ring.submission().push(&sqe)?; }
                offset += aligned_len as u64;
                global_idx += 1;
            }

            // 4. Submit fsync with IOSQE_IO_DRAIN.
            //    Tag with u64::MAX to distinguish from write CQEs.
            let fsync_sqe = opcode::Fsync::new(types::Fixed(WAL_FD_INDEX))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .flags(types::Flags::IO_DRAIN)
                .user_data(u64::MAX);
            unsafe { self.ring.submission().push(&fsync_sqe)?; }

            // 5. Submit all SQEs in one syscall.
            //    On failure, drain any CQEs that were already posted to
            //    prevent stale CQEs from misaligning the next submission.
            if let Err(e) = self.ring.submit_and_wait(total_sqes) {
                while let Some(_cqe) = self.ring.completion().next() {}
                anyhow::bail!("io_uring submit_and_wait failed: {}", e);
            }

            // 6. Poll for all CQEs — obtain the completion queue ONCE.
            //    Process ALL CQEs in a single loop, checking user_data to
            //    distinguish write CQEs from the fsync CQE.
            let mut write_err: Option<i32> = None;
            let mut fsync_err: Option<i32> = None;
            let chunk_start = global_idx - chunk.len() as u64;
            let mut cq = self.ring.completion();
            for _ in 0..total_sqes {
                let cqe = cq.next()
                    .ok_or_else(|| anyhow!("io_uring: missing CQE"))?;
                let user_data = cqe.user_data();
                if user_data == u64::MAX {
                    if cqe.result() < 0 {
                        fsync_err = Some(cqe.result());
                    }
                } else {
                    // Validate that this CQE belongs to the current chunk.
                    // Stale CQEs from a previous failed submission have
                    // user_data outside the current chunk's range.
                    let local_idx = user_data.checked_sub(chunk_start);
                    match local_idx {
                        Some(idx) if idx < chunk.len() as u64 => {
                            if cqe.result() < 0 {
                                if write_err.is_none() {
                                    write_err = Some(cqe.result());
                                }
                            } else {
                                let written = cqe.result() as usize;
                                let expected = Self::align_up(chunk[idx as usize].len());
                                if written < expected {
                                    if write_err.is_none() {
                                        write_err = Some(-libc::EIO);
                                    }
                                }
                            }
                        }
                        _ => {
                            // Stale CQE from a previous submission — discard.
                            log::warn!("io_uring: stale CQE with user_data={}", user_data);
                        }
                    }
                }
            }
            drop(cq);
            // On I/O error, poison the WAL and return buffers to pool
            // before bailing. Without the pool return, repeated errors
            // would exhaust the 16-buffer pool (buffers freed via Drop
            // are never returned to the ArrayQueue).
            //
            // CRITICAL: All CQEs for this chunk have already been polled
            // above (the loop runs total_sqes times). This guarantees no
            // in-flight SQEs remain before we return the buffers to the
            // pool — returning buffers while SQEs are still in-flight
            // would cause use-after-free when the kernel completes them.
            if write_err.is_some() || fsync_err.is_some() {
                self.poisoned.store(true, Ordering::Release);
                for buf in bufs {
                    let _ = self.buf_pool.push(buf);
                }
                if let Some(err) = write_err {
                    anyhow::bail!("io_uring write error: {}", err);
                }
                if let Some(err) = fsync_err {
                    anyhow::bail!("io_uring fsync error: {}", err);
                }
            }
        }

        // 7. Return buffers to pool on success.
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

**Key design decisions:**

1. **Arc-wrapped errors:** `anyhow::Error` does not implement `Clone`. To share
   the result among multiple waiters, errors are wrapped in `Arc`.
2. **Generation counter:** Each commit cycle increments a generation. Waiters
   check the generation to avoid consuming a stale result from a previous batch.
3. **Ring buffer for results:** A 256-slot ring buffer indexed by
   `generation % RING_SIZE` stores per-generation results. This prevents
   generation N+1 from overwriting generation N's result before a slow
   waiter has read it.
4. **Single mutex:** `completion_state` replaces the nested `completion_mutex` +
   `last_completion_result` — one lock protects both the result and the condvar.
4. **Empty-pending fast path:** If `pending` is empty AND no commit is in flight
   (generation hasn't changed), return immediately. This prevents `sync()` from
   blocking forever on a clean WAL.
5. **No result clearing:** The result is never cleared between cycles. If a
   waiter from generation N wakes up while generation N+1 is in flight, it
   can still read generation N's result. The generation counter is sufficient
   to distinguish current from previous results.

```rust
use std::sync::Arc;

/// Shared completion state for the fate-sharing barrier.
struct CompletionState {
    mutex: Mutex<CompletionInner>,
    cond: Condvar,
}

struct CompletionInner {
    /// Generation counter — incremented on each commit cycle.
    /// Waiters compare this against the generation they observed before
    /// waiting to detect stale results.
    generation: u64,
    /// Ring buffer of recent commit results, indexed by generation % RING_SIZE.
    /// Each slot stores (generation, result) so that a waiter can verify it
    /// is reading its own generation's result, not a stale one from a future
    /// generation that wrapped around. Wrapped in Arc so each waiter can
    /// clone its result (anyhow::Error is !Clone).
    results: [Option<(u64, Result<(), Arc<anyhow::Error>>)>; RING_SIZE],
    /// True while a submitter is actively running I/O.
    in_flight: bool,
}

const RING_SIZE: usize = 256;  // must be power of 2; large enough to prevent wrap under normal concurrency

impl Wal {
    pub fn submit_and_commit(&self) -> Result<()> {
        // Serialize the drain-or-wait decision to prevent TOCTOU races.
        // Without this lock, two threads could both drain pending (one gets
        // bufs, the other gets empty) and both proceed — one submits I/O
        // while the other returns Ok() before the data is durable.
        let _submit_guard = self.submission_lock.lock();

        let my_generation = {
            let state = self.completion_state.mutex.lock();
            state.generation
        };

        let bufs = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };

        if bufs.is_empty() {
            // Our data was already taken by a prior submitter (or there was
            // nothing pending). Check if a commit is actually in flight.
            {
                let state = self.completion_state.mutex.lock();
                if !state.in_flight && state.generation == my_generation {
                    // No commit in flight, nothing to wait for.
                    return Ok(());
                }
            }
            // Wait for the completion signal at our generation.
            // This correctly propagates the result (success OR failure) of the
            // batch that contained our data — the generation counter ensures
            // we read the right result from the ring buffer.
            drop(_submit_guard);
            return self.wait_for_completion(my_generation);
        }

        // I am the submitter — mark in-flight and do the I/O.
        // submission_lock prevents any other thread from entering this path
        // until we finish and signal.
        {
            let mut state = self.completion_state.mutex.lock();
            state.in_flight = true;
            // NOTE: Do NOT clear state.result here. If a waiter from a
            // previous generation wakes up after we set in_flight but before
            // we finish, clearing result would force it to wait for our
            // cycle unnecessarily (or worse, propagate our error instead of
            // the previous success). The generation counter is sufficient
            // to distinguish current from previous results.
        }

        let result = self.submit_sqes_and_poll(bufs);

        // Signal all waiters (even on error).
        let shared_result = result.as_ref()
            .map(|_| ())
            .map_err(|e| Arc::new(anyhow::anyhow!("{e}")));
        {
            let mut state = self.completion_state.mutex.lock();
            let idx = (state.generation as usize) % RING_SIZE;
            state.results[idx] = Some((state.generation, shared_result));
            state.in_flight = false;
            state.generation += 1;
            self.completion_state.cond.notify_all();
        }

        result
    }

    fn wait_for_completion(&self, min_generation: u64) -> Result<()> {
        let mut state = self.completion_state.mutex.lock();
        // Wait until the generation advances past ours (meaning a new result
        // is available) AND our specific result exists in the ring buffer.
        while state.generation <= min_generation
            || state.results[(min_generation as usize) % RING_SIZE].is_none()
        {
            if !state.in_flight && state.generation > min_generation {
                break;
            }
            self.completion_state.cond.wait(&mut state);
        }
        // Read OUR generation's result from the ring buffer.
        // Verify the stored generation matches to detect wrap-around.
        let idx = (min_generation as usize) % RING_SIZE;
        match &state.results[idx] {
            Some((gen, Ok(()))) if *gen == min_generation => Ok(()),
            Some((gen, Err(e))) if *gen == min_generation => Err(anyhow::anyhow!("{e}")),
            Some((gen, _)) => {
                // Generation mismatch — wrap-around occurred.
                // The slot was overwritten by a newer generation.
                // Fall through to Ok(()) as a best-effort fallback.
                log::warn!("ring buffer wrap-around: expected gen {}, got {}", min_generation, gen);
                Ok(())
            }
            None => Ok(()),  // no commit was in flight
        }
    }
}
```

This ensures all callers see the correct result for their specific generation.
The submitter broadcasts the CQE outcome to all waiters via condvar. The
generation counter and ring buffer prevent a late-arriving waiter from
consuming a stale result from a previous commit cycle.

**Chunked submission interaction:** When `submit_and_commit` processes multiple
chunks, the entire drained set is one logical commit group. The generation
counter is incremented once after all chunks complete. All waiters from the
same drain cycle wait on the same generation and receive the same result.
If any chunk fails, all waiters receive the error — partial success is not
reported to callers.

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
        debug_assert!(size % 4096 == 0, "O_DIRECT requires 4KB-aligned size, got {}", size);
        self.next_offset.fetch_add(size, Ordering::AcqRel)
    }
}
```

**Initialization after recovery:** `next_offset` is set to the WAL file length
after recovery truncation. The `recover_mvcc()` function already computes
`valid_file` (wal.rs:443) — this value is passed through to the allocator.

**Preallocation:** On ext4, extending direct I/O takes the exclusive inode
path (`ext4_direct_IO_write` → `ext4_alloc_file_blocks`), serializing all
concurrent writes to the same inode. To stay on the shared overwrite path,
the WAL file is preallocated ahead of `alloc_offset`:

```rust
/// Preallocate WAL space in 1 MiB increments.
/// Uses fallocate (not ftruncate) to ensure physical blocks are allocated
/// on disk, so subsequent O_DIRECT writes are true overwrites (parallel path).
/// Tracks preallocated size in memory to avoid stat() syscalls on the hot path.
fn maybe_preallocate(&self) -> Result<()> {
    let offset = self.alloc_offset.load(Ordering::Relaxed);
    let file_size = self.preallocated_size.load(Ordering::Relaxed);
    if offset + PREALLOC_THRESHOLD > file_size {
        let new_size = file_size + PREALLOC_BLOCK;  // 1 MiB
        // fallocate (mode=0) allocates physical blocks AND extends the
        // logical EOF. This is critical: O_DIRECT writes that extend beyond
        // the logical EOF are treated as "extending writes" by the kernel,
        // which acquires the exclusive inode lock and serializes all
        // concurrent writes. By preallocating with extended EOF, all writes
        // are "overwrites" (within the file size), allowing true parallel DIO.
        //
        // Recovery is protected by the v4 alignment-gap skipping logic:
        // the preallocated zero-filled tail is beyond the last valid batch,
        // and recovery stops at the first batch that fails CRC or has
        // entry_count=0 at an unexpected offset.
        let ret = unsafe {
            libc::fallocate(
                self.write_file.as_raw_fd(),
                0,  // mode=0: allocate AND extend EOF
                file_size as i64,
                PREALLOC_BLOCK as i64,
            )
        };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            // EOPNOTSUPP/ENOSYS: filesystem doesn't support fallocate.
            // Fall back to ftruncate — less optimal (may take exclusive
            // inode lock on first write) but keeps the engine usable.
            if err.raw_os_error() == Some(libc::EOPNOTSUPP)
                || err.raw_os_error() == Some(libc::ENOSYS)
            {
                log::warn!("fallocate not supported, falling back to ftruncate: {}", err);
                self.write_file.set_len(new_size)?;
            } else {
                anyhow::bail!("fallocate failed: {}", err);
            }
        }
        self.preallocated_size.store(new_size, Ordering::Relaxed);
    }
    Ok(())
}
```

This keeps writes within the preallocated extent, where ext4 uses the shared
overwrite path and allows true parallel DIO. The preallocation cost is
amortized — 1 MiB blocks last for ~16 batches at 64 KiB each.

**In-memory size tracking:** `preallocated_size: AtomicU64` is initialized
during recovery/open and updated after each `fallocate` call. This avoids
a `stat()` syscall on every `submit_and_commit()` call, preserving the
syscall-reduction benefit of io_uring.

### 5.9 Alignment Gap Handling

O_DIRECT requires 4KB-aligned write sizes. Each batch buffer is zero-padded
to the aligned length after encoding:

```rust
fn encode_batch(&self, data: &[(&[u8], &[u8])], commit_ts: u64) -> Result<DirectBuf> {
    // Compute encoded size first, then allocate a buffer that fits.
    // Large transactions or range-tombstone batches may exceed the 64 KiB
    // pool buffer; sizing from the encoded length prevents out-of-bounds writes.
    let encoded_size = Self::compute_encoded_size(data);
    let alloc_size = Self::align_up(encoded_size).max(BUFFER_POOL_BUF_SIZE);
    let mut buf = self.buf_pool.pop()
        .filter(|b| b.cap() >= alloc_size)
        .unwrap_or_else(|| DirectBuf::new(alloc_size));
    // ... encode batch data into buf.as_mut_slice() ...

    // Zero-pad to 4KB alignment (O_DIRECT requirement).
    // This also ensures recovery doesn't read stale bytes as batch data.
    let len = buf.len();
    let aligned_len = Self::align_up(len);
    let slice = buf.as_mut_slice();
    for i in len..aligned_len {
        slice[i] = 0;
    }
    buf.set_len(aligned_len);

    Ok(buf)
}
```

**Why zero-pad?** Without padding, the alignment gap contains stale data from
previous buffer reuse. The recovery scanner would read this garbage as batch
headers, fail CRC validation, and truncate the WAL — losing all batches
after the first gap.

**Recovery handling of zero-padded regions:** Each batch header includes a
`data_len` field (the data-only size, excluding the header). After reading
a valid batch, recovery skips to the next 4KB boundary:

```rust
// After reading a valid batch at offset O with data_len L:
let total_batch_size = BATCH_HEADER_SIZE + L;
let aligned_size = align_up(total_batch_size);
next_offset = current_offset + aligned_size;
```

This ensures recovery jumps over the entire alignment gap — including partial
zero bytes that don't form a complete 16-byte empty batch header. Without
this, recovery would combine trailing padding bytes with the next batch's
header, fail CRC validation, and truncate the WAL.

### 5.10 Record Checksum & Header (Modified)

Each batch has a CRC32 checksum over the **entry data only** (the payload
bytes after the header), consistent with the existing v3 implementation in
`wal.rs`. The CRC does NOT cover header fields. O_DIRECT does not affect
the checksum — it covers only the valid bytes (before zero-padding).

**New field: `data_len`** (4 bytes, added after `data_crc32`). Stores the
**data-only** size of the batch (entries only, excluding the header and
alignment padding). Recovery uses this to skip to the next 4KB boundary
after reading a batch, preventing padding bytes from being misinterpreted
as batch headers.

**Backward compatibility:** The new header is 20 bytes, introduced as
`WAL_FORMAT_VERSION_V4`. Old v3 files (16-byte headers) are still supported —
recovery detects the version from the WAL file header and uses the
corresponding header size. New batches are always written with v4 headers.

```
WAL_FORMAT_VERSION_V4: BATCH_HEADER_SIZE = 20 bytes (was 16 in v3):
  commit_ts:     u64   (8 bytes)
  entry_count:   u32   (4 bytes)
  data_crc32:    u32   (4 bytes)
  data_len:      u32   (4 bytes)  ← NEW (data-only, excludes header)

WAL_FORMAT_VERSION_V3: BATCH_HEADER_SIZE = 16 bytes (unchanged, matches wal.rs):
  commit_ts:     u64   (8 bytes)
  entry_count:   u32   (4 bytes)
  data_crc32:    u32   (4 bytes)
```

The CRC covers **entry data only** (the payload bytes after the header),
consistent with the existing v3 implementation in `wal.rs`. The CRC does NOT
cover header fields (`commit_ts`, `entry_count`, `data_crc32`, `data_len`) —
including `data_crc32` in its own checksum is not well-defined. `data_len` is
outside the CRC — it is an optimization hint for recovery, not a data
integrity field. An incorrect `data_len` causes recovery to skip incorrectly
and fail CRC on the next batch, which is safe (truncation, not corruption).

**Recovery offset calculation (v4):**
```rust
let total_batch_size = BATCH_HEADER_SIZE + batch.data_len as usize;
let aligned_size = align_up(total_batch_size);
next_offset = current_offset + aligned_size;
```

This does NOT double-count the header because `data_len` is data-only.

### 5.11 Recovery Scan (Modified)

Recovery reads the WAL file sequentially using the buffered file handle,
validates CRC per batch, and stops at the first invalid entry. The WAL file
format is backward compatible — old v3 batches are handled as before.

**v4 initial scan offset:** For v4 files, the first batch starts at
`align_up(WAL_HEADER_SIZE)` (4096), not at `WAL_HEADER_SIZE` (6). Recovery
must begin scanning at the aligned offset to avoid reading the zero-padded
header region as batch data:

```rust
let scan_start = match wal_version {
    V4 => align_up(WAL_HEADER_SIZE),  // 4096
    V3 => WAL_HEADER_SIZE,            // 6
};
```

**Alignment-gap skipping (v4):** After reading a valid batch, recovery advances
the read offset to the next 4KB boundary using `data_len`:

```rust
let total_batch_size = BATCH_HEADER_SIZE + batch.data_len as usize;
let aligned_size = align_up(total_batch_size);
offset += aligned_size;
```

**v3 fallback:** Old v3 batches don't have `data_len`. Recovery uses the
existing sequential scan (header + entry_count-based parsing) for v3 files.
The alignment-gap skipping only applies to v4+ batches written with O_DIRECT.

This prevents trailing zero-padding bytes from being combined with the next
batch's header, which would fail CRC and cause truncation.

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
fn new_buf_pool() -> ArrayQueue<DirectBuf> {
    let pool = ArrayQueue::new(BUFFER_POOL_CAPACITY);
    for _ in 0..BUFFER_POOL_CAPACITY {
        let _ = pool.push(DirectBuf::new(BUFFER_POOL_BUF_SIZE));
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
        if !self.mvcc_format {
            // Legacy path: flush and fsync the BufWriter directly.
            // submit_and_commit() only handles the io_uring path.
            self.buf_writer.lock().flush()?;
            self.buf_writer.get_ref().sync_data()?;
            return Ok(());
        }
        self.submit_and_commit()
    }
}
```

`sync()` is equivalent to `submit_and_commit()` for the MVCC/io_uring path.
If there are pending buffers, they are drained and committed. If not, the
completion barrier checks whether a commit is in flight: if so, it waits;
if not, it returns immediately. For the legacy `!mvcc_format` path, `sync()`
flushes and fsyncs the `BufWriter` directly — `submit_and_commit()` does not
handle buffered writes.

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

**Fallback format consistency:** The fallback path continues writing v3-format
batches (16-byte headers, no alignment padding). This ensures the fallback
WAL is readable by the existing v3 recovery logic without the v4 alignment-gap
skipping. Only the io_uring path writes v4-format batches. The WAL file header
records which format version is in use, so recovery selects the correct
parsing strategy automatically.

On Linux, attempt full io_uring initialization at WAL open: ring creation
AND `register_files`. If **any** step fails (kernel too old, EMFILE, ENOMEM),
fall back to the synchronous path. Store `use_io_uring: bool` in the `Wal`
struct. Feature detection is runtime, not compile-time — a Linux binary works
on kernels 5.11+ (io_uring) and <5.11 (fallback).

```rust
let ring = match try_init_io_uring(&write_file) {
    Ok(ring) => ring,
    Err(e) => {
        log::warn!("io_uring unavailable ({}), falling back to group commit", e);
        return Wal::new_group_commit(path);
    }
};
```

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
