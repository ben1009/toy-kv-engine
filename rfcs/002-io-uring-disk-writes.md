# RFC 002: io_uring for Disk Writes

**Status:** Research / Not yet implemented  
**Date:** 2026-06-01

## 1. Current I/O Patterns

All disk I/O uses synchronous `std::fs` APIs. No async, no mmap, no io_uring.

### Write Path Summary

| Component | Write Pattern | Buffering | Sync Frequency | Syscalls per Entry |
|-----------|--------------|-----------|----------------|-------------------|
| **SST** | Monolithic (`std::fs::write`) | None (entire file in memory) | `sync_all()` once per file | 2 (write + fsync) |
| **WAL** | Sequential append | `BufWriter` (8KB default) | `sync_all()` on user `sync()` | 1-2 (write_all, flush + fsync) |
| **vLog** | Sequential append | `BufWriter` (8KB default) | `sync_data()` at `close()` | 4 per entry (header + key + value + padding) |
| **vLog index** | Single write per flush | `BufWriter` (8KB default) | `sync_data()` at close | 2 (write + fdatasync) |
| **Manifest** | Sequential append | **None** (raw File) | `sync_all()` on **every record** | 2 per record (write + fsync) |

### Read Path Summary

| Component | Read Pattern | API |
|-----------|-------------|-----|
| **SST** | Random access | `pread` via `FileExt::read_exact_at` |
| **vLog** | Sequential scan + random access | `pread` via `FileExt::read_exact_at` |
| **Manifest** | Sequential scan on open | `BufReader` |

### Key Bottlenecks

1. **Manifest: `sync_all()` on every record** — The most syscall-heavy path. Every `add_record()` does `write_all()` + `sync_all()` with zero buffering. Each `sync_all()` is a blocking `fsync(2)` that flushes all dirty pages to disk.

2. **SST: `sync_all()` blocks the flush thread** — The entire SST is assembled in memory, then written in one shot. The write itself is efficient, but `sync_all()` blocks the calling thread until all data reaches stable storage.

3. **WAL: Mutex serializes all writes** — `parking_lot::Mutex` on every `put()` and `sync()`. The BufWriter helps batch small writes, but the lock is held during the entire write+sync cycle.

4. **vLog: 4 `write_all` calls per entry** — Header, key, value, padding are separate writes. BufWriter mitigates this when the buffer is not full, but on buffer-full flushes there are multiple syscalls.

---

## 2. io_uring Library Options

### Comparison

| Crate | Version | Maturity | Min Kernel | Model | Best For |
|-------|---------|----------|------------|-------|----------|
| `io-uring` | 0.7.12 | Moderate (tokio-rs, 57% docs) | 5.1+ | Low-level binding | Full control, targeted integration |
| `tokio-uring` | 0.5.0 | Young ("very young") | 5.11+ | Tokio + uring | Tokio ecosystem compatibility |
| `glommio` | 0.9.0 | Most mature for storage | 5.8+ | Thread-per-core | Storage workloads (ScyllaDB lineage) |
| `monoio` | 0.2.4 | Moderate (Bytedance) | 5.6+ | Thread-per-core | Cross-platform fallback (kqueue/epoll) |

### Key io_uring Opcodes for Storage

| Opcode | Syscall | Use Case |
|--------|---------|----------|
| `IORING_OP_WRITE` / `IORING_OP_WRITEV` | pwrite / pwritev | SST, WAL, vLog writes |
| `IORING_OP_FSYNC` | fsync | Durability guarantees |
| `IORING_OP_OPENAT` / `IORING_OP_CLOSE` | openat / close | File lifecycle |
| `IORING_OP_FALLOCATE` | fallocate | Pre-allocate SST/vLog space |
| `IORING_OP_SYNC_FILE_RANGE` | sync_file_range | Partial sync (data only, no metadata) |
| `IORING_OP_PREAD` / `IORING_OP_PREADV` | pread / preadv | SST random reads |

### No Rust KV Engine Uses io_uring Today

- **redb**, **fjall**, **sled** — all use standard `std::fs`
- **RocksDB** (C++) — has production io_uring for `Iterator` and `MultiGet` (async block reads)
- **ScyllaDB**, **Redpanda** (C++) — use io_uring via Seastar's reactor

---

## 3. Where io_uring Would Help Most

### Tier 1: High Benefit

**Manifest writes** — Every record write is `write_all()` + `sync_all()` with no buffering. io_uring could:
- Batch multiple records into a single submission
- Chain write + fsync as linked operations (`IOSQE_IO_LINK`)
- Submit fsync asynchronously and continue processing

Estimated syscall reduction: 2x → 1x per batch (submit N writes + 1 fsync in one `io_uring_enter`).

**SST `sync_all()`** — The flush thread blocks on `sync_all()` after writing the SST. io_uring could:
- Submit the fsync asynchronously
- Return control to the flush thread to start building the next SST (deferring WAL deletion and manifest commit until the fsync completion is reaped)
- Reap the completion later

**Important:** The `ManifestRecord::Flush` and WAL/memtable cleanup for the flushed SST must NOT be committed until the async fsync is confirmed complete. Otherwise a crash could leave the manifest pointing to an SST whose data was never made durable.

### Tier 2: Medium Benefit

**WAL writes** — The mutex + sync pattern could benefit from:
- Submitting writes to the ring while holding the mutex briefly
- Chaining write + fsync in the ring, so the mutex is released before fsync completes
- Reducing the critical section from "write + sync" to "submit to ring"

**vLog writes** — The 4 `write_all` calls per entry could be coalesced:
- `IORING_OP_WRITEV` with scatter/gather iovecs for header+key+value+padding
- Single submission instead of 4 BufWriter calls

### Tier 3: Low Benefit

**SST monolithic write** — Already a single `write(2)` syscall. The only benefit is async fsync.

**vLog index** — Infrequent, small files. Not worth the complexity.

---

## 4. Trade-offs

### Advantages

1. **Reduced syscall overhead** — Shared memory rings between kernel and userspace. Multiple I/Os submitted with one `io_uring_enter`.
2. **True async I/O** — Submit write and continue processing while I/O is in flight.
3. **Vectored I/O** — `writev` coalesces multiple buffers (vLog header+key+value+padding).
4. **Linked operations** — `IOSQE_IO_LINK` chains write→fsync without userspace coordination.
5. **Fixed buffers** — Pre-registered buffers avoid per-I/O setup overhead (primarily beneficial for fixed-size SST block I/O rather than variable-sized sequential appends).
6. **Poll mode** — NVMe latency near hardware minimum (glommio's poll ring).

### Disadvantages

1. **Linux-only** — Eliminates macOS, Windows, FreeBSD.
2. **Kernel version** — Requires 5.8+ (glommio) to 5.11+ (tokio-uring).
3. **Complexity** — Ownership-transfer buffer model is a significant departure from standard Rust I/O.
4. **Debugging** — io_uring bugs (use-after-free, ring corruption) are harder to diagnose.
5. **Memlock limits** — Historically required locked memory, but since Linux 5.12, io_uring memory is charged to cgroup limits instead of `RLIMIT_MEMLOCK`, eliminating this restriction on modern kernels. Registered/fixed buffers (`IORING_REGISTER_BUFFERS`) still count against `RLIMIT_MEMLOCK` on pre-5.12 kernels.
6. **Not always faster** — For purely sequential writes, the async machinery overhead can exceed simple `write()` + `fsync()`.
7. **Ecosystem immaturity** — No production Rust KV engine uses io_uring today.

---

## 5. Recommended Approach

### Option A: Targeted Integration (Recommended if implementing)

Use the low-level `io-uring` crate directly for specific write paths. Keep the existing synchronous API — swap the underlying I/O implementation only.

**Target paths:**
1. Manifest: batch write + linked fsync
2. SST: async fsync after monolithic write
3. vLog: `writev` for entry coalescing

**Not targeted:**
- WAL (mutex contention is the real bottleneck, not I/O)
- Reads (pread is already efficient for random access)

**Fallback:** `cfg(target_os = "linux")` gates io_uring code; `std::fs` fallback on other platforms.

### Option B: Full Async Rewrite (High effort, high reward)

Adopt **glommio** as the I/O runtime. Thread-per-core model with DMA buffers. Requires restructuring flush, compaction, and GC into async tasks. Major architectural change.

**Benefits:** Eliminates all mutex contention, enables true parallel I/O, Direct I/O bypasses page cache.

**Cost:** Complete rewrite of the I/O layer. `!Send`/`!Sync` constraints propagate through the codebase.

### Option C: Benchmark Study (Practical first step)

Write benchmarks comparing:
- `std::fs::write` + `sync_all` vs `io_uring` write + fsync
- Single write vs batched writes (10, 100, 1000 entries)
- Manifest-like pattern: write + sync per record
- SST-like pattern: monolithic write + sync

Measure: latency, throughput, syscall count (via `strace`), CPU usage.

---

## 6. Kernel Version Check

```bash
uname -r  # Check current kernel version
```

Minimum required:
- `io-uring` crate: 5.1+ (basic), 5.4+ (stable fsync), 5.6+ (ring-mapped buffers)
- glommio: 5.8+
- tokio-uring: 5.11+

---

## 7. Decision

**Status: Research only. No implementation planned yet.**

Next steps when ready:
1. Run kernel version check on target machines
2. Write benchmarks (Option C) to quantify the benefit
3. Decide between Option A (targeted) and Option B (full rewrite)
4. Prototype manifest writes with io_uring (highest benefit, smallest scope)
