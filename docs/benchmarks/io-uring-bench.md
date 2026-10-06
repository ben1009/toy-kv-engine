# io_uring Benchmark Results

**Date:** 2026-06-02
**Kernel:** 6.18.9-arch1-2
**Crate:** `io-uring` 0.7.12

## Summary

io_uring is **3-12x slower** than `std::fs` for the synchronous single-operation pattern used by this engine. The overhead of `io_uring_enter` + CQE drain per operation negates any benefit without batching.

## Results

| Benchmark | std::fs | io_uring | Ratio |
|-----------|---------|----------|-------|
| fsync (4KB write + fsync) | 2.15µs | 7.83µs | **3.6x slower** |
| writev (vLog entry: header+key+value+padding) | 2.15µs | 8.02µs | **3.7x slower** |
| manifest write+fsync (128B record) | 301ns | 3.54µs | **11.8x slower** |
| batch 10x fsync (512B × 10) | 5.09µs | 36.87µs | **7.2x slower** |

## Why io_uring Is Slower Here

Each io_uring operation requires:
1. Push SQE to submission queue (`io_uring_enter` syscall)
2. Wait for completion queue entry
3. Drain CQ and match on `user_data`

For a single fsync, this is 3 steps vs `sync_all()`'s 1 step (direct `fsync(2)` syscall). The per-operation overhead is ~5-6µs.

io_uring's advantage is **batching**: submit N operations in one `io_uring_enter`, wait once for all completions. The current engine pattern (write → fsync, one at a time) doesn't exploit this.

## When io_uring Would Help

Per the RFC (`rfcs/002-io-uring-disk-writes.md`):

- **Batched writes**: Submit multiple manifest records or SST blocks in one `io_uring_enter`
- **Linked operations**: `IOSQE_IO_LINK` chains write→fsync without userspace coordination
- **True async**: Submit fsync and continue building the next SST while I/O is in flight

These require restructuring the engine from synchronous to async, which is a much larger change (Option B in the RFC).

## Decision

Reverted. The targeted integration (Option A) with synchronous per-operation io_uring adds overhead without benefit. The RFC's recommendation to start with benchmarks (Option C) was correct — the numbers show io_uring doesn't help for this engine's I/O patterns without a full async rewrite.

## Benchmark Code

```bash
cargo bench --bench io_uring_benchmarks
```

See `kv-engine/benches/io_uring_benchmarks.rs` for the benchmark implementation.
