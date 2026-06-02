# Performance Profiling Report

**Date:** 2026-06-02
**Kernel:** 6.18.9-arch1-2
**Tool:** `perf record -g -F 4999 --call-graph dwarf`
**Build:** release (optimized)

## Summary

**I/O is NOT the bottleneck.** The engine is CPU-bound across all workloads. Context switches: 0. I/O accounts for ~0-4% of CPU time.

## Throughput by Workload

| Workload | ops/sec | Dominant Cost |
|----------|---------|---------------|
| Sequential inline (256B vals) | 2.9M | SST building, memtable insert |
| Sequential vLog (4KB vals) | 892k | **vLog writes (3x slower)** |
| Random inline (256B vals) | 3.1M | SST building (reuses keys) |
| Random vLog (4KB vals) | 1.6M | vLog writes |
| Mixed 50/50 read/write | 319k | **Skiplist reads dominate** |
| Mixed 90/10 read/write | 422k | Reads still dominant |
| Concurrent 1 thread | 3.1M | Same as sequential |
| Concurrent 4 threads | 5.2M | **Scales 1.7x with threads** |

Config: 200k ops, 1MB SSTs, 2 memtable limit, leveled compaction, block cache 1024.

## Profile: Write-Only (500k entries, 1KB vals, no WAL)

```
41.6%  SsTableBuilder::add_inner       CPU — block building, farmhash, mem copies
 9.3%  MemTable::flush (skiplist iter)  CPU — iterator traversal
 3.9%  crossbeam_skiplist::insert       CPU — skiplist ops
 0.0%  fsync / kernel I/O              I/O
```

moka housekeeper thread: 63% of total samples (cache rehash, eviction, epoch GC).

## Profile: Write-Only with WAL (50k entries, fsync per put)

```
26.2%  SsTableBuilder::add_inner       CPU
 4.9%  crossbeam_skiplist::search       CPU
 4.0%  fsync                           I/O (the only I/O-visible function)
 3.1%  Wal::put                        CPU — BufWriter write
 3.3%  try_freeze_memtable             CPU
```

WAL `put()` calls `sync()` (fsync) on every write. But even then, fsync is only 4% of samples.

## Profile: Mixed Read/Write (200k ops, 50/50 split, 256B vals)

```
20.0%  crossbeam_skiplist::try_pin_loop (SkipMap::get)  CPU — skiplist lookup for reads
 7.8%  libc (memory allocation)                          CPU
 7.8%  SsTableBuilder::add_inner                         CPU
 4.3%  crc32fast::pclmulqdq::calculate                  CPU — CRC32 checksums
 3.9%  crossbeam_epoch::with_handle (pin)                CPU — epoch pinning
 3.6%  skiplist::search_position                         CPU
 1.8%  skiplist::insert_internal                         CPU
```

Reads dominate: skiplist lookup (20%) + epoch pinning (4%) + CRC32 (4%) = 28% just for the read path.
I/O: ~0%.

## Profile: WAL-Enabled (50k entries, fsync per put)

```
26.2%  SsTableBuilder::add_inner       CPU
 4.9%  crossbeam_skiplist::search       CPU — skiplist lookup
 4.0%  fsync                           I/O — the only visible I/O
 3.1%  Wal::put                        CPU — BufWriter
 3.3%  try_freeze_memtable             CPU
```

## Per-Thread Breakdown

| Thread | Write-only | Mixed r/w |
|--------|-----------|-----------|
| write-perf (main) | 37% | 82% |
| moka-housekeeper | 63% | 18% |

The moka housekeeper consumes a disproportionate amount of CPU for cache maintenance.

## Hardware Counters

```
Write-only (500k entries):
  3.5B instructions in 3.5B cycles (IPC ~3.7)
  0 context switches
  0 CPU migrations
  266ms sys (kernel), 686ms user

All workloads combined (200k ops each):
  23.5B instructions in 12.5B cycles (IPC ~1.9)
  0 context switches
  0 CPU migrations
```

Zero context switches confirms: no I/O blocking, no thread sleeping on locks.

## CPU Bottleneck Breakdown

### 1. SST Block Building (7-42% depending on workload)

`SsTableBuilder::add_inner` is the single largest CPU consumer in write-heavy workloads:
- `farmhash::hash32` for bloom filter
- `BlockBuilder::add` — key/value encoding into block format
- Memory allocation: `Bytes::copy_from_slice`, `Vec::extend`
- Block finalization: `BlockBuilder::build().encode()`

### 2. moka Block Cache (18-63% of total)

The moka housekeeper thread does heavy concurrent data structure maintenance:
- `BucketArray::rehash` — hash table resizing
- `Deques::unlink_ao` / `push_back_ao` — eviction deque management
- `crossbeam_epoch::Global::collect` — epoch-based GC
- `Inner::sync` — cache admission/eviction

This runs on a background thread but consumes significant CPU.

### 3. Crossbeam Skiplist (4-20%)

- `SkipMap::get` via `try_pin_loop` — epoch pin + search (dominant in read workloads)
- `SkipMap::insert` via `insert_internal` — search + link
- `crossbeam_epoch::with_handle` — epoch pin overhead on every operation

### 4. CRC32 Checksums (1-4%)

`crc32fast::pclmulqdq::calculate` uses hardware PCLMULQDQ instructions. Efficient but visible in mixed workloads.

### 5. Memory Allocation (2-8%)

`malloc`, `realloc`, `cfree` from libc — significant in mixed workloads due to `Bytes::copy_from_slice` on every put.

## Conclusions

1. **I/O is not the bottleneck.** Async io_uring would not improve throughput. The RFC 003 proposal to adopt compio is deferred.

2. **vLog is 3x slower than inline** for sequential writes. The 4 `write_all` calls per entry (header+key+value+padding) are CPU-bound BufWriter operations, not I/O-bound. A `write_vectored` coalescing would help even without io_uring.

3. **Reads are expensive.** Mixed 50/50 r/w drops throughput to 319k ops/sec (from 2.9M write-only). The skiplist `try_pin_loop` + epoch pin overhead dominates.

4. **moka housekeeper is a CPU hog.** 63% of samples in write-only, 18% in mixed. Worth investigating: smaller cache, different eviction policy, or lazy housekeeping.

5. **Concurrent writes scale.** 4 threads achieve 1.7x throughput over 1 thread. The lock-free skiplist enables this.

## Recommendations

| Optimization | Expected Impact | Effort |
|-------------|----------------|--------|
| Replace `farmhash` with faster hash (e.g. `ahash`) | 5-10% write throughput | Low |
| Reduce `Bytes::copy_from_slice` in put path | 5-8% write throughput | Medium |
| Investigate moka housekeeper CPU cost | 10-20% overall CPU | Medium |
| `write_vectored` for vLog entries | 2-3x vLog throughput | Low |
| Reduce epoch pin overhead (batch operations?) | 5-10% read throughput | High |
| Manifest batching with `std::fs` | Reduces manifest fsyncs | Low (10 lines) |
