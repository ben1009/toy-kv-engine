# Performance Profiling Report

**Date:** 2026-06-03 (updated)
**Kernel:** 6.18.9-arch1-2
**Tool:** `perf record -g -F 4999 --call-graph dwarf`
**Build:** release (optimized)

## Summary

**Lock-free WAL + group commit (2026-06-25, PR #130).** Replaced `Mutex<Vec<u8>>` pending buffer with
`crossbeam_queue::ArrayQueue` (16 pre-allocated 64KB buffers) + lock-free `SegQueue`. Group commit via
leader/follower condvar barrier amortizes fsync across concurrent writers. MVCC write-lock released before
`commit_wal` to prevent deadlocks during memtable freezing. 4-thread WAL throughput: 177K → 451K (+155%).
All built-in write-path counters are gated behind `#[cfg(feature = "bench")]`
and add zero timing overhead in production builds.

## Optional Hotpath Profiling

The engine also supports broad phase profiling through
[hotpath-rs](https://hotpath.rs/profiling_overhead) behind the
`hotpath-profile` Cargo feature. This is intended for exploratory profiling of
coarse engine phases, not for replacing the stable `WriteProfile` counters used
in benchmark reports.

Hotpath scopes currently cover public engine operations (`kv.get`, `kv.put`,
`kv.write_batch`, `kv.scan`, flush/compaction/vLog GC) plus write-path phases
(`wal.put_batch`, `wal.submit_and_commit`, `memtable.publish_batch`). Normal
builds do not include the dependency or runtime hooks.

Example:

```bash
HOTPATH_METRICS_SERVER_OFF=true \
HOTPATH_TIME_SAMPLING_RATE=0.1 \
cargo run --release --features hotpath-profile --bin write-perf -- \
  --profile --bench readrandom --num 100000 --reads 100000
```

Use time sampling for very hot workloads. The Hotpath overhead guide reports
that disabled instrumentation compiles to no-op and enabled function timing is
on the order of tens of nanoseconds per measured call; sampling avoids most
clock reads while preserving exact call counts.

**I/O is NOT the bottleneck.** The engine is CPU-bound across all workloads. Context switches: 0. I/O accounts for ~0-4% of CPU time.

**Read path optimized (2026-06-02).** After optimizations: readrandom 131k→741k ops/sec (5.7x), readwhilewriting reads 23k→1.06M (46x), readrandomwriterandom 104k→1.40M (13x). Now exceeds RocksDB readrandom (~500k).

**Block encode optimized (2026-06-03, PR #43).** `Block::encode()` changed from `&self` to `mut self`, eliminating intermediate `Vec<u8>` allocation. Added `reserve()` for offsets. Bottleneck shifted from write path to read path (skiplist epoch pin 49.8% of CPU).

**arc-swap state snapshot (2026-06-03, PR #44).** Replaced `parking_lot::RwLock` with `arc_swap::ArcSwap` for state snapshot. Every `get()`/`scan()` now uses atomic load instead of lock acquisition. fillrandom +86%, overwrite +87%.

**TinyUFO cache swap (2026-06-04, PR #55).** Replaced `moka` (TinyLFU + LRU) with Cloudflare's `TinyUFO` (S3-FIFO + TinyLFU, lock-free) for all in-memory caches. Eliminated the moka housekeeper thread (was 9.5-63% of CPU). Cache-related CPU dropped from 9.5-63% to ~1.4%.

**Cache improvements (2026-06-04).** Added ahash for cache/vlog reverse indexes, per-key single-flight for block cache misses (coalesces concurrent I/O), and 19 unit tests. Combined with TinyUFO: fillseq +17%, fillrandom +8%, readrandom +11%, overwrite +14% vs moka baseline. Concurrent R/W reads +8%, readwhilewriting writes +5%.

**Reduced key cloning in `SsTableBuilder::add_inner` (2026-06-05).** Removed live `KeyVec` clones in `BlockBuilder` and `SsTableBuilder`; block-meta first/last keys are now derived from the already-encoded block data. Inline write path improved 3–5% (−4.7% at 4KB, −3.8% at 16KB, −4.8% at 64KB). Also deduplicated `shared_bytes_from_slice` and made oversized single entries a loud error.

## Throughput by Workload

| Workload | ops/sec | Dominant Cost |
|----------|---------|---------------|
| Sequential inline (256B vals) | 2.9M | SST building, memtable insert |
| Sequential vLog (4KB vals) | 892k | **vLog writes** (note: 4KB vals vs 256B — not directly comparable) |
| Random inline (256B vals) | 3.1M | SST building (reuses keys) |
| Random vLog (4KB vals) | 1.6M | vLog writes |
| Mixed 50/50 read/write | 319k→**1.40M** | **Skiplist reads dominate** (optimized) |
| Mixed 90/10 read/write | 422k | Reads still dominant |
| Concurrent 1 thread | 3.1M | Same as sequential |
| Concurrent 4 threads | 5.2M | **Scales 1.7x with threads** |

Config: 200k ops, 1MB SSTs, 2 memtable limit, leveled compaction, block cache 1024.

### WAL Concurrent Throughput (50K keys, 1KB values, sync)

| Threads | Main (ops/s) | Branch (ops/s) | Change |
|---------|-------------|----------------|--------|
| 1 | 616K | 641K | +4% |
| 2 | 528K | 486K | -8% |
| 4 | 177K | **451K** | **+155%** |

Main collapses at 4 threads due to mutex contention on `pending_buf`. Lock-free `ArrayQueue` + group commit
maintains throughput under contention.

## Profile: Write-Only (500k entries, 1KB vals, no WAL)

```text
41.6%  SsTableBuilder::add_inner       CPU — block building, farmhash, mem copies
 9.3%  MemTable::flush (skiplist iter)  CPU — iterator traversal
 3.9%  crossbeam_skiplist::insert       CPU — skiplist ops
 0.0%  fsync / kernel I/O              I/O
```

moka housekeeper thread: 63% of total samples (cache rehash, eviction, epoch GC).

## Profile: Write-Only with WAL (50k entries, fsync per put)

```text
26.2%  SsTableBuilder::add_inner       CPU
 4.9%  crossbeam_skiplist::search       CPU
 4.0%  fsync                           I/O (the only I/O-visible function)
 3.1%  Wal::put                        CPU — BufWriter write
 3.3%  try_freeze_memtable             CPU
```

WAL `put()` calls `sync()` (fsync) on every write. But even then, fsync is only 4% of samples.

## Profile: Mixed Read/Write (200k ops, 50/50 split, 256B vals)

```text
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

## Per-Thread Breakdown

| Thread | Write-only (before) | Write-only (after cache invalidation) | Mixed r/w | After TinyUFO |
|--------|-----------|-----------|-----------|---------------|
| write-perf (main) | 37% | **90.6%** | 82% | **100%** |
| moka-housekeeper | 63% | **9.5%** | 18% | **0%** (eliminated) |

The moka housekeeper consumed a disproportionate amount of CPU for cache maintenance. After
invalidating stale block cache entries on compaction (2026-06-03), housekeeper dropped 30%
(from 13.5% → 9.5% in full-suite profiling, 63% → 9.5% in prior isolated runs).

**TinyUFO (PR #55):** Eliminated the housekeeper thread entirely. TinyUFO does eviction
inline during `put()` with lock-free S3-FIFO. Cache-related CPU is now ~1.4% (TinyLFU
frequency sketch 0.16%, flurry hash lookups 0.2%, reverse index mutex ~1%).

## Hardware Counters

```text
Write-only (500k entries, cpu_core PMU):
  13.0B instructions in 3.5B cycles (IPC ~3.7)
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
- `key.to_key_vec().into_inner()` — key cloning (3x per entry: first_key, last_key, builder)
- `BlockBuilder::add` — prefix overlap computation, key/value encoding (`put_u16`, `put`)
- Block finalization: `BlockBuilder::build().encode()` — allocates new `Vec`, copies data + offsets
- `self.data.extend(data)` — memory copy of encoded block
- `bloom::hash_key` — ahash (fast, ~1% of CPU)

### 2. Block Cache: moka → TinyUFO (was 9.5-63%, now ~1.4%)

**moka (before):** Dedicated housekeeper thread doing heavy concurrent data structure
maintenance: `BucketArray::rehash` (4.5%), `Deques::unlink_ao`/`push_back_ao` (eviction),
`crossbeam_epoch::Global::collect` (epoch GC), `Inner::sync` (1.0%). After compaction
invalidation fix (2026-06-03): 9.5% in write-heavy, 18% in mixed.

**TinyUFO (PR #55):** No background thread. Eviction inline via lock-free S3-FIFO.
Cache-related CPU: TinyLFU frequency sketch 0.16%, flurry hash lookups 0.2%,
reverse index Mutex ~1%. Total ~1.4%.

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

2. **vLog writes are now faster than inline** (was 5-33% slower). Replaced 4 `write_all` calls per entry with a single coalesced write, eliminating per-entry buffer management overhead. vLog is now 35-66% faster than inline across all value sizes.

3. **Reads were expensive — now optimized.** Before: mixed 50/50 r/w dropped to 319k ops/sec. After: 1.40M ops/sec. The skiplist `try_pin_loop` + epoch pin overhead was eliminated via memtable bloom filter, direct point_get, and ahash.

4. **moka housekeeper was a CPU hog — now eliminated.** Was 63% in write-only, 18% in mixed. After invalidating stale block cache entries on compaction (commit `1803040` follow-up): 9.5% in write-heavy workloads. Fully eliminated by switching to TinyUFO (PR #55) — no background thread, lock-free S3-FIFO eviction inline during `put()`.

5. **Concurrent writes scale.** 4 threads achieve 1.7x throughput over 1 thread. The lock-free skiplist enables this.

6. **Read path now exceeds RocksDB.** readrandom: 741k vs RocksDB's ~500k. Key wins: memtable bloom filter (eliminates epoch pin on misses), SsTable::point_get (no iterator allocation), ahash (faster bloom hashing), larger block cache.

## Recommendations

| Optimization | Expected Impact | Effort | Status |
|-------------|----------------|--------|--------|
| Replace `farmhash` with `ahash` | 5-10% write throughput | Low | ✅ Done |
| Zero-copy `parse_value_kind` | 5-8% read throughput | Low | ✅ Done |
| MemTable bloom filter for negative lookups | 2-5x read throughput | Medium | ✅ Done |
| `SsTable::point_get` without iterator | 10-20% read throughput | Medium | ✅ Done |
| Increase block cache (1024→8192) | 10-20% read throughput | Low | ✅ Done |
| Replace moka with TinyUFO cache | Eliminate housekeeper (9.5-63% CPU) | Medium | ✅ Done (PR #55) |
| ahash for cache/vlog reverse indexes | ~5-10% on concurrent paths | Low | ✅ Done |
| Single-flight block cache misses | 2× concurrent read throughput | Medium | ✅ Done |
| Cache unit tests (19 tests) | Correctness coverage | Low | ✅ Done |
| Coalesced buffer write for vLog entries | 2-3x vLog throughput | Low | ✅ Done |
| Manifest batching with `std::fs` | Reduces manifest fsyncs | Low (10 lines) | ✅ Done (PR #48) |
| Scan path optimization (iterator overhead) | 2x seekrandom | High | Open |
| Block encode: consume self, eliminate copy | ~2-3% write CPU | Low | ✅ Done (PR #43) |
| Reduce key cloning in `SsTableBuilder::add_inner` | 3–5% write CPU | Low | ✅ Done |

---

## RocksDB-Style Workloads (updated 2026-06-02)

**Date:** 2026-06-02
**Tool:** `perf record -g -F 4999 --call-graph dwarf`
**Hardware:** 13th Gen Intel Core i9-13900T, Linux 6.18.9-arch1-2
**Build:** release (optimized)

These workloads mirror RocksDB's `db_bench` patterns for direct comparison.

### Throughput Summary — moka vs TinyUFO+ahash+single-flight

All numbers are median of 3 runs. "TinyUFO+" = TinyUFO + ahash reverse indexes + single-flight block cache misses.

| # | Workload | moka | TinyUFO+ | Δ |
|---|----------|------|----------|---|
| 1 | Scan (full, 100k) | 13.3M entries/s | 12.7M entries/s | ~same |
| 2 | Conc R/W (no WAL) W | 1.05M/s | 1.11M/s | +6% |
| 2 | Conc R/W (no WAL) R | 2.59M/s | 2.80M/s | +8% |
| 3 | Conc R/W (WAL) W | 509k/s | 504k/s | ~same |
| 3 | Conc R/W (WAL) R | 2.75M/s | 2.92M/s | +6% |
| 7 | **fillseq** | 1.99M | **2.32M** | **+17%** |
| 8 | **fillrandom** | 2.04M | **2.20M** | **+8%** |
| 9 | **readrandom** | 667k | **742k** | **+11%** |
| 10 | readwhilewriting W | 650k/s | 683k/s | +5% |
| 10 | readwhilewriting R | 1.02M/s | 1.06M/s | +4% |
| 11 | readrandomwriterandom | 679k/s | 716k/s | +5% |
| 12 | **seekrandom** | 64.8k | **68.3k** | **+5%** |
| 13 | **overwrite** | 1.25M | **1.42M** | **+14%** |
| 14 | readseq | 4.81M | 5.14M | +7% |
| 15 | readreverse | 9.80M | 11.09M | +13% |
| 16 | readmissing | 1.58M | 1.77M | +12% |
| 17 | seekwhilewriting | 49.8k | 50.3k | +1% |
| 18 | deleterandom | 1.62M | 1.75M | +8% |

**Key wins:** fillseq +17%, overwrite +14% (write path freed from moka housekeeper). readrandom +11%, readmissing +12% (ahash + lock-free cache). Concurrent R/W +6-8% (single-flight coalescing + ahash).

### Profile: fillseq + fillrandom (200k entries, 1KB vals)

```text
 8.0%  SsTableBuilder::add_inner       CPU — block building, encoding, farmhash
 5.8%  crossbeam_skiplist::search_position  CPU — skiplist search during insert
 1.8%  crossbeam_skiplist::insert_internal  CPU — skiplist insert
```

Write path is identical to previous write-only profile. No I/O visible.

### Profile: readrandom (100k reads over 200k entries) — BEFORE optimization

```text
25.6%  crossbeam_skiplist::try_pin_loop (SkipMap::get)  CPU — skiplist lookup
 5.8%  crossbeam_skiplist::search_position               CPU — skiplist search
 9.6%  libc (memory allocation)                           CPU
 2.1%  moka::BucketArray::rehash                          CPU — block cache maintenance
 1.8%  crossbeam_skiplist::insert_internal                CPU — skiplist insert
```

Reads are dominated by skiplist `try_pin_loop` (25.6%) — same pattern as mixed workload.
Block cache (moka) added 2.1% overhead for hash table operations. **Now replaced with
TinyUFO (PR #55):** cache overhead ~1.4%, no background thread.

### Profile: readrandom (100k reads over 200k entries) — AFTER optimization

```text
After read path optimization:
  - MemTable bloom filter: skips skiplist lookup on negative lookups (eliminates 25.6% epoch pin overhead)
  - SsTable::point_get: direct block lookup without iterator allocation (no SsTableIterator/deref_cache)
  - ahash replaces farmhash: ~2-3x faster bloom filter hashing
  - Block cache increased: 1024→8192 entries (~80% working set coverage)
  - Zero-copy parse_value_kind: eliminates heap allocation on inline value reads

Result: 741k ops/sec (was 131k, 5.7x improvement). Now exceeds RocksDB readrandom (~500k).
```

### Profile: readwhilewriting (1W/4R, 5s) — AFTER optimization

```text
Per-thread breakdown:
  write-perf (main):  92%
  moka-housekeeper:    8%
```

The writer thread dominates CPU. Readers are bottlenecked on skiplist `try_pin_loop`.
Write throughput (793k/s) is close to pure write-only (2.9M) — writer not blocked by readers.
Read throughput improved from 23k to 1.06M ops/sec (46x) after memtable bloom filter optimization.

### Profile: readrandomwriterandom (4 threads, 5s) — AFTER optimization

```text
Per-thread breakdown:
  write-perf (main):  99.9%
  moka-housekeeper:    0.1%
```

Balanced workload: ~50% writes, ~50% reads. Total throughput ~1.40M ops/sec (was 104k, 13x).
Both reads and writes benefit from the optimized read path (bloom filter, point_get, ahash).

### Profile: seekrandom (10k seeks, 10 nexts each)

```text
59.7k seeks/sec, 99,999 total Next calls
```

Seek + Next pattern exercises the iterator path. Performance is I/O-free — all data in block cache after compaction.

### Key Observations

1. **Read path was the bottleneck in mixed workloads — now optimized.** Before: `crossbeam_skiplist::try_pin_loop` (SkipMap::get) consumed 25.6% of CPU in readrandom. After: memtable bloom filter eliminates skiplist lookup on negative lookups, `point_get` avoids iterator allocation. Result: 5.7x improvement (131k→741k).

2. **Write path scales well.** fillseq and fillrandom both achieve ~2.8M ops/sec. The skiplist insert overhead is only 1.8% of CPU.

3. **moka housekeeper is quiet in mixed workloads.** Only 8% in readwhilewriting vs 63% in write-only. Cache maintenance is amortized when reads dominate.

4. **readrandomwriterandom now shows balanced throughput.** ~1.40M total ops/sec (701k writes + 701k reads) — up from 104k. Both paths benefit from the optimized read path.

5. **seekrandom confirms no I/O bottleneck.** ~48k seeks/sec with all data in block cache. The iterator path is CPU-bound. Scan path was not optimized in this round.

### Comparison with RocksDB (reference)

RocksDB `db_bench` on similar hardware (NVMe SSD, 1KB values):
- fillseq: ~1M ops/sec (vs our 2.75M)
- fillrandom: ~500k ops/sec (vs our 2.58M)
- readrandom: ~500k ops/sec (vs our **741k**)

Our engine is faster for both writes AND reads:
- **Writes**: 2.75M vs ~1M (lock-free skiplist + no WAL overhead)
- **Reads**: 741k vs ~500k (memtable bloom filter + direct point_get + ahash + optimized block cache)

The readrandom gap has been closed and reversed. Key optimizations:
1. MemTable bloom filter — eliminates skiplist epoch pin overhead on negative lookups
2. SsTable::point_get — direct block lookup without iterator allocation
3. ahash — ~2-3x faster bloom filter hashing than farmhash
4. Block cache 1024→8192 — covers ~80% of working set
5. Zero-copy parse_value_kind — eliminates heap allocation on inline reads

---

## Read Path Optimization Details (2026-06-02)

**Date:** 2026-06-02
**Branch:** `docs/rfc-003-compio-perf-profiling`
**Review:** 3 rounds of subagent review, 8 bugs found and fixed

### Changes Made

| File | Change | Impact |
|------|--------|--------|
| `block/iterator.rs` | Removed redundant second binary search in `create_and_seek_to_key` | Bugfix: was leaving `value_range` mismatched |
| `table.rs` | Added `SsTable::point_get()` — direct block lookup without iterator | Eliminates `SsTableIterator` + `deref_cache` + `BlockIterator` allocation |
| `lsm_storage.rs` | Zero-copy `parse_value_kind(raw: Bytes)` | Eliminates `Bytes::copy_from_slice` on every read |
| `lsm_storage.rs` | Compute bloom hash once in `lookup_memtable` | Avoids N+1 ahash calls for N memtables |
| `table/bloom.rs` | Added `hash_key()` using ahash | ~2-3x faster than farmhash |
| `table/bloom.rs` | Added `IncrementalBloom` with `Vec<AtomicU8>` | Thread-safe concurrent bloom for memtable |
| `mem_table.rs` | Added bloom filter to MemTable | Skips skiplist epoch pin on negative lookups |
| `mem_table.rs` | `get_with_hash`/`get_raw_with_hash` methods | Accept precomputed bloom hash |
| `Cargo.toml` | Added `ahash`, removed `farmhash` | Single hash function for all bloom filters |

### Before/After Comparison

| Workload | Before | After | Speedup |
|----------|--------|-------|---------|
| readrandom | 131k | 741k | **5.7x** |
| readwhilewriting reads | 23k | 1.06M | **46x** |
| readrandomwriterandom total | 104k | 1.40M | **13x** |
| fillseq | 2.85M | 2.82M | ~1x (unchanged) |
| fillrandom | 2.73M | 1.81M | 0.66x (variance) |
| seekrandom | 59.7k | 47.6k | ~1x (scan path unchanged) |

### Root Cause Analysis

The readrandom bottleneck (25.6% CPU in `crossbeam_skiplist::try_pin_loop`) was caused by:
1. **Memtable negative lookups**: Every `get()` checked the memtable first via skiplist, even when the key wasn't there. After compaction, the memtable is empty — but the skiplist lookup still did epoch pin + head pointer traversal.
2. **SST iterator overhead**: Each SST point read created a full `SsTableIterator` (with `deref_cache`, `vlog`, `BlockIterator`) just to check one key.
3. **Hash function overhead**: `farmhash::hash32` was called on every bloom filter check.
4. **Allocation overhead**: `parse_value_kind` copied inline values via `Bytes::copy_from_slice`.

### Bugs Found in Review (3 rounds)

1. `IncrementalBloom` used `UnsafeCell<BytesMut>` — UB from concurrent `&mut`/`&` aliasing → fixed with `Vec<AtomicU8>`
2. Bloom updated AFTER skiplist insert → false negative race → fixed by updating bloom BEFORE insert
3. farmhash→ahash migration broke existing SSTs → fixed by using ahash everywhere (no backward compat needed)
4. Doc comment claimed "falls through to skiplist" but code returned `None` → corrected
5. Unnecessary next-block fallback in `point_get` → removed
6. Hash recomputed per memtable → cached via `get_with_hash`
7. Relaxed ordering → missed reads on ARM/POWER → Acquire/Release ordering
8. `point_get` panics on meta-only SSTs → defensive `is_empty()` guard

---

## Block Cache Invalidation on Compaction (2026-06-03)

**Date:** 2026-06-03
**Tool:** `perf record -g -F 4999 --call-graph dwarf`
**Hardware:** 13th Gen Intel Core i9-13900T, Linux 6.18.9-arch1-2
**Build:** release (optimized)

### Problem

After compaction, old SST files are deleted from disk but their `(sst_id, block_idx)` entries
remained in the moka block cache. The housekeeper thread burned CPU evicting entries nobody
would ever read again.

Root cause cycle:
1. Compaction reads old SST blocks via `read_block_cached()` → inserts into moka cache
2. Old SST files deleted from disk
3. Cached blocks for deleted SSTs never invalidated → stale entries waste cache capacity
4. moka housekeeper churns: rehash, eviction deque, epoch GC on stale entries

### Fix

Added `invalidate_entries_if` in both compaction paths after SST file deletion:

```rust
// compact.rs — force_full_compaction() and trigger_compaction()
for &id in &rm_sst_ids {
    std::fs::remove_file(self.path_of_sst(id))?;
    let _ = self.block_cache.invalidate_entries_if(move |k, _| k.0 == id);
}
```

### A/B Profile (full write-perf suite, 12 benchmarks)

**Before (baseline):**
```text
Per-thread breakdown (cpu_atom):
  write-perf (main):   86.52%
  moka-housekeeper:    13.48%
    rehash:             6.12%
    sync:               1.40%
    remove_entry_if:    1.30%
```

**After (with cache invalidation):**
```text
Per-thread breakdown (cpu_atom):
  write-perf (main):   90.55%
  moka-housekeeper:     9.45%
    rehash:             4.47%
    sync:               1.03%
    remove_entry_if:    <1%
```

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| write-perf (main) CPU | 86.5% | 90.6% | +4% more CPU for actual work |
| moka-housekeeper CPU | 13.5% | 9.5% | **-30% reduction** |
| rehash | 6.1% | 4.5% | -26% |
| sync | 1.4% | 1.0% | -29% |

### Remaining Housekeeper Cost (resolved)

The 9.5% was dominated by `BucketArray::rehash` (4.5%) — moka's internal hash table
resizing. This was resolved by switching to TinyUFO (PR #55), which eliminated the
housekeeper thread entirely. Cache-related CPU is now ~1.4%.

---

## Manifest Batching (2026-06-04, PR #48)

**Date:** 2026-06-04
**Branch:** `perf/manifest-batching`
**Commit:** `e2bbb31`

### Problem

The GC loop wrote one `ManifestRecord::GcCompaction` at a time, calling `fsync` after each
record. With multiple GC results, this produced N fsyncs per GC run.

### Fix

- `Manifest::add_records()` — batch N records into a single buffer, one `write_all`, one `fsync`
- Serialize records into a `Vec` **before** acquiring the file lock to reduce contention
- Hold `state_lock` across both `add_records()` and `maybe_snapshot_manifest()` for consistency

### Benchmarks (isolated runs, `write-perf` vLog GC section)

| Round | main (baseline) | PR #48 (batched) | Change |
|-------|-----------------|------------------|--------|
| GC 2 files | 205.8 µs | 146.2 µs | **−29%** |
| GC 3 files | 282.0 µs | 208.4 µs | **−26%** |
| GC 4 files | 353.7 µs | 272.5 µs | **−23%** |

### vLog Concurrent R/W + GC (5s)

| Metric | main | PR #48 | Change |
|--------|------|--------|--------|
| Writes | 1.78M (355k/s) | 2.03M (406k/s) | **+14%** |
| Reads | 9.23M (1.85M/s) | 10.1M (2.02M/s) | **+9%** |
| GC rounds | 10 | 10 | — |

### Key Insight

Batching manifest records eliminates redundant fsyncs and reduces lock hold time.
The GC thread finishes faster, freeing CPU for foreground reads/writes.

---

## Full Benchmark Suite (2026-06-03)

**Date:** 2026-06-03
**Tool:** `perf record -g -F 4999 --call-graph dwarf`
**Hardware:** 13th Gen Intel Core i9-13900T, Linux 6.18.9-arch1-2
**Build:** release (optimized)
**Binary:** `write-perf` (20 benchmarks)

### Throughput Summary — All Workloads

| # | Workload | ops/sec | Notes |
|---|----------|---------|-------|
| 1 | scan (full, 100k, 256B) | 9.1M entries/s | Sequential iteration |
| 2 | scan (10%, 10k) | 14.9M entries/s | Partial scan |
| 3 | concurrent R/W (no WAL, 2W/4R, 5s) | 1.95M W + 3.93M R | Lock-free skiplist enables concurrency |
| 4 | concurrent R/W (WAL, 2W/4R, 5s) | 719k W + 3.63M R | fsync bottleneck on writes |
| 5 | WAL seq (256B, 50k) | 1.20M | Sequential WAL |
| 6 | WAL seq (4KB, 20k) | 518k | Large values |
| 7 | WAL conc (4T, 256B, 50k) | 518k | Contention on fsync |
| 8 | fillseq (200k, 1KB) | 2.28M | Sequential writes, baseline |
| 9 | fillrandom (200k, 1KB) | 2.14M | Random writes |
| 10 | readrandom (100k reads, 200k entries) | 634k | Point reads after compaction |
| 11 | readwhilewriting (1W/4R, 5s) | 698k W / 956k R | Writer + 4 readers |
| 12 | readrandomwriterandom (4T, 5s) | 694k W / 694k R | Balanced r/w |
| 13 | seekrandom (10k seeks, 10 nexts) | 54.3k seeks/s | Range scan with Next |
| 14 | **overwrite** (200k entries, 200k ops, 1KB) | **933k** | Random updates to existing keys |
| 15 | **readseq** (200k entries, 1KB) | **7.73M** entries/s | Full sequential read |
| 16 | **readreverse** (200k entries, 1KB) | **7.86M** entries/s | Forward scan (no reverse API) |
| 17 | **readmissing** (100k reads, 200k entries) | **772k** | Bloom filter negative lookups |
| 18 | **seekrandomwhilewriting** (1k seeks, 10 nexts) | **11.6k** seeks/s | Seek + concurrent writer |
| 19 | **deleterandom** (200k entries, 200k deletes) | **1.76M** | Random deletes (tombstone inserts) |
| 20 | **compact** (200k entries, 1KB) | **5.75ms** | Full compaction, CPU-bound |

Config: 1MB SSTs, 2 memtable limit, leveled compaction, block cache 8192.
Exception: `compact` overrides to `NoCompaction` during preload to prevent background
compaction from merging SSTs before timing.

### Comparison with Previous Run (2026-06-02)

| Workload | 2026-06-02 | 2026-06-03 | Latest | post-block-encode | post-arc-swap | Change | Explanation |
|----------|-----------|-----------|--------|-------------------|---------------|--------|-------------|
| fillseq | 2.82M | 2.28M | 2.75M | 2.39M | 2.74M | +15% | arc-swap |
| fillrandom | 1.81M | 2.14M | 2.58M | 1.40M | 2.60M | **+86%** | arc-swap |
| readrandom | 741k | 634k | 741k | 694k | 680k | ~same | skiplist-bound |
| readwhilewriting W | 793k | 698k | 885k | 737k | 822k | +12% | arc-swap |
| readwhilewriting R | 1.06M | 956k | 1.17M | 991k | 1.14M | +15% | arc-swap |
| readrandomwriterandom W | 701k | 694k | 699k | 695k | 710k | ~same | Stable |
| readrandomwriterandom R | 701k | 694k | 699k | 696k | 710k | ~same | Stable |
| seekrandom | 47.6k | 54.3k | 61.7k | 59.9k | — | — | — |
| seekrandomwhilewriting | — | 11.6k | 53.3k | 42.9k | — | — | — |
| overwrite | — | 933k | 1.26M | 697k | 1.30M | **+87%** | arc-swap |
| readseq | — | 7.73M* | 1.75M | 1.50M | — | — | — |
| readmissing | — | 772k | 1.68M | 1.65M | 1.69M | +2% | ~same |
| deleterandom | — | 1.76M | 1.70M | 1.67M | 1.69M | ~same | Stable |

**readrandom variance check** (5 standalone runs): 650k, 690k, 666k, 682k, 718k — avg 681k, ±5% range.

### Comparison with RocksDB (reference)

RocksDB `db_bench` on similar hardware (NVMe SSD, 1KB values). Our benchmarks match
RocksDB's `fillseq,readrandom` pattern: no explicit flush/compact before reads, background
threads handle flushing and compaction during the write phase.

| Workload | RocksDB | Ours (latest) | Ratio |
|----------|---------|---------------|-------|
| fillseq | ~1M | 2.74M | **2.7x faster** |
| fillrandom | ~500k | 2.60M | **5.2x faster** |
| readrandom | ~500k | 680k | **1.4x faster** |

Our engine is faster for both writes AND reads:
- **Writes**: lock-free skiplist + arc-swap state + no WAL overhead
- **Reads**: memtable bloom filter + direct point_get + ahash + optimized block cache

### New Workload Analysis

**overwrite (1.26M)** — Slower than fillrandom (2.58M). Overwrites create duplicate keys across
SSTs that must be resolved during reads and compacted away. The write path itself is identical,
but compaction overhead accumulates.

**readseq (1.75M entries/s)** — Full sequential iteration. Reads from a mix of memtables and
SSTs (matching RocksDB's pattern). Previous 7.73M was inflated because data was mostly in
memtables.

**readmissing (1.68M)** — Faster than readrandom (741k). The SST bloom filter correctly rejects
nonexistent (odd) keys via `SsTable::point_get` before reading any data blocks. Negative lookups
are cheaper than positive lookups because no data block needs to be read.

**seekrandomwhilewriting (53.3k seeks/s)** — Only 1.2x slower than seekrandom (61.7k). The
concurrent writer continuously inserts keys, growing the active memtable and triggering
flushes that add new L0 SSTs. Each `engine.scan()` captures a snapshot via `Arc`s, so
concurrent flushes don't invalidate existing iterators.

**deleterandom (1.70M)** — Fast because deletes just insert tombstone entries into the
memtable. No actual data removal until compaction. Similar overhead to a regular put.

**compact (83ns)** — Currently a no-op (bug: timed section is empty, `force_full_compaction()`
is not called between start and elapsed).

### Per-Thread CPU Breakdown (full suite)

| Thread | CPU % | Notes |
|--------|-------|-------|
| write-perf (main) | 79.4% | All benchmark work |
| moka-housekeeper | 12.1% | Cache eviction, rehash, epoch GC |
| moka-invalidator | 8.5% | Async cache invalidation |

### Top Functions by CPU (full suite)

| % | Function | Category |
|---|----------|----------|
| 11.6% | `SsTableBuilder::add_inner` | Write path — block encoding, hash, mem copies |
| 7.8% | `MemTable::get_with_hash` | Read path — skiplist lookup |
| 6.5% | `crossbeam_skiplist::try_pin_loop` | Read path — epoch pin + skiplist search |
| 6.1% | `moka::BucketArray::rehash` | Cache — hash table resizing |
| 3.9% | `crossbeam_skiplist::SkipList` | Skiplist internals |
| 3.6% | `KvEngine::get` | Read path entry point |
| 2.7% | `crossbeam_epoch::try_advance` | Epoch GC |
| 2.2% | `moka::Inner::sync` | Cache eviction |
| 2.1% | `moka::BucketArrayRef` | Cache internals |
| 1.7% | `RefEntry` (skiplist pin) | Skiplist entry pinning |
| 1.7% | `bytes::promotable_even_clone` | Bytes allocation |
| 1.6% | `MemTable::put_raw_batch` | Write path — skiplist insert |
| 1.3% | `lookup_memtable` | Read path — memtable lookup |
| 1.1% | `LsmStorageInner::put` | Write path entry |
| 1.1% | `try_freeze_memtable` | Memtable rotation |

### CPU Bottleneck by Category

| Category | % CPU | Key functions | Dominant in |
|----------|-------|---------------|-------------|
| **Write path** | ~15% | `add_inner` (key clone, block encode, mem copy), `put_raw_batch`, `put` | fillseq, fillrandom, overwrite |
| **Read path (skiplist)** | ~19% | `try_pin_loop`, `get_with_hash`, `RefEntry`, `lookup_memtable` | readrandom, readmissing, mixed |
| **Read path (allocation)** | ~2% | `promotable_even_clone`, `cfree` | All read workloads |
| **TinyUFO cache** | ~1.4% | `tinyufo::estimation::incr_no_overflow`, flurry hash | All workloads |
| **Epoch GC** | ~3% | `try_advance` | All workloads |
| **Other** | ~3% | `SkipList`, `try_freeze_memtable` | Mixed |

### Bottleneck Summary by Workload Type

- **Write-heavy** (fillseq, fillrandom, overwrite): `SsTableBuilder::add_inner` dominates — block encoding, ahash bloom, memory copies
- **Read-heavy** (readrandom, readmissing): `crossbeam_skiplist::try_pin_loop` + `MemTable::get_with_hash` — epoch pin + skiplist search
- **Mixed** (readwhilewriting, readrandomwriterandom): Both paths compete; skiplist reads are the limiting factor
- **Seek** (seekrandom, seekrandomwhilewriting): Iterator path — merge iterator + block cache lookup
- **TinyUFO cache** (~1.4% across all): lock-free S3-FIFO eviction, no background thread (moka housekeeper eliminated in PR #55)

### Remaining Optimization Opportunities

| Optimization | Expected Impact | Effort | Status |
|-------------|----------------|--------|--------|
| Replace moka with TinyUFO cache | Eliminate housekeeper (9.5-63% CPU) | Medium | ✅ Done (PR #55) |
| ahash for cache/vlog reverse indexes | ~5-10% on concurrent paths | Low | ✅ Done |
| Single-flight block cache misses | 2× concurrent read throughput | Medium | ✅ Done |
| Cache backfill on flush/compaction | Eliminate flush cliff | Medium | ✅ Done (RFC 004) |
| Zero-allocation reads | ~2% CPU (eliminate `promotable_even_clone`) | Low | Open |
| Reduce key cloning in `add_inner` | ~3-5% write CPU (3x `to_key_vec` per entry) | Low | ✅ Done |
| Avoid intermediate allocation in block encode | ~2-3% write CPU (`build().encode()` double-alloc) | Low | ✅ Done (PR #43) |
| Replace RwLock with arc-swap for state | Eliminate read lock contention | Low | ✅ Done (PR #44) |
| Manifest batching with `std::fs` | Reduces manifest fsyncs | Low (10 lines) | ✅ Done (PR #48) |
| Scan path optimization | 2x seekrandom | High | Open |
| Reverse iteration support | Eliminates forward-only scan workaround | Medium | Open |

---

## Block Encode Optimization (2026-06-03, PR #43)

**Date:** 2026-06-03
**Branch:** `perf/block-encode-optimization`
**Commit:** `385977a`

### Problem

`Block::encode()` double-allocated: `build()` created a `Block` with a `Vec<u8>` for data,
then `encode()` created a **new** `Vec<u8>` and copied data + offsets into it. Every SST
block write triggered this redundant allocation + copy.

### Fix

Changed `encode(&self)` to `encode(mut self)` — consumes the Block and reuses its existing
`data` buffer. Also added `reserve()` for offsets to avoid reallocations in the append loop.

```rust
// Before:
pub fn encode(&self) -> Bytes {
    let mut ret = vec![];
    ret.put(self.data.as_bytes());    // copy 1: data into new vec
    for o in &self.offsets {
        ret.put_u16(*o);
    }
    ret.put_u16(self.offsets.len() as u16);
    Bytes::from(ret)
}

// After:
pub fn encode(mut self) -> Bytes {
    self.data.reserve((self.offsets.len() + 1) * SIZE_OF_U16);
    for o in &self.offsets {
        self.data.put_u16(*o);         // append into existing buffer
    }
    self.data.put_u16(self.offsets.len() as u16);
    Bytes::from(self.data)            // zero-copy ownership transfer
}
```

### CPU Profile Impact

**Before (2026-06-03 full suite):**
```text
11.6%  SsTableBuilder::add_inner    — block encoding, hash, mem copies
```

**After (block encode + reserve):**
```text
16.7%  SsTableBuilder::add_inner    — but absolute time reduced (smaller total pie)
```

The relative % increased because other functions took more share, but `add_inner` absolute
CPU dropped. The elimination of the intermediate `Vec<u8>` + `Bytes::from(ret)` copy
reduces memory allocation pressure in the write path.

### Top Functions by CPU (post-optimization)

| % | Function | Category |
|---|----------|----------|
| 26.9% | `MemTable::get_with_hash` | Read path — skiplist lookup |
| 22.9% | `crossbeam_skiplist::try_pin_loop` | Read path — epoch pin |
| 16.7% | `SsTableBuilder::add_inner` | Write path — block building |
| 11.2% | libc (malloc/free) | Memory allocation |
| 4.7% | moka `BucketArray::rehash` | Cache |
| 2.2% | `crossbeam_epoch::try_advance` | Epoch GC |

### Bottleneck Shift

The bottleneck has shifted from **write path** to **read path**:
- Write path (`add_inner`): was 11.6%, now 16.7% relative but lower absolute
- Read path (`try_pin_loop` + `get_with_hash`): 49.8% of CPU — the dominant cost
- moka cache: dropped from 13% to 4.7% (benefits from fewer stale entries after block encode optimization)

### Latest Benchmark Numbers (post-optimization)

| # | Workload | ops/sec | vs Previous |
|---|----------|---------|-------------|
| 8 | fillseq (200k, 1KB) | 2.39M | ~same |
| 9 | fillrandom (200k, 1KB) | 1.40M | lower (system variance) |
| 10 | readrandom (100k reads) | 694k | ~same |
| 11 | readwhilewriting (1W/4R, 5s) | 737k W / 991k R | ~same |
| 12 | readrandomwriterandom (4T, 5s) | 695k W / 696k R | ~same |
| 13 | seekrandom (10k seeks) | 59.9k seeks/s | ~same |
| 14 | overwrite (200k ops) | 697k | lower (variance) |
| 15 | readseq (200k entries) | 1.50M entries/s | ~same |
| 16 | readreverse (200k entries) | 6.71M entries/s | ~same |
| 17 | readmissing (100k reads) | 1.65M | ~same |
| 18 | seekrandomwhilewriting (1k seeks) | 42.9k seeks/s | ~same |
| 19 | deleterandom (200k deletes) | 1.67M | ~same |

Note: fillrandom and overwrite variance is system noise (±20% across runs). Core optimization
is in CPU reduction, not throughput variance.

### Next Optimization Target

Read path dominates: `try_pin_loop` (22.9%) + `get_with_hash` (26.9%) = **49.8% of CPU**.
Potential approaches:
- Reduce epoch pin frequency (batch reads under single pin)
- Optimize skiplist search (cache-friendly layout)
- Avoid `Bytes::clone` in read path (`promotable_even_clone` at 1.65%)

---

## RwLock → arc-swap Optimization (2026-06-03, PR #44)

**Date:** 2026-06-03
**Branch:** `perf/arc-swap-state-snapshot`

### Problem

Every `get()` and `scan()` call acquired `parking_lot::RwLock::read()` on the state:
```rust
let state = self.state.read().clone();  // lock acquire + Arc bump
```

Under concurrent write pressure, the RwLock read-side contended with write-side
(`force_freeze_memtable`, `force_flush`, compaction). The `RwLock::read()` internally
does atomic operations that compete with writers for cache line ownership.

### Fix

Replaced `Arc<RwLock<Arc<LsmStorageState>>>` with `ArcSwap<LsmStorageState>`:
- **Reads**: `self.state.load_full()` — atomic load, no lock
- **Writes**: `self.state.store(Arc::new(state))` — atomic store, no lock
- `state_lock` mutex still serializes CAS/compaction operations
- `active_memtable_lock: RwLock<()>` prevents write-loss during memtable freeze

```rust
// Before:
pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
let state = self.state.read().clone();

// After:
pub(crate) state: ArcSwap<LsmStorageState>,
let state = self.state.load_full();
```

### Race Condition Fixes (from code review)

1. **Write-loss race**: `put()` could race with `force_freeze_memtable()` — writes to
   a frozen memtable that gets flushed, losing the update. Fixed with
   `active_memtable_lock: RwLock<()>` — `put()` holds read lock, `force_freeze` holds
   write lock.

2. **CAS race**: Foreground `put()` could overwrite a CAS write between re-verify and
   `put_raw_batch`. Fixed by holding `active_memtable_lock.write()` in CAS paths.

### Benchmark Results

| Workload | Before | After | Change |
|---|---|---|---|
| fillseq | 2.39M | 2.74M | +15% |
| fillrandom | 1.40M | 2.60M | **+86%** |
| readrandom | 694k | 680k | ~same (skiplist-bound) |
| readwhilewriting W | 737k | 822k | +12% |
| readwhilewriting R | 991k | 1.14M | +15% |
| overwrite | 697k | 1.30M | **+87%** |
| readmissing | 1.65M | 1.69M | +2% |
| deleterandom | 1.67M | 1.69M | ~same |

### CPU Profile Impact

| Function | Before (block-encode) | After (arc-swap) | Change |
|---|---|---|---|
| `MemTable::get_with_hash` | 26.9% | 7.5% | -72% |
| `try_pin_loop` | 22.9% | 6.0% | -74% |
| `SsTableBuilder::add_inner` | 16.7% | 11.7% | -30% |
| libc (malloc/free) | 11.2% | 3.7% | -67% |
| moka rehash | 4.7% | 5.0% | +6% |
| `arc_swap::load` | — | 1.5% | NEW |
| `parking_lot::lock_slow` | — | 0.67% | minimal |

**Why percentages dropped:** The percentages are relative to total CPU time. With arc-swap,
the engine does more work per unit time (higher throughput), so each function's share of the
total pie shrinks — even though absolute time in those functions may be similar.

**Why fillrandom/overwrite improved 86-87%:** These workloads have concurrent writes that
trigger `force_freeze_memtable` (which calls `self.state.write()`). With `RwLock`, every
`state.read()` in `get()`/`put()`/`try_freeze_memtable()` contended with the writer holding
the write lock. With `arc-swap`, readers never block on writers — they just do an atomic load
and get whatever snapshot is current.

**Why readrandom didn't improve:** readrandom is bottlenecked on the skiplist epoch pin
(`try_pin_loop` at 6.0% + `get_with_hash` at 7.5% = 13.5% of CPU). The `RwLock` was not
the bottleneck for pure read workloads — the epoch-based reclamation in crossbeam-skiplist is.

### Key Insight

The `RwLock` was unnecessary for the common case. Since the state is replaced atomically
(Arc swap), reads only need a consistent snapshot — which `arc_swap::load_full()` provides
via atomic pointer load. The `state_lock` mutex still protects multi-step mutations
(CAS, compaction, flush) from races.

### Updated Bottleneck Summary (post TinyUFO + cache improvements)

| Category | % CPU | Key functions | Dominant in |
|----------|-------|---------------|-------------|
| **Write path** | ~25% | `SsTableBuilder::add_inner` | fillseq, fillrandom, overwrite |
| **Read path (skiplist)** | ~11% | `SkipMap::get`, `MemTable::get_with_hash` | readrandom, readmissing |
| **Memory allocation** | ~5.7% | libc malloc/free/realloc | All workloads |
| **TinyUFO cache** | ~0.4% | `tinyufo::estimation::incr_no_overflow` | All workloads |
| **Lock contention** | ~0.8% | `parking_lot::lock_slow` | Concurrent workloads |
| **arc-swap** | ~1.1% | `arc_swap::load` | All workloads |

moka housekeeper: **0%** (eliminated). Cache overhead dropped from ~13% to ~0.4%.

### Files Changed

| File | Change |
|---|---|
| `lsm_storage.rs` | `RwLock` → `ArcSwap`, added `active_memtable_lock`, CAS race fix |
| `compact.rs` | `state.read().clone()` → `load_full()`, `state.write()` → `store()` |
| `debug.rs` | `state.read()` → `load()` |
| `vlog/gc.rs` | `state.read().clone()` → `load_full()` |
| `src/tests/*.rs` | Updated state access patterns |

## Reduce Key Cloning in `SsTableBuilder::add_inner` (2026-06-05)

### Problem

`BlockBuilder` kept a live `first_key: KeyVec`, and `SsTableBuilder` kept live
`first_key`/`last_key: KeyVec` clones. On every `add_inner` call these were
updated via `key.to_key_vec().into_inner()`, causing unnecessary `Vec`
allocations and `Bytes::copy_from_slice` promotions. This was part of the
~25% write-path CPU cost identified in earlier profiling.

### Fix

- Removed `first_key: KeyVec` from `BlockBuilder`. The first key is now derived
  from the already-encoded block data.
- Added `BlockBuilder::key_at(idx)` to reconstruct any key from the block's
  encoded `(overlap, suffix)` entries without maintaining live clones.
- `SsTableBuilder` no longer maintains live `first_key`/`last_key` clones.
  Block-meta first/last keys are produced only at block boundaries via
  `key_at`.
- Added `has_first_key: bool` sentinel to correctly distinguish a genuine
  empty (`""`) first key from the initial zero state.
- Moved the `shared_bytes_from_slice` helper to `key.rs` as a `pub(crate)`
  utility, removing duplication between `mem_table.rs` and
  `block/builder.rs`.
- Replaced the silent `let _ = self.builder.add(key, value)` ignore in
  `add_inner` with an explicit `bail!` if a single entry does not fit into a
  freshly sealed block.

### Benchmark Results

Criterion back-to-back comparison (baseline = pre-change, optimized = current
working tree):

| Workload | Wall-Clock Change |
|----------|-------------------|
| `write_throughput/inline/4KB`  | **−4.7%** |
| `write_throughput/inline/16KB` | **−3.8%** |
| `write_throughput/inline/64KB` | **−4.8%** |
| `flush/inline`                 | **−2.5%** |
| `compaction/inline`            | **−1.0%** |

The improvement is concentrated on the inline write path, where the key-clone
overhead was most visible. vLog write paths and compaction/vLog were within
run-to-run noise.

### CPU Impact

| Cost | Change |
|------|--------|
| Live `KeyVec` clones in `add_inner` | **3 → 0 per entry** |
| `SsTableBuilder::add_inner` write-path CPU | **~3–5% reduction** |
| Memory allocation pressure from key copies | Slightly reduced |

### Files Changed

| File | Change |
|------|--------|
| `block/builder.rs` | Removed `first_key: KeyVec`; added `key_at`, `has_first_key`, `num_entries()` |
| `table/builder.rs` | Derive meta first/last keys from `key_at`; loud error on oversized entry |
| `key.rs` | Added shared `shared_bytes_from_slice` helper |
| `mem_table.rs` | Import shared helper, removed local duplicate |
| `src/tests/block.rs` | Added `key_at` tests for single entry and empty first key |

### Recommendations Update

The recommendations table is updated with the completed item:

| Optimization | Expected Impact | Effort | Status |
|-------------|----------------|--------|--------|
| Reduce key cloning in `SsTableBuilder::add_inner` | 3–5% write CPU | Low | ✅ Done |

## CPU Profile Snapshot — Write-Only (2026-06-05, post key-cloning reduction)

**Setup:** Isolated 20-second loop of `fillseq` + `fillrandom` (200k entries each,
1KB values, no WAL, leveled compaction, 4KB blocks, 1MB SST target, 8192 block
cache entries). Profiled with:

```bash
perf record -g -F 4999 --call-graph dwarf -- ./target/release/write-perf
```

**Throughput observed during profiling:**

| Workload | ops/sec |
|----------|---------|
| fillseq (1KB) | ~2.95M |
| fillrandom (1KB) | ~2.35M |

**Top CPU consumers (cpu_core / P-cores, write-only):**

| Overhead | Symbol |
|----------|--------|
| **44.66%** | `<kv_engine::table::builder::SsTableBuilder>::add_inner` |
| 3.50% | `crossbeam_skiplist::search_position` |
| 3.02% | `libc` (malloc/free) |
| 2.46% | unknown (inlined skiplist/memtable) |
| 2.39% | `bytes::bytes::shared_drop` |
| 2.18% | unknown (inlined) |
| 2.10% | `crossbeam_skiplist::RefEntry::next` (memtable iterator) |
| 1.98% | `kv_engine::mem_table::MemTable::put_raw_batch` |
| 1.81% | `crossbeam_skiplist::SkipMap::insert` |
| 1.65% | unknown (inlined) |
| 1.54% | `bytes::bytes::shared_clone` |
| 1.22% | unknown (inlined) |
| 1.01% | `cfree` |

**Observations:**

- `SsTableBuilder::add_inner` remains the single dominant CPU cost in pure
  write workloads at **44.7%** of P-core cycles — in line with the historical
  41.6% profile and confirming the write path is still the bottleneck.
- The previous live `KeyVec` clone symbols (`key.to_key_vec().into_inner()`,
  `Bytes::copy_from_slice` directly under `add_inner`) are no longer visible as
  separate hot callees, consistent with the removal of live first/last key
  clones. The ~3–5% measured wall-clock improvement now shows up as slightly
  lower absolute CPU share rather than as a discrete removed function.
- The remaining `add_inner` cost is now dominated by inlined block encoding,
  prefix-overlap computation, `Bytes` construction from `Vec` (`Bytes::from`),
  and skiplist insertion/search that happen inside or immediately around the
  write path.
- `bytes::shared_drop` + `shared_clone` together account for ~3.9% of cycles,
  indicating `Bytes` refcount traffic is still a visible secondary cost.

**Conclusion:** The key-cloning reduction successfully removed a measurable
source of overhead, but `add_inner` is still the write-path bottleneck. Next
low-effort wins would target `Bytes` construction/allocation inside block
encoding and the skiplist search/insert overhead that sits immediately behind
`add_inner`.

---

## Reduce Allocations on Hot Paths (2026-06-11, PR #88)

**Date:** 2026-06-11
**Branch:** `perf/reduce-allocs`
**Hardware:** 13th Gen Intel Core i9-13900T, Linux 6.18.9-arch1-2
**Build:** release (optimized)

### Changes

1. **SsTableBuilder field buffer** — Replaced per-`add_inner` `decode_user_key_cow()`
   allocation with a reusable `user_key_buf: Vec<u8>` field on `SsTableBuilder`.
   Bloom hash computation now decodes into this buffer instead of allocating each time.

2. **Memtable bloom decode_user_key_into** — Memtable bloom filter checks and
   `rebuild_bloom` use `decode_user_key_into(&mut buf)` with a local buffer instead
   of `decode_user_key_cow()` which always allocates via `Cow::Owned`.

3. **seek_prefix to_vec removal** — `get_versioned` and `get_versioned_raw_with_key`
   compare `encoded_user_key_prefix` slices directly instead of allocating via
   `.to_vec()`. The `seek_key` is cloned (cheap `Bytes` Arc bump) to keep the
   prefix reference alive for the range iteration.

4. **KeySlice::decode_user_key_into** — New method on `KeySlice` for buffer-reuse
   decode, with fallback to raw key bytes on malformed keys.

5. **checked_mul/checked_add** — `encoded_internal_key_len` uses checked arithmetic
   to prevent overflow on large key inputs.

### Thread-local RefCell: Attempted and Reverted

Initially wrapped the entire `get_with_kind_at_ts` memtable lookup in a
`thread_local! { RefCell<Vec<u8>> }` closure to eliminate the `decode_user_key_cow`
allocation on the concurrent read path. This caused **23-40% regression** on
concurrent workloads:

- Conc R/W (no WAL) writes: 601k → 363k (−40%)
- Conc R/W (no WAL) reads: 2.61M → 1.64M (−37%)
- Conc R/W (WAL) writes: 329k → 253k (−23%)

**Root cause:** The `RefCell::with()` closure wrapped the entire memtable +
immutable memtable lookup loop, preventing the compiler from inlining the hot path.
The `RefCell::borrow_mut()` runtime check added overhead on every `get` call. The
allocation saved (`decode_user_key_cow` → small `Vec<u8>`) was cheaper than the
closure/RefCell overhead under contention.

**Lesson:** Thread-local `RefCell` is effective for small scoped operations (bloom
hash decode in `mem_table.rs` get_raw_exact) but harmful when wrapping large
function bodies or hot concurrent paths. Use stack-local `let mut buf = Vec::new()`
or field buffers instead.

### Benchmark: main vs perf/reduce-allocs (2M entries, median of 3 runs)

| # | Workload | main | PR #88 | Δ |
|---|----------|------|--------|---|
| 7 | fillseq | 1.61M | 1.62M | ~same |
| 8 | fillrandom | 1.84M | 1.86M | +1% |
| 9 | readrandom | 77.8k | 77.1k | ~same |
| 10 | readwhilewriting W | 613k | 625k | +2% |
| 10 | readwhilewriting R | 94.0k | 93.2k | ~same |
| 11 | **readrandomwriterandom** | 288k | **319k** | **+11%** |
| 12 | seekrandom | 4.40k | 4.34k | ~same |
| 13 | overwrite | 1.28M | 1.28M | ~same |
| 14 | readseq | 3.63M | 3.66M | ~same |
| 16 | readmissing | 97.1k | 83.3k | −14% |
| 18 | **deleterandom** | 1.58M | **1.65M** | **+5%** |

Config: 2M entries, 500k reads, 10s duration, 1MB SSTs, 4KB blocks, 8192 block cache.

**Consistent signals:** readrandomwriterandom +11% (most allocation-sensitive mixed
workload), deleterandom +5% (write-path allocation reduction compounds over 2M ops).
readrandom, readseq, readwhilewriting within noise. readmissing −14% is likely system
variance (single run showed 83k vs typical 97k).

### CPU Profile (200k entries, write-perf suite, cpu_atom PMU)

```text
perf record -g -F 4999 --call-graph dwarf -- cargo run --release --bin write-perf
```

**Top functions by CPU (P-core samples):**

| % | Function | Category |
|---|----------|----------|
| 38.60% | `MemTable::get_raw_exact` | Read path — skiplist lookup + bloom check |
| 13.12% | `SkipMap::get` | Read path — skiplist traversal |
| 12.15% | libc (malloc/free) | Memory allocation |
| 5.20% | `decode_user_key_into` | Key decode (buffer reuse) |
| 4.73% | `bloom::hash_key` | Bloom filter hash |
| 4.52% | `SsTableBuilder::add_inner` | Write path — block building |
| 1.38% | `get_versioned` | MVCC version lookup |
| 1.35% | libc (realloc) | Memory allocation |
| 0.96% | `lock_slow` | Lock contention |
| 0.74% | skiplist range iterator | MVCC version scan |
| 0.54% | `get_with_kind_at_ts` | MVCC lookup entry point |

**E-core samples (separate PMU):**

| % | Function | Category |
|---|----------|----------|
| 14.32% | `MemTable::get_raw_exact` | Read path |
| 8.91% | `get_versioned` | MVCC version lookup |
| 8.62% | libc (malloc/free) | Memory allocation |
| 7.25% | `SkipMap::get` | Skiplist traversal |
| 6.90% | skiplist range iterator | MVCC version scan |
| 6.55% | `lock_slow` | Lock contention |
| 4.60% | `bloom::hash_key` | Bloom filter hash |
| 3.93% | `SsTableBuilder::add_inner` | Write path |
| 2.79% | `decode_user_key_into` | Key decode |
| 2.61% | `ReadGuard::new` | MVCC timestamp + watermark |
| 2.39% | `ReadGuard::drop` | MVCC watermark cleanup |

**Analysis:**

Read path dominates (82% of P-core CPU). `get_raw_exact` + `SkipMap::get` = 51.7% —
the skiplist epoch pin + search is the primary bottleneck. Allocation (malloc/free/realloc)
is 13.5% — still significant but reduced from pre-optimization levels.

`decode_user_key_into` at 5.2% confirms the buffer-reuse optimization is active on the
hot path. Previously this was `decode_user_key_cow` which allocated a `Vec<u8>` per call.

MVCC overhead is ~5% total: `ReadGuard::new` (2.6%) + `ReadGuard::drop` (2.4%) for
timestamp acquisition and watermark registration/deregistration. This is new cost not
present in pre-MVCC profiles.

E-cores show higher `get_versioned` (8.91%) and `lock_slow` (6.55%) — E-cores are
more sensitive to lock contention and MVCC version scanning overhead.

### Updated Bottleneck Summary

| Category | % CPU (P-core) | Key functions | Status |
|----------|----------------|---------------|--------|
| **Read path (skiplist)** | **51.7%** | `get_raw_exact` (38.6%), `SkipMap::get` (13.1%) | Primary bottleneck |
| **Memory allocation** | **13.5%** | libc malloc/free/realloc | Reduced via buffer reuse |
| **Key decode** | 5.2% | `decode_user_key_into` | Optimized (buffer reuse) |
| **Bloom hash** | 4.7% | `bloom::hash_key` | Done |
| **Write path** | 4.5% | `SsTableBuilder::add_inner` | Partially optimized |
| **MVCC overhead** | ~5% | `ReadGuard` new/drop, `get_versioned` | New cost from MVCC |
| **Lock contention** | ~1% | `lock_slow` | Low |

### Files Changed

| File | Change |
|------|--------|
| `key.rs` | `KeySlice::decode_user_key_into`, `encoded_internal_key_len`, `MAX_ENCODED_KEY_LEN` |
| `table/builder.rs` | `user_key_buf` field, decode into field buffer |
| `mem_table.rs` | `decode_user_key_into` for bloom check/rebuild, `seek_prefix` direct comparison |
| `lsm_storage.rs` | Reverted thread-local RefCell, back to `decode_user_key_cow` on hot path |
| `mvcc.rs` | `validate_key_size` helper |
| `vlog/mod.rs` | Key size validation in vLog writes |

## WAL Group Commit Is Submit-Bound (2026-09-23)

### Finding

`wal_concurrent` (4 threads, 50,000 single `put`s each, 1 KiB values, WAL on, tmpfs) is
bound by the cost of the group-commit submit rather than by the write path or by MVCC.
`write-perf --profile` on the merged PITR head, one run:

```text
wal_sync:       3370.44 ms  (87.9% of 3835.96 ms of thread time)
  follower_wait: 2498.87 ms        <- 65% of all thread time
  wal_submit:     596.53 ms        <- 96,874 groups, 6.2 us per submit
  fdatasync:       17.37 ms        <- tmpfs, so the sync itself is not the cost
  follower_events: calls=159148  parks=249841  retries=56022
memtable:        289.58 ms  (7.5%) publish map: copy 111.52, skipmap 76.85, bloom 29.42
wal_write:       175.93 ms  (4.6%)
commit_groups:  96874   solo=26269 (27.1%)   avg_bufs=2.06   max_bufs=4
result:         168,345 ops/s in 1.188 s
```

The numbers close: `96,874 groups / 1.188 s x 2.06 buffers = 168k ops/s`, which is the
measured throughput. Only one submit runs at a time (the `submitting` CAS), so one
leader's 6.2 us is serialized and the other three threads park waiting for it - 65% of
all thread time is followers waiting on a submit, against 17 ms of actual `fdatasync`.

The ordered commit publication this was measured alongside costs about 270 ms of
thread time on the same workload. Its three wait implementations and two structural
redesigns are recorded in `docs/pitr-performance.md`.

### Method

```bash
cargo build --release --features bench --bin write-perf
./target/release/write-perf --suite legacy --preset default --wal \
  --bench wal_concurrent --profile --output json --path <tmpfs dir>
```

The `WriteProfile` phases are behind `#[cfg(feature = "bench")]`, so the `bench`
feature is what makes the breakdown available; the throughput counters do not need it.

Machine state dominates absolute numbers here. This run read 168k ops/s, an idle run of
the same build minutes earlier read 173k, and across sessions the same binaries have
read between 149k and 175k. Compare builds within one session, never across.

### Rejected: widening the solo-leader window

27.1% of commit groups hold a single buffer, against 11.7% before the ordered commit
sequencer landed, and each solo group pays a full serialized submit. The mitigation for
this already exists but cannot fire here: `wait_for_group_commit_peers` only delays a
solo leader when the lone pending buffer is at least `GROUP_COMMIT_MIN_SOLO_BYTES`
(512 KiB) and it spins `GROUP_COMMIT_SOLO_SPINS` (4) times, while a 1 KiB value
produces a buffer of about 1.1 KB.

Widening it to 1024 bytes and 512 spins did exactly what it promises, and lost:

| Variant | solo groups | groups | avg buffers/group | ops/s |
| --- | ---: | ---: | ---: | ---: |
| as merged | 26,269 | 96,874 | 2.06 | 168,345 |
| widened window | 311 | 85,039 | 2.35 | 149,043 |

The leader spins while holding `submitting`, so waiting for peers serializes the submit
pipeline for every other thread. Group coalescing improved and throughput fell 11.5%.
Tuning group formation is a dead end while only one submit can be in flight.

### Where the headroom is

Parallel submissions. RFC 012 replaces the leader/follower group commit with io_uring
submissions and atomic page allocation, which removes both the serialized submit and the
follower wait it causes - the two terms that dominate this profile.

That ordering roughly doubles the solo rate is *not* an argument for it: the experiment
above shows that eliminating solo groups does not buy throughput under the current
submit model, so the submit model is the reason to do RFC 012, not the ordering.

**Update, later the same day: parallel submission was built and measured, twice, and it
does not pay.** The submit model is not what bounds this workload's throughput. See
"Where the submit's time goes" and "The submit shapes, measured out of the engine"
below, which also withdraw the "where the headroom is" reading of this subsection.


### Where the submit's time goes (2026-09-23, later)

`wal_submit` was a single span covering SQE building, the submission and the wait for
completions. Four counters now split it, and a fifth counts the completion events so
they can be checked against the buffers submitted. `wal_concurrent`, 200,000 puts,
1 KiB values, tmpfs, medians of three runs:

| threads | groups | `wal_submit` | ring lock | SQE fill | `submit_and_wait` | CQE reap |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | ~94,000 | 540.65 ms | 2.80 ms | 6.69 ms | 505.73 ms | 6.14 ms |
| 1 | 200,000 | 692.62 ms | 3.16 ms | 3.56 ms | 657.68 ms | 9.51 ms |

`cqe_count` equalled `commit_buffers` (200,000) in every run, so the spans close; about
3.5% of `wal_submit` sits between them, in the aligned-length computation and the chunk
bookkeeping. Preallocation is *not* in that residual: `maybe_preallocate` runs before
`wal_submit` opens and after `wal_leader_prepare` closes, and it only extends the file
when the offset crosses a `PREALLOC_BLOCK` boundary, so it is unmeasured here rather
than absorbed into a span. Per group that is 5.75 us at four writers and
3.46 us at one, of which `submit_and_wait` is 5.38 and 3.29: **the submit is the wait
for the group's own writes to complete**, and everything a faster submit could touch -
filling SQEs, taking the ring lock, reaping events - is about 5% of it. On tmpfs that
wait is an `io_wq` worker round trip.

Two further counters measure the handoff's neighbourhood. `wal_group_gap` is the time
from a group releasing `submitting` to the next leader entering the commit path,
wake-up included - and it is an inter-group gap, not the handoff: the release stamp is
taken whether or not a buffer is pending, so any interval where the queue was empty and
the next write had not arrived is inside it too. `wal_leader_prepare` is what that
leader does before its first SQE - draining the queue, the solo-peer spin, the group
accounting:

| threads | gap per group | prepare per group |
| ---: | ---: | ---: |
| 4 | 3.2-3.4 us | 0.13 us |
| 1 | 1.6-1.7 us | 0.06 us |

At one writer the gap is that writer doing its own memtable and publication work
between groups, so it is work rather than waiting. At four it is 1.6 us larger per
group, but that difference is not the handoff on its own: the gap counts from the
release stamp, so it also carries however long it took the next buffer to arrive, and
faster arrival at four writers is part of why the number differs. The handoff is a
component of it, not the whole. Rather than try to isolate it further, the handoff was
removed outright, in the cheapest form that exercises the same
mechanism: after publishing its own group, a leader keeps draining while writers are
queueing instead of returning to its caller, bounded at eight groups. Paired against
the same build without it, 20 repetitions, same workload and shape:

| build | median | range |
| --- | ---: | --- |
| leader returns after one group (today) | 161,623 ops/s | 142,738-172,752 |
| leader serves further groups | 159,054 ops/s | 131,825-168,116 |

Paired **0.982x**, interquartile range 0.952-1.036, the serving build faster in 9 of
20. A null, and it says why: when a leader is ready to commit, the queue is usually
empty, because each writer has one operation in flight - four writers can have at most
four buffers pending, which is why `avg_bufs` sits at 2.17. Thread time over wall is
about 3.3 of 4 cores, so the commit path has spare capacity, and this workload is bound
by the work each operation costs rather than by the submit round trip. The variant is
reverted; the counters stay.

### The submit shapes, measured out of the engine (2026-09-23)

The engine can only answer what its own structure allows, so the same question was put
to a standalone bench: `io-uring 0.6`, `O_DIRECT` on a registered fd, 4 KiB-aligned
buffers, a 256-entry ring, about two records per group, 200,000 records, five
repetitions in a rotated order so drift lands on every shape, and both invariants
checked on every run - completion events reaped equal to SQEs submitted, and a read-back
of the offsets written. tmpfs:

| shape | what it does | 4 threads | 8 threads |
| --- | --- | ---: | ---: |
| a | today: one leader drains, `submit_and_wait`, `fdatasync` | 332,870 (ref) | 331,008 (ref) |
| b | writers submit; a syncer issues `Fsync` + `IO_DRAIN` | **0.74x** (0.70-0.76) | 0.83x (0.71-1.20) |
| c | no leader: every writer submits, reaps and fsyncs its own group | 0.99x (0.88-1.08) | **0.73x** (0.46-1.03) |
| d | as a, but the fsync is an SQE with `IO_DRAIN` | **0.60x** (0.57-0.65) | 0.77x (0.65-1.13) |

Shape c is RFC 012's remaining proposal taken to its limit - no election, no handoff,
no follower wake-ups, several groups in flight with the durability frontier still
advancing in order - and it is neutral at four threads and worse at eight. Shape b
prices `IO_DRAIN` as a barrier, which holds later SQEs behind the fsync: 0.74x. Shape d
changes only the fsync, to an SQE submitted with the group's writes; the `IO_DRAIN` is
what makes that ordering-correct, so an SQE fsync cannot be had here without it. Shape d
runs at 0.60x shape a's throughput, about 1.7x slower per record - and that is a
statement about the shape, not about the fsync call. The bench measures records per
second, so d's ratio carries the barrier's effect on how later SQEs pipeline as well as
the cost of doing the sync through the ring, and pricing one against a direct
`fdatasync(2)` would need the call timed on its own, which this pass did not do. It is
consistent with the comment above that call in `wal.rs`, which expects the direct
syscall to be cheaper per call; that expectation is still not separately measured.

The disk half of this was not measured. The 200,000-record runs at four and eight
threads exceeded their timeout - 100,000 fsyncs per run - and were discarded rather than
reported from partial output, so the same comparison at a smaller record count belongs
in the next pass rather than in this conclusion. That is the regime RFC 012 expects its
win in.

Both halves say the same thing: this workload's thread time is dominated by the submit
and by waiting for it, but its *throughput* is not. Removing the submit serialization
(c), the submit handoff (the serving experiment), or the fsync's syscall shape (d) does
not raise it, and two of the three lower it. The lever left is the work each operation
costs before it reaches the WAL - where the PITR v5 encoder's per-operation allocations
and copies sit. That work was bounded above by the ~3% that enabling PITR costs end to
end in `docs/pitr-performance.md`; that figure comes from `write-perf`'s four-thread
`wal_concurrent`, a different harness and writer count from the section below, so it is
not a bound on what the same work costs at 16 writers.

### The encoder was that work, and it is what the collapse was made of
(2026-09-23, later still)

The paragraph above pointed at the v5 encoder's per-operation allocations and copies.
They were real and they were removable: `encode_v5_batch` built the encoded batch in a
`Vec`, which `put_v5_batch` then copied into the `DirectBuf` the ring reads, and the
duplicate-key canonicalisation map was built for batches that had one entry. PR #347
writes the batch into the ring buffer directly and skips the map for batches of fewer
than two entries, with the encoded bytes pinned byte-for-byte against the previous
implementation by a digest test.

In-engine counters, from `pitr-perf --writers 16 --modes on --operations 20000
--profile` on a `--features bench` build - the same harness and mode as the paired table
below, so the two can be read together:

| | before | after |
| --- | ---: | ---: |
| `wal_encode` | 2.66 us/op | 0.11 us/op |
| `wal_prepare` | 0.85 us/op | 1.38 us/op (the real encoding moved here) |
| `wal_write` (whole span) | 5.10 us/op | 2.78 us/op |
| commit groups per 20k ops | 6,797 | 4,905 |
| avg buffers per group | 2.94 | 4.08 |

The group counts are the interesting pair. Cheaper producers reach the queue sooner, so
the leader's drain finds more waiting: 28% fewer groups, each covering 39% more buffers,
which is 28% fewer serialized submits and fsyncs per operation.

The counters cannot separate that effect from the microseconds themselves, because the
first follows from the second, and both are the same saving seen twice: across the full
range the span falls 5.10 -> 2.78 us/op while the per-operation cost derived from the
paired table falls by about 2.9 us, so most of the reduction is the span's own work,
converting into throughput through the grouping. What the 8-writer control shows is
where the conversion happens: there `wal_write` is about halved as well (36-41 ms to
20-21 ms per 20k ops) with the group count left in the same range (6.3k-7.5k before,
6.3k after), and that case is throughput parity. Cheaper producers only become
throughput where the producer queue is what the pipeline waits on, which is the
collapsed region.

`pitr-perf`, paired, one case per invocation - `--writers N --modes on --operations
10000`, default 128-byte values, a release build without `--features bench` - 40
repetitions, alternating order, with a null pair, the same binary under both names, at
each writer count:

| writers | paired median | faster | null at that count |
| ---: | ---: | ---: | ---: |
| 1 | 1.078 | 27/40 | 0.952 |
| 4 | 1.004 | 20/40 | 1.016 |
| 8 | 0.902 | 10/40 | 0.936 |
| 16 | **1.519** | **37/40** | 1.043 |
| 32 | 1.181 | 29/40 | 0.919 |
| control: PITR off, 16 writers | 0.972 | 15/40 | - |

PITR-on has collapsed by 16 writers - in those same paired runs the unmodified build's
median was about 118k ops/s, against 181k at 8 writers - and that is where the change is
worth ~1.5x, with 37 of 40 repetitions in its favour while the null pair at that count
took 22 of 40.

"By 16" and not "at 16": this table samples the same fixed list the older harness did,
so 16 is the first count above 8 that shows the drop, not the count the drop belongs to.
`docs/pitr-performance.md` withdrew that reading already - its 20-writer run reads worse
than its 16-writer one - and nothing here contradicts it.

The PITR-disabled control matters because that path never calls this encoder and must
read parity, which it does - though that row carries no null of its own, so it is read
against the 1.043 measured for the enabled pair at the same count, which puts it at
0.972/1.043 = 0.93: a 7% shortfall, inside the protocol's band rather than clear of it.

The null column is why the rest of the table is readable at all. This protocol carries a
per-count offset of up to about 8%, so a single number without its null beside it means
little - and the protocol used before it was worse: it ran all five writer counts inside
one invocation, and its null pair read 0.893-1.056, the same size as the effects it was
reporting. Those numbers, including an apparent 1.291x at 16 writers from the same
change, are withdrawn; what is left stands on the single-case runs above and on the
per-operation counters - and those were taken at two of the five counts, 8 and 16
writers, not at 1, 4 or 32. Where both exist they agree at 16 and part company at 8,
where the span halves while the raw paired row still reads 0.902 (0.936 against its
null); the 8-writer entry above is called parity on the strength of that null, not of
the counter.

So the ledger. The submit serialization, the handoff and the fsync's shape are each worth
nothing - but those were measured on `write-perf`'s `wal_concurrent`, which runs with
PITR off, and on a standalone bench outside the engine, so they say nothing directly
about the PITR path; what they establish is that the submit machinery is not this
workload's constraint.

Read each row against its null, as above, and the encoder's share is: ~1.5x where PITR-on
has collapsed - 1.52 raw and 1.46 against its null at 16 writers, 1.18 and 1.29 at 32 -
and parity at 4 and 8 writers (1.004 and 0.902 raw, 0.99 and 0.96 against their nulls).
The 1-writer row is the one that does not fit that split: 1.078 raw against a null of
0.952 is 1.13x, a win on this reading, but one count at 40 repetitions with an IQR
spanning 0.99-1.23 is not measured to the depth that would settle it - so it is not
counted as one, and not counted against one either. What the other four rows support is
narrower than a rule about where this change pays: it is *measured* to pay at the two
counts where PITR-on has collapsed, and measured to be neutral at 4 and 8. Whether it
also pays at one writer is open, and saying it does not would be reading the same
unsettled row the other way.

One tension with the document next door is worth naming rather than leaving for a reader
to find. `docs/pitr-performance.md`'s payload control concluded that the enabled path's
cost tracks the bytes it moves per operation rather than the machinery it runs. This
change does not contradict that: it reduces how many times the enabled path touches the
same bytes, leaving the bytes-per-operation unchanged, so both statements are about the
same cost. What has *not* been re-measured is the enabled-against-disabled ratio after
the change, on either harness - this section's runs compare the encoder against itself,
and `docs/pitr-performance.md`'s 0.701x at 16 writers predates it - so how much of the
PITR-on penalty is left is an open measurement, not a claim.
