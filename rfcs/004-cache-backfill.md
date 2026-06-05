# RFC 004: Cache Backfill on Flush and Compaction

**Status:** Implemented  
**Date:** 2026-06-05  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

When a memtable is flushed to SST or when compaction produces new SSTs, the
newly created blocks are **not** inserted into the block cache. This creates a
cache-miss cliff: data that was hot in memory (in the skiplist memtable) or
that previously existed in cache (before compaction invalidation) suddenly
becomes cold on disk.

This RFC proposes **cache backfill** — automatically inserting newly produced
blocks into the block cache (and optionally the value cache) during flush and
compaction, with lightweight admission heuristics to prevent cache pollution.

---

## 2. Motivation

### 2.1 The Flush Cliff

Current flush path (`force_flush_next_imm_memtable`):

1. Iterate memtable (hot, in-memory skiplist).
2. Encode blocks → write SST to disk.
3. Drop memtable. Blocks are **not** cached.
4. Next read to recently flushed data → cache miss → disk I/O.

For write-heavy workloads with read-after-write patterns (e.g., fillseq +
readseq, or application-level polling), this guarantees a cache miss for the
most recently written data — the exact subset most likely to be read again.

### 2.2 The Compaction Dip

Current compaction path (`compact_from_iter`):

1. Read old SST blocks (possibly from cache).
2. Merge entries into new SST blocks.
3. Write new SST.
4. Call `block_cache.invalidate_ssts(removed_ids)` — **all** cached blocks for
the old SSTs are removed.
5. New blocks are **not** inserted.

Result: after compaction, the working set that was previously cache-resident is
ejected and not replaced. Read latency spikes until the working set is re-read
from disk.

### 2.3 Why Now

The engine recently migrated to `TinyUFO` (RFC implied by PR #55), which uses
S3-FIFO + TinyLFU admission. TinyUFO handles high insertion rates well and
evicts cold data quickly. This makes backfill practical: we can insert
aggressively and rely on the cache's admission policy to reject cold blocks,
rather than building complex admission logic ourselves.

---

## 3. Design Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                    Backfill Architecture                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   Flush Path                                                │
│   ───────────                                               │
│   MemTable ──► SsTableBuilder ──► SST file                 │
│                                     │                       │
│                                     ▼                       │
│                               BlockCache::backfill()       │
│                                                             │
│   Compaction Path                                           │
│   ────────────────                                          │
│   Old SSTs ──► Merge Iterator ──► SsTableBuilder            │
│                                     │                       │
│                                     ▼                       │
│                               BlockCache::backfill()       │
│                               (before invalidate_ssts)     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.1 Design Principles

1. **Background-only**: Backfill runs in flush/compaction threads, never
   blocking foreground reads/writes.
2. **Admission heuristics**: Not every block is backfilled. We use lightweight
   rules to avoid polluting the cache with cold data.
3. **Reuse existing infrastructure**: Leverage `TinyUFO::force_put` and the
   existing reverse-index in `BlockCache`.
4. **Optional / configurable**: Backfill can be disabled via
   `LsmStorageOptions` if the workload is known to be write-only or if cache
   memory is severely constrained.

---

## 4. Detailed Design

### 4.1 BlockCache API Extension

Add a `backfill` method to `BlockCache` that bypasses single-flight miss
coalescing (there are no concurrent misses for brand-new blocks) and updates
the reverse index in one batch:

```rust
impl BlockCache {
    /// Insert a batch of blocks for a newly created SST.
    /// This is used by flush and compaction threads to warm the cache.
    pub fn backfill(&self, sst_id: usize, blocks: Vec<Arc<Block>>) {
        if blocks.is_empty() {
            return;
        }
        let num_blocks = blocks.len();
        // Insert into cache outside the sst_blocks lock to avoid blocking
        // concurrent readers during force_put (which may trigger eviction).
        let mut keys = Vec::with_capacity(num_blocks);
        for (idx, block) in blocks.into_iter().enumerate() {
            let key = (sst_id, idx);
            self.inner.force_put(key, block, 1);
            keys.push(key);
        }
        self.count.fetch_add(num_blocks as u64, Ordering::Relaxed);
        let mut index = self.sst_blocks.lock();
        index.entry(sst_id).or_default().extend(keys);
    }
}
```

**Rationale:** `force_put` skips TinyUFO's frequency-based admission. For
backfill this is correct: we want the blocks to enter the cache immediately.
TinyUFO's S3-FIFO queue will evict them quickly if they are not re-read,
preventing long-term pollution.

**Why `force_put` instead of `get_or_insert`:** The blocks are brand new —
there is no value in checking for existing entries. `force_put` is the same
path used by `get_or_insert` on cache miss.

### 4.2 Flush Backfill

#### Option A: Read-Back from Disk (Minimal Change)

After `builder.build()` writes the SST file, read each block back from the
file and insert into cache:

```rust
let sst = builder.build(sst_id, Some(self.block_cache.clone()), self.path_of_sst(sst_id))?;
let mut blocks = Vec::with_capacity(sst.block_meta.len());
for idx in 0..sst.block_meta.len() {
    let block = sst.read_block(idx)?; // fail fast on corruption
    blocks.push(block);
}
self.block_cache.backfill(sst_id, blocks);
```

**Pros:**
- Simple, localized change (~10 lines).
- No memory overhead during build.

**Cons:**
- Extra disk read per block (sequential, likely OS page-cache resident).
- Syscall overhead (~1–2 µs per block on Linux).

**Verdict:** Acceptable as an MVP. Flush is background, so a few milliseconds
of extra I/O is fine.

**Error handling note:** A failure in `sst.read_block(idx)?` means the SST
was just written but cannot be read back — this indicates disk corruption or
hardware failure. Aborting the flush via `?` is the correct behavior; the
memtable will remain and be retried on the next flush cycle.

#### Option B: Capture Blocks from Builder (Zero Extra I/O)

`SsTableBuilder` already builds `Block` objects in memory before encoding them.
We can capture the `Arc<Block>` at each block boundary and expose them after
`build()`.

Modify `SsTableBuilder`:

```rust
pub struct SsTableBuilder {
    // ... existing fields ...
    /// Collected blocks for cache backfill. Only populated when
    /// `collect_blocks` is true (e.g., during flush).
    collected_blocks: Vec<Arc<Block>>,
    collect_blocks: bool,
}
```

In `add_inner`, when sealing a full block, preserve the existing
`first_key` / `last_key` extraction (from the `BlockBuilder`) and clone the
block before encoding:

```rust
let old_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
let first_key = KeyBytes::from_bytes(old_builder.key_at(0));
let last_idx = old_builder.num_entries().saturating_sub(1);
let last_key = KeyBytes::from_bytes(old_builder.key_at(last_idx));
let block = old_builder.build();
if self.collect_blocks {
    self.collected_blocks.push(Arc::new(block.clone()));
}
let data = block.encode();
// ... push meta, extend self.data ...
```

In `build`, after the final block:

```rust
let final_block = self.builder.build();
if self.collect_blocks && self.has_data {
    self.collected_blocks.push(Arc::new(final_block.clone()));
}
let data = final_block.encode();
self.data.extend(data);
```

> **Note:** `Block::encode` consumes `self`. We clone the block first so we can
> keep an `Arc<Block>` for backfill while still consuming the original for
> encoding. `Block` clone is cheap (`Bytes` refcount bump + `Vec<u16>` clone).
> `Block` gains `#[derive(Clone)]` in a companion code PR (see block.rs).
> Clone is cheap: `Bytes` refcount bump + `Vec<u16>` clone.

Add a new `build_with_backfill` method that returns both the SST and the
 collected blocks. The existing `build()` delegates to it and discards the
 blocks, preserving backward compatibility for tests and non-backfill callers:

```rust
impl SsTableBuilder {
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let (sst, _blocks) = self.build_with_backfill(id, block_cache, path)?;
        Ok(sst)
    }

    pub fn build_with_backfill(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<(SsTable, Vec<Arc<Block>>)> {
        // ... existing build logic ...
        let blocks = self.collected_blocks;
        // ... construct SsTable ...
        Ok((sst, blocks))
    }
}
```

**Usage in flush:**

```rust
let mut builder = SsTableBuilder::new(self.options.block_size);
builder.set_collect_blocks(self.options.enable_cache_backfill); // new API
memtable_to_flush.flush(&mut builder)?;
let (sst, blocks) = builder.build_with_backfill(
    sst_id,
    Some(self.block_cache.clone()),
    self.path_of_sst(sst_id),
)?;
self.block_cache.backfill(sst_id, blocks);
```

> **Note:** `SsTableBuilder::new_with_vlog` should also support `collect_blocks`.
> The flag is set in the same way after construction — `new_with_vlog` initializes
> `collect_blocks: false`, and the caller calls `set_collect_blocks(true)` after.
> When using `build_with_backfill`, call `take_vlog_entries()` **before** it,
> because `build_with_backfill` consumes the builder just like `build()`.
>
> **Empty-builder edge case:** If `SsTableBuilder` has no data, `build_with_backfill`
> returns an empty `Vec<Arc<Block>>`. Callers (flush, compaction) never build empty
> SSTs in practice, but `backfill` is a harmless no-op if passed an empty vec.

**Pros:**
- Zero extra disk I/O.
- Blocks are already in memory; just keep a refcount.
- No final-block loss: the last block is collected inside `build_with_backfill`
  before the builder is consumed.

**Cons:**
- Higher memory usage during flush. Peak memory is ~3x SST size because
  `self.data` (encoded), `collected_blocks` (uncompressed), and the final
  `buf` (self.data + final block + metadata + bloom) all coexist briefly.
  With `target_sst_size = 2 MB` (production default), peak overhead is ~6 MB.
- Requires API changes to `SsTableBuilder`.

**Verdict:** Recommended for the final implementation. The memory overhead is
bounded and flush already allocates the full SST buffer anyway.

**API style note:** The existing `SsTableBuilder` uses constructor parameters
(`new(block_size)`, `new_with_vlog(...)`). Adding `set_collect_blocks(bool)`
is a setter pattern, which is a minor departure. An alternative is a builder
pattern or a `new_with_options` constructor. The setter approach is simpler
for now; revisit if more options accumulate.

### 4.3 Compaction Backfill

Compaction is trickier because:
1. Output data may be cold (e.g., L4→L5 compaction).
2. We currently invalidate old SST blocks from cache.
3. Aggressive backfill could evict hotter blocks.

#### Heuristic: Cache-Resident Backfill

**Rule:** Only backfill output blocks whose **source blocks** were present in
the cache. This preserves the invariant: *"data that was hot before compaction
stays hot after compaction."*

**Implementation sketch:**

Since `compact_from_iter` uses a merge iterator over multiple source SSTs, we
do not have per-block provenance for each output entry. Tracking exact mapping
from input block → output block is complex.

A practical approximation:

1. Before compaction starts, sample the cache for each input SST. This
   requires a lightweight `BlockCache::get` peek method (a one-liner wrapping
   `self.inner.get(&(sst_id, block_idx))`) that does not trigger a load:
   ```rust
   let input_cached: Vec<bool> = input_ssts.iter()
       .map(|sst| sst.block_meta.iter().enumerate()
           .map(|(idx, _)| block_cache.get(sst.sst_id(), idx).is_some())
           .collect())
       .collect();
   ```
2. If **any** input SST had ≥ `CACHE_RESIDENT_THRESHOLD` fraction of its
   blocks cached, treat the entire compaction output as "warm" and backfill
   all output blocks.
3. Otherwise, skip backfill.

**Threshold:** `CACHE_RESIDENT_THRESHOLD = 0.25` (25% of blocks cached).
This is a tunable parameter.

**Simpler alternative:** Backfill only L0→L1 compactions (by definition
recent, likely hot data) and skip deeper compactions entirely. This avoids
the sampling overhead and is more predictable than a percentage threshold.

**Why this works:**
- L0→L1 compactions typically operate on recently flushed data, which is often
  cache-resident. They will pass the threshold and be backfilled.
- Deep-level (L4→L5) compactions operate on old, cold data that is rarely
  cached. They will fail the threshold and skip backfill, avoiding pollution.

#### Simpler Alternative: Always Backfill Compaction Output

Rely entirely on TinyUFO's S3-FIFO to evict cold blocks. This is the simplest
to implement and may be sufficient given TinyUFO's fast cold eviction.

**Risk:** A large full compaction could insert hundreds of MB of cold blocks
and transiently evict hot data before TinyUFO's frequency sketch catches up.

**Mitigation:** Backfill compaction output **inside** `compact_from_iter`,
immediately after each `build_with_backfill()`. This runs before the state
update and before `invalidate_ssts`, so for a brief period both old and new
blocks coexist in cache. TinyUFO handles this via natural eviction — the net
effect is that new blocks replace old blocks as they are evicted.

**Verdict:** Start with "Always Backfill" for compaction. If benchmarks show
cache hit-rate dips, add the threshold heuristic or the L0-only heuristic.

#### Integration with `compact_from_iter`

`compact_from_iter` creates multiple `SsTableBuilder`s in a loop. The collected
blocks must be drained **before** `build()` consumes the builder:

```rust
let mut builder = SsTableBuilder::new(self.options.block_size);
builder.set_collect_blocks(self.options.enable_cache_backfill);
while iter.is_valid() {
    if builder.estimated_size() >= self.options.target_sst_size {
        all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
        let sst_id = self.next_sst_id();
        let (sst, blocks) = builder.build_with_backfill(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        self.block_cache.backfill(sst_id, blocks);
        ret.push(Arc::new(sst));
        builder = SsTableBuilder::new(self.options.block_size);
        builder.set_collect_blocks(self.options.enable_cache_backfill);
    }
    // ... add entries ...
}

if !builder.is_empty() {
    all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
    let sst_id = self.next_sst_id();
    let (sst, blocks) = builder.build_with_backfill(
        sst_id,
        Some(self.block_cache.clone()),
        self.path_of_sst(sst_id),
    )?;
    self.block_cache.backfill(sst_id, blocks);
    ret.push(Arc::new(sst));
}
```

> **Ordering note:** `build_with_backfill()` consumes the builder, so it must be
> called **after** any other "take" operations such as `take_vlog_entries()`.

### 4.4 Value Cache Backfill (Future Work)

The value cache (`ValueCache`) stores vLog values keyed by `(file_id, offset)`.
Backfill is less critical here because:
- Flush: large values are written to vLog, but the value cache is only useful
  for point reads. Backfilling every flushed value would be expensive.
- Compaction: vLog values are not rewritten (only pointers move in SST).
- GC: vLog garbage collection rewrites values. The new `(file_id, offset)` keys
  differ from old ones. Backfill would require tracking the mapping from old
  → new offsets, which is non-trivial.

**Recommendation:** Defer value cache backfill until GC is the dominant
latency source.

### 4.5 Interaction with Cache Invalidation

Current flow during compaction:

```rust
let (new_ssts, vlog_ids) = self.compact(task)?;
// ... update state ...
let removed_ids: HashSet<usize> = ...;
self.block_cache.invalidate_ssts(&removed_ids);
```

With backfill:

- **Flush:** Backfill happens immediately after `build_with_backfill()` in
  `force_flush_next_imm_memtable`, under `state_lock` and before the
  new SST is published:

```rust
let (sst, blocks) = builder.build_with_backfill(
    sst_id,
    Some(self.block_cache.clone()),
    path,
)?;
self.block_cache.backfill(sst_id, blocks);
// ... state update happens here ...
```

Backfill completes **before** the new SST becomes visible to readers. There
is no window where a reader sees a new SST but misses its block in cache.

- **Compaction:** Backfill happens **inside** `compact_from_iter` immediately
  after each `build_with_backfill()`. This runs **before** `self.compact(task)`
  returns, before the state update, and before `invalidate_ssts`. For a brief
  period, both old and new blocks coexist in cache. This is harmless — TinyUFO
  handles the temporary over-capacity via natural eviction, and old blocks are
  cleaned up by `invalidate_ssts` shortly after.

  > **Why not backfill after invalidation?** Moving backfill after `invalidate_ssts`
  > would free cache capacity first, but creates a window where the new SST is
  > visible yet its blocks are not cached — the exact cache-miss cliff this RFC
  > eliminates. The temporary over-capacity is bounded (~3x `target_sst_size`) and
  > TinyUFO's S3-FIFO eviction handles it naturally. See Section 5 risk table for
  > the "Cache pollution from cold compaction data" mitigation.

### 4.6 Synchronous vs. Async Backfill

Backfill is **synchronous** (inline in the flush/compaction path).

**Why synchronous:**
- Simpler implementation.
- Backfill completes before the new SST is visible to readers (no cache-miss
  window).
- The flush path already holds `state_lock` and does disk I/O (writing the SST
  file, fsync). The extra overhead of `force_put` calls (~0.1 ms for Option B,
  ~1 ms for Option A) is small relative to the existing I/O.

**Why not async:**
- Async creates a window where the new SST is visible but blocks are not cached.
- Requires handling cancellation (what if the SST is compacted away before
  backfill completes?).
- The ordering guarantee of synchronous backfill is valuable and simple.

**Future consideration:** If write stall concerns materialize under extreme
write throughput, async backfill can be added as an optimization. The
synchronous path should be the default.

### 4.7 Interaction with `get_or_insert` Single-Flight

When a reader calls `read_block_cached` and the block was already backfilled,
`inner.get(&key)` returns it immediately — no issue.

No race with concurrent readers: backfill runs **before** the new SST becomes
visible in both flush (under `state_lock`) and compaction (inside
`compact_from_iter`, before state update). By the time a reader can reference
the new SST, all its blocks are already in cache.

---

## 5. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Cache pollution from cold compaction data | Medium | Use TinyUFO's natural eviction; add `CACHE_RESIDENT_THRESHOLD` heuristic or L0-only heuristic if needed. |
| Memory pressure during flush (Option B) | Low | Bounded by ~3x `target_sst_size` (production default 2 MB → ~6 MB peak). Flush already allocates the full SST buffer. |
| Extra I/O during flush (Option A) | Low | Flush is background; sequential reads are fast. |
| Race between backfill and concurrent read | Low | `force_put` is atomic per-call. For flush, backfill runs under `state_lock`. For compaction, a brief window exists where readers may load the same block from disk — harmless, single-block wasted I/O. |
| Reverse index leak (stale keys after eviction) | Low (pre-existing) | Same as today: evicted blocks leave stale reverse-index entries until SST deletion. Backfill does not worsen this. |
| `entry_count` drift | Low (pre-existing) | Backfill assumes `sst_id` is unique (guaranteed by `next_sst_id()`), so each block key is new and `count.fetch_add` is safe without pre-checking. Remaining drift from TinyUFO evictions is pre-existing and unchanged. |
| Compaction backfill before invalidation | Low | For a brief period both old and new blocks coexist in cache. TinyUFO handles over-capacity via natural eviction. Old blocks are cleaned up by `invalidate_ssts` shortly after. |

---

## 6. Alternatives Considered

### 6.1 No Backfill (Status Quo)
- **Pros:** Zero code change, zero risk.
- **Cons:** Cache misses for all recently flushed / compacted data.

### 6.2 Read-Ahead Prefetching Instead of Backfill
- **Pros:** General solution; works for any read pattern.
- **Cons:** Reactive, not proactive. Does not help the first read after flush.

### 6.3 Insert Only Metadata (BlockMeta) into Cache
- **Pros:** Very low memory overhead.
- **Cons:** Does not eliminate disk I/O; only helps `find_block_idx`.

### 6.4 OS Page Cache Hints (`posix_fadvise`)
- **Pros:** Free for the application (no memory duplication). Handles the
  common case where the OS page cache is large enough.
- **Cons:** OS page cache is shared with everything else on the system, is not
  under the application's control, and does not integrate with the application's
  eviction policy. Does not help when the OS page cache is under memory
  pressure.

### 6.5 Warming Cache on SST Open
- **Pros:** Eliminates cold-start penalty when the engine restarts. Every SST
  is cold on startup — this is the most impactful scenario.
- **Cons:** Does not help the flush/compaction dip during normal operation.
  Orthogonal to this RFC; could be a separate optimization.

### 6.6 Tiered Cache (Hot In-Process, Warm on Disk)
- **Pros:** Keeps the hottest blocks in-process and warm blocks on disk,
  extending effective cache capacity.
- **Cons:** Significant implementation complexity. Defer until the block cache
  capacity is the dominant bottleneck.

---

## 7. Implementation Plan

### Phase 1: BlockCache API + Flush Backfill (Option A)
- Add `BlockCache::backfill()`.
- Modify `force_flush_next_imm_memtable` to read blocks from the new SST and
  backfill.
- Add `LsmStorageOptions::enable_cache_backfill: bool` (default `true`).
- Benchmark: `fillseq` + `readseq` latency before/after.

### Phase 2: Flush Backfill (Option B — Zero I/O)
- Add `SsTableBuilder::collect_blocks` / `build_with_backfill()`.
- Switch flush path from disk-read backfill to in-memory backfill.
- Benchmark: flush throughput impact.

### Phase 3: Compaction Backfill
- Add backfill to `compact_from_iter` output.
- Start with "always backfill".
- Benchmark: mixed R/W workloads, monitor cache hit rate.

### Phase 4: Admission Heuristic (if needed)
- Implement `CACHE_RESIDENT_THRESHOLD` sampling or L0-only heuristic.
- A/B test against Phase 3.

### Phase 5: Value Cache Backfill (Optional / Future)
- Investigate vLog GC offset mapping for value cache backfill.

---

## 8. Benchmarking Plan

### 8.1 Workloads

| Workload | Description | What It Tests |
|----------|-------------|---------------|
| `fillseq` + immediate `readseq` | Write sequentially, then read sequentially with no delay | Flush cliff (best case — OS page cache may be warm) |
| `fillrandom` + `readrandom` | Write randomly, then read randomly | Flush cliff with random access patterns |
| `fillrandom` + force compaction + drop page cache + `readrandom` | Write, compact, evict OS page cache, then read | **Cold-start after compaction** — the exact scenario backfill targets |
| `readwhilewriting` with compaction | Concurrent reads and writes that trigger compaction | Compaction dip under load |
| Zipfian hot/cold mixed | Only some keys are hot (Zipfian distribution) | Whether backfill warms hot data without polluting with cold compaction output |
| Oversized working set | Working set exceeds block cache capacity | Whether backfill of cold data evicts hot data |
| `fillseq` throughput (regression) | Write-only, measure ops/sec | Throughput regression from backfill overhead |

### 8.2 Metrics

| Metric | Why |
|--------|-----|
| Read latency (p50, p99) | Primary improvement metric |
| Cache hit rate | Whether backfill actually warms the cache |
| Throughput (ops/sec) | Regression detection |
| Peak RSS / memory delta | Option B memory overhead |
| Cache eviction rate | Whether backfill increases churn |
| `entry_count` drift | Verify unique `sst_id` guarantee prevents double-counting |
| `sst_blocks` lock contention | Under concurrent flush/compaction |
| `perf stat` cache misses | GC pressure from `Arc` refcount traffic |

### 8.3 Expected Improvements

| Scenario | Expected Impact |
|----------|----------------|
| Cold-start read after flush (OS page cache cold) | Significant: eliminates first-read latency spike. Magnitude depends on disk vs. cache hit latency (typically 100–1000x for SSD). Effect on p99 tail is most visible. |
| Cold-start read after compaction (OS page cache cold) | Significant: same as above, but for compacted data |
| Warm OS page cache | Modest: `pread` already hits page cache; backfill adds marginal benefit |
| `fillseq` throughput | ≤5% regression for Option B (in-memory capture + `force_put` calls). Option A may be higher (~5–10%) due to syscall overhead — benchmark before committing. |

### 8.4 Actual Results

Measured on `feat/cache-backfill` branch (2026-06-05), AMD EPYC, NVMe SSD, Linux 6.x.
OS page cache was explicitly dropped between flush/compaction and reads using
`posix_fadvise(POSIX_FADV_DONTNEED)` on all SST files.

| Scenario | Backfill | Time | Speedup |
|----------|----------|------|---------|
| **Flush cliff** — 1000 entries (4KB values), point gets after flush + cold OS cache | enabled | **224 µs** | **6.1×** |
| **Flush cliff** — same configuration | disabled | 1.36 ms | — |
| **Compaction dip** — L0→L1 compaction, 1000 entries, point gets + cold OS cache | enabled | **2.65 ms** | **1.8×** |
| **Compaction dip** — same configuration | disabled | 4.66 ms | — |

**Observations:**

- **Flush backfill** delivers a **6.1×** latency reduction when the working set fits
  in the application block cache and the OS page cache is cold.
- **Compaction backfill** provides a **1.8×** improvement because compaction
  invalidates old cached blocks; without backfill, the new SSTs start completely
  cold.
- **Working set must fit in cache.** When the block cache (512 blocks) was tested
  with a dataset larger than capacity (5000 blocks), backfill showed no benefit
  and added slight overhead because inserted blocks were evicted before being read.
  This validates the admission-heuristic design (Section 4.3).
- **OS page cache masks the benefit.** On a warm OS page cache, both paths are
  fast because `pread` hits the kernel cache. The benefit is most visible under
  memory pressure or when the OS page cache is cold.

---

## 9. Open Questions

1. **Should backfill use `force_put` or TinyUFO's normal `put` with admission?**
   - `force_put` is simpler and gives immediate warmth.
   - Normal `put` would let TinyUFO's TinyLFU reject cold blocks, but adds
     complexity (need frequency sketch updates for new keys).
   - **Tentative answer:** Use `force_put`. Monitor hit rates.

2. **Should we backfill the block cache during WAL replay / recovery?**
   - During recovery, SSTs are re-flushed from WAL. Backfilling could speed up
     post-recovery reads, but recovery is rare.
   - **Tentative answer:** No. Keep recovery simple. Consider SST-open warming
     as a separate optimization.

3. **How does backfill interact with `Arc<Block>` refcounting?**
   - `Block` data is `Bytes`, which uses atomic refcounts. Holding an extra
     `Arc<Block>` in cache is cheap.
   - No concerns.

4. **L0-only heuristic vs. percentage threshold?**
   - L0-only is simpler and more predictable (backfill flush + L0→L1 compaction
     only; skip deeper compactions).
   - Percentage threshold is more adaptive but requires sampling.
   - **Tentative answer:** Start with "always backfill." If benchmarks show
     cache hit-rate dips, try L0-only first before the percentage threshold.

---

## 10. Related Work

- **RocksDB:** Supports `cache_index_and_filter_blocks` and
  `pin_l0_filter_and_index_blocks_in_cache`. Has `prepopulate_block_cache`
  (added in 6.29) which can prepopulate the block cache on flush with data
  blocks — the direct analog to this RFC. Also has `compaction_readahead_size`
  for read-ahead during compaction input reads.
- **BadgerDB:** Uses a `blockcache` similar to ours; backfills on open but not
  on flush/compaction.
- **PebblesDB / TerarkDB:** Some variants backfill based on access frequency
  heuristics.

---

## 11. Summary

Cache backfill is a low-risk, medium-reward optimization that eliminates the
cache-miss cliff after flush and compaction. By inserting newly produced blocks
into `TinyUFO` during background operations, we keep recently written and
frequently compacted data warm in memory. The recommended implementation path
starts with a simple disk-read backfill on flush, graduates to zero-I/O
capture from `SsTableBuilder`, and then extends to compaction with a fallback
threshold heuristic if needed.
