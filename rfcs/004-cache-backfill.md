# RFC 004: Cache Backfill on Flush and Compaction

**Status:** Proposal  
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
│                               (after old SSTs invalidated) │
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
        let keys: Vec<BlockKey> = blocks
            .iter()
            .enumerate()
            .map(|(idx, _)| (sst_id, idx))
            .collect();
        for (key, block) in keys.iter().zip(blocks) {
            self.inner.force_put(*key, block, 1);
            // Note: this may overcount if the key already exists (e.g., re-backfill
            // of the same SST). entry_count is intentionally an upper bound.
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        let mut index = self.sst_blocks.lock();
        for key in &keys {
            index.entry(sst_id).or_default().insert(*key);
        }
    }
}
```

**Rationale:** `force_put` skips TinyUFO's frequency-based admission. For
backfill this is correct: we want the blocks to enter the cache immediately.
TinyUFO's S3-FIFO queue will evict them quickly if they are not re-read,
preventing long-term pollution.

### 4.2 Flush Backfill

#### Option A: Read-Back from Disk (Minimal Change)

After `builder.build()` writes the SST file, read each block back from the
file and insert into cache:

```rust
let sst = builder.build(sst_id, Some(self.block_cache.clone()), path)?;
if let Some(ref cache) = self.block_cache {
    let mut blocks = Vec::with_capacity(sst.block_meta.len());
    for idx in 0..sst.block_meta.len() {
        let block = sst.read_block(idx)?; // fail fast on corruption
        blocks.push(block);
    }
    cache.backfill(sst_id, blocks);
}
```

**Pros:**
- Simple, localized change (~10 lines).
- No memory overhead during build.

**Cons:**
- Extra disk read per block (sequential, likely OS page-cache resident).
- Syscall overhead (~1–2 µs per block on Linux).

**Verdict:** Acceptable as an MVP. Flush is background, so a few milliseconds
of extra I/O is fine.

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
if self.collect_blocks {
    self.collected_blocks.push(Arc::new(final_block.clone()));
}
let data = final_block.encode();
self.data.extend(data);
```

> **Note:** `Block::encode` consumes `self`. We clone the block first so we can
> keep an `Arc<Block>` for backfill while still consuming the original for
> encoding. `Block` clone is cheap (`Bytes` refcount bump + `Vec<u16>` clone).
> `#[derive(Clone)]` must be added to `Block` in `block.rs`.

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
        let blocks = std::mem::take(&mut self.collected_blocks);
        // ... construct SsTable ...
        Ok((sst, blocks))
    }
}
```

**Usage in flush:**

```rust
let mut builder = SsTableBuilder::new(self.options.block_size);
builder.set_collect_blocks(true); // new API
memtable_to_flush.flush(&mut builder)?;
let (sst, blocks) = builder.build_with_backfill(
    sst_id,
    Some(self.block_cache.clone()),
    path,
)?;
if let Some(ref cache) = self.block_cache {
    cache.backfill(sst_id, blocks);
}
```

> **Note:** `SsTableBuilder::new_with_vlog` should also support `collect_blocks`.
> The flag is set in the same way after construction. When using
> `build_with_backfill`, call `take_vlog_entries()` **before** it, because
> `build_with_backfill` consumes the builder just like `build()`.
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
- Slightly higher memory usage during flush (~SST size in RAM until `build`
  returns).
- Requires API changes to `SsTableBuilder`.

**Verdict:** Recommended for the final implementation. The memory overhead is
bounded by `target_sst_size` (default 1 MB) and flush already allocates the
full SST buffer anyway.

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
           .map(|(idx, _)| block_cache.get(sst.id(), idx).is_some())
           .collect())
       .collect();
   ```
2. If **any** input SST had ≥ `CACHE_RESIDENT_THRESHOLD` fraction of its
   blocks cached, treat the entire compaction output as "warm" and backfill
   all output blocks.
3. Otherwise, skip backfill.

**Threshold:** `CACHE_RESIDENT_THRESHOLD = 0.25` (25% of blocks cached).
This is a tunable parameter.

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

**Mitigation:** Backfill compaction output **after** `invalidate_ssts` runs,
so the net cache size change is roughly zero (old blocks removed, new blocks
added).

**Verdict:** Start with "Always Backfill" for compaction. If benchmarks show
cache hit-rate dips, add the threshold heuristic.

#### Integration with `compact_from_iter`

`compact_from_iter` creates multiple `SsTableBuilder`s in a loop. The collected
blocks must be drained **before** `build()` consumes the builder:

```rust
let mut builder = SsTableBuilder::new(self.options.block_size);
builder.set_collect_blocks(true);

while iter.is_valid() {
    if builder.estimated_size() >= self.options.target_sst_size {
        all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
        let sst_id = self.next_sst_id();
        let (sst, blocks) = builder.build_with_backfill(
            sst_id,
            Some(self.block_cache.clone()),
            path,
        )?;
        if let Some(ref cache) = self.block_cache {
            cache.backfill(sst_id, blocks);
        }
        ret.push(Arc::new(sst));
        builder = SsTableBuilder::new(self.options.block_size);
        builder.set_collect_blocks(true);
    }
    // ... add entries ...
}

if !builder.is_empty() {
    all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
    let sst_id = self.next_sst_id();
    let (sst, blocks) = builder.build_with_backfill(
        sst_id,
        Some(self.block_cache.clone()),
        path,
    )?;
    if let Some(ref cache) = self.block_cache {
        cache.backfill(sst_id, blocks);
    }
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

- **Compaction:** Backfill happens **inside** `compact_from_iter` immediately
  after each `build_with_backfill()` (see §4.3). Because `self.compact(task)`
  runs **before** the state is updated, new blocks are backfilled and visible
  in cache *before* readers can see the new SSTs. Old blocks are invalidated
  later, after `self.compact(task)` returns and the old SST files are deleted.
- **Flush:** Backfill happens immediately after `build_with_backfill()` in
  `force_flush_next_imm_memtable`, still under `state_lock` and before the
  new SST is published:

```rust
let (sst, blocks) = builder.build_with_backfill(
    sst_id,
    Some(self.block_cache.clone()),
    path,
)?;
if let Some(ref cache) = self.block_cache {
    cache.backfill(sst_id, blocks);
}
// ... state update happens here ...
```

**Ordering:** Backfill completes **before** the new SSTs become visible to
readers. There is no window where a reader sees a new SST but misses its block
in cache. Old blocks remain in cache briefly after state update until
`invalidate_ssts` runs; this is safe and identical to today's behavior.

---

## 5. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Cache pollution from cold compaction data | Medium | Use TinyUFO's natural eviction; add `CACHE_RESIDENT_THRESHOLD` heuristic if needed. |
| Memory pressure during flush (Option B) | Low | Bounded by `target_sst_size` (default 1 MB). Flush already allocates the full SST buffer. |
| Extra I/O during flush (Option A) | Low | Flush is background; sequential reads are fast. |
| Race between backfill and concurrent read | Low | `force_put` is atomic. For flush, backfill runs under `state_lock`. For compaction, a brief window exists after `state_lock` is dropped where readers may see new SSTs before backfill; reads simply fall back to disk. |
| Reverse index leak (stale keys after eviction) | Low (pre-existing) | Same as today: evicted blocks leave stale reverse-index entries until SST deletion. Backfill does not worsen this. |
| `entry_count` drift | Low (pre-existing) | TinyUFO evictions do not notify the reverse index, so `entry_count` is an upper bound. Backfilled blocks that are later evicted contribute the same drift as `get_or_insert` does today. |

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
- Implement `CACHE_RESIDENT_THRESHOLD` sampling.
- A/B test against Phase 3.

### Phase 5: Value Cache Backfill (Optional / Future)
- Investigate vLog GC offset mapping for value cache backfill.

---

## 8. Benchmarking Plan

| Workload | Metric | Expected Improvement |
|----------|--------|---------------------|
| `fillseq` + immediate `readseq` | Read latency (p50/p99) | Up to 2–5× reduction when OS page cache has not yet warmed the SST file; less if the page cache already holds the data |
| `readrandom` after `fillrandom` | Cache hit rate | +10–20% hit rate for recent data |
| `readwhilewriting` after compaction | Read tail latency | Reduced p99 spikes |
| `fillseq` throughput | ops/sec | ≤1% regression (background backfill) |

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
   - **Tentative answer:** No. Keep recovery simple.

3. **How does backfill interact with `Arc<Block>` refcounting?**
   - `Block` data is `Bytes`, which uses atomic refcounts. Holding an extra
     `Arc<Block>` in cache is cheap.
   - No concerns.

---

## 10. Related Work

- **RocksDB:** Supports `cache_index_and_filter_blocks` and
  `pin_l0_filter_and_index_blocks_in_cache`. Does not auto-backfill data
  blocks on flush, but users can enable `readahead` and `compaction_readahead`.
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
