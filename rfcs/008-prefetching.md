# RFC 008: Block and Value Prefetching

**Status:** Proposed  
**Date:** 2026-06-12  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding prefetching (read-ahead) to kv-engine's sequential read
paths. When the engine is scanning data in a predictable order — iterating SST
blocks, dereferencing value-log pointers, or transitioning between SSTs — it
should initiate the next I/O operation **before** the current one is consumed,
so the data is ready by the time it is needed.

The three prefetch targets, in priority order:

1. **Next-block prefetch** in `SsTableIterator` — read block N+1 while
   processing the last entries of block N.
2. **vLog value prefetch** — read the next 3–4 vLog entries while the current
   value is being consumed.
3. **Next-SST prefetch** in `SstConcatIterator` — open block 0 of the next SST
   while the current SST is being exhausted.

All three exploit the same insight: sequential scan access patterns are
deterministic, so the engine can overlap I/O with computation.

---

## 2. Motivation

### 2.1 The Per-Block Stall

`SsTableIterator::next()` loads the next block synchronously when the current
block is exhausted:

```rust
// table/iterator.rs:198-207
if !self.blk_iter.is_valid() {
    let idx = self.blk_idx + 1;
    if idx >= self.table.num_of_blocks() {
        return Ok(());
    }
    let b = self.table.read_block_cached(idx)?;   // blocking I/O
    self.blk_idx = idx;
    self.blk_iter = BlockIterator::create_and_seek_to_first(b);
}
```

When the block is not in the `BlockCache` (cold data, post-compaction, or
cache pressure), `read_block_cached` falls through to `FileObject::read`,
which issues a `pread` syscall. This stalls the scan for the full disk
latency (~10–100 µs on NVMe SSD, ~1–10 ms on HDD) for **every block
boundary**.

For a scan over a 100 MB SST with 4 KB blocks, this means ~25,000 block
transitions. Even on NVMe, that is 250 ms of pure I/O wait — not counting
decompression or key comparison.

### 2.2 The vLog Dereference Stall

With value separation enabled, every `SsTableIterator::value()` call for a
`ValuePointer` entry triggers a synchronous `vlog.read()`:

```rust
// table/iterator.rs:164-166
let bytes = vlog
    .read(&ptr, &vlog_key)
    .expect("SsTableIterator: failed to read value from vLog");
```

During a scan, vLog entries are accessed in write order (the SST was built
from a memtable flush that wrote vLog entries sequentially). This means the
next vLog offset is predictable from the current entry's `ValuePointer`.

Each dereference is a separate `pread` syscall. For a scan of 10,000 entries
with value separation, that is 10,000 syscalls — each blocking on disk I/O.

### 2.3 The SST Transition Stall

`SstConcatIterator::next()` opens the next SST and loads its first block
when the current SST is exhausted:

```rust
// iterators/concat_iterator.rs:167-175
let mut it = SsTableIterator::create_and_seek_to_first(
    self.sstables[self.next_sst_idx].clone(),
)?;
```

This involves opening the SST file (if not cached), reading the footer,
bloom filter, block metadata, and the first data block — a burst of 5+
syscalls. During leveled compaction or full scans, many SSTs are visited
sequentially, and this transition cost adds up.

### 2.4 Why Now

1. The engine already has a `BlockCache` (RFC 004) that handles cache hits
   efficiently. Prefetching targets the **cache-miss** path — cold data,
   large working sets, and post-compaction reads.
2. The scan iterator stack (`LsmIterator` → `MergeIterator` →
   `TwoMergeIterator` → `SstConcatIterator` → `SsTableIterator` →
   `BlockIterator`) is mature and well-understood.
3. Prefix scans (RFC 006) and prefix bloom filters (RFC 007) increase the
   importance of fast sequential reads.
4. A small thread pool (2–4 threads) with channels provides a simple,
   zero-dependency mechanism for background prefetch I/O. No async runtime or
   io_uring is required for the MVP.

---

## 3. Goals

1. Eliminate per-block I/O stalls during sequential SST scans by prefetching
   the next block before the current block is exhausted.
2. Pipeline vLog reads during scans with value separation.
3. Reduce SST transition latency in `SstConcatIterator`.
4. Keep the implementation simple: no async runtime, no new dependencies, no
   on-disk format changes.
5. Make prefetching configurable and disableable.
6. Do not regress point-get latency or memory usage.

## 4. Non-Goals

1. io_uring-based prefetching (see RFC 002 for future work).
2. Prefetching for point gets (random access patterns are not predictable).
3. Prefetching across the merge iterator (deferred — see Section 8.5 for
   `TwoMergeIterator` SST-side prefetch as a natural follow-up).
4. OS-level `posix_fadvise` hints (orthogonal, may be added separately).
5. Prefetching for WAL or manifest reads (not hot paths).

---

## 5. Design Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                     Prefetch Architecture                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   SsTableIterator                                                   │
│   ─────────────────                                                 │
│                                                                     │
│   Block N (current)          Block N+1 (prefetched)                 │
│   ┌──────────────┐           ┌──────────────┐                       │
│   │ entry 0      │           │              │                       │
│   │ entry 1      │  ──────►  │  loaded into │                       │
│   │ ...          │  prefetch │  lookahead   │                       │
│   │ entry K-3    │  trigger  │  buffer      │                       │
│   │ entry K-2    │           │              │                       │
│   │ entry K-1    │           └──────────────┘                       │
│   │ (last)       │                                                   │
│   └──────────────┘           On block transition: swap buffer       │
│                              into blk_iter, no I/O needed.          │
│                                                                     │
│   vLog Prefetch (when value separation enabled)                     │
│   ─────────────────────────────────────────────                     │
│                                                                     │
│   Current VP        Next VPs (prefetched)                           │
│   ┌────────┐        ┌────────┐ ┌────────┐ ┌────────┐               │
│   │ ptr A  │ ───►   │ ptr B  │ │ ptr C  │ │ ptr D  │               │
│   │ read() │        │ bg I/O │ │ bg I/O │ │ bg I/O │               │
│   └────────┘        └────────┘ └────────┘ └────────┘               │
│                                                                     │
│   SstConcatIterator                                                 │
│   ──────────────────                                                │
│                                                                     │
│   SST[i] (current)         SST[i+1] (prefetched)                    │
│   ┌──────────────┐         ┌──────────────┐                         │
│   │ last block   │ ──────► │ block 0      │                         │
│   │ near end     │ prefetch│ loaded into  │                         │
│   └──────────────┘ trigger │ lookahead    │                         │
│                            └──────────────┘                         │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.1 Design Principles

1. **Synchronous swap, async fill**: The prefetch I/O runs on a shared thread
   pool (2–4 threads). The iterator swap is synchronous — when the prefetched
   data is ready, it is consumed immediately with zero I/O.
2. **One entry lookahead**: Only one block / one batch of vLog values is
   prefetched at a time. This bounds memory overhead to ~1 extra block
   (~4–8 KB) plus a few vLog entries.
3. **Fail-open**: If prefetching fails or is not ready in time, the iterator
   falls back to synchronous I/O. Prefetching is a performance optimization,
   not a correctness requirement.
4. **Configurable**: `LsmStorageOptions` gains `enable_prefetch: bool`
   (default `true`), `prefetch_block_threshold: usize` (default `4`),
   `prefetch_vlog_depth: usize` (default `3`), and `prefetch_pool_threads`
   (default `2`).

---

## 6. Detailed Design

### 6.1 Prefetch Handle and Thread Pool

A lightweight handle for a background I/O operation, backed by a shared thread
pool:

```rust
use std::sync::mpsc;

/// A pending prefetch result. The background thread sends the result
/// through a channel; `join()` blocks until it is ready.
struct PrefetchHandle<T> {
    rx: mpsc::Receiver<Result<T>>,
}

impl<T> PrefetchHandle<T> {
    /// Block until the prefetch result is available.
    fn join(self) -> Result<T> {
        self.rx
            .recv()
            .map_err(|_| anyhow::anyhow!("prefetch thread panicked"))?
    }
}
```

The handle is created by submitting a closure to the `PrefetchPool`:

```rust
/// A fixed-size thread pool for prefetch I/O tasks.
struct PrefetchPool {
    tx: crossbeam_channel::Sender<Box<dyn FnOnce() + Send>>,
}

impl PrefetchPool {
    fn new(num_threads: usize) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        for _ in 0..num_threads {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name("prefetch".into())
                .stack_size(256 * 1024)  // 256 KB — enough for pread + decode
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        job();
                    }
                })
                .expect("failed to spawn prefetch thread");
        }
        PrefetchPool { tx }
    }

    /// Submit a prefetch task and return a handle for the result.
    fn submit<F, T>(&self, f: F) -> PrefetchHandle<T>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = mpsc::channel();
        self.tx
            .send(Box::new(move || {
                let result = f();
                let _ = result_tx.send(result);
            }))
            .expect("prefetch pool shut down");
        PrefetchHandle { rx: result_rx }
    }
}
```

The pool is created once per engine instance (2–4 threads) and shared across
all iterators via `Arc<PrefetchPool>`. This eliminates per-prefetch thread
creation overhead: a scan over 25,000 blocks reuses the same 2–4 threads
instead of spawning 25,000 `pthread_create` calls.

**Why threads, not async:** The engine is synchronous today. Adding an async
runtime (tokio, smol) for prefetching alone would be disproportionate. A small
thread pool is simple, has bounded memory overhead (~256 KB × 4 = 1 MB total),
and the prefetch tasks are short-lived (one I/O call each).

**Why a pool from day one:** Thread creation overhead is ~10–50 µs on Linux,
~50–100 µs on macOS. A scan with 25,000 block transitions would spend 1.25
seconds on thread creation alone on macOS — negating the I/O overlap benefit.
The pool eliminates this entirely.

**`PrefetchPool` instance:** Created in `LsmStorageInner::new()` and stored as
`Arc<PrefetchPool>`. Passed to iterators via `set_prefetch_pool()` (see
Section 6.5). The pool is `Send + Sync` and cheap to clone via `Arc`.

### 6.2 Next-Block Prefetch in `SsTableIterator`

Add a lookahead buffer to `SsTableIterator`:

```rust
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    vlog: Option<Arc<ValueLog>>,
    deref_cache: UnsafeCell<Option<(KeyBytes, Vec<u8>)>>,
    // --- new fields ---
    /// Prefetched next block, ready to consume.
    prefetched_block: Option<(usize, Arc<Block>)>,
    /// Handle for in-flight prefetch I/O.
    prefetch_handle: Option<PrefetchHandle<(usize, Arc<Block>)>>,
    /// Whether prefetching is enabled for this iterator.
    prefetch_enabled: bool,
    /// Shared prefetch thread pool. None = prefetching disabled.
    prefetch_pool: Option<Arc<PrefetchPool>>,
    /// Config: entries remaining before triggering block prefetch.
    prefetch_block_threshold: usize,
    /// Config: vLog prefetch depth.
    prefetch_vlog_depth: usize,
}
```

#### Trigger Condition

When `BlockIterator::next()` advances past a threshold within the current
block, fire the prefetch:

```rust
impl SsTableIterator {
    /// Returns true when the block iterator is close enough to the end
    /// that we should start prefetching the next block.
    fn should_prefetch(&self) -> bool {
        if !self.prefetch_enabled {
            return false;
        }
        if self.prefetched_block.is_some() || self.prefetch_handle.is_some() {
            return false; // already prefetched or in-flight
        }
        let remaining = self.blk_iter.remaining_entries();
        remaining <= self.prefetch_block_threshold
    }

    fn fire_prefetch(&mut self) {
        let next_idx = self.blk_idx + 1;
        if next_idx >= self.table.num_of_blocks() {
            return; // no next block
        }
        let pool = self.prefetch_pool.as_ref().unwrap();
        let table = self.table.clone();
        self.prefetch_handle = Some(pool.submit(move || {
            let block = table.read_block_cached(next_idx)?;
            Ok((next_idx, block))
        }));
    }
}
```

#### Consumption in `next()`

```rust
fn next(&mut self) -> Result<()> {
    *self.deref_cache.get_mut() = None;
    self.blk_iter.next();

    // Trigger prefetch if we're near the end of the current block.
    if self.should_prefetch() {
        self.fire_prefetch();
    }

    if !self.blk_iter.is_valid() {
        let idx = self.blk_idx + 1;
        if idx >= self.table.num_of_blocks() {
            return Ok(());
        }

        // Try to consume the prefetched block first.
        let b = if let Some((prefetched_idx, block)) = self.prefetched_block.take() {
            debug_assert_eq!(prefetched_idx, idx);
            block
        } else if let Some(handle) = self.prefetch_handle.take() {
            let (prefetched_idx, block) = handle.join()?;
            debug_assert_eq!(prefetched_idx, idx);
            block
        } else {
            // Fallback: synchronous read (prefetch disabled or missed).
            self.table.read_block_cached(idx)?
        };

        self.blk_idx = idx;
        self.blk_iter = BlockIterator::create_and_seek_to_first(b);
    }

    Ok(())
}
```

#### BlockIterator Extension

`BlockIterator` needs a `remaining_entries()` method:

```rust
impl BlockIterator {
    /// Number of entries remaining from the current position to the end
    /// of the block.
    pub(crate) fn remaining_entries(&self) -> usize {
        if self.idx >= self.block.offsets.len() {
            0
        } else {
            self.block.offsets.len() - self.idx
        }
    }
}
```

#### Prefetch Trigger Threshold

`prefetch_block_threshold` (default 4). When 4 or fewer entries remain in
the current block, fire the prefetch. Since the trigger fires inside `next()`
after advancing past one entry, this gives the background thread 3 entries of
processing time (not 4) to complete the I/O before the block transition.

**Tuning:** The threshold should be ≥ 1 and ≤ block_size / min_entry_size. For
a 4 KB block with 100-byte entries (~40 entries per block), a threshold of 4 is
conservative. A larger threshold (e.g., 5–10) provides more I/O overlap but
prefetches blocks that may not be needed if the scan stops early.

### 6.3 vLog Value Prefetch

When `SsTableIterator` has a vLog and the current entry is a `ValuePointer`,
peek at upcoming block entries to prefetch their vLog values.

#### Interior Mutability Note

`StorageIterator::value()` takes `&self`, so any fields mutated inside
`value()` need interior mutability. The existing `deref_cache` already uses
`UnsafeCell`. The new `prefetched_values` field uses the same pattern.

```rust
pub struct SsTableIterator {
    // ... existing fields ...
    /// Prefetched vLog values: (entry_idx, key_bytes, value_bytes).
    /// Uses fixed-size array (PREFETCH_VLOG_DEPTH ≤ 4). Wrapped in
    /// UnsafeCell because `value()` takes `&self` and must remove entries.
    prefetched_values: UnsafeCell<[Option<(usize, KeyBytes, Vec<u8>)>; 4]>,
    /// Handles for in-flight vLog prefetch I/O.
    vlog_prefetch_handles: Vec<PrefetchHandle<(usize, KeyBytes, Vec<u8>)>>,
}
```

**Why a fixed-size array instead of `HashMap`:** With at most 4 entries, a
`[Option<...>; 4]` with linear scan avoids hash computation, heap allocation,
and cache-unfriendly linked lists. The `contains_key` check becomes a simple
loop over 4 elements.

#### BlockIterator Extensions for vLog Prefetch

`BlockIterator` needs methods to peek at arbitrary entries without advancing.
These are `pub(crate)` — they exist solely to serve the vLog prefetch logic
inside `SsTableIterator`:

```rust
impl BlockIterator {
    /// Current entry index.
    pub(crate) fn idx(&self) -> usize {
        self.idx
    }

    /// Number of entries in the block.
    pub(crate) fn block_offsets_len(&self) -> usize {
        self.block.offsets.len()
    }

    /// Read the raw value at entry `idx` without changing iterator position.
    pub(crate) fn value_at(&self, idx: usize) -> &[u8] {
        if idx >= self.block.offsets.len() {
            return &[];
        }
        let offset = self.block.offsets[idx] as usize;
        let mut data = &self.block.data[offset..];
        let _overlap_len = data.get_u16() as usize;
        let ret_key_len = data.get_u16() as usize;
        data.advance(ret_key_len);
        let value_len = data.get_u16() as usize;
        let start = offset + SIZE_OF_U16 * 2 + ret_key_len + SIZE_OF_U16;
        &self.block.data[start..start + value_len]
    }
}
```

**Note:** `key_at()` is intentionally omitted from the MVP. Reconstructing a
`KeySlice` at an arbitrary index requires managing a temporary `KeyVec` whose
lifetime must outlive the `PrefetchHandle` closure. Instead, the vLog prefetch
closure captures the `ValuePointer` (which is `Copy`) and reads the key as an
owned `Vec<u8>` by copying from the block data before constructing the closure.
This avoids complex lifetime management.

#### Trigger and Execution

After each `next()` call, if vLog is enabled, scan the next few block entries
for `ValuePointer` entries and prefetch them. **All data is copied into owned
locals before constructing the closure** to satisfy `Send + 'static`:

```rust
fn maybe_prefetch_vlog_values(&mut self) {
    if self.vlog.is_none() || !self.prefetch_enabled {
        return;
    }
    let pool = self.prefetch_pool.as_ref().unwrap();

    let start_idx = self.blk_iter.idx() + 1;
    let end_idx = (start_idx + self.prefetch_vlog_depth)
        .min(self.blk_iter.block_offsets_len());

    for entry_idx in start_idx..end_idx {
        // Check if already prefetched.
        let values = unsafe { &*self.prefetched_values.get() };
        if values.iter().any(|v| v.as_ref().map_or(false, |(i, _, _)| *i == entry_idx)) {
            continue;
        }
        // Check if we've hit the capacity limit.
        if self.vlog_prefetch_handles.len() >= self.prefetch_vlog_depth {
            break;
        }

        // Peek at the raw value — copy into owned Vec to avoid borrowing self.
        let raw: Vec<u8> = self.blk_iter.value_at(entry_idx).to_vec();
        if raw.is_empty() || raw[0] != KvKind::ValuePointer as u8 {
            continue;
        }
        let payload = &raw[1..];
        let ptr = match ValuePointer::try_decode(payload) {
            Some(p) => p,
            None => continue,
        };

        // Copy the key bytes out of the block for the closure.
        // Reconstruct from first_key prefix + suffix at entry_idx.
        let vlog_key = self.blk_iter.key_bytes_at(entry_idx);  // returns Vec<u8>

        let vlog = self.vlog.as_ref().unwrap().clone();
        let vlog_key_for_key_bytes = vlog_key.clone();
        self.vlog_prefetch_handles.push(pool.submit(move || {
            let bytes = vlog.read(&ptr, &vlog_key)?;
            let key_bytes = KeyBytes::from_bytes(
                crate::key::Key::from_vec(vlog_key_for_key_bytes).into_key_bytes(),
            );
            Ok((entry_idx, key_bytes, bytes.to_vec()))
        }));
    }
}
```

#### Result Collection

The vLog prefetch handles must be drained and their results inserted into
`prefetched_values` before `value()` tries to read them. This happens in
`next()`, after calling `maybe_prefetch_vlog_values()`:

```rust
fn next(&mut self) -> Result<()> {
    // ... existing next() logic ...

    // After triggering block prefetch and advancing:
    self.maybe_prefetch_vlog_values();

    // Collect completed vLog prefetch results.
    let values = unsafe { &mut *self.prefetched_values.get() };
    self.vlog_prefetch_handles.retain(|handle| {
        // Non-blocking check: if the result is ready, store it.
        match handle.try_join() {
            std::sync::mpsc::TryRecvError::Empty => true, // keep handle
            std::sync::mpsc::TryRecvError::Disconnected => false, // thread panicked
            Ok(Ok((idx, key, val))) => {
                // Insert into first empty slot.
                if let Some(slot) = values.iter_mut().find(|s| s.is_none()) {
                    *slot = Some((idx, key, val));
                }
                false
            }
            Ok(Err(_)) => false, // I/O error; fall back to sync read in value()
        }
    });

    Ok(())
}
```

**Note:** `try_join()` is used here (non-blocking) rather than `join()`
(blocking) so that `next()` does not stall waiting for vLog I/O that may not
be needed yet. If the result is not ready, the handle is retained and checked
again on the next `next()` call. If `value()` is called before the result
arrives, it falls through to the synchronous path.

#### Consumption in `value()`

In the `ValuePointer` branch of `value()`, check the prefetch cache first:

```rust
Some(KvKind::ValuePointer) => {
    let current_idx = self.blk_iter.idx();
    // Check prefetched values.
    let values = unsafe { &mut *self.prefetched_values.get() };
    if let Some(slot) = values.iter_mut().find(|s| {
        s.as_ref().map_or(false, |(i, _, _)| *i == current_idx)
    }) {
        let (_, key_bytes, val) = slot.take().unwrap();
        let cache_mut = unsafe { &mut *self.deref_cache.get() };
        *cache_mut = Some((key_bytes, val));
        return &cache_mut.as_ref().unwrap().1;
    }
    // ... existing synchronous fallback ...
}
```

### 6.4 Next-SST Prefetch in `SstConcatIterator`

When the current SST is near exhaustion, prefetch block 0 of the next SST.

**Note:** `SsTable` objects are pre-opened when loaded; their metadata (footer,
bloom filter, block metadata) is already in memory. Only the data blocks
themselves require disk I/O. So prefetching block 0 is a single `pread` call,
not a full SST open.

```rust
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
    vlog: Option<Arc<ValueLog>>,
    // --- new fields ---
    /// Prefetched first block of the next SST.
    prefetched_first: Option<(usize, Arc<Block>)>,
    /// Handle for in-flight next-SST prefetch.
    sst_prefetch_handle: Option<PrefetchHandle<(usize, Arc<Block>)>>,
    /// Whether prefetching is enabled.
    prefetch_enabled: bool,
    /// Shared prefetch thread pool.
    prefetch_pool: Option<Arc<PrefetchPool>>,
}
```

#### Trigger

The trigger fires when the `SsTableIterator` signals it has entered its
**last block**. Add a method to `SsTableIterator`:

```rust
impl SsTableIterator {
    /// Returns true if the iterator is currently on the last block of the SST.
    pub(crate) fn on_last_block(&self) -> bool {
        self.blk_idx + 1 >= self.table.num_of_blocks()
    }
}
```

#### New Constructor for Prefetched Block

`create_and_seek_to_first` already loads block 0 via `read_block_cached(0)`.
To avoid wasting that I/O when a prefetched block is available, add a
constructor that accepts a pre-loaded block:

```rust
impl SsTableIterator {
    /// Create an iterator using a pre-fetched block 0. Skips the initial
    /// `read_block_cached(0)` call. Used by `SstConcatIterator` to consume
    /// prefetched blocks without redundant I/O.
    pub(crate) fn create_with_prefetched_block(
        table: Arc<SsTable>,
        block: Arc<Block>,
    ) -> Self {
        SsTableIterator {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
            vlog: None,
            deref_cache: UnsafeCell::new(None),
            prefetched_block: None,
            prefetch_handle: None,
            prefetch_enabled: false,
            prefetch_pool: None,
            prefetch_block_threshold: 4,
            prefetch_vlog_depth: 0,
            prefetched_values: UnsafeCell::new([None, None, None, None]),
            vlog_prefetch_handles: Vec::new(),
        }
    }
}
```

#### Consumption in `SstConcatIterator::next()`

```rust
fn next(&mut self) -> Result<()> {
    self.current.as_mut().unwrap().next()?;

    // Trigger next-SST prefetch when current SST enters its last block.
    if self.prefetch_enabled
        && self.sst_prefetch_handle.is_none()
        && self.prefetched_first.is_none()
        && self.next_sst_idx < self.sstables.len()
        && self.current.as_ref().map_or(false, |c| c.on_last_block())
    {
        let next_sst = self.sstables[self.next_sst_idx].clone();
        let idx = self.next_sst_idx;
        let pool = self.prefetch_pool.as_ref().unwrap();
        self.sst_prefetch_handle = Some(pool.submit(move || {
            let block = next_sst.read_block_cached(0)?;
            Ok((idx, block))
        }));
    }

    while let Some(ref current) = self.current {
        if current.is_valid() {
            break;
        }
        if self.next_sst_idx < self.sstables.len() {
            let sst = self.sstables[self.next_sst_idx].clone();

            // Branch: use prefetched block if available, otherwise sync read.
            let mut it = if let Some((_, block)) = self.prefetched_first.take() {
                SsTableIterator::create_with_prefetched_block(sst, block)
            } else if let Some(handle) = self.sst_prefetch_handle.take() {
                match handle.join() {
                    Ok((_, block)) => {
                        SsTableIterator::create_with_prefetched_block(sst, block)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "next-SST prefetch failed, falling back to sync: {e}"
                        );
                        SsTableIterator::create_and_seek_to_first(sst)?
                    }
                }
            } else {
                SsTableIterator::create_and_seek_to_first(sst)?
            };

            if let Some(ref v) = self.vlog {
                it.set_vlog(v.clone());
            }
            it.prefetch_enabled = self.prefetch_enabled;
            it.prefetch_pool = self.prefetch_pool.clone();
            self.current = Some(it);
            self.next_sst_idx += 1;
        } else {
            self.current = None;
        }
    }

    Ok(())
}
```

### 6.5 Configuration and Propagation

Add to `LsmStorageOptions`:

```rust
pub struct LsmStorageOptions {
    // ... existing fields ...

    /// Enable block and value prefetching during sequential scans.
    /// Default: true.
    pub enable_prefetch: bool,

    /// Number of entries remaining in a block before triggering next-block
    /// prefetch. Default: 4 (gives 3 entries of overlap).
    pub prefetch_block_threshold: usize,

    /// Number of vLog entries to prefetch ahead during scans with value
    /// separation. Default: 3.
    pub prefetch_vlog_depth: usize,

    /// Number of threads in the prefetch pool. Default: 2.
    pub prefetch_pool_threads: usize,
}
```

#### Propagation Path

```
LsmStorageInner::new(options)
  │
  ├── Creates PrefetchPool::new(options.prefetch_pool_threads)
  │   stored as self.prefetch_pool: Arc<PrefetchPool>
  │
  ├── scan_inner() constructs iterators at 8 sites:
  │   ├── L0: SsTableIterator::create_and_seek_to_first(t.clone())?
  │   │   └── After construction: it.set_prefetch_config(
  │   │           self.options.enable_prefetch,
  │   │           self.prefetch_pool.clone(),
  │   │           self.options.prefetch_block_threshold,
  │   │           self.options.prefetch_vlog_depth,
  │   │       )
  │   └── L1+: SstConcatIterator::create_and_seek_to_first(ss_tables)?
  │       └── After construction: it.set_prefetch_config(
  │               self.options.enable_prefetch,
  │               self.prefetch_pool.clone(),
  │           )
  │
  └── compact() / compact_from_iter() constructs iterators:
      └── Same pattern: set_prefetch_config() after construction
```

Add a `set_prefetch_config` method to both iterator types (mirroring the
existing `set_vlog` pattern):

```rust
impl SsTableIterator {
    pub(crate) fn set_prefetch_config(
        &mut self,
        enabled: bool,
        pool: Option<Arc<PrefetchPool>>,
        block_threshold: usize,
        vlog_depth: usize,
    ) {
        self.prefetch_enabled = enabled && pool.is_some();
        self.prefetch_pool = pool;
        self.prefetch_block_threshold = block_threshold;
        self.prefetch_vlog_depth = vlog_depth;
    }
}

impl SstConcatIterator {
    pub(crate) fn set_prefetch_config(
        &mut self,
        enabled: bool,
        pool: Option<Arc<PrefetchPool>>,
    ) {
        self.prefetch_enabled = enabled && pool.is_some();
        self.prefetch_pool = pool;
    }
}
```

**Compaction always prefetches:** Compaction iterators are created inside
`compact_from_iter` and `compact`. They should **always** have prefetching
enabled regardless of the user-facing `enable_prefetch` option, because
compaction is always sequential and always benefits from read-ahead. The
`set_prefetch_config` call in compaction uses `true` unconditionally:

```rust
// In compact_from_iter:
let mut it = SsTableIterator::create_and_seek_to_first(t.clone())?;
it.set_prefetch_config(true, Some(self.prefetch_pool.clone()), ...);
```

### 6.6 Thread Safety

All prefetch operations read immutable data:

- `SsTable::read_block_cached` reads from an immutable `FileObject` and the
  `BlockCache` (which is `Send + Sync`).
- `ValueLog::read` reads from an immutable `ValueLogReader` and the
  `ValueCache` (which is `Send + Sync`).
- `Arc<SsTable>` and `Arc<ValueLog>` are cloned into the prefetch thread.

No mutable state is shared between the iterator thread and the prefetch thread.
Communication is one-shot via `mpsc::channel`.

### 6.7 Error Handling

- **Block prefetch I/O error:** The error is returned through the channel. The
  iterator consumes it via `handle.join()` and propagates it with `?`. This
  is identical to a synchronous I/O error.
- **SST prefetch I/O error:** Logged at `warn` level, then falls back to
  synchronous `create_and_seek_to_first`. The sync path will hit the same
  error and propagate it. The log preserves diagnostic information.
- **Prefetch thread panic:** `join()` returns an error. The iterator
  propagates it.
- **Scan stops early (e.g., prefix scan hits upper bound):** The prefetched
  block is never consumed. It is dropped when the iterator is dropped. The
  pool thread finishes and the channel is closed. The wasted I/O is
  bounded to 1 block + a few vLog entries — acceptable.
- **Iterator seek (random jump):** On `seek_to_key` or `seek_to_first`,
  any in-flight prefetch is discarded:
  ```rust
  pub(crate) fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
      self.prefetched_block = None;
      self.prefetch_handle = None;
      // Clear vLog prefetch state (UnsafeCell access)
      let values = unsafe { &mut *self.prefetched_values.get() };
      for slot in values.iter_mut() {
          *slot = None;
      }
      self.vlog_prefetch_handles.clear();
      // ... existing seek logic ...
  }
  ```
- **vLog GC invalidation:** If a background GC rewrites a vLog file between
  when the prefetch is fired and when the prefetched value is consumed, the
  old offset may point to a different entry. The `ValueLog::read` key
  validation catches this and returns an error, which propagates through the
  channel. The iterator falls back to synchronous read (which will also fail
  for a stale pointer). This is a correctness issue in the GC layer (GC must
  update SST entries atomically), not in prefetching. The prefetch design
  handles it correctly by propagating errors.

---

## 7. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Wasted I/O when scan stops early | Low | Bounded to 1 block (~4–8 KB) + a few vLog entries. Acceptable for sequential workloads. |
| Thread pool memory overhead | Low | 2–4 threads × 256 KB stack = ~1 MB total. Shared across all iterators. |
| Memory overhead from prefetched blocks | Low | 1 extra block in memory (~4–8 KB) + up to 4 vLog values. Bounded and configurable. |
| Prefetch cache pollution | Low | The prefetched block enters the cache via the same `try_get_with` path as a synchronous read. If the block is already cached, no I/O occurs. The prefetch does not bypass admission. |
| Interaction with `seek_to_key` mid-scan | Low | In-flight prefetch is discarded on seek. No stale data is served. Prefetched block may remain in BlockCache (acceptable waste). |
| Concurrent iterator instances sharing BlockCache | Low | Each iterator has its own prefetch handle. The BlockCache handles concurrent access via single-flight miss coalescing. |
| Pool thread holds `Arc<SsTable>` after iterator drop | Low | The pool thread finishes its current I/O and drops the `Arc`. On Unix, unlinked files remain readable through open FDs, so this is safe even during compaction. |
| Tombstone skip triggers unnecessary prefetches | Low | `LsmIterator::skip_tombstones` calls `next()` in a loop. Each call may trigger `should_prefetch()`. At most one prefetch is in flight at a time. The wasted I/O is bounded to 1 block per skip sequence. |
| `TwoMergeIterator` advances skipped child | Low | When `TwoMergeIterator` advances the non-winning child to skip a duplicate, it may trigger a prefetch for a block that is never consumed. Bounded to 1 block per duplicate skip. |

---

## 8. Alternatives Considered

### 8.1 io_uring / Async I/O

Use `io_uring` for zero-copy, kernel-level read-ahead (RFC 002).

- **Pros:** No userspace threads; kernel handles I/O scheduling; lower overhead.
- **Cons:** Requires an async runtime or manual `io_uring` management; Linux
  5.1+ only; more complex error handling; RFC 002 is not yet implemented.
- **Verdict:** Future enhancement. Threads are sufficient for the MVP and
  work on all platforms.

### 8.2 `posix_fadvise(POSIX_FADV_WILLNEED)`

Tell the kernel to prefetch into the page cache.

- **Pros:** Zero userspace overhead; kernel handles scheduling.
- **Cons:** Hints are advisory and may be ignored under memory pressure;
  does not integrate with the application's block cache; only helps when
  the OS page cache is not already warm.
- **Verdict:** Complementary optimization. May be added alongside prefetching
  as a lightweight hint for cold SSTs.

### 8.3 Larger Block Size

Increase the default block size from 4 KB to 16–64 KB to reduce the number
of block transitions.

- **Pros:** Fewer I/O calls; simpler implementation.
- **Cons:** Larger blocks waste space for point reads (must decompress the
  entire block for one key); increases memory pressure on the block cache;
  does not help vLog dereferences or SST transitions.
- **Verdict:** Orthogonal. Block size is a tuning parameter, not a prefetch
  replacement.

### 8.4 Read-Ahead in `FileObject`

Add a `read_ahead` method to `FileObject` that issues a `pread` for the next
N bytes at the current offset.

- **Pros:** Simpler than thread-based prefetching; no thread overhead.
- **Cons:** Still synchronous — the caller blocks until the read-ahead
  completes. Only helps if the kernel pipeline is deep enough to overlap
  with userspace processing.
- **Verdict:** Insufficient. The whole point is to overlap I/O with
  computation, which requires asynchronous I/O.

### 8.5 Prefetch in the Merge Iterator

Prefetch from child iterators in `MergeIterator` or `TwoMergeIterator`.

- **Pros:** Could reduce latency for multi-way merges.
- **Cons:** For `MergeIterator` with many children, the merge order is not
  fully predictable — after advancing the current front, a heap swap may
  select a different child. Prefetching from all children simultaneously
  would issue I/O for entries that may not be needed.
- **`TwoMergeIterator` exception:** The `TwoMergeIterator` used in the scan
  path merges a memtable (in-memory, no I/O) with SST layers. After each
  advance, it knows which side was advanced. Prefetching the SST-side
  iterator's next block is straightforward and nearly always correct (the
  SST side is advanced for most keys in a typical workload). This is a
  natural Phase 3.5 enhancement that composes cleanly with the per-iterator
  prefetching in this RFC.
- **Verdict:** Deferred for `MergeIterator`. The `TwoMergeIterator` SST-side
  prefetch is a low-hanging follow-up.

---

## 9. Implementation Plan

### Phase 1: Thread Pool + Next-Block Prefetch

- Add `PrefetchPool` (2 threads, 256 KB stacks).
- Add `PrefetchHandle<T>`.
- Add `remaining_entries()` to `BlockIterator` (`pub(crate)`).
- Add `prefetched_block`, `prefetch_handle`, `prefetch_enabled`,
  `prefetch_pool` fields to `SsTableIterator`.
- Implement `should_prefetch()`, `fire_prefetch()`, and updated `next()`.
- Add `LsmStorageOptions` fields: `enable_prefetch`, `prefetch_block_threshold`,
  `prefetch_pool_threads`.
- Add `set_prefetch_config()` to `SsTableIterator` and `SstConcatIterator`.
- Wire up `scan_inner()` and `compact_from_iter()` iterator construction.
- Benchmark: `fillseq` → drop page cache → `readseq` latency.

### Phase 2: vLog Value Prefetch

- Add `value_at()` peek method to `BlockIterator` (`pub(crate)`).
- Add `prefetched_values` (UnsafeCell + fixed-size array) and
  `vlog_prefetch_handles` to `SsTableIterator`.
- Implement `maybe_prefetch_vlog_values()` with owned-data closure pattern.
- Add result collection step in `next()` using `try_join()`.
- Update `value()` to check prefetched values first.
- Add `LsmStorageOptions::prefetch_vlog_depth`.
- Benchmark: prefix scan over value-separated data.

### Phase 3: Next-SST Prefetch in `SstConcatIterator`

- Add `on_last_block()` to `SsTableIterator` (`pub(crate)`).
- Add `create_with_prefetched_block()` constructor to `SsTableIterator`.
- Add prefetch fields to `SstConcatIterator`.
- Implement trigger and consumption logic with error logging.
- Benchmark: leveled compaction throughput, full scan over multiple SSTs.

### Phase 4: TwoMergeIterator SST-Side Prefetch (future)

- After advancing the SST side in `TwoMergeIterator::next()`, fire a
  prefetch on that iterator's next block.
- The memtable side is in-memory and needs no prefetching.
- Composes with Phase 1–3 per-iterator prefetching.

---

## 10. Benchmarking Plan

### 10.1 Workloads

| Workload | Description | What It Tests |
|----------|-------------|---------------|
| `fillseq` → cold cache → `readseq` | Write sequentially, drop page cache, read sequentially | Next-block prefetch benefit on cold data |
| `fillrandom` + prefix scan (cold cache) | Write randomly, drop page cache, prefix scan | Block + vLog prefetch combined |
| `fillrandom` + value separation → prefix scan | Write large values to vLog, prefix scan | vLog prefetch benefit |
| Full scan over 10 SSTs (cold cache) | Multi-SST sequential scan | SST transition prefetch |
| Leveled compaction (cold cache) | Trigger L1+ compaction, measure throughput | Compaction block prefetch |
| `readrandom` (point gets) | Random point reads | Regression check (prefetch should not hurt) |

### 10.2 Metrics

| Metric | Why |
|--------|-----|
| Scan latency (p50, p99) | Primary improvement metric |
| I/O syscalls per scan | Should decrease with prefetching |
| Scan throughput (entries/sec) | Overall improvement |
| Point-get latency (p50, p99) | Regression check |
| Peak RSS | Memory overhead from prefetch buffers |
| Prefetch hit rate | How often the prefetched data was actually consumed |
| Prefetch waste rate | How often prefetched data was dropped unused |

### 10.3 Expected Improvements

**Amdahl's law analysis:** The improvement is bounded by the fraction of time
spent on I/O stalls at block boundaries. For a cold scan on NVMe SSD:

- Block read latency: ~50 µs (midrange NVMe)
- Entry processing per entry: ~3 µs (key comparison + decompression)
- Entries per block: ~40 (4 KB / 100 bytes)
- Time per block: 40 × 3 µs = 120 µs processing + 50 µs I/O = 170 µs
- I/O fraction: 50/170 = ~29%
- With perfect prefetch (I/O fully overlapped): 120 µs per block
- **Theoretical max speedup: 1.42× (~30% latency reduction)**

For HDD (1–10 ms I/O), the I/O fraction dominates and higher speedups are
achievable.

| Scenario | Expected Impact | Rationale |
|----------|----------------|-----------|
| Cold sequential scan (NVMe SSD) | 20–40% latency reduction | I/O fraction ~29%; perfect overlap saves 30%. Real-world overhead (cache lookups, decompression) reduces to 20–40%. |
| Cold sequential scan (HDD) | 50–80% latency reduction | I/O fraction >90%; nearly all time is I/O wait. |
| vLog prefix scan (cold, depth=3) | 15–30% latency reduction | With depth 3 and ~3 µs/entry processing, we overlap 9 µs of processing with ~50 µs of I/O per read. Increasing `prefetch_vlog_depth` to 10–20 would approach 50–70%. |
| Multi-SST scan (cold) | 15–30% latency reduction | Eliminates SST transition stalls (~5 syscalls per transition). |
| Compaction throughput (cold) | 20–40% latency reduction | Compaction reads every block sequentially; same I/O fraction analysis as cold scan. |
| Point gets (regression) | <1% change | Prefetch is not triggered for random access patterns. |

---

## 11. Open Questions

1. **Should we prefetch block 0 of the next SST during `seek_to_key` in
   `SstConcatIterator`?** If the seek lands on an SST, we know the next SST
   to visit. Prefetching block 0 eagerly could help, but adds complexity for
   a less common path. Note: after `create_and_seek_to_key`, the prefetched
   block 0 would be discarded because the constructor seeks to a specific
   block, not block 0. So this optimization only helps if we also prefetch
   the seeked-to block (not just block 0).

2. **Should the prefetch threshold adapt dynamically?** A fixed threshold of
   4 entries works for typical block sizes. An adaptive threshold based on
   observed I/O latency (from previous prefetch completions) could be more
   robust across storage types. Defer to Phase 4 if needed.

3. **Should we integrate `posix_fadvise(WILLNEED)` alongside thread-based
   prefetching?** The kernel hint is nearly free and may help for data that
   is not in the block cache but is in the OS page cache. Could be added as
   a complementary optimization in Phase 2.

4. **Should the pool size adapt to workload?** 2 threads is sufficient for
   sequential scans (the prefetch I/O completes in ~50 µs, well before the
   next block transition at ~170 µs). For vLog prefetch with depth >10,
   more threads may be needed. Consider making pool size proportional to
   `prefetch_vlog_depth`.

---

## 12. References

- **RocksDB:** `compaction_readahead_size` and `level0_file_num_compaction_trigger`
  options for read-ahead during compaction. `NewInternalIterator` can issue
  `ReadAsync` for block reads.
- **LevelDB:** No explicit prefetching; relies on OS read-ahead for sequential
  `pread` calls.
- **BadgerDB:** No prefetching; uses `mmap` for value log reads.
- **Pebble (CockroachDB):** Has `readahead` for compaction and scan iterators
  using a state machine that tracks sequential access patterns.
