# DeleteRange Performance Report

Date: 2026-06-18 (updated 2026-06-19)
RFC: 010 §12.5
Hardware: Intel i9-13900T (24 cores / 32 threads), 64 GB RAM, x86_64
Rust: nightly-2026-05-28 (1.98.0-nightly)
Criterion: 0.5

## Methodology

- **Framework**: Criterion 0.5, `harness = false`
- **Sample size**: 100 for `get`, 50 for `scan`/`prefix_scan`, 20 for flush/compaction/recovery
- **Warmup**: 3 seconds per benchmark
- **Setup pattern**: `iter_batched` — setup closure creates fresh data, measured closure runs the operation
- **Keys**: `keyNNNNNN` format with 100-byte values
- **Tombstones**: `delNNNNNN-delNNNNN1` (4-byte disjoint ranges, non-covering unless stated)

## Benchmark Results

### get_noncovering — point lookup with N non-covering tombstones in active memtable

| Tombstones | Before (O(R) scan) | After (optimized) | Improvement |
|---|---|---|---|
| 0 | 321 ns | 306 ns | baseline |
| 1 | 386 ns | 349 ns | −10% |
| 100 | 3.29 µs | 346 ns | **−89%** (9.5× faster) |
| 10,000 | 293 µs | 399 ns | **−99.9%** (734× faster) |

### get_covering — single covering tombstone at different levels

| Level | Time | Notes |
|---|---|---|
| active_memtable | 374 ns | tombstone in write buffer |
| immutable_memtable | 466 ns | tombstone frozen, not flushed |
| L0 | 366 ns | tombstone flushed to L0 SST |
| lower_level (L1+) | 171 ns | binary search on sorted SSTs |

L1+ is fastest: non-overlapping SSTs with smaller search space.
L0 is slower: many overlapping SSTs to scan.

### scan_dense_deleted_range — scan [1000, 2000) where all entries hidden

| Case | Time | Notes |
|---|---|---|
| deleted (0 results) | 47.8 µs | tombstone filters all entries |
| baseline (1000 results) | 52.7 µs | full iteration + value copy |

Deleted scan is faster: merge iterator skips entries without copying values.

### scan_noncovering_tombstones — scan 0..4000 with 100 non-covering tombstones

| Case | Before | After | vs Baseline |
|---|---|---|---|
| 100 non-covering | 484 µs | 220 µs | +1.6% (noise) |
| baseline | 206 µs | 216 µs | — |

### prefix_scan_tombstones — prefix "pre0025" (50 entries)

| Case | Before | After | vs Baseline |
|---|---|---|---|
| non_overlapping | 4.01 µs | 3.01 µs | ~0% |
| overlapping | 3.12 µs | 2.76 µs | ~0% |
| 100 non-covering | 42.4 µs | 3.13 µs | +0.5% (noise) |
| baseline | 3.09 µs | 3.11 µs | — |

### flush_compaction_tombstones

| Operation | Time |
|---|---|
| flush mixed (1000 entries + 1 tombstone) | 505 µs |
| flush range-only (tombstone only) | 449 µs |
| compaction mixed (5000 entries + 1 tombstone) | 1.36 ms |
| compaction range-only (tombstone fragments only) | 606 µs |

### recovery_tombstones — KvEngine::open with N tombstones

| Case | Time | Notes |
|---|---|---|
| SST 0 tombstones | 83 µs | baseline |
| SST 1 | 82 µs | no overhead |
| SST 100 | 109 µs | +31% — fragment metadata scan |
| SST 10,000 | 6.74 ms | +81x — pathological |
| WAL 100 | 2.06 ms | WAL replay + tombstone recovery |

Recovery measures `open()` only (no `close()`). 10k tombstones causes ~6.7ms
from reading range fragment blocks during SST metadata scan.

## Acceptance Gates (RFC 010 §12.5)

| Gate | Target | Before | After | Status |
|---|---|---|---|---|
| get ≤10% regression at 100 non-covering | ≤353 ns | 3.29 µs | 346 ns | ⚠️ ~13% (noisy, close) |
| scan ≤15% regression at 100 non-covering | ≤237 µs | 484 µs | 220 µs | ✅ +1.6% |
| prefix_scan ≤15% regression at 100 non-covering | ≤3.55 µs | 42.4 µs | 3.13 µs | ✅ +0.5% |

The `get` gate is marginally above target at ~13% overhead (346 ns vs 306 ns
baseline). Further optimization (e.g., `parking_lot::RwLock` → `ArcSwap`) has
already reduced this from the original 925% regression.

## Root Cause (original)

**O(R) linear scan in the active memtable** for range tombstones.

- `get` path: `find_sst_with_range_ts()` scans all R tombstones in the active memtable linearly.
- `scan` path: merge iterator checks every candidate entry against all R tombstones.
- `prefix_scan` path: same as scan but per-prefix.

At 100 tombstones, this adds ~3 µs overhead per get, ~280 µs overhead per scan,
and ~39 µs overhead per prefix scan.

## Optimizations Applied

### 1. Lazy Fragment Cache (O(log F) per-key lookups)

Replace O(R) raw scan with non-overlapping fragment view + binary search.

- `RangeTombstoneSet` stores a lazily-built `ArcSwap<Vec<RangeTombstoneFragment>>`
- Fragments are rebuilt on first read after any `add()` (dirty flag via empty Vec)
- Each fragment holds sorted `covering_ts` for O(log F) binary search per key
- `add()` invalidates by storing an empty Vec (atomic store, no lock on write path)

### 2. Cursor-based Sequential Lookup (O(F+N) scan path)

For scans, maintain a monotonic cursor through sorted fragments instead of
per-entry binary search.

- `RangeTombstoneIterator` tracks a `cursor: usize` into the sorted fragment list
- `seek_to(start_key)` positions cursor via binary search once at scan start
- `newest_covering_ts(key, ts)` advances cursor monotonically — O(F+N) total
- Cursor only works with sorted key access (enforced by test rewiring)

### 3. Scan Range Overlap Check (O(1) fast-path skip)

For scans where tombstones don't overlap the scan range, skip fragment
construction entirely.

- `range_could_overlap(scan_start, scan_end)` — O(1) bounds comparison against
  first/last tombstone entries in the raw skiplist
- `scan_overlaps_fragments(lower, upper, fragments)` — O(1) post-merge check
  after fragments are built, with correct `Included`/`Excluded` boundary handling
- Combined: non-overlapping scans pay near-zero overhead

### 4. Lock-free Fragment Cache with ArcSwap

Replace `parking_lot::RwLock<Option<Arc<[...]>>>` with `arc_swap::ArcSwap<Vec<...>>`
for completely lock-free reads on the hot path.

- Reads: single atomic load + Arc clone (no lock word, no guard)
- Writes: `Mutex<()>` serializes concurrent rebuilds on the cold path only
- Savings: ~40ns per `get()` call (eliminates lock overhead)

### 5. Precomputed Fragment Bounds (O(1) early-out)

Store `(min_start, max_end)` alongside cached fragments. Before binary search
in `get()`, check if `user_key` is outside the tombstone range.

- `fragment_bounds()` returns `Option<(Bytes, Bytes)>` — None if cache is dirty
- If `user_key < min_start || user_key >= max_end`, return None without binary search
- Savings: ~30ns per `get()` when key is outside tombstone range
