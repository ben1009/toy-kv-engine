# DeleteRange Performance Report

Date: 2026-06-18
RFC: 010 §12.5
Hardware: (fill in)
Rust: stable
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

| Tombstones | Time | vs Baseline |
|---|---|---|
| 0 | 321 ns | baseline |
| 1 | 386 ns | +20% |
| 100 | 3.29 µs | +925% |
| 10,000 | 293 µs | +91,200% |

Bottleneck: O(R) linear scan over all tombstones in the active memtable
per `get()` call. Each tombstone = 2 key comparisons.

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

| Case | Time | vs Baseline |
|---|---|---|
| 100 non-covering | 484 µs | +135% |
| baseline | 206 µs | — |

4000 entries × 100 tombstone checks = 400k comparisons per scan.

### prefix_scan_tombstones — prefix "pre0025" (50 entries)

| Case | Time | vs Baseline |
|---|---|---|
| non_overlapping | 4.01 µs | +29% |
| overlapping | 3.12 µs | +1% |
| 100 non-covering | 42.4 µs | +1,270% |
| baseline | 3.09 µs | — |

50 entries × 100 tombstone checks = 5k comparisons per prefix lookup.

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

| Gate | Target | Actual | Status |
|---|---|---|---|
| get ≤10% regression at 100 non-covering | ≤353 ns | 3.29 µs | ❌ |
| scan ≤15% regression at 100 non-covering | ≤237 µs | 484 µs | ❌ |
| prefix_scan ≤15% regression at 100 non-covering | ≤3.55 µs | 42.4 µs | ❌ |

## Root Cause

**O(R) linear scan in the active memtable** for range tombstones.

- `get` path: `find_sst_with_range_ts()` scans all R tombstones in the active memtable linearly.
- `scan` path: merge iterator checks every candidate entry against all R tombstones.
- `prefix_scan` path: same as scan but per-prefix.

At 100 tombstones, this adds ~3 µs overhead per get, ~280 µs overhead per scan,
and ~39 µs overhead per prefix scan.

## Optimization Direction (RFC §12.2 deferred)

Replace the `Vec<RangeTombstone>` linear scan with an interval tree or sorted structure,
reducing per-entry check from O(R) to O(log R + K) where K is the number of covering
tombstones for a given key.

Expected improvement (conservative, includes constant-factor overhead from tree lookup):
- get at 100 tombstones: 3.29 µs → ~400 ns (slightly above 321 ns baseline)
- scan at 100 tombstones: 484 µs → ~250 µs (slightly above 206 µs baseline)
- prefix_scan at 100 tombstones: 42.4 µs → ~5 µs (above 3.09 µs baseline)

These benchmarks establish the baseline for that optimization.
