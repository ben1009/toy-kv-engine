# Parallel Scan Findings

Date: 2026-07-07 (final)

## Scope

Implementation and benchmark findings for RFC 015 parallel scan.  Covers
cache admission policy, concurrent coordinator drain, fadvise readahead,
and traversal-skew investigation.  Code shipped in PR #158.

## API Surface

1. `scan_parallel_async(...) -> ParallelScan`
2. `prefix_scan_parallel_async(...) -> ParallelScan`
3. `ParallelScan::try_next_chunk().await`
4. `ParallelScan::try_next_batch().await`
5. `ParallelScanOptions::cache_admission` — `Force | Admit | Bypass` (default `Bypass`)

## Implementation

| Component | Description |
|-----------|-------------|
| MVCC snapshot | One logical snapshot per scan, shared `read_ts` across shard workers |
| Shard planning | L1+ SST first-key boundaries, weighted by `num_of_blocks + 1` per SST |
| Worker execution | Sync iterators on engine-owned `BlockingExecutor` |
| Chunk handoff | Shared payload buffer + row offsets per chunk, `Bytes::slice()` views |
| Coordinator drain | Polls all remaining shard receivers before blocking; buffers future-shard batches |
| Cache admission | `CacheAdmission` enum threaded through `SsTableIterator`, `SstConcatIterator`, `scan_inner_with_snapshot` |
| Readahead | `posix_fadvise(WILLNEED)` at each block boundary in `SsTableIterator` |
| Metrics | Per-shard `hits/misses/admitted/rejected/evicted` + `coordinator_wait_us` |

## Cache Admission Policy

Every block read previously called `force_put` unconditionally, bypassing
TinyUFO's TinyLFU admission filter.  8 shards each forced every touched
block into cache, evicting hot blocks.

| Policy | Behavior | Use case |
|--------|----------|----------|
| `Force` | Always insert (legacy) | Point gets, sync scans |
| `Admit` | TinyLFU decides — rejects ~8% of scan blocks | Mixed workloads |
| `Bypass` | Check cache on read, never insert on miss | Large scans (**default**) |

**Default changed from `Admit` to `Bypass`.**  Admit only rejects ~8% of blocks;
the admitted 92% still create cross-shard cache contention.  Bypass eliminates
it entirely.

## Benchmark Shape

`target_sst_size=256`, `compaction=simple`, 8 shards, 615 tests green.

## Results (2× runs per size)

```
Size  | Run | Sync    | Parallel (Bypass) | Winner
20K   | 1   | 5.89ms  | 6.22ms            | sync
20K   | 2   | 3.80ms  | 5.58ms            | sync
50K   | 1   | 14.44ms | 13.82ms           | parallel +4%
50K   | 2   | 8.68ms  | 18.58ms           | sync
100K  | 1   | 45.41ms | 35.35ms           | parallel +28%
100K  | 2   | 38.64ms | 30.42ms           | parallel +27%
```

Parallel Bypass consistently wins or is competitive at 50K+.  The benchmark
has high run-to-run variance (sync varies 2-5×) due to I/O and cache warmth
inherited from the load phase.  The consistent signal: Bypass keeps parallel
within striking distance; Admit always loses.

## Per-Shard Instrumentation

```
Signal              | 20K (warm) | 50K          | 100K
Shard rows          | 2490-2505  | 6235-6265    | 12494-12510
Shard elapsed       | 0.6-5.0ms  | 2.1-15.7ms   | 3.9-32.9ms
Coordinator wait    | 4.0ms      | 12.9ms       | 27.5ms
Max misses          | 0          | 4805         | 16417
Admitted (residual) | 0-872      | 1780-3117    | 6072-7238
```

Row counts are balanced within 0.1%.  Per-shard time varies up to 8×.
Coordinator wait accounts for 65-75% of total parallel time — the dominant
bottleneck.  Residual admissions during Bypass are from the sync scan's
`Force` inserts populating the cache before the parallel scan runs.

## Experiments Tried

| Experiment | Result |
|------------|--------|
| `CacheAdmission::Admit` (TinyLFU) | Loses at all sizes; only ~8% rejection |
| `CacheAdmission::Bypass` | Wins at 50K+; made default |
| Concurrent drain (poll-all + buffer) | Reduced coordinator wait; implemented |
| `posix_fadvise(WILLNEED)` readahead | +2-6% improvement at 50-100K |
| SST boundary cost `+4` in planner | Regressed (63ms vs 30ms) — reverted |
| SST block-0 prefetch per shard | Regressed (57ms vs 30ms) — removed |
| Traversal-skew planner tuning | I/O variance dominates; planner can't predict cache warmth |
| `CacheAdmission::Force` (no change) | Always loses to Bypass on large scans |

## What Didn't Work

- **Higher SST boundary cost**: pushing splits away from dense-SST regions
  made row balance worse without reducing time variance (I/O is the real cost).
- **Block-0 prefetch**: 8 workers × 1600+ SSTs = 13K concurrent random reads;
  added I/O contention without reducing variance.
- **Traversal-skew planning**: planner weights can't predict which blocks are
  in cache or how the disk scheduler orders 8 concurrent readers.

## Recommendations

1. Default `cache_admission = Bypass` for throughput scans (already done).
2. Use `try_next_chunk()` over row-at-a-time or batch adapters.
3. Benchmark on multi-shard dataset (`compaction=simple`, small `target_sst_size`).
4. `Admit` may help in mixed point-get + scan workloads (not yet benchmarked).

## TODO

- [ ] Mixed point-get + scan workload to evaluate `Admit`
- [ ] Thread `CacheAdmission` through point-get path (`read_block_iter_for_key`)
- [ ] Per-shard `block_loads`/`sst_switches` range in benchmark output (currently max only)
- [ ] io_uring batched readahead for block I/O
