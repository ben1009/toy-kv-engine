# Parallel Scan Findings

Date: 2026-07-06

## Scope

This note records the implementation and benchmark findings for RFC 015
parallel scan work in the current checkout.

## Current API Shape

The parallel scan surface is now explicitly chunk-first:

1. `scan_parallel_async(...) -> ParallelScan`
2. `prefix_scan_parallel_async(...) -> ParallelScan`
3. `ParallelScan::try_next_chunk().await`
4. `ParallelScan::try_next_batch().await`

`ParallelScan::try_next()` was removed. The implementation and benchmarks now
assume chunked consumption is the intended fast path.

## Implementation State

The current implementation provides:

1. one logical MVCC snapshot per parallel scan, reused across shard workers;
2. conservative shard planning from L1+ boundaries with explicit single-shard
   fallback when memtable, immutable memtable, or L0 overlap dominates;
3. worker-owned synchronous shard scans executed behind the engine-owned
   blocking executor; and
4. ordered coordinator drain for chunk consumers.

The worker-to-coordinator handoff no longer materializes one owned `(Bytes,
Bytes)` pair per row. Instead, each chunk carries one shared payload buffer
plus row offsets, and the public chunk iterator returns `Bytes::slice()`
views.

## Benchmark Shape

The parallel scan benchmark uses a deliberate multi-shard fixture:

1. load data in explicit flush batches;
2. use small `target_sst_size`;
3. force compaction so the planner can see L1+ split points; and
4. compare sync scan against the parallel scan surface on the same loaded
   dataset.

This avoids the misleading single-shard fallback measurements that occur when
all data is still in memtables or overlapping L0 SSTs.

## Key Results

### Before chunk-first handoff

The early worker-backed path was correct but much slower than sync scan. The
main costs were:

1. per-row owned `Bytes` allocation/copying;
2. per-row or per-small-batch handoff overhead; and
3. coordinator wakeup cost.

### After chunk-first handoff

On the current benchmark shape (`scan_num=20000`, `value_size=256`,
`target_sst_size=256`, `compaction=simple`), the chunk-first path is now
competitive and often faster than sync scan.

Representative result:

```text
parallel_scan/sync_full_scan num=20000 value=256B measure=5.99ms entries=20000 entries/s=3339662
parallel_scan/parallel_chunk_full_scan num=20000 value=256B measure=3.95ms entries=20000 entries/s=5062080 shards=8 shard_rows=2490..2506 shard_ms=0.55..2.84 active_iters_max=4 coordinator_wait_ms=3.34 shard_cache_max_hits=2081 max_misses=0 shard_blocks_max=335 shard_sst_switches_max=167
```

Another stable compare after removing the row API:

```text
parallel_scan/sync_full_scan num=20000 value=256B measure=4.86ms entries=20000 entries/s=4116199
parallel_scan/parallel_chunk_full_scan num=20000 value=256B measure=4.17ms entries=20000 entries/s=4801083 shards=8 shard_rows=2490..2506 shard_ms=0.89..3.20 active_iters_max=4 coordinator_wait_ms=3.79 shard_cache_max_hits=2201 max_misses=86 shard_blocks_max=335 shard_sst_switches_max=167
```

These results are good enough to justify the chunk-first API direction, but
they are not proof that parallel scan is always faster on all workloads.

## What the instrumentation says

The current instrumentation gives four useful signals:

1. shard rows are well balanced (`2490..2506` in the example runs);
2. cache misses are usually zero or near-zero in the warmed benchmark path;
3. per-shard elapsed time still varies substantially even when row counts are
   balanced; and
4. coordinator wait remains a meaningful part of total parallel scan time.

This means:

1. row-count skew is not the main bottleneck on the benchmarked path;
2. cold-cache misses are not the main bottleneck on the benchmarked path; and
3. the remaining variance is likely a mix of traversal complexity
   (`block_loads`, `sst_switches`) and ordered coordinator wait.

## Tuning Notes

The best simple default change so far was increasing `batch_rows` from `128`
to `256`.

Other small coordinator-side tweaks that were tried did not obviously improve
the benchmark:

1. eager first-batch flushing; and
2. merging immediately ready batches before delivery.

The queue-based redesign experiment was also attempted and then reverted. It
introduced instability without a clear performance win.

## Current recommendation

For throughput-sensitive callers:

1. prefer `try_next_chunk()` over any row-at-a-time abstraction;
2. treat `try_next_batch()` as a convenience adapter, not the fastest path;
3. benchmark on a true multi-shard dataset before drawing conclusions; and
4. use the shard-level instrumentation to decide whether a workload is limited
   by coordinator wait or shard traversal work.

## Open direction

The next meaningful work, if continued, should likely focus on one of:

1. reducing ordered coordinator wait without reintroducing instability; or
2. improving split planning using traversal-cost signals rather than just
   boundary count or row count.
