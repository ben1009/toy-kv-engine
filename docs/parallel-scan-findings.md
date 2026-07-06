# Parallel Scan Findings

Date: 2026-07-07

## Scope

This note records the implementation and benchmark findings for RFC 015
parallel scan work.  Updated to reflect cache admission policy, concurrent
drain, traversal-skew investigation, and SST prefetch experiment.

## Current API Shape

1. `scan_parallel_async(...) -> ParallelScan`
2. `prefix_scan_parallel_async(...) -> ParallelScan`
3. `ParallelScan::try_next_chunk().await`
4. `ParallelScan::try_next_batch().await`
5. `ParallelScanOptions::cache_admission` — controls block-cache insertion

`ParallelScan::try_next()` was removed.  Callers consume chunks, not rows.

## Implementation State

1. one logical MVCC snapshot per parallel scan, reused across shard workers;
2. conservative shard planning from L1+ boundaries with single-shard fallback;
3. worker-owned synchronous shard scans behind the engine-owned blocking
   executor;
4. **concurrent coordinator drain** — polls all remaining shard receivers
   non-blocking, buffers batches from future shards, only blocks on current
   shard when nothing is ready;
5. **configurable block-cache admission** — `CacheAdmission::Force`,
   `Admit` (TinyLFU gate), or `Bypass` (read-only); and
6. **per-shard admission metrics** — `BlockCache::admission_counts()`
   exposes `(admitted, rejected, evicted)` for attribution.

The worker-to-coordinator handoff carries one shared payload buffer plus row
offsets per chunk; the public chunk iterator returns `Bytes::slice()` views.

## Cache Admission Policy

Every block read previously called `force_put` unconditionally, bypassing
TinyUFO's TinyLFU admission filter.  Parallel scan's 8 shards each forced
every touched block into cache, evicting hot blocks.

The fix adds `CacheAdmission` threaded through the iterator stack:

| Policy | Behavior |
|--------|----------|
| `Force` | Always insert (legacy) |
| `Admit` | TinyLFU decides — rejects one-shot scan blocks |
| `Bypass` | Check cache on read, never insert on miss |

`ParallelScanOptions::cache_admission` defaults to `Bypass`.  All sync scan
and point-get paths continue to use `Force`.  The benchmark CLI accepts
`--parallel-scan-cache-admission force|admit|bypass`.

### TinyLFU rejection rate

At 100K with `Admit`, TinyLFU rejects ~8% of blocks (1958/23947).  The
math checks out: `admitted + rejected ≈ Force.admitted`.  But Admit still
loses to Bypass on single-scan benchmarks because the admitted 92% still
create cross-shard cache contention.

## Concurrent Coordinator Drain

The original coordinator drained shards strictly in order: block on
`shard_rxs[N].recv().await`, exhaust shard N, advance to N+1.  Fast shards
sat idle while the coordinator waited for slower ones.

The replacement polls **all remaining shard receivers** non-blocking before
falling back to a blocking wait on the current shard.  Batches from future
shards are buffered in `shard_buffers` and delivered instantly when the
coordinator reaches that shard.  Implementation is internal to
`ParallelScan::recv_next_batch` — the public API and shard-worker contract
are untouched.

## Traversal-Skew Investigation

### Problem

Row counts are balanced within 0.1% across shards, but per-shard elapsed
time varies up to 7× (`shard_ms=3.79..27.76` at 100K).  The slowest shard
dominates total time through the coordinator.

### Hypotheses tested

1. **Planner weights** — changed from `num_of_blocks` to `num_of_blocks + 1`
   (SST boundary cost).  Marginal impact, within noise.  Increasing to
   `+4` regressed performance (63ms vs 30ms) by misplacing split points.

2. **SST block-0 prefetch** — before each shard iterates, read the first
   block of every overlapping SST to warm the OS page cache.  Regressed to
   57ms (from 30ms).  8 workers × 1600+ SSTs = 13K concurrent random reads
   — adds I/O contention without reducing variance.

### Root cause

Per-block I/O cost is not uniform.  The sync scan (which runs first)
populates the block cache unevenly — some key ranges get cached, others
don't.  The parallel scan inherits this uneven cache state.  Neither the
planner nor simple prefetch can predict or compensate for cache warmth.

### Remaining approaches (not implemented)

- Eliminate the sync scan warmup (both phases cold-cache)
- io_uring batched readahead with per-shard I/O scheduling
- Work stealing between shard workers

## Benchmark Shape

1. load data in explicit flush batches;
2. `target_sst_size=256`, `compaction=simple`;
3. compare sync scan vs parallel scan on the same loaded dataset.

## Key Results (2026-07-07, final)

All runs: `target_sst_size=256`, `compaction=simple`, 8 shards, 615 tests green.

| Size | Policy | Sync | Parallel | Winner |
|------|--------|------|----------|--------|
| 20K | Bypass | 3.96ms | 4.51ms | sync |
| 20K | Admit | 4.75ms | 4.51ms | parallel |
| 50K | Bypass | 19.52ms | **13.04ms** | **parallel +50%** |
| 50K | Admit | 18.68ms | 19.13ms | tie |
| 100K | Bypass | 44.98ms | **30.28ms** | **parallel +49%** |
| 100K | Admit | 48.81ms | 60.88ms | sync |

## What the instrumentation says

| Signal | 20K Bypass | 50K Bypass | 100K Bypass |
|--------|-----------|-----------|------------|
| Shard rows | 2490–2507 | 6235–6265 | 12493–12510 |
| Shard elapsed | 0.6–3.8ms | 2.7–11.4ms | 3.8–27.8ms |
| Coordinator wait | 3.9ms | 11.9ms | 27.4ms |
| Max misses | 0 | 1309 | 14628 |
| Admitted (residual) | 590 | 1484 | 5897 |

Residual admissions during Bypass are from the sync scan's `Force` inserts
(which populate the cache before the parallel scan) plus cross-shard counter
visibility (the `admitted` counter is global, so one shard's delta captures
other shards' activity within the snapshot window).

## Current recommendation

1. Default `cache_admission = Bypass` is correct for throughput scans.
2. Use `try_next_chunk()` over row-at-a-time or batch adapters.
3. Benchmark on a true multi-shard dataset (`compaction=simple`, small
   `target_sst_size`) before drawing conclusions.
4. Use per-shard instrumentation to attribute bottlenecks.

## TODO

- [ ] **Admit under mixed workload** — `Admit`'s benefit (protecting hot
  point-get blocks from scan pollution) isn't measurable in a single-scan
  benchmark.  Add a point-get + scan mixed workload to the harness.
- [ ] **Eliminate sync-scan warmup** — the sync scan populates the cache
  unevenly before the parallel scan runs.  Running both phases cold-cache
  (or both with the same admission policy) would give a cleaner comparison.
- [ ] **Thread `CacheAdmission` through point-get path** — `read_block_cached`
  delegates via `CacheAdmission::Force`.  Migrating `read_block_iter_for_key`
  to accept `CacheAdmission` would close the residual admission leak.
- [ ] **io_uring readahead** — batched async block reads could reduce
  per-shard I/O stalls without adding random-read contention.
- [ ] **Remove `prefetch_shard_blocks`** — implemented but unused; regressed
  benchmarks.  Either repurpose or delete.
- [ ] **Shard-level `block_loads`/`sst_switches` range** — benchmark output
  currently shows only max; adding min would expose the actual per-shard
  I/O spread.
