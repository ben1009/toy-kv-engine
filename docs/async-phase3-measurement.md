# Async Phase 3 Measurement Plan

Status: current harness plan. `kv-engine/src/bin/async-phase3-perf.rs` now
exists and is the intended open/close measurement entry point.

## Goal

Measure whether the Phase 3 async-internal changes from RFC 014 improve
observable behavior enough to justify more internal async conversion.

This plan focuses on the async work that has already landed:

1. `open_async()` staged recovery plus parallel SST open.
2. Runtime-owned periodic flush and compaction tasks.
3. Runtime-owned post-compaction vLog GC.

The plan does not assume these changes are a throughput win. The point is to
measure:

1. cold-start latency;
2. request-path tail latency while maintenance work is active;
3. graceful shutdown latency;
4. regressions in the sync compatibility surface.

## Questions

1. Does `open_async()` reduce wall-clock open latency on databases with many
   SSTs?
2. Does moving flush/compaction/GC ownership onto the engine runtime change
   foreground write latency, especially p95/p99, under maintenance pressure?
3. Does `close_async()` remain bounded and predictable while background work is
   live?
4. Did the sync compatibility path (`open()` / `close()`) regress after it was
   moved onto the background runtime shell?

## Metrics

Record these for every run:

1. wall-clock duration;
2. p50 / p95 / p99 latency where the workload is per-operation;
3. bytes written or ops/sec when throughput matters;
4. SST count, live levels, WAL enabled/disabled, vLog enabled/disabled;
5. host info: CPU model, kernel, toolchain, build profile.

## Scenarios

### 1. Open Latency

Compare `open()` vs `open_async()` on the same prepared database directory.

Matrix:

1. small: tens of SSTs;
2. medium: hundreds of SSTs;
3. large: thousands of SSTs;
4. with and without vLog files present.

Primary metric:

1. wall-clock open latency.

Success signal:

1. `open_async()` is materially better on medium/large SST counts, or at
   least not worse on small databases.

### 2. Foreground Write Latency Under Background Maintenance

Drive sustained writes until periodic flush and compaction are active.

Matrix:

1. WAL off / WAL on;
2. leveled / tiered / simple leveled;
3. inline values / vLog values;
4. 1 writer / 4 writers.

Primary metrics:

1. write p95 / p99 latency;
2. total ops/sec;
3. background events observed during the run.

Success signal:

1. no clear regression in p95/p99 versus the pre-runtime-ownership baseline;
2. any improvement is a bonus, not an assumption.

### 3. Close Latency

Measure `close_async()` while the engine has:

1. no pending maintenance work;
2. active imm memtables to flush;
3. compaction work already triggered;
4. vLog GC work queued from post-compaction cleanup.

Also compare sync `close()`.

Primary metric:

1. wall-clock close latency.

Success signal:

1. graceful close remains bounded and deterministic;
2. sync `close()` does not regress badly relative to the old thread-owned
   shell.

### 4. Sync Compatibility Regression Check

Because `open()` and `close()` now route through the background runtime shell,
measure:

1. sync open latency;
2. sync close latency;
3. short write/read smoke throughput.

This is a guardrail, not a performance claim.

## Harness Plan

Use existing infrastructure first.

### Existing Surfaces

1. `kv-engine/src/bin/write-perf.rs` for write-heavy workloads and controlled
   option matrices.
2. `kv-engine/benches/wal_bench.rs` for WAL-sensitive durable write behavior.
3. `kv-engine/src/bin/async-phase3-perf.rs` for prepared-dataset
   open/close measurements with text or JSON output.

### Remaining Harness Additions

1. Add stronger machine-readable environment capture to every emitted record
   (kernel, CPU model, toolchain, profile).
2. Add a write-latency-under-maintenance mode that emits p95/p99 directly
   instead of inferring from external tooling.
3. Archive repeated runs in a stable output directory so before/after slices
   can be diffed without manual shell redirection.

## Proposed Commands

### Open / Close Latency

Prepare a directory with many SSTs, then compare:

```bash
cargo run --release --bin async-phase3-perf -- prepare \
  --path /tmp/async-phase3-openclose \
  --num-keys 2000000 \
  --value-size 1024 \
  --flush-every 50000 \
  --target-sst-size 262144 \
  --memtable-limit 2 \
  --output both
```

Then measure open/close on the prepared directory:

```bash
cargo run --release --bin async-phase3-perf -- measure-open-close \
  --path /tmp/async-phase3-openclose \
  --repetitions 5 \
  --mode both \
  --target-sst-size 262144 \
  --memtable-limit 2 \
  --output both
```

### Write Latency Under Maintenance

```bash
cargo run --release --bin write-perf -- \
  --bench fillrandom \
  --num 2000000 \
  --threads 4 \
  --compaction leveled \
  --output json
```

Repeat with:

1. `--wal`
2. `--vlog`
3. `--compaction simple`
4. `--compaction tiered`

### WAL-Specific Baseline

```bash
cargo bench --package kv-engine --bench wal_bench
```

## Comparison Rules

1. Run at least 5 repetitions per scenario.
2. Use medians for headline numbers.
3. Report p95/p99 for latency-sensitive paths.
4. Treat single-run deltas below noise level as non-findings.
5. Separate “no regression” from “real improvement”.

## Acceptance Bar

Phase 3 is good enough to stop here if:

1. `open_async()` is neutral-to-better on realistic datasets;
2. runtime-owned maintenance does not regress foreground tail latency;
3. `close_async()` remains bounded and reliable;
4. sync compatibility paths do not show a meaningful regression.

Only move deeper into internal async overlap if the measurements show a clear
problem or clear upside.

## Likely Follow-Ups

If measurements show a real opportunity:

1. overlap expensive substeps inside compaction or vLog GC;
2. overlap flush publish and non-critical cleanup where crash ordering allows;
3. add finer-grained instrumentation around maintenance task queueing and
   blocking-executor saturation.
4. optimize `open_async()` for vLog-heavy reopen cases if reopen-heavy data
   shows the current staged path is still a material bottleneck.
   show that the async path helps on large inline SST reopen, but loses on
   live-vLog reopen even with high file counts. Investigate a vLog-aware open
   strategy instead of only parallelizing SST open. Likely targets:
   parallel vLog directory/file discovery, reducing `spawn_blocking` overhead
   when SST open is cheap, and gating SST-open parallelism based on recovery
   shape rather than always paying the async orchestration cost.
5. longer-term: optimize the async recovery pipeline itself so it can win
   across recovery shapes instead of depending on an adaptive sync/async
   policy. Candidate directions: batch SST opens instead of one task per SST,
   reduce `spawn_blocking` transition count, fuse recovery phases where safe,
   and parallelize the vLog-side work that currently stays mostly sequential.
   This is a larger follow-up than the recovery-shape-aware policy and should
   be driven by profiling, not assumed.

If measurements are neutral:

1. stop at the current Phase 3 landing;
2. avoid Phase 4 iterator-internal async conversion.
