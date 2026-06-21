# RFC 011: db_bench-Style Benchmark Harness

**Status:** Draft  
**Date:** 2026-06-21  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes turning `kv-engine/src/bin/write-perf.rs` into a
db_bench-style benchmark harness for kv-engine. The current binary already
contains many RocksDB-inspired workloads:

```text
fillseq
fillrandom
readrandom
readwhilewriting
readrandomwriterandom
seekrandom
overwrite
readseq
readseq_validate_order
readmissing
seekrandomwhilewriting
deleterandom
compact
```

The missing piece is not workload coverage alone. The current runner is a fixed
script with hard-coded paths, sizes, engine options, and text output. That makes
it useful for manual profiling, but weaker as a repeatable benchmark contract
for RFCs, pull requests, and external comparisons.

The proposed harness keeps the existing default suite, but adds:

1. Configurable workload selection.
2. Configurable data sizes, value sizes, durations, thread counts, cache size,
   WAL, vLog, and compaction options.
3. Machine-readable output.
4. Optional latency sampling.
5. Explicit load, reopen, measure, and cleanup phases so read workloads cannot
   accidentally benchmark an empty or incompatible database.
6. A stable report schema for comparing kv-engine changes over time and, later,
   comparing against RocksDB or Fjall adapters.

The goal is a practical local tool:

```bash
cargo run --release --bin write-perf -- \
  --bench fillrandom,readrandom,readwhilewriting \
  --num 1000000 \
  --reads 250000 \
  --value-size 1024 \
  --duration 10 \
  --threads 4 \
  --compaction leveled \
  --output json
```

---

## 2. Motivation

kv-engine increasingly uses benchmark evidence to justify storage-engine
changes. Recent RFCs and PRs rely on workloads such as cache-backfill
read-after-write, prefix scan pruning, vLog read/write amplification, range
tombstone overhead, and RocksDB-style point-read/write patterns.

Today, there are two benchmark styles in the repo:

1. Criterion micro/macro benchmarks under `kv-engine/benches/`.
2. The `write-perf` binary, which runs a fixed sequence of operational
   workloads and prints human-readable throughput numbers.

Both are useful, but the current `write-perf` runner has practical limits:

1. Workload parameters are compiled into the binary.
2. Every run executes the whole suite unless the source is edited.
3. Output is hard to parse in CI, scripts, or comparison dashboards.
4. Path lifecycle is implicit: many workloads delete their directory before and
   after the run.
5. Warm-cache, reopen, and cache-pressure measurements are easy to
   accidentally conflate.
6. Engine options such as WAL, vLog, compaction strategy, cache capacity, and
   prefix bloom settings are not consistently exposed.
7. Comparisons to RocksDB's `db_bench` are currently documented manually rather
   than produced by a repeatable harness.

That is enough friction that benchmark numbers can become stale or difficult to
reproduce. A db_bench-style CLI gives each performance claim a concrete command
line and structured output.

---

## 3. Goals

1. Preserve the existing `write-perf` default suite for continuity.
2. Add workload selection via `--bench`.
3. Add configurable benchmark dimensions: key count, read count, value size,
   duration, reader/writer threads, seek-next count, random seed, and database
   path.
4. Add configurable engine options: WAL, vLog, compaction strategy, cache
   capacity, cache backfill, prefix bloom, target SST size, memtable limit, and
   manifest snapshot threshold.
5. Emit both human-readable text and JSON Lines, with diagnostics on stderr so
   stdout remains parseable.
6. Include enough per-workload metadata to reproduce a run.
7. Track both throughput and optional sampled latency.
8. Track engine counters that explain results, separating currently available
   counters from follow-up engine instrumentation.
9. Make database lifecycle explicit: fresh, reuse, reopen, and cleanup modes.
10. Add tests for CLI parsing, workload selection, output schema stability, and
    lifecycle behavior.

## 4. Non-Goals

1. Replacing Criterion benchmarks. Criterion remains better for statistically
   controlled microbenchmarks and focused regression tracking.
2. Guaranteeing stable performance numbers across machines.
3. Building a dashboard or long-term metrics service in the MVP.
4. Adding RocksDB or Fjall dependencies to the kv-engine crate in the MVP.
5. Dropping OS page cache from the benchmark runner by default. That requires
   elevated privileges and is environment-specific.
6. Enforcing performance gates in normal CI in the MVP.
7. Rewriting engine internals to fit the benchmark harness.

---

## 5. Existing Baseline

The current `write-perf` binary already has the right workload seed. It runs:

```text
1.  scan
2.  concurrent R/W without WAL
3.  concurrent R/W with WAL
4.  WAL throughput
5.  vLog GC pressure
6.  vLog concurrent R/W + GC
7.  fillseq
8.  fillrandom
9.  readrandom
10. readwhilewriting
11. readrandomwriterandom
12. seekrandom
13. overwrite
14. readseq
15. readseq_validate_order (current `readreverse` placeholder)
16. readmissing
17. seekrandomwhilewriting
18. deleterandom
19. compact
```

This is 19 printed sections today, but more than 19 reported measurements
because some sections emit multiple rows. For example, WAL throughput reports
separate 256B, 4KB, and concurrent cases, while `scan` reports full-scan and
10% scan measurements.

The binary has a small `--large` mode:

```text
default: num=200_000, reads=100_000, duration=5s, scan=100_000,
         seekrandom=10_000 seeks x 10 nexts,
         seekrandomwhilewriting=1_000 seeks x 10 nexts,
         wal_seq=(50_000 x 256B, 20_000 x 4KB),
         wal_concurrent=(50_000 x 256B x 4 threads),
         vlog_gc=(3 rounds x 5_000 entries)
large:   num=2_000_000, reads=500_000, duration=10s, scan=1_000_000,
         with the same per-workload constants unless explicitly overridden
```

The proposed harness should keep those presets:

```bash
cargo run --release --bin write-perf
cargo run --release --bin write-perf -- --preset large
```

The first implementation can treat `--large` as an alias for `--preset large`
to avoid breaking existing local usage.

---

## 6. Design Overview

### 6.1 CLI Shape

Use `clap`, which is already a normal crate dependency, for typed argument
parsing. Use `serde` and `serde_json`, which are also already dependencies, for
machine-readable output.

Recommended top-level options:

```text
--bench <list>              Comma-separated workloads, default: all
--preset <default|large>    Default workload sizes
--path <path>               Database path or base path
--fresh                     Delete path before each workload
--reuse                     Reuse existing path
--cleanup                   Delete path after workload
--no-cleanup                Preserve path after workload
--reopen                    Close and reopen before measured read phase
--cache-pressure <bytes>    Best-effort cache pressure before measurement
--output <text|json|both>   Output format
--seed <u64>                Base RNG seed
--num <usize>               Number of inserted keys
--reads <usize>             Number of read operations
--value-size <usize>        Value size in bytes
--duration <secs>           Duration for concurrent workloads
--threads <usize>           Worker thread count for mixed workloads
--readers <usize>           Reader thread count for readwhilewriting
--writers <usize>           Writer thread count where supported
--seek-nexts <usize>        Number of next() calls after each seek
--sample-latency <n>        Target latency sample interval; 0 disables
```

Engine options:

```text
--wal <on|off>
--serializable <on|off>
--compaction <none|simple|leveled|tiered>
--target-sst-size <bytes>
--memtable-limit <usize>
--cache-capacity <entries>
--cache-backfill <on|off>
--vlog <on|off>
--vlog-threshold <bytes>
--vlog-cache <bytes>
--prefix-bloom <off|len-list>
--manifest-snapshot-threshold <bytes>
```

The CLI should reject incompatible options early. For example:

```text
--bench readwhilewriting --duration 0
--vlog on --vlog-threshold 0
--prefix-bloom ""
--compaction unknown
```

### 6.2 Workload Registry

Replace the fixed `main()` sequence with a registry:

```rust
struct BenchSpec {
    name: &'static str,
    aliases: &'static [&'static str],
    run: fn(&BenchContext) -> anyhow::Result<Vec<BenchMeasurement>>,
}
```

The registry provides:

1. Stable names for reports.
2. Alias compatibility with RocksDB names where useful.
3. A single place to list supported workloads.
4. A single place to validate workload-specific options.

Example aliases:

```text
fillseq                 -> fillseq
fillrandom              -> fillrandom
readrandom              -> readrandom
readwhilewriting        -> readwhilewriting,rww
readrandomwriterandom   -> readrandomwriterandom,rwrw
seekrandom              -> seekrandom
prefixscanrandom        -> prefixscanrandom,prefixscan
```

### 6.3 Load and Measure Phases

Every workload must declare how its data is prepared before measurement:

```rust
enum LoadMode {
    SelfContained,
    ReuseExisting,
}

struct WorkloadPlan {
    load_mode: LoadMode,
    measured_phase: &'static str,
}
```

The default mode is `SelfContained`: the workload creates or refreshes the
dataset required for its measurement, then measures only the intended
operation. This preserves the current behavior of read workloads such as
`readrandom`, which populate data before timing reads.

`--reuse` switches compatible read workloads to `ReuseExisting`. In that mode
the harness must validate a small metadata file in the database path before
measuring:

```json
{
  "schema": "kv-engine.write-perf.dataset.v1",
  "workload_family": "ordered_keyspace",
  "num": 200000,
  "value_size": 1024,
  "seed": 42,
  "engine_options_hash": "..."
}
```

If the metadata is missing or incompatible with the requested benchmark
parameters, the harness must fail instead of silently reporting throughput over
an empty, stale, or differently configured database.

Workloads with separate reopen behavior must report timings separately:

```text
load_elapsed_ms
close_elapsed_ms
reopen_elapsed_ms
warmup_elapsed_ms
measure_elapsed_ms
```

`ops_per_sec` is always computed from `measure_elapsed_ms`. End-to-end
workloads may additionally report `end_to_end_elapsed_ms`, but they must not be
compared directly with steady-state read throughput.

### 6.4 Benchmark Context

All workloads should receive the same normalized context:

```rust
struct BenchContext {
    config: BenchConfig,
    engine_options: LsmStorageOptions,
    path: PathBuf,
    rng_seed: u64,
}
```

Workloads should not call `remove_dir_all` directly. They should request a
database from a lifecycle helper:

```rust
let engine = ctx.open_engine("fillrandom")?;
```

The helper owns:

1. Per-workload path construction.
2. Fresh/reuse/reopen cleanup behavior.
3. Engine option construction.
4. Consistent close/drop ordering.

### 6.5 Output Schema

JSON Lines output should emit one object per measurement. A workload that
produces multiple measurements, such as full scan plus 10% scan or multiple WAL
cases, emits multiple JSON lines with the same `run_id` and `workload` but
different `measurement` names:

```json
{
  "schema": "kv-engine.write-perf.v1",
  "unix_epoch_ms": 1782045296000,
  "run_id": "20260621T123456Z-0001",
  "workload": "readrandom",
  "measurement": "point_get",
  "preset": "default",
  "engine": "kv-engine",
  "engine_options": {
    "wal": false,
    "compaction": "leveled",
    "value_separation": false,
    "cache_capacity": 8192
  },
  "params": {
    "num": 200000,
    "reads": 100000,
    "value_size": 1024,
    "threads": 1,
    "seed": 123
  },
  "result": {
    "load_elapsed_ms": 900.12,
    "reopen_elapsed_ms": null,
    "measure_elapsed_ms": 147.06,
    "ops": 100000,
    "ops_per_sec": 680000.0,
    "found": 100000
  },
  "latency": {
    "sampled": false
  },
  "counters": {
    "block_cache_entry_count": 8192,
    "vlog_cache_hits": 0,
    "vlog_cache_misses": 0,
    "range_tombstones": 0
  }
}
```

`schema` must be stable. Additive fields are allowed. Renaming or changing the
meaning of existing fields requires a new schema name.

All diagnostics, lifecycle warnings, cleanup errors, and progress logs must go
to stderr. JSON Lines output must keep stdout machine-readable.

### 6.6 Text Output

Text output should remain compact and readable:

```text
readrandom/point_get num=200000 reads=100000 value=1024B measure=147.06ms ops=100000 ops/s=680000 found=100000
```

This format is intentionally less decorative than the current sectioned output
so it is easy to scan and grep.

### 6.7 Latency Sampling

Throughput is the default metric. Latency sampling is optional because timing
every operation can distort high-throughput workloads.

When `--sample-latency N` is set:

1. Use per-thread randomized sampling with an expected interval of N
   operations. Do not deterministically time every Nth operation, because that
   can alias with key generation, flush cadence, or mixed workload patterns.
2. Store sampled latencies in memory or a bounded reservoir for that workload.
3. Report p50, p95, p99, and max.
4. Include the sample interval and sample count in output.
5. Report samples separately for operation types such as read and write in
   mixed workloads.

Example:

```json
"latency": {
  "sampled": true,
  "target_interval": 1000,
  "samples": 100,
  "operation": "read",
  "p50_us": 12.4,
  "p95_us": 37.8,
  "p99_us": 61.2,
  "max_us": 81.0
}
```

Latency sampling should start disabled.

---

## 7. Workloads

### 7.1 Existing Workloads to Preserve

The MVP should preserve these workloads:

| Workload | Purpose |
|----------|---------|
| `scan` | Full forward scan throughput |
| `fillseq` | Sequential write throughput |
| `fillrandom` | Random write throughput |
| `readrandom` | Random point-read throughput |
| `readwhilewriting` | One writer plus N readers |
| `readrandomwriterandom` | Balanced random reads and writes |
| `seekrandom` | Random seek plus bounded `next()` calls |
| `overwrite` | Random updates to existing keys |
| `readseq` | Sequential scan after load |
| `readseq_validate_order` | Current forward-scan order validation formerly printed as `readreverse` |
| `readmissing` | Negative point-read throughput |
| `seekrandomwhilewriting` | Seek workload under write pressure |
| `deleterandom` | Random point-delete throughput |
| `compact` | Manual full-compaction cost |
| `wal_throughput` | WAL write throughput |
| `wal_concurrent` | WAL under concurrent writers |
| `vlog_gc` | vLog GC pressure |
| `vlog_concurrent_gc` | vLog GC under concurrent activity |

The current binary prints a `readreverse` section, but the engine does not yet
support reverse iteration and the workload measures a forward scan placeholder.
The refactor should rename that measurement to `readseq_validate_order` and
mark `readreverse` as unsupported until the iterator stack has real reverse
support. The harness must not emit db_bench-compatible `readreverse` results
from a forward scan.

### 7.2 New Workloads

Add the following workloads after the CLI refactor, not necessarily in the same
patch:

#### prefixscanrandom

Populate structured keys:

```text
tenant:{tenant_id}:user:{user_id}
```

Then issue random prefix scans:

```text
tenant:{tenant_id}:
```

This workload measures prefix-scan iterator setup, prefix bloom pruning, and
scan throughput over tenant-shaped keyspaces.

Config:

```text
--tenants <usize>
--prefix-scan-limit <usize>
--prefix-hit-rate <0.0..1.0>
```

#### rangeseekrandom

Issue random bounded range scans:

```text
[keyNNNNNNNN, keyNNNNNNNN + range_width)
```

This isolates range-bound handling, seek cost, and iterator merge overhead.

Config:

```text
--range-width <usize>
--ranges <usize>
```

#### readhot

Use a skewed distribution where most reads hit a small hot set. This exercises
block cache policy, value cache policy, and memtable negative lookup behavior.

Config:

```text
--hot-keys <usize>
--hot-ratio <0.0..1.0>
```

#### readwhilecompacting

Populate enough data to trigger compaction, then measure reads while compaction
is known to be active. The current public API has `force_full_compaction()`,
which is blocking, and no public "compaction in progress" signal. This workload
therefore requires either a test-only/benchmark-only observation hook or a
narrower name such as `read_after_compaction_trigger` that reports it is
best-effort and does not prove overlap.

Config:

```text
--compaction-inputs <usize>
--duration <secs>
--readers <usize>
```

#### reopen_readrandom

Populate, close, reopen, then measure random reads. This separates recovery,
manifest load, vLog index load, engine-cache reset, and metadata loading costs
from steady-state point-read throughput.

Config:

```text
--reopen
--reads <usize>
```

#### blob_fillrandom and blob_readrandom

Large-value workloads that force value separation:

```text
--vlog on --value-size 4096 --vlog-threshold 1024
```

These should report LSM bytes, vLog bytes, vLog GC stats, and value cache
hit/miss counts.

---

## 8. Database Lifecycle

Benchmark lifecycle must be explicit because it changes results.

### 8.1 Fresh

`--fresh` deletes the workload path before opening the engine. This should be
the default for write workloads.

### 8.2 Cleanup

`--cleanup` deletes the workload path after the run. This should remain default
for the current full-suite behavior, but reusable benchmark paths should be
possible.

### 8.3 Reuse

`--reuse` opens an existing path and does not delete it first. This is useful
for read-only measurement after a separate load step.

### 8.4 Reopen

`--reopen` closes and reopens the engine between load and measured phase. It
should be supported by read workloads and rejected for workloads that have no
separate load phase.

### 8.5 Cache Pressure

The harness should support a `--cache-pressure <bytes>` option only as best
effort:

1. Close and reopen the engine.
2. Optionally read an unrelated file to create cache pressure if configured.
3. Do not require privileged OS page-cache dropping.

The output must report:

```text
engine_reopened=true|false
engine_cache_cleared=true|false
os_page_cache_dropped=true|false
pressure_file_bytes=<bytes|null>
```

The MVP should not attempt privileged cache dropping. Results must not be
described as "cold cache" unless true OS page-cache dropping happened.

---

## 9. Engine Counters

Each workload should capture counters before and after the measured phase.

MVP counters using currently exposed APIs:

1. Block cache entry count. This is an approximate upper bound because the
   current TinyUFO wrapper cannot observe every eviction.
2. vLog value-cache hits and misses.
3. vLog file count and bytes from `vlog_stats()`.
4. Range tombstone stats.
5. Compaction filter stats.

Follow-up counters:

1. Block cache hits and misses.
2. Flush count.
3. Compaction count.
4. Compaction elapsed time.
5. Bytes written to SSTs.
6. Bytes written to WAL.
7. Bytes written to vLog.
8. SST count by level.
9. L0 file count.
10. Manifest size.
11. Whether flush or compaction was active during the measured phase.

If a counter is unavailable, the JSON field should be omitted or set to `null`
rather than guessed.

Counters should be captured in phases:

```text
setup_counters
measured_delta_counters
post_quiesce_counters
```

The measured delta is the primary value. Setup counters and post-quiesce
counters help detect background flush/compaction or cache warmup effects that
would otherwise pollute interpretation.

---

## 10. External Comparisons

The MVP should benchmark only kv-engine. However, the output schema should leave
room for external adapters:

```text
--engine kv-engine
--engine rocksdb
--engine fjall
```

Adapters should be separate optional binaries or feature-gated crates. The
main kv-engine crate should not depend on RocksDB or Fjall just to run local
benchmarks.

Adapter requirements:

1. Use the same key generator.
2. Use the same value generator.
3. Use the same workload definitions where semantics match.
4. Record adapter-specific options in `engine_options`.
5. Declare a comparison profile covering durability, fsync policy, WAL/journal
   mode, compression, block/cache sizing units, bloom/filter settings,
   background compaction threads, value-separation/blob settings, write-buffer
   limits, and data lifecycle.
6. Mark profiles as `equivalent`, `best_effort`, or `non_equivalent`.
7. Clearly mark unsupported workloads.

RocksDB is the reference for db_bench naming and broad workload shape. Fjall is
useful for comparing embedded Rust API ergonomics and keyspace-oriented
workloads, but it does not need to define the harness shape.

---

## 11. Implementation Plan

### Phase 1: CLI and Registry

1. Introduce `BenchConfig`, `BenchContext`, `BenchSpec`, and
   `BenchMeasurement`.
2. Move the existing fixed `main()` list into a workload registry.
3. Add `--bench`, `--preset`, and `--large` compatibility.
4. Add parser tests for valid and invalid option combinations.
5. Keep text output only in this phase.
6. Split multi-row sections into named measurements or return
   `Vec<BenchMeasurement>`.

Validation:

```bash
cargo run --release --bin write-perf -- --bench fillseq --num 10000
cargo run --release --bin write-perf -- --bench fillseq,readrandom --preset default
```

### Phase 2: Lifecycle and Engine Options

1. Add path lifecycle helpers.
2. Add `--path`, `--fresh`, `--reuse`, `--cleanup`, and `--reopen`.
3. Add `--no-cleanup` rather than accepting ad hoc `--cleanup=false` syntax.
4. Refactor workloads into explicit load, optional reopen, measure, and cleanup
   phases.
5. Add dataset metadata validation for `--reuse`.
6. Add engine option flags for WAL, compaction, cache, vLog, and prefix bloom.
7. Normalize all existing workloads to use the lifecycle helper.

Validation:

```bash
cargo run --release --bin write-perf -- --bench fillrandom --path /tmp/kvbench --fresh --no-cleanup
cargo run --release --bin write-perf -- --bench readrandom --path /tmp/kvbench --reuse --reopen
```

### Phase 3: JSON Lines Output

1. Add `--output text|json|both`.
2. Define `kv-engine.write-perf.v1`.
3. Emit one JSON object per measurement.
4. Add snapshot tests for JSON field names and required metadata.
5. Keep JSON Lines on stdout and all diagnostics on stderr.

Validation:

```bash
cargo run --release --bin write-perf -- --bench readrandom --output json
```

### Phase 4: Counters and Latency Sampling

1. Capture available engine counters before and after each workload.
2. Add optional `--sample-latency`.
3. Report p50, p95, p99, max, and sample count when enabled.
4. Use randomized per-thread sampling or a bounded reservoir instead of
   deterministic every-Nth-operation sampling.
5. Ensure latency sampling can be disabled with zero overhead in the hot loop
   except a branch outside the measured operation path where practical.

Validation:

```bash
cargo run --release --bin write-perf -- --bench readrandom --sample-latency 1000 --output both
```

### Phase 5: New Workloads

Add focused workloads:

1. `prefixscanrandom`
2. `rangeseekrandom`
3. `readhot`
4. `readwhilecompacting`
5. `reopen_readrandom`
6. `blob_fillrandom`
7. `blob_readrandom`

Each workload should include at least one small smoke test for setup and output
shape. Performance assertions should not be part of normal unit tests.

### Phase 6: Optional External Adapters

After the kv-engine harness is stable, add optional comparison adapters outside
the core crate:

1. `bench-adapter-rocksdb`
2. `bench-adapter-fjall`

Adapters should consume the same workload config and emit the same JSON schema
with `engine` set appropriately.

---

## 12. Testing Strategy

### 12.1 Unit Tests

Add tests for:

1. `--bench` parsing.
2. Preset normalization.
3. Engine option normalization.
4. Invalid option combinations.
5. JSON schema required fields.
6. Workload registry lookup.
7. Path lifecycle behavior using temporary directories.
8. Dataset metadata compatibility checks for `--reuse`.
9. Multi-measurement output for workloads such as `scan` and WAL throughput.
10. stdout/stderr separation for JSON output.

### 12.2 Smoke Tests

Add tiny benchmark smoke tests that run with small inputs:

```text
--bench fillseq --num 100 --value-size 16
--bench readrandom --num 100 --reads 50 --value-size 16
--bench readwhilewriting --num 100 --duration 1 --readers 1
```

These tests should verify completion and output shape, not throughput.

### 12.3 Manual Performance Runs

Manual comparison commands should be documented:

```bash
cargo run --release --bin write-perf -- --preset default --output json > default.jsonl
cargo run --release --bin write-perf -- --preset large --output json > large.jsonl
```

For optimization PRs, authors should include the exact command and the relevant
JSON rows in the PR body or linked benchmark report.

---

## 13. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Benchmark harness complexity grows too much | Keep Criterion for focused benchmarks; keep `write-perf` as operational workloads only |
| Latency sampling changes throughput | Disable by default; use randomized sampling; report sampling configuration |
| JSON schema churn breaks scripts | Version schema as `kv-engine.write-perf.v1`; only add fields compatibly |
| Cache-pressure numbers are mislabeled as cold-cache results | Use `--cache-pressure`; report whether OS cache was actually dropped; do not claim cold-cache unless true |
| Benchmarks become CI-flaky | Use smoke tests in CI; keep real performance runs manual or scheduled |
| External adapters add dependency burden | Keep adapters optional and outside the core crate |
| Workload names drift from RocksDB terminology | Keep RocksDB-compatible aliases in the registry |
| Read workloads measure empty or incompatible reused data | Require self-contained loading by default and dataset metadata validation for `--reuse` |
| Background work pollutes counter deltas | Report setup, measured, and post-quiesce counter phases |

---

## 14. Open Questions

1. Should benchmark reports include hardware metadata such as CPU model, memory,
   filesystem, kernel, and rustc version?
2. Should the harness support loading once and running multiple read workloads
   against the same database path?
3. Should optional RocksDB/Fjall adapters live in this repository or in a
   separate benchmarking repository?
4. Should long-running performance gates be added as manual GitHub Actions
   workflows after the harness stabilizes?
5. Should follow-up engine instrumentation add exact block-cache hit/miss
   counters and compaction activity counters?
6. Should timestamp output remain Unix epoch milliseconds only, or should the
   repo add a time-formatting dependency for RFC3339 strings?

---

## 15. Acceptance Criteria

The RFC is implemented when:

1. Existing default `write-perf` behavior still runs the current suite.
2. `--bench` can run one workload or a comma-separated subset.
3. `--preset default` and `--preset large` work, and `--large` remains
   compatible.
4. Benchmark sizes and durations are configurable without editing source.
5. Engine options for WAL, compaction, cache, and vLog are configurable.
6. Path lifecycle is explicit and tested, including `--no-cleanup`.
7. Read workloads are self-contained by default, and `--reuse` validates dataset
   metadata before measuring.
8. JSON Lines output emits stable `kv-engine.write-perf.v1` records to stdout,
   one object per measurement.
9. Currently exposed cache, vLog, compaction filter, and range tombstone
   counters are included when available; missing counters are omitted or null.
10. `readreverse` is not emitted as a db_bench-compatible result until real
    reverse iteration exists.
11. Smoke tests cover parser, registry, lifecycle, reuse validation,
    multi-measurement output, and JSON output shape.
12. `docs/perf-profile.md` documents the new benchmark commands and replaces
    stale fixed-run instructions.
