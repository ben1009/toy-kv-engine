# RFC 018: Steady-State Benchmark Suite

**Status:** Draft
**Date:** 2026-08-13
**Author:** kv-engine Contributors
**References:**
- RFC 011: db_bench-Style Benchmark Harness
- `docs/bench-report-crud-bench-rocksdb.md`
- SlateDB benchmark suite at commit `15564e675fe960a48dd3747ff43f5493506ea753`
- SlateDB 0.15.0 `balanced` workload results

---

## 1. Summary

This RFC proposes adding a steady-state benchmark suite for kv-engine. The
suite complements the existing `write-perf` and `crud-bench` rows with
longer-running, validated workloads that start from a prepared database state
and measure read/write churn over a fixed keyspace.

The main new benchmark shape is a `balanced_zipfian` workload:

```text
initial state: prepared golden database
clients:       closed-loop workers
warmup:        configurable, default 60s locally
measurement:   configurable, default 180s locally
key selection: scrambled Zipfian, exponent 0.99
operation mix: 50% point read, 50% point update
durability:    WAL-enabled by default for production-style runs
```

The steady-state suite does not inherit all existing `write-perf` defaults
implicitly. It defines its own suite defaults and records the resolved values
in every result:

```text
records:       1,000,000 local default
key bytes:     20
value bytes:   400 local default
clients:       16 local default
warmup:        60s local default
measurement:   180s local default
WAL:           enabled by default
compaction:    leveled by default
```

Explicit CLI values override suite defaults. WAL must be represented as a
tri-state config in the implementation: unspecified means "use the suite
default", explicit enabled means "force WAL on", and explicit disabled means
"force WAL off". An explicit WAL-off request wins over the production-style
default.

The suite also adds related watch workloads:

```text
point_read_uniform
point_read_zipfian
point_read_missing_in_range
read_heavy_zipfian
balanced_zipfian
update_heavy_zipfian
range_scan_uniform
sustained_ingest
transaction_contention
idle
```

The benchmark contract is more important than the exact first implementation.
Each run should record enough metadata, counters, latency percentiles, and
validation results to make performance claims reproducible and to prevent
incorrect fast paths from looking like wins.

---

## 2. Motivation

Current kv-engine performance work has two useful surfaces:

1. `write-perf`, which is close to a local db_bench-style harness.
2. `crud-bench`, which compares ToyKV against peer embedded databases.

Those surfaces are good for targeted throughput rows and backend comparisons.
They are weaker for steady-state engine behavior:

1. Short fixed-count rows can hide p99 latency spikes from flush, compaction,
   WAL group commit, and cache misses.
2. Some rows include setup cost while others depend on a freshly loaded
   database, making it easy to compare unlike states.
3. Uniform read/write workloads do not stress hot-key churn, cache admission,
   memtable publish pressure, or compaction overlap the same way production
   workloads do.
4. Aggregate OPS does not explain whether a change improved API latency,
   durability latency, background drain time, or only measurement noise.
5. Workload correctness is mostly implicit. A read workload that accidentally
   misses or a range scan that returns too few rows can still produce a number.

SlateDB's benchmark suite is a useful reference because it treats workload
shape, warmup, durability drain, time-series sampling, and validation as part
of the benchmark result. This RFC adapts those ideas to kv-engine without
copying SlateDB's object-store-specific reporting.

---

## 3. Goals

1. Add steady-state workloads that run over a prepared fixed keyspace.
2. Add explicit prepare, warmup, measurement, drain, and cleanup phases.
3. Add a reusable "golden database" preparation mode for read-heavy and
   mixed workloads.
4. Add Zipfian key selection with exponent `0.99`.
5. Add in-range missing-key reads that are distributed through the loaded
   keyspace rather than beyond its end and cannot collide with ordinary
   generated keys.
6. Record per-operation latency percentiles, including p95 and p99 at minimum.
7. Record per-second throughput windows so instability is visible.
8. Split API return latency from durability or drain latency where possible.
9. Validate operation mix, read hit/miss expectations, scan result counts, and
   transaction outcomes.
10. Keep stdout machine-readable for JSON output and move human diagnostics to
    stderr.
11. Reuse RFC 011's `write-perf` schema and CLI direction where possible.

## 4. Non-Goals

1. Replacing `crud-bench` as the cross-database comparison harness.
2. Copying SlateDB object-store request, throughput, or S3 cost metrics.
3. Requiring 300M-record release-scale benchmark runs for local development.
4. Requiring privileged OS page-cache dropping.
5. Adding automated performance gates to normal CI in the MVP.
6. Making the first implementation statistically perfect across machines.
7. Rewriting storage internals only to fit the benchmark harness.

---

## 5. Existing Baseline

RFC 011 already defines a db_bench-style direction for
`kv-engine/src/bin/write-perf.rs`:

1. workload selection with `--bench`;
2. configurable sizes, value sizes, thread counts, cache size, WAL, vLog, and
   compaction options;
3. JSON output;
4. explicit load, reopen, measure, and cleanup phases;
5. optional latency sampling.

Since RFC 011 was written, `write-perf` has gained a richer CLI and structured
measurement records. The current binary already exposes workload selection,
presets, path control, JSON output, seeds, compaction mode, WAL, vLog, cache
capacity, SST and memtable sizing, parallel scan controls, and write-profile
instrumentation.

The remaining gap is benchmark semantics. Existing rows are still mostly
single-purpose throughput tests:

1. fill and read patterns inspired by db_bench;
2. WAL and memtable publish micro/macro rows;
3. vLog GC pressure rows;
4. scan and parallel-scan rows;
5. focused `crud-bench` rows maintained in the sibling benchmark repository.

Those rows should remain. The new suite adds longer, validated, steady-state
workloads that can catch regressions across the read path, write path, cache,
flush, compaction, and MVCC together.

---

## 6. Workload Model

### 6.1 Dataset Shape

The default local dataset should be large enough to exceed tiny in-memory
effects but small enough to run on a developer machine:

```text
record count: 1,000,000 local default
key bytes:    20
value bytes:  400 local default
key format:   8-byte big-endian ID + ASCII '0' padding
```

The key format preserves numeric ordering and makes range scans predictable.
It also matches the shape used by db_bench-style tools and SlateDB's benchmark
suite. The exact first slice may keep using the existing `key%08d` shape in
`write-perf`; switching the steady-state suite to fixed 20-byte keys should be
treated as part of the workload contract, not an incidental formatting change.

For the 20-byte steady-state key format, bytes `0..8` are the big-endian
logical ID and bytes `8..20` are ASCII `0` (`0x30`) for loaded records. The
last padding byte, byte offset `19`, is reserved for in-range absent probes.
Bulk-load and every future normal key generator must write only `0x30` at that
offset. The missing-key generator maps logical ID `i` to the same first 19
bytes as the loaded key and writes ASCII `1` (`0x31`) at byte offset `19`. This
keeps the missing key in the same logical region of the keyspace while making a
collision with a loaded key invalid by construction.

If a compatibility slice keeps the legacy `key%08d` format, the manifest must
record a collision-free absent-key mapping. The recommended mapping is:
loaded logical ID `i` becomes physical ID `2 * i`, missing logical ID `i`
becomes physical ID `2 * i + 1`, and both are formatted with the same
`key%08d` encoder over the doubled physical ID domain. That compatibility mode
does not use the reserved padding-byte rule.

The harness should support scaled runs:

```text
smoke:   10,000 records, short warmup and measurement
default: 1,000,000 records
large:   configurable, intended for dedicated benchmark hosts
```

Scaling must preserve operation mix, key selection, value size, durability mode,
and phase order unless the user explicitly overrides them.

### 6.2 Suite Defaults and Config Precedence

The suite should expose named scale presets while keeping benchmark semantics
constant:

```text
smoke:          10,000 records, 1 client, 0s warmup, 1s measurement
local default:  1,000,000 records, 16 clients, 60s warmup, 180s measurement
benchmark host: configurable records, 64 clients, 300s warmup, 900s measurement
```

All presets use 20-byte keys, 400-byte values, WAL enabled, and leveled
compaction unless explicitly overridden.

Configuration resolution order is:

1. suite defaults;
2. selected scale preset;
3. config-file values, if the implementation adds config files later;
4. explicit CLI flags.

Explicit CLI flags always win. In particular, explicit WAL-off must override
the suite's WAL-enabled default.

### 6.3 Randomness and Key Selection

Every steady-state result must record the random contract used for the run:
base seed, RNG algorithm, seed-derivation version, Zipfian exponent, scramble
function, key format, and operation-mix scheduler.

The MVP contract is:

1. Use `ChaCha12Rng` for generated streams and record the `rand_chacha` crate
   version from `Cargo.lock`.
2. Derive each per-client stream seed with `splitmix64(base_seed ^ label ^
   client_id)`.
3. Use separate streams for operation selection, key selection, and value
   generation. Operation-mix selection must not consume from the key-selection
   stream.
4. For scrambled Zipfian, sample a rank from the configured Zipfian
   distribution over `[0, record_count)`, compute `splitmix64(rank) %
   record_count`, and map that ID through the workload's key encoder.
5. Preserve the existing `cfg.seed + 123` convention only for legacy
   `write-perf` rows that already use it. Steady-state rows replace it with
   the labeled stream contract above.

### 6.4 Golden Database Preparation

Golden workloads start from a prepared database instead of loading data inside
each measured row.

Preparation phases:

1. **bulk-load**: insert every key exactly once, preferably in ordered batches;
2. **flush**: force durable storage of the loaded data;
3. **settle**: let background flush and compaction reach a stable state before
   a configured timeout;
4. **manifest**: record a golden-state summary, including file counts, level
   layout, key count, value size, engine options, source commit, and a stable
   engine-options hash.

The first implementation can clone the golden directory on local disk for each
workload. A later implementation can add cheaper snapshot or hard-link-based
cloning if needed.

`sustained_ingest` is the exception: it starts from an empty database.

Golden workloads must validate the manifest immediately after cloning or
opening the prepared database and fail if the manifest is missing, stale, or
incompatible with the requested task. This reuses RFC 011's dataset metadata
rule for `--reuse`; the new golden manifest is the same safety requirement
applied to a reusable prepared database.

The manifest must also record settle status:

```text
settle_status: settled|timed_out
settle_elapsed_ms
settle_timeout_secs
```

A golden preparation that reaches the timeout before quiescing is invalid and
must not produce a gate-acceptable manifest. Gates must reject
`settle_status=timed_out`, even if a manifest file exists.

`--settle-timeout-secs` applies to both golden preparation settle and optional
post-measurement background drain. Each phase records its own elapsed time and
status so a timeout in one phase cannot be confused with a timeout in the
other.

Warmup for mixed workloads may intentionally mutate the clone. In that case the
original golden manifest remains the provenance check, not the measurement
baseline. The harness should record a post-warmup baseline summary after the
warmup drain and before measurement starts, including at least file counts,
level layout, and engine counters that are available. Result interpretation
should compare measured-window deltas against that post-warmup baseline.

### 6.5 Closed-Loop Clients

Steady-state workloads use closed-loop workers. Each worker waits for its
operation to complete before issuing the next one. This models service callers
with bounded in-flight work and keeps latency meaningful.

Default local settings:

```text
clients:      16
warmup:       60s
measurement:  180s
```

Dedicated benchmark-host settings may use larger values, for example 64
clients, 300s warmup, and 900s measurement. The local defaults should keep the
suite practical while preserving the same workload semantics.

### 6.6 Warmup and Measurement

Warmup must run the same operation path as measurement. Warmup output is not
part of the main result, but warmup errors still fail the workload.

For write-capable workloads, the harness should flush or drain warmup writes
before starting measurement so the measured window starts from a clear phase
boundary.

Measurement records:

1. total operations per API;
2. completed operation count per API and total completed operation count;
3. total logical bytes per API;
4. per-second operations and logical bytes;
5. per-operation latency samples;
6. engine counters before and after the measured window;
7. post-measurement drain time.

### 6.7 Durability and Drain

For ToyKV's current synchronous WAL mode, API return latency includes the
durable write path. Still, the benchmark should record drain separately:

1. `api_latency_ms`: time from issuing the API call until it returns;
2. `flush_drain_ms`: time to force pending flush work after measured clients
   stop;
3. `background_drain_ms`: time to let configured background work settle, if the
   workload requests it;
4. `background_drain_status`: `settled`, `timed_out`, or `not_requested`;
5. `durability_latency_ms`: optional future metric if ToyKV gains an async
   durable-frontier API.

The MVP should not invent a fake durability-frontier metric. It should record
the metrics ToyKV can measure honestly today and leave the async durability row
as a follow-up.

---

## 7. Workloads

### 7.1 `idle`

Open a clone of the golden database, wait for startup to finish, then measure
with no client API calls.

Purpose:

1. expose background flush or compaction work after open;
2. capture process and engine baseline counters;
3. catch unexpected background churn in a supposedly settled database.

### 7.2 `point_read_uniform`

Run 100% point reads over existing keys selected uniformly.

Validation:

1. every read must hit;
2. observed errors must be zero.

### 7.3 `point_read_zipfian`

Run 100% point reads over existing keys selected with scrambled Zipfian
distribution, exponent `0.99`.

Purpose:

1. measure hot-key cache behavior;
2. expose contention on shared read-side structures;
3. complement uniform `readrandom` rows.

### 7.4 `point_read_missing_in_range`

Run 100% point reads for absent keys distributed through the loaded keyspace.
For the 20-byte key contract, missing logical ID `i` uses the same bytes
`0..18` as loaded logical ID `i`, but writes byte offset `19` as ASCII `1`
instead of ASCII `0`. If the first implementation keeps `write-perf`'s shorter
`key%08d` keys, it must use the even/odd physical ID compatibility mapping from
Section 6.1 and document that mapping in the golden manifest.

Validation:

1. every operation must miss;
2. any hit fails the workload.

Purpose:

1. measure bloom-filter and negative lookup behavior;
2. avoid making missing reads artificially cheap by probing only beyond the
   loaded key range.

### 7.5 `read_heavy_zipfian`

Run 95% point reads and 5% updates over existing keys selected with scrambled
Zipfian distribution.

Purpose:

1. measure read path behavior under light write churn;
2. catch cache and compaction interference without making the workload mostly
   writes.

### 7.6 `balanced_zipfian`

Run 50% point reads and 50% updates over existing keys selected with scrambled
Zipfian distribution.

Operation selection uses the operation stream from Section 6.3, and key
selection uses the independent key stream. For the default 50/50 mix, the
operation-mix scheduler uses a deterministic 1,000-slot period containing 500
reads and 500 updates, shuffled per client from the operation stream and then
cycled. Other mixed workloads use the same period rule when their configured
ratios are representable at that period; otherwise config validation rejects
the run unless a larger explicit period is provided.

Purpose:

1. primary steady-state regression row;
2. stress memtable publish, WAL group commit, cache churn, and compaction
   overlap together;
3. provide a production-style alternative to isolated create/update/delete
   rows.

### 7.7 `update_heavy_zipfian`

Run 5% point reads and 95% updates over existing keys selected with scrambled
Zipfian distribution.

Purpose:

1. measure overwrite churn over a fixed keyspace;
2. expose write amplification and compaction pressure;
3. avoid growing the logical database during the measured window.

### 7.8 `range_scan_uniform`

Run 100% forward scans. Each scan starts at a uniformly selected existing key
and returns up to `scan_limit` records, default `10`.

Validation:

1. each scan must return `min(scan_limit, record_count - start_id)` rows;
2. returned keys must be ordered.

Purpose:

1. measure iterator creation plus consumption as one operation;
2. keep scan latency meaningful;
3. complement full-scan and parallel-scan throughput rows.

### 7.9 `sustained_ingest`

Run 100% inserts into an empty database for a fixed duration. Each worker uses
unique sequential IDs from a shared allocator.

Purpose:

1. measure long-running ingest throughput;
2. expose backpressure, flush, and compaction behavior as the database grows;
3. produce a final drain measurement after clients stop.

### 7.10 `transaction_contention`

Run serializable transactions over a hot fixed key set. Each transaction
contains five reads and five updates in random order, then commits.

This workload is not part of the MVP unless the harness first exposes
`serializable: true` engine options. The config must define hot-set size, read
count, update count, retry policy, and whether expected conflicts contribute to
latency summaries.

Validation:

1. attempted transactions must equal commits plus expected conflicts;
2. read operations in committed transactions must hit;
3. unexpected engine errors fail the workload.

Purpose:

1. measure MVCC/OCC overhead under contention;
2. keep expected conflicts visible instead of mixing them into generic errors.

---

## 8. Metrics

### 8.1 Application Operations

For each API row, record:

```text
total
avg_per_sec
p1_per_sec
p50_per_sec
p95_per_sec
p99_per_sec
min_per_sec
max_per_sec
```

The rate percentiles should come from complete one-second measurement windows.
Partial boundary windows should be excluded from percentile calculations.

### 8.2 Application Throughput

Record logical bytes per API:

1. `get`: request key plus returned value; a miss contributes the request key
   bytes and zero value bytes, both to total logical bytes and per-second byte
   throughput;
2. `put`: key plus value;
3. `delete`: key;
4. `scan`: all returned keys and values;
5. transaction reads and writes under separate API names.

### 8.3 Application Latency

At minimum, record:

```text
count
avg_ms
p50_ms
p95_ms
p99_ms
min_ms
max_ms
```

If the sample volume is high, the MVP may use bounded latency sampling, but the
output must state the sampling policy.

Latency output must also record the unsampled completed operation count. A
non-idle workload with zero completed operations is invalid even if its latency
summary object is syntactically present.

### 8.4 Engine Counters

The first implementation should record the counters that already exist in
`write-perf`'s structured output:

1. block-cache entry count;
2. value-cache hits and misses;
3. vLog bytes and file count when vLog is enabled;
4. vLog GC entries, bytes, and files processed when available;
5. range tombstone and compaction-filter counters already exposed by
   `write-perf`.

Counters should be recorded as before, after, and delta values where possible.

The following counters are desirable, but require additional engine
instrumentation or schema work and should not be assumed available in the MVP:

1. block-cache hits and misses outside the parallel-scan shard summaries;
2. flush count and flush elapsed time;
3. compaction count, input bytes, output bytes, and elapsed time;
4. WAL bytes written and sync count beyond the existing write-profile timing
   fields.

### 8.5 Process and Machine Metrics

The MVP can omit host-level process and machine time series if it would add too
much dependency surface. A later slice should add:

1. process RSS;
2. process CPU time;
3. disk read/write bytes;
4. disk read/write operations.

These are useful for benchmark-host runs but should not block the core suite.

---

## 9. Output Schema

Prefer extending the existing additive `kv-engine.write-perf.v1` record if the
steady-state rows live in `write-perf`. Introduce `kv-engine.steady-state.v1`
only if the suite becomes a separate binary or the record meaning differs from
RFC 011's existing schema. The schema should include:

```text
schema
run_id
unix_epoch_ms
source_commit
workload
phase
preset
engine_options
dataset
task
latency
result
validation
counters
```

If `kv-engine.write-perf.v1` is reused, the following existing paths must keep
their current meaning:

```text
measurement
params
result.found
counters
```

`measurement` remains the row or measurement label used by existing parsers.
`params` remains the legacy parameter object. `result.found` remains the legacy
workload-specific found count and must not be reinterpreted as
`validation.read_hits`; for example, a missing-read workload can legitimately
produce `result.found = 0`. Existing `counters` names remain additive and
backward-compatible.

The steady-state suite adds new optional paths instead of changing those paths:

```text
phase
task
latency
validation
golden_manifest
post_warmup_baseline
drain
```

`phase` is one of `prepare`, `warmup`, `measurement`, or `drain`. `task`
contains the resolved steady-state workload config. `validation` contains
workload-specific validation counts and gate status. `golden_manifest` records
the source manifest path, digest, settle status, and summary used for
provenance. `post_warmup_baseline` records the baseline summary used for
measured-window deltas when warmup mutates the clone. `drain` records flush and
background drain elapsed times and statuses.

`task` should include:

```text
clients
warmup_secs
measurement_secs
operation_mix
key_selection
scan_limit
seed
```

`validation` should include:

```text
errors
read_hits
read_misses
expected_read_hits
expected_read_misses
observed_operation_mix
scan_count_errors
transaction_attempts
transaction_commits
transaction_conflicts
selected_operations
completed_operations
min_completed_operations
```

JSON output must remain parseable on stdout. Human progress logs, profile
tables, and warnings go to stderr.

If `kv-engine.write-perf.v1` is reused, existing field meanings must not change.
Latency, validation, and golden-manifest details should be additive fields so
existing parsers continue to work.

Schema tests must include compatibility fixtures before the suite is considered
implemented:

1. an existing `kv-engine.write-perf.v1` record parses unchanged;
2. steady-state hit-read, missing-read, scan, and mixed-workload records parse;
3. `result.found` is not mapped to `validation.read_hits`;
4. every per-workload fixture includes the resolved task config, validation
   object, and completed operation count.

---

## 10. CLI

RFC 011 already defines most CLI dimensions. Its lifecycle flags are not all
implemented in current `write-perf`, so Phase 1 must either finish the RFC 011
path lifecycle first or explicitly supersede it with the golden-dataset
lifecycle below. The implementation should not maintain two incompatible
meanings for fresh, reuse, cleanup, or reopen behavior.

This RFC adds steady-state specific options:

```text
--suite steady-state
--prepare-golden
--golden-path <path>
--clone-golden
--clients <n>
--warmup-secs <n>
--measurement-secs <n>
--operation-mix get=0.5,put=0.5
--operation-mix-period <n>
--key-selection uniform|scrambled-zipfian-0.99|uniform-absent|unique-sequential
--scan-limit <n>
--latency-sample-every <n>
--settle-timeout-secs <n>
```

Steady-state config validation must run before opening or creating the
database:

1. `--clients` must be positive.
2. `--latency-sample-every` is optional; if present, it must be positive. Use
   absence, not zero, to disable latency sampling.
3. Operation-mix values must be finite, non-negative, and sum to `1.0` within
   `1e-9`. Each non-zero ratio must be representable by
   `--operation-mix-period`, which defaults to `1000`.
4. `--warmup-secs` may be zero. `--measurement-secs` must be positive for every
   non-idle workload.
5. `--scan-limit` must be positive for scan workloads.
6. `--settle-timeout-secs` must be positive whenever golden preparation settle
   or post-measurement background drain is requested.

Example:

```bash
cargo run --release --bin write-perf -- \
  --suite steady-state \
  --prepare-golden \
  --golden-path /tmp/toykv-golden \
  --num 1000000 \
  --value-size 400 \
  --wal \
  --compaction leveled \
  --output json
```

```bash
cargo run --release --bin write-perf -- \
  --suite steady-state \
  --bench balanced_zipfian \
  --golden-path /tmp/toykv-golden \
  --clients 16 \
  --warmup-secs 60 \
  --measurement-secs 180 \
  --wal \
  --output json
```

---

## 11. Validation Rules

Every workload should fail if engine operations return unexpected errors.

Additional rules:

1. Hit-only read workloads must report zero misses.
2. Missing-read workloads must report at least one miss and zero hits.
3. Non-idle workloads must report completed operations greater than or equal to
   `min_completed_operations`. The MVP default is `1`; benchmark-gate profiles
   should raise it to a workload-specific value.
4. Mixed get/put workloads validate the configured mix against selected
   operations, not completed operations. The selected-operation denominator must
   be positive. For gated mixed runs, each client must use the deterministic
   operation-mix period from Section 7.6, and the selected counts must match the
   configured period counts for every complete period. Tail operations from an
   incomplete final period are recorded separately and are excluded from the
   hard mix check.
5. Range scans must return the expected number of records for the chosen start
   key.
6. Sustained ingest must allocate unique keys without collision.
7. Transaction attempts must reconcile with commits plus expected conflicts.
8. Golden workloads must start from the expected golden manifest summary.
9. Golden preparation and requested post-measurement background drain must not
   time out for a gate-valid artifact.
10. Workloads should record but not automatically fail on ordinary compaction
   activity unless the engine reports a hard compaction error.

The validation section is part of the benchmark result so downstream gate tools
can reject incomplete or invalid artifacts.

---

## 12. Implementation Plan

### Phase 1: RFC 011 Alignment

1. Keep existing `write-perf` workloads unchanged.
2. Decide whether steady-state work completes RFC 011's lifecycle flags or
   supersedes them with the golden-dataset lifecycle.
3. Add shared structs for steady-state task config, latency summaries, and
   per-second rate windows.
4. Add JSON schema tests for the new records.

### Phase 2: Golden Dataset

1. Add `--prepare-golden`.
2. Bulk-load ordered keys into `--golden-path`.
3. Flush and drain the loaded database.
4. Write a small golden manifest JSON file next to the database.
5. Add smoke tests with a tiny record count.

### Phase 3: Read-Only Workloads

1. Implement `point_read_uniform`.
2. Implement `point_read_zipfian`.
3. Implement `point_read_missing_in_range`.
4. Implement `range_scan_uniform`.
5. Add validation for hit/miss and scan counts.

### Phase 4: Mixed Workloads

1. Implement `read_heavy_zipfian`.
2. Implement `balanced_zipfian`.
3. Implement `update_heavy_zipfian`.
4. Add operation-mix validation.
5. Add counter deltas for currently available counters.
6. Leave cache hit/miss, WAL byte, flush, and compaction byte counters as
   follow-up instrumentation unless the engine APIs land first.

### Phase 5: Write and Transaction Workloads

1. Implement `sustained_ingest`.
2. Add drain metrics after client stop.
3. Add `transaction_contention` only after the harness exposes serializable
   engine configuration and the transaction workload contract is fully
   parameterized.
4. Add transaction outcome validation with the transaction workload.

### Phase 6: Gate Integration

1. Keep artifact parsing and gating with the schema owner. If `write-perf`
   owns `kv-engine.write-perf.v1` or `kv-engine.steady-state.v1`, local
   validation for those records belongs in this repository.
2. Keep cross-database comparison gates in the sibling `crud-bench` repository,
   where the comparison CSV and row schema live.
3. Add recommended gate rows to `docs/bench-report-crud-bench-rocksdb.md`.
4. Keep normal CI free of long benchmark requirements.

---

## 13. Open Questions

1. Should the suite live entirely inside `write-perf`, or should it become a
   dedicated `steady-state-perf` binary once the config grows?
2. Should golden database cloning use directory copy first, or should the MVP
   add hard-link-based cloning for SST and vLog files?
3. What local default is large enough to catch realistic cache and compaction
   behavior without making ordinary development painful?
4. Should latency summaries be exact for all operations or sampled by default?
5. Should `transaction_contention` land in this suite or in a separate MVCC/OCC
   benchmark once serializable configuration is exposed?

---

## 14. Expected Impact

The new suite should make performance follow-up decisions less dependent on
short, noisy rows. It gives kv-engine a stable way to answer:

1. Did this change improve throughput without damaging p99 latency?
2. Did this change only help a setup-heavy row, or does it help steady-state
   churn?
3. Did the read path remain strong under concurrent updates?
4. Did WAL, flush, or compaction drain get worse?
5. Are benchmark results valid, or did the workload silently miss, scan too
   little, or drift away from the configured operation mix?

The first important acceptance target is not a speedup. It is a repeatable,
validated `balanced_zipfian` row that can become a default regression watch
before further write-path or cache-path optimization work.
