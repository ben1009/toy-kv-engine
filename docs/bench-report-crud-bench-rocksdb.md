# crud-bench: ToyKV vs RocksDB

This report tracks ToyKV against the embedded RocksDB backend in a sibling
`crud-bench` checkout.

## Summary

Current RFC 018 data: 2026-08-26, using 5-second warmup and 15-second
measurement windows. Current isolated steady-state range-scan confirmation:
2026-08-26 after three repeats per backend. Historical data includes the
2026-07-23 PR #194 embedded durable rerun and the 2026-08-01 focused scan
comparison after the ToyKV count-only iterator optimization. Earlier durable
RocksDB/Fjall runs are from 2026-07-13 to
2026-07-16, with focused scan and batch reruns from 2026-07-14 to 2026-07-15.

Configuration:

```bash
cd <crud-bench checkout>
cargo run --release --no-default-features --features fjall,rocksdb,toykv -- \
  --database <toykv|rocksdb|fjall> \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --color never
```

Latest PR #194 embedded comparison command shape:

```bash
cd <crud-bench checkout>
cargo run --release --bin crud-bench \
  --no-default-features \
  --features toykv,fjall,rocksdb,redb,surrealkv -- \
  --database <toykv|fjall|rocksdb|redb|surrealkv> \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-scans
```

RocksDB backend version: `surrealdb-rocksdb 0.24.0-surreal.5` maps to
`surrealdb-librocksdb-sys 0.18.3+11.0.0-4`, whose vendored
`rocksdb/version.h` reports raw RocksDB `11.0.0`. The latest upstream raw
RocksDB release checked on 2026-07-16 is `11.1.2`, so these results are against
the latest available SurrealDB Rust binding but not the latest upstream RocksDB
source.

## Latest PR #194 Embedded Sync Comparison

Artifacts:

- `result-compare_embedded_sync_100k_toykv_pr194.{csv,json,html}`
- `result-compare_embedded_sync_100k_fjall.{csv,json,html}`
- `result-compare_embedded_sync_100k_rocksdb.{csv,json,html}`
- `result-compare_embedded_sync_100k_redb.{csv,json,html}`
- `result-compare_embedded_sync_100k_surrealkv.{csv,json,html}`

All rows are OPS. Higher is better. This pass skipped scan rows to focus on the
write path affected by PR #194.

| Row | ToyKV | Fjall | RocksDB | Redb | SurrealKV | Winner |
|---|---:|---:|---:|---:|---:|---|
| Create | **13,217.74** | 2,231.21 | 12,793.72 | 1,600.00 | 1,689.85 | ToyKV, near tie with RocksDB |
| Read | **3,607,177.24** | 1,671,018.11 | 1,610,136.21 | 459,820.62 | 1,357,430.66 | ToyKV |
| Update | **13,233.50** | 1,706.49 | 13,090.03 | 1,669.70 | 1,694.87 | ToyKV, near tie with RocksDB |
| Delete | **14,225.56** | 1,809.39 | 13,605.36 | 1,871.66 | 1,738.64 | ToyKV, near tie with RocksDB |
| batch_create_100 | **7,607.83** | 829.68 | 648.83 | 277.76 | 1,228.24 | ToyKV |
| batch_read_100 | 33,510.64 | 30,455.86 | 29,860.19 | 24,174.78 | **34,113.49** | SurrealKV, near tie with ToyKV |
| batch_update_100 | **6,476.86** | 703.36 | 655.22 | 264.29 | 1,362.95 | ToyKV |
| batch_delete_100 | **10,675.25** | 852.99 | 3,954.19 | 277.42 | 1,676.95 | ToyKV |
| batch_create_1000 | **1,750.44** | 440.72 | 249.12 | 72.29 | 487.72 | ToyKV |
| batch_read_1000 | **6,655.78** | 5,193.24 | 5,043.51 | 3,737.96 | 3,315.66 | ToyKV |
| batch_update_1000 | **1,403.24** | 235.77 | 406.97 | 71.78 | 546.16 | ToyKV |
| batch_delete_1000 | **6,026.26** | 361.09 | 315.56 | 81.73 | 1,106.60 | ToyKV |

ToyKV wins 11 of 12 rows. The only peer lead is `batch_read_100`, where
SurrealKV is ahead by about 1.8%, which is within the noise range seen in prior
short batch-read reruns. On point create/update/delete, ToyKV and RocksDB are
near ties. The meaningful PR #194 signal is the large durable batch write set:
ToyKV is ahead of every peer on `batch_create_1000`, `batch_update_1000`, and
`batch_delete_1000`. `batch_delete_1000` is 5.45x faster than SurrealKV, 16.69x
faster than Fjall, and 19.10x faster than RocksDB in this run.

Latest three-way rerun:

Artifacts:

- `result-toykv_latest_sync_100k.{csv,json,html}`
- `result-fjall_latest_sync_100k.{csv,json,html}`
- `result-rocksdb_latest_sync_100k.{csv,json,html}`

| Row | ToyKV | RocksDB | Fjall | Winner |
|---|---:|---:|---:|---|
| Create | 13,329.43 | **13,671.98** | 2,232.51 | RocksDB |
| Read | **2,699,841.67** | 1,495,577.91 | 1,162,106.26 | ToyKV |
| Update | **13,280.17** | 12,276.99 | 1,705.52 | ToyKV |
| Delete | **14,035.21** | 12,948.37 | 1,798.72 | ToyKV |
| batch_create_100 | **7,097.12** | 686.80 | 843.01 | ToyKV |
| batch_read_100 | **30,279.26** | 30,218.28 | 26,109.64 | ToyKV, near tie |
| batch_update_100 | **8,049.65** | 646.40 | 562.58 | ToyKV |
| batch_delete_100 | **11,919.60** | 3,518.71 | 508.40 | ToyKV |
| batch_create_1000 | **1,652.78** | 356.43 | 370.47 | ToyKV |
| batch_read_1000 | **5,635.30** | 5,331.97 | 5,217.01 | ToyKV |
| batch_update_1000 | **1,214.94** | 289.52 | 166.54 | ToyKV |
| batch_delete_1000 | **5,179.18** | 290.81 | 323.37 | ToyKV |

ToyKV wins 11 of 12 rows in the latest three-way compare. RocksDB only wins
`Create`, and Fjall is never best on this rerun.

ToyKV is ahead of RocksDB on point reads and durable batch writes, including
large batch create/update/delete rows. The PR #170 focused scan rerun flipped
four of the five previously RocksDB-winning scan rows. PR #173 repeated the
remaining `select(*) limit(100)` gap and put ToyKV ahead on all five focused
no-index scan watch rows. The 2026-08-01 scan rerun after the ToyKV count-only
iterator optimization keeps ToyKV ahead of both RocksDB and Fjall on all
supported focused no-index scan watch rows; `count()` is now 683.48 OPS for
ToyKV vs 431.77 OPS for RocksDB and 105.63 OPS for Fjall. The focused
`batch_read_100` row changed substantially across the default
250-iteration reruns, so it was repeated with a temporary 10,000-iteration
batch-read config. That longer timed row puts ToyKV ahead by 15.6%. ToyKV also
still leads `batch_read_1000`.

Artifacts:

- `result-toykv_rocksdb_compare_toykv_sync_100k.{csv,json,html}`
- `result-toykv_rocksdb_compare_rocksdb_sync_100k.{csv,json,html}`
- `result-toykv_rocksdb_compare_fjall_sync_100k.{csv,json,html}`
- `result-toykv_read_rerun_pr170_sync_100k.{csv,json,html}`
- `result-rocksdb_read_rerun_pr170_sync_100k.{csv,json,html}`
- `result-toykv_batch_push_output_sync_100k.{csv,json,html}`
- `result-rocksdb_batch_compare_sync_100k.{csv,json,html}`
- `result-toykv_batch_rerun_pr170_sync_100k.{csv,json,html}`
- `result-rocksdb_batch_rerun_pr170_sync_100k.{csv,json,html}`
- `result-toykv_batch_confirm_pr170_sync_100k.{csv,json,html}`
- `result-rocksdb_batch_confirm_pr170_sync_100k.{csv,json,html}`
- `result-toykv_batch100_iter10000_pr170_sync_100k.{csv,json,html}`
- `result-rocksdb_batch100_iter10000_pr170_sync_100k.{csv,json,html}`
- `result-toykv_read_rerun_pr173_sync_100k.{csv,json,html}`
- `result-rocksdb_read_rerun_pr173_sync_100k.{csv,json,html}`
- `result.{csv,json,html}` from the 2026-08-01 ToyKV scan confirmation run
- `result-rocksdb-scan-compare.{csv,json,html}`
- `result-fjall-scan-compare.{csv,json,html}`

## Durable 100k Results

All rows are OPS. Higher is better.

| Row | ToyKV | RocksDB | Fjall | Result |
|---|---:|---:|---:|---|
| Create | 13,285 | **13,769** | 2,224 | RocksDB +3.6%, near tie |
| Read | **4,024,724** | 1,576,567 | 2,002,965 | ToyKV +155.3% |
| Update | 13,268 | **13,974** | 1,717 | RocksDB +5.3%, near tie |
| Delete | 14,151 | **14,194** | 1,805 | RocksDB +0.3%, tie |
| batch_create_100 | **6,642** | 1,580 | 645 | ToyKV +320.4% |
| batch_read_100 | 36,283 | 48,411 | **48,467** | RocksDB +33.4% over ToyKV |
| batch_update_100 | **6,499** | 2,167 | 667 | ToyKV +199.9% |
| batch_delete_100 | **11,092** | 10,781 | 730 | ToyKV +2.9%, near tie |
| batch_create_1000 | **1,562** | 481 | 318 | ToyKV +224.7% |
| batch_read_1000 | **5,995** | 5,100 | 5,220 | ToyKV +17.5% over RocksDB |
| batch_update_1000 | **1,643** | 459 | 204 | ToyKV +258.0% |
| batch_delete_1000 | **4,693** | 393 | 299 | ToyKV +1,094.1% |

## Focused Scan Rerun

These rows come from the 2026-08-01 focused scan rerun after the ToyKV
count-only iterator optimization. The command used
`--sync --skip-indexes --skip-batches` to isolate the read-only no-index scan
rows. All rows are OPS. Higher is better.

| Row | ToyKV | RocksDB | Fjall | Result |
|---|---:|---:|---:|---|
| Read | **3,630,047.09** | 1,502,250.12 | 1,669,290.01 | ToyKV +141.6% over RocksDB |
| count() | **683.48** | 431.77 | 105.63 | ToyKV +58.3% over RocksDB |
| select(id) limit(100) | **553,042.81** | 494,049.35 | 105,512.11 | ToyKV +11.9% over RocksDB |
| select(*) limit(100) | **515,849.87** | 510,486.54 | 88,287.29 | ToyKV +1.1%, near tie with RocksDB |
| select(id) start(5000) limit(100) | **15,211.95** | 11,850.74 | 2,067.49 | ToyKV +28.4% over RocksDB |
| select(*) start(5000) limit(100) | **15,136.67** | 11,898.40 | 2,051.96 | ToyKV +27.2% over RocksDB |

The count-only iterator patch moved ToyKV `count()` from 623.35 OPS before the
change to 688.72 OPS on the first patched run and 683.48 OPS on the confirmation
run, a roughly 9.6%-10.5% improvement. The 2026-07-13 full durable run had
RocksDB ahead on all five scan rows before the PR #170, PR #173, and count-only
iterator scan work. Keep the full-run artifacts for historical comparison, but
use this focused rerun as the current scan baseline.

## RFC 018 Seven-Row Steady-State Comparison Subset

Run date: 2026-08-26. This sequential full-matrix run used one client and one
thread, sync enabled, 10,000 records, a 5-second warmup, a 15-second
measurement window, and latency sampling on every operation. The same settings
were used for ToyKV and RocksDB, with the seven CRUD comparison rows selected.
This is a comparison subset, not the complete RFC 018 workload list; it omits
`idle`, `point_read_uniform`, and `transaction_contention`.

Artifacts:

- `result-full-compare-toykv-15s.json`
- `result-full-compare-rocksdb-15s.json`

All rows completed with zero validation errors, and every observed operation
mix matched its expected prefix.

| Row | RocksDB OPS | ToyKV OPS | OPS delta | RocksDB p95 | ToyKV p95 | RocksDB p99 | ToyKV p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `balanced_zipfian` | 985.81 | **3,521.76** | **+257.24%** | 2.012 ms | **0.575 ms** | 9.775 ms | **1.181 ms** |
| `read_heavy_zipfian` | 10,900.60 | **29,833.36** | **+173.69%** | 0.123 ms | **0.121 ms** | 1.347 ms | **0.525 ms** |
| `update_heavy_zipfian` | 1,844.92 | **1,860.61** | **+0.85%** | 1.071 ms | **1.040 ms** | 1.248 ms | **1.192 ms** |
| `point_read_zipfian` | 538,022.35 | **616,460.10** | **+14.58%** | 0.002 ms | 0.002 ms | 0.002 ms | 0.002 ms |
| `point_read_missing_in_range` | 1,195,977.72 | **2,846,696.89** | **+138.02%** | 0.000 ms | 0.000 ms | 0.001 ms | 0.000 ms |
| `range_scan_uniform` | **293.22** | 148.92 | -49.21% | **6.747 ms** | 13.807 ms | **7.123 ms** | 15.063 ms |
| `sustained_ingest` | 1,767.25 | **1,772.31** | **+0.29%** | 1.141 ms | 1.156 ms | 1.240 ms | **1.192 ms** |

The cross-database gate passes data validation but fails the configured 5%
performance thresholds on `range_scan_uniform`: ToyKV is 49.21% slower in OPS,
104.64% slower in p95, and 111.47% slower in p99 in this sequential matrix.
The isolated three-repeat range-scan confirmation below shows the opposite
result, so the full-matrix result is retained as evidence of run-order or
background-state sensitivity rather than a standalone optimization decision.

## RFC 018 Steady-State Range Scan Confirmation

Run date: 2026-08-26. This confirmation used the merged RFC 018
`range_scan_uniform` workload with one client and one thread, sync enabled,
10,000 prepared records, a 5-second warmup, a 15-second measurement window,
and latency sampling on every operation. Each backend was run three times in
isolation with the same command shape.

Artifacts:

- `result-range-repeat-toykv-{1,2,3}.json`
- `result-range-repeat-rocksdb-{1,2,3}.json`

All six runs completed with `validation.errors = 0` and the expected scan
operation mix.

| Backend | Run 1 OPS | Run 2 OPS | Run 3 OPS | Average OPS | Average p95 | Average p99 |
|---|---:|---:|---:|---:|---:|---:|
| ToyKV | 2,470.63 | 2,369.01 | 2,365.40 | **2,401.68** | **0.785 ms** | **0.823 ms** |
| RocksDB | 1,466.16 | 1,471.71 | 1,450.31 | 1,462.73 | 1.283 ms | 1.351 ms |

Per-run latency values:

| Backend | Run | p95 | p99 |
|---|---:|---:|---:|
| ToyKV | 1 | 0.765 ms | 0.801 ms |
| ToyKV | 2 | 0.794 ms | 0.832 ms |
| ToyKV | 3 | 0.797 ms | 0.836 ms |
| RocksDB | 1 | 1.281 ms | 1.346 ms |
| RocksDB | 2 | 1.275 ms | 1.334 ms |
| RocksDB | 3 | 1.293 ms | 1.374 ms |

ToyKV is ahead of RocksDB by 64.19% in average OPS, with 38.79% lower p95
latency and 39.10% lower p99 latency. An earlier single-run full-matrix pass
showed the opposite result, but these isolated repeats do not reproduce that
regression. The current evidence does not justify a ToyKV range-scan
optimization; future comparisons should continue using isolated repeated
runs or longer benchmark-host windows.

## Focused Batch Rerun

These rows come from the 2026-07-14 focused PR #170 batch rerun after the
small-batch `batch_get` output construction change. The command used
`--skip-indexes --skip-scans` to isolate batch workloads. The `batch_read_100`
row uses a temporary config that raises both `batch_create_100` and
`batch_read_100` from 250 to 10,000 iterations, because the default row only
runs for a few milliseconds and varied too much across single samples.

| Row | ToyKV | RocksDB | Result |
|---|---:|---:|---|
| batch_read_100 | **43,641.35** | 37,748.88 | ToyKV +15.6% |
| batch_read_1000 | **6,611.48** | 5,697.10 | ToyKV +16.0% |

The default 250-iteration `batch_read_100` reruns were too short to use as hard
evidence: ToyKV ranged from `28,638.24` to `50,390.36` OPS, and RocksDB ranged
from `29,952.76` to `50,949.15` OPS. Increasing the timed read row to 10,000
iterations lengthened the row to 229-265 ms and moved the comparison back to
ToyKV +15.6%. Keep `batch_read_100` as a regression watch row, but do not use
the 250-iteration percentages as decision-grade data.

## Backend Parity Notes

The current RocksDB adapter configures:

- Level compaction.
- 64 KiB data blocks.
- 256 MiB write buffers.
- Bloom filters.
- Blob files enabled with `min_blob_size = 4 KiB`.
- LZ4/Snappy dependencies available, with per-level compression configured.
- `WriteOptions::set_sync(true)` for durable benchmark runs.

The current ToyKV adapter configures:

- Leveled compaction.
- 64 KiB data blocks.
- 256 MiB target SST size.
- WAL enabled for `--sync`.
- vLog value separation enabled at `min_value_size = 4 KiB`.
- Large TinyUFO block cache and cache backfill.

This is close enough for a first production-style comparison. Result claims must
still state that RocksDB has many more mature tuning knobs and that this is a
matched-adapter benchmark, not a universal RocksDB result.

## Interpretation

Do not prioritize single-write optimization from this run. RocksDB is only
0.3%-5.3% ahead on create/update/delete, which is below the profiling gate and
small enough to treat as a near tie until repeated.

Keep the current durable batch-write path intact. ToyKV is substantially ahead
on `batch_create_100`, `batch_update_100`, `batch_create_1000`,
`batch_update_1000`, and `batch_delete_1000`.

The focused read-path gap remains closed after the 2026-08-01 scan rerun:

- `count()`: ToyKV +58.3% over RocksDB after the count-only iterator
  optimization.
- `select(*) limit(100)`: ToyKV +1.1%, effectively tied but still ahead in the
  latest focused scan rerun.

Do not start deeper full-projection scan work from the old PR #170 gap unless a
future repeat reproduces a stable RocksDB lead. Keep `batch_read_100`,
`batch_read_1000`, `count()`, `select(id) limit(100)`, `select(*) limit(100)`,
and the two `start(5000) limit(100)` rows as regression watch rows because
ToyKV now leads or ties them.

## Gates

Use these gates before accepting performance-oriented ToyKV changes:

- No ToyKV row in the durable RocksDB comparison regresses by more than 5%
  against the previous ToyKV baseline.
- `hotpath-profile`, `clippy`, unit, integration, coverage, and sanitizer CI
  remain green.
- If RocksDB wins a workload by more than 10%, profile that exact workload
  before choosing an implementation change.
- Do not accept buffered-only improvements that regress durable `--sync`
  workloads.

The repeatable gate checker lives in the
[`crud-bench`](https://github.com/ben1009/crud-bench) checkout:

```bash
cd "$CRUD_BENCH_CHECKOUT"
cargo run --release --bin perf-gate -- \
  --baseline-sync previous-toykv-sync.csv \
  --current-sync current-toykv-sync.csv \
  --baseline-nosync previous-toykv-nosync.csv \
  --current-nosync current-toykv-nosync.csv \
  --fjall-sync current-fjall-sync.csv
```

The default rows are `put_c`, `batch_create_100`, `batch_create_1000`,
`batch_delete_100`, and `batch_delete_1000`. The default sync/no-sync ratio
gate requires improvement on at least two of `put_c`, `batch_create_1000`, and
`batch_delete_1000`. Add `--baseline-latency-sync` and
`--current-latency-sync` with single-client sync CSVs to enforce the latency
gate on the same default rows. Both p95 and p99 must pass, and each metric may
regress by at most 5% versus the baseline latency CSV.

### Steady-State Regression Rows

RFC 018's steady-state suite now supplies the in-repository regression rows for
longer, validated ToyKV-only runs. Keep the cross-database CRUD gate in
`crud-bench`, but run these `write-perf` rows before accepting storage-engine
changes that affect reads under update churn, WAL behavior, flush scheduling, or
compaction:

- `idle` captures baseline background work after open.
- `point_read_uniform` protects fixed-keyspace point-read throughput without a
  hot-key distribution.
- `point_read_zipfian` protects hot-key read throughput and latency without
  concurrent writes.
- `point_read_missing_in_range` protects bloom-filter and negative lookup
  behavior for absent keys distributed inside the loaded keyspace.
- `read_heavy_zipfian`, `balanced_zipfian`, and `update_heavy_zipfian` cover
  read/update churn at 95/5, 50/50, and 5/95 mixes. `balanced_zipfian` is the
  primary watch row. It starts from a prepared golden
  database, runs closed-loop clients, uses `get=0.5,put=0.5`, records p95/p99
  sampled latency, and validates completed operations plus the deterministic
  operation mix.
- `range_scan_uniform` protects bounded scan behavior and validates that each
  scan returns the expected row count.
- `sustained_ingest` protects the steady write path without golden-database
  cloning.
- `transaction_contention` protects serializable MVCC/OCC behavior over a hot
  fixed key set. It prepares a fresh serializable database instead of cloning a
  normal golden database, then reports transaction attempts, commits, and
  expected conflicts in the validation record.

Use a smoke-sized local gate while iterating:

```bash
cargo run --release --bin write-perf -- \
  --suite steady-state \
  --preset smoke \
  --prepare-golden \
  --golden-path /tmp/toykv-steady-state-golden \
  --path /tmp/toykv-steady-state-prepare \
  --settle-timeout-secs 30 \
  --output json

cargo run --release --bin write-perf -- \
  --suite steady-state \
  --preset smoke \
  --bench balanced_zipfian \
  --golden-path /tmp/toykv-steady-state-golden \
  --clone-golden \
  --path /tmp/toykv-steady-state-runs \
  --latency-sample-every 100 \
  --settle-timeout-secs 30 \
  --output json

cargo run --release --bin write-perf -- \
  --suite steady-state \
  --preset smoke \
  --bench transaction_contention \
  --path /tmp/toykv-steady-state-runs \
  --transaction-hot-set 128 \
  --transaction-reads 5 \
  --transaction-updates 5 \
  --latency-sample-every 100 \
  --settle-timeout-secs 30 \
  --output json
```

For benchmark-host gates, use the `default` or `large` preset, keep WAL enabled
unless a patch explicitly targets buffered mode, and archive the JSON rows as
the comparison artifact. Gate-valid measurement rows must include `task`,
`validation`, `drain`, `counter_snapshots`, and `golden_manifest` where
applicable. Non-idle measurement rows must also include `latency` and
`throughput`. The `idle` row is a measurement row but has zero completed
operations and does not emit latency or throughput. Prepare rows
must include `golden_manifest`, `drain`, `params.num`, `params.value_size`, and
`engine_options`, with those params and options matching the embedded manifest.
Reject rows with validation errors, zero completed operations for non-idle
measurements, missing non-idle latency samples, timed-out golden preparation, or
timed-out requested background drain if a future build adds that drain request.
The current MVP records `background_drain_status=not_requested` and includes
block-cache hit/miss/admission counters, WAL commit group/byte counters, and
value-cache hit/miss counters in each steady-state measurement's counter
snapshots. `counter_snapshots` contains absolute before/after values; top-level
`counters` is the validated saturating delta derived from those snapshots.

Before handing ToyKV rows to cross-database comparison tooling, run:

```bash
cargo run --release --bin write-perf -- \
  --validate-json /path/to/toykv-write-perf.jsonl
```

The validator accepts legacy `kv-engine.write-perf.v1` rows for compatibility
and applies the stricter steady-state checks to `kv-engine.write-perf.v2`
measurement and prepare rows, including embedded golden-manifest digest and
engine-options hash validation plus manifest binding to row params and engine
options. Runtime `--clone-golden` runs reject manifests whose `source_commit`
does not match the current build metadata from `GITHUB_SHA` or local git
metadata; unknown source metadata is not cloneable. Measurement-row validation
also binds resolved `task` metadata to the matching row params.

Priority profiling rows:

- None currently confirmed above the profiling gate. The durable
  `batch_read_100` row still records the original 100k comparison result, but
  the 10,000-iteration focused rerun supersedes it for current gating because
  the default timed row was too short and unstable.

## Reproduction

Build the narrow compare binary:

```bash
cd <crud-bench checkout>
cargo build --release --no-default-features --features fjall,rocksdb,toykv
```

Run durable 100k-sample comparisons:

```bash
cd <crud-bench checkout>

cargo run --release --no-default-features --features fjall,rocksdb,toykv -- \
  --name toykv_rocksdb_compare_toykv_sync_100k \
  --database toykv \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --color never

cargo run --release --no-default-features --features fjall,rocksdb,toykv -- \
  --name toykv_rocksdb_compare_rocksdb_sync_100k \
  --database rocksdb \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --color never

cargo run --release --no-default-features --features fjall,rocksdb,toykv -- \
  --name toykv_rocksdb_compare_fjall_sync_100k \
  --database fjall \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --color never
```

Run the 2026-08-01 focused scan rerun:

```bash
cd <crud-bench checkout>

cargo run --release --no-default-features --features "rocksdb fjall toykv toykv/bench" --bin crud-bench -- \
  --database toykv \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-batches \
  --color never

cargo run --release --no-default-features --features "rocksdb fjall toykv toykv/bench" --bin crud-bench -- \
  --name rocksdb-scan-compare \
  --database rocksdb \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-batches \
  --color never

cargo run --release --no-default-features --features "rocksdb fjall toykv toykv/bench" --bin crud-bench -- \
  --name fjall-scan-compare \
  --database fjall \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-batches \
  --color never
```

Run the focused PR #170 batch rerun. The temporary config raises only
`batch_create_100` and `batch_read_100` to 10,000 iterations so the timed
`batch_read_100` row is long enough to compare:

```bash
cd <crud-bench checkout>

cp config/bench.toml /tmp/crud-bench-batch100-iter10000.toml
perl -0pi -e 's/(name = "batch_create_100"\noperation = "CREATE"\nbatch_size = 100\niterations = )250/${1}10000/; s/(name = "batch_read_100"\noperation = "READ"\nbatch_size = 100\niterations = )250/${1}10000/' \
  /tmp/crud-bench-batch100-iter10000.toml

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name toykv_batch100_iter10000_pr170_sync_100k \
  --database toykv \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-scans \
  --config /tmp/crud-bench-batch100-iter10000.toml \
  --color never

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name rocksdb_batch100_iter10000_pr170_sync_100k \
  --database rocksdb \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-scans \
  --config /tmp/crud-bench-batch100-iter10000.toml \
  --color never
```

The release benchmark command shape was first smoke-validated with `--samples
10000 --clients 1 --threads 1 --sync`. A smaller `--samples 100` smoke is
invalid for this harness because the built-in `start(5000) limit(100)` scan
expects at least 5100 rows.
