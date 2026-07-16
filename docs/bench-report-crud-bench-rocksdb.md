# crud-bench: ToyKV vs RocksDB

This report tracks ToyKV against the embedded RocksDB backend in a sibling
`crud-bench` checkout.

## Summary

Run date: 2026-07-16 latest three-way durable rerun. The historical full
durable run is 2026-07-13, with focused scan and batch reruns from 2026-07-14
to 2026-07-15.

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

RocksDB backend version: `surrealdb-rocksdb 0.24.0-surreal.5` maps to
`surrealdb-librocksdb-sys 0.18.3+11.0.0-4`, whose vendored
`rocksdb/version.h` reports raw RocksDB `11.0.0`. The latest upstream raw
RocksDB release checked on 2026-07-16 is `11.1.2`, so these results are against
the latest available SurrealDB Rust binding but not the latest upstream RocksDB
source.

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
remaining `select(*) limit(100)` gap and now puts ToyKV ahead on all five
focused no-index scan watch rows. The repeated `select(*) limit(100)` row is
646,130.69 OPS for ToyKV vs 503,709.26 OPS for RocksDB, a 28.3% ToyKV lead. The
focused `batch_read_100` row changed substantially across the default
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

These rows come from the 2026-07-15 focused PR #173 scan rerun on head
`4bd002d`. The command used `--skip-indexes --skip-batches` to isolate the
read-only no-index scan rows after the scan iterator fast paths landed and the
remaining full-projection limit row was repeated. Fjall was not rerun in this
focused pass.

| Row | ToyKV | RocksDB | Result |
|---|---:|---:|---|
| count() | **639.73** | 422.17 | ToyKV +51.5% |
| select(id) limit(100) | **531,176.56** | 531,124.54 | ToyKV +0.01%, tie |
| select(*) limit(100) | **646,130.69** | 503,709.26 | ToyKV +28.3% |
| select(id) start(5000) limit(100) | **15,064.87** | 12,260.85 | ToyKV +22.9% |
| select(*) start(5000) limit(100) | **14,969.44** | 12,209.65 | ToyKV +22.6% |

The 2026-07-13 full durable run had RocksDB ahead on all five scan rows before
the PR #170 and PR #173 scan work. Keep the full-run artifacts for historical
comparison, but use this focused rerun as the current scan baseline.

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

The focused read-path gap is closed after PR #173:

- `select(*) limit(100)`: ToyKV +28.3% in the focused scan rerun.

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

The repeatable gate checker lives in the `crud-bench` checkout:

```bash
cd <crud-bench checkout>
cargo run --bin perf-gate -- \
  --baseline-sync <previous-toykv-sync.csv> \
  --current-sync <current-toykv-sync.csv> \
  --baseline-nosync <previous-toykv-nosync.csv> \
  --current-nosync <current-toykv-nosync.csv> \
  --fjall-sync <current-fjall-sync.csv>
```

The default rows are `put_c`, `batch_create_100`, `batch_create_1000`,
`batch_delete_100`, and `batch_delete_1000`. The default sync/no-sync ratio
gate requires improvement on at least two of `put_c`, `batch_create_1000`, and
`batch_delete_1000`. Add `--baseline-latency-sync` and
`--current-latency-sync` with single-client sync CSVs to enforce the p95/p99
latency gate.

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

Run the focused PR #173 scan rerun:

```bash
cd <crud-bench checkout>

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name toykv_read_rerun_pr173_sync_100k \
  --database toykv \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-batches \
  --color never

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name rocksdb_read_rerun_pr173_sync_100k \
  --database rocksdb \
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
