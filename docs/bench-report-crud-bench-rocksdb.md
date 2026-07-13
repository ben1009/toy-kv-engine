# crud-bench: ToyKV vs RocksDB

This report tracks ToyKV against the embedded RocksDB backend in a sibling
`crud-bench` checkout.

## Summary

Run date: 2026-07-13.

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

ToyKV is ahead of RocksDB on point reads and durable batch writes, including
large batch create/update/delete rows. RocksDB is ahead on all measured scan
rows and on `batch_read_100`. The next useful work is therefore not single-write
optimization; it is profiling the scan and small batch-read paths where RocksDB
wins by more than the 10% gate.

Artifacts:

- `result-toykv_rocksdb_compare_toykv_sync_100k.{csv,json,html}`
- `result-toykv_rocksdb_compare_rocksdb_sync_100k.{csv,json,html}`
- `result-toykv_rocksdb_compare_fjall_sync_100k.{csv,json,html}`

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

## Scan Rows

All scan rows are read-only, no-index rows from the same 100k run.

| Row | ToyKV | RocksDB | Fjall | Result |
|---|---:|---:|---:|---|
| count() | 318 | **401** | 107 | RocksDB +26.1% |
| select(id) limit(100) | 318,606 | **501,120** | 106,820 | RocksDB +57.3% |
| select(*) limit(100) | 308,630 | **554,986** | 86,801 | RocksDB +79.8% |
| select(id) start(5000) limit(100) | 9,656 | **12,144** | 2,105 | RocksDB +25.8% |
| select(*) start(5000) limit(100) | 9,667 | **12,050** | 1,913 | RocksDB +24.7% |

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

The next implementation target is scan and small batch-read performance:

- `batch_read_100`: RocksDB +33.4%.
- `count()`: RocksDB +26.1%.
- `select(id) limit(100)`: RocksDB +57.3%.
- `select(*) limit(100)`: RocksDB +79.8%.
- `start(5000) limit(100)`: RocksDB +24.7%-25.8%.

Profile those exact rows before changing storage code. The likely areas to
inspect are iterator layering, block cache hit/miss behavior, vLog dereference
cost, cache admission on scans, and block/value prefetch.

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

Priority profiling rows:

- `batch_read_100`
- `count()`
- `select(id) limit(100)`
- `select(*) limit(100)`
- `select(id) start(5000) limit(100)`
- `select(*) start(5000) limit(100)`

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

The release benchmark command shape was first smoke-validated with `--samples
10000 --clients 1 --threads 1 --sync`. A smaller `--samples 100` smoke is
invalid for this harness because the built-in `start(5000) limit(100)` scan
expects at least 5100 rows.
