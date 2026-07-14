# crud-bench: ToyKV vs RocksDB

This report tracks ToyKV against the embedded RocksDB backend in a sibling
`crud-bench` checkout.

## Summary

Run date: 2026-07-13 full durable run. Focused PR #170 scan and batch reruns:
2026-07-14.

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
large batch create/update/delete rows. The PR #170 focused scan rerun flips four
of the five previously RocksDB-winning scan rows: ToyKV now leads `count()`,
`select(id) limit(100)`, and both `start(5000) limit(100)` scan rows. RocksDB
still leads `select(*) limit(100)` by 3.8% over ToyKV, and `batch_read_100`
now favors ToyKV in the focused batch-only rerun.

Artifacts:

- `result-toykv_rocksdb_compare_toykv_sync_100k.{csv,json,html}`
- `result-toykv_rocksdb_compare_rocksdb_sync_100k.{csv,json,html}`
- `result-toykv_rocksdb_compare_fjall_sync_100k.{csv,json,html}`
- `result-toykv_read_rerun_pr170_sync_100k.{csv,json,html}`
- `result-rocksdb_read_rerun_pr170_sync_100k.{csv,json,html}`
- `result-toykv_batch_push_output_sync_100k.{csv,json,html}`
- `result-rocksdb_batch_compare_sync_100k.{csv,json,html}`

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

These rows come from the 2026-07-14 focused PR #170 scan rerun on head
`95c0811`. The command used `--skip-indexes --skip-batches` to isolate the
read-only no-index scan rows after the scan iterator fast paths landed. Fjall
was not rerun in this focused pass.

| Row | ToyKV | RocksDB | Result |
|---|---:|---:|---|
| count() | **626.90** | 378.79 | ToyKV +65.5% |
| select(id) limit(100) | **518,545.76** | 487,420.34 | ToyKV +6.4% |
| select(*) limit(100) | 544,404.19 | **564,861.78** | RocksDB +3.8% |
| select(id) start(5000) limit(100) | **14,860.10** | 12,218.91 | ToyKV +21.6% |
| select(*) start(5000) limit(100) | **14,914.80** | 12,308.01 | ToyKV +21.2% |

The 2026-07-13 full durable run had RocksDB ahead on all five scan rows before
the PR #170 scan work. Keep the full-run artifacts for historical comparison,
but use this focused rerun as the current scan baseline.

## Focused Batch Rerun

These rows come from the 2026-07-14 focused PR #170 batch rerun after the
small-batch `batch_get` output construction change. The command used
`--skip-indexes --skip-scans` to isolate batch workloads.

| Row | ToyKV | RocksDB | Result |
|---|---:|---:|---|
| batch_read_100 | **50,390.36** | 29,952.76 | ToyKV +68.2% |
| batch_read_1000 | **6,059.45** | 4,906.63 | ToyKV +23.5% |

The 2026-07-13 full durable run had RocksDB ahead on `batch_read_100`. The
focused rerun is the current read-batch baseline after the PR #170 batch-get
small-path update.

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

The remaining read-path target is narrow after PR #170:

- `select(*) limit(100)`: RocksDB +3.8% in the focused scan rerun.

Repeat `select(*) limit(100)` before deeper work because its current gap is
below the 10% gate. If chasing that last scan row, inspect
projection/materialization and value decode cost before broad iterator setup
changes. Keep `batch_read_100`, `batch_read_1000`, `count()`,
`select(id) limit(100)`, and the two `start(5000) limit(100)` rows as regression
watch rows because ToyKV now leads them.

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

- `select(*) limit(100)` after a repeat run confirms a stable gap

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

Run the focused PR #170 scan rerun:

```bash
cd <crud-bench checkout>

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name toykv_read_rerun_pr170_sync_100k \
  --database toykv \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-batches \
  --color never

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name rocksdb_read_rerun_pr170_sync_100k \
  --database rocksdb \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-batches \
  --color never
```

Run the focused PR #170 batch rerun:

```bash
cd <crud-bench checkout>

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name toykv_batch_push_output_sync_100k \
  --database toykv \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-scans \
  --color never

cargo run --release --no-default-features --features rocksdb,toykv -- \
  --name rocksdb_batch_compare_sync_100k \
  --database rocksdb \
  --samples 100000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --skip-indexes \
  --skip-scans \
  --color never
```

The release benchmark command shape was first smoke-validated with `--samples
10000 --clients 1 --threads 1 --sync`. A smaller `--samples 100` smoke is
invalid for this harness because the built-in `start(5000) limit(100)` scan
expects at least 5100 rows.
