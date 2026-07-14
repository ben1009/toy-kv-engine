<p align="center">
  <img src="docs/assets/toykv-logo.png" alt="toy-kv-engine logo" width="180">
  <br>
  <sub>Logo artwork is a cartoonized interpretation of <a href="https://en.wikipedia.org/wiki/Diaoshuilou_Falls">Diaoshuilou Falls</a>.</sub>
</p>

# toy-kv-engine

[![Test](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ben1009/toy-kv-engine/graph/badge.svg)](https://codecov.io/gh/ben1009/toy-kv-engine)

`toy-kv-engine` is a small Rust LSM-tree key-value engine built for learning and
experimentation. The codebase is intentionally readable, but it also exercises
many production storage-system ideas: WAL durability, MVCC, compaction,
value-log separation, cache admission, TTL, range tombstones, async APIs, and
parallel scan.

It is not a production database. It is a compact place to study how these pieces
fit together and to benchmark tradeoffs against engines such as Fjall.

## Quick Start

```bash
# Build the workspace
cargo build --workspace --all-features

# Install the preferred test runner once
cargo make install-nextest

# Run the default local test suite
cargo make test

# Run the full local gate: fmt, dep order, clippy, tests, typos
cargo make check

# Run process-level crash/failpoint chaos tests
cargo make test-chaos

# Start the interactive CLI
cargo run --bin kv-engine-cli -- --path /tmp/lsm.db --compaction leveled
```

The CLI supports basic manual operations such as `fill`, `get`, `del`, `scan`,
`dump`, `flush`, `full_compaction`, and `quit`.

## What It Implements

### Storage Core

- LSM-tree storage with active and immutable memtables, L0 overlap, and L1+
  non-overlapping levels.
- SSTables with block indexes, Bloom filters, optional prefix Bloom filters,
  checksums, and block cache integration.
- Pluggable compaction policy: no compaction, simple leveled, leveled, and
  tiered compaction.
- Copy-on-write state publication through `ArcSwap`, so reads take lock-free
  snapshots while flush and compaction install new versions.

### Durability And Writes

- Write-ahead logging with ticket-based group commit.
- `io_uring` + `O_DIRECT` WAL write path for durable writes.
- Batched writes through `write_batch`, including optimized same-batch publish
  and WAL grouping.
- Manifest recovery for SST and value-log metadata.

### Reads, Transactions, And Deletion

- Point reads, batch reads, range scans, prefix scans, and async scan cursors.
- MVCC snapshot isolation with optional serializable transactions using OCC.
- Watermark-based MVCC garbage collection.
- Point tombstones, range tombstones, and TTL-aware read/scan/compaction
  filtering.
- Compaction filters with manifest persistence.

### Caching And Value Separation

- Lock-free TinyUFO block cache with cache backfill on flush and compaction.
- Configurable cache admission for parallel scans.
- WiscKey-style value separation for large values through `.vlog` files.
- Per-file `.vidx` indexes for value-log GC liveness analysis.
- Weighted TinyUFO value cache for separated values.

### Async And Parallel Scan

- Async wrappers for open, close, get, batch_get, put, delete, delete_range,
  write_batch, sync, scan, prefix_scan, flush, compaction, and transactions.
- Engine-owned `BlockingExecutor` for cancellation-safe async wrappers around
  the synchronous engine.
- Chunk-first parallel async scan with shard planning, concurrent worker drain,
  `try_next_chunk`, `try_next_batch`, and execution stats.

## API Surface

Primary entry points live in `kv-engine/src/lsm_storage.rs`.

```rust
use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};

let db = KvEngine::open(path, LsmStorageOptions::default_for_test())?;

db.put(b"user:1", b"alice")?;
let value = db.get(b"user:1")?;

let mut iter = db.prefix_scan(b"user:")?;
while iter.is_valid() {
    println!("{:?} = {:?}", iter.key(), iter.value());
    iter.next()?;
}

db.close()?;
```

Async callers can use the corresponding async methods:

```rust
use kv_engine::lsm_storage::{KvEngine, ParallelScanOptions};

let db = KvEngine::open_async(path, options).await?;
db.put_async(b"user:1", b"alice").await?;

let mut scan = db.prefix_scan_parallel_async(b"user:", ParallelScanOptions::default()).await?;
while let Some(chunk) = scan.try_next_chunk().await? {
    for (key, value) in chunk.into_rows() {
        println!("{key:?} = {value:?}");
    }
}

db.close_async().await?;
```

## Architecture

```text
                        ┌─────────────────────────────┐
                        │       Public API Surface    │
                        │ sync:  open/get/put/scan    │
                        │ async: *_async + parallel   │
                        └──────────────┬──────────────┘
                                       │
                        ┌──────────────▼──────────────┐
                        │      BlockingExecutor       │
                        │   engine-owned sync->async  │
                        └──────────────┬──────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
┌───────▼────────┐            ┌────────▼────────┐            ┌────────▼────────┐
│   Write Path   │            │    Read Path    │            │  Parallel Scan  │
│                │            │                 │            │                 │
│ put ──► WAL ───┼──────┐     │ get ──► active  ├──────┐     │ plan_shards()   │
│   │   io_uring │      │     │   │      │      │      │     │   │             │
│   │ + O_DIRECT │      │     │   │   imm mems  │      │     │   ▼             │
│   ▼            │      │     │   │      │      │      │     │ ┌──┐ ┌──┐ ┌──┐  │
│ MemTable       │      │     │   │    concat   │      │     │ │W0│ │W1│ │W2│  │
│   │            │      │     │   ▼      │      │      │     │ └──┘ └──┘ └──┘  │
│   ▼            │      │     │ Bloom   Block   │      │     │   │    │    │    │
│ Immutable      │      │     │ Filter  Cache   │      │     │   ▼    ▼    ▼    │
│ MemTables      │      │     │   │      │      │      │     │ concurrent drain │
│   │            │      │     │   │   TinyUFO   │      │     │  try_next_chunk  │
│   ▼            │      │     │   │ + admission │      │     └──────────────────┘
│ Flush ──► SST ─┼──────┼────►│   ▼      ▼      │
│   │      │     │      │     │ vLog   SSTables │
│   │      └────►│ vLog │◄────┘ value sep       │
│   ▼            │      │
│ Compact ───────┼──────┘
│   │            │
│   └──── state update ──────────────────────────────────────┐
└───┼─────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────┐
│                    LSM Storage                       │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────┐ │
│  │ MemTable │  │ L0 SSTs  │  │ L1+ Levels         │ │
│  │ active   │  │ overlap  │  │ non-overlap        │ │
│  └──────────┘  └──────────┘  └────────────────────┘ │
│  ┌────────────┐ ┌──────────┐  ┌──────────────────┐ │
│  │ Immutable  │ │ Manifest │  │ vLog + .vidx     │ │
│  │ MemTables  │ │          │  │ value cache      │ │
│  └────────────┘ └──────────┘  └──────────────────┘ │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │ MVCC Layer                                     │  │
│  │ snapshot isolation · OCC transactions          │  │
│  │ watermark GC · range tombstones · TTL          │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

## Repository Map

- `kv-engine/src/lsm_storage.rs` - core engine API, state management, reads,
  writes, scans, async wrappers, and parallel scan.
- `kv-engine/src/wal.rs` - WAL, group commit, and `io_uring` durable writes.
- `kv-engine/src/mem_table.rs` - lock-free skip-list memtable.
- `kv-engine/src/block.rs`, `kv-engine/src/table.rs` - SST block and table
  formats.
- `kv-engine/src/compact.rs`, `kv-engine/src/compact/` - compaction
  orchestration and policies.
- `kv-engine/src/mvcc.rs`, `kv-engine/src/mvcc/txn.rs` - timestamps,
  watermarks, snapshots, and transactions.
- `kv-engine/src/vlog/` - value-log writer, reader, GC, and `.vidx` index.
- `kv-engine/src/cache.rs` - block cache and admission policy.
- `kv-engine/src/bin/` - CLI, write benchmark, async scan benchmark,
  compaction simulator, and chaos child process.
- `kv-engine/tests/` - process-level chaos and cross-process persistence tests.
- `kv-engine/benches/` - Criterion benchmarks for vLog, WAL, memtable, and
  range deletion paths.
- `rfcs/` - design notes for major features.
- `docs/` - benchmark reports and focused performance notes.

## Testing And Benchmarks

```bash
# Library tests
cargo nextest run --workspace --all-features --lib

# All tests and targets
cargo nextest run --workspace --all-features --all-targets

# Process-level crash/failpoint chaos tests
cargo make test-chaos

# Coverage report
cargo make test-cov

# Example Criterion benchmarks
cargo bench --package kv-engine --bench wal_bench
cargo bench --package kv-engine --bench vlog_benchmarks
```

The repo also carries a `write-perf` binary and an `async-phase3-perf` binary for
scenario-driven performance checks.

`cargo make check` runs the normal local gate and does not include the dedicated
chaos harness. Run `cargo make test-chaos` when validating crash recovery,
failpoint injection, and cross-process persistence behavior.

## Performance Notes

The current benchmark reports compare ToyKV with Fjall and RocksDB through
`crud-bench` using roughly matched adapter settings. In the latest 2026-07-13
durable Fjall comparison, ToyKV wins 16 of 17 full-run rows; a focused
`batch_read_100` rerun puts ToyKV ahead by about 13.5% after seeding the batch
workload correctly.

The 2026-07-13 durable RocksDB comparison shows ToyKV ahead on point reads and
large durable batch writes. A 2026-07-14 focused PR #170 scan rerun moved ToyKV
ahead on four of five scan rows. The latest focused batch rerun keeps
`batch_read_1000` ahead of RocksDB, but confirms `batch_read_100` as a remaining
gap. The remaining focused read gaps are `select(*) limit(100)`, where RocksDB
leads by 3.8%, and `batch_read_100`.

See [ToyKV vs Fjall Benchmark Report](docs/bench-report-crud-bench-fjall.md)
for the full numbers, caveats, and artifact names. See
[ToyKV vs RocksDB Benchmark Report](docs/bench-report-crud-bench-rocksdb.md) for
the RocksDB comparison numbers, parity notes, gates, and next target.

## Docs And RFCs

### Reports

- [ToyKV vs Fjall Benchmark Report](docs/bench-report-crud-bench-fjall.md)
- [ToyKV vs RocksDB Benchmark Report](docs/bench-report-crud-bench-rocksdb.md)
- [vLog Benchmark Report](docs/bench-report-vlog.md)
- [DeleteRange Benchmark Report](docs/bench-report-deleterange.md)
- [io_uring Benchmark Notes](docs/io-uring-bench.md)
- [Performance Profiling Report](docs/perf-profile.md)
- [Async Scan Findings](docs/async-scan-findings.md)
- [Parallel Scan Findings](docs/parallel-scan-findings.md)
- [Async Phase 3 Measurement Plan](docs/async-phase3-measurement.md)

### RFCs

- [001: Key-Value Separation](rfcs/001-key-value-separation.md)
- [004: Cache Backfill](rfcs/004-cache-backfill.md)
- [005: MVCC](rfcs/005-mvcc.md)
- [006: Prefix Search](rfcs/006-prefix-search.md)
- [007: Prefix Bloom Filter](rfcs/007-prefix-bloom-filter.md)
- [009: Compaction Filter](rfcs/009-compaction-filter.md)
- [010: Range Tombstones](rfcs/010-delete-range.md)
- [011: db_bench Harness](rfcs/011-db-bench-harness.md)
- [012: Parallel WAL](rfcs/012-parallel-wal.md)
- [013: Chaos Testing](rfcs/013-chaos-testing.md)
- [014: Async Operations](rfcs/014-async-operations.md)
- [015: Parallel Scan](rfcs/015-parallel-scan.md)
- [016: TTL](rfcs/016-ttl.md)
- [017: MVCC Garbage Collection](rfcs/017-mvcc-garbage-collection.md)

## License

See [LICENSE](LICENSE).
