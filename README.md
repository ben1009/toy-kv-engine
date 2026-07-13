# toy-kv-engine

[![Test](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ben1009/toy-kv-engine/graph/badge.svg)](https://codecov.io/gh/ben1009/toy-kv-engine)

A toy LSM-tree-based key-value storage engine written in Rust. It stays small
enough to read end to end, but the current codebase also carries MVCC, TTL,
range tombstones, value separation, async wrappers, and parallel scan.

## Features

- **LSM-Tree Architecture**: Log-structured merge tree with tiered memory and disk layers
- **MVCC**: Multi-version concurrency control with snapshot isolation and serializable transactions (OCC)
- **WAL**: io_uring + O_DIRECT write-ahead logging with ticket-based group commit and parallel write submission
- **Compaction Strategies**: Simple leveled, leveled, and tiered compaction
- **Key-Value Separation**: WiscKey-style vLog for large values to reduce write amplification
- **Block Cache**: Lock-free `TinyUFO` (S3-FIFO + TinyLFU) with cache backfill on flush and compaction
- **Value Cache**: TinyUFO-based weighted cache for vLog values (configurable, default 64MB)
- **vLog Index**: Per-file `.vidx` companion files for GC liveness optimization
- **Bloom Filters**: ahash-based (AES-NI accelerated) key membership tests
- **Prefix Bloom Filters**: Per-SST prefix Bloom filters for prefix scan pruning
- **Prefix Search**: `prefix_scan` API with prefix-aware iterator and Bloom filter integration
- **Range Tombstones**: `DeleteRange` for efficient bulk deletion with O(log F) fragment cache
- **Compaction Filters**: Custom per-key drop predicates during compaction with manifest persistence
- **TTL**: `put_with_ttl` write path, TTL-aware reads/scans, compaction expiry, and bottom-level wholesale TTL-only SST drop
- **Structured Logging**: `logforth` JSON logging on stderr, configurable via `RUST_LOG`
- **Chaos Testing**: Deterministic seeded stress harness with process-level crash/recovery, failpoint injection, and full key-universe reconciliation oracle — all gated behind `chaos-testing` feature
- **Async Operations**: Async open/close/read/write/scan wrappers with an engine-owned blocking executor and cancellation-safe scan cursors
- **Parallel Scan**: Chunk-first parallel async scan with shard planning, concurrent coordinator drain, configurable block-cache admission (Force/Admit/Bypass), and `posix_fadvise` readahead

## Quick Start

```bash
# Build
cargo build --workspace --all-features

# Install the preferred test runner once
cargo make install-nextest

# Run the default local test suite
cargo make test

# Run the interactive CLI
cargo run --bin kv-engine-cli -- --path /tmp/lsm.db --compaction leveled
```

For the full local gate, run `cargo make check`.

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
                        │   (engine-owned sync→async) │
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
│   │            │      │     │   │   TinyUFO   │      │     │   (try_next_     │
│   ▼            │      │     │   │ + admission │      │     │      chunk)      │
│ Flush ──► SST ─┼──────┼────►│   ▼      ▼      │      │     └──────────────────┘
│   │      │     │      │     │ vLog   SSTables │      │
│   │      └────►│ vLog │◄────┘ (value sep)     │◄─────┘
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
│  │ (active) │  │ overlap  │  │ non-overlap        │ │
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

## Project Structure

- `kv-engine/src/lsm_storage.rs` — Core engine state and operations
- `kv-engine/src/mem_table.rs` — Lock-free skip-list memtable with bloom filter
- `kv-engine/src/block.rs`, `kv-engine/src/table.rs` — SST block and table formats
- `kv-engine/src/compact.rs` — Compaction orchestration
- `kv-engine/src/mvcc.rs` — MVCC internals (timestamp, watermark, committed txns)
- `kv-engine/src/mvcc/txn.rs` — Transaction API with snapshot isolation and serializable OCC
- `kv-engine/src/wal.rs` — Write-ahead log with io_uring + O_DIRECT, ticket-based group commit
- `kv-engine/src/vlog/` — Key-value separation (builder, reader, GC, index)
- `kv-engine/src/cache.rs` — Block cache (TinyUFO) with configurable admission policy
- `kv-engine/src/manifest.rs` — SST/vLog manifest tracking
- `kv-engine/src/lsm_iterator.rs` — Ordered LSM iterator with MVCC tombstone filtering
- `kv-engine/src/blocking_executor.rs` — Engine-owned sync-to-async bridge used by the async API
- `kv-engine/src/scan_trace.rs` — Lightweight per-thread block-load and SST-switch counters
- `kv-engine/src/future_ext.rs` — Compatibility shim for async runtimes
- `kv-engine/src/bin/` — CLI, `write-perf`, `async-phase3-perf`, compaction simulator, and chaos-testing helpers

## Documentation

- [ToyKV vs Fjall Benchmark Report](docs/bench-report-crud-bench-fjall.md)
- [vLog Benchmark Report](docs/bench-report-vlog.md)
- [DeleteRange Benchmark Report](docs/bench-report-deleterange.md)
- [io_uring Benchmark Notes](docs/io-uring-bench.md)
- [Performance Profiling Report](docs/perf-profile.md)
- [Async Scan Findings](docs/async-scan-findings.md)
- [Async Phase 3 Measurement Plan](docs/async-phase3-measurement.md)
- [Key-Value Separation RFC](rfcs/001-key-value-separation.md)
- [Cache Backfill RFC](rfcs/004-cache-backfill.md)
- [MVCC RFC](rfcs/005-mvcc.md)
- [Prefix Search RFC](rfcs/006-prefix-search.md)
- [Prefix Bloom Filter RFC](rfcs/007-prefix-bloom-filter.md)
- [Compaction Filter RFC](rfcs/009-compaction-filter.md)
- [Range Tombstones RFC](rfcs/010-delete-range.md)
- [Parallel WAL RFC](rfcs/012-parallel-wal.md)
- [Chaos Testing RFC](rfcs/013-chaos-testing.md)
- [Async Operations RFC](rfcs/014-async-operations.md)
- [Parallel Scan RFC](rfcs/015-parallel-scan.md)
- [TTL RFC](rfcs/016-ttl.md)
- [MVCC GC RFC](rfcs/017-mvcc-garbage-collection.md)
- [Parallel Scan Findings](docs/parallel-scan-findings.md)

## License

See [LICENSE](LICENSE).
