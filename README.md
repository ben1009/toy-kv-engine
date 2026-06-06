# toy-kv-engine

[![Test](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ben1009/toy-kv-engine/graph/badge.svg)](https://codecov.io/gh/ben1009/toy-kv-engine)

A toy LSM-tree-based key-value storage engine written in Rust. This is an educational yet functional implementation that explores production-grade storage concepts including MVCC, WAL, multiple compaction strategies, and key-value separation (vLog).

## Features

- **LSM-Tree Architecture**: Log-structured merge tree with tiered memory and disk layers
- **MVCC**: Multi-version concurrency control with timestamp allocation and watermark tracking
- **WAL**: Optional write-ahead logging for crash recovery
- **Compaction Strategies**: Simple leveled, leveled, and tiered compaction
- **Key-Value Separation**: WiscKey-style vLog for large values to reduce write amplification
- **Block Cache**: Lock-free `TinyUFO` (S3-FIFO + TinyLFU) with cache backfill on flush and compaction
- **Value Cache**: Dedicated weighted LRU cache for vLog values (configurable, default 64MB)
- **vLog Index**: Per-file `.vidx` companion files for GC liveness optimization
- **Bloom Filters**: ahash-based (AES-NI accelerated) key membership tests

## Quick Start

```bash
# Build
cargo build --workspace --all-features

# Run tests
cargo nextest run --workspace --all-features --all-targets

# Run the interactive CLI
cargo run --bin kv-engine-cli -- --path /tmp/lsm.db --compaction leveled
```

## Architecture

```text
├── MemTable (crossbeam-skiplist)
├── Immutable MemTables (pending flush)
├── L0 SSTables
├── L1+ Levels/Tiers
├── WAL (optional)
├── Manifest
└── vLog (optional, for key-value separation)
```

## Project Structure

- `kv-engine/src/lsm_storage.rs` — Core engine state and operations
- `kv-engine/src/mem_table.rs` — Lock-free skip-list memtable with bloom filter
- `kv-engine/src/block.rs`, `kv-engine/src/table.rs` — SST block and table formats
- `kv-engine/src/compact.rs` — Compaction orchestration
- `kv-engine/src/mvcc.rs` — MVCC transaction support
- `kv-engine/src/wal.rs` — Write-ahead log
- `kv-engine/src/vlog/` — Key-value separation (builder, reader, GC, index)
- `kv-engine/src/cache.rs` — Block cache (TinyUFO)
- `kv-engine/src/manifest.rs` — SST/vLog manifest tracking
- `kv-engine/src/bin/` — CLI, compaction simulator, and benchmark binary

## Documentation

- [vLog Benchmark Report](docs/bench-report-vlog.md)
- [Performance Profiling Report](docs/perf-profile.md)
- [Key-Value Separation RFC](rfcs/001-key-value-separation.md)
- [Cache Backfill RFC](rfcs/004-cache-backfill.md)

## License

See [LICENSE](LICENSE).
