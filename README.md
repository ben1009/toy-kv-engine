# toy-kv-engine

[![Test](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ben1009/toy-kv-engine/graph/badge.svg)](https://codecov.io/gh/ben1009/toy-kv-engine)

A toy LSM-tree based key-value storage engine written in Rust. This is an educational yet functional implementation that explores production-grade storage concepts including MVCC, WAL, multiple compaction strategies, and key-value separation (vLog).

## Features

- **LSM-Tree Architecture**: Log-structured merge tree with tiered memory and disk layers
- **MVCC**: Multi-version concurrency control with timestamp allocation and watermark tracking
- **WAL**: Optional write-ahead logging for crash recovery
- **Compaction Strategies**: Simple leveled, leveled, and tiered compaction
- **Key-Value Separation**: WiscKey-style vLog for large values to reduce write amplification
- **Block Cache**: Configurable caching with `moka`
- **Bloom Filters**: Space-efficient key membership tests

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

```
├── MemTable (crossbeam-skiplist)
├── Immutable MemTables (pending flush)
├── L0 SSTables
├── L1+ Levels/Tiers
├── WAL (optional)
├── Manifest
└── vLog (optional, for key-value separation)
```

## Project Structure

- `src/lsm_storage.rs` — Core engine state and operations
- `src/mem_table.rs` — Lock-free skip-list memtable
- `src/block.rs`, `src/table.rs` — SST block and table formats
- `src/compact.rs` — Compaction orchestration
- `src/mvcc.rs` — MVCC transaction support
- `src/wal.rs` — Write-ahead log
- `src/vlog/` — Key-value separation
- `src/bin/` — CLI and compaction simulator

## Documentation

- [vLog Benchmark Report](docs/bench-report-vlog.md)
- [Key-Value Separation RFC](rfcs/001-key-value-separation.md)

## License

See [LICENSE](LICENSE).
