# kv-engine

[![codecov](https://codecov.io/gh/ben1009/toy-kv-engine/graph/badge.svg?token=RGJXBL7DFV)](https://codecov.io/gh/ben1009/toy-kv-engine)
[![Test](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml/badge.svg)](https://github.com/ben1009/toy-kv-engine/actions/workflows/test.yml)

An LSM-tree based key-value storage engine in Rust.

## Features

- **LSM-tree architecture** — MemTable, immutable memtables, SSTables with block-based format
- **Key-value separation** — Value log (vLog) for large values, inline storage for small values
- **MVCC** — Multi-version concurrency control with snapshot isolation
- **Compaction strategies** — Leveled, simple-leveled, tiered, and no-compaction modes
- **WAL** — Write-ahead log for crash recovery
- **CLI** — Interactive REPL for manual testing

## Quick Start

```bash
# Run with leveled compaction (default)
cargo run --bin kv-engine-cli -- --path lsm.db

# Choose compaction strategy
cargo run --bin kv-engine-cli -- --compaction tiered --path lsm.db

# Enable WAL for crash recovery
cargo run --bin kv-engine-cli -- --enable-wal --path lsm.db
```

### CLI Usage

```
put <key> <value>    Store a key-value pair
get <key>            Retrieve value by key
delete <key>         Remove a key
scan <start> <end>   Range scan
```

## API

```rust
use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};

let engine = KvEngine::open("my-db", LsmStorageOptions::default())?;

engine.put(b"key", b"value")?;
let value = engine.get(b"key")?;      // Some(Bytes)
engine.delete(b"key")?;

let iter = engine.scan(
    std::ops::Bound::Included(b"start"),
    std::ops::Bound::Excluded(b"end"),
)?;

engine.close()?;
```

## Testing

```bash
cargo test --lib --tests          # All tests (96)
cargo test --lib --tests --all-features  # With vLog enabled
cargo bench                       # vLog benchmarks
```

## Architecture

```
kv-engine/src/
├── lsm_storage.rs      # KvEngine, LsmStorageInner, state machine
├── mem_table.rs        # Skip-list based MemTable
├── block.rs / table.rs # SSTable block format and reader/writer
├── compact.rs          # Compaction trigger and execution
│   └── compact/        # Strategy implementations
├── iterators/          # Merge, concat, two-merge iterators
├── mvcc/               # Transaction manager, watermark
├── manifest.rs         # SST/compaction metadata persistence
├── wal.rs              # Write-ahead log
├── key.rs              # Key types with optional timestamp suffix
└── bin/
    ├── kv-engine-cli.rs        # Interactive REPL
    └── compaction-simulator.rs # Compaction strategy simulator
```

## RFCs

- [001-key-value-separation](rfcs/001-key-value-separation.md) — Design for vLog-based KV separation

## Development

See [AGENTS.md](AGENTS.md) for project conventions, architecture details, and workflow guidelines.
