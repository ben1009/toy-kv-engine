# toy-kv-engine

A toy LSM-tree-based key-value storage engine written in Rust. This is an educational yet functional implementation that explores production-grade storage concepts including MVCC, WAL, multiple compaction strategies, and key-value separation (vLog).

## Technology Stack

- **Language**: Rust (Edition 2024)
- **Toolchain**: Nightly (`nightly-2026-05-28`), managed via `rust-toolchain` file
- **Build Tool**: Cargo + cargo-make (`Makefile.toml`)
- **Test Runner**: cargo-nextest
- **Coverage**: cargo-llvm-cov

Key dependencies:
- `crossbeam-skiplist` — lock-free memtable
- `parking_lot` — synchronization primitives
- `moka` — block cache and vLog reader cache
- `bytes` — zero-copy byte buffers
- `crc32fast` — checksums
- `farmhash` — bloom filter hashing
- `nom` — CLI parser combinators
- `ouroboros` — self-referencing structs
- `criterion` — benchmarking

## Project Structure

```text
├── Cargo.toml              # Workspace root (single member: kv-engine)
├── rust-toolchain          # Pins nightly toolchain
├── rustfmt.toml            # Formatting configuration
├── Makefile.toml           # cargo-make tasks for dev workflow
├── .config/nextest.toml    # nextest profile (retries, timeouts)
├── .typos.toml             # Spell-check allowlist
├── lsan-suppressions.txt   # LeakSanitizer suppressions
├── docs/
│   └── bench-report-vlog.md
├── rfcs/
│   └── 001-key-value-separation.md
└── kv-engine/
    ├── Cargo.toml
    ├── README.md
    ├── benches/
    │   └── vlog_benchmarks.rs
    └── src/
        ├── lib.rs                     # Module declarations + test modules
        ├── bin/
        │   ├── kv-engine-cli.rs       # Interactive REPL CLI
        │   ├── compaction-simulator.rs
        │   └── wrapper.rs
        ├── block.rs                   # SST block format
        ├── block/
        │   ├── builder.rs
        │   └── iterator.rs
        ├── table.rs                   # SSTable format
        ├── table/
        │   ├── builder.rs
        │   ├── iterator.rs
        │   └── bloom.rs
        ├── mem_table.rs               # In-memory skip-list memtable
        ├── lsm_storage.rs             # Core LSM engine (state, flush, get, put, scan)
        ├── lsm_iterator.rs            # Full-LSM iterator
        ├── iterators.rs               # Iterator trait definitions
        ├── iterators/
        │   ├── merge_iterator.rs
        │   ├── two_merge_iterator.rs
        │   └── concat_iterator.rs
        ├── compact.rs                 # Compaction orchestration
        ├── compact/
        │   ├── simple_leveled.rs
        │   ├── leveled.rs
        │   └── tiered.rs
        ├── wal.rs                     # Write-ahead log
        ├── manifest.rs                # SST/vLog manifest tracking
        ├── mvcc.rs                    # MVCC internals
        ├── mvcc/
        │   ├── txn.rs
        │   └── watermark.rs
        ├── key.rs                     # Key types and helpers
        ├── vlog/                      # Key-value separation (WiscKey-style)
        │   ├── mod.rs
        │   ├── builder.rs
        │   ├── reader.rs
        │   └── gc.rs
        ├── debug.rs
        └── tests/                     # Integration tests
            ├── block.rs
            ├── bloom_compression.rs
            ├── compaction.rs
            ├── compaction_integration.rs
            ├── compaction_integration_2.rs
            ├── harness.rs
            ├── iterators.rs
            ├── leveled_compaction.rs
            ├── memtable.rs
            ├── merge_iterator.rs
            ├── scan_flush.rs
            ├── simple_leveled_compaction.rs
            ├── sst.rs
            ├── tiered_compaction.rs
            └── vlog_integration_tests/
                ├── mod.rs
                ├── sst_builder.rs
                ├── basic.rs
                ├── gc.rs
                ├── advanced.rs
                ├── cache.rs
                └── manifest.rs
```

## Build and Test Commands

### Building

```bash
cargo build --workspace --all-features
cargo build --release --package kv-engine
```

### Testing

The project uses **cargo-nextest** as the preferred test runner. Install it first:

```bash
cargo make install-nextest    # or: cargo install cargo-nextest --locked
```

Run tests:

```bash
# Fast path (library tests only)
cargo nextest run --workspace --all-features --lib

# All tests including integration tests
cargo nextest run --workspace --all-features --all-targets

# Via cargo-make
cargo make test
```

Coverage:

```bash
cargo make test-cov           # Generates HTML coverage report
```

### Benchmarks

```bash
cargo bench --package kv-engine --bench vlog_benchmarks
```

The vLog benchmark compares inline vs key-value separation across write throughput, compaction time, point-get latency, and scan throughput.

### All-in-one Check

```bash
cargo make check              # Runs fmt, dep-sort, clippy, machete, test, typos
```

Individual checks:

```bash
cargo make check-fmt
cargo make check-clippy       # -D warnings
cargo make check-typos
cargo make check-machete      # unused dependency check
cargo make check-dep-sort     # cargo-sort
cargo make check-hakari       # workspace-hack verification
```

## Code Style Guidelines

Formatting is governed by `rustfmt.toml`:

- Edition / style edition: 2024
- `tab_spaces = 4`
- `comment_width = 120`
- `wrap_comments = true`
- `normalize_comments = true`
- `reorder_imports = true`
- `reorder_impl_items = true`
- `format_code_in_doc_comments = true`
- `format_macro_bodies = true`
- `format_macro_matchers = true`

Run `cargo fmt --all` before committing. CI enforces `cargo fmt --check`.

### Dependency Management

- Add new deps to `kv-engine/Cargo.toml` (the only crate in the workspace).
- Keep deps sorted alphabetically (`cargo make check-dep-sort` enforces this).
- `cargo-hakari` is used for workspace-hack management; run `cargo make check-hakari` if you modify deps.
- `cargo-machete` is used to detect unused dependencies.

## Testing Instructions

### Test Organization

- **Unit tests** live in the same file as the code they test (e.g., `block.rs` has `#[cfg(test)]` blocks).
- **Integration tests** live under `kv-engine/src/tests/` and are declared in `kv-engine/src/tests.rs`.
- **vLog integration tests** are in `kv-engine/src/tests/vlog_integration_tests/` (split into `sst_builder.rs`, `basic.rs`, `gc.rs`, `advanced.rs`, `cache.rs`, `manifest.rs`).

### Test Configuration

`.config/nextest.toml`:
- Slow-timeout: 10s period, terminate after 3 retries, 3s grace period
- Retries: up to 3 with exponential backoff + jitter
- Test threads: `num-cpus`

### Key Test Modules

- `tests::block` — block encoding/decoding and iteration
- `tests::sst` — SSTable builder and iterator correctness
- `tests::iterators` / `merge_iterator` — merge/concat iterator behavior
- `tests::memtable` — memtable operations
- `tests::compaction` / `compaction_integration*` / `*compaction` — compaction strategies
- `tests::bloom_compression` — bloom filter false-positive rates
- `tests::harness` — shared test utilities

## Security Considerations

### CI Security

- Workflows use `step-security/harden-runner` with egress-policy audit (intended to become `block` after validation).
- Actions are pinned to specific commit SHAs.
- OSSF Scorecard workflow runs on a weekly schedule.
- Dependency review workflow runs on PRs.

### Sanitizers

The `safety.yml` workflow runs tests under:
- **AddressSanitizer** (`-Z sanitizer=address`)
- **LeakSanitizer** (`-Z sanitizer=leak`) with `lsan-suppressions.txt`

Note: Miri is disabled because `crossbeam-skiplist` uses epoch-based GC incompatible with Miri.

### Runtime Safety

- The engine uses `crossbeam-epoch` for lock-free data structures.
- `parking_lot` is used for mutexes/rwlocks instead of `std::sync`.
- CRC32 checksums protect SST blocks and vLog entries.
- vLog entries include key validation on read to detect stale/corrupted pointers.

## Architecture Notes

### LSM Storage State

`LsmStorageState` (in `lsm_storage.rs`) is the central mutable state:
- `memtable` — current writable memtable (crossbeam SkipMap)
- `imm_memtables` — frozen memtables awaiting flush
- `l0_sstables` — L0 SST file IDs
- `levels` — L1+ tiers or levels
- `sstables` — map of SST ID → `Arc<SsTable>`

State mutations follow a copy-on-write pattern: the state is behind `Arc<RwLock<Arc<...>>>` so readers get snapshots while background tasks (flush, compaction) produce new state versions under a separate `state_lock`.

### Key-Value Separation (vLog)

Large values can be stored in separate `.vlog` files instead of inline in SSTs. This is inspired by WiscKey and reduces compaction write amplification.

Key structs:
- `ValuePointer` — 16-byte reference `(file_id, offset, size)` stored in the LSM tree
- `ValueLog` — manages vLog files, reference tracking, and reader caching
- `ValueLogBuilder` — writes vLog entries during SST construction
- `GarbageCollector` — reclaims stale vLog space post-compaction

Enable via `LsmStorageOptions::value_separation`.

### Compaction Strategies

`CompactionOptions` supports four strategies:
- `NoCompaction` — disables background compaction
- `SimpleLeveledCompaction` — simple size-ratio based
- `LeveledCompaction` — standard leveled compaction
- `TieredCompaction` — tiered/leveling hybrid

### MVCC

`mvcc.rs` provides multi-version concurrency control with:
- Timestamp allocation and watermark tracking
- Serializable transaction support (optional, gated by `serializable` option)
- `Transaction` API in `mvcc/txn.rs`

### WAL

Write-ahead logging is optional (`enable_wal: bool`). When enabled, each memtable has an associated WAL file for crash recovery.

## Development Workflow

1. Install the nightly toolchain (the `rust-toolchain` file handles this automatically).
2. Make changes.
3. Run `cargo make check` locally to verify fmt, clippy, tests, and typos.
4. Open a PR. CI runs `check.yml`, `test.yml`, and `safety.yml`.

## Useful Binaries

- `kv-engine-cli` — interactive REPL for manual testing:
  ```bash
  cargo run --bin kv-engine-cli -- --path /tmp/lsm.db --compaction leveled
  ```
  Supports commands: `fill <begin> <end>`, `get <key>`, `del <key>`, `scan [begin] [end]`, `dump`, `flush`, `full_compaction`, `quit`.

- `compaction-simulator` — compaction strategy simulation
