# kv-engine

`kv-engine` is the workspace crate that implements the storage engine used by
the repository root examples, tests, benchmarks, and RFCs.

## What Lives Here

- `src/lsm_storage.rs` holds the main engine API and async wrappers.
- `src/wal.rs` implements the WAL, including the io_uring durable path.
- `src/vlog/` contains value-separation storage, indexing, and GC.
- `src/tests/` contains in-crate integration coverage for MVCC, compaction,
  TTL, scans, and cache behavior.
- `tests/` contains process-level chaos and cross-process persistence tests.
- `benches/` contains Criterion benchmarks for vLog, WAL, DeleteRange, and
  memtable hot paths.

## Useful Commands

```bash
# Build just the crate
cargo build -p kv-engine --all-features

# Preferred local test suite
cargo make test

# Full local gate
cargo make check

# Chaos harness tests
cargo make test-chaos
```

See the repository [README](../README.md) for the top-level feature list, RFC
index, and benchmark notes.
