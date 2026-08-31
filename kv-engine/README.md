# kv-engine

`kv-engine` is the workspace crate that implements the storage engine used by
the repository root examples, tests, benchmarks, and RFCs.

## What Lives Here

- `src/lsm_storage.rs` holds the main engine API and async wrappers.
- `src/checkpoint.rs` implements sync/async checkpoint creation, target locks,
  stale-temp validation, and atomic no-replace publication.
- `../rfcs/022-incremental-backup.md` specifies the proposed incremental backup
  repository built on immutable SST/vLog object identity and checkpoint capture.
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

# Optional compaction accounting verifier
TOYKV_COMPACTION_SETSUM=1 cargo test --locked --package kv-engine \
  --features compaction-setsum --lib tests::compaction

# Focused checkpoint/backup coverage, including failpoint crash windows
cargo test --package kv-engine checkpoint --features chaos-testing
```

See the repository [README](../README.md) for the top-level feature list, RFC
index, benchmark notes, and details on when to use the `compaction-setsum`
verifier.
