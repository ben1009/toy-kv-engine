# kv-engine

`kv-engine` is the workspace crate that implements the storage engine used by
the repository root examples, tests, benchmarks, and RFCs.

## What Lives Here

- `src/lsm_storage.rs` holds the main engine API and async wrappers.
- `src/lsm_storage/` implements shared async shutdown, native point commits,
  and memtable leases for default parallel v4 WALs; see the
  [integration report](../docs/wal/rfc-024-native-async-integration-20261005.md) and
  [one-put and batch64 matrix](../docs/wal/benchmarks/rfc-024-native-async-vs-leader-20261005.md).
  The [latest batch64 rerun](../docs/wal/benchmarks/rfc-024-native-batch64-leader-rerun-20261006.md)
  records the retained backend's comparison with leader. The full RFC
  performance gate remains unqualified.
- `src/pitr/` contains point-in-time recovery, archive, and restore modules.
- `src/checkpoint.rs` implements sync/async checkpoint creation, target locks,
  stale-temp validation, and atomic no-replace publication.
- `../rfcs/023-point-in-time-recovery.md` documents the PITR design.
- `../rfcs/022-incremental-backup.md` documents the implemented incremental
  backup repository built on immutable SST/vLog object identity and checkpoint
  capture.
- `src/wal.rs` implements the WAL, including the default parallel v4 io_uring
  durable path. PITR and legacy formats retain their existing paths; see the
  [default adoption note](../docs/wal/rfc-024-parallel-wal-default-20261005.md).
- `src/vlog/` contains value-separation storage, indexing, and GC.
- `src/tests/` contains in-crate test coverage for MVCC, compaction,
  TTL, scans, and cache behavior.
- `integration_tests/` contains Cargo integration targets for process-level
  chaos and cross-process persistence tests. The targets are declared explicitly
  in `Cargo.toml`.
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
