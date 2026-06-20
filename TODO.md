# Refactoring TODO

All items completed. See individual PRs for details.

## Completed

### Early refactoring (PRs #31-#87)
- [x] **WAL recovery deduplication** (`wal.rs`) — `RecoveryHandler` trait + shared `recover_mvcc`/`recover_legacy_with` helpers. 830 → 669 lines.
- [x] **Tombstone detection consolidation** — `KvKind::is_tombstone_value(&[u8])` in `vlog/mod.rs`, used across 14 call sites.
- [x] **Eliminate `vlog_enabled` branching** — `MemTable::create(id, vlog_enabled)` replaces paired constructors.
- [x] **Dedup `LsmStorageState::create` level init** — `init_levels` closure replaces copy-pasted blocks.
- [x] **Dedup `map_bound`** — removed from `mvcc/txn.rs`, imports from `mem_table`.
- [x] **Fix range tombstone + vlog recovery bug** — `recover_from_wal_with_range_tombstones` now accepts `vlog_enabled`.
- [x] **Remove blanket `#![allow(dead_code)]`** — replaced with targeted `#[allow(dead_code)]` on intentional dead code.
- [x] **Fix `unwrap()` in production** — `scan_with_vlog` now uses `expect()` with invariant message.
- [x] **Remove unused `_vlog_enabled` param** — `resolve_item_value` signature cleaned up.
- [x] **Add `debug_assert!` for corrupt timestamps** — `extract_ts` failures caught in debug builds.
- [x] **Add unit test for `KvKind::is_tombstone_value`** — 6 edge cases covered.
- [x] **WAL validation bounds checks** — full entry size verified before updating `expected_size` in `recover_mvcc`.
- [x] **Range tombstone ordinal reset per batch** — `reset_range_ordinals()` on `RecoveryHandler` trait.
- [x] **Tombstone classification in recovery** — `handle_put` routes tombstone values to `point_tombstones`.
- [x] **WAL version preservation** — `is_v3` field on `Wal`, `put_batch` writes matching format.
- [x] **v2 range tombstone rejection** — `put_range_tombstone_batch` rejects v2 WALs.
- [x] **Debug assert safety** — `debug_assert!` on `extract_ts` guarded with `!TS_ENABLED`.

### Late refactoring (PRs #116-#122)
- [x] **WAL recovery dedup** — PR #116
- [x] **Replace `expect()` with `Result`** — PR #117
- [x] **Replace `eprintln!` with logforth** — PR #118
- [x] **Decompose `lsm_storage.rs`** — PR #119
- [x] **Consolidate test option constructors** — PR #120
- [x] **Simplify `range_overlap` match arms** — PR #121
- [x] **Extract `compare_and_set` shared logic** — PR #122
