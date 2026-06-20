# Refactoring TODO

## Completed

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

## Remaining (future PRs)

- [x] **Replace `expect()` with `Result`** on data-dependent paths in `lsm_storage.rs` (lines ~1504, ~1636, ~2357). These can panic on corrupted data.
- [x] **Replace `eprintln!` in background threads** — compaction, flush, GC threads use `eprintln!` for errors. Add proper logging or error channel.
- [x] **Decompose `lsm_storage.rs`** — extract `open()` manifest replay (~190-line match block) into a `replay_manifest_record` method.
- [x] **Consolidate test option constructors** — three `default_for_*` differ only in two fields. Use a builder or single constructor with overrides.
- [ ] **Simplify `range_overlap` match arms** in `mem_table.rs`. A bound-normalizing helper would cut the 100-line match.
- [ ] **Extract `compare_and_set` shared logic** — three CAS methods duplicate the read-then-verify-then-write pattern.
