# MVCC TODO

**RFC:** [rfcs/005-mvcc.md](rfcs/005-mvcc.md)
**Plan:** [.claude/plans/mvcc.md](.claude/plans/mvcc.md)

---

## Phase 1: Timestamped Keys ✅

PR #70 (merged 2026-06-07). Internal key encoding, MVCC-aware reads/scans/compaction, watermark scaffolding.

---

## Phase 2: Format Hardening ✅

- [x] Add format markers for MVCC WAL and SST
- [x] Replace unchecked fixed-width casts with checked conversions
- [x] Implement WAL batch framing and checksum validation
- [x] Persist SST `max_ts` in format-versioned table metadata
- [x] Recover max timestamp from WAL batches and SST `max_ts`
- [x] Initialize `LsmMvccInner::new(max_commit_ts)` on recovery (not `0`)

---

## Phase 3: Format Detection ✅

- [x] Add `FormatVersion(u32)` manifest record (version 2 = MVCC)
- [x] Write `FormatVersion(2)` on fresh database creation
- [x] Persist format version in `Snapshot` records for manifest compaction
- [x] Reject pre-MVCC directories on startup (no format marker)
- [x] Reject unsupported format versions on startup
- [x] Tests for format detection and rejection

---

## Phase 4: MVCC State and Watermark (partially done)

- [x] Watermark
- [x] `LsmMvccInner` initialization
- [x] Recover max timestamp from WAL/SST
- [x] `ReadGuard` registration and cleanup

---

## Phase 5: Versioned Writes and Reads (partially done)

- [x] Add `KvKind::Tombstone` and update all parsers (PR #77)
- [x] Canonicalize duplicate user keys in `put`, `delete`, `write_batch` (PR #77)
- [x] Commit timestamps and internal keys in memtables
- [x] WAL write/recovery for versioned keys (batch framing + CRC32 + max_ts recovery)
- [x] Version-aware `get`
- [x] Bloom filters hash by user key

---

## Phase 6: Snapshot Scans ✅

- [x] `LsmIterator` collapses duplicate user keys
- [x] Memtable/SST range bounds are timestamp-aware
- [x] Handle `Bound::Excluded` for MVCC-encoded keys (already implemented, verified with tests)
- [x] Add scan tests for concurrent writes during iteration

---

## Phase 7: Transactions ✅

PR #80 (merged 2026-06-09). Transaction API with snapshot isolation.

- [x] Implement `LsmMvccInner::new_txn`
- [x] Implement `Transaction::{get, scan, put, delete, commit}`
- [x] Implement `TxnLocalIterator` (ouroboros self-referencing) and `TxnIterator` (TwoMergeIterator merge layer)
- [x] Add repeatable snapshot read tests and transaction behavior tests

---

## Phase 8: Point-Key Serializable OCC ✅

PR #82 (merged 2026-06-10). Optimistic concurrency control for serializable isolation.

- [x] Read/write user-key sets (replace `HashSet<u32>` sketches)
- [x] Committed transaction pruning by watermark
- [x] Record non-transactional writes in `committed_txns`
- [x] Record point reads, negative reads, tombstone reads, scan keys in `read_set`
- [x] Detect read/write conflicts at commit
- [x] Conflict, no-conflict, double-commit, mutation-after-commit tests

---

## Phase 9: Compaction GC ✅

PR #84 (merged 2026-06-10). Watermark-aware version dropping in compaction.

- [x] Preserve tombstones during compaction when MVCC enabled
- [x] Populate SST `max_ts` (persisted in v2 footer, recovered on open)
- [x] Watermark-aware version dropping in compaction
- [x] Tests for old-version reclamation

---

## Phase 10: vLog Integration ✅

PR #85 (merged 2026-06-10). Version-aware GC with internal key storage in vLog.

- [x] Store full internal keys in vLog entries (table/builder.rs, mem_table.rs)
- [x] Version-specific liveness check in GC (`get_with_kind_at_ts`)
- [x] Version-aware CAS for GC rewrite (`compare_and_set_batch_at_ts`)
- [x] Thread found internal key through read path for vLog verification
- [x] Skip WAL for GC rewrites (`put_raw_batch_no_wal`)
- [x] Adjacent SST scanning in `get_with_kind_at_ts` for split versions
- [x] Tests for version-aware GC (preserve old version, drop unreferenced, multi-version, index keys, adjacent SST)

---

## Performance Optimizations

- [x] `decode_user_key_cow` to avoid heap allocs in bloom hash, vLog deref (PR #87)
- [x] Avoid cloning `encoded_user_key` in `lsm_iterator::next()` (PR #83: `decode_user_key_into` buffer reuse)
- [x] Replace `is_some()` + `.unwrap()` with `if let Some(ref mvcc)` (PR #83)
- [x] Avoid `to_vec()` allocation in memtable seek prefix (PR #87)
- [x] Bloom filter in `get_raw_exact` to skip skiplist lookups (PR #85)
- [x] Encoded prefix comparison in `lookup_by_user_key` to avoid heap allocs (PR #85)
- [x] `partition_point` for leveled SST lookup in `get_with_kind_at_ts` (PR #85)
- [x] Lock-free watermark: `DashMap<u64, AtomicUsize>` + `watermark.read()` — 3.4× read throughput (PR #126)

### Pending: Close gap with Fjall

See `docs/bench-report-crud-bench-fjall.md` for benchmark details.

- [x] **Batch reads** — Closed 5× gap to ~1.06× (tied). `batch_get` with shared state, sorted keys, reusable encode buffer (PR #127).
- [ ] **Durable updates** — Fjall 1.8× faster with `--sync`. Group commit for WAL.
- [ ] **Batch delete_100** — Fjall 1.4× faster. Profile batch delete path.
- [ ] **Batch create_1000** — Fjall 1.5× faster. Profile large batch create path.

---

## Testing Progress (30/30 from RFC §9) ✅

PR #86 (merged 2026-06-10). Final 4 tests (21, 22, 24, 25) + review fixes.

- [x] 1. Internal key ordering: same user key sorts newest timestamp first
- [x] 2. `get` returns newest version at or below read timestamp (read_ts wiring done; advanced filtering in Phase 5)
- [x] 3. `delete` hides older versions for newer snapshots
- [x] 4. `scan` yields one visible version per user key
- [x] 5. Long-running scan does not observe concurrent writes (snapshot isolation tests in mvcc_scan.rs)
- [x] 6. WAL recovery restores versioned keys and max timestamp
- [x] 7. Snapshot transaction reads are repeatable (test_txn_snapshot_isolation in mvcc.rs)
- [x] 8. Transaction local writes shadow snapshot state (test_txn_local_writes_shadow_engine in mvcc.rs)
- [x] 9. Point-key serializable transaction aborts on read/write conflict
- [x] 10. Point-key serializable transaction commits when write sets do not conflict
- [x] 11. Compaction keeps versions with `commit_ts > watermark`
- [x] 12. Compaction keeps newest version with `commit_ts <= watermark`
- [x] 13. Compaction does not resurrect deleted keys
- [x] 14. vLog values remain readable across multiple versions
- [x] 15. vLog GC does not remove pointer still visible to old snapshot
- [x] 16. Prefix user keys sort and seek correctly
- [x] 17. WAL recovery ignores/truncates incomplete MVCC batch records
- [x] 18. WAL recovery follows crash contract for complete synced batch
- [x] 19. Escaped user keys with `0x00` bytes decode correctly
- [x] 20. Bloom filters hash decoded user keys consistently
- [x] 21. Keys exceeding format limit are rejected before writes
- [x] 22. Duplicate user keys in batch/commit are canonicalized last-op-wins
- [x] 23. vLog index entries use full encoded internal keys
- [x] 24. Point-key serializable OCC records negative point reads
- [x] 25. MVCC tombstone parser tests
- [x] 26. `scan` records yielded keys in `read_set`
- [x] 27. Non-transactional writes conflict with point-key serializable transactions
- [x] 28. Transaction `commit` is single-use (test_txn_double_commit_fails in mvcc.rs)
- [x] 29. Pre-MVCC format detection and rejection tests
- [x] 30. SST `max_ts` persists in format-versioned metadata
