# MVCC TODO

**RFC:** [rfcs/005-mvcc.md](rfcs/005-mvcc.md)
**Plan:** [.claude/plans/mvcc.md](.claude/plans/mvcc.md)

---

## Phase 1: Timestamped Keys ✅

PR #70 (merged 2026-06-07). Internal key encoding, MVCC-aware reads/scans/compaction, watermark scaffolding.

---

## Phase 2: Format Hardening ← NEXT

- [ ] Add format markers for MVCC WAL, SST, value, vLog, and `.vidx`
- [ ] Replace unchecked fixed-width casts with checked conversions
- [ ] Implement WAL batch framing and checksum validation
- [ ] Persist SST `max_ts` in format-versioned table metadata
- [ ] Recover max timestamp from WAL batches and SST `max_ts`
- [ ] Initialize `LsmMvccInner::new(max_commit_ts)` on recovery (not `0`)

---

## Phase 3: Migration and Compatibility

- [ ] Detect pre-MVCC vs MVCC directories (manifest + file format)
- [ ] Reject mixed-format directories on startup
- [ ] Implement migration path (raw keys → timestamp zero)
- [ ] Add rollback and failure-injection tests

---

## Phase 4: MVCC State and Watermark (partially done)

- [x] Watermark
- [x] `LsmMvccInner` initialization
- [x] `ReadGuard` registration and cleanup
- [ ] Recover max timestamp from WAL/SST (blocked on Phase 2)

---

## Phase 5: Versioned Writes and Reads (partially done)

- [ ] Add `KvKind::Tombstone` and update all parsers
- [ ] Canonicalize duplicate user keys in `put`, `delete`, `write_batch`
- [x] Commit timestamps and internal keys in memtables/WALs
- [x] Version-aware `get`
- [x] Bloom filters hash by user key

---

## Phase 6: Snapshot Scans (mostly done)

- [x] `LsmIterator` collapses duplicate user keys
- [x] Memtable/SST range bounds are timestamp-aware
- [ ] Handle `Bound::Excluded` for MVCC-encoded keys
- [ ] Add scan tests for concurrent writes during iteration

---

## Phase 7: Transactions

- [ ] Implement `LsmMvccInner::new_txn`
- [ ] Implement `Transaction::{get, scan, put, delete, commit}`
- [ ] Implement `TxnEntry` merge layer or extend `StorageIterator` with value-kind
- [ ] Add repeatable snapshot read tests

---

## Phase 8: Point-Key Serializable OCC

- [ ] Read/write user-key sets (replace `HashSet<u32>` sketches)
- [ ] Committed transaction pruning by watermark
- [ ] Record non-transactional writes in `committed_txns`
- [ ] Record point reads, negative reads, tombstone reads, scan keys in `read_set`
- [ ] Detect read/write conflicts at commit
- [ ] Conflict, no-conflict, double-commit, mutation-after-commit tests

---

## Phase 9: Compaction GC

- [x] Preserve tombstones during compaction when MVCC enabled
- [ ] Populate SST `max_ts` (blocked on Phase 2)
- [ ] Watermark-aware version dropping in compaction
- [ ] Tests for old-version reclamation

---

## Phase 10: vLog Integration

- [ ] Validate vLog entries against full internal keys
- [ ] Compaction-output vLog GC rewrites exact live internal-key versions
- [ ] Internal-version CAS for background GC path
- [ ] Verify vLog GC with multiple versions of same user key
- [ ] Tests for pointer-bearing historical versions

---

## Performance Optimizations

- [ ] `decode_user_key_cow` to avoid heap allocs in bloom hash, vLog deref
- [ ] Avoid cloning `encoded_user_key` in `lsm_iterator::next()`
- [ ] Replace `is_some()` + `.unwrap()` with `if let Some(ref mvcc)`
- [ ] Avoid `to_vec()` allocation in memtable seek prefix

---

## Testing Progress (8/30 from RFC §9)

- [x] 1. Internal key ordering: same user key sorts newest timestamp first
- [x] 2. `get` returns newest version at or below read timestamp
- [x] 3. `delete` hides older versions for newer snapshots
- [x] 4. `scan` yields one visible version per user key
- [ ] 5. Long-running scan does not observe concurrent writes
- [ ] 6. WAL recovery restores versioned keys and max timestamp
- [ ] 7. Snapshot transaction reads are repeatable
- [ ] 8. Transaction local writes shadow snapshot state
- [ ] 9. Point-key serializable transaction aborts on read/write conflict
- [ ] 10. Point-key serializable transaction commits when write sets do not conflict
- [ ] 11. Compaction keeps versions with `commit_ts > watermark`
- [ ] 12. Compaction keeps newest version with `commit_ts <= watermark`
- [ ] 13. Compaction does not resurrect deleted keys
- [ ] 14. vLog values remain readable across multiple versions
- [ ] 15. vLog GC does not remove pointer still visible to old snapshot
- [x] 16. Prefix user keys sort and seek correctly
- [ ] 17. WAL recovery ignores/truncates incomplete MVCC batch records
- [ ] 18. WAL recovery follows crash contract for complete synced batch
- [x] 19. Escaped user keys with `0x00` bytes decode correctly
- [x] 20. Bloom filters hash decoded user keys consistently
- [ ] 21. Keys exceeding format limit are rejected before writes
- [ ] 22. Duplicate user keys in batch/commit are canonicalized last-op-wins
- [ ] 23. vLog index entries use full encoded internal keys
- [ ] 24. Point-key serializable OCC records negative point reads
- [ ] 25. MVCC tombstone parser tests
- [ ] 26. `scan` records yielded keys in `read_set`
- [ ] 27. Non-transactional writes conflict with point-key serializable transactions
- [ ] 28. Transaction `commit` is single-use
- [ ] 29. Pre-MVCC migration tests
- [ ] 30. SST `max_ts` persists in format-versioned metadata
