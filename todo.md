# MVCC TODO

**RFC:** [rfcs/005-mvcc.md](rfcs/005-mvcc.md)
**Plan:** [.claude/plans/mvcc.md](.claude/plans/mvcc.md)

---

## RFC 017: Standalone MVCC GC Follow-up

**RFC:** [rfcs/017-mvcc-garbage-collection.md](rfcs/017-mvcc-garbage-collection.md)
**Status:** partially implemented on `feat/mvcc-gc-scheduler`

### Landed slice

- [x] Periodic background wakeup can trigger MVCC GC even when ordinary
  compaction is idle
- [x] MVCC watermark is used as the effective GC cutoff
- [x] Candidate generation exists for leveled, simple, and tiered modes
- [x] SST reservations prevent overlapping GC / compaction ownership
- [x] TTL-aware bottom-level candidate picking exists
- [x] Current compaction rewrite path remains the execution mechanism

### Remaining implementation work

- [ ] Split RFC 017 candidate picking into a dedicated picker surface instead of
  keeping all policy embedded in `generate_mvcc_gc_task()`
- [ ] Add the missing stats surfaces the RFC assumes for candidate scoring
  Current code mostly uses `max_ts`, TTL metadata, overlap shape, and
  reservations. The RFC still calls out tombstone density,
  range-tombstone density, and redundant-version estimates.
- [ ] Define the minimum viable GC scoring policy beyond `max_ts` / TTL
  metadata
  Start with cheap metadata-only scoring, then layer in richer reclaim signals
  only where they improve candidate quality measurably.
- [ ] Tighten scheduler backpressure / retry semantics
  Document and implement what happens when reservation succeeds but submission
  cannot proceed, and make retry behavior explicit rather than implicit in the
  periodic wakeup loop.
- [ ] Add deterministic tests for ordinary-compaction vs GC-compaction
  coexistence and reservation conflicts
- [ ] Add deterministic tests for stats-driven candidate selection once those
  stats exist
- [ ] Add tests showing TTL-heavy SSTs are selected without size-pressure
- [ ] Add tests showing standalone MVCC GC improves later vLog reclaim
  opportunities by removing obsolete pointer-bearing versions
- [ ] Reconcile RFC 017 wording with the actually shipped slice
  If richer stats-driven scoring is deferred, keep the RFC explicit that it is
  future work rather than already implemented behavior.

### Current PR-loop blockers on `feat/mvcc-gc-scheduler`

- [ ] Clear the remaining GitHub `sanitizers-unit` failure on the latest PR head
- [ ] Keep the PR review loop running until CI is green or a concrete new
  blocker appears

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

### Pending: Production sync performance

See `docs/bench-report-crud-bench-fjall.md` for benchmark details.

- [x] **Batch reads** — Closed 5× gap to ~1.06× (tied). `batch_get` with shared state, sorted keys, reusable encode buffer (PR #127).
- [x] **Durable create/delete target rows** — Post-optimization focused rerun wins all targeted sync cases:
  `put_c` +92.2%, `delete_c` +326.4%, `batch_create_100` +6.5%, `batch_create_1000` +6.0%,
  `batch_delete_100` +26.9%, `batch_delete_1000` +24.1% vs Fjall. Source CSVs:
  `/tmp/result-toykv_batch_opt_sync_100k.csv` and `/tmp/result-fjall_compare_sync_100k.csv`. Later `opt2`
  CSVs came from a reverted small-batch duplicate-scan experiment and are rejected. `write_batch` now avoids the
  full last-op map/sort when batch keys are unique.
- [ ] **Close sync-vs-no-sync gap for production workloads** — Production uses `--sync`, so prioritize reducing the
  ToyKV durable penalty rather than optimizing buffered-only paths. Current focused ToyKV gaps: `put_c` 275,683 vs
  332,689 no-sync (-17%), `batch_create_100` 3,059 vs 3,237 (-5.5%), `batch_create_1000` 316 vs 342 (-7.6%),
  `batch_delete_100` 10,146 vs 8,138 (sync faster/noisy), `batch_delete_1000` 1,738 vs 2,071 (-16%).
  The huge single-row `Create` gap in crud-bench is expected and should not be read the same way as the batch rows:
  ToyKV `no-sync` disables WAL entirely in the adapter, while ToyKV `sync` enables WAL and waits for
  `commit_wal()`/`submit_and_commit()`. That means `Create` is comparing a non-durable in-memory write path against a
  durable WAL+sync barrier. The more decision-useful sync/no-sync signal is the batch workloads, where fsync cost is
  amortized by group commit.
- [x] **Profile sync write path** — `WriteProfile` in `mem_table.rs` + `--profile` flag in write-perf (PR #130).
  Measures WAL append, fsync, and memtable insert time per workload.
- [x] **Group commit / batched WAL sync** — PR #130. Lock-free `ArrayQueue` buffer pool + `SegQueue` ready queue,
  leader/follower condvar barrier in `submit_and_commit()`, ring buffer for per-batch result tracking, Case 3 early
  break for followers, `advance_ts()` to defer `current_ts` until after publish. 4-thread throughput: 177K → 451K
  (+155%) vs main. WAL-only write path prevents ghost writes. 10 new tests for coverage.
- [ ] **Reduce sync batch overhead before fsync** — Audit `Wal::put_batch`, `MemTable::put_raw_batch_inner`, and MVCC
  `write_batch` allocation paths for avoidable per-record buffers/copies. The next useful target is making durable
  `batch_create_1000` closer to the no-sync path without hurting duplicate-key last-op-wins semantics.
- [ ] **Add sync perf gates to the comparison workflow** — Track both absolute Fjall-relative OPS and
  sync/no-sync ratio for `put_c`, `batch_create_100`, `batch_create_1000`, `batch_delete_100`, and
  `batch_delete_1000`. Do not accept buffered-only improvements that regress sync production cases. Initial gates:
  no focused sync row regresses by more than 5%, sync/no-sync ratio improves for at least two of `put_c`,
  `batch_create_1000`, and `batch_delete_1000`, and single-client sync p95/p99 latency does not materially regress.
- [x] **Ticket-based group commit** — Replace CAS-based leader election with ticket/sequence design to eliminate
  O(N) leader-election cascade. Assign monotonic ticket on `put_batch`, leader drains queue + records max ticket,
  sets `durable_sequence` atomic after I/O. Followers check `durable_sequence >= my_ticket` and return immediately
  without touching CAS. Avoids N-1 wasted empty-bufs leader elections after each real commit. Suggested by
  gemini-code-assist in PR #134 review.

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
