# MVCC / GC / Perf TODO

**RFC:** [rfcs/005-mvcc.md](rfcs/005-mvcc.md)
**Plan:** [.claude/plans/mvcc.md](.claude/plans/mvcc.md)

---

## RFC 017: Standalone MVCC GC Follow-up

**RFC:** [rfcs/017-mvcc-garbage-collection.md](rfcs/017-mvcc-garbage-collection.md)
**Status:** landed on `main` via PR #165 and follow-up commit `fc93578`

### Landed slice

- [x] Periodic background wakeup can trigger MVCC GC even when ordinary
  compaction is idle
- [x] MVCC watermark is used as the effective GC cutoff
- [x] Candidate generation exists for leveled, simple, and tiered modes
- [x] SST reservations prevent overlapping GC / compaction ownership
- [x] TTL-aware bottom-level candidate picking exists
- [x] Leveled MVCC GC skips no-op bottom SST rewrites without reclaimable MVCC
  entries
- [x] vLog reference unregister happens after publication succeeds
- [x] Current compaction rewrite path remains the execution mechanism

### Follow-up checklist

- [x] Split RFC 017 candidate picking into a dedicated picker surface instead of
  keeping all policy embedded in `generate_mvcc_gc_task()`
- [x] Add the missing stats surfaces the RFC assumes for candidate scoring
  Current code mostly uses `max_ts`, TTL metadata, overlap shape, and
  reservations. The RFC still calls out tombstone density,
  range-tombstone density, and redundant-version estimates.
- [x] Define the minimum viable GC scoring policy beyond `max_ts` / TTL
  metadata
  Candidate scoring now uses per-SST GC stats for redundant versions,
  point tombstones, range-tombstone fragments, TTL counts, and size.
- [x] Tighten scheduler backpressure / retry semantics
  Reservation conflicts are surfaced as `CompactionTriggerOutcome::Deferred`.
  Submission failures bubble out of the trigger path and are retried on the
  next periodic wakeup after logging.
- [x] Add deterministic tests for ordinary-compaction vs GC-compaction
  coexistence and reservation conflicts
  Covered both candidate generators in one snapshot and verified the
  reservation conflict returns `Deferred` before ordinary compaction is
  submitted.
- [x] Add deterministic tests for stats-driven candidate selection once those
  stats exist
  Covered leveled GC picking by redundant-version density and TTL-expired SSTs
  without relying on size pressure.
- [x] Add tests showing TTL-heavy SSTs are selected without size-pressure
- [x] Add tests showing standalone MVCC GC improves later vLog reclaim
  opportunities by removing obsolete pointer-bearing versions
- [x] Reconcile RFC 017 wording with the actually shipped slice
  The RFC now names the implemented stats signals and the actual retry behavior
  instead of leaving those pieces as future work.

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

### Landed since the original MVCC rollout

- [x] TTL write/read/scan/compaction support (see `rfcs/016-ttl.md`)
- [x] Standalone MVCC GC scheduling and picker/scoring follow-up (see RFC 017 section above)
- [x] Async wrapper surface plus staged `open_async()` / `close_async()` runtime ownership work
- [x] Parallel async scan with chunk API and cache-admission controls

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
  First slice: public MVCC `write_batch` now builds publish-ready kind-prefixed values once, while transaction commits
  still mark raw values explicitly. Same-machine `crud-bench` A/B on 2026-07-15 (`--samples 100000 --clients 4
  --threads 4 --skip-indexes --skip-scans`) moved sync `batch_create_1000` 1,653.63 → 1,735.30 OPS (+4.9%) and
  sync `batch_delete_1000` 4,111.07 → 4,961.03 OPS (+20.7%). Sync/no-sync ratio improved for `put_c` and
  `batch_delete_1000`, but not yet for `batch_create_1000`.
  Follow-up same-window A/B: collapsing public `write_batch` classify/validate scans and swapping the batch state read
  to `ArcSwap::load()` moved sync `batch_create_1000` 1,301.73 → 1,588.21 OPS (+22.0%) versus the immediate control,
  with `batch_delete_1000` trading down 4,912.19 → 4,591.15 OPS (-6.5%) while still staying above the original
  baseline.
  Next slice: MVCC point batches now build entries while proving uniqueness and only fall back to the full last-op-wins
  dedup path after observing a duplicate. Same command moved `batch_create_1000` essentially flat at 1,588.00 OPS and
  improved `batch_delete_1000` to 5,453.39 OPS.
  Follow-up: write-batch duplicate detection now uses in-memory `AHashSet`/`AHashMap` instead of the default hasher.
  Focused sync rerun moved `batch_create_1000` to 1,678.16 OPS and kept `batch_delete_1000` near flat at 5,414.47 OPS.
  Follow-up: `MemTable::publish_raw_batch` now batches `approximate_size` accounting into one relaxed atomic add per
  publish. Immediate outside-sandbox rerun moved `batch_create_1000` 1,554.01 → 1,675.39 OPS, `batch_update_1000`
  1,132.67 → 1,744.71 OPS, and `batch_delete_1000` 5,062.92 → 5,341.41 OPS. Follow-up: publish now reuses a
  thread-local user-key decode buffer instead of allocating one per publish call. Same-window sync A/B moved
  `batch_create_1000` 1,608.03 → 1,767.15 OPS, `batch_update_1000` 1,675.26 → 1,729.03 OPS, and
  `batch_delete_1000` 4,828.19 → 4,983.64 OPS. Current no-sync comparison before these publish slices:
  `batch_create_1000` 1,678.16 / 2,660.18 OPS (63.1%), and `batch_delete_1000` 5,414.47 / 7,408.65 OPS (73.1%).
  The next remaining gap is still durable `batch_create_1000`.
  Rejected follow-ups: consuming the prepared entry vector in MVCC WAL staging regressed `batch_delete_1000`, and fusing
  point key validation into entry construction was too noisy/regressive in rerun (`batch_create_1000` fell to 1,099.95
  OPS). Carrying precomputed user-key bloom hashes through deferred publish also regressed sync `batch_create_1000`
  (1,182.16 OPS) and `batch_delete_1000` (3,932.22 OPS). Borrowing user keys through MVCC WAL staging also regressed the
  current kept sync patch (`batch_create_1000` 1,636.52 OPS versus 1,678.16 OPS, and `batch_delete_100` 11,023.76 OPS
  versus 13,477.38 OPS). Replacing hash-based uniqueness with a strictly-ordered-key fast path also regressed
  `batch_create_1000` to 1,648.14 OPS and `batch_delete_1000` to 4,793.31 OPS. Replacing MVCC publish-data iterator
  construction with an explicit preallocated loop improved `batch_delete_1000` but regressed `batch_create_1000` to
  1,029.54 OPS, so it was reverted. Replacing `DeferredBatchPublish` refs-builder collection with an explicit
  preallocated loop also regressed `batch_create_1000` to 1,573.97 OPS and `batch_update_1000` to 1,602.41 OPS.
  Skipping `try_freeze_memtable()`'s state load when the just-written memtable was below threshold also regressed the
  sync batch rows (`batch_create_1000` 1,090.00 OPS, `batch_update_1000` 1,085.67 OPS, `batch_delete_1000` 2,097.61
  OPS), so it was reverted. Removing the WAL point-batch validated length vector was also rejected: same-window
  outside-sandbox sync A/B on 2026-07-15 moved `batch_create_1000` 1,682.54 → 1,682.08 OPS, but regressed
  `batch_update_1000` 1,724.53 → 1,162.76 OPS and `batch_delete_1000` 5,716.15 → 4,862.19 OPS. Replacing WAL
  submission chunk-range collection with a direct index loop was also rejected: same-window sync A/B moved
  `batch_create_1000` 1,074.83 → 1,565.98 OPS in a noisy baseline window, but regressed `batch_update_1000`
  1,587.33 → 1,563.96 OPS and `batch_delete_1000` 5,078.36 → 4,752.27 OPS. Increasing WAL fallocate granularity
  from 1 MiB to 16 MiB was a hard reject: same-window sync A/B moved `batch_create_1000` 1,726.79 → 983.89 OPS,
  `batch_update_1000` 1,674.46 → 629.46 OPS, and `batch_delete_1000` 4,989.86 → 1,481.66 OPS. Splitting
  `WriteBatchRecord` into separate key/value generic types so the `crud-bench` ToyKV adapter could use stack `[u8; 4]`
  integer keys was also rejected: three outside-sandbox focused sync A/B samples averaged `batch_create_1000`
  1,788.90 → 1,732.37 OPS (-3.2%) and `batch_delete_1000` 6,601.51 → 5,950.58 OPS (-9.9%), despite
  `batch_update_1000` moving 1,874.72 → 1,922.31 OPS (+2.5%).
  Kept sync-side follow-up: group-commit leaders now briefly delay only for solo WAL buffers at least 512 KiB, giving
  peer writers a chance to join the same `fdatasync` without taxing smaller writes. Same-window sync A/B moved
  `batch_create_100` 7,119.76 → 6,992.20 OPS, `batch_update_100` 7,041.70 → 7,878.78 OPS,
  `batch_delete_100` 10,356.25 → 11,011.69 OPS, `batch_create_1000` 1,609.98 → 1,695.84 OPS,
  `batch_update_1000` 1,174.04 → 1,198.38 OPS, and `batch_delete_1000` 4,000.73 → 4,630.94 OPS. A lower 128 KiB gate
  improved large batches more, but regressed `batch_update_100`, so it was rejected. Follow-up: switching the solo
  leader wait from `yield_now()` to `spin_loop()` kept the same durability semantics while avoiding scheduler handoff
  latency. Same-window sync A/B moved `batch_create_100` 3,085.46 → 6,738.57 OPS, `batch_update_100`
  2,220.15 → 5,991.32 OPS, `batch_delete_100` 3,789.87 → 11,160.01 OPS, `batch_create_1000`
  1,008.80 → 1,593.44 OPS, `batch_update_1000` 886.57 → 1,657.93 OPS, and `batch_delete_1000`
  2,039.08 → 4,079.75 OPS. Follow-up after merging the `crud-bench` ToyKV adapter: shortening the solo
  leader spin window from 8 to 4 iterations kept the same 512 KiB gate and improved the fresh focused sync run
  (`--samples 100000 --clients 4 --threads 4 --sync --skip-indexes --skip-scans`) from `batch_create_1000`
  1,661.72 to 1,679.79 OPS, `batch_update_1000` 1,582.71 to 1,593.73 OPS, and `batch_delete_1000`
  4,458.76 to 5,376.32 OPS. Increasing the spin window to 16 was rejected: the same run shape regressed
  `batch_create_1000` to 1,197.35 OPS and `batch_update_1000` to 1,551.00 OPS.
  Follow-up instrumentation: `write-perf --bench wal_concurrent --num 100000 --threads 4 --value-size 1024
  --profile --features bench` now reports WAL commit-group shape. The first profile showed `wal_sync` still at
  91.6% of profiled time, with 43,821 commit groups for 100,000 writes, 10.6% solo groups, 2.28 buffers per group on
  average, and a max group size of 4 buffers / 16 KiB. That points the next optimization away from blind solo-delay
  tuning and toward either larger effective commit groups or cheaper sync submission.
  Follow-up instrumentation split `wal_sync` into leader write submission, fdatasync, and follower barrier wait. A
  same-shape profile showed `wal_submit` at 312.39 ms, `fdatasync` at 8.41 ms, and `follower_wait` at 1,353.49 ms
  cumulative. The next optimization should target follower wake/wait overhead or reduce leader cycles per durable
  ticket group; fdatasync itself is not the bottleneck in this profile.
  Rejected follow-ups (2026-07-21): adding a 64-iteration follower spin before parking improved some sync write rows but
  failed the CRUD gate by regressing `batch_delete_1000` from 5,521.22 to 5,080.44 OPS (-8.0%) under
  `crud-bench --samples 100000 --clients 4 --threads 4 --sync --skip-indexes --skip-scans`. A smaller 16-iteration
  spin used the same WAL microprofile command as above (`write-perf --bench wal_concurrent --num 100000 --threads 4
  --value-size 1024 --profile`) and was worse than the same-day accepted control window (about 98.5K OPS) at
  `wal_concurrent` 92,942 OPS, `wal_sync` 1,771 ms, and `follower_wait` 1,330.92 ms, so it was not carried to CRUD.
  Moving `notify_all()` after the completion mutex unlock was also rejected under the same CRUD gate: it improved large
  create/update batches but regressed `batch_create_100` 7,909.75 → 6,675.72 OPS (-15.6%), `batch_delete_100`
  13,270.83 → 11,795.13 OPS (-11.1%), and `batch_delete_1000` 5,521.22 → 4,306.69 OPS (-22.0%).
  Follow-up rejected before CRUD: coalescing small multi-buffer commit groups into one 256 KiB direct buffer used the
  same 100,000-op / 4-thread / 1 KiB WAL microprofile command and made the refreshed PR-head control worse:
  `wal_concurrent` 92,929 → 91,484 OPS, `wal_sync` 1,798.27 → 1,831.11 ms, with the control commit-group shape at
  11.1% solo groups, 2.30 average buffers/group, and max 4 buffers / 16 KiB. Copying the aligned buffers costs more
  than the saved SQE/CQE work for this shape.
  Follow-up instrumentation: `write-perf --bench wal_batch --num 100000 --threads 4 --value-size 1024 --profile`
  now splits WAL write time into validate/prepare/encode/enqueue. The batch-size 100 profile still points at
  sync/follower wait (`wal_sync` 270.51 ms, `follower_wait` 174.09 ms), while the batch-size 1000 profile shows
  direct-buffer preparation dominating the WAL write bucket (`wal_prepare` 28.46 ms versus `wal_encode` 7.41 ms).
  The next large-batch target should inspect direct-buffer pool sizing/reuse before more encoding-loop changes.
  Rejected follow-up: retaining up to 2 MiB direct buffers while letting oversized allocations replace undersized
  256 KiB pool entries did not reduce large-batch prepare time and regressed `wal_batch_size=1000` to 468,984 OPS.
  Prefilling the pool with 2 MiB buffers removed `wal_prepare` but was a hard reject: the same profile collapsed to
  124,450 OPS, with `wal_sync`/`follower_wait` increasing despite lower write preparation time.
  Rejected follow-up: publishing `DeferredBatchPublish` by cloning its owned `Bytes` handles into the memtable instead
  of copying borrowed payloads cut profiled memtable time but was mixed end-to-end: same-window profile moved
  `wal_batch_size=1000` 405,409 → 460,487 OPS, but regressed `wal_batch_size=100` 169,555 → 140,555 OPS.
  Rejected follow-up: adding a separate, non-prefilled large DirectBuf pool for 512 KiB-2 MiB buffers reduced
  profiled `wal_prepare` on one large-batch run, but same-window control was faster: candidate `wal_batch_size=1000`
  445,582 OPS versus control 502,948 OPS. The small-batch case was neutral in the same window (candidate
  154,243-156,297 OPS versus control 154,749 OPS), so the reject is the large-batch throughput loss.
  Rejected follow-up: lowering `GROUP_COMMIT_MIN_SOLO_BYTES` from 512 KiB to 128 KiB to include
  `wal_batch_size=100` in the solo-leader spin path regressed the focused profile to 143,988 OPS and raised solo
  groups to 70.7%, so spinning smaller batches did not improve group formation.
  Accepted follow-up: raw DirectBuf encoding now skips full-buffer memset on allocation and initializes only the WAL
  header, encoded entries, and O_DIRECT padding. Focused `write-perf` improved `wal_batch_size=1000` to 1,175,368
  OPS with `wal_prepare` down to 0.52 ms, and `wal_batch_size=100` to 494,581 OPS. The focused CRUD sync gate
  `result-toykv_raw_directbuf_encode_pr189_sync_100k.csv` improved targeted durable batch write/delete rows versus
  `result-toykv_pr174_final_sync_100k.csv`: `batch_create_100` 6,583.98 -> 7,692.27 OPS, `batch_update_100`
  7,170.94 -> 7,305.25 OPS, `batch_delete_100` 10,679.07 -> 11,929.03 OPS, `batch_create_1000`
  1,245.03 -> 1,635.38 OPS, `batch_update_1000` 1,548.54 -> 1,829.53 OPS, and `batch_delete_1000`
  3,397.84 -> 5,383.25 OPS.
  Rejected follow-up: computing CRC incrementally while encoding only for WAL payloads >=512 KiB improved the
  `write-perf` large-batch microprofile (`wal_batch_size=1000` same-window control 736,265 OPS, candidate up to
  1,009,827 OPS) and kept `wal_batch_size=100` near control, but did not improve the focused CRUD sync gate versus the
  accepted PR #189 artifact. `result-toykv_inline_crc_pr190_sync_100k.csv` moved targeted rows to
  `batch_create_100` 7,667.52 OPS, `batch_update_100` 7,046.86 OPS, `batch_delete_100` 11,421.73 OPS,
  `batch_create_1000` 1,631.38 OPS, `batch_update_1000` 1,758.70 OPS, and `batch_delete_1000` 5,380.71 OPS, which is
  flat/slightly down against `result-toykv_raw_directbuf_encode_pr189_sync_100k.csv`.
  Kept follow-up: routing raw DirectBuf WAL payload encoding through a small cursor removed repeated manual offset
  updates in the hot loop without changing the encoded bytes. Focused `write-perf` moved `wal_batch_size=1000` to
  1,141,209 OPS, `wal_batch_size=100` to 584,235 OPS, and `wal_concurrent` to 179,615 OPS. The CRUD sync evidence was
  mixed and noisy rather than a clean durable-gate win: rerun `result-toykv_directbuf_cursor_pr190_sync_100k_rerun2.csv`
  recovered from an anomalous first run and beat the anomalously slow same-window baseline
  `result-toykv_pr189_control_for_cursor_sync_100k.csv` (`batch_create_1000` 1,764.33 / 1,174.21 OPS,
  `batch_update_1000` 1,791.25 / 934.30 OPS, `batch_delete_1000` 3,851.35 / 2,184.58 OPS), but remained mixed against
  the accepted PR #189 artifact: `batch_create_100` 7,692.27 -> 6,932.80 OPS, `batch_update_1000`
  1,829.53 -> 1,791.25 OPS, and `batch_delete_1000` 5,383.25 -> 3,851.35 OPS. Fresh rerun
  `result-toykv_directbuf_cursor_pr190_sync_100k_rerun3.csv` stayed mixed against PR #189: `batch_create_100`
  7,692.27 -> 6,457.65 OPS, `batch_update_100` 7,305.25 -> 7,530.86 OPS, `batch_delete_100`
  11,929.03 -> 12,492.57 OPS, `batch_create_1000` 1,635.38 -> 1,771.70 OPS, `batch_update_1000`
  1,829.53 -> 1,360.18 OPS, and `batch_delete_1000` 5,383.25 -> 4,739.29 OPS.
  Rejected follow-up: gating owned-`Bytes` memtable publish to deferred batches with at least 512 entries improved
  focused `write-perf` (`wal_batch_size=1000` 1,053,342 -> 1,542,212 OPS, memtable 99.31 -> 68.69 ms) and kept
  `wal_batch_size=100` neutral, but failed the same-window CRUD sync gate. Candidate reruns
  `result-toykv_large_owned_publish_pr191_sync_100k.csv` and
  `result-toykv_large_owned_publish_pr191_sync_100k_rerun2.csv` improved `batch_delete_1000` versus control
  `result-toykv_pr190_control_for_owned_publish_sync_100k.csv` (4,209.48 -> 5,211.11 / 5,130.56 OPS), but regressed
  `batch_update_1000` (1,811.29 -> 1,687.30 / 1,381.44 OPS), so the large-batch owned publish path is not a safe
  CRUD optimization. Follow-up rejected before CRUD: making staged batch key/value `Bytes` shared before the same
  owned-publish gate cut focused memtable time further (37.24 ms) but regressed `wal_batch_size=1000` throughput to
  791,198 OPS by increasing WAL write/sync time, so clone promotion was not the only problem. Follow-up rejected
  before CRUD: splitting the common v4/v3 WAL point-entry encode loop to avoid checking `is_v3` per entry regressed the
  refreshed large-batch profile to 797,762 OPS with `wal_encode` rising to 48.38 ms, so the branch was not the encode
  bottleneck. Follow-up rejected before CRUD: coalescing large-batch memtable bloom updates before skiplist insertion
  lowered the focused memtable bucket (99.31 -> 88.39 ms) but still regressed `wal_batch_size=1000` throughput to
  878,871 OPS, so the extra publish pass/scratch work did not pay off end-to-end. Follow-up rejected before CRUD:
  staging MVCC batch encoded keys in one temporary `BytesMut` slab reduced per-entry key allocations in theory, but the
  focused large-batch profile regressed to 747,154 OPS with higher WAL sync/submit time, so the shared-slab lifetime and
  extra staging work were not a win.
  Accepted follow-up: hot write paths now commit the caller's own WAL ticket instead of the memtable's latest ticket,
  so an earlier writer no longer waits for later tickets before publishing. Focused `write-perf` improved
  `wal_batch_size=100` from 468,367 to 661,830 OPS and `wal_batch_size=1000` from 795,221 to 1,057,081 OPS in the
  refreshed same-session profile, while `wal_concurrent` stayed effectively flat (161,309 -> 159,031 OPS). The
  same-window CRUD sync gate was strongly positive: control `result-toykv_ticket_commit_control_sync_100k.csv` versus
  candidate rerun `result-toykv_ticket_commit_pr193_sync_100k_rerun2.csv` moved `batch_create_100`
  4,480.67 -> 7,845.61 OPS, `batch_update_100` 2,063.21 -> 8,060.23 OPS, `batch_delete_100`
  3,658.38 -> 11,376.21 OPS, `batch_create_1000` 721.23 -> 1,385.87 OPS, `batch_update_1000`
  612.92 -> 1,824.51 OPS, and `batch_delete_1000` 1,775.83 -> 5,994.75 OPS.
  Final PR-head sync/no-sync comparison artifacts:
  `result-toykv_pr174_final_sync_100k.csv` and `result-toykv_pr174_final_nosync_100k.csv`. Same command shape
  (`--samples 100000 --clients 4 --threads 4 --skip-indexes --skip-scans`) shows durable batch writes remain below
  buffered mode: `batch_create_100` 6,583.98 / 18,522.37 OPS (35.5%), `batch_update_100` 7,170.94 / 25,770.59 OPS
  (27.8%), `batch_delete_100` 10,679.07 / 31,217.23 OPS (34.2%), `batch_create_1000` 1,245.03 / 2,534.93 OPS
  (49.1%), `batch_update_1000` 1,548.54 / 2,550.96 OPS (60.7%), and `batch_delete_1000` 3,397.84 / 7,703.75 OPS
  (44.1%). Read rows are effectively tied or better under sync: `batch_read_100` 48,687.01 / 49,568.01 OPS and
  `batch_read_1000` 6,618.30 / 5,590.60 OPS.
  Fair RocksDB sync rerun used the same `rocksdb,toykv` feature set for both binaries. Artifacts:
  `result-toykv_pr174_fair_sync_100k.csv` and `result-rocksdb_pr174_fair_sync_100k.csv`. The RocksDB adapter used
  `surrealdb-rocksdb 0.24.0-surreal.5`, mapping to raw RocksDB `11.0.0` through
  `surrealdb-librocksdb-sys 0.18.3+11.0.0-4`; latest upstream raw RocksDB was `11.1.2` when checked on 2026-07-16.
  Under the same sync command, ToyKV wins 11 of
  12 rows versus RocksDB: `Create` 13,350.01 / 13,275.94 OPS (+0.6%), `Read` 3,515,416.56 / 1,495,393.47 OPS
  (+135.1%), `Delete` 14,223.39 / 13,806.87 OPS (+3.0%), `batch_create_100` 5,564.77 / 1,710.81 OPS (+225.3%),
  `batch_read_100` 36,685.90 / 27,777.33 OPS (+32.1%), `batch_update_100` 5,653.76 / 1,590.98 OPS (+255.4%),
  `batch_delete_100` 11,340.75 / 4,741.47 OPS (+139.2%), `batch_create_1000` 1,497.21 / 413.55 OPS (+262.0%),
  `batch_read_1000` 5,719.06 / 5,011.33 OPS (+14.1%), `batch_update_1000` 1,532.62 / 369.53 OPS (+314.7%), and
  `batch_delete_1000` 4,547.50 / 318.29 OPS (+1328.7%). RocksDB is slightly ahead only on single-op `Update`.
- [x] **Add sync perf gates to the comparison workflow** — Track both absolute Fjall-relative OPS and
  sync/no-sync ratio for `put_c`, `batch_create_100`, `batch_create_1000`, `batch_delete_100`, and
  `batch_delete_1000`. Do not accept buffered-only improvements that regress sync production cases. Initial gates:
  no focused sync row regresses by more than 5%, sync/no-sync ratio improves for at least two of `put_c`,
  `batch_create_1000`, and `batch_delete_1000`, and single-client sync p95/p99 latencies on the same default rows each
  regress by no more than 5%.
  Implemented in the sibling `crud-bench` checkout using the `perf-gate` command; see `README.md` for the full
  arguments. The CSV schema is owned there.
- [x] **Add durable RocksDB comparison** — Ran the existing `crud-bench` embedded RocksDB backend alongside ToyKV and
  Fjall with `--sync --samples 100000 --clients 4 --threads 4`, then filled in
  `docs/bench-report-crud-bench-rocksdb.md`. ToyKV wins point reads and large durable batch writes; RocksDB wins
  scan rows and `batch_read_100` in the initial full run; the PR #170 focused scan rerun moves ToyKV ahead on four of
  five scan rows, while a 10,000-iteration focused batch rerun moves `batch_read_100` back ahead and keeps
  `batch_read_1000` ahead of RocksDB.
- [x] **Repeat remaining focused read gap** — PR #173 repeated the remaining `select(*) limit(100)` gap with
  `--sync --samples 100000 --clients 4 --threads 4 --skip-indexes --skip-batches`. ToyKV now leads RocksDB on all five
  focused no-index scan watch rows; the repeated `select(*) limit(100)` row is 646,130.69 vs 503,709.26 OPS
  (+28.3%). Keep the scan rows plus `batch_read_100` and `batch_read_1000` as regression watch rows.
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
