# MVCC / GC / Perf TODO

**RFC:** [rfcs/005-mvcc.md](rfcs/005-mvcc.md)
**Plan:** [.claude/plans/mvcc.md](.claude/plans/mvcc.md)

---

## RFC 021: Parallel Snapshot Scan Lifetime Redesign

**RFC:** [rfcs/021-public-snapshot-api.md](rfcs/021-public-snapshot-api.md)
**Status:** landed on `main` via PR #242.

**Design direction:** run the parallel-scan coordinator on the engine-owned
background runtime rather than the caller's Tokio runtime. The cursor can then
signal cancellation synchronously, while the coordinator continues draining
worker shutdown and retaining its MVCC/lifecycle pins independently of a
current-thread caller runtime.

- [x] Redesign parallel-scan lifetime ownership so a dropped cursor releases
  synchronous-close admission without relying on caller-runtime polling.
- [x] Keep the coordinator's exact-timestamp MVCC and lifecycle pins on the
  engine-owned background runtime until all workers exit.
- [x] Preserve the equivalent `SnapshotInner` lifetime contract for snapshot
  parallel scans, including Tokio runtime shutdown/cancellation.
- [x] Add deterministic nextest coverage for dropped live cursors and
  synchronous `close()` without caller-runtime polling.
- [x] Re-run the parallel-scan performance comparison and full CI before
  merging PR #242.

---

## RFC 022: Incremental Backup and Restore

**RFC:** [rfcs/022-incremental-backup.md](rfcs/022-incremental-backup.md)
**Implementation plan:** [docs/rfc-022-incremental-backup-plan.md](docs/rfc-022-incremental-backup-plan.md)
**Status:** RFC landed on `main` via PR #243; implementation is the next
follow-up.

- [x] RFC design: define immutable SST/vLog object identity and metadata-only reuse.
- [x] RFC design: define crash-consistent repository catalog, retention, verification, and
  restore contracts.
- [x] RFC design: define visible/durable/unknown publication outcomes and legacy manifest
  checksum migration.
### Implementation plan

#### 1. Foundation: manifest-v6 immutable-file identity

- [x] Add SHA-256 support plus `ImmutableFileKind` and serialized
  `ImmutableFileMetadata`, including exact-width ID validation at backup
  capture boundaries.
- [x] Implement full manifest-v6 migration and write-path enforcement.
- [ ] Persist metadata in `LsmStorageState` and `ManifestRecord::Snapshot`
  across every immutable write path and snapshot writer.
  Recovery now hydrates persisted metadata into live state, and state updates
  use canonical ordering/duplicate checks (foundation slice). Complete
  coverage validation is gated on manifest format v6 until all writers migrate.
  Flush publication now records newly created SST/vLog identities; point and
  range-only compaction removes old/adds new identities and persists them via
  CompactionV4 replay. vLog GC now removes/adds identities with
  deduplication and emits metadata-aware records for single and batch paths;
  background persistence failures are logged. Global v6 enforcement and legacy
  migration are complete; centralized publication remains pending.
- [x] Implement idempotent `ensure_manifest_v6()` legacy migration, with
  pinning, reconciliation, and atomic snapshot publication.
  Shared live-file hashing, snapshot backfill, and the durable v6 migration
  transaction are now in place.
- [ ] Centralize SST/vLog publication so flush, compaction, range-only SST
  creation, and vLog GC preserve the live-metadata invariant.
- [ ] Add durable synchronized runtime reclamation for rewritten vLog source
  files: retain the old file until rewritten pointers are flushed into SSTs
  and their manifest publication is durable, then retire and unlink it. Cover
  the crash window with `chaos_vlog` and repeated ASan/LSan runs.
  Compaction now queues retired source IDs only after durable publication and
  defers physical unlinking out of the immediate post-compaction GC task.
  A `VlogRetire` manifest record now records those durable retirement events;
  replay coverage for shared live files is now present; final crash-window
  coverage and automatic unlinking remain pending.

#### 2. Shared physical capture boundary

- [x] Extract RFC 019's flush, stable-state capture, canonical manifest
  snapshot, and SST/vLog pinning into a reusable internal helper.
- [x] Require an empty immutable-memtable set and exclude WAL and mutable
  `.vidx` files from the captured immutable set.

#### 3. Secure repository core

- [x] Add a Linux descriptor-relative (`openat`, `O_NOFOLLOW`,
  `renameat2`, `flock`) repository primitive layer and bootstrap lock.
- [x] Implement bounded, framed catalog encoding/replay and recovery for
  `HighWater`, `Prepare`, `Commit`, and semantic-corruption rejection.
- [x] Implement immutable object publication with copy/reuse, no-replace
  rename, source identity verification, and directory fsync.
- [x] Add hard-link publication fallback when requested by `BackupOptions`,
  while retaining copy semantics for cross-filesystem sources.

#### 4. Synchronous create, inspect, and restore

- [x] Implement the internal synchronous `create_backup`,
  `BackupRepository::open`, `list`, and structural `verify` paths, including
  metadata-only reuse and full-byte verification.
- [ ] Expose explicit durable, non-durable, and publication-unknown outcomes
  from the backup execution path, including `RepositoryPublishedButNotDurable`,
  `CommitPublishedButNotDurable`, and `CommitPublicationUnknown`.
- [ ] Implement validated, no-follow staged restore and reopen coverage across
  inline, WAL, vLog, range-tombstone, TTL, and serializable fixtures.
  Target absence/symlink validation and unique staging cleanup are implemented;
  object materialization, manifest staging, atomic publish, and basic reopen
  coverage are implemented; option compatibility and full fixture coverage remain pending.

#### 5. Retention

- [ ] Implement `purge(retain)` with `CatalogSnapshot` publication before
  generation/object reclamation and reference recomputation.
  Retained-generation and unreferenced-object analysis primitives are now
  available, including a read-only purge plan; catalog mutation and
  reclamation are now implemented with durable retention records and
  orphan-generation recovery, idempotent retries, and end-to-end coverage.
  Catalog snapshot compaction is implemented and publicly callable; failpoint-
  driven crash windows and retention snapshot migration remain pending; empty,
  post-purge, stale-temp recovery, non-UTF-8 entry, and compaction failpoint
  coverage is implemented.

#### 6. Async API

- [x] Add an initially thin blocking-executor wrapper over the proven sync
  implementation.
- [ ] Add the eagerly dispatched `BackupTask` cancellation state machine and
  exact-once terminal wake-up behavior.

#### 7. Verification gate

- [ ] Add deterministic failpoint, torn-tail, corruption, concurrency,
  cancellation, retention, restore/reopen, and byte-accounting coverage.
- [ ] Run `cargo make check` after all implementation phases are complete.

---

## RFC 018: Steady-State Comparison Follow-Up

**RFC:** [rfcs/018-steady-state-benchmark-suite.md](rfcs/018-steady-state-benchmark-suite.md)

### Completed

- [x] Add `point_read_uniform` with unbiased deterministic key sampling in the CRUD comparison harness
- [x] Add optional `idle` workload with timing-only result validation in the CRUD comparison harness
- [x] Add explicit cross-backend gate support and labeled comparison output

### Next: Transaction Contention

The ToyKV `write-perf` transaction workload is already implemented. The
remaining work belongs to the sibling `crud-bench` repository:

- [x] Define the common transactional operation contract in CRUD's `BenchmarkClient`
- [x] Integrate the existing ToyKV transaction workload through the CRUD adapter
- [x] Configure serializable ToyKV transactions without changing ordinary CRUD semantics
- [x] Add the RocksDB optimistic-transaction adapter and conflict classification
- [x] Add `transaction_contention` scheduling, retries, and latency metrics
- [x] Extend steady-state JSON and perf-gate validation with transaction counters
- [x] Add deterministic tests and ToyKV/RocksDB smoke comparisons
- [x] Document commands and acceptance thresholds

The repeated two-client comparison used ten transaction retries, a 5-second
warmup, and a 15-second measurement window. All six runs had zero validation
errors and zero read misses. Median committed transaction throughput was
1,737.30 OPS for ToyKV and 1,706.53 OPS for RocksDB; see the RFC 018 report
for the full repeated-run table and conflict counts.

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
  Structural follow-up: prototype alternate ordered concurrent memtable backends before doing more publish-loop
  micro-edits. Current profiles show `SkipMap` insertion dominates large delete publish and is still a major part of
  create/update publish. Candidate crates to benchmark first: `scc::TreeIndex` (concurrent B+ tree with range
  iteration), `concurrent_map` (sled-derived lock-free B+ tree), `skl` (skiplist crate aimed at LSM/MVCC memtables),
  and `arctic-map` (lock-free adaptive radix tree with ordered range scans). Keep the existing `crossbeam_skiplist`
  backend as the control, require point get + range scan + flush correctness, and judge candidates on same-window
  `crud-bench` sync `batch_create_1000`, `batch_update_1000`, and `batch_delete_1000`.
  First production-swap prototype rejected: moving the memtable point map to `scc::TreeIndex` with a vector-backed scan
  snapshot passed focused correctness checks, and the ordered microbench had looked promising, but the external sync
  CRUD gate regressed large create/update versus the kept branch (`batch_create_1000` 1,344.90 OPS,
  `batch_update_1000` 1,634.01 OPS, `batch_delete_1000` 5,392.91 OPS). Keep the benchmark-only evidence, but do not
  retry this shape without a non-cloning range iterator and same-window CRUD control.
  Second `scc::TreeIndex` production-swap prototype also rejected: replacing the full-range scan clone with a `Send`
  cursor iterator avoided storing `scc::Guard` across async boundaries and passed `cargo test --package kv-engine
  memtable` outside sandbox, but external sync CRUD exposed a large point-read regression (`batch_read_1000` collapsed
  to 17.16 OPS) even though publish-heavy rows looked decent (`batch_create_1000` 1,736.51 OPS,
  `batch_update_1000` 1,826.38 OPS, `batch_delete_1000` 5,756.76 OPS). Treat TreeIndex as rejected for the production
  memtable unless the lookup path can avoid per-read guarded B-tree range seeks. Next structural candidate:
  `arctic-map`.
  `arctic-map` API read: defer the production memtable prototype until there is a dedicated key adapter. Its
  `ConcurrentMap` gives lock-free writes, wait-free point reads, and ordered scans, but dynamic byte keys must satisfy
  the crate's prefix-property wrappers (`NonNull` or `Terminated<N>`). ToyKV internal keys are arbitrary bytes:
  memcomparable user-key padding deliberately includes `0x00`, and the inverted timestamp suffix can contain any byte.
  That makes `NonNull` invalid and makes `Terminated<N>` invalid without an order-preserving escaping/termination layer
  or a custom unsafe `Key` proof. A safe `arctic-map` attempt should first design and benchmark that key adapter;
  otherwise the benchmark would measure a different key space or rely on unsound unchecked construction.
  `skl` API/safety and production-swap prototype rejected: the crate is arena-backed and has built-in MVCC, but ToyKV's
  memtable already encodes MVCC in the internal key, so the plausible swap used `generic::unique::sync::SkipMap<[u8],
  [u8]>`. That shape compiled and passed focused correctness (`memtable` 86/86 outside sandbox, `write_batch` 23/23,
  and `scan` 116/116 outside sandbox), but the practical gate failed: a 64 MiB heap arena exhausted during the 100k x
  1 KiB focused CRUD profile, and raising the arena to 256 MiB made the same `crud_phase_batch` profile fail to produce
  rows after roughly two minutes before it was killed. Do not keep the `skl` production swap unless there is first a
  bounded/dynamic arena design and a profile proving scan/insert does not stall the publish workload. Next structural
  candidate: inspect `concurrent_map`.
  `concurrent-map` API rejected before production swap: the crate provides a lock-free linearizable B+ tree and returns
  owned `(K, V)` pairs from range iteration, but its `ConcurrentMap` handle is explicitly `Send` and not `Sync` because
  each cloned handle owns local EBR state. ToyKV shares `MemTable` through `Arc` across write/read/flush paths, so
  replacing the point map with a single `ConcurrentMap` handle would make `MemTable` not `Sync`. A mutex wrapper would
  benchmark a serialized B+ tree rather than the intended lock-free publish path, and an unsafe `Sync` wrapper would
  bypass the crate's reclamation contract. Do not prototype this backend unless the design first gives each accessing
  thread its own cloned handle without weakening `MemTable`'s sharing model.
  Rejected follow-ups: consuming the prepared entry vector in MVCC WAL staging regressed `batch_delete_1000`, and fusing
  point key validation into entry construction was too noisy/regressive in rerun (`batch_create_1000` fell to 1,099.95
  OPS). Carrying precomputed user-key bloom hashes through deferred publish also regressed sync `batch_create_1000`
  (1,182.16 OPS) and `batch_delete_1000` (3,932.22 OPS). Borrowing user keys through MVCC WAL staging also regressed the
  current kept sync patch (`batch_create_1000` 1,636.52 OPS versus 1,678.16 OPS, and `batch_delete_100` 11,023.76 OPS
  versus 13,477.38 OPS). The narrower retry that only borrowed public `WriteBatchRecord` keys until MVCC built owned
  internal keys looked good in focused `crud_phase_batch`, but the external same-window CRUD sync gate rejected it:
  `toykv_borrow_point_keys_control_sync_100k` versus `toykv_borrow_point_keys_candidate_sync_100k` moved
  `batch_create_1000` flat at 1,732.50 -> 1,736.67 OPS, regressed `batch_update_1000` 1,631.92 -> 1,262.48 OPS, and
  regressed `batch_delete_1000` 5,316.40 -> 3,996.11 OPS, so it was reverted in `0f1cc87`. Replacing hash-based
  uniqueness with a strictly-ordered-key fast path also regressed
  `batch_create_1000` to 1,648.14 OPS and `batch_delete_1000` to 4,793.31 OPS. Replacing MVCC publish-data iterator
  construction with an explicit preallocated loop improved `batch_delete_1000` but regressed `batch_create_1000` to
  1,029.54 OPS, so it was reverted. Replacing `DeferredBatchPublish` refs-builder collection with an explicit
  preallocated loop also regressed `batch_create_1000` to 1,573.97 OPS and `batch_update_1000` to 1,602.41 OPS.
  Skipping `try_freeze_memtable()`'s state load when the just-written memtable was below threshold also regressed the
  sync batch rows (`batch_create_1000` 1,090.00 OPS, `batch_update_1000` 1,085.67 OPS, `batch_delete_1000` 2,097.61
  OPS), so it was reverted. Removing the WAL point-batch validated length vector was also rejected: same-window
  outside-sandbox sync A/B on 2026-07-15 moved `batch_create_1000` 1,682.54 → 1,682.08 OPS, but regressed
  `batch_update_1000` 1,724.53 → 1,162.76 OPS and `batch_delete_1000` 5,716.15 → 4,862.19 OPS. Replacing WAL
  point-batch validation's first length-check pass with validation only in the existing encoded-length collection was
  also rejected: focused `crud_phase_batch` control stayed faster than the candidate on create/update/delete
  (`batch_create` 1,901,929 vs 1,771,400 OPS, `batch_update` 1,842,048 vs 1,680,386 OPS, `batch_delete` 4,050,344 vs
  3,434,640 OPS), while the external CRUD control window was too noisy to use. Replacing WAL submission chunk-range
  collection with a direct index loop was also rejected: same-window sync A/B moved
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
  Follow-up instrumentation: `write-perf --bench crud_phase_batch --num 100000 --threads 4 --value-size 1024
  --wal-batch-size 1000 --profile` now runs the CRUD phase shape inside the repo and reports create/update/delete
  batch profiles after the single-row create/update/delete warmup. A same-branch rerun measured create/update at
  1.60M/1.62M OPS with similar profile shape (`batch_build` 51-53 ms, `wal_encode` 33-34 ms, `wal_sync` 43-44 ms,
  `memtable` 52-56 ms), while delete measured 3.75M OPS and stayed memtable-bound (`memtable` 66 ms, `wal_write`
  1.62 ms). This confirms that further group-commit delay tuning is the wrong target for large batch rows: the
  workload still forms almost entirely solo 1 MiB commit groups, and delete is dominated by SkipMap publication.
  Rejected follow-ups after this instrumentation: dedicated delete publish paths, precomputed publish hashes, borrowed
  user keys through MVCC staging, consuming prepared `Bytes` into deferred publish, and retuning solo-delay constants
  each regressed at least one CRUD batch row. The next worthwhile production slice should be a larger write-batch/WAL
  format change that avoids building publish data and then re-encoding equivalent WAL payload bytes, not another
  localized loop micro-edit. A first owned-large-batch WAL attempt that skipped the unused self-referencing borrowed-ref
  view for batches >=512 improved the in-repo CRUD-phase profile and large CRUD rows, but failed the focused sync CRUD
  gate twice by regressing `batch_create_100`: same-window rerun control/candidate was 6,578.23 -> 3,126.48 OPS
  (-52.5%), while `batch_create_1000` only moved 1,651.22 -> 1,678.63 OPS (+1.7%),
  `batch_update_1000` 1,606.71 -> 1,821.36 OPS (+13.4%), and `batch_delete_1000` 4,518.24 -> 4,750.70 OPS (+5.1%).
  That path was reverted; the next attempt needs a correctness-preserving format/API change that does not perturb
  smaller batch scheduling.
  Lowering the owned-publish threshold from 512 to 100 was also rejected. It removed the in-repo batch-100 publish-copy
  bucket (`create` publish copy 57.07 ms -> 1.42 ms; `create` 1.50M -> 1.63M OPS, `update` 1.64M -> 1.75M OPS,
  `delete` 2.30M -> 3.82M OPS), but failed the focused sync CRUD gate against the same-window control:
  `batch_create_100` 6,578.23 -> 5,853.75 OPS (-11.0%), while `batch_update_100` and `batch_delete_100` improved.
  Keep the threshold at 512 unless a full CRUD rerun proves the scheduling interaction has been fixed.
  Follow-up instrumentation added `write-perf --bench crud_batch_create_100`, matching the default CRUD batch-create row
  more closely: 250 timed batches of 100, ordered integer keys encoded with `u32::to_ne_bytes`, ToyKV adapter options,
  and the single-row create/update/delete warmup before timing. Baseline profile measured 946,035 OPS with 226 commit
  groups, 89.8% solo groups, 21.33 ms WAL sync, and 65.25 ms memtable publish. A temporary threshold-100 rerun still
  improved locally to 1,020,771 OPS while eliminating publish copy (23.02 ms -> 0.43 ms), so the standalone in-repo
  repro does not explain the external `crud-bench batch_create_100` regression. Next evidence should come from the
  external adapter/full-run path, not more local threshold tuning.
  Follow-up external instrumentation: with the ToyKV `bench` feature enabled,
  `TOYKV_WRITE_BATCH_PROFILE_EVERY=250 cargo run --release --no-default-features --features 'toykv toykv/bench' --bin
  crud-bench -- -d toykv -s 100000 -c 4 -t 4 --sync --skip-indexes --skip-scans` now emits one ToyKV write-batch
  profile window per default CRUD batch write row. Clean current rerun (`toykv_profile_windows_current_rerun2`) measured
  `batch_create_100` at 7,438.61 OPS with 36 commit groups for 250 batch calls, 2.8% solo groups, 353.43 ms accumulated
  WAL sync, 322.71 ms follower wait, 49.06 ms memtable publish, and 7.14 ms publish copy. A temporary threshold-100
  rerun (`toykv_profile_windows_threshold100`) reproduced the external failure: `batch_create_100` fell to 1,331.60 OPS,
  while WAL sync/follower wait jumped to 2,539.31/2,369.65 ms and memtable publish rose to 112.46 ms even though publish
  copy fell to 1.07 ms. This confirms the threshold change disrupts concurrent commit scheduling enough to swamp the
  saved copy. Reverted; keep the env logger as the next external-gate diagnostic.
  Rejected follow-up: gating WAL `notify_all()` on a counted condvar waiter set was neutral for `batch_create_100`
  (7,438.61 -> 7,375.77 OPS) and helped `batch_create_1000` slightly (1,656.03 -> 1,690.74 OPS), but badly regressed
  `batch_update_1000` (1,658.15 -> 1,086.10 OPS) and `batch_delete_1000` (5,163.59 -> 4,626.49 OPS). The profile showed
  worse update/delete follower wait and solo-group shape, so unconditional wakeup remains the safer commit barrier.
  Rejected follow-up: extending the large-buffer leader spin to wait for small groups below 4 buffers regressed the
  external gate. `batch_create_100` fell 7,438.61 -> 6,022.87 OPS, `batch_create_1000` fell 1,656.03 -> 1,157.31 OPS,
  and `batch_delete_1000` fell 5,163.59 -> 4,051.59 OPS. The candidate raised solo-group share and follower wait on
  large create/delete, so the existing solo-only wait remains better.
  Rejected external-adapter follow-up: changing the `crud-bench` ToyKV u32 batch adapter to store stack `[u8; 4]` keys
  plus encoded value `Vec`s, then call `write_batch` with borrowed `WriteBatchRecord<&[u8]>`, helped small create/update
  rows but regressed large create. Same-window profile moved `batch_create_100` 7,438.61 -> 8,059.50 OPS and
  `batch_update_100` 5,724.03 -> 7,467.48 OPS, but `batch_create_1000` fell 1,656.03 -> 1,538.30 OPS and
  `batch_delete_1000` was slightly lower at 5,163.59 -> 5,056.26 OPS. The ToyKV-internal `batch_build` bucket did not
  improve for large create, so caller key-Vec allocation is not the right next durable-batch target.
  Rejected follow-up before external CRUD: adding a native-endian `u32` monotonic-key uniqueness proof for ToyKV's
  external adapter shape avoided the hash-set dedup pass for `u32::to_ne_bytes()` batch keys while preserving duplicate
  fallback, but the focused current-head `crud_phase_batch` profile regressed create/update
  (`batch_create_after_crud_phase` 1,748,290 -> 1,706,555 OPS, `batch_update_after_crud_phase` 1,948,903 ->
  1,732,647 OPS) and only helped delete versus that one baseline. Do not add more key-shape uniqueness fast paths
  without an external profile showing `batch_build` as the limiting bucket.
  Follow-up profiling split approximate-size accounting out of memtable publish map time. Review cleanup narrowed the
  accounting bucket to the batch-level relaxed `approximate_size.fetch_add`, leaving per-entry length accumulation
  untimed so the profile does not mostly measure `Instant::now()` overhead. Do not optimize approximate-size bookkeeping
  further unless this narrower bucket becomes material; the remaining durable-batch targets are SkipMap publish cost and
  WAL group/wait shape.
  Rejected follow-up: hashing the decoded user key directly from the memcomparable internal-key prefix avoided
  materializing the user key for bloom insertion, and helped the local CRUD-shaped write-perf profile, but failed the
  external CRUD gate. `toykv_direct_bloom_hash_candidate` moved `batch_create_100` only flat (7,438.61 -> 7,437.06
  OPS), regressed `batch_create_1000` (1,656.03 -> 1,611.07 OPS), and badly regressed `batch_update_1000`
  (1,658.15 -> 1,349.01 OPS), despite improving `batch_delete_1000` (5,163.59 -> 5,260.20 OPS). The prior decode path
  is safer.
  Rejected follow-up: reversing large sorted owned publish batches before `SkipMap` insertion was intended to test
  whether insertion order was the hidden cost, but it badly disrupted the external CRUD gate. `toykv_reverse_sorted_publish_candidate`
  moved `batch_create_100` 7,438.61 -> 2,507.90 OPS, `batch_update_100` 5,724.03 -> 1,551.57 OPS,
  `batch_delete_100` 11,854.17 -> 2,931.01 OPS, `batch_create_1000` 1,656.03 -> 700.37 OPS,
  `batch_update_1000` 1,658.15 -> 833.46 OPS, and `batch_delete_1000` 5,163.59 -> 1,926.97 OPS. The profile showed
  both `publish_skipmap_ms` and `follower_wait_ms` exploding, so sorted-order manipulation is not a viable local fix.
  Rejected follow-up before external CRUD: serializing large owned memtable publish batches behind a per-memtable mutex
  reduced focused create publish cost, but removed useful parallelism for update/delete. Same-session
  `crud_phase_batch` moved `batch_create_after_crud_phase` 1,748,290 -> 1,827,055 OPS, but regressed
  `batch_update_after_crud_phase` 1,948,903 -> 1,711,812 OPS and collapsed `batch_delete_after_crud_phase`
  3,120,418 -> 779,426 OPS. Do not serialize SkipMap publish without a delete-specific design.
  Next direction after the backend/prototype sweep: stop local staging, key-shape, and serialization micro-edits until a
  larger design changes the durable batch shape. External profile-window artifact
  `result-toykv_profile_windows_current_after_backend_sweep` shows large create/update dominated by WAL group
  wait/submit plus SkipMap publish, while large delete is mostly SkipMap-publish bound. The next useful PR should either
  design a WAL/publish representation that avoids building publish data and then re-encoding equivalent WAL payload
  bytes without moving that work under `write_lock`, or design a delete-specific tombstone publication/index path that
  avoids one SkipMap insertion per tombstone. Gate any candidate directly with external `crud-bench` profile windows
  before accepting it.
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
  `--sync --samples 100000 --clients 4 --threads 4 --skip-indexes --skip-batches`. The 2026-08-01 focused scan rerun
  after the count-only iterator optimization keeps ToyKV ahead of RocksDB on all focused no-index scan watch rows:
  `count()` is 683.48 vs 431.77 OPS (+58.3%) and `select(*) limit(100)` is 515,849.87 vs 510,486.54 OPS (+1.1%).
  Keep the scan rows plus `batch_read_100` and `batch_read_1000` as regression watch rows.
  Rejected follow-up: for non-MVCC batches with no range tombstones, forcing the direct small-batch lookup path for all
  batch sizes avoided sorting/context setup but regressed the external CRUD sync `batch_read_1000` row
  (6,294.15 -> 5,635.68 OPS). `batch_read_100` improved in that noisy run (30,086.96 -> 53,194.00 OPS), but the large
  batch regression means the sorted large-batch path still helps enough to keep.
- [x] **Ticket-based group commit** — Replace CAS-based leader election with ticket/sequence design to eliminate
  O(N) leader-election cascade. Assign monotonic ticket on `put_batch`, leader drains queue + records max ticket,
  sets `durable_sequence` atomic after I/O. Followers check `durable_sequence >= my_ticket` and return immediately
  without touching CAS. Avoids N-1 wasted empty-bufs leader elections after each real commit. Suggested by
  gemini-code-assist in PR #134 review.
  Rejected follow-up: precomputing decoded-user-key bloom hashes for large owned MVCC publish avoided the owned
  publish decode path, but moved work earlier and worsened the same-window focused CRUD-phase profile. Control versus
  candidate was `batch_create_after_crud_phase` 1,794,006 -> 1,839,480 OPS (+2.5%), `batch_update_after_crud_phase`
  2,062,548 -> 1,741,810 OPS (-15.6%), and `batch_delete_after_crud_phase` 3,667,005 -> 3,428,113 OPS (-6.5%).
  The next concurrency-oriented write-path slice should not simply remove `write_lock`: it needs a separate timestamp
  allocator plus a contiguous published-ts visibility frontier, otherwise readers can take a high `read_ts` while a
  lower timestamp is still unpublished and then observe a non-repeatable snapshot when that lower timestamp appears.
  Rejected follow-up: implementing that allocator/frontier directly with `next_ts`, a completed timestamp set, and
  skipped timestamps on WAL/publish errors improved the focused in-repo CRUD-phase profile
  (`batch_create_after_crud_phase` 1,794,006 -> 1,844,719 OPS, `batch_update_after_crud_phase` 2,062,548 -> 2,188,471
  OPS, `batch_delete_after_crud_phase` 3,667,005 -> 4,915,413 OPS), but failed the external CRUD sync gate. First
  external run was mixed (`batch_create_1000` 1,673.58 -> 1,678.56 OPS, `batch_update_1000` 1,784.42 -> 1,760.23 OPS,
  `batch_delete_1000` 4,332.19 -> 4,146.61 OPS, `batch_delete_100` 12,419.88 -> 11,342.41 OPS), and rerun collapsed
  batch scheduling (`batch_create_1000` 872.61 OPS, `batch_update_1000` 734.67 OPS, `batch_delete_1000` 1,989.73
  OPS). Keep the serialized timestamp staging unless a later design can prove stable external grouping behavior.
  Rejected follow-up before CRUD: adding a pending-arrival condvar and waiting up to 25us for a peer before draining a
  single WAL buffer of at least 512 KiB from a solo focused group kept most focused groups as single-buffer groups
  while adding latency. The same-window focused profile moved
  `batch_create_after_crud_phase` 1,794,006 -> 1,578,814 OPS, `batch_update_after_crud_phase` 2,062,548 -> 1,718,337
  OPS, and `batch_delete_after_crud_phase` 3,667,005 -> 2,899,435 OPS, so time-based leader waiting is the wrong
  scheduler direction without a stronger admission signal.
  Rejected follow-up before CRUD: opportunistically draining late pending WAL buffers after the leader's first write
  wave but before fdatasync improved focused create (`batch_create_after_crud_phase` 1,794,006 -> 1,968,388 OPS) and
  formed some larger commit groups, but regressed focused update/delete (`batch_update_after_crud_phase` 2,062,548 ->
  1,730,213 OPS, `batch_delete_after_crud_phase` 3,667,005 -> 3,288,824 OPS). The extra submit work/follower wait is
  not a broad win while publish remains the dominant delete cost.
  Rejected follow-up: retaining large DirectBufs in the existing WAL buffer pool confirmed churn but was not a safe
  win. Bench-only counters showed 1 MiB create/update batches popped undersized 256 KiB buffers and allocated new
  DirectBufs, while delete reused the small pool. Letting undersized buffers drop and retaining buffers up to 2 MiB
  warmed the pool for later large batches, but retention-only focused results regressed update/delete
  (`batch_update_after_crud_phase` 2,062,548 -> 1,702,466 OPS, `batch_delete_after_crud_phase` 3,667,005 ->
  3,262,991 OPS). The external CRUD sync probe was mixed and raised memory to ~1.1 GiB; `batch_delete_1000` improved
  to 5,073.75 OPS, but `batch_update_100` regressed to 6,122.49 OPS and `batch_update_1000` to 1,697.31 OPS. Do not
  retain oversized buffers in the shared pool without a more selective policy.
  Follow-up: large delete-only MVCC batches now use a key-only staging path before WAL encoding, avoiding construction
  of empty value buffers and per-entry kind tuples for the common `batch_delete_1000` CRUD shape while leaving smaller
  delete batches and serializable writes on the existing path. Same-window external CRUD sync comparison moved
  `batch_delete_1000` 4,680.46 -> 4,930.22 OPS; unrelated create/update rows were noisy and are not the target signal
  for this delete-only route.
  Rejected follow-up: replacing `DeferredBatchPublish`'s always-built borrowed-ref cache with on-demand refs plus a
  WAL encode path over owned `Bytes` entries did not produce a stable external CRUD win. The all-owned WAL variant
  improved `batch_create_100` (7,480.56 -> 8,263.38 OPS), `batch_update_100` (6,988.44 -> 7,633.13 OPS), and
  `batch_update_1000` (1,684.61 -> 1,806.74 OPS), but regressed `batch_create_1000` (1,805.39 -> 1,744.04 OPS) and
  `batch_delete_1000` (4,929.18 -> 4,615.44 OPS). A value-carrying large-batch-only variant avoided the first
  `batch_create_100` outlier on rerun and moved `batch_create_1000` to 1,815.19 OPS once, but confirmation collapsed
  `batch_create_1000` to 1,396.24 OPS and left `batch_delete_1000` at 5,088.48 OPS. Do not retry this ref-cache-only
  staging shape; the next WAL format attempt needs to remove encoded payload duplication, not merely change how refs
  are passed into the existing encoder. A conservative prepared-DirectBuf prototype that kept publish `Bytes`
  ownership but filled the WAL payload while building MVCC publish entries also failed the same-session focused gate:
  `batch_create_after_crud_phase` fell 1,901,083 -> 1,573,896 OPS while update was only flat and delete stayed flat.
  The WAL encode work disappeared from `wal_write`, but the copy cost moved into MVCC staging and hurt the measured
  create path. Do not keep a prepared-WAL path unless it actually removes a copy/owned payload, not just moves it.
  A direct public `write_batch` MVCC staging prototype also regressed badly (`batch_create_after_crud_phase` 1,901,083
  -> 875,738 OPS, `batch_update_after_crud_phase` 1,907,356 -> 535,094 OPS, `batch_delete_after_crud_phase` 3,121,189
  -> 2,674,529 OPS). It skipped the temporary user-key `Bytes`, but had to build commit-ts-bearing internal keys under
  `mvcc.write_lock`, serializing work that previously ran in parallel before timestamp assignment. Do not move
  large-batch entry construction under `write_lock`; a direct-staging design needs a separate timestamp reservation
  mechanism or another way to keep pre-WAL preparation parallel. Narrowing the pre-encoded staging path to delete-only
  batches and folding delete-only detection into validation looked good in the focused gate
  (`batch_delete_after_crud_phase` 3,121,189 -> 3,665,831 OPS while create/update held), but failed the real external
  CRUD gate: same-session `main` beat the candidate on every batch write row, including `batch_delete_1000` 4,680.46
  -> 2,119.97 OPS. Do not keep this pre-encoded delete-only staging path.

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
