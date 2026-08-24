# RFC 019: Checkpoint and Backup API

**Status:** Implemented
**Date:** 2026-08-23
**Author:** kv-engine Contributors
**References:**
- RFC 001: Key-Value Separation
- RFC 005: MVCC
- RFC 010: Delete Range
- RFC 013: Chaos Testing
- RFC 014: Async Operations
- RFC 016: Native Time-To-Live Support
- RFC 017: Standalone MVCC Garbage Collection
- `docs/bench-report-crud-bench-rocksdb.md`

---

## 1. Summary

This RFC proposes a user-visible checkpoint and backup API for kv-engine. A
checkpoint is a consistent, reopenable copy of a live database directory at a
specific engine state. The full feature exposes:

```rust
impl KvEngine {
    pub fn create_checkpoint(&self, target_dir: impl AsRef<Path>) -> Result<CheckpointStats>;
    pub async fn create_checkpoint_async(
        &self,
        target_dir: impl AsRef<Path> + Send,
    ) -> Result<CheckpointStats>;
}
```

The implementation exposes both the synchronous API and async wrappers. The
async path delegates to the engine-owned blocking executor so checkpoint I/O
does not run on async worker threads.

The checkpoint is published atomically for a new target:

```text
target_dir.checkpoint-<attempt>.staging/  marker is created here first
target_dir.checkpoint-<attempt>.tmp/      build here
target_dir/          final reopenable checkpoint
```

The implementation uses a conservative synchronous algorithm:

1. block new writes using the existing write/freeze coordination;
2. sync the active WAL and flush all memtables so the checkpoint only needs SST
   and manifest files;
3. take a stable `LsmStorageState` snapshot and collect the exact live file set;
4. pin those live SST files so compaction cannot delete them mid-copy;
5. create and fsync `CHECKPOINT_IN_PROGRESS` in a staging directory whose name
   is excluded from retry cleanup, then no-replace rename it to
   `target_dir.checkpoint-<attempt>.tmp`;
6. hard-link or copy every referenced file into
   `target_dir.checkpoint-<attempt>.tmp`;
7. write a checkpoint-local manifest snapshot that names that state;
8. fsync copied files, manifest files, and directories;
9. atomically rename the temporary directory to `target_dir`;
10. release the file pins.

The same algorithm supports vLog databases by pinning and copying referenced
value-log and value-log index files.

The output directory can be opened with `KvEngine::open()` using the same
options that are compatible with the source database format. The checkpoint is
read/write after open: it is an independent database copy, not a live view into
the source.

This feature closes a practical gap with RocksDB-style operational workflows.
It does not target CRUD benchmark speed; it targets safe backup, debugging,
reproducibility, and production-like administration.

---

## 2. Motivation

kv-engine already has a strong set of storage-system features: WAL durability,
MVCC, compaction, value separation, range tombstones, TTL, async APIs, chaos
tests, and benchmark coverage. Against the current embedded RocksDB comparison,
ToyKV is already competitive or ahead on the measured CRUD rows. The next useful
RocksDB-parity work is therefore operational maturity rather than another
point-read or batch-write micro-optimization.

RocksDB users commonly rely on checkpoints and backups to:

1. take consistent local snapshots without shutting down the database;
2. seed replicas or benchmark fixtures;
3. archive a known-good state before risky maintenance;
4. capture a compact debugging artifact for corruption or performance issues;
5. run offline tools without holding locks in the live process.

Today, kv-engine users can only approximate this by closing the engine and
copying the directory. That is too coarse:

1. it forces downtime;
2. it is easy to copy an inconsistent mix of old and new files;
3. it does not define what to do with vLog files, `.vidx` files, WAL files, or
   `MANIFEST_SNAPSHOT`;
4. it does not integrate with crash testing;
5. it cannot be used by benchmarks that need a stable prepared dataset while a
   source database stays open.

Adding a checkpoint API gives kv-engine a bounded, high-value feature that
exercises the manifest/state model without destabilizing hot paths.

---

## 3. Goals

1. Expose a synchronous checkpoint API on `KvEngine`.
2. Expose an async wrapper through the existing engine-owned blocking executor.
3. Produce a directory that can be opened independently by `KvEngine::open()`.
4. Support databases with SSTs, WAL, MVCC metadata, range tombstones, TTL
   metadata, compaction filters, value-log files, and value-log indexes before
   the RFC is considered fully implemented.
5. Avoid copying unreferenced SST/vLog files into the checkpoint.
6. Publish the checkpoint atomically: callers either see no checkpoint or a
   complete checkpoint.
7. Leave an interrupted checkpoint in a recognizable temporary directory that
   can be removed safely.
8. Prefer hard links for large immutable files when source and target are on
   the same filesystem, with copy fallback.
9. Keep the source database usable after checkpoint creation.
10. Add deterministic tests, including crash-window tests through the existing
    chaos failpoint infrastructure.
11. Add enough stats to make checkpoint behavior observable in tests and
    benchmark setup.

---

## 4. Non-Goals

1. **Remote backup storage.** The MVP writes to a local filesystem path only.
   S3, object stores, streaming upload, and encryption are future work.
2. **Incremental backups.** Every checkpoint is self-contained.
3. **Point-in-time restore from WAL.** The MVP flushes current memtables and
   does not include WAL replay as part of the checkpoint contract.
4. **Hot-copying active WAL files.** The MVP avoids this by flushing all
   memtables before collecting files.
5. **A read-only checkpoint mode.** The checkpoint can be opened and mutated as
   an independent database.
6. **Cross-version format conversion.** A checkpoint preserves the source
   format version.
7. **Snapshot isolation API for user reads.** This RFC only covers physical
   checkpoint directories. Named user snapshots are separate future work.
8. **Online backup while avoiding all write stalls.** The MVP may briefly block
   writes while it syncs and freezes the checkpoint source state.

---

## 5. Existing Building Blocks

### 5.1 State Publication

`LsmStorageState` already describes the live storage state:

```text
memtable
imm_memtables
l0_sstables
levels
range_only_ssts
sstables
max_sst_ts
has_sst_range_tombstones
has_sst_ttl_entries
```

Readers load state through `ArcSwap`, while flush and compaction publish new
state under `state_lock`. This gives checkpoint creation a natural way to
capture a stable list of live SST IDs.

### 5.2 Manifest Snapshots

The manifest module already supports a recovery-oriented `MANIFEST_SNAPSHOT`.
That file is internal compaction metadata for shortening manifest replay. It is
not a user-visible checkpoint.

The checkpoint implementation should reuse the same `ManifestRecord::Snapshot`
shape, but the output checkpoint must have its own `MANIFEST` and
`MANIFEST_SNAPSHOT` files inside the target directory. The source manifest is
not copied blindly because it may contain historical records that reference
deleted files or post-snapshot state.

### 5.3 Immutable Files

SST files are immutable after publication. vLog files are append-only while a
builder owns them, and they become immutable once referenced by a published SST.
`.vidx` files are derived companion indexes for vLog GC and can be copied when
present or rebuilt when missing.

This immutability is what makes hard-link checkpoints safe for SSTs and fully
published vLog files.

---

## 6. Public API

### 6.1 Synchronous API

```rust
pub struct CheckpointOptions {
    pub overwrite: bool,
    pub use_hard_links: bool,
    pub include_vlog_indexes: bool,
}

impl Default for CheckpointOptions {
    fn default() -> Self {
        Self {
            overwrite: false,
            use_hard_links: true,
            include_vlog_indexes: true,
        }
    }
}

pub struct CheckpointStats {
    pub sst_files: usize,
    pub vlog_files: usize,
    pub vlog_index_files: usize,
    pub manifest_files: usize,
    pub hard_linked_files: usize,
    pub copied_files: usize,
    pub bytes_copied: u64,
    pub bytes_referenced: u64,

    // Compatibility aliases retained for existing callers.
    pub sst_count: usize,
    pub files_copied: usize,
    pub files_hard_linked: usize,
}

impl KvEngine {
    pub fn create_checkpoint(&self, target_dir: impl AsRef<Path>) -> Result<CheckpointStats>;

    pub fn create_checkpoint_with_options(
        &self,
        target_dir: impl AsRef<Path>,
        options: CheckpointOptions,
    ) -> Result<CheckpointStats>;
}
```

`create_checkpoint()` uses `CheckpointOptions::default()`.

Checkpoint errors should distinguish ordinary creation failures from
post-publication durability failures. If the checkpoint directory has already
been renamed into `target_dir` but the parent-directory fsync fails, the API must
return a distinct `PublishedButNotDurable` error carrying `target_dir` and the
underlying sync error. That result is not retry-equivalent to a pre-publication
failure because `target_dir` may already exist.

### 6.2 Async API

```rust
impl KvEngine {
    pub async fn create_checkpoint_async(
        &self,
        target_dir: impl AsRef<Path> + Send,
    ) -> Result<CheckpointStats>;

    pub async fn create_checkpoint_with_options_async(
        &self,
        target_dir: impl AsRef<Path> + Send,
        options: CheckpointOptions,
    ) -> Result<CheckpointStats>;
}
```

The async methods use the same `BlockingExecutor` pattern as `open_async`,
`close_async`, `force_flush_async`, and `force_full_compaction_async`. The
checkpoint algorithm is filesystem-heavy and should not run on Tokio worker
threads directly.

### 6.3 CLI Hook

The interactive CLI should add:

```text
checkpoint <target_dir>
```

The command calls `KvEngine::create_checkpoint(target_dir)` and prints the
returned stats. This keeps manual testing simple and avoids adding a separate
backup binary in the MVP.

---

## 7. Checkpoint Algorithm

### 7.1 High-Level Flow

```text
create_checkpoint(target_dir):
  validate target_dir
  tmp_dir = target_dir.with_file_name(format!("{target_name}.checkpoint-{attempt}.tmp"))

  acquire target checkpoint lock
  remove stale tmp_dir if it belongs to a previous checkpoint attempt
  acquire checkpoint_lock
  create staging_tmp_dir
  write CHECKPOINT_IN_PROGRESS marker with canonical target_dir and final attempt id
  fsync CHECKPOINT_IN_PROGRESS and staging_tmp_dir
  atomically rename staging_tmp_dir to tmp_dir
  fsync target parent directory

  acquire write/freeze coordination
  sync active WAL
  freeze active memtable if non-empty
  flush every immutable memtable
  acquire state_lock
  capture state + compaction filter registry
  if vLog checkpointing is enabled:
    capture vLog references
  install checkpoint file pins for every selected SST
  if vLog checkpointing is enabled:
    install checkpoint file pins for every selected vLog file
  release state_lock and write/freeze coordination

  copy or hard-link referenced SST files
  if vLog checkpointing is enabled:
    copy or hard-link referenced vLog files
    copy or hard-link referenced .vidx files when present
  write checkpoint manifest files into tmp_dir
  fsync data files, manifest files, and tmp_dir
  write CHECKPOINT_READY with final CHECKPOINT metadata
  fsync CHECKPOINT_READY
  remove CHECKPOINT_IN_PROGRESS
  rename CHECKPOINT_READY to CHECKPOINT
  fsync tmp_dir
  atomically rename tmp_dir to target_dir
  fsync target parent directory
  release checkpoint file pins
  release checkpoint_lock
  release target checkpoint lock
```

`checkpoint_lock` is a new mutex on `LsmStorageInner` that serializes checkpoint
preparation and publication inside one process. Concurrent checkpoint calls
return `WouldBlock` or wait; the first slice should wait for simplicity.

The target checkpoint lock is a cross-process guard for one target path. The
lock file is opened or created next to `target_dir` and then exclusively locked.
It records the canonical target path and attempt id. Stale temporary cleanup is
allowed only after the target checkpoint lock is held and before the
in-process `checkpoint_lock` is acquired. If the implementation cannot prove the
recorded owner is inactive, it must fail safely instead of deleting a marked
temporary directory.

The selected source files must also be pinned until every link/copy operation is
finished. Merely collecting paths under `state_lock` is not enough: compaction,
TTL wholesale drop, MVCC GC, or vLog GC may publish a newer state and delete old
files before checkpoint copying reaches them. The implementation should add a
checkpoint file-retention registry, described in Section 8.3, and deletion paths
must consult that registry before removing SST, vLog, or `.vidx` files.

### 7.2 Source Quiescing

The MVP should checkpoint only flushed state. This avoids copying active WAL
segments and avoids defining restore behavior for partially committed WAL
tickets. The source phase is:

1. call `sync()` to force current WAL durability;
2. freeze the active memtable if it is non-empty;
3. flush immutable memtables until none remain;
4. capture the current `LsmStorageState`;
5. write a checkpoint-local manifest snapshot.

This may briefly block writes. That is acceptable for the MVP because it keeps
the correctness model simple. A future online variant can include WAL files and
reduce the stall.

### 7.3 Manifest Output

The checkpoint should contain:

```text
MANIFEST
MANIFEST_SNAPSHOT
00001.sst
00002.sst
...
vlog/
  3.vlog
  3.vidx
  ...
CHECKPOINT
```

`MANIFEST` should be an empty valid manifest file. `MANIFEST_SNAPSHOT` should
contain one serialized `ManifestRecord::Snapshot` representing the captured
state:

```text
l0_sstables
levels
range_only_ssts
next_sst_id
vlog_references
imm_memtable_ids = []
active_compaction_filters
next_compaction_filter_id
format_version = MANIFEST_FORMAT_VERSION
```

The checkpoint must not copy the source `MANIFEST` as-is. Historical manifest
records may mention files that are no longer live, and source `MANIFEST` may
continue changing after the checkpoint state is captured.

The optional `CHECKPOINT` metadata file is JSON:

```json
{
  "version": 1,
  "state": "complete",
  "target_dir": "/absolute/checkpoint/path",
  "attempt_id": "checkpoint-42.tmp",
  "sst_files": 12,
  "vlog_files": 3,
  "vlog_index_files": 3,
  "manifest_files": 2,
  "hard_linked_files": 17,
  "copied_files": 3,
  "sst_count": 12,
  "files_copied": 3,
  "files_hard_linked": 17,
  "bytes_copied": 1048576,
  "bytes_referenced": 8388608
}
```

`CHECKPOINT` is informational only. `KvEngine::open()` must not require it.

### 7.4 File Set Collection

The live SST set is:

```text
state.sstables.keys()
```

`state.sstables` is the complete live SST map. Every ID in `range_only_ssts`
must also be present in `state.sstables`, so the checkpoint does not need to
union both collections. Phase 2 tests should cover this invariant across
recovery and compaction paths before range-only checkpoint support is accepted.

The live vLog set is derived from the captured vLog references for live SSTs:

```text
for each live sst_id:
  collect vlog.get_sst_references(sst_id)
```

The implementation should sort all file IDs before copying for deterministic
checkpoints and tests.

When vLog checkpointing is enabled, the checkpoint should copy selected vLog
files to `<tmp_dir>/vlog/{id}.vlog` because the engine opens value separation at
`db_path/vlog`. It should include `<tmp_dir>/vlog/{id}.vidx` files when they
exist and `include_vlog_indexes` is true. Missing `.vidx` files are not fatal
because indexes can be rebuilt from vLog contents.

### 7.5 Copy vs Hard Link

For immutable files, the implementation should try hard links first when
`use_hard_links` is true:

```text
fs::hard_link(source, target)
```

If hard linking fails with `EXDEV`, `EPERM`, or `EOPNOTSUPP`, fall back to
copying the file. Other errors should fail the checkpoint unless the target file
already exists as part of a known stale temporary directory cleanup.

Hard links are safe because the source files selected for checkpoint are
immutable after publication. The implementation must not hard-link active WAL
files or temporary compaction outputs.

### 7.6 Atomic Publication

Checkpoint creation must never publish a partial final directory:

1. fail if `target_dir` exists and `overwrite == false`;
2. in Phase 1, fail if `overwrite == true`; callers must remove or choose a new
   target explicitly;
3. build the new checkpoint in
   `target_dir.checkpoint-<attempt>.tmp`, where `<attempt>` is unique enough to
   avoid collisions between distinct checkpoint calls; the implementation first
   creates a non-matching staging directory and renames it to the `.tmp` name
   only after `CHECKPOINT_IN_PROGRESS` is durable, so retry cleanup never sees
   a markerless cleanup-managed temp directory;
4. fsync every copied file;
5. fsync the temporary directory;
6. rename the temporary directory to `target_dir` with no-replace publication
   semantics;
7. fsync the parent directory.

If the process crashes before the staging-to-temp rename, no cleanup-managed
`.tmp` directory exists. If it crashes after that rename but before final
publication, only a marked temporary directory may exist. If it crashes after
the final rename, `target_dir` must be reopenable.

If the no-replace rename succeeds but fsyncing the parent directory fails,
`create_checkpoint` must return a distinct `PublishedButNotDurable` error that
includes `target_dir`. This tells callers the checkpoint has been published but
the final directory entry may not survive power loss. The implementation must
still release checkpoint file pins, the target checkpoint lock, and
`checkpoint_lock` on that path. Callers should inspect `target_dir` before
retrying.

Overwrite can be added later only with an explicit crash contract. Renaming an
existing `target_dir` away before publishing the new checkpoint is not atomic as
a replace operation for directories and can leave no checkpoint at `target_dir`
after a crash.

The no-replace publication rule must hold even if another process creates
`target_dir` between validation and final rename. If the platform lacks a
directory-level no-replace primitive, the implementation must serialize against a
checkpoint target marker/lock or fail rather than replacing another directory.

### 7.7 Cleanup Policy

If checkpoint creation fails before final publication, the implementation should
best-effort remove the temporary directory only when it still owns the target
checkpoint lock and the directory metadata matches the current attempt. If
cleanup fails, return the original checkpoint error and log the cleanup error.

The implementation must write `CHECKPOINT_IN_PROGRESS` immediately after
creating the staging temporary directory, include the intended canonical
`target_dir` and final attempt id in that marker, and fsync both the marker and
staging directory before renaming it to the cleanup-managed temporary directory.
After all checkpoint files are fsynced, write `CHECKPOINT_READY` with the final
`CHECKPOINT` metadata, fsync it, remove `CHECKPOINT_IN_PROGRESS`, then rename
`CHECKPOINT_READY` to `CHECKPOINT` in the temporary directory. Fsync the
temporary directory again before the no-replace directory rename. A failure in
this marker transition can leave a temporary directory with `CHECKPOINT_READY`
or `CHECKPOINT`; stale-temp cleanup accepts either marker only when target and
attempt metadata prove ownership. A published checkpoint must never contain
`CHECKPOINT_IN_PROGRESS` or `CHECKPOINT_READY`.

On startup, kv-engine should not scan for or remove checkpoint temp directories.
Checkpoint temp cleanup is a caller-level concern because the target may be
outside the database directory.

---

## 8. Correctness Requirements

### 8.1 Snapshot Consistency

The checkpoint must represent one logical engine state. It must not combine:

1. a newer manifest snapshot with older SST files;
2. older manifest state with newer vLog references;
3. SST files from before and after a compaction publication in the same level;
4. range-only SST metadata without the matching range-only SST files.

Capturing the file list from one `LsmStorageState` after flushing all memtables
is the core consistency rule.

### 8.2 MVCC and Active Readers

Active MVCC readers do not block checkpoint creation. A checkpoint is a physical
copy of the latest published state after the checkpoint's flush phase. It does
not preserve every historical version pinned by currently active read guards
unless those versions are still present in live SSTs.

The checkpoint must not trigger MVCC GC or compaction by itself. It only flushes
memtables.

### 8.3 Compaction and GC Races

Flush, compaction, TTL wholesale drop, MVCC GC, and vLog GC can create and
delete files. The checkpoint must hold enough coordination while collecting the
file list to prevent selecting a file that is concurrently deleted before it is
linked or copied.

The first implementation should take the conservative path:

1. hold `state_lock` while collecting the manifest snapshot payload, live file
   paths, and file IDs;
2. register those file IDs in a checkpoint file-retention registry before
   releasing `state_lock`;
3. make SST deletion consult that registry;
4. release `state_lock` before copying large files;
5. unregister the pinned file IDs only after checkpoint publication or cleanup
   has completed.

Phase 2 should extend the same registry to vLog deletion, vLog orphan cleanup,
and `.vidx` deletion before vLog checkpointing is enabled.

The registry can be a small refcounted set:

```text
pinned_ssts:  sst_id -> refcount
pinned_vlogs: vlog_file_id -> refcount
```

Deletion paths that encounter a pinned file must defer physical deletion. The
file can be retried during ordinary orphan cleanup after the checkpoint releases
its pins. If copying a selected file later fails with `NotFound`, treat that as
a bug in the retention contract and fail the checkpoint.

### 8.4 Value Separation

For vLog-enabled databases, the checkpoint must include every vLog file
referenced by any live SST in the captured state. It must place those files
under `target_dir/vlog/`, preserving the source naming convention
`{file_id}.vlog`. It must not include vLog files that are only referenced by
memtables because the flush phase should have removed memtable-only state from
the checkpoint contract.

When `.vidx` files are included, they must match their vLog file. If copying a
`.vidx` file fails with `NotFound`, the checkpoint can continue without it. If
copying fails for any other reason, fail the checkpoint.

### 8.5 WAL

The MVP checkpoint excludes WAL files. This is only correct because the
algorithm flushes every memtable before capturing state. The checkpoint
`imm_memtable_ids` list must be empty.

Future online checkpoints may include WAL files to reduce write stalls, but that
requires a separate restore contract.

---

## 9. Failure Model

### 9.1 Source Database Safety

Checkpoint creation must not corrupt or roll back the source database. All
checkpoint output is written outside the source directory unless the user
chooses a target inside it. The implementation should reject targets inside the
source database directory to avoid recursive copies and confusing recovery
artifacts.

### 9.2 Target Directory Crash Windows

| Crash Point | Expected Recovery |
|---|---|
| Before the marked temporary directory exists | No checkpoint output |
| While linking/copying files with `CHECKPOINT_IN_PROGRESS` | Marked temporary directory may exist; `target_dir` absent |
| After `CHECKPOINT_READY` fsync, before marker rename | Marked temporary directory may exist; cleanup can discard or restart it under the target lock |
| After `CHECKPOINT` marker rename, before directory rename | Ready temporary directory may exist; cleanup validates matching metadata and removes it under the target lock |
| After directory rename, before parent fsync | Process crash should see `target_dir`; API parent-fsync failure returns `PublishedButNotDurable` |
| After parent fsync | `target_dir` is durable and reopenable |

Chaos tests cover in-crate failpoint panic interruption after staging temp
creation, during file copy, after manifest write, before final rename, and after
final rename.

### 9.3 Stale Temporary Directories

A stale `target_dir.checkpoint-<attempt>.tmp` from a previous failed checkpoint
may be removed before starting a new checkpoint if it contains a
`CHECKPOINT_IN_PROGRESS`, `CHECKPOINT_READY`, or `CHECKPOINT` marker created by
kv-engine and its target/attempt metadata matches the requested `target_dir`,
the target checkpoint lock is held, and the recorded owner is no longer active.
Without that marker and target match, fail instead of deleting unknown user
data. Markerless staging directories use a non-matching suffix and are not
managed by retry cleanup.

If a stale temporary directory contains final `CHECKPOINT` metadata instead of
`CHECKPOINT_IN_PROGRESS`, it represents a completed-but-unpublished checkpoint
attempt. The implementation does not complete publication from stale temporary
directories; it validates that the `target_dir` and attempt metadata match the
current cleanup target, then removes the stale temporary directory under the
target checkpoint lock.

---

## 10. Implementation Plan

### Phase 1: Synchronous MVP

1. Add `CheckpointOptions` and `CheckpointStats`.
2. Add `checkpoint_lock` to `LsmStorageInner`.
3. Add the checkpoint file-retention registry and wire SST deletion through it.
4. Implement `KvEngine::create_checkpoint_with_options`.
5. Support non-vLog databases first.
6. Copy checkpoint manifest files and selected SST files.
7. Add helper methods:
   - `collect_checkpoint_state()`;
   - `pin_checkpoint_files()`;
   - `write_checkpoint_manifest()`;
   - `copy_or_link_file()`;
   - `fsync_dir()`;
   - `validate_checkpoint_target()`.
8. Add CLI `checkpoint <target_dir>`.
9. Add tests for a non-vLog database:
   - checkpoint fresh empty database;
   - checkpoint after puts and flush;
   - checkpoint after updates/deletes;
   - source remains usable after checkpoint;
   - checkpoint can be opened and mutated independently.

Phase 1 landed the synchronous MVP and was followed by the later phase work
below.

### Phase 2: vLog, TTL, Range Tombstones, and Filters

1. Checkpoint vLog-enabled databases.
2. Include optional `.vidx` files.
3. Wire vLog deletion, vLog orphan cleanup, and `.vidx` deletion through the
   checkpoint file-retention registry.
4. Verify missing `.vidx` rebuild works after restore.
5. Checkpoint range tombstones and range-only SSTs.
6. Test that every `range_only_ssts` ID is present in `state.sstables` across
   recovery and compaction paths.
7. Checkpoint TTL metadata and expired-but-not-yet-compacted entries.
8. Checkpoint installed compaction filters.

### Phase 3: Failure Testing

1. Add failpoints:
   - `checkpoint.after_in_progress_marker`;
   - `checkpoint.after_tmp_dir_create`;
   - `checkpoint.after_manifest_write`;
   - `checkpoint.after_file_copy`;
   - `checkpoint.after_ready_marker`;
   - `checkpoint.after_checkpoint_marker`;
   - `checkpoint.before_publish_rename`;
   - `checkpoint.after_publish_rename_before_dir_sync`.
2. Add in-crate failpoint panic tests for each failpoint.
3. Verify failed checkpoints never affect source reopen.
4. Verify published checkpoints reopen.
5. Verify stale marked temp directory cleanup.

### Phase 4: Async and Benchmark Integration

1. Add async wrappers.
2. Add checkpoint stats to CLI output.
3. Use checkpoints in `write-perf` setup after the API is proven stable. The
   steady-state `--prepare-golden` path now bulk-loads into a temporary source
   database and publishes the reusable golden directory through the checkpoint
   API; `--clone-golden` copies that checkpoint-derived source without opening
   or mutating it.

---

## 11. Tests

Phase 1 required deterministic tests:

1. `checkpoint_empty_database_reopens`
2. `checkpoint_flushed_database_reopens`
3. `checkpoint_flushes_active_memtable`
4. `checkpoint_preserves_deletes`
5. `checkpoint_rejects_existing_target_by_default`
6. `checkpoint_rejects_target_inside_source_dir`
7. `checkpoint_source_remains_writable`
8. `checkpoint_restored_database_is_independent`
9. `checkpoint_concurrent_calls_serialize`
10. `checkpoint_stats_match_file_set`

Full RFC deterministic tests:

1. `checkpoint_preserves_range_tombstones`
2. `checkpoint_preserves_ttl_visibility`
3. `checkpoint_preserves_vlog_values`
4. `checkpoint_missing_vidx_rebuilds`
5. `checkpoint_does_not_copy_orphan_vlog`

Phase 3 required failpoint panic tests:

1. fail after in-progress marker publication;
2. fail after temp directory creation;
3. fail after manifest write;
4. fail during SST copy;
5. fail after ready marker publication;
6. fail after checkpoint marker publication;
7. fail before publish rename;
8. fail after publish rename before parent dir sync.

---

## 12. Observability

`CheckpointStats` should be returned to callers and printed by the CLI. The
source engine should also expose cumulative checkpoint counters in a future
stats surface:

```rust
pub struct CheckpointEngineStats {
    pub checkpoints_started: u64,
    pub checkpoints_succeeded: u64,
    pub checkpoints_failed: u64,
    pub checkpoint_bytes_copied: u64,
    pub checkpoint_bytes_referenced: u64,
    pub checkpoint_last_duration_micros: u64,
}
```

The MVP can keep cumulative engine stats as future work, but per-call
`CheckpointStats` is required.

---

## 13. Open Questions

1. Should overwrite support exist in a later API, and what crash-recovery
   contract should it expose for existing target directories?
2. Should `.vidx` files be copied by default, or should restore always rebuild
   them to reduce checkpoint size?
3. Resolved: `CheckpointStats` reports copied bytes separately from
   hard-linked referenced bytes.
4. Should a future online checkpoint include WAL files to reduce write stalls?

---

## 14. Future Work

1. Incremental backups keyed by SST/vLog file IDs and file checksums.
2. Remote backup sinks.
3. Backup verification command that opens and scans a checkpoint.
4. Checkpoint compression or archive packaging.
5. Named user snapshots built on MVCC read guards.
6. Hot online checkpoints that include WAL instead of flushing all memtables.
7. Additional benchmark fixture workflows beyond steady-state golden
   preparation.
