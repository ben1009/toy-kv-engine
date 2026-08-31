# RFC 022 Incremental Backup Implementation Plan

## Context

RFC 022 adds a Linux-only local incremental-backup repository. A backup is a
captured, flushed physical state: immutable SST/vLog files are stored once in a
repository and each committed generation references that exact object set.
WAL and `.vidx` files are excluded. Restore creates a separate, reopenable
database directory from the canonical captured `MANIFEST_SNAPSHOT`.

The implementation must preserve RFC 019's flush-and-pin consistency boundary.
It must also introduce manifest v6 immutable-file identity before the first
backup: metadata-only reuse is part of the MVP, not a later optimization.

## Step 1: Manifest v6 identity foundation

**Files:** `kv-engine/Cargo.toml`, `src/manifest.rs`, `src/lsm_storage.rs`,
`src/compact.rs`, `src/vlog/`

1. Add `sha2` and private serialized `FileKey`, `FileKind`, and
   `ImmutableFileMetadata` types.
2. Bump `MANIFEST_FORMAT_VERSION` to 6; carry the complete live metadata map
   in every `ManifestRecord::Snapshot` and recovery state.
3. Implement idempotent `ensure_manifest_v6()` for v3-v5 databases. It pins
   live files, hashes each once, reconciles under state/manifest serialization,
   and writes one atomic v6 snapshot. A failure publishes neither v6 metadata
   nor a backup generation.
4. Update flush, all compaction-output paths (including range-only SSTs), and
   vLog GC/finalization to add output metadata before their manifest edit and
   remove obsolete input metadata only after durable state publication.

**Acceptance:** metadata keys equal the current immutable SST/vLog live set
after flush, compaction, GC, reopen, and manifest compaction.

## Step 2: Shared capture boundary

**Files:** `src/checkpoint.rs`, new `src/backup.rs`

1. Reuse a `CheckpointCapture` containing a canonical
   `ManifestRecord::Snapshot`, sorted SST/vLog IDs, and private RAII file pins.
2. Capture by syncing/flushing committed data, then require
   `imm_memtable_ids == []` because backups intentionally exclude WAL.
3. Use the capture while objects are linked/copied; drop it on all completion,
   cancellation, and error paths.

**Acceptance:** compaction and vLog GC cannot delete a captured object during
repository publication; mutable `.vidx` files are never captured.

## Step 3: Repository primitives and catalog

**Files:** new `src/backup.rs`, `src/lib.rs`

1. Implement Linux descriptor-relative helpers for `openat`, `mkdirat`,
   `unlinkat`, `O_NOFOLLOW`, regular-file checks, bounded reads, fsync, and
   `renameat2(RENAME_NOREPLACE)`.
2. Bootstrap `files/`, `generations/`, `LOCK`, and a framed `BACKUP_MANIFEST`
   under the parent-scoped initialization lock.
3. Encode bounded, checksummed catalog frames for `HighWater`, `Prepare`,
   `Commit`, and later `CatalogSnapshot` records.
4. Replay only valid committed generations; truncate only a torn final frame
   or terminal unmatched prepare. Semantic corruption fails `open`.

**Acceptance:** concurrent processes serialize create/purge; no catalog or
metadata path follows a symlink.

## Step 4: Synchronous backup API

**Files:** `src/backup.rs`, `src/lsm_storage.rs`

1. Add `BackupOptions`, `BackupInfo`, `CreateBackupOutcome`, and
   `KvEngine::create_backup`.
2. Allocate and fsync `HighWater`, capture the source, and publish each object
   under its derived `files/<kind>-<id>-<sha256>` name. Hash exactly the inode
   being published; reuse only exact persisted identity and length matches.
3. Write/validate `GENERATION` plus canonical `MANIFEST_SNAPSHOT`, fsync the
   generation, then append/fsync bound `Prepare` and `Commit` records.
4. Preserve repository-root and Commit post-publication fsync errors in the
   RFC's non-retry-safe outcome variants.

**Acceptance:** a second unchanged backup publishes no new immutable objects;
changed bytes behind a reused ID produce a distinct object or fail safely.

## Step 5: Inspect, verify, and restore

**Files:** `src/backup.rs`, backup integration tests

1. Add `BackupRepository::{open,list,verify,restore}`.
2. Verify the generation/map/snapshot relationship before creating destination
   files, then hash every copied repository object from an opened no-follow
   descriptor.
3. Restore into a no-follow sibling staging directory, create an empty
   `MANIFEST`, install the exact snapshot, and publish with no-replace rename.
4. Do not restore `.vidx`; rely on existing lazy vLog index rebuild.

**Acceptance:** restored inline, WAL, vLog, TTL, range-tombstone, and
serializable fixtures reopen and expose the captured data.

## Step 6: Retention and async API

1. Implement `purge(retain)` by installing a complete `CatalogSnapshot` before
   removing generations or unreferenced objects; preserve the high-water ID.
2. First provide an engine blocking-executor async wrapper for the synchronous
   create path.
3. Add eager `BackupTask` dispatch, cancellation checkpoints, lifecycle
   admission, exact-once waker completion, and the committed-after-cancellation
   outcome.

## Test and verification plan

Add deterministic unit, integration, and failpoint coverage for manifest
migration, catalog framing/replay, torn tails, commit and root-fsync ambiguity,
object deduplication, restore path attacks, purge interruption, concurrent
repository operations, and async cancellation. Run targeted backup tests
throughout; the final gate is `cargo make check`.
