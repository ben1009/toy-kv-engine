# RFC 022: Incremental Backup and Restore

**Status:** Proposed
**Date:** 2026-08-29
**Author:** kv-engine Contributors
**References:**
- RFC 001: Key-Value Separation
- RFC 013: Chaos Testing
- RFC 019: Checkpoint and Backup API
- RFC 021: Public Snapshot API
- [RocksDB Backup and Checkpoint](https://github.com/facebook/rocksdb/wiki/Basic-Operations)
- [How to backup RocksDB](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB)

---

## 1. Summary

This RFC adds a local incremental backup repository to kv-engine. The first
backup copies the complete live file set. Later backups reuse immutable SST and
vLog files already present in the repository and copy only new
or changed files.

```rust
pub struct BackupOptions {
    pub repository: PathBuf,
    pub use_hard_links: bool,
}

// Phase 1 fixes the identity algorithm; this is not yet an algorithm plug-in.
pub const MANIFEST_FORMAT_VERSION: u32 = 6;

pub struct ImmutableFileMetadata {
    pub kind: FileKind,
    pub file_id: u64,
    pub file_size: u64,
    pub checksum_algorithm: ChecksumAlgorithm, // always Sha256 in Phase 1
    pub file_checksum: [u8; 32],
}

pub struct BackupInfo {
    pub id: u64,
    pub created_at_secs: u64,
    pub logical_bytes: u64,
    pub new_object_bytes: u64,
    pub file_count: u64,
    pub parent_id: Option<u64>,
}

impl KvEngine {
    pub fn create_backup(&self, options: BackupOptions) -> Result<CreateBackupOutcome>;
    pub fn create_backup_async(&self, options: BackupOptions) -> BackupTask;
}

pub enum CreateBackupOutcome {
    Committed(BackupInfo),
    RepositoryPublishedButNotDurable { repository: PathBuf, error: std::io::Error },
    CommitPublishedButNotDurable { info: BackupInfo, error: std::io::Error },
    CommitPublicationUnknown {
        info: BackupInfo,
        fsync_error: std::io::Error,
        revalidation_error: Error,
    },
}

pub enum BackupOutcome {
    Committed(BackupInfo),
    CancelledBeforeCommit,
    CommittedAfterCancellation(BackupInfo),
    RepositoryPublishedButNotDurable { repository: PathBuf, error: std::io::Error },
    CommitPublishedButNotDurable { info: BackupInfo, error: std::io::Error },
    CommitPublicationUnknown {
        info: BackupInfo,
        fsync_error: std::io::Error,
        revalidation_error: Error,
    },
}

pub struct BackupTask { /* Future<Output = Result<BackupOutcome>> + Send + 'static; cancel on Drop */ }

impl BackupTask {
    /// Idempotently requests cancellation; awaiting the task yields a
    /// `BackupOutcome` that distinguishes pre-commit cancellation from a
    /// durable commit race.
    pub fn cancel(&self) { /* request cancellation */ }
}

pub enum RestoreOutcome {
    Restored,
    PublishedButNotDurable { target: PathBuf, error: std::io::Error },
}

`Error` above is the crate's existing non-I/O error type; the two errors in
`CommitPublicationUnknown` are preserved separately so callers can diagnose
both failures without treating the outcome as retry-safe.

pub struct BackupRepository { /* private */ }

impl BackupRepository {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn list(&self) -> Result<Vec<BackupInfo>>;
    pub fn verify(&self, id: u64) -> Result<()>;
    pub fn restore(&self, id: u64, target: impl AsRef<Path>, options: LsmStorageOptions) -> Result<RestoreOutcome>;
    pub fn purge(&self, retain: usize) -> Result<()>;
}
```

Backups are immutable generations. `MANIFEST_SNAPSHOT` is the sole canonical,
versioned full-state manifest produced from the captured `LsmStorageState`;
`GENERATION` is only its envelope, identity, options, and object map. Together
they record the exact SST and vLog file set needed to reopen that state. `.vidx` indexes are
mutable derived artifacts and are rebuilt on restore. Shared files are stored
once and referenced by generation metadata. Restore materializes
a standalone database directory without modifying the source or repository.

This is an operational feature, not a logical MVCC snapshot. It complements
RFC 021 snapshots and RFC 019 checkpoints.

Phase 1 is Linux-only because correctness depends on `openat`, `O_NOFOLLOW`,
`renameat2(RENAME_NOREPLACE)`, and advisory `flock`; future portability layers
must provide equivalent no-follow, no-replace, and locking guarantees.

---

## 2. Motivation and RocksDB Comparison

RocksDB's `BackupEngine` creates periodic incremental backups, shares unchanged
table files between generations, exposes backup metadata, verifies backups, and
restores a selected generation. kv-engine currently creates self-contained local
checkpoints; repeated checkpoints recopy unchanged files.

Incremental backups reduce backup bandwidth and storage for databases whose
SSTs are immutable and whose changes are append-like. They also provide explicit
retention and restore semantics instead of requiring callers to manage a set of
checkpoint directories manually.

---

## 3. Goals

1. Create a complete first backup and incremental later generations.
2. Reuse unchanged immutable SST and vLog files by stable file ID and
   persisted creation-time checksum metadata.
3. Persist generation metadata atomically and recover it after a crash.
4. List, verify, restore, and purge backup generations.
5. Preserve the existing checkpoint consistency and file-pin contract.
6. Keep the source database open and usable after backup creation.
7. Support WAL-enabled sources by flushing all committed state before capture;
   backups exclude WAL files and do not provide WAL point-in-time replay.
   MVCC, range tombstones, TTL, vLog, and manifest snapshots remain supported.
8. Provide synchronous and engine-owned blocking-executor async APIs.
9. Add deterministic crash-window, retention, restore, and deduplication tests.

---

## 4. Non-Goals

1. Remote object-store or streaming backup sinks.
2. Encryption, compression, or client-side key management.
3. Incremental restore into an already-open database.
4. WAL point-in-time replay or continuous replication.
5. Sharing files across unrelated backup repositories.
6. Cross-format-version conversion during restore.
7. Automatic scheduling; callers or a future maintenance service trigger backups.

---

## 5. Repository Layout

The repository is separate from the source database:

```text
backup-repository/
├── BACKUP_MANIFEST          # atomic repository catalog
├── files/
│   ├── sst-<id>-<digest>
│   └── vlog-<id>-<digest>
└── generations/
    └── <backup-id>/
        ├── GENERATION       # canonical state, options, paths, and file map
        └── MANIFEST_SNAPSHOT # canonical captured-state manifest bytes
```

The repository catalog maps each backup ID to its logical file set and maps
each logical source file to one immutable stored object. Stored object names
include the source kind, file ID, and a SHA-256 digest; source-relative paths
live in `GENERATION`. The digest is over the complete file bytes and is
persisted in source file metadata when the file is finalized. Normal
incremental backup compares that persisted identity (and the repository
object's `fstat` length) with the derived object name; it does not full-hash an
unchanged source or repository object merely to decide reuse. Full byte hashing
remains part of object publication and explicit `verify`. File ID alone is
insufficient after file replacement or database restore. `GENERATION` also records the
source format version and the minimum compatible storage options required by
restore. The metadata records manifest format version, vLog enablement and vLog
file format version, whether TTL records exist, and whether serializable mode
was enabled at capture time. Serializable OCC state is in-memory and is not
required to reopen data, so restore accepts either serializable setting. Restore
requires exact manifest format and vLog
enablement/format compatibility. TTL is a format behavior rather than an option
toggle; restore preserves TTL records under the matching manifest format.
For vLog, `min_value_size`,
`max_value_size`, `max_vlog_file_size`, GC threshold ratio, open-file limit,
reader-cache capacity, and value-cache capacity are runtime policy for future
writes and reads; restore accepts caller-selected values. Compaction strategy
and block-cache settings are likewise not part of backup compatibility.

Every stored path uses canonical slash-separated relative syntax. It must be
non-empty, must not begin with `/`, must not contain `.` or `..` components, and
must be one of `<id:05>.sst` (decimal ID, minimum width five) or
`vlog/<id>.vlog`. `MANIFEST` and
`MANIFEST_SNAPSHOT` are generated artifacts, never immutable-object entries.
Both `verify` and `restore` reject invalid paths before
opening any destination. Restore requires an absent final target in a trusted
parent. It creates a uniquely named sibling staging directory using
descriptor-relative `mkdirat`/`openat` traversal with `O_NOFOLLOW` for every
component and file, so a symlink replacement cannot escape the requested target
directory.

`create_backup` is the sole initialization path for an absent repository. The
parent-scoped bootstrap lock is a persistent regular no-follow sibling named
`.<repository-name>.incremental-backup.init.lock`, opened with `O_CREAT` and
held by advisory `flock` on the stable parent inode; a crashed initializer
releases it automatically. Under that lock it creates `files/`, `generations/`, persistent `LOCK`, and a framed empty
`BACKUP_MANIFEST` in staging, fsyncs each directory and catalog file, then
publishes the repository root atomically and fsyncs the trusted parent directory
before reporting success. If that final fsync fails, `create_backup` reports a
named `CreateBackupOutcome::RepositoryPublishedButNotDurable { repository,
error }` outcome; `create_backup_async` reports the equivalent `BackupOutcome`
variant. Callers must not retry the same repository path.
`BackupRepository::open` requires an
already initialized repository. Concurrent first creates serialize on the same
parent-scoped repository lock; recovery removes incomplete initialization
staging before another create attempt.

Every initialized repository uses the persistent regular `LOCK` inode, opened
descriptor-relatively with `O_NOFOLLOW` and held with advisory `flock`: exclusive
for `create_backup` and `purge`; shared for the entire `list`, `verify`, and
restore validation/copy/fsync phase. Restore releases its shared lock only after
staging is complete; it needs no repository write lock for target publication.
Lock release follows process exit, so recovery
never infers liveness from a reusable PID. All repository writes use
descriptor-relative no-follow directory/file operations; a symlinked `files/`,
`generations/`, `LOCK`, or catalog path fails the operation.

`BackupRepository::open` and every operation first acquire the exclusive
repository lock for recovery (catalog truncation, purge-temp promotion, orphan
quarantine, and reference recomputation). They downgrade to the operation's
shared/exclusive lock only after recovery completes, so no reader races a
mutating recovery pass.

The concurrency contract is conservative: `create_backup` and `purge` require
exclusive access and exclude each other, restore, and list/verify; restore and
list/verify may share access only after the exclusive recovery phase completes.
This means a long restore blocks new backups until its staged copy and fsyncs
finish. Releasing that lock earlier would require generation/object reference
pins and is deferred to a later optimization.

---

## 6. Backup Algorithm

1. Validate the repository path and acquire its repository lock.
2. Reuse RFC 019's checkpoint boundary: sync/flush the source, capture a stable
   state, and pin every referenced file during copying.
   The captured canonical manifest snapshot must have `imm_memtable_ids == []`;
   otherwise backup fails because WAL files are intentionally excluded.
3. Allocate the next durable backup ID by appending and fsyncing
   `HighWater(sequence, allocated_id)` before creating any generation directory.
4. Build the exact logical file set for the generation.
5. For each immutable file, derive identity from persisted `(file_id, kind,
   file_size, checksum_algorithm, file_checksum)` metadata. Reuse a verified
   repository object or create a
   per-object temporary file, fsync it, atomically publish it with a no-replace
   rename into `files/<kind>-<id>-<sha256>`, and fsync `files/`. A name
   collision is reusable only when its identity exactly matches; otherwise the
   backup fails without overwriting the object.
   SST and vLog finalization persists `file_id`, file kind, file size,
   `checksum_algorithm`, and `file_checksum` in immutable file metadata and the
   manifest state. Incremental backup reads this metadata instead of rereading
   unchanged file contents solely to determine object reuse; only newly
   published objects are read for copy/hash verification.
6. Serialize one canonical `ManifestRecord::Snapshot` (including the complete
   `immutable_file_metadata` map) into
   `MANIFEST_SNAPSHOT`, then write `GENERATION` containing its byte length and
   SHA-256, source format version, storage-option metadata, source-relative
   file paths and identities, creation time, logical/new-object byte accounting,
   file count, and parent backup ID.
7. Fsync staged objects, generation metadata, and repository directories.
8. Append and fsync a checksummed
   `Prepare(sequence, id, parent_id, generation_checksum)`
   record to `BACKUP_MANIFEST`.
9. Fsync the staged generation, rename it into `generations/<id>`, fsync the
   generations directory, then append and fsync a checksummed
   `Commit(sequence, id, prepare_sequence, prepare_digest)` record. Here
   `prepare_digest` is SHA-256 over the canonical encoded `Prepare` record
   payload. Only committed records are visible.
10. Release source file pins and repository locks.

`GENERATION` object entries contain only canonical SST and vLog target paths;
they never contain `MANIFEST`, `MANIFEST_SNAPSHOT`, or repository object paths.
Repository object paths are derived, never trusted from metadata, as
`files/<kind>-<id>-<sha256>`. Verify and restore open them descriptor-relatively
under `files/` with `O_NOFOLLOW`, require a regular file, and copy bytes from the
opened descriptor. Restore never hard-links from a repository object, preventing
a corrupted object symlink from entering the restored database.
Before creating any restore destination file, restore performs the same full
generation validation as `verify`: exactly one entry per canonical manifest SST
or vLog ID, exact target path derived from kind and ID, a 64-lowercase-hex
digest, no duplicate targets, and bounded counts/lengths. It does not trust an
individually safe but manifest-inconsistent object map.

Generation directories are never named by catalog input: their path is derived
as the decimal backup ID beneath a descriptor-opened `generations/` directory.
Recovery, verify, restore, and purge use `openat`/`unlinkat` with `O_NOFOLLOW`
there as well, rejecting any catalog entry whose ID/path mapping is not exact.
Generation publication uses descriptor-relative `renameat2(RENAME_NOREPLACE)`
under `generations/`; any existing ID directory is a collision and fails rather
than overwriting it. Recovery removes only validated uncommitted orphan
generation directories before a new ID is allocated, so a crash before Commit
cannot cause ID reuse or replacement.

The catalog is an append-only sequence of versioned records:

```text
Prepare(sequence, id, parent_id, generation_checksum)
Commit(sequence, id, prepare_sequence, prepare_digest)
HighWater(sequence, allocated_id)
CatalogSnapshot(sequence, base_catalog_digest, high_water_id, [GenerationEntry])
```

Each record has a length, record type, payload checksum, and sequence number.
Before creating a generation directory, allocation appends and fsyncs
`HighWater(sequence, allocated_id)`. Backup IDs are therefore monotonically
allocated above the durable high-water mark even when a crash leaves an
uncommitted orphan; the parent is the highest visible generation. Catalog sequences are strictly
monotonic: after a valid replay base at sequence N, the next appended record is
N + 1; duplicate or non-increasing sequences are invalid. Allocation grammar
is strict for an uninterrupted transaction: `HighWater(N, id)` allocates exactly
the prior maximum high-water plus one, then is immediately followed by
`Prepare(N + 1, id, ...)` and its bound `Commit(N + 2, ...)`. A crash or
cancellation may leave terminal `HighWater` or terminal `HighWater + Prepare`.
Recovery retains the high-water reservation, discards the trailing Prepare and
staging generation, and the next transaction begins with a new `HighWater` for
the next ID; this high-water-to-high-water transition is valid only across that
recovery boundary. Recovery considers only
`Commit` records whose
`prepare_sequence` and `prepare_digest` bind exactly to one matching `Prepare`,
generation checksum, and generation directory. Duplicate/reused IDs are
rejected during replay. Before a committed generation becomes visible, recovery
opens `generations/<id>/GENERATION` descriptor-relatively with `O_NOFOLLOW`,
checks its bytes against `Prepare.generation_checksum`, and validates its bound
`MANIFEST_SNAPSHOT` length/SHA-256. Missing or mismatched published metadata
invalidates repository open rather than silently listing an unrestoreable backup.
All catalog and generation metadata reads (`BACKUP_MANIFEST`,
`BACKUP_MANIFEST.purge.tmp`, `GENERATION`, and `MANIFEST_SNAPSHOT`) use
descriptor-relative `O_NOFOLLOW` opens, require regular files via `fstat`, and
enforce bounded metadata sizes before parsing; FIFO, device, directory, or
oversized metadata entries fail repository open.
Only an incomplete final frame or a checksum-invalid partial final frame is
discardable and truncated. A complete framed record with semantic corruption
(duplicate ID, invalid sequence, bad binding, or missing generation) fails
repository open and is never silently truncated.

While holding the repository lock, recovery retains every validated
visibility-neutral `HighWater` record and records the byte offset immediately
after the last retained `HighWater`, visible `Commit`, or `CatalogSnapshot`
boundary. A fully framed trailing unmatched `Prepare` is discarded with its
staged generation before any new append. Before appending, recovery truncates
the catalog to that retained boundary and fsyncs both catalog and repository
directory. The next allocation is
`max(CatalogSnapshot.high_water_id, replayed HighWater.allocated_id) + 1`, so a
later backup cannot be hidden behind a torn tail, stale prepared record, or
durably allocated orphan ID.

`CatalogSnapshot` is a self-contained compacted catalog used by purge. Its
top-level `base_catalog_digest` is SHA-256 over the exact last-valid primary
catalog byte prefix, not a generation field. Each
`GenerationEntry` contains `id`, `parent_id`, derived generation directory ID, generation
checksum, canonical `MANIFEST_SNAPSHOT` length/SHA-256, creation time,
    logical/new-object byte accounting, and file count. A snapshot at sequence N is the
replay base: recovery validates every listed generation directory, `GENERATION`
checksum, and manifest-snapshot identity, then replays only valid
`Prepare`/`Commit` records with sequence greater than N. Generations absent
from the snapshot are purged and cannot be listed or restored. A catalog
snapshot with any missing or mismatched retained generation is invalid. Because
purge may delete pre-snapshot generations, recovery never revives older history:
an installed primary snapshot validates independently. A temporary successor is
accepted only when its `base_catalog_digest` matches the last-valid primary
prefix and its sequence is exactly primary_last_sequence + 1; otherwise
repository open fails. `CatalogSnapshot.high_water_id` must be at least every
retained `GenerationEntry.id`; a lower or malformed value is semantic corruption
and fails repository open.

`CatalogSnapshot.high_water_id` preserves the largest ever allocated ID across
purge. Recovery allocates the next backup above that value even when it removes
an uncommitted orphan. A malformed orphan directory is never reused or deleted
as a normal generation; recovery quarantines it under descriptor-safe
`generations/lost+found/` or fails repository open if quarantine cannot complete.

In a compacted retention snapshot, `parent_id` is provenance only and is never
rewritten. It may refer to a purged generation; replay and restore do not require
that parent generation to remain visible.

The source remains usable throughout. A failed attempt leaves only a named
staging directory and no visible generation record. Repository recovery
discards uncommitted `Prepare` records and generation directories not named by
a committed catalog record; orphan objects are reclaimable after reference
recomputation.

### 6.1 File Identity

SST and vLog files are immutable after publication, but IDs can be reused by a
different database or after manual intervention. Reuse requires matching kind,
source ID, byte length, and SHA-256 digest from persisted source metadata. The
repository checks the existing object is a regular file of the expected length,
then reuses the derived immutable name without reading its complete contents.
A later `verify` detects corruption by hashing the object. `.vidx` is intentionally excluded:
the current engine updates it in place and checkpoint pinning does not protect
it. Restore rebuilds missing indexes from the restored immutable vLog files.

### 6.1.1 Canonical checksum metadata and legacy migration

Phase 1 fixes `checksum_algorithm` to `Sha256`; the object filename, validation,
and `ImmutableFileMetadata.file_checksum` are all exactly 32 bytes / 64 lowercase
hex characters. `LsmStorageState` owns an
`immutable_file_metadata: HashMap<FileKey, ImmutableFileMetadata>` and every
`ManifestRecord::Snapshot` includes this map. Flush, compaction, and new vLog
file finalization add entries before publishing the corresponding manifest
edit. Manifest compaction carries the complete map into the next snapshot, so
backup and restore cannot lose it.

The canonical manifest format is bumped to `MANIFEST_FORMAT_VERSION = 6`.
Readers of earlier formats remain able to open earlier databases, but an older
binary is not required to open a v6 manifest. A v6 reader opening a legacy
manifest without checksum metadata performs a one-time per-file backfill while
holding the normal checkpoint file pins: it opens each live immutable file once,
computes its bounded length and SHA-256, updates the in-memory state, and
atomically publishes a v6 snapshot. SST/vLog bytes are never rewritten.

Backfill is crash-safe, idempotent, and resumable. A crash leaves the previous
legacy manifest readable and recomputes missing entries on the next open; the
new snapshot is installed only after its file and parent fsyncs succeed. If
backfill metadata cannot be durably persisted, `create_backup` returns an
explicit backfill error and publishes no generation. It must not silently
full-hash unchanged files on every subsequent backup. Once the v6 snapshot is
durable, later incremental backups use metadata-only reuse, and a restored
database carries the same metadata into its canonical v6 snapshot.

### 6.2 Hard Links and Copies

When enabled and supported, immutable objects may be hard-linked directly from
the pinned source SST or vLog file into an object temporary, then published by
the same no-replace rename. The source is opened once with no-follow, and the
implementation must either link that exact opened inode or hard-link first and
hash/verify the linked temporary before deriving its final name. Copy fallback
hashes and copies from the same opened descriptor. The bytes hashed must always
be the bytes of the inode published. The repository never hard-links mutable
`.vidx` files.

---

## 7. Restore, Verify, and Purge

`restore(id, target, options)` requires a trusted, descriptor-opened parent and
a single normal basename final target; it rejects multi-component or symlinked
target parents. It creates sibling staging and performs every staging write and
the final no-replace `renameat2` through that same parent descriptor. It creates
`vlog/` with no-follow `mkdirat` from the staging root and rejects an unexpected
existing or symlinked component. It then creates a new target directory using the selected
generation's canonical state metadata and stored objects. Restore follows RFC
019's checkpoint manifest form: it writes a valid empty `MANIFEST` and stores
exactly one `ManifestRecord::Snapshot` containing the generation's canonical
state and format marker in `MANIFEST_SNAPSHOT`. It copies each repository
object into the staged target at the source-relative path recorded in
`GENERATION`. During that copy it computes bounded byte length and SHA-256 from
the same opened `O_NOFOLLOW` regular-file descriptor, rejecting a mismatch
before staging publication (restore never hard-links repository objects), and
places vLog files under `target/vlog/` with their original
file IDs. It does not restore `.vidx`; indexes are rebuilt lazily by the first
GC operation that needs them, from the restored vLog. The caller supplies compatible
`LsmStorageOptions`; restore rejects incompatible manifest format or
value-separation enablement/file format before publication. TTL records reopen under the matching
manifest format. Restore fsyncs all staged files and directories, atomically
publishes staging with `renameat2(RENAME_NOREPLACE)` to the final target, then
fsyncs the trusted parent directory. Publication is no-replace and atomic,
matching RFC 019. If the final parent fsync fails after rename, restore returns
`PublishedButNotDurable`; the target may exist and callers must not retry the
same target path.

`verify(id)` checks generation metadata, object existence, lengths, and checksums
without opening the source database. It parses the bound canonical manifest
snapshot and requires its referenced SST/vLog IDs to match the `GENERATION`
object map exactly: no missing, extra, or mismatched kind/ID entries are valid.
A later implementation may add a full reopen-and-scan verification mode.

`purge(retain)` retains the highest `retain` committed visible generation
entries, ordered by committed backup ID; uncommitted `HighWater` reservations
and ID gaps do not consume retention slots. `retain == 0` is rejected; retaining more generations than exist is a
no-op for the excess count. Purge serializes with create, restore, and verify
under the repository lock. It writes a checksummed temporary
`BACKUP_MANIFEST.purge.tmp` containing a `CatalogSnapshot`
containing complete metadata for every retained generation. The temporary file
is a complete replacement catalog stream with exactly one versioned,
length-delimited, checksummed
`CatalogSnapshot(sequence, base_catalog_digest, high_water_id, entries)` record;
it is fsynced, renamed over `BACKUP_MANIFEST`, and followed by a repository
directory fsync before deleting unreferenced objects and generation directories.
Recovery considers only the fixed `BACKUP_MANIFEST.purge.tmp` successor path;
it accepts it only when its framing, record checksum, sequence (exactly
primary_last_sequence + 1), base-catalog digest, and every retained generation
entry validate. Any other temp
name is discarded. Before using an accepted successor for list, next-ID
allocation, or cleanup, recovery atomically renames it over `BACKUP_MANIFEST`
and fsyncs the repository directory. If that promotion fails, repository open
fails and performs no cleanup. An installed primary `CatalogSnapshot` validates
independently; `base_catalog_digest` is checked only for an uninstalled
temporary successor. Recovery then recomputes references before orphan cleanup.

---

## 8. Crash and Concurrency Contract

1. A generation is *visible* when its complete `Commit` frame and generation
   directory are present and pass catalog revalidation. A generation is
   *durable* only after the Commit fsync (and the preceding object, generation,
   and directory fsyncs) succeeds. These states are intentionally distinct:
   a successful Commit append followed by a Commit fsync error may leave a
   visible generation whose crash durability is uncertain.
2. A crash before publication leaves no listed generation.
3. A crash after directory rename but before the bound `Commit` record leaves an orphan
   generation that recovery removes from the visible catalog.
4. Concurrent backups serialize per repository but do not serialize unrelated
   source databases.
5. Restore and purge cannot observe a partially published generation.
6. Source compaction cannot delete a file while it is being copied.
7. Repository objects are immutable and are never overwritten in place.
8. Recovery scans committed catalog records, removes uncommitted generation
   directories, and eventually reclaims unreferenced objects.

---

## 9. API and Metrics

`BackupInfo.logical_bytes` is the sum of referenced SST and vLog object lengths
for the complete generation; it excludes generated manifest/catalog metadata
and lazily rebuilt `.vidx`. `file_count` is the number of those referenced SST
and vLog objects. `new_object_bytes` is the sum of logical lengths of immutable
repository objects newly published by that generation, regardless of whether the
implementation used hard links or byte copies. Operational counters separately
report `bytes_copied`, `files_copied`, `files_hard_linked`, and
`reused_object_bytes`; the difference between logical and new-object bytes makes
deduplication visible and portable.

The implementation should expose repository errors with the operation and path.
The synchronous API returns `Ok(CreateBackupOutcome::Committed(info))` only
after both repository initialization and the generation commit are durable. If
the repository-root rename succeeds but its parent-directory fsync fails, it
returns `Ok(CreateBackupOutcome::RepositoryPublishedButNotDurable { repository,
error })`. If the generation's bound `Commit` append succeeds, its fsync fails,
and catalog revalidation finds that commit visible, it returns
`Ok(CreateBackupOutcome::CommitPublishedButNotDurable { info, error })`. Each
variant carries the exact published path or `BackupInfo` and the original
`std::io::Error` returned by fsync, without replacing its kind or OS error. If
revalidation proves the record absent, the synchronous API returns `Err` with
confirmed no visible generation. If revalidation itself fails, it returns
`Ok(CreateBackupOutcome::CommitPublicationUnknown { info, fsync_error,
revalidation_error })`: visibility and durability are both uncertain and the
caller MUST NOT automatically retry. A published-but-not-durable or unknown
outcome is not a retry-safe failure; callers must inspect/reopen the repository
before deciding what to do.

`create_backup_async` eagerly registers lifecycle admission and dispatches the
worker before returning `BackupTask`; the task may be moved or canceled without
ever being polled. Cancellation, including drop before first poll, is best
effort: a worker that already durably commits produces a visible generation.
`BackupTask` itself is the awaitable future;
`BackupTask::cancel()` and dropping it request cancellation through a
shared token; callers that await an explicit cancellation receive
`BackupOutcome::CancelledBeforeCommit` or
`BackupOutcome::CommittedAfterCancellation(info)`. Async backup owns states `Running`,
`CancelRequested`, `CommitDecided`, `Committed`, `CancelledBeforeCommit`, and
`Failed`. The worker checks that token after each object publication and
immediately before `Prepare`. Immediately before appending `Commit`, it takes
the task-state lock: cancellation before this serialized commit-decision point
transitions to `CancelledBeforeCommit`; cancellation after it is a commit race.
If `Commit` append fails, the worker transitions to `Failed` with no visible
generation. If append succeeds but its fsync fails, it reopens and validates the
catalog: when the bound Commit record is visible it returns
`BackupOutcome::CommitPublishedButNotDurable { info, error }` and callers must
not retry that generation; when the record is absent it returns an error with
confirmed no visible generation. If revalidation fails, it returns
`BackupOutcome::CommitPublicationUnknown { info, fsync_error,
revalidation_error }`; visibility and durability are unknown and automatic
retry is forbidden. Only a successfully fsynced Commit transitions to
`Committed`; a visible but non-durable Commit remains listable and is never
retried or rolled back by the worker. Repository initialization uses the same revalidation and original
fsync-error preservation contract as the synchronous API and returns
`BackupOutcome::RepositoryPublishedButNotDurable { repository, error }` after a
successful root rename followed by a failed parent fsync.
Cancellation before the decision leaves
no visible generation: the worker removes staging or leaves reclaimable orphan
objects and never writes `Commit`. Once `Commit` is fsynced, the state is
`Committed`; a cancellation race returns
`BackupOutcome::CommittedAfterCancellation(info)` but does not roll back the
visible generation, which callers can discover through `list()`.
The task stores one terminal `Result<BackupOutcome>` and wakes every registered
future waker exactly once when that terminal state is published; cancel from a
different task or thread therefore cannot leave an awaited task pending.
If lifecycle admission or executor dispatch fails during construction, the
returned task is immediately ready with that `Err` and publishes no generation.

---

## 10. Implementation Plan

### Phase 1: Repository and Full Backup

1. Add repository catalog and generation metadata formats, including
   `Prepare`/`Commit` records and startup reconciliation.
2. Extract RFC 019's exact live-file-set capture into a reusable internal helper.
3. Implement first full backup, atomic publication, list, verify, and restore
   with explicit compatible `LsmStorageOptions`.
4. Add sync/async APIs and basic crash-window tests.

### Phase 2: Incremental Reuse and Retention

1. Add immutable object identity and deduplication.
2. Reuse SST and vLog objects across generations; rebuild `.vidx` on restore.
3. Implement purge with reference-aware object reclamation.
4. Add concurrent backup/restore/purge tests and byte-accounting checks.

### Phase 3: Operational Follow-Up

1. Add backup verification that reopens and scans a restored generation.
2. Add optional compression, encryption, and remote sinks.
3. Add benchmark fixtures comparing full versus incremental backup time and size.

---

## 11. Test Plan

1. First backup restores all supported database formats.
2. Second backup reuses unchanged SST/vLog objects and restores rebuildable
   `.vidx` indexes.
3. Changed files are copied and old generations remain restorable.
4. File IDs with changed content are not incorrectly reused.
5. WAL and vLog references remain valid after restore.
6. Purge retains objects referenced by surviving generations.
7. Crashes at staging, object copy, generation publish, and catalog publish do
   not create a falsely listed generation.
8. Concurrent source writes and compaction preserve backup consistency.
9. Async cancellation releases pins and temporary files after dispatched work.
10. `BackupInfo` logical/new-object byte counters match the repository contents.
11. WAL-enabled backup flushes the committed boundary and restores the same
    state without copying or replaying WAL files.
12. A crash between generation rename and catalog commit leaves no visible
    generation and is cleaned up on repository open.
13. Purge catalog publication precedes object deletion and retained backups
    remain restorable after an interrupted purge.
14. Restore rejects absolute, parent-traversal, and symlink-race destination
    paths without writing outside its no-follow staging directory.
15. A crash before final restore rename leaves no visible target, while a crash
    after rename leaves a complete reopenable target; staging cleanup cannot
    follow attacker-controlled symlinks.
16. Purge, reopen, list, verify, and restore retain only the catalog-snapshot
    generations; every purged generation is absent and unrestoreable.
17. Purge followed by a new backup replays post-snapshot `Prepare`/`Commit`
    records and restores both retained and newly created generations.
18. Missing or corrupted retained `GENERATION` or `MANIFEST_SNAPSHOT` makes a
    catalog snapshot invalid and cannot silently list an unrestoreable backup.
19. A crash between immutable-object publication and generation/catalog commit
    leaves only reclaimable unreferenced objects; no object name is overwritten.
20. Restore omits `.vidx`, never links a mutable source index, and the first GC
    operation lazily rebuilds indexes from restored vLog files.
21. `new_object_bytes` reports identical logical object lengths for equivalent
    hard-link and copy backups.
22. Corrupt repository object symlinks are rejected without placing a symlink
    in the restored database.
23. Async cancellation before commit leaves no visible generation; cancellation
    after durable commit leaves a listable and restorable generation.
24. Corrupt catalog generation IDs, traversal attempts, and symlinked generation
    directories are rejected by descriptor-relative recovery and purge paths.
25. Recovery accepts only a valid fixed `BACKUP_MANIFEST.purge.tmp` successor
    and discards arbitrary temporary catalog names.
26. Verify rejects a generation whose canonical manifest snapshot and object map
    disagree; restore copies only validated regular repository objects.
27. A final restore parent-fsync failure reports `PublishedButNotDurable` and a
    retry at the same target is rejected.
28. Deterministic failpoints pause immediately before `Prepare` and `Commit` to
    validate the async cancellation state machine and commit-race outcome.
29. Recovery truncates a torn catalog tail durably before a new backup append;
    reopen, list, and restore include that later committed generation.
30. A stale purge temporary with a mismatched base-catalog digest is rejected
    when the primary catalog is damaged.
31. Normal replay rejects a committed generation whose published no-follow
    `GENERATION` or bound `MANIFEST_SNAPSHOT` is missing or corrupted.
32. Cancellation immediately after immutable-object publication leaves no
    visible generation and releases source pins and staging artifacts.
33. First backup atomically initializes an absent repository; concurrent first
    create/open attempts leave one valid framed catalog and no partial root.
34. Restore rejects a corrupted regular repository object by hashing the same
    no-follow descriptor copied into staging.
35. WAL-excluding capture rejects a canonical manifest snapshot with nonempty
    `imm_memtable_ids`.
36. Dropping or cancelling an eagerly dispatched `BackupTask` before its first
    poll either leaves no visible generation or yields a committed generation
    discoverable through `list()`, and always releases lifecycle admission.
37. A fully framed unmatched trailing `Prepare` is durably discarded before the
    next append; sequence and ID allocation resume from the committed boundary.
38. FIFO, device, directory, symlink, and oversized catalog/generation metadata
    are rejected before parsing.
39. A crash after generation rename but before Commit cannot reuse or overwrite
    that generation ID on the next backup.
40. Concurrent processes serialize create/purge through `LOCK`; stale
    initialization and repository locks recover without catalog corruption.
41. Restore rejects a raced intermediate target-parent component and a
    preexisting/symlinked staging `vlog/` directory.
42. A pending `BackupTask` canceled from another task or thread wakes promptly
    and publishes exactly one terminal outcome.
43. A paused restore holds its shared repository lock, so concurrent purge
    cannot unlink any generation or object until staging is complete.
44. Restore rejects a manifest-inconsistent, duplicate, malformed-digest, or
    out-of-bounds `GENERATION` object map before creating target files.
45. `BackupTask` and its future are compile-time `Send + 'static`; executor or
    lifecycle-admission failure publishes one terminal error outcome.
46. A crash after durable `HighWater` but before `Prepare` preserves that ID
    through recovery; the next backup allocates a strictly higher ID.
47. Malformed `HighWater`, a non-adjacent `Prepare`, a mismatched allocated ID,
    or a low snapshot high-water value fails repository open.
48. Cancellation immediately before the serialized commit decision returns
    `CancelledBeforeCommit`; after that decision it returns `Committed`,
    `CommitPublishedButNotDurable`, `CommitPublicationUnknown`, or an error
    with confirmed no visible generation.
49. Retention selects the highest committed visible generations even when
    abandoned HighWater IDs create gaps.
50. Dead bootstrap initializers release the parent advisory lock; concurrent
    live initialization remains serialized and no incomplete root is published.
51. Deterministic repository-root parent-fsync failure injection exercises both
    `create_backup` and `create_backup_async`: each returns its
    `RepositoryPublishedButNotDurable` outcome with the exact repository path
    and original injected `std::io::Error`, and the published repository can be
    opened so the caller need not retry initialization.
52. Deterministic Commit-fsync failure injection exercises both
    `create_backup` and `create_backup_async`: each returns its
    `CommitPublishedButNotDurable` outcome with `BackupInfo` matching the
    generation returned by `list()` and the original injected
    `std::io::Error`, so the caller can identify the publication and avoid an
    unsafe duplicate backup.
53. Commit-fsync revalidation failure exercises both APIs and returns
    `CommitPublicationUnknown` carrying both the original fsync error and the
    revalidation error; retry is explicitly forbidden.
54. An incremental backup with unchanged files performs no full source or
    repository-object hash reads for identity discovery; `verify` still detects
    a deliberately corrupted reused object.
55. Opening a legacy MANIFEST without checksum metadata performs an idempotent,
    crash-safe per-file backfill; a failed metadata fsync returns an explicit
    backfill error and publishes no generation, and a later successful run
    enables metadata-only reuse.

Required checks:

```bash
cargo fmt --all -- --check
cargo nextest run --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

---

## 12. Acceptance Criteria

This RFC is implemented when a caller can create multiple local backup
generations, observe that unchanged immutable files are stored once without
full-hashing them during reuse, restore any retained generation into an
independently openable database, verify its file
integrity, and purge old generations without breaking retained restores. All
crash-window and concurrency tests must pass.
