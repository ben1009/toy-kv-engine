# RFC 023: Point-in-Time Recovery

**Status:** Proposed  
**Date:** 2026-09-09  
**Author:** kv-engine Contributors  
**References:**
- RFC 005: MVCC
- RFC 012: Parallel WAL
- RFC 013: Chaos Testing
- RFC 017: MVCC Garbage Collection
- RFC 019: Checkpoint and Backup API
- RFC 022: Incremental Backup and Restore
- [RocksDB Transaction Log Iterator](https://github.com/facebook/rocksdb/wiki/Transaction-Log-Iterator)

---

## 1. Summary

This RFC adds local point-in-time recovery (PITR) to kv-engine. PITR combines a
committed RFC 022 backup generation with a continuous archive of immutable WAL
segments. Restore selects the newest usable base backup, replays complete WAL
batches in commit order through a requested recovery point, and atomically
publishes a standalone database directory.

The exact recovery coordinate is the MVCC `commit_ts`, not wall-clock time:

```rust
pub struct PitrOptions {
    pub repository: PathBuf,
    pub config: PersistedPitrConfig,
}

pub struct PersistedPitrConfig {
    pub archive_interval: Duration,
    pub max_segment_bytes: u64,
    pub max_unarchived_bytes: u64,
    pub max_source_spool_bytes: u64,
}

pub enum RecoveryTarget {
    /// Newest commit in a verified, base-backed recoverable interval. If the
    /// selected interval is an empty base, this succeeds with no resolved
    /// commit timestamp.
    Latest,
    CommitTs(u64),
    AtOrBeforeSystemTime(SystemTime),
}

pub struct RecoveryPoint {
    pub commit_ts: Option<u64>,
    pub observed_at: SystemTime,
}

pub struct CommitTimeHighWater {
    pub archive_epoch_id: [u8; 16],
    pub segment_id: u64,
    pub commit_ts: u64,
    pub recorded_at: SystemTime,
    pub entry_digest: [u8; 32],
}

pub struct SegmentAnchor {
    pub segment_id: u64,
    pub wal_digest: [u8; 32],
    pub seal_digest: [u8; 32],
}

pub enum ChainAnchor {
    Genesis { archive_epoch_id: [u8; 16] },
    Segment(SegmentAnchor),
}

pub struct PitrStatus {
    pub state: PitrArchiveState,
    pub archive_epoch_id: Option<[u8; 16]>,
    pub latest_durable_commit_ts: Option<u64>,
    pub latest_archived_commit_ts: Option<u64>,
    pub recoverable_intervals: Vec<RecoveryInterval>,
    pub active_wal_bytes: u64,
    pub sealed_unarchived_wal_bytes: u64,
    pub source_spool_bytes: u64,
    pub archive_lag_commits: u64,
    pub archive_lag_bytes: u64,
    pub oldest_unarchived_recorded_at: Option<SystemTime>,
    pub archive_lag_duration: Option<Duration>,
    pub scheduler_delay: Duration,
    pub repository_staging_bytes: u64,
    pub repository_orphan_bytes: u64,
    pub next_cursor: Option<PitrStatusCursor>,
    pub last_archive_error: Option<PitrArchiveError>,
}

pub enum PitrArchiveState {
    NeverEnabled,
    Disabled,
    Active,
    ReconciliationRequired,
}

pub struct PitrStatusOptions {
    pub cursor: Option<PitrStatusCursor>,
    pub page_size: NonZeroUsize,
}

pub const MAX_STATUS_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const MAX_VERIFY_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();
pub const MAX_VERIFY_SAMPLED_TARGETS: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

pub struct PitrStatusCursor {
    pub catalog_digest: [u8; 32],
    pub catalog_high_water: u64,
    pub interval_index: u64,
}

impl KvEngine {
    pub fn enable_pitr(&self, options: PitrOptions) -> Result<EnablePitrOutcome>;
    pub fn resume_pitr(&self, repository: impl AsRef<Path>) -> Result<PitrResumeOutcome>;
    pub fn create_recovery_point(&self) -> Result<RecoveryPointOutcome>;
    pub fn pitr_status(&self, options: PitrStatusOptions) -> Result<PitrStatus>;
    pub fn close_pitr(&self) -> Result<PitrCloseOutcome>;
    pub fn disable_pitr(&self) -> Result<DisablePitrOutcome>;
    pub fn disable_pitr_allow_gap(&self) -> Result<DisablePitrOutcome>;
}

impl BackupRepository {
    pub fn restore_to(
        &self,
        target: RecoveryTarget,
        destination: impl AsRef<Path>,
        options: PitrRestoreOptions,
    ) -> Result<RestoreToOutcome>;
    pub fn verify_pitr(&self, options: VerifyPitrOptions) -> Result<VerifyPitrReport>;
    pub fn purge_pitr(&self, policy: PitrRetentionPolicy) -> Result<PitrPurgeOutcome>;
}
```

`LsmStorageOptions` gains `pitr_repository: Option<PathBuf>`. On open, a matching
descriptor-validated repository resumes the persisted epoch; it never creates a
new one. Without it, a PITR-enabled database opens readable but write-disabled.
`resume_pitr` attaches such an already-open database to the exact persisted
repository ID/epoch and returns `Resumed`, `ReconciliationRequired`, or a typed
identity/availability error. A wrong repository is rejected. Only
`enable_pitr` after a clean disable or reconciled gap creates a new epoch.

`RecoveryInterval` contains archive epoch, optional inclusive commit and
recorded-time bounds, base backup ID, boundary `ChainAnchor`, and a
`BaseTimeAnchor`. An empty-base singleton uses
`ObservedBoundary { commit_ts: None, observed_at }`; it is independently
restorable but contains no committed timestamp.
`PitrRetentionPolicy` contains `minimum_window`, `retain_timelines`, and
`retain_base_backups`. `VerifyPitrOptions` selects shallow or deep
verification and optional interval/target sampling. Its bounded report page
names verified intervals and a structured first failure, if any; status uses the
same interval ordering to continue listing. Verification uses its own cursor
whose query digest binds selector, depth, and sampling policy; changing any of
them makes the cursor invalid. Purge returns aggregate planned and actual
cleanup counts, and callers page retained intervals through status.
`pitr_status` rejects a page size above `MAX_STATUS_PAGE_SIZE`.
`verify_pitr` rejects a page size above `MAX_VERIFY_PAGE_SIZE` or a deep sample
count above `MAX_VERIFY_SAMPLED_TARGETS`, before opening repository objects.

The public data contracts are:

```rust
pub struct RecoveryInterval {
    pub repository_id: [u8; 16],
    pub timeline_id: [u8; 16],
    pub archive_epoch_id: [u8; 16],
    pub base_backup_id: u64,
    pub boundary: ChainAnchor,
    pub commit_bounds: Option<RangeInclusive<u64>>,
    pub recorded_time_bounds: Option<RangeInclusive<SystemTime>>,
    pub base_time_anchor: BaseTimeAnchor,
}

pub enum BaseTimeAnchor {
    Indexed { segment_id: u64, commit_ts: u64, recorded_at: SystemTime, entry_digest: [u8; 32] },
    ObservedBoundary { commit_ts: Option<u64>, observed_at: SystemTime },
}

pub struct RecoverySelector {
    pub timeline_id: [u8; 16],
    pub archive_epoch_id: Option<[u8; 16]>,
    pub base_backup_id: Option<u64>,
}

pub struct PitrRestoreOptions {
    pub selector: RecoverySelector,
    pub implementations: ImplementationRegistry,
    pub executor_threads: NonZeroUsize,
    pub cache_capacity: usize,
}

pub enum VerifyPitrDepth { Shallow, Deep { sampled_targets: NonZeroUsize } }
pub struct VerifyPitrOptions {
    pub depth: VerifyPitrDepth,
    pub selector: Option<RecoverySelector>,
    pub cursor: Option<VerifyPitrCursor>,
    pub page_size: NonZeroUsize,
}
pub struct VerifyPitrReport {
    pub verified_intervals: Vec<RecoveryInterval>,
    pub next_cursor: Option<VerifyPitrCursor>,
    pub first_failure: Option<SegmentFailureLocator>,
    pub last_verified_commit_ts: Option<u64>,
}

pub struct VerifyPitrCursor {
    pub catalog_digest: [u8; 32],
    pub catalog_high_water: u64,
    pub query_digest: [u8; 32],
    pub interval_index: u64,
}

pub struct RestoreToInfo {
    pub requested_target: RecoveryTarget,
    pub resolved_commit_ts: Option<u64>,
    pub last_applied_commit_ts: Option<u64>,
    pub selected_interval: RecoveryInterval,
    pub replayed_segments: u64,
    pub replayed_batches: u64,
    pub replayed_bytes: u64,
}

pub struct PitrPurgeInfo {
    pub retained_interval_count: u64,
    pub planned_reclaim_segments: u64,
    pub planned_reclaim_bytes: u64,
    pub deleted_segments: Option<u64>,
    pub deleted_bytes: Option<u64>,
    pub oldest_recoverable_commit_ts: Option<u64>,
}

pub struct RecoveryIntervalPage {
    pub items: Vec<RecoveryInterval>,
    pub next_cursor: Option<PitrStatusCursor>,
}

pub struct SegmentFailureLocator {
    pub expected_segment_id: Option<u64>,
    pub decoded_anchor: Option<SegmentAnchor>,
    pub catalog_sequence: Option<u64>,
    pub kind: SegmentFailureKind,
}

pub struct PitrArchiveError {
    pub operation: PitrOperation,
    pub path: PathBuf,
    pub kind: PitrArchiveErrorKind,
    pub source: Error,
}

pub enum PitrResumeOutcome { Resumed, ReconciliationRequired(PitrArchiveError) }
pub struct RecoveryGap {
    pub repository_id: [u8; 16],
    pub timeline_id: [u8; 16],
    pub archive_epoch_id: [u8; 16],
    pub after: ChainAnchor,
    pub last_archived_commit_ts: Option<u64>,
    pub first_uncovered_commit_ts: Option<u64>,
    pub reason: CoverageBreakReason,
}
pub struct PitrRetentionPolicy {
    pub minimum_window: Duration,
    pub retain_timelines: NonZeroUsize,
    pub retain_base_backups: NonZeroUsize,
}
```

`RecoverySelector.timeline_id` is mandatory because one repository may contain
multiple independent timelines. When epoch or base is supplied it must belong to
that timeline; otherwise selection uses the newest compatible recoverable epoch.

The publication-sensitive outcomes are explicit rather than a generic status:

```rust
pub enum EnablePitrOutcome {
    Enabled { repository_id: [u8; 16], archive_epoch_id: [u8; 16] },
    RepositoryPublishedButNotDurable { repository: PathBuf, error: io::Error },
    SourceManifestPublishedButNotDurable { archive_epoch_id: [u8; 16], error: io::Error },
    PublicationUnknown {
        repository: PathBuf,
        repository_id: Option<[u8; 16]>,
        archive_epoch_id: Option<[u8; 16]>,
        request_id: [u8; 16],
        fsync_error: io::Error,
        revalidation_error: Error,
    },
}

pub enum RecoveryPointOutcome {
    Durable(RecoveryPoint),
    CommitPublishedButNotDurable { point: RecoveryPoint, error: io::Error },
    PublicationUnknown { point: RecoveryPoint, fsync_error: io::Error, revalidation_error: Error },
}

pub enum DisablePitrOutcome {
    Disabled { final_point: Option<RecoveryPoint> },
    GapRecorded(RecoveryGap),
    FinalArchivePublishedButNotDurable { point: RecoveryPoint, error: io::Error },
    FinalArchivePublicationUnknown {
        point: RecoveryPoint,
        fsync_error: io::Error,
        revalidation_error: Error,
    },
    SourceManifestPublishedButNotDurable { gap: Option<RecoveryGap>, error: io::Error },
    PublicationUnknown { gap: Option<RecoveryGap>, fsync_error: io::Error, revalidation_error: Error },
}

pub enum PitrCloseOutcome {
    ClosedDurably { final_point: Option<RecoveryPoint> },
    ArchiveNotDurable { point: Option<RecoveryPoint>, error: Error },
    PublicationUnknown { point: Option<RecoveryPoint>, error: Error },
}

pub enum RestoreToOutcome {
    Restored(RestoreToInfo),
    NoRecoverablePoint,
    PublishedButNotDurable { info: RestoreToInfo, error: io::Error },
    PublicationUnknown {
        info: RestoreToInfo,
        rename_error: io::Error,
        revalidation_error: Error,
    },
}

pub enum PitrPurgeOutcome {
    Purged(PitrPurgeInfo),
    CatalogsDurableCleanupIncomplete { info: PitrPurgeInfo, error: io::Error },
    CatalogsPublishedButNotDurable { info: PitrPurgeInfo, error: io::Error },
    PublicationUnknown { info: PitrPurgeInfo, fsync_error: io::Error, revalidation_error: Error },
}
```

Confirmed absence during an ambiguous publication revalidation is returned as
`Err`, matching RFC 022; it is not a publication outcome variant.
`NoRecoverablePoint` remains a normal restore-selection outcome. `RecoveryGap`
contains timeline, abandoned repository and archive-epoch
IDs, last durable archived commit, predecessor `ChainAnchor`, and the first
commit known not to be covered. Every unknown outcome forbids automatic retry.
Async Phase 2 tasks return equivalent terminal variants. Report/info structs
contain the fields specified in Sections 9, 10, and 12; errors retain operation,
path, original I/O error, and revalidation error without erasure.

`PitrPurgeInfo.planned_reclaim_*` is computed before catalog publication and is
valid in every outcome. `deleted_*` is `Some` in `Purged` and
`CatalogsDurableCleanupIncomplete`, after the paired catalog transaction is
durable; the incomplete variant reports actual progress and permits retrying
cleanup only, never catalog publication. Non-durable or unknown publication
outcomes return `None` and perform no deletion.

When PITR is enabled, the existing `close()`/`close_async()` delegate to this
state machine and map a non-durable outcome to an error; callers needing the
full decision use `close_pitr` or its Phase 2 async equivalent. Close stops new
admission, coalesces one recovery-point barrier, and returns
`ClosedDurably` only after archive reconciliation and source-manifest close are
durable. Otherwise it leaves a sealed pinned obligation for reopen and does not
claim a clean close. Drop requests best-effort non-blocking shutdown and never
claims an archive guarantee. Reopen reports the prior unclean close and resumes
archival before WAL reclamation.

`create_recovery_point` is a durability barrier. It syncs all writes admitted
before the barrier, seals the active WAL segment, archives that segment and its
catalog record, and returns a typed publication decision. Only
`RecoveryPointOutcome::Durable` establishes that the point is durable and closes
the reported archive gap; non-durable or unknown outcomes do not. It does not
flush a memtable or create a new full backup.
The barrier linearizes when it stops admission in the commit sequencer: every
reservation before that point is drained into the sealed segment, while later
writes enter its successor and may proceed while archival I/O runs. Concurrent
barriers coalesce on the same or a later sealed boundary and each returns the
greatest commit covered by its own linearization point.

The API above is the target public surface for the completed RFC. Phase 1 keeps
its exact-`CommitTs` prototype crate-private; no public PITR API ships until
Phase 2 supplies every type and behavior shown above, including status,
retention, verification, typed outcomes, and compatibility validation.

Ordinary acknowledged WAL writes remain durable in the source database before
they are necessarily archived. Therefore a source-device loss can lose the
reported archive lag. A clean shutdown and `create_recovery_point` close that
gap only with a `Durable` outcome; shutdown is unsuccessful and the barrier
returns its typed non-durable/unknown outcome otherwise.

Phase 1 is Linux-only and uses RFC 022's trusted-descriptor, no-follow,
no-replace, file-sync, directory-sync, and repository-locking rules.

## 2. Motivation and Comparison

RFC 022 produces crash-consistent incremental backups, but each generation is
only one recovery point. Its backups intentionally flush committed state and
exclude WAL files. Writes after the latest generation cannot be recovered if
the database directory is lost.

RocksDB exposes WAL updates through `GetUpdatesSince` and `TransactionLogIterator`,
which replication and backup systems can use to recover changes after a base
backup. kv-engine needs a narrower operational contract: archive its own WAL,
detect every gap, and restore atomic batches without requiring an application
to reconstruct internal keys or range tombstones.

PITR is useful for recovering from accidental writes as well as hardware loss.
An operator can restore immediately before a destructive batch, inspect the
result, and promote the restored directory separately.

## 3. Goals

1. Recover a WAL-enabled database to an exact committed MVCC timestamp.
2. Recover to the newest durably archived commit.
3. Resolve a wall-clock target conservatively to an archived commit observed at
   or before that time.
4. Preserve atomicity of write batches, transactions, point tombstones, range
   tombstones, TTL values, and vLog-backed values.
5. Detect missing, duplicated, reordered, truncated, corrupt, or incompatible
   WAL segments before publishing a restore.
6. Integrate with RFC 022 backup generations, verification, locking, retention,
   and atomic restore publication.
7. Bound foreground memory and disk consumption when archival is slow.
8. Expose recovery coverage and lag so operators can measure the recovery point
   objective rather than infer it from file ages.
9. Validate seal, archive, catalog, retention, and replay crash windows with RFC
   013 failpoints and process-kill tests.
10. Introduce an ordered commit sequencing protocol (commit allocator plus
    publication watermarker) so each successful MVCC batch has one unique
    recovery coordinate and readers never observe a timestamp frontier with an
    unpublished earlier commit.

## 4. Non-Goals

1. Remote or multi-region storage in Phase 1.
2. Synchronous mirroring of every write to the backup repository.
3. Recovery without any base backup.
4. Restoring into an existing or open database.
5. Continuing the original serializable transaction conflict-history window;
   restored committed data is authoritative, while in-memory transaction state
   starts empty as it does after an ordinary reopen.
6. Treating `SystemTime` as a transactional timestamp or providing a strict
   external-time guarantee.
7. Selective table, key-prefix, or transaction undo.
8. Replaying WAL from an untrusted or incompatible engine build without format
   validation.

## 5. Recovery Coordinates and Guarantees

### 5.1 Commit timestamps

Every non-empty MVCC write batch must have one nonzero `commit_ts`. The WAL
already stores that timestamp in the batch header and recovery ignores
incomplete tail batches. The current split allocation/WAL-sync/publication path
does not by itself guarantee unique timestamps or ordered publication under
concurrent writers, so an ordered commit sequencing protocol is a prerequisite
for PITR. The allocator/watermarker owns timestamp and ticket reservation plus
advancement of one contiguous published frontier. A later commit may not become visible or advance
`latest_commit_ts` while an earlier reservation is unresolved.

The persistent protocol invariants are:

1. commit timestamps are strictly increasing across successfully published
   batches in one database timeline;
2. a timestamp identifies the whole batch, never an individual operation;
3. replay applies a batch only when `commit_ts <= target_commit_ts`;
4. a target inside a nonexistent timestamp gap resolves to the greatest
   archived commit timestamp below it;
5. restoring to a timestamp older than the base backup's included boundary is
   rejected rather than approximated.

A reservation that definitively fails before any complete WAL batch is durable
is not a commit coordinate and may be reused after restart; it was never
externally observable. During the running process the sequencer retires it
before admitting a replacement at that coordinate. Once a complete batch may
be durable, its timestamp is consumed permanently. A WAL fsync outcome whose
durability is unknown poisons the sequencer and blocks new writes until reopen
determines whether the complete checksummed batch is present. Recovery scans
WALs in segment/batch order, reconstructs durable coordinates, publishes only
the ordered valid frontier, and sets the next reservation above every durable
batch timestamp. Segment rotation and recovery-point barriers stop admission
and drain both durable WAL tickets and ordered memtable publication through
their linearization point.

The commit allocator/publication watermarker is a logical protocol, not a
durable standalone object. After a crash, its next timestamp, ticket frontier,
and recorded-time clamp are reconstructed from durable WAL v5 batches, seal
metadata, and v7 manifest state before new admission resumes.

Non-MVCC and legacy WAL batches with `commit_ts == 0` are not archivable. PITR
requires the current MVCC WAL format and `enable_wal = true`.
`RecoveryTarget::CommitTs(0)` is rejected during target validation.
An empty database has no commit coordinate: status fields and a recovery-point
barrier return `None`, `Latest` returns `NoRecoverablePoint` until a base exists,
and empty sealed segments use `commit_range: None` while still participating in
the predecessor chain and backup-boundary anchoring. Timestamp `0` is never used
as a sentinel recovery point.

### 5.2 PITR WAL v5 wire format

PITR uses WAL format version **5**. It is not an interpretation of v4: v2/v3/v4
files remain readable only by legacy recovery and are rejected by
`enable_pitr` until rotated into v5. The fixed 4096-byte file header is
big-endian with this byte layout:

| Offset | Width | Field | Allowed value |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `WAL2` |
| 4 | 2 | version | `5` |
| 6 | 2 | flags | `0` in v5; unknown bits reject |
| 8 | 2 | header_len | `4096` |
| 10 | 2 | reserved | `0` |
| 12 | 16 | timeline_id | fixed bytes |
| 28 | 16 | archive_epoch_id | fixed bytes |
| 44 | 8 | segment_id | big-endian u64 |
| 52 | 1 | predecessor_kind | `0` Genesis, `1` Segment |
| 53 | 3 | reserved | `0` |
| 56 | 8 | predecessor_segment_id | `0` for Genesis |
| 64 | 32 | predecessor_wal_digest | all zero for Genesis |
| 96 | 32 | predecessor_seal_digest | all zero for Genesis |
| 128 | 4 | header_crc32 | CRC over offsets 0..127 |
| 132..4095 | 3964 | reserved | all zero |

A v5 file is valid only when all fixed fields and reserved bytes match this
table. CRC-32 uses the IEEE reflected polynomial `0xEDB88320`, initial value
`0xffffffff`, final XOR `0xffffffff`, matching `crc32fast`.

Each batch has this fixed 40-byte header:

```text
commit_ts:u64 | recorded_at_secs:i64 | recorded_at_nanos:u32 |
entry_count:u32 | data_len:u32 | data_crc32:u32 | header_crc32:u32 |
reserved:u32
```

`recorded_at_nanos < 1_000_000_000`; `data_crc32` covers exactly the following
`data_len` bytes; `header_crc32` covers the preceding fields and excludes both
CRC fields. The entry stream is `entry_count` repetitions of
`kind:u8 | flags:u8 | payload_len:u32 | payload[payload_len]`, with stable tags
`0x01 Put`, `0x02 PointDelete`, and `0x03 RangeDelete`. Entry `flags` must be
zero in v5; unknown kinds/flags reject. Payloads use canonical big-endian,
length-delimited key/value or start/end encoding. No padding is part of a batch;
`entry_count` must be greater than zero and `data_len` must be nonzero for every
commit batch; an empty barrier is represented outside the commit-batch stream.
The decoder must consume exactly `data_len` bytes after parsing exactly
`entry_count` entries; an underflow, overflow, or trailing byte rejects the
batch. Batches are aligned to 4096 bytes. Zero alignment gaps between batches
are included in the logical WAL prefix and validated as zero. The seal sidecar
stores `logical_length:u64` as the byte offset immediately after the final
batch's alignment gap. For a header-only empty segment with `commit_range: None`,
`logical_length` is exactly 4096; no batch or additional alignment gap exists.
For a non-empty segment, `logical_length` must be at least 4096, 4096-aligned,
and no greater than the source file's allocated extent. It never includes preallocated tail
bytes, and every other logical-length/framing interpretation is invalid.

Payload layouts are fixed and use u32 byte lengths:

| Kind | Payload |
| --- | --- |
| `0x01 Put` | `key_len:u32 \| key \| value_len:u32 \| value` |
| `0x02 PointDelete` | `key_len:u32 \| key` |
| `0x03 RangeDelete` | `start_len:u32 \| start \| end_len:u32 \| end` |

Keys and values are arbitrary bytes; lengths must fit the configured maximums,
and range start/end must satisfy the existing range-tombstone ordering rules.
`recorded_at_secs` uses floor division for negative Unix times, so nanos is
always nonnegative and below one billion (for example, -1.5 seconds is
`secs=-2,nanos=500_000_000`). This is the unique canonical representation.

Before assigning `commit_ts` or encoding a v5 envelope, callers' raw user keys
are canonicalized using RFC 005 rules. Duplicate point operations for one user
key collapse to the last operation in caller order; the resulting key appears
at most once. Range-delete entries retain caller order and are not collapsed
with point operations: replay applies the canonical point set and ordered range
set atomically at the shared timestamp, with the existing range ordering rules.
Normal recovery and PITR must consume the same canonical envelope bytes and
produce the same final state.

`CommitTimeHighWater.entry_digest` is the SHA-256 of this exact unframed
preimage, with no length prefix or text encoding:

```text
ASCII("PITR-COMMIT-TIME-V1") || archive_epoch_id[16] ||
segment_id:u64_be || commit_ts:u64_be || recorded_at_secs:i64_be ||
recorded_at_nanos:u32_be
```

The domain string is exactly the UTF-8 bytes shown (no trailing NUL). Verifiers
recompute this digest before accepting an indexed base anchor; the segment ID
binds the time entry to its sealed WAL provenance even after WAL reclamation.

The v5 recovery matrix is strict: valid v5 batches preserve one mixed-operation
boundary and operation order for normal recovery and PITR; v2/v3/v4 recovery
may reopen a database but cannot archive, restore through PITR, or satisfy its
recorded-time/mixed-batch guarantees. A v4-to-v5 rotation drains and freezes
legacy state, writes a v5 successor with a `Genesis`/`ChainAnchor`, and requires
the first base backup before advertising coverage. Unsupported versions, flags,
nonzero reserved fields, malformed lengths, CRC failures, and trailing nonzero
bytes fail closed.

### 5.3 Wall-clock targets

The PITR WAL format adds a checksummed `recorded_at` field to every batch header.
The commit sequencer samples it immediately before ordered WAL enqueue and
clamps it to the preceding value when the wall clock moves backward. Because it
is covered by the batch CRC and WAL durability decision, active-WAL crash
recovery preserves the mapping. The seal sidecar contains a redundant,
length-delimited index of `(commit_ts, recorded_at)` pairs derived and verified
from those batch headers.

The latest clamped `recorded_at` and current-epoch `last_commit_anchor` are
persisted in every source-manifest seal transition and base boundary. A
non-empty sealed segment updates the anchor from its final batch (including its
archive epoch and segment ID); an empty segment carries the current epoch's
previous anchor forward. A new epoch always resets the anchor to `None`. Before writes resume
after reopen, the sequencer
initializes its clamp from the maximum across the source manifest and every
retained/recovered local WAL batch. These source-local artifacts are the sole
write-admission authority; archived WAL reclamation cannot erase the manifest
high-water. Repository catalog replay independently rejects decreasing times
across segments but repository availability is not required merely to seed the
source clock clamp.

`AtOrBeforeSystemTime(t)` resolves to the greatest indexed commit whose clamped
recording time is not later than `t`. This is deterministic but not an
external-time guarantee: recording precedes durable commit and acknowledgement,
so a concurrent batch recorded before `t` may finish afterward. The resolved
exact `commit_ts` is always returned for operator inspection.

The API returns the resolved `commit_ts` in `RestoreToOutcome`. Operators that
need an exact boundary should record `create_recovery_point()`'s result with
their application event and later restore by `CommitTs`.

### 5.4 Recovery window

The recoverable set is the union of intervals supplied by committed base
backups and contiguous archived WAL segments. A status range is advertised only
when every segment after the chosen backup boundary is present and verified.
Catalog metadata never turns a gap into a best-effort restore.

## 6. WAL Segment Lifecycle

The current WAL is owned by the active memtable and may be removed after that
memtable flushes. PITR adds an archive pin and an explicit segment lifecycle:

```text
Active -> Sealing -> Sealed -> Archived -> Reclaimable
                   \-> Quarantined
```

1. **Active:** accepts complete WAL batches.
2. **Sealing:** under the existing write/freeze coordination, stop admission,
   drain the commit sequencer, `fdatasync` the WAL, persist a source-manifest
   `Sealing` intent with its fixed logical length and successor ID, write and
   fsync a separate checksummed `.seal` file, create/fsync the successor WAL
   header and database directory entry, persist `Sealed` plus the new active
   memtable/WAL in one manifest record, and only then release writers.
3. **Sealed:** immutable and pinned against normal WAL deletion. Its filename
   contains a monotonic segment ID; its WAL header and seal sidecar bind database timeline,
   WAL format, segment ID, the complete predecessor `ChainAnchor`, first and last
   `commit_ts` when non-empty, per-batch recorded-time index, batch
   count, logical length, and SHA-256 checksum.
4. **Archived:** the repository object and catalog commit are durable, but the
   source obligation may still await its manifest transition.
5. **Reclaimable:** the source manifest durably records `Archived` and ordinary
   source recovery no longer needs the segment. Repository retention never
   controls release of the source pin.

The seal sidecar leaves the current WAL byte stream valid for ordinary recovery.
The source manifest is authoritative. A crash before durable `Sealing` intent
leaves an active WAL; any unbound sidecar is quarantined and the WAL may accept
more batches. A crash after that intent can no longer reopen the WAL for append:
recovery validates or regenerates the sidecar for the fixed logical length,
installs `Sealed` and its successor, and resumes archival. A crash after
`Sealed` sees a fully sealed segment plus the new active WAL. These rules cover
every sidecar/manifest ordering window without allowing two active segments.

The sealed WAL object is exactly byte range `[0, logical_length)`. It includes
defined zero-filled alignment gaps required by the direct-I/O format and
excludes any preallocated physical tail. Seal hashing, archive copy, restore
verification, `max_segment_bytes`, and `max_unarchived_bytes` all use this same
logical representation. The implementation may durably truncate the sealed
source file to that length before hashing, but never derives identity or
accounting from `st_size` alone.

Source reclamation treats the WAL and its bound `.seal` as one logical cleanup
unit. Once a segment is `Reclaimable`, the engine unlinks the source
`<segment>.wal` and `<segment>.seal`, fsyncs the source WAL/manifest directory,
and only then releases the segment's source-spool reservation. If either unlink
or the directory fsync fails, the obligation and reservation remain recorded for
reopen retry; a successfully removed WAL with a leftover seal is not considered
reclaimed.

The separate total `max_source_spool_bytes` bound charges actual allocated WAL
extents and all reserved preallocation, not just logical length. While PITR is
active, WAL preallocation must first reserve its full capacity from the shared
spool allocator and cannot create uncharged sparse or physical tails. Truncate
or hole-release returns capacity only after allocated-block verification; this
does not change the logical prefix used for archive identity.

Segment IDs are independent of SST IDs. Database creation writes a random
128-bit timeline ID to the manifest. Each enable or re-enable creates a random
128-bit archive epoch ID. Backup snapshots and every archived segment carry
both, preventing a repository from joining histories or epochs that merely
reuse file or commit timestamp values.

PITR enablement, repository identity, timeline, archive epoch, the active segment
ID, every unarchived `Sealing`/`Sealed` obligation, and the complete
`PersistedPitrConfig` are checksummed source-manifest state. The repository path
in `PitrOptions` is only a reopen locator and is never persisted as identity.
The engine persists a sealed obligation before installing its successor active
WAL. Reopen reconstructs archive pins and resumes those obligations before any
flush path may reclaim their WALs. Initial enablement is durable before the
first PITR segment is admitted. Clean disable records its final archived
boundary before releasing pins; forced-gap disable durably records the coverage
break first. Reopen requires matching repository configuration or opens with
writes disabled until the caller supplies it or explicitly accepts a gap.

The source manifest format advances to v7. Every v7 `Snapshot` carries timeline,
PITR enable/disable state, repository and archive-epoch IDs, active segment,
outstanding seal/archive obligations, the complete epoch-scoped `PersistedPitrConfig`
(including all four limits/timers), the last clamped `recorded_at`, the optional
durable `last_commit_anchor: Option<CommitTimeHighWater>` for the current epoch,
and the epoch-genesis anchor,
so manifest snapshot replacement cannot discard PITR state. `EnableIntent`
contains the same immutable configuration before the v7 snapshot is published.
`resume_pitr` must use those persisted values and cannot override them. Changing
any limit or interval requires clean disable, a new epoch, and a new base.
After clean disable, the new archive epoch resets `last_commit_anchor` to `None`.
Writes performed while PITR is disabled are treated like legacy state. The first
base chooses one anchor for its actual included high-water: `Indexed` when that
high-water equals a current-epoch `last_commit_anchor`, otherwise
`ObservedBoundary` with `commit_ts: Some(included_high_water)` for the entirely
legacy/unindexed portion.

Enabling an existing v3-v6 database uses a durable `EnableIntent` transition.
It stops admission, drains and freezes the legacy active WAL/memtable as
non-archivable input to the mandatory first base, creates and fsyncs a new
PITR-format WAL, and publishes a v7 manifest snapshot containing a random
timeline/epoch plus a distinguished genesis `ChainAnchor` before writes resume.
The genesis is only the predecessor root for the first PITR segment. When the
mandatory first base is later captured, it flushes every legacy/boundary
memtable and binds the actual `ChainAnchor` produced by its capture rotation;
that anchor may be genesis only if no PITR batch was admitted. No interval is
advertised before this base commits. If v5 batches were admitted, the base uses
the current epoch's final `last_commit_anchor`; otherwise its time metadata is
`ObservedBoundary { commit_ts: Some(max legacy commit_ts) or None, observed_at:
barrier time }`, never an invented v5 index entry. Before the v7 snapshot, reopen
remains legacy and removes an unbound new WAL. After it, reopen is PITR-enabled,
uses only the recorded active WAL, and reconstructs obligations. Enable fsync
ambiguity is revalidated against the v7 snapshot and reported through
`EnablePitrOutcome`; it is never retried with a second timeline or epoch while
unknown.

### 6.1 Write ordering

The existing write contract remains WAL-sync-before-memtable-publication. PITR
adds these rules:

The commit sequencing protocol is the logical combination of monotonic
commit-timestamp/ticket allocation and contiguous publication-watermark
advancement. It does not own WAL file-offset allocation or WAL I/O scheduling.
The WAL layer MUST materialize batches in ticket order (therefore assigning
physical offsets in that order), while RFC 012 may encode and submit multiple
reserved batches as parallel `pwrite`/io_uring requests, drain one group
`fdatasync`, and observe completion in any order before ordered publication:

```text
canonicalize/encode payload -> data_crc32 -> size/admission
    -> reserve(commit_ts, ticket) -> recorded_at/header_crc32
    -> ordered WAL queue -> parallel pwrite/io_uring -> group fdatasync
    -> ordered publication frontier
```

The implementation must preserve RFC 012's batching and direct-I/O behavior;
the protocol is a correctness frontier, not a global WAL-I/O mutex. The batch
payload is canonicalized, encoded, its `data_crc32` is computed, sized, and
admitted before it reserves its commit timestamp/ticket. After reservation, the
writer samples `recorded_at`, fills the fixed v5 batch header, computes only the
small `header_crc32`, and immediately offers the batch to the ordered WAL queue.
This minimizes the reservation-to-enqueue window and prevents payload encoding
or payload checksumming from creating avoidable head-of-line stalls; thread
preemption can still briefly delay a later ticket.

Publishing a finalized batch into a lock-free ordered WAL slot uses Release
ordering on the slot's `READY` state, and the WAL drainer observes readiness
with Acquire ordering before reading the header or payload. A mutex or channel
queue may replace those atomics only when its handoff provides the equivalent
happens-before relation; Relaxed ticket allocation alone is insufficient.

The reservation unit is one transaction/WAL batch, never one key-value
operation. A reservation uses an atomic ticket/commit counter (Relaxed is
sufficient for allocation); it does not hold a mutex across encoding, payload
checksum, header finalization,
`pwrite`, `io_uring`, `fdatasync`, or memtable work. Completion publishes a
per-ticket durable marker with Release ordering. Frontier advancement must load
each marker with Acquire ordering (or use an equivalent AcqRel operation) before
observing its associated completion metadata. Group commit performs the same
Acquire observation before advancing the covered run. Frontier advancement is
cooperative: a completing writer may advance a bounded contiguous run.
Writers must not
spin-scanning a globally shared completion array indefinitely; stalled or
unknown tickets transition to the existing reconciliation/backpressure state.
The only globally ordered operations are ticket allocation and advancing the
contiguous published frontier. Implementations may shard allocation/frontier
metadata by WAL group or core, provided one durable total order is reconstructed
for barriers and restore.

This is a normative performance constraint: the commit sequencer MUST NOT
serialize WAL writes or fsyncs. Reservation and publication metadata are
serialized logically, while WAL I/O remains parallel and group-committed.

1. the archiver copies only sealed immutable segments;
2. a memtable flush may initiate source segment-pair deletion only after the
   archive pin is released and the source manifest says `Reclaimable`;
3. an RFC 022 backup records `included_commit_ts`, the greatest commit fully
   represented in its canonical manifest snapshot, plus the exact
   included `ChainAnchor`;
4. backup capture rotates the WAL at its write barrier, so later writes belong
   to a strictly later segment;
5. rotation makes the boundary segment wholly included in the backup; the next
   accepted archived segment must name that exact `ChainAnchor` as its
   predecessor, including when the boundary segment contains no batches.

Commit timestamps need not be numerically consecutive. Continuity is proven by
the segment predecessor chain and by strictly increasing batch timestamps, not
by requiring `next_ts == previous_ts + 1`.

### 6.2 Backpressure and failure

`max_segment_bytes` triggers rotation before admitting a batch that would cross
the target segment size. `max_unarchived_bytes` is a hard admission bound over
active, sealing, and sealed-but-not-durably-committed source WAL logical bytes,
including an atomic reservation for the incoming encoded batch. Concurrent
writes reserve under the same admission sequencer and cannot collectively
overshoot the bound. One batch larger than either configured bound is rejected
before WAL admission. Archive staging files are repository usage, reported
separately, and are bounded to one in-flight object per source.

Writes resume when durable archive commits and pin release reduce reserved
source bytes below the exact incoming reservation requirement; there is no
hidden hysteresis. At the limit, new writes fail with
`PitrArchiveUnavailable` before WAL admission and do not create a recovery gap.
Already durable source writes remain valid. The background archiver retries
transient errors with bounded backoff and exposes the last error through
`pitr_status`.

The logical WAL bound reserves one minimum successor WAL header/alignment region
as maintenance headroom outside user batch admission. Rotation begins before
`max_unarchived_bytes - successor_minimum`; sealing may consume that reserved
region even at the user limit, after which archival can release the old segment.
Enablement rejects a bound that cannot hold one maximum allowed segment plus the
successor minimum. Successor creation never waits for archival while holding the
seal/write locks.

Admission also reserves the projected seal-index entry/count bytes for every
batch and rotates before any configured per-segment count/index limit would be
crossed. Header-only and empty seal metadata must fit all configured minima; an
options set that cannot represent one legal empty segment is rejected.
`max_source_spool_bytes` is a hard bound over actual allocated WAL extents and
reserved preallocation, seal sidecars/temporaries, and PITR source-manifest
obligation growth.
Reservations include the incoming batch plus its metadata and are released only
at the durability/reclamation boundary. Hitting either WAL or total-spool bound
rejects admission before the WAL write. Status exposes the total separately
from its WAL components.

Source-spool accounting uses physical bytes for the active manifest, its
snapshot/compaction temporary successor, seal files/temporaries, actual WAL
allocated extents, and reserved WAL preallocation;
it does not pretend appended obligation records disappear when logically
retired. Before admitting a batch, the engine reserves worst-case space for its
seal/index entry, terminal `Archived` or gap record, directory metadata budget,
and one manifest snapshot/compaction successor. Maintenance consumes this
reserved headroom even when user admission is stopped. A manifest compaction
atomically replaces and fsyncs the compact v7 snapshot, then releases physical
reservation only after the old file is unlinked and the directory fsynced. If
actual filesystem allocation exceeds the conservative budget, admission remains
blocked but terminal cleanup retains its pre-reserved space, preventing a
full-spool deadlock.

All source-manifest writers, including ordinary flush, compaction, GC, manifest
snapshotting, PITR transitions, and enable migration, reserve through one atomic
physical-spool allocator before creating or appending bytes. Background installs
wait or fail before exceeding the bound and never consume terminal-maintenance
headroom. Enablement validates the current manifest allocation plus worst-case
v7 migration, one successor snapshot, and terminal record headroom, not merely
fixed metadata plus a segment. Reservation ordering is spool allocator before
state/manifest locks, and maintenance never waits for user admission, avoiding
lock inversion.

Enablement rejects a zero `archive_interval`, zero byte bounds,
`max_segment_bytes > max_unarchived_bytes`, a repository equal to or nested
inside the source path (or vice versa), incompatible ownership/permissions, and
unsupported WAL/manifest/features, or a total spool bound smaller than required
fixed metadata plus one legal segment. Encoded length and duration calculations
are overflow-checked. A single batch whose WAL-plus-index reservation exceeds
any applicable bound is rejected before reservation.

If the repository is unavailable when PITR is enabled, reads, flushes, and
compactions may continue while pinned WALs remain available and the byte limit
has not been reached. Disabling PITR requires a successful final recovery-point
barrier unless the caller uses an explicit `disable_pitr_allow_gap` operation;
that operation first persists a source-manifest `RecoveryGap` containing the
abandoned repository, timeline and epoch IDs, durable predecessor `ChainAnchor`,
first uncovered commit, and reason. Pins may
then be released even if the repository is unavailable. That repository/timeline
pair can never resume archival until repository reconciliation durably records
the same break. After reconciliation, clean or forced-gap re-enable may reuse
the repository identity but always starts a new archive epoch and requires a new
base backup. If the abandoned repository can never be reconciled, the operator
must choose a new repository identity and create a new base. The database
retains its data timeline. Archive epoch is included in source-manifest state, segment headers and
seal sidecars, object names, catalogs, base anchors, status, recovery gaps, and
intervals. This is the explicit escape hatch from write-disabled limbo.

Both disable operations linearize by stopping write admission first and keep it
stopped through their terminal source-manifest decision. Clean disable drains
the sequencer, seals and durably archives the final segment, durably records
`Disabled`, then releases pins; only then may non-PITR writes resume. A
non-durable or unknown transition leaves PITR enabled, admission blocked, and
all obligations pinned for retry/reopen. Forced-gap disable drains publication,
then durably records a source-manifest `RecoveryGap` containing the exact epoch,
predecessor `ChainAnchor`, and first uncovered committed timestamp (or `None`
when no commit is uncovered) before releasing pins and resuming writes. It need
not reach the unavailable repository. Mandatory later reconciliation appends a
`CoverageBreak` with canonically equal identity, predecessor,
first-uncovered-commit, and reason fields before that repository/timeline can be
reused.

For a non-empty active segment, `archive_interval` starts when its first batch
crosses the publication frontier. The maintenance scheduler requests sealing no
later than that interval, subject only to an already-running seal; the sequencer
then provides the precise rotation boundary. Empty segments are not created by
the timer. Size, explicit-barrier, shutdown, and timer requests coalesce, and a
clean shutdown waits for the resulting archive commit. Scheduler delay is
measured and exposed, not silently subtracted from archive lag.

## 7. Archive Repository Format

RFC 022's repository gains these entries:

```text
backup-repository/
├── REPOSITORY_ID            # immutable random 128-bit identity
├── BACKUP_MANIFEST
├── files/
├── generations/
├── PITR_CATALOG
└── wal/
    ├── <timeline>-<epoch>-<segment-id>-<wal-digest>.wal
    └── <timeline>-<epoch>-<segment-id>-<seal-digest>.seal
```

IDs use lowercase fixed-width hexadecimal and digests use 64 lowercase SHA-256
hex digits; catalog validation reconstructs the canonical basename and rejects
collisions or any stored path component.

Repository bootstrap creates `REPOSITORY_ID` with restrictive permissions
through RFC 022's trusted parent descriptor, fsyncs it and the staged root, and
binds its digest into both catalogs before atomically publishing the repository.
The source manifest stores this UUID rather than treating a path as identity.
Every reopen validates it descriptor-relatively; renaming a repository is safe,
while replacing one at the same path is rejected.

An existing RFC 022 repository is migrated under its existing `LOCK` before
PITR enablement. The migrator writes and fsyncs `REPOSITORY_ID.tmp`, an empty
versioned `PITR_CATALOG.tmp`, and a complete successor backup-catalog snapshot
whose root-metadata record binds the repository UUID. It then fsyncs a
`PITR_MIGRATION` descriptor containing the old catalog digest and all successor
digests, installs `REPOSITORY_ID` and `PITR_CATALOG`, installs the successor
`BACKUP_MANIFEST`, fsyncs the root, and marks/removes the descriptor. Open with
an incomplete descriptor validates the old digest and every successor, then
rolls forward before serving an operation; it never guesses or cleans objects
from a mixed state. A bootstrap or migration fsync ambiguity is returned through
`EnablePitrOutcome` using the same reopen/revalidation rules as RFC 022. Existing
generation objects are not rewritten. `enable_pitr` may perform this migration,
but it enables source-manifest state only after migration is durably complete.

`PITR_CATALOG` is an append-only, versioned, length-delimited, checksummed
stream. Records are:

```rust
enum PitrCatalogRecord {
    CommitSegment {
        sequence: u64,
        metadata: SegmentMetadata,
    },
    CoverageBreak {
        sequence: u64,
        repository_id: [u8; 16],
        timeline_id: [u8; 16],
        archive_epoch_id: [u8; 16],
        after: ChainAnchor,
        first_uncovered_commit_ts: Option<u64>,
        reason: CoverageBreakReason,
    },
    RetentionSnapshot { sequence: u64, /* complete retained state */ },
}
```

Phase 1 fixes canonical wire format v1: unsigned integers are big-endian;
timestamps are `(unix_seconds: i64, nanos: u32)` with `nanos < 1_000_000_000`;
durations are `(seconds: u64, nanos: u32)` with the same nanos bound; IDs and digests are fixed-width;
enums use explicit stable tags; maps are key-sorted; and variable fields are
length-delimited. SHA-256 digests cover the canonical record payload, while the
outer frame checksum covers version, length, payload, and sequence. Unknown
versions/tags, out-of-range time values, non-canonical order, and trailing bytes
are rejected.

Record length, batch count, per-batch entry count, key/value length, segment
index bytes, segment count per interval, and catalog bytes have named checked
limits in the format module. Validation and replay stream rather than allocate
from aggregate untrusted lengths. Status paginates recovery intervals with a
composite cursor containing the immutable catalog digest/high-water and the
next interval index; the `Vec` shown in the summary is one bounded page. A
catalog replacement makes an old cursor stale and returns a typed
restart-required error rather than skipping or repeating intervals inside one
`RetentionSnapshot`.

`SegmentMetadata` includes repository, timeline, and archive-epoch IDs, format
versions, its `SegmentAnchor`, predecessor `ChainAnchor`, optional commit
range, batch count, byte length, WAL checksum, the digest of the seal
sidecar and its per-batch clamped recorded-time index, and source
immutable-file identity.

Publication follows RFC 022's object protocol for both the WAL and its bound
seal sidecar: copy through no-follow regular file descriptors, verify length and
SHA-256 while copying, fsync each temporary object, publish with no-replace
rename, fsync `wal/`, append one complete `CommitSegment` containing the
canonical metadata and both object digests, and fsync the catalog. Existing objects
are reused only after their complete metadata and digests validate.

Every record sequence is exactly its predecessor plus one. A Commit is
self-contained and uniquely keyed by repository, timeline, archive epoch, and
segment ID inside `SegmentMetadata`; duplicate or conflicting keys are invalid.
A CoverageBreak applies only to its named repository/timeline/epoch chain and
exact predecessor `ChainAnchor`, including `Genesis` with no object pair.

A segment is visible only when its object and bound `CommitSegment` validate.
If Commit fsync fails, the archiver reopens and revalidates the catalog and
reports committed-but-not-durable, confirmed-absent, or publication-unknown
states using RFC 022's typed outcome pattern. It must not automatically publish
a second identity when publication is unknown.

Because background archival has no waiting caller, an unresolved revalidation
durably marks the archive epoch `ReconciliationRequired` in the source manifest.
The engine retains the segment pin and byte reservation, forbids later catalog
mutation for that epoch, and exposes the state/error through status. If reopen
confirms the Commit is readable, it still retains the pin: presence after a
failed fsync does not prove crash durability. It retries `fdatasync` on the same
catalog prefix and releases the pin only after that succeeds and the source
manifest durably records `Archived`. If reopen confirms absence, it truncates
only an incomplete terminal frame and retries the same segment/object identity.
If uncertainty remains, writes continue only until the admission byte bound is
reached; no automatic retry or successor publication is allowed.

Repository open builds the longest valid predecessor chain per timeline and
archive epoch. It
rejects forks, overlaps, timestamp regressions, metadata/object disagreement,
duplicate commits, cross-chain substitution, and committed records
after a coverage break in the same archive epoch. A later base may start an
advertised interval only in a distinct new epoch.

Record lengths are bounded before allocation. EOF inside the one terminal frame
is an uncommitted crash tail; under the exclusive lock, open truncates it back
to the last complete validated sequence and fsyncs before any append. A fully
present frame with a bad checksum or semantic error, an invalid length, or any
corruption before the terminal incomplete frame fails closed. The last durable
catalog prefix and sequence are revalidated after an ambiguous fsync. Orphan
temporary and uncommitted objects are reclaimable only after
that replay completes.

When the catalog reaches its configured byte/record threshold, the exclusive
repository transaction protocol writes `PITR_CATALOG.snapshot.tmp` containing a
single `RetentionSnapshot`: format version, new sequence/high-water, digest of
the old validated prefix, repository ID, complete retained chain/break state,
and the successor `BACKUP_MANIFEST` high-water plus prefix digest. It fsyncs the file, records it
in the root transaction descriptor, renames/fsyncs it over `PITR_CATALOG`, and
then resumes append at `high_water + 1`. Open accepts a temporary successor only
when all bindings and the base-prefix digest validate; otherwise it keeps the
primary. Retry/open reuse canonical temp names after validation, and orphan
accounting/cleanup is bounded and exposed as repository staging bytes.

## 8. Base Backup Contract

PITR extends, but does not weaken, RFC 022:

1. `GENERATION` records repository, timeline, and archive-epoch IDs,
   a canonical `BaseTimeAnchor`, the exact wholly included boundary
   `ChainAnchor`, manifest format, WAL replay
   format, and active feature/options compatibility metadata.
2. A base backup still contains no WAL and restores independently to exactly
   `included_commit_ts`; an empty base records
   `ObservedBoundary { commit_ts: None, observed_at }` and restores empty. The
   observed boundary is sampled at the capture barrier and is distinct from
   general backup creation-time provenance.
3. The capture barrier holds write admission, memtable freeze, flush install,
   and compaction/GC state mutation exclusion while it rotates the boundary,
   flushes all boundary memtables, captures the exact canonical state/file set,
   and acquires immutable-file pins. Only after those pins and the manifest
   snapshot are fixed may successor writes and background installs proceed.
   Thus selected SST/vLog files include every commit at or below the boundary
   and none above it; long-running object copies occur after exclusion release.
4. Failed backup publication does not interrupt WAL archival; its segments may
   later support another base generation.
5. A backup imported from another repository may be used only if its timeline,
   canonical manifest, and exact boundary-segment anchor bind to the archived
   chain.

Existing RFC 022 generations without timeline and included-boundary fields stay
restorable as ordinary backups but are not PITR bases. The first PITR-enabled
backup upgrades the repository metadata without rewriting old objects.

Wall-clock selection considers an `Indexed` base only when its
`recorded_at <= target`; it never chooses a base already containing a later
recorded commit. An `ObservedBoundary` base is eligible only when its
`observed_at <= target`, so a newly created migration or empty base cannot
satisfy an older wall-clock request. These anchors remain canonical after
covered WAL segments are purged and define interval time eligibility.

If `included_commit_ts` equals the current epoch's durable
`last_commit_anchor.commit_ts`, the base uses `BaseTimeAnchor::Indexed` from
that manifest anchor, including its segment ID. This remains valid when the
boundary WAL is empty or has already been archived and reclaimed: the anchor was
validated when its original
segment sealed. Otherwise, when the included high-water is entirely legacy or
pre-epoch/unindexed, the base uses `ObservedBoundary {
commit_ts: Some(included_commit_ts), observed_at: barrier time }`; an empty
database uses `commit_ts: None`. The generation's `archive_epoch_id` and the
indexed anchor's segment ID, recorded time, and entry digest must match the
current manifest anchor and capture boundary.

Backup commit validates an indexed anchor against the pinned boundary WAL/index
when that segment is non-empty, or against the durable current-epoch manifest
`last_commit_anchor` when the boundary segment is empty. It stores the canonical
anchor in `GENERATION` and binds it through the backup and PITR catalogs. After
retention deletes the original WAL, verification checks the retained anchor and
its digest rather than requiring the removed index. Observed boundaries make no
claim about an unindexed WAL entry and remain conservative about wall-clock time.
Creating any `ObservedBoundary` advances the source clock clamp to
`max(last_recorded_at, observed_at)`. That updated high-water is persisted in
the v7 manifest and fsynced before write admission is released, so a clock
rollback cannot make a subsequent batch's `recorded_at` precede the base
boundary.

## 9. Restore Algorithm

`restore_to` follows RFC 022's exclusive repository-lock and validated pinned
descriptor handoff rather than introducing a weaker shared lock. It pins and
revalidates both catalogs, the base generation, seal sidecars, and every needed
WAL object before releasing the lock for long-running replay. It performs:

1. validate the target, timeline, repository catalogs, and retained object set;
2. resolve a wall-clock or `Latest` target to an optional exact `commit_ts`;
3. choose the newest committed compatible backup whose
   included boundary is not later than the target and whose following segment
   chain covers it;
4. perform a complete streaming validation pass over every required segment,
   including bytes after the target in the final segment, checking every WAL
   header, seal sidecar, checksum, batch CRC, feature tag, timestamp, operation
   order, and batch boundary;
5. materialize that generation into a trusted sibling staging directory using
   RFC 022 restore rules;
6. transform the canonical snapshot into recovery-only state: assign the new
   timeline, set PITR disabled, clear the source repository/epoch/active segment,
   archive pins/obligations and source clamp, create/fsync a fresh recovery WAL,
   persist/fsync the sanitized manifest, and retain the source PITR fields only
   as `RECOVERY_INFO` provenance;
7. open a private recovery writer in a special mode that ignores source PITR
   lifecycle state, with background flush, compaction, GC,
   callbacks, and user access disabled;
8. skip batches already included in the base, apply whole batches through the
   target, and stop before the first later batch;
9. flush replayed state, persist a canonical manifest snapshot and
   `latest_commit_ts`, remove recovery WALs, fsync all files/directories, and
   close the private writer;
10. write and fsync a checksummed `RECOVERY_INFO` containing repository identity, backup
   ID, requested and resolved targets, last applied commit, segment digests,
   and restore time, then fsync the staging directory;
11. publish the destination with no-replace rename and fsync its trusted parent.

`Latest` means the newest commit in a verified interval anchored by a retained
base backup, never merely the newest archived object. If no such interval
exists, restore returns typed `NoRecoverablePoint`; it does not cross a coverage
break or select an unanchored segment.
An empty retained base is independently recoverable: `Latest` restores it with
`requested_target: RecoveryTarget::Latest` preserved and only
`resolved_commit_ts`/`last_applied_commit_ts` set to `None`. It becomes
`NoRecoverablePoint` only when no compatible retained base exists. Recovery
intervals and selection compare `Option<commit_ts>` with the empty boundary
ordered before every real nonzero commit.

Restore staging starts with a fsynced `RECOVERY_IN_PROGRESS` marker containing
a random request ID, repository/base/catalog digests, and requested target.
Confirmed cancellation, validation failure, or ENOSPC before final rename closes
all descriptors and removes only that descriptor-verified sibling staging tree
through the trusted parent. Cleanup failure is returned with the staging path
and marker identity for safe operator retry. After rename success or rename
ambiguity, cleanup is forbidden and the outcome requires destination
revalidation. Restore is not resumable in Phase 1; reopen may reclaim only
marker-valid abandoned staging directories under the same parent policy. Async
cancellation checks occur between bounded streamed writes, never inside atomic
catalog or rename decisions.

If final rename reports an error, restore revalidates the destination through
the trusted parent descriptor and marker/provenance digest. Confirmed absence is
an `Err` and permits a new request; confirmed presence follows the published
path outcome. If revalidation fails, `RestoreToOutcome::PublicationUnknown`
preserves the request identity, rename error, and revalidation error. The caller
must not retry or clean staging/destination until a later revalidation decides
which name owns the published tree.

Replay uses a dedicated internal batch applier. It decodes logical WAL entries
but preserves their original `commit_ts`; it does not allocate timestamps,
perform serializable conflict checks, invoke application callbacks, or append
the archived bytes to a live WAL. Current foreground WAL entries contain inline
kind-prefixed values; vLog pointers are created only while flushing an SST.
Consequently replay into a vLog-enabled recovery writer consumes inline WAL
values and may create new vLog files during its final flush. A future WAL format
that contains vLog pointers must define and validate object-before-segment
publication before PITR accepts it.

The PITR WAL version also replaces the current separate point/range recovery
aggregation with one ordered batch envelope. Each entry carries a point-put,
point-delete, or range-delete tag and length-delimited payload; one header, CRC,
`commit_ts`, and `recorded_at` cover the complete mixed-operation sequence.
Normal recovery and PITR return batch objects without discarding boundaries or
operation order. The private applier validates the entire envelope, then makes
all its operations visible at the shared timestamp as one publication step.
Until this format is active, enablement rejects any configuration/API path that
can emit mixed point/range batches rather than overstating atomic replay.

TTL recovery preserves the encoded absolute expiration timestamp. Visibility
is evaluated against wall time when the restored database is read, not the
historical target's wall time; PITR is not an as-of-time TTL query facility.

The restore validates the entire required segment chain before staging
publication, even when the requested timestamp occurs early in the final
segment. A missing target timestamp is not an error if it lies within a proven
chain; the outcome reports the greatest applied timestamp. Corruption or a
segment-chain gap is an error regardless of the numeric target.

The restored engine allocates its next timestamp above the maximum durable batch
timestamp in the base and replayed history. During staging it receives and
durably persists a new timeline ID, so its future archive cannot accidentally
fork the source timeline. `RECOVERY_INFO` retains the source timeline as
provenance. Ordinary open never performs a deferred timeline transition. An
explicit administrative continuation mode may preserve timeline in a later RFC.

## 10. Retention and Verification

PITR retention is interval-based, not “keep N WAL files.” A policy specifies a
minimum recovery window and a number of base backups. Purge retires and deletes
a segment's repository WAL and `.seal` object together, and only when every
advertised target that could select that segment is either:

1. covered by a newer retained base backup, or
2. older than the published oldest recoverable point.

Purge first selects a finite timeline set: timelines containing a recoverable
commit within `minimum_window` or an empty base whose
an `ObservedBoundary.observed_at` is within it, plus the newest `retain_timelines` timelines by
latest base-boundary time and backup ID. Entire older timelines outside both sets
are unadvertised and may be deleted; at least one newest timeline is retained
when any valid base exists.

For each selected timeline, purge then selects a finite epoch set: epochs containing at
least one recoverable commit whose `recorded_at` is within the minimum window or
an empty base whose boundary observation is within it,
plus epochs owning any of the newest `retain_base_backups` committed compatible
bases across that timeline, ordered by base boundary time then backup ID. Entire
older epochs outside both sets are unadvertised and may be deleted. Within each
selected epoch, purge retains the newest base at or before the cutoff as its
window anchor when one exists; otherwise it retains the earliest independently
restorable base after the cutoff. When selection was caused by a qualifying
recent empty base, that exact base is retained. These anchors are additive to
any base selected by the newest-count rule. Thus the count is
a floor, window anchors are additive only for currently selected epochs, and
epoch churn cannot retain one base forever. Bases from another selected timeline
never consume its per-timeline base count; a broken chain is not independently
recoverable or counted.

RFC 022's existing `BackupRepository::purge(retain)` detects PITR metadata and
returns a typed `PitrRetentionRequired` error without changing either catalog.
It never performs a backup-catalog-only purge on a PITR repository; callers must
use `purge_pitr` and its combined transaction.

The minimum-window cutoff is computed from the same clamped per-batch
`recorded_at` index used by wall-clock restore. Purge retains the anchor chosen
above—newest at/before the cutoff, otherwise earliest after it—and every segment
needed through the newest recoverable commit. Because that retained base can physically restore earlier state, the
advertised interval begins at its included boundary; `minimum_window` is an
at-least retention guarantee, not an exact expiration cutoff. Numeric timestamp
gaps do not split an interval. The repository persists the greatest
retention cutoff time and oldest advertised point in each `RetentionSnapshot`;
a later purge uses `max(previous_cutoff, clamped_now - minimum_window)`, so wall
clock rollback cannot move the cutoff or re-advertise deleted history.

Generation removal remains owned by RFC 022's durable `BACKUP_MANIFEST` purge
protocol. Under the exclusive repository lock, a combined purge first writes
and fsyncs a root transaction descriptor containing both complete successor
catalog snapshots and their digests. It then installs the RFC 022 catalog state
for retained generations, installs a `PITR_CATALOG` `RetentionSnapshot` bound to
that exact backup-catalog high-water/prefix digest, marks the root transaction
complete, and only
then deletes unreferenced WAL/`.seal` pairs and generation objects. Repository open permits no
reader while an incomplete descriptor exists; it validates both successor
snapshots and rolls the transaction forward before cleanup. It never exposes a
mixed catalog pair or attempts cleanup from one. A crash may leak files but
cannot falsely advertise coverage.
Later normal backup creation may append to `BACKUP_MANIFEST`; the binding stays
valid only when the current catalog equals or validates as a descendant of the
bound high-water/prefix. Rewriting or diverging before that prefix is corruption.
At least one independently restorable base backup among the selected timelines
is retained when the repository has any valid base. Source WAL pins
are released only after the archive catalog commit is durable, regardless of
repository retention.

`verify_pitr` checks catalogs, timeline, the predecessor chain, file lengths,
SHA-256 digests, WAL framing, batch CRCs, timestamp ordering, base boundaries,
and every advertised interval. A deep mode performs a restore to sampled
boundaries, including immediately before and at multi-operation batches.

## 11. Concurrency, Crash, and Security Contract

1. Segment rotation serializes with writes and memtable freeze; archival I/O
   does not hold the write lock.
2. Backup, archive publication, verify, restore, and purge use RFC 022's
   exclusive repository lock, with only its validated pinned-descriptor handoff
   for long reads, and never observe a half-published object or catalog record.
3. A crash before a segment catalog commit may leave an orphan but cannot
   advertise it as recoverable.
4. A crash after segment commit but before source-pin release may leak a source
   WAL; reopen validates the archive record before reclaiming it.
5. No restore destination is visible until every required object, replayed SST,
   manifest, and `RECOVERY_INFO` file is durable.
6. Restore never follows symlinks, overwrites a destination, hard-links mutable
   repository files, or trusts filenames in catalog data.
7. Lengths, counts, timestamps, and allocation sizes are checked before use;
   checksum validation is mandatory before replay.
8. Repository corruption or a chain gap fails closed and names the first
   unusable segment and last verified recovery point.
9. Dropping the async form of a barrier or restore requests cancellation but
   cannot roll back a durable archive/catalog commit or published destination;
   typed outcomes follow RFC 022's commit-decision rules.
10. Phase 1 trusts the repository owner and storage administrator. Checksums,
    no-follow traversal, restrictive creation permissions, and ownership/mode
    validation inherited from RFC 022 detect accidental corruption and unsafe
    filesystem substitution; they do not provide authenticity, confidentiality,
    anti-rollback protection, or safety from an attacker able to rewrite both
    catalogs and objects.

## 12. API Outcomes and Observability

`RestoreToOutcome` reports the selected base backup, requested target, resolved
target, last applied commit, replayed batch/byte/segment counts, and either
`Restored`, `NoRecoverablePoint`, `PublishedButNotDurable`, or
`PublicationUnknown`. The unknown variant preserves rename and revalidation
errors and forbids retry. A parent-directory fsync failure after rename is not
retry-safe for the same destination.

Metrics include:

1. current active/sealed WAL bytes, total source-spool bytes, reservations, and
   configured high-water bounds;
2. archive lag in commits, bytes, and elapsed observation time;
3. segment seal, copy, verification, and catalog-commit latency;
4. archive retries and last typed failure;
5. oldest/latest advertised recovery points;
6. restore verification and replay throughput;
7. bytes retained by each base generation and recovery interval;
8. repository staging/orphan bytes, reconciliation state, and catalog paging.

The engine logs timeline, segment ID, commit range, and repository-relative
object identity, but never user keys or values.

Before PITR has ever been enabled, status has `archive_epoch_id: None`, empty
intervals, zero byte/commit counters, `None` times/errors, and state
`NeverEnabled`.
When PITR is disabled after use, status returns the last persisted epoch/state
and marks it `Disabled` rather than synthesizing live lag. Before a usable base exists,
`recoverable_intervals` is empty; after a gap it
contains only independently base-anchored intervals that retention still
advertises. `latest_archived_commit_ts` remains an archival fact and is not presented
as recoverable. `archive_lag_duration` is `None` before the first unarchived
commit and otherwise is the saturating difference between the current clamped
wall-clock sample and `oldest_unarchived_recorded_at`. It is paired with
`archive_lag_commits`, active/sealed byte counts, and scheduler delay; it does
not claim to be the age of the newest source write. `archive_lag_commits` counts
complete published-but-not-durably-archived batches; it is never computed by
subtracting commit timestamps, which need not be consecutive.

Restore uses a dedicated `PitrRestoreOptions`, not arbitrary
`LsmStorageOptions`. Timeline, manifest/WAL versions, MVCC, TTL encoding, value
separation format, merge-operator identity, and compaction-filter identity come
from and must match the generation metadata. Destination path, executor limits,
cache capacity, logging, and other non-persistent resource settings may be
overridden. Validation completes before materialization begins.

`PitrRestoreOptions` contains destination resource settings plus an
`ImplementationRegistry`. The registry resolves stable persisted merge-operator
and compaction-filter semantic IDs to installed implementations and code-version
IDs; missing or mismatched resolution is rejected before staging. Built-in
active filter records remain data, distinct from the implementation semantic
ID. Until merge operators are implemented, generations record `None` and any
non-`None` identity is rejected rather than reconstructed from metadata bytes.

### 12.1 Operational Semantics: RPO and RTO

**Recovery point objective (RPO)** is determined by durable archive lag, not by
`archive_interval` alone. In a source-device loss, acknowledged commits newer
than `latest_archived_commit_ts` may be absent from PITR recovery; current lag
is exposed as `archive_lag_commits`, `archive_lag_bytes`, and
`archive_lag_duration`. A `RecoveryPointOutcome::Durable` from
`create_recovery_point()` establishes an explicit zero-gap boundary through its
returned commit timestamp, or an empty boundary when it is `None`. Repository
outages and scheduler delay can make actual lag exceed `archive_interval`, so
that setting is a rotation target and operational trigger, not an RPO SLA.

**Recovery time objective (RTO)** is not guaranteed by this RFC. Restore time is
primarily affected by the selected base backup's age, the amount of archived WAL
that must be validated and replayed, repository throughput, destination write
throughput, and verification depth. More frequent base backups generally reduce
replay work and therefore improve expected RTO, at the cost of additional base
backup I/O and storage.

## 13. Implementation Plan

### Phase 1: Segment protocol and exact restore

1. Add the commit allocator/publication watermarker, PITR batch timestamp
   format, and persist
   database timeline and the backup boundary-segment anchor.
2. Add WAL seal sidecars, durable source-manifest obligations, archive pins, and
   crash recovery. Replace the unconditional post-flush `remove_file` path with
   pin-aware reclamation and test that exact integration point.
3. Add `PITR_CATALOG` and local immutable-object publication.
4. Implement explicit recovery-point barriers and exact `CommitTs` restore.
5. Support point values/deletes, atomic batches, range tombstones, TTL encoding,
   and current inline-WAL replay with vLog on or off; reject unknown formats at
   enable time.

### Phase 2: Complete format support and operations

1. Add compatibility rejection for any future WAL-carried vLog pointer format.
2. Add compaction-filter identity and all current manifest-format checks. The
   Phase 1 prototype rejects databases using any persistence-affecting identity
   or format whose validation is deferred to this phase.
3. Add wall-clock lookup, `Latest`, status metrics, retention, and deep verify.
4. Add engine-owned async barrier and restore tasks with typed cancellation.

### Phase 3: Follow-up

1. Remote archive sinks with the same immutable-object and catalog contract.
2. Encryption and compression with checksums over canonical plaintext and
   authenticated stored envelopes.
3. Standby tailing, export/import tooling, and explicit timeline continuation.

## 14. Test Plan

1. Restore at the base boundary, between commit timestamps, at each commit, and
   at the latest recoverable commit.
2. Never expose half of a multi-key transaction, batch, or range tombstone.
3. Rotate concurrently with writers, freeze, flush, compaction, backup, and
   shutdown without missing or duplicating a batch.
4. Kill after every seal/sidecar/fsync/rename/catalog/pin-release step and verify
   the advertised recovery interval after reopen.
5. Reject a missing predecessor, fork, overlap, timestamp regression, corrupt
   WAL header/seal sidecar, bad batch CRC, wrong timeline, incompatible format,
   symlink, non-regular file, and changed object.
6. Verify source WAL and `.seal` pair reclamation only after durable archive
   publication, source-manifest `Archived`, and directory fsync; a failed
   unlink/fsync retains the obligation and spool reservation.
7. Exercise archive unavailability below and at `max_unarchived_bytes` and
   prove pre-admission write failure at the limit.
8. Restore after the base's source SSTs and WALs have been compacted or deleted.
9. Verify retention interruption leaks at most objects and never overstates the
   recoverable interval.
9a. Verify repository retention retires/deletes WAL and `.seal` pairs together,
    never one object independently, and preserves both while any interval
    references the segment.
10. Check wall-clock clamping and return of the resolved exact commit.
11. Cover TTL expiration semantics at restore-open time and vLog values at all
    supported sizes before those configurations leave the rejection list.
12. Run process-level chaos with an external durable-operation oracle and
    compare raw committed state at every target against a model snapshot;
    compare TTL visibility separately using the restore/read wall clock.
13. Exercise duplicate-reservation prevention, out-of-order WAL completion,
    definite pre-durability failure/reuse, unknown durability, and
    publication-frontier recovery.
14. Test boundary anchors, empty boundary segments, oversized batches, atomic
    byte reservations, reopen without repository configuration, catalog-pair
    purge crashes, staged timeline persistence, time targets inside a segment,
    low-rate timer rotation, timer/size/barrier coalescing, and forced-gap
    restart into a new archive epoch.
15. Kill at every legacy-to-v7 enable/migration step and verify exactly one
    timeline/epoch/genesis, no advertised pre-base interval, and snapshot survival.
16. Test ambiguous background Commit publication, pin retention, blocked
    successor publication, same-identity retry after confirmed absence, and
    recovery after confirmed presence.
17. Test clean-close failure/retry, non-blocking Drop, reopen of an unclean
    sealed obligation, restore cancellation, ENOSPC, cleanup failure, and rename
    ambiguity without deleting a possibly published destination.
18. Exercise catalog torn writes, complete-frame bit flips, snapshot compaction,
    sequence high-water restart, bounded interval pagination, orphan cleanup, and
    configured metadata/index limits.
19. Test equal segment IDs across timelines/epochs, cross-chain commit/break
    substitution, both predecessor digests, logical-length versus preallocated
    tails, and restart with a reclaimed WAL plus backward wall clock.
20. Verify retention never advertises a boundary later than its retained base
    unless restore enforces it, and that repeated purge after clock rollback
    never re-advertises deleted history.
21. Restore a generation captured with source PITR enabled and outstanding
    sealed obligations; verify the staged/published database is PITR-disabled,
    requires and touches no source repository, has no inherited pins, and keeps
    source lifecycle metadata only in `RECOVERY_INFO`.
22. Race clean and forced-gap disable with writers and crashes; prove admission
    remains stopped through the durable terminal record and that the exact first
    uncovered commit/predecessor is recorded before any pin release.
23. Purge WAL fully covered by a base, then resolve wall-clock targets on both
    sides of its persisted indexed-anchor `recorded_at`; never choose a base containing
    a commit later than the target.
24. Fill segments with tiny batches to their count/index limit and fill the
    total source spool with seal/temp/manifest bytes; verify pre-admission
    rotation/rejection, terminal `Archived` append, manifest snapshot/compaction
    and reopen using reserved maintenance headroom without an unsealable WAL or
    full-spool deadlock.
25. Inject final-rename error plus revalidation failure and verify the restore
    returns `PublicationUnknown`, retains the request identity, and performs no
    cleanup or automatic retry.
26. Force-disable while the repository is unavailable, verify local gap/pin
    ordering, then reconcile an exactly matching repository break before reuse.
27. Race flush/compaction manifest writers with a full source spool and enable
    PITR on a large existing manifest; verify shared reservation ordering and
    maintenance headroom prevent overshoot or deadlock.
28. Create a backup after a `RetentionSnapshot` and after purge/crash recovery;
    validate that descendant catalog appends preserve the bound prefix while
    divergence before its high-water fails closed.
29. Charge sparse/preallocated WAL tails by allocated blocks, reject unreserved
    preallocation at the total spool bound, and release capacity only after
    verified truncate/hole reclamation.
30. Call legacy RFC 022 `purge(retain)` on a PITR repository and verify it returns
    `PitrRetentionRequired` without changing either catalog or deleting a base.
31. Restore from a repository containing colliding timestamps across timelines;
    require the selector and reject mismatched timeline/epoch/base combinations.
32. Reopen with missing, matching, replaced, and unavailable repositories;
    verify write-disable, same-epoch `resume_pitr`, and no accidental new epoch.
33. Fill logical WAL bytes to the user limit and prove reserved successor
    headroom permits rotation/archive progress without exceeding either bound.
34. Reject a wall-clock target earlier than an empty base's durable boundary
    observation, while allowing `Latest` to restore that empty base.
35. Inject migration ambiguity before epoch allocation and clean-disable final
    archive ambiguity; verify every typed outcome is constructible and retry-safe.
36. Enable PITR on a non-empty v4 database with no new writes, and after a
    clean-disable/re-enable cycle with disabled-period writes; verify the first
    base uses `ObservedBoundary`, never fabricates an indexed v5 entry, and
    restores exactly at the included legacy boundary commit timestamp. Add a
    variant with new v5 writes before first-base capture and verify the base
    uses the current epoch's `Indexed(last_commit_anchor)` at its actual
    included high-water.
37. Reopen with persisted PITR configuration and reject attempts to override
    limits/timers through `resume_pitr`.
38. Decode every v5 header/batch/entry field by the byte-layout table, reject
    nonzero flags/reserved bytes, bad tags, alternate CRC parameters, malformed
    lengths, and invalid alignment gaps.
39. Recompute `CommitTimeHighWater.entry_digest` from the exact domain-separated
    preimage, including epoch and segment ID, and reject any field or digest
    mutation after the originating WAL has been reclaimed.
40. Create many expired timelines and epochs and verify retention removes both
    outside the time window/newest-count sets rather than retaining one base per
    timeline or epoch forever.
41. Page status and verify reports across more intervals than one result page;
    verify bounded allocation, cursor continuation, stale-cursor rejection, and
    selector/depth query binding plus purge planned-versus-actual summary counts.
42. Purge the WAL index containing a base boundary and verify its recorded time
    through the catalog-bound entry digest; reject a tampered value or digest.
43. Retain a recent empty timeline/epoch through its boundary observation even
    when it is outside newest-count sets, then expire and reclaim it later.
44. Fail cleanup after durable paired-catalog retirement and verify the purge
    reports actual partial deletion plus cleanup-only retry semantics.
45. Reject status/verification page sizes and deep sample counts above their
    documented maxima before opening repository objects.
46. Reject zero-entry or trailing-data v5 batches and verify exact data-length,
    4096-alignment, zero-gap, logical-length, and preallocated-tail rules.
47. Verify RFC 005 duplicate-key canonicalization and mixed point/range envelope
    bytes produce identical normal-recovery and PITR replay states.
48. Run weak-memory completion tests proving Acquire observation of Release
    durable markers before contiguous frontier advancement.

## 15. Acceptance Criteria

RFC 023 is implemented when:

1. a WAL-enabled database can create a base backup and continuously archive a
   gap-free, verified segment chain;
2. every advertised commit target restores atomically to the model-equivalent
   state without partial batches or later writes;
3. source loss is bounded by the reported durable archive point, and an
   explicit recovery-point barrier reports `Durable` only after its target is
   durable while preserving non-durable and unknown publication outcomes;
4. unsupported WAL, manifest, vLog, and feature combinations fail at PITR
   enable or restore validation rather than during replay;
5. retention cannot delete an object needed by an advertised recovery target;
6. crash and corruption tests prove that metadata never advertises an
   incomplete recovery interval;
7. RFC/docs-only checks pass, followed by the full repository gate when code is
   implemented.
8. Benchmarks compare PITR-enabled and disabled write throughput and p99 latency
   before/after WAL v5, demonstrating that parallel WAL submission remains
   batched and the sequencer does not serialize the I/O path. The benchmark
   matrix uses 1, 4, 8, 16, and 32 writers in two modes (PITR disabled and PITR
   enabled with the archive caught up), and reports throughput, p50/p99 commit
   latency, CPU utilization, and sequencer/frontier contention. A separate
   archive-lag run measures bounded backpressure rather than conflating it with
   sequencer overhead.

## 16. Alternatives Considered

### Keep every live WAL in each backup

This only recovers to backup creation time and duplicates mutable tails. It does
not provide a continuous, independently verifiable history.

### Archive the active WAL by periodically copying it

Copying a growing file creates ambiguous tails and repeated bandwidth, and it
cannot safely release the source WAL. Immutable sealed segments give one
checksum and one publication decision.

### Use wall-clock time as the WAL ordering key

Wall clocks can repeat or move backward and do not define transaction order.
MVCC `commit_ts` is the exact coordinate; wall time is only an indexed operator
convenience.

### Replay through the public write API

That would allocate new timestamps, re-run conflict checks, alter TTL encoding,
and potentially invoke side effects. Recovery requires a private validated
applier that preserves committed metadata.

### Permit best-effort restore across a missing segment

Silently skipping a WAL segment can resurrect deleted data or omit an atomic
transaction. PITR must fail closed at the first gap.
