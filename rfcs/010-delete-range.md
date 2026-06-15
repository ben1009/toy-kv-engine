# RFC 010: Range Tombstones (`DeleteRange`)

**Status:** Proposed  
**Date:** 2026-06-15  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding `DeleteRange` to kv-engine. A range delete writes a
durable **range tombstone** that hides every key in a half-open user-key range:

```rust
engine.delete_range(b"tenant:42:", b"tenant:43:")?;
```

After the call returns, reads at newer timestamps must treat all keys in
`[start, end)` as deleted until a later write recreates them. Unlike compaction
filters, range tombstones are a read-path feature: they provide immediate
logical deletion, while compaction later performs physical cleanup.

The MVP supports half-open bounded ranges over user keys:

```text
DeleteRange { start: Bytes, end: Bytes, ts: u64 }

start <= key < end
```

The feature is intended for contiguous namespace deletion: tenant drops, table
drops, partition drops, expired prefix ranges, and benchmark/test cleanup. It is
not a replacement for compaction filters, which remain better for scattered
keys or value-dependent predicates.

---

## 2. Motivation

Today, deleting a logical namespace requires one point tombstone per key:

1. Scan the namespace.
2. Write one `delete(key)` tombstone per discovered key.
3. Flush those tombstones.
4. Compact all overlapped SSTs until the tombstones and hidden values can be
   garbage-collected.

That works for small ranges, but it has poor write amplification for large
contiguous keyspaces. Dropping a tenant with one million keys writes one million
tombstones, bloats memtables/WALs/SSTs, and adds unnecessary compaction work.

Compaction filters solve a different problem. They can remove matching keys
while compaction rewrites SSTs, but they do not hide data immediately. That is
not acceptable for table drops or tenant drops, where a caller expects the data
to disappear from `get`, `scan`, transaction reads, and prefix scans once the
delete operation commits.

Range tombstones provide the missing two-layer model:

1. A small durable metadata record gives immediate read invisibility.
2. Compaction later removes covered point records and obsolete range
   tombstones.

This mirrors the separation already documented in RFC 009: `DeleteRange` is
the primary mechanism for contiguous namespace deletion; compaction filters are
for eventual cleanup of non-contiguous or predicate-selected data.

---

## 3. Goals

1. Add a public `delete_range(start, end)` API with immediate read invisibility
   for newer snapshots.
2. Preserve MVCC snapshot semantics: readers at timestamps before the range
   tombstone continue seeing older data; readers at or after the tombstone
   timestamp do not.
3. Make later writes in the deleted range visible again when their commit
   timestamp is greater than the range tombstone timestamp.
4. Persist range tombstones across WAL replay, flush, compaction, and manifest
   snapshot recovery.
5. Integrate `get`, `scan`, `prefix_scan`, and transaction reads with range
   tombstone visibility.
6. Let compaction physically drop point entries covered by range tombstones once
   active-reader and overlap rules prove it is safe.
7. Keep the MVP bounded and deterministic: half-open bounded ranges only,
   engine-assigned timestamps only, no user callbacks.
8. Keep the first implementation conservative: standalone range deletes only,
   no mixed same-timestamp point/range batches, no transaction `delete_range`,
   eager tombstone metadata loading, and no range-tombstone GC unless full
   coverage is proven.
9. Follow RocksDB's proven shape where it fits this engine: range tombstones are
   stored separately from point keys, memtables can keep unfragmented range
   records, table readers expose fragmented range-tombstone views, and
   compaction treats range tombstones as first-class metadata that must be
   preserved or safely dropped.

## 4. Non-Goals

1. Unbounded range deletes (`[..end)`, `[start..]`, or full database delete) in
   the MVP.
2. Arbitrary predicates, value-based deletion, TTL evaluation, or callbacks.
3. Predicate-lock serializability for transaction range scans.
4. SQL-level table catalogs, table IDs, namespace generations, or schema
   management.
5. Removing all physical data synchronously before `delete_range` returns.
6. Changing user-key ordering or the MVCC internal-key format.
7. Supporting timestamp-disabled engines; the current engine always initializes
   MVCC state and should treat range tombstones as timestamped records.
8. Mixed point/range write batches in the MVP.
9. Transaction-local range tombstones in the MVP.
10. Lazy or paged range-tombstone metadata loading in the MVP.

---

## 5. RocksDB Model Adopted by This RFC

RocksDB's `DeleteRange` design is the baseline for this RFC, adjusted for
kv-engine's MVCC timestamp model and simpler SST format. The important
takeaways are:

1. A range delete is a half-open interval tombstone, not a batch of point
   deletes.
2. Range tombstones are not inserted into point-key data blocks or bloom
   filters. They live in separate range-deletion metadata.
3. Memtables retain range tombstones as cheap raw records, conceptually
   `start -> end` plus the write sequence number. They are not expanded into
   point tombstones and are not fragmented on the write path.
4. Read paths derive the newest visible covering tombstone for a key by
   comparing the point version timestamp with the range tombstone timestamps
   visible to the reader.
5. SST/table readers should expose a fragmented range-tombstone view:
   overlapping tombstones are split into non-overlapping spans so point and scan
   reads can advance through tombstone metadata in key order.
6. Compaction must merge point entries and range tombstone metadata together.
   It can physically drop covered point entries only when it keeps the covering
   tombstone or proves the tombstone is obsolete for the whole affected
   interval.

The MVP should not implement every RocksDB optimization, such as lazy loading,
snapshots through sequence numbers, or advanced tombstone compaction heuristics.
It should adopt the correctness model: separate metadata, half-open intervals,
raw memtable tombstones, fragmented read views for SSTs, and conservative
compaction cleanup.

---

## 6. Semantics

### 6.1 Range Shape

All range tombstones are half-open user-key ranges:

```text
[start, end)
```

A key is covered when:

```text
start <= user_key && user_key < end
```

The API rejects empty or inverted ranges:

```text
start >= end  -> error
```

The MVP requires both bounds to be finite. Prefix deletion should use the
existing prefix upper-bound helper:

```rust
let end = prefix_upper_bound(b"tenant:42:")
    .ok_or_else(|| anyhow!("prefix has no finite upper bound"))?;
engine.delete_range(b"tenant:42:", &end)?;
```

If a prefix has no finite upper bound, `delete_prefix(prefix)` is unsupported
in the MVP. Callers that need that case must either use scan plus point deletes
or wait for a later unbounded-range design. Range tombstones stored by the MVP
are always finite, so prefix scans only need to check finite tombstones that
overlap the scan interval.

### 6.2 MVCC Visibility

`delete_range(start, end)` receives a commit timestamp `delete_ts` from the
same MVCC timestamp allocator used by `put`, `delete`, and `write_batch`.

For a reader at `read_ts`, a point version `(key, value_ts)` is hidden by a
range tombstone when all conditions hold:

```text
start <= key < end
value_ts <= delete_ts
delete_ts <= read_ts
```

This means:

1. Snapshots before the range delete still see earlier data.
2. Snapshots at or after the range delete hide earlier data.
3. Writes after the range delete are visible again.

Example:

```text
put("tenant:42:a", "old")  @ ts=10
delete_range("tenant:42:", "tenant:43:") @ ts=20
put("tenant:42:a", "new")  @ ts=30

read_ts=15 -> "old"
read_ts=25 -> None
read_ts=35 -> "new"
```

### 6.3 Interaction With Point Tombstones

Point tombstones and range tombstones are independent deletion records.

For one user key, visibility is determined by the newest visible deletion or
put record at or before `read_ts`:

1. A put is visible if it is newer than every visible point tombstone and range
   tombstone covering the key.
2. A point tombstone hides only that key.
3. A range tombstone hides all keys in its interval.
4. A later put recreates the key.

Read code should implement this by comparing the candidate value timestamp with
the newest covering range tombstone timestamp, not by materializing synthetic
point tombstones.

### 6.4 Read-Your-Writes

A successful `delete_range` is immediately visible to subsequent operations on
the same engine handle. If the delete is written into the active memtable, the
range-tombstone index must be updated before the public call returns.

### 6.5 Durability

Durability follows the same policy as point writes:

| Configuration | After `delete_range` returns | After flush to SST | Crash behavior |
| --- | --- | --- | --- |
| `enable_wal = true` | Durable in WAL | Durable in SST | Recovered from WAL or SST |
| `enable_wal = false` | In-memory only | Durable in SST | May be lost before flush |

When WAL is enabled, `delete_range` must be durable before it returns. Recovery
must reconstruct both point records and range tombstones from WAL and manifest
state. It is not acceptable for keys to reappear after restart because a
WAL-protected range tombstone lived only in memory.

When WAL is disabled, the API remains useful for tests and ephemeral engines,
but it is not crash-durable until the containing memtable has flushed. The
public API must document this explicitly.

---

## 7. API

### 7.1 Public Storage API

Add:

```rust
impl KvEngine {
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()>;
}

impl LsmStorageInner {
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()>;
}
```

`delete_range` should:

1. Validate `start < end`.
2. Validate encoded key lengths for both bounds.
3. Reserve one commit timestamp without publishing it to new readers.
4. Append the range tombstone to the WAL when WAL is enabled.
5. Insert the range tombstone into the active memtable.
6. Update in-memory range-tombstone summaries/indexes.
7. Publish the commit timestamp only after the tombstone is visible to readers.

Errors are atomic before the WAL append: if validation, reservation, or WAL
append/sync fails, the range tombstone is not published and no reader may
observe a read timestamp that includes it. After a durable WAL append succeeds,
the operation is committed for crash recovery; the in-memory publish steps
should minimise the chance of failure (e.g. by pre-allocating where the data
structure permits it), but the implementation must handle an OOM after WAL
append by aborting the engine rather than leaving a committed tombstone
unpublished.

| Condition | Result |
| --- | --- |
| `start >= end` | Error: invalid range |
| encoded `start` or `end` exceeds key-size limit | Error: key too large |
| WAL append or sync fails when WAL is enabled | Error; timestamp is not published |
| mixed `DelRange` and point records in one batch | Error: unsupported mixed batch |
| transaction `delete_range` in MVP | Error: unsupported |
| `LsmStorageOptions::serializable` enabled for non-transactional `delete_range` | Error: unsupported serializable conflict tracking |
| prefix has no finite upper bound | Error: unsupported unbounded range |
| unsupported manifest/WAL/SST version | Error before serving reads |

### 7.2 WriteBatch API

Extend `WriteBatchRecord`:

```rust
pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
    DelRange(T, T),
}
```

The MVP rejects any batch that contains `DelRange` together with `Put` or `Del`.
This avoids ambiguous same-timestamp ordering. With the MVCC visibility rule in
Section 6.2, a same-timestamp `Put(k)` inside a same-batch `DelRange(a, z)`
would be indistinguishable from a pre-delete put unless the storage format grew
an intra-batch sequence number.

Allowed MVP batches:

1. Point-only batches: existing `Put`/`Del` behavior.
2. Range-only batches: one or more `DelRange` records sharing one commit
   timestamp.

Range-only batches are applied atomically. A later phase may add ordered mixed
batches by storing and comparing `(commit_ts, sequence_number)` for both point
records and range tombstones.

### 7.3 Transaction API

Transaction `delete_range` is deferred in the MVP:

```rust
impl Transaction {
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()>;
}
```

The method may exist for API discoverability, but it must return an unsupported
error for both serializable and non-serializable transactions until a
transaction-local range-tombstone overlay is designed.

Reason: the current transaction implementation buffers point writes only. A
correct transaction-local `delete_range` would need:

1. Local range tombstones that affect `get`, `scan`, and `prefix_scan` before
   commit.
2. Explicit ordering against local point puts/deletes.
3. Commit encoding into WAL, memtable, and conflict tracking.
4. Serializable range-write validation against point and range reads.

While `LsmStorageOptions::serializable` is enabled, non-transactional
`delete_range` is also unsupported in the MVP. Current OCC conflict tracking is
point-key based; a range delete could otherwise commit after a serializable
transaction read and fail to appear in that transaction's conflict checks.

---

## 8. Data Model

### 8.1 In-Memory Representation

Introduce a timestamped range tombstone:

```rust
use bytes::Bytes;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Bytes,
    pub end: Bytes,
    pub ts: u64,
}
```

`start` and `end` are raw user keys, not encoded internal keys. Keeping raw user
keys avoids repeated decode work when checking user-key visibility.

Each memtable should own two logical collections:

```rust
pub struct MemTable {
    map: SkipMap<Bytes, Bytes>,
    range_tombstones: RangeTombstoneSet,
}
```

`RangeTombstoneSet` should support:

```rust
impl RangeTombstoneSet {
    pub fn add(&self, tombstone: RangeTombstone) -> Result<()>;
    pub fn newest_covering_ts(&self, user_key: &[u8], read_ts: u64) -> Option<u64>;
    pub fn overlaps(&self, start: &[u8], end: &[u8], read_ts: u64) -> bool;
    pub fn iter_overlapping(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = RangeTombstone>;
}
```

The memtable representation intentionally stores tombstones unfragmented, as
RocksDB does for the write path. That keeps `delete_range` cheap and avoids
rewriting existing memtable metadata on every new overlapping range.

This mirrors RocksDB's in-memory shape: a range tombstone is stored under its
start user key with the end user key and write timestamp as metadata:

```text
memtable range-delete entry:
  key   = start
  value = end
  ts    = delete_ts
```

The memtable does not synthesize one point tombstone per covered key. It also
does not fragment overlapping ranges when they are inserted. Fragmentation is a
read/flush/compaction view over these raw records, not the active write
representation.

The MVP uses the same `SkipMap` backing the point memtable, with a dedicated
range-tombstone map that stores raw tombstones keyed by `(start, ts, ordinal)`:

```rust
pub struct RangeTombstoneSet {
    raw: Arc<SkipMap<RangeTombstoneKey, Bytes>>,
    min_start: AtomicBytes,
    max_end: AtomicBytes,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct RangeTombstoneKey {
    start: Bytes,
    ts: u64,
    ordinal: u32,
}
```

`ordinal` disambiguates multiple `DelRange` entries in a range-only batch that
share the same `(start, ts)`. `ordinal` is assigned as a zero-based sequential
index within each range-only batch (0, 1, 2, ...) and is always less than the
batch's `entry_count`.

**Active memtable: no shared fragment cache.** The active memtable does not
maintain a shared `fragment_cache`. The write path can insert new tombstones
concurrently with readers, making a shared cache vulnerable to a stale-install
race:

1. Reader sees `cache = None`, starts building a fragment view from the current
   `raw` snapshot.
2. Writer inserts a new tombstone and invalidates the cache (still `None`).
3. Writer publishes `delete_ts` via the timestamp allocator.
4. Reader finishes building the old fragment view and installs it into the
   cache.
5. A new reader with `read_ts >= delete_ts` uses the stale cache that omits the
   newly published tombstone.

This is not a snapshot-consistency issue; the new reader in step 5 is not an old
snapshot — it is a reader that started after the tombstone was published. It
must see the tombstone.

To avoid this race, the active memtable uses direct raw-skipmap queries:

- **Get:** Scan `raw` tombstones whose `start <= key` and whose `(start, ts)`
  satisfies `ts <= read_ts`. Cost is `O(R)` per lookup.
- **Scan:** Build a private, per-scan fragmented view from the raw skipmap.
  The view is not shared across scans. Cost is `O(R log R)` per scan start.

This matches RocksDB: the active memtable stores raw `kTypeRangeDeletion`
entries in a dedicated skip-list; readers query raw entries directly. The
fragmented view (`FragmentedRangeTombstoneList` in RocksDB) is built only for
immutable memtables and SSTs.

For active memtable point reads, the direct query is:

```text
newest delete_ts where start <= key < end and delete_ts <= read_ts
```

The returned timestamp is then compared with the candidate point version
timestamp. A range tombstone newer than the reader's snapshot is ignored, while
an older covering tombstone still hides older point versions.

Range tombstones count toward memtable size. `delete_range` must add
`start.len() + end.len() + metadata overhead` to `approximate_size` and call the
same freeze path as `put` and `delete`. `MemTable::is_empty()` must return
`false` when range tombstones are present, even if there are no point entries.

### 8.2 SST Representation

Range tombstones should be persisted in SST metadata, not mixed into the point
key/value block stream. They are metadata that covers many point keys; storing
them as synthetic point keys would complicate ordering, bloom filters, and scan
deduplication.

Add a new SST format version:

```text
SST v4 = MVCC + prefix bloom + range tombstones
```

The footer points to a range-tombstone block:

```text
[data blocks][meta][bloom][prefix bloom][range tombstone block][footer]
```

The range-tombstone block contains sorted records:

```text
record_count: u32
repeated:
  start_len: u16
  end_len: u16
  ts_count: u32
  start: [u8]
  end: [u8]
  repeated ts: u64
crc32
```

Each persisted record is one fragmented span plus the full set of tombstone
timestamps covering that span. This is required for MVCC snapshot correctness:
a newer covering tombstone can be invisible to an older reader, while an older
covering tombstone for the same span must still hide old point versions.

The range-tombstone block stores fragmented records. Flush and compaction may
accept raw overlapping range tombstones as input, but the table builder must
fragment them before writing the SST metadata block. On `SsTable` open, the
table reader eagerly decodes and validates the block, then caches the fragmented
view. This matches RocksDB's model more closely than storing overlapping raw
records in each table.

The fragmented view consists of non-overlapping spans:

```rust
pub struct RangeTombstoneFragment {
    pub start: Bytes,
    pub end: Bytes,
    pub covering_ts: Vec<u64>,
}
```

For each fragment `[start, end)`, `covering_ts` contains the tombstone
timestamps covering every key in that fragment, sorted in ascending order
(oldest first). This enables binary search via `partition_point` to find the
last `ts <= read_ts`. If a key is covered by multiple tombstones, visibility
uses the newest covering timestamp that is visible to the reader. Adjacent
fragments with the same timestamp set may be coalesced. Fragments with no
covering tombstone are omitted.

Each fragment's `covering_ts` must contain every tombstone timestamp that covers
every key in that fragment's `[start, end)` range. It is not sufficient to store
only the newest timestamp; older readers at earlier snapshots may need older
covering timestamps from the same span.

During compaction, fragmented tombstone views from upper and lower levels are
merged using an interval-union algorithm: walk both sorted fragment lists in key
order, union the `covering_ts` vectors for overlapping spans, deduplicate, and
produce a sorted ascending result.

Fragmentation is part of the MVP for SSTs and immutable read views. It is not
merely an optimization: it gives scans and compaction a deterministic
key-ordered tombstone iterator and prevents repeated overlap searches across
dense deleted ranges.

SST v4 footer:

```text
bloom_offset: u32
prefix_bloom_offset: u32       // 0 if absent
range_tombstone_offset: u32    // 0 if absent
max_ts: u64
magic: u32
version: u8                    // 4
```

The fixed v4 footer tail is 25 bytes. `SsTable::open` must reject unsupported
versions before serving reads. v2 and v3 files remain readable by the v4 binary.
New files are written as v4 only after the manifest has been upgraded.

`max_ts` is the maximum timestamp across all entries in the SST, including range
tombstones:

```text
sst.max_ts = max(max_point_entry_ts, max_range_tombstone_ts)
```

For a range-only SST, `max_ts` is the maximum range-tombstone timestamp even
though there are no point entries. The engine uses `max_ts` during recovery to
rebuild the MVCC timestamp allocator; if range-tombstone timestamps were
excluded, the allocator could reassign timestamps that are already committed,
breaking MVCC correctness.

Range-only SSTs are valid. A range-only SST contains no point data blocks, but
it must contain a range-tombstone block and coverage metadata:

```rust
pub struct SsTableRangeCoverage {
    pub point_first_key: Option<Bytes>,
    pub point_last_key: Option<Bytes>,
    pub file_lower_bound: Option<Bytes>,
    pub file_upper_bound_exclusive: Option<Bytes>,
    pub has_range_tombstones: bool,
    pub tombstone_min_start: Option<Bytes>,
    pub tombstone_max_end: Option<Bytes>,
}
```

`point_first_key` / `point_last_key` are the actual first and last point keys
stored in the SST (both inclusive). They are `None` for range-only SSTs.

`file_lower_bound` / `file_upper_bound_exclusive` define the logical
semi-open partition `[file_lower_bound, file_upper_bound_exclusive)` that the
SST owns within its level. These fields are only used for range-only SSTs,
where they equal the tombstone coverage range. For point SSTs (including mixed
point+tombstone SSTs), these fields are `None`; point-file selection uses the
existing `point_first_key` / `point_last_key` binary search.

**Mixed SST tombstone truncation rule.** A mixed point+tombstone SST's range
tombstones are intersected with the SST's point span:

```text
fragment_start = max(tombstone.start, point_first_key)
fragment_end   = min(tombstone.end,   next_valid_user_key(point_last_key))
```

If `fragment_start >= fragment_end`, the tombstone does not overlap this SST
and is not written. The complement of all mixed-SST fragments within the
original tombstone range must be carried by range-only output SSTs.

`next_valid_user_key(key)` returns the smallest user key strictly greater than
`key`, respecting the engine's maximum encoded-key length:

```rust
fn next_valid_user_key(key: &[u8]) -> Option<Bytes> {
    if key.len() < MAX_USER_KEY_LEN {
        // Can append a zero byte.
        let mut out = BytesMut::with_capacity(key.len() + 1);
        out.put(key);
        out.put_u8(0x00);
        Some(out.freeze())
    } else {
        // At max length; find the last byte that can be incremented.
        let mut out = BytesMut::from(key);
        for i in (0..out.len()).rev() {
            if out[i] != 0xff {
                out[i] += 1;
                out.truncate(i + 1);
                return Some(out.freeze());
            }
        }
        // All bytes are 0xff and at max length; no valid successor exists.
        // This means the point SST covers the entire key space up to the limit.
        // The tombstone has no suffix beyond this SST.
        None
    }
}
```

If `next_valid_user_key` returns `None`, the mixed SST covers the entire
remaining key space and no suffix range-only SST is needed. The function
operates on raw user keys, not encoded internal keys with sequence numbers.

This rule is required because the current engine's L1+ Get uses binary search
on `first_key`/`last_key`. If a mixed SST's tombstone extended beyond
`next_valid_user_key(point_last_key)` but its `last_key` were `m`, a Get for
key `y` would skip the SST (`m < y`) and miss the tombstone, potentially
resurrecting lower-level values.

This rule is required because the current engine's L1+ Get uses binary search
on `first_key`/`last_key`. If a mixed SST's tombstone extended to `z` but its
`last_key` were `m`, a Get for key `y` would skip the SST (`m < y`) and miss
the tombstone, potentially resurrecting lower-level values.

Point-file ordering and point-iterator selection use point-key bounds only.
Range-tombstone pruning uses tombstone coverage bounds only. Compaction closure
examines both. L0 coarse relevance checks point overlap OR tombstone overlap.
L1+ point SSTs carry tombstones truncated to `successor(point_last_key)`;
range-only SSTs are checked through the level's `range_only_ssts` list via
linear scan. A tombstone-only SST is therefore discoverable even though it has
no point-key range.

This requires relaxing current point-data-only SST invariants:

1. `SsTable` first/last point keys become optional, or move into
   `point_first_key` / `point_last_key`.
2. `SsTable::open` must accept an SST with empty point block metadata when a
   non-empty range-tombstone block is present.
3. `SsTableBuilder::is_empty()` must consider pending range tombstones, not only
   point data.
4. Point iterators over a range-only SST are simply empty; range-tombstone
   iterators still participate in read visibility and compaction.

Range-only SSTs may reside in any level. Each L1+ level tracks them in a
separate `range_only_ssts` list alongside `point_ssts`. During compaction, the
compaction must attach and truncate range tombstones to output SST point
boundaries where possible; when a covered interval has no surviving point keys,
the compaction must emit a range-only output SST into the target level's
`range_only_ssts` list (Section 9.5, invariant 7).

**Range-only SST lookup: linear scan for MVP.** Range-only SST coverage can
overlap (e.g. `[a, z)` and `[m, n)` in the same level). Sorting by
`tombstone_min_start` alone does not enable correct binary search: the nearest
`min_start` may not be the file whose coverage actually spans the query key.
The MVP therefore linearly scans all `range_only_ssts` in a level, checking
each file's `tombstone_min_start <= key < tombstone_max_end`. This is correct
regardless of overlap. If the count of range-only SSTs per level grows large, a
later optimisation can add prefix-max-end metadata or enforce non-overlapping
coverage during compaction.

### 8.3 Manifest Representation

Bump the manifest format version:

```rust
pub const MANIFEST_FORMAT_VERSION: u32 = 4;
```

Version meanings become:

```text
2 = MVCC
3 = MVCC + compaction filters
4 = MVCC + compaction filters + range tombstones
```

Manifest snapshots do not need to list every range tombstone separately if they
are stored inside live SSTs and WAL-backed memtables. Snapshot recovery already
lists live SST IDs and immutable memtable IDs. The format version bump is still
required so older binaries reject databases containing SST v4 files.

Upgrade behavior:

1. Existing manifest v3 databases with only SST v2/v3 files can be opened by a
   v4 binary.
2. Once upgraded, downgrade to an older binary is unsupported; older binaries
   reject manifest v4 or SST v4 before serving reads.
3. Manifest snapshot retention must treat range-only immutable memtables as
   live. `imm_memtable_ids` must include unflushed memtables that contain only
   range tombstones.

**Manifest v3→v4 upgrade state machine.** The upgrade happens eagerly at
database open, not lazily on the first `delete_range`. This avoids the
complexity of upgrading an active WAL mid-session. The current code requires
`detected_version == MANIFEST_FORMAT_VERSION` before serving reads; the upgrade
must complete before that check.

```text
open v3 database
  ↓
recover all v2 WAL entries and v2/v3 SSTs into memtable state
  ↓
atomically write and fsync a manifest v4 snapshot
  ↓
freeze/rotate the current WAL v2 memtable (if non-empty)
  ↓
create a new WAL v3 active memtable
  ↓
publish state and begin serving reads and writes
```

The key ordering constraints:

1. The manifest v4 snapshot must be durably written before any WAL v3 or SST v4
   artifact is created. This ensures recovery never sees a v4 artifact while the
   manifest still claims v3.
2. The existing WAL v2 memtable must be frozen and flushed before the new WAL v3
   memtable accepts writes. This avoids appending WAL v3 typed entries to a WAL
   v2 file, which would corrupt the format.
3. The upgrade is a one-time operation. Once the manifest is v4, subsequent opens
   skip the upgrade path.

**Recovering `point_ssts` / `range_only_ssts` per level.** The manifest
snapshot persists all live SST IDs per level. Recovery uses a two-phase
approach because the current engine's `apply_compaction_result` sorts output
SSTs by `first_key()`, which does not exist for range-only SSTs.

Phase 1 — replay manifest to raw topology:

```text
for each manifest record:
    apply to RawLevelTopology (SST IDs per level, no file I/O)
determine final live SST ID set
```

Phase 2 — open SSTs and classify:

```text
for each live SST ID:
    open the SST file
    read SsTableRangeCoverage
    if point_first_key is None and has_range_tombstones:
        add to range_only_ssts
    else:
        add to point_ssts
sort point_ssts by point_first_key within each level
construct and publish LsmStorageState
```

This avoids a manifest format change to persist the two lists separately. The
classification is deterministic: it depends only on the SST file content, not on
runtime state. Phase 1 must complete before any SST is opened, so that the
final live set is known. Phase 2 must complete before the state is published
for reads.

### 8.4 WAL Representation

Extend WAL entries to include range tombstones:

```text
EntryKind::Put
EntryKind::PointTombstone
EntryKind::RangeTombstone
```

This requires WAL format v3:

```text
header:
  magic: u32 = "WAL2"
  version: u16 = 3

batch:
  commit_ts: u64
  entry_count: u32
  data_crc32: u32
  typed entries...
```

Typed entries:

```text
Put:
  kind: u8 = 0
  key_len: u16
  value_len: u16
  key
  value

PointTombstone:
  kind: u8 = 1
  key_len: u16
  key

RangeTombstone:
  kind: u8 = 2
  start_len: u16
  end_len: u16
  start
  end
```

The range tombstone timestamp is the batch `commit_ts`; it is not repeated in
the entry. WAL v2 files remain readable and contain only point entries. WAL v3
files must be rejected by older binaries through the version field.

`Wal::recover` must return or populate both point entries and range tombstones
before any reads can run. The current point-only recovery API is not enough; the
range-delete implementation should use an explicit carrier such as:

```rust
pub struct RecoveredWalBatch {
    pub points: Vec<(Bytes, Bytes)>,
    pub point_tombstones: Vec<Bytes>,
    pub range_tombstones: Vec<RangeTombstone>,
    pub max_ts: u64,
}
```

Alternatively, `Wal::recover` may receive both the point skiplist and the
`RangeTombstoneSet` to populate. Either way, recovery must not drop a memtable
just because its point skiplist is empty; a range-only WAL is live data.

**Recovery interleaving.** Each WAL batch is decoded into either a point batch
(`Put`/`PointTombstone` entries) or a range-only batch (`RangeTombstone`
entries). Mixed batches are rejected by the writer (Section 7.2) and must be
rejected by the reader. Point batches populate the memtable's point skiplist;
range-only batches populate the memtable's `RangeTombstoneSet`. `max_ts` in
`RecoveredWalBatch` must include range-tombstone timestamps (it is the batch
`commit_ts`). The recovery API must distinguish batch types and route entries
accordingly.

---

### 8.5 Compatibility Summary

| Artifact | Old files read by v4 binary | New v4 artifact read by old binary |
| --- | --- | --- |
| Manifest v3 | Yes, then upgraded before first range tombstone | No |
| WAL v2 | Yes, point entries only | N/A |
| WAL v3 | Yes | No |
| SST v2/v3 | Yes | N/A |
| SST v4 | Yes | No |

Format errors are fail-closed. A database must not serve reads after detecting
an unsupported manifest, WAL, or SST version.

---

## 9. Read Path

### 9.1 Point Get

`get(key)` currently finds the newest visible point version at or before
`read_ts`. With range tombstones, it must also find the newest covering range
tombstone at or before `read_ts`.

Pseudo-code:

```rust
let candidate = newest_visible_point_version(key, read_ts)?;
// range_ts is the maximum covering delete_ts across ALL sources,
// not just the level where candidate was found.
let range_ts = newest_covering_ts_across_all_sources(key, read_ts);

match candidate {
    None => None,
    Some((value_ts, value)) if range_ts.is_some_and(|ts| value_ts <= ts) => None,
    Some((_value_ts, value)) => Some(value),
}
```

The range-tombstone lookup must examine range-tombstone metadata from ALL
sources, regardless of which level provided the point entry candidate:

1. Active memtable.
2. Immutable memtables.
3. L0 SSTs, newest to oldest.
4. L1+ point SSTs whose point-key range overlaps `key` (found via existing
   point-file index). These carry tombstones truncated to
   `[point_first_key, successor(point_last_key))`.
5. Range-only SSTs in each L1+ level whose `[tombstone_min_start,
   tombstone_max_end)` overlaps `key` (found via per-level linear scan).

`newest_covering_ts_across_all_sources` must return the maximum `delete_ts`
across all sources where `delete_ts <= read_ts` and the tombstone covers `key`.
It must not stop at the first covering tombstone found. The L0 point-key search
and tombstone search can be done in the same newest-to-oldest pass, but both
the newest put and the newest covering tombstone must be tracked independently.

For single point queries (`get`), the active memtable always queries raw
tombstones directly (`O(R)`) — it has no shared fragment cache (Section 8.1).
Immutable memtables and SSTs use their cached fragmented view (`O(log F)`).

The MVP performs a simple metadata scan over overlapping memtables and SSTs.
Each SST contributes its fragmented tombstone view rather than its raw
overlapping records. The MVP does not maintain a global range-tombstone index in
`LsmStorageState`; see Section 9.5 for the design invariants that govern this
choice.

The range-tombstone view used by a read must come from the same immutable state
snapshot as the point data being read. Each memtable and SST owns its own
range-tombstone metadata; readers must not load point data from one state and
tombstone metadata from another.

Read helpers must be classified explicitly:

1. User-visible helpers (`get`, `scan`, `prefix_scan`, conditional writes, and
   transaction reads when later supported) are visibility-aware and must consult
   range tombstones.
2. Physical maintenance helpers may bypass range tombstone visibility only when
   their caller explicitly reasons about physical versions. vLog GC liveness and
   compaction cleanup are not allowed to treat range-covered versions as live
   user data once the covering tombstone is safe under the MVCC watermark.

### 9.2 Scan

`scan(lower, upper)` must skip point versions hidden by range tombstones. The
scan iterator already collapses MVCC versions by decoded user key. Add one more
visibility check before yielding a key:

```text
if newest_covering_range_ts(user_key, read_ts).is_some_and(|ts| value_ts <= ts):
    skip user_key
else:
    yield user_key
```

The iterator should not expand range tombstones into point keys. It should
query tombstone metadata for each candidate user key.

Implementation boundary: `LsmIterator` or a wrapper around it must receive a
`RangeTombstoneView` and `read_ts`. The existing tombstone/version skipping
logic must skip an entire decoded user-key group when the selected visible point
version has `value_ts <= newest_covering_range_ts(user_key, read_ts)`.
This check belongs inside the MVCC user-key collapse loop, not only after a key
has already been yielded. Otherwise a scan can incorrectly fall through to an
older version of the same user key that is also hidden by the range tombstone.

Scan implementations should cache the current covering tombstone fragment while
iterating. When the scan key advances past the fragment end, the iterator
advances the fragmented tombstone iterator in key order. It should not
recompute the same overlapping tombstone set for every internal version in a
dense deleted range.

#### Merged Range-Tombstone Iterator

A scan iterates point entries from a merged point iterator that combines the
active memtable, immutable memtables, L0 SSTs, and L1+ SSTs. The range-tombstone
view must also be merged across all those same sources. Unlike `get`, which can
query each source independently and take the max, a scan needs a key-ordered
merged tombstone iterator that produces fragments with the union of covering
timestamps from all sources, advancing alongside the point iterator.

The merged range-tombstone iterator is built once per scan from the immutable
state snapshot:

1. Collect fragmented views from all sources: the active memtable's
   `RangeTombstoneSet` (build a private per-scan fragmented view from the raw
   skipmap, not a shared cache), each immutable
   memtable's cached fragments, each L0 SST's cached fragments, and each L1+
   SST's cached fragments (including `range_only_ssts`).
2. Merge these sorted fragment lists into a single key-ordered stream. Where
   spans from different sources overlap, the merged fragment's `covering_ts` is
   the sorted union of all sources' timestamps for that span.
3. The scan advances this merged iterator in lockstep with the point iterator.
   For each candidate user key, the scan checks the current merged fragment to
   determine the newest covering timestamp visible to the reader.

This is analogous to RocksDB's `FragmentedRangeTombstoneIterator`, which merges
fragments from all live SSTs and memtables into a single key-ordered stream. The
merged iterator is O(S * F log(S * F)) to build (where S is the number of
sources and F is the average fragment count), but only once per scan; each
per-user-key check is O(1) amortised.

`LsmIterator` (or a wrapper around it) receives the merged
`RangeTombstoneIterator` and `read_ts`. The range-tombstone check is inserted
inside the MVCC user-key collapse loop, between the `ts > read_ts` skip and the
point-tombstone check. The iterator maintains a cursor into the merged fragment
stream so that advancing past a fragment end is O(1) amortised.

### 9.3 Prefix Scan

`prefix_scan(prefix)` is equivalent to a bounded scan over
`[prefix, prefix_upper_bound(prefix))` when the prefix has a finite upper bound.
It inherits scan semantics.

If the prefix has no finite upper bound, the scan may continue using the
existing fallback for point keys, but MVP range tombstones are still finite. The
fallback only needs to check finite tombstones that overlap yielded candidate
keys; it does not imply support for unbounded range tombstones.

### 9.4 Bloom Filters

Point bloom filters and prefix bloom filters remain indexes over point keys.
Range tombstones must not be inserted into those filters. A bloom miss can only
prove no point key exists in that SST; it cannot prove no range tombstone covers
the searched key. Therefore:

1. `get` may use point bloom filters to avoid reading point data blocks.
2. `get` must still consult range-tombstone metadata for SSTs whose key range
   could cover the target key.
3. `prefix_scan` may use prefix bloom filters to skip point-key data from an
   SST when the filter proves the SST has no matching point candidates. Range
   tombstone metadata must still be consulted separately if that SST's
   tombstone coverage overlaps the scan range, because those tombstones can hide
   point candidates from other sources.

In other words, bloom filters decide whether to read this SST's point blocks.
They do not decide whether this SST's range-tombstone metadata participates in
visibility.

### 9.5 Design Invariants

The following invariants govern the read-path interaction between point data and
range tombstones. They ensure correctness without requiring Get to open every
SST or maintain a global range-tombstone index.

1. **No global range-tombstone index.** No global range-tombstone index is
   planned. Range tombstones remain owned by their mutable memtable, immutable
   memtable, or SST. If read-path overhead becomes excessive, the engine should
   address it through fragment caching, tombstone compaction, per-SST coverage
   metadata, or per-memtable tombstone limits — not a state-level index.

2. **Per-file range-tombstone views.** Each mutable memtable, immutable
   memtable, and SST independently owns its range-tombstone metadata. Active
   memtables answer point lookups by scanning the raw `RangeTombstoneSet`
   directly (`O(R)`); they have no shared fragment cache to avoid the
   stale-install race (Section 8.1). Scan builds a private per-scan fragmented
   view from the raw skipmap. Immutable memtables and SSTs expose shared
   fragmented views (`Vec<RangeTombstoneFragment>`). SSTs build the view
   eagerly at `SsTable::open` time. Immutable memtables build the view lazily
   on first read after freeze via `OnceCell`, which guarantees exactly-once
   initialization with no concurrent writer invalidation.

3. **SST coverage metadata for pruning.** Each SST stores lightweight coverage
   metadata (`SsTableRangeCoverage`: `has_range_tombstones`, `tombstone_min_start`,
   `tombstone_max_end`) alongside its point-key bounds. The MVP eagerly decodes
   and caches the fragmented tombstone view at `SsTable::open` time, so coverage
   metadata does not avoid I/O. Its role is to let the read path skip SSTs
   whose cached tombstone fragments cannot cover the target key, avoiding
   unnecessary fragment-vector lookups.

4. **Point bloom filters do not prune range-tombstone lookups.** A bloom miss
   proves no point key exists in that SST; it does not prove no range tombstone
   covers the searched key. The read path must consult range-tombstone metadata
   for SSTs whose key range or tombstone coverage range overlaps the target key,
   regardless of bloom filter result.

5. **L0 queries all relevant files.** L0 SSTs may overlap in key range, so Get
   must check every L0 file whose point-key range or tombstone coverage range
   overlaps the target key. It is not safe to stop at the first L0 file that
   contains a range tombstone covering the key, because a newer L0 file may
   contain a point put that recreates the key.

6. **L1+ tombstones are intersected with the output SST's point span.** During
   compaction, when a range tombstone is written into a mixed point+tombstone
   L1+ output SST, the tombstone fragment is the **intersection** of the
   original tombstone range and the output SST's point span:

   ```text
   fragment_start = max(tombstone.start, point_first_key)
   fragment_end   = min(tombstone.end,   next_valid_user_key(point_last_key))
   ```

   If `fragment_start >= fragment_end`, the tombstone does not overlap this
   SST's point span and is not written into it. For example, a raw tombstone
   `[m, n)@50` written into an SST whose point keys are `{a, z}` produces
   `[m, n)@50` (not `[a, z\0)@50` which would incorrectly expand the deletion
   range). A tombstone `[a, z)@50` written into an SST with points `{m, x}`
   produces `[m, x\0)@50`.

   The **complement** of all mixed-SST fragments within the original tombstone
   range must be carried by range-only output SSTs. This complement includes:

   - The **prefix gap**: from `tombstone.start` to the first point SST's
     `point_first_key` (e.g. `[a, m)` when the first point SST starts at `m`).
   - **Inter-SST gaps**: between adjacent point SSTs (e.g. `[d\0, m)` between
     SSTs ending at `d` and starting at `m`).
   - The **suffix gap**: from the last point SST's `successor(point_last_key)`
     to `tombstone.end`.

   This ensures that L1+ point lookup via the existing point-file index
   (binary search on `first_key`/`last_key`) discovers the relevant range
   tombstones without scanning the entire level, while no tombstone coverage
   is lost.

7. **Range-only SSTs.** A memtable containing only range tombstones flushes into
   a range-only SST. L0 may contain range-only SSTs. Each L1+ level maintains a
   separate `range_only_ssts` file list alongside its `point_ssts` list; this is
   not a global index, only per-level bookkeeping. Because coverage can overlap,
   the MVP linearly scans all `range_only_ssts` in a level (Section 8.2).
   Range-only SSTs count toward level size for compaction triggering.
   During compaction, the selector must include range-only SSTs whose coverage
   overlaps the selected point SSTs' key ranges. The compaction should attach and
   truncate range tombstones to output SST point boundaries wherever possible.
   When a covered interval has no surviving point keys in the output, the
   compaction must emit a range-only output SST into the target level's
   `range_only_ssts` list. Get at L1+ checks the point SST via the existing
   point-range binary search, then linearly scans `range_only_ssts` whose
   `[tombstone_min_start, tombstone_max_end)` overlaps the key.
   A range-only SST may seed a compaction when no point SST in the same level
   overlaps the range-only SST's coverage. Its tombstone coverage becomes the
   initial compaction range. The selector must include all overlapping point SSTs
   and range-only SSTs in the target and lower levels. Without this rule,
   range-only files in a level with no point SSTs could never compact downward.

8. **Range-tombstone visibility is timestamp-based, not location-based.** A
   range tombstone in the active memtable must never hide a point entry whose
   encoded timestamp is strictly greater than the tombstone's timestamp, even if
   both reside in the same memtable. Visibility is always determined by comparing
   the point version's timestamp against the covering tombstone's timestamp, never
   by the physical location of the entries.

---

## 10. Compaction

### 10.1 Physical Cleanup

During compaction, a point entry can be dropped when it is covered by a visible
range tombstone and no active reader can still observe it:

```text
entry_ts <= range_delete_ts <= gc_watermark
the covering range tombstone is retained in the output, or proven obsolete
the compaction input is a full key-range closure for the covered interval
```

The overlap rule is critical. Dropping a point value without preserving the
covering range tombstone can resurrect older lower-level values. Existing point
tombstone GC already has similar bottom-level and overlap constraints; range
tombstone GC should reuse that reasoning.

The MVP must be conservative:

1. It may drop covered point entries only when it also carries the covering
   range tombstone into the output, unless the full interval is proven clear.
2. It may not rely on target level alone. "Bottommost" is necessary but not
   sufficient; the compaction must include every live SST whose point-key range
   or tombstone coverage overlaps the interval being cleared.
3. For L0, the closure includes every overlapping L0 file plus all overlapping
   lower-level files.
4. For leveled/simple/tiered compaction, the closure is strategy-specific but
   must include all files that could contain older versions or tombstones for
   the interval.

**Compaction input selection with range tombstones.** When an upper-level SST
contains range tombstones, the overlapping-file scan for lower levels must use
the union of the upper-level SSTs' point-key ranges AND their tombstone coverage
ranges. The existing `find_overlapping_ssts` logic selects lower-level files
based on point-key overlap only; it must be extended so that an L0 range
tombstone covering `[a, z)` causes all L2 files in `[a, z)` to be included,
not only those overlapping L1's point-key ranges. Without this, the "full
key-range closure" invariant is not achievable.

**Range-only SSTs in compaction input selection.** When the compaction selector
picks point SSTs for a level, it must also include any range-only SST in that
level whose tombstone coverage overlaps the selected point SSTs' key ranges.
Range-only SSTs count toward level size for compaction triggering. Because
coverage can overlap, the MVP linearly scans `range_only_ssts` for overlapping
candidates (Section 8.2).

**Range-only SST as compaction seed.** When a level's size exceeds the limit
and no point SST overlaps a range-only SST's coverage, the range-only SST may
seed a compaction. Its tombstone coverage becomes the initial compaction range.
The selector must include all overlapping point SSTs and range-only SSTs in the
target and lower levels. Without this rule, range-only files in a level with no
point SSTs could never compact downward.

### 10.2 Tombstone Retention

A range tombstone may be dropped only when both are true:

1. It is older than or equal to the MVCC GC watermark.
2. There is no live point entry in any non-compacted lower level that it still
   needs to hide.

For leveled compaction, the easiest safe MVP rule is:

```text
Keep range tombstones unless compacting a full key-range closure to the
bottommost level.
At the bottommost level, drop a range tombstone or fragment only after all live
files overlapping its interval are included and all point entries it covers have
been removed.
All live files includes both point SSTs and range-only SSTs in the level whose
key range or tombstone coverage overlaps the fragment's interval.
The "interval" for a fragment is [fragment_start, fragment_end), not the
original tombstone's interval. A fragment can only be dropped after all live
files overlapping the fragment's own interval are included.
```

This is conservative but correct. Because SST and compaction views are already
fragmented, compaction can drop only fully-cleared fragments while preserving
the remaining covered subranges.

### 10.3 Fragmentation

Overlapping range tombstones must be fragmented before they are used as SST
read metadata or compaction metadata. RocksDB does this so iterators can merge
point keys and range tombstones in key order without repeatedly searching all
overlapping ranges.

For example, these raw tombstones:

```text
[a, z) @ 10
[m, p) @ 20
[b, c) @ 30
```

produce these fragments:

```text
[a, b) covering_ts=[10]
[b, c) covering_ts=[10, 30]
[c, m) covering_ts=[10]
[m, p) covering_ts=[10, 20]
[p, z) covering_ts=[10]
```

The fragmenter can be implemented as a simple sweep-line algorithm over finite
range endpoints:

1. Create start/end events for each raw tombstone.
2. Walk endpoints in user-key order.
3. Maintain the active tombstone timestamps.
4. Emit `[prev_endpoint, endpoint)` with the active timestamp set when the
   active set is non-empty.
5. Coalesce adjacent fragments with the same timestamp set.

This is required for SST reader views, immutable-memtable read views, and
compaction input views. Active memtables may still answer point lookups from
the unfragmented `RangeTombstoneSet`, but immutable memtables must expose the
same MVCC-correct fragmented view as SSTs before they participate in scans,
flush, or compaction.

Compaction output splitting must preserve tombstone coverage across the full
tombstone interval. When a wide range tombstone spans multiple output SSTs, the
compaction must ensure coverage for every sub-interval:

1. For each output SST with point data, compute the **intersection** of the
   tombstone range and the SST's point span:

   ```text
   fragment_start = max(tombstone.start, point_first_key)
   fragment_end   = min(tombstone.end,   next_valid_user_key(point_last_key))
   ```

   If `fragment_start < fragment_end`, write this fragment into the SST's
   range-tombstone block.

2. The **complement** of all mixed-SST fragments within the original tombstone
   range must be covered by range-only output SSTs. This complement includes:
   - **Prefix gap**: `[tombstone.start, first_point_SST.point_first_key)`.
   - **Inter-SST gaps**: `[successor(prev_SST.point_last_key), next_SST.point_first_key)`.
   - **Suffix gap**: `[last_point_SST.successor(point_last_key), tombstone.end)`.

These are complementary, not alternatives. A single wide tombstone typically
requires both: intersected fragments in point SSTs for intervals with point
data, and range-only SSTs for the complement. For example, a tombstone
`[a, z)@50` compacted into output SSTs with point keys `{m, x}` produces:

```text
mixed SST:           [m, x\0) @50
range-only prefix:   [a, m)   @50
range-only suffix:   [x\0, z) @50
```

Without the prefix range-only SST, a lower-level point entry at key `b` would
not be covered by the tombstone and could be incorrectly exposed.

This ensures that the existing point-file index (binary search on
`first_key`/`last_key`) can discover all mixed SSTs whose tombstones cover a
given key, while range-only SSTs are found via per-level linear scan.

It is not sufficient to place the tombstone only in the output SST whose point
keys contain the tombstone start. This is the same correctness constraint that
motivates RocksDB's fragmented tombstone iterator: tombstone coverage is over
intervals, not point-key ownership.

For each output SST, the range-tombstone block contains all input tombstone
fragments (from both upper and lower levels) that overlap the output SST's
point-key range, truncated to that range. The fragment-merge algorithm
(Section 10.3) handles combining upper-level and lower-level fragments.

### 10.4 vLog GC

Range tombstones do not directly reference vLog files. They make older point
entries unreachable. Once compaction drops those covered point entries, the
normal SST-to-vLog reference accounting must unregister their value pointers.
Range tombstone visibility alone is not permission to unlink vLog data while a
published SST still contains the covered value pointer.

The vLog invariant remains:

```text
A vLog file can be deleted only after no live SST references it.
```

Range tombstone cleanup must not delete vLog files directly.

vLog GC liveness checks must be range-aware inside compaction and publication
code that also updates SST-to-vLog reference accounting. A physical value
version covered by a visible range tombstone is not live user data once
`entry_ts <= range_delete_ts <= gc_watermark`, but the deletion invariant is
unchanged: a vLog file can be unlinked only after no published live SST
references it.

---

## 11. Concurrency and Crash Safety

### 11.1 Atomic Publication

`delete_range` must publish the range tombstone atomically with its timestamp:

1. Acquire the same MVCC write lock used by `put`, `delete`, and
   `write_batch`.
2. Reserve `delete_ts` without advancing the globally visible latest timestamp.
3. Pre-reserve memtable/index capacity where the data structure permits it and
   fail before WAL append if reservation fails.
4. Append to WAL if enabled.
5. Insert into the active memtable's `RangeTombstoneSet` (`raw`, `min_start`,
   `max_end`).
6. Advance the globally visible latest timestamp to publish `delete_ts`.
7. Release the write lock.

Readers that acquire a snapshot after step 7 must see the range tombstone.
Readers that started earlier use their existing `read_ts` and remain isolated.
No reader may observe `read_ts >= delete_ts` before the tombstone is visible.

**Freeze ordering.** After step 7 (release write lock), the implementation may
call `try_freeze_memtable` if the memtable exceeds `target_sst_size`. The
`active_memtable_lock` read guard from the `delete_range` call must be dropped
before `try_freeze_memtable` acquires the write lock; otherwise the same thread
would deadlock. This is the same ordering requirement as for `put` and `delete`.
The `delete_range` size contribution (`start.len() + end.len() + metadata`) must
be accounted before the freeze check.

### 11.2 Memtable Freeze

When the active memtable is frozen, its range tombstones move with it into the
immutable-memtable list. The frozen memtable wraps the raw `SkipMap` in an
`ImmutableRangeTombstoneSet` that adds a one-time fragment cache:

```rust
pub struct ImmutableRangeTombstoneSet {
    raw: Arc<SkipMap<RangeTombstoneKey, Bytes>>,
    min_start: Bytes,
    max_end: Bytes,
    fragments: OnceCell<Arc<[RangeTombstoneFragment]>>,
}
```

The immutable set is read-only after freeze. `OnceCell` guarantees the fragment
view is built exactly once, with no concurrent writer invalidation. All readers
of the same immutable memtable share the cached fragment view. This avoids the
stale-install race that affects the active memtable (Section 8.1).

Flush must write both point entries and range
tombstones into the new SST before the immutable memtable can be removed from
state.

A memtable containing only range tombstones is not empty. It must freeze, flush,
recover, and be retained in manifest snapshots the same way as a memtable with
point entries. Flush of a range-only memtable produces a valid range-only SST or
a mixed SST whose range-tombstone metadata is discoverable through the normal
SST manifest state. The MVP does not introduce a second durable range-tombstone
artifact outside SST/WAL files.

`SsTableBuilder` must accept range tombstones via an `add_range_tombstone`
method. `SsTableBuilder::is_empty()` returns `false` when any range tombstone
has been added, even if no point entries exist. When building the SST, the
builder fragments the pending range tombstones and writes the range-tombstone
block before the footer. For a range-only memtable, the resulting SST has no
data blocks but has a valid range-tombstone block and coverage metadata.

**Flush guard (Phase 1/2).** The flush path must refuse to flush a memtable
containing range tombstones until SST v4 write support exists (Phase 3). This
guard must be a compile-time or config gate, not a runtime heuristic. The guard
must be present in the same PR as the Phase 2 test hook; CI must enforce this.
Without this guard, a range-only memtable flushed before Phase 3 would produce
an SST with no range-tombstone data, and the WAL deletion would permanently
lose the tombstones.

### 11.3 Compaction Publication

Compaction must publish new SSTs and remove old SSTs atomically under the
existing state lock. If a compaction drops point entries because a range
tombstone covers them, the range tombstone that justifies the drop must either:

1. Be present in the new SST set, or
2. Be proven obsolete under the bottommost-level GC rule.

### 11.4 Recovery

Recovery order:

1. Recover manifest and live SST state.
2. Eagerly load and validate SST range-tombstone metadata.
3. Replay WALs for active and immutable memtables, including range tombstones.
4. Rebuild `next_commit_ts` from the max of all live SST `max_ts` values and
   all recovered WAL batch `max_ts` values. This must include range-tombstone
   timestamps; without this step, range-only SSTs or WALs with high timestamps
   could cause timestamp reuse after restart.
5. Publish `LsmStorageState`.

No reads may run between steps 1 and 5.

---

## 12. Performance

### 12.1 Expected Costs

`DeleteRange` trades small read-path overhead for large write-amplification
reduction.

Expected MVP costs:

1. `delete_range`: O(1) WAL append plus O(log R) insertion into the active
   range-tombstone set, plus memtable size accounting.
2. `get`: extra lookup for newest covering range tombstone, normally skipped by
   summary bounds when no tombstone can cover the key.
3. `scan`: extra visibility checks with cached covering spans.
4. Compaction: extra coverage checks and range-tombstone metadata writes.

### 12.2 Indexing Strategy

No global range-tombstone index is planned. Each data source owns its own
range-tombstone metadata:

```text
active memtable: SkipMap<RangeTombstoneKey, Bytes> + summary bounds (no shared fragment cache)
immutable memtable: same raw SkipMap + OnceCell fragment cache
per SST: sorted Vec<RangeTombstoneFragment> + coverage metadata
```

The active memtable has no shared fragment cache to avoid the stale-install race
(Section 8.1). Get scans raw tombstones (`O(R)`). Scan builds a private
per-scan fragmented view (`O(R log R)`). The immutable memtable and SSTs use a
shared fragment cache (`O(log F)` per lookup).

If read-path overhead becomes excessive, address it through:

1. Freeze-and-flush — force the active memtable to immutable so its fragment
   cache is built once and shared.
2. Tombstone compaction — compact overlapping tombstones early to reduce `R`.
3. Per-SST coverage metadata — skip SSTs whose tombstones cannot cover the key.
4. Per-memtable tombstone limits — freeze memtables when range-tombstone count
   exceeds a threshold, forcing flush and reducing active lookup cost.

### 12.3 Cache Interaction

Range-tombstone metadata should be loaded with the SST metadata and kept in the
`SsTable` object. It is small compared with point data blocks and should not use
the block cache. If workloads create very large tombstone sets, a later design
can page tombstone blocks separately.

The MVP eagerly loads and validates all SST range-tombstone metadata during
`SsTable::open`. Lazy metadata loading is deferred because read-path I/O errors
and first-read latency spikes would complicate correctness.

Memory must be accounted. Add range-tombstone metadata bytes to table stats and
expose a configurable soft limit. If the limit is exceeded, the engine should
surface this through stats and prefer compactions that reduce tombstone
metadata. Hard rejection of new range tombstones can be added later if needed.

### 12.4 Observability

Add:

```rust
pub struct RangeTombstoneStats {
    pub active_count: usize,
    pub immutable_count: usize,
    pub sst_count: usize,
    pub metadata_bytes: usize,
    pub covering_lookups: u64,
    pub covering_hits: u64,
    pub covered_point_drops: u64,
    pub tombstone_drops: u64,
}

impl KvEngine {
    pub fn range_tombstone_stats(&self) -> RangeTombstoneStats;
}
```

These counters are required to diagnose read-path regressions and verify that
compaction eventually removes covered data.

### 12.5 Benchmark Gates

Before implementation is considered complete, benchmark:

1. `get` baseline with 0, 1, 100, and 10k non-covering tombstones.
2. `get` with covering tombstones across active memtable, immutable memtable,
   L0, and lower levels.
3. `scan` over fully deleted dense ranges.
4. `prefix_scan` with non-overlapping and overlapping tombstones.
5. Flush and compaction with range-only and mixed point/range memtables.
6. Recovery/open time with 0, 1, 100, and 10k tombstones.

Default acceptance gate: no more than 10% p95 regression for point-get and 15%
p95 regression for scan/prefix-scan at 100 non-covering tombstones. Larger
tombstone counts should be reported with evidence and may motivate fragment
cache tuning, tombstone compaction, or per-memtable tombstone limits.

---

## 13. Comparison With Compaction Filters

| Factor | `DeleteRange` | Compaction filter |
| --- | --- | --- |
| Read invisibility | Immediate after commit | Eventual after compaction |
| Best for | Contiguous ranges | Scattered keys or predicates |
| Write amplification | One tombstone per range | One filter record |
| Read-path overhead | Yes, checks covering tombstones | None |
| Value-based predicates | No | Yes |
| Tombstone buildup | Possible with many ranges | No read-path tombstones |
| Cleanup trigger | Compaction | Compaction |

Use `DeleteRange` when:

1. The keys are contiguous in user-key order.
2. New reads must stop seeing the deleted data immediately.
3. The number of ranges is modest enough that read-path overhead is acceptable.

Use compaction filters when:

1. Keys are scattered or predicate-selected.
2. Eventual cleanup is acceptable.
3. Avoiding read-path overhead is more important than immediate hiding.

---

## 14. Testing Plan

### 14.1 Unit Tests

1. Range validation rejects `start >= end`.
2. `RangeTombstoneSet::newest_covering_ts` handles non-overlap, boundary
   equality, overlapping tombstones, and read timestamps.
3. WAL encode/decode round-trips `RangeTombstone`.
4. SST v4 range-tombstone block encode/decode round-trips sorted records and
   rejects corrupt CRCs, including fragmented records with multiple covering
   timestamps.
5. Manifest format version rejects databases with unsupported v4 records when
   opened by older code.
6. `MemTable::is_empty` returns false for range-only memtables.
7. Memtable approximate size increases for range tombstones.
8. Range-only SST footer and coverage metadata decode correctly.
8a. `RangeTombstoneKey` ordering with identical `(start, ts)` but different
    `ordinal` values is correct and deterministic.
9. The fragmenter converts overlapping finite tombstones into ordered,
   non-overlapping fragments and coalesces adjacent fragments with the same
   timestamp set.
10. Fragmented views preserve half-open boundaries exactly, especially when one
    tombstone's `end` equals another tombstone's `start`.
11. Fragmenter event ordering is deterministic when multiple tombstones start
    or end at the same key.
12. Duplicate ranges with different timestamps preserve both timestamps in the
    fragment view.
13. Nested ranges where the newest timestamp exits reveal the older covering
    timestamp for older suffix fragments.
14. Empty fragments are suppressed.
15. Adjacent same-timestamp tombstones `[a, b)@10` and `[b, c)@10` coalesce
    into `[a, c)@10`.
16. Two tombstones covering the same span at different timestamps, read at the
    older timestamp, verify the older tombstone hides the key.
17. Multiple tombstones sharing the same `start` key but different `end` keys
    and timestamps produce correct fragments.

### 14.2 Read Semantics Tests

1. `get` hides keys in `[start, end)` immediately after `delete_range`.
2. `get` keeps keys equal to `end` visible.
3. `scan` skips covered keys but yields uncovered keys before and after the
   range.
4. `prefix_scan` respects range tombstones.
5. A later `put` inside the range recreates the key.
6. A snapshot opened before `delete_range` still sees old data.
7. A snapshot opened after `delete_range` hides old data.
8. Point tombstones and range tombstones compose correctly.
9. User-visible exact-kind and conditional paths consult range tombstones.
10. Prefix scans with no finite upper bound check finite overlapping tombstones
    without implying unbounded tombstone support.
11. Scans integrate range-tombstone filtering inside the MVCC user-key collapse
    loop: a key with versions before and after a range tombstone, including a
    too-new version above `read_ts`, must not expose an older hidden version.
12. Prefix bloom pruning does not skip tombstone metadata: an SST with no
    matching point-prefix bloom entry but with tombstone coverage must still
    hide matching point keys from another source.

### 14.3 Persistence Tests

1. Range tombstone survives WAL recovery before flush.
2. Range tombstone survives flush into SST.
3. Range tombstone survives manifest snapshot recovery.
4. Range tombstone survives compaction.
5. Crash after durable WAL append but before in-memory publish recovers the
   range tombstone as committed data.
6. Range-only active WAL memtable survives recovery.
7. Range-only immutable WAL memtable survives recovery and manifest snapshot.
8. Existing manifest v3 database opens under a v4 binary and upgrades only
   before the first v4 durable artifact.
9. WAL v3 and SST v4 are rejected by older format readers.
10. `sst.max_ts` includes range-tombstone timestamps: a range-only SST with
    `[a,z)@100` must report `max_ts = 100`. After flush and WAL removal,
    recovery must set `next_commit_ts > 100` so the allocator never reuses a
    committed timestamp.

### 14.4 Compaction Tests

1. Bottom-level compaction drops covered point values and retained range
   tombstones when safe.
2. Non-bottom compaction keeps range tombstones to avoid resurrection.
3. Lower-level older values are not resurrected after range-tombstone GC.
4. vLog references for covered values are unregistered only after compacted SST
   state is installed.
5. Overlapping range tombstones retain the newest covering timestamp.
6. Compaction carries wide tombstones across multiple output SSTs.
7. Compaction can emit range-only output when no point entries survive.
8. Tombstones are not dropped unless full key-range closure coverage is proven.
9. Compaction consumes fragmented tombstone views and writes output metadata
   that preserves coverage across split output files.
10. A range tombstone in L0 continues hiding an older covered value in L1/L2
    when the older file is outside the selected compaction input.
11. Split-output compaction does not place a wide tombstone only in the output
    file containing the tombstone start.
12. Gap coverage: when a range tombstone spans a gap between L1 point SSTs, the
    compaction emits a range-only output SST covering the gap, preventing value
    resurrection in lower levels.
13. Prefix gap coverage: when a range tombstone starts before the first output
    point SST's `point_first_key`, the compaction emits a range-only output SST
    covering the prefix gap (e.g. tombstone `[a, z)` with first point SST at
    `m` must produce a range-only SST for `[a, m)`).
14. Compaction input selection includes L2 files under L0 tombstone coverage,
    not only those overlapping L1 point-key ranges.
15. Range-only SSTs in the compaction input are carried forward (not dropped
    silently) and their tombstones are merged into the output.
16. `next_valid_user_key` with `point_last_key` at maximum allowed length.
17. `next_valid_user_key` with `point_last_key` containing trailing `0xff` bytes.
18. `next_valid_user_key` with `point_last_key` all `0xff` at maximum length
    returns `None`, and compaction correctly omits the suffix range-only SST.

### 14.5 Transaction Tests

1. Non-serializable transaction `delete_range` returns unsupported in the MVP.
2. Serializable transaction `delete_range` returns unsupported in the MVP.
3. Non-transactional `delete_range` is rejected while point-key serializable mode
   is enabled.

### 14.6 Concurrency and Crash Tests

1. A reader cannot obtain `read_ts >= delete_ts` before the tombstone is visible.
2. WAL append failure before durable commit does not expose a partial delete.
3. Crash after durable WAL append but before in-memory timestamp publication
   recovers the range tombstone as committed data.
4. Freeze-before-flush preserves range-only immutable memtables.
5. Manifest snapshot during immutable range-only memtable retention keeps the
   corresponding WAL ID.
6. Concurrent `delete_range` and `get` on overlapping key ranges: every `get`
   returns either the pre-delete value or `None`, never a partial or corrupted
   result.
7. Concurrent `get`/`scan` and `delete_range` on the same active memtable: all
   readers see a consistent (if potentially stale-from-their-snapshot) view. The
   active memtable has no shared fragment cache, so there is no stale-install
   race to test; verify that readers always query raw tombstones directly.
8. `delete_range` is the exact write that triggers memtable freeze
   (`target_sst_size` sized accordingly): the frozen memtable's range tombstones
   survive flush to SST v4 and subsequent recovery.
9. Simulate OOM during memtable insert after successful WAL append: the engine
   aborts cleanly rather than serving inconsistent reads.
10. Immutable memtable fragment cache: freeze an active memtable with range
    tombstones, then run concurrent `get`/`scan` readers on the immutable
    memtable. Verify that the `OnceCell` fragment view is built exactly once and
    that all readers see the same consistent fragmented view.

### 14.7 CLI and Observability Tests

1. `delrange <start> <end>` help documents half-open `[start, end)` semantics.
2. CLI rejects `start >= end`.
3. CLI output makes half-open behavior clear when `end` exists as a key.
4. `range_tombstone_stats()` reports active, immutable, SST, lookup, and
   compaction counters.

---

## 15. Rollout Plan

### Phase 1: Internal Metadata and WAL

1. Add `RangeTombstone` and `RangeTombstoneSet`.
2. Add manifest v4 upgrade gating before any durable WAL v3 or SST v4 artifact
   can be created.
3. Add WAL record encoding/decoding. Until manifest upgrade is wired in tests,
   WAL v3 coverage is codec-only or uses temporary non-database recovery
   fixtures that cannot leave a manifest-v3 database with WAL v3 files.
4. Extend `Wal::recover` to return or populate range tombstones alongside point
   entries, using the `RecoveredWalBatch` carrier or equivalent. The existing
   `Wal::recover` accepts only a `SkipMap<Bytes, Bytes>` for point entries; it
   must be extended to also populate the memtable's `RangeTombstoneSet`.
5. Add memtable storage for range tombstones.
6. Update `MemTable::is_empty()` to return `false` when range tombstones are
   present. This must also be verified in the engine close/drop path so that
   range-only memtables are flushed on shutdown.
7. Add internal `delete_range` test hooks only. Do not expose the public API
   until persistence, recovery, and read visibility are complete.

### Phase 2: Memtable Read Path and Fragmenter

1. Add the range tombstone fragmenter and a `RangeTombstoneView` abstraction.
2. Add `ImmutableRangeTombstoneSet` with `OnceCell` fragment cache for frozen
   memtables.
3. Add range-tombstone lookup across active and immutable memtables only.
4. Integrate `get` for unflushed range tombstones (active: raw scan; immutable:
   cached fragments).
5. Integrate `scan` and `prefix_scan` with per-scan fragmented view for active
   memtable and shared cached fragments for immutable memtables.
6. Add MVCC snapshot visibility tests for active and immutable memtables.
7. Guard the flush path so it refuses to flush a memtable containing range
   tombstones until Phase 3 SST v4 write support exists. This must be a
   compile-time or config gate (not a runtime heuristic relying on test memtable
   sizing). The guard must be present in the same PR as the Phase 2 test hook;
   CI must enforce this.
7. Phase gate: range-only WAL/memtable recovery and memtable-only read
   visibility tests pass.

### Phase 3: SST Format and Durable Reads

1. Add SST v4 range-tombstone block.
2. Flush memtable range tombstones into SSTs.
3. Eagerly build and cache fragmented table-reader views at `SsTable::open`.
4. Recover range tombstones from SST metadata.
5. Support range-only SSTs and eager metadata loading.
6. Extend `get`, `scan`, and `prefix_scan` tombstone lookup to SST metadata.
7. Integrate visibility-aware helper paths used by conditional writes and vLog
   liveness.
8. Phase gate: flushed, recovered, and range-only SST read tests pass.

### Phase 4: Compaction GC

1. Carry range tombstones through compaction outputs.
2. Merge point entries with fragmented tombstone input views.
3. Extend compaction input selection to include lower-level files under
   tombstone coverage (not just point-key overlap) and range-only SSTs whose
   coverage overlaps the selected point SSTs.
4. Drop covered point values when safe.
5. Drop obsolete range tombstones only at bottommost safe compactions. A
   fragment can only be dropped after all live files overlapping the fragment's
   own interval (not the original tombstone's interval) are included.
6. Wire vLog reference accounting through the existing SST publication path.
7. Preserve wide tombstones across output splits: truncated fragments in point
   SSTs, range-only output SSTs for gaps.
8. Range-only SSTs count toward level size for compaction triggering.
9. Phase gate: split-output compaction coverage, gap-coverage, mixed-level
   resurrection, and vLog reference tests pass.

### Phase 5: Public API, CLI, and Observability

1. Expose `KvEngine::delete_range`.
2. Extend `write_batch` with range-only `DelRange` batches and reject mixed
   point/range batches.
3. Add CLI command:

```text
delrange <start> <end>
```

4. Add `range_tombstone_stats()`.
5. Add benchmark coverage and enforce the gates in Section 12.5.

---

## 16. Open Questions

1. Should a later phase add ordered mixed `DelRange` and point writes using
   `(commit_ts, sequence_number)`?
2. Should range tombstone metadata eventually support lazy/paged loading for
   very large tombstone sets?
3. Should `delete_range` expose a prefix convenience API:
   `delete_prefix(prefix)`?
4. Should large numbers of range tombstones trigger a dedicated compaction
   priority to reduce read-path overhead?
5. Should transaction-local range tombstones be supported once range conflict
   tracking exists?

---

## 17. References

1. RFC 005: Multi-Version Concurrency Control.
2. RFC 009: Compaction Filters.
3. [RocksDB `DeleteRange`](https://github.com/facebook/rocksdb/wiki/DeleteRange):
   range tombstone API and compaction cleanup model.
4. [RocksDB `DeleteRange` implementation](https://github.com/facebook/rocksdb/wiki/DeleteRange-Implementation):
   fragmented range tombstone iterator and table-reader model.
5. TiKV GC and table-drop model: range deletion for contiguous table/keyspace
   drops, compaction filters for MVCC cleanup.
