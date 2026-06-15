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

## 5. Semantics

### 5.1 Range Shape

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

### 5.2 MVCC Visibility

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

### 5.3 Interaction With Point Tombstones

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

### 5.4 Read-Your-Writes

A successful `delete_range` is immediately visible to subsequent operations on
the same engine handle. If the delete is written into the active memtable, the
range-tombstone index must be updated before the public call returns.

### 5.5 Durability

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

## 6. API

### 6.1 Public Storage API

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
4. Append a range tombstone to the active memtable and WAL.
5. Update in-memory range-tombstone indexes.
6. Publish the commit timestamp only after the tombstone is visible to readers.

Errors are atomic: if validation, WAL append, or memtable insertion fails, the
range tombstone is not published and no reader may observe a read timestamp that
includes it.

| Condition | Result |
| --- | --- |
| `start >= end` | Error: invalid range |
| encoded `start` or `end` exceeds key-size limit | Error: key too large |
| WAL append or sync fails when WAL is enabled | Error; timestamp is not published |
| mixed `DelRange` and point records in one batch | Error: unsupported mixed batch |
| transaction `delete_range` in MVP | Error: unsupported |
| prefix has no finite upper bound | Error: unsupported unbounded range |
| unsupported manifest/WAL/SST version | Error before serving reads |

### 6.2 WriteBatch API

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
Section 5.2, a same-timestamp `Put(k)` inside a same-batch `DelRange(a, z)`
would be indistinguishable from a pre-delete put unless the storage format grew
an intra-batch sequence number.

Allowed MVP batches:

1. Point-only batches: existing `Put`/`Del` behavior.
2. Range-only batches: one or more `DelRange` records sharing one commit
   timestamp.

Range-only batches are applied atomically. A later phase may add ordered mixed
batches by storing and comparing `(commit_ts, sequence_number)` for both point
records and range tombstones.

### 6.3 Transaction API

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

## 7. Data Model

### 7.1 In-Memory Representation

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

The MVP uses an augmented `BTreeMap<Bytes, Vec<RangeTombstone>>` keyed by
`start`, plus cached summary bounds:

```rust
pub struct RangeTombstoneSet {
    by_start: BTreeMap<Bytes, Vec<RangeTombstone>>,
    min_start: Option<Bytes>,
    max_end: Option<Bytes>,
    count: usize,
    metadata_bytes: usize,
}
```

`newest_covering_ts(key)` is `O(log R + K)`, where `K` is the number of
candidate tombstones with `start <= key` that must be checked. The summary
bounds let reads skip structures that cannot cover the key. A later
optimization can replace this with an interval tree or fragmented tombstone map
if benchmarks exceed the thresholds in Section 11.

Range tombstones count toward memtable size. `delete_range` must add
`start.len() + end.len() + metadata overhead` to `approximate_size` and call the
same freeze path as `put` and `delete`. `MemTable::is_empty()` must return
`false` when range tombstones are present, even if there are no point entries.

### 7.2 SST Representation

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
  ts: u64
  start: [u8]
  end: [u8]
crc32
```

Records are sorted by `(start, end, ts)` and may overlap. The MVP does not need
to fragment or coalesce them on write, but compaction should opportunistically
drop obsolete tombstones.

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

Range-only SSTs are valid. A range-only SST contains no point data blocks, but
it must contain a range-tombstone block and coverage metadata:

```rust
pub struct SsTableRangeCoverage {
    pub point_first_key: Option<Bytes>,
    pub point_last_key: Option<Bytes>,
    pub tombstone_min_start: Option<Bytes>,
    pub tombstone_max_end: Option<Bytes>,
}
```

All SST overlap checks used for `get`, `scan`, compaction selection, and prefix
bloom pruning must use the union of point-key bounds and tombstone coverage
bounds. A tombstone-only SST is therefore discoverable even though it has no
point-key range.

### 7.3 Manifest Representation

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
2. The database is upgraded to manifest v4 before the first durable range
   tombstone, WAL v3 file, or SST v4 file is created.
3. Once upgraded, downgrade to an older binary is unsupported; older binaries
   reject manifest v4 or SST v4 before serving reads.
4. The manifest version bump and the first v4 durable artifact must be ordered
   so recovery never sees a WAL v3 or SST v4 artifact while the manifest still
   claims v3 compatibility.
5. Manifest snapshot retention must treat range-only immutable memtables as
   live. `imm_memtable_ids` must include unflushed memtables that contain only
   range tombstones.

### 7.4 WAL Representation

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

`Wal::recover` must restore both point entries and range tombstones into the
recovered memtable before any reads can run. Recovery must not drop a memtable
just because its point skiplist is empty; a range-only WAL is live data.

---

## 7.5 Compatibility Summary

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

## 8. Read Path

### 8.1 Point Get

`get(key)` currently finds the newest visible point version at or before
`read_ts`. With range tombstones, it must also find the newest covering range
tombstone at or before `read_ts`.

Pseudo-code:

```rust
let candidate = newest_visible_point_version(key, read_ts)?;
let range_ts = range_tombstones.newest_covering_ts(key, read_ts);

match candidate {
    None => None,
    Some((value_ts, value)) if range_ts.is_some_and(|ts| value_ts <= ts) => None,
    Some((_value_ts, value)) => Some(value),
}
```

The range-tombstone lookup must include:

1. Active memtable.
2. Immutable memtables.
3. L0 SSTs, newest to oldest.
4. Leveled/tiered SSTs whose point-key range or tombstone coverage range
   overlaps `key`.

The MVP may perform a simple metadata scan over overlapping SSTs. Later work
can build a global range-tombstone index in `LsmStorageState` to reduce point
lookup overhead.

The range-tombstone view used by a read must come from the same immutable state
snapshot as the point data being read. If a state-level index is added, it must
be embedded inside `LsmStorageState` or published atomically with it; readers
must not load point data from one state and tombstone metadata from another.

Read helpers must be classified explicitly:

1. User-visible helpers (`get`, `scan`, `prefix_scan`, conditional writes, and
   transaction reads when later supported) are visibility-aware and must consult
   range tombstones.
2. Physical maintenance helpers may bypass range tombstone visibility only when
   their caller explicitly reasons about physical versions. vLog GC liveness and
   compaction cleanup are not allowed to treat range-covered versions as live
   user data once the covering tombstone is safe under the MVCC watermark.

### 8.2 Scan

`scan(lower, upper)` must skip point versions hidden by range tombstones. The
scan iterator already collapses MVCC versions by decoded user key. Add one more
visibility check before yielding a key:

```text
if newest_covering_range_ts(user_key, read_ts) >= value_ts:
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

Scan implementations should cache the current covering tombstone span while
iterating. They should not repeatedly recompute the same overlapping tombstone
set for every internal version in a dense deleted range.

### 8.3 Prefix Scan

`prefix_scan(prefix)` is equivalent to a bounded scan over
`[prefix, prefix_upper_bound(prefix))` when the prefix has a finite upper bound.
It inherits scan semantics.

If the prefix has no finite upper bound, the scan may continue using the
existing fallback for point keys, but MVP range tombstones are still finite. The
fallback only needs to check finite tombstones that overlap yielded candidate
keys; it does not imply support for unbounded range tombstones.

### 8.4 Bloom Filters

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

---

## 9. Compaction

### 9.1 Physical Cleanup

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

### 9.2 Tombstone Retention

A range tombstone may be dropped only when both are true:

1. It is older than or equal to the MVCC GC watermark.
2. There is no live point entry in any non-compacted lower level that it still
   needs to hide.

For leveled compaction, the easiest safe MVP rule is:

```text
Keep range tombstones unless compacting a full key-range closure to the
bottommost level.
At the bottommost level, drop a range tombstone only after all live files
overlapping its interval are included and all point entries it covers have been
removed.
```

This is conservative but correct. Future work can fragment tombstones and drop
only fully-cleared subranges.

### 9.3 Fragmentation

Overlapping range tombstones can create expensive visibility checks:

```text
[a, z) @ 10
[m, p) @ 20
[b, c) @ 30
```

The MVP does not need to fragment these into disjoint spans. It may store
overlapping records and compute `max(ts)` among records covering a key. This
keeps implementation simple.

A later optimization can fragment tombstones during compaction:

```text
[a, b) @ 10
[b, c) @ 30
[c, m) @ 10
[m, p) @ 20
[p, z) @ 10
```

Fragmentation improves scan performance and tombstone GC precision, but it is
not required for correctness.

Compaction output splitting must preserve tombstone coverage. When a wide range
tombstone spans multiple output SSTs, the compaction must either:

1. Fragment it to the output key intervals that still need coverage.
2. Duplicate it into every output SST whose point or tombstone coverage may need
   it.
3. Emit a range-only output SST for a covered interval with no surviving point
   keys.

It is not sufficient to place the tombstone only in the output SST whose point
keys contain the tombstone start.

### 9.4 vLog GC

Range tombstones do not directly reference vLog files. They make older point
entries unreachable. Once compaction drops those covered point entries, the
normal SST-to-vLog reference accounting must unregister their value pointers.

The vLog invariant remains:

```text
A vLog file can be deleted only after no live SST references it.
```

Range tombstone cleanup must not delete vLog files directly.

vLog GC liveness checks must be range-aware. A physical value version covered by
a visible range tombstone is not live user data once
`range_delete_ts <= gc_watermark`. The deletion invariant is unchanged: a vLog
file can be unlinked only after no live SST references it.

---

## 10. Concurrency and Crash Safety

### 10.1 Atomic Publication

`delete_range` must publish the range tombstone atomically with its timestamp:

1. Acquire the same MVCC write lock used by `put`, `delete`, and
   `write_batch`.
2. Reserve `delete_ts` without advancing the globally visible latest timestamp.
3. Append to WAL if enabled.
4. Insert into the active memtable's range-tombstone set.
5. Update range-tombstone summaries/indexes.
6. Advance the globally visible latest timestamp to publish `delete_ts`.
7. Release the write lock.

Readers that acquire a snapshot after step 6 must see the range tombstone.
Readers that started earlier use their existing `read_ts` and remain isolated.
No reader may observe `read_ts >= delete_ts` before the tombstone is visible.

### 10.2 Memtable Freeze

When the active memtable is frozen, its range tombstones move with it into the
immutable-memtable list. Flush must write both point entries and range
tombstones into the new SST before the immutable memtable can be removed from
state.

A memtable containing only range tombstones is not empty. It must freeze, flush,
recover, and be retained in manifest snapshots the same way as a memtable with
point entries. Flush of a range-only memtable produces a valid range-only SST or
an equivalent durable range-tombstone structure.

### 10.3 Compaction Publication

Compaction must publish new SSTs and remove old SSTs atomically under the
existing state lock. If a compaction drops point entries because a range
tombstone covers them, the range tombstone that justifies the drop must either:

1. Be present in the new SST set, or
2. Be proven obsolete under the bottommost-level GC rule.

### 10.4 Recovery

Recovery order:

1. Recover manifest and live SST state.
2. Eagerly load and validate SST range-tombstone metadata.
3. Replay WALs for active and immutable memtables, including range tombstones.
4. Publish `LsmStorageState`.

No reads may run between steps 1 and 4.

---

## 11. Performance

### 11.1 Expected Costs

`DeleteRange` trades small read-path overhead for large write-amplification
reduction.

Expected MVP costs:

1. `delete_range`: O(1) WAL append plus O(log R) insertion into the active
   range-tombstone set, plus memtable size accounting.
2. `get`: extra lookup for newest covering range tombstone, normally skipped by
   summary bounds when no tombstone can cover the key.
3. `scan`: extra visibility checks with cached covering spans.
4. Compaction: extra coverage checks and range-tombstone metadata writes.

### 11.2 Indexing Strategy

The MVP should start with a bounded simple structure:

```text
per memtable: BTreeMap<start, Vec<RangeTombstone>> + summary bounds
per SST: sorted Vec<RangeTombstone> + summary bounds loaded from metadata
```

Point lookup can check records whose `start <= key`, then filter by `end > key`
and `ts <= read_ts`.

If benchmarks show this is expensive, add a state-level range-tombstone index:

```rust
pub struct RangeTombstoneIndex {
    memtables: Vec<Arc<RangeTombstoneSet>>,
    ssts: Vec<(usize, Arc<RangeTombstoneSet>)>,
}
```

This index can be rebuilt on state publication and shared by readers through
`LsmStorageState`. It must not be a separate unsynchronized `ArcSwap`; otherwise
readers can pair point data from one state with tombstones from another.

Index escalation triggers:

1. p95 point-get latency regresses by more than 10% with 100 non-covering range
   tombstones.
2. p95 scan latency regresses by more than 15% over dense deleted ranges.
3. total in-memory tombstone metadata exceeds a configured limit.
4. recovery/open time regresses by more than 10% with 10k range tombstones.

### 11.3 Cache Interaction

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

### 11.4 Observability

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

### 11.5 Benchmark Gates

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
tombstone counts should be reported with evidence and may motivate the
state-level index.

---

## 12. Comparison With Compaction Filters

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

## 13. Testing Plan

### 13.1 Unit Tests

1. Range validation rejects `start >= end`.
2. `RangeTombstoneSet::newest_covering_ts` handles non-overlap, boundary
   equality, overlapping tombstones, and read timestamps.
3. WAL encode/decode round-trips `RangeTombstone`.
4. SST v4 range-tombstone block encode/decode round-trips sorted records and
   rejects corrupt CRCs.
5. Manifest format version rejects databases with unsupported v4 records when
   opened by older code.
6. `MemTable::is_empty` returns false for range-only memtables.
7. Memtable approximate size increases for range tombstones.
8. Range-only SST footer and coverage metadata decode correctly.

### 13.2 Read Semantics Tests

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

### 13.3 Persistence Tests

1. Range tombstone survives WAL recovery before flush.
2. Range tombstone survives flush into SST.
3. Range tombstone survives manifest snapshot recovery.
4. Range tombstone survives compaction.
5. Crash after WAL append but before memtable publish is handled by existing
   write-path ordering.
6. Range-only active WAL memtable survives recovery.
7. Range-only immutable WAL memtable survives recovery and manifest snapshot.
8. Existing manifest v3 database opens under a v4 binary and upgrades only
   before the first v4 durable artifact.
9. WAL v3 and SST v4 are rejected by older format readers.

### 13.4 Compaction Tests

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

### 13.5 Transaction Tests

1. Non-serializable transaction `delete_range` returns unsupported in the MVP.
2. Serializable transaction `delete_range` returns unsupported in the MVP.
3. Non-transactional `delete_range` is rejected while point-key serializable mode
   is enabled.

### 13.6 Concurrency and Crash Tests

1. A reader cannot obtain `read_ts >= delete_ts` before the tombstone is visible.
2. Failure after WAL append but before timestamp publication does not expose a
   partial delete.
3. Failure after memtable insert but before timestamp publication recovers from
   WAL without serving an intermediate state.
4. Freeze-before-flush preserves range-only immutable memtables.
5. Manifest snapshot during immutable range-only memtable retention keeps the
   corresponding WAL ID.

### 13.7 CLI and Observability Tests

1. `delrange <start> <end>` help documents half-open `[start, end)` semantics.
2. CLI rejects `start >= end`.
3. CLI output makes half-open behavior clear when `end` exists as a key.
4. `range_tombstone_stats()` reports active, immutable, SST, lookup, and
   compaction counters.

---

## 14. Rollout Plan

### Phase 1: Internal Metadata and WAL

1. Add `RangeTombstone` and `RangeTombstoneSet`.
2. Add WAL record encoding/decoding.
3. Add memtable storage for range tombstones.
4. Add internal `delete_range` test hooks only. Do not expose the public API
   until persistence, recovery, and read visibility are complete.

### Phase 2: Read Path

1. Add range-tombstone lookup across active/immutable memtables and SST
   metadata.
2. Integrate `get`.
3. Integrate `scan` and `prefix_scan`.
4. Add MVCC snapshot visibility tests.
5. Integrate visibility-aware helper paths used by conditional writes and vLog
   liveness.

### Phase 3: SST Format

1. Add SST v4 range-tombstone block.
2. Flush memtable range tombstones into SSTs.
3. Recover range tombstones from SST metadata.
4. Bump manifest format version to 4.
5. Support range-only SSTs and eager metadata loading.

### Phase 4: Compaction GC

1. Carry range tombstones through compaction outputs.
2. Drop covered point values when safe.
3. Drop obsolete range tombstones only at bottommost safe compactions.
4. Wire vLog reference accounting through the existing SST publication path.
5. Preserve wide tombstones across output splits or emit range-only outputs.

### Phase 5: Public API, CLI, and Observability

1. Expose `KvEngine::delete_range`.
2. Extend `write_batch` with range-only `DelRange` batches and reject mixed
   point/range batches.
3. Add CLI command:

```text
delrange <start> <end>
```

4. Add `range_tombstone_stats()`.
5. Add benchmark coverage and enforce the gates in Section 11.5.

---

## 15. Open Questions

1. Should a later phase add ordered mixed `DelRange` and point writes using
   `(commit_ts, sequence_number)`?
2. Should range tombstone metadata eventually support lazy/paged loading for
   very large tombstone sets?
3. Should range tombstones be fragmented during compaction in a later
   implementation, or deferred until benchmarks show a need?
4. Should `delete_range` expose a prefix convenience API:
   `delete_prefix(prefix)`?
5. Should large numbers of range tombstones trigger a dedicated compaction
   priority to reduce read-path overhead?
6. Should transaction-local range tombstones be supported once range conflict
   tracking exists?

---

## 16. References

1. RFC 005: Multi-Version Concurrency Control.
2. RFC 009: Compaction Filters.
3. RocksDB `DeleteRange`: range tombstone API and compaction cleanup model.
4. TiKV GC and table-drop model: range deletion for contiguous table/keyspace
   drops, compaction filters for MVCC cleanup.
