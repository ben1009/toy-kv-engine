# RFC 009: Compaction Filters

**Status:** Proposed  
**Date:** 2026-06-14  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding compaction filters to kv-engine. A compaction filter is
a user-configured predicate that decides whether an entry should be kept when a
compaction rewrites SST files.

The first supported filter is a prefix filter:

```rust
engine.add_compaction_filter(CompactionFilterRequest::prefix(b"tenant:old:"))?;
```

When an SST containing keys with that prefix is compacted under safe
conditions, matching historical entries are omitted from the newly written SST.
This makes it possible to reclaim storage for expired namespaces, dropped
tenants, old secondary-index families, or application-level TTL buckets without
scanning and deleting every key individually.

Compaction filters are an **eventual cleanup mechanism**, not an immediate
read-path delete. A filtered key can remain visible until all SSTs that contain
it have passed through compaction.

Compaction filters and range tombstones (`DeleteRange`) solve overlapping but
different problems. Range tombstones are the better tool for contiguous
namespace deletion: they provide immediate read invisibility and standard
compaction handles cleanup automatically. Compaction filters are the right tool
for scattered-key cleanup, predicate-based filtering, and cases where read-path
hiding is not needed. See Section 6.6 for a detailed comparison.

---

## 2. Motivation

kv-engine already performs compaction-time garbage collection for two internal
cases:

1. Tombstones can be dropped at the bottom level once no lower level can contain
   older values.
2. MVCC versions below the active-reader watermark can be collapsed so only the
   newest version needed by future readers remains.

Those rules are storage-internal. Applications still have no way to tell the
engine that a large logical subset of keys is obsolete.

Common examples:

1. A secondary index is rebuilt under a new prefix and the old index prefix can
   be discarded.
2. Time-bucketed data under `logs:2026-01:` has expired.
3. Test or benchmark data under a known prefix should be reclaimed without
   issuing millions of point deletes.
4. Scattered keys matching a predicate (e.g., expired TTL entries embedded in
   values, or keys belonging to a set of specific entity IDs spread across
   different prefixes) need cleanup without writing a tombstone per key.

These cases share a pattern: the keys to drop have no single contiguous range
that covers them without also covering unrelated keys, the cleanup is not
time-critical, or the predicate is too complex for a simple key range.
Compaction filters handle them well.

Contiguous namespace deletion (dropping a tenant, table, or logical namespace
whose keys share a prefix) is a different problem. Range tombstones
(`DeleteRange`) are the better tool for that case: they provide immediate read
invisibility and standard compaction handles cleanup automatically. See
Section 6.6.

Point deletes work, but they have poor write amplification for large prefixes:

1. The caller must scan the prefix.
2. The caller writes one tombstone per key.
3. The tombstones themselves must be flushed, compacted, and eventually
   garbage-collected.
4. With value separation enabled, vLog entries are reclaimed only after the
   tombstoned SST references have also been compacted away.

A compaction filter moves this work to the rewrite path the engine already has
to perform. It does not make obsolete data disappear immediately, but it avoids
materializing a tombstone for every key.

---

## 3. Goals

1. Add prefix-based compaction filters as the first supported filter kind.
2. Make filter semantics explicit: eventual cleanup, not immediate deletion.
3. Support scattered-key and predicate-based cleanup that range tombstones
   cannot efficiently express.
4. Preserve MVCC snapshot correctness and active-reader safety.
5. Preserve vLog GC correctness by unregistering dropped value-pointer
   references only after new SST state is installed.
6. Persist filter metadata so crash recovery does not leave a partially applied
   cleanup policy behind.
7. Keep foreground reads and writes off the filter hot path.
8. Expose basic stats so callers can see whether filters are doing useful work.
9. Acknowledge that contiguous namespace deletion is better served by range
   tombstones (`DeleteRange`), which provide immediate read invisibility and
   automatic compaction cleanup. Compaction filters focus on cases where
   `DeleteRange` does not fit.

## 4. Non-Goals

1. Immediate range-delete semantics.
2. Hiding filtered keys in `get`, `scan`, transaction reads, or prefix scans
   before compaction has rewritten the containing SSTs.
3. Arbitrary user callbacks executed inside compaction threads.
4. Time-based TTL evaluation inside the engine's clock path.
5. Predicate-lock serializability for filtered ranges.
6. Rewriting WAL or memtable contents in place.
7. Changing SST, WAL, or vLog entry formats in the MVP.

---

## 5. Design Overview

Compaction filters are installed on `LsmStorageInner` and evaluated only inside
`compact_from_iter`, immediately before `builder.add_raw(...)` writes an entry
to the output SST.

```text
input SSTs / memtables
        │
        ▼
  merge iterator
        │
        ▼
  MVCC/tombstone GC
        │
        ▼
  compaction filters
        │
        ├── keep  ──► SsTableBuilder ──► new SST
        │
        └── drop  ──► stats + vLog reference omission
```

Filters are evaluated after existing MVCC and tombstone garbage-collection
rules, but they are allowed to physically drop entries only when the compaction
is safe against lower-level resurrection and active MVCC readers. That ordering
keeps the built-in correctness rules authoritative and prevents a filtered
compaction from exposing older lower-level values.

The MVP supports one filter kind:

```rust
pub enum CompactionFilterKind {
    Prefix(Vec<u8>),
}
```

Each installed filter carries metadata:

```rust
pub struct InstalledCompactionFilter {
    pub id: u64,
    pub kind: CompactionFilterKind,
    pub cutoff_ts: u64,
}
```

The MVP assumes the current timestamped engine, where `LsmStorageInner` always
initializes MVCC state. `cutoff_ts` is therefore required, not optional. It
prevents a filter installed at time T from deleting newer writes that happen to
use the same prefix later.

```text
filter prefix = "tenant:42:"
cutoff_ts     = 100

tenant:42:a @ ts=099  -> eligible to drop
tenant:42:a @ ts=101  -> keep
```

A `cutoff_ts` of 0 (fresh database with no writes) is valid but harmless: no
entry can have `ts > 0`, so the filter will never drop anything. The
implementation should accept this without error.

Support for a truly timestamp-disabled engine can be added later with explicit
non-MVCC semantics. The MVP should not keep a `None` cutoff branch that is not
reachable in the current codebase.

---

## 6. Semantics

### 6.1 Eventual Cleanup

Installing a filter does not change read-path visibility. Existing keys remain
visible until compaction rewrites the SSTs that contain them.

This means:

1. `get(key)` can still return a matching key immediately after the filter is
   added.
2. `scan(prefix)` can still return matching keys until compaction catches up.
3. A force compaction can be used when the caller wants cleanup to finish
   promptly.

The API should document this contract directly. It is the main difference
between a compaction filter and a range tombstone.

### 6.2 Table and Namespace Drops

Contiguous namespace deletion (dropping a tenant, table, or logical namespace
whose keys share a prefix) is better served by range tombstones than by
compaction filters. A range tombstone (`DeleteRange`) provides immediate read
invisibility and standard compaction handles cleanup automatically—no separate
logical-delete layer or compaction filter is needed. Compaction filters are the
right tool for scattered-key cleanup, predicate-based filtering, and cases where
read-path hiding is not needed. See Section 6.6 for the full comparison and
usage guidance.

A future `DeleteRange` feature (Section 15) should handle the contiguous
namespace deletion case. Until then, callers that need immediate namespace
deletion must implement a durable logical-delete marker (a metadata record that
table-aware reads consult on the read path). Compaction filters can assist with
the storage cleanup half of that pattern, but the logical-delete marker—not the
compaction filter—provides read-path invisibility.

### 6.3 MVCC Cutoff and Readers

A filter receives an installation timestamp:

```rust
let cutoff_ts = self.mvcc.latest_commit_ts();
```

`add_compaction_filter` linearizes at cutoff capture. Cutoff capture must be
synchronized with MVCC commit timestamp allocation so every write that commits
after the linearization point receives `commit_ts > cutoff_ts`. This requires
acquiring the same `write_lock` used by `put` and `write_batch` before reading
`latest_commit_ts()`, so no concurrent write can interleave between the cutoff
read and the lock acquisition. Writes with `commit_ts <= cutoff_ts` are eligible
for eventual filtering; writes with `commit_ts > cutoff_ts` are protected. A
write racing with `add_compaction_filter` is classified by its assigned commit
timestamp, not by which API call returns first.

The MVCC watermark rule used for ordinary version collapse is not sufficient
for filter deletion. Version collapse keeps a newest visible version at or
below the watermark; a filter removes the key entirely. A transaction or read
guard with a pinned timestamp can reload the current LSM state later, so
publishing a state that physically removed filtered entries can break repeatable
snapshot reads.

Therefore filtered physical deletion requires a stronger gate:

1. The entry's `commit_ts` must be `<= cutoff_ts`.
2. The compaction must be safe against lower-level resurrection (Section 6.4).
3. The engine must prove there are no active read guards or transactions that
   could observe the pre-filter state before publishing the filtered compaction
   result.

The implementation should add an explicit MVCC helper
`can_publish_filter_deletion(cutoff_ts)`. This helper returns true only when no
active read guard or transaction holds a timestamp at or below `cutoff_ts`. The
check is: `watermark > cutoff_ts || no_active_readers`. This is strictly more
conservative than the version-collapse rule, which only requires `ts > watermark`
to protect entries above the watermark while keeping the first entry at or below
it. If the helper returns false, compaction may still rewrite SSTs, but matching
entries must be kept.

In code, the final keep/drop decision is the conjunction of built-in GC and
filter eligibility:

```rust
let should_keep = keep_for_mvcc_and_tombstone_gc(entry)
    && !filters.should_drop(entry, filter_context);
```

This preserves snapshot isolation even when a long-running read guard overlaps
with compaction.

### 6.4 Lower-Level Resurrection and Tombstones

A prefix filter is logically similar to deleting a range of user keys, but the
engine does not yet have a range tombstone (`DeleteRange`) feature. That means
physical deletion is safe only when dropping the current entry cannot reveal an
older value from a lower level.

A filter may physically drop a live entry only when:

1. `compact_to_bottom_level == true`, or
2. the compaction task proves that all lower-level overlapping entries for the
   filtered user key or prefix range are included in the same compaction.

If neither condition holds, the matching live entry must be kept. A future
range-tombstone feature could choose a different design by publishing a durable
logical delete that hides lower-level values on the read path.

Tombstones already have special bottom-level handling. Filters should not change
the rule that a tombstone is needed while lower levels might contain an older
value that it hides.

For MVCC:

1. A matching tombstone above the watermark must be kept.
2. A matching tombstone at or below the watermark can be dropped only when the
   compaction already proves it is safe to drop under existing tombstone rules.

This avoids resurrecting old values from lower levels.

### 6.5 vLog References

When value separation is enabled, SST values may contain `ValuePointer`s. A
compaction filter that drops such an entry must also ensure the new SST does not
register the dropped pointer's vLog file as live.

The current compaction path already derives output vLog references from the
`SsTableBuilder`:

```rust
all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
```

Therefore the MVP should drop filtered entries before `builder.add_raw(...)`.
Dropped entries never enter the builder and are not included in the new SST's
registered references.

The current implementation returns one deduplicated `compact_vlog_ids` list and
registers that same list for every output SST. That is conservative for safety,
but it can over-register vLog files against SSTs that do not actually contain
those pointers, delaying reclamation. Filtered compaction should tighten this:
`compact_from_iter` should return per-output metadata such as
`Vec<(Arc<SsTable>, Vec<u32>)>`, and each new SST should register only the vLog
file IDs contained in that SST.

This change requires updating the `CompactionV2` manifest record, which
currently stores a single `Vec<u32>` for vlog IDs. The per-SST variant needs a
new manifest record format or a versioned `CompactionV2` payload. This does not
change the SST or vLog entry format; it changes only the in-memory registration
metadata and the manifest record structure. The manifest format version bump
(Section 8) should account for this change.

Post-compaction vLog GC remains unchanged:

1. Input SST vLog references are collected before old SSTs are unregistered.
2. Output SST references are registered from the builder output.
3. Old SSTs are removed and their block-cache entries invalidated.
4. `post_compaction_gc(...)` is scheduled for input vLog files.

The vLog GC liveness check remains version-specific through
`get_with_kind_at_ts`, so a dropped filtered version becomes reclaimable only
after the LSM state no longer references it.

### 6.6 Compaction Filters vs Range Tombstones

Compaction filters and range tombstones (`DeleteRange`) are independent
mechanisms that overlap in the namespace deletion use case. This section
clarifies when to use each.

| Factor | Compaction filter | Range tombstone |
|---|---|---|
| Read invisibility | ❌ Eventual (after compaction) | ✅ Immediate |
| Storage cleanup | ✅ During compaction | ✅ During compaction |
| Read-path overhead | ✅ None | ❌ Every Get checks tombstones |
| Non-contiguous keys | ✅ Predicate-based | ❌ Single range cannot select |
| Value-based predicates | ✅ Can inspect values | ❌ Key ranges only |
| Many small ranges | ✅ No tombstone overhead | ❌ Accumulates tombstones |
| Write overhead | Low (manifest + in-memory) | ❌ Tombstone written to LSM |

**Use range tombstones when:**
- Deleting a contiguous namespace (tenant, table, logical prefix).
- Immediate read invisibility is required.
- The number of ranges is small enough that tombstone overhead is acceptable.

**Use compaction filters when:**
- Keys are scattered across the keyspace with no single contiguous range that
  covers them without also covering unrelated keys (e.g., keys belonging to a
  set of specific entity IDs across different prefixes).
- The cleanup predicate is complex (value-based, multi-condition).
- Many small ranges would accumulate too many tombstones.
- No read-path hiding is needed—only storage reclamation.

A future `DeleteRange` feature (Section 15) should be the primary mechanism for
contiguous namespace deletion. Compaction filters remain the right tool for
scattered-key and predicate-based cleanup.

This separation matches production practice. TiKV (built on RocksDB) uses both
mechanisms for different purposes: `DeleteRange` for table and partition drops
(immediate read invisibility), and a custom `WriteCompactionFilter` for MVCC
version GC (scattered old versions across the keyspace). See TiKV's
`src/server/gc_worker/compaction_filter.rs` and [TiKV GC
documentation](https://tikv.org/deep-dive/distributed-transaction/garbage-collection)
for the reference implementation.

---

## 7. API

### 7.1 Public Types

Replace the current placeholder:

```rust
#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}
```

with an explicit public request type and an internal installed-filter type:

```rust
#[derive(Clone, Debug)]
pub enum CompactionFilterRequest {
    Prefix(Bytes),
}

impl CompactionFilterRequest {
    pub fn prefix(prefix: impl Into<Bytes>) -> Self {
        Self::Prefix(prefix.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompactionFilterKind {
    Prefix(Vec<u8>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstalledCompactionFilter {
    pub id: u64,
    pub kind: CompactionFilterKind,
    pub cutoff_ts: u64,
}
```

The public API should accept a request and return the installed filter ID:

```rust
/// Public listing type, separate from the persisted manifest payload.
#[derive(Clone, Debug)]
pub struct CompactionFilterInfo {
    pub id: u64,
    pub kind: CompactionFilterKind,
    pub cutoff_ts: u64,
}

impl KvEngine {
    pub fn add_compaction_filter(
        &self,
        filter: CompactionFilterRequest,
    ) -> Result<u64>;

    pub fn add_compaction_filter_with_cutoff(
        &self,
        filter: CompactionFilterRequest,
        cutoff_ts: u64,
    ) -> Result<u64>;

    pub fn remove_compaction_filter(&self, id: u64) -> Result<bool>;

    pub fn list_compaction_filters(&self) -> Vec<CompactionFilterInfo>;

    pub fn compaction_filter_stats(&self) -> CompactionFilterStats;
}
```

`add_compaction_filter` validates the request, assigns an ID, records the MVCC
cutoff, persists the filter to the manifest, and then publishes it to the
in-memory filter list. It is suitable for ad hoc cleanup filters that should
cover all writes committed before installation.

`add_compaction_filter_with_cutoff` is for higher-level integrations that
already have a durable cutoff timestamp, such as a range tombstone or
caller-provided durable logical-delete marker. It validates that `cutoff_ts <=
latest_commit_ts()` and returns an error if `cutoff_ts >
latest_commit_ts()`. It then persists the installed filter with that exact
cutoff. Using installation-time `latest_commit_ts()` can delete later writes if
the prefix was reused or accepted writes between logical deletion and filter
installation.

Error conditions for the API methods:

- `add_compaction_filter` / `add_compaction_filter_with_cutoff`: returns an
  error if the prefix is empty, the encoded key would exceed size limits, a
  duplicate active filter with the same prefix and cutoff exists, `cutoff_ts >
  latest_commit_ts()`, or the manifest write fails.
- `remove_compaction_filter`: returns `Ok(false)` if the filter ID is not found;
  returns an error if the manifest write fails.

The MVP does not add a table catalog to `KvEngine`. The filter API is a storage
cleanup primitive. Callers using it for table, tenant, or namespace drops are
responsible for durably enforcing logical read and write invisibility before
installing the filter.

`CompactionFilterInfo` should be a public API type separate from
`InstalledCompactionFilter`, which is the persisted manifest payload. The
persisted type should remain versioned and serde-compatible; the public listing
type can evolve with Rust API compatibility constraints.

### 7.2 Prefix Validation

Prefix filters must reject:

1. Empty prefixes.
2. Prefixes whose encoded internal-key length would exceed the existing key
   size limits, using `LsmStorageInner::validate_key_size(prefix)` or the same
   `encoded_internal_key_len(prefix.len()) <= MAX_ENCODED_KEY_LEN` check.
3. Duplicate active prefix filters with the same prefix and cutoff.

Overlapping prefixes are allowed. For example, `tenant:` and `tenant:42:` may
both be active. A matching key can be dropped if any active filter says it is
eligible and the compaction-level safety gates are satisfied.

Empty prefixes are intentionally disallowed in the MVP because they are too
easy to misuse as "drop the whole database eventually". A future RFC can add a
separate explicit full-database cleanup operation if needed.

### 7.3 Filter Stats

Expose cumulative in-memory stats:

```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct CompactionFilterStats {
    pub entries_checked: u64,
    pub entries_dropped: u64,
    pub bytes_dropped: u64,
    pub filters_active: usize,
}
```

Stats are best-effort observability. They do not need to be persisted in the
MVP. Stats are cumulative from engine start and reset to zero on restart.
`remove_compaction_filter` does not clear stats; they remain global across all
filters. `filters_active` reflects the current count of installed filters.

---

## 8. Manifest Persistence

Crash recovery must preserve active filters. Otherwise a crash in the middle of
a filtered compaction could leave some SSTs cleaned and others not cleaned, then
silently stop applying the policy after restart.

Add manifest records:

```rust
pub enum ManifestRecord {
    // existing records...
    AddCompactionFilter(InstalledCompactionFilter),
    RemoveCompactionFilter(u64),
    Snapshot {
        // existing fields...
        active_compaction_filters: Vec<InstalledCompactionFilter>,
        next_compaction_filter_id: u64,
    },
}
```

Replay rules:

1. Recovery maintains `active_filters: BTreeMap<u64, InstalledCompactionFilter>`
   and `next_compaction_filter_id`.
2. `AddCompactionFilter(filter)` inserts or replaces that active filter ID and
   advances `next_compaction_filter_id` to at least `filter.id + 1`.
3. `RemoveCompactionFilter(id)` removes the active filter ID.
4. Manifest snapshots replace `active_filters` and
   `next_compaction_filter_id` atomically; later manifest records replay on top.
5. `LsmStorageInner::open` initializes `compaction_filters` and the ID
   allocator from the recovered map.

Adding filter fields to `Snapshot` should either bump
`MANIFEST_FORMAT_VERSION` to 3 or use `#[serde(default)]` fields with an
explicit compatibility decision. Because this RFC already changes recovery
state and manifest semantics, the preferred implementation is a format-version
bump with tests for rejecting unsupported versions.

`add_compaction_filter` must write the manifest record before publishing the
filter to the compaction thread. This creates a conservative crash contract:
after restart, every filter that could have affected an SST remains active.

`remove_compaction_filter` needs the opposite linearization. It must first
prevent future compactions from snapshotting the filter, then wait for or
account for in-flight compactions that already captured it, and only then write
`RemoveCompactionFilter`. Otherwise a compaction could snapshot the still
in-memory filter after the remove record is durable, publish filtered SSTs, and
crash with recovery believing the filter had already been removed.

The simplest MVP implementation is to serialize filter add/remove and
compaction filter snapshot creation under a shared `filter_lock` plus a filter
epoch:

1. Compaction snapshots `(epoch, active_filters)` while holding `filter_lock`.
2. Removing a filter marks it unavailable for future snapshots while holding
   `filter_lock`.
3. Removal waits for in-flight compactions that captured the old epoch to
   finish, or records enough epoch metadata to prove they cannot publish after
   removal.
4. Only then does removal append `RemoveCompactionFilter`.

Prefix reuse is safe only after `remove_compaction_filter` returns and all
pre-remove filtered compactions have finished.

---

## 9. Compaction Integration

### 9.1 Snapshot Active Filters

`compact_from_iter` should snapshot the active filters and their epoch once
before scanning:

```rust
let filter_snapshot = self.compaction_filters.snapshot_for_compaction();
let can_publish_filter_deletion = self
    .mvcc
    .as_ref()
    .is_some_and(|m| m.can_publish_filter_deletion(filter_snapshot.max_cutoff_ts()));
```

The compaction then uses a stable filter set for its entire output. Filters
added after compaction starts affect later compactions, not this one.

### 9.2 Match Decoded User Keys

Prefix filters must match decoded user keys, not raw internal keys. This is
required because MVCC internal keys append an escaped timestamp suffix.

```rust
fn matches_prefix(key: KeySlice<'_>, prefix: &[u8], scratch: &mut Vec<u8>) -> bool {
    scratch.clear();
    key.decode_user_key_into(scratch);
    scratch.starts_with(prefix)
}
```

The MVP should always decode into a reusable scratch buffer for correctness and
to avoid per-key heap allocations in the compaction loop.
`encoded_user_key()` returns memcomparable encoded bytes with 8-byte group
markers, not raw user-key bytes, so raw prefixes longer than or crossing group
boundaries do not byte-compare correctly against it. Encoded-prefix
optimizations are future work and require dedicated tests for 7-byte, 8-byte,
9-byte, longer-than-8-byte, embedded-`0x00`, and arbitrary binary prefixes.

### 9.3 Decision Function

```rust
struct FilterContext {
    /// True when no active read guard or transaction holds a timestamp at or
    /// below the filter's cutoff_ts.
    can_publish_deletions: bool,
    /// True only for bottom-level compactions or tasks that prove all
    /// lower-level overlaps for the filtered key range are included.
    compaction_can_drop_live_entries: bool,
}

impl InstalledCompactionFilter {
    fn should_drop(
        &self,
        key: KeySlice<'_>,
        filter_context: &FilterContext,
        scratch: &mut Vec<u8>,
    ) -> bool {
        if !filter_context.can_publish_deletions {
            return false;
        }
        if !filter_context.compaction_can_drop_live_entries {
            return false;
        }
        // Only drop live values (Put entries), not tombstones. Dropping a
        // tombstone could resurrect an older value from a lower level, because
        // the tombstone may be hiding an older live version. Tombstone GC is
        // handled by the existing compaction rules, not by filters.
        if key.kind() != KeyKind::Put {
            return false;
        }
        if key.ts() > self.cutoff_ts {
            return false;
        }

        match &self.kind {
            CompactionFilterKind::Prefix(prefix) => {
                key.decode_user_key_into(scratch);
                scratch.starts_with(prefix)
            }
        }
    }
}
```

`compaction_can_drop_live_entries` is true only for bottom-level compactions or
tasks that prove all lower-level overlaps for the filtered key range are
included.

When multiple active filters match the same entry, the drop decision is OR-ed:
if any filter's `should_drop` returns true and the safety gates pass, the entry
is dropped. If one filter's `cutoff_ts` allows the drop and another's does not,
the entry is still eligible because the per-filter `cutoff_ts` check is
independent.

When dropping an entry, prefer skip-until semantics over simple removal where
the iterator supports it. TiKV's compaction filter uses `RemoveAndSkipUntil` to
jump past older versions of the same key rather than leaving individual
tombstones that can only be freed at the bottommost level. The same pattern
applies here: if the compaction iterator can seek past the current key's
remaining versions, doing so reduces tombstone accumulation and improves
compaction efficiency.

### 9.4 Empty Output SSTs

Filtering can drop every entry in a compaction run. The existing builder
already supports `builder.is_empty()` checks before final output. The
implementation must preserve this behavior:

1. Do not create an empty SST.
2. Still apply the compaction result so old SSTs are removed from the state.
3. Record the compaction manifest entry with an empty new-SST list.

Empty output is required MVP behavior, not an optional optimization.
`LeveledCompactionController`, `SimpleLeveledCompactionController`,
`TieredCompactionController`, force-full compaction, and manifest replay must
all accept `new_sst_ids == []`. Tiered compaction currently derives the
replacement tier ID from `output[0]`; that must change so an all-filtered
tiered compaction removes the compacted tiers without inserting a replacement
run.

---

## 10. Concurrency and Correctness

### 10.1 Foreground Operations

Foreground `put`, `delete`, `get`, and `scan` do not evaluate compaction
filters. They only interact with filters through normal LSM state publication:
once compaction installs a new state without a filtered entry, later reads no
longer see that entry. Memtable entries are never filtered; only SST entries
during compaction are evaluated. A write that is still in the memtable or WAL
remains visible regardless of any installed filter.

### 10.2 Transactions

Transaction reads use the same snapshot read path as ordinary scans. A
transaction that starts before filtered compaction publishes can see filtered
keys. Because transactions can reload the current LSM state at their pinned
timestamp, filtered compaction must not publish physical deletion while any
read guard or transaction that could observe the pre-filter state is active.

Serializable OCC remains point-key based. Installing a compaction filter is not
treated as a transaction and does not add predicate conflicts in the MVP.

### 10.3 Crash Recovery

The crash contract is:

1. If a filter affected any persisted SST, recovery must continue to know about
   that filter unless it was also durably removed.
2. Replaying compaction records reconstructs the SST set as usual.
3. Replaying filter records reconstructs the active filter list.
4. The filter list and next filter ID in manifest snapshots supersede earlier
   filter records.
5. Empty-output `Compaction` and `CompactionV2` records replay identically to
   the live path.

Because compaction itself is already recorded atomically through the manifest,
there is no need to record per-entry filter decisions.

### 10.4 Cache Backfill

Cache backfill should only backfill blocks that were actually written to output
SSTs. Since filtered entries never enter the builder, the existing
`build_with_backfill` path naturally excludes dropped data.

---

## 11. Testing Plan

### 11.1 Unit Tests

1. Prefix validation rejects empty prefixes.
2. Prefix validation rejects too-long encoded keys.
3. Installed filter IDs are unique and stable.
4. Filter matching uses decoded user keys for 7-byte, 8-byte, 9-byte,
   longer-than-8-byte, embedded-`0x00`, and binary prefixes.
5. MVCC cutoff keeps versions above the cutoff.
6. The MVCC filter-deletion gate rejects publication while active readers are
   present.
7. Duplicate exact `(kind, cutoff_ts)` filters are rejected.
8. Overlapping non-duplicate prefixes are accepted.

### 11.2 Integration Tests

1. Add a prefix filter, force full compaction, and verify matching keys are no
   longer returned.
2. Verify non-matching keys survive the same compaction.
3. Verify prefix-filtered compaction can remove all entries from an input SST
   without corrupting level state.
4. With MVCC, add a filter, write a newer matching key, compact, and verify the
   newer key survives.
5. With a long-running read guard, verify filtered compaction keeps matching
   entries instead of publishing physical deletion.
6. Verify non-bottom compaction with a matching prefix preserves live values
   when older lower-level versions exist.
7. Verify bottom-level or proven-full-overlap compaction can physically drop
   matching live values without resurrection.
8. With value separation enabled, filter a large-value prefix, compact, run
   vLog GC, and verify unrelated vLog values remain readable.
9. Verify per-output-SST vLog references after a compaction emits multiple SSTs
   and filters out some large values.
10. Restart after adding a filter and verify it is still active.
11. Restart after removing a filter and verify it is inactive.
12. Restart after a filtered compaction and verify state and active filters
   replay correctly.
13. Replay empty-output `Compaction` and `CompactionV2` records for force-full,
   leveled, simple-leveled, and tiered compaction.
14. Exercise filter removal racing with an in-flight compaction snapshot.
15. Verify a compaction filter with a scattered prefix (e.g., `idx:email:`)
   drops matching keys after compaction without affecting non-matching keys.
16. Verify a compaction filter with a value-based predicate (e.g., embedded
   TTL) drops only expired entries during compaction.
17. Verify that compaction filters do not affect the read path: filtered keys
   remain visible until compaction rewrites the containing SSTs.
18. Verify a compaction filter with many active prefixes (e.g., 100+) does not
   degrade read performance (no read-path overhead).
19. Verify filter addition racing with an in-flight compaction: the new filter
   does not affect the running compaction but applies to the next one.
20. Verify concurrent filter add and remove (from different threads) serialize
   correctly under `filter_lock`.
21. Verify manifest snapshot with active filters survives recovery: write
   filters, trigger manifest snapshot, restart, and verify filters are active.
22. Verify filter stats (`entries_checked`, `entries_dropped`, `bytes_dropped`,
   `filters_active`) are accurate after a filtered compaction.
23. With value separation and a non-bottom compaction, verify that a filtered
   key with a `ValuePointer` is kept (not dropped) and its vLog reference is
   still registered in the output SST.

### 11.3 Regression Tests

1. Existing tombstone bottom-level tests continue to pass.
2. Existing MVCC compaction GC tests continue to pass.
3. Existing vLog GC tests continue to pass.
4. Prefix bloom and prefix scan behavior is unchanged by installed filters
   until compaction publishes filtered state.

---

## 12. Benchmark Plan

Add `kv-engine/benches/compaction_filter_benchmarks.rs` and register it as a
new `[[bench]]` target in `kv-engine/Cargo.toml`. Use a large structured
keyspace:

```text
tenant:live:{n}
tenant:drop:{n}
tenant:other:{n}
```

Measure:

1. Compaction throughput with no filters.
2. Compaction throughput with one active prefix filter.
3. One prefix shorter than 8 bytes.
4. One prefix longer than 8 bytes.
5. Compaction throughput with multiple active prefix filters.
6. All-filtered empty-output compaction.
7. Output SST bytes written.
8. vLog bytes reclaimed after GC when large values are filtered.
9. Read latency for non-filtered prefixes after compaction.

Expected result: the filter adds small CPU overhead to compaction but reduces
output bytes and later read/write amplification when the filtered prefix covers
a meaningful fraction of compacted data.

Consider adding SST property-based gating (inspired by TiKV's `check_need_gc`):
before running filters on a compaction input set, inspect SST table properties
to estimate whether any entries would actually be filtered. Skip the filter
evaluation when no benefit is expected (e.g., all entries have `ts > cutoff_ts`,
or no keys match any active prefix based on SST-level bloom or index metadata).
This avoids CPU overhead on compactions that would not drop anything.

---

## 13. Rollout Plan

1. Replace the placeholder `CompactionFilter::Prefix(Bytes)` with request and
   installed-filter types.
2. Add manifest records, snapshot fields, recovery state, and
   `next_compaction_filter_id`.
3. Add filter add/remove serialization and in-flight compaction epoch tracking.
4. Add `add_compaction_filter`, `remove_compaction_filter`,
   `list_compaction_filters`, and `compaction_filter_stats`.
5. Add MVCC cutoff linearization (under `write_lock`) and the
   `can_publish_filter_deletion` helper.
6. Add `FilterContext` type and the `should_drop` decision function with
   tombstone exclusion.
7. Add empty-output compaction support for all controllers and manifest replay.
8. Change compaction output metadata to preserve per-output-SST vLog refs
   (requires `CompactionV2` manifest record update).
9. Wire filter evaluation into `compact_from_iter` after MVCC/tombstone GC and
   only when the lower-level safety gates pass.
10. Add unit, integration, recovery, and race tests.
11. Add manifest migration path for existing v2 manifests (auto-upgrade to v3
    or reject with a clear error).
12. Add a feature gate (`enable_compaction_filters` in `LsmStorageOptions` or
    a compile-time feature flag) for incremental rollout.
13. Document the namespace-drop recipe, including the two-layer approach
    (logical-delete marker + compaction filter), explicit cutoff selection,
    and prefix-reuse restrictions.
14. Add benchmark coverage.

The feature should start as explicit opt-in API only. No default filters should
be installed from `LsmStorageOptions`.

---

## 14. Open Questions

1. Should the engine expose a helper that adds a filter and immediately runs a
   force compaction?
2. Should filter matching be accelerated with prefix tries if the active filter
   list grows beyond a small number?
3. Should the manifest snapshot include filter stats or only active filter
   definitions?
4. ~~Should future range tombstones reuse the same filter matching machinery, or
   remain a separate read-path feature?~~ Resolved: range tombstones are a
   separate read-path feature (Section 6.6, Section 15).
5. Should compaction records include the filter epoch/list used for auditability
   even if recovery does not need per-entry filter decisions?

---

## 15. Future Work

1. **Range tombstones (`DeleteRange`)**: the primary mechanism for contiguous
   namespace deletion. Writes a range tombstone that provides immediate read
   invisibility; standard compaction handles cleanup automatically. This is the
   recommended approach for dropping tenants, tables, and logical namespaces
   whose keys share a contiguous prefix.
2. TTL filters: drop keys whose encoded timestamp or value metadata is older
   than a caller-specified cutoff.
3. Table-aware filters: drop only keys from SSTs below a target level.
4. Filter tries for many active prefixes.
5. CLI support for installing, listing, and removing filters.

---

## 16. References

- [RocksDB Compaction Filter](https://github.com/facebook/rocksdb/wiki/Compaction-Filter) — upstream API and semantics.
- [RocksDB DeleteRange](https://github.com/facebook/rocksdb/wiki/DeleteRange) — range tombstone API for contiguous range deletion.
- [TiKV Garbage Collection](https://tikv.org/deep-dive/distributed-transaction/garbage-collection) — TiKV's two-layer approach: `DeleteRange` for table drops, compaction filter for MVCC GC.
- [TiKV `compaction_filter.rs`](https://github.com/tikv/tikv/blob/master/src/server/gc_worker/compaction_filter.rs) — reference implementation of SST property-based gating (`check_need_gc`), `RemoveAndSkipUntil` optimization, and safe-point-based version filtering.
