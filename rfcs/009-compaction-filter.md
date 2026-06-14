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
it have passed through compaction. Callers that need immediate deletion
semantics should continue to write tombstones or use a future range-tombstone
feature.

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

1. A tenant is deleted and all keys under `tenant:{id}:` should eventually
   disappear.
2. A secondary index is rebuilt under a new prefix and the old index prefix can
   be discarded.
3. Time-bucketed data under `logs:2026-01:` has expired.
4. Test or benchmark data under a known prefix should be reclaimed without
   issuing millions of point deletes.

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
3. Preserve MVCC snapshot correctness and active-reader safety.
4. Preserve vLog GC correctness by unregistering dropped value-pointer
   references only after new SST state is installed.
5. Persist filter metadata so crash recovery does not leave a partially applied
   cleanup policy behind.
6. Keep foreground reads and writes off the filter hot path.
7. Expose basic stats so callers can see whether filters are doing useful work.

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

### 6.2 MVCC Cutoff and Readers

A filter receives an installation timestamp:

```rust
let cutoff_ts = self.mvcc.latest_commit_ts();
```

`add_compaction_filter` linearizes at cutoff capture. Cutoff capture must be
synchronized with MVCC commit timestamp allocation so every write that commits
after the linearization point receives `commit_ts > cutoff_ts`. Writes with
`commit_ts <= cutoff_ts` are eligible for eventual filtering; writes with
`commit_ts > cutoff_ts` are protected. A write racing with
`add_compaction_filter` is classified by its assigned commit timestamp, not by
which API call returns first.

The MVCC watermark rule used for ordinary version collapse is not sufficient
for filter deletion. Version collapse keeps a newest visible version at or
below the watermark; a filter removes the key entirely. A transaction or read
guard with a pinned timestamp can reload the current LSM state later, so
publishing a state that physically removed filtered entries can break repeatable
snapshot reads.

Therefore filtered physical deletion requires a stronger gate:

1. The entry's `commit_ts` must be `<= cutoff_ts`.
2. The compaction must be safe against lower-level resurrection (Section 6.3).
3. The engine must prove there are no active read guards or transactions that
   could observe the pre-filter state before publishing the filtered compaction
   result.

The implementation should add an explicit MVCC helper such as
`can_publish_filter_deletion(cutoff_ts)` rather than inferring this from
`watermark()`, because `watermark() == latest_commit_ts()` can also occur when
the oldest active reader is at the latest timestamp. If the helper returns
false, compaction may still rewrite SSTs, but matching entries must be kept.

In code, the final keep/drop decision is the conjunction of built-in GC and
filter eligibility:

```rust
let should_keep = keep_for_mvcc_and_tombstone_gc(entry)
    && !filters.should_drop(entry, filter_context);
```

This preserves snapshot isolation even when a long-running read guard overlaps
with compaction.

### 6.3 Lower-Level Resurrection and Tombstones

A prefix filter is logically similar to deleting a range of user keys, but the
MVP does not write durable range tombstones. That means physical deletion is
safe only when dropping the current entry cannot reveal an older value from a
lower level.

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

### 6.4 vLog References

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

Post-compaction vLog GC remains unchanged:

1. Input SST vLog references are collected before old SSTs are unregistered.
2. Output SST references are registered from the builder output.
3. Old SSTs are removed and their block-cache entries invalidated.
4. `post_compaction_gc(...)` is scheduled for input vLog files.

The vLog GC liveness check remains version-specific through
`get_with_kind_at_ts`, so a dropped filtered version becomes reclaimable only
after the LSM state no longer references it.

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
impl KvEngine {
    pub fn add_compaction_filter(
        &self,
        filter: CompactionFilterRequest,
    ) -> Result<u64>;

    pub fn remove_compaction_filter(&self, id: u64) -> Result<bool>;

    pub fn list_compaction_filters(&self) -> Vec<CompactionFilterInfo>;

    pub fn compaction_filter_stats(&self) -> CompactionFilterStats;
}
```

`add_compaction_filter` validates the request, assigns an ID, records the MVCC
cutoff, persists the filter to the manifest, and then publishes it to the
in-memory filter list.

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
MVP.

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
longer see that entry.

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

---

## 13. Rollout Plan

1. Replace the placeholder `CompactionFilter::Prefix(Bytes)` with request and
   installed-filter types.
2. Add manifest records, snapshot fields, recovery state, and
   `next_compaction_filter_id`.
3. Add filter add/remove serialization and in-flight compaction epoch tracking.
4. Add `add_compaction_filter`, `remove_compaction_filter`,
   `list_compaction_filters`, and `compaction_filter_stats`.
5. Add MVCC cutoff linearization and the no-active-reader publication gate.
6. Add empty-output compaction support for all controllers and manifest replay.
7. Change compaction output metadata to preserve per-output-SST vLog refs.
8. Wire filter evaluation into `compact_from_iter` after MVCC/tombstone GC and
   only when the lower-level safety gates pass.
9. Add unit, integration, recovery, and race tests.
10. Add benchmark coverage.

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
4. Should future range tombstones reuse the same filter matching machinery, or
   remain a separate read-path feature?
5. Should compaction records include the filter epoch/list used for auditability
   even if recovery does not need per-entry filter decisions?

---

## 15. Future Work

1. Range filters: drop keys in `[lower, upper)` during compaction.
2. TTL filters: drop keys whose encoded timestamp or value metadata is older
   than a caller-specified cutoff.
3. Table-aware filters: drop only keys from SSTs below a target level.
4. Filter tries for many active prefixes.
5. Immediate range tombstones for read-path delete semantics.
6. CLI support for installing, listing, and removing filters.
