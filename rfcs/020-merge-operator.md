# RFC 020: Merge Operator

**Status:** Proposed
**Date:** 2026-08-27
**Author:** kv-engine Contributors
**References:**
- RFC 005: MVCC
- RFC 009: Compaction Filter
- RFC 010: Delete Range
- RFC 013: Chaos Testing
- RFC 018: Steady-State Benchmark Suite
- RFC 019: Checkpoint and Backup API
- RocksDB Merge Operator: https://github.com/facebook/rocksdb/wiki/Merge-Operator
- RocksDB Merge Operator Implementation: https://github.com/facebook/rocksdb/wiki/Merge-Operator-Implementation
- RocksDB Overview: https://github.com/facebook/rocksdb/wiki/RocksDB-Overview

---

## 1. Summary

This RFC proposes a RocksDB-style merge operator for kv-engine. A merge
operation stores an application-defined delta for a key instead of forcing the
caller to run a read-modify-write loop in user code.

The public API shape is:

```rust
pub trait MergeOperator: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;

    fn full_merge(&self, key: &[u8], base_value: Option<&[u8]>, operands: &[&[u8]]) -> Result<Bytes>;

    fn partial_merge(&self, key: &[u8], left_operand: &[u8], right_operand: &[u8]) -> Result<Option<Bytes>> {
        let _ = (key, left_operand, right_operand);
        Ok(None)
    }
}

impl KvEngine {
    pub fn merge(&self, key: &[u8], operand: &[u8]) -> Result<()>;
    pub fn merge_batch(&self, entries: &[(Bytes, Bytes)]) -> Result<()>;
    pub async fn merge_async(&self, key: &[u8], operand: &[u8]) -> Result<()>;
    pub async fn merge_batch_async(&self, entries: &[(Bytes, Bytes)]) -> Result<()>;
}
```

The first implementation should persist merge operands as distinct value kinds
in the existing MVCC value encoding, resolve merge chains through every public
read API, and preserve complete chains through flush and compaction. Materialized
compaction collapse is a later phase, enabled only after the complete-chain and
snapshot invariants are enforced.

This feature closes a real RocksDB feature gap without requiring column
families or a full API rewrite. It also gives kv-engine a practical primitive
for counters, append-only metadata, secondary-index maintenance, and aggregate
state.

---

## 2. Motivation

RocksDB exposes `Merge` as a first-class read-modify-write primitive. Instead
of doing:

```text
old = get(key)
new = update(old, delta)
put(key, new)
```

the caller can submit:

```text
merge(key, delta)
```

The engine records the intent and may combine it later during reads, iterator
materialization, flush, or compaction. This avoids extra user-visible point
reads, reduces write contention for hot counters, and lets the storage engine
choose when operand chains should be collapsed.

kv-engine already has many RocksDB-like features: WAL durability, MVCC,
serializable transactions, prefix scan and prefix bloom support, delete range,
TTL, checkpoints, and steady-state RocksDB comparison coverage. Merge operator
support is a good next feature because it touches the core LSM record model
without requiring a new multi-keyspace architecture.

The feature is intentionally more than an API convenience. If `merge()` simply
called `get()` and `put()` internally, it would preserve behavior but miss the
main storage-system value. The goal is to let merge operands remain lazy in the
LSM until reads or compaction need to materialize them.

---

## 3. Goals

1. Add a configurable merge operator to `LsmStorageOptions`.
2. Add `KvEngine::merge()` and `KvEngine::merge_batch()` APIs.
3. Persist merge records durably through WAL and recovery.
4. Represent merge operands distinctly from put values, tombstones, TTL values,
   and range tombstones.
5. Resolve merge chains correctly in point reads, batch reads, transaction
   snapshot reads, forward scans, prefix scans, and their async wrappers.
6. Preserve MVCC snapshot semantics: a snapshot must see only merge operands
   whose commit timestamp is visible to that snapshot.
7. Preserve complete merge chains during flush and compaction, then collapse
   them only when the operator can produce a stable materialized value without
   violating an active snapshot.
8. Preserve delete semantics: a newer delete hides older values and operands;
   older deletes act as the base absence for newer operands.
9. Preserve range-delete semantics: a covering range tombstone is a merge-chain
    boundary and hides all point records at or below its timestamp.
10. Add deterministic tests for counters, append-like values, delete/merge
    ordering, scans, flush, compaction, WAL replay, and transaction interaction.
11. Add benchmark coverage that compares merge-heavy workloads against the
    equivalent get-modify-put loop and, where possible, RocksDB.

---

## 4. Non-Goals

1. **Column families.** A merge operator is configured for the current single
   keyspace. Per-column-family operators are future work.
2. **Multiple named merge operators in one database.** Applications can
   multiplex behavior inside operands or key prefixes if needed.
3. **User-defined code loading.** Operators are Rust values supplied by the
   embedding process, not dynamic plugins.
4. **Making all operators associative.** The generic operator can decline
   partial merges.
5. **Transparent conflict-free replicated data types.** CRDTs can be built by
   an application-level operator, but the engine only provides merge plumbing.
6. **Compaction-filter replacement.** Merge operators transform update intent;
   compaction filters decide whether existing records are kept or dropped.
7. **Cross-process recovery without operator registration.** A database with
   merge records cannot be opened for normal reads unless a compatible operator
   is configured.
8. **vLog-backed merge operands in the MVP.** The first release rejects
   `merge()` and `merge_batch()` when value separation is enabled. Supporting
   merge-specific vLog records and GC is follow-up work.
9. **Compaction filters in the MVP.** A v6 merge-capable database cannot open
   with, add, or run a compaction filter. Filters need whole-chain semantics and
   are follow-up work.

---

## 5. Existing Building Blocks

### 5.1 MVCC Value Encoding

kv-engine already prefixes stored user values with an internal value kind for
MVCC operations. This RFC extends that encoding with a new merge kind:

```text
existing put, value-pointer, tombstone, and TTL kinds
merge operand
merge operand list (one ordered list for one user key in a batch)
```

The exact byte values must extend `KvKind` after its existing assigned values.
The important contract is that merge operands and ordered operand lists remain
distinguishable after WAL replay, memtable flush, SST readback, and compaction.
Merge values are always inline in the MVP; writes reject value separation until
merge-specific vLog storage and GC exist.

`MergeOperand` stores its operand as the bytes after the kind tag.
`MergeOperandList` has this canonical payload after its kind tag:

```text
operand_count: u32 big-endian
repeat operand_count times:
    operand_len: u32 big-endian
    operand_bytes: [u8; operand_len]
```

The count must be nonzero; zero-length operands are valid and preserved. The
decoder rejects truncated headers, lengths that exceed the remaining bytes,
integer overflow, a zero count, and trailing bytes. Encoding must fit the
existing WAL and SST value-size limits. A malformed merge payload is semantic
corruption. WAL replay validates its outer record framing and preserves the raw
merge kind and payload without decoding the operand list; the first read, scan,
or compaction that interprets the payload returns a clear corruption error. It
must not silently reinterpret, truncate, or skip operands.

### 5.2 Internal Keys

Internal keys already include the user key and timestamp. Merge records use the
same internal-key ordering as puts and deletes. Newer timestamps must be
visited before older timestamps for the same user key so point reads can collect
the visible merge chain until they find a base value, delete, or the oldest
visible record.

The MVP does not add an intra-timestamp suffix to internal keys. A merge-only
batch therefore groups all entries by user key and emits exactly one
`MergeOperandList` value per key, containing that key's operands in their
original input order. This preserves a single commit timestamp and one WAL
durability boundary without inserting duplicate physical keys into the memtable.
The read path expands that list in order before applying older operands. For
example, `merge(k, +1)`, `merge(other, +1)`, `merge(k, +2)` produces one list
for `k` with `+1, +2`, not two same-timestamp records for `k`.

### 5.3 WAL and Batch Writes

The write path already supports raw batches, durable WAL submission, MVCC commit
timestamps, and last-op-wins behavior for public batches. Merge batches reuse
the same durability and timestamp allocation path. Their ordered operand-list
value is written using the existing WAL `Put` entry tag; the merge kind lives in
the value bytes, so this does not add an unversioned WAL entry tag.

### 5.4 Compaction

Compaction already merges sorted streams and removes hidden versions. That is
the natural place to collapse old merge chains into materialized put values
when the merge operator is configured and all relevant operands are available
in the compaction input.

---

## 6. Public API

### 6.1 Options

Add an optional merge operator to storage options:

```rust
pub struct LsmStorageOptions {
    pub merge_operator: Option<Arc<dyn MergeOperator>>,
    // existing fields...
}
```

`LsmStorageOptions` currently derives `Debug`. The implementation must retain
that API by providing a manual `Debug` implementation that reports whether an
operator is configured, rather than requiring every application operator to
implement `Debug`.

The default remains `None`. Calling `merge()` without an operator returns
`Error::InvalidInput` or a more specific configuration error.

Opening a database that contains persisted merge records without an operator is
allowed only for operations that do not need to interpret the records, such as
metadata inspection. Normal `get`, `scan`, `flush`, and compaction should fail
fast with a clear error if unresolved merge operands are encountered and no
operator is configured.

### 6.2 Merge Operator Trait

The generic operator receives a user key, an optional base value, and merge
operands ordered from oldest to newest:

```rust
pub trait MergeOperator: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;

    fn full_merge(&self, key: &[u8], base_value: Option<&[u8]>, operands: &[&[u8]]) -> Result<Bytes>;

    fn partial_merge(&self, key: &[u8], left_operand: &[u8], right_operand: &[u8]) -> Result<Option<Bytes>> {
        let _ = (key, left_operand, right_operand);
        Ok(None)
    }
}
```

`full_merge` is required. It materializes a final value from a base value and a
non-empty operand list.

`name()` and `version()` form the stable persisted operator identity. The name
must be an application-controlled, immutable identifier; changing merge
semantics requires a version bump. The engine records this identity in the
manifest before the first merge record is durable and rejects a configured
operator whose identity differs on reopen, before normal reads or compaction.

`partial_merge` is optional. It may combine two adjacent operands into one
operand when doing so preserves application semantics. Returning `Ok(None)`
means the operands must remain separate and ordered.

Operator errors are storage errors. They should abort the read, scan, flush, or
compaction that triggered them. Compaction must not drop input files if merge
materialization fails.

### 6.3 Engine Methods

```rust
impl KvEngine {
    pub fn merge(&self, key: &[u8], operand: &[u8]) -> Result<()>;
    pub fn merge_batch(&self, entries: &[(Bytes, Bytes)]) -> Result<()>;
    pub async fn merge_async(&self, key: &[u8], operand: &[u8]) -> Result<()>;
    pub async fn merge_batch_async(&self, entries: &[(Bytes, Bytes)]) -> Result<()>;
}
```

`merge()` appends one operand for one key.

`merge_async()` and `merge_batch_async()` ship with the synchronous methods.
They follow the existing mutation-wrapper pattern: copy borrowed input before
dispatching to the blocking worker, hold a write admission guard, and delegate
to the same durable synchronous implementation.

`merge_batch()` appends many merge operands under one MVCC commit timestamp and
one WAL durability boundary. It groups all entries by user key and encodes one
ordered operand-list record per key, preserving the input order of that key's
operands without requiring duplicate internal keys at the same timestamp.

The existing `write_batch()` API should not silently reinterpret puts as merge
operands. If the project later adds typed batch operations, that API can include
`BatchOp::Merge`.

---

## 7. Semantics

### 7.1 Point Reads

For `get(key)` at snapshot timestamp `T`:

1. Find the newest range-tombstone timestamp `R` that covers `key` and is
   visible at `T`.
2. Walk point records for `key` from newest to oldest, considering only records
   visible at the snapshot: `record_ts <= T` and, when a covering range
   tombstone exists, `record_ts > R`.
3. Collect merge record groups. A `MergeOperand` is a one-operand group; a
   `MergeOperandList` is one group whose operands remain in its encoded order.
4. Stop at the first visible put, delete, or expired TTL record.
5. If no merge operands were collected, return the put value, not found, or the
   current tombstone result as today.
6. If groups were collected, reverse the group sequence into oldest-to-newest
   order, preserve operand order inside every group, then flatten the groups.
7. Call `full_merge(key, base_value, flattened_operands)`.
8. Return the materialized value.

A newer put shadows older merge operands. A newer delete shadows older put and
merge records. Newer merge operands over an older delete call `full_merge` with
`base_value = None`.

For example, an older `MergeOperand(+1)` followed by a newer
`MergeOperandList(+2, +3)` resolves operands as `+1, +2, +3`. Reversing a
flattened newest-to-oldest collection would incorrectly produce `+1, +3, +2`.

A covering range tombstone is the equivalent boundary for point records at or
below its timestamp. For example, `merge @3` over `delete_range @2` over
`put @1` calls `full_merge` with `base_value = None`; a range tombstone at `@4`
hides the `merge @3` as well. This rule applies identically to point reads,
batch reads, transaction reads, scans, and compaction materialization.

Every resolver uses the same `record_ts <= T` visibility bound before collecting
operands or choosing a base. This includes `get`, batch reads, transaction reads,
all scan variants, and compaction work performed for a pinned snapshot.

### 7.2 Scans and Prefix Scans

Scans must expose one logical value per user key. If a key has visible merge
operands, the iterator must materialize them before yielding the key.

The first implementation may materialize one key at a time inside the LSM merge
iterator. It does not need to precompute the full scan result. This keeps memory
bounded for large ranges.

Scan errors from merge operators must be returned through the iterator status or
the existing result path. The iterator must not silently skip keys whose merge
operator fails.

The same materialization contract applies to `batch_get()`, `batch_get_async()`,
transaction snapshot `get()`, transaction range and prefix scans and their async
variants, `get_async()`, `scan_async()`, `prefix_scan_async()`, and parallel
scans. Async wrappers may delegate to the synchronous resolver, but must not
bypass it.

### 7.3 Writes and Deletes

`put(key, value)` writes a materialized value and shadows older merge operands.

`delete(key)` writes a tombstone and shadows older merge operands.

`merge(key, operand)` writes a merge operand. It does not check whether the key
currently exists.

The MVP rejects `merge()` and `merge_batch()` when `value_separation` is
enabled. It also rejects opening a merge-enabled v6 database with value
separation enabled, even if the caller will not immediately issue a merge: a
later flush must never reinterpret an existing merge record as an inline value
or value pointer. Merge operands are inline records, and a future vLog
integration must add merge-specific pointer kinds plus liveness accounting
before lifting this restriction.

The MVP also rejects opening a merge-enabled v6 database with active compaction
filters or adding a compaction filter to one. A filter can otherwise drop one
member of a merge chain independently of MVCC pruning. Filter-aware
whole-chain materialization is future work.

An expired TTL value is a tombstone base. A newer merge over an expired TTL base
calls `full_merge` with `base_value = None`; a newer merge over an unexpired TTL
value receives that TTL value as its base. Compaction uses the same fixed
wall-clock snapshot as its input visibility check and must retain the original
records if it cannot materialize with those rules.

An expired TTL record is terminal for older history, exactly like a point
tombstone. If no newer merge operand was collected, it returns not found and
hides every older put and merge; if newer operands were collected, it supplies
`base_value = None`. For example, `merge @1; ttl_put @2; expiry` returns not
found, while `ttl_put @1; expiry; merge @2` merges with no base.

In the MVP, TTL expiration remains a logical read-time rule for every v6
merge-capable database. Compaction must not remove an expired TTL record: it can
be the boundary between a newer merge operand and an older put. A later TTL-GC
phase may remove such a record only while atomically materializing the complete
chain, including range-tombstone visibility, and only when no active snapshot
can observe a pre-materialization version.

### 7.4 Batches

Public batch semantics remain last-op-wins for the existing `write_batch()`
put/delete API.

`merge_batch()` has order-preserving semantics for duplicate keys because
multiple operands for one key may all be meaningful. The entries:

```text
merge(k, +1)
merge(k, +2)
merge(k, +3)
```

must be encoded as one ordered operand-list record and resolved as `+1`, then
`+2`, then `+3`.

Mixed put/delete/merge batches are not part of the MVP. A future typed batch
must define a physical representation that preserves operation order for the
same key; it must not reuse the existing last-op-wins batch path. For example:

```text
merge(k, +1)
put(k, 10)
merge(k, +2)
```

should resolve to `12`, not `13`.

---

## 8. Compaction Rules

Compaction must treat a merge chain as indivisible for MVCC pruning. Generic
bottom-level logic must never retain a merge operand while dropping an older
operand or its base. A merge operand can be in an overlapping SST that was not
selected as a compaction input, so input-local detection is insufficient.
Until the chain is fully materialized from all live overlapping sources,
compaction preserves all of its physical records.

Compaction may collapse merge operands into a put value only when it includes
every live overlapping source for that user key and can see a complete chain for
the visible key range.

The first collapse implementation prohibits collapse for any chain whose base or
visible records carry TTL metadata. `full_merge` returns only bytes, so writing
an ordinary put would otherwise lose an unexpired base's expiration deadline.
TTL-preserving materialization, including an output TTL record with the original
deadline, is future work and must be specified before TTL-backed collapse is
enabled.

Safe collapse cases:

1. A run of merge operands followed by an older put in the full live source set.
2. A run of merge operands followed by an older delete in the full live source set.
3. A run of merge operands older than the oldest active snapshot, where there is
   no older visible base value.
4. Adjacent operands where `partial_merge()` returns a replacement operand.

Unsafe collapse cases:

1. There may be an older base value outside the compaction input that is still
   visible to a snapshot.
2. An active snapshot can still observe an older pre-collapse version.
3. The merge operator is missing or returns an error.
4. TTL or compaction-filter logic would make the base visibility ambiguous.
5. The chain has TTL metadata; the MVP does not yet propagate expiration through
   a materialized output value.

The MVP preserves operands during compaction and disables generic
obsolete-version pruning for every v6 merge-capable database. This broad rule
avoids dropping a base whose newer operand is outside the selected input. A later
collapse phase may replace a complete chain only after it includes every live
overlapping source for that user key and no active snapshot needs any
pre-collapse version. It also retains expired TTL records and all range-tombstone
fragments. Retaining every fragment is necessary because a historical point
record covered by a range tombstone can become a later merge base; the fragment
may be removed only with every covered historical point record, or while
atomically materializing the complete chain. Correctness is more important than
aggressive space amplification reduction in the first slice.

---

## 9. Transactions

Serializable transactions should support merge operations after the non-
transactional API is stable.

Transaction API shape:

```rust
impl Transaction {
    pub fn merge(&self, key: &[u8], operand: &[u8]) -> Result<()>;
}
```

Transaction semantics:

1. A transaction-local merge is visible to later reads in the same transaction.
2. A transaction-local put shadows earlier local merges for that key.
3. A transaction-local delete shadows earlier local puts and merges for that key.
4. Serializable conflict detection treats merge as a write.
5. Read-write transactions that merge after reading a key retain the existing
   read-set validation behavior.

The first implementation may reject `txn.merge()` with `Unsupported` and track
transaction support as a follow-up. Non-transactional merge support should not
be blocked on transaction-local merge materialization.

---

## 10. WAL and Recovery

Merge records must be durable in WAL before they are published to the memtable
when WAL is enabled.

Recovery restores merge records exactly as merge records. It does not materialize
them during replay because the operator may be unavailable, may be expensive, or
may depend on future records in the replay stream. Merge values use the existing
WAL `Put` entry tag with a merge `KvKind` payload, so WAL recovery needs no new
entry tag; it recognizes and retains the merge value kind rather than treating it
as inline data. Recovery deliberately does not decode `MergeOperandList` payloads:
after replay, the first read, scan, or compaction performs that semantic validation
and reports any malformed payload as corruption.

Crash-recovery tests should cover:

1. merge record replay into an empty database;
2. merge over put before crash;
3. put over merge before crash;
4. delete over merge before crash;
5. merge batch with duplicate keys before crash;
6. missing merge operator after replay.

Chaos tests should add failpoints around WAL append, WAL sync, memtable publish,
flush materialization, and compaction output installation.

---

## 11. Format Compatibility

Adding a merge value kind changes the logical record model but does not need a
new SST container format if the current value-kind encoding has room for a new
tag.

Compatibility rules:

1. Old databases without merge records continue to open normally.
2. New databases with merge records require a build that understands the merge
   value kind.
3. Reads that encounter merge records require a configured operator.
4. Checkpoints preserve merge records exactly.
5. Backups or copied directories must be opened with the same logical merge
   operator as the source.

Merge persistence requires a per-database manifest transition before the first
merge record is written. New databases and existing databases that have never
merged remain format v5 and retain normal vLog and compaction-filter behavior.
Immediately before the first merge WAL append, the engine performs the existing
crash-safe manifest snapshot transition to v6 and durably records:

```text
MergeEnabledV6 { name: String, version: u32 }
```

The transition and identity record must be durable before the merge WAL append.
The v6 snapshot retains the identity through later manifest snapshots and
checkpoint copies. Older builds reject the unknown transition record rather than
misinterpreting merge values. Normal reads and compaction reject a configured
operator that does not match; metadata-only open without an operator remains
permitted under the existing missing-operator restrictions.

Every v6 `ManifestRecord::Snapshot` includes a backward-compatible field:

```text
format_version: 6
merge_operator_identity: Option<MergeOperatorIdentity>
```

Every v6 snapshot writer sets `format_version = 6`. The identity field defaults
to `None` only when deserializing pre-v6 snapshots; recovery applies the
identity rule only after confirming `format_version == 6`. A v6 snapshot with a
missing identity is corrupt metadata, including a truncated snapshot whose
defaulted field would otherwise deserialize as `None`.

Every v6 snapshot writer, including metadata-only and checkpoint snapshot paths,
copies the recovered database identity into `Some(...)` even when the open call
does not configure an operator. V6 recovery rejects a missing identity and a
configured identity mismatch before normal reads or compaction. This prevents
manifest compaction from erasing the transition record that established the
identity.

The transition fails if any compaction filter is installed. The caller must
remove every filter and complete that manifest update before enabling merge; the
transition rechecks the empty filter registry while holding the state lock. It
must not silently remove filters, because that would change the database's
retention policy.

Tests must cover interrupted v5-to-v6 upgrade recovery and clear rejection of a
merge-record database opened by a pre-v6 implementation.

They must also force a later manifest snapshot, reopen with a mismatched
operator identity, and verify rejection. The same mismatch check must be run
against a checkpoint copy after that snapshot, proving that identity survives
both manifest compaction and checkpoint creation.

---

## 12. Benchmark Plan

Add merge-specific rows to `write-perf` first:

```text
merge_counter
merge_counter_get_modify_put
merge_counter_flush
merge_counter_compaction
merge_append
merge_hot_counter_zipfian
```

The minimum comparison should measure:

1. `merge_counter` vs equivalent `get` + `put`;
2. cold read of long merge chains;
3. read after flush;
4. read after compaction;
5. write throughput with WAL sync enabled;
6. correctness counters for expected final values.

The initial benchmark operator is a benchmark-only `CounterMergeOperator` with
`name() = "benchmark.counter"` and `version() = 1`. Its base values, operands,
and materialized values are signed 64-bit little-endian integers; a missing base
is zero; `full_merge` adds operands in oldest-to-newest order using checked
addition. Each workload preloads its key set with zero values, performs only
`+1` operands, and validates every sampled final value against the operation
count recorded by the runner. It uses the existing `write-perf` steady-state
options for key count, client count, duration, warmup, WAL mode, and key
distribution; a workload result is invalid if validation fails.

The `merge_counter_get_modify_put` baseline is deliberately single-client. It
uses the same deterministic uniform key schedule and encoding as the one-client
merge counter run, so every issued update has an exact expected final value.
It is a serialized read-modify-write cost baseline, not a claim that concurrent
unconditional `get` plus `put` preserves counter updates.

`merge_append` uses a benchmark-only `AppendMergeOperator`: a missing base is
empty bytes and `full_merge` concatenates the base and operands in oldest-to-
newest order. It is also single-client. Each operand is one little-endian `u64`
operation sequence number; validation checks that every key's final bytes equal
the concatenation of the emitted sequence numbers for that key in schedule
order.

`merge_counter` uses uniform key selection. `merge_hot_counter_zipfian` uses
the existing Zipfian selector. `merge_counter_get_modify_put` uses the same key
stream and encoding but performs the serialized read-modify-write baseline. The
flush and compaction variants use the same counter workload with the existing
flush/compaction configuration and report both throughput and final-state
validation.

If `crud-bench` gets merge support later, compare ToyKV and RocksDB with the
same counter operator and the same prepared key distribution. Until then, the
engine-local benchmark is enough to validate that merge is useful and not just
API surface.

---

## 13. Implementation Plan

### Phase 1: Core Record Support

1. Add `MergeOperator` trait and `merge_operator` option.
2. Add merge value-kind encoding and decoding.
3. Add the first-merge v5-to-v6 transition, operator identity persistence, and
   transition-time rejection when a compaction filter is installed.
4. Add v6 open-time rejection for vLog and compaction filters.
5. Persist and replay merge records through WAL.
6. Add point-read, batch-get, transaction-snapshot, range-scan, prefix-scan,
   async-scan, parallel-scan, and transaction scan merge-chain resolution.
7. Preserve complete chains during flush and disable generic MVCC pruning, TTL
   cleanup, range-tombstone GC, and compaction filters for merge-enabled v6.
8. Add `KvEngine::{merge, merge_async}`.
9. Add unit tests for put/merge/delete ordering, TTL bases, vLog rejection,
   transition preconditions, and operator-mismatch rejection.

Phase 1 is the first shippable release boundary. No public merge API is exposed
until every persistence and compaction guard in this phase is complete.

### Phase 2: Batch and Iterator Support

1. Add `KvEngine::{merge_batch, merge_batch_async}`.
2. Group all entries by user key and encode one ordered operand-list record per
   key inside a batch.
3. Add batch tests spanning memtable, immutable memtable, and SST sources.

### Phase 3: Compaction Collapse

1. Add conservative compaction collapse for complete chains only after the
   pruning invariant is covered by tests.
2. Add `partial_merge()` support for adjacent operands.
3. Add tests with active snapshots that prevent unsafe collapse.
4. Add compaction failure tests where operator errors abort output publish.

### Phase 4: Transactions and Benchmarks

1. Add transaction-local `merge()`.
2. Integrate merge writes with serializable conflict detection.
3. Add merge workloads to `write-perf`.
4. Optionally add `crud-bench` merge rows for ToyKV vs RocksDB comparison.
5. Update `docs/bench-report-crud-bench-rocksdb.md` if cross-backend merge
   measurements are added.

---

## 14. Test Plan

Required focused tests:

1. merge over missing key;
2. merge over put;
3. put over merge;
4. delete over merge;
5. merge over delete;
6. repeated merge operands preserve order;
7. merge batch duplicate keys preserve order;
8. merge reads after flush;
9. merge reads after compaction;
10. scan materializes merged values once per user key;
11. prefix scan materializes merged values once per matching user key;
12. snapshot read sees only visible operands;
13. active snapshot prevents unsafe compaction collapse;
14. WAL replay preserves unresolved merge records;
15. missing merge operator returns a clear error;
16. operator error aborts read;
17. operator error aborts compaction without deleting old files.
18. `batch_get`, transaction point/range/prefix reads, their async variants,
    async engine scans, and parallel scans return materialized values;
19. non-adjacent duplicate-key merge batches are one logical timestamp and
    preserve each key's input order;
20. bottom-level compaction cannot retain a partial merge chain;
21. first-merge v5-to-v6 manifest transition is crash-safe and older-build open
    is rejected, while a database that never merges stays v5;
22. merge WAL replay uses the existing put entry format;
23. vLog-enabled merge writes and v6 merge-capable database opens are rejected;
24. merge over an expired TTL base uses `None` as the base.
25. merge over a covering range tombstone uses `None` as the base, while a newer
    range tombstone hides the merge;
26. compaction retains an expired TTL boundary and all range-tombstone fragments
    until it safely materializes the complete affected chains.
27. v6 rejects compaction filters both at open and when adding a filter;
28. all range-tombstone fragments remain until their covered historical points
    are removed or the complete affected chain is materialized;
29. reopening with a mismatched merge operator name or version is rejected
    before reads or compaction.
30. enabling merge fails while any compaction filter is installed, and async
    merge methods have the same durable semantics as their synchronous forms.
31. an expired TTL record hides older merges and puts when no newer merge exists.
32. an older single operand followed by a newer duplicate-key operand list
    preserves the list's internal order after merge-chain resolution;
33. `MergeOperandList` round-trips arbitrary and zero-length operands; malformed
    encodings replay through WAL unchanged but fail clearly at the first read,
    scan, or compaction that interprets them;
34. a forced later manifest snapshot and a checkpoint copy both persist
    `Some(MergeOperatorIdentity)`, and recovery rejects a missing or mismatched
    identity for v6.
35. compaction before an unexpired TTL base does not materialize the merge chain,
    and reads after the original expiration still return not found.
36. every v6 snapshot writer, including metadata-only and checkpoint paths, sets
    `format_version = 6` and preserves recovered identity; pre-v6 snapshots may
    omit identity, while a truncated v6 snapshot with a missing identity fails
    recovery as corrupt metadata.

Required regression checks:

```bash
cargo fmt --all -- --check
cargo test --package kv-engine merge
cargo nextest run --workspace --all-features --all-targets
```

The first implementation PR may run a narrower target set locally, but the
feature should not be marked implemented until the full nextest target passes.

---

## 15. Open Questions

1. Should `merge_operator` be required at open time if the manifest advertises
   merge support, or only when a read/compaction encounters a merge record?
2. Should `write_batch()` grow a typed operation enum after it can preserve
   mixed same-key operation order?
3. When should vLog support add merge-specific pointer kinds and GC liveness?

---

## 16. Acceptance Criteria

This RFC is implemented when:

1. `KvEngine::{merge, merge_batch, merge_async, merge_batch_async}` are public
   and documented.
2. Merge records survive WAL replay, flush, compaction, and checkpoint copy.
3. Point reads, batch reads, transaction snapshot reads, range scans, prefix
   scans, and all scan wrappers materialize merge chains correctly.
4. Deletes and puts interact with merge operands according to this RFC.
5. Missing or failing operators produce clear errors without corrupting state.
6. At least one built-in test operator demonstrates numeric counter semantics.
7. Focused merge tests and full workspace nextest pass.
8. `write-perf` has deterministic counter merge and get-modify-put workloads
   with registered steady-state runners, shared input streams, and final-state
   validation.
9. The manifest is upgraded to v6 before persistence, vLog-enabled merge writes
   are rejected, and bottom-level compaction cannot leave partial merge chains.
10. The manifest stores the merge operator identity, v6 rejects compaction
    filters, and range-tombstone GC cannot expose a historical merge base.
11. Enabling merge requires an empty compaction-filter registry, and async
    mutation methods use the same durable implementation as synchronous methods.
