# RFC 005: Multi-Version Concurrency Control

**Status:** Proposed  
**Date:** 2026-06-06  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding multi-version concurrency control (MVCC) to kv-engine.
MVCC stores multiple committed versions of the same user key, each tagged with a
monotonic commit timestamp. Readers choose a stable read timestamp and only see
versions committed at or before that timestamp, while writers create newer
versions without overwriting older ones immediately.

The design provides:

1. Snapshot reads for `get` and `scan`.
2. Atomic write batches with one commit timestamp per batch.
3. Optional point-key serializable optimistic transactions through an explicit
   isolation-level setting.
4. Safe garbage collection of old versions using an active-reader watermark.
5. Compatibility with WAL, compaction, block cache, and value-log separation.

---

## 2. Motivation

The current engine is effectively single-version:

1. `MemTable` maps `user_key -> value`.
2. SSTs contain one visible value per key after iterator merge.
3. Deletes are represented as empty values.
4. `KvEngine::new_txn()` and the `mvcc` module are mostly placeholders.
5. `key.rs` has compatibility helpers marked for removal once MVCC is enabled.

This is enough for basic LSM behavior, but it creates correctness and API
limitations:

1. A long-range scan can observe a mix of old and new writes.
2. Transaction APIs cannot provide repeatable reads.
3. Point-key serializable transactions need a read/write conflict model.
4. Compaction cannot safely drop overwritten versions without knowing active
   reader timestamps.
5. vLog GC can race with historical readers unless version liveness is tied to
   the MVCC watermark.

MVCC addresses these by making version visibility explicit and by giving
compaction a precise lower bound for reclaiming obsolete versions.

---

## 3. Goals

1. Preserve the existing non-transactional API shape where possible.
2. Make every user write visible at exactly one commit timestamp.
3. Provide snapshot isolation for reads and scans.
4. Provide optional point-key serializable optimistic transactions when
   `LsmStorageOptions::isolation_level` is set to `PointKeySerializable`.
5. Keep foreground reads lock-free apart from short timestamp bookkeeping.
6. Let compaction reclaim obsolete versions once watermark, tombstone, and
   overlap rules prove they are no longer needed.
7. Keep WAL recovery deterministic by preserving versioned keys on disk.

## 4. Non-Goals

1. Distributed transactions.
2. External timestamp assignment by callers.
3. Cross-database atomicity.
4. Predicate-lock based serializability for arbitrary range predicates in the
   first implementation.
5. Retaining unlimited historical versions after they fall below the MVCC
   watermark.

---

## 5. Design Overview

```text
User key:  "user:42"

Internal versions, sorted by user key ascending and timestamp descending:

  user:42 @ ts=105  -> "newest"
  user:42 @ ts=101  -> "older"
  user:42 @ ts=097  -> tombstone

Snapshot read at read_ts=102 sees ts=101.
Snapshot read at read_ts=110 sees ts=105.
Compaction can drop ts=097 once it is below the MVCC watermark and shadowed.
```

The core change is replacing raw user keys in memtables, WALs, SST blocks, and
iterators with byte-order-preserving internal keys. Bloom filters remain
user-key-based so snapshot point lookups do not need to know an exact commit
timestamp:

```text
internal_key = escaped_user_key || terminator || inverted_ts
inverted_ts = u64::MAX - commit_ts, encoded big-endian
```

The timestamp suffix alone does **not** preserve raw byte ordering for prefix
keys such as `a` and `aa`. To keep the existing byte-ordered memtable, block,
and SST search machinery correct, the internal-key encoding must preserve this
logical order under ordinary byte comparison:

1. User keys sort ascending.
2. For the same user key, newer timestamps sort before older timestamps.

Reads seek to `encode_internal_key(user_key, read_ts)` and choose the first
version with the same decoded user key whose commit timestamp is less than or
equal to `read_ts`.
Scans merge all LSM sources using raw byte order over encoded internal keys,
collapse duplicate decoded user keys, and expose only the first visible version
per user key.

---

## 6. Detailed Design

### 6.1 Internal Key Format

Enable timestamped keys by replacing the current `TS_ENABLED: false` placeholder
with a real key layout.

```rust
pub const TS_ENABLED: bool = true;

pub struct Key<T: AsRef<[u8]>>(T);
```

The byte layout is:

```text
[escaped_user_key][0x00 0x00 terminator][inverted_ts: u64 big-endian]
```

Escaping rules:

1. A non-zero user-key byte `b` is encoded as `b`.
2. A zero byte `0x00` in the user key is encoded as `0x00 0xff`.
3. The end of the user key is encoded as `0x00 0x00`.
4. The timestamp suffix follows the terminator.

This preserves raw user-key lexicographic order under ordinary byte comparison:
the terminator sorts before an escaped embedded zero, and both sort before any
non-zero byte after a shared prefix. For equal user keys, `inverted_ts` makes
newer versions sort first. The implementation can therefore keep
`SkipMap<Bytes, _>` and existing block/SST binary search data structures, but
all user-key extraction, bound construction, bloom hashing, and deduplication
must decode the escaped user-key portion.

Required helpers:

```rust
impl<T: AsRef<[u8]>> Key<T> {
    pub fn decode_user_key(&self) -> Cow<'_, [u8]>;
    pub fn decode_user_key_into(&self, scratch: &mut Vec<u8>);
    pub fn encoded_user_key(&self) -> &[u8];
    pub fn ts(&self) -> u64;
    pub fn raw_ref(&self) -> &[u8];
}

impl Key<Vec<u8>> {
    pub fn from_user_key_ts(user_key: &[u8], ts: u64) -> Self;
    pub fn set_from_user_key_ts(&mut self, user_key: &[u8], ts: u64);
}
```

The current `for_testing_*_no_ts` helpers should stay during migration, but
production code should stop using no-timestamp constructors once MVCC is
enabled.

All physical seek and range paths must operate on encoded internal keys:

1. Physical `MemTable` storage may continue to use `SkipMap<Bytes, _>` because
   the encoding is byte-order preserving.
2. `BlockIterator` binary search, `SsTable::find_block_idx`, `point_get`,
   concat iterator table selection, and SST `range_overlap` must compare
   encoded internal keys or decoded user-key bounds consistently.
3. Any remaining comparison of raw unescaped user keys against encoded internal
   keys is a correctness bug and should be covered by prefix-key tests.

Public APIs accept raw user keys. Physical memtable/SST/block APIs accept
encoded internal keys. The implementation should make this boundary explicit
with distinct wrappers such as `UserKeySlice` and `InternalKeySlice`, or with
helper names that include `user_key` vs `internal_key`. Reusing
`KeySlice::from_slice` for both roles is no longer acceptable because it can
silently pass raw user bounds into encoded-key structures.

The logical user key is not always borrowable from an escaped internal key:
embedded `0x00` bytes must be decoded from `0x00 0xff`. Helpers should either
return `Cow<[u8]>`, decode into caller-provided scratch, or operate on the
encoded user-key prefix when only ordering/equality is needed.

Encoded internal key length must be validated before writing to any format that
stores key lengths as `u16` or otherwise has fixed-width lengths. The effective
limit is on encoded internal key bytes, not raw user-key bytes. The MVP keeps
the current fixed-width fields and fails before persistence with typed errors
when an encoded key, value, offset, or metadata entry does not fit. Any widening
requires a later format-versioned RFC.

Important current limits and validation points:

| Format area | Current constraint | MVCC requirement |
| --- | --- | --- |
| WAL records | key/value lengths are stored as `u16` | Validate encoded key and tagged value length before WAL append. |
| Block entries | key length, prefix-overlap, suffix length, value length, and entry offsets use `u16` | Validate each encoded key/value and block offset before adding to `BlockBuilder`. |
| Block metadata | per-block metadata offsets, first/last key lengths, block offsets, and metadata offset-vector entries use fixed-width fields or direct casts | Validate every encoded first/last key length, block offset, metadata-entry offset, and metadata offset-vector entry before persisting. |
| SST footer/offsets | metadata and bloom offsets are fixed-width fields | Validate metadata and bloom offsets before writing the footer. |
| vLog entries | key lengths are fixed-width in the current entry format | Store and validate full encoded internal keys. |
| vLog index | key/index entry lengths inherit current fixed-width assumptions | Validate encoded internal key length before index insert and persistence. |

### 6.2 Timestamp Allocation

`LsmMvccInner` owns the global timestamp state:

```rust
pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Mutex<(u64, Watermark)>,
    pub(crate) committed_txns: Mutex<BTreeMap<u64, CommittedTxnData>>,
}
```

Rules:

1. `latest_commit_ts()` returns the highest published commit timestamp only.
   Reserved or merely WAL-durable timestamps are not visible to foreground
   readers.
2. A read transaction gets its timestamp through `ReadGuard::new_latest`, which
   reads `latest_commit_ts()` and registers that timestamp atomically.
3. A write commit increments the global timestamp under `commit_lock`.
4. All records in one `write_batch` share the same `commit_ts`.
5. On recovery, the engine initializes the timestamp counter to the highest
   commit timestamp found in WALs and SSTs.

Before timestamp assignment, each write batch must be canonicalized by raw user
key. If the same user key appears multiple times in one batch or transaction
commit, the last operation wins and only one internal key is written for that
user key at the batch `commit_ts`. The MVCC API does not reject duplicates as a
normal conflict policy, and it must never silently write multiple records with
the same `(user_key, commit_ts)`.

Commit timestamp assignment has two states:

1. **Reserved:** a writer has allocated `commit_ts`, but readers must not use it
   yet.
2. **Published:** all WAL records are durable, all memtable entries for the
   batch are visible, and `latest_commit_ts` has advanced.

The required commit order is:

1. Acquire `commit_lock`.
2. Reserve `commit_ts = latest_commit_ts + 1` without publishing it.
3. Append and sync one atomic WAL batch record when WAL is enabled.
4. Insert all versioned records into the active memtable while the memtable
   cannot be frozen.
5. Publish `latest_commit_ts = commit_ts`.
6. Release `commit_lock`.

This prevents a reader from choosing a timestamp whose batch is only partially
installed.

The `write_lock` is used only for APIs that need single-writer semantics during
the first implementation. The long-term target is to keep independent writes
concurrent and serialize only commit timestamp assignment plus memtable append.

### 6.3 Watermark

`Watermark` tracks active read timestamps:

```rust
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}
```

Semantics:

1. `add_reader(ts)` increments the active reader count for `ts`.
2. `remove_reader(ts)` decrements the count and removes the entry at zero.
3. `watermark()` returns the smallest active read timestamp.
4. If there are no active readers, `watermark()` returns `None`.
5. `LsmMvccInner::watermark()` returns the smallest active timestamp, or the
   latest commit timestamp when no readers are active.

Versions with `commit_ts < watermark` are candidates for reclamation, not
automatically invisible garbage. For each user key, compaction must keep the
newest version below the watermark when that version could be the state observed
by the oldest active reader, and must also preserve tombstones or older versions
when overlap rules require them.

All snapshot readers need a guard, not only explicit transactions:

```rust
pub(crate) struct ReadGuard {
    read_ts: u64,
    mvcc: Arc<LsmMvccInner>,
}
```

`LsmStorageInner` should store `mvcc: Option<Arc<LsmMvccInner>>` so guards and
iterators can own the MVCC handle independently of borrowed storage state.
`ReadGuard::new_latest` must acquire the MVCC timestamp mutex, read the
published latest timestamp, register that timestamp in the watermark, and only
then release the mutex. Splitting timestamp acquisition and registration would
allow compaction to observe a watermark that excludes the new reader. `Drop`
unregisters the timestamp. `Transaction` stores one guard for the transaction
lifetime. `scan` stores one guard inside the returned iterator wrapper. Point
`get` may use a short-lived guard around lookup and value resolution.

### 6.4 Non-Transactional Reads

`KvEngine::get(key)` should become a snapshot read at the latest committed
timestamp:

```text
guard = ReadGuard::new_latest(mvcc)
lookup(key, guard.read_ts())
```

The lookup path changes from exact raw-key lookup to version lookup:

1. Build seek key `encode_internal_key(user_key, read_ts)`.
2. Check active memtable, immutable memtables, L0 SSTs, and lower levels.
3. For each source, return the first entry with matching `user_key`.
4. Merge candidates by internal key order; the first visible candidate is the
   latest version at or below `read_ts`.
5. A tombstone returns `None`.

Because internal keys include timestamps, bloom filters should hash only
`user_key` for point lookups. This avoids false negatives when looking up
`user_key` at a read timestamp that does not exactly match a committed version.

### 6.5 Snapshot Scans

`scan(lower, upper)` uses one `ReadGuard` and one `read_ts` for the whole
iterator lifetime. It must not observe writes committed after iterator creation.

The scan flow is:

1. Seek broadly using internal-key bounds, then apply user-key bound filtering
   in the MVCC iterator.
2. Merge memtable, immutable memtables, L0, and lower-level SST iterators.
3. Skip entries with `commit_ts > read_ts`.
4. Collapse duplicate user keys by yielding only the newest visible version.
5. Suppress tombstones.

Bounds are interpreted on user keys, not internal keys:

1. `Included(lower)`: start at the first internal key whose user key is
   `>= lower`.
2. `Excluded(lower)`: start at the first internal key whose user key is
   `> lower`.
3. `Included(upper)`: stop after yielding versions whose user key is `<= upper`.
4. `Excluded(upper)`: stop before yielding any version whose user key is
   `>= upper`.
5. `Unbounded`: no user-key-side restriction.

The implementation can use approximate internal seeks for efficiency, but the
iterator must enforce these user-key predicates after decoding each internal
key. This avoids accidentally including historical versions of an excluded
bound key.

`FusedIterator<LsmIterator>` can remain the public iterator wrapper, but
`LsmIterator` must become MVCC-aware: comparisons for deduplication use
`user_key`, while source ordering still uses full internal keys.

### 6.6 Writes and Tombstones

All writes are stored as internal keys with a commit timestamp:

```text
put("k", "v")    -> ("k" @ commit_ts) = "v"
delete("k")      -> ("k" @ commit_ts) = tombstone
write_batch(...) -> every record uses the same commit_ts
```

MVCC should add an explicit tombstone representation instead of relying on empty
values:

```rust
pub enum WriteOp {
    Put(Bytes),
    Delete,
}

pub enum KvKind {
    Inline = 0,
    ValuePointer = 1,
    Tombstone = 2,
}
```

The current empty-value tombstone encoding is ambiguous because `put(k, b"")`
and `delete(k)` encode identically, especially in vLog mode where
`[KvKind::Inline]` with no payload is already treated as a delete. Transaction
local storage should store `WriteOp`, and persisted values should use
`KvKind::Tombstone` once MVCC is enabled.

### 6.7 Transactions

`KvEngine::new_txn()` should return `Result<Arc<Transaction>>` instead of
`Result<()>`.
Each transaction has:

```rust
pub struct Transaction {
    read_ts: u64,
    read_guard: ReadGuard,
    inner: Arc<LsmStorageInner>,
    /// Raw user-key ordered local writes, not encoded internal keys.
    /// Wrapped in `Arc` so `TxnIterator` can hold an independent reference
    /// for merging local writes with the LSM snapshot during `scan`.
    local_storage: Arc<SkipMap<Bytes, WriteOp>>,
    committed: Arc<AtomicBool>,
    key_sets: Option<Mutex<(HashSet<Bytes>, HashSet<Bytes>)>>,
}
```

Transaction behavior:

1. `get` checks `local_storage` first, then reads the LSM at `read_ts`.
2. `put` and `delete` write uncommitted raw user-key entries into
   `local_storage`. Multiple writes to the same local key collapse with
   last-op-wins semantics before commit.
3. `scan` merges `local_storage` with the stable LSM snapshot at `read_ts`.
   The transaction-local merge layer emits decoded user keys plus explicit
   `WriteOp`/value-kind information; it must not rely on
   `StorageIterator::value() -> &[u8]` to represent tombstones.
4. `commit` uses the Section 6.2 commit protocol: reserve `commit_ts`, rewrite
   local entries as internal keys, encode `WriteOp::Delete` as
   `KvKind::Tombstone`, append/sync the WAL batch when enabled, insert all
   entries while the active memtable cannot be frozen, and publish
   `latest_commit_ts` only after the full batch is visible.
5. `Drop` unregisters `read_ts` via `read_guard` if the transaction has not
   already cleaned it up.

`TxnLocalIterator` should not directly implement the existing byte-value
`StorageIterator` contract for local deletes. Instead, introduce a transaction
merge layer such as:

```rust
pub enum TxnEntry {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}
```

`TxnIterator` then merges `TxnEntry` values with the MVCC LSM iterator by
decoded user key and suppresses `Delete` entries in the public scan output.
Alternatively, extend `StorageIterator` with an explicit value-kind/tombstone
method before using it for transaction-local state.

### 6.8 Isolation Levels and Point-Key Serializable Mode

`LsmStorageOptions::isolation_level` selects the transaction conflict model:

```rust
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableSnapshot,
    PointKeySerializable,
}
```

`RepeatableSnapshot` is the default MVCC transaction level. Transactions provide
repeatable snapshot reads, and write-write conflicts are resolved by version
order: a later commit creates a newer version. This is intentionally
last-writer-wins rather than full snapshot isolation, which would reject
concurrent write-write conflicts. Last-writer-wins is chosen deliberately: it
avoids write-write conflict aborts for the common case of independent key
updates, at the cost of allowing lost updates when two transactions write the
same key concurrently. Callers who need stronger guarantees should use
`PointKeySerializable`.

`ReadCommitted` is reserved for non-transactional API behavior that should read
the latest published timestamp per operation. Each non-transactional `get` or
`scan` registers a short-lived `ReadGuard` (pin latest, read, unpin) so the
watermark is temporarily raised for the duration of the operation, but does not
hold a long-lived snapshot across multiple calls. `PointKeySerializable` adds
point-key optimistic concurrency control. `Transaction` tracks:

1. `read_set`: raw user keys read from the LSM snapshot.
2. `write_set`: raw user keys written by the transaction.

Every point key requested through `get` is added to `read_set` when the
transaction consults the snapshot, including keys that return `None`, keys whose
latest visible version is a tombstone, and keys later shadowed by local writes.
This prevents a concurrent insert after `read_ts` from bypassing point-key
conflict detection.

`scan` also adds each yielded LSM snapshot key to `read_set`. This covers keys
that were actually observed during a range read. It does not cover predicates or
gaps, so inserts into the scanned range after `read_ts` can still be phantoms
until a future range-validation phase exists.

Non-transactional `put`, `delete`, and `write_batch` operations must record
their canonicalized write sets in `committed_txns` when
`PointKeySerializable` is enabled. Otherwise, a point-key serializable
transaction that read key `k` before a normal `put(k)` could commit without
seeing the conflict. These non-transactional write paths should also prune
`committed_txns` entries older than `mvcc.watermark()` under `commit_lock` to
prevent unbounded memory growth in workloads dominated by non-transactional
writes with infrequent transaction commits.

Transactions have an atomic completion flag. `put`, `delete`, and `commit`
first check that the transaction is still open. `commit` changes the flag from
open to committed before acquiring `commit_lock`; a second `commit` or any later
mutation returns an error and cannot append WAL records or memtable entries.
Because `Transaction` is shared through `Arc` and local writes go into a
concurrent `SkipMap`, the close-and-drain step in `commit` must be atomic with
respect to `put`/`delete` — e.g., via a transaction-state mutex or `RwLock` that
makes the check-open-and-insert indivisible with commit's close-and-drain.
`abort`/`Drop` releases the read guard for transactions that did not commit, and
successful commit releases the read guard after the commit path finishes.

Read-only transactions (empty `write_set`) bypass the write-related commit steps:
they release their `ReadGuard` and return success immediately without acquiring
`commit_lock`, allocating a `commit_ts`, or appending to the WAL.

On commit:

1. Atomically close the transaction for commit.
2. Acquire `commit_lock`.
3. Remove committed transaction records older than `mvcc.watermark()`.
4. Check committed transactions with `commit_ts > read_ts`.
5. Abort if any committed transaction's `write_set` intersects this
   transaction's `read_set`.
6. Reserve `commit_ts = latest_commit_ts + 1` without publishing it.
7. Append and sync one atomic WAL batch record when WAL is enabled.
8. Insert all versioned records while the memtable cannot be frozen.
9. Record this transaction's write set in `committed_txns`.
10. Publish `latest_commit_ts = commit_ts`.
11. Release the read guard.
12. Release `commit_lock`.

This is optimistic concurrency control. It prevents stale-read anomalies for
point-key conflicts. It is not full predicate serializability: range reads can
miss phantoms inserted after `read_ts` unless a later phase adds range predicate
tracking or range validation. The public option should not be named simply
`serializable` because the initial guarantee is point-key serializability.

### 6.9 WAL and Recovery

WAL records should store internal keys exactly as they appear in the memtable.
This keeps recovery simple:

1. Recover memtables from WAL into `SkipMap<encoded_internal_key, value>`.
2. Recover SST metadata as today.
3. Recover the maximum commit timestamp from complete WAL batches and persisted
   SST `max_ts` metadata.
4. Initialize `LsmMvccInner::new(max_commit_ts)`.

WAL writes must be batch-framed. A committed batch should be encoded as a single
logical record containing `commit_ts`, entry count, entry lengths, payloads, and
a checksum, or as data records followed by a checksum-protected commit marker.
Recovery must replay either the whole batch or none of it. A truncated or
checksum-invalid batch is ignored or causes recovery to truncate the WAL at the
last valid batch boundary. Replaying a prefix of a batch with one commit
timestamp is not allowed.

Crash semantics are durable-batch based: if the engine syncs a complete WAL
batch and then crashes before publishing `latest_commit_ts` to live readers,
recovery treats that complete batch as committed. This is the usual
durable-before-ack window: the client may not have observed success, but the
recovered database may include the write. The MVP does not add a separate
durable publish marker.

The WAL already stores opaque key bytes, but mixed pre-MVCC and MVCC WALs are
not distinguishable by the WAL record alone. MVCC therefore needs a manifest or
directory format marker. Startup should reject mixed formats unless an explicit
migration maps existing raw keys to timestamp zero.

The persisted value format also needs this format marker. MVCC values should be
kind-tagged in both vLog and non-vLog modes so `KvKind::Tombstone` is
unambiguous. Migration rules must be storage-location aware: current non-vLog
WAL/memtable values are raw bytes, while flushed SST values produced through
`SsTableBuilder::add` may already be `KvKind::Inline`-prefixed, and vLog mode is
kind-prefixed in more paths. The MVCC format marker must select the correct
decoder for WAL recovery, SST reads, memtable values, and vLog pointer values.
`KvKind::from_u8` and every parser must recognize `Tombstone = 2`.

MVCC tombstones are encoded as exactly one byte: `[KvKind::Tombstone]`.
Tombstone payload bytes are invalid. `[KvKind::Inline]` with an empty payload is
a valid empty value in MVCC; only `[KvKind::Tombstone]` marks a delete.

#### 6.9.1 Migration Strategy

MVCC startup detects the directory format through manifest and file-format
markers. A pre-MVCC directory is rejected by default unless the caller runs an
explicit migration tool or opens with an explicit migration option.

The migration maps every existing raw user key to timestamp zero, rewrites
values into the MVCC kind-tagged format, and emits format-versioned WAL, SST,
vLog, and `.vidx` files. For safety, migration writes into a temporary
directory or manifest generation, fsyncs all new files, then atomically publishes
the new manifest. Failure leaves the original directory readable by the old
format. Rollback is the original directory plus the pre-migration manifest.

Mixed-format directories are not opened for normal reads or writes. They are
accepted only by the migration tool, which validates every file and either
finishes publication or leaves the old manifest active.

SST recovery must persist `max_ts` in a format-versioned SST footer or table
metadata and load it in `SsTable::open`. The full key scan is only a fallback
for pre-MVCC migration, explicit repair, or corrupt/missing metadata handling;
it is not the normal MVCC startup path. Current block metadata stores first and
last keys only; because internal-key ordering is user-key-first, the maximum
timestamp can appear in the middle of a block or table. Metadata-only recovery
therefore requires adding per-table `max_ts` metadata.

### 6.10 SST Metadata and Bloom Filters

`SsTable` already has a `max_ts` placeholder. MVCC should populate:

1. `first_key`: first internal key in the SST.
2. `last_key`: last internal key in the SST.
3. `max_ts`: highest commit timestamp in the SST.
4. `min_ts`: lowest commit timestamp in the SST.
5. Bloom filter entries: user-key hashes, not full internal-key hashes.

Persisting `min_ts` allows older snapshot readers (`read_ts < sst.min_ts`) to
skip the entire SST during point lookups and scans, avoiding unnecessary I/O
for historical snapshots.

Range overlap must compare user-key portions of bounds. Point-get bloom checks
must also use decoded user-key hashes. Memtable incremental bloom filters must
make the same change; hashing the full internal key would create false
negatives for lookups at read timestamps that do not exactly match a committed
version.

### 6.11 Compaction and Version GC

Compaction becomes responsible for dropping obsolete versions, but only when the
compaction input proves that no older version outside the input can be
resurrected. This is safe for bottommost/full-overlap compactions, or for tasks
that explicitly expand overlaps to include every older version for the affected
user-key range.

For each user key in a safe compaction scope, while iterating versions from
newest to oldest:

1. Keep every version with `commit_ts > watermark`.
2. Also keep the newest version with `commit_ts <= watermark`.
3. Drop older versions below that retained boundary version.
4. Preserve tombstones until they are below the watermark and no older version
   needs to be hidden.

Example with `watermark = 100`:

```text
k@120 -> keep, active readers may see it
k@105 -> keep, active readers may see it
k@090 -> keep, newest version <= watermark
k@080 -> drop, hidden for all current and future readers
```

If the newest below-watermark version is a tombstone, the tombstone can be
dropped only when the compaction scope proves that all older versions for the
key are also being dropped or do not exist. This prevents deleted keys from
reappearing from an older SST outside the compaction input.

### 6.12 vLog Interaction

MVCC and vLog can coexist because value pointers are stored as values under
versioned internal keys.

Required rules:

1. vLog entry key validation must use the full internal key, not only the user
   key, if different versions can point to different vLog entries.
2. vLog reference tracking remains SST-based, but compaction version GC can
   remove old pointer-bearing entries from new SSTs.
3. vLog file reclamation remains safe only after SST references, memtable
   references, active reader guards, and physical `Arc<SsTable>` snapshots that
   may still contain the pointer are gone.
4. A value pointer below the MVCC watermark may still be live if it is the
   newest below-watermark version kept by compaction.

The implementation can satisfy this by tying vLog file deletion to both MVCC
watermark rules and a physical table-lifetime/epoch mechanism. Current manifest
references alone are insufficient because old iterators can hold `Arc<SsTable>`
after a compaction installs a newer state.

vLog GC must be MVCC-version aware. Current-style key-current-state CAS is not
sufficient because a historical value pointer belongs to one exact encoded
internal key. The primary strategy is to rewrite value pointers while building
compaction output, replacing the value for the exact internal key that survives
version GC. This keeps pointer movement coordinated with SST version selection
and avoids inventing a new latest version just to move an old value.

**Tradeoff:** Rewriting value pointers during compaction reintroduces value
write amplification — the core cost that WiscKey-style key/value separation was
designed to avoid. This is acceptable when only live versions are rewritten (dead
versions are dropped, not copied), and when large values dominate the vLog. If
write amplification becomes a bottleneck, the independent background vLog GC
(next paragraph) should be prioritized so that compaction only rewrites pointers
for versions it is already touching, while background GC handles cold vLog files
separately.

An independent background vLog GC may be added later, but it must use an
internal-version CAS API keyed by encoded internal key and expected
`ValuePointer`, never by raw user key alone. GC must not create a new latest
version just to move an old value, and it must not rewrite a pointer unless the
exact internal-key version remains live.

### 6.13 Cache Interaction

Block cache keys remain `(sst_id, block_idx)`. MVCC does not require cache API
changes.

Backfill behavior remains valid:

1. Flush backfills blocks containing versioned internal keys.
2. Compaction backfills blocks after MVCC version GC.
3. Old blocks are invalidated when their SSTs are removed.

The value cache used by vLog must include enough identity to distinguish
different versions. Caching by `ValuePointer` is sufficient because pointers are
unique physical locations.

---

## 7. API Changes

Proposed public API:

```rust
impl KvEngine {
    pub fn new_txn(&self) -> Result<Arc<Transaction>>;
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator>;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;
    pub fn commit(&self) -> Result<()>;
}
```

`Result<Arc<Transaction>>` keeps the outer API compatible with fallible engine
methods and lets the implementation return an error if MVCC is unavailable
during migration.

---

## 8. Implementation Plan

### Phase 1: Timestamped Keys

1. Implement escaped internal key helpers in `key.rs`.
2. Remove production use of no-timestamp constructors.
3. Update memtable, block iterator, SST seek, range overlap, and test helpers
   to use encoded internal keys and decoded user-key bounds correctly.
4. Keep `TS_ENABLED` guarded tests to ease migration.

### Phase 2: Format Hardening

1. Add format markers for MVCC WAL, SST, value, vLog, and `.vidx` decoding.
2. Replace unchecked fixed-width casts with checked conversions and error
   propagation in block metadata, SST footer offsets, WAL records, vLog entries,
   and `.vidx` persistence.
3. Implement WAL batch framing, checksum validation, truncation behavior, and
   max-timestamp extraction from complete batches only.
4. Persist and recover SST `max_ts` from format-versioned table metadata.

### Phase 3: Migration and Compatibility

1. Add manifest and file-format detection for pre-MVCC and MVCC directories.
2. Reject mixed-format directories during normal startup.
3. Implement an explicit migration path that maps raw keys to timestamp zero,
   rewrites kind-tagged values, and publishes through a temporary manifest or
   directory.
4. Add rollback and failure-injection tests for interrupted migration.

### Phase 4: MVCC State and Watermark

1. Implement `Watermark`.
2. Initialize `mvcc: Some(Arc<LsmMvccInner>)` in `LsmStorageInner::open`.
3. Recover max timestamp from complete WAL batches and persisted SST `max_ts`.
4. Add atomic `ReadGuard::new_latest` registration and cleanup.

### Phase 5: Versioned Writes and Reads

1. Add `KvKind::Tombstone` and update `KvKind::from_u8`, value parsers, SST
   builder `add`/`add_raw` paths, compaction tombstone checks, vLog
   reader/index logic, and compatibility handling.
2. Canonicalize duplicate user keys in `put`, `delete`, transaction commits,
   and `write_batch`.
3. Assign commit timestamps and store internal keys in memtables and WALs.
4. Implement version-aware `get`.
5. Hash SST and memtable bloom entries by user key.

### Phase 6: Snapshot Scans

1. Make `LsmIterator` collapse duplicate user keys.
2. Make memtable and SST range bounds timestamp-aware.
3. Add scan tests for concurrent writes during iteration.

### Phase 7: Transactions

1. Implement `LsmMvccInner::new_txn`.
2. Implement `Transaction::{get, scan, put, delete, commit}`.
3. Implement the `TxnEntry`/transaction merge layer or extend
   `StorageIterator` with explicit value-kind support.
4. Add repeatable snapshot read tests.

### Phase 8: Point-Key Serializable OCC

1. Replace the current `HashSet<u32>` sketches with read and write user-key
   sets such as `HashSet<Bytes>`.
2. Implement committed transaction pruning by watermark.
3. Record non-transactional write batches in `committed_txns`.
4. Record point reads, negative reads, tombstone reads, and yielded scan keys in
   `read_set`.
5. Detect read/write conflicts at commit.
6. Add conflict, no-conflict, double-commit, and mutation-after-commit tests.

### Phase 9: Compaction GC

1. Populate SST `max_ts`.
2. Add watermark-aware version dropping in compaction builders.
3. Preserve tombstone semantics.
4. Add tests for old-version reclamation.

### Phase 10: vLog Integration

1. Validate vLog entries against full internal keys.
2. Make compaction-output vLog GC rewrite exact live internal-key versions.
3. Keep internal-version CAS only for a later independent background GC path.
4. Verify vLog GC with multiple versions of the same user key.
5. Add tests covering pointer-bearing historical versions.

---

## 9. Testing Plan

Required tests:

1. Internal key ordering: same user key sorts newest timestamp first.
2. `get` returns the newest version at or below the read timestamp.
3. `delete` hides older versions for newer snapshots.
4. `scan` yields one visible version per user key.
5. Long-running scan does not observe concurrent writes.
6. WAL recovery restores versioned keys and max timestamp.
7. Snapshot transaction reads are repeatable.
8. Transaction local writes shadow snapshot state.
9. Point-key serializable transaction aborts on read/write conflict.
10. Point-key serializable transaction commits when write sets do not conflict.
11. Compaction keeps versions with `commit_ts > watermark`.
12. Compaction keeps the newest version with `commit_ts <= watermark`.
13. Compaction does not resurrect deleted keys.
14. vLog values remain readable across multiple versions.
15. vLog GC does not remove a pointer still visible to an old snapshot.
16. Prefix user keys such as `a`, `a\x00`, and `aa` sort and seek correctly
    across memtables, block binary search, SST point lookup, and range scans.
17. WAL recovery ignores or truncates incomplete MVCC batch records and never
    exposes a partial one-timestamp batch.
18. WAL recovery follows the documented crash contract for a complete synced
    batch that was not published before crash.
19. Escaped user keys containing multiple `0x00` bytes decode back to the exact
    original user key and deduplicate correctly during scans.
20. Bloom filters hash decoded user keys consistently across memtable and SST
    lookups.
21. Keys whose encoded internal length exceeds the active format limit are
    rejected before WAL, SST, vLog, or vLog-index writes.
22. Duplicate user keys in one batch or transaction commit are canonicalized
    with last-op-wins behavior.
23. vLog index entries use full encoded internal keys and GC rewrites only exact
    live internal-key versions, including embedded-zero keys and oversized-key
    rejection.
24. Point-key serializable OCC records negative point reads: if one transaction
    reads an absent or tombstoned key and another transaction inserts that key
    after `read_ts`, the first transaction aborts on commit.
25. MVCC tombstone parser tests cover valid `[KvKind::Tombstone]` (single byte,
    no payload), invalid tombstone payloads, and `[KvKind::Inline]` with empty
    payload as a valid empty value under MVCC decoding.
26. `scan` records yielded LSM snapshot keys in the `PointKeySerializable`
    `read_set`.
27. Non-transactional `put`, `delete`, and `write_batch` operations conflict
    with overlapping point-key serializable transactions.
28. Transaction `commit` is single-use, and `put`/`delete` after commit are
    rejected before any WAL or memtable mutation.
29. Pre-MVCC migration maps raw keys to timestamp zero, rewrites kind-tagged
    values, rejects mixed-format startup outside migration, and rolls back after
    an interrupted migration.
30. SST `max_ts` persists in format-versioned metadata and is recovered without
    scanning keys during normal startup.

Recommended commands:

```bash
cargo nextest run --workspace --all-features --lib
cargo nextest run --workspace --all-features --all-targets
cargo make check
```

---

## 10. Risks and Tradeoffs

### 10.1 Key Size Overhead

Every internal key gains at least 10 bytes: a 2-byte escaped-key terminator plus
an 8-byte timestamp. Embedded `0x00` user-key bytes expand to `0x00 0xff`, so
the overhead can be larger. Encoded-size validation must use the expanded
internal key length. This increases memtable memory, SST block size, bloom input
processing, and cache footprint. The tradeoff is necessary for versioned reads
and simple compaction ordering.

### 10.2 Iterator Complexity

Scans must deduplicate by user key while ordering by internal key. This is the
largest correctness risk. Tests should heavily cover boundary conditions,
tombstones, and mixed memtable/SST versions.

### 10.3 Bloom Filter Semantics

Hashing full internal keys would make point lookups incorrect because callers do
not know the exact commit timestamp. Bloom filters must hash user keys only.

### 10.4 Serializable Granularity

Point-key set tracking is lightweight but not full predicate serializability.
The first implementation records point keys and yielded scan keys only, and
documents that range predicates are not serializable. Preventing range phantoms
requires a later predicate/range validation phase.

### 10.5 vLog Key Validation

If vLog entries continue validating only user keys, stale pointers from older
versions can appear valid for newer versions of the same key. Using full
internal keys avoids this ambiguity.

### 10.6 Watermark Lag from Long-Running Transactions

A long-running read transaction (e.g., a large scan or a backup) pins the MVCC
watermark to its `read_ts`. This prevents compaction from garbage collecting old
versions newer than the pinned watermark and prevents pruning of `committed_txns`
newer than the pinned watermark. The result is disk space amplification (obsolete
versions accumulate) and memory growth (unpruned `committed_txns` metadata).

Mitigations to consider during implementation:

1. **Transaction timeouts** — abort or warn if a transaction holds a `ReadGuard`
   beyond a configurable duration.
2. **Maximum watermark age** — periodically check whether the oldest `ReadGuard`
   exceeds a threshold and log or alert.
3. **Background GC advisory** — let compaction report the gap between the
   watermark and `latest_commit_ts` so operators can detect lag.

---

## 11. Open Questions

1. Should a future `IsolationLevel::Serializable` add predicate/range
   validation for full serializability, and what range-lock representation
   should it use?

---

## 12. Success Criteria

The MVCC implementation is complete when:

1. `TS_ENABLED` is true and production paths use timestamped keys.
2. `KvEngine::get` and `scan` are snapshot-consistent.
3. `Transaction` supports repeatable reads and atomic commits.
4. `PointKeySerializable` detects point-key read/write conflicts.
5. WAL recovery preserves versions and resumes timestamp allocation safely.
6. Compaction reclaims obsolete versions without breaking active snapshots or
   resurrecting older data.
7. vLog GC remains safe with historical versions.
8. `cargo make check` passes.
