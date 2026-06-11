# RFC 006: Prefix Search

**Status:** Proposed  
**Date:** 2026-06-11  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding first-class prefix search to kv-engine. Prefix search
returns all visible keys whose user key starts with a caller-provided byte
prefix, in sorted key order.

The initial design exposes prefix search as a small API layer over the existing
range-scan machinery:

```rust
let iter = engine.prefix_scan(b"user:")?;
```

Internally, the engine converts the prefix into a lexicographic range:

```text
[prefix, next_prefix(prefix))
```

where `next_prefix` is the smallest byte string that sorts strictly after every
key with the requested prefix. This preserves compatibility with memtables,
SSTables, MVCC snapshot scans, value-log resolution, and existing iterator
composition.

---

## 2. Motivation

Many key-value workloads group related records by key prefix:

1. `user:{id}:profile`, `user:{id}:settings`, `user:{id}:session`
2. `tenant:{tenant_id}:...`
3. `table:{table_id}:row:{row_id}`
4. `index:{name}:{secondary_key}:{primary_key}`

The engine already supports range scans, so callers can implement prefix search
manually. However, requiring every caller to compute prefix upper bounds has
several problems:

1. It is easy to get the byte-boundary rules wrong, especially for prefixes
   ending in `0xff`.
2. It duplicates low-level key-ordering logic outside the engine.
3. It hides an important storage access pattern from the API, making future
   prefix-aware optimizations harder to introduce.
4. It complicates the CLI, examples, and tests for a common operation.

Prefix search should be a native operation with precise byte semantics and the
same snapshot guarantees as `scan`.

---

## 3. Goals

1. Add `prefix_scan(prefix)` to the public engine API.
2. Add `prefix_scan(prefix)` to the transaction API for snapshot reads.
3. Define exact byte-prefix semantics for arbitrary binary keys.
4. Reuse existing range-scan iterators for the MVP.
5. Preserve MVCC visibility rules and duplicate-version collapse.
6. Avoid new on-disk formats in the first implementation.
7. Provide focused tests for empty prefixes, ordinary prefixes, and prefixes
   containing `0xff`.

## 4. Non-Goals

1. Full-text search, substring search, suffix search, or pattern matching.
2. Secondary indexes beyond prefix-encoded keys already chosen by callers.
3. Trie-based memtables or new SST prefix indexes in the MVP.
4. Prefix bloom filters in the MVP.
5. Serializable transaction range-predicate tracking in the MVP.

---

## 5. Design Overview

Prefix search is implemented by mapping a prefix to an equivalent range:

```text
prefix = b"user:"
lower  = Included(b"user:")
upper  = Excluded(b"user;")
```

For binary keys, the upper bound is computed by incrementing the last byte that
is not `0xff` and truncating after that byte:

```text
prefix              upper bound
------              -----------
[]                  Unbounded
[0x61]              [0x62]
[0x61, 0x62]        [0x61, 0x63]
[0x61, 0xff]        [0x62]
[0xff]              Unbounded
[0xff, 0xff]        Unbounded
```

The scan range is:

```text
lower = Included(prefix)
upper = Excluded(next_prefix(prefix)), or Unbounded if no successor exists
```

When the prefix is empty, prefix search is equivalent to a full scan.

This design intentionally uses user-key bounds. Existing MVCC scan code is
responsible for translating user-key bounds into encoded internal-key bounds,
including the timestamp suffix and memcomparable user-key encoding.

---

## 6. Detailed Design

### 6.1 Prefix Bound Helper

Add a small helper that computes the exclusive upper user-key bound:

```rust
pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.last_mut() {
        if *last != 0xff {
            *last += 1;
            return Some(upper);
        }
        upper.pop();
    }
    None
}
```

`None` means there is no finite byte string that sorts after every key with the
prefix and before every key outside that prefix. This happens for the empty
prefix and for prefixes made entirely of `0xff` bytes.

Callers must special-case the empty prefix as a full scan before calling this
helper, or treat `None` with an empty prefix as full-scan intent. For all-`0xff`
prefixes, `[prefix, Unbounded)` is still an exact prefix range: no non-matching
key can sort after the prefix except extensions that also start with that
prefix.

The helper operates on raw user keys, not encoded internal keys. This keeps it
independent from MVCC internals and usable by public API code, transaction code,
tests, and the CLI.

### 6.2 Public Engine API

Add a convenience method next to `scan`:

```rust
impl KvEngine {
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<ScanIterator> {
        if prefix.is_empty() {
            return self.scan(Bound::Unbounded, Bound::Unbounded);
        }
        match prefix_upper_bound(prefix) {
            Some(upper) => self.scan(Bound::Included(prefix), Bound::Excluded(&upper)),
            None => self.scan(Bound::Included(prefix), Bound::Unbounded),
        }
    }
}
```

The method returns the same iterator type as `scan`. It should not allocate or
materialize results beyond the upper-bound vector.

For an empty prefix, the implementation may call:

```rust
self.scan(Bound::Unbounded, Bound::Unbounded)
```

This avoids creating `Included(b"")` for the common "scan all" case and exactly
matches existing full-scan behavior.

### 6.3 Transaction API

Add the same helper beside `Transaction::scan` in `mvcc/txn.rs`. It must use
the same `self: &Arc<Self>` receiver and return `TxnIterator`, so local writes
continue to be merged through the existing transaction iterator path:

```rust
impl Transaction {
    pub fn prefix_scan(self: &Arc<Self>, prefix: &[u8]) -> Result<TxnIterator> {
        if prefix.is_empty() {
            return self.scan(Bound::Unbounded, Bound::Unbounded);
        }
        match prefix_upper_bound(prefix) {
            Some(upper) => self.scan(Bound::Included(prefix), Bound::Excluded(&upper)),
            None => self.scan(Bound::Included(prefix), Bound::Unbounded),
        }
    }
}
```

The method inherits the current `scan` transaction behavior:

1. Snapshot transactions can use prefix search.
2. Serializable point-key transactions reject prefix search until range
   predicate tracking exists.
3. Local transaction writes are merged with the engine snapshot by the existing
   `TxnIterator`.

This is intentionally conservative. Prefix search is a range predicate and can
observe phantoms under point-key OCC unless the transaction layer tracks the
prefix predicate in the read set.

### 6.4 CLI Support

Extend the REPL with a `prefix <key-prefix>` command:

```text
> prefix user:
user:1 => ...
user:2 => ...
2 keys scanned
```

The CLI MVP should use the same whitespace-delimited string key syntax as
`get`, `del`, and `scan` arguments. Arbitrary binary key input for the CLI is
future work. The command is a convenience command; it does not introduce a new
storage path.

### 6.5 MVCC Interaction

Prefix search must operate on decoded user-key semantics, not raw internal-key
bytes.

With MVCC enabled, physical keys are encoded as:

```text
[memcomparable_user_key][inverted_ts]
```

The prefix bounds are computed on raw user keys first, then passed into the
existing scan path. The scan path encodes those user-key bounds into internal
keys and `LsmIterator` continues to:

1. Skip versions newer than the read timestamp.
2. Collapse multiple versions of the same decoded user key.
3. Stop at the decoded upper user-key bound.
4. Hide tombstones.

This preserves snapshot semantics without adding prefix-specific MVCC logic.

### 6.6 Value-Log Interaction

Prefix search uses the same iterator stack as range scans, so value-log pointer
resolution remains unchanged. Inline values and value-log-backed values should
be returned exactly as they are for `scan`.

No vLog index changes are required for the MVP because prefix search is ordered
by LSM keys, not by value-log layout.

### 6.7 SST and Cache Interaction

The MVP relies on existing SST range overlap checks and iterator seeks. L0 SSTs
are filtered through the same `range_overlap` logic used by `scan`. Sorted lower
levels seek to the lower bound through `SstConcatIterator`, then stop through
the iterator's upper-bound checks.

For prefixes whose upper bound is unbounded, such as `[0xff]`, the logical range
is still exact, but pruning is weaker because there is no finite upper bound to
pass into range-overlap checks. Those prefixes are rare in human-readable
schemas but should still be tested.

Block cache behavior is unchanged. Prefix scans naturally populate the cache
through the normal block-read path.

---

## 7. Correctness Rules

### 7.1 Byte Prefix Semantics

A key matches a prefix if and only if:

```rust
key.starts_with(prefix)
```

No UTF-8, path separator, token, or delimiter semantics are implied. Prefixes
and keys are arbitrary byte strings.

### 7.2 Upper Bound Construction

For non-empty prefixes with at least one byte below `0xff`, the exclusive upper
bound must be the prefix with the last non-`0xff` byte incremented and all
following bytes removed.

For prefixes where every byte is `0xff`, the upper bound is unbounded.

### 7.3 Empty Prefix

An empty prefix matches every key and is equivalent to:

```rust
scan(Bound::Unbounded, Bound::Unbounded)
```

### 7.4 Snapshot Visibility

For MVCC reads, prefix search must return exactly the visible version of each
matching user key at the reader's timestamp. It must not return multiple
versions of the same user key.

### 7.5 Tombstones

Deleted keys are filtered by the existing scan iterator. Prefix search must not
return tombstones as empty values.

---

## 8. Testing Plan

### 8.1 Unit Tests

Add tests for `prefix_upper_bound`:

1. `b"" -> None`, with `prefix_scan` special-casing the empty prefix as a full
   scan before calling the helper.
2. `b"a" -> Some(b"b")`.
3. `b"ab" -> Some(b"ac")`.
4. `b"a\xff" -> Some(b"b")`.
5. `b"\xff" -> None`.
6. `b"\xff\xff" -> None`.

### 8.2 Integration Tests

Add storage tests that write mixed keys and verify sorted prefix results:

```text
a
aa
ab
b
user:1
user:10
user:2
user2:1
```

Required cases:

1. Prefix `b"user:"` returns only `user:1`, `user:10`, and `user:2`.
2. Prefix `b"user"` returns both `user:*` and `user2:*`.
3. Prefix `b"missing"` returns an empty iterator.
4. Empty prefix matches full scan.
5. Prefix ending in `0xff` returns the correct subset.
6. Prefix consisting only of `0xff` returns only matching keys even though the
   physical upper bound is unbounded.

### 8.3 MVCC Tests

Add MVCC-specific tests:

1. A prefix scan at an old read timestamp sees old values.
2. A prefix scan at a newer read timestamp sees newer values.
3. Deleted matching keys are hidden.
4. Multiple versions of the same matching key collapse to one result.
5. Transaction-local writes are merged into `Transaction::prefix_scan`.
6. Serializable transactions reject `prefix_scan` with the same error class as
   `scan`.
7. Keys containing `0x00`, exact 8-byte user keys, and empty user keys preserve
   correct prefix behavior under memcomparable MVCC encoding.

### 8.4 Flush and Compaction Tests

Prefix search should be tested across storage boundaries:

1. Active memtable only.
2. Immutable memtable after freeze.
3. Flushed SST.
4. Multiple L0 SSTs.
5. Leveled or tiered compaction output.
6. Mixed inline and value-log-separated values.

---

## 9. Performance

The MVP has the same asymptotic behavior as a range scan over the equivalent
range:

```text
O(log N + K)
```

where `K` is the number of scanned matching entries plus any entries examined
because the upper bound is unbounded.

Expected performance is good for common structured prefixes because the engine
can seek directly to the lower bound and stop at the computed upper bound.

Future optimizations can build on this API without changing callers:

1. Prefix bloom filters per SST block or per SST.
2. Block-level prefix min/max metadata.
3. Prefix-aware cache admission hints.
4. Optional prefix extractor configuration for schemas with fixed delimiters.
5. CLI and benchmark workloads for prefix-heavy access patterns.

---

## 10. Alternatives Considered

### 10.1 Require Callers to Use `scan`

Callers can already write:

```rust
engine.scan(Bound::Included(prefix), Bound::Excluded(next_prefix))
```

This keeps the engine API smaller, but it leaks byte-bound computation to every
caller and makes it harder to optimize prefix scans later.

### 10.2 Filter a Full Scan

Prefix search could scan all keys and filter with `starts_with`. This is simple
but unacceptable for large keyspaces because it ignores ordering.

### 10.3 Add Prefix Bloom Filters Immediately

Prefix bloom filters may improve negative prefix lookups and sparse prefix
queries, but they require additional metadata choices:

1. Which prefix lengths or extractors are supported.
2. Whether filters are per block or per SST.
3. How false positives interact with arbitrary binary prefixes.
4. How metadata affects SST format compatibility.

The MVP should prove the API and correctness first, then add prefix-aware
metadata in a separate RFC if benchmarks justify it.

---

## 11. Rollout Plan

1. Add `prefix_upper_bound` helper and tests.
2. Add `KvEngine::prefix_scan`.
3. Add `Transaction::prefix_scan`, inheriting existing `scan` restrictions.
4. Add CLI `prefix` command.
5. Add integration tests for memtable, SST, MVCC, and value-log paths.
6. Add a small benchmark that compares manual range scan and `prefix_scan`.
7. Document the API in `kv-engine/README.md`.

---

## 12. Open Questions

1. Should the helper be public for callers that still want to build custom
   range scans, or remain crate-private?
2. Should prefix search expose a future options struct for limit, reverse scan,
   or key-only scans?
3. Should serializable transactions eventually track prefix predicates as range
   reads, and if so should `prefix_scan` become available under that isolation
   level?
4. Should benchmarks include delimiter-style prefixes such as `tenant:123:` and
   binary prefixes containing `0x00` and `0xff`?
