# RFC 007: Prefix Bloom Filters

**Status:** Implemented  
**Date:** 2026-06-12  
**Author:** kv-engine Contributors  
**Tracking Issue:** N/A (design RFC)

---

## 1. Summary

This RFC proposes adding optional prefix bloom filters to SSTables so
`prefix_scan(prefix)` can skip SSTs that cannot contain matching user keys.

RFC 006 added prefix search by translating a prefix into an ordered range:

```text
[prefix, prefix_upper_bound(prefix))
```

That design is correct and simple, but negative or sparse prefix scans can
still open many SSTs whose key ranges overlap the requested range. A prefix
bloom filter provides a cheap probabilistic test before creating an SST
iterator:

```rust
if !sst.may_contain_prefix(prefix) {
    skip_sst();
}
```

The filter never changes query results. It may return false positives, but it
must never return false negatives for prefixes it claims to support.

---

## 2. Motivation

Prefix search is common for structured key schemas:

1. `tenant:{id}:...`
2. `user:{id}:...`
3. `table:{table}:row:{row}`
4. `index:{name}:{secondary}:{primary}`

The current prefix scan path already seeks to the lower bound and stops at the
upper bound. This is efficient once the iterator reaches a matching SST, but
there are still costly cases:

1. A missing tenant prefix overlaps broad SST ranges in L0 or lower levels.
2. A sparse prefix is present in only a few SSTs but range metadata cannot prove
   that most SSTs are irrelevant.
3. Compaction produces large SSTs with broad first/last key ranges, increasing
   the chance that range-overlap checks admit unrelated files.
4. Negative prefix probes are common in multi-tenant systems when callers check
   optional namespaces or secondary indexes.

Existing point-get bloom filters do not help because they hash full user keys.
For a prefix query, the engine does not know the exact keys it will encounter.

---

## 3. Goals

1. Add an SST-level prefix bloom filter that can reject irrelevant SSTs before
   iterator creation.
2. Preserve exact byte-prefix semantics from RFC 006.
3. Keep prefix bloom filters optional and configurable.
4. Avoid false negatives for supported query prefixes.
5. Hash decoded user keys, not MVCC internal keys.
6. Preserve compatibility with existing SST files.
7. Add tests and benchmarks that prove both correctness and useful pruning.

## 4. Non-Goals

1. Replacing range scans with a trie or prefix index.
2. Supporting substring, suffix, regex, or token search.
3. Guaranteeing pruning for every arbitrary prefix length.
4. Per-block prefix bloom filters in the first implementation.
5. Serializable transaction predicate tracking.
6. Changing value-log layout or vLog GC behavior.

---

## 5. Design Overview

Each new SST may contain one or more prefix bloom filters. Each filter is built
for a configured prefix length:

```text
configured lengths: [4, 8]

key: tenant:123:user:1
filter len 4 entry: "tena"
filter len 8 entry: "tenant:1"
```

For a query prefix, the engine uses the longest configured length that is less
than or equal to the query prefix length:

```text
query prefix: "tenant:123:"
chosen filter length: 8
probe bytes: "tenant:1"
```

If the filter says the SST cannot contain that fixed prefix fragment, the SST is
skipped. If no configured length is usable, the engine falls back to the normal
RFC 006 range-scan path.

This rule is safe:

```text
If a key starts with query_prefix, then the key also starts with
query_prefix[..configured_len].
```

Therefore a negative result for the shorter fixed prefix proves that the SST
cannot contain a full match for the longer query prefix.

Filters are built from unique prefix fragments per SST and per configured
length. If an SST contains many keys with the same `tenant:1` prefix fragment,
that fragment is inserted once into the corresponding bloom filter. This keeps
filter sizing tied to prefix cardinality instead of raw key count.

### 5.1 Why Fixed Prefix Lengths

Arbitrary prefix lengths are hard to support without storing every prefix of
every key. That would multiply bloom entries by key length and make SST metadata
large.

Fixed lengths are a pragmatic compromise:

1. They support common schemas where useful prefixes have predictable lengths.
2. They keep metadata bounded.
3. They preserve simple no-false-negative reasoning.
4. They can be extended later with delimiter-based extractors if benchmarks
   justify the complexity.

---

## 6. Detailed Design

### 6.1 Options

Add prefix bloom configuration to `LsmStorageOptions`:

```rust
pub struct PrefixBloomOptions {
    /// Enable SST prefix bloom filters.
    pub enabled: bool,
    /// Prefix lengths, in bytes, to materialize per SST.
    pub prefix_lengths: Vec<usize>,
    /// Target false positive rate for each prefix filter.
    pub false_positive_rate: f64,
}
```

Recommended defaults:

```rust
PrefixBloomOptions {
    enabled: false,
    prefix_lengths: vec![8],
    false_positive_rate: 0.01,
}
```

The feature should start disabled by default because it changes SST metadata
size and should be benchmarked for the workload.

Validation rules:

1. `prefix_lengths` must be non-empty when enabled.
2. Lengths must be unique and sorted during option normalization.
3. Length `0` is invalid.
4. `prefix_lengths` values must be less than or equal to `64` bytes. Values
   above this limit are a hard option-validation error, not silently capped.
   The cap keeps filter construction and memory usage bounded while covering
   typical structured key prefixes and preserving interoperability across
   implementations.
5. `false_positive_rate` must be in `(0.0, 1.0)`.

### 6.2 Prefix Entry Construction

Prefix bloom entries are derived from decoded user keys:

```rust
fn prefix_bloom_entry(user_key: &[u8], len: usize) -> Option<&[u8]> {
    if user_key.len() >= len {
        Some(&user_key[..len])
    } else {
        None
    }
}
```

Keys shorter than a configured length are not inserted into that length's
filter. This is safe because a query prefix of at least that length cannot
match a shorter key.

For MVCC SSTs, the builder must decode the user key before slicing the prefix,
matching the existing full-key bloom behavior. Prefix filters must not hash the
timestamp suffix.

The builder maintains one hash set per configured prefix length while the SST is
being built:

```rust
HashMap<usize, HashSet<u32>>
```

For each key and each configured length `len`, the builder inserts
`hash_key(&user_key[..len])` only when `user_key.len() >= len`. At build time,
each non-empty set is converted into a `Vec<u32>` before calling
`Bloom::build_from_key_hashes`. Bloom filter construction is order-independent
because it only sets bits, so the resulting SST bytes are deterministic without
sorting the hashes.

The bloom filter should be sized by the number of unique prefix fragments for
that length, not by the total number of keys in the SST. This avoids oversized
filters for heavily clustered tenant keys and avoids over-saturating filters
with duplicate inserts.

Partially empty configurations produce v3 SSTs containing only the non-empty
filters. For example, with `prefix_lengths = [4, 8, 16]`, if an SST has unique
fragments for lengths `4` and `8` but none for `16`, the prefix bloom section
contains entries only for lengths `4` and `8`. The v3 section table explicitly
stores each encoded filter's `prefix_len`, so readers can map filters back to
their configured lengths without consulting runtime options.

If every configured prefix length has zero unique fragments, for example all
keys in the SST are shorter than the configured length, the writer emits a v2
SST without prefix bloom metadata. It must not call
`Bloom::bloom_bits_per_key(0, false_positive_rate)` and must not encode an
empty v3 prefix bloom section.

### 6.3 Query-Time Probing

Add a method on `SsTable`:

```rust
impl SsTable {
    pub(crate) fn may_contain_prefix(&self, prefix: &[u8]) -> bool {
        let Some(prefix_blooms) = &self.prefix_blooms else {
            return true;
        };
        let Some(filter) = prefix_blooms.best_filter_for(prefix.len()) else {
            return true;
        };
        debug_assert!(prefix.len() >= filter.prefix_len);
        filter.may_contain(hash_key(&prefix[..filter.prefix_len]))
    }
}
```

The method returns `true` when metadata is absent or no configured filter can
answer the query. That keeps old SST files and short-prefix scans correct.

### 6.4 Scan Integration

Prefix bloom pruning should happen in the SST selection layer used by
`prefix_scan`, after ordinary range-overlap checks. Pruning requires both
`options.prefix_bloom.enabled == true` and present SST prefix bloom metadata:

```text
for each candidate SST:
    if !range_overlap(sst, lower, upper):
        continue
    if options.prefix_bloom.enabled:
        if let Some(prefix) = prefix_hint:
            if !sst.may_contain_prefix(prefix):
                continue
    add iterator for SST
```

This RFC should not change the public `scan` API. Generic range scans do not
have a prefix value and should not use prefix bloom filters.

The clean implementation is to keep the existing public `scan(lower, upper)`
path and add a crate-private scan builder that accepts both the MVCC read
timestamp and an optional `prefix_hint`:

```rust
fn scan_inner(
    &self,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    read_ts: Option<u64>,
    prefix_hint: Option<&[u8]>,
) -> Result<FusedIterator<LsmIterator>>
```

Wrapper behavior:

1. `KvEngine::scan` creates the read guard, calls
   `scan_inner(lower, upper, Some(read_ts), None)`, and wraps the returned
   `FusedIterator<LsmIterator>` in `ScanIterator`.
2. `KvEngine::prefix_scan` creates the read guard, calls
   `scan_inner(lower, upper, Some(read_ts), Some(prefix))`, and wraps the
   returned iterator in `ScanIterator`.
3. Snapshot transaction scans call `scan_inner(lower, upper, Some(read_ts), None)`.
4. Snapshot transaction prefix scans call
   `scan_inner(lower, upper, Some(read_ts), Some(prefix))`.
5. Serializable transactions keep the existing scan and prefix-scan rejection
   until range predicate tracking exists.

For L0, prefix pruning is applied to each overlapping SST independently before
creating the per-SST iterator. For L1+ levels, pruning is also applied per SST
before `SstConcatIterator` construction. The filtered SST list must preserve the
existing sorted level order, and empty filtered levels are skipped instead of
constructing empty concat iterators.

### 6.5 SST Format

The current SST footer stores:

```text
[data blocks][block meta][meta_offset:u32][full_key_bloom][bloom_offset:u32]
[max_ts:u64][magic:u32][version:u8]
```

Prefix bloom filters require a compatible format extension. Add a new SST
footer version:

```text
version 3:
[data blocks]
[block meta]
[meta_offset:u32]
[full_key_bloom]
[bloom_offset:u32]
[prefix_bloom_section]
[prefix_bloom_offset:u32]
[max_ts:u64]
[magic:u32]
[version:u8]
```

The `prefix_bloom_section` encoding:

```text
u16 filter_count
repeated filter_count times:
    u16 prefix_len
    u32 filter_offset
    u32 filter_len
filter bytes...
```

Offsets inside the section are relative to the start of the filter byte area.
Each filter uses the existing `Bloom::encode` format.

#### 6.5.1 Footer Decode Algorithm

Readers identify footer versions from the end of the file:

1. Read the last 17 bytes, matching the existing MVCC footer window:
   `[offset:u32][max_ts:u64][magic:u32][version:u8]`.
2. If `magic == SST_MVCC_MAGIC` and `version == 2`, interpret `offset` as
   `bloom_offset`. The file has no prefix bloom section.
3. If `magic == SST_MVCC_MAGIC` and `version == 3`, interpret `offset` as
   `prefix_bloom_offset`.
4. Any other `version` with the MVCC magic is rejected as unsupported.
5. Legacy footer parsing remains unchanged for files without the MVCC magic.

For v3, define:

```text
footer_start = file_size - 13
prefix_bloom_offset_field = footer_start - 4
prefix_bloom_offset = u32 at prefix_bloom_offset_field
bloom_offset_field = prefix_bloom_offset - 4
bloom_offset = u32 at bloom_offset_field
meta_offset_field = bloom_offset - 4
meta_offset = u32 at meta_offset_field
```

These definitional relationships must hold:

```text
meta_offset_field + 4 == bloom_offset
bloom_offset_field + 4 == prefix_bloom_offset
prefix_bloom_offset_field + 4 == footer_start
```

Bounds must satisfy:

```text
4 <= meta_offset
meta_offset + 2 <= meta_offset_field
bloom_offset + 2 <= bloom_offset_field
prefix_bloom_offset + 2 <= prefix_bloom_offset_field
```

The decoded regions are:

```text
block_meta           = [meta_offset, meta_offset_field)
full_key_bloom       = [bloom_offset, bloom_offset_field)
prefix_bloom_section = [prefix_bloom_offset, prefix_bloom_offset_field)
```

Readers must reject offset underflow, overflow, non-monotonic offsets, empty
full-key bloom data, and empty prefix bloom sections in v3 files. The full-key
bloom region must contain at least one filter byte plus the trailing `k` byte
used by `Bloom::encode`; otherwise `Bloom::may_contain` could later operate on
a zero-bit filter. The parsed block metadata must contain `num_of_elements > 0`,
matching the existing v2 rejection of empty block metadata. The parsed prefix
bloom section must contain `filter_count > 0`.

#### 6.5.2 Prefix Bloom Section Canonical Encoding

For a section starting at `section_start = prefix_bloom_offset` and ending at
`section_end = prefix_bloom_offset_field`:

```text
filter_count = u16 at section_start
filter_table_len = 2 + filter_count * 10
filter_bytes_start = section_start + filter_table_len
filter_bytes_len = section_end - filter_bytes_start
```

Each table entry is:

```text
u16 prefix_len
u32 filter_offset
u32 filter_len
```

Canonical encoding rules:

1. `filter_count > 0`.
2. `prefix_len` values are strictly increasing, unique, and within the same
   validation cap as `PrefixBloomOptions`.
3. `filter_offset + filter_len` must stay within `filter_bytes_len`.
4. Filter byte ranges must be non-overlapping and sorted by `filter_offset`.
5. `filter_len >= 2`, enough for at least one filter byte plus the `k` byte
   used by `Bloom::encode`.
6. No unreferenced leading or trailing bytes are allowed. The first filter must
   start at offset `0`, and the last filter must end exactly at
   `filter_bytes_len`.

Rejecting non-canonical sections makes corruption handling deterministic and
keeps future format migrations simpler.

Compatibility rules:

1. Version 2 SSTs have no prefix bloom section and remain readable.
2. Version 3 readers validate all offsets before decoding filters.
3. Version 3 writers still write the existing full-key bloom for point gets.
4. If prefix bloom options are disabled, writers may continue producing version
   2 SSTs.
5. `enabled = false` disables writing new prefix bloom metadata and disables
   using existing v3 prefix bloom metadata for pruning. Readers still decode and
   validate v3 metadata so old files remain compatible.
6. A future cleanup may always write version 3 once compatibility pressure is
   lower.

### 6.6 Manifest and Recovery

No manifest change is required. Prefix bloom metadata is fully contained in the
SST file and is reconstructed by `SsTable::open`.

WAL recovery is unchanged. Recovered memtables flush through the normal SST
builder, which will rebuild prefix bloom filters from recovered keys if the
feature is enabled.

### 6.7 Compaction

Compaction output SSTs build prefix bloom filters exactly like flush output
SSTs. The compaction iterator already emits the surviving keys, so the builder
does not need to inspect input SST metadata.

This also keeps prefix bloom filters correct after MVCC version GC and
tombstone dropping. Filters describe the keys present in the new SST, not the
keys that may have existed in the old SSTs.

### 6.8 Value-Log Interaction

Prefix bloom filters operate only on LSM user keys. Inline values and
value-log-backed values are identical from the filter's perspective.

No vLog index, reader cache, or GC change is required.

### 6.9 Memtable Interaction

The first implementation should only add SST prefix bloom filters. Active and
immutable memtables continue to use ordered skiplist range iteration.

Memtable prefix bloom filters can be considered later, but they would add write
path atomic updates and memory overhead. SST pruning has the better first-order
payoff because it avoids iterator creation and disk reads.

---

## 7. Correctness Rules

### 7.1 No False Negatives

For every supported query prefix `p`, if an SST contains any visible or
physical key whose user key starts with `p`, then `may_contain_prefix(p)` must
return `true`.

The filter may return `true` for SSTs that do not contain matching keys.

### 7.2 Physical Keys, Not Visibility

Prefix bloom filters are built from physical keys in the SST, including keys
whose newest visible value may be a tombstone or whose versions may be hidden
from a specific reader timestamp.

This is intentional. Bloom filters are a pruning structure, not a visibility
structure. MVCC and tombstone filtering remain the iterator's responsibility.

### 7.3 User-Key Hashing

All prefix bloom entries are based on decoded user keys. Internal MVCC
timestamps are never part of the prefix bloom hash.

### 7.4 Short Prefixes

If a query prefix is shorter than every configured prefix length, the SST must
not be rejected by prefix bloom metadata. The method returns `true` and falls
back to range scanning.

### 7.5 Old SSTs

SSTs without prefix bloom metadata must behave as if the filter answered
`true`.

### 7.6 Transaction Predicate Semantics

Prefix bloom filters do not change transaction isolation semantics. Snapshot
transactions may use prefix bloom pruning because pruning only removes SSTs that
cannot contain matching physical keys. Serializable transactions must continue
to reject `prefix_scan` until range or prefix predicate tracking is implemented.

---

## 8. Testing Plan

### 8.1 Unit Tests

Add tests for prefix bloom construction and probing:

1. A filter built with length `4` accepts prefixes whose first four bytes match
   an inserted key.
2. A filter rejects a missing fixed prefix.
3. Query prefixes shorter than the configured length are not used for pruning.
4. Keys shorter than the configured length are not inserted.
5. MVCC internal keys are decoded before prefix hashing.
6. Multiple configured lengths choose the longest usable length.

### 8.2 SST Format Tests

Add SST encode/decode tests:

1. Version 2 SSTs remain readable and have no prefix bloom metadata.
2. Version 3 SSTs decode prefix bloom sections correctly.
3. Corrupt prefix bloom offsets are rejected.
4. Empty prefix bloom sections are rejected when version 3 claims metadata.
5. Full-key point-get bloom filters still work on version 3 SSTs.
6. Duplicate or unsorted `prefix_len` entries are rejected.
7. Overlapping filter byte ranges are rejected.
8. Out-of-range `prefix_len` values are rejected.
9. A v3 footer with valid magic/version but inconsistent section bounds is
   rejected.
10. If all keys are shorter than every configured prefix length, the writer
    emits a v2 SST without prefix bloom metadata.

### 8.3 Storage Integration Tests

Add storage-level tests:

1. `prefix_scan` over flushed SSTs returns the same results with prefix bloom
   enabled and disabled.
2. Missing prefix scans skip SSTs, verified through a test-only counter.
3. Sparse prefix scans still find matching keys in one SST while skipping
   unrelated SSTs.
4. Prefix scans after compaction remain correct.
5. Tombstones and overwritten MVCC versions do not change correctness.
6. Value-log-separated values are returned correctly under prefix bloom pruning.
7. A database containing both v2 SSTs and v3 SSTs returns correct prefix-scan
   results.
8. Reopening after flush and compaction preserves prefix bloom metadata.
9. Disabling prefix bloom options after reopening still reads existing v3 SSTs
   but does not use their prefix bloom metadata for pruning and does not write
   prefix bloom metadata for new SSTs.

The "skip SSTs" assertion should use a narrow test-only hook in scan candidate
selection, counting how many SST iterators are constructed after range and
prefix-bloom pruning. It should not add broad production metrics just for tests.

### 8.4 Property Tests

For randomly generated binary keys:

1. Build SSTs with prefix bloom enabled.
2. Compare `prefix_scan(prefix)` against a reference `starts_with` filter over
   all inserted user keys.
3. Include prefixes containing `0x00`, `0xff`, empty prefixes, and prefixes
   longer than some keys.

---

## 9. Benchmarks

Add prefix scan workloads to `write-perf` or a dedicated Criterion benchmark:

1. Negative tenant lookup: many SSTs, no matching prefix.
2. Sparse tenant lookup: one matching SST out of many.
3. Dense tenant lookup: many matching keys where bloom checks should be
   negligible compared with iteration.
4. Short-prefix fallback: query shorter than configured prefix length.
5. Multiple prefix lengths: compare `[8]` against `[4, 8, 16]`.

Metrics:

1. SST iterators created.
2. Blocks read.
3. Wall-clock latency.
4. Prefix bloom false-positive rate.
5. SST size overhead.
6. Flush and compaction build-time overhead.

Success criteria for an implementation PR:

1. On a cold-cache benchmark with at least 100 SSTs and one tenant-style prefix
   per SST, negative and sparse prefix scans reduce SST iterators and below-cache
   block reads by at least 80% compared with prefix bloom disabled. The
   benchmark should reopen the database or otherwise reset the block cache
   before the measured read phase.
2. Dense prefix scans over a matching range regress wall-clock latency by no
   more than 5%.
3. Flush and compaction build time regress by no more than 10% with
   `prefix_lengths = [8]` and `false_positive_rate = 0.01`.
4. SST size overhead is reported as bytes per unique prefix fragment per
   configured length. For `[8]` at 1% FPR, expected filter bytes should be
   close to `ceil(unique_prefixes * Bloom::bloom_bits_per_key(unique_prefixes, 0.01) / 8)`
   plus the section table overhead.

---

## 10. Alternatives Considered

### 10.1 Store Every Prefix

The builder could insert every prefix of every key into one bloom filter. This
would support arbitrary query lengths, but it would multiply metadata by average
key length and make long keys expensive.

### 10.2 Delimiter-Based Prefix Extractor

The engine could support a user-configured extractor such as "prefix up to the
second colon". This may match application schemas better than fixed lengths,
but it adds API and correctness complexity for binary keys.

Fixed lengths are easier to implement first. A delimiter extractor can be added
later as a new `PrefixBloomExtractor` variant.

### 10.3 Per-Block Prefix Bloom Filters

Per-block filters could avoid reading irrelevant blocks inside a broad SST.
They are more precise, but they require block-level metadata changes and more
filter probes during iteration.

SST-level filters are the simpler first step because they prune whole files.

### 10.4 Reuse Full-Key Bloom Filters

Full-key bloom filters cannot answer prefix queries without knowing all
candidate keys. Probing `hash(prefix)` against a full-key bloom would be
incorrect because the inserted entries are full user keys, not prefixes.

---

## 11. Rollout Plan

1. Add `PrefixBloomOptions` and option validation.
2. Add prefix bloom data structures and unit tests.
3. Extend `SsTableBuilder` to collect prefix hashes from decoded user keys.
4. Add SST footer version 3 encode/decode support.
5. Add `SsTable::may_contain_prefix`.
6. Refactor scan construction to accept an optional `prefix_hint`.
7. Wire `KvEngine::prefix_scan` and `Transaction::prefix_scan` to pass the hint.
8. Add integration tests and corrupt-format tests.
9. Add benchmarks and document results.
10. Decide whether to enable a default prefix length based on benchmark data.

---

## 12. Open Questions

1. Should prefix bloom filters be enabled by default for new databases after
   benchmarks, or remain opt-in?
2. Should version 3 SSTs always be written once implemented, even when prefix
   bloom filters are disabled?
3. Do we need a delimiter-based extractor for common schemas such as
   `tenant:{id}:`, or are fixed byte lengths enough?
