# Design: Reduce key cloning in `SsTableBuilder::add_inner`

**Date:** 2026-06-05  
**Author:** kimi  
**Status:** Approved (Option A)

## Summary

`SsTableBuilder::add_inner` is the current #1 CPU consumer in write-heavy workloads
(~25% of total CPU). A large part of that cost is full-key cloning:

- `self.last_key.set_from_slice(key)` copies the full key on **every** entry.
- `BlockBuilder` copies the first key of each block into `self.first_key: KeyVec`.
- At block boundaries `self.first_key` / `self.last_key` are refreshed with another
copy of the current key.

This design eliminates almost all of those copies by tracking **positions** inside
the already-encoded block data instead of keeping parallel `Vec<u8>` copies of keys.

## Goals

- Remove the per-entry full-key clone in `SsTableBuilder::add_inner`.
- Remove the per-block first-key clone in `BlockBuilder::add`.
- Preserve exact SST format and all existing tests.
- Target 3–5% reduction in `add_inner` CPU (more on small-key workloads).

## Non-goals

- Change the SST on-disk format.
- Change public `SsTableBuilder::add` / `add_raw` signatures.
- Optimize value copying or block encoding beyond key storage.

## Design

### 1. `BlockBuilder`: remove `first_key: KeyVec`

The first entry of a block is always encoded with `overlap=0` and a suffix that
equals the full first key. Therefore the first key is already present in
`self.data` at `offsets[0]`.

Replace:

```rust
first_key: KeyVec,
```

with:

```rust
first_key_offset: usize,
first_key_len:    u16,
has_first_key:    bool,
```

- On the very first `add`, set `has_first_key = true` and record the offset and
  suffix length of the first entry. The explicit boolean sentinel correctly
  handles an empty (`""`) first key, which would otherwise be ambiguous with the
  initial `first_key_len == 0` state.
- `overlap_len(key)` reads the first key from `self.data[first_key_offset ..
  first_key_offset + first_key_len]` instead of from a separate `Vec`.

This removes `key.to_key_vec()` from the hot path entirely.

### 2. `SsTableBuilder`: derive first/last keys from encoded block data

Remove:

```rust
first_key: KeyVec,
last_key:  KeyVec,
```

Add:

```rust
has_data: bool,
```

`is_empty()` becomes `!self.has_data`.

When a block fills up and is sealed:

1. `old_builder.build().encode()` returns a `Bytes` buffer containing the encoded
   block. This buffer already holds every full key (the first entry stores the
   full first key; subsequent entries store `overlap + suffix`, which together
   reconstruct the full key).
2. A small helper parses the first and last entries from that `Bytes` and
   returns the full key bytes for each.
3. Those key bytes are copied into small dedicated `KeyBytes` buffers (one per
   meta key). Because this only happens at block boundaries (~1/1000 entries for
   the default 4 KB block / 1 KB value configuration), the copy cost is
   negligible compared to the per-entry clone we are removing.
4. The encoded block `Bytes` is then appended to `self.data` (a `Vec<u8>`). The
   meta keys do not retain references to the encoded block buffer, so there is
   no long-term memory overhead per SST.

For the trailing partial block at final `build()`, the same helper extracts the
first and last keys from the final encoded block.

### 3. Key-extraction helper

A new private helper on `Block` (or a standalone utility) decodes a single entry
header from raw block bytes:

```rust
fn read_entry_key(buf: &Bytes, offset: usize, first_key: &[u8]) -> Bytes
```

Given the entry offset and the first key bytes, it reads `(overlap, suffix_len)`,
slices `suffix` from the buffer, and returns:

```rust
if overlap == 0 {
    suffix.clone()
} else {
    let mut out = BytesMut::with_capacity(overlap + suffix_len);
    out.extend_from_slice(&first_key[..overlap]);
    out.extend_from_slice(&suffix);
    out.freeze()
}
```

At block-seal time we call it twice: once for entry 0 (which ignores
`first_key` because `overlap == 0`) and once for the last entry.

### 4. Block format invariants used

- Entry layout is fixed: `[overlap: u16][suffix_len: u16][suffix][value_len: u16][value]`.
- The first entry always has `overlap == 0`, therefore `suffix == full key`.
- All offsets are recorded in `BlockBuilder::offsets`, so entry boundaries are
  known without scanning.

These invariants are already guaranteed by the existing block builder and are
covered by existing block tests.

## Testing plan

1. **Existing tests must pass unchanged:**
   - `tests::block::*`
   - `tests::sst::*`
   - `tests::compaction::*`
   - `tests::vlog_integration_tests::sst_builder`
   - All harness-based integration tests.

2. **Add focused tests for the new helper:**
   - Build a block with keys that share long prefixes.
   - Use the helper to extract first and last keys and assert they match the
     original keys.

3. **Benchmark validation:**
   - Run `cargo bench --package kv-engine --bench vlog_benchmarks` or the
     write-perf suite and compare `fillseq` / `fillrandom` / `overwrite`
     throughput before/after.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Parsing raw block bytes incorrectly corrupts `BlockMeta` | Covered by existing SST decode tests plus a new unit test for the helper. |
| Copying key bytes at block boundary reintroduces a small cost | Boundary copies are ~1/1000 of entries; still far cheaper than the old per-entry clone. |
| Performance regression on tiny blocks where boundary copies were already cheap | The overhead of parsing two entries per block is tiny compared to eliminating a memcpy per entry. |

## Open questions

None. Approved for implementation as Option A.
