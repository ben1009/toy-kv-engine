# Reduce key cloning in `SsTableBuilder::add_inner` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Eliminate the per-entry full-key clone in `SsTableBuilder::add_inner` and the per-block first-key clone in `BlockBuilder::add` by tracking positions inside already-encoded block data.

**Architecture:** Replace `BlockBuilder.first_key: KeyVec` with offset/len into `self.data`. Replace `SsTableBuilder.first_key/last_key: KeyVec` with a `has_data` flag and derive first/last meta keys from the `BlockBuilder` only at block boundaries (rare) via a new `BlockBuilder::key_at` helper.

**Tech Stack:** Rust 2024, `bytes`, `cargo nextest`, existing `KeyVec`/`KeyBytes`/`KeySlice` types.

---

## File structure

| File | Responsibility |
|---|---|
| `kv-engine/src/block/builder.rs` | `BlockBuilder` structure and `add()` logic; new `key_at()` helper. |
| `kv-engine/src/table/builder.rs` | `SsTableBuilder` structure and `add_inner()` / `build()` logic. |
| `kv-engine/src/tests/block.rs` | Unit tests for the new `key_at()` helper. |

---

### Task 1: Update `BlockBuilder` to avoid the first-key `KeyVec`

**Files:**
- Modify: `kv-engine/src/block/builder.rs`

**Why:** The first key of a block is already stored in `self.data` at the first entry's suffix. We only need its offset and length to compute overlap for later entries.

- [x] **Step 1: Change `BlockBuilder` fields**

Replace:

```rust
    /// The first key in the block
    first_key: KeyVec,
```

with:

```rust
    /// Offset in `self.data` where the first key's suffix begins.
    first_key_offset: usize,
    /// Length of the first key in bytes.
    first_key_len: u16,
    /// Whether the first key has been recorded. Required to disambiguate an
    /// empty first key from the initial zero state.
    has_first_key: bool,
```

- [x] **Step 2: Update `BlockBuilder::new`**

Initialize the new fields:

```rust
            block_size,
            first_key_offset: 0,
            first_key_len: 0,
            has_first_key: false,
```

- [x] **Step 3: Update `overlap_len` to read from `self.data`**

Replace the body with:

```rust
    fn overlap_len(&self, key: &[u8]) -> usize {
        let first_key = &self.data[self.first_key_offset
            ..self.first_key_offset + self.first_key_len as usize];
        let chunk_size = 128;
        let offset = std::iter::zip(
            first_key.chunks_exact(chunk_size),
            key.chunks_exact(chunk_size),
        )
        .take_while(|(a, b)| a == b)
        .count()
            * chunk_size;

        offset
            + std::iter::zip(&first_key[offset..], &key[offset..])
                .take_while(|(a, b)| a == b)
                .count()
    }
```

- [x] **Step 4: Update `BlockBuilder::add` to record the first-key position**

After pushing the entry data, record where the first key lives:

```rust
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty()
            && self.current_size() + key.len() + value.len() + 3 * SIZE_OF_U16 >= self.block_size
        {
            return false;
        }

        let entry_start = self.data.len();
        self.offsets.push(self.data.len() as u16);

        let overlap = if self.is_empty() {
            0
        } else {
            self.overlap_len(key.raw_ref())
        };
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len() - overlap) as u16);
        self.data.put(&key.raw_ref()[overlap..]);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key_len == 0 {
            // First entry always has overlap == 0, so the suffix is the full key.
            self.first_key_offset = entry_start + 2 * SIZE_OF_U16;
            self.first_key_len = key.len() as u16;
        }

        true
    }
```

- [x] **Step 5: Add `BlockBuilder::key_at` helper**

Add this `pub(crate)` method after `add`:

```rust
    /// Return the full key bytes of the `idx`-th entry.
    /// Used at block boundaries to produce `BlockMeta` first/last keys without
    /// maintaining a live clone of the latest key.
    pub(crate) fn key_at(&self, idx: usize) -> bytes::Bytes {
        use bytes::Bytes;
        assert!(idx < self.offsets.len(), "key_at index out of bounds");

        let off = self.offsets[idx] as usize;
        let overlap = u16::from_be_bytes([self.data[off], self.data[off + 1]]) as usize;
        let suffix_len = u16::from_be_bytes([
            self.data[off + SIZE_OF_U16],
            self.data[off + SIZE_OF_U16 + 1],
        ]) as usize;
        let suffix_start = off + 2 * SIZE_OF_U16;
        let suffix = &self.data[suffix_start..suffix_start + suffix_len];

        if overlap == 0 {
            shared_bytes_from_slice(suffix)
        } else {
            let first_key =
                &self.data[self.first_key_offset..self.first_key_offset + self.first_key_len as usize];
            let mut out = Vec::with_capacity(overlap + suffix_len + 1);
            out.extend_from_slice(&first_key[..overlap]);
            out.extend_from_slice(suffix);
            Bytes::from(out)
        }
    }
```

- [x] **Step 6: Add a local `shared_bytes_from_slice` helper at the top of the file**

At the top of `kv-engine/src/block/builder.rs`, add:

```rust
fn shared_bytes_from_slice(src: &[u8]) -> bytes::Bytes {
    if src.is_empty() {
        return bytes::Bytes::new();
    }
    let mut vec = Vec::with_capacity(src.len() + 1);
    vec.extend_from_slice(src);
    bytes::Bytes::from(vec)
}
```

- [x] **Step 7: Import `bytes::Bytes` if not already imported**

The file already imports `bytes::{BufMut, Bytes}`. If `Bytes` is present, no change is needed.

- [x] **Step 8: Run block unit tests to catch format regressions early**

```bash
cargo nextest run --package kv-engine --lib block
```

Expected: all block tests pass.

---

### Task 2: Update `SsTableBuilder` to avoid live first/last `KeyVec` clones

**Files:**
- Modify: `kv-engine/src/table/builder.rs`

**Why:** `self.last_key.set_from_slice(key)` copied the full key on every entry. We now derive first/last keys only when sealing a block or finishing the SST.

- [x] **Step 1: Change `SsTableBuilder` fields**

Remove:

```rust
    first_key: KeyVec,
    last_key: KeyVec,
```

Add:

```rust
    has_data: bool,
```

- [x] **Step 2: Update `SsTableBuilder::new` and `new_with_vlog`**

In both constructors, replace:

```rust
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
```

with:

```rust
            has_data: false,
```

- [x] **Step 3: Update `is_empty`**

Replace the body with:

```rust
    pub fn is_empty(&self) -> bool {
        !self.has_data
    }
```

- [x] **Step 4: Rewrite `add_inner`**

Replace the entire method with:

```rust
    fn add_inner(&mut self, key: KeySlice, value: &[u8]) -> Result<()> {
        if !self.has_data {
            self.has_data = true;
        }

        if !self.builder.add(key, value) {
            let old_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let first_key = KeyBytes::from_bytes(old_builder.key_at(0));
            let last_idx = old_builder.offsets.len().saturating_sub(1);
            let last_key = KeyBytes::from_bytes(old_builder.key_at(last_idx));
            let data = old_builder.build().encode();

            let meta = BlockMeta {
                offset: self.data.len(),
                first_key,
                last_key,
            };
            self.meta.push(meta);
            self.data.extend(data);

            let _ = self.builder.add(key, value);
        }

        self.key_hashes.push(super::bloom::hash_key(key.raw_ref()));
        Ok(())
    }
```

- [x] **Step 5: Update `SsTableBuilder::build` final-block handling**

Replace the final meta creation block:

```rust
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        };
```

with:

```rust
        let (first_key, last_key) = if self.has_data {
            let last_idx = self.builder.offsets.len().saturating_sub(1);
            (
                KeyBytes::from_bytes(self.builder.key_at(0)),
                KeyBytes::from_bytes(self.builder.key_at(last_idx)),
            )
        } else {
            (
                KeyBytes::from_bytes(bytes::Bytes::new()),
                KeyBytes::from_bytes(bytes::Bytes::new()),
            )
        };
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key,
        };
```

- [x] **Step 6: Update imports in `kv-engine/src/table/builder.rs`**

The file currently imports:

```rust
use bytes::BufMut;
```

Change it to:

```rust
use bytes::{BufMut, Bytes};
```

And update the crate import:

```rust
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    ...
};
```

Change it to:

```rust
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice, KeyVec},
    ...
};
```

- [x] **Step 7: Run SST unit tests**

```bash
cargo nextest run --package kv-engine --lib sst
```

Expected: all SST tests pass.

---

### Task 3: Add a focused unit test for `BlockBuilder::key_at`

**Files:**
- Modify: `kv-engine/src/tests/block.rs`

**Why:** We are now parsing raw block bytes to reconstruct keys. A dedicated test guards against format drift.

- [x] **Step 1: Append the test at the end of the file**

```rust
#[test]
fn test_block_builder_key_at() {
    let mut builder = BlockBuilder::new(10000);
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:03}", i * 5).into_bytes())
        .collect();
    for key in &keys {
        assert!(builder.add(
            KeySlice::for_testing_from_slice_no_ts(key),
            &format!("value_{:010}", key.len()).into_bytes(),
        ));
    }

    for (i, expected) in keys.iter().enumerate() {
        let actual = builder.key_at(i);
        assert_eq!(actual.as_ref(), expected.as_slice(), "key_at({i}) mismatch");
    }
}
```

- [x] **Step 2: Run the new test**

```bash
cargo nextest run --package kv-engine --lib block::test_block_builder_key_at
```

Expected: PASS.

---

### Task 4: Run the full test matrix and checks

- [x] **Step 1: Library + integration tests**

```bash
cargo nextest run --workspace --all-features --all-targets
```

Expected: all tests pass.

- [x] **Step 2: Clippy**

```bash
cargo clippy --package kv-engine --all-features -- -D warnings
```

Expected: clean.

- [x] **Step 3: Format**

```bash
cargo fmt --all -- --check
```

Expected: clean.

---

### Task 5: Optional benchmark validation

- [x] **Step 1: Run the write-heavy benchmark subset**

```bash
cargo run --release --bin write-perf -- --only fillseq fillrandom overwrite
```

If the `write-perf` binary does not accept `--only`, run the full binary and compare the three workloads against a baseline recorded before the change.

Expected: `fillseq`, `fillrandom`, and `overwrite` throughput improves by ~3–5% (variance permitting).

---

## Spec coverage check

| Spec requirement | Task |
|---|---|
| Remove `BlockBuilder.first_key: KeyVec` | Task 1, Steps 1–5 |
| `overlap_len` reads first key from `self.data` | Task 1, Step 3 |
| Remove `SsTableBuilder.first_key/last_key: KeyVec` | Task 2, Steps 1–3 |
| Derive meta first/last keys at block boundaries | Task 2, Steps 4–5 |
| Preserve SST format and all tests | Tasks 2–4 |
| Add unit test for key extraction | Task 3 |
| Verify with benchmarks | Task 5 |

## Placeholder scan

No placeholders, TBDs, or vague steps. Every code block contains the actual change; every command has an expected outcome.

## Type consistency check

- `BlockBuilder::key_at` returns `bytes::Bytes` (explicitly qualified to avoid shadowing).
- `SsTableBuilder` uses `KeyBytes::from_bytes(...)` to wrap the returned `Bytes`.
- `has_data: bool` replaces `first_key.is_empty()` checks consistently.
