use std::{mem, path::Path, sync::Arc};

use anyhow::{Result, bail};
use bytes::{BufMut, Bytes};

use super::{
    BlockMeta, FileObject, SsTable,
    bloom::{self, Bloom},
};
use crate::{
    block::{Block, BlockBuilder},
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    vlog::{KvKind, ValueLogBuilder, ValuePointer, ValueSeparationOptions, index::VlogIndexEntry},
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    key_hashes: Vec<u32>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    vlog_builder: Option<ValueLogBuilder>,
    vlog_options: ValueSeparationOptions,
    referenced_vlog_ids: Vec<u32>,
    /// Whether any entry has been added. Used by `is_empty()` and `build()`
    /// to guard against calling `key_at()` on an empty builder.
    has_data: bool,
    /// Collected blocks for cache backfill. Only populated when
    /// `collect_blocks` is true (e.g., during flush and compaction).
    collected_blocks: Vec<Arc<Block>>,
    /// Whether to collect blocks during build for cache backfill.
    collect_blocks: bool,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            vlog_builder: None,
            vlog_options: ValueSeparationOptions::default(),
            referenced_vlog_ids: Vec::new(),
            has_data: false,
            collected_blocks: Vec::new(),
            collect_blocks: false,
        }
    }

    /// Create a builder with vLog support for value separation.
    pub fn new_with_vlog(
        block_size: usize,
        vlog_builder: ValueLogBuilder,
        vlog_options: ValueSeparationOptions,
    ) -> Self {
        let file_id = vlog_builder.file_id();
        Self {
            builder: BlockBuilder::new(block_size),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            vlog_builder: Some(vlog_builder),
            vlog_options,
            referenced_vlog_ids: vec![file_id],
            has_data: false,
            collected_blocks: Vec::new(),
            collect_blocks: false,
        }
    }

    /// Enable or disable block collection for cache backfill.
    /// When enabled, `build_with_backfill()` will return the collected blocks.
    pub fn set_collect_blocks(&mut self, collect: bool) {
        self.collect_blocks = collect;
    }

    pub fn is_empty(&self) -> bool {
        !self.has_data
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// The value is prefixed with a 1-byte `KvKind` tag. If a vLog builder is set
    /// and the value is large enough, the value is written to the vLog and a
    /// `ValuePointer` is stored instead.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> Result<()> {
        if self.vlog_options.enabled
            && value.len() >= self.vlog_options.min_value_size
            && !value.is_empty()
        {
            // Write value to vLog and store a pointer
            let vlog = self.vlog_builder.as_mut().expect("vLog builder required");
            let ptr = vlog.add(key.raw_ref(), value)?;
            let mut buf = [0u8; 1 + ValuePointer::encoded_size()];
            buf[0] = KvKind::ValuePointer as u8;
            ptr.encode(&mut &mut buf[1..]);
            self.add_inner(key, &buf)?;
        } else {
            // Store inline: [KvKind::Inline][value]
            let total_len = 1 + value.len();
            if total_len <= 256 {
                let mut buf = [0u8; 256];
                buf[0] = KvKind::Inline as u8;
                buf[1..total_len].copy_from_slice(value);
                self.add_inner(key, &buf[..total_len])?;
            } else {
                let mut buf = Vec::with_capacity(total_len);
                buf.push(KvKind::Inline as u8);
                buf.extend_from_slice(value);
                self.add_inner(key, &buf)?;
            }
        }

        Ok(())
    }

    /// Adds a key-value pair with a raw (already kind-prefixed) value to SSTable.
    /// Used during compaction to preserve existing ValuePointers.
    pub fn add_raw(&mut self, key: KeySlice, raw_value: &[u8]) -> Result<()> {
        // Track vLog file IDs referenced by ValuePointer entries
        if raw_value.len() > 1
            && raw_value[0] == KvKind::ValuePointer as u8
            && let Some(ptr) = ValuePointer::try_decode(&raw_value[1..])
            && !self.referenced_vlog_ids.contains(&ptr.file_id)
        {
            self.referenced_vlog_ids.push(ptr.file_id);
        }
        self.add_inner(key, raw_value)
    }

    fn add_inner(&mut self, key: KeySlice, value: &[u8]) -> Result<()> {
        if self.builder.add(key, value)? {
            // Set on success (both first-add and post-seal re-add paths).
            self.has_data = true;
        } else {
            let old_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let first_key = KeyBytes::from_bytes(old_builder.key_at(0));
            let last_idx = old_builder.num_entries().saturating_sub(1);
            let last_key = KeyBytes::from_bytes(old_builder.key_at(last_idx));
            let block = old_builder.build();
            let data = if self.collect_blocks {
                let data = block.encode_ref()?;
                self.collected_blocks.push(Arc::new(block));
                data
            } else {
                block.encode()
            };

            let meta = BlockMeta {
                offset: self.data.len(),
                first_key,
                last_key,
            };
            self.meta.push(meta);

            self.data.extend(data);
            if !self.builder.add(key, value)? {
                bail!(
                    "key-value entry exceeds maximum block size and cannot be stored: \
                     key_len={}, value_len={}, block_size={}",
                    key.len(),
                    value.len(),
                    self.block_size
                );
            }
            // Set here too: this is the post-seal re-add success path.
            self.has_data = true;
        }

        self.key_hashes.push(super::bloom::hash_key(key.raw_ref()));
        Ok(())
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Returns the vLog file IDs referenced by entries in this SST.
    pub fn vlog_file_ids(&self) -> &[u32] {
        &self.referenced_vlog_ids
    }

    /// Take the collected vLog index entries from the vLog builder.
    /// Must be called before `build()` which consumes the builder.
    pub fn take_vlog_entries(&mut self) -> Vec<VlogIndexEntry> {
        self.vlog_builder
            .as_mut()
            .map(|b| b.take_entries())
            .unwrap_or_default()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to
    /// manipulate the disk objects.
    ///
    /// Delegates to [`build_with_backfill`] and discards the collected blocks.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let (sst, _blocks) = self.build_with_backfill(id, block_cache, path)?;
        Ok(sst)
    }

    /// Builds the SSTable, writes it to the given path, and returns both the
    /// SST and the collected blocks (if `collect_blocks` was enabled).
    ///
    /// The returned blocks can be inserted into the block cache via
    /// [`BlockCache::backfill`] to warm the cache after flush or compaction.
    pub fn build_with_backfill(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<(SsTable, Vec<Arc<Block>>)> {
        // Close the vLog builder (fsync) before writing the SST.
        if let Some(vlog) = self.vlog_builder.take() {
            vlog.close()?;
        }

        let (first_key, last_key) = if self.has_data {
            let last_idx = self.builder.num_entries().saturating_sub(1);
            (
                KeyBytes::from_bytes(self.builder.key_at(0)),
                KeyBytes::from_bytes(self.builder.key_at(last_idx)),
            )
        } else {
            (
                KeyBytes::from_bytes(Bytes::new()),
                KeyBytes::from_bytes(Bytes::new()),
            )
        };
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key,
        };
        self.meta.push(meta);

        let final_block = self.builder.build();
        let data = if self.collect_blocks && self.has_data {
            let data = final_block.encode_ref()?;
            self.collected_blocks.push(Arc::new(final_block));
            data
        } else {
            final_block.encode()
        };
        self.data.extend(data);
        let mut buf = self.data;

        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);

        let bloom_offset = buf.len();
        let b: usize = bloom::Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(self.key_hashes.as_slice(), b);
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;

        if self.collect_blocks && self.has_data {
            debug_assert_eq!(
                self.collected_blocks.len(),
                self.meta.len(),
                "backfill block count mismatch: collected {} but SST has {} meta entries",
                self.collected_blocks.len(),
                self.meta.len()
            );
        }

        let meta = mem::take(&mut self.meta);
        let first_key = meta[0].first_key.clone();
        let last_key = meta[meta.len() - 1].last_key.clone();

        let sst = SsTable {
            file,
            block_meta: meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        };
        Ok((sst, self.collected_blocks))
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
