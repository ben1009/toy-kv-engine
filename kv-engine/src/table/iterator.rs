use std::{cell::UnsafeCell, sync::Arc};

use anyhow::Result;

use super::SsTable;
use crate::{
    block::BlockIterator,
    cache::CacheAdmission,
    iterators::StorageIterator,
    key::{KeyBytes, KeySlice},
    vlog::{KvKind, ValueLog, ValuePointer},
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    vlog: Option<Arc<ValueLog>>,
    /// Cache for dereferenced ValuePointer values. Uses UnsafeCell for interior
    /// mutability since `StorageIterator::value()` takes `&self`.
    deref_cache: UnsafeCell<Option<(KeyBytes, Vec<u8>)>>,
    /// Admission policy for block cache insertions on miss.
    cache_admission: CacheAdmission,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(
        table: Arc<SsTable>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        crate::scan_trace::note_block_load();
        let b = table.read_block_cached_with_admission(0, cache_admission)?;
        table.prefetch_block(1);
        Ok(SsTableIterator {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(b),
            blk_idx: 0,
            vlog: None,
            deref_cache: UnsafeCell::new(None),
            cache_admission,
        })
    }

    /// Create a new iterator with vLog support and seek to the first key-value pair.
    pub fn create_and_seek_to_first_with_vlog(
        table: Arc<SsTable>,
        vlog: Arc<ValueLog>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        let mut it = Self::create_and_seek_to_first(table, cache_admission)?;
        it.vlog = Some(vlog);
        Ok(it)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        crate::scan_trace::note_block_load();
        let b = self
            .table
            .read_block_cached_with_admission(0, self.cache_admission)?;
        self.table.prefetch_block(1);
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(b);
        *self.deref_cache.get_mut() = None;

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(
        table: Arc<SsTable>,
        key: KeySlice,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key, cache_admission)?;

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx,
            vlog: None,
            deref_cache: UnsafeCell::new(None),
            cache_admission,
        })
    }

    /// Create a new iterator with vLog support and seek to the first key >= `key`.
    pub fn create_and_seek_to_key_with_vlog(
        table: Arc<SsTable>,
        key: KeySlice,
        vlog: Arc<ValueLog>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        let mut it = Self::create_and_seek_to_key(table, key, cache_admission)?;
        it.vlog = Some(vlog);
        Ok(it)
    }

    /// Set the vLog for ValuePointer dereferencing.
    pub fn set_vlog(&mut self, vlog: Arc<ValueLog>) {
        self.vlog = Some(vlog);
    }

    fn seek_to_key_inner(
        table: &Arc<SsTable>,
        key: KeySlice,
        cache_admission: CacheAdmission,
    ) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        crate::scan_trace::note_block_load();
        let mut blk_iter = BlockIterator::create_and_seek_to_key(
            table.read_block_cached_with_admission(blk_idx, cache_admission)?,
            key,
        );
        table.prefetch_block(blk_idx + 1);
        // If the block iterator is invalid OR positioned before the target key,
        // advance to subsequent blocks until we find one that contains entries >= key.
        // This handles the case where encoded keys are longer than raw keys,
        // shifting block boundaries so the target falls past the found block's last key.
        while !blk_iter.is_valid() || blk_iter.key().raw_ref() < key.raw_ref() {
            blk_idx += 1;
            if blk_idx >= table.num_of_blocks() {
                break;
            }
            crate::scan_trace::note_block_load();
            blk_iter = BlockIterator::create_and_seek_to_key(
                table.read_block_cached_with_admission(blk_idx, cache_admission)?,
                key,
            );
            table.prefetch_block(blk_idx + 1);
        }

        Ok((blk_idx, blk_iter))
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key, self.cache_admission)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        *self.deref_cache.get_mut() = None;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&'_ self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the resolved value: strips the KvKind prefix and dereferences ValuePointers.
    fn value(&self) -> &[u8] {
        let raw = self.blk_iter.value();
        if raw.is_empty() {
            return &[];
        }
        let kind = raw[0];
        let payload = &raw[1..];

        match KvKind::from_u8(kind) {
            Some(KvKind::Inline) => {
                // Inline value or legacy tombstone (empty payload)
                payload
            }
            Some(KvKind::Tombstone) => {
                // Tombstone marker — return the raw kind byte so callers can
                // detect it via `is_tombstone_value`.
                &raw[..1]
            }
            Some(KvKind::TtlInline) => {
                // TTL inline: strip 9-byte prefix (kind + expire_at_secs)
                if raw.len() < 9 {
                    return &[];
                }
                &raw[9..]
            }
            Some(KvKind::TtlValuePointer) => {
                // TTL value pointer: strip 9-byte prefix, resolve vLog
                if raw.len() < 25 {
                    return &[];
                }
                // Check cache first (safe: only accessed from this iterator)
                let cache = unsafe { &*self.deref_cache.get() };
                if let Some((cached_key, cached_val)) = cache
                    && cached_key.as_key_slice().raw_ref() == self.blk_iter.key().raw_ref()
                {
                    return cached_val;
                }
                let vlog = self
                    .vlog
                    .as_ref()
                    .expect("SsTableIterator encountered TtlValuePointer but no vLog was provided");
                let ptr = ValuePointer::try_decode(&raw[9..])
                    .expect("SsTableIterator: invalid TtlValuePointer encoding in block");
                let vlog_key = self.blk_iter.key().raw_ref().to_vec();
                let bytes = vlog
                    .read(&ptr, &vlog_key)
                    .expect("SsTableIterator: failed to read value from vLog");
                let val = bytes.to_vec();
                let cache_mut = unsafe { &mut *self.deref_cache.get() };
                *cache_mut = Some((self.blk_iter.key().to_key_vec().into_key_bytes(), val));
                &cache_mut.as_ref().unwrap().1
            }
            Some(KvKind::ValuePointer) => {
                // Check cache first (safe: only accessed from this iterator)
                let cache = unsafe { &*self.deref_cache.get() };
                if let Some((cached_key, cached_val)) = cache
                    && cached_key.as_key_slice().raw_ref() == self.blk_iter.key().raw_ref()
                {
                    return cached_val;
                }
                // Cache miss: dereference from vLog
                let vlog = self
                    .vlog
                    .as_ref()
                    .expect("SsTableIterator encountered ValuePointer but no vLog was provided");
                let ptr = ValuePointer::try_decode(payload)
                    .expect("SsTableIterator: invalid ValuePointer encoding in block");
                // With MVCC, vLog entries are keyed by the full encoded internal
                // key (user key + ts). Pass it directly for verification.
                let vlog_key = self.blk_iter.key().raw_ref().to_vec();
                let bytes = vlog
                    .read(&ptr, &vlog_key)
                    .expect("SsTableIterator: failed to read value from vLog");
                let val = bytes.to_vec();
                // Update cache (safe: single-threaded, only written here)
                let cache_mut = unsafe { &mut *self.deref_cache.get() };
                *cache_mut = Some((self.blk_iter.key().to_key_vec().into_key_bytes(), val));
                &cache_mut.as_ref().unwrap().1
            }
            None => {
                // Unknown kind byte — treat as inline value
                raw
            }
        }
    }

    /// Return the raw value bytes including the KvKind prefix.
    fn raw_value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        // Clear deref cache
        *self.deref_cache.get_mut() = None;

        self.blk_iter.next();

        if !self.blk_iter.is_valid() {
            let idx = self.blk_idx + 1;
            if idx >= self.table.num_of_blocks() {
                return Ok(());
            }

            crate::scan_trace::note_block_load();
            let b = self
                .table
                .read_block_cached_with_admission(idx, self.cache_admission)?;
            self.table.prefetch_block(idx + 1);
            self.blk_idx = idx;
            self.blk_iter = BlockIterator::create_and_seek_to_first(b);
        }

        Ok(())
    }
}
