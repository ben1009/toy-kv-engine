use std::{cell::UnsafeCell, sync::Arc};

use anyhow::Result;

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::{KeyBytes, KeySlice},
    prefetch::{PrefetchHandle, PrefetchPool},
    vlog::{KvKind, ValueLog, ValuePointer},
};

/// Prefetched vLog value: (entry index, key bytes, value bytes).
type PrefetchedValue = Option<(usize, KeyBytes, Vec<u8>)>;

/// Maximum number of vLog prefetch slots. The fixed-size array must be at
/// least as large as the maximum allowed `prefetch_vlog_depth`.
const MAX_VLOG_PREFETCH_DEPTH: usize = 4;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    vlog: Option<Arc<ValueLog>>,
    /// Cache for dereferenced ValuePointer values. Uses UnsafeCell for interior
    /// mutability since `StorageIterator::value()` takes `&self`. The returned
    /// `&[u8]` borrows from this cache — callers must consume the reference
    /// before calling `value()` again.
    deref_cache: UnsafeCell<Option<(KeyBytes, Vec<u8>)>>,
    // --- prefetch fields ---
    /// Handle for in-flight prefetch I/O.
    prefetch_handle: Option<PrefetchHandle<(usize, Arc<Block>)>>,
    /// Whether prefetching is enabled for this iterator.
    prefetch_enabled: bool,
    /// Shared prefetch thread pool. None = prefetching disabled.
    prefetch_pool: Option<Arc<PrefetchPool>>,
    /// Config: entries remaining before triggering block prefetch.
    prefetch_block_threshold: usize,
    /// Config: vLog prefetch depth. Clamped to MAX_VLOG_PREFETCH_DEPTH.
    prefetch_vlog_depth: usize,
    /// Prefetched vLog values: (entry_idx, key_bytes, value_bytes).
    /// Fixed-size array with linear scan — faster than HashMap for ≤ 4 entries.
    /// Wrapped in UnsafeCell because `value()` takes `&self` and must remove entries.
    prefetched_values: UnsafeCell<[PrefetchedValue; MAX_VLOG_PREFETCH_DEPTH]>,
    /// Handles for in-flight vLog prefetch I/O.
    vlog_prefetch_handles: Vec<PrefetchHandle<(usize, KeyBytes, Vec<u8>)>>,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let b = table.read_block_cached(0)?;
        Ok(SsTableIterator {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(b),
            blk_idx: 0,
            vlog: None,
            deref_cache: UnsafeCell::new(None),
            prefetch_handle: None,
            prefetch_enabled: false,
            prefetch_pool: None,
            prefetch_block_threshold: 4,
            prefetch_vlog_depth: 3,
            prefetched_values: UnsafeCell::new([None, None, None, None]),
            vlog_prefetch_handles: Vec::new(),
        })
    }

    /// Create a new iterator with vLog support and seek to the first key-value pair.
    pub fn create_and_seek_to_first_with_vlog(
        table: Arc<SsTable>,
        vlog: Arc<ValueLog>,
    ) -> Result<Self> {
        let mut it = Self::create_and_seek_to_first(table)?;
        it.vlog = Some(vlog);
        Ok(it)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let b = self.table.read_block_cached(0)?;
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(b);
        *self.deref_cache.get_mut() = None;
        self.clear_prefetch_state();

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx,
            vlog: None,
            deref_cache: UnsafeCell::new(None),
            prefetch_handle: None,
            prefetch_enabled: false,
            prefetch_pool: None,
            prefetch_block_threshold: 4,
            prefetch_vlog_depth: 3,
            prefetched_values: UnsafeCell::new([None, None, None, None]),
            vlog_prefetch_handles: Vec::new(),
        })
    }

    /// Create a new iterator with vLog support and seek to the first key >= `key`.
    pub fn create_and_seek_to_key_with_vlog(
        table: Arc<SsTable>,
        key: KeySlice,
        vlog: Arc<ValueLog>,
    ) -> Result<Self> {
        let mut it = Self::create_and_seek_to_key(table, key)?;
        it.vlog = Some(vlog);
        Ok(it)
    }

    /// Create an iterator using a pre-fetched block 0. Skips the initial
    /// `read_block_cached(0)` call. Used by `SstConcatIterator` to consume
    /// prefetched blocks without redundant I/O.
    pub(crate) fn create_with_prefetched_block(table: Arc<SsTable>, block: Arc<Block>) -> Self {
        SsTableIterator {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
            vlog: None,
            deref_cache: UnsafeCell::new(None),
            prefetch_handle: None,
            prefetch_enabled: false,
            prefetch_pool: None,
            prefetch_block_threshold: 4,
            prefetch_vlog_depth: 3,
            prefetched_values: UnsafeCell::new([None, None, None, None]),
            vlog_prefetch_handles: Vec::new(),
        }
    }

    /// Set the vLog for ValuePointer dereferencing.
    pub fn set_vlog(&mut self, vlog: Arc<ValueLog>) {
        self.vlog = Some(vlog);
    }

    /// Configure prefetch settings for this iterator.
    pub(crate) fn set_prefetch_config(
        &mut self,
        enabled: bool,
        pool: Option<Arc<PrefetchPool>>,
        block_threshold: usize,
        vlog_depth: usize,
    ) {
        self.prefetch_enabled = enabled && pool.is_some();
        self.prefetch_pool = pool;
        self.prefetch_block_threshold = block_threshold;
        // Clamp to the fixed array size to avoid silent result drops.
        self.prefetch_vlog_depth = vlog_depth.min(MAX_VLOG_PREFETCH_DEPTH);

        // Trigger prefetch immediately so the second key isn't always sync.
        if self.prefetch_enabled {
            if self.should_prefetch_block() {
                self.fire_block_prefetch();
            }
            self.maybe_prefetch_vlog_values();
        }
    }

    /// Returns true if the iterator is currently on the last block of the SST.
    pub(crate) fn on_last_block(&self) -> bool {
        self.blk_idx + 1 >= self.table.num_of_blocks()
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);
        // If the block iterator is invalid OR positioned before the target key,
        // advance to subsequent blocks until we find one that contains entries >= key.
        // This handles the case where encoded keys are longer than raw keys,
        // shifting block boundaries so the target falls past the found block's last key.
        while !blk_iter.is_valid() || blk_iter.key().raw_ref() < key.raw_ref() {
            blk_idx += 1;
            if blk_idx >= table.num_of_blocks() {
                break;
            }
            blk_iter =
                BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);
        }

        Ok((blk_idx, blk_iter))
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        *self.deref_cache.get_mut() = None;
        self.clear_prefetch_state();

        Ok(())
    }

    /// Clear all prefetch state (block + vLog). Called on seek and when
    /// the iterator transitions to a new block.
    fn clear_prefetch_state(&mut self) {
        self.prefetch_handle = None;
        let values = unsafe { &mut *self.prefetched_values.get() };
        for slot in values.iter_mut() {
            *slot = None;
        }
        self.vlog_prefetch_handles.clear();
    }

    /// Returns true when the block iterator is close enough to the end
    /// that we should start prefetching the next block.
    fn should_prefetch_block(&self) -> bool {
        if !self.prefetch_enabled {
            return false;
        }
        if self.prefetch_handle.is_some() {
            return false; // already in-flight
        }
        let remaining = self.blk_iter.remaining_entries();
        remaining <= self.prefetch_block_threshold
    }

    /// Submit a prefetch for the next block to the thread pool.
    fn fire_block_prefetch(&mut self) {
        let next_idx = self.blk_idx + 1;
        if next_idx >= self.table.num_of_blocks() {
            return; // no next block
        }
        let pool = self.prefetch_pool.as_ref().unwrap();
        let table = self.table.clone();
        self.prefetch_handle = Some(pool.submit(move || {
            let block = table.read_block_cached(next_idx)?;
            Ok((next_idx, block))
        }));
    }

    /// Peek at upcoming block entries to prefetch their vLog values.
    fn maybe_prefetch_vlog_values(&mut self) {
        if self.vlog.is_none() || !self.prefetch_enabled {
            return;
        }
        let pool = self.prefetch_pool.as_ref().unwrap();

        let values = unsafe { &*self.prefetched_values.get() };
        let occupied_count = values.iter().filter(|v| v.is_some()).count();
        let mut active_count = self.vlog_prefetch_handles.len();

        let start_idx = self.blk_iter.idx() + 1;
        let end_idx = (start_idx + self.prefetch_vlog_depth).min(self.blk_iter.block_offsets_len());

        for entry_idx in start_idx..end_idx {
            // Check if already prefetched.
            if values
                .iter()
                .any(|v| v.as_ref().is_some_and(|(i, _, _)| *i == entry_idx))
            {
                continue;
            }
            // Check if we've hit the capacity limit (occupied slots + in-flight handles).
            if occupied_count + active_count >= self.prefetch_vlog_depth {
                break;
            }

            // Peek at the raw value — check kind byte before allocating.
            let raw_ref = self.blk_iter.value_at(entry_idx);
            if raw_ref.is_empty() || raw_ref[0] != KvKind::ValuePointer as u8 {
                continue;
            }
            let payload = &raw_ref[1..];
            let ptr = match ValuePointer::try_decode(payload) {
                Some(p) => p,
                None => continue,
            };

            // Copy the key bytes out of the block for the closure.
            let vlog_key = self.blk_iter.key_bytes_at(entry_idx);

            let vlog = self.vlog.as_ref().unwrap().clone();
            self.vlog_prefetch_handles.push(pool.submit(move || {
                let bytes = vlog.read(&ptr, &vlog_key)?;
                // vlog_key is borrowed by read() above, then moved into from_vec().
                let key_bytes = crate::key::Key::from_vec(vlog_key).into_key_bytes();
                Ok((entry_idx, key_bytes, bytes.to_vec()))
            }));
            active_count += 1;
        }
    }

    /// Collect completed vLog prefetch results into the prefetched_values array.
    fn collect_vlog_prefetches(&mut self) {
        let values = unsafe { &mut *self.prefetched_values.get() };
        self.vlog_prefetch_handles
            .retain(|handle| match handle.try_join() {
                Err(std::sync::mpsc::TryRecvError::Empty) => true, // keep handle
                Err(std::sync::mpsc::TryRecvError::Disconnected) => false, // thread panicked
                Ok(Ok((idx, key, val))) => {
                    // Insert into first empty slot.
                    if let Some(slot) = values.iter_mut().find(|s| s.is_none()) {
                        *slot = Some((idx, key, val));
                    }
                    false
                }
                Ok(Err(_)) => false, // I/O error; fall back to sync read in value()
            });
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
            Some(KvKind::ValuePointer) => {
                let current_idx = self.blk_iter.idx();
                // Check prefetched values first.
                let values = unsafe { &mut *self.prefetched_values.get() };
                if let Some(slot) = values
                    .iter_mut()
                    .find(|s| s.as_ref().is_some_and(|(i, _, _)| *i == current_idx))
                {
                    let (_, key_bytes, val) = slot.take().unwrap();
                    let cache_mut = unsafe { &mut *self.deref_cache.get() };
                    *cache_mut = Some((key_bytes, val));
                    return &cache_mut.as_ref().unwrap().1;
                }
                // Check deref cache (safe: only accessed from this iterator)
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

        // Trigger prefetch if we're near the end of the current block.
        if self.should_prefetch_block() {
            self.fire_block_prefetch();
        }

        if !self.blk_iter.is_valid() {
            let idx = self.blk_idx + 1;
            if idx >= self.table.num_of_blocks() {
                return Ok(());
            }

            // Try to consume the prefetched block first.
            let b = if let Some(handle) = self.prefetch_handle.take() {
                let (prefetched_idx, block) = handle.join()?;
                debug_assert_eq!(prefetched_idx, idx);
                block
            } else {
                // Fallback: synchronous read (prefetch disabled or missed).
                self.table.read_block_cached(idx)?
            };

            self.blk_idx = idx;
            self.blk_iter = BlockIterator::create_and_seek_to_first(b);

            // Clear vLog prefetch state since we're on a new block.
            let values = unsafe { &mut *self.prefetched_values.get() };
            for slot in values.iter_mut() {
                *slot = None;
            }
            self.vlog_prefetch_handles.clear();
        }

        // Trigger vLog prefetch for upcoming entries.
        self.maybe_prefetch_vlog_values();
        // Collect any completed vLog prefetch results.
        self.collect_vlog_prefetches();

        Ok(())
    }
}
