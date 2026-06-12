use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    prefetch::{PrefetchHandle, PrefetchPool},
    table::{SsTable, SsTableIterator},
    vlog::ValueLog,
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not
/// want to create the iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
    vlog: Option<Arc<ValueLog>>,
    // --- prefetch fields ---
    /// Handle for in-flight next-SST prefetch.
    sst_prefetch_handle: Option<PrefetchHandle<(usize, Arc<crate::block::Block>)>>,
    /// Whether prefetching is enabled.
    prefetch_enabled: bool,
    /// Shared prefetch thread pool.
    prefetch_pool: Option<Arc<PrefetchPool>>,
    /// Config: entries remaining before triggering block prefetch.
    prefetch_block_threshold: usize,
    /// Config: vLog prefetch depth.
    prefetch_vlog_depth: usize,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::create_and_seek_to_first_inner(sstables, None)
    }

    pub fn create_and_seek_to_first_with_vlog(
        sstables: Vec<Arc<SsTable>>,
        vlog: Arc<ValueLog>,
    ) -> Result<Self> {
        Self::create_and_seek_to_first_inner(sstables, Some(vlog))
    }

    fn create_and_seek_to_first_inner(
        sstables: Vec<Arc<SsTable>>,
        vlog: Option<Arc<ValueLog>>,
    ) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                vlog,
                sst_prefetch_handle: None,
                prefetch_enabled: false,
                prefetch_pool: None,
                prefetch_block_threshold: 4,
                prefetch_vlog_depth: 3,
            });
        }

        let mut it = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        if let Some(ref v) = vlog {
            it.set_vlog(v.clone());
        }
        let ret = SstConcatIterator {
            current: Some(it),
            next_sst_idx: 1,
            sstables,
            vlog,
            sst_prefetch_handle: None,
            prefetch_enabled: false,
            prefetch_pool: None,
            prefetch_block_threshold: 4,
            prefetch_vlog_depth: 3,
        };

        Ok(ret)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::create_and_seek_to_key_inner(sstables, key, None)
    }

    pub fn create_and_seek_to_key_with_vlog(
        sstables: Vec<Arc<SsTable>>,
        key: KeySlice,
        vlog: Arc<ValueLog>,
    ) -> Result<Self> {
        Self::create_and_seek_to_key_inner(sstables, key, Some(vlog))
    }

    fn create_and_seek_to_key_inner(
        sstables: Vec<Arc<SsTable>>,
        key: KeySlice,
        vlog: Option<Arc<ValueLog>>,
    ) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                vlog,
                sst_prefetch_handle: None,
                prefetch_enabled: false,
                prefetch_pool: None,
                prefetch_block_threshold: 4,
                prefetch_vlog_depth: 3,
            });
        }
        if key > sstables[sstables.len() - 1].last_key().as_key_slice() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                vlog,
                sst_prefetch_handle: None,
                prefetch_enabled: false,
                prefetch_pool: None,
                prefetch_block_threshold: 4,
                prefetch_vlog_depth: 3,
            });
        }

        let mut lo = 0;
        let mut hi = sstables.len() - 1;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let s = sstables[mid].clone();
            if key >= s.first_key().as_key_slice() && key <= s.last_key().as_key_slice() {
                lo = mid;
                break;
            }
            if key < s.first_key().as_key_slice() {
                if mid == 0 {
                    lo = 0;
                    break;
                }
                hi = mid - 1;
            } else if key > s.last_key().as_key_slice() {
                if mid == sstables.len() - 1 {
                    lo = sstables.len() - 1;
                    break;
                }
                lo = mid + 1;
            }
        }

        let mut it = SsTableIterator::create_and_seek_to_key(sstables[lo].clone(), key)?;
        if let Some(ref v) = vlog {
            it.set_vlog(v.clone());
        }
        let ret = SstConcatIterator {
            current: Some(it),
            next_sst_idx: lo + 1,
            sstables,
            vlog,
            sst_prefetch_handle: None,
            prefetch_enabled: false,
            prefetch_pool: None,
            prefetch_block_threshold: 4,
            prefetch_vlog_depth: 3,
        };

        Ok(ret)
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
        self.prefetch_vlog_depth = vlog_depth;

        // Propagate config to the currently active child iterator so the first
        // SST also benefits from prefetching.
        if let Some(ref mut current) = self.current {
            current.set_prefetch_config(
                self.prefetch_enabled,
                self.prefetch_pool.clone(),
                self.prefetch_block_threshold,
                self.prefetch_vlog_depth,
            );
        }

        // Trigger next-SST prefetch immediately if the current SST is already on its last block.
        if self.prefetch_enabled
            && self.sst_prefetch_handle.is_none()
            && self.next_sst_idx < self.sstables.len()
            && self.current.as_ref().is_some_and(|c| c.on_last_block())
        {
            let next_sst = self.sstables[self.next_sst_idx].clone();
            let idx = self.next_sst_idx;
            let pool = self
                .prefetch_pool
                .as_ref()
                .expect("prefetch_pool must be Some when prefetching is enabled");
            self.sst_prefetch_handle = Some(pool.submit(move || {
                let block = next_sst.read_block_cached(0)?;
                Ok((idx, block))
            }));
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&'_ self) -> KeySlice<'_> {
        // if !self.is_valid() {
        //     return KeySlice::from_slice(&[]);
        // }

        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        // if !self.is_valid() {
        //     return &[];
        // }

        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // if !self.is_valid() {
        //     return Ok(());
        // }

        self.current.as_mut().unwrap().next()?;

        // Trigger next-SST prefetch when current SST enters its last block.
        if self.prefetch_enabled
            && self.sst_prefetch_handle.is_none()
            && self.next_sst_idx < self.sstables.len()
            && self.current.as_ref().is_some_and(|c| c.on_last_block())
        {
            let next_sst = self.sstables[self.next_sst_idx].clone();
            let idx = self.next_sst_idx;
            let pool = self
                .prefetch_pool
                .as_ref()
                .expect("prefetch_pool must be Some when prefetching is enabled");
            self.sst_prefetch_handle = Some(pool.submit(move || {
                let block = next_sst.read_block_cached(0)?;
                Ok((idx, block))
            }));
        }

        while let Some(ref current) = self.current {
            if current.is_valid() {
                break;
            }
            if self.next_sst_idx < self.sstables.len() {
                let sst = self.sstables[self.next_sst_idx].clone();

                // Branch: use prefetched block if available, otherwise sync read.
                let mut it = if let Some(handle) = self.sst_prefetch_handle.take() {
                    match handle.join() {
                        Ok((_, block)) => SsTableIterator::create_with_prefetched_block(sst, block),
                        Err(e) => {
                            eprintln!("next-SST prefetch failed, falling back to sync: {e}");
                            SsTableIterator::create_and_seek_to_first(sst)?
                        }
                    }
                } else {
                    SsTableIterator::create_and_seek_to_first(sst)?
                };

                if let Some(ref v) = self.vlog {
                    it.set_vlog(v.clone());
                }
                it.set_prefetch_config(
                    self.prefetch_enabled,
                    self.prefetch_pool.clone(),
                    self.prefetch_block_threshold,
                    self.prefetch_vlog_depth,
                );
                self.current = Some(it);
                self.next_sst_idx += 1;

                // Trigger next-SST prefetch if the newly loaded SST is already on its last block.
                if self.prefetch_enabled
                    && self.sst_prefetch_handle.is_none()
                    && self.next_sst_idx < self.sstables.len()
                    && self.current.as_ref().is_some_and(|c| c.on_last_block())
                {
                    let next_sst = self.sstables[self.next_sst_idx].clone();
                    let idx = self.next_sst_idx;
                    let pool = self
                        .prefetch_pool
                        .as_ref()
                        .expect("prefetch_pool must be Some when prefetching is enabled");
                    self.sst_prefetch_handle = Some(pool.submit(move || {
                        let block = next_sst.read_block_cached(0)?;
                        Ok((idx, block))
                    }));
                }
            } else {
                self.current = None;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }

    fn raw_value(&self) -> &[u8] {
        self.current.as_ref().unwrap().raw_value()
    }
}
