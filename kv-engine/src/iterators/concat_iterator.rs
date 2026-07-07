use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    cache::CacheAdmission,
    key::KeySlice,
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
    cache_admission: CacheAdmission,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(
        sstables: Vec<Arc<SsTable>>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        Self::create_and_seek_to_first_inner(sstables, None, cache_admission)
    }

    pub fn create_and_seek_to_first_with_vlog(
        sstables: Vec<Arc<SsTable>>,
        vlog: Arc<ValueLog>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        Self::create_and_seek_to_first_inner(sstables, Some(vlog), cache_admission)
    }

    fn create_and_seek_to_first_inner(
        sstables: Vec<Arc<SsTable>>,
        vlog: Option<Arc<ValueLog>>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        // Filter out range-only SSTs (no point data) — their range tombstones
        // are handled separately by the read path.
        let sstables: Vec<_> = sstables
            .into_iter()
            .filter(|sst| sst.first_key().is_some())
            .collect();
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                vlog,
                cache_admission,
            });
        }

        let mut it =
            SsTableIterator::create_and_seek_to_first(sstables[0].clone(), cache_admission)?;
        if let Some(ref v) = vlog {
            it.set_vlog(v.clone());
        }
        let ret = SstConcatIterator {
            current: Some(it),
            next_sst_idx: 1,
            sstables,
            vlog,
            cache_admission,
        };

        Ok(ret)
    }

    pub fn create_and_seek_to_key(
        sstables: Vec<Arc<SsTable>>,
        key: KeySlice,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        Self::create_and_seek_to_key_inner(sstables, key, None, cache_admission)
    }

    pub fn create_and_seek_to_key_with_vlog(
        sstables: Vec<Arc<SsTable>>,
        key: KeySlice,
        vlog: Arc<ValueLog>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        Self::create_and_seek_to_key_inner(sstables, key, Some(vlog), cache_admission)
    }

    fn create_and_seek_to_key_inner(
        sstables: Vec<Arc<SsTable>>,
        key: KeySlice,
        vlog: Option<Arc<ValueLog>>,
        cache_admission: CacheAdmission,
    ) -> Result<Self> {
        // Filter out range-only SSTs (no point data).
        let sstables: Vec<_> = sstables
            .into_iter()
            .filter(|sst| sst.first_key().is_some())
            .collect();
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                vlog,
                cache_admission,
            });
        }
        if key
            > sstables[sstables.len() - 1]
                .last_key_or_panic()
                .as_key_slice()
        {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
                vlog,
                cache_admission,
            });
        }

        let mut lo = 0;
        let mut hi = sstables.len() - 1;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let s = sstables[mid].clone();
            if key >= s.first_key_or_panic().as_key_slice()
                && key <= s.last_key_or_panic().as_key_slice()
            {
                lo = mid;
                break;
            }
            if key < s.first_key_or_panic().as_key_slice() {
                if mid == 0 {
                    lo = 0;
                    break;
                }
                hi = mid - 1;
            } else if key > s.last_key_or_panic().as_key_slice() {
                if mid == sstables.len() - 1 {
                    lo = sstables.len() - 1;
                    break;
                }
                lo = mid + 1;
            }
        }

        let mut it =
            SsTableIterator::create_and_seek_to_key(sstables[lo].clone(), key, cache_admission)?;
        if let Some(ref v) = vlog {
            it.set_vlog(v.clone());
        }
        let ret = SstConcatIterator {
            current: Some(it),
            next_sst_idx: lo + 1,
            sstables,
            vlog,
            cache_admission,
        };

        Ok(ret)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&'_ self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;

        while let Some(ref current) = self.current {
            if current.is_valid() {
                break;
            }
            if self.next_sst_idx < self.sstables.len() {
                crate::scan_trace::note_sst_switch();
                let mut it = SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                    self.cache_admission,
                )?;
                if let Some(ref v) = self.vlog {
                    it.set_vlog(v.clone());
                }
                self.current = Some(it);
                self.next_sst_idx += 1;
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
