#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::{
    ops::Bound,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::{
    iterators::StorageIterator,
    key::{Key, KeySlice},
    table::SsTableBuilder,
    wal::Wal,
};

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally
/// implemented in other chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table. id is the sst_id when flush this memtable to sst
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut ret = Self::create(id);
        let wal = Wal::create(path)?;
        ret.wal = Some(wal);

        Ok(ret)
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut ret = Self::create(id);
        let wal = Wal::recover(path, &ret.map)?;
        ret.wal = Some(wal);

        Ok(ret)
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|x| x.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.put(key, value)?;
            // TODO: change to write_batch sync instead
            wal.sync()?;
        }

        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        self.approximate_size.fetch_add(
            std::mem::size_of_val(key) + std::mem::size_of_val(value),
            std::sync::atomic::Ordering::SeqCst,
        );

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for e in self.map.iter() {
            builder.add(Key::from_bytes(e.key().clone()).as_key_slice(), e.value());
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn range_overlap(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        if self.map.is_empty() {
            return false;
        }

        let e = self.map.front().unwrap();
        let lo = e.key();
        let e = self.map.back().unwrap();
        let hi = e.key();
        match (lower, upper) {
            (Bound::Included(x), Bound::Included(y)) => x >= lo && x <= hi || y >= lo && y <= hi,
            (Bound::Excluded(x), Bound::Excluded(y)) => x > lo && x < hi || y > lo && y < hi,
            _ => true,
        }
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to
/// week 1, day 2 chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        Key::from_slice(self.borrow_item().0.as_ref())
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let n = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
        });

        self.with_mut(|m| *m.item = n);

        Ok(())
    }
}
