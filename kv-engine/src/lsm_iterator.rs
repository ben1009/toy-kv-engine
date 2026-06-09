use std::ops::Bound;

use anyhow::{Ok, Result, bail};

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    key::TS_ENABLED,
    mem_table::MemTableIterator,
    mvcc::ReadGuard,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed in future iterations
/// for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Vec<u8>>,
    /// Decoded user key (MVCC only) — returned by `key()` and used for bound checks.
    user_key: Vec<u8>,
    /// Encoded user-key prefix (MVCC only) — used to skip all versions of the
    /// current user key in `next()`.  Must stay in sync with `user_key`.
    encoded_user_key: Vec<u8>,
    /// MVCC read timestamp — if set, versions with `ts > read_ts` are skipped
    /// so the iterator only yields versions visible at this snapshot.
    read_ts: Option<u64>,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        upper: Bound<Vec<u8>>,
        read_ts: Option<u64>,
    ) -> Result<Self> {
        let mut iter = iter;
        // Skip tombstones and versions beyond read_ts at the start
        Self::skip_tombstones(&mut iter, read_ts)?;

        let (user_key, encoded_user_key) = if iter.is_valid() && TS_ENABLED {
            (
                iter.key().decode_user_key(),
                iter.key().encoded_user_key().to_vec(),
            )
        } else {
            (Vec::new(), Vec::new())
        };

        Ok(Self {
            inner: iter,
            upper,
            user_key,
            encoded_user_key,
            read_ts,
        })
    }

    /// Check if a value represents a tombstone (single KvKind::Tombstone byte).
    fn is_tombstone_value(v: &[u8]) -> bool {
        v.len() == 1 && v[0] == crate::vlog::KvKind::Tombstone as u8
    }

    /// Skip entries with empty values (tombstones) and versions beyond `read_ts`.
    /// When MVCC is enabled, skip all versions of a dead user key, and for
    /// each user key skip versions with `ts > read_ts` (invisible to this
    /// snapshot) until we find one at or below `read_ts`.
    fn skip_tombstones(iter: &mut LsmIteratorInner, read_ts: Option<u64>) -> Result<()> {
        while iter.is_valid() {
            // Extract the current encoded user key once per user-key group
            // and reuse it in both the read_ts filtering and tombstone skip.
            let encoded_user_key = if TS_ENABLED {
                iter.key().encoded_user_key().to_vec()
            } else {
                Vec::new()
            };
            // Skip versions invisible to this snapshot
            if TS_ENABLED && let Some(rts) = read_ts {
                while iter.is_valid()
                    && iter.key().encoded_user_key() == encoded_user_key.as_slice()
                    // Note: `extract_ts` returning None (malformed key) falls
                    // through here, treating the key as visible.  All keys
                    // produced by this engine carry a valid timestamp suffix
                    // when TS_ENABLED, so None indicates corrupt data.
                    && crate::key::extract_ts(iter.key().raw_ref()).is_some_and(|ts| ts > rts)
                {
                    iter.next()?;
                }
                // If we exhausted all versions of this user key, move on
                if !iter.is_valid() || iter.key().encoded_user_key() != encoded_user_key.as_slice()
                {
                    continue;
                }
            }
            if !Self::is_tombstone_value(iter.value()) {
                return Ok(());
            }
            // Tombstone — skip all versions of this dead user key
            if TS_ENABLED {
                while iter.is_valid()
                    && iter.key().encoded_user_key() == encoded_user_key.as_slice()
                {
                    iter.next()?;
                }
            } else {
                iter.next()?;
            }
        }
        Ok(())
    }

    /// Check if the current decoded user key is within the upper bound.
    fn check_bound(&self) -> bool {
        match self.upper.as_ref() {
            Bound::Unbounded => true,
            Bound::Included(key) => self.user_key.as_slice() <= key.as_slice(),
            Bound::Excluded(key) => self.user_key.as_slice() < key.as_slice(),
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() & self.check_bound()
    }

    fn key(&self) -> &[u8] {
        if TS_ENABLED {
            &self.user_key
        } else {
            self.inner.key().into_inner()
        }
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if TS_ENABLED {
            // Skip all remaining versions of the current user key.
            // Compare encoded prefixes (not decoded) so keys with 0x00 bytes
            // match correctly through the memcomparable encoding layer.
            let current_encoded = self.encoded_user_key.clone();
            while self.inner.is_valid()
                && self.inner.key().encoded_user_key() == current_encoded.as_slice()
            {
                self.inner.next()?;
            }
            // Skip tombstones and invisible versions of the next user key(s)
            Self::skip_tombstones(&mut self.inner, self.read_ts)?;
            // Update cached user keys (decoded for key(), encoded for next())
            if self.inner.is_valid() {
                self.user_key = self.inner.key().decode_user_key();
                self.encoded_user_key = self.inner.key().encoded_user_key().to_vec();
            }
        } else {
            self.inner.next()?;
            // Legacy: skip tombstone values
            while self.inner.is_valid() && Self::is_tombstone_value(self.inner.value()) {
                self.inner.next()?;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an
/// error, `is_valid` should return false, and `next` should always return an error. ref: https://doc.rust-lang.org/std/iter/trait.FusedIterator.html,
/// about the naming, https://www.reddit.com/r/rust/comments/sbdb9t/i_finally_understand_the_naming_of_iteratorfuse/
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted");
        }

        if self.iter.is_valid()
            && let Err(e) = self.iter.next()
        {
            self.has_errored = true;
            return Err(e);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

/// Iterator wrapper that holds a [`ReadGuard`] for the duration of a scan.
/// This ensures the MVCC watermark reflects the active reader so compaction
/// does not GC versions the iterator might still need.
///
/// Implements [`StorageIterator`] by delegating to the inner
/// `FusedIterator<LsmIterator>`.  Cannot be constructed directly — obtained
/// from [`crate::lsm_storage::KvEngine::scan`].
pub struct ScanIterator {
    _guard: Option<ReadGuard>,
    iter: FusedIterator<LsmIterator>,
}

impl ScanIterator {
    pub(crate) fn new(iter: FusedIterator<LsmIterator>, guard: Option<ReadGuard>) -> Self {
        Self {
            _guard: guard,
            iter,
        }
    }

    pub(crate) fn into_inner(self) -> FusedIterator<LsmIterator> {
        self.iter
    }
}

impl StorageIterator for ScanIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
