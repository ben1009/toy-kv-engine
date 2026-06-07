use std::ops::Bound;

use anyhow::{Ok, Result, bail};

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    key::TS_ENABLED,
    mem_table::MemTableIterator,
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
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Vec<u8>>) -> Result<Self> {
        let mut iter = iter;
        // Skip tombstones at the start
        Self::skip_tombstones(&mut iter)?;

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
        })
    }

    /// Skip entries with empty values (tombstones). When MVCC is enabled,
    /// skip all versions of a dead user key.
    fn skip_tombstones(iter: &mut LsmIteratorInner) -> Result<()> {
        while iter.is_valid() {
            if !iter.value().is_empty() {
                return Ok(());
            }
            if TS_ENABLED {
                // Skip all versions of this dead user key
                let dead_key = iter.key().encoded_user_key().to_vec();
                while iter.is_valid() && iter.key().encoded_user_key() == dead_key.as_slice() {
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
            // match correctly through the escaping layer.
            let current_encoded = self.encoded_user_key.clone();
            while self.inner.is_valid()
                && self.inner.key().encoded_user_key() == current_encoded.as_slice()
            {
                self.inner.next()?;
            }
            // Skip tombstones of the next user key(s)
            Self::skip_tombstones(&mut self.inner)?;
            // Update cached user keys (decoded for key(), encoded for next())
            if self.inner.is_valid() {
                self.user_key = self.inner.key().decode_user_key();
                self.encoded_user_key = self.inner.key().encoded_user_key().to_vec();
            }
        } else {
            self.inner.next()?;
            // Legacy: skip empty values
            while self.inner.is_valid() && self.inner.value().is_empty() {
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
