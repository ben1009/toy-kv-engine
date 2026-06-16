use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

/// A range tombstone that hides all keys in `[start, end)` at timestamp `ts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Bytes,
    pub end: Bytes,
    pub ts: u64,
}

/// Key for the range-tombstone skipmap, ordered by `(start, ts, ordinal)`.
///
/// `ordinal` disambiguates multiple `DelRange` entries in a range-only batch
/// that share the same `(start, ts)`. It is assigned as a zero-based sequential
/// index within each range-only batch.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RangeTombstoneKey {
    pub start: Bytes,
    pub ts: u64,
    pub ordinal: u32,
}

/// A concurrent set of range tombstones backed by a `SkipMap`.
///
/// Tombstones are stored unfragmented (as inserted). Reads query the raw
/// entries directly. Fragmentation happens at read/flush/compaction time.
pub struct RangeTombstoneSet {
    /// Raw tombstone entries: key = `(start, ts, ordinal)`, value = `end`.
    raw: Arc<SkipMap<RangeTombstoneKey, Bytes>>,
    /// Approximate byte size of all tombstones in the set.
    approximate_size: AtomicUsize,
}

impl RangeTombstoneSet {
    /// Create an empty range-tombstone set.
    pub fn new() -> Self {
        Self {
            raw: Arc::new(SkipMap::new()),
            approximate_size: AtomicUsize::new(0),
        }
    }

    /// Insert a range tombstone. The `ordinal` disambiguates tombstones with
    /// the same `(start, ts)` from the same batch.
    pub fn add(&self, tombstone: RangeTombstone, ordinal: u32) {
        // Approximate per-entry overhead: ts(8) + ordinal(4) + Bytes headers
        // (~48 for start/end) + RangeTombstoneKey struct (~36) + SkipMap node
        // pointers (~64). This is intentionally approximate — it drives the
        // memtable freeze threshold, not an exact memory budget.
        let size = tombstone.start.len() + tombstone.end.len() + 80;
        let key = RangeTombstoneKey {
            start: tombstone.start,
            ts: tombstone.ts,
            ordinal,
        };
        self.raw.insert(key, tombstone.end);
        self.approximate_size.fetch_add(size, Ordering::Relaxed);
    }

    /// Find the newest covering tombstone timestamp for `user_key` at `read_ts`.
    ///
    /// Returns `Some(ts)` if a tombstone covers `user_key` (i.e.,
    /// `start <= user_key < end`) and `ts <= read_ts`. Returns the maximum
    /// such `ts` across all covering tombstones.
    pub fn newest_covering_ts(&self, user_key: &[u8], read_ts: u64) -> Option<u64> {
        let mut best_ts: Option<u64> = None;

        // The skipmap is ordered by (start, ts, ordinal). Once we see an
        // entry with start > user_key, all subsequent entries also have
        // start > user_key and cannot cover it, so we break.
        for entry in self.raw.iter() {
            let key = entry.key();

            // Entries with start > user_key can never cover user_key.
            // Since the skipmap is sorted by start, we can stop here.
            if key.start.as_ref() > user_key {
                break;
            }

            // Skip entries with ts > read_ts (not visible to this reader).
            if key.ts > read_ts {
                continue;
            }

            // Check that user_key < end.
            let end = entry.value();
            if user_key < end.as_ref() {
                best_ts = Some(best_ts.map_or(key.ts, |best| best.max(key.ts)));
            }
        }

        best_ts
    }

    /// Check if any tombstone covers any key in `[start, end)` at `read_ts`.
    pub fn overlaps(&self, start: &[u8], end: &[u8], read_ts: u64) -> bool {
        for entry in self.raw.iter() {
            let key = entry.key();
            // Since the skipmap is sorted by start, once start >= end (query),
            // no subsequent entries can overlap.
            if key.start.as_ref() >= end {
                break;
            }
            if key.ts > read_ts {
                continue;
            }
            let tomb_end = entry.value();
            // Two ranges [a, b) and [c, d) overlap iff a < d && c < b.
            if start < tomb_end.as_ref() {
                return true;
            }
        }

        false
    }

    /// Iterate all tombstones that overlap `[start, end)`, regardless of timestamp.
    pub fn iter_overlapping<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> impl Iterator<Item = RangeTombstone> + 'a {
        self.raw.iter().filter_map(move |entry| {
            let key = entry.key();
            let tomb_end = entry.value();
            // Two ranges [a, b) and [c, d) overlap iff a < d && c < b.
            if key.start.as_ref() < end && start < tomb_end.as_ref() {
                Some(RangeTombstone {
                    start: key.start.clone(),
                    end: tomb_end.clone(),
                    ts: key.ts,
                })
            } else {
                None
            }
        })
    }

    /// Return the approximate byte size of all tombstones in the set.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Return `true` if the set contains no tombstones.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Return the number of tombstones in the set.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Return a reference to the raw skipmap for building fragment views.
    pub fn raw(&self) -> &Arc<SkipMap<RangeTombstoneKey, Bytes>> {
        &self.raw
    }
}

impl Default for RangeTombstoneSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(start: &[u8], end: &[u8], ts: u64) -> RangeTombstone {
        RangeTombstone {
            start: Bytes::copy_from_slice(start),
            end: Bytes::copy_from_slice(end),
            ts,
        }
    }

    #[test]
    fn test_empty_set() {
        let set = RangeTombstoneSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert_eq!(set.newest_covering_ts(b"a", 100), None);
        assert!(!set.overlaps(b"a", b"z", 100));
        assert!(set.iter_overlapping(b"a", b"z").count() == 0);
    }

    #[test]
    fn test_add_and_basic_lookup() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 10), 0);

        assert!(!set.is_empty());
        assert_eq!(set.len(), 1);

        // Key inside range is covered.
        assert_eq!(set.newest_covering_ts(b"m", 100), Some(10));
        // Key at start is covered (half-open: start <= key < end).
        assert_eq!(set.newest_covering_ts(b"a", 100), Some(10));
        // Key at end is NOT covered.
        assert_eq!(set.newest_covering_ts(b"z", 100), None);
        // Key before start is NOT covered.
        assert_eq!(set.newest_covering_ts(b"`", 100), None);
    }

    #[test]
    fn test_read_ts_visibility() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 20), 0);

        // Reader at ts=20 sees the tombstone.
        assert_eq!(set.newest_covering_ts(b"m", 20), Some(20));
        // Reader at ts=19 does NOT see the tombstone.
        assert_eq!(set.newest_covering_ts(b"m", 19), None);
        // Reader at ts=30 sees the tombstone.
        assert_eq!(set.newest_covering_ts(b"m", 30), Some(20));
    }

    #[test]
    fn test_multiple_tombstones_newest_wins() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 10), 0);
        set.add(ts(b"a", b"z", 20), 1);
        set.add(ts(b"a", b"z", 15), 2);

        // Newest visible tombstone (ts=20) wins.
        assert_eq!(set.newest_covering_ts(b"m", 100), Some(20));
        // At read_ts=15, the ts=15 tombstone wins.
        assert_eq!(set.newest_covering_ts(b"m", 15), Some(15));
        // At read_ts=14, only ts=10 is visible.
        assert_eq!(set.newest_covering_ts(b"m", 14), Some(10));
    }

    #[test]
    fn test_overlapping_ranges() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"m", 10), 0);
        set.add(ts(b"f", b"z", 20), 1);

        // Key in intersection of both ranges.
        assert_eq!(set.newest_covering_ts(b"h", 100), Some(20));
        // Key only in first range.
        assert_eq!(set.newest_covering_ts(b"b", 100), Some(10));
        // Key only in second range.
        assert_eq!(set.newest_covering_ts(b"x", 100), Some(20));
    }

    #[test]
    fn test_overlaps_check() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"f", b"p", 10), 0);

        // Overlapping query.
        assert!(set.overlaps(b"a", b"g", 100));
        assert!(set.overlaps(b"m", b"z", 100));
        // Non-overlapping query.
        assert!(!set.overlaps(b"a", b"f", 100));
        assert!(!set.overlaps(b"p", b"z", 100));
    }

    #[test]
    fn test_iter_overlapping() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"m", 10), 0);
        set.add(ts(b"f", b"z", 20), 1);
        set.add(ts(b"p", b"t", 30), 2);

        // Query overlapping all three.
        let results: Vec<_> = set.iter_overlapping(b"g", b"q").collect();
        assert_eq!(results.len(), 3);

        // Query overlapping only one.
        let results: Vec<_> = set.iter_overlapping(b"n", b"o").collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].ts, 20);
    }

    #[test]
    fn test_approximate_size() {
        let set = RangeTombstoneSet::new();
        let before = set.approximate_size();
        set.add(ts(b"a", b"z", 10), 0);
        let after = set.approximate_size();
        assert!(after > before);
    }

    #[test]
    fn test_range_tombstone_key_ordering() {
        // Same start, different ts: higher ts comes first (since it's used
        // as-is, not inverted like point keys).
        let k1 = RangeTombstoneKey {
            start: Bytes::from_static(b"a"),
            ts: 10,
            ordinal: 0,
        };
        let k2 = RangeTombstoneKey {
            start: Bytes::from_static(b"a"),
            ts: 20,
            ordinal: 0,
        };
        assert!(k1 < k2);

        // Same (start, ts), different ordinal.
        let k3 = RangeTombstoneKey {
            start: Bytes::from_static(b"a"),
            ts: 10,
            ordinal: 1,
        };
        assert!(k1 < k3);

        // Different start.
        let k4 = RangeTombstoneKey {
            start: Bytes::from_static(b"b"),
            ts: 5,
            ordinal: 0,
        };
        assert!(k1 < k4);
    }

    #[test]
    fn test_boundary_equality() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 10), 0);

        // start == key: covered (start <= key).
        assert_eq!(set.newest_covering_ts(b"a", 100), Some(10));
        // end == key: NOT covered (key < end).
        assert_eq!(set.newest_covering_ts(b"z", 100), None);
    }
}
