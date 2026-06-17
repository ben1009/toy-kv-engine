use std::collections::BTreeMap;
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

        // Only scan entries with start <= user_key. Entries with start >
        // user_key can never cover it. The bound key uses max ts/ordinal
        // so that all entries sharing start == user_key are included.
        let bound_key = RangeTombstoneKey {
            start: Bytes::copy_from_slice(user_key),
            ts: u64::MAX,
            ordinal: u32::MAX,
        };
        for entry in self.raw.range(..=bound_key) {
            let key = entry.key();

            // Skip entries with ts > read_ts (not visible to this reader).
            if key.ts > read_ts {
                continue;
            }

            // Skip entries that cannot improve our best timestamp.
            if best_ts.is_some() && key.ts <= best_ts.unwrap() {
                continue;
            }

            // Check that user_key < end.
            let end = entry.value();
            if user_key < end.as_ref() {
                best_ts = Some(key.ts);
            }
        }

        best_ts
    }

    /// Check if any tombstone covers any key in `[start, end)` at `read_ts`.
    pub fn overlaps(&self, start: &[u8], end: &[u8], read_ts: u64) -> bool {
        // Empty or invalid range cannot overlap anything.
        if start >= end {
            return false;
        }
        // Only scan entries with start < end (query). Entries with start >= end
        // can never overlap the query range [start, end).
        let bound_key = RangeTombstoneKey {
            start: Bytes::copy_from_slice(end),
            ts: 0,
            ordinal: 0,
        };
        for entry in self.raw.range(..bound_key) {
            let key = entry.key();
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
    ) -> Box<dyn Iterator<Item = RangeTombstone> + 'a> {
        // Empty or invalid range cannot overlap anything — return immediately
        // without allocating bound_key or scanning the skipmap.
        if start >= end {
            return Box::new(std::iter::empty());
        }
        // Only scan entries with start < end (query). Entries with start >= end
        // can never overlap the query range [start, end).
        let bound_key = RangeTombstoneKey {
            start: Bytes::copy_from_slice(end),
            ts: 0,
            ordinal: 0,
        };
        Box::new(self.raw.range(..bound_key).filter_map(move |entry| {
            let key = entry.key();
            let tomb_end = entry.value();
            // Two ranges [a, b) and [c, d) overlap iff a < d && c < b.
            // The a < d check is handled by the range bound above.
            if start < tomb_end.as_ref() {
                Some(RangeTombstone {
                    start: key.start.clone(),
                    end: tomb_end.clone(),
                    ts: key.ts,
                })
            } else {
                None
            }
        }))
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

// ---------------------------------------------------------------------------
// Range tombstone fragmentation
//
// Raw tombstones may overlap. The fragmenter converts them into non-overlapping
// spans, each carrying the sorted set of tombstone timestamps that cover every
// key in that span. This enables O(log F) per-key lookups during scans and
// compaction instead of O(R) raw scans.
// ---------------------------------------------------------------------------

/// A non-overlapping range-tombstone span with all covering timestamps.
///
/// `covering_ts` is sorted ascending (oldest first). For any key `k` in
/// `[start, end)`, every timestamp in `covering_ts` covers `k`. Use
/// `partition_point(|ts| ts <= read_ts)` to find the newest visible covering
/// timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstoneFragment {
    pub start: Bytes,
    pub end: Bytes,
    pub covering_ts: Vec<u64>,
}

/// Fragment raw tombstones from a skipmap into non-overlapping spans.
///
/// Implements a sweep-line algorithm over sorted endpoints. Each unique
/// endpoint becomes a potential fragment boundary. Adjacent fragments with
/// identical `covering_ts` are coalesced.
pub fn fragment_range(raw: &Arc<SkipMap<RangeTombstoneKey, Bytes>>) -> Vec<RangeTombstoneFragment> {
    // Collect all (start, end, ts) triples.
    let mut tombstones: Vec<(Bytes, Bytes, u64)> = Vec::new();
    for entry in raw.iter() {
        let key = entry.key();
        let end = entry.value();
        tombstones.push((key.start.clone(), end.clone(), key.ts));
    }

    if tombstones.is_empty() {
        return Vec::new();
    }

    // Collect all unique endpoints and sort.
    let mut endpoints: Vec<Bytes> = Vec::with_capacity(tombstones.len() * 2);
    for (start, end, _) in &tombstones {
        endpoints.push(start.clone());
        endpoints.push(end.clone());
    }
    endpoints.sort();
    endpoints.dedup();

    // Sweep-line: walk adjacent endpoint pairs, track active tombstone timestamps.
    // BTreeMap<u64, u32> acts as a multiset — refcount per timestamp — so that
    // two tombstones sharing the same ts don't lose coverage when one ends
    // before the other.
    let mut active: BTreeMap<u64, u32> = BTreeMap::new();
    // Pre-index: for each endpoint, which tombstones start / end here.
    let mut starts_at: Vec<Vec<(usize, u64)>> = vec![Vec::new(); endpoints.len()];
    let mut ends_at: Vec<Vec<(usize, u64)>> = vec![Vec::new(); endpoints.len()];

    for (idx, (start, end, ts)) in tombstones.iter().enumerate() {
        let start_pos = endpoints
            .binary_search(start)
            .expect("start endpoint must exist in collected endpoints");
        let end_pos = endpoints
            .binary_search(end)
            .expect("end endpoint must exist in collected endpoints");
        starts_at[start_pos].push((idx, *ts));
        ends_at[end_pos].push((idx, *ts));
    }

    let mut fragments: Vec<RangeTombstoneFragment> = Vec::new();

    for i in 0..endpoints.len() - 1 {
        // Update active set at this endpoint.
        // Process ends BEFORE starts so that adjacent intervals [a,b) and [b,c)
        // with the same timestamp produce a continuous [a,c) fragment rather than
        // two separate fragments with a gap at b.
        for &(_, ts) in &ends_at[i] {
            if let Some(cnt) = active.get_mut(&ts) {
                *cnt -= 1;
                if *cnt == 0 {
                    active.remove(&ts);
                }
            }
        }
        for &(_, ts) in &starts_at[i] {
            *active.entry(ts).or_insert(0) += 1;
        }

        if active.is_empty() {
            continue;
        }

        let frag_start = endpoints[i].clone();
        let frag_end = endpoints[i + 1].clone();

        // Skip empty spans (shouldn't happen with deduped endpoints, but guard).
        if frag_start >= frag_end {
            continue;
        }

        // Coalesce with previous fragment if covering_ts are identical.
        // Compare iterators to avoid allocating a Vec when coalescing.
        if let Some(last) = fragments.last_mut()
            && last.end == frag_start
            && last.covering_ts.len() == active.len()
            && last
                .covering_ts
                .iter()
                .zip(active.keys())
                .all(|(a, b)| a == b)
        {
            last.end = frag_end;
            continue;
        }

        let covering_ts: Vec<u64> = active.keys().copied().collect();

        fragments.push(RangeTombstoneFragment {
            start: frag_start,
            end: frag_end,
            covering_ts,
        });
    }

    fragments
}

/// Merge multiple sorted fragment lists into a single non-overlapping list.
///
/// Each input list must be sorted by `start` and non-overlapping within itself.
/// The output is sorted by `start`, non-overlapping, with `covering_ts` being
/// the union of all input sources for each span. Adjacent fragments with
/// identical `covering_ts` are coalesced.
pub fn merge_fragment_lists(lists: &[&[RangeTombstoneFragment]]) -> Vec<RangeTombstoneFragment> {
    // Short-circuit: empty input or single list (already sorted, non-overlapping).
    if lists.len() <= 1 {
        return lists.first().map_or_else(Vec::new, |l| l.to_vec());
    }
    // Collect all fragments and sort by start.
    let all: Vec<&RangeTombstoneFragment> = lists.iter().copied().flatten().collect();
    if all.is_empty() {
        return Vec::new();
    }

    // Collect all unique endpoints and sort.
    let mut endpoints: Vec<Bytes> = Vec::with_capacity(all.len() * 2);
    for f in &all {
        endpoints.push(f.start.clone());
        endpoints.push(f.end.clone());
    }
    endpoints.sort();
    endpoints.dedup();

    // Pre-index: for each endpoint position, which fragments start / end here.
    // Store (fragment_idx, covering_ts_slice_idx) but since we need all ts
    // from a fragment, store the fragment index and expand later.
    let mut starts_at: Vec<Vec<usize>> = vec![Vec::new(); endpoints.len()];
    let mut ends_at: Vec<Vec<usize>> = vec![Vec::new(); endpoints.len()];

    for (idx, f) in all.iter().enumerate() {
        let start_pos = endpoints
            .binary_search(&f.start)
            .expect("start endpoint must exist in collected endpoints");
        let end_pos = endpoints
            .binary_search(&f.end)
            .expect("end endpoint must exist in collected endpoints");
        starts_at[start_pos].push(idx);
        ends_at[end_pos].push(idx);
    }

    // Sweep-line: track active timestamps with a refcount multiset.
    let mut active: BTreeMap<u64, u32> = BTreeMap::new();
    let mut fragments: Vec<RangeTombstoneFragment> = Vec::new();

    for i in 0..endpoints.len() - 1 {
        // Process ends BEFORE starts for correct adjacent-interval handling.
        for &frag_idx in &ends_at[i] {
            for &ts in &all[frag_idx].covering_ts {
                if let Some(cnt) = active.get_mut(&ts) {
                    *cnt -= 1;
                    if *cnt == 0 {
                        active.remove(&ts);
                    }
                }
            }
        }
        for &frag_idx in &starts_at[i] {
            for &ts in &all[frag_idx].covering_ts {
                *active.entry(ts).or_insert(0) += 1;
            }
        }

        if active.is_empty() {
            continue;
        }

        let frag_start = endpoints[i].clone();
        let frag_end = endpoints[i + 1].clone();

        if frag_start >= frag_end {
            continue;
        }

        // Coalesce with previous fragment if covering_ts are identical.
        if let Some(last) = fragments.last_mut()
            && last.end == frag_start
            && last.covering_ts.len() == active.len()
            && last
                .covering_ts
                .iter()
                .zip(active.keys())
                .all(|(a, b)| a == b)
        {
            last.end = frag_end;
            continue;
        }

        let covering_ts: Vec<u64> = active.keys().copied().collect();
        fragments.push(RangeTombstoneFragment {
            start: frag_start,
            end: frag_end,
            covering_ts,
        });
    }

    fragments
}

/// Iterator over sorted range-tombstone fragments for scan-path visibility checks.
///
/// Wraps a pre-built fragment list and provides O(log F + log T) per-key
/// lookups via binary search on fragments and their covering timestamps.
pub struct RangeTombstoneIterator {
    fragments: Arc<[RangeTombstoneFragment]>,
}

impl RangeTombstoneIterator {
    /// Create a new iterator over sorted, non-overlapping fragments.
    pub fn new(fragments: Arc<[RangeTombstoneFragment]>) -> Self {
        Self { fragments }
    }

    /// Find the newest covering tombstone timestamp for `user_key` at `read_ts`.
    ///
    /// Uses binary search on the sorted fragment list, then binary search on
    /// `covering_ts` within the matching fragment. O(log F + log T) where F
    /// is the fragment count and T is the max tombstone count per fragment.
    pub fn newest_covering_ts(&self, user_key: &[u8], read_ts: u64) -> Option<u64> {
        if self.fragments.is_empty() {
            return None;
        }

        // Binary search: find the last fragment with start <= user_key.
        let idx = self
            .fragments
            .partition_point(|f| f.start.as_ref() <= user_key);
        if idx == 0 {
            return None;
        }
        let frag = &self.fragments[idx - 1];
        // Verify user_key < end (half-open interval).
        if user_key >= frag.end.as_ref() {
            return None;
        }
        // Binary search within covering_ts for the newest ts <= read_ts.
        let ts_idx = frag.covering_ts.partition_point(|&ts| ts <= read_ts);
        if ts_idx > 0 {
            Some(frag.covering_ts[ts_idx - 1])
        } else {
            None
        }
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
        // Same start, different ts: smaller ts comes first (since it's used
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

    // -----------------------------------------------------------------------
    // Fragmenter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fragment_empty_input() {
        let set = RangeTombstoneSet::new();
        let frags = fragment_range(set.raw());
        assert!(frags.is_empty());
    }

    #[test]
    fn test_fragment_single_tombstone() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 10), 0);

        let frags = fragment_range(set.raw());
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"z");
        assert_eq!(frags[0].covering_ts, vec![10]);
    }

    #[test]
    fn test_fragment_non_overlapping() {
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"m", 10), 0);
        set.add(ts(b"p", b"z", 20), 1);

        let frags = fragment_range(set.raw());
        assert_eq!(frags.len(), 2);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"m");
        assert_eq!(frags[0].covering_ts, vec![10]);
        assert_eq!(frags[1].start.as_ref(), b"p");
        assert_eq!(frags[1].end.as_ref(), b"z");
        assert_eq!(frags[1].covering_ts, vec![20]);
    }

    #[test]
    fn test_fragment_overlapping() {
        // [a, z) @ 10 and [m, p) @ 20
        // Should produce: [a, m)@[10], [m, p)@[10, 20], [p, z)@[10]
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 10), 0);
        set.add(ts(b"m", b"p", 20), 1);

        let frags = fragment_range(set.raw());
        assert_eq!(frags.len(), 3);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"m");
        assert_eq!(frags[0].covering_ts, vec![10]);
        assert_eq!(frags[1].start.as_ref(), b"m");
        assert_eq!(frags[1].end.as_ref(), b"p");
        assert_eq!(frags[1].covering_ts, vec![10, 20]);
        assert_eq!(frags[2].start.as_ref(), b"p");
        assert_eq!(frags[2].end.as_ref(), b"z");
        assert_eq!(frags[2].covering_ts, vec![10]);
    }

    #[test]
    fn test_fragment_coalesce() {
        // Adjacent tombstones with same ts should coalesce.
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"m", 10), 0);
        set.add(ts(b"m", b"z", 10), 1);

        let frags = fragment_range(set.raw());
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"z");
        assert_eq!(frags[0].covering_ts, vec![10]);
    }

    #[test]
    fn test_fragment_nested() {
        // Inner tombstone with newer ts: [a, z)@10, [c, x)@20
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"z", 10), 0);
        set.add(ts(b"c", b"x", 20), 1);

        let frags = fragment_range(set.raw());
        assert_eq!(frags.len(), 3);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"c");
        assert_eq!(frags[0].covering_ts, vec![10]);
        assert_eq!(frags[1].start.as_ref(), b"c");
        assert_eq!(frags[1].end.as_ref(), b"x");
        assert_eq!(frags[1].covering_ts, vec![10, 20]);
        assert_eq!(frags[2].start.as_ref(), b"x");
        assert_eq!(frags[2].end.as_ref(), b"z");
        assert_eq!(frags[2].covering_ts, vec![10]);
    }

    #[test]
    fn test_fragment_overlapping_same_ts() {
        // Two tombstones with the same ts that partially overlap: [a,m)@10 and [f,z)@10.
        // Together they cover [a,z) — the fragmenter must NOT lose coverage at [m,z).
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"m", 10), 0);
        set.add(ts(b"f", b"z", 10), 1);

        let frags = fragment_range(set.raw());
        // Should coalesce into a single [a,z)@[10] fragment.
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"z");
        assert_eq!(frags[0].covering_ts, vec![10]);
    }

    #[test]
    fn test_fragment_overlapping_same_ts_different_endpoints() {
        // [a,m)@10, [f,p)@10, [h,z)@10 — all same ts, varying endpoints.
        // Together they cover [a,z).
        let set = RangeTombstoneSet::new();
        set.add(ts(b"a", b"m", 10), 0);
        set.add(ts(b"f", b"p", 10), 1);
        set.add(ts(b"h", b"z", 10), 2);

        let frags = fragment_range(set.raw());
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].start.as_ref(), b"a");
        assert_eq!(frags[0].end.as_ref(), b"z");
        assert_eq!(frags[0].covering_ts, vec![10]);
    }

    #[test]
    fn test_merge_fragment_lists() {
        // Two non-overlapping fragment lists from different sources.
        let frags1 = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"a"),
            end: Bytes::from_static(b"m"),
            covering_ts: vec![10],
        }];
        let frags2 = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"m"),
            end: Bytes::from_static(b"z"),
            covering_ts: vec![20],
        }];

        let merged = merge_fragment_lists(&[&frags1, &frags2]);
        // Adjacent with different ts — should NOT coalesce.
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].covering_ts, vec![10]);
        assert_eq!(merged[1].covering_ts, vec![20]);
    }

    #[test]
    fn test_merge_fragment_lists_overlapping() {
        // Overlapping fragments from two sources.
        let frags1 = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"a"),
            end: Bytes::from_static(b"z"),
            covering_ts: vec![10],
        }];
        let frags2 = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"m"),
            end: Bytes::from_static(b"p"),
            covering_ts: vec![20],
        }];

        let merged = merge_fragment_lists(&[&frags1, &frags2]);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].start.as_ref(), b"a");
        assert_eq!(merged[0].end.as_ref(), b"m");
        assert_eq!(merged[0].covering_ts, vec![10]);
        assert_eq!(merged[1].start.as_ref(), b"m");
        assert_eq!(merged[1].end.as_ref(), b"p");
        assert_eq!(merged[1].covering_ts, vec![10, 20]);
        assert_eq!(merged[2].start.as_ref(), b"p");
        assert_eq!(merged[2].end.as_ref(), b"z");
        assert_eq!(merged[2].covering_ts, vec![10]);
    }

    #[test]
    fn test_merge_coalesce_same_ts() {
        // Adjacent fragments with identical ts from different sources coalesce.
        let frags1 = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"a"),
            end: Bytes::from_static(b"m"),
            covering_ts: vec![10],
        }];
        let frags2 = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"m"),
            end: Bytes::from_static(b"z"),
            covering_ts: vec![10],
        }];

        let merged = merge_fragment_lists(&[&frags1, &frags2]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].start.as_ref(), b"a");
        assert_eq!(merged[0].end.as_ref(), b"z");
        assert_eq!(merged[0].covering_ts, vec![10]);
    }

    // -----------------------------------------------------------------------
    // RangeTombstoneIterator tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_iterator_empty() {
        let iter = RangeTombstoneIterator::new(Arc::from([]));
        assert_eq!(iter.newest_covering_ts(b"m", 100), None);
    }

    #[test]
    fn test_iterator_basic_lookup() {
        let frags = vec![
            RangeTombstoneFragment {
                start: Bytes::from_static(b"a"),
                end: Bytes::from_static(b"m"),
                covering_ts: vec![10],
            },
            RangeTombstoneFragment {
                start: Bytes::from_static(b"m"),
                end: Bytes::from_static(b"z"),
                covering_ts: vec![10, 20],
            },
        ];
        let iter = RangeTombstoneIterator::new(Arc::from(frags));

        // Key in first fragment.
        assert_eq!(iter.newest_covering_ts(b"b", 100), Some(10));
        // Key in second fragment — newest ts=20.
        assert_eq!(iter.newest_covering_ts(b"n", 100), Some(20));
        // Key at boundary.
        assert_eq!(iter.newest_covering_ts(b"m", 100), Some(20));
        // Key not covered.
        assert_eq!(iter.newest_covering_ts(b"z", 100), None);
        assert_eq!(iter.newest_covering_ts(b"a", 100), Some(10));
    }

    #[test]
    fn test_iterator_read_ts_visibility() {
        let frags = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"a"),
            end: Bytes::from_static(b"z"),
            covering_ts: vec![10, 20, 30],
        }];
        let iter = RangeTombstoneIterator::new(Arc::from(frags));

        // read_ts < all covering timestamps.
        assert_eq!(iter.newest_covering_ts(b"m", 5), None);
        // read_ts == first covering timestamp.
        assert_eq!(iter.newest_covering_ts(b"m", 10), Some(10));
        // read_ts between covering timestamps.
        assert_eq!(iter.newest_covering_ts(b"m", 15), Some(10));
        // read_ts == last covering timestamp.
        assert_eq!(iter.newest_covering_ts(b"m", 30), Some(30));
        // read_ts > all covering timestamps.
        assert_eq!(iter.newest_covering_ts(b"m", 100), Some(30));
    }

    #[test]
    fn test_iterator_key_before_all_fragments() {
        let frags = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"m"),
            end: Bytes::from_static(b"z"),
            covering_ts: vec![10],
        }];
        let iter = RangeTombstoneIterator::new(Arc::from(frags));
        assert_eq!(iter.newest_covering_ts(b"a", 100), None);
    }

    #[test]
    fn test_iterator_key_after_all_fragments() {
        let frags = vec![RangeTombstoneFragment {
            start: Bytes::from_static(b"a"),
            end: Bytes::from_static(b"m"),
            covering_ts: vec![10],
        }];
        let iter = RangeTombstoneIterator::new(Arc::from(frags));
        assert_eq!(iter.newest_covering_ts(b"z", 100), None);
    }

    #[test]
    fn test_iterator_key_at_fragment_boundary() {
        // Fragments [a, m)@[10] and [m, z)@[20].
        // Key "m" is NOT in [a, m) but IS in [m, z).
        let frags = vec![
            RangeTombstoneFragment {
                start: Bytes::from_static(b"a"),
                end: Bytes::from_static(b"m"),
                covering_ts: vec![10],
            },
            RangeTombstoneFragment {
                start: Bytes::from_static(b"m"),
                end: Bytes::from_static(b"z"),
                covering_ts: vec![20],
            },
        ];
        let iter = RangeTombstoneIterator::new(Arc::from(frags));
        assert_eq!(iter.newest_covering_ts(b"m", 100), Some(20));
    }
}
