use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;

/// Tracks the oldest active read timestamp across all readers.
/// When there are no readers, the watermark is `None`, meaning
/// all committed versions below the latest commit ts can be GC'd.
///
/// Uses `DashMap` + `AtomicUsize` for lock-free `add_reader` / `remove_reader`
/// so that the read path does not need an exclusive lock.
#[derive(Debug)]
pub struct Watermark {
    readers: DashMap<u64, AtomicUsize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: DashMap::new(),
        }
    }

    /// Register a reader at `ts`. Uses `entry()` to atomically obtain or
    /// insert the counter, then unconditionally increments it. This avoids
    /// the TOCTOU race of `get()` + `or_insert()` where two concurrent
    /// threads could both see "no entry" and one's increment gets lost.
    pub fn add_reader(&self, ts: u64) {
        self.readers
            .entry(ts)
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Unregister a reader at `ts`. Lock-free on the hot path (counter
    /// decrement). Removes the entry when the count reaches zero.
    pub fn remove_reader(&self, ts: u64) {
        if let Some(cnt) = self.readers.get(&ts)
            && cnt.fetch_sub(1, Ordering::AcqRel) == 1
        {
            // Count was 1 before decrement → now 0 → remove.
            // Drop the read guard first to avoid deadlock with DashMap.
            drop(cnt);
            self.readers.remove(&ts);
        }
    }

    /// Returns the smallest active read timestamp, or `None` if no readers exist.
    /// Only called during compaction / GC, not on the read hot path.
    pub fn watermark(&self) -> Option<u64> {
        self.readers
            .iter()
            .filter(|r| r.load(Ordering::Relaxed) > 0)
            .map(|r| *r.key())
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_empty() {
        let wm = Watermark::new();
        assert_eq!(wm.watermark(), None);
    }

    #[test]
    fn test_watermark_single_reader() {
        let wm = Watermark::new();
        wm.add_reader(10);
        assert_eq!(wm.watermark(), Some(10));
        wm.remove_reader(10);
        assert_eq!(wm.watermark(), None);
    }

    #[test]
    fn test_watermark_multiple_readers() {
        let wm = Watermark::new();
        wm.add_reader(20);
        wm.add_reader(10);
        wm.add_reader(15);
        assert_eq!(wm.watermark(), Some(10)); // oldest
        wm.remove_reader(10);
        assert_eq!(wm.watermark(), Some(15));
        wm.remove_reader(15);
        assert_eq!(wm.watermark(), Some(20));
        wm.remove_reader(20);
        assert_eq!(wm.watermark(), None);
    }

    #[test]
    fn test_watermark_duplicate_reader_ts() {
        let wm = Watermark::new();
        wm.add_reader(5);
        wm.add_reader(5); // same ts, count=2
        assert_eq!(wm.watermark(), Some(5));
        wm.remove_reader(5); // count=1
        assert_eq!(wm.watermark(), Some(5));
        wm.remove_reader(5); // count=0, removed
        assert_eq!(wm.watermark(), None);
    }
}
