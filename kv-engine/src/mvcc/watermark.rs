use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;

/// Tracks the oldest active read timestamp across all readers.
/// When there are no readers, the watermark is `None`, meaning
/// all committed versions below the latest commit ts can be GC'd.
///
/// Uses `DashMap` + `AtomicUsize` for concurrent `add_reader` / `remove_reader`
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

    /// Register a reader at `ts`. Fast-path uses a shared shard lock (`get`)
    /// for the common case where the entry already exists. Falls back to
    /// `entry()` with `and_modify` to handle the race where a concurrent
    /// thread inserted the entry between our `get()` and `entry()`.
    pub fn add_reader(&self, ts: u64) {
        if let Some(cnt) = self.readers.get(&ts) {
            cnt.fetch_add(1, Ordering::Relaxed);
        } else {
            self.readers
                .entry(ts)
                .and_modify(|cnt| {
                    cnt.fetch_add(1, Ordering::Relaxed);
                })
                .or_insert_with(|| AtomicUsize::new(1));
        }
    }

    /// Unregister a reader at `ts`. Lock-free on the hot path (counter
    /// decrement). Uses `remove_if` to atomically verify the count is still 0
    /// under the shard lock, preventing a race where a concurrent `add_reader`
    /// re-increments the counter between `fetch_sub` and `remove`.
    pub fn remove_reader(&self, ts: u64) {
        if let Some(cnt) = self.readers.get(&ts)
            && cnt.fetch_sub(1, Ordering::AcqRel) == 1
        {
            // Count was 1 before decrement → now 0.
            // Drop the read guard first to avoid deadlock with DashMap.
            drop(cnt);
            // Atomically verify count is still 0 under the shard lock.
            // Prevents a race where concurrent add_reader increments back to 1
            // before we remove the entry.
            self.readers
                .remove_if(&ts, |_, v| v.load(Ordering::Acquire) == 0);
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
