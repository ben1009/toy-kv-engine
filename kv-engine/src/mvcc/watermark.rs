use std::collections::BTreeMap;

/// Tracks the oldest active read timestamp across all readers.
/// When there are no readers, the watermark is `None`, meaning
/// all committed versions below the latest commit ts can be GC'd.
#[derive(Debug)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_insert(0) += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        if let Some(cnt) = self.readers.get_mut(&ts) {
            *cnt -= 1;
            if *cnt == 0 {
                self.readers.remove(&ts);
            }
        }
    }

    /// Returns the smallest active read timestamp, or `None` if no readers exist.
    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().next().copied()
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
        let mut wm = Watermark::new();
        wm.add_reader(10);
        assert_eq!(wm.watermark(), Some(10));
        wm.remove_reader(10);
        assert_eq!(wm.watermark(), None);
    }

    #[test]
    fn test_watermark_multiple_readers() {
        let mut wm = Watermark::new();
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
        let mut wm = Watermark::new();
        wm.add_reader(5);
        wm.add_reader(5); // same ts, count=2
        assert_eq!(wm.watermark(), Some(5));
        wm.remove_reader(5); // count=1
        assert_eq!(wm.watermark(), Some(5));
        wm.remove_reader(5); // count=0, removed
        assert_eq!(wm.watermark(), None);
    }
}
