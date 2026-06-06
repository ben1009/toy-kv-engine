use std::collections::BTreeMap;

/// Tracks the oldest active read timestamp across all readers.
/// When there are no readers, the watermark is `None`, meaning
/// all committed versions below the latest commit ts can be GC'd.
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
