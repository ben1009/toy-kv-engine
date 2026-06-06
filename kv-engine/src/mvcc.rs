pub mod txn;
mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use parking_lot::Mutex;

use self::{txn::Transaction, watermark::Watermark};
use crate::{key::encode_internal_key, lsm_storage::LsmStorageInner, mem_table::MemTable};

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    /// Allocate a commit timestamp under the write lock and write to the memtable.
    pub fn write(
        &self,
        user_key: &[u8],
        value: &[u8],
        memtable: &MemTable,
    ) -> Result<(), anyhow::Error> {
        let _write_guard = self.write_lock.lock();
        let commit_ts = {
            let mut ts = self.ts.lock();
            ts.0 += 1;
            ts.0
        };
        let encoded_key = encode_internal_key(user_key, commit_ts);
        memtable.put(&encoded_key, value)?;
        Ok(())
    }

    /// Get a read timestamp (the latest committed ts).
    /// This does NOT add a reader to the watermark — that's done via ReadGuard.
    pub fn read_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, serializable: bool) -> Arc<Transaction> {
        unimplemented!()
    }
}
