#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File},
    ops::Bound,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
};

use anyhow::{Context, Ok, Result, anyhow};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
    block::Block,
    compact::{
        CompactionController, CompactionOptions, LeveledCompactionController,
        LeveledCompactionOptions, SimpleLeveledCompactionController,
        SimpleLeveledCompactionOptions, TieredCompactionController,
    },
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    key::KeySlice,
    lsm_iterator::{FusedIterator, LsmIterator},
    manifest::{Manifest, ManifestRecord},
    mem_table::{self, MemTable},
    mvcc::LsmMvccInner,
    table::{FileObject, SsTable, SsTableBuilder, SsTableIterator},
    vlog::{KvKind, ValueLog, ValuePointer, ValueSeparationOptions},
};

// TODO: try this one https://github.com/cloudflare/pingora/tree/main/tinyufo with bech later
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// A CAS entry: (key, old_value, old_kind, new_value, new_kind).
pub type CasEntry = (Vec<u8>, Vec<u8>, KvKind, Vec<u8>, KvKind);

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable. the memtable here do not need lock portection, since it is a
    /// crossbeam_skiplist::SkipMap if only operate the memtable, lock could be released as
    /// soon as possible
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions, vlog_enabled: bool) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(if vlog_enabled {
                MemTable::create_vlog(0)
            } else {
                MemTable::create(0)
            }),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
    /// Options for key-value separation (vLog). If `Some` with `enabled` true, large
    /// values are stored in a separate Value Log file. Defaults to `None` (disabled).
    pub value_separation: Option<ValueSeparationOptions>,
}

impl LsmStorageOptions {
    pub fn default_for_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
            value_separation: None,
        }
    }

    pub fn default_for_scan_flush_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
            value_separation: None,
        }
    }

    pub fn default_for_compaction_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
            value_separation: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    /// the state behind Arc is read only, modify is done by replace with a new one,
    /// so read will get a snapshot, only the memtable in the snapshot will see the latest change
    /// with skipmap support
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    // with the separate state_lock instead of rwlock only, the state can still be accessed while
    // the state_lock is locked, but with rwlock, that is impossible.
    // so the state_lock is only used in background tasks, for example, like compaction, flush to
    // imm_memtables, flush to l0, so the foreground tasks are not blocked
    // kind of similar to https://twitter.com/MarkCallaghanDB/status/1574425353564475394
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
    /// Value Log manager for key-value separation. `None` if value separation is disabled.
    pub(crate) vlog: Option<Arc<ValueLog>>,
    /// Weak reference to the owning `Arc<LsmStorageInner>`, set after construction.
    /// Allows background threads (e.g., async GC) to obtain a strong reference.
    pub(crate) weak_self: std::sync::OnceLock<std::sync::Weak<Self>>,
    /// Handles for background GC threads, joined during close().
    pub(crate) gc_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for `KvEngine`.
pub struct KvEngine {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In scan and flush)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In scan and flush)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In compaction)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In compaction)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for KvEngine {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl KvEngine {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        if let Some(f) = self.flush_thread.lock().take() {
            f.join().map_err(|e| anyhow!("{:?}", e))?;
        }
        self.compaction_notifier.send(()).ok();
        if let Some(f) = self.compaction_thread.lock().take() {
            f.join().map_err(|e| anyhow!("{:?}", e))?;
        }
        // Join all background GC threads before proceeding
        let handles = std::mem::take(&mut *self.inner.gc_handles.lock());
        for h in handles {
            let _ = h.join();
        }
        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;

            return Ok(());
        }

        // flush memtable to imm_memtable
        let new_id = self.inner.next_sst_id();
        let new_mt = if self.inner.vlog.is_some() {
            MemTable::create_vlog(new_id)
        } else {
            MemTable::create(new_id)
        };
        self.inner.force_freeze_with_new_memtable(new_mt)?;

        // flush all imm_memtable to disk
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if
    /// the directory does not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        // Set the weak self-reference so background threads (e.g., async GC) can
        // obtain a strong reference to the engine.
        let _ = inner.weak_self.set(Arc::downgrade(&inner));
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }

    /// Trigger garbage collection on all vLog files.
    /// Returns the number of files that were GC'd.
    pub fn trigger_gc(&self) -> Result<usize> {
        let Some(ref vlog) = self.inner.vlog else {
            return Ok(0);
        };
        let gc = crate::vlog::gc::GarbageCollector::new(
            vlog,
            &self.inner,
            vlog.options.gc_threshold_ratio,
        );
        let results = gc.gc_all()?;
        let count = results.len();

        // Write manifest records for GC operations
        for result in &results {
            if let Some(ref manifest) = self.inner.manifest {
                manifest.add_record(
                    &self.inner.state_lock.lock(),
                    ManifestRecord::GcCompaction(
                        result.old_file_id,
                        result.new_file_id,
                        result.keys_rewritten,
                    ),
                )?;
            }
        }

        // Attempt to reclaim vLog files that are no longer referenced by any SST.
        // Note: files with pending memtable CAS writes will still be referenced
        // (via the SST that hasn't been re-flushed yet), so they won't be deleted.
        let _ = vlog.reclaim_pending_deletions();

        Ok(count)
    }

    /// Get runtime statistics about the value log.
    pub fn vlog_stats(&self) -> Result<crate::vlog::ValueLogStats> {
        let Some(ref vlog) = self.inner.vlog else {
            return Err(anyhow::anyhow!("value separation is not enabled"));
        };
        vlog.stats()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if
    /// the directory does not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let vlog_enabled = options
            .value_separation
            .as_ref()
            .is_some_and(|vs| vs.enabled);
        let mut state = LsmStorageState::create(&options, vlog_enabled);
        // seems the cache is not cleaned forever ? just let lru do the gc job.
        // better refill the cache somehow after compaction
        let block_cache = Arc::new(BlockCache::new(1024));
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let path = path.as_ref();
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        // Initialize Value Log early so memtables can be created with vlog_enabled
        let value_separation = options.value_separation.clone().unwrap_or_default();
        let vlog_enabled = value_separation.enabled;
        let vlog = if value_separation.enabled {
            let vlog_path = path.join("vlog");
            if !vlog_path.exists() {
                fs::create_dir_all(&vlog_path)?;
            }
            Some(Arc::new(ValueLog::open(&vlog_path, value_separation)?))
        } else {
            None
        };

        let mut max_id = state.memtable.id();
        let manifest_path = path.join("MANIFEST");
        let mut recovered_vlog_refs: HashMap<usize, Vec<u32>> = HashMap::new();
        let manifest = if !manifest_path.exists() {
            if options.enable_wal {
                let id = state.memtable.id();
                let wal_path = Self::path_of_wal_static(path, id);
                state.memtable = Arc::new(if vlog_enabled {
                    MemTable::create_with_wal_vlog(id, wal_path)?
                } else {
                    MemTable::create_with_wal(id, wal_path)?
                })
            } else if vlog_enabled {
                state.memtable = Arc::new(MemTable::create_vlog(state.memtable.id()));
            }
            let m = Manifest::create(manifest_path).context("failed to create manifest")?;
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;

            m
        } else {
            let ret = Manifest::recover(manifest_path).context("failed to recover manifest")?;
            // need order by sst_id when recover
            let mut im_memtables = BTreeSet::new();
            // redo manifest log
            for record in ret.1 {
                match record {
                    ManifestRecord::NewMemtable(id) => {
                        im_memtables.insert(id);
                        max_id = std::cmp::max(max_id, id);
                    }
                    ManifestRecord::Flush(id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            // in tiered compaction, Every time flush L0 SSTs,
                            // should flush the SST into a tier placed at the front of the vector
                            state.levels.insert(0, (id, vec![id]));
                        }
                        im_memtables.remove(&id);
                        max_id = std::cmp::max(max_id, id);
                    }
                    ManifestRecord::Compaction(task, ids) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            ids.as_slice(),
                        );
                        state = new_state;
                        max_id = std::cmp::max(max_id, *ids.last().unwrap_or(&max_id));
                    }
                    ManifestRecord::FlushV2(id, vlog_ids) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                        im_memtables.remove(&id);
                        max_id = std::cmp::max(max_id, id);
                        if !vlog_ids.is_empty() {
                            recovered_vlog_refs.insert(id, vlog_ids);
                        }
                    }
                    ManifestRecord::CompactionV2(task, ids, vlog_ids) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            ids.as_slice(),
                        );
                        state = new_state;
                        max_id = std::cmp::max(max_id, *ids.last().unwrap_or(&max_id));
                        if !vlog_ids.is_empty() {
                            for &sst_id in &ids {
                                recovered_vlog_refs.insert(sst_id, vlog_ids.clone());
                            }
                        }
                    }
                    ManifestRecord::NewVlogFile(_id) | ManifestRecord::DeleteVlogFile(_id) => {
                        // vLog file lifecycle — will be handled in vLog recovery
                    }
                    ManifestRecord::GcCompaction(_old_id, _new_id, _count) => {
                        // GC compaction — references are updated via CAS + flush
                    }
                }
            }
            max_id += 1;
            // build imm_memtables and memtable
            if options.enable_wal {
                // just recover all to imm_memtables, then create a new memtable
                for id in im_memtables {
                    let wal_path = Self::path_of_wal_static(path, id);
                    let m = if vlog_enabled {
                        MemTable::recover_from_wal_vlog(id, wal_path)?
                    } else {
                        MemTable::recover_from_wal(id, wal_path)?
                    };
                    if !m.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(m));
                    }
                }
                let wal_path = Self::path_of_wal_static(path, max_id);
                state.memtable = Arc::new(if vlog_enabled {
                    MemTable::create_with_wal_vlog(max_id, wal_path)?
                } else {
                    MemTable::create_with_wal(max_id, wal_path)?
                });
            } else {
                state.memtable = Arc::new(if vlog_enabled {
                    MemTable::create_vlog(max_id)
                } else {
                    MemTable::create(max_id)
                });
            }
            ret.0
                .add_record_when_init(ManifestRecord::NewMemtable(max_id))?;

            // build sstables
            let ids = state
                .levels
                .iter()
                .flat_map(|(_, ids)| ids)
                .chain(state.l0_sstables.iter());
            for id in ids {
                // so the block_cache is shared by all sstables
                let sst = SsTable::open(
                    *id,
                    Some(block_cache.clone()),
                    FileObject::open(Self::path_of_sst_static(path, *id).as_path())
                        .context("failed to open SST")?,
                )?;
                state.sstables.insert(*id, Arc::new(sst));
            }

            ret.0
        };

        // Register vLog references recovered from manifest records (only for active SSTs)
        if let Some(ref vlog) = vlog {
            for (sst_id, vlog_ids) in &recovered_vlog_refs {
                if state.sstables.contains_key(sst_id) {
                    vlog.register_sst_references(*sst_id, vlog_ids);
                }
            }
            // Clean up orphaned vLog files left by a crash during GC or flush.
            // Safe here because all active SST references are now registered.
            // Collect vLog IDs from memtable entries first — a crash after GC
            // CAS but before flush leaves ValuePointer entries in the WAL-
            // recovered memtable that reference vLog files with no SST refs.
            let mut active_vlog_ids = state.memtable.collect_vlog_file_ids();
            for imm in &state.imm_memtables {
                active_vlog_ids.extend(imm.collect_vlog_file_ids());
            }
            if let Err(e) = vlog.cleanup_orphan_vlog_files(&active_vlog_ids) {
                eprintln!("vLog orphan cleanup error: {}", e);
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(max_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
            vlog,
            weak_self: std::sync::OnceLock::new(),
            gc_handles: Mutex::new(Vec::new()),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = self.state.read().clone();
        let vlog_enabled = self.vlog.is_some();

        if vlog_enabled {
            // Use get_raw to get kind-prefixed value, then resolve (zero-copy for inline)
            if let Some(raw) = state.memtable.get_raw(key) {
                return self.resolve_vlog_value_bytes(key, raw);
            }
            for m in state.imm_memtables.iter() {
                if let Some(raw) = m.get_raw(key) {
                    return self.resolve_vlog_value_bytes(key, raw);
                }
            }
        } else {
            if let Some(v) = state.memtable.get(key) {
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
            for m in state.imm_memtables.iter() {
                if let Some(v) = m.get(key) {
                    if v.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(v));
                }
            }
        }

        // L0 SSTs, from latest to earliest. Single-pass: check range + bloom
        // and create iterator immediately, avoiding intermediate Vec allocation.
        let key_hash = farmhash::hash32(key);
        for id in state.l0_sstables.iter() {
            if let Some(s) = state.sstables.get(id) {
                if key < s.first_key().raw_ref() || key > s.last_key().raw_ref() {
                    continue;
                }
                if let Some(b) = &s.bloom
                    && !b.may_contain(key_hash)
                {
                    continue;
                }
                let mut s_it =
                    SsTableIterator::create_and_seek_to_key(s.clone(), KeySlice::from_slice(key))?;
                if let Some(ref vlog) = self.vlog {
                    s_it.set_vlog(vlog.clone());
                }
                if s_it.is_valid() && s_it.key().raw_ref() == key {
                    let val = s_it.value();
                    if val.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(val)));
                }
            }
        }

        // L1-lmax SSTs: binary search on sorted, non-overlapping sst_ids.
        // At most one SST per level can contain the key, so O(log N) per level.
        for (_, sst_ids) in state.levels.iter() {
            let idx = sst_ids.partition_point(|id| state.sstables[id].first_key().raw_ref() <= key);

            if idx == 0 {
                continue;
            }
            let candidate_idx = idx - 1;
            if let Some(s) = state.sstables.get(&sst_ids[candidate_idx]) {
                if key < s.first_key().raw_ref() || key > s.last_key().raw_ref() {
                    continue;
                }
                if let Some(b) = &s.bloom
                    && !b.may_contain(key_hash)
                {
                    continue;
                }
                let mut s_it =
                    SsTableIterator::create_and_seek_to_key(s.clone(), KeySlice::from_slice(key))?;
                if let Some(ref vlog) = self.vlog {
                    s_it.set_vlog(vlog.clone());
                }
                if s_it.is_valid() && s_it.key().raw_ref() == key {
                    let val = s_it.value();
                    if val.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(val)));
                }
            }
        }

        Ok(None)
    }

    /// Resolve a kind-prefixed value from the memtable.
    /// If it's a ValuePointer, dereferences through the vLog with key verification.
    /// If it's Inline, strips the kind prefix and returns the value.
    fn resolve_vlog_value(&self, key: &[u8], prefixed: &[u8]) -> Result<Option<Bytes>> {
        if prefixed.is_empty() {
            return Ok(None);
        }
        match KvKind::from_u8(prefixed[0]) {
            Some(KvKind::ValuePointer) => {
                let ptr = ValuePointer::try_decode(&prefixed[1..]).ok_or_else(|| {
                    anyhow!(
                        "invalid ValuePointer in memtable: len={}, bytes={:?}",
                        prefixed.len(),
                        &prefixed[..prefixed.len().min(20)]
                    )
                })?;
                let vlog = self.vlog.as_ref().unwrap();
                let bytes = vlog.read(&ptr, key)?;
                Ok(Some(bytes))
            }
            _ => {
                // Inline value — strip the kind prefix
                if prefixed.len() == 1 {
                    // Tombstone
                    Ok(None)
                } else {
                    Ok(Some(Bytes::copy_from_slice(&prefixed[1..])))
                }
            }
        }
    }

    /// Resolve a kind-prefixed `Bytes` value from the memtable using zero-copy slicing.
    /// For inline values, returns `prefixed.slice(1..)` (cheap refcount bump) instead of copying.
    fn resolve_vlog_value_bytes(&self, key: &[u8], prefixed: Bytes) -> Result<Option<Bytes>> {
        if prefixed.is_empty() {
            return Ok(None);
        }
        match KvKind::from_u8(prefixed[0]) {
            Some(KvKind::ValuePointer) => {
                let ptr = ValuePointer::try_decode(&prefixed[1..]).ok_or_else(|| {
                    anyhow!(
                        "invalid ValuePointer in memtable: len={}, bytes={:?}",
                        prefixed.len(),
                        &prefixed[..prefixed.len().min(20)]
                    )
                })?;
                let vlog = self.vlog.as_ref().unwrap();
                let bytes = vlog.read(&ptr, key)?;
                Ok(Some(bytes))
            }
            _ => {
                // Inline value — strip the kind prefix with zero-copy slice
                if prefixed.len() == 1 {
                    // Tombstone
                    Ok(None)
                } else {
                    Ok(Some(prefixed.slice(1..)))
                }
            }
        }
    }

    /// Parse a kind-prefixed raw value into (value, kind).
    fn parse_value_kind(raw: &[u8]) -> (Option<Bytes>, KvKind) {
        if raw.is_empty() {
            return (None, KvKind::Inline);
        }
        match KvKind::from_u8(raw[0]) {
            Some(KvKind::ValuePointer) => (Some(Bytes::copy_from_slice(raw)), KvKind::ValuePointer),
            Some(KvKind::Inline) | None => {
                if raw.len() == 1 {
                    // Tombstone: [KvKind::Inline] only
                    (None, KvKind::Inline)
                } else {
                    (Some(Bytes::copy_from_slice(&raw[1..])), KvKind::Inline)
                }
            }
        }
    }

    /// Get a key from the storage, returning both the value and its KvKind.
    /// Used by GC to determine if a key still points to a specific vLog entry.
    pub(crate) fn get_with_kind(&self, key: &[u8]) -> Result<(Option<Bytes>, KvKind)> {
        let state = self.state.read().clone();
        self.get_with_kind_inner(&state, key)
    }

    /// Inner helper that operates on an already-cloned state snapshot.
    /// Used by both `get_with_kind` (public) and `compare_and_set_with_kind`
    /// (which holds a write lock and passes the state directly).
    fn get_with_kind_inner(
        &self,
        state: &LsmStorageState,
        key: &[u8],
    ) -> Result<(Option<Bytes>, KvKind)> {
        let vlog_enabled = self.vlog.is_some();

        // Memtable
        if vlog_enabled {
            if let Some(raw) = state.memtable.get_raw(key) {
                return Ok(Self::parse_value_kind(&raw));
            }
        } else if let Some(v) = state.memtable.get(key) {
            if v.is_empty() {
                return Ok((None, KvKind::Inline));
            }
            return Ok((Some(v), KvKind::Inline));
        }

        // Immutable memtables
        for m in state.imm_memtables.iter() {
            if vlog_enabled {
                if let Some(raw) = m.get_raw(key) {
                    return Ok(Self::parse_value_kind(&raw));
                }
            } else if let Some(v) = m.get(key) {
                if v.is_empty() {
                    return Ok((None, KvKind::Inline));
                }
                return Ok((Some(v), KvKind::Inline));
            }
        }

        // L0 SSTs, from latest to earliest. Single-pass: check range + bloom
        // and create iterator immediately, avoiding intermediate Vec allocation.
        let key_hash = farmhash::hash32(key);
        for id in state.l0_sstables.iter() {
            if let Some(s) = state.sstables.get(id) {
                if key < s.first_key().raw_ref() || key > s.last_key().raw_ref() {
                    continue;
                }
                if let Some(b) = &s.bloom
                    && !b.may_contain(key_hash)
                {
                    continue;
                }
                let mut s_it =
                    SsTableIterator::create_and_seek_to_key(s.clone(), KeySlice::from_slice(key))?;
                if let Some(ref vlog) = self.vlog {
                    s_it.set_vlog(vlog.clone());
                }
                if s_it.is_valid() && s_it.key().raw_ref() == key {
                    let raw = s_it.raw_value();
                    return Ok(Self::parse_value_kind(raw));
                }
            }
        }

        // L1-lmax SSTs: binary search on sorted, non-overlapping sst_ids.
        // At most one SST per level can contain the key, so O(log N) per level.
        for (_, sst_ids) in state.levels.iter() {
            let idx = sst_ids.partition_point(|id| state.sstables[id].first_key().raw_ref() <= key);

            if idx == 0 {
                continue;
            }
            let candidate_idx = idx - 1;
            if let Some(s) = state.sstables.get(&sst_ids[candidate_idx]) {
                if key < s.first_key().raw_ref() || key > s.last_key().raw_ref() {
                    continue;
                }
                if let Some(b) = &s.bloom
                    && !b.may_contain(key_hash)
                {
                    continue;
                }
                let mut s_it =
                    SsTableIterator::create_and_seek_to_key(s.clone(), KeySlice::from_slice(key))?;
                if let Some(ref vlog) = self.vlog {
                    s_it.set_vlog(vlog.clone());
                }
                if s_it.is_valid() && s_it.key().raw_ref() == key {
                    let raw = s_it.raw_value();
                    return Ok(Self::parse_value_kind(raw));
                }
            }
        }

        Ok((None, KvKind::Inline))
    }

    /// Atomic compare-and-swap with kind checking.
    /// Check whether the current value+kind matches the expected (old, old_kind).
    fn values_match(
        current_val: &Option<Bytes>,
        current_kind: KvKind,
        old: &[u8],
        old_kind: KvKind,
    ) -> bool {
        match (current_kind, old_kind) {
            (KvKind::Inline, KvKind::Inline) => match current_val {
                Some(v) => v.as_ref() == old,
                None => old.is_empty(),
            },
            (KvKind::ValuePointer, KvKind::ValuePointer) => match current_val {
                Some(v) => v.as_ref() == old,
                None => false,
            },
            _ => false,
        }
    }

    /// Acquires state_lock, does a full LSM lookup, and conditionally writes
    /// the new value to the memtable if the current value matches (old, old_kind).
    /// Returns true if the swap succeeded.
    pub(crate) fn compare_and_set_with_kind(
        &self,
        key: &[u8],
        old: &[u8],
        old_kind: KvKind,
        new: &[u8],
        new_kind: KvKind,
    ) -> Result<bool> {
        let _lock = self.state_lock.lock();
        let guard = self.state.write();
        let state = guard.as_ref().clone();
        let (current_val, current_kind) = self.get_with_kind_inner(&state, key)?;

        if !Self::values_match(&current_val, current_kind, old, old_kind) {
            return Ok(false);
        }

        let mut prefixed = Vec::with_capacity(1 + new.len());
        prefixed.push(new_kind as u8);
        prefixed.extend_from_slice(new);

        state.memtable.put_raw(key, &prefixed)?;
        Ok(true)
    }

    /// Batch CAS: atomically compare-and-swap multiple entries under a single
    /// `state_lock` acquisition. Returns a Vec<bool> indicating success per entry.
    ///
    /// Each entry is `(key, old_value, old_kind, new_value, new_kind)`.
    ///
    /// Uses optimistic two-phase concurrency control:
    /// - Phase 1 (read lock): perform all LSM lookups to identify matching candidates. Concurrent
    ///   reads are not blocked during this phase.
    /// - Phase 2 (write lock): re-verify matched candidates and write to memtable. Only matched
    ///   entries are re-checked, so the exclusive lock hold is minimal.
    ///
    /// NOTE: If the batch contains duplicate keys that both match, all report
    /// success but only the last value is stored. The GC use case never produces
    /// duplicate keys (vLog entries are unique per key).
    pub(crate) fn compare_and_set_batch_with_kind(
        &self,
        entries: &[CasEntry],
    ) -> Result<Vec<bool>> {
        let _lock = self.state_lock.lock();

        // Phase 1: Lookups under read lock — concurrent reads not blocked
        let mut candidates = Vec::with_capacity(entries.len());
        {
            let state = self.state.read().as_ref().clone();
            for (key, old, old_kind, _, _) in entries {
                let (current_val, current_kind) = self.get_with_kind_inner(&state, key)?;
                candidates.push(Self::values_match(
                    &current_val,
                    current_kind,
                    old,
                    *old_kind,
                ));
            }
        }

        // Phase 2: Re-verify matched candidates and write under exclusive lock.
        // Since `state_lock` is held, no flush or compaction can run, and the
        // memtable cannot be frozen. Any concurrent write between Phase 1 and
        // Phase 2 must have gone to `state.memtable`. Therefore, we only need
        // to check the memtable for newer values — no disk I/O required.
        let guard = self.state.write();
        let state = guard.as_ref().clone();
        let vlog_enabled = self.vlog.is_some();

        let mut results = vec![false; entries.len()];
        let mut writes: Vec<(KeySlice, Vec<u8>)> = Vec::with_capacity(entries.len());

        for (i, (key, old, old_kind, new, new_kind)) in entries.iter().enumerate() {
            if !candidates[i] {
                continue;
            }

            // Re-verify: only check the memtable for a newer value.
            // If the key is not in the memtable, no concurrent write changed
            // it since Phase 1, so the Phase 1 match still holds.
            let mut still_matches = true;
            if vlog_enabled {
                if let Some(raw) = state.memtable.get_raw(key) {
                    let (current_val, current_kind) = Self::parse_value_kind(&raw);
                    still_matches = Self::values_match(&current_val, current_kind, old, *old_kind);
                }
            } else if let Some(v) = state.memtable.get(key) {
                let current_val = if v.is_empty() { None } else { Some(v) };
                still_matches = Self::values_match(&current_val, KvKind::Inline, old, *old_kind);
            }

            if still_matches {
                let mut prefixed = Vec::with_capacity(1 + new.len());
                prefixed.push(*new_kind as u8);
                prefixed.extend_from_slice(new);
                writes.push((KeySlice::from_slice(key), prefixed));
                results[i] = true;
            }
        }

        if !writes.is_empty() {
            let raw_refs: Vec<(KeySlice, &[u8])> =
                writes.iter().map(|(k, v)| (*k, v.as_slice())).collect();
            state.memtable.put_raw_batch(&raw_refs)?;
        }

        Ok(results)
    }

    /// Write a batch of data into the storage. Implement in compaction.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let mut data = vec![];
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    data.push((KeySlice::from_slice(key.as_ref()), b"".as_slice()));
                }
                WriteBatchRecord::Put(key, value) => {
                    data.push((KeySlice::from_slice(key.as_ref()), value.as_ref()));
                }
            }
        }

        {
            let state = self.state.read();
            state.memtable.put_batch(data.as_slice())?;
        }

        self.try_freeze_memtable()
    }

    fn try_freeze_memtable(&self) -> Result<()> {
        let state = self.state.read();
        if state.memtable.approximate_size() >= self.options.target_sst_size {
            drop(state);
            let lock = &self.state_lock.lock();
            // reset approximate_size when force_freeze_memtable is called
            // check again
            let state = self.state.read();
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state);
                self.force_freeze_memtable(lock)?;
            }
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    /// As our memtable implementation only requires an immutable reference for put,
    /// you ONLY need to take the read lock on state in order to modify the memtable.
    /// This allows concurrent access to the memtable from multiple threads.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        {
            let state = self.state.read();
            state.memtable.put(key, value)?;
        }

        self.try_freeze_memtable()
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{id:05}.sst"))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{id:05}.wal"))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    // only needed when have files created or deleted
    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?
            .sync_all()
            .context("failed to sync dir")
    }

    fn force_freeze_with_new_memtable(&self, new_memtable: mem_table::MemTable) -> Result<()> {
        let mut guard = self.state.write();
        let mut state = guard.as_ref().clone();
        let m = std::mem::replace(&mut state.memtable, new_memtable.into());
        // make test happy. but why? kind of wired design decision
        state.imm_memtables.insert(0, m.clone());
        *guard = Arc::new(state);

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable,
    /// the `_state_lock_observer` will be dropped after `force_freeze_memtable` called
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let sst_id = self.next_sst_id();
        let vlog_enabled = self.vlog.is_some();
        let mem_table = if self.options.enable_wal {
            if vlog_enabled {
                mem_table::MemTable::create_with_wal_vlog(sst_id, self.path_of_wal(sst_id))?
            } else {
                mem_table::MemTable::create_with_wal(sst_id, self.path_of_wal(sst_id))?
            }
        } else if vlog_enabled {
            mem_table::MemTable::create_vlog(sst_id)
        } else {
            mem_table::MemTable::create(sst_id)
        };
        self.force_freeze_with_new_memtable(mem_table)?;

        self.sync_dir()?;

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(_state_lock_observer, ManifestRecord::NewMemtable(sst_id))
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();
        // since update state is just create a new one and replace it with the old one,
        // so this is a snapshot, no need to hold the lock for the whole process
        let memtable_to_flush = {
            let guard = self.state.read();
            match guard.imm_memtables.last() {
                Some(mt) => mt.clone(),
                None => return Ok(()),
            }
        };

        let sst_id = memtable_to_flush.id();

        // Build SST with optional vLog support
        let (sst, vlog_ids) = if let Some(ref vlog) = self.vlog {
            let vlog_file_id = vlog.next_file_id();
            let vs_opts = self.options.value_separation.as_ref().unwrap().clone();
            let vlog_builder = crate::vlog::ValueLogBuilder::create(
                vlog.path_of_file(vlog_file_id),
                vlog_file_id,
                vs_opts.clone(),
            )?;
            let mut builder =
                SsTableBuilder::new_with_vlog(self.options.block_size, vlog_builder, vs_opts);
            memtable_to_flush.flush(&mut builder)?;
            let vlog_ids = builder.vlog_file_ids().to_vec();
            let sst = builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            // Register vLog references
            if !vlog_ids.is_empty() {
                vlog.register_sst_references(sst_id, &vlog_ids);
            }
            (sst, vlog_ids)
        } else {
            let mut builder = SsTableBuilder::new(self.options.block_size);
            memtable_to_flush.flush(&mut builder)?;
            let sst = builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            (sst, vec![])
        };

        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();

            state.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                state.l0_sstables.insert(0, sst.sst_id());
            } else {
                // in tiered compaction, Every time flush L0 SSTs,
                // should flush the SST into a tier placed at the front of the vector
                state.levels.insert(0, (sst.sst_id(), vec![sst.sst_id()]));
            }
            state.sstables.insert(sst.sst_id(), Arc::new(sst));
            *guard = Arc::new(state);
        }

        self.sync_dir()?;

        let manifest_record = if vlog_ids.is_empty() {
            ManifestRecord::Flush(sst_id)
        } else {
            ManifestRecord::FlushV2(sst_id, vlog_ids)
        };
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, manifest_record)?;

        // WAL GC: once the memtable is durably flushed to SST and recorded in
        // the manifest, the corresponding WAL file is no longer needed for
        // recovery. Remove it on a best-effort basis.
        if self.options.enable_wal {
            let wal_path = self.path_of_wal(sst_id);
            if let Err(e) = std::fs::remove_file(&wal_path) {
                // The file may already have been removed (e.g. by a crash
                // recovery that re-flushed and then cleaned up). Log and
                // continue — this is not a fatal error.
                eprintln!(
                    "warning: failed to remove WAL {}: {}",
                    wal_path.display(),
                    e
                );
            }
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = self.state.read().clone();

        // memtable
        let vlog = self.vlog.clone();
        let mut m_merge_iterators = vec![Box::new(state.memtable.scan_with_vlog(
            lower,
            upper,
            vlog.clone(),
        ))];
        for i in state.imm_memtables.iter() {
            let it = i.scan_with_vlog(lower, upper, vlog.clone());
            m_merge_iterators.push(Box::new(it));
        }
        let m_memo_iter = MergeIterator::create(m_merge_iterators);

        // l0 sstables
        let mut l0_iters = vec![];
        for i in state.l0_sstables.iter() {
            let t = state.sstables[i].clone();
            if !t.range_overlap(lower, upper) {
                continue;
            }

            let mut s = match lower {
                Bound::Included(lower) => {
                    SsTableIterator::create_and_seek_to_key(t.clone(), KeySlice::from_slice(lower))?
                }
                Bound::Excluded(lower) => {
                    // if t.first_key().as_key_slice() >= KeySlice::from_slice(lower)
                    //     || t.last_key().as_key_slice() <= KeySlice::from_slice(lower)
                    // {
                    //     continue;
                    // }
                    let mut s = SsTableIterator::create_and_seek_to_key(
                        t.clone(),
                        KeySlice::from_slice(lower),
                    )?;
                    if s.is_valid() && s.key().raw_ref() == lower {
                        s.next()?;
                    }

                    s
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(t.clone())?,
            };
            if let Some(ref vlog) = self.vlog {
                s.set_vlog(vlog.clone());
            }
            l0_iters.push(Box::new(s));
        }
        let m_l0_iter = MergeIterator::create(l0_iters);
        let two_l0_iter = TwoMergeIterator::create(m_memo_iter, m_l0_iter)?;

        // l1-lmax sstables
        let mut concat_iters = vec![];
        for (_, sst_ids) in &state.levels {
            let mut ss_tables = vec![];
            for i in sst_ids {
                let t = state.sstables[i].clone();
                ss_tables.push(t);
            }
            let concat_iter = if let Some(ref vlog) = self.vlog {
                match lower {
                    Bound::Included(lower) => SstConcatIterator::create_and_seek_to_key_with_vlog(
                        ss_tables,
                        KeySlice::from_slice(lower),
                        vlog.clone(),
                    )?,
                    Bound::Excluded(lower) => {
                        let mut iter = SstConcatIterator::create_and_seek_to_key_with_vlog(
                            ss_tables,
                            KeySlice::from_slice(lower),
                            vlog.clone(),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == lower {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first_with_vlog(
                        ss_tables,
                        vlog.clone(),
                    )?,
                }
            } else {
                match lower {
                    Bound::Included(lower) => SstConcatIterator::create_and_seek_to_key(
                        ss_tables,
                        KeySlice::from_slice(lower),
                    )?,
                    Bound::Excluded(lower) => {
                        let mut iter = SstConcatIterator::create_and_seek_to_key(
                            ss_tables,
                            KeySlice::from_slice(lower),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == lower {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ss_tables)?,
                }
            };
            concat_iters.push(Box::new(concat_iter));
        }
        let m_iter = MergeIterator::create(concat_iters);
        let two_m = TwoMergeIterator::create(two_l0_iter, m_iter)?;
        let lit = LsmIterator::new(two_m, Self::into_vec(upper))?;

        Ok(FusedIterator::new(lit))
    }

    fn into_vec(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
        match b {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}
