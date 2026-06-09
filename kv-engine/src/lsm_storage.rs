#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File},
    ops::Bound,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
};

use anyhow::{Context, Ok, Result, anyhow};
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
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
    lsm_iterator::{FusedIterator, LsmIterator, ScanIterator},
    manifest::{Manifest, ManifestRecord},
    mem_table::{self, MemTable},
    mvcc::LsmMvccInner,
    table::{FileObject, SsTable, SsTableBuilder, SsTableIterator},
    vlog::{KvKind, ValueLog, ValuePointer, ValueSeparationOptions},
};

// Re-export the BlockCache wrapper (TinyUFO-backed with reverse-index invalidation).
pub use crate::cache::BlockCache;

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
    /// Threshold in bytes for triggering a manifest snapshot. When the MANIFEST file
    /// exceeds this size, a snapshot of the current state is written to MANIFEST_SNAPSHOT
    /// and the manifest is truncated. Set to 0 to disable. Defaults to 4MB.
    pub manifest_snapshot_threshold_bytes: u64,
    /// Maximum number of entries in the block cache. Minimum 1.
    /// Defaults to 8192 (~32MB with 4KB blocks). Use 1024 for tests.
    pub block_cache_capacity: u64,
    /// Whether to backfill the block cache with newly produced blocks during
    /// flush and compaction. When enabled, recently flushed data stays warm in
    /// cache, eliminating the cache-miss cliff. Defaults to `true`.
    pub enable_cache_backfill: bool,
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
            manifest_snapshot_threshold_bytes: 0, // disabled by default in tests
            block_cache_capacity: 1792,
            enable_cache_backfill: true,
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
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity: 1792,
            enable_cache_backfill: true,
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
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity: 1792,
            enable_cache_backfill: true,
        }
    }
}

/// Aggregated cache statistics for the storage engine.
#[derive(Clone, Debug)]
pub struct CacheStats {
    /// Number of entries currently in the block cache.
    pub block_cache_entry_count: u64,
    /// Number of value cache hits (only available when value separation is enabled).
    pub value_cache_hit_count: u64,
    /// Number of value cache misses (only available when value separation is enabled).
    pub value_cache_miss_count: u64,
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
    pub(crate) state: ArcSwap<LsmStorageState>,
    // with the separate state_lock instead of rwlock only, the state can still be accessed while
    // the state_lock is locked, but with rwlock, that is impossible.
    // so the state_lock is only used in background tasks, for example, like compaction, flush to
    // imm_memtables, flush to l0, so the foreground tasks are not blocked
    // kind of similar to https://twitter.com/MarkCallaghanDB/status/1574425353564475394
    pub(crate) state_lock: Mutex<()>,
    /// Protects the active memtable during writes. `put()` holds a read lock (concurrent
    /// writes OK), `force_freeze_with_new_memtable()` holds a write lock (blocks writes
    /// during swap). Prevents writes to a memtable that has already been frozen+flushed.
    pub(crate) active_memtable_lock: parking_lot::RwLock<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<Arc<LsmMvccInner>>,
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
        while !self.inner.state.load().imm_memtables.is_empty() {
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

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<ScanIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.load().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.load().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    /// Flush all memtables (current + all immutable) to SSTs.
    /// Unlike `force_flush()` which only flushes one immutable memtable,
    /// this drains the entire queue.
    ///
    /// # Warning
    /// Inherits the same race conditions as [`force_flush`] — only use in
    /// tests or when no concurrent writes are happening.
    pub fn drain_flush(&self) -> Result<()> {
        self.force_flush()?;
        while !self.inner.state.load().imm_memtables.is_empty() {
            self.force_flush()?;
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

        // Batch manifest records for GC operations into a single fsync.
        if let Some(ref manifest) = self.inner.manifest
            && !results.is_empty()
        {
            let records: Vec<ManifestRecord> = results
                .iter()
                .map(|r| {
                    ManifestRecord::GcCompaction(r.old_file_id, r.new_file_id, r.keys_rewritten)
                })
                .collect();
            let state_lock = self.inner.state_lock.lock();
            manifest.add_records(&state_lock, &records)?;
            self.inner.maybe_snapshot_manifest(&state_lock)?;
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

    /// Get aggregated cache statistics for both block and value caches.
    pub fn cache_stats(&self) -> CacheStats {
        let (vc_hits, vc_misses) = self
            .inner
            .vlog
            .as_ref()
            .map_or((0, 0), |vlog| vlog.cache_hit_miss_counts());
        CacheStats {
            block_cache_entry_count: self.inner.block_cache.entry_count(),
            value_cache_hit_count: vc_hits,
            value_cache_miss_count: vc_misses,
        }
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
        let block_cache = Arc::new(BlockCache::new(
            options
                .block_cache_capacity
                .try_into()
                .unwrap_or(usize::MAX),
        ));
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
        // Maximum commit timestamp recovered from WAL batches and SST metadata.
        let mut max_commit_ts: u64 = 0;
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
            m.add_records_when_init(&[
                ManifestRecord::FormatVersion(crate::manifest::MANIFEST_FORMAT_VERSION),
                ManifestRecord::NewMemtable(state.memtable.id()),
            ])?;

            m
        } else {
            let ret = Manifest::recover(manifest_path).context("failed to recover manifest")?;

            // Validate format version: the first record must be FormatVersion(2)
            // or a Snapshot with format_version == 2. Pre-MVCC directories (no
            // format marker) are rejected to prevent silent data corruption.
            let detected_version = match ret.1.first() {
                Some(ManifestRecord::FormatVersion(v)) => *v,
                Some(ManifestRecord::Snapshot { format_version, .. }) => *format_version,
                None => anyhow::bail!(
                    "empty manifest file detected; \
                     please start with a fresh database"
                ),
                _ => anyhow::bail!(
                    "pre-MVCC directory detected (no format version marker); \
                     MVCC format (version 2) is required. \
                     Please start with a fresh database."
                ),
            };
            anyhow::ensure!(
                detected_version == crate::manifest::MANIFEST_FORMAT_VERSION,
                "unsupported manifest format version: got {}, expected {}; \
                 please start with a fresh database",
                detected_version,
                crate::manifest::MANIFEST_FORMAT_VERSION
            );

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
                    ManifestRecord::FormatVersion(_) => {
                        // Already validated above; nothing to replay.
                    }
                    ManifestRecord::Snapshot {
                        l0_sstables: snap_l0,
                        levels: snap_levels,
                        next_sst_id,
                        vlog_references: snap_vlog_refs,
                        imm_memtable_ids: snap_imm_ids,
                        format_version: _,
                    } => {
                        // Snapshot supersedes all prior records — reconstruct state directly
                        state.l0_sstables = snap_l0;
                        state.levels = snap_levels;
                        // next_sst_id is the next-to-allocate counter. max_id tracks the
                        // highest observed ID (incremented by 1 at the end of the loop).
                        // Use next_sst_id - 1 so the post-loop +1 yields next_sst_id.
                        max_id = next_sst_id.saturating_sub(1);
                        // Clear any previously recovered refs; snapshot has the authoritative set
                        recovered_vlog_refs.clear();
                        for (sst_id, vlog_ids) in snap_vlog_refs {
                            recovered_vlog_refs.insert(sst_id, vlog_ids);
                        }
                        // Preserve immutable memtable IDs from the snapshot so WAL
                        // recovery can rebuild them. These are frozen memtables that
                        // have not yet been flushed to SST.
                        im_memtables.clear();
                        for id in snap_imm_ids {
                            im_memtables.insert(id);
                            max_id = max_id.max(id);
                        }
                    }
                }
            }
            max_id += 1;
            // build imm_memtables and memtable
            if options.enable_wal {
                // just recover all to imm_memtables, then create a new memtable
                for id in im_memtables {
                    let wal_path = Self::path_of_wal_static(path, id);
                    let (m, wal_max_ts) = if vlog_enabled {
                        MemTable::recover_from_wal_vlog(id, wal_path)?
                    } else {
                        MemTable::recover_from_wal(id, wal_path)?
                    };
                    if wal_max_ts > max_commit_ts {
                        max_commit_ts = wal_max_ts;
                    }
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
                let sst_max_ts = sst.max_ts();
                if sst_max_ts > max_commit_ts {
                    max_commit_ts = sst_max_ts;
                }
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
            state: ArcSwap::from_pointee(state),
            state_lock: Mutex::new(()),
            active_memtable_lock: RwLock::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(max_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(Arc::new(LsmMvccInner::new(max_commit_ts))),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
            vlog,
            weak_self: std::sync::OnceLock::new(),
            gc_handles: Mutex::new(Vec::new()),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.load().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = self.state.load();
        let bloom_hash = crate::table::bloom::hash_key(key);
        // Pin read_ts once so memtable and SST lookups see the same snapshot.
        // The ReadGuard registers in the watermark so compaction won't GC
        // versions we might still read.
        let read_guard = self.mvcc.as_ref().map(|m| m.new_read_guard());
        let mvcc_read_ts = read_guard.as_ref().map(|g| g.read_ts());
        // With MVCC, encode the user key with u64::MAX to seek to the newest version.
        let lookup_key;
        let encoded = if self.mvcc.is_some() {
            lookup_key = crate::key::encode_internal_key(key, u64::MAX);
            lookup_key.as_slice()
        } else {
            key
        };
        // Check memtable first — route through resolve_vlog_value_bytes for
        // zero-copy inline slicing (refcount bump instead of heap allocation).
        if let Some((value, kind)) =
            self.lookup_memtable(&state, encoded, bloom_hash, mvcc_read_ts)?
        {
            return match kind {
                KvKind::ValuePointer => self.resolve_vlog_value_bytes(
                    key,
                    value.expect("ValuePointer kind must have a value"),
                ),
                KvKind::Inline | KvKind::Tombstone => Ok(value),
            };
        }
        // SST path — delegate to get_with_kind_inner which uses lookup_sst_raw.
        if let Some((value, kind)) =
            self.lookup_sst_raw(&state, encoded, bloom_hash, mvcc_read_ts)?
        {
            return match kind {
                KvKind::ValuePointer => self.resolve_vlog_value_bytes(
                    key,
                    value.expect("ValuePointer kind must have a value"),
                ),
                KvKind::Inline | KvKind::Tombstone => Ok(value),
            };
        }
        Ok(None)
    }

    /// Resolve a kind-prefixed `Bytes` value using zero-copy slicing.
    /// For ValuePointers, dereferences through the vLog with key verification.
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
            Some(KvKind::Tombstone) => Ok(None),
            _ => {
                // Inline value — strip the kind prefix with zero-copy slice
                if prefixed.len() == 1 {
                    // Legacy tombstone: single KvKind::Inline byte with no payload
                    Ok(None)
                } else {
                    Ok(Some(prefixed.slice(1..)))
                }
            }
        }
    }

    /// Parse a kind-prefixed raw value into (value, kind).
    /// Takes owned `Bytes` to enable zero-copy slicing for inline values.
    /// Check if a raw value represents a tombstone (single KvKind::Tombstone byte).
    fn is_tombstone_value(v: &Bytes) -> bool {
        v.len() == 1 && v[0] == crate::vlog::KvKind::Tombstone as u8
    }

    fn parse_value_kind(raw: Bytes) -> (Option<Bytes>, KvKind) {
        if raw.is_empty() {
            return (Some(raw), KvKind::Inline);
        }
        match KvKind::from_u8(raw[0]) {
            Some(KvKind::ValuePointer) => (Some(raw), KvKind::ValuePointer),
            Some(KvKind::Tombstone) => (None, KvKind::Tombstone),
            Some(KvKind::Inline) | None => {
                if raw.len() == 1 {
                    // Legacy tombstone: [KvKind::Inline] only
                    (None, KvKind::Inline)
                } else {
                    // Zero-copy slice: strip the 1-byte KvKind prefix
                    (Some(raw.slice(1..)), KvKind::Inline)
                }
            }
        }
    }

    /// Get a key from the storage, returning both the value and its KvKind.
    /// Used by GC to determine if a key still points to a specific vLog entry.
    pub(crate) fn get_with_kind(&self, key: &[u8]) -> Result<(Option<Bytes>, KvKind)> {
        let state = self.state.load();
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
        let bloom_hash = crate::table::bloom::hash_key(key);
        let read_guard = self.mvcc.as_ref().map(|m| m.new_read_guard());
        let mvcc_read_ts = read_guard.as_ref().map(|g| g.read_ts());
        let lookup_key;
        let encoded = if self.mvcc.is_some() {
            lookup_key = crate::key::encode_internal_key(key, u64::MAX);
            lookup_key.as_slice()
        } else {
            key
        };
        if let Some(result) = self.lookup_memtable(state, encoded, bloom_hash, mvcc_read_ts)? {
            return Ok(result);
        }
        if let Some(result) = self.lookup_sst_raw(state, encoded, bloom_hash, mvcc_read_ts)? {
            return Ok(result);
        }
        Ok((None, KvKind::Inline))
    }

    /// Shared memtable lookup used by `get()` and `get_with_kind_inner()`.
    /// Returns `Ok(None)` if the key is not found in any memtable.
    /// Returns `Ok(Some((value, kind)))` if found (value=None means tombstone).
    fn lookup_memtable(
        &self,
        state: &LsmStorageState,
        key: &[u8],
        bloom_hash: u32,
        mvcc_read_ts: Option<u64>,
    ) -> Result<Option<(Option<Bytes>, KvKind)>> {
        let vlog_enabled = self.vlog.is_some();
        if let Some(read_ts) = mvcc_read_ts {
            // MVCC path: key is encode(user_key, u64::MAX), need versioned lookup
            let user_key =
                crate::key::decode_user_key_cow(key).unwrap_or(std::borrow::Cow::Borrowed(key));
            if vlog_enabled {
                if let Some(raw) = state.memtable.get_versioned_raw(&user_key, read_ts) {
                    return Ok(Some(Self::parse_value_kind(raw)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some(raw) = m.get_versioned_raw(&user_key, read_ts) {
                        return Ok(Some(Self::parse_value_kind(raw)));
                    }
                }
            } else {
                if let Some(v) = state.memtable.get_versioned(&user_key, read_ts) {
                    if Self::is_tombstone_value(&v) {
                        return Ok(Some((None, KvKind::Inline)));
                    }
                    return Ok(Some((Some(v), KvKind::Inline)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some(v) = m.get_versioned(&user_key, read_ts) {
                        if Self::is_tombstone_value(&v) {
                            return Ok(Some((None, KvKind::Inline)));
                        }
                        return Ok(Some((Some(v), KvKind::Inline)));
                    }
                }
            }
        } else {
            // Non-MVCC path: exact key lookup
            if vlog_enabled {
                if let Some(raw) = state.memtable.get_raw_with_hash(key, bloom_hash) {
                    return Ok(Some(Self::parse_value_kind(raw)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some(raw) = m.get_raw_with_hash(key, bloom_hash) {
                        return Ok(Some(Self::parse_value_kind(raw)));
                    }
                }
            } else {
                if let Some(v) = state.memtable.get_with_hash(key, bloom_hash) {
                    if Self::is_tombstone_value(&v) {
                        return Ok(Some((None, KvKind::Inline)));
                    }
                    return Ok(Some((Some(v), KvKind::Inline)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some(v) = m.get_with_hash(key, bloom_hash) {
                        if Self::is_tombstone_value(&v) {
                            return Ok(Some((None, KvKind::Inline)));
                        }
                        return Ok(Some((Some(v), KvKind::Inline)));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Shared SST lookup for `get()` and `get_with_kind_inner()`.
    /// Searches L0 + leveled SSTs. Returns `Ok(None)` if not found.
    /// Returns `Ok(Some((value, kind)))` with the parsed value and kind.
    fn lookup_sst_raw(
        &self,
        state: &LsmStorageState,
        key: &[u8],
        bloom_hash: u32,
        mvcc_read_ts: Option<u64>,
    ) -> Result<Option<(Option<Bytes>, KvKind)>> {
        // For MVCC: accumulate the newest visible version across ALL levels
        // (L0 + L1+) before returning. A key's versions may be split across
        // levels after compaction, so we must check every level.
        let mut best: Option<(Bytes, u64)> = None;

        // L0 SSTs — may overlap, check each one
        if let Some(read_ts) = mvcc_read_ts {
            for id in state.l0_sstables.iter() {
                let s = match state.sstables.get(id) {
                    Some(s) => s,
                    None => continue,
                };
                // Skip SSTs that cannot contain a newer version than what
                // we already found (max_ts is the highest ts in this SST).
                if let Some((_, best_ts)) = best
                    && s.max_ts() <= best_ts
                {
                    continue;
                }
                if let Some((raw, found_key)) =
                    s.point_get_with_hash_and_key(key, bloom_hash, Some(read_ts))?
                {
                    let ts = crate::key::extract_ts(&found_key).unwrap_or(0);
                    if best.as_ref().is_none_or(|(_, best_ts)| ts > *best_ts) {
                        best = Some((raw, ts));
                    }
                }
            }
        } else {
            for id in state.l0_sstables.iter() {
                if let Some(s) = state.sstables.get(id)
                    && let Some(raw) = s.point_get_with_hash(key, bloom_hash)?
                {
                    return Ok(Some(Self::parse_value_kind(raw)));
                }
            }
        }
        // Leveled SSTs — with MVCC, accumulate the newest version across
        // all levels without returning early.
        let search_prefix = mvcc_read_ts.map(|_| {
            crate::key::encoded_user_key_prefix(key)
                .expect("key must be a valid encoded internal key when mvcc_read_ts is set")
        });
        for (_, sst_ids) in state.levels.iter() {
            if let Some(read_ts) = mvcc_read_ts {
                // Leveled SSTs (L1+) have non-overlapping user key ranges.
                // Binary search on user key prefix to find the rightmost
                // candidate, then scan left while the SST's last_key still
                // carries the same user key prefix — compaction may split a
                // key's versions across multiple adjacent SSTs.
                let search_prefix =
                    search_prefix.expect("search_prefix must be present when mvcc_read_ts is Some");
                let idx = sst_ids.partition_point(|id| {
                    state
                        .sstables
                        .get(id)
                        .expect("SST must exist in sstables map")
                        .first_key()
                        .encoded_user_key()
                        <= search_prefix
                });
                for i in (0..idx).rev() {
                    let sst = state
                        .sstables
                        .get(&sst_ids[i])
                        .expect("SST must exist in sstables map");
                    if sst.last_key().encoded_user_key() < search_prefix {
                        break;
                    }
                    // Remaining SSTs are sorted descending by key prefix;
                    // Skip SSTs that cannot contain a newer version.
                    // max_ts is NOT monotonically ordered across leveled SSTs
                    // (different SSTs cover different key ranges), so we must
                    // continue scanning rather than breaking.
                    if let Some((_, best_ts)) = best
                        && sst.max_ts() <= best_ts
                    {
                        continue;
                    }
                    if let Some((raw, found_key)) =
                        sst.point_get_with_hash_and_key(key, bloom_hash, Some(read_ts))?
                    {
                        let ts = crate::key::extract_ts(&found_key).unwrap_or(0);
                        if best.as_ref().is_none_or(|(_, best_ts)| ts > *best_ts) {
                            best = Some((raw, ts));
                        }
                    }
                }
            } else {
                let idx =
                    sst_ids.partition_point(|id| state.sstables[id].first_key().raw_ref() <= key);
                if idx == 0 {
                    continue;
                }
                let candidate_idx = idx - 1;
                if let Some(s) = state.sstables.get(&sst_ids[candidate_idx])
                    && let Some(raw) = s.point_get_with_hash(key, bloom_hash)?
                {
                    return Ok(Some(Self::parse_value_kind(raw)));
                }
            }
        }
        if let Some((raw, _)) = best {
            return Ok(Some(Self::parse_value_kind(raw)));
        }
        Ok(None)
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
        // Write lock prevents foreground put() from racing between check and write.
        let _mt_guard = self.active_memtable_lock.write();
        let state = self.state.load_full();
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
            let state = self.state.load_full();
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
        // memtable cannot be frozen. Write lock on active_memtable prevents
        // foreground put() from racing between re-verify and write.
        let _mt_guard = self.active_memtable_lock.write();
        let state = self.state.load_full();
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
                    let (current_val, current_kind) = Self::parse_value_kind(raw);
                    still_matches = Self::values_match(&current_val, current_kind, old, *old_kind);
                }
            } else if let Some(v) = state.memtable.get(key) {
                let current_val = if Self::is_tombstone_value(&v) {
                    None
                } else {
                    Some(v)
                };
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

    /// Write a batch of data into the storage.
    /// Canonicalizes duplicate user keys: only the last operation per key in
    /// the batch is written. When MVCC is enabled, all entries share a single
    /// commit timestamp.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        // Deduplicate: keep only the last operation per user key.
        let mut last_op = std::collections::HashMap::new();
        for (idx, record) in batch.iter().enumerate() {
            let key = match record {
                WriteBatchRecord::Del(k) => k.as_ref(),
                WriteBatchRecord::Put(k, _) => k.as_ref(),
            };
            last_op.insert(key, idx);
        }

        // Sort indices to preserve original insertion order.
        let mut indices: Vec<_> = last_op.values().copied().collect();
        indices.sort_unstable();

        {
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                // MVCC path: allocate a single commit_ts for the entire batch.
                let entries: Vec<(&[u8], &[u8], bool)> = indices
                    .iter()
                    .map(|&idx| match &batch[idx] {
                        WriteBatchRecord::Put(key, value) => {
                            (key.as_ref(), value.as_ref(), false)
                        }
                        WriteBatchRecord::Del(key) => (key.as_ref(), &[] as &[u8], true),
                    })
                    .collect();
                mvcc.write_batch(&entries, &state.memtable)?;
            } else {
                // Non-MVCC path: write raw user keys directly.
                let mut raw_data = vec![];
                for idx in indices {
                    match &batch[idx] {
                        WriteBatchRecord::Del(key) => {
                            raw_data.push((
                                KeySlice::from_slice(key.as_ref()),
                                vec![crate::vlog::KvKind::Tombstone as u8],
                            ));
                        }
                        WriteBatchRecord::Put(key, value) => {
                            let mut p = Vec::with_capacity(1 + value.as_ref().len());
                            p.push(crate::vlog::KvKind::Inline as u8);
                            p.extend_from_slice(value.as_ref());
                            raw_data.push((KeySlice::from_slice(key.as_ref()), p));
                        }
                    }
                }
                let refs: Vec<(KeySlice, &[u8])> =
                    raw_data.iter().map(|(k, v)| (*k, v.as_slice())).collect();
                state.memtable.put_raw_batch(&refs)?;
            }
        }

        self.try_freeze_memtable()
    }

    fn try_freeze_memtable(&self) -> Result<()> {
        let state = self.state.load();
        if state.memtable.approximate_size() >= self.options.target_sst_size {
            drop(state);
            let lock = &self.state_lock.lock();
            // reset approximate_size when force_freeze_memtable is called
            // check again
            let state = self.state.load();
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state);
                self.force_freeze_memtable(lock)?;
            }
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    /// When MVCC is enabled, encodes the key with an allocated commit timestamp.
    ///
    /// # Panics / Errors
    /// Rejects values that are exactly the tombstone marker byte (`[0x02]`),
    /// since those would be indistinguishable from a deletion marker.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        anyhow::ensure!(
            !(value.len() == 1 && value[0] == crate::vlog::KvKind::Tombstone as u8),
            "value must not be the tombstone marker byte (0x02)"
        );
        {
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                mvcc.write(key, value, &state.memtable)?;
            } else {
                state.memtable.put(key, value)?;
            }
        }

        self.try_freeze_memtable()
    }

    /// Remove a key from the storage by writing a tombstone marker.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        {
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                mvcc.write_tombstone(key, &state.memtable)?;
            } else {
                state.memtable.put_tombstone(key)?;
            }
        }
        self.try_freeze_memtable()
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

    /// Check if the manifest file exceeds the snapshot threshold and, if so, take a
    /// snapshot of the current state to MANIFEST_SNAPSHOT and truncate the manifest.
    /// No-op if the threshold is 0 (disabled) or manifest is None.
    pub(crate) fn maybe_snapshot_manifest(&self, state_lock: &MutexGuard<'_, ()>) -> Result<()> {
        let threshold = self.options.manifest_snapshot_threshold_bytes;
        if threshold == 0 {
            return Ok(());
        }
        let manifest = match self.manifest {
            Some(ref m) => m,
            None => return Ok(()),
        };
        if manifest.file_size()? < threshold {
            return Ok(());
        }

        // Capture current state snapshot (atomic load, no lock)
        let guard = self.state.load();
        let state = guard.as_ref();

        let mut vlog_references = Vec::new();
        if let Some(ref vlog) = self.vlog {
            // Sort SST IDs for deterministic snapshot serialization
            let mut sst_ids: Vec<usize> = state.sstables.keys().copied().collect();
            sst_ids.sort_unstable();
            for sst_id in sst_ids {
                if let Some(refs) = vlog.get_sst_references(sst_id)
                    && !refs.is_empty()
                {
                    vlog_references.push((sst_id, refs));
                }
            }
        }

        let record = ManifestRecord::Snapshot {
            l0_sstables: state.l0_sstables.clone(),
            levels: state.levels.clone(),
            next_sst_id: self.next_sst_id.load(std::sync::atomic::Ordering::Acquire),
            vlog_references,
            imm_memtable_ids: state.imm_memtables.iter().map(|m| m.id()).collect(),
            format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
        };
        drop(guard);

        manifest.snapshot(record)
    }

    fn force_freeze_with_new_memtable(&self, new_memtable: mem_table::MemTable) -> Result<()> {
        // Write lock blocks concurrent put() calls from writing to the old memtable
        // while we swap it out. This prevents writes to a memtable that has been
        // frozen and potentially flushed.
        let _guard = self.active_memtable_lock.write();
        let mut state = self.state.load().as_ref().clone();
        let m = std::mem::replace(&mut state.memtable, new_memtable.into());
        // make test happy. but why? kind of wired design decision
        state.imm_memtables.insert(0, m.clone());
        self.state.store(Arc::new(state));

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
            .expect("manifest initialized")
            .add_record(_state_lock_observer, ManifestRecord::NewMemtable(sst_id))?;

        self.maybe_snapshot_manifest(_state_lock_observer)
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();
        // since update state is just create a new one and replace it with the old one,
        // so this is a snapshot, no need to hold the lock for the whole process
        let memtable_to_flush = {
            let guard = self.state.load();
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
            builder.set_collect_blocks(self.options.enable_cache_backfill);
            memtable_to_flush.flush(&mut builder)?;
            let vlog_ids = builder.vlog_file_ids().to_vec();
            // Extract vLog index entries before build_with_backfill() consumes the builder
            let vlog_index_entries = builder.take_vlog_entries();
            let (sst, blocks) = builder.build_with_backfill(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            self.block_cache.backfill(sst_id, blocks);
            // Register vLog references
            if !vlog_ids.is_empty() {
                vlog.register_sst_references(sst_id, &vlog_ids);
            }
            // Persist the vLog index for faster GC
            if !vlog_index_entries.is_empty()
                && let Err(e) = vlog.save_index(vlog_file_id, vlog_index_entries)
            {
                eprintln!(
                    "warning: failed to save vLog index for {}: {}",
                    vlog_file_id, e
                );
            }
            // Sync the vLog directory to ensure the .vlog and .vidx directory
            // entries are durable. The main data directory is synced separately
            // via sync_dir() below, but the vLog subdirectory needs its own sync.
            if let std::result::Result::Ok(dir) = std::fs::File::open(&vlog.path) {
                let _ = dir.sync_all();
            }
            (sst, vlog_ids)
        } else {
            let mut builder = SsTableBuilder::new(self.options.block_size);
            builder.set_collect_blocks(self.options.enable_cache_backfill);
            memtable_to_flush.flush(&mut builder)?;
            let (sst, blocks) = builder.build_with_backfill(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            self.block_cache.backfill(sst_id, blocks);
            (sst, vec![])
        };

        {
            let mut state = self.state.load().as_ref().clone();

            state.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                state.l0_sstables.insert(0, sst.sst_id());
            } else {
                // in tiered compaction, Every time flush L0 SSTs,
                // should flush the SST into a tier placed at the front of the vector
                state.levels.insert(0, (sst.sst_id(), vec![sst.sst_id()]));
            }
            state.sstables.insert(sst.sst_id(), Arc::new(sst));
            self.state.store(Arc::new(state));
        }

        self.sync_dir()?;

        let manifest_record = if vlog_ids.is_empty() {
            ManifestRecord::Flush(sst_id)
        } else {
            ManifestRecord::FlushV2(sst_id, vlog_ids)
        };
        self.manifest
            .as_ref()
            .expect("manifest initialized")
            .add_record(&state_lock, manifest_record)?;

        // Check if manifest needs snapshotting after flush
        self.maybe_snapshot_manifest(&state_lock)?;

        // WAL GC: once the memtable is durably flushed to SST and recorded in
        // the manifest, the corresponding WAL file is no longer needed for
        // recovery. Remove it on a best-effort basis.
        //
        // Drop the memtable first to release the WAL file handle (the MemTable
        // owns the Wal which holds a BufWriter<File>). This prevents sharing
        // violations on Windows and ensures space is reclaimed promptly on Unix.
        drop(memtable_to_flush);

        if self.options.enable_wal {
            let wal_path = self.path_of_wal(sst_id);
            if let Err(e) = std::fs::remove_file(&wal_path) {
                // The file may already have been removed (e.g. by a crash
                // recovery that re-flushed and then cleaned up). Log and
                // continue — this is not a fatal error.
                if e.kind() != std::io::ErrorKind::NotFound {
                    eprintln!(
                        "warning: failed to remove WAL {}: {}",
                        wal_path.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<ScanIterator> {
        // Capture read_ts BEFORE state snapshot to ensure atomicity:
        // any write that commits before this point is visible; any write
        // after is not. The write path holds ts.lock() during commit, and
        // new_read_guard() also reads ts under that lock, so acquiring the
        // guard first guarantees read_ts ≤ every write that lands in the
        // state snapshot we load next.
        let read_guard = self.mvcc.as_ref().map(|m| m.new_read_guard());
        let state = self.state.load_full();
        let mvcc_read_ts = read_guard.as_ref().map(|g| g.read_ts());
        let mvcc_enabled = self.mvcc.is_some();

        // Encode bounds for MVCC using suffix-timestamp format.
        // Lower bound: encode(key, u64::MAX) → newest version of key, sorts
        //   before all real versions (inverted ts = 0x00*8).
        // Upper bound: encode(key, 0) → inverted ts = 0xFF*8, sorts after all
        //   real versions of key, ensuring the scan covers every version.
        //
        // For Excluded lower bound: encode(key, 0) → largest encoded position
        //   for key (inverted ts = 0xFF*8). Excluding it skips ALL versions of
        //   that user key, not just the ts=u64::MAX entry.
        let enc_lower;
        let enc_upper;
        let (physical_lower, physical_upper) = if mvcc_enabled {
            enc_lower = match lower {
                Bound::Included(k) => Bound::Included(crate::key::encode_internal_key(k, u64::MAX)),
                Bound::Excluded(k) => Bound::Excluded(crate::key::encode_internal_key(k, 0)),
                Bound::Unbounded => Bound::Unbounded,
            };
            enc_upper = match upper {
                Bound::Included(k) => Bound::Included(crate::key::encode_internal_key(k, 0)),
                Bound::Excluded(k) => Bound::Excluded(crate::key::encode_internal_key(k, 0)),
                Bound::Unbounded => Bound::Unbounded,
            };
            (
                enc_lower.as_ref().map(|v| v.as_slice()),
                enc_upper.as_ref().map(|v| v.as_slice()),
            )
        } else {
            (lower, upper)
        };

        // Helper to encode a lower bound key for SST/concat seek.
        let encode_seek = |k: &[u8]| -> Vec<u8> {
            if mvcc_enabled {
                crate::key::encode_internal_key(k, u64::MAX)
            } else {
                k.to_vec()
            }
        };

        // memtable
        let vlog = self.vlog.clone();
        let mut m_merge_iterators = vec![Box::new(state.memtable.scan_with_vlog(
            physical_lower,
            physical_upper,
            vlog.clone(),
        ))];
        for i in state.imm_memtables.iter() {
            let it = i.scan_with_vlog(physical_lower, physical_upper, vlog.clone());
            m_merge_iterators.push(Box::new(it));
        }
        let m_memo_iter = MergeIterator::create(m_merge_iterators);

        // l0 sstables
        let mut l0_iters = vec![];
        for i in state.l0_sstables.iter() {
            let t = state.sstables[i].clone();
            if !t.range_overlap(physical_lower, physical_upper) {
                continue;
            }

            let mut s = match lower {
                Bound::Included(lower_key) => {
                    let seek = encode_seek(lower_key);
                    SsTableIterator::create_and_seek_to_key(t.clone(), KeySlice::from_slice(&seek))?
                }
                Bound::Excluded(lower_key) => {
                    let seek = encode_seek(lower_key);
                    let mut s = SsTableIterator::create_and_seek_to_key(
                        t.clone(),
                        KeySlice::from_slice(&seek),
                    )?;
                    // Skip all versions of the excluded key
                    if s.is_valid() {
                        let seek_user_key = crate::key::encoded_user_key_prefix(&seek);
                        while s.is_valid()
                            && crate::key::encoded_user_key_prefix(s.key().raw_ref())
                                == seek_user_key
                        {
                            s.next()?;
                        }
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
                    Bound::Included(lower_key) => {
                        let seek = encode_seek(lower_key);
                        SstConcatIterator::create_and_seek_to_key_with_vlog(
                            ss_tables,
                            KeySlice::from_slice(&seek),
                            vlog.clone(),
                        )?
                    }
                    Bound::Excluded(lower_key) => {
                        let seek = encode_seek(lower_key);
                        let mut iter = SstConcatIterator::create_and_seek_to_key_with_vlog(
                            ss_tables,
                            KeySlice::from_slice(&seek),
                            vlog.clone(),
                        )?;
                        // Skip all versions of the excluded key
                        if iter.is_valid() {
                            let seek_user_key = crate::key::encoded_user_key_prefix(&seek);
                            while iter.is_valid()
                                && crate::key::encoded_user_key_prefix(iter.key().raw_ref())
                                    == seek_user_key
                            {
                                iter.next()?;
                            }
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
                    Bound::Included(lower_key) => {
                        let seek = encode_seek(lower_key);
                        SstConcatIterator::create_and_seek_to_key(
                            ss_tables,
                            KeySlice::from_slice(&seek),
                        )?
                    }
                    Bound::Excluded(lower_key) => {
                        let seek = encode_seek(lower_key);
                        let mut iter = SstConcatIterator::create_and_seek_to_key(
                            ss_tables,
                            KeySlice::from_slice(&seek),
                        )?;
                        // Skip all versions of the excluded key
                        if iter.is_valid() {
                            let seek_user_key = crate::key::encoded_user_key_prefix(&seek);
                            while iter.is_valid()
                                && crate::key::encoded_user_key_prefix(iter.key().raw_ref())
                                    == seek_user_key
                            {
                                iter.next()?;
                            }
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
        // Upper bound for LsmIterator uses decoded user keys (not encoded)
        let lit = LsmIterator::new(two_m, Self::into_vec(upper), mvcc_read_ts)?;

        Ok(ScanIterator::new(FusedIterator::new(lit), read_guard))
    }

    fn into_vec(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
        match b {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}
