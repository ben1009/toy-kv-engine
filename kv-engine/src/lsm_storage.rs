use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File},
    ops::Bound,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
};

use anyhow::{Context, Result, anyhow};
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};
use serde::{Deserialize, Serialize};

use crate::{
    compact::{
        CompactionController, CompactionOptions, CompactionTask, LeveledCompactionController,
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

/// Versioned CAS entry: `(user_key, ts, old_value, old_kind, new_value, new_kind)`.
pub type VersionedCasEntry = (Vec<u8>, u64, Vec<u8>, KvKind, Vec<u8>, KvKind);

/// Lookup result: `(value, kind, found_internal_key, version_ts)`.
/// `version_ts` is the MVCC timestamp of the found version (0 when MVCC disabled).
type LookupResult = (Option<Bytes>, KvKind, Bytes, u64);

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
    /// Range-only SSTs per level. Each entry is `(level_number, Vec<sst_id>)`.
    /// Range-only SSTs contain only range-tombstone blocks, no point data.
    /// They are not included in concat iterators but are checked during
    /// compaction input selection and level-size computation.
    pub range_only_ssts: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
    /// Range delete: hides all keys in `[start, end)`.
    /// MVP: range-only batches only (no mixed point/range batches).
    DelRange(T, T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions, vlog_enabled: bool) -> Self {
        // Both leveled and simple-leveled compaction need per-level vectors;
        // tiered needs none; no-compaction needs a single level.
        let init_levels = |opts: &CompactionOptions| -> Vec<(usize, Vec<usize>)> {
            match opts {
                CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
                | CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                    max_levels, ..
                }) => (1..=*max_levels).map(|level| (level, Vec::new())).collect(),
                CompactionOptions::Tiered(_) => Vec::new(),
                CompactionOptions::NoCompaction => vec![(1, Vec::new())],
            }
        };
        let levels = init_levels(&options.compaction_options);
        let range_only_ssts = init_levels(&options.compaction_options);
        Self {
            memtable: Arc::new(MemTable::create(0, vlog_enabled)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            range_only_ssts,
            sstables: Default::default(),
        }
    }
}

/// Mutable state accumulated during manifest replay in `open()`.
///
/// Encapsulates the various counters, sets, and maps that get updated
/// as each `ManifestRecord` is replayed, keeping `open()` concise.
struct ManifestRecoveryState<'a> {
    state: &'a mut LsmStorageState,
    compaction_controller: &'a CompactionController,
    max_id: usize,
    im_memtables: BTreeSet<usize>,
    recovered_vlog_refs: HashMap<usize, Vec<u32>>,
    recovered_compaction_filters: BTreeMap<u64, InstalledCompactionFilter>,
    next_compaction_filter_id: u64,
    /// Reusable buffer for collecting input SST IDs during CompactionV3
    /// processing, avoiding repeated heap allocations.
    input_ids_buf: Vec<usize>,
}

impl ManifestRecoveryState<'_> {
    /// Replay a single manifest record, updating the recovery state.
    fn replay_manifest_record(&mut self, record: ManifestRecord) -> Result<()> {
        match record {
            ManifestRecord::NewMemtable(id) => {
                self.im_memtables.insert(id);
                self.max_id = std::cmp::max(self.max_id, id);
            }
            ManifestRecord::Flush(id) => {
                if self.compaction_controller.flush_to_l0() {
                    self.state.l0_sstables.insert(0, id);
                } else {
                    self.state.levels.insert(0, (id, vec![id]));
                }
                self.im_memtables.remove(&id);
                self.max_id = std::cmp::max(self.max_id, id);
            }
            ManifestRecord::Compaction(task, ids) => {
                let (new_state, _) = self.compaction_controller.apply_compaction_result(
                    self.state,
                    &task,
                    ids.as_slice(),
                );
                *self.state = new_state;
                self.max_id = std::cmp::max(self.max_id, *ids.last().unwrap_or(&self.max_id));
            }
            ManifestRecord::FlushV2(id, vlog_ids) => {
                if self.compaction_controller.flush_to_l0() {
                    self.state.l0_sstables.insert(0, id);
                } else {
                    self.state.levels.insert(0, (id, vec![id]));
                }
                self.im_memtables.remove(&id);
                self.max_id = std::cmp::max(self.max_id, id);
                if !vlog_ids.is_empty() {
                    self.recovered_vlog_refs.insert(id, vlog_ids);
                }
            }
            ManifestRecord::CompactionV2(task, ids, vlog_ids) => {
                let (new_state, _) = self.compaction_controller.apply_compaction_result(
                    self.state,
                    &task,
                    ids.as_slice(),
                );
                *self.state = new_state;
                self.max_id = std::cmp::max(self.max_id, *ids.last().unwrap_or(&self.max_id));
                if !vlog_ids.is_empty()
                    && let Some((last_id, first_ids)) = ids.split_last()
                {
                    for &sst_id in first_ids {
                        self.recovered_vlog_refs.insert(sst_id, vlog_ids.clone());
                    }
                    self.recovered_vlog_refs.insert(*last_id, vlog_ids);
                }
            }
            ManifestRecord::CompactionV3(task, ids, vlog_ids, ro_ids) => {
                let (new_state, _) = self.compaction_controller.apply_compaction_result(
                    self.state,
                    &task,
                    ids.as_slice(),
                );
                *self.state = new_state;
                self.max_id = std::cmp::max(self.max_id, *ids.last().unwrap_or(&self.max_id));
                self.max_id = std::cmp::max(self.max_id, *ro_ids.last().unwrap_or(&self.max_id));
                if !vlog_ids.is_empty()
                    && let Some((last_id, first_ids)) = ids.split_last()
                {
                    for &sst_id in first_ids {
                        self.recovered_vlog_refs.insert(sst_id, vlog_ids.clone());
                    }
                    self.recovered_vlog_refs.insert(*last_id, vlog_ids);
                }
                // Track range-only SSTs in the target level.
                let target_level = match &task {
                    CompactionTask::Leveled(t) => Some(t.lower_level),
                    CompactionTask::Simple(t) => Some(t.lower_level),
                    CompactionTask::ForceFullCompaction { .. } => Some(1),
                    CompactionTask::Tiered(_) => None,
                };
                if let Some(level) = target_level {
                    self.input_ids_buf.clear();
                    match &task {
                        CompactionTask::ForceFullCompaction {
                            l0_sstables,
                            l1_sstables,
                        } => {
                            self.input_ids_buf
                                .extend(l0_sstables.iter().chain(l1_sstables.iter()).copied());
                        }
                        CompactionTask::Leveled(t) => {
                            self.input_ids_buf.extend(
                                t.upper_level_sst_ids
                                    .iter()
                                    .chain(t.lower_level_sst_ids.iter())
                                    .copied(),
                            );
                        }
                        CompactionTask::Simple(t) => {
                            self.input_ids_buf.extend(
                                t.upper_level_sst_ids
                                    .iter()
                                    .chain(t.lower_level_sst_ids.iter())
                                    .copied(),
                            );
                            if let Some((_, ro_ids_in_state)) = self
                                .state
                                .range_only_ssts
                                .iter()
                                .find(|(lvl, _)| *lvl == level)
                            {
                                self.input_ids_buf.extend(ro_ids_in_state.iter().copied());
                            }
                        }
                        CompactionTask::Tiered(t) => {
                            self.input_ids_buf
                                .extend(t.tiers.iter().flat_map(|(_, ids)| ids.iter().copied()));
                        }
                    };

                    if let Some((_, existing)) = self
                        .state
                        .range_only_ssts
                        .iter_mut()
                        .find(|(l, _)| *l == level)
                    {
                        existing.retain(|id| !self.input_ids_buf.contains(id));
                        existing.extend(ro_ids);
                    } else if !ro_ids.is_empty() {
                        self.state.range_only_ssts.push((level, ro_ids));
                    }
                }
            }
            ManifestRecord::NewVlogFile(_id) | ManifestRecord::DeleteVlogFile(_id) => {
                // vLog file lifecycle — will be handled in vLog recovery
            }
            ManifestRecord::GcCompaction(_old_id, _new_id, _count) => {
                // GC compaction — references are updated via CAS + flush
            }
            ManifestRecord::AddCompactionFilter(filter) => {
                self.next_compaction_filter_id = self
                    .next_compaction_filter_id
                    .max(filter.id.saturating_add(1));
                self.recovered_compaction_filters.insert(filter.id, filter);
            }
            ManifestRecord::RemoveCompactionFilter(id) => {
                self.recovered_compaction_filters.remove(&id);
            }
            ManifestRecord::FormatVersion(_) => {
                // Already validated above; nothing to replay.
            }
            ManifestRecord::Snapshot {
                l0_sstables: snap_l0,
                levels: snap_levels,
                range_only_ssts: snap_ro,
                next_sst_id,
                vlog_references: snap_vlog_refs,
                imm_memtable_ids: snap_imm_ids,
                active_compaction_filters,
                next_compaction_filter_id: snap_next_compaction_filter_id,
                format_version: _,
            } => {
                // Snapshot supersedes all prior records
                self.state.l0_sstables = snap_l0;
                self.state.levels = snap_levels;
                self.state.range_only_ssts = snap_ro;
                self.max_id = next_sst_id.saturating_sub(1);
                self.recovered_vlog_refs.clear();
                for (sst_id, vlog_ids) in snap_vlog_refs {
                    self.recovered_vlog_refs.insert(sst_id, vlog_ids);
                }
                self.im_memtables.clear();
                for id in snap_imm_ids {
                    self.im_memtables.insert(id);
                    self.max_id = self.max_id.max(id);
                }
                self.recovered_compaction_filters = active_compaction_filters
                    .into_iter()
                    .map(|filter| (filter.id, filter))
                    .collect();
                self.next_compaction_filter_id = snap_next_compaction_filter_id;
            }
        }
        Ok(())
    }
}

/// Options for SST-level prefix bloom filters.
///
/// When enabled, each SST may contain one or more prefix bloom filters
/// (one per configured prefix length). These filters allow `prefix_scan`
/// to skip SSTs that provably cannot contain matching prefixes before
/// creating iterators.
#[derive(Debug, Clone)]
pub struct PrefixBloomOptions {
    /// Enable SST prefix bloom filters. Defaults to `false`.
    pub enabled: bool,
    /// Prefix lengths, in bytes, to materialize per SST.
    /// Must be non-empty when enabled, unique, sorted, all > 0, and all <= 64.
    /// Defaults to `vec![8]`.
    pub prefix_lengths: Vec<usize>,
    /// Target false positive rate for each prefix filter.
    /// Must be in `(0.0, 1.0)`. Defaults to `0.01`.
    pub false_positive_rate: f64,
}

impl Default for PrefixBloomOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            prefix_lengths: vec![8],
            false_positive_rate: 0.01,
        }
    }
}

impl PrefixBloomOptions {
    /// Validate prefix bloom options. Returns an error if the options are inconsistent.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        anyhow::ensure!(
            !self.prefix_lengths.is_empty(),
            "prefix_bloom.prefix_lengths must be non-empty when enabled"
        );
        for (i, &len) in self.prefix_lengths.iter().enumerate() {
            anyhow::ensure!(len > 0, "prefix_bloom.prefix_lengths[{}] must be > 0", i);
            anyhow::ensure!(
                len <= 64,
                "prefix_bloom.prefix_lengths[{}] must be <= 64, got {}",
                i,
                len
            );
            if i > 0 {
                anyhow::ensure!(
                    len > self.prefix_lengths[i - 1],
                    "prefix_bloom.prefix_lengths must be strictly increasing and unique, \
                     but {} <= {}",
                    len,
                    self.prefix_lengths[i - 1]
                );
            }
        }
        anyhow::ensure!(
            self.false_positive_rate > 0.0 && self.false_positive_rate < 1.0,
            "prefix_bloom.false_positive_rate must be in (0.0, 1.0), got {}",
            self.false_positive_rate
        );
        Ok(())
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
    /// Defaults to 8192 (~32MB with 4KB blocks).
    pub block_cache_capacity: u64,
    /// Whether to backfill the block cache with newly produced blocks during
    /// flush and compaction. When enabled, recently flushed data stays warm in
    /// cache, eliminating the cache-miss cliff. Defaults to `true`.
    pub enable_cache_backfill: bool,
    /// Options for SST-level prefix bloom filters. When enabled, `prefix_scan`
    /// can skip SSTs that provably cannot contain matching prefixes.
    /// Defaults to disabled.
    pub prefix_bloom: PrefixBloomOptions,
}

impl Default for LsmStorageOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
            value_separation: None,
            manifest_snapshot_threshold_bytes: 4 * 1024 * 1024, // 4MB
            block_cache_capacity: 8192,
            enable_cache_backfill: true,
            prefix_bloom: PrefixBloomOptions::default(),
        }
    }
}

impl LsmStorageOptions {
    /// Test-friendly defaults: disables manifest snapshotting and uses a small
    /// block cache. Equivalent to the old `default_for_test()`.
    pub fn default_for_test() -> Self {
        Self {
            manifest_snapshot_threshold_bytes: 0,
            block_cache_capacity: 1792,
            ..Self::default()
        }
    }

    /// Test defaults with a lower `num_memtable_limit` for flush-heavy tests.
    pub fn default_for_scan_flush_test() -> Self {
        Self {
            num_memtable_limit: 2,
            ..Self::default_for_test()
        }
    }

    /// Test defaults for compaction tests: smaller SST size and lower
    /// `num_memtable_limit`, with caller-supplied compaction options.
    pub fn default_for_compaction_test(compaction_options: CompactionOptions) -> Self {
        Self {
            compaction_options,
            num_memtable_limit: 2,
            target_sst_size: 1 << 20, // 1MB
            ..Self::default_for_test()
        }
    }
}

/// Aggregated cache statistics for the storage engine.
#[derive(Clone, Copy, Debug)]
pub struct CacheStats {
    /// Number of entries currently in the block cache.
    pub block_cache_entry_count: u64,
    /// Number of value cache hits (only available when value separation is enabled).
    pub value_cache_hit_count: u64,
    /// Number of value cache misses (only available when value separation is enabled).
    pub value_cache_miss_count: u64,
}

/// Range tombstone statistics for the storage engine.
///
/// Memtable counts (`active_count`, `immutable_count`) are raw tombstones;
/// SST counts (`sst_count`, `total_sst_fragment_count`) are post-fragmentation.
#[derive(Clone, Copy, Debug)]
pub struct RangeTombstoneStats {
    /// Number of range tombstones in the active (mutable) memtable.
    pub active_count: u64,
    /// Total range tombstones across all immutable memtables.
    pub immutable_count: u64,
    /// Number of SSTs that contain range-tombstone metadata (including range-only SSTs).
    pub sst_count: u64,
    /// Total range-tombstone fragment count across all SSTs.
    pub total_sst_fragment_count: u64,
    /// Approximate bytes of range-tombstone metadata across all SSTs.
    pub metadata_bytes: u64,
    /// Number of point lookups that consulted range-tombstone metadata.
    pub covering_lookups: u64,
    /// Number of point lookups where a range tombstone covered the key.
    pub covering_hits: u64,
    /// Number of point entries dropped by compaction due to range-tombstone coverage.
    pub covered_point_drops: u64,
    /// Number of range-tombstone fragments dropped by compaction (obsolete).
    pub tombstone_drops: u64,
}

#[derive(Clone, Debug)]
pub enum CompactionFilterRequest {
    Prefix(Bytes),
}

impl CompactionFilterRequest {
    pub fn prefix(prefix: impl Into<Bytes>) -> Self {
        Self::Prefix(prefix.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionFilterKind {
    Prefix(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InstalledCompactionFilter {
    pub(crate) id: u64,
    pub(crate) kind: CompactionFilterKind,
    pub(crate) cutoff_ts: u64,
}

impl InstalledCompactionFilter {
    fn info(&self) -> CompactionFilterInfo {
        CompactionFilterInfo {
            id: self.id,
            kind: self.kind.clone(),
            cutoff_ts: self.cutoff_ts,
        }
    }

    pub(crate) fn matches(&self, user_key: &[u8], ts: u64) -> bool {
        if ts > self.cutoff_ts {
            return false;
        }
        match &self.kind {
            CompactionFilterKind::Prefix(prefix) => user_key.starts_with(prefix),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionFilterInfo {
    pub id: u64,
    pub kind: CompactionFilterKind,
    pub cutoff_ts: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactionFilterStats {
    pub entries_eligible: u64,
    pub entries_dropped: u64,
    pub bytes_dropped: u64,
    pub filters_active: usize,
}

/// Lock-free atomic counters for compaction filter stats. These are updated
/// from the compaction hot path without acquiring the registry mutex.
pub(crate) struct CompactionFilterAtomicStats {
    entries_eligible: AtomicU64,
    entries_dropped: AtomicU64,
    bytes_dropped: AtomicU64,
}

/// Atomic counters for range-tombstone runtime observability.
pub(crate) struct RangeTombstoneAtomicStats {
    covering_lookups: AtomicU64,
    covering_hits: AtomicU64,
    covered_point_drops: AtomicU64,
    tombstone_drops: AtomicU64,
}

impl Default for RangeTombstoneAtomicStats {
    fn default() -> Self {
        Self {
            covering_lookups: AtomicU64::new(0),
            covering_hits: AtomicU64::new(0),
            covered_point_drops: AtomicU64::new(0),
            tombstone_drops: AtomicU64::new(0),
        }
    }
}

impl RangeTombstoneAtomicStats {
    pub(crate) fn note_lookup(&self) {
        self.covering_lookups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn note_hit(&self) {
        self.covering_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn note_covered_drop(&self) {
        self.covered_point_drops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn note_tombstone_drops_n(&self, n: u64) {
        self.tombstone_drops
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.covering_lookups
                .load(std::sync::atomic::Ordering::Relaxed),
            self.covering_hits
                .load(std::sync::atomic::Ordering::Relaxed),
            self.covered_point_drops
                .load(std::sync::atomic::Ordering::Relaxed),
            self.tombstone_drops
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

impl Default for CompactionFilterAtomicStats {
    fn default() -> Self {
        Self {
            entries_eligible: AtomicU64::new(0),
            entries_dropped: AtomicU64::new(0),
            bytes_dropped: AtomicU64::new(0),
        }
    }
}

impl CompactionFilterAtomicStats {
    fn note_check(&self) {
        self.entries_eligible
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn note_drop(&self, bytes_dropped: u64) {
        self.entries_dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_dropped
            .fetch_add(bytes_dropped, std::sync::atomic::Ordering::Relaxed);
    }

    fn snapshot(&self, filters_active: usize) -> CompactionFilterStats {
        CompactionFilterStats {
            entries_eligible: self
                .entries_eligible
                .load(std::sync::atomic::Ordering::Relaxed),
            entries_dropped: self
                .entries_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_dropped: self
                .bytes_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            filters_active,
        }
    }
}

#[derive(Default)]
struct CompactionFilterRegistry {
    active_filters: BTreeMap<u64, InstalledCompactionFilter>,
    next_compaction_filter_id: u64,
}

impl CompactionFilterRegistry {
    fn snapshot_filters(&self) -> Vec<InstalledCompactionFilter> {
        self.active_filters.values().cloned().collect()
    }

    fn list(&self) -> Vec<CompactionFilterInfo> {
        self.active_filters
            .values()
            .map(|filter| filter.info())
            .collect()
    }
}

/// Compute the exclusive upper bound for a prefix scan.
///
/// Increments the last byte that is not `0xff` and truncates after it.
/// Returns `None` when the prefix is empty or consists entirely of `0xff`
/// bytes (no finite successor exists).
pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut len = prefix.len();
    while len > 0 && prefix[len - 1] == 0xff {
        len -= 1;
    }
    if len == 0 {
        None
    } else {
        let mut upper = prefix[..len].to_vec();
        upper[len - 1] += 1;
        Some(upper)
    }
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
    compaction_filters: Mutex<CompactionFilterRegistry>,
    filter_stats: Arc<CompactionFilterAtomicStats>,
    pub(crate) rt_stats: Arc<RangeTombstoneAtomicStats>,
    /// Value Log manager for key-value separation. `None` if value separation is disabled.
    pub(crate) vlog: Option<Arc<ValueLog>>,
    /// Weak reference to the owning `Arc<LsmStorageInner>`, set after construction.
    /// Allows background threads (e.g., async GC) to obtain a strong reference.
    pub(crate) weak_self: std::sync::OnceLock<std::sync::Weak<Self>>,
    /// Handles for background GC threads, joined during close().
    pub(crate) gc_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// Cumulative write-path profiling counters (persists across memtable freezes).
    pub(crate) write_profile: Arc<crate::mem_table::WriteProfile>,
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
        // M4: Join background threads to prevent data loss if close() was
        // not called. This mirrors close() but without the full flush/sync
        // sequence — Drop cannot return errors.
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        if let Some(f) = self.flush_thread.lock().take() {
            let _ = f.join();
        }
        if let Some(f) = self.compaction_thread.lock().take() {
            let _ = f.join();
        }
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
        let new_mt = MemTable::create(new_id, self.inner.vlog.is_some());
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

    /// Create a new MVCC transaction with snapshot isolation.
    ///
    /// The transaction reads from a consistent snapshot at its creation
    /// timestamp. Writes are buffered locally until `commit()`.
    pub fn new_txn(&self) -> Result<Arc<crate::mvcc::txn::Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilterRequest) -> Result<u64> {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn add_compaction_filter_with_cutoff(
        &self,
        compaction_filter: CompactionFilterRequest,
        cutoff_ts: u64,
    ) -> Result<u64> {
        self.inner
            .add_compaction_filter_with_cutoff(compaction_filter, cutoff_ts)
    }

    pub fn remove_compaction_filter(&self, id: u64) -> Result<bool> {
        self.inner.remove_compaction_filter(id)
    }

    pub fn list_compaction_filters(&self) -> Vec<CompactionFilterInfo> {
        self.inner.list_compaction_filters()
    }

    pub fn compaction_filter_stats(&self) -> CompactionFilterStats {
        self.inner.compaction_filter_stats()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    /// Batch point-read for multiple keys.
    ///
    /// Optimized for throughput when reading many keys at once. Results are
    /// returned in the same order as the input keys.
    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>> {
        self.inner.batch_get(keys)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    /// Delete all keys in the half-open range `[start, end)`.
    ///
    /// The range tombstone is immediately visible to readers at newer
    /// timestamps. Older snapshots continue to see pre-existing data.
    ///
    /// # Errors
    ///
    /// Returns an error if `start >= end`, if either key exceeds the maximum
    /// key size, or if the engine is in serializable mode or has value
    /// separation enabled (not supported in the MVP).
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        self.inner.delete_range_internal(start, end)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<ScanIterator> {
        self.inner.scan(lower, upper)
    }

    /// Return all visible keys whose user key starts with `prefix`, in sorted
    /// key order. An empty prefix is equivalent to a full scan.
    ///
    /// When prefix bloom filters are enabled, irrelevant SSTs are skipped
    /// before creating iterators.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<ScanIterator> {
        self.inner.prefix_scan(prefix)
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

    /// Get range tombstone statistics for the storage engine.
    ///
    /// Note: iterates SST metadata and may be non-trivial for large LSM states.
    /// Not intended for hot-path use.
    pub fn range_tombstone_stats(&self) -> RangeTombstoneStats {
        let state = self.inner.state.load();
        let active_count = state.memtable.range_tombstones().len() as u64;
        let immutable_count: u64 = state
            .imm_memtables
            .iter()
            .map(|mt| mt.range_tombstones().len() as u64)
            .sum();
        let mut sst_count: u64 = 0;
        let mut total_sst_fragment_count: u64 = 0;
        let mut metadata_bytes: u64 = 0;
        for sst in state.sstables.values() {
            if sst.has_range_tombstones() {
                sst_count += 1;
                if let Some(fragments) = sst.range_tombstone_fragments() {
                    total_sst_fragment_count += fragments.len() as u64;
                    // Estimate: each fragment ≈ 2 (start_len) + 2 (end_len) + 4 (ts_count)
                    // + start.len() + end.len() + covering_ts.len() * 8
                    for f in fragments.iter() {
                        metadata_bytes += 8
                            + f.start.len() as u64
                            + f.end.len() as u64
                            + f.covering_ts.len() as u64 * 8;
                    }
                }
            }
        }
        let (covering_lookups, covering_hits, covered_point_drops, tombstone_drops) =
            self.inner.rt_stats.snapshot();

        RangeTombstoneStats {
            active_count,
            immutable_count,
            sst_count,
            total_sst_fragment_count,
            metadata_bytes,
            covering_lookups,
            covering_hits,
            covered_point_drops,
            tombstone_drops,
        }
    }

    /// Snapshot of cumulative write-path profiling counters for this engine.
    pub fn write_profile(&self) -> crate::mem_table::WriteProfileSnapshot {
        self.inner.write_profile.snapshot()
    }
}

impl LsmStorageInner {
    /// Batch point-read for multiple keys.
    ///
    /// Optimized for throughput when reading many keys at once:
    /// - Sorts keys by encoded order for SST block locality
    /// - Single state snapshot load and read-guard pin for the entire batch
    /// - Pre-computes bloom hashes in bulk
    /// - Uses iterator-based batch memtable lookup for sorted keys
    ///
    /// Returns results in the same order as the input keys.
    /// Threshold below which we use a simpler unsorted path that avoids
    /// per-batch overhead (sorting, range-tombstone pre-scanning).
    const SMALL_BATCH_THRESHOLD: usize = 128;

    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if
    /// the directory does not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        options.prefix_bloom.validate()?;
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
        let mut recovered_compaction_filters: BTreeMap<u64, InstalledCompactionFilter> =
            BTreeMap::new();
        let mut next_compaction_filter_id: u64 = 0;
        // Maximum commit timestamp recovered from WAL batches and SST metadata.
        let mut max_commit_ts: u64 = 0;
        // Whether we need to upgrade from manifest v3 to v4.
        let mut needs_v3_to_v4_upgrade = false;
        let manifest = if !manifest_path.exists() {
            if options.enable_wal {
                let id = state.memtable.id();
                let wal_path = Self::path_of_wal_static(path, id);
                state.memtable = Arc::new(MemTable::create_with_wal(id, vlog_enabled, wal_path)?)
            } else {
                state.memtable = Arc::new(MemTable::create(state.memtable.id(), vlog_enabled));
            }
            let m = Manifest::create(manifest_path).context("failed to create manifest")?;
            m.add_records_when_init(&[
                ManifestRecord::FormatVersion(crate::manifest::MANIFEST_FORMAT_VERSION),
                ManifestRecord::NewMemtable(state.memtable.id()),
            ])?;

            m
        } else {
            let ret = Manifest::recover(manifest_path).context("failed to recover manifest")?;

            // Validate format version: the first record must be FormatVersion(v)
            // or a Snapshot with format_version == v. Pre-MVCC directories (no
            // format marker) are rejected to prevent silent data corruption.
            // Accept v3 (will be upgraded to v4) and v4 (current).
            let detected_version = match ret.1.first() {
                Some(ManifestRecord::FormatVersion(v)) => *v,
                Some(ManifestRecord::Snapshot { format_version, .. }) => *format_version,
                None => anyhow::bail!(
                    "empty manifest file detected; \
                     please start with a fresh database"
                ),
                _ => anyhow::bail!(
                    "pre-MVCC directory detected (no format version marker); \
                     MVCC format (version 4) is required; \
                     please start with a fresh database"
                ),
            };
            anyhow::ensure!(
                detected_version == 3 || detected_version == 4,
                "unsupported manifest format version: got {}, expected 3 or 4; \
                 please start with a fresh database",
                detected_version
            );
            // Track whether we need to upgrade from v3 to v4.
            if detected_version == 3 {
                needs_v3_to_v4_upgrade = true;
            }

            // Replay manifest records using the recovery state helper.
            let mut recovery = ManifestRecoveryState {
                state: &mut state,
                compaction_controller: &compaction_controller,
                max_id,
                im_memtables: BTreeSet::new(),
                recovered_vlog_refs,
                recovered_compaction_filters,
                next_compaction_filter_id,
                input_ids_buf: Vec::new(),
            };
            for record in ret.1 {
                recovery.replay_manifest_record(record)?;
            }
            // Propagate recovery state back to local variables.
            max_id = recovery.max_id;
            let im_memtables = recovery.im_memtables;
            recovered_vlog_refs = recovery.recovered_vlog_refs;
            recovered_compaction_filters = recovery.recovered_compaction_filters;
            next_compaction_filter_id = recovery.next_compaction_filter_id;
            if let Some(max_filter_id) = recovered_compaction_filters.keys().next_back().copied() {
                next_compaction_filter_id =
                    next_compaction_filter_id.max(max_filter_id.saturating_add(1));
            }
            max_id += 1;
            // build imm_memtables and memtable
            if options.enable_wal {
                // just recover all to imm_memtables, then create a new memtable
                for id in im_memtables {
                    let wal_path = Self::path_of_wal_static(path, id);
                    let (m, wal_max_ts) = MemTable::recover_from_wal_with_range_tombstones(
                        id,
                        vlog_enabled,
                        wal_path,
                    )?;
                    if wal_max_ts > max_commit_ts {
                        max_commit_ts = wal_max_ts;
                    }
                    if !m.is_empty() {
                        m.freeze_range_tombstones();
                        state.imm_memtables.insert(0, Arc::new(m));
                    }
                }
            }

            // build sstables — open all SSTs referenced by levels, l0, and
            // range_only_ssts so they're available for compaction and reads.
            let ids = state
                .levels
                .iter()
                .flat_map(|(_, ids)| ids)
                .chain(state.l0_sstables.iter())
                .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids));
            for id in ids {
                if state.sstables.contains_key(id) {
                    continue;
                }
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

            // Classify range-only SSTs: move them from `levels` to
            // `range_only_ssts`. Old manifests may have range-only SSTs
            // mixed into the point SST lists in `levels`. L0 range-only
            // SSTs stay in `l0_sstables` (they're filtered during iteration).
            {
                let mut to_move: Vec<(usize, Vec<usize>)> = Vec::new();
                for (level_idx, (_, sst_ids)) in state.levels.iter_mut().enumerate() {
                    let mut range_only_ids = Vec::new();
                    sst_ids.retain(|id| {
                        if let Some(sst) = state.sstables.get(id)
                            && sst.is_range_only()
                        {
                            range_only_ids.push(*id);
                            return false;
                        }
                        true
                    });
                    if !range_only_ids.is_empty() {
                        to_move.push((level_idx, range_only_ids));
                    }
                }
                for (level_idx, ids) in to_move {
                    let level_num = state.levels[level_idx].0;
                    if let Some((_, ro_ids)) = state
                        .range_only_ssts
                        .iter_mut()
                        .find(|(lvl, _)| *lvl == level_num)
                    {
                        ro_ids.extend(ids);
                    } else {
                        state.range_only_ssts.push((level_num, ids));
                    }
                }
            }

            // Eager v3→v4 manifest upgrade: write a v4 snapshot BEFORE creating
            // any WAL v3 artifact, per RFC Section 8.3 ordering constraint.
            if needs_v3_to_v4_upgrade {
                let snapshot = ManifestRecord::Snapshot {
                    l0_sstables: state.l0_sstables.clone(),
                    levels: state.levels.clone(),
                    range_only_ssts: state.range_only_ssts.clone(),
                    next_sst_id: max_id,
                    vlog_references: if recovered_vlog_refs.is_empty() {
                        Default::default()
                    } else {
                        let mut refs: Vec<_> = recovered_vlog_refs
                            .iter()
                            .filter(|(k, _)| state.sstables.contains_key(k))
                            .map(|(k, v)| (*k, v.clone()))
                            .collect();
                        refs.sort_unstable_by_key(|(k, _)| *k);
                        refs
                    },
                    imm_memtable_ids: state.imm_memtables.iter().map(|m| m.id()).collect(),
                    active_compaction_filters: recovered_compaction_filters
                        .values()
                        .cloned()
                        .collect(),
                    next_compaction_filter_id,
                    format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
                };
                ret.0.snapshot(snapshot)?;
            }

            // Create the new active memtable (with WAL v3 if enabled).
            if options.enable_wal {
                let wal_path = Self::path_of_wal_static(path, max_id);
                state.memtable =
                    Arc::new(MemTable::create_with_wal(max_id, vlog_enabled, wal_path)?);
            } else {
                state.memtable = Arc::new(MemTable::create(max_id, vlog_enabled));
            }
            ret.0
                .add_record_when_init(ManifestRecord::NewMemtable(max_id))?;

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
                log::error!("vLog orphan cleanup error: {}", e);
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
            compaction_filters: Mutex::new(CompactionFilterRegistry {
                active_filters: recovered_compaction_filters,
                next_compaction_filter_id,
            }),
            filter_stats: Arc::new(CompactionFilterAtomicStats::default()),
            rt_stats: Arc::new(RangeTombstoneAtomicStats::default()),
            vlog,
            weak_self: std::sync::OnceLock::new(),
            gc_handles: Mutex::new(Vec::new()),
            write_profile: Arc::new(crate::mem_table::WriteProfile::default()),
        };
        // Propagate the engine-level write profile to the initial memtable.
        storage
            .state
            .load()
            .memtable
            .set_write_profile(storage.write_profile.clone());
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.load().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilterRequest) -> Result<u64> {
        // Capture cutoff_ts under the same lock used by add_compaction_filter_with_cutoff,
        // so no concurrent write can interleave between the cutoff read and validation.
        let _state_lock = self.state_lock.lock();
        let cutoff_ts = self.mvcc.as_ref().map_or(0, |mvcc| {
            let _write_guard = mvcc.write_lock.lock();
            mvcc.latest_commit_ts()
        });
        self.add_compaction_filter_inner(&_state_lock, compaction_filter, cutoff_ts)
    }

    pub fn add_compaction_filter_with_cutoff(
        &self,
        compaction_filter: CompactionFilterRequest,
        cutoff_ts: u64,
    ) -> Result<u64> {
        let _state_lock = self.state_lock.lock();
        if let Some(mvcc) = self.mvcc.as_ref() {
            let _write_guard = mvcc.write_lock.lock();
            anyhow::ensure!(
                cutoff_ts <= mvcc.latest_commit_ts(),
                "compaction filter cutoff_ts {} exceeds latest commit ts {}",
                cutoff_ts,
                mvcc.latest_commit_ts()
            );
        }
        self.add_compaction_filter_inner(&_state_lock, compaction_filter, cutoff_ts)
    }

    fn add_compaction_filter_inner(
        &self,
        _state_lock: &MutexGuard<'_, ()>,
        compaction_filter: CompactionFilterRequest,
        cutoff_ts: u64,
    ) -> Result<u64> {
        let mut registry = self.compaction_filters.lock();
        let kind = Self::validate_compaction_filter_request(compaction_filter)?;
        anyhow::ensure!(
            !registry
                .active_filters
                .values()
                .any(|filter| filter.kind == kind && filter.cutoff_ts == cutoff_ts),
            "duplicate active compaction filter"
        );

        let id = registry.next_compaction_filter_id;
        let installed = InstalledCompactionFilter {
            id,
            kind,
            cutoff_ts,
        };
        self.manifest
            .as_ref()
            .expect("manifest initialized")
            .add_record(
                _state_lock,
                ManifestRecord::AddCompactionFilter(installed.clone()),
            )?;
        registry.active_filters.insert(id, installed);
        registry.next_compaction_filter_id = registry.next_compaction_filter_id.saturating_add(1);
        drop(registry);
        self.maybe_snapshot_manifest(_state_lock)?;

        Ok(id)
    }

    pub fn remove_compaction_filter(&self, id: u64) -> Result<bool> {
        let _state_lock = self.state_lock.lock();
        let mut registry = self.compaction_filters.lock();
        let Some(filter) = registry.active_filters.remove(&id) else {
            return Ok(false);
        };

        if let Err(err) = self
            .manifest
            .as_ref()
            .expect("manifest initialized")
            .add_record(&_state_lock, ManifestRecord::RemoveCompactionFilter(id))
        {
            registry.active_filters.insert(id, filter);
            return Err(err);
        }
        drop(registry);
        self.maybe_snapshot_manifest(&_state_lock)?;

        Ok(true)
    }

    pub fn list_compaction_filters(&self) -> Vec<CompactionFilterInfo> {
        self.compaction_filters.lock().list()
    }

    pub fn compaction_filter_stats(&self) -> CompactionFilterStats {
        let filters_active = self.compaction_filters.lock().active_filters.len();
        self.filter_stats.snapshot(filters_active)
    }

    fn validate_compaction_filter_request(
        compaction_filter: CompactionFilterRequest,
    ) -> Result<CompactionFilterKind> {
        match compaction_filter {
            CompactionFilterRequest::Prefix(prefix) => {
                anyhow::ensure!(
                    !prefix.is_empty(),
                    "compaction filter prefix must not be empty"
                );
                anyhow::ensure!(
                    crate::key::encoded_internal_key_len(prefix.len())
                        <= crate::key::MAX_ENCODED_KEY_LEN,
                    "compaction filter prefix encoded key length exceeds maximum {}",
                    crate::key::MAX_ENCODED_KEY_LEN
                );
                Ok(CompactionFilterKind::Prefix(prefix.to_vec()))
            }
        }
    }

    pub(crate) fn snapshot_compaction_filters(&self) -> Vec<InstalledCompactionFilter> {
        self.compaction_filters.lock().snapshot_filters()
    }

    pub(crate) fn record_compaction_filter_check(&self) {
        self.filter_stats.note_check();
    }

    pub(crate) fn record_compaction_filter_drop(&self, bytes_dropped: u64) {
        self.filter_stats.note_drop(bytes_dropped);
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
        // Check memtable first — pass raw user key to avoid re-encoding.
        let read_ts_for_range = mvcc_read_ts.unwrap_or(u64::MAX);
        let memtable_range_ts = self.newest_memtable_range_ts(&state, key, read_ts_for_range);
        self.rt_stats.note_lookup();
        if let Some((value, kind, found_key, value_ts)) =
            self.lookup_memtable(&state, key, bloom_hash, mvcc_read_ts)?
        {
            if memtable_range_ts.is_some_and(|rt| value_ts <= rt) {
                // Covered by range tombstone — any SST version is older and
                // also covered, so return immediately.
                self.rt_stats.note_hit();
                return Ok(None);
            }
            return self.resolve_value(&found_key, value, kind);
        }
        // SST path — only compute SST range tombstone ts when we actually
        // need it. Skip entirely if memtable already covers at read_ts.
        let range_ts = if memtable_range_ts.is_some_and(|ts| ts >= read_ts_for_range) {
            memtable_range_ts
        } else {
            memtable_range_ts.max(self.newest_sst_range_ts(&state, key, read_ts_for_range))
        };
        if let Some((value, kind, found_key, value_ts)) =
            self.lookup_sst_raw(&state, encoded, bloom_hash, mvcc_read_ts, None, None)?
        {
            if range_ts.is_some_and(|rt| value_ts <= rt) {
                self.rt_stats.note_hit();
                return Ok(None);
            }
            return self.resolve_value(&found_key, value, kind);
        }

        // No point entry found, but a range tombstone covers this key —
        // after compaction removed the covered point entry, the key is
        // effectively hidden by range-tombstone metadata alone.
        if range_ts.is_some() {
            self.rt_stats.note_hit();
        }

        Ok(None)
    }

    pub fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Bytes>>> {
        let n = keys.len();
        if n == 0 {
            return Vec::new();
        }
        if n == 1 {
            return vec![self.get(keys[0])];
        }

        // Snapshot state once for the entire batch.
        let state = self.state.load();
        let read_guard = self.mvcc.as_ref().map(|m| m.new_read_guard());
        let mvcc_read_ts = read_guard.as_ref().map(|g| g.read_ts());

        // Fast path for small batches: skip sorting and range-tombstone
        // pre-scanning. The per-batch setup cost (sorting, O(S) RT scan,
        // multiple Vec allocations) dominates at small n.
        if n < Self::SMALL_BATCH_THRESHOLD {
            return self.batch_get_small(&state, keys, mvcc_read_ts, read_guard);
        }

        // Pre-compute bloom hashes for all keys.
        let bloom_hashes: Vec<u32> = keys
            .iter()
            .map(|k| crate::table::bloom::hash_key(k))
            .collect();

        // Sort keys by raw bytes for SST block locality.
        let mut sorted_indices: Vec<usize> = (0..n).collect();
        sorted_indices.sort_unstable_by(|&a, &b| keys[a].cmp(keys[b]));
        let sorted_keys: Vec<(usize, &[u8])> =
            sorted_indices.iter().map(|&i| (i, keys[i])).collect();

        // Batch memtable lookup for sorted keys.
        let memtable_results = match self.batch_lookup_memtable(
            &state,
            &sorted_keys,
            &bloom_hashes,
            mvcc_read_ts,
            n,
        ) {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                return (0..n).map(|_| Err(anyhow::anyhow!("{msg}"))).collect();
            }
        };

        // Build output, falling back to SST lookup for keys not found in memtable.
        let mut output: Vec<Result<Option<Bytes>>> = Vec::with_capacity(n);
        output.resize_with(n, || Ok(None));
        let read_ts_for_range = mvcc_read_ts.unwrap_or(u64::MAX);

        // Reusable buffer for encoding keys (avoids per-key Vec allocation).
        let mut encode_buf: Vec<u8> = Vec::new();

        // Pre-collect range tombstone fragments once to avoid per-key
        // discovery overhead. O(S) once instead of O(K×S) across the batch.
        // Cheap non-allocating checks first to short-circuit when no RTs exist.
        let active_rt_frags = state.memtable.range_tombstones().cached_fragments();
        let has_active_rt = !active_rt_frags.is_empty();
        let has_imm_rt = state
            .imm_memtables
            .iter()
            .any(|m| m.imm_range_tombstones().is_some());
        let has_sst_rt = state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, ids)| ids))
            .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids))
            .any(|id| {
                state
                    .sstables
                    .get(id)
                    .is_some_and(|s| s.has_range_tombstones())
            });
        let has_any_rt = has_active_rt || has_imm_rt || has_sst_rt;

        // Only allocate vectors when range tombstones actually exist.
        let imm_rt_list: Vec<_> = if has_imm_rt {
            state
                .imm_memtables
                .iter()
                .filter_map(|m| m.imm_range_tombstones())
                .collect()
        } else {
            Vec::new()
        };
        let sst_rt_frags: Vec<&[crate::range_tombstone::RangeTombstoneFragment]> = if has_sst_rt {
            state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, ids)| ids))
                .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids))
                .filter_map(|id| {
                    state
                        .sstables
                        .get(id)
                        .and_then(|sst| sst.range_tombstone_fragments())
                        .filter(|frags| !frags.is_empty())
                        .map(|arc| arc.as_ref())
                })
                .collect()
        } else {
            Vec::new()
        };

        // Closures that search pre-collected fragments instead of re-discovering per key.
        let get_memtable_range_ts = |user_key: &[u8]| -> Option<u64> {
            if !has_any_rt {
                return None;
            }
            let mut best_ts: Option<u64> = None;
            if has_active_rt
                && let (Some(first), Some(last)) = (active_rt_frags.first(), active_rt_frags.last())
                && user_key >= first.start.as_ref()
                && user_key < last.end.as_ref()
            {
                best_ts = crate::range_tombstone::find_newest_covering_ts(
                    &active_rt_frags,
                    user_key,
                    read_ts_for_range,
                );
            }
            for imm in &imm_rt_list {
                let frags = imm.fragments();
                if let (Some(first), Some(last)) = (frags.first(), frags.last())
                    && user_key >= first.start.as_ref()
                    && user_key < last.end.as_ref()
                {
                    best_ts = best_ts.max(imm.newest_covering_ts(user_key, read_ts_for_range));
                }
            }
            best_ts
        };
        // Pre-compute global boundary for SST range tombstone fragments.
        // This gives an O(1) early-exit before iterating per-fragment sets.
        let (sst_rt_global_start, sst_rt_global_end): (Vec<u8>, Vec<u8>) =
            if !sst_rt_frags.is_empty() {
                let mut min_start: Vec<u8> = sst_rt_frags[0][0].start.as_ref().to_vec();
                let mut max_end: Vec<u8> = sst_rt_frags[0].last().unwrap().end.as_ref().to_vec();
                for frags in &sst_rt_frags[1..] {
                    if let Some(first) = frags.first()
                        && first.start.as_ref() < min_start.as_slice()
                    {
                        min_start = first.start.as_ref().to_vec();
                    }
                    if let Some(last) = frags.last()
                        && last.end.as_ref() > max_end.as_slice()
                    {
                        max_end = last.end.as_ref().to_vec();
                    }
                }
                (min_start, max_end)
            } else {
                (Vec::new(), Vec::new())
            };

        let get_sst_range_ts = |user_key: &[u8]| -> Option<u64> {
            if sst_rt_frags.is_empty() {
                return None;
            }
            // O(1) global boundary check — skip all fragments if key is
            // outside the union of all fragment ranges.
            if user_key < sst_rt_global_start.as_slice() || user_key >= sst_rt_global_end.as_slice()
            {
                return None;
            }
            let mut best_ts: Option<u64> = None;
            for frags in &sst_rt_frags {
                // O(1) boundary check — skip binary search if key is outside
                // the fragment range. Fragments are sorted and non-overlapping.
                if let (Some(first), Some(last)) = (frags.first(), frags.last())
                    && (user_key < first.start.as_ref() || user_key >= last.end.as_ref())
                {
                    continue;
                }
                let ts = crate::range_tombstone::find_newest_covering_ts(
                    frags,
                    user_key,
                    read_ts_for_range,
                );
                best_ts = best_ts.max(ts);
                if best_ts == Some(read_ts_for_range) {
                    break;
                }
            }
            best_ts
        };

        // Per-level SST index hint for sorted batch lookups. When consecutive
        // keys land in the same leveled SST, this skips the O(log S) binary search.
        let mut level_hint: ahash::AHashMap<usize, usize> = ahash::AHashMap::new();
        // L0 SST hint — when consecutive sorted keys land in the same L0 SST,
        // checking it first avoids iterating through earlier L0 SSTs.
        let mut l0_hint: usize = 0;

        for &(orig_idx, user_key) in &sorted_keys {
            self.rt_stats.note_lookup();

            if let Some((value, kind, found_key, value_ts)) = memtable_results[orig_idx].as_ref() {
                // Memtable hit — only check range tombstones to see if this value is covered.
                let memtable_range_ts = get_memtable_range_ts(user_key);
                if memtable_range_ts.is_some_and(|rt| *value_ts <= rt) {
                    self.rt_stats.note_hit();
                    output[orig_idx] = Ok(None);
                    continue;
                }
                output[orig_idx] = self.resolve_value(found_key, value.clone(), *kind);
                continue;
            }

            // No memtable hit — check if a memtable range tombstone covers
            // everything before falling through to the SST path.
            let memtable_range_ts = get_memtable_range_ts(user_key);
            if memtable_range_ts.is_some_and(|ts| ts >= read_ts_for_range) {
                self.rt_stats.note_hit();
                output[orig_idx] = Ok(None);
                continue;
            }

            // SST path — encode key into reusable buffer.
            let encoded_ref = if self.mvcc.is_some() {
                encode_buf.clear();
                crate::key::encode_internal_key_to_buf(&mut encode_buf, user_key, u64::MAX);
                encode_buf.as_slice()
            } else {
                user_key
            };

            let bloom_hash = bloom_hashes[orig_idx];
            let sst_result = self.lookup_sst_raw(
                &state,
                encoded_ref,
                bloom_hash,
                mvcc_read_ts,
                Some(&mut level_hint),
                Some(&mut l0_hint),
            );
            // Combined range tombstone ts — memtable RT may partially cover
            // older SST versions that the early-out above did not catch.
            let sst_range_ts = get_sst_range_ts(user_key);
            let range_ts = memtable_range_ts.max(sst_range_ts);
            match sst_result {
                Ok(Some((value, kind, found_key, value_ts))) => {
                    // Value found in SST — check combined range tombstones.
                    // The memtable RT may cover an older SST version even
                    // when it does not cover ALL versions up to read_ts.
                    if range_ts.is_some_and(|rt| value_ts <= rt) {
                        self.rt_stats.note_hit();
                        output[orig_idx] = Ok(None);
                    } else {
                        output[orig_idx] = self.resolve_value(&found_key, value, kind);
                    }
                }
                Ok(None) => {
                    // No point entry found in SSTs — check if a range
                    // tombstone covers the key (point entry may have been
                    // removed by compaction, leaving only range metadata).
                    if range_ts.is_some() {
                        self.rt_stats.note_hit();
                    }
                    output[orig_idx] = Ok(None);
                }
                Err(e) => {
                    output[orig_idx] = Err(e);
                }
            }
        }

        output
    }

    /// Fast path for small batches (n < `SMALL_BATCH_THRESHOLD`).
    ///
    /// Avoids the per-batch overhead that dominates at small n:
    /// - No key sorting (processes keys in original order)
    /// - No range-tombstone pre-scanning (O(S) SST iteration)
    /// - Fewer Vec allocations (no sorted_indices, sorted_keys)
    fn batch_get_small(
        &self,
        state: &LsmStorageState,
        keys: &[&[u8]],
        mvcc_read_ts: Option<u64>,
        _read_guard: Option<crate::mvcc::ReadGuard>,
    ) -> Vec<Result<Option<Bytes>>> {
        let n = keys.len();
        let mut output: Vec<Result<Option<Bytes>>> = Vec::with_capacity(n);
        output.resize_with(n, || Ok(None));
        let read_ts_for_range = mvcc_read_ts.unwrap_or(u64::MAX);

        // Pre-compute bloom hashes once.
        let bloom_hashes: Vec<u32> = keys
            .iter()
            .map(|k| crate::table::bloom::hash_key(k))
            .collect();

        // Check if memtable range tombstones exist (cheap check).
        let active_rt_frags = state.memtable.range_tombstones().cached_fragments();
        let has_active_rt = !active_rt_frags.is_empty();
        let has_imm_rt = state
            .imm_memtables
            .iter()
            .any(|m| m.imm_range_tombstones().is_some());
        let has_any_memtable_rt = has_active_rt || has_imm_rt;
        let has_imm_memtables = !state.imm_memtables.is_empty();

        // Helper closure: check memtable range tombstones for a key.
        let get_memtable_range_ts = |uk: &[u8]| -> Option<u64> {
            if !has_any_memtable_rt {
                return None;
            }
            let mut best_ts: Option<u64> = None;
            if has_active_rt
                && let (Some(first), Some(last)) = (active_rt_frags.first(), active_rt_frags.last())
                && uk >= first.start.as_ref()
                && uk < last.end.as_ref()
            {
                best_ts = crate::range_tombstone::find_newest_covering_ts(
                    &active_rt_frags,
                    uk,
                    read_ts_for_range,
                );
            }
            if has_imm_rt {
                for m in &state.imm_memtables {
                    if let Some(imm) = m.imm_range_tombstones() {
                        let frags = imm.fragments();
                        if let (Some(first), Some(last)) = (frags.first(), frags.last())
                            && uk >= first.start.as_ref()
                            && uk < last.end.as_ref()
                        {
                            best_ts = best_ts.max(crate::range_tombstone::find_newest_covering_ts(
                                frags,
                                uk,
                                read_ts_for_range,
                            ));
                        }
                    }
                }
            }
            best_ts
        };

        // Reusable buffers — avoid per-key allocations.
        let mut seek_buf: Vec<u8> = Vec::new();
        let mut encode_buf: Vec<u8> = Vec::new();
        // SST hint for consecutive lookups.
        let mut level_hint: ahash::AHashMap<usize, usize> = ahash::AHashMap::new();
        let mut l0_hint: usize = 0;

        for (i, user_key) in keys.iter().enumerate() {
            let uk: &[u8] = user_key;
            self.rt_stats.note_lookup();

            // --- Memtable lookup using buffer-reusing variant ---
            // Avoids per-key Bytes allocation for seek key.
            let mut memtable_hit: Option<(Option<Bytes>, KvKind, Vec<u8>, u64)> = None;

            if let Some(read_ts) = mvcc_read_ts {
                // Active memtable — reuse seek_buf.
                if let Some((raw, found_key)) = state.memtable.get_versioned_with_buf(
                    uk,
                    read_ts,
                    bloom_hashes[i],
                    &mut seek_buf,
                ) {
                    let (val, kind) = Self::parse_value_kind(raw);
                    let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                    memtable_hit = Some((val, kind, found_key, vts));
                }
                // Immutable memtables — only if active missed.
                if memtable_hit.is_none() && has_imm_memtables {
                    for m in state.imm_memtables.iter() {
                        if let Some((raw, found_key)) =
                            m.get_versioned_with_buf(uk, read_ts, bloom_hashes[i], &mut seek_buf)
                        {
                            let (val, kind) = Self::parse_value_kind(raw);
                            let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                            memtable_hit = Some((val, kind, found_key, vts));
                            break;
                        }
                    }
                }
            } else {
                // Non-MVCC path.
                if let Some(raw) = state.memtable.get_raw_with_hash(uk, bloom_hashes[i]) {
                    let (val, kind) = Self::parse_value_kind(raw);
                    memtable_hit = Some((val, kind, uk.to_vec(), 0));
                }
                if memtable_hit.is_none() && has_imm_memtables {
                    for m in state.imm_memtables.iter() {
                        if let Some(raw) = m.get_raw_with_hash(uk, bloom_hashes[i]) {
                            let (val, kind) = Self::parse_value_kind(raw);
                            memtable_hit = Some((val, kind, uk.to_vec(), 0));
                            break;
                        }
                    }
                }
            }

            if let Some((value, kind, found_key, value_ts)) = memtable_hit {
                // Check memtable range tombstones (cheap — no SST iteration).
                if let Some(rt) = get_memtable_range_ts(uk)
                    && value_ts <= rt
                {
                    self.rt_stats.note_hit();
                    output[i] = Ok(None);
                    continue;
                }
                output[i] = self.resolve_value(&found_key, value, kind);
                continue;
            }

            // If memtable missed, check if a memtable range tombstone covers
            // the key. Since SST versions are strictly older, they will also
            // be covered — skip the SST lookup entirely.
            if let Some(_rt) = get_memtable_range_ts(uk) {
                self.rt_stats.note_hit();
                output[i] = Ok(None);
                continue;
            }

            // --- SST path — encode key into reusable buffer. ---
            let encoded_ref = if self.mvcc.is_some() {
                encode_buf.clear();
                crate::key::encode_internal_key_to_buf(&mut encode_buf, uk, u64::MAX);
                encode_buf.as_slice()
            } else {
                uk
            };

            match self.lookup_sst_raw(
                state,
                encoded_ref,
                bloom_hashes[i],
                mvcc_read_ts,
                Some(&mut level_hint),
                Some(&mut l0_hint),
            ) {
                Ok(Some((value, kind, found_key, value_ts))) => {
                    // Check both memtable AND SST range tombstones.
                    let memtable_rt = get_memtable_range_ts(uk);
                    // SST range tombstones — iterates L0 + levels + range-only SSTs.
                    let sst_range_ts = self.newest_sst_range_ts(state, uk, read_ts_for_range);
                    let range_ts = memtable_rt.max(sst_range_ts);
                    if range_ts.is_some_and(|rt| value_ts <= rt) {
                        self.rt_stats.note_hit();
                        output[i] = Ok(None);
                        continue;
                    }
                    output[i] = self.resolve_value(&found_key, value, kind);
                }
                Ok(None) => {
                    // No point entry found in SSTs — check if an SST range
                    // tombstone covers the key.
                    let sst_range_ts = self.newest_sst_range_ts(state, uk, read_ts_for_range);
                    if sst_range_ts.is_some() {
                        self.rt_stats.note_hit();
                    }
                }
                Err(e) => {
                    output[i] = Err(e);
                }
            }
        }

        output
    }

    /// Resolve a `(value, KvKind)` pair from a lookup into a final `Option<Bytes>`.
    /// For `ValuePointer`, dereferences through the vLog. For `Inline`/`Tombstone`,
    /// returns the value as-is.
    /// `key` is the full encoded internal key of the found entry, used for
    /// vLog key verification.
    fn resolve_value(
        &self,
        key: &[u8],
        value: Option<Bytes>,
        kind: KvKind,
    ) -> Result<Option<Bytes>> {
        match kind {
            KvKind::ValuePointer => self.resolve_vlog_value_bytes(
                key,
                value.ok_or_else(|| {
                    anyhow::anyhow!("ValuePointer entry missing value for key {:?}", key)
                })?,
            ),
            KvKind::Inline | KvKind::Tombstone => Ok(value),
        }
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
                let vlog = self
                    .vlog
                    .as_ref()
                    .ok_or_else(|| anyhow!("value pointer found but vLog is not enabled"))?;
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
    pub(crate) fn validate_key_size(key: &[u8]) -> Result<()> {
        if key.len() > crate::key::MAX_ENCODED_KEY_LEN {
            anyhow::bail!(
                "raw key length {} exceeds maximum allowed encoded length {}",
                key.len(),
                crate::key::MAX_ENCODED_KEY_LEN
            );
        }
        let encoded_len = crate::key::encoded_internal_key_len(key.len());
        anyhow::ensure!(
            encoded_len <= crate::key::MAX_ENCODED_KEY_LEN,
            "encoded key length {} exceeds maximum {} (raw key {} bytes)",
            encoded_len,
            crate::key::MAX_ENCODED_KEY_LEN,
            key.len()
        );
        Ok(())
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

    /// Look up the exact version `(user_key, ts)` and return its value and kind.
    /// Used by vLog GC to check if a specific version's pointer still matches.
    ///
    /// Unlike `get_with_kind` (which returns the newest visible version), this
    /// checks whether the specific encoded internal key exists in the LSM tree.
    pub(crate) fn get_with_kind_at_ts(
        &self,
        user_key: &[u8],
        ts: u64,
    ) -> Result<(Option<Bytes>, KvKind)> {
        let state = self.state.load();
        let encoded = crate::key::encode_internal_key(user_key, ts);
        let bloom_hash = crate::table::bloom::hash_key(user_key);

        // Check active + immutable memtables (exact key match, no version scan)
        if let Some(raw) = state.memtable.get_raw_exact_with_hash(&encoded, bloom_hash) {
            return Ok(Self::parse_value_kind(raw));
        }
        for m in state.imm_memtables.iter() {
            if let Some(raw) = m.get_raw_exact_with_hash(&encoded, bloom_hash) {
                return Ok(Self::parse_value_kind(raw));
            }
        }

        // SST lookup — use point_get which seeks to the exact key.
        // With the full internal key, point_get positions at the exact version
        // (or the nearest one). We then verify the found key matches exactly.
        for id in state.l0_sstables.iter() {
            if let Some(s) = state.sstables.get(id)
                && let Some((raw, found_key)) =
                    s.point_get_with_hash_and_key(&encoded, bloom_hash, None)?
                && found_key == encoded
            {
                return Ok(Self::parse_value_kind(raw));
            }
        }
        for (_, sst_ids) in state.levels.iter() {
            // Leveled SSTs have non-overlapping key ranges, but a user
            // key's versions can span adjacent SSTs. Binary search to find
            // the rightmost candidate, then scan left while the SST's
            // last_key still carries the same user key prefix.
            let user_key_prefix = crate::key::encoded_user_key_prefix(&encoded)
                .expect("encoded key must have valid user key prefix");
            let idx = sst_ids.partition_point(|id| {
                let sst = state.sstables.get(id).expect("SST must exist");
                match sst.first_key() {
                    Some(fk) => fk.encoded_user_key() <= user_key_prefix,
                    None => false, // range-only SST: sort before all others
                }
            });
            for i in (0..idx).rev() {
                let sst = state.sstables.get(&sst_ids[i]).expect("SST must exist");
                let Some(last_key) = sst.last_key() else {
                    continue; // range-only SST, no point data
                };
                if last_key.encoded_user_key() < user_key_prefix {
                    break;
                }
                if let Some((raw, found_key)) =
                    sst.point_get_with_hash_and_key(&encoded, bloom_hash, None)?
                    && found_key == encoded
                {
                    return Ok(Self::parse_value_kind(raw));
                }
            }
        }

        Ok((None, KvKind::Inline))
    }

    /// Get a value at a specific read timestamp (used by transactions).
    pub(crate) fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let state = self.state.load();
        let bloom_hash = crate::table::bloom::hash_key(key);
        let lookup_key = crate::key::encode_internal_key(key, u64::MAX);
        let memtable_range_ts = self.newest_memtable_range_ts(&state, key, read_ts);
        self.rt_stats.note_lookup();
        if let Some((value, kind, found_key, value_ts)) =
            self.lookup_memtable(&state, key, bloom_hash, Some(read_ts))?
        {
            if memtable_range_ts.is_some_and(|rt| value_ts <= rt) {
                // Covered by memtable range tombstone — SST versions are older
                // and also covered, so return immediately.
                self.rt_stats.note_hit();
                return Ok(None);
            }
            return self.resolve_value(&found_key, value, kind);
        }
        // Only compute SST range tombstone ts when we fall through to SSTs.
        // Skip entirely if memtable already covers at read_ts.
        let range_ts = if memtable_range_ts.is_some_and(|ts| ts >= read_ts) {
            memtable_range_ts
        } else {
            memtable_range_ts.max(self.newest_sst_range_ts(&state, key, read_ts))
        };
        if let Some((value, kind, found_key, value_ts)) =
            self.lookup_sst_raw(&state, &lookup_key, bloom_hash, Some(read_ts), None, None)?
        {
            if range_ts.is_some_and(|rt| value_ts <= rt) {
                self.rt_stats.note_hit();
                return Ok(None);
            }
            return self.resolve_value(&found_key, value, kind);
        }

        if range_ts.is_some() {
            self.rt_stats.note_hit();
        }

        Ok(None)
    }

    /// Scan at a specific read timestamp (used by transactions).
    pub(crate) fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<crate::lsm_iterator::FusedIterator<crate::lsm_iterator::LsmIterator>> {
        self.scan_inner(lower, upper, Some(read_ts), None)
    }

    /// Scan with a prefix hint for prefix bloom filter pruning.
    pub(crate) fn scan_with_prefix_hint(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
        prefix_hint: &[u8],
    ) -> Result<crate::lsm_iterator::FusedIterator<crate::lsm_iterator::LsmIterator>> {
        self.scan_inner(lower, upper, Some(read_ts), Some(prefix_hint))
    }

    /// Shared scan logic used by both `scan` and `scan_with_ts`.
    fn scan_inner(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        mvcc_read_ts: Option<u64>,
        prefix_hint: Option<&[u8]>,
    ) -> Result<crate::lsm_iterator::FusedIterator<crate::lsm_iterator::LsmIterator>> {
        let state = self.state.load_full();
        let mvcc_enabled = self.mvcc.is_some();

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
            // Prefix bloom pruning: skip SSTs that provably cannot contain
            // keys matching the prefix hint.
            if self.options.prefix_bloom.enabled
                && let Some(prefix) = prefix_hint
                && !t.may_contain_prefix(prefix)
            {
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
                // Prefix bloom pruning for L1+ SSTs.
                if self.options.prefix_bloom.enabled
                    && let Some(prefix) = prefix_hint
                    && !t.may_contain_prefix(prefix)
                {
                    continue;
                }
                ss_tables.push(t);
            }
            if ss_tables.is_empty() {
                continue;
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

        // Build merged range-tombstone fragments from all memtables.
        // Active memtable: private per-scan fragments (no shared cache).
        // Immutable memtables: shared cached fragments (borrowed, not cloned).
        // Fast path: skip active memtable tombstones if the scan range
        // doesn't overlap any tombstone span. O(1) skipmap bounds check.
        let has_active = !state.memtable.range_tombstones().is_empty()
            && match upper {
                Bound::Unbounded => true,
                _ => state
                    .memtable
                    .range_tombstones()
                    .range_could_overlap(lower, upper),
            };
        let has_imm = state
            .imm_memtables
            .iter()
            .any(|m| m.imm_range_tombstones().is_some());
        let has_sst_rt = state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, ids)| ids))
            .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids))
            .any(|id| {
                state
                    .sstables
                    .get(id)
                    .is_some_and(|s| s.has_range_tombstones())
            });
        let range_ts_iter = if has_active || has_imm || has_sst_rt {
            let active_frags = if has_active {
                state.memtable.range_tombstones().cached_fragments()
            } else {
                std::sync::Arc::new(Vec::new())
            };
            let mut lists: Vec<&[crate::range_tombstone::RangeTombstoneFragment]> =
                Vec::with_capacity(1 + state.imm_memtables.len());
            if !active_frags.is_empty() {
                lists.push(&active_frags);
            }
            for m in &state.imm_memtables {
                if let Some(imm) = m.imm_range_tombstones() {
                    let frags = imm.fragments();
                    if !frags.is_empty() {
                        lists.push(frags);
                    }
                }
            }
            // Collect SST range-tombstone fragments (including range-only SSTs).
            for id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, ids)| ids))
                .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids))
            {
                if let Some(sst) = state.sstables.get(id)
                    && let Some(frags) = sst.range_tombstone_fragments()
                    && !frags.is_empty()
                {
                    lists.push(frags);
                }
            }
            if lists.is_empty() {
                None
            } else {
                let merged = crate::range_tombstone::merge_fragment_lists(&lists);
                if merged.is_empty() {
                    None
                } else {
                    // Check if the scan range overlaps any fragment.
                    // If not, skip creating the iterator entirely — avoids
                    // per-entry function call overhead when there's no coverage.
                    let overlaps = Self::scan_overlaps_fragments(lower, upper, &merged);
                    if !overlaps {
                        None
                    } else {
                        let mut rt_iter =
                            crate::range_tombstone::RangeTombstoneIterator::new(Arc::from(merged));
                        // Position cursor at the scan's lower bound to skip
                        // fragments entirely before the scan range. O(log F).
                        if let Bound::Included(key) | Bound::Excluded(key) = lower {
                            rt_iter.seek_to(key);
                        }
                        Some(rt_iter)
                    }
                }
            }
        } else {
            None
        };

        let lit = LsmIterator::new(two_m, Self::into_vec(upper), mvcc_read_ts, range_ts_iter)?;

        Ok(FusedIterator::new(lit))
    }

    /// Check if the scan range `[lower, upper)` overlaps any fragment.
    ///
    /// Returns `false` if the scan range is entirely outside all fragments,
    /// meaning the `RangeTombstoneIterator` can be skipped entirely.
    /// O(1) — compares scan bounds against first/last fragment boundaries.
    fn scan_overlaps_fragments(
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        fragments: &[crate::range_tombstone::RangeTombstoneFragment],
    ) -> bool {
        if fragments.is_empty() {
            return false;
        }
        // Scan lower bound past all fragment ends → no overlap.
        // For half-open fragments [start, end):
        //   Included(key) >= end → key is at or past the last fragment → no overlap
        //   Excluded(key) >= end → key is past the last fragment → no overlap
        if let Bound::Included(key) | Bound::Excluded(key) = lower
            && let Some(last) = fragments.last()
            && key >= last.end.as_ref()
        {
            return false;
        }
        // Scan upper bound before all fragment starts → no overlap.
        // For half-open fragments [start, end):
        //   Included(key) < start → key is strictly before the first fragment → no overlap
        //   Excluded(key) <= start → key is at or before the first fragment → no overlap
        // Note: Included(key) == start means key IS in the fragment → overlap.
        match upper {
            Bound::Included(key)
                if let Some(first) = fragments.first()
                    && key < first.start.as_ref() =>
            {
                return false;
            }
            Bound::Excluded(key)
                if let Some(first) = fragments.first()
                    && key <= first.start.as_ref() =>
            {
                return false;
            }
            _ => {}
        }
        true
    }

    /// Write a batch through MVCC (used by transaction commit).
    pub(crate) fn mvcc_write_batch(&self, entries: &[(&[u8], &[u8], bool)]) -> Result<()> {
        let mvcc = self.mvcc.as_ref().expect("mvcc_write_batch requires MVCC");
        let (commit_ts, memtable, publish_data) = {
            let _read_guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            let (commit_ts, data) = mvcc.write_batch_wal_only(entries, &state.memtable)?;
            (commit_ts, state.memtable.clone(), data)
        };
        memtable.commit_wal()?;
        if !publish_data.is_empty() {
            let refs: Vec<(KeySlice, &[u8])> = publish_data
                .iter()
                .map(|(k, v)| (KeySlice::from_slice(k), v.as_slice()))
                .collect();
            memtable.publish_raw_batch(&refs)?;
        }
        // Advance current_ts AFTER publish.
        if commit_ts > 0 {
            mvcc.advance_ts(commit_ts);
        }
        self.try_freeze_memtable()?;

        Ok(())
    }

    /// Write a batch through MVCC without freezing the memtable.
    /// Used by serializable Transaction::commit which already holds commit_lock
    /// and manages its own lifecycle.
    /// Record a single-key write in `committed_txns` for serializable OCC.
    fn record_write(mvcc: &crate::mvcc::LsmMvccInner, commit_ts: u64, key: &[u8]) {
        let mut write_set = std::collections::HashSet::new();
        write_set.insert(bytes::Bytes::copy_from_slice(key));
        mvcc.record_committed_txn(commit_ts, write_set, 0);
    }

    pub(crate) fn mvcc_write_batch_inner(&self, entries: &[(&[u8], &[u8], bool)]) -> Result<u64> {
        let mvcc = self
            .mvcc
            .as_ref()
            .expect("mvcc_write_batch_inner requires MVCC");
        let (commit_ts, memtable, publish_data) = {
            let _read_guard = self.active_memtable_lock.read();
            // L5: Use load() (borrow) instead of load_full() (Arc clone)
            // to avoid an unnecessary atomic increment.
            let guard = self.state.load();
            let (commit_ts, data) = mvcc.write_batch_wal_only(entries, &guard.memtable)?;
            (commit_ts, guard.memtable.clone(), data)
        };
        memtable.commit_wal()?;
        if !publish_data.is_empty() {
            let refs: Vec<(KeySlice, &[u8])> = publish_data
                .iter()
                .map(|(k, v)| (KeySlice::from_slice(k), v.as_slice()))
                .collect();
            memtable.publish_raw_batch(&refs)?;
        }
        // Advance current_ts AFTER publish.
        if commit_ts > 0 {
            mvcc.advance_ts(commit_ts);
        }
        Ok(commit_ts)
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
        let read_ts_for_range = mvcc_read_ts.unwrap_or(u64::MAX);
        let memtable_range_ts = self.newest_memtable_range_ts(state, key, read_ts_for_range);
        self.rt_stats.note_lookup();
        if let Some((val, kind, _key, value_ts)) =
            self.lookup_memtable(state, key, bloom_hash, mvcc_read_ts)?
        {
            if memtable_range_ts.is_some_and(|rt| value_ts <= rt) {
                self.rt_stats.note_hit();
                return Ok((None, KvKind::Inline));
            }
            return Ok((val, kind));
        }
        // Only compute SST range tombstone ts when we fall through to the SST path.
        // Skip entirely if memtable already covers at read_ts.
        let range_ts = if memtable_range_ts.is_some_and(|ts| ts >= read_ts_for_range) {
            memtable_range_ts
        } else {
            memtable_range_ts.max(self.newest_sst_range_ts(state, key, read_ts_for_range))
        };
        if let Some((val, kind, _key, value_ts)) =
            self.lookup_sst_raw(state, encoded, bloom_hash, mvcc_read_ts, None, None)?
        {
            if range_ts.is_some_and(|rt| value_ts <= rt) {
                self.rt_stats.note_hit();
                return Ok((None, KvKind::Inline));
            }
            return Ok((val, kind));
        }

        if range_ts.is_some() {
            self.rt_stats.note_hit();
        }

        Ok((None, KvKind::Inline))
    }

    /// Find the newest range-tombstone timestamp covering `user_key` across
    /// all memtables (active + immutable) at `read_ts`.
    ///
    /// Returns `Some(ts)` if any memtable range tombstone covers the key.
    /// Both active and immutable memtables use cached fragment views (O(log F)).
    fn newest_memtable_range_ts(
        &self,
        state: &LsmStorageState,
        user_key: &[u8],
        read_ts: u64,
    ) -> Option<u64> {
        // Active memtable: lock-free fragment cache with O(1) bounds check.
        // Skip the is_empty() SkipMap traversal — cached_fragments() returns
        // an empty Vec when no tombstones exist, so frags.is_empty() is O(1).
        let rt = state.memtable.range_tombstones();
        let frags = rt.cached_fragments();
        let active_ts = if !frags.is_empty() {
            if let (Some(first), Some(last)) = (frags.first(), frags.last()) {
                if user_key < first.start.as_ref() || user_key >= last.end.as_ref() {
                    None
                } else {
                    crate::range_tombstone::find_newest_covering_ts(&frags, user_key, read_ts)
                }
            } else {
                None
            }
        } else {
            None
        };

        // Immutable memtables: cached fragment view.
        // Option<u64> implements Ord, so max correctly handles None cases.
        state.imm_memtables.iter().fold(active_ts, |best_ts, m| {
            let imm_ts = m
                .imm_range_tombstones()
                .and_then(|imm| imm.newest_covering_ts(user_key, read_ts));
            best_ts.max(imm_ts)
        })
    }

    /// Find the newest range-tombstone timestamp across all SSTs that cover
    /// `user_key` at `read_ts`.
    ///
    /// Iterates L0 + leveled SSTs and checks each SST's cached range-tombstone
    /// fragments. Returns the maximum covering timestamp, or `None` if no SST
    /// tombstone covers the key.
    fn newest_sst_range_ts(
        &self,
        state: &LsmStorageState,
        user_key: &[u8],
        read_ts: u64,
    ) -> Option<u64> {
        let mut best_ts: Option<u64> = None;
        let all_ssts = state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, ids)| ids))
            .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids));
        for id in all_ssts {
            if let Some(sst) = state.sstables.get(id)
                && let Some(frags) = sst.range_tombstone_fragments()
            {
                let ts = crate::range_tombstone::find_newest_covering_ts(frags, user_key, read_ts);
                best_ts = best_ts.max(ts);
                // find_newest_covering_ts only returns timestamps <= read_ts,
                // so best_ts can never exceed read_ts. Early exit.
                if best_ts == Some(read_ts) {
                    return best_ts;
                }
            }
        }
        best_ts
    }

    /// Shared memtable lookup used by `get()` and `get_with_kind_inner()`.
    /// Returns `Ok(None)` if the key is not found in any memtable.
    /// Returns `Ok(Some((value, kind, found_key)))` if found.
    /// `found_key` is the full encoded internal key of the matching entry,
    /// used for vLog key verification when the value is a ValuePointer.
    fn lookup_memtable(
        &self,
        state: &LsmStorageState,
        key: &[u8],
        bloom_hash: u32,
        mvcc_read_ts: Option<u64>,
    ) -> Result<Option<LookupResult>> {
        let vlog_enabled = self.vlog.is_some();
        if let Some(read_ts) = mvcc_read_ts {
            // MVCC path: pass user_key directly to avoid re-encoding.
            // The caller already has the raw user key; we use it for
            // versioned memtable lookup which encodes the seek key internally.
            // Skip bloom filter for the active memtable — it's small and the
            // key is very likely there (positive lookup), so the bloom hash
            // is pure overhead.
            if vlog_enabled {
                if let Some((raw, found_key)) = state
                    .memtable
                    .get_versioned_raw_with_key_and_hash(key, read_ts, bloom_hash)
                {
                    let (val, kind) = Self::parse_value_kind(raw);
                    let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                    return Ok(Some((val, kind, Bytes::from(found_key), vts)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some((raw, found_key)) = m.get_versioned_raw_with_key(key, read_ts) {
                        let (val, kind) = Self::parse_value_kind(raw);
                        let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                        return Ok(Some((val, kind, Bytes::from(found_key), vts)));
                    }
                }
            } else {
                // Non-vlog MVCC path: use get_versioned_raw_with_key to also
                // obtain the found key for version timestamp extraction.
                if let Some((raw, found_key)) = state
                    .memtable
                    .get_versioned_raw_with_key_and_hash(key, read_ts, bloom_hash)
                {
                    let (val, kind) = Self::parse_value_kind(raw);
                    let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                    return Ok(Some((val, kind, Bytes::from(found_key), vts)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some((raw, found_key)) = m.get_versioned_raw_with_key(key, read_ts) {
                        let (val, kind) = Self::parse_value_kind(raw);
                        let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                        return Ok(Some((val, kind, Bytes::from(found_key), vts)));
                    }
                }
            }
        } else {
            // Non-MVCC path: exact key lookup
            if vlog_enabled {
                if let Some(raw) = state.memtable.get_raw_with_hash(key, bloom_hash) {
                    let (val, kind) = Self::parse_value_kind(raw);
                    return Ok(Some((val, kind, Bytes::copy_from_slice(key), 0)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some(raw) = m.get_raw_with_hash(key, bloom_hash) {
                        let (val, kind) = Self::parse_value_kind(raw);
                        return Ok(Some((val, kind, Bytes::copy_from_slice(key), 0)));
                    }
                }
            } else {
                if let Some(v) = state.memtable.get_with_hash(key, bloom_hash) {
                    if crate::vlog::KvKind::is_tombstone_value(&v) {
                        return Ok(Some((None, KvKind::Inline, Bytes::new(), 0)));
                    }
                    return Ok(Some((Some(v), KvKind::Inline, Bytes::new(), 0)));
                }
                for m in state.imm_memtables.iter() {
                    if let Some(v) = m.get_with_hash(key, bloom_hash) {
                        if crate::vlog::KvKind::is_tombstone_value(&v) {
                            return Ok(Some((None, KvKind::Inline, Bytes::new(), 0)));
                        }
                        return Ok(Some((Some(v), KvKind::Inline, Bytes::new(), 0)));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Batch memtable lookup for sorted keys.
    ///
    /// `sorted_keys` must be sorted by raw user key bytes. Each entry is
    /// `(original_index, user_key)`. `bloom_hashes` is indexed by original_index.
    ///
    /// Returns a `Vec<Option<LookupResult>>` indexed by original key position.
    fn batch_lookup_memtable(
        &self,
        state: &LsmStorageState,
        sorted_keys: &[(usize, &[u8])],
        bloom_hashes: &[u32],
        mvcc_read_ts: Option<u64>,
        n: usize,
    ) -> Result<Vec<Option<LookupResult>>> {
        let mut results: Vec<Option<LookupResult>> = Vec::with_capacity(n);
        results.resize_with(n, || None);
        if sorted_keys.is_empty() {
            return Ok(results);
        }
        let Some(read_ts) = mvcc_read_ts else {
            // Non-MVCC: fall back to per-key lookup.
            for &(orig_idx, user_key) in sorted_keys {
                let h = bloom_hashes[orig_idx];
                if let Some(res) = self.lookup_memtable(state, user_key, h, mvcc_read_ts)? {
                    results[orig_idx] = Some(res);
                }
            }
            return Ok(results);
        };

        // Active memtable batch lookup.
        let active_hits = state
            .memtable
            .batch_get_versioned(sorted_keys, read_ts, bloom_hashes);
        for (orig_idx, raw, found_key) in active_hits {
            let (val, kind) = Self::parse_value_kind(raw);
            let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
            results[orig_idx] = Some((val, kind, found_key, vts));
        }

        // Immutable memtables — only look up keys not yet found.
        // Skip entirely if all keys were found in the active memtable.
        if !state.imm_memtables.is_empty() {
            let mut remaining: Vec<(usize, &[u8])> = sorted_keys
                .iter()
                .filter(|(idx, _)| results[*idx].is_none())
                .copied()
                .collect();
            if remaining.is_empty() {
                return Ok(results);
            }
            for m in state.imm_memtables.iter() {
                if remaining.is_empty() {
                    break;
                }
                let hits = m.batch_get_versioned(&remaining, read_ts, bloom_hashes);
                let mut found_any = false;
                for (orig_idx, raw, found_key) in hits {
                    let (val, kind) = Self::parse_value_kind(raw);
                    let vts = crate::key::extract_ts(&found_key).unwrap_or(0);
                    results[orig_idx] = Some((val, kind, found_key, vts));
                    found_any = true;
                }
                if found_any {
                    remaining.retain(|(idx, _)| results[*idx].is_none());
                }
            }
        }

        Ok(results)
    }

    /// Shared SST lookup for `get()` and `get_with_kind_inner()`.
    /// Searches L0 + leveled SSTs. Returns `Ok(None)` if not found.
    /// Returns `Ok(Some((value, kind, found_key, version_ts)))` with the parsed
    /// value, kind, the full encoded internal key, and the version timestamp.
    fn lookup_sst_raw(
        &self,
        state: &LsmStorageState,
        key: &[u8],
        bloom_hash: u32,
        mvcc_read_ts: Option<u64>,
        mut level_hint: Option<&mut ahash::AHashMap<usize, usize>>,
        mut l0_hint: Option<&mut usize>,
    ) -> Result<Option<LookupResult>> {
        // For MVCC: accumulate the newest visible version across ALL levels
        // (L0 + L1+) before returning. A key's versions may be split across
        // levels after compaction, so we must check every level.
        let mut best: Option<(Bytes, u64, Vec<u8>)> = None;

        // L0 SSTs — may overlap, check each one.
        // For sorted batch lookups, check the previously-hit L0 SST first
        // since consecutive keys often land in the same L0 SST.
        let l0_hint_val = l0_hint.as_ref().and_then(|h| {
            if **h < state.l0_sstables.len() {
                Some(**h)
            } else {
                None
            }
        });
        let mut best_l0: Option<usize> = None;
        if let Some(read_ts) = mvcc_read_ts {
            // Check hinted L0 SST first for an O(1) fast path.
            if let Some(hi) = l0_hint_val
                && let Some(s) = state
                    .l0_sstables
                    .get(hi)
                    .and_then(|id| state.sstables.get(id))
                && best
                    .as_ref()
                    .is_none_or(|(_, best_ts, _)| s.max_ts() > *best_ts)
                && let Some((raw, found_key)) =
                    s.point_get_with_hash_and_key(key, bloom_hash, Some(read_ts))?
            {
                let ts = crate::key::extract_ts(&found_key).unwrap_or(0);
                if best.as_ref().is_none_or(|(_, best_ts, _)| ts > *best_ts) {
                    best = Some((raw, ts, found_key));
                    best_l0 = Some(hi);
                }
            }
            for (i, id) in state.l0_sstables.iter().enumerate() {
                // Skip the hinted SST — already checked above.
                if l0_hint_val.is_some_and(|hi| hi == i) {
                    continue;
                }
                let s = match state.sstables.get(id) {
                    Some(s) => s,
                    None => continue,
                };
                // Skip SSTs that cannot contain a newer version than what
                // we already found (max_ts is the highest ts in this SST).
                if let Some((_, best_ts, _)) = best
                    && s.max_ts() <= best_ts
                {
                    continue;
                }
                if let Some((raw, found_key)) =
                    s.point_get_with_hash_and_key(key, bloom_hash, Some(read_ts))?
                {
                    let ts = crate::key::extract_ts(&found_key).unwrap_or(0);
                    if best.as_ref().is_none_or(|(_, best_ts, _)| ts > *best_ts) {
                        best = Some((raw, ts, found_key));
                        best_l0 = Some(i);
                    }
                }
            }
        } else {
            // Check hinted L0 SST first for an O(1) fast path.
            if let Some(hi) = l0_hint_val
                && let Some(s) = state
                    .l0_sstables
                    .get(hi)
                    .and_then(|id| state.sstables.get(id))
                && let Some(raw) = s.point_get_with_hash(key, bloom_hash)?
            {
                let (val, kind) = Self::parse_value_kind(raw);
                // Update hint before returning for next lookup.
                if let Some(ref mut h) = l0_hint {
                    **h = hi;
                }
                return Ok(Some((val, kind, Bytes::copy_from_slice(key), 0)));
            }
            for (i, id) in state.l0_sstables.iter().enumerate() {
                // Skip the hinted SST — already probed above.
                if l0_hint_val.is_some_and(|hi| hi == i) {
                    continue;
                }
                if let Some(s) = state.sstables.get(id)
                    && let Some(raw) = s.point_get_with_hash(key, bloom_hash)?
                {
                    let (val, kind) = Self::parse_value_kind(raw);
                    // Update hint before returning for next lookup.
                    if let Some(ref mut h) = l0_hint {
                        **h = i;
                    }
                    return Ok(Some((val, kind, Bytes::copy_from_slice(key), 0)));
                }
            }
        }
        if let Some(ref mut h) = l0_hint
            && let Some(idx) = best_l0
        {
            **h = idx;
        }
        // Leveled SSTs — with MVCC, accumulate the newest version across
        // all levels without returning early.
        let search_prefix = mvcc_read_ts
            .map(|_| {
                crate::key::encoded_user_key_prefix(key).ok_or_else(|| {
                    anyhow::anyhow!("invalid encoded internal key ({} bytes)", key.len())
                })
            })
            .transpose()?;
        for (level_idx, (_, sst_ids)) in state.levels.iter().enumerate() {
            if let Some(read_ts) = mvcc_read_ts {
                // Leveled SSTs (L1+) have non-overlapping user key ranges.
                // Binary search on user key prefix to find the rightmost
                // candidate, then scan left while the SST's last_key still
                // carries the same user key prefix — compaction may split a
                // key's versions across multiple adjacent SSTs.
                let search_prefix =
                    search_prefix.expect("search_prefix must be present when mvcc_read_ts is Some");

                // O(1) hint check: if the hinted SST's range still covers
                // the key, skip the binary search entirely.
                let try_hint = || -> Option<usize> {
                    let hint = level_hint.as_ref()?.get(&level_idx)?;
                    if *hint >= sst_ids.len() {
                        return None;
                    }
                    let sst = state.sstables.get(&sst_ids[*hint])?;
                    let fk = sst.first_key()?;
                    if fk.encoded_user_key() > search_prefix {
                        return None;
                    }
                    let lk = sst.last_key()?;
                    if lk.encoded_user_key() < search_prefix {
                        return None;
                    }
                    // Return hint + 1 as upper bound for (0..idx).rev() loop
                    // so the hinted SST itself is included in the scan.
                    Some(*hint + 1)
                };
                let idx = try_hint().unwrap_or_else(|| {
                    sst_ids.partition_point(|id| {
                        let sst = state
                            .sstables
                            .get(id)
                            .expect("SST must exist in sstables map");
                        match sst.first_key() {
                            Some(fk) => fk.encoded_user_key() <= search_prefix,
                            None => false,
                        }
                    })
                });
                if let Some(ref mut hint) = level_hint {
                    hint.insert(level_idx, idx.saturating_sub(1));
                }
                for i in (0..idx).rev() {
                    let sst = state
                        .sstables
                        .get(&sst_ids[i])
                        .expect("SST must exist in sstables map");
                    let Some(last_key) = sst.last_key() else {
                        continue;
                    };
                    if last_key.encoded_user_key() < search_prefix {
                        break;
                    }
                    // Remaining SSTs are sorted descending by key prefix;
                    // Skip SSTs that cannot contain a newer version.
                    // max_ts is NOT monotonically ordered across leveled SSTs
                    // (different SSTs cover different key ranges), so we must
                    // continue scanning rather than breaking.
                    if let Some((_, best_ts, _)) = best
                        && sst.max_ts() <= best_ts
                    {
                        continue;
                    }
                    if let Some((raw, found_key)) =
                        sst.point_get_with_hash_and_key(key, bloom_hash, Some(read_ts))?
                    {
                        let ts = crate::key::extract_ts(&found_key).unwrap_or(0);
                        if best.as_ref().is_none_or(|(_, best_ts, _)| ts > *best_ts) {
                            best = Some((raw, ts, found_key));
                        }
                    }
                }
            } else {
                // O(1) hint check for non-MVCC path.
                let try_hint = || -> Option<usize> {
                    let hint = level_hint.as_ref()?.get(&level_idx)?;
                    if *hint >= sst_ids.len() {
                        return None;
                    }
                    let sst = state.sstables.get(&sst_ids[*hint])?;
                    let fk = sst.first_key()?;
                    let lk = sst.last_key()?;
                    if key < fk.raw_ref() || key > lk.raw_ref() {
                        return None;
                    }
                    Some(*hint + 1)
                };
                let idx = try_hint().unwrap_or_else(|| {
                    sst_ids.partition_point(|id| match state.sstables[id].first_key() {
                        Some(fk) => fk.raw_ref() <= key,
                        None => false,
                    })
                });
                if let Some(ref mut hint) = level_hint {
                    hint.insert(level_idx, idx.saturating_sub(1));
                }
                if idx == 0 {
                    continue;
                }
                let candidate_idx = idx - 1;
                if let Some(s) = state.sstables.get(&sst_ids[candidate_idx])
                    && let Some(raw) = s.point_get_with_hash(key, bloom_hash)?
                {
                    let (val, kind) = Self::parse_value_kind(raw);
                    return Ok(Some((val, kind, Bytes::copy_from_slice(key), 0)));
                }
            }
        }
        if let Some((raw, best_ts, found_key)) = best {
            let (val, kind) = Self::parse_value_kind(raw);
            return Ok(Some((val, kind, Bytes::from(found_key), best_ts)));
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

    /// Encode a value with its kind byte prefix: `[kind_byte, value...]`.
    fn encode_kind_value(kind: KvKind, value: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + value.len());
        buf.push(kind as u8);
        buf.extend_from_slice(value);
        buf
    }

    /// Acquires state_lock, does a full LSM lookup, and conditionally writes
    /// the new value to the memtable if the current value matches (old, old_kind).
    /// Returns true if the swap succeeded.
    #[allow(dead_code)]
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

        let prefixed = Self::encode_kind_value(new_kind, new);
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
                let current_val = if crate::vlog::KvKind::is_tombstone_value(&v) {
                    None
                } else {
                    Some(v)
                };
                still_matches = Self::values_match(&current_val, KvKind::Inline, old, *old_kind);
            }

            if still_matches {
                let prefixed = Self::encode_kind_value(*new_kind, new);
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

    /// Version-aware batch CAS for vLog GC.
    ///
    /// Each entry specifies `(user_key, ts, old_value, old_kind, new_value, new_kind)`.
    /// Uses exact internal-key lookup (`get_raw_exact`) instead of version-scanning,
    /// so it swaps the correct version even when newer versions exist.
    pub(crate) fn compare_and_set_batch_at_ts(
        &self,
        entries: &[VersionedCasEntry],
    ) -> Result<Vec<bool>> {
        let _lock = self.state_lock.lock();

        // Phase 1: Lookups under read lock — check exact version.
        // Carry pre-computed (encoded, bloom_hash) forward so Phase 2 can
        // reuse them without re-encoding or re-hashing.
        let mut candidates: Vec<Option<(Vec<u8>, u32)>> = Vec::with_capacity(entries.len());
        {
            let state = self.state.load_full();
            let mut encode_buf = Vec::with_capacity(64);
            for (user_key, ts, old, old_kind, _, _) in entries {
                encode_buf.clear();
                crate::key::encode_internal_key_to_buf(&mut encode_buf, user_key, *ts);
                let bloom_hash = crate::table::bloom::hash_key(user_key);
                // Check memtable + imm_memtables for the exact version
                let current = std::iter::once(&state.memtable)
                    .chain(state.imm_memtables.iter())
                    .find_map(|m| {
                        m.get_raw_exact_with_hash(&encode_buf, bloom_hash)
                            .map(Self::parse_value_kind)
                    });
                let matches = match current {
                    Some((val, kind)) => Self::values_match(&val, kind, old, *old_kind),
                    None => {
                        let (val, kind) = self.get_with_kind_at_ts(user_key, *ts)?;
                        Self::values_match(&val, kind, old, *old_kind)
                    }
                };
                candidates.push(if matches {
                    Some((encode_buf.clone(), bloom_hash))
                } else {
                    None
                });
            }
        }

        // Phase 2: Re-verify and write under exclusive lock
        let _mt_guard = self.active_memtable_lock.write();
        let state = self.state.load_full();

        let mut results = vec![false; entries.len()];
        // Store owned encoded keys to avoid lifetime issues.
        let mut writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());

        for (i, (_, _, old, old_kind, new, new_kind)) in entries.iter().enumerate() {
            let Some((encoded, bloom_hash)) = candidates[i].take() else {
                continue;
            };

            // Re-verify against the memtable using the pre-computed key
            let mut still_matches = true;
            if let Some(raw) = state.memtable.get_raw_exact_with_hash(&encoded, bloom_hash) {
                let (current_val, current_kind) = Self::parse_value_kind(raw);
                still_matches = Self::values_match(&current_val, current_kind, old, *old_kind);
            }

            if still_matches {
                let prefixed = Self::encode_kind_value(*new_kind, new);
                writes.push((encoded, prefixed));
                results[i] = true;
            }
        }

        if !writes.is_empty() {
            let raw_refs: Vec<(KeySlice, &[u8])> = writes
                .iter()
                .map(|(k, v)| (KeySlice::from_slice(k), v.as_slice()))
                .collect();
            // GC rewrites are idempotent — skip WAL to avoid replaying
            // stale pointers on recovery.
            state.memtable.put_raw_batch_no_wal(&raw_refs)?;
        }

        Ok(results)
    }

    /// Write a batch of data into the storage.
    /// Canonicalizes duplicate user keys: only the last operation per key in
    /// the batch is written. When MVCC is enabled, all entries share a single
    /// commit timestamp.
    #[allow(clippy::type_complexity)]
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        // MVP: reject mixed batches containing both point and range operations.
        let has_point = batch
            .iter()
            .any(|r| matches!(r, WriteBatchRecord::Put(..) | WriteBatchRecord::Del(_)));
        let has_range = batch
            .iter()
            .any(|r| matches!(r, WriteBatchRecord::DelRange(..)));
        anyhow::ensure!(
            !(has_point && has_range),
            "mixed point/range batches are not supported in the MVP"
        );

        // Range-only batch: allocate a single commit_ts for all entries.
        if has_range {
            anyhow::ensure!(
                !self.options.serializable,
                "delete_range is not supported with serializable mode in the MVP"
            );
            anyhow::ensure!(
                self.vlog.is_none(),
                "delete_range is not supported when value-log (vlog) is enabled in the MVP"
            );
            let entries: Vec<(&[u8], &[u8])> = batch
                .iter()
                .filter_map(|r| {
                    if let WriteBatchRecord::DelRange(start, end) = r {
                        Some((start.as_ref(), end.as_ref()))
                    } else {
                        None
                    }
                })
                .collect();
            for (start, end) in &entries {
                Self::validate_key_size(start)?;
                Self::validate_key_size(end)?;
                anyhow::ensure!(
                    start < end,
                    "invalid range: start ({:?}) must be less than end ({:?})",
                    start,
                    end
                );
            }
            // H3: Write to WAL buffer only under the lock, then release
            // the lock before syncing. Publish to memtable after sync succeeds.
            let (memtable, rt_ts) = {
                let _guard = self.active_memtable_lock.read();
                let state = self.state.load_full();
                let ts = if let Some(ref mvcc) = self.mvcc {
                    mvcc.write_range_batch_wal_only(&entries, &state.memtable)?
                } else {
                    state
                        .memtable
                        .put_range_tombstone_batch_wal_only(&entries, 0, 0)?;
                    0
                };
                (state.memtable.clone(), ts)
            };
            memtable.commit_wal()?;
            // Publish range tombstones AFTER WAL sync succeeds.
            memtable.publish_range_tombstones(&entries, rt_ts, 0)?;
            // Advance current_ts AFTER publish.
            if rt_ts > 0
                && let Some(ref mvcc) = self.mvcc
            {
                mvcc.advance_ts(rt_ts);
            }
            return self.try_freeze_memtable();
        }

        // Validate key sizes before any writes.
        for record in batch {
            let key = match record {
                WriteBatchRecord::Del(k) => k.as_ref(),
                WriteBatchRecord::Put(k, _) => k.as_ref(),
                WriteBatchRecord::DelRange(_, _) => unreachable!(),
            };
            Self::validate_key_size(key)?;
        }
        // Most benchmark and application batches contain unique keys. Avoid
        // building and sorting a full last-op map on that hot path.
        let mut has_duplicate_keys = false;
        if batch.len() > 1 {
            let mut seen = std::collections::HashSet::with_capacity(batch.len());
            for record in batch {
                let key = match record {
                    WriteBatchRecord::Del(k) => k.as_ref(),
                    WriteBatchRecord::Put(k, _) => k.as_ref(),
                    WriteBatchRecord::DelRange(_, _) => unreachable!(),
                };
                if !seen.insert(key) {
                    has_duplicate_keys = true;
                    break;
                }
            }
        }

        let dedup_indices = if has_duplicate_keys {
            // Deduplicate: keep only the last operation per user key.
            let mut last_op = std::collections::HashMap::with_capacity(batch.len());
            for (idx, record) in batch.iter().enumerate() {
                let key = match record {
                    WriteBatchRecord::Del(k) => k.as_ref(),
                    WriteBatchRecord::Put(k, _) => k.as_ref(),
                    WriteBatchRecord::DelRange(_, _) => unreachable!(),
                };
                last_op.insert(key, idx);
            }

            // Sort indices to preserve original insertion order.
            let mut indices: Vec<_> = last_op.values().copied().collect();
            indices.sort_unstable();
            Some(indices)
        } else {
            None
        };

        // Defer serializable txn recording until after commit_wal succeeds.
        let mut txn_info: Option<(u64, std::collections::HashSet<bytes::Bytes>)> = None;
        // Track commit_ts for advancing current_ts after publish.
        let mut mvcc_commit_ts: u64 = 0;

        // M5: WAL-only writes under the lock. Publish to skiplist happens
        // AFTER commit_wal succeeds, preventing ghost entries on sync failure.
        let (memtable, publish_data): (Arc<MemTable>, Vec<(Vec<u8>, Vec<u8>)>) = {
            let _commit_guard = self.mvcc.as_ref().and_then(|mvcc| {
                if self.options.serializable {
                    Some(mvcc.commit_lock.lock())
                } else {
                    None
                }
            });
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                // MVCC path: allocate a single commit_ts for the entire batch.
                let entries: Vec<(&[u8], &[u8], bool)> =
                    if let Some(indices) = dedup_indices.as_ref() {
                        indices
                            .iter()
                            .map(|&idx| match &batch[idx] {
                                WriteBatchRecord::Put(key, value) => {
                                    (key.as_ref(), value.as_ref(), false)
                                }
                                WriteBatchRecord::Del(key) => (key.as_ref(), &[] as &[u8], true),
                                WriteBatchRecord::DelRange(_, _) => unreachable!(),
                            })
                            .collect()
                    } else {
                        batch
                            .iter()
                            .map(|record| match record {
                                WriteBatchRecord::Put(key, value) => {
                                    (key.as_ref(), value.as_ref(), false)
                                }
                                WriteBatchRecord::Del(key) => (key.as_ref(), &[] as &[u8], true),
                                WriteBatchRecord::DelRange(_, _) => unreachable!(),
                            })
                            .collect()
                    };
                // WAL-only: do NOT publish to skiplist yet.
                let (commit_ts, data) = mvcc.write_batch_wal_only(&entries, &state.memtable)?;
                mvcc_commit_ts = commit_ts;
                // Defer record_committed_txn until after commit_wal succeeds
                // so that failed WAL syncs don't poison serializable validation.
                if self.options.serializable && commit_ts > 0 {
                    let mut write_set = std::collections::HashSet::with_capacity(
                        dedup_indices
                            .as_ref()
                            .map_or(batch.len(), std::vec::Vec::len),
                    );
                    if let Some(indices) = dedup_indices.as_ref() {
                        for &idx in indices {
                            let key = match &batch[idx] {
                                WriteBatchRecord::Put(k, _) => k.as_ref(),
                                WriteBatchRecord::Del(k) => k.as_ref(),
                                WriteBatchRecord::DelRange(_, _) => unreachable!(),
                            };
                            write_set.insert(bytes::Bytes::copy_from_slice(key));
                        }
                    } else {
                        for record in batch {
                            let key = match record {
                                WriteBatchRecord::Put(k, _) => k.as_ref(),
                                WriteBatchRecord::Del(k) => k.as_ref(),
                                WriteBatchRecord::DelRange(_, _) => unreachable!(),
                            };
                            write_set.insert(bytes::Bytes::copy_from_slice(key));
                        }
                    }
                    txn_info = Some((commit_ts, write_set));
                }
                (state.memtable.clone(), data)
            } else {
                // Non-MVCC path: write raw user keys to WAL only.
                let mut raw_data = Vec::with_capacity(
                    dedup_indices
                        .as_ref()
                        .map_or(batch.len(), std::vec::Vec::len),
                );
                if let Some(indices) = dedup_indices.as_ref() {
                    for &idx in indices {
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
                            WriteBatchRecord::DelRange(_, _) => unreachable!(),
                        }
                    }
                } else {
                    for record in batch {
                        match record {
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
                            WriteBatchRecord::DelRange(_, _) => unreachable!(),
                        }
                    }
                }
                let refs: Vec<(KeySlice, &[u8])> =
                    raw_data.iter().map(|(k, v)| (*k, v.as_slice())).collect();
                state.memtable.write_wal_batch_only(&refs)?;
                // Build owned publish data from raw_data.
                let data: Vec<(Vec<u8>, Vec<u8>)> = raw_data
                    .into_iter()
                    .map(|(k, v)| (k.raw_ref().to_vec(), v))
                    .collect();
                (state.memtable.clone(), data)
            }
        };
        // Phase 2: After locks released, commit_wal() then publish to skiplist.
        memtable.commit_wal()?;
        // Publish to skiplist + bloom AFTER WAL sync succeeds.
        if !publish_data.is_empty() {
            let refs: Vec<(KeySlice, &[u8])> = publish_data
                .iter()
                .map(|(k, v)| (KeySlice::from_slice(k), v.as_slice()))
                .collect();
            memtable.publish_raw_batch(&refs)?;
        }
        // Advance current_ts AFTER publish — readers must not see the
        // timestamp before data is visible in the skiplist.
        if mvcc_commit_ts > 0
            && let Some(ref mvcc) = self.mvcc
        {
            mvcc.advance_ts(mvcc_commit_ts);
        }
        // Record serializable txn AFTER WAL sync succeeds, so that failed
        // syncs don't poison the committed_txns set.
        if let Some((commit_ts, write_set)) = txn_info
            && let Some(ref mvcc) = self.mvcc
        {
            mvcc.record_committed_txn(commit_ts, write_set, 0);
        }
        self.try_freeze_memtable()
    }

    pub(crate) fn try_freeze_memtable(&self) -> Result<()> {
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
        Self::validate_key_size(key)?;
        // Phase 1: Under locks, write to WAL buffer only (no skiplist insert).
        // Phase 2: After locks released, commit_wal() then publish to skiplist.
        // This ensures data is not visible to readers if the WAL sync fails.
        let (memtable, publish_data) = {
            let _commit_guard = self.mvcc.as_ref().and_then(|mvcc| {
                if self.options.serializable {
                    Some(mvcc.commit_lock.lock())
                } else {
                    None
                }
            });
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                let (commit_ts, encoded_key, prefixed_val) =
                    mvcc.write_wal_only(key, value, &state.memtable)?;
                (
                    state.memtable.clone(),
                    Some((commit_ts, encoded_key, prefixed_val)),
                )
            } else {
                // Non-MVCC path: write to WAL, publish after sync.
                let mut prefixed = Vec::with_capacity(1 + value.len());
                prefixed.push(crate::vlog::KvKind::Inline as u8);
                prefixed.extend_from_slice(value);
                state.memtable.write_wal_batch_only(&[(
                    crate::key::KeySlice::from_slice(key),
                    prefixed.as_slice(),
                )])?;
                (state.memtable.clone(), Some((0, key.to_vec(), prefixed)))
            }
        };
        // M5: commit_wal() is called outside the active_memtable_lock.
        memtable.commit_wal()?;
        // Publish to skiplist + bloom AFTER WAL sync succeeds.
        if let Some((commit_ts, encoded_key, prefixed_val)) = publish_data {
            memtable.publish_raw_batch(&[(
                crate::key::KeySlice::from_slice(&encoded_key),
                prefixed_val.as_slice(),
            )])?;
            // Advance current_ts AFTER publish — readers must not see the
            // timestamp before data is visible in the skiplist.
            if commit_ts > 0
                && let Some(ref mvcc) = self.mvcc
            {
                mvcc.advance_ts(commit_ts);
            }
            if self.options.serializable
                && let Some(ref mvcc) = self.mvcc
            {
                Self::record_write(mvcc, commit_ts, key);
            }
        }
        self.try_freeze_memtable()
    }

    /// Remove a key from the storage by writing a tombstone marker.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        Self::validate_key_size(key)?;
        let (memtable, publish_data) = {
            let _commit_guard = self.mvcc.as_ref().and_then(|mvcc| {
                if self.options.serializable {
                    Some(mvcc.commit_lock.lock())
                } else {
                    None
                }
            });
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                let (commit_ts, encoded_key, tombstone_val) =
                    mvcc.write_tombstone_wal_only(key, &state.memtable)?;
                (
                    state.memtable.clone(),
                    Some((commit_ts, encoded_key, tombstone_val)),
                )
            } else {
                // Non-MVCC path: write to WAL, publish after sync.
                let tombstone_val = vec![crate::vlog::KvKind::Tombstone as u8];
                state.memtable.write_wal_batch_only(&[(
                    crate::key::KeySlice::from_slice(key),
                    tombstone_val.as_slice(),
                )])?;
                (
                    state.memtable.clone(),
                    Some((0, key.to_vec(), tombstone_val)),
                )
            }
        };
        memtable.commit_wal()?;
        if let Some((commit_ts, encoded_key, tombstone_val)) = publish_data {
            memtable.publish_raw_batch(&[(
                crate::key::KeySlice::from_slice(&encoded_key),
                tombstone_val.as_slice(),
            )])?;
            // Advance current_ts AFTER publish — readers must not see the
            // timestamp before data is visible in the skiplist.
            if commit_ts > 0
                && let Some(ref mvcc) = self.mvcc
            {
                mvcc.advance_ts(commit_ts);
            }
            if self.options.serializable
                && let Some(ref mvcc) = self.mvcc
            {
                Self::record_write(mvcc, commit_ts, key);
            }
        }
        self.try_freeze_memtable()
    }

    /// Internal range delete: hides all keys in `[start, end)` at a new commit timestamp.
    ///
    /// This is the Phase 1 test hook. It validates bounds, writes the range
    /// tombstone into the active memtable, and publishes the commit timestamp.
    /// The tombstone is also WAL-durable when WAL is enabled (via the
    /// memtable's WAL integration — to be wired in Phase 2/3).
    pub fn delete_range_internal(&self, start: &[u8], end: &[u8]) -> Result<()> {
        anyhow::ensure!(
            !self.options.serializable,
            "delete_range is not supported with serializable mode in the MVP"
        );
        anyhow::ensure!(
            self.vlog.is_none(),
            "delete_range is not supported when value-log (vlog) is enabled in the MVP"
        );
        anyhow::ensure!(
            start < end,
            "invalid range: start ({:?}) must be less than end ({:?})",
            start,
            end
        );
        Self::validate_key_size(start)?;
        Self::validate_key_size(end)?;

        {
            let _guard = self.active_memtable_lock.read();
            let state = self.state.load_full();
            if let Some(ref mvcc) = self.mvcc {
                mvcc.write_range_tombstone(start, end, &state.memtable)?;
            } else {
                // Non-MVCC path: use ts=0 as a sentinel.
                state.memtable.put_range_tombstone(start, end, 0, 0)?;
            }
        }
        // M5: The WAL file lifecycle is managed by flush, which only deletes
        // the WAL file after the SST is durably written. Between releasing
        // the lock above and the commit_wal inside the mvcc/memtable calls,
        // the memtable may be frozen but the WAL file is still alive.
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
    pub(crate) fn maybe_snapshot_manifest(&self, _state_lock: &MutexGuard<'_, ()>) -> Result<()> {
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

        let mut vlog_references = if self.vlog.is_some() {
            Vec::with_capacity(state.sstables.len())
        } else {
            Vec::new()
        };
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
        let registry = self.compaction_filters.lock();

        let record = ManifestRecord::Snapshot {
            l0_sstables: state.l0_sstables.clone(),
            levels: state.levels.clone(),
            range_only_ssts: state.range_only_ssts.clone(),
            next_sst_id: self.next_sst_id.load(std::sync::atomic::Ordering::Acquire),
            vlog_references,
            imm_memtable_ids: state.imm_memtables.iter().map(|m| m.id()).collect(),
            active_compaction_filters: registry.snapshot_filters(),
            next_compaction_filter_id: registry.next_compaction_filter_id,
            format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
        };
        drop(registry);
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
        // Build immutable range-tombstone fragment cache before sharing.
        // OnceLock ensures exactly-once initialization even through &self.
        m.freeze_range_tombstones();
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
            mem_table::MemTable::create_with_wal(sst_id, vlog_enabled, self.path_of_wal(sst_id))?
        } else {
            mem_table::MemTable::create(sst_id, vlog_enabled)
        };
        mem_table.set_write_profile(self.write_profile.clone());
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
            let vs_opts = self
                .options
                .value_separation
                .as_ref()
                .ok_or_else(|| anyhow!("vLog present but value_separation options missing"))?
                .clone();
            let vlog_builder = crate::vlog::ValueLogBuilder::create(
                vlog.path_of_file(vlog_file_id),
                vlog_file_id,
                vs_opts.clone(),
            )?;
            let mut builder =
                SsTableBuilder::new_with_vlog(self.options.block_size, vlog_builder, vs_opts);
            builder.set_collect_blocks(self.options.enable_cache_backfill);
            builder.set_prefix_bloom_options(Some(self.options.prefix_bloom.clone()));
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
                log::warn!("failed to save vLog index for {}: {}", vlog_file_id, e);
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
            builder.set_prefix_bloom_options(Some(self.options.prefix_bloom.clone()));
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
                    log::warn!("failed to remove WAL {}: {}", wal_path.display(), e);
                }
            }
        }

        Ok(())
    }

    /// Create a new MVCC transaction with snapshot isolation.
    ///
    /// Requires MVCC to be enabled. The transaction's read timestamp
    /// is pinned in the watermark to prevent GC of visible versions.
    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<crate::mvcc::txn::Transaction>> {
        let mvcc = self.mvcc.as_ref().expect("new_txn requires MVCC");
        Ok(mvcc.new_txn(Arc::clone(self), self.options.serializable))
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
        let mvcc_read_ts = read_guard.as_ref().map(|g| g.read_ts());
        let lit = self.scan_inner(lower, upper, mvcc_read_ts, None)?;

        Ok(ScanIterator::new(lit, read_guard))
    }

    /// Create an iterator for a prefix scan with prefix bloom filter pruning.
    pub fn prefix_scan(&self, prefix: &[u8]) -> Result<ScanIterator> {
        if prefix.is_empty() {
            return self.scan(Bound::Unbounded, Bound::Unbounded);
        }
        let read_guard = self.mvcc.as_ref().map(|m| m.new_read_guard());
        let mvcc_read_ts = read_guard.as_ref().map(|g| g.read_ts());
        let upper_bound = prefix_upper_bound(prefix);
        let lower = Bound::Included(prefix);
        let upper = match &upper_bound {
            Some(upper) => Bound::Excluded(upper.as_slice()),
            None => Bound::Unbounded,
        };
        let lit = self.scan_inner(lower, upper, mvcc_read_ts, Some(prefix))?;
        Ok(ScanIterator::new(lit, read_guard))
    }

    fn into_vec(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
        match b {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}
