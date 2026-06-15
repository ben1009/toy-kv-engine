mod leveled;
mod simple_leveled;
mod tiered;

use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    key::KeySlice,
    lsm_storage::{LsmStorageInner, LsmStorageState},
    manifest::ManifestRecord,
    table::{SsTable, SsTableBuilder, SsTableIterator},
    vlog::gc::GarbageCollector,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    /// only used when only have l0, l1 levels, so when used, always compact_to_bottom_level = true
    /// by default
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }

    /// Returns `true` if the compaction target is within the upper levels
    /// (L0, L1, L2). Data in these levels is recently flushed or recently
    /// compacted — likely hot and safe to backfill.
    fn is_upper_level_compaction(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => false,
            CompactionTask::Leveled(task) => task.lower_level <= 2,
            CompactionTask::Simple(task) => task.lower_level <= 2,
            // Tiered: tier_id is SST ID (not level), so check via
            // bottom_tier_included. Minor compactions (not including
            // the cold bottom tier) are safe to backfill.
            CompactionTask::Tiered(task) => !task.bottom_tier_included,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_to_bottom_level() {
        let force = CompactionTask::ForceFullCompaction {
            l0_sstables: vec![],
            l1_sstables: vec![],
        };
        assert!(force.compact_to_bottom_level());

        let leveled = CompactionTask::Leveled(LeveledCompactionTask {
            upper_level: Some(1),
            upper_level_sst_ids: vec![],
            lower_level: 2,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: true,
        });
        assert!(leveled.compact_to_bottom_level());

        let leveled_no = CompactionTask::Leveled(LeveledCompactionTask {
            upper_level: Some(1),
            upper_level_sst_ids: vec![],
            lower_level: 2,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: false,
        });
        assert!(!leveled_no.compact_to_bottom_level());

        let simple = CompactionTask::Simple(SimpleLeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids: vec![],
            lower_level: 1,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: true,
        });
        assert!(simple.compact_to_bottom_level());

        let tiered = CompactionTask::Tiered(TieredCompactionTask {
            tiers: vec![],
            bottom_tier_included: true,
        });
        assert!(tiered.compact_to_bottom_level());

        let tiered_no = CompactionTask::Tiered(TieredCompactionTask {
            tiers: vec![],
            bottom_tier_included: false,
        });
        assert!(!tiered_no.compact_to_bottom_level());
    }

    #[test]
    fn test_is_upper_level_compaction() {
        let force = CompactionTask::ForceFullCompaction {
            l0_sstables: vec![],
            l1_sstables: vec![],
        };
        assert!(!force.is_upper_level_compaction());

        // Leveled: lower_level=1 → upper
        let lev1 = CompactionTask::Leveled(LeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids: vec![],
            lower_level: 1,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: false,
        });
        assert!(lev1.is_upper_level_compaction());

        // Leveled: lower_level=2 → upper
        let lev2 = CompactionTask::Leveled(LeveledCompactionTask {
            upper_level: Some(1),
            upper_level_sst_ids: vec![],
            lower_level: 2,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: false,
        });
        assert!(lev2.is_upper_level_compaction());

        // Leveled: lower_level=3 → NOT upper
        let lev3 = CompactionTask::Leveled(LeveledCompactionTask {
            upper_level: Some(2),
            upper_level_sst_ids: vec![],
            lower_level: 3,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: true,
        });
        assert!(!lev3.is_upper_level_compaction());

        // Simple: lower_level=2 → upper
        let simp = CompactionTask::Simple(SimpleLeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids: vec![],
            lower_level: 2,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: false,
        });
        assert!(simp.is_upper_level_compaction());

        // Simple: lower_level=5 → NOT upper
        let simp5 = CompactionTask::Simple(SimpleLeveledCompactionTask {
            upper_level: Some(4),
            upper_level_sst_ids: vec![],
            lower_level: 5,
            lower_level_sst_ids: vec![],
            is_lower_level_bottom_level: true,
        });
        assert!(!simp5.is_upper_level_compaction());

        // Tiered: bottom_tier_included=false → upper (minor compaction)
        let tiered_minor = CompactionTask::Tiered(TieredCompactionTask {
            tiers: vec![],
            bottom_tier_included: false,
        });
        assert!(tiered_minor.is_upper_level_compaction());

        // Tiered: bottom_tier_included=true → NOT upper
        let tiered_major = CompactionTask::Tiered(TieredCompactionTask {
            tiers: vec![],
            bottom_tier_included: true,
        });
        assert!(!tiered_major.is_upper_level_compaction());
    }

    #[test]
    fn test_compaction_controller_flush_to_l0() {
        assert!(CompactionController::NoCompaction.flush_to_l0());
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        new_sst_ids: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, new_sst_ids, false)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, new_sst_ids)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, new_sst_ids)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        compact_to_bottom_level: bool,
        is_upper_level_compaction: bool,
    ) -> Result<(Vec<Arc<SsTable>>, Vec<u32>)> {
        // Only backfill upper-level (L0/L1/L2) compactions. Data in these
        // levels is recently flushed or recently compacted — likely hot.
        // Deeper compactions operate on cold data — backfilling them would
        // evict hotter blocks via force_put.
        let should_backfill = self.options.enable_cache_backfill && is_upper_level_compaction;
        let mut ret = vec![];
        let mut all_vlog_ids = vec![];
        let mut builder = SsTableBuilder::new(self.options.block_size);
        builder.set_collect_blocks(should_backfill);
        builder.set_prefix_bloom_options(Some(self.options.prefix_bloom.clone()));

        // Snapshot the watermark once before the loop. When no active readers
        // exist, watermark() returns latest_commit_ts so everything below it
        // is eligible for GC.
        let watermark = self.mvcc.as_ref().map(|m| m.watermark());
        let compaction_filters = self.snapshot_compaction_filters();
        let max_filter_cutoff = compaction_filters.iter().map(|f| f.cutoff_ts).max();
        let can_publish_filter_deletion = compact_to_bottom_level
            && !compaction_filters.is_empty()
            && max_filter_cutoff.is_some_and(|cutoff_ts| {
                self.mvcc
                    .as_ref()
                    .is_some_and(|m| m.can_publish_filter_deletion(cutoff_ts))
            });

        // Track state for watermark-aware version dropping. Keys iterate
        // newest-first within each user key (inverted ts encoding), so we
        // keep all versions above the watermark and only the newest version
        // at or below the watermark per user key.
        let mut prev_user_key: Vec<u8> = Vec::new();
        let mut seen_below_watermark = false;
        let mut decoded_user_key = Vec::new();

        while iter.is_valid() {
            if builder.estimated_size() >= self.options.target_sst_size {
                all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
                let sst_id = self.next_sst_id();
                let (sst, blocks) = builder.build_with_backfill(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;
                if should_backfill {
                    self.block_cache.backfill(sst_id, blocks);
                }
                ret.push(Arc::new(sst));
                builder = SsTableBuilder::new(self.options.block_size);
                builder.set_collect_blocks(should_backfill);
                builder.set_prefix_bloom_options(Some(self.options.prefix_bloom.clone()));
            }

            // Detect tombstones: single KvKind::Tombstone byte.
            let raw = iter.raw_value();
            let is_tombstone = raw.len() == 1 && raw[0] == crate::vlog::KvKind::Tombstone as u8;

            let should_keep = if let Some(wm) = watermark {
                let key = iter.key();
                let ts = key.ts();
                let user_key = key.encoded_user_key();

                // New user key group — reset tracking state
                if user_key != prev_user_key {
                    prev_user_key.clear();
                    prev_user_key.extend_from_slice(user_key);
                    seen_below_watermark = false;
                }

                if ts > wm {
                    // Active readers might need this version — always keep
                    true
                } else if !seen_below_watermark {
                    // First version at or below the watermark for this user
                    // key — keep as the newest visible version. Drop tombstones
                    // at the bottom level since no reader can see them.
                    seen_below_watermark = true;
                    !(is_tombstone && compact_to_bottom_level)
                } else {
                    // Older version at or below the watermark — drop
                    false
                }
            } else {
                // No MVCC — drop tombstones at the bottom level only
                !is_tombstone || !compact_to_bottom_level
            };

            let should_drop_for_filter =
                should_keep && can_publish_filter_deletion && !is_tombstone && {
                    let key = iter.key();
                    key.decode_user_key_into(&mut decoded_user_key);
                    self.record_compaction_filter_check();
                    compaction_filters
                        .iter()
                        .any(|filter| filter.matches(&decoded_user_key, key.ts()))
                };

            if should_keep && !should_drop_for_filter {
                builder.add_raw(iter.key(), iter.raw_value())?;
            } else if should_drop_for_filter {
                let dropped_bytes = (iter.key().raw_ref().len() + iter.raw_value().len()) as u64;
                self.record_compaction_filter_drop(dropped_bytes);
            }
            iter.next()?;
        }

        if !builder.is_empty() {
            all_vlog_ids.extend_from_slice(builder.vlog_file_ids());
            let sst_id = self.next_sst_id();
            let (sst, blocks) = builder.build_with_backfill(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            if should_backfill {
                self.block_cache.backfill(sst_id, blocks);
            }
            ret.push(Arc::new(sst));
        }

        all_vlog_ids.sort_unstable();
        all_vlog_ids.dedup();
        Ok((ret, all_vlog_ids))
    }

    fn compact_from_iters(
        &self,
        upper_level_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        lower_level_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        compact_to_bottom_level: bool,
        is_upper_level_compaction: bool,
    ) -> Result<(Vec<Arc<SsTable>>, Vec<u32>)> {
        let s_it = TwoMergeIterator::create(upper_level_iter, lower_level_iter)?;

        self.compact_from_iter(s_it, compact_to_bottom_level, is_upper_level_compaction)
    }

    fn compact_from_l0_l1(
        &self,
        l0_sst_ids: &[usize],
        l1_sst_ids: &[usize],
        compact_to_bottom_level: bool,
        is_upper_level_compaction: bool,
    ) -> Result<(Vec<Arc<SsTable>>, Vec<u32>)> {
        let state = self.state.load_full();
        let mut m_it = vec![];
        for i in l0_sst_ids {
            let t = state.sstables[i].clone();
            let s = SsTableIterator::create_and_seek_to_first(t.clone())?;
            m_it.push(Box::new(s));
        }
        let upper_level_iter = MergeIterator::create(m_it);

        let mut s_lower = vec![];
        for i in l1_sst_ids {
            let t = state.sstables[i].clone();
            s_lower.push(t);
        }
        let lower_level_iter = SstConcatIterator::create_and_seek_to_first(s_lower)?;

        self.compact_from_iters(
            upper_level_iter,
            lower_level_iter,
            compact_to_bottom_level,
            is_upper_level_compaction,
        )
    }

    fn compact(&self, task: &CompactionTask) -> Result<(Vec<Arc<SsTable>>, Vec<u32>)> {
        let state = self.state.load_full();
        match task {
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => {
                if upper_level.is_none() {
                    self.compact_from_l0_l1(
                        upper_level_sst_ids,
                        lower_level_sst_ids,
                        task.compact_to_bottom_level(),
                        task.is_upper_level_compaction(),
                    )
                } else {
                    let mut s_upper = vec![];
                    for i in upper_level_sst_ids.iter() {
                        let t = state.sstables[i].clone();
                        s_upper.push(t);
                    }
                    let upper_level_iter = SstConcatIterator::create_and_seek_to_first(s_upper)?;

                    let mut s_lower = vec![];
                    for i in lower_level_sst_ids.iter() {
                        let t = state.sstables[i].clone();
                        s_lower.push(t);
                    }
                    let lower_level_iter = SstConcatIterator::create_and_seek_to_first(s_lower)?;

                    self.compact_from_iters(
                        upper_level_iter,
                        lower_level_iter,
                        task.compact_to_bottom_level(),
                        task.is_upper_level_compaction(),
                    )
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_from_l0_l1(
                l0_sstables,
                l1_sstables,
                task.compact_to_bottom_level(),
                task.is_upper_level_compaction(),
            ),
            CompactionTask::Tiered(t) => {
                let mut s_iters = vec![];
                for tier in &t.tiers {
                    let mut sstables = vec![];
                    for i in tier.1.iter() {
                        let t = state.sstables[i].clone();
                        sstables.push(t);
                    }
                    s_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        sstables,
                    )?));
                }
                let m_iter = MergeIterator::create(s_iters);
                self.compact_from_iter(
                    m_iter,
                    task.compact_to_bottom_level(),
                    task.is_upper_level_compaction(),
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let state = self.state.load_full();
        let ssts_to_compact = (&state.l0_sstables, &state.levels[0].1);
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        };

        let (new_ssts, compact_vlog_ids) = self.compact(&task)?;

        // Collect vLog IDs from input SSTs before unregistering (for GC)
        let input_vlog_ids: Vec<u32> = if let Some(ref vlog) = self.vlog {
            let mut ids = Vec::with_capacity(ssts_to_compact.0.len() + ssts_to_compact.1.len());
            for id in ssts_to_compact.0.iter().chain(ssts_to_compact.1) {
                if let Some(refs) = vlog.get_sst_references(*id) {
                    ids.extend(refs);
                }
            }
            ids.sort_unstable();
            ids.dedup();
            ids
        } else {
            vec![]
        };

        {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.load().as_ref().clone();

            // Unregister vLog references for old SSTs
            if let Some(ref vlog) = self.vlog {
                for id in ssts_to_compact.0.iter().chain(ssts_to_compact.1) {
                    vlog.unregister_sst_references(*id);
                }
                // Register vLog references for new SSTs
                for sst in &new_ssts {
                    vlog.register_sst_references(sst.sst_id(), &compact_vlog_ids);
                }
            }

            ssts_to_compact
                .0
                .iter()
                .chain(ssts_to_compact.1)
                .for_each(|id| {
                    snapshot.sstables.remove(id);
                });
            let new_sst_ids: Vec<_> = new_ssts.iter().map(|t| t.sst_id()).collect();
            snapshot.levels[0].1.clone_from(&new_sst_ids);
            new_ssts.iter().for_each(|id| {
                snapshot.sstables.insert(id.sst_id(), id.clone());
            });
            let l0_rm = ssts_to_compact.0.iter().collect::<HashSet<_>>();
            // might have new l0 insert into snapshot.l0_sstables during compact
            snapshot.l0_sstables.retain(|id| !l0_rm.contains(id));

            self.state.store(Arc::new(snapshot));

            self.sync_dir()?;
            // Use CompactionV2 if vLog references exist
            let manifest_record = if self.vlog.is_some() {
                ManifestRecord::CompactionV2(task, new_sst_ids.clone(), compact_vlog_ids)
            } else {
                ManifestRecord::Compaction(task, new_sst_ids.clone())
            };
            self.manifest
                .as_ref()
                .expect("manifest initialized")
                .add_record(&_state_lock, manifest_record)?;
            self.maybe_snapshot_manifest(&_state_lock)?;
        }

        let removed_ids: HashSet<usize> = ssts_to_compact
            .0
            .iter()
            .chain(ssts_to_compact.1)
            .copied()
            .collect();
        for &id in &removed_ids {
            let path = self.path_of_sst(id);
            // Ignore NotFound errors — the background flush thread may have
            // already removed the file during a concurrent operation.
            match std::fs::remove_file(&path) {
                std::result::Result::Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        // Invalidate cached blocks from deleted SSTs via the reverse index.
        self.block_cache.invalidate_ssts(&removed_ids);

        // Run GC on vLog files that may have stale entries
        if !input_vlog_ids.is_empty() {
            self.post_compaction_gc(&input_vlog_ids);
        }

        Ok(())
    }

    /// Run GC on vLog files that were referenced by compacted SSTs.
    /// Spawns a background thread so compaction is not blocked by GC I/O.
    /// Falls back to synchronous GC if `weak_self` is not initialized.
    fn post_compaction_gc(&self, input_vlog_ids: &[u32]) {
        let Some(ref vlog) = self.vlog else { return };
        let vlog = vlog.clone();
        let mut ids: Vec<u32> = input_vlog_ids.to_vec();
        ids.sort_unstable();

        let weak = self.weak_self.get().cloned();
        let vlog2 = vlog.clone();

        if let Some(weak) = weak {
            let handle = std::thread::spawn(move || {
                for &file_id in &ids {
                    // Upgrade inside the loop — if the engine is shutting down,
                    // stop GC early and drop the strong ref after each file.
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let gc = GarbageCollector::new(&vlog, &inner, vlog.options.gc_threshold_ratio);
                    match gc.gc_file(file_id) {
                        std::result::Result::Ok(Some(result)) => {
                            if let Some(ref manifest) = inner.manifest {
                                let _ = manifest.add_record(
                                    &inner.state_lock.lock(),
                                    ManifestRecord::GcCompaction(
                                        result.old_file_id,
                                        result.new_file_id,
                                        result.keys_rewritten,
                                    ),
                                );
                            }
                        }
                        std::result::Result::Ok(None) => {}
                        std::result::Result::Err(e) => {
                            eprintln!("GC error for vlog file {}: {}", file_id, e);
                        }
                    }
                }
                if let Err(e) = vlog.reclaim_pending_deletions() {
                    eprintln!("vLog reclaim error: {}", e);
                }
            });
            {
                let mut handles = std::mem::take(&mut *self.gc_handles.lock());
                handles.retain(|h| !h.is_finished());
                handles.push(handle);
                *self.gc_handles.lock() = handles;
            }
        } else {
            // Fallback to synchronous GC if weak_self is not set
            let gc = GarbageCollector::new(&vlog2, self, vlog2.options.gc_threshold_ratio);
            for &file_id in &ids {
                match gc.gc_file(file_id) {
                    std::result::Result::Ok(Some(result)) => {
                        if let Some(ref manifest) = self.manifest {
                            let _ = manifest.add_record(
                                &self.state_lock.lock(),
                                ManifestRecord::GcCompaction(
                                    result.old_file_id,
                                    result.new_file_id,
                                    result.keys_rewritten,
                                ),
                            );
                        }
                    }
                    std::result::Result::Ok(None) => {}
                    std::result::Result::Err(e) => {
                        eprintln!("GC error for vlog file {}: {}", file_id, e);
                    }
                }
            }
            if let Err(e) = vlog2.reclaim_pending_deletions() {
                eprintln!("vLog reclaim error: {}", e);
            }
        }
    }

    pub(crate) fn trigger_compaction(&self) -> Result<()> {
        let task = self
            .compaction_controller
            .generate_compaction_task(self.state.load_full().as_ref());
        let Some(t) = task.as_ref() else {
            return Ok(());
        };
        let (new_ssts, compact_vlog_ids) = self.compact(t)?;
        let new_sst_ids = new_ssts.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

        // Collect input SST IDs for post-compaction GC
        let input_sst_ids: Vec<usize> = match t {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => l0_sstables
                .iter()
                .chain(l1_sstables.iter())
                .copied()
                .collect(),
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => upper_level_sst_ids
                .iter()
                .chain(lower_level_sst_ids.iter())
                .copied()
                .collect(),
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => upper_level_sst_ids
                .iter()
                .chain(lower_level_sst_ids.iter())
                .copied()
                .collect(),
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => tiers
                .iter()
                .flat_map(|(_, ids)| ids.iter().copied())
                .collect(),
        };

        // Collect vLog IDs from input SSTs before unregistering (for GC)
        let input_vlog_ids: Vec<u32> = if let Some(ref vlog) = self.vlog {
            let mut ids = Vec::with_capacity(input_sst_ids.len());
            for &sst_id in &input_sst_ids {
                if let Some(refs) = vlog.get_sst_references(sst_id) {
                    ids.extend(refs);
                }
            }
            ids.sort_unstable();
            ids.dedup();
            ids
        } else {
            vec![]
        };

        let rm_sst_ids = {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.load().as_ref().clone();
            // specific for leveled compaction, need the sstables in the snapshot
            for s in &new_ssts {
                snapshot.sstables.insert(s.sst_id(), s.clone());
            }
            let (snapshot_partial, rm_sst_ids) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, t, new_sst_ids.as_slice());

            // Unregister vLog references for removed SSTs
            if let Some(ref vlog) = self.vlog {
                for id in &rm_sst_ids {
                    vlog.unregister_sst_references(*id);
                }
                // Register vLog references for new SSTs
                for sst in &new_ssts {
                    vlog.register_sst_references(sst.sst_id(), &compact_vlog_ids);
                }
            }

            let mut snapshot = self.state.load().as_ref().clone();
            // specific for leveled compaction
            snapshot.sstables = snapshot_partial.sstables;
            for s in &rm_sst_ids {
                snapshot.sstables.remove(s);
            }
            snapshot.l0_sstables = snapshot_partial.l0_sstables;
            snapshot.levels = snapshot_partial.levels;
            self.state.store(Arc::new(snapshot));

            self.sync_dir()?;
            // Use CompactionV2 if vLog is enabled
            let task = task.expect("task checked for Some above");
            let manifest_record = if self.vlog.is_some() {
                ManifestRecord::CompactionV2(task, new_sst_ids, compact_vlog_ids)
            } else {
                ManifestRecord::Compaction(task, new_sst_ids)
            };
            self.manifest
                .as_ref()
                .expect("manifest initialized")
                .add_record(&_state_lock, manifest_record)?;
            self.maybe_snapshot_manifest(&_state_lock)?;

            rm_sst_ids
        };

        let removed_ids: HashSet<usize> = rm_sst_ids.iter().copied().collect();
        for &id in &removed_ids {
            let path = self.path_of_sst(id);
            // Ignore NotFound errors — the background flush thread may have
            // already removed the file during a concurrent operation.
            match std::fs::remove_file(&path) {
                std::result::Result::Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        // Invalidate cached blocks from deleted SSTs via the reverse index.
        self.block_cache.invalidate_ssts(&removed_ids);

        // Run GC on vLog files that may have stale entries
        if !input_vlog_ids.is_empty() {
            self.post_compaction_gc(&input_vlog_ids);
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {e}");
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }

        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let state = self.state.load_full();
        if state.imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {e}");
                    },
                    recv(rx) -> _ => return
                }
            }
        });

        Ok(Some(handle))
    }
}
