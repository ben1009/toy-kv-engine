mod leveled;
mod simple_leveled;
mod tiered;

use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::{
    cache::CacheAdmission,
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

/// Return type for `compact_from_iter` and friends:
/// (point_ssts, vlog_ids, applied_filters, output_point_ranges)
/// (point_ssts, vlog_ids, filter_deletion, output_ranges, gc_dropped_fragments)
type CompactIterResult = Result<(
    Vec<Arc<SsTable>>,
    Vec<u32>,
    bool,
    Vec<(Vec<u8>, Vec<u8>)>,
    u64,
)>;

/// Return type for `compact`:
/// (point_ssts, range_only_ssts, vlog_ids, applied_filters)
type CompactResult = Result<(Vec<Arc<SsTable>>, Vec<Arc<SsTable>>, Vec<u32>, bool)>;

struct CompactionOutputState<'a> {
    current_first_key: &'a mut Option<Vec<u8>>,
    current_last_key: &'a mut Option<Vec<u8>>,
    output_point_ranges: &'a mut Vec<(Vec<u8>, Vec<u8>)>,
    ret: &'a mut Vec<Arc<SsTable>>,
    all_vlog_ids: &'a mut Vec<u32>,
}

struct CompactionEntryState<'a> {
    seen_below_watermark: &'a mut bool,
    prev_user_key: &'a mut Vec<u8>,
}

struct CompactionEntryDecision<'a> {
    watermark: Option<u64>,
    compact_to_bottom_level: bool,
    fragments_for_drop_check: &'a [crate::range_tombstone::RangeTombstoneFragment],
    decoded_user_key: &'a [u8],
    key_ts: u64,
    is_tombstone: bool,
}

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
        self.apply_compaction_result_inner(snapshot, task, new_sst_ids, false)
    }

    pub fn apply_compaction_result_recovery(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        new_sst_ids: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        self.apply_compaction_result_inner(snapshot, task, new_sst_ids, true)
    }

    fn apply_compaction_result_inner(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        new_sst_ids: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, new_sst_ids, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, new_sst_ids)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, new_sst_ids)
            }
            // ForceFullCompaction: move all SSTs to level 1. This can be
            // replayed during manifest recovery regardless of controller type.
            (
                _,
                CompactionTask::ForceFullCompaction {
                    l0_sstables,
                    l1_sstables,
                },
            ) => {
                let mut snapshot = snapshot.clone();
                // Remove L0 SSTs that were in the input
                snapshot.l0_sstables.retain(|x| !l0_sstables.contains(x));
                // Remove old L1 SSTs and replace with new ones
                snapshot.levels[0].1.retain(|x| !l1_sstables.contains(x));
                snapshot.levels[0].1.extend(new_sst_ids);
                snapshot.levels[0].1.sort_by(|a, b| {
                    let a_key = snapshot.sstables.get(a).and_then(|sst| sst.first_key());
                    let b_key = snapshot.sstables.get(b).and_then(|sst| sst.first_key());
                    match (a_key, b_key) {
                        (Some(ak), Some(bk)) => ak.cmp(bk),
                        _ => a.cmp(b),
                    }
                });
                let mut rm_ids = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                rm_ids.extend_from_slice(l0_sstables);
                rm_ids.extend_from_slice(l1_sstables);
                (snapshot, rm_ids)
            }
            // Mismatched controller/task (should not happen in practice)
            _ => {
                panic!("mismatched compaction controller and task")
            }
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
    /// Ratio of TTL entries in an SST above which we consider it
    /// worthwhile to trigger compaction for expired data.
    const TTL_COMPACTION_RATIO_THRESHOLD: f64 = 0.5;

    fn compact_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        compact_to_bottom_level: bool,
        is_upper_level_compaction: bool,
        merged_fragments: Vec<crate::range_tombstone::RangeTombstoneFragment>,
    ) -> CompactIterResult {
        use crate::range_tombstone::gc_range_fragments;

        // Only backfill upper-level (L0/L1/L2) compactions. Data in these
        // levels is recently flushed or recently compacted — likely hot.
        // Deeper compactions operate on cold data — backfilling them would
        // evict hotter blocks via force_put.
        let should_backfill = self.options.enable_cache_backfill && is_upper_level_compaction;
        let mut ret = vec![];
        let mut all_vlog_ids: Vec<u32> = vec![];
        let mut builder = self.new_compaction_builder(should_backfill);

        // Snapshot the watermark once before the loop. When no active readers
        // exist, watermark() returns latest_commit_ts so everything below it
        // is eligible for GC.
        let watermark = self.mvcc.as_ref().map(|m| m.watermark());
        let compaction_filters = self.snapshot_compaction_filters();
        let can_publish_filter_deletion = compact_to_bottom_level
            && !compaction_filters.is_empty()
            && self
                .mvcc
                .as_ref()
                .is_some_and(|m| m.can_publish_filter_deletion());

        // TTL deletion gate: bottom-level compaction + no active readers.
        // Does NOT require installed compaction filters.
        let can_publish_ttl_deletion = compact_to_bottom_level
            && self
                .mvcc
                .as_ref()
                .is_none_or(|m| m.can_publish_filter_deletion());

        // Capture wall-clock time once for all TTL checks in this compaction.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::ZERO)
            .as_secs();

        // At bottommost compactions, GC range tombstone fragments: remove
        // covering timestamps at or below the watermark. These tombstones
        // are permanently visible to all readers and their covered point
        // entries can be safely dropped.
        //
        // Keep the original fragments for covered-value dropping (the GC'd
        // fragments may have lost the covering_ts needed to determine that
        // a point entry is hidden). Use the GC'd fragments only for writing
        // to output SSTs.
        let fragments_for_drop_check = if compact_to_bottom_level {
            merged_fragments.clone()
        } else {
            Vec::new()
        };
        let (merged_fragments, gc_dropped_fragments) = if compact_to_bottom_level {
            if let Some(wm) = watermark {
                let before = merged_fragments.len();
                let after = gc_range_fragments(merged_fragments, wm);
                let dropped = before.saturating_sub(after.len()) as u64;
                (after, dropped)
            } else {
                (merged_fragments, 0)
            }
        } else {
            (merged_fragments, 0)
        };

        // Track state for watermark-aware version dropping. Keys iterate
        // newest-first within each user key (inverted ts encoding), so we
        // keep all versions above the watermark and only the newest version
        // at or below the watermark per user key.
        let mut prev_user_key: Vec<u8> = Vec::new();
        let mut seen_below_watermark = false;
        let mut decoded_user_key = Vec::new();

        // Track the first and last user key per output SST for range-tombstone
        // truncation. Keys are decoded (raw) user keys.
        let mut current_first_key: Option<Vec<u8>> = None;
        let mut current_last_key: Option<Vec<u8>> = None;
        let mut output_point_ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut output_state = CompactionOutputState {
            current_first_key: &mut current_first_key,
            current_last_key: &mut current_last_key,
            output_point_ranges: &mut output_point_ranges,
            ret: &mut ret,
            all_vlog_ids: &mut all_vlog_ids,
        };
        let mut entry_state = CompactionEntryState {
            seen_below_watermark: &mut seen_below_watermark,
            prev_user_key: &mut prev_user_key,
        };

        while iter.is_valid() {
            if builder.estimated_size() >= self.options.target_sst_size {
                builder = self.finalize_compaction_output_sst(
                    builder,
                    &mut output_state,
                    &merged_fragments,
                    should_backfill,
                )?;
            }

            // Detect tombstones: single KvKind::Tombstone byte.
            let raw = iter.raw_value();
            let is_tombstone = crate::vlog::KvKind::is_tombstone_value(raw);

            // Decode user key once into the reused buffer to avoid heap
            // allocations. Reused for watermark logic, drop-check, and
            // output SST key range tracking.
            let key = iter.key();
            let ts = key.ts();
            decoded_user_key.clear();
            key.decode_user_key_into(&mut decoded_user_key);
            let user_key: &[u8] = &decoded_user_key;

            let should_keep = self.should_keep_compaction_entry(
                CompactionEntryDecision {
                    watermark,
                    compact_to_bottom_level,
                    fragments_for_drop_check: &fragments_for_drop_check,
                    decoded_user_key: user_key,
                    key_ts: ts,
                    is_tombstone,
                },
                &mut entry_state,
            );

            let should_drop_for_filter =
                should_keep && can_publish_filter_deletion && !is_tombstone && {
                    // decoded_user_key already populated above
                    self.record_compaction_filter_check();
                    compaction_filters
                        .iter()
                        .any(|filter| filter.matches(&decoded_user_key, key.ts()))
                };

            // TTL cleanup: drop expired entries when safe.
            let should_drop_for_ttl =
                should_keep && !should_drop_for_filter && can_publish_ttl_deletion && {
                    crate::vlog::TtlMetadata::parse(raw)
                        .is_some_and(|(meta, _)| now_secs >= meta.expire_at_secs)
                };

            if should_keep && !should_drop_for_filter && !should_drop_for_ttl {
                // Track output SST key range using decoded (raw) user keys,
                // since range-tombstone fragment boundaries are raw user keys.
                // Reuses user_key decoded above.
                if output_state.current_first_key.is_none() {
                    *output_state.current_first_key = Some(user_key.to_vec());
                }
                if let Some(last_key) = output_state.current_last_key {
                    last_key.clear();
                    last_key.extend_from_slice(user_key);
                } else {
                    *output_state.current_last_key = Some(user_key.to_vec());
                }
                builder.add_raw(iter.key(), iter.raw_value())?;
            } else if should_drop_for_filter {
                let dropped_bytes = (iter.key().raw_ref().len() + iter.raw_value().len()) as u64;
                self.record_compaction_filter_drop(dropped_bytes);
            }
            iter.next()?;
        }

        // Finalize last SST: attach truncated range tombstones
        if !builder.is_empty() {
            self.finalize_compaction_output_sst(
                builder,
                &mut output_state,
                &merged_fragments,
                should_backfill,
            )?;
        }

        all_vlog_ids.sort_unstable();
        all_vlog_ids.dedup();
        Ok((
            ret,
            all_vlog_ids,
            can_publish_filter_deletion,
            output_point_ranges,
            gc_dropped_fragments,
        ))
    }

    fn new_compaction_builder(&self, should_backfill: bool) -> SsTableBuilder {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        builder.set_collect_blocks(should_backfill);
        builder.set_prefix_bloom_options(Some(self.options.prefix_bloom.clone()));
        builder
    }

    fn finalize_compaction_output_sst(
        &self,
        mut builder: SsTableBuilder,
        output_state: &mut CompactionOutputState<'_>,
        merged_fragments: &[crate::range_tombstone::RangeTombstoneFragment],
        should_backfill: bool,
    ) -> Result<SsTableBuilder> {
        if builder.is_empty() {
            return Ok(builder);
        }

        if let (Some(first), Some(last)) = (
            output_state.current_first_key.as_ref(),
            output_state.current_last_key.as_ref(),
        ) {
            let succ_last = {
                let mut v = last.clone();
                v.push(0x00);
                v
            };
            let truncated =
                crate::range_tombstone::truncate_fragments(merged_fragments, first, &succ_last);
            if !truncated.is_empty() {
                builder.add_range_tombstones(truncated);
            }
            output_state
                .output_point_ranges
                .push((first.clone(), succ_last));
        }

        output_state
            .all_vlog_ids
            .extend_from_slice(builder.vlog_file_ids());
        let sst_id = self.next_sst_id();
        let (sst, blocks) = builder.build_with_backfill(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        if should_backfill {
            self.block_cache.backfill(sst_id, blocks);
        }
        output_state.ret.push(Arc::new(sst));
        *output_state.current_first_key = None;
        *output_state.current_last_key = None;

        Ok(self.new_compaction_builder(should_backfill))
    }

    fn should_keep_compaction_entry(
        &self,
        decision: CompactionEntryDecision<'_>,
        entry_state: &mut CompactionEntryState<'_>,
    ) -> bool {
        let CompactionEntryDecision {
            watermark,
            compact_to_bottom_level,
            fragments_for_drop_check,
            decoded_user_key,
            key_ts,
            is_tombstone,
        } = decision;
        match watermark {
            Some(wm) => {
                if decoded_user_key != entry_state.prev_user_key.as_slice() {
                    entry_state.prev_user_key.clear();
                    entry_state
                        .prev_user_key
                        .extend_from_slice(decoded_user_key);
                    *entry_state.seen_below_watermark = false;
                }

                if key_ts > wm {
                    true
                } else if !*entry_state.seen_below_watermark {
                    *entry_state.seen_below_watermark = true;
                    if is_tombstone && compact_to_bottom_level {
                        false
                    } else if compact_to_bottom_level
                        && !fragments_for_drop_check.is_empty()
                        && crate::range_tombstone::find_newest_covering_ts(
                            fragments_for_drop_check,
                            decoded_user_key,
                            wm,
                        )
                        .is_some_and(|rt_ts| key_ts <= rt_ts)
                    {
                        self.rt_stats.note_covered_drop();
                        false
                    } else {
                        true
                    }
                } else {
                    false
                }
            }
            None => !is_tombstone || !compact_to_bottom_level,
        }
    }

    fn compact_from_iters(
        &self,
        upper_level_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        lower_level_iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> + 'static,
        compact_to_bottom_level: bool,
        is_upper_level_compaction: bool,
        merged_fragments: Vec<crate::range_tombstone::RangeTombstoneFragment>,
    ) -> CompactIterResult {
        let s_it = TwoMergeIterator::create(upper_level_iter, lower_level_iter)?;

        self.compact_from_iter(
            s_it,
            compact_to_bottom_level,
            is_upper_level_compaction,
            merged_fragments,
        )
    }

    fn compact_from_l0_l1(
        &self,
        l0_sst_ids: &[usize],
        l1_sst_ids: &[usize],
        compact_to_bottom_level: bool,
        is_upper_level_compaction: bool,
        merged_fragments: Vec<crate::range_tombstone::RangeTombstoneFragment>,
    ) -> CompactIterResult {
        let state = self.state.load_full();
        let mut m_it = vec![];
        for i in l0_sst_ids {
            let t = state.sstables[i].clone();
            // Skip range-only SSTs — they have no point data to iterate.
            if t.is_range_only() {
                continue;
            }
            let s = SsTableIterator::create_and_seek_to_first(t, CacheAdmission::Force)?;
            m_it.push(Box::new(s));
        }
        let upper_level_iter = MergeIterator::create(m_it);

        let mut s_lower = vec![];
        for i in l1_sst_ids {
            let t = state.sstables[i].clone();
            s_lower.push(t);
        }
        let lower_level_iter =
            SstConcatIterator::create_and_seek_to_first(s_lower, CacheAdmission::Force)?;

        self.compact_from_iters(
            upper_level_iter,
            lower_level_iter,
            compact_to_bottom_level,
            is_upper_level_compaction,
            merged_fragments,
        )
    }

    /// Build range-only SSTs for gap ranges not covered by any point output SST.
    fn build_range_only_ssts(
        &self,
        merged_fragments: &[crate::range_tombstone::RangeTombstoneFragment],
        output_point_ranges: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<Vec<Arc<SsTable>>> {
        use crate::range_tombstone::{compute_gap_ranges, truncate_fragments};

        if merged_fragments.is_empty() {
            return Ok(Vec::new());
        }

        let tombstone_start = merged_fragments[0].start.as_ref();
        let tombstone_end = merged_fragments[merged_fragments.len() - 1].end.as_ref();

        let gaps = compute_gap_ranges(tombstone_start, tombstone_end, output_point_ranges);
        let mut result = Vec::new();

        for (gap_start, gap_end) in gaps {
            let truncated = truncate_fragments(merged_fragments, &gap_start, &gap_end);
            if truncated.is_empty() {
                continue;
            }

            let sst_id = self.next_sst_id();
            let mut builder = SsTableBuilder::new(self.options.block_size);
            builder.add_range_tombstones(truncated);
            let (sst, _blocks) = builder.build_with_backfill(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            result.push(Arc::new(sst));
        }

        Ok(result)
    }

    /// Collect and merge range-tombstone fragments from all input SSTs.
    fn collect_merged_fragments(
        &self,
        state: &LsmStorageState,
        upper_sst_ids: &[usize],
        lower_sst_ids: &[usize],
        lower_level: Option<usize>,
    ) -> Vec<crate::range_tombstone::RangeTombstoneFragment> {
        let ro_ids_len = lower_level
            .and_then(|level| state.range_only_ssts.iter().find(|(lvl, _)| *lvl == level))
            .map(|(_, ro_ids)| ro_ids.len())
            .unwrap_or(0);
        let mut fragment_lists: Vec<&[crate::range_tombstone::RangeTombstoneFragment]> =
            Vec::with_capacity(upper_sst_ids.len() + lower_sst_ids.len() + ro_ids_len);

        // Collect from all input SSTs
        for id in upper_sst_ids.iter().chain(lower_sst_ids.iter()) {
            if let Some(sst) = state.sstables.get(id)
                && let Some(frags) = sst.range_tombstone_fragments()
                && !frags.is_empty()
            {
                fragment_lists.push(frags);
            }
        }

        // Also collect from range-only SSTs in the lower level. Point SST
        // IDs (in levels) and range-only SST IDs (in range_only_ssts) are
        // disjoint, so no dedup is needed. Active for force-full and Simple
        // compaction; Leveled passes None since find_overlapping_ssts already
        // includes overlapping range-only SSTs in lower_level_sst_ids.
        if let Some(level) = lower_level
            && let Some((_, ro_ids)) = state.range_only_ssts.iter().find(|(lvl, _)| *lvl == level)
        {
            for sst_id in ro_ids {
                if let Some(sst) = state.sstables.get(sst_id)
                    && let Some(frags) = sst.range_tombstone_fragments()
                    && !frags.is_empty()
                {
                    fragment_lists.push(frags);
                }
            }
        }

        if fragment_lists.is_empty() {
            Vec::new()
        } else {
            crate::range_tombstone::merge_fragment_lists(&fragment_lists)
        }
    }

    fn compact(&self, task: &CompactionTask) -> CompactResult {
        let state = self.state.load_full();
        let (merged_fragments, lower_level) = self.collect_compaction_fragments(&state, task);
        let (point_ssts, vlog_ids, apply_filters, output_ranges, gc_dropped_fragments) =
            self.run_compaction_task(&state, task, &merged_fragments)?;

        // Build range-only SSTs for gap ranges not covered by point output SSTs.
        // Skip for Tiered compaction: range_only_ssts are not tracked per-tier,
        // so generated range-only SSTs would become permanently orphaned.
        // Note: merged_fragments retain full timestamps even at bottommost
        // compactions (GC is applied only to point SSTs in compact_from_iter).
        // This is intentional — range-only SSTs preserve tombstone coverage
        // semantics and GC of their timestamps would require returning the
        // filtered fragments from compact_from_iter (deferred to follow-up).
        let range_only_ssts = if lower_level.is_none() || merged_fragments.is_empty() {
            Vec::new()
        } else if output_ranges.is_empty() {
            // All point entries were dropped — entire tombstone range is a gap.
            // Build a single range-only SST with all fragments.
            let sst_id = self.next_sst_id();
            let mut builder = SsTableBuilder::new(self.options.block_size);
            builder.add_range_tombstones(merged_fragments);
            let (sst, _) = builder.build_with_backfill(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            vec![Arc::new(sst)]
        } else {
            self.build_range_only_ssts(&merged_fragments, &output_ranges)?
        };

        // Only count tombstone drops when fragments are truly removed (no
        // range-only SSTs written). When range-only SSTs are produced the
        // GC'd fragments are re-persisted there, so they are not reclaimed.
        if gc_dropped_fragments > 0 && range_only_ssts.is_empty() {
            self.rt_stats.note_tombstone_drops_n(gc_dropped_fragments);
        }

        Ok((point_ssts, range_only_ssts, vlog_ids, apply_filters))
    }

    fn collect_compaction_fragments(
        &self,
        state: &LsmStorageState,
        task: &CompactionTask,
    ) -> (
        Vec<crate::range_tombstone::RangeTombstoneFragment>,
        Option<usize>,
    ) {
        match task {
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level: _,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => {
                let frags = self.collect_merged_fragments(
                    state,
                    upper_level_sst_ids,
                    lower_level_sst_ids,
                    None,
                );
                (frags, Some(*lower_level))
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level: _,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => {
                let frags = self.collect_merged_fragments(
                    state,
                    upper_level_sst_ids,
                    lower_level_sst_ids,
                    Some(*lower_level),
                );
                (frags, Some(*lower_level))
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let frags = self.collect_merged_fragments(state, l0_sstables, l1_sstables, Some(1));
                (frags, Some(1))
            }
            CompactionTask::Tiered(t) => {
                let mut fragment_lists: Vec<&[crate::range_tombstone::RangeTombstoneFragment]> =
                    Vec::new();
                for (_, sst_ids) in &t.tiers {
                    for id in sst_ids {
                        if let Some(sst) = state.sstables.get(id)
                            && let Some(frags) = sst.range_tombstone_fragments()
                            && !frags.is_empty()
                        {
                            fragment_lists.push(frags);
                        }
                    }
                }
                let frags = if fragment_lists.is_empty() {
                    Vec::new()
                } else {
                    crate::range_tombstone::merge_fragment_lists(&fragment_lists)
                };
                (frags, None)
            }
        }
    }

    fn run_compaction_task(
        &self,
        state: &LsmStorageState,
        task: &CompactionTask,
        merged_fragments: &[crate::range_tombstone::RangeTombstoneFragment],
    ) -> CompactIterResult {
        let (point_ssts, vlog_ids, apply_filters, output_ranges, gc_dropped_fragments) = match task
        {
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
                        merged_fragments.to_vec(),
                    )?
                } else {
                    let mut s_upper = vec![];
                    for i in upper_level_sst_ids.iter() {
                        let t = state.sstables[i].clone();
                        s_upper.push(t);
                    }
                    let upper_level_iter = SstConcatIterator::create_and_seek_to_first(
                        s_upper,
                        CacheAdmission::Force,
                    )?;

                    let mut s_lower = vec![];
                    for i in lower_level_sst_ids.iter() {
                        let t = state.sstables[i].clone();
                        s_lower.push(t);
                    }
                    let lower_level_iter = SstConcatIterator::create_and_seek_to_first(
                        s_lower,
                        CacheAdmission::Force,
                    )?;

                    self.compact_from_iters(
                        upper_level_iter,
                        lower_level_iter,
                        task.compact_to_bottom_level(),
                        task.is_upper_level_compaction(),
                        merged_fragments.to_vec(),
                    )?
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
                merged_fragments.to_vec(),
            )?,
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
                        CacheAdmission::Force,
                    )?));
                }
                let m_iter = MergeIterator::create(s_iters);
                self.compact_from_iter(
                    m_iter,
                    task.compact_to_bottom_level(),
                    task.is_upper_level_compaction(),
                    merged_fragments.to_vec(),
                )?
            }
        };
        Ok((
            point_ssts,
            vlog_ids,
            apply_filters,
            output_ranges,
            gc_dropped_fragments,
        ))
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let state = self.state.load_full();
        let ssts_to_compact = (&state.l0_sstables, &state.levels[0].1);
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        };

        let (new_ssts, new_range_only_ssts, compact_vlog_ids, apply_filters) =
            self.compact(&task)?;

        // Collect vLog IDs from input SSTs before unregistering (for GC)
        let input_sst_ids = ssts_to_compact
            .0
            .iter()
            .chain(ssts_to_compact.1.iter())
            .copied()
            .collect::<Vec<_>>();
        let input_vlog_ids = self.collect_compaction_input_vlog_ids(&input_sst_ids);

        let Some(old_range_only_ids) = self.publish_force_full_compaction_result(
            (ssts_to_compact.0.as_slice(), ssts_to_compact.1.as_slice()),
            task,
            &new_ssts,
            &new_range_only_ssts,
            &compact_vlog_ids,
            apply_filters,
        )?
        else {
            return Ok(());
        };

        let removed_ids: HashSet<usize> = ssts_to_compact
            .0
            .iter()
            .chain(ssts_to_compact.1)
            .chain(old_range_only_ids.iter())
            .copied()
            .collect();
        self.remove_sst_files(removed_ids.iter().copied())?;
        // Invalidate cached blocks from deleted SSTs via the reverse index.
        self.block_cache.invalidate_ssts(&removed_ids);

        // Run GC on vLog files that may have stale entries
        if !input_vlog_ids.is_empty() {
            self.post_compaction_gc(&input_vlog_ids);
        }

        Ok(())
    }

    fn record_gc_compaction(
        manifest: Option<&crate::manifest::Manifest>,
        state_lock: &parking_lot::Mutex<()>,
        result: crate::vlog::gc::GcResult,
    ) {
        if let Some(manifest) = manifest {
            let _ = manifest.add_record(
                &state_lock.lock(),
                ManifestRecord::GcCompaction(
                    result.old_file_id,
                    result.new_file_id,
                    result.keys_rewritten,
                ),
            );
        }
    }

    pub(crate) fn gc_single_vlog_file(
        vlog: &Arc<crate::vlog::ValueLog>,
        inner: &LsmStorageInner,
        file_id: u32,
    ) {
        let gc = GarbageCollector::new(vlog, inner, vlog.options.gc_threshold_ratio);
        match gc.gc_file(file_id) {
            std::result::Result::Ok(Some(result)) => {
                Self::record_gc_compaction(inner.manifest.as_ref(), &inner.state_lock, result);
            }
            std::result::Result::Ok(None) => {}
            std::result::Result::Err(e) => {
                log::error!("GC error for vlog file {}: {}", file_id, e);
            }
        }
    }

    /// Run GC on vLog files that were referenced by compacted SSTs.
    /// Schedules GC on the engine-owned background runtime so compaction is
    /// not blocked by GC I/O. Falls back to synchronous GC if the runtime is
    /// unavailable (for example, inner-only tests or shutdown).
    fn post_compaction_gc(&self, input_vlog_ids: &[u32]) {
        let Some(ref vlog) = self.vlog else { return };
        let vlog = vlog.clone();
        let mut ids: Vec<u32> = input_vlog_ids.to_vec();
        ids.sort_unstable();

        let weak = self.weak_self.get().cloned();
        let vlog2 = vlog.clone();

        let submitter = self.background_tasks.lock().clone();
        if let Some(weak) = weak
            && let Some(submitter) = submitter
            && submitter
                .spawn_post_compaction_gc(weak, vlog, ids.clone(), self.blocking.clone())
                .is_ok()
        {
            return;
        }

        // Fallback to synchronous GC if weak_self is not set, the engine was
        // opened without background workers, or shutdown has already started.
        for &file_id in &ids {
            Self::gc_single_vlog_file(&vlog2, self, file_id);
        }
        if let Err(e) = vlog2.reclaim_pending_deletions() {
            log::error!("vLog reclaim error: {}", e);
        }
    }

    fn collect_compaction_input_sst_ids(task: &CompactionTask) -> Vec<usize> {
        match task {
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
        }
    }

    fn collect_compaction_input_vlog_ids(&self, input_sst_ids: &[usize]) -> Vec<u32> {
        let Some(ref vlog) = self.vlog else {
            return vec![];
        };

        let mut ids = Vec::with_capacity(input_sst_ids.len());
        for &sst_id in input_sst_ids {
            if let Some(refs) = vlog.get_sst_references(sst_id) {
                ids.extend(refs);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn compaction_target_level(task: &CompactionTask) -> Option<usize> {
        match task {
            CompactionTask::Leveled(LeveledCompactionTask { lower_level, .. })
            | CompactionTask::Simple(SimpleLeveledCompactionTask { lower_level, .. }) => {
                Some(*lower_level)
            }
            CompactionTask::ForceFullCompaction { .. } => Some(1),
            CompactionTask::Tiered(_) => None,
        }
    }

    fn collect_input_range_only_ids(
        &self,
        task: &CompactionTask,
        target_level: Option<usize>,
        input_sst_ids: &[usize],
    ) -> Vec<usize> {
        let Some(level) = target_level else {
            return Vec::new();
        };

        let state = self.state.load();
        let Some((_, ro_ids)) = state.range_only_ssts.iter().find(|(lvl, _)| *lvl == level) else {
            return Vec::new();
        };

        match task {
            CompactionTask::Simple(_) => {
                // Simple compaction compacts the whole level, so every range-only SST at the
                // target level is part of the input set.
                ro_ids.clone()
            }
            _ => ro_ids
                .iter()
                .filter(|id| input_sst_ids.contains(id))
                .copied()
                .collect(),
        }
    }

    fn remove_sst_files<I>(&self, sst_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = usize>,
    {
        for id in sst_ids {
            let path = self.path_of_sst(id);
            // Ignore NotFound errors — the background flush thread may have
            // already removed the file during a concurrent operation.
            match std::fs::remove_file(&path) {
                std::result::Result::Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn remove_orphan_compaction_outputs(
        &self,
        new_ssts: &[Arc<SsTable>],
        new_range_only_ssts: &[Arc<SsTable>],
    ) {
        for sst in new_ssts.iter().chain(new_range_only_ssts.iter()) {
            let _ = std::fs::remove_file(self.path_of_sst(sst.sst_id()));
        }
    }

    fn publish_force_full_compaction_result(
        &self,
        ssts_to_compact: (&[usize], &[usize]),
        task: CompactionTask,
        new_ssts: &[Arc<SsTable>],
        new_range_only_ssts: &[Arc<SsTable>],
        compact_vlog_ids: &[u32],
        apply_filters: bool,
    ) -> Result<Option<Vec<usize>>> {
        let _state_lock = self.state_lock.lock();

        // Re-check filter safety under state_lock. Between the initial
        // check in compact_from_iter and this publication point, a new
        // ReadGuard may have been created. Filters are optional cleanup,
        // so if new readers appeared, skip publishing the filtered result.
        if apply_filters
            && self
                .mvcc
                .as_ref()
                .is_some_and(|m| !m.can_publish_filter_deletion())
        {
            // Clean up orphan SST files that were built but will not be
            // published to the LSM state.
            self.remove_orphan_compaction_outputs(new_ssts, new_range_only_ssts);
            return Ok(None);
        }

        let mut snapshot = self.state.load().as_ref().clone();

        if let Some(ref vlog) = self.vlog {
            for id in ssts_to_compact.0.iter().chain(ssts_to_compact.1) {
                vlog.unregister_sst_references(*id);
            }
            // Register vLog references for new point SSTs only.
            // Range-only SSTs have no point data and no vLog references.
            for sst in new_ssts {
                vlog.register_sst_references(sst.sst_id(), compact_vlog_ids);
            }
        }

        ssts_to_compact
            .0
            .iter()
            .chain(ssts_to_compact.1)
            .for_each(|id| {
                snapshot.sstables.remove(id);
            });
        // Remove old range-only SSTs from level 1 and drop their table entries.
        let old_range_only_ids = if let Some((_, ro_ids)) = snapshot
            .range_only_ssts
            .iter_mut()
            .find(|(lvl, _)| *lvl == 1)
        {
            std::mem::take(ro_ids)
        } else {
            Vec::new()
        };
        for id in &old_range_only_ids {
            snapshot.sstables.remove(id);
        }

        let new_sst_ids: Vec<_> = new_ssts.iter().map(|t| t.sst_id()).collect();
        snapshot.levels[0].1.clone_from(&new_sst_ids);
        for sst in new_ssts.iter().chain(new_range_only_ssts.iter()) {
            snapshot.sstables.insert(sst.sst_id(), sst.clone());
        }
        // Add range-only SSTs to level 1 (create entry if missing).
        let new_ro_ids: Vec<usize> = new_range_only_ssts.iter().map(|t| t.sst_id()).collect();
        if !new_ro_ids.is_empty() {
            if let Some((_, ro_ids)) = snapshot
                .range_only_ssts
                .iter_mut()
                .find(|(lvl, _)| *lvl == 1)
            {
                ro_ids.extend(new_ro_ids.iter().copied());
            } else {
                snapshot.range_only_ssts.push((1, new_ro_ids));
            }
        }
        let l0_rm = ssts_to_compact.0.iter().collect::<HashSet<_>>();
        // might have new l0 insert into snapshot.l0_sstables during compact
        snapshot.l0_sstables.retain(|id| !l0_rm.contains(id));

        self.state.store(Arc::new(snapshot));

        self.sync_dir()?;

        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("compaction.after_output_sync_before_manifest");
        }

        let manifest_record = ManifestRecord::CompactionV3(
            task,
            new_sst_ids,
            compact_vlog_ids.to_vec(),
            new_range_only_ssts.iter().map(|t| t.sst_id()).collect(),
        );
        self.manifest
            .as_ref()
            .expect("manifest initialized")
            .add_record(&_state_lock, manifest_record)?;
        self.maybe_snapshot_manifest(&_state_lock)?;

        Ok(Some(old_range_only_ids))
    }

    /// Scan all SSTs for fully-expired TTL data and drop them wholesale.
    ///
    /// An SST qualifies when:
    /// - It has a v8/v9 footer with TTL metadata (`has_non_ttl_entries == false`)
    /// - All TTL entries have expired (`max_ttl_expire_ts < now_secs`)
    /// - No active readers prevent physical deletion
    ///
    /// This avoids reading any data blocks — only the 42-byte footer is checked.
    /// Returns the number of SSTs dropped.
    fn try_ttl_wholesale_drop(&self) -> Result<usize> {
        // MVCC safety gate: don't physically delete while readers exist.
        if self
            .mvcc
            .as_ref()
            .is_some_and(|m| !m.can_publish_filter_deletion())
        {
            return Ok(0);
        }
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::ZERO)
            .as_secs();

        // Phase 1: scan SST footers for fully-expired TTL-only candidates
        // without holding the state_lock (cheap metadata scan, no mutation).
        let snapshot = self.state.load_full();
        let mut candidates: Vec<usize> = Vec::new();
        for (&id, sst) in snapshot.sstables.iter() {
            let meta = &sst.ttl_metadata;
            if meta.has_non_ttl_entries {
                continue;
            }
            if meta.max_ttl_expire_ts > 0 && meta.max_ttl_expire_ts <= now_secs {
                candidates.push(id);
            }
        }
        if candidates.is_empty() {
            return Ok(0);
        }

        // Phase 2: under state_lock, re-compute bottom level and filter
        // candidates to avoid TOCTOU race with concurrent compactions.
        let _state_lock = self.state_lock.lock();
        // Re-check MVCC safety under lock.
        if self
            .mvcc
            .as_ref()
            .is_some_and(|m| !m.can_publish_filter_deletion())
        {
            return Ok(0);
        }
        let snapshot = self.state.load().as_ref().clone();
        // Only bottommost-level SSTs are safe to drop wholesale.
        let bottom_level = snapshot
            .levels
            .iter()
            .map(|(lvl, _)| *lvl)
            .max()
            .unwrap_or(0);
        let bottom_ids: std::collections::HashSet<usize> = snapshot
            .levels
            .iter()
            .filter(|(lvl, _)| *lvl == bottom_level)
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect();
        let expired_ids: Vec<usize> = candidates
            .into_iter()
            .filter(|id| bottom_ids.contains(id))
            .collect();
        if expired_ids.is_empty() {
            return Ok(0);
        }
        let count = expired_ids.len();
        let mut snapshot = snapshot;
        for &id in &expired_ids {
            snapshot.sstables.remove(&id);
            snapshot.l0_sstables.retain(|x| *x != id);
            for (_, ids) in snapshot.levels.iter_mut() {
                ids.retain(|x| *x != id);
            }
            // Unregister vLog references if any.
            if let Some(ref vlog) = self.vlog {
                vlog.unregister_sst_references(id);
            }
        }
        // Persist the new state via a full snapshot so that after a crash
        // the manifest does not reference deleted SST files.
        // Rebuild vLog references from the remaining SSTs.
        let mut vlog_refs = Vec::new();
        if let Some(ref vlog) = self.vlog {
            let mut ids: Vec<usize> = snapshot.sstables.keys().copied().collect();
            ids.sort_unstable();
            for id in ids {
                if let Some(refs) = vlog.get_sst_references(id) {
                    if !refs.is_empty() {
                        vlog_refs.push((id, refs));
                    }
                }
            }
        }
        let snapshot_record = ManifestRecord::Snapshot {
            l0_sstables: snapshot.l0_sstables.clone(),
            levels: snapshot.levels.clone(),
            range_only_ssts: snapshot.range_only_ssts.clone(),
            next_sst_id: self.next_sst_id(),
            vlog_references: vlog_refs,
            imm_memtable_ids: snapshot.imm_memtables.iter().map(|m| m.id()).collect(),
            active_compaction_filters: self.snapshot_compaction_filters(),
            next_compaction_filter_id: self.snapshot_compaction_filter_next_id(),
            format_version: crate::manifest::MANIFEST_FORMAT_VERSION,
        };
        if let Some(ref manifest) = self.manifest {
            manifest.snapshot(snapshot_record)?;
        }
        self.state.store(Arc::new(snapshot));
        // Delete SST files from disk.
        self.remove_sst_files(expired_ids)?;
        Ok(count)
    }

    /// Check whether any mixed SSTs have a high enough proportion of
    /// expired TTL entries to justify compaction.
    fn scan_expired_ttl_heavy_ssts(&self, now_secs: u64) -> usize {
        if self
            .mvcc
            .as_ref()
            .is_some_and(|m| !m.can_publish_filter_deletion())
        {
            return 0;
        }
        let snapshot = self.state.load_full();
        snapshot
            .sstables
            .iter()
            .filter(|(_, sst)| {
                let m = &sst.ttl_metadata;
                m.has_non_ttl_entries
                    && m.total_entry_count > 0
                    && m.max_ttl_expire_ts > 0
                    && m.max_ttl_expire_ts <= now_secs
                    && (m.ttl_entry_count as f64 / m.total_entry_count as f64)
                        >= Self::TTL_COMPACTION_RATIO_THRESHOLD
            })
            .count()
    }

    pub(crate) fn trigger_compaction(&self) -> Result<()> {
        // TTL background scan: drop fully-expired TTL-only SSTs.
        // Mixed SSTs (TTL + non-TTL) are handled by normal compaction's
        // per-entry should_drop_for_ttl — forcing a full compaction here
        // for a single expired entry would be disproportionately expensive.
        self.try_ttl_wholesale_drop()?;

        let task = self
            .compaction_controller
            .generate_compaction_task(self.state.load_full().as_ref());
        let Some(t) = task.as_ref() else {
            // No normal compaction task. Check if any mixed SSTs have a high
            // proportion of fully-expired TTL entries — if so, force compaction
            // to reclaim the space. Gated on max_ttl < now (all TTL expired)
            // AND ttl_entry_count >= 50% of total entries.
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or(std::time::Duration::ZERO)
                .as_secs();
            if self.scan_expired_ttl_heavy_ssts(now_secs) > 0 {
                return self.force_full_compaction();
            }
            return Ok(());
        };
        let (new_ssts, new_range_only_ssts, compact_vlog_ids, apply_filters) = self.compact(t)?;
        let new_sst_ids = new_ssts.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
        let new_range_only_sst_ids: Vec<usize> =
            new_range_only_ssts.iter().map(|x| x.sst_id()).collect();

        // Collect input SST IDs for post-compaction GC
        let input_sst_ids = Self::collect_compaction_input_sst_ids(t);
        let input_vlog_ids = self.collect_compaction_input_vlog_ids(&input_sst_ids);
        let target_level = Self::compaction_target_level(t);

        // Determine range-only SST IDs that were in the input (to remove them)
        let input_range_only_ids =
            self.collect_input_range_only_ids(t, target_level, &input_sst_ids);

        let rm_sst_ids = {
            let _state_lock = self.state_lock.lock();

            // Re-check filter safety under state_lock — same rationale as
            // force_full_compaction. If new readers appeared since the initial
            // check, skip publishing the filtered compaction result.
            if apply_filters
                && self
                    .mvcc
                    .as_ref()
                    .is_some_and(|m| !m.can_publish_filter_deletion())
            {
                self.remove_orphan_compaction_outputs(&new_ssts, &new_range_only_ssts);
                return Ok(());
            }

            let mut snapshot = self.state.load().as_ref().clone();
            // Insert both point and range-only SSTs into the snapshot
            for s in new_ssts.iter().chain(new_range_only_ssts.iter()) {
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
                // Register vLog references for new point SSTs only.
                // Range-only SSTs have no point data and no vLog references,
                // so registering them would pin vLog files unnecessarily.
                for sst in new_ssts.iter() {
                    vlog.register_sst_references(sst.sst_id(), &compact_vlog_ids);
                }
            }

            let mut snapshot = self.state.load().as_ref().clone();
            // specific for leveled compaction
            snapshot.sstables = snapshot_partial.sstables;
            for s in &rm_sst_ids {
                snapshot.sstables.remove(s);
            }
            // Remove consumed range-only SSTs from sstables map
            for id in &input_range_only_ids {
                snapshot.sstables.remove(id);
            }
            snapshot.l0_sstables = snapshot_partial.l0_sstables;
            snapshot.levels = snapshot_partial.levels;

            // Add new range-only SSTs to the target level and remove old ones
            if let Some(level) = target_level {
                if let Some((_, ro_ids)) = snapshot
                    .range_only_ssts
                    .iter_mut()
                    .find(|(lvl, _)| *lvl == level)
                {
                    // Remove old range-only SSTs that were in the input
                    ro_ids.retain(|id| !input_range_only_ids.contains(id));
                    // Add new range-only SSTs
                    for &id in &new_range_only_sst_ids {
                        ro_ids.push(id);
                    }
                } else {
                    snapshot
                        .range_only_ssts
                        .push((level, new_range_only_sst_ids.clone()));
                }
            }

            self.state.store(Arc::new(snapshot));

            self.sync_dir()?;

            #[cfg(feature = "chaos-testing")]
            {
                crate::chaos::failpoint::fail_point!(
                    "compaction.after_output_sync_before_manifest"
                );
            }

            let task = task.expect("task checked for Some above");
            let manifest_record = ManifestRecord::CompactionV3(
                task,
                new_sst_ids,
                compact_vlog_ids,
                new_range_only_sst_ids,
            );
            self.manifest
                .as_ref()
                .expect("manifest initialized")
                .add_record(&_state_lock, manifest_record)?;
            self.maybe_snapshot_manifest(&_state_lock)?;

            rm_sst_ids
        };

        let removed_ids: HashSet<usize> = rm_sst_ids
            .iter()
            .copied()
            .chain(input_range_only_ids.iter().copied())
            .collect();
        self.remove_sst_files(removed_ids.iter().copied())?;
        // Invalidate cached blocks from deleted SSTs via the reverse index.
        self.block_cache.invalidate_ssts(&removed_ids);

        // Run GC on vLog files that may have stale entries
        if !input_vlog_ids.is_empty() {
            self.post_compaction_gc(&input_vlog_ids);
        }

        Ok(())
    }

    pub(crate) fn trigger_flush(&self) -> Result<()> {
        let state = self.state.load_full();
        if state.imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }
}
