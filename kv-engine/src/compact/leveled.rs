use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    /// Minimum fraction of TTL entries in an SST to trigger compaction.
    const TTL_COMPACTION_RATIO_THRESHOLD: f64 = 0.5;

    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Compute total size for a level, including range-only SSTs.
    fn level_total_size(snapshot: &LsmStorageState, level_idx: usize) -> u64 {
        let mut size: u64 = snapshot.levels[level_idx]
            .1
            .iter()
            .filter_map(|id| snapshot.sstables.get(id))
            .map(|sst| sst.table_size())
            .sum();

        let level_num = snapshot.levels[level_idx].0;
        if let Some((_, ro_ids)) = snapshot
            .range_only_ssts
            .iter()
            .find(|(lvl, _)| *lvl == level_num)
        {
            size += ro_ids
                .iter()
                .filter_map(|id| snapshot.sstables.get(id))
                .map(|sst| sst.table_size())
                .sum::<u64>();
        }
        size
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        // Filter out range-only SSTs (no point data) when computing key range.
        let Some(first_key) = sst_ids
            .iter()
            .filter_map(|id| snapshot.sstables[id].first_key())
            .min()
        else {
            return vec![];
        };
        let Some(last_key) = sst_ids
            .iter()
            .filter_map(|id| snapshot.sstables[id].last_key())
            .max()
        else {
            return vec![];
        };

        let mut ret = vec![];
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            // Skip range-only SSTs — they have no point data to compact.
            let (Some(fk), Some(lk)) = (sst.first_key(), sst.last_key()) else {
                continue;
            };
            if !(fk > last_key || lk < first_key) {
                ret.push(*sst_id);
            }
        }

        // Also include range-only SSTs whose tombstone range overlaps the key range.
        // Tombstone boundaries are raw user keys from delete_range, so decode
        // the internal keys to raw form for comparison.
        // The point range is [first_key, last_key] (inclusive). The tombstone
        // range is [ts_start, ts_end) (half-open). They overlap iff
        // ts_start <= last_key AND ts_end > first_key.
        let first_key_user = first_key.decode_user_key_cow();
        let last_key_user = last_key.decode_user_key_cow();
        if let Some((_, ro_ids)) = snapshot
            .range_only_ssts
            .iter()
            .find(|(lvl, _)| *lvl == in_level)
        {
            for sst_id in ro_ids {
                if let Some(sst) = snapshot.sstables.get(sst_id)
                    && let Some((ts_start, ts_end)) = sst.tombstone_range()
                    && ts_start <= last_key_user.as_ref()
                    && ts_end > first_key_user.as_ref()
                {
                    ret.push(*sst_id);
                }
            }
        }

        ret
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // build target level size
        let mut target_sizes = vec![0; snapshot.levels.len()];
        let bottom_size = Self::level_total_size(snapshot, snapshot.levels.len() - 1);
        target_sizes[snapshot.levels.len() - 1] = bottom_size as usize;
        for i in (0..snapshot.levels.len() - 1).rev() {
            if target_sizes[i + 1] >= self.options.base_level_size_mb * (1 << 20) {
                target_sizes[i] = target_sizes[i + 1] / self.options.level_size_multiplier;
            }
        }
        // l0 first
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let mut lower_level = 0;
            for (i, item) in target_sizes.iter().enumerate() {
                if *item > 0 {
                    lower_level = i;
                    break;
                }
            }

            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, lower_level + 1);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: lower_level + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: lower_level + 1 == self.options.max_levels,
            });
        }

        // find max ratio, skipping levels with no point SSTs (only range-only
        // SSTs present — compacting those would produce no useful output).
        let mut ratio_max = (0.0, 0_usize);
        for (i, &t) in target_sizes.iter().enumerate() {
            if snapshot.levels[i].1.is_empty() {
                continue;
            }
            let size = Self::level_total_size(snapshot, i) as usize;
            let ratio = size as f64 / t as f64;
            if ratio > ratio_max.0 {
                ratio_max = (ratio, i);
            }
        }
        if ratio_max.0 <= 1.0 {
            // No level exceeds its target size. Check if any SST has a high
            // proportion of fully-expired TTL entries — compact the most
            // TTL-heavy one to reclaim space.
            if let Some(task) = self.generate_ttl_compaction_task(snapshot) {
                return Some(task);
            }
            return None;
        }

        let upper_level = ratio_max.1;
        // oldest sst in upper level
        let upper_sstid = *snapshot.levels[upper_level].1.iter().min()?;

        Some(LeveledCompactionTask {
            upper_level: Some(upper_level + 1),
            upper_level_sst_ids: vec![upper_sstid],
            lower_level: upper_level + 1 + 1,
            lower_level_sst_ids: self.find_overlapping_ssts(
                snapshot,
                &[upper_sstid],
                upper_level + 1 + 1,
            ),
            is_lower_level_bottom_level: upper_level + 1 + 1 == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        new_sst_ids: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        match task.upper_level {
            // might have new l0 insert into snapshot.l0_sstables during compaction
            None => snapshot
                .l0_sstables
                .retain(|x| !task.upper_level_sst_ids.contains(x)),
            Some(u) => snapshot.levels[u - 1]
                .1
                .retain(|x| !task.upper_level_sst_ids.contains(x)),
        };
        snapshot.levels[task.lower_level - 1]
            .1
            .retain(|x| !task.lower_level_sst_ids.contains(x));
        snapshot.levels[task.lower_level - 1].1.extend(new_sst_ids);
        if !in_recovery {
            snapshot.levels[task.lower_level - 1].1.sort_by(|a, b| {
                snapshot.sstables[a]
                    .first_key()
                    .cmp(&snapshot.sstables[b].first_key())
            });
        }

        let mut rm_ids =
            Vec::with_capacity(task.upper_level_sst_ids.len() + task.lower_level_sst_ids.len());
        rm_ids.extend_from_slice(&task.upper_level_sst_ids);
        rm_ids.extend_from_slice(&task.lower_level_sst_ids);

        (snapshot, rm_ids)
    }

    /// Scan all levels for the SST with the highest proportion of fully-expired
    /// TTL entries. If found, generate a compaction task targeting its level.
    fn generate_ttl_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let now_secs = crate::vlog::wall_clock_secs();
        let bottom_level_idx = snapshot.levels.len().checked_sub(1)?;

        // Find the best candidate: highest TTL ratio among bottom-level SSTs
        // with fully-expired TTL entries. Physical removal of an expired TTL
        // entry above the bottom can expose older versions from lower levels,
        // so reclamation stays bottom-level only.
        let mut best: Option<(usize, usize, f64)> = None; // (level_idx, sst_id, ratio)
        for (level_idx, (_, ids)) in snapshot.levels.iter().enumerate() {
            if level_idx != bottom_level_idx {
                continue;
            }
            for &sst_id in ids {
                if let Some(sst) = snapshot.sstables.get(&sst_id) {
                    let m = &sst.ttl_metadata;
                    if m.total_entry_count == 0 || m.max_ttl_expire_ts == 0 {
                        continue;
                    }
                    let ratio = m.ttl_entry_count as f64 / m.total_entry_count as f64;
                    if m.max_ttl_expire_ts <= now_secs
                        && ratio >= Self::TTL_COMPACTION_RATIO_THRESHOLD
                        && best.is_none_or(|(_, _, r)| ratio > r)
                    {
                        best = Some((level_idx, sst_id, ratio));
                    }
                }
            }
        }

        let (level_idx, upper_sst_id, _ratio) = best?;
        let is_bottom = level_idx + 1 == snapshot.levels.len();
        // For non-bottom levels, compact with overlapping SSTs in the next
        // level. For the bottom level, self-compact — read the SST, drop
        // expired entries via should_drop_for_ttl, write back.
        let (lower_level, lower_level_sst_ids) = if is_bottom {
            (level_idx + 1, Vec::new())
        } else {
            let ll = level_idx + 2;
            (
                ll,
                self.find_overlapping_ssts(snapshot, &[upper_sst_id], ll),
            )
        };

        Some(LeveledCompactionTask {
            upper_level: Some(level_idx + 1),
            upper_level_sst_ids: vec![upper_sst_id],
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level: lower_level == snapshot.levels.len(),
        })
    }
}
