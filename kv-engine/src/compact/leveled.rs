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
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Compute total size for a level, including range-only SSTs.
    fn level_total_size(snapshot: &LsmStorageState, level_idx: usize) -> u64 {
        let mut size = 0u64;
        snapshot.levels[level_idx]
            .1
            .iter()
            .for_each(|id| size += snapshot.sstables[id].table_size());
        let level_num = snapshot.levels[level_idx].0;
        if let Some((_, ro_ids)) = snapshot
            .range_only_ssts
            .iter()
            .find(|(lvl, _)| *lvl == level_num)
        {
            ro_ids.iter().for_each(|id| {
                if let Some(sst) = snapshot.sstables.get(id) {
                    size += sst.table_size();
                }
            });
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
        // Tombstone boundaries are raw user keys, so decode the internal keys
        // to raw form for comparison.
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
                    && ts_start < last_key_user.as_ref()
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

        // find max ratio
        let mut ratio_max = (0.0, 0_usize);
        for (i, &t) in target_sizes.iter().enumerate() {
            let size = Self::level_total_size(snapshot, i) as usize;
            let ratio = size as f64 / t as f64;
            if ratio > ratio_max.0 {
                ratio_max = (ratio, i);
            }
        }
        if ratio_max.0 <= 1.0 {
            return None;
        }

        let upper_level = ratio_max.1;
        // Skip levels with no point SSTs (only range-only SSTs present).
        // Compacting a level with only range-only SSTs would produce no useful
        // output and would loop indefinitely.
        if snapshot.levels[upper_level].1.is_empty() {
            return None;
        }
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
        _in_recovery: bool,
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
        snapshot.levels[task.lower_level - 1].1.sort_by(|a, b| {
            snapshot.sstables[a]
                .first_key()
                .cmp(&snapshot.sstables[b].first_key())
        });

        let mut rm_ids =
            Vec::with_capacity(task.upper_level_sst_ids.len() + task.lower_level_sst_ids.len());
        rm_ids.extend_from_slice(&task.upper_level_sst_ids);
        rm_ids.extend_from_slice(&task.lower_level_sst_ids);

        (snapshot, rm_ids)
    }
}
