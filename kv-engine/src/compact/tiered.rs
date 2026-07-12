use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    fn tier_run_size(snapshot: &LsmStorageState, tier_id: usize, sst_ids: &[usize]) -> usize {
        let range_only = snapshot
            .range_only_ssts
            .iter()
            .find(|(level, _)| *level == tier_id)
            .map(|(_, ids)| ids.len())
            .unwrap_or(0);
        sst_ids.len() + range_only
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // only trigger tasks when the number of tiers (sorted runs) is larger than num_tiers
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Triggered by Space Amplification Ratio
        let mut all_levels_but_last_size = 0;
        for l in 0..snapshot.levels.len() - 1 {
            all_levels_but_last_size +=
                Self::tier_run_size(snapshot, snapshot.levels[l].0, &snapshot.levels[l].1);
        }
        let last_tier = snapshot.levels.last().unwrap();
        let last_size = Self::tier_run_size(snapshot, last_tier.0, &last_tier.1);

        if all_levels_but_last_size as f64 / last_size as f64
            >= self.options.max_size_amplification_percent as f64 / 100_f64
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Triggered by Size Ratio
        let mut pre_size = 0;
        for l in 0..snapshot.levels.len() - 1 {
            pre_size += Self::tier_run_size(snapshot, snapshot.levels[l].0, &snapshot.levels[l].1);
            let next_tier = &snapshot.levels[l + 1];
            let ratio =
                pre_size as f64 / Self::tier_run_size(snapshot, next_tier.0, &next_tier.1) as f64;

            if l + 2 >= self.options.min_merge_width
                && ratio >= (100_f64 + self.options.size_ratio as f64) / 100_f64
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[0..l + 1 + 1].to_vec(),
                    bottom_tier_included: l + 2 >= snapshot.levels.len(),
                });
            };
        }
        // Reduce Sorted Runs
        let top_most = snapshot.levels.len() - self.options.num_tiers + 1;

        Some(TieredCompactionTask {
            tiers: snapshot.levels[0..top_most + 1].to_vec(),
            bottom_tier_included: top_most + 1 >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();

        for (tier_id, sstids) in &snapshot.levels {
            // might have new files added to sstables.levels when flush_immtables, so rm by id
            if let Some(f) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                assert_eq!(f, sstids, "file changed after issuing compaction task");
                files_to_remove.extend(f.iter());
            } else {
                // retain the tier
                levels.push((*tier_id, sstids.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                // tricky one, insert the new generated tier to the LSM tree, this may be in the
                // middle of the LSM tree
                new_tier_added = true;
                if !output.is_empty() {
                    // use the first output SST id as the level/tier id for new sorted run
                    let new_tier_id = output[0];
                    let point_output =
                        if output.iter().all(|id| snapshot.sstables.contains_key(id)) {
                            output
                                .iter()
                                .copied()
                                .filter(|id| {
                                    snapshot
                                        .sstables
                                        .get(id)
                                        .map_or(true, |sst| !sst.is_range_only())
                                })
                                .collect()
                        } else {
                            output.to_vec()
                        };
                    levels.push((new_tier_id, point_output));
                }
            }
        }
        if !tier_to_remove.is_empty() && !new_tier_added && !output.is_empty() {
            let new_tier_id = output[0];
            let point_output = if output.iter().all(|id| snapshot.sstables.contains_key(id)) {
                output
                    .iter()
                    .copied()
                    .filter(|id| {
                        snapshot
                            .sstables
                            .get(id)
                            .map_or(true, |sst| !sst.is_range_only())
                    })
                    .collect()
            } else {
                output.to_vec()
            };
            levels.push((new_tier_id, point_output));
        }
        snapshot.levels = levels;

        (snapshot, files_to_remove)
    }
}
