use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction
    /// task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        for i in 0..self.options.max_levels - 1 {
            let lower_level = i + 1;
            let size_ratio =
                snapshot.levels[lower_level].1.len() as f64 / snapshot.levels[i].1.len() as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    i, lower_level, size_ratio
                );

                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i + 1),
                    upper_level_sst_ids: snapshot.levels[i].1.clone(),
                    lower_level: lower_level + 1,
                    lower_level_sst_ids: snapshot.levels[lower_level].1.clone(),
                    is_lower_level_bottom_level: lower_level + 1 == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction `task` and the list of
    /// `new_sst_ids` generated from compaction. This function applies the result and generates
    /// a new `LSM state` and `sst_ids` need to remove. The functions should only change
    /// `l0_sstables` and `levels` without changing memtables and `sstables` hash map. Though
    /// there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind,
    /// you should do some sanity checks in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        new_sst_ids: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        match task.upper_level {
            // might have new l0 insert into snashot.l0_sstables during compaction
            None => snapshot
                .l0_sstables
                .retain(|x| !task.upper_level_sst_ids.contains(x)),
            Some(u) => snapshot.levels[u - 1].1.clear(),
        };
        snapshot.levels[task.lower_level - 1].1 = new_sst_ids.to_vec();

        let mut rm_ids = task.upper_level_sst_ids.clone();
        rm_ids.extend(task.lower_level_sst_ids.clone());

        (snapshot, rm_ids)
    }
}
