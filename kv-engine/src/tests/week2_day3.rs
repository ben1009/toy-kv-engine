use tempfile::tempdir;

use super::harness::{check_compaction_ratio, compaction_bench};
use crate::{
    compact::{CompactionOptions, TieredCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(CompactionOptions::Tiered(
            TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 1,
                min_merge_width: 2,
            },
        )),
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
