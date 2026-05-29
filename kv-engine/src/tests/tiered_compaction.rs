use tempfile::tempdir;

use super::harness::{check_compaction_ratio, compaction_bench};
use crate::{
    compact::{CompactionOptions, TieredCompactionOptions},
    lsm_storage::{LsmStorageOptions, KvEngine},
};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(
        &dir,
        LsmStorageOptions::default_for_compaction_test(CompactionOptions::Tiered(
            TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 1,
                min_merge_width: 2,

                max_merge_width: None,
            },
        )),
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
