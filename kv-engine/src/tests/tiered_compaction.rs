use tempfile::tempdir;

use super::harness::{check_compaction_ratio, compaction_bench};
use crate::{
    compact::{CompactionOptions, TieredCompactionOptions},
    lsm_storage::{KvEngine, LsmStorageOptions},
};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(
        &dir,
        LsmStorageOptions {
            compaction_options: CompactionOptions::Tiered(TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 1,
                min_merge_width: 2,
                max_merge_width: None,
            }),
            num_memtable_limit: 2,
            target_sst_size: 1 << 20,
            ..LsmStorageOptions::default()
        },
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
