use tempfile::tempdir;

use super::harness::{check_compaction_ratio, compaction_bench};
use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{KvEngine, LsmStorageOptions},
};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(
        &dir,
        LsmStorageOptions {
            compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                level_size_multiplier: 2,
                base_level_size_mb: 1,
                max_levels: 4,
            }),
            num_memtable_limit: 2,
            target_sst_size: 1 << 20,
            ..LsmStorageOptions::default_for_test()
        },
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
