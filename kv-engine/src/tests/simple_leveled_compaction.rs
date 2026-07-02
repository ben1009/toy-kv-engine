use tempfile::tempdir;

use super::block_on_result;
use super::harness::{check_compaction_ratio, compaction_bench};
use crate::{
    compact::{CompactionOptions, SimpleLeveledCompactionOptions},
    lsm_storage::{KvEngine, LsmStorageOptions},
};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let storage = block_on_result(KvEngine::open(
        &dir,
        LsmStorageOptions {
            compaction_options: CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                max_levels: 3,
                size_ratio_percent: 200,
            }),
            num_memtable_limit: 2,
            target_sst_size: 1 << 20,
            ..LsmStorageOptions::default_for_test()
        },
    ))
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
