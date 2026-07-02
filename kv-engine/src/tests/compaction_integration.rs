use tempfile::tempdir;

use super::block_on_result;
use crate::{
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    lsm_storage::{KvEngine, LsmStorageOptions},
    tests::harness::dump_files_in_dir,
};

#[test]
fn test_integration_leveled() {
    test_integration(CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
        base_level_size_mb: 1,
    }))
}

#[test]
fn test_integration_tiered() {
    test_integration(CompactionOptions::Tiered(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 200,
        size_ratio: 1,
        min_merge_width: 3,

        max_merge_width: None,
    }))
}

#[test]
fn test_integration_simple() {
    test_integration(CompactionOptions::Simple(SimpleLeveledCompactionOptions {
        size_ratio_percent: 200,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
    }));
}

fn test_integration(compaction_options: CompactionOptions) {
    let dir = tempdir().unwrap();
    let storage = block_on_result(KvEngine::open(
        &dir,
        LsmStorageOptions {
            compaction_options: compaction_options.clone(),
            num_memtable_limit: 2,
            target_sst_size: 1 << 20,
            ..LsmStorageOptions::default_for_test()
        },
    ))
    .unwrap();
    for i in 0..=20 {
        block_on_result(storage.put(b"0", format!("v{i}").as_bytes())).unwrap();
        if i % 2 == 0 {
            block_on_result(storage.put(b"1", format!("v{i}").as_bytes())).unwrap();
        } else {
            block_on_result(storage.delete(b"1")).unwrap();
        }
        if i % 2 == 1 {
            block_on_result(storage.put(b"2", format!("v{i}").as_bytes())).unwrap();
        } else {
            block_on_result(storage.delete(b"2")).unwrap();
        }
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
    }
    block_on_result(storage.close()).unwrap();
    // ensure all SSTs are flushed
    assert!(storage.inner.state.load().memtable.is_empty());
    assert!(storage.inner.state.load().imm_memtables.is_empty());
    storage.dump_structure();
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = block_on_result(KvEngine::open(
        &dir,
        LsmStorageOptions {
            compaction_options: compaction_options.clone(),
            num_memtable_limit: 2,
            target_sst_size: 1 << 20,
            ..LsmStorageOptions::default_for_test()
        },
    ))
    .unwrap();
    assert_eq!(
        &block_on_result(storage.get(b"0")).unwrap().unwrap()[..],
        b"v20".as_slice()
    );
    assert_eq!(
        &block_on_result(storage.get(b"1")).unwrap().unwrap()[..],
        b"v20".as_slice()
    );
    assert_eq!(block_on_result(storage.get(b"2")).unwrap(), None);
}
