mod advanced;
mod basic;
mod cache;
mod gc;
mod manifest;
mod sst_builder;

use std::sync::Arc;

use bytes::Bytes;

pub(crate) use crate::FutureResultExt;

use crate::{
    compact::CompactionOptions,
    iterators::StorageIterator,
    key::KeySlice,
    lsm_storage::{KvEngine, LsmStorageInner, LsmStorageOptions, PrefixBloomOptions},
    table::SsTableBuilder,
    vlog::ValueSeparationOptions,
};

fn options_with_vlog_enabled(block_size: usize, target_sst_size: usize) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size,
        target_sst_size,
        num_memtable_limit: 2,
        compaction_options: CompactionOptions::NoCompaction,
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size: 16, // Separate values >= 16 bytes
            ..Default::default()
        }),
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1024,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
    }
}

fn options_with_vlog_and_compaction(
    block_size: usize,
    target_sst_size: usize,
) -> LsmStorageOptions {
    use crate::compact::LeveledCompactionOptions;
    LsmStorageOptions {
        block_size,
        target_sst_size,
        num_memtable_limit: 2,
        // High trigger prevents background compaction from racing with manual
        // force_full_compaction() calls in tests.
        compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 100,
            max_levels: 3,
            base_level_size_mb: 1,
            level_size_multiplier: 2,
        }),
        enable_wal: false,
        serializable: false,
        value_separation: Some(ValueSeparationOptions {
            enabled: true,
            min_value_size: 16,
            ..Default::default()
        }),
        manifest_snapshot_threshold_bytes: 0,
        block_cache_capacity: 1024,
        enable_cache_backfill: true,
        prefix_bloom: PrefixBloomOptions::default(),
    }
}

/// Flush the active memtable to an SST (and vLog if enabled).
fn force_flush(storage: &LsmStorageInner) {
    storage
        .force_freeze_memtable(&storage.state_lock.lock())
        .unwrap();
    storage.force_flush_next_imm_memtable().unwrap();
}
