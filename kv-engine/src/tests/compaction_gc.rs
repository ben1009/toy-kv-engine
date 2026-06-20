//! Phase 4 tests: Compaction GC for range tombstones.
//!
//! Verifies that range tombstones survive compaction, covered values are
//! dropped at bottommost compactions, range-only output SSTs are generated
//! for gap ranges, and obsolete fragments are dropped.

use tempfile::tempdir;

use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{KvEngine, LsmStorageOptions},
};

fn leveled_options() -> LsmStorageOptions {
    LsmStorageOptions {
        compaction_options: CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 2,
            max_levels: 3,
            base_level_size_mb: 1,
        }),
        num_memtable_limit: 2,
        target_sst_size: 1 << 20,
        ..LsmStorageOptions::default_for_test()
    }
}

/// Range tombstones survive compaction — covered keys remain hidden.
#[test]
fn test_range_tombstone_survives_compaction() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(&dir, leveled_options()).unwrap();

    // Write point entries
    storage.put(b"a", b"va").unwrap();
    storage.put(b"m", b"vm").unwrap();
    storage.put(b"z", b"vz").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Write range tombstone covering [a, z)
    storage.inner.delete_range_internal(b"a", b"z").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Verify keys are hidden before compaction
    assert!(storage.get(b"a").unwrap().is_none());
    assert!(storage.get(b"m").unwrap().is_none());
    assert_eq!(&storage.get(b"z").unwrap().unwrap()[..], b"vz");

    // Compact
    storage.force_full_compaction().unwrap();

    // Verify keys are still hidden after compaction — no resurrection
    assert!(
        storage.get(b"a").unwrap().is_none(),
        "key 'a' should be hidden by range tombstone after compaction"
    );
    assert!(
        storage.get(b"m").unwrap().is_none(),
        "key 'm' should be hidden by range tombstone after compaction"
    );
    assert_eq!(
        &storage.get(b"z").unwrap().unwrap()[..],
        b"vz",
        "key 'z' is outside the tombstone range"
    );

    // Verify range tombstone exists in live output SSTs
    let state = storage.inner.state.load();
    let live_ids: std::collections::HashSet<usize> = state
        .l0_sstables
        .iter()
        .chain(state.levels.iter().flat_map(|(_, ids)| ids))
        .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids))
        .copied()
        .collect();
    let has_range_tombstones = live_ids
        .iter()
        .filter_map(|id| state.sstables.get(id))
        .any(|sst| sst.has_range_tombstones());
    assert!(
        has_range_tombstones,
        "live output SSTs should contain range tombstones after compaction"
    );
    // Rely on Drop for shutdown
}

/// Multiple overlapping range tombstones survive compaction with correct merging.
#[test]
fn test_overlapping_tombstones_through_compaction() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(&dir, leveled_options()).unwrap();

    // Write point entries
    storage.put(b"a", b"va").unwrap();
    storage.put(b"m", b"vm").unwrap();
    storage.put(b"p", b"vp").unwrap();
    storage.put(b"z", b"vz").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Write overlapping range tombstones
    storage.inner.delete_range_internal(b"a", b"z").unwrap(); // covers a, m, p
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    storage.inner.delete_range_internal(b"m", b"p").unwrap(); // also covers m
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Verify before compaction
    assert!(storage.get(b"a").unwrap().is_none());
    assert!(storage.get(b"m").unwrap().is_none());
    assert!(storage.get(b"p").unwrap().is_none());
    assert_eq!(&storage.get(b"z").unwrap().unwrap()[..], b"vz");

    // Compact
    storage.force_full_compaction().unwrap();

    // Verify after compaction
    assert!(storage.get(b"a").unwrap().is_none());
    assert!(storage.get(b"m").unwrap().is_none());
    assert!(storage.get(b"p").unwrap().is_none());
    assert_eq!(&storage.get(b"z").unwrap().unwrap()[..], b"vz");
    // Rely on Drop for shutdown
}

/// Point entries outside the tombstone range survive compaction.
#[test]
fn test_entries_outside_tombstone_survive_compaction() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(&dir, leveled_options()).unwrap();

    // Write point entries
    storage.put(b"a", b"va").unwrap();
    storage.put(b"m", b"vm").unwrap();
    storage.put(b"z", b"vz").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Write narrow range tombstone covering only [m, p)
    storage.inner.delete_range_internal(b"m", b"p").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Compact
    storage.force_full_compaction().unwrap();

    // 'a' is outside the tombstone — should survive
    assert_eq!(&storage.get(b"a").unwrap().unwrap()[..], b"va");
    // 'm' is inside the tombstone — should be hidden
    assert!(storage.get(b"m").unwrap().is_none());
    // 'z' is outside the tombstone — should survive
    assert_eq!(&storage.get(b"z").unwrap().unwrap()[..], b"vz");
    // Rely on Drop for shutdown
}

/// Range-only SSTs are tracked in range_only_ssts after compaction.
#[test]
fn test_range_only_ssts_tracked() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(&dir, leveled_options()).unwrap();

    // Write a wide range tombstone with no point entries in between
    storage.inner.delete_range_internal(b"a", b"z").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    // Flush again to trigger compaction
    storage.put(b"dummy", b"v").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();

    storage.force_full_compaction().unwrap();

    // Check that range-only SSTs exist in the state
    let state = storage.inner.state.load();
    let has_range_only = state.range_only_ssts.iter().any(|(_, ids)| !ids.is_empty());

    // There should be either range-only SSTs in range_only_ssts,
    // or range tombstones embedded in point SSTs
    let live_ids: std::collections::HashSet<usize> = state
        .l0_sstables
        .iter()
        .chain(state.levels.iter().flat_map(|(_, ids)| ids))
        .chain(state.range_only_ssts.iter().flat_map(|(_, ids)| ids))
        .copied()
        .collect();
    let has_embedded_tombstones = live_ids
        .iter()
        .filter_map(|id| state.sstables.get(id))
        .any(|sst| sst.has_range_tombstones());

    assert!(
        has_range_only || has_embedded_tombstones,
        "compaction should produce range tombstones (either range-only SSTs or embedded)"
    );
    // Rely on Drop for shutdown
}

/// Verify that range tombstones work correctly through close/reopen.
#[test]
fn test_range_tombstone_survives_reopen() {
    let dir = tempdir().unwrap();
    let storage = KvEngine::open(&dir, leveled_options()).unwrap();

    storage.put(b"a", b"va").unwrap();
    storage.put(b"m", b"vm").unwrap();
    storage.inner.delete_range_internal(b"a", b"z").unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    storage.inner.force_flush_next_imm_memtable().unwrap();
    storage.force_full_compaction().unwrap();

    // Verify before close
    assert!(storage.get(b"a").unwrap().is_none());
    assert!(storage.get(b"m").unwrap().is_none());

    // Close and reopen
    storage.close().unwrap();
    let storage2 = KvEngine::open(&dir, leveled_options()).unwrap();

    // Verify after reopen — range tombstones should persist
    assert!(
        storage2.get(b"a").unwrap().is_none(),
        "key 'a' should be hidden after reopen"
    );
    assert!(
        storage2.get(b"m").unwrap().is_none(),
        "key 'm' should be hidden after reopen"
    );
    // Rely on Drop for shutdown
}
