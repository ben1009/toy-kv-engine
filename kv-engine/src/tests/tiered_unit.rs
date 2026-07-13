use std::{collections::HashMap, sync::Arc};

use crate::{
    compact::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask},
    lsm_storage::LsmStorageState,
    mem_table::MemTable,
};

fn make_state(levels: Vec<(usize, Vec<usize>)>) -> LsmStorageState {
    LsmStorageState {
        memtable: Arc::new(MemTable::create(0, false)),
        imm_memtables: Vec::new(),
        l0_sstables: Vec::new(),
        levels,
        range_only_ssts: Vec::new(),
        sstables: HashMap::new(),
        max_sst_ts: 0,
        has_sst_range_tombstones: false,
        has_sst_ttl_entries: false,
    }
}

fn default_options() -> TieredCompactionOptions {
    TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 200,
        size_ratio: 1,
        min_merge_width: 2,
        max_merge_width: None,
    }
}

// --- generate_compaction_task ---

#[test]
fn test_no_task_when_below_num_tiers() {
    let ctrl = TieredCompactionController::new(default_options());
    let state = make_state(vec![(1, vec![1, 2]), (2, vec![3, 4])]);
    assert!(ctrl.generate_compaction_task(&state).is_none());
}

#[test]
fn test_space_amplification_triggers_full_compaction() {
    let ctrl = TieredCompactionController::new(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 200,
        size_ratio: 1,
        min_merge_width: 2,
        max_merge_width: None,
    });
    // 3 tiers: first two have 1 SST each, last has 1 → ratio = 2/1 = 200% → triggers
    let state = make_state(vec![(1, vec![1]), (2, vec![2]), (3, vec![3])]);
    let task = ctrl.generate_compaction_task(&state).unwrap();
    assert!(task.bottom_tier_included);
    assert_eq!(task.tiers.len(), 3);
}

#[test]
fn test_size_ratio_triggers_partial_compaction() {
    let ctrl = TieredCompactionController::new(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 200,
        size_ratio: 1,
        min_merge_width: 2,
        max_merge_width: None,
    });
    // 4 tiers: first has 10, second has 1 → ratio = 10/1 = 1000% → triggers size ratio
    // Space amp: (10+1+10)/10 = 2.1 → NOT triggered (< 200% threshold means >= 2.0 triggers)
    // Actually 2.1 >= 2.0 → still triggers space amp. Need to keep all_levels_but_last / last <
    // 2.0. Use: first=3, second=1, third=1, fourth=10 → space amp = 5/10=0.5 < 2.0
    // Size ratio l=0: pre_size=3, ratio=3/1=3.0 >= 1.01, l+2=2 >= 2 → triggers!
    let state = make_state(vec![
        (1, vec![1, 2, 3]),
        (2, vec![4]),
        (3, vec![5]),
        (4, vec![6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    ]);
    let task = ctrl.generate_compaction_task(&state).unwrap();
    // Should compact first two tiers (l=0, l+1=1 → tiers[0..2])
    assert_eq!(task.tiers.len(), 2);
    assert!(!task.bottom_tier_included);
}

#[test]
fn test_reduce_sorted_runs_fallback() {
    let ctrl = TieredCompactionController::new(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 1000, // very high → won't trigger
        size_ratio: 1000,                     // very high → won't trigger
        min_merge_width: 2,
        max_merge_width: None,
    });
    // 5 tiers, all equal size → no space amp or size ratio trigger
    let state = make_state(vec![
        (1, vec![1, 2]),
        (2, vec![3, 4]),
        (3, vec![5, 6]),
        (4, vec![7, 8]),
        (5, vec![9, 10]),
    ]);
    let task = ctrl.generate_compaction_task(&state).unwrap();
    // top_most = 5 - 3 + 1 = 3, so tiers[0..4] = first 4 tiers
    assert_eq!(task.tiers.len(), 4);
    assert!(!task.bottom_tier_included);
}

#[test]
fn test_reduce_sorted_runs_includes_bottom_when_all() {
    let ctrl = TieredCompactionController::new(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 1000,
        size_ratio: 1000,
        min_merge_width: 2,
        max_merge_width: None,
    });
    // Exactly 3 tiers → top_most = 3 - 3 + 1 = 1, tiers[0..2] = first 2
    let state = make_state(vec![(1, vec![1]), (2, vec![2]), (3, vec![3])]);
    let task = ctrl.generate_compaction_task(&state).unwrap();
    assert_eq!(task.tiers.len(), 2);
    // top_most + 1 = 2 < 3 → not bottom
    assert!(!task.bottom_tier_included);
}

// --- apply_compaction_result ---

#[test]
fn test_apply_compaction_result_basic() {
    let ctrl = TieredCompactionController::new(default_options());
    let state = make_state(vec![(1, vec![1, 2]), (2, vec![3, 4]), (3, vec![5, 6])]);
    let task = TieredCompactionTask {
        tiers: vec![(1, vec![1, 2]), (2, vec![3, 4])],
        bottom_tier_included: false,
    };
    let output = vec![100, 101];
    let (new_state, to_remove) = ctrl.apply_compaction_result(&state, &task, &output);

    // Should have: new tier [100, 101] + retained tier (3, [5, 6])
    assert_eq!(new_state.levels.len(), 2);
    assert_eq!(new_state.levels[0].0, 100); // new tier id = first output id
    assert_eq!(new_state.levels[0].1, vec![100, 101]);
    assert_eq!(new_state.levels[1], (3, vec![5, 6]));
    assert_eq!(to_remove, vec![1, 2, 3, 4]);
}

#[test]
fn test_apply_compaction_result_all_tiers() {
    let ctrl = TieredCompactionController::new(default_options());
    let state = make_state(vec![(1, vec![1]), (2, vec![2]), (3, vec![3])]);
    let task = TieredCompactionTask {
        tiers: vec![(1, vec![1]), (2, vec![2]), (3, vec![3])],
        bottom_tier_included: true,
    };
    let output = vec![50];
    let (new_state, to_remove) = ctrl.apply_compaction_result(&state, &task, &output);

    assert_eq!(new_state.levels.len(), 1);
    assert_eq!(new_state.levels[0].0, 50);
    assert_eq!(new_state.levels[0].1, vec![50]);
    assert_eq!(to_remove, vec![1, 2, 3]);
}

#[test]
fn test_apply_compaction_result_new_tier_inserted_in_middle() {
    let ctrl = TieredCompactionController::new(default_options());
    let state = make_state(vec![(1, vec![1]), (2, vec![2]), (3, vec![3]), (4, vec![4])]);
    // Compact tiers 1 and 2, keep 3 and 4
    let task = TieredCompactionTask {
        tiers: vec![(1, vec![1]), (2, vec![2])],
        bottom_tier_included: false,
    };
    let output = vec![99];
    let (new_state, to_remove) = ctrl.apply_compaction_result(&state, &task, &output);

    // After removing tiers 1,2 → new tier 99 inserted → then tiers 3,4
    assert_eq!(new_state.levels.len(), 3);
    assert_eq!(new_state.levels[0].0, 99);
    assert_eq!(new_state.levels[1], (3, vec![3]));
    assert_eq!(new_state.levels[2], (4, vec![4]));
    assert_eq!(to_remove, vec![1, 2]);
}
