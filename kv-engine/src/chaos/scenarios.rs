//! Deterministic crash-test scenarios (RFC 013 Phase 1).
//!
//! Each scenario function executes a scripted workload against the engine while
//! recording operation intents and durability boundaries to the control log.
//! The parent harness kills the child process and uses the control log to validate
//! post-crash state.

use crate::chaos::control_log::{ControlLogWriter, OperationKind};
use crate::lsm_storage::{KvEngine, LsmStorageOptions};

/// Options for a scenario.
pub struct ScenarioConfig {
    pub name: &'static str,
    pub num_keys: usize,
    pub key_prefix: &'static str,
    pub storage_options: LsmStorageOptions,
}

impl ScenarioConfig {
    pub fn wal_only() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 0;
        Self {
            name: "wal-only",
            num_keys: 150,
            key_prefix: "k",
            storage_options: opts,
        }
    }

    pub fn flush_boundary() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 0;
        Self {
            name: "flush-boundary",
            num_keys: 80,
            key_prefix: "k",
            storage_options: opts,
        }
    }

    pub fn manifest_snapshot() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 256;
        Self {
            name: "manifest-snapshot",
            num_keys: 100,
            key_prefix: "k",
            storage_options: opts,
        }
    }

    pub fn range_tombstone() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 0;
        // delete_range is incompatible with serializable and value_separation
        // in the MVP — default_for_test() already sets both off, but be explicit.
        opts.serializable = false;
        opts.value_separation = None;
        Self {
            name: "range-tombstone",
            num_keys: 100,
            key_prefix: "rt",
            storage_options: opts,
        }
    }

    pub fn vlog() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 0;
        opts.serializable = false;
        // Enable value separation with a low threshold so most values are separated.
        opts.value_separation = Some(crate::vlog::ValueSeparationOptions {
            enabled: true,
            min_value_size: 512,
            max_value_size: 128 << 20,
            max_vlog_file_size: 64 << 20,
            gc_threshold_ratio: 0.5,
            max_open_vlog_files: 64,
            value_cache_capacity_bytes: 0, // disable cache to exercise read path
        });
        Self {
            name: "vlog",
            num_keys: 80,
            key_prefix: "vl",
            storage_options: opts,
        }
    }

    pub fn leveled_compaction() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 0;
        opts.serializable = false;
        // Small SST size creates many files, triggering compaction quickly.
        opts.target_sst_size = 4096;
        opts.compaction_options =
            crate::compact::CompactionOptions::Leveled(crate::compact::LeveledCompactionOptions {
                level_size_multiplier: 10,
                level0_file_num_compaction_trigger: 2,
                max_levels: 3,
                base_level_size_mb: 1,
            });
        Self {
            name: "leveled-compaction",
            num_keys: 60,
            key_prefix: "lc",
            storage_options: opts,
        }
    }

    pub fn tiered_compaction() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 0;
        opts.serializable = false;
        // Small SST size creates many files, triggering compaction quickly.
        opts.target_sst_size = 4096;
        opts.compaction_options =
            crate::compact::CompactionOptions::Tiered(crate::compact::TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 10,
                min_merge_width: 2,
                max_merge_width: None,
            });
        Self {
            name: "tiered-compaction",
            num_keys: 60,
            key_prefix: "tc",
            storage_options: opts,
        }
    }
}

/// Run the WAL-only restart scenario.
///
/// 1. Write 100 keys (op_id 1-100)
/// 2. Write 50 more keys (101-150) + delete 20 keys (151-170)
/// 3. Write sync point marker
///
/// Intentionally avoids `force_flush()` so this scenario isolates pure WAL
/// recovery instead of exercising flushed-SST crash behavior.
pub fn wal_only_restart(
    engine: &KvEngine,
    log: &mut ControlLogWriter,
    config: &ScenarioConfig,
    _seed: u64,
) -> Result<(), String> {
    // Phase 1: Write 100 keys
    for i in 0..100 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = format!("v_{}", i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 2: Write 50 more + delete 20
    for i in 100..150 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = format!("v_{}", i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }
    for i in 0..20 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let op_id = log.next_op_id();
        log.write_intent(op_id, OperationKind::Delete { key: key.clone() })
            .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .delete(key.as_bytes())
            .map_err(|e| format!("delete failed: {e}"))?;
        log.write_durability_boundary(op_id, OperationKind::Delete { key: key.clone() })
            .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Sync point — parent will kill here
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;

    Ok(())
}

/// Run the flush boundary scenario (WAL-heavy recovery, mixed value sizes).
///
/// Mixes small and large values — all WAL-backed, no explicit flushes.
pub fn flush_boundary(
    engine: &KvEngine,
    log: &mut ControlLogWriter,
    config: &ScenarioConfig,
    _seed: u64,
) -> Result<(), String> {
    let large_value = vec![b'x'; 2000];
    let large_value_str = "x".repeat(2000);

    // Phase 1: Write 50 small keys
    for i in 0..50 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = if i % 2 == 0 {
            format!("v_{i}")
        } else {
            "y".repeat(1000)
        };
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 2: Write 30 keys with large values
    for i in 50..80 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: large_value_str.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), &large_value)
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: large_value_str.clone(),
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Force a flush so at least one SST is created, exercising
    // the crash-recovery path through flushed (not just WAL) data.
    engine
        .force_flush()
        .map_err(|e| format!("force_flush failed: {e}"))?;

    // Sync point
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;

    Ok(())
}

/// Run the manifest snapshot churn scenario.
///
/// Writes 100 keys sequentially with manifest snapshot threshold of 1024 bytes
/// to exercise the crash-safe snapshot handoff. No explicit flushes.
pub fn manifest_snapshot_churn(
    engine: &KvEngine,
    log: &mut ControlLogWriter,
    config: &ScenarioConfig,
    _seed: u64,
) -> Result<(), String> {
    // Write all 100 keys sequentially
    for i in 0..config.num_keys {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = if i % 2 == 0 {
            format!("v_{i}")
        } else {
            "y".repeat(1000)
        };
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;

        if i % 10 == 9 {
            // Flush periodically so the manifest grows and crosses the snapshot threshold.
            engine
                .force_flush()
                .map_err(|e| format!("force_flush failed: {e}"))?;
        }
    }

    // Sync point
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;

    Ok(())
}

/// Run the range tombstone restart scenario.
///
/// 1. Write 100 keys (op_id 0-99) with committed durability boundaries
/// 2. delete_range covering keys 20-69
/// 3. Write keys 30-39 with new values (after delete_range, these survive)
/// 4. Sync point
///
/// After crash + reopen the oracle verifies:
/// - Keys 0-19:  original values survive
/// - Keys 20-29: deleted (covered by delete_range, not rewritten)
/// - Keys 30-39: new post-delete values survive
/// - Keys 40-69: deleted (covered by delete_range)
/// - Keys 70-99: original values survive
pub fn range_tombstone_restart(
    engine: &KvEngine,
    log: &mut ControlLogWriter,
    config: &ScenarioConfig,
    _seed: u64,
) -> Result<(), String> {
    // Phase 1: Write all 100 keys
    for i in 0..config.num_keys {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = format!("v_{}", i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 2: Delete range covering keys 20-69
    let range_start = format!("{}_{:010}", config.key_prefix, 20);
    let range_end = format!("{}_{:010}", config.key_prefix, 70);
    {
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::DeleteRange {
                start: range_start.clone(),
                end: range_end.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .delete_range(range_start.as_bytes(), range_end.as_bytes())
            .map_err(|e| format!("delete_range failed: {e}"))?;
        // delete_range does not internally commit the WAL — sync
        // explicitly so the range tombstone is durable before we
        // record the durability-boundary marker in the control log.
        engine
            .sync()
            .map_err(|e| format!("sync after delete_range failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::DeleteRange {
                start: range_start,
                end: range_end,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 3: Write keys 30-39 with new values after the delete_range.
    // These keys were in the deleted range but are rewritten — they must survive.
    for i in 30..40 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = format!("v_{}_after_delete", i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Sync point — parent will kill here
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;

    Ok(())
}

/// Run the vLog restart scenario.
///
/// 1. Write 50 keys with large values (2000 bytes, value-separated)
/// 2. Write 30 keys with small values (100 bytes, inline in SST)
/// 3. Force flush to create SSTs with value pointers
/// 4. Delete 30 large-value keys (0-29) to exceed GC threshold
/// 5. Force flush + full compaction to exercise vLog GC
/// 6. Sync point
///
/// After crash + reopen the oracle verifies:
/// - Surviving large-value keys still have their values
/// - Small-value keys still have their values
/// - Deleted keys are absent
/// - No dangling or resurrected value references
pub fn vlog_restart(
    engine: &KvEngine,
    log: &mut ControlLogWriter,
    config: &ScenarioConfig,
    _seed: u64,
) -> Result<(), String> {
    let large_value_str = "X".repeat(2000);
    let small_value_str = "s".repeat(100);

    // Phase 1: Write 50 keys with large values (value-separated)
    for i in 0..50 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: large_value_str.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), large_value_str.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: large_value_str.clone(),
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 2: Write 30 keys with small values (inline in SST, not separated)
    for i in 50..80 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: small_value_str.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), small_value_str.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: small_value_str.clone(),
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 3: Force flush to create SSTs containing value pointers
    engine
        .force_flush()
        .map_err(|e| format!("force_flush failed: {e}"))?;

    // Phase 4: Delete 30 large-value keys (0-29) to exceed the 0.5 GC threshold
    // (30/50 = 60% stale ratio > 50% gc_threshold_ratio).
    for i in 0..30 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let op_id = log.next_op_id();
        log.write_intent(op_id, OperationKind::Delete { key: key.clone() })
            .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .delete(key.as_bytes())
            .map_err(|e| format!("delete failed: {e}"))?;
        log.write_durability_boundary(op_id, OperationKind::Delete { key: key.clone() })
            .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Phase 5: Flush + full compaction + trigger GC to exercise vLog GC path
    engine
        .force_flush()
        .map_err(|e| format!("force_flush after deletes failed: {e}"))?;
    engine
        .force_full_compaction()
        .map_err(|e| format!("force_full_compaction failed: {e}"))?;
    engine
        .trigger_gc()
        .map_err(|e| format!("trigger_gc failed: {e}"))?;

    // Sync point — parent will kill here
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;

    Ok(())
}

/// Run a compaction restart scenario (leveled or tiered).
///
/// Uses small SST sizes to create multiple files, forces a full compaction, then
/// writes additional keys. Validates post-crash data integrity across the compacted
/// SST structure. The compaction strategy is determined by the scenario config.
///
/// 1. Write 40 keys in 2 batches of 20, flushing between batches → multiple L0 SSTs
/// 2. Force full compaction
/// 3. Write 20 more keys after compaction
/// 4. Sync point
pub fn compaction_restart(
    engine: &KvEngine,
    log: &mut ControlLogWriter,
    config: &ScenarioConfig,
    _seed: u64,
) -> Result<(), String> {
    // Phase 1: Write 2 batches of 20 keys, flushing between batches
    for batch in 0..2 {
        for i in (batch * 20)..(batch * 20 + 20) {
            let key = format!("{}_{:010}", config.key_prefix, i);
            let value = format!("v_{}", i);
            let op_id = log.next_op_id();
            log.write_intent(
                op_id,
                OperationKind::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
            )
            .map_err(|e| format!("write_intent failed: {e}"))?;
            engine
                .put(key.as_bytes(), value.as_bytes())
                .map_err(|e| format!("put failed: {e}"))?;
            log.write_durability_boundary(
                op_id,
                OperationKind::Put {
                    key: key.clone(),
                    value,
                },
            )
            .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
        }
        engine
            .force_flush()
            .map_err(|e| format!("force_flush failed: {e}"))?;
    }

    // Phase 2: Force full compaction.
    // Note: under tiered compaction, force_full_compaction only compacts
    // the newest tier; other tiers are not merged. Precise compaction
    // boundary injection is deferred to Phase 3 failpoints.
    engine
        .force_full_compaction()
        .map_err(|e| format!("force_full_compaction failed: {e}"))?;

    // Phase 3: Write 20 more keys after compaction
    for i in 40..60 {
        let key = format!("{}_{:010}", config.key_prefix, i);
        let value = format!("v_{}", i);
        let op_id = log.next_op_id();
        log.write_intent(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .map_err(|e| format!("write_intent failed: {e}"))?;
        engine
            .put(key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("put failed: {e}"))?;
        log.write_durability_boundary(
            op_id,
            OperationKind::Put {
                key: key.clone(),
                value,
            },
        )
        .map_err(|e| format!("write_durability_boundary failed: {e}"))?;
    }

    // Sync point — parent will kill here
    log.write_sync_point()
        .map_err(|e| format!("write_sync_point failed: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaos::control_log::ControlLogReader;

    #[test]
    fn test_scenario_config_wal_only() {
        let cfg = ScenarioConfig::wal_only();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.num_keys, 150);
        assert_eq!(cfg.name, "wal-only");
    }

    #[test]
    fn test_scenario_config_flush_boundary() {
        let cfg = ScenarioConfig::flush_boundary();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.num_keys, 80);
        assert_eq!(cfg.name, "flush-boundary");
    }

    #[test]
    fn test_scenario_config_manifest_snapshot() {
        let cfg = ScenarioConfig::manifest_snapshot();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.storage_options.manifest_snapshot_threshold_bytes, 256);
        assert_eq!(cfg.name, "manifest-snapshot");
    }

    #[test]
    fn test_wal_only_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::wal_only();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        wal_only_restart(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 150 puts + 20 deletes + 1 sync point = 171 committed ops
        assert_eq!(
            committed.len(),
            171,
            "expected 171 committed ops, got {}",
            committed.len()
        );
    }

    #[test]
    fn test_flush_boundary_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::flush_boundary();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        flush_boundary(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 80 puts + 1 sync point = 81 committed ops
        assert_eq!(
            committed.len(),
            81,
            "expected 81 committed ops, got {}",
            committed.len()
        );
    }

    #[test]
    fn test_manifest_snapshot_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::manifest_snapshot();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        manifest_snapshot_churn(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 100 puts + 1 sync point = 101 committed ops
        assert_eq!(
            committed.len(),
            101,
            "expected 101 committed ops, got {}",
            committed.len()
        );
    }

    #[test]
    fn test_scenario_config_range_tombstone() {
        let cfg = ScenarioConfig::range_tombstone();
        assert!(cfg.storage_options.enable_wal);
        assert!(!cfg.storage_options.serializable);
        assert!(cfg.storage_options.value_separation.is_none());
        assert_eq!(cfg.num_keys, 100);
        assert_eq!(cfg.name, "range-tombstone");
    }

    #[test]
    fn test_range_tombstone_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::range_tombstone();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        range_tombstone_restart(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 100 puts + 1 delete_range + 10 puts (keys 30-39) + 1 sync point = 112 committed ops
        assert_eq!(
            committed.len(),
            112,
            "expected 112 committed ops, got {}",
            committed.len()
        );
    }

    #[test]
    fn test_scenario_config_vlog() {
        let cfg = ScenarioConfig::vlog();
        assert!(cfg.storage_options.enable_wal);
        assert!(!cfg.storage_options.serializable);
        assert!(cfg.storage_options.value_separation.is_some());
        let vs = cfg.storage_options.value_separation.as_ref().unwrap();
        assert!(vs.enabled);
        assert_eq!(vs.min_value_size, 512);
        assert_eq!(cfg.num_keys, 80);
        assert_eq!(cfg.name, "vlog");
    }

    #[test]
    fn test_vlog_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::vlog();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        vlog_restart(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 50 large puts + 30 small puts + 30 deletes + 1 sync point = 111 committed ops
        assert_eq!(
            committed.len(),
            111,
            "expected 111 committed ops, got {}",
            committed.len()
        );
    }

    #[test]
    fn test_scenario_config_leveled_compaction() {
        let cfg = ScenarioConfig::leveled_compaction();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.storage_options.target_sst_size, 4096);
        assert_eq!(cfg.num_keys, 60);
        assert_eq!(cfg.name, "leveled-compaction");
    }

    #[test]
    fn test_leveled_compaction_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::leveled_compaction();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        compaction_restart(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 60 puts + 1 sync point = 61 committed ops
        assert_eq!(
            committed.len(),
            61,
            "expected 61 committed ops, got {}",
            committed.len()
        );
    }

    #[test]
    fn test_scenario_config_tiered_compaction() {
        let cfg = ScenarioConfig::tiered_compaction();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.storage_options.target_sst_size, 4096);
        assert_eq!(cfg.num_keys, 60);
        assert_eq!(cfg.name, "tiered-compaction");
    }

    #[test]
    fn test_tiered_compaction_scenario_control_log() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let log_path = dir.path().join("control.log");
        let cfg = ScenarioConfig::tiered_compaction();
        let engine =
            crate::lsm_storage::KvEngine::open(&db_path, cfg.storage_options.clone()).unwrap();
        let mut log = ControlLogWriter::new(&log_path).unwrap();
        compaction_restart(&engine, &mut log, &cfg, 42).unwrap();
        engine.close().unwrap();
        let reader = ControlLogReader::open(&log_path).unwrap();
        let (committed, _) = reader.classify_visibility();
        // 60 puts + 1 sync point = 61 committed ops
        assert_eq!(
            committed.len(),
            61,
            "expected 61 committed ops, got {}",
            committed.len()
        );
    }
}
