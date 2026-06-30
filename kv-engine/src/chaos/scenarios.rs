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
            num_keys: 200,
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
            num_keys: 100,
            key_prefix: "k",
            storage_options: opts,
        }
    }

    pub fn manifest_snapshot() -> Self {
        let mut opts = LsmStorageOptions::default_for_test();
        opts.enable_wal = true;
        opts.manifest_snapshot_threshold_bytes = 1024;
        Self {
            name: "manifest-snapshot",
            num_keys: 100,
            key_prefix: "k",
            storage_options: opts,
        }
    }
}

/// Run the WAL-only restart scenario.
///
/// 1. Write 100 keys (op_id 1-100)
/// 2. Write 50 more keys (101-150) + delete 20 keys (151-170)
/// 3. Write sync point marker
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
    }

    // Sync point
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
        assert_eq!(cfg.num_keys, 200);
        assert_eq!(cfg.name, "wal-only");
    }

    #[test]
    fn test_scenario_config_flush_boundary() {
        let cfg = ScenarioConfig::flush_boundary();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.num_keys, 100);
        assert_eq!(cfg.name, "flush-boundary");
    }

    #[test]
    fn test_scenario_config_manifest_snapshot() {
        let cfg = ScenarioConfig::manifest_snapshot();
        assert!(cfg.storage_options.enable_wal);
        assert_eq!(cfg.storage_options.manifest_snapshot_threshold_bytes, 1024);
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
}
