//! Oracle for chaos-testing.
//!
//! Provides a bounded-key-universe reconciliation that compares the post-crash
//! database state against the expected state derived from the control log.
//! Also provides structural checks (double-reopen, close-after-reopen, etc.).

use crate::chaos::control_log::{self, Checkpoint, OperationKind};
use crate::lsm_storage::{KvEngine, LsmStorageOptions};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Generate the bounded key universe used in a scenario.
///
/// Keys are deterministically generated: `f"{prefix}_{i:010}"` for i in 0..count.
pub struct BoundedKeyUniverse {
    keys: Vec<Vec<u8>>,
    pub prefix: String,
}

impl BoundedKeyUniverse {
    pub fn new(prefix: &str, count: usize) -> Self {
        let keys: Vec<Vec<u8>> = (0..count)
            .map(|i| format!("{prefix}_{i:010}").into_bytes())
            .collect();
        Self {
            keys,
            prefix: prefix.to_string(),
        }
    }

    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }
}

/// Expected state derived from replaying committed control-log operations.
pub struct ReferenceState {
    /// For each key, the expected post-crash value (None = key should be absent/deleted).
    pub expected: HashMap<Vec<u8>, Option<Bytes>>,
    /// Keys that may legitimately appear even if not in `expected` (durable-before-ack window).
    pub possibly_visible: BTreeSet<Vec<u8>>,
    /// Committed operation ids used to build this state.
    pub committed_op_ids: Vec<u64>,
    /// Possibly-visible (intent-only) operation ids.
    pub possibly_visible_op_ids: Vec<u64>,
}

impl ReferenceState {
    /// Build a reference state from the control log and the bounded key universe.
    ///
    /// Committed operations are applied to the reference map. Possibly-visible operations
    /// are tracked in `possibly_visible` so the oracle can distinguish "lost durable write"
    /// from "non-durable write that happened to survive."
    pub fn from_control_log(reader: &control_log::ControlLogReader) -> Self {
        let (committed_ids, possibly_visible_ids) = reader.classify_visibility();

        // Collect committed records in op_id order for deterministic replay
        let mut records_by_op: BTreeMap<u64, Vec<&control_log::ControlLogRecord>> = BTreeMap::new();
        for rec in reader.records() {
            if rec.checkpoint == Checkpoint::DurabilityBoundaryPassed {
                records_by_op.entry(rec.op_id).or_default().push(rec);
            }
        }

        let mut expected: HashMap<Vec<u8>, Option<Bytes>> = HashMap::new();
        let mut possibly_visible: BTreeSet<Vec<u8>> = BTreeSet::new();

        // Apply committed operations
        for records in records_by_op.values() {
            for rec in records {
                apply_operation(&mut expected, &rec.kind);
            }
        }

        // Track possibly-visible keys — only for uncommitted (intent-only) op_ids
        let committed_set: std::collections::BTreeSet<u64> =
            committed_ids.iter().copied().collect();
        for rec in reader.records() {
            if rec.checkpoint == Checkpoint::IntentStarted && !committed_set.contains(&rec.op_id) {
                match &rec.kind {
                    OperationKind::Put { key, .. }
                    | OperationKind::Delete { key }
                    | OperationKind::DeleteRange { start: key, .. } => {
                        possibly_visible.insert(key.as_bytes().to_vec());
                    }
                    OperationKind::WriteBatch { entries } => {
                        for e in entries {
                            possibly_visible.insert(e.key.as_bytes().to_vec());
                        }
                    }
                    _ => {}
                }
            }
        }

        Self {
            expected,
            possibly_visible,
            committed_op_ids: committed_ids,
            possibly_visible_op_ids: possibly_visible_ids,
        }
    }
}

fn apply_operation(state: &mut HashMap<Vec<u8>, Option<Bytes>>, kind: &OperationKind) {
    match kind {
        OperationKind::Put { key, value } => {
            state.insert(
                key.as_bytes().to_vec(),
                Some(Bytes::copy_from_slice(value.as_bytes())),
            );
        }
        OperationKind::Delete { key } => {
            state.insert(key.as_bytes().to_vec(), None);
        }
        OperationKind::DeleteRange { start, end } => {
            // Mark all keys in the speculative range as deleted
            // (the concrete key range is scenario-specific)
            let start_bytes = start.as_bytes();
            let end_bytes = end.as_bytes();
            state
                .keys()
                .filter(|k| k.as_slice() >= start_bytes && k.as_slice() < end_bytes)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .for_each(|k| {
                    state.insert(k, None);
                });
        }
        OperationKind::WriteBatch { entries } => {
            for entry in entries {
                match entry.kind {
                    control_log::BatchOpKind::Put => {
                        let val = entry.value.as_deref().unwrap_or("");
                        state.insert(
                            entry.key.as_bytes().to_vec(),
                            Some(Bytes::copy_from_slice(val.as_bytes())),
                        );
                    }
                    control_log::BatchOpKind::Delete => {
                        state.insert(entry.key.as_bytes().to_vec(), None);
                    }
                }
            }
        }
        OperationKind::Flush | OperationKind::FullCompaction | OperationKind::SyncPoint => {
            // No-ops for state reconciliation
        }
    }
}

/// The result of reconciling the actual post-crash state against the reference.
#[derive(Debug, Default)]
pub struct ReconciliationResult {
    pub violations: Vec<Violation>,
    pub total_keys_checked: usize,
    pub committed_op_count: usize,
    pub possibly_visible_op_count: usize,
}

#[derive(Debug)]
pub struct Violation {
    pub key: Vec<u8>,
    pub kind: ViolationKind,
}

#[derive(Debug)]
pub enum ViolationKind {
    LostDurableWrite { expected_value: Bytes },
    ResurrectedDelete,
    WrongValue { expected: Bytes, actual: Bytes },
    UnexpectedKey { actual_value: Bytes },
}

/// Reconcile the post-crash database state against the reference.
///
/// Scans all keys in the bounded universe, checks each against the expected state,
/// and reports any violations.
pub fn reconcile(
    engine: &KvEngine,
    universe: &BoundedKeyUniverse,
    reference: &ReferenceState,
) -> anyhow::Result<ReconciliationResult> {
    let mut result = ReconciliationResult::default();

    for key_bytes in &universe.keys {
        result.total_keys_checked += 1;
        let actual = engine.get(key_bytes)?;

        let expected = reference.expected.get(key_bytes);

        match (expected, actual) {
            (Some(Some(expected_val)), Some(actual_val)) => {
                if expected_val != &*actual_val && !reference.possibly_visible.contains(key_bytes) {
                    result.violations.push(Violation {
                        key: key_bytes.clone(),
                        kind: ViolationKind::WrongValue {
                            expected: expected_val.clone(),
                            actual: actual_val,
                        },
                    });
                }
            }
            (Some(Some(expected_val)), None) => {
                // Key should have a value but was not found - lost durable write
                if !reference.possibly_visible.contains(key_bytes) {
                    result.violations.push(Violation {
                        key: key_bytes.clone(),
                        kind: ViolationKind::LostDurableWrite {
                            expected_value: expected_val.clone(),
                        },
                    });
                }
            }
            (Some(None), Some(_actual_val)) => {
                // Key should be deleted but has a value
                // Check if it's in the possibly-visible window
                if !reference.possibly_visible.contains(key_bytes) {
                    result.violations.push(Violation {
                        key: key_bytes.clone(),
                        kind: ViolationKind::ResurrectedDelete,
                    });
                }
            }
            (Some(None), None) => {
                // Key correctly deleted - OK
            }
            (None, Some(actual_val)) => {
                // Key not in expected state - may be in possibly-visible window
                if !reference.possibly_visible.contains(key_bytes) {
                    result.violations.push(Violation {
                        key: key_bytes.clone(),
                        kind: ViolationKind::UnexpectedKey {
                            actual_value: actual_val,
                        },
                    });
                }
            }
            (None, None) => {
                // Key not touched by any operation - OK
            }
        }
    }

    result.committed_op_count = reference.committed_op_ids.len();
    result.possibly_visible_op_count = reference.possibly_visible_op_ids.len();

    Ok(result)
}

/// Run structural checks on a reopened database.
///
/// Verifies:
/// 1. Repeated reopen succeeds
/// 2. close() after reopen succeeds
/// 3. A follow-up write after reopen succeeds
pub fn structural_checks(
    engine: &KvEngine,
    db_path: &std::path::Path,
    options: &LsmStorageOptions,
) -> Result<(), String> {
    // 1. Close the current instance
    engine
        .close()
        .map_err(|e| format!("close after reopen failed: {e}"))?;

    // 2. Reopen - second open
    let re2 = KvEngine::open(db_path, options.clone())
        .map_err(|e| format!("second reopen failed: {e}"))?;

    // 3. Close again
    re2.close()
        .map_err(|e| format!("close after second reopen failed: {e}"))?;

    // 4. Final reopen + follow-up write
    let re3 = KvEngine::open(db_path, options.clone())
        .map_err(|e| format!("third reopen failed: {e}"))?;

    re3.put(b"__chaos_probe__", b"__chaos_probe__")
        .map_err(|e| format!("follow-up write failed: {e}"))?;

    let val = re3
        .get(b"__chaos_probe__")
        .map_err(|e| format!("follow-up read failed: {e}"))?;

    match val {
        Some(v) if &*v == b"__chaos_probe__" => {}
        Some(v) => {
            return Err(format!(
                "follow-up read got wrong value: expected __chaos_probe__, got {:?}",
                v
            ));
        }
        None => {
            return Err("follow-up read returned None".to_string());
        }
    }

    re3.close()
        .map_err(|e| format!("close after third reopen failed: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaos::control_log::ControlLogWriter;

    #[test]
    fn test_reference_state_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.log");
        std::fs::write(&path, b"").unwrap();
        let reader = control_log::ControlLogReader::open(&path).unwrap();
        let state = ReferenceState::from_control_log(&reader);
        assert!(state.expected.is_empty());
        assert!(state.possibly_visible.is_empty());
    }

    #[test]
    fn test_reference_state_committed_put() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.json");
        let mut writer = ControlLogWriter::new(&path).unwrap();
        let op1 = writer.next_op_id();
        writer
            .write_intent(
                op1,
                OperationKind::Put {
                    key: "hello".into(),
                    value: "hello".into(),
                },
            )
            .unwrap();
        writer
            .write_durability_boundary(
                op1,
                OperationKind::Put {
                    key: "hello".into(),
                    value: "hello".into(),
                },
            )
            .unwrap();
        drop(writer);

        let reader = control_log::ControlLogReader::open(&path).unwrap();
        let state = ReferenceState::from_control_log(&reader);
        assert_eq!(state.expected.len(), 1);
        assert!(state.expected.contains_key(b"hello".as_slice()));
    }

    #[test]
    fn test_reference_state_intent_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.json");
        let mut writer = ControlLogWriter::new(&path).unwrap();
        let op1 = writer.next_op_id();
        writer
            .write_intent(
                op1,
                OperationKind::Put {
                    key: "partial_key".into(),
                    value: "partial_val".into(),
                },
            )
            .unwrap();
        drop(writer);

        let reader = control_log::ControlLogReader::open(&path).unwrap();
        let state = ReferenceState::from_control_log(&reader);
        assert!(state.expected.is_empty());
        assert!(state.possibly_visible.contains(b"partial_key".as_slice()));
    }

    #[test]
    fn test_reconcile_no_violations() {
        // This is a structural test: with an empty engine and empty reference, reconcile passes.
        let universe = BoundedKeyUniverse::new("k", 5);
        let reference = ReferenceState {
            expected: HashMap::new(),
            possibly_visible: BTreeSet::new(),
            committed_op_ids: vec![],
            possibly_visible_op_ids: vec![],
        };
        let dir = tempfile::tempdir().unwrap();
        let engine = KvEngine::open(dir.path(), LsmStorageOptions::default_for_test()).unwrap();
        let result = reconcile(&engine, &universe, &reference).unwrap();
        assert_eq!(result.violations.len(), 0);
        assert_eq!(result.total_keys_checked, 5);
        drop(engine);
    }

    #[test]
    fn test_bounded_key_universe_generation() {
        let universe = BoundedKeyUniverse::new("k", 3);
        let keys = universe.keys();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"k_0000000000");
        assert_eq!(keys[1], b"k_0000000001");
        assert_eq!(keys[2], b"k_0000000002");
    }
}
