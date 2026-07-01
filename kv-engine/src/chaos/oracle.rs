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
    /// Allowed post-crash values for each key, including committed and intent-only outcomes.
    pub allowed_values: HashMap<Vec<u8>, BTreeSet<Option<Bytes>>>,
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

        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        for rec in reader.records() {
            match &rec.kind {
                OperationKind::Put { key, .. } => {
                    all_keys.insert(key.as_bytes().to_vec());
                }
                OperationKind::Delete { key } => {
                    all_keys.insert(key.as_bytes().to_vec());
                }
                OperationKind::DeleteRange { start, end } => {
                    all_keys.insert(start.as_bytes().to_vec());
                    all_keys.insert(end.as_bytes().to_vec());
                }
                OperationKind::WriteBatch { entries } => {
                    for e in entries {
                        all_keys.insert(e.key.as_bytes().to_vec());
                    }
                }
                OperationKind::Flush | OperationKind::FullCompaction | OperationKind::SyncPoint => {
                }
            }
        }

        // Track the latest operation per key.
        // A key is only possibly_visible if its LATEST operation is uncommitted.
        // If the latest operation on a key is committed, the expected state is deterministic.
        let committed_set: std::collections::BTreeSet<u64> =
            committed_ids.iter().copied().collect();
        let mut latest_ops: std::collections::HashMap<Vec<u8>, (u64, bool)> =
            std::collections::HashMap::new();
        for rec in reader.records() {
            let is_committed = committed_set.contains(&rec.op_id);
            let mut update_key = |k: &[u8]| {
                let entry = latest_ops
                    .entry(k.to_vec())
                    .or_insert((rec.op_id, is_committed));
                if rec.op_id > entry.0 {
                    *entry = (rec.op_id, is_committed);
                }
            };
            match &rec.kind {
                OperationKind::Put { key, .. } => update_key(key.as_bytes()),
                OperationKind::Delete { key } => update_key(key.as_bytes()),
                OperationKind::DeleteRange { start, end } => {
                    let start_bytes = start.as_bytes();
                    let end_bytes = end.as_bytes();
                    for key in &all_keys {
                        if key.as_slice() >= start_bytes && key.as_slice() < end_bytes {
                            update_key(key);
                        }
                    }
                }
                OperationKind::WriteBatch { entries } => {
                    for e in entries {
                        update_key(e.key.as_bytes());
                    }
                }
                _ => {}
            }
        }
        for (key, (_op_id, is_committed)) in latest_ops {
            if !is_committed {
                possibly_visible.insert(key);
            }
        }

        let mut allowed_values: HashMap<Vec<u8>, BTreeSet<Option<Bytes>>> = HashMap::new();
        for key in &all_keys {
            let mut values = BTreeSet::new();
            values.insert(None);
            allowed_values.insert(key.clone(), values);
        }
        for (key, value) in &expected {
            if let Some(values) = allowed_values.get_mut(key) {
                values.clear();
                values.insert(value.clone());
            }
        }
        let mut latest_committed_op_by_key: HashMap<Vec<u8>, u64> = HashMap::new();
        for rec in reader.records() {
            if !committed_set.contains(&rec.op_id) {
                continue;
            }
            let mut record_committed = |k: &[u8]| {
                latest_committed_op_by_key.insert(k.to_vec(), rec.op_id);
            };
            match &rec.kind {
                OperationKind::Put { key, .. } => record_committed(key.as_bytes()),
                OperationKind::Delete { key } => record_committed(key.as_bytes()),
                OperationKind::DeleteRange { start, end } => {
                    let start_bytes = start.as_bytes();
                    let end_bytes = end.as_bytes();
                    for key in &all_keys {
                        if key.as_slice() >= start_bytes && key.as_slice() < end_bytes {
                            record_committed(key);
                        }
                    }
                }
                OperationKind::WriteBatch { entries } => {
                    for entry in entries {
                        record_committed(entry.key.as_bytes());
                    }
                }
                OperationKind::Flush | OperationKind::FullCompaction | OperationKind::SyncPoint => {
                }
            }
        }
        for rec in reader.records() {
            if committed_set.contains(&rec.op_id) {
                continue;
            }
            match &rec.kind {
                OperationKind::Put { key, value } => {
                    if latest_committed_op_by_key
                        .get(key.as_bytes())
                        .is_none_or(|latest| rec.op_id > *latest)
                        && let Some(values) = allowed_values.get_mut(key.as_bytes())
                    {
                        values.insert(Some(Bytes::copy_from_slice(value.as_bytes())));
                    }
                }
                OperationKind::Delete { key } => {
                    if latest_committed_op_by_key
                        .get(key.as_bytes())
                        .is_none_or(|latest| rec.op_id > *latest)
                        && let Some(values) = allowed_values.get_mut(key.as_bytes())
                    {
                        values.insert(None);
                    }
                }
                OperationKind::DeleteRange { start, end } => {
                    let start_bytes = start.as_bytes();
                    let end_bytes = end.as_bytes();
                    for key in &all_keys {
                        if key.as_slice() >= start_bytes
                            && key.as_slice() < end_bytes
                            && latest_committed_op_by_key
                                .get(key)
                                .is_none_or(|latest| rec.op_id > *latest)
                            && let Some(values) = allowed_values.get_mut(key)
                        {
                            values.insert(None);
                        }
                    }
                }
                OperationKind::WriteBatch { entries } => {
                    for entry in entries {
                        if latest_committed_op_by_key
                            .get(entry.key.as_bytes())
                            .is_none_or(|latest| rec.op_id > *latest)
                            && let Some(values) = allowed_values.get_mut(entry.key.as_bytes())
                        {
                            let value = entry
                                .value
                                .as_deref()
                                .map(|v| Bytes::copy_from_slice(v.as_bytes()));
                            values.insert(value);
                        }
                    }
                }
                OperationKind::Flush | OperationKind::FullCompaction | OperationKind::SyncPoint => {
                }
            }
        }

        Self {
            expected,
            possibly_visible,
            allowed_values,
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
        let allowed = reference.allowed_values.get(key_bytes);
        let is_allowed = allowed.is_some_and(|values| values.contains(&actual));
        if is_allowed {
            continue;
        }

        let expected = reference.expected.get(key_bytes);
        match (expected, actual) {
            (Some(Some(expected_val)), Some(actual_val)) => {
                result.violations.push(Violation {
                    key: key_bytes.clone(),
                    kind: ViolationKind::WrongValue {
                        expected: expected_val.clone(),
                        actual: actual_val,
                    },
                });
            }
            (Some(Some(expected_val)), None) => {
                result.violations.push(Violation {
                    key: key_bytes.clone(),
                    kind: ViolationKind::LostDurableWrite {
                        expected_value: expected_val.clone(),
                    },
                });
            }
            (Some(None), Some(_actual_val)) => {
                result.violations.push(Violation {
                    key: key_bytes.clone(),
                    kind: ViolationKind::ResurrectedDelete,
                });
            }
            (Some(None), None) => continue,
            (None, Some(actual_val)) => {
                result.violations.push(Violation {
                    key: key_bytes.clone(),
                    kind: ViolationKind::UnexpectedKey {
                        actual_value: actual_val,
                    },
                });
            }
            (None, None) => continue,
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
///
/// Opens fresh instances directly — callers should close any existing
/// engine handle on the same `db_path` before calling this function.
pub fn structural_checks(
    db_path: &std::path::Path,
    options: &LsmStorageOptions,
) -> Result<(), String> {
    // Open a fresh instance for the reopen test (don't touch the borrowed handle).
    let re2 = KvEngine::open(db_path, options.clone())
        .map_err(|e| format!("first reopen failed: {e}"))?;

    // Close and reopen again
    re2.close()
        .map_err(|e| format!("close after first reopen failed: {e}"))?;
    drop(re2);

    // Final reopen + follow-up write
    let re3 = KvEngine::open(db_path, options.clone())
        .map_err(|e| format!("second reopen failed: {e}"))?;

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
        .map_err(|e| format!("close after final reopen failed: {e}"))?;

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
        let allowed = state.allowed_values.get(b"hello".as_slice()).unwrap();
        assert!(allowed.contains(&Some(Bytes::from("hello"))));
        assert!(!allowed.contains(&None));
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
        let allowed = state.allowed_values.get(b"partial_key".as_slice()).unwrap();
        assert!(allowed.contains(&None));
        assert!(allowed.contains(&Some(Bytes::from("partial_val"))));
    }

    #[test]
    fn test_reconcile_no_violations() {
        // This is a structural test: with an empty engine and empty reference, reconcile passes.
        let universe = BoundedKeyUniverse::new("k", 5);
        let reference = ReferenceState {
            expected: HashMap::new(),
            possibly_visible: BTreeSet::new(),
            allowed_values: HashMap::new(),
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
