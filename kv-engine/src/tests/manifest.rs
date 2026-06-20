use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    lsm_storage::{CompactionFilterRequest, LsmStorageInner, LsmStorageOptions},
    manifest::{MANIFEST_FORMAT_VERSION, Manifest, ManifestRecord},
};

/// A fresh database must write FormatVersion as the first manifest record.
#[test]
fn test_fresh_db_writes_format_version() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
    drop(storage);

    // Re-read the manifest and check the first record.
    let manifest_path = dir.path().join("MANIFEST");
    let (_, records) = Manifest::recover(&manifest_path).unwrap();
    assert!(
        !records.is_empty(),
        "manifest should have at least one record"
    );
    match &records[0] {
        ManifestRecord::FormatVersion(v) => {
            assert_eq!(*v, MANIFEST_FORMAT_VERSION, "format version should match");
        }
        other => panic!(
            "first record should be FormatVersion, got: {:?}",
            std::mem::discriminant(other)
        ),
    }
}

/// Opening a pre-MVCC directory (manifest without FormatVersion) must fail.
#[test]
fn test_reject_pre_mvcc_directory() {
    let dir = tempdir().unwrap();

    // Create a manifest that looks like a pre-MVCC directory: the first record
    // is NewMemtable, not FormatVersion.
    let manifest_path = dir.path().join("MANIFEST");
    let manifest = Manifest::create(&manifest_path).unwrap();
    manifest
        .add_record_when_init(ManifestRecord::NewMemtable(1))
        .unwrap();
    drop(manifest);

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(result.is_err(), "should reject pre-MVCC directory");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("pre-MVCC"),
        "error should mention pre-MVCC, got: {}",
        err
    );
}

/// Opening a directory with an unsupported format version must fail.
#[test]
fn test_reject_unsupported_format_version() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let manifest = Manifest::create(&manifest_path).unwrap();
    manifest
        .add_record_when_init(ManifestRecord::FormatVersion(99))
        .unwrap();
    drop(manifest);

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(result.is_err(), "should reject unsupported format version");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("unsupported"),
        "error should mention unsupported, got: {}",
        err
    );
}

/// Opening a directory with an empty MANIFEST (no snapshot) must fail
/// with a distinct error message.
#[test]
fn test_reject_empty_manifest() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    // Create an empty MANIFEST — simulates a crash during init after
    // Manifest::create() but before writing any records.
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(result.is_err(), "should reject empty manifest");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("empty manifest"),
        "error should mention 'empty manifest', got: {}",
        err
    );
}

/// A Snapshot with the current format_version must be accepted — this is
/// the happy path after manifest compaction.
#[test]
fn test_accept_snapshot_with_format_version() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let snapshot_path = dir.path().join("MANIFEST_SNAPSHOT");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: MANIFEST_FORMAT_VERSION,
    };
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(
        result.is_ok(),
        "should accept snapshot with current format_version, got: {:?}",
        result.err()
    );
}

/// If MANIFEST_SNAPSHOT.tmp exists (crash before rename), recovery must
/// rename it to MANIFEST_SNAPSHOT and succeed.
#[test]
fn test_snapshot_tmp_crash_recovery() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let tmp_path = dir.path().join("MANIFEST_SNAPSHOT.tmp");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: MANIFEST_FORMAT_VERSION,
    };
    std::fs::write(&tmp_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(
        result.is_ok(),
        "should recover from MANIFEST_SNAPSHOT.tmp, got: {:?}",
        result.err()
    );
    // The tmp file should have been renamed.
    assert!(
        !tmp_path.exists(),
        "MANIFEST_SNAPSHOT.tmp should be renamed after recovery"
    );
}

/// FormatVersion(0) in the manifest must be rejected (0 means legacy/absent).
#[test]
fn test_reject_format_version_zero() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let manifest = Manifest::create(&manifest_path).unwrap();
    manifest
        .add_record_when_init(ManifestRecord::FormatVersion(0))
        .unwrap();
    drop(manifest);

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(result.is_err(), "should reject FormatVersion(0)");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("unsupported"),
        "error should mention unsupported, got: {}",
        err
    );
}

/// A snapshot with format_version == 0 (old snapshot without the field)
/// must be rejected as pre-MVCC.
#[test]
fn test_reject_snapshot_without_format_version() {
    let dir = tempdir().unwrap();

    // Write a Snapshot record with format_version = 0 (simulates an old
    // snapshot that predates the FormatVersion feature).
    let manifest_path = dir.path().join("MANIFEST");
    let snapshot_path = dir.path().join("MANIFEST_SNAPSHOT");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: 0, // old snapshot, no format version
    };
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();

    // Also create an empty MANIFEST so recovery can open it.
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default());
    assert!(
        result.is_err(),
        "should reject snapshot without format version"
    );
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("pre-MVCC") || err.contains("unsupported"),
        "error should mention pre-MVCC or unsupported, got: {}",
        err
    );
}

#[test]
fn test_compaction_filter_recovery_add_remove() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
    let keep_id = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"keep:".to_vec()))
        .unwrap();
    let drop_id = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"drop:".to_vec()))
        .unwrap();
    assert!(storage.remove_compaction_filter(drop_id).unwrap());
    drop(storage);

    let reopened = Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default()).unwrap());
    let filters = reopened.list_compaction_filters();
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].id, keep_id);
    let next_id = reopened
        .add_compaction_filter(CompactionFilterRequest::prefix(b"later:".to_vec()))
        .unwrap();
    assert!(next_id > drop_id);
}

#[test]
fn test_manifest_snapshot_preserves_compaction_filters_and_next_id() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        manifest_snapshot_threshold_bytes: 1,
        ..LsmStorageOptions::default()
    };
    let storage = Arc::new(LsmStorageInner::open(&dir, options).unwrap());
    let first = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"a:".to_vec()))
        .unwrap();
    let second = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"b:".to_vec()))
        .unwrap();
    let state_lock = storage.state_lock.lock();
    storage.maybe_snapshot_manifest(&state_lock).unwrap();
    drop(state_lock);
    drop(storage);

    let snapshot_path = dir.path().join("MANIFEST_SNAPSHOT");
    let snapshot: ManifestRecord =
        serde_json::from_slice(&std::fs::read(snapshot_path).unwrap()).unwrap();
    match snapshot {
        ManifestRecord::Snapshot {
            active_compaction_filters,
            next_compaction_filter_id,
            ..
        } => {
            assert_eq!(active_compaction_filters.len(), 2);
            assert_eq!(active_compaction_filters[0].id, first);
            assert_eq!(active_compaction_filters[1].id, second);
            assert_eq!(next_compaction_filter_id, second + 1);
        }
        _ => panic!("expected snapshot record"),
    }
}

#[test]
fn test_compaction_filter_replay_after_snapshot() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions {
        manifest_snapshot_threshold_bytes: 1,
        ..LsmStorageOptions::default()
    };
    let storage = Arc::new(LsmStorageInner::open(&dir, options.clone()).unwrap());
    let first = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"a:".to_vec()))
        .unwrap();
    let second = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"b:".to_vec()))
        .unwrap();
    assert!(storage.remove_compaction_filter(first).unwrap());
    drop(storage);

    let reopened = Arc::new(LsmStorageInner::open(&dir, options).unwrap());
    let filters = reopened.list_compaction_filters();
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].id, second);
}
