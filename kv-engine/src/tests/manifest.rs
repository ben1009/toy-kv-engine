use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    manifest::{MANIFEST_FORMAT_VERSION, Manifest, ManifestRecord},
};

/// A fresh database must write FormatVersion(2) as the first manifest record.
#[test]
fn test_fresh_db_writes_format_version() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
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
            assert_eq!(*v, MANIFEST_FORMAT_VERSION, "format version should be 2");
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

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
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

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
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

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
    assert!(result.is_err(), "should reject empty manifest");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("empty manifest"),
        "error should mention 'empty manifest', got: {}",
        err
    );
}

/// A Snapshot with format_version == 2 must be accepted — this is the
/// happy path after manifest compaction.
#[test]
fn test_accept_snapshot_with_format_version() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let snapshot_path = dir.path().join("MANIFEST_SNAPSHOT");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        format_version: MANIFEST_FORMAT_VERSION,
    };
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
    assert!(
        result.is_ok(),
        "should accept snapshot with format_version=2, got: {:?}",
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
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        format_version: MANIFEST_FORMAT_VERSION,
    };
    std::fs::write(&tmp_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
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

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
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
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        format_version: 0, // old snapshot, no format version
    };
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();

    // Also create an empty MANIFEST so recovery can open it.
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
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
