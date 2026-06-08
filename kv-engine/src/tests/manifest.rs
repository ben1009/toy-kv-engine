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
