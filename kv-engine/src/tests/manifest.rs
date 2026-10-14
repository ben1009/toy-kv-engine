use std::sync::Arc;

use tempfile::tempdir;

use crate::{
    lsm_storage::{CompactionFilterRequest, LsmStorageInner, LsmStorageOptions},
    manifest::{MANIFEST_FORMAT_VERSION, Manifest, ManifestRecord},
};

fn empty_manifest_snapshot(next_sst_id: usize) -> ManifestRecord {
    ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        pitr_memtable_segments: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: MANIFEST_FORMAT_VERSION,
        immutable_file_metadata: vec![],
        pitr_state: Some(crate::pitr::manifest::PitrState::default()),
    }
}

fn legacy_pitr_sealing_state() -> crate::pitr::manifest::PitrState {
    use crate::pitr::manifest::{PersistedPitrConfig, PitrManifestRecord, replay_pitr_records};

    replay_pitr_records([
        PitrManifestRecord::EnableIntent {
            repository_id: [1; 16],
            timeline_id: [2; 16],
            archive_epoch_id: [3; 16],
            config: PersistedPitrConfig {
                archive_interval_ms: 1000,
                max_segment_bytes: 8192,
                max_unarchived_bytes: 8192,
                max_source_spool_bytes: 16384,
            },
        },
        PitrManifestRecord::EnableComplete {
            active_segment_id: 1,
        },
        PitrManifestRecord::SealStarted {
            segment_id: 1,
            successor_segment_id: 2,
            logical_length: 4096,
            immutable_boundary: None,
        },
    ])
    .unwrap()
}

fn untagged_pitr_snapshot(state: &crate::pitr::manifest::PitrState) -> serde_json::Value {
    let mut state = serde_json::to_value(state).unwrap();
    let obligation = state["obligations"]["1"].as_object_mut().unwrap();
    obligation.remove("wal_format").unwrap();
    obligation.remove("immutable_boundary").unwrap();

    state
}

fn write_pitr_snapshot_fixture(
    directory: &std::path::Path,
    version: u32,
    state: serde_json::Value,
    engine_snapshot: bool,
) {
    if engine_snapshot {
        let mut snapshot = serde_json::to_value(empty_manifest_snapshot(1)).unwrap();
        snapshot["Snapshot"]["format_version"] = serde_json::json!(version);
        snapshot["Snapshot"]["pitr_state"] = state;
        std::fs::write(
            directory.join("ENGINE_MANIFEST"),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();
        std::fs::write(directory.join("MANIFEST"), []).unwrap();
    } else {
        let mut records = serde_json::to_vec(&ManifestRecord::FormatVersion(version)).unwrap();
        records.extend_from_slice(
            &serde_json::to_vec(&serde_json::json!({ "Pitr": { "Snapshot": state } })).unwrap(),
        );
        std::fs::write(directory.join("MANIFEST"), records).unwrap();
    }
}

#[test]
fn pitr_snapshot_migration_uses_the_persisted_engine_version() {
    let state = untagged_pitr_snapshot(&legacy_pitr_sealing_state());
    for version in [7, MANIFEST_FORMAT_VERSION] {
        for engine_snapshot in [false, true] {
            let directory = tempdir().unwrap();
            write_pitr_snapshot_fixture(directory.path(), version, state.clone(), engine_snapshot);
            let result = LsmStorageInner::open(&directory, LsmStorageOptions::default_for_test());
            if version == 7 {
                let storage = result.unwrap();
                assert_eq!(
                    storage.pitr_state.lock().obligations[&1].wal_format,
                    crate::pitr::manifest::PitrWalFormat::LegacyV5V6
                );
                drop(storage);
                let (_, records) = Manifest::recover(directory.path().join("MANIFEST")).unwrap();
                assert!(matches!(
                    &records[0],
                    ManifestRecord::Snapshot { format_version, pitr_state: Some(state), .. }
                        if *format_version == MANIFEST_FORMAT_VERSION
                            && state.obligations[&1].wal_format
                                == crate::pitr::manifest::PitrWalFormat::LegacyV5V6
                ));
                drop(
                    LsmStorageInner::open(&directory, LsmStorageOptions::default_for_test())
                        .unwrap(),
                );
            } else {
                let error = result
                    .err()
                    .expect("current snapshots require explicit format markers");
                assert!(format!("{error:#}").contains("missing its WAL format marker"));
            }
        }
    }
}

#[test]
fn pitr_ambiguous_legacy_engine_snapshots_report_upgrade_guidance() {
    use crate::pitr::manifest::{PersistedChainAnchor, PitrManifestRecord, replay_pitr_records};

    let sealed = replay_pitr_records([
        PitrManifestRecord::Snapshot(Box::new(legacy_pitr_sealing_state())),
        PitrManifestRecord::SegmentSealed {
            segment_id: 1,
            segment_anchor: PersistedChainAnchor::Segment {
                segment_id: 1,
                wal_digest: [4; 32],
                seal_digest: [5; 32],
            },
            last_recorded_at: None,
            last_commit_anchor: None,
        },
    ])
    .unwrap();
    for engine_snapshot in [false, true] {
        let directory = tempdir().unwrap();
        write_pitr_snapshot_fixture(
            directory.path(),
            7,
            untagged_pitr_snapshot(&sealed),
            engine_snapshot,
        );
        let error = LsmStorageInner::open(&directory, LsmStorageOptions::default_for_test())
            .err()
            .expect("ambiguous legacy obligations must fail closed");
        assert!(format!("{error:#}").contains("v7-compatible build"));
        assert!(format!("{error:#}").contains("drain seal, archive, and reclaim work"));
    }
}

#[test]
fn pitr_snapshot_cannot_replace_previously_recovered_lifecycle_state() {
    use crate::pitr::manifest::PitrManifestRecord;

    for engine_snapshot in [false, true] {
        let directory = tempdir().unwrap();
        write_pitr_snapshot_fixture(
            directory.path(),
            MANIFEST_FORMAT_VERSION,
            serde_json::to_value(legacy_pitr_sealing_state()).unwrap(),
            engine_snapshot,
        );
        let mut manifest = std::fs::OpenOptions::new()
            .append(true)
            .open(directory.path().join("MANIFEST"))
            .unwrap();
        serde_json::to_writer(
            &mut manifest,
            &ManifestRecord::Pitr(PitrManifestRecord::Snapshot(Box::default())),
        )
        .unwrap();
        drop(manifest);
        let error = LsmStorageInner::open(&directory, LsmStorageOptions::default_for_test())
            .err()
            .expect("a later snapshot must not discard recovered lifecycle state");
        assert!(format!("{error:#}").contains("replace previously recovered lifecycle state"));
    }
}

#[test]
fn manifest_schema_upgrade_preserves_persisted_immutable_checksums() {
    let directory = tempdir().unwrap();
    let engine =
        crate::lsm_storage::KvEngine::open(&directory, LsmStorageOptions::default_for_test())
            .unwrap();
    engine.put(b"key", b"value").unwrap();
    engine.force_flush().unwrap();
    engine.inner.ensure_manifest_v7().unwrap();
    let metadata = engine.inner.state.load().immutable_file_metadata.clone();
    let sst = metadata
        .iter()
        .find(|entry| entry.kind == crate::manifest::ImmutableFileKind::Sst)
        .unwrap();
    let sst_path = directory.path().join(format!("{:05}.sst", sst.file_id));
    engine.close().unwrap();
    drop(engine);

    let snapshot_path = directory.path().join("ENGINE_MANIFEST");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&snapshot_path).unwrap()).unwrap();
    snapshot["Snapshot"]["format_version"] = serde_json::json!(7);
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    // Damage a data-block byte without changing the metadata read by SST open.
    let mut sst_bytes = std::fs::read(&sst_path).unwrap();
    sst_bytes[0] ^= 1;
    std::fs::write(&sst_path, sst_bytes).unwrap();

    let storage = LsmStorageInner::open(&directory, LsmStorageOptions::default_for_test()).unwrap();
    assert_eq!(storage.state.load().immutable_file_metadata, metadata);
    drop(storage);
    let (_, records) = Manifest::recover(directory.path().join("MANIFEST")).unwrap();
    assert!(matches!(
        &records[0],
        ManifestRecord::Snapshot { format_version, immutable_file_metadata, .. }
            if *format_version == MANIFEST_FORMAT_VERSION && *immutable_file_metadata == metadata
    ));
}

#[test]
fn manifest_schema_upgrade_preserves_vlog_identities_with_value_separation_disabled() {
    use crate::{lsm_storage::KvEngine, manifest::ImmutableFileKind, vlog::ValueSeparationOptions};

    for version in [5, 7] {
        for explicitly_disabled in [false, true] {
            let directory = tempdir().unwrap();
            let options = LsmStorageOptions {
                value_separation: Some(ValueSeparationOptions {
                    enabled: true,
                    min_value_size: 16,
                    ..Default::default()
                }),
                ..LsmStorageOptions::default_for_test()
            };
            let engine = KvEngine::open(&directory, options.clone()).unwrap();
            let value = vec![b'v'; 256];
            engine.put(b"key", &value).unwrap();
            engine.force_flush().unwrap();
            engine.inner.ensure_manifest_v7().unwrap();
            let metadata = engine.inner.state.load().immutable_file_metadata.clone();
            assert!(
                metadata
                    .iter()
                    .any(|entry| entry.kind == ImmutableFileKind::Vlog)
            );
            engine.close().unwrap();
            drop(engine);

            let snapshot_path = directory.path().join("ENGINE_MANIFEST");
            let mut snapshot: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&snapshot_path).unwrap()).unwrap();
            snapshot["Snapshot"]["format_version"] = serde_json::json!(version);
            if version == 5 {
                snapshot["Snapshot"]["immutable_file_metadata"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|entry| entry["kind"].as_str() != Some("Vlog"));
            }
            std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();

            let disabled_options = LsmStorageOptions {
                value_separation: explicitly_disabled.then(ValueSeparationOptions::default),
                ..LsmStorageOptions::default_for_test()
            };
            let storage = LsmStorageInner::open(&directory, disabled_options).unwrap();
            assert!(storage.vlog.is_none());
            assert_eq!(storage.state.load().immutable_file_metadata, metadata);
            drop(storage);
            let (_, records) = Manifest::recover(directory.path().join("MANIFEST")).unwrap();
            assert!(matches!(
                &records[0],
                ManifestRecord::Snapshot { format_version, immutable_file_metadata, .. }
                    if *format_version == MANIFEST_FORMAT_VERSION && *immutable_file_metadata == metadata
            ));
            let reopened = KvEngine::open(&directory, options).unwrap();
            assert_eq!(
                reopened.get(b"key").unwrap().as_deref(),
                Some(value.as_slice())
            );
            reopened.close().unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
fn failpoint_manifest_recovery_rewrites_uncertain_stream_on_a_fresh_inode() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    let manifest = Manifest::create(&path).unwrap();
    manifest
        .add_record_when_init(ManifestRecord::FormatVersion(MANIFEST_FORMAT_VERSION))
        .unwrap();
    crate::manifest::set_manifest_sync_failure(&path);
    assert!(
        manifest
            .add_record_when_init(ManifestRecord::NewMemtable(1))
            .is_err()
    );
    let accepted_bytes = std::fs::read(&path).unwrap();
    let old_file = std::fs::File::open(&path).unwrap();
    drop(manifest);

    let (recovered, records) = Manifest::recover(&path).unwrap();
    assert_ne!(
        old_file.metadata().unwrap().ino(),
        std::fs::metadata(&path).unwrap().ino(),
        "recovery must rewrite bytes rather than retry sync on the old inode"
    );
    assert_eq!(std::fs::read(&path).unwrap(), accepted_bytes);
    assert!(matches!(
        records.as_slice(),
        [
            ManifestRecord::FormatVersion(_),
            ManifestRecord::NewMemtable(1)
        ]
    ));
    recovered
        .add_record_when_init(ManifestRecord::NewMemtable(2))
        .unwrap();
    drop(recovered);
    let (_, records) = Manifest::recover(&path).unwrap();
    assert!(matches!(
        records.as_slice(),
        [
            ManifestRecord::FormatVersion(_),
            ManifestRecord::NewMemtable(1),
            ManifestRecord::NewMemtable(2)
        ]
    ));
}

#[cfg(unix)]
#[test]
fn failpoint_manifest_recovery_sync_failure_preserves_original_stream() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("MANIFEST");
    let manifest = Manifest::create(&path).unwrap();
    manifest
        .add_record_when_init(ManifestRecord::FormatVersion(MANIFEST_FORMAT_VERSION))
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let old_file = std::fs::File::open(&path).unwrap();
    drop(manifest);

    crate::manifest::set_manifest_sync_failure(&path);
    assert!(Manifest::recover(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(
        old_file.metadata().unwrap().ino(),
        std::fs::metadata(&path).unwrap().ino()
    );
    let (_, records) = Manifest::recover(&path).unwrap();
    assert!(matches!(
        records.as_slice(),
        [ManifestRecord::FormatVersion(_)]
    ));
}

#[cfg(unix)]
#[test]
fn manifest_recovery_rewrites_snapshot_and_preserves_exact_suffix() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    for pending in [false, true] {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let snapshot_path = dir.path().join("ENGINE_MANIFEST");
        let source_path = if pending {
            snapshot_path.with_extension("tmp")
        } else {
            snapshot_path.clone()
        };
        let snapshot_bytes = serde_json::to_vec_pretty(&empty_manifest_snapshot(4)).unwrap();
        std::fs::write(&source_path, &snapshot_bytes).unwrap();
        std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let old_snapshot = std::fs::File::open(&source_path).unwrap();
        let suffix = if pending {
            vec![]
        } else {
            let mut bytes = serde_json::to_vec_pretty(&ManifestRecord::NewMemtable(4)).unwrap();
            bytes.push(b'\n');
            bytes
        };
        // Missing MANIFEST is also allowed after a pending snapshot handoff.
        if !pending {
            std::fs::write(&manifest_path, &suffix).unwrap();
        }
        std::fs::write(manifest_path.with_extension("recover"), b"stale scratch").unwrap();
        std::fs::write(snapshot_path.with_extension("recover"), b"stale scratch").unwrap();

        let (manifest, records) = Manifest::recover(&manifest_path).unwrap();
        assert_ne!(
            old_snapshot.metadata().unwrap().ino(),
            std::fs::metadata(&snapshot_path).unwrap().ino()
        );
        assert_eq!(std::fs::read(&snapshot_path).unwrap(), snapshot_bytes);
        assert_eq!(
            std::fs::metadata(&snapshot_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
        assert_eq!(std::fs::read(&manifest_path).unwrap(), suffix);
        assert!(matches!(
            &records[0],
            ManifestRecord::Snapshot { next_sst_id: 4, .. }
        ));
        assert_eq!(records.len(), if pending { 1 } else { 2 });
        assert!(!snapshot_path.with_extension("tmp").exists());
        assert!(!snapshot_path.with_extension("recover").exists());
        assert!(!manifest_path.with_extension("recover").exists());
        manifest
            .add_record_when_init(ManifestRecord::NewMemtable(5))
            .unwrap();
        drop(manifest);
        let (_, records) = Manifest::recover(&manifest_path).unwrap();
        assert!(matches!(
            records.last(),
            Some(ManifestRecord::NewMemtable(5))
        ));
    }
}

#[cfg(unix)]
#[test]
fn failpoint_manifest_snapshot_recovery_sync_failure_preserves_authoritative_files() {
    use std::os::unix::fs::MetadataExt;

    for pending in [false, true] {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let snapshot_path = dir.path().join("ENGINE_MANIFEST");
        let source_path = if pending {
            snapshot_path.with_extension("tmp")
        } else {
            snapshot_path.clone()
        };
        let snapshot_bytes = serde_json::to_vec(&empty_manifest_snapshot(4)).unwrap();
        std::fs::write(&source_path, &snapshot_bytes).unwrap();
        std::fs::write(&manifest_path, b"").unwrap();
        let old_snapshot = std::fs::File::open(&source_path).unwrap();
        crate::manifest::set_manifest_sync_failure(&source_path);

        assert!(Manifest::recover(&manifest_path).is_err());
        assert_eq!(std::fs::read(&source_path).unwrap(), snapshot_bytes);
        assert_eq!(
            old_snapshot.metadata().unwrap().ino(),
            std::fs::metadata(&source_path).unwrap().ino()
        );
        assert!(std::fs::read(&manifest_path).unwrap().is_empty());
        if pending {
            assert!(!snapshot_path.exists());
        }
        let (_, records) = Manifest::recover(&manifest_path).unwrap();
        assert!(matches!(
            records.as_slice(),
            [ManifestRecord::Snapshot { next_sst_id: 4, .. }]
        ));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn manifest_recovery_crash_retries_exact_snapshot_and_suffix() {
    if let Some(path) = std::env::var_os("PITR_MANIFEST_RECOVERY_CHILD_PATH") {
        Manifest::recover(path).unwrap();
        unreachable!("child must exit during manifest recovery");
    }
    for (target, pending) in [
        ("MANIFEST", false),
        ("ENGINE_MANIFEST", false),
        ("MANIFEST", true),
        ("ENGINE_MANIFEST.tmp", true),
    ] {
        for boundary in [
            "PITR_PROCESS_KILL_AFTER_MANIFEST_RECOVERY_SYNC",
            "PITR_PROCESS_KILL_AFTER_MANIFEST_RECOVERY_RENAME",
        ] {
            let dir = tempdir().unwrap();
            let manifest_path = dir.path().join("MANIFEST");
            let snapshot_path = dir.path().join("ENGINE_MANIFEST");
            let source_path = if pending {
                snapshot_path.with_extension("tmp")
            } else {
                snapshot_path.clone()
            };
            let snapshot_bytes = serde_json::to_vec(&empty_manifest_snapshot(4)).unwrap();
            std::fs::write(&source_path, &snapshot_bytes).unwrap();
            let old_snapshot_bytes = serde_json::to_vec(&empty_manifest_snapshot(2)).unwrap();
            if pending {
                std::fs::write(&snapshot_path, &old_snapshot_bytes).unwrap();
            }
            let suffix = if pending {
                vec![]
            } else {
                serde_json::to_vec(&ManifestRecord::NewMemtable(4)).unwrap()
            };
            std::fs::write(&manifest_path, &suffix).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("tests::manifest::manifest_recovery_crash_retries_exact_snapshot_and_suffix")
                .arg("--nocapture")
                .env("PITR_MANIFEST_RECOVERY_CHILD_PATH", &manifest_path)
                .env(boundary, dir.path().join(target))
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(137));
            if pending {
                assert_eq!(
                    std::fs::read(&snapshot_path).unwrap(),
                    old_snapshot_bytes,
                    "pending snapshot must not be promoted before MANIFEST is durable"
                );
                assert!(source_path.exists());
            }
            let (_, records) = Manifest::recover(&manifest_path).unwrap();
            assert_eq!(std::fs::read(&snapshot_path).unwrap(), snapshot_bytes);
            assert_eq!(std::fs::read(&manifest_path).unwrap(), suffix);
            assert!(matches!(
                &records[0],
                ManifestRecord::Snapshot { next_sst_id: 4, .. }
            ));
            assert_eq!(records.len(), if pending { 1 } else { 2 });
        }
    }
}

/// A fresh database must write FormatVersion as the first manifest record.
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

/// A CatalogSnapshot with the current format_version must be accepted — this is
/// the happy path after manifest compaction.
#[test]
fn test_accept_snapshot_with_format_version() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let snapshot_path = dir.path().join("ENGINE_MANIFEST");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        pitr_memtable_segments: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: MANIFEST_FORMAT_VERSION,
        immutable_file_metadata: vec![],
        pitr_state: Some(crate::pitr::manifest::PitrState::default()),
    };
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
    assert!(
        result.is_ok(),
        "should accept snapshot with current format_version, got: {:?}",
        result.err()
    );
}

/// If ENGINE_MANIFEST.tmp exists (crash before rename), recovery must
/// rename it to ENGINE_MANIFEST and succeed.
#[test]
fn test_snapshot_tmp_crash_recovery() {
    let dir = tempdir().unwrap();

    let manifest_path = dir.path().join("MANIFEST");
    let tmp_path = dir.path().join("ENGINE_MANIFEST.tmp");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        pitr_memtable_segments: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: MANIFEST_FORMAT_VERSION,
        immutable_file_metadata: vec![],
        pitr_state: Some(crate::pitr::manifest::PitrState::default()),
    };
    std::fs::write(&tmp_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    std::fs::File::create(&manifest_path).unwrap();

    let result = LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test());
    assert!(
        result.is_ok(),
        "should recover from ENGINE_MANIFEST.tmp, got: {:?}",
        result.err()
    );
    // The tmp file should have been renamed.
    assert!(
        !tmp_path.exists(),
        "ENGINE_MANIFEST.tmp should be renamed after recovery"
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

    // Write a CatalogSnapshot record with format_version = 0 (simulates an old
    // snapshot that predates the FormatVersion feature).
    let manifest_path = dir.path().join("MANIFEST");
    let snapshot_path = dir.path().join("ENGINE_MANIFEST");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 1,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        pitr_memtable_segments: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: 0, // old snapshot, no format version
        immutable_file_metadata: vec![],
        pitr_state: None,
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

/// A database written at v6 must be upgraded in place to the current snapshot on open,
/// and the upgrade must preserve PITR state (RFC 023).
#[test]
fn test_manifest_v6_upgrades_to_current_version() {
    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("MANIFEST");

    // Simulate a database written before the v7 bump: the only manifest record
    // is the v6 format marker, with no live immutable files.
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&ManifestRecord::FormatVersion(6)).unwrap(),
    )
    .unwrap();

    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    drop(storage);

    // The upgrade rewrote the manifest as a current snapshot carrying PITR state.
    let (_, records) = Manifest::recover(&manifest_path).unwrap();
    match &records[0] {
        ManifestRecord::Snapshot {
            format_version,
            pitr_state,
            ..
        } => {
            assert_eq!(*format_version, MANIFEST_FORMAT_VERSION);
            assert!(
                *format_version > 6,
                "a v6 manifest must be upgraded to a newer format version"
            );
            assert!(
                pitr_state.is_some(),
                "upgraded snapshot must carry PITR state"
            );
        }
        other => panic!(
            "first record should be the upgraded snapshot, got: {:?}",
            std::mem::discriminant(other)
        ),
    }

    // The upgraded database reopens without a second upgrade.
    let reopened =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    drop(reopened);
}

#[test]
fn test_compaction_filter_recovery_add_remove() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
    let keep_id = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"keep:".to_vec()))
        .unwrap();
    let drop_id = storage
        .add_compaction_filter(CompactionFilterRequest::prefix(b"drop:".to_vec()))
        .unwrap();
    assert!(storage.remove_compaction_filter(drop_id).unwrap());
    drop(storage);

    let reopened =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_test()).unwrap());
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
        ..LsmStorageOptions::default_for_test()
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

    let snapshot_path = dir.path().join("ENGINE_MANIFEST");
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
        ..LsmStorageOptions::default_for_test()
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

#[cfg(target_os = "linux")]
#[test]
fn pitr_manifest_append_crash_replays_record_after_reopen() {
    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    if std::env::var_os("PITR_MANIFEST_CRASH_CHILD_ROOT").is_some() {
        let child_path = std::env::var_os("PITR_MANIFEST_CRASH_CHILD_ROOT").unwrap();
        let manifest = Manifest::create(child_path).unwrap();
        manifest
            .add_record_when_init(ManifestRecord::Pitr(
                crate::pitr::manifest::PitrManifestRecord::SegmentArchived { segment_id: 7 },
            ))
            .unwrap();
        unreachable!("child must exit after manifest append");
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("tests::manifest::pitr_manifest_append_crash_replays_record_after_reopen")
        .arg("--nocapture")
        .env("PITR_MANIFEST_CRASH_CHILD_ROOT", &manifest_path)
        .env("PITR_PROCESS_KILL_AFTER_MANIFEST_APPEND", "1")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(137));
    let (_, records) = Manifest::recover(&manifest_path).unwrap();
    assert!(matches!(
        records.as_slice(),
        [ManifestRecord::Pitr(
            crate::pitr::manifest::PitrManifestRecord::SegmentArchived { segment_id: 7 }
        )]
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn pitr_manifest_snapshot_rename_crash_recovers_snapshot() {
    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("MANIFEST");
    let snapshot = ManifestRecord::Snapshot {
        l0_sstables: vec![],
        levels: vec![],
        range_only_ssts: vec![],
        next_sst_id: 9,
        vlog_references: vec![],
        imm_memtable_ids: vec![],
        pitr_memtable_segments: vec![],
        active_compaction_filters: vec![],
        next_compaction_filter_id: 0,
        format_version: MANIFEST_FORMAT_VERSION,
        immutable_file_metadata: vec![],
        pitr_state: Some(crate::pitr::manifest::PitrState::default()),
    };
    if std::env::var_os("PITR_MANIFEST_SNAPSHOT_CHILD_ROOT").is_some() {
        let child_path = std::env::var_os("PITR_MANIFEST_SNAPSHOT_CHILD_ROOT").unwrap();
        let manifest = Manifest::create(child_path).unwrap();
        manifest.snapshot(snapshot).unwrap();
        unreachable!("child must exit after manifest snapshot rename");
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("tests::manifest::pitr_manifest_snapshot_rename_crash_recovers_snapshot")
        .arg("--nocapture")
        .env("PITR_MANIFEST_SNAPSHOT_CHILD_ROOT", &manifest_path)
        .env("PITR_PROCESS_KILL_AFTER_MANIFEST_SNAPSHOT_RENAME", "1")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(137));
    let (_, records) = Manifest::recover(&manifest_path).unwrap();
    assert!(matches!(
        records.as_slice(),
        [ManifestRecord::Snapshot { next_sst_id: 9, .. }]
    ));
}
