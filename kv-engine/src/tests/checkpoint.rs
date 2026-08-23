use std::{fs, sync::Arc, thread};
#[cfg(feature = "chaos-testing")]
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use tempfile::tempdir;

#[cfg(feature = "chaos-testing")]
use crate::chaos::failpoint::{self, FailScenario};
use crate::{
    checkpoint::CheckpointOptions,
    lsm_storage::{KvEngine, LsmStorageInner, LsmStorageOptions},
    vlog::ValueSeparationOptions,
};

fn open(path: impl AsRef<std::path::Path>) -> Arc<KvEngine> {
    KvEngine::open(path, LsmStorageOptions::default_for_test()).unwrap()
}

#[cfg(feature = "chaos-testing")]
fn checkpoint_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static CHECKPOINT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    CHECKPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(not(feature = "chaos-testing"))]
fn checkpoint_test_guard() {}

#[test]
fn checkpoint_empty_database_reopens() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);

    let stats = engine.create_checkpoint(&checkpoint_path).unwrap();
    assert_eq!(stats.sst_count, 0);
    assert!(checkpoint_path.join("MANIFEST").exists());
    assert!(checkpoint_path.join("MANIFEST_SNAPSHOT").exists());
    assert!(checkpoint_path.join("CHECKPOINT").exists());
    assert!(!checkpoint_path.join("CHECKPOINT_IN_PROGRESS").exists());

    let checkpoint = open(&checkpoint_path);
    assert_eq!(checkpoint.get(b"missing").unwrap(), None);
}

#[test]
fn checkpoint_flushed_database_reopens() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k1", b"v1").unwrap();
    engine.force_flush().unwrap();

    let stats = engine.create_checkpoint(&checkpoint_path).unwrap();
    assert_eq!(stats.sst_count, 1);

    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"k1").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
}

#[test]
fn checkpoint_flushes_active_memtable() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"active", b"value").unwrap();

    let stats = engine.create_checkpoint(&checkpoint_path).unwrap();
    assert_eq!(stats.sst_count, 1);

    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"active").unwrap(),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn checkpoint_preserves_deletes() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"gone", b"value").unwrap();
    engine.force_flush().unwrap();
    engine.delete(b"gone").unwrap();

    engine.create_checkpoint(&checkpoint_path).unwrap();

    let checkpoint = open(&checkpoint_path);
    assert_eq!(checkpoint.get(b"gone").unwrap(), None);
}

#[test]
fn checkpoint_rejects_existing_target_by_default() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    fs::create_dir(&checkpoint_path).unwrap();

    let err = engine.create_checkpoint(&checkpoint_path).unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
#[cfg(unix)]
fn checkpoint_recovers_stale_target_lock_for_dead_process() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let lock_path = dir.path().join("checkpoint.checkpoint.lock");
    let engine = open(&db_path);
    engine.put(b"k", b"v").unwrap();
    fs::write(&lock_path, "pid=2147483647\n").unwrap();

    engine.create_checkpoint(&checkpoint_path).unwrap();

    assert!(checkpoint_path.join("CHECKPOINT").exists());
    assert!(lock_path.exists());
    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"k").unwrap(),
        Some(Bytes::from_static(b"v"))
    );
}

#[test]
#[cfg(unix)]
fn checkpoint_keeps_live_target_lock() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let lock_path = dir.path().join("checkpoint.checkpoint.lock");
    let engine = open(&db_path);
    let _live_lock = hold_checkpoint_lock(&lock_path);

    let err = engine.create_checkpoint(&checkpoint_path).unwrap_err();

    assert!(
        err.to_string()
            .contains("failed to acquire checkpoint target lock")
    );
    assert!(lock_path.exists());
}

#[test]
fn checkpoint_rejects_overwrite_option_in_phase_1() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);

    let err = engine
        .create_checkpoint_with_options(
            &checkpoint_path,
            CheckpointOptions {
                overwrite: true,
                ..CheckpointOptions::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("overwrite is not supported"));
}

#[test]
fn checkpoint_rejects_target_inside_source_dir() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = db_path.join("checkpoint");
    let engine = open(&db_path);

    let err = engine.create_checkpoint(&checkpoint_path).unwrap_err();
    assert!(
        err.to_string()
            .contains("must not be inside source database")
    );
    assert!(!db_path.join("checkpoint.checkpoint.lock").exists());
}

#[test]
fn checkpoint_rejects_normalized_target_inside_source_dir() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = db_path.join("..").join("db").join("checkpoint");
    let engine = open(&db_path);

    let err = engine.create_checkpoint(&checkpoint_path).unwrap_err();
    assert!(
        err.to_string()
            .contains("must not be inside source database")
    );
    assert!(!db_path.join("checkpoint.checkpoint.lock").exists());
}

#[test]
#[cfg(unix)]
fn checkpoint_rejects_symlink_target_inside_source_dir() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let link_path = dir.path().join("link-to-db");
    let checkpoint_path = link_path.join("checkpoint");
    let engine = open(&db_path);
    std::os::unix::fs::symlink(&db_path, &link_path).unwrap();

    let err = engine.create_checkpoint(&checkpoint_path).unwrap_err();
    assert!(
        err.to_string()
            .contains("must not be inside source database")
    );
    assert!(!db_path.join("checkpoint.checkpoint.lock").exists());
}

#[test]
fn checkpoint_rejects_vlog_enabled_database_in_phase_1() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = KvEngine::open(
        &db_path,
        LsmStorageOptions {
            value_separation: Some(ValueSeparationOptions {
                enabled: true,
                min_value_size: 16,
                ..ValueSeparationOptions::default()
            }),
            ..LsmStorageOptions::default_for_test()
        },
    )
    .unwrap();

    let err = engine.create_checkpoint(&checkpoint_path).unwrap_err();
    assert!(
        err.to_string()
            .contains("does not support value separation")
    );
}

#[test]
fn checkpoint_source_remains_writable() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"before", b"v1").unwrap();
    engine.create_checkpoint(&checkpoint_path).unwrap();
    engine.put(b"after", b"v2").unwrap();

    assert_eq!(
        engine.get(b"after").unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"before").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(checkpoint.get(b"after").unwrap(), None);
}

#[test]
fn checkpoint_restored_database_is_independent() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k", b"source").unwrap();
    engine.create_checkpoint(&checkpoint_path).unwrap();

    let checkpoint = open(&checkpoint_path);
    checkpoint.put(b"k", b"checkpoint").unwrap();

    assert_eq!(
        engine.get(b"k").unwrap(),
        Some(Bytes::from_static(b"source"))
    );
    assert_eq!(
        checkpoint.get(b"k").unwrap(),
        Some(Bytes::from_static(b"checkpoint"))
    );
}

#[test]
fn checkpoint_concurrent_calls_serialize() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let engine = open(&db_path);
    for i in 0..4 {
        engine
            .put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }

    let left_engine = Arc::clone(&engine);
    let left_path = dir.path().join("checkpoint-left");
    let left = thread::spawn(move || left_engine.create_checkpoint(left_path).unwrap());

    let right_engine = Arc::clone(&engine);
    let right_path = dir.path().join("checkpoint-right");
    let right = thread::spawn(move || right_engine.create_checkpoint(right_path).unwrap());

    assert_eq!(left.join().unwrap().sst_count, 1);
    assert_eq!(right.join().unwrap().sst_count, 1);
}

#[test]
#[cfg(feature = "chaos-testing")]
fn checkpoint_blocks_force_flush_until_checkpoint_boundary_finishes() {
    let _test_guard = checkpoint_test_guard();
    let scenario = FailScenario::setup();
    failpoint::cfg("checkpoint.after_in_progress_marker", "pause").unwrap();

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k", b"v").unwrap();

    let checkpoint_engine = Arc::clone(&engine);
    let checkpoint_path_for_thread = checkpoint_path.clone();
    let checkpoint = thread::spawn(move || {
        checkpoint_engine
            .create_checkpoint(checkpoint_path_for_thread)
            .unwrap()
    });

    wait_until(Duration::from_secs(2), || {
        checkpoint_tmp_with_marker(dir.path(), "checkpoint", "CHECKPOINT_IN_PROGRESS").is_some()
    });
    engine.put(b"during", b"checkpoint").unwrap();

    let (tx, rx) = mpsc::channel();
    let flush_engine = Arc::clone(&engine);
    let flush = thread::spawn(move || {
        flush_engine.force_flush().unwrap();
        tx.send(()).unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "force_flush must wait while a checkpoint owns checkpoint_lock"
    );

    failpoint::cfg("checkpoint.after_in_progress_marker", "off").unwrap();
    assert_eq!(checkpoint.join().unwrap().sst_count, 1);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    flush.join().unwrap();

    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"k").unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    assert_eq!(
        checkpoint.get(b"during").unwrap(),
        Some(Bytes::from_static(b"checkpoint"))
    );
    scenario.teardown();
}

#[test]
#[cfg(feature = "chaos-testing")]
fn checkpoint_blocks_drain_flush_async_until_checkpoint_boundary_finishes() {
    let _test_guard = checkpoint_test_guard();
    let scenario = FailScenario::setup();
    failpoint::cfg("checkpoint.after_in_progress_marker", "pause").unwrap();

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k", b"v").unwrap();

    let checkpoint_engine = Arc::clone(&engine);
    let checkpoint_path_for_thread = checkpoint_path.clone();
    let checkpoint = thread::spawn(move || {
        checkpoint_engine
            .create_checkpoint(checkpoint_path_for_thread)
            .unwrap()
    });

    wait_until(Duration::from_secs(2), || {
        checkpoint_tmp_with_marker(dir.path(), "checkpoint", "CHECKPOINT_IN_PROGRESS").is_some()
    });

    let (tx, rx) = mpsc::channel();
    let flush_engine = Arc::clone(&engine);
    let flush = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(flush_engine.drain_flush_async()).unwrap();
        tx.send(()).unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "drain_flush_async must wait while a checkpoint owns checkpoint_lock"
    );

    failpoint::cfg("checkpoint.after_in_progress_marker", "off").unwrap();
    assert_eq!(checkpoint.join().unwrap().sst_count, 1);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    flush.join().unwrap();

    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"k").unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    scenario.teardown();
}

#[test]
#[cfg(feature = "chaos-testing")]
fn checkpoint_pins_ssts_until_copy_finishes_during_compaction() {
    let _test_guard = checkpoint_test_guard();
    let scenario = FailScenario::setup();
    failpoint::cfg("checkpoint.after_sst_pin_before_copy", "pause").unwrap();

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k1", b"v1").unwrap();
    engine.force_flush().unwrap();
    engine.put(b"k2", b"v2").unwrap();
    engine.force_flush().unwrap();
    let pinned_ssts = current_sst_ids(&engine);
    assert!(pinned_ssts.len() >= 2);

    let checkpoint_engine = Arc::clone(&engine);
    let checkpoint_path_for_thread = checkpoint_path.clone();
    let checkpoint = thread::spawn(move || {
        checkpoint_engine
            .create_checkpoint(checkpoint_path_for_thread)
            .unwrap()
    });

    wait_until(Duration::from_secs(2), || {
        pinned_ssts.iter().all(|sst_id| {
            engine
                .inner
                .checkpoint_file_pins
                .lock()
                .is_sst_pinned(*sst_id)
        })
    });

    engine.force_full_compaction().unwrap();
    for sst_id in &pinned_ssts {
        assert!(
            engine.inner.path_of_sst(*sst_id).exists(),
            "pinned SST {sst_id} must remain copyable during checkpoint"
        );
    }

    failpoint::cfg("checkpoint.after_sst_pin_before_copy", "off").unwrap();
    checkpoint.join().unwrap();

    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"k1").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        checkpoint.get(b"k2").unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    scenario.teardown();
}

#[test]
#[cfg(feature = "chaos-testing")]
fn checkpoint_crash_after_in_progress_marker_leaves_target_reusable() {
    let _test_guard = checkpoint_test_guard();
    run_checkpoint_marker_failpoint("checkpoint.after_in_progress_marker");
}

#[test]
#[cfg(feature = "chaos-testing")]
fn checkpoint_crash_after_ready_marker_leaves_target_reusable() {
    let _test_guard = checkpoint_test_guard();
    run_checkpoint_marker_failpoint("checkpoint.after_ready_marker");
}

#[test]
#[cfg(feature = "chaos-testing")]
fn checkpoint_crash_after_checkpoint_marker_leaves_target_reusable() {
    let _test_guard = checkpoint_test_guard();
    run_checkpoint_marker_failpoint("checkpoint.after_checkpoint_marker");
}

#[test]
fn checkpoint_stats_match_file_set() {
    let _test_guard = checkpoint_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k1", b"v1").unwrap();
    engine.force_flush().unwrap();
    engine.put(b"k2", b"v2").unwrap();
    engine.force_flush().unwrap();

    let stats = engine
        .create_checkpoint_with_options(
            &checkpoint_path,
            CheckpointOptions {
                use_hard_links: false,
                ..CheckpointOptions::default()
            },
        )
        .unwrap();
    let sst_files = fs::read_dir(&checkpoint_path)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "sst")
                .then_some(entry.path())
        })
        .collect::<Vec<_>>();
    let total_bytes: u64 = sst_files
        .iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum();

    assert_eq!(stats.sst_count, sst_files.len());
    assert_eq!(stats.files_copied, sst_files.len());
    assert_eq!(stats.files_hard_linked, 0);
    assert_eq!(stats.bytes_copied, total_bytes);
    assert_eq!(stats.bytes_referenced, 0);

    for path in sst_files {
        let id = path
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .parse::<usize>()
            .unwrap();
        assert!(path.ends_with(LsmStorageInner::path_of_sst_static(&checkpoint_path, id)));
    }
}

#[cfg(feature = "chaos-testing")]
fn run_checkpoint_marker_failpoint(failpoint_name: &str) {
    let scenario = FailScenario::setup();
    failpoint::cfg(failpoint_name, "panic").unwrap();

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db");
    let checkpoint_path = dir.path().join("checkpoint");
    let engine = open(&db_path);
    engine.put(b"k", b"v").unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.create_checkpoint(&checkpoint_path).unwrap();
    }));
    assert!(result.is_err(), "{failpoint_name} did not fire");
    failpoint::cfg(failpoint_name, "off").unwrap();
    assert!(
        !checkpoint_path.exists(),
        "failed checkpoint attempt must not publish target"
    );
    assert_eq!(
        checkpoint_tmp_count(dir.path(), "checkpoint"),
        1,
        "failed checkpoint attempt should leave one temp dir before retry cleanup"
    );

    engine.create_checkpoint(&checkpoint_path).unwrap();

    assert!(checkpoint_path.join("CHECKPOINT").exists());
    assert!(!checkpoint_path.join("CHECKPOINT_IN_PROGRESS").exists());
    assert_eq!(
        checkpoint_tmp_count(dir.path(), "checkpoint"),
        0,
        "successful retry should clean stale checkpoint temp dirs"
    );
    let checkpoint = open(&checkpoint_path);
    assert_eq!(
        checkpoint.get(b"k").unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    scenario.teardown();
}

#[cfg(feature = "chaos-testing")]
fn checkpoint_tmp_with_marker(
    parent: &std::path::Path,
    target_name: &str,
    marker: &str,
) -> Option<std::path::PathBuf> {
    fs::read_dir(parent).ok()?.find_map(|entry| {
        let path = entry.ok()?.path();
        let name = path.file_name()?.to_string_lossy();
        (name.starts_with(&format!("{target_name}.checkpoint-")) && path.join(marker).exists())
            .then_some(path)
    })
}

#[cfg(feature = "chaos-testing")]
fn checkpoint_tmp_count(parent: &std::path::Path, target_name: &str) -> usize {
    fs::read_dir(parent)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(&format!("{target_name}.checkpoint-")) && name.ends_with(".tmp")
        })
        .count()
}

#[cfg(feature = "chaos-testing")]
fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("condition was not met within {timeout:?}");
}

#[cfg(feature = "chaos-testing")]
fn current_sst_ids(engine: &KvEngine) -> Vec<usize> {
    let mut sst_ids = engine
        .inner
        .state
        .load()
        .sstables
        .keys()
        .copied()
        .collect::<Vec<_>>();
    sst_ids.sort_unstable();
    sst_ids
}

#[cfg(unix)]
fn hold_checkpoint_lock(path: &std::path::Path) -> fs::File {
    use std::os::fd::AsRawFd;

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0);
    file
}
