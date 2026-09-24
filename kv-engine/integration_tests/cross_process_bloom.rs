#![cfg(feature = "chaos-testing")]

use std::process::Command;

use bytes::Bytes;
use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};

#[test]
fn cross_process_reopen_preserves_mvcc_point_gets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("db");

    let status = Command::new(env!("CARGO_BIN_EXE_bloom-crossproc-writer"))
        .arg(&db_path)
        .status()
        .expect("spawn cross-process writer");
    assert!(status.success(), "writer exited with {status}");

    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.manifest_snapshot_threshold_bytes = 256;

    let engine = KvEngine::open(&db_path, opts).expect("reopen db");
    assert_eq!(
        engine.get(b"k_0000000000").unwrap(),
        Some(Bytes::from("v_0"))
    );
    assert_eq!(
        engine.get(b"k_0000000071").unwrap(),
        Some(Bytes::from("y".repeat(1000)))
    );
    assert_eq!(
        engine.get(b"k_0000000098").unwrap(),
        Some(Bytes::from("v_98"))
    );
}
