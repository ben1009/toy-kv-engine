use std::{
    num::NonZeroU64,
    path::PathBuf,
    sync::{Arc, Barrier},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, ensure};
use clap::Parser;
use kv_engine::{
    ArchiveIoPriority, BackupOptions, CreateBackupOutcome, EnablePitrOutcome, PersistedPitrConfig,
    PitrOptions, PitrRuntimeOptions, RecoveryPointOutcome,
    lsm_storage::{KvEngine, LsmStorageOptions},
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 10_000)]
    operations: usize,
    #[arg(long, default_value = "/tmp")]
    root: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.operations > 0, "operations must be nonzero");
    for writers in [1usize, 4, 8, 16, 32] {
        run_case(&args, writers, false)?;
        run_case(&args, writers, true)?;
    }
    Ok(())
}

fn run_case(args: &Args, writers: usize, pitr: bool) -> Result<()> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = args.root.join(format!(
        "toy-kv-pitr-perf-{}-{nonce}-{writers}-{}",
        std::process::id(),
        u8::from(pitr)
    ));
    let database = root.join("db");
    let repository = root.join("repository");
    std::fs::create_dir_all(&root)?;
    let engine = KvEngine::open(
        &database,
        LsmStorageOptions {
            enable_wal: true,
            target_sst_size: 512 * 1024 * 1024,
            num_memtable_limit: 128,
            ..LsmStorageOptions::default()
        },
    )?;
    if pitr {
        ensure!(
            matches!(
                engine.create_backup(BackupOptions {
                    repository: repository.clone(),
                    use_hard_links: false,
                })?,
                CreateBackupOutcome::Committed(_)
            ),
            "repository bootstrap backup was not committed"
        );
        ensure!(
            matches!(
                engine.enable_pitr(PitrOptions {
                    repository: repository.clone(),
                    config: PersistedPitrConfig {
                        archive_interval: Duration::from_secs(60),
                        max_segment_bytes: 128 * 1024 * 1024,
                        max_unarchived_bytes: 256 * 1024 * 1024,
                        max_source_spool_bytes: 384 * 1024 * 1024,
                    },
                    runtime: PitrRuntimeOptions {
                        archive_io_bytes_per_second: None,
                        archive_burst_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
                        archive_io_priority: ArchiveIoPriority::Background,
                    },
                })?,
                EnablePitrOutcome::Enabled { .. }
            ),
            "PITR enable did not complete"
        );
    }

    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut threads = Vec::with_capacity(writers);
    for writer in 0..writers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let operations = args.operations;
        threads.push(std::thread::spawn(move || -> Result<()> {
            barrier.wait();
            for operation in (writer..operations).step_by(writers) {
                let key = format!("writer-{writer:02}-key-{operation:08}");
                let value = [operation as u8; 128];
                engine.put(key.as_bytes(), &value)?;
            }
            Ok(())
        }));
    }
    barrier.wait();
    let started = Instant::now();
    for thread in threads {
        thread
            .join()
            .map_err(|_| anyhow::anyhow!("writer panicked"))??;
    }
    let write_elapsed = started.elapsed();
    let catchup_started = Instant::now();
    if pitr {
        ensure!(
            matches!(
                engine.create_recovery_point()?,
                RecoveryPointOutcome::Durable(_)
            ),
            "PITR catch-up point was not durable"
        );
    }
    let catchup_elapsed = catchup_started.elapsed();
    engine.close()?;
    println!(
        "{}",
        serde_json::json!({
            "writers": writers,
            "operations": args.operations,
            "pitr_enabled": pitr,
            "write_seconds": write_elapsed.as_secs_f64(),
            "writes_per_second": args.operations as f64 / write_elapsed.as_secs_f64(),
            "catchup_seconds": pitr.then_some(catchup_elapsed.as_secs_f64()),
        })
    );
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
