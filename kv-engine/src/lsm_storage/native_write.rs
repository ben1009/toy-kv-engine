//! Native async waits for ordinary, non-serializable v4 point commits.
//!
//! The caller prepares owned entries; an owned task finishes admission,
//! durability and publication even when the caller drops its response future.
//! Memtable leases exclude freeze and GC CAS until publication is finished.
//! Runtime cancellation after admission poisons the unknown commit, while a
//! ready commit that a predecessor already made visible remains successful.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;

use super::{AdmissionGuard, LsmStorageInner};
use crate::mvcc::{BatchEntryKind, LsmMvccInner};

struct AdmittedCommit<'a> {
    mvcc: &'a LsmMvccInner,
    timestamp: u64,
    finished: bool,
}

impl Drop for AdmittedCommit<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.mvcc.cancel_admitted_commit(self.timestamp);
        }
    }
}

impl LsmStorageInner {
    pub(super) fn uses_native_async_writes(&self) -> bool {
        self.selects_parallel_wal_io() && self.mvcc.is_some() && !self.options.serializable
    }

    pub(super) async fn write_entries_native(
        self: &Arc<Self>,
        entries: Vec<(Bytes, Bytes, BatchEntryKind)>,
        shared_publish_bytes: bool,
        admission: AdmissionGuard,
    ) -> Result<()> {
        let admission = Arc::new(admission);
        if entries.is_empty() {
            return Ok(());
        }
        let mvcc = self.mvcc.as_ref().context("native writes require MVCC")?;
        let mut encoded_size = None;
        let mut retries = 0;

        loop {
            // Queue native preparers fairly. Unrestricted try-lock retries
            // repeatedly discard prepared buffers and can starve individual
            // tasks, even when aggregate throughput is high.
            let preparation = mvcc.native_preparation_permit().await?;
            let lease = self.active_memtable_lock.read_async().await;
            let memtable = self.state.load().memtable.clone();
            if !memtable.uses_parallel_wal() {
                // PITR may temporarily install a v5/v6 WAL. Preserve its
                // existing I/O and seal protocol on the blocking executor.
                drop(lease);
                drop(preparation);
                let inner = Arc::clone(self);
                return self
                    .blocking
                    .run_result(move || {
                        let _admission = admission;
                        inner.mvcc_write_batch(&entries)
                    })
                    .await;
            }
            let entries_size = match encoded_size {
                Some(size) => size,
                None => {
                    let size = LsmMvccInner::point_batch_wal_size(&entries)?;
                    encoded_size = Some(size);
                    size
                }
            };
            let buffer = memtable.prepare_wal_buffer_async(entries_size).await?;
            #[cfg(feature = "bench")]
            let started = std::time::Instant::now();
            let result = mvcc.write_batch_wal_only_prepared(
                &entries,
                &memtable,
                shared_publish_bytes,
                buffer,
            );
            drop(preparation);
            #[cfg(feature = "bench")]
            self.write_profile
                .record_mvcc_wal_only_ns(started.elapsed().as_nanos() as u64);

            let (timestamp, data, ticket) = match result {
                Ok(Some(prepared)) => prepared,
                Ok(None) => {
                    drop(lease);
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    drop(lease);
                    if !crate::wal::Wal::is_retryable_full_error(&error) {
                        return Err(error);
                    }
                    let inner = Arc::clone(self);
                    let rotation_admission = Arc::clone(&admission);
                    retries = self
                        .blocking
                        .run_result(move || {
                            let _admission = rotation_admission;
                            inner.retry_after_wal_full(error, &memtable, &mut retries)?;

                            Ok::<_, anyhow::Error>(retries)
                        })
                        .await?;
                    continue;
                }
            };

            let mut commit = AdmittedCommit {
                mvcc,
                timestamp,
                finished: false,
            };
            #[cfg(test)]
            self.pause_native_write(NativeWriteStage::Admitted, timestamp)
                .await;
            memtable.commit_wal_ticket_async(ticket).await?;
            #[cfg(test)]
            self.pause_native_write(NativeWriteStage::Durable, timestamp)
                .await;
            Self::publish_deferred_batch_or_poison(&memtable, data, mvcc, timestamp)?;
            mvcc.publish_commit_ts_async(timestamp).await?;
            commit.finished = true;
            drop(commit);
            drop(lease);

            // Freeze may create files, drain writers and join WAL threads.
            // Keep it off the executor and keep close waiting for its owner.
            if self.state.load().memtable.approximate_size() >= self.options.target_sst_size {
                let inner = Arc::clone(self);
                return self
                    .blocking
                    .run_result(move || {
                        let _admission = admission;
                        inner.try_freeze_memtable()
                    })
                    .await;
            }

            return Ok(());
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeWriteStage {
    Admitted,
    Durable,
}

#[cfg(test)]
pub(crate) struct NativeWritePause {
    pub(crate) stage: NativeWriteStage,
    pub(crate) reached: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
    pub(crate) timestamp: std::sync::atomic::AtomicU64,
    fired: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl NativeWritePause {
    pub(crate) fn new(stage: NativeWriteStage) -> Arc<Self> {
        Arc::new(Self {
            stage,
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            timestamp: std::sync::atomic::AtomicU64::new(0),
            fired: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
impl LsmStorageInner {
    async fn pause_native_write(&self, stage: NativeWriteStage, timestamp: u64) {
        let pause = self.native_write_pause.lock().clone();
        if let Some(pause) = pause
            && pause.stage == stage
            && !pause.fired.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            pause
                .timestamp
                .store(timestamp, std::sync::atomic::Ordering::Release);
            pause.reached.notify_one();
            pause.release.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::atomic::Ordering, time::Duration};

    use crate::{
        compact::CompactionOptions,
        lsm_storage::{KvEngine, LsmStorageOptions, WriteBatchRecord},
        wal::WalIoMode,
    };

    fn options() -> LsmStorageOptions {
        LsmStorageOptions {
            enable_wal: true,
            target_sst_size: 1 << 30,
            compaction_options: CompactionOptions::NoCompaction,
            ..LsmStorageOptions::default_for_test()
        }
    }

    fn open(path: &std::path::Path) -> Arc<KvEngine> {
        KvEngine::open_with_wal_io_mode(path, options(), WalIoMode::Parallel).unwrap()
    }

    async fn until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition must become true");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_queued_preparation_does_not_pin_the_old_memtable() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        engine.put_async(b"first", b"value").await.unwrap();
        let old_memtable = engine.inner.state.load().memtable.clone();
        let mvcc = engine.inner.mvcc.as_ref().unwrap();
        let preparation = mvcc.native_preparation_permit().await.unwrap();
        let writer_engine = Arc::clone(&engine);
        let writer =
            tokio::spawn(async move { writer_engine.put_async(b"queued", b"value").await });
        until(|| {
            engine
                .inner
                .lifecycle
                .0
                .active_writes
                .load(Ordering::Acquire)
                == 1
        })
        .await;
        tokio::task::yield_now().await;
        assert!(!writer.is_finished());
        assert_eq!(mvcc.latest_commit_ts(), 1);

        tokio::time::timeout(Duration::from_secs(5), engine.force_flush_async())
            .await
            .expect("a queued preparation must not hold a memtable lease")
            .unwrap();
        assert_ne!(engine.inner.state.load().memtable.id(), old_memtable.id());
        assert_eq!(old_memtable.wal_batch_count(), Some(1));
        drop(preparation);
        writer.await.unwrap().unwrap();
        assert_eq!(
            engine.inner.state.load().memtable.wal_batch_count(),
            Some(1)
        );
        assert_eq!(mvcc.latest_commit_ts(), 2);
        engine.close_async().await.unwrap();
        drop(engine);

        let engine = open(directory.path());
        for key in [b"first".as_slice(), b"queued"] {
            assert_eq!(
                engine.get(key).unwrap().as_deref(),
                Some(b"value".as_slice())
            );
        }
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_point_commits_progress_without_blocking_slots_and_recover() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        let blocked_slots = engine.inner.blocking.hold_all_slots_for_test();
        let mut writers = tokio::task::JoinSet::new();
        for writer in 0..16 {
            let engine = Arc::clone(&engine);
            writers.spawn(async move {
                for batch in 0..8 {
                    let records = (0..4)
                        .map(|record| {
                            WriteBatchRecord::Put(
                                format!("{writer:02}/{batch:02}/{record}").into_bytes(),
                                vec![writer as u8; 1024],
                            )
                        })
                        .collect::<Vec<_>>();
                    engine.write_batch_async(&records).await.unwrap();
                }
            });
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = writers.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("a single executor thread must keep native commits moving");
        assert_eq!(
            engine.inner.state.load().memtable.wal_batch_count(),
            Some(128)
        );
        for writer in 0..16 {
            let key = format!("{writer:02}/07/3");
            assert_eq!(
                engine.get(key.as_bytes()).unwrap(),
                Some(Bytes::from(vec![writer as u8; 1024]))
            );
        }
        drop(blocked_slots);
        engine.close_async().await.unwrap();
        drop(engine);
        let engine = open(directory.path());
        for writer in 0..16 {
            let key = format!("{writer:02}/07/3");
            assert_eq!(
                engine.get(key.as_bytes()).unwrap(),
                Some(Bytes::from(vec![writer as u8; 1024]))
            );
        }
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_cancelled_caller_leaves_an_owned_commit_and_close_drains_it() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        let pause = NativeWritePause::new(NativeWriteStage::Admitted);
        *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
        let writer_engine = Arc::clone(&engine);
        let writer =
            tokio::spawn(async move { writer_engine.put_async(b"cancelled", b"value").await });
        pause.reached.notified().await;
        assert_eq!(engine.get(b"cancelled").unwrap(), None);
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        let close_engine = Arc::clone(&engine);
        let close = tokio::spawn(async move { close_engine.close_async().await });
        until(|| engine.inner.lifecycle.ensure_open().is_err()).await;
        assert!(!close.is_finished());
        assert_eq!(
            engine.inner.state.load().memtable.parallel_wal_is_closed(),
            Some(false)
        );
        pause.release.notify_one();
        close.await.unwrap().unwrap();
        assert_eq!(
            engine.get(b"cancelled").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        drop(engine);
        let engine = open(directory.path());
        assert_eq!(
            engine.get(b"cancelled").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_detached_commit_keeps_engine_alive_when_the_last_caller_drops_it() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        let weak = Arc::downgrade(&engine);
        let pause = NativeWritePause::new(NativeWriteStage::Admitted);
        *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
        let writer_engine = Arc::clone(&engine);
        let writer = tokio::spawn(async move { writer_engine.put_async(b"drop", b"value").await });
        pause.reached.notified().await;
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        drop(engine);
        assert!(
            weak.upgrade().is_some(),
            "the owned commit must retain the public owner until its admission settles"
        );
        pause.release.notify_one();
        until(|| weak.upgrade().is_none()).await;
        let engine = open(directory.path());
        assert_eq!(
            engine.get(b"drop").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_checkpoint_waits_for_pending_publication_before_freezing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db");
        let target = directory.path().join("checkpoint");
        let engine = open(&path);
        let old_memtable = engine.inner.state.load().memtable.clone();
        let pause = NativeWritePause::new(NativeWriteStage::Durable);
        *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
        let writer_engine = Arc::clone(&engine);
        let writer =
            tokio::spawn(async move { writer_engine.put_async(b"checkpoint", b"value").await });
        pause.reached.notified().await;
        let checkpoint_engine = Arc::clone(&engine);
        let checkpoint_target = target.clone();
        let checkpoint = tokio::spawn(async move {
            checkpoint_engine
                .create_checkpoint_async(checkpoint_target)
                .await
        });
        until(|| {
            engine
                .inner
                .active_memtable_lock
                .exclusive_pending_for_test()
        })
        .await;
        assert!(!checkpoint.is_finished());
        assert!(engine.inner.state.load().imm_memtables.is_empty());
        assert_eq!(old_memtable.parallel_wal_is_closed(), Some(false));
        pause.release.notify_one();
        writer.await.unwrap().unwrap();
        checkpoint.await.unwrap().unwrap();
        assert_eq!(old_memtable.parallel_wal_is_closed(), Some(true));
        let copy = open(&target);
        assert_eq!(
            copy.get(b"checkpoint").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        copy.close_async().await.unwrap();
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_checkpoint_rejects_hidden_entries_after_publication_poison() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db");
        let target = directory.path().join("checkpoint");
        let engine = open(&path);
        engine.put_async(b"prefix", b"success").await.unwrap();
        let memtable = engine.inner.state.load().memtable.clone();
        let pause = NativeWritePause::new(NativeWriteStage::Durable);
        *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
        let admission = engine.inner.lifecycle.admit_write().unwrap();
        let task_inner = Arc::clone(&engine.inner);
        let predecessor = tokio::spawn(async move {
            task_inner
                .write_entries_native(
                    vec![(
                        Bytes::from_static(b"aborted"),
                        Bytes::from_static(b"unknown"),
                        BatchEntryKind::PutRaw,
                    )],
                    false,
                    admission,
                )
                .await
        });
        pause.reached.notified().await;
        assert_eq!(pause.timestamp.load(Ordering::Acquire), 2);
        let writer_engine = Arc::clone(&engine);
        let successor =
            tokio::spawn(async move { writer_engine.put_async(b"hidden", b"value").await });
        until(|| memtable.get_versioned_raw(b"hidden", 3).is_some()).await;
        assert_eq!(engine.get(b"hidden").unwrap(), None);

        let checkpoint_engine = Arc::clone(&engine);
        let checkpoint_target = target.clone();
        let checkpoint = tokio::spawn(async move {
            checkpoint_engine
                .create_checkpoint_async(checkpoint_target)
                .await
        });
        until(|| {
            engine
                .inner
                .active_memtable_lock
                .exclusive_pending_for_test()
        })
        .await;
        assert!(!checkpoint.is_finished());
        predecessor.abort();
        assert!(predecessor.await.unwrap_err().is_cancelled());
        assert!(successor.await.unwrap().is_err());
        let error = checkpoint.await.unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("requires recovery"));
        assert!(!target.exists());
        assert_eq!(engine.inner.mvcc.as_ref().unwrap().latest_commit_ts(), 1);
        assert_eq!(engine.get(b"hidden").unwrap(), None);
        assert!(engine.force_flush_async().await.is_err());
        assert_eq!(engine.inner.state.load().memtable.id(), memtable.id());
        assert!(engine.inner.state.load().imm_memtables.is_empty());
        assert!(engine.inner.state.load().sstables.is_empty());
        assert_eq!(memtable.parallel_wal_is_closed(), Some(false));

        assert!(engine.close_async().await.is_err());
        assert_eq!(memtable.parallel_wal_is_closed(), Some(true));
        drop(memtable);
        drop(engine);
        // Recovery may replay unacknowledged commits, but must retain the
        // complete WAL history rather than a checkpoint missing its predecessor.
        let recovered = open(&path);
        for (key, value) in [
            (b"prefix".as_slice(), b"success".as_slice()),
            (b"aborted".as_slice(), b"unknown".as_slice()),
            (b"hidden".as_slice(), b"value".as_slice()),
        ] {
            assert_eq!(recovered.get(key).unwrap().as_deref(), Some(value));
        }
        recovered.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_checkpoint_rejects_publication_poison_with_an_empty_memtable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db");
        let target = directory.path().join("checkpoint");
        let engine = open(&path);
        let pause = NativeWritePause::new(NativeWriteStage::Durable);
        *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
        let admission = engine.inner.lifecycle.admit_write().unwrap();
        let task_inner = Arc::clone(&engine.inner);
        let writer = tokio::spawn(async move {
            task_inner
                .write_entries_native(
                    vec![(
                        Bytes::from_static(b"aborted"),
                        Bytes::from_static(b"unknown"),
                        BatchEntryKind::PutRaw,
                    )],
                    false,
                    admission,
                )
                .await
        });
        pause.reached.notified().await;
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert!(engine.inner.state.load().memtable.is_empty());
        let error = engine.create_checkpoint_async(&target).await.unwrap_err();
        assert!(format!("{error:#}").contains("requires recovery"));
        assert!(!target.exists());
        assert!(engine.close_async().await.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_owned_task_cancellation_poison_preserves_prefix_and_drains_wal() {
        for stage in [NativeWriteStage::Admitted, NativeWriteStage::Durable] {
            let directory = tempfile::tempdir().unwrap();
            let engine = open(directory.path());
            engine.put_async(b"prefix", b"success").await.unwrap();
            let pause = NativeWritePause::new(stage);
            *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
            let admission = engine.inner.lifecycle.admit_write().unwrap();
            let task_inner = Arc::clone(&engine.inner);
            let task = tokio::spawn(async move {
                task_inner
                    .write_entries_native(
                        vec![(
                            Bytes::from_static(b"aborted"),
                            Bytes::from_static(b"unknown"),
                            BatchEntryKind::PutRaw,
                        )],
                        false,
                        admission,
                    )
                    .await
            });
            pause.reached.notified().await;
            assert_eq!(pause.timestamp.load(Ordering::Acquire), 2);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(
                engine.get(b"prefix").unwrap().as_deref(),
                Some(b"success".as_slice())
            );
            assert_eq!(engine.get(b"aborted").unwrap(), None);
            assert!(engine.put_async(b"later", b"rejected").await.is_err());
            let error = engine.close_async().await.unwrap_err();
            assert!(error.to_string().contains("requires recovery"));
            assert!(engine.inner.lifecycle.is_closed());
            assert_eq!(
                engine.inner.state.load().memtable.parallel_wal_is_closed(),
                Some(true)
            );
            assert!(
                engine.close_async().await.is_err(),
                "terminal close error must survive a second caller"
            );
            drop(engine);
            let engine = open(directory.path());
            assert_eq!(
                engine.get(b"prefix").unwrap().as_deref(),
                Some(b"success".as_slice())
            );
            assert_eq!(
                engine.get(b"aborted").unwrap().as_deref(),
                Some(b"unknown".as_slice())
            );
            assert_eq!(engine.get(b"later").unwrap(), None);
            engine.close_async().await.unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_full_wal_rotates_without_consuming_a_wal_ticket_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        let old_memtable = engine.inner.state.load().memtable.clone();
        old_memtable
            .set_parallel_wal_file_size_limit_for_test(1 << 20)
            .unwrap();
        let mut writers = tokio::task::JoinSet::new();
        for index in 0..12 {
            let engine = Arc::clone(&engine);
            writers.spawn(async move {
                let records = [
                    WriteBatchRecord::Put(
                        format!("rotate/{index:02}/a").into_bytes(),
                        vec![index as u8; 60_000],
                    ),
                    WriteBatchRecord::Put(
                        format!("rotate/{index:02}/b").into_bytes(),
                        vec![index as u8; 60_000],
                    ),
                ];
                engine.write_batch_async(&records).await.unwrap();
            });
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = writers.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("WAL-full retries must release every old-memtable lease before rotating");
        assert_ne!(engine.inner.state.load().memtable.id(), old_memtable.id());
        assert_eq!(old_memtable.parallel_wal_is_closed(), Some(true));
        assert_eq!(old_memtable.wal_batch_count(), Some(8));
        assert_eq!(
            engine.inner.state.load().memtable.wal_batch_count(),
            Some(4)
        );
        engine.close_async().await.unwrap();
        drop(engine);
        let engine = open(directory.path());
        for index in 0..12 {
            for suffix in ["a", "b"] {
                let key = format!("rotate/{index:02}/{suffix}");
                assert_eq!(
                    engine.get(key.as_bytes()).unwrap(),
                    Some(Bytes::from(vec![index as u8; 60_000]))
                );
            }
        }
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_invalid_batch_does_not_leave_a_timestamp_or_wal_ticket_hole() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        assert!(
            engine
                .put_async(b"invalid", &vec![1; u16::MAX as usize])
                .await
                .is_err()
        );
        assert!(
            engine
                .put_async(b"invalid", crate::mvcc::TOMBSTONE_VALUE)
                .await
                .is_err()
        );
        engine.write_batch_async::<Vec<u8>>(&[]).await.unwrap();
        assert_eq!(
            engine.inner.state.load().memtable.wal_batch_count(),
            Some(0)
        );
        engine.put_async(b"valid", b"value").await.unwrap();
        assert_eq!(engine.inner.mvcc.as_ref().unwrap().latest_commit_ts(), 1);
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_buffer_backpressure_does_not_block_the_commit_sequencer() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        let memtable = engine.inner.state.load().memtable.clone();
        let mvcc = engine.inner.mvcc.as_ref().unwrap();
        let mut prepared = Vec::new();
        // Normal residency is 64 MiB; each pooled DirectBuf owns 256 KiB.
        for _ in 0..256 {
            prepared.push(memtable.prepare_wal_buffer_async(0).await.unwrap());
        }
        let sync_engine = Arc::clone(&engine);
        let sync_writer = tokio::task::spawn_blocking(move || sync_engine.put(b"sync", b"value"));
        until(|| mvcc.write_lock.try_lock().is_none()).await;
        let buffer = prepared.pop().unwrap();
        let attempt = mvcc
            .write_batch_wal_only_prepared(
                &[(
                    Bytes::from_static(b"native"),
                    Bytes::from_static(b"value"),
                    BatchEntryKind::PutRaw,
                )],
                &memtable,
                false,
                buffer,
            )
            .unwrap();
        assert!(
            attempt.is_none(),
            "a busy sequencer must release memory without assigning a timestamp"
        );
        tokio::time::timeout(Duration::from_secs(5), sync_writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(prepared);
        engine.put_async(b"native", b"value").await.unwrap();
        assert_eq!(mvcc.latest_commit_ts(), 2);
        assert_eq!(memtable.wal_batch_count(), Some(2));
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_mixed_sync_writers_and_value_separation_survive_automatic_freeze() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = options();
        options.target_sst_size = 64 * 1024;
        options.value_separation = Some(crate::vlog::ValueSeparationOptions {
            enabled: true,
            min_value_size: 64,
            ..Default::default()
        });
        let engine =
            KvEngine::open_with_wal_io_mode(directory.path(), options.clone(), WalIoMode::Parallel)
                .unwrap();
        let first_memtable = engine.inner.state.load().memtable.clone();
        let mut writers = tokio::task::JoinSet::new();
        for writer in 0..12 {
            let engine = Arc::clone(&engine);
            if writer < 4 {
                writers.spawn_blocking(move || {
                    for batch in 0..8 {
                        engine.write_batch(&[WriteBatchRecord::Put(
                            format!("mixed/{writer:02}/{batch}").into_bytes(),
                            vec![writer as u8; 1024],
                        )])?;
                    }

                    Ok::<_, anyhow::Error>(())
                });
            } else {
                writers.spawn(async move {
                    for batch in 0..8 {
                        engine
                            .write_batch_async(&[WriteBatchRecord::Put(
                                format!("mixed/{writer:02}/{batch}").into_bytes(),
                                vec![writer as u8; 1024],
                            )])
                            .await?;
                    }

                    Ok::<_, anyhow::Error>(())
                });
            }
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = writers.join_next().await {
                result.unwrap().unwrap();
            }
        })
        .await
        .expect("mixed sync/native writers must finish through automatic rotation");
        assert_ne!(engine.inner.state.load().memtable.id(), first_memtable.id());
        assert_eq!(first_memtable.parallel_wal_is_closed(), Some(true));
        engine.drain_flush_async().await.unwrap();
        assert!(engine.vlog_stats().unwrap().vlog_file_count > 0);
        engine.close_async().await.unwrap();
        drop(engine);
        let engine =
            KvEngine::open_with_wal_io_mode(directory.path(), options, WalIoMode::Parallel)
                .unwrap();
        for writer in 0..12 {
            for batch in 0..8 {
                assert_eq!(
                    engine
                        .get(format!("mixed/{writer:02}/{batch}").as_bytes())
                        .unwrap(),
                    Some(Bytes::from(vec![writer as u8; 1024]))
                );
            }
        }
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_cancelled_blocking_read_and_maintenance_keep_close_waiting() {
        for maintenance in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let engine = open(directory.path());
            engine.background_workers.begin_shutdown(&engine.inner);
            engine.background_workers.join_blocking().unwrap();
            let inner = Arc::clone(&engine.inner);
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = std::thread::spawn(move || {
                let checkpoint = maintenance.then(|| inner.checkpoint_lock.lock());
                let reader =
                    (!maintenance).then(|| inner.mvcc.as_ref().unwrap().reader_lock.write());
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                drop((checkpoint, reader));
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let operation_engine = Arc::clone(&engine);
            let operation = tokio::spawn(async move {
                if maintenance {
                    operation_engine.force_flush_async().await.unwrap();
                } else {
                    for result in operation_engine.batch_get_async(&[b"a", b"b"]).await {
                        result.unwrap();
                    }
                }
            });
            until(|| {
                engine.inner.blocking.available_permits()
                    < crate::blocking_executor::DEFAULT_MAX_BLOCKING_THREADS
            })
            .await;
            operation.abort();
            assert!(operation.await.unwrap_err().is_cancelled());
            assert_eq!(
                engine
                    .inner
                    .lifecycle
                    .0
                    .active_writes
                    .load(Ordering::Acquire),
                1,
                "the detached blocking closure must keep its admission after cancellation"
            );
            let close_engine = Arc::clone(&engine);
            let close = tokio::spawn(async move { close_engine.close_async().await });
            until(|| engine.inner.lifecycle.ensure_open().is_err()).await;
            assert!(!close.is_finished());
            release_tx.send(()).unwrap();
            blocker.join().unwrap();
            close.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_point_dedup_ttl_and_delete_survive_rotation() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        engine.put_async(b"deleted", b"old").await.unwrap();
        let records = [
            WriteBatchRecord::Put(b"dedup".to_vec(), b"first".to_vec()),
            WriteBatchRecord::PutWithTtl(b"ttl".to_vec(), b"live".to_vec(), 3600),
            WriteBatchRecord::Del(b"deleted".to_vec()),
            WriteBatchRecord::Put(b"dedup".to_vec(), b"last".to_vec()),
        ];
        engine.write_batch_async(&records).await.unwrap();
        engine.put_async(b"delete_api", b"old").await.unwrap();
        engine.delete_async(b"delete_api").await.unwrap();
        engine.force_flush_async().await.unwrap();
        engine.close_async().await.unwrap();
        drop(engine);
        let engine = open(directory.path());
        assert_eq!(
            engine.get(b"dedup").unwrap().as_deref(),
            Some(b"last".as_slice())
        );
        assert_eq!(
            engine.get(b"ttl").unwrap().as_deref(),
            Some(b"live".as_slice())
        );
        assert_eq!(engine.get(b"deleted").unwrap(), None);
        assert_eq!(engine.get(b"delete_api").unwrap(), None);
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_cancelled_close_keeps_a_shutdown_owner_and_a_second_caller_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        let admission = engine.inner.lifecycle.admit_write().unwrap();
        let close_engine = Arc::clone(&engine);
        let close = tokio::spawn(async move { close_engine.close_async().await });
        until(|| engine.inner.lifecycle.ensure_open().is_err()).await;
        close.abort();
        assert!(close.await.unwrap_err().is_cancelled());
        drop(admission);
        tokio::time::timeout(Duration::from_secs(5), engine.close_async())
            .await
            .unwrap()
            .unwrap();
        assert!(engine.inner.lifecycle.is_closed());
        assert_eq!(
            engine.inner.state.load().memtable.parallel_wal_is_closed(),
            Some(true)
        );
    }

    #[test]
    fn native_async_close_leaves_one_blocking_thread_available_for_pending_freeze() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let mut options = options();
            options.target_sst_size = 1;
            let engine =
                KvEngine::open_with_wal_io_mode(directory.path(), options, WalIoMode::Parallel)
                    .unwrap();
            engine.background_workers.begin_shutdown(&engine.inner);
            engine.background_workers.join_blocking().unwrap();
            let initial_memtable = engine.inner.state.load().memtable.id();
            let pause = NativeWritePause::new(NativeWriteStage::Admitted);
            *engine.inner.native_write_pause.lock() = Some(Arc::clone(&pause));
            let writer_engine = Arc::clone(&engine);
            let writer =
                tokio::spawn(async move { writer_engine.put_async(b"key", b"value").await });
            pause.reached.notified().await;

            let first_engine = Arc::clone(&engine);
            let first_close = tokio::spawn(async move { first_engine.close_async().await });
            until(|| engine.inner.lifecycle.ensure_open().is_err()).await;
            first_close.abort();
            assert!(first_close.await.unwrap_err().is_cancelled());
            let second_engine = Arc::clone(&engine);
            let third_engine = Arc::clone(&engine);
            let second_close = tokio::spawn(async move { second_engine.close_async().await });
            let third_close = tokio::spawn(async move { third_engine.close_async().await });
            tokio::task::yield_now().await;
            assert_eq!(
                engine.inner.blocking.available_permits(),
                crate::blocking_executor::DEFAULT_MAX_BLOCKING_THREADS,
                "close callers must not reserve a blocking slot while draining writes"
            );
            pause.release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), async {
                writer.await.unwrap().unwrap();
                second_close.await.unwrap().unwrap();
                third_close.await.unwrap().unwrap();
            })
            .await
            .unwrap();
            assert_ne!(engine.inner.state.load().memtable.id(), initial_memtable);
            assert!(engine.inner.lifecycle.is_closed());
            assert_eq!(
                engine.inner.state.load().memtable.parallel_wal_is_closed(),
                Some(true)
            );
        });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_transaction_full_wal_retries_with_a_fresh_occ_check() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = options();
        options.serializable = true;
        let engine =
            KvEngine::open_with_wal_io_mode(directory.path(), options, WalIoMode::Parallel)
                .unwrap();
        let old_memtable = engine.inner.state.load().memtable.clone();
        old_memtable
            .set_parallel_wal_file_size_limit_for_test(1 << 20)
            .unwrap();
        let value = vec![1; 60 * 1024];
        for _ in 0..15 {
            engine.put_async(b"filler", &value).await.unwrap();
        }
        let txn = engine.new_txn_async().unwrap();
        txn.put(b"transaction", &value).unwrap();
        txn.commit_async().await.unwrap();
        drop(txn);
        assert_ne!(engine.inner.state.load().memtable.id(), old_memtable.id());
        assert_eq!(old_memtable.wal_batch_count(), Some(15));
        assert_eq!(
            engine.inner.state.load().memtable.wal_batch_count(),
            Some(1)
        );
        assert_eq!(
            engine.get(b"transaction").unwrap().as_deref(),
            Some(value.as_slice())
        );
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_read_created_during_a_failed_commit_registers_occ_on_first_poll() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = options();
        options.serializable = true;
        let engine =
            KvEngine::open_with_wal_io_mode(directory.path(), options, WalIoMode::Parallel)
                .unwrap();
        engine.background_workers.begin_shutdown(&engine.inner);
        engine.background_workers.join_blocking().unwrap();
        let memtable = engine.inner.state.load().memtable.clone();
        memtable
            .set_parallel_wal_file_size_limit_for_test(1 << 20)
            .unwrap();
        engine.put(b"conflict", b"old").unwrap();
        let value = vec![1; 60 * 1024];
        for _ in 0..15 {
            engine.put(b"filler", &value).unwrap();
        }
        let txn = engine.new_txn_async().unwrap();
        txn.put(b"unpublished", &value).unwrap();
        // Block rotation and make its successor file creation fail before
        // swapping memtables. This is a real recoverable commit failure.
        let successor = engine
            .inner
            .path_of_wal(engine.inner.next_sst_id.load(Ordering::Acquire));
        std::fs::create_dir(&successor).unwrap();
        let inner = Arc::clone(&engine.inner);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = std::thread::spawn(move || {
            let _checkpoint = inner.checkpoint_lock.lock();
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let commit = tokio::spawn(txn.commit_async());
        until(|| txn.committed.load(Ordering::SeqCst)).await;
        let read = txn.get_async(b"conflict");
        release_tx.send(()).unwrap();
        blocker.join().unwrap();
        let error = commit.await.unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("failed to rotate full active WAL"));
        assert!(!txn.committed.load(Ordering::SeqCst));
        assert!(txn.read_guard.lock().is_some());
        std::fs::remove_dir(&successor).unwrap();
        assert_eq!(read.await.unwrap().as_deref(), Some(&b"old"[..]));
        engine.put(b"conflict", b"new").unwrap();
        let error = txn.commit_async().await.unwrap_err();
        assert!(error.to_string().contains("serializable conflict"));
        assert_eq!(engine.get(b"unpublished").unwrap(), None);
        drop(txn);
        engine.close_async().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_cancelled_transaction_keeps_snapshot_and_close_waits_for_its_owner() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = options();
        options.serializable = true;
        let engine =
            KvEngine::open_with_wal_io_mode(directory.path(), options, WalIoMode::Parallel)
                .unwrap();
        let transaction = Arc::try_unwrap(engine.new_txn_async().unwrap())
            .unwrap_or_else(|_| panic!("new transaction must have one owner"));
        transaction.put(b"cancelled-transaction", b"value").unwrap();
        let committed = Arc::clone(&transaction.committed);
        let snapshot = Arc::clone(&transaction.read_guard);
        let mvcc = Arc::clone(engine.inner.mvcc.as_ref().unwrap());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = std::thread::spawn(move || {
            let _guard = mvcc.commit_lock.lock();
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let commit = tokio::spawn(async move { transaction.commit_async().await });
        until(|| committed.load(Ordering::SeqCst)).await;
        commit.abort();
        assert!(commit.await.unwrap_err().is_cancelled());
        assert!(
            snapshot.lock().is_some(),
            "the dispatched OCC job still owns its snapshot"
        );
        let close_engine = Arc::clone(&engine);
        let close = tokio::spawn(async move { close_engine.close_async().await });
        until(|| engine.inner.lifecycle.ensure_open().is_err()).await;
        assert!(
            !close.is_finished(),
            "shutdown must wait for the detached commit owner"
        );
        release_tx.send(()).unwrap();
        blocker.join().unwrap();
        close.await.unwrap().unwrap();
        assert!(snapshot.lock().is_none());
        assert_eq!(
            engine.get(b"cancelled-transaction").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_async_shutdown_join_error_still_closes_wal_and_records_a_terminal_error() {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        engine.put_async(b"prefix", b"value").await.unwrap();
        engine.background_workers.begin_shutdown(&engine.inner);
        engine.background_workers.join_blocking().unwrap();
        *engine.background_workers.runtime_thread.lock() = Some(std::thread::spawn(|| {
            panic!("injected background join failure")
        }));
        assert!(engine.close_async().await.is_err());
        assert!(engine.inner.lifecycle.is_closed());
        assert_eq!(
            engine.inner.state.load().memtable.parallel_wal_is_closed(),
            Some(true)
        );
        assert!(engine.close_async().await.is_err());
    }

    #[cfg(feature = "chaos-testing")]
    #[tokio::test(flavor = "current_thread")]
    async fn failpoint_native_async_sync_failure_poisons_waiters_without_ghost_publication() {
        use crate::chaos::failpoint::{self, FailScenario};

        let _scenario = FailScenario::setup();
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        engine.put_async(b"prefix", b"value").await.unwrap();
        failpoint::cfg("parallel_wal.fdatasync_failure", "return").unwrap();
        let result = engine.put_async(b"failed", b"invisible").await;
        failpoint::cfg("parallel_wal.fdatasync_failure", "off").unwrap();
        assert!(result.is_err());
        assert_eq!(
            engine.get(b"prefix").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        assert_eq!(engine.get(b"failed").unwrap(), None);
        assert!(engine.put_async(b"later", b"rejected").await.is_err());
        assert!(engine.close_async().await.is_err());
        assert_eq!(
            engine.inner.state.load().memtable.parallel_wal_is_closed(),
            Some(true)
        );
    }
}
