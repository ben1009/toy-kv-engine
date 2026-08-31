use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    lsm_storage::{KvEngine, LsmStorageInner},
    manifest::{
        ImmutableFileKind, ImmutableFileMetadata, MANIFEST_FORMAT_VERSION, Manifest, ManifestRecord,
    },
};

#[derive(Clone, Debug)]
pub struct CheckpointOptions {
    pub overwrite: bool,
    pub use_hard_links: bool,
    pub include_vlog_indexes: bool,
}

impl Default for CheckpointOptions {
    fn default() -> Self {
        Self {
            overwrite: false,
            use_hard_links: true,
            include_vlog_indexes: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointStats {
    pub sst_files: usize,
    pub vlog_files: usize,
    pub vlog_index_files: usize,
    pub manifest_files: usize,
    pub hard_linked_files: usize,
    pub copied_files: usize,
    pub sst_count: usize,
    pub files_copied: usize,
    pub files_hard_linked: usize,
    pub bytes_copied: u64,
    pub bytes_referenced: u64,
}

#[derive(Debug)]
pub struct PublishedButNotDurable {
    target_dir: PathBuf,
    source: std::io::Error,
}

impl PublishedButNotDurable {
    pub fn target_dir(&self) -> &Path {
        &self.target_dir
    }
}

impl fmt::Display for PublishedButNotDurable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "checkpoint was published at {} but parent directory fsync failed: {}",
            self.target_dir.display(),
            self.source
        )
    }
}

impl Error for PublishedButNotDurable {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Default)]
pub(crate) struct CheckpointFilePins {
    ssts: HashMap<usize, usize>,
    pending_delete_ssts: HashSet<usize>,
}

impl CheckpointFilePins {
    fn pin_ssts(&mut self, sst_ids: &[usize]) {
        for id in sst_ids {
            *self.ssts.entry(*id).or_insert(0) += 1;
        }
    }

    fn unpin_ssts(&mut self, sst_ids: &[usize]) -> Vec<usize> {
        let mut ready_to_delete = Vec::new();
        for id in sst_ids {
            if let Some(count) = self.ssts.get_mut(id) {
                *count -= 1;
                if *count == 0 {
                    self.ssts.remove(id);
                    if self.pending_delete_ssts.remove(id) {
                        ready_to_delete.push(*id);
                    }
                }
            }
        }

        ready_to_delete
    }

    pub(crate) fn is_sst_pinned(&self, sst_id: usize) -> bool {
        self.ssts.contains_key(&sst_id)
    }

    pub(crate) fn mark_sst_delete_if_pinned(&mut self, sst_id: usize) -> bool {
        if self.is_sst_pinned(sst_id) {
            self.pending_delete_ssts.insert(sst_id);
            return true;
        }

        false
    }
}

struct CheckpointPinGuard<'a> {
    inner: &'a LsmStorageInner,
    sst_ids: Vec<usize>,
}

impl Drop for CheckpointPinGuard<'_> {
    fn drop(&mut self) {
        let pending_delete_ssts = self
            .inner
            .checkpoint_file_pins
            .lock()
            .unpin_ssts(&self.sst_ids);
        for sst_id in pending_delete_ssts {
            let path = self.inner.path_of_sst(sst_id);
            if let Err(err) = fs::remove_file(&path)
                && err.kind() != ErrorKind::NotFound
            {
                log::warn!(
                    "failed to remove SST {} after checkpoint unpin: {}",
                    path.display(),
                    err
                );
            }
        }
    }
}

/// A flushed, pinned physical view of the engine.
///
/// The guard fields deliberately remain private: callers may copy only the
/// listed immutable files while this value is alive.  Dropping it releases the
/// SST and vLog pins, including on an error path.
pub(crate) struct CheckpointCapture<'a> {
    pub(crate) snapshot_record: ManifestRecord,
    pub(crate) sst_ids: Vec<usize>,
    pub(crate) vlog_ids: Vec<u32>,
    #[allow(dead_code)] // consumed by the forthcoming repository publisher
    pub(crate) immutable_file_metadata: Vec<ImmutableFileMetadata>,
    _pin_guard: CheckpointPinGuard<'a>,
    _vlog_pin_guard: Option<VlogCheckpointPinGuard<'a>>,
}

struct CheckpointSnapshotPins<'a> {
    snapshot_record: ManifestRecord,
    sst_ids: Vec<usize>,
    vlog_ids: Vec<u32>,
    immutable_file_metadata: Vec<ImmutableFileMetadata>,
    sst_pin_guard: CheckpointPinGuard<'a>,
    vlog_pin_guard: Option<VlogCheckpointPinGuard<'a>>,
}

struct VlogCheckpointPinGuard<'a> {
    vlog: &'a crate::vlog::ValueLog,
    file_ids: Vec<u32>,
}

impl Drop for VlogCheckpointPinGuard<'_> {
    fn drop(&mut self) {
        self.vlog.unpin_files_for_checkpoint(&self.file_ids);
    }
}

struct TargetCheckpointLock {
    #[allow(dead_code)]
    file: File,
}

impl TargetCheckpointLock {
    fn acquire(target_dir: &Path, attempt_id: &str) -> Result<Self> {
        let lock_path = checkpoint_lock_path(target_dir)?;
        let target_identity = checkpoint_target_identity(target_dir)?;
        let mut lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| {
                format!(
                    "failed to open checkpoint target lock {}",
                    lock_path.display()
                )
            })?;
        lock_checkpoint_file(&lock_file).with_context(|| {
            format!(
                "failed to acquire checkpoint target lock {}",
                lock_path.display()
            )
        })?;
        lock_file.set_len(0)?;
        writeln!(lock_file, "pid={}", std::process::id())?;
        writeln!(lock_file, "target_dir={target_identity}")?;
        writeln!(lock_file, "attempt_id={attempt_id}")?;
        lock_file.sync_all()?;

        Ok(Self { file: lock_file })
    }
}

#[derive(Deserialize, Serialize)]
struct CheckpointMarker {
    version: u32,
    state: String,
    target_dir: String,
    attempt_id: String,
    sst_files: usize,
    vlog_files: usize,
    vlog_index_files: usize,
    manifest_files: usize,
    hard_linked_files: usize,
    copied_files: usize,
    sst_count: usize,
    files_copied: usize,
    files_hard_linked: usize,
    bytes_copied: u64,
    bytes_referenced: u64,
}

impl KvEngine {
    pub fn create_checkpoint(&self, target_dir: impl AsRef<Path>) -> Result<CheckpointStats> {
        self.inner
            .create_checkpoint_with_options(target_dir, CheckpointOptions::default())
    }

    pub fn create_checkpoint_with_options(
        &self,
        target_dir: impl AsRef<Path>,
        options: CheckpointOptions,
    ) -> Result<CheckpointStats> {
        self.inner
            .create_checkpoint_with_options(target_dir, options)
    }

    pub async fn create_checkpoint_async(
        &self,
        target_dir: impl AsRef<Path> + Send,
    ) -> Result<CheckpointStats> {
        self.create_checkpoint_with_options_async(target_dir, CheckpointOptions::default())
            .await
    }

    pub async fn create_checkpoint_with_options_async(
        &self,
        target_dir: impl AsRef<Path> + Send,
        options: CheckpointOptions,
    ) -> Result<CheckpointStats> {
        let guard = self.inner.lifecycle.admit_write()?;
        let inner = self.inner.clone();
        let target_dir = target_dir.as_ref().to_path_buf();
        self.inner
            .blocking
            .run_result(move || {
                let _guard = guard;
                inner.create_checkpoint_with_options(target_dir, options)
            })
            .await
    }
}

impl LsmStorageInner {
    pub(crate) fn create_checkpoint_with_options(
        &self,
        target_dir: impl AsRef<Path>,
        options: CheckpointOptions,
    ) -> Result<CheckpointStats> {
        self.validate_checkpoint_options(target_dir.as_ref(), &options)?;

        let target_dir = absolute_path(target_dir.as_ref())?;
        self.validate_checkpoint_target(&target_dir, &options)?;
        let tmp_dir = checkpoint_tmp_dir(&target_dir);
        let attempt_id = checkpoint_attempt_id(&tmp_dir);
        let _target_lock = TargetCheckpointLock::acquire(&target_dir, &attempt_id)?;
        self.validate_checkpoint_target(&target_dir, &options)?;
        cleanup_checkpoint_tmps_for_target(&target_dir)?;

        let result = (|| {
            let _checkpoint_guard = self.checkpoint_lock.lock();
            let prepared = self.prepare_checkpoint(&target_dir, &tmp_dir)?;
            drop(_checkpoint_guard);
            self.publish_prepared_checkpoint(&target_dir, &tmp_dir, &options, prepared)
        })();
        if result.is_err() {
            let _ = cleanup_checkpoint_tmp(&tmp_dir, &target_dir);
            let _ = cleanup_checkpoint_staging_tmp(&checkpoint_staging_tmp_dir(&tmp_dir));
        }

        result
    }

    fn validate_checkpoint_options(
        &self,
        target_dir: &Path,
        options: &CheckpointOptions,
    ) -> Result<()> {
        ensure!(
            !target_dir.as_os_str().is_empty(),
            "checkpoint target must not be empty"
        );
        ensure!(
            !options.overwrite,
            "checkpoint overwrite is not supported in phase 1"
        );
        ensure_checkpoint_publish_supported(target_dir)?;

        Ok(())
    }

    fn validate_checkpoint_target(
        &self,
        target_dir: &Path,
        options: &CheckpointOptions,
    ) -> Result<()> {
        if !options.overwrite && target_dir.exists() {
            bail!("checkpoint target {} already exists", target_dir.display());
        }

        let source_dir = self
            .db_path()
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", self.db_path().display()))?;
        let target_for_inside_check = canonicalize_existing_prefix(target_dir)?;
        if target_for_inside_check.starts_with(&source_dir) {
            bail!(
                "checkpoint target {} must not be inside source database {}",
                target_dir.display(),
                source_dir.display()
            );
        }

        Ok(())
    }

    fn prepare_checkpoint<'a>(
        &'a self,
        target_dir: &Path,
        tmp_dir: &Path,
    ) -> Result<CheckpointCapture<'a>> {
        let staging_tmp_dir = checkpoint_staging_tmp_dir(tmp_dir);
        fs::create_dir_all(&staging_tmp_dir).with_context(|| {
            format!(
                "failed to create checkpoint staging tmp {}",
                staging_tmp_dir.display()
            )
        })?;
        write_marker(
            staging_tmp_dir.join("CHECKPOINT_IN_PROGRESS"),
            target_dir,
            tmp_dir,
            "in_progress",
            None,
        )?;
        fsync_dir(&staging_tmp_dir)?;
        rename_no_replace(&staging_tmp_dir, tmp_dir).with_context(|| {
            format!(
                "failed to publish marked checkpoint tmp {} from {}",
                tmp_dir.display(),
                staging_tmp_dir.display()
            )
        })?;
        if let Some(parent) = tmp_dir.parent() {
            fsync_dir(parent)?;
        }
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_tmp_dir_create");
        }
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_in_progress_marker");
        }

        self.capture_checkpoint_state_locked()
    }

    fn publish_prepared_checkpoint(
        &self,
        target_dir: &Path,
        tmp_dir: &Path,
        options: &CheckpointOptions,
        prepared: CheckpointCapture<'_>,
    ) -> Result<CheckpointStats> {
        let mut stats = CheckpointStats {
            sst_files: prepared.sst_ids.len(),
            sst_count: prepared.sst_ids.len(),
            ..CheckpointStats::default()
        };
        copy_checkpoint_ssts(self, tmp_dir, &prepared.sst_ids, options, &mut stats)?;
        copy_checkpoint_vlogs(self, tmp_dir, &prepared.vlog_ids, options, &mut stats)?;
        write_checkpoint_manifest(tmp_dir, prepared.snapshot_record, &mut stats)?;
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_manifest_write");
        }
        write_marker(
            tmp_dir.join("CHECKPOINT_READY"),
            target_dir,
            tmp_dir,
            "ready",
            Some(stats),
        )?;
        fsync_dir(tmp_dir)?;
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_ready_marker");
        }
        fs::remove_file(tmp_dir.join("CHECKPOINT_IN_PROGRESS"))
            .context("failed to remove in-progress checkpoint marker")?;
        fs::rename(tmp_dir.join("CHECKPOINT_READY"), tmp_dir.join("CHECKPOINT"))
            .context("failed to publish checkpoint marker")?;
        fsync_dir(tmp_dir)?;
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_checkpoint_marker");
        }

        if target_dir.exists() {
            bail!("checkpoint target {} already exists", target_dir.display());
        }
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.before_publish_rename");
        }
        rename_no_replace(tmp_dir, target_dir).with_context(|| {
            format!(
                "failed to publish checkpoint {} from {}",
                target_dir.display(),
                tmp_dir.display()
            )
        })?;
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_publish_rename_before_dir_sync");
        }
        if let Some(parent) = target_dir.parent()
            && let Err(source) = fsync_dir(parent)
        {
            return Err(PublishedButNotDurable {
                target_dir: target_dir.to_path_buf(),
                source: io_error_from_anyhow(source),
            }
            .into());
        }

        Ok(stats)
    }

    fn flush_all_memtables_for_checkpoint(&self) -> Result<()> {
        {
            let state_lock = self.state_lock.lock();
            let active_memtable_guard = self.active_memtable_lock.write();
            self.sync().context("failed to sync active WAL")?;
            if !self.state.load().memtable.is_empty() {
                self.force_freeze_memtable_with_active_guard(&state_lock, &active_memtable_guard)?;
            }
        }
        while !self.state.load().imm_memtables.is_empty() {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Flush committed state, then capture and pin the exact immutable file
    /// set named by a canonical manifest snapshot.
    ///
    /// Checkpoints and RFC 022 incremental backups share this physical
    /// consistency boundary. The `checkpoint_lock` is intentionally held only
    /// while the boundary is created; the returned capture owns the file pins
    /// for the potentially much longer copy/link phase.
    #[allow(dead_code)] // consumed by the forthcoming backup publisher
    pub(crate) fn capture_checkpoint_state(&self) -> Result<CheckpointCapture<'_>> {
        let _checkpoint_guard = self.checkpoint_lock.lock();
        self.capture_checkpoint_state_locked()
    }

    fn capture_checkpoint_state_locked(&self) -> Result<CheckpointCapture<'_>> {
        self.flush_all_memtables_for_checkpoint()?;

        let snapshot_pins = self.checkpoint_manifest_snapshot_record_and_pin()?;
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_sst_pin_before_copy");
        }

        Ok(CheckpointCapture {
            snapshot_record: snapshot_pins.snapshot_record,
            sst_ids: snapshot_pins.sst_ids,
            vlog_ids: snapshot_pins.vlog_ids,
            immutable_file_metadata: snapshot_pins.immutable_file_metadata,
            _pin_guard: snapshot_pins.sst_pin_guard,
            _vlog_pin_guard: snapshot_pins.vlog_pin_guard,
        })
    }

    fn checkpoint_manifest_snapshot_record_and_pin(&self) -> Result<CheckpointSnapshotPins<'_>> {
        let _state_lock = self.state_lock.lock();
        let guard = self.state.load();
        let state = guard.as_ref();
        ensure!(
            state.imm_memtables.is_empty(),
            "checkpoint capture requires all immutable memtables to be flushed"
        );
        let mut sst_ids: Vec<usize> = state.sstables.keys().copied().collect();
        sst_ids.sort_unstable();
        let mut vlog_references = Vec::new();
        let mut vlog_ids = HashSet::new();
        if let Some(ref vlog) = self.vlog {
            for sst_id in &sst_ids {
                if let Some(mut refs) = vlog.get_sst_references(*sst_id)
                    && !refs.is_empty()
                {
                    refs.sort_unstable();
                    refs.dedup();
                    vlog_ids.extend(refs.iter().copied());
                    vlog_references.push((*sst_id, refs));
                }
            }
        }
        let mut vlog_ids = vlog_ids.into_iter().collect::<Vec<_>>();
        vlog_ids.sort_unstable();
        let (active_compaction_filters, next_compaction_filter_id) =
            self.checkpoint_compaction_filter_snapshot();
        self.checkpoint_file_pins.lock().pin_ssts(&sst_ids);
        let sst_pin_guard = CheckpointPinGuard {
            inner: self,
            sst_ids: sst_ids.clone(),
        };
        let vlog_pin_guard = self.vlog.as_ref().map(|vlog| {
            vlog.pin_files_for_checkpoint(&vlog_ids);
            VlogCheckpointPinGuard {
                vlog,
                file_ids: vlog_ids.clone(),
            }
        });
        // Checkpoint capture must retain its existing bounded critical section.
        // Backup-specific publication hashes these pinned files after capture.
        let immutable_file_metadata = Vec::new();
        Ok(CheckpointSnapshotPins {
            snapshot_record: ManifestRecord::Snapshot {
                l0_sstables: state.l0_sstables.clone(),
                levels: state.levels.clone(),
                range_only_ssts: state.range_only_ssts.clone(),
                next_sst_id: self.current_sst_id(),
                vlog_references,
                imm_memtable_ids: Vec::new(),
                active_compaction_filters,
                next_compaction_filter_id,
                format_version: MANIFEST_FORMAT_VERSION,
                immutable_file_metadata: state.immutable_file_metadata.clone(),
            },
            sst_ids,
            vlog_ids,
            immutable_file_metadata,
            sst_pin_guard,
            vlog_pin_guard,
        })
    }

    #[allow(dead_code)]
    fn capture_immutable_file_metadata(
        &self,
        sst_ids: &[usize],
        vlog_ids: &[u32],
    ) -> Result<Vec<ImmutableFileMetadata>> {
        let mut metadata = Vec::with_capacity(sst_ids.len() + vlog_ids.len());
        for &id in sst_ids {
            metadata.push(hash_immutable_file(
                ImmutableFileKind::Sst,
                id as u64,
                &self.path_of_sst(id),
            )?);
        }
        if let Some(vlog) = self.vlog.as_ref() {
            for &id in vlog_ids {
                metadata.push(hash_immutable_file(
                    ImmutableFileKind::Vlog,
                    u64::from(id),
                    &vlog.path_of_file(id),
                )?);
            }
        }
        Ok(metadata)
    }
}

fn hash_immutable_file(
    kind: ImmutableFileKind,
    file_id: u64,
    path: &Path,
) -> Result<ImmutableFileMetadata> {
    let mut file = File::open(path)
        .with_context(|| format!("failed to open immutable file {}", path.display()))?;
    let file_size = file.metadata()?.len();
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(ImmutableFileMetadata {
        kind,
        file_id,
        file_size,
        file_checksum: hasher.finalize().into(),
    })
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn immutable_file_identity_hashes_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00001.sst");
        std::fs::write(&path, b"incremental-backup").unwrap();

        let identity = hash_immutable_file(ImmutableFileKind::Sst, 1, &path).unwrap();
        assert_eq!(identity.kind, ImmutableFileKind::Sst);
        assert_eq!(identity.file_id, 1);
        assert_eq!(identity.file_size, 18);
        let expected_checksum: [u8; 32] = Sha256::digest(b"incremental-backup").into();
        assert_eq!(identity.file_checksum, expected_checksum);
    }
}

fn write_checkpoint_manifest(
    tmp_dir: &Path,
    snapshot_record: ManifestRecord,
    stats: &mut CheckpointStats,
) -> Result<()> {
    let manifest = Manifest::create(tmp_dir.join("MANIFEST"))?;
    manifest.snapshot(snapshot_record)?;
    stats.manifest_files += 2;

    Ok(())
}

fn copy_checkpoint_ssts(
    storage: &LsmStorageInner,
    tmp_dir: &Path,
    sst_ids: &[usize],
    options: &CheckpointOptions,
    stats: &mut CheckpointStats,
) -> Result<()> {
    for sst_id in sst_ids {
        let source = storage.path_of_sst(*sst_id);
        let target = LsmStorageInner::path_of_sst_static(tmp_dir, *sst_id);
        copy_or_link_file(&source, &target, options, stats)
            .with_context(|| format!("failed to checkpoint SST {}", source.display()))?;
    }

    Ok(())
}

fn copy_checkpoint_vlogs(
    storage: &LsmStorageInner,
    tmp_dir: &Path,
    vlog_ids: &[u32],
    options: &CheckpointOptions,
    stats: &mut CheckpointStats,
) -> Result<()> {
    let Some(ref vlog) = storage.vlog else {
        return Ok(());
    };
    if vlog_ids.is_empty() {
        return Ok(());
    }

    let target_vlog_dir = tmp_dir.join("vlog");
    fs::create_dir_all(&target_vlog_dir).with_context(|| {
        format!(
            "failed to create checkpoint vLog dir {}",
            target_vlog_dir.display()
        )
    })?;

    for file_id in vlog_ids {
        let source = vlog.path_of_file(*file_id);
        let target = target_vlog_dir.join(format!("{file_id}.vlog"));
        copy_or_link_file(&source, &target, options, stats)
            .with_context(|| format!("failed to checkpoint vLog {}", source.display()))?;
        stats.vlog_files += 1;

        if options.include_vlog_indexes {
            let source_index = crate::vlog::index::index_path_for_vlog(&source);
            match fs::metadata(&source_index) {
                Ok(metadata) if metadata.is_file() => {
                    let target_index = target_vlog_dir.join(format!("{file_id}.vidx"));
                    match copy_or_link_optional_file(&source_index, &target_index, options, stats) {
                        Ok(true) => stats.vlog_index_files += 1,
                        Ok(false) => {}
                        Err(err) => {
                            return Err(err).with_context(|| {
                                format!(
                                    "failed to checkpoint vLog index {}",
                                    source_index.display()
                                )
                            });
                        }
                    }
                }
                Ok(_) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to stat vLog index {}", source_index.display())
                    });
                }
            }
        }
    }
    fsync_dir(&target_vlog_dir)?;

    Ok(())
}

fn copy_or_link_optional_file(
    source: &Path,
    target: &Path,
    options: &CheckpointOptions,
    stats: &mut CheckpointStats,
) -> Result<bool> {
    match copy_or_link_file(source, target, options, stats) {
        Ok(()) => Ok(true),
        Err(err) if is_not_found(&err) => Ok(false),
        Err(err) => Err(err),
    }
}

fn copy_or_link_file(
    source: &Path,
    target: &Path,
    options: &CheckpointOptions,
    stats: &mut CheckpointStats,
) -> Result<()> {
    let size = fs::metadata(source)
        .with_context(|| format!("failed to stat {}", source.display()))?
        .len();
    if options.use_hard_links {
        match fs::hard_link(source, target) {
            Ok(()) => {
                stats.hard_linked_files += 1;
                stats.files_hard_linked = stats.hard_linked_files;
                stats.bytes_referenced += size;
                #[cfg(feature = "chaos-testing")]
                {
                    crate::chaos::failpoint::fail_point!("checkpoint.after_file_copy");
                }
                return Ok(());
            }
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::CrossesDevices
                        | ErrorKind::PermissionDenied
                        | ErrorKind::Unsupported
                ) => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to hard-link {} to {}",
                        source.display(),
                        target.display()
                    )
                });
            }
        }
    }
    fs::copy(source, target).with_context(|| {
        format!(
            "failed to copy {} to {}",
            source.display(),
            target.display()
        )
    })?;
    File::open(target)
        .with_context(|| format!("failed to open copied file {}", target.display()))?
        .sync_all()
        .with_context(|| format!("failed to sync copied file {}", target.display()))?;
    stats.copied_files += 1;
    stats.files_copied = stats.copied_files;
    stats.bytes_copied += size;
    #[cfg(feature = "chaos-testing")]
    {
        crate::chaos::failpoint::fail_point!("checkpoint.after_file_copy");
    }

    Ok(())
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|err| err.kind() == ErrorKind::NotFound)
    })
}

fn write_marker(
    path: impl AsRef<Path>,
    target_dir: &Path,
    tmp_dir: &Path,
    state: &str,
    stats: Option<CheckpointStats>,
) -> Result<()> {
    let stats = stats.unwrap_or_default();
    let marker = CheckpointMarker {
        version: 1,
        state: state.to_string(),
        target_dir: checkpoint_target_identity(target_dir)?,
        attempt_id: checkpoint_attempt_id(tmp_dir),
        sst_files: stats.sst_files,
        vlog_files: stats.vlog_files,
        vlog_index_files: stats.vlog_index_files,
        manifest_files: stats.manifest_files,
        hard_linked_files: stats.hard_linked_files,
        copied_files: stats.copied_files,
        sst_count: stats.sst_count,
        files_copied: stats.files_copied,
        files_hard_linked: stats.files_hard_linked,
        bytes_copied: stats.bytes_copied,
        bytes_referenced: stats.bytes_referenced,
    };
    let mut file = File::create_new(path.as_ref()).with_context(|| {
        format!(
            "failed to create checkpoint marker {}",
            path.as_ref().display()
        )
    })?;
    serde_json::to_writer_pretty(&mut file, &marker)?;
    file.write_all(b"\n")?;
    file.sync_all()?;

    Ok(())
}

fn checkpoint_target_identity(target_dir: &Path) -> Result<String> {
    Ok(canonicalize_existing_prefix(target_dir)?
        .display()
        .to_string())
}

fn checkpoint_attempt_id(tmp_dir: &Path) -> String {
    tmp_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| tmp_dir.display().to_string())
}

fn checkpoint_lock_path(target_dir: &Path) -> Result<PathBuf> {
    let file_name = target_dir
        .file_name()
        .ok_or_else(|| anyhow!("checkpoint target must have a final path component"))?
        .to_string_lossy();
    Ok(target_dir.with_file_name(format!("{file_name}.checkpoint.lock")))
}

#[cfg(unix)]
fn lock_checkpoint_file(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_checkpoint_file(_file: &File) -> std::io::Result<()> {
    Ok(())
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("failed to open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to sync directory {}", path.display()))
}

fn cleanup_checkpoint_tmp(path: &Path, target_dir: &Path) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            ensure_stale_checkpoint_tmp_matches(path, target_dir)?;
            fs::remove_dir_all(path)
                .with_context(|| format!("failed to remove checkpoint tmp {}", path.display()))
        }
        Ok(_) => bail!(
            "checkpoint tmp {} exists and is not a directory",
            path.display()
        ),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to stat checkpoint tmp {}", path.display()))
        }
    }
}

fn cleanup_checkpoint_staging_tmp(path: &Path) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove checkpoint staging tmp {}", path.display())),
        Ok(_) => bail!(
            "checkpoint staging tmp {} exists and is not a directory",
            path.display()
        ),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err)
            .with_context(|| format!("failed to stat checkpoint staging tmp {}", path.display())),
    }
}

fn cleanup_checkpoint_tmps_for_target(target_dir: &Path) -> Result<()> {
    let parent = target_dir
        .parent()
        .ok_or_else(|| anyhow!("checkpoint target must have a parent directory"))?;
    let Some(target_name) = target_dir.file_name().map(|name| name.to_string_lossy()) else {
        return Ok(());
    };
    if !parent.exists() {
        return Ok(());
    }

    for entry in
        fs::read_dir(parent).with_context(|| format!("failed to read {}", parent.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{target_name}.checkpoint-")) && name.ends_with(".tmp") {
            cleanup_checkpoint_tmp(&path, target_dir)?;
        }
    }

    Ok(())
}

fn ensure_stale_checkpoint_tmp_matches(path: &Path, target_dir: &Path) -> Result<()> {
    let marker_path = ["CHECKPOINT_IN_PROGRESS", "CHECKPOINT", "CHECKPOINT_READY"]
        .into_iter()
        .map(|marker| path.join(marker))
        .find(|marker_path| marker_path.exists())
        .ok_or_else(|| {
            anyhow!(
                "checkpoint tmp {} has no kv-engine checkpoint marker",
                path.display()
            )
        })?;

    let marker = read_checkpoint_marker(&marker_path)?;
    ensure!(
        marker.version == 1,
        "checkpoint tmp {} has unsupported marker version {}",
        path.display(),
        marker.version
    );
    let target_identity = checkpoint_target_identity(target_dir)?;
    ensure!(
        marker.target_dir == target_identity,
        "checkpoint tmp {} belongs to target {} not {}",
        path.display(),
        marker.target_dir,
        target_identity
    );
    ensure!(
        marker.attempt_id == checkpoint_attempt_id(path),
        "checkpoint tmp {} has mismatched attempt id {}",
        path.display(),
        marker.attempt_id
    );

    Ok(())
}

fn read_checkpoint_marker(path: &Path) -> Result<CheckpointMarker> {
    let contents = fs::read(path)
        .with_context(|| format!("failed to read checkpoint marker {}", path.display()))?;
    serde_json::from_slice(&contents)
        .with_context(|| format!("failed to parse checkpoint marker {}", path.display()))
}

fn checkpoint_tmp_dir(target_dir: &Path) -> PathBuf {
    let name = target_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "checkpoint".to_string());
    target_dir.with_file_name(format!(
        "{name}.checkpoint-{}-{}.tmp",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ))
}

fn checkpoint_staging_tmp_dir(tmp_dir: &Path) -> PathBuf {
    tmp_dir.with_extension("staging")
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(normalize_path(&path))
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }

    normalized
}

fn canonicalize_existing_prefix(path: &Path) -> Result<PathBuf> {
    let mut existing = PathBuf::new();
    let mut rest = Vec::new();
    for component in path.components() {
        let candidate = existing.join(component.as_os_str());
        if rest.is_empty() && candidate.exists() {
            existing = candidate;
        } else {
            rest.push(component);
        }
    }

    let mut resolved = if existing.as_os_str().is_empty() {
        PathBuf::new()
    } else {
        existing
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", existing.display()))?
    };
    for component in rest {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            component => resolved.push(component.as_os_str()),
        }
    }

    Ok(resolved)
}

fn io_error_from_anyhow(error: anyhow::Error) -> std::io::Error {
    match error.downcast::<std::io::Error>() {
        Ok(error) => error,
        Err(error) => std::io::Error::other(error.to_string()),
    }
}

fn rename_no_replace(from: &Path, to: &Path) -> Result<()> {
    rename_no_replace_platform(from, to)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn ensure_checkpoint_publish_supported(_target_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn ensure_checkpoint_publish_supported(target_dir: &Path) -> Result<()> {
    rename_no_replace_unavailable(&checkpoint_tmp_dir(target_dir), target_dir)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_no_replace_platform(from: &Path, to: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let from_path = from.to_path_buf();
    let to_path = to.to_path_buf();
    let from = CString::new(from.as_os_str().as_bytes())?;
    let to = CString::new(to.as_os_str().as_bytes())?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        return Ok(());
    }

    let err = std::io::Error::last_os_error();
    if matches!(err.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) {
        rename_no_replace_unavailable(&from_path, &to_path)
    } else {
        Err(err).context("failed to rename without replacing target")
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn rename_no_replace_platform(from: &Path, to: &Path) -> Result<()> {
    rename_no_replace_unavailable(from, to)
}

fn rename_no_replace_unavailable(from: &Path, to: &Path) -> Result<()> {
    bail!(
        "atomic no-replace rename is unavailable for checkpoint publish from {} to {}",
        from.display(),
        to.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_copy_skips_missing_source_without_stats() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("missing.vidx");
        let target = dir.path().join("target.vidx");
        let mut stats = CheckpointStats::default();

        assert!(
            !copy_or_link_optional_file(
                &source,
                &target,
                &CheckpointOptions::default(),
                &mut stats
            )
            .unwrap()
        );

        assert!(!target.exists());
        assert_eq!(stats.files_copied, 0);
        assert_eq!(stats.files_hard_linked, 0);
        assert_eq!(stats.bytes_copied, 0);
        assert_eq!(stats.bytes_referenced, 0);
    }

    #[test]
    fn strict_copy_errors_on_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("missing.vlog");
        let target = dir.path().join("target.vlog");
        let mut stats = CheckpointStats::default();

        let err = copy_or_link_file(&source, &target, &CheckpointOptions::default(), &mut stats)
            .unwrap_err();

        assert!(is_not_found(&err));
        assert!(!target.exists());
        assert_eq!(stats.files_copied, 0);
        assert_eq!(stats.files_hard_linked, 0);
    }

    #[test]
    fn cleanup_accepts_ready_marker_without_in_progress_marker() {
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("checkpoint");
        let tmp_dir = dir.path().join("checkpoint.checkpoint-test.tmp");
        fs::create_dir(&tmp_dir).unwrap();
        write_marker(
            tmp_dir.join("CHECKPOINT_READY"),
            &target_dir,
            &tmp_dir,
            "ready",
            Some(CheckpointStats::default()),
        )
        .unwrap();

        cleanup_checkpoint_tmp(&tmp_dir, &target_dir).unwrap();

        assert!(!tmp_dir.exists());
    }

    #[test]
    #[cfg(unix)]
    fn cleanup_matches_target_identity_through_symlink_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let real_parent = dir.path().join("real");
        let link_parent = dir.path().join("link");
        fs::create_dir(&real_parent).unwrap();
        std::os::unix::fs::symlink(&real_parent, &link_parent).unwrap();

        let target_via_link = link_parent.join("checkpoint");
        let target_via_real = real_parent.join("checkpoint");
        let tmp_dir = real_parent.join("checkpoint.checkpoint-test.tmp");
        fs::create_dir(&tmp_dir).unwrap();
        write_marker(
            tmp_dir.join("CHECKPOINT_IN_PROGRESS"),
            &target_via_link,
            &tmp_dir,
            "in_progress",
            Some(CheckpointStats::default()),
        )
        .unwrap();

        cleanup_checkpoint_tmp(&tmp_dir, &target_via_real).unwrap();

        assert!(!tmp_dir.exists());
    }

    #[test]
    fn staging_tmp_name_is_excluded_from_retry_cleanup_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("checkpoint");
        let tmp_dir = checkpoint_tmp_dir(&target_dir);
        let staging_tmp_dir = checkpoint_staging_tmp_dir(&tmp_dir);
        let staging_name = staging_tmp_dir.file_name().unwrap().to_string_lossy();

        assert!(staging_name.starts_with("checkpoint.checkpoint-"));
        assert!(!staging_name.ends_with(".tmp"));
    }
}
