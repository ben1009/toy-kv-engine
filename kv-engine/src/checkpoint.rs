use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::Serialize;

use crate::{
    lsm_storage::{KvEngine, LsmStorageInner},
    manifest::{MANIFEST_FORMAT_VERSION, Manifest, ManifestRecord},
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

struct PreparedCheckpoint<'a> {
    snapshot_record: ManifestRecord,
    sst_ids: Vec<usize>,
    vlog_ids: Vec<u32>,
    _pin_guard: CheckpointPinGuard<'a>,
    _vlog_pin_guard: Option<VlogCheckpointPinGuard<'a>>,
}

struct CheckpointSnapshotPins<'a> {
    snapshot_record: ManifestRecord,
    sst_ids: Vec<usize>,
    vlog_ids: Vec<u32>,
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
    fn acquire(target_dir: &Path) -> Result<Self> {
        let lock_path = checkpoint_lock_path(target_dir)?;
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
        lock_file.sync_all()?;

        Ok(Self { file: lock_file })
    }
}

#[derive(Serialize)]
struct CheckpointMarker<'a> {
    version: u32,
    state: &'a str,
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
        let _target_lock = TargetCheckpointLock::acquire(&target_dir)?;
        self.validate_checkpoint_target(&target_dir, &options)?;
        cleanup_checkpoint_tmps_for_target(&target_dir)?;
        let tmp_dir = checkpoint_tmp_dir(&target_dir);

        let result = (|| {
            let prepared = {
                let _checkpoint_guard = self.checkpoint_lock.lock();
                self.prepare_checkpoint(&tmp_dir)?
            };
            self.publish_prepared_checkpoint(&target_dir, &tmp_dir, &options, prepared)
        })();
        if result.is_err() {
            let _ = cleanup_checkpoint_tmp(&tmp_dir);
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

    fn prepare_checkpoint<'a>(&'a self, tmp_dir: &Path) -> Result<PreparedCheckpoint<'a>> {
        fs::create_dir_all(tmp_dir)
            .with_context(|| format!("failed to create checkpoint tmp {}", tmp_dir.display()))?;
        write_marker(tmp_dir.join("CHECKPOINT_IN_PROGRESS"), "in_progress", None)?;
        fsync_dir(tmp_dir)?;
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_in_progress_marker");
        }

        self.flush_all_memtables_for_checkpoint()?;

        let snapshot_pins = self.checkpoint_manifest_snapshot_record_and_pin();
        let pin_guard = CheckpointPinGuard {
            inner: self,
            sst_ids: snapshot_pins.sst_ids.clone(),
        };
        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("checkpoint.after_sst_pin_before_copy");
        }

        Ok(PreparedCheckpoint {
            snapshot_record: snapshot_pins.snapshot_record,
            sst_ids: snapshot_pins.sst_ids,
            vlog_ids: snapshot_pins.vlog_ids,
            _pin_guard: pin_guard,
            _vlog_pin_guard: snapshot_pins.vlog_pin_guard,
        })
    }

    fn publish_prepared_checkpoint(
        &self,
        target_dir: &Path,
        tmp_dir: &Path,
        options: &CheckpointOptions,
        prepared: PreparedCheckpoint<'_>,
    ) -> Result<CheckpointStats> {
        let mut stats = CheckpointStats {
            sst_count: prepared.sst_ids.len(),
            ..CheckpointStats::default()
        };
        copy_checkpoint_ssts(self, tmp_dir, &prepared.sst_ids, options, &mut stats)?;
        copy_checkpoint_vlogs(self, tmp_dir, &prepared.vlog_ids, options, &mut stats)?;
        write_checkpoint_manifest(tmp_dir, prepared.snapshot_record)?;
        write_marker(tmp_dir.join("CHECKPOINT_READY"), "ready", Some(stats))?;
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
        rename_no_replace(tmp_dir, target_dir).with_context(|| {
            format!(
                "failed to publish checkpoint {} from {}",
                target_dir.display(),
                tmp_dir.display()
            )
        })?;
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

    fn checkpoint_manifest_snapshot_record_and_pin(&self) -> CheckpointSnapshotPins<'_> {
        let _state_lock = self.state_lock.lock();
        let guard = self.state.load();
        let state = guard.as_ref();
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
        let vlog_pin_guard = self.vlog.as_ref().map(|vlog| {
            vlog.pin_files_for_checkpoint(&vlog_ids);
            VlogCheckpointPinGuard {
                vlog,
                file_ids: vlog_ids.clone(),
            }
        });
        CheckpointSnapshotPins {
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
            },
            sst_ids,
            vlog_ids,
            vlog_pin_guard,
        }
    }
}

fn write_checkpoint_manifest(tmp_dir: &Path, snapshot_record: ManifestRecord) -> Result<()> {
    let manifest = Manifest::create(tmp_dir.join("MANIFEST"))?;
    manifest.snapshot(snapshot_record)
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

        if options.include_vlog_indexes {
            let source_index = crate::vlog::index::index_path_for_vlog(&source);
            match fs::metadata(&source_index) {
                Ok(metadata) if metadata.is_file() => {
                    let target_index = target_vlog_dir.join(format!("{file_id}.vidx"));
                    if let Err(err) =
                        copy_or_link_optional_file(&source_index, &target_index, options, stats)
                    {
                        return Err(err).with_context(|| {
                            format!("failed to checkpoint vLog index {}", source_index.display())
                        });
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
) -> Result<()> {
    match copy_or_link_file(source, target, options, stats) {
        Ok(()) => Ok(()),
        Err(err) if is_not_found(&err) => Ok(()),
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
                stats.files_hard_linked += 1;
                stats.bytes_referenced += size;
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
    stats.files_copied += 1;
    stats.bytes_copied += size;

    Ok(())
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|err| err.kind() == ErrorKind::NotFound)
    })
}

fn write_marker(path: impl AsRef<Path>, state: &str, stats: Option<CheckpointStats>) -> Result<()> {
    let stats = stats.unwrap_or_default();
    let marker = CheckpointMarker {
        version: 1,
        state,
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

fn cleanup_checkpoint_tmp(path: &Path) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove checkpoint tmp {}", path.display())),
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
            cleanup_checkpoint_tmp(&path)?;
        }
    }

    Ok(())
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

        copy_or_link_optional_file(&source, &target, &CheckpointOptions::default(), &mut stats)
            .unwrap();

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
}
