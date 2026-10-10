use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Ok, Result, ensure};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::{
    compact::CompactionTask, lsm_storage::InstalledCompactionFilter,
    pitr::manifest::PitrManifestRecord,
};

pub(crate) struct Manifest {
    file: Arc<Mutex<File>>,
    path: PathBuf,
    durability_uncertain: AtomicBool,
}

#[cfg(test)]
static MANIFEST_SYNC_FAILURE: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn set_manifest_sync_failure(path: &Path) {
    *MANIFEST_SYNC_FAILURE.lock().unwrap() = Some(path.to_path_buf());
}

/// Current manifest format version for MVCC-enabled databases.
/// Version numbers align with the feature phase: 2 = MVCC Phase 2
/// (format hardening), 3 = compaction filters, 4 = range tombstones,
/// 5 = TTL (native key-value time-to-live). Version 6 reserves immutable-file
/// identities for incremental backup and is published only once every immutable
/// write path maintains that metadata. Version 7 requires every snapshot to
/// carry PITR state, so manifest snapshot replacement cannot discard it.
/// Version 0 is reserved to mean "legacy/field-absent" and must never be
/// assigned as a valid format version.
pub const MANIFEST_FORMAT_VERSION: u32 = 7;

/// The immutable file kinds that can be referenced by a physical backup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ImmutableFileKind {
    Sst,
    Vlog,
}

/// Persisted identity for a fully published immutable file.
///
/// Phase 1 fixes the algorithm to SHA-256. This is stored in manifest
/// snapshots so normal incremental backups need not hash unchanged source
/// files merely to decide object reuse.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImmutableFileMetadata {
    pub kind: ImmutableFileKind,
    pub file_id: u64,
    pub file_size: u64,
    pub file_checksum: [u8; 32],
}

impl ImmutableFileMetadata {
    /// Returns the logical identity used to match immutable files across snapshots.
    pub fn identity(&self) -> (ImmutableFileKind, u64) {
        (self.kind, self.file_id)
    }

    /// Returns whether another metadata record describes the same immutable bytes.
    pub fn matches(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.file_id == other.file_id
            && self.file_size == other.file_size
            && self.file_checksum == other.file_checksum
    }
}

#[cfg(test)]
mod manifest_revalidation_tests {
    use super::{Manifest, ManifestRecord};
    use std::io::Write;

    #[test]
    fn revalidation_reports_whether_a_batch_is_already_appended() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::create(dir.path().join("MANIFEST")).unwrap();
        let state_lock = parking_lot::Mutex::new(());
        let state_guard = state_lock.lock();
        let appended = [ManifestRecord::Flush(1)];

        assert!(
            !manifest.revalidate_appended_records(&appended, 0).unwrap(),
            "a batch that was never appended must not revalidate"
        );

        manifest.add_records(&state_guard, &appended).unwrap();
        assert!(
            manifest.revalidate_appended_records(&appended, 0).unwrap(),
            "an appended batch must revalidate as present"
        );
        let after_first = manifest.current_length().unwrap();
        assert!(
            !manifest
                .revalidate_appended_records(&[ManifestRecord::Flush(2)], after_first)
                .unwrap(),
            "a different batch must not match the appended suffix"
        );
    }

    #[test]
    fn revalidation_discards_a_short_write_so_recovery_can_still_read_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let manifest = Manifest::create(&path).unwrap();
        let length_before = manifest.current_length().unwrap();
        let batch = [ManifestRecord::Flush(7)];
        let mut encoded = Vec::new();
        for record in &batch {
            serde_json::to_writer(&mut encoded, record).unwrap();
        }
        assert!(encoded.len() > 4);

        // A failing `write_all` can leave a proper prefix of the batch behind.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&encoded[..encoded.len() / 2]).unwrap();
        file.sync_all().unwrap();

        assert!(
            !manifest
                .revalidate_appended_records(&batch, length_before)
                .unwrap(),
            "a partial append is not a landed batch, so it must not read as present"
        );
        let after = std::fs::read(&path).unwrap();
        assert_eq!(
            after.len() as u64,
            length_before,
            "the partial prefix must be discarded, not stranded in the stream"
        );
        // Discarding it is what makes the answer usable: recovery parses the
        // manifest as JSON, so a torn tail would make the database refuse to open.
        let parsed = serde_json::Deserializer::from_slice(&after)
            .into_iter::<ManifestRecord>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(parsed.is_empty());
    }

    /// Appends write at the manifest descriptor's current offset rather than at
    /// the end, so discarding a partial prefix from a second descriptor leaves
    /// that offset past the new end and the next record lands behind a run of
    /// zeros. Recovery parses the manifest as JSON, so that corrupts the stream
    /// in the middle instead of merely ending it.
    #[test]
    fn revalidation_keeps_the_manifest_offset_with_its_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let manifest = Manifest::create(&path).unwrap();
        let state_lock = parking_lot::Mutex::new(());
        let state_guard = state_lock.lock();

        manifest
            .add_records(&state_guard, &[ManifestRecord::Flush(1)])
            .unwrap();
        let length_before = manifest.current_length().unwrap();
        assert!(length_before > 0);

        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &ManifestRecord::Flush(7)).unwrap();
        assert!(encoded.len() > 4);
        // The partial prefix a failing `write_all` leaves, written through the
        // manifest's own descriptor so its offset moves exactly as it would.
        {
            let mut file = manifest.file.lock();
            file.write_all(&encoded[..encoded.len() / 2]).unwrap();
            file.sync_all().unwrap();
        }

        assert!(
            !manifest
                .revalidate_appended_records(&[ManifestRecord::Flush(7)], length_before)
                .unwrap(),
            "a partial append is not a landed batch"
        );
        assert_eq!(manifest.current_length().unwrap(), length_before);

        // The next append has to follow the last good record directly.
        manifest
            .add_records(&state_guard, &[ManifestRecord::Flush(8)])
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let parsed = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<ManifestRecord>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(parsed.len(), 2, "both records must be readable");
        assert!(matches!(&parsed[0], ManifestRecord::Flush(1)));
        assert!(matches!(&parsed[1], ManifestRecord::Flush(8)));
    }
}

#[cfg(test)]
mod immutable_file_metadata_tests {
    use super::{ImmutableFileKind, ImmutableFileMetadata};

    #[test]
    fn identity_key_uses_kind_and_file_id() {
        let metadata = ImmutableFileMetadata {
            kind: ImmutableFileKind::Sst,
            file_id: 42,
            file_size: 7,
            file_checksum: [0xab; 32],
        };
        assert_eq!(metadata.identity(), (ImmutableFileKind::Sst, 42));
        assert!(metadata.matches(&metadata));
        let mut changed = metadata.clone();
        changed.file_size += 1;
        assert!(!metadata.matches(&changed));
    }
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ManifestRecord {
    /// Written as the first record in a new database to identify the format
    /// version. Version 5 = MVCC + compaction filters + range tombstones + TTL,
    /// 6 = immutable-file identities, 7 = mandatory PITR state in snapshots.
    /// Absence of this record means pre-MVCC.
    FormatVersion(u32),
    /// Dormant PITR v7 transition record. Replayed only when PITR is enabled.
    Pitr(PitrManifestRecord),
    Flush(usize),
    NewMemtable(usize),
    /// A new memtable whose WAL is a PITR segment. `NewMemtable` stays for the
    /// non-PITR path; a log that predates this variant carries no segment, and
    /// recovery matches those memtables to WALs positionally.
    NewPitrMemtable {
        id: usize,
        segment_id: u64,
    },
    /// (task, new_sst_ids)
    Compaction(CompactionTask, Vec<usize>),
    /// Flush with vLog references: (sst_id, vlog_file_ids)
    FlushV2(usize, Vec<u32>),
    /// Flush with vLog references and immutable-file identity metadata.
    FlushV3(usize, Vec<u32>, Vec<ImmutableFileMetadata>),
    /// Compaction with vLog references: (task, new_sst_ids, vlog_file_ids)
    CompactionV2(CompactionTask, Vec<usize>, Vec<u32>),
    /// Compaction with range-only SSTs: (task, new_sst_ids, vlog_file_ids, range_only_sst_ids)
    CompactionV3(CompactionTask, Vec<usize>, Vec<u32>, Vec<usize>),
    /// Compaction with output immutable-file metadata for recovery.
    CompactionV4(
        CompactionTask,
        Vec<usize>,
        Vec<u32>,
        Vec<usize>,
        Vec<ImmutableFileMetadata>,
    ),
    /// A new vLog file was created
    NewVlogFile(u32),
    /// A vLog file was deleted
    DeleteVlogFile(u32),
    /// GC rewrote entries: old_vlog_id, new_vlog_id, keys_rewritten
    GcCompaction(u32, u32, usize),
    /// vLog GC with immutable metadata for the newly created file.
    GcCompactionV2(u32, u32, usize, ImmutableFileMetadata),
    /// A source vLog file whose consuming SST references were durably retired.
    VlogRetire(u32),
    AddCompactionFilter(InstalledCompactionFilter),
    RemoveCompactionFilter(u64),
    /// A snapshot of the current LSM state for manifest compaction.
    /// Contains the full state needed to reconstruct the engine without
    /// replaying the entire manifest log.
    Snapshot {
        l0_sstables: Vec<usize>,
        levels: Vec<(usize, Vec<usize>)>,
        /// Range-only SSTs per level. Added in Phase 4 (SST v4 compaction GC).
        #[serde(default)]
        range_only_ssts: Vec<(usize, Vec<usize>)>,
        next_sst_id: usize,
        vlog_references: Vec<(usize, Vec<u32>)>,
        /// IDs of immutable memtables that have not yet been flushed.
        /// Preserved so WAL recovery can rebuild them on restart.
        imm_memtable_ids: Vec<usize>,
        /// The segment each listed memtable's WAL belongs to. A snapshot replaces
        /// the records that named them, so it carries the mapping itself.
        /// Snapshots written before this field existed carry none, and recovery
        /// pairs those memtables with segment WALs by mint order.
        #[serde(default)]
        pitr_memtable_segments: Vec<(usize, u64)>,
        #[serde(default)]
        active_compaction_filters: Vec<InstalledCompactionFilter>,
        #[serde(default)]
        next_compaction_filter_id: u64,
        /// Manifest format version. 0 = pre-MVCC (legacy/field-absent),
        /// 2 = MVCC, 3 = MVCC + compaction filters, 6 = immutable-file
        /// identities, 7 = mandatory PITR state.
        /// Defaults to 0 when the field is missing from old snapshots written
        /// before this field existed. Version 0 is rejected on open — it is
        /// not a valid format version, only a sentinel for "field absent".
        #[serde(default)]
        format_version: u32,
        /// Complete metadata for currently live immutable SST/vLog files.
        #[serde(default)]
        immutable_file_metadata: Vec<ImmutableFileMetadata>,
        /// PITR lifecycle state preserved across manifest compaction.
        /// Required for version 7 and later snapshots; optional on older ones.
        #[serde(default)]
        pitr_state: Option<crate::pitr::manifest::PitrState>,
    },
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let f = File::create_new(&path).context("failed to create manifest")?;

        Ok(Self {
            file: Arc::new(Mutex::new(f)),
            path,
            durability_uncertain: AtomicBool::new(false),
        })
    }

    /// Recover manifest from file. If a `ENGINE_MANIFEST` file exists alongside,
    /// reads the snapshot first, then replays any manifest records on top of it.
    /// The snapshot() method truncates the manifest BEFORE renaming the snapshot
    /// into place, so when the snapshot exists, any manifest records are guaranteed
    /// to be post-snapshot records (written after the snapshot completed).
    /// If no snapshot exists, returns all records (backward compatible).
    /// If MANIFEST is missing but ENGINE_MANIFEST exists, creates a new empty MANIFEST.
    /// Accepted bytes are durably rewritten before replay is returned. Recovery
    /// requires exclusive database ownership with all old manifest handles closed.
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let path = path.as_ref();
        let snapshot = Self::read_recovery_snapshot(path)?;
        let mut records = if let Some((_, bytes)) = &snapshot {
            vec![serde_json::from_slice(bytes).context("failed to deserialize manifest snapshot")?]
        } else {
            Vec::new()
        };
        let mut f = Self::open_recovery_manifest(path)?;
        let bytes = Self::recover_manifest_records(&mut f, &mut records)?;
        drop(f);
        if let Some((source_path, snapshot_bytes)) = &snapshot {
            // Stabilize a pending snapshot under its pending name before making
            // MANIFEST's empty state durable. Its final rename must wait until
            // the accepted MANIFEST bytes are installed, or a failed truncate
            // could restore old suffix records alongside the new snapshot.
            drop(Self::rewrite_recovered_file(source_path, snapshot_bytes)?);
        }
        // A prior process may have observed an fsync error after appending a
        // complete record. Failed writeback can leave clean, readable cache
        // pages, and a new descriptor need not report the old error. Rewrite
        // every accepted byte on a fresh inode before exposing replayed state.
        let f = Self::rewrite_recovered_file(path, &bytes)?;
        if let Some((source_path, _)) = snapshot
            && source_path == Self::snapshot_tmp_path(path)
        {
            let snapshot_path = Self::snapshot_path(path);
            fs::rename(source_path, &snapshot_path)
                .context("failed to rename ENGINE_MANIFEST.tmp to ENGINE_MANIFEST")?;
            Self::sync_parent_directory(&snapshot_path)?;
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(f)),
                path: path.to_path_buf(),
                durability_uncertain: AtomicBool::new(false),
            },
            records,
        ))
    }

    /// Take a snapshot of the current state and replace the manifest file.
    ///
    /// Crash-safe ordering:
    /// 1. Write snapshot to temp file + fsync
    /// 2. Truncate MANIFEST to empty + fsync
    /// 3. Atomic rename temp → ENGINE_MANIFEST + fsync dir
    ///
    /// By truncating the manifest BEFORE renaming the snapshot, we guarantee
    /// that at most one of {old MANIFEST, ENGINE_MANIFEST} is visible on
    /// recovery. This avoids the ambiguous "both exist" window where replaying
    /// old manifest records on top of a snapshot would create duplicates.
    ///
    /// Crash windows:
    /// - Before step 2: old MANIFEST intact, no snapshot → full replay
    /// - After step 2, before step 3 durable: MANIFEST empty, no snapshot. Old data is lost. To
    ///   prevent this, step 2+3 are performed while holding the manifest lock, so no new records
    ///   can be written between truncate and rename. If the process crashes between them, the old
    ///   MANIFEST data is lost but the snapshot tmp file exists on disk. Recovery creates a fresh
    ///   MANIFEST from the snapshot.
    /// - After step 3 durable: snapshot + empty manifest → clean recovery
    pub fn snapshot(&self, record: ManifestRecord) -> Result<()> {
        self.ensure_durability_certain()?;
        let snapshot_path = Self::snapshot_path(&self.path);
        let tmp_path = snapshot_path.with_extension("tmp");

        // Step 1: Write snapshot to temp file and fsync
        let buf = serde_json::to_vec(&record)?;
        {
            let mut tmp_file =
                File::create(&tmp_path).context("failed to create ENGINE_MANIFEST.tmp")?;
            tmp_file.write_all(&buf)?;
            tmp_file
                .sync_all()
                .context("failed to sync ENGINE_MANIFEST.tmp")?;
        }

        #[cfg(feature = "chaos-testing")]
        {
            crate::chaos::failpoint::fail_point!("manifest.after_snapshot_tmp_sync");
        }

        // Step 2+3: Truncate MANIFEST then rename snapshot, all under the
        // manifest lock to prevent new records from being written between them.
        let dir = self.path.parent().unwrap_or(Path::new("."));

        {
            let mut file = self.file.lock();
            self.ensure_durability_certain()?;
            if let Err(error) = file.set_len(0) {
                self.mark_durability_uncertain();
                return Err(error).context("failed to truncate manifest for snapshot");
            }
            if let Err(error) = file.seek(SeekFrom::Start(0)) {
                self.mark_durability_uncertain();
                return Err(error).context("failed to reposition truncated manifest");
            }
            self.sync_locked_file(&file)
                .context("failed to sync truncated manifest")?;

            #[cfg(feature = "chaos-testing")]
            {
                crate::chaos::failpoint::fail_point!("manifest.after_truncate_before_rename");
            }

            // Step 3: Atomic rename over ENGINE_MANIFEST
            if let Err(error) = fs::rename(&tmp_path, &snapshot_path) {
                self.mark_durability_uncertain();
                return Err(error).context("failed to rename ENGINE_MANIFEST");
            }

            #[cfg(test)]
            if std::env::var_os("PITR_PROCESS_KILL_AFTER_MANIFEST_SNAPSHOT_RENAME").is_some() {
                // SAFETY: this is an isolated child-process crash test at the
                // manifest snapshot rename boundary.
                unsafe { libc::_exit(137) }
            }

            #[cfg(feature = "chaos-testing")]
            {
                crate::chaos::failpoint::fail_point!("manifest.after_rename_before_dir_sync");
            }

            // Fsync parent directory to ensure rename is durable
            let dir_sync = File::open(dir)
                .context("failed to open parent dir for sync")
                .and_then(|dir| {
                    dir.sync_all()
                        .context("failed to sync dir after ENGINE_MANIFEST rename")
                });
            if let Err(error) = dir_sync {
                self.mark_durability_uncertain();
                return Err(error);
            }
        }

        Ok(())
    }

    /// Return the current size of the manifest file in bytes.
    pub fn file_size(&self) -> Result<u64> {
        let file = self.file.lock();
        let metadata = file.metadata()?;

        Ok(metadata.len())
    }

    /// The path for the ENGINE_MANIFEST file (sibling of MANIFEST).
    fn snapshot_path(manifest_path: &Path) -> PathBuf {
        manifest_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("ENGINE_MANIFEST")
    }

    fn snapshot_tmp_path(manifest_path: &Path) -> PathBuf {
        Self::snapshot_path(manifest_path).with_extension("tmp")
    }

    fn read_recovery_snapshot(path: &Path) -> Result<Option<(PathBuf, Vec<u8>)>> {
        let snapshot_path = Self::snapshot_path(path);
        let tmp_path = Self::snapshot_tmp_path(path);
        let manifest_empty_or_missing = match fs::metadata(path) {
            std::result::Result::Ok(meta) => meta.len() == 0,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
            Err(err) => return Err(err).context("failed to stat MANIFEST"),
        };

        // Only recover the tmp snapshot after MANIFEST was truncated or is
        // missing. If MANIFEST still has data, the tmp file may have been
        // written before truncation and replaying both would duplicate or
        // stale snapshot history.
        let recover_tmp = tmp_path.exists() && manifest_empty_or_missing;
        let source_path = if recover_tmp {
            &tmp_path
        } else if snapshot_path.exists() {
            &snapshot_path
        } else {
            return Ok(None);
        };
        let buf = fs::read(source_path).context("failed to read manifest snapshot")?;

        Ok(Some((source_path.clone(), buf)))
    }

    /// Install the exact accepted bytes without overwriting the source inode.
    /// Recovery runs under exclusive database ownership. Leftover scratch is
    /// never authoritative and can be recreated after an interrupted attempt.
    fn rewrite_recovered_file(path: &Path, bytes: &[u8]) -> Result<File> {
        let tmp_path = path.with_extension("recover");
        match fs::remove_file(&tmp_path) {
            std::result::Result::Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("failed to remove recovery scratch"),
        }
        let result = (|| {
            let mut file = File::options()
                .read(true)
                .append(true)
                .create_new(true)
                .open(&tmp_path)
                .context("failed to create recovered manifest image")?;
            file.set_permissions(fs::metadata(path)?.permissions())?;
            file.write_all(bytes)
                .context("failed to write recovered manifest image")?;
            Self::sync_file(&file, path).context("failed to sync recovered manifest image")?;
            #[cfg(all(test, target_os = "linux"))]
            if std::env::var_os("PITR_PROCESS_KILL_AFTER_MANIFEST_RECOVERY_SYNC").as_deref()
                == Some(path.as_os_str())
            {
                // SAFETY: this is an isolated child-process crash test after
                // the replacement image is synced, before the canonical rename.
                unsafe { libc::_exit(137) }
            }
            fs::rename(&tmp_path, path).context("failed to install recovered manifest image")?;
            #[cfg(all(test, target_os = "linux"))]
            if std::env::var_os("PITR_PROCESS_KILL_AFTER_MANIFEST_RECOVERY_RENAME").as_deref()
                == Some(path.as_os_str())
            {
                // SAFETY: this is an isolated child-process crash test after
                // replacement, before its directory entry is synced.
                unsafe { libc::_exit(137) }
            }
            Self::sync_parent_directory(path)?;
            Ok(file)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }

        result
    }

    fn sync_parent_directory(path: &Path) -> Result<()> {
        let directory = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(directory)
            .context("failed to open recovered manifest directory")?
            .sync_all()
            .context("failed to sync recovered manifest directory")
    }

    fn open_recovery_manifest(path: &Path) -> Result<File> {
        // The manifest may be empty after snapshot truncation, or missing if the
        // snapshot rename completed after the old manifest was deleted.
        if path.exists() {
            return File::options()
                .read(true)
                .append(true)
                .open(path)
                .context("failed to open recover manifest");
        }

        File::options()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)
            .context("failed to create new manifest after snapshot")
    }

    fn recover_manifest_records(
        file: &mut File,
        records: &mut Vec<ManifestRecord>,
    ) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        if buf.is_empty() {
            return Ok(buf);
        }

        let manifest_records =
            serde_json::Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();

        for record in manifest_records {
            records.push(record?);
        }

        Ok(buf)
    }

    /// take a record of the changes in the LsmStorageState
    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    /// Batch multiple manifest records with a single fsync.
    pub fn add_records(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        records: &[ManifestRecord],
    ) -> Result<()> {
        self.add_records_when_init(records)
    }

    /// Establish durability for previously appended records without changing the
    /// manifest stream or its append position. If the sync itself fails, this
    /// instance is poisoned and must be reopened before it can be mutated again.
    pub(crate) fn sync(&self, _state_lock_observer: &MutexGuard<()>) -> Result<()> {
        self.sync_locked_file(&self.file.lock())
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        self.add_records_when_init(std::slice::from_ref(&record))
    }

    /// Batch multiple manifest records into a single fsync.
    /// Records are serialized into a buffer before acquiring the lock
    /// to reduce contention and ensure atomic write.
    pub fn add_records_when_init(&self, records: &[ManifestRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for record in records {
            serde_json::to_writer(&mut buf, record)?;
        }
        let mut file = self.file.lock();
        self.ensure_durability_certain()?;
        file.write_all(&buf)?;

        #[cfg(test)]
        if std::env::var_os("PITR_PROCESS_KILL_AFTER_MANIFEST_APPEND").is_some() {
            // SAFETY: this is an isolated child-process crash test at the
            // manifest append-before-sync boundary.
            unsafe { libc::_exit(137) }
        }

        #[cfg(feature = "chaos-testing")]
        {
            let retirement_batch = records
                .iter()
                .all(|record| matches!(record, ManifestRecord::VlogRetire(_)));
            crate::chaos::failpoint::fail_point!(
                "manifest.before_vlog_retirement_sync",
                retirement_batch,
                |_| Err(anyhow::anyhow!("injected vLog retirement manifest failure"))
            );
        }

        #[cfg(feature = "chaos-testing")]
        {
            // The closure gives tests a `return(<reason>)` action. The records are
            // already in the file at this point, so a returned failure is exactly the
            // published-but-not-durable case a caller has to revalidate instead of
            // assuming the append was lost.
            crate::chaos::failpoint::fail_point!("manifest.after_append_before_sync", |_| Err(
                anyhow::anyhow!("injected manifest sync failure")
            ));
        }

        self.sync_locked_file(&file)
    }

    fn sync_locked_file(&self, file: &File) -> Result<()> {
        self.ensure_durability_certain()?;
        let result = Self::sync_file(file, &self.path);
        if result.is_err() {
            // A later sync cannot prove that the old writeback error was cleared.
            // Recovery must rewrite accepted bytes before mutations resume.
            self.mark_durability_uncertain();
        }
        result
    }

    fn sync_file(file: &File, path: &Path) -> Result<()> {
        #[cfg(test)]
        {
            let mut configured = MANIFEST_SYNC_FAILURE.lock().unwrap();
            if configured
                .as_ref()
                .is_some_and(|configured| configured == path)
            {
                configured.take();
                return Err(std::io::Error::other("injected manifest sync failure").into());
            }
        }
        #[cfg(not(test))]
        let _ = path;

        file.sync_all().context("failed to sync manifest")
    }

    pub(crate) fn ensure_durability_certain(&self) -> Result<()> {
        ensure!(
            !self.durability_uncertain.load(Ordering::Acquire),
            "manifest durability is uncertain; reopen required"
        );
        Ok(())
    }

    fn mark_durability_uncertain(&self) {
        self.durability_uncertain.store(true, Ordering::Release);
    }

    /// Decide whether a failed append actually landed.
    ///
    /// `add_records` writes before it syncs, so an fsync error returns `Err`
    /// while the records may already be durable. Callers that must not repeat a
    /// durable transition re-check the file instead of assuming the append was
    /// lost. `length_before` is the length observed immediately before the failed
    /// append, read under the same state lock that serializes appenders.
    ///
    /// `Ok(true)` means the exact byte suffix the records would have appended is
    /// present. `Ok(false)` means the batch is known not to be there: either the
    /// file is byte-for-byte unchanged, or it grew by a proper prefix that is not
    /// a whole batch, which this call truncates away before saying so. Discarding
    /// that prefix is what makes the answer true - a torn record left in the
    /// stream parses as JSON to nobody, so `recover_manifest_records` would fail
    /// on it and the database would refuse to open, and that is the one outcome a
    /// caller cannot settle by reopening.
    pub(crate) fn revalidate_appended_records(
        &self,
        records: &[ManifestRecord],
        length_before: u64,
    ) -> Result<bool> {
        let mut expected = Vec::new();
        for record in records {
            serde_json::to_writer(&mut expected, record)?;
        }
        let bytes = fs::read(&self.path).context("failed to revalidate manifest append")?;
        if bytes.ends_with(&expected) {
            return Ok(true);
        }
        if bytes.len() as u64 == length_before {
            return Ok(false);
        }
        ensure!(
            bytes.len() as u64 > length_before,
            "the manifest shrank below the length it had before the failed append, so its \
             contents are unknown"
        );
        self.ensure_durability_certain()?;
        // A short write left a proper prefix of the batch behind. Cut it back to
        // the last known-good boundary so the append really is absent, which is
        // what `Ok(false)` reports and what the caller acts on.
        //
        // This has to go through the manifest's own descriptor and move its offset
        // with it. Appends write at the current offset rather than at the end, so
        // truncating from a second descriptor would leave that offset past the new
        // end and make the next record land behind a run of zeros, which recovery
        // cannot parse either - it reads the manifest as a stream of records and
        // refuses to open on any parse error, so a hole in the middle is no more
        // survivable than a tear at the end.
        let mut file = self.file.lock();
        self.ensure_durability_certain()?;
        if let Err(error) = file.set_len(length_before) {
            self.mark_durability_uncertain();
            return Err(error).context("failed to discard a partial manifest append");
        }
        if let Err(error) = file.seek(SeekFrom::Start(length_before)) {
            self.mark_durability_uncertain();
            return Err(error)
                .context("failed to reposition the manifest after discarding a partial append");
        }
        self.sync_locked_file(&file)
            .context("failed to sync the manifest after discarding a partial append")?;

        Ok(false)
    }

    /// Current manifest length. Read under the caller's state lock so it cannot
    /// race an append.
    pub(crate) fn current_length(&self) -> Result<u64> {
        Ok(fs::metadata(&self.path)
            .context("failed to read manifest length")?
            .len())
    }
}
