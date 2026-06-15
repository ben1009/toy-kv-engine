use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Ok, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::{compact::CompactionTask, lsm_storage::InstalledCompactionFilter};

pub(crate) struct Manifest {
    file: Arc<Mutex<File>>,
    path: PathBuf,
}

/// Current manifest format version for MVCC-enabled databases.
/// Version numbers align with the feature phase: 2 = MVCC Phase 2
/// (format hardening), 3 = compaction filters. Version 0 is reserved to
/// mean "legacy/field-absent" and must never be assigned as a valid format
/// version.
pub const MANIFEST_FORMAT_VERSION: u32 = 3;

#[derive(Serialize, Deserialize)]
pub(crate) enum ManifestRecord {
    /// Written as the first record in a new database to identify the format
    /// version. Version 3 = MVCC + compaction filters. Absence of this
    /// record means pre-MVCC.
    FormatVersion(u32),
    Flush(usize),
    NewMemtable(usize),
    /// (task, new_sst_ids)
    Compaction(CompactionTask, Vec<usize>),
    /// Flush with vLog references: (sst_id, vlog_file_ids)
    FlushV2(usize, Vec<u32>),
    /// Compaction with vLog references: (task, new_sst_ids, vlog_file_ids)
    CompactionV2(CompactionTask, Vec<usize>, Vec<u32>),
    /// A new vLog file was created
    NewVlogFile(u32),
    /// A vLog file was deleted
    DeleteVlogFile(u32),
    /// GC rewrote entries: old_vlog_id, new_vlog_id, keys_rewritten
    GcCompaction(u32, u32, usize),
    AddCompactionFilter(InstalledCompactionFilter),
    RemoveCompactionFilter(u64),
    /// A snapshot of the current LSM state for manifest compaction.
    /// Contains the full state needed to reconstruct the engine without
    /// replaying the entire manifest log.
    Snapshot {
        l0_sstables: Vec<usize>,
        levels: Vec<(usize, Vec<usize>)>,
        next_sst_id: usize,
        vlog_references: Vec<(usize, Vec<u32>)>,
        /// IDs of immutable memtables that have not yet been flushed.
        /// Preserved so WAL recovery can rebuild them on restart.
        imm_memtable_ids: Vec<usize>,
        #[serde(default)]
        active_compaction_filters: Vec<InstalledCompactionFilter>,
        #[serde(default)]
        next_compaction_filter_id: u64,
        /// Manifest format version. 0 = pre-MVCC (legacy/field-absent),
        /// 2 = MVCC, 3 = MVCC + compaction filters.
        /// Defaults to 0 when the field is missing from old snapshots written
        /// before this field existed. Version 0 is rejected on open — it is
        /// not a valid format version, only a sentinel for "field absent".
        #[serde(default)]
        format_version: u32,
    },
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let f = File::create_new(&path).context("failed to create manifest")?;

        Ok(Self {
            file: Arc::new(Mutex::new(f)),
            path,
        })
    }

    /// Recover manifest from file. If a `MANIFEST_SNAPSHOT` file exists alongside,
    /// reads the snapshot first, then replays any manifest records on top of it.
    /// The snapshot() method truncates the manifest BEFORE renaming the snapshot
    /// into place, so when the snapshot exists, any manifest records are guaranteed
    /// to be post-snapshot records (written after the snapshot completed).
    /// If no snapshot exists, returns all records (backward compatible).
    /// If MANIFEST is missing but MANIFEST_SNAPSHOT exists, creates a new empty MANIFEST.
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let path = path.as_ref();
        let snapshot_path = Self::snapshot_path(path);

        let mut records = Vec::new();
        let tmp_path = snapshot_path.with_extension("tmp");

        // Check for MANIFEST_SNAPSHOT first, then MANIFEST_SNAPSHOT.tmp.
        // The tmp file exists if the process crashed after truncating MANIFEST
        // but before renaming the tmp file into place.
        let snapshot_buf = if snapshot_path.exists() {
            Some(fs::read(&snapshot_path).context("failed to read MANIFEST_SNAPSHOT")?)
        } else if tmp_path.exists() {
            // Tmp file exists but wasn't renamed — rename it now to complete
            // the handoff that was interrupted by the crash.
            let buf = fs::read(&tmp_path).context("failed to read MANIFEST_SNAPSHOT.tmp")?;
            // Validate it's valid JSON before renaming
            let _: ManifestRecord =
                serde_json::from_slice(&buf).context("failed to validate MANIFEST_SNAPSHOT.tmp")?;
            fs::rename(&tmp_path, &snapshot_path)
                .context("failed to rename MANIFEST_SNAPSHOT.tmp to MANIFEST_SNAPSHOT")?;
            Some(buf)
        } else {
            None
        };

        if let Some(buf) = snapshot_buf {
            let record: ManifestRecord =
                serde_json::from_slice(&buf).context("failed to deserialize MANIFEST_SNAPSHOT")?;
            records.push(record);
        }

        // Read manifest file (may be empty after snapshot truncation, or missing if
        // snapshot was renamed after manifest was deleted). Any records here are
        // post-snapshot records safe to replay on top of the snapshot state.
        let manifest_exists = path.exists();
        let mut f = if manifest_exists {
            File::options()
                .read(true)
                .append(true)
                .open(path)
                .context("failed to open recover manifest")?
        } else {
            File::options()
                .create_new(true)
                .read(true)
                .append(true)
                .open(path)
                .context("failed to create new manifest after snapshot")?
        };

        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        if !buf.is_empty() {
            let manifest_records =
                serde_json::Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();
            for record in manifest_records {
                records.push(record?);
            }
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(f)),
                path: path.to_path_buf(),
            },
            records,
        ))
    }

    /// Take a snapshot of the current state and replace the manifest file.
    ///
    /// Crash-safe ordering:
    /// 1. Write snapshot to temp file + fsync
    /// 2. Truncate MANIFEST to empty + fsync
    /// 3. Atomic rename temp → MANIFEST_SNAPSHOT + fsync dir
    ///
    /// By truncating the manifest BEFORE renaming the snapshot, we guarantee
    /// that at most one of {old MANIFEST, MANIFEST_SNAPSHOT} is visible on
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
        let snapshot_path = Self::snapshot_path(&self.path);
        let tmp_path = snapshot_path.with_extension("tmp");

        // Step 1: Write snapshot to temp file and fsync
        let buf = serde_json::to_vec(&record)?;
        {
            let mut tmp_file =
                File::create(&tmp_path).context("failed to create MANIFEST_SNAPSHOT.tmp")?;
            tmp_file.write_all(&buf)?;
            tmp_file
                .sync_all()
                .context("failed to sync MANIFEST_SNAPSHOT.tmp")?;
        }

        // Step 2+3: Truncate MANIFEST then rename snapshot, all under the
        // manifest lock to prevent new records from being written between them.
        let dir = self.path.parent().unwrap_or(Path::new("."));
        {
            let mut file = self.file.lock();
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.sync_all()
                .context("failed to sync truncated manifest")?;

            // Step 3: Atomic rename over MANIFEST_SNAPSHOT
            fs::rename(&tmp_path, &snapshot_path).context("failed to rename MANIFEST_SNAPSHOT")?;

            // Fsync parent directory to ensure rename is durable
            File::open(dir)
                .context("failed to open parent dir for sync")?
                .sync_all()
                .context("failed to sync dir after MANIFEST_SNAPSHOT rename")?;
        }

        Ok(())
    }

    /// Return the current size of the manifest file in bytes.
    pub fn file_size(&self) -> Result<u64> {
        let file = self.file.lock();
        let metadata = file.metadata()?;

        Ok(metadata.len())
    }

    /// The path for the MANIFEST_SNAPSHOT file (sibling of MANIFEST).
    fn snapshot_path(manifest_path: &Path) -> PathBuf {
        manifest_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("MANIFEST_SNAPSHOT")
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
        file.write_all(&buf)?;

        file.sync_all().context("failed to sync manifest")
    }
}
