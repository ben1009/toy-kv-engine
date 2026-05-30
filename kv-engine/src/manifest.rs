use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Ok, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
    path: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
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
    /// A snapshot of the current LSM state for manifest compaction.
    /// Contains the full state needed to reconstruct the engine without
    /// replaying the entire manifest log.
    Snapshot {
        l0_sstables: Vec<usize>,
        levels: Vec<(usize, Vec<usize>)>,
        next_sst_id: usize,
        vlog_references: Vec<(usize, Vec<u32>)>,
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
    /// reads the snapshot first and returns only the post-snapshot records from the
    /// manifest file. If no snapshot exists, returns all records (backward compatible).
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let path = path.as_ref();
        let snapshot_path = Self::snapshot_path(path);

        let (snapshot_record, mut records) = if snapshot_path.exists() {
            // Read snapshot
            let snapshot_buf =
                fs::read(&snapshot_path).context("failed to read MANIFEST_SNAPSHOT")?;
            let record: ManifestRecord = serde_json::from_slice(&snapshot_buf)
                .context("failed to deserialize MANIFEST_SNAPSHOT")?;
            (Some(record), vec![])
        } else {
            (None, vec![])
        };

        // Read manifest file (may be empty after snapshot truncation)
        let mut f = File::options()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open recover manifest")?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        if !buf.is_empty() {
            let manifest_records =
                serde_json::Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();
            for record in manifest_records {
                records.push(record?);
            }
        }

        // If we have a snapshot, prepend it so recovery can reconstruct state from it
        if let Some(snapshot) = snapshot_record {
            records.insert(0, snapshot);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(f)),
                path: path.to_path_buf(),
            },
            records,
        ))
    }

    /// Take a snapshot of the current state and truncate the manifest file.
    ///
    /// This writes the snapshot atomically to `MANIFEST_SNAPSHOT` (via temp file + rename),
    /// then truncates `MANIFEST` to empty. On crash:
    /// - Before snapshot durable: old MANIFEST has all records, snapshot absent → full replay
    /// - After snapshot, before truncate: snapshot exists, old MANIFEST has redundant records →
    ///   snapshot + replay (idempotent)
    /// - After truncate: snapshot + empty manifest → clean recovery
    pub fn snapshot(&self, record: ManifestRecord) -> Result<()> {
        let snapshot_path = Self::snapshot_path(&self.path);
        let tmp_path = snapshot_path.with_extension("tmp");

        // Write snapshot to temp file and fsync
        let buf = serde_json::to_vec(&record)?;
        {
            let mut tmp_file =
                File::create(&tmp_path).context("failed to create MANIFEST_SNAPSHOT.tmp")?;
            tmp_file.write_all(&buf)?;
            tmp_file
                .sync_all()
                .context("failed to sync MANIFEST_SNAPSHOT.tmp")?;
        }

        // Atomic rename over MANIFEST_SNAPSHOT
        fs::rename(&tmp_path, &snapshot_path).context("failed to rename MANIFEST_SNAPSHOT")?;

        // Truncate MANIFEST to empty
        let mut file = self.file.lock();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.sync_all()
            .context("failed to sync truncated manifest")?;

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

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let buf = serde_json::to_vec(&record)?;
        file.write_all(buf.as_slice())?;

        file.sync_all().context("failed to sync manifest")
    }
}
